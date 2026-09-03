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

// HTTP API server for Antfly inference.
// Uses generated types and server router from openapi-zig.

const std = @import("std");
const builtin = @import("builtin");
const build_options = @import("build_options");
const platform = @import("antfly_platform");
const httpx = @import("httpx");
const api = @import("inference_api");
const generating_api = @import("antfly_generating_openapi");
const extraction_api = @import("antfly_extraction_openapi");
const readers_api = @import("antfly_readers");
const transcribing_api = @import("antfly_transcribing");
const extracting_api = @import("antfly_extracting");
const scraping = @import("antfly_scraping");
const ant_json = @import("antfly-json");
const jsonschema = @import("antfly_jsonschema");
const lib_chunker = @import("inference_chunker");
const hf_tokenizer_mod = @import("inference_hf_tokenizer");
const backends_mod = @import("../backends/backends.zig");
const session_factory = @import("../architectures/session_factory.zig");
const registry_mod = @import("../registry/registry.zig");
const extractors_mod = @import("../extractors/extractor.zig");
const cache_mod = @import("../cache/cache.zig");
const model_manager_mod = @import("model_manager.zig");
const model_caps = @import("../models/capabilities.zig");
const manifest_mod = @import("../models/manifest.zig");
const safetensors_mod = @import("../models/safetensors.zig");
const gpt_model_mod = @import("../models/gpt.zig");
const model_compatibility = @import("../models/compatibility.zig");
const chunking_mod = @import("../pipelines/chunking.zig");
const embedding_mod = @import("../pipelines/embedding.zig");
const extraction_mod = @import("../pipelines/extraction.zig");
const image_pipeline = @import("../pipelines/image.zig");
const sparse_embedding_mod = @import("../pipelines/sparse_embedding.zig");
const generation = @import("../pipelines/generation.zig");
const multimodal_reranker = @import("../pipelines/multimodal_reranker.zig");
const multimodal_qwen_adapter = @import("../pipelines/multimodal_qwen_adapter.zig");
const qwen3vl_multimodal_reranker = @import("../pipelines/qwen3vl_multimodal_reranker.zig");
const qwen3vl_projector = @import("../architectures/qwen3vl_projector.zig");
const qwen3vl_reranker = @import("../architectures/qwen3vl_reranker.zig");
const document_classification = @import("../pipelines/document_classification.zig");
const document_token_classification = @import("../pipelines/document_token_classification.zig");
const graph_mod = @import("../graph/root.zig");
const gliner_mod = @import("../pipelines/gliner.zig");
const grammar_mod = @import("../pipelines/grammar.zig");
const audio_mod = @import("../pipelines/audio.zig");
const readers_mod = @import("../readers/reader.zig");
const rebel_mod = @import("../pipelines/rebel.zig");
const resolver_mod = @import("../pipelines/resolver.zig");
const cleanup_pipeline_mod = @import("../pipelines/entity_cleanup.zig");
const cleanup_model_mod = @import("../finetune/entity_cleanup_model.zig");
const onnx_decoder_only_vlm = @import("../pipelines/onnx_decoder_only_vlm.zig");
const tool_parser_mod = @import("../pipelines/tool_parser.zig");
const ops = @import("../ops/ops.zig");
const runtime = @import("../runtime/root.zig");
const tabular_mod = @import("../tabular/root.zig");
const c_file = @import("../util/c_file.zig");
const a4b_prepared_pack = @import("../ops/cuda/a4b_prepared_pack.zig");
const native_backend_choice = @import("../native_backend_choice.zig");
const pjrt_lib = if (build_options.enable_pjrt) @import("pjrt") else struct {
    pub const pjrt = struct {
        pub const Client = struct {
            pub fn init(_: [:0]const u8) !@This() {
                return error.PjrtNotEnabled;
            }
            pub fn deinit(_: *@This()) void {}
        };
    };
};
pub const metrics_mod = @import("metrics.zig");
const inference_admission_mod = @import("inference_admission.zig");

fn spinLock(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) std.atomic.spinLoopHint();
}

fn shouldSkipAutoMtpDraftLoad(config: generation.GenerationConfig, draft_cfg: gpt_model_mod.Config) bool {
    if (config.speculation_policy != .auto) return false;
    if (!draft_cfg.gemma4_mtp_assistant) return false;
    if (config.speculation_calibration == .none) return true;
    const requested_max_tokens: usize = @intCast(@max(config.max_tokens, 1));
    return requested_max_tokens < generation.gemma4MtpAutoMinGenerationTokens();
}

fn requiresNativeChannelProjection(config: gpt_model_mod.Config) bool {
    return config.usesGemma4Channels();
}

fn generationPipelineSession(pipeline: anytype) ?backends_mod.Session {
    const Pipeline = switch (@typeInfo(@TypeOf(pipeline))) {
        .pointer => |pointer| pointer.child,
        else => @TypeOf(pipeline),
    };
    if (comptime @hasField(Pipeline, "session")) return pipeline.session;
    return null;
}

fn generationPipelineUsesReasoningChannels(pipeline: anytype) bool {
    const Pipeline = switch (@typeInfo(@TypeOf(pipeline))) {
        .pointer => |pointer| pointer.child,
        else => @TypeOf(pipeline),
    };
    if (comptime @hasField(Pipeline, "gpt_config")) {
        return requiresNativeChannelProjection(pipeline.gpt_config);
    }
    return false;
}

fn shouldDeferStreamContentForReasoning(
    pipeline: anytype,
    config: generation.GenerationConfig,
) bool {
    return generationPipelineUsesReasoningChannels(pipeline) and
        config.enable_thinking != false and
        // Grammar-constrained Gemma4 generation rewrites the prompt to open
        // the public final channel directly, so no private reasoning can
        // precede its incrementally streamed content.
        config.grammar == null;
}

test "channel streaming defers content only when reasoning can precede it" {
    const Probe = struct { gpt_config: gpt_model_mod.Config };
    const pipeline = Probe{ .gpt_config = .{
        .family = .gemma,
        .gemma4_channel_protocol = true,
    } };
    try std.testing.expect(shouldDeferStreamContentForReasoning(&pipeline, .{}));
    try std.testing.expect(!shouldDeferStreamContentForReasoning(&pipeline, .{ .enable_thinking = false }));
    try std.testing.expect(!shouldDeferStreamContentForReasoning(&pipeline, .{ .grammar = "json" }));
    const ordinary = Probe{ .gpt_config = .{ .family = .llama } };
    try std.testing.expect(!shouldDeferStreamContentForReasoning(&ordinary, .{}));
}

fn serverA4bTelemetryEnabled() bool {
    return platform.env.getenvBool("TERMITE_SERVER_A4B_TELEMETRY");
}

const ServerA4bTelemetry = struct {
    route_select_attempts: u64 = 0,
    route_select_successes: u64 = 0,
    route_select_fallbacks: u64 = 0,
    cuda_resident_source_bytes: u64 = 0,
    cuda_resident_source_count: u64 = 0,
    cuda_route_calls: u64 = 0,
    cuda_decode_calls: u64 = 0,
    cuda_prefill_calls: u64 = 0,
    slot_lookup_attempts: u64 = 0,
    slot_route_hits: u64 = 0,
    slot_route_misses: u64 = 0,
    slot_all_hit_layers: u64 = 0,
    slot_map_publications: u64 = 0,
    slot_map_publish_failures: u64 = 0,
    slot_arena_attempts: u64 = 0,
    slot_arena_successes: u64 = 0,
    slot_arena_failures: u64 = 0,
    selected_page_attempts: u64 = 0,
    selected_page_successes: u64 = 0,
    selected_page_failures: u64 = 0,
    selected_page_logical_bytes: u64 = 0,
    selected_page_mapped_bytes: u64 = 0,
    selected_page_allocations: u64 = 0,
    checkpoint_attempts: u64 = 0,
    checkpoint_all_hit_tokens: u64 = 0,
    checkpoint_miss_tokens: u64 = 0,
    checkpoint_replays: u64 = 0,
    slot_upload_batches: u64 = 0,
    slot_uploads: u64 = 0,
    slot_upload_bytes: u64 = 0,
    mapped_attempts: u64 = 0,
    mapped_fallbacks: u64 = 0,
    mapped_failures: u64 = 0,
    linear_attempts: u64 = 0,
    linear_successes: u64 = 0,
    linear_fallbacks: u64 = 0,
    pair_attempts: u64 = 0,
    pair_successes: u64 = 0,
    pair_fallbacks: u64 = 0,
    scatter_attempts: u64 = 0,
    scatter_successes: u64 = 0,
    scatter_fallbacks: u64 = 0,
    frame_begins: u64 = 0,
    frame_submits: u64 = 0,
    compute_encoders: u64 = 0,
    blit_encoders: u64 = 0,
    bootstrap_misses: u64 = 0,
};

fn serverA4bTelemetrySnapshot(cb: ?*const ops.ComputeBackend) ?ops.NativeQuantTimingStats {
    if (!serverA4bTelemetryEnabled()) return null;
    const backend = cb orelse return null;
    if (backend.kind() != .metal and backend.kind() != .cuda) return null;
    return backend.debugTimingSnapshot().provider;
}

fn serverA4bTelemetryDelta(
    after: ops.NativeQuantTimingStats,
    before: ops.NativeQuantTimingStats,
) ServerA4bTelemetry {
    return .{
        .route_select_attempts = after.a4b_moe_route_select_attempts -| before.a4b_moe_route_select_attempts,
        .route_select_successes = after.a4b_moe_route_select_successes -| before.a4b_moe_route_select_successes,
        .route_select_fallbacks = after.a4b_moe_route_select_fallbacks -| before.a4b_moe_route_select_fallbacks,
        .cuda_resident_source_bytes = after.a4b_cuda_resident_source_bytes,
        .cuda_resident_source_count = after.a4b_cuda_resident_source_count,
        .cuda_route_calls = after.a4b_cuda_route_calls -| before.a4b_cuda_route_calls,
        .cuda_decode_calls = after.a4b_cuda_decode_calls -| before.a4b_cuda_decode_calls,
        .cuda_prefill_calls = after.a4b_cuda_prefill_calls -| before.a4b_cuda_prefill_calls,
        .slot_lookup_attempts = after.a4b_moe_slot_lookup_attempts -| before.a4b_moe_slot_lookup_attempts,
        .slot_route_hits = after.a4b_moe_slot_route_hits -| before.a4b_moe_slot_route_hits,
        .slot_route_misses = after.a4b_moe_slot_route_misses -| before.a4b_moe_slot_route_misses,
        .slot_all_hit_layers = after.a4b_moe_slot_all_hit_layers -| before.a4b_moe_slot_all_hit_layers,
        .slot_map_publications = after.a4b_moe_slot_map_publications -| before.a4b_moe_slot_map_publications,
        .slot_map_publish_failures = after.a4b_moe_slot_map_publish_failures -| before.a4b_moe_slot_map_publish_failures,
        .slot_arena_attempts = after.a4b_moe_slot_arena_attempts -| before.a4b_moe_slot_arena_attempts,
        .slot_arena_successes = after.a4b_moe_slot_arena_successes -| before.a4b_moe_slot_arena_successes,
        .slot_arena_failures = after.a4b_moe_slot_arena_failures -| before.a4b_moe_slot_arena_failures,
        .selected_page_attempts = after.a4b_moe_selected_page_attempts -| before.a4b_moe_selected_page_attempts,
        .selected_page_successes = after.a4b_moe_selected_page_successes -| before.a4b_moe_selected_page_successes,
        .selected_page_failures = after.a4b_moe_selected_page_failures -| before.a4b_moe_selected_page_failures,
        .selected_page_logical_bytes = after.a4b_moe_selected_page_logical_bytes -| before.a4b_moe_selected_page_logical_bytes,
        .selected_page_mapped_bytes = after.a4b_moe_selected_page_mapped_bytes -| before.a4b_moe_selected_page_mapped_bytes,
        .selected_page_allocations = after.a4b_moe_selected_page_allocations -| before.a4b_moe_selected_page_allocations,
        .checkpoint_attempts = after.a4b_moe_checkpoint_attempts -| before.a4b_moe_checkpoint_attempts,
        .checkpoint_all_hit_tokens = after.a4b_moe_checkpoint_all_hit_tokens -| before.a4b_moe_checkpoint_all_hit_tokens,
        .checkpoint_miss_tokens = after.a4b_moe_checkpoint_miss_tokens -| before.a4b_moe_checkpoint_miss_tokens,
        .checkpoint_replays = after.a4b_moe_checkpoint_replays -| before.a4b_moe_checkpoint_replays,
        .slot_upload_batches = after.a4b_moe_slot_upload_batches -| before.a4b_moe_slot_upload_batches,
        .slot_uploads = after.a4b_moe_slot_uploads -| before.a4b_moe_slot_uploads,
        .slot_upload_bytes = after.a4b_moe_slot_upload_bytes -| before.a4b_moe_slot_upload_bytes,
        .mapped_attempts = after.metal_provider_quantized_runtime_mapped_attempts -| before.metal_provider_quantized_runtime_mapped_attempts,
        .mapped_fallbacks = after.metal_provider_quantized_runtime_mapped_fallbacks -| before.metal_provider_quantized_runtime_mapped_fallbacks,
        .mapped_failures = after.metal_provider_quantized_runtime_mapped_failures -| before.metal_provider_quantized_runtime_mapped_failures,
        .linear_attempts = after.a4b_packed_q4_0_linear_attempts -| before.a4b_packed_q4_0_linear_attempts,
        .linear_successes = after.a4b_packed_q4_0_linear_successes -| before.a4b_packed_q4_0_linear_successes,
        .linear_fallbacks = after.a4b_packed_q4_0_linear_fallbacks -| before.a4b_packed_q4_0_linear_fallbacks,
        .pair_attempts = after.a4b_packed_q4_0_pair_attempts -| before.a4b_packed_q4_0_pair_attempts,
        .pair_successes = after.a4b_packed_q4_0_pair_successes -| before.a4b_packed_q4_0_pair_successes,
        .pair_fallbacks = after.a4b_packed_q4_0_pair_fallbacks -| before.a4b_packed_q4_0_pair_fallbacks,
        .scatter_attempts = after.a4b_moe_scatter_attempts -| before.a4b_moe_scatter_attempts,
        .scatter_successes = after.a4b_moe_scatter_successes -| before.a4b_moe_scatter_successes,
        .scatter_fallbacks = after.a4b_moe_scatter_fallbacks -| before.a4b_moe_scatter_fallbacks,
        .frame_begins = after.decoder_runtime_frame_begins -| before.decoder_runtime_frame_begins,
        .frame_submits = after.decoder_runtime_frame_submits -| before.decoder_runtime_frame_submits,
        .compute_encoders = after.metal_runtime_compute_encoder_count -| before.metal_runtime_compute_encoder_count,
        .blit_encoders = after.metal_runtime_blit_encoder_count -| before.metal_runtime_blit_encoder_count,
        .bootstrap_misses = after.compressed_block_active_frame_bootstrap_misses -| before.compressed_block_active_frame_bootstrap_misses,
    };
}

fn logServerA4bTelemetry(
    cb: ?*const ops.ComputeBackend,
    before: ?ops.NativeQuantTimingStats,
    model_name: []const u8,
) void {
    const start = before orelse return;
    const backend = cb orelse return;
    const delta = serverA4bTelemetryDelta(backend.debugTimingSnapshot().provider, start);
    if (backend.kind() == .cuda) {
        std.log.info(
            "cuda_a4b_server_request: scope=process_delta model={s} resident_source_bytes={d} resident_source_count={d} route_calls={d} decode_calls={d} prefill_calls={d} route_fallbacks={d}",
            .{
                model_name,
                delta.cuda_resident_source_bytes,
                delta.cuda_resident_source_count,
                delta.cuda_route_calls,
                delta.cuda_decode_calls,
                delta.cuda_prefill_calls,
                delta.route_select_fallbacks,
            },
        );
        return;
    }
    std.log.info(
        "metal_a4b_server_request: scope=process_delta model={s} route_select_attempts={d} route_select_successes={d} route_select_fallbacks={d} slot_lookup_attempts={d} slot_route_hits={d} slot_route_misses={d} slot_all_hit_layers={d} slot_map_publications={d} slot_map_publish_failures={d} slot_arena_attempts={d} slot_arena_successes={d} slot_arena_failures={d} slot_upload_batches={d} slot_uploads={d} mapped_attempts={d} mapped_fallbacks={d} mapped_failures={d} linear_attempts={d} linear_successes={d} linear_fallbacks={d} pair_attempts={d} pair_successes={d} pair_fallbacks={d} scatter_attempts={d} scatter_successes={d} scatter_fallbacks={d} frame_begins={d} frame_submits={d} compute_encoders={d} blit_encoders={d} bootstrap_misses={d}",
        .{
            model_name,
            delta.route_select_attempts,
            delta.route_select_successes,
            delta.route_select_fallbacks,
            delta.slot_lookup_attempts,
            delta.slot_route_hits,
            delta.slot_route_misses,
            delta.slot_all_hit_layers,
            delta.slot_map_publications,
            delta.slot_map_publish_failures,
            delta.slot_arena_attempts,
            delta.slot_arena_successes,
            delta.slot_arena_failures,
            delta.slot_upload_batches,
            delta.slot_uploads,
            delta.mapped_attempts,
            delta.mapped_fallbacks,
            delta.mapped_failures,
            delta.linear_attempts,
            delta.linear_successes,
            delta.linear_fallbacks,
            delta.pair_attempts,
            delta.pair_successes,
            delta.pair_fallbacks,
            delta.scatter_attempts,
            delta.scatter_successes,
            delta.scatter_fallbacks,
            delta.frame_begins,
            delta.frame_submits,
            delta.compute_encoders,
            delta.blit_encoders,
            delta.bootstrap_misses,
        },
    );
    std.log.info(
        "metal_a4b_server_selected_page: scope=process_delta model={s} attempts={d} successes={d} failures={d} logical_bytes={d} mapped_bytes={d} allocations={d}",
        .{
            model_name,
            delta.selected_page_attempts,
            delta.selected_page_successes,
            delta.selected_page_failures,
            delta.selected_page_logical_bytes,
            delta.selected_page_mapped_bytes,
            delta.selected_page_allocations,
        },
    );
    std.log.info(
        "metal_a4b_server_checkpoint: scope=process_delta model={s} attempts={d} all_hit_tokens={d} miss_tokens={d} replays={d}",
        .{
            model_name,
            delta.checkpoint_attempts,
            delta.checkpoint_all_hit_tokens,
            delta.checkpoint_miss_tokens,
            delta.checkpoint_replays,
        },
    );
}

test "server A4B telemetry reports saturating request deltas" {
    var before = ops.NativeQuantTimingStats{};
    before.a4b_moe_route_select_attempts = 10;
    before.a4b_packed_q4_0_linear_attempts = 90;
    before.decoder_runtime_frame_submits = 20;
    before.metal_provider_quantized_runtime_mapped_fallbacks = 2;

    var after = before;
    after.a4b_moe_route_select_attempts = 130;
    after.a4b_moe_route_select_successes = 120;
    after.a4b_cuda_resident_source_bytes = 14_000_000_000;
    after.a4b_cuda_resident_source_count = 60;
    after.a4b_cuda_route_calls = 120;
    after.a4b_cuda_decode_calls = 90;
    after.a4b_cuda_prefill_calls = 30;
    after.a4b_moe_slot_lookup_attempts = 120;
    after.a4b_moe_slot_route_hits = 880;
    after.a4b_moe_slot_route_misses = 80;
    after.a4b_moe_slot_all_hit_layers = 70;
    after.a4b_moe_slot_map_publications = 120;
    after.a4b_moe_slot_arena_attempts = 120;
    after.a4b_moe_slot_arena_successes = 118;
    after.a4b_moe_slot_arena_failures = 2;
    after.a4b_moe_selected_page_attempts = 120;
    after.a4b_moe_selected_page_successes = 120;
    after.a4b_moe_selected_page_logical_bytes = 3_211_591_680;
    after.a4b_moe_selected_page_mapped_bytes = 3_232_235_520;
    after.a4b_moe_selected_page_allocations = 1_920;
    after.a4b_moe_checkpoint_attempts = 3;
    after.a4b_moe_checkpoint_all_hit_tokens = 2;
    after.a4b_moe_checkpoint_miss_tokens = 1;
    after.a4b_moe_checkpoint_replays = 1;
    after.a4b_moe_slot_upload_batches = 10;
    after.a4b_moe_slot_uploads = 80;
    after.a4b_moe_slot_upload_bytes = 268_697_600;
    after.a4b_packed_q4_0_linear_attempts = 810;
    after.a4b_packed_q4_0_linear_successes = 720;
    after.a4b_packed_q4_0_pair_attempts = 240;
    after.a4b_packed_q4_0_pair_successes = 240;
    after.a4b_moe_scatter_attempts = 240;
    after.a4b_moe_scatter_successes = 240;
    after.decoder_runtime_frame_submits = 477;
    // A runtime counter reset must never wrap to a false giant failure count.
    after.metal_provider_quantized_runtime_mapped_fallbacks = 0;

    const delta = serverA4bTelemetryDelta(after, before);
    try std.testing.expectEqual(@as(u64, 120), delta.route_select_attempts);
    try std.testing.expectEqual(@as(u64, 120), delta.route_select_successes);
    try std.testing.expectEqual(@as(u64, 14_000_000_000), delta.cuda_resident_source_bytes);
    try std.testing.expectEqual(@as(u64, 60), delta.cuda_resident_source_count);
    try std.testing.expectEqual(@as(u64, 120), delta.cuda_route_calls);
    try std.testing.expectEqual(@as(u64, 90), delta.cuda_decode_calls);
    try std.testing.expectEqual(@as(u64, 30), delta.cuda_prefill_calls);
    try std.testing.expectEqual(@as(u64, 120), delta.slot_lookup_attempts);
    try std.testing.expectEqual(@as(u64, 880), delta.slot_route_hits);
    try std.testing.expectEqual(@as(u64, 80), delta.slot_route_misses);
    try std.testing.expectEqual(@as(u64, 70), delta.slot_all_hit_layers);
    try std.testing.expectEqual(@as(u64, 120), delta.slot_map_publications);
    try std.testing.expectEqual(@as(u64, 120), delta.slot_arena_attempts);
    try std.testing.expectEqual(@as(u64, 118), delta.slot_arena_successes);
    try std.testing.expectEqual(@as(u64, 2), delta.slot_arena_failures);
    try std.testing.expectEqual(@as(u64, 120), delta.selected_page_attempts);
    try std.testing.expectEqual(@as(u64, 120), delta.selected_page_successes);
    try std.testing.expectEqual(@as(u64, 0), delta.selected_page_failures);
    try std.testing.expectEqual(@as(u64, 3_211_591_680), delta.selected_page_logical_bytes);
    try std.testing.expectEqual(@as(u64, 3_232_235_520), delta.selected_page_mapped_bytes);
    try std.testing.expectEqual(@as(u64, 1_920), delta.selected_page_allocations);
    try std.testing.expectEqual(@as(u64, 3), delta.checkpoint_attempts);
    try std.testing.expectEqual(@as(u64, 2), delta.checkpoint_all_hit_tokens);
    try std.testing.expectEqual(@as(u64, 1), delta.checkpoint_miss_tokens);
    try std.testing.expectEqual(@as(u64, 1), delta.checkpoint_replays);
    try std.testing.expectEqual(@as(u64, 10), delta.slot_upload_batches);
    try std.testing.expectEqual(@as(u64, 80), delta.slot_uploads);
    try std.testing.expectEqual(@as(u64, 268_697_600), delta.slot_upload_bytes);
    try std.testing.expectEqual(@as(u64, 720), delta.linear_attempts);
    try std.testing.expectEqual(@as(u64, 720), delta.linear_successes);
    try std.testing.expectEqual(@as(u64, 240), delta.pair_attempts);
    try std.testing.expectEqual(@as(u64, 240), delta.scatter_successes);
    try std.testing.expectEqual(@as(u64, 457), delta.frame_submits);
    try std.testing.expectEqual(@as(u64, 0), delta.mapped_fallbacks);
}

fn generationStreamShouldContinue(
    write_failed: bool,
    cancellation: ?*const std.atomic.Value(bool),
) bool {
    if (write_failed) return false;
    if (cancellation) |signal| return !signal.load(.acquire);
    return true;
}

fn generationStreamWriteIsPeerDisconnect(err: anyerror) bool {
    return switch (err) {
        error.Canceled,
        error.Cancelled,
        error.EndOfStream,
        error.StreamClosed,
        error.ConnectionResetByPeer,
        error.BrokenPipe,
        error.SendFailed,
        error.Timeout,
        => true,
        else => false,
    };
}

test "generation pipeline session lookup is field safe" {
    var pipeline = struct {}{};
    try std.testing.expect(generationPipelineSession(&pipeline) == null);
}

test "generation stream continuation observes writes and peer cancellation" {
    var cancellation = std.atomic.Value(bool).init(false);
    try std.testing.expect(generationStreamShouldContinue(false, &cancellation));
    cancellation.store(true, .release);
    try std.testing.expect(!generationStreamShouldContinue(false, &cancellation));
    try std.testing.expect(!generationStreamShouldContinue(true, null));
}

test "generation stream write classification separates peer disconnects from server faults" {
    try std.testing.expect(generationStreamWriteIsPeerDisconnect(error.SendFailed));
    try std.testing.expect(generationStreamWriteIsPeerDisconnect(error.StreamClosed));
    try std.testing.expect(generationStreamWriteIsPeerDisconnect(error.Canceled));
    try std.testing.expect(!generationStreamWriteIsPeerDisconnect(error.OutOfMemory));
    try std.testing.expect(!generationStreamWriteIsPeerDisconnect(error.EventTooLarge));
}

test "channel-aware Gemma generation stays on projected native backends" {
    try std.testing.expect(!requiresNativeChannelProjection(.{}));
    try std.testing.expect(!requiresNativeChannelProjection(.{ .family = .gemma }));
    try std.testing.expect(requiresNativeChannelProjection(.{
        .family = .gemma,
        .ple_hidden_size = 256,
    }));
    try std.testing.expect(requiresNativeChannelProjection(.{
        .family = .gemma,
        .gemma4_channel_protocol = true,
    }));
}

test "generate API serializes public content and reasoning separately" {
    const rendered = try std.json.Stringify.valueAlloc(std.testing.allocator, api.GenerateMessage{
        .role = .assistant,
        .content = .{ .value = "Paris" },
        .reasoning_content = .{ .value = "The capital of France is Paris." },
    }, .{ .emit_null_optional_fields = false });
    defer std.testing.allocator.free(rendered);

    try std.testing.expect(std.mem.indexOf(u8, rendered, "\"content\":\"Paris\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, rendered, "\"reasoning_content\":\"The capital of France is Paris.\"") != null);
}

const GenerateSpeculationOptions = struct {
    k: u32,
    policy: generation.SpeculationPolicy,
    calibration: generation.SpeculationCalibration,
};

const GenerateNumericOptions = struct {
    max_tokens: i32,
    top_k: i32,
};

const GenerateSamplingOptions = struct {
    temperature: f32,
    top_p: f32,
    min_p: f32,
    repetition_penalty: f32,
    frequency_penalty: f32,
    presence_penalty: f32,
};

const GenerateSamplingError = error{
    InvalidTemperature,
    InvalidTopP,
    InvalidMinP,
    InvalidRepetitionPenalty,
    InvalidFrequencyPenalty,
    InvalidPresencePenalty,
};

fn parseGenerateNumericOptions(max_tokens_raw: ?i64, top_k_raw: ?i64) !GenerateNumericOptions {
    const max_tokens = std.math.cast(i32, max_tokens_raw orelse 256) orelse return error.InvalidMaxTokens;
    if (max_tokens < 1) return error.InvalidMaxTokens;
    const top_k = std.math.cast(i32, top_k_raw orelse 0) orelse return error.InvalidTopK;
    if (top_k < 0) return error.InvalidTopK;
    return .{ .max_tokens = max_tokens, .top_k = top_k };
}

fn parseGenerateSamplingOptions(
    temperature_raw: ?f32,
    top_p_raw: ?f32,
    min_p_raw: ?f32,
    repetition_penalty_raw: ?f32,
    frequency_penalty_raw: ?f32,
    presence_penalty_raw: ?f32,
) GenerateSamplingError!GenerateSamplingOptions {
    const temperature = temperature_raw orelse 0;
    if (!std.math.isFinite(temperature) or temperature < 0 or temperature > 2) return error.InvalidTemperature;
    const top_p = top_p_raw orelse 0;
    if (!std.math.isFinite(top_p) or top_p < 0 or top_p > 1) return error.InvalidTopP;
    const min_p = min_p_raw orelse 0;
    if (!std.math.isFinite(min_p) or min_p < 0 or min_p > 1) return error.InvalidMinP;
    const repetition_penalty = repetition_penalty_raw orelse 1;
    if (!std.math.isFinite(repetition_penalty) or repetition_penalty <= 0) return error.InvalidRepetitionPenalty;
    const frequency_penalty = frequency_penalty_raw orelse 0;
    if (!std.math.isFinite(frequency_penalty) or frequency_penalty < -2 or frequency_penalty > 2) return error.InvalidFrequencyPenalty;
    const presence_penalty = presence_penalty_raw orelse 0;
    if (!std.math.isFinite(presence_penalty) or presence_penalty < -2 or presence_penalty > 2) return error.InvalidPresencePenalty;
    return .{
        .temperature = temperature,
        .top_p = top_p,
        .min_p = min_p,
        .repetition_penalty = repetition_penalty,
        .frequency_penalty = frequency_penalty,
        .presence_penalty = presence_penalty,
    };
}

fn generateSamplingErrorMessage(err: GenerateSamplingError) []const u8 {
    return switch (err) {
        error.InvalidTemperature => "temperature must be finite and between 0 and 2",
        error.InvalidTopP => "top_p must be finite and between 0 and 1",
        error.InvalidMinP => "min_p must be finite and between 0 and 1",
        error.InvalidRepetitionPenalty => "repetition_penalty must be finite and greater than 0",
        error.InvalidFrequencyPenalty => "frequency_penalty must be finite and between -2 and 2",
        error.InvalidPresencePenalty => "presence_penalty must be finite and between -2 and 2",
    };
}

fn parseGenerateSpeculationOptions(
    draft_requested: bool,
    speculative_k: ?i64,
    policy_raw: ?[]const u8,
    calibration_raw: ?[]const u8,
) !GenerateSpeculationOptions {
    if (!draft_requested and (speculative_k != null or policy_raw != null or calibration_raw != null)) {
        return error.SpeculationRequiresDraftModel;
    }
    const k = speculative_k orelse 4;
    if (k < 1 or k > 16) return error.InvalidSpeculativeK;
    const policy: generation.SpeculationPolicy = if (policy_raw) |raw|
        generation.parseSpeculationPolicy(raw) orelse return error.InvalidSpeculationPolicy
    else
        .auto;
    const calibration: generation.SpeculationCalibration = if (calibration_raw) |raw| blk: {
        if (!std.mem.eql(u8, raw, "none") and
            !std.mem.eql(u8, raw, "probe") and
            !std.mem.eql(u8, raw, "positive"))
        {
            return error.InvalidSpeculationCalibration;
        }
        break :blk generation.parseSpeculationCalibration(raw).?;
    } else .none;
    return .{
        .k = @intCast(k),
        .policy = policy,
        .calibration = calibration,
    };
}

fn generateSpeculationErrorMessage(err: anyerror) []const u8 {
    return switch (err) {
        error.InvalidSpeculativeK => "speculative_k must be between 1 and 16",
        error.InvalidSpeculationPolicy => "speculation_policy must be auto, force, or off",
        error.InvalidSpeculationCalibration => "speculation_calibration must be none, probe, or positive",
        error.SpeculationRequiresDraftModel => "speculative_k, speculation_policy, and speculation_calibration require draft_model",
        else => "speculation options are invalid",
    };
}

fn generateSpeculationStatus(stats: ?generation.SpeculativeDecodeStats) ?api.GenerateSpeculationStatus {
    const value = stats orelse return null;
    const disabled_reason = value.mtp_disabled_reason orelse switch (value.speculation_policy_decision) {
        .disabled_off => "speculation_policy_off",
        .disabled_unavailable => "draft_backend_unavailable",
        .disabled_uncalibrated => "speculation_calibration_required",
        .disabled_low_acceptance => "mtp_auto_low_acceptance",
        .disabled_zero_match => "mtp_auto_zero_match",
        .disabled_slow => "mtp_auto_cost_probe_slow",
        .disabled_insufficient_probe => "mtp_auto_insufficient_cost_probe",
        .inactive, .active, .forced => null,
    };
    return .{
        .policy = value.speculation_policy.name(),
        .calibration = value.speculation_calibration.name(),
        .decision = value.speculation_policy_decision.name(),
        .disabled_reason = if (disabled_reason) |reason| .{ .value = reason } else .absent,
    };
}

fn shouldResolveDraftModel(policy: generation.SpeculationPolicy) bool {
    return policy != .off;
}

fn effectiveDraftModelName(requested: ?[]const u8, policy: generation.SpeculationPolicy) ?[]const u8 {
    return if (shouldResolveDraftModel(policy)) requested else null;
}

fn generationSpeculationRequested(effective_draft: ?[]const u8, auto_discovered_draft: ?[]const u8) bool {
    return effective_draft != null or auto_discovered_draft != null;
}

fn qualifyAutoDiscoveredDraftSpeculation(options: *GenerateSpeculationOptions) void {
    // A client cannot name calibration controls without an explicit drafter at
    // the HTTP trust boundary. Once the server discovers its own best-effort
    // MTP companion, AUTO therefore has to opt into the bounded cost probe or
    // the drafter is loaded and admitted only to be disabled as uncalibrated.
    if (options.policy == .auto and options.calibration == .none) {
        options.calibration = .probe;
    }
}

/// Opt-in: when a request carries no draft_model and the speculation policy is
/// auto, discover the Gemma4 MTP assistant artifact sitting next to the target
/// model dir. Kept fail-closed until Metal auto-MTP qualifies as a default.
fn gemma4MtpAutoDraftDiscoveryEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_GEMMA4_MTP_AUTO_DRAFT_DISCOVERY", false);
}

/// Sibling MTP assistant dir for a Gemma4 QAT gguf artifact
/// (`...-q4_0-gguf` -> `...-q4_0-unquantized-assistant`), or null when the
/// model dir has no such sibling on disk. Caller owns the returned path.
/// The name qualification mirrors registry.gemma4MtpAssistantCompanionRefAlloc
/// so discovery only ever matches artifacts the companion pull can create.
fn gemma4MtpAssistantSiblingPath(
    allocator: std.mem.Allocator,
    io: std.Io,
    model_path: []const u8,
) ?[]const u8 {
    const suffix = "-gguf";
    if (!std.mem.endsWith(u8, model_path, suffix)) return null;
    const base_name = std.fs.path.basename(model_path);
    if (std.ascii.indexOfIgnoreCase(base_name, "gemma-4-") == null) return null;
    if (std.ascii.indexOfIgnoreCase(base_name, "-qat-") == null) return null;
    const sibling = std.fmt.allocPrint(
        allocator,
        "{s}-unquantized-assistant",
        .{model_path[0 .. model_path.len - suffix.len]},
    ) catch return null;
    _ = std.Io.Dir.cwd().statFile(io, sibling, .{}) catch {
        allocator.free(sibling);
        return null;
    };
    return sibling;
}

/// A discovered drafter must parse as a model before it may join the request:
/// stale or partial sibling directories degrade to non-speculative generation
/// rather than failing generate calls the client never asked to speculate.
fn validateDiscoveredDraftDir(allocator: std.mem.Allocator, dir: []const u8) bool {
    var draft_manifest = manifest_mod.loadFromDir(allocator, dir) catch return false;
    defer draft_manifest.deinit();
    _ = session_factory.loadGptConfigFromModelDir(allocator, dir, draft_manifest) catch return false;
    return true;
}

fn validateQualifiedProfileDraft(
    qualified_profile_requested: bool,
    effective_draft_model: ?[]const u8,
) !void {
    if (qualified_profile_requested and effective_draft_model != null) {
        return error.KernelJitQualifiedProfileDraftUnsupported;
    }
}

fn generateAdmissionUnitsForSpeculation(
    base_units: usize,
    draft_requested: bool,
    policy: generation.SpeculationPolicy,
) usize {
    if (!draft_requested or !shouldResolveDraftModel(policy)) return base_units;
    return std.math.add(usize, base_units, base_units) catch std.math.maxInt(usize);
}

/// Remove only the admission units added for an auto-discovered drafter.
/// `reserved_units` may have grown independently after media decoding, so
/// restoring an older base estimate would silently undercharge the request.
fn takeAutoDraftAdmissionExtra(reserved_units: *usize, auto_draft_extra_units: *usize) usize {
    const released = @min(reserved_units.*, auto_draft_extra_units.*);
    reserved_units.* -= released;
    auto_draft_extra_units.* = 0;
    return released;
}

fn generationBackendKind(backend: backends_mod.BackendType) ?runtime.kv.pool.BackendKind {
    return switch (backend) {
        .native => .native,
        .metal => .metal,
        .cuda => .cuda,
        .onnx, .pjrt, .wasm => null,
    };
}

fn promptCacheBackendEligible(mode: runtime.kv.prompt_cache.Mode, backend: runtime.kv.pool.BackendKind) bool {
    return switch (backend) {
        .native, .metal => true,
        // CUDA still owns device KV by transient sequence id. Keep the new
        // radix mode fail-closed there until its page pool has block lifetime.
        .cuda => mode != .radix,
    };
}

test "radix prompt cache is opt in and excludes CUDA device storage" {
    try std.testing.expect(promptCacheBackendEligible(.radix, .metal));
    try std.testing.expect(promptCacheBackendEligible(.radix, .native));
    try std.testing.expect(!promptCacheBackendEligible(.radix, .cuda));
    try std.testing.expect(promptCacheBackendEligible(.block_hash, .cuda));
}

fn generationKvSlidingTrimForced() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_KV_SLIDING_TRIM", false);
}

fn validateCacheCompactionRatio(ratio: ?f32) !void {
    if (ratio) |value| try (runtime.kv.compaction.CompactionConfig{ .target_ratio = value }).validate();
}

fn effectiveSpeculationStats(
    stats: ?generation.SpeculativeDecodeStats,
    requested: bool,
    policy: generation.SpeculationPolicy,
    calibration: generation.SpeculationCalibration,
) ?generation.SpeculativeDecodeStats {
    if (stats != null) return stats;
    if (!requested or policy != .off) return null;
    return .{
        .speculation_policy = policy,
        .speculation_calibration = calibration,
        .speculation_policy_decision = .disabled_off,
        .mtp_disabled_reason = "speculation_policy_off",
    };
}

fn recordSpeculationOutcome(metrics: *metrics_mod.Metrics, stats: ?generation.SpeculativeDecodeStats) void {
    const value = stats orelse return;
    switch (value.speculation_policy_decision) {
        .active, .forced => metrics.incSpeculationActive(),
        .inactive,
        .disabled_off,
        .disabled_unavailable,
        .disabled_uncalibrated,
        .disabled_low_acceptance,
        .disabled_zero_match,
        .disabled_slow,
        .disabled_insufficient_probe,
        => metrics.incSpeculationDisabled(),
    }
}

pub const BudgetOverrides = struct {
    host_limit_bytes: usize = 0,
    backend_limit_bytes: usize = 0,
    combined_limit_bytes: usize = 0,
    kv_limit_bytes: usize = 0,
    scratch_limit_bytes: usize = 0,

    pub fn apply(self: @This(), defaults: runtime.tier.memory.Limits) runtime.tier.memory.Limits {
        return runtime.tier.memory.applyLimitOverrides(defaults, .{
            .host_limit_bytes = self.host_limit_bytes,
            .backend_limit_bytes = self.backend_limit_bytes,
            .combined_limit_bytes = self.combined_limit_bytes,
            .kv_limit_bytes = self.kv_limit_bytes,
            .scratch_limit_bytes = self.scratch_limit_bytes,
        });
    }
};

pub const PromptCacheConfig = struct {
    enabled: bool = false,
    mode: runtime.kv.prompt_cache.Mode = .block_hash,
    max_bytes_mb: usize = 512,
    min_tokens: usize = 64,
    ttl_ms: u64 = 300_000,

    pub fn validate(self: @This()) !void {
        if (self.max_bytes_mb > runtime.kv.prompt_cache.max_config_bytes_mb or
            self.ttl_ms > runtime.kv.prompt_cache.max_config_ttl_ms)
        {
            return error.InvalidPromptCacheConfig;
        }
    }

    /// max_bytes_mb is the node-wide target for the single active model cache.
    pub fn runtimeConfig(
        self: @This(),
        resource_usage_observer: ?runtime.kv.prompt_cache.ResourceUsageObserver,
    ) runtime.kv.prompt_cache.Config {
        return .{
            .enabled = self.enabled,
            .mode = self.mode,
            // Node.init rejects overflow; saturation also keeps this boundary
            // safe if an embedding mutates the public config after init.
            .max_bytes = std.math.mul(usize, self.max_bytes_mb, runtime.kv.prompt_cache.bytes_per_mebibyte) catch std.math.maxInt(usize),
            .min_tokens = self.min_tokens,
            .ttl_ms = self.ttl_ms,
            .resource_usage_observer = resource_usage_observer,
        };
    }
};

test "extraction resolution distinguishes absence resource pressure and internal failure" {
    try std.testing.expectEqual(
        ExtractionResolutionFailure.not_found,
        classifyExtractionResolutionFailure(error.ModelNotFound),
    );
    try std.testing.expectEqual(
        ExtractionResolutionFailure.resource_exhausted,
        classifyExtractionResolutionFailure(error.OutOfMemory),
    );
    try std.testing.expectEqual(
        ExtractionResolutionFailure.internal,
        classifyExtractionResolutionFailure(error.Unexpected),
    );
}

test "prompt cache config reports node target in bytes" {
    const cfg = PromptCacheConfig{ .enabled = true, .max_bytes_mb = 512 };
    try std.testing.expectEqual(@as(usize, 512 * 1024 * 1024), cfg.runtimeConfig(null).max_bytes);
}

test "prompt cache config rejects unrepresentable runtime values" {
    try std.testing.expectError(error.InvalidPromptCacheConfig, (PromptCacheConfig{
        .max_bytes_mb = runtime.kv.prompt_cache.max_config_bytes_mb + 1,
    }).validate());
    try std.testing.expectError(error.InvalidPromptCacheConfig, (PromptCacheConfig{
        .ttl_ms = runtime.kv.prompt_cache.max_config_ttl_ms + 1,
    }).validate());
}

test "native prompt cache seeds a second request from cache-owned storage" {
    const allocator = std.testing.allocator;
    var cache = runtime.kv.prompt_cache.PromptPrefixCache.init(allocator);
    defer cache.deinit();
    cache.configure(.{
        .enabled = true,
        .mode = .block_hash,
        .min_tokens = 2,
        .max_bytes = 1 << 20,
    });

    const pool_config = runtime.kv.pool.KvPoolConfig{
        .backend = .native,
        .dtype = .f32,
        .page_size_tokens = 2,
        .num_layers_packed = 1,
        .num_kv_heads = 1,
        .head_dim = 2,
    };
    const storage = (try cache.ensureStorage(pool_config)).?.storage;
    const pool_id = cache.pool_id.?;

    const source_id = try cache.manager.attachSequence(pool_id);
    try cache.manager.appendTokens(source_id, 4);
    const storage_source_id = try storage.attachSequence(storage.poolId());
    try std.testing.expectEqual(source_id, storage_source_id);
    try storage.appendTokens(storage_source_id, 4);
    try cache.storeFromSequence("native", &.{ 1, 2, 3, 4 }, source_id);
    try cache.manager.releaseSequence(source_id);
    try storage.releaseSequence(storage_source_id);

    const hit = (try cache.attachLongestPrefix("native", &.{ 1, 2, 3, 9 }, 2)).?;
    var decode_state = generation.NativeDecodeState.initPaged(allocator, cache.managerPtr(), pool_id, null);
    decode_state.kv_storage = storage;
    defer decode_state.deinit();
    try decode_state.seedAttachedPrefix(hit.sequence_id, hit.token_count);

    try std.testing.expectEqual(@as(usize, 2), hit.token_count);
    try std.testing.expectEqual(@as(?usize, 2), cache.manager.tokenCount(hit.sequence_id));
    try std.testing.expectEqual(@as(?usize, 2), storage.tokenCount(hit.sequence_id));
}

test "concurrent first prompt cache activations share the node budget" {
    const allocator = std.testing.allocator;
    var manager = model_manager_mod.ModelManager.init(allocator, backends_mod.SessionManager.init(allocator));
    defer manager.loaded.deinit(allocator);
    defer manager.loaded_aliases.deinit(allocator);

    var first: model_manager_mod.LoadedModel = undefined;
    first.prompt_prefix_cache = runtime.kv.prompt_cache.PromptPrefixCache.init(allocator);
    defer first.prompt_prefix_cache.deinit();
    var second: model_manager_mod.LoadedModel = undefined;
    second.prompt_prefix_cache = runtime.kv.prompt_cache.PromptPrefixCache.init(allocator);
    defer second.prompt_prefix_cache.deinit();
    try manager.loaded.put(allocator, "first", &first);
    try manager.loaded.put(allocator, "second", &second);

    const node_config = runtime.kv.prompt_cache.Config{
        .enabled = true,
        .max_bytes = 1024,
    };
    // Reproduce the production interleaving: both handlers rebalance before
    // either has reached ensurePool().
    manager.rebalancePromptCaches(&first, node_config);
    manager.rebalancePromptCaches(&second, node_config);

    // Rebalancing a foreign cache must not synchronously evict from its pool;
    // its owning generation path applies the queued budget on the next request.
    try std.testing.expectEqual(@as(usize, 512), first.prompt_prefix_cache.config.max_bytes);
    try std.testing.expectEqual(@as(usize, 512), first.prompt_prefix_cache.pending_config.?.max_bytes);
    try std.testing.expectEqual(@as(usize, 512), second.prompt_prefix_cache.config.max_bytes);
    try std.testing.expect(first.prompt_prefix_cache.isParticipating());
    try std.testing.expect(second.prompt_prefix_cache.isParticipating());

    manager.cancelPromptCacheActivation(&second, node_config);
    try std.testing.expectEqual(@as(usize, 512), first.prompt_prefix_cache.config.max_bytes);
    try std.testing.expectEqual(@as(usize, 1024), first.prompt_prefix_cache.pending_config.?.max_bytes);
    try std.testing.expect(!second.prompt_prefix_cache.isParticipating());
}

test "deferred foreign prompt cache eviction does not collapse active share" {
    const allocator = std.testing.allocator;
    var manager = model_manager_mod.ModelManager.init(allocator, backends_mod.SessionManager.init(allocator));
    defer manager.loaded.deinit(allocator);
    defer manager.loaded_aliases.deinit(allocator);

    var idle: model_manager_mod.LoadedModel = undefined;
    idle.prompt_prefix_cache = runtime.kv.prompt_cache.PromptPrefixCache.init(allocator);
    defer idle.prompt_prefix_cache.deinit();
    var active: model_manager_mod.LoadedModel = undefined;
    active.prompt_prefix_cache = runtime.kv.prompt_cache.PromptPrefixCache.init(allocator);
    defer active.prompt_prefix_cache.deinit();
    try manager.loaded.put(allocator, "idle", &idle);
    try manager.loaded.put(allocator, "active", &active);

    const node_config = runtime.kv.prompt_cache.Config{
        .enabled = true,
        .mode = .simple,
        .min_tokens = 2,
        .max_bytes = 1 << 20,
    };
    manager.rebalancePromptCaches(&idle, node_config);
    const pool_id = (try idle.prompt_prefix_cache.ensurePool(.{
        .backend = .native,
        .dtype = .f32,
        .page_size_tokens = 2,
        .num_layers_packed = 1,
        .num_kv_heads = 1,
        .head_dim = 64,
    })).?;
    const sequence_id = try idle.prompt_prefix_cache.managerPtr().attachSequence(pool_id);
    defer idle.prompt_prefix_cache.managerPtr().releaseSequence(sequence_id) catch {};
    try idle.prompt_prefix_cache.managerPtr().appendTokens(sequence_id, 1024);
    const tokens = try allocator.alloc(i64, 1024);
    defer allocator.free(tokens);
    for (tokens, 0..) |*token, idx| token.* = @intCast(idx);
    try idle.prompt_prefix_cache.storeFromSequence("idle", tokens, sequence_id);
    try std.testing.expect(idle.prompt_prefix_cache.stats().live_bytes > node_config.max_bytes / 2);

    manager.rebalancePromptCaches(&active, node_config);
    try std.testing.expectEqual(
        node_config.max_bytes / 2,
        active.prompt_prefix_cache.config.max_bytes,
    );
    try std.testing.expectEqual(
        node_config.max_bytes / 2,
        idle.prompt_prefix_cache.pending_config.?.max_bytes,
    );
}

fn chatTemplateKwargsValueIsValid(value: std.json.Value) bool {
    const object = switch (value) {
        .object => |object| object,
        else => return false,
    };
    var iterator = object.iterator();
    while (iterator.next()) |entry| {
        if (!std.mem.eql(u8, entry.key_ptr.*, "enable_thinking")) return false;
        switch (entry.value_ptr.*) {
            .bool => {},
            else => return false,
        }
    }
    return true;
}

fn generateRequestChatTemplateKwargsAreValid(value: std.json.Value) bool {
    const object = switch (value) {
        .object => |object| object,
        else => return true,
    };
    const kwargs = object.get("chat_template_kwargs") orelse return true;
    return chatTemplateKwargsValueIsValid(kwargs);
}

fn rawGenerateChatTemplateKwargsAreValid(
    allocator: std.mem.Allocator,
    raw: []const u8,
    batch: bool,
) bool {
    // Cheap pre-gate: the field cannot be present when its name never appears in
    // the raw body, so skip the full Value parse (substring false positives just
    // fall through to the full validation below).
    if (std.mem.indexOf(u8, raw, "chat_template_kwargs") == null) return true;
    var parsed = std.json.parseFromSlice(std.json.Value, allocator, raw, .{}) catch return true;
    defer parsed.deinit();
    if (!batch) return generateRequestChatTemplateKwargsAreValid(parsed.value);

    const root = switch (parsed.value) {
        .object => |object| object,
        else => return true,
    };
    const requests_value = root.get("requests") orelse return true;
    const requests = switch (requests_value) {
        .array => |array| array,
        else => return true,
    };
    for (requests.items) |item| {
        const item_object = switch (item) {
            .object => |object| object,
            else => continue,
        };
        const request_body = item_object.get("body") orelse continue;
        if (!generateRequestChatTemplateKwargsAreValid(request_body)) return false;
    }
    return true;
}

pub const NodeConfig = struct {
    models_dir: []const u8 = "./models",
    ml_dir: []const u8 = "./ml",
    content_security: ?scraping.ContentSecurityConfig = null,
    s3_credentials: ?scraping.S3CredentialsConfig = null,
    preload: []const WarmModel = &.{},
    keep_alive_ms: u64 = 300_000,
    max_loaded_models: usize = 10,
    max_concurrent_requests: usize = 32,
    /// Transport concurrency is bounded independently from inference work.
    /// Keep enough connection headroom for health, discovery, and idle
    /// keep-alive clients without allowing retained Threaded workers to grow
    /// with an unbounded connection population.
    http_max_connections: u32 = 64,
    /// Protocol-neutral application request capacity. Handlers run separately
    /// from connection lifetimes; in particular, HTTP/2 frame pumps remain
    /// available to receive streaming request bodies.
    http_max_request_tasks: u32 = 64,
    pool_size: usize = 2,
    /// Optional hard envelope for all memory charged to this process/container.
    /// This is required when the runtime has no finite cgroup memory.max and
    /// should normally match the orchestrator's process memory allocation.
    process_memory_limit_bytes: usize = 0,
    /// Where the finite byte value came from. Automatic is the fail-safe
    /// default: callers must opt into the less conservative explicit-envelope
    /// live reserve, while composing runtimes preserve their exact probe source.
    process_memory_limit_provenance: runtime.tier.memory.ProcessMemoryLimitProvenance = .automatic,
    resource_ownership: model_manager_mod.ResourceOwnership = .local,
    generation_budget_overrides: BudgetOverrides = .{},
    generation_batching: GenerationBatchingConfig = .{},
    kernel_jit: graph_mod.kernel_jit.Config = .{},
    prompt_cache: PromptCacheConfig = .{},
    prompt_cache_resource_usage_observer: ?runtime.kv.prompt_cache.ResourceUsageObserver = null,
    tokenizer_cache: hf_tokenizer_mod.HfTokenizer.BpeCacheConfig = .{},
    tokenizer_parallel_bpe: hf_tokenizer_mod.HfTokenizer.ParallelBpeConfig = .{},
    allow_insecure_public_bind: bool = false,
    /// Permit artifacts whose compatibility cannot be proven by this build.
    /// Known incompatible or unsafe artifacts remain blocked.
    allow_unknown_models: bool = false,
};

fn isLoopbackBindHost(host: []const u8) bool {
    const address = std.Io.net.IpAddress.parse(host, 0) catch return false;
    return switch (address) {
        .ip4 => |ip4| ip4.bytes[0] == 127,
        .ip6 => |ip6| blk: {
            const loopback = std.Io.net.Ip6Address.loopback(0);
            break :blk std.mem.eql(u8, &ip6.bytes, &loopback.bytes);
        },
    };
}

fn validateStandaloneBind(host: []const u8, allow_insecure_public_bind: bool) !void {
    if (!allow_insecure_public_bind and !isLoopbackBindHost(host)) return error.InsecurePublicBind;
}

test "standalone inference bind is loopback-only unless explicitly overridden" {
    for ([_][]const u8{ "127.0.0.1", "127.42.0.9", "::1" }) |host| {
        try validateStandaloneBind(host, false);
    }
    for ([_][]const u8{ "0.0.0.0", "::", "192.168.1.10", "localhost", "inference.internal" }) |host| {
        try std.testing.expectError(error.InsecurePublicBind, validateStandaloneBind(host, false));
    }
    try validateStandaloneBind("0.0.0.0", true);
}

pub const GenerationBatchingMode = enum {
    off,
    auto,
    on,
};

fn cudaDecodeGraphReplayRequested(raw_opt: ?[]const u8) bool {
    const raw = raw_opt orelse return false;
    return !(std.mem.eql(u8, raw, "off") or
        std.mem.eql(u8, raw, "0") or
        std.mem.eql(u8, raw, "false") or
        std.mem.eql(u8, raw, "no"));
}

pub const GenerationBatchingConfig = struct {
    // Experimental CUDA batching: prefill remains singleton and decode batches
    // homogeneous sequence positions within the bounded row-2 envelope. Keep
    // the default and `auto` serialized until the promotion gate passes;
    // `mode: on` is an explicit opt-in for validation and benchmarking.
    mode: GenerationBatchingMode = .off,
    max_step_items: usize = 2,
    max_step_query_tokens: usize = 512,
    max_decode_wait_us: u32 = 1_000,
    max_idle_prefill_chunk_size: usize = 2048,

    pub fn enabledForBackend(self: @This(), backend: backends_mod.BackendType) bool {
        if (backend != .cuda) return false;
        if (platform.env.getenvBool("ANTFLY_INFERENCE_DISABLE_CONTINUOUS_BATCHING")) return false;
        // Request-scoped CUDA graph state is shared by the model backend. If a
        // deployment enables graph reset manually, preserve the model-wide
        // lock even when the server config requests batching.
        if (platform.env.getenvBool("ANTFLY_INFERENCE_CUDA_SERVER_REQUEST_GRAPH_RESET")) return false;
        if (cudaDecodeGraphReplayRequested(platform.env.getenv("ANTFLY_INFERENCE_CUDA_DECODE_GRAPH_REPLAY"))) return false;
        return self.mode == .on;
    }

    pub fn enabledForRequest(
        self: @This(),
        backend: backends_mod.BackendType,
        graph_mode: bool,
        speculation_requested: bool,
        has_multimodal_input: bool,
    ) bool {
        if (!self.enabledForBackend(backend)) return false;
        // These paths issue CUDA work outside the row scheduler. They retain
        // the model-wide generation lock until their execution is scheduler-
        // aware, while ordinary decode requests remain batchable.
        return !graph_mode and !speculation_requested and !has_multimodal_input;
    }

    pub fn schedulerPolicy(self: @This()) runtime.scheduler.native_generate.Policy {
        const validated_max_items: usize = 2;
        return .{
            .max_step_items = @min(@max(self.max_step_items, 1), validated_max_items),
            .max_step_query_tokens = @max(self.max_step_query_tokens, 1),
            .max_decode_wait_us = if (self.mode == .on) self.max_decode_wait_us else 0,
            .max_idle_prefill_chunk_size = @min(@max(self.max_idle_prefill_chunk_size, 32), 2048),
            // Wider active sets currently lose response equivalence even when
            // individual steps are capped to two rows. Preserve the proven
            // direct path until the multi-wave row scheduler is validated.
            .max_active_requests_for_batching = validated_max_items,
        };
    }
};

fn logAdmittedPrefillChunkPlan(plan: runtime.scheduler.native_generate.PrefillChunkPlan) void {
    const tail = plan.tail();
    std.log.info(
        "prefill_chunk_plan: status=admitted fingerprint={x} selection={s} fairness={s} prompt_tokens={d} chunks={d} max_chunk_rows={d} max_scratch_bytes={d} forced_drain_budget_bytes={d} tail_rows={d} tail_weight_route={s}",
        .{
            plan.fingerprint,
            @tagName(plan.selection),
            @tagName(plan.fairness),
            plan.prompt_tokens,
            plan.chunks.len,
            plan.max_chunk_rows,
            plan.max_scratch_bytes,
            plan.forced_drain_budget_bytes,
            tail.rows,
            @tagName(tail.weight_route_hint),
        },
    );
}

fn promptCacheEligibleForNativeRequest(
    enabled: bool,
    continuous_batching: bool,
    backend: runtime.kv.pool.BackendKind,
    has_compiled_partition: bool,
    has_draft: bool,
    has_compaction: bool,
) bool {
    return enabled and !continuous_batching and
        (backend == .native or backend == .metal or backend == .cuda) and
        !has_compiled_partition and !has_draft and !has_compaction;
}

pub const WarmModelKind = enum {
    generator,
    embedder,
    reranker,
    chunker,
    classifier,
    rewriter,
    reader,
    transcriber,
    extractor,
};

pub const WarmModelStartupStrategy = enum {
    eager,
    prefetch,
};

pub const WarmModel = struct {
    kind: WarmModelKind = .generator,
    name: []const u8,
    backend: ?backends_mod.BackendType = null,
    format: ?[]const u8 = null,
    quantization: ?[]const u8 = null,
    residency_mode: ?ops.A4bResidencyMode = null,
    memory_budget_mb: ?u32 = null,
    load_strategy: ?ops.A4bLoadStrategy = null,
    load_workers: ?u8 = null,
    load_staging_mb: ?u32 = null,
    prepared_pack: ?ops.A4bPreparedPackMode = null,
    drop_host_cache_after_load: bool = false,
    startup_strategy: WarmModelStartupStrategy = .eager,

    pub fn a4bRequest(self: WarmModel) ?ops.A4bInferenceRequest {
        const residency_mode = self.residency_mode orelse .auto;
        const memory_budget_mb = self.memory_budget_mb orelse 0;
        const load_strategy = self.load_strategy orelse .auto;
        const load_workers = self.load_workers orelse 0;
        const load_staging_mb = self.load_staging_mb orelse 0;
        const prepared_pack = self.prepared_pack orelse .auto;
        // The documented wire defaults are policy-neutral. Treating explicit
        // `auto`/`0` as an A4B request makes an otherwise ordinary generator
        // fail geometry qualification merely because defaults were serialized.
        if (residency_mode == .auto and memory_budget_mb == 0 and
            load_strategy == .auto and load_workers == 0 and load_staging_mb == 0 and
            prepared_pack == .auto and !self.drop_host_cache_after_load) return null;
        return .{
            .residency_mode = residency_mode,
            .memory_budget_mb = memory_budget_mb,
            .load_strategy = load_strategy,
            .load_workers = load_workers,
            .load_staging_mb = load_staging_mb,
            .prepared_pack = prepared_pack,
            .drop_host_cache_after_load = self.drop_host_cache_after_load,
        };
    }
};

fn validatedWarmModelA4bRequest(model: WarmModel) !?ops.A4bInferenceRequest {
    const request = model.a4bRequest();
    if (request != null and model.kind != .generator) return error.A4bUnsupportedModelKind;
    if (request != null and model.backend != null and
        model.backend.? != .metal and model.backend.? != .cuda)
        return error.A4bRequiresGpu;
    return request;
}

test "warm model A4B controls are GPU generator only" {
    try std.testing.expect((WarmModel{
        .name = "ordinary-generator",
        .residency_mode = .auto,
        .memory_budget_mb = 0,
    }).a4bRequest() == null);
    const request = (try validatedWarmModelA4bRequest(.{
        .name = "gemma4-a4b",
        .backend = .metal,
        .residency_mode = .streamed,
        .memory_budget_mb = 4096,
    })).?;
    try std.testing.expectEqual(ops.A4bResidencyMode.streamed, request.residency_mode);
    try std.testing.expectEqual(@as(u32, 4096), request.memory_budget_mb);
    const cuda_request = (try validatedWarmModelA4bRequest(.{
        .name = "gemma4-a4b-cuda",
        .backend = .cuda,
        .residency_mode = .resident,
        .memory_budget_mb = 16384,
    })).?;
    try std.testing.expectEqual(ops.A4bResidencyMode.resident, cuda_request.residency_mode);
    const prepared_request = (try validatedWarmModelA4bRequest(.{
        .name = "gemma4-a4b-cuda-pack",
        .backend = .cuda,
        .load_strategy = .pipeline,
        .load_workers = 6,
        .load_staging_mb = 384,
        .prepared_pack = .required,
        .drop_host_cache_after_load = true,
    })).?;
    try std.testing.expectEqual(ops.A4bLoadStrategy.pipeline, prepared_request.load_strategy);
    try std.testing.expectEqual(@as(u8, 6), prepared_request.load_workers);
    try std.testing.expectEqual(ops.A4bPreparedPackMode.required, prepared_request.prepared_pack);
    try std.testing.expect(prepared_request.drop_host_cache_after_load);
    try std.testing.expectError(error.A4bRequiresGpu, validatedWarmModelA4bRequest(.{
        .name = "gemma4-a4b",
        .backend = .native,
        .memory_budget_mb = 4096,
    }));
    try std.testing.expectError(error.A4bUnsupportedModelKind, validatedWarmModelA4bRequest(.{
        .kind = .embedder,
        .name = "embedder",
        .residency_mode = .streamed,
    }));
}

fn warmModelTaskDir(kind: WarmModelKind) []const u8 {
    return switch (kind) {
        .generator => "generators",
        .embedder => "embedders",
        .reranker => "rerankers",
        .chunker => "chunkers",
        .classifier => "classifiers",
        .rewriter => "rewriters",
        .reader => "readers",
        .transcriber => "transcribers",
        .extractor => "extractors",
    };
}

fn ensureKernelJitRequestSurfacesPublishable(
    mode: graph_mod.kernel_jit.Mode,
    qualified_profile_requested: bool,
    startup_preloads_materialized: bool,
) !void {
    if ((mode.failClosed() or qualified_profile_requested) and !startup_preloads_materialized) {
        return error.KernelJitRequiredPreloadUnmaterialized;
    }
}

fn kernelJitMaterializesOptionalSessions(mode: graph_mod.kernel_jit.Mode) bool {
    return mode.compiles();
}

pub const ai_api_prefix = "/ai/v1";
pub const public_api_prefix = "/ml/v1";
const max_generate_batch_items: usize = 128;
const max_read_batch_images: usize = 64;
const default_read_admission_max_tokens: usize = 256;
const max_read_tokens: usize = 1024;
const default_max_read_batch_bytes: usize = 256 * 1024 * 1024;
const default_max_request_media_bytes: usize = 100 * 1024 * 1024;
const read_admission_bytes_per_unit: usize = 16 * 1024 * 1024;
const default_max_audio_decode_working_bytes: usize = audio_mod.default_decode_working_bytes;
const read_admission_images_per_unit: usize = 2;
// Conservative admission estimate for decoder canvas/scratch, RGB conversion,
// and preprocessing overlap. This bounds request pressure; allocator OOM
// handling remains the final hard limit.
const read_decoded_working_bytes_per_pixel: usize = 16;
const default_max_read_decoded_working_bytes: usize = 512 * 1024 * 1024;
const default_request_content_security = scraping.ContentSecurityConfig{
    .allowed_hosts = &.{},
    .allowed_paths = &.{},
    .block_private_ips = true,
    .max_download_size_bytes = 100 * 1024 * 1024,
};

const GenerateBackendSelection = struct {
    native_choice: native_backend_choice.Choice = .auto,
    compiled_partition_backend: ?ops.BackendKind = null,
    compiled_attachment_target: graph_mod.compiled_backend.AttachmentTarget = .partitioned,
    graph_mode_requested: bool = false,
    eager_mode_requested: bool = false,
};

fn parseGenerateBackendSelection(
    backend_value: ?api.ModelBackend,
    mode_value: ?[]const u8,
    compiled_target_value: ?[]const u8,
) !GenerateBackendSelection {
    const requested_choice = if (backend_value) |value|
        modelBackendToNativeChoice(value)
    else
        native_backend_choice.Choice.auto;
    const choice = native_backend_choice.withPreferredDefault(
        requested_choice,
        native_backend_choice.preferredChoiceFromEnv(),
    );
    try native_backend_choice.validate(choice);

    var eager_mode_requested = false;
    const compiled_mode_requested = if (mode_value) |value| blk: {
        if (std.mem.eql(u8, value, "eager")) {
            eager_mode_requested = true;
            break :blk false;
        }
        if (std.mem.eql(u8, value, "compiled")) break :blk true;
        return error.InvalidGenerateMode;
    } else false;

    const explicit_partition_backend = native_backend_choice.compiledPartitionBackendForMode(
        choice,
        compiled_mode_requested,
    );
    const compiled_attachment_target: graph_mod.compiled_backend.AttachmentTarget = if (compiled_target_value) |value| blk: {
        if (std.mem.eql(u8, value, "partitioned")) break :blk graph_mod.compiled_backend.AttachmentTarget.partitioned;
        if (std.mem.eql(u8, value, "whole-model")) break :blk graph_mod.compiled_backend.AttachmentTarget.whole_model;
        return error.InvalidCompiledTarget;
    } else blk: {
        if (compiled_mode_requested and explicit_partition_backend == .metal) break :blk graph_mod.compiled_backend.AttachmentTarget.whole_model;
        break :blk graph_mod.compiled_backend.AttachmentTarget.partitioned;
    };

    return .{
        .native_choice = choice,
        .compiled_partition_backend = explicit_partition_backend,
        .compiled_attachment_target = compiled_attachment_target,
        .graph_mode_requested = compiled_mode_requested,
        .eager_mode_requested = eager_mode_requested,
    };
}

fn validateGenerateDraftBackend(selection: GenerateBackendSelection, draft_model: ?[]const u8) !void {
    if (draft_model != null and selection.native_choice == .onnx) return error.OnnxDraftUnsupported;
}

fn shouldAutoUseMetalWholeModelGenerate(
    loaded_backend: backends_mod.BackendType,
    metal_executor_supported: bool,
    deepseek_compressed_cache: bool,
    prompt_cache_requested: bool,
    speculation_requested: bool,
    selection: GenerateBackendSelection,
) bool {
    if (!build_options.enable_metal) return false;
    // An eligible cache request must stay on the eager paged-KV route. Silently
    // auto-selecting whole-model compiled execution would ignore an explicit
    // prompt_cache_key and make the opt-in cache appear enabled but inert.
    if (prompt_cache_requested) return false;
    // Gemma4 MTP and ordinary speculative decoding both need model-specific
    // draft state, while MTP additionally taps the target's final hidden rows.
    // The whole-model compiled contract currently exposes logits/tokens only;
    // selecting it here turns an otherwise supported eager request into a
    // late MissingGraphCacheForCompiledPartitionBackend failure. Keep draft
    // requests on the qualified eager decoder-runtime route until compiled
    // speculation has an explicit two-model graph and hidden-output contract.
    if (speculation_requested) return false;
    if (selection.eager_mode_requested) return false;
    if (selection.compiled_partition_backend != null) return false;
    if (selection.native_choice == .native) return false;
    return loaded_backend == .metal and metal_executor_supported and !deepseek_compressed_cache;
}

fn validatePromptCacheExecutionMode(
    prompt_cache_requested: bool,
    selection: GenerateBackendSelection,
) !void {
    if (prompt_cache_requested and selection.graph_mode_requested)
        return error.PromptCacheCompiledModeUnsupported;
}

fn modelBackendToNativeChoice(value: api.ModelBackend) native_backend_choice.Choice {
    return switch (value) {
        .auto => .auto,
        .onnx => .onnx,
        .native => .native,
        .metal => .metal,
        .cuda => .cuda,
        .xla => .xla,
        .webgpu, .wasm => .webgpu,
    };
}

fn configureGenerateBackendPreference(
    session_manager: *backends_mod.SessionManager,
    selection: GenerateBackendSelection,
) void {
    native_backend_choice.configureSessionPreference(session_manager, selection.native_choice);
}

fn singleBackendPreference(backend: backends_mod.BackendType) []const backends_mod.BackendType {
    return switch (backend) {
        .native => &.{.native},
        .onnx => &.{.onnx},
        .metal => &.{.metal},
        .cuda => &.{.cuda},
        .pjrt => &.{.pjrt},
        .wasm => &.{.wasm},
    };
}

/// Global node pointer for operational handlers.
fn embedTimingEnabled() bool {
    return platform.env.getenvBoolDefault("TERMITE_EMBED_TIMING", false);
}

fn embedTimingNowNs() u128 {
    var ts: std.posix.timespec = undefined;
    return switch (std.posix.errno(std.posix.system.clock_gettime(.MONOTONIC, &ts))) {
        .SUCCESS => @intCast(@as(i128, ts.sec) * std.time.ns_per_s + ts.nsec),
        else => 0,
    };
}

const DenseEmbedRequestContext = struct {
    io: std.Io,
    deadline_ns: ?u64 = null,
};

fn remainingDirectEmbeddingDeadlineMs(deadline_ns: ?u64, now_ns: u128) !?u64 {
    const deadline: u128 = deadline_ns orelse return null;
    if (now_ns >= deadline) return error.Timeout;
    const remaining_ns = deadline - now_ns;
    const timeout_ms = (remaining_ns - 1) / std.time.ns_per_ms + 1;
    return @intCast(timeout_ms);
}

fn denseEmbedDownloadContext(context: DenseEmbedRequestContext) !scraping.DownloadContext {
    return .{
        .io = context.io,
        .timeout_ms = try remainingDirectEmbeddingDeadlineMs(context.deadline_ns, embedTimingNowNs()),
    };
}

fn ensureDirectEmbeddingDeadline(deadline_ns: ?u64) !void {
    _ = try remainingDirectEmbeddingDeadlineMs(deadline_ns, embedTimingNowNs());
}

fn isRecoverableEmbeddingRuntimeIntegrityError(err: anyerror) bool {
    return err == error.MissingWeight or err == error.WeightNotFound;
}

fn shouldAbortDensePartialFallback(err: anyerror) bool {
    return err == error.OutOfMemory or isRecoverableEmbeddingRuntimeIntegrityError(err);
}

const LoadedEmbeddingRuntimeAttemptFn = *const fn (
    ctx: *anyopaque,
    model: *model_manager_mod.LoadedModel,
) anyerror!void;

const LoadedEmbeddingRecoveryOptions = struct {
    preferred_backends: ?[]const backends_mod.BackendType = null,
    cache_default_alias: bool = true,
    pin_on_success: bool = false,
    failure_stage: ?*EmbeddingRuntimeFailureStage = null,
};

const EmbeddingRuntimeFailureStage = enum { acquire, execute };

fn runEmbeddingRuntimeRecoveryLoop(adapter: anytype) !void {
    var recovery_attempted = false;
    while (true) {
        var handle = try adapter.acquire();
        defer adapter.release(&handle);

        adapter.execute(&handle) catch |err| {
            if (isRecoverableEmbeddingRuntimeIntegrityError(err)) {
                adapter.retire(&handle);
                if (!recovery_attempted) {
                    recovery_attempted = true;
                    continue;
                }
            }
            return err;
        };
        adapter.succeed(&handle);
        return;
    }
}

fn runLoadedEmbeddingRuntimeWithRecovery(
    node: *Node,
    model_path: []const u8,
    options: LoadedEmbeddingRecoveryOptions,
    ctx: *anyopaque,
    attempt: LoadedEmbeddingRuntimeAttemptFn,
) anyerror!void {
    const Adapter = struct {
        node: *Node,
        model_path: []const u8,
        options: LoadedEmbeddingRecoveryOptions,
        ctx: *anyopaque,
        attempt: LoadedEmbeddingRuntimeAttemptFn,

        fn acquire(self: *@This()) !model_manager_mod.ModelHandle {
            if (self.options.failure_stage) |stage| stage.* = .acquire;
            return if (self.options.preferred_backends) |preferred_backends|
                try self.node.model_manager.acquireFromDirWithPreferredBackends(
                    self.model_path,
                    preferred_backends,
                    self.options.cache_default_alias,
                )
            else
                try self.node.model_manager.acquireFromDir(self.model_path);
        }

        fn execute(self: *@This(), handle: *model_manager_mod.ModelHandle) !void {
            if (self.options.failure_stage) |stage| stage.* = .execute;
            try self.attempt(self.ctx, handle.get());
        }

        fn retire(_: *@This(), handle: *model_manager_mod.ModelHandle) void {
            // The callback has unwound every model-owned asset lease before
            // control returns here, so it is safe to detach the failed
            // runtime. The loop retires the bounded retry as well if it is
            // corrupt, preventing later requests from reusing a known-bad
            // cache entry.
            handle.retire();
        }

        fn release(_: *@This(), handle: *model_manager_mod.ModelHandle) void {
            handle.release();
        }

        fn succeed(self: *@This(), handle: *model_manager_mod.ModelHandle) void {
            if (self.options.pin_on_success) handle.pin();
        }
    };
    var adapter = Adapter{
        .node = node,
        .model_path = model_path,
        .options = options,
        .ctx = ctx,
        .attempt = attempt,
    };
    return runEmbeddingRuntimeRecoveryLoop(&adapter);
}

test "embedding runtime recovery retries once and retires every corrupt runtime" {
    const Probe = struct {
        const FailureMode = enum { first_missing, persistent_missing, capacity };
        const Handle = struct { active: bool = true };

        mode: FailureMode,
        acquire_count: usize = 0,
        execute_count: usize = 0,
        retire_count: usize = 0,
        release_count: usize = 0,
        succeed_count: usize = 0,

        fn acquire(self: *@This()) !Handle {
            self.acquire_count += 1;
            return .{};
        }

        fn execute(self: *@This(), _: *Handle) !void {
            self.execute_count += 1;
            switch (self.mode) {
                .first_missing => if (self.execute_count == 1) return error.MissingWeight,
                .persistent_missing => return error.MissingWeight,
                .capacity => return error.ResourceTemporarilyUnavailable,
            }
        }

        fn retire(self: *@This(), handle: *Handle) void {
            std.debug.assert(handle.active);
            handle.active = false;
            self.retire_count += 1;
        }

        fn release(self: *@This(), handle: *Handle) void {
            if (!handle.active) return;
            handle.active = false;
            self.release_count += 1;
        }

        fn succeed(self: *@This(), _: *Handle) void {
            self.succeed_count += 1;
        }
    };

    var recovered = Probe{ .mode = .first_missing };
    try runEmbeddingRuntimeRecoveryLoop(&recovered);
    try std.testing.expectEqual(@as(usize, 2), recovered.acquire_count);
    try std.testing.expectEqual(@as(usize, 2), recovered.execute_count);
    try std.testing.expectEqual(@as(usize, 1), recovered.retire_count);
    try std.testing.expectEqual(@as(usize, 1), recovered.release_count);
    try std.testing.expectEqual(@as(usize, 1), recovered.succeed_count);

    var persistent = Probe{ .mode = .persistent_missing };
    try std.testing.expectError(error.MissingWeight, runEmbeddingRuntimeRecoveryLoop(&persistent));
    try std.testing.expectEqual(@as(usize, 2), persistent.acquire_count);
    try std.testing.expectEqual(@as(usize, 2), persistent.retire_count);
    try std.testing.expectEqual(@as(usize, 0), persistent.release_count);

    var capacity = Probe{ .mode = .capacity };
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, runEmbeddingRuntimeRecoveryLoop(&capacity));
    try std.testing.expectEqual(@as(usize, 1), capacity.acquire_count);
    try std.testing.expectEqual(@as(usize, 0), capacity.retire_count);
    try std.testing.expectEqual(@as(usize, 1), capacity.release_count);
}

test "embedding runtime recovery is limited to weight integrity failures" {
    try std.testing.expect(isRecoverableEmbeddingRuntimeIntegrityError(error.MissingWeight));
    try std.testing.expect(isRecoverableEmbeddingRuntimeIntegrityError(error.WeightNotFound));
    try std.testing.expect(!isRecoverableEmbeddingRuntimeIntegrityError(error.ResourceLimitExceeded));
    try std.testing.expect(!isRecoverableEmbeddingRuntimeIntegrityError(error.Canceled));
    try std.testing.expect(shouldAbortDensePartialFallback(error.MissingWeight));
    try std.testing.expect(shouldAbortDensePartialFallback(error.WeightNotFound));
    try std.testing.expect(shouldAbortDensePartialFallback(error.OutOfMemory));
    try std.testing.expect(!shouldAbortDensePartialFallback(error.ResourceTemporarilyUnavailable));
}

test "dense embed download deadline is a nonzero remaining ceiling" {
    try std.testing.expect((try remainingDirectEmbeddingDeadlineMs(null, 100)) == null);
    try std.testing.expectError(error.Timeout, remainingDirectEmbeddingDeadlineMs(100, 100));
    try std.testing.expectError(error.Timeout, remainingDirectEmbeddingDeadlineMs(99, 100));
    try std.testing.expectEqual(@as(?u64, 1), try remainingDirectEmbeddingDeadlineMs(101, 100));
    try std.testing.expectEqual(@as(?u64, 1), try remainingDirectEmbeddingDeadlineMs(std.time.ns_per_ms, 0));
    try std.testing.expectEqual(@as(?u64, 2), try remainingDirectEmbeddingDeadlineMs(std.time.ns_per_ms + 1, 0));
}

test "dense embed parser contexts reject expired image fetches" {
    const allocator = std.testing.allocator;
    var node: Node = undefined;
    node.config = .{};
    const manifest = manifest_mod.ModelManifest{
        .allocator = allocator,
        .model_type = .embedder,
        .visual_model_path = "visual.onnx",
    };
    const context = DenseEmbedRequestContext{ .io = std.testing.io, .deadline_ns = 0 };

    var fail_fast_input = try std.json.parseFromSlice(
        std.json.Value,
        allocator,
        "[{\"type\":\"image_url\",\"image_url\":\"data:image/png;base64,AA==\"}]",
        .{},
    );
    defer fail_fast_input.deinit();
    var fail_fast_budget = RequestMediaBudget.init(1024);
    resetRequestWorkTestCounters();
    try std.testing.expectError(
        error.Timeout,
        parseDenseEmbedInputsWithBudgetAndContext(
            &node,
            allocator,
            &manifest,
            fail_fast_input.value,
            &fail_fast_budget,
            context,
        ),
    );
    try std.testing.expectEqual(@as(usize, 0), request_work_test_counters.media_fetch_attempts);
    try std.testing.expectEqual(@as(usize, 0), fail_fast_budget.used_bytes);

    var per_item_input = try std.json.parseFromSlice(
        std.json.Value,
        allocator,
        "[{\"type\":\"image_url\",\"image_url\":{\"url\":\"data:image/png;base64,AA==\"}}]",
        .{},
    );
    defer per_item_input.deinit();
    var per_item_budget = RequestMediaBudget.init(1024);
    resetRequestWorkTestCounters();
    try std.testing.expectError(
        error.Timeout,
        parseDenseEmbedInputsPerItemWithBudgetAndContext(
            &node,
            allocator,
            &manifest,
            per_item_input.value,
            &per_item_budget,
            context,
        ),
    );
    try std.testing.expectEqual(@as(usize, 0), request_work_test_counters.media_fetch_attempts);
    try std.testing.expectEqual(@as(usize, 0), per_item_budget.used_bytes);

    const parts = [_]Node.DirectDenseEmbedPart{.{ .image_url = "data:image/png;base64,AA==" }};
    var direct_budget = RequestMediaBudget.init(1024);
    resetRequestWorkTestCounters();
    try std.testing.expectError(
        error.Timeout,
        parseDirectDenseEmbedInputsWithContext(
            &node,
            allocator,
            &manifest,
            &parts,
            &direct_budget,
            context,
        ),
    );
    try std.testing.expectEqual(@as(usize, 0), request_work_test_counters.media_fetch_attempts);
    try std.testing.expectEqual(@as(usize, 0), direct_budget.used_bytes);
}

fn freeDirectDenseVectors(allocator: std.mem.Allocator, vectors: [][]f32) void {
    for (vectors) |vector| allocator.free(vector);
    allocator.free(vectors);
}

fn embedTimingStart() u128 {
    if (!embedTimingEnabled()) return 0;
    return embedTimingNowNs();
}

fn logEmbedTiming(phase: []const u8, count: usize, start_ns: u128) void {
    if (start_ns == 0) return;
    const now = embedTimingNowNs();
    const elapsed_us = if (now > start_ns) @divTrunc(now - start_ns, 1000) else 0;
    std.log.info("antfly inference embed timing phase={s} count={d} elapsed_us={d}", .{ phase, count, elapsed_us });
}

fn serverGenerateTimingEnabled() bool {
    return platform.env.getenvBool("TERMITE_SERVER_GENERATE_TIMING");
}

fn elapsedMs(from_ns: u128, to_ns: u128) u64 {
    if (to_ns <= from_ns) return 0;
    return @intCast(@divTrunc(to_ns - from_ns, std.time.ns_per_ms));
}

fn allocCompletionId(allocator: std.mem.Allocator) ![]u8 {
    var bytes: [8]u8 = undefined;
    try fillRandomBytes(&bytes);
    const value = std.mem.readInt(u64, &bytes, .little);
    var scratch: [16]u8 = undefined;
    const rendered = try std.fmt.bufPrint(&scratch, "{x}", .{value});
    var padded = [_]u8{'0'} ** 16;
    @memcpy(padded[padded.len - rendered.len ..], rendered);
    return std.fmt.allocPrint(allocator, "chatcmpl-{s}", .{padded[0..]});
}

fn fillRandomBytes(buffer: []u8) !void {
    if (buffer.len == 0) return;

    if (builtin.os.tag == .linux) {
        var offset: usize = 0;
        while (offset < buffer.len) {
            const remaining = buffer[offset..];
            const rc = std.os.linux.getrandom(remaining.ptr, remaining.len, 0);
            switch (std.posix.errno(rc)) {
                .SUCCESS => {
                    const n: usize = @intCast(rc);
                    if (n == 0) return error.EntropyUnavailable;
                    offset += n;
                },
                .INTR => continue,
                else => return error.EntropyUnavailable,
            }
        }
        return;
    }

    if (comptime @TypeOf(std.c.arc4random_buf) != void) {
        std.c.arc4random_buf(buffer.ptr, buffer.len);
        return;
    }

    return error.EntropyUnavailable;
}

fn completionCreatedTimestamp() i64 {
    var ts: std.posix.timespec = undefined;
    return switch (std.posix.errno(std.posix.system.clock_gettime(.REALTIME, &ts))) {
        .SUCCESS => @intCast(ts.sec),
        else => 0,
    };
}

fn tokenUsage(prompt_tokens: usize, completion_tokens: usize) api.GenerateUsage {
    return .{
        .prompt_tokens = @intCast(prompt_tokens),
        .completion_tokens = @intCast(completion_tokens),
        .total_tokens = @intCast(prompt_tokens + completion_tokens),
    };
}

fn tokenUsageWithCachedPrompt(
    prompt_tokens: usize,
    completion_tokens: usize,
    cached_prompt_tokens: usize,
) api.GenerateUsage {
    var usage = tokenUsage(prompt_tokens, completion_tokens);
    usage.cached_prompt_tokens = if (cached_prompt_tokens == 0) null else @intCast(cached_prompt_tokens);
    return usage;
}

fn estimateTextTokens(text: []const u8) usize {
    var count: usize = 0;
    var in_token = false;
    for (text) |ch| {
        const ws = switch (ch) {
            ' ', '\n', '\r', '\t' => true,
            else => false,
        };
        if (ws) {
            in_token = false;
        } else if (!in_token) {
            count += 1;
            in_token = true;
        }
    }
    return count;
}

fn estimateTextsTokens(texts: []const []const u8) usize {
    var total: usize = 0;
    for (texts) |text| total += estimateTextTokens(text);
    return total;
}

// std.Io owns the actual worker pool, so this bounds queued tokenizer
// consumers without creating or oversubscribing OS threads. Sixteen gives the
// encoder enough pull-scheduled chunks for heterogeneous server CPUs while its
// 256 KiB threshold keeps normal request prompts on the serial path.
const tokenizer_parallel_max_tasks = 16;

fn countTokenizerTokens(
    allocator: std.mem.Allocator,
    io: ?std.Io,
    tokenizer: anytype,
    text: []const u8,
) !usize {
    var ids: std.ArrayListUnmanaged(i32) = .empty;
    defer ids.deinit(allocator);
    if (io) |runtime_io| {
        try tokenizer.encodeIntoParallel(
            runtime_io,
            allocator,
            text,
            &ids,
            tokenizer_parallel_max_tasks,
        );
    } else {
        try tokenizer.encodeInto(allocator, text, &ids);
    }
    return ids.items.len;
}

fn countTokenizerTexts(
    allocator: std.mem.Allocator,
    io: ?std.Io,
    tokenizer: anytype,
    texts: []const []const u8,
) !usize {
    var total: usize = 0;
    for (texts) |text| total += try countTokenizerTokens(allocator, io, tokenizer, text);
    return total;
}

fn countParsedDenseEmbedTextTokens(
    allocator: std.mem.Allocator,
    io: ?std.Io,
    tokenizer: anytype,
    inputs: *const ParsedDenseEmbedInputs,
) usize {
    var total: usize = 0;
    for (inputs.texts.items) |item| {
        total += countTokenizerTokens(allocator, io, tokenizer, item.text) catch estimateTextTokens(item.text);
    }
    return total;
}

test "token counting uses the attached std.Io tokenizer path" {
    const ProbeTokenizer = struct {
        serial_calls: *usize,
        parallel_calls: *usize,

        pub fn encodeInto(
            self: @This(),
            allocator: std.mem.Allocator,
            _: []const u8,
            ids: *std.ArrayListUnmanaged(i32),
        ) !void {
            self.serial_calls.* += 1;
            try ids.append(allocator, 1);
        }

        pub fn encodeIntoParallel(
            self: @This(),
            _: std.Io,
            allocator: std.mem.Allocator,
            _: []const u8,
            ids: *std.ArrayListUnmanaged(i32),
            max_tasks: usize,
        ) !void {
            try std.testing.expectEqual(tokenizer_parallel_max_tasks, max_tasks);
            self.parallel_calls.* += 1;
            try ids.appendSlice(allocator, &.{ 1, 2 });
        }
    };

    var serial_calls: usize = 0;
    var parallel_calls: usize = 0;
    const tokenizer = ProbeTokenizer{
        .serial_calls = &serial_calls,
        .parallel_calls = &parallel_calls,
    };
    try std.testing.expectEqual(
        @as(usize, 2),
        try countTokenizerTokens(
            std.testing.allocator,
            std.testing.io,
            tokenizer,
            "parallel",
        ),
    );
    try std.testing.expectEqual(
        @as(usize, 1),
        try countTokenizerTokens(
            std.testing.allocator,
            null,
            tokenizer,
            "serial",
        ),
    );
    try std.testing.expectEqual(@as(usize, 1), parallel_calls);
    try std.testing.expectEqual(@as(usize, 1), serial_calls);
}

fn estimateParsedDenseEmbedPromptTokens(inputs: *const ParsedDenseEmbedInputs) usize {
    var total: usize = 0;
    for (inputs.texts.items) |item| total += estimateTextTokens(item.text);
    return total;
}

fn isOpenAiListTask(task: []const u8) bool {
    return std.mem.eql(u8, task, "generators") or std.mem.eql(u8, task, "embedders");
}

const CompatibilitySummary = model_manager_mod.CompatibilitySummary;
const CompatibilitySignature = [std.crypto.hash.sha2.Sha256.digest_length]u8;
const compatibility_sidecar_hash_limit: u64 = 4 * 1024 * 1024;
const compatibility_artifact_sample_bytes: usize = 64 * 1024;

const CachedCompatibility = struct {
    allocator: std.mem.Allocator,
    refs: std.atomic.Value(usize) = .init(1),
    signature: CompatibilitySignature,
    summary: CompatibilitySummary,
    external_paths: [][]u8,
    external_paths_valid: bool,

    fn create(
        allocator: std.mem.Allocator,
        signature: CompatibilitySignature,
        summary: CompatibilitySummary,
        dependencies: *OnnxDependencies,
    ) !*CachedCompatibility {
        const self = try allocator.create(CachedCompatibility);
        errdefer allocator.destroy(self);
        self.* = .{
            .allocator = allocator,
            .signature = signature,
            .summary = summary,
            .external_paths = try dependencies.paths.toOwnedSlice(allocator),
            .external_paths_valid = dependencies.valid,
        };
        return self;
    }

    fn retain(self: *CachedCompatibility) void {
        _ = self.refs.fetchAdd(1, .monotonic);
    }

    fn release(self: *CachedCompatibility) void {
        if (self.refs.fetchSub(1, .acq_rel) != 1) return;
        for (self.external_paths) |path| self.allocator.free(path);
        self.allocator.free(self.external_paths);
        const allocator = self.allocator;
        allocator.destroy(self);
    }
};

const OnnxDependencies = struct {
    paths: std.ArrayListUnmanaged([]u8) = .empty,
    valid: bool = true,

    fn deinit(self: *OnnxDependencies, allocator: std.mem.Allocator) void {
        for (self.paths.items) |path| allocator.free(path);
        self.paths.deinit(allocator);
        self.* = .{};
    }

    fn append(self: *OnnxDependencies, allocator: std.mem.Allocator, path: []const u8) !void {
        for (self.paths.items) |existing| {
            if (std.mem.eql(u8, existing, path)) return;
        }
        const owned_path = try allocator.dupe(u8, path);
        errdefer allocator.free(owned_path);
        try self.paths.append(allocator, owned_path);
    }
};

fn updateCompatibilitySignatureSlice(
    signature: *std.crypto.hash.sha2.Sha256,
    value: []const u8,
) void {
    const len: u64 = @intCast(value.len);
    signature.update(std.mem.asBytes(&len));
    signature.update(value);
}

fn addArtifactIdentityToSignature(
    allocator: std.mem.Allocator,
    io: std.Io,
    signature: *std.crypto.hash.sha2.Sha256,
    path: ?[]const u8,
    hash_small_contents: bool,
) !void {
    const artifact_path = path orelse return;
    updateCompatibilitySignatureSlice(signature, artifact_path);
    const stat = std.Io.Dir.cwd().statFile(io, artifact_path, .{}) catch {
        signature.update("missing");
        return;
    };
    signature.update(std.mem.asBytes(&stat.inode));
    signature.update(std.mem.asBytes(&stat.size));
    const mtime_ns = stat.mtime.toNanoseconds();
    const ctime_ns = stat.ctime.toNanoseconds();
    // std.Io.Timestamp uses i96, whose ABI representation can contain
    // indeterminate padding. Hash a fully represented integer instead.
    const canonical_mtime_ns: i128 = mtime_ns;
    const canonical_ctime_ns: i128 = ctime_ns;
    signature.update(std.mem.asBytes(&canonical_mtime_ns));
    signature.update(std.mem.asBytes(&canonical_ctime_ns));
    if (hash_small_contents and stat.size <= compatibility_sidecar_hash_limit) {
        const bytes = c_file.readFileMax(
            allocator,
            artifact_path,
            compatibility_sidecar_hash_limit,
        ) catch |err| {
            if (err == error.OutOfMemory) return err;
            signature.update("content-unreadable");
            return;
        };
        defer allocator.free(bytes);
        updateCompatibilitySignatureSlice(signature, bytes);
    } else if (stat.size > 0) {
        // Stat identity alone misses atomic/in-place deployments that preserve
        // inode, length, and timestamp. Hash bounded samples so cache hits stay
        // O(1) in artifact size while still tracking large graph/weight swaps.
        const sample_len: usize = @intCast(@min(
            stat.size,
            compatibility_artifact_sample_bytes,
        ));
        const offsets = [_]u64{
            0,
            if (stat.size > sample_len) (stat.size - sample_len) / 2 else 0,
            if (stat.size > sample_len) stat.size - sample_len else 0,
        };
        for (offsets, 0..) |offset, index| {
            if (index > 0 and offset == offsets[index - 1]) continue;
            const bytes = c_file.readRegion(allocator, artifact_path, offset, sample_len) catch |err| {
                if (err == error.OutOfMemory) return err;
                signature.update("sample-unreadable");
                return;
            };
            defer allocator.free(bytes);
            signature.update(std.mem.asBytes(&offset));
            updateCompatibilitySignatureSlice(signature, bytes);
        }
    }

    const final_stat = std.Io.Dir.cwd().statFile(io, artifact_path, .{}) catch {
        signature.update("changed-during-read");
        return;
    };
    const final_mtime_ns = final_stat.mtime.toNanoseconds();
    const final_ctime_ns = final_stat.ctime.toNanoseconds();
    if (final_stat.inode != stat.inode or
        final_stat.size != stat.size or
        final_mtime_ns != mtime_ns or
        final_ctime_ns != ctime_ns)
        signature.update("changed-during-read");
}

fn addOnnxArtifactToSignature(
    allocator: std.mem.Allocator,
    io: std.Io,
    signature: *std.crypto.hash.sha2.Sha256,
    maybe_path: ?[]const u8,
) !void {
    const path = maybe_path orelse return;
    try addArtifactIdentityToSignature(allocator, io, signature, path, false);
}

fn collectOnnxDependenciesForPath(
    allocator: std.mem.Allocator,
    dependencies: *OnnxDependencies,
    maybe_path: ?[]const u8,
) !void {
    const path = maybe_path orelse return;
    if (!std.mem.endsWith(u8, path, ".onnx")) return;
    var artifacts = backends_mod.imported_onnx_session.inspectArtifactSet(
        allocator,
        path,
    ) catch |err| {
        if (err == error.OutOfMemory) return err;
        dependencies.valid = false;
        return;
    };
    defer artifacts.deinit();
    for (artifacts.external_paths) |external_path| {
        try dependencies.append(allocator, external_path);
    }
}

fn collectCompatibilityOnnxDependencies(
    allocator: std.mem.Allocator,
    man: *const manifest_mod.ModelManifest,
) !OnnxDependencies {
    var dependencies = OnnxDependencies{};
    errdefer dependencies.deinit(allocator);
    const paths = [_]?[]const u8{
        man.onnx_path,
        man.visual_model_path,
        man.audio_model_path,
        man.text_projection_path,
        man.visual_projection_path,
        man.audio_projection_path,
    };
    for (paths) |path| try collectOnnxDependenciesForPath(allocator, &dependencies, path);
    return dependencies;
}

fn addModelSidecarToSignature(
    allocator: std.mem.Allocator,
    io: std.Io,
    signature: *std.crypto.hash.sha2.Sha256,
    model_path: []const u8,
    sidecar_name: []const u8,
) !void {
    var path_buffer: [std.fs.max_path_bytes]u8 = undefined;
    const separator = if (std.mem.endsWith(u8, model_path, "/")) "" else "/";
    const path = std.fmt.bufPrint(
        &path_buffer,
        "{s}{s}{s}",
        .{ model_path, separator, sidecar_name },
    ) catch {
        // Keep overlong paths distinct and deterministically uncacheable from a
        // shorter valid sidecar path.
        signature.update(model_path);
        signature.update(sidecar_name);
        signature.update("sidecar-path-too-long");
        return;
    };
    try addArtifactIdentityToSignature(allocator, io, signature, path, true);
}

fn addShardedArtifactStatsToSignature(
    allocator: std.mem.Allocator,
    io: std.Io,
    signature: *std.crypto.hash.sha2.Sha256,
    index_path: ?[]const u8,
) !void {
    const path = index_path orelse return;
    const bytes = try c_file.readFile(allocator, path);
    defer allocator.free(bytes);
    var index = try safetensors_mod.ShardedIndex.load(allocator, bytes);
    defer index.deinit();
    const model_dir = std.fs.path.dirname(path) orelse return error.InvalidPath;
    var seen = std.StringHashMapUnmanaged(void).empty;
    defer seen.deinit(allocator);
    var it = index.weight_map.iterator();
    while (it.next()) |entry| {
        const shard = entry.value_ptr.*;
        if (seen.contains(shard)) continue;
        try seen.put(allocator, shard, {});
        const shard_path = try std.fs.path.join(allocator, &.{ model_dir, shard });
        defer allocator.free(shard_path);
        try addArtifactIdentityToSignature(
            allocator,
            io,
            signature,
            shard_path,
            false,
        );
    }
}

fn addCompatibilityManifestFacts(
    signature: *std.crypto.hash.sha2.Sha256,
    man: *const manifest_mod.ModelManifest,
) void {
    const model_type: u8 = @intFromEnum(man.model_type);
    const model_type_origin: u8 = @intFromEnum(man.model_type_origin);
    const native_arch_hint: u8 = @intFromEnum(man.native_arch_hint);
    signature.update(&.{ model_type, model_type_origin, native_arch_hint });
    updateCompatibilitySignatureSlice(signature, man.config_model_arch);
    updateCompatibilitySignatureSlice(signature, man.gliner_model_type);
    updateCompatibilitySignatureSlice(signature, man.inference_bundle_family);
    signature.update(&.{
        @intFromBool(man.hasIncompleteGlinerBundle()),
        @intFromBool(man.hasIncompleteColqwenBundle()),
        @intFromBool(man.hasIncompleteClipclapGgufBundle()),
        @intFromBool(man.hasIncompleteFlorence2GgufBundle()),
        @intFromBool(man.isClipclapGgufBundle()),
    });
}

fn computeCompatibilitySignatureWithDependencies(
    allocator: std.mem.Allocator,
    io: std.Io,
    model_path: []const u8,
    man: *const manifest_mod.ModelManifest,
    external_paths: []const []const u8,
    external_paths_valid: bool,
) !CompatibilitySignature {
    var signature = std.crypto.hash.sha2.Sha256.init(.{});
    addCompatibilityManifestFacts(&signature, man);

    // Hash every small metadata input parsed by loadListingFromDir. Content
    // hashing avoids stale policy decisions when deployment tools preserve
    // size and timestamps during an in-place replacement.
    for (manifest_mod.listing_compatibility_sidecars) |sidecar_name| {
        try addModelSidecarToSignature(
            allocator,
            io,
            &signature,
            model_path,
            sidecar_name,
        );
    }
    try addArtifactIdentityToSignature(allocator, io, &signature, man.model_manifest_path, true);
    try addArtifactIdentityToSignature(allocator, io, &signature, man.config_path, true);
    try addArtifactIdentityToSignature(allocator, io, &signature, man.gguf_path, false);
    try addArtifactIdentityToSignature(allocator, io, &signature, man.gguf_projector_path, false);
    try addArtifactIdentityToSignature(allocator, io, &signature, man.safetensors_path, false);
    try addArtifactIdentityToSignature(allocator, io, &signature, man.safetensors_index_path, true);
    addShardedArtifactStatsToSignature(
        allocator,
        io,
        &signature,
        man.safetensors_index_path,
    ) catch |err| {
        if (err == error.OutOfMemory) return err;
        signature.update("invalid-sharded-index");
    };
    try addOnnxArtifactToSignature(allocator, io, &signature, man.onnx_path);
    try addArtifactIdentityToSignature(allocator, io, &signature, man.gliner_head_gguf_path, false);
    try addArtifactIdentityToSignature(allocator, io, &signature, man.gliner_head_safetensors_path, false);
    try addOnnxArtifactToSignature(allocator, io, &signature, man.visual_model_path);
    try addOnnxArtifactToSignature(allocator, io, &signature, man.audio_model_path);
    try addOnnxArtifactToSignature(allocator, io, &signature, man.text_projection_path);
    try addOnnxArtifactToSignature(allocator, io, &signature, man.visual_projection_path);
    try addOnnxArtifactToSignature(allocator, io, &signature, man.audio_projection_path);
    try addArtifactIdentityToSignature(allocator, io, &signature, man.tokenizer_json_path, false);
    try addArtifactIdentityToSignature(allocator, io, &signature, man.tokenizer_config_path, true);
    try addArtifactIdentityToSignature(allocator, io, &signature, man.preprocessor_config_path, true);
    try addArtifactIdentityToSignature(allocator, io, &signature, man.processor_config_path, true);
    if (!external_paths_valid) signature.update("invalid-onnx-artifact-set");
    for (external_paths) |external_path| {
        try addArtifactIdentityToSignature(
            allocator,
            io,
            &signature,
            external_path,
            false,
        );
    }

    var digest: CompatibilitySignature = undefined;
    signature.final(&digest);
    return digest;
}

fn computeCompatibilitySignature(
    allocator: std.mem.Allocator,
    io: std.Io,
    model_path: []const u8,
    man: *const manifest_mod.ModelManifest,
) !CompatibilitySignature {
    var dependencies = try collectCompatibilityOnnxDependencies(allocator, man);
    defer dependencies.deinit(allocator);
    return computeCompatibilitySignatureWithDependencies(
        allocator,
        io,
        model_path,
        man,
        dependencies.paths.items,
        dependencies.valid,
    );
}

test "compatibility signature tracks implicit GLiNER classification sidecar" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const model_dir = try std.fs.path.join(
        allocator,
        &.{ ".zig-cache", "tmp", tmp.sub_path[0..] },
    );
    defer allocator.free(model_dir);

    var missing = std.crypto.hash.sha2.Sha256.init(.{});
    try addModelSidecarToSignature(
        allocator,
        std.testing.io,
        &missing,
        model_dir,
        "special_tokens_map.json",
    );

    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "special_tokens_map.json",
        .data = "{\"additional_special_tokens\":[\"[P]\",\"[C]\",\"[E]\",\"[R]\",\"[SEP_TEXT]\"]}",
    });
    var present = std.crypto.hash.sha2.Sha256.init(.{});
    try addModelSidecarToSignature(
        allocator,
        std.testing.io,
        &present,
        model_dir,
        "special_tokens_map.json",
    );
    var missing_digest: CompatibilitySignature = undefined;
    var present_digest: CompatibilitySignature = undefined;
    missing.final(&missing_digest);
    present.final(&present_digest);
    try std.testing.expect(!std.mem.eql(
        u8,
        missing_digest[0..],
        present_digest[0..],
    ));
}

test "compatibility signature hashes same-size sidecar replacements" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const model_dir = try std.fs.path.join(
        allocator,
        &.{ ".zig-cache", "tmp", tmp.sub_path[0..] },
    );
    defer allocator.free(model_dir);

    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "config.json",
        .data = "{\"model_type\":\"bert\"}",
    });
    var first = std.crypto.hash.sha2.Sha256.init(.{});
    try addModelSidecarToSignature(
        allocator,
        std.testing.io,
        &first,
        model_dir,
        "config.json",
    );
    var first_digest: CompatibilitySignature = undefined;
    first.final(&first_digest);

    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "config.json",
        .data = "{\"model_type\":\"bart\"}",
    });
    var second = std.crypto.hash.sha2.Sha256.init(.{});
    try addModelSidecarToSignature(
        allocator,
        std.testing.io,
        &second,
        model_dir,
        "config.json",
    );
    var second_digest: CompatibilitySignature = undefined;
    second.final(&second_digest);
    try std.testing.expect(!std.mem.eql(
        u8,
        first_digest[0..],
        second_digest[0..],
    ));
}

test "cached compatibility signature tracks known ONNX external dependencies without reparsing" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const model_dir = try std.fs.path.join(
        allocator,
        &.{ ".zig-cache", "tmp", tmp.sub_path[0..] },
    );
    defer allocator.free(model_dir);
    const external_path = try std.fs.path.join(
        allocator,
        &.{ model_dir, "weights.bin" },
    );
    defer allocator.free(external_path);
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "weights.bin",
        .data = "first-external-generation",
    });

    var man = manifest_mod.ModelManifest{ .allocator = allocator };
    defer man.deinit();
    const first = try computeCompatibilitySignatureWithDependencies(
        allocator,
        std.testing.io,
        model_dir,
        &man,
        &.{external_path},
        true,
    );
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "weights.bin",
        .data = "second-external-generat",
    });
    const second = try computeCompatibilitySignatureWithDependencies(
        allocator,
        std.testing.io,
        model_dir,
        &man,
        &.{external_path},
        true,
    );
    try std.testing.expect(!std.mem.eql(u8, first[0..], second[0..]));
}

/// Apply the serving policy before model loading. `ModelManager` repeats this check as a
/// safety backstop so a new endpoint or direct Node helper cannot bypass it.
fn rejectDisallowedModel(
    self: *Node,
    ctx: *httpx.Context,
    model_path: []const u8,
) !?httpx.Response {
    const summary = self.compatibilitySummaryForDir(ctx.allocator, model_path) catch {
        if (self.config.allow_unknown_models) return null;
        return try ctx.status(400).json(.{
            .@"error" = "UNKNOWN_MODEL_COMPATIBILITY",
            .message = "model compatibility could not be determined; restart with --allow-unknown-models to opt in",
        });
    };
    switch (summary.level) {
        .compatible => return null,
        .unknown => {
            if (self.config.allow_unknown_models) return null;
            return try ctx.status(400).json(.{
                .@"error" = "UNKNOWN_MODEL_COMPATIBILITY",
                .message = summary.message,
            });
        },
        .incompatible => return try ctx.status(400).json(.{
            .@"error" = "INCOMPATIBLE_MODEL",
            .message = summary.message,
        }),
    }
}

const transient_capacity_retry_after_seconds = "1";
const transient_capacity_retry_after_ms = 1000;

fn transientCapacityFailureResponse(
    ctx: *httpx.Context,
    code: []const u8,
    message: []const u8,
    reason: []const u8,
) !httpx.Response {
    // Retry-After is expressed in seconds by HTTP. The millisecond field keeps
    // SDK clients from having to parse headers and makes retryability explicit.
    try ctx.setHeader("Retry-After", transient_capacity_retry_after_seconds);
    return ctx.status(503).json(.{
        .@"error" = code,
        .message = message,
        .reason = reason,
        .retryable = true,
        .retry_after_ms = transient_capacity_retry_after_ms,
    });
}

fn modelResourceBusyResponse(ctx: *httpx.Context) !httpx.Response {
    return transientCapacityFailureResponse(
        ctx,
        "MODEL_RESOURCE_BUSY",
        "insufficient inference capacity is currently available",
        "inference_capacity",
    );
}

fn modelArtifactsChangingResponse(ctx: *httpx.Context) !httpx.Response {
    return transientCapacityFailureResponse(
        ctx,
        "MODEL_ARTIFACTS_CHANGING",
        "model artifacts are being published; retry with a consistent generation",
        "model_publication",
    );
}

fn modelLoadFailureResponse(ctx: *httpx.Context, err: anyerror) !httpx.Response {
    return switch (err) {
        error.UnknownModelCompatibility => ctx.status(400).json(.{
            .@"error" = "UNKNOWN_MODEL_COMPATIBILITY",
            .message = "model compatibility is unknown; restart with --allow-unknown-models to opt in",
        }),
        error.IncompatibleModel => ctx.status(400).json(.{
            .@"error" = "INCOMPATIBLE_MODEL",
            .message = "model artifact is incompatible with the available runtime",
        }),
        error.ResourceLimitExceeded => ctx.status(400).json(.{
            .@"error" = "MODEL_RESOURCE_LIMIT",
            .message = "model resource plan exceeds the configured inference budget",
        }),
        error.ResourceTemporarilyUnavailable => modelResourceBusyResponse(ctx),
        error.ModelArtifactsChanging,
        error.IncompleteManagedDownload,
        => modelArtifactsChangingResponse(ctx),
        else => ctx.status(500).json(.{
            .@"error" = "MODEL_LOAD_FAILED",
            .message = @errorName(err),
        }),
    };
}

fn inferenceFailureResponse(ctx: *httpx.Context, err: anyerror) !httpx.Response {
    return switch (err) {
        error.ResourceLimitExceeded => ctx.status(400).json(.{
            .@"error" = "MODEL_RESOURCE_LIMIT",
            .message = "request resource plan exceeds the configured inference budget",
        }),
        error.ResourceTemporarilyUnavailable => modelResourceBusyResponse(ctx),
        else => ctx.status(500).json(.{
            .@"error" = "INFERENCE_FAILED",
            .message = @errorName(err),
        }),
    };
}

fn rejectExplicitBackendIncompatibility(
    ctx: *httpx.Context,
    model_path: []const u8,
    choice: native_backend_choice.Choice,
    allow_unknown: bool,
) !?httpx.Response {
    const backend: backends_mod.BackendType = switch (choice) {
        .auto => return null,
        .onnx => .onnx,
        .native => .native,
        .metal => .metal,
        .cuda => .cuda,
        .xla, .webgpu => return null,
    };
    var man = try manifest_mod.loadListingFromDir(ctx.allocator, model_path);
    defer man.deinit();
    const summary = try model_manager_mod.compatibilitySummaryForBackend(
        ctx.allocator,
        model_path,
        &man,
        backend,
    ) orelse {
        return try ctx.status(400).json(.{
            .@"error" = "INCOMPATIBLE_MODEL",
            .message = "the selected backend has no compatible artifact in this model bundle",
        });
    };
    if (summary.level == .incompatible or
        (summary.level == .unknown and !allow_unknown))
    {
        return try ctx.status(400).json(.{
            .@"error" = if (summary.level == .unknown)
                "UNKNOWN_MODEL_COMPATIBILITY"
            else
                "INCOMPATIBLE_MODEL",
            .message = summary.message,
        });
    }
    return null;
}

fn appendOpenAiModelEntry(
    buf: *std.ArrayListUnmanaged(u8),
    allocator: std.mem.Allocator,
    model_id: []const u8,
    created: i64,
    compatibility_level: []const u8,
) !void {
    try buf.appendSlice(allocator, "{\"id\":");
    try jsonEncodeString(buf, allocator, model_id);
    const metadata = try std.fmt.allocPrint(
        allocator,
        ",\"object\":\"model\",\"created\":{d},\"owned_by\":\"antfly\",\"compatibility\":",
        .{created},
    );
    defer allocator.free(metadata);
    try buf.appendSlice(allocator, metadata);
    try jsonEncodeString(buf, allocator, compatibility_level);
    try buf.append(allocator, '}');
}

test "OpenAI model entries expose artifact compatibility" {
    const allocator = std.testing.allocator;
    var buf = std.ArrayListUnmanaged(u8).empty;
    defer buf.deinit(allocator);

    try appendOpenAiModelEntry(
        &buf,
        allocator,
        "owner/model",
        42,
        "unknown",
    );
    try std.testing.expectEqualStrings(
        "{\"id\":\"owner/model\",\"object\":\"model\",\"created\":42,\"owned_by\":\"antfly\",\"compatibility\":\"unknown\"}",
        buf.items,
    );
}

fn appendUniqueOpenAiModelEntry(
    buf: *std.ArrayListUnmanaged(u8),
    allocator: std.mem.Allocator,
    seen: *std.StringHashMapUnmanaged(void),
    count: *usize,
    model_id: []const u8,
    created: i64,
    compatibility_level: []const u8,
) !void {
    const entry = try seen.getOrPut(allocator, model_id);
    if (entry.found_existing) return;
    if (count.* > 0) try buf.append(allocator, ',');
    try appendOpenAiModelEntry(
        buf,
        allocator,
        model_id,
        created,
        compatibility_level,
    );
    count.* += 1;
}

const DiscoveredModelListing = struct {
    entry_index: usize,
    manifest: manifest_mod.ModelManifest,
    reader_supported: bool,
    /// `@tagName(manifest.model_type)`, not the path-derived registry kind.
    kind: []const u8,
    /// Compatibility derived from the artifact and available runtime paths.
    compatibility_level: []const u8,

    fn deinit(self: *@This()) void {
        self.manifest.deinit();
        self.* = undefined;
    }
};

const ModelCounts = struct {
    embedders: usize = 0,
    rerankers: usize = 0,
    chunkers: usize = 0,
    generators: usize = 0,
    classifiers: usize = 0,
    rewriters: usize = 0,
    readers: usize = 0,
    transcribers: usize = 0,
    extractors: usize = 0,

    fn total(self: @This()) usize {
        return self.embedders +
            self.rerankers +
            self.chunkers +
            self.generators +
            self.classifiers +
            self.rewriters +
            self.readers +
            self.transcribers +
            self.extractors;
    }
};

const ReadinessInventorySnapshot = struct {
    counts: ModelCounts = .{},
    initialized: bool = false,
};

/// A tiny, lock-protected publication point for the relatively expensive model
/// inventory. Operational probes must never walk a shared model cache or parse
/// manifests: they copy this snapshot and return immediately.
const ReadinessInventory = struct {
    lock: std.atomic.Mutex = .unlocked,
    snapshot: ReadinessInventorySnapshot = .{},

    fn publish(self: *@This(), counts: ModelCounts) void {
        spinLock(&self.lock);
        defer self.lock.unlock();
        self.snapshot = .{ .counts = counts, .initialized = true };
    }

    fn load(self: *@This()) ReadinessInventorySnapshot {
        spinLock(&self.lock);
        defer self.lock.unlock();
        return self.snapshot;
    }
};

// External pull/install commands publish into the shared models directory
// without going through this process. Refresh often enough for operational
// state to converge, but avoid continuously reparsing a large remote cache.
const readiness_inventory_refresh_interval_ms: u64 = 60_000;

fn incrementModelCount(counts: *ModelCounts, task: []const u8) void {
    if (std.mem.eql(u8, task, "embedders")) counts.embedders += 1 else if (std.mem.eql(u8, task, "rerankers")) counts.rerankers += 1 else if (std.mem.eql(u8, task, "chunkers")) counts.chunkers += 1 else if (std.mem.eql(u8, task, "generators")) counts.generators += 1 else if (std.mem.eql(u8, task, "classifiers")) counts.classifiers += 1 else if (std.mem.eql(u8, task, "rewriters")) counts.rewriters += 1 else if (std.mem.eql(u8, task, "readers")) counts.readers += 1 else if (std.mem.eql(u8, task, "transcribers")) counts.transcribers += 1 else if (std.mem.eql(u8, task, "extractors")) counts.extractors += 1;
}

fn collectModelCounts(node: *Node, allocator: std.mem.Allocator, io: std.Io) ModelCounts {
    const task_names = [_][]const u8{
        "embedders",  "rerankers",   "chunkers",
        "generators", "classifiers", "extractors",
        "rewriters",  "readers",     "transcribers",
    };
    var counts = ModelCounts{};

    const ra = node.registry.allocator;
    const discovered = node.registry.discover(io) catch &[_]registry_mod.ModelEntry{};
    defer {
        for (discovered) |entry| {
            ra.free(entry.name);
            ra.free(entry.path);
        }
        if (discovered.len > 0) ra.free(discovered);
    }

    for (discovered) |entry| {
        if (!model_manager_mod.isModelDirPotentiallyLoadableInCurrentBuild(allocator, entry.path)) continue;

        var maybe_manifest: ?manifest_mod.ModelManifest = manifest_mod.loadFromDir(allocator, entry.path) catch null;
        defer if (maybe_manifest) |*man| man.deinit();

        const tasks = if (maybe_manifest) |*man| man.tasks else &.{};
        const capabilities = if (maybe_manifest) |*man| man.capabilities else &.{};
        const gliner_model_type = if (maybe_manifest) |*man| man.gliner_model_type else "";
        const zero_shot_classification = if (maybe_manifest) |*man| manifestSupportsZeroShotClassification(man) else false;

        for (task_names) |task| {
            if (std.mem.eql(u8, task, "chunkers")) continue;
            if (std.mem.eql(u8, task, "readers") and !readers_mod.isSupportedModelDir(allocator, entry.path)) continue;
            if (taskMatchesModelListing(task, @tagName(entry.kind), gliner_model_type, tasks, capabilities, zero_shot_classification)) {
                incrementModelCount(&counts, task);
            }
        }
    }

    node.model_manager.lockLoadedModels();
    defer node.model_manager.unlockLoadedModels();
    var it = node.model_manager.loaded.iterator();
    while (it.next()) |entry| {
        var already_listed = false;
        for (discovered) |d| {
            if (std.mem.eql(u8, d.path, entry.value_ptr.*.model_dir)) {
                already_listed = true;
                break;
            }
        }
        if (already_listed) continue;

        const model = entry.value_ptr.*;
        const model_task = @tagName(model.manifest.model_type);
        for (task_names) |task| {
            if (std.mem.eql(u8, task, "chunkers")) continue;
            if (taskMatchesModelListing(
                task,
                model_task,
                model.manifest.gliner_model_type,
                model.manifest.tasks,
                model.manifest.capabilities,
                manifestSupportsZeroShotClassification(&model.manifest),
            )) {
                incrementModelCount(&counts, task);
            }
        }
    }

    return counts;
}

fn collectDiscoveredModelCounts(models_dir: []const u8, allocator: std.mem.Allocator, io: std.Io) !ModelCounts {
    const task_names = [_][]const u8{
        "embedders",  "rerankers",   "chunkers",
        "generators", "classifiers", "extractors",
        "rewriters",  "readers",     "transcribers",
    };
    var counts = ModelCounts{};

    var registry = registry_mod.ModelRegistry.init(allocator, models_dir);
    defer registry.deinit();
    // The manifest is loaded below, so shallow discovery avoids parsing every
    // manifest twice during each refresh.
    const discovered = try registry.discoverShallow(io);
    defer {
        for (discovered) |entry| {
            allocator.free(entry.name);
            allocator.free(entry.path);
        }
        if (discovered.len > 0) allocator.free(discovered);
    }

    for (discovered) |entry| {
        // Read only listing metadata. Full manifest loading parses large GGUF
        // tokenizer tables and was the dominant cost of readiness discovery.
        var man = manifest_mod.loadListingFromDir(allocator, entry.path) catch continue;
        defer man.deinit();
        if (!model_manager_mod.isManifestPotentiallyLoadableInCurrentBuild(man)) continue;

        const zero_shot_classification = manifestSupportsZeroShotClassification(&man);

        for (task_names) |task| {
            if (std.mem.eql(u8, task, "chunkers")) continue;
            if (std.mem.eql(u8, task, "readers") and !readers_mod.isSupportedManifest(allocator, entry.path, man)) continue;
            if (taskMatchesModelListing(
                task,
                @tagName(man.model_type),
                man.gliner_model_type,
                man.tasks,
                man.capabilities,
                zero_shot_classification,
            )) {
                incrementModelCount(&counts, task);
            }
        }
    }

    return counts;
}

fn validateRequestModelIdentifier(raw: []const u8) !void {
    if (raw.len == 0 or
        std.fs.path.isAbsolute(raw) or
        std.mem.indexOfScalar(u8, raw, '\\') != null or
        std.mem.indexOfScalar(u8, raw, 0) != null)
    {
        return error.InvalidModelIdentifier;
    }

    const value = if (std.mem.startsWith(u8, raw, "hf:")) raw[3..] else raw;
    if (value.len == 0) return error.InvalidModelIdentifier;
    const colon = std.mem.indexOfScalar(u8, value, ':');
    const identifier = if (colon) |index| value[0..index] else value;
    if (colon) |index| {
        const variant = value[index + 1 ..];
        if (!registry_mod.modelVariantIsSafe(variant)) return error.InvalidModelIdentifier;
    }

    var components = std.mem.splitScalar(u8, identifier, '/');
    while (components.next()) |component| {
        if (component.len == 0 or std.mem.eql(u8, component, ".") or std.mem.eql(u8, component, "..")) {
            return error.InvalidModelIdentifier;
        }
    }
}

fn realPathExistingAlloc(allocator: std.mem.Allocator, io: std.Io, path: []const u8) ![]u8 {
    var buffer: [std.Io.Dir.max_path_bytes]u8 = undefined;
    const len = if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.realPathFileAbsolute(io, path, &buffer)
    else
        try std.Io.Dir.cwd().realPathFile(io, path, &buffer);
    return allocator.dupe(u8, buffer[0..len]);
}

fn pathHasComponentPrefix(path: []const u8, prefix: []const u8) bool {
    if (prefix.len == 0 or !std.mem.startsWith(u8, path, prefix)) return false;
    return path.len == prefix.len or prefix[prefix.len - 1] == std.fs.path.sep or path[prefix.len] == std.fs.path.sep;
}

fn discoveredContainsModelDir(discovered: []const registry_mod.ModelEntry, model_dir: []const u8) bool {
    for (discovered) |entry| {
        if (std.mem.eql(u8, entry.path, model_dir)) return true;
    }
    return false;
}

fn stringSliceContains(values: []const []const u8, needle: []const u8) bool {
    for (values) |value| {
        if (std.mem.eql(u8, value, needle)) return true;
    }
    return false;
}

/// Return the exact request identifier for an existing canonical model path.
/// Internal cache keys and paths outside models_dir deliberately have no
/// public identifier.
fn loadedModelRequestIdentifier(canonical_models_dir: []const u8, canonical_model_dir: []const u8) ?[]const u8 {
    if (!pathHasComponentPrefix(canonical_model_dir, canonical_models_dir) or
        canonical_model_dir.len == canonical_models_dir.len)
    {
        return null;
    }

    const relative_start = canonical_models_dir.len + @intFromBool(canonical_models_dir[canonical_models_dir.len - 1] != std.fs.path.sep);
    if (relative_start >= canonical_model_dir.len) return null;
    const identifier = canonical_model_dir[relative_start..];
    if (std.mem.indexOfScalar(u8, identifier, ':') != null) return null;
    for (identifier) |byte| {
        if (byte < 0x20 or byte == 0x7f) return null;
    }
    validateRequestModelIdentifier(identifier) catch return null;
    return identifier;
}

fn loadedModelRequestIdentifierAlloc(
    allocator: std.mem.Allocator,
    io: std.Io,
    canonical_models_dir: []const u8,
    canonical_model_dir: []const u8,
) !?[]u8 {
    const managed_identifier = registry_mod.managedModelRequestNameAlloc(
        allocator,
        io,
        canonical_model_dir,
    ) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => null,
    };
    if (managed_identifier) |identifier| return identifier;

    const relative = loadedModelRequestIdentifier(
        canonical_models_dir,
        canonical_model_dir,
    ) orelse return null;
    return try allocator.dupe(u8, relative);
}

test "loaded model listing uses model directories and safe relative identifiers" {
    const discovered = [_]registry_mod.ModelEntry{.{
        .name = "owner/model",
        .kind = .embedder,
        .path = "models/owner/model",
        .variant = "auto",
    }};

    try std.testing.expect(discoveredContainsModelDir(&discovered, "models/owner/model"));
    try std.testing.expect(!discoveredContainsModelDir(&discovered, "models/owner/model\nbackend=metal"));
    // Canonical comparison closes the relative-vs-absolute spelling gap.
    try std.testing.expect(!discoveredContainsModelDir(&discovered, "/srv/models/owner/model"));
    try std.testing.expect(stringSliceContains(&.{ "/srv/models/owner/model", "/srv/models/other" }, "/srv/models/owner/model"));
    try std.testing.expectEqualStrings(
        "embedders/owner/model",
        loadedModelRequestIdentifier("/srv/models", "/srv/models/embedders/owner/model").?,
    );
    try std.testing.expect(loadedModelRequestIdentifier("/srv/models", "/srv/models") == null);
    try std.testing.expect(loadedModelRequestIdentifier("/srv/models", "/srv/models-old/owner/model") == null);
    try std.testing.expect(loadedModelRequestIdentifier("/srv/models", "/tmp/private-model") == null);
    try std.testing.expect(loadedModelRequestIdentifier("/srv/models", "/srv/models/owner/model\nbackend=metal") == null);
    try std.testing.expect(loadedModelRequestIdentifier("/srv/models", "/srv/models/owner/model:q4") == null);
}

test "loaded model listing preserves managed variant identity without exposing cache leaves" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.createDirPath(io, "models/owner/model--antfly-0123456789abcdef");
    try tmp.dir.writeFile(io, .{
        .sub_path = "models/owner/model--antfly-0123456789abcdef/model.gguf",
        .data = "decoder",
    });
    try tmp.dir.writeFile(io, .{
        .sub_path = "models/owner/model--antfly-0123456789abcdef/.antfly-download-complete.json",
        .data =
        \\{"version":2,"source":{"owner":"owner","name":"model","variant":"gguf:Q4_K_M"},"artifacts":[{"path":"model.gguf","size":7}]}
        ,
    });

    const models_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..], "models" });
    defer allocator.free(models_dir);
    const model_dir = try std.fs.path.join(allocator, &.{ models_dir, "owner", "model--antfly-0123456789abcdef" });
    defer allocator.free(model_dir);

    const identifier = (try loadedModelRequestIdentifierAlloc(
        allocator,
        io,
        models_dir,
        model_dir,
    )).?;
    defer allocator.free(identifier);
    try std.testing.expectEqualStrings("owner/model:gguf:Q4_K_M", identifier);
}

const RequestModelResolutionErrorKind = enum { invalid, missing, internal };

const RequestWorkTestCounters = struct {
    model_resolution_attempts: usize = 0,
    model_load_attempts: usize = 0,
    media_fetch_attempts: usize = 0,
};
// Ordering tests assert real side-effect boundaries. Both this storage and all
// mutations become void/dead code in production builds.
var request_work_test_counters: if (builtin.is_test) RequestWorkTestCounters else void = if (builtin.is_test) .{} else {};

fn resetRequestWorkTestCounters() void {
    if (comptime builtin.is_test) request_work_test_counters = .{};
}

fn requestModelResolutionErrorKind(err: anyerror) RequestModelResolutionErrorKind {
    return switch (err) {
        error.InvalidModelIdentifier, error.ModelOutsideModelsDir => .invalid,
        error.ModelNotFound, error.ModelNotSpecified, error.FileNotFound, error.NotDir => .missing,
        else => .internal,
    };
}

const ExtractionResolutionFailure = enum {
    not_found,
    resource_exhausted,
    internal,
};

fn classifyExtractionResolutionFailure(err: anyerror) ExtractionResolutionFailure {
    return switch (err) {
        error.ModelNotFound => .not_found,
        error.OutOfMemory => .resource_exhausted,
        else => .internal,
    };
}

fn compatibilityBackendsForSessionManager(
    session_manager: *const backends_mod.SessionManager,
    required_scratch: *[1]backends_mod.BackendType,
) ![]const backends_mod.BackendType {
    return session_manager.requiredBackendCandidates(
        session_manager.preferred_backends,
        required_scratch,
    );
}

test "compatibility backend candidates honor required policy" {
    var session_manager = backends_mod.SessionManager.init(std.testing.allocator);
    session_manager.preferred_backends = &.{ .native, .cuda };
    session_manager.required_backend = .cuda;
    session_manager.required_backend_invalid = false;
    var required_scratch: [1]backends_mod.BackendType = undefined;
    try std.testing.expectEqualSlices(
        backends_mod.BackendType,
        &.{.cuda},
        try compatibilityBackendsForSessionManager(&session_manager, &required_scratch),
    );

    session_manager.required_backend = .pjrt;
    try std.testing.expectError(
        error.RequiredBackendUnavailable,
        compatibilityBackendsForSessionManager(&session_manager, &required_scratch),
    );
}

pub const Node = struct {
    config: NodeConfig,
    allocator: std.mem.Allocator,
    session_manager: backends_mod.SessionManager,
    model_manager: model_manager_mod.ModelManager,
    registry: registry_mod.ModelRegistry,
    extraction_reader_resolver: extractors_mod.ReaderResolver,
    tabular_registry: tabular_mod.registry.Registry,
    embed_cache: cache_mod.ResultCache([]const f32),
    metrics: metrics_mod.Metrics,
    inference_admission: inference_admission_mod.InferenceAdmission,
    admission_metrics_mutex: std.atomic.Mutex = .unlocked,
    compatibility_cache: std.StringHashMapUnmanaged(*CachedCompatibility) = .empty,
    compatibility_cache_lock: std.atomic.Mutex = .unlocked,
    readiness_inventory: ReadinessInventory = .{},
    readiness_refresh_group: std.Io.Group = .init,
    readiness_refresh_io: ?std.Io = null,
    readiness_refresh_started: bool = false,
    /// Runtime JIT qualification is limited to the single-threaded startup phase.
    request_surfaces_published: bool = false,
    /// Set after configured preloads, including declared optional sessions,
    /// have completed successfully.
    startup_preloads_materialized: bool = false,

    pub const DirectSparseEmbedding = sparse_embedding_mod.SparseVector;

    /// Borrowed inputs for the embedded dense-embedding API. The caller keeps
    /// every slice alive until the synchronous call returns; the Node never
    /// stores or frees them.
    pub const DirectDenseEmbedPart = union(enum) {
        text: []const u8,
        image_url: []const u8,
        media: struct {
            mime_type: []const u8,
            data: []const u8,
        },
    };

    pub const DirectGeneratePreflight = struct {
        text_bytes: usize = 0,
        encoded_media_bytes: usize = 0,
        decoded_media_bytes: usize = 0,
        media_count: usize = 0,
        image_count: usize = 0,
        has_audio: bool = false,
    };

    /// Owns one direct-generation admission from source preflight through
    /// media conversion and model execution. Call deinit exactly once when
    /// ownership ends; repeated calls are harmless to keep error cleanup safe.
    pub const DirectGenerateAdmission = struct {
        node: ?*Node,
        reserved_units: usize,
        resident_bytes: usize,
        expected: DirectGeneratePreflight,
        max_tokens: i32,
        prepared: bool = false,

        fn prepareMessages(self: *DirectGenerateAdmission, messages: []const generation.Message) !void {
            const node = self.node orelse return error.InvalidGenerationAdmission;
            if (self.prepared) return error.InvalidGenerationAdmission;

            const actual = try directGeneratePreflightForMessages(messages);
            if (actual.text_bytes != self.expected.text_bytes or
                actual.decoded_media_bytes != self.expected.decoded_media_bytes or
                actual.media_count != self.expected.media_count or
                actual.image_count != self.expected.image_count or
                actual.has_audio != self.expected.has_audio)
            {
                return error.InvalidGenerationAdmission;
            }

            if (actual.image_count > 0) {
                const max_dimension = effectiveRequestContentSecurity(node).max_image_dimension;
                const decoded_pixel_cap = readDecodedPixelCapForLimits(
                    actual.image_count,
                    node.inference_admission.capacity,
                    max_dimension,
                    self.resident_bytes,
                );
                const image_admission = ReadRequestAdmission{
                    .units = self.reserved_units,
                    .byte_cap = self.resident_bytes,
                    .resident_byte_cap = self.resident_bytes,
                    .decoded_pixel_cap = decoded_pixel_cap,
                };
                var decoded_budget = ReadDecodedImageBudget.init(image_admission, max_dimension);
                for (messages) |message| {
                    if (message.image_bytes) |images| {
                        for (images) |image_bytes| try decoded_budget.addImage(image_bytes);
                    }
                }

                const required_units = @max(self.reserved_units, decoded_budget.requiredUnits());
                try node.growAdmissionUnits(self.reserved_units, required_units);
                self.reserved_units = required_units;
            }
            self.prepared = true;
        }

        pub fn deinit(self: *DirectGenerateAdmission) void {
            const node = self.node orelse return;
            self.node = null;
            node.releaseAdmissionUnits(self.reserved_units);
            node.metrics.decActive();
        }
    };

    pub fn init(allocator: std.mem.Allocator, config: NodeConfig) !Node {
        if (config.http_max_connections == 0 or config.http_max_request_tasks == 0)
            return error.InvalidHttpTransportCapacity;
        if (config.kernel_jit.profile_capture_only) {
            return error.KernelJitProfileCaptureUnsupportedInServer;
        }
        try config.kernel_jit.validate();
        try config.prompt_cache.validate();
        var session_manager = backends_mod.SessionManager.init(allocator);
        try session_manager.validateRequiredBackendPolicy();
        session_manager.kernel_jit = config.kernel_jit;
        try graph_mod.kernel_jit.validateMetalProfileBackend(
            backends_mod.BackendType.metal.available(),
            false,
            config.kernel_jit.qualified_profile_path != null,
        );
        if (config.kernel_jit.mode.compiles()) {
            if (session_manager.bestKernelJitBackend()) |backend| {
                std.log.info(
                    "kernel_jit_backend mode={s} preferred_backend={s}",
                    .{ @tagName(config.kernel_jit.mode), @tagName(backend) },
                );
            } else {
                std.log.warn(
                    "kernel_jit_backend mode={s} preferred_backend=none; backend preference is configured separately (set ANTFLY_INFERENCE_PREFERRED_BACKEND=cuda for CUDA)",
                    .{@tagName(config.kernel_jit.mode)},
                );
            }
        }
        var node: Node = .{
            .config = config,
            .allocator = allocator,
            .session_manager = session_manager,
            .model_manager = model_manager_mod.ModelManager.init(allocator, session_manager),
            .registry = registry_mod.ModelRegistry.init(allocator, config.models_dir),
            .extraction_reader_resolver = extractors_mod.ReaderResolver.init(allocator),
            .tabular_registry = tabular_mod.registry.Registry.init(allocator),
            .embed_cache = cache_mod.ResultCache([]const f32).init(allocator, 120_000),
            .metrics = metrics_mod.Metrics.default,
            .inference_admission = inference_admission_mod.InferenceAdmission.init(config.max_concurrent_requests),
            .compatibility_cache = .empty,
        };
        try node.model_manager.configureResourceOwnership(config.resource_ownership);
        node.model_manager.configureProcessMemoryLimit(
            config.process_memory_limit_bytes,
            config.process_memory_limit_provenance,
        );
        node.model_manager.configureServingPolicy(.{
            .allow_unknown = config.allow_unknown_models,
        });
        node.model_manager.configureModelCache(
            config.keep_alive_ms,
            config.max_loaded_models,
        );
        node.model_manager.configureAdmissionLimits(.{
            .host_limit_bytes = config.generation_budget_overrides.host_limit_bytes,
            .backend_limit_bytes = config.generation_budget_overrides.backend_limit_bytes,
            .combined_limit_bytes = config.generation_budget_overrides.combined_limit_bytes,
            .kv_limit_bytes = config.generation_budget_overrides.kv_limit_bytes,
            .scratch_limit_bytes = config.generation_budget_overrides.scratch_limit_bytes,
        });
        if (!builtin.is_test) {
            std.log.info(
                "inference resource policy ownership={s} process_memory_limit_bytes={d} process_memory_limit_provenance={s}",
                .{
                    @tagName(config.resource_ownership),
                    config.process_memory_limit_bytes,
                    @tagName(config.process_memory_limit_provenance),
                },
            );
        }
        try node.model_manager.configureTokenizerCaches(config.tokenizer_cache);
        node.model_manager.tokenizer_parallel_bpe_config =
            config.tokenizer_parallel_bpe;
        node.updateAdmissionMetrics();
        return node;
    }

    fn defaultGenerationLimits(
        self: *const Node,
        backend: runtime.tier.memory.BackendClass,
    ) runtime.tier.memory.Limits {
        return runtime.tier.memory.defaultLimitsForBackendWithProcessLimit(
            backend,
            self.config.process_memory_limit_bytes,
        );
    }

    /// Attach process-wide tokenizer-cache admission before loading models.
    pub fn configureTokenizerCaches(
        self: *Node,
        config: hf_tokenizer_mod.HfTokenizer.BpeCacheConfig,
    ) !void {
        try self.model_manager.configureTokenizerCaches(config);
        self.config.tokenizer_cache = self.model_manager.tokenizer_cache_config;
        // NodeConfig is public policy, not an owning runtime capability.
        self.config.tokenizer_cache.resource_budget = null;
    }

    pub fn configureExternalResourceBudgets(
        self: *Node,
        admission_budget: runtime.tier.memory.AdmissionResourceBudget,
        tokenizer_budget: hf_tokenizer_mod.HfTokenizer.BpeCacheResourceBudget,
    ) !void {
        try self.model_manager.configureExternalResourceBudgets(
            admission_budget,
            tokenizer_budget,
        );
        self.config.tokenizer_cache = self.model_manager.tokenizer_cache_config;
        self.config.tokenizer_cache.resource_budget = null;
    }

    pub fn configureForcedRunAdmissionDenialsForTesting(
        self: *Node,
        count: usize,
    ) void {
        self.model_manager.configureForcedRunAdmissionDenialsForTesting(count);
    }

    pub fn configureForcedRunAdmissionDenialsFromEnvironmentForTesting(
        self: *Node,
    ) void {
        const count = platform.env.getenvUsize(
            "ANTFLY_INFERENCE_TEST_FORCE_RUN_ADMISSION_DENIALS",
        ) orelse return;
        if (count == 0) return;
        std.log.warn(
            "enabling test-only forced inference run admission denials count={d}",
            .{count},
        );
        self.configureForcedRunAdmissionDenialsForTesting(count);
    }

    /// Configure the std.Io tokenizer scheduler and optional consumer-local
    /// tables before model load. Table memory is admitted by the cache
    /// resource budget configured above.
    pub fn configureTokenizerParallelBpe(
        self: *Node,
        config: hf_tokenizer_mod.HfTokenizer.ParallelBpeConfig,
    ) !void {
        try self.model_manager.configureTokenizerParallelBpe(config);
        self.config.tokenizer_parallel_bpe = config;
    }

    pub fn deinit(self: *Node) void {
        // The refresher borrows Node, its allocator, and the models directory.
        // Cancel and join it before releasing any of those dependencies.
        if (self.readiness_refresh_io) |io| self.readiness_refresh_group.cancel(io);
        self.model_manager.deinit();
        self.registry.deinit();
        self.extraction_reader_resolver.deinit();
        self.tabular_registry.deinit();
        self.embed_cache.deinit();
        var compatibility_it = self.compatibility_cache.iterator();
        while (compatibility_it.next()) |entry| {
            entry.value_ptr.*.release();
            self.allocator.free(entry.key_ptr.*);
        }
        self.compatibility_cache.deinit(self.allocator);
    }

    /// Derive artifact compatibility once per immutable artifact signature. Discovery
    /// calls this repeatedly, so caching prevents GGUF/ONNX metadata inspection from
    /// becoming request-path work while still invalidating when a sidecar is replaced.
    fn compatibilitySummaryForDir(
        self: *Node,
        allocator: std.mem.Allocator,
        model_path: []const u8,
    ) !CompatibilitySummary {
        var required_backend_scratch: [1]backends_mod.BackendType = undefined;
        const compatibility_backends = try compatibilityBackendsForSessionManager(
            &self.session_manager,
            &required_backend_scratch,
        );
        var io_impl: ?std.Io.Threaded = null;
        defer if (io_impl) |*threaded| threaded.deinit();
        const io = self.session_manager.io orelse blk: {
            io_impl = std.Io.Threaded.init(allocator, .{});
            break :blk io_impl.?.io();
        };

        // A deployment can replace several sidecars and artifacts without an
        // atomic directory snapshot. Derive and validate the manifest twice so
        // the cached assessment is published only when both parsed facts and
        // artifact identities describe the same stable generation.
        const max_snapshot_attempts = 3;
        var attempt: usize = 0;
        while (attempt < max_snapshot_attempts) : (attempt += 1) {
            var man = try manifest_mod.loadListingFromDir(allocator, model_path);
            defer man.deinit();

            spinLock(&self.compatibility_cache_lock);
            const cached = self.compatibility_cache.get(model_path);
            if (cached) |entry| entry.retain();
            self.compatibility_cache_lock.unlock();
            if (cached) |entry| {
                const cached_signature = computeCompatibilitySignatureWithDependencies(
                    allocator,
                    io,
                    model_path,
                    &man,
                    entry.external_paths,
                    entry.external_paths_valid,
                ) catch |err| {
                    entry.release();
                    return err;
                };
                const cache_hit = std.mem.eql(
                    u8,
                    entry.signature[0..],
                    cached_signature[0..],
                );
                const cached_summary = entry.summary;
                entry.release();
                if (cache_hit) return cached_summary;
            }

            var dependencies = try collectCompatibilityOnnxDependencies(allocator, &man);
            defer dependencies.deinit(allocator);
            const artifact_signature = try computeCompatibilitySignatureWithDependencies(
                allocator,
                io,
                model_path,
                &man,
                dependencies.paths.items,
                dependencies.valid,
            );

            const summary = try model_manager_mod.compatibilitySummaryForBackends(
                allocator,
                model_path,
                &man,
                compatibility_backends,
            );

            var verification_manifest = try manifest_mod.loadListingFromDir(
                allocator,
                model_path,
            );
            defer verification_manifest.deinit();
            var verification_dependencies = try collectCompatibilityOnnxDependencies(
                allocator,
                &verification_manifest,
            );
            defer verification_dependencies.deinit(allocator);
            const verification_signature = try computeCompatibilitySignatureWithDependencies(
                allocator,
                io,
                model_path,
                &verification_manifest,
                verification_dependencies.paths.items,
                verification_dependencies.valid,
            );
            if (!std.mem.eql(
                u8,
                artifact_signature[0..],
                verification_signature[0..],
            )) continue;

            const new_cached = try CachedCompatibility.create(
                self.allocator,
                artifact_signature,
                summary,
                &verification_dependencies,
            );
            var replaced: ?*CachedCompatibility = null;
            spinLock(&self.compatibility_cache_lock);
            if (self.compatibility_cache.getPtr(model_path)) |cached_ptr| {
                replaced = cached_ptr.*;
                cached_ptr.* = new_cached;
            } else {
                const owned_path = self.allocator.dupe(u8, model_path) catch |err| {
                    self.compatibility_cache_lock.unlock();
                    new_cached.release();
                    return err;
                };
                self.compatibility_cache.put(
                    self.allocator,
                    owned_path,
                    new_cached,
                ) catch |err| {
                    self.allocator.free(owned_path);
                    self.compatibility_cache_lock.unlock();
                    new_cached.release();
                    return err;
                };
            }
            self.compatibility_cache_lock.unlock();
            if (replaced) |old| old.release();
            return summary;
        }
        return error.ModelArtifactsChanging;
    }

    pub fn attachIo(self: *Node, io: std.Io) void {
        self.session_manager.io = io;
        self.model_manager.attachIo(io);
    }

    fn refreshReadinessInventory(self: *Node, io: std.Io) !void {
        const counts = try collectDiscoveredModelCounts(
            self.config.models_dir,
            self.allocator,
            io,
        );
        self.readiness_inventory.publish(counts);
    }

    fn readinessRefreshLoop(self: *Node, io: std.Io) std.Io.Cancelable!void {
        while (true) {
            try io.sleep(
                std.Io.Duration.fromMilliseconds(readiness_inventory_refresh_interval_ms),
                .awake,
            );
            self.refreshReadinessInventory(io) catch |err| {
                // Preserve the last-known-good snapshot through transient cache
                // or publication failures. A never-successful initialization
                // remains not-ready.
                std.log.warn(
                    "readiness model inventory refresh failed err={s}",
                    .{@errorName(err)},
                );
            };
        }
    }

    /// Build the initial readiness view before listener publication and start
    /// one runtime-owned refresher for model changes made by external pull
    /// commands. Repeated calls are harmless (the runtime cannot be replaced).
    pub fn startReadinessInventory(self: *Node, io: std.Io) void {
        if (self.readiness_refresh_started) return;
        self.refreshReadinessInventory(io) catch |err| {
            // Liveness should still come up when a model volume is temporarily
            // unavailable. The empty, uninitialized snapshot fails readiness
            // closed until a later refresh succeeds.
            std.log.warn(
                "initial readiness model inventory failed err={s}",
                .{@errorName(err)},
            );
        };
        self.readiness_refresh_started = true;
        self.readiness_refresh_io = io;
        self.readiness_refresh_group.async(io, readinessRefreshLoop, .{ self, io });
    }

    pub fn detachPromptCacheResourceUsageObserver(self: *Node) void {
        self.config.prompt_cache_resource_usage_observer = null;
        self.model_manager.detachPromptCacheResourceUsageObserver();
    }

    pub fn embedDenseTextsDirect(
        self: *Node,
        allocator: std.mem.Allocator,
        model_name: []const u8,
        texts: []const []const u8,
    ) ![][]f32 {
        var io_impl = std.Io.Threaded.init(allocator, .{});
        defer io_impl.deinit();
        return try self.embedDenseTextsDirectWithContext(allocator, io_impl.io(), null, model_name, texts);
    }

    pub fn embedDenseTextsDirectWithContext(
        self: *Node,
        allocator: std.mem.Allocator,
        io: std.Io,
        deadline_ns: ?u64,
        model_name: []const u8,
        texts: []const []const u8,
    ) ![][]f32 {
        if (texts.len == 0) return try allocator.alloc([]f32, 0);
        try ensureDirectEmbeddingDeadline(deadline_ns);
        try self.acquireAdmissionUnits(1);
        defer self.releaseAdmission();
        self.metrics.incRequest("embed.local");
        defer self.metrics.decActive();

        const model_path = try self.resolveModelPath(io, if (model_name.len > 0) model_name else null, "embedders");
        defer self.allocator.free(model_path);
        try ensureDirectEmbeddingDeadline(deadline_ns);
        var admission_manifest = try manifest_mod.loadFromDir(allocator, model_path);
        defer admission_manifest.deinit();
        if (admission_manifest.hasCapability("sparse")) return error.UnsupportedEmbeddingProvider;

        const Attempt = struct {
            allocator: std.mem.Allocator,
            deadline_ns: ?u64,
            texts: []const []const u8,
            vectors: ?[][]f32 = null,

            fn run(ctx: *anyopaque, model: *model_manager_mod.LoadedModel) !void {
                const attempt: *@This() = @ptrCast(@alignCast(ctx));
                if (model.manifest.hasCapability("sparse")) return error.UnsupportedEmbeddingProvider;
                var asset_lease = model.acquireEmbeddingAssetLease(false);
                defer asset_lease.release();
                const vectors = try embedDenseTextsOnLoadedModel(
                    attempt.allocator,
                    attempt.deadline_ns,
                    model,
                    attempt.texts,
                );
                asset_lease.release();
                errdefer freeDirectDenseVectors(attempt.allocator, vectors);
                try ensureDirectEmbeddingDeadline(attempt.deadline_ns);
                attempt.vectors = vectors;
            }
        };
        var attempt = Attempt{
            .allocator = allocator,
            .deadline_ns = deadline_ns,
            .texts = texts,
        };
        try runLoadedEmbeddingRuntimeWithRecovery(self, model_path, .{}, &attempt, Attempt.run);
        return attempt.vectors.?;
    }

    fn embedDenseTextsOnLoadedModel(
        allocator: std.mem.Allocator,
        deadline_ns: ?u64,
        model: *model_manager_mod.LoadedModel,
        texts: []const []const u8,
    ) ![][]f32 {
        try model.ensureEmbeddingAssets(true, false, false);
        try ensureDirectEmbeddingDeadline(deadline_ns);
        var pipeline = model.embeddingPipeline(allocator);
        return pipeline.embed(texts);
    }

    pub fn embedSparseTextsDirect(
        self: *Node,
        allocator: std.mem.Allocator,
        model_name: []const u8,
        texts: []const []const u8,
    ) ![]DirectSparseEmbedding {
        if (texts.len == 0) return try allocator.alloc(DirectSparseEmbedding, 0);
        try self.acquireAdmissionUnits(1);
        defer self.releaseAdmission();
        self.metrics.incRequest("embed_sparse.local");
        defer self.metrics.decActive();

        var io_impl = std.Io.Threaded.init(allocator, .{});
        defer io_impl.deinit();

        const model_path = try self.resolveModelPath(io_impl.io(), if (model_name.len > 0) model_name else null, "embedders");
        defer self.allocator.free(model_path);
        const Attempt = struct {
            allocator: std.mem.Allocator,
            texts: []const []const u8,
            vectors: ?[]DirectSparseEmbedding = null,

            fn run(ctx: *anyopaque, model: *model_manager_mod.LoadedModel) !void {
                const attempt: *@This() = @ptrCast(@alignCast(ctx));
                if (!model.manifest.hasCapability("sparse")) return error.UnsupportedEmbeddingProvider;
                var pipeline = sparse_embedding_mod.SparseEmbeddingPipeline{
                    .allocator = attempt.allocator,
                    .session = model.session,
                    .tok = model.getTokenizer(),
                    .config = sparse_embedding_mod.SparseEmbeddingConfig.fromManifest(&model.manifest),
                    .execution_lock = model.embeddingExecutionLock(),
                };
                attempt.vectors = try pipeline.embed(attempt.texts);
            }
        };
        var attempt = Attempt{ .allocator = allocator, .texts = texts };
        try runLoadedEmbeddingRuntimeWithRecovery(self, model_path, .{}, &attempt, Attempt.run);
        return attempt.vectors.?;
    }

    pub fn rerankTextsDirect(
        self: *Node,
        allocator: std.mem.Allocator,
        model_name: []const u8,
        query: []const u8,
        documents: []const []const u8,
    ) ![]f32 {
        if (documents.len == 0) return try allocator.alloc(f32, 0);
        try self.acquireAdmissionUnits(1);
        defer self.releaseAdmission();
        self.metrics.incRequest("rerank.local");
        defer self.metrics.decActive();

        var io_impl = std.Io.Threaded.init(allocator, .{});
        defer io_impl.deinit();

        const model_path = try self.resolveModelPath(io_impl.io(), if (model_name.len > 0) model_name else null, "rerankers");
        var model_handle = try self.model_manager.acquireFromDir(model_path);
        defer model_handle.release();
        const model = model_handle.get();
        var pipeline = model.rerankingPipeline(allocator);
        return try pipeline.rerank(query, documents);
    }

    pub fn generateTextDirect(
        self: *Node,
        allocator: std.mem.Allocator,
        model_name: []const u8,
        roles: []const []const u8,
        contents: []const []const u8,
    ) ![]u8 {
        if (roles.len != contents.len) return error.InvalidGenerationRequest;
        if (roles.len == 0) return error.InvalidGenerationRequest;

        var messages = try allocator.alloc(generation.Message, roles.len);
        defer allocator.free(messages);
        for (roles, contents, 0..) |role, content, i| {
            messages[i] = .{
                .role = role,
                .content = content,
            };
        }

        return self.generateMessagesDirectMaxTokens(allocator, model_name, messages, 256, null, false, null, false, null);
    }

    pub fn generateMessagesDirect(
        self: *Node,
        allocator: std.mem.Allocator,
        model_name: []const u8,
        messages: []const generation.Message,
    ) ![]u8 {
        return self.generateMessagesDirectMaxTokens(allocator, model_name, messages, 256, null, false, null, false, null);
    }

    pub fn beginDirectGenerateAdmission(
        self: *Node,
        preflight: DirectGeneratePreflight,
        max_tokens: i32,
    ) !DirectGenerateAdmission {
        if (max_tokens < 1 or
            preflight.image_count > preflight.media_count or
            preflight.has_audio != (preflight.media_count > preflight.image_count) or
            (preflight.media_count == 0 and
                (preflight.encoded_media_bytes != 0 or preflight.decoded_media_bytes != 0)))
        {
            return error.InvalidGenerationRequest;
        }

        const resident_bytes = std.math.add(
            usize,
            preflight.encoded_media_bytes,
            preflight.decoded_media_bytes,
        ) catch return error.RemoteContentTooLarge;
        if (resident_bytes > requestMediaMaxBytes(self)) return error.RemoteContentTooLarge;

        const audio_working_bytes = if (preflight.has_audio)
            default_max_audio_decode_working_bytes
        else
            0;
        const peak_bytes = std.math.add(usize, resident_bytes, audio_working_bytes) catch
            std.math.maxInt(usize);
        if (preflight.has_audio and self.inference_admission.capacity != 0) {
            const capacity_bytes = std.math.mul(
                usize,
                self.inference_admission.capacity,
                read_admission_bytes_per_unit,
            ) catch std.math.maxInt(usize);
            if (peak_bytes > capacity_bytes) return error.AudioTooLarge;
        }
        const generation_units = estimateGenerateAdmissionUnitsFromShape(
            preflight.text_bytes,
            preflight.media_count,
            max_tokens,
        );
        const byte_units = @max(
            @as(usize, 1),
            admissionUnitsFor(peak_bytes, read_admission_bytes_per_unit),
        );
        const reserved_units = @max(generation_units, byte_units);

        try self.acquireAdmissionUnits(reserved_units);
        self.metrics.incRequest("generate.local");
        return .{
            .node = self,
            .reserved_units = reserved_units,
            .resident_bytes = resident_bytes,
            .expected = preflight,
            .max_tokens = max_tokens,
        };
    }

    pub fn generateMessagesDirectAdmitted(
        self: *Node,
        allocator: std.mem.Allocator,
        model_name: []const u8,
        messages: []const generation.Message,
        admission: *DirectGenerateAdmission,
    ) ![]u8 {
        return self.generateMessagesDirectWithAdmission(
            allocator,
            model_name,
            messages,
            admission,
            null,
            false,
            null,
            false,
            null,
        );
    }

    const DirectGenerateTiming = struct {
        resolve_ms: u64 = 0,
        load_ms: u64 = 0,
        setup_ms: u64 = 0,
        generate_ms: u64 = 0,
        total_ms: u64 = 0,
    };

    const NativePromptTokenCount = struct {
        token_count: usize,
        media_admission: generation.NativeGenerationMediaAdmission,
    };

    fn countPromptTokens(
        allocator: std.mem.Allocator,
        model_dir: []const u8,
        model: *model_manager_mod.LoadedModel,
        gpt_config: gpt_model_mod.Config,
        messages: []const generation.Message,
        max_tokens: i32,
    ) !NativePromptTokenCount {
        const prompt = if (model.chat_tmpl) |ct|
            try ct.apply(allocator, messages, true)
        else
            try generation.formatMessages(allocator, messages);
        defer allocator.free(prompt);

        const media_admission = try generation.nativeGenerationMediaAdmission(
            allocator,
            model_dir,
            messages,
            gpt_config,
        );
        const preliminary_media_allowance = generation.nativeGenerationPreliminaryMediaTokenAllowance(messages, gpt_config);
        const prompt_token_limit = try generation.nativeGenerationPromptTokenLimit(
            gpt_config,
            null,
            @intCast(@max(max_tokens, 1)),
            0,
            preliminary_media_allowance,
        );
        var encoded = try generation.encodeNativeGenerationPrompt(
            model.getTokenizer(),
            allocator,
            prompt,
            prompt_token_limit,
            model.manifest.add_bos_token,
            model.manifest.bos_token,
        );
        defer encoded.deinit();

        var prompt_tokens: usize = 0;
        while (prompt_tokens < encoded.attention_mask.len and encoded.attention_mask[prompt_tokens] != 0) : (prompt_tokens += 1) {}
        if (prompt_tokens == 0) return error.EmptyPrompt;
        return .{
            .token_count = std.math.add(usize, prompt_tokens, media_admission.token_allowance) catch
                return error.PromptTooLong,
            .media_admission = media_admission,
        };
    }

    fn generateMessagesDirectMaxTokens(
        self: *Node,
        allocator: std.mem.Allocator,
        model_name: []const u8,
        messages: []const generation.Message,
        max_tokens: i32,
        preferred_backends: ?[]const backends_mod.BackendType,
        cache_default_alias: bool,
        timing: ?*DirectGenerateTiming,
        pin_after_success: bool,
        a4b_request: ?ops.A4bInferenceRequest,
    ) ![]u8 {
        if (messages.len == 0) return error.InvalidGenerationRequest;
        const preflight = try directGeneratePreflightForMessages(messages);
        var admission = try self.beginDirectGenerateAdmission(preflight, max_tokens);
        defer admission.deinit();
        return self.generateMessagesDirectWithAdmission(
            allocator,
            model_name,
            messages,
            &admission,
            preferred_backends,
            cache_default_alias,
            timing,
            pin_after_success,
            a4b_request,
        );
    }

    fn generateMessagesDirectWithAdmission(
        self: *Node,
        allocator: std.mem.Allocator,
        model_name: []const u8,
        messages: []const generation.Message,
        admission: *DirectGenerateAdmission,
        preferred_backends: ?[]const backends_mod.BackendType,
        cache_default_alias: bool,
        timing: ?*DirectGenerateTiming,
        pin_after_success: bool,
        a4b_request: ?ops.A4bInferenceRequest,
    ) ![]u8 {
        if (messages.len == 0) return error.InvalidGenerationRequest;
        const admitted_node = admission.node orelse return error.InvalidGenerationAdmission;
        if (admitted_node != self) return error.InvalidGenerationAdmission;
        try admission.prepareMessages(messages);
        const max_tokens = admission.max_tokens;
        const started_at_ns = embedTimingNowNs();

        var io_impl = std.Io.Threaded.init(allocator, .{});
        defer io_impl.deinit();
        const io = io_impl.io();

        const model_path = self.resolveModelPath(io, if (model_name.len > 0) model_name else null, "generators") catch |err| {
            std.log.err("direct generator resolve failed model={s}: {s}", .{ model_name, @errorName(err) });
            return err;
        };
        defer self.allocator.free(model_path);
        const resolved_at_ns = embedTimingNowNs();
        var model_handle = if (a4b_request) |request|
            if (preferred_backends) |backends|
                try self.model_manager.acquireFromDirWithPreferredBackendsAndA4bRequest(
                    model_path,
                    backends,
                    cache_default_alias,
                    request,
                )
            else
                try self.model_manager.acquireFromDirWithA4bRequest(model_path, request)
        else if (preferred_backends) |backends|
            try self.model_manager.acquireFromDirWithPreferredBackends(model_path, backends, cache_default_alias)
        else
            try self.model_manager.acquireFromDir(model_path);
        defer model_handle.release();
        const model = model_handle.get();
        const loaded_at_ns = embedTimingNowNs();
        if (timing != null) {
            std.log.info("direct generator loaded model={s} backend={s}", .{ model_name, @tagName(model.session.backend()) });
        }
        model.lockNativeGeneration(io);
        defer model.unlockNativeGeneration();
        const gpt_config = session_factory.getGptConfig(model.session) orelse return error.UnsupportedGeneratorProvider;
        const backend_kind: runtime.kv.pool.BackendKind = switch (model.session.backend()) {
            .native => .native,
            .metal => .metal,
            .cuda => .cuda,
            .pjrt, .onnx, .wasm => return error.UnsupportedGeneratorProvider,
        };
        const kv_dtype = session_factory.recommendedKvDTypeForSession(model.session, backend_kind);
        const use_metal_whole_model = build_options.enable_metal and
            model.session.backend() == .metal and
            graph_mod.metal_executor.supportsSession(model.session) and
            !generation.NativeDecodeState.requiresDeepSeekV4CompressedCache(gpt_config);
        var generation_config = generation.GenerationConfig{
            .max_tokens = max_tokens,
        };
        const kv_capacity_policy = generation.generationKvCapacityPolicyForRoute(
            if (use_metal_whole_model) .metal_whole_model else .standard,
            gpt_config,
            generation_config,
            kv_dtype,
        );
        const budget_backend_class: runtime.tier.memory.BackendClass = switch (backend_kind) {
            .native => .cpu,
            .metal, .cuda => .gpu,
        };
        const budget_limits = self.config.generation_budget_overrides.apply(session_factory.widenBudgetLimitsForSession(
            model.session,
            self.defaultGenerationLimits(budget_backend_class),
        ));
        var run_budget = runtime.tier.memory.RunBudget.init(budget_limits);
        const prompt_estimate = try countPromptTokens(allocator, model_path, model, gpt_config, messages, max_tokens);
        const prompt_tokens = prompt_estimate.token_count;
        const budget_components = [_]runtime.tier.memory.GptGenerationBudgetComponent{
            .{
                .backend = backend_kind,
                .kv_dtype = kv_dtype,
                .config = gpt_config,
                .kv_capacity_policy = kv_capacity_policy,
            },
        };
        const direct_prefill_ceiling = @min(
            prompt_tokens,
            generation.nativeGenerationPrefillChunkCeiling(
                backend_kind,
                gpt_config,
                self.config.generation_batching.schedulerPolicy().max_idle_prefill_chunk_size,
            ),
        );
        const admitted_prefill_chunk = runtime.tier.memory.reserveGptGenerationAtLargestChunk(
            &run_budget,
            &budget_components,
            prompt_tokens,
            @intCast(@max(max_tokens, 1)),
            direct_prefill_ceiling,
        ) catch |err| {
            if (err == error.MemoryBudgetExceeded) {
                var buf: [512]u8 = undefined;
                std.log.warn("{s}", .{
                    session_factory.memoryBudgetExceededDetail(model.session, &run_budget, &buf) catch
                        "request exceeds native generation memory budget",
                });
            }
            return err;
        };
        generation_config.prefill_chunk_size = admitted_prefill_chunk;
        const resource_estimate = try runtime.tier.memory.estimateGptGenerationForKvPolicy(
            backend_kind,
            kv_dtype,
            gpt_config,
            prompt_tokens,
            @intCast(@max(max_tokens, 1)),
            admitted_prefill_chunk,
            kv_capacity_policy,
        );
        var admission_amounts = runtime.tier.memory.AdmissionAmounts.fromEstimate(resource_estimate);
        if (generation.messagesHaveImages(messages) or generation.messagesHaveAudio(messages)) {
            admission_amounts = try admission_amounts.merge(
                try model_manager_mod.projectorRunAdmissionAmounts(
                    model.manifest,
                    backend_kind,
                    prompt_estimate.media_admission,
                ),
            );
        }
        var admission_lease = try self.model_manager.acquireRunResourceAmounts(
            budget_backend_class,
            budget_limits,
            admission_amounts,
        );
        defer admission_lease.release();

        var kv_manager = runtime.kv.manager.KvManager.init(allocator);
        defer kv_manager.deinit();
        var cb = session_factory.getComputeBackendWithBudget(model.session, allocator, &run_budget) catch |err| {
            if (err == error.MemoryBudgetExceeded) {
                var buf: [512]u8 = undefined;
                std.log.warn("{s}", .{
                    session_factory.memoryBudgetExceededDetail(model.session, &run_budget, &buf) catch
                        "request exceeds native generation memory budget",
                });
            }
            return err;
        };
        defer cb.deinit();

        const kv_pool_config = generation.kvPoolConfig(backend_kind, kv_dtype, gpt_config, generationKvSlidingTrimForced());
        const pool_id = try kv_manager.addPool(kv_pool_config);
        var kv_storage = try runtime.kv.storage_runtime.KvStorageRuntime.init(allocator, kv_pool_config);
        defer kv_storage.deinit();
        try cb.provisionKvDeviceWriteHook(&kv_storage);
        var decode_state = generation.NativeDecodeState.initPaged(allocator, &kv_manager, pool_id, model.shared_moe_cache);
        decode_state.kv_storage = &kv_storage;
        defer decode_state.deinit();

        var pipeline = generation.NativeGenerationPipeline{
            .allocator = allocator,
            .io = io,
            .cb = cb,
            .session = model.session,
            .gpt_config = gpt_config,
            .kv_dtype = kv_dtype,
            .tokenizer = model.getTokenizer(),
            .add_bos_token = model.manifest.add_bos_token,
            .bos_token = model.manifest.bos_token,
            .chat_template = model.chat_tmpl,
            .print_timing = timing != null,
            .model_dir = model_path,
            .gguf_projector_path = model.manifest.gguf_projector_path,
            .decode_state = &decode_state,
            .graph_cache = if (use_metal_whole_model) &model.native_generation_graph_cache else null,
            .compiled_partition_backend = if (use_metal_whole_model) .metal else null,
            .compiled_attachment_target = if (use_metal_whole_model) .whole_model else .partitioned,
        };
        const debug_metal_timing = timing != null and use_metal_whole_model and platform.env.getenvBool("TERMITE_DEBUG_METAL_TIMING");
        if (debug_metal_timing) graph_mod.metal_executor.resetTimingStats();
        const setup_at_ns = embedTimingNowNs();
        if (timing != null) {
            std.log.info("direct generator starting generation model={s} backend={s}", .{ model_name, @tagName(model.session.backend()) });
        }
        // Snapshot CUDA op-profile counters before generation so the emitted
        // line below reflects this request alone (warmup/other requests excluded).
        const cuda_profile_before: ?session_factory.CudaRuntimeStats =
            if (comptime build_options.enable_cuda)
                (if (session_factory.cudaOpProfileLoggingEnabled()) session_factory.getCudaRuntimeStats(model.session) else null)
            else
                null;
        var result = pipeline.generate(messages, generation_config) catch |err| {
            if (comptime build_options.enable_cuda) session_factory.drainCudaProfile(model.session);
            if (err == error.MemoryBudgetExceeded)
                logMemoryBudgetExceeded(model.session, &run_budget);
            std.log.err("direct generator generation failed model={s} backend={s}: {s}", .{
                model_name,
                @tagName(model.session.backend()),
                @errorName(err),
            });
            return err;
        };
        const generated_at_ns = embedTimingNowNs();
        defer result.deinit();
        // Drain buffered per-op profile events (single host sync, off the hot
        // path) and emit the same lines the CLI prints, so profiling works
        // against the warm server too.
        if (comptime build_options.enable_cuda) {
            if (cuda_profile_before) |before| {
                session_factory.drainCudaProfile(model.session);
                if (session_factory.getCudaRuntimeStats(model.session)) |after| {
                    const profile_delta = session_factory.cudaStatsDelta(after, before);
                    var cuda_profile_line_buf: [768]u8 = undefined;
                    std.log.info("{s}", .{session_factory.formatCudaDecodeProfileLine(&cuda_profile_line_buf, profile_delta)});
                    std.log.info("{s}", .{session_factory.formatCudaPrefillProfileLine(&cuda_profile_line_buf, profile_delta)});
                }
            }
        }
        if (debug_metal_timing) {
            if (model.native_generation_graph_cache.getSessionCompiledModelRuntime(.metal, .whole_model)) |runtime_model| {
                runtime_model.printDebugTiming();
            }
        }
        if (timing) |t| {
            t.* = .{
                .resolve_ms = elapsedMs(started_at_ns, resolved_at_ns),
                .load_ms = elapsedMs(resolved_at_ns, loaded_at_ns),
                .setup_ms = elapsedMs(loaded_at_ns, setup_at_ns),
                .generate_ms = elapsedMs(setup_at_ns, generated_at_ns),
                .total_ms = elapsedMs(started_at_ns, generated_at_ns),
            };
        }
        const text = try allocator.dupe(u8, result.text);
        if (pin_after_success) model_handle.pin();
        return text;
    }

    pub fn warmConfiguredModels(self: *Node, allocator: std.mem.Allocator) !void {
        for (self.config.preload) |model| {
            self.warmModel(allocator, model) catch |err| {
                const backend_name = if (model.backend) |backend| @tagName(backend) else "auto";
                if (!builtin.is_test) {
                    std.log.err("failed to warm configured inference model kind={s} model={s} backend={s} err={}", .{
                        @tagName(model.kind),
                        model.name,
                        backend_name,
                        err,
                    });
                }
                return err;
            };
        }
    }

    /// Warm configured models while the caller still owns the exclusive
    /// pre-serving startup phase. This is the only server path allowed to run
    /// live kernel compilation and GPU qualification. Call before publishing
    /// any listener or request handler that can reach this Node.
    pub fn warmConfiguredModelsBeforeServing(self: *Node, allocator: std.mem.Allocator) !void {
        if (self.request_surfaces_published) return error.KernelJitStartupWindowClosed;
        try self.model_manager.ensureResourceOwnerReady();
        // A retry must earn readiness again. Otherwise a failed second warm
        // could leave a stale successful latch and permit route publication.
        self.startup_preloads_materialized = false;
        if ((self.config.kernel_jit.mode.failClosed() or
            self.config.kernel_jit.qualified_profile_path != null) and
            self.config.preload.len == 0)
        {
            if (!builtin.is_test) {
                std.log.err(
                    "kernel_jit required/qualified activation needs one startup preload; configure inference.preload before serving",
                    .{},
                );
            }
            return error.KernelJitRequiredPreloadMissing;
        }
        if (self.config.kernel_jit.mode.failClosed() or
            self.config.kernel_jit.qualified_profile_path != null)
        {
            for (self.config.preload) |model| {
                if (model.startup_strategy == .prefetch)
                    return error.KernelJitRequiredPreloadCannotBePrefetchOnly;
            }
        }
        if (self.config.kernel_jit.qualified_profile_path != null and self.config.preload.len != 1) {
            return error.KernelJitQualifiedProfileMultiplePreloadsUnsupported;
        }
        if (self.config.kernel_jit.qualified_profile_path != null and
            self.config.preload[0].backend != null and
            self.config.preload[0].backend.? != .metal)
        {
            return error.KernelJitQualifiedProfileRequiresMetalPreload;
        }
        const model_previous = self.model_manager.session_manager.kernel_jit_load_context;
        const direct_previous = self.session_manager.kernel_jit_load_context;
        self.model_manager.session_manager.kernel_jit_load_context = .startup_preload;
        self.session_manager.kernel_jit_load_context = .startup_preload;
        defer {
            self.model_manager.session_manager.kernel_jit_load_context = model_previous;
            self.session_manager.kernel_jit_load_context = direct_previous;
        }
        try self.warmConfiguredModels(allocator);
        self.startup_preloads_materialized = true;
    }

    pub fn warmConfiguredGenerators(self: *Node, allocator: std.mem.Allocator) !void {
        try self.warmConfiguredModelsBeforeServing(allocator);
    }

    pub fn warmModel(self: *Node, allocator: std.mem.Allocator, model: WarmModel) !void {
        if (model.startup_strategy == .prefetch)
            return self.prefetchWarmModel(allocator, model);
        const a4b_request = try validatedWarmModelA4bRequest(model);
        const materialize_optional_sessions = kernelJitMaterializesOptionalSessions(self.config.kernel_jit.mode);
        switch (model.kind) {
            .generator => try self.warmGeneratorWithBackend(allocator, model.name, model.backend, a4b_request),
            .embedder => try self.warmEmbedder(allocator, model.name, model.backend, materialize_optional_sessions),
            .reranker => try self.warmReranker(allocator, model.name, model.backend),
            .chunker, .classifier, .rewriter, .reader, .transcriber, .extractor => try self.warmLoadOnlyModel(allocator, model),
        }
        if (materialize_optional_sessions and model.kind != .embedder) {
            try self.materializeWarmModelOptionalSessions(allocator, model);
        }
    }

    fn prefetchWarmModel(self: *Node, allocator: std.mem.Allocator, model: WarmModel) !void {
        if (model.kind != .generator) return error.A4bPrefetchRequiresGenerator;
        if (model.backend != null and model.backend.? != .cuda)
            return error.A4bPrefetchRequiresCuda;
        var io_impl = std.Io.Threaded.init(allocator, .{});
        defer io_impl.deinit();
        const model_path = try self.resolveModelPath(io_impl.io(), model.name, warmModelTaskDir(model.kind));
        defer self.allocator.free(model_path);
        var manifest = try manifest_mod.loadFromDir(allocator, model_path);
        defer manifest.deinit();
        const gguf_path = manifest.gguf_path orelse return error.A4bPrefetchRequiresGguf;
        const workers = model.load_workers orelse ops.A4bInferenceConfig.default_load_workers;
        const started_ns = embedTimingNowNs();
        if ((model.prepared_pack orelse .auto) != .off) {
            if (try a4b_prepared_pack.prefetchInstalled(allocator, model_path, gguf_path, workers)) |prepared| {
                std.log.info(
                    "prefetched A4B prepared pack model={s} shards={d} bytes={d} workers={d} elapsed_ms={d}",
                    .{ model.name, prepared.shard_count, prepared.bytes, prepared.workers, elapsedMs(started_ns, embedTimingNowNs()) },
                );
                return;
            } else if ((model.prepared_pack orelse .auto) == .required) {
                return error.A4bPreparedPackRequired;
            }
        }
        const result = try c_file.prefetchFile(allocator, gguf_path, workers);
        std.log.info(
            "prefetched inference generator model={s} artifact={s} bytes={d} workers={d} elapsed_ms={d}",
            .{ model.name, gguf_path, result.bytes, result.workers, elapsedMs(started_ns, embedTimingNowNs()) },
        );
    }

    fn materializeWarmModelOptionalSessions(self: *Node, allocator: std.mem.Allocator, model: WarmModel) !void {
        var io_impl = std.Io.Threaded.init(allocator, .{});
        defer io_impl.deinit();
        const model_path = try self.resolveModelPath(io_impl.io(), model.name, warmModelTaskDir(model.kind));
        defer self.allocator.free(model_path);
        const a4b_request = model.a4bRequest();
        var loaded_handle = if (a4b_request) |request|
            if (model.backend) |backend|
                try self.model_manager.acquireFromDirWithPreferredBackendsAndA4bRequest(
                    model_path,
                    singleBackendPreference(backend),
                    true,
                    request,
                )
            else
                try self.model_manager.acquireFromDirWithA4bRequest(model_path, request)
        else if (model.backend) |backend|
            try self.model_manager.acquireFromDirWithPreferredBackends(model_path, singleBackendPreference(backend), true)
        else
            try self.model_manager.acquireFromDir(model_path);
        defer loaded_handle.release();
        const loaded = loaded_handle.get();

        try loaded.materializeDeclaredOptionalSessions();
        if (self.config.kernel_jit.mode.failClosed() and
            !loaded.declaredOptionalSessionsMaterialized())
        {
            return error.KernelJitRequiredOptionalSessionUnmaterialized;
        }
    }

    pub fn warmGenerator(self: *Node, allocator: std.mem.Allocator, model_name: []const u8) !void {
        try self.warmGeneratorWithBackend(allocator, model_name, null, null);
    }

    fn warmGeneratorWithBackend(
        self: *Node,
        allocator: std.mem.Allocator,
        model_name: []const u8,
        backend: ?backends_mod.BackendType,
        a4b_request: ?ops.A4bInferenceRequest,
    ) !void {
        if (model_name.len == 0) return error.InvalidGenerationRequest;
        const started_at_ns = embedTimingNowNs();
        std.log.info("warming inference generator model={s}", .{model_name});
        const preferred_backends = if (backend) |value| singleBackendPreference(value) else null;
        const messages = [_]generation.Message{.{
            .role = "user",
            .content = "ping",
        }};
        var timing = DirectGenerateTiming{};
        const text = try self.generateMessagesDirectMaxTokens(
            allocator,
            model_name,
            &messages,
            1,
            preferred_backends,
            true,
            &timing,
            true,
            a4b_request,
        );
        defer allocator.free(text);
        const elapsed_ms = elapsedMs(started_at_ns, embedTimingNowNs());
        std.log.info(
            "warmed inference generator model={s} elapsed_ms={d} resolve_ms={d} load_ms={d} setup_ms={d} generate_ms={d}",
            .{
                model_name,
                elapsed_ms,
                timing.resolve_ms,
                timing.load_ms,
                timing.setup_ms,
                timing.generate_ms,
            },
        );
    }

    fn warmEmbedder(
        self: *Node,
        allocator: std.mem.Allocator,
        model_name: []const u8,
        backend: ?backends_mod.BackendType,
        materialize_optional_sessions: bool,
    ) !void {
        if (model_name.len == 0) return error.InvalidGenerationRequest;
        const started_at_ns = embedTimingNowNs();
        std.log.info("warming inference embedder model={s}", .{model_name});
        const texts = [_][]const u8{"ping"};

        var io_impl = std.Io.Threaded.init(allocator, .{});
        defer io_impl.deinit();
        const model_path = try self.resolveModelPath(io_impl.io(), model_name, "embedders");
        defer self.allocator.free(model_path);
        const Attempt = struct {
            allocator: std.mem.Allocator,
            texts: []const []const u8,
            materialize_optional_sessions: bool,
            fail_closed: bool,

            fn run(ctx: *anyopaque, loaded: *model_manager_mod.LoadedModel) !void {
                const attempt: *@This() = @ptrCast(@alignCast(ctx));
                if (loaded.manifest.hasCapability("sparse")) {
                    var pipeline = sparse_embedding_mod.SparseEmbeddingPipeline{
                        .allocator = attempt.allocator,
                        .session = loaded.session,
                        .tok = loaded.getTokenizer(),
                        .config = sparse_embedding_mod.SparseEmbeddingConfig.fromManifest(&loaded.manifest),
                        .execution_lock = loaded.embeddingExecutionLock(),
                    };
                    const sparse = try pipeline.embed(attempt.texts);
                    defer {
                        for (sparse) |*item| item.deinit(attempt.allocator);
                        attempt.allocator.free(sparse);
                    }
                } else {
                    var asset_lease = loaded.acquireEmbeddingAssetLease(false);
                    defer asset_lease.release();
                    try loaded.ensureEmbeddingAssets(true, false, false);
                    var pipeline = loaded.embeddingPipeline(attempt.allocator);
                    const embeddings = try pipeline.embed(attempt.texts);
                    defer {
                        for (embeddings) |embedding| attempt.allocator.free(embedding);
                        attempt.allocator.free(embeddings);
                    }
                }
                if (attempt.materialize_optional_sessions) {
                    try loaded.materializeDeclaredOptionalSessions();
                    if (attempt.fail_closed and !loaded.declaredOptionalSessionsMaterialized()) {
                        return error.KernelJitRequiredOptionalSessionUnmaterialized;
                    }
                }
            }
        };
        var attempt = Attempt{
            .allocator = allocator,
            .texts = &texts,
            .materialize_optional_sessions = materialize_optional_sessions,
            .fail_closed = self.config.kernel_jit.mode.failClosed(),
        };
        try runLoadedEmbeddingRuntimeWithRecovery(self, model_path, .{
            .preferred_backends = if (backend) |value| singleBackendPreference(value) else null,
            .cache_default_alias = false,
            .pin_on_success = true,
        }, &attempt, Attempt.run);
        std.log.info("warmed inference embedder model={s} elapsed_ms={d}", .{ model_name, elapsedMs(started_at_ns, embedTimingNowNs()) });
    }

    fn warmReranker(self: *Node, allocator: std.mem.Allocator, model_name: []const u8, backend: ?backends_mod.BackendType) !void {
        if (model_name.len == 0) return error.InvalidGenerationRequest;
        const started_at_ns = embedTimingNowNs();
        std.log.info("warming inference reranker model={s}", .{model_name});
        const documents = [_][]const u8{"pong"};
        var io_impl = std.Io.Threaded.init(allocator, .{});
        defer io_impl.deinit();
        const model_path = try self.resolveModelPath(io_impl.io(), model_name, "rerankers");
        var model_handle = if (backend) |value|
            try self.model_manager.acquireFromDirWithPreferredBackends(model_path, singleBackendPreference(value), false)
        else
            try self.model_manager.acquireFromDir(model_path);
        defer model_handle.release();
        const model = model_handle.get();
        var pipeline = model.rerankingPipeline(allocator);
        const scores = try pipeline.rerank("ping", &documents);
        defer allocator.free(scores);
        model_handle.pin();
        std.log.info("warmed inference reranker model={s} elapsed_ms={d}", .{ model_name, elapsedMs(started_at_ns, embedTimingNowNs()) });
    }

    fn warmLoadOnlyModel(self: *Node, allocator: std.mem.Allocator, model: WarmModel) !void {
        if (model.name.len == 0) return error.InvalidGenerationRequest;
        const task_dir = warmModelTaskDir(model.kind);
        const started_at_ns = embedTimingNowNs();
        std.log.info("loading inference {s} model={s}", .{ @tagName(model.kind), model.name });
        var io_impl = std.Io.Threaded.init(allocator, .{});
        defer io_impl.deinit();
        const model_path = try self.resolveModelPath(io_impl.io(), model.name, task_dir);
        var model_handle = if (model.backend) |backend|
            try self.model_manager.acquireFromDirWithPreferredBackends(model_path, singleBackendPreference(backend), false)
        else
            try self.model_manager.acquireFromDir(model_path);
        defer model_handle.release();
        model_handle.pin();
        std.log.info("loaded inference {s} model={s} elapsed_ms={d}", .{ @tagName(model.kind), model.name, elapsedMs(started_at_ns, embedTimingNowNs()) });
    }

    pub fn embedDenseJsonInputDirect(
        self: *Node,
        allocator: std.mem.Allocator,
        model_name: []const u8,
        input: std.json.Value,
    ) ![][]f32 {
        var io_impl = std.Io.Threaded.init(allocator, .{});
        defer io_impl.deinit();
        return try self.embedDenseJsonInputDirectWithContext(allocator, io_impl.io(), null, model_name, input);
    }

    pub fn embedDenseJsonInputDirectWithContext(
        self: *Node,
        allocator: std.mem.Allocator,
        io: std.Io,
        deadline_ns: ?u64,
        model_name: []const u8,
        input: std.json.Value,
    ) ![][]f32 {
        try ensureDirectEmbeddingDeadline(deadline_ns);
        const media_admission = requestMediaAdmission(self, denseEmbedRequestMediaShape(input));
        try self.acquireAdmissionUnits(media_admission.units);
        var reserved_units = media_admission.units;
        defer self.releaseAdmissionUnits(reserved_units);
        self.metrics.incRequest("embed.local");
        defer self.metrics.decActive();

        const model_path = try self.resolveModelPath(io, if (model_name.len > 0) model_name else null, "embedders");
        defer self.allocator.free(model_path);
        try ensureDirectEmbeddingDeadline(deadline_ns);
        var admission_manifest = try manifest_mod.loadFromDir(allocator, model_path);
        defer admission_manifest.deinit();
        if (admission_manifest.hasCapability("sparse")) return error.UnsupportedEmbeddingProvider;
        try ensureDirectEmbeddingDeadline(deadline_ns);

        var media_budget = RequestMediaBudget.init(media_admission.byte_cap);
        var parsed = try parseDenseEmbedInputsWithBudgetAndContext(
            self,
            allocator,
            &admission_manifest,
            input,
            &media_budget,
            .{ .io = io, .deadline_ns = deadline_ns },
        );
        defer parsed.deinit(allocator);
        return try self.embedParsedDenseInputsDirect(
            allocator,
            model_path,
            media_admission,
            &parsed,
            &reserved_units,
            deadline_ns,
        );
    }

    pub fn embedDensePartsDirect(
        self: *Node,
        allocator: std.mem.Allocator,
        model_name: []const u8,
        parts: []const DirectDenseEmbedPart,
    ) ![][]f32 {
        var io_impl = std.Io.Threaded.init(allocator, .{});
        defer io_impl.deinit();
        return try self.embedDensePartsDirectWithContext(allocator, io_impl.io(), null, model_name, parts);
    }

    pub fn embedDensePartsDirectWithContext(
        self: *Node,
        allocator: std.mem.Allocator,
        io: std.Io,
        deadline_ns: ?u64,
        model_name: []const u8,
        parts: []const DirectDenseEmbedPart,
    ) ![][]f32 {
        if (parts.len == 0) return try allocator.alloc([]f32, 0);
        try ensureDirectEmbeddingDeadline(deadline_ns);

        const preflight = try directDenseEmbedPreflight(parts);
        const media_admission = requestMediaAdmission(self, preflight.shape);
        if (preflight.known_media_bytes > media_admission.byte_cap) return error.RemoteContentTooLarge;
        const audio_admission = if (preflight.has_audio)
            audioDecodeAdmission(self, media_admission.resident_byte_cap)
        else
            null;
        if (audio_admission) |admission| {
            if (self.inference_admission.capacity != 0 and admission.max_decode_working_bytes == 0)
                return error.AudioTooLarge;
        }
        const initial_units = if (audio_admission) |admission|
            @max(media_admission.units, admission.units)
        else
            media_admission.units;

        // Only bounded arithmetic and borrowed-slice inspection precede this
        // lease. Fetch, decode, model resolution/loading, and output allocation
        // all happen while the weighted request remains admitted.
        try self.acquireAdmissionUnits(initial_units);
        var reserved_units = initial_units;
        defer self.releaseAdmissionUnits(reserved_units);
        self.metrics.incRequest("embed.local");
        defer self.metrics.decActive();

        const model_path = try self.resolveModelPath(io, if (model_name.len > 0) model_name else null, "embedders");
        defer self.allocator.free(model_path);
        try ensureDirectEmbeddingDeadline(deadline_ns);
        var admission_manifest = try manifest_mod.loadFromDir(allocator, model_path);
        defer admission_manifest.deinit();
        if (admission_manifest.hasCapability("sparse")) return error.UnsupportedEmbeddingProvider;

        var media_budget = RequestMediaBudget.init(media_admission.byte_cap);
        var parsed = try parseDirectDenseEmbedInputsWithContext(
            self,
            allocator,
            &admission_manifest,
            parts,
            &media_budget,
            .{ .io = io, .deadline_ns = deadline_ns },
        );
        defer parsed.deinit(allocator);
        return try self.embedParsedDenseInputsDirect(
            allocator,
            model_path,
            media_admission,
            &parsed,
            &reserved_units,
            deadline_ns,
        );
    }

    fn embedParsedDenseInputsDirect(
        self: *Node,
        allocator: std.mem.Allocator,
        model_path: []const u8,
        media_admission: ReadRequestAdmission,
        parsed: *ParsedDenseEmbedInputs,
        reserved_units: *usize,
        deadline_ns: ?u64,
    ) ![][]f32 {
        if (parsed.total_count == 0) return try allocator.alloc([]f32, 0);
        try ensureDirectEmbeddingDeadline(deadline_ns);

        var audio_decode_working_bytes = default_max_audio_decode_working_bytes;

        if (parsed.images.items.len > 0) {
            var decoded_budget = ReadDecodedImageBudget.init(media_admission, effectiveRequestContentSecurity(self).max_image_dimension);
            for (parsed.images.items) |image| try decoded_budget.addImage(image.bytes);
            const required_units = @max(reserved_units.*, decoded_budget.requiredUnits());
            try self.growAdmissionUnits(reserved_units.*, required_units);
            reserved_units.* = required_units;
        }

        if (parsed.audio.items.len > 0) {
            const audio_admission = audioDecodeAdmission(self, media_admission.resident_byte_cap);
            const required_units = @max(reserved_units.*, audio_admission.units);
            try self.growAdmissionUnits(reserved_units.*, required_units);
            reserved_units.* = required_units;
            audio_decode_working_bytes = audio_admission.max_decode_working_bytes;
        }

        const Attempt = struct {
            allocator: std.mem.Allocator,
            parsed: *ParsedDenseEmbedInputs,
            audio_decode_working_bytes: usize,
            deadline_ns: ?u64,
            vectors: ?[][]f32 = null,

            fn run(ctx: *anyopaque, model: *model_manager_mod.LoadedModel) !void {
                const attempt: *@This() = @ptrCast(@alignCast(ctx));
                if (model.manifest.hasCapability("sparse")) return error.UnsupportedEmbeddingProvider;
                var asset_lease = model.acquireEmbeddingAssetLease(attempt.parsed.audio.items.len > 0);
                defer asset_lease.release();
                try ensureDirectEmbeddingDeadline(attempt.deadline_ns);
                var pipeline = try prepareInitialDenseEmbeddingPipeline(model, attempt.allocator, attempt.parsed);
                pipeline.config.max_audio_decode_working_bytes = attempt.audio_decode_working_bytes;
                var audio_asset_guard = AudioEmbeddingAssetGuard.init(
                    model,
                    attempt.parsed.audio.items.len > 0,
                );
                defer audio_asset_guard.deinit();
                const vectors = try embedDenseInputs(
                    attempt.allocator,
                    &pipeline,
                    attempt.parsed,
                    model,
                    &audio_asset_guard,
                    &asset_lease,
                );
                asset_lease.release();
                errdefer freeDirectDenseVectors(attempt.allocator, vectors);
                try ensureDirectEmbeddingDeadline(attempt.deadline_ns);
                attempt.vectors = vectors;
            }
        };
        var attempt = Attempt{
            .allocator = allocator,
            .parsed = parsed,
            .audio_decode_working_bytes = audio_decode_working_bytes,
            .deadline_ns = deadline_ns,
        };
        try runLoadedEmbeddingRuntimeWithRecovery(self, model_path, .{}, &attempt, Attempt.run);
        return attempt.vectors.?;
    }

    pub fn readImagesDirect(
        self: *Node,
        allocator: std.mem.Allocator,
        model_name: []const u8,
        request: readers_api.Request,
    ) ![]readers_api.Result {
        if (request.images.len == 0) return try allocator.alloc(readers_api.Result, 0);
        if (request.images.len > max_read_batch_images) return error.ReadBatchTooLarge;
        const max_tokens = try validateReadMaxTokens(request.max_tokens);
        const inline_source_cap = readInlineSourceByteCap(self);
        var inline_source_bytes: usize = 0;
        for (request.images) |url| {
            inline_source_bytes = try addReadInlineSourceBytes(inline_source_bytes, url, inline_source_cap);
        }
        const admission = readRequestAdmission(self, request.images.len, inline_source_bytes, max_tokens);
        try self.acquireAdmissionUnits(admission.units);
        var reserved_units = admission.units;
        defer self.releaseAdmissionUnits(reserved_units);
        self.metrics.incRequest("read.local");
        defer self.metrics.decActive();

        var io_impl = std.Io.Threaded.init(allocator, .{});
        defer io_impl.deinit();

        const model_path = try self.resolveModelPath(io_impl.io(), if (model_name.len > 0) model_name else null, "readers");
        defer self.allocator.free(model_path);

        const downloaded = try allocator.alloc(scraping.DownloadedContent, request.images.len);
        var downloaded_count: usize = 0;
        defer {
            for (downloaded[0..downloaded_count]) |*item| item.deinit(allocator);
            allocator.free(downloaded);
        }
        const image_datas = try allocator.alloc([]const u8, request.images.len);
        defer allocator.free(image_datas);

        const batch_byte_cap = admission.byte_cap;
        var batch_bytes: usize = 0;
        var decoded_budget = ReadDecodedImageBudget.init(admission, effectiveRequestContentSecurity(self).max_image_dimension);
        for (request.images, 0..) |image_url, i| {
            const inline_content_trust: InlineContentTrust = switch (request.inline_content_trust) {
                .untrusted => .untrusted,
                .trusted_internal => .trusted_internal,
            };
            var item = try downloadReadBatchContent(self, allocator, image_url, batch_byte_cap, batch_bytes, inline_content_trust);
            errdefer item.deinit(allocator);
            batch_bytes = try addReadBatchDownloadedBytes(batch_bytes, item, batch_byte_cap);
            try decoded_budget.addImage(item.data);
            downloaded[i] = item;
            downloaded_count += 1;
            image_datas[i] = downloaded[i].data;
        }

        const required_units = @max(admission.units, decoded_budget.requiredUnits());
        try self.growAdmissionUnits(reserved_units, required_units);
        reserved_units = required_units;

        var reader = try readers_mod.LoadedReader.loadFromDir(allocator, model_path, &self.session_manager, &self.model_manager);
        defer reader.deinit();

        const out = try allocator.alloc(readers_api.Result, request.images.len);
        var initialized: usize = 0;
        errdefer {
            for (out[0..initialized]) |*result| readers_api.deinitResult(allocator, result);
            allocator.free(out);
        }

        const results = try reader.readBatch(image_datas, .{
            .prompt = normalizeReadPrompt(request.prompt),
            .max_tokens = max_tokens,
            .source_fingerprint = request.source_fingerprint,
        });
        defer {
            for (results) |result| {
                var tmp = result;
                tmp.deinit();
            }
            allocator.free(results);
        }
        if (results.len != request.images.len) return error.InvalidReadResultCount;

        for (results, 0..) |result, i| {
            var item: readers_api.Result = .{
                .text = try allocator.dupe(u8, result.text),
            };
            errdefer readers_api.deinitResult(allocator, &item);
            item.fields_json = try readerFieldsJsonAlloc(allocator, result.fields);
            item.regions_json = try readerRegionsJsonAlloc(allocator, result.regions);
            out[i] = item;
            initialized += 1;
        }
        return out;
    }

    pub fn transcribeAudioDirect(
        self: *Node,
        allocator: std.mem.Allocator,
        model_name: []const u8,
        request: transcribing_api.Request,
    ) !transcribing_api.Response {
        var media_shape: RequestMediaAdmissionShape = .{};
        if (std.mem.startsWith(u8, request.url, "data:"))
            media_shape.addInline(request.url.len, false)
        else
            media_shape.has_remote = true;
        const media_admission = requestMediaAdmission(self, media_shape);
        try self.acquireAdmissionUnits(media_admission.units);
        var reserved_units = media_admission.units;
        defer self.releaseAdmissionUnits(reserved_units);
        self.metrics.incRequest("transcribe.local");
        defer self.metrics.decActive();

        var io_impl = std.Io.Threaded.init(allocator, .{});
        defer io_impl.deinit();

        const model_path = try self.resolveModelPath(io_impl.io(), if (model_name.len > 0) model_name else null, "transcribers");
        defer self.allocator.free(model_path);

        var media_budget = RequestMediaBudget.init(media_admission.byte_cap);
        var downloaded = try downloadRemoteContentWithBudgetForRequest(self, allocator, request.url, &media_budget);
        defer downloaded.deinit(allocator);
        const decode_options = audio_mod.DecodeOptions{ .mime_hint = downloaded.content_type };
        const resident_bytes = if (std.mem.startsWith(u8, request.url, "data:"))
            std.math.add(usize, media_budget.used_bytes, downloaded.data.len) catch std.math.maxInt(usize)
        else
            downloaded.data.len;
        const audio_admission = audioDecodeAdmission(self, resident_bytes);
        // Keep the reservation monotonic: a remote URL is admitted against the
        // *potential* max download, which is typically larger than the actual
        // clip's decode reservation. growUnits() is grow-only, so overwriting
        // reserved_units downward here would under-release on the deferred
        // releaseAdmissionUnits() and permanently leak shared admission capacity.
        const required_units = @max(reserved_units, audio_admission.units);
        try self.growAdmissionUnits(reserved_units, required_units);
        reserved_units = required_units;

        var decoded = audio_mod.decodeBounded(
            allocator,
            downloaded.data,
            decode_options,
            audio_admission.max_decode_working_bytes,
        ) catch |err| switch (err) {
            error.AudioTooLarge, error.OutOfMemory => return err,
            else => return error.UnsupportedAudioInput,
        };
        defer decoded.deinit();

        var model_handle = try self.model_manager.acquireFromDir(model_path);
        defer model_handle.release();
        const model = model_handle.get();
        const whisper_config = session_factory.getWhisperConfig(model.session) orelse return error.UnsupportedTranscriberProvider;

        const transcription = @import("../pipelines/transcription.zig");
        const whisper_prompt = @import("../pipelines/whisper_prompt.zig");
        const prompt_cache = if (model.whisper_prompt_cache) |*cache|
            cache
        else
            return error.InvalidWhisperDecoderConfig;
        var prompt_scratch: [3]whisper_prompt.ForcedDecoderId = undefined;
        const forced_ids = try prompt_cache.resolve(&prompt_scratch, request.language);
        var pipeline = transcription.TranscriptionPipeline.init(
            allocator,
            model.session,
            model.session,
            model.getTokenizer(),
            .{
                .max_length = @intCast(whisper_config.max_target_positions),
                .decoder_start_token_id = whisper_config.decoder_start_token_id,
                .eos_token_id = whisper_config.eos_token_id,
                .language = request.language,
                .forced_decoder_ids = forced_ids,
                .max_decode_working_bytes = audio_admission.max_decode_working_bytes,
                .language_tokens = prompt_cache.language_tokens,
            },
        );

        var result = try pipeline.transcribePcm(decoded.samples, decoded.sample_rate);
        defer result.deinit();
        return .{
            .text = try allocator.dupe(u8, result.text),
            .language = if (result.language) |language| try allocator.dupe(u8, language) else null,
        };
    }

    pub fn extractDirect(
        self: *Node,
        allocator: std.mem.Allocator,
        model_name: []const u8,
        request: extracting_api.Request,
    ) !extracting_api.Response {
        return self.extractWithAdmission(allocator, model_name, request, .direct);
    }

    const ExtractionAdmissionOwner = enum { direct, http_route };

    fn extractWithAdmission(
        self: *Node,
        allocator: std.mem.Allocator,
        model_name: []const u8,
        request: extracting_api.Request,
        admission_owner: ExtractionAdmissionOwner,
    ) !extracting_api.Response {
        switch (admission_owner) {
            .direct => try self.acquireAdmissionUnits(1),
            .http_route => try self.reserveAdmissionUnits(1),
        }
        var reserved_units: usize = 1;
        defer switch (admission_owner) {
            .direct => self.releaseAdmissionUnits(reserved_units),
            .http_route => self.releaseSlotUnits(reserved_units),
        };
        self.metrics.incRequest(if (admission_owner == .direct) "extract.local" else "extract");
        defer self.metrics.decActive();

        var schema_parsed = try std.json.parseFromSlice(extraction_api.ExtractionSchema, allocator, request.schema_json, .{
            .allocate = .alloc_always,
            .ignore_unknown_fields = true,
        });
        defer schema_parsed.deinit();
        const operation = try canonicalExtractionOperation(schema_parsed.value);

        if (operation != .structures) {
            // Entity, relation, and classification models are text-only. Parse
            // them with a no-I/O path so a request that will be rejected cannot
            // trigger remote media fetches or decoding work first.
            var typed_options = try std.json.parseFromSlice(extraction_api.ExtractionOptions, allocator, if (request.options_json.len > 0) request.options_json else "{}", .{
                .allocate = .alloc_always,
                .ignore_unknown_fields = true,
            });
            defer typed_options.deinit();
            var parsed_inputs = try parseDirectTextExtractionInputs(allocator, request.inputs);
            defer parsed_inputs.deinit();
            const json = switch (operation) {
                .entities_relations => try self.extractEntitiesAndRelationsDirectJsonAlloc(
                    allocator,
                    model_name,
                    schema_parsed.value,
                    typed_options.value,
                    request.inputs,
                    parsed_inputs.texts.items,
                ),
                .classifications => try self.extractClassificationsDirectJsonAlloc(
                    allocator,
                    model_name,
                    schema_parsed.value,
                    typed_options.value,
                    request.inputs,
                    parsed_inputs.texts.items,
                ),
                .structures => unreachable,
            };
            return .{ .allocator = allocator, .json = json };
        }

        const schemas = try parseCanonicalStructureSchemas(allocator, schema_parsed.value);
        defer {
            for (schemas) |*schema| schema.deinit(allocator);
            allocator.free(schemas);
        }

        // This parser bounds max_tokens before resolver or media work begins.
        var options = try parseExtractionOptionsJson(allocator, request.options_json);
        defer options.deinit();
        const media_shape = try directExtractionMediaShape(allocator, request.inputs);
        if (media_shape.image_count > max_read_batch_images) return error.ReadBatchTooLarge;
        const media_admission = requestMediaAdmission(self, media_shape);
        try self.growAdmissionUnits(reserved_units, media_admission.units);
        reserved_units = media_admission.units;
        const config = extraction_mod.ExtractionConfig{
            .threshold = options.threshold orelse 0.3,
            .flat_ner = options.flat_ner orelse true,
            .include_confidence = options.include_confidence orelse false,
            .include_spans = options.include_spans orelse false,
        };

        var io_impl = std.Io.Threaded.init(allocator, .{});
        defer io_impl.deinit();

        const extractor_ctx = extractors_mod.Context{
            .allocator = allocator,
            .io = io_impl.io(),
            .models_dir = self.config.models_dir,
            .session_manager = &self.session_manager,
            .model_manager = &self.model_manager,
            .reader_resolver = &self.extraction_reader_resolver,
        };
        // Resolve the cheap manifest/path surface before any remote fetch or
        // media decode. The extractor itself does not load model weights until
        // extractText/extractImages below.
        var extractor = try extractors_mod.resolve(extractor_ctx, model_name, media_shape.image_count > 0);
        defer extractor.deinit(allocator);

        // Fetch and decode request media only after resolver preflight succeeds.
        var parsed_inputs = try parseDirectExtractionInputs(
            self,
            allocator,
            request.inputs,
            options.prompt,
            options.max_tokens,
            media_admission.byte_cap,
        );
        defer parsed_inputs.deinit();
        try validateExtractionInputKinds(parsed_inputs.texts.items.len, parsed_inputs.images.items.len);
        if (parsed_inputs.images.items.len > max_read_batch_images) return error.ReadBatchTooLarge;
        if (parsed_inputs.images.items.len > 0) {
            var decoded_budget = ReadDecodedImageBudget.init(media_admission, effectiveRequestContentSecurity(self).max_image_dimension);
            for (parsed_inputs.images.items) |image_bytes| try decoded_budget.addImage(image_bytes);
            const required_units = @max(
                @max(reserved_units, decoded_budget.requiredUnits()),
                estimateReadAdmissionUnits(parsed_inputs.images.items.len, parsed_inputs.max_tokens),
            );
            try self.growAdmissionUnits(reserved_units, required_units);
            reserved_units = required_units;
        }

        const results = if (parsed_inputs.images.items.len > 0)
            try extractor.extractImages(extractor_ctx, schemas, config, parsed_inputs.images.items, .{
                .prompt = parsed_inputs.prompt,
                .max_tokens = parsed_inputs.max_tokens,
            })
        else
            try extractor.extractText(extractor_ctx, schemas, config, parsed_inputs.texts.items);
        defer {
            for (results) |*result| result.deinit(allocator);
            allocator.free(results);
        }

        return .{
            .allocator = allocator,
            .json = try extractionResponseJsonAlloc(allocator, model_name, request.inputs, results),
        };
    }

    fn extractEntitiesAndRelationsDirectJsonAlloc(
        self: *Node,
        allocator: std.mem.Allocator,
        model_name: []const u8,
        schema: extraction_api.ExtractionSchema,
        options: extraction_api.ExtractionOptions,
        inputs: []const extracting_api.Input,
        texts: []const []const u8,
    ) ![]u8 {
        if (schema.entities) |labels| {
            for (labels) |label| if (!isCanonicalLabelSegment(label)) return error.InvalidEntitySchema;
        }
        const relation_schemas = schema.relations orelse &.{};
        const relation_labels = try canonicalRelationLabels(allocator, relation_schemas);
        defer freeStringSlice(allocator, relation_labels);
        const want_relations = relation_labels.len > 0;
        const input_ids = try allocator.alloc(?[]const u8, inputs.len);
        defer allocator.free(input_ids);
        for (inputs, 0..) |input, index| input_ids[index] = input.id;
        const request = EntityExtractionRequest{
            .model = model_name,
            .labels = schema.entities,
            .relation_labels = if (want_relations) relation_labels else null,
            .resolver = options.resolver,
            .threshold = options.threshold,
            .flat_ner = options.flat_ner,
            .include_confidence = options.include_confidence orelse false,
            .include_spans = options.include_spans orelse false,
            .input_ids = input_ids,
        };

        var io_impl = std.Io.Threaded.init(allocator, .{});
        defer io_impl.deinit();
        const model_path = try self.resolveRequestModelPath(allocator, io_impl.io(), model_name, "extractors");
        defer allocator.free(model_path);
        if (rebel_mod.isRebelModel(allocator, model_path)) {
            try rebel_mod.validateSchemaSupport(request.labels, request.relation_labels);
            var failure_stage: RebelExtractionFailureStage = .model_layout;
            return self.extractRebelJsonAlloc(
                allocator,
                model_path,
                request,
                texts,
                want_relations,
                &failure_stage,
            );
        }

        var model_handle = try self.model_manager.acquireFromDir(model_path);
        defer model_handle.release();
        const model = model_handle.get();
        try validateTextEntityExtractionManifest(&model.manifest);

        if (model.isGlinerModel()) {
            var pipeline = model.glinerPipeline(allocator);
            pipeline.config.threshold = options.threshold orelse pipeline.config.threshold;
            pipeline.config.flat_ner = options.flat_ner orelse pipeline.config.flat_ner;
            const labels: ?[]const []const u8 = if (schema.entities) |values|
                if (values.len > 0) values else null
            else
                null;

            if (want_relations) {
                if (!model.supportsRelationExtraction() or !pipeline.supportsRelationExtraction())
                    return error.UnsupportedRelationExtraction;
                const relation_entity_labels = try relationEntityLabelsAlloc(
                    allocator,
                    labels,
                    pipeline.config.default_labels,
                    relation_labels,
                );
                defer if (relation_entity_labels) |values| allocator.free(values);
                const extracted = try pipeline.extractRelationsBatch(texts, relation_entity_labels, relation_labels);
                defer {
                    freeBorrowedLabelEntityBatches(allocator, extracted.entities);
                    for (extracted.relations) |relations| {
                        for (relations) |*relation| relation.deinit(allocator);
                        allocator.free(relations);
                    }
                    allocator.free(extracted.relations);
                }
                if (options.resolver) |resolver_cfg| {
                    var resolved = try resolveExtractedOutput(allocator, extracted.entities, extracted.relations, resolver_cfg);
                    defer resolved.deinit(allocator);
                    return try entityExtractionResponseJsonAlloc(allocator, request, resolved.entities, resolved.relations, texts);
                }
                return try entityExtractionResponseJsonAlloc(allocator, request, extracted.entities, extracted.relations, texts);
            }

            const all_entities = try pipeline.recognizeBatch(texts, labels);
            defer freeBorrowedLabelEntityBatches(allocator, all_entities);
            const cleaned_entities = try applyLearnedCleanupIfPresent(allocator, try model.getCleanupHead(), texts, all_entities);
            defer if (cleaned_entities) |entities| freeEntityBatches(allocator, entities);
            const entities_for_response = cleaned_entities orelse all_entities;
            if (options.resolver) |resolver_cfg| {
                var resolved = try resolveExtractedOutput(allocator, entities_for_response, null, resolver_cfg);
                defer resolved.deinit(allocator);
                return try entityExtractionResponseJsonAlloc(allocator, request, resolved.entities, resolved.relations, texts);
            }
            return try entityExtractionResponseJsonAlloc(allocator, request, entities_for_response, null, texts);
        }

        if (want_relations) return error.UnsupportedRelationExtraction;
        var pipeline = model.nerPipeline(allocator);
        pipeline.config.threshold = options.threshold orelse pipeline.config.threshold;
        const all_entities = try pipeline.recognizeBatch(texts);
        defer freeBorrowedLabelEntityBatches(allocator, all_entities);
        const cleaned_entities = try applyLearnedCleanupIfPresent(allocator, try model.getCleanupHead(), texts, all_entities);
        defer if (cleaned_entities) |entities| freeEntityBatches(allocator, entities);
        const entities_for_response = cleaned_entities orelse all_entities;
        if (options.resolver) |resolver_cfg| {
            var resolved = try resolveExtractedOutput(allocator, entities_for_response, null, resolver_cfg);
            defer resolved.deinit(allocator);
            return try entityExtractionResponseJsonAlloc(allocator, request, resolved.entities, resolved.relations, texts);
        }
        return try entityExtractionResponseJsonAlloc(allocator, request, entities_for_response, null, texts);
    }

    fn extractClassificationsDirectJsonAlloc(
        self: *Node,
        allocator: std.mem.Allocator,
        model_name: []const u8,
        schema: extraction_api.ExtractionSchema,
        options: extraction_api.ExtractionOptions,
        inputs: []const extracting_api.Input,
        texts: []const []const u8,
    ) ![]u8 {
        var io_impl = std.Io.Threaded.init(allocator, .{});
        defer io_impl.deinit();
        const model_path = try self.resolveClassificationRequestModelPath(allocator, io_impl.io(), model_name);
        defer allocator.free(model_path);
        var model_handle = try self.model_manager.acquireFromDir(model_path);
        defer model_handle.release();
        const model = model_handle.get();
        if (!model.supportsClassification()) return error.UnsupportedClassificationExtraction;

        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const alloc = arena.allocator();
        const lists = try alloc.alloc(std.ArrayListUnmanaged(extraction_api.ExtractionClassification), texts.len);
        for (lists) |*list| list.* = .empty;
        var prompt_tokens: usize = 0;
        for (schema.classifications orelse &.{}) |classification_schema| {
            if (classification_schema.name.len == 0 or classification_schema.labels.len == 0)
                return error.InvalidClassificationSchema;
            if (classification_schema.top_k) |top_k| if (top_k < 1) return error.InvalidTopK;

            if (model.isGlinerModel()) {
                var pipeline = model.glinerPipeline(allocator);
                const multi_label = classification_schema.multi_label orelse false;
                const results = try pipeline.classifyBatch(texts, classification_schema.labels, .{
                    .threshold = options.threshold orelse 0.0,
                    .multi_label = multi_label,
                    .top_k = if (multi_label) 0 else @intCast(classification_schema.top_k orelse 1),
                });
                defer freeClassificationBatch(allocator, results);
                try appendExtractionClassificationBatch(alloc, lists, classification_schema, options, results);
                const schema_prompt_tokens = try std.math.add(
                    usize,
                    estimateTextsTokens(texts),
                    estimateTextsTokens(classification_schema.labels),
                );
                prompt_tokens = try std.math.add(usize, prompt_tokens, schema_prompt_tokens);
                continue;
            }

            if (!manifestSupportsZeroShotClassification(&model.manifest))
                return error.UnsupportedClassificationExtraction;
            var pipeline = model.classificationPipeline(allocator, .{
                .max_length = model.manifest.max_position_embeddings,
                .hypothesis_template = classification_schema.hypothesis_template orelse "This example is {}.",
                .multi_label = classification_schema.multi_label orelse false,
                .entailment_index = nliEntailmentIndex(model.manifest.id2label),
            });
            var schema_prompt_tokens: usize = 0;
            const results = try pipeline.classifyBatchWithPromptTokens(texts, classification_schema.labels, &schema_prompt_tokens);
            defer freeClassificationBatch(allocator, results);
            try appendExtractionClassificationBatch(alloc, lists, classification_schema, options, results);
            prompt_tokens = try std.math.add(usize, prompt_tokens, schema_prompt_tokens);
        }

        const data = try alloc.alloc(extraction_api.ExtractionObject, texts.len);
        for (data, 0..) |*item, index| item.* = .{
            .id = if (index < inputs.len) inputs[index].id else null,
            .classifications = lists[index].items,
        };
        var usage: std.json.ObjectMap = .empty;
        try usage.ensureTotalCapacity(alloc, 3);
        usage.putAssumeCapacity("prompt_tokens", .{ .integer = @intCast(prompt_tokens) });
        usage.putAssumeCapacity("completion_tokens", .{ .integer = 0 });
        usage.putAssumeCapacity("total_tokens", .{ .integer = @intCast(prompt_tokens) });
        return try std.json.Stringify.valueAlloc(allocator, extraction_api.ExtractionResponse{
            .object = "extraction",
            .model = model_name,
            .data = data,
            .usage = .{ .map = usage },
        }, .{});
    }

    /// Returns a newly allocated canonical path. The caller must free it with
    /// the allocator passed to this function.
    fn resolveRequestModelPath(
        self: *Node,
        allocator: std.mem.Allocator,
        io: std.Io,
        name: ?[]const u8,
        task_type: ?[]const u8,
    ) ![]const u8 {
        if (comptime builtin.is_test) request_work_test_counters.model_resolution_attempts += 1;
        if (name) |raw| try validateRequestModelIdentifier(raw);
        const resolved = try self.resolveModelPath(io, name, task_type);
        defer self.allocator.free(resolved);

        const root = try realPathExistingAlloc(allocator, io, self.config.models_dir);
        defer allocator.free(root);
        const canonical = try realPathExistingAlloc(allocator, io, resolved);
        errdefer allocator.free(canonical);
        if (!pathHasComponentPrefix(canonical, root)) return error.ModelOutsideModelsDir;
        return canonical;
    }

    fn requestModelResolutionError(ctx: *httpx.Context, err: anyerror) !httpx.Response {
        return switch (requestModelResolutionErrorKind(err)) {
            .invalid => ctx.status(400).json(.{
                .@"error" = "INVALID_REQUEST",
                .message = "model must be a relative identifier within models_dir",
            }),
            .missing => ctx.status(404).json(.{
                .@"error" = "MODEL_NOT_FOUND",
                .message = "model not found",
                .hint = "Run `antfly inference list`; pull the requested model or restart with --models-dir <path>",
            }),
            .internal => ctx.status(500).json(.{
                .@"error" = "MODEL_RESOLUTION_FAILED",
                .message = internalErrorMessage("MODEL_RESOLUTION_FAILED", err),
            }),
        };
    }

    /// Resolve a model name to a directory path for trusted in-process callers.
    /// Supports: absolute path, "hf:owner/name:variant", "owner/name", variant resolution.
    /// Matches Go inference's resolveModel: exact match → re-scan → variant resolution.
    /// When task_type is provided (e.g. "embedders"), also searches models_dir/task_type/.
    /// Always returns memory owned by `self.allocator`; the caller must free it.
    pub fn resolveModelPath(self: *Node, io: std.Io, name: ?[]const u8, task_type: ?[]const u8) ![]const u8 {
        if (name) |raw| {
            // Strip "hf:" prefix if present
            const n = if (std.mem.startsWith(u8, raw, "hf:")) raw[3..] else raw;

            // Explicit Hub variants installed by the registry live in stable,
            // variant-specific leaf directories. Resolve that identity before
            // the legacy variant-stripping fallback below.
            if (registry_mod.ModelRef.parse(n)) |ref| {
                if (!std.mem.eql(u8, ref.variant, "auto")) {
                    const variant_path = try registry_mod.modelInstallDirAlloc(self.allocator, self.config.models_dir, ref);
                    if (dirContainsModel(variant_path)) return variant_path;
                    self.allocator.free(variant_path);

                    if (task_type) |tt| {
                        const task_dir = try std.fmt.allocPrint(self.allocator, "{s}/{s}", .{ self.config.models_dir, tt });
                        defer self.allocator.free(task_dir);
                        const task_variant_path = try registry_mod.modelInstallDirAlloc(self.allocator, task_dir, ref);
                        if (dirContainsModel(task_variant_path)) return task_variant_path;
                        self.allocator.free(task_variant_path);
                    }
                }
            } else |_| {}

            // Strip ":variant" suffix for path resolution (variant is for pulling, not path lookup)
            const name_without_variant = if (std.mem.indexOfScalar(u8, n, ':')) |colon| n[0..colon] else n;

            // Absolute path
            if (std.mem.startsWith(u8, name_without_variant, "/")) return try self.allocator.dupe(u8, name_without_variant);

            // Try exact match: models_dir/name
            const path = try std.fmt.allocPrint(self.allocator, "{s}/{s}", .{ self.config.models_dir, name_without_variant });
            if (dirContainsModel(path)) {
                return path;
            } else {
                self.allocator.free(path);
            }

            // Try task-type subdirectory: models_dir/task_type/name
            if (task_type) |tt| {
                const task_path = try std.fmt.allocPrint(self.allocator, "{s}/{s}/{s}", .{ self.config.models_dir, tt, name_without_variant });
                if (dirContainsModel(task_path)) {
                    return task_path;
                } else {
                    self.allocator.free(task_path);
                }
            }

            // Discovery publishes validated managed-receipt identities instead
            // of cache leaf names. Only scan after cheap legacy exact matches:
            // those paths do not depend on receipt metadata and must not become
            // O(models x artifacts) or fail because an unrelated receipt is
            // unreadable/ambiguous.
            if (try self.resolveDiscoveredRequestName(io, n)) |managed_path|
                return managed_path;

            if (task_type) |tt| {
                // Variant resolution within task-type dir
                const task_dir = try std.fmt.allocPrint(self.allocator, "{s}/{s}", .{ self.config.models_dir, tt });
                defer self.allocator.free(task_dir);
                if (try registry_mod.resolveVariant(self.allocator, io, task_dir, name_without_variant)) |variant_path| {
                    return variant_path;
                }
            }

            // Variant resolution: look for "name-{suffix}" with shortest suffix wins
            if (try registry_mod.resolveVariant(self.allocator, io, self.config.models_dir, name_without_variant)) |variant_path| {
                return variant_path;
            }

            // If name has "owner/model" format, try just the model part (flat layout)
            if (std.mem.indexOfScalar(u8, name_without_variant, '/')) |slash| {
                const model_only = name_without_variant[slash + 1 ..];
                const flat_path = try std.fmt.allocPrint(self.allocator, "{s}/{s}", .{ self.config.models_dir, model_only });
                if (dirContainsModel(flat_path)) {
                    return flat_path;
                } else {
                    self.allocator.free(flat_path);
                }

                // Variant resolution on model-only name
                if (try registry_mod.resolveVariant(self.allocator, io, self.config.models_dir, model_only)) |variant_path| {
                    return variant_path;
                }
            }

            return error.ModelNotFound;
        }

        // No model specified — use models_dir itself if it contains model files,
        // otherwise scan for the first subdirectory.
        if (dirContainsModel(self.config.models_dir)) {
            return try self.allocator.dupe(u8, self.config.models_dir);
        }
        // Scan models_dir for subdirectories that contain model files
        // If task_type is provided, prefer scanning within that subdirectory
        if (task_type) |tt| {
            const task_dir = try std.fmt.allocPrint(self.allocator, "{s}/{s}", .{ self.config.models_dir, tt });
            if (dirContainsModel(task_dir)) {
                return task_dir;
            }
            if (self.findFirstModelInDir(task_dir)) |p| {
                self.allocator.free(task_dir);
                return p;
            }
            self.allocator.free(task_dir);
        }
        return self.findFirstModelDir() orelse error.ModelNotSpecified;
    }

    fn resolveDiscoveredRequestName(
        self: *Node,
        io: std.Io,
        request_name: []const u8,
    ) !?[]u8 {
        // Model-path resolution is also used by narrow unit and direct-runtime
        // construction paths that initialize only Node's request fields. Build
        // this read-only discovery view from the authoritative configured root
        // instead of relying on the long-lived listing registry to be present.
        // It also keeps a request resolution race from sharing mutable listing
        // state with a concurrent inventory refresh.
        var registry = registry_mod.ModelRegistry.init(self.allocator, self.config.models_dir);
        const discovered = try registry.discoverShallow(io);
        defer {
            for (discovered) |entry| {
                registry.allocator.free(entry.name);
                registry.allocator.free(entry.path);
            }
            if (discovered.len > 0) registry.allocator.free(discovered);
        }

        var match: ?[]const u8 = null;
        for (discovered) |entry| {
            if (!std.mem.eql(u8, entry.name, request_name)) continue;
            if (match != null and !std.mem.eql(u8, match.?, entry.path))
                return error.AmbiguousModelIdentifier;
            match = entry.path;
        }
        return if (match) |path| try self.allocator.dupe(u8, path) else null;
    }

    fn findFirstModelInDir(self: *Node, dir_path: []const u8) ?[]const u8 {
        if (!build_options.link_libc) {
            var dir = std.Io.Dir.cwd().openDir(std.Options.debug_io, dir_path, .{ .iterate = true }) catch return null;
            defer dir.close(std.Options.debug_io);
            var iter = dir.iterate();
            while (iter.next(std.Options.debug_io) catch null) |entry| {
                const ename_slice = entry.name;
                if (ename_slice.len == 0 or ename_slice[0] == '.') continue;

                const sub_path = std.fmt.allocPrint(self.allocator, "{s}/{s}", .{ dir_path, ename_slice }) catch continue;
                if (dirContainsModel(sub_path)) {
                    return sub_path;
                }
                if (self.findFirstModelInDir(sub_path)) |p| {
                    self.allocator.free(sub_path);
                    return p;
                }
                self.allocator.free(sub_path);
            }
            return null;
        }

        const dir_z = self.allocator.dupeZ(u8, dir_path) catch return null;
        defer self.allocator.free(dir_z);

        const cc = c_file.c;
        const dir = cc.opendir(dir_z.ptr);
        if (dir == null) return null;
        defer _ = cc.closedir(dir);

        while (cc.readdir(dir)) |entry| {
            const ename: [*:0]const u8 = @ptrCast(&entry.*.d_name);
            const ename_slice = std.mem.span(ename);
            if (ename_slice.len == 0 or ename_slice[0] == '.') continue;

            const sub_path = std.fmt.allocPrint(self.allocator, "{s}/{s}", .{ dir_path, ename_slice }) catch continue;
            if (dirContainsModel(sub_path)) {
                return sub_path;
            }
            // Recurse one level for owner/model layout
            if (self.findFirstModelInDir(sub_path)) |p| {
                self.allocator.free(sub_path);
                return p;
            }
            self.allocator.free(sub_path);
        }
        return null;
    }

    fn findFirstModelDir(self: *Node) ?[]const u8 {
        if (!build_options.link_libc) {
            var dir = std.Io.Dir.cwd().openDir(std.Options.debug_io, self.config.models_dir, .{ .iterate = true }) catch return null;
            defer dir.close(std.Options.debug_io);
            var iter = dir.iterate();
            while (iter.next(std.Options.debug_io) catch null) |entry| {
                const name_slice = entry.name;
                if (name_slice.len == 0 or name_slice[0] == '.') continue;

                const sub_path = std.fmt.allocPrint(self.allocator, "{s}/{s}", .{ self.config.models_dir, name_slice }) catch continue;
                if (dirContainsModel(sub_path)) {
                    return sub_path;
                }
                self.allocator.free(sub_path);
            }
            return null;
        }

        const dir_z = self.allocator.dupeZ(u8, self.config.models_dir) catch return null;
        defer self.allocator.free(dir_z);

        const cc = c_file.c;
        const dir = cc.opendir(dir_z.ptr);
        if (dir == null) return null;
        defer _ = cc.closedir(dir);

        while (cc.readdir(dir)) |entry| {
            const name: [*:0]const u8 = @ptrCast(&entry.*.d_name);
            const name_slice = std.mem.span(name);
            if (name_slice.len == 0 or name_slice[0] == '.') continue;

            const sub_path = std.fmt.allocPrint(self.allocator, "{s}/{s}", .{ self.config.models_dir, name_slice }) catch continue;
            if (dirContainsModel(sub_path)) {
                return sub_path;
            }
            self.allocator.free(sub_path);
        }
        return null;
    }

    // --- ServerRouter handler methods ---
    // These implement the interface required by api.ServerRouter(Node).

    /// Acquire a request slot; returns 503 if inference admission is exhausted.
    fn acquireSlot(self: *Node, ctx: *httpx.Context) !?httpx.Response {
        return self.acquireSlotUnits(ctx, 1);
    }

    /// Mutate the shared admission state and account every rejection,
    /// including embedded/direct calls that do not have an HTTP response.
    fn acquireAdmissionUnits(self: *Node, units: usize) !void {
        const requested_units = self.inference_admission.requestedUnits(units);
        self.inference_admission.acquireUnits(units) catch |err| {
            self.metrics.incError();
            self.metrics.recordAdmissionRejection(requested_units);
            self.updateAdmissionMetrics();
            return err;
        };
        self.updateAdmissionMetrics();
    }

    fn reserveAdmissionUnits(self: *Node, units: usize) !void {
        const requested_units = self.inference_admission.requestedUnits(units);
        self.inference_admission.reserveUnits(units) catch |err| {
            self.metrics.incError();
            self.metrics.recordAdmissionRejection(requested_units);
            self.updateAdmissionMetrics();
            return err;
        };
        self.updateAdmissionMetrics();
    }

    fn growAdmissionUnits(self: *Node, current_units: usize, target_units: usize) !void {
        const requested_units = self.inference_admission.requestedUnits(target_units);
        self.inference_admission.growUnits(current_units, target_units) catch |err| {
            self.metrics.incError();
            self.metrics.recordAdmissionRejection(requested_units);
            self.updateAdmissionMetrics();
            return err;
        };
        self.updateAdmissionMetrics();
    }

    fn acquireSlotUnits(self: *Node, ctx: *httpx.Context, units: usize) !?httpx.Response {
        self.reserveAdmissionUnits(units) catch {
            const resp = try transientCapacityFailureResponse(
                ctx,
                "SERVICE_UNAVAILABLE",
                "server at capacity, try again later",
                "inference_admission",
            );
            return resp;
        };
        return null;
    }

    fn growSlotUnits(self: *Node, ctx: *httpx.Context, current_units: usize, target_units: usize) !?httpx.Response {
        self.growAdmissionUnits(current_units, target_units) catch {
            return try transientCapacityFailureResponse(
                ctx,
                "SERVICE_UNAVAILABLE",
                "server at capacity for expanded request workload, try again later",
                "inference_admission",
            );
        };
        return null;
    }

    fn releaseSlot(self: *Node) void {
        self.releaseSlotUnits(1);
    }

    fn releaseSlotUnits(self: *Node, units: usize) void {
        self.inference_admission.releaseReservedUnits(units);
        self.updateAdmissionMetrics();
    }

    fn releaseAdmission(self: *Node) void {
        self.releaseAdmissionUnits(1);
    }

    fn releaseAdmissionUnits(self: *Node, units: usize) void {
        self.inference_admission.releaseUnits(units);
        self.updateAdmissionMetrics();
    }

    /// Acquire the node-wide request-count permit shared by HTTP routes and
    /// embedding runtimes that forward inference work through the public API.
    pub fn tryAcquireRequestSlot(self: *Node) bool {
        self.inference_admission.acquireRequestSlot() catch {
            self.metrics.incError();
            self.metrics.recordAdmissionRejection(1);
            self.updateAdmissionMetrics();
            return false;
        };
        self.updateAdmissionMetrics();
        return true;
    }

    pub fn releaseRequestSlot(self: *Node) void {
        self.inference_admission.releaseRequestSlot();
        self.updateAdmissionMetrics();
    }

    fn updateAdmissionMetrics(self: *Node) void {
        // Admission mutations complete before they publish. Serialize the
        // publication and read the state only after taking this lock so an
        // older caller cannot overwrite a newer snapshot after being
        // descheduled between the mutation and metric update.
        spinLock(&self.admission_metrics_mutex);
        defer self.admission_metrics_mutex.unlock();
        const admission = self.inference_admission.stats();
        self.metrics.setAdmissionState(
            admission.capacity_requests,
            admission.capacity_units,
            admission.in_flight_units,
            admission.available_units,
            admission.in_flight_requests,
        );
    }

    fn estimateHttpRequestAdmissionUnits(self: *Node, ctx: *httpx.Context) usize {
        _ = self;
        const body_len = if (ctx.request.body) |body| body.len else 0;
        const bytes_per_unit: usize = 1024 * 1024;
        return 1 + (body_len / bytes_per_unit);
    }

    fn directGeneratePreflightForMessages(messages: []const generation.Message) !DirectGeneratePreflight {
        var preflight: DirectGeneratePreflight = .{};
        for (messages) |message| {
            preflight.text_bytes = std.math.add(
                usize,
                preflight.text_bytes,
                message.content.len,
            ) catch return error.RemoteContentTooLarge;
            if (message.image_bytes) |images| {
                for (images) |image_bytes| {
                    preflight.decoded_media_bytes = std.math.add(
                        usize,
                        preflight.decoded_media_bytes,
                        image_bytes.len,
                    ) catch return error.RemoteContentTooLarge;
                    preflight.media_count = std.math.add(
                        usize,
                        preflight.media_count,
                        1,
                    ) catch return error.RemoteContentTooLarge;
                    preflight.image_count = std.math.add(
                        usize,
                        preflight.image_count,
                        1,
                    ) catch return error.RemoteContentTooLarge;
                }
            }
            if (message.audio_bytes) |audio_clips| {
                for (audio_clips) |audio_bytes| {
                    preflight.decoded_media_bytes = std.math.add(
                        usize,
                        preflight.decoded_media_bytes,
                        audio_bytes.len,
                    ) catch return error.RemoteContentTooLarge;
                    preflight.media_count = std.math.add(
                        usize,
                        preflight.media_count,
                        1,
                    ) catch return error.RemoteContentTooLarge;
                    preflight.has_audio = true;
                }
            }
        }
        return preflight;
    }

    fn estimateGenerateAdmissionUnits(self: *Node, messages: []const generation.Message, max_tokens: i32) usize {
        _ = self;
        var text_bytes: usize = 0;
        var media_count: usize = 0;
        for (messages) |msg| {
            text_bytes = std.math.add(usize, text_bytes, msg.content.len) catch std.math.maxInt(usize);
            if (msg.image_bytes) |images| media_count = std.math.add(usize, media_count, images.len) catch std.math.maxInt(usize);
            if (msg.audio_bytes) |audio| media_count = std.math.add(usize, media_count, audio.len) catch std.math.maxInt(usize);
        }

        return estimateGenerateAdmissionUnitsFromShape(text_bytes, media_count, max_tokens);
    }

    fn estimateGenerateRequestAdmissionUnits(body: api.GenerateRequest, max_tokens: i32) usize {
        var text_bytes: usize = 0;
        var media_count: usize = 0;
        for (body.messages) |msg| {
            const content = msg.content orelse continue;
            switch (content) {
                .string => |text| text_bytes = std.math.add(usize, text_bytes, text.len) catch std.math.maxInt(usize),
                .array => |parts| for (parts.items) |part| {
                    if (part != .object) {
                        media_count = std.math.add(usize, media_count, 1) catch std.math.maxInt(usize);
                        continue;
                    }
                    const part_type = part.object.get("type") orelse {
                        media_count = std.math.add(usize, media_count, 1) catch std.math.maxInt(usize);
                        continue;
                    };
                    if (part_type == .string and std.mem.eql(u8, part_type.string, "text")) {
                        if (part.object.get("text")) |text_value| {
                            if (text_value == .string) {
                                text_bytes = std.math.add(usize, text_bytes, text_value.string.len) catch std.math.maxInt(usize);
                            }
                        }
                    } else {
                        media_count = std.math.add(usize, media_count, 1) catch std.math.maxInt(usize);
                    }
                },
                else => media_count = std.math.add(usize, media_count, 1) catch std.math.maxInt(usize),
            }
        }

        const base_units = estimateGenerateAdmissionUnitsFromShape(text_bytes, media_count, max_tokens);
        const speculation = parseGenerateSpeculationOptions(
            body.draft_model != null,
            body.speculative_k,
            body.speculation_policy,
            body.speculation_calibration,
        ) catch return base_units;
        return generateAdmissionUnitsForSpeculation(
            base_units,
            body.draft_model != null,
            speculation.policy,
        );
    }

    fn estimateGenerateBatchAdmissionUnitsPreflight(self: *Node, requests: []const api.GenerateBatchRequestItem, pending: []const bool) usize {
        _ = self;
        var total: usize = 1;
        for (requests, pending) |item, is_pending| {
            if (!is_pending) continue;
            const max_tokens: i32 = if (item.body.max_tokens) |value|
                if (value >= 1 and value <= std.math.maxInt(i32)) @intCast(value) else std.math.maxInt(i32)
            else
                256;
            total = std.math.add(
                usize,
                total,
                estimateGenerateRequestAdmissionUnits(item.body, max_tokens),
            ) catch std.math.maxInt(usize);
        }
        return total;
    }

    fn estimateGenerateAdmissionUnitsFromShape(text_bytes: usize, media_count: usize, max_tokens: i32) usize {
        const prompt_units = std.math.add(usize, 1, text_bytes / 2048) catch std.math.maxInt(usize);
        const decode_units: usize = @intCast(@max(@divTrunc(max_tokens, 256), 0));
        const media_units = std.math.mul(usize, media_count, 2) catch std.math.maxInt(usize);
        var total = std.math.add(usize, 1, prompt_units) catch std.math.maxInt(usize);
        total = std.math.add(usize, total, decode_units) catch std.math.maxInt(usize);
        return std.math.add(usize, total, media_units) catch std.math.maxInt(usize);
    }

    fn estimateGenerateBatchAdmissionUnits(
        self: *Node,
        requests: []const api.GenerateBatchRequestItem,
        owned_messages: []const OwnedGenerateMessages,
        pending: []const bool,
    ) usize {
        var total: usize = 1;
        for (requests, pending, 0..) |item, is_pending, idx| {
            if (!is_pending) continue;
            const max_tokens: i32 = if (item.body.max_tokens) |mt| @intCast(mt) else 256;
            const item_units = self.estimateGenerateAdmissionUnits(owned_messages[idx].messages, max_tokens);
            total = std.math.add(usize, total, item_units) catch std.math.maxInt(usize);
        }
        return total;
    }

    fn estimateGeneratePromptBytes(self: *Node, messages: []const generation.Message) usize {
        _ = self;
        var text_bytes: usize = 0;
        for (messages) |msg| {
            text_bytes += msg.content.len;
        }
        return text_bytes;
    }

    fn acquireNativeGenerateLease(
        self: *const Node,
        coordinator: *runtime.scheduler.native_generate.NativeGenerateCoordinator,
        admission: runtime.scheduler.native_generate.Admission,
    ) !runtime.scheduler.native_generate.Lease {
        coordinator.setPolicy(self.config.generation_batching.schedulerPolicy());
        return coordinator.acquire(admission);
    }

    /// Rendered prompt and tokenization retained from an admission estimate so
    /// the generation pipeline can skip re-rendering and re-encoding. Owns its
    /// memory; must stay alive until generation for the request completes.
    const NativePromptEstimate = struct {
        allocator: std.mem.Allocator,
        prompt: []const u8,
        encoded: @import("inference_tokenizer").EncodeResult,
        prompt_token_limit: usize,
        media_admission: generation.NativeGenerationMediaAdmission,

        fn deinit(self: *NativePromptEstimate) void {
            self.allocator.free(self.prompt);
            self.encoded.deinit();
        }
    };

    fn estimateNativePromptTokens(
        self: *Node,
        allocator: std.mem.Allocator,
        model_dir: []const u8,
        model: *model_manager_mod.LoadedModel,
        gpt_config: gpt_model_mod.Config,
        messages: []const generation.Message,
        max_tokens: i32,
        speculative_bonus_tokens: usize,
        enable_thinking: ?bool,
        out_estimate: ?*?NativePromptEstimate,
    ) !usize {
        _ = self;
        const prompt = if (model.chat_tmpl) |ct|
            try ct.applyWithOptions(allocator, messages, .{ .enable_thinking = enable_thinking })
        else
            try generation.formatMessages(allocator, messages);
        var prompt_owned = true;
        defer if (prompt_owned) allocator.free(prompt);
        const media_admission = try generation.nativeGenerationMediaAdmission(
            allocator,
            model_dir,
            messages,
            gpt_config,
        );
        const preliminary_media_allowance = generation.nativeGenerationPreliminaryMediaTokenAllowance(messages, gpt_config);
        const prompt_token_limit = try generation.nativeGenerationPromptTokenLimit(
            gpt_config,
            null,
            @intCast(@max(max_tokens, 1)),
            speculative_bonus_tokens,
            preliminary_media_allowance,
        );
        var encoded = try generation.encodeNativeGenerationPrompt(
            model.getTokenizer(),
            allocator,
            prompt,
            prompt_token_limit,
            model.manifest.add_bos_token,
            model.manifest.bos_token,
        );
        var encoded_owned = true;
        defer if (encoded_owned) encoded.deinit();
        var count: usize = 0;
        while (count < encoded.attention_mask.len and encoded.attention_mask[count] != 0) : (count += 1) {}
        const total = std.math.add(usize, count, media_admission.token_allowance) catch return error.PromptTooLong;
        if (out_estimate) |out| {
            out.* = .{
                .allocator = allocator,
                .prompt = prompt,
                .encoded = encoded,
                .prompt_token_limit = prompt_token_limit,
                .media_admission = media_admission,
            };
            prompt_owned = false;
            encoded_owned = false;
        }
        return total;
    }

    pub fn generateEmbeddings(self: *Node, ctx: *httpx.Context) !httpx.Response {
        return self.createEmbedding(ctx);
    }

    pub fn createEmbedding(self: *Node, ctx: *httpx.Context) !httpx.Response {
        var parsed = (try ctx.parseJson(std.json.Value)) orelse
            return ctx.status(400).json(.{ .@"error" = "missing_body", .message = "Request body required" });
        defer parsed.deinit();
        const request = parseEmbedRequest(parsed.value) catch |err| {
            return ctx.status(400).json(.{
                .@"error" = "INVALID_REQUEST",
                .message = embedRequestParseErrorMessage(err),
            });
        };

        validateEmbeddingEncodingFormat(request.encoding_format) catch {
            return ctx.status(400).json(.{
                .@"error" = "INVALID_REQUEST",
                .message = "encoding_format must be \"float\"",
            });
        };
        const requested_dimensions = parseRequestedEmbeddingDimensions(request.dimensions) catch {
            return ctx.status(400).json(.{
                .@"error" = "INVALID_REQUEST",
                .message = "dimensions must be a positive integer",
            });
        };

        const media_admission = requestMediaAdmission(self, denseEmbedRequestMediaShape(request.input));
        const admission_units = @max(self.estimateHttpRequestAdmissionUnits(ctx), media_admission.units);
        if (try self.acquireSlotUnits(ctx, admission_units)) |resp| return resp;
        var reserved_units = admission_units;
        defer self.releaseSlotUnits(reserved_units);
        self.metrics.incRequest("embed");
        defer self.metrics.decActive();

        // Read only the lightweight manifest first so media can be admitted
        // before loading tokenizer, weights, or accelerator sessions.
        const model_name: ?[]const u8 = if (request.model.len > 0) request.model else null;
        const model_path = self.resolveRequestModelPath(ctx.allocator, ctx.io, model_name, "embedders") catch |err|
            return requestModelResolutionError(ctx, err);
        defer ctx.allocator.free(model_path);

        var admission_manifest = manifest_mod.loadFromDir(ctx.allocator, model_path) catch |err|
            return modelLoadFailureResponse(ctx, err);
        defer admission_manifest.deinit();

        if (admission_manifest.hasCapability("sparse")) {
            const sparse_texts = parseSparseEmbedInputs(ctx.allocator, request.input) catch {
                return ctx.status(400).json(.{
                    .@"error" = "INVALID_REQUEST",
                    .message = "sparse models only support text input (string or array of strings)",
                });
            };
            defer ctx.allocator.free(sparse_texts);

            if (sparse_texts.len == 0) {
                return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = "input is empty" });
            }
            if (requested_dimensions != null) {
                return ctx.status(400).json(.{
                    .@"error" = "INVALID_REQUEST",
                    .message = "dimensions is not supported for sparse embedding models",
                });
            }
            if (try rejectDisallowedModel(self, ctx, model_path)) |response| return response;
            const Attempt = struct {
                allocator: std.mem.Allocator,
                io: ?std.Io,
                texts: []const []const u8,
                vectors: ?[]DirectSparseEmbedding = null,
                prompt_tokens: usize = 0,

                fn run(attempt_ctx: *anyopaque, model: *model_manager_mod.LoadedModel) !void {
                    const attempt: *@This() = @ptrCast(@alignCast(attempt_ctx));
                    if (!model.manifest.hasCapability("sparse")) return error.UnsupportedEmbeddingProvider;
                    var pipeline = sparse_embedding_mod.SparseEmbeddingPipeline{
                        .allocator = attempt.allocator,
                        .session = model.session,
                        .tok = model.getTokenizer(),
                        .config = sparse_embedding_mod.SparseEmbeddingConfig.fromManifest(&model.manifest),
                        .execution_lock = model.embeddingExecutionLock(),
                    };
                    const vectors = try pipeline.embed(attempt.texts);
                    errdefer {
                        for (vectors) |*item| item.deinit(attempt.allocator);
                        attempt.allocator.free(vectors);
                    }
                    attempt.prompt_tokens = countTokenizerTexts(attempt.allocator, attempt.io, model.getTokenizer(), attempt.texts) catch estimateTextsTokens(attempt.texts);
                    attempt.vectors = vectors;
                }
            };
            var attempt = Attempt{
                .allocator = ctx.allocator,
                .io = self.session_manager.io,
                .texts = sparse_texts,
            };
            runLoadedEmbeddingRuntimeWithRecovery(self, model_path, .{}, &attempt, Attempt.run) catch |err|
                return inferenceFailureResponse(ctx, err);
            const sparse_vecs = attempt.vectors.?;
            defer {
                for (sparse_vecs) |*sv| @constCast(sv).deinit(ctx.allocator);
                ctx.allocator.free(sparse_vecs);
            }

            var arena = std.heap.ArenaAllocator.init(ctx.allocator);
            defer arena.deinit();
            const response = try buildEmbedSparseResponse(arena.allocator(), request.model, sparse_vecs, attempt.prompt_tokens);
            return ctx.json(response);
        }

        var media_budget = RequestMediaBudget.init(media_admission.byte_cap);
        const download_context = DenseEmbedRequestContext{ .io = ctx.io };
        var inputs = switch (request.error_policy) {
            .fail_fast => parseDenseEmbedInputsWithBudgetAndContext(self, ctx.allocator, &admission_manifest, request.input, &media_budget, download_context),
            .per_item => parseDenseEmbedInputsPerItemWithBudgetAndContext(self, ctx.allocator, &admission_manifest, request.input, &media_budget, download_context),
        } catch |err| {
            if (isRemoteContentRequestError(err)) return remoteContentErrorResponse(ctx, err);
            if (isDenseEmbedRequestAbort(err)) return err;
            return ctx.status(400).json(.{
                .@"error" = "INVALID_REQUEST",
                .message = embedInputParseErrorMessage(err),
            });
        };
        defer inputs.deinit(ctx.allocator);

        if (inputs.total_count == 0) {
            return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = "input is empty" });
        }

        var audio_decode_working_bytes = default_max_audio_decode_working_bytes;

        if (inputs.images.items.len > 0) {
            var decoded_budget = ReadDecodedImageBudget.init(media_admission, effectiveRequestContentSecurity(self).max_image_dimension);
            for (inputs.images.items) |image| decoded_budget.addImage(image.bytes) catch |err|
                return readImageErrorResponse(ctx, err);
            const required_units = @max(reserved_units, decoded_budget.requiredUnits());
            if (try self.growSlotUnits(ctx, reserved_units, required_units)) |resp| return resp;
            reserved_units = required_units;
        }

        if (inputs.audio.items.len > 0) {
            const audio_admission = audioDecodeAdmission(self, media_admission.resident_byte_cap);
            const required_units = @max(reserved_units, audio_admission.units);
            if (try self.growSlotUnits(ctx, reserved_units, required_units)) |resp| return resp;
            reserved_units = required_units;
            audio_decode_working_bytes = audio_admission.max_decode_working_bytes;
        }

        if (try rejectDisallowedModel(self, ctx, model_path)) |response| return response;
        const ExecutionResult = union(EmbedErrorPolicy) {
            fail_fast: [][]f32,
            per_item: DenseEmbedPartialResult,
        };
        const Attempt = struct {
            allocator: std.mem.Allocator,
            io: ?std.Io,
            inputs: *ParsedDenseEmbedInputs,
            request: ParsedEmbedRequest,
            audio_decode_working_bytes: usize,
            result: ?ExecutionResult = null,
            prompt_tokens: usize = 0,

            fn run(attempt_ctx: *anyopaque, model: *model_manager_mod.LoadedModel) !void {
                const attempt: *@This() = @ptrCast(@alignCast(attempt_ctx));
                if (model.manifest.hasCapability("sparse")) return error.UnsupportedEmbeddingProvider;
                var asset_lease = model.acquireEmbeddingAssetLease(attempt.inputs.audio.items.len > 0);
                defer asset_lease.release();
                var pipeline = try prepareInitialDenseEmbeddingPipeline(model, attempt.allocator, attempt.inputs);
                var audio_asset_guard = AudioEmbeddingAssetGuard.init(
                    model,
                    attempt.inputs.audio.items.len > 0,
                );
                defer audio_asset_guard.deinit();
                pipeline.config.max_audio_decode_working_bytes = attempt.audio_decode_working_bytes;
                const owned_request_prefix = try applyDenseEmbeddingRequestOptions(
                    attempt.allocator,
                    &pipeline,
                    &model.manifest,
                    attempt.request,
                );
                defer if (owned_request_prefix) |prefix| attempt.allocator.free(prefix);
                attempt.prompt_tokens = if (attempt.inputs.texts.items.len > 0)
                    countParsedDenseEmbedTextTokens(attempt.allocator, attempt.io, model.getTokenizer(), attempt.inputs)
                else
                    estimateParsedDenseEmbedPromptTokens(attempt.inputs);

                attempt.result = switch (attempt.request.error_policy) {
                    .fail_fast => .{ .fail_fast = try embedDenseInputs(
                        attempt.allocator,
                        &pipeline,
                        attempt.inputs,
                        model,
                        &audio_asset_guard,
                        &asset_lease,
                    ) },
                    .per_item => .{ .per_item = try embedDenseInputsPartial(
                        attempt.allocator,
                        &pipeline,
                        attempt.inputs,
                        model,
                        &audio_asset_guard,
                        &asset_lease,
                    ) },
                };
                asset_lease.release();
            }
        };
        var attempt = Attempt{
            .allocator = ctx.allocator,
            .io = self.session_manager.io,
            .inputs = &inputs,
            .request = request,
            .audio_decode_working_bytes = audio_decode_working_bytes,
        };
        var failure_stage: EmbeddingRuntimeFailureStage = .acquire;
        const pipeline_start = embedTimingStart();
        runLoadedEmbeddingRuntimeWithRecovery(self, model_path, .{
            .failure_stage = &failure_stage,
        }, &attempt, Attempt.run) catch |err| {
            if (isEmbedRequestOptionError(err)) {
                return ctx.status(400).json(.{
                    .@"error" = "INVALID_REQUEST",
                    .message = embedRequestOptionErrorMessage(err),
                });
            }
            return switch (failure_stage) {
                .acquire => modelLoadFailureResponse(ctx, err),
                .execute => embedDenseInputFailureResponse(ctx, err),
            };
        };
        logEmbedTiming("embed.pipeline", inputs.total_count, pipeline_start);
        var arena = std.heap.ArenaAllocator.init(ctx.allocator);
        defer arena.deinit();
        const response_build_start = embedTimingStart();

        switch (attempt.result.?) {
            .fail_fast => |embeddings| {
                defer {
                    for (embeddings) |e| ctx.allocator.free(e);
                    ctx.allocator.free(embeddings);
                }
                const response = buildEmbedDenseResponse(arena.allocator(), request.model, embeddings, requested_dimensions, admission_manifest.normalize, attempt.prompt_tokens) catch |err| switch (err) {
                    error.InvalidEmbeddingDimensions => {
                        return ctx.status(400).json(.{
                            .@"error" = "INVALID_REQUEST",
                            .message = "dimensions exceeds the model embedding size",
                        });
                    },
                    else => return err,
                };
                logEmbedTiming("embed.response_build", inputs.total_count, response_build_start);
                const response_json_start = embedTimingStart();
                const http_response = try ctx.json(response);
                logEmbedTiming("embed.response_json", inputs.total_count, response_json_start);
                return http_response;
            },
            .per_item => |partial_value| {
                var partial = partial_value;
                defer partial.deinit(ctx.allocator);
                const response = buildEmbedDensePartialResponse(arena.allocator(), request.model, &partial, requested_dimensions, admission_manifest.normalize, attempt.prompt_tokens) catch |err| switch (err) {
                    error.InvalidEmbeddingDimensions => {
                        return ctx.status(400).json(.{
                            .@"error" = "INVALID_REQUEST",
                            .message = "dimensions exceeds the model embedding size",
                        });
                    },
                    else => return err,
                };
                logEmbedTiming("embed.response_build", inputs.total_count, response_build_start);
                const response_json_start = embedTimingStart();
                const http_response = try ctx.json(response);
                logEmbedTiming("embed.response_json", inputs.total_count, response_json_start);
                return http_response;
            },
        }
    }

    pub fn chunkText(self: *Node, ctx: *httpx.Context) !httpx.Response {
        var parsed = (ctx.parseJson(api.ChunkRequest) catch |err|
            return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = chunkRequestParseErrorMessage(err) })) orelse
            return ctx.status(400).json(.{ .@"error" = "missing_body", .message = "Request body required" });
        defer parsed.deinit();
        const body = parsed.value;
        const admission_units = self.estimateHttpRequestAdmissionUnits(ctx);
        if (try self.acquireSlotUnits(ctx, admission_units)) |resp| return resp;
        defer self.releaseSlotUnits(admission_units);
        self.metrics.incRequest("chunk");
        defer self.metrics.decActive();

        const input = parseChunkRequestInput(ctx.allocator, body.input) catch |err|
            return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = chunkInputParseErrorMessage(err) });
        defer deinitChunkRequestInput(ctx.allocator, input);

        var config = lib_chunker.FixedChunkConfig{};
        if (body.config) |cfg| {
            if (cfg.model) |model| config.model = model;
            if (cfg.max_chunks) |max_chunks| config.max_chunks = @intCast(max_chunks);
            config.threshold = cfg.threshold;
            if (cfg.text) |text_cfg| {
                if (text_cfg.target_tokens) |tt| config.text.target_tokens = @intCast(tt);
                if (text_cfg.overlap_tokens) |ot| config.text.overlap_tokens = @intCast(ot);
                if (text_cfg.separator) |separator| config.text.separator = separator;
            }
            if (cfg.audio) |audio_cfg| {
                if (audio_cfg.window_duration_ms) |window| config.audio.window_duration_ms = @intCast(window);
                if (audio_cfg.overlap_duration_ms) |overlap| config.audio.overlap_duration_ms = @intCast(overlap);
            }
        }

        const chunks = lib_chunker.fixed_multimodal.chunkInput(ctx.allocator, input, config) catch |err|
            return ctx.status(500).json(.{ .@"error" = "CHUNKING_FAILED", .message = internalErrorMessage("CHUNKING_FAILED", err) });
        defer lib_chunker.types.freeChunks(ctx.allocator, chunks);

        const api_chunks = try ctx.allocator.alloc(api.ChunkObject, chunks.len);
        defer ctx.allocator.free(api_chunks);

        // Base64-encoded copies of binary chunks; kept alive until ctx.json serializes.
        var encoded_datas = std.ArrayListUnmanaged([]u8).empty;
        defer {
            for (encoded_datas.items) |e| ctx.allocator.free(e);
            encoded_datas.deinit(ctx.allocator);
        }

        for (chunks, 0..) |chunk, i| {
            var encoded_data: ?[]const u8 = null;
            if (chunk.data) |data| {
                const encoded_len = std.base64.standard.Encoder.calcSize(data.len);
                const encoded = try ctx.allocator.alloc(u8, encoded_len);
                errdefer ctx.allocator.free(encoded);
                _ = std.base64.standard.Encoder.encode(encoded, data);
                try encoded_datas.append(ctx.allocator, encoded);
                encoded_data = encoded;
            }

            api_chunks[i] = .{
                .object = "chunk",
                .index = @intCast(i),
                .id = @intCast(chunk.id),
                .mime_type = chunk.mime_type,
                .text = chunk.text,
                .start_char = if (chunk.start_char) |v| @intCast(v) else null,
                .end_char = if (chunk.end_char) |v| @intCast(v) else null,
                .data = encoded_data,
                .start_time_ms = chunk.start_time_ms,
                .end_time_ms = chunk.end_time_ms,
                .frame_index = if (chunk.frame_index) |v| @intCast(v) else null,
                .frame_delay_ms = if (chunk.frame_delay_ms) |v| @intCast(v) else null,
            };
        }

        const prompt_tokens = switch (input) {
            .text => |text| estimateTextTokens(text),
            .binary => 0,
        };

        return ctx.json(api.ChunkResponse{
            .object = "list",
            .data = api_chunks,
            .model = if (config.model.len > 0) config.model else "fixed-bert-tokenizer",
            .usage = tokenUsage(prompt_tokens, 0),
            .cache_hit = false,
        });
    }

    pub fn rerankPrompts(self: *Node, ctx: *httpx.Context) !httpx.Response {
        var parsed = (try ctx.parseJson(api.RerankRequest)) orelse
            return ctx.status(400).json(.{ .@"error" = "missing_body", .message = "Request body required" });
        defer parsed.deinit();
        const body = parsed.value;
        if (try self.acquireSlot(ctx)) |resp| return resp;
        defer self.releaseSlot();
        self.metrics.incRequest("rerank");
        defer self.metrics.decActive();

        const model_name: ?[]const u8 = if (body.model.len > 0) body.model else null;
        const model_path = self.resolveRequestModelPath(ctx.allocator, ctx.io, model_name, "rerankers") catch |err|
            return requestModelResolutionError(ctx, err);
        defer ctx.allocator.free(model_path);

        var model_handle = self.model_manager.acquireFromDir(model_path) catch |err|
            return modelLoadFailureResponse(ctx, err);
        defer model_handle.release();
        const model = model_handle.get();

        var pipeline = model.rerankingPipeline(ctx.allocator);
        const scores = pipeline.rerank(body.query, body.prompts) catch |err|
            return inferenceFailureResponse(ctx, err);
        defer ctx.allocator.free(scores);

        const prompt_tokens =
            (countTokenizerTokens(ctx.allocator, self.session_manager.io, model.getTokenizer(), body.query) catch estimateTextTokens(body.query)) * body.prompts.len +
            (countTokenizerTexts(ctx.allocator, self.session_manager.io, model.getTokenizer(), body.prompts) catch estimateTextsTokens(body.prompts));
        return writeRerankScoresResponse(ctx, body.model, scores, prompt_tokens);
    }

    pub fn rerankMultimodalPrompts(self: *Node, ctx: *httpx.Context) !httpx.Response {
        var parsed_body = (try ctx.parseJson(api.RerankMultimodalRequest)) orelse
            return ctx.status(400).json(.{ .@"error" = "missing_body", .message = "Request body required" });
        defer parsed_body.deinit();
        const body = parsed_body.value;
        const media_shape = multimodalRerankRequestMediaShape(body);
        const media_admission = requestMediaAdmission(self, media_shape);
        if (try self.acquireSlotUnits(ctx, media_admission.units)) |resp| return resp;
        var reserved_units = media_admission.units;
        defer self.releaseSlotUnits(reserved_units);
        self.metrics.incRequest("rerank");
        defer self.metrics.decActive();

        if (body.documents.len == 0) {
            return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = "documents must not be empty" });
        }

        const model_name: ?[]const u8 = if (body.model.len > 0) body.model else null;
        const model_path = self.resolveRequestModelPath(ctx.allocator, ctx.io, model_name, "rerankers") catch |err|
            return requestModelResolutionError(ctx, err);
        defer ctx.allocator.free(model_path);

        // Reject a text-only model from its lightweight manifest before
        // fetching request media or loading weights and accelerator sessions.
        if (media_shape.image_count > 0) {
            var admission_manifest = manifest_mod.loadFromDir(ctx.allocator, model_path) catch |err|
                return modelLoadFailureResponse(ctx, err);
            defer admission_manifest.deinit();
            const supports_qwen3vl_pointwise = admission_manifest.isQwen3VlRerankerGgufBundle() and
                admission_manifest.gguf_projector_path != null;
            if (!(supports_qwen3vl_pointwise or admission_manifest.hasCapability("colqwen") or admission_manifest.hasCapability("multimodal_late_interaction"))) {
                return ctx.status(400).json(.{
                    .@"error" = "MODEL_NOT_SUPPORTED",
                    .message = "model does not advertise a supported multimodal reranking capability",
                });
            }
        }

        var parsed_docs = std.ArrayListUnmanaged(ParsedMultimodalRerankDocument).empty;
        defer {
            for (parsed_docs.items) |*doc| doc.deinit();
            parsed_docs.deinit(ctx.allocator);
        }

        var image_count: usize = 0;
        var media_budget = RequestMediaBudget.init(media_admission.byte_cap);
        for (body.documents) |doc| {
            const parsed = parseChatMessageContentToTextAndImagesWithBudget(self, ctx.allocator, doc.content, &media_budget) catch |err| switch (err) {
                error.InvalidImageDataUri => return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = "invalid image data URI" }),
                error.RemoteContentTooLarge,
                error.RemoteContentNotAllowed,
                error.RemoteContentInvalid,
                error.RemoteContentNotConfigured,
                error.RemoteContentUnavailable,
                => return remoteContentErrorResponse(ctx, err),
                error.UnsupportedContentPartType => return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = "multimodal rerank documents only support text and image content parts" }),
                error.OutOfMemory, error.Timeout, error.Canceled => return err,
                else => return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = "invalid multimodal rerank document content" }),
            };
            image_count = std.math.add(usize, image_count, parsed.images.len) catch std.math.maxInt(usize);
            try parsed_docs.append(ctx.allocator, parsed);
        }

        if (image_count > 0) {
            var decoded_budget = ReadDecodedImageBudget.init(media_admission, effectiveRequestContentSecurity(self).max_image_dimension);
            for (parsed_docs.items) |doc| for (doc.images) |image| decoded_budget.addImage(image) catch |err|
                return readImageErrorResponse(ctx, err);
            const required_units = @max(reserved_units, decoded_budget.requiredUnits());
            if (try self.growSlotUnits(ctx, reserved_units, required_units)) |resp| return resp;
            reserved_units = required_units;
        }

        var model_handle = self.model_manager.acquireFromDir(model_path) catch |err|
            return modelLoadFailureResponse(ctx, err);
        defer model_handle.release();
        const model = model_handle.get();

        if (image_count == 0) {
            const flat_texts = try ctx.allocator.alloc([]const u8, parsed_docs.items.len);
            defer ctx.allocator.free(flat_texts);
            for (parsed_docs.items, 0..) |doc, idx| flat_texts[idx] = doc.text;

            var pipeline = model.rerankingPipeline(ctx.allocator);
            const scores = pipeline.rerank(body.query, flat_texts) catch |err|
                return inferenceFailureResponse(ctx, err);
            defer ctx.allocator.free(scores);
            const prompt_tokens =
                (countTokenizerTokens(ctx.allocator, self.session_manager.io, model.getTokenizer(), body.query) catch estimateTextTokens(body.query)) * flat_texts.len +
                (countTokenizerTexts(ctx.allocator, self.session_manager.io, model.getTokenizer(), flat_texts) catch estimateTextsTokens(flat_texts));
            return writeRerankScoresResponse(ctx, body.model, scores, prompt_tokens);
        }

        if (model.manifest.isQwen3VlReranker()) {
            if (!model.manifest.isQwen3VlRerankerGgufBundle()) {
                return ctx.status(400).json(.{
                    .@"error" = "MODEL_NOT_SUPPORTED",
                    .message = "Qwen3-VL safetensors rerankers are text-only; multimodal reranking requires a qualified GGUF projector bundle",
                });
            }
            const projector_path = model.manifest.gguf_projector_path orelse
                return ctx.status(400).json(.{ .@"error" = "MODEL_NOT_SUPPORTED", .message = "Qwen3-VL reranker bundle is missing its GGUF projector" });
            const gpt_cfg = session_factory.getGptConfig(model.session) orelse
                return ctx.status(400).json(.{ .@"error" = "MODEL_NOT_SUPPORTED", .message = "Qwen3-VL multimodal reranking requires a native GPT decoder session" });
            const projector_media = qwen3VlRerankerMediaAdmission(
                parsed_docs.items,
                gpt_cfg,
                @min(model.manifest.maxTextSequenceLength(), qwen3vl_reranker.default_max_length),
            ) catch return ctx.status(400).json(.{
                .@"error" = "INVALID_REQUEST",
                .message = "Qwen3-VL reranker image geometry exceeds the supported admission envelope",
            });
            const backend_kind: runtime.kv.pool.BackendKind = switch (model.session.backend()) {
                .native => .native,
                .metal => .metal,
                .cuda => .cuda,
                else => return ctx.status(400).json(.{
                    .@"error" = "MODEL_NOT_SUPPORTED",
                    .message = "Qwen3-VL multimodal reranking requires a native, Metal, or CUDA decoder session",
                }),
            };
            const backend_class: runtime.tier.memory.BackendClass = switch (backend_kind) {
                .native => .cpu,
                .metal, .cuda => .gpu,
            };
            const admission_limits = self.config.generation_budget_overrides.apply(
                session_factory.widenBudgetLimitsForSession(
                    model.session,
                    self.defaultGenerationLimits(backend_class),
                ),
            );
            const admission_amounts = model_manager_mod.projectorRunAdmissionAmounts(
                model.manifest,
                backend_kind,
                projector_media,
            ) catch |err| return inferenceFailureResponse(ctx, err);
            var resource_lease = self.model_manager.acquireRunResourceAmounts(
                backend_class,
                admission_limits,
                admission_amounts,
            ) catch |err| return inferenceFailureResponse(ctx, err);
            defer resource_lease.release();

            // Projector and decoder calls share the loaded model's stateful
            // accelerator runtime. Serialize their complete lifetime while
            // leaving request parsing and media admission outside the lane.
            const execution_mutex = model.targetInferenceExecutionMutex();
            if (execution_mutex) |mutex| platform.sync.lockYieldingIo(mutex, ctx.io);
            defer if (execution_mutex) |mutex| mutex.unlock();

            var cb = session_factory.getComputeBackend(model.session, ctx.allocator) catch |err|
                return ctx.status(400).json(.{ .@"error" = "MODEL_NOT_SUPPORTED", .message = @errorName(err) });
            defer cb.deinit();
            var qwen_pipeline = qwen3vl_multimodal_reranker.Pipeline.init(
                ctx.allocator,
                &cb,
                model.getTokenizer(),
                gpt_cfg,
                projector_path,
                .{
                    .max_length = @min(model.manifest.maxTextSequenceLength(), qwen3vl_reranker.default_max_length),
                },
            ) catch |err|
                return ctx.status(400).json(.{ .@"error" = "MODEL_NOT_SUPPORTED", .message = @errorName(err) });

            const scores = try ctx.allocator.alloc(f32, parsed_docs.items.len);
            defer ctx.allocator.free(scores);
            var prompt_tokens: usize = 0;
            for (parsed_docs.items, 0..) |doc, idx| {
                if (doc.images.len == 0) {
                    var text_pipeline = model.rerankingPipeline(ctx.allocator);
                    text_pipeline.execution_lock = null;
                    const text_scores = text_pipeline.rerank(body.query, &.{doc.text}) catch |err|
                        return inferenceFailureResponse(ctx, err);
                    defer ctx.allocator.free(text_scores);
                    scores[idx] = text_scores[0];
                    const text_tokens =
                        (countTokenizerTokens(ctx.allocator, self.session_manager.io, model.getTokenizer(), body.query) catch estimateTextTokens(body.query)) +
                        (countTokenizerTokens(ctx.allocator, self.session_manager.io, model.getTokenizer(), doc.text) catch estimateTextTokens(doc.text));
                    prompt_tokens = std.math.add(usize, prompt_tokens, text_tokens) catch
                        return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = "rerank token accounting overflow" });
                    continue;
                }

                const result = qwen_pipeline.scoreDocument(
                    body.query,
                    doc.qwen_content,
                    doc.images,
                ) catch |err| switch (err) {
                    error.InvalidRerankerImageCount,
                    error.ImageLimitExceeded,
                    error.InputTokenLimitExceeded,
                    error.RerankerPromptTooLarge,
                    error.InvalidRerankerPromptLimit,
                    error.InvalidRerankerSequence,
                    error.InvalidRerankerTokenId,
                    error.ImagePlaceholderCountMismatch,
                    error.SpecialTokenBudgetExceeded,
                    error.InvalidRerankerMaxLength,
                    => return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = @errorName(err) }),
                    error.InvalidMultimodalConfig,
                    error.ImageProjectionSizeMismatch,
                    error.ImageTokenLengthMismatch,
                    error.InvalidPreparedPrompt,
                    error.InvalidDeepstackContext,
                    error.InvalidRerankerInputShape,
                    error.InvalidRerankerScoreShape,
                    error.UnsupportedRerankerArchitecture,
                    => return ctx.status(400).json(.{ .@"error" = "MODEL_NOT_SUPPORTED", .message = @errorName(err) }),
                    else => return inferenceFailureResponse(ctx, err),
                };
                scores[idx] = result.score;
                prompt_tokens = std.math.add(usize, prompt_tokens, result.prompt_tokens) catch
                    return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = "rerank token accounting overflow" });
            }
            return writeRerankScoresResponse(ctx, body.model, scores, prompt_tokens);
        }

        if (!(model.manifest.hasCapability("colqwen") or model.manifest.hasCapability("multimodal_late_interaction"))) {
            return ctx.status(400).json(.{
                .@"error" = "MODEL_NOT_SUPPORTED",
                .message = "model does not advertise multimodal late-interaction reranking capability",
            });
        }

        model.ensureVisionSession() catch |err|
            return inferenceFailureResponse(ctx, err);
        const vision_session = model.vision_session;
        const gpt_cfg = session_factory.getGptConfig(model.session) orelse
            return ctx.status(400).json(.{ .@"error" = "MODEL_NOT_SUPPORTED", .message = "multimodal late-interaction reranking currently requires a native qwen/gpt text session" });
        if (vision_session == null and !gpt_cfg.supportsNativeQwen2VlVision()) {
            return ctx.status(400).json(.{ .@"error" = "MODEL_NOT_SUPPORTED", .message = "model lacks both visual_model.onnx and native qwen2-vl vision config" });
        }
        const prep_cfg = multimodal_qwen_adapter.loadPreprocessorConfig(ctx.allocator, model_path) catch
            return ctx.status(400).json(.{ .@"error" = "MODEL_NOT_SUPPORTED", .message = "model preprocessor configuration is unsupported" });

        // The compute backend and resident slots below share the loaded
        // model's stateful GPU runtime. Hold the target-model lane for their
        // complete lifetime; request parsing and image preparation remain
        // outside it.
        const execution_mutex = model.targetInferenceExecutionMutex();
        if (execution_mutex) |mutex| platform.sync.lockYieldingIo(mutex, ctx.io);
        defer if (execution_mutex) |mutex| mutex.unlock();

        var cb = session_factory.getComputeBackend(model.session, ctx.allocator) catch |err|
            return ctx.status(400).json(.{ .@"error" = "MODEL_NOT_SUPPORTED", .message = @errorName(err) });
        defer cb.deinit();

        var mm_pipeline = multimodal_reranker.Pipeline.init(
            ctx.allocator,
            &cb,
            vision_session,
            model.getTokenizer(),
            gpt_cfg,
            prep_cfg,
            model.manifest.max_position_embeddings,
            model.manifest.add_bos_token,
            .{ .distributed = runtime.distributed.configFromEnv() },
        );

        var query_encoded = mm_pipeline.encodeQueryText(body.query) catch |err|
            return inferenceFailureResponse(ctx, err);
        defer query_encoded.deinit();

        const scores = try ctx.allocator.alloc(f32, parsed_docs.items.len);
        defer ctx.allocator.free(scores);

        for (parsed_docs.items, 0..) |doc, idx| {
            if (doc.images.len == 0) {
                var text_pipeline = model.rerankingPipeline(ctx.allocator);
                // This request already owns the non-reentrant model lane.
                text_pipeline.execution_lock = null;
                const text_scores = text_pipeline.rerank(body.query, &.{doc.text}) catch |err|
                    return inferenceFailureResponse(ctx, err);
                defer ctx.allocator.free(text_scores);
                scores[idx] = text_scores[0];
                continue;
            }

            scores[idx] = mm_pipeline.scoreDocumentText(
                query_encoded,
                doc.text,
                doc.images,
            ) catch |err| switch (err) {
                error.InvalidImageDataUri, error.UnsupportedContentPartType => return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = @errorName(err) }),
                error.ImageTokenLengthMismatch, error.ImageProjectionSizeMismatch, error.UnexpectedOutputShape => return ctx.status(400).json(.{ .@"error" = "MODEL_NOT_SUPPORTED", .message = @errorName(err) }),
                else => return inferenceFailureResponse(ctx, err),
            };
        }

        var doc_texts = try ctx.allocator.alloc([]const u8, parsed_docs.items.len);
        defer ctx.allocator.free(doc_texts);
        for (parsed_docs.items, 0..) |doc, idx| doc_texts[idx] = doc.text;
        const prompt_tokens =
            (countTokenizerTokens(ctx.allocator, self.session_manager.io, model.getTokenizer(), body.query) catch estimateTextTokens(body.query)) * doc_texts.len +
            (countTokenizerTexts(ctx.allocator, self.session_manager.io, model.getTokenizer(), doc_texts) catch estimateTextsTokens(doc_texts));
        return writeRerankScoresResponse(ctx, body.model, scores, prompt_tokens);
    }

    pub fn generateContent(self: *Node, ctx: *httpx.Context) !httpx.Response {
        const raw_body = (try ctx.body()) orelse
            return ctx.status(400).json(.{ .@"error" = "missing_body", .message = "Request body required" });
        // Single JSON parse per request: only bodies that can carry
        // chat_template_kwargs (substring pre-gate, false positives harmless)
        // are parsed as an untyped Value, which then doubles as the kwargs
        // validator input and the typed-parse source. Malformed bodies fall
        // back to the raw typed parse so their errors stay identical.
        var parsed_body_value: ?std.json.Parsed(std.json.Value) = null;
        defer if (parsed_body_value) |*owned| owned.deinit();
        if (std.mem.indexOf(u8, raw_body, "chat_template_kwargs") != null) {
            parsed_body_value = std.json.parseFromSlice(std.json.Value, ctx.allocator, raw_body, .{}) catch null;
        }
        if (parsed_body_value) |owned| {
            if (!generateRequestChatTemplateKwargsAreValid(owned.value)) {
                return ctx.status(400).json(.{
                    .@"error" = "INVALID_REQUEST",
                    .message = "chat_template_kwargs accepts only a boolean enable_thinking field",
                });
            }
        }
        var parsed = if (parsed_body_value) |owned|
            try std.json.parseFromValue(api.GenerateRequest, ctx.allocator, owned.value, .{ .ignore_unknown_fields = true })
        else
            (try ctx.parseJson(api.GenerateRequest)) orelse
                return ctx.status(400).json(.{ .@"error" = "missing_body", .message = "Request body required" });
        defer parsed.deinit();
        const body = parsed.value;
        if (body.prompt_cache_key) |key| {
            if (key.len > runtime.kv.prompt_cache.max_namespace_bytes) {
                return ctx.status(400).json(.{
                    .@"error" = "INVALID_REQUEST",
                    .message = "prompt_cache_key is too long",
                });
            }
        }
        self.metrics.incRequest("generate");
        defer self.metrics.decActive();

        const numeric = parseGenerateNumericOptions(body.max_tokens, body.top_k) catch |err| {
            return ctx.status(400).json(.{
                .@"error" = "INVALID_REQUEST",
                .message = switch (err) {
                    error.InvalidMaxTokens => "max_tokens must be between 1 and 2147483647",
                    error.InvalidTopK => "top_k must be between 0 and 2147483647",
                },
            });
        };
        const sampling = parseGenerateSamplingOptions(
            body.temperature,
            body.top_p,
            body.min_p,
            body.repetition_penalty,
            body.frequency_penalty,
            body.presence_penalty,
        ) catch |err| return ctx.status(400).json(.{
            .@"error" = "INVALID_REQUEST",
            .message = generateSamplingErrorMessage(err),
        });

        // Admission precedes model resolution and media decoding so rejected
        // requests cannot consume model or download work first.
        const media_admission = requestMediaAdmission(self, generateRequestMediaShape(body));
        const admission_units = @max(estimateGenerateRequestAdmissionUnits(body, numeric.max_tokens), media_admission.units);
        if (try self.acquireSlotUnits(ctx, admission_units)) |resp| return resp;
        var reserved_units = admission_units;
        defer self.releaseSlotUnits(reserved_units);
        var media_budget = RequestMediaBudget.init(media_admission.byte_cap);

        const model_name: ?[]const u8 = if (body.model.len > 0) body.model else null;
        const model_path = self.resolveRequestModelPath(ctx.allocator, ctx.io, model_name, "generators") catch |err|
            return requestModelResolutionError(ctx, err);
        defer ctx.allocator.free(model_path);
        var draft_model_path_storage: ?[]const u8 = null;
        defer if (draft_model_path_storage) |path| ctx.allocator.free(path);

        var owned_messages = self.parseGenerateMessagesWithBudget(ctx.allocator, body, &media_budget) catch |err| {
            if (err == error.InvalidImageUrl) {
                return ctx.status(400).json(.{
                    .@"error" = "INVALID_REQUEST",
                    .message = "image_url must contain a URL string",
                });
            }
            return remoteContentErrorResponse(ctx, err);
        };
        defer owned_messages.deinit();

        if (owned_messages.decoded_images.len > 0) {
            var decoded_budget = ReadDecodedImageBudget.init(media_admission, effectiveRequestContentSecurity(self).max_image_dimension);
            for (owned_messages.decoded_images) |image| decoded_budget.addImage(image) catch |err|
                return readImageErrorResponse(ctx, err);
            const required_units = @max(reserved_units, decoded_budget.requiredUnits());
            if (try self.growSlotUnits(ctx, reserved_units, required_units)) |resp| return resp;
            reserved_units = required_units;
        }

        if (try rejectDisallowedModel(self, ctx, model_path)) |response| return response;

        // Tool prompting can mutate the message list; media ownership remains
        // with OwnedGenerateMessages while this list owns message text.
        var messages = std.ArrayListUnmanaged(generation.Message).fromOwnedSlice(owned_messages.messages);
        owned_messages.messages = &.{};
        defer {
            for (messages.items) |message| ctx.allocator.free(message.content);
            messages.deinit(ctx.allocator);
        }

        if (messages.items.len == 0) {
            return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = "'messages' must not be empty" });
        }

        const parsed_tool_choice = tool_parser_mod.parseToolChoice(body.tool_choice) catch {
            return ctx.status(400).json(.{
                .@"error" = "INVALID_REQUEST",
                .message = "invalid tool_choice",
            });
        };
        if (body.tool_choice != null and (body.tools == null or body.tools.?.len == 0)) {
            return ctx.status(400).json(.{
                .@"error" = "INVALID_REQUEST",
                .message = "tools are required when tool_choice is set",
            });
        }

        var tool_parser: ?tool_parser_mod.Parser = null;
        defer if (tool_parser) |*parser| parser.deinit();

        if (body.tools) |tools| {
            if (tools.len > 0 and tool_parser_mod.toolCallsEnabled(parsed_tool_choice)) {
                tool_parser = tool_parser_mod.loadParser(ctx.allocator, model_path) catch |err| switch (err) {
                    error.UnknownToolCallFormat => return ctx.status(400).json(.{
                        .@"error" = "INVALID_MODEL",
                        .message = "model has an unsupported tool_call_format",
                    }),
                    else => return ctx.status(500).json(.{ .@"error" = "MODEL_LOAD_FAILED", .message = @errorName(err) }),
                };
                if (tool_parser == null) {
                    return ctx.status(400).json(.{
                        .@"error" = "INVALID_MODEL",
                        .message = "model does not support tool calling",
                    });
                }

                var selected_tools = std.ArrayListUnmanaged(tool_parser_mod.ToolDefinition).empty;
                defer selected_tools.deinit(ctx.allocator);

                const forced_function = tool_parser_mod.forcedFunctionName(parsed_tool_choice);
                for (tools) |tool| {
                    if (!std.mem.eql(u8, tool.type, "function")) {
                        return ctx.status(400).json(.{
                            .@"error" = "INVALID_REQUEST",
                            .message = "only function tools are supported",
                        });
                    }
                    if (forced_function) |forced| {
                        if (!std.mem.eql(u8, tool.function.name, forced)) continue;
                    }
                    try selected_tools.append(ctx.allocator, .{
                        .type = tool.type,
                        .function = .{
                            .name = tool.function.name,
                            .description = tool.function.description orelse "",
                            .parameters = tool.function.parameters,
                            .strict = tool.function.strict orelse false,
                        },
                    });
                }

                if (forced_function != null and selected_tools.items.len == 0) {
                    return ctx.status(400).json(.{
                        .@"error" = "INVALID_REQUEST",
                        .message = "forced function was not found in tools",
                    });
                }

                const tools_prompt = try tool_parser.?.formatToolsPrompt(ctx.allocator, selected_tools.items);
                defer ctx.allocator.free(tools_prompt);

                const prompt = if (forced_function) |forced|
                    try std.fmt.allocPrint(ctx.allocator, "{s}\nYou MUST call the {s} function. Do not respond with text, only call the function.\n", .{
                        tools_prompt,
                        forced,
                    })
                else
                    try ctx.allocator.dupe(u8, tools_prompt);
                defer ctx.allocator.free(prompt);

                try prependSystemPrompt(ctx.allocator, &messages, prompt);
            }
        }

        const requested_draft_model_name = body.draft_model;
        if (requested_draft_model_name != null and requested_draft_model_name.?.len == 0) {
            return ctx.status(400).json(.{
                .@"error" = "INVALID_REQUEST",
                .message = "draft_model must not be empty",
            });
        }
        var speculation = parseGenerateSpeculationOptions(
            requested_draft_model_name != null,
            body.speculative_k,
            body.speculation_policy,
            body.speculation_calibration,
        ) catch |err| {
            return ctx.status(400).json(.{
                .@"error" = "INVALID_REQUEST",
                .message = generateSpeculationErrorMessage(err),
            });
        };
        const effective_draft_model_name = effectiveDraftModelName(requested_draft_model_name, speculation.policy);
        // Auto-discovered MTP drafter, resolved before validation, admission,
        // and the prompt estimate so they all see the same speculation state
        // as the pipeline. The sibling's manifest/config are validated here so
        // a stale or foreign directory degrades to non-speculative generation
        // instead of failing the request.
        var auto_draft_sibling: ?[]const u8 = null;
        defer if (auto_draft_sibling) |path| ctx.allocator.free(path);
        var auto_draft_extra_units: usize = 0;
        if (requested_draft_model_name == null and
            speculation.policy == .auto and
            gemma4MtpAutoDraftDiscoveryEnabled())
        {
            if (gemma4MtpAssistantSiblingPath(ctx.allocator, ctx.io, model_path)) |sibling| {
                if (validateDiscoveredDraftDir(ctx.allocator, sibling)) {
                    auto_draft_sibling = sibling;
                } else {
                    std.log.warn("ignoring invalid Gemma4 MTP assistant sibling {s}", .{sibling});
                    ctx.allocator.free(sibling);
                }
            }
        }
        if (auto_draft_sibling != null) {
            qualifyAutoDiscoveredDraftSpeculation(&speculation);
            // The discovered drafter loads a second model backend; reserve the
            // same speculation admission a client-requested draft would get.
            // A discovered drafter is an optimization, not a client
            // requirement: when capacity is unavailable the request degrades
            // to non-speculative generation instead of a capacity rejection.
            const speculative_units = generateAdmissionUnitsForSpeculation(admission_units, true, speculation.policy);
            if (speculative_units > reserved_units) {
                const extra_units = speculative_units - reserved_units;
                if (self.reserveAdmissionUnits(extra_units)) {
                    reserved_units += extra_units;
                    auto_draft_extra_units = extra_units;
                } else |_| {
                    std.log.warn("skipping discovered MTP drafter: insufficient admission capacity", .{});
                    ctx.allocator.free(auto_draft_sibling.?);
                    auto_draft_sibling = null;
                }
            }
        }
        if (auto_draft_sibling != null and
            effective_draft_model_name == null and
            self.config.kernel_jit.qualified_profile_path != null)
        {
            // Qualified profiles reject a client-requested draft, but automatic
            // discovery is best-effort and must not turn a plain request into a
            // 400 response.
            std.log.warn("skipping discovered MTP drafter: qualified-profile JIT is active", .{});
            const released = takeAutoDraftAdmissionExtra(&reserved_units, &auto_draft_extra_units);
            if (released > 0) self.releaseSlotUnits(released);
            ctx.allocator.free(auto_draft_sibling.?);
            auto_draft_sibling = null;
        }
        validateQualifiedProfileDraft(
            self.config.kernel_jit.qualified_profile_path != null,
            effective_draft_model_name orelse auto_draft_sibling,
        ) catch {
            return ctx.status(400).json(.{
                .@"error" = "UNSUPPORTED_FEATURE",
                .message = "qualified-profile server JIT does not yet support draft_model; use local generate with separate target and draft profiles",
            });
        };
        if (effective_draft_model_name) |draft_model_name| {
            if (body.model.len > 0 and std.mem.eql(u8, draft_model_name, body.model)) {
                return ctx.status(400).json(.{
                    .@"error" = "INVALID_REQUEST",
                    .message = "draft_model must identify a model different from model",
                });
            }
        }
        validateCacheCompactionRatio(body.cache_compaction_ratio) catch {
            return ctx.status(400).json(.{
                .@"error" = "INVALID_REQUEST",
                .message = "cache_compaction_ratio must be finite, greater than 0, and at most 1",
            });
        };
        if (body.cache_compaction_ratio != null) {
            return ctx.status(400).json(.{
                .@"error" = "UNSUPPORTED_FEATURE",
                .message = "cache_compaction_ratio is not supported by resident server generation",
            });
        }
        if (requested_draft_model_name != null) self.metrics.incSpeculationRequested();

        const want_stream = body.stream orelse false;
        const include_stream_usage = want_stream and (if (body.stream_options) |options|
            options.include_usage orelse false
        else
            false);
        // Caching requires an explicit non-empty key: keyless requests would all
        // share one per-model namespace, leaking prompt presence across callers.
        const prompt_cache_key: ?[]const u8 = if (body.prompt_cache_key) |key|
            (if (key.len > 0) key else null)
        else
            null;
        const configured_max_tokens = numeric.max_tokens;

        var config = generation.GenerationConfig{
            .max_tokens = configured_max_tokens,
            .ignore_eos = body.ignore_eos orelse false,
            .enable_thinking = if (body.chat_template_kwargs) |kwargs| kwargs.enable_thinking else null,
            .temperature = sampling.temperature,
            .top_p = sampling.top_p,
            .top_k = numeric.top_k,
            .min_p = sampling.min_p,
            .repetition_penalty = sampling.repetition_penalty,
            .frequency_penalty = sampling.frequency_penalty,
            .presence_penalty = sampling.presence_penalty,
            .speculative_k = speculation.k,
            .speculation_requested = generationSpeculationRequested(effective_draft_model_name, auto_draft_sibling),
            .speculation_policy = speculation.policy,
            .speculation_calibration = speculation.calibration,
            .prefill_chunk_size = 0,
            .cache_dtype = body.cache_dtype,
            .cache_compaction_ratio = body.cache_compaction_ratio,
            .prompt_cache_enabled = self.config.prompt_cache.enabled and (body.prompt_cache orelse true) and prompt_cache_key != null and !want_stream,
            .prompt_cache_key = prompt_cache_key,
        };
        const backend_selection = parseGenerateBackendSelection(body.backend, body.mode, body.compiled_target) catch |err| {
            return ctx.status(400).json(.{
                .@"error" = "INVALID_REQUEST",
                .message = switch (err) {
                    error.InvalidGenerateMode => "unsupported generation mode",
                    error.InvalidCompiledTarget => "unsupported compiled_target",
                    else => "unsupported backend",
                },
            });
        };
        if (auto_draft_sibling != null and backend_selection.native_choice == .onnx) {
            // ONNX cannot host the discovered native/Metal drafter. As with all
            // automatic-discovery incompatibilities, continue without it.
            std.log.warn("skipping discovered MTP drafter: ONNX backend selected", .{});
            const released = takeAutoDraftAdmissionExtra(&reserved_units, &auto_draft_extra_units);
            if (released > 0) self.releaseSlotUnits(released);
            ctx.allocator.free(auto_draft_sibling.?);
            auto_draft_sibling = null;
            config.speculation_requested = false;
        }
        validatePromptCacheExecutionMode(config.prompt_cache_enabled, backend_selection) catch {
            return ctx.status(400).json(.{
                .@"error" = "UNSUPPORTED_FEATURE",
                .message = "prompt caching requires eager generation; omit mode or set mode=eager",
            });
        };
        validateGenerateDraftBackend(backend_selection, effective_draft_model_name) catch {
            return ctx.status(400).json(.{
                .@"error" = "INVALID_REQUEST",
                .message = "draft_model is not supported with backend=onnx; use native, metal, or cuda",
            });
        };
        if (try rejectExplicitBackendIncompatibility(
            ctx,
            model_path,
            backend_selection.native_choice,
            self.config.allow_unknown_models,
        )) |response|
            return response;
        const required_policy_allows_onnx = self.session_manager.allowsDirectBackend(.onnx) catch |err|
            return modelLoadFailureResponse(ctx, err);
        const allow_onnx = required_policy_allows_onnx and
            effective_draft_model_name == null and
            !backend_selection.graph_mode_requested and
            (body.backend == null or backend_selection.native_choice == .onnx);

        if (body.response_format) |rf| {
            if (std.mem.eql(u8, rf.type, "json_object")) {
                config.grammar = "json";
            } else if (std.mem.eql(u8, rf.type, "json_schema")) {
                const schema_cfg = rf.json_schema orelse {
                    return ctx.status(400).json(.{
                        .@"error" = "INVALID_REQUEST",
                        .message = "response_format.json_schema is required for type=json_schema",
                    });
                };
                const schema = schema_cfg.schema orelse {
                    return ctx.status(400).json(.{
                        .@"error" = "INVALID_REQUEST",
                        .message = "response_format.json_schema.schema is required for type=json_schema",
                    });
                };
                config.grammar = grammar_mod.buildJsonSchemaGrammar(ctx.allocator, .{ .object = schema.map }) catch |err| {
                    return ctx.status(400).json(.{
                        .@"error" = "INVALID_REQUEST",
                        .message = @errorName(err),
                    });
                };
            } else if (!std.mem.eql(u8, rf.type, "text")) {
                return ctx.status(400).json(.{
                    .@"error" = "INVALID_REQUEST",
                    .message = "unsupported response_format.type",
                });
            }
        }

        if (body.grammar) |grammar| {
            if (grammar.len == 0) {
                return ctx.status(400).json(.{
                    .@"error" = "INVALID_REQUEST",
                    .message = "grammar must not be empty",
                });
            }
            if (!std.mem.eql(u8, grammar, "json")) {
                var compiled = grammar_mod.GbnfGrammar.parse(ctx.allocator, grammar) catch |err| {
                    return ctx.status(400).json(.{
                        .@"error" = "INVALID_REQUEST",
                        .message = @errorName(err),
                    });
                };
                compiled.deinit();
            }
            config.grammar = grammar;
        }

        // Try plain HF ONNX decoder-only VLM packages before Ort GenAI overlays.
        if (allow_onnx and build_options.enable_onnx and
            !c_file.fileExistsInDir(ctx.allocator, model_path, "genai_config.json") and
            onnx_decoder_only_vlm.isSupportedModelDir(ctx.allocator, model_path))
        {
            var prompt_override: ?[]u8 = null;
            defer if (prompt_override) |prompt| ctx.allocator.free(prompt);
            if (tool_parser) |*parser| {
                if (std.mem.eql(u8, parser.name(), "functiongemma")) {
                    prompt_override = try buildFunctionGemmaPrompt(
                        ctx.allocator,
                        "",
                        messages.items,
                    );
                }
            }

            var pipeline = onnx_decoder_only_vlm.Pipeline.load(ctx.allocator, model_path) catch |err|
                return modelLoadFailureResponse(ctx, err);
            defer pipeline.deinit();
            pipeline.prompt_override = if (prompt_override) |prompt| prompt else null;

            if (requiresNativeChannelProjection(pipeline.gpt_config)) {
                return ctx.status(400).json(.{
                    .@"error" = "UNSUPPORTED_FEATURE",
                    .message = "channel-aware Gemma generation requires the native, Metal, or CUDA backend",
                });
            }

            if (config.grammar != null) {
                return ctx.status(400).json(.{
                    .@"error" = "INVALID_REQUEST",
                    .message = "grammar-constrained decoding is native-backend only; ONNX generation remains unconstrained-only",
                });
            }

            if (want_stream) {
                return self.streamGenerate(
                    ctx,
                    body.model,
                    &pipeline,
                    null,
                    messages.items,
                    config,
                    if (tool_parser) |*parser| parser else null,
                    include_stream_usage,
                );
            }

            var result = generateMaybeStopOnTool(&pipeline, messages.items, config, if (tool_parser) |*parser| parser else null) catch |err|
                return generationErrorResponse(ctx, err);
            defer result.deinit();

            var response_text = result.text;
            var tool_response_text: ?[]u8 = null;
            defer if (tool_response_text) |text| ctx.allocator.free(text);
            const parsed_tool_calls = if (tool_parser) |*parser| blk: {
                parser.reset();
                _ = parser.feed(result.text) catch |err|
                    return ctx.status(500).json(.{ .@"error" = "GENERATION_FAILED", .message = @errorName(err) });
                tool_response_text = parser.finishText(ctx.allocator) catch |err|
                    return ctx.status(500).json(.{ .@"error" = "GENERATION_FAILED", .message = @errorName(err) });
                response_text = tool_response_text.?;
                if (response_text.len == 0) response_text = result.text;
                const calls = parser.toolCalls();
                break :blk if (calls.len > 0) calls else null;
            } else null;

            if (parsed_tool_calls == null and tool_parser != null and response_text.len == 0) {
                response_text = "No tool call was emitted.";
            }

            var formatted_response_text: ?[]u8 = null;
            defer if (formatted_response_text) |text| ctx.allocator.free(text);
            if (parsed_tool_calls == null) {
                formatted_response_text = coerceGenerateResponseFormat(ctx.allocator, body.response_format, response_text) catch |err|
                    return ctx.status(500).json(.{
                        .@"error" = "STRUCTURED_OUTPUT_INVALID",
                        .message = @errorName(err),
                    });
                if (formatted_response_text) |text| response_text = text;
            }

            return self.buildGenerateResponse(
                ctx,
                body.model,
                response_text,
                result.reasoning_text,
                if (parsed_tool_calls != null) "tool_calls" else result.finish_reason,
                result.prompt_tokens,
                result.tokens_used,
                0,
                parsed_tool_calls,
            );
        }

        // Try ortgenai first (models with genai_config.json)
        if (allow_onnx and build_options.enable_onnx) {
            const ortgenai = backends_mod.ortgenai;
            const ort_model_dir = ortgenai.prepareGenerativeModelPackage(ctx.allocator, model_path) catch null;
            defer if (ort_model_dir) |prepared| ctx.allocator.free(prepared);
            if (ort_model_dir) |prepared_model_dir| {
                if (config.ignore_eos) {
                    return ctx.status(400).json(.{
                        .@"error" = "UNSUPPORTED_FEATURE",
                        .message = "ignore_eos is not supported by the ONNX Runtime GenAI backend",
                    });
                }
                var ort_manifest = manifest_mod.loadFromDir(ctx.allocator, prepared_model_dir) catch |err|
                    return modelLoadFailureResponse(ctx, err);
                defer ort_manifest.deinit();
                const ort_gpt_config = session_factory.loadGptConfigFromModelDir(
                    ctx.allocator,
                    prepared_model_dir,
                    ort_manifest,
                ) catch null;
                if (ort_gpt_config) |gpt_config| {
                    if (requiresNativeChannelProjection(gpt_config)) {
                        return ctx.status(400).json(.{
                            .@"error" = "UNSUPPORTED_FEATURE",
                            .message = "channel-aware Gemma generation requires the native, Metal, or CUDA backend",
                        });
                    }
                }

                const use_functiongemma_prompt_override = if (tool_parser) |*parser|
                    std.mem.eql(u8, parser.name(), "functiongemma")
                else
                    false;

                var ort_chat_template_storage: ?generation.ChatTemplate = null;
                defer if (ort_chat_template_storage) |*ct| ct.deinit();
                if (!use_functiongemma_prompt_override) {
                    if (ort_manifest.chat_template) |ct_source| {
                        ort_chat_template_storage = generation.ChatTemplate.init(
                            ctx.allocator,
                            ct_source,
                            ort_manifest.bos_token,
                            ort_manifest.eos_token,
                            ort_manifest.unk_token,
                            ort_manifest.pad_token,
                        ) catch |err| blk: {
                            std.log.warn("chat template init failed for {s}: {s}", .{ model_path, @errorName(err) });
                            break :blk null;
                        };
                    }
                }

                var prompt_override: ?[]u8 = null;
                defer if (prompt_override) |prompt| ctx.allocator.free(prompt);
                if (use_functiongemma_prompt_override) {
                    prompt_override = try buildFunctionGemmaPrompt(
                        ctx.allocator,
                        ort_manifest.bos_token,
                        messages.items,
                    );
                }

                var gen_model = ortgenai.GenAiModel.load(ctx.allocator, prepared_model_dir) catch |err|
                    return modelLoadFailureResponse(ctx, err);
                defer gen_model.deinit();

                var pipeline = generation.GenerationPipeline{
                    .allocator = ctx.allocator,
                    .model = &gen_model,
                    .chat_template = if (ort_chat_template_storage) |*ct| ct else null,
                    .prompt_override = if (prompt_override) |prompt| prompt else null,
                };

                if (config.grammar != null) {
                    return ctx.status(400).json(.{
                        .@"error" = "INVALID_REQUEST",
                        .message = "grammar-constrained decoding is native-backend only; ONNX generation remains unconstrained-only",
                    });
                }

                if (want_stream) {
                    return self.streamGenerate(
                        ctx,
                        body.model,
                        &pipeline,
                        null,
                        messages.items,
                        config,
                        if (tool_parser) |*parser| parser else null,
                        include_stream_usage,
                    );
                }

                var result = generateMaybeStopOnTool(&pipeline, messages.items, config, if (tool_parser) |*parser| parser else null) catch |err|
                    return generationErrorResponse(ctx, err);
                defer result.deinit();

                var response_text = result.text;
                var tool_response_text: ?[]u8 = null;
                defer if (tool_response_text) |text| ctx.allocator.free(text);
                const parsed_tool_calls = if (tool_parser) |*parser| blk: {
                    parser.reset();
                    _ = parser.feed(result.text) catch |err|
                        return ctx.status(500).json(.{ .@"error" = "GENERATION_FAILED", .message = @errorName(err) });
                    tool_response_text = parser.finishText(ctx.allocator) catch |err|
                        return ctx.status(500).json(.{ .@"error" = "GENERATION_FAILED", .message = @errorName(err) });
                    response_text = tool_response_text.?;
                    if (response_text.len == 0) response_text = result.text;
                    const calls = parser.toolCalls();
                    break :blk if (calls.len > 0) calls else null;
                } else null;

                if (parsed_tool_calls == null and tool_parser != null and response_text.len == 0) {
                    response_text = "No tool call was emitted.";
                }

                var formatted_response_text: ?[]u8 = null;
                defer if (formatted_response_text) |text| ctx.allocator.free(text);
                if (parsed_tool_calls == null) {
                    formatted_response_text = coerceGenerateResponseFormat(ctx.allocator, body.response_format, response_text) catch |err|
                        return ctx.status(500).json(.{
                            .@"error" = "STRUCTURED_OUTPUT_INVALID",
                            .message = @errorName(err),
                        });
                    if (formatted_response_text) |text| response_text = text;
                }

                return self.buildGenerateResponse(
                    ctx,
                    body.model,
                    response_text,
                    result.reasoning_text,
                    if (parsed_tool_calls != null) "tool_calls" else result.finish_reason,
                    result.prompt_tokens,
                    result.tokens_used,
                    0,
                    parsed_tool_calls,
                );
            }
        }

        // Fall back to native generation (CPU/GPU GPT arch forward pass).
        var model_handle = if (backend_selection.native_choice != .auto) blk: {
            var request_session_manager = backends_mod.SessionManager.init(ctx.allocator);
            configureGenerateBackendPreference(&request_session_manager, backend_selection);
            break :blk self.model_manager.acquireFromDirWithPreferredBackends(model_path, request_session_manager.preferred_backends, false) catch |err|
                return modelLoadFailureResponse(ctx, err);
        } else self.model_manager.acquireFromDir(model_path) catch |err|
            return modelLoadFailureResponse(ctx, err);
        defer model_handle.release();
        const model = model_handle.get();

        const gpt_config = session_factory.getGptConfig(model.session) orelse {
            // The session is not a decoder at all, which in practice means the
            // architecture was never recognized and the model fell through to the
            // default encoder path. Name it, so the caller can tell "unsupported model"
            // apart from "Antfly is broken".
            var inspection: model_compatibility.Inspection = model_compatibility.inspectAlloc(ctx.allocator, &model.manifest) catch .{
                .architecture = try ctx.allocator.dupe(u8, "unknown"),
            };
            defer inspection.deinit(ctx.allocator);
            return ctx.status(400).json(.{
                .@"error" = "INCOMPATIBLE_MODEL",
                .message = try std.fmt.allocPrint(
                    ctx.allocator,
                    "model architecture \"{s}\" does not provide a generation runtime",
                    .{inspection.architecture},
                ),
            });
        };
        const backend_kind: runtime.kv.pool.BackendKind = switch (model.session.backend()) {
            .native => .native,
            .metal => .metal,
            .cuda => .cuda,
            .pjrt => return ctx.status(500).json(.{ .@"error" = "BACKEND_ERROR", .message = "unexpected PJRT backend in native generation path" }),
            .onnx => return ctx.status(500).json(.{ .@"error" = "BACKEND_ERROR", .message = "unexpected ONNX backend in native generation path" }),
            .wasm => return ctx.status(500).json(.{ .@"error" = "BACKEND_ERROR", .message = "unexpected WASM backend in server generation path" }),
        };
        const auto_metal_whole_model = shouldAutoUseMetalWholeModelGenerate(
            model.session.backend(),
            graph_mod.metal_executor.supportsSession(model.session),
            generation.NativeDecodeState.requiresDeepSeekV4CompressedCache(gpt_config),
            config.prompt_cache_enabled,
            config.speculation_requested,
            backend_selection,
        );
        const effective_compiled_partition_backend: ?ops.BackendKind = if (auto_metal_whole_model)
            .metal
        else
            backend_selection.compiled_partition_backend;
        native_backend_choice.validateRequiredCompiledBackend(
            &self.session_manager,
            effective_compiled_partition_backend,
        ) catch |err| return modelLoadFailureResponse(ctx, err);
        const effective_compiled_attachment_target: graph_mod.compiled_backend.AttachmentTarget = if (auto_metal_whole_model)
            .whole_model
        else
            backend_selection.compiled_attachment_target;
        const kv_dtype = if (config.cache_dtype) |name|
            runtime.kv.pool.parseKvDType(name) orelse
                return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = "invalid cache_dtype value" })
        else
            session_factory.recommendedKvDTypeForSession(model.session, backend_kind);
        const target_kv_capacity_policy = generation.generationKvCapacityPolicyForRoute(
            if (build_options.enable_metal and
                model.session.backend() == .metal and
                effective_compiled_partition_backend == .metal and
                effective_compiled_attachment_target == .whole_model and
                graph_mod.metal_executor.supportsSession(model.session) and
                !generation.NativeDecodeState.requiresDeepSeekV4CompressedCache(gpt_config))
                .metal_whole_model
            else
                .standard,
            gpt_config,
            config,
            kv_dtype,
        );
        const prompt_bytes = self.estimateGeneratePromptBytes(messages.items);
        // Kept alive for the whole request so the pipeline can reuse the
        // estimate's rendered prompt and token ids instead of redoing both.
        var prompt_estimate: ?NativePromptEstimate = null;
        defer if (prompt_estimate) |*estimate| estimate.deinit();
        const prompt_tokens = self.estimateNativePromptTokens(
            ctx.allocator,
            model_path,
            model,
            gpt_config,
            messages.items,
            configured_max_tokens,
            if (config.speculation_requested and config.speculation_policy != .off and config.speculative_k > 0) 1 else 0,
            config.enable_thinking,
            &prompt_estimate,
        ) catch |err| {
            if (err == error.PromptTooLong) {
                return ctx.status(400).json(.{
                    .@"error" = "INVALID_REQUEST",
                    .message = "prompt exceeds the model context window after reserving output tokens",
                });
            }
            return ctx.status(500).json(.{ .@"error" = "TOKENIZE_FAILED", .message = internalErrorMessage("TOKENIZE_FAILED", err) });
        };
        const idle_prefill_ceiling = generation.nativeGenerationPrefillChunkCeiling(
            backend_kind,
            gpt_config,
            self.config.generation_batching.schedulerPolicy().max_idle_prefill_chunk_size,
        );
        var native_generate_lease: ?runtime.scheduler.native_generate.Lease = null;
        defer if (native_generate_lease) |lease| {
            if (model.native_generate_coordinator) |coordinator| coordinator.release(lease);
        };
        if (model.native_generate_coordinator) |coordinator| {
            native_generate_lease = self.acquireNativeGenerateLease(coordinator, .{
                .requested_units = reserved_units,
                .prompt_bytes = prompt_bytes,
                .prompt_tokens = prompt_tokens,
                .prefill_chunk_limit = if (config.prefill_chunk_size == 0) idle_prefill_ceiling else 0,
                .max_tokens = configured_max_tokens,
            }) catch |err| blk: {
                // A discovered drafter is an optimization: give back its extra
                // admission and retry single-model before surfacing capacity
                // errors the client's own request never earned.
                if (auto_draft_sibling != null) {
                    std.log.warn("skipping discovered MTP drafter: insufficient scheduler capacity", .{});
                    const released = takeAutoDraftAdmissionExtra(&reserved_units, &auto_draft_extra_units);
                    if (released > 0) self.releaseSlotUnits(released);
                    ctx.allocator.free(auto_draft_sibling.?);
                    auto_draft_sibling = null;
                    config.speculation_requested = false;
                    break :blk try self.acquireNativeGenerateLease(coordinator, .{
                        .requested_units = reserved_units,
                        .prompt_bytes = prompt_bytes,
                        .prompt_tokens = prompt_tokens,
                        .prefill_chunk_limit = if (config.prefill_chunk_size == 0) idle_prefill_ceiling else 0,
                        .max_tokens = configured_max_tokens,
                    });
                }
                return err;
            };
        }

        const tok = model.getTokenizer();
        var draft_cb: ?ops.ComputeBackend = null;
        defer if (draft_cb) |*cb_value| cb_value.deinit();
        var draft_gpt_config: ?@import("../models/gpt.zig").Config = null;
        var draft_model_handle: ?model_manager_mod.ModelHandle = null;
        defer if (draft_model_handle) |*handle| handle.release();
        var draft_model_for_generation: ?*model_manager_mod.LoadedModel = null;
        var draft_backend_kind: ?runtime.kv.pool.BackendKind = null;
        var draft_kv_dtype: ?runtime.kv.pool.KvDType = null;
        var draft_resource_estimate: ?runtime.tier.memory.Estimate = null;
        var pjrt_client: ?pjrt_lib.pjrt.Client = null;
        defer if (pjrt_client) |*client| client.deinit();
        var pjrt_plugin_path: ?[:0]u8 = null;
        defer if (pjrt_plugin_path) |path| ctx.allocator.free(path);
        if (backend_selection.compiled_partition_backend == .pjrt) {
            pjrt_plugin_path = try native_backend_choice.pjrtPluginPathFromEnv(ctx.allocator);
            const plugin_path = pjrt_plugin_path orelse
                return ctx.status(400).json(.{
                    .@"error" = "INVALID_REQUEST",
                    .message = "xla backend requires ANTFLY_INFERENCE_XLA_PLUGIN or ANTFLY_INFERENCE_PJRT_PLUGIN",
                });
            pjrt_client = pjrt_lib.pjrt.Client.init(plugin_path) catch |err|
                return ctx.status(500).json(.{ .@"error" = "BACKEND_ERROR", .message = @errorName(err) });
        }

        var resolved_draft_model_path: ?[]const u8 = null;
        var draft_was_discovered = false;
        if (effective_draft_model_name) |draft_model_name| {
            if (shouldResolveDraftModel(config.speculation_policy)) {
                const draft_model_path = self.resolveRequestModelPath(ctx.allocator, ctx.io, draft_model_name, "generators") catch |err|
                    return requestModelResolutionError(ctx, err);
                draft_model_path_storage = draft_model_path;
                if (std.mem.eql(u8, draft_model_path, model_path)) {
                    return ctx.status(400).json(.{
                        .@"error" = "INVALID_REQUEST",
                        .message = "draft_model must resolve to a model different from model",
                    });
                }
                resolved_draft_model_path = draft_model_path;
            }
        } else if (auto_draft_sibling) |sibling| {
            // Ownership moves to the request-scoped draft path storage;
            // config.speculation_requested was already set at config build so
            // admission, the estimate, and scheduler batching stayed
            // consistent with the pipeline.
            draft_model_path_storage = sibling;
            resolved_draft_model_path = sibling;
            auto_draft_sibling = null;
            draft_was_discovered = true;
        }
        var draft_degraded = false;
        if (resolved_draft_model_path) |draft_model_path| draft_setup: {
            config.draft_model = draft_model_path;
            var load_draft_backend = true;
            if (config.speculation_policy == .auto) {
                var draft_manifest = manifest_mod.loadFromDir(ctx.allocator, draft_model_path) catch |err| {
                    if (draft_was_discovered) {
                        draft_degraded = true;
                        break :draft_setup;
                    }
                    return modelLoadFailureResponse(ctx, err);
                };
                defer draft_manifest.deinit();
                const draft_cfg = session_factory.loadGptConfigFromModelDir(ctx.allocator, draft_model_path, draft_manifest) catch |err| {
                    if (draft_was_discovered) {
                        draft_degraded = true;
                        break :draft_setup;
                    }
                    return ctx.status(400).json(.{ .@"error" = "INVALID_MODEL", .message = @errorName(err) });
                };
                if (shouldSkipAutoMtpDraftLoad(config, draft_cfg)) {
                    draft_gpt_config = draft_cfg;
                    load_draft_backend = false;
                }
            }
            if (load_draft_backend) {
                draft_model_handle = if (backend_selection.native_choice != .auto) blk: {
                    var request_session_manager = backends_mod.SessionManager.init(ctx.allocator);
                    configureGenerateBackendPreference(&request_session_manager, backend_selection);
                    break :blk self.model_manager.acquireFromDirWithPreferredBackends(draft_model_path, request_session_manager.preferred_backends, false) catch |err| {
                        if (draft_was_discovered) {
                            draft_degraded = true;
                            break :draft_setup;
                        }
                        return modelLoadFailureResponse(ctx, err);
                    };
                } else self.model_manager.acquireFromDir(draft_model_path) catch |err| {
                    if (draft_was_discovered) {
                        draft_degraded = true;
                        break :draft_setup;
                    }
                    return modelLoadFailureResponse(ctx, err);
                };
                const draft_model = draft_model_handle.?.get();
                const draft_cfg = session_factory.getGptConfig(draft_model.session) orelse {
                    if (draft_was_discovered) {
                        draft_degraded = true;
                        break :draft_setup;
                    }
                    return ctx.status(400).json(.{
                        .@"error" = "INVALID_MODEL",
                        .message = "draft_model does not support generation",
                    });
                };
                const draft_tok = draft_model.getTokenizer();
                const target_special = tok.specialTokens();
                const draft_special = draft_tok.specialTokens();
                if (draft_tok.vocabSize() != tok.vocabSize() or
                    draft_cfg.vocab_size != gpt_config.vocab_size or
                    draft_special.cls_id != target_special.cls_id or
                    draft_special.sep_id != target_special.sep_id or
                    draft_special.pad_id != target_special.pad_id or
                    draft_special.unk_id != target_special.unk_id)
                {
                    if (draft_was_discovered) {
                        draft_degraded = true;
                        break :draft_setup;
                    }
                    return ctx.status(400).json(.{
                        .@"error" = "INVALID_REQUEST",
                        .message = "draft_model tokenizer is incompatible with target model",
                    });
                }

                const actual_draft_backend: runtime.kv.pool.BackendKind = switch (draft_model.session.backend()) {
                    .native => .native,
                    .metal => .metal,
                    .cuda => .cuda,
                    .pjrt, .onnx, .wasm => {
                        if (draft_was_discovered) {
                            draft_degraded = true;
                            break :draft_setup;
                        }
                        return ctx.status(400).json(.{
                            .@"error" = "INVALID_MODEL",
                            .message = "draft_model does not use a supported generation backend",
                        });
                    },
                };
                const actual_draft_kv_dtype = session_factory.recommendedKvDTypeForSession(
                    draft_model.session,
                    actual_draft_backend,
                );
                draft_model_for_generation = draft_model;
                draft_backend_kind = actual_draft_backend;
                draft_kv_dtype = actual_draft_kv_dtype;
                draft_gpt_config = draft_cfg;
            }
        }
        if (draft_degraded) {
            // A discovered drafter is best-effort: unwind it and continue as a
            // plain single-model request instead of failing the generate call.
            std.log.warn("skipping discovered MTP drafter: draft setup failed", .{});
            config.draft_model = null;
            config.speculation_requested = false;
            if (draft_model_handle) |*handle| handle.release();
            draft_model_handle = null;
            draft_model_for_generation = null;
            draft_backend_kind = null;
            draft_kv_dtype = null;
            draft_gpt_config = null;
        }
        if (draft_was_discovered and draft_model_for_generation == null and auto_draft_extra_units > 0) {
            // Setup failures and deliberate AUTO-policy disablement both mean
            // there is no second backend. Give back its exact slot and
            // scheduler reservations while preserving any media-driven growth.
            if (native_generate_lease) |lease| {
                if (model.native_generate_coordinator) |coordinator| coordinator.release(lease);
                native_generate_lease = null;
            }
            const released = takeAutoDraftAdmissionExtra(&reserved_units, &auto_draft_extra_units);
            if (released > 0) self.releaseSlotUnits(released);
            if (model.native_generate_coordinator) |coordinator| {
                native_generate_lease = try self.acquireNativeGenerateLease(coordinator, .{
                    .requested_units = reserved_units,
                    .prompt_bytes = prompt_bytes,
                    .prompt_tokens = prompt_tokens,
                    .prefill_chunk_limit = if (config.prefill_chunk_size == 0) idle_prefill_ceiling else 0,
                    .max_tokens = configured_max_tokens,
                });
            }
        }

        const budget_backend_class: runtime.tier.memory.BackendClass =
            if (backend_kind != .native or (draft_backend_kind != null and draft_backend_kind.? != .native))
                .gpu
            else
                .cpu;
        var budget_limits = self.defaultGenerationLimits(budget_backend_class);
        budget_limits = session_factory.widenBudgetLimitsForSession(model.session, budget_limits);
        if (draft_model_for_generation) |draft_model| {
            const draft_budget_class: runtime.tier.memory.BackendClass =
                if (draft_backend_kind.? == .native) .cpu else .gpu;
            const draft_budget_limits = self.config.generation_budget_overrides.apply(
                session_factory.widenBudgetLimitsForSession(
                    draft_model.session,
                    self.defaultGenerationLimits(draft_budget_class),
                ),
            );
            budget_limits = runtime.tier.memory.maxCompositeLimits(budget_limits, draft_budget_limits);
        }
        budget_limits = self.config.generation_budget_overrides.apply(budget_limits);
        var run_budget = runtime.tier.memory.RunBudget.init(budget_limits);

        if (draft_model_for_generation != null and config.speculation_policy != .off) {
            const shared_prompt_limit = generation.nativeGenerationPromptTokenLimit(
                gpt_config,
                draft_gpt_config,
                @intCast(@max(config.max_tokens, 1)),
                if (config.speculative_k > 0) 1 else 0,
                0,
            ) catch return ctx.status(400).json(.{
                .@"error" = "INVALID_REQUEST",
                .message = "requested output leaves no prompt capacity in the target or draft model context window",
            });
            // Qwen visual spans are dynamic. `prompt_tokens` deliberately uses
            // the conservative maximum for admission, so only the pipeline's
            // post-preprocessing exact count may enforce its context limit.
            if (gpt_config.family != .qwen3_5 and prompt_tokens > shared_prompt_limit) {
                return ctx.status(400).json(.{
                    .@"error" = "INVALID_REQUEST",
                    .message = "prompt exceeds the target or draft model context window",
                });
            }
        }

        var budget_components: [2]runtime.tier.memory.GptGenerationBudgetComponent = undefined;
        budget_components[0] = .{
            .backend = backend_kind,
            .kv_dtype = kv_dtype,
            .config = gpt_config,
            .kv_capacity_policy = target_kv_capacity_policy,
        };
        var budget_component_count: usize = 1;
        if (draft_model_for_generation != null) {
            budget_components[1] = .{
                .backend = draft_backend_kind.?,
                .kv_dtype = draft_kv_dtype.?,
                .config = draft_gpt_config.?,
            };
            budget_component_count = 2;
        }
        const scheduler_prefill_ceiling = if (native_generate_lease) |lease|
            lease.prefill_chunk_size
        else
            @min(
                prompt_tokens,
                if (config.prefill_chunk_size == 0)
                    idle_prefill_ceiling
                else
                    self.config.generation_batching.schedulerPolicy().max_idle_prefill_chunk_size,
            );
        const admission_prefill_ceiling = if (config.prefill_chunk_size > 0)
            @min(config.prefill_chunk_size, scheduler_prefill_ceiling)
        else
            scheduler_prefill_ceiling;
        const speculative_budget_bonus: usize = if (draft_model_for_generation != null and
            config.speculation_policy != .off and
            config.speculative_k > 0) 1 else 0;
        const budget_max_tokens = @as(usize, @intCast(@max(config.max_tokens, 1))) + speculative_budget_bonus;
        const prefill_admission = runtime.tier.memory.reserveGptGenerationPrefill(
            &run_budget,
            budget_components[0..budget_component_count],
            prompt_tokens,
            budget_max_tokens,
            admission_prefill_ceiling,
        ) catch |err| {
            if (err == error.MemoryBudgetExceeded) {
                return generationMemoryBudgetResponse(ctx, model.session, &run_budget);
            }
            return ctx.status(500).json(.{ .@"error" = "BACKEND_ERROR", .message = internalErrorMessage("BACKEND_ERROR", err) });
        };
        config.prefill_chunk_size = prefill_admission.max_chunk_rows;
        var standalone_prefill_plan: ?runtime.scheduler.native_generate.PrefillChunkPlan = null;
        defer if (standalone_prefill_plan) |*plan| plan.deinit(ctx.allocator);
        const prefill_plan_applicable = !generation.messagesHaveImages(messages.items) and
            !generation.messagesHaveAudio(messages.items) and
            !(effective_compiled_partition_backend != null and
                effective_compiled_attachment_target == .whole_model);
        if (prefill_plan_applicable) {
            if (model.native_generate_coordinator) |coordinator| {
                if (native_generate_lease) |*lease| {
                    coordinator.admitPrefillChunkPlan(
                        lease,
                        prompt_tokens,
                        prefill_admission.max_chunk_rows,
                        prefill_admission.max_scratch_bytes,
                        .fixed_ceiling,
                    ) catch |err| return ctx.status(500).json(.{
                        .@"error" = "BACKEND_ERROR",
                        .message = internalErrorMessage("PREFILL_PLAN_ADMISSION_FAILED", err),
                    });
                    config.prefill_chunk_plan = lease.prefill_chunk_plan;
                }
            }
            if (config.prefill_chunk_plan == null) {
                standalone_prefill_plan = runtime.scheduler.native_generate.PrefillChunkPlan.init(
                    ctx.allocator,
                    prompt_tokens,
                    prefill_admission.max_chunk_rows,
                    prefill_admission.max_scratch_bytes,
                    .sole_idle,
                    .fixed_ceiling,
                ) catch |err| return ctx.status(500).json(.{
                    .@"error" = "BACKEND_ERROR",
                    .message = internalErrorMessage("PREFILL_PLAN_ADMISSION_FAILED", err),
                });
                config.prefill_chunk_plan = standalone_prefill_plan.?;
            }
            logAdmittedPrefillChunkPlan(config.prefill_chunk_plan.?);
        }
        const resource_estimate = try runtime.tier.memory.estimateGptGenerationForKvPolicy(
            backend_kind,
            kv_dtype,
            gpt_config,
            prompt_tokens,
            budget_max_tokens,
            config.prefill_chunk_size,
            target_kv_capacity_policy,
        );
        if (draft_model_for_generation != null) {
            draft_resource_estimate = try runtime.tier.memory.estimateGptGeneration(
                draft_backend_kind.?,
                draft_kv_dtype.?,
                draft_gpt_config.?,
                prompt_tokens,
                budget_max_tokens,
                config.prefill_chunk_size,
            );
        }
        const target_backend_class: runtime.tier.memory.BackendClass =
            if (backend_kind == .native) .cpu else .gpu;
        const target_admission_limits = self.config.generation_budget_overrides.apply(
            session_factory.widenBudgetLimitsForSession(
                model.session,
                self.defaultGenerationLimits(target_backend_class),
            ),
        );
        var admission_requests: [2]runtime.tier.memory.AdmissionRequest = undefined;
        admission_requests[0] = .{
            .backend_class = target_backend_class,
            .limits = target_admission_limits,
            .amounts = .fromEstimate(resource_estimate),
        };
        if (generation.messagesHaveImages(messages.items) or generation.messagesHaveAudio(messages.items)) {
            admission_requests[0].amounts = try admission_requests[0].amounts.merge(
                try model_manager_mod.projectorRunAdmissionAmounts(
                    model.manifest,
                    backend_kind,
                    prompt_estimate.?.media_admission,
                ),
            );
        }
        const admission_request_count: usize = if (draft_resource_estimate) |estimate| blk: {
            const draft_backend_class: runtime.tier.memory.BackendClass =
                if (draft_backend_kind.? == .native) .cpu else .gpu;
            admission_requests[1] = .{
                .backend_class = draft_backend_class,
                .limits = self.config.generation_budget_overrides.apply(
                    session_factory.widenBudgetLimitsForSession(
                        draft_model_for_generation.?.session,
                        self.defaultGenerationLimits(draft_backend_class),
                    ),
                ),
                .amounts = .fromEstimate(estimate),
            };
            break :blk 2;
        } else 1;
        var admission_lease = self.model_manager.acquireRunResourceEstimates(
            admission_requests[0..admission_request_count],
        ) catch |err| switch (err) {
            error.ResourceLimitExceeded => return ctx.status(400).json(.{
                .@"error" = "MODEL_RESOURCE_LIMIT",
                .message = "request exceeds the configured inference resource budget",
            }),
            error.ResourceTemporarilyUnavailable => return modelResourceBusyResponse(ctx),
            // Ownership configuration errors should have failed startup before
            // request surfaces were published. Keep the HTTP boundary total so
            // a violated invariant is reported as an internal failure instead
            // of widening this inferred error set into a compile failure.
            else => return inferenceFailureResponse(ctx, err),
        };
        defer admission_lease.release();

        const first_locked_model = if (draft_model_for_generation) |draft_model|
            if (@intFromPtr(draft_model) < @intFromPtr(model)) draft_model else model
        else
            model;
        const second_locked_model: ?*model_manager_mod.LoadedModel = if (draft_model_for_generation) |draft_model|
            // Lock only a *distinct* second model. If the draft and target
            // resolve to the same *LoadedModel (e.g. a future path-canonicalizing
            // cache that aliases two dir strings to one pointer), locking the
            // non-reentrant native-generation mutex twice would self-deadlock and
            // permanently wedge all generation on that model.
            if (draft_model == model)
                null
            else if (first_locked_model == model) draft_model else model
        else
            null;
        first_locked_model.lockNativeGeneration(ctx.io);
        if (second_locked_model) |second| second.lockNativeGeneration(ctx.io);
        defer {
            if (second_locked_model) |second| second.unlockNativeGeneration();
            first_locked_model.unlockNativeGeneration();
        }

        if (draft_model_for_generation) |draft_model| {
            draft_cb = session_factory.getComputeBackendWithBudget(draft_model.session, ctx.allocator, &run_budget) catch |err| {
                if (err == error.MemoryBudgetExceeded) {
                    return generationMemoryBudgetResponse(ctx, draft_model.session, &run_budget);
                }
                return ctx.status(500).json(.{ .@"error" = "BACKEND_ERROR", .message = @errorName(err) });
            };
        }

        var kv_manager = runtime.kv.manager.KvManager.init(ctx.allocator);
        defer kv_manager.deinit();
        var draft_kv_manager: ?runtime.kv.manager.KvManager = null;
        defer if (draft_kv_manager) |*manager| manager.deinit();

        var cb = session_factory.getComputeBackendWithBudget(model.session, ctx.allocator, &run_budget) catch |err| {
            if (err == error.MemoryBudgetExceeded) {
                return generationMemoryBudgetResponse(ctx, model.session, &run_budget);
            }
            return ctx.status(500).json(.{ .@"error" = "BACKEND_ERROR", .message = @errorName(err) });
        };
        defer cb.deinit();
        const pool_config = generation.kvPoolConfig(backend_kind, kv_dtype, gpt_config, generationKvSlidingTrimForced());

        var prompt_cache: ?*runtime.kv.prompt_cache.PromptPrefixCache = null;
        var active_kv_manager: *runtime.kv.manager.KvManager = &kv_manager;
        var active_kv_storage: ?*runtime.kv.storage_runtime.KvStorageRuntime = null;
        var pool_id: runtime.kv.block.KvPoolId = undefined;
        if (config.prompt_cache_enabled and promptCacheBackendEligible(self.config.prompt_cache.mode, backend_kind) and
            effective_compiled_partition_backend == null and
            effective_draft_model_name == null and
            config.cache_compaction_ratio == null)
        {
            const prompt_cache_config = self.config.prompt_cache.runtimeConfig(
                self.config.prompt_cache_resource_usage_observer,
            );
            self.model_manager.rebalancePromptCaches(
                model,
                prompt_cache_config,
            );
            const cache_ready = if (backend_kind == .metal or backend_kind == .cuda) blk: {
                const ensured = model.prompt_prefix_cache.ensureStorage(pool_config) catch |err| {
                    self.model_manager.cancelPromptCacheActivation(model, prompt_cache_config);
                    std.log.warn("prompt cache storage activation failed; using request-local KV: {s}", .{@errorName(err)});
                    break :blk false;
                };
                const storage = if (ensured) |result| result.storage else break :blk false;
                if (storage.device_write_hook == null) {
                    cb.provisionKvDeviceWriteHook(storage) catch |err| {
                        self.model_manager.cancelPromptCacheActivation(model, prompt_cache_config);
                        std.log.warn("prompt cache device activation failed; using request-local KV: {s}", .{@errorName(err)});
                        break :blk false;
                    };
                }
                if (storage.device_write_hook == null) break :blk false;
                active_kv_storage = storage;
                break :blk true;
            } else blk: {
                const maybe_cache_pool_id = model.prompt_prefix_cache.ensurePool(pool_config) catch |err| {
                    self.model_manager.cancelPromptCacheActivation(model, prompt_cache_config);
                    std.log.warn("prompt cache activation failed; using request-local KV: {s}", .{@errorName(err)});
                    break :blk false;
                };
                break :blk maybe_cache_pool_id != null;
            };

            if (cache_ready) {
                active_kv_manager = model.prompt_prefix_cache.managerPtr();
                pool_id = model.prompt_prefix_cache.pool_id.?;
                prompt_cache = &model.prompt_prefix_cache;
            } else {
                pool_id = kv_manager.addPool(pool_config) catch |err|
                    return ctx.status(500).json(.{ .@"error" = "BACKEND_ERROR", .message = @errorName(err) });
                config.prompt_cache_enabled = false;
                self.model_manager.cancelPromptCacheActivation(model, prompt_cache_config);
            }
        } else {
            config.prompt_cache_enabled = false;
            pool_id = kv_manager.addPool(pool_config) catch |err|
                return ctx.status(500).json(.{ .@"error" = "BACKEND_ERROR", .message = @errorName(err) });
        }

        var kv_storage: ?runtime.kv.storage_runtime.KvStorageRuntime = if (active_kv_storage == null)
            runtime.kv.storage_runtime.KvStorageRuntime.init(ctx.allocator, pool_config) catch |err|
                return ctx.status(500).json(.{ .@"error" = "BACKEND_ERROR", .message = @errorName(err) })
        else
            null;
        defer if (kv_storage) |*storage| storage.deinit();
        if (kv_storage) |*storage| {
            cb.provisionKvDeviceWriteHook(storage) catch |err|
                return ctx.status(500).json(.{ .@"error" = "BACKEND_ERROR", .message = @errorName(err) });
        }
        var decode_state = generation.NativeDecodeState.initPaged(ctx.allocator, active_kv_manager, pool_id, model.shared_moe_cache);
        if (active_kv_storage) |storage| {
            decode_state.kv_storage = storage;
        } else if (kv_storage) |*storage| {
            decode_state.kv_storage = storage;
        }
        defer decode_state.deinit();

        var draft_decode_state: ?generation.NativeDecodeState = null;
        defer if (draft_decode_state) |*state| state.deinit();

        if (draft_cb != null) {
            if (draft_gpt_config) |draft_cfg| {
                draft_kv_manager = runtime.kv.manager.KvManager.init(ctx.allocator);
                const draft_pool_config = generation.kvPoolConfig(
                    draft_backend_kind.?,
                    draft_kv_dtype.?,
                    draft_cfg,
                    generationKvSlidingTrimForced(),
                );
                const draft_pool_id = draft_kv_manager.?.addPool(draft_pool_config) catch |err|
                    return ctx.status(500).json(.{ .@"error" = "BACKEND_ERROR", .message = @errorName(err) });
                draft_decode_state = generation.NativeDecodeState.initPaged(ctx.allocator, &draft_kv_manager.?, draft_pool_id, null);
            }
        }

        const graph_mode = backend_selection.graph_mode_requested or
            effective_compiled_partition_backend != null or
            graphModeEnabled();
        const use_scheduler = !graph_mode;
        const use_model_graph_cache = graph_mode and
            build_options.enable_metal and
            model.session.backend() == .metal and
            effective_compiled_partition_backend == .metal and
            effective_compiled_attachment_target == .whole_model and
            graph_mod.metal_executor.supportsSession(model.session) and
            !generation.NativeDecodeState.requiresDeepSeekV4CompressedCache(gpt_config);
        var request_graph_cache: ?graph_mod.cache.GraphCache = if (graph_mode and !use_model_graph_cache)
            graph_mod.cache.GraphCache.init(ctx.allocator)
        else
            null;
        defer if (request_graph_cache) |*cache| cache.deinit();
        const graph_cache = if (!graph_mode)
            null
        else if (use_model_graph_cache)
            &model.native_generation_graph_cache
        else
            &request_graph_cache.?;
        const request_generate_timing = serverGenerateTimingEnabled();
        const debug_metal_timing = request_generate_timing and
            use_model_graph_cache and
            platform.env.getenvBool("TERMITE_DEBUG_METAL_TIMING");
        if (debug_metal_timing) graph_mod.metal_executor.resetTimingStats();

        var pipeline = generation.NativeGenerationPipeline{
            .allocator = ctx.allocator,
            .io = ctx.io,
            .cb = cb,
            .session = model.session,
            .gpt_config = gpt_config,
            .kv_dtype = kv_dtype,
            .shared_moe_cache = model.shared_moe_cache,
            .tokenizer = tok,
            .add_bos_token = model.manifest.add_bos_token,
            .bos_token = model.manifest.bos_token,
            .chat_template = model.chat_tmpl,
            // The admission estimate rendered and encoded with the same
            // template, messages, and enable_thinking (after the final
            // tool-prompt prepend), so the pipeline may reuse both. The
            // pipeline revalidates the encode's token limit itself.
            .preformatted_prompt = if (prompt_estimate) |estimate| estimate.prompt else null,
            .pre_encoded_prompt = if (prompt_estimate) |estimate| .{
                .ids = estimate.encoded.ids,
                .attention_mask = estimate.encoded.attention_mask,
                .prompt_token_limit = estimate.prompt_token_limit,
            } else null,
            .print_timing = request_generate_timing,
            .model_dir = model_path,
            .gguf_projector_path = model.manifest.gguf_projector_path,
            .decode_state = &decode_state,
            .scheduler = if (use_scheduler) model.native_generate_coordinator else null,
            .scheduler_lease = if (use_scheduler) if (native_generate_lease) |*lease| lease else null else null,
            .draft_cb = if (draft_cb) |cb_value| cb_value else null,
            .draft_gpt_config = draft_gpt_config,
            .draft_decode_state = if (draft_decode_state) |*state| state else null,
            .prompt_cache = prompt_cache,
            .graph_cache = graph_cache,
            .compiled_partition_backend = effective_compiled_partition_backend,
            .compiled_attachment_target = effective_compiled_attachment_target,
            .pjrt_client = if (pjrt_client) |*client| client else null,
        };

        if (want_stream) {
            return self.streamGenerate(
                ctx,
                body.model,
                &pipeline,
                &pipeline.cb,
                messages.items,
                config,
                if (tool_parser) |*parser| parser else null,
                include_stream_usage,
            );
        }

        const server_a4b_telemetry_before = serverA4bTelemetrySnapshot(&pipeline.cb);
        defer logServerA4bTelemetry(&pipeline.cb, server_a4b_telemetry_before, body.model);
        var result = generateMaybeStopOnTool(&pipeline, messages.items, config, if (tool_parser) |*parser| parser else null) catch |err| {
            if (err == error.MemoryBudgetExceeded)
                return generationMemoryBudgetResponse(ctx, model.session, &run_budget);
            return generationErrorResponse(ctx, err);
        };
        defer result.deinit();
        if (debug_metal_timing) {
            if (model.native_generation_graph_cache.getSessionCompiledModelRuntime(.metal, .whole_model)) |runtime_model| {
                runtime_model.printDebugTiming();
            }
        }

        var response_text = result.text;
        var tool_response_text: ?[]u8 = null;
        defer if (tool_response_text) |text| ctx.allocator.free(text);
        const parsed_tool_calls = if (tool_parser) |*parser| blk: {
            parser.reset();
            _ = parser.feed(result.text) catch |err|
                return ctx.status(500).json(.{ .@"error" = "GENERATION_FAILED", .message = @errorName(err) });
            tool_response_text = parser.finishText(ctx.allocator) catch |err|
                return ctx.status(500).json(.{ .@"error" = "GENERATION_FAILED", .message = @errorName(err) });
            response_text = tool_response_text.?;
            if (response_text.len == 0) response_text = result.text;
            const calls = parser.toolCalls();
            break :blk if (calls.len > 0) calls else null;
        } else null;

        if (parsed_tool_calls == null and tool_parser != null and response_text.len == 0) {
            response_text = "No tool call was emitted.";
        }

        var formatted_response_text: ?[]u8 = null;
        defer if (formatted_response_text) |text| ctx.allocator.free(text);
        if (parsed_tool_calls == null) {
            formatted_response_text = coerceGenerateResponseFormat(ctx.allocator, body.response_format, response_text) catch |err|
                return ctx.status(500).json(.{
                    .@"error" = "STRUCTURED_OUTPUT_INVALID",
                    .message = @errorName(err),
                });
            if (formatted_response_text) |text| response_text = text;
        }

        return self.buildGenerateResponse(
            ctx,
            body.model,
            response_text,
            result.reasoning_text,
            if (parsed_tool_calls != null) "tool_calls" else result.finish_reason,
            result.prompt_tokens,
            result.tokens_used,
            result.cached_prompt_tokens,
            parsed_tool_calls,
            effectiveSpeculationStats(
                result.speculative,
                config.speculation_requested,
                config.speculation_policy,
                config.speculation_calibration,
            ),
        );
    }

    const OwnedGenerateMessages = struct {
        allocator: std.mem.Allocator,
        messages: []generation.Message = &.{},
        decoded_images: [][]u8 = &.{},
        image_slices: [][]const []const u8 = &.{},
        content_parts: [][]const generation.Message.ContentPart = &.{},

        fn deinit(self: *OwnedGenerateMessages) void {
            for (self.messages) |msg| self.allocator.free(msg.content);
            self.allocator.free(self.messages);
            for (self.decoded_images) |img| self.allocator.free(img);
            self.allocator.free(self.decoded_images);
            for (self.image_slices) |slice| self.allocator.free(slice);
            self.allocator.free(self.image_slices);
            for (self.content_parts) |parts| self.allocator.free(parts);
            self.allocator.free(self.content_parts);
            self.* = .{ .allocator = self.allocator };
        }
    };

    fn parseGenerateMessages(self: *Node, allocator: std.mem.Allocator, body: api.GenerateRequest) !OwnedGenerateMessages {
        var media_budget = RequestMediaBudget.init(requestMediaMaxBytes(self));
        return self.parseGenerateMessagesWithBudget(allocator, body, &media_budget);
    }

    fn parseGenerateMessagesWithBudget(
        self: *Node,
        allocator: std.mem.Allocator,
        body: api.GenerateRequest,
        media_budget: *RequestMediaBudget,
    ) !OwnedGenerateMessages {
        var messages = std.ArrayListUnmanaged(generation.Message).empty;
        errdefer {
            for (messages.items) |msg| allocator.free(msg.content);
            messages.deinit(allocator);
        }
        var decoded_images = std.ArrayListUnmanaged([]u8).empty;
        errdefer {
            for (decoded_images.items) |img| allocator.free(img);
            decoded_images.deinit(allocator);
        }
        var image_slices = std.ArrayListUnmanaged([]const []const u8).empty;
        errdefer {
            for (image_slices.items) |slice| allocator.free(slice);
            image_slices.deinit(allocator);
        }
        var content_parts = std.ArrayListUnmanaged([]const generation.Message.ContentPart).empty;
        errdefer {
            for (content_parts.items) |parts| allocator.free(parts);
            content_parts.deinit(allocator);
        }

        for (body.messages) |msg| {
            const role: []const u8 = switch (msg.role) {
                .system => "system",
                .user => "user",
                .assistant => "assistant",
                .tool => "tool",
            };

            var text_buf = std.ArrayListUnmanaged(u8).empty;
            defer text_buf.deinit(allocator);
            var msg_images = std.ArrayListUnmanaged([]const u8).empty;
            defer msg_images.deinit(allocator);
            var msg_parts = std.ArrayListUnmanaged(generation.Message.ContentPart).empty;
            defer msg_parts.deinit(allocator);

            if (msg.content) |cv| {
                switch (cv) {
                    .string => |s| try text_buf.appendSlice(allocator, s),
                    .array => |arr| {
                        for (arr.items) |part| {
                            if (part != .object) continue;
                            const obj = part.object;
                            const type_val = obj.get("type") orelse continue;
                            if (type_val != .string) continue;
                            const ptype = type_val.string;

                            if (std.mem.eql(u8, ptype, "text")) {
                                if (obj.get("text")) |tv| {
                                    if (tv == .string) {
                                        try text_buf.appendSlice(allocator, tv.string);
                                        try msg_parts.append(allocator, .{ .text = tv.string });
                                    }
                                }
                            } else if (std.mem.eql(u8, ptype, "image_url")) {
                                const url_str = blk: {
                                    const iu = obj.get("image_url") orelse return error.InvalidImageUrl;
                                    if (iu == .object) {
                                        if (iu.object.get("url")) |u| {
                                            if (u == .string) break :blk u.string;
                                        }
                                    } else if (iu == .string) break :blk iu.string;
                                    return error.InvalidImageUrl;
                                };
                                const downloaded = try downloadRemoteContentWithBudgetForRequest(self, allocator, url_str, media_budget);
                                defer allocator.free(downloaded.content_type);
                                var owns_downloaded_data = true;
                                errdefer if (owns_downloaded_data) allocator.free(downloaded.data);
                                try decoded_images.append(allocator, downloaded.data);
                                owns_downloaded_data = false;
                                try msg_images.append(allocator, downloaded.data);
                                try msg_parts.append(allocator, .{ .image = msg_images.items.len - 1 });
                            }
                        }
                    },
                    else => {},
                }
            }

            const content = try allocator.dupe(u8, text_buf.items);
            var owns_content = true;
            errdefer if (owns_content) allocator.free(content);
            const msg_img_slice: ?[]const []const u8 = if (msg_images.items.len > 0)
                try allocator.dupe([]const u8, msg_images.items)
            else
                null;
            var owns_msg_img_slice = msg_img_slice != null;
            errdefer if (owns_msg_img_slice) allocator.free(msg_img_slice.?);
            if (msg_img_slice) |slice| {
                try image_slices.append(allocator, slice);
                owns_msg_img_slice = false;
            }
            const msg_part_slice: ?[]const generation.Message.ContentPart = if (msg_parts.items.len > 0)
                try allocator.dupe(generation.Message.ContentPart, msg_parts.items)
            else
                null;
            var owns_msg_part_slice = msg_part_slice != null;
            errdefer if (owns_msg_part_slice) allocator.free(msg_part_slice.?);
            if (msg_part_slice) |parts| {
                try content_parts.append(allocator, parts);
                owns_msg_part_slice = false;
            }

            try messages.append(allocator, .{
                .role = role,
                .content = content,
                .image_bytes = msg_img_slice,
                .content_parts = msg_part_slice,
            });
            owns_content = false;
        }

        const owned_messages = try messages.toOwnedSlice(allocator);
        errdefer {
            for (owned_messages) |msg| allocator.free(msg.content);
            allocator.free(owned_messages);
        }
        const owned_decoded_images = try decoded_images.toOwnedSlice(allocator);
        errdefer {
            for (owned_decoded_images) |img| allocator.free(img);
            allocator.free(owned_decoded_images);
        }
        const owned_image_slices = try image_slices.toOwnedSlice(allocator);
        errdefer {
            for (owned_image_slices) |slice| allocator.free(slice);
            allocator.free(owned_image_slices);
        }
        const owned_content_parts = try content_parts.toOwnedSlice(allocator);
        return .{
            .allocator = allocator,
            .messages = owned_messages,
            .decoded_images = owned_decoded_images,
            .image_slices = owned_image_slices,
            .content_parts = owned_content_parts,
        };
    }

    fn generateBatchUnsupportedReasonPreflight(body: api.GenerateRequest) ?api.GenerateBatchError {
        if (body.stream orelse false) return .{ .code = "UNSUPPORTED_STREAM", .message = "batch generation does not support stream=true", .retryable = false };
        if (body.tools != null or body.tool_choice != null) return .{ .code = "UNSUPPORTED_TOOLS", .message = "batch generation does not support tools yet", .retryable = false };
        _ = parseGenerateNumericOptions(body.max_tokens, body.top_k) catch |err| return .{
            .code = "INVALID_REQUEST",
            .message = switch (err) {
                error.InvalidMaxTokens => "max_tokens must be between 1 and 2147483647",
                error.InvalidTopK => "top_k must be between 0 and 2147483647",
            },
            .retryable = false,
        };
        _ = parseGenerateSamplingOptions(
            body.temperature,
            body.top_p,
            body.min_p,
            body.repetition_penalty,
            body.frequency_penalty,
            body.presence_penalty,
        ) catch |err| return .{
            .code = "INVALID_REQUEST",
            .message = generateSamplingErrorMessage(err),
            .retryable = false,
        };
        _ = parseGenerateSpeculationOptions(
            body.draft_model != null,
            body.speculative_k,
            body.speculation_policy,
            body.speculation_calibration,
        ) catch |err| return .{
            .code = "INVALID_REQUEST",
            .message = generateSpeculationErrorMessage(err),
            .retryable = false,
        };
        if (body.cache_compaction_ratio) |ratio| {
            validateCacheCompactionRatio(ratio) catch
                return .{ .code = "INVALID_CACHE_COMPACTION_RATIO", .message = "cache_compaction_ratio must be finite, greater than 0, and at most 1", .retryable = false };
            return .{ .code = "UNSUPPORTED_CACHE_COMPACTION", .message = "batch generation does not support cache_compaction_ratio", .retryable = false };
        }
        if (body.draft_model != null) return .{ .code = "UNSUPPORTED_DRAFT_MODEL", .message = "batch generation does not support draft_model yet", .retryable = false };
        if (body.mode) |mode| {
            if (!std.mem.eql(u8, mode, "eager")) return .{ .code = "UNSUPPORTED_MODE", .message = "batch generation requires eager native mode", .retryable = false };
        }
        if (body.compiled_target != null) return .{ .code = "UNSUPPORTED_COMPILED_TARGET", .message = "batch generation does not support compiled_target yet", .retryable = false };
        if (body.backend) |backend| switch (backend) {
            .auto, .native, .metal, .cuda => {},
            .onnx, .xla, .webgpu, .wasm => return .{ .code = "UNSUPPORTED_BACKEND", .message = "batch generation requires a native backend", .retryable = false },
        };
        if (generateRequestHasNonTextContentParts(body)) {
            return .{ .code = "UNSUPPORTED_MULTIMODAL", .message = "batch generation currently supports text-only native requests", .retryable = false };
        }
        return null;
    }

    fn generateBatchMessageParseError(err: anyerror) ?api.GenerateBatchError {
        if (err == error.OutOfMemory) return null;
        if (remoteContentRequestFailure(err)) |failure| {
            return .{
                .code = failure.code,
                .message = failure.message,
                .retryable = failure.retryable,
            };
        }
        return .{
            .code = "INVALID_REQUEST",
            .message = switch (err) {
                error.InvalidImageUrl => "image_url must contain a URL string",
                else => "request messages are invalid",
            },
            .retryable = false,
        };
    }

    fn generateRequestHasNonTextContentParts(body: api.GenerateRequest) bool {
        for (body.messages) |msg| {
            const content = msg.content orelse continue;
            switch (content) {
                .array => |parts| {
                    for (parts.items) |part| {
                        if (part != .object) continue;
                        const type_value = part.object.get("type") orelse continue;
                        if (type_value != .string) continue;
                        if (!std.mem.eql(u8, type_value.string, "text")) return true;
                    }
                },
                .object => return true,
                else => {},
            }
        }
        return false;
    }

    fn generateBatchUnsupportedReason(body: api.GenerateRequest, messages: []const generation.Message) ?api.GenerateBatchError {
        if (generateBatchUnsupportedReasonPreflight(body)) |reason| return reason;
        if (generation.messagesHaveImages(messages) or generation.messagesHaveAudio(messages)) {
            return .{ .code = "UNSUPPORTED_MULTIMODAL", .message = "batch generation currently supports text-only native requests", .retryable = false };
        }
        return null;
    }

    fn generateConfigFromBody(
        allocator: std.mem.Allocator,
        body: api.GenerateRequest,
        owned_grammar_out: *?[]u8,
    ) !generation.GenerationConfig {
        std.debug.assert(owned_grammar_out.* == null);
        var owned_grammar: ?[]u8 = null;
        errdefer if (owned_grammar) |grammar| allocator.free(grammar);
        const numeric = try parseGenerateNumericOptions(body.max_tokens, body.top_k);
        const sampling = try parseGenerateSamplingOptions(
            body.temperature,
            body.top_p,
            body.min_p,
            body.repetition_penalty,
            body.frequency_penalty,
            body.presence_penalty,
        );
        const speculation = try parseGenerateSpeculationOptions(
            body.draft_model != null,
            body.speculative_k,
            body.speculation_policy,
            body.speculation_calibration,
        );
        var config = generation.GenerationConfig{
            .max_tokens = numeric.max_tokens,
            .ignore_eos = body.ignore_eos orelse false,
            .enable_thinking = if (body.chat_template_kwargs) |kwargs| kwargs.enable_thinking else null,
            .temperature = sampling.temperature,
            .top_p = sampling.top_p,
            .top_k = numeric.top_k,
            .min_p = sampling.min_p,
            .repetition_penalty = sampling.repetition_penalty,
            .frequency_penalty = sampling.frequency_penalty,
            .presence_penalty = sampling.presence_penalty,
            .speculative_k = speculation.k,
            .speculation_requested = false,
            .speculation_policy = speculation.policy,
            .speculation_calibration = speculation.calibration,
            .prefill_chunk_size = 0,
            .cache_dtype = body.cache_dtype,
            .cache_compaction_ratio = body.cache_compaction_ratio,
        };
        if (body.response_format) |rf| {
            if (std.mem.eql(u8, rf.type, "json_object")) {
                config.grammar = "json";
            } else if (std.mem.eql(u8, rf.type, "json_schema")) {
                const schema_cfg = rf.json_schema orelse return error.MissingJsonSchema;
                const schema = schema_cfg.schema orelse return error.MissingJsonSchema;
                owned_grammar = try grammar_mod.buildJsonSchemaGrammar(allocator, .{ .object = schema.map });
                config.grammar = owned_grammar;
            } else if (!std.mem.eql(u8, rf.type, "text")) {
                return error.UnsupportedResponseFormat;
            }
        }
        if (body.grammar) |grammar| {
            if (grammar.len == 0) return error.EmptyGrammar;
            if (!std.mem.eql(u8, grammar, "json")) {
                var compiled = try grammar_mod.GbnfGrammar.parse(allocator, grammar);
                compiled.deinit();
            }
            if (owned_grammar) |owned| {
                allocator.free(owned);
                owned_grammar = null;
            }
            config.grammar = grammar;
        }
        owned_grammar_out.* = owned_grammar;
        return config;
    }

    fn buildGenerateResponseValue(
        allocator: std.mem.Allocator,
        model_name: []const u8,
        response_text: []const u8,
        reasoning_text: ?[]const u8,
        finish_reason: []const u8,
        prompt_tokens: usize,
        completion_tokens: usize,
    ) !api.GenerateResponse {
        const completion_id = try allocCompletionId(allocator);
        const created = completionCreatedTimestamp();
        const content = try allocator.dupe(u8, response_text);
        const choices = try allocator.alloc(api.GenerateChoice, 1);
        choices[0] = .{
            .index = 0,
            .message = .{
                .role = .assistant,
                .content = .{ .value = content },
                .reasoning_content = if (reasoning_text) |reasoning|
                    .{ .value = try allocator.dupe(u8, reasoning) }
                else
                    .absent,
            },
            .finish_reason = parseFinishReason(finish_reason),
        };
        return .{
            .id = completion_id,
            .object = "chat.completion",
            .created = created,
            .model = model_name,
            .choices = choices,
            .usage = tokenUsage(prompt_tokens, completion_tokens),
        };
    }

    const BatchGenerateTaskResult = struct {
        text: ?[]const u8 = null,
        reasoning_text: ?[]const u8 = null,
        finish_reason: []const u8 = "length",
        prompt_tokens: usize = 0,
        completion_tokens: usize = 0,
        @"error": ?api.GenerateBatchError = null,
    };

    fn applyBatchGenerateTaskResult(
        allocator: std.mem.Allocator,
        model_name: []const u8,
        task_result: BatchGenerateTaskResult,
        result: *api.GenerateBatchResultItem,
    ) !void {
        if (task_result.@"error") |batch_err| {
            result.@"error" = batch_err;
        } else if (task_result.text) |text| {
            result.response = try buildGenerateResponseValue(
                allocator,
                model_name,
                text,
                task_result.reasoning_text,
                task_result.finish_reason,
                task_result.prompt_tokens,
                task_result.completion_tokens,
            );
        } else {
            result.@"error" = .{
                .code = "GENERATION_FAILED",
                .message = "missing batch generation result",
                .retryable = true,
            };
        }
    }

    fn batchModelLoadError(err: anyerror) api.GenerateBatchError {
        return switch (err) {
            error.UnknownModelCompatibility => .{
                .code = "UNKNOWN_MODEL_COMPATIBILITY",
                .message = "model compatibility is unknown",
                .retryable = false,
            },
            error.IncompatibleModel => .{
                .code = "INCOMPATIBLE_MODEL",
                .message = "model artifact is incompatible with the selected runtime",
                .retryable = false,
            },
            error.ResourceLimitExceeded => .{
                .code = "MODEL_RESOURCE_LIMIT",
                .message = "model resource plan exceeds the configured inference budget",
                .retryable = false,
            },
            error.ResourceTemporarilyUnavailable => .{
                .code = "MODEL_RESOURCE_BUSY",
                .message = "insufficient inference capacity is currently available",
                .retryable = true,
                .retry_after_ms = .{ .value = transient_capacity_retry_after_ms },
            },
            else => .{
                .code = "MODEL_LOAD_FAILED",
                .message = @errorName(err),
                .retryable = true,
            },
        };
    }

    fn batchAdmissionError(err: anyerror) api.GenerateBatchError {
        const retryable = err == error.ResourceTemporarilyUnavailable;
        return .{
            .code = if (retryable) "MODEL_RESOURCE_BUSY" else "MODEL_RESOURCE_LIMIT",
            .message = if (retryable)
                "insufficient inference capacity is currently available"
            else
                "request exceeds the configured inference resource budget",
            .retryable = retryable,
            .retry_after_ms = if (retryable)
                .{ .value = transient_capacity_retry_after_ms }
            else
                .absent,
        };
    }

    const BatchGenerateTask = struct {
        allocator: std.mem.Allocator,
        pipeline: generation.NativeGenerationPipeline,
        messages: []const generation.Message,
        config: generation.GenerationConfig,
        response_format: ?api.GenerateResponseFormat,
        out: *BatchGenerateTaskResult,

        fn run(self: *@This()) std.Io.Cancelable!void {
            self.runInner() catch |err| {
                self.out.@"error" = batchGenerationError(err);
            };
        }

        fn runInner(self: *@This()) !void {
            var result = try self.pipeline.generate(self.messages, self.config);
            defer result.deinit();
            var response_text = result.text;
            var formatted_response_text: ?[]u8 = null;
            defer if (formatted_response_text) |text| self.allocator.free(text);
            formatted_response_text = try coerceGenerateResponseFormat(self.allocator, self.response_format, response_text);
            if (formatted_response_text) |text| response_text = text;
            self.out.text = try self.allocator.dupe(u8, response_text);
            self.out.reasoning_text = if (result.reasoning_text) |reasoning|
                try self.allocator.dupe(u8, reasoning)
            else
                null;
            self.out.finish_reason = result.finish_reason;
            self.out.prompt_tokens = result.prompt_tokens;
            self.out.completion_tokens = result.tokens_used;
        }
    };

    const BatchExecutionMode = enum {
        /// NativeCompute is cheap request state over a shared, internally
        /// synchronized weight store. Give each item its own instance so task
        /// allocators and RunBudget accounting remain independent.
        isolated_parallel,
        /// Metal/CUDA sessions own stateful command/runtime objects. Reusing
        /// those objects serially avoids racing streams, scratch buffers, and
        /// graph-capture state while still amortizing backend construction.
        shared_serial,
    };

    fn batchExecutionMode(backend: runtime.kv.pool.BackendKind) BatchExecutionMode {
        return switch (backend) {
            .native => .isolated_parallel,
            .metal, .cuda => .shared_serial,
        };
    }

    const BatchModelLock = struct {
        mutex: *std.atomic.Mutex,
        owns_outer_lock: bool,

        fn init(
            execution_mode: BatchExecutionMode,
            mutex: *std.atomic.Mutex,
            io: std.Io,
        ) @This() {
            const owns_outer_lock = execution_mode == .shared_serial;
            if (owns_outer_lock) platform.sync.lockYieldingIo(mutex, io);
            return .{ .mutex = mutex, .owns_outer_lock = owns_outer_lock };
        }

        fn pipelineExecutionLock(self: *const @This()) ?*std.atomic.Mutex {
            // The request owns the model mutex for the complete lifetime of a
            // shared Metal/CUDA backend, so the pipeline must not lock it
            // again. Besides avoiding self-deadlock, this keeps request-scoped
            // backend state (including CUDA's RunBudget pointer) from being
            // rebound by another request between generation steps.
            return if (self.owns_outer_lock) null else self.mutex;
        }

        fn deinit(self: *@This()) void {
            if (!self.owns_outer_lock) return;
            self.mutex.unlock();
            self.owns_outer_lock = false;
        }
    };

    const BatchAdmission = struct {
        lease: runtime.tier.memory.AdmissionLease,
        estimate: runtime.tier.memory.Estimate,
    };

    fn mergeSerialBatchEstimate(
        current: ?runtime.tier.memory.Estimate,
        next: runtime.tier.memory.Estimate,
    ) runtime.tier.memory.Estimate {
        const existing = current orelse return next;
        std.debug.assert(existing.kv_tier == next.kv_tier);
        std.debug.assert(existing.scratch_tier == next.scratch_tier);
        return .{
            .prompt_tokens = @max(existing.prompt_tokens, next.prompt_tokens),
            .retained_tokens = @max(existing.retained_tokens, next.retained_tokens),
            .kv_bytes = @max(existing.kv_bytes, next.kv_bytes),
            .kv_tier = existing.kv_tier,
            .scratch_bytes = @max(existing.scratch_bytes, next.scratch_bytes),
            .scratch_tier = existing.scratch_tier,
        };
    }

    fn acquireBatchAdmission(
        self: *Node,
        backend_class: runtime.tier.memory.BackendClass,
        limits: runtime.tier.memory.Limits,
        run_budget: *runtime.tier.memory.RunBudget,
        estimate: runtime.tier.memory.Estimate,
    ) !BatchAdmission {
        try run_budget.reserveEstimate(estimate);
        errdefer run_budget.releaseEstimate(estimate);
        return .{
            .lease = try self.model_manager.acquireRunResources(
                backend_class,
                limits,
                estimate,
            ),
            .estimate = estimate,
        };
    }

    fn releaseBatchAdmission(
        run_budget: *runtime.tier.memory.RunBudget,
        admission: *?BatchAdmission,
    ) void {
        if (admission.*) |*owned| {
            owned.lease.release();
            run_budget.releaseEstimate(owned.estimate);
            admission.* = null;
        }
    }

    pub fn generateBatchContent(self: *Node, ctx: *httpx.Context) !httpx.Response {
        const raw_body = (try ctx.body()) orelse
            return ctx.status(400).json(.{ .@"error" = "missing_body", .message = "Request body required" });
        if (!rawGenerateChatTemplateKwargsAreValid(ctx.allocator, raw_body, true)) {
            return ctx.status(400).json(.{
                .@"error" = "INVALID_REQUEST",
                .message = "chat_template_kwargs accepts only a boolean enable_thinking field",
            });
        }
        var parsed = (try ctx.parseJson(api.GenerateBatchRequest)) orelse
            return ctx.status(400).json(.{ .@"error" = "missing_body", .message = "Request body required" });
        defer parsed.deinit();
        const body = parsed.value;
        if (body.mode) |mode| {
            if (mode != .sync) return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = "only mode=sync is supported" });
        }
        if (body.requests.len == 0) {
            return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = "'requests' must not be empty" });
        }
        if (body.requests.len > max_generate_batch_items) {
            return ctx.status(413).json(.{
                .@"error" = "BATCH_TOO_LARGE",
                .message = try std.fmt.allocPrint(ctx.allocator, "'requests' must contain at most {d} items", .{max_generate_batch_items}),
            });
        }

        var response_arena = std.heap.ArenaAllocator.init(ctx.allocator);
        defer response_arena.deinit();
        const response_alloc = response_arena.allocator();

        const results = try response_alloc.alloc(api.GenerateBatchResultItem, body.requests.len);
        var owned_messages = try ctx.allocator.alloc(OwnedGenerateMessages, body.requests.len);
        defer {
            for (owned_messages) |*owned| owned.deinit();
            ctx.allocator.free(owned_messages);
        }
        var pending = try ctx.allocator.alloc(bool, body.requests.len);
        defer ctx.allocator.free(pending);

        for (body.requests, 0..) |item, idx| {
            results[idx] = .{
                .custom_id = item.custom_id,
                .index = @intCast(idx),
            };
            owned_messages[idx] = .{ .allocator = ctx.allocator };
            pending[idx] = true;
            if (generateBatchUnsupportedReasonPreflight(item.body)) |batch_err| {
                results[idx].@"error" = batch_err;
                pending[idx] = false;
            }
        }

        const admission_units = self.estimateGenerateBatchAdmissionUnitsPreflight(body.requests, pending);
        if (try self.acquireSlotUnits(ctx, admission_units)) |resp| return resp;
        defer self.releaseSlotUnits(admission_units);
        self.metrics.incRequest("generate_batch");
        defer self.metrics.decActive();

        var media_budget = RequestMediaBudget.init(requestMediaMaxBytes(self));
        for (body.requests, 0..) |item, idx| {
            if (!pending[idx]) continue;
            owned_messages[idx] = parseGenerateMessagesWithBudget(self, ctx.allocator, item.body, &media_budget) catch |err| blk: {
                if (err == error.OutOfMemory) return err;
                results[idx].@"error" = generateBatchMessageParseError(err).?;
                break :blk .{ .allocator = ctx.allocator };
            };
            if (results[idx].@"error" == null and owned_messages[idx].messages.len == 0) {
                results[idx].@"error" = .{ .code = "INVALID_REQUEST", .message = "'messages' must not be empty", .retryable = false };
            }
            if (results[idx].@"error" == null) {
                if (generateBatchUnsupportedReason(item.body, owned_messages[idx].messages)) |batch_err| results[idx].@"error" = batch_err;
            }
            pending[idx] = results[idx].@"error" == null;
        }

        while (true) {
            const first_idx = blk: {
                for (pending, 0..) |is_pending, idx| {
                    if (is_pending) break :blk idx;
                }
                break :blk null;
            } orelse break;
            const first_body = body.requests[first_idx].body;
            const model_path = self.resolveRequestModelPath(ctx.allocator, ctx.io, first_body.model, "generators") catch |err| {
                results[first_idx].@"error" = switch (requestModelResolutionErrorKind(err)) {
                    .invalid => .{ .code = "INVALID_REQUEST", .message = "model must be a relative identifier within models_dir", .retryable = false },
                    .missing => .{ .code = "MODEL_NOT_FOUND", .message = "model not found", .retryable = false },
                    .internal => .{ .code = "MODEL_RESOLUTION_FAILED", .message = internalErrorMessage("MODEL_RESOLUTION_FAILED", err), .retryable = true },
                };
                pending[first_idx] = false;
                continue;
            };
            defer ctx.allocator.free(model_path);
            const selection = parseGenerateBackendSelection(first_body.backend, first_body.mode, first_body.compiled_target) catch {
                results[first_idx].@"error" = .{ .code = "INVALID_REQUEST", .message = "unsupported backend", .retryable = false };
                pending[first_idx] = false;
                continue;
            };

            var group_indices = std.ArrayListUnmanaged(usize).empty;
            defer group_indices.deinit(ctx.allocator);
            for (pending, 0..) |is_pending, idx| {
                if (!is_pending) continue;
                const candidate = body.requests[idx].body;
                if (!std.mem.eql(u8, candidate.model, first_body.model)) continue;
                if (candidate.backend != first_body.backend) continue;
                if (!std.mem.eql(u8, candidate.mode orelse "", first_body.mode orelse "")) continue;
                if (!std.mem.eql(u8, candidate.compiled_target orelse "", first_body.compiled_target orelse "")) continue;
                if (!std.mem.eql(u8, candidate.cache_dtype orelse "", first_body.cache_dtype orelse "")) continue;
                try group_indices.append(ctx.allocator, idx);
            }

            const compatibility_summary = self.compatibilitySummaryForDir(ctx.allocator, model_path) catch CompatibilitySummary{
                .level = .unknown,
                .code = .artifact_unreadable,
                .message = "model compatibility could not be determined",
            };
            const compatibility_blocked = compatibility_summary.level == .incompatible or
                (compatibility_summary.level == .unknown and !self.config.allow_unknown_models);
            if (compatibility_blocked) {
                const code = if (compatibility_summary.level == .incompatible)
                    "INCOMPATIBLE_MODEL"
                else
                    "UNKNOWN_MODEL_COMPATIBILITY";
                for (group_indices.items) |idx| {
                    results[idx].@"error" = .{
                        .code = code,
                        .message = compatibility_summary.message,
                        .retryable = false,
                    };
                    pending[idx] = false;
                }
                continue;
            }

            var model_handle = if (selection.native_choice != .auto) blk: {
                var request_session_manager = backends_mod.SessionManager.init(ctx.allocator);
                configureGenerateBackendPreference(&request_session_manager, selection);
                break :blk self.model_manager.acquireFromDirWithPreferredBackends(model_path, request_session_manager.preferred_backends, false) catch |err| {
                    for (group_indices.items) |idx| {
                        results[idx].@"error" = batchModelLoadError(err);
                        pending[idx] = false;
                    }
                    continue;
                };
            } else self.model_manager.acquireFromDir(model_path) catch |err| {
                for (group_indices.items) |idx| {
                    results[idx].@"error" = batchModelLoadError(err);
                    pending[idx] = false;
                }
                continue;
            };
            const model = model_handle.get();

            {
                defer model_handle.release();

                const gpt_config = session_factory.getGptConfig(model.session) orelse {
                    for (group_indices.items) |idx| {
                        results[idx].@"error" = .{ .code = "INVALID_MODEL", .message = "model does not support native generation", .retryable = false };
                        pending[idx] = false;
                    }
                    continue;
                };
                const backend_kind: runtime.kv.pool.BackendKind = switch (model.session.backend()) {
                    .native => .native,
                    .metal => .metal,
                    .cuda => .cuda,
                    else => {
                        for (group_indices.items) |idx| {
                            results[idx].@"error" = .{ .code = "UNSUPPORTED_BACKEND", .message = "batch generation requires a native backend", .retryable = false };
                            pending[idx] = false;
                        }
                        continue;
                    },
                };
                const execution_mode = batchExecutionMode(backend_kind);
                const idle_prefill_ceiling = generation.nativeGenerationPrefillChunkCeiling(
                    backend_kind,
                    gpt_config,
                    self.config.generation_batching.schedulerPolicy().max_idle_prefill_chunk_size,
                );

                var configs = try ctx.allocator.alloc(generation.GenerationConfig, group_indices.items.len);
                defer ctx.allocator.free(configs);
                var owned_grammars = try ctx.allocator.alloc(?[]u8, group_indices.items.len);
                @memset(owned_grammars, null);
                defer {
                    for (owned_grammars) |owned_grammar| {
                        if (owned_grammar) |grammar| ctx.allocator.free(grammar);
                    }
                    ctx.allocator.free(owned_grammars);
                }
                var prompt_tokens = try ctx.allocator.alloc(usize, group_indices.items.len);
                defer ctx.allocator.free(prompt_tokens);
                var prompt_bytes = try ctx.allocator.alloc(usize, group_indices.items.len);
                defer ctx.allocator.free(prompt_bytes);
                var valid_count: usize = 0;
                for (group_indices.items, 0..) |idx, pos| {
                    configs[pos] = generateConfigFromBody(ctx.allocator, body.requests[idx].body, &owned_grammars[pos]) catch |err| {
                        if (err == error.OutOfMemory) return err;
                        results[idx].@"error" = .{ .code = "INVALID_REQUEST", .message = "generation request options are invalid", .retryable = false };
                        pending[idx] = false;
                        continue;
                    };
                    const item_prompt_estimate = self.estimateNativePromptTokens(
                        ctx.allocator,
                        model_path,
                        model,
                        gpt_config,
                        owned_messages[idx].messages,
                        configs[pos].max_tokens,
                        if (configs[pos].speculation_requested and configs[pos].speculation_policy != .off and configs[pos].speculative_k > 0) 1 else 0,
                        configs[pos].enable_thinking,
                        null,
                    ) catch |err| {
                        results[idx].@"error" = if (err == error.PromptTooLong)
                            .{
                                .code = "INVALID_REQUEST",
                                .message = "prompt exceeds the model context window after reserving output tokens",
                                .retryable = false,
                            }
                        else
                            .{ .code = "TOKENIZE_FAILED", .message = internalErrorMessage("TOKENIZE_FAILED", err), .retryable = false };
                        pending[idx] = false;
                        continue;
                    };
                    prompt_tokens[pos] = item_prompt_estimate;
                    prompt_bytes[pos] = self.estimateGeneratePromptBytes(owned_messages[idx].messages);
                    valid_count += 1;
                }
                if (valid_count == 0) continue;

                const kv_dtype = if (first_body.cache_dtype) |name|
                    runtime.kv.pool.parseKvDType(name) orelse {
                        for (group_indices.items) |idx| {
                            results[idx].@"error" = .{ .code = "INVALID_REQUEST", .message = "invalid cache_dtype value", .retryable = false };
                            pending[idx] = false;
                        }
                        continue;
                    }
                else
                    session_factory.recommendedKvDTypeForSession(model.session, backend_kind);
                const budget_backend_class: runtime.tier.memory.BackendClass = switch (backend_kind) {
                    .native => .cpu,
                    .metal, .cuda => .gpu,
                };
                const budget_limits = self.config.generation_budget_overrides.apply(session_factory.widenBudgetLimitsForSession(
                    model.session,
                    self.defaultGenerationLimits(budget_backend_class),
                ));
                var leases = try ctx.allocator.alloc(runtime.scheduler.native_generate.Lease, group_indices.items.len);
                for (leases) |*lease| lease.* = .{ .request_id = 0, .reserved_units = 0, .prompt_bytes = 0, .max_tokens = 0, .prefill_chunk_size = 0, .active_requests_snapshot = 0 };
                const standalone_prefill_plans = try ctx.allocator.alloc(
                    ?runtime.scheduler.native_generate.PrefillChunkPlan,
                    group_indices.items.len,
                );
                @memset(standalone_prefill_plans, null);
                defer {
                    if (model.native_generate_coordinator) |coordinator| {
                        for (leases) |*lease| {
                            if (lease.request_id != 0) {
                                coordinator.release(lease.*);
                                lease.request_id = 0;
                            }
                        }
                    }
                    for (standalone_prefill_plans) |*maybe_plan| {
                        if (maybe_plan.*) |*plan| plan.deinit(ctx.allocator);
                    }
                    ctx.allocator.free(standalone_prefill_plans);
                    ctx.allocator.free(leases);
                }
                if (model.native_generate_coordinator) |coordinator| {
                    for (group_indices.items, 0..) |idx, pos| {
                        if (!pending[idx]) continue;
                        const admission_item_units = self.estimateGenerateAdmissionUnits(owned_messages[idx].messages, configs[pos].max_tokens);
                        leases[pos] = self.acquireNativeGenerateLease(coordinator, .{
                            .requested_units = admission_item_units,
                            .prompt_bytes = prompt_bytes[pos],
                            .prompt_tokens = prompt_tokens[pos],
                            .prefill_chunk_limit = if (configs[pos].prefill_chunk_size == 0) idle_prefill_ceiling else 0,
                            .max_tokens = configs[pos].max_tokens,
                        }) catch |err| {
                            results[idx].@"error" = .{ .code = "QUEUE_FULL", .message = @errorName(err), .retryable = true };
                            pending[idx] = false;
                            continue;
                        };
                    }
                    for (group_indices.items, 0..) |idx, pos| {
                        if (!pending[idx]) continue;
                        leases[pos].prefill_chunk_size = coordinator.recommendPrefillChunkFor(leases[pos].request_id);
                    }
                }
                var shared_run_budget = runtime.tier.memory.RunBudget.init(budget_limits);
                const task_run_budgets = try ctx.allocator.alloc(
                    runtime.tier.memory.RunBudget,
                    group_indices.items.len,
                );
                defer ctx.allocator.free(task_run_budgets);
                for (task_run_budgets) |*budget| {
                    budget.* = runtime.tier.memory.RunBudget.init(budget_limits);
                }
                const resource_estimates = try ctx.allocator.alloc(
                    ?runtime.tier.memory.Estimate,
                    group_indices.items.len,
                );
                @memset(resource_estimates, null);
                defer ctx.allocator.free(resource_estimates);
                // Allocate ownership slots before acquiring capacity. Once a
                // process-wide lease is granted, publishing it here cannot fail.
                const admissions = try ctx.allocator.alloc(?BatchAdmission, group_indices.items.len);
                @memset(admissions, null);
                defer {
                    for (admissions, 0..) |*maybe_admission, pos| {
                        const item_run_budget = if (execution_mode == .isolated_parallel)
                            &task_run_budgets[pos]
                        else
                            &shared_run_budget;
                        releaseBatchAdmission(item_run_budget, maybe_admission);
                    }
                    ctx.allocator.free(admissions);
                }
                var runnable_count: usize = 0;
                const budget_components = [_]runtime.tier.memory.GptGenerationBudgetComponent{
                    .{ .backend = backend_kind, .kv_dtype = kv_dtype, .config = gpt_config },
                };
                for (group_indices.items, 0..) |idx, pos| {
                    if (!pending[idx]) continue;
                    const scheduler_ceiling = if (model.native_generate_coordinator != null)
                        leases[pos].prefill_chunk_size
                    else
                        @min(
                            prompt_tokens[pos],
                            if (configs[pos].prefill_chunk_size == 0)
                                idle_prefill_ceiling
                            else
                                self.config.generation_batching.schedulerPolicy().max_idle_prefill_chunk_size,
                        );
                    const admission_prefill_ceiling = if (configs[pos].prefill_chunk_size > 0)
                        @min(configs[pos].prefill_chunk_size, scheduler_ceiling)
                    else
                        scheduler_ceiling;
                    const speculative_budget_bonus: usize = if (configs[pos].speculation_requested and
                        configs[pos].speculation_policy != .off and
                        configs[pos].speculative_k > 0) 1 else 0;
                    const budget_max_tokens = @as(usize, @intCast(@max(configs[pos].max_tokens, 1))) + speculative_budget_bonus;
                    var sizing_budget = runtime.tier.memory.RunBudget.init(budget_limits);
                    const prefill_admission = runtime.tier.memory.reserveGptGenerationPrefill(
                        &sizing_budget,
                        &budget_components,
                        prompt_tokens[pos],
                        budget_max_tokens,
                        admission_prefill_ceiling,
                    ) catch |err| {
                        results[idx].@"error" = .{
                            .code = "MEMORY_BUDGET_EXCEEDED",
                            .message = @errorName(err),
                            .retryable = false,
                        };
                        pending[idx] = false;
                        if (model.native_generate_coordinator) |coordinator| {
                            coordinator.release(leases[pos]);
                            leases[pos].request_id = 0;
                        }
                        continue;
                    };
                    configs[pos].prefill_chunk_size = prefill_admission.max_chunk_rows;
                    if (model.native_generate_coordinator) |coordinator| {
                        coordinator.admitPrefillChunkPlan(
                            &leases[pos],
                            prompt_tokens[pos],
                            prefill_admission.max_chunk_rows,
                            prefill_admission.max_scratch_bytes,
                            .fixed_ceiling,
                        ) catch |err| {
                            results[idx].@"error" = .{
                                .code = "PREFILL_PLAN_ADMISSION_FAILED",
                                .message = @errorName(err),
                                .retryable = false,
                            };
                            pending[idx] = false;
                            coordinator.release(leases[pos]);
                            leases[pos].request_id = 0;
                            continue;
                        };
                        configs[pos].prefill_chunk_plan = leases[pos].prefill_chunk_plan;
                    } else {
                        standalone_prefill_plans[pos] = runtime.scheduler.native_generate.PrefillChunkPlan.init(
                            ctx.allocator,
                            prompt_tokens[pos],
                            prefill_admission.max_chunk_rows,
                            prefill_admission.max_scratch_bytes,
                            if (valid_count == 1) .sole_idle else .shared,
                            .fixed_ceiling,
                        ) catch |err| {
                            results[idx].@"error" = .{
                                .code = "PREFILL_PLAN_ADMISSION_FAILED",
                                .message = @errorName(err),
                                .retryable = false,
                            };
                            pending[idx] = false;
                            continue;
                        };
                        configs[pos].prefill_chunk_plan = standalone_prefill_plans[pos].?;
                    }
                    logAdmittedPrefillChunkPlan(configs[pos].prefill_chunk_plan.?);
                    const resource_estimate = runtime.tier.memory.estimateGptGeneration(
                        backend_kind,
                        kv_dtype,
                        gpt_config,
                        prompt_tokens[pos],
                        budget_max_tokens,
                        configs[pos].prefill_chunk_size,
                    ) catch |err| {
                        results[idx].@"error" = .{
                            .code = if (err == error.InvalidModelConfig) "INVALID_MODEL" else "MODEL_RESOURCE_LIMIT",
                            .message = @errorName(err),
                            .retryable = false,
                        };
                        pending[idx] = false;
                        if (model.native_generate_coordinator) |coordinator| {
                            coordinator.release(leases[pos]);
                            leases[pos].request_id = 0;
                        }
                        continue;
                    };
                    resource_estimates[pos] = resource_estimate;
                    if (execution_mode == .shared_serial) {
                        runnable_count += 1;
                        continue;
                    }
                    admissions[pos] = self.acquireBatchAdmission(
                        budget_backend_class,
                        budget_limits,
                        &task_run_budgets[pos],
                        resource_estimate,
                    ) catch |err| {
                        results[idx].@"error" = batchAdmissionError(err);
                        pending[idx] = false;
                        if (model.native_generate_coordinator) |coordinator| {
                            coordinator.release(leases[pos]);
                            leases[pos].request_id = 0;
                        }
                        continue;
                    };
                    runnable_count += 1;
                }
                if (runnable_count == 0) continue;

                // A shared GPU backend executes these requests serially, so
                // one reservation covering the largest per-item KV and scratch
                // working sets is sufficient. Acquire it before the model lock:
                // admission may evict an idle model and destroy its backend.
                var shared_batch_admission: ?BatchAdmission = null;
                defer releaseBatchAdmission(&shared_run_budget, &shared_batch_admission);
                if (execution_mode == .shared_serial) {
                    while (runnable_count > 0 and shared_batch_admission == null) {
                        var combined: ?runtime.tier.memory.Estimate = null;
                        var largest_pos: ?usize = null;
                        var largest_pressure: usize = 0;
                        for (group_indices.items, 0..) |idx, pos| {
                            if (!pending[idx]) continue;
                            const estimate = resource_estimates[pos].?;
                            combined = mergeSerialBatchEstimate(combined, estimate);
                            const pressure = estimate.kv_bytes +| estimate.scratch_bytes;
                            if (largest_pos == null or pressure > largest_pressure) {
                                largest_pos = pos;
                                largest_pressure = pressure;
                            }
                        }
                        const estimate = combined orelse break;
                        shared_batch_admission = self.acquireBatchAdmission(
                            budget_backend_class,
                            budget_limits,
                            &shared_run_budget,
                            estimate,
                        ) catch |err| {
                            // Preserve per-item batch semantics: if the largest
                            // request cannot be admitted, reject it and retry a
                            // reservation sized for the remaining requests.
                            const pos = largest_pos.?;
                            const idx = group_indices.items[pos];
                            results[idx].@"error" = batchAdmissionError(err);
                            pending[idx] = false;
                            runnable_count -= 1;
                            if (model.native_generate_coordinator) |coordinator| {
                                coordinator.release(leases[pos]);
                                leases[pos].request_id = 0;
                            }
                            continue;
                        };
                    }
                    if (shared_batch_admission == null) continue;
                }

                // Match the single-request lock order: scheduler and resource
                // admission must complete before the model mutex is acquired.
                // Metal/CUDA backends reference stateful session-owned runtime
                // or provider state, so from backend binding onward retain the
                // mutex until every backend/KV defer has run. Native mode
                // creates an independent backend per item and instead uses the
                // scheduler's per-step execution lock below.
                var batch_model_lock = BatchModelLock.init(
                    execution_mode,
                    model.nativeGenerationMutex(),
                    ctx.io,
                );
                defer batch_model_lock.deinit();

                var shared_cb: ?ops.ComputeBackend = null;
                if (execution_mode == .shared_serial) {
                    shared_cb = session_factory.getComputeBackendWithBudget(
                        model.session,
                        ctx.allocator,
                        &shared_run_budget,
                    ) catch |err| {
                        for (group_indices.items) |idx| {
                            if (!pending[idx]) continue;
                            results[idx].@"error" = .{ .code = "BACKEND_ERROR", .message = @errorName(err), .retryable = true };
                            pending[idx] = false;
                        }
                        continue;
                    };
                }
                defer if (shared_cb) |*cb| cb.deinit();

                var kv_manager = runtime.kv.manager.KvManager.init(ctx.allocator);
                defer kv_manager.deinit();
                const kv_pool_config = generation.kvPoolConfig(backend_kind, kv_dtype, gpt_config, generationKvSlidingTrimForced());
                const pool_id = kv_manager.addPool(kv_pool_config) catch |err| {
                    for (group_indices.items) |idx| {
                        if (!pending[idx]) continue;
                        results[idx].@"error" = .{ .code = "BACKEND_ERROR", .message = internalErrorMessage("BACKEND_ERROR", err), .retryable = true };
                        pending[idx] = false;
                    }
                    continue;
                };
                var kv_storage = runtime.kv.storage_runtime.KvStorageRuntime.init(ctx.allocator, kv_pool_config) catch |err| {
                    for (group_indices.items) |idx| {
                        if (!pending[idx]) continue;
                        results[idx].@"error" = .{ .code = "BACKEND_ERROR", .message = internalErrorMessage("BACKEND_ERROR", err), .retryable = true };
                        pending[idx] = false;
                    }
                    continue;
                };
                defer kv_storage.deinit();
                if (shared_cb) |*cb| {
                    cb.provisionKvDeviceWriteHook(&kv_storage) catch |err| {
                        for (group_indices.items) |idx| {
                            if (!pending[idx]) continue;
                            results[idx].@"error" = .{ .code = "BACKEND_ERROR", .message = @errorName(err), .retryable = true };
                            pending[idx] = false;
                        }
                        continue;
                    };
                }

                var kv_mutex: std.atomic.Mutex = .unlocked;
                var task_arenas = try ctx.allocator.alloc(std.heap.ArenaAllocator, group_indices.items.len);
                for (task_arenas) |*arena| arena.* = std.heap.ArenaAllocator.init(ctx.allocator);
                defer {
                    for (task_arenas) |*arena| arena.deinit();
                    ctx.allocator.free(task_arenas);
                }
                const task_cbs = try ctx.allocator.alloc(?ops.ComputeBackend, group_indices.items.len);
                @memset(task_cbs, null);
                defer {
                    for (task_cbs) |*maybe_cb| {
                        if (maybe_cb.*) |*cb| cb.deinit();
                    }
                    ctx.allocator.free(task_cbs);
                }
                const decode_states = try ctx.allocator.alloc(generation.NativeDecodeState, group_indices.items.len);
                for (decode_states) |*state| state.* = generation.NativeDecodeState.initContiguous(ctx.allocator);
                defer {
                    for (decode_states) |*state| state.deinit();
                    ctx.allocator.free(decode_states);
                }
                var task_results = try ctx.allocator.alloc(BatchGenerateTaskResult, group_indices.items.len);
                for (task_results) |*task_result| task_result.* = .{};
                defer ctx.allocator.free(task_results);
                var task_ran = try ctx.allocator.alloc(bool, group_indices.items.len);
                @memset(task_ran, false);
                defer ctx.allocator.free(task_ran);
                var tasks = try ctx.allocator.alloc(BatchGenerateTask, group_indices.items.len);
                defer ctx.allocator.free(tasks);

                var spawned_any = false;
                var group = std.Io.Group.init;
                for (group_indices.items, 0..) |idx, pos| {
                    if (!pending[idx]) continue;
                    const task_alloc = task_arenas[pos].allocator();
                    if (execution_mode == .isolated_parallel) {
                        task_cbs[pos] = session_factory.getComputeBackendWithBudget(
                            model.session,
                            task_alloc,
                            &task_run_budgets[pos],
                        ) catch |err| {
                            releaseBatchAdmission(&task_run_budgets[pos], &admissions[pos]);
                            results[idx].@"error" = .{ .code = "BACKEND_ERROR", .message = @errorName(err), .retryable = true };
                            pending[idx] = false;
                            if (model.native_generate_coordinator) |coordinator| {
                                coordinator.release(leases[pos]);
                                leases[pos].request_id = 0;
                            }
                            continue;
                        };
                    }
                    const task_cb = if (execution_mode == .isolated_parallel)
                        task_cbs[pos].?
                    else
                        shared_cb.?;
                    decode_states[pos] = generation.NativeDecodeState.initPaged(task_alloc, &kv_manager, pool_id, model.shared_moe_cache);
                    decode_states[pos].kv_lock = &kv_mutex;
                    decode_states[pos].kv_storage = &kv_storage;
                    tasks[pos] = .{
                        .allocator = task_alloc,
                        .pipeline = .{
                            .allocator = task_alloc,
                            .io = ctx.io,
                            .cb = task_cb,
                            .session = model.session,
                            .gpt_config = gpt_config,
                            .kv_dtype = kv_dtype,
                            .shared_moe_cache = model.shared_moe_cache,
                            .tokenizer = model.getTokenizer(),
                            .add_bos_token = model.manifest.add_bos_token,
                            .bos_token = model.manifest.bos_token,
                            .chat_template = model.chat_tmpl,
                            .print_timing = serverGenerateTimingEnabled(),
                            .model_dir = model_path,
                            .gguf_projector_path = model.manifest.gguf_projector_path,
                            .decode_state = &decode_states[pos],
                            .scheduler = model.native_generate_coordinator,
                            .scheduler_lease = if (model.native_generate_coordinator != null) &leases[pos] else null,
                            .execution_lock = batch_model_lock.pipelineExecutionLock(),
                        },
                        .messages = owned_messages[idx].messages,
                        .config = configs[pos],
                        .response_format = body.requests[idx].body.response_format,
                        .out = &task_results[pos],
                    };
                    task_ran[pos] = true;
                    if (execution_mode == .shared_serial) {
                        tasks[pos].run() catch {};
                        if (model.native_generate_coordinator) |coordinator| {
                            if (leases[pos].request_id != 0) {
                                coordinator.release(leases[pos]);
                                leases[pos].request_id = 0;
                            }
                        }
                        decode_states[pos].deinit();
                        decode_states[pos] = generation.NativeDecodeState.initContiguous(ctx.allocator);
                        try applyBatchGenerateTaskResult(
                            response_alloc,
                            body.requests[idx].body.model,
                            task_results[pos],
                            &results[idx],
                        );
                        pending[idx] = false;
                        task_arenas[pos].deinit();
                        task_arenas[pos] = std.heap.ArenaAllocator.init(ctx.allocator);
                        continue;
                    }
                    group.concurrent(ctx.io, BatchGenerateTask.run, .{&tasks[pos]}) catch {
                        tasks[pos].run() catch {};
                        continue;
                    };
                    spawned_any = true;
                }
                if (spawned_any) group.await(ctx.io) catch {};
                for (group_indices.items, 0..) |idx, pos| {
                    if (!pending[idx]) continue;
                    if (!task_ran[pos]) {
                        pending[idx] = false;
                        continue;
                    }
                    try applyBatchGenerateTaskResult(
                        response_alloc,
                        body.requests[idx].body.model,
                        task_results[pos],
                        &results[idx],
                    );
                    pending[idx] = false;
                }
            }
        }

        var succeeded: i64 = 0;
        for (results) |item| {
            if (item.response != null and item.@"error" == null) succeeded += 1;
        }
        const total: i64 = @intCast(results.len);
        return ctx.json(api.GenerateBatchResponse{
            .object = "generate.batch",
            .data = results,
            .summary = .{ .total = total, .succeeded = succeeded, .failed = total - succeeded },
        });
    }

    pub fn chatCompletions(self: *Node, ctx: *httpx.Context) !httpx.Response {
        return self.generateContent(ctx);
    }

    const ToolStopCtx = struct {
        parser: *tool_parser_mod.Parser,
        errored: ?anyerror = null,

        fn onToken(raw_ctx: *anyopaque, token_text: []const u8) bool {
            const self: *@This() = @ptrCast(@alignCast(raw_ctx));
            _ = self.parser.feed(token_text) catch |err| {
                self.errored = err;
                return false;
            };
            return self.parser.toolCalls().len == 0;
        }
    };

    fn generateMaybeStopOnTool(
        pipeline: anytype,
        messages: []const generation.Message,
        config: generation.GenerationConfig,
        tool_parser: ?*tool_parser_mod.Parser,
    ) !generation.GenerationResult {
        var tool_stop_ctx: ?ToolStopCtx = if (tool_parser) |parser| blk: {
            parser.reset();
            break :blk .{ .parser = parser };
        } else null;

        var result = if (tool_stop_ctx) |*stop_ctx|
            try pipeline.generateStreaming(messages, config, @ptrCast(stop_ctx), ToolStopCtx.onToken)
        else
            try pipeline.generate(messages, config);
        errdefer result.deinit();

        if (tool_stop_ctx) |stop_ctx| {
            if (stop_ctx.errored) |err| return err;
        }

        return result;
    }

    /// Send a completed GenerationResult as a single SSE stream.
    fn sendResultAsSSE(
        _: *Node,
        ctx: *httpx.Context,
        model_name: []const u8,
        result: *@import("../pipelines/generation.zig").GenerationResult,
        tool_parser: ?*tool_parser_mod.Parser,
        include_usage: bool,
    ) !httpx.Response {
        var writer = ctx.streamResponse(200) catch |err| {
            std.debug.print("streamResponse failed: {}\n", .{err});
            return ctx.status(500).json(.{ .@"error" = "STREAM_INIT_FAILED", .message = internalErrorMessage("STREAM_INIT_FAILED", err) });
        };
        const stream_id = try allocCompletionId(ctx.allocator);
        defer ctx.allocator.free(stream_id);
        const stream_created = completionCreatedTimestamp();
        var chunk_payload: std.Io.Writer.Allocating = .init(ctx.allocator);
        defer chunk_payload.deinit();

        emitRoleDelta(&writer, &chunk_payload, stream_id, stream_created, model_name) catch |err| {
            writeInternalStreamError(&writer, "STREAM_WRITE_FAILED", err);
            writer.close() catch {};
            return ctx.response.build();
        };

        writeStreamCompletion(
            &chunk_payload,
            &writer,
            stream_id,
            stream_created,
            model_name,
            result.text,
            result.reasoning_text,
            result.finish_reason,
            tool_parser,
            result.speculative,
        ) catch |err| {
            writeInternalStreamError(&writer, "STREAM_WRITE_FAILED", err);
            writer.close() catch {};
            return ctx.response.build();
        };
        emitTerminalUsageOrDone(
            &writer,
            &chunk_payload,
            stream_id,
            stream_created,
            model_name,
            result.prompt_tokens,
            result.tokens_used,
            result.cached_prompt_tokens,
            include_usage,
        ) catch |err| {
            writeInternalStreamError(&writer, "STREAM_WRITE_FAILED", err);
            writer.close() catch {};
            return ctx.response.build();
        };
        writer.close() catch {};
        return ctx.response.build();
    }

    const ParsedMultimodalRerankDocument = struct {
        allocator: std.mem.Allocator,
        text: []u8,
        /// Text plus one canonical Qwen vision marker at each image's exact
        /// content-part position. ColQwen continues to consume `text`.
        qwen_content: []u8,
        images: [][]const u8,

        fn deinit(self: *ParsedMultimodalRerankDocument) void {
            self.allocator.free(self.text);
            self.allocator.free(self.qwen_content);
            for (self.images) |img| self.allocator.free(img);
            self.allocator.free(self.images);
        }
    };

    fn qwen3VlRerankerMediaAdmission(
        documents: []const ParsedMultimodalRerankDocument,
        config: gpt_model_mod.Config,
        max_length: usize,
    ) !generation.NativeGenerationMediaAdmission {
        if (config.family != .qwen3_vl or max_length == 0) {
            return error.InvalidRerankerConfiguration;
        }
        var result = generation.NativeGenerationMediaAdmission{};
        for (documents) |document| {
            if (document.images.len == 0) continue;
            const context_limit = max_length / document.images.len;
            if (context_limit < 4) return error.InputTokenLimitExceeded;
            const estimate = try qwen3vl_projector.estimateAdmission(
                document.images,
                config,
                .{
                    .max_images = 8,
                    .min_merged_tokens = 4,
                    .max_merged_tokens = @min(@as(usize, 576), context_limit),
                },
            );
            result.token_allowance = @max(
                result.token_allowance,
                std.math.add(usize, estimate.visual_tokens, document.images.len) catch
                    return error.InputTokenLimitExceeded,
            );
            result.host_scratch_bytes = @max(result.host_scratch_bytes, estimate.host_scratch_bytes);
            result.backend_scratch_bytes = @max(result.backend_scratch_bytes, estimate.backend_scratch_bytes);
        }
        if (result.token_allowance == 0) return error.InvalidRerankerImageCount;
        return result;
    }

    fn writeRerankScoresResponse(
        ctx: *httpx.Context,
        model_name: []const u8,
        scores: []const f32,
        prompt_tokens: usize,
    ) !httpx.Response {
        const data = try ctx.allocator.alloc(api.RerankObject, scores.len);
        defer ctx.allocator.free(data);
        for (scores, 0..) |score, i| {
            data[i] = .{
                .object = "rerank.score",
                .index = @intCast(i),
                .score = score,
            };
        }
        return ctx.json(api.RerankResponse{
            .object = "list",
            .data = data,
            .model = model_name,
            .usage = tokenUsage(prompt_tokens, 0),
        });
    }

    fn parseChatMessageContentToTextAndImages(self: *Node, allocator: std.mem.Allocator, content: api.ChatMessageContent) !ParsedMultimodalRerankDocument {
        var media_budget = RequestMediaBudget.init(requestMediaMaxBytes(self));
        return self.parseChatMessageContentToTextAndImagesWithBudget(allocator, content, &media_budget);
    }

    fn parseChatMessageContentToTextAndImagesWithBudget(
        self: *Node,
        allocator: std.mem.Allocator,
        content: api.ChatMessageContent,
        media_budget: *RequestMediaBudget,
    ) !ParsedMultimodalRerankDocument {
        var text_buf = std.ArrayListUnmanaged(u8).empty;
        errdefer text_buf.deinit(allocator);
        var qwen_content_buf = std.ArrayListUnmanaged(u8).empty;
        errdefer qwen_content_buf.deinit(allocator);
        var images = std.ArrayListUnmanaged([]const u8).empty;
        errdefer {
            for (images.items) |img| allocator.free(img);
            images.deinit(allocator);
        }

        switch (content) {
            .string => |s| {
                try text_buf.appendSlice(allocator, s);
                try qwen_content_buf.appendSlice(allocator, s);
            },
            .array => |arr| {
                for (arr.items) |part| {
                    if (part != .object) return error.UnsupportedContentPartType;
                    const obj = part.object;
                    const type_val = obj.get("type") orelse return error.UnsupportedContentPartType;
                    if (type_val != .string) return error.UnsupportedContentPartType;
                    const ptype = type_val.string;

                    if (std.mem.eql(u8, ptype, "text")) {
                        const text_val = obj.get("text") orelse return error.UnsupportedContentPartType;
                        if (text_val != .string) return error.UnsupportedContentPartType;
                        try text_buf.appendSlice(allocator, text_val.string);
                        try qwen_content_buf.appendSlice(allocator, text_val.string);
                    } else if (std.mem.eql(u8, ptype, "image_url")) {
                        const iu = obj.get("image_url") orelse return error.UnsupportedContentPartType;
                        const url_str = if (iu == .object)
                            if (iu.object.get("url")) |u| (if (u == .string) u.string else null) else null
                        else if (iu == .string)
                            iu.string
                        else
                            null;
                        const url = url_str orelse return error.UnsupportedContentPartType;
                        if (std.mem.startsWith(u8, url, "data:")) {
                            const decoded = decodeDataUriWithBudget(allocator, url, media_budget) catch |err| switch (err) {
                                error.OutOfMemory, error.RemoteContentTooLarge => return err,
                                else => return error.InvalidImageDataUri,
                            };
                            errdefer decoded.deinit(allocator);
                            try qwen_content_buf.appendSlice(allocator, qwen3vl_reranker.image_marker);
                            try images.append(allocator, decoded.data);
                        } else {
                            const downloaded = try downloadRemoteContentWithBudgetForRequest(self, allocator, url, media_budget);
                            defer allocator.free(downloaded.content_type);
                            errdefer allocator.free(downloaded.data);
                            try qwen_content_buf.appendSlice(allocator, qwen3vl_reranker.image_marker);
                            try images.append(allocator, downloaded.data);
                        }
                    } else if (std.mem.eql(u8, ptype, "media")) {
                        const data_val = obj.get("data") orelse return error.UnsupportedContentPartType;
                        const mime_val = obj.get("mime_type") orelse return error.UnsupportedContentPartType;
                        if (data_val != .string or mime_val != .string) return error.UnsupportedContentPartType;
                        if (!std.mem.startsWith(u8, mime_val.string, "image/")) return error.UnsupportedContentPartType;
                        const decoded_payload = decodeMediaDataWithBudget(allocator, data_val.string, media_budget) catch |err| switch (err) {
                            error.OutOfMemory, error.RemoteContentTooLarge => return err,
                            else => return error.UnsupportedContentPartType,
                        };
                        const decoded = decoded_payload.data;
                        errdefer allocator.free(decoded);
                        if (!mediaMimeMatches(mime_val.string, decoded_payload.mime_type)) return error.UnsupportedContentPartType;
                        try qwen_content_buf.appendSlice(allocator, qwen3vl_reranker.image_marker);
                        try images.append(allocator, decoded);
                    } else {
                        return error.UnsupportedContentPartType;
                    }
                }
            },
            else => return error.UnsupportedContentPartType,
        }

        const owned_text = try text_buf.toOwnedSlice(allocator);
        errdefer allocator.free(owned_text);
        const owned_qwen_content = try qwen_content_buf.toOwnedSlice(allocator);
        errdefer allocator.free(owned_qwen_content);
        const owned_images = try images.toOwnedSlice(allocator);
        return .{
            .allocator = allocator,
            .text = owned_text,
            .qwen_content = owned_qwen_content,
            .images = owned_images,
        };
    }

    fn buildGenerateResponse(
        self: *Node,
        ctx: *httpx.Context,
        model_name: []const u8,
        response_text: []const u8,
        reasoning_text: ?[]const u8,
        finish_reason: []const u8,
        prompt_tokens: usize,
        completion_tokens: usize,
        cached_prompt_tokens: usize,
        tool_calls: ?[]const tool_parser_mod.ToolCall,
        speculative: ?generation.SpeculativeDecodeStats,
    ) !httpx.Response {
        recordSpeculationOutcome(&self.metrics, speculative);
        var arena = std.heap.ArenaAllocator.init(ctx.allocator);
        defer arena.deinit();
        const alloc = arena.allocator();

        const completion_id = try allocCompletionId(alloc);
        const created = completionCreatedTimestamp();

        var message: api.GenerateMessage = .{
            .role = .assistant,
            .content = .{ .value = response_text },
            .reasoning_content = if (reasoning_text) |reasoning|
                .{ .value = reasoning }
            else
                .absent,
        };
        if (tool_calls) |calls| {
            if (calls.len > 0) {
                const api_calls = try alloc.alloc(generating_api.ToolCall, calls.len);
                for (calls, 0..) |call, i| {
                    api_calls[i] = .{
                        .id = call.id,
                        .type = call.type,
                        .function = .{ .name = call.function.name, .arguments = call.function.arguments },
                    };
                }
                message.content = .null_value;
                message.tool_calls = api_calls;
            }
        }

        const choices = [_]api.GenerateChoice{.{
            .index = 0,
            .message = message,
            .finish_reason = parseFinishReason(finish_reason),
        }};
        return ctx.json(api.GenerateResponse{
            .id = completion_id,
            .object = "chat.completion",
            .created = created,
            .model = model_name,
            .choices = &choices,
            .usage = tokenUsageWithCachedPrompt(prompt_tokens, completion_tokens, cached_prompt_tokens),
            .speculation = if (generateSpeculationStatus(speculative)) |status|
                .{ .value = status }
            else
                .absent,
        });
    }

    fn prependSystemPrompt(
        allocator: std.mem.Allocator,
        messages: *std.ArrayListUnmanaged(generation.Message),
        prompt: []const u8,
    ) !void {
        if (messages.items.len > 0 and std.mem.eql(u8, messages.items[0].role, "system")) {
            const merged = try std.fmt.allocPrint(allocator, "{s}\n\n{s}", .{ prompt, messages.items[0].content });
            allocator.free(messages.items[0].content);
            messages.items[0].content = merged;
            return;
        }

        const content = try allocator.dupe(u8, prompt);
        errdefer allocator.free(content);
        try messages.insert(allocator, 0, .{
            .role = "system",
            .content = content,
            .image_bytes = null,
        });
    }

    fn buildFunctionGemmaPrompt(
        allocator: std.mem.Allocator,
        bos_token: []const u8,
        messages: []const generation.Message,
    ) ![]u8 {
        var buf = std.ArrayListUnmanaged(u8).empty;
        errdefer buf.deinit(allocator);

        if (bos_token.len > 0) try buf.appendSlice(allocator, bos_token);
        for (messages) |message| {
            const role = if (std.mem.eql(u8, message.role, "assistant"))
                "model"
            else if (std.mem.eql(u8, message.role, "system"))
                "developer"
            else
                message.role;
            try buf.appendSlice(allocator, "<start_of_turn>");
            try buf.appendSlice(allocator, role);
            try buf.append(allocator, '\n');
            try buf.appendSlice(allocator, message.content);
            try buf.appendSlice(allocator, "<end_of_turn>\n");
        }
        try buf.appendSlice(allocator, "<start_of_turn>model\n");
        return try buf.toOwnedSlice(allocator);
    }

    /// SSE streaming generation: sends token-by-token events via chunked transfer encoding.
    /// OpenAI-compatible format: data: {"choices":[{"delta":{"content":"token"}}]}\n\n
    fn streamGenerate(
        self: *Node,
        ctx: *httpx.Context,
        model_name: []const u8,
        pipeline: anytype,
        telemetry_cb: ?*const ops.ComputeBackend,
        messages: []const @import("../pipelines/generation.zig").Message,
        config: @import("../pipelines/generation.zig").GenerationConfig,
        tool_parser: ?*tool_parser_mod.Parser,
        include_usage: bool,
    ) !httpx.Response {
        const server_a4b_telemetry_before = serverA4bTelemetrySnapshot(telemetry_cb);
        defer logServerA4bTelemetry(telemetry_cb, server_a4b_telemetry_before, model_name);
        var writer = ctx.streamResponse(200) catch |err| {
            std.debug.print("streamResponse failed: {}\n", .{err});
            return ctx.status(500).json(.{ .@"error" = "STREAM_INIT_FAILED", .message = internalErrorMessage("STREAM_INIT_FAILED", err) });
        };
        const stream_id = try allocCompletionId(ctx.allocator);
        defer ctx.allocator.free(stream_id);
        const stream_created = completionCreatedTimestamp();

        // Context for the token callback — carries the writer and model name for building SSE events
        const StreamCtx = struct {
            writer: *httpx.Context.StreamWriter,
            stream_id: []const u8,
            stream_created: i64,
            model_name: []const u8,
            payload: *std.Io.Writer.Allocating,
            parser: ?*tool_parser_mod.Parser,
            request_context: *const httpx.Context,
            defer_content: bool,
            write_error: ?anyerror = null,
            parser_failed: bool = false,

            fn shouldContinue(raw_ctx: *anyopaque) bool {
                const stream: *@This() = @ptrCast(@alignCast(raw_ctx));
                return stream.write_error == null and
                    !stream.parser_failed and
                    !stream.request_context.isCancellationRequested();
            }

            fn onToken(raw_ctx: *anyopaque, token_text: []const u8) bool {
                const stream: *@This() = @ptrCast(@alignCast(raw_ctx));
                // Reasoning is projected only once generation completes. For
                // channel-aware thinking requests, retain protocol order by
                // emitting the buffered reasoning before the completed public
                // content below.
                if (stream.defer_content) return true;
                if (stream.parser) |parser| {
                    const update = parser.feed(token_text) catch {
                        stream.parser_failed = true;
                        return false;
                    };
                    if (update.ready_text.len > 0) {
                        emitContentDelta(stream.writer, stream.payload, stream.stream_id, stream.stream_created, stream.model_name, update.ready_text) catch |err| {
                            stream.write_error = err;
                            return false;
                        };
                    }
                    if (!parser.streamsIncrementalToolDeltas() and update.new_calls.len > 0) {
                        for (update.new_calls, 0..) |call, idx| {
                            emitToolCallDelta(stream.writer, stream.payload, stream.stream_id, stream.stream_created, stream.model_name, update.call_start_index + idx, call) catch |err| {
                                stream.write_error = err;
                                return false;
                            };
                        }
                    }
                    if (update.active_tool_delta) |delta| {
                        emitToolCallDeltaUpdate(stream.writer, stream.payload, stream.stream_id, stream.stream_created, stream.model_name, delta) catch |err| {
                            stream.write_error = err;
                            return false;
                        };
                    }
                    return true;
                }
                // Build OpenAI-compatible SSE chunk
                emitContentDelta(stream.writer, stream.payload, stream.stream_id, stream.stream_created, stream.model_name, token_text) catch |err| {
                    stream.write_error = err;
                    return false;
                };
                return true;
            }
        };

        var chunk_payload: std.Io.Writer.Allocating = .init(ctx.allocator);
        defer chunk_payload.deinit();
        var stream_ctx = StreamCtx{
            .writer = &writer,
            .stream_id = stream_id,
            .stream_created = stream_created,
            .model_name = model_name,
            .payload = &chunk_payload,
            .parser = tool_parser,
            .request_context = ctx,
            .defer_content = shouldDeferStreamContentForReasoning(pipeline, config),
        };

        if (comptime @hasField(@TypeOf(pipeline.*), "continue_ctx") and
            @hasField(@TypeOf(pipeline.*), "continue_fn"))
        {
            pipeline.continue_ctx = @ptrCast(&stream_ctx);
            pipeline.continue_fn = StreamCtx.shouldContinue;
        }

        emitRoleDelta(&writer, &chunk_payload, stream_id, stream_created, model_name) catch |err| {
            writeInternalStreamError(&writer, "STREAM_WRITE_FAILED", err);
            writer.close() catch {};
            return ctx.response.build();
        };

        // Snapshot CUDA op-profile counters before generation so the emitted
        // line below reflects this request alone. Mirrors the direct-generator
        // path so per-op profiling works against the warm server's SSE route
        // (the one the long-context benchmark harness uses). Only the native
        // pipeline exposes a backend session; ONNX/ORT pipeline instantiations
        // must remain valid in CUDA-enabled builds too.
        const cuda_profile_session: ?backends_mod.Session =
            if (comptime build_options.enable_cuda) generationPipelineSession(pipeline) else null;
        const cuda_profile_before: ?session_factory.CudaRuntimeStats =
            if (comptime build_options.enable_cuda) blk: {
                if (!session_factory.cudaOpProfileLoggingEnabled()) break :blk null;
                const sess = cuda_profile_session orelse break :blk null;
                break :blk session_factory.getCudaRuntimeStats(sess);
            } else null;

        var result = pipeline.generateStreaming(
            messages,
            config,
            @ptrCast(&stream_ctx),
            StreamCtx.onToken,
        ) catch |err| {
            if (comptime build_options.enable_cuda) {
                if (cuda_profile_session) |sess| session_factory.drainCudaProfile(sess);
            }
            // Try to send an error event before closing
            writeGenerationStreamError(&writer, err);
            writer.close() catch {};
            return ctx.response.build();
        };
        defer result.deinit();

        // Drain buffered per-op profile events (single host sync, off the hot
        // path) and emit the same lines the CLI prints.
        if (comptime build_options.enable_cuda) {
            if (cuda_profile_before) |before| {
                if (cuda_profile_session) |sess| {
                    session_factory.drainCudaProfile(sess);
                    if (session_factory.getCudaRuntimeStats(sess)) |after| {
                        const profile_delta = session_factory.cudaStatsDelta(after, before);
                        var cuda_profile_line_buf: [768]u8 = undefined;
                        std.log.info("{s}", .{session_factory.formatCudaDecodeProfileLine(&cuda_profile_line_buf, profile_delta)});
                        std.log.info("{s}", .{session_factory.formatCudaPrefillProfileLine(&cuda_profile_line_buf, profile_delta)});
                    }
                }
            }
        }

        if (stream_ctx.write_error) |err| {
            // Generation already stopped through shouldContinue. Classify only
            // transport/cancellation errors as routine peer abandonment; JSON
            // formatting, allocation, and other server faults remain errors.
            const peer_disconnect = generationStreamWriteIsPeerDisconnect(err);
            if (!builtin.is_test) {
                if (peer_disconnect)
                    std.log.info("generation stream closed after client disconnect", .{})
                else
                    std.log.err("inference request failed code=STREAM_WRITE_FAILED: {s}", .{@errorName(err)});
            }
            if (!peer_disconnect) writer.writeEvent("error", internal_error_message) catch {};
            writer.close() catch {};
            return ctx.response.build();
        }
        if (stream_ctx.parser_failed) {
            if (!builtin.is_test) std.log.err("inference request failed code=STREAM_PARSE_FAILED", .{});
            writer.writeEvent("error", internal_error_message) catch {};
            writer.close() catch {};
            return ctx.response.build();
        }

        const speculative = effectiveSpeculationStats(
            result.speculative,
            config.speculation_requested,
            config.speculation_policy,
            config.speculation_calibration,
        );
        recordSpeculationOutcome(&self.metrics, speculative);

        if (stream_ctx.defer_content) {
            writeStreamCompletion(
                &chunk_payload,
                &writer,
                stream_id,
                stream_created,
                model_name,
                result.text,
                result.reasoning_text,
                result.finish_reason,
                tool_parser,
                speculative,
            ) catch |err| {
                writeInternalStreamError(&writer, "STREAM_WRITE_FAILED", err);
                writer.close() catch {};
                return ctx.response.build();
            };
        } else {
            if (result.reasoning_text) |reasoning| {
                if (reasoning.len > 0) emitReasoningDelta(
                    &writer,
                    &chunk_payload,
                    stream_id,
                    stream_created,
                    model_name,
                    reasoning,
                ) catch |err| {
                    writeInternalStreamError(&writer, "STREAM_WRITE_FAILED", err);
                    writer.close() catch {};
                    return ctx.response.build();
                };
            }
            if (tool_parser != null) {
                flushStreamParserState(&chunk_payload, &writer, stream_id, stream_created, model_name, result.finish_reason, tool_parser.?, speculative) catch |err| {
                    writeInternalStreamError(&writer, "STREAM_WRITE_FAILED", err);
                    writer.close() catch {};
                    return ctx.response.build();
                };
            } else {
                emitFinishDelta(&writer, &chunk_payload, stream_id, stream_created, model_name, result.finish_reason, speculative) catch |err| {
                    writeInternalStreamError(&writer, "STREAM_WRITE_FAILED", err);
                    writer.close() catch {};
                    return ctx.response.build();
                };
            }
        }

        emitTerminalUsageOrDone(
            &writer,
            &chunk_payload,
            stream_id,
            stream_created,
            model_name,
            result.prompt_tokens,
            result.tokens_used,
            result.cached_prompt_tokens,
            include_usage,
        ) catch |err| {
            writeInternalStreamError(&writer, "STREAM_WRITE_FAILED", err);
            writer.close() catch {};
            return ctx.response.build();
        };
        writer.close() catch {};

        return ctx.response.build();
    }

    fn writeStreamCompletion(
        payload: *std.Io.Writer.Allocating,
        writer: *httpx.Context.StreamWriter,
        stream_id: []const u8,
        stream_created: i64,
        model_name: []const u8,
        full_text: []const u8,
        reasoning_text: ?[]const u8,
        default_finish_reason: []const u8,
        tool_parser: ?*tool_parser_mod.Parser,
        speculative: ?generation.SpeculativeDecodeStats,
    ) !void {
        if (reasoning_text) |reasoning| {
            if (reasoning.len > 0) try emitReasoningDelta(writer, payload, stream_id, stream_created, model_name, reasoning);
        }
        if (tool_parser) |parser| {
            parser.reset();
            _ = try parser.feed(full_text);
            const remaining = try parser.finishText(payload.allocator);
            defer payload.allocator.free(remaining);
            const calls = parser.toolCalls();
            if (remaining.len > 0) try emitContentDelta(writer, payload, stream_id, stream_created, model_name, remaining);
            if (calls.len > 0) {
                for (calls, 0..) |call, idx| try emitToolCallDeltaUpdate(writer, payload, stream_id, stream_created, model_name, .{
                    .index = idx,
                    .id = call.id,
                    .type = call.type,
                    .name = call.function.name,
                    .arguments = call.function.arguments,
                });
                try emitFinishDelta(writer, payload, stream_id, stream_created, model_name, "tool_calls", speculative);
                return;
            }
            if (remaining.len == 0 and full_text.len > 0) try emitContentDelta(writer, payload, stream_id, stream_created, model_name, full_text);
            try emitFinishDelta(writer, payload, stream_id, stream_created, model_name, default_finish_reason, speculative);
            return;
        }

        if (full_text.len > 0) try emitContentDelta(writer, payload, stream_id, stream_created, model_name, full_text);
        try emitFinishDelta(writer, payload, stream_id, stream_created, model_name, default_finish_reason, speculative);
    }

    fn flushStreamParserState(
        payload: *std.Io.Writer.Allocating,
        writer: *httpx.Context.StreamWriter,
        stream_id: []const u8,
        stream_created: i64,
        model_name: []const u8,
        default_finish_reason: []const u8,
        parser: *tool_parser_mod.Parser,
        speculative: ?generation.SpeculativeDecodeStats,
    ) !void {
        const remaining = try parser.finishRemainingText(payload.allocator);
        defer payload.allocator.free(remaining);
        if (remaining.len > 0) try emitContentDelta(writer, payload, stream_id, stream_created, model_name, remaining);
        const finish_reason = if (parser.toolCalls().len > 0) "tool_calls" else default_finish_reason;
        try emitFinishDelta(writer, payload, stream_id, stream_created, model_name, finish_reason, speculative);
    }

    fn parseFinishReason(s: []const u8) api.FinishReason {
        const map = std.StaticStringMap(api.FinishReason).initComptime(.{
            .{ "stop", .stop },
            .{ "length", .length },
            .{ "tool_calls", .tool_calls },
            .{ "content_filter", .content_filter },
            .{ "function_call", .function_call },
        });
        return map.get(s) orelse .stop;
    }

    fn writeGenerateChunkEvent(
        writer: anytype,
        payload: *std.Io.Writer.Allocating,
        chunk: api.GenerateChunk,
    ) !void {
        // Stream-owned buffer, reset per event: streaming emits one chunk per
        // token and per-token alloc/free churn is measurable overhead.
        payload.clearRetainingCapacity();
        std.json.Stringify.value(chunk, .{}, &payload.writer) catch return error.OutOfMemory;
        try writer.writeEvent(null, payload.written());
    }

    fn emitRoleDelta(
        writer: *httpx.Context.StreamWriter,
        payload: *std.Io.Writer.Allocating,
        stream_id: []const u8,
        stream_created: i64,
        model_name: []const u8,
    ) !void {
        const choices = [_]api.GenerateChunkChoice{.{
            .index = 0,
            .delta = .{ .role = .assistant },
        }};
        try writeGenerateChunkEvent(writer, payload, .{
            .id = stream_id,
            .object = "chat.completion.chunk",
            .created = stream_created,
            .model = model_name,
            .choices = &choices,
        });
    }

    fn emitContentDelta(
        writer: *httpx.Context.StreamWriter,
        payload: *std.Io.Writer.Allocating,
        stream_id: []const u8,
        stream_created: i64,
        model_name: []const u8,
        token_text: []const u8,
    ) !void {
        const choices = [_]api.GenerateChunkChoice{.{
            .index = 0,
            .delta = .{ .content = .{ .value = token_text } },
        }};
        try writeGenerateChunkEvent(writer, payload, .{
            .id = stream_id,
            .object = "chat.completion.chunk",
            .created = stream_created,
            .model = model_name,
            .choices = &choices,
        });
    }

    fn emitReasoningDelta(
        writer: *httpx.Context.StreamWriter,
        payload: *std.Io.Writer.Allocating,
        stream_id: []const u8,
        stream_created: i64,
        model_name: []const u8,
        reasoning_text: []const u8,
    ) !void {
        const choices = [_]api.GenerateChunkChoice{.{
            .index = 0,
            .delta = .{ .reasoning_content = .{ .value = reasoning_text } },
        }};
        try writeGenerateChunkEvent(writer, payload, .{
            .id = stream_id,
            .object = "chat.completion.chunk",
            .created = stream_created,
            .model = model_name,
            .choices = &choices,
        });
    }

    fn emitToolCallDeltaUpdate(
        writer: *httpx.Context.StreamWriter,
        payload: *std.Io.Writer.Allocating,
        stream_id: []const u8,
        stream_created: i64,
        model_name: []const u8,
        delta: tool_parser_mod.ToolCallDeltaUpdate,
    ) !void {
        const function_delta: ?api.ToolCallFunctionDelta = if (delta.name != null or delta.arguments != null)
            .{ .name = delta.name, .arguments = delta.arguments }
        else
            null;
        const tool_calls = [_]api.ToolCallDelta{.{
            .index = @intCast(delta.index),
            .id = delta.id,
            .type = delta.type,
            .function = function_delta,
        }};
        const choices = [_]api.GenerateChunkChoice{.{
            .index = 0,
            .delta = .{ .tool_calls = &tool_calls },
        }};
        try writeGenerateChunkEvent(writer, payload, .{
            .id = stream_id,
            .object = "chat.completion.chunk",
            .created = stream_created,
            .model = model_name,
            .choices = &choices,
        });
    }

    fn emitToolCallDelta(
        writer: *httpx.Context.StreamWriter,
        payload: *std.Io.Writer.Allocating,
        stream_id: []const u8,
        stream_created: i64,
        model_name: []const u8,
        index: usize,
        call: tool_parser_mod.ToolCall,
    ) !void {
        try emitToolCallDeltaUpdate(writer, payload, stream_id, stream_created, model_name, .{
            .index = index,
            .id = call.id,
            .type = call.type,
            .name = call.function.name,
            .arguments = call.function.arguments,
        });
    }

    fn emitFinishDelta(
        writer: anytype,
        payload: *std.Io.Writer.Allocating,
        stream_id: []const u8,
        stream_created: i64,
        model_name: []const u8,
        finish_reason: []const u8,
        speculative: ?generation.SpeculativeDecodeStats,
    ) !void {
        const choices = [_]api.GenerateChunkChoice{.{
            .index = 0,
            .delta = .{},
            .finish_reason = parseFinishReason(finish_reason),
        }};
        try writeGenerateChunkEvent(writer, payload, .{
            .id = stream_id,
            .object = "chat.completion.chunk",
            .created = stream_created,
            .model = model_name,
            .choices = &choices,
            .speculation = if (generateSpeculationStatus(speculative)) |status|
                .{ .value = status }
            else
                .absent,
        });
    }

    fn emitTerminalUsageAndDone(
        writer: anytype,
        payload: *std.Io.Writer.Allocating,
        stream_id: []const u8,
        stream_created: i64,
        model_name: []const u8,
        prompt_tokens: usize,
        completion_tokens: usize,
        cached_prompt_tokens: usize,
    ) !void {
        try writeGenerateChunkEvent(writer, payload, .{
            .id = stream_id,
            .object = "chat.completion.chunk",
            .created = stream_created,
            .model = model_name,
            .choices = &.{},
            .usage = .{ .value = tokenUsageWithCachedPrompt(prompt_tokens, completion_tokens, cached_prompt_tokens) },
        });
        try writer.writeEvent(null, "[DONE]");
    }

    fn emitTerminalUsageOrDone(
        writer: anytype,
        payload: *std.Io.Writer.Allocating,
        stream_id: []const u8,
        stream_created: i64,
        model_name: []const u8,
        prompt_tokens: usize,
        completion_tokens: usize,
        cached_prompt_tokens: usize,
        include_usage: bool,
    ) !void {
        if (include_usage) {
            return emitTerminalUsageAndDone(
                writer,
                payload,
                stream_id,
                stream_created,
                model_name,
                prompt_tokens,
                completion_tokens,
                cached_prompt_tokens,
            );
        }
        try writer.writeEvent(null, "[DONE]");
    }

    const EntityExtractionRequest = struct {
        model: []const u8,
        labels: ?[]const []const u8 = null,
        relation_labels: ?[]const []const u8 = null,
        resolver: ?extraction_api.ExtractionResolverOptions = null,
        threshold: ?f32 = null,
        flat_ner: ?bool = null,
        include_confidence: bool = false,
        include_spans: bool = false,
        input_ids: []const ?[]const u8,
    };

    fn extractEntitiesAndRelations(
        self: *Node,
        ctx: *httpx.Context,
        body: EntityExtractionRequest,
        texts: []const []const u8,
        want_relations: bool,
    ) !httpx.Response {
        const model_name: ?[]const u8 = if (body.model.len > 0) body.model else null;
        const model_path = self.resolveRequestModelPath(ctx.allocator, ctx.io, model_name, "extractors") catch |err|
            return requestModelResolutionError(ctx, err);
        defer ctx.allocator.free(model_path);

        if (rebel_mod.isRebelModel(ctx.allocator, model_path)) {
            rebel_mod.validateSchemaSupport(body.labels, body.relation_labels) catch |err|
                return rebelSchemaFailureResponse(ctx, err);
            return self.extractRebel(ctx, model_path, body, texts, want_relations);
        }

        var model_handle = self.model_manager.acquireFromDir(model_path) catch |err|
            return modelLoadFailureResponse(ctx, err);
        defer model_handle.release();
        const model = model_handle.get();
        validateTextEntityExtractionManifest(&model.manifest) catch |err| switch (err) {
            error.InvalidModelForExtraction => return ctx.status(400).json(.{
                .@"error" = "INVALID_MODEL",
                .message = "model does not support entity extraction",
            }),
            error.UnsupportedInput => return ctx.status(400).json(.{
                .@"error" = "INVALID_REQUEST",
                .message = "model does not support text extraction",
            }),
        };

        // Use GLiNER pipeline for GLiNER models, standard NER for BIO models
        if (model.isGlinerModel()) {
            return self.extractGliner(ctx, model, body, texts, want_relations);
        }

        if (want_relations) {
            return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = "model does not support relation extraction" });
        }

        var pipeline = model.nerPipeline(ctx.allocator);
        pipeline.config.threshold = body.threshold orelse pipeline.config.threshold;
        const all_entities = pipeline.recognizeBatch(texts) catch |err|
            return inferenceFailureResponse(ctx, err);
        defer freeBorrowedLabelEntityBatches(ctx.allocator, all_entities);

        const cleaned_entities = try applyLearnedCleanupIfPresent(ctx.allocator, try model.getCleanupHead(), texts, all_entities);
        defer if (cleaned_entities) |entities| freeEntityBatches(ctx.allocator, entities);
        const entities_for_response = cleaned_entities orelse all_entities;

        if (body.resolver) |resolver_cfg| {
            var resolved = try resolveExtractedOutput(ctx.allocator, entities_for_response, null, resolver_cfg);
            defer resolved.deinit(ctx.allocator);
            return self.buildEntityExtractionResponse(ctx, body, resolved.entities, resolved.relations, texts);
        }

        return self.buildEntityExtractionResponse(ctx, body, entities_for_response, null, texts);
    }

    fn extractRebel(
        self: *Node,
        ctx: *httpx.Context,
        model_path: []const u8,
        body: EntityExtractionRequest,
        texts: []const []const u8,
        want_relations: bool,
    ) !httpx.Response {
        var failure_stage: RebelExtractionFailureStage = .model_layout;
        const json = self.extractRebelJsonAlloc(
            ctx.allocator,
            model_path,
            body,
            texts,
            want_relations,
            &failure_stage,
        ) catch |err| {
            if (err == error.OutOfMemory) return err;
            return switch (failure_stage) {
                .model_layout => ctx.status(400).json(.{
                    .@"error" = "INVALID_MODEL",
                    .message = "model is not a supported encoder-decoder recognizer",
                }),
                .tokenizer => ctx.status(500).json(.{
                    .@"error" = "TOKENIZER_LOAD_FAILED",
                    .message = internalErrorMessage("TOKENIZER_LOAD_FAILED", err),
                }),
                .model_load => modelLoadFailureResponse(ctx, err),
                .inference => inferenceFailureResponse(ctx, err),
                .response => switch (err) {
                    error.InvalidExtractionResponse => ctx.status(500).json(.{
                        .@"error" = "INVALID_EXTRACTION_RESULT",
                        .message = "model returned an invalid entity or relation response",
                    }),
                    else => return err,
                },
            };
        };
        defer ctx.allocator.free(json);
        try ctx.setHeader("content-type", "application/json");
        _ = ctx.response.body(json);
        return ctx.response.build();
    }

    const RebelExtractionFailureStage = enum {
        model_layout,
        tokenizer,
        model_load,
        inference,
        response,
    };

    fn extractRebelJsonAlloc(
        self: *Node,
        allocator: std.mem.Allocator,
        model_path: []const u8,
        request: EntityExtractionRequest,
        texts: []const []const u8,
        want_relations: bool,
        failure_stage: *RebelExtractionFailureStage,
    ) ![]u8 {
        const enc_dec_mod = @import("../pipelines/encoder_decoder.zig");

        failure_stage.* = .model_layout;
        const paths = try enc_dec_mod.findEncoderDecoderPaths(allocator, model_path);
        defer allocator.free(paths.encoder);
        defer allocator.free(paths.decoder);

        failure_stage.* = .tokenizer;
        const tokenizer_path = try std.fmt.allocPrint(allocator, "{s}/tokenizer.json", .{model_path});
        defer allocator.free(tokenizer_path);
        const tokenizer_bytes = try c_file.readFile(allocator, tokenizer_path);
        defer allocator.free(tokenizer_bytes);
        var tokenizer = try hf_tokenizer_mod.HfTokenizer.loadFromBytes(allocator, tokenizer_bytes);
        defer tokenizer.deinitSelf();

        failure_stage.* = .model_load;
        var config = try rebel_mod.loadConfig(allocator, model_path);
        var config_owned = true;
        errdefer if (config_owned) config.deinit();
        const decoder_config = enc_dec_mod.loadDecoderConfig(allocator, model_path) catch enc_dec_mod.DecoderConfig{};
        if (decoder_config.max_length > 0) config.max_length = decoder_config.max_length;

        var component_loader = try self.model_manager.componentLoaderForPaths(
            model_path,
            self.session_manager.preferred_backends,
            &.{ paths.encoder, paths.decoder },
        );
        var encoder_managed = try component_loader.load(paths.encoder);
        defer encoder_managed.deinit();
        var strict_loader = try component_loader.restrictToBackend(encoder_managed.session.backend());
        var decoder_managed = try strict_loader.load(paths.decoder);
        defer decoder_managed.deinit();
        const encoder_session = encoder_managed.disownSession();
        const decoder_session = decoder_managed.disownSession();

        var pipeline = rebel_mod.RebelPipeline{
            .allocator = allocator,
            .enc_dec = .{
                .allocator = allocator,
                .encoder = encoder_session,
                .decoder = decoder_session,
                .config = decoder_config,
            },
            .tokenizer = tokenizer.tokenizer(),
            .config = config,
        };
        config_owned = false;
        defer pipeline.deinit();

        failure_stage.* = .inference;
        if (want_relations) {
            const relation_labels = request.relation_labels orelse return error.InvalidRelationSchema;
            const extracted = try pipeline.extractRelationsBatch(texts, request.labels, relation_labels);
            defer {
                rebel_mod.deinitEntityBatches(allocator, extracted.entities);
                for (extracted.relations) |relations| {
                    for (relations) |*relation| relation.deinit(allocator);
                    allocator.free(relations);
                }
                allocator.free(extracted.relations);
            }

            failure_stage.* = .response;
            if (request.resolver) |resolver_config| {
                var resolved = try resolveExtractedOutput(allocator, extracted.entities, extracted.relations, resolver_config);
                defer resolved.deinit(allocator);
                return entityExtractionResponseJsonAlloc(allocator, request, resolved.entities, resolved.relations, texts);
            }
            return entityExtractionResponseJsonAlloc(allocator, request, extracted.entities, extracted.relations, texts);
        }

        const all_entities = try pipeline.recognizeBatch(texts);
        defer rebel_mod.deinitEntityBatches(allocator, all_entities);
        failure_stage.* = .response;
        const cleaned_entities = try applyLearnedCleanupIfPresent(allocator, null, texts, all_entities);
        defer if (cleaned_entities) |entities| freeEntityBatches(allocator, entities);
        const entities_for_response = cleaned_entities orelse all_entities;

        if (request.resolver) |resolver_config| {
            var resolved = try resolveExtractedOutput(allocator, entities_for_response, null, resolver_config);
            defer resolved.deinit(allocator);
            return entityExtractionResponseJsonAlloc(allocator, request, resolved.entities, resolved.relations, texts);
        }
        return entityExtractionResponseJsonAlloc(allocator, request, entities_for_response, null, texts);
    }

    fn extractGliner(
        self: *Node,
        ctx: *httpx.Context,
        model: *model_manager_mod.LoadedModel,
        body: EntityExtractionRequest,
        texts: []const []const u8,
        want_relations: bool,
    ) !httpx.Response {
        var pipeline = model.glinerPipeline(ctx.allocator);
        pipeline.config.threshold = body.threshold orelse pipeline.config.threshold;
        pipeline.config.flat_ner = body.flat_ner orelse pipeline.config.flat_ner;

        // Parse labels from request body (or use defaults)
        const labels: ?[]const []const u8 = if (body.labels) |lbls| blk: {
            if (lbls.len > 0) {
                break :blk lbls;
            }
            break :blk null;
        } else null;

        if (want_relations) {
            if (!model.supportsRelationExtraction() or !pipeline.supportsRelationExtraction()) {
                return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = "model does not support relation extraction" });
            }

            const relation_labels = body.relation_labels.?;
            const relation_entity_labels = try relationEntityLabelsAlloc(
                ctx.allocator,
                labels,
                pipeline.config.default_labels,
                relation_labels,
            );
            defer if (relation_entity_labels) |values| ctx.allocator.free(values);
            const extracted = pipeline.extractRelationsBatch(texts, relation_entity_labels, relation_labels) catch |err|
                return inferenceFailureResponse(ctx, err);
            defer {
                freeBorrowedLabelEntityBatches(ctx.allocator, extracted.entities);

                for (extracted.relations) |relations| {
                    for (relations) |*relation| relation.deinit(ctx.allocator);
                    ctx.allocator.free(relations);
                }
                ctx.allocator.free(extracted.relations);
            }

            if (body.resolver) |resolver_cfg| {
                var resolved = try resolveExtractedOutput(ctx.allocator, extracted.entities, extracted.relations, resolver_cfg);
                defer resolved.deinit(ctx.allocator);
                return self.buildEntityExtractionResponse(ctx, body, resolved.entities, resolved.relations, texts);
            }

            return self.buildEntityExtractionResponse(ctx, body, extracted.entities, extracted.relations, texts);
        }

        const all_entities = pipeline.recognizeBatch(texts, labels) catch |err|
            return inferenceFailureResponse(ctx, err);
        defer freeBorrowedLabelEntityBatches(ctx.allocator, all_entities);

        const cleaned_entities = try applyLearnedCleanupIfPresent(ctx.allocator, try model.getCleanupHead(), texts, all_entities);
        defer if (cleaned_entities) |entities| freeEntityBatches(ctx.allocator, entities);
        const entities_for_response = cleaned_entities orelse all_entities;

        if (body.resolver) |resolver_cfg| {
            var resolved = try resolveExtractedOutput(ctx.allocator, entities_for_response, null, resolver_cfg);
            defer resolved.deinit(ctx.allocator);
            return self.buildEntityExtractionResponse(ctx, body, resolved.entities, resolved.relations, texts);
        }

        return self.buildEntityExtractionResponse(ctx, body, entities_for_response, null, texts);
    }

    fn buildEntityExtractionResponse(
        _: *Node,
        ctx: *httpx.Context,
        request: EntityExtractionRequest,
        all_entities: []const []const @import("../pipelines/ner.zig").Entity,
        all_relations: ?[]const []const gliner_mod.Relation,
        input_texts: []const []const u8,
    ) !httpx.Response {
        const json = entityExtractionResponseJsonAlloc(
            ctx.allocator,
            request,
            all_entities,
            all_relations,
            input_texts,
        ) catch |err| switch (err) {
            error.InvalidExtractionResponse => return ctx.status(500).json(.{
                .@"error" = "INVALID_EXTRACTION_RESULT",
                .message = "model returned an invalid entity or relation response",
            }),
            else => return err,
        };
        defer ctx.allocator.free(json);
        try ctx.setHeader("content-type", "application/json");
        _ = ctx.response.body(json);
        return ctx.response.build();
    }

    fn toExtractionApiEntity(entity: @import("../pipelines/ner.zig").Entity, request: EntityExtractionRequest) extraction_api.ExtractionEntity {
        return .{
            .text = entity.text,
            .label = entity.label,
            .start = if (request.include_spans) @intCast(entity.start) else null,
            .end = if (request.include_spans) @intCast(entity.end) else null,
            .score = if (request.include_confidence) entity.score else null,
        };
    }

    fn findEntityIndex(entities: []const @import("../pipelines/ner.zig").Entity, needle: @import("../pipelines/ner.zig").Entity) ?usize {
        for (entities, 0..) |entity, index| {
            if (entity.start == needle.start and entity.end == needle.end and
                std.mem.eql(u8, entity.label, needle.label) and std.mem.eql(u8, entity.text, needle.text)) return index;
        }
        return null;
    }

    pub fn classifyText(self: *Node, ctx: *httpx.Context) !httpx.Response {
        var parsed = (try ctx.parseJson(api.ClassifyRequest)) orelse
            return ctx.status(400).json(.{ .@"error" = "missing_body", .message = "Request body required" });
        defer parsed.deinit();
        const body = parsed.value;
        const admission_units = self.estimateHttpRequestAdmissionUnits(ctx);
        if (try self.acquireSlotUnits(ctx, admission_units)) |resp| return resp;
        defer self.releaseSlotUnits(admission_units);
        self.metrics.incRequest("classify");
        defer self.metrics.decActive();

        const model_name: ?[]const u8 = if (body.model.len > 0) body.model else null;
        if (self.resolveRequestModelPath(ctx.allocator, ctx.io, model_name, "classifiers")) |model_path| {
            defer ctx.allocator.free(model_path);
            if (try rejectDisallowedModel(self, ctx, model_path)) |response| return response;
            var model_handle = self.model_manager.acquireFromDir(model_path) catch |err|
                return modelLoadFailureResponse(ctx, err);
            defer model_handle.release();
            const model = model_handle.get();

            // Detect entailment index from id2label (varies by NLI model)
            const entailment_idx: ?usize = if (model.manifest.id2label) |labels| blk: {
                for (labels, 0..) |label, i| {
                    if (std.mem.eql(u8, label, "entailment") or std.mem.eql(u8, label, "ENTAILMENT")) {
                        break :blk i;
                    }
                }
                break :blk null;
            } else null;

            const config = @import("../pipelines/classification.zig").ClassificationConfig{
                .max_length = model.manifest.max_position_embeddings,
                .hypothesis_template = body.hypothesis_template orelse "This example is {}.",
                .multi_label = body.multi_label orelse false,
                .entailment_index = entailment_idx,
            };
            var pipeline = model.classificationPipeline(ctx.allocator, config);

            const all_results = pipeline.classifyBatch(body.texts, body.labels) catch |err|
                return inferenceFailureResponse(ctx, err);
            defer {
                for (all_results) |r| ctx.allocator.free(r);
                ctx.allocator.free(all_results);
            }

            const prompt_tokens =
                (countTokenizerTexts(ctx.allocator, self.session_manager.io, model.getTokenizer(), body.texts) catch estimateTextsTokens(body.texts)) +
                (countTokenizerTexts(ctx.allocator, self.session_manager.io, model.getTokenizer(), body.labels) catch estimateTextsTokens(body.labels));
            return buildClassificationResponse(ctx, body.model, all_results, prompt_tokens);
        } else |err| switch (requestModelResolutionErrorKind(err)) {
            .missing => {},
            .invalid, .internal => return requestModelResolutionError(ctx, err),
        }

        if (self.resolveRequestModelPath(ctx.allocator, ctx.io, model_name, "extractors")) |model_path| {
            defer ctx.allocator.free(model_path);
            if (try rejectDisallowedModel(self, ctx, model_path)) |response| return response;
            var model_handle = self.model_manager.acquireFromDir(model_path) catch |err|
                return modelLoadFailureResponse(ctx, err);
            defer model_handle.release();
            const model = model_handle.get();
            if (!model.isGlinerModel() or !model.supportsClassification()) {
                return ctx.status(404).json(.{ .@"error" = "MODEL_NOT_FOUND", .message = "model not found" });
            }

            var pipeline = model.glinerPipeline(ctx.allocator);
            const all_results = pipeline.classifyBatch(body.texts, body.labels, .{
                .threshold = 0.0,
                .multi_label = body.multi_label orelse false,
            }) catch |err| switch (err) {
                error.MissingSpecialTokenIds => return ctx.status(500).json(.{ .@"error" = "MODEL_CONFIG_INVALID", .message = @errorName(err) }),
                else => return inferenceFailureResponse(ctx, err),
            };
            defer {
                for (all_results) |r| ctx.allocator.free(r);
                ctx.allocator.free(all_results);
            }

            const prompt_tokens =
                (countTokenizerTexts(ctx.allocator, self.session_manager.io, model.getTokenizer(), body.texts) catch estimateTextsTokens(body.texts)) +
                (countTokenizerTexts(ctx.allocator, self.session_manager.io, model.getTokenizer(), body.labels) catch estimateTextsTokens(body.labels));
            return buildClassificationResponse(ctx, body.model, all_results, prompt_tokens);
        } else |err| switch (requestModelResolutionErrorKind(err)) {
            .missing => {},
            .invalid, .internal => return requestModelResolutionError(ctx, err),
        }

        return ctx.status(404).json(.{ .@"error" = "MODEL_NOT_FOUND", .message = "model not found" });
    }

    pub fn classifyDocument(self: *Node, ctx: *httpx.Context) !httpx.Response {
        var parsed = (try ctx.parseJson(api.DocumentClassificationRequest)) orelse
            return ctx.status(400).json(.{ .@"error" = "missing_body", .message = "Request body required" });
        defer parsed.deinit();
        const body = parsed.value;
        if (try self.acquireSlot(ctx)) |resp| return resp;
        defer self.releaseSlot();
        self.metrics.incRequest("classify");
        defer self.metrics.decActive();

        const model_name: ?[]const u8 = if (body.model.len > 0) body.model else null;
        const model_path = self.resolveRequestModelPath(ctx.allocator, ctx.io, model_name, "classifiers") catch |err|
            return requestModelResolutionError(ctx, err);
        defer ctx.allocator.free(model_path);

        const prefix = body.prefix orelse document_classification.default_prefix;
        const checkpoint_path = document_classification.resolveCheckpointPath(ctx.allocator, model_path) catch |err| switch (err) {
            error.CheckpointNotFound => return ctx.status(404).json(.{
                .@"error" = "CHECKPOINT_NOT_FOUND",
                .message = "layoutdoc_sequence_head.safetensors not found",
            }),
            else => return ctx.status(500).json(.{ .@"error" = "MODEL_LOAD_FAILED", .message = internalErrorMessage("MODEL_LOAD_FAILED", err) }),
        };
        defer ctx.allocator.free(checkpoint_path);

        var head = document_classification.Head.load(ctx.allocator, checkpoint_path, prefix) catch |err|
            return modelLoadFailureResponse(ctx, err);
        defer head.deinit();

        const num_tokens: usize = std.math.cast(usize, body.num_tokens) orelse
            return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = "num_tokens out of range" });

        const input = document_classification.ExampleInput{
            .image_path = body.image_path,
            .num_tokens = num_tokens,
        };

        const features = document_classification.extractFeatures(ctx.allocator, input) catch |err| switch (err) {
            error.FileNotFound => return ctx.status(404).json(.{ .@"error" = "IMAGE_NOT_FOUND", .message = "image not found" }),
            else => return inferenceFailureResponse(ctx, err),
        };

        const results = document_classification.classifyWithHead(ctx.allocator, &head, body.labels, input) catch |err| switch (err) {
            error.LabelCountMismatch => return ctx.status(400).json(.{
                .@"error" = "INVALID_REQUEST",
                .message = "label count does not match checkpoint output width",
            }),
            error.FileNotFound => return ctx.status(404).json(.{ .@"error" = "IMAGE_NOT_FOUND", .message = "image not found" }),
            else => return inferenceFailureResponse(ctx, err),
        };
        defer ctx.allocator.free(results);

        var input_obj: std.json.ObjectMap = .empty;
        defer input_obj.deinit(ctx.allocator);
        try input_obj.put(ctx.allocator, "image_path", .{ .string = body.image_path });
        try input_obj.put(ctx.allocator, "num_tokens", .{ .integer = @intCast(num_tokens) });

        var best_obj: std.json.ObjectMap = .empty;
        defer best_obj.deinit(ctx.allocator);
        const best_value: ?std.json.Value = if (results.len > 0) blk: {
            try best_obj.put(ctx.allocator, "label", .{ .string = results[0].label });
            try best_obj.put(ctx.allocator, "score", .{ .float = results[0].score });
            break :blk .{ .object = best_obj };
        } else null;

        const api_scores = try ctx.allocator.alloc(api.DocumentClassificationResult, results.len);
        defer ctx.allocator.free(api_scores);
        for (results, 0..) |result, idx| {
            api_scores[idx] = .{ .label = result.label, .score = result.score };
        }

        const data = [_]api.DocumentClassificationObject{.{
            .object = "document.classification",
            .index = 0,
            .checkpoint_path = checkpoint_path,
            .prefix = prefix,
            .input = .{ .object = input_obj },
            .features = .{
                .num_tokens = @intCast(features.num_tokens),
                .image_width = @intCast(features.image_width),
                .image_height = @intCast(features.image_height),
                .image_components = @intCast(features.image_components),
                .mean_darkness = features.mean_darkness,
                .std_darkness = features.std_darkness,
                .top_darkness = features.top_darkness,
                .bottom_darkness = features.bottom_darkness,
                .left_darkness = features.left_darkness,
                .right_darkness = features.right_darkness,
                .center_darkness = features.center_darkness,
            },
            .best = best_value,
            .scores = api_scores,
        }};

        return ctx.json(api.DocumentClassificationResponse{
            .object = "list",
            .data = &data,
            .model = body.model,
            .usage = tokenUsage(@intCast(num_tokens), 0),
        });
    }

    pub fn classifyDocumentTokens(self: *Node, ctx: *httpx.Context) !httpx.Response {
        var parsed = (try ctx.parseJson(api.DocumentTokenClassificationRequest)) orelse
            return ctx.status(400).json(.{ .@"error" = "missing_body", .message = "Request body required" });
        defer parsed.deinit();
        const body = parsed.value;
        if (try self.acquireSlot(ctx)) |resp| return resp;
        defer self.releaseSlot();
        self.metrics.incRequest("classify");
        defer self.metrics.decActive();

        const model_name: ?[]const u8 = if (body.model.len > 0) body.model else null;
        const model_path = self.resolveRequestModelPath(ctx.allocator, ctx.io, model_name, "classifiers") catch |err|
            return requestModelResolutionError(ctx, err);
        defer ctx.allocator.free(model_path);

        const prefix = body.prefix orelse document_token_classification.default_prefix;
        const checkpoint_path = document_token_classification.resolveCheckpointPath(ctx.allocator, model_path) catch |err| switch (err) {
            error.CheckpointNotFound => return ctx.status(404).json(.{
                .@"error" = "CHECKPOINT_NOT_FOUND",
                .message = "layoutdoc_token_head.safetensors not found",
            }),
            else => return ctx.status(500).json(.{ .@"error" = "MODEL_LOAD_FAILED", .message = internalErrorMessage("MODEL_LOAD_FAILED", err) }),
        };
        defer ctx.allocator.free(checkpoint_path);

        var head = document_token_classification.Head.load(ctx.allocator, checkpoint_path, prefix) catch |err|
            return modelLoadFailureResponse(ctx, err);
        defer head.deinit();

        const tokens = try ctx.allocator.alloc(document_token_classification.TokenBox, body.tokens.len);
        defer ctx.allocator.free(tokens);
        for (body.tokens, 0..) |tok, idx| {
            if (tok.bbox.len != 4) {
                return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = "each token bbox must contain 4 integers" });
            }
            tokens[idx] = .{
                .text = tok.text,
                .bbox = .{
                    @intCast(tok.bbox[0]),
                    @intCast(tok.bbox[1]),
                    @intCast(tok.bbox[2]),
                    @intCast(tok.bbox[3]),
                },
            };
        }

        const predictions = document_token_classification.classifyWithHead(ctx.allocator, &head, body.labels, tokens) catch |err| switch (err) {
            error.LabelCountMismatch => return ctx.status(400).json(.{
                .@"error" = "INVALID_REQUEST",
                .message = "label count does not match checkpoint output width",
            }),
            else => return inferenceFailureResponse(ctx, err),
        };
        defer {
            for (predictions) |pred| ctx.allocator.free(pred.scores);
            ctx.allocator.free(predictions);
        }

        const api_predictions = try ctx.allocator.alloc(api.DocumentTokenClassificationPrediction, predictions.len);
        defer ctx.allocator.free(api_predictions);

        var bbox_bufs = std.ArrayListUnmanaged([]i64).empty;
        defer {
            for (bbox_bufs.items) |b| ctx.allocator.free(b);
            bbox_bufs.deinit(ctx.allocator);
        }
        var feat_bbox_bufs = std.ArrayListUnmanaged([]i64).empty;
        defer {
            for (feat_bbox_bufs.items) |b| ctx.allocator.free(b);
            feat_bbox_bufs.deinit(ctx.allocator);
        }
        var score_bufs = std.ArrayListUnmanaged([]api.DocumentTokenClassificationResult).empty;
        defer {
            for (score_bufs.items) |s| ctx.allocator.free(s);
            score_bufs.deinit(ctx.allocator);
        }
        var best_objs = std.ArrayListUnmanaged(*std.json.ObjectMap).empty;
        defer {
            for (best_objs.items) |o| {
                o.deinit(ctx.allocator);
                ctx.allocator.destroy(o);
            }
            best_objs.deinit(ctx.allocator);
        }

        for (predictions, 0..) |pred, pred_idx| {
            const bbox_slice = try ctx.allocator.alloc(i64, pred.bbox.len);
            for (pred.bbox, 0..) |coord, ci| bbox_slice[ci] = coord;
            try bbox_bufs.append(ctx.allocator, bbox_slice);

            const feat_bbox_slice = try ctx.allocator.alloc(i64, pred.features.bbox.len);
            for (pred.features.bbox, 0..) |coord, ci| feat_bbox_slice[ci] = coord;
            try feat_bbox_bufs.append(ctx.allocator, feat_bbox_slice);

            const api_scores = try ctx.allocator.alloc(api.DocumentTokenClassificationResult, pred.scores.len);
            for (pred.scores, 0..) |score, si| {
                api_scores[si] = .{ .label = score.label, .score = score.score };
            }
            try score_bufs.append(ctx.allocator, api_scores);

            const best_value: ?std.json.Value = if (pred.best) |best| blk: {
                const obj = try ctx.allocator.create(std.json.ObjectMap);
                obj.* = .empty;
                try best_objs.append(ctx.allocator, obj);
                try obj.put(ctx.allocator, "label", .{ .string = best.label });
                try obj.put(ctx.allocator, "score", .{ .float = best.score });
                break :blk .{ .object = obj.* };
            } else null;

            api_predictions[pred_idx] = .{
                .token_index = @intCast(pred.token_index),
                .text = pred.text,
                .bbox = bbox_slice,
                .features = .{
                    .text_length = @intCast(pred.features.text_length),
                    .bbox = feat_bbox_slice,
                    .width = pred.features.width,
                    .height = pred.features.height,
                    .relative_position = pred.features.relative_position,
                    .bbox_phase_sin = pred.features.bbox_phase_sin,
                },
                .best = best_value,
                .scores = api_scores,
            };
        }

        const data = [_]api.DocumentTokenClassificationObject{.{
            .object = "document.token_classification",
            .index = 0,
            .checkpoint_path = checkpoint_path,
            .prefix = prefix,
            .num_tokens = @intCast(predictions.len),
            .predictions = api_predictions,
        }};

        return ctx.json(api.DocumentTokenClassificationResponse{
            .object = "list",
            .data = &data,
            .model = body.model,
            .usage = tokenUsage(body.tokens.len, 0),
        });
    }

    pub fn rewriteText(self: *Node, ctx: *httpx.Context) !httpx.Response {
        var parsed = (try ctx.parseJson(api.RewriteRequest)) orelse
            return ctx.status(400).json(.{ .@"error" = "missing_body", .message = "Request body required" });
        defer parsed.deinit();
        const body = parsed.value;
        if (try self.acquireSlot(ctx)) |resp| return resp;
        defer self.releaseSlot();
        self.metrics.incRequest("rewrite");
        defer self.metrics.decActive();

        // Resolve model
        const model_name: ?[]const u8 = if (body.model.len > 0) body.model else null;
        const model_path = self.resolveRequestModelPath(ctx.allocator, ctx.io, model_name, "rewriters") catch |err|
            return requestModelResolutionError(ctx, err);
        defer ctx.allocator.free(model_path);

        // Check if this is an encoder-decoder model and find ONNX file paths
        const enc_dec_mod = @import("../pipelines/encoder_decoder.zig");
        const paths = enc_dec_mod.findEncoderDecoderPaths(ctx.allocator, model_path) catch
            return ctx.status(400).json(.{
                .@"error" = "INVALID_MODEL",
                .message = "model does not support rewriting (missing encoder/decoder model files)",
            });
        defer ctx.allocator.free(paths.encoder);
        defer ctx.allocator.free(paths.decoder);

        var component_loader = self.model_manager.componentLoaderForPaths(
            model_path,
            self.session_manager.preferred_backends,
            &.{ paths.encoder, paths.decoder },
        ) catch |err| return modelLoadFailureResponse(ctx, err);
        var encoder_managed = component_loader.load(paths.encoder) catch |err|
            return modelLoadFailureResponse(ctx, err);
        defer encoder_managed.deinit();
        var strict_loader = component_loader.restrictToBackend(encoder_managed.session.backend()) catch |err|
            return modelLoadFailureResponse(ctx, err);
        var decoder_managed = strict_loader.load(paths.decoder) catch |err|
            return modelLoadFailureResponse(ctx, err);
        defer decoder_managed.deinit();
        const encoder_session = encoder_managed.session;
        const decoder_session = decoder_managed.session;

        // Parse decoder config
        const dec_config = enc_dec_mod.loadDecoderConfig(ctx.allocator, model_path) catch enc_dec_mod.DecoderConfig{};

        // Load tokenizer
        const hf_tokenizer = @import("inference_hf_tokenizer");
        const tok_path = std.fmt.allocPrint(ctx.allocator, "{s}/tokenizer.json", .{model_path}) catch |err|
            return ctx.status(500).json(.{ .@"error" = "TOKENIZER_LOAD_FAILED", .message = internalErrorMessage("TOKENIZER_LOAD_FAILED", err) });
        defer ctx.allocator.free(tok_path);

        const tok_bytes = c_file.readFile(ctx.allocator, tok_path) catch |err|
            return ctx.status(500).json(.{ .@"error" = "TOKENIZER_LOAD_FAILED", .message = internalErrorMessage("TOKENIZER_LOAD_FAILED", err) });
        defer ctx.allocator.free(tok_bytes);

        var hf_tok = hf_tokenizer.HfTokenizer.loadFromBytes(ctx.allocator, tok_bytes) catch |err|
            return ctx.status(500).json(.{ .@"error" = "TOKENIZER_LOAD_FAILED", .message = internalErrorMessage("TOKENIZER_LOAD_FAILED", err) });
        defer hf_tok.deinitSelf();

        const rewriting = @import("../pipelines/rewriting.zig");
        var pipeline = rewriting.RewritingPipeline{
            .allocator = ctx.allocator,
            .enc_dec = .{
                .allocator = ctx.allocator,
                .encoder = encoder_session,
                .decoder = decoder_session,
                .config = dec_config,
            },
            .tokenizer = hf_tok.tokenizer(),
            .config = .{
                .max_length = dec_config.max_length,
            },
        };

        const data = try ctx.allocator.alloc(api.RewriteObject, body.inputs.len);
        var filled: usize = 0;
        defer {
            for (data[0..filled]) |item| {
                for (item.texts) |s| ctx.allocator.free(s);
                ctx.allocator.free(item.texts);
            }
            ctx.allocator.free(data);
        }

        var completion_tokens: usize = 0;
        for (body.inputs, 0..) |input_text, i| {
            var result = pipeline.rewrite(input_text) catch |err|
                return inferenceFailureResponse(ctx, err);
            defer result.deinit();

            const inner = try ctx.allocator.alloc([]const u8, 1);
            errdefer ctx.allocator.free(inner);
            inner[0] = try ctx.allocator.dupe(u8, result.text);
            completion_tokens += countTokenizerTokens(ctx.allocator, self.session_manager.io, hf_tok.tokenizer(), result.text) catch estimateTextTokens(result.text);
            data[i] = .{
                .object = "rewrite",
                .index = @intCast(i),
                .texts = inner,
            };
            filled = i + 1;
        }

        const prompt_tokens = countTokenizerTexts(ctx.allocator, self.session_manager.io, hf_tok.tokenizer(), body.inputs) catch estimateTextsTokens(body.inputs);
        return ctx.json(api.RewriteResponse{
            .object = "list",
            .data = data,
            .model = body.model,
            .usage = tokenUsage(prompt_tokens, completion_tokens),
        });
    }

    pub fn readImages(self: *Node, ctx: *httpx.Context) !httpx.Response {
        var parsed = (try ctx.parseJson(api.ReadRequest)) orelse
            return ctx.status(400).json(.{ .@"error" = "missing_body", .message = "Request body required" });
        defer parsed.deinit();
        const body = parsed.value;
        if (body.images.len == 0) {
            return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = "'images' must not be empty" });
        }
        if (body.images.len > max_read_batch_images) {
            return ctx.status(413).json(.{
                .@"error" = "BATCH_TOO_LARGE",
                .message = try std.fmt.allocPrint(ctx.allocator, "'images' must contain at most {d} items", .{max_read_batch_images}),
            });
        }
        const max_tokens = validateReadMaxTokens(body.max_tokens) catch
            return ctx.status(400).json(.{
                .@"error" = "INVALID_REQUEST",
                .message = "'max_tokens' must be between 1 and 1024",
            });
        const inline_source_cap = readInlineSourceByteCap(self);
        var inline_source_bytes: usize = 0;
        for (body.images) |image| {
            inline_source_bytes = addReadInlineSourceBytes(inline_source_bytes, image.url, inline_source_cap) catch
                return ctx.status(413).json(.{
                    .@"error" = "BATCH_TOO_LARGE",
                    .message = "total inline image source bytes exceed server capacity",
                });
        }
        const admission = readRequestAdmission(self, body.images.len, inline_source_bytes, max_tokens);
        if (try self.acquireSlotUnits(ctx, admission.units)) |resp| return resp;
        var reserved_units = admission.units;
        defer self.releaseSlotUnits(reserved_units);
        self.metrics.incRequest("read");
        defer self.metrics.decActive();

        // Resolve model
        const model_name: ?[]const u8 = if (body.model.len > 0) body.model else null;
        const model_path = self.resolveRequestModelPath(ctx.allocator, ctx.io, model_name, "readers") catch |err|
            return requestModelResolutionError(ctx, err);
        defer ctx.allocator.free(model_path);

        var arena = std.heap.ArenaAllocator.init(ctx.allocator);
        defer arena.deinit();
        const alloc = arena.allocator();

        const results_out = try alloc.alloc(api.ReadObject, body.images.len);
        var filled: usize = 0;
        defer {
            for (0..filled) |i| {
                var tmp = results_out[i];
                if (tmp.fields) |*f| f.deinit(alloc);
            }
        }

        const downloaded = try ctx.allocator.alloc(scraping.DownloadedContent, body.images.len);
        var downloaded_count: usize = 0;
        defer {
            for (downloaded[0..downloaded_count]) |*item| item.deinit(ctx.allocator);
            ctx.allocator.free(downloaded);
        }
        const image_datas = try ctx.allocator.alloc([]const u8, body.images.len);
        defer ctx.allocator.free(image_datas);

        const batch_byte_cap = admission.byte_cap;
        var batch_bytes: usize = 0;
        var decoded_budget = ReadDecodedImageBudget.init(admission, effectiveRequestContentSecurity(self).max_image_dimension);
        for (body.images, 0..) |img_url, i| {
            var item = downloadReadBatchContentForRequest(self, ctx.allocator, img_url.url, batch_byte_cap, batch_bytes) catch |err| switch (err) {
                error.ReadBatchTooLarge => return ctx.status(413).json(.{
                    .@"error" = "BATCH_TOO_LARGE",
                    .message = try std.fmt.allocPrint(ctx.allocator, "total downloaded image bytes must be at most {d}", .{batch_byte_cap}),
                }),
                else => return remoteContentErrorResponse(ctx, err),
            };
            var item_owned = true;
            defer if (item_owned) item.deinit(ctx.allocator);
            batch_bytes = addReadBatchDownloadedBytes(batch_bytes, item, batch_byte_cap) catch |err| switch (err) {
                error.ReadBatchTooLarge => return ctx.status(413).json(.{
                    .@"error" = "BATCH_TOO_LARGE",
                    .message = try std.fmt.allocPrint(ctx.allocator, "total downloaded image bytes must be at most {d}", .{batch_byte_cap}),
                }),
            };
            decoded_budget.addImage(item.data) catch |err|
                return readImageErrorResponse(ctx, err);
            downloaded[i] = item;
            downloaded_count += 1;
            image_datas[i] = downloaded[i].data;
            item_owned = false;
        }

        const required_units = @max(admission.units, decoded_budget.requiredUnits());
        if (try self.growSlotUnits(ctx, reserved_units, required_units)) |resp| return resp;
        reserved_units = required_units;

        var reader = readers_mod.LoadedReader.loadFromDir(
            ctx.allocator,
            model_path,
            &self.session_manager,
            &self.model_manager,
        ) catch |err| switch (err) {
            error.InvalidModelForReading => return ctx.status(400).json(.{
                .@"error" = "INVALID_MODEL",
                .message = "model does not support reading (missing encoder/decoder model files)",
            }),
            error.NativePix2StructNotYetSupported => return ctx.status(400).json(.{
                .@"error" = "INVALID_MODEL",
                .message = "native Pix2Struct reader checkpoints are not supported yet",
            }),
            error.MultiStageReaderNotYetSupported => return ctx.status(400).json(.{
                .@"error" = "INVALID_MODEL",
                .message = "multi-stage OCR model uses unsupported stages or configuration",
            }),
            else => return ctx.status(500).json(.{ .@"error" = "MODEL_LOAD_FAILED", .message = internalErrorMessage("MODEL_LOAD_FAILED", err) }),
        };
        defer reader.deinit();

        const results = reader.readBatch(image_datas, .{
            .prompt = normalizeReadPrompt(body.prompt),
            .max_tokens = max_tokens,
        }) catch |err| switch (err) {
            error.ImageDecodeFailed => return readImageErrorResponse(ctx, err),
            error.InvalidMaxTokens => return ctx.status(400).json(.{
                .@"error" = "INVALID_REQUEST",
                .message = "'max_tokens' exceeds the selected model's context limit",
            }),
            else => return inferenceFailureResponse(ctx, err),
        };
        defer {
            for (results) |result| {
                var tmp = result;
                tmp.deinit();
            }
            ctx.allocator.free(results);
        }
        if (results.len != body.images.len)
            return internalErrorResponse(ctx, "INFERENCE_FAILED", error.InvalidReadResultCount);

        var completion_tokens: usize = 0;
        for (results, 0..) |result, i| {
            completion_tokens += estimateTextTokens(result.text);
            results_out[i] = try toApiReadObject(alloc, result, i);
            filled = i + 1;
        }

        const prompt_tokens = if (normalizeReadPrompt(body.prompt)) |prompt| estimateTextTokens(prompt) * body.images.len else 0;
        return ctx.json(api.ReadResponse{
            .object = "list",
            .data = results_out,
            .model = body.model,
            .usage = tokenUsage(prompt_tokens, completion_tokens),
        });
    }

    fn toApiReadObject(alloc: std.mem.Allocator, result: readers_mod.Result, index: usize) !api.ReadObject {
        const text_copy = try alloc.dupe(u8, result.text);

        var fields_map: ?std.json.ArrayHashMap([]const u8) = null;
        if (result.fields.len > 0) {
            var m: std.json.ArrayHashMap([]const u8) = .{};
            try m.map.ensureTotalCapacity(alloc, result.fields.len);
            for (result.fields) |field| {
                const name = try alloc.dupe(u8, field.name);
                const value = try alloc.dupe(u8, field.value);
                m.map.putAssumeCapacity(name, value);
            }
            fields_map = m;
        }

        var regions: ?[]const api.TextRegion = null;
        if (result.regions.len > 0) {
            const out = try alloc.alloc(api.TextRegion, result.regions.len);
            for (result.regions, 0..) |region, i| {
                const bbox = try alloc.alloc(f64, region.bbox.len);
                for (region.bbox, 0..) |c, j| bbox[j] = c;
                out[i] = .{
                    .text = try alloc.dupe(u8, region.text),
                    .bbox = bbox,
                    .confidence = if (region.confidence) |c| @floatCast(c) else null,
                    .label = if (region.label) |l| try alloc.dupe(u8, l) else null,
                };
            }
            regions = out;
        }

        return .{
            .text = text_copy,
            .fields = fields_map,
            .regions = regions,
            .object = "read",
            .index = @intCast(index),
        };
    }

    pub fn transcribeAudio(self: *Node, ctx: *httpx.Context) !httpx.Response {
        var parsed = (try ctx.parseJson(api.TranscribeRequest)) orelse
            return ctx.status(400).json(.{ .@"error" = "missing_body", .message = "Request body required" });
        defer parsed.deinit();
        const body = parsed.value;

        var media_shape: RequestMediaAdmissionShape = .{};
        media_shape.addInline(body.audio.len, false);
        const media_admission = requestMediaAdmission(self, media_shape);
        // The encoded JSON value and decoded compressed payload coexist with
        // PCM decode, so reserve both before allocating either decoded form.
        const resident_bytes = std.math.add(
            usize,
            media_admission.byte_cap,
            media_admission.byte_cap,
        ) catch std.math.maxInt(usize);
        const audio_admission = audioDecodeAdmission(self, resident_bytes);
        const admission_units = @max(self.estimateHttpRequestAdmissionUnits(ctx), audio_admission.units);
        if (try self.acquireSlotUnits(ctx, admission_units)) |resp| return resp;
        const reserved_units = admission_units;
        defer self.releaseSlotUnits(reserved_units);
        self.metrics.incRequest("transcribe");
        defer self.metrics.decActive();

        var media_budget = RequestMediaBudget.init(media_admission.byte_cap);
        const decoded_audio = decodeMediaDataWithBudget(ctx.allocator, body.audio, &media_budget) catch |err| switch (err) {
            error.RemoteContentTooLarge => return remoteContentErrorResponse(ctx, err),
            error.InvalidDataUri, error.InvalidBase64 => return ctx.status(400).json(.{
                .@"error" = "INVALID_REQUEST",
                .message = if (std.mem.startsWith(u8, body.audio, "data:")) "invalid audio data URI" else "invalid base64 audio data",
            }),
            error.OutOfMemory => return err,
        };
        defer decoded_audio.deinit(ctx.allocator);

        const decode_options = audio_mod.DecodeOptions{ .mime_hint = decoded_audio.mime_type };
        var decoded = audio_mod.decodeBounded(
            ctx.allocator,
            decoded_audio.data,
            decode_options,
            audio_admission.max_decode_working_bytes,
        ) catch |err| switch (err) {
            error.AudioTooLarge => return audioTooLargeResponse(ctx),
            error.OutOfMemory => return err,
            else => return unsupportedAudioResponse(ctx, "unsupported or corrupt audio input"),
        };
        defer decoded.deinit();

        // Resolve model
        const transcribe_model_name: ?[]const u8 = if (body.model) |m| (if (m.len > 0) m else null) else null;
        const model_path = self.resolveRequestModelPath(ctx.allocator, ctx.io, transcribe_model_name, "transcribers") catch |err|
            return requestModelResolutionError(ctx, err);
        defer ctx.allocator.free(model_path);

        // Find encoder/decoder sessions
        const enc_dec_mod = @import("../pipelines/encoder_decoder.zig");
        const tokenizer_mod = @import("inference_tokenizer");
        const whisper_prompt = @import("../pipelines/whisper_prompt.zig");
        var encoder_session: backends_mod.Session = undefined;
        var decoder_session: backends_mod.Session = undefined;
        var encoder_managed: ?model_manager_mod.ManagedSession = null;
        defer if (encoder_managed) |*managed| managed.deinit();
        var decoder_managed: ?model_manager_mod.ManagedSession = null;
        defer if (decoder_managed) |*managed| managed.deinit();
        var tokenizer: tokenizer_mod.Tokenizer = undefined;
        var decoder_config: enc_dec_mod.DecoderConfig = undefined;
        var loaded_model_handle: ?model_manager_mod.ModelHandle = null;
        defer if (loaded_model_handle) |*handle| handle.release();
        var whisper_assets_handle: ?model_manager_mod.WhisperAssetsHandle = null;
        defer if (whisper_assets_handle) |*handle| handle.release();
        var prompt_cache: ?*const whisper_prompt.PromptCache = null;

        if (enc_dec_mod.findEncoderDecoderPaths(ctx.allocator, model_path)) |paths| {
            defer ctx.allocator.free(paths.encoder);
            defer ctx.allocator.free(paths.decoder);

            whisper_assets_handle = self.model_manager.acquireWhisperCompositeAssets(
                model_path,
                &.{ paths.encoder, paths.decoder },
            ) catch |err| return modelLoadFailureResponse(ctx, err);
            var component_loader = self.model_manager.componentLoaderForPaths(
                model_path,
                self.session_manager.preferred_backends,
                &.{ paths.encoder, paths.decoder },
            ) catch |err| return modelLoadFailureResponse(ctx, err);
            encoder_managed = component_loader.load(paths.encoder) catch |err|
                return modelLoadFailureResponse(ctx, err);
            encoder_session = encoder_managed.?.session;
            var strict_loader = component_loader.restrictToBackend(encoder_session.backend()) catch |err|
                return modelLoadFailureResponse(ctx, err);
            decoder_managed = strict_loader.load(paths.decoder) catch |err|
                return modelLoadFailureResponse(ctx, err);
            decoder_session = decoder_managed.?.session;
            self.model_manager.validateWhisperAssetsCurrent(
                &whisper_assets_handle.?,
                model_path,
                &.{ paths.encoder, paths.decoder },
            ) catch |err| return modelLoadFailureResponse(ctx, err);
            const assets = whisper_assets_handle.?.get();
            tokenizer = assets.tokenizer();
            decoder_config = assets.decoder_config;
            prompt_cache = &assets.prompt_cache;
        } else |_| {
            loaded_model_handle = self.model_manager.acquireFromDir(model_path) catch |err|
                return modelLoadFailureResponse(ctx, err);
            const model = loaded_model_handle.?.get();
            const whisper_config = session_factory.getWhisperConfig(model.session) orelse {
                return ctx.status(400).json(.{
                    .@"error" = "INVALID_MODEL",
                    .message = "model does not support transcription",
                });
            };
            encoder_session = model.session;
            decoder_session = model.session;
            tokenizer = model.getTokenizer();
            prompt_cache = if (model.whisper_prompt_cache) |*cache| cache else null;
            decoder_config = .{
                .max_length = @intCast(whisper_config.max_target_positions),
                .decoder_start_token_id = whisper_config.decoder_start_token_id,
                .eos_token_id = whisper_config.eos_token_id,
                .pad_token_id = whisper_config.pad_token_id,
                .vocab_size = @intCast(whisper_config.vocab_size),
            };
        }

        const effective_prompt_cache = prompt_cache orelse
            return ctx.status(500).json(.{ .@"error" = "INVALID_MODEL", .message = "WhisperPromptCacheUnavailable" });
        var prompt_scratch: [3]whisper_prompt.ForcedDecoderId = undefined;
        const forced_ids = effective_prompt_cache.resolve(&prompt_scratch, body.language) catch
            return ctx.status(400).json(.{
                .@"error" = "INVALID_REQUEST",
                .message = "language is not supported by this Whisper model",
            });
        const transcription = @import("../pipelines/transcription.zig");
        var pipeline = transcription.TranscriptionPipeline.init(
            ctx.allocator,
            encoder_session,
            decoder_session,
            tokenizer,
            .{
                .max_length = decoder_config.max_length,
                .decoder_start_token_id = decoder_config.decoder_start_token_id,
                .eos_token_id = decoder_config.eos_token_id,
                .language = body.language,
                .forced_decoder_ids = forced_ids,
                .max_decode_working_bytes = audio_admission.max_decode_working_bytes,
                .language_tokens = effective_prompt_cache.language_tokens,
            },
        );

        var result = pipeline.transcribePcm(decoded.samples, decoded.sample_rate) catch |err| switch (err) {
            error.UnsupportedAudioFormat => return unsupportedAudioResponse(ctx, "unsupported audio input"),
            error.OutOfMemory => return err,
            else => return ctx.status(500).json(.{ .@"error" = "INFERENCE_FAILED", .message = internalErrorMessage("INFERENCE_FAILED", err) }),
        };
        defer result.deinit();

        const model_str = body.model orelse "default";
        const data = [_]api.TranscribeObject{.{
            .object = "transcription",
            .index = 0,
            .text = result.text,
            .language = result.language,
        }};
        return ctx.json(api.TranscribeResponse{
            .object = "list",
            .data = &data,
            .model = model_str,
            .usage = tokenUsage(0, countTokenizerTokens(ctx.allocator, self.session_manager.io, tokenizer, result.text) catch estimateTextTokens(result.text)),
        });
    }

    pub fn extractJSON(self: *Node, ctx: *httpx.Context) !httpx.Response {
        var parsed = (try ctx.parseJson(extraction_api.ExtractionRequest)) orelse
            return ctx.status(400).json(.{ .@"error" = "missing_body", .message = "Request body required" });
        defer parsed.deinit();
        const body = parsed.value;
        if (body.model.len == 0) {
            return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = "model is required" });
        }
        if (body.inputs.len == 0) {
            return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = "inputs are required" });
        }
        const has_relations = if (body.schema.relations) |relations| relations.len > 0 else false;
        const operation = canonicalExtractionOperation(body.schema) catch |err| switch (err) {
            error.MissingExtractionOperation => return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = "schema must request at least one extraction operation" }),
            error.MixedExtractionOperations => return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = "mixed extraction operations are not supported by this model runtime" }),
        };
        if (body.schema.entities) |labels| {
            for (labels) |label| if (!isCanonicalLabelSegment(label)) {
                return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = "entity labels must be non-empty and cannot contain '::'" });
            };
        }

        if (operation == .structures) {
            const inputs = try canonicalExtractingInputs(ctx.allocator, body.inputs);
            defer freeCanonicalExtractingInputs(ctx.allocator, inputs);
            const schema_json = try std.json.Stringify.valueAlloc(ctx.allocator, body.schema, .{});
            defer ctx.allocator.free(schema_json);
            const options_json = if (body.options) |options|
                try std.json.Stringify.valueAlloc(ctx.allocator, options, .{})
            else
                try ctx.allocator.dupe(u8, "{}");
            defer ctx.allocator.free(options_json);
            var response = self.extractWithAdmission(ctx.allocator, body.model, .{
                .inputs = inputs,
                .schema_json = schema_json,
                .options_json = options_json,
            }, .http_route) catch |err| return extractionDirectFailureResponse(ctx, err);
            defer response.deinit();
            try ctx.setHeader("content-type", "application/json");
            _ = ctx.response.body(response.json);
            return ctx.response.build();
        }

        if (operation == .classifications) {
            var classification_inputs = parseCanonicalTextInputs(ctx.allocator, body.inputs) catch
                return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = "classification extraction requires text inputs" });
            defer classification_inputs.deinit();
            return self.extractClassifications(ctx, body, classification_inputs.texts.items);
        }

        var direct_inputs = parseCanonicalTextInputs(ctx.allocator, body.inputs) catch
            return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = "entity and relation extraction requires text inputs" });
        defer direct_inputs.deinit();
        if (try self.acquireSlot(ctx)) |resp| return resp;
        defer self.releaseSlot();
        self.metrics.incRequest("extract");
        defer self.metrics.decActive();

        const relation_labels = canonicalRelationLabels(ctx.allocator, body.schema.relations orelse &.{}) catch
            return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = "invalid relation schema" });
        defer freeStringSlice(ctx.allocator, relation_labels);
        const options = body.options orelse extraction_api.ExtractionOptions{};
        const entity_request = EntityExtractionRequest{
            .model = body.model,
            .labels = body.schema.entities,
            .relation_labels = if (relation_labels.len > 0) relation_labels else null,
            .resolver = options.resolver,
            .threshold = options.threshold,
            .flat_ner = options.flat_ner,
            .include_confidence = options.include_confidence orelse false,
            .include_spans = options.include_spans orelse false,
            .input_ids = try canonicalInputIds(ctx.allocator, body.inputs),
        };
        defer ctx.allocator.free(entity_request.input_ids);
        return self.extractEntitiesAndRelations(ctx, entity_request, direct_inputs.texts.items, has_relations);
    }

    /// Resolve a public classification model by stable request ID before
    /// consulting legacy task directories. The model manifest, not its cache
    /// bucket, decides which classification pipeline runs.
    fn resolveClassificationRequestModelPath(
        self: *Node,
        allocator: std.mem.Allocator,
        io: std.Io,
        model_name: []const u8,
    ) ![]const u8 {
        const lookup_scopes = [_]?[]const u8{ null, "classifiers", "extractors" };
        for (lookup_scopes) |scope| {
            if (self.resolveRequestModelPath(allocator, io, model_name, scope)) |path| {
                return path;
            } else |err| switch (requestModelResolutionErrorKind(err)) {
                .missing => continue,
                .invalid, .internal => return err,
            }
        }
        return error.ModelNotFound;
    }

    fn extractClassifications(
        self: *Node,
        ctx: *httpx.Context,
        body: extraction_api.ExtractionRequest,
        texts: []const []const u8,
    ) !httpx.Response {
        if (try self.acquireSlot(ctx)) |resp| return resp;
        defer self.releaseSlot();
        self.metrics.incRequest("extract");
        defer self.metrics.decActive();

        const model_path = self.resolveClassificationRequestModelPath(ctx.allocator, ctx.io, body.model) catch |err|
            return requestModelResolutionError(ctx, err);
        defer ctx.allocator.free(model_path);
        if (try rejectDisallowedModel(self, ctx, model_path)) |response| return response;
        var model_handle = self.model_manager.acquireFromDir(model_path) catch |err|
            return modelLoadFailureResponse(ctx, err);
        defer model_handle.release();
        const model = model_handle.get();
        if (!model.supportsClassification()) {
            return ctx.status(400).json(.{ .@"error" = "INVALID_MODEL", .message = "model does not support classification extraction" });
        }

        var arena = std.heap.ArenaAllocator.init(ctx.allocator);
        defer arena.deinit();
        const alloc = arena.allocator();
        const lists = try alloc.alloc(std.ArrayListUnmanaged(extraction_api.ExtractionClassification), texts.len);
        for (lists) |*list| list.* = .empty;

        const schemas = body.schema.classifications orelse &.{};
        const options = body.options orelse extraction_api.ExtractionOptions{};
        var prompt_tokens: usize = 0;
        for (schemas) |schema| {
            if (schema.name.len == 0 or schema.labels.len == 0) {
                return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = "classification name and labels are required" });
            }
            if (schema.top_k) |top_k| {
                if (top_k < 1) {
                    return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = "classification top_k must be at least 1" });
                }
            }

            if (model.isGlinerModel()) {
                var pipeline = model.glinerPipeline(ctx.allocator);
                const multi_label = schema.multi_label orelse false;
                const results = pipeline.classifyBatch(texts, schema.labels, .{
                    .threshold = options.threshold orelse 0.0,
                    .multi_label = multi_label,
                    .top_k = if (multi_label) 0 else @intCast(schema.top_k orelse 1),
                }) catch |err| return inferenceFailureResponse(ctx, err);
                defer freeClassificationBatch(ctx.allocator, results);
                try appendExtractionClassificationBatch(alloc, lists, schema, options, results);
                const schema_prompt_tokens =
                    (countTokenizerTexts(ctx.allocator, self.session_manager.io, model.getTokenizer(), texts) catch estimateTextsTokens(texts)) +
                    (countTokenizerTexts(ctx.allocator, self.session_manager.io, model.getTokenizer(), schema.labels) catch estimateTextsTokens(schema.labels));
                prompt_tokens = std.math.add(usize, prompt_tokens, schema_prompt_tokens) catch
                    return inferenceFailureResponse(ctx, error.ResourceLimitExceeded);
                continue;
            }

            if (!manifestSupportsZeroShotClassification(&model.manifest)) {
                return ctx.status(400).json(.{
                    .@"error" = "INVALID_MODEL",
                    .message = "fixed-label classifiers do not accept request-defined labels",
                });
            }
            var pipeline = model.classificationPipeline(ctx.allocator, .{
                .max_length = model.manifest.max_position_embeddings,
                .hypothesis_template = schema.hypothesis_template orelse "This example is {}.",
                .multi_label = schema.multi_label orelse false,
                .entailment_index = nliEntailmentIndex(model.manifest.id2label),
            });
            var schema_prompt_tokens: usize = 0;
            const results = pipeline.classifyBatchWithPromptTokens(texts, schema.labels, &schema_prompt_tokens) catch |err|
                return inferenceFailureResponse(ctx, err);
            defer freeClassificationBatch(ctx.allocator, results);
            try appendExtractionClassificationBatch(alloc, lists, schema, options, results);
            prompt_tokens = std.math.add(usize, prompt_tokens, schema_prompt_tokens) catch
                return inferenceFailureResponse(ctx, error.ResourceLimitExceeded);
        }

        const data = try alloc.alloc(extraction_api.ExtractionObject, texts.len);
        for (data, 0..) |*item, index| item.* = .{
            .id = body.inputs[index].id,
            .classifications = lists[index].items,
        };

        var usage: std.json.ObjectMap = .empty;
        try usage.ensureTotalCapacity(alloc, 3);
        usage.putAssumeCapacity("prompt_tokens", .{ .integer = @intCast(prompt_tokens) });
        usage.putAssumeCapacity("completion_tokens", .{ .integer = 0 });
        usage.putAssumeCapacity("total_tokens", .{ .integer = @intCast(prompt_tokens) });
        return ctx.json(extraction_api.ExtractionResponse{
            .object = "extraction",
            .model = body.model,
            .data = data,
            .usage = .{ .map = usage },
        });
    }

    pub fn extract(self: *Node, ctx: *httpx.Context) !httpx.Response {
        return self.extractJSON(ctx);
    }

    fn downloadImagesForExtraction(
        self: *Node,
        ctx: *httpx.Context,
        images: []const api.ImageURL,
        batch_byte_cap: usize,
        decoded_budget: *ReadDecodedImageBudget,
    ) ![][]const u8 {
        const image_datas = try ctx.allocator.alloc([]const u8, images.len);
        var initialized: usize = 0;
        errdefer {
            for (image_datas[0..initialized]) |image_data| ctx.allocator.free(image_data);
            ctx.allocator.free(image_datas);
        }

        var batch_bytes: usize = 0;
        for (images, 0..) |img_url, i| {
            const downloaded = try downloadReadBatchContentForRequest(self, ctx.allocator, img_url.url, batch_byte_cap, batch_bytes);
            defer ctx.allocator.free(downloaded.content_type);
            errdefer ctx.allocator.free(downloaded.data);

            batch_bytes = addReadBatchDownloadedBytes(batch_bytes, downloaded, batch_byte_cap) catch
                return error.ReadBatchTooLarge;
            try decoded_budget.addImage(downloaded.data);
            image_datas[i] = downloaded.data;
            initialized += 1;
        }

        return image_datas;
    }

    pub fn listModels(self: *Node, ctx: *httpx.Context) !httpx.Response {
        const json_body = try self.listModelsJsonAlloc(ctx.allocator, ctx.io);
        defer ctx.allocator.free(json_body);
        try ctx.setHeader("content-type", "application/json");
        _ = ctx.response.body(json_body);
        return ctx.response.build();
    }

    /// Build the /ai/v1/models response body. Exposed so an embedding host
    /// (e.g. the antfly server's /connections endpoint) can list the embedded
    /// node's models without going through HTTP. Caller owns the result.
    pub fn listModelsJsonAlloc(self: *Node, a: std.mem.Allocator, io: std.Io) ![]u8 {
        var buf = std.ArrayListUnmanaged(u8).empty;
        defer buf.deinit(a);
        var body = std.ArrayListUnmanaged(u8).empty;
        defer body.deinit(a);
        var openai_data = std.ArrayListUnmanaged(u8).empty;
        defer openai_data.deinit(a);
        var openai_models = std.StringHashMapUnmanaged(void).empty;
        defer openai_models.deinit(a);
        var openai_data_count: usize = 0;
        const list_created = completionCreatedTimestamp();

        // Discover models from filesystem registry
        const ra = self.registry.allocator;
        const discovered = self.registry.discoverShallow(io) catch &[_]registry_mod.ModelEntry{};
        defer {
            for (discovered) |entry| {
                ra.free(entry.name);
                ra.free(entry.path);
            }
            if (discovered.len > 0) ra.free(discovered);
        }

        var discovered_listings = std.ArrayListUnmanaged(DiscoveredModelListing).empty;
        defer {
            for (discovered_listings.items) |*listing| listing.deinit();
            discovered_listings.deinit(a);
        }
        try discovered_listings.ensureTotalCapacity(a, discovered.len);
        for (discovered, 0..) |entry, entry_index| {
            var manifest = manifest_mod.loadFromDir(a, entry.path) catch continue;
            if (!model_manager_mod.isManifestPotentiallyLoadableInCurrentBuild(manifest)) {
                manifest.deinit();
                continue;
            }
            const model_kind = @tagName(manifest.model_type);
            const reader_candidate = taskMatchesModelListing(
                "readers",
                model_kind,
                manifest.gliner_model_type,
                manifest.tasks,
                manifest.capabilities,
                manifestSupportsZeroShotClassification(&manifest),
            );
            const compatibility_summary = self.compatibilitySummaryForDir(a, entry.path) catch CompatibilitySummary{
                .level = .unknown,
                .code = .artifact_unreadable,
                .message = "model compatibility could not be determined",
            };
            discovered_listings.appendAssumeCapacity(.{
                .entry_index = entry_index,
                .manifest = manifest,
                .reader_supported = reader_candidate and readers_mod.isSupportedModelDir(a, entry.path),
                .kind = model_kind,
                .compatibility_level = @tagName(compatibility_summary.level),
            });
        }

        const task_names = [_][]const u8{
            "embedders",  "rerankers", "chunkers", "generators",   "classifiers",
            "extractors", "rewriters", "readers",  "transcribers",
        };
        // Keep every snapshotted model alive while filesystem canonicalization
        // and manifest rendering run without the manager lock.
        var loaded_model_snapshot = std.ArrayListUnmanaged(model_manager_mod.ModelHandle).empty;
        defer {
            for (loaded_model_snapshot.items) |*handle| handle.release();
            loaded_model_snapshot.deinit(a);
        }
        self.model_manager.lockLoadedModels();
        loaded_model_snapshot.ensureTotalCapacity(a, self.model_manager.loaded.count()) catch |err| {
            self.model_manager.unlockLoadedModels();
            return err;
        };
        var loaded_it = self.model_manager.loaded.valueIterator();
        while (loaded_it.next()) |model| {
            model.*.active_handles += 1;
            loaded_model_snapshot.appendAssumeCapacity(.{
                .manager = &self.model_manager,
                .model = model.*,
            });
        }
        self.model_manager.unlockLoadedModels();

        const LoadedListing = struct {
            model: *model_manager_mod.LoadedModel,
            canonical_model_dir: []u8,
            identifier: []u8,
        };
        var loaded_listings = std.ArrayListUnmanaged(LoadedListing).empty;
        var selected_loaded_dirs = std.ArrayListUnmanaged([]const u8).empty;
        var canonical_discovered_dirs = std.ArrayListUnmanaged([]u8).empty;
        defer {
            for (loaded_listings.items) |listing| {
                a.free(listing.canonical_model_dir);
                a.free(listing.identifier);
            }
            for (canonical_discovered_dirs.items) |path| a.free(path);
            loaded_listings.deinit(a);
            selected_loaded_dirs.deinit(a);
            canonical_discovered_dirs.deinit(a);
        }

        for (discovered) |entry| {
            const canonical_path = realPathExistingAlloc(a, io, entry.path) catch |err| {
                if (err == error.OutOfMemory) return err;
                continue;
            };
            if (stringSliceContains(canonical_discovered_dirs.items, canonical_path)) {
                a.free(canonical_path);
                continue;
            }
            canonical_discovered_dirs.append(a, canonical_path) catch |err| {
                a.free(canonical_path);
                return err;
            };
        }

        const canonical_models_dir: ?[]u8 = realPathExistingAlloc(a, io, self.config.models_dir) catch |err| blk: {
            if (err == error.OutOfMemory) return err;
            break :blk null;
        };
        defer if (canonical_models_dir) |path| a.free(path);

        if (canonical_models_dir) |models_root| {
            for (loaded_model_snapshot.items) |*model_handle| {
                const model = model_handle.get();
                if (discoveredContainsModelDir(discovered, model.model_dir)) continue;

                const canonical_model_dir = realPathExistingAlloc(a, io, model.model_dir) catch |err| {
                    if (err == error.OutOfMemory) return err;
                    continue;
                };
                if (stringSliceContains(canonical_discovered_dirs.items, canonical_model_dir)) {
                    a.free(canonical_model_dir);
                    continue;
                }
                const identifier = loadedModelRequestIdentifierAlloc(
                    a,
                    io,
                    models_root,
                    canonical_model_dir,
                ) catch |err| {
                    a.free(canonical_model_dir);
                    return err;
                } orelse {
                    a.free(canonical_model_dir);
                    continue;
                };
                if (stringSliceContains(selected_loaded_dirs.items, canonical_model_dir)) {
                    a.free(identifier);
                    a.free(canonical_model_dir);
                    continue;
                }

                loaded_listings.append(a, .{
                    .model = model,
                    .canonical_model_dir = canonical_model_dir,
                    .identifier = identifier,
                }) catch |err| {
                    a.free(identifier);
                    a.free(canonical_model_dir);
                    return err;
                };
                selected_loaded_dirs.append(a, canonical_model_dir) catch |err| {
                    loaded_listings.items.len -= 1;
                    a.free(identifier);
                    a.free(canonical_model_dir);
                    return err;
                };
            }
        }

        for (task_names, 0..) |task, task_idx| {
            if (task_idx > 0) try body.append(a, ',');
            try body.append(a, '"');
            try body.appendSlice(a, task);
            try body.appendSlice(a, "\":{");

            var model_count: usize = 0;
            var listed_model_names = std.StringHashMapUnmanaged(void).empty;
            defer listed_model_names.deinit(a);

            // Add built-in chunkers
            if (std.mem.eql(u8, task, "chunkers")) {
                try body.appendSlice(a, "\"fixed_bert\":{\"inputs\":[\"text\"]},\"fixed_bpe\":{\"inputs\":[\"text\"]}");
                model_count += 2;
            }

            // Add discovered models matching this task
            for (discovered_listings.items) |*listing| {
                const entry = discovered[listing.entry_index];
                if (std.mem.eql(u8, task, "readers") and !listing.reader_supported) continue;

                const tasks = listing.manifest.tasks;
                const capabilities = listing.manifest.capabilities;
                const gliner_model_type = listing.manifest.gliner_model_type;
                const inputs = listing.manifest.inputs;
                const has_visual = listing.manifest.visual_model_path != null or listing.manifest.visual_projection_path != null;
                const has_audio = listing.manifest.audio_model_path != null or listing.manifest.audio_projection_path != null;
                if (!taskMatchesModelListing(
                    task,
                    listing.kind,
                    gliner_model_type,
                    tasks,
                    capabilities,
                    manifestSupportsZeroShotClassification(&listing.manifest),
                )) continue;
                const listed = try listed_model_names.getOrPut(a, entry.name);
                if (listed.found_existing) continue;

                if (model_count > 0) try body.append(a, ',');
                try jsonEncodeString(&body, a, entry.name);
                try body.append(a, ':');
                // Discovery does not parse chat templates (that is what made this handler
                // slow), so only models already loaded can report a template failure.
                const chat_template_failed =
                    self.model_manager.loadedChatTemplateFailed(entry.path) orelse false;
                try appendModelInfo(
                    &body,
                    a,
                    listing.kind,
                    gliner_model_type,
                    capabilities,
                    inputs,
                    manifestSupportsZeroShotClassification(&listing.manifest),
                    has_visual,
                    has_audio,
                    chat_template_failed,
                    listing.compatibility_level,
                );
                if (isOpenAiListTask(task)) {
                    const enabled = listing.compatibility_level.len > 0 and
                        (!std.mem.eql(u8, listing.compatibility_level, "unknown") or self.config.allow_unknown_models) and
                        !std.mem.eql(u8, listing.compatibility_level, "incompatible");
                    if (enabled) {
                        try appendUniqueOpenAiModelEntry(
                            &openai_data,
                            a,
                            &openai_models,
                            &openai_data_count,
                            entry.name,
                            list_created,
                            listing.compatibility_level,
                        );
                    }
                }
                model_count += 1;
            }

            // Add loaded models not currently visible to discovery. The
            // snapshot keeps them alive, and each listing has already been
            // canonicalized to a safe request identifier outside the lock.
            for (loaded_listings.items) |listing| {
                const model = listing.model;
                const model_task = @tagName(model.manifest.model_type);
                if (!taskMatchesModelListing(
                    task,
                    model_task,
                    model.manifest.gliner_model_type,
                    model.manifest.tasks,
                    model.manifest.capabilities,
                    manifestSupportsZeroShotClassification(&model.manifest),
                )) continue;
                const listed = try listed_model_names.getOrPut(a, listing.identifier);
                if (listed.found_existing) continue;

                if (model_count > 0) try body.append(a, ',');
                try jsonEncodeString(&body, a, listing.identifier);
                try body.append(a, ':');
                const loaded_compatibility = self.compatibilitySummaryForDir(a, listing.canonical_model_dir) catch CompatibilitySummary{
                    .level = .unknown,
                    .code = .artifact_unreadable,
                    .message = "model compatibility could not be determined",
                };
                try appendModelInfo(
                    &body,
                    a,
                    model_task,
                    model.manifest.gliner_model_type,
                    model.manifest.capabilities,
                    model.manifest.inputs,
                    manifestSupportsZeroShotClassification(&model.manifest),
                    model.manifest.visual_model_path != null or model.manifest.visual_projection_path != null,
                    model.manifest.audio_model_path != null or model.manifest.audio_projection_path != null,
                    model.chat_template_failed,
                    @tagName(loaded_compatibility.level),
                );
                if (isOpenAiListTask(task)) {
                    const enabled = loaded_compatibility.level == .compatible or
                        (loaded_compatibility.level == .unknown and self.config.allow_unknown_models);
                    if (enabled) {
                        try appendUniqueOpenAiModelEntry(
                            &openai_data,
                            a,
                            &openai_models,
                            &openai_data_count,
                            listing.identifier,
                            list_created,
                            @tagName(loaded_compatibility.level),
                        );
                    }
                }
                model_count += 1;
            }

            try body.append(a, '}');
        }
        try buf.appendSlice(a, "{\"object\":\"list\",\"data\":[");
        try buf.appendSlice(a, openai_data.items);
        try buf.appendSlice(a, "],");
        try buf.appendSlice(a, body.items);
        try buf.append(a, '}');

        return try buf.toOwnedSlice(a);
    }

    pub fn listPredictors(self: *Node, ctx: *httpx.Context) !httpx.Response {
        try self.discoverPredictors(ctx);
        const infos = self.tabular_registry.list(ctx.allocator) catch
            return ctx.status(500).json(.{ .@"error" = "INTERNAL_ERROR", .message = "failed to list predictors" });
        defer ctx.allocator.free(infos);

        var predictors: std.json.ArrayHashMap(api.PredictorInfo) = .{};
        for (infos) |info| {
            try predictors.map.put(ctx.allocator, info.name, .{
                .task = predictorTaskFromTabular(info.task),
                .num_features = @intCast(info.num_features),
                .num_outputs = @intCast(info.num_outputs),
                .feature_names = if (info.feature_names.len > 0) info.feature_names else null,
                .source_framework = if (info.source_framework.len > 0) info.source_framework else null,
            });
        }

        return ctx.json(api.PredictorsResponse{
            .object = "list",
            .predictors = predictors,
        });
    }

    pub fn predict(self: *Node, ctx: *httpx.Context) !httpx.Response {
        var parsed = (try ctx.parseJson(api.PredictRequest)) orelse
            return ctx.status(400).json(.{ .@"error" = "missing_body", .message = "Request body required" });
        defer parsed.deinit();
        const body = parsed.value;
        const admission_units = self.estimateHttpRequestAdmissionUnits(ctx);
        if (try self.acquireSlotUnits(ctx, admission_units)) |resp| return resp;
        defer self.releaseSlotUnits(admission_units);
        self.metrics.incRequest("predict");
        defer self.metrics.decActive();

        try self.discoverPredictors(ctx);
        const result = tabular_mod.http.predict(ctx.io, ctx.allocator, &self.tabular_registry, .{
            .model = body.model,
            .input = body.input,
        }) catch |err| switch (err) {
            error.ModelNotFound => return ctx.status(404).json(.{ .@"error" = "MODEL_NOT_FOUND", .message = "predictor not found" }),
            error.BatchTooLarge => return ctx.status(413).json(.{ .@"error" = "BATCH_TOO_LARGE", .message = "batch too large" }),
            error.FeatureMismatch => return ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = "feature vector length mismatch" }),
            error.LoadFailed => return ctx.status(500).json(.{ .@"error" = "MODEL_LOAD_FAILED", .message = "failed to load predictor" }),
            else => return ctx.status(500).json(.{ .@"error" = "INTERNAL_ERROR", .message = internalErrorMessage("INTERNAL_ERROR", err) }),
        };

        return ctx.json(api.PredictResponse{
            .model = result.model,
            .task = predictorTaskFromTabular(result.task),
            .predictions = result.predictions,
        });
    }

    fn discoverPredictors(self: *Node, ctx: *httpx.Context) !void {
        _ = tabular_mod.discovery.seedBuiltins(ctx.io, self.config.ml_dir) catch {};
        _ = tabular_mod.discovery.discover(ctx.io, ctx.allocator, &self.tabular_registry, self.config.ml_dir) catch {};
    }

    pub fn getVersion(_: *Node, ctx: *httpx.Context) !httpx.Response {
        return ctx.json(.{
            .version = build_options.inference_version,
            .git_commit = build_options.git_commit,
            .build_time = build_options.build_time,
            .go_version = build_options.go_version,
            .allow_downloads = build_options.allow_downloads,
            .runtime = "antfly-inference",
            .backends = .{
                .native = build_options.enable_native,
                .onnx = true,
                .onnx_runtime = build_options.enable_onnx,
                .wasm = build_options.enable_wasm,
            },
        });
    }

    /// Register inference API routes on an external server with a compile-time prefix.
    /// Used by standalone mode to register on the unified httpx.Server.
    pub fn registerRoutesOn(self: *Node, comptime prefix: []const u8, server: anytype) !void {
        try ensureKernelJitRequestSurfacesPublishable(
            self.config.kernel_jit.mode,
            self.config.kernel_jit.qualified_profile_path != null,
            self.startup_preloads_materialized,
        );
        self.request_surfaces_published = true;
        const router = api.ServerRouter(Node).init(self);
        var prefixed = PrefixedServer(prefix, @TypeOf(server.*)){
            .inner = server,
            .node = self,
            .models_handler = if (comptime std.mem.eql(u8, prefix, public_api_prefix))
                httpx.Handler.bind(self, mlModelsHandler)
            else
                null,
        };
        try router.register(&prefixed);
        try server.get(prefix ++ "/metrics", httpx.Handler.bind(self, metricsHandler));
    }

    /// Register AI routes without the Traditional ML predictor endpoints.
    pub fn registerAiRoutesOn(self: *Node, comptime prefix: []const u8, server: anytype) !void {
        try ensureKernelJitRequestSurfacesPublishable(
            self.config.kernel_jit.mode,
            self.config.kernel_jit.qualified_profile_path != null,
            self.startup_preloads_materialized,
        );
        self.request_surfaces_published = true;
        const router = api.ServerRouter(Node).init(self);
        var prefixed = AiPrefixedServer(prefix, @TypeOf(server.*)){ .inner = server, .node = self };
        try router.register(&prefixed);
        // Keep the retired endpoint deterministic when inference routes share
        // the standalone public server. Without an explicit tombstone, httpx
        // reports 405 because nearby generated routes match the path shape,
        // while the dedicated inference server reports 404.
        try server.post(prefix ++ "/recognize", httpx.Handler.from(retiredRecognizeHandler));
    }

    fn retiredRecognizeHandler(ctx: *httpx.Context) anyerror!httpx.Response {
        return ctx.status(404).json(.{
            .@"error" = "NOT_FOUND",
            .message = "resource not found",
        });
    }

    fn registerRootOperationalRoutes(self: *Node, server: anytype) !void {
        try server.get("/healthz", healthzHandler);
        try server.get("/readyz", httpx.Handler.bind(self, readyzHandler));
    }

    /// Registers every inference route on a caller-owned httpx server.
    pub fn registerHttpRoutes(self: *Node, server: anytype) !void {
        try self.registerRoutesOn(public_api_prefix, server);
        try self.registerAiRoutesOn(ai_api_prefix, server);
        try self.registerRootOperationalRoutes(server);
    }

    pub fn validateHttpBind(self: *const Node, host: []const u8) !void {
        try validateStandaloneBind(host, self.config.allow_insecure_public_bind);
    }

    /// Returns the single transport policy used by standalone and embedded
    /// inference listeners. Keeping this next to NodeConfig prevents the two
    /// launch paths from silently acquiring different executor bounds.
    pub fn httpServerConfig(self: *const Node, host: []const u8, port: u16) httpx.ServerConfig {
        return .{
            .host = host,
            .port = port,
            .max_connections = self.config.http_max_connections,
            .max_request_tasks = self.config.http_max_request_tasks,
            // Generation can legitimately take longer than the generic 30s HTTP
            // default during cold model startup or first-token GPU execution.
            .header_read_timeout_ms = 300_000,
            .body_read_timeout_ms = 300_000,
            .response_write_timeout_ms = 300_000,
            .keep_alive_timeout_ms = 300_000,
        };
    }

    pub fn serve(self: *Node, allocator: std.mem.Allocator, io: std.Io, host: []const u8, port: u16) !void {
        try self.model_manager.ensureResourceOwnerReady();
        validateStandaloneBind(host, self.config.allow_insecure_public_bind) catch |err| {
            std.log.err(
                "refusing non-loopback inference bind {s}:{d}: standalone inference has no built-in auth or TLS; pass --allow-insecure-public-bind only behind trusted network controls",
                .{ host, port },
            );
            return err;
        };
        self.attachIo(io);
        self.startReadinessInventory(io);
        var server = httpx.Server.initWithConfig(allocator, io, self.httpServerConfig(host, port));
        defer server.deinit();

        try self.registerHttpRoutes(&server);

        try server.listen();
    }

    fn mlModelsHandler(self: *Node, ctx: *httpx.Context) anyerror!httpx.Response {
        return self.listPredictors(ctx);
    }

    fn metricsHandler(self: *Node, ctx: *httpx.Context) anyerror!httpx.Response {
        var writer: std.Io.Writer.Allocating = .init(ctx.allocator);
        defer writer.deinit();

        // Pin model lifetimes while load_lock is held, then read lazy-session
        // counters only after releasing it. Optional-session loading takes the
        // inverse embedding-session -> load-lock path under admission pressure.
        var loaded_snapshot = try self.model_manager.acquireLoadedModelSnapshot(ctx.allocator);
        defer loaded_snapshot.deinit();

        // Core metrics via prometheus lib
        try @constCast(&self.metrics).render(&writer.writer);

        // Scheduler metrics (computed on-the-fly from loaded models)
        {
            self.model_manager.lockLoadedModels();
            defer self.model_manager.unlockLoadedModels();
            const aggregate = runtime.scheduler.native_generate.aggregateStats(self.model_manager.loaded);
            try appendPromMetric(&writer.writer, "antfly_inference_native_scheduler_waiting_requests", "gauge", "Waiting native scheduler requests across loaded models", aggregate.snapshot.waiting_requests);
            try appendPromMetric(&writer.writer, "antfly_inference_native_scheduler_prefill_requests", "gauge", "Prefill-phase native scheduler requests across loaded models", aggregate.snapshot.prefill_requests);
            try appendPromMetric(&writer.writer, "antfly_inference_native_scheduler_decode_requests", "gauge", "Decode-phase native scheduler requests across loaded models", aggregate.snapshot.decode_requests);
            try appendPromMetric(&writer.writer, "antfly_inference_native_scheduler_active_units", "gauge", "Active native scheduler units across loaded models", aggregate.snapshot.active_units);
            try appendPromMetric(&writer.writer, "antfly_inference_native_scheduler_step_batches_total", "counter", "Total unified scheduler steps (one fused forward pass per step)", aggregate.stats.step_batches_total);
            try appendPromMetric(&writer.writer, "antfly_inference_native_scheduler_step_prefill_items_total", "counter", "Total prefill items packed into unified scheduler steps", aggregate.stats.step_prefill_items_total);
            try appendPromMetric(&writer.writer, "antfly_inference_native_scheduler_step_decode_items_total", "counter", "Total decode items packed into unified scheduler steps", aggregate.stats.step_decode_items_total);
            try appendPromMetric(&writer.writer, "antfly_inference_native_scheduler_step_query_tokens_total", "counter", "Total query tokens fused across unified scheduler steps", aggregate.stats.step_query_tokens_total);
            try appendPromMetric(&writer.writer, "antfly_inference_native_scheduler_step_singleton_batches_total", "counter", "Total unified scheduler steps that contained only the leader item", aggregate.stats.step_singleton_batches_total);
            try appendPromMetric(&writer.writer, "antfly_inference_native_scheduler_step_kv_block_skips_total", "counter", "Total pending items skipped due to per-step KV-block budget", aggregate.stats.step_kv_block_skips_total);
            try appendPromMetric(&writer.writer, "antfly_inference_native_scheduler_decode_coalesce_waits_total", "counter", "Decode steps intentionally delayed to form a batch", aggregate.stats.decode_coalesce_waits_total);
            try appendPromMetric(&writer.writer, "antfly_inference_native_scheduler_decode_coalesce_wait_us_total", "counter", "Configured microseconds spent waiting to coalesce decode work", aggregate.stats.decode_coalesce_wait_us_total);
            try appendPromMetric(&writer.writer, "antfly_inference_native_scheduler_step_batch_size_2_total", "counter", "Unified scheduler steps containing two items", aggregate.stats.step_batch_size_2_total);
            try appendPromMetric(&writer.writer, "antfly_inference_native_scheduler_step_batch_size_3_4_total", "counter", "Unified scheduler steps containing three or four items", aggregate.stats.step_batch_size_3_4_total);
            try appendPromMetric(&writer.writer, "antfly_inference_native_scheduler_step_batch_size_5_8_total", "counter", "Unified scheduler steps containing five through eight items", aggregate.stats.step_batch_size_5_8_total);
            try appendPromMetric(&writer.writer, "antfly_inference_native_scheduler_step_batch_size_9_16_total", "counter", "Unified scheduler steps containing nine through sixteen items", aggregate.stats.step_batch_size_9_16_total);
            try appendPromMetric(&writer.writer, "antfly_inference_native_scheduler_turn_yields_total", "counter", "Total cooperative scheduler yields while waiting for turns", aggregate.stats.turn_yields_total);
            try appendResidentProjectionMetrics(&writer.writer, aggregateResidentProjectionStats(self.model_manager.loaded));
            try appendGraphExecutorMetrics(&writer.writer, graph_mod.executor_stats.snapshot());
            try appendPromptCacheMetrics(&writer.writer, self.model_manager.loaded);
        }
        try appendMetalExactJitMetrics(&writer.writer, aggregateMetalExactJitStats(loaded_snapshot.handles));

        try ctx.setHeader("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
        return ctx.text(writer.writer.buffered());
    }

    fn healthzHandler(ctx: *httpx.Context) anyerror!httpx.Response {
        return ctx.json(.{ .status = "ok" });
    }

    fn readyzHandler(self: *Node, ctx: *httpx.Context) anyerror!httpx.Response {
        const snapshot = self.readiness_inventory.load();
        const required_backend_ready = blk: {
            self.session_manager.validateRequiredBackendPolicy() catch break :blk false;
            break :blk true;
        };
        const is_ready = required_backend_ready and snapshot.initialized and snapshot.counts.total() > 0;
        const status_text = if (is_ready) "ready" else "not_ready";
        const status_code: u16 = if (is_ready) 200 else 503;
        return ctx.status(status_code).json(.{
            .status = status_text,
            .models = snapshot.counts,
        });
    }
};

fn nliEntailmentIndex(id2label: ?[]const []const u8) ?usize {
    const labels = id2label orelse return null;
    for (labels, 0..) |label, index| {
        if (std.ascii.eqlIgnoreCase(label, "entailment")) return index;
    }
    return null;
}

fn manifestSupportsZeroShotClassification(manifest: *const manifest_mod.ModelManifest) bool {
    if (model_caps.hasCapability(manifest.capabilities, "zero_shot")) return true;
    if (std.mem.eql(u8, manifest.gliner_model_type, "gliner2")) return true;
    return manifest.model_type == .classifier and nliEntailmentIndex(manifest.id2label) != null;
}

fn freeClassificationBatch(allocator: std.mem.Allocator, results: anytype) void {
    for (results) |items| allocator.free(items);
    allocator.free(results);
}

fn appendExtractionClassificationBatch(
    allocator: std.mem.Allocator,
    lists: []std.ArrayListUnmanaged(extraction_api.ExtractionClassification),
    schema: extraction_api.ExtractionClassificationSchema,
    options: extraction_api.ExtractionOptions,
    results: anytype,
) !void {
    if (results.len != lists.len) return error.InvalidExtractionResponse;
    const multi_label = schema.multi_label orelse false;
    const top_k = std.math.cast(usize, schema.top_k orelse 1) orelse return error.InvalidTopK;
    const threshold = options.threshold orelse 0.0;

    for (results, 0..) |items, input_index| {
        var appended: usize = 0;
        for (items) |item| {
            if (multi_label and item.score < threshold) continue;
            if (!multi_label and appended >= top_k) break;
            try lists[input_index].append(allocator, .{
                .name = schema.name,
                .label = item.label,
                .score = if (options.include_confidence orelse false) item.score else null,
            });
            appended += 1;
        }
    }
}

const CanonicalExtractionOperation = enum {
    entities_relations,
    classifications,
    structures,
};

fn canonicalExtractionOperation(schema: extraction_api.ExtractionSchema) !CanonicalExtractionOperation {
    const has_entities = if (schema.entities) |entities| entities.len > 0 else false;
    const has_relations = if (schema.relations) |relations| relations.len > 0 else false;
    const has_classifications = if (schema.classifications) |classifications| classifications.len > 0 else false;
    const has_structures = if (schema.structures) |structures| structures.map.count() > 0 else false;
    const operation_count: usize = @as(usize, @intFromBool(has_entities or has_relations)) +
        @as(usize, @intFromBool(has_classifications)) + @as(usize, @intFromBool(has_structures));
    if (operation_count == 0) return error.MissingExtractionOperation;
    if (operation_count > 1) return error.MixedExtractionOperations;
    if (has_classifications) return .classifications;
    if (has_structures) return .structures;
    return .entities_relations;
}

/// Entity extraction has two supported loaded-model layouts: GLiNER models
/// with an extraction-capable head and standard BIO token recognizers. Reject
/// other model families before constructing a pipeline or running inference.
fn validateTextEntityExtractionManifest(manifest: *const manifest_mod.ModelManifest) !void {
    if (manifest.model_type != .recognizer) return error.InvalidModelForExtraction;
    if (!model_caps.modelAcceptsInput(manifest, "text")) return error.UnsupportedInput;
    if (manifest.gliner_model_type.len > 0 and !model_caps.modelSupportsCapability(
        @tagName(manifest.model_type),
        manifest.gliner_model_type,
        manifest.capabilities,
        "extraction",
    )) return error.InvalidModelForExtraction;
}

fn entityExtractionResponseJsonAlloc(
    allocator: std.mem.Allocator,
    request: Node.EntityExtractionRequest,
    all_entities: []const []const @import("../pipelines/ner.zig").Entity,
    all_relations: ?[]const []const gliner_mod.Relation,
    input_texts: []const []const u8,
) ![]u8 {
    if (all_entities.len != input_texts.len or (all_relations != null and all_relations.?.len != input_texts.len))
        return error.InvalidExtractionResponse;
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const alloc = arena.allocator();
    const data = try alloc.alloc(extraction_api.ExtractionObject, all_entities.len);
    for (all_entities, 0..) |entities, text_index| {
        const entity_objects = try alloc.alloc(extraction_api.ExtractionEntity, entities.len);
        for (entities, 0..) |entity, entity_index|
            entity_objects[entity_index] = Node.toExtractionApiEntity(entity, request);

        const relation_objects: ?[]const extraction_api.ExtractionRelation = if (all_relations) |relations_by_text| blk: {
            const relations = relations_by_text[text_index];
            const objects = try alloc.alloc(extraction_api.ExtractionRelation, relations.len);
            for (relations, 0..) |relation, relation_index| {
                const source_index = Node.findEntityIndex(entities, relation.head) orelse return error.InvalidExtractionResponse;
                const target_index = Node.findEntityIndex(entities, relation.tail) orelse return error.InvalidExtractionResponse;
                objects[relation_index] = .{
                    .type = relation.label,
                    .source = .{ .entity_index = @intCast(source_index) },
                    .target = .{ .entity_index = @intCast(target_index) },
                    .score = if (request.include_confidence) relation.score else null,
                };
            }
            break :blk objects;
        } else null;
        data[text_index] = .{
            .id = if (text_index < request.input_ids.len) request.input_ids[text_index] else null,
            .entities = entity_objects,
            .relations = relation_objects,
        };
    }
    const prompt_tokens = estimateTextsTokens(input_texts);
    var usage: std.json.ObjectMap = .empty;
    try usage.ensureTotalCapacity(alloc, 3);
    usage.putAssumeCapacity("prompt_tokens", .{ .integer = @intCast(prompt_tokens) });
    usage.putAssumeCapacity("completion_tokens", .{ .integer = 0 });
    usage.putAssumeCapacity("total_tokens", .{ .integer = @intCast(prompt_tokens) });
    return try std.json.Stringify.valueAlloc(allocator, extraction_api.ExtractionResponse{
        .object = "extraction",
        .data = data,
        .model = request.model,
        .usage = .{ .map = usage },
    }, .{});
}

fn predictorTaskFromTabular(task: @import("ml_tabular").ir.TaskType) api.PredictorTask {
    return switch (task) {
        .regression => .regression,
        .binary_classification => .binary_classification,
        .multiclass => .multiclass,
        .ranking => .ranking,
    };
}

fn buildClassificationResponse(
    ctx: *httpx.Context,
    model_name: []const u8,
    all_results: anytype,
    prompt_tokens: usize,
) !httpx.Response {
    var arena = std.heap.ArenaAllocator.init(ctx.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const data = try alloc.alloc(api.ClassifyObject, all_results.len);
    for (all_results, 0..) |results, ti| {
        const inner = try alloc.alloc(api.ClassifyResult, results.len);
        for (results, 0..) |r, ri| inner[ri] = .{ .label = r.label, .score = r.score };
        data[ti] = .{
            .object = "classification",
            .index = @intCast(ti),
            .classifications = inner,
        };
    }

    return ctx.json(api.ClassifyResponse{
        .object = "list",
        .data = data,
        .model = model_name,
        .usage = tokenUsage(prompt_tokens, 0),
    });
}

fn extractionResponseJsonAlloc(
    allocator: std.mem.Allocator,
    model_name: []const u8,
    inputs: []const extracting_api.Input,
    all_results: []const extraction_mod.ExtractionResult,
) ![]u8 {
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const data = try alloc.alloc(extraction_api.ExtractionObject, all_results.len);
    for (all_results, 0..) |result, result_index| {
        var structures_map: std.json.ObjectMap = .empty;
        try structures_map.ensureTotalCapacity(alloc, result.structures.len);
        for (result.structures) |structure| {
            var instances: std.json.Array = .init(alloc);
            try instances.ensureTotalCapacity(structure.instances.len);
            for (structure.instances, 0..) |instance, instance_index| {
                var instance_obj: std.json.ObjectMap = .empty;
                try instance_obj.ensureTotalCapacity(alloc, instance.fields.len);
                for (instance.fields) |field| {
                    try instance_obj.put(alloc, field.name, try extractedFieldToValue(alloc, field.value));
                }
                _ = instance_index;
                instances.appendAssumeCapacity(.{ .object = instance_obj });
            }
            structures_map.putAssumeCapacity(structure.name, .{ .array = instances });
        }
        data[result_index] = .{
            .id = if (result_index < inputs.len) inputs[result_index].id else null,
            .structures = .{ .map = structures_map },
        };
    }

    return try std.json.Stringify.valueAlloc(allocator, extraction_api.ExtractionResponse{
        .object = "extraction",
        .data = data,
        .model = model_name,
        .usage = null,
    }, .{});
}

fn readerFieldsJsonAlloc(allocator: std.mem.Allocator, fields: []const readers_mod.Field) !?[]const u8 {
    if (fields.len == 0) return null;
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const alloc = arena.allocator();
    var obj: std.json.ObjectMap = .empty;
    try obj.ensureTotalCapacity(alloc, fields.len);
    for (fields) |field| {
        try obj.put(alloc, field.name, .{ .string = field.value });
    }
    return try std.json.Stringify.valueAlloc(allocator, std.json.Value{ .object = obj }, .{});
}

fn readerRegionsJsonAlloc(allocator: std.mem.Allocator, regions: []const readers_mod.Region) !?[]const u8 {
    if (regions.len == 0) return null;
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const alloc = arena.allocator();
    var arr = std.json.Array.init(alloc);
    try arr.ensureTotalCapacity(regions.len);
    for (regions) |region| {
        var obj: std.json.ObjectMap = .empty;
        try obj.ensureTotalCapacity(alloc, 4);
        try obj.put(alloc, "text", .{ .string = region.text });
        var bbox = std.json.Array.init(alloc);
        try bbox.ensureTotalCapacity(region.bbox.len);
        for (region.bbox) |coord| bbox.appendAssumeCapacity(.{ .float = coord });
        try obj.put(alloc, "bbox", .{ .array = bbox });
        if (region.confidence) |confidence| try obj.put(alloc, "confidence", .{ .float = confidence });
        if (region.label) |label| try obj.put(alloc, "label", .{ .string = label });
        arr.appendAssumeCapacity(.{ .object = obj });
    }
    return try std.json.Stringify.valueAlloc(allocator, std.json.Value{ .array = arr }, .{});
}

const DirectExtractionOptions = struct {
    allocator: std.mem.Allocator,
    threshold: ?f32 = null,
    flat_ner: ?bool = null,
    include_confidence: ?bool = null,
    include_spans: ?bool = null,
    prompt: ?[]u8 = null,
    max_tokens: ?usize = null,

    fn deinit(self: *@This()) void {
        if (self.prompt) |prompt| self.allocator.free(prompt);
        self.* = undefined;
    }
};

fn parseExtractionOptionsJson(allocator: std.mem.Allocator, raw: []const u8) !DirectExtractionOptions {
    var out = DirectExtractionOptions{ .allocator = allocator };
    errdefer out.deinit();
    if (raw.len == 0) return out;
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, raw, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return out;
    const obj = parsed.value.object;
    out.threshold = jsonFloatField(obj, "threshold");
    out.flat_ner = jsonBoolField(obj, "flat_ner");
    out.include_confidence = jsonBoolField(obj, "include_confidence");
    out.include_spans = jsonBoolField(obj, "include_spans");
    if (jsonStringField(obj, "prompt")) |prompt| out.prompt = try allocator.dupe(u8, prompt);
    out.max_tokens = try readMaxTokensJsonField(obj, "max_tokens");
    return out;
}

const DirectExtractionInputs = struct {
    allocator: std.mem.Allocator,
    texts: std.ArrayListUnmanaged([]const u8) = .empty,
    images: std.ArrayListUnmanaged([]const u8) = .empty,
    prompt: ?[]u8 = null,
    max_tokens: ?usize = null,

    fn deinit(self: *@This()) void {
        for (self.texts.items) |text| self.allocator.free(@constCast(text));
        self.texts.deinit(self.allocator);
        for (self.images.items) |image| self.allocator.free(@constCast(image));
        self.images.deinit(self.allocator);
        if (self.prompt) |prompt| self.allocator.free(prompt);
        self.* = undefined;
    }
};

fn directExtractionMediaShape(
    allocator: std.mem.Allocator,
    inputs: []const extracting_api.Input,
) !RequestMediaAdmissionShape {
    var shape: RequestMediaAdmissionShape = .{};
    for (inputs) |input| try addDirectExtractionContentMediaShape(allocator, &shape, input.content_json);
    return shape;
}

fn addDirectExtractionContentMediaShape(
    allocator: std.mem.Allocator,
    shape: *RequestMediaAdmissionShape,
    content_json: []const u8,
) !void {
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, content_json, .{});
    defer parsed.deinit();
    if (parsed.value != .array) return;
    for (parsed.value.array.items) |part| {
        if (part != .object) continue;
        const part_type = part.object.get("type") orelse continue;
        if (part_type != .string) continue;
        if (std.mem.eql(u8, part_type.string, "image_url")) {
            if (part.object.get("image_url")) |image_url| shape.addImageUrl(image_url);
        } else if (std.mem.eql(u8, part_type.string, "media")) {
            if (part.object.get("url")) |url| {
                shape.addImageUrl(url);
            } else if (part.object.get("data")) |data| {
                if (data == .string) shape.addInline(data.string.len, true);
            }
        }
    }
}

fn canonicalExtractingInputs(allocator: std.mem.Allocator, inputs: []const extraction_api.ExtractionInput) ![]extracting_api.Input {
    const out = try allocator.alloc(extracting_api.Input, inputs.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |input| allocator.free(input.content_json);
        allocator.free(out);
    }
    for (inputs, 0..) |input, index| {
        out[index] = .{
            .id = input.id,
            .content_json = try std.json.Stringify.valueAlloc(allocator, input.content, .{}),
        };
        initialized += 1;
    }
    return out;
}

fn validateExtractionInputKinds(text_count: usize, image_count: usize) !void {
    if (text_count == 0 and image_count == 0) return error.UnsupportedInput;
    if (text_count > 0 and image_count > 0) return error.UnsupportedInput;
}

fn parseCanonicalStructureSchemas(
    allocator: std.mem.Allocator,
    schema: extraction_api.ExtractionSchema,
) ![]extraction_mod.ExtractionSchema {
    const structures = schema.structures orelse return error.MissingStructureSchema;
    if (structures.map.count() == 0) return error.MissingStructureSchema;
    const out = try allocator.alloc(extraction_mod.ExtractionSchema, structures.map.count());
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*item| item.deinit(allocator);
        allocator.free(out);
    }
    var iterator = structures.map.iterator();
    while (iterator.next()) |entry| {
        if (entry.key_ptr.*.len == 0) return error.InvalidStructureField;
        const fields_map = entry.value_ptr.fields;
        if (fields_map.map.count() == 0) return error.MissingStructureFields;
        const fields = try allocator.alloc(extraction_mod.SchemaField, fields_map.map.count());
        var fields_initialized: usize = 0;
        errdefer {
            for (fields[0..fields_initialized]) |*field| field.deinit(allocator);
            allocator.free(fields);
        }
        var fields_iterator = fields_map.map.iterator();
        while (fields_iterator.next()) |field_entry| {
            if (field_entry.key_ptr.*.len == 0) return error.InvalidStructureField;
            const descriptor = field_entry.value_ptr.*;
            var field_type: extraction_mod.FieldType = .str;
            var choices = std.ArrayListUnmanaged([]const u8).empty;
            errdefer {
                for (choices.items) |choice| allocator.free(choice);
                choices.deinit(allocator);
            }
            switch (descriptor) {
                .string => |kind| {
                    if (std.ascii.eqlIgnoreCase(kind, "list") or std.ascii.eqlIgnoreCase(kind, "array")) {
                        field_type = .list;
                    } else if (!std.ascii.eqlIgnoreCase(kind, "str") and !std.ascii.eqlIgnoreCase(kind, "string")) {
                        return error.InvalidStructureField;
                    }
                },
                .object => |object| {
                    if (jsonStringField(object, "type")) |kind| {
                        if (std.ascii.eqlIgnoreCase(kind, "list") or std.ascii.eqlIgnoreCase(kind, "array")) {
                            field_type = .list;
                        } else if (!std.ascii.eqlIgnoreCase(kind, "str") and !std.ascii.eqlIgnoreCase(kind, "string")) {
                            return error.InvalidStructureField;
                        }
                    }
                    if (object.get("enum")) |enum_value| if (enum_value == .array) {
                        if (enum_value.array.items.len < 2) return error.InvalidStructureField;
                        for (enum_value.array.items) |choice| {
                            if (choice != .string or choice.string.len == 0) return error.InvalidStructureField;
                            try choices.append(allocator, try allocator.dupe(u8, choice.string));
                        }
                    } else return error.InvalidStructureField;
                },
                else => return error.InvalidStructureField,
            }
            const owned_name = try allocator.dupe(u8, field_entry.key_ptr.*);
            errdefer allocator.free(owned_name);
            const owned_choices = try choices.toOwnedSlice(allocator);
            errdefer {
                for (owned_choices) |choice| allocator.free(choice);
                allocator.free(owned_choices);
            }
            fields[fields_initialized] = .{
                .name = owned_name,
                .field_type = field_type,
                .choices = owned_choices,
            };
            fields_initialized += 1;
        }
        out[initialized] = .{
            .name = try allocator.dupe(u8, entry.key_ptr.*),
            .fields = fields,
        };
        initialized += 1;
    }
    return out;
}

fn freeCanonicalExtractingInputs(allocator: std.mem.Allocator, inputs: []extracting_api.Input) void {
    for (inputs) |input| allocator.free(input.content_json);
    allocator.free(inputs);
}

fn parseCanonicalTextInputs(allocator: std.mem.Allocator, inputs: []const extraction_api.ExtractionInput) !DirectExtractionInputs {
    const direct = try canonicalExtractingInputs(allocator, inputs);
    defer freeCanonicalExtractingInputs(allocator, direct);
    return parseDirectTextExtractionInputs(allocator, direct);
}

fn canonicalInputIds(allocator: std.mem.Allocator, inputs: []const extraction_api.ExtractionInput) ![]?[]const u8 {
    const ids = try allocator.alloc(?[]const u8, inputs.len);
    for (inputs, 0..) |input, index| ids[index] = input.id;
    return ids;
}

fn canonicalRelationLabels(
    allocator: std.mem.Allocator,
    schemas: []const extraction_api.ExtractionRelationSchema,
) ![][]const u8 {
    const labels = try allocator.alloc([]const u8, schemas.len);
    var initialized: usize = 0;
    errdefer {
        for (labels[0..initialized]) |label| allocator.free(label);
        allocator.free(labels);
    }
    for (schemas, 0..) |schema, index| {
        if (!isCanonicalLabelSegment(schema.type)) return error.InvalidRelationSchema;
        if (schema.target != null and schema.source == null) return error.InvalidRelationSchema;
        if (schema.source) |source| if (!isCanonicalLabelSegment(source)) return error.InvalidRelationSchema;
        if (schema.target) |target| if (!isCanonicalLabelSegment(target)) return error.InvalidRelationSchema;
        labels[index] = if (schema.source) |source|
            if (schema.target) |target|
                try std.fmt.allocPrint(allocator, "{s}::{s}::{s}", .{ source, schema.type, target })
            else
                try std.fmt.allocPrint(allocator, "{s}::{s}", .{ source, schema.type })
        else
            try allocator.dupe(u8, schema.type);
        initialized += 1;
    }
    return labels;
}

/// Build the entity-label vocabulary needed by GLiNER relation extraction.
/// Explicit entity labels remain authoritative; otherwise model defaults are
/// retained. Canonical relation endpoint constraints are always added so a
/// legal relations-only schema does not depend on those endpoints happening
/// to exist in a model's default vocabulary.
fn relationEntityLabelsAlloc(
    allocator: std.mem.Allocator,
    requested: ?[]const []const u8,
    defaults: []const []const u8,
    canonical_relation_labels: []const []const u8,
) !?[]const []const u8 {
    var labels = std.ArrayListUnmanaged([]const u8).empty;
    errdefer labels.deinit(allocator);

    const seed = if (requested) |values|
        if (values.len > 0) values else defaults
    else
        defaults;
    for (seed) |label| try appendUniqueLabel(allocator, &labels, label);

    for (canonical_relation_labels) |label| {
        const first_separator = std.mem.indexOf(u8, label, "::") orelse continue;
        try appendUniqueLabel(allocator, &labels, label[0..first_separator]);
        const remainder = label[first_separator + 2 ..];
        const second_separator = std.mem.indexOf(u8, remainder, "::") orelse continue;
        try appendUniqueLabel(allocator, &labels, remainder[second_separator + 2 ..]);
    }

    if (labels.items.len == 0) {
        labels.deinit(allocator);
        return null;
    }
    return try labels.toOwnedSlice(allocator);
}

fn appendUniqueLabel(
    allocator: std.mem.Allocator,
    labels: *std.ArrayListUnmanaged([]const u8),
    candidate: []const u8,
) !void {
    for (labels.items) |label| {
        if (std.ascii.eqlIgnoreCase(label, candidate)) return;
    }
    try labels.append(allocator, candidate);
}

fn isCanonicalLabelSegment(value: []const u8) bool {
    return value.len > 0 and std.mem.indexOf(u8, value, "::") == null;
}

fn freeStringSlice(allocator: std.mem.Allocator, values: []const []const u8) void {
    for (values) |value| allocator.free(value);
    allocator.free(values);
}

fn extractionDirectFailureResponse(ctx: *httpx.Context, err: anyerror) !httpx.Response {
    return switch (err) {
        error.QueueFull => transientCapacityFailureResponse(
            ctx,
            "SERVICE_UNAVAILABLE",
            "server at capacity, try again later",
            "inference_admission",
        ),
        error.InvalidExtractionConfig,
        error.InvalidClassificationSchema,
        error.InvalidEntitySchema,
        error.InvalidModelForExtraction,
        error.InvalidRelationSchema,
        error.InvalidStructureField,
        error.InvalidTopK,
        error.MissingExtractionOperation,
        error.MissingStructureFields,
        error.MissingStructureSchema,
        error.MixedExtractionOperations,
        error.UnsupportedClassificationExtraction,
        error.UnsupportedInput,
        error.UnsupportedRelationExtraction,
        error.InvalidMaxTokens,
        => ctx.status(400).json(.{ .@"error" = "INVALID_REQUEST", .message = @errorName(err) }),
        error.UnsupportedRebelEntityLabels,
        error.UnsupportedRebelRelationEndpoints,
        => rebelSchemaFailureResponse(ctx, err),
        error.ReadBatchTooLarge,
        error.StreamTooLong,
        => ctx.status(413).json(.{ .@"error" = "BATCH_TOO_LARGE", .message = @errorName(err) }),
        error.ModelNotFound => ctx.status(404).json(.{ .@"error" = "MODEL_NOT_FOUND", .message = "model not found" }),
        else => inferenceFailureResponse(ctx, err),
    };
}

fn rebelSchemaFailureResponse(ctx: *httpx.Context, err: anyerror) !httpx.Response {
    return switch (err) {
        error.UnsupportedRebelEntityLabels => ctx.status(400).json(.{
            .@"error" = "INVALID_REQUEST",
            .message = "REBEL supports only the generic entity label 'ENTITY'; use an entity-typing extractor for typed entity schemas",
        }),
        error.UnsupportedRebelRelationEndpoints => ctx.status(400).json(.{
            .@"error" = "INVALID_REQUEST",
            .message = "REBEL supports relation-type filters, but source and target constraints must use the generic 'ENTITY' label",
        }),
        else => return err,
    };
}

fn parseDirectTextExtractionInputs(
    allocator: std.mem.Allocator,
    inputs: []const extracting_api.Input,
) !DirectExtractionInputs {
    var out = DirectExtractionInputs{ .allocator = allocator };
    errdefer out.deinit();
    for (inputs) |input| try appendDirectTextExtractionContent(allocator, &out, input.content_json);
    if (out.texts.items.len != inputs.len or out.texts.items.len == 0) return error.UnsupportedInput;
    return out;
}

fn appendDirectTextExtractionContent(
    allocator: std.mem.Allocator,
    out: *DirectExtractionInputs,
    content_json: []const u8,
) !void {
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, content_json, .{});
    defer parsed.deinit();

    const text = switch (parsed.value) {
        .string => |value| try allocator.dupe(u8, value),
        .array => |parts| blk: {
            var text_buf = std.ArrayListUnmanaged(u8).empty;
            defer text_buf.deinit(allocator);
            var saw_text = false;
            for (parts.items) |part| {
                if (part != .object) continue;
                const type_value = part.object.get("type") orelse continue;
                if (type_value != .string) continue;
                if (std.mem.eql(u8, type_value.string, "image_url") or
                    std.mem.eql(u8, type_value.string, "media")) return error.UnsupportedInput;
                if (!std.mem.eql(u8, type_value.string, "text")) continue;
                const text_value = part.object.get("text") orelse continue;
                if (text_value != .string) continue;
                if (saw_text) try text_buf.append(allocator, '\n');
                try text_buf.appendSlice(allocator, text_value.string);
                saw_text = true;
            }
            if (!saw_text) return error.UnsupportedInput;
            break :blk try allocator.dupe(u8, text_buf.items);
        },
        else => try std.json.Stringify.valueAlloc(allocator, parsed.value, .{}),
    };
    errdefer allocator.free(text);
    try out.texts.append(allocator, text);
}

fn parseDirectExtractionInputs(
    node: *Node,
    allocator: std.mem.Allocator,
    inputs: []const extracting_api.Input,
    prompt: ?[]const u8,
    max_tokens: ?usize,
    max_media_bytes: usize,
) !DirectExtractionInputs {
    var media_budget = RequestMediaBudget.init(max_media_bytes);
    var out = DirectExtractionInputs{
        .allocator = allocator,
        .prompt = if (prompt) |value| try allocator.dupe(u8, value) else null,
        .max_tokens = max_tokens,
    };
    errdefer out.deinit();

    for (inputs) |input| {
        try appendDirectExtractionContent(node, allocator, &out, input.content_json, &media_budget);
    }
    if (out.texts.items.len > 0 and out.images.items.len > 0) return error.UnsupportedInput;
    if (out.texts.items.len == 0 and out.images.items.len == 0) return error.UnsupportedInput;
    return out;
}

fn appendDirectExtractionContent(
    node: *Node,
    allocator: std.mem.Allocator,
    out: *DirectExtractionInputs,
    content_json: []const u8,
    media_budget: *RequestMediaBudget,
) !void {
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, content_json, .{});
    defer parsed.deinit();

    switch (parsed.value) {
        .string => |text| {
            const owned_text = try allocator.dupe(u8, text);
            errdefer allocator.free(owned_text);
            try out.texts.append(allocator, owned_text);
        },
        .array => |parts| {
            var text_buf = std.ArrayListUnmanaged(u8).empty;
            defer text_buf.deinit(allocator);
            var saw_media = false;
            for (parts.items) |part| {
                if (part != .object) continue;
                const type_value = part.object.get("type") orelse continue;
                if (type_value != .string) continue;
                if (std.mem.eql(u8, type_value.string, "text")) {
                    const text_value = part.object.get("text") orelse continue;
                    if (text_value != .string) continue;
                    if (text_buf.items.len > 0) try text_buf.append(allocator, '\n');
                    try text_buf.appendSlice(allocator, text_value.string);
                } else if (std.mem.eql(u8, type_value.string, "image_url")) {
                    const image_url = part.object.get("image_url") orelse continue;
                    const url = if (image_url == .object)
                        if (image_url.object.get("url")) |url_value| (if (url_value == .string) url_value.string else null) else null
                    else if (image_url == .string)
                        image_url.string
                    else
                        null;
                    if (url) |value| {
                        try appendDownloadedExtractionImage(node, allocator, out, value, media_budget);
                        saw_media = true;
                    }
                } else if (std.mem.eql(u8, type_value.string, "media")) {
                    if (part.object.get("url")) |url_value| {
                        if (url_value == .string) {
                            try appendDownloadedExtractionImage(node, allocator, out, url_value.string, media_budget);
                            saw_media = true;
                        }
                    } else if (part.object.get("data")) |data_value| {
                        if (data_value == .string) {
                            const decoded = try decodeMediaDataWithBudget(allocator, data_value.string, media_budget);
                            var owns_decoded = true;
                            errdefer if (owns_decoded) allocator.free(decoded.data);
                            if (decoded.mime_type) |mime| {
                                if (!std.mem.startsWith(u8, mime, "image/")) return error.UnsupportedInput;
                            }
                            try out.images.append(allocator, decoded.data);
                            owns_decoded = false;
                            saw_media = true;
                        }
                    }
                }
            }
            if (saw_media) {
                if (out.prompt == null and text_buf.items.len > 0) out.prompt = try text_buf.toOwnedSlice(allocator);
            } else if (text_buf.items.len > 0) {
                const owned_text = try text_buf.toOwnedSlice(allocator);
                errdefer allocator.free(owned_text);
                try out.texts.append(allocator, owned_text);
            }
        },
        else => {
            const text = try std.json.Stringify.valueAlloc(allocator, parsed.value, .{});
            errdefer allocator.free(text);
            try out.texts.append(allocator, text);
        },
    }
}

fn appendDownloadedExtractionImage(
    node: *Node,
    allocator: std.mem.Allocator,
    out: *DirectExtractionInputs,
    url: []const u8,
    media_budget: *RequestMediaBudget,
) !void {
    const downloaded = try downloadRemoteContentWithBudgetForRequest(node, allocator, url, media_budget);
    defer allocator.free(downloaded.content_type);
    errdefer allocator.free(downloaded.data);
    if (!std.mem.startsWith(u8, downloaded.content_type, "image/")) return error.UnsupportedInput;
    try out.images.append(allocator, downloaded.data);
}

fn jsonStringField(obj: std.json.ObjectMap, name: []const u8) ?[]const u8 {
    const value = obj.get(name) orelse return null;
    return if (value == .string) value.string else null;
}

fn jsonBoolField(obj: std.json.ObjectMap, name: []const u8) ?bool {
    const value = obj.get(name) orelse return null;
    return if (value == .bool) value.bool else null;
}

fn jsonFloatField(obj: std.json.ObjectMap, name: []const u8) ?f32 {
    const value = obj.get(name) orelse return null;
    return switch (value) {
        .float => |number| @floatCast(number),
        .integer => |number| @floatFromInt(number),
        .number_string => |raw| std.fmt.parseFloat(f32, raw) catch null,
        else => null,
    };
}

fn extractedFieldToValue(
    alloc: std.mem.Allocator,
    field: extraction_mod.ExtractedField,
) !std.json.Value {
    switch (field) {
        .single => |value| return try extractedFieldValueToValue(alloc, value),
        .list => |values| {
            var arr: std.json.Array = .init(alloc);
            try arr.ensureTotalCapacity(values.len);
            for (values) |value| {
                arr.appendAssumeCapacity(try extractedFieldValueToValue(alloc, value));
            }
            return .{ .array = arr };
        },
    }
}

fn extractedFieldValueToValue(
    alloc: std.mem.Allocator,
    value: extraction_mod.ExtractedFieldValue,
) !std.json.Value {
    var obj: std.json.ObjectMap = .empty;
    try obj.ensureTotalCapacity(alloc, 4);
    obj.putAssumeCapacity("value", .{ .string = value.value });
    if (value.score) |score| obj.putAssumeCapacity("score", .{ .float = score });
    if (value.start) |start| obj.putAssumeCapacity("start", .{ .integer = @intCast(start) });
    if (value.end) |end| obj.putAssumeCapacity("end", .{ .integer = @intCast(end) });
    return .{ .object = obj };
}

const ResolvedExtractionOutput = struct {
    entities: [][]@import("../pipelines/ner.zig").Entity,
    relations: ?[][]gliner_mod.Relation,

    fn deinit(self: *ResolvedExtractionOutput, allocator: std.mem.Allocator) void {
        for (self.entities) |entities| {
            for (entities) |entity| {
                allocator.free(entity.text);
                allocator.free(entity.label);
            }
            allocator.free(entities);
        }
        allocator.free(self.entities);

        if (self.relations) |relations_by_text| {
            for (relations_by_text) |relations| {
                for (relations) |*relation| {
                    allocator.free(relation.head.label);
                    allocator.free(relation.tail.label);
                    relation.deinit(allocator);
                }
                allocator.free(relations);
            }
            allocator.free(relations_by_text);
        }
    }
};

fn resolveExtractedOutput(
    allocator: std.mem.Allocator,
    all_entities: []const []const @import("../pipelines/ner.zig").Entity,
    all_relations: ?[]const []const gliner_mod.Relation,
    resolver_cfg: extraction_api.ExtractionResolverOptions,
) !ResolvedExtractionOutput {
    const cfg = resolver_mod.ResolverConfig{
        .similarity_threshold = if ((resolver_cfg.similarity_threshold orelse 0.0) == 0.0) 0.85 else resolver_cfg.similarity_threshold.?,
        .type_must_match = resolver_cfg.type_must_match orelse true,
        .min_entity_confidence = resolver_cfg.min_entity_confidence orelse 0.0,
        .min_relation_confidence = resolver_cfg.min_relation_confidence orelse 0.0,
        .deduplicate_relations = resolver_cfg.deduplicate_relations orelse true,
        .track_provenance = resolver_cfg.track_provenance orelse true,
    };

    var kg = try resolver_mod.buildKnowledgeGraph(allocator, all_entities, all_relations, cfg);
    defer kg.deinit(allocator);

    const input_count = all_entities.len;
    const entity_batches = try allocator.alloc([]@import("../pipelines/ner.zig").Entity, input_count);
    var initialized_entity_batches: usize = 0;
    errdefer {
        freeOwnedEntityBatchContents(allocator, entity_batches[0..initialized_entity_batches]);
        allocator.free(entity_batches);
    }

    for (0..input_count) |text_index| {
        var resolved_for_text = std.ArrayListUnmanaged(@import("../pipelines/ner.zig").Entity).empty;
        var owned = false;
        defer if (!owned) {
            for (resolved_for_text.items) |entity| {
                allocator.free(entity.text);
                allocator.free(entity.label);
            }
            resolved_for_text.deinit(allocator);
        };

        for (kg.entities) |entity| {
            if (!resolvedHasTextIndex(entity.text_indices, text_index)) continue;
            const cloned = try cloneResolvedEntity(allocator, entity, text_index);
            errdefer {
                allocator.free(cloned.text);
                allocator.free(cloned.label);
            }
            try resolved_for_text.append(allocator, cloned);
        }

        entity_batches[text_index] = try resolved_for_text.toOwnedSlice(allocator);
        owned = true;
        initialized_entity_batches += 1;
    }

    var relation_batches: ?[][]gliner_mod.Relation = null;
    if (kg.relations.len > 0) {
        const batches = try allocator.alloc([]gliner_mod.Relation, input_count);
        var initialized_relation_batches: usize = 0;
        errdefer {
            freeRelationBatches(allocator, batches[0..initialized_relation_batches]);
            allocator.free(batches);
        }

        for (0..input_count) |text_index| {
            var resolved_for_text = std.ArrayListUnmanaged(gliner_mod.Relation).empty;
            var owned = false;
            defer if (!owned) {
                for (resolved_for_text.items) |*relation| {
                    allocator.free(relation.head.label);
                    allocator.free(relation.tail.label);
                    relation.deinit(allocator);
                }
                resolved_for_text.deinit(allocator);
            };

            for (kg.relations) |relation| {
                if (!resolvedHasTextIndex(relation.text_indices, text_index)) continue;
                var cloned = try cloneResolvedRelation(allocator, kg.entities, relation, text_index);
                errdefer {
                    allocator.free(cloned.head.label);
                    allocator.free(cloned.tail.label);
                    cloned.deinit(allocator);
                }
                try resolved_for_text.append(allocator, cloned);
            }

            batches[text_index] = try resolved_for_text.toOwnedSlice(allocator);
            owned = true;
            initialized_relation_batches += 1;
        }

        relation_batches = batches;
    }

    return .{
        .entities = entity_batches,
        .relations = relation_batches,
    };
}

fn resolvedHasTextIndex(indices: ?[]const usize, text_index: usize) bool {
    const values = indices orelse return text_index == 0;
    for (values) |value| {
        if (value == text_index) return true;
    }
    return false;
}

fn cloneResolvedEntity(
    allocator: std.mem.Allocator,
    entity: resolver_mod.ResolvedEntity,
    text_index: usize,
) !@import("../pipelines/ner.zig").Entity {
    const occurrence = resolvedOccurrenceForInput(entity, text_index) orelse return error.InvalidResolvedEntity;
    const text = try allocator.dupe(u8, occurrence.text);
    errdefer allocator.free(text);
    const label = try allocator.dupe(u8, entity.label);
    errdefer allocator.free(label);

    return .{
        .text = text,
        .label = label,
        .start = occurrence.start,
        .end = occurrence.end,
        .score = occurrence.score,
    };
}

fn cloneResolvedRelation(
    allocator: std.mem.Allocator,
    entities: []const resolver_mod.ResolvedEntity,
    relation: resolver_mod.ResolvedRelation,
    text_index: usize,
) !gliner_mod.Relation {
    const head_entity = findResolvedEntityById(entities, relation.head_id) orelse return error.InvalidResolvedRelation;
    const tail_entity = findResolvedEntityById(entities, relation.tail_id) orelse return error.InvalidResolvedRelation;
    const head_occurrence = resolvedOccurrenceForInput(head_entity, text_index) orelse return error.InvalidResolvedRelation;
    const tail_occurrence = resolvedOccurrenceForInput(tail_entity, text_index) orelse return error.InvalidResolvedRelation;

    const head_text = try allocator.dupe(u8, head_occurrence.text);
    errdefer allocator.free(head_text);
    const head_label = try allocator.dupe(u8, head_entity.label);
    errdefer allocator.free(head_label);
    const tail_text = try allocator.dupe(u8, tail_occurrence.text);
    errdefer allocator.free(tail_text);
    const tail_label = try allocator.dupe(u8, tail_entity.label);
    errdefer allocator.free(tail_label);
    const label = try allocator.dupe(u8, relation.label);
    errdefer allocator.free(label);

    return .{
        .head = .{
            .text = head_text,
            .label = head_label,
            .start = head_occurrence.start,
            .end = head_occurrence.end,
            .score = head_occurrence.score,
        },
        .tail = .{
            .text = tail_text,
            .label = tail_label,
            .start = tail_occurrence.start,
            .end = tail_occurrence.end,
            .score = tail_occurrence.score,
        },
        .label = label,
        .score = relation.score,
    };
}

fn resolvedOccurrenceForInput(entity: resolver_mod.ResolvedEntity, text_index: usize) ?resolver_mod.ResolvedMention {
    var best: ?resolver_mod.ResolvedMention = null;
    for (entity.occurrences) |occurrence| {
        if (occurrence.text_index != text_index) continue;
        if (best == null or occurrence.score > best.?.score or
            (occurrence.score == best.?.score and occurrence.text.len > best.?.text.len))
        {
            best = occurrence;
        }
    }
    return best;
}

fn freeRelationBatches(allocator: std.mem.Allocator, batches: [][]gliner_mod.Relation) void {
    for (batches) |relations| {
        for (relations) |*relation| {
            allocator.free(relation.head.label);
            allocator.free(relation.tail.label);
            relation.deinit(allocator);
        }
        allocator.free(relations);
    }
}

fn findResolvedEntityById(
    entities: []const resolver_mod.ResolvedEntity,
    id: []const u8,
) ?resolver_mod.ResolvedEntity {
    for (entities) |entity| {
        if (std.mem.eql(u8, entity.id, id)) return entity;
    }
    return null;
}

fn applyLearnedCleanupIfPresent(
    allocator: std.mem.Allocator,
    cleanup_head: ?*const cleanup_model_mod.CleanupHead,
    texts: []const []const u8,
    entities_by_text: []const []const @import("../pipelines/ner.zig").Entity,
) !?[][]@import("../pipelines/ner.zig").Entity {
    if (texts.len != entities_by_text.len) return error.ShapeMismatch;

    const head = cleanup_head orelse return null;

    const out = try allocator.alloc([]@import("../pipelines/ner.zig").Entity, texts.len);
    var built: usize = 0;
    errdefer {
        freeOwnedEntityBatchContents(allocator, out[0..built]);
        allocator.free(out);
    }

    for (texts, entities_by_text, 0..) |text, entities, idx| {
        var cleanup_entities = try allocator.alloc(cleanup_pipeline_mod.Entity, entities.len);
        defer allocator.free(cleanup_entities);
        for (entities, 0..) |entity, entity_idx| {
            cleanup_entities[entity_idx] = .{
                .text = entity.text,
                .label = entity.label,
                .start = entity.start,
                .end = entity.end,
                .score = entity.score,
            };
        }

        const scored = try cleanup_model_mod.scoreEntities(allocator, head, text, cleanup_entities);
        defer {
            for (scored) |*mention| mention.deinit(allocator);
            allocator.free(scored);
        }

        var cleaned = try cleanup_pipeline_mod.cleanupMentions(allocator, scored, .{
            .min_validity_score = head.min_validity_score,
            .dedup_similarity_threshold = head.dedup_similarity_threshold,
        });
        defer cleaned.deinit(allocator);

        out[idx] = try cloneCleanedEntityBatch(allocator, cleaned.resolved_entities);
        built += 1;
    }

    return out;
}

fn cloneCleanedEntityBatch(
    allocator: std.mem.Allocator,
    resolved_entities: []const cleanup_pipeline_mod.ResolvedEntity,
) ![]@import("../pipelines/ner.zig").Entity {
    const out = try allocator.alloc(@import("../pipelines/ner.zig").Entity, resolved_entities.len);
    var built: usize = 0;
    errdefer {
        for (out[0..built]) |entity| {
            allocator.free(entity.text);
            allocator.free(entity.label);
        }
        allocator.free(out);
    }

    for (resolved_entities, 0..) |resolved_entity, idx| {
        const text = try allocator.dupe(u8, resolved_entity.text);
        errdefer allocator.free(text);
        const label = try allocator.dupe(u8, resolved_entity.label);
        errdefer allocator.free(label);
        out[idx] = .{
            .text = text,
            .label = label,
            .start = resolved_entity.start,
            .end = resolved_entity.end,
            .score = resolved_entity.detect_score * resolved_entity.validity_score,
        };
        built += 1;
    }

    return out;
}

fn freeOwnedEntityBatchContents(
    allocator: std.mem.Allocator,
    all_entities: []const []@import("../pipelines/ner.zig").Entity,
) void {
    for (all_entities) |entities| {
        for (entities) |entity| {
            allocator.free(entity.text);
            allocator.free(entity.label);
        }
        allocator.free(entities);
    }
}

fn freeEntityBatches(allocator: std.mem.Allocator, all_entities: []const []@import("../pipelines/ner.zig").Entity) void {
    freeOwnedEntityBatchContents(allocator, all_entities);
    allocator.free(all_entities);
}

/// Release raw NER/GLiNER results. Their span text and batch containers are
/// owned by the caller, while labels borrow model configuration or request
/// schema storage and must remain untouched.
fn freeBorrowedLabelEntityBatches(
    allocator: std.mem.Allocator,
    all_entities: []const []@import("../pipelines/ner.zig").Entity,
) void {
    for (all_entities) |entities| {
        for (entities) |entity| allocator.free(entity.text);
        allocator.free(entities);
    }
    allocator.free(all_entities);
}

fn appendPromMetric(writer: *std.Io.Writer, name: []const u8, metric_type: []const u8, help: []const u8, value: u64) !void {
    try writer.print("# HELP {s} {s}\n# TYPE {s} {s}\n{s} {d}\n", .{ name, help, name, metric_type, name, value });
}

fn appendResidentProjectionMetrics(writer: *std.Io.Writer, stats: embedding_mod.ResidentProjectionStats) !void {
    try appendPromMetric(writer, "antfly_inference_embed_resident_projection_text_success_total", "counter", "Total successful text resident projection attempts", stats.text_success);
    try appendPromMetric(writer, "antfly_inference_embed_resident_projection_text_fallback_total", "counter", "Total text resident projection fallbacks", stats.text_fallback);
    try appendPromMetric(writer, "antfly_inference_embed_resident_projection_image_success_total", "counter", "Total successful image resident projection attempts", stats.image_success);
    try appendPromMetric(writer, "antfly_inference_embed_resident_projection_image_fallback_total", "counter", "Total image resident projection fallbacks", stats.image_fallback);
    try appendPromMetric(writer, "antfly_inference_embed_resident_projection_audio_success_total", "counter", "Total successful audio resident projection attempts", stats.audio_success);
    try appendPromMetric(writer, "antfly_inference_embed_resident_projection_audio_fallback_total", "counter", "Total audio resident projection fallbacks", stats.audio_fallback);
}

fn appendGraphExecutorMetrics(writer: *std.Io.Writer, stats: graph_mod.executor_stats.ExecutionStats) !void {
    try appendPromMetric(writer, "inference_graph_executor_partitions_total", "counter", "Total graph executor partitions executed", stats.partitions_executed);
    try appendPromMetric(writer, "inference_graph_executor_cross_device_transfers_total", "counter", "Total graph executor cross-device transfers", stats.cross_device_transfers);
    try appendPromMetric(writer, "inference_graph_executor_runtime_input_transfers_total", "counter", "Total graph executor runtime input transfers", stats.runtime_input_transfers);
    try appendPromMetric(writer, "inference_graph_executor_device_resident_transfers_total", "counter", "Total graph executor device-resident transfers", stats.device_resident_transfers);
    try appendPromMetric(writer, "inference_graph_executor_backend_command_dispatches_total", "counter", "Total graph executor backend command dispatches", stats.backend_command_dispatches);
    try appendPromMetric(writer, "inference_graph_executor_planned_operator_dispatches_total", "counter", "Total graph executor planned operator dispatches", stats.planned_operator_dispatches);
    try appendPromMetric(writer, "inference_graph_executor_interpreter_fallbacks_total", "counter", "Total graph executor interpreter fallback partitions", stats.interpreter_fallbacks);
    try appendPromMetric(writer, "inference_graph_executor_device_resident_outputs_total", "counter", "Total graph executor device-resident outputs", stats.device_resident_outputs);
    try appendPromMetric(writer, "inference_graph_executor_device_resident_parameter_outputs_total", "counter", "Total graph executor device-resident parameter outputs", stats.device_resident_parameter_outputs);
    try appendPromMetric(writer, "inference_graph_executor_host_materialized_outputs_total", "counter", "Total graph executor host-materialized outputs", stats.host_materialized_outputs);
    try appendPromMetric(writer, "inference_graph_executor_boundary_output_materializations_total", "counter", "Total graph executor boundary output materializations", stats.boundary_output_materializations);
    try appendPromMetric(writer, "inference_graph_executor_graph_plan_slots_reserved_total", "counter", "Total graph executor planned buffer slots reserved", stats.graph_plan_slots_reserved);
    try appendPromMetric(writer, "inference_graph_executor_graph_plan_bytes_reserved_total", "counter", "Total graph executor planned buffer bytes reserved", stats.graph_plan_bytes_reserved);
    try appendPromMetric(writer, "inference_quant_kernel_planned_ops_total", "counter", "Total quant kernel planned operations", stats.quant_kernel_planned_ops);
    try appendPromMetric(writer, "inference_quant_kernel_handwritten_production_total", "counter", "Total quant kernel handwritten production routes", stats.quant_kernel_handwritten_production);
    try appendPromMetric(writer, "inference_quant_kernel_generated_production_total", "counter", "Total quant kernel generated production routes", stats.quant_kernel_generated_production);
    try appendPromMetric(writer, "inference_quant_kernel_unsupported_routes_total", "counter", "Total quant kernel unsupported routes", stats.quant_kernel_unsupported_routes);
    try appendPromMetric(writer, "inference_quant_kernel_fast_path_misses_total", "counter", "Total quant kernel fast-path misses", graph_mod.executor_stats.quantKernelFastPathMisses(stats));
    try appendPromMetric(writer, "inference_quant_kernel_generated_candidates_total", "counter", "Total quant kernel generated dev candidates observed", stats.quant_kernel_generated_candidates);
    try appendPromMetric(writer, "inference_quant_kernel_fallback_generated_artifact_missing_total", "counter", "Total quant kernel fallbacks because generated artifacts are missing", stats.quant_kernel_fallback_generated_artifact_missing);
    try appendPromMetric(writer, "inference_quant_kernel_fallback_generated_runtime_not_wired_total", "counter", "Total quant kernel fallbacks because generated artifacts are promoted but runtime dispatch is not wired", stats.quant_kernel_fallback_generated_runtime_not_wired);
    try appendPromMetric(writer, "inference_quant_kernel_fallback_unsupported_format_total", "counter", "Total quant kernel fallbacks due to unsupported formats", stats.quant_kernel_fallback_unsupported_format);
    try appendPromMetric(writer, "inference_quant_kernel_fallback_unsupported_shape_total", "counter", "Total quant kernel fallbacks due to unsupported shapes", stats.quant_kernel_fallback_unsupported_shape);
    try appendPromMetric(writer, "inference_quant_kernel_fallback_unsupported_epilogue_total", "counter", "Total quant kernel fallbacks due to unsupported epilogues", stats.quant_kernel_fallback_unsupported_epilogue);
    try appendPromMetric(writer, "inference_quant_kernel_fallback_unsupported_backend_total", "counter", "Total quant kernel fallbacks due to unsupported backends", stats.quant_kernel_fallback_unsupported_backend);
    try appendPromMetric(writer, "inference_quant_kernel_fallback_tensor_core_repack_required_total", "counter", "Total quant kernel fallbacks requiring tensor-core repacking", stats.quant_kernel_fallback_tensor_core_repack_required);
    try appendPromMetric(writer, "inference_quant_kernel_fallback_unsupported_total", "counter", "Total quant kernel unsupported fallbacks", stats.quant_kernel_fallback_unsupported);
    const top_fallback = graph_mod.executor_stats.quantKernelTopFallbackReason(stats);
    try writer.print(
        "# HELP inference_quant_kernel_top_fallback_reason Current top quant kernel fallback reason by count\n# TYPE inference_quant_kernel_top_fallback_reason gauge\ninference_quant_kernel_top_fallback_reason{{reason=\"{s}\"}} {d}\n",
        .{ top_fallback.name, top_fallback.count },
    );
}

fn appendMetalExactJitMetrics(writer: *std.Io.Writer, stats: session_factory.MetalExactJitDispatchStats) !void {
    try appendPromMetric(writer, "antfly_inference_metal_jit_exact_q4_0_hits", "gauge", "Exact-profile Q4_0 JIT dispatches retained by currently loaded sessions", stats.q4_0_hits);
    try appendPromMetric(writer, "antfly_inference_metal_jit_exact_q4_k_hits", "gauge", "Exact-profile Q4_K JIT dispatches retained by currently loaded sessions", stats.q4_k_hits);
}

fn aggregateMetalExactJitStats(handles: []const model_manager_mod.ModelHandle) session_factory.MetalExactJitDispatchStats {
    var aggregate = session_factory.MetalExactJitDispatchStats{};
    for (handles) |handle| aggregate.add(handle.get().metalExactJitDispatchStats());
    return aggregate;
}

fn appendPromptCacheMetrics(writer: *std.Io.Writer, models: anytype) !void {
    var hits: u64 = 0;
    var misses: u64 = 0;
    var evictions: u64 = 0;
    var cached_tokens: u64 = 0;
    var live_entries: u64 = 0;
    var live_bytes: u64 = 0;
    var block_hash_hits: u64 = 0;
    var block_hash_misses: u64 = 0;
    var block_hash_evictions: u64 = 0;
    var block_hash_cached_blocks: u64 = 0;
    var block_hash_collision_guards: u64 = 0;
    var radix_hits: u64 = 0;
    var radix_misses: u64 = 0;
    var radix_evictions: u64 = 0;
    var radix_cached_nodes: u64 = 0;
    var radix_collision_guards: u64 = 0;
    var it = models.iterator();
    while (it.next()) |entry| {
        const stats = entry.value_ptr.*.prompt_prefix_cache.stats();
        hits += stats.hits;
        misses += stats.misses;
        evictions += stats.evictions;
        cached_tokens += stats.cached_tokens;
        live_entries += @intCast(stats.live_entries);
        live_bytes += @intCast(stats.live_bytes);
        block_hash_hits += stats.block_hash_hits;
        block_hash_misses += stats.block_hash_misses;
        block_hash_evictions += stats.block_hash_evictions;
        block_hash_cached_blocks += @intCast(stats.block_hash_cached_blocks);
        block_hash_collision_guards += stats.block_hash_collision_guards;
        radix_hits += stats.radix_hits;
        radix_misses += stats.radix_misses;
        radix_evictions += stats.radix_evictions;
        radix_cached_nodes += @intCast(stats.radix_cached_nodes);
        radix_collision_guards += stats.radix_collision_guards;
    }
    try appendPromMetric(writer, "antfly_inference_prompt_cache_hits_total", "counter", "Total prompt prefix cache hits", hits);
    try appendPromMetric(writer, "antfly_inference_prompt_cache_misses_total", "counter", "Total prompt prefix cache misses", misses);
    try appendPromMetric(writer, "antfly_inference_prompt_cache_evictions_total", "counter", "Total prompt prefix cache evictions", evictions);
    try appendPromMetric(writer, "antfly_inference_prompt_cache_cached_tokens", "gauge", "Prompt prefix cache retained prompt tokens", cached_tokens);
    try appendPromMetric(writer, "antfly_inference_prompt_cache_live_entries", "gauge", "Prompt prefix cache live entries", live_entries);
    try appendPromMetric(writer, "antfly_inference_prompt_cache_live_bytes", "gauge", "Prompt prefix cache estimated logical bytes", live_bytes);
    try appendPromMetric(writer, "antfly_inference_prompt_cache_block_hash_hits_total", "counter", "Total block-hash prompt cache hits", block_hash_hits);
    try appendPromMetric(writer, "antfly_inference_prompt_cache_block_hash_misses_total", "counter", "Total block-hash prompt cache misses", block_hash_misses);
    try appendPromMetric(writer, "antfly_inference_prompt_cache_block_hash_evictions_total", "counter", "Total block-hash prompt cache evictions", block_hash_evictions);
    try appendPromMetric(writer, "antfly_inference_prompt_cache_block_hash_cached_blocks", "gauge", "Block-hash prompt cache retained KV blocks", block_hash_cached_blocks);
    try appendPromMetric(writer, "antfly_inference_prompt_cache_block_hash_collision_guards_total", "counter", "Total block-hash prompt cache collisions detected", block_hash_collision_guards);
    try appendPromMetric(writer, "antfly_inference_prompt_cache_radix_hits_total", "counter", "Total radix prompt cache hits", radix_hits);
    try appendPromMetric(writer, "antfly_inference_prompt_cache_radix_misses_total", "counter", "Total radix prompt cache misses", radix_misses);
    try appendPromMetric(writer, "antfly_inference_prompt_cache_radix_evictions_total", "counter", "Total radix prompt cache node evictions", radix_evictions);
    try appendPromMetric(writer, "antfly_inference_prompt_cache_radix_cached_nodes", "gauge", "Radix prompt cache retained nodes", radix_cached_nodes);
    try appendPromMetric(writer, "antfly_inference_prompt_cache_radix_collision_guards_total", "counter", "Total radix edge hash collisions detected", radix_collision_guards);
}

fn aggregateResidentProjectionStats(models: anytype) embedding_mod.ResidentProjectionStats {
    var aggregate = embedding_mod.ResidentProjectionStats{};
    var it = models.iterator();
    while (it.next()) |entry| {
        aggregate.add(entry.value_ptr.*.resident_projection_stats.snapshot());
    }
    return aggregate;
}

test "resident projection metrics render counters" {
    var writer: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer writer.deinit();

    try appendResidentProjectionMetrics(&writer.writer, .{
        .text_success = 1,
        .text_fallback = 2,
        .image_success = 3,
        .image_fallback = 4,
        .audio_success = 5,
        .audio_fallback = 6,
    });
    const output = writer.writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_inference_embed_resident_projection_text_success_total 1\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_inference_embed_resident_projection_audio_fallback_total 6\n") != null);
}

test "Metal exact JIT metrics render loaded-session gauges" {
    var writer: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer writer.deinit();
    try appendMetalExactJitMetrics(&writer.writer, .{
        .q4_0_hits = 7,
        .q4_k_hits = 9,
    });
    const output = writer.writer.buffered();
    try std.testing.expect(std.mem.containsAtLeast(
        u8,
        output,
        1,
        "# TYPE antfly_inference_metal_jit_exact_q4_0_hits gauge\nantfly_inference_metal_jit_exact_q4_0_hits 7",
    ));
    try std.testing.expect(std.mem.containsAtLeast(
        u8,
        output,
        1,
        "# TYPE antfly_inference_metal_jit_exact_q4_k_hits gauge\nantfly_inference_metal_jit_exact_q4_k_hits 9",
    ));
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_inference_metal_jit_exact_q4_0_hits_total") == null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_inference_metal_jit_exact_q4_k_hits_total") == null);
}

test "graph executor metrics render counters" {
    var writer: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer writer.deinit();

    try appendGraphExecutorMetrics(&writer.writer, .{
        .partitions_executed = 1,
        .interpreter_fallbacks = 2,
        .host_materialized_outputs = 3,
        .quant_kernel_planned_ops = 4,
        .quant_kernel_handwritten_production = 5,
        .quant_kernel_generated_production = 6,
        .quant_kernel_unsupported_routes = 7,
        .quant_kernel_generated_candidates = 8,
        .quant_kernel_fallback_generated_artifact_missing = 9,
        .quant_kernel_fallback_generated_runtime_not_wired = 10,
        .quant_kernel_fallback_unsupported_format = 10,
        .quant_kernel_fallback_unsupported_shape = 11,
        .quant_kernel_fallback_unsupported_epilogue = 12,
        .quant_kernel_fallback_unsupported_backend = 13,
        .quant_kernel_fallback_tensor_core_repack_required = 14,
        .quant_kernel_fallback_unsupported = 15,
    });
    const output = writer.writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, output, "inference_graph_executor_partitions_total 1\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "inference_graph_executor_interpreter_fallbacks_total 2\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "inference_graph_executor_host_materialized_outputs_total 3\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "inference_quant_kernel_planned_ops_total 4\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "inference_quant_kernel_handwritten_production_total 5\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "inference_quant_kernel_generated_production_total 6\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "inference_quant_kernel_unsupported_routes_total 7\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "inference_quant_kernel_fast_path_misses_total 79\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "inference_quant_kernel_generated_candidates_total 8\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "inference_quant_kernel_fallback_generated_artifact_missing_total 9\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "inference_quant_kernel_fallback_generated_runtime_not_wired_total 10\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "inference_quant_kernel_fallback_unsupported_format_total 10\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "inference_quant_kernel_fallback_unsupported_shape_total 11\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "inference_quant_kernel_fallback_unsupported_epilogue_total 12\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "inference_quant_kernel_fallback_unsupported_backend_total 13\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "inference_quant_kernel_fallback_tensor_core_repack_required_total 14\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "inference_quant_kernel_fallback_unsupported_total 15\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "inference_quant_kernel_top_fallback_reason{reason=\"tensor_core_repack_required\"} 14\n") != null);
}

test "OpenAI model listing deduplicates multi-task models" {
    var output = std.ArrayListUnmanaged(u8).empty;
    defer output.deinit(std.testing.allocator);
    var seen = std.StringHashMapUnmanaged(void).empty;
    defer seen.deinit(std.testing.allocator);
    var count: usize = 0;

    try appendUniqueOpenAiModelEntry(
        &output,
        std.testing.allocator,
        &seen,
        &count,
        "antflydb/clipclap",
        17,
        "compatible",
    );
    try appendUniqueOpenAiModelEntry(
        &output,
        std.testing.allocator,
        &seen,
        &count,
        "antflydb/clipclap",
        17,
        "compatible",
    );

    try std.testing.expectEqual(@as(usize, 1), count);
    try std.testing.expectEqual(
        @as(usize, 1),
        std.mem.count(u8, output.items, "\"id\":\"antflydb/clipclap\""),
    );
}

fn taskMatchesModelListing(
    task: []const u8,
    model_kind: []const u8,
    gliner_model_type: []const u8,
    tasks: []const []const u8,
    capabilities: []const []const u8,
    zero_shot_classification: bool,
) bool {
    // Classification is a public extraction capability. Keep `classifier` as
    // an internal pipeline kind without making its cache bucket a public API.
    if (std.mem.eql(u8, task, "classifiers")) return false;
    if (std.mem.eql(u8, task, "extractors") and
        model_caps.modelSupportsCapability(model_kind, gliner_model_type, capabilities, "classification"))
    {
        return !std.mem.eql(u8, model_kind, "classifier") or zero_shot_classification;
    }
    if (tasks.len > 0) {
        const singular_task: ?[]const u8 = if (std.mem.eql(u8, task, "embedders"))
            "embed"
        else if (std.mem.eql(u8, task, "rerankers"))
            "rerank"
        else if (std.mem.eql(u8, task, "chunkers"))
            "chunk"
        else if (std.mem.eql(u8, task, "generators"))
            "generate"
        else if (std.mem.eql(u8, task, "classifiers"))
            "classify"
        else if (std.mem.eql(u8, task, "rewriters"))
            "rewrite"
        else if (std.mem.eql(u8, task, "readers"))
            "read"
        else if (std.mem.eql(u8, task, "transcribers"))
            "transcribe"
        else if (std.mem.eql(u8, task, "extractors"))
            "extract"
        else
            null;

        if (singular_task) |expected| {
            for (tasks) |candidate| {
                if (std.mem.eql(u8, candidate, expected)) return true;
            }
        }
        return false;
    }
    if (task.len > 0 and std.mem.eql(u8, task[0 .. task.len - 1], model_kind)) return true;
    return std.mem.eql(u8, task, "extractors") and
        model_caps.modelSupportsCapability(model_kind, gliner_model_type, capabilities, "extraction");
}

fn appendModelInfo(
    buf: *std.ArrayListUnmanaged(u8),
    allocator: std.mem.Allocator,
    model_kind: []const u8,
    gliner_model_type: []const u8,
    capabilities: []const []const u8,
    inputs: []const []const u8,
    zero_shot_classification: bool,
    has_visual: bool,
    has_audio: bool,
    /// Set when the model shipped a chat template we could not parse. Without this the
    /// degradation to raw prompting is invisible to API clients.
    chat_template_failed: bool,
    /// Artifact compatibility derived for the current build.
    compatibility_level: []const u8,
) !void {
    const inferred_classification = model_caps.modelSupportsCapability(model_kind, gliner_model_type, capabilities, "classification") and !model_caps.hasCapability(capabilities, "classification");
    const inferred_zero_shot = zero_shot_classification and !model_caps.hasCapability(capabilities, "zero_shot");
    const inferred_multi_label = zero_shot_classification and !model_caps.hasCapability(capabilities, "multi_label");
    const inferred_relations = model_caps.modelSupportsCapability(model_kind, gliner_model_type, capabilities, "relations") and !model_caps.hasCapability(capabilities, "relations");
    const inferred_extraction = model_caps.modelSupportsCapability(model_kind, gliner_model_type, capabilities, "extraction") and !model_caps.hasCapability(capabilities, "extraction");
    const has_known_inputs = model_caps.modelKindAcceptsInput(model_kind, gliner_model_type, inputs, has_visual, has_audio, "text") or
        model_caps.modelKindAcceptsInput(model_kind, gliner_model_type, inputs, has_visual, has_audio, "image") or
        model_caps.modelKindAcceptsInput(model_kind, gliner_model_type, inputs, has_visual, has_audio, "audio");

    if (capabilities.len == 0 and !inferred_classification and !inferred_zero_shot and !inferred_multi_label and !inferred_relations and !inferred_extraction and !has_known_inputs) {
        if (!chat_template_failed and compatibility_level.len == 0) {
            try buf.appendSlice(allocator, "{}");
            return;
        }
        try buf.append(allocator, '{');
        if (chat_template_failed) try buf.appendSlice(allocator, "\"chat_template\":false");
        if (compatibility_level.len > 0) {
            if (chat_template_failed) try buf.append(allocator, ',');
            try buf.appendSlice(allocator, "\"compatibility\":");
            try jsonEncodeString(buf, allocator, compatibility_level);
        }
        try buf.append(allocator, '}');
        return;
    }

    try buf.appendSlice(allocator, "{\"capabilities\":[");
    var cap_index: usize = 0;
    for (capabilities) |cap| {
        if (cap_index > 0) try buf.append(allocator, ',');
        try jsonEncodeString(buf, allocator, cap);
        cap_index += 1;
    }
    if (inferred_classification) {
        if (cap_index > 0) try buf.append(allocator, ',');
        try jsonEncodeString(buf, allocator, "classification");
        cap_index += 1;
    }
    if (inferred_zero_shot) {
        if (cap_index > 0) try buf.append(allocator, ',');
        try jsonEncodeString(buf, allocator, "zero_shot");
        cap_index += 1;
    }
    if (inferred_multi_label) {
        if (cap_index > 0) try buf.append(allocator, ',');
        try jsonEncodeString(buf, allocator, "multi_label");
        cap_index += 1;
    }
    if (inferred_relations) {
        if (cap_index > 0) try buf.append(allocator, ',');
        try jsonEncodeString(buf, allocator, "relations");
        cap_index += 1;
    }
    if (inferred_extraction) {
        if (cap_index > 0) try buf.append(allocator, ',');
        try jsonEncodeString(buf, allocator, "extraction");
        cap_index += 1;
    }
    try buf.appendSlice(allocator, "],\"inputs\":[");
    var input_index: usize = 0;
    for ([_][]const u8{ "text", "image", "audio" }) |input| {
        if (!model_caps.modelKindAcceptsInput(model_kind, gliner_model_type, inputs, has_visual, has_audio, input)) continue;
        if (input_index > 0) try buf.append(allocator, ',');
        try jsonEncodeString(buf, allocator, input);
        input_index += 1;
    }
    try buf.append(allocator, ']');
    if (chat_template_failed) try buf.appendSlice(allocator, ",\"chat_template\":false");
    if (compatibility_level.len > 0) {
        try buf.appendSlice(allocator, ",\"compatibility\":");
        try jsonEncodeString(buf, allocator, compatibility_level);
    }
    try buf.append(allocator, '}');
}

const InferenceHttpRouteAdmission = enum { none, inference };

/// Every generated inference route must make an explicit admission decision.
/// Because the generated router supplies comptime paths, an unclassified new
/// route is a build error rather than a silent admission bypass.
fn inferenceHttpRouteAdmission(comptime method: []const u8, comptime path: []const u8) InferenceHttpRouteAdmission {
    if (comptime std.mem.eql(u8, method, "GET")) {
        if (comptime std.mem.eql(u8, path, "/models") or std.mem.eql(u8, path, "/predictors")) return .none;
    } else if (comptime std.mem.eql(u8, method, "POST")) {
        if (comptime std.mem.eql(u8, path, "/chat/completions") or
            std.mem.eql(u8, path, "/chunk") or
            std.mem.eql(u8, path, "/embed") or
            std.mem.eql(u8, path, "/embeddings") or
            std.mem.eql(u8, path, "/extract") or
            std.mem.eql(u8, path, "/generate") or
            std.mem.eql(u8, path, "/generate/batch") or
            std.mem.eql(u8, path, "/predict") or
            std.mem.eql(u8, path, "/read") or
            std.mem.eql(u8, path, "/rerank") or
            std.mem.eql(u8, path, "/rerank_multimodal") or
            std.mem.eql(u8, path, "/rewrite") or
            std.mem.eql(u8, path, "/transcribe")) return .inference;
    }
    @compileError(std.fmt.comptimePrint("unclassified inference HTTP route: {s} {s}", .{ method, path }));
}

fn admittedInferenceHandler(node: *Node, handler: httpx.Handler, ctx: *httpx.Context) anyerror!httpx.Response {
    if (!node.tryAcquireRequestSlot()) {
        return try transientCapacityFailureResponse(
            ctx,
            "SERVICE_UNAVAILABLE",
            "server at concurrent request capacity, try again later",
            "inference_admission",
        );
    }
    defer node.releaseRequestSlot();
    return handler.invoke(ctx);
}

/// Wrapper that prepends a path prefix and applies the route-owned request
/// permit. Endpoint handlers retain responsibility for weighted work units.
fn PrefixedServer(comptime prefix: []const u8, comptime Inner: type) type {
    return struct {
        inner: *Inner,
        node: *Node,
        models_handler: ?httpx.Handler = null,

        pub fn post(self: *const @This(), comptime path: []const u8, handler: httpx.Handler) !void {
            comptime std.debug.assert(inferenceHttpRouteAdmission("POST", path) == .inference);
            try self.inner.post(prefix ++ path, httpx.Handler.wrap(
                self.node,
                handler,
                admittedInferenceHandler,
            ));
        }

        pub fn get(self: *const @This(), comptime path: []const u8, handler: httpx.Handler) !void {
            comptime std.debug.assert(inferenceHttpRouteAdmission("GET", path) == .none);
            const route_handler = if (comptime std.mem.eql(u8, path, "/models"))
                self.models_handler orelse handler
            else
                handler;
            try self.inner.get(prefix ++ path, route_handler);
        }

        pub fn put(self: *const @This(), comptime path: []const u8, handler: httpx.Handler) !void {
            _ = inferenceHttpRouteAdmission("PUT", path);
            try self.inner.put(prefix ++ path, handler);
        }

        pub fn delete(self: *const @This(), comptime path: []const u8, handler: httpx.Handler) !void {
            _ = inferenceHttpRouteAdmission("DELETE", path);
            try self.inner.delete(prefix ++ path, handler);
        }
    };
}

fn isMlOnlyRoute(comptime path: []const u8) bool {
    return std.mem.eql(u8, path, "/predict") or std.mem.eql(u8, path, "/predictors");
}

fn AiPrefixedServer(comptime prefix: []const u8, comptime Inner: type) type {
    return struct {
        inner: *Inner,
        node: *Node,

        pub fn post(self: *const @This(), comptime path: []const u8, handler: httpx.Handler) !void {
            if (comptime isMlOnlyRoute(path)) return;
            comptime std.debug.assert(inferenceHttpRouteAdmission("POST", path) == .inference);
            try self.inner.post(prefix ++ path, httpx.Handler.wrap(
                self.node,
                handler,
                admittedInferenceHandler,
            ));
        }

        pub fn get(self: *const @This(), comptime path: []const u8, handler: httpx.Handler) !void {
            if (comptime isMlOnlyRoute(path)) return;
            comptime std.debug.assert(inferenceHttpRouteAdmission("GET", path) == .none);
            try self.inner.get(prefix ++ path, handler);
        }

        pub fn put(self: *const @This(), comptime path: []const u8, handler: httpx.Handler) !void {
            if (comptime isMlOnlyRoute(path)) return;
            _ = inferenceHttpRouteAdmission("PUT", path);
            try self.inner.put(prefix ++ path, handler);
        }

        pub fn delete(self: *const @This(), comptime path: []const u8, handler: httpx.Handler) !void {
            if (comptime isMlOnlyRoute(path)) return;
            _ = inferenceHttpRouteAdmission("DELETE", path);
            try self.inner.delete(prefix ++ path, handler);
        }
    };
}

const RecordingRouteMethod = enum {
    get,
    post,
    put,
    delete,
};

const RecordingRoute = struct {
    method: RecordingRouteMethod,
    path: []u8,
};

const RecordingServer = struct {
    allocator: std.mem.Allocator,
    routes: std.ArrayListUnmanaged(RecordingRoute) = .empty,

    fn deinit(self: *@This()) void {
        for (self.routes.items) |route| self.allocator.free(route.path);
        self.routes.deinit(self.allocator);
    }

    fn append(self: *@This(), method: RecordingRouteMethod, comptime path: []const u8) !void {
        if (self.hasRoute(method, path)) return error.DuplicateRoute;
        try self.routes.append(self.allocator, .{
            .method = method,
            .path = try self.allocator.dupe(u8, path),
        });
    }

    pub fn get(self: *@This(), comptime path: []const u8, _: anytype) !void {
        try self.append(.get, path);
    }

    pub fn post(self: *@This(), comptime path: []const u8, _: anytype) !void {
        try self.append(.post, path);
    }

    pub fn put(self: *@This(), comptime path: []const u8, _: anytype) !void {
        try self.append(.put, path);
    }

    pub fn delete(self: *@This(), comptime path: []const u8, _: anytype) !void {
        try self.append(.delete, path);
    }

    fn hasRoute(self: *const @This(), method: RecordingRouteMethod, path: []const u8) bool {
        for (self.routes.items) |route| {
            if (route.method == method and std.mem.eql(u8, route.path, path)) return true;
        }
        return false;
    }

    fn routeCount(self: *const @This(), method: RecordingRouteMethod, path: []const u8) usize {
        var count: usize = 0;
        for (self.routes.items) |route| {
            if (route.method == method and std.mem.eql(u8, route.path, path)) count += 1;
        }
        return count;
    }
};

/// Check if an entity label matches a schema field name.
/// Case-insensitive comparison; also matches if label contains the schema name.
fn labelMatchesSchema(label: []const u8, schema_name: []const u8) bool {
    if (label.len == 0 or schema_name.len == 0) return false;
    // Exact match (case-insensitive)
    if (std.ascii.eqlIgnoreCase(label, schema_name)) return true;
    // Label contains schema name (e.g. "B-PERSON" matches "person")
    if (label.len > schema_name.len) {
        // Check suffix after "B-", "I-", etc.
        if (label.len >= 2 and label[1] == '-') {
            if (std.ascii.eqlIgnoreCase(label[2..], schema_name)) return true;
        }
    }
    return false;
}

test "node config accepts shared scraping config" {
    const cfg = NodeConfig{
        .content_security = .{ .block_private_ips = true },
        .s3_credentials = .{ .endpoint = @constCast("s3.amazonaws.com") },
    };
    try std.testing.expectEqual(@as(?bool, true), cfg.content_security.?.block_private_ips);
    try std.testing.expectEqualStrings("s3.amazonaws.com", cfg.s3_credentials.?.endpoint.?);
}

test "generate speculation options validate the HTTP trust boundary" {
    const defaults = try parseGenerateSpeculationOptions(true, null, null, null);
    try std.testing.expectEqual(@as(u32, 4), defaults.k);
    try std.testing.expectEqual(generation.SpeculationPolicy.auto, defaults.policy);
    try std.testing.expectEqual(generation.SpeculationCalibration.none, defaults.calibration);
    var discovered = defaults;
    qualifyAutoDiscoveredDraftSpeculation(&discovered);
    try std.testing.expectEqual(generation.SpeculationCalibration.probe, discovered.calibration);
    const probing = try parseGenerateSpeculationOptions(true, null, null, "probe");
    try std.testing.expectEqual(generation.SpeculationCalibration.probe, probing.calibration);
    var forced = try parseGenerateSpeculationOptions(true, null, "force", null);
    try std.testing.expectEqual(generation.SpeculationCalibration.none, forced.calibration);
    qualifyAutoDiscoveredDraftSpeculation(&forced);
    try std.testing.expectEqual(generation.SpeculationCalibration.none, forced.calibration);
    try std.testing.expect(!shouldResolveDraftModel(.off));
    try std.testing.expect(!generationSpeculationRequested(
        effectiveDraftModelName("draft", .off),
        null,
    ));
    try std.testing.expect(generationSpeculationRequested(
        effectiveDraftModelName("draft", .auto),
        null,
    ));
    try std.testing.expect(generationSpeculationRequested(null, "auto-draft"));
    try std.testing.expectEqual(@as(usize, 7), generateAdmissionUnitsForSpeculation(7, false, .auto));
    try std.testing.expectEqual(@as(usize, 7), generateAdmissionUnitsForSpeculation(7, true, .off));
    try std.testing.expectEqual(@as(usize, 14), generateAdmissionUnitsForSpeculation(7, true, .auto));
    try std.testing.expectEqual(std.math.maxInt(usize), generateAdmissionUnitsForSpeculation(std.math.maxInt(usize), true, .force));
    try std.testing.expectError(error.SpeculationRequiresDraftModel, parseGenerateSpeculationOptions(false, 4, null, null));
    try std.testing.expectError(error.SpeculationRequiresDraftModel, parseGenerateSpeculationOptions(false, null, "auto", null));
    try std.testing.expectError(error.SpeculationRequiresDraftModel, parseGenerateSpeculationOptions(false, null, null, "probe"));
    try std.testing.expectError(error.InvalidSpeculativeK, parseGenerateSpeculationOptions(true, 0, null, null));
    try std.testing.expectError(error.InvalidSpeculativeK, parseGenerateSpeculationOptions(true, 17, null, null));
    try std.testing.expectError(error.InvalidSpeculationPolicy, parseGenerateSpeculationOptions(true, 4, "sometimes", null));
    try std.testing.expectError(error.InvalidSpeculationCalibration, parseGenerateSpeculationOptions(true, 4, null, "calibrate"));
    try std.testing.expectError(error.InvalidSpeculationCalibration, parseGenerateSpeculationOptions(true, 4, null, "unknown"));
}

test "generate admission units conservatively charge active draft requests" {
    const request_json =
        \\{"model":"m","draft_model":"draft","messages":[{"role":"user","content":"hello"}]}
    ;
    var parsed = try std.json.parseFromSlice(api.GenerateRequest, std.testing.allocator, request_json, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();

    try std.testing.expectEqual(@as(usize, 6), Node.estimateGenerateRequestAdmissionUnits(parsed.value, 256));
    var probing = parsed.value;
    probing.speculation_calibration = "probe";
    try std.testing.expectEqual(@as(usize, 6), Node.estimateGenerateRequestAdmissionUnits(probing, 256));
    var off = parsed.value;
    off.speculation_policy = "off";
    try std.testing.expectEqual(@as(usize, 3), Node.estimateGenerateRequestAdmissionUnits(off, 256));
}

test "auto draft admission unwind preserves independent request growth" {
    var reserved_units: usize = 10;
    var auto_draft_extra_units: usize = 2;
    try std.testing.expectEqual(
        @as(usize, 2),
        takeAutoDraftAdmissionExtra(&reserved_units, &auto_draft_extra_units),
    );
    try std.testing.expectEqual(@as(usize, 8), reserved_units);
    try std.testing.expectEqual(@as(usize, 0), auto_draft_extra_units);

    // Repeated degradation is idempotent and cannot consume media admission.
    try std.testing.expectEqual(
        @as(usize, 0),
        takeAutoDraftAdmissionExtra(&reserved_units, &auto_draft_extra_units),
    );
    try std.testing.expectEqual(@as(usize, 8), reserved_units);
}

test "generate numeric options validate narrowing at the HTTP trust boundary" {
    const defaults = try parseGenerateNumericOptions(null, null);
    try std.testing.expectEqual(@as(i32, 256), defaults.max_tokens);
    try std.testing.expectEqual(@as(i32, 0), defaults.top_k);
    try std.testing.expectError(error.InvalidMaxTokens, parseGenerateNumericOptions(0, null));
    try std.testing.expectError(error.InvalidMaxTokens, parseGenerateNumericOptions(std.math.maxInt(i64), null));
    try std.testing.expectError(error.InvalidTopK, parseGenerateNumericOptions(null, -1));
    try std.testing.expectError(error.InvalidTopK, parseGenerateNumericOptions(null, std.math.maxInt(i64)));
}

test "generate sampling options validate the HTTP trust boundary" {
    const defaults = try parseGenerateSamplingOptions(null, null, null, null, null, null);
    try std.testing.expectEqual(@as(f32, 0), defaults.temperature);
    try std.testing.expectEqual(@as(f32, 0), defaults.top_p);
    try std.testing.expectEqual(@as(f32, 0), defaults.min_p);
    try std.testing.expectEqual(@as(f32, 1), defaults.repetition_penalty);
    try std.testing.expectEqual(@as(f32, 0), defaults.frequency_penalty);
    try std.testing.expectEqual(@as(f32, 0), defaults.presence_penalty);

    try std.testing.expectError(error.InvalidTemperature, parseGenerateSamplingOptions(std.math.inf(f32), null, null, null, null, null));
    try std.testing.expectError(error.InvalidTemperature, parseGenerateSamplingOptions(2.01, null, null, null, null, null));
    try std.testing.expectError(error.InvalidTopP, parseGenerateSamplingOptions(null, std.math.nan(f32), null, null, null, null));
    try std.testing.expectError(error.InvalidTopP, parseGenerateSamplingOptions(null, 1.01, null, null, null, null));
    try std.testing.expectError(error.InvalidMinP, parseGenerateSamplingOptions(null, null, -0.01, null, null, null));
    try std.testing.expectError(error.InvalidRepetitionPenalty, parseGenerateSamplingOptions(null, null, null, 0, null, null));
    try std.testing.expectError(error.InvalidFrequencyPenalty, parseGenerateSamplingOptions(null, null, null, null, -2.01, null));
    try std.testing.expectError(error.InvalidPresencePenalty, parseGenerateSamplingOptions(null, null, null, null, null, 2.01));
}

test "generation SSE usage chunk serializes authoritative accounting" {
    const allocator = std.testing.allocator;
    const payload = try std.json.Stringify.valueAlloc(allocator, api.GenerateChunk{
        .id = "chatcmpl-test",
        .object = "chat.completion.chunk",
        .created = 42,
        .model = "gemma-4",
        .choices = &.{},
        .usage = .{ .value = tokenUsageWithCachedPrompt(2003, 300, 512) },
    }, .{});
    defer allocator.free(payload);

    var parsed = try std.json.parseFromSlice(api.GenerateChunk, allocator, payload, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 0), parsed.value.choices.len);
    const usage = parsed.value.usage.valueOrNull() orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(i64, 2003), usage.prompt_tokens);
    try std.testing.expectEqual(@as(i64, 300), usage.completion_tokens);
    try std.testing.expectEqual(@as(i64, 2303), usage.total_tokens);
    try std.testing.expectEqual(@as(?i64, 512), usage.cached_prompt_tokens);

    const uncached = tokenUsageWithCachedPrompt(7, 3, 0);
    try std.testing.expectEqual(@as(?i64, null), uncached.cached_prompt_tokens);
}

test "generation SSE orders finish then usage then done" {
    const allocator = std.testing.allocator;
    const Capture = struct {
        allocator: std.mem.Allocator,
        events: std.ArrayListUnmanaged([]u8) = .empty,

        fn deinit(self: *@This()) void {
            for (self.events.items) |event| self.allocator.free(event);
            self.events.deinit(self.allocator);
        }

        fn writeEvent(self: *@This(), event_name: ?[]const u8, data: []const u8) !void {
            try std.testing.expect(event_name == null);
            const owned = try self.allocator.dupe(u8, data);
            errdefer self.allocator.free(owned);
            try self.events.append(self.allocator, owned);
        }
    };

    var capture = Capture{ .allocator = allocator };
    defer capture.deinit();
    var payload: std.Io.Writer.Allocating = .init(allocator);
    defer payload.deinit();
    try Node.emitFinishDelta(&capture, &payload, "chatcmpl-test", 42, "gemma-4", "length", null);
    try Node.emitTerminalUsageAndDone(&capture, &payload, "chatcmpl-test", 42, "gemma-4", 2003, 300, 0);

    try std.testing.expectEqual(@as(usize, 3), capture.events.items.len);
    var finish = try std.json.parseFromSlice(api.GenerateChunk, allocator, capture.events.items[0], .{});
    defer finish.deinit();
    try std.testing.expectEqual(@as(usize, 1), finish.value.choices.len);
    try std.testing.expectEqual(api.FinishReason.length, finish.value.choices[0].finish_reason.?);
    try std.testing.expect(!finish.value.usage.isPresent());

    var usage = try std.json.parseFromSlice(api.GenerateChunk, allocator, capture.events.items[1], .{});
    defer usage.deinit();
    try std.testing.expectEqual(@as(usize, 0), usage.value.choices.len);
    try std.testing.expectEqual(@as(i64, 300), usage.value.usage.valueOrNull().?.completion_tokens);
    try std.testing.expectEqualStrings("[DONE]", capture.events.items[2]);
}

test "generation SSE omits usage unless requested" {
    const allocator = std.testing.allocator;
    const Capture = struct {
        allocator: std.mem.Allocator,
        events: std.ArrayListUnmanaged([]u8) = .empty,

        fn deinit(self: *@This()) void {
            for (self.events.items) |event| self.allocator.free(event);
            self.events.deinit(self.allocator);
        }

        fn writeEvent(self: *@This(), event_name: ?[]const u8, data: []const u8) !void {
            try std.testing.expect(event_name == null);
            const owned = try self.allocator.dupe(u8, data);
            errdefer self.allocator.free(owned);
            try self.events.append(self.allocator, owned);
        }
    };

    var capture = Capture{ .allocator = allocator };
    defer capture.deinit();
    var payload: std.Io.Writer.Allocating = .init(allocator);
    defer payload.deinit();
    try Node.emitFinishDelta(&capture, &payload, "chatcmpl-test", 42, "gemma-4", "length", null);
    try Node.emitTerminalUsageOrDone(&capture, &payload, "chatcmpl-test", 42, "gemma-4", 2003, 300, 0, false);

    try std.testing.expectEqual(@as(usize, 2), capture.events.items.len);
    try std.testing.expectEqualStrings("[DONE]", capture.events.items[1]);
}

test "generate ignore eos defaults false and requires explicit true" {
    const allocator = std.testing.allocator;
    const omitted_json =
        \\{"model":"m","messages":[{"role":"user","content":"hello"}]}
    ;
    var omitted = try std.json.parseFromSlice(api.GenerateRequest, allocator, omitted_json, .{ .ignore_unknown_fields = true });
    defer omitted.deinit();
    try std.testing.expect(omitted.value.ignore_eos == null);
    var omitted_grammar: ?[]u8 = null;
    const omitted_config = try Node.generateConfigFromBody(allocator, omitted.value, &omitted_grammar);
    defer if (omitted_grammar) |grammar| allocator.free(grammar);
    try std.testing.expect(!omitted_config.ignore_eos);

    const false_json =
        \\{"model":"m","ignore_eos":false,"messages":[{"role":"user","content":"hello"}]}
    ;
    var explicit_false = try std.json.parseFromSlice(api.GenerateRequest, allocator, false_json, .{ .ignore_unknown_fields = true });
    defer explicit_false.deinit();
    try std.testing.expectEqual(@as(?bool, false), explicit_false.value.ignore_eos);
    var false_grammar: ?[]u8 = null;
    const false_config = try Node.generateConfigFromBody(allocator, explicit_false.value, &false_grammar);
    defer if (false_grammar) |grammar| allocator.free(grammar);
    try std.testing.expect(!false_config.ignore_eos);

    const true_json =
        \\{"model":"m","ignore_eos":true,"messages":[{"role":"user","content":"hello"}]}
    ;
    var explicit_true = try std.json.parseFromSlice(api.GenerateRequest, allocator, true_json, .{ .ignore_unknown_fields = true });
    defer explicit_true.deinit();
    try std.testing.expectEqual(@as(?bool, true), explicit_true.value.ignore_eos);
    var true_grammar: ?[]u8 = null;
    const true_config = try Node.generateConfigFromBody(allocator, explicit_true.value, &true_grammar);
    defer if (true_grammar) |grammar| allocator.free(grammar);
    try std.testing.expect(true_config.ignore_eos);
}

test "generate chat template kwargs preserve default and parse explicit thinking control" {
    const allocator = std.testing.allocator;
    const omitted_json =
        \\{"model":"m","messages":[{"role":"user","content":"hello"}]}
    ;
    var omitted = try std.json.parseFromSlice(api.GenerateRequest, allocator, omitted_json, .{ .ignore_unknown_fields = true });
    defer omitted.deinit();
    try std.testing.expect(omitted.value.chat_template_kwargs == null);
    var omitted_grammar: ?[]u8 = null;
    const omitted_config = try Node.generateConfigFromBody(allocator, omitted.value, &omitted_grammar);
    defer if (omitted_grammar) |grammar| allocator.free(grammar);
    try std.testing.expect(omitted_config.enable_thinking == null);

    const disabled_json =
        \\{"model":"m","chat_template_kwargs":{"enable_thinking":false},"messages":[{"role":"user","content":"hello"}]}
    ;
    var disabled = try std.json.parseFromSlice(api.GenerateRequest, allocator, disabled_json, .{ .ignore_unknown_fields = true });
    defer disabled.deinit();
    try std.testing.expectEqual(@as(?bool, false), disabled.value.chat_template_kwargs.?.enable_thinking);
    var disabled_grammar: ?[]u8 = null;
    const disabled_config = try Node.generateConfigFromBody(allocator, disabled.value, &disabled_grammar);
    defer if (disabled_grammar) |grammar| allocator.free(grammar);
    try std.testing.expectEqual(@as(?bool, false), disabled_config.enable_thinking);

    try std.testing.expect(rawGenerateChatTemplateKwargsAreValid(allocator, omitted_json, false));
    try std.testing.expect(rawGenerateChatTemplateKwargsAreValid(allocator, disabled_json, false));
    try std.testing.expect(rawGenerateChatTemplateKwargsAreValid(
        allocator,
        \\{"model":"m","chat_template_kwargs":{"enable_thinking":true},"messages":[]}
    ,
        false,
    ));
    try std.testing.expect(!rawGenerateChatTemplateKwargsAreValid(
        allocator,
        \\{"model":"m","chat_template_kwargs":{"enable_thinkng":false},"messages":[]}
    ,
        false,
    ));
    try std.testing.expect(!rawGenerateChatTemplateKwargsAreValid(
        allocator,
        \\{"model":"m","chat_template_kwargs":{"enable_thinking":null},"messages":[]}
    ,
        false,
    ));
    try std.testing.expect(!rawGenerateChatTemplateKwargsAreValid(
        allocator,
        \\{"requests":[{"custom_id":"one","body":{"model":"m","chat_template_kwargs":{"unknown":false},"messages":[]}}]}
    ,
        true,
    ));
    // Bodies without the field name skip the Value parse entirely via the
    // substring pre-gate and validate successfully.
    try std.testing.expect(rawGenerateChatTemplateKwargsAreValid(
        allocator,
        \\{"model":"m","messages":[{"role":"user","content":"no kwargs here"}]}
    ,
        false,
    ));
    // The field name appearing only inside a string value is a pre-gate false
    // positive: the full parse still runs and the request remains accepted.
    try std.testing.expect(rawGenerateChatTemplateKwargsAreValid(
        allocator,
        \\{"model":"m","messages":[{"role":"user","content":"mention chat_template_kwargs in text"}]}
    ,
        false,
    ));
    // ... and a real invalid field alongside such a string is still rejected.
    try std.testing.expect(!rawGenerateChatTemplateKwargsAreValid(
        allocator,
        \\{"model":"m","chat_template_kwargs":{"unknown":true},"messages":[{"role":"user","content":"chat_template_kwargs"}]}
    ,
        false,
    ));

    // The single-parse request path validates the untyped Value and then
    // builds the typed request from that same Value instead of re-parsing
    // the raw body.
    var parsed_value = try std.json.parseFromSlice(std.json.Value, allocator, disabled_json, .{});
    defer parsed_value.deinit();
    try std.testing.expect(generateRequestChatTemplateKwargsAreValid(parsed_value.value));
    var from_value = try std.json.parseFromValue(api.GenerateRequest, allocator, parsed_value.value, .{ .ignore_unknown_fields = true });
    defer from_value.deinit();
    try std.testing.expectEqual(@as(?bool, false), from_value.value.chat_template_kwargs.?.enable_thinking);
    try std.testing.expectEqualStrings("m", from_value.value.model);
    try std.testing.expectEqual(@as(usize, 1), from_value.value.messages.len);
}

test "generate speculation status exposes disabled decisions" {
    const status = generateSpeculationStatus(.{
        .speculation_policy = .auto,
        .speculation_calibration = .none,
        .speculation_policy_decision = .disabled_uncalibrated,
        .mtp_disabled_reason = "speculation_calibration_required",
    }).?;
    try std.testing.expectEqualStrings("auto", status.policy);
    try std.testing.expectEqualStrings("none", status.calibration);
    try std.testing.expectEqualStrings("disabled_uncalibrated", status.decision);
    try std.testing.expectEqualStrings("speculation_calibration_required", status.disabled_reason.valueOrNull().?);

    const unavailable = generateSpeculationStatus(.{ .speculation_policy_decision = .disabled_unavailable }).?;
    try std.testing.expectEqualStrings("draft_backend_unavailable", unavailable.disabled_reason.valueOrNull().?);
}

test "generate policy off has no effective draft and reports disabled" {
    try std.testing.expect(effectiveDraftModelName("draft", .off) == null);
    try std.testing.expectEqualStrings("draft", effectiveDraftModelName("draft", .auto).?);

    const stats = effectiveSpeculationStats(null, true, .off, .none).?;
    try std.testing.expectEqual(generation.SpeculativeDecodeStats.PolicyDecision.disabled_off, stats.speculation_policy_decision);
    try std.testing.expectEqualStrings("speculation_policy_off", stats.mtp_disabled_reason.?);
    try std.testing.expect(effectiveSpeculationStats(null, false, .off, .none) == null);
}

test "qualified-profile server rejects active draft models explicitly" {
    try std.testing.expectError(
        error.KernelJitQualifiedProfileDraftUnsupported,
        validateQualifiedProfileDraft(true, "draft"),
    );
    try validateQualifiedProfileDraft(true, null);
    try validateQualifiedProfileDraft(false, "draft");
}

test "generate auto MTP probe does not require replay" {
    const threshold = generation.gemma4MtpAutoMinGenerationTokens();
    var config: generation.GenerationConfig = .{
        .max_tokens = @intCast(threshold),
        .speculation_policy = .auto,
        .speculation_calibration = .probe,
    };
    const draft_config: gpt_model_mod.Config = .{ .gemma4_mtp_assistant = true };
    try std.testing.expect(!shouldSkipAutoMtpDraftLoad(config, draft_config));

    config.max_tokens = @intCast(threshold - 1);
    try std.testing.expect(shouldSkipAutoMtpDraftLoad(config, draft_config));
    config.max_tokens = @intCast(threshold);
    config.speculation_calibration = .none;
    try std.testing.expect(shouldSkipAutoMtpDraftLoad(config, draft_config));
    config.speculation_policy = .force;
    try std.testing.expect(!shouldSkipAutoMtpDraftLoad(config, draft_config));
}

test "generate draft resources follow the draft backend" {
    try std.testing.expectEqual(@as(?runtime.kv.pool.BackendKind, .native), generationBackendKind(.native));
    try std.testing.expectEqual(@as(?runtime.kv.pool.BackendKind, .metal), generationBackendKind(.metal));
    try std.testing.expectEqual(@as(?runtime.kv.pool.BackendKind, .cuda), generationBackendKind(.cuda));
    try std.testing.expectEqual(@as(?runtime.kv.pool.BackendKind, null), generationBackendKind(.onnx));
    try std.testing.expectEqual(@as(?runtime.kv.pool.BackendKind, null), generationBackendKind(.pjrt));
    try std.testing.expectEqual(@as(?runtime.kv.pool.BackendKind, null), generationBackendKind(.wasm));

    const limits = runtime.tier.memory.maxCompositeLimits(
        .{ .host_limit_bytes = 2, .backend_limit_bytes = 8, .kv_limit_bytes = 3 },
        .{ .host_limit_bytes = 5, .backend_limit_bytes = 4, .combined_limit_bytes = 9, .scratch_limit_bytes = 7 },
    );
    try std.testing.expectEqual(@as(usize, 5), limits.host_limit_bytes);
    try std.testing.expectEqual(@as(usize, 8), limits.backend_limit_bytes);
    try std.testing.expectEqual(@as(usize, 9), limits.combined_limit_bytes);
    try std.testing.expectEqual(@as(usize, 3), limits.kv_limit_bytes);
    try std.testing.expectEqual(@as(usize, 7), limits.scratch_limit_bytes);
}

test "generate draft KV pool uses safe model geometry" {
    const mha = generation.kvPoolConfig(.native, .f16, .{
        .hidden_size = 1024,
        .num_hidden_layers = 2,
        .num_attention_heads = 8,
        .num_key_value_heads = 0,
        .position_encoding = .rope,
        .max_position_embeddings = 4096,
    }, false);
    try std.testing.expectEqual(@as(u32, 8), mha.num_kv_heads);
    try std.testing.expectEqual(@as(u32, 128), mha.head_dim);
    try std.testing.expectEqual(@as(?u32, 4096), mha.sliding_window_size);

    const gemma4 = generation.kvPoolConfig(.metal, .bf16, .{
        .family = .gemma,
        .hidden_size = 512,
        .num_hidden_layers = 2,
        .num_attention_heads = 8,
        .num_key_value_heads = 4,
        .attention_head_dim = 256,
        .num_global_key_value_heads = 8,
        .global_head_dim = 512,
        .position_encoding = .rope,
        .sliding_window = 1024,
    }, false);
    try std.testing.expectEqual(@as(u32, 8), gemma4.num_kv_heads);
    try std.testing.expectEqual(@as(u32, 512), gemma4.head_dim);
    try std.testing.expectEqual(@as(?u32, 1024), gemma4.sliding_window_size);

    const mixed_gemma4_config: gpt_model_mod.Config = .{
        .family = .gemma,
        .hidden_size = 512,
        .num_hidden_layers = 6,
        .num_attention_heads = 8,
        .num_key_value_heads = 4,
        .position_encoding = .rope,
        .sliding_window = 1024,
        .sliding_window_pattern = 6,
    };
    try std.testing.expectEqual(
        @as(?u32, null),
        generation.kvPoolConfig(.metal, .bf16, mixed_gemma4_config, false).sliding_window_size,
    );
    try std.testing.expectEqual(
        @as(?u32, 1024),
        generation.kvPoolConfig(.metal, .bf16, mixed_gemma4_config, true).sliding_window_size,
    );
}

test "generate rejects an ONNX draft before model loading" {
    const onnx = GenerateBackendSelection{ .native_choice = .onnx };
    try std.testing.expectError(error.OnnxDraftUnsupported, validateGenerateDraftBackend(onnx, "draft"));
    try validateGenerateDraftBackend(onnx, null);
    try validateGenerateDraftBackend(.{ .native_choice = .native }, "draft");
}

test "generate speculation outcome metrics classify completed requests" {
    var metrics = metrics_mod.Metrics.default;
    recordSpeculationOutcome(&metrics, .{ .speculation_policy_decision = .active });
    recordSpeculationOutcome(&metrics, .{ .speculation_policy_decision = .disabled_off });
    recordSpeculationOutcome(&metrics, null);

    var writer: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer writer.deinit();
    try metrics.render(&writer.writer);
    const output = writer.writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_inference_speculation_active_total 1\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_inference_speculation_disabled_total 1\n") != null);
}

test "generate cache compaction ratio validates the HTTP trust boundary" {
    try validateCacheCompactionRatio(null);
    try validateCacheCompactionRatio(0.01);
    try validateCacheCompactionRatio(1.0);
    try std.testing.expectError(error.InvalidCompactionRatio, validateCacheCompactionRatio(0.0));
    try std.testing.expectError(error.InvalidCompactionRatio, validateCacheCompactionRatio(-0.1));
    try std.testing.expectError(error.InvalidCompactionRatio, validateCacheCompactionRatio(1.01));
    try std.testing.expectError(error.InvalidCompactionRatio, validateCacheCompactionRatio(std.math.nan(f32)));
    try std.testing.expectError(error.InvalidCompactionRatio, validateCacheCompactionRatio(std.math.inf(f32)));
}

test "generate config grammar ownership stays flat on success override and later error" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer std.debug.assert(gpa.deinit() == .ok);
    const allocator = gpa.allocator();
    const request_json =
        \\{"model":"m","messages":[],"response_format":{"type":"json_schema","json_schema":{"schema":{"type":"object","properties":{"answer":{"type":"string"}},"required":["answer"],"additionalProperties":false}}}}
    ;
    var parsed = try std.json.parseFromSlice(api.GenerateRequest, allocator, request_json, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();

    for (0..64) |_| {
        var owned_grammar: ?[]u8 = null;
        const config = try Node.generateConfigFromBody(allocator, parsed.value, &owned_grammar);
        try std.testing.expect(owned_grammar != null);
        try std.testing.expectEqualStrings(owned_grammar.?, config.grammar.?);
        allocator.free(owned_grammar.?);

        var override_body = parsed.value;
        override_body.grammar = "json";
        var override_owned: ?[]u8 = null;
        const override_config = try Node.generateConfigFromBody(allocator, override_body, &override_owned);
        try std.testing.expect(override_owned == null);
        try std.testing.expectEqualStrings("json", override_config.grammar.?);

        var invalid_body = parsed.value;
        invalid_body.grammar = "foo ::= \"bar\"";
        var invalid_owned: ?[]u8 = null;
        try std.testing.expectError(
            error.NoRootRule,
            Node.generateConfigFromBody(allocator, invalid_body, &invalid_owned),
        );
        try std.testing.expect(invalid_owned == null);
    }
}

test "generate batch rejects draft models instead of ignoring them" {
    const request_json =
        \\{"model":"m","draft_model":"draft","messages":[{"role":"user","content":"hello"}]}
    ;
    var parsed = try std.json.parseFromSlice(api.GenerateRequest, std.testing.allocator, request_json, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();

    const reason = Node.generateBatchUnsupportedReasonPreflight(parsed.value) orelse return error.TestExpectedEqual;
    try std.testing.expectEqualStrings("UNSUPPORTED_DRAFT_MODEL", reason.code);
}

test "generate batch validates sampling and speculative options before execution" {
    const invalid_sampling_json =
        \\{"model":"m","temperature":2.1,"messages":[{"role":"user","content":"hello"}]}
    ;
    var invalid_sampling = try std.json.parseFromSlice(api.GenerateRequest, std.testing.allocator, invalid_sampling_json, .{ .ignore_unknown_fields = true });
    defer invalid_sampling.deinit();
    const sampling_reason = Node.generateBatchUnsupportedReasonPreflight(invalid_sampling.value) orelse return error.TestExpectedEqual;
    try std.testing.expectEqualStrings("INVALID_REQUEST", sampling_reason.code);
    try std.testing.expectEqualStrings("temperature must be finite and between 0 and 2", sampling_reason.message);

    const invalid_speculation_json =
        \\{"model":"m","speculative_k":4,"messages":[{"role":"user","content":"hello"}]}
    ;
    var invalid_speculation = try std.json.parseFromSlice(api.GenerateRequest, std.testing.allocator, invalid_speculation_json, .{ .ignore_unknown_fields = true });
    defer invalid_speculation.deinit();
    const speculation_reason = Node.generateBatchUnsupportedReasonPreflight(invalid_speculation.value) orelse return error.TestExpectedEqual;
    try std.testing.expectEqualStrings("INVALID_REQUEST", speculation_reason.code);
    try std.testing.expectEqualStrings("speculative_k, speculation_policy, and speculation_calibration require draft_model", speculation_reason.message);
}

test "generate batch rejects cache compaction clearly" {
    const unsupported_json =
        \\{"model":"m","cache_compaction_ratio":0.5,"messages":[{"role":"user","content":"hello"}]}
    ;
    var unsupported = try std.json.parseFromSlice(api.GenerateRequest, std.testing.allocator, unsupported_json, .{ .ignore_unknown_fields = true });
    defer unsupported.deinit();
    const unsupported_reason = Node.generateBatchUnsupportedReasonPreflight(unsupported.value) orelse return error.TestExpectedEqual;
    try std.testing.expectEqualStrings("UNSUPPORTED_CACHE_COMPACTION", unsupported_reason.code);

    const invalid_json =
        \\{"model":"m","cache_compaction_ratio":0,"messages":[{"role":"user","content":"hello"}]}
    ;
    var invalid = try std.json.parseFromSlice(api.GenerateRequest, std.testing.allocator, invalid_json, .{ .ignore_unknown_fields = true });
    defer invalid.deinit();
    const invalid_reason = Node.generateBatchUnsupportedReasonPreflight(invalid.value) orelse return error.TestExpectedEqual;
    try std.testing.expectEqualStrings("INVALID_CACHE_COMPACTION_RATIO", invalid_reason.code);
}

test "generate batch preflight rejects image content without parsing media" {
    const request_json =
        \\{"model":"m","messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"https://example.invalid/image.png"}}]}]}
    ;
    var parsed = try std.json.parseFromSlice(api.GenerateRequest, std.testing.allocator, request_json, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();

    const reason = Node.generateBatchUnsupportedReasonPreflight(parsed.value) orelse return error.TestExpectedEqual;
    try std.testing.expectEqualStrings("UNSUPPORTED_MULTIMODAL", reason.code);
}

test "generate batch isolates native execution and serializes stateful GPU backends" {
    try std.testing.expectEqual(
        Node.BatchExecutionMode.isolated_parallel,
        Node.batchExecutionMode(.native),
    );
    try std.testing.expectEqual(
        Node.BatchExecutionMode.shared_serial,
        Node.batchExecutionMode(.metal),
    );
    try std.testing.expectEqual(
        Node.BatchExecutionMode.shared_serial,
        Node.batchExecutionMode(.cuda),
    );
}

test "generate batch admission units sum pending generation work" {
    const alloc = std.testing.allocator;
    var node = try Node.init(alloc, .{});
    defer node.deinit();

    const request_json =
        \\{"mode":"sync","requests":[
        \\{"custom_id":"a","body":{"model":"m","max_tokens":512,"messages":[{"role":"user","content":"short"}]}},
        \\{"custom_id":"b","body":{"model":"m","max_tokens":256,"messages":[{"role":"user","content":"this prompt is deliberately longer than one prompt admission block so the estimate is weighted"}]}},
        \\{"custom_id":"c","body":{"model":"m","max_tokens":256,"messages":[{"role":"user","content":"already rejected"}]}}
        \\]}
    ;
    var parsed = try std.json.parseFromSlice(api.GenerateBatchRequest, alloc, request_json, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();

    var owned_messages = try alloc.alloc(Node.OwnedGenerateMessages, parsed.value.requests.len);
    defer {
        for (owned_messages) |*owned| owned.deinit();
        alloc.free(owned_messages);
    }
    for (parsed.value.requests, 0..) |item, idx| {
        owned_messages[idx] = try node.parseGenerateMessages(alloc, item.body);
    }
    const pending = [_]bool{ true, true, false };

    const expected =
        @as(usize, 1) +
        node.estimateGenerateAdmissionUnits(owned_messages[0].messages, 512) +
        node.estimateGenerateAdmissionUnits(owned_messages[1].messages, 256);
    try std.testing.expectEqual(expected, node.estimateGenerateBatchAdmissionUnits(parsed.value.requests, owned_messages, pending[0..]));
}

test "generate batch shared backends own the outer model lock" {
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    var mutex: std.atomic.Mutex = .unlocked;

    var isolated = Node.BatchModelLock.init(.isolated_parallel, &mutex, io_impl.io());
    defer isolated.deinit();
    try std.testing.expectEqual(
        &mutex,
        isolated.pipelineExecutionLock().?,
    );
    try std.testing.expect(mutex.tryLock());
    mutex.unlock();

    var shared = Node.BatchModelLock.init(.shared_serial, &mutex, io_impl.io());
    try std.testing.expect(shared.pipelineExecutionLock() == null);
    try std.testing.expect(!mutex.tryLock());
    shared.deinit();
    try std.testing.expect(mutex.tryLock());
    mutex.unlock();
}

test "serial batch admission covers each request without summing them" {
    const first = runtime.tier.memory.Estimate{
        .prompt_tokens = 100,
        .retained_tokens = 120,
        .kv_bytes = 900,
        .kv_tier = .backend,
        .scratch_bytes = 200,
        .scratch_tier = .backend,
    };
    const second = runtime.tier.memory.Estimate{
        .prompt_tokens = 300,
        .retained_tokens = 320,
        .kv_bytes = 400,
        .kv_tier = .backend,
        .scratch_bytes = 700,
        .scratch_tier = .backend,
    };
    const merged = Node.mergeSerialBatchEstimate(first, second);
    try std.testing.expectEqual(@as(usize, 300), merged.prompt_tokens);
    try std.testing.expectEqual(@as(usize, 320), merged.retained_tokens);
    try std.testing.expectEqual(@as(usize, 900), merged.kv_bytes);
    try std.testing.expectEqual(@as(usize, 700), merged.scratch_bytes);
}

test "direct generation admission owns capacity exactly once" {
    var node = try Node.init(std.testing.allocator, .{ .max_concurrent_requests = 32 });
    defer node.deinit();

    var admission = try node.beginDirectGenerateAdmission(.{
        .text_bytes = 5,
        .encoded_media_bytes = 4,
        .decoded_media_bytes = 3,
        .media_count = 1,
        .has_audio = true,
    }, 256);
    try std.testing.expectEqual(@as(usize, 9), node.inference_admission.inFlightUnits());
    try std.testing.expectEqual(@as(usize, 1), node.inference_admission.inFlightRequests());

    admission.deinit();
    try std.testing.expectEqual(@as(usize, 0), node.inference_admission.inFlightUnits());
    try std.testing.expectEqual(@as(usize, 0), node.inference_admission.inFlightRequests());

    admission.deinit();
    try std.testing.expectEqual(@as(usize, 0), node.inference_admission.inFlightUnits());
    try std.testing.expectEqual(@as(usize, 0), node.inference_admission.inFlightRequests());
}

test "direct generation admission rejects resident media before acquisition" {
    var node = try Node.init(std.testing.allocator, .{
        .content_security = .{ .max_download_size_bytes = 4 },
        .max_concurrent_requests = 32,
    });
    defer node.deinit();

    try std.testing.expectError(
        error.RemoteContentTooLarge,
        node.beginDirectGenerateAdmission(.{
            .encoded_media_bytes = 3,
            .decoded_media_bytes = 2,
            .media_count = 1,
            .image_count = 1,
        }, 256),
    );
    try std.testing.expectEqual(@as(usize, 0), node.inference_admission.inFlightUnits());
    try std.testing.expectEqual(@as(usize, 0), node.inference_admission.inFlightRequests());
}

test "direct generation audio admission honors configured byte capacity boundary" {
    var bounded = try Node.init(std.testing.allocator, .{ .max_concurrent_requests = 8 });
    defer bounded.deinit();

    var exact = try bounded.beginDirectGenerateAdmission(.{
        .media_count = 1,
        .has_audio = true,
    }, 256);
    try std.testing.expectEqual(@as(usize, 8), bounded.inference_admission.inFlightUnits());
    exact.deinit();

    try std.testing.expectError(
        error.AudioTooLarge,
        bounded.beginDirectGenerateAdmission(.{
            .decoded_media_bytes = 1,
            .media_count = 1,
            .has_audio = true,
        }, 256),
    );
    try std.testing.expectEqual(@as(usize, 0), bounded.inference_admission.inFlightUnits());
    try std.testing.expectEqual(@as(usize, 0), bounded.inference_admission.inFlightRequests());

    var unlimited = try Node.init(std.testing.allocator, .{ .max_concurrent_requests = 0 });
    defer unlimited.deinit();
    var admitted = try unlimited.beginDirectGenerateAdmission(.{
        .decoded_media_bytes = 1,
        .media_count = 1,
        .has_audio = true,
    }, 256);
    try std.testing.expectEqual(@as(usize, 0), unlimited.inference_admission.inFlightUnits());
    try std.testing.expectEqual(@as(usize, 1), unlimited.inference_admission.inFlightRequests());
    admitted.deinit();
}

test "direct generation media inspection releases admission on early error" {
    var node = try Node.init(std.testing.allocator, .{ .max_concurrent_requests = 32 });
    defer node.deinit();
    const images = [_][]const u8{"not-an-image"};
    const messages = [_]generation.Message{.{
        .role = "user",
        .content = "describe",
        .image_bytes = &images,
    }};

    try std.testing.expectError(
        error.ImageDecodeFailed,
        node.generateMessagesDirect(std.testing.allocator, "unused", &messages),
    );
    try std.testing.expectEqual(@as(usize, 0), node.inference_admission.inFlightUnits());
    try std.testing.expectEqual(@as(usize, 0), node.inference_admission.inFlightRequests());
}

test "direct generation image admission subtracts resident bytes from capacity" {
    var node = try Node.init(std.testing.allocator, .{ .max_concurrent_requests = 1 });
    defer node.deinit();

    var png = [_]u8{0} ** 24;
    const signature = [_]u8{ 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n' };
    @memcpy(png[0..8], &signature);
    std.mem.writeInt(u32, png[16..20], 1024, .big);
    std.mem.writeInt(u32, png[20..24], 1024, .big);
    const images = [_][]const u8{&png};
    const messages = [_]generation.Message{.{
        .role = "user",
        .content = "describe",
        .image_bytes = &images,
    }};

    try std.testing.expectError(
        error.ImageBatchTooLarge,
        node.generateMessagesDirect(std.testing.allocator, "unused", &messages),
    );
    try std.testing.expectEqual(@as(usize, 0), node.inference_admission.inFlightUnits());
    try std.testing.expectEqual(@as(usize, 0), node.inference_admission.inFlightRequests());
}

test "generate batch capacity errors include actionable retry metadata" {
    const busy_load = Node.batchModelLoadError(error.ResourceTemporarilyUnavailable);
    try std.testing.expectEqualStrings("MODEL_RESOURCE_BUSY", busy_load.code);
    try std.testing.expectEqual(true, busy_load.retryable);
    try std.testing.expectEqual(
        @as(?i64, transient_capacity_retry_after_ms),
        busy_load.retry_after_ms.valueOrNull(),
    );

    const busy_admission = Node.batchAdmissionError(error.ResourceTemporarilyUnavailable);
    try std.testing.expectEqualStrings("MODEL_RESOURCE_BUSY", busy_admission.code);
    try std.testing.expectEqual(true, busy_admission.retryable);
    try std.testing.expectEqual(
        @as(?i64, transient_capacity_retry_after_ms),
        busy_admission.retry_after_ms.valueOrNull(),
    );

    const permanent = Node.batchAdmissionError(error.ResourceLimitExceeded);
    try std.testing.expectEqualStrings("MODEL_RESOURCE_LIMIT", permanent.code);
    try std.testing.expectEqual(false, permanent.retryable);
    try std.testing.expect(!permanent.retry_after_ms.isPresent());
}

test "read batch downloaded byte accounting enforces aggregate cap" {
    const item = scraping.DownloadedContent{
        .content_type = @constCast("image/png"),
        .data = @constCast("12345"),
    };
    try std.testing.expectEqual(@as(usize, 14), try addReadBatchDownloadedBytes(0, item, 14));
    try std.testing.expectError(error.ReadBatchTooLarge, addReadBatchDownloadedBytes(10, item, 14));
}

test "read inline source accounting is cumulative bounded and overflow safe" {
    try std.testing.expectEqual(@as(usize, 5), try addReadInlineSourceBytes(0, "data:", 5));
    try std.testing.expectEqual(@as(usize, 5), try addReadInlineSourceBytes(5, "https://example.invalid/image.png", 5));
    try std.testing.expectError(error.ReadBatchTooLarge, addReadInlineSourceBytes(5, "data:,x", 11));
    try std.testing.expectError(error.ReadBatchTooLarge, addReadInlineSourceBytes(std.math.maxInt(usize), "data:", std.math.maxInt(usize)));
}

test "audio admission accounts resident media plus bounded decode working set" {
    const normal = audioDecodeAdmissionForLimits(32 * 1024 * 1024, 32);
    try std.testing.expectEqual(default_max_audio_decode_working_bytes, normal.max_decode_working_bytes);
    try std.testing.expectEqual(@as(usize, 10), normal.units);

    const small_capacity = audioDecodeAdmissionForLimits(16 * 1024 * 1024, 4);
    try std.testing.expectEqual(@as(usize, 48 * 1024 * 1024), small_capacity.max_decode_working_bytes);
    try std.testing.expectEqual(@as(usize, 4), small_capacity.units);

    const exhausted_capacity = audioDecodeAdmissionForLimits(16 * 1024 * 1024, 1);
    try std.testing.expectEqual(@as(usize, 0), exhausted_capacity.max_decode_working_bytes);
    try std.testing.expectEqual(@as(usize, 1), exhausted_capacity.units);

    const unlimited_admission = audioDecodeAdmissionForLimits(16 * 1024 * 1024, 0);
    try std.testing.expectEqual(default_max_audio_decode_working_bytes, unlimited_admission.max_decode_working_bytes);
    try std.testing.expectEqual(@as(usize, 9), unlimited_admission.units);
}

test "read admission reserves default batch bytes and image pressure without overflow" {
    const batch = readRequestAdmissionForLimits(
        max_read_batch_images,
        default_max_read_batch_bytes,
        default_max_request_media_bytes,
        32,
        null,
        0,
    );
    try std.testing.expectEqual(default_max_read_batch_bytes, batch.byte_cap);
    try std.testing.expectEqual(default_max_read_batch_bytes, batch.resident_byte_cap);
    try std.testing.expectEqual(@as(usize, 32), batch.units);

    const single = readRequestAdmissionForLimits(
        1,
        default_max_read_batch_bytes,
        default_max_request_media_bytes,
        32,
        null,
        0,
    );
    try std.testing.expectEqual(default_max_request_media_bytes, single.byte_cap);
    try std.testing.expectEqual(default_max_request_media_bytes, single.resident_byte_cap);
    try std.testing.expectEqual(@as(usize, 7), single.units);

    const small_capacity = readRequestAdmissionForLimits(
        1,
        default_max_read_batch_bytes,
        default_max_request_media_bytes,
        4,
        null,
        0,
    );
    try std.testing.expectEqual(4 * read_admission_bytes_per_unit, small_capacity.byte_cap);
    try std.testing.expectEqual(4 * read_admission_bytes_per_unit, small_capacity.resident_byte_cap);
    try std.testing.expectEqual(@as(usize, 4), small_capacity.units);

    const overflow = readRequestAdmissionForLimits(
        std.math.maxInt(usize),
        std.math.maxInt(usize),
        std.math.maxInt(usize),
        32,
        null,
        0,
    );
    try std.testing.expectEqual(32 * read_admission_bytes_per_unit, overflow.byte_cap);
    var admission = inference_admission_mod.InferenceAdmission.init(32);
    try std.testing.expectEqual(@as(usize, 32), admission.capacityUnits(overflow.units));

    try std.testing.expectEqual(@as(usize, 16 * 1024 * 1024), batch.decoded_pixel_cap);
    try std.testing.expectEqual(
        (32 * read_admission_bytes_per_unit - default_max_request_media_bytes) / read_decoded_working_bytes_per_pixel,
        single.decoded_pixel_cap,
    );
    try std.testing.expectEqual(@as(usize, 0), small_capacity.decoded_pixel_cap);
    const dimension_limited = readRequestAdmissionForLimits(2, default_max_read_batch_bytes, default_max_request_media_bytes, 32, 2048, 0);
    try std.testing.expectEqual(@as(usize, 2 * 2048 * 2048), dimension_limited.decoded_pixel_cap);
    const unlimited_admission = readRequestAdmissionForLimits(max_read_batch_images, default_max_read_batch_bytes, default_max_request_media_bytes, 0, null, 0);
    try std.testing.expectEqual(default_max_read_decoded_working_bytes / read_decoded_working_bytes_per_pixel, unlimited_admission.decoded_pixel_cap);

    const boundary = readRequestAdmissionForLimits(
        1,
        read_admission_bytes_per_unit,
        read_admission_bytes_per_unit,
        4,
        null,
        read_admission_bytes_per_unit,
    );
    try std.testing.expectEqual(2 * read_admission_bytes_per_unit, boundary.resident_byte_cap);
    try std.testing.expectEqual(@as(usize, 2 * 1024 * 1024), boundary.decoded_pixel_cap);
    var boundary_budget = ReadDecodedImageBudget.init(boundary, null);
    try boundary_budget.addPixels(boundary.decoded_pixel_cap);
    try std.testing.expectEqual(@as(usize, 4), boundary_budget.requiredUnits());
    try std.testing.expectError(error.ImageBatchTooLarge, boundary_budget.addPixels(1));
}

test "remote and inline media reserve distinct resident peaks before download" {
    const allocator = std.testing.allocator;
    const remote_json =
        \\[{"type":"image_url","image_url":{"url":"https://media.example/large.png"}}]
    ;
    var remote_input = try std.json.parseFromSlice(std.json.Value, allocator, remote_json, .{});
    defer remote_input.deinit();
    const remote_shape = denseEmbedRequestMediaShape(remote_input.value);
    try std.testing.expectEqual(@as(usize, 1), remote_shape.image_count);
    try std.testing.expect(remote_shape.has_remote);

    const remote = requestMediaAdmissionForLimits(remote_shape, default_max_request_media_bytes, 32, null);
    try std.testing.expectEqual(default_max_request_media_bytes, remote.byte_cap);
    try std.testing.expectEqual(default_max_request_media_bytes, remote.resident_byte_cap);
    try std.testing.expectEqual(@as(usize, 7), remote.units);

    var admission = inference_admission_mod.InferenceAdmission.init(32);
    for (0..4) |_| try admission.acquireUnits(remote.units);
    try std.testing.expectEqual(@as(usize, 28), admission.inFlightUnits());
    try std.testing.expectError(error.QueueFull, admission.acquireUnits(remote.units));
    for (0..4) |_| admission.releaseUnits(remote.units);
    try std.testing.expectEqual(@as(usize, 0), admission.inFlightUnits());

    const capacity_limited = requestMediaAdmissionForLimits(remote_shape, default_max_request_media_bytes, 1, null);
    try std.testing.expectEqual(read_admission_bytes_per_unit, capacity_limited.byte_cap);
    try std.testing.expectEqual(@as(usize, 1), capacity_limited.units);

    const inline_json =
        \\[{"type":"image_url","image_url":{"url":"data:image/png;base64,AA=="}}]
    ;
    var inline_input = try std.json.parseFromSlice(std.json.Value, allocator, inline_json, .{});
    defer inline_input.deinit();
    const inline_shape = denseEmbedRequestMediaShape(inline_input.value);
    const inline_admission = requestMediaAdmissionForLimits(inline_shape, default_max_request_media_bytes, 32, null);
    try std.testing.expectEqual(@as(usize, "data:image/png;base64,AA==".len), inline_admission.byte_cap);
    try std.testing.expectEqual(@as(usize, 2 * "data:image/png;base64,AA==".len), inline_admission.resident_byte_cap);
    try std.testing.expectEqual(@as(usize, 1), inline_admission.units);
}

test "direct dense embed admission counts borrowed media once" {
    const inline_url = "data:image/png;base64,AA==";
    const parts = [_]Node.DirectDenseEmbedPart{
        .{ .media = .{ .mime_type = "image/png", .data = "abc" } },
        .{ .image_url = inline_url },
        .{ .media = .{ .mime_type = "audio/wav", .data = "de" } },
    };
    const preflight = try directDenseEmbedPreflight(&parts);
    try std.testing.expectEqual(@as(usize, 5), preflight.shape.borrowed_bytes);
    try std.testing.expectEqual(@as(usize, inline_url.len), preflight.shape.inline_bytes);
    try std.testing.expectEqual(@as(usize, 2), preflight.shape.image_count);
    try std.testing.expect(preflight.has_audio);
    try std.testing.expectEqual(@as(usize, 5 + inline_url.len), preflight.known_media_bytes);

    const unlimited = requestMediaAdmissionForLimits(
        preflight.shape,
        default_max_request_media_bytes,
        0,
        null,
    );
    try std.testing.expectEqual(preflight.known_media_bytes, unlimited.byte_cap);
    try std.testing.expectEqual(preflight.known_media_bytes + inline_url.len, unlimited.resident_byte_cap);

    var borrowed_only: RequestMediaAdmissionShape = .{};
    borrowed_only.addBorrowed(8 * 1024 * 1024, true);
    const bounded = requestMediaAdmissionForLimits(borrowed_only, default_max_request_media_bytes, 1, null);
    try std.testing.expectEqual(@as(usize, 8 * 1024 * 1024), bounded.byte_cap);
    try std.testing.expectEqual(bounded.byte_cap, bounded.resident_byte_cap);
    try std.testing.expectEqual(@as(usize, 512 * 1024), bounded.decoded_pixel_cap);

    var saturated: RequestMediaAdmissionShape = .{};
    saturated.addBorrowed(std.math.maxInt(usize), false);
    saturated.addInline(1, false);
    try std.testing.expectEqual(
        default_max_request_media_bytes,
        saturated.potentialBytes(default_max_request_media_bytes),
    );
}

test "direct dense embed parser borrows media and propagates allocator exhaustion" {
    const backing_allocator = std.testing.allocator;
    var raw_image = [_]u8{ 1, 2, 3 };
    var raw_audio = [_]u8{ 4, 5 };
    const parts = [_]Node.DirectDenseEmbedPart{
        .{ .text = "caption" },
        .{ .media = .{ .mime_type = "image/png", .data = &raw_image } },
        .{ .media = .{ .mime_type = "audio/wav", .data = &raw_audio } },
    };
    var node: Node = undefined;
    node.config = .{};
    const manifest = manifest_mod.ModelManifest{
        .allocator = backing_allocator,
        .model_type = .embedder,
        .visual_model_path = "visual.onnx",
        .audio_model_path = "audio.onnx",
    };

    var budget = RequestMediaBudget.init(32);
    var parsed = try parseDirectDenseEmbedInputs(&node, backing_allocator, &manifest, &parts, &budget);
    try std.testing.expectEqual(@as(usize, 3), parsed.total_count);
    try std.testing.expect(!parsed.images.items[0].owned);
    try std.testing.expect(!parsed.audio.items[0].owned);
    try std.testing.expectEqual(@intFromPtr(raw_image[0..].ptr), @intFromPtr(parsed.images.items[0].bytes.ptr));
    try std.testing.expectEqual(@intFromPtr(raw_audio[0..].ptr), @intFromPtr(parsed.audio.items[0].bytes.ptr));
    parsed.deinit(backing_allocator);
    raw_image[0] = 9;
    raw_audio[0] = 8;
    try std.testing.expectEqual(@as(u8, 9), raw_image[0]);
    try std.testing.expectEqual(@as(u8, 8), raw_audio[0]);

    const Runner = struct {
        fn run(
            allocator: std.mem.Allocator,
            target: *Node,
            model_manifest: *const manifest_mod.ModelManifest,
            direct_parts: []const Node.DirectDenseEmbedPart,
        ) !void {
            var local_budget = RequestMediaBudget.init(32);
            var inputs = try parseDirectDenseEmbedInputs(
                target,
                allocator,
                model_manifest,
                direct_parts,
                &local_budget,
            );
            defer inputs.deinit(allocator);
        }
    };
    var fail_index: usize = 0;
    while (true) : (fail_index += 1) {
        var failing = std.testing.FailingAllocator.init(backing_allocator, .{
            .fail_index = fail_index,
            .resize_fail_index = 0,
        });
        Runner.run(failing.allocator(), &node, &manifest, &parts) catch |err| switch (err) {
            error.OutOfMemory => {
                try std.testing.expect(failing.has_induced_failure);
                try std.testing.expectEqual(failing.allocated_bytes, failing.freed_bytes);
                continue;
            },
            else => return err,
        };
        try std.testing.expect(!failing.has_induced_failure);
        try std.testing.expectEqual(failing.allocated_bytes, failing.freed_bytes);
        break;
    }
}

test "direct dense embed precharges borrowed media independent of image url order" {
    const allocator = std.testing.allocator;
    const image_url = "data:image/png;base64,AQ==";
    var raw = [_]u8{ 1, 2, 3 };
    const url_first = [_]Node.DirectDenseEmbedPart{
        .{ .image_url = image_url },
        .{ .media = .{ .mime_type = "image/png", .data = &raw } },
    };
    const borrowed_first = [_]Node.DirectDenseEmbedPart{
        .{ .media = .{ .mime_type = "image/png", .data = &raw } },
        .{ .image_url = image_url },
    };
    var node: Node = undefined;
    node.config = .{};
    const manifest = manifest_mod.ModelManifest{
        .allocator = allocator,
        .model_type = .embedder,
        .visual_model_path = "visual.onnx",
    };
    const exact_cap = raw.len + image_url.len;

    for ([_][]const Node.DirectDenseEmbedPart{ &url_first, &borrowed_first }) |ordered| {
        var budget = RequestMediaBudget.init(exact_cap);
        resetRequestWorkTestCounters();
        var parsed = try parseDirectDenseEmbedInputs(&node, allocator, &manifest, ordered, &budget);
        defer parsed.deinit(allocator);
        try std.testing.expectEqual(exact_cap, budget.used_bytes);
        try std.testing.expectEqual(@as(usize, 1), request_work_test_counters.media_fetch_attempts);
    }

    for ([_][]const Node.DirectDenseEmbedPart{ &url_first, &borrowed_first }) |ordered| {
        var budget = RequestMediaBudget.init(exact_cap - 1);
        resetRequestWorkTestCounters();
        try std.testing.expectError(
            error.RemoteContentTooLarge,
            parseDirectDenseEmbedInputs(&node, allocator, &manifest, ordered, &budget),
        );
        try std.testing.expectEqual(raw.len, budget.used_bytes);
        try std.testing.expectEqual(@as(usize, 0), request_work_test_counters.media_fetch_attempts);
    }
}

test "direct dense embed rejects media and reserves audio before model work" {
    const allocator = std.testing.allocator;
    var oversized_node = try Node.init(allocator, .{
        .models_dir = "missing-model-root",
        .max_concurrent_requests = 1,
        .content_security = .{ .max_download_size_bytes = 3 },
    });
    defer oversized_node.deinit();
    const oversized = [_]Node.DirectDenseEmbedPart{.{ .media = .{
        .mime_type = "image/png",
        .data = "1234",
    } }};
    try std.testing.expectError(
        error.RemoteContentTooLarge,
        oversized_node.embedDensePartsDirect(allocator, "missing", &oversized),
    );
    try std.testing.expectEqual(@as(usize, 0), oversized_node.inference_admission.inFlightUnits());
    try std.testing.expectEqual(@as(usize, 0), oversized_node.inference_admission.inFlightRequests());

    const capacity_audio = try allocator.alloc(u8, read_admission_bytes_per_unit);
    defer allocator.free(capacity_audio);
    var tiny_node = try Node.init(allocator, .{
        .models_dir = "missing-model-root",
        .max_concurrent_requests = 1,
    });
    defer tiny_node.deinit();
    const no_working_room = [_]Node.DirectDenseEmbedPart{.{ .media = .{
        .mime_type = "audio/wav",
        .data = capacity_audio,
    } }};
    resetRequestWorkTestCounters();
    try std.testing.expectError(
        error.AudioTooLarge,
        tiny_node.embedDensePartsDirect(allocator, "missing", &no_working_room),
    );
    try std.testing.expectEqual(@as(usize, 0), request_work_test_counters.model_load_attempts);
    try std.testing.expectEqual(@as(usize, 0), request_work_test_counters.media_fetch_attempts);
    try std.testing.expectEqual(@as(usize, 0), tiny_node.inference_admission.inFlightUnits());
    try std.testing.expectEqual(@as(usize, 0), tiny_node.inference_admission.inFlightRequests());

    var reserve_node = try Node.init(allocator, .{
        .models_dir = "missing-model-root",
        .max_concurrent_requests = 8,
    });
    defer reserve_node.deinit();
    try reserve_node.acquireAdmissionUnits(1);
    const small_audio = [_]Node.DirectDenseEmbedPart{.{ .media = .{
        .mime_type = "audio/wav",
        .data = "x",
    } }};
    try std.testing.expectError(
        error.QueueFull,
        reserve_node.embedDensePartsDirect(allocator, "missing", &small_audio),
    );
    try std.testing.expectEqual(@as(usize, 1), reserve_node.inference_admission.inFlightUnits());
    try std.testing.expectEqual(@as(usize, 1), reserve_node.inference_admission.inFlightRequests());
    var metrics_writer: std.Io.Writer.Allocating = .init(allocator);
    defer metrics_writer.deinit();
    try reserve_node.metrics.render(&metrics_writer.writer);
    try std.testing.expect(std.mem.indexOf(
        u8,
        metrics_writer.writer.buffered(),
        "antfly_admission_inference_rejected_requests_total 1\n",
    ) != null);
    reserve_node.releaseAdmission();
    try std.testing.expectEqual(@as(usize, 0), reserve_node.inference_admission.inFlightUnits());
    try std.testing.expectEqual(@as(usize, 0), reserve_node.inference_admission.inFlightRequests());
}

test "direct dense embed rejects borrowed image expansion before model loading" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.createDirPath(std.testing.io, "models/embedders/owner/embed");
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "models/embedders/owner/embed/config.json",
        .data = "{}",
    });
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "models/embedders/owner/embed/model_manifest.json",
        .data = "{\"type\":\"embedder\",\"inputs\":[\"image\"]}",
    });
    const models_root = try tmp.dir.realPathFileAlloc(std.testing.io, "models", allocator);
    defer allocator.free(models_root);

    var png = [_]u8{0} ** 24;
    const signature = [_]u8{ 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n' };
    @memcpy(png[0..8], &signature);
    std.mem.writeInt(u32, png[16..20], 800, .big);
    std.mem.writeInt(u32, png[20..24], 800, .big);
    const parts = [_]Node.DirectDenseEmbedPart{
        .{ .media = .{ .mime_type = "image/png", .data = &png } },
        .{ .media = .{ .mime_type = "image/png", .data = &png } },
    };

    var node = try Node.init(allocator, .{ .models_dir = models_root, .max_concurrent_requests = 1 });
    defer node.deinit();
    resetRequestWorkTestCounters();
    try std.testing.expectError(
        error.ImageBatchTooLarge,
        node.embedDensePartsDirect(allocator, "owner/embed", &parts),
    );
    try std.testing.expectEqual(@as(usize, 0), request_work_test_counters.model_load_attempts);
    try std.testing.expectEqual(@as(usize, 0), request_work_test_counters.media_fetch_attempts);
    try std.testing.expectEqual(@as(usize, 0), node.inference_admission.inFlightUnits());
    try std.testing.expectEqual(@as(usize, 0), node.inference_admission.inFlightRequests());
}

test "read decoded image budget rejects aggregate expansion and overflow" {
    const admission = ReadRequestAdmission{ .units = 1, .byte_cap = 1024, .resident_byte_cap = 1024, .decoded_pixel_cap = 4 };
    var budget = ReadDecodedImageBudget.init(admission, 2);

    var png = [_]u8{0} ** 24;
    const signature = [_]u8{ 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n' };
    @memcpy(png[0..8], &signature);
    std.mem.writeInt(u32, png[16..20], 2, .big);
    std.mem.writeInt(u32, png[20..24], 2, .big);
    try budget.addImage(&png);
    try std.testing.expectError(error.ImageBatchTooLarge, budget.addImage(&png));

    budget.used_pixels = std.math.maxInt(usize);
    budget.max_pixels = std.math.maxInt(usize);
    try std.testing.expectError(error.ImageBatchTooLarge, budget.addPixels(1));
    try std.testing.expect(budget.requiredUnits() > 0);

    var count_weighted = ReadDecodedImageBudget.init(.{ .units = 7, .byte_cap = 1024, .resident_byte_cap = 1024, .decoded_pixel_cap = 8 }, null);
    try count_weighted.addPixels(1);
    try std.testing.expectEqual(@as(usize, 7), count_weighted.requiredUnits());

    const pixels_per_unit = read_admission_bytes_per_unit / read_decoded_working_bytes_per_pixel;
    var additive = ReadDecodedImageBudget.init(.{
        .units = 7,
        .byte_cap = 7 * read_admission_bytes_per_unit,
        .resident_byte_cap = 7 * read_admission_bytes_per_unit,
        .decoded_pixel_cap = pixels_per_unit + 1,
    }, null);
    try additive.addPixels(pixels_per_unit + 1);
    try std.testing.expectEqual(@as(usize, 9), additive.requiredUnits());

    var saturated = ReadDecodedImageBudget.init(.{
        .units = std.math.maxInt(usize),
        .byte_cap = std.math.maxInt(usize),
        .resident_byte_cap = std.math.maxInt(usize),
        .decoded_pixel_cap = pixels_per_unit + 1,
    }, null);
    try saturated.addPixels(pixels_per_unit + 1);
    try std.testing.expectEqual(std.math.maxInt(usize), saturated.requiredUnits());
}

test "accepted multimodal routes reject tiny high-pixel batches before model loading" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const model_dirs = [_][]const u8{
        "models/generators/owner/generate",
        "models/embedders/owner/embed",
        "models/rerankers/owner/rerank",
    };
    for (model_dirs) |dir| {
        try tmp.dir.createDirPath(std.testing.io, dir);
        const config_path = try std.fs.path.join(allocator, &.{ dir, "config.json" });
        defer allocator.free(config_path);
        try tmp.dir.writeFile(std.testing.io, .{ .sub_path = config_path, .data = "{}" });
    }
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "models/embedders/owner/embed/model_manifest.json",
        .data = "{\"type\":\"embedder\",\"inputs\":[\"image\"]}",
    });
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "models/rerankers/owner/rerank/model_manifest.json",
        .data = "{\"type\":\"reranker\",\"capabilities\":[\"colqwen\"],\"inputs\":[\"text\",\"image\"]}",
    });
    const models_root = try tmp.dir.realPathFileAlloc(std.testing.io, "models", allocator);
    defer allocator.free(models_root);

    // Two 24-byte PNG headers each declare a modest 800x800 canvas. They fit
    // the encoded-media ceiling but together exceed one 16 MiB admission unit.
    var png = [_]u8{0} ** 24;
    const signature = [_]u8{ 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n' };
    @memcpy(png[0..8], &signature);
    std.mem.writeInt(u32, png[16..20], 800, .big);
    std.mem.writeInt(u32, png[20..24], 800, .big);
    const encoded = try allocator.alloc(u8, std.base64.standard.Encoder.calcSize(png.len));
    defer allocator.free(encoded);
    _ = std.base64.standard.Encoder.encode(encoded, &png);
    const image_parts = try std.fmt.allocPrint(
        allocator,
        "[{{\"type\":\"image_url\",\"image_url\":{{\"url\":\"data:image/png;base64,{s}\"}}}},{{\"type\":\"image_url\",\"image_url\":{{\"url\":\"data:image/png;base64,{s}\"}}}}]",
        .{ encoded, encoded },
    );
    defer allocator.free(image_parts);

    var node = try Node.init(allocator, .{ .models_dir = models_root, .max_concurrent_requests = 1 });
    defer node.deinit();

    {
        const body = try std.fmt.allocPrint(allocator, "{{\"model\":\"owner/generate\",\"messages\":[{{\"role\":\"user\",\"content\":{s}}}]}}", .{image_parts});
        defer allocator.free(body);
        var request = try httpx.Request.init(allocator, .POST, "/ai/v1/generate");
        defer request.deinit();
        try request.setJson(body);
        var ctx = httpx.Context.init(allocator, std.testing.io, &request);
        defer ctx.deinit();
        var response = try node.generateContent(&ctx);
        defer response.deinit();
        try std.testing.expectEqual(@as(u16, 413), response.status.code);
        try std.testing.expect(std.mem.indexOf(u8, response.body.?, "IMAGE_BATCH_TOO_LARGE") != null);
    }

    const embed_body = try std.fmt.allocPrint(allocator, "{{\"model\":\"owner/embed\",\"input\":{s}}}", .{image_parts});
    defer allocator.free(embed_body);
    {
        var request = try httpx.Request.init(allocator, .POST, "/ai/v1/embeddings");
        defer request.deinit();
        try request.setJson(embed_body);
        var ctx = httpx.Context.init(allocator, std.testing.io, &request);
        defer ctx.deinit();
        var response = try node.createEmbedding(&ctx);
        defer response.deinit();
        try std.testing.expectEqual(@as(u16, 413), response.status.code);
        try std.testing.expect(std.mem.indexOf(u8, response.body.?, "IMAGE_BATCH_TOO_LARGE") != null);
    }

    {
        const per_item_body = try std.fmt.allocPrint(allocator, "{{\"model\":\"owner/embed\",\"error_policy\":\"per_item\",\"input\":{s}}}", .{image_parts});
        defer allocator.free(per_item_body);
        var request = try httpx.Request.init(allocator, .POST, "/ai/v1/embeddings");
        defer request.deinit();
        try request.setJson(per_item_body);
        var ctx = httpx.Context.init(allocator, std.testing.io, &request);
        defer ctx.deinit();
        var response = try node.createEmbedding(&ctx);
        defer response.deinit();
        try std.testing.expectEqual(@as(u16, 413), response.status.code);
        try std.testing.expect(std.mem.indexOf(u8, response.body.?, "IMAGE_BATCH_TOO_LARGE") != null);
    }

    {
        const body = try std.fmt.allocPrint(allocator, "{{\"model\":\"owner/rerank\",\"query\":\"q\",\"documents\":[{{\"content\":{s}}}]}}", .{image_parts});
        defer allocator.free(body);
        var request = try httpx.Request.init(allocator, .POST, "/ai/v1/rerank_multimodal");
        defer request.deinit();
        try request.setJson(body);
        var ctx = httpx.Context.init(allocator, std.testing.io, &request);
        defer ctx.deinit();
        var response = try node.rerankMultimodalPrompts(&ctx);
        defer response.deinit();
        try std.testing.expectEqual(@as(u16, 413), response.status.code);
        try std.testing.expect(std.mem.indexOf(u8, response.body.?, "IMAGE_BATCH_TOO_LARGE") != null);
    }

    var parsed_input = try std.json.parseFromSlice(std.json.Value, allocator, image_parts, .{});
    defer parsed_input.deinit();
    try std.testing.expectError(
        error.ImageBatchTooLarge,
        node.embedDenseJsonInputDirect(allocator, "owner/embed", parsed_input.value),
    );
    try std.testing.expectEqual(@as(usize, 0), node.inference_admission.inFlightUnits());
}

test "sparse embed validates text-only input before model loading" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.createDirPath(std.testing.io, "models/embedders/owner/sparse");
    // The lightweight manifest is sufficient for request validation. A full
    // load from this deliberately incomplete model directory would return 500.
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "models/embedders/owner/sparse/config.json",
        .data = "{}",
    });
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "models/embedders/owner/sparse/model_manifest.json",
        .data = "{\"type\":\"embedder\",\"capabilities\":[\"sparse\"],\"inputs\":[\"text\"]}",
    });
    const models_root = try tmp.dir.realPathFileAlloc(std.testing.io, "models", allocator);
    defer allocator.free(models_root);

    var node = try Node.init(allocator, .{ .models_dir = models_root, .max_concurrent_requests = 1 });
    defer node.deinit();
    resetRequestWorkTestCounters();
    var request = try httpx.Request.init(allocator, .POST, "/ai/v1/embeddings");
    defer request.deinit();
    try request.setJson(
        "{\"model\":\"owner/sparse\",\"input\":[{\"type\":\"image_url\",\"image_url\":{\"url\":\"data:image/png;base64,AA==\"}}]}",
    );
    var ctx = httpx.Context.init(allocator, std.testing.io, &request);
    defer ctx.deinit();

    var response = try node.createEmbedding(&ctx);
    defer response.deinit();
    try std.testing.expectEqual(@as(u16, 400), response.status.code);
    try std.testing.expect(std.mem.indexOf(u8, response.body.?, "sparse models only support text input") != null);
    try std.testing.expectEqual(@as(usize, 0), request_work_test_counters.model_load_attempts);
    try std.testing.expectEqual(@as(usize, 0), request_work_test_counters.media_fetch_attempts);
    try std.testing.expectEqual(@as(usize, 0), node.inference_admission.inFlightUnits());
}

test "multimodal rerank rejects incompatible manifest before media or model loading" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.createDirPath(std.testing.io, "models/rerankers/owner/text-only");
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "models/rerankers/owner/text-only/config.json",
        .data = "{}",
    });
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "models/rerankers/owner/text-only/model_manifest.json",
        .data = "{\"type\":\"reranker\",\"inputs\":[\"text\"]}",
    });
    const models_root = try tmp.dir.realPathFileAlloc(std.testing.io, "models", allocator);
    defer allocator.free(models_root);

    var node = try Node.init(allocator, .{ .models_dir = models_root, .max_concurrent_requests = 1 });
    defer node.deinit();
    resetRequestWorkTestCounters();
    var request = try httpx.Request.init(allocator, .POST, "/ai/v1/rerank_multimodal");
    defer request.deinit();
    // The default deny-all policy makes this deterministic and network-free:
    // reaching media materialization would increment the attempt counter and
    // return a content-policy error instead of MODEL_NOT_SUPPORTED.
    try request.setJson(
        "{\"model\":\"owner/text-only\",\"query\":\"q\",\"documents\":[{\"content\":[{\"type\":\"image_url\",\"image_url\":{\"url\":\"https://example.invalid/x\"}}]}]}",
    );
    var ctx = httpx.Context.init(allocator, std.testing.io, &request);
    defer ctx.deinit();

    var response = try node.rerankMultimodalPrompts(&ctx);
    defer response.deinit();
    try std.testing.expectEqual(@as(u16, 400), response.status.code);
    try std.testing.expect(std.mem.indexOf(u8, response.body.?, "MODEL_NOT_SUPPORTED") != null);
    try std.testing.expectEqual(@as(usize, 0), request_work_test_counters.model_load_attempts);
    try std.testing.expectEqual(@as(usize, 0), request_work_test_counters.media_fetch_attempts);
    try std.testing.expectEqual(@as(usize, 0), node.inference_admission.inFlightUnits());
}

test "read weighted admission rejects before model resolution or download" {
    const allocator = std.testing.allocator;
    var node = try Node.init(allocator, .{ .max_concurrent_requests = 8 });
    defer node.deinit();
    try node.inference_admission.acquire();
    defer node.inference_admission.release();

    var body = std.ArrayListUnmanaged(u8).empty;
    defer body.deinit(allocator);
    try body.appendSlice(allocator, "{\"model\":\"missing\",\"images\":[");
    for (0..max_read_batch_images) |idx| {
        if (idx != 0) try body.append(allocator, ',');
        try body.appendSlice(allocator, "{\"url\":\"data:image/png;base64,%%%\"}");
    }
    try body.appendSlice(allocator, "]}");

    var request = try httpx.Request.init(allocator, .POST, "/ai/v1/read");
    defer request.deinit();
    try request.setJson(body.items);
    var ctx = httpx.Context.init(allocator, std.testing.io, &request);
    defer ctx.deinit();

    var response = try node.readImages(&ctx);
    defer response.deinit();
    try std.testing.expectEqual(@as(u16, 503), response.status.code);
    try std.testing.expectEqualStrings("1", response.headers.get("Retry-After").?);
}

test "transcribe validates encoded audio before model resolution and releases admission" {
    const allocator = std.testing.allocator;
    var node = try Node.init(allocator, .{
        .max_concurrent_requests = 1,
        .content_security = .{ .max_download_size_bytes = 3 },
    });
    defer node.deinit();

    resetRequestWorkTestCounters();
    var request = try httpx.Request.init(allocator, .POST, "/ai/v1/transcribe");
    defer request.deinit();
    try request.setJson("{\"model\":\"missing\",\"audio\":\"YQ==\"}");
    var ctx = httpx.Context.init(allocator, std.testing.io, &request);
    defer ctx.deinit();

    var response = try node.transcribeAudio(&ctx);
    defer response.deinit();
    try std.testing.expectEqual(@as(u16, 413), response.status.code);
    try std.testing.expectEqual(@as(usize, 0), request_work_test_counters.model_resolution_attempts);
    try std.testing.expectEqual(@as(usize, 0), request_work_test_counters.model_load_attempts);
    try std.testing.expectEqual(@as(usize, 0), node.inference_admission.inFlightUnits());
    try std.testing.expectEqual(@as(usize, 0), node.inference_admission.inFlightRequests());
}

test "transcribe bounded-decodes corrupt and metadata-amplified audio before model resolution" {
    const allocator = std.testing.allocator;
    var node = try Node.init(allocator, .{ .max_concurrent_requests = 1 });
    defer node.deinit();

    const malformed_wav = "RIFFxxxxWAVE";
    const oversized_caf_packet_table = [_]u8{
        'c',  'a',  'f',  'f',  0x00, 0x01, 0x00, 0x00,
        'p',  'a',  'k',  't',  0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x18, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x4c, 0x4b, 0x40, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00,
    };
    const cases = [_]struct {
        bytes: []const u8,
        expected_status: u16,
        expected_code: []const u8,
    }{
        .{ .bytes = malformed_wav, .expected_status = 400, .expected_code = "UNSUPPORTED" },
        .{ .bytes = &oversized_caf_packet_table, .expected_status = 413, .expected_code = "AUDIO_TOO_LARGE" },
    };

    for (cases) |case| {
        resetRequestWorkTestCounters();
        const encoded = try allocator.alloc(u8, std.base64.standard.Encoder.calcSize(case.bytes.len));
        defer allocator.free(encoded);
        _ = std.base64.standard.Encoder.encode(encoded, case.bytes);
        const body = try std.fmt.allocPrint(allocator, "{{\"model\":\"missing\",\"audio\":\"{s}\"}}", .{encoded});
        defer allocator.free(body);

        var request = try httpx.Request.init(allocator, .POST, "/ai/v1/transcribe");
        defer request.deinit();
        try request.setJson(body);
        var ctx = httpx.Context.init(allocator, std.testing.io, &request);
        defer ctx.deinit();

        var response = try node.transcribeAudio(&ctx);
        defer response.deinit();
        try std.testing.expectEqual(case.expected_status, response.status.code);
        try std.testing.expect(std.mem.indexOf(u8, response.body.?, case.expected_code) != null);
        try std.testing.expectEqual(@as(usize, 0), request_work_test_counters.model_resolution_attempts);
        try std.testing.expectEqual(@as(usize, 0), request_work_test_counters.model_load_attempts);
        try std.testing.expectEqual(@as(usize, 0), node.inference_admission.inFlightUnits());
        try std.testing.expectEqual(@as(usize, 0), node.inference_admission.inFlightRequests());
    }
}

test "direct extraction media shape reserves remote and cumulative inline sources" {
    const allocator = std.testing.allocator;
    var remote_shape: RequestMediaAdmissionShape = .{};
    try addDirectExtractionContentMediaShape(
        allocator,
        &remote_shape,
        "[{\"type\":\"image_url\",\"image_url\":{\"url\":\"https://example.invalid/image.png\"}}]",
    );
    try std.testing.expect(remote_shape.has_remote);
    try std.testing.expectEqual(@as(usize, 1), remote_shape.image_count);
    const remote_admission = requestMediaAdmissionForLimits(remote_shape, default_max_request_media_bytes, 32, null);
    try std.testing.expectEqual(default_max_request_media_bytes, remote_admission.byte_cap);
    try std.testing.expectEqual(@as(usize, 7), remote_admission.units);

    const data_uri = "data:image/png;base64,YQ==";
    var inline_shape: RequestMediaAdmissionShape = .{};
    try addDirectExtractionContentMediaShape(
        allocator,
        &inline_shape,
        "[{\"type\":\"image_url\",\"image_url\":{\"url\":\"data:image/png;base64,YQ==\"}},{\"type\":\"media\",\"data\":\"Yg==\"}]",
    );
    try std.testing.expect(!inline_shape.has_remote);
    try std.testing.expectEqual(@as(usize, 2), inline_shape.image_count);
    try std.testing.expectEqual(data_uri.len + "Yg==".len, inline_shape.inline_bytes);
    const inline_admission = requestMediaAdmissionForLimits(inline_shape, default_max_request_media_bytes, 32, null);
    try std.testing.expectEqual(inline_shape.inline_bytes, inline_admission.byte_cap);
    try std.testing.expectEqual(2 * inline_shape.inline_bytes, inline_admission.resident_byte_cap);
    try std.testing.expectEqual(@as(usize, 1), inline_admission.units);

    var text_shape: RequestMediaAdmissionShape = .{};
    try addDirectExtractionContentMediaShape(allocator, &text_shape, "\"text only\"");
    const text_admission = requestMediaAdmissionForLimits(text_shape, default_max_request_media_bytes, 32, null);
    try std.testing.expectEqual(@as(usize, 0), text_admission.byte_cap);
    try std.testing.expectEqual(@as(usize, 1), text_admission.units);

    try std.testing.expectError(error.UnexpectedEndOfInput, addDirectExtractionContentMediaShape(allocator, &text_shape, "{"));
}

test "read image preflight maps malformed dimension and aggregate errors before model load" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.createDirPath(std.testing.io, "models/owner/model");
    // Enough for path resolution; reaching model load would fail this test with 500.
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "models/owner/model/config.json", .data = "{}" });
    const models_root = try tmp.dir.realPathFileAlloc(std.testing.io, "models", allocator);
    defer allocator.free(models_root);

    var node = try Node.init(allocator, .{ .models_dir = models_root, .max_concurrent_requests = 1 });
    defer node.deinit();

    {
        var request = try httpx.Request.init(allocator, .POST, "/ai/v1/read");
        defer request.deinit();
        try request.setJson("{\"model\":\"owner/model\",\"images\":[{\"url\":\"data:image/png;base64,bm90LWFuLWltYWdl\"}]}");
        var ctx = httpx.Context.init(allocator, std.testing.io, &request);
        defer ctx.deinit();
        var response = try node.readImages(&ctx);
        defer response.deinit();
        try std.testing.expectEqual(@as(u16, 400), response.status.code);
    }

    var png = [_]u8{0} ** 24;
    const signature = [_]u8{ 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n' };
    @memcpy(png[0..8], &signature);
    std.mem.writeInt(u32, png[16..20], 2, .big);
    std.mem.writeInt(u32, png[20..24], 2, .big);
    const encoded = try allocator.alloc(u8, std.base64.standard.Encoder.calcSize(png.len));
    defer allocator.free(encoded);
    _ = std.base64.standard.Encoder.encode(encoded, &png);
    const body = try std.fmt.allocPrint(allocator, "{{\"model\":\"owner/model\",\"images\":[{{\"url\":\"data:image/png;base64,{s}\"}}]}}", .{encoded});
    defer allocator.free(body);

    node.config.content_security = .{ .max_image_dimension = 1 };
    {
        var request = try httpx.Request.init(allocator, .POST, "/ai/v1/read");
        defer request.deinit();
        try request.setJson(body);
        var ctx = httpx.Context.init(allocator, std.testing.io, &request);
        defer ctx.deinit();
        var response = try node.readImages(&ctx);
        defer response.deinit();
        try std.testing.expectEqual(@as(u16, 413), response.status.code);
    }

    node.config.content_security = null;
    std.mem.writeInt(u32, png[16..20], 1025, .big);
    std.mem.writeInt(u32, png[20..24], 1025, .big);
    _ = std.base64.standard.Encoder.encode(encoded, &png);
    const aggregate_body = try std.fmt.allocPrint(allocator, "{{\"model\":\"owner/model\",\"images\":[{{\"url\":\"data:image/png;base64,{s}\"}}]}}", .{encoded});
    defer allocator.free(aggregate_body);
    {
        var request = try httpx.Request.init(allocator, .POST, "/ai/v1/read");
        defer request.deinit();
        try request.setJson(aggregate_body);
        var ctx = httpx.Context.init(allocator, std.testing.io, &request);
        defer ctx.deinit();
        var response = try node.readImages(&ctx);
        defer response.deinit();
        try std.testing.expectEqual(@as(u16, 413), response.status.code);
    }
    try std.testing.expectEqual(@as(usize, 0), node.inference_admission.inFlightUnits());
}

test "extraction image downloads transfer ownership and clean up aggregate failures" {
    const allocator = std.testing.allocator;
    var node: Node = undefined;
    node.config = .{};
    var request = try httpx.Request.init(allocator, .GET, "/");
    defer request.deinit();
    var ctx = httpx.Context.init(allocator, std.testing.io, &request);
    defer ctx.deinit();
    const images = [_]api.ImageURL{
        .{ .url = "data:image/gif;base64,R0lGODlhAQABAIAAAAAAAP///ywAAAAAAQABAAACAUwAOw==" },
        .{ .url = "data:image/gif;base64,R0lGODlhAQABAIAAAAAAAP///ywAAAAAAQABAAACAUwAOw==" },
    };

    const admission = ReadRequestAdmission{ .units = 1, .byte_cap = 128, .resident_byte_cap = 128, .decoded_pixel_cap = 2 };
    var budget = ReadDecodedImageBudget.init(admission, null);
    const downloaded = try node.downloadImagesForExtraction(&ctx, &images, admission.byte_cap, &budget);
    defer {
        for (downloaded) |data| allocator.free(data);
        allocator.free(downloaded);
    }
    try std.testing.expectEqual(@as(usize, 1), (try image_pipeline.inspectEncodedForInference(downloaded[0], null)).width);
    try std.testing.expectEqual(@as(usize, 2), budget.used_pixels);

    var small_budget = ReadDecodedImageBudget.init(admission, null);
    try std.testing.expectError(
        error.ReadBatchTooLarge,
        node.downloadImagesForExtraction(&ctx, &images, 80, &small_budget),
    );
}

test "read max tokens preserves omission and rejects unsafe signed values" {
    try std.testing.expectEqual(@as(?usize, null), try validateReadMaxTokens(null));
    try std.testing.expectEqual(@as(?usize, 1), try validateReadMaxTokens(1));
    try std.testing.expectEqual(@as(?usize, max_read_tokens), try validateReadMaxTokens(@intCast(max_read_tokens)));
    try std.testing.expectError(error.InvalidMaxTokens, validateReadMaxTokens(-1));
    try std.testing.expectError(error.InvalidMaxTokens, validateReadMaxTokens(0));
    try std.testing.expectError(error.InvalidMaxTokens, validateReadMaxTokens(@intCast(max_read_tokens + 1)));
    try std.testing.expectError(error.InvalidMaxTokens, validateReadMaxTokens(std.math.maxInt(i64)));
}

test "read prompt treats empty public values as an omitted OCR prompt" {
    try std.testing.expectEqual(@as(?[]const u8, null), normalizeReadPrompt(null));
    try std.testing.expectEqual(@as(?[]const u8, null), normalizeReadPrompt(""));
    try std.testing.expectEqual(@as(?[]const u8, null), normalizeReadPrompt(" \t\n"));
    try std.testing.expectEqualStrings("<OCR>", normalizeReadPrompt("<OCR>").?);
}

test "read admission units scale with image batch and decode length" {
    try std.testing.expectEqual(@as(usize, 1), estimateReadAdmissionUnits(1, null));
    try std.testing.expectEqual(@as(usize, 4), estimateReadAdmissionUnits(4, null));
    try std.testing.expectEqual(@as(usize, 8), estimateReadAdmissionUnits(4, default_read_admission_max_tokens + 1));
    try std.testing.expectEqual(@as(usize, 16), estimateReadAdmissionUnits(4, max_read_tokens));
}

test "direct extraction validates read max tokens" {
    var valid = try parseExtractionOptionsJson(std.testing.allocator, "{\"max_tokens\":1024}");
    defer valid.deinit();
    try std.testing.expectEqual(@as(?usize, max_read_tokens), valid.max_tokens);
    try std.testing.expectError(
        error.InvalidMaxTokens,
        parseExtractionOptionsJson(std.testing.allocator, "{\"max_tokens\":-1}"),
    );
    try std.testing.expectError(
        error.InvalidMaxTokens,
        parseExtractionOptionsJson(std.testing.allocator, "{\"max_tokens\":1025}"),
    );
}

test "direct extraction content shares aggregate budget across downloaded and inline media" {
    const allocator = std.testing.allocator;
    var node: Node = undefined;
    const first_uri = "data:image/png;base64,YWJj";
    node.config = .{ .content_security = .{ .max_download_size_bytes = first_uri.len + 1 } };
    var out = DirectExtractionInputs{ .allocator = allocator };
    defer out.deinit();
    var media_budget = RequestMediaBudget.init(requestMediaMaxBytes(&node));

    try appendDirectExtractionContent(
        &node,
        allocator,
        &out,
        "[{\"type\":\"image_url\",\"image_url\":{\"url\":\"data:image/png;base64,YWJj\"}}]",
        &media_budget,
    );
    try std.testing.expectEqual(first_uri.len, media_budget.used_bytes);
    try std.testing.expectError(
        error.RemoteContentTooLarge,
        appendDirectExtractionContent(
            &node,
            allocator,
            &out,
            "[{\"type\":\"media\",\"data\":\"ZGVm\"}]",
            &media_budget,
        ),
    );
    try std.testing.expectEqual(@as(usize, 1), out.images.items.len);
}

test "direct extraction content releases temporary ownership on every allocation failure" {
    const backing_allocator = std.testing.allocator;
    var node: Node = undefined;
    node.config = .{};

    const Runner = struct {
        fn run(allocator: std.mem.Allocator, target: *Node) !void {
            var out = DirectExtractionInputs{ .allocator = allocator };
            defer out.deinit();
            var budget = RequestMediaBudget.init(32);
            try appendDirectExtractionContent(target, allocator, &out, "\"plain\"", &budget);
            try appendDirectExtractionContent(
                target,
                allocator,
                &out,
                "[{\"type\":\"text\",\"text\":\"first\"},{\"type\":\"text\",\"text\":\"second\"}]",
                &budget,
            );
            try appendDirectExtractionContent(target, allocator, &out, "42", &budget);
            try std.testing.expectEqual(@as(usize, 3), out.texts.items.len);
        }
    };

    var fail_index: usize = 0;
    while (true) : (fail_index += 1) {
        var failing = std.testing.FailingAllocator.init(backing_allocator, .{
            .fail_index = fail_index,
            .resize_fail_index = 0,
        });
        Runner.run(failing.allocator(), &node) catch |err| switch (err) {
            error.OutOfMemory => {
                try std.testing.expect(failing.has_induced_failure);
                try std.testing.expectEqual(failing.allocated_bytes, failing.freed_bytes);
                continue;
            },
            else => return err,
        };
        try std.testing.expect(!failing.has_induced_failure);
        try std.testing.expectEqual(failing.allocated_bytes, failing.freed_bytes);
        break;
    }
}

test "fail-fast embedding capacity response exposes retry contract" {
    const allocator = std.testing.allocator;
    var request = try httpx.Request.init(allocator, .GET, "/ai/v1/embeddings");
    defer request.deinit();

    var ctx = httpx.Context.init(allocator, std.testing.io, &request);
    defer ctx.deinit();

    var response = try embedDenseInputFailureResponse(
        &ctx,
        error.ResourceTemporarilyUnavailable,
    );
    defer response.deinit();

    try std.testing.expectEqual(@as(u16, 503), response.status.code);
    try std.testing.expectEqualStrings(
        transient_capacity_retry_after_seconds,
        response.header("Retry-After").?,
    );

    const RetryBody = struct {
        @"error": []const u8,
        message: []const u8,
        reason: []const u8,
        retryable: bool,
        retry_after_ms: i64,
    };
    var parsed = try std.json.parseFromSlice(
        RetryBody,
        allocator,
        response.body.?,
        .{},
    );
    defer parsed.deinit();

    try std.testing.expectEqualStrings("MODEL_RESOURCE_BUSY", parsed.value.@"error");
    try std.testing.expectEqualStrings(
        "insufficient inference capacity is currently available",
        parsed.value.message,
    );
    try std.testing.expectEqualStrings("inference_capacity", parsed.value.reason);
    try std.testing.expect(parsed.value.retryable);
    try std.testing.expectEqual(
        @as(i64, transient_capacity_retry_after_ms),
        parsed.value.retry_after_ms,
    );
}

test "changing model publication returns an explicit retry contract" {
    const allocator = std.testing.allocator;
    var request = try httpx.Request.init(allocator, .POST, "/ai/v1/transcribe");
    defer request.deinit();
    var ctx = httpx.Context.init(allocator, std.testing.io, &request);
    defer ctx.deinit();

    var response = try modelLoadFailureResponse(&ctx, error.ModelArtifactsChanging);
    defer response.deinit();
    try std.testing.expectEqual(@as(u16, 503), response.status.code);
    try std.testing.expectEqualStrings(
        transient_capacity_retry_after_seconds,
        response.header("Retry-After").?,
    );
    try std.testing.expect(std.mem.indexOf(u8, response.body.?, "MODEL_ARTIFACTS_CHANGING") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.body.?, "\"retryable\":true") != null);
}

test "managed download markers return the model publication retry contract" {
    const allocator = std.testing.allocator;
    var request = try httpx.Request.init(allocator, .POST, "/ai/v1/transcribe");
    defer request.deinit();
    var ctx = httpx.Context.init(allocator, std.testing.io, &request);
    defer ctx.deinit();

    var response = try modelLoadFailureResponse(&ctx, error.IncompleteManagedDownload);
    defer response.deinit();
    try std.testing.expectEqual(@as(u16, 503), response.status.code);
    try std.testing.expectEqualStrings(
        transient_capacity_retry_after_seconds,
        response.header("Retry-After").?,
    );
    try std.testing.expect(std.mem.indexOf(u8, response.body.?, "MODEL_ARTIFACTS_CHANGING") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.body.?, "\"reason\":\"model_publication\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.body.?, "\"retryable\":true") != null);
}

test "registerRoutesOn prefixes embed aliases and metrics route" {
    var node = try Node.init(std.testing.allocator, .{});
    defer node.deinit();

    var server = RecordingServer{ .allocator = std.testing.allocator };
    defer server.deinit();

    try node.registerRoutesOn(public_api_prefix, &server);

    try std.testing.expect(server.hasRoute(.post, public_api_prefix ++ "/embed"));
    try std.testing.expect(server.hasRoute(.post, public_api_prefix ++ "/embeddings"));
    try std.testing.expect(server.hasRoute(.post, public_api_prefix ++ "/generate/batch"));
    try std.testing.expect(server.hasRoute(.get, public_api_prefix ++ "/models"));
    try std.testing.expectEqual(@as(usize, 1), server.routeCount(.get, public_api_prefix ++ "/models"));
    try std.testing.expect(server.hasRoute(.get, public_api_prefix ++ "/metrics"));
    try std.testing.expect(!server.hasRoute(.get, public_api_prefix ++ "/healthz"));
    try std.testing.expect(!server.hasRoute(.get, public_api_prefix ++ "/readyz"));
}

test "node attachIo wires model session manager" {
    var node = try Node.init(std.testing.allocator, .{});
    defer node.deinit();

    node.attachIo(std.testing.io);

    try std.testing.expect(node.session_manager.io != null);
    try std.testing.expect(node.model_manager.session_manager.io != null);
}

test "canonical relation schemas preserve endpoint label constraints" {
    const schemas = [_]extraction_api.ExtractionRelationSchema{
        .{ .type = "works_at", .source = "person", .target = "organization" },
        .{ .type = "located_in", .source = "organization" },
        .{ .type = "knows" },
    };
    const labels = try canonicalRelationLabels(std.testing.allocator, &schemas);
    defer freeStringSlice(std.testing.allocator, labels);

    try std.testing.expectEqual(@as(usize, 3), labels.len);
    try std.testing.expectEqualStrings("person::works_at::organization", labels[0]);
    try std.testing.expectEqualStrings("organization::located_in", labels[1]);
    try std.testing.expectEqualStrings("knows", labels[2]);

    try std.testing.expectError(
        error.InvalidRelationSchema,
        canonicalRelationLabels(std.testing.allocator, &.{.{ .type = "works_at", .target = "organization" }}),
    );
    try std.testing.expectError(
        error.InvalidRelationSchema,
        canonicalRelationLabels(std.testing.allocator, &.{.{ .type = "person::works_at" }}),
    );
}

test "relation entity labels merge defaults and canonical endpoint constraints" {
    const defaults = [_][]const u8{ "person", "organization", "location" };
    const canonical_relations = [_][]const u8{
        "employee::works_at::company",
        "company::located_in::location",
        "knows",
    };

    const inferred = (try relationEntityLabelsAlloc(
        std.testing.allocator,
        null,
        &defaults,
        &canonical_relations,
    )).?;
    defer std.testing.allocator.free(inferred);
    const expected_inferred = [_][]const u8{ "person", "organization", "location", "employee", "company" };
    try std.testing.expectEqual(expected_inferred.len, inferred.len);
    for (&expected_inferred, inferred) |expected, actual|
        try std.testing.expectEqualStrings(expected, actual);

    const requested = [_][]const u8{"employee"};
    const constrained = (try relationEntityLabelsAlloc(
        std.testing.allocator,
        &requested,
        &defaults,
        &canonical_relations,
    )).?;
    defer std.testing.allocator.free(constrained);
    const expected_constrained = [_][]const u8{ "employee", "company", "location" };
    try std.testing.expectEqual(expected_constrained.len, constrained.len);
    for (&expected_constrained, constrained) |expected, actual|
        try std.testing.expectEqualStrings(expected, actual);

    try std.testing.expect((try relationEntityLabelsAlloc(
        std.testing.allocator,
        null,
        &.{},
        &.{"knows"},
    )) == null);
}

test "canonical extraction operation routes every documented schema family" {
    const cases = [_]struct { json: []const u8, expected: CanonicalExtractionOperation }{
        .{ .json = "{\"entities\":[\"person\"]}", .expected = .entities_relations },
        .{ .json = "{\"relations\":[{\"type\":\"works_for\"}]}", .expected = .entities_relations },
        .{ .json = "{\"entities\":[\"person\"],\"relations\":[{\"type\":\"works_for\"}]}", .expected = .entities_relations },
        .{ .json = "{\"classifications\":[{\"name\":\"sentiment\",\"labels\":[\"positive\",\"negative\"]}]}", .expected = .classifications },
        .{ .json = "{\"structures\":{\"person\":{\"fields\":{\"name\":\"string\"}}}}", .expected = .structures },
    };
    for (cases) |case| {
        var parsed = try std.json.parseFromSlice(extraction_api.ExtractionSchema, std.testing.allocator, case.json, .{
            .allocate = .alloc_always,
        });
        defer parsed.deinit();
        try std.testing.expectEqual(case.expected, try canonicalExtractionOperation(parsed.value));
    }
}

test "entity extraction model preflight rejects incompatible families and modalities" {
    var manifest = manifest_mod.ModelManifest{
        .allocator = std.testing.allocator,
        .model_type = .recognizer,
    };
    try validateTextEntityExtractionManifest(&manifest);

    manifest.model_type = .classifier;
    try std.testing.expectError(
        error.InvalidModelForExtraction,
        validateTextEntityExtractionManifest(&manifest),
    );

    var image_inputs = [_][]const u8{"image"};
    manifest.model_type = .recognizer;
    manifest.inputs = &image_inputs;
    try std.testing.expectError(
        error.UnsupportedInput,
        validateTextEntityExtractionManifest(&manifest),
    );

    manifest.inputs = &.{};
    manifest.gliner_model_type = "uniencoder";
    try std.testing.expectError(
        error.InvalidModelForExtraction,
        validateTextEntityExtractionManifest(&manifest),
    );

    var extraction_capabilities = [_][]const u8{"extraction"};
    manifest.capabilities = &extraction_capabilities;
    try validateTextEntityExtractionManifest(&manifest);
}

test "canonical extraction operation repeatedly rejects empty and mixed schemas without leaking" {
    var empty = try std.json.parseFromSlice(extraction_api.ExtractionSchema, std.testing.allocator, "{}", .{
        .allocate = .alloc_always,
    });
    defer empty.deinit();
    const mixed_json = "{\"entities\":[\"person\"],\"structures\":{\"person\":{\"fields\":{\"name\":\"string\"}}}}";
    var mixed = try std.json.parseFromSlice(
        extraction_api.ExtractionSchema,
        std.testing.allocator,
        mixed_json,
        .{ .allocate = .alloc_always },
    );
    defer mixed.deinit();
    for (0..64) |_| {
        try std.testing.expectError(error.MissingExtractionOperation, canonicalExtractionOperation(empty.value));
        try std.testing.expectError(error.MixedExtractionOperations, canonicalExtractionOperation(mixed.value));
    }

    var node = try Node.init(std.testing.allocator, .{});
    defer node.deinit();
    const inputs = [_]extracting_api.Input{.{ .content_json = "\"Grace Hopper built compilers.\"" }};
    for (0..64) |_| {
        try std.testing.expectError(error.MissingExtractionOperation, node.extractDirect(
            std.testing.allocator,
            "antflydb/gliner2-base-v1",
            .{ .inputs = &inputs, .schema_json = "{}" },
        ));
        try std.testing.expectError(error.MixedExtractionOperations, node.extractDirect(
            std.testing.allocator,
            "antflydb/gliner2-base-v1",
            .{ .inputs = &inputs, .schema_json = mixed_json },
        ));
    }
}

test "text extraction rejects media before fetch or model resolution" {
    var node = try Node.init(std.testing.allocator, .{
        .models_dir = "missing-model-root",
        .max_concurrent_requests = 1,
    });
    defer node.deinit();
    const inputs = [_]extracting_api.Input{.{
        .content_json =
        \\[{"type":"image_url","image_url":{"url":"https://example.com/image.png"}}]
        ,
    }};

    resetRequestWorkTestCounters();
    try std.testing.expectError(error.UnsupportedInput, node.extractDirect(
        std.testing.allocator,
        "antflydb/gliner2-base-v1",
        .{ .inputs = &inputs, .schema_json = "{\"entities\":[\"person\"]}" },
    ));
    try std.testing.expectEqual(@as(usize, 0), request_work_test_counters.media_fetch_attempts);
    try std.testing.expectEqual(@as(usize, 0), request_work_test_counters.model_resolution_attempts);
    try std.testing.expectEqual(@as(usize, 0), node.inference_admission.inFlightUnits());
    try std.testing.expectEqual(@as(usize, 0), node.inference_admission.inFlightRequests());

    var canonical_input = try std.json.parseFromSlice(extraction_api.ExtractionInput, std.testing.allocator,
        \\{"content":[{"type":"image_url","image_url":{"url":"https://example.com/image.png"}}]}
    , .{ .allocate = .alloc_always });
    defer canonical_input.deinit();
    const canonical_inputs = [_]extraction_api.ExtractionInput{canonical_input.value};
    try std.testing.expectError(
        error.UnsupportedInput,
        parseCanonicalTextInputs(std.testing.allocator, &canonical_inputs),
    );
    try std.testing.expectEqual(@as(usize, 0), request_work_test_counters.media_fetch_attempts);
    try std.testing.expectEqual(@as(usize, 0), request_work_test_counters.model_resolution_attempts);
}

test "text extraction preserves multipart text as one input" {
    const inputs = [_]extracting_api.Input{.{
        .content_json =
        \\[{"type":"text","text":"Grace Hopper"},{"type":"text","text":"built compilers."}]
        ,
    }};
    var parsed = try parseDirectTextExtractionInputs(std.testing.allocator, &inputs);
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 1), parsed.texts.items.len);
    try std.testing.expectEqualStrings("Grace Hopper\nbuilt compilers.", parsed.texts.items[0]);
}

test "structured extraction validates generation options before resolver and media work" {
    var node = try Node.init(std.testing.allocator, .{
        .models_dir = "missing-model-root",
        .max_concurrent_requests = 1,
    });
    defer node.deinit();
    // Three images require two admission units. With a one-unit node this
    // proves invalid options win over capacity checks, not merely over fetch.
    const inputs = [_]extracting_api.Input{
        .{ .content_json =
        \\[{"type":"image_url","image_url":{"url":"https://example.com/one.png"}}]
        },
        .{ .content_json =
        \\[{"type":"image_url","image_url":{"url":"https://example.com/two.png"}}]
        },
        .{ .content_json =
        \\[{"type":"image_url","image_url":{"url":"https://example.com/three.png"}}]
        },
    };

    resetRequestWorkTestCounters();
    try std.testing.expectError(error.InvalidMaxTokens, node.extractDirect(
        std.testing.allocator,
        "missing/structured-extractor",
        .{
            .inputs = &inputs,
            .schema_json =
            \\{"structures":{"person":{"fields":{"name":"string"}}}}
            ,
            .options_json = "{\"max_tokens\":0}",
        },
    ));
    try std.testing.expectEqual(@as(usize, 0), request_work_test_counters.media_fetch_attempts);
    try std.testing.expectEqual(@as(usize, 0), node.inference_admission.inFlightUnits());
    try std.testing.expectEqual(@as(usize, 0), node.inference_admission.inFlightRequests());

    try std.testing.expectError(error.MissingStructureFields, node.extractDirect(
        std.testing.allocator,
        "missing/structured-extractor",
        .{
            .inputs = &inputs,
            .schema_json =
            \\{"structures":{"person":{"fields":{}}}}
            ,
        },
    ));
    try std.testing.expectEqual(@as(usize, 0), request_work_test_counters.media_fetch_attempts);
    try std.testing.expectEqual(@as(usize, 0), node.inference_admission.inFlightUnits());
    try std.testing.expectEqual(@as(usize, 0), node.inference_admission.inFlightRequests());
}

test "valid direct entity failures release admission and partial model state" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.createDirPath(std.testing.io, "models/owner/broken-gliner");
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "models/owner/broken-gliner/config.json",
        .data = "{\"model_type\":\"gliner2\"}",
    });
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "models/owner/broken-gliner/gliner_config.json",
        .data = "{\"model_type\":\"gliner2\",\"default_labels\":[\"person\"]}",
    });
    const models_root = try tmp.dir.realPathFileAlloc(std.testing.io, "models", allocator);
    defer allocator.free(models_root);

    var node = try Node.init(allocator, .{
        .models_dir = models_root,
        .max_concurrent_requests = 1,
        .allow_unknown_models = true,
    });
    defer node.deinit();
    const inputs = [_]extracting_api.Input{.{ .content_json = "\"Grace Hopper built compilers.\"" }};

    resetRequestWorkTestCounters();
    for (0..64) |_| {
        var response = node.extractDirect(
            allocator,
            "owner/broken-gliner",
            .{ .inputs = &inputs, .schema_json = "{\"entities\":[\"person\"]}" },
        ) catch |err| {
            try std.testing.expect(err != error.MissingExtractionOperation);
            try std.testing.expect(err != error.MixedExtractionOperations);
            try std.testing.expect(err != error.UnsupportedInput);
            try std.testing.expectEqual(@as(usize, 0), node.inference_admission.inFlightUnits());
            try std.testing.expectEqual(@as(usize, 0), node.inference_admission.inFlightRequests());
            continue;
        };
        defer response.deinit();
        return error.TestUnexpectedResult;
    }
    try std.testing.expectEqual(@as(usize, 64), request_work_test_counters.model_resolution_attempts);
}

test "direct entity extraction response populates requested canonical fields" {
    const ner = @import("../pipelines/ner.zig");
    const entities = [_]ner.Entity{.{
        .text = "Grace Hopper",
        .label = "person",
        .start = 0,
        .end = 12,
        .score = 0.889,
    }};
    const batches = [_][]const ner.Entity{&entities};
    const input_ids = [_]?[]const u8{"doc-1"};
    const request = Node.EntityExtractionRequest{
        .model = "antflydb/gliner2-base-v1",
        .include_confidence = true,
        .include_spans = true,
        .input_ids = &input_ids,
    };
    for (0..64) |_| {
        const json = try entityExtractionResponseJsonAlloc(
            std.testing.allocator,
            request,
            &batches,
            null,
            &.{"Grace Hopper built compilers."},
        );
        defer std.testing.allocator.free(json);
        try ant_json.testing.expectSubsetJsonText(std.testing.allocator,
            \\{
            \\  "object": "extraction",
            \\  "model": "antflydb/gliner2-base-v1",
            \\  "data": [{
            \\    "id": "doc-1",
            \\    "entities": [{
            \\      "text": "Grace Hopper",
            \\      "label": "person",
            \\      "start": 0,
            \\      "end": 12,
            \\      "score": 0.8889999985694885
            \\    }]
            \\  }]
            \\}
        , json);
    }
}

test "raw entity cleanup preserves borrowed labels" {
    const allocator = std.testing.allocator;
    const ner = @import("../pipelines/ner.zig");
    const borrowed_label = "person";
    const batches = try allocator.alloc([]ner.Entity, 1);
    batches[0] = try allocator.alloc(ner.Entity, 1);
    batches[0][0] = .{
        .text = try allocator.dupe(u8, "Grace Hopper"),
        .label = borrowed_label,
        .start = 0,
        .end = 12,
        .score = 0.889,
    };

    freeBorrowedLabelEntityBatches(allocator, batches);
    try std.testing.expectEqualStrings("person", borrowed_label);
}

test "resolved extraction releases partial entity batches on every allocation failure" {
    const ner = @import("../pipelines/ner.zig");
    const first_entities = [_]ner.Entity{ .{
        .text = "Grace Hopper",
        .label = "person",
        .start = 0,
        .end = 12,
        .score = 0.95,
    }, .{
        .text = "COBOL",
        .label = "technology",
        .start = 13,
        .end = 18,
        .score = 0.91,
    } };
    const second_entities = [_]ner.Entity{ .{
        .text = "Ada Lovelace",
        .label = "person",
        .start = 0,
        .end = 12,
        .score = 0.94,
    }, .{
        .text = "Analytical Engine",
        .label = "technology",
        .start = 13,
        .end = 30,
        .score = 0.90,
    } };
    const batches = [_][]const ner.Entity{ &first_entities, &second_entities };
    const relations = [_][]const gliner_mod.Relation{
        &.{.{
            .head = first_entities[0],
            .tail = first_entities[1],
            .label = "created",
            .score = 0.89,
        }},
        &.{.{
            .head = second_entities[0],
            .tail = second_entities[1],
            .label = "described",
            .score = 0.88,
        }},
    };

    const Runner = struct {
        fn run(
            allocator: std.mem.Allocator,
            source: []const []const ner.Entity,
            source_relations: []const []const gliner_mod.Relation,
        ) !void {
            var resolved = try resolveExtractedOutput(allocator, source, source_relations, .{});
            defer resolved.deinit(allocator);
            try std.testing.expectEqual(@as(usize, 2), resolved.entities.len);
            try std.testing.expectEqual(@as(usize, 2), resolved.relations.?.len);
        }
    };

    var fail_index: usize = 0;
    while (true) : (fail_index += 1) {
        var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{
            .fail_index = fail_index,
            .resize_fail_index = 0,
        });
        Runner.run(failing.allocator(), &batches, &relations) catch |err| switch (err) {
            error.OutOfMemory => {
                try std.testing.expect(failing.has_induced_failure);
                try std.testing.expectEqual(failing.allocated_bytes, failing.freed_bytes);
                continue;
            },
            else => return err,
        };
        try std.testing.expect(!failing.has_induced_failure);
        try std.testing.expectEqual(failing.allocated_bytes, failing.freed_bytes);
        break;
    }
}

test "learned entity cleanup releases partial batches on every allocation failure" {
    const ner = @import("../pipelines/ner.zig");
    var head = try cleanup_model_mod.CleanupHead.init(std.testing.allocator, 32, 4, 8);
    defer head.deinit();

    const texts = [_][]const u8{
        "Grace Hopper built compilers.",
        "COBOL influenced business computing.",
    };
    const first_entities = [_]ner.Entity{.{
        .text = "Grace Hopper",
        .label = "person",
        .start = 0,
        .end = 12,
        .score = 0.95,
    }};
    const second_entities = [_]ner.Entity{.{
        .text = "COBOL",
        .label = "technology",
        .start = 0,
        .end = 5,
        .score = 0.91,
    }};
    const batches = [_][]const ner.Entity{ &first_entities, &second_entities };

    const Runner = struct {
        fn run(
            allocator: std.mem.Allocator,
            cleanup_head: *const cleanup_model_mod.CleanupHead,
            source_texts: []const []const u8,
            source_entities: []const []const ner.Entity,
        ) !void {
            const cleaned = (try applyLearnedCleanupIfPresent(
                allocator,
                cleanup_head,
                source_texts,
                source_entities,
            )).?;
            defer freeEntityBatches(allocator, cleaned);
            try std.testing.expectEqual(source_texts.len, cleaned.len);
        }
    };

    var fail_index: usize = 0;
    while (true) : (fail_index += 1) {
        var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{
            .fail_index = fail_index,
            .resize_fail_index = 0,
        });
        Runner.run(failing.allocator(), &head, &texts, &batches) catch |err| switch (err) {
            error.OutOfMemory => {
                try std.testing.expect(failing.has_induced_failure);
                try std.testing.expectEqual(failing.allocated_bytes, failing.freed_bytes);
                continue;
            },
            else => return err,
        };
        try std.testing.expect(!failing.has_induced_failure);
        try std.testing.expectEqual(failing.allocated_bytes, failing.freed_bytes);
        break;
    }
}

test "REBEL rejects unsupported typed schemas before model loading" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.createDirPath(std.testing.io, "models/owner/rebel");
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "models/owner/rebel/config.json",
        .data = "{}",
    });
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "models/owner/rebel/rebel_config.json",
        .data = "{}",
    });
    const models_root = try tmp.dir.realPathFileAlloc(std.testing.io, "models", allocator);
    defer allocator.free(models_root);

    var node = try Node.init(allocator, .{
        .models_dir = models_root,
        .allow_unknown_models = true,
    });
    defer node.deinit();
    const input_ids = [_]?[]const u8{null};
    const texts = [_][]const u8{"Grace Hopper served in the Navy."};
    const direct_inputs = [_]extracting_api.Input{.{ .content_json = "\"Grace Hopper served in the Navy.\"" }};

    resetRequestWorkTestCounters();
    try std.testing.expectError(
        error.UnsupportedRebelEntityLabels,
        node.extractDirect(allocator, "owner/rebel", .{
            .inputs = &direct_inputs,
            .schema_json = "{\"entities\":[\"person\"]}",
        }),
    );
    try std.testing.expectEqual(@as(usize, 1), request_work_test_counters.model_resolution_attempts);
    try std.testing.expectEqual(@as(usize, 0), request_work_test_counters.model_load_attempts);
    try std.testing.expectEqual(@as(usize, 0), node.inference_admission.inFlightUnits());
    try std.testing.expectEqual(@as(usize, 0), node.inference_admission.inFlightRequests());

    const cases = [_]struct {
        labels: ?[]const []const u8,
        relation_labels: ?[]const []const u8,
        want_relations: bool,
        expected_message: []const u8,
    }{
        .{
            .labels = &.{"person"},
            .relation_labels = null,
            .want_relations = false,
            .expected_message = "REBEL supports only the generic entity label 'ENTITY'",
        },
        .{
            .labels = null,
            .relation_labels = &.{"person::served in::organization"},
            .want_relations = true,
            .expected_message = "source and target constraints must use the generic 'ENTITY' label",
        },
    };

    for (cases) |case| {
        resetRequestWorkTestCounters();
        var request = try httpx.Request.init(allocator, .POST, "/ai/v1/extract");
        defer request.deinit();
        var ctx = httpx.Context.init(allocator, std.testing.io, &request);
        defer ctx.deinit();

        var response = try node.extractEntitiesAndRelations(
            &ctx,
            .{
                .model = "owner/rebel",
                .labels = case.labels,
                .relation_labels = case.relation_labels,
                .input_ids = &input_ids,
            },
            &texts,
            case.want_relations,
        );
        defer response.deinit();

        try std.testing.expectEqual(@as(u16, 400), response.status.code);
        try ant_json.testing.expectSubsetJsonText(
            allocator,
            "{\"error\":\"INVALID_REQUEST\"}",
            response.body.?,
        );
        try std.testing.expect(std.mem.indexOf(u8, response.body.?, case.expected_message) != null);
        try std.testing.expectEqual(@as(usize, 1), request_work_test_counters.model_resolution_attempts);
        try std.testing.expectEqual(@as(usize, 0), request_work_test_counters.model_load_attempts);
    }
}

test "canonical structure schemas preserve field types and choices" {
    var parsed = try std.json.parseFromSlice(
        extraction_api.ExtractionSchema,
        std.testing.allocator,
        \\{"structures":{"person":{"fields":{"name":"string","tags":{"type":"array"},"role":{"type":"str","enum":["engineer","manager"]}}}}}
    ,
        .{ .allocate = .alloc_always },
    );
    defer parsed.deinit();
    const schemas = try parseCanonicalStructureSchemas(std.testing.allocator, parsed.value);
    defer {
        for (schemas) |*schema| schema.deinit(std.testing.allocator);
        std.testing.allocator.free(schemas);
    }

    try std.testing.expectEqual(@as(usize, 1), schemas.len);
    try std.testing.expectEqualStrings("person", schemas[0].name);
    try std.testing.expectEqual(@as(usize, 3), schemas[0].fields.len);
    for (schemas[0].fields) |field| {
        if (std.mem.eql(u8, field.name, "name")) {
            try std.testing.expectEqual(extraction_mod.FieldType.str, field.field_type);
        } else if (std.mem.eql(u8, field.name, "tags")) {
            try std.testing.expectEqual(extraction_mod.FieldType.list, field.field_type);
        } else if (std.mem.eql(u8, field.name, "role")) {
            try std.testing.expectEqual(@as(usize, 2), field.choices.len);
            try std.testing.expectEqualStrings("engineer", field.choices[0]);
            try std.testing.expectEqualStrings("manager", field.choices[1]);
        } else return error.UnexpectedField;
    }
}

test "canonical structure schemas reject invalid enum descriptors without leaking" {
    var parsed = try std.json.parseFromSlice(
        extraction_api.ExtractionSchema,
        std.testing.allocator,
        \\{"structures":{"person":{"fields":{"role":{"enum":["only-one"]}}}}}
    ,
        .{ .allocate = .alloc_always },
    );
    defer parsed.deinit();
    try std.testing.expectError(
        error.InvalidStructureField,
        parseCanonicalStructureSchemas(std.testing.allocator, parsed.value),
    );
}

test "canonical structured extraction rejects empty and mixed input batches" {
    try std.testing.expectError(error.UnsupportedInput, validateExtractionInputKinds(0, 0));
    try std.testing.expectError(error.UnsupportedInput, validateExtractionInputKinds(1, 1));
    try validateExtractionInputKinds(2, 0);
    try validateExtractionInputKinds(0, 2);
}

test "component session pool adapter retains explicit resource-managed backend policy" {
    const session_manager = backends_mod.SessionManager.init(std.testing.allocator);
    var manager = model_manager_mod.ModelManager.init(std.testing.allocator, session_manager);
    defer manager.deinit();

    var loader = model_manager_mod.ModelManager.ComponentLoader{ .manager = &manager };
    loader.allowed_backends[0] = .native;
    loader.allowed_backends[1] = .metal;
    loader.allowed_backend_count = 2;
    std.crypto.hash.sha2.Sha256.hash(
        "/models/component.onnx",
        &loader.component_path_digests[0],
        .{},
    );
    loader.component_path_count = 1;
    const strict = try loader.restrictToBackend(.metal);
    try std.testing.expectEqualSlices(
        backends_mod.BackendType,
        &.{.metal},
        strict.preferredBackends(),
    );
    var pool_loader = try loader.sessionPoolLoader();
    defer pool_loader.deinit();
    loader = undefined;
    try std.testing.expectError(
        error.UnvalidatedModelComponent,
        pool_loader.load("/models/substituted.onnx"),
    );
}

test "registerAiRoutesOn excludes Traditional ML predictor routes" {
    var node = try Node.init(std.testing.allocator, .{});
    defer node.deinit();

    var server = RecordingServer{ .allocator = std.testing.allocator };
    defer server.deinit();

    try node.registerAiRoutesOn(ai_api_prefix, &server);

    try std.testing.expect(server.hasRoute(.post, ai_api_prefix ++ "/embed"));
    try std.testing.expect(server.hasRoute(.post, ai_api_prefix ++ "/embeddings"));
    try std.testing.expect(server.hasRoute(.post, ai_api_prefix ++ "/generate/batch"));
    try std.testing.expect(server.hasRoute(.get, ai_api_prefix ++ "/models"));
    try std.testing.expect(server.hasRoute(.post, ai_api_prefix ++ "/recognize"));
    try std.testing.expect(!server.hasRoute(.post, ai_api_prefix ++ "/predict"));
    try std.testing.expect(!server.hasRoute(.get, ai_api_prefix ++ "/predictors"));
}

test "root operational routes stay outside inference API prefix" {
    var node = try Node.init(std.testing.allocator, .{});
    defer node.deinit();
    var server = RecordingServer{ .allocator = std.testing.allocator };
    defer server.deinit();

    try node.registerRootOperationalRoutes(&server);

    try std.testing.expect(server.hasRoute(.get, "/healthz"));
    try std.testing.expect(server.hasRoute(.get, "/readyz"));
    try std.testing.expect(!server.hasRoute(.get, public_api_prefix ++ "/healthz"));
    try std.testing.expect(!server.hasRoute(.get, public_api_prefix ++ "/readyz"));
}

test "readyz serves only the published inventory snapshot" {
    const allocator = std.testing.allocator;
    var node = try Node.init(allocator, .{
        // A request-path filesystem probe would fail against this path. The
        // published snapshot must nevertheless be returned successfully.
        .models_dir = "/nonexistent/antfly-readyz-must-not-scan",
    });
    defer node.deinit();

    {
        var request = try httpx.Request.init(allocator, .GET, "/readyz");
        defer request.deinit();
        var ctx = httpx.Context.init(allocator, std.testing.io, &request);
        defer ctx.deinit();

        var response = try node.readyzHandler(&ctx);
        defer response.deinit();
        try std.testing.expectEqual(@as(u16, 503), response.status.code);
        try std.testing.expect(std.mem.indexOf(u8, response.body.?, "\"status\":\"not_ready\"") != null);
    }

    node.readiness_inventory.publish(.{ .generators = 2, .embedders = 1 });
    {
        var request = try httpx.Request.init(allocator, .GET, "/readyz");
        defer request.deinit();
        var ctx = httpx.Context.init(allocator, std.testing.io, &request);
        defer ctx.deinit();

        var response = try node.readyzHandler(&ctx);
        defer response.deinit();
        try std.testing.expectEqual(@as(u16, 200), response.status.code);
        try std.testing.expect(std.mem.indexOf(u8, response.body.?, "\"status\":\"ready\"") != null);
        try std.testing.expect(std.mem.indexOf(u8, response.body.?, "\"generators\":2") != null);
        try std.testing.expect(std.mem.indexOf(u8, response.body.?, "\"embedders\":1") != null);
    }

    node.session_manager.required_backend_invalid = true;
    {
        var request = try httpx.Request.init(allocator, .GET, "/readyz");
        defer request.deinit();
        var ctx = httpx.Context.init(allocator, std.testing.io, &request);
        defer ctx.deinit();

        var response = try node.readyzHandler(&ctx);
        defer response.deinit();
        try std.testing.expectEqual(@as(u16, 503), response.status.code);
        try std.testing.expect(std.mem.indexOf(u8, response.body.?, "\"status\":\"not_ready\"") != null);
    }
}

test "failed readiness refresh preserves the last known good inventory" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "models-is-a-file",
        .data = "not a directory",
    });
    const models_path = try std.fs.path.join(allocator, &.{
        ".zig-cache",
        "tmp",
        tmp.sub_path[0..],
        "models-is-a-file",
    });
    defer allocator.free(models_path);

    var node = try Node.init(allocator, .{ .models_dir = models_path });
    defer node.deinit();
    node.readiness_inventory.publish(.{ .classifiers = 1 });

    try std.testing.expectError(
        error.NotDir,
        node.refreshReadinessInventory(std.testing.io),
    );
    const snapshot = node.readiness_inventory.load();
    try std.testing.expect(snapshot.initialized);
    try std.testing.expectEqual(@as(usize, 1), snapshot.counts.classifiers);
}

test "readiness refresh observes an externally published model" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const models_path = try std.fs.path.join(allocator, &.{
        ".zig-cache",
        "tmp",
        tmp.sub_path[0..],
    });
    defer allocator.free(models_path);

    var node = try Node.init(allocator, .{ .models_dir = models_path });
    defer node.deinit();
    try node.refreshReadinessInventory(std.testing.io);
    try std.testing.expectEqual(@as(usize, 0), node.readiness_inventory.load().counts.total());

    // Model pulls happen in another process and atomically publish directories
    // into this cache. A tiny GGUF marker is enough for listing discovery; no
    // model is loaded or parsed by this test.
    try tmp.dir.createDirPath(std.testing.io, "generators/acme/demo");
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "generators/acme/demo/model.gguf",
        .data = "listing-only fixture",
    });
    try node.refreshReadinessInventory(std.testing.io);

    const refreshed = node.readiness_inventory.load();
    try std.testing.expect(refreshed.initialized);
    try std.testing.expectEqual(@as(usize, 1), refreshed.counts.generators);
}

test "readiness inventory initializes once and owns its refresh task" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const models_path = try std.fs.path.join(allocator, &.{
        ".zig-cache",
        "tmp",
        tmp.sub_path[0..],
    });
    defer allocator.free(models_path);

    var node = try Node.init(allocator, .{ .models_dir = models_path });
    defer node.deinit();
    node.startReadinessInventory(std.testing.io);
    node.startReadinessInventory(std.testing.io);

    const snapshot = node.readiness_inventory.load();
    try std.testing.expect(snapshot.initialized);
    try std.testing.expectEqual(@as(usize, 0), snapshot.counts.total());
    try std.testing.expect(node.readiness_refresh_started);
    try std.testing.expect(node.readiness_refresh_io != null);
}

test "internal error response hides implementation error names" {
    const allocator = std.testing.allocator;
    var request = try httpx.Request.init(allocator, .GET, "/ai/v1/models");
    defer request.deinit();
    var ctx = httpx.Context.init(allocator, std.testing.io, &request);
    defer ctx.deinit();

    var response = try internalErrorResponse(&ctx, "MODEL_LOAD_FAILED", error.SecretModelFilesystemFailure);
    defer response.deinit();

    try std.testing.expectEqual(@as(u16, 500), response.status.code);
    try std.testing.expect(std.mem.indexOf(u8, response.body.?, "\"error\":\"MODEL_LOAD_FAILED\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.body.?, internal_error_message) != null);
    try std.testing.expect(std.mem.indexOf(u8, response.body.?, "SecretModelFilesystemFailure") == null);
}

test "post-preprocessing prompt overflow remains a client generation error" {
    const allocator = std.testing.allocator;
    var request = try httpx.Request.init(allocator, .POST, "/ai/v1/generate");
    defer request.deinit();
    var ctx = httpx.Context.init(allocator, std.testing.io, &request);
    defer ctx.deinit();

    var response = try generationErrorResponse(&ctx, error.PromptTooLong);
    defer response.deinit();

    try std.testing.expectEqual(@as(u16, 400), response.status.code);
    try std.testing.expect(std.mem.indexOf(u8, response.body.?, "\"error\":\"INVALID_REQUEST\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.body.?, "prompt exceeds the model context window") != null);

    const batch_error = batchGenerationError(error.PromptTooLong);
    try std.testing.expectEqualStrings("INVALID_REQUEST", batch_error.code);
    try std.testing.expect(!batch_error.retryable);
}

test "generation memory exhaustion is an actionable non-retryable capacity error" {
    const allocator = std.testing.allocator;
    var request = try httpx.Request.init(allocator, .POST, "/ai/v1/generate");
    defer request.deinit();
    var ctx = httpx.Context.init(allocator, std.testing.io, &request);
    defer ctx.deinit();

    var response = try generationErrorResponse(&ctx, error.MemoryBudgetExceeded);
    defer response.deinit();

    try std.testing.expectEqual(@as(u16, 507), response.status.code);
    try std.testing.expect(std.mem.indexOf(u8, response.body.?, "\"error\":\"MEMORY_BUDGET_EXCEEDED\"") != null);
    for ([_][]const u8{
        "--host-budget-mb",
        "--backend-budget-mb",
        "--combined-budget-mb",
        "--kv-budget-mb",
        "--scratch-budget-mb",
        "--process-memory-budget-mb",
    }) |option| {
        try std.testing.expect(std.mem.indexOf(u8, response.body.?, option) != null);
    }
    try std.testing.expect(std.mem.indexOf(u8, response.body.?, "\"retryable\":false") != null);

    const batch_error = batchGenerationError(error.MemoryBudgetExceeded);
    try std.testing.expectEqualStrings("MEMORY_BUDGET_EXCEEDED", batch_error.code);
    try std.testing.expect(!batch_error.retryable);
}

test "generation live pressure is an actionable retryable capacity error" {
    const allocator = std.testing.allocator;
    var request = try httpx.Request.init(allocator, .POST, "/ai/v1/generate");
    defer request.deinit();
    var ctx = httpx.Context.init(allocator, std.testing.io, &request);
    defer ctx.deinit();

    var response = try generationErrorResponse(&ctx, error.ResourceTemporarilyUnavailable);
    defer response.deinit();

    try std.testing.expectEqual(@as(u16, 503), response.status.code);
    try std.testing.expect(std.mem.indexOf(u8, response.body.?, "\"error\":\"MODEL_RESOURCE_BUSY\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.body.?, "\"retryable\":true") != null);

    const batch_error = batchGenerationError(error.ResourceTemporarilyUnavailable);
    try std.testing.expectEqualStrings("MODEL_RESOURCE_BUSY", batch_error.code);
    try std.testing.expect(batch_error.retryable);
}

test "registerRoutesOn supports alternate prefixes through the shared router" {
    var node = try Node.init(std.testing.allocator, .{});
    defer node.deinit();

    var server = RecordingServer{ .allocator = std.testing.allocator };
    defer server.deinit();

    try node.registerRoutesOn("/custom/v9", &server);

    try std.testing.expect(server.hasRoute(.post, "/custom/v9/embed"));
    try std.testing.expect(server.hasRoute(.post, "/custom/v9/embeddings"));
    try std.testing.expect(server.hasRoute(.get, "/custom/v9/models"));
    try std.testing.expect(server.hasRoute(.get, "/custom/v9/metrics"));
}

test "budget overrides apply selectively" {
    const defaults: runtime.tier.memory.Limits = .{
        .host_limit_bytes = 100,
        .backend_limit_bytes = 200,
        .combined_limit_bytes = 300,
        .kv_limit_bytes = 400,
        .scratch_limit_bytes = 500,
    };
    const applied = (BudgetOverrides{
        .host_limit_bytes = 150,
        .combined_limit_bytes = 350,
        .scratch_limit_bytes = 600,
    }).apply(defaults);
    try std.testing.expectEqual(@as(usize, 150), applied.host_limit_bytes);
    try std.testing.expectEqual(@as(usize, 200), applied.backend_limit_bytes);
    try std.testing.expectEqual(@as(usize, 350), applied.combined_limit_bytes);
    try std.testing.expectEqual(@as(usize, 400), applied.kv_limit_bytes);
    try std.testing.expectEqual(@as(usize, 600), applied.scratch_limit_bytes);
}

test "generation batching keeps the experimental CUDA envelope default-off" {
    const config = GenerationBatchingConfig{};
    try std.testing.expectEqual(GenerationBatchingMode.off, config.mode);
    try std.testing.expect(!(GenerationBatchingConfig{ .mode = .auto }).enabledForRequest(.cuda, false, false, false));

    const policy = config.schedulerPolicy();
    try std.testing.expectEqual(@as(usize, 2), policy.max_step_items);
    try std.testing.expectEqual(@as(?usize, 2), policy.max_active_requests_for_batching);
    try std.testing.expectEqual(@as(u32, 0), policy.max_decode_wait_us);
    try std.testing.expectEqual(@as(usize, 2048), policy.max_idle_prefill_chunk_size);

    const enabled_policy = (GenerationBatchingConfig{ .mode = .on }).schedulerPolicy();
    try std.testing.expectEqual(@as(u32, 1_000), enabled_policy.max_decode_wait_us);

    const clamped = (GenerationBatchingConfig{ .max_step_items = 128 }).schedulerPolicy();
    try std.testing.expectEqual(@as(usize, 2), clamped.max_step_items);
    try std.testing.expectEqual(
        @as(usize, 32),
        (GenerationBatchingConfig{ .max_idle_prefill_chunk_size = 1 }).schedulerPolicy().max_idle_prefill_chunk_size,
    );
    try std.testing.expectEqual(
        @as(usize, 2048),
        (GenerationBatchingConfig{ .max_idle_prefill_chunk_size = 4096 }).schedulerPolicy().max_idle_prefill_chunk_size,
    );
}

test "native generation applies scheduler policy before the first lease" {
    var node = try Node.init(std.testing.allocator, .{
        .generation_batching = .{ .mode = .on, .max_step_items = 128 },
    });
    defer node.deinit();

    var coordinator = runtime.scheduler.native_generate.NativeGenerateCoordinator.init(std.testing.allocator);
    defer coordinator.deinit();
    const leases = [_]runtime.scheduler.native_generate.Lease{
        try node.acquireNativeGenerateLease(&coordinator, .{ .requested_units = 1, .prompt_bytes = 8, .max_tokens = 8 }),
        try node.acquireNativeGenerateLease(&coordinator, .{ .requested_units = 1, .prompt_bytes = 8, .max_tokens = 8 }),
        try node.acquireNativeGenerateLease(&coordinator, .{ .requested_units = 1, .prompt_bytes = 8, .max_tokens = 8 }),
    };
    defer for (leases) |lease| coordinator.release(lease);

    try std.testing.expectEqual(@as(usize, 3), leases[2].active_requests_snapshot);
    try std.testing.expectEqual(@as(usize, 1), coordinator.defaultStepBudget().max_items);
}

test "generation batching treats every requested CUDA graph replay mode as serialized" {
    try std.testing.expect(!cudaDecodeGraphReplayRequested(null));
    inline for (.{ "off", "0", "false", "no" }) |value| {
        try std.testing.expect(!cudaDecodeGraphReplayRequested(value));
    }
    inline for (.{ "auto", "required", "on", "1", "true", "yes", "unknown", "" }) |value| {
        try std.testing.expect(cudaDecodeGraphReplayRequested(value));
    }
}

test "generation batching serializes request paths outside the row scheduler" {
    const config = GenerationBatchingConfig{ .mode = .on };
    try std.testing.expect(config.enabledForRequest(.cuda, false, false, false));
    try std.testing.expect(!config.enabledForRequest(.cuda, true, false, false));
    try std.testing.expect(!config.enabledForRequest(.cuda, false, true, false));
    try std.testing.expect(!config.enabledForRequest(.cuda, false, false, true));
    try std.testing.expect(!config.enabledForRequest(.metal, false, false, false));
}

test "prompt cache stays disabled while CUDA continuous batching releases the model lock" {
    try std.testing.expect(promptCacheEligibleForNativeRequest(true, false, .cuda, false, false, false));
    try std.testing.expect(!promptCacheEligibleForNativeRequest(true, true, .cuda, false, false, false));
    try std.testing.expect(!promptCacheEligibleForNativeRequest(true, false, .metal, true, false, false));
}

test "taskMatchesModelListing exposes extraction-capable models only as extractors" {
    try std.testing.expect(taskMatchesModelListing("extractors", "extractor", "", &.{}, &.{}, false));
    try std.testing.expect(taskMatchesModelListing("extractors", "recognizer", "", &.{}, &.{"extraction"}, false));
    try std.testing.expect(taskMatchesModelListing("extractors", "reader", "", &.{}, &.{"extraction"}, false));
    try std.testing.expect(taskMatchesModelListing("extractors", "recognizer", "gliner2", &.{}, &.{"labels"}, true));
    try std.testing.expect(taskMatchesModelListing("extractors", "classifier", "", &.{"classify"}, &.{}, true));
    try std.testing.expect(!taskMatchesModelListing("extractors", "classifier", "", &.{"classify"}, &.{}, false));
    try std.testing.expect(!taskMatchesModelListing("classifiers", "classifier", "", &.{"classify"}, &.{}, true));
    try std.testing.expect(!taskMatchesModelListing("classifiers", "recognizer", "gliner2", &.{}, &.{"classification"}, true));
}

test "classification extraction applies top-k to single-label and threshold to multi-label taxonomies" {
    const Result = struct { label: []const u8, score: f32 };
    const row = [_]Result{
        .{ .label = "first", .score = 0.9 },
        .{ .label = "second", .score = 0.7 },
        .{ .label = "third", .score = 0.4 },
    };
    const batch = [_][]const Result{&row};

    var single_lists = [_]std.ArrayListUnmanaged(extraction_api.ExtractionClassification){.empty};
    defer single_lists[0].deinit(std.testing.allocator);
    try appendExtractionClassificationBatch(
        std.testing.allocator,
        &single_lists,
        .{ .name = "topic", .labels = &.{ "first", "second", "third" }, .top_k = 2 },
        .{ .threshold = 0.95, .include_confidence = true },
        &batch,
    );
    try std.testing.expectEqual(@as(usize, 2), single_lists[0].items.len);
    try std.testing.expectEqualStrings("first", single_lists[0].items[0].label);
    try std.testing.expectEqualStrings("second", single_lists[0].items[1].label);

    var multi_lists = [_]std.ArrayListUnmanaged(extraction_api.ExtractionClassification){.empty};
    defer multi_lists[0].deinit(std.testing.allocator);
    try appendExtractionClassificationBatch(
        std.testing.allocator,
        &multi_lists,
        .{ .name = "topic", .labels = &.{ "first", "second", "third" }, .multi_label = true, .top_k = 1 },
        .{ .threshold = 0.5, .include_confidence = true },
        &batch,
    );
    try std.testing.expectEqual(@as(usize, 2), multi_lists[0].items.len);
}

test "NLI entailment labels are detected case-insensitively" {
    const labels = [_][]const u8{ "CONTRADICTION", "NEUTRAL", "ENTAILMENT" };
    try std.testing.expectEqual(@as(?usize, 2), nliEntailmentIndex(&labels));
    try std.testing.expectEqual(@as(?usize, null), nliEntailmentIndex(null));
}

test "taskMatchesModelListing prefers explicit tasks when present" {
    try std.testing.expect(taskMatchesModelListing("generators", "generator", "", &.{"generate"}, &.{}, false));
    try std.testing.expect(taskMatchesModelListing("extractors", "generator", "", &.{"extract"}, &.{}, false));
    try std.testing.expect(!taskMatchesModelListing("extractors", "generator", "", &.{"generate"}, &.{"extraction"}, false));
}

test "generate backend selection keeps compiled mode explicit" {
    const eager_webgpu = parseGenerateBackendSelection(.webgpu, null, null);
    if (build_options.enable_wasm and build_options.enable_webgpu) {
        const eager = try eager_webgpu;
        try std.testing.expectEqual(native_backend_choice.Choice.webgpu, eager.native_choice);
        try std.testing.expectEqual(@as(?ops.BackendKind, null), eager.compiled_partition_backend);
        try std.testing.expect(!eager.graph_mode_requested);

        const compiled = try parseGenerateBackendSelection(.webgpu, "compiled", null);
        try std.testing.expectEqual(native_backend_choice.Choice.webgpu, compiled.native_choice);
        try std.testing.expectEqual(@as(?ops.BackendKind, .webgpu), compiled.compiled_partition_backend);
        try std.testing.expectEqual(graph_mod.compiled_backend.AttachmentTarget.partitioned, compiled.compiled_attachment_target);
        try std.testing.expect(compiled.graph_mode_requested);
    } else {
        try std.testing.expectError(error.BackendUnavailable, eager_webgpu);
    }

    const auto_compiled = try parseGenerateBackendSelection(null, "compiled", null);
    try std.testing.expectEqual(native_backend_choice.Choice.auto, auto_compiled.native_choice);
    try std.testing.expectEqual(@as(?ops.BackendKind, null), auto_compiled.compiled_partition_backend);
    try std.testing.expect(auto_compiled.graph_mode_requested);

    const auto_default = try parseGenerateBackendSelection(null, null, null);
    try std.testing.expectEqual(build_options.enable_metal, shouldAutoUseMetalWholeModelGenerate(.metal, true, false, false, false, auto_default));
    try std.testing.expect(!shouldAutoUseMetalWholeModelGenerate(.native, true, false, false, false, auto_default));
    try std.testing.expect(!shouldAutoUseMetalWholeModelGenerate(.metal, false, false, false, false, auto_default));
    try std.testing.expect(!shouldAutoUseMetalWholeModelGenerate(.metal, true, true, false, false, auto_default));
    try std.testing.expect(!shouldAutoUseMetalWholeModelGenerate(.metal, true, false, true, false, auto_default));
    try std.testing.expect(!shouldAutoUseMetalWholeModelGenerate(.metal, true, false, false, true, auto_default));
    try validatePromptCacheExecutionMode(true, auto_default);

    const explicit_compiled = try parseGenerateBackendSelection(null, "compiled", null);
    try std.testing.expectError(
        error.PromptCacheCompiledModeUnsupported,
        validatePromptCacheExecutionMode(true, explicit_compiled),
    );
    try validatePromptCacheExecutionMode(false, explicit_compiled);

    if (build_options.enable_metal) {
        const metal_eager = try parseGenerateBackendSelection(.metal, "eager", null);
        try std.testing.expect(metal_eager.eager_mode_requested);
        try std.testing.expect(!shouldAutoUseMetalWholeModelGenerate(.metal, true, false, false, false, metal_eager));
    } else {
        try std.testing.expectError(
            error.BackendUnavailable,
            parseGenerateBackendSelection(.metal, "eager", null),
        );
    }

    try std.testing.expectError(error.InvalidGenerateMode, parseGenerateBackendSelection(null, "graph", null));
    try std.testing.expectError(error.InvalidCompiledTarget, parseGenerateBackendSelection(null, "compiled", "full"));
}

test "singleBackendPreference is strict" {
    try std.testing.expectEqualSlices(backends_mod.BackendType, &.{.metal}, singleBackendPreference(.metal));
}

test "HTTP model identifiers reject path and malformed variant syntax" {
    for ([_][]const u8{
        "model",
        "owner/model",
        "model:q4_0",
        "owner/model:q4_0",
        "hf:owner/model",
        "hf:owner/model:q4_0",
        "owner/model:gguf:Q4_K_M",
    }) |identifier| try validateRequestModelIdentifier(identifier);

    for ([_][]const u8{
        "",
        "/tmp/model",
        ".",
        "..",
        "../model",
        "owner/../model",
        "owner//model",
        "owner/model/",
        "owner\\model",
        "owner/model:",
        "owner/model:q4::extra",
        "hf:",
        "hf:/model",
        "owner\x00/model",
    }) |identifier| try std.testing.expectError(error.InvalidModelIdentifier, validateRequestModelIdentifier(identifier));

    try std.testing.expectEqual(RequestModelResolutionErrorKind.invalid, requestModelResolutionErrorKind(error.InvalidModelIdentifier));
    try std.testing.expectEqual(RequestModelResolutionErrorKind.missing, requestModelResolutionErrorKind(error.ModelNotFound));
}

test "HTTP model resolution is canonical and contained while trusted resolution accepts absolute paths" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.createDirPath(std.testing.io, "models/owner/model");
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "models/owner/model/config.json", .data = "{}" });
    const models_root = try tmp.dir.realPathFileAlloc(std.testing.io, "models", alloc);
    defer alloc.free(models_root);
    const model_root = try tmp.dir.realPathFileAlloc(std.testing.io, "models/owner/model", alloc);
    defer alloc.free(model_root);
    const explicit_ref = try registry_mod.ModelRef.parse("owner/model:gguf:Q4_K_M");
    const explicit_variant_root = try registry_mod.modelInstallDirAlloc(alloc, models_root, explicit_ref);
    defer alloc.free(explicit_variant_root);
    try std.Io.Dir.cwd().createDirPath(std.testing.io, explicit_variant_root);
    const explicit_variant_config = try std.fs.path.join(alloc, &.{ explicit_variant_root, "config.json" });
    defer alloc.free(explicit_variant_config);
    try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = explicit_variant_config, .data = "{}" });

    var node: Node = undefined;
    node.config = .{ .models_dir = models_root };
    node.allocator = alloc;
    var request_arena = std.heap.ArenaAllocator.init(alloc);
    defer request_arena.deinit();
    const request_allocator = request_arena.allocator();

    const resolved = try node.resolveRequestModelPath(request_allocator, std.testing.io, "hf:owner/model:q4_0", "generators");
    defer request_allocator.free(resolved);
    try std.testing.expectEqualStrings(model_root, resolved);
    const explicit_resolved = try node.resolveRequestModelPath(request_allocator, std.testing.io, "owner/model:gguf:Q4_K_M", "generators");
    defer request_allocator.free(explicit_resolved);
    try std.testing.expectEqualStrings(explicit_variant_root, explicit_resolved);
    const trusted_resolved = try node.resolveModelPath(std.testing.io, model_root, "generators");
    defer alloc.free(trusted_resolved);
    try std.testing.expectEqualStrings(model_root, trusted_resolved);

    const builtin_os = @import("builtin").os.tag;
    if (builtin_os != .windows and builtin_os != .wasi and builtin_os != .freestanding) {
        try tmp.dir.createDirPath(std.testing.io, "outside");
        try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "outside/config.json", .data = "{}" });
        try tmp.dir.symLink(std.testing.io, "../outside", "models/link", .{});
        try std.testing.expectError(error.ModelOutsideModelsDir, node.resolveRequestModelPath(request_allocator, std.testing.io, "link", "generators"));
    }
}

test "trusted model resolution prefers an exact legacy path before receipt discovery" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.createDirPath(io, "models/owner/model");
    try tmp.dir.writeFile(io, .{ .sub_path = "models/owner/model/config.json", .data = "{}" });
    inline for (.{ "duplicate-a", "duplicate-b" }) |leaf| {
        try tmp.dir.createDirPath(io, "models/" ++ leaf);
        try tmp.dir.writeFile(io, .{ .sub_path = "models/" ++ leaf ++ "/config.json", .data = "{}" });
        try tmp.dir.writeFile(io, .{ .sub_path = "models/" ++ leaf ++ "/model.gguf", .data = "decoder" });
        try tmp.dir.writeFile(io, .{
            .sub_path = "models/" ++ leaf ++ "/.antfly-download-complete.json",
            .data =
            \\{"version":2,"source":{"owner":"owner","name":"model","variant":"q4_0"},"artifacts":[{"path":"config.json","size":2},{"path":"model.gguf","size":7}]}
            ,
        });
    }

    const models_root = try tmp.dir.realPathFileAlloc(io, "models", allocator);
    defer allocator.free(models_root);
    const model_root = try tmp.dir.realPathFileAlloc(io, "models/owner/model", allocator);
    defer allocator.free(model_root);

    var node: Node = undefined;
    node.config = .{ .models_dir = models_root };
    node.allocator = allocator;

    const resolved = try node.resolveModelPath(io, "owner/model:q4_0", "generators");
    defer allocator.free(resolved);
    try std.testing.expectEqualStrings(model_root, resolved);
}

test "HTTP model resolution accepts the managed identity advertised by discovery" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.createDirPath(io, "models/provisioned/arbitrary-leaf");
    try tmp.dir.writeFile(io, .{
        .sub_path = "models/provisioned/arbitrary-leaf/config.json",
        .data = "{}",
    });
    try tmp.dir.writeFile(io, .{
        .sub_path = "models/provisioned/arbitrary-leaf/model.gguf",
        .data = "decoder",
    });
    try tmp.dir.writeFile(io, .{
        .sub_path = "models/provisioned/arbitrary-leaf/.antfly-download-complete.json",
        .data =
        \\{"version":2,"source":{"owner":"owner","name":"model","variant":"q8-bundle-v1"},"artifacts":[{"path":"config.json","size":2},{"path":"model.gguf","size":7}]}
        ,
    });

    const models_root = try tmp.dir.realPathFileAlloc(io, "models", allocator);
    defer allocator.free(models_root);
    const model_root = try tmp.dir.realPathFileAlloc(
        io,
        "models/provisioned/arbitrary-leaf",
        allocator,
    );
    defer allocator.free(model_root);

    var node: Node = undefined;
    node.config = .{ .models_dir = models_root };
    node.allocator = allocator;
    node.registry = registry_mod.ModelRegistry.init(allocator, models_root);

    const resolved = try node.resolveRequestModelPath(
        allocator,
        io,
        "owner/model:q8-bundle-v1",
        "rerankers",
    );
    defer allocator.free(resolved);
    try std.testing.expectEqualStrings(model_root, resolved);
}

test "HTTP model resolution caller ownership stays flat across repeated requests" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer std.debug.assert(gpa.deinit() == .ok);
    const allocator = gpa.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.createDirPath(std.testing.io, "models/owner/model");
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "models/owner/model/config.json", .data = "{}" });
    const models_root = try tmp.dir.realPathFileAlloc(std.testing.io, "models", allocator);
    defer allocator.free(models_root);
    const model_root = try tmp.dir.realPathFileAlloc(std.testing.io, "models/owner/model", allocator);
    defer allocator.free(model_root);

    var node: Node = undefined;
    node.config = .{ .models_dir = models_root };
    node.allocator = allocator;

    for (0..64) |_| {
        const resolved = try node.resolveRequestModelPath(allocator, std.testing.io, "hf:owner/model:q4_0", "generators");
        try std.testing.expectEqualStrings(model_root, resolved);
        allocator.free(resolved);
    }
}

test "trusted model resolution caller ownership stays flat across every return shape" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer std.debug.assert(gpa.deinit() == .ok);
    const allocator = gpa.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.createDirPath(std.testing.io, "models/owner/model");
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "models/config.json", .data = "{}" });
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "models/owner/model/config.json", .data = "{}" });
    const models_root = try tmp.dir.realPathFileAlloc(std.testing.io, "models", allocator);
    defer allocator.free(models_root);
    const model_root = try tmp.dir.realPathFileAlloc(std.testing.io, "models/owner/model", allocator);
    defer allocator.free(model_root);

    var node: Node = undefined;
    node.config = .{ .models_dir = models_root };
    node.allocator = allocator;

    for (0..64) |_| {
        const named = try node.resolveModelPath(std.testing.io, "hf:owner/model:q4_0", "generators");
        try std.testing.expectEqualStrings(model_root, named);
        allocator.free(named);

        const absolute = try node.resolveModelPath(std.testing.io, model_root, "generators");
        try std.testing.expectEqualStrings(model_root, absolute);
        allocator.free(absolute);

        const configured_root = try node.resolveModelPath(std.testing.io, null, null);
        try std.testing.expectEqualStrings(models_root, configured_root);
        allocator.free(configured_root);
    }
}

test "download remote content accepts data uri" {
    const alloc = std.testing.allocator;
    const node = Node{
        .config = .{},
        .allocator = undefined,
        .session_manager = undefined,
        .model_manager = undefined,
        .registry = undefined,
        .extraction_reader_resolver = undefined,
        .tabular_registry = undefined,
        .embed_cache = undefined,
        .metrics = undefined,
        .inference_admission = undefined,
    };
    var downloaded = try downloadRemoteContent(&node, alloc, "data:text/plain;base64,aGVsbG8=");
    defer downloaded.deinit(alloc);
    try std.testing.expectEqualStrings("text/plain", downloaded.content_type);
    try std.testing.expectEqualStrings("hello", downloaded.data);
}

test "remote content request errors preserve client, policy, capacity, and upstream semantics" {
    const cases = [_]struct {
        source: anyerror,
        normalized: anyerror,
        status: u16,
        code: []const u8,
        retryable: bool,
    }{
        .{ .source = error.StreamTooLong, .normalized = error.RemoteContentTooLarge, .status = 413, .code = "CONTENT_TOO_LARGE", .retryable = false },
        .{ .source = error.PrivateIpBlocked, .normalized = error.RemoteContentNotAllowed, .status = 403, .code = "CONTENT_NOT_ALLOWED", .retryable = false },
        .{ .source = error.UnsupportedUrlScheme, .normalized = error.RemoteContentInvalid, .status = 400, .code = "INVALID_CONTENT_URL", .retryable = false },
        .{ .source = error.MissingS3Credentials, .normalized = error.RemoteContentNotConfigured, .status = 503, .code = "CONTENT_FETCH_NOT_CONFIGURED", .retryable = false },
        .{ .source = error.HttpFetchFailed, .normalized = error.RemoteContentUnavailable, .status = 502, .code = "CONTENT_FETCH_FAILED", .retryable = true },
    };

    for (cases) |case| {
        const normalized = normalizeRemoteContentRequestError(case.source);
        try std.testing.expectEqual(case.normalized, normalized);
        const failure = remoteContentRequestFailure(normalized).?;
        try std.testing.expectEqual(case.status, failure.status);
        try std.testing.expectEqualStrings(case.code, failure.code);
        try std.testing.expectEqual(case.retryable, failure.retryable);
    }
    try std.testing.expectEqual(error.OutOfMemory, normalizeRemoteContentRequestError(error.OutOfMemory));
    try std.testing.expectEqual(error.Timeout, normalizeRemoteContentRequestError(error.Timeout));
    try std.testing.expectEqual(error.Canceled, normalizeRemoteContentRequestError(error.Canceled));
    try std.testing.expect(remoteContentRequestFailure(error.OutOfMemory) == null);
    try std.testing.expect(remoteContentRequestFailure(error.Timeout) == null);
    try std.testing.expect(remoteContentRequestFailure(error.Canceled) == null);
}

test "request media budget accounting is cumulative and overflow safe" {
    var budget = RequestMediaBudget.init(5);
    try budget.add(3);
    try std.testing.expectEqual(@as(usize, 3), budget.used_bytes);
    try std.testing.expectEqual(@as(usize, 2), budget.remaining());
    try std.testing.expectError(error.RemoteContentTooLarge, budget.add(3));
    try std.testing.expectEqual(@as(usize, 3), budget.used_bytes);

    var overflow_budget = RequestMediaBudget{ .max_bytes = std.math.maxInt(usize), .used_bytes = std.math.maxInt(usize) - 1 };
    try std.testing.expectError(error.RemoteContentTooLarge, overflow_budget.add(2));
    try std.testing.expectEqual(std.math.maxInt(usize) - 1, overflow_budget.used_bytes);

    var zero_budget = RequestMediaBudget.init(0);
    try zero_budget.add(0);
    try std.testing.expectError(error.RemoteContentTooLarge, zero_budget.add(1));
}

test "request media budget clamps an explicit configured maximum to the hard ceiling" {
    var node: Node = undefined;
    node.config = .{ .content_security = .{ .max_download_size_bytes = std.math.maxInt(u64) } };
    try std.testing.expectEqual(default_max_request_media_bytes, requestMediaMaxBytes(&node));
}

test "configured zero media budget rejects nonempty remote and inline media" {
    const alloc = std.testing.allocator;
    var node: Node = undefined;
    node.config = .{ .content_security = .{ .max_download_size_bytes = 0 } };
    try std.testing.expectEqual(@as(usize, 0), requestMediaMaxBytes(&node));

    var remote_budget = RequestMediaBudget.init(requestMediaMaxBytes(&node));
    try std.testing.expectError(
        error.RemoteContentTooLarge,
        downloadRemoteContentWithBudgetForRequest(&node, alloc, "data:image/png;base64,AA==", &remote_budget),
    );
    var empty_remote = try downloadRemoteContentWithBudgetForRequest(
        &node,
        alloc,
        "data:image/png;base64,",
        &remote_budget,
    );
    defer empty_remote.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), empty_remote.data.len);

    var inline_budget = RequestMediaBudget.init(requestMediaMaxBytes(&node));
    try std.testing.expectError(
        error.RemoteContentTooLarge,
        decodeMediaDataWithBudget(alloc, "AA==", &inline_budget),
    );
    var empty = try decodeMediaDataWithBudget(alloc, "", &inline_budget);
    defer empty.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), empty.data.len);
}

test "bounded remote content helper enforces cumulative data URI bytes" {
    const alloc = std.testing.allocator;
    var node: Node = undefined;
    node.config = .{};
    const first_uri = "data:image/png;base64,YWJj";
    var budget = RequestMediaBudget.init(first_uri.len + 1);

    var first = try downloadRemoteContentWithBudgetForRequest(&node, alloc, first_uri, &budget);
    defer first.deinit(alloc);
    try std.testing.expectEqual(first_uri.len, budget.used_bytes);
    try std.testing.expectError(
        error.RemoteContentTooLarge,
        downloadRemoteContentWithBudgetForRequest(&node, alloc, "data:image/png;base64,ZGVm", &budget),
    );
    try std.testing.expectEqual(first_uri.len, budget.used_bytes);
}

test "inline media budgets charge encoded sources before decoding" {
    const alloc = std.testing.allocator;
    resetRequestWorkTestCounters();

    // Four encoded bytes produce one decoded byte; the encoded ceiling is the
    // admission contract and must reject before allocating the output.
    var raw_budget = RequestMediaBudget.init(3);
    try std.testing.expectError(
        error.RemoteContentTooLarge,
        decodeMediaDataWithBudget(alloc, "YQ==", &raw_budget),
    );
    try std.testing.expectEqual(@as(usize, 0), raw_budget.used_bytes);

    const uri = "data:image/png;base64,YQ==";
    var node: Node = undefined;
    node.config = .{};
    var short_budget = RequestMediaBudget.init(uri.len - 1);
    try std.testing.expectError(
        error.RemoteContentTooLarge,
        downloadRemoteContentWithBudgetForRequest(&node, alloc, uri, &short_budget),
    );
    try std.testing.expectEqual(@as(usize, 0), short_budget.used_bytes);

    var exact_budget = RequestMediaBudget.init(uri.len);
    var downloaded = try downloadRemoteContentWithBudgetForRequest(&node, alloc, uri, &exact_budget);
    defer downloaded.deinit(alloc);
    try std.testing.expectEqualStrings("a", downloaded.data);
    try std.testing.expectEqual(uri.len, exact_budget.used_bytes);

    const percent_uri = "data:image/png,%41";
    var percent_budget = RequestMediaBudget.init(percent_uri.len - 1);
    try std.testing.expectError(
        error.RemoteContentTooLarge,
        downloadRemoteContentWithBudgetForRequest(&node, alloc, percent_uri, &percent_budget),
    );
    try std.testing.expectEqual(@as(usize, 0), percent_budget.used_bytes);
    try std.testing.expectEqual(@as(usize, 1), request_work_test_counters.media_fetch_attempts);
}

test "download helpers fail closed for null and explicit empty policies" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "image.png", .data = "png" });
    const file_path = try tmp.dir.realPathFileAlloc(std.testing.io, "image.png", alloc);
    defer alloc.free(file_path);
    const file_uri = try std.fmt.allocPrint(alloc, "file://{s}", .{file_path});
    defer alloc.free(file_uri);

    const configs = [_]NodeConfig{
        .{ .s3_credentials = .{
            .endpoint = @constCast("s3.example.com"),
            .access_key_id = @constCast("test"),
            .secret_access_key = @constCast("test"),
        } },
        .{
            .content_security = .{},
            .s3_credentials = .{
                .endpoint = @constCast("s3.example.com"),
                .access_key_id = @constCast("test"),
                .secret_access_key = @constCast("test"),
            },
        },
    };
    for (configs) |config| {
        var node: Node = undefined;
        node.config = config;
        try std.testing.expectError(error.HostNotAllowed, downloadRemoteContent(&node, alloc, "https://example.com/a.png"));
        try std.testing.expectError(error.HostNotAllowed, downloadReadBatchContent(&node, alloc, "https://example.com/a.png", 1024, 0, .untrusted));
        try std.testing.expectError(error.PathNotAllowed, downloadRemoteContent(&node, alloc, "s3://bucket/object.png"));
        try std.testing.expectError(error.PathNotAllowed, downloadReadBatchContent(&node, alloc, "s3://bucket/object.png", 1024, 0, .untrusted));
        try std.testing.expectError(error.PathNotAllowed, downloadRemoteContent(&node, alloc, file_uri));
        try std.testing.expectError(error.PathNotAllowed, downloadReadBatchContent(&node, alloc, file_uri, 1024, 0, .untrusted));
    }
}

test "partial content policy retains deny by default allowlists" {
    var node: Node = undefined;
    node.config = .{ .content_security = .{ .max_download_size_bytes = 4096 } };
    const effective = effectiveRequestContentSecurity(&node);
    try std.testing.expectEqual(@as(usize, 0), effective.allowed_hosts.?.len);
    try std.testing.expectEqual(@as(usize, 0), effective.allowed_paths.?.len);
    try std.testing.expectEqual(@as(?bool, true), effective.block_private_ips);
    try std.testing.expectEqual(@as(?u64, 4096), effective.max_download_size_bytes);
}

test "explicit private IP override does not implicitly allow hosts or files" {
    var node: Node = undefined;
    node.config = .{ .content_security = .{ .block_private_ips = false } };
    const effective = effectiveRequestContentSecurity(&node);
    try std.testing.expectEqual(@as(usize, 0), effective.allowed_hosts.?.len);
    try std.testing.expectEqual(@as(usize, 0), effective.allowed_paths.?.len);
    try std.testing.expectEqual(@as(?bool, false), effective.block_private_ips);
}

test "download remote content honors an explicit file allowlist" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "image.png", .data = "png" });
    const allowed_root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(allowed_root);
    const file_path = try tmp.dir.realPathFileAlloc(std.testing.io, "image.png", alloc);
    defer alloc.free(file_path);
    const file_uri = try std.fmt.allocPrint(alloc, "file://{s}", .{file_path});
    defer alloc.free(file_uri);
    const allowed_paths = [_][]u8{allowed_root};

    var node: Node = undefined;
    node.config = .{ .content_security = .{ .allowed_paths = &allowed_paths } };
    var downloaded = try downloadRemoteContent(&node, alloc, file_uri);
    defer downloaded.deinit(alloc);
    try std.testing.expectEqualStrings("png", downloaded.data);
}

test "download remote content blocks private ip urls when configured" {
    const alloc = std.testing.allocator;
    const allowed_hosts = [_][]u8{@constCast("127.0.0.1")};
    const node = Node{
        .config = .{ .content_security = .{
            .allowed_hosts = &allowed_hosts,
            .block_private_ips = true,
        } },
        .allocator = undefined,
        .session_manager = undefined,
        .model_manager = undefined,
        .registry = undefined,
        .extraction_reader_resolver = undefined,
        .tabular_registry = undefined,
        .embed_cache = undefined,
        .metrics = undefined,
        .inference_admission = undefined,
    };
    try std.testing.expectError(error.PrivateIpBlocked, downloadRemoteContent(&node, alloc, "http://127.0.0.1/test.png"));
}

test "download remote content blocks hosts outside allowlist" {
    const alloc = std.testing.allocator;
    const allowed_hosts = [_][]u8{@constCast("cdn.example.com")};
    const node = Node{
        .config = .{ .content_security = .{ .allowed_hosts = &allowed_hosts } },
        .allocator = undefined,
        .session_manager = undefined,
        .model_manager = undefined,
        .registry = undefined,
        .extraction_reader_resolver = undefined,
        .tabular_registry = undefined,
        .embed_cache = undefined,
        .metrics = undefined,
        .inference_admission = undefined,
    };
    try std.testing.expectError(error.HostNotAllowed, downloadRemoteContent(&node, alloc, "https://example.com/a.png"));
}

test "chunk request requires input in generated schema" {
    try std.testing.expectError(
        error.MissingField,
        std.json.parseFromSlice(api.ChunkRequest, std.testing.allocator, "{}", .{ .ignore_unknown_fields = true }),
    );
}

test "chunk request input parser rejects invalid content parts" {
    const cases = [_]struct {
        name: []const u8,
        input_json: []const u8,
        expected_error: anyerror,
        expected_message: []const u8,
    }{
        .{
            .name = "empty text input",
            .input_json = "\"\"",
            .expected_error = error.ChunkInputRequired,
            .expected_message = "missing 'input' field",
        },
        .{
            .name = "missing content part type",
            .input_json = "{\"text\":\"hello\"}",
            .expected_error = error.UnsupportedChunkInputContentPartType,
            .expected_message = "input content part type must be 'text' or 'media'",
        },
        .{
            .name = "unsupported content part type",
            .input_json = "{\"type\":\"image_url\",\"image_url\":{\"url\":\"https://example.test/a.png\"}}",
            .expected_error = error.UnsupportedChunkInputContentPartType,
            .expected_message = "input content part type must be 'text' or 'media'",
        },
        .{
            .name = "missing text",
            .input_json = "{\"type\":\"text\"}",
            .expected_error = error.ChunkTextContentPartMissingText,
            .expected_message = "text content part missing 'text' field",
        },
        .{
            .name = "empty text",
            .input_json = "{\"type\":\"text\",\"text\":\"\"}",
            .expected_error = error.ChunkTextContentPartMissingText,
            .expected_message = "text content part missing 'text' field",
        },
        .{
            .name = "missing media data",
            .input_json = "{\"type\":\"media\",\"mime_type\":\"audio/wav\"}",
            .expected_error = error.ChunkMediaContentPartMissingData,
            .expected_message = "media content part missing 'data' field",
        },
        .{
            .name = "empty media data",
            .input_json = "{\"type\":\"media\",\"data\":\"\",\"mime_type\":\"audio/wav\"}",
            .expected_error = error.ChunkMediaContentPartMissingData,
            .expected_message = "media content part missing 'data' field",
        },
        .{
            .name = "empty media data uri payload",
            .input_json = "{\"type\":\"media\",\"data\":\"data:audio/wav;base64,\",\"mime_type\":\"audio/wav\"}",
            .expected_error = error.ChunkMediaContentPartMissingData,
            .expected_message = "media content part missing 'data' field",
        },
        .{
            .name = "missing media mime",
            .input_json = "{\"type\":\"media\",\"data\":\"AA==\"}",
            .expected_error = error.ChunkMediaContentPartMissingMimeType,
            .expected_message = "media content part missing 'mime_type' field",
        },
        .{
            .name = "blank media mime",
            .input_json = "{\"type\":\"media\",\"data\":\"AA==\",\"mime_type\":\"  \"}",
            .expected_error = error.ChunkMediaContentPartMissingMimeType,
            .expected_message = "media content part missing 'mime_type' field",
        },
    };

    for (cases) |case| {
        var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, case.input_json, .{});
        defer parsed.deinit();

        try std.testing.expectError(case.expected_error, parseChunkRequestInput(std.testing.allocator, parsed.value));
        try std.testing.expectEqualStrings(case.expected_message, chunkInputParseErrorMessage(case.expected_error));
    }
}

test "chunk request input parser accepts valid text and media" {
    var text_parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, "{\"type\":\"text\",\"text\":\"hello\"}", .{});
    defer text_parsed.deinit();
    const text_input = try parseChunkRequestInput(std.testing.allocator, text_parsed.value);
    defer deinitChunkRequestInput(std.testing.allocator, text_input);
    try std.testing.expectEqualStrings("hello", text_input.text);

    var media_parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, "{\"type\":\"media\",\"data\":\"aGVsbG8=\",\"mime_type\":\"audio/wav\"}", .{});
    defer media_parsed.deinit();
    const media_input = try parseChunkRequestInput(std.testing.allocator, media_parsed.value);
    defer deinitChunkRequestInput(std.testing.allocator, media_input);
    try std.testing.expectEqualStrings("audio/wav", media_input.binary.mime_type);
    try std.testing.expectEqualStrings("hello", media_input.binary.data);
}

fn dirContainsModel(path: []const u8) bool {
    var buf: [4096]u8 = undefined;
    inline for ([_][]const u8{ "/tokenizer.json", "/config.json", "/genai_config.json", "/model.onnx", "/model_i8.onnx", "/onnx/model.onnx" }) |suffix| {
        if (path.len + suffix.len < buf.len) {
            @memcpy(buf[0..path.len], path);
            @memcpy(buf[path.len .. path.len + suffix.len], suffix);
            buf[path.len + suffix.len] = 0;
            if (c_file.fileExistsZ(buf[0 .. path.len + suffix.len :0])) return true;
        }
    }

    if (!build_options.link_libc) {
        var dir = std.Io.Dir.cwd().openDir(std.Options.debug_io, path, .{ .iterate = true }) catch return false;
        defer dir.close(std.Options.debug_io);
        var iter = dir.iterate();
        while (iter.next(std.Options.debug_io) catch null) |entry| {
            const name = entry.name;
            if (name.len > 5 and std.mem.endsWith(u8, name, ".gguf")) return true;
        }
        return false;
    }

    const path_z = std.heap.page_allocator.dupeZ(u8, path) catch return false;
    defer std.heap.page_allocator.free(path_z);
    const dir = c_file.c.opendir(path_z.ptr);
    if (dir == null) return false;
    defer _ = c_file.c.closedir(dir);

    while (c_file.c.readdir(dir)) |entry| {
        const name_z: [*:0]const u8 = @ptrCast(&entry.*.d_name);
        const name = std.mem.span(name_z);
        if (name.len > 5 and std.mem.endsWith(u8, name, ".gguf")) return true;
    }

    return false;
}

fn dirExists(path: []const u8) bool {
    var buf: [4096]u8 = undefined;
    if (path.len >= buf.len) return false;
    @memcpy(buf[0..path.len], path);
    buf[path.len] = 0;
    return c_file.fileExistsZ(buf[0..path.len :0]);
}

fn jsonEncodeString(buf: *std.ArrayListUnmanaged(u8), allocator: std.mem.Allocator, s: []const u8) !void {
    try buf.append(allocator, '"');
    for (s) |ch| {
        switch (ch) {
            '"' => try buf.appendSlice(allocator, "\\\""),
            '\\' => try buf.appendSlice(allocator, "\\\\"),
            '\n' => try buf.appendSlice(allocator, "\\n"),
            '\r' => try buf.appendSlice(allocator, "\\r"),
            '\t' => try buf.appendSlice(allocator, "\\t"),
            else => {
                if (ch < 0x20) {
                    const hex = try std.fmt.allocPrint(allocator, "\\u{x:0>4}", .{ch});
                    defer allocator.free(hex);
                    try buf.appendSlice(allocator, hex);
                } else {
                    try buf.append(allocator, ch);
                }
            },
        }
    }
    try buf.append(allocator, '"');
}

fn jsonBytesResponse(ctx: *httpx.Context, body: []const u8) !httpx.Response {
    try ctx.setHeader("Content-Type", "application/json");
    _ = ctx.response.body(body);
    return ctx.response.build();
}

fn parseChunkRequestInput(allocator: std.mem.Allocator, input: std.json.Value) !lib_chunker.Input {
    return switch (input) {
        .string => |s| blk: {
            if (s.len == 0) return error.ChunkInputRequired;
            break :blk .{ .text = s };
        },
        .object => |obj| blk: {
            const type_val = obj.get("type") orelse return error.UnsupportedChunkInputContentPartType;
            if (type_val != .string) return error.ChunkContentPartTypeMustBeString;
            if (std.mem.eql(u8, type_val.string, "text")) {
                const text_val = obj.get("text") orelse return error.ChunkTextContentPartMissingText;
                if (text_val != .string or text_val.string.len == 0) return error.ChunkTextContentPartMissingText;
                break :blk .{ .text = text_val.string };
            }
            if (!std.mem.eql(u8, type_val.string, "media")) return error.UnsupportedChunkInputContentPartType;

            const data_val = obj.get("data") orelse return error.ChunkMediaContentPartMissingData;
            if (data_val != .string) return error.ChunkMediaDataMustBeBase64String;
            if (data_val.string.len == 0) return error.ChunkMediaContentPartMissingData;
            const mime_val = obj.get("mime_type") orelse return error.ChunkMediaContentPartMissingMimeType;
            if (mime_val != .string) return error.ChunkMediaMimeTypeMustBeString;
            if (std.mem.trim(u8, mime_val.string, &std.ascii.whitespace).len == 0) return error.ChunkMediaContentPartMissingMimeType;

            const decoded_payload = decodeMediaData(allocator, data_val.string) catch return error.ChunkInvalidBase64Data;
            const decoded = decoded_payload.data;
            errdefer allocator.free(decoded);
            if (decoded.len == 0) return error.ChunkMediaContentPartMissingData;
            if (!mediaMimeMatches(mime_val.string, decoded_payload.mime_type)) return error.ChunkMediaDataMimeTypeMismatch;
            break :blk .{ .binary = .{
                .mime_type = mime_val.string,
                .data = decoded,
            } };
        },
        else => error.ChunkInputMustBeStringOrContentPartObject,
    };
}

fn deinitChunkRequestInput(allocator: std.mem.Allocator, input: lib_chunker.Input) void {
    switch (input) {
        .binary => |binary| allocator.free(binary.data),
        .text => {},
    }
}

fn chunkInputParseErrorMessage(err: anyerror) []const u8 {
    return switch (err) {
        error.ChunkInputRequired => "missing 'input' field",
        error.UnsupportedChunkInputContentPartType => "input content part type must be 'text' or 'media'",
        error.ChunkContentPartTypeMustBeString => "content part 'type' must be a string",
        error.ChunkTextContentPartMissingText => "text content part missing 'text' field",
        error.ChunkMediaContentPartMissingData => "media content part missing 'data' field",
        error.ChunkMediaDataMustBeBase64String => "media 'data' must be a base64 string",
        error.ChunkMediaContentPartMissingMimeType => "media content part missing 'mime_type' field",
        error.ChunkMediaMimeTypeMustBeString => "media 'mime_type' must be a string",
        error.ChunkInvalidBase64Data => "invalid base64 data",
        error.ChunkMediaDataMimeTypeMismatch => "media data URI mime_type does not match content part mime_type",
        error.ChunkInputMustBeStringOrContentPartObject => "'input' must be a string or content part object",
        else => "invalid chunk input",
    };
}

fn chunkRequestParseErrorMessage(err: anyerror) []const u8 {
    return switch (err) {
        error.MissingField => "missing required 'input' field",
        error.SyntaxError => "request body must be valid JSON",
        error.UnexpectedToken => "request body does not match chunk request schema",
        else => "invalid chunk request",
    };
}

fn validateEmbeddingEncodingFormat(encoding_format: ?[]const u8) !void {
    const value = encoding_format orelse "float";
    if (!std.mem.eql(u8, value, "float")) return error.UnsupportedEncodingFormat;
}

fn parseRequestedEmbeddingDimensions(dimensions: ?i64) !?usize {
    const value = dimensions orelse return null;
    if (value <= 0) return error.InvalidEmbeddingDimensions;
    return @intCast(value);
}

const ParsedEmbedRequest = struct {
    model: []const u8,
    input: std.json.Value,
    encoding_format: ?[]const u8,
    dimensions: ?i64,
    task_type: ?EmbeddingTaskType,
    /// Optional task description overriding the model's default query
    /// instruction. Instruction-aware embedders (Qwen3-Embedding) wrap
    /// query-side inputs as "Instruct: {instruction}\nQuery:{text}".
    instruction: ?[]const u8 = null,
    error_policy: EmbedErrorPolicy = .fail_fast,
};

const EmbedErrorPolicy = enum {
    fail_fast,
    per_item,
};

fn parseEmbedErrorPolicy(value: []const u8) ?EmbedErrorPolicy {
    if (std.mem.eql(u8, value, "fail_fast")) return .fail_fast;
    if (std.mem.eql(u8, value, "per_item")) return .per_item;
    return null;
}

const EmbeddingTaskType = enum {
    RETRIEVAL_QUERY,
    RETRIEVAL_DOCUMENT,
    QUESTION_ANSWERING,
    FACT_VERIFICATION,
    CODE_RETRIEVAL_QUERY,
    CLASSIFICATION,
    CLUSTERING,
    SEMANTIC_SIMILARITY,

    fn usesQueryPrefix(self: EmbeddingTaskType) bool {
        return switch (self) {
            .RETRIEVAL_QUERY,
            .QUESTION_ANSWERING,
            .FACT_VERIFICATION,
            .CODE_RETRIEVAL_QUERY,
            => true,
            else => false,
        };
    }

    fn usesDocumentPrefix(self: EmbeddingTaskType) bool {
        return switch (self) {
            .RETRIEVAL_DOCUMENT => true,
            else => false,
        };
    }
};

fn parseEmbeddingTaskType(value: []const u8) ?EmbeddingTaskType {
    if (std.mem.eql(u8, value, "RETRIEVAL_QUERY")) return .RETRIEVAL_QUERY;
    if (std.mem.eql(u8, value, "RETRIEVAL_DOCUMENT")) return .RETRIEVAL_DOCUMENT;
    if (std.mem.eql(u8, value, "QUESTION_ANSWERING")) return .QUESTION_ANSWERING;
    if (std.mem.eql(u8, value, "FACT_VERIFICATION")) return .FACT_VERIFICATION;
    if (std.mem.eql(u8, value, "CODE_RETRIEVAL_QUERY")) return .CODE_RETRIEVAL_QUERY;
    if (std.mem.eql(u8, value, "CLASSIFICATION")) return .CLASSIFICATION;
    if (std.mem.eql(u8, value, "CLUSTERING")) return .CLUSTERING;
    if (std.mem.eql(u8, value, "SEMANTIC_SIMILARITY")) return .SEMANTIC_SIMILARITY;
    return null;
}

fn parseLegacyEmbeddingInputType(value: []const u8) ?EmbeddingTaskType {
    if (std.mem.eql(u8, value, "search_query") or std.mem.eql(u8, value, "query")) return .RETRIEVAL_QUERY;
    if (std.mem.eql(u8, value, "search_document") or std.mem.eql(u8, value, "document")) return .RETRIEVAL_DOCUMENT;
    if (std.mem.eql(u8, value, "classification")) return .CLASSIFICATION;
    if (std.mem.eql(u8, value, "clustering")) return .CLUSTERING;
    return null;
}

const ParsedTextEmbedInput = struct {
    index: usize,
    text: []const u8,
};

const ParsedBinaryEmbedInput = struct {
    index: usize,
    bytes: []const u8,
    mime_type: ?[]const u8 = null,
    owned: bool = true,
};

const ParsedDenseEmbedInputs = struct {
    texts: std.ArrayListUnmanaged(ParsedTextEmbedInput) = .empty,
    images: std.ArrayListUnmanaged(ParsedBinaryEmbedInput) = .empty,
    audio: std.ArrayListUnmanaged(ParsedBinaryEmbedInput) = .empty,
    parse_errors: std.ArrayListUnmanaged(EmbedItemError) = .empty,
    total_count: usize = 0,

    fn deinit(self: *ParsedDenseEmbedInputs, allocator: std.mem.Allocator) void {
        self.texts.deinit(allocator);
        for (self.images.items) |item| if (item.owned) allocator.free(@constCast(item.bytes));
        self.images.deinit(allocator);
        for (self.audio.items) |item| if (item.owned) allocator.free(@constCast(item.bytes));
        self.audio.deinit(allocator);
        self.parse_errors.deinit(allocator);
    }
};

fn parseEmbedRequest(body: std.json.Value) !ParsedEmbedRequest {
    if (body != .object) return error.RequestBodyMustBeObject;
    const obj = body.object;

    const model_value = obj.get("model") orelse return error.ModelRequired;
    if (model_value != .string or model_value.string.len == 0) return error.ModelRequired;

    const input_value = obj.get("input") orelse return error.InputRequired;

    const encoding_format: ?[]const u8 = if (obj.get("encoding_format")) |value| blk: {
        if (value != .string) return error.EncodingFormatMustBeString;
        break :blk value.string;
    } else null;

    const dimensions: ?i64 = if (obj.get("dimensions")) |value| blk: {
        if (value != .integer) return error.DimensionsMustBeInteger;
        break :blk value.integer;
    } else null;

    const task_type: ?EmbeddingTaskType = if (obj.get("task_type")) |value| blk: {
        if (value != .string) return error.TaskTypeMustBeString;
        break :blk parseEmbeddingTaskType(value.string) orelse return error.UnsupportedEmbeddingTaskType;
    } else null;

    const legacy_task_type: ?EmbeddingTaskType = if (obj.get("input_type")) |value| blk: {
        if (value != .string) return error.InputTypeMustBeString;
        break :blk parseLegacyEmbeddingInputType(value.string) orelse return error.UnsupportedEmbeddingInputType;
    } else null;

    if (task_type != null and legacy_task_type != null and task_type.? != legacy_task_type.?) {
        return error.ConflictingEmbeddingTaskTypes;
    }

    const error_policy: EmbedErrorPolicy = if (obj.get("error_policy")) |value| blk: {
        if (value != .string) return error.ErrorPolicyMustBeString;
        break :blk parseEmbedErrorPolicy(value.string) orelse return error.UnsupportedEmbeddingErrorPolicy;
    } else .fail_fast;

    const instruction: ?[]const u8 = if (obj.get("instruction")) |value| blk: {
        if (value != .string) return error.InstructionMustBeString;
        break :blk if (value.string.len > 0) value.string else null;
    } else null;

    return .{
        .model = model_value.string,
        .input = input_value,
        .encoding_format = encoding_format,
        .dimensions = dimensions,
        .task_type = task_type orelse legacy_task_type,
        .instruction = instruction,
        .error_policy = error_policy,
    };
}

fn embedRequestParseErrorMessage(err: anyerror) []const u8 {
    return switch (err) {
        error.RequestBodyMustBeObject => "request body must be a JSON object",
        error.ModelRequired => "model is required",
        error.InputRequired => "input is required",
        error.EncodingFormatMustBeString => "encoding_format must be a string",
        error.DimensionsMustBeInteger => "dimensions must be an integer",
        error.TaskTypeMustBeString => "task_type must be a string",
        error.UnsupportedEmbeddingTaskType => "task_type must be one of RETRIEVAL_QUERY, RETRIEVAL_DOCUMENT, QUESTION_ANSWERING, FACT_VERIFICATION, CODE_RETRIEVAL_QUERY, CLASSIFICATION, CLUSTERING, or SEMANTIC_SIMILARITY",
        error.InputTypeMustBeString => "input_type must be a string",
        error.UnsupportedEmbeddingInputType => "input_type must be a legacy alias for a supported embedding task type",
        error.ConflictingEmbeddingTaskTypes => "task_type and input_type specify different embedding task types",
        error.ErrorPolicyMustBeString => "error_policy must be a string",
        error.UnsupportedEmbeddingErrorPolicy => "error_policy must be one of fail_fast or per_item",
        error.InstructionMustBeString => "instruction must be a string",
        else => "invalid embedding request",
    };
}

/// Configure query/document prefixes for last-token decoder embedders
/// (Jina v5, Qwen3-Embedding) from the manifest and request. Returns an
/// owned prefix buffer when a per-request instruction was rendered; the
/// caller must keep it alive for the pipeline run and free it afterwards.
fn applyDenseEmbeddingRequestOptions(
    allocator: std.mem.Allocator,
    pipeline: *embedding_mod.EmbeddingPipeline,
    manifest: *const manifest_mod.ModelManifest,
    request: ParsedEmbedRequest,
) !?[]u8 {
    if (!manifest.isLastTokenDecoderEmbedder()) {
        if (request.instruction != null) return error.InstructionNotSupportedForModel;
        return null;
    }

    // Manifests written before embedding_style existed match the legacy
    // Jina heuristic; treat them as Jina v5.
    const style: manifest_mod.EmbeddingStyle = if (manifest.embedding_style == .none)
        .jina_v5
    else
        manifest.embedding_style;

    const task_type = request.task_type orelse EmbeddingTaskType.RETRIEVAL_DOCUMENT;
    const query_side = switch (style) {
        // Qwen3-Embedding instructs every non-document task (matches the
        // model card's MTEB usage: classification/clustering/STS inputs all
        // carry a task instruction).
        .qwen3_embedding => !task_type.usesDocumentPrefix(),
        else => task_type.usesQueryPrefix(),
    };

    if (query_side) {
        if (request.instruction) |instr| {
            if (style != .qwen3_embedding) return error.InstructionNotSupportedForModel;
            const owned = try std.fmt.allocPrint(allocator, "Instruct: {s}\nQuery:", .{instr});
            pipeline.config.text_prefix = owned;
            return owned;
        }
        pipeline.config.text_prefix = switch (style) {
            .qwen3_embedding => manifest.queryPrefix(),
            else => if (manifest.embedding_query_prefix.len > 0)
                manifest.embedding_query_prefix
            else
                "Query: ",
        };
        return null;
    }
    if (task_type.usesDocumentPrefix()) {
        if (request.instruction != null) return error.InstructionRequiresQueryTask;
        pipeline.config.text_prefix = if (manifest.embedding_text_prefix.len > 0 or style == .qwen3_embedding)
            manifest.embedding_text_prefix
        else
            "Document: ";
        return null;
    }
    return error.UnsupportedEmbeddingTaskType;
}

fn isEmbedRequestOptionError(err: anyerror) bool {
    return switch (err) {
        error.UnsupportedEmbeddingTaskType,
        error.InstructionNotSupportedForModel,
        error.InstructionRequiresQueryTask,
        => true,
        else => false,
    };
}

fn embedRequestOptionErrorMessage(err: anyerror) []const u8 {
    return switch (err) {
        error.UnsupportedEmbeddingTaskType => "task_type must be a query/document retrieval task for this embedding model",
        error.InstructionNotSupportedForModel => "instruction is only supported for instruction-aware embedding models",
        error.InstructionRequiresQueryTask => "instruction requires a query-side task_type (documents are embedded without instructions)",
        else => "invalid embedding options",
    };
}

fn parseSparseEmbedInputs(
    allocator: std.mem.Allocator,
    input: std.json.Value,
) ![]const []const u8 {
    var texts = std.ArrayListUnmanaged([]const u8).empty;
    errdefer texts.deinit(allocator);

    switch (input) {
        .string => |value| try texts.append(allocator, value),
        .array => |arr| {
            for (arr.items) |item| {
                if (item != .string) return error.SparseModelsRequireTextInput;
                try texts.append(allocator, item.string);
            }
        },
        else => return error.SparseModelsRequireTextInput,
    }

    return try texts.toOwnedSlice(allocator);
}

fn parseDenseEmbedInputs(
    self: *Node,
    allocator: std.mem.Allocator,
    manifest: *const manifest_mod.ModelManifest,
    input: std.json.Value,
) !ParsedDenseEmbedInputs {
    var media_budget = RequestMediaBudget.init(requestMediaMaxBytes(self));
    return parseDenseEmbedInputsWithBudget(self, allocator, manifest, input, &media_budget);
}

fn parseDenseEmbedInputsWithBudget(
    self: *Node,
    allocator: std.mem.Allocator,
    manifest: *const manifest_mod.ModelManifest,
    input: std.json.Value,
    media_budget: *RequestMediaBudget,
) !ParsedDenseEmbedInputs {
    return parseDenseEmbedInputsWithBudgetOptionalContext(self, allocator, manifest, input, media_budget, null);
}

fn parseDenseEmbedInputsWithBudgetAndContext(
    self: *Node,
    allocator: std.mem.Allocator,
    manifest: *const manifest_mod.ModelManifest,
    input: std.json.Value,
    media_budget: *RequestMediaBudget,
    request_context: DenseEmbedRequestContext,
) !ParsedDenseEmbedInputs {
    return parseDenseEmbedInputsWithBudgetOptionalContext(self, allocator, manifest, input, media_budget, request_context);
}

fn parseDenseEmbedInputsWithBudgetOptionalContext(
    self: *Node,
    allocator: std.mem.Allocator,
    manifest: *const manifest_mod.ModelManifest,
    input: std.json.Value,
    media_budget: *RequestMediaBudget,
    request_context: ?DenseEmbedRequestContext,
) !ParsedDenseEmbedInputs {
    var parsed: ParsedDenseEmbedInputs = .{};
    errdefer parsed.deinit(allocator);

    switch (input) {
        .string => |value| {
            if (!model_caps.modelAcceptsInput(manifest, "text")) return error.ModelDoesNotSupportTextInput;
            try parsed.texts.append(allocator, .{ .index = 0, .text = value });
            parsed.total_count = 1;
        },
        .array => |arr| {
            if (arr.items.len == 0) return parsed;

            for (arr.items, 0..) |item, index| {
                try appendDenseEmbedInput(self, allocator, manifest, &parsed, item, index, media_budget, request_context);
            }

            parsed.total_count = arr.items.len;
        },
        else => return error.InputMustBeStringOrArrayOfStringsOrContentParts,
    }

    return parsed;
}

fn parseDirectDenseEmbedInputs(
    self: *Node,
    allocator: std.mem.Allocator,
    manifest: *const manifest_mod.ModelManifest,
    parts: []const Node.DirectDenseEmbedPart,
    media_budget: *RequestMediaBudget,
) !ParsedDenseEmbedInputs {
    return parseDirectDenseEmbedInputsOptionalContext(self, allocator, manifest, parts, media_budget, null);
}

fn parseDirectDenseEmbedInputsWithContext(
    self: *Node,
    allocator: std.mem.Allocator,
    manifest: *const manifest_mod.ModelManifest,
    parts: []const Node.DirectDenseEmbedPart,
    media_budget: *RequestMediaBudget,
    request_context: DenseEmbedRequestContext,
) !ParsedDenseEmbedInputs {
    return parseDirectDenseEmbedInputsOptionalContext(self, allocator, manifest, parts, media_budget, request_context);
}

fn parseDirectDenseEmbedInputsOptionalContext(
    self: *Node,
    allocator: std.mem.Allocator,
    manifest: *const manifest_mod.ModelManifest,
    parts: []const Node.DirectDenseEmbedPart,
    media_budget: *RequestMediaBudget,
    request_context: ?DenseEmbedRequestContext,
) !ParsedDenseEmbedInputs {
    var parsed: ParsedDenseEmbedInputs = .{};
    errdefer parsed.deinit(allocator);

    // Borrowed buffers are already resident before parsing begins. Charge all
    // of them up front so a leading remote/image URL can only consume the
    // remaining request budget, independent of content-part order.
    const preflight = try directDenseEmbedPreflight(parts);
    try media_budget.add(preflight.shape.borrowed_bytes);

    for (parts, 0..) |part, index| switch (part) {
        .text => |text| {
            if (!model_caps.modelAcceptsInput(manifest, "text")) return error.ModelDoesNotSupportTextInput;
            try parsed.texts.append(allocator, .{ .index = index, .text = text });
        },
        .image_url => |url| try appendDenseEmbedImageUrl(
            self,
            allocator,
            manifest,
            &parsed,
            url,
            index,
            media_budget,
            request_context,
        ),
        .media => |media| {
            try appendDenseEmbedBinary(
                allocator,
                manifest,
                &parsed,
                media.data,
                media.mime_type,
                index,
                false,
            );
        },
    };
    parsed.total_count = parts.len;
    return parsed;
}

fn parseDenseEmbedInputsPerItem(
    self: *Node,
    allocator: std.mem.Allocator,
    manifest: *const manifest_mod.ModelManifest,
    input: std.json.Value,
) !ParsedDenseEmbedInputs {
    var media_budget = RequestMediaBudget.init(requestMediaMaxBytes(self));
    return parseDenseEmbedInputsPerItemWithBudget(self, allocator, manifest, input, &media_budget);
}

fn parseDenseEmbedInputsPerItemWithBudget(
    self: *Node,
    allocator: std.mem.Allocator,
    manifest: *const manifest_mod.ModelManifest,
    input: std.json.Value,
    media_budget: *RequestMediaBudget,
) !ParsedDenseEmbedInputs {
    return parseDenseEmbedInputsPerItemWithBudgetOptionalContext(self, allocator, manifest, input, media_budget, null);
}

fn parseDenseEmbedInputsPerItemWithBudgetAndContext(
    self: *Node,
    allocator: std.mem.Allocator,
    manifest: *const manifest_mod.ModelManifest,
    input: std.json.Value,
    media_budget: *RequestMediaBudget,
    request_context: DenseEmbedRequestContext,
) !ParsedDenseEmbedInputs {
    return parseDenseEmbedInputsPerItemWithBudgetOptionalContext(self, allocator, manifest, input, media_budget, request_context);
}

fn parseDenseEmbedInputsPerItemWithBudgetOptionalContext(
    self: *Node,
    allocator: std.mem.Allocator,
    manifest: *const manifest_mod.ModelManifest,
    input: std.json.Value,
    media_budget: *RequestMediaBudget,
    request_context: ?DenseEmbedRequestContext,
) !ParsedDenseEmbedInputs {
    var parsed: ParsedDenseEmbedInputs = .{};
    errdefer parsed.deinit(allocator);

    switch (input) {
        .string => |value| {
            parsed.total_count = 1;
            appendDenseEmbedInput(self, allocator, manifest, &parsed, .{ .string = value }, 0, media_budget, request_context) catch |err| {
                if (isDenseEmbedRequestAbort(err)) return err;
                try parsed.parse_errors.append(allocator, embedInputItemFailure(0, err));
            };
        },
        .array => |arr| {
            parsed.total_count = arr.items.len;
            for (arr.items, 0..) |item, index| {
                appendDenseEmbedInput(self, allocator, manifest, &parsed, item, index, media_budget, request_context) catch |err| {
                    if (isDenseEmbedRequestAbort(err)) return err;
                    try parsed.parse_errors.append(allocator, embedInputItemFailure(index, err));
                };
            }
        },
        else => return error.InputMustBeStringOrArrayOfStringsOrContentParts,
    }

    return parsed;
}

fn appendDenseEmbedImageUrl(
    self: *Node,
    allocator: std.mem.Allocator,
    manifest: *const manifest_mod.ModelManifest,
    parsed: *ParsedDenseEmbedInputs,
    url: []const u8,
    index: usize,
    media_budget: *RequestMediaBudget,
    request_context: ?DenseEmbedRequestContext,
) !void {
    if (!model_caps.modelAcceptsInput(manifest, "image")) return error.ModelDoesNotSupportImageInput;
    const downloaded = if (request_context) |context|
        try downloadRemoteContentWithBudgetForRequestWithContext(self, allocator, context, url, media_budget)
    else
        try downloadRemoteContentWithBudgetForRequest(self, allocator, url, media_budget);
    errdefer allocator.free(downloaded.data);
    defer allocator.free(downloaded.content_type);

    if (!std.mem.startsWith(u8, downloaded.content_type, "image/")) return error.ImageUrlMustResolveToImage;
    try parsed.images.append(allocator, .{
        .index = index,
        .bytes = downloaded.data,
        .mime_type = null,
    });
}

fn appendDenseEmbedBinary(
    allocator: std.mem.Allocator,
    manifest: *const manifest_mod.ModelManifest,
    parsed: *ParsedDenseEmbedInputs,
    bytes: []const u8,
    mime_type: []const u8,
    index: usize,
    owned: bool,
) !void {
    if (std.mem.startsWith(u8, mime_type, "image/")) {
        if (!model_caps.modelAcceptsInput(manifest, "image")) return error.ModelDoesNotSupportImageInput;
        try parsed.images.append(allocator, .{
            .index = index,
            .bytes = bytes,
            .mime_type = mime_type,
            .owned = owned,
        });
        return;
    }

    if (std.mem.startsWith(u8, mime_type, "audio/")) {
        if (!model_caps.modelAcceptsInput(manifest, "audio")) return error.ModelDoesNotSupportAudioInput;
        try parsed.audio.append(allocator, .{
            .index = index,
            .bytes = bytes,
            .mime_type = mime_type,
            .owned = owned,
        });
        return;
    }

    return error.UnsupportedMediaMimeType;
}

fn appendDenseEmbedInput(
    self: *Node,
    allocator: std.mem.Allocator,
    manifest: *const manifest_mod.ModelManifest,
    parsed: *ParsedDenseEmbedInputs,
    item: std.json.Value,
    index: usize,
    media_budget: *RequestMediaBudget,
    request_context: ?DenseEmbedRequestContext,
) !void {
    if (item == .string) {
        if (!model_caps.modelAcceptsInput(manifest, "text")) return error.ModelDoesNotSupportTextInput;
        try parsed.texts.append(allocator, .{ .index = index, .text = item.string });
        return;
    }

    if (item != .object) return error.InputMustBeStringOrArrayOfStringsOrContentParts;

    const obj = item.object;
    const type_value = obj.get("type") orelse return error.ContentPartTypeRequired;
    if (type_value != .string) return error.ContentPartTypeMustBeString;
    const part_type = type_value.string;

    if (std.mem.eql(u8, part_type, "text")) {
        if (!model_caps.modelAcceptsInput(manifest, "text")) return error.ModelDoesNotSupportTextInput;
        const text_value = obj.get("text") orelse return error.TextContentPartMissingText;
        if (text_value != .string) return error.TextContentPartMissingText;
        try parsed.texts.append(allocator, .{ .index = index, .text = text_value.string });
        return;
    }

    if (std.mem.eql(u8, part_type, "image_url")) {
        const image_url = obj.get("image_url") orelse return error.ImageUrlContentPartMissingImageUrl;
        const url = switch (image_url) {
            .string => image_url.string,
            .object => blk: {
                const url_value = image_url.object.get("url") orelse return error.ImageUrlContentPartMissingUrl;
                if (url_value != .string) return error.ImageUrlContentPartMissingUrl;
                break :blk url_value.string;
            },
            else => return error.ImageUrlContentPartMissingUrl,
        };
        return appendDenseEmbedImageUrl(self, allocator, manifest, parsed, url, index, media_budget, request_context);
    }

    if (std.mem.eql(u8, part_type, "media")) {
        const data_value = obj.get("data") orelse return error.MediaContentPartMissingData;
        if (data_value != .string) return error.MediaContentPartMissingData;
        const mime_value = obj.get("mime_type") orelse return error.MediaContentPartMissingMimeType;
        if (mime_value != .string) return error.MediaContentPartMissingMimeType;

        const decoded_payload = decodeMediaDataWithBudget(allocator, data_value.string, media_budget) catch |err| switch (err) {
            error.OutOfMemory, error.RemoteContentTooLarge => return err,
            else => return error.InvalidMediaBase64,
        };
        const decoded = decoded_payload.data;
        errdefer allocator.free(decoded);
        if (!mediaMimeMatches(mime_value.string, decoded_payload.mime_type)) return error.MediaDataMimeTypeMismatch;
        return appendDenseEmbedBinary(
            allocator,
            manifest,
            parsed,
            decoded,
            mime_value.string,
            index,
            true,
        );
    }

    return error.UnknownContentPartType;
}

fn embedInputParseErrorMessage(err: anyerror) []const u8 {
    return switch (err) {
        error.InputMustBeStringOrArrayOfStringsOrContentParts => "input must be a string, array of strings, or array of content parts",
        error.ContentPartTypeRequired => "content part missing 'type' field",
        error.ContentPartTypeMustBeString => "content part 'type' must be a string",
        error.TextContentPartMissingText => "text content part missing 'text' field",
        error.ImageUrlContentPartMissingImageUrl => "image_url content part missing 'image_url' field",
        error.ImageUrlContentPartMissingUrl => "image_url must contain a 'url' string",
        error.ImageUrlDownloadFailed => "failed to download image_url content",
        error.ImageUrlMustResolveToImage => "image_url content must resolve to an image",
        error.MediaContentPartMissingData => "media content part missing 'data' field",
        error.MediaContentPartMissingMimeType => "media content part missing 'mime_type' field",
        error.InvalidMediaBase64 => "invalid base64 media data",
        error.MediaDataMimeTypeMismatch => "media data URI mime_type does not match content part mime_type",
        error.UnsupportedMediaMimeType => "media content part must have an image/* or audio/* mime_type",
        error.ModelDoesNotSupportTextInput => "model does not support text input",
        error.ModelDoesNotSupportImageInput => "model does not support image input",
        error.ModelDoesNotSupportAudioInput => "model does not support audio input",
        error.UnknownContentPartType => "unsupported content part type",
        else => "invalid embedding input",
    };
}

const EmbedDenseInputFailure = struct {
    status: u16,
    code: []const u8,
    message: []const u8,
};

fn embedDenseInputFailure(err: anyerror) EmbedDenseInputFailure {
    return switch (err) {
        error.ImageDecodeFailed => .{
            .status = 400,
            .code = "INVALID_IMAGE",
            .message = "unsupported or corrupt image input",
        },
        error.ResourceLimitExceeded => .{
            .status = 400,
            .code = "MODEL_RESOURCE_LIMIT",
            .message = "request resource plan exceeds the configured inference budget",
        },
        error.ResourceTemporarilyUnavailable => .{
            .status = 503,
            .code = "MODEL_RESOURCE_BUSY",
            .message = "insufficient inference capacity is currently available",
        },
        else => .{
            .status = 500,
            .code = "INFERENCE_FAILED",
            .message = internalErrorMessage("INFERENCE_FAILED", err),
        },
    };
}

fn embedDenseInputFailureResponse(ctx: *httpx.Context, err: anyerror) !httpx.Response {
    if (err == error.ResourceTemporarilyUnavailable) {
        return modelResourceBusyResponse(ctx);
    }
    const failure = embedDenseInputFailure(err);
    return ctx.status(failure.status).json(.{
        .@"error" = failure.code,
        .message = failure.message,
    });
}

const EmbedItemError = struct {
    index: i64,
    code: []const u8,
    message: []const u8,
    stage: []const u8,
    retryable: bool,
    status: u16,
    retry_after_ms: ?i64 = null,
};

const EmbedPartialSummary = struct {
    total: i64,
    succeeded: i64,
    failed: i64,
};

const EmbedResponseStrict = struct {
    object: []const u8,
    data: []const api.EmbeddingObject,
    model: []const u8,
    usage: api.EmbeddingUsage,
};

const EmbedDensePartialResponse = struct {
    object: []const u8,
    data: []const api.EmbeddingObject,
    model: []const u8,
    errors: []const EmbedItemError,
    summary: EmbedPartialSummary,
    usage: api.EmbeddingUsage,
};

const DenseEmbedPartialResult = struct {
    embeddings: []?[]f32,
    errors: []EmbedItemError,

    fn deinit(self: *DenseEmbedPartialResult, allocator: std.mem.Allocator) void {
        for (self.embeddings) |maybe_embedding| {
            if (maybe_embedding) |embedding| allocator.free(embedding);
        }
        allocator.free(self.embeddings);
        allocator.free(self.errors);
        self.* = undefined;
    }

    fn successCount(self: *const DenseEmbedPartialResult) usize {
        var count: usize = 0;
        for (self.embeddings) |maybe_embedding| {
            if (maybe_embedding != null) count += 1;
        }
        return count;
    }
};

fn embedItemFailure(index: usize, err: anyerror, stage: []const u8) EmbedItemError {
    return switch (err) {
        error.ImageDecodeFailed => .{
            .index = @intCast(index),
            .code = "INVALID_IMAGE",
            .message = "unsupported or corrupt image input",
            .stage = "image_decode",
            .retryable = false,
            .status = 400,
        },
        error.ResourceLimitExceeded => .{
            .index = @intCast(index),
            .code = "MODEL_RESOURCE_LIMIT",
            .message = "request resource plan exceeds the configured inference budget",
            .stage = stage,
            .retryable = false,
            .status = 400,
        },
        error.ResourceTemporarilyUnavailable => .{
            .index = @intCast(index),
            .code = "MODEL_RESOURCE_BUSY",
            .message = "insufficient inference capacity is currently available",
            .stage = stage,
            .retryable = true,
            .status = 503,
            .retry_after_ms = transient_capacity_retry_after_ms,
        },
        else => .{
            .index = @intCast(index),
            .code = "INFERENCE_FAILED",
            .message = internalErrorMessage("INFERENCE_FAILED", err),
            .stage = stage,
            .retryable = true,
            .status = 500,
        },
    };
}

fn embedInputItemFailure(index: usize, err: anyerror) EmbedItemError {
    if (remoteContentRequestFailure(err)) |failure| {
        return .{
            .index = @intCast(index),
            .code = failure.code,
            .message = failure.message,
            .stage = "fetch",
            .retryable = failure.retryable,
            .status = failure.status,
        };
    }
    return switch (err) {
        error.ImageUrlMustResolveToImage => .{
            .index = @intCast(index),
            .code = "INVALID_IMAGE_URL",
            .message = "image_url content must resolve to an image",
            .stage = "fetch",
            .retryable = false,
            .status = 400,
        },
        error.InvalidMediaBase64,
        error.MediaDataMimeTypeMismatch,
        => .{
            .index = @intCast(index),
            .code = "INVALID_MEDIA",
            .message = embedInputParseErrorMessage(err),
            .stage = "parse",
            .retryable = false,
            .status = 400,
        },
        error.UnsupportedMediaMimeType => .{
            .index = @intCast(index),
            .code = "UNSUPPORTED_MEDIA",
            .message = embedInputParseErrorMessage(err),
            .stage = "parse",
            .retryable = false,
            .status = 400,
        },
        error.ModelDoesNotSupportTextInput,
        error.ModelDoesNotSupportImageInput,
        error.ModelDoesNotSupportAudioInput,
        => .{
            .index = @intCast(index),
            .code = "UNSUPPORTED_INPUT_MODALITY",
            .message = embedInputParseErrorMessage(err),
            .stage = "parse",
            .retryable = false,
            .status = 400,
        },
        error.InputMustBeStringOrArrayOfStringsOrContentParts,
        error.ContentPartTypeRequired,
        error.ContentPartTypeMustBeString,
        error.TextContentPartMissingText,
        error.ImageUrlContentPartMissingImageUrl,
        error.ImageUrlContentPartMissingUrl,
        error.MediaContentPartMissingData,
        error.MediaContentPartMissingMimeType,
        error.UnknownContentPartType,
        => .{
            .index = @intCast(index),
            .code = "INVALID_INPUT",
            .message = embedInputParseErrorMessage(err),
            .stage = "parse",
            .retryable = false,
            .status = 400,
        },
        else => .{
            .index = @intCast(index),
            .code = "INPUT_PREP_FAILED",
            .message = internalErrorMessage("INPUT_PREP_FAILED", err),
            .stage = "parse",
            .retryable = true,
            .status = 500,
        },
    };
}

fn prepareInitialDenseEmbeddingPipeline(
    model: *model_manager_mod.LoadedModel,
    allocator: std.mem.Allocator,
    inputs: *const ParsedDenseEmbedInputs,
) !embedding_mod.EmbeddingPipeline {
    model.lockEmbeddingAssets();
    defer model.unlockEmbeddingAssets();
    if (inputs.audio.items.len > 0) {
        // Audio is deliberately the only optional phase admitted up front.
        // Text/image assets are admitted after the audio outputs are copied.
        try model.ensureAudioEmbeddingAssetsLocked();
    } else {
        try model.ensurePrimaryEmbeddingAssetsLocked(
            inputs.texts.items.len > 0,
            inputs.images.items.len > 0,
        );
    }
    return model.embeddingPipelineLocked(allocator);
}

fn admitPrimaryDenseEmbeddingAssetsAfterAudio(
    model: *model_manager_mod.LoadedModel,
    pipeline: *embedding_mod.EmbeddingPipeline,
    inputs: *const ParsedDenseEmbedInputs,
) !void {
    if (inputs.texts.items.len == 0 and inputs.images.items.len == 0) return;
    model.lockEmbeddingAssets();
    defer model.unlockEmbeddingAssets();
    try model.ensurePrimaryEmbeddingAssetsLocked(
        inputs.texts.items.len > 0,
        inputs.images.items.len > 0,
    );
    model.bindEmbeddingPipelineAssetsLocked(pipeline);
}

const PartialPrimaryDenseEmbeddingAdmission = struct {
    text_error: ?anyerror = null,
    image_error: ?anyerror = null,
};

fn admitPrimaryDenseEmbeddingAssetsAfterAudioPartial(
    model: *model_manager_mod.LoadedModel,
    pipeline: *embedding_mod.EmbeddingPipeline,
    inputs: *const ParsedDenseEmbedInputs,
) PartialPrimaryDenseEmbeddingAdmission {
    var admission = PartialPrimaryDenseEmbeddingAdmission{};
    model.lockEmbeddingAssets();
    defer model.unlockEmbeddingAssets();

    if (inputs.texts.items.len > 0) {
        model.ensurePrimaryEmbeddingAssetsLocked(true, false) catch |err| {
            admission.text_error = err;
        };
    }
    if (inputs.images.items.len > 0) {
        model.ensurePrimaryEmbeddingAssetsLocked(false, true) catch |err| {
            admission.image_error = err;
        };
    }
    model.bindEmbeddingPipelineAssetsLocked(pipeline);
    return admission;
}

fn releaseAudioDenseEmbeddingAssets(model: *model_manager_mod.LoadedModel) void {
    model.lockEmbeddingAssets();
    defer model.unlockEmbeddingAssets();
    model.releaseAudioEmbeddingAssetsLocked();
}

/// Owns the ephemeral audio sidecars from successful admission until the
/// audio phase has either materialized its outputs or unwound. Keeping this
/// ownership explicit closes the sessions on every error path before the
/// caller releases its exclusive embedding-asset lease.
const AudioEmbeddingAssetGuard = struct {
    model: *model_manager_mod.LoadedModel,
    held: bool,

    fn init(model: *model_manager_mod.LoadedModel, held: bool) AudioEmbeddingAssetGuard {
        return .{ .model = model, .held = held };
    }

    fn release(self: *AudioEmbeddingAssetGuard) void {
        if (!self.held) return;
        releaseAudioDenseEmbeddingAssets(self.model);
        self.held = false;
    }

    fn deinit(self: *AudioEmbeddingAssetGuard) void {
        self.release();
    }
};

fn embedDenseInputs(
    allocator: std.mem.Allocator,
    pipeline: *embedding_mod.EmbeddingPipeline,
    inputs: *const ParsedDenseEmbedInputs,
    model: *model_manager_mod.LoadedModel,
    audio_asset_guard: *AudioEmbeddingAssetGuard,
    asset_lease: *model_manager_mod.EmbeddingAssetLease,
) ![][]f32 {
    const embeddings = try allocator.alloc([]f32, inputs.total_count);
    errdefer allocator.free(embeddings);

    var filled = try allocator.alloc(bool, inputs.total_count);
    defer allocator.free(filled);
    @memset(filled, false);
    errdefer {
        for (embeddings, 0..) |embedding, i| {
            if (filled[i]) allocator.free(embedding);
        }
    }

    // Materialize the audio outputs while its ephemeral sidecars are resident,
    // release them, and only then admit reusable text/image sidecars.
    if (inputs.audio.items.len > 0) {
        {
            defer audio_asset_guard.release();
            const audio_inputs = try allocator.alloc(embedding_mod.EncodedAudioClip, inputs.audio.items.len);
            defer allocator.free(audio_inputs);
            for (inputs.audio.items, 0..) |item, i| {
                audio_inputs[i] = .{
                    .bytes = item.bytes,
                    .decode_options = .{
                        .mime_hint = item.mime_type,
                    },
                };
            }

            const audio_embeddings = try pipeline.embedEncodedAudio(audio_inputs);
            defer allocator.free(audio_embeddings);
            for (inputs.audio.items, 0..) |item, i| {
                embeddings[item.index] = audio_embeddings[i];
                filled[item.index] = true;
            }
        }
        if (inputs.texts.items.len > 0 or inputs.images.items.len > 0) {
            asset_lease.downgradeExclusiveToShared();
            try admitPrimaryDenseEmbeddingAssetsAfterAudio(model, pipeline, inputs);
        } else {
            asset_lease.release();
        }
    }

    if (inputs.texts.items.len > 0) {
        const texts = try allocator.alloc([]const u8, inputs.texts.items.len);
        defer allocator.free(texts);
        for (inputs.texts.items, 0..) |item, i| texts[i] = item.text;

        const text_embeddings = try pipeline.embed(texts);
        defer allocator.free(text_embeddings);
        for (inputs.texts.items, 0..) |item, i| {
            embeddings[item.index] = text_embeddings[i];
            filled[item.index] = true;
        }
    }

    if (inputs.images.items.len > 0) {
        const images = try allocator.alloc([]const u8, inputs.images.items.len);
        defer allocator.free(images);
        for (inputs.images.items, 0..) |item, i| images[i] = item.bytes;

        const image_embeddings = try pipeline.embedImages(images);
        defer allocator.free(image_embeddings);
        for (inputs.images.items, 0..) |item, i| {
            embeddings[item.index] = image_embeddings[i];
            filled[item.index] = true;
        }
    }

    for (filled) |was_filled| {
        if (!was_filled) return error.MissingEmbeddingResult;
    }

    return embeddings;
}

fn embedDenseInputsPartial(
    allocator: std.mem.Allocator,
    pipeline: *embedding_mod.EmbeddingPipeline,
    inputs: *const ParsedDenseEmbedInputs,
    model: *model_manager_mod.LoadedModel,
    audio_asset_guard: *AudioEmbeddingAssetGuard,
    asset_lease: *model_manager_mod.EmbeddingAssetLease,
) !DenseEmbedPartialResult {
    var result = try initDenseEmbedPartialResult(allocator, inputs.total_count);
    errdefer result.deinit(allocator);

    var errors = std.ArrayListUnmanaged(EmbedItemError).empty;
    errdefer errors.deinit(allocator);
    try errors.appendSlice(allocator, inputs.parse_errors.items);

    var primary_admission = PartialPrimaryDenseEmbeddingAdmission{};
    if (inputs.audio.items.len > 0) {
        {
            defer audio_asset_guard.release();
            const audio_inputs = try allocator.alloc(embedding_mod.EncodedAudioClip, inputs.audio.items.len);
            defer allocator.free(audio_inputs);
            for (inputs.audio.items, 0..) |item, i| {
                audio_inputs[i] = .{
                    .bytes = item.bytes,
                    .decode_options = .{ .mime_hint = item.mime_type },
                };
            }

            if (pipeline.embedEncodedAudio(audio_inputs)) |embeddings| {
                defer allocator.free(embeddings);
                for (inputs.audio.items, 0..) |item, i| {
                    result.embeddings[item.index] = embeddings[i];
                }
            } else |err| {
                if (shouldAbortDensePartialFallback(err)) return err;
                try embedAudioInputsIndividually(allocator, pipeline, inputs.audio.items, audio_inputs, &errors, &result);
            }
        }
        if (inputs.texts.items.len > 0 or inputs.images.items.len > 0) {
            asset_lease.downgradeExclusiveToShared();
            primary_admission = admitPrimaryDenseEmbeddingAssetsAfterAudioPartial(model, pipeline, inputs);
        } else {
            asset_lease.release();
        }
        if (primary_admission.text_error) |err| {
            if (shouldAbortDensePartialFallback(err)) return err;
            try appendDenseEmbeddingItemErrors(allocator, &errors, inputs.texts.items, err, "model_admission");
        }
        if (primary_admission.image_error) |err| {
            if (shouldAbortDensePartialFallback(err)) return err;
            try appendDenseEmbeddingItemErrors(allocator, &errors, inputs.images.items, err, "model_admission");
        }
    }

    if (primary_admission.text_error == null and inputs.texts.items.len > 0) {
        const texts = try allocator.alloc([]const u8, inputs.texts.items.len);
        defer allocator.free(texts);
        for (inputs.texts.items, 0..) |item, i| texts[i] = item.text;

        if (pipeline.embed(texts)) |embeddings| {
            defer allocator.free(embeddings);
            for (inputs.texts.items, 0..) |item, i| {
                result.embeddings[item.index] = embeddings[i];
            }
        } else |err| {
            if (shouldAbortDensePartialFallback(err)) return err;
            try embedTextInputsIndividually(allocator, pipeline, inputs.texts.items, texts, &errors, &result);
        }
    }

    if (primary_admission.image_error == null and inputs.images.items.len > 0) {
        const images = try allocator.alloc([]const u8, inputs.images.items.len);
        defer allocator.free(images);
        for (inputs.images.items, 0..) |item, i| images[i] = item.bytes;

        if (pipeline.embedImages(images)) |embeddings| {
            defer allocator.free(embeddings);
            for (inputs.images.items, 0..) |item, i| {
                result.embeddings[item.index] = embeddings[i];
            }
        } else |err| {
            if (shouldAbortDensePartialFallback(err)) return err;
            try embedImageInputsIndividually(allocator, pipeline, inputs.images.items, images, &errors, &result);
        }
    }

    for (result.embeddings, 0..) |maybe_embedding, index| {
        if (maybe_embedding == null) {
            var already_reported = false;
            for (errors.items) |item_error| {
                if (item_error.index == @as(i64, @intCast(index))) {
                    already_reported = true;
                    break;
                }
            }
            if (!already_reported) {
                try errors.append(allocator, embedItemFailure(index, error.MissingEmbeddingResult, "inference"));
            }
        }
    }

    const owned_errors = try errors.toOwnedSlice(allocator);
    allocator.free(result.errors);
    result.errors = owned_errors;
    return result;
}

fn initDenseEmbedPartialResult(allocator: std.mem.Allocator, total_count: usize) !DenseEmbedPartialResult {
    const embeddings = try allocator.alloc(?[]f32, total_count);
    errdefer allocator.free(embeddings);
    const errors = try allocator.alloc(EmbedItemError, 0);
    @memset(embeddings, null);
    return .{
        .embeddings = embeddings,
        .errors = errors,
    };
}

fn appendDenseEmbeddingItemErrors(
    allocator: std.mem.Allocator,
    errors: *std.ArrayListUnmanaged(EmbedItemError),
    items: anytype,
    err: anyerror,
    stage: []const u8,
) !void {
    for (items) |item| {
        try errors.append(allocator, embedItemFailure(item.index, err, stage));
    }
}

fn embedTextInputsIndividually(
    allocator: std.mem.Allocator,
    pipeline: *embedding_mod.EmbeddingPipeline,
    items: []const ParsedTextEmbedInput,
    texts: []const []const u8,
    errors: *std.ArrayListUnmanaged(EmbedItemError),
    result: *DenseEmbedPartialResult,
) !void {
    for (items, 0..) |item, i| {
        const single = pipeline.embed(texts[i .. i + 1]) catch |single_err| {
            if (shouldAbortDensePartialFallback(single_err)) return single_err;
            try errors.append(allocator, embedItemFailure(item.index, single_err, "text_inference"));
            continue;
        };
        defer allocator.free(single);
        if (single.len != 1) {
            for (single) |embedding| allocator.free(embedding);
            try errors.append(allocator, embedItemFailure(item.index, error.MissingEmbeddingResult, "text_inference"));
            continue;
        }
        result.embeddings[item.index] = single[0];
    }
}

fn embedImageInputsIndividually(
    allocator: std.mem.Allocator,
    pipeline: *embedding_mod.EmbeddingPipeline,
    items: []const ParsedBinaryEmbedInput,
    images: []const []const u8,
    errors: *std.ArrayListUnmanaged(EmbedItemError),
    result: *DenseEmbedPartialResult,
) !void {
    for (items, 0..) |item, i| {
        const single = pipeline.embedImages(images[i .. i + 1]) catch |single_err| {
            if (shouldAbortDensePartialFallback(single_err)) return single_err;
            try errors.append(allocator, embedItemFailure(item.index, single_err, "image_inference"));
            continue;
        };
        defer allocator.free(single);
        if (single.len != 1) {
            for (single) |embedding| allocator.free(embedding);
            try errors.append(allocator, embedItemFailure(item.index, error.MissingEmbeddingResult, "image_inference"));
            continue;
        }
        result.embeddings[item.index] = single[0];
    }
}

fn embedAudioInputsIndividually(
    allocator: std.mem.Allocator,
    pipeline: *embedding_mod.EmbeddingPipeline,
    items: []const ParsedBinaryEmbedInput,
    audio_inputs: []const embedding_mod.EncodedAudioClip,
    errors: *std.ArrayListUnmanaged(EmbedItemError),
    result: *DenseEmbedPartialResult,
) !void {
    for (items, 0..) |item, i| {
        const single = pipeline.embedEncodedAudio(audio_inputs[i .. i + 1]) catch |single_err| {
            if (shouldAbortDensePartialFallback(single_err)) return single_err;
            try errors.append(allocator, embedItemFailure(item.index, single_err, "audio_inference"));
            continue;
        };
        defer allocator.free(single);
        if (single.len != 1) {
            for (single) |embedding| allocator.free(embedding);
            try errors.append(allocator, embedItemFailure(item.index, error.MissingEmbeddingResult, "audio_inference"));
            continue;
        }
        result.embeddings[item.index] = single[0];
    }
}

/// Scale factor restoring unit L2 norm after Matryoshka truncation. MRL
/// embeddings must be re-normalized after truncating to `dimensions`;
/// serving raw truncated vectors silently breaks cosine/dot equivalence.
fn truncatedEmbeddingScale(emb: []const f32, dimensions: usize, renormalize: bool) f64 {
    if (!renormalize or dimensions >= emb.len) return 1.0;
    var norm_sq: f64 = 0;
    for (emb[0..dimensions]) |val| norm_sq += @as(f64, val) * @as(f64, val);
    if (norm_sq <= 0) return 1.0;
    return 1.0 / @sqrt(norm_sq);
}

fn buildEmbedDenseResponse(
    arena: std.mem.Allocator,
    model_name: []const u8,
    embeddings: []const []const f32,
    requested_dimensions: ?usize,
    renormalize_truncated: bool,
    prompt_tokens: usize,
) !EmbedResponseStrict {
    const data = try arena.alloc(api.EmbeddingObject, embeddings.len);
    for (embeddings, 0..) |emb, i| {
        const dimensions = requested_dimensions orelse emb.len;
        if (dimensions > emb.len) return error.InvalidEmbeddingDimensions;
        var arr: std.json.Array = .init(arena);
        try arr.ensureTotalCapacity(dimensions);
        const scale = truncatedEmbeddingScale(emb, dimensions, renormalize_truncated);
        for (emb[0..dimensions]) |val| arr.appendAssumeCapacity(.{ .float = @as(f64, val) * scale });
        data[i] = .{
            .object = "embedding",
            .index = @intCast(i),
            .embedding = .{ .array = arr },
        };
    }
    return .{
        .object = "list",
        .data = data,
        .model = model_name,
        .usage = .{
            .prompt_tokens = @intCast(prompt_tokens),
            .total_tokens = @intCast(prompt_tokens),
        },
    };
}

fn buildEmbedDensePartialResponse(
    arena: std.mem.Allocator,
    model_name: []const u8,
    result: *const DenseEmbedPartialResult,
    requested_dimensions: ?usize,
    renormalize_truncated: bool,
    prompt_tokens: usize,
) !EmbedDensePartialResponse {
    const succeeded = result.successCount();
    const data = try arena.alloc(api.EmbeddingObject, succeeded);
    var out_index: usize = 0;
    for (result.embeddings, 0..) |maybe_embedding, input_index| {
        const emb = maybe_embedding orelse continue;
        const dimensions = requested_dimensions orelse emb.len;
        if (dimensions > emb.len) return error.InvalidEmbeddingDimensions;
        var arr: std.json.Array = .init(arena);
        try arr.ensureTotalCapacity(dimensions);
        const scale = truncatedEmbeddingScale(emb, dimensions, renormalize_truncated);
        for (emb[0..dimensions]) |val| arr.appendAssumeCapacity(.{ .float = @as(f64, val) * scale });
        data[out_index] = .{
            .object = "embedding",
            .index = @intCast(input_index),
            .embedding = .{ .array = arr },
        };
        out_index += 1;
    }

    const errors = try arena.dupe(EmbedItemError, result.errors);
    return .{
        .object = "list",
        .data = data,
        .model = model_name,
        .errors = errors,
        .summary = .{
            .total = @intCast(result.embeddings.len),
            .succeeded = @intCast(succeeded),
            .failed = @intCast(result.errors.len),
        },
        .usage = .{
            .prompt_tokens = @intCast(prompt_tokens),
            .total_tokens = @intCast(prompt_tokens),
        },
    };
}

fn buildEmbedSparseResponse(
    arena: std.mem.Allocator,
    model_name: []const u8,
    sparse_vecs: []const sparse_embedding_mod.SparseVector,
    prompt_tokens: usize,
) !EmbedResponseStrict {
    const data = try arena.alloc(api.EmbeddingObject, sparse_vecs.len);
    for (sparse_vecs, 0..) |sv, i| {
        var indices: std.json.Array = .init(arena);
        try indices.ensureTotalCapacity(sv.indices.len);
        for (sv.indices) |idx| indices.appendAssumeCapacity(.{ .integer = @intCast(idx) });

        var values: std.json.Array = .init(arena);
        try values.ensureTotalCapacity(sv.values.len);
        for (sv.values) |val| values.appendAssumeCapacity(.{ .float = val });

        var obj: std.json.ObjectMap = .empty;
        try obj.put(arena, "indices", .{ .array = indices });
        try obj.put(arena, "values", .{ .array = values });

        data[i] = .{
            .object = "embedding",
            .index = @intCast(i),
            .embedding = .{ .object = obj },
        };
    }
    return .{
        .object = "list",
        .data = data,
        .model = model_name,
        .usage = .{
            .prompt_tokens = @intCast(prompt_tokens),
            .total_tokens = @intCast(prompt_tokens),
        },
    };
}

test "Antfly inference embeddings validates encoding format and dimensions" {
    try validateEmbeddingEncodingFormat(null);
    try validateEmbeddingEncodingFormat("float");
    try std.testing.expectError(error.UnsupportedEncodingFormat, validateEmbeddingEncodingFormat("base64"));
    try std.testing.expectEqual(@as(?usize, null), try parseRequestedEmbeddingDimensions(null));
    try std.testing.expectEqual(@as(?usize, 128), try parseRequestedEmbeddingDimensions(128));
    try std.testing.expectError(error.InvalidEmbeddingDimensions, parseRequestedEmbeddingDimensions(0));
    try std.testing.expectError(error.InvalidEmbeddingDimensions, parseRequestedEmbeddingDimensions(-1));
}

/// Test shim: applies request options and asserts no owned prefix escaped
/// (owned prefixes are only produced by per-request instructions).
fn applyDenseEmbeddingRequestOptionsForTest(
    pipeline: *embedding_mod.EmbeddingPipeline,
    manifest: *const manifest_mod.ModelManifest,
    request: ParsedEmbedRequest,
) !void {
    const owned = try applyDenseEmbeddingRequestOptions(std.testing.allocator, pipeline, manifest, request);
    try std.testing.expectEqual(@as(?[]u8, null), owned);
}

test "qwen3 embedding request options wrap queries with instructions" {
    const allocator = std.testing.allocator;
    var manifest = manifest_mod.ModelManifest{
        .allocator = allocator,
        .pooling = .last,
        .embedding_style = .qwen3_embedding,
        .model_type = .embedder,
    };
    defer manifest.deinit();

    var pipeline = embedding_mod.EmbeddingPipeline{
        .allocator = allocator,
        .session = undefined,
        .tok = undefined,
        .config = .{},
    };

    // Queries get the model-card default instruction when none is supplied.
    const query_request = ParsedEmbedRequest{
        .model = "qwen3-embedding",
        .input = .{ .string = "hello" },
        .encoding_format = null,
        .dimensions = null,
        .task_type = .RETRIEVAL_QUERY,
    };
    try applyDenseEmbeddingRequestOptionsForTest(&pipeline, &manifest, query_request);
    try std.testing.expectEqualStrings(
        manifest_mod.qwen3_embedding_default_query_prefix,
        pipeline.config.text_prefix,
    );

    // Documents are embedded raw — no prefix at all.
    const document_request = ParsedEmbedRequest{
        .model = "qwen3-embedding",
        .input = .{ .string = "hello" },
        .encoding_format = null,
        .dimensions = null,
        .task_type = null,
    };
    try applyDenseEmbeddingRequestOptionsForTest(&pipeline, &manifest, document_request);
    try std.testing.expectEqualStrings("", pipeline.config.text_prefix);

    // Non-retrieval task types are instruction-side for Qwen3-Embedding
    // (unlike Jina, which rejects them).
    const clustering_request = ParsedEmbedRequest{
        .model = "qwen3-embedding",
        .input = .{ .string = "hello" },
        .encoding_format = null,
        .dimensions = null,
        .task_type = .CLUSTERING,
    };
    try applyDenseEmbeddingRequestOptionsForTest(&pipeline, &manifest, clustering_request);
    try std.testing.expectEqualStrings(
        manifest_mod.qwen3_embedding_default_query_prefix,
        pipeline.config.text_prefix,
    );

    // A custom instruction renders into the Instruct/Query wrapper.
    const custom_request = ParsedEmbedRequest{
        .model = "qwen3-embedding",
        .input = .{ .string = "hello" },
        .encoding_format = null,
        .dimensions = null,
        .task_type = .RETRIEVAL_QUERY,
        .instruction = "Given a legal question, retrieve statutes that answer it",
    };
    const owned = try applyDenseEmbeddingRequestOptions(allocator, &pipeline, &manifest, custom_request);
    defer if (owned) |p| allocator.free(p);
    try std.testing.expectEqualStrings(
        "Instruct: Given a legal question, retrieve statutes that answer it\nQuery:",
        pipeline.config.text_prefix,
    );

    // Instructions on document-side requests are rejected loudly.
    const doc_instruction_request = ParsedEmbedRequest{
        .model = "qwen3-embedding",
        .input = .{ .string = "hello" },
        .encoding_format = null,
        .dimensions = null,
        .task_type = .RETRIEVAL_DOCUMENT,
        .instruction = "some task",
    };
    try std.testing.expectError(
        error.InstructionRequiresQueryTask,
        applyDenseEmbeddingRequestOptions(allocator, &pipeline, &manifest, doc_instruction_request),
    );
}

test "instruction is rejected for non-instruction models" {
    const allocator = std.testing.allocator;
    var manifest = manifest_mod.ModelManifest{ .allocator = allocator };
    defer manifest.deinit();

    var pipeline = embedding_mod.EmbeddingPipeline{
        .allocator = allocator,
        .session = undefined,
        .tok = undefined,
        .config = .{},
    };

    const request = ParsedEmbedRequest{
        .model = "bge-m3",
        .input = .{ .string = "hello" },
        .encoding_format = null,
        .dimensions = null,
        .task_type = null,
        .instruction = "some task",
    };
    try std.testing.expectError(
        error.InstructionNotSupportedForModel,
        applyDenseEmbeddingRequestOptions(allocator, &pipeline, &manifest, request),
    );
}

test "jina embedding request options switch query and document prefixes" {
    const allocator = std.testing.allocator;
    var manifest = manifest_mod.ModelManifest{
        .allocator = allocator,
        .pooling = .last,
    };
    defer manifest.deinit();
    manifest.embedding_text_prefix = try allocator.dupe(u8, "Document: ");
    manifest.tasks = try allocator.alloc([]const u8, 1);
    manifest.tasks[0] = try allocator.dupe(u8, "retrieval");

    var pipeline = embedding_mod.EmbeddingPipeline{
        .allocator = allocator,
        .session = undefined,
        .tok = undefined,
        .config = .{},
    };

    const query_request = ParsedEmbedRequest{
        .model = "jina",
        .input = .{ .string = "hello" },
        .encoding_format = null,
        .dimensions = null,
        .task_type = .RETRIEVAL_QUERY,
    };
    try applyDenseEmbeddingRequestOptionsForTest(&pipeline, &manifest, query_request);
    try std.testing.expectEqualStrings("Query: ", pipeline.config.text_prefix);

    const qa_request = ParsedEmbedRequest{
        .model = "jina",
        .input = .{ .string = "hello" },
        .encoding_format = null,
        .dimensions = null,
        .task_type = .QUESTION_ANSWERING,
    };
    try applyDenseEmbeddingRequestOptionsForTest(&pipeline, &manifest, qa_request);
    try std.testing.expectEqualStrings("Query: ", pipeline.config.text_prefix);

    const document_request = ParsedEmbedRequest{
        .model = "jina",
        .input = .{ .string = "hello" },
        .encoding_format = null,
        .dimensions = null,
        .task_type = .RETRIEVAL_DOCUMENT,
    };
    try applyDenseEmbeddingRequestOptionsForTest(&pipeline, &manifest, document_request);
    try std.testing.expectEqualStrings("Document: ", pipeline.config.text_prefix);

    const bad_task_type = ParsedEmbedRequest{
        .model = "jina",
        .input = .{ .string = "hello" },
        .encoding_format = null,
        .dimensions = null,
        .task_type = .CLASSIFICATION,
    };
    try std.testing.expectError(error.UnsupportedEmbeddingTaskType, applyDenseEmbeddingRequestOptionsForTest(&pipeline, &manifest, bad_task_type));
}

test "jina embedding request options support legacy input_type aliases" {
    const allocator = std.testing.allocator;
    var manifest = manifest_mod.ModelManifest{
        .allocator = allocator,
        .pooling = .last,
    };
    defer manifest.deinit();
    manifest.embedding_text_prefix = try allocator.dupe(u8, "Document: ");

    var pipeline = embedding_mod.EmbeddingPipeline{
        .allocator = allocator,
        .session = undefined,
        .tok = undefined,
        .config = .{},
    };

    const body =
        \\{
        \\  "model": "jina-merged",
        \\  "input": "hello",
        \\  "input_type": "query"
        \\}
    ;
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, body, .{});
    defer parsed.deinit();

    const request = try parseEmbedRequest(parsed.value);
    try std.testing.expectEqual(EmbeddingTaskType.RETRIEVAL_QUERY, request.task_type.?);
    try applyDenseEmbeddingRequestOptionsForTest(&pipeline, &manifest, request);
    try std.testing.expectEqualStrings("Query: ", pipeline.config.text_prefix);
}

test "embedding request parser uses Google task_type as canonical field" {
    const allocator = std.testing.allocator;
    const body =
        \\{
        \\  "model": "jina",
        \\  "input": "hello",
        \\  "task_type": "RETRIEVAL_DOCUMENT"
        \\}
    ;
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, body, .{});
    defer parsed.deinit();

    const request = try parseEmbedRequest(parsed.value);
    try std.testing.expectEqual(EmbeddingTaskType.RETRIEVAL_DOCUMENT, request.task_type.?);
}

test "embedding request parser rejects conflicting task aliases" {
    const allocator = std.testing.allocator;
    const body =
        \\{
        \\  "model": "jina",
        \\  "input": "hello",
        \\  "task_type": "RETRIEVAL_QUERY",
        \\  "input_type": "search_document"
        \\}
    ;
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, body, .{});
    defer parsed.deinit();

    try std.testing.expectError(error.ConflictingEmbeddingTaskTypes, parseEmbedRequest(parsed.value));
}

test "Antfly inference embed parser accepts per-item error policy" {
    const alloc = std.testing.allocator;
    const body =
        \\{
        \\  "model": "clipclap",
        \\  "input": ["hello"],
        \\  "error_policy": "per_item"
        \\}
    ;

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
    defer parsed.deinit();

    const request = try parseEmbedRequest(parsed.value);
    try std.testing.expectEqual(EmbedErrorPolicy.per_item, request.error_policy);
}

test "Antfly inference embed parser rejects unknown error policy" {
    const alloc = std.testing.allocator;
    const body =
        \\{
        \\  "model": "clipclap",
        \\  "input": ["hello"],
        \\  "error_policy": "best_effort"
        \\}
    ;

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
    defer parsed.deinit();

    try std.testing.expectError(error.UnsupportedEmbeddingErrorPolicy, parseEmbedRequest(parsed.value));
}

test "Antfly inference per-item dense parser records invalid media and keeps siblings" {
    const alloc = std.testing.allocator;
    const body =
        \\{
        \\  "model": "clipclap",
        \\  "error_policy": "per_item",
        \\  "input": [
        \\    "text that can still be embedded",
        \\    {"type":"media","mime_type":"image/webp","data":"not-valid-base64-or-image"}
        \\  ]
        \\}
    ;

    var parsed_json = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
    defer parsed_json.deinit();

    const request = try parseEmbedRequest(parsed_json.value);
    var node: Node = undefined;
    node.config = .{};
    const manifest = manifest_mod.ModelManifest{
        .allocator = alloc,
        .model_type = .embedder,
        .visual_model_path = "visual.onnx",
    };

    var inputs = try parseDenseEmbedInputsPerItem(&node, alloc, &manifest, request.input);
    defer inputs.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), inputs.total_count);
    try std.testing.expectEqual(@as(usize, 1), inputs.texts.items.len);
    try std.testing.expectEqual(@as(usize, 0), inputs.images.items.len);
    try std.testing.expectEqual(@as(usize, 1), inputs.parse_errors.items.len);
    try std.testing.expectEqual(@as(i64, 1), inputs.parse_errors.items[0].index);
    try std.testing.expectEqualStrings("INVALID_MEDIA", inputs.parse_errors.items[0].code);
    try std.testing.expectEqualStrings("parse", inputs.parse_errors.items[0].stage);
    try std.testing.expectEqual(false, inputs.parse_errors.items[0].retryable);
}

test "Antfly inference per-item dense parser never masks allocator exhaustion" {
    const backing_allocator = std.testing.allocator;
    const body =
        \\{
        \\  "model": "clipclap",
        \\  "error_policy": "per_item",
        \\  "input": [{"type":"media","mime_type":"image/png","data":"AQI="}]
        \\}
    ;
    var parsed_json = try std.json.parseFromSlice(std.json.Value, backing_allocator, body, .{});
    defer parsed_json.deinit();
    const request = try parseEmbedRequest(parsed_json.value);
    var node: Node = undefined;
    node.config = .{};
    const manifest = manifest_mod.ModelManifest{
        .allocator = backing_allocator,
        .model_type = .embedder,
        .visual_model_path = "visual.onnx",
    };

    const Runner = struct {
        fn run(
            allocator: std.mem.Allocator,
            target: *Node,
            model_manifest: *const manifest_mod.ModelManifest,
            input: std.json.Value,
        ) !void {
            var budget = RequestMediaBudget.init(1024);
            var inputs = try parseDenseEmbedInputsPerItemWithBudget(target, allocator, model_manifest, input, &budget);
            defer inputs.deinit(allocator);
        }
    };

    var fail_index: usize = 0;
    while (true) : (fail_index += 1) {
        var failing = std.testing.FailingAllocator.init(backing_allocator, .{
            .fail_index = fail_index,
            .resize_fail_index = 0,
        });
        Runner.run(failing.allocator(), &node, &manifest, request.input) catch |err| switch (err) {
            error.OutOfMemory => {
                try std.testing.expect(failing.has_induced_failure);
                try std.testing.expectEqual(failing.allocated_bytes, failing.freed_bytes);
                continue;
            },
            else => return err,
        };
        try std.testing.expect(!failing.has_induced_failure);
        try std.testing.expectEqual(failing.allocated_bytes, failing.freed_bytes);
        break;
    }
}

test "dense partial result cleanup owns every allocation exactly once" {
    const Probe = struct {
        fn failAfterPopulating(allocator: std.mem.Allocator) !void {
            var result = try initDenseEmbedPartialResult(allocator, 2);
            errdefer result.deinit(allocator);
            result.embeddings[0] = try allocator.dupe(f32, &.{ 1.0, 2.0 });
            return error.InjectedFailure;
        }
    };

    try std.testing.expectError(error.InjectedFailure, Probe.failAfterPopulating(std.testing.allocator));
}

test "audio embedding asset guard releases admission on early failure" {
    var model: model_manager_mod.LoadedModel = undefined;
    model.embedding_session_lock = .unlocked;
    model.audio_session = null;
    model.audio_projection = null;
    model.audio_resource_lease = null;
    model.audio_projection_resource_lease = null;

    var guard = AudioEmbeddingAssetGuard.init(&model, true);
    const Probe = struct {
        fn fail(owned: *AudioEmbeddingAssetGuard) !void {
            defer owned.deinit();
            return error.InjectedFailure;
        }
    };

    try std.testing.expectError(error.InjectedFailure, Probe.fail(&guard));
    try std.testing.expect(!guard.held);
    guard.deinit();
}

test "primary embedding admission failures are recorded per affected item" {
    const allocator = std.testing.allocator;
    const texts = [_]ParsedTextEmbedInput{
        .{ .index = 1, .text = "first" },
        .{ .index = 3, .text = "second" },
    };
    var image_bytes = [_]u8{ 1, 2, 3 };
    const images = [_]ParsedBinaryEmbedInput{
        .{ .index = 4, .bytes = &image_bytes, .mime_type = "image/png" },
    };
    var errors = std.ArrayListUnmanaged(EmbedItemError).empty;
    defer errors.deinit(allocator);

    try appendDenseEmbeddingItemErrors(
        allocator,
        &errors,
        &texts,
        error.ResourceTemporarilyUnavailable,
        "model_admission",
    );
    try appendDenseEmbeddingItemErrors(
        allocator,
        &errors,
        &images,
        error.ResourceLimitExceeded,
        "model_admission",
    );

    try std.testing.expectEqual(@as(usize, 3), errors.items.len);
    try std.testing.expectEqual(@as(i64, 1), errors.items[0].index);
    try std.testing.expectEqualStrings("MODEL_RESOURCE_BUSY", errors.items[0].code);
    try std.testing.expectEqual(@as(u16, 503), errors.items[0].status);
    try std.testing.expect(errors.items[0].retryable);
    try std.testing.expectEqual(
        @as(?i64, transient_capacity_retry_after_ms),
        errors.items[0].retry_after_ms,
    );
    try std.testing.expectEqual(@as(i64, 3), errors.items[1].index);
    try std.testing.expectEqualStrings("model_admission", errors.items[1].stage);
    try std.testing.expectEqual(@as(i64, 4), errors.items[2].index);
    try std.testing.expectEqualStrings("MODEL_RESOURCE_LIMIT", errors.items[2].code);
    try std.testing.expectEqual(@as(u16, 400), errors.items[2].status);
    try std.testing.expect(!errors.items[2].retryable);
    try std.testing.expectEqual(@as(?i64, null), errors.items[2].retry_after_ms);
}

fn expectJsonNumber(expected: f64, value: std.json.Value) !void {
    const actual: f64 = switch (value) {
        .float => |float| float,
        .integer => |integer| @floatFromInt(integer),
        else => return error.ExpectedJsonNumber,
    };
    try std.testing.expectEqual(expected, actual);
}

test "Antfly inference embeddings dense response supports truncation" {
    const alloc = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const embedding = [_]f32{ 1.0, 2.0, 3.0 };
    const embeddings = [_][]const f32{embedding[0..]};
    const response = try buildEmbedDenseResponse(arena.allocator(), "dense-model", &embeddings, 2, false, 7);
    const body = try std.json.Stringify.valueAlloc(alloc, response, .{});
    defer alloc.free(body);

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
    defer parsed.deinit();
    const data = parsed.value.object.get("data").?.array.items;
    try std.testing.expectEqual(@as(usize, 1), data.len);
    const embedding_json = data[0].object.get("embedding").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), embedding_json.len);
    try expectJsonNumber(1.0, embedding_json[0]);
    try expectJsonNumber(2.0, embedding_json[1]);
    try std.testing.expectEqual(@as(i64, 7), parsed.value.object.get("usage").?.object.get("prompt_tokens").?.integer);
    try std.testing.expectEqual(@as(i64, 7), parsed.value.object.get("usage").?.object.get("total_tokens").?.integer);
}

test "Antfly inference embeddings truncation renormalizes for MRL models" {
    const alloc = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    // Unit-norm 3-vector; truncating to the first two components must
    // rescale them back onto the unit sphere (Matryoshka contract).
    const embedding = [_]f32{ 0.6, 0.8, 0.0 };
    const full_norm = [_]f32{ 0.48, 0.64, 0.6 };
    const embeddings = [_][]const f32{ embedding[0..], full_norm[0..] };
    const response = try buildEmbedDenseResponse(arena.allocator(), "dense-model", &embeddings, 2, true, 7);
    const body = try std.json.Stringify.valueAlloc(alloc, response, .{});
    defer alloc.free(body);

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
    defer parsed.deinit();
    const data = parsed.value.object.get("data").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), data.len);
    for (data) |item| {
        const embedding_json = item.object.get("embedding").?.array.items;
        try std.testing.expectEqual(@as(usize, 2), embedding_json.len);
        var norm_sq: f64 = 0;
        for (embedding_json) |component| norm_sq += component.float * component.float;
        try std.testing.expectApproxEqAbs(@as(f64, 1.0), norm_sq, 1e-9);
    }
    // Full-dimension responses stay untouched even with renormalize on.
    const full_response = try buildEmbedDenseResponse(arena.allocator(), "dense-model", &embeddings, null, true, 7);
    const full_first = full_response.data[0].embedding.?.array.items;
    try expectJsonNumber(@as(f64, @floatCast(@as(f32, 0.6))), full_first[0]);
}

test "Antfly inference embeddings per-item response includes successes and indexed errors" {
    const alloc = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();

    const first = try alloc.dupe(f32, &[_]f32{ 1.0, 2.0, 3.0 });
    defer alloc.free(first);
    const third = try alloc.dupe(f32, &[_]f32{ 4.0, 5.0, 6.0 });
    defer alloc.free(third);
    var errors = [_]EmbedItemError{
        embedItemFailure(1, error.ImageDecodeFailed, "image_inference"),
    };
    var embeddings = [_]?[]f32{ first, null, third };
    const partial = DenseEmbedPartialResult{
        .embeddings = embeddings[0..],
        .errors = errors[0..],
    };

    const response = try buildEmbedDensePartialResponse(arena.allocator(), "dense-model", &partial, 2, false, 7);
    const body = try std.json.Stringify.valueAlloc(alloc, response, .{});
    defer alloc.free(body);

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
    defer parsed.deinit();

    const data = parsed.value.object.get("data").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), data.len);
    try std.testing.expectEqual(@as(i64, 0), data[0].object.get("index").?.integer);
    try std.testing.expectEqual(@as(i64, 2), data[1].object.get("index").?.integer);
    try std.testing.expectEqual(@as(usize, 2), data[0].object.get("embedding").?.array.items.len);

    const response_errors = parsed.value.object.get("errors").?.array.items;
    try std.testing.expectEqual(@as(usize, 1), response_errors.len);
    try std.testing.expectEqual(@as(i64, 1), response_errors[0].object.get("index").?.integer);
    try std.testing.expectEqualStrings("INVALID_IMAGE", response_errors[0].object.get("code").?.string);
    try std.testing.expectEqual(false, response_errors[0].object.get("retryable").?.bool);

    const summary = parsed.value.object.get("summary").?.object;
    try std.testing.expectEqual(@as(i64, 3), summary.get("total").?.integer);
    try std.testing.expectEqual(@as(i64, 2), summary.get("succeeded").?.integer);
    try std.testing.expectEqual(@as(i64, 1), summary.get("failed").?.integer);
}

test "Antfly inference embeddings sparse response uses the shared embedding field" {
    const alloc = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const indices = try alloc.dupe(u32, &[_]u32{ 7, 42 });
    defer alloc.free(indices);
    const values = try alloc.dupe(f32, &[_]f32{ 1.5, 0.5 });
    defer alloc.free(values);
    const sparse = [_]sparse_embedding_mod.SparseVector{
        .{
            .indices = indices,
            .values = values,
        },
    };

    const response = try buildEmbedSparseResponse(arena.allocator(), "sparse-model", &sparse, 11);
    const body = try std.json.Stringify.valueAlloc(alloc, response, .{});
    defer alloc.free(body);

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
    defer parsed.deinit();
    const data = parsed.value.object.get("data").?.array.items;
    try std.testing.expectEqual(@as(usize, 1), data.len);
    const sparse_embedding = data[0].object.get("embedding").?.object;
    try std.testing.expectEqual(@as(usize, 2), sparse_embedding.get("indices").?.array.items.len);
    try std.testing.expectEqual(@as(usize, 2), sparse_embedding.get("values").?.array.items.len);
    try std.testing.expectEqual(@as(i64, 11), parsed.value.object.get("usage").?.object.get("prompt_tokens").?.integer);
    try std.testing.expectEqual(@as(i64, 11), parsed.value.object.get("usage").?.object.get("total_tokens").?.integer);
}

test "Antfly inference embed request parser accepts multimodal content parts" {
    const alloc = std.testing.allocator;
    const body =
        \\{
        \\  "model": "clipclap",
        \\  "input": [
        \\    {"type":"text","text":"hello world"},
        \\    {"type":"image_url","image_url":{"url":"data:image/png;base64,AA=="}},
        \\    {"type":"media","mime_type":"audio/wav","data":"AA=="}
        \\  ]
        \\}
    ;

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
    defer parsed.deinit();

    const request = try parseEmbedRequest(parsed.value);
    var node: Node = undefined;
    node.config = .{};
    const manifest = manifest_mod.ModelManifest{
        .allocator = alloc,
        .model_type = .embedder,
        .visual_model_path = "visual.onnx",
        .audio_model_path = "audio.onnx",
    };

    var inputs = try parseDenseEmbedInputs(&node, alloc, &manifest, request.input);
    defer inputs.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 3), inputs.total_count);
    try std.testing.expectEqual(@as(usize, 1), inputs.texts.items.len);
    try std.testing.expectEqual(@as(usize, 1), inputs.images.items.len);
    try std.testing.expectEqual(@as(usize, 1), inputs.audio.items.len);
    try std.testing.expectEqualStrings("hello world", inputs.texts.items[0].text);
}

test "Antfly inference embed request parser accepts mixed strings and content parts" {
    const alloc = std.testing.allocator;
    const body =
        \\{
        \\  "model": "clipclap",
        \\  "input": [
        \\    "hello world",
        \\    {"type":"image_url","image_url":{"url":"data:image/png;base64,AA=="}},
        \\    {"type":"media","mime_type":"audio/wav","data":"AA=="}
        \\  ]
        \\}
    ;

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
    defer parsed.deinit();

    const request = try parseEmbedRequest(parsed.value);
    var node: Node = undefined;
    node.config = .{};
    const manifest = manifest_mod.ModelManifest{
        .allocator = alloc,
        .model_type = .embedder,
        .visual_model_path = "visual.onnx",
        .audio_model_path = "audio.onnx",
    };

    var inputs = try parseDenseEmbedInputs(&node, alloc, &manifest, request.input);
    defer inputs.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 3), inputs.total_count);
    try std.testing.expectEqual(@as(usize, 1), inputs.texts.items.len);
    try std.testing.expectEqual(@as(usize, 1), inputs.images.items.len);
    try std.testing.expectEqual(@as(usize, 1), inputs.audio.items.len);
    try std.testing.expectEqual(@as(usize, 0), inputs.texts.items[0].index);
    try std.testing.expectEqual(@as(usize, 1), inputs.images.items[0].index);
    try std.testing.expectEqual(@as(usize, 2), inputs.audio.items[0].index);
}

test "Antfly inference embed media-only usage does not require text tokens" {
    const alloc = std.testing.allocator;
    const body =
        \\{
        \\  "model": "clipclap",
        \\  "input": [
        \\    {"type":"image_url","image_url":{"url":"data:image/png;base64,AA=="}},
        \\    {"type":"media","mime_type":"audio/wav","data":"AA=="}
        \\  ]
        \\}
    ;

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
    defer parsed.deinit();

    const request = try parseEmbedRequest(parsed.value);
    var node: Node = undefined;
    node.config = .{};
    const manifest = manifest_mod.ModelManifest{
        .allocator = alloc,
        .model_type = .embedder,
        .visual_model_path = "visual.onnx",
        .audio_model_path = "audio.onnx",
    };

    var inputs = try parseDenseEmbedInputs(&node, alloc, &manifest, request.input);
    defer inputs.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), inputs.total_count);
    try std.testing.expectEqual(@as(usize, 0), inputs.texts.items.len);
    try std.testing.expectEqual(@as(usize, 0), estimateParsedDenseEmbedPromptTokens(&inputs));
}

test "embedding image decode failures are permanent client input errors" {
    const failure = embedDenseInputFailure(error.ImageDecodeFailed);
    try std.testing.expectEqual(@as(u16, 400), failure.status);
    try std.testing.expectEqualStrings("INVALID_IMAGE", failure.code);
    try std.testing.expectEqualStrings("unsupported or corrupt image input", failure.message);

    const runtime_failure = embedDenseInputFailure(error.OutOfMemory);
    try std.testing.expectEqual(@as(u16, 500), runtime_failure.status);
    try std.testing.expectEqualStrings("INFERENCE_FAILED", runtime_failure.code);

    const limit_failure = embedDenseInputFailure(error.ResourceLimitExceeded);
    try std.testing.expectEqual(@as(u16, 400), limit_failure.status);
    try std.testing.expectEqualStrings("MODEL_RESOURCE_LIMIT", limit_failure.code);

    const busy_failure = embedDenseInputFailure(error.ResourceTemporarilyUnavailable);
    try std.testing.expectEqual(@as(u16, 503), busy_failure.status);
    try std.testing.expectEqualStrings("MODEL_RESOURCE_BUSY", busy_failure.code);
}

test "Antfly inference embed parser accepts data uri media payloads" {
    const alloc = std.testing.allocator;
    const body =
        \\{
        \\  "model": "clipclap",
        \\  "input": [
        \\    {"type":"media","mime_type":"image/png","data":"data:image/png;base64,AQI="}
        \\  ]
        \\}
    ;

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
    defer parsed.deinit();

    const request = try parseEmbedRequest(parsed.value);
    var node: Node = undefined;
    node.config = .{};
    const manifest = manifest_mod.ModelManifest{
        .allocator = alloc,
        .model_type = .embedder,
        .visual_model_path = "visual.onnx",
    };

    var inputs = try parseDenseEmbedInputs(&node, alloc, &manifest, request.input);
    defer inputs.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), inputs.total_count);
    try std.testing.expectEqual(@as(usize, 1), inputs.images.items.len);
    try std.testing.expectEqualSlices(u8, &.{ 1, 2 }, inputs.images.items[0].bytes);
}

test "Antfly inference embed parser applies one aggregate budget to inline media" {
    const alloc = std.testing.allocator;
    const body =
        \\{
        \\  "model": "clipclap",
        \\  "input": [
        \\    {"type":"media","mime_type":"image/png","data":"YWJj"},
        \\    {"type":"media","mime_type":"image/png","data":"ZGVm"}
        \\  ]
        \\}
    ;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
    defer parsed.deinit();
    const request = try parseEmbedRequest(parsed.value);
    var node: Node = undefined;
    node.config = .{};
    const manifest = manifest_mod.ModelManifest{
        .allocator = alloc,
        .model_type = .embedder,
        .visual_model_path = "visual.onnx",
    };
    var budget = RequestMediaBudget.init(5);

    try std.testing.expectError(
        error.RemoteContentTooLarge,
        parseDenseEmbedInputsWithBudget(&node, alloc, &manifest, request.input, &budget),
    );
    try std.testing.expectEqual(@as(usize, 4), budget.used_bytes);
}

test "Antfly inference embed parser rejects mismatched data uri media mime type" {
    const alloc = std.testing.allocator;
    const body =
        \\{
        \\  "model": "clipclap",
        \\  "input": [
        \\    {"type":"media","mime_type":"audio/wav","data":"data:image/png;base64,AQI="}
        \\  ]
        \\}
    ;

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
    defer parsed.deinit();

    const request = try parseEmbedRequest(parsed.value);
    var node: Node = undefined;
    node.config = .{};
    const manifest = manifest_mod.ModelManifest{
        .allocator = alloc,
        .model_type = .embedder,
        .visual_model_path = "visual.onnx",
        .audio_model_path = "audio.onnx",
    };

    try std.testing.expectError(
        error.MediaDataMimeTypeMismatch,
        parseDenseEmbedInputs(&node, alloc, &manifest, request.input),
    );
}

test "Antfly inference sparse embed parser rejects multimodal content parts" {
    const alloc = std.testing.allocator;
    const body =
        \\[
        \\  {"type":"text","text":"hello world"}
        \\]
    ;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
    defer parsed.deinit();

    try std.testing.expectError(error.SparseModelsRequireTextInput, parseSparseEmbedInputs(alloc, parsed.value));
}

test "multimodal rerank parser accepts colqwen-style text and image content parts" {
    const alloc = std.testing.allocator;
    const body =
        \\{
        \\  "model": "vidore/colqwen2-v1.0",
        \\  "query": "invoice total due date",
        \\  "documents": [
        \\    {
        \\      "content": [
        \\        {"type":"text","text":"invoice page"},
        \\        {"type":"image_url","image_url":{"url":"data:image/png;base64,AA=="}},
        \\        {"type":"media","mime_type":"image/png","data":"AQ=="},
        \\        {"type":"text","text":" appendix"}
        \\      ]
        \\    }
        \\  ]
        \\}
    ;

    var parsed = try std.json.parseFromSlice(api.RerankMultimodalRequest, alloc, body, .{});
    defer parsed.deinit();

    var node: Node = undefined;
    node.config = .{};

    var doc = try node.parseChatMessageContentToTextAndImages(alloc, parsed.value.documents[0].content);
    defer doc.deinit();

    try std.testing.expectEqualStrings("invoice page appendix", doc.text);
    try std.testing.expectEqualStrings(
        "invoice page" ++ qwen3vl_reranker.image_marker ++ qwen3vl_reranker.image_marker ++ " appendix",
        doc.qwen_content,
    );
    try std.testing.expectEqual(@as(usize, 2), doc.images.len);
    try std.testing.expectEqual(@as(usize, 1), doc.images[0].len);
    try std.testing.expectEqual(@as(usize, 1), doc.images[1].len);
    try std.testing.expectEqual(@as(u8, 0), doc.images[0][0]);
    try std.testing.expectEqual(@as(u8, 1), doc.images[1][0]);
}

test "Qwen3-VL multimodal reranker reserves projector scratch before execution" {
    var png = [_]u8{0} ** 24;
    png[0..8].* = .{ 0x89, 'P', 'N', 'G', 0x0d, 0x0a, 0x1a, 0x0a };
    png[12..16].* = .{ 'I', 'H', 'D', 'R' };
    std.mem.writeInt(u32, png[16..20], 227, .big);
    std.mem.writeInt(u32, png[20..24], 149, .big);
    var images = [_][]const u8{png[0..]};
    const documents = [_]Node.ParsedMultimodalRerankDocument{.{
        .allocator = std.testing.allocator,
        .text = &.{},
        .qwen_content = &.{},
        .images = images[0..],
    }};
    const config = gpt_model_mod.Config{
        .family = .qwen3_vl,
        .hidden_size = 2048,
        .vision_hidden_size = 1024,
        .vision_intermediate_size = 4096,
        .vision_num_attention_heads = 16,
        .vision_patch_size = 16,
        .vision_spatial_merge_size = 2,
        .vision_deepstack_visual_indexes_len = 3,
    };

    const estimate = try Node.qwen3VlRerankerMediaAdmission(&documents, config, 8192);
    try std.testing.expect(estimate.token_allowance >= 5);
    try std.testing.expect(estimate.host_scratch_bytes > 0);
    try std.testing.expect(estimate.backend_scratch_bytes > 0);
    try std.testing.expectError(
        error.InputTokenLimitExceeded,
        Node.qwen3VlRerankerMediaAdmission(&documents, config, 3),
    );
}

test "multimodal rerank parser releases both owned slices on every allocation failure" {
    const backing_allocator = std.testing.allocator;
    const body =
        \\{
        \\  "model": "vidore/colqwen2-v1.0",
        \\  "query": "invoice",
        \\  "documents": [{"content": [
        \\    {"type":"text","text":"invoice page"},
        \\    {"type":"image_url","image_url":{"url":"data:image/png;base64,YWJj"}},
        \\    {"type":"media","mime_type":"image/png","data":"ZGVm"}
        \\  ]}]
        \\}
    ;
    var parsed = try std.json.parseFromSlice(api.RerankMultimodalRequest, backing_allocator, body, .{});
    defer parsed.deinit();
    var node: Node = undefined;
    node.config = .{};

    const Runner = struct {
        fn run(allocator: std.mem.Allocator, target: *Node, content: api.ChatMessageContent) !void {
            var budget = RequestMediaBudget.init(32);
            var document = try target.parseChatMessageContentToTextAndImagesWithBudget(allocator, content, &budget);
            defer document.deinit();
        }
    };

    var fail_index: usize = 0;
    while (true) : (fail_index += 1) {
        var failing = std.testing.FailingAllocator.init(backing_allocator, .{
            .fail_index = fail_index,
            .resize_fail_index = 0,
        });
        Runner.run(failing.allocator(), &node, parsed.value.documents[0].content) catch |err| switch (err) {
            error.OutOfMemory => {
                try std.testing.expect(failing.has_induced_failure);
                try std.testing.expectEqual(failing.allocated_bytes, failing.freed_bytes);
                continue;
            },
            else => return err,
        };
        try std.testing.expect(!failing.has_induced_failure);
        try std.testing.expectEqual(failing.allocated_bytes, failing.freed_bytes);
        break;
    }
}

test "multimodal rerank parser applies one aggregate budget to data URI media" {
    const alloc = std.testing.allocator;
    const body =
        \\{
        \\  "model": "vidore/colqwen2-v1.0",
        \\  "query": "invoice",
        \\  "documents": [{"content": [
        \\    {"type":"image_url","image_url":{"url":"data:image/png;base64,YWJj"}},
        \\    {"type":"image_url","image_url":{"url":"data:image/png;base64,ZGVm"}}
        \\  ]}]
        \\}
    ;
    var parsed = try std.json.parseFromSlice(api.RerankMultimodalRequest, alloc, body, .{});
    defer parsed.deinit();
    var node: Node = undefined;
    node.config = .{};
    const first_uri = "data:image/png;base64,YWJj";
    var budget = RequestMediaBudget.init(first_uri.len + 1);

    try std.testing.expectError(
        error.RemoteContentTooLarge,
        node.parseChatMessageContentToTextAndImagesWithBudget(alloc, parsed.value.documents[0].content, &budget),
    );
    try std.testing.expectEqual(first_uri.len, budget.used_bytes);
}

test "multimodal rerank parser rejects non-image media content parts" {
    const alloc = std.testing.allocator;
    const body =
        \\{
        \\  "model": "vidore/colqwen2-v1.0",
        \\  "query": "invoice total due date",
        \\  "documents": [
        \\    {
        \\      "content": [
        \\        {"type":"media","mime_type":"audio/wav","data":"AA=="}
        \\      ]
        \\    }
        \\  ]
        \\}
    ;

    var parsed = try std.json.parseFromSlice(api.RerankMultimodalRequest, alloc, body, .{});
    defer parsed.deinit();

    var node: Node = undefined;
    node.config = .{};

    try std.testing.expectError(
        error.UnsupportedContentPartType,
        node.parseChatMessageContentToTextAndImages(alloc, parsed.value.documents[0].content),
    );
}

test "multimodal rerank parser rejects invalid image data uris" {
    const alloc = std.testing.allocator;
    const body =
        \\{
        \\  "model": "vidore/colqwen2-v1.0",
        \\  "query": "invoice total due date",
        \\  "documents": [
        \\    {
        \\      "content": [
        \\        {"type":"image_url","image_url":{"url":"data:image/png;base64,%%%"}}
        \\      ]
        \\    }
        \\  ]
        \\}
    ;

    var parsed = try std.json.parseFromSlice(api.RerankMultimodalRequest, alloc, body, .{});
    defer parsed.deinit();

    var node: Node = undefined;
    node.config = .{};

    try std.testing.expectError(
        error.InvalidImageDataUri,
        node.parseChatMessageContentToTextAndImages(alloc, parsed.value.documents[0].content),
    );
}

/// Decode a data URI (data:mime/type;base64,...) to raw bytes.
fn effectiveRequestContentSecurity(self: *const Node) scraping.ContentSecurityConfig {
    var effective = default_request_content_security;
    const configured = self.config.content_security orelse return effective;
    if (configured.allowed_hosts != null) effective.allowed_hosts = configured.allowed_hosts;
    if (configured.block_private_ips != null) effective.block_private_ips = configured.block_private_ips;
    if (configured.max_download_size_bytes != null) effective.max_download_size_bytes = configured.max_download_size_bytes;
    if (configured.download_timeout_seconds != null) effective.download_timeout_seconds = configured.download_timeout_seconds;
    if (configured.max_image_dimension != null) effective.max_image_dimension = configured.max_image_dimension;
    if (configured.allowed_paths != null) effective.allowed_paths = configured.allowed_paths;
    if (configured.user_agent != null) effective.user_agent = configured.user_agent;
    return effective;
}

const RequestMediaBudget = struct {
    // Bounds downloaded and inline encoded media. Accepted image routes apply
    // decoded-pixel admission separately before model execution.
    max_bytes: usize,
    used_bytes: usize = 0,

    fn init(max_bytes: usize) RequestMediaBudget {
        return .{ .max_bytes = max_bytes };
    }

    fn remaining(self: *const RequestMediaBudget) usize {
        return self.max_bytes -| self.used_bytes;
    }

    fn add(self: *RequestMediaBudget, bytes: usize) !void {
        if (bytes > self.remaining()) return error.RemoteContentTooLarge;
        self.used_bytes += bytes;
    }
};

const RequestMediaAdmissionShape = struct {
    image_count: usize = 0,
    // Inline encoded sources coexist with a separately allocated decoded copy.
    inline_bytes: usize = 0,
    // Direct callers already own decoded media. It is part of the logical
    // media budget but is borrowed and therefore resident only once.
    borrowed_bytes: usize = 0,
    has_remote: bool = false,

    fn addInline(self: *RequestMediaAdmissionShape, encoded_bytes: usize, is_image: bool) void {
        if (is_image) self.image_count = std.math.add(usize, self.image_count, 1) catch std.math.maxInt(usize);
        self.inline_bytes = std.math.add(usize, self.inline_bytes, encoded_bytes) catch std.math.maxInt(usize);
    }

    fn addBorrowed(self: *RequestMediaAdmissionShape, bytes: usize, is_image: bool) void {
        if (is_image) self.image_count = std.math.add(usize, self.image_count, 1) catch std.math.maxInt(usize);
        self.borrowed_bytes = std.math.add(usize, self.borrowed_bytes, bytes) catch std.math.maxInt(usize);
    }

    fn addImageUrlSlice(self: *RequestMediaAdmissionShape, source: []const u8) void {
        if (std.mem.startsWith(u8, source, "data:")) {
            self.addInline(source.len, true);
            return;
        }
        self.image_count = std.math.add(usize, self.image_count, 1) catch std.math.maxInt(usize);
        self.has_remote = true;
    }

    fn addImageUrl(self: *RequestMediaAdmissionShape, value: std.json.Value) void {
        const url: ?[]const u8 = switch (value) {
            .string => |string| string,
            .object => |object| if (object.get("url")) |candidate|
                if (candidate == .string) candidate.string else null
            else
                null,
            else => null,
        };
        self.addImageUrlSlice(url orelse return);
    }

    fn potentialBytes(self: RequestMediaAdmissionShape, request_cap: usize) usize {
        if (self.has_remote) return request_cap;
        const known_bytes = std.math.add(usize, self.inline_bytes, self.borrowed_bytes) catch std.math.maxInt(usize);
        return @min(known_bytes, request_cap);
    }
};

const DirectDenseEmbedPreflight = struct {
    shape: RequestMediaAdmissionShape,
    known_media_bytes: usize,
    has_audio: bool,
};

fn directDenseEmbedPreflight(parts: []const Node.DirectDenseEmbedPart) !DirectDenseEmbedPreflight {
    var shape: RequestMediaAdmissionShape = .{};
    var has_audio = false;
    for (parts) |part| switch (part) {
        .text => {},
        .image_url => |url| shape.addImageUrlSlice(url),
        .media => |media| {
            const is_image = std.mem.startsWith(u8, media.mime_type, "image/");
            const is_audio = std.mem.startsWith(u8, media.mime_type, "audio/");
            if (!is_image and !is_audio) return error.UnsupportedMediaMimeType;
            has_audio = has_audio or is_audio;
            shape.addBorrowed(media.data.len, is_image);
        },
    };
    return .{
        .shape = shape,
        .known_media_bytes = std.math.add(usize, shape.inline_bytes, shape.borrowed_bytes) catch
            std.math.maxInt(usize),
        .has_audio = has_audio,
    };
}

fn generateRequestMediaShape(body: api.GenerateRequest) RequestMediaAdmissionShape {
    var shape: RequestMediaAdmissionShape = .{};
    for (body.messages) |message| {
        const content = message.content orelse continue;
        if (content != .array) continue;
        for (content.array.items) |part| {
            if (part != .object) continue;
            const part_type = part.object.get("type") orelse continue;
            if (part_type != .string or !std.mem.eql(u8, part_type.string, "image_url")) continue;
            const image_url = part.object.get("image_url") orelse continue;
            shape.addImageUrl(image_url);
        }
    }
    return shape;
}

fn denseEmbedRequestMediaShape(input: std.json.Value) RequestMediaAdmissionShape {
    var shape: RequestMediaAdmissionShape = .{};
    if (input != .array) return shape;
    for (input.array.items) |part| {
        if (part != .object) continue;
        const part_type = part.object.get("type") orelse continue;
        if (part_type != .string) continue;
        if (std.mem.eql(u8, part_type.string, "image_url")) {
            const image_url = part.object.get("image_url") orelse continue;
            shape.addImageUrl(image_url);
            continue;
        }
        if (!std.mem.eql(u8, part_type.string, "media")) continue;
        const data = part.object.get("data") orelse continue;
        const mime = part.object.get("mime_type") orelse continue;
        if (data != .string or mime != .string) continue;
        shape.addInline(data.string.len, std.mem.startsWith(u8, mime.string, "image/"));
    }
    return shape;
}

fn multimodalRerankRequestMediaShape(body: api.RerankMultimodalRequest) RequestMediaAdmissionShape {
    var shape: RequestMediaAdmissionShape = .{};
    for (body.documents) |document| {
        const content = document.content;
        if (content != .array) continue;
        for (content.array.items) |part| {
            if (part != .object) continue;
            const part_type = part.object.get("type") orelse continue;
            if (part_type != .string) continue;
            if (std.mem.eql(u8, part_type.string, "image_url")) {
                const image_url = part.object.get("image_url") orelse continue;
                shape.addImageUrl(image_url);
                continue;
            }
            if (!std.mem.eql(u8, part_type.string, "media")) continue;
            const data = part.object.get("data") orelse continue;
            const mime = part.object.get("mime_type") orelse continue;
            if (data != .string or mime != .string or !std.mem.startsWith(u8, mime.string, "image/")) continue;
            shape.addInline(data.string.len, true);
        }
    }
    return shape;
}

fn requestMediaMaxBytes(self: *const Node) usize {
    const configured_u64 = effectiveRequestContentSecurity(self).max_download_size_bytes orelse
        default_max_request_media_bytes;
    const configured = std.math.cast(usize, configured_u64) orelse std.math.maxInt(usize);
    return @min(default_max_request_media_bytes, configured);
}

fn downloadRemoteContent(self: *const Node, alloc: std.mem.Allocator, url: []const u8) !scraping.DownloadedContent {
    var security = effectiveRequestContentSecurity(self);
    const s3_credentials = if (self.config.s3_credentials) |*cfg| cfg else null;
    return try scraping.downloadContentAlloc(alloc, url, &security, s3_credentials);
}

const RemoteContentRequestFailure = struct {
    status: u16,
    code: []const u8,
    message: []const u8,
    retryable: bool,
};

fn normalizeRemoteContentRequestError(err: anyerror) anyerror {
    return switch (err) {
        error.OutOfMemory, error.Timeout, error.Canceled => err,
        error.StreamTooLong => error.RemoteContentTooLarge,
        error.HostNotAllowed,
        error.PathNotAllowed,
        error.PrivateIpBlocked,
        => error.RemoteContentNotAllowed,
        error.InvalidDataUri,
        error.InvalidBase64,
        error.InvalidHost,
        error.InvalidS3Url,
        error.UnexpectedCharacter,
        error.InvalidFormat,
        error.InvalidPort,
        error.InvalidHostName,
        error.UnsupportedUrlScheme,
        => error.RemoteContentInvalid,
        error.MissingS3Credentials,
        error.MissingEndpoint,
        error.MissingAccessKeyId,
        error.MissingSecretAccessKey,
        => error.RemoteContentNotConfigured,
        else => error.RemoteContentUnavailable,
    };
}

fn isDenseEmbedRequestAbort(err: anyerror) bool {
    return err == error.OutOfMemory or err == error.Timeout or err == error.Canceled;
}

fn downloadRemoteContentWithBudgetForRequest(
    self: *Node,
    alloc: std.mem.Allocator,
    url: []const u8,
    budget: *RequestMediaBudget,
) !scraping.DownloadedContent {
    return downloadRemoteContentWithBudgetForRequestOptionalContext(self, alloc, null, url, budget);
}

fn downloadRemoteContentWithBudgetForRequestWithContext(
    self: *Node,
    alloc: std.mem.Allocator,
    request_context: DenseEmbedRequestContext,
    url: []const u8,
    budget: *RequestMediaBudget,
) !scraping.DownloadedContent {
    return downloadRemoteContentWithBudgetForRequestOptionalContext(self, alloc, request_context, url, budget);
}

fn downloadRemoteContentWithBudgetForRequestOptionalContext(
    self: *Node,
    alloc: std.mem.Allocator,
    request_context: ?DenseEmbedRequestContext,
    url: []const u8,
    budget: *RequestMediaBudget,
) !scraping.DownloadedContent {
    const remaining = budget.remaining();
    // data: URLs are already resident in the request body. Enforce the
    // encoded-source ceiling before allocating their decoded payload; remote
    // sources continue to charge the downloaded payload bytes.
    const inline_budget_bytes: ?usize = if (std.mem.startsWith(u8, url, "data:"))
        encodedMediaBudgetSize(url) catch null
    else
        null;
    if (inline_budget_bytes) |bytes| {
        if (bytes > remaining) return error.RemoteContentTooLarge;
    }

    var bounded_security = effectiveRequestContentSecurity(self);
    const remaining_u64: u64 = @intCast(remaining);
    bounded_security.max_download_size_bytes = if (bounded_security.max_download_size_bytes) |configured|
        @min(configured, remaining_u64)
    else
        remaining_u64;
    const s3_credentials = if (self.config.s3_credentials) |*cfg| cfg else null;
    const download_context = if (request_context) |context|
        try denseEmbedDownloadContext(context)
    else
        null;
    if (comptime builtin.is_test) request_work_test_counters.media_fetch_attempts += 1;
    var downloaded = (if (download_context) |context|
        scraping.downloadContentAllocWithContext(alloc, context, url, &bounded_security, s3_credentials)
    else
        scraping.downloadContentAlloc(alloc, url, &bounded_security, s3_credentials)) catch |err|
        return normalizeRemoteContentRequestError(err);
    errdefer downloaded.deinit(alloc);
    try budget.add(inline_budget_bytes orelse downloaded.data.len);
    return downloaded;
}

fn remoteContentRequestFailure(err: anyerror) ?RemoteContentRequestFailure {
    return switch (err) {
        error.RemoteContentTooLarge => .{
            .status = 413,
            .code = "CONTENT_TOO_LARGE",
            .message = "media content exceeds the configured size limit",
            .retryable = false,
        },
        error.RemoteContentNotAllowed => .{
            .status = 403,
            .code = "CONTENT_NOT_ALLOWED",
            .message = "remote content URL is blocked by content security policy",
            .retryable = false,
        },
        error.RemoteContentInvalid => .{
            .status = 400,
            .code = "INVALID_CONTENT_URL",
            .message = "remote content URL or inline data is invalid",
            .retryable = false,
        },
        error.RemoteContentNotConfigured => .{
            .status = 503,
            .code = "CONTENT_FETCH_NOT_CONFIGURED",
            .message = "remote content storage is not configured",
            .retryable = false,
        },
        error.RemoteContentUnavailable => .{
            .status = 502,
            .code = "CONTENT_FETCH_FAILED",
            .message = "remote content could not be fetched",
            .retryable = true,
        },
        else => null,
    };
}

fn isRemoteContentRequestError(err: anyerror) bool {
    return remoteContentRequestFailure(err) != null;
}

const internal_error_message = "internal inference error";

const GenerationRequestFailure = struct {
    status: u16,
    code: []const u8,
    message: []const u8,
    retryable: bool,
};

const memory_budget_exceeded_message =
    "model execution exceeds the configured inference memory budget; increase the applicable --host-budget-mb, --backend-budget-mb, --combined-budget-mb, --kv-budget-mb, --scratch-budget-mb, or --process-memory-budget-mb, or choose a smaller/quantized model";

fn logMemoryBudgetExceeded(
    session: backends_mod.Session,
    run_budget: *const runtime.tier.memory.RunBudget,
) void {
    if (builtin.is_test) return;
    var buf: [512]u8 = undefined;
    const detail = session_factory.memoryBudgetExceededDetail(session, run_budget, &buf) catch
        "request exceeds native generation memory budget";
    std.log.warn("generation memory admission denied: {s}", .{detail});
}

fn generationMemoryBudgetResponse(
    ctx: *httpx.Context,
    session: backends_mod.Session,
    run_budget: *const runtime.tier.memory.RunBudget,
) !httpx.Response {
    logMemoryBudgetExceeded(session, run_budget);
    return ctx.status(507).json(.{
        .@"error" = "MEMORY_BUDGET_EXCEEDED",
        .message = memory_budget_exceeded_message,
        .retryable = false,
    });
}

fn generationRequestFailure(err: anyerror) ?GenerationRequestFailure {
    return switch (err) {
        error.PromptTooLong => .{
            .status = 400,
            .code = "INVALID_REQUEST",
            .message = "prompt exceeds the model context window after reserving output tokens",
            .retryable = false,
        },
        error.MemoryBudgetExceeded => .{
            .status = 507,
            .code = "MEMORY_BUDGET_EXCEEDED",
            .message = memory_budget_exceeded_message,
            .retryable = false,
        },
        error.ResourceTemporarilyUnavailable => .{
            .status = 503,
            .code = "MODEL_RESOURCE_BUSY",
            .message = "insufficient inference capacity is currently available",
            .retryable = true,
        },
        else => null,
    };
}

fn internalErrorMessage(code: []const u8, err: anyerror) []const u8 {
    if (!builtin.is_test) std.log.err("inference request failed code={s}: {s}", .{ code, @errorName(err) });
    return internal_error_message;
}

fn internalErrorResponse(ctx: *httpx.Context, code: []const u8, err: anyerror) !httpx.Response {
    return ctx.status(500).json(.{
        .@"error" = code,
        .message = internalErrorMessage(code, err),
    });
}

fn generationErrorResponse(ctx: *httpx.Context, err: anyerror) !httpx.Response {
    if (err == error.ResourceTemporarilyUnavailable)
        return modelResourceBusyResponse(ctx);
    if (generationRequestFailure(err)) |failure| {
        return ctx.status(failure.status).json(.{
            .@"error" = failure.code,
            .message = failure.message,
            .retryable = failure.retryable,
        });
    }
    return internalErrorResponse(ctx, "GENERATION_FAILED", err);
}

fn batchGenerationError(err: anyerror) api.GenerateBatchError {
    if (generationRequestFailure(err)) |failure| {
        return .{
            .code = failure.code,
            .message = failure.message,
            .retryable = failure.retryable,
        };
    }
    if (err == error.InvalidStructuredOutput) {
        return .{
            .code = "STRUCTURED_OUTPUT_INVALID",
            .message = @errorName(err),
            .retryable = true,
        };
    }
    return .{
        .code = "GENERATION_FAILED",
        .message = @errorName(err),
        .retryable = true,
    };
}

fn writeInternalStreamError(
    writer: *httpx.Context.StreamWriter,
    stage: []const u8,
    err: anyerror,
) void {
    if (std.mem.eql(u8, stage, "STREAM_WRITE_FAILED") and generationStreamWriteIsPeerDisconnect(err)) {
        if (!builtin.is_test) std.log.info("generation stream closed after client disconnect", .{});
        return;
    }
    writer.writeEvent("error", internalErrorMessage(stage, err)) catch {};
}

fn writeGenerationStreamError(writer: *httpx.Context.StreamWriter, err: anyerror) void {
    if (generationRequestFailure(err)) |failure| {
        if (err == error.MemoryBudgetExceeded and !builtin.is_test)
            std.log.warn("generation memory admission denied during streaming", .{});
        writer.writeEvent("error", failure.message) catch {};
        return;
    }
    writeInternalStreamError(writer, "GENERATION_FAILED", err);
}

fn remoteContentErrorResponse(ctx: *httpx.Context, err: anyerror) !httpx.Response {
    if (err == error.OutOfMemory) return err;
    const failure = remoteContentRequestFailure(err) orelse return err;
    return ctx.status(failure.status).json(.{
        .@"error" = failure.code,
        .message = failure.message,
        .retryable = failure.retryable,
    });
}

fn readImageErrorResponse(ctx: *httpx.Context, err: anyerror) !httpx.Response {
    return switch (err) {
        error.ImageDecodeFailed => ctx.status(400).json(.{
            .@"error" = "INVALID_IMAGE",
            .message = "image input is unsupported, corrupt, or has a malformed header",
            .retryable = false,
        }),
        error.ImageTooLarge => ctx.status(413).json(.{
            .@"error" = "IMAGE_TOO_LARGE",
            .message = "image dimensions exceed the configured inference limit",
            .retryable = false,
        }),
        error.ImageBatchTooLarge => ctx.status(413).json(.{
            .@"error" = "IMAGE_BATCH_TOO_LARGE",
            .message = "aggregate decoded image pixels exceed server capacity",
            .retryable = false,
        }),
        else => err,
    };
}

fn downloadReadBatchContent(
    self: *const Node,
    alloc: std.mem.Allocator,
    url: []const u8,
    max_bytes: usize,
    current_bytes: usize,
    inline_content_trust: InlineContentTrust,
) !scraping.DownloadedContent {
    if (current_bytes >= max_bytes) return error.ReadBatchTooLarge;
    const remaining = max_bytes - current_bytes;
    const remaining_u64: u64 = @intCast(remaining);
    // Apply the node's deny-by-default policy before imposing the per-request
    // byte ceiling. Starting from the optional raw override would turn an
    // absent or empty override into an allow-all policy for this read path.
    var bounded_security = boundedReadContentSecurity(effectiveRequestContentSecurity(self), url, remaining_u64, inline_content_trust);
    const s3_credentials = if (self.config.s3_credentials) |*cfg| cfg else null;
    return try scraping.downloadContentAlloc(alloc, url, &bounded_security, s3_credentials);
}

fn downloadReadBatchContentForRequest(
    self: *const Node,
    alloc: std.mem.Allocator,
    url: []const u8,
    max_bytes: usize,
    current_bytes: usize,
) !scraping.DownloadedContent {
    return downloadReadBatchContent(self, alloc, url, max_bytes, current_bytes, .untrusted) catch |err| {
        if (err == error.ReadBatchTooLarge) return err;
        return normalizeRemoteContentRequestError(err);
    };
}

const InlineContentTrust = enum { untrusted, trusted_internal };

fn boundedReadContentSecurity(
    configured: scraping.ContentSecurityConfig,
    url: []const u8,
    remaining_bytes: u64,
    inline_content_trust: InlineContentTrust,
) scraping.ContentSecurityConfig {
    var bounded = configured;
    // Only the in-process enrichment bridge can assert trusted_internal.
    // Public callers can also submit data URIs, so URL shape alone must never
    // weaken the configured content policy.
    if (inline_content_trust == .trusted_internal and std.mem.startsWith(u8, url, "data:")) {
        bounded.max_download_size_bytes = remaining_bytes;
    } else {
        bounded.max_download_size_bytes = if (bounded.max_download_size_bytes) |limit|
            @min(limit, remaining_bytes)
        else
            remaining_bytes;
    }
    return bounded;
}

test "read content caps internal data images by inference budget and preserves remote limits" {
    const configured = scraping.ContentSecurityConfig{
        .block_private_ips = true,
        .max_download_size_bytes = 1024,
        .max_image_dimension = 8192,
    };
    const inline_security = boundedReadContentSecurity(configured, "data:image/png;base64,AA==", 64 * 1024, .trusted_internal);
    try std.testing.expectEqual(@as(?u64, 64 * 1024), inline_security.max_download_size_bytes);
    try std.testing.expectEqual(@as(?bool, true), inline_security.block_private_ips);
    try std.testing.expectEqual(@as(?u32, 8192), inline_security.max_image_dimension);

    const public_inline = boundedReadContentSecurity(configured, "data:image/png;base64,AA==", 64 * 1024, .untrusted);
    try std.testing.expectEqual(@as(?u64, 1024), public_inline.max_download_size_bytes);

    const remote = boundedReadContentSecurity(configured, "https://example.com/page.png", 64 * 1024, .untrusted);
    try std.testing.expectEqual(@as(?u64, 1024), remote.max_download_size_bytes);
    const tighter_batch = boundedReadContentSecurity(configured, "https://example.com/page.png", 512, .untrusted);
    try std.testing.expectEqual(@as(?u64, 512), tighter_batch.max_download_size_bytes);
}

fn readBatchMaxBytes() usize {
    return @max(@as(usize, 1), platform.env.getenvUsize("ANTFLY_INFERENCE_READ_BATCH_BYTES") orelse default_max_read_batch_bytes);
}

fn readInlineSourceByteCap(self: *const Node) usize {
    if (self.inference_admission.capacity == 0) return readBatchMaxBytes();
    const capacity_bytes = std.math.mul(
        usize,
        self.inference_admission.capacity,
        read_admission_bytes_per_unit,
    ) catch std.math.maxInt(usize);
    return @min(readBatchMaxBytes(), capacity_bytes);
}

fn addReadInlineSourceBytes(current: usize, url: []const u8, max_bytes: usize) !usize {
    if (!std.mem.startsWith(u8, url, "data:")) return current;
    const total = std.math.add(usize, current, url.len) catch return error.ReadBatchTooLarge;
    if (total > max_bytes) return error.ReadBatchTooLarge;
    return total;
}

const ReadRequestAdmission = struct {
    units: usize,
    // Logical downloaded/encoded-media ceiling passed to decoders and fetchers.
    byte_cap: usize,
    // Peak compressed-media residence. Inline sources keep their encoded bytes
    // alive while a separately allocated decoded copy is processed.
    resident_byte_cap: usize,
    decoded_pixel_cap: usize,
};

fn admissionUnitsFor(value: usize, per_unit: usize) usize {
    if (value == 0) return 0;
    return 1 + ((value - 1) / per_unit);
}

const AudioDecodeAdmission = struct {
    units: usize,
    max_decode_working_bytes: usize,
};

fn audioDecodeAdmissionForLimits(resident_bytes: usize, max_concurrent_units: usize) AudioDecodeAdmission {
    const admitted_resident, const max_decode_working_bytes = if (max_concurrent_units == 0)
        .{ resident_bytes, default_max_audio_decode_working_bytes }
    else blk: {
        const capacity_bytes = std.math.mul(
            usize,
            max_concurrent_units,
            read_admission_bytes_per_unit,
        ) catch std.math.maxInt(usize);
        const admitted = @min(resident_bytes, capacity_bytes);
        break :blk .{
            admitted,
            @min(default_max_audio_decode_working_bytes, capacity_bytes - admitted),
        };
    };
    const combined_bytes = std.math.add(usize, admitted_resident, max_decode_working_bytes) catch
        std.math.maxInt(usize);
    return .{
        .units = @max(@as(usize, 1), admissionUnitsFor(combined_bytes, read_admission_bytes_per_unit)),
        .max_decode_working_bytes = max_decode_working_bytes,
    };
}

fn audioDecodeAdmission(self: *const Node, resident_bytes: usize) AudioDecodeAdmission {
    return audioDecodeAdmissionForLimits(resident_bytes, self.inference_admission.capacity);
}

fn readRequestAdmissionForLimits(
    image_count: usize,
    configured_batch_byte_cap: usize,
    configured_per_image_byte_cap: usize,
    max_concurrent_units: usize,
    max_image_dimension: ?u32,
    inline_source_bytes: usize,
) ReadRequestAdmission {
    const capacity_bytes = if (max_concurrent_units == 0)
        configured_batch_byte_cap
    else
        std.math.mul(usize, max_concurrent_units, read_admission_bytes_per_unit) catch std.math.maxInt(usize);
    const aggregate_image_cap = std.math.mul(usize, image_count, configured_per_image_byte_cap) catch std.math.maxInt(usize);
    const available_download_bytes = if (max_concurrent_units == 0)
        configured_batch_byte_cap
    else
        capacity_bytes -| inline_source_bytes;
    const byte_cap = @min(@min(configured_batch_byte_cap, available_download_bytes), aggregate_image_cap);
    const resident_byte_cap = std.math.add(usize, inline_source_bytes, byte_cap) catch std.math.maxInt(usize);
    const byte_units = admissionUnitsFor(resident_byte_cap, read_admission_bytes_per_unit);
    const image_units = admissionUnitsFor(image_count, read_admission_images_per_unit);
    return .{
        .units = @max(@as(usize, 1), @max(byte_units, image_units)),
        .byte_cap = byte_cap,
        .resident_byte_cap = resident_byte_cap,
        .decoded_pixel_cap = readDecodedPixelCapForLimits(
            image_count,
            max_concurrent_units,
            max_image_dimension,
            resident_byte_cap,
        ),
    };
}

fn readDecodedPixelCapForLimits(
    image_count: usize,
    max_concurrent_units: usize,
    max_image_dimension: ?u32,
    resident_byte_cap: usize,
) usize {
    const configured_working_bytes = if (max_concurrent_units == 0)
        default_max_read_decoded_working_bytes
    else blk: {
        const capacity_bytes = std.math.mul(usize, max_concurrent_units, read_admission_bytes_per_unit) catch
            std.math.maxInt(usize);
        break :blk capacity_bytes -| resident_byte_cap;
    };
    const working_byte_cap = @min(default_max_read_decoded_working_bytes, configured_working_bytes);
    const capacity_pixels = working_byte_cap / read_decoded_working_bytes_per_pixel;

    var per_image_pixels = image_pipeline.DecodeLimits.inference_default.max_pixels;
    if (max_image_dimension) |dimension| {
        const dimension_usize: usize = @intCast(dimension);
        const configured_pixels = std.math.mul(usize, dimension_usize, dimension_usize) catch std.math.maxInt(usize);
        per_image_pixels = @min(per_image_pixels, configured_pixels);
    }
    const requested_pixels = std.math.mul(usize, image_count, per_image_pixels) catch std.math.maxInt(usize);
    return @min(capacity_pixels, requested_pixels);
}

fn readRequestAdmission(
    self: *const Node,
    image_count: usize,
    inline_source_bytes: usize,
    max_tokens: ?usize,
) ReadRequestAdmission {
    const security = effectiveRequestContentSecurity(self);
    const configured_u64 = security.max_download_size_bytes orelse
        default_max_request_media_bytes;
    const per_image_byte_cap = std.math.cast(usize, configured_u64) orelse std.math.maxInt(usize);
    var admission = readRequestAdmissionForLimits(
        image_count,
        readBatchMaxBytes(),
        per_image_byte_cap,
        self.inference_admission.capacity,
        security.max_image_dimension,
        inline_source_bytes,
    );
    admission.units = @max(admission.units, estimateReadAdmissionUnits(image_count, max_tokens));
    return admission;
}

fn requestMediaAdmissionForLimits(
    shape: RequestMediaAdmissionShape,
    request_byte_cap: usize,
    max_concurrent_units: usize,
    max_image_dimension: ?u32,
) ReadRequestAdmission {
    const potential_bytes = shape.potentialBytes(request_byte_cap);
    const byte_cap = if (max_concurrent_units == 0)
        potential_bytes
    else blk: {
        const capacity_bytes = std.math.mul(usize, max_concurrent_units, read_admission_bytes_per_unit) catch
            std.math.maxInt(usize);
        const inline_potential = @min(shape.inline_bytes, potential_bytes);
        const max_logical_bytes = if (inline_potential >= capacity_bytes / 2)
            capacity_bytes / 2
        else
            capacity_bytes - inline_potential;
        break :blk @min(potential_bytes, max_logical_bytes);
    };
    const resident_byte_cap = std.math.add(usize, byte_cap, @min(shape.inline_bytes, byte_cap)) catch
        std.math.maxInt(usize);
    return .{
        .units = @max(
            @as(usize, 1),
            @max(
                admissionUnitsFor(resident_byte_cap, read_admission_bytes_per_unit),
                admissionUnitsFor(shape.image_count, read_admission_images_per_unit),
            ),
        ),
        .byte_cap = byte_cap,
        .resident_byte_cap = resident_byte_cap,
        .decoded_pixel_cap = readDecodedPixelCapForLimits(
            shape.image_count,
            max_concurrent_units,
            max_image_dimension,
            resident_byte_cap,
        ),
    };
}

fn requestMediaAdmission(self: *const Node, shape: RequestMediaAdmissionShape) ReadRequestAdmission {
    return requestMediaAdmissionForLimits(
        shape,
        requestMediaMaxBytes(self),
        self.inference_admission.capacity,
        effectiveRequestContentSecurity(self).max_image_dimension,
    );
}

const ReadDecodedImageBudget = struct {
    base_units: usize,
    base_bytes: usize,
    max_pixels: usize,
    used_pixels: usize = 0,
    max_dimension: ?u32,

    fn init(admission: ReadRequestAdmission, max_dimension: ?u32) ReadDecodedImageBudget {
        return .{
            .base_units = admission.units,
            .base_bytes = admission.resident_byte_cap,
            .max_pixels = admission.decoded_pixel_cap,
            .max_dimension = max_dimension,
        };
    }

    fn addPixels(self: *ReadDecodedImageBudget, pixels: usize) !void {
        if (pixels > self.max_pixels -| self.used_pixels) return error.ImageBatchTooLarge;
        self.used_pixels += pixels;
    }

    fn addImage(self: *ReadDecodedImageBudget, image_bytes: []const u8) !void {
        const info = try image_pipeline.inspectEncodedForInference(image_bytes, self.max_dimension);
        const pixels = std.math.mul(usize, @as(usize, info.width), @as(usize, info.height)) catch
            return error.ImageBatchTooLarge;
        try self.addPixels(pixels);
    }

    fn requiredUnits(self: *const ReadDecodedImageBudget) usize {
        const working_bytes = std.math.mul(usize, self.used_pixels, read_decoded_working_bytes_per_pixel) catch
            std.math.maxInt(usize);
        const total_bytes = std.math.add(usize, self.base_bytes, working_bytes) catch std.math.maxInt(usize);
        return @max(self.base_units, @max(@as(usize, 1), admissionUnitsFor(total_bytes, read_admission_bytes_per_unit)));
    }
};

fn validateReadMaxTokens(value: ?i64) !?usize {
    const requested = value orelse return null;
    if (requested < 1 or requested > @as(i64, @intCast(max_read_tokens))) return error.InvalidMaxTokens;
    return @intCast(requested);
}

fn readMaxTokensJsonField(obj: std.json.ObjectMap, name: []const u8) !?usize {
    const value = obj.get(name) orelse return null;
    const requested = switch (value) {
        .integer => |number| number,
        .number_string => |raw| std.fmt.parseInt(i64, raw, 10) catch return error.InvalidMaxTokens,
        else => return error.InvalidMaxTokens,
    };
    return validateReadMaxTokens(requested);
}

fn normalizeReadPrompt(prompt: ?[]const u8) ?[]const u8 {
    const value = prompt orelse return null;
    return if (std.mem.trim(u8, value, " \t\r\n").len == 0) null else value;
}

fn estimateReadAdmissionUnits(image_count: usize, max_tokens: ?usize) usize {
    const estimated_max_tokens = max_tokens orelse default_read_admission_max_tokens;
    const token_units = 1 + ((@max(estimated_max_tokens, 1) - 1) / default_read_admission_max_tokens);
    return std.math.mul(usize, @max(image_count, 1), token_units) catch std.math.maxInt(usize);
}

fn addReadBatchDownloadedBytes(current: usize, item: scraping.DownloadedContent, max_bytes: usize) !usize {
    const with_data = std.math.add(usize, current, item.data.len) catch return error.ReadBatchTooLarge;
    const total = std.math.add(usize, with_data, item.content_type.len) catch return error.ReadBatchTooLarge;
    if (total > max_bytes) return error.ReadBatchTooLarge;
    return total;
}

const DecodedDataUri = struct {
    mime_type: ?[]const u8,
    data: []u8,

    fn deinit(self: DecodedDataUri, allocator: std.mem.Allocator) void {
        allocator.free(self.data);
    }
};

fn decodeDataUri(allocator: std.mem.Allocator, uri: []const u8) !DecodedDataUri {
    // Expect: data:<mime>;base64,<data>
    const prefix = "data:";
    if (!std.mem.startsWith(u8, uri, prefix)) return error.InvalidDataUri;

    const b64_marker = ";base64,";
    const marker_pos = std.mem.indexOf(u8, uri, b64_marker) orelse return error.InvalidDataUri;
    const mime_start = prefix.len;
    const mime_raw = uri[mime_start..marker_pos];
    const b64_start = marker_pos + b64_marker.len;
    const b64_data = uri[b64_start..];

    const decoded_len = std.base64.standard.Decoder.calcSizeForSlice(b64_data) catch return error.InvalidBase64;
    const decoded = try allocator.alloc(u8, decoded_len);
    errdefer allocator.free(decoded);

    std.base64.standard.Decoder.decode(decoded, b64_data) catch return error.InvalidBase64;
    return .{
        .mime_type = if (mime_raw.len == 0) null else mime_raw,
        .data = decoded,
    };
}

fn decodeMediaData(allocator: std.mem.Allocator, data: []const u8) !DecodedDataUri {
    if (std.mem.startsWith(u8, data, "data:")) return try decodeDataUri(allocator, data);

    const decoded_len = std.base64.standard.Decoder.calcSizeForSlice(data) catch return error.InvalidBase64;
    const decoded = try allocator.alloc(u8, decoded_len);
    errdefer allocator.free(decoded);
    std.base64.standard.Decoder.decode(decoded, data) catch return error.InvalidBase64;
    return .{
        .mime_type = null,
        .data = decoded,
    };
}

fn decodedMediaDataSize(data: []const u8) !usize {
    const encoded = if (std.mem.startsWith(u8, data, "data:")) blk: {
        const marker = ";base64,";
        const marker_pos = std.mem.indexOf(u8, data, marker) orelse return error.InvalidDataUri;
        break :blk data[marker_pos + marker.len ..];
    } else data;
    return std.base64.standard.Decoder.calcSizeForSlice(encoded) catch error.InvalidBase64;
}

fn encodedMediaBudgetSize(data: []const u8) !usize {
    // Size data URIs independently of their transfer encoding. The scraping
    // layer accepts both base64 and percent-encoded payloads, and both must be
    // admitted by their resident request-body footprint before decoding.
    if (std.mem.startsWith(u8, data, "data:")) {
        const comma = std.mem.indexOfScalar(u8, data, ',') orelse return error.InvalidDataUri;
        return if (comma + 1 == data.len) 0 else data.len;
    }
    return if (data.len == 0) 0 else data.len;
}

fn decodeDataUriWithBudget(
    allocator: std.mem.Allocator,
    uri: []const u8,
    budget: *RequestMediaBudget,
) !DecodedDataUri {
    const encoded_size = try encodedMediaBudgetSize(uri);
    if (encoded_size > budget.remaining()) return error.RemoteContentTooLarge;
    const decoded = try decodeDataUri(allocator, uri);
    errdefer decoded.deinit(allocator);
    try budget.add(encoded_size);
    return decoded;
}

fn decodeMediaDataWithBudget(
    allocator: std.mem.Allocator,
    data: []const u8,
    budget: *RequestMediaBudget,
) !DecodedDataUri {
    const encoded_size = try encodedMediaBudgetSize(data);
    if (encoded_size > budget.remaining()) return error.RemoteContentTooLarge;
    const decoded = try decodeMediaData(allocator, data);
    errdefer decoded.deinit(allocator);
    try budget.add(encoded_size);
    return decoded;
}

fn mediaMimeMatches(declared: ?[]const u8, embedded: ?[]const u8) bool {
    const embedded_mime = embedded orelse return true;
    const declared_mime = declared orelse return true;
    return std.ascii.eqlIgnoreCase(trimMimeParametersLocal(declared_mime), trimMimeParametersLocal(embedded_mime));
}

fn trimMimeParametersLocal(value: []const u8) []const u8 {
    const semi = std.mem.indexOfScalar(u8, value, ';') orelse return std.mem.trim(u8, value, &std.ascii.whitespace);
    return std.mem.trim(u8, value[0..semi], &std.ascii.whitespace);
}

fn unsupportedAudioResponse(ctx: *httpx.Context, message: []const u8) !httpx.Response {
    return ctx.status(400).json(.{
        .@"error" = "UNSUPPORTED",
        .message = message,
    });
}

fn audioTooLargeResponse(ctx: *httpx.Context) !httpx.Response {
    return ctx.status(413).json(.{
        .@"error" = "AUDIO_TOO_LARGE",
        .message = "decoded audio exceeds the server working-memory limit",
        .retryable = false,
    });
}

fn coerceGenerateResponseFormat(
    allocator: std.mem.Allocator,
    response_format: ?api.GenerateResponseFormat,
    json_text: []const u8,
) !?[]u8 {
    const rf = response_format orelse return null;
    if (std.mem.eql(u8, rf.type, "json_object")) {
        var parsed = std.json.parseFromSlice(std.json.Value, allocator, json_text, .{}) catch
            return error.InvalidStructuredOutput;
        defer parsed.deinit();
        if (parsed.value == .object) return null;
        return error.InvalidStructuredOutput;
    }
    if (!std.mem.eql(u8, rf.type, "json_schema")) return null;

    const schema_cfg = rf.json_schema orelse return error.MissingJsonSchema;
    validateGeneratedJsonSchema(allocator, json_text, schema_cfg) catch |err| {
        if (err == error.OutOfMemory) return err;
        return error.InvalidStructuredOutput;
    };
    return null;
}

fn validateGeneratedJsonSchema(
    allocator: std.mem.Allocator,
    json_text: []const u8,
    schema_cfg: api.GenerateJsonSchemaConfig,
) !void {
    const schema = schema_cfg.schema orelse return error.MissingJsonSchema;
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, json_text, .{});
    defer parsed.deinit();
    try jsonschema.validateJsonSchemaValue(allocator, .{ .object = schema.map }, parsed.value);
}

test "structured output validation fails closed instead of fabricating JSON" {
    const allocator = std.testing.allocator;
    try std.testing.expectError(
        error.InvalidStructuredOutput,
        coerceGenerateResponseFormat(
            allocator,
            .{ .type = "json_object" },
            "not json",
        ),
    );
    try std.testing.expectError(
        error.InvalidStructuredOutput,
        coerceGenerateResponseFormat(
            allocator,
            .{ .type = "json_object" },
            "[]",
        ),
    );

    var parsed_schema = try std.json.parseFromSlice(
        std.json.ArrayHashMap(std.json.Value),
        allocator,
        "{\"type\":\"object\",\"required\":[\"answer\"],\"properties\":{\"answer\":{\"type\":\"string\"}}}",
        .{},
    );
    defer parsed_schema.deinit();
    try std.testing.expectError(
        error.InvalidStructuredOutput,
        coerceGenerateResponseFormat(
            allocator,
            .{
                .type = "json_schema",
                .json_schema = .{ .schema = parsed_schema.value },
            },
            "{}",
        ),
    );
    try std.testing.expectEqual(
        @as(?[]u8, null),
        try coerceGenerateResponseFormat(
            allocator,
            .{
                .type = "json_schema",
                .json_schema = .{ .schema = parsed_schema.value },
            },
            "{\"answer\":\"ok\"}",
        ),
    );
}

test "admission rejection includes Retry-After" {
    const allocator = std.testing.allocator;
    var node = try Node.init(allocator, .{ .max_concurrent_requests = 1 });
    defer node.deinit();

    try node.inference_admission.acquire();
    defer node.inference_admission.release();

    var request = try httpx.Request.init(allocator, .GET, "/ai/v1/models");
    defer request.deinit();
    var ctx = httpx.Context.init(allocator, std.testing.io, &request);
    defer ctx.deinit();

    var response = (try node.acquireSlot(&ctx)) orelse return error.TestExpectedAdmissionRejection;
    defer response.deinit();
    try std.testing.expectEqual(@as(u16, 503), response.status.code);
    try std.testing.expectEqualStrings("1", response.headers.get("Retry-After").?);
    var payload = try std.json.parseFromSlice(
        struct { retryable: bool },
        allocator,
        response.body.?,
        .{ .ignore_unknown_fields = true },
    );
    defer payload.deinit();
    try std.testing.expect(payload.value.retryable);
}

test "route-owned request admission rejects before endpoint work" {
    const allocator = std.testing.allocator;
    var node = try Node.init(allocator, .{ .max_concurrent_requests = 1 });
    defer node.deinit();

    try node.inference_admission.acquireRequestSlot();
    defer node.inference_admission.releaseRequestSlot();

    const Endpoint = struct {
        fn run(_: *Node, ctx: *httpx.Context) anyerror!httpx.Response {
            return ctx.status(204).text("");
        }
    };
    const handler = httpx.Handler.wrap(
        &node,
        httpx.Handler.bind(&node, Endpoint.run),
        admittedInferenceHandler,
    );
    var request = try httpx.Request.init(allocator, .POST, "/ai/v1/test");
    defer request.deinit();
    var ctx = httpx.Context.init(allocator, std.testing.io, &request);
    defer ctx.deinit();

    var response = try handler.invoke(&ctx);
    defer response.deinit();
    try std.testing.expectEqual(@as(u16, 503), response.status.code);
    try std.testing.expectEqualStrings("1", response.headers.get("Retry-After").?);
    try std.testing.expectEqual(@as(usize, 1), node.inference_admission.inFlightRequests());
    try std.testing.expectEqual(@as(usize, 0), node.inference_admission.inFlightUnits());
}

test "admission rejection metrics retain unclamped requested units" {
    const allocator = std.testing.allocator;
    var node = try Node.init(allocator, .{ .max_concurrent_requests = 2 });
    defer node.deinit();

    try node.acquireAdmissionUnits(2);
    defer node.releaseAdmissionUnits(2);
    try std.testing.expectError(error.QueueFull, node.acquireAdmissionUnits(9));

    var writer: std.Io.Writer.Allocating = .init(allocator);
    defer writer.deinit();
    try node.metrics.render(&writer.writer);
    try std.testing.expect(std.mem.indexOf(
        u8,
        writer.writer.buffered(),
        "antfly_admission_inference_rejected_units_total 9\n",
    ) != null);
}

test "structured extract maps weighted admission exhaustion to retryable capacity" {
    const allocator = std.testing.allocator;
    var node = try Node.init(allocator, .{ .max_concurrent_requests = 1 });
    defer node.deinit();

    try node.inference_admission.reserveUnits(1);
    defer node.inference_admission.releaseReservedUnits(1);

    var request = try httpx.Request.init(allocator, .POST, "/ai/v1/extract");
    defer request.deinit();
    request.body =
        \\{"model":"demo","inputs":[{"content":"hello"}],"schema":{"structures":{"answer":{"fields":{"value":"string"}}}}}
    ;
    var ctx = httpx.Context.init(allocator, std.testing.io, &request);
    defer ctx.deinit();

    var response = try node.extract(&ctx);
    defer response.deinit();
    try std.testing.expectEqual(@as(u16, 503), response.status.code);
    try std.testing.expectEqualStrings("1", response.headers.get("Retry-After").?);
    try std.testing.expect(std.mem.indexOf(u8, response.body.?, "\"reason\":\"inference_admission\"") != null);
}

test "predict endpoint shares inference request admission" {
    const allocator = std.testing.allocator;
    var node = try Node.init(allocator, .{ .max_concurrent_requests = 1 });
    defer node.deinit();

    try node.inference_admission.acquire();
    defer node.inference_admission.release();

    var request = try httpx.Request.init(allocator, .POST, "/ml/v1/predict");
    defer request.deinit();
    request.body = "{\"model\":\"demo\",\"input\":[[1.0]]}";
    var ctx = httpx.Context.init(allocator, std.testing.io, &request);
    defer ctx.deinit();

    var response = try node.predict(&ctx);
    defer response.deinit();
    try std.testing.expectEqual(@as(u16, 503), response.status.code);
    try std.testing.expectEqualStrings("1", response.headers.get("Retry-After").?);
}

test "shared json schema validator: additionalProperties schema object" {
    const allocator = std.testing.allocator;
    var schema_parsed = try std.json.parseFromSlice(std.json.Value, allocator,
        \\{
        \\  "type": "object",
        \\  "properties": { "name": { "type": "string" } },
        \\  "additionalProperties": { "type": "integer", "minimum": 1 }
        \\}
    , .{});
    defer schema_parsed.deinit();

    var value_parsed = try std.json.parseFromSlice(std.json.Value, allocator,
        \\{"name":"ok","score":2}
    , .{});
    defer value_parsed.deinit();

    try jsonschema.validateJsonSchemaValue(allocator, schema_parsed.value, value_parsed.value);
}

test "shared json schema validator: combinators and bounds" {
    const allocator = std.testing.allocator;
    var schema_parsed = try std.json.parseFromSlice(std.json.Value, allocator,
        \\{
        \\  "allOf": [{ "type": "integer", "minimum": 2 }],
        \\  "anyOf": [{ "const": 3 }, { "const": 4 }],
        \\  "oneOf": [{ "const": 3 }, { "const": 5 }]
        \\}
    , .{});
    defer schema_parsed.deinit();

    try jsonschema.validateJsonSchemaValue(allocator, schema_parsed.value, .{ .integer = 3 });
}

test "shared json schema validator: array bounds" {
    const allocator = std.testing.allocator;
    var schema_parsed = try std.json.parseFromSlice(std.json.Value, allocator,
        \\{
        \\  "type": "array",
        \\  "items": { "type": "integer" },
        \\  "minItems": 1,
        \\  "maxItems": 2
        \\}
    , .{});
    defer schema_parsed.deinit();

    var value_parsed = try std.json.parseFromSlice(std.json.Value, allocator,
        \\[1,2]
    , .{});
    defer value_parsed.deinit();

    try jsonschema.validateJsonSchemaValue(allocator, schema_parsed.value, value_parsed.value);
}

fn appendIntJson(buf: *std.ArrayListUnmanaged(u8), allocator: std.mem.Allocator, value: anytype) !void {
    const num = try std.fmt.allocPrint(allocator, "{d}", .{value});
    defer allocator.free(num);
    try buf.appendSlice(allocator, num);
}

fn appendBase64Json(buf: *std.ArrayListUnmanaged(u8), allocator: std.mem.Allocator, value: []const u8) !void {
    const encoded_len = std.base64.standard.Encoder.calcSize(value.len);
    const encoded = try allocator.alloc(u8, encoded_len);
    defer allocator.free(encoded);
    _ = std.base64.standard.Encoder.encode(encoded, value);
    try jsonEncodeString(buf, allocator, encoded);
}

fn graphModeEnabled() bool {
    if (comptime @import("builtin").os.tag == .freestanding) return false;
    return platform.env.getenvBool("TERMITE_GRAPH_MODE");
}
