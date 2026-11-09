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

//! Decoder route and output-placement policy. Tasks supply semantic request
//! facts; this owner selects physical schedules without task configuration,
//! tokenizers, streaming callbacks or pipeline ownership.
const std = @import("std");
const platform = @import("antfly_platform");
const ops = @import("../ops/ops.zig");
const runtime = @import("../runtime/root.zig");
const gpt_mod = @import("../models/gpt.zig");
const gpt_arch = @import("../architectures/gpt.zig");
const gemma4_runtime = @import("../architectures/gemma4_runtime.zig");

pub const Request = struct {
    storage: @import("decoder_storage.zig").Request,
    has_token_observer: bool,
    exact_hybrid: bool,
};
pub const PrefillOutputIntent = enum {
    kv_only,
    last_logits,
    greedy_token,
    last_logits_and_hidden,
    greedy_token_and_hidden,
};

pub fn ordinaryMetalGemmaDraftNeedsContiguousState(
    backend: ops.BackendKind,
    config: gpt_mod.Config,
    has_paged_kv_state: bool,
) bool {
    return backend == .metal and
        config.family == .gemma and
        !config.gemma4_mtp_assistant and
        !has_paged_kv_state;
}

pub fn supportsSpeculativeTargetVerification(
    backend: ops.BackendKind,
    target_config: gpt_mod.Config,
) bool {
    // Qualified A4B Metal decode is qLen=1-only. Keep speculative decoding out
    // of the request before either target or draft state is mutated; the
    // reserve-time check remains as a fail-closed backstop.
    return backend != .metal or
        !gemma4_runtime.isQualifiedA4bArchitecture(target_config);
}

pub fn shouldUseCudaPendingTokenReadback(
    backend_kind: ops.BackendKind,
    enabled: bool,
    pure_greedy: bool,
    use_speculative: bool,
    has_grammar: bool,
    graph_runtime_present: bool,
    execution_lock_present: bool,
    max_tokens: usize,
) bool {
    return enabled and
        backend_kind == .cuda and
        pure_greedy and
        !use_speculative and
        !has_grammar and
        !graph_runtime_present and
        !execution_lock_present and
        max_tokens >= 2;
}

pub fn requiresCudaDecodeGraphBeforeEagerFallback(
    backend_kind: ops.BackendKind,
    graph_replay_required: bool,
    pure_greedy: bool,
    query_seq_len: usize,
    has_external_graph_runtime: bool,
    has_grammar: bool,
) bool {
    return graph_replay_required and
        backend_kind == .cuda and
        pure_greedy and
        query_seq_len == 1 and
        !has_external_graph_runtime and
        !has_grammar;
}

pub fn prefillOutputIntent(is_final_chunk: bool, use_greedy_token: bool, capture_last_hidden: bool) PrefillOutputIntent {
    if (!is_final_chunk) return .kv_only;
    if (use_greedy_token) return if (capture_last_hidden) .greedy_token_and_hidden else .greedy_token;
    return if (capture_last_hidden) .last_logits_and_hidden else .last_logits;
}

pub fn shouldUsePrefillFirstTokenPath(backend_kind: ops.BackendKind, pure_greedy: bool, use_speculative: bool, enabled: bool) bool {
    return enabled and backend_kind == .cuda and pure_greedy and !use_speculative;
}

pub fn directPrefillExecutionMutex(
    prepared_multimodal: bool,
    use_cuda_prefill_greedy_token: bool,
    use_metal_prefill_greedy_token: bool,
    execution_lock: ?*std.atomic.Mutex,
) ?*std.atomic.Mutex {
    return if (prepared_multimodal or use_cuda_prefill_greedy_token or use_metal_prefill_greedy_token) execution_lock else null;
}

pub fn prefillFinalChunkNeedsDirectExecution(
    total_chunk_end: usize,
    seq_len: usize,
    output_intent: PrefillOutputIntent,
    prefer_cuda_direct_last_logits: bool,
    use_metal_target_last_logits: bool,
) bool {
    return total_chunk_end == seq_len and
        switch (output_intent) {
            .kv_only => false,
            .greedy_token, .greedy_token_and_hidden, .last_logits_and_hidden => true,
            .last_logits => prefer_cuda_direct_last_logits or use_metal_target_last_logits,
        };
}

pub fn shouldUseMetalGemmaTargetLastLogits(
    kind: ops.BackendKind,
    config: *const gpt_mod.Config,
    capture_last_hidden: bool,
) bool {
    return !capture_last_hidden and
        kind == .metal and
        config.family == .gemma and
        config.hasPle() and
        !config.gemma4_mtp_assistant;
}

pub fn batchKvMutationMutex(
    execution_lock: ?*std.atomic.Mutex,
    kv_lock: ?*std.atomic.Mutex,
) ?*std.atomic.Mutex {
    return if (kv_lock != null) execution_lock else null;
}

pub fn coalescedPrefillChunkSizeForFirstToken(
    backend_kind: ops.BackendKind,
    pure_greedy: bool,
    use_speculative: bool,
    enabled: bool,
    seq_len: usize,
    current_chunk_size: usize,
    coalesce_token_limit: usize,
) usize {
    if (!shouldUsePrefillFirstTokenPath(backend_kind, pure_greedy, use_speculative, enabled)) return current_chunk_size;
    if (coalesce_token_limit == 0 or seq_len > coalesce_token_limit) return current_chunk_size;
    return @max(current_chunk_size, seq_len);
}

pub fn schedulerChunkForPrefillIteration(scheduler_chunk: usize, current_chunk_size: usize, first_token_coalesced: bool) usize {
    if (first_token_coalesced) return current_chunk_size;
    if (scheduler_chunk > 0) return scheduler_chunk;
    return current_chunk_size;
}

pub fn wholeModelPrefillChunkSize(configured_chunk: usize, scheduler_chunk: usize, prompt_tokens: usize) usize {
    const requested = if (configured_chunk > 0)
        configured_chunk
    else
        runtime.tier.memory.generation_default_prefill_chunk_tokens;
    return @max(@min(@min(requested, if (scheduler_chunk > 0) scheduler_chunk else requested), prompt_tokens), 1);
}

pub fn wholeModelExecutionPrefillChunkSize(
    backend_kind: ?ops.BackendKind,
    configured_chunk: usize,
    scheduler_chunk: usize,
    prompt_tokens: usize,
    bounded_swa_prefill: bool,
) usize {
    // Only the Metal whole-model runtime supports incremental prefill today.
    // PJRT resets its runtime on every prefill call, so splitting a
    // prompt would discard the KV state produced by the preceding chunk.
    if (backend_kind != .metal) return @max(prompt_tokens, 1);
    if (!bounded_swa_prefill and configured_chunk == 0) return @max(prompt_tokens, 1);
    return wholeModelPrefillChunkSize(configured_chunk, scheduler_chunk, prompt_tokens);
}

pub fn cudaE4bPrefillKvCapacity(config: gpt_mod.Config, prompt_tokens: usize, output_tokens: i32) ?usize {
    if (config.family != .gemma or config.hidden_size != 2560 or config.num_hidden_layers != 42 or
        config.num_kv_shared_layers != 18 or config.num_attention_heads != 8 or
        config.num_key_value_heads != 2 or config.attention_head_dim != 256 or config.global_head_dim != 512 or
        config.gemma4_mtp_assistant or config.usesMoe() or config.isMultimodal() or
        prompt_tokens <= 2048 or output_tokens <= 0) return null;
    const tokens = std.math.add(usize, prompt_tokens, @intCast(output_tokens)) catch return null;
    if (tokens > @min(@as(usize, 32768), config.max_position_embeddings)) return null;
    const reserved_tokens = prompt_tokens + @min(@as(usize, @intCast(output_tokens)), 256);
    return std.mem.alignForward(usize, reserved_tokens, 16);
}

pub fn compiledSampleRequestEligible(
    request: Request,
    has_grammar: bool,
    has_suppress_token_ids: bool,
) bool {
    return !request.storage.pure_greedy and
        !has_grammar and
        !request.storage.grammar and
        !has_suppress_token_ids;
}

pub fn shouldUseCudaPrefillGreedyToken(
    backend_kind: ops.BackendKind,
    request: Request,
    allow_resident_greedy_token: bool,
    has_suppress_token_ids: bool,
    enabled: bool,
) bool {
    _ = has_suppress_token_ids;
    return enabled and
        allow_resident_greedy_token and
        backend_kind == .cuda and
        request.storage.pure_greedy and
        !request.storage.grammar;
}

pub fn shouldUsePreparedQwenPrefillGreedyToken(backend: ops.BackendKind, model: gpt_mod.Config, request: Request, allow: bool, needs_host_logits: bool, enabled: bool) bool {
    return !needs_host_logits and model.hasQwen35VisionArchitectureContract() and
        !request.has_token_observer and request.exact_hybrid and !request.storage.compaction and
        gpt_arch.canUseFastGreedyArgmaxForConfig(model) and
        shouldUseCudaPrefillGreedyToken(backend, request, allow, model.suppressTokenIds().len != 0, enabled);
}

pub const default_positioned_prefill_items: usize = 8;
pub const max_positioned_prefill_items: usize = 16;
// The qualified serial prefill pack pool covers at most 4096 rows.
// Keep larger page counts within that complete transient lifetime bound.
pub const max_positioned_prefill_rows: usize = 4096;

pub fn positionedPrefillItemLimit(backend: ops.BackendKind, wide: bool) usize {
    return if (backend == .cuda and wide) max_positioned_prefill_items else default_positioned_prefill_items;
}

pub fn positionedPrefillBatchLimit(configured: usize, seq: usize) !usize {
    return positionedPrefillGeometryLimit(configured, seq, false);
}

pub fn positionedPrefillBatchLimitForBackend(kind: ops.BackendKind, configured: usize, seq: usize) !usize {
    return positionedPrefillGeometryLimit(configured, seq, kind == .cuda and platform.env.getenvBool("ANTFLY_LIGHTONOCR3_CUDA_WIDE_MICROBATCHES"));
}

pub fn positionedPrefillGeometryLimit(configured: usize, seq: usize, wide: bool) !usize {
    const capacity = if (wide) max_positioned_prefill_items else default_positioned_prefill_items;
    const rows = if (wide) 8192 else max_positioned_prefill_rows;
    if (configured == 0 or configured > capacity or seq == 0) return error.InvalidBatchDecodeState;
    return @min(configured, @max(@as(usize, 1), rows / seq));
}

pub fn positionedPrefillHostScratch(items: usize, seq: usize, vocab: usize) !usize {
    if (items > default_positioned_prefill_items) return error.InvalidBatchDecodeState;
    return positionedPrefillHostScratchCapacity(items, seq, vocab);
}

pub fn positionedPrefillHostScratchForBackend(kind: ops.BackendKind, items: usize, seq: usize, vocab: usize) !usize {
    if (kind == .cuda and platform.env.getenvBool("ANTFLY_LIGHTONOCR3_CUDA_WIDE_MICROBATCHES"))
        return positionedPrefillHostScratchCapacity(items, seq, vocab);
    return positionedPrefillHostScratch(items, seq, vocab);
}

pub fn positionedPrefillHostScratchCapacity(items: usize, seq: usize, vocab: usize) !usize {
    if (items == 0 or items > max_positioned_prefill_items or seq == 0 or vocab == 0) return error.InvalidBatchDecodeState;
    const logits = std.math.mul(usize, items, vocab) catch return error.ResourceLimitExceeded;
    const logits_bytes = std.math.mul(usize, logits, 2 * @sizeOf(f32)) catch return error.ResourceLimitExceeded;
    const positions = std.math.mul(usize, items, seq) catch return error.ResourceLimitExceeded;
    const position_bytes = std.math.mul(usize, positions, 3 * @sizeOf(u32)) catch return error.ResourceLimitExceeded;
    const arrays = std.math.add(usize, logits_bytes, position_bytes) catch return error.ResourceLimitExceeded;
    return std.math.add(usize, arrays, 32 * 1024) catch error.ResourceLimitExceeded;
}

pub fn cudaPreparedPrefillGreedyEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_PREFILL_PREPARED_TAIL_GREEDY", false);
}

pub fn metalPreparedPrefillHiddenCaptureEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_GEMMA4_MTP_ENABLE_METAL_PREFILL_HIDDEN_CAPTURE", false) and
        !platform.env.getenvBool("ANTFLY_GEMMA4_MTP_DISABLE_METAL_PREFILL_HIDDEN_CAPTURE");
}
