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

// Metal-native provider for termite's decoder runtime.
//
// Mirrors the Metal-native portion of `MetalProvider` (metal_provider.zig)
// without any `c.backend_array`-typed methods. Used by
// `MetalCompute` when the build has `-Dmetal=true`. The fields
// are laid out to match what `metal_runtime.zig` duck-types on `self` so the
// same helper functions work for both provider variants.

const std = @import("std");
const build_options = @import("build_options");
const metal_runtime = @import("metal_runtime.zig");
const kernel_jit = @import("../graph/kernel_jit.zig");
const metal_tensor = @import("metal_tensor.zig");
const weight_source_mod = @import("../models/weight_source.zig");

const MetalTensor = metal_tensor.MetalTensor;
const QuantizedStorage = weight_source_mod.QuantizedStorage;
const RawMetalProvider = metal_runtime.RawMetalProvider;
const RawMetalDecodeRuntime = metal_runtime.RawMetalDecodeRuntime;
const decoder_runtime_layer_norm_slot_capacity = metal_runtime.decoder_runtime_layer_norm_slot_capacity;
const decoder_runtime_rms_norm_slot_capacity = metal_runtime.decoder_runtime_rms_norm_slot_capacity;
const decoder_runtime_linear_slot_capacity = metal_runtime.decoder_runtime_linear_slot_capacity;
const RawLinearSlotKind = metal_runtime.RawLinearSlotKind;
const RawQuantizedRuntimeLinearKind = metal_runtime.RawQuantizedRuntimeLinearKind;
const RawQuantizedRuntimeLinearStorageMode = metal_runtime.RawQuantizedRuntimeLinearStorageMode;
const GatheredSpanKey = metal_runtime.GatheredSpanKey;
const GatheredSpanEntry = metal_runtime.GatheredSpanEntry;
// One primary pipeline plus an optional homogeneous QKV companion per winner.
pub const exact_jit_pipeline_owner_capacity: usize = 2 * metal_runtime.workload_tuning_maximum_winners;

pub const DebertaRelativeProjectionCacheEntry = struct {
    q: ?MetalTensor = null,
    k: ?MetalTensor = null,
    k_slot: usize = 0,
    seq_len: usize = 0,
    relative_count: usize = 0,
    hidden_size: usize = 0,
    owner_token: usize = 0,
    valid: bool = false,

    pub fn deinit(self: *DebertaRelativeProjectionCacheEntry) void {
        if (self.q) |*tensor| tensor.deinit();
        if (self.k) |*tensor| tensor.deinit();
        self.* = .{};
    }
};

pub const DebertaRelativeEmbeddingCacheEntry = struct {
    tensor: ?MetalTensor = null,
    bucket_ids_hash: u64 = 0,
    bucket_count: usize = 0,
    hidden_size: usize = 0,
    norm_eps_bits: u32 = 0,
    owner_token: usize = 0,
    valid: bool = false,

    pub fn deinit(self: *DebertaRelativeEmbeddingCacheEntry) void {
        if (self.tensor) |*tensor| tensor.deinit();
        self.* = .{};
    }
};
comptime {
    if (exact_jit_pipeline_owner_capacity < metal_runtime.workload_tuning_maximum_winners) {
        @compileError("Metal exact JIT owner capacity must cover every per-regime workload winner");
    }
}

pub const MetalNativeProvider = if (build_options.enable_metal) struct {
    raw_provider: ?*RawMetalProvider,
    raw_decode_runtime: ?*RawMetalDecodeRuntime,
    jit_mode: kernel_jit.Mode = .off,
    jit_scope: metal_runtime.MetalJitRouteScope = metal_runtime.MetalJitRouteScope.none(),
    jit_session: ?*metal_runtime.MetalJitSession = null,
    jit_pipeline_owners: metal_runtime.MetalJitPipelineOwners = metal_runtime.empty_metal_jit_pipeline_owners,
    jit_exact_pipeline_owners: [exact_jit_pipeline_owner_capacity]?*metal_runtime.RawMetalGeneratedPipeline =
        @as([exact_jit_pipeline_owner_capacity]?*metal_runtime.RawMetalGeneratedPipeline, @splat(null)),
    jit_exact_pipeline_owner_count: usize = 0,
    jit_artifact_keys: metal_runtime.MetalJitArtifactKeys = metal_runtime.empty_metal_jit_artifact_keys,
    jit_route_states: metal_runtime.MetalJitRouteStates = metal_runtime.empty_metal_jit_route_states,
    jit_qualified_routes: metal_runtime.MetalJitQualifiedRoutes = metal_runtime.empty_metal_jit_qualified_routes,
    raw_decoder_family_prepared: bool = false,
    raw_decoder_prepared_kv_tokens: usize = 0,
    raw_absolute_embeddings_prepared: bool = false,
    raw_absolute_embeddings_vocab_size: usize = 0,
    raw_absolute_embeddings_position_count: usize = 0,
    raw_absolute_embeddings_hidden_size: usize = 0,
    raw_layer_norm_slots_prepared: [decoder_runtime_layer_norm_slot_capacity]bool = @as([decoder_runtime_layer_norm_slot_capacity]bool, @splat(false)),
    raw_layer_norm_slot_hidden_sizes: [decoder_runtime_layer_norm_slot_capacity]usize = @as([decoder_runtime_layer_norm_slot_capacity]usize, @splat(0)),
    raw_layer_norm_slot_weights: [decoder_runtime_layer_norm_slot_capacity]?MetalTensor = @as([decoder_runtime_layer_norm_slot_capacity]?MetalTensor, @splat(null)),
    raw_layer_norm_slot_biases: [decoder_runtime_layer_norm_slot_capacity]?MetalTensor = @as([decoder_runtime_layer_norm_slot_capacity]?MetalTensor, @splat(null)),
    raw_rms_norm_slots_prepared: [decoder_runtime_rms_norm_slot_capacity]bool = @as([decoder_runtime_rms_norm_slot_capacity]bool, @splat(false)),
    raw_rms_norm_slot_hidden_sizes: [decoder_runtime_rms_norm_slot_capacity]usize = @as([decoder_runtime_rms_norm_slot_capacity]usize, @splat(0)),
    raw_rms_norm_slot_weights: [decoder_runtime_rms_norm_slot_capacity]?MetalTensor = @as([decoder_runtime_rms_norm_slot_capacity]?MetalTensor, @splat(null)),
    raw_linear_slots_prepared: [decoder_runtime_linear_slot_capacity]bool = @as([decoder_runtime_linear_slot_capacity]bool, @splat(false)),
    raw_linear_slot_kinds: [decoder_runtime_linear_slot_capacity]RawLinearSlotKind = @as([decoder_runtime_linear_slot_capacity]RawLinearSlotKind, @splat(.none)),
    raw_linear_slot_in_dims: [decoder_runtime_linear_slot_capacity]usize = @as([decoder_runtime_linear_slot_capacity]usize, @splat(0)),
    raw_linear_slot_out_dims: [decoder_runtime_linear_slot_capacity]usize = @as([decoder_runtime_linear_slot_capacity]usize, @splat(0)),
    raw_linear_slot_quantized_storage: [decoder_runtime_linear_slot_capacity]?*QuantizedStorage = @as([decoder_runtime_linear_slot_capacity]?*QuantizedStorage, @splat(null)),
    raw_linear_slot_dense_weights: [decoder_runtime_linear_slot_capacity]?MetalTensor = @as([decoder_runtime_linear_slot_capacity]?MetalTensor, @splat(null)),
    raw_linear_slot_dense_biases: [decoder_runtime_linear_slot_capacity]?MetalTensor = @as([decoder_runtime_linear_slot_capacity]?MetalTensor, @splat(null)),
    raw_linear_slot_bias_is_zero: [decoder_runtime_linear_slot_capacity]bool = @as([decoder_runtime_linear_slot_capacity]bool, @splat(false)),
    raw_linear_slot_runtime_prepared_kind: [decoder_runtime_linear_slot_capacity]RawQuantizedRuntimeLinearKind = @as([decoder_runtime_linear_slot_capacity]RawQuantizedRuntimeLinearKind, @splat(.none)),
    raw_linear_slot_runtime_prepared_modes: [decoder_runtime_linear_slot_capacity]RawQuantizedRuntimeLinearStorageMode = @as([decoder_runtime_linear_slot_capacity]RawQuantizedRuntimeLinearStorageMode, @splat(.none)),
    raw_linear_slot_disable_mapped_quant_weight: [decoder_runtime_linear_slot_capacity]bool = @as([decoder_runtime_linear_slot_capacity]bool, @splat(false)),
    /// Main transformed lm-head slot -> checkpoint-format correction slot.
    raw_linear_slot_lm_head_refine_slots: [decoder_runtime_linear_slot_capacity]?usize = @as([decoder_runtime_linear_slot_capacity]?usize, @splat(null)),
    raw_lm_head_q4_resident_sampling_rejections: u64 = 0,
    raw_model_mapped_buffer_prepared: bool = false,
    raw_linear_slot_model_buffer_bound: [decoder_runtime_linear_slot_capacity]bool = @as([decoder_runtime_linear_slot_capacity]bool, @splat(false)),
    raw_quant_runtime_private_prepare_nanos: u128 = 0,
    raw_quant_runtime_mapped_prepare_nanos: u128 = 0,
    raw_quant_runtime_mapped_attempts: u64 = 0,
    raw_quant_runtime_mapped_fallbacks: u64 = 0,
    raw_quant_runtime_mapped_failures: u64 = 0,
    gathered_spans: std.AutoHashMapUnmanaged(GatheredSpanKey, GatheredSpanEntry) = .empty,
    deberta_relative_projection_cache_mutex: std.atomic.Mutex = .unlocked,
    deberta_relative_projection_cache: [decoder_runtime_linear_slot_capacity]DebertaRelativeProjectionCacheEntry =
        @as([decoder_runtime_linear_slot_capacity]DebertaRelativeProjectionCacheEntry, @splat(.{})),
    deberta_relative_embedding_cache: DebertaRelativeEmbeddingCacheEntry = .{},

    pub fn create() !MetalNativeProvider {
        return createWithKernelJit(.{});
    }

    pub fn createWithKernelJit(config: kernel_jit.Config) !MetalNativeProvider {
        return createWithKernelJitOptions(.{ .config = config });
    }

    pub fn createWithKernelJitOptions(options: metal_runtime.MetalJitOptions) !MetalNativeProvider {
        const raw_provider = metal_runtime.termite_metal_provider_create();
        const raw_decode_runtime = metal_runtime.termite_metal_decode_runtime_create();
        var result = MetalNativeProvider{
            .raw_provider = raw_provider,
            .raw_decode_runtime = raw_decode_runtime,
        };
        errdefer result.deinitOwned();
        // Non-generative Metal graphs (embedding, vision, and audio encoders)
        // have no later regime transition hook. Generation explicitly switches
        // to prefill/decode/speculative regimes before dispatching its work.
        try result.workloadProfileSetRegime(.encoder);
        // Compilation, qualification lookup, and every slot install finish
        // before publication. Routes not admitted before the preload budget
        // expires keep their bundled implementation; no GPU benchmark runs
        // against a published provider.
        try metal_runtime.initializeMetalKernelJitWithOptions(&result, options);
        return result;
    }

    pub fn hasDecoderRuntime(self: *const MetalNativeProvider) bool {
        const runtime = self.raw_decode_runtime orelse return false;
        return metal_runtime.termite_metal_decode_runtime_ready(runtime) != 0;
    }

    pub fn reserveDecoderRuntime(self: *MetalNativeProvider, scratch_bytes: usize, token_bytes: usize) !bool {
        const runtime = self.raw_decode_runtime orelse return false;
        return metal_runtime.termite_metal_decode_runtime_reserve(runtime, scratch_bytes, token_bytes) == 0;
    }

    pub fn workloadProfileBegin(self: *MetalNativeProvider, regime: metal_runtime.WorkloadRegime) !void {
        try metal_runtime.workloadProfileBegin(self.raw_decode_runtime, regime);
    }

    pub fn workloadProfileSetRegime(self: *MetalNativeProvider, regime: metal_runtime.WorkloadRegime) !void {
        if (self.raw_decode_runtime == null) return;
        try metal_runtime.workloadProfileSetRegime(self.raw_decode_runtime, regime);
    }

    pub fn workloadProfileEnd(self: *MetalNativeProvider) !void {
        try metal_runtime.workloadProfileEnd(self.raw_decode_runtime);
    }

    pub fn workloadProfileSnapshot(self: *const MetalNativeProvider) !metal_runtime.RawWorkloadProfileSnapshot {
        return metal_runtime.workloadProfileSnapshot(self.raw_decode_runtime);
    }

    /// Takes ownership after an exact-signature pipeline was installed into
    /// the Metal provider and decode runtime. Both retain the pipeline state; keeping
    /// the generated owner also preserves the compiled library for its full
    /// published lifetime.
    pub fn canOwnExactJitPipeline(self: *const MetalNativeProvider) bool {
        return self.jit_exact_pipeline_owner_count < self.jit_exact_pipeline_owners.len;
    }

    pub fn ownExactJitPipeline(self: *MetalNativeProvider, generated: *metal_runtime.RawMetalGeneratedPipeline) void {
        std.debug.assert(self.canOwnExactJitPipeline());
        self.jit_exact_pipeline_owners[self.jit_exact_pipeline_owner_count] = generated;
        self.jit_exact_pipeline_owner_count += 1;
    }

    /// Releases every owned resource. The provider is consumed and must not be
    /// used or deinitialized again.
    pub fn deinitOwned(self: *MetalNativeProvider) void {
        // Release the synchronous JIT session before provider, cache, or
        // pipeline dependencies are released.
        metal_runtime.destroyMetalJitSession(self.jit_session);
        metal_runtime.flushActiveFrame(self.raw_decode_runtime) catch {};
        if (metal_runtime.hasActiveFrame(self.raw_decode_runtime)) {
            metal_runtime.waitFrame(self.raw_decode_runtime) catch {};
        }
        metal_runtime.resetGatheredSpans(self);
        self.deberta_relative_embedding_cache.deinit();
        for (&self.deberta_relative_projection_cache) |*entry| entry.deinit();
        for (0..decoder_runtime_linear_slot_capacity) |slot| metal_runtime.releaseRawLinearSlot(self, slot);
        for (0..decoder_runtime_layer_norm_slot_capacity) |slot| metal_runtime.releaseRawLayerNormSlot(self, slot);
        for (0..decoder_runtime_rms_norm_slot_capacity) |slot| metal_runtime.releaseRawRmsNormSlot(self, slot);
        metal_runtime.termite_metal_provider_destroy(self.raw_provider);
        metal_runtime.termite_metal_decode_runtime_destroy(self.raw_decode_runtime);
        for (&self.jit_pipeline_owners) |*generated| metal_runtime.termite_metal_generated_pipeline_destroy(generated.*);
        for (&self.jit_exact_pipeline_owners) |*generated| metal_runtime.termite_metal_generated_pipeline_destroy(generated.*);
    }
} else void;
