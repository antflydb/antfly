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
const memory = @import("../runtime/tier/memory.zig");

const MetalTensor = metal_tensor.MetalTensor;
const QuantizedStorage = weight_source_mod.QuantizedStorage;
const RawMetalProvider = metal_runtime.RawMetalProvider;
const RawMetalDecodeRuntime = metal_runtime.RawMetalDecodeRuntime;
const tier_cache_mod = @import("../runtime/tier/cache.zig");
const prepared_slot = @import("prepared_slot_owner.zig");
pub const PreparedConstant = struct { tensor: MetalTensor, bytes: usize };
pub const EmbeddingGemma2LinearCacheEntry = struct {
    owner: ?*prepared_slot.Owner = null,
    projection_widths: [3]usize = @splat(0),
    slot: usize,
    in_dim: usize,
    out_dim: usize,
    bytes: usize,
};
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
    pub const image_coefficient_budget: usize = 16 * 1024 * 1024;
    raw_provider: ?*RawMetalProvider,
    raw_decode_runtime: ?*RawMetalDecodeRuntime,
    embedding_workspace_leases: [64]?memory.AdmissionLease = @splat(null),
    embedding_workspace_lease_count: usize = 0,
    embedding_workspace_bytes: usize = 0,
    /// Protected by the model's shared-provider execution lease. Entries own
    /// their names and raw slots, never a request tensor or allocator. The
    /// admission pointer belongs to the same drained model WeightStore.
    embeddinggemma2_linear_cache: std.StringHashMapUnmanaged(EmbeddingGemma2LinearCacheEntry) = .empty,
    embeddinggemma2_constant_cache: std.StringHashMapUnmanaged(PreparedConstant) = .empty,
    embeddinggemma2_constant_bytes: usize = 0,
    embeddinggemma2_linear_cache_bytes: usize = 0,
    linear_slot_owner_epoch: u64 = 1,
    linear_slot_index_mutex: std.atomic.Mutex = .unlocked,
    operations: @import("../execution/operation_owner.zig").Owner = .{},
    // Atomic marks own no caller metadata. The provider already owns the
    // corresponding SDK payload and fallback clones until actual retirement.
    private_retirement_dirty: std.atomic.Value(bool) = .init(false),
    private_linear_retirements: [decoder_runtime_linear_slot_capacity]std.atomic.Value(u64) = @splat(.init(0)),
    private_layer_norm_retirements: [decoder_runtime_layer_norm_slot_capacity]std.atomic.Value(u64) = @splat(.init(0)),
    private_rms_norm_retirements: [decoder_runtime_rms_norm_slot_capacity]std.atomic.Value(u64) = @splat(.init(0)),

    embeddinggemma2_vision_linear_cache_bytes: usize = 0,
    embeddinggemma2_audio_linear_cache_bytes: usize = 0,
    embedding_vision_coefficients: ?*@import("../ops/metal_embedding_bindings.zig").VisionCoefficients = null,
    embedding_vision_workspace_builds: u64 = 0,
    embedding_vision_coefficient_builds: u64 = 0,
    embedding_vision_workspaces: [2]?*@import("../ops/metal_embedding_bindings.zig").VisionWorkspace = @splat(null),
    /// One model-owned RGB preparer; coefficients replace by geometry and are
    /// admitted independently of request workspace. The provider lease serializes it.
    embedding_image_preparer: ?@import("antfly_video").preparation.Metal = null,
    embedding_rotary: [4]?*@import("../ops/metal_embedding_bindings.zig").Rotary = @splat(null),
    embedding_rotary_clock: u64 = 0,
    embedding_rotary_builds: u64 = 0,
    embedding_rotary_hits: u64 = 0,
    embedding_vision_clock: u64 = 0,
    embedding_vision_retry: u64 = 0,
    embedding_bindings: ?*@import("../ops/metal_embedding_bindings.zig").State = null,
    embedding_binding_retry: u64 = 0,
    embedding_binding_clock: u64 = 0,
    audio_schedules: [16]?*@import("../ops/metal_audio_execution.zig").Plan = @splat(null),
    audio_schedule_clock: u64 = 0,
    audio_schedule_history: @import("../ops/metal_audio_execution.zig").History = .{},
    embeddinggemma2_audio_pending_release: usize = 0,
    embeddinggemma2_audio_storage_bytes: usize = 0,
    embeddinggemma2_cache_admission: ?*tier_cache_mod.SharedCache = null,
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

    // Declaration ownership is independent of lazy SDK materialization.
    pub fn privateSlotDeclaration(self: *MetalNativeProvider, kind: metal_runtime.PreparedBindingKind, slot: usize) u64 {
        return metal_runtime.antfly_metal_prepared_declaration(self.raw_decode_runtime, kind, slot);
    }
    pub fn replacePrivateSlotDeclaration(self: *MetalNativeProvider, kind: metal_runtime.PreparedBindingKind, slot: usize) void {
        metal_runtime.antfly_metal_replace_prepared_declaration(self.raw_decode_runtime, kind, slot);
    }

    fn privateRetirementMarks(self: *MetalNativeProvider, kind: metal_runtime.PreparedBindingKind) []std.atomic.Value(u64) {
        return switch (kind) {
            .linear => &self.private_linear_retirements,
            .layer_norm => &self.private_layer_norm_retirements,
            .rms_norm => &self.private_rms_norm_retirements,
        };
    }

    /// Metadata-only, including concurrent last-output retirement. Nonwrapping
    /// binding generations ensure a late old mark cannot overwrite a newer one.
    pub fn requestPrivateSlotRetirement(self: *MetalNativeProvider, kind: metal_runtime.PreparedBindingKind, slot: usize, generation: u64) void {
        const marks = self.privateRetirementMarks(kind);
        std.debug.assert(slot < marks.len and generation != 0);
        const retired_generation = metal_runtime.antfly_metal_retire_prepared_binding(self.raw_decode_runtime, kind, slot, generation);
        if (retired_generation == 0) return;
        var previous = marks[slot].load(.acquire);
        while (previous < retired_generation) {
            previous = marks[slot].cmpxchgWeak(previous, retired_generation, .release, .acquire) orelse break;
        }
        // Publish AFTER the mark. Racing terminal cleanup may leave one benign
        // extra scan, but can never erase the last unseen retirement request.
        self.private_retirement_dirty.store(true, .release);
    }

    /// Actual host provider ownership is supplied explicitly by its terminal
    /// finalizer. No callback waits for GPU work or acquires numerical authority.
    pub inline fn retirePrivateSlotsAtTerminal(self: *MetalNativeProvider, current: *const @import("../execution/operation_owner.zig").Owner.Record.Snapshot) !usize {
        if (!self.private_retirement_dirty.load(.acquire)) return 0;
        return self.retirePrivateSlotsAtTerminalCold(current);
    }
    noinline fn retirePrivateSlotsAtTerminalCold(self: *MetalNativeProvider, current: *const @import("../execution/operation_owner.zig").Owner.Record.Snapshot) !usize {
        if (current.provider != @as(*anyopaque, @ptrCast(self)) or !current.entered or current.mode == .metadata or !try current.ticket.ownsGate()) return error.BackendCleanupAuthorityRequired;
        if (metal_runtime.hasActiveFrame(self.raw_decode_runtime) or metal_runtime.hasSubmittedFrame(self.raw_decode_runtime)) return 0;
        if (!self.private_retirement_dirty.swap(false, .acq_rel)) return 0;
        while (!self.linear_slot_index_mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.linear_slot_index_mutex.unlock();
        var retired: usize = 0;
        inline for ([_]metal_runtime.PreparedBindingKind{ .linear, .layer_norm, .rms_norm }) |kind| {
            const marks = self.privateRetirementMarks(kind);
            for (marks, 0..) |*mark, slot| {
                const expected = mark.swap(0, .acq_rel);
                if (expected == 0 or expected != self.privateSlotDeclaration(kind, slot)) continue;
                switch (kind) {
                    .linear => {
                        // A shared admitted cache entry owns its own payload,
                        // charge and retirement protocol. Private teardown must
                        // never evict it or refund its charge through this path.
                        if (!self.allowsOwnedSlotMutation(slot)) continue;
                        metal_runtime.clearRawLinearSlot(self, slot);
                    },
                    .layer_norm => metal_runtime.clearRawLayerNormSlot(self, slot),
                    .rms_norm => metal_runtime.clearRawRmsNormSlot(self, slot),
                }
                retired += 1;
            }
        }
        return retired;
    }

    pub fn create() !MetalNativeProvider {
        return createWithKernelJit(.{});
    }

    pub fn createWithKernelJit(config: kernel_jit.Config) !MetalNativeProvider {
        return createWithKernelJitOptions(.{ .config = config });
    }

    pub fn createWithKernelJitOptions(options: metal_runtime.MetalJitOptions) !MetalNativeProvider {
        return createWithKernelJitOptionsAndPipelineFormats(options, null);
    }

    /// A null census preserves complete preparation for open or custom stores.
    pub fn createWithKernelJitOptionsAndPipelineFormats(options: metal_runtime.MetalJitOptions, formats: ?u16) !MetalNativeProvider {
        const raw_provider = metal_runtime.termite_metal_provider_create();
        const raw_decode_runtime = metal_runtime.termite_metal_decode_runtime_create_for_quant_formats(formats orelse std.math.maxInt(u16));
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

    pub fn trimEmbeddingWorkspace(self: *MetalNativeProvider) !void {
        if (metal_runtime.termite_metal_decode_runtime_trim_embedding_workspace(self.raw_decode_runtime) != 0)
            return error.MetalWorkspaceBusy;
        self.releaseEmbeddingWorkspaceLeases();
    }

    fn clearRotary(self: *MetalNativeProvider, vision: ?bool) usize {
        var bytes: usize = 0;
        for (&self.embedding_rotary) |*slot| if (slot.*) |entry| {
            if (vision) |kind| if (entry.spec.vision != kind) continue;
            bytes += entry.owner.host_bytes + entry.owner.backend_bytes;
            entry.owner.deinit();
            if (self.embeddinggemma2_cache_admission) |cache| {
                cache.noteRelease(.host, entry.owner.host_bytes);
                cache.noteRelease(.backend, entry.owner.backend_bytes);
            }
            std.heap.c_allocator.destroy(entry);
            slot.* = null;
        };
        return bytes;
    }
    fn clearVisionWorkspaces(self: *MetalNativeProvider) usize {
        var bytes: usize = 0;
        for (&self.embedding_vision_workspaces) |*entry| if (entry.*) |workspace| {
            bytes += workspace.owner.host_bytes + workspace.owner.backend_bytes;
            workspace.owner.deinit();
            if (self.embeddinggemma2_cache_admission) |cache| {
                cache.noteRelease(.host, workspace.owner.host_bytes);
                cache.noteRelease(.backend, workspace.owner.backend_bytes);
            }
            std.heap.c_allocator.destroy(workspace);
            entry.* = null;
        };
        if (self.embedding_vision_coefficients) |coefficients| {
            bytes += coefficients.owner.host_bytes + coefficients.owner.backend_bytes;
            coefficients.owner.deinit();
            if (self.embeddinggemma2_cache_admission) |cache| {
                cache.noteRelease(.host, coefficients.owner.host_bytes);
                cache.noteRelease(.backend, coefficients.owner.backend_bytes);
            }
            std.heap.c_allocator.destroy(coefficients);
            self.embedding_vision_coefficients = null;
        }
        if (self.embedding_image_preparer) |*preparer| {
            preparer.deinit(); // Fence before releasing persistent admission.
            self.embedding_image_preparer = null;
            if (self.embeddinggemma2_cache_admission) |cache| {
                cache.noteRelease(.host, @sizeOf(@import("antfly_video").preparation.Metal));
                cache.noteRelease(.backend, image_coefficient_budget);
            }
            bytes += image_coefficient_budget + @sizeOf(@import("antfly_video").preparation.Metal);
        }
        self.embedding_vision_retry = 0;
        return bytes;
    }
    fn clearEmbeddingBindings(self: *MetalNativeProvider) usize {
        const state = self.embedding_bindings orelse return 0;
        const bytes = state.owner.host_bytes + state.owner.backend_bytes;
        state.deinit();
        if (self.embeddinggemma2_cache_admission) |cache| {
            cache.noteRelease(.host, state.owner.host_bytes);
            cache.noteRelease(.backend, state.owner.backend_bytes);
        }
        std.heap.c_allocator.destroy(state);
        self.embedding_bindings = null;
        self.embedding_binding_retry = 0;
        return bytes;
    }
    fn clearAudioSchedules(self: *MetalNativeProvider) usize {
        var bytes: usize = 0;
        for (&self.audio_schedules) |*entry| if (entry.*) |plan| {
            bytes += plan.host_bytes + plan.backend_bytes;
            plan.deinit();
            if (self.embeddinggemma2_cache_admission) |cache| {
                cache.noteRelease(.host, plan.host_bytes);
                cache.noteRelease(.backend, plan.backend_bytes);
            }
            std.heap.c_allocator.destroy(plan);
            entry.* = null;
        };
        return bytes;
    }
    /// Caller owns the shared provider execution lease and residency lock.
    /// Optional immutable constants yield to admission by class. Retained
    /// execution workspaces keep their existing ownership. No allocation occurs.
    pub fn reclaimEmbeddingGemma2Weights(self: *MetalNativeProvider, cache_class: @import("../models/embedding_gemma2_weight_cache.zig").MetalClass) !usize {
        while (!self.linear_slot_index_mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.linear_slot_index_mutex.unlock();
        if (metal_runtime.hasActiveFrame(self.raw_decode_runtime) or
            metal_runtime.hasSubmittedFrame(self.raw_decode_runtime)) return error.MetalWorkspaceBusy;
        const cache = self.embeddinggemma2_cache_admission orelse return 0;
        const policy = @import("../models/embedding_gemma2_weight_cache.zig");
        var reclaimed: usize = 0;
        if (cache_class != .audio) reclaimed += self.clearRotary(cache_class == .vision);
        if (cache_class == .language) reclaimed += self.clearEmbeddingBindings();
        if (cache_class == .vision) reclaimed += self.clearVisionWorkspaces();
        if (cache_class == .audio) {
            if (metal_runtime.antfly_metal_audio_storage_clear(self.raw_decode_runtime) != 0) return error.MetalWorkspaceBusy;
            reclaimed += self.clearAudioSchedules();
            if (self.embeddinggemma2_audio_storage_bytes != 0) {
                cache.noteRelease(.backend, self.embeddinggemma2_audio_storage_bytes);
                reclaimed += self.embeddinggemma2_audio_storage_bytes;
                self.embeddinggemma2_audio_storage_bytes = 0;
                self.embeddinggemma2_audio_pending_release = 0;
            }
            while (self.embeddinggemma2_constant_cache.count() > 0) {
                var constants = self.embeddinggemma2_constant_cache.iterator();
                const key = constants.next().?.key_ptr.*;
                var removed = self.embeddinggemma2_constant_cache.fetchRemove(key).?;
                removed.value.tensor.deinit();
                self.embeddinggemma2_constant_bytes -= removed.value.bytes;
                self.embeddinggemma2_linear_cache_bytes -= removed.value.bytes;
                self.embeddinggemma2_audio_linear_cache_bytes -= removed.value.bytes;
                cache.noteRelease(.backend, removed.value.bytes);
                reclaimed += removed.value.bytes;
                std.heap.c_allocator.free(removed.key);
            }
        }
        // Restart iteration after removal; never retain an invalidated iterator
        // or allocate bookkeeping while recovering from memory pressure.
        while (true) {
            var it = self.embeddinggemma2_linear_cache.iterator();
            const name = blk: {
                while (it.next()) |entry| {
                    if (policy.metalClass(entry.key_ptr.*) != cache_class) continue;
                    if (entry.value_ptr.owner) |owner| {
                        if (owner.beginRetirement() == null) continue;
                    }
                    break :blk entry.key_ptr.*;
                }
                break :blk null;
            } orelse break;
            const candidate = self.embeddinggemma2_linear_cache.get(name).?;
            // These slots were published only from owned F32 snapshots. The
            // original checkpoint and shared canonical zero bias stay intact.
            if (metal_runtime.termite_metal_decode_runtime_discard_owned_linear_slot(self.raw_decode_runtime, candidate.slot) != 0) {
                // Every nonzero bridge status is explicitly before teardown.
                if (candidate.owner) |owner| (prepared_slot.Owner.Retirement{ .owner = owner }).abortBeforeTeardown();
                return error.MetalWorkspaceBusy;
            }
            metal_runtime.clearRawLinearSlot(self, candidate.slot);
            const removed = self.embeddinggemma2_linear_cache.fetchRemove(name).?;
            self.embeddinggemma2_linear_cache_bytes -= removed.value.bytes;
            if (cache_class == .vision) self.embeddinggemma2_vision_linear_cache_bytes -= removed.value.bytes;
            if (cache_class == .audio) self.embeddinggemma2_audio_linear_cache_bytes -= removed.value.bytes;
            if (removed.value.owner) |owner|
                (prepared_slot.Owner.Retirement{ .owner = owner }).commitRetiredPayload()
            else
                cache.noteRelease(.backend, removed.value.bytes);
            reclaimed += removed.value.bytes;
            std.heap.c_allocator.free(removed.key);
        }
        return reclaimed;
    }

    fn releaseEmbeddingWorkspaceLeases(self: *MetalNativeProvider) void {
        for (self.embedding_workspace_leases[0..self.embedding_workspace_lease_count]) |*lease| {
            if (lease.*) |*owned| owned.release();
            lease.* = null;
        }
        self.embedding_workspace_lease_count = 0;
        self.embedding_workspace_bytes = 0;
    }

    /// Called with the model execution gate held, after all GPU frames drain.
    /// Transfer exactly the newly retained bytes; the request still owns its
    /// other peak reservation until this transition succeeds.
    pub fn retainEmbeddingWorkspace(self: *MetalNativeProvider, lease: *?memory.AdmissionLease) !void {
        const stats = metal_runtime.runtimeMemorySnapshot(self.raw_decode_runtime);
        if (stats.frame_retained_bytes != 0) return error.MetalWorkspaceBusy;
        const bytes = metal_runtime.termite_metal_decode_runtime_embedding_workspace_bytes(self.raw_decode_runtime);
        if (bytes == std.math.maxInt(usize)) return error.InvalidMetalWorkspaceAccounting;
        if (stats.attention_span_bytes != 0 or bytes < self.embedding_workspace_bytes)
            return error.InvalidMetalWorkspaceAccounting;
        const growth = bytes - self.embedding_workspace_bytes;
        if (growth == 0) return;
        if (self.embedding_workspace_lease_count == self.embedding_workspace_leases.len)
            return error.ResourceLimitExceeded;
        if (lease.*) |*owned| {
            try owned.retain(.{ .backend_scratch_bytes = growth });
            self.embedding_workspace_leases[self.embedding_workspace_lease_count] = owned.*;
            lease.* = null;
            self.embedding_workspace_lease_count += 1;
            self.embedding_workspace_bytes = bytes;
        }
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

    /// Allocate metadata before driver preparation; existing reservation guards own
    /// error cleanup until the fully initialized slot is published to the cache.
    pub fn createLinearSlotOwner(self: *MetalNativeProvider, slot: usize, bytes: usize) !*prepared_slot.Owner {
        const epoch = self.linear_slot_owner_epoch;
        const next = try std.math.add(u64, epoch, 1);
        const owner = try prepared_slot.Owner.create(std.heap.c_allocator, slot, epoch, .{ .context = self, .bytes = bytes, .release_retired = releaseRetiredLinearCharge });
        self.linear_slot_owner_epoch = next;
        return owner;
    }

    fn releaseRetiredLinearCharge(owner: *const prepared_slot.Owner) void {
        const self: *MetalNativeProvider = @ptrCast(@alignCast(owner.resource.context));
        const cache = self.embeddinggemma2_cache_admission orelse unreachable;
        cache.noteRelease(.backend, owner.resource.bytes);
    }

    /// Call under the existing provider/residency mutation domain. The returned pin
    /// owns the actual cache member and unchanged charge, not a source tensor key.
    pub fn pinLinearSlot(self: *MetalNativeProvider, slot: usize) ?prepared_slot.Owner.Pin {
        if (!self.linear_slot_index_mutex.tryLock()) return null;
        defer self.linear_slot_index_mutex.unlock();
        var iterator = self.embeddinggemma2_linear_cache.valueIterator();
        while (iterator.next()) |entry| {
            if (entry.slot != slot) continue;
            const owner = entry.owner orelse return null;
            return owner.tryPin(owner.epoch);
        }
        return null;
    }

    /// A cached payload cannot be cleared/reassigned behind a frozen selector.
    /// Only its marked retirement (or a noncached legacy slot) permits mutation.
    pub fn allowsOwnedSlotMutation(self: *MetalNativeProvider, slot: usize) bool {
        var iterator = self.embeddinggemma2_linear_cache.valueIterator();
        while (iterator.next()) |entry| {
            if (entry.slot != slot) continue;
            const owner = entry.owner orelse return true;
            return owner.retirementInProgress();
        }
        return true;
    }

    /// Idle selectors borrow metadata only. Cache publication/lookup is already
    /// serialized by the shared provider execution+residency mutation domain.
    pub fn profileLinearSlot(self: *MetalNativeProvider, slot: usize) ?prepared_slot.Owner.Profile {
        if (!self.linear_slot_index_mutex.tryLock()) return null;
        defer self.linear_slot_index_mutex.unlock();
        return self.profileLinearSlotLocked(slot);
    }

    /// Caller already owns the narrow index mutex; this never reenters it.
    pub fn profileLinearSlotLocked(self: *MetalNativeProvider, slot: usize) ?prepared_slot.Owner.Profile {
        var iterator = self.embeddinggemma2_linear_cache.valueIterator();
        while (iterator.next()) |entry| {
            if (entry.slot != slot) continue;
            const owner = entry.owner orelse return null;
            return owner.profile();
        }
        return null;
    }

    fn preparedUseUncertain(_: *anyopaque) callconv(.c) void {
        std.log.err("Metal prepared submission lost terminal completion proof", .{});
        @import("antfly_platform").inference_process_supervisor.restartWorker();
    }

    /// Registration owns an independent pin through the real command buffer's
    /// completed handler. Caller still owns its root claim across later CB chunks.
    pub fn retainLinearSlotForActiveSubmission(self: *MetalNativeProvider, pin: prepared_slot.Owner.Pin) !void {
        while (!self.linear_slot_index_mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.linear_slot_index_mutex.unlock();
        if (!pin.valid()) return error.PreparedLinearSlotChanged;
        var iterator = self.embeddinggemma2_linear_cache.valueIterator();
        const found = while (iterator.next()) |entry| {
            if (entry.owner == pin.owner) break true;
        } else false;
        if (!found) return error.PreparedLinearSlotChanged;
        if (metal_runtime.antfly_metal_frame_retain_prepared_use(self.raw_decode_runtime, pin.owner, prepared_slot.Owner.retainSubmittedUse, prepared_slot.Owner.releaseSubmittedUse, preparedUseUncertain) != 0)
            return error.PreparedLinearSubmissionRetentionFailed;
    }

    /// Ordinary numerical callers already own the provider execution domain. Return
    /// by value so HashMap internals never escape its index mutation guard. This is
    /// NOT an independently retained claim; cold selectors must acquire a Profile
    /// under the same lock instead of keeping this owner pointer.
    pub fn lookupLinearCache(self: *MetalNativeProvider, name: []const u8) ?EmbeddingGemma2LinearCacheEntry {
        while (!self.linear_slot_index_mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.linear_slot_index_mutex.unlock();
        return self.embeddinggemma2_linear_cache.get(name);
    }

    /// Complete all submitted work, then close flush's freshly reopened empty
    /// frame. Do not wait for a frame that has never been submitted.
    pub fn quiesceOwnedRuntimeChecked(self: *MetalNativeProvider) !void {
        return self.quiesceOwnedRuntimeWith(OwnedDriver);
    }

    const OwnedDriver = struct {
        fn flush(runtime: ?*RawMetalDecodeRuntime) !void {
            try metal_runtime.flushActiveFrame(runtime);
        }
        fn cancel(runtime: ?*RawMetalDecodeRuntime) !void {
            try metal_runtime.cancelFrame(runtime);
        }
        fn active(runtime: ?*RawMetalDecodeRuntime) bool {
            return metal_runtime.hasActiveFrame(runtime);
        }
        fn submitted(runtime: ?*RawMetalDecodeRuntime) bool {
            return metal_runtime.hasSubmittedFrame(runtime);
        }
    };

    fn quiesceOwnedRuntimeWith(self: *MetalNativeProvider, comptime Driver: type) !void {
        // Creation error cleanup may own a provider before a decoder exists.
        const runtime = self.raw_decode_runtime orelse return;
        try Driver.flush(runtime);
        if (Driver.active(runtime)) try Driver.cancel(runtime);
        if (Driver.active(runtime) or Driver.submitted(runtime)) return error.MetalProviderDrainIncomplete;
    }

    /// Releases every owned resource after checked completion. The provider
    /// is consumed and must not be used or deinitialized again.
    pub fn deinitOwned(self: *MetalNativeProvider) void {
        // Release the synchronous JIT session before provider, cache, or
        // pipeline dependencies are released.
        while (!self.linear_slot_index_mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.linear_slot_index_mutex.unlock();
        self.quiesceOwnedRuntimeChecked() catch |err| {
            std.log.err("Metal provider checked teardown failed: {s}", .{@errorName(err)});
            @import("antfly_platform").inference_process_supervisor.restartWorker();
        };
        self.operations.assertIdle();
        // Completed handlers have retired submitted pins before quiesce returns.
        // Idle profile metadata may survive; live captured/armed pins may not.
        var owned_slots = self.embeddinggemma2_linear_cache.valueIterator();
        while (owned_slots.next()) |entry| if (entry.owner) |owner| {
            if (owner.beginRetirement() == null) {
                while (!owner.mutex.tryLock()) std.atomic.spinLoopHint();
                const pins = owner.payload_pins;
                const references = owner.metadata_references;
                const retiring = owner.retiring;
                owner.mutex.unlock();
                std.log.err("Metal provider still owns prepared slot {d}: pins={d} refs={d} retiring={}", .{ owner.slot, pins, references, retiring });
                @import("antfly_platform").inference_process_supervisor.restartWorker();
            }
        };
        metal_runtime.destroyMetalJitSession(self.jit_session);
        metal_runtime.resetGatheredSpans(self);
        _ = self.clearRotary(null);
        _ = self.clearEmbeddingBindings();
        _ = self.clearVisionWorkspaces();
        _ = self.clearAudioSchedules();
        if (self.embeddinggemma2_audio_storage_bytes != 0) {
            _ = metal_runtime.antfly_metal_audio_storage_clear(self.raw_decode_runtime);
            if (self.embeddinggemma2_cache_admission) |cache| cache.noteRelease(.backend, self.embeddinggemma2_audio_storage_bytes);
            self.embeddinggemma2_audio_storage_bytes = 0;
            self.embeddinggemma2_audio_pending_release = 0;
        }
        self.deberta_relative_embedding_cache.deinit();
        for (&self.deberta_relative_projection_cache) |*entry| entry.deinit();
        for (0..decoder_runtime_linear_slot_capacity) |slot| metal_runtime.releaseRawLinearSlot(self, slot);
        var constants = self.embeddinggemma2_constant_cache.iterator();
        while (constants.next()) |entry| {
            entry.value_ptr.tensor.deinit();
            std.heap.c_allocator.free(entry.key_ptr.*);
            if (self.embeddinggemma2_cache_admission) |cache| cache.noteRelease(.backend, entry.value_ptr.bytes);
        }
        self.embeddinggemma2_constant_cache.deinit(std.heap.c_allocator);
        self.embeddinggemma2_constant_cache = .empty;
        self.embeddinggemma2_constant_bytes = 0;
        var embeddinggemma2_it = self.embeddinggemma2_linear_cache.iterator();
        while (embeddinggemma2_it.next()) |entry| {
            std.heap.c_allocator.free(entry.key_ptr.*);
            if (entry.value_ptr.owner) |owner|
                (prepared_slot.Owner.Retirement{ .owner = owner }).commitRetiredPayload()
            else if (self.embeddinggemma2_cache_admission) |cache|
                cache.noteRelease(.backend, entry.value_ptr.bytes);
        }
        self.embeddinggemma2_linear_cache.deinit(std.heap.c_allocator);
        self.embeddinggemma2_linear_cache = .empty;
        self.embeddinggemma2_linear_cache_bytes = 0;
        self.embeddinggemma2_vision_linear_cache_bytes = 0;
        self.embeddinggemma2_audio_linear_cache_bytes = 0;
        for (0..decoder_runtime_layer_norm_slot_capacity) |slot| metal_runtime.releaseRawLayerNormSlot(self, slot);
        for (0..decoder_runtime_rms_norm_slot_capacity) |slot| metal_runtime.releaseRawRmsNormSlot(self, slot);
        metal_runtime.termite_metal_provider_destroy(self.raw_provider);
        metal_runtime.termite_metal_decode_runtime_destroy(self.raw_decode_runtime);
        self.releaseEmbeddingWorkspaceLeases();
        for (&self.jit_pipeline_owners) |*generated| metal_runtime.termite_metal_generated_pipeline_destroy(generated.*);
        for (&self.jit_exact_pipeline_owners) |*generated| metal_runtime.termite_metal_generated_pipeline_destroy(generated.*);
    }
} else void;
