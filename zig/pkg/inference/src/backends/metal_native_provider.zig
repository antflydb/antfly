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

// MLX-free Metal provider for termite's decoder runtime.
//
// Mirrors the Metal-native portion of `MetalProvider` (metal_provider.zig)
// without any MLX JIT kernel fields or `c.mlx_array`-typed methods. Used by
// `MetalCompute` when the build has `-Dmetal=true -Dmlx=false`. The fields
// are laid out to match what `metal_runtime.zig` duck-types on `self` so the
// same helper functions work for both provider variants.

const std = @import("std");
const build_options = @import("build_options");
const metal_runtime = @import("metal_runtime.zig");
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

pub const MetalNativeProvider = if (build_options.enable_metal) struct {
    raw_provider: ?*RawMetalProvider,
    raw_decode_runtime: ?*RawMetalDecodeRuntime,
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
    raw_linear_slot_runtime_prepared_kind: [decoder_runtime_linear_slot_capacity]RawQuantizedRuntimeLinearKind = @as([decoder_runtime_linear_slot_capacity]RawQuantizedRuntimeLinearKind, @splat(.none)),
    raw_linear_slot_runtime_prepared_modes: [decoder_runtime_linear_slot_capacity]RawQuantizedRuntimeLinearStorageMode = @as([decoder_runtime_linear_slot_capacity]RawQuantizedRuntimeLinearStorageMode, @splat(.none)),
    raw_quant_runtime_private_prepare_nanos: u128 = 0,
    raw_quant_runtime_mapped_prepare_nanos: u128 = 0,
    raw_quant_runtime_mapped_attempts: u64 = 0,
    raw_quant_runtime_mapped_fallbacks: u64 = 0,
    raw_quant_runtime_mapped_failures: u64 = 0,
    gathered_spans: std.AutoHashMapUnmanaged(GatheredSpanKey, GatheredSpanEntry) = .empty,

    pub fn create() !MetalNativeProvider {
        const raw_provider = metal_runtime.termite_metal_provider_create();
        errdefer metal_runtime.termite_metal_provider_destroy(raw_provider);
        const raw_decode_runtime = metal_runtime.termite_metal_decode_runtime_create();
        errdefer metal_runtime.termite_metal_decode_runtime_destroy(raw_decode_runtime);
        return .{
            .raw_provider = raw_provider,
            .raw_decode_runtime = raw_decode_runtime,
        };
    }

    pub fn hasDecoderRuntime(self: *const MetalNativeProvider) bool {
        const runtime = self.raw_decode_runtime orelse return false;
        return metal_runtime.termite_metal_decode_runtime_ready(runtime) != 0;
    }

    pub fn reserveDecoderRuntime(self: *MetalNativeProvider, scratch_bytes: usize, token_bytes: usize) !bool {
        const runtime = self.raw_decode_runtime orelse return false;
        return metal_runtime.termite_metal_decode_runtime_reserve(runtime, scratch_bytes, token_bytes) == 0;
    }

    pub fn deinitOwned(self: *MetalNativeProvider) void {
        metal_runtime.flushActiveFrame(self.raw_decode_runtime) catch {};
        if (metal_runtime.hasActiveFrame(self.raw_decode_runtime)) {
            metal_runtime.waitFrame(self.raw_decode_runtime) catch {};
        }
        metal_runtime.resetGatheredSpans(self);
        for (0..decoder_runtime_linear_slot_capacity) |slot| metal_runtime.clearRawLinearSlot(self, slot);
        for (0..decoder_runtime_layer_norm_slot_capacity) |slot| {
            if (self.raw_layer_norm_slot_weights[slot]) |*t| t.deinit();
            self.raw_layer_norm_slot_weights[slot] = null;
            if (self.raw_layer_norm_slot_biases[slot]) |*t| t.deinit();
            self.raw_layer_norm_slot_biases[slot] = null;
        }
        for (0..decoder_runtime_rms_norm_slot_capacity) |slot| {
            if (self.raw_rms_norm_slot_weights[slot]) |*t| t.deinit();
            self.raw_rms_norm_slot_weights[slot] = null;
        }
        metal_runtime.termite_metal_provider_destroy(self.raw_provider);
        metal_runtime.termite_metal_decode_runtime_destroy(self.raw_decode_runtime);
    }
} else void;
