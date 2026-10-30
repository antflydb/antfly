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

const std = @import("std");
const platform = @import("antfly_platform");
const runtime = @import("../runtime/root.zig");
const generation = @import("../pipelines/generation.zig");
const gpt_model_mod = @import("../models/gpt.zig");
const qwen3vl_projector = @import("../architectures/qwen3vl_projector.zig");

/// Cohort peak and per-page accounting share the same immutable geometry.
pub fn CohortMemory(comptime Hooks: type) type {
    return struct {
        backend: runtime.kv.pool.BackendKind = .metal,
        amounts: runtime.tier.memory.AdmissionAmounts = .{},
        vision_backend_peak: usize = 0,
        vision_host_peak: usize = 0,
        share_decoder_scratch: bool = false,
        decoder_rows: usize = 1,
        max_prefill_batch_items: usize = 1,
        packed_mixed: bool = false,
        prefill_query_chunk_rows: usize = 0,
        decoder_backend_peak: usize = 0,

        pub fn product(values: []const usize) !usize {
            var bytes: usize = 1;
            for (values) |value| bytes = std.math.mul(usize, bytes, value) catch return error.ResourceLimitExceeded;
            return bytes;
        }
        pub fn add(self: *@This(), cfg: gpt_model_mod.Config, prompt_tokens: usize, output_tokens: usize, decoder: runtime.tier.memory.AdmissionAmounts, vision: runtime.tier.memory.AdmissionAmounts) !void {
            const prefill_capacity = if (self.backend == .cuda and Hooks.cudaLightOnBatchProfileEnabled() and
                platform.env.getenvBool("ANTFLY_LIGHTONOCR3_CUDA_WIDE_MICROBATCHES"))
                generation.NativeGenerationPipeline.max_prepared_qwen_prefill_items
            else
                generation.NativeGenerationPipeline.default_prepared_qwen_prefill_items;
            if (self.share_decoder_scratch and (self.decoder_rows == 0 or self.decoder_rows > 128 or
                self.max_prefill_batch_items == 0 or self.max_prefill_batch_items > prefill_capacity)) return error.ResourceLimitExceeded;
            if (self.prefill_query_chunk_rows != 0 and
                (!self.share_decoder_scratch or !self.packed_mixed or
                    self.prefill_query_chunk_rows < 64 or self.prefill_query_chunk_rows > 1024)) return error.ResourceLimitExceeded;
            const working_rows = if (self.prefill_query_chunk_rows != 0)
                @min(prompt_tokens, self.prefill_query_chunk_rows)
            else
                prompt_tokens;
            // Only transient, gate-owned activations use the bounded query
            // shape. Full page KV, projected inputs, recurrent histories and
            // escaping outputs retain their original admission below. The
            // scratch estimate is independent of KV dtype; its KV result is
            // deliberately discarded, rather than replacing the page estimate.
            // The generic estimate includes persistent CUDA histories. Remove
            // them from shareable workspace; the per-page persistent lease below
            // charges that same history once, independently of the GPU gate.
            const recurrent_bytes = try runtime.tier.memory.gptRecurrentBackendStateBytes(self.backend, cfg);
            if (decoder.backend_scratch_bytes < recurrent_bytes) return error.ResourceLimitExceeded;
            var transient_decoder = decoder;
            transient_decoder.backend_scratch_bytes -= recurrent_bytes;
            const working_scratch = if (self.prefill_query_chunk_rows != 0)
                (try runtime.tier.memory.estimateGptGeneration(self.backend, .f16, cfg, prompt_tokens, output_tokens, working_rows)).scratch_bytes - recurrent_bytes
            else
                transient_decoder.backend_scratch_bytes;
            var retained_decoder = transient_decoder;
            if (self.share_decoder_scratch) {
                // A single GPU gate owns prefill and decode. Their frame
                // resources retire after their last queued consumer. CUDA may
                // release the gate during a retained readback: all subsequent
                // scratch reuse remains ordered on that same model stream.
                // Keep a full prompt hidden tensor and logits additive in case
                // a final-row view or sampling output outlives that gate.
                const logits = try product(&.{ cfg.vocab_size, @sizeOf(f32) });
                const hidden = try product(&.{ prompt_tokens, cfg.hidden_size, @sizeOf(f32) });
                // Packed forwards publish host logits and token IDs. Their
                // device hidden, row gather and head logits retire before the
                // GPU gate returns. Full projected inputs remain charged below,
                // together with KV, recurrent history and both working sets.
                const retained = if (self.packed_mixed) logits else std.math.add(usize, logits, hidden) catch return error.ResourceLimitExceeded;
                retained_decoder.backend_scratch_bytes = @min(transient_decoder.backend_scratch_bytes, retained);
                if (!cfg.qwen35_device_only_state)
                    retained_decoder.host_scratch_bytes = std.math.add(usize, retained_decoder.host_scratch_bytes, logits) catch return error.ResourceLimitExceeded;
                // Bound both an active and submitted set, plus the complete
                // physical-batch vocabulary output. Power-of-two growth keeps
                // this monotonic lease bounded across a long mixed-page epoch.
                // Batched prefill returns only independent host logit rows;
                // all combined device intermediates drain before it returns.
                // Reserve two complete maximum-sized group working sets.
                var two_sets = try product(&.{ working_scratch, 2, self.max_prefill_batch_items });
                if (self.packed_mixed) {
                    if (working_rows == 0) return error.ResourceLimitExceeded;
                    // Dense mixed intermediates include one token from each
                    // other physical page. Round the entire existing working
                    // set upward, retaining its fixed-cost allowance as well.
                    const rows = std.math.add(usize, working_rows, self.decoder_rows - 1) catch return error.ResourceLimitExceeded;
                    two_sets = std.math.divCeil(usize, try product(&.{ two_sets, rows }), working_rows) catch return error.ResourceLimitExceeded;
                }
                const batch_head = try product(&.{ self.decoder_rows, logits });
                const target = std.math.add(usize, two_sets, batch_head) catch return error.ResourceLimitExceeded;
                const envelope = std.math.ceilPowerOfTwo(usize, @max(target, 1)) catch return error.ResourceLimitExceeded;
                self.decoder_backend_peak = @max(self.decoder_backend_peak, envelope);
            }
            self.amounts = try self.amounts.merge(retained_decoder);
            // CPU image preparation may overlap across pages. Its complete
            // host estimate remains additive. The GPU lock still reserves one
            // backend working set plus each page's retained prompt and history.
            var retained = vision;
            retained.backend_scratch_bytes = 0;
            self.amounts = try self.amounts.merge(retained);
            self.vision_backend_peak = @max(self.vision_backend_peak, vision.backend_scratch_bytes);
            self.vision_host_peak = @max(self.vision_host_peak, vision.host_scratch_bytes);
            const key_dim = try product(&.{ cfg.qwen35_linear_num_key_heads, cfg.qwen35_linear_key_head_dim });
            const value_dim = try product(&.{ cfg.qwen35_linear_num_value_heads, cfg.qwen35_linear_value_head_dim });
            const conv_dim = std.math.add(usize, try product(&.{ key_dim, 2 }), value_dim) catch return error.ResourceLimitExceeded;
            const conv = try product(&.{ conv_dim, cfg.qwen35_linear_conv_kernel_dim, @sizeOf(f32) });
            const recurrent = try product(&.{ cfg.qwen35_linear_num_value_heads, cfg.qwen35_linear_key_head_dim, cfg.qwen35_linear_value_head_dim, @sizeOf(f32) });
            const host_state = std.math.add(usize, conv, recurrent) catch return error.ResourceLimitExceeded;
            // Two convolution histories, one recurrent matrix, and the four
            // small immutable GPU weights: conv, A_log, dt_bias, gated norm.
            const side_weights = std.math.add(usize, try product(&.{ cfg.qwen35_linear_num_value_heads, 2 }), value_dim) catch return error.ResourceLimitExceeded;
            const gpu_state = std.math.add(usize, try product(&.{ conv, 3 }), std.math.add(usize, recurrent, try product(&.{ side_weights, @sizeOf(f32) })) catch return error.ResourceLimitExceeded) catch return error.ResourceLimitExceeded;
            var persistent = runtime.tier.memory.AdmissionAmounts{};
            if (cfg.qwen35_device_only_state) {
                // The resident cache still allocates one descriptor per layer,
                // and first-token host logits can outlive the GPU gate.
                persistent.host_scratch_bytes = std.math.add(usize, try product(&.{ cfg.num_hidden_layers, @sizeOf(@import("../architectures/gpt.zig").Qwen35LinearLayerState) }), try product(&.{ cfg.vocab_size, @sizeOf(f32) })) catch return error.ResourceLimitExceeded;
            }
            for (0..cfg.num_hidden_layers) |layer| if (cfg.layerUsesQwen35LinearAttention(layer)) {
                if (!cfg.qwen35_device_only_state)
                    persistent.host_scratch_bytes = std.math.add(usize, persistent.host_scratch_bytes, host_state) catch return error.ResourceLimitExceeded;
                persistent.backend_scratch_bytes = std.math.add(usize, persistent.backend_scratch_bytes, gpu_state) catch return error.ResourceLimitExceeded;
            };
            // Independently owned device projected embeddings can await prefill
            // while another page owns the runtime. Their full per-page bytes
            // remain charged until retirement; no batched parent is borrowed.
            // Token plans and output buffers also outlive vision.
            const tokens = std.math.add(usize, prompt_tokens, output_tokens) catch return error.ResourceLimitExceeded;
            persistent = try persistent.merge(.{
                .host_scratch_bytes = try product(&.{ tokens, 128 }),
                .backend_scratch_bytes = try product(&.{ prompt_tokens, cfg.hidden_size, @sizeOf(f32) }),
            });
            self.amounts = try self.amounts.merge(persistent);
        }
        pub fn visionBatchPeak(self: @This(), max_items: usize) !usize {
            if (max_items == 0 or max_items > qwen3vl_projector.maxResidentVisionBatchItems(if (self.backend == .cuda) .cuda else .metal)) return error.ResourceLimitExceeded;
            return std.math.mul(usize, self.vision_backend_peak, max_items) catch error.ResourceLimitExceeded;
        }
        pub fn visionFrameHostPeak(self: @This(), blocks: usize, max_items: usize) !usize {
            if (blocks == 0 or blocks > 4 or max_items == 0 or max_items > qwen3vl_projector.maxResidentVisionBatchItems(if (self.backend == .cuda) .cuda else .metal)) return error.ResourceLimitExceeded;
            // Page leases already cover one complete host working set each,
            // including overlapping CPU image preparation. Grouped blocks
            // can add at most this many complete sets at the single GPU gate.
            return product(&.{ self.vision_host_peak, blocks - 1, max_items });
        }
        pub fn finish(self: @This()) !runtime.tier.memory.AdmissionAmounts {
            const peaks = std.math.add(usize, self.vision_backend_peak, self.decoder_backend_peak) catch return error.ResourceLimitExceeded;
            return self.amounts.merge(.{ .backend_scratch_bytes = peaks });
        }
    };
}
