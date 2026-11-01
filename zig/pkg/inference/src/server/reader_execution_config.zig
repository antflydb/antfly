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
const metal_runtime = @import("../backends/metal_runtime.zig");
const gpt_model_mod = @import("../models/gpt.zig");
const runtime = @import("../runtime/root.zig");

/// One immutable serving configuration shared by admission and execution.
pub fn Features(comptime Hooks: type) type {
    return struct {
        prepared: bool = false,
        prepared_rows: u8 = 16,
        scheduling_rows: u8 = 0,
        cpu_preprocess: bool = false,
        vision_batch_items: u8 = 1,
        prefill_batch_items: u8 = 1,
        vision_frame_blocks: u8 = 1,
        pipeline_vision: bool = false,
        prefill_stream_layers: u8 = 0,
        prefill_decode_streak: u8 = 4,
        activation_pool: bool = false,
        prefill_activation_pool: bool = false,
        worker: bool = false,
        epoch_auxiliary: bool = false,
        single_frame: bool = false,
        gpu_token_burst: bool = false,
        gpu_token_burst_width: u8 = 2,
        cuda_burst_extra_bytes: usize = 0,
        gpu_greedy_stream: bool = false,
        stream_chunks: bool = false,
        vision_workspace_cache: bool = false,
        device_vision_features: bool = false,
        vision_late_seal: bool = false,
        queued_phase_priority: bool = false,
        resident_vision_budget: bool = false,
        prefill_late_seal: bool = false,
        shared_core_weights: bool = false,
        stream_first_layers: u8 = 0,
        batch_attention: bool = false,
        prefill_batch: bool = false,
        packed_mixed: bool = false,
        packed_prefill_items: u8 = 1,
        prepared_prefill_chunk_rows: u16 = 0,
        packed_shared_frame: bool = false,
        shared_decoder_scratch: bool = false,

        pub const Profile = enum {
            conservative,
            metal_lighton08,
            metal_lighton4b,
            cuda_lighton08,
            cuda_lighton4b,
        };

        /// Select only model/backend combinations with a complete execution
        /// and admission contract; partial feature switches do not form modes.
        pub fn selectProfile(cfg: gpt_model_mod.Config, backend: runtime.kv.pool.BackendKind, cuda_requested: bool, metal_qualified: bool, metal_requested: bool) Profile {
            if (!cfg.hasQwen35VisionArchitectureContract() or !cfg.qwen35_device_only_state or
                (cfg.hidden_size != 1024 and cfg.hidden_size != 2560)) return .conservative;
            if (backend == .cuda and cuda_requested)
                return if (cfg.hidden_size == 1024) .cuda_lighton08 else .cuda_lighton4b;
            if (backend == .metal and metal_qualified and metal_requested and cfg.qwen35_metal_resident_vision)
                return if (cfg.hidden_size == 1024) .metal_lighton08 else .metal_lighton4b;
            return .conservative;
        }

        /// A profile is resolved once for the epoch. Operator row limits can
        /// shrink it; no profile changes byte limits or selects extra branches.
        pub fn forProfile(profile: Profile, cfg: gpt_model_mod.Config, requested_rows: usize) @This() {
            if (profile == .conservative) return .{ .prepared_rows = 0, .resident_vision_budget = cfg.qwen35_metal_resident_vision };
            const cuda = profile == .cuda_lighton08 or profile == .cuda_lighton4b;
            const large = profile == .cuda_lighton4b or profile == .metal_lighton4b;
            const rows = std.math.clamp(requested_rows, 1, if (cuda) @as(usize, 128) else 64);
            return .{
                .prepared = true,
                .prepared_rows = if (rows > 64) 128 else if (rows > 32) 64 else if (rows > 16) 32 else 16,
                .scheduling_rows = @intCast(rows),
                .cpu_preprocess = true,
                .vision_batch_items = if (cuda and !large) 8 else 1,
                .vision_frame_blocks = if (cuda) 1 else if (large) 3 else 4,
                .prefill_decode_streak = 0,
                .activation_pool = !cuda,
                .worker = true,
                .epoch_auxiliary = true,
                .single_frame = !cuda,
                .gpu_token_burst = !cuda,
                .gpu_token_burst_width = 4,
                .vision_workspace_cache = !cuda,
                .device_vision_features = cuda,
                .vision_late_seal = cuda,
                .prefill_late_seal = cuda,
                .queued_phase_priority = cuda,
                .resident_vision_budget = cfg.qwen35_metal_resident_vision,
                .shared_core_weights = !cuda,
                .batch_attention = !cuda,
                .packed_mixed = true,
                .packed_prefill_items = if (!cuda) 1 else if (large) 4 else 8,
                .prepared_prefill_chunk_rows = if (cuda and large) 256 else 0,
                .packed_shared_frame = !cuda,
                .shared_decoder_scratch = true,
            };
        }

        /// Keep the selected default when unset. Zero explicitly disables
        /// chunking; valid positive values retain the operator's bound. This
        /// value is shared by admission estimation and the execution pipeline.
        pub fn withPreparedPrefillChunkOverride(self: @This(), text: ?[]const u8) @This() {
            if (!self.prepared) return self;
            const configured = text orelse return self;
            const rows = std.fmt.parseInt(u16, configured, 10) catch return self;
            if (rows != 0 and (rows < 64 or rows > 1024)) return self;
            var result = self;
            result.prepared_prefill_chunk_rows = rows;
            return result;
        }

        /// Batch sizes are bounded operator controls, resolved once for the
        /// same configuration used by admission, brokers, and the decoder.
        pub fn withBatchOverrides(self: @This(), vision: ?[]const u8, prefill: ?[]const u8, max_vision: u8, max_prefill: u8) @This() {
            if (!self.prepared) return self;
            const vision_cap = std.math.clamp(max_vision, 1, 16);
            const prefill_cap = std.math.clamp(max_prefill, 1, 16);
            var result = self;
            result.vision_batch_items = @min(result.vision_batch_items, vision_cap);
            result.packed_prefill_items = @min(result.packed_prefill_items, prefill_cap);
            if (vision) |text| if (std.fmt.parseInt(u8, text, 10)) |items| {
                if (items >= 1 and items <= 16) result.vision_batch_items = @min(items, vision_cap);
            } else |_| {};
            if (prefill) |text| if (std.fmt.parseInt(u8, text, 10)) |items| {
                if (items >= 1 and items <= 16) result.packed_prefill_items = @min(items, prefill_cap);
            } else |_| {};
            return result;
        }

        /// The existing operator limit bounds leader fairness. It does not
        /// prevent compatible prefills from filling a decode-led step.
        pub fn withDecodeStreakOverride(self: @This(), text: ?[]const u8) @This() {
            if (!self.prepared) return self;
            const configured = text orelse return self;
            const turns = std.fmt.parseInt(u8, configured, 10) catch return self;
            if (turns > 16) return self;
            var result = self;
            result.prefill_decode_streak = turns;
            return result;
        }

        /// Keep foreground gate fairness when queued broker priority is
        /// disabled. Resolve the existing rollback control once per epoch.
        pub fn withQueuedPhasePriorityOverride(self: @This(), text: ?[]const u8) @This() {
            if (!self.prepared) return self;
            const value = text orelse return self;
            const enabled = if (std.mem.eql(u8, value, "0") or std.ascii.eqlIgnoreCase(value, "false") or std.ascii.eqlIgnoreCase(value, "off") or std.ascii.eqlIgnoreCase(value, "no")) false else if (std.mem.eql(u8, value, "1") or std.ascii.eqlIgnoreCase(value, "true") or std.ascii.eqlIgnoreCase(value, "on") or std.ascii.eqlIgnoreCase(value, "yes")) true else return self;
            var result = self;
            result.queued_phase_priority = enabled;
            return result;
        }

        /// Explicit CUDA token chaining uses the existing bounded operator
        /// switch; the selected profile stays unchanged when it is disabled.
        pub fn withCudaTokenBurstOverride(self: @This(), enabled: bool, width_text: ?[]const u8) @This() {
            if (!self.prepared) return self;
            var result = self;
            result.gpu_token_burst = enabled;
            if (width_text) |text| if (std.fmt.parseInt(u8, text, 10)) |width| {
                if (width >= 2 and width <= 8) result.gpu_token_burst_width = width;
            } else |_| {};
            return result;
        }

        pub fn fromEnvironment() @This() {
            // Without a model and backend there is no qualified epoch profile.
            return .{};
        }

        pub fn fromEnvironmentForModel(cfg: gpt_model_mod.Config) @This() {
            return resolve(cfg, if (Hooks.cudaLightOnBatchProfileEnabled()) .cuda else .metal);
        }

        pub fn withHostVisionFeatures(self: @This()) @This() {
            var result = self;
            result.device_vision_features = false;
            return result;
        }

        pub fn resolve(cfg: gpt_model_mod.Config, backend: runtime.kv.pool.BackendKind) @This() {
            return resolveWithPolicy(cfg, backend, Hooks.cudaLightOnBatchProfileEnabled(), Hooks.effectiveLightOnReadRows());
        }

        pub fn resolveWithPolicy(cfg: gpt_model_mod.Config, backend: runtime.kv.pool.BackendKind, cuda_requested: bool, rows: usize) @This() {
            const profile = selectProfile(cfg, backend, cuda_requested, metal_runtime.appleSimdgroupDeviceDefault(), metal_runtime.lightOnThroughputProfileEnabled());
            // Profiling and concurrent planned dispatch do not share the
            // selected frame/lifetime contract. Fall back as one complete mode.
            const restricted_metal_mode = backend == .metal and
                (platform.env.getenvBool("TERMITE_METAL_DISABLE_PIPELINED_DECODE_FRAME") or
                    platform.env.getenvBool("TERMITE_METAL_ENABLE_CONCURRENT_PLANNED_DISPATCH") or
                    platform.env.getenvBool("TERMITE_QWEN3VL_PROFILE") or
                    platform.env.getenv("ANTFLY_QWEN3VL_QUALIFICATION_TRACE_LAYER") != null);
            if (!platform.env.getenvBoolDefault("ANTFLY_LIGHTONOCR3_PREPARED_DECODE", true) or restricted_metal_mode)
                return forProfile(.conservative, cfg, 1);
            var result = forProfile(profile, cfg, rows);
            if (backend == .cuda) {
                // Canonical batching groups decode work independently of
                // vision. An eight-image vision reservation can exceed the
                // whole scratch budget for one full-size page. Wider vision
                // batches remain an explicit, separately qualified override.
                result.vision_batch_items = 1;
                result = result.withCudaTokenBurstOverride(platform.env.getenvBool("ANTFLY_LIGHTONOCR3_GPU_TOKEN_BURST"), platform.env.getenv("ANTFLY_LIGHTONOCR3_GPU_TOKEN_BURST_WIDTH"));
                result = result.withDecodeStreakOverride(platform.env.getenv("ANTFLY_LIGHTONOCR3_PREFILL_DECODE_STREAK"));
                result = result.withQueuedPhasePriorityOverride(platform.env.getenv("ANTFLY_LIGHTONOCR3_QUEUED_PHASE_PRIORITY"));
                result = result.withPreparedPrefillChunkOverride(platform.env.getenv("ANTFLY_LIGHTONOCR3_PREFILL_QUERY_CHUNK_ROWS"));
                const wide = platform.env.getenvBool("ANTFLY_LIGHTONOCR3_CUDA_WIDE_MICROBATCHES");
                result = result.withBatchOverrides(platform.env.getenv("ANTFLY_LIGHTONOCR3_VISION_BATCH_ITEMS"), platform.env.getenv("ANTFLY_LIGHTONOCR3_PACKED_PREFILL_ITEMS"), if (wide) 16 else 8, if (wide) 16 else 4);
            }
            if (platform.env.getenvBool("ANTFLY_LIGHTONOCR3_DISABLE_SHARED_VISION_PEAK"))
                result.vision_workspace_cache = false;
            // Keep the encoder handoff rollback independent of shared-pool
            // admission and the backend workspace policy.
            if (platform.env.getenvBool("ANTFLY_LIGHTONOCR3_DISABLE_DEVICE_VISION"))
                result = result.withHostVisionFeatures();
            return result;
        }
        pub fn packedSharedFrameEligible(features: @This(), cfg: gpt_model_mod.Config) bool {
            return features.packed_mixed and features.single_frame and
                features.shared_decoder_scratch and cfg.qwen35_device_only_state and
                cfg.hasQwen35VisionArchitectureContract() and
                features.prefill_stream_layers == 0 and features.stream_first_layers == 0 and
                !features.prefill_activation_pool and !features.gpu_greedy_stream;
        }
        pub fn visionRetainedBlocks(features: @This()) usize {
            return @as(usize, features.vision_frame_blocks) * (if (features.pipeline_vision) @as(usize, 2) else 1);
        }
        pub fn auxiliaryAmounts(features: @This()) runtime.tier.memory.AdmissionAmounts {
            var amounts = runtime.tier.memory.AdmissionAmounts{};
            // The same conservative envelopes as page admission. Only
            // their owner changes: these allocations exist once per epoch.
            if (features.prepared) {
                amounts.backend_scratch_bytes += (if (features.prepared_rows > 64) @as(usize, 48) else if (features.prepared_rows > 32) @as(usize, 24) else if (features.prepared_rows > 16) @as(usize, 12) else 8) * 1024 * 1024;
                amounts.host_scratch_bytes += 1024 * 1024;
            }
            if (features.activation_pool) {
                amounts.backend_scratch_bytes += (if (features.prefill_activation_pool) @as(usize, 64) else 8) * 1024 * 1024;
                amounts.host_scratch_bytes += 1024 * 1024;
            }
            if (features.worker) amounts.host_scratch_bytes += 32 * 1024 * 1024;
            if (features.shared_core_weights) {
                amounts.backend_scratch_bytes += 8 * 1024 * 1024;
                amounts.host_scratch_bytes += 1024 * 1024;
            }
            if (features.batch_attention) {
                // Two 9 MiB banks permit one submitted prefix plus one
                // active suffix without reusing in-flight attention scratch.
                amounts.backend_scratch_bytes += 18 * 1024 * 1024;
                amounts.host_scratch_bytes += 1024 * 1024;
            }
            if (features.stream_first_layers != 0 or features.gpu_token_burst) {
                // Bound a prefix in flight while the suffix is encoded.
                amounts.backend_scratch_bytes += (if (features.prepared_rows > 64) @as(usize, 48) else if (features.prepared_rows > 32) @as(usize, 24) else if (features.prepared_rows > 16) @as(usize, 12) else 8) * 1024 * 1024;
                amounts.host_scratch_bytes += 1024 * 1024;
            }
            if (features.gpu_token_burst and features.gpu_token_burst_width > 2) amounts.backend_scratch_bytes += 4 * 1024 * 1024;
            amounts.backend_scratch_bytes += features.cuda_burst_extra_bytes;
            return amounts;
        }
        pub fn needsAuxiliary(features: @This()) bool {
            return features.epoch_auxiliary and (features.prepared or features.activation_pool or features.worker or features.shared_core_weights or features.stream_first_layers != 0 or features.batch_attention);
        }
        /// Resources one page holds when admitted alone: its own lease plus
        /// the epoch auxiliary and shared peaks, which stay leased while it
        /// waits. If this exceeds the limits, waiting can never admit it.
        pub fn soloPagePlan(features: @This(), page: runtime.tier.memory.AdmissionAmounts, shared_backend_scratch: usize, shared_host_scratch: usize) !runtime.tier.memory.AdmissionAmounts {
            var plan = try page.merge(.{ .backend_scratch_bytes = shared_backend_scratch, .host_scratch_bytes = shared_host_scratch });
            if (features.needsAuxiliary()) plan = try plan.merge(features.auxiliaryAmounts());
            return plan;
        }
        /// Prepared-plan workspace capacity to configure on the backend.
        /// Conservative epochs have no prepared plan and keep the default.
        pub fn preparedRowCapacity(features: @This()) ?usize {
            return if (features.prepared) features.prepared_rows else null;
        }
    };
}

test "LightOnOCR3 supported reader profiles isolate models backends and conservative fallback" {
    const Hooks = struct {
        pub fn effectiveLightOnReadRows() usize {
            return 128;
        }
        pub fn cudaLightOnBatchProfileEnabled() bool {
            return true;
        }
    };
    const F = Features(Hooks);
    for ([_][]const u8{ @embedFile("../models/testdata/lightonocr3/config.json"), @embedFile("../models/testdata/lightonocr3/config-4b.json") }) |json| {
        var cfg = try gpt_model_mod.parseConfig(std.testing.allocator, json);
        cfg.qwen35_device_only_state = true;
        cfg.qwen35_metal_resident_vision = true;
        const large = cfg.hidden_size == 2560;
        try std.testing.expectEqual(if (large) F.Profile.cuda_lighton4b else F.Profile.cuda_lighton08, F.selectProfile(cfg, .cuda, true, true, true));
        try std.testing.expectEqual(if (large) F.Profile.metal_lighton4b else F.Profile.metal_lighton08, F.selectProfile(cfg, .metal, true, true, true));
        try std.testing.expectEqual(F.Profile.conservative, F.selectProfile(cfg, .native, true, true, true));
        try std.testing.expectEqual(F.Profile.conservative, F.selectProfile(cfg, .cuda, false, true, true));
        try std.testing.expectEqual(F.Profile.conservative, F.selectProfile(cfg, .metal, true, false, true));
        cfg.qwen35_device_only_state = false;
        try std.testing.expectEqual(F.Profile.conservative, F.selectProfile(cfg, .cuda, true, true, true));
    }
    try std.testing.expectEqual(F.Profile.conservative, F.selectProfile(.{}, .cuda, true, true, true));
}

test "LightOnOCR3 conservative reader leaves Metal prepared capacity unconfigured" {
    const Hooks = struct {
        pub fn effectiveLightOnReadRows() usize {
            return 16;
        }
        pub fn cudaLightOnBatchProfileEnabled() bool {
            return false;
        }
    };
    const F = Features(Hooks);
    const cfg = gpt_model_mod.Config{ .qwen35_device_only_state = true, .qwen35_metal_resident_vision = true };
    // Metal accepts only 16, 32 or 64 prepared rows. The default profile
    // must not configure capacity it never uses.
    try std.testing.expectEqual(@as(?usize, null), F.forProfile(.conservative, cfg, 1).preparedRowCapacity());
    for ([_]F.Profile{ .metal_lighton08, .metal_lighton4b }) |profile| {
        for ([_]usize{ 0, 1, 16, 17, 33, 64, 999 }) |requested| {
            const rows = F.forProfile(profile, cfg, requested).preparedRowCapacity() orelse return error.TestUnexpectedResult;
            try std.testing.expect(rows == 16 or rows == 32 or rows == 64);
        }
    }
}

test "LightOnOCR3 solo page plan includes the shared peaks a waiting page keeps leased" {
    const Hooks = struct {
        pub fn effectiveLightOnReadRows() usize {
            return 16;
        }
        pub fn cudaLightOnBatchProfileEnabled() bool {
            return false;
        }
    };
    const F = Features(Hooks);
    const cfg = gpt_model_mod.Config{ .qwen35_device_only_state = true, .qwen35_metal_resident_vision = true };
    const throughput = F.forProfile(.metal_lighton08, cfg, 16);
    const page = runtime.tier.memory.AdmissionAmounts{ .backend_scratch_bytes = 66 * 1024 * 1024 };
    const shared_peaks: usize = 1460 * 1024 * 1024;
    // Measured on an M5 Pro: automatic limits admitted the shared peaks, then
    // denied every page as temporarily unavailable until its wait expired.
    const automatic = runtime.tier.memory.Limits{ .scratch_limit_bytes = 1536 * 1024 * 1024 };
    const plan = try throughput.soloPagePlan(page, shared_peaks, 0);
    try std.testing.expect(!try plan.fitsLimits(automatic));
    try std.testing.expect(try plan.fitsLimits(.{ .scratch_limit_bytes = 6144 * 1024 * 1024 }));
    const auxiliary = throughput.auxiliaryAmounts();
    try std.testing.expectEqual(page.backend_scratch_bytes + shared_peaks + auxiliary.backend_scratch_bytes, plan.backend_scratch_bytes);
    try std.testing.expectEqual(auxiliary.host_scratch_bytes, plan.host_scratch_bytes);
    // Conservative epochs lease no auxiliary state.
    const conservative = try F.forProfile(.conservative, cfg, 1).soloPagePlan(page, shared_peaks, 4096);
    try std.testing.expectEqual(page.backend_scratch_bytes + shared_peaks, conservative.backend_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 4096), conservative.host_scratch_bytes);
}

test "LightOnOCR3 selected reader profiles retain admission ownership row caps and CUDA chunking" {
    const Hooks = struct {
        pub fn effectiveLightOnReadRows() usize {
            return 128;
        }
        pub fn cudaLightOnBatchProfileEnabled() bool {
            return true;
        }
    };
    const F = Features(Hooks);
    for ([_]F.Profile{ .metal_lighton08, .metal_lighton4b, .cuda_lighton08, .cuda_lighton4b }) |profile| {
        const cfg = gpt_model_mod.Config{ .qwen35_device_only_state = true, .qwen35_metal_resident_vision = true };
        const selected = F.forProfile(profile, cfg, 999);
        const cuda = profile == .cuda_lighton08 or profile == .cuda_lighton4b;
        try std.testing.expectEqual(if (cuda) @as(u8, 128) else 64, selected.scheduling_rows);
        try std.testing.expect(selected.needsAuxiliary());
        try std.testing.expect(selected.packed_mixed and selected.prepared and selected.worker and selected.shared_decoder_scratch);
        try std.testing.expect(!selected.gpu_greedy_stream and !selected.stream_chunks and !selected.prefill_activation_pool and selected.stream_first_layers == 0 and selected.prefill_stream_layers == 0);
        const full = selected.auxiliaryAmounts();
        const partial = F.forProfile(profile, cfg, 48);
        try std.testing.expectEqual(@as(u8, 64), partial.prepared_rows);
        try std.testing.expectEqual(@as(u8, 48), partial.scheduling_rows);
        const limited = F.forProfile(profile, cfg, 8);
        try std.testing.expectEqual(@as(u8, 8), limited.scheduling_rows);
        try std.testing.expect(limited.auxiliaryAmounts().backend_scratch_bytes <= full.backend_scratch_bytes);
        var controller = runtime.tier.memory.AdmissionController{};
        try std.testing.expectError(error.ResourceLimitExceeded, controller.tryAcquire(.gpu, .{ .backend_limit_bytes = full.backend_scratch_bytes - 1 }, full, false));
        var lease = try controller.tryAcquire(.gpu, .{ .backend_limit_bytes = full.backend_scratch_bytes, .host_limit_bytes = full.host_scratch_bytes }, full, false);
        lease.release();
        try std.testing.expectEqual(@as(usize, 0), controller.snapshot().backend_scratch_bytes);
        try std.testing.expectEqual(@as(u16, if (profile == .cuda_lighton4b) 256 else 0), selected.prepared_prefill_chunk_rows);
    }
}

test "LightOnOCR3 CUDA reader prefix limit honors explicit zero and valid operator bounds" {
    const F = Features(struct {});
    const defaults = F.forProfile(.cuda_lighton4b, .{}, 128);
    try std.testing.expectEqual(@as(u16, 256), defaults.prepared_prefill_chunk_rows);
    try std.testing.expectEqualDeep(defaults, defaults.withPreparedPrefillChunkOverride(null));
    for ([_][]const u8{ "0", "64", "128", "256", "1024" }, [_]u16{ 0, 64, 128, 256, 1024 }) |text, rows| {
        var resolved = defaults.withPreparedPrefillChunkOverride(text);
        try std.testing.expectEqual(rows, resolved.prepared_prefill_chunk_rows);
        resolved.prepared_prefill_chunk_rows = defaults.prepared_prefill_chunk_rows;
        try std.testing.expectEqualDeep(defaults, resolved);
    }
    for ([_][]const u8{ "", "invalid", "63", "1025", "65536" }) |text|
        try std.testing.expectEqualDeep(defaults, defaults.withPreparedPrefillChunkOverride(text));
    const fallback = F.forProfile(.conservative, .{}, 1);
    try std.testing.expectEqualDeep(fallback, fallback.withPreparedPrefillChunkOverride("128"));
}

test "LightOnOCR3 CUDA reader batch overrides preserve operator bounds and backend ceilings" {
    const F = Features(struct {});
    const defaults = F.forProfile(.cuda_lighton4b, .{}, 128);
    try std.testing.expectEqualDeep(defaults, defaults.withBatchOverrides(null, null, 16, 16));
    var selected = defaults.withBatchOverrides("4", "8", 16, 16);
    try std.testing.expectEqual(@as(u8, 4), selected.vision_batch_items);
    try std.testing.expectEqual(@as(u8, 8), selected.packed_prefill_items);
    selected.vision_batch_items = defaults.vision_batch_items;
    selected.packed_prefill_items = defaults.packed_prefill_items;
    try std.testing.expectEqualDeep(defaults, selected);
    const bounded = defaults.withBatchOverrides("16", "16", 8, 4);
    try std.testing.expectEqual(@as(u8, 8), bounded.vision_batch_items);
    try std.testing.expectEqual(@as(u8, 4), bounded.packed_prefill_items);
    const lowered = defaults.withBatchOverrides("1", "2", 16, 16);
    try std.testing.expectEqual(@as(u8, 1), lowered.vision_batch_items);
    try std.testing.expectEqual(@as(u8, 2), lowered.packed_prefill_items);
    for ([_][]const u8{ "0", "17", "invalid", "256" }) |text|
        try std.testing.expectEqualDeep(defaults, defaults.withBatchOverrides(text, text, 16, 16));
    const narrow = F.forProfile(.cuda_lighton08, .{}, 64).withBatchOverrides(null, null, 8, 4);
    try std.testing.expectEqual(@as(u8, 4), narrow.packed_prefill_items);
    const fallback = F.forProfile(.conservative, .{}, 1);
    try std.testing.expectEqualDeep(fallback, fallback.withBatchOverrides("16", "16", 16, 16));
}

test "LightOnOCR3 CUDA prefill fairness limit preserves selected defaults and operator bounds" {
    const F = Features(struct {});
    const selected = F.forProfile(.cuda_lighton4b, .{}, 128);
    try std.testing.expectEqualDeep(selected, selected.withDecodeStreakOverride(null));
    for ([_]u8{ 0, 1, 4, 8, 16 }) |turns| {
        var text: [3]u8 = undefined;
        const value = try std.fmt.bufPrint(&text, "{d}", .{turns});
        var resolved = selected.withDecodeStreakOverride(value);
        try std.testing.expectEqual(turns, resolved.prefill_decode_streak);
        resolved.prefill_decode_streak = selected.prefill_decode_streak;
        try std.testing.expectEqualDeep(selected, resolved);
    }
    for ([_][]const u8{ "", "-1", "17", "256", "junk" }) |text| try std.testing.expectEqualDeep(selected, selected.withDecodeStreakOverride(text));
    const fallback = F.forProfile(.conservative, .{}, 1);
    try std.testing.expectEqualDeep(fallback, fallback.withDecodeStreakOverride("8"));
}

test "LightOnOCR3 CUDA token chaining opt in shares bounded admission configuration" {
    const F = Features(struct {});
    const selected = F.forProfile(.cuda_lighton4b, .{}, 128);
    try std.testing.expectEqualDeep(selected, selected.withCudaTokenBurstOverride(false, null));
    const burst = selected.withCudaTokenBurstOverride(true, "8");
    try std.testing.expect(burst.gpu_token_burst);
    try std.testing.expectEqual(@as(u8, 8), burst.gpu_token_burst_width);
    try std.testing.expect(burst.auxiliaryAmounts().backend_scratch_bytes > selected.auxiliaryAmounts().backend_scratch_bytes);
    try std.testing.expect(burst.auxiliaryAmounts().host_scratch_bytes > selected.auxiliaryAmounts().host_scratch_bytes);
    for ([_][]const u8{ "", "-1", "1", "9", "256", "junk" }) |text| try std.testing.expectEqualDeep(selected.withCudaTokenBurstOverride(true, null), selected.withCudaTokenBurstOverride(true, text));
    const fallback = F.forProfile(.conservative, .{}, 1);
    try std.testing.expectEqualDeep(fallback, fallback.withCudaTokenBurstOverride(true, "8"));
}

test "LightOnOCR3 CUDA queued phase rollback retains foreground fairness and admission bounds" {
    const F = Features(struct {});
    for ([_]F.Profile{ .cuda_lighton08, .cuda_lighton4b }) |profile| {
        const selected = F.forProfile(profile, .{}, 128);
        try std.testing.expect(selected.queued_phase_priority);
        try std.testing.expectEqualDeep(selected, selected.withQueuedPhasePriorityOverride(null));
        for ([_][]const u8{ "0", "false", "OFF", "no" }) |text| {
            var resolved = selected.withQueuedPhasePriorityOverride(text);
            try std.testing.expect(!resolved.queued_phase_priority);
            try std.testing.expectEqualDeep(selected.auxiliaryAmounts(), resolved.auxiliaryAmounts());
            for ([_][]const u8{ "1", "TRUE", "on", "yes" }) |enabled| try std.testing.expectEqualDeep(selected, resolved.withQueuedPhasePriorityOverride(enabled));
            resolved.queued_phase_priority = selected.queued_phase_priority;
            try std.testing.expectEqualDeep(selected, resolved);
        }
        for ([_][]const u8{ "", "invalid", "2", "-1" }) |text| try std.testing.expectEqualDeep(selected, selected.withQueuedPhasePriorityOverride(text));
    }
    const fallback = F.forProfile(.conservative, .{}, 1);
    try std.testing.expectEqualDeep(fallback, fallback.withQueuedPhasePriorityOverride("1"));
}

test "LightOnOCR3 CUDA device vision profile keeps existing admission and host rollback" {
    const F = Features(struct {});
    for ([_]F.Profile{ .cuda_lighton08, .cuda_lighton4b }) |profile| {
        const selected = F.forProfile(profile, .{}, 128);
        try std.testing.expect(selected.device_vision_features);
        var fallback = selected.withHostVisionFeatures();
        try std.testing.expect(!fallback.device_vision_features);
        try std.testing.expectEqualDeep(selected.auxiliaryAmounts(), fallback.auxiliaryAmounts());
        fallback.device_vision_features = true;
        try std.testing.expectEqualDeep(selected, fallback);
    }
    for ([_]F.Profile{ .metal_lighton08, .metal_lighton4b, .conservative }) |profile|
        try std.testing.expect(!F.forProfile(profile, .{}, 64).device_vision_features);
}

test "LightOnOCR3 configured execution selects CUDA reader profile without environment switches" {
    const F = Features(struct {});
    for ([_][]const u8{ @embedFile("../models/testdata/lightonocr3/config.json"), @embedFile("../models/testdata/lightonocr3/config-4b.json") }) |json| {
        var cfg = try gpt_model_mod.parseConfig(std.testing.allocator, json);
        cfg.qwen35_device_only_state = true;
        const enabled = F.resolveWithPolicy(cfg, .cuda, true, 2);
        try std.testing.expect(enabled.prepared);
        try std.testing.expectEqual(@as(u16, 2), enabled.scheduling_rows);
        try std.testing.expectEqual(@as(u8, 1), enabled.vision_batch_items);
        try std.testing.expect(!F.resolveWithPolicy(cfg, .cuda, false, 2).prepared);
        cfg.qwen35_device_only_state = false;
        try std.testing.expect(!F.resolveWithPolicy(cfg, .cuda, true, 2).prepared);
    }
}
