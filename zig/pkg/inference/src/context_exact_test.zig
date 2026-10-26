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

//! Installed-artifact exact-context observations. Neither probe grants qualification.
//! External documents are length checked with the loaded template and tokenizer;
//! the repeated-token default remains a capacity diagnostic only.
const std = @import("std");
const build_options = @import("build_options");
const backends = @import("backends/backends.zig");
const manager_mod = @import("server/model_manager.zig");
const factory = @import("architectures/session_factory.zig");
const generation = @import("pipelines/generation.zig");
const memory = @import("runtime/tier/memory.zig");
const kv = @import("runtime/kv/root.zig");
const policy = @import("runtime/context_policy.zig");
const profiles = @import("runtime/kv/compaction_profile.zig");

fn env(name: [:0]const u8) ?[]const u8 {
    return if (std.c.getenv(name)) |value| std.mem.span(value) else null;
}

fn number(name: [:0]const u8, default: usize, maximum: usize) !usize {
    const value = if (env(name)) |raw| try std.fmt.parseInt(usize, raw, 10) else default;
    if (value == 0 or value > maximum) return error.InvalidContextProbeLimit;
    return value;
}

fn promptCount(a: std.mem.Allocator, model: *manager_mod.LoadedModel, text: []const u8) !usize {
    const template = model.chat_tmpl orelse return error.MissingChatTemplate;
    const rendered = try template.applyWithOptions(a, &.{.{ .role = "user", .content = text }}, .{ .enable_thinking = false });
    defer a.free(rendered);
    var encoded = try generation.encodeNativeGenerationPrompt(model.getTokenizer(), a, rendered, 131072, model.manifest.add_bos_token, model.manifest.bos_token);
    defer encoded.deinit();
    var count: usize = 0;
    for (encoded.attention_mask) |mask| {
        if (mask == 0) break;
        count += 1;
    }
    return count;
}

// Use the real template/tokenizer and verify the final length. This repeated
// filler exercises capacity only, not retrieval or model-quality qualification.
fn sizedPrompt(a: std.mem.Allocator, model: *manager_mod.LoadedModel, target: usize) ![]u8 {
    const prefix = "Remember marker AZURE-731. Reference text:";
    const suffix = "\nWhat is the marker?";
    var low: usize = 0;
    var high = target;
    while (low <= high) {
        const repeats = low + (high - low) / 2;
        const text = try a.alloc(u8, prefix.len + repeats * 2 + suffix.len);
        @memcpy(text[0..prefix.len], prefix);
        for (0..repeats) |i| @memcpy(text[prefix.len + i * 2 ..][0..2], " a");
        @memcpy(text[prefix.len + repeats * 2 ..], suffix);
        const count = promptCount(a, model, text) catch |err| {
            a.free(text);
            return err;
        };
        if (count == target) return text;
        a.free(text);
        if (count < target) low = repeats + 1 else {
            if (repeats == 0) break;
            high = repeats - 1;
        }
    }
    return error.UnrepresentableContextProbeLength;
}

test "context Metal installed Gemma exact capacity observation" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!std.mem.eql(u8, env("ANTFLY_CONTEXT_EXACT_PROBE") orelse return error.SkipZigTest, "1")) return error.SkipZigTest;
    const path = env("ANTFLY_CONTEXT_MODEL_DIR") orelse return error.MissingModelPath;
    const target = try number("ANTFLY_CONTEXT_PROMPT_TOKENS", 8192, 32768);
    const output = try number("ANTFLY_CONTEXT_OUTPUT_TOKENS", 128, 512);
    const chunk = try number("ANTFLY_CONTEXT_CHUNK_ROWS", 64, 512);
    const storage = env("ANTFLY_CONTEXT_EXACT_STORAGE") orelse "automatic";
    const use_packed = std.mem.eql(u8, storage, "packed_exact");
    const split_decode = std.mem.eql(u8, env("ANTFLY_CONTEXT_SPLIT_DECODE") orelse "0", "1");
    const flash_prefill = std.mem.eql(u8, env("ANTFLY_CONTEXT_FLASH_PREFILL") orelse "0", "1");
    if (!use_packed and (split_decode or flash_prefill)) return error.InvalidContextProbeKernel;
    if (!use_packed and !std.mem.eql(u8, storage, "automatic")) return error.InvalidContextProbeStorage;
    const a = std.testing.allocator;
    var backend_manager = backends.SessionManager.initWithIo(a, std.testing.io);
    backend_manager.preferred_backends = &.{.metal};
    backend_manager.required_backend = .metal;
    var manager = manager_mod.ModelManager.init(a, backend_manager);
    defer manager.deinit();
    const model = try manager.loadFromDir(path);
    const config = factory.getGptConfig(model.session) orelse return error.InvalidModelForGeneration;
    if (config.family != .gemma or config.gemma4_mtp_assistant) return error.InvalidModelForGeneration;
    const loaded = try model.compactionLoadedIdentity();
    const identity = try profiles.Identity.fromLoaded(loaded.bytes, try policy.Layout.init(config), .{
        .algorithm_version = 1,
        .packed_abi_version = 2,
        .kv_dtype = .f16,
        .kernel_route = if (split_decode or flash_prefill) .metal_context_optimized else .metal_context_correctness,
        .split_decode = split_decode,
        .flash_prefill = flash_prefill,
        .block_tokens = 512,
        .solver = .bounded_coordinate_descent,
    });
    const artifact_hex = std.fmt.bytesToHex(identity.artifact, .lower);
    const tokenizer_hex = std.fmt.bytesToHex(identity.tokenizer, .lower);
    const template_hex = std.fmt.bytesToHex(identity.template, .lower);
    const layout_hex = std.fmt.bytesToHex(identity.layout, .lower);
    const implementation_hex = std.fmt.bytesToHex(identity.runtime, .lower);
    const prompt_path = env("ANTFLY_CONTEXT_PROBE_TEXT_FILE");
    const prompt = if (prompt_path) |source|
        try std.Io.Dir.cwd().readFileAlloc(std.testing.io, source, a, .limited(1024 * 1024))
    else
        try sizedPrompt(a, model, target);
    defer a.free(prompt);
    if (try promptCount(a, model, prompt) != target) return error.ContextProbePromptLengthMismatch;
    var prompt_digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(prompt, &prompt_digest, .{});
    const prompt_hex = std.fmt.bytesToHex(prompt_digest, .lower);
    if (env("ANTFLY_CONTEXT_EXPORT_PROMPT")) |destination|
        try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = destination, .data = prompt });
    const total = target + output;
    const context_policy = policy.Policy{ .max_context_tokens = @intCast(total), .max_kv_tokens = @intCast(total) };
    var budget = memory.RunBudget.init(factory.widenBudgetLimitsForSession(model.session, memory.defaultLimitsForBackend(.gpu)));
    const component = memory.GptGenerationBudgetComponent{ .backend = .metal, .kv_dtype = .f16, .config = config, .context_policy = context_policy };
    const packed_config = @import("backends/packed_context_session.zig");
    const selected_chunk = if (use_packed) chunk else try memory.reserveGptGenerationAtLargestChunk(&budget, &.{component}, target, output, chunk);
    const packed_bytes = if (use_packed) (try packed_config.allocation(.{ .plan = try context_policy.admit(config.max_position_embeddings, null, target, output, 0), .layout = try policy.Layout.init(config), .max_query_rows = @intCast(selected_chunk), .physical_budget_bytes = 0, .optimize_decode = split_decode, .optimize_prefill = flash_prefill })).physical_bytes else 0;
    if (use_packed) {
        var estimate = try component.estimate(target, output, selected_chunk);
        estimate.kv_bytes = packed_bytes;
        try budget.reserveEstimate(estimate);
    }
    var cb = try factory.getComputeBackendWithBudget(model.session, a, &budget);
    defer cb.deinit();
    var manager_kv = kv.manager.KvManager.init(a);
    defer manager_kv.deinit();
    const pool = generation.kvPoolConfig(.metal, .f16, config, false);
    const pool_id = try manager_kv.addPool(pool);
    var store = try kv.storage_runtime.KvStorageRuntime.init(a, pool);
    defer store.deinit();
    try cb.provisionKvDeviceWriteHook(&store);
    var state = generation.NativeDecodeState.initPaged(a, &manager_kv, pool_id, null);
    defer state.deinit();
    state.kv_storage = &store;
    var pipeline = generation.NativeGenerationPipeline{ .allocator = a, .io = std.testing.io, .cb = cb, .session = model.session, .gpt_config = config, .kv_dtype = .f16, .tokenizer = model.getTokenizer(), .chat_template = model.chat_tmpl, .add_bos_token = model.manifest.add_bos_token, .bos_token = model.manifest.bos_token, .decode_state = &state, .return_raw_token_ids = true };
    const Watch = struct {
        start: std.Io.Timestamp,
        first_ns: ?u64 = null,
        count: usize = 0,
        fn emit(raw: *anyopaque, text: []const u8) bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (text.len > 0) {
                if (self.first_ns == null) self.first_ns = @intCast(@max(0, self.start.durationTo(std.Io.Timestamp.now(std.testing.io, .awake)).nanoseconds));
                self.count += 1;
            }
            return true;
        }
    };
    const request = generation.GenerationConfig{ .context_policy = context_policy, .context_storage = if (use_packed) .packed_exact else .automatic, .context_budget_bytes = packed_bytes, .context_split_decode = split_decode, .context_flash_prefill = flash_prefill, .max_tokens = @intCast(output), .ignore_eos = true, .enable_thinking = false, .prefill_chunk_size = selected_chunk };
    var watch = Watch{ .start = std.Io.Timestamp.now(std.testing.io, .awake) };
    const provider_start = cb.debugTimingSnapshot().provider;
    var result = try pipeline.generateStreaming(&.{.{ .role = "user", .content = prompt }}, request, &watch, Watch.emit);
    defer result.deinit();
    const elapsed_ns: u64 = @intCast(@max(0, watch.start.durationTo(std.Io.Timestamp.now(std.testing.io, .awake)).nanoseconds));
    try std.testing.expectEqual(target, result.prompt_tokens);
    try std.testing.expectEqual(output, result.tokens_used);
    try std.testing.expect(state.context_session == null);
    // The ordinary paged path owns its sequence until state.deinit(). Packed
    // context owns a separate hook and must never attach a legacy sequence.
    if (use_packed) try std.testing.expect(state.sequence_id == null);
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    for (result.token_ids orelse return error.MissingRawTokenIds) |id| {
        var bytes: [4]u8 = undefined;
        std.mem.writeInt(i32, &bytes, id, .little);
        hash.update(&bytes);
    }
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    const hex_digest = std.fmt.bytesToHex(digest, .lower);
    const metrics = state.last_context_metrics;
    const device_start = state.last_context_device_allocation_start;
    const device_end = state.last_context_device_allocation_end;
    const provider_stats = cb.debugTimingSnapshot().provider;
    if (provider_stats.context_precomputed_tail_blocks < provider_start.context_precomputed_tail_blocks or
        provider_stats.context_precomputed_tail_failures < provider_start.context_precomputed_tail_failures or
        provider_stats.f32_kv_quant_direct_block_successes < provider_start.f32_kv_quant_direct_block_successes)
        return error.InvalidContextRouteCounters;
    const row = .{ .schema_version = @as(u32, 2), .route = "native_pipeline", .precomputed_tail_blocks = provider_stats.context_precomputed_tail_blocks - provider_start.context_precomputed_tail_blocks, .precomputed_tail_failures = provider_stats.context_precomputed_tail_failures - provider_start.context_precomputed_tail_failures, .f32_kv_quant_direct_block_successes = provider_stats.f32_kv_quant_direct_block_successes - provider_start.f32_kv_quant_direct_block_successes, .storage = storage, .framed_attention_dispatches = state.last_context_framed_attention_dispatches, .standalone_attention_dispatches = state.last_context_standalone_attention_dispatches, .device_allocation_start = device_start, .device_allocation_end = device_end, .split_decode_dispatches = state.last_context_split_decode_dispatches, .decode_kernel = if (!use_packed) "standard" else if (split_decode) "split_candidate" else "serial_reference", .flash_prefill_dispatches = state.last_context_flash_prefill_dispatches, .prefill_kernel = if (flash_prefill) "flash_candidate" else if (use_packed) "serial_reference" else "standard", .workload = if (prompt_path != null) "independent_document" else "repeated_token_capacity", .prompt_sha256 = prompt_hex[0..], .output_token_ids = result.token_ids.?, .loaded_identity = .{ .load_generation = loaded.generation, .artifact_sha256 = artifact_hex[0..], .tokenizer_sha256 = tokenizer_hex[0..], .template_sha256 = template_hex[0..], .layout_sha256 = layout_hex[0..], .implementation_sha256 = implementation_hex[0..] }, .prompt_tokens = result.prompt_tokens, .reserved_output_tokens = output, .produced_output_tokens = result.tokens_used, .total_positions = total, .prefill_chunk_rows = selected_chunk, .output_sha256 = hex_digest[0..], .elapsed_ns = elapsed_ns, .first_callback_ns = watch.first_ns, .callback_count = watch.count, .phases_ms = result.timing_ms, .context_live_bytes = if (metrics) |m| @as(?u64, m.live_bytes) else null, .context_peak_bytes = if (metrics) |m| @as(?u64, m.peak_bytes) else null, .backend_kv_readback_bytes = if (metrics) |m| @as(?u64, m.kv_readback_bytes) else if (device_end.available) @as(?u64, device_end.readback_bytes - device_start.readback_bytes) else null };
    const json = try std.json.Stringify.valueAlloc(a, row, .{});
    defer a.free(json);
    std.debug.print("context_exact_observation: {s}\n", .{json});
    // Emit diagnostics before strict route checks; a failing process still
    // cannot satisfy a qualification resource receipt.
    if (use_packed) {
        try std.testing.expect(state.last_context_framed_attention_dispatches > 0);
        try std.testing.expectEqual(@as(u64, 0), state.last_context_standalone_attention_dispatches);
    } else {
        try std.testing.expect(device_start.available and device_end.available);
        try std.testing.expectEqual(@as(u64, 0), device_start.live_bytes);
        try std.testing.expect(device_end.kv_bytes > 0);
        try std.testing.expectEqual(@as(u64, 0), device_end.readback_bytes - device_start.readback_bytes);
    }
    if (use_packed and split_decode) try std.testing.expect(state.last_context_split_decode_dispatches > 0) else try std.testing.expectEqual(@as(u64, 0), state.last_context_split_decode_dispatches);
    if (use_packed and flash_prefill and selected_chunk > 1) try std.testing.expect(state.last_context_flash_prefill_dispatches > 0) else try std.testing.expectEqual(@as(u64, 0), state.last_context_flash_prefill_dispatches);
}

test "context Metal installed Gemma exact CLI qualification entrypoint" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!std.mem.eql(u8, env("ANTFLY_CONTEXT_EXACT_CLI_PROBE") orelse return error.SkipZigTest, "1")) return error.SkipZigTest;
    const path = env("ANTFLY_CONTEXT_MODEL_DIR") orelse return error.MissingModelPath;
    const prompt_path = env("ANTFLY_CONTEXT_PROBE_TEXT_FILE") orelse return error.MissingReplayCaptureInput;
    const output_path = env("ANTFLY_CONTEXT_TIMING_FILE") orelse return error.MissingTimingPath;
    const a = std.testing.allocator;
    const prompt = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, prompt_path, a, .limited(1024 * 1024));
    defer a.free(prompt);
    try @import("native_generate.zig").main(a, std.testing.io, &.{ path, prompt, "--backend", "metal", "--cache-dtype", "f16", "--context-mode", "exact", "--context-window", env("ANTFLY_CONTEXT_WINDOW") orelse "131072", "--max-tokens", env("ANTFLY_CONTEXT_OUTPUT_TOKENS") orelse "128", "--disable-thinking", "--ignore-eos", "--print-token-ids", "--print-timing", "--json-timing", output_path });
}
