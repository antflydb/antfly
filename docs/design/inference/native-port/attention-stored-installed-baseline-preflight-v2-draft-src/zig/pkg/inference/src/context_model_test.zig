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

//! Opt-in installed-artifact correctness probe. This never changes serving
//! capabilities and is not a substitute for long-context quality qualification.
const std = @import("std");
const build_options = @import("build_options");
const backends = @import("backends/backends.zig");
const manager_mod = @import("server/model_manager.zig");
const factory = @import("architectures/session_factory.zig");
const gpt = @import("architectures/gpt.zig");
const context = @import("backends/metal_context_session.zig");
const policy = @import("runtime/context_policy.zig");
const memory = @import("runtime/tier/memory.zig");
const generation = @import("pipelines/generation.zig");
const kv = @import("runtime/kv/root.zig");
const compaction_profiles = @import("runtime/kv/compaction_profile.zig");
const numerical_capture_inputs = @import("context_numerical_capture_inputs.zig");
const server_mod = @import("server/server.zig");
const campaign_capture_mod = @import("context_query_campaign_capture.zig");
const campaign_coordinator = @import("backends/compaction_coordinator.zig");
const campaign_protocol = @import("backends/context_query_protocol.zig");
const campaign_studies = @import("backends/context_study_templates.zig");
const context_execution_pool = @import("backends/context_execution_pool.zig");
const packed_session_mod = @import("backends/packed_context_session.zig");
const stored_installed_caller = @import("context_stored_installed_caller.zig");

test "context Metal installed Gemma artifact chunked logits parity" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    const path = std.c.getenv("ANTFLY_CONTEXT_MODEL_DIR") orelse return error.SkipZigTest;
    const a = std.testing.allocator;
    var backends_manager = backends.SessionManager.initWithIo(a, std.testing.io);
    backends_manager.preferred_backends = &.{.metal};
    backends_manager.required_backend = .metal;
    var manager = manager_mod.ModelManager.init(a, backends_manager);
    defer manager.deinit();
    const model = try manager.loadFromDir(std.mem.span(path));
    const config = factory.getGptConfig(model.session) orelse return error.InvalidModelForGeneration;
    if (config.family != .gemma or config.gemma4_mtp_assistant) return error.InvalidModelForGeneration;
    var budget = memory.RunBudget.init(.{ .host_limit_bytes = 8 * 1024 * 1024 * 1024, .backend_limit_bytes = 8 * 1024 * 1024 * 1024, .combined_limit_bytes = 10 * 1024 * 1024 * 1024 });
    var cb = try factory.getComputeBackendWithBudget(model.session, a, &budget);
    defer cb.deinit();
    const ids = [_]i64{ 2, 105, 2364, 108, 107, 235, 106, 109 };
    const plan = try (policy.Policy{}).admit(config.max_position_embeddings, null, ids.len - 1, 1, 0);
    var settings = context.Config{ .plan = plan, .layout = try policy.Layout.init(config), .max_query_rows = 8, .physical_budget_bytes = 0 };
    settings.physical_budget_bytes = (try context.allocation(settings)).physical_bytes;
    var full = try context.Session.init(a, settings);
    defer full.deinit();
    var chunked = try context.Session.init(a, settings);
    defer chunked.deinit();
    const full_context = gpt.DecodeContext{ .context_session = full.hook(), .attention_mode = .paged_prefill, .total_sequence_len = ids.len, .query_sequence_len = ids.len, .kv_sequence_len = ids.len };
    const baseline = try gpt.forward(&cb, a, config, &ids, 1, ids.len, &full_context);
    defer a.free(baseline);
    // Shared Gemma donor layers require the real serving KV store even for a
    // single full prefill. A null decode context is not a valid baseline.
    var legacy_manager = kv.manager.KvManager.init(a);
    defer legacy_manager.deinit();
    const pool_config = generation.kvPoolConfig(.metal, .f16, config, false);
    const pool_id = try legacy_manager.addPool(pool_config);
    var legacy_storage = try kv.storage_runtime.KvStorageRuntime.init(a, pool_config);
    defer legacy_storage.deinit();
    try cb.provisionKvDeviceWriteHook(&legacy_storage);
    var legacy_state = generation.NativeDecodeState.initPaged(a, &legacy_manager, pool_id, null);
    defer legacy_state.deinit();
    legacy_state.kv_storage = &legacy_storage;
    legacy_state.configureForGptConfig(config);
    try legacy_state.notePrefill(ids.len);
    const legacy_context = legacy_state.gptDecodeContext(ids.len, ids.len);
    const original = try gpt.forward(&cb, a, config, &ids, 1, ids.len, &legacy_context);
    defer a.free(original);
    try std.testing.expectEqual(baseline.len, original.len);
    var original_error: f32 = 0;
    for (baseline, original) |got, want| {
        try std.testing.expect(std.math.isFinite(got) and std.math.isFinite(want));
        original_error = @max(original_error, @abs(got - want));
    }
    try std.testing.expect(original_error <= 0.05);
    try std.testing.expectEqual(@as(usize, ids.len * config.vocab_size), baseline.len);
    var low: f32 = std.math.inf(f32);
    var high: f32 = -std.math.inf(f32);
    for (baseline) |value| {
        try std.testing.expect(std.math.isFinite(value));
        low = @min(low, value);
        high = @max(high, value);
    }
    try std.testing.expect(high - low > 1); // A constant/empty decoder cannot pass parity.
    try std.testing.expectEqual(@as(u32, ids.len), full.metrics.logical_tokens);
    var max_error: f32 = 0;
    for (0..2) |chunk| {
        const start = chunk * 4;
        const dc = gpt.DecodeContext{ .context_session = chunked.hook(), .attention_mode = .paged_prefill, .total_sequence_len = start + 4, .query_sequence_len = 4, .kv_sequence_len = start + 4 };
        const actual = try gpt.forward(&cb, a, config, ids[start..][0..4], 1, start + 4, &dc);
        defer a.free(actual);
        for (actual, baseline[start * config.vocab_size ..][0 .. 4 * config.vocab_size]) |got, want| {
            try std.testing.expect(std.math.isFinite(got) and std.math.isFinite(want));
            max_error = @max(max_error, @abs(got - want));
        }
        // Quantized projection batch geometry can round differently. Compare
        // complete logit vectors and the greedy token for every position.
        for (0..4) |row| {
            const expected = baseline[(start + row) * config.vocab_size ..][0..config.vocab_size];
            const got = actual[row * config.vocab_size ..][0..config.vocab_size];
            try std.testing.expectEqual(std.mem.indexOfMax(f32, expected), std.mem.indexOfMax(f32, got));
        }
    }
    try std.testing.expectEqual(@as(u32, ids.len), chunked.metrics.logical_tokens);
    std.debug.print("context_model_probe: path={s} layers={d} max_logit_abs_error={d} original_route_logit_error={d} context_bytes={d} kv_readback_bytes={d}\n", .{ std.mem.span(path), config.num_hidden_layers, max_error, original_error, settings.physical_budget_bytes, chunked.metrics.kv_readback_bytes });
    try std.testing.expect(max_error <= 0.05);
}

fn runContextProbe(cb: *@import("ops/ops.zig").ComputeBackend, config: @import("models/gpt.zig").Config, session: anytype, ids: []const i64) ![]f32 {
    return runContextProbeRange(cb, config, session, 0, ids);
}

fn runContextProbeRange(cb: *@import("ops/ops.zig").ComputeBackend, config: @import("models/gpt.zig").Config, session: anytype, position: usize, ids: []const i64) ![]f32 {
    const a = std.testing.allocator;
    var start: usize = 0;
    while (start < ids.len) {
        const end = position + start;
        const rows = try session.hook().nextChunk(@intCast(end), @intCast(ids.len - start));
        const dc = gpt.DecodeContext{ .context_session = session.hook(), .attention_mode = .paged_prefill, .total_sequence_len = end + rows, .query_sequence_len = rows, .kv_sequence_len = end + rows };
        const logits = try gpt.forward(cb, a, config, ids[start..][0..rows], 1, end + rows, &dc);
        defer a.free(logits);
        start += rows;
        if (start == ids.len) return a.dupe(f32, logits[(rows - 1) * config.vocab_size ..][0..config.vocab_size]);
    }
    return error.EmptyPrompt;
}

test "context Metal installed Gemma generation sampling grammar and stream stop" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    const path = std.c.getenv("ANTFLY_CONTEXT_MODEL_DIR") orelse return error.SkipZigTest;
    const a = std.testing.allocator;
    var backend_manager = backends.SessionManager.initWithIo(a, std.testing.io);
    backend_manager.preferred_backends = &.{.metal};
    backend_manager.required_backend = .metal;
    var manager = manager_mod.ModelManager.init(a, backend_manager);
    defer manager.deinit();
    const model = try manager.loadFromDir(std.mem.span(path));
    const config = factory.getGptConfig(model.session) orelse return error.InvalidModelForGeneration;
    if (config.family != .gemma or config.gemma4_mtp_assistant) return error.InvalidModelForGeneration;
    var cb = try factory.getComputeBackend(model.session, a);
    defer cb.deinit();
    const stop = struct {
        fn keepGoing(ctx: *anyopaque) bool {
            const calls: *u32 = @ptrCast(@alignCast(ctx));
            calls.* += 1;
            return calls.* < 2;
        }
        fn token(_: *anyopaque, _: []const u8) bool {
            return true;
        }
    };
    for (0..8) |iteration| {
        const route = iteration % 4;
        const owned_packed = iteration >= 4;
        // Exact policies exercise the same generic model/sampler integration
        // without bypassing the public approximate-mode qualification gate.
        var settings = context.Config{ .plan = try (policy.Policy{}).admit(config.max_position_embeddings, null, 1, 511, 0), .layout = try policy.Layout.init(config), .max_query_rows = 8, .physical_budget_bytes = 0 };
        settings.physical_budget_bytes = (try context.allocation(settings)).physical_bytes;
        var session = try context.Session.init(a, settings);
        defer session.deinit();
        var state = generation.NativeDecodeState.initContiguous(a);
        defer state.deinit();
        if (!owned_packed) state.context_session = session.hook();
        var calls: u32 = 0;
        var pipeline = generation.NativeGenerationPipeline{
            .allocator = a,
            .io = std.testing.io,
            .cb = cb,
            .session = model.session,
            .gpt_config = config,
            .kv_dtype = .f16,
            .tokenizer = model.getTokenizer(),
            .chat_template = model.chat_tmpl,
            .add_bos_token = model.manifest.add_bos_token,
            .bos_token = model.manifest.bos_token,
            .decode_state = &state,
            .continue_ctx = if (route == 3) &calls else null,
            .continue_fn = if (route == 3) stop.keepGoing else null,
        };
        const request = generation.GenerationConfig{
            .context_storage = if (owned_packed) .packed_exact else .automatic,
            .context_budget_bytes = 256 * 1024 * 1024,
            .max_tokens = 3,
            .ignore_eos = true,
            .enable_thinking = false,
            .prefill_chunk_size = 8,
            .temperature = if (route == 1) 0.7 else 0,
            .top_k = if (route == 1) 16 else 0,
            .grammar = if (route == 2) "root ::= \"a\\nb\\nc\\nd\\ne\\nf\\ng\\nh\\ni\\nj\\n\"" else null,
        };
        const messages = [_]generation.Message{.{ .role = "user", .content = "Say hello." }};
        var result = if (route == 3)
            try pipeline.generateStreaming(&messages, request, &calls, stop.token)
        else
            try pipeline.generate(&messages, request);
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, if (route == 3) 2 else 3), result.tokens_used);
        if (owned_packed) {
            try std.testing.expect(state.context_session == null);
            const final_metrics = state.last_context_metrics orelse return error.MissingContextMetrics;
            try std.testing.expectEqual(result.prompt_tokens + result.tokens_used - 1, final_metrics.logical_tokens);
            try std.testing.expectEqual(@as(u64, 0), final_metrics.kv_readback_bytes);
            try std.testing.expect(final_metrics.live_bytes > 0 and final_metrics.peak_bytes >= final_metrics.live_bytes);
            // A following rejected request must clear the previous telemetry.
            var rejected = request;
            rejected.context_budget_bytes = 0;
            try std.testing.expectError(error.ContextStagingBudgetExceeded, pipeline.generate(&messages, rejected));
            try std.testing.expect(state.last_context_metrics == null and state.context_session == null);
            std.debug.print("context_owned_generation_metrics: route={d} logical={d} live_bytes={d} peak_bytes={d} epoch={d} kv_readback_bytes={d} hook_restored=true rejected_request_cleared=true\n", .{ route, final_metrics.logical_tokens, final_metrics.live_bytes, final_metrics.peak_bytes, final_metrics.epoch, final_metrics.kv_readback_bytes });
        } else try std.testing.expectEqual(result.prompt_tokens + result.tokens_used - 1, session.metrics.logical_tokens);
        try std.testing.expect(state.sequence_id == null);
        try std.testing.expectEqual(@as(u64, 0), session.metrics.kv_readback_bytes);
        if (route == 3) {
            try std.testing.expectEqual(@as(u32, 2), calls);
            try std.testing.expectEqualStrings("stop", result.finish_reason);
        }
        std.debug.print("context_generation_probe: path={s} route={d} prompt_tokens={d} generated={d} owned_packed={}\n", .{ std.mem.span(path), route, result.prompt_tokens, result.tokens_used, owned_packed });
    }
}

// Explicit, test-only numerical diagnostics. This reads only the first original
// block after GPU completion; serving never calls it or reads KV back to fit.
// A failed fit remains a test failure even when a diagnostic dump is requested.
fn dumpCompactionProbe(session: *const context.Session) !void {
    const prefix = std.c.getenv("ANTFLY_CONTEXT_PROBE_DUMP_PREFIX") orelse return;
    const policy_value = session.config.plan.policy;
    const replay_start = session.config.calibration_replay_start;
    const all_blocks = if (std.c.getenv("ANTFLY_CONTEXT_PROBE_ALL_BLOCKS")) |value|
        std.mem.eql(u8, std.mem.span(value), "1")
    else
        false;
    if (all_blocks and replay_start == null) return error.InvalidReplayCaptureInput;
    if (session.compacted_blocks != 0 or (replay_start == null and (session.metrics.rejected_transactions == 0 or
        @as(u64, policy_value.keep_prefix_tokens) + 512 + policy_value.keep_recent_tokens > session.step_end))) return;
    owners: for (session.owners.items) |owner| {
        if (owner.staged == null) continue;
        if (owner.scale == null or @as(u64, policy_value.keep_prefix_tokens) + 512 > owner.live.rows) continue;
        for (owner.positions) |position| if (position == std.math.maxInt(u32)) continue :owners;
        const row_bytes = @as(usize, owner.shape.kv_heads) * owner.shape.head_dim * 2;
        // Capture the actual slot provenance with the tensors. Offline
        // consumers must not infer GQA/shared-layer ownership from filenames.
        // The campaign controller seals this descriptor together with the
        // capture-time source, artifact, tokenizer and document identities.
        const Consumer = struct { layer: usize, query_heads: u32 };
        const Stratum = struct { consumer_layer: usize, query_head: u32, kv_head: u32, training_start: u32, training_count: u32, validation_start: u32, validation_count: u32 };
        var consumers: std.ArrayList(Consumer) = .empty;
        defer consumers.deinit(std.testing.allocator);
        var strata: std.ArrayList(Stratum) = .empty;
        defer strata.deinit(std.testing.allocator);
        for (session.config.layout.layers[0..session.config.layout.len], 0..) |shape, layer| {
            if (shape.owner != owner.layer) continue;
            try consumers.append(std.testing.allocator, .{ .layer = layer, .query_heads = shape.query_heads });
            const groups = shape.query_heads / shape.kv_heads;
            for (0..shape.query_heads) |query_head| {
                const head: u32 = @intCast(query_head);
                const stratum = session.consumer_stratum_offsets[layer] + head % groups;
                try strata.append(std.testing.allocator, .{
                    .consumer_layer = layer,
                    .query_head = head,
                    .kv_head = head / groups,
                    .training_start = stratum * owner.geometry.train_per_stratum,
                    .training_count = owner.geometry.train_per_stratum,
                    .validation_start = stratum * owner.geometry.held_per_stratum,
                    .validation_count = owner.geometry.held_per_stratum,
                });
            }
        }
        const first_block = policy_value.keep_prefix_tokens;
        const block_limit = if (all_blocks) replay_start.? else first_block + 512;
        var block_start = first_block;
        while (@as(u64, block_start) + 512 <= @as(u64, block_limit) and
            @as(u64, block_start) + 512 <= @as(u64, owner.live.rows)) : (block_start += 512)
        {
            const start = @as(usize, block_start) * row_bytes;
            const name = if (all_blocks)
                try std.fmt.allocPrint(std.testing.allocator, "{s}-layer{d}-b{d}-h{d}-d{d}-q{d}-v{d}-scale{d}", .{ std.mem.span(prefix), owner.layer, block_start, owner.shape.kv_heads, owner.shape.head_dim, owner.geometry.training_rows, owner.geometry.validation_rows, owner.scale.? })
            else
                try std.fmt.allocPrint(std.testing.allocator, "{s}-layer{d}-h{d}-d{d}-q{d}-v{d}-scale{d}", .{ std.mem.span(prefix), owner.layer, owner.shape.kv_heads, owner.shape.head_dim, owner.geometry.training_rows, owner.geometry.validation_rows, owner.scale.? });
            defer std.testing.allocator.free(name);
            // Before publication the original pages are contiguous. All-block
            // qualification writes only per-block KV plus the shared query
            // evidence; duplicating the complete context for every block would
            // make a 32K campaign consume tens of GiB without adding evidence.
            for ([_][]const u8{ owner.live.keys.bytes[start..][0 .. 512 * row_bytes], owner.live.values.bytes[start..][0 .. 512 * row_bytes], owner.queries.?.bytes[0..owner.queries.?.logical_bytes], std.mem.sliceAsBytes(owner.positions) }, [_][]const u8{ "keys.f16", "values.f16", "queries.f32", "positions.u32" }) |bytes, suffix| {
                const path = try std.fmt.allocPrint(std.testing.allocator, "{s}-{s}", .{ name, suffix });
                defer std.testing.allocator.free(path);
                try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = path, .data = bytes });
            }
            if (!all_blocks) for ([_][]const u8{ owner.live.keys.bytes[0 .. @as(usize, owner.live.rows) * row_bytes], owner.live.values.bytes[0 .. @as(usize, owner.live.rows) * row_bytes] }, [_][]const u8{ "all-keys.f16", "all-values.f16" }) |bytes, suffix| {
                const path = try std.fmt.allocPrint(std.testing.allocator, "{s}-{s}", .{ name, suffix });
                defer std.testing.allocator.free(path);
                try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = path, .data = bytes });
            };
            const metadata = try std.json.Stringify.valueAlloc(std.testing.allocator, .{
                .rows = 512,
                .heads = owner.shape.kv_heads,
                .dim = owner.shape.head_dim,
                .training_rows = owner.geometry.training_rows,
                .validation_rows = owner.geometry.validation_rows,
                .block_start = block_start,
                .owner_layer = owner.layer,
                .scale = owner.scale.?,
                .query_origin = if (replay_start != null) "repeat_prefill" else "request_prefill",
                .query_sampling = if (replay_start != null) "uniform_partition_v1" else "rolling_partition_v1",
                .replay_start = replay_start,
                .replay_tokens = if (replay_start) |start_position| session.logical_tokens - start_position else @as(u32, 0),
                .consumers = consumers.items,
                .strata = strata.items,
            }, .{ .whitespace = .indent_2 });
            defer std.testing.allocator.free(metadata);
            const metadata_path = try std.fmt.allocPrint(std.testing.allocator, "{s}-capture.json", .{name});
            defer std.testing.allocator.free(metadata_path);
            try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = metadata_path, .data = metadata });
        }
    }
}

const QueryLoadedIdentityObservation = struct {
    load_generation: u64,
    artifact_sha256: []const u8,
    tokenizer_sha256: []const u8,
    template_sha256: []const u8,
    layout_sha256: []const u8,
    implementation_sha256: []const u8,
};

const QuerySourceObservation = struct {
    source_id: []const u8,
    kind: []const u8,
    partition: []const u8,
    capture_route: []const u8,
    conditioning: []const u8,
    physical_branch_id: []const u8,
    study_pair_id: ?[]const u8 = null,
    generation_contract_sha256: ?[]const u8 = null,
    generation_mode: ?[]const u8 = null,
    loaded_identity: QueryLoadedIdentityObservation,
    logical_start: u32,
    logical_count: u32,
    branch_logical_start: u32,
    branch_logical_count: u32,
    branch_rows_per_stratum: u32,
    owner_layer: u32,
    heads: u32,
    dim: u32,
    rows_per_head: u32,
    rows_per_stratum: u32,
    physical_bytes: usize,
    query_bytes: usize,
    elapsed_ns: u64,
    status_readback_bytes: u64,
    kv_readback_bytes: u64,
    completed: bool = true,
    cancelled: bool = false,
};

/// Stream one source partition head-by-head. `queries` and `positions` retain
/// the runtime pool's full stride, so no second materialized tensor is hidden
/// from the source's allocation or process peak. The campaign sealer hashes
/// the emitted token stream and binds its fixed-study/template identities.
fn dumpQuerySourcePartition(
    prefix: []const u8,
    observation: QuerySourceObservation,
    stride_rows: u32,
    row_offset: u32,
    queries: []const f32,
    positions: []const u32,
) !void {
    if (observation.rows_per_head == 0 or observation.rows_per_stratum == 0 or
        observation.rows_per_head % observation.rows_per_stratum != 0 or observation.heads == 0 or
        observation.dim == 0 or observation.branch_rows_per_stratum < observation.rows_per_stratum or
        observation.branch_logical_start > observation.logical_start or
        @as(u64, observation.logical_start) + observation.logical_count > @as(u64, observation.branch_logical_start) + observation.branch_logical_count or
        observation.physical_bytes < observation.query_bytes or
        observation.elapsed_ns == 0 or observation.status_readback_bytes == 0 or observation.kv_readback_bytes != 0)
        return error.InvalidReferenceReplay;
    const vectors = try std.math.mul(usize, @as(usize, observation.heads), @as(usize, stride_rows));
    if (positions.len != vectors or queries.len != try std.math.mul(usize, vectors, @as(usize, observation.dim)))
        return error.InvalidReferenceReplay;
    const stem = try std.fmt.allocPrint(std.testing.allocator, "{s}-{s}-layer{d}", .{ prefix, observation.source_id, observation.owner_layer });
    defer std.testing.allocator.free(stem);
    const query_path = try std.fmt.allocPrint(std.testing.allocator, "{s}-queries.f32", .{stem});
    defer std.testing.allocator.free(query_path);
    const position_path = try std.fmt.allocPrint(std.testing.allocator, "{s}-positions.u32", .{stem});
    defer std.testing.allocator.free(position_path);
    const query_file = try std.Io.Dir.cwd().createFile(std.testing.io, query_path, .{ .exclusive = true });
    defer query_file.close(std.testing.io);
    const position_file = try std.Io.Dir.cwd().createFile(std.testing.io, position_path, .{ .exclusive = true });
    defer position_file.close(std.testing.io);
    for (0..observation.heads) |head| {
        const row = @as(usize, @intCast(head)) * @as(usize, stride_rows) + @as(usize, row_offset);
        const selected_queries = queries[row * @as(usize, observation.dim) ..][0 .. @as(usize, observation.rows_per_head) * @as(usize, observation.dim)];
        const selected_positions = positions[row..][0..observation.rows_per_head];
        for (selected_queries) |value| if (!std.math.isFinite(value)) return error.InvalidReferenceReplay;
        for (selected_positions) |position| if (position < observation.logical_start or
            @as(u64, position) >= @as(u64, observation.logical_start) + observation.logical_count) return error.InvalidReferenceReplay;
        try query_file.writeStreamingAll(std.testing.io, std.mem.sliceAsBytes(selected_queries));
        try position_file.writeStreamingAll(std.testing.io, std.mem.sliceAsBytes(selected_positions));
    }
    const metadata = try std.json.Stringify.valueAlloc(std.testing.allocator, observation, .{ .whitespace = .indent_2 });
    defer std.testing.allocator.free(metadata);
    const metadata_path = try std.fmt.allocPrint(std.testing.allocator, "{s}-source.json", .{stem});
    defer std.testing.allocator.free(metadata_path);
    const metadata_file = try std.Io.Dir.cwd().createFile(std.testing.io, metadata_path, .{ .exclusive = true });
    defer metadata_file.close(std.testing.io);
    try metadata_file.writeStreamingAll(std.testing.io, metadata);
}

fn dumpFilteredQuerySourcePartition(
    prefix: []const u8,
    observation: QuerySourceObservation,
    view: @import("backends/model_replay_context.zig").QueryPartitionView,
) !void {
    if (observation.rows_per_stratum == 0 or observation.branch_rows_per_stratum == 0 or
        view.rows_per_head % observation.branch_rows_per_stratum != 0 or
        observation.rows_per_head != observation.rows_per_stratum * (view.rows_per_head / observation.branch_rows_per_stratum))
        return error.InvalidReferenceReplay;
    const stem = try std.fmt.allocPrint(std.testing.allocator, "{s}-{s}-layer{d}", .{ prefix, observation.source_id, observation.owner_layer });
    defer std.testing.allocator.free(stem);
    const query_path = try std.fmt.allocPrint(std.testing.allocator, "{s}-queries.f32", .{stem});
    defer std.testing.allocator.free(query_path);
    const position_path = try std.fmt.allocPrint(std.testing.allocator, "{s}-positions.u32", .{stem});
    defer std.testing.allocator.free(position_path);
    const query_file = try std.Io.Dir.cwd().createFile(std.testing.io, query_path, .{ .exclusive = true });
    defer query_file.close(std.testing.io);
    const position_file = try std.Io.Dir.cwd().createFile(std.testing.io, position_path, .{ .exclusive = true });
    defer position_file.close(std.testing.io);
    const logical_end = try std.math.add(u32, observation.logical_start, observation.logical_count);
    const strata = view.rows_per_head / observation.branch_rows_per_stratum;
    for (0..view.heads) |head| {
        const query_head = try view.queryHead(head);
        const position_head = try view.positionHead(head);
        for (0..strata) |stratum| {
            var selected: u32 = 0;
            const base = stratum * observation.branch_rows_per_stratum;
            for (position_head[base..][0..observation.branch_rows_per_stratum], 0..) |position, row| {
                if (position < observation.logical_start or position >= logical_end) continue;
                const vector = query_head[(base + row) * observation.dim ..][0..observation.dim];
                for (vector) |value| if (!std.math.isFinite(value)) return error.InvalidReferenceReplay;
                try query_file.writeStreamingAll(std.testing.io, std.mem.sliceAsBytes(vector));
                try position_file.writeStreamingAll(std.testing.io, std.mem.asBytes(&position));
                selected += 1;
            }
            if (selected != observation.rows_per_stratum) return error.InvalidReferenceReplay;
        }
    }
    const metadata = try std.json.Stringify.valueAlloc(std.testing.allocator, observation, .{ .whitespace = .indent_2 });
    defer std.testing.allocator.free(metadata);
    const metadata_path = try std.fmt.allocPrint(std.testing.allocator, "{s}-source.json", .{stem});
    defer std.testing.allocator.free(metadata_path);
    const metadata_file = try std.Io.Dir.cwd().createFile(std.testing.io, metadata_path, .{ .exclusive = true });
    defer metadata_file.close(std.testing.io);
    try metadata_file.writeStreamingAll(std.testing.io, metadata);
}

fn runPrivateStudySource(
    a: std.mem.Allocator,
    cb: *@import("ops/ops.zig").ComputeBackend,
    config: @import("models/gpt.zig").Config,
    layout: policy.Layout,
    origins: []const @import("backends/model_replay_context.zig").Origin,
    prompt_tokens: usize,
    study_prompt: []const i64,
    generated_rows: u32,
    partition: @import("backends/model_replay_context.zig").QueryPartition,
    prefix: []const u8,
    source_id: []const u8,
    source_kind: []const u8,
    study_pair_id: []const u8,
    generation_contract_sha256: ?[]const u8,
    loaded_identity: QueryLoadedIdentityObservation,
) !void {
    const branch_mod = @import("backends/model_replay_context.zig");
    const response = generated_rows > 0;
    if (study_prompt.len < 8 or study_prompt.len > 8192 or response != std.mem.eql(u8, source_kind, "fixed_study_generated_response") or
        (response and (generated_rows < 8 or generated_rows > 8192 or generation_contract_sha256 == null)) or
        (!response and !std.mem.eql(u8, source_kind, "fixed_study_prefill"))) return error.InvalidReplayCaptureLength;
    const appended = try std.math.add(usize, study_prompt.len, @as(usize, generated_rows));
    var private_config = branch_mod.Config{
        .plan = try (policy.Policy{}).admit(config.max_position_embeddings, null, prompt_tokens, appended, 0),
        .layout = layout,
        .max_query_rows = 128,
        .capture_start = @intCast(prompt_tokens + if (response) study_prompt.len else 0),
        .physical_budget_bytes = 0,
    };
    private_config.physical_budget_bytes = (try branch_mod.allocation(private_config, origins)).physical_bytes;
    const started = std.Io.Timestamp.now(std.testing.io, .awake);
    var branch = try branch_mod.Session.init(a, private_config, origins);
    defer branch.deinit();
    var logits = try runContextProbeRange(cb, config, &branch, prompt_tokens, study_prompt);
    defer a.free(logits);
    const generated = try a.alloc(i64, @as(usize, generated_rows));
    defer a.free(generated);
    for (generated, 0..) |*token, index| {
        token.* = @intCast(std.mem.indexOfMax(f32, logits));
        const next_logits = try runContextProbeRange(cb, config, &branch, prompt_tokens + study_prompt.len + index, generated[index .. index + 1]);
        a.free(logits);
        logits = next_logits;
    }
    try branch.finish();
    try branch.seal();
    const elapsed_ns: u64 = @intCast(@max(1, started.durationTo(std.Io.Timestamp.now(std.testing.io, .awake)).nanoseconds));
    const source_tokens = if (response) generated else study_prompt;
    const token_path = try std.fmt.allocPrint(a, "{s}-{s}-tokens.i64", .{ prefix, source_id });
    defer a.free(token_path);
    const token_file = try std.Io.Dir.cwd().createFile(std.testing.io, token_path, .{ .exclusive = true });
    defer token_file.close(std.testing.io);
    try token_file.writeStreamingAll(std.testing.io, std.mem.sliceAsBytes(source_tokens));
    for (branch.owners) |owner| if (owner.queries != null) {
        const view = try branch.sealedQueryPartition(owner.layer, partition);
        const pool = owner.queries.?;
        try dumpQuerySourcePartition(prefix, .{
            .source_id = source_id,
            .kind = source_kind,
            .partition = if (partition == .training) "training" else "validation",
            .capture_route = "private_model_replay",
            .conditioning = if (response) "completed_prompt_study_generated_response" else "completed_prompt_private_replay",
            .physical_branch_id = source_id,
            .study_pair_id = study_pair_id,
            .generation_contract_sha256 = generation_contract_sha256,
            .generation_mode = if (response) "greedy_fixed_length_ignore_eos" else null,
            .loaded_identity = loaded_identity,
            .logical_start = private_config.capture_start.?,
            .logical_count = @intCast(source_tokens.len),
            .branch_logical_start = private_config.capture_start.?,
            .branch_logical_count = @intCast(source_tokens.len),
            .owner_layer = owner.layer,
            .heads = view.heads,
            .dim = view.dim,
            .rows_per_head = view.rows_per_head,
            .rows_per_stratum = if (partition == .training) pool.shape.train_per_stratum else pool.shape.held_per_stratum,
            .branch_rows_per_stratum = if (partition == .training) pool.shape.train_per_stratum else pool.shape.held_per_stratum,
            .physical_bytes = branch.sizes.physical_bytes,
            .query_bytes = branch.sizes.query_bytes,
            .elapsed_ns = elapsed_ns,
            .status_readback_bytes = branch.status_readback_bytes,
            .kv_readback_bytes = branch.kv_readback_bytes,
        }, view.stride_rows, view.row_offset, view.queries, view.positions);
    };
}

fn runPrivateStudyPair(
    a: std.mem.Allocator,
    cb: *@import("ops/ops.zig").ComputeBackend,
    config: @import("models/gpt.zig").Config,
    layout: policy.Layout,
    origins: []const @import("backends/model_replay_context.zig").Origin,
    prompt_tokens: usize,
    study_prompt: []const i64,
    question_tokens: usize,
    fit_study_tokens: usize,
    gate_study_tokens: usize,
    generated_rows: u32,
    partition: @import("backends/model_replay_context.zig").QueryPartition,
    prefix: []const u8,
    study_pair_id: []const u8,
    generation_contract_sha256: []const u8,
    loaded_identity: QueryLoadedIdentityObservation,
) !void {
    const branch_mod = @import("backends/model_replay_context.zig");
    const query_protocol = @import("backends/context_query_protocol.zig");
    if (study_prompt.len < 8 or study_prompt.len > 8192 or generated_rows < 8 or generated_rows > 8192 or
        !isLowerHexSha256(generation_contract_sha256)) return error.InvalidReplayCaptureLength;
    const appended = try std.math.add(usize, study_prompt.len, @as(usize, generated_rows));
    const prompt: u32 = @intCast(prompt_tokens);
    const question_end = try std.math.add(u32, prompt, @intCast(question_tokens));
    const fit_end = try std.math.add(u32, prompt, @intCast(fit_study_tokens));
    const fit_response_end = try std.math.add(u32, fit_end, generated_rows);
    const gate_end = try std.math.add(u32, prompt, @intCast(gate_study_tokens));
    const gate_response_end = try std.math.add(u32, gate_end, generated_rows);
    const training = partition == .training;
    const protocol_sources = [_]query_protocol.Source{
        .{ .kind = .request_question_replay, .partition = .training, .start = prompt, .end = question_end, .active = false },
        .{ .kind = .fixed_study_prefill, .partition = .training, .start = prompt, .end = fit_end, .active = training },
        .{ .kind = .fixed_study_generated_response, .partition = .training, .start = fit_end, .end = fit_response_end, .active = training },
        .{ .kind = .fixed_study_prefill, .partition = .validation, .start = prompt, .end = gate_end, .active = !training },
        .{ .kind = .fixed_study_generated_response, .partition = .validation, .start = gate_end, .end = gate_response_end, .active = !training },
    };
    var private_config = branch_mod.Config{
        .plan = try (policy.Policy{}).admit(config.max_position_embeddings, null, prompt_tokens, appended, 0),
        .layout = layout,
        .max_query_rows = 128,
        .capture_start = @intCast(prompt_tokens),
        .query_sources = &protocol_sources,
        .physical_budget_bytes = 0,
    };
    private_config.physical_budget_bytes = (try branch_mod.allocation(private_config, origins)).physical_bytes;
    const started = std.Io.Timestamp.now(std.testing.io, .awake);
    var branch = try branch_mod.Session.init(a, private_config, origins);
    defer branch.deinit();
    var logits = try runContextProbeRange(cb, config, &branch, prompt_tokens, study_prompt);
    defer a.free(logits);
    const generated = try a.alloc(i64, @as(usize, generated_rows));
    defer a.free(generated);
    for (generated, 0..) |*token, index| {
        token.* = @intCast(std.mem.indexOfMax(f32, logits));
        const next_logits = try runContextProbeRange(cb, config, &branch, prompt_tokens + study_prompt.len + index, generated[index .. index + 1]);
        a.free(logits);
        logits = next_logits;
    }
    try branch.finish();
    try branch.seal();
    const elapsed_ns: u64 = @intCast(@max(1, started.durationTo(std.Io.Timestamp.now(std.testing.io, .awake)).nanoseconds));
    const prefill_id = try std.fmt.allocPrint(a, "{s}-prefill", .{study_pair_id});
    defer a.free(prefill_id);
    const response_id = try std.fmt.allocPrint(a, "{s}-response", .{study_pair_id});
    defer a.free(response_id);
    for ([_][]const i64{ study_prompt, generated }, [_][]const u8{ prefill_id, response_id }) |tokens, source_id| {
        const token_path = try std.fmt.allocPrint(a, "{s}-{s}-tokens.i64", .{ prefix, source_id });
        defer a.free(token_path);
        const token_file = try std.Io.Dir.cwd().createFile(std.testing.io, token_path, .{ .exclusive = true });
        defer token_file.close(std.testing.io);
        try token_file.writeStreamingAll(std.testing.io, std.mem.sliceAsBytes(tokens));
    }
    const response_start = try std.math.add(u32, @intCast(prompt_tokens), @intCast(study_prompt.len));
    const branch_count: u32 = @intCast(appended);
    for (branch.owners) |owner| if (owner.queries != null) {
        const view = try branch.sealedQueryPartition(owner.layer, partition);
        const pool = owner.queries.?;
        const source_rows = if (partition == .training) pool.shape.train_per_stratum else pool.shape.held_per_stratum;
        const branch_rows = try std.math.mul(u32, source_rows, 2);
        if (branch_rows == 0 or view.rows_per_head % branch_rows != 0) return error.InvalidReferenceReplay;
        const first_positions = (try view.positionHead(0))[0..branch_rows];
        var prefill_rows: u32 = 0;
        for (first_positions) |position| if (position < response_start) {
            prefill_rows += 1;
        };
        const response_rows = branch_rows - prefill_rows;
        if (prefill_rows == 0 or response_rows == 0) return error.InsufficientContextQueries;
        const common = .{
            .partition = if (partition == .training) "training" else "validation",
            .capture_route = "private_model_replay",
            .physical_branch_id = study_pair_id,
            .study_pair_id = study_pair_id,
            .loaded_identity = loaded_identity,
            .branch_logical_start = @as(u32, @intCast(prompt_tokens)),
            .branch_logical_count = branch_count,
            .branch_rows_per_stratum = branch_rows,
            .owner_layer = owner.layer,
            .heads = view.heads,
            .dim = view.dim,
            .physical_bytes = branch.sizes.physical_bytes,
            .query_bytes = branch.sizes.query_bytes,
            .elapsed_ns = elapsed_ns,
            .status_readback_bytes = branch.status_readback_bytes,
            .kv_readback_bytes = branch.kv_readback_bytes,
        };
        const strata = view.rows_per_head / branch_rows;
        try dumpFilteredQuerySourcePartition(prefix, .{
            .source_id = prefill_id,
            .kind = "fixed_study_prefill",
            .conditioning = "completed_prompt_private_replay",
            .logical_start = @intCast(prompt_tokens),
            .logical_count = @intCast(study_prompt.len),
            .rows_per_head = prefill_rows * strata,
            .rows_per_stratum = prefill_rows,
            .generation_contract_sha256 = null,
            .generation_mode = null,
            .partition = common.partition,
            .capture_route = common.capture_route,
            .physical_branch_id = common.physical_branch_id,
            .study_pair_id = common.study_pair_id,
            .loaded_identity = common.loaded_identity,
            .branch_logical_start = common.branch_logical_start,
            .branch_logical_count = common.branch_logical_count,
            .branch_rows_per_stratum = common.branch_rows_per_stratum,
            .owner_layer = common.owner_layer,
            .heads = common.heads,
            .dim = common.dim,
            .physical_bytes = common.physical_bytes,
            .query_bytes = common.query_bytes,
            .elapsed_ns = common.elapsed_ns,
            .status_readback_bytes = common.status_readback_bytes,
            .kv_readback_bytes = common.kv_readback_bytes,
        }, view);
        try dumpFilteredQuerySourcePartition(prefix, .{
            .source_id = response_id,
            .kind = "fixed_study_generated_response",
            .conditioning = "completed_prompt_study_generated_response",
            .logical_start = response_start,
            .logical_count = generated_rows,
            .rows_per_head = response_rows * strata,
            .rows_per_stratum = response_rows,
            .generation_contract_sha256 = generation_contract_sha256,
            .generation_mode = "greedy_fixed_length_ignore_eos",
            .partition = common.partition,
            .capture_route = common.capture_route,
            .physical_branch_id = common.physical_branch_id,
            .study_pair_id = common.study_pair_id,
            .loaded_identity = common.loaded_identity,
            .branch_logical_start = common.branch_logical_start,
            .branch_logical_count = common.branch_logical_count,
            .branch_rows_per_stratum = common.branch_rows_per_stratum,
            .owner_layer = common.owner_layer,
            .heads = common.heads,
            .dim = common.dim,
            .physical_bytes = common.physical_bytes,
            .query_bytes = common.query_bytes,
            .elapsed_ns = common.elapsed_ns,
            .status_readback_bytes = common.status_readback_bytes,
            .kv_readback_bytes = common.kv_readback_bytes,
        }, view);
    };
}

fn encodeRenderedSuffix(a: std.mem.Allocator, tokenizer: anytype, prefix: []const u8, suffix_path: []const u8) ![]i64 {
    const suffix = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, suffix_path, a, .limited(256 * 1024));
    defer a.free(suffix);
    const prefix_ids = try tokenizer.encode(a, prefix);
    defer a.free(prefix_ids);
    const rendered = try a.alloc(u8, try std.math.add(usize, prefix.len, suffix.len));
    defer a.free(rendered);
    @memcpy(rendered[0..prefix.len], prefix);
    @memcpy(rendered[prefix.len..], suffix);
    const all_ids = try tokenizer.encode(a, rendered);
    defer a.free(all_ids);
    if (all_ids.len < prefix_ids.len or !std.mem.eql(i32, prefix_ids, all_ids[0..prefix_ids.len]))
        return error.InvalidReplayCaptureBoundary;
    const suffix_ids = all_ids[prefix_ids.len..];
    if (suffix_ids.len < 8 or suffix_ids.len > 8192) return error.InvalidReplayCaptureLength;
    const result = try a.alloc(i64, suffix_ids.len);
    for (result, suffix_ids) |*id, token| id.* = token;
    return result;
}

fn isLowerHexSha256(value: []const u8) bool {
    if (value.len != 64) return false;
    for (value) |c| if (!std.ascii.isDigit(c) and !(c >= 'a' and c <= 'f')) return false;
    return true;
}

test "context Metal installed Gemma real query compaction smoke" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    const path = std.c.getenv("ANTFLY_CONTEXT_MODEL_DIR") orelse return error.SkipZigTest;
    const enabled = std.c.getenv("ANTFLY_CONTEXT_COMPACTION_PROBE") orelse return error.SkipZigTest;
    if (!std.mem.eql(u8, std.mem.span(enabled), "1")) return error.SkipZigTest;
    const a = std.testing.allocator;
    var backend_manager = backends.SessionManager.initWithIo(a, std.testing.io);
    backend_manager.preferred_backends = &.{.metal};
    backend_manager.required_backend = .metal;
    var manager = manager_mod.ModelManager.init(a, backend_manager);
    defer manager.deinit();
    const model = try manager.loadFromDir(std.mem.span(path));
    const config = factory.getGptConfig(model.session) orelse return error.InvalidModelForGeneration;
    if (config.family != .gemma or config.gemma4_mtp_assistant) return error.InvalidModelForGeneration;
    var cb = try factory.getComputeBackend(model.session, a);
    defer cb.deinit();
    const defaults = if (std.c.getenv("ANTFLY_CONTEXT_PROBE_DEFAULTS")) |value| std.mem.eql(u8, std.mem.span(value), "1") else false;
    const matching = if (defaults) policy.Policy{ .mode = .attention_matching, .max_kv_tokens = 4096 } else policy.Policy{ .mode = .attention_matching, .max_kv_tokens = 2048, .keep_prefix_tokens = 64, .keep_recent_tokens = 512 };
    const ids = try a.alloc(i64, @as(usize, matching.keep_prefix_tokens) + 512 + matching.keep_recent_tokens + 1);
    defer a.free(ids);
    const pattern = [_]i64{ 105, 2364, 108, 107, 235, 106, 109, 108 };
    for (ids, 0..) |*id, i| id.* = if (i == 0) 2 else pattern[(i - 1) % pattern.len];
    if (std.c.getenv("ANTFLY_CONTEXT_PROBE_TEXT_FILE")) |text_path| {
        const bytes = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, std.mem.span(text_path), a, .limited(1024 * 1024));
        defer a.free(bytes);
        const encoded = try model.getTokenizer().encode(a, bytes);
        defer a.free(encoded);
        if (encoded.len < ids.len - 1) return error.ContextProbeTextTooShort;
        for (ids[1..], encoded[0 .. ids.len - 1]) |*id, token| id.* = token;
        std.debug.print("context_compaction_input: kind=text path={s} tokens={d}\n", .{ std.mem.span(text_path), ids.len });
    } else {
        std.debug.print("context_compaction_input: kind=periodic_control_tokens tokens={d}\n", .{ids.len});
    }
    std.debug.print("context_compaction_policy: prefix={d} recent={d} max_kv_tokens={d}\n", .{ matching.keep_prefix_tokens, matching.keep_recent_tokens, matching.max_kv_tokens.? });
    var settings = context.Config{ .plan = try (policy.Policy{}).admit(config.max_position_embeddings, null, ids.len - 1, 1, 0), .layout = try policy.Layout.init(config), .max_query_rows = 128, .physical_budget_bytes = 0 };
    // Charge the full fitter/query/staging envelope at useful context sizes.
    // This is an admission estimate, not a device/RSS or quality benchmark.
    for ([_]usize{ 32768, 65536 }) |logical| {
        if (logical > config.max_position_embeddings) continue;
        var comparison = settings;
        comparison.plan = try (policy.Policy{}).admit(config.max_position_embeddings, null, logical - 1, 1, 0);
        const full = try context.allocation(comparison);
        var bounded = policy.Policy{ .mode = .attention_matching, .max_kv_tokens = @intCast(logical) };
        const protected = @as(usize, bounded.keep_prefix_tokens) + bounded.keep_recent_tokens;
        const boundary = protected + ((logical - protected) / 512) * 512;
        bounded.max_kv_tokens = @intCast(@max(try bounded.retainedRows(logical), (try bounded.retainedRows(boundary - 1)) + 1));
        comparison.plan = try bounded.admit(config.max_position_embeddings, null, logical - 1, 1, 0);
        const fitted = try context.allocation(comparison);
        std.debug.print("context_long_allocation: logical={d} max_kv_tokens={d} exact_bytes={d} matching_bytes={d} staging_bytes={d} query_bytes={d}\n", .{ logical, bounded.max_kv_tokens.?, full.physical_bytes, fitted.physical_bytes, fitted.staging_bytes, fitted.query_bytes });
        try std.testing.expect(fitted.physical_bytes < full.physical_bytes);
    }
    settings.physical_budget_bytes = (try context.allocation(settings)).physical_bytes;
    var exact = try context.Session.init(a, settings);
    defer exact.deinit();
    const baseline = try runContextProbe(&cb, config, &exact, ids);
    defer a.free(baseline);
    settings.plan = try matching.admit(config.max_position_embeddings, null, ids.len - 1, 1, 0);
    settings.physical_budget_bytes = (try context.allocation(settings)).physical_bytes;
    var compact = try context.Session.init(a, settings);
    defer compact.deinit();
    std.debug.print("context_compaction_allocation: exact_bytes={d} matching_bytes={d} query_bytes={d} reserved_rows={d}\n", .{ exact.config.physical_budget_bytes, settings.physical_budget_bytes, compact.metrics.query_bytes, settings.plan.retained_row_limit });
    const actual = runContextProbe(&cb, config, &compact, ids) catch |err| {
        std.debug.print("context_compaction_probe: path={s} rejected={s} epoch={d} rejected_transactions={d} status_readback_bytes={d}\n", .{ std.mem.span(path), @errorName(err), compact.publication.epoch, compact.metrics.rejected_transactions, compact.metrics.status_readback_bytes });
        if (compact.fitter.last_report) |report| std.debug.print("context_compaction_fit: flags={d} iterations={d} training_mass_error={?d} mass_error={?d} output_rmse={?d}\n", .{ report.failure_flags, report.iterations, if (report.evaluated & 1 != 0) report.training_mass_relative_max_error else @as(?f32, null), if (report.evaluated & 2 != 0) report.mass_relative_max_error else @as(?f32, null), if (report.evaluated & 4 != 0) report.output_relative_rmse else @as(?f32, null) });
        try dumpCompactionProbe(&compact);
        return err;
    };
    defer a.free(actual);
    var max_error: f32 = 0;
    for (baseline, actual) |want, got| {
        try std.testing.expect(std.math.isFinite(want) and std.math.isFinite(got));
        max_error = @max(max_error, @abs(want - got));
    }
    if (compact.metrics.compacted_blocks == 0) {
        std.debug.print("context_compaction_probe: path={s} published=false exact_tail_fallbacks={d} logical={d} retained={d} max_logit_abs_error={d} epoch={d}\n", .{ std.mem.span(path), compact.metrics.exact_tail_fallbacks, compact.metrics.logical_tokens, compact.metrics.retained_rows, max_error, compact.metrics.epoch });
        if (compact.fitter.last_report) |report| std.debug.print("context_compaction_fit: flags={d} evaluated={d} training_mass_error={d} mass_error={?d} output_rmse={?d}\n", .{ report.failure_flags, report.evaluated, report.training_mass_relative_max_error, if (report.evaluated & 2 != 0) report.mass_relative_max_error else @as(?f32, null), if (report.evaluated & 4 != 0) report.output_relative_rmse else @as(?f32, null) });
        try dumpCompactionProbe(&compact);
        try std.testing.expectEqual(@as(u32, 1), compact.metrics.exact_tail_fallbacks);
        try std.testing.expectEqual(@as(u32, @intCast(ids.len)), compact.metrics.logical_tokens);
        try std.testing.expect(compact.metrics.retained_rows <= matching.max_kv_tokens.?);
        try std.testing.expect(max_error <= 0.05);
        try std.testing.expectEqual(std.mem.indexOfMax(f32, baseline), std.mem.indexOfMax(f32, actual));
        // Completing with exact KV is safe fallback evidence, not a successful
        // compaction qualification. Keep this opt-in smoke test failing.
        return error.CompactionNotPublished;
    }
    try std.testing.expectEqual(@as(u32, 1), compact.metrics.compacted_blocks);
    try std.testing.expectEqual(@as(u64, 2), compact.metrics.epoch);
    try std.testing.expectEqual(@as(u32, @intCast(ids.len)), compact.metrics.logical_tokens);
    std.debug.print("context_compaction_probe: path={s} tokens={d} blocks={d} retained={d} max_logit_abs_error={d} greedy_match={} context_bytes={d} status_readback_bytes={d} kv_readback_bytes={d}\n", .{ std.mem.span(path), ids.len, compact.metrics.compacted_blocks, compact.metrics.retained_rows, max_error, std.mem.indexOfMax(f32, baseline) == std.mem.indexOfMax(f32, actual), settings.physical_budget_bytes, compact.metrics.status_readback_bytes, compact.metrics.kv_readback_bytes });
    // A candidate passing local attention gates is not sufficient evidence of
    // model-level parity. Apply the same final-logit gate as the exact fallback.
    try std.testing.expect(max_error <= 0.05);
    try std.testing.expectEqual(std.mem.indexOfMax(f32, baseline), std.mem.indexOfMax(f32, actual));
}

fn originalGlobalDigest(session: *const context.Session, rows: usize) ![32]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    for (session.owners.items) |owner| {
        if (owner.shape.sliding_window != 0) continue;
        if (owner.live.rows < rows) return error.MissingContextHistory;
        for (owner.live.spans[0..owner.live.len]) |span| if (span.kind != .exact) return error.InvalidReferenceReplay;
        const bytes = rows * owner.shape.kv_heads * owner.shape.head_dim * 2;
        hash.update(owner.live.keys.bytes[0..bytes]);
        hash.update(owner.live.values.bytes[0..bytes]);
    }
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    return digest;
}

test "context Metal installed Gemma post ingestion replay capture" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    const enabled = std.c.getenv("ANTFLY_CONTEXT_REPLAY_CAPTURE") orelse return error.SkipZigTest;
    if (!std.mem.eql(u8, std.mem.span(enabled), "1")) return error.SkipZigTest;
    const path = std.c.getenv("ANTFLY_CONTEXT_MODEL_DIR") orelse return error.MissingReplayCaptureInput;
    const text_path = std.c.getenv("ANTFLY_CONTEXT_PROBE_TEXT_FILE") orelse return error.MissingReplayCaptureInput;
    const prefix = std.c.getenv("ANTFLY_CONTEXT_PROBE_DUMP_PREFIX") orelse return error.MissingReplayCaptureInput;
    const replay = @import("runtime/kv/reference_replay.zig");
    const a = std.testing.allocator;
    var backend_manager = backends.SessionManager.initWithIo(a, std.testing.io);
    backend_manager.preferred_backends = &.{.metal};
    backend_manager.required_backend = .metal;
    var manager = manager_mod.ModelManager.init(a, backend_manager);
    defer manager.deinit();
    const model = try manager.loadFromDir(std.mem.span(path));
    const config = factory.getGptConfig(model.session) orelse return error.InvalidModelForGeneration;
    if (config.family != .gemma or config.gemma4_mtp_assistant) return error.InvalidModelForGeneration;
    const bytes = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, std.mem.span(text_path), a, .limited(1024 * 1024));
    defer a.free(bytes);
    const encoded = try model.getTokenizer().encode(a, bytes);
    defer a.free(encoded);
    const source_tokens = if (std.c.getenv("ANTFLY_CONTEXT_REPLAY_SOURCE_TOKENS")) |value|
        try std.fmt.parseInt(usize, std.mem.span(value), 10)
    else
        4097;
    if (source_tokens < 1025 or source_tokens > 16384 or encoded.len < source_tokens - 1) return error.InvalidReplayCaptureLength;
    const ids = try a.alloc(i64, source_tokens);
    defer a.free(ids);
    ids[0] = 2;
    for (ids[1..], encoded[0 .. source_tokens - 1]) |*id, token| id.* = token;
    const instruction_ids = try model.getTokenizer().encode(a, replay.instruction);
    defer a.free(instruction_ids);
    const instruction = try a.alloc(i64, instruction_ids.len);
    defer a.free(instruction);
    for (instruction, instruction_ids) |*id, token| id.* = token;
    // Offline raw-text capture has exactly one explicitly selected payload.
    // This does not infer eligibility for rendered chat messages in serving.
    var reference = try replay.prepare(a, ids, &.{.{ .start = 1, .end = @intCast(ids.len) }}, instruction, config.max_position_embeddings);
    defer reference.deinit();
    const total = source_tokens + reference.tokens.len;
    const matching = policy.Policy{ .mode = .attention_matching, .max_kv_tokens = @intCast(total) };
    var settings = context.Config{
        .plan = try matching.admit(config.max_position_embeddings, null, source_tokens, reference.tokens.len, 0),
        .layout = try policy.Layout.init(config),
        .max_query_rows = 128,
        .physical_budget_bytes = 0,
        .calibration_replay_start = @intCast(source_tokens),
    };
    settings.physical_budget_bytes = (try context.allocation(settings)).physical_bytes;
    // A fresh private numerical-probe session owns both ingestion and replay.
    // No production sequence, sampler, grammar, or output/usage object is used.
    // The serving implementation must separately add immutable-prefix sharing.
    var session = try context.Session.init(a, settings);
    defer session.deinit();
    var cb = try factory.getComputeBackend(model.session, a);
    defer cb.deinit();
    const prefill = try runContextProbeRange(&cb, config, &session, 0, ids);
    defer a.free(prefill);
    const original = try originalGlobalDigest(&session, source_tokens);
    for (session.owners.items) |owner| for (owner.positions) |position|
        try std.testing.expectEqual(std.math.maxInt(u32), position);
    const after = try runContextProbeRange(&cb, config, &session, source_tokens, reference.tokens);
    defer a.free(after);
    try std.testing.expectEqualSlices(u8, &original, &(try originalGlobalDigest(&session, source_tokens)));
    try std.testing.expectEqual(@as(u64, 1), session.publication.epoch);
    try std.testing.expectEqual(@as(u32, 0), session.metrics.compacted_blocks);
    try std.testing.expectEqual(@as(u64, 0), session.metrics.rejected_transactions);
    try std.testing.expectEqual(@as(u32, @intCast(total)), session.logical_tokens);
    for (session.owners.items) |owner| {
        if (owner.shape.sliding_window != 0) continue;
        for (owner.positions) |position| try std.testing.expect(position >= source_tokens and position < total);
        for (owner.queries.?.floats()) |value| try std.testing.expect(std.math.isFinite(value));
    }
    for ([_][]const u8{ std.mem.sliceAsBytes(ids), std.mem.sliceAsBytes(reference.tokens), std.mem.sliceAsBytes(reference.source_positions) }, [_][]const u8{ "prompt.i64", "replay.i64", "replay-source-positions.u32" }) |data, suffix| {
        const filename = try std.fmt.allocPrint(a, "{s}-{s}", .{ std.mem.span(prefix), suffix });
        defer a.free(filename);
        try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = filename, .data = data });
    }
    try dumpCompactionProbe(&session);
    std.debug.print("context_reference_capture: model={s} text={s} source_tokens={d} replay_tokens={d} payload_tokens={d} excerpts={d} entire_payload={} context_bytes={d} source_unchanged=true published=false\n", .{ std.mem.span(path), std.mem.span(text_path), source_tokens, reference.tokens.len, reference.payload_tokens, reference.excerpt_count, reference.entire_payload, settings.physical_budget_bytes });
}

test "context Metal installed Gemma original question prefill query capture" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    const enabled = std.c.getenv("ANTFLY_CONTEXT_QUESTION_PREFILL_CAPTURE") orelse return error.SkipZigTest;
    if (!std.mem.eql(u8, std.mem.span(enabled), "1")) return error.SkipZigTest;
    const model_path = std.c.getenv("ANTFLY_CONTEXT_MODEL_DIR") orelse return error.MissingReplayCaptureInput;
    const document_path = std.c.getenv("ANTFLY_CONTEXT_PROBE_TEXT_FILE") orelse return error.MissingReplayCaptureInput;
    const question_path = std.c.getenv("ANTFLY_CONTEXT_QUESTION_FILE") orelse return error.MissingReplayCaptureInput;
    const dump_prefix = std.c.getenv("ANTFLY_CONTEXT_QUERY_SOURCE_DUMP_PREFIX") orelse return error.MissingReplayCaptureInput;
    const a = std.testing.allocator;
    var backend_manager = backends.SessionManager.initWithIo(a, std.testing.io);
    backend_manager.preferred_backends = &.{.metal};
    backend_manager.required_backend = .metal;
    var manager = manager_mod.ModelManager.init(a, backend_manager);
    defer manager.deinit();
    const model = try manager.loadFromDir(std.mem.span(model_path));
    const config = factory.getGptConfig(model.session) orelse return error.InvalidModelForGeneration;
    if (config.family != .gemma or config.gemma4_mtp_assistant) return error.InvalidModelForGeneration;
    const loaded = try model.compactionIdentity(try policy.Layout.init(config), .{
        .algorithm_version = 1,
        .packed_abi_version = 2,
        .kv_dtype = .f16,
        .kernel_route = .metal_context_correctness,
        .block_tokens = 512,
        .solver = .bounded_coordinate_descent,
    });
    const artifact_hex = std.fmt.bytesToHex(loaded.identity.artifact, .lower);
    const tokenizer_hex = std.fmt.bytesToHex(loaded.identity.tokenizer, .lower);
    const template_hex = std.fmt.bytesToHex(loaded.identity.template, .lower);
    const layout_hex = std.fmt.bytesToHex(loaded.identity.layout, .lower);
    const implementation_hex = std.fmt.bytesToHex(loaded.identity.runtime, .lower);
    const loaded_identity = QueryLoadedIdentityObservation{ .load_generation = loaded.load_generation, .artifact_sha256 = artifact_hex[0..], .tokenizer_sha256 = tokenizer_hex[0..], .template_sha256 = template_hex[0..], .layout_sha256 = layout_hex[0..], .implementation_sha256 = implementation_hex[0..] };
    const document = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, std.mem.span(document_path), a, .limited(1024 * 1024));
    defer a.free(document);
    const encoded_document = try model.getTokenizer().encode(a, document);
    defer a.free(encoded_document);
    const source_tokens = if (std.c.getenv("ANTFLY_CONTEXT_REPLAY_SOURCE_TOKENS")) |value|
        try std.fmt.parseInt(usize, std.mem.span(value), 10)
    else
        encoded_document.len + 1;
    if (source_tokens < 1025 or source_tokens > 16384 or encoded_document.len != source_tokens - 1)
        return error.InvalidReplayCaptureLength;
    const prompt = try a.alloc(i64, source_tokens);
    defer a.free(prompt);
    prompt[0] = 2;
    for (prompt[1..], encoded_document[0 .. source_tokens - 1]) |*id, token| id.* = token;
    const question_bytes = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, std.mem.span(question_path), a, .limited(256 * 1024));
    defer a.free(question_bytes);
    const rendered = try a.alloc(u8, try std.math.add(usize, document.len, question_bytes.len));
    defer a.free(rendered);
    @memcpy(rendered[0..document.len], document);
    @memcpy(rendered[document.len..], question_bytes);
    const encoded_rendered = try model.getTokenizer().encode(a, rendered);
    defer a.free(encoded_rendered);
    // A byte split is a valid original-prompt query boundary only when the
    // tokenizer produces exactly the same prefix while encoding the complete
    // rendered prompt. Derive the question token suffix from that one full
    // encoding; never concatenate two independently tokenized fragments.
    if (encoded_rendered.len < encoded_document.len or
        !std.mem.eql(i32, encoded_rendered[0..encoded_document.len], encoded_document))
        return error.InvalidReplayCaptureBoundary;
    const encoded_question = encoded_rendered[encoded_document.len..];
    if (encoded_question.len < 8 or encoded_question.len > 8192 or source_tokens + encoded_question.len > config.max_position_embeddings)
        return error.InvalidReplayCaptureLength;
    const question = try a.alloc(i64, encoded_question.len);
    defer a.free(question);
    for (question, encoded_question) |*id, token| id.* = token;
    const matching = policy.Policy{ .mode = .attention_matching, .max_kv_tokens = @intCast(source_tokens + question.len), .keep_recent_tokens = @intCast(question.len) };
    var settings = context.Config{
        .plan = try matching.admit(config.max_position_embeddings, null, source_tokens, question.len, 0),
        .layout = try policy.Layout.init(config),
        .max_query_rows = 128,
        .physical_budget_bytes = 0,
        .calibration_replay_start = @intCast(source_tokens),
    };
    const allocation = try context.allocation(settings);
    settings.physical_budget_bytes = allocation.physical_bytes;
    var session = try context.Session.init(a, settings);
    defer session.deinit();
    var cb = try factory.getComputeBackend(model.session, a);
    defer cb.deinit();
    const prefill = try runContextProbeRange(&cb, config, &session, 0, prompt);
    defer a.free(prefill);
    const started = std.Io.Timestamp.now(std.testing.io, .awake);
    const question_logits = try runContextProbeRange(&cb, config, &session, source_tokens, question);
    defer a.free(question_logits);
    const elapsed_ns: u64 = @intCast(@max(1, started.durationTo(std.Io.Timestamp.now(std.testing.io, .awake)).nanoseconds));
    try std.testing.expectEqual(@as(u32, @intCast(source_tokens + question.len)), session.logical_tokens);
    try std.testing.expectEqual(@as(u64, 0), session.metrics.kv_readback_bytes);
    const prefix = std.mem.span(dump_prefix);
    const token_path = try std.fmt.allocPrint(a, "{s}-request-question-prefill-tokens.i64", .{prefix});
    defer a.free(token_path);
    const token_file = try std.Io.Dir.cwd().createFile(std.testing.io, token_path, .{ .exclusive = true });
    defer token_file.close(std.testing.io);
    try token_file.writeStreamingAll(std.testing.io, std.mem.sliceAsBytes(question));
    for (session.owners.items) |owner| if (owner.shape.sliding_window == 0) {
        const stride = owner.geometry.training_rows + owner.geometry.validation_rows;
        try dumpQuerySourcePartition(prefix, .{
            .source_id = "request-question-prefill",
            .kind = "request_question_prefill",
            .partition = "training",
            .capture_route = "native_prefill",
            .conditioning = "original_prompt_prefix",
            .physical_branch_id = "request-question-prefill",
            .loaded_identity = loaded_identity,
            .logical_start = @intCast(source_tokens),
            .logical_count = @intCast(question.len),
            .branch_logical_start = @intCast(source_tokens),
            .branch_logical_count = @intCast(question.len),
            .owner_layer = owner.layer,
            .heads = owner.shape.kv_heads,
            .dim = owner.shape.head_dim,
            .rows_per_head = owner.geometry.training_rows,
            .rows_per_stratum = owner.geometry.train_per_stratum,
            .branch_rows_per_stratum = owner.geometry.train_per_stratum,
            .physical_bytes = allocation.physical_bytes,
            .query_bytes = allocation.query_bytes,
            .elapsed_ns = elapsed_ns,
            .status_readback_bytes = session.metrics.attention_status_readback_bytes,
            .kv_readback_bytes = session.metrics.kv_readback_bytes,
        }, stride, 0, owner.queries.?.floats(), owner.positions);
    };
    try dumpCompactionProbe(&session);
    std.debug.print("context_question_prefill_capture: source_tokens={d} question_tokens={d} elapsed_ns={d} peak_bytes={d} query_bytes={d} status_readback_bytes={d} kv_readback_bytes=0 conditioning=original_prompt_position serving_qualified=false\n", .{ source_tokens, question.len, elapsed_ns, allocation.physical_bytes, allocation.query_bytes, session.metrics.attention_status_readback_bytes });
}

test "context Metal installed Gemma payload provenance parity" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    const enabled = std.c.getenv("ANTFLY_CONTEXT_PAYLOAD_PROBE") orelse return error.SkipZigTest;
    if (!std.mem.eql(u8, std.mem.span(enabled), "1")) return error.SkipZigTest;
    const path = std.c.getenv("ANTFLY_CONTEXT_MODEL_DIR") orelse return error.MissingReplayCaptureInput;
    const a = std.testing.allocator;
    var backend_manager = backends.SessionManager.initWithIo(a, std.testing.io);
    backend_manager.preferred_backends = &.{.metal};
    backend_manager.required_backend = .metal;
    var manager = manager_mod.ModelManager.init(a, backend_manager);
    defer manager.deinit();
    const model = try manager.loadFromDir(std.mem.span(path));
    const config = factory.getGptConfig(model.session) orelse return error.InvalidModelForGeneration;
    if (config.family != .gemma or config.gemma4_mtp_assistant) return error.InvalidModelForGeneration;
    const template = model.chat_tmpl orelse return error.MissingChatTemplate;
    var text: std.ArrayListUnmanaged(u8) = .empty;
    defer text.deinit(a);
    for (0..200) |_| try text.appendSlice(a, "  Payload café é 中文. Keep AZURE-731 exactly.\n");
    var settings = policy.Policy{ .mode = .attention_matching, .max_kv_tokens = 8192, .keep_prefix_tokens = 0, .keep_recent_tokens = 1 };
    try settings.compaction_targets.add(.{ .message_index = 1, .part_index = 0 });
    for ([_]bool{ false, true }) |structured| {
        const parts = [_]generation.Message.ContentPart{ .{ .text = text.items }, .{ .text = text.items } };
        const messages = [_]generation.Message{
            .{ .role = "system", .content = text.items },
            .{ .role = "user", .content = if (structured) "" else text.items, .content_parts = if (structured) &parts else null },
        };
        var prepared = try generation.prepareCompactionPayload(a, template, model.getTokenizer(), &messages, .{}, settings, 16384, model.manifest.add_bos_token);
        defer prepared.deinit();
        const ordinary_prompt = try template.apply(a, &messages, true);
        defer a.free(ordinary_prompt);
        var ordinary = try generation.encodeNativeGenerationPrompt(model.getTokenizer(), a, ordinary_prompt, 16384, model.manifest.add_bos_token, model.manifest.bos_token);
        defer ordinary.deinit();
        try std.testing.expectEqualStrings(ordinary_prompt, prepared.rendered.text);
        try std.testing.expectEqualSlices(i32, ordinary.ids, prepared.encoded.ids);
        try std.testing.expectEqualSlices(i32, ordinary.attention_mask, prepared.encoded.attention_mask);
        try std.testing.expect(prepared.eligible.blocks.len > 0);
        var eligible: usize = 0;
        for (prepared.eligible.labels) |label| {
            eligible += @intFromBool(label != 0);
        }
        std.debug.print("context_payload_probe: path={s} structured={} prompt_tokens={d} eligible_tokens={d} blocks={d} rendered_and_token_identity=true published=false\n", .{ std.mem.span(path), structured, prepared.eligible.labels.len, eligible, prepared.eligible.blocks.len });
    }
}

// The migration below belongs only to the parity oracle. Production ingestion
// must create packed owners directly; a legacy-to-packed copy is separately
// charged and must never be reported as a zero-copy replay fork.
test "context Metal installed Gemma shared model replay parity" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    const enabled = std.c.getenv("ANTFLY_CONTEXT_SHARED_REPLAY_PROBE") orelse return error.SkipZigTest;
    if (!std.mem.eql(u8, std.mem.span(enabled), "1")) return error.SkipZigTest;
    const path = std.c.getenv("ANTFLY_CONTEXT_MODEL_DIR") orelse return error.MissingReplayCaptureInput;
    const campaign_capture = std.c.getenv("ANTFLY_CONTEXT_STUDY_CAMPAIGN_DUMP_PREFIX") != null;
    const text_path = std.c.getenv("ANTFLY_CONTEXT_PROBE_TEXT_FILE");
    const branch_mod = @import("backends/model_replay_context.zig");
    const storage = @import("backends/packed_context_storage.zig");
    const views = @import("runtime/kv/context_view.zig");
    const replay = @import("runtime/kv/reference_replay.zig");
    const a = std.testing.allocator;
    var backend_manager = backends.SessionManager.initWithIo(a, std.testing.io);
    backend_manager.preferred_backends = &.{.metal};
    backend_manager.required_backend = .metal;
    var manager = manager_mod.ModelManager.init(a, backend_manager);
    defer manager.deinit();
    const model = try manager.loadFromDir(std.mem.span(path));
    const config = factory.getGptConfig(model.session) orelse return error.InvalidModelForGeneration;
    if (config.family != .gemma or config.gemma4_mtp_assistant) return error.InvalidModelForGeneration;
    const implementation = if (campaign_capture)
        server_mod.contextServingCompactionImplementation(true, true)
    else
        compaction_profiles.Implementation{
            .algorithm_version = 1,
            .packed_abi_version = 2,
            .kv_dtype = .f16,
            .kernel_route = .metal_context_correctness,
            .block_tokens = 512,
            .solver = .bounded_coordinate_descent,
        };
    const loaded = try model.compactionIdentity(try policy.Layout.init(config), implementation);
    const artifact_hex = std.fmt.bytesToHex(loaded.identity.artifact, .lower);
    const tokenizer_hex = std.fmt.bytesToHex(loaded.identity.tokenizer, .lower);
    const template_hex = std.fmt.bytesToHex(loaded.identity.template, .lower);
    const layout_hex = std.fmt.bytesToHex(loaded.identity.layout, .lower);
    const implementation_hex = std.fmt.bytesToHex(loaded.identity.runtime, .lower);
    const loaded_identity = QueryLoadedIdentityObservation{ .load_generation = loaded.load_generation, .artifact_sha256 = artifact_hex[0..], .tokenizer_sha256 = tokenizer_hex[0..], .template_sha256 = template_hex[0..], .layout_sha256 = layout_hex[0..], .implementation_sha256 = implementation_hex[0..] };
    var capture_inputs: ?numerical_capture_inputs.Inputs = null;
    defer if (capture_inputs) |*value| value.deinit();
    if (campaign_capture) {
        const campaign_path = std.c.getenv("ANTFLY_CONTEXT_CAPTURE_CAMPAIGN_DIR") orelse return error.MissingReplayCaptureInput;
        const manifest_path = std.c.getenv("ANTFLY_CONTEXT_CAPTURE_INPUT_MANIFEST") orelse return error.MissingReplayCaptureInput;
        const manifest_sha_text = std.c.getenv("ANTFLY_CONTEXT_CAPTURE_INPUT_MANIFEST_SHA256") orelse return error.MissingReplayCaptureInput;
        const source_receipt = std.c.getenv("ANTFLY_CONTEXT_SOURCE_BUILD_RECEIPT") orelse return error.MissingReplayCaptureInput;
        var manifest_sha: [32]u8 = undefined;
        _ = std.fmt.hexToBytes(&manifest_sha, std.mem.span(manifest_sha_text)) catch return error.InvalidReplayCaptureInput;
        var campaign_dir = try std.Io.Dir.cwd().openDir(std.testing.io, std.mem.span(campaign_path), .{ .follow_symlinks = false });
        defer campaign_dir.close(std.testing.io);
        capture_inputs = try numerical_capture_inputs.load(a, std.testing.io, campaign_dir, std.mem.span(manifest_path), manifest_sha, std.mem.span(source_receipt), .{
            .load_generation = loaded.load_generation,
            .artifact_sha256 = loaded.identity.artifact,
            .tokenizer_sha256 = loaded.identity.tokenizer,
            .template_sha256 = loaded.identity.template,
            .layout_sha256 = loaded.identity.layout,
            .implementation_sha256 = loaded.identity.runtime,
        });
    }
    var text: ?[]u8 = null;
    defer if (text) |bytes| a.free(bytes);
    var encoded: ?[]i32 = null;
    defer if (encoded) |tokens| a.free(tokens);
    var manual_ids: ?[]i64 = null;
    defer if (manual_ids) |tokens| a.free(tokens);
    if (capture_inputs == null) {
        const bytes = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, std.mem.span(text_path orelse return error.MissingReplayCaptureInput), a, .limited(1024 * 1024));
        text = bytes;
        encoded = try model.getTokenizer().encode(a, bytes);
        const source_tokens = if (std.c.getenv("ANTFLY_CONTEXT_REPLAY_SOURCE_TOKENS")) |value|
            try std.fmt.parseInt(usize, std.mem.span(value), 10)
        else
            encoded.?.len + 1;
        if (source_tokens < 1025 or source_tokens > 16384 or encoded.?.len < source_tokens - 1 or
            (std.c.getenv("ANTFLY_CONTEXT_REPLAY_SOURCE_TOKENS") == null and encoded.?.len != source_tokens - 1))
            return error.InvalidReplayCaptureLength;
        manual_ids = try a.alloc(i64, source_tokens);
        manual_ids.?[0] = 2;
        for (manual_ids.?[1..], encoded.?[0 .. source_tokens - 1]) |*id, token| id.* = token;
    }
    const ids: []const i64 = if (capture_inputs) |*value| value.prompt else manual_ids.?;
    const source_tokens = ids.len;
    if (source_tokens < 1025 or source_tokens > 131072) return error.InvalidReplayCaptureLength;
    const source_tokens_u32: u32 = @intCast(source_tokens);
    var encoded_instruction: ?[]i32 = null;
    defer if (encoded_instruction) |tokens| a.free(tokens);
    var instruction: ?[]i64 = null;
    defer if (instruction) |tokens| a.free(tokens);
    var reference: ?replay.Plan = null;
    defer if (reference) |*value| value.deinit();
    if (capture_inputs == null) {
        encoded_instruction = try model.getTokenizer().encode(a, replay.instruction);
        instruction = try a.alloc(i64, encoded_instruction.?.len);
        for (instruction.?, encoded_instruction.?) |*id, token| id.* = token;
        reference = try replay.prepare(a, ids, &.{.{ .start = 1, .end = source_tokens_u32 }}, instruction.?, config.max_position_embeddings);
    }
    var direct_tokens: ?[]i64 = null;
    defer if (direct_tokens) |tokens| a.free(tokens);
    if (capture_inputs == null) {
        if (std.c.getenv("ANTFLY_CONTEXT_QUERY_SOURCE_FILE")) |source_path| {
            direct_tokens = try encodeRenderedSuffix(a, model.getTokenizer(), text.?, std.mem.span(source_path));
        }
    }
    const branch_tokens: []const i64 = if (capture_inputs) |*value| value.question else direct_tokens orelse reference.?.tokens;
    const total = source_tokens + branch_tokens.len;
    const matching = policy.Policy{ .mode = .attention_matching, .max_kv_tokens = @intCast(total), .keep_recent_tokens = @intCast(branch_tokens.len + 3) };
    var settings = context.Config{
        .plan = try matching.admit(config.max_position_embeddings, null, source_tokens, branch_tokens.len, 0),
        .layout = try policy.Layout.init(config),
        .max_query_rows = numerical_capture_inputs.required_prefill_chunk_rows,
        .physical_budget_bytes = 0,
        .calibration_replay_start = source_tokens_u32,
    };
    settings.physical_budget_bytes = (try context.allocation(settings)).physical_bytes;
    var baseline = try context.Session.init(a, settings);
    defer baseline.deinit();
    var budget = memory.RunBudget.init(.{ .host_limit_bytes = 8 * 1024 * 1024 * 1024, .backend_limit_bytes = 8 * 1024 * 1024 * 1024, .combined_limit_bytes = 10 * 1024 * 1024 * 1024 });
    var cb = try factory.getComputeBackendWithBudget(model.session, a, &budget);
    defer cb.deinit();
    const prefill = try runContextProbe(&cb, config, &baseline, ids);
    defer a.free(prefill);
    var copier = try storage.Copier.init();
    defer copier.deinit();
    const originals = try a.alloc(storage.Generation, baseline.owners.items.len);
    defer a.free(originals);
    var original_count: usize = 0;
    defer for (originals[0..original_count]) |*g| g.deinit();
    const origins = try a.alloc(branch_mod.Origin, baseline.owners.items.len);
    defer a.free(origins);
    var migration_bytes: usize = 0;
    for (baseline.owners.items, origins) |owner, *origin| {
        const src = storage.Source{
            .view = .{ .epoch = baseline.publication.epoch, .logical_tokens = source_tokens_u32, .capacity_rows = owner.geometry.capacity, .bias_rows = if (owner.live.mass != null) owner.geometry.capacity else 0, .kv_heads = owner.shape.kv_heads, .head_dim = owner.shape.head_dim, .spans = owner.live.spans[0..owner.live.len] },
            .keys = owner.live.keys.ref(),
            .values = owner.live.values.ref(),
        };
        if (owner.shape.sliding_window > 0) {
            origin.* = .{ .layer = owner.layer, .state = .{ .sliding = src } };
            continue;
        }
        const counts: [views.max_heads]u32 = @splat(source_tokens_u32);
        var plan = try storage.Plan.init(a, .{ .head_dim = owner.shape.head_dim, .kv_heads = owner.shape.kv_heads, .continuation_rows = 1, .max_retained_rows = source_tokens_u32 + 1 }, &.{.{ .logical_count = source_tokens_u32, .retained = counts[0..owner.shape.kv_heads] }});
        originals[original_count] = storage.Generation.init(&copier, plan, plan.sizes.physical_bytes, 0) catch |err| {
            plan.deinit();
            return err;
        };
        const original = &originals[original_count];
        original_count += 1;
        for (0..owner.shape.kv_heads) |head| try original.copyExact(&copier, head, 0, src);
        try original.finish();
        migration_bytes += original.plan.sizes.physical_bytes;
        origin.* = .{ .layer = owner.layer, .state = .{ .global = original } };
    }
    var private_config = branch_mod.Config{ .plan = try (policy.Policy{}).admit(config.max_position_embeddings, null, source_tokens, branch_tokens.len, 0), .layout = settings.layout, .max_query_rows = settings.max_query_rows, .physical_budget_bytes = 0 };
    private_config.physical_budget_bytes = (try branch_mod.allocation(private_config, origins)).physical_bytes;
    const query_protocol = @import("backends/context_query_protocol.zig");
    var campaign_sources: [query_protocol.max_sources]query_protocol.Source = undefined;
    const initial_digest = packedOriginalDigest(originals[0..original_count]);
    // Sliding origins borrow the exact baseline's spans. Run every private
    // study while that seed still describes source_tokens; the parity branch
    // below appends to baseline and intentionally mutates those live spans.
    if (std.c.getenv("ANTFLY_CONTEXT_STUDY_CAMPAIGN_DUMP_PREFIX")) |campaign_prefix_raw| {
        const capture = if (capture_inputs) |*value| value else return error.MissingReplayCaptureInput;
        const fit_study = capture.fit;
        const gate_study = capture.gate;
        const contract_hex = std.fmt.bytesToHex(capture.generation_contract_sha256, .lower);
        const contract_sha = contract_hex[0..];
        const fit_generated_rows = capture.fit_response_tokens;
        const gate_generated_rows = capture.gate_response_tokens;
        const prompt = source_tokens_u32;
        const question_end = try std.math.add(u32, prompt, @intCast(branch_tokens.len));
        const fit_end = try std.math.add(u32, prompt, @intCast(fit_study.len));
        const gate_end = try std.math.add(u32, prompt, @intCast(gate_study.len));
        campaign_sources = .{
            .{ .kind = .request_question_replay, .partition = .training, .start = prompt, .end = question_end, .active = true },
            .{ .kind = .fixed_study_prefill, .partition = .training, .start = prompt, .end = fit_end, .active = false },
            .{ .kind = .fixed_study_generated_response, .partition = .training, .start = fit_end, .end = try std.math.add(u32, fit_end, fit_generated_rows), .active = false },
            .{ .kind = .fixed_study_prefill, .partition = .validation, .start = prompt, .end = gate_end, .active = false },
            .{ .kind = .fixed_study_generated_response, .partition = .validation, .start = gate_end, .end = try std.math.add(u32, gate_end, gate_generated_rows), .active = false },
        };
        private_config.query_sources = &campaign_sources;
        private_config.physical_budget_bytes = (try branch_mod.allocation(private_config, origins)).physical_bytes;
        const campaign_prefix = std.mem.span(campaign_prefix_raw);
        const seed_digest = legacyStateDigest(&baseline);
        try runPrivateStudyPair(a, &cb, config, settings.layout, origins, source_tokens, fit_study, branch_tokens.len, fit_study.len, gate_study.len, fit_generated_rows, .training, campaign_prefix, "fit-study", contract_sha, loaded_identity);
        try std.testing.expectEqualSlices(u8, &initial_digest, &packedOriginalDigest(originals[0..original_count]));
        try std.testing.expectEqualSlices(u8, &seed_digest, &legacyStateDigest(&baseline));
        try runPrivateStudyPair(a, &cb, config, settings.layout, origins, source_tokens, gate_study, branch_tokens.len, fit_study.len, gate_study.len, gate_generated_rows, .validation, campaign_prefix, "gate-study", contract_sha, loaded_identity);
        try std.testing.expectEqualSlices(u8, &initial_digest, &packedOriginalDigest(originals[0..original_count]));
        try std.testing.expectEqualSlices(u8, &seed_digest, &legacyStateDigest(&baseline));
    }
    const replay_started = std.Io.Timestamp.now(std.testing.io, .awake);
    var branch = try branch_mod.Session.init(a, private_config, origins);
    var branch_alive = true;
    defer if (branch_alive) branch.deinit();
    for (origins, branch.owners) |origin, owner| if (origin.state == .global) {
        try std.testing.expectEqual(origin.state.global.keys.handle, owner.state.global.lease.source.keys.handle);
    };
    var max_logit_error: f32 = 0;
    var greedy_matches: usize = 0;
    var start: usize = 0;
    while (start < branch_tokens.len) {
        const position = source_tokens + start;
        const rows = try branch.hook().nextChunk(@intCast(position), @intCast(branch_tokens.len - start));
        try std.testing.expectEqual(rows, try baseline.hook().nextChunk(@intCast(position), @intCast(branch_tokens.len - start)));
        const old_baseline = legacyStateDigest(&baseline);
        const before_position = baseline.logical_tokens;
        const dc = gpt.DecodeContext{ .context_session = branch.hook(), .attention_mode = .paged_prefill, .total_sequence_len = position + rows, .query_sequence_len = rows, .kv_sequence_len = position + rows };
        const actual = try gpt.forward(&cb, a, config, branch_tokens[start..][0..rows], 1, position + rows, &dc);
        defer a.free(actual);
        // Replay changes neither the live/reference source nor its position.
        try std.testing.expectEqual(before_position, baseline.logical_tokens);
        try std.testing.expectEqualSlices(u8, &old_baseline, &legacyStateDigest(&baseline));
        try std.testing.expectEqualSlices(u8, &initial_digest, &packedOriginalDigest(originals[0..original_count]));
        var baseline_dc = dc;
        baseline_dc.context_session = baseline.hook();
        const expected = try gpt.forward(&cb, a, config, branch_tokens[start..][0..rows], 1, position + rows, &baseline_dc);
        defer a.free(expected);
        try std.testing.expectEqual(expected.len, actual.len);
        for (actual, expected) |got, want| {
            try std.testing.expect(std.math.isFinite(got) and std.math.isFinite(want));
            max_logit_error = @max(max_logit_error, @abs(got - want));
        }
        for (0..rows) |row| {
            const offset = row * config.vocab_size;
            try std.testing.expectEqual(std.mem.indexOfMax(f32, expected[offset..][0..config.vocab_size]), std.mem.indexOfMax(f32, actual[offset..][0..config.vocab_size]));
            greedy_matches += 1;
        }
        start += rows;
    }
    try branch.finish();
    try std.testing.expect(max_logit_error <= 0.05);
    try branch.seal();
    const replay_elapsed_ns: u64 = @intCast(@max(1, replay_started.durationTo(std.Io.Timestamp.now(std.testing.io, .awake)).nanoseconds));
    if (std.c.getenv("ANTFLY_CONTEXT_QUERY_SOURCE_DUMP_PREFIX")) |dump_prefix| {
        const prefix = std.mem.span(dump_prefix);
        const source_id = if (std.c.getenv("ANTFLY_CONTEXT_QUERY_SOURCE_ID")) |value| std.mem.span(value) else "request-question-replay";
        const source_kind = if (std.c.getenv("ANTFLY_CONTEXT_QUERY_SOURCE_KIND")) |value| std.mem.span(value) else "request_question_replay";
        const partition_name = if (std.c.getenv("ANTFLY_CONTEXT_QUERY_SOURCE_PARTITION")) |value| std.mem.span(value) else "training";
        const partition: branch_mod.QueryPartition = if (std.mem.eql(u8, partition_name, "training")) .training else if (std.mem.eql(u8, partition_name, "validation")) .validation else return error.InvalidReferenceReplay;
        // The default reference branch contains an instruction plus document
        // excerpts. It is not the request question and must never be emitted
        // under request-question provenance.
        if (direct_tokens == null) return error.InvalidReferenceReplay;
        if (std.mem.eql(u8, source_kind, "request_question_replay")) {
            if (!std.mem.eql(u8, source_id, "request-question-replay") or partition != .training)
                return error.InvalidReferenceReplay;
        } else if (!std.mem.eql(u8, source_kind, "fixed_study") and
            !std.mem.eql(u8, source_kind, "exact_teacher_continuation")) return error.InvalidReferenceReplay;
        const token_path = try std.fmt.allocPrint(a, "{s}-{s}-tokens.i64", .{ prefix, source_id });
        defer a.free(token_path);
        const token_file = try std.Io.Dir.cwd().createFile(std.testing.io, token_path, .{ .exclusive = true });
        defer token_file.close(std.testing.io);
        try token_file.writeStreamingAll(std.testing.io, std.mem.sliceAsBytes(branch_tokens));
        for (branch.owners) |owner| if (owner.queries != null) {
            const view = try branch.sealedQueryPartition(owner.layer, partition);
            const pool = owner.queries.?;
            try dumpQuerySourcePartition(prefix, .{
                .source_id = source_id,
                .kind = source_kind,
                .partition = partition_name,
                .capture_route = "private_model_replay",
                .conditioning = "completed_prompt_private_replay",
                .physical_branch_id = source_id,
                .loaded_identity = loaded_identity,
                .logical_start = source_tokens_u32,
                .logical_count = @intCast(branch_tokens.len),
                .branch_logical_start = source_tokens_u32,
                .branch_logical_count = @intCast(branch_tokens.len),
                .owner_layer = owner.layer,
                .heads = view.heads,
                .dim = view.dim,
                .rows_per_head = view.rows_per_head,
                .rows_per_stratum = if (partition == .training) pool.shape.train_per_stratum else pool.shape.held_per_stratum,
                .branch_rows_per_stratum = if (partition == .training) pool.shape.train_per_stratum else pool.shape.held_per_stratum,
                .physical_bytes = branch.sizes.physical_bytes,
                .query_bytes = branch.sizes.query_bytes,
                .elapsed_ns = replay_elapsed_ns,
                .status_readback_bytes = branch.status_readback_bytes,
                .kv_readback_bytes = branch.kv_readback_bytes,
            }, view.stride_rows, view.row_offset, view.queries, view.positions);
        };
    }
    for (originals[0..original_count]) |*original| try std.testing.expect(!original.hasReplayReaders());
    var max_query_error: f32 = 0;
    var max_query_relative_rmse: f64 = 0;
    var query_vectors: usize = 0;
    for (baseline.owners.items, branch.owners) |expected, got| if (got.queries) |pool| {
        const sealed = try branch.sealedQueries(got.layer);
        try std.testing.expectEqual(pool.queries.handle, sealed.queries.handle);
        try std.testing.expectEqualSlices(u32, expected.positions, pool.positions);
        try std.testing.expectEqual(expected.queries.?.floats().len, pool.queries.floats().len);
        // Check every captured vector, including each donor/query-head stratum.
        const dim = expected.shape.head_dim;
        for (0..pool.positions.len) |row| {
            var squared_error: f64 = 0;
            var squared_reference: f64 = 0;
            for (expected.queries.?.floats()[row * dim ..][0..dim], pool.queries.floats()[row * dim ..][0..dim]) |want, value| {
                try std.testing.expect(std.math.isFinite(want) and std.math.isFinite(value));
                max_query_error = @max(max_query_error, @abs(want - value));
                squared_error += std.math.pow(f64, @as(f64, value) - want, 2);
                squared_reference += @as(f64, want) * want;
            }
            max_query_relative_rmse = @max(max_query_relative_rmse, @sqrt(squared_error / @max(squared_reference, 1e-20)));
        }
        query_vectors += pool.positions.len;
    };
    try std.testing.expect(max_query_relative_rmse <= 0.001);
    try std.testing.expectEqual(@as(u64, 0), branch.kv_readback_bytes);
    try std.testing.expectEqual(@as(u64, 1), baseline.publication.epoch);
    const seed_copy_bytes = branch.sizes.seed_copy_bytes;
    const readback = branch.status_readback_bytes;
    branch.deinit();
    branch_alive = false;
    for (originals[0..original_count]) |*original| try std.testing.expect(!original.hasReplayReaders());
    try std.testing.expectEqualSlices(u8, &initial_digest, &packedOriginalDigest(originals[0..original_count]));
    std.debug.print("context_shared_model_replay: source_tokens={d} replay_tokens={d} layers={d} global_owners={d} greedy_matches={d} max_logit_abs_error={d} query_vectors={d} max_query_abs_error={d} max_query_relative_rmse={d} branch_peak_bytes={d} legacy_migration_bytes={d} sliding_seed_copy_bytes={d} global_fork_copy_bytes=0 status_readback_bytes={d} kv_readback_bytes=0 original_unchanged=true leases_released=true serving_qualified=false\n", .{ source_tokens, branch_tokens.len, config.num_hidden_layers, original_count, greedy_matches, max_logit_error, query_vectors, max_query_error, max_query_relative_rmse, private_config.physical_budget_bytes, migration_bytes, seed_copy_bytes, readback });
}

fn packedOriginalDigest(originals: []const @import("backends/packed_context_storage.zig").Generation) [32]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    for (originals) |g| {
        hash.update(g.keys.bytes[0..g.keys.logical_bytes]);
        hash.update(g.values.bytes[0..g.values.logical_bytes]);
        hash.update(std.mem.sliceAsBytes(g.plan.heads));
        hash.update(std.mem.sliceAsBytes(g.plan.spans));
        hash.update(std.mem.asBytes(&g.plan.descriptor.logical_tokens));
        hash.update(std.mem.asBytes(&g.plan.descriptor.epoch));
    }
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    return digest;
}
fn legacyStateDigest(session: *const context.Session) [32]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    for (session.owners.items) |o| {
        hash.update(o.live.keys.bytes[0..o.live.keys.logical_bytes]);
        hash.update(o.live.values.bytes[0..o.live.values.logical_bytes]);
        hash.update(std.mem.sliceAsBytes(o.live.spans[0..o.live.len]));
        hash.update(std.mem.sliceAsBytes(o.positions));
        if (o.queries) |q| hash.update(q.bytes[0..q.logical_bytes]);
        if (o.live.mass) |mass| hash.update(mass.bytes[0..mass.logical_bytes]);
    }
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    return digest;
}

// Direct owners ingest the actual rendered token sequence, fork from those
// same allocations, then continue the user sequence after discarding replay.
// This proves exact ownership/integration; it is not a lossy quality campaign.
test "context Metal installed Gemma direct packed ingestion replay and continuation" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    const enabled = std.c.getenv("ANTFLY_CONTEXT_PACKED_LIFECYCLE_PROBE") orelse return error.SkipZigTest;
    if (!std.mem.eql(u8, std.mem.span(enabled), "1")) return error.SkipZigTest;
    const path = std.c.getenv("ANTFLY_CONTEXT_MODEL_DIR") orelse return error.MissingReplayCaptureInput;
    const text_path = std.c.getenv("ANTFLY_CONTEXT_PROBE_TEXT_FILE") orelse return error.MissingReplayCaptureInput;
    const packed_session = @import("backends/packed_context_session.zig");
    const reference_replay = @import("runtime/kv/reference_replay.zig");
    const a = std.testing.allocator;
    var backend_manager = backends.SessionManager.initWithIo(a, std.testing.io);
    backend_manager.preferred_backends = &.{.metal};
    backend_manager.required_backend = .metal;
    var manager = manager_mod.ModelManager.init(a, backend_manager);
    defer manager.deinit();
    const model = try manager.loadFromDir(std.mem.span(path));
    const config = factory.getGptConfig(model.session) orelse return error.InvalidModelForGeneration;
    if (config.family != .gemma or config.gemma4_mtp_assistant) return error.InvalidModelForGeneration;
    const text = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, std.mem.span(text_path), a, .limited(1024 * 1024));
    defer a.free(text);
    // Fixed byte excerpt; selected user text is surrounded by repeated system
    // and user instructions so substring ownership would be unsafe.
    const messages = [_]generation.Message{
        .{ .role = "system", .content = "Preserve AZURE-731. Answer only the user's final question." },
        .{ .role = "user", .content = text[0..@min(text.len, 4096)] },
        .{ .role = "user", .content = "Preserve AZURE-731. What is the marker?" },
    };
    var selection = policy.Policy{ .mode = .attention_matching, .max_kv_tokens = 8192, .keep_prefix_tokens = 32, .keep_recent_tokens = 32 };
    try selection.compaction_targets.add(.{ .message_index = 1, .part_index = 0 });
    const template = model.chat_tmpl orelse return error.MissingChatTemplate;
    var prepared = try generation.prepareCompactionPayload(a, template, model.getTokenizer(), &messages, .{ .enable_thinking = false }, selection, 8192, model.manifest.add_bos_token);
    defer prepared.deinit();
    try std.testing.expect(prepared.eligible.blocks.len > 0);
    const ids = try a.alloc(i64, prepared.eligible.labels.len);
    defer a.free(ids);
    for (ids, prepared.encoded.ids[0..ids.len]) |*id, token| id.* = token;
    const raw_instruction = try model.getTokenizer().encode(a, reference_replay.instruction);
    defer a.free(raw_instruction);
    const instruction = try a.alloc(i64, raw_instruction.len);
    defer a.free(instruction);
    for (instruction, raw_instruction) |*id, token| id.* = token;
    var replay_plan = try reference_replay.prepare(a, ids, prepared.eligible.ranges, instruction, config.max_position_embeddings);
    defer replay_plan.deinit();
    var settings = packed_session.Config{ .plan = try (policy.Policy{}).admit(config.max_position_embeddings, null, ids.len, 4, 0), .layout = try policy.Layout.init(config), .max_query_rows = 64, .physical_budget_bytes = 0 };
    settings.physical_budget_bytes = (try packed_session.allocation(settings)).physical_bytes;
    var live = try packed_session.Session.init(a, settings);
    defer live.deinit();
    var baseline_settings = context.Config{ .plan = settings.plan, .layout = settings.layout, .max_query_rows = 64, .physical_budget_bytes = 0 };
    baseline_settings.physical_budget_bytes = (try context.allocation(baseline_settings)).physical_bytes;
    var baseline = try context.Session.init(a, baseline_settings);
    defer baseline.deinit();
    var budget = memory.RunBudget.init(.{ .host_limit_bytes = 8 * 1024 * 1024 * 1024, .backend_limit_bytes = 8 * 1024 * 1024 * 1024, .combined_limit_bytes = 10 * 1024 * 1024 * 1024 });
    var cb = try factory.getComputeBackendWithBudget(model.session, a, &budget);
    defer cb.deinit();
    const expected_prefill = try runContextProbe(&cb, config, &baseline, ids);
    defer a.free(expected_prefill);
    const actual_prefill = try runContextProbe(&cb, config, &live, ids);
    defer a.free(actual_prefill);
    var max_logit_error: f32 = 0;
    for (actual_prefill, expected_prefill) |got, want| {
        try std.testing.expect(std.math.isFinite(got) and std.math.isFinite(want));
        max_logit_error = @max(max_logit_error, @abs(got - want));
    }
    try std.testing.expectEqual(std.mem.indexOfMax(f32, expected_prefill), std.mem.indexOfMax(f32, actual_prefill));
    const initial_digest = packedOriginalDigest(live.globals);
    const old_baseline = legacyStateDigest(&baseline);
    var maintenance = try live.beginReplay(@intCast(replay_plan.tokens.len), 1024 * 1024 * 1024);
    var active = true;
    defer if (active) maintenance.deinit();
    try std.testing.expectError(error.ContextReplayActive, live.hook().nextChunk(@intCast(ids.len), 1));
    const replay_logits = try runContextProbeRange(&cb, config, &maintenance.branch, ids.len, replay_plan.tokens);
    defer a.free(replay_logits);
    try maintenance.seal();
    try std.testing.expectEqual(@as(u32, @intCast(ids.len)), live.logical_tokens);
    try std.testing.expectEqualSlices(u8, &initial_digest, &packedOriginalDigest(live.globals));
    try std.testing.expectEqualSlices(u8, &old_baseline, &legacyStateDigest(&baseline));
    var query_vectors: usize = 0;
    for (maintenance.branch.owners) |owner| if (owner.queries) |pool| {
        query_vectors += pool.positions.len;
    };
    const branch_bytes = maintenance.branch.sizes.physical_bytes;
    const seed_bytes = maintenance.branch.sizes.seed_copy_bytes;
    maintenance.deinit();
    active = false;
    for (live.globals) |*g| try std.testing.expect(!g.hasReplayReaders());
    var next: i64 = @intCast(std.mem.indexOfMax(f32, expected_prefill));
    for (0..4) |index| {
        const position = ids.len + index;
        const expected = try runContextProbeRange(&cb, config, &baseline, position, &.{next});
        defer a.free(expected);
        const actual = try runContextProbeRange(&cb, config, &live, position, &.{next});
        defer a.free(actual);
        for (actual, expected) |got, want| {
            try std.testing.expect(std.math.isFinite(got) and std.math.isFinite(want));
            max_logit_error = @max(max_logit_error, @abs(got - want));
        }
        try std.testing.expectEqual(std.mem.indexOfMax(f32, expected), std.mem.indexOfMax(f32, actual));
        next = @intCast(std.mem.indexOfMax(f32, expected));
    }
    try std.testing.expect(max_logit_error <= 0.05);
    try std.testing.expectEqual(@as(u64, 0), live.metrics.kv_readback_bytes);
    std.debug.print("context_direct_packed_model: prompt_tokens={d} replay_tokens={d} continuation_tokens=4 layers={d} global_owners={d} eligible_blocks={d} query_vectors={d} max_logit_abs_error={d} live_bytes={d} branch_bytes={d} sliding_seed_copy_bytes={d} legacy_migration_bytes=0 global_fork_copy_bytes=0 kv_readback_bytes=0 original_unchanged=true leases_released=true serving_qualified=false\n", .{ ids.len, replay_plan.tokens.len, config.num_hidden_layers, live.globals.len, prepared.eligible.blocks.len, query_vectors, max_logit_error, live.metrics.live_bytes, branch_bytes, seed_bytes });
}

fn parseStrictCampaignQualificationBool(raw: []const u8) !bool {
    if (std.mem.eql(u8, raw, "1")) return true;
    return error.InvalidReplayCaptureInput;
}

fn strictCampaignQualificationEnvBool(name: [*:0]const u8) !bool {
    const raw = std.c.getenv(name) orelse return false;
    return parseStrictCampaignQualificationBool(std.mem.span(raw));
}

test "context campaign qualification boolean is strict" {
    try std.testing.expect(try parseStrictCampaignQualificationBool("1"));
    try std.testing.expectError(error.InvalidReplayCaptureInput, parseStrictCampaignQualificationBool("true"));
    try std.testing.expectError(error.InvalidReplayCaptureInput, parseStrictCampaignQualificationBool("0"));
    try std.testing.expectError(error.InvalidReplayCaptureInput, parseStrictCampaignQualificationBool(""));
}

fn campaignTokenDigest(tokens: []const i64) [32]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    for (tokens) |token| {
        var encoded: [8]u8 = undefined;
        std.mem.writeInt(i64, &encoded, token, .little);
        hash.update(&encoded);
    }
    var result: [32]u8 = undefined;
    hash.final(&result);
    return result;
}

// Installed-model numerical evidence path. It ingests the exact sealed
// coverage prompt directly into packed storage and invokes the same bounded
// question -> fit -> gate campaign routine used by serving. The observer is
// test-only, cannot publish, and writes raw evidence before request teardown.
test "context Metal installed Gemma production query campaign capture" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    const enabled = std.c.getenv("ANTFLY_CONTEXT_CAMPAIGN_CAPTURE") orelse return error.SkipZigTest;
    if (!std.mem.eql(u8, std.mem.span(enabled), "1")) return error.SkipZigTest;
    const model_path = std.c.getenv("ANTFLY_CONTEXT_MODEL_DIR") orelse return error.MissingReplayCaptureInput;
    const campaign_path = std.c.getenv("ANTFLY_CONTEXT_CAPTURE_CAMPAIGN_DIR") orelse return error.MissingReplayCaptureInput;
    const manifest_path = std.c.getenv("ANTFLY_CONTEXT_CAPTURE_INPUT_MANIFEST") orelse return error.MissingReplayCaptureInput;
    const manifest_sha_text = std.c.getenv("ANTFLY_CONTEXT_CAPTURE_INPUT_MANIFEST_SHA256") orelse return error.MissingReplayCaptureInput;
    const source_receipt = std.c.getenv("ANTFLY_CONTEXT_SOURCE_BUILD_RECEIPT") orelse return error.MissingReplayCaptureInput;
    const capture_stem = std.c.getenv("ANTFLY_CONTEXT_CAMPAIGN_CAPTURE_STEM") orelse return error.MissingReplayCaptureInput;
    const peak_text = std.c.getenv("ANTFLY_CONTEXT_CAMPAIGN_PEAK_BYTES") orelse return error.MissingReplayCaptureInput;
    const peak_budget = try std.fmt.parseInt(usize, std.mem.span(peak_text), 10);
    if (peak_budget < 16 * 1024 * 1024 or peak_budget > 8 * 1024 * 1024 * 1024)
        return error.InvalidReplayCaptureInput;

    const a = std.testing.allocator;
    var backend_manager = backends.SessionManager.initWithIo(a, std.testing.io);
    backend_manager.preferred_backends = &.{.metal};
    backend_manager.required_backend = .metal;
    var manager = manager_mod.ModelManager.init(a, backend_manager);
    defer manager.deinit();
    const model = try manager.loadFromDir(std.mem.span(model_path));
    const config = factory.getGptConfig(model.session) orelse return error.InvalidModelForGeneration;
    if (config.family != .gemma or config.gemma4_mtp_assistant) return error.InvalidModelForGeneration;
    const layout = try policy.Layout.init(config);
    const implementation = server_mod.contextServingCompactionImplementation(true, true);
    const actual = try model.compactionIdentity(layout, implementation);

    var manifest_sha: [32]u8 = undefined;
    if (std.mem.span(manifest_sha_text).len != 64) return error.InvalidReplayCaptureInput;
    _ = std.fmt.hexToBytes(&manifest_sha, std.mem.span(manifest_sha_text)) catch return error.InvalidReplayCaptureInput;
    var campaign_dir = try std.Io.Dir.cwd().openDir(std.testing.io, std.mem.span(campaign_path), .{ .follow_symlinks = false });
    defer campaign_dir.close(std.testing.io);
    var inputs = try numerical_capture_inputs.load(a, std.testing.io, campaign_dir, std.mem.span(manifest_path), manifest_sha, std.mem.span(source_receipt), .{
        .load_generation = actual.load_generation,
        .artifact_sha256 = actual.identity.artifact,
        .tokenizer_sha256 = actual.identity.tokenizer,
        .template_sha256 = actual.identity.template,
        .layout_sha256 = actual.identity.layout,
        .implementation_sha256 = actual.identity.runtime,
    });
    defer inputs.deinit();
    const qualified_context_tokens = try std.math.add(u32, @intCast(inputs.prompt.len), inputs.reserved_output_tokens);
    if (inputs.qualified_context_tokens != qualified_context_tokens or
        inputs.qualified_context_tokens > @as(u32, @intCast(config.max_position_embeddings)))
        return error.InvalidReplayCaptureLength;

    var authorities = try campaign_protocol.captureAuthorities(a, layout, campaign_studies.authority(.fit), campaign_studies.authority(.gate));
    defer authorities.deinit();
    const sources = campaign_coordinator.PreparedQuerySources{
        .question = .{
            .ids = inputs.question,
            .source_start = inputs.question_source_start,
            .source_end = inputs.question_source_end,
            .prompt_tokens = @intCast(inputs.prompt.len),
            .prompt_tokens_sha256 = inputs.prompt_tokens_sha256,
        },
        .fit = .{
            .ids = inputs.fit,
            .template_bytes = campaign_studies.template(.fit),
            .generation_contract_bytes = campaign_studies.generation_contract,
            .prompt_tokens = @intCast(inputs.prompt.len),
            .prompt_tokens_sha256 = inputs.prompt_tokens_sha256,
            .response_tokens = inputs.fit_response_tokens,
        },
        .gate = .{
            .ids = inputs.gate,
            .template_bytes = campaign_studies.template(.gate),
            .generation_contract_bytes = campaign_studies.generation_contract,
            .prompt_tokens = @intCast(inputs.prompt.len),
            .prompt_tokens_sha256 = inputs.prompt_tokens_sha256,
            .response_tokens = inputs.gate_response_tokens,
        },
        .loaded_private_position_limit = config.max_position_embeddings,
    };

    var execution_pool = try context_execution_pool.Pool.init(actual.load_generation, context_execution_pool.admitted_backend_bytes);
    defer execution_pool.deinit();
    try execution_pool.warm(null);
    var execution_lease = try execution_pool.acquire(actual.load_generation);
    var lease_active = true;
    defer if (lease_active) execution_lease.poisonAndRelease();
    const capture_policy = policy.Policy{
        .max_context_tokens = inputs.qualified_context_tokens,
        .max_kv_tokens = inputs.qualified_context_tokens,
    };
    const capture_plan = try capture_policy.admit(config.max_position_embeddings, null, inputs.prompt.len, @as(usize, inputs.reserved_output_tokens), 0);
    const qualified_context_limit: usize = inputs.qualified_context_tokens;
    if (capture_plan.context_limit != qualified_context_limit or
        capture_plan.logical_token_limit != qualified_context_limit or
        capture_plan.retained_row_limit != qualified_context_limit)
        return error.InvalidReplayCaptureLength;
    var settings = packed_session_mod.Config{
        .plan = capture_plan,
        .layout = layout,
        .max_query_rows = 64,
        .physical_budget_bytes = 0,
        .execution_lease = &execution_lease,
        .optimize_decode = true,
        .optimize_prefill = true,
    };
    settings.physical_budget_bytes = (try packed_session_mod.allocation(settings)).physical_bytes;
    var live = try packed_session_mod.Session.init(a, settings);
    var live_active = true;
    defer if (live_active) live.deinit();

    var budget = memory.RunBudget.init(.{ .host_limit_bytes = 8 * 1024 * 1024 * 1024, .backend_limit_bytes = 8 * 1024 * 1024 * 1024, .combined_limit_bytes = 10 * 1024 * 1024 * 1024 });
    var cb = try factory.getComputeBackendWithBudget(model.session, a, &budget);
    defer cb.deinit();
    var decode_state = generation.NativeDecodeState.initContiguous(a);
    defer decode_state.deinit();
    decode_state.context_session = live.hook();
    var pipeline = generation.NativeGenerationPipeline{
        .allocator = a,
        .io = std.testing.io,
        .cb = cb,
        .session = model.session,
        .load_generation = actual.load_generation,
        .gpt_config = config,
        .kv_dtype = .f16,
        .tokenizer = model.getTokenizer(),
        .chat_template = model.chat_tmpl,
        .add_bos_token = model.manifest.add_bos_token,
        .bos_token = model.manifest.bos_token,
        .decode_state = &decode_state,
        .return_raw_token_ids = true,
    };
    const prefill_config = generation.GenerationConfig{
        .context_storage = .packed_exact,
        .context_budget_bytes = settings.physical_budget_bytes,
        .context_flash_prefill = true,
        .context_split_decode = true,
        .prefill_chunk_size = numerical_capture_inputs.required_prefill_chunk_rows,
        .max_tokens = std.math.cast(i32, inputs.reserved_output_tokens) orelse return error.InvalidReplayCaptureLength,
        .ignore_eos = true,
    };
    try pipeline.runCompactionCapturePrefill(inputs.prompt, &decode_state, prefill_config, inputs.admitted_prefill_chunk_rows);
    try std.testing.expectEqual(@as(u32, @intCast(inputs.prompt.len)), live.logical_tokens);
    if (live.flash_prefill_dispatches == 0) return error.InvalidCampaignCapture;
    var exporter = campaign_capture_mod.Capture{
        .allocator = a,
        .io = std.testing.io,
        .directory = campaign_dir,
        .stem = std.mem.span(capture_stem),
        .layout = layout,
        .identity = actual.identity,
        .provenance = .{ .binary_sha256 = inputs.binary_sha256, .source_build_sha256 = inputs.source_build_sha256 },
        .capture_inputs_manifest_sha256 = manifest_sha,
        .coverage_receipt_sha256 = inputs.coverage_receipt_sha256,
        .request_content_sha256 = inputs.request_content_sha256,
        .prompt_tokens = @intCast(inputs.prompt.len),
        .prompt_tokens_sha256 = inputs.prompt_tokens_sha256,
        .question_source_start = inputs.question_source_start,
        .question_source_end = inputs.question_source_end,
        .question_tokens_sha256 = campaignTokenDigest(inputs.question),
        .admitted_prefill_chunk_rows = inputs.admitted_prefill_chunk_rows,
        .prefix_flash_prefill_dispatches = live.flash_prefill_dispatches,
        .implementation_sha256 = actual.identity.runtime,
        .fit_template_sha256 = inputs.fit_template_sha256,
        .gate_template_sha256 = inputs.gate_template_sha256,
        .generation_contract_sha256 = inputs.generation_contract_sha256,
        .live = &live,
        .eligible_block_starts = inputs.eligible_block_starts,
        .document_id = inputs.document_id,
        .document_sha256 = inputs.document_sha256,
        .document_bucket = inputs.document_bucket,
    };
    const study_singleton_paged_decode = try strictCampaignQualificationEnvBool(
        "ANTFLY_CONTEXT_CAMPAIGN_STUDY_PAGED_DECODE",
    );
    const replay_options = blk: {
        const split_text = std.c.getenv("ANTFLY_CONTEXT_CAMPAIGN_REPLAY_SPLIT_COUNT") orelse
            break :blk campaign_coordinator.CampaignCaptureReplayOptions{
                .study_singleton_paged_decode = study_singleton_paged_decode,
            };
        const split_count = try std.fmt.parseInt(u32, std.mem.span(split_text), 10);
        break :blk campaign_coordinator.CampaignCaptureReplayOptions{
            .global_attention_split = .{ .max_query_rows = 8, .split_count = split_count },
            .study_singleton_paged_decode = study_singleton_paged_decode,
        };
    };
    try campaign_coordinator.runCampaignCaptureWithOptions(&live, inputs.prompt, sources, .{ .capture = .{
        .load_generation = actual.load_generation,
        .loaded_position_limit = config.max_position_embeddings,
        .identity = actual.identity,
        .protocols = authorities.values,
    } }, actual.identity, pipeline.compactionCampaignRunner(), exporter.observer(), peak_budget, replay_options);

    decode_state.context_session = null;
    live.deinit();
    live_active = false;
    try execution_lease.release();
    lease_active = false;
    std.debug.print("context_campaign_capture: prompt_tokens={d} question_tokens={d} qualified_context_tokens={d} production_query_campaign=true publication=false replay_split={} study_paged_decode={}\n", .{ inputs.prompt.len, inputs.question.len, inputs.qualified_context_tokens, replay_options.global_attention_split != null, replay_options.study_singleton_paged_decode });
}

test "context Metal installed Gemma stored all36 diagnostic from frozen external manifest" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    const enabled = std.c.getenv("ANTFLY_CONTEXT_STORED_INSTALLED_DIAGNOSTIC") orelse return error.SkipZigTest;
    if (!std.mem.eql(u8, std.mem.span(enabled), "1")) return error.SkipZigTest;
    const model_path = std.c.getenv("ANTFLY_CONTEXT_MODEL_DIR") orelse return error.MissingReplayCaptureInput;
    const campaign_path = std.c.getenv("ANTFLY_CONTEXT_CAPTURE_CAMPAIGN_DIR") orelse return error.MissingReplayCaptureInput;
    const manifest_path = std.c.getenv("ANTFLY_CONTEXT_CAPTURE_INPUT_MANIFEST") orelse return error.MissingReplayCaptureInput;
    const manifest_sha_text = std.c.getenv("ANTFLY_CONTEXT_CAPTURE_INPUT_MANIFEST_SHA256") orelse return error.MissingReplayCaptureInput;
    const source_receipt = std.c.getenv("ANTFLY_CONTEXT_SOURCE_BUILD_RECEIPT") orelse return error.MissingReplayCaptureInput;
    const peak_text = std.c.getenv("ANTFLY_CONTEXT_CAMPAIGN_PEAK_BYTES") orelse return error.MissingReplayCaptureInput;
    const peak_budget = try std.fmt.parseInt(usize, std.mem.span(peak_text), 10);
    const stored_manifest_path = std.c.getenv("ANTFLY_CONTEXT_STORED_ALL36_MANIFEST") orelse return error.MissingReplayCaptureInput;
    const stored_manifest_sha_text = std.c.getenv("ANTFLY_CONTEXT_STORED_ALL36_MANIFEST_SHA256") orelse return error.MissingReplayCaptureInput;
    const stored_receipt_path = std.c.getenv("ANTFLY_CONTEXT_STORED_INSTALLED_RECEIPT") orelse return error.MissingReplayCaptureInput;
    var stored_manifest_sha: [32]u8 = undefined;
    if (std.mem.span(stored_manifest_sha_text).len != 64) return error.InvalidReplayCaptureInput;
    _ = std.fmt.hexToBytes(&stored_manifest_sha, std.mem.span(stored_manifest_sha_text)) catch return error.InvalidReplayCaptureInput;
    if (peak_budget < 16 * 1024 * 1024 or peak_budget > 8 * 1024 * 1024 * 1024)
        return error.InvalidReplayCaptureInput;

    const a = std.testing.allocator;
    var backend_manager = backends.SessionManager.initWithIo(a, std.testing.io);
    backend_manager.preferred_backends = &.{.metal};
    backend_manager.required_backend = .metal;
    var manager = manager_mod.ModelManager.init(a, backend_manager);
    defer manager.deinit();
    const model = try manager.loadFromDir(std.mem.span(model_path));
    const config = factory.getGptConfig(model.session) orelse return error.InvalidModelForGeneration;
    if (config.family != .gemma or config.gemma4_mtp_assistant) return error.InvalidModelForGeneration;
    const layout = try policy.Layout.init(config);
    const implementation = server_mod.contextServingCompactionImplementation(true, true);
    const actual = try model.compactionIdentity(layout, implementation);

    var manifest_sha: [32]u8 = undefined;
    if (std.mem.span(manifest_sha_text).len != 64) return error.InvalidReplayCaptureInput;
    _ = std.fmt.hexToBytes(&manifest_sha, std.mem.span(manifest_sha_text)) catch return error.InvalidReplayCaptureInput;
    var campaign_dir = try std.Io.Dir.cwd().openDir(std.testing.io, std.mem.span(campaign_path), .{ .follow_symlinks = false });
    defer campaign_dir.close(std.testing.io);
    var inputs = try numerical_capture_inputs.load(a, std.testing.io, campaign_dir, std.mem.span(manifest_path), manifest_sha, std.mem.span(source_receipt), .{
        .load_generation = actual.load_generation,
        .artifact_sha256 = actual.identity.artifact,
        .tokenizer_sha256 = actual.identity.tokenizer,
        .template_sha256 = actual.identity.template,
        .layout_sha256 = actual.identity.layout,
        .implementation_sha256 = actual.identity.runtime,
    });
    defer inputs.deinit();
    const qualified_context_tokens = try std.math.add(u32, @intCast(inputs.prompt.len), inputs.reserved_output_tokens);
    if (inputs.qualified_context_tokens != qualified_context_tokens or
        inputs.qualified_context_tokens > @as(u32, @intCast(config.max_position_embeddings)))
        return error.InvalidReplayCaptureLength;

    var authorities = try campaign_protocol.captureAuthorities(a, layout, campaign_studies.authority(.fit), campaign_studies.authority(.gate));
    defer authorities.deinit();
    const sources = campaign_coordinator.PreparedQuerySources{
        .question = .{
            .ids = inputs.question,
            .source_start = inputs.question_source_start,
            .source_end = inputs.question_source_end,
            .prompt_tokens = @intCast(inputs.prompt.len),
            .prompt_tokens_sha256 = inputs.prompt_tokens_sha256,
        },
        .fit = .{
            .ids = inputs.fit,
            .template_bytes = campaign_studies.template(.fit),
            .generation_contract_bytes = campaign_studies.generation_contract,
            .prompt_tokens = @intCast(inputs.prompt.len),
            .prompt_tokens_sha256 = inputs.prompt_tokens_sha256,
            .response_tokens = inputs.fit_response_tokens,
        },
        .gate = .{
            .ids = inputs.gate,
            .template_bytes = campaign_studies.template(.gate),
            .generation_contract_bytes = campaign_studies.generation_contract,
            .prompt_tokens = @intCast(inputs.prompt.len),
            .prompt_tokens_sha256 = inputs.prompt_tokens_sha256,
            .response_tokens = inputs.gate_response_tokens,
        },
        .loaded_private_position_limit = config.max_position_embeddings,
    };

    var execution_pool = try context_execution_pool.Pool.init(actual.load_generation, context_execution_pool.admitted_backend_bytes);
    defer execution_pool.deinit();
    try execution_pool.warm(null);
    var execution_lease = try execution_pool.acquire(actual.load_generation);
    var lease_active = true;
    defer if (lease_active) execution_lease.poisonAndRelease();
    const capture_policy = policy.Policy{
        .max_context_tokens = inputs.qualified_context_tokens,
        .max_kv_tokens = inputs.qualified_context_tokens,
    };
    const capture_plan = try capture_policy.admit(config.max_position_embeddings, null, inputs.prompt.len, @as(usize, inputs.reserved_output_tokens), 0);
    const qualified_context_limit: usize = inputs.qualified_context_tokens;
    if (capture_plan.context_limit != qualified_context_limit or
        capture_plan.logical_token_limit != qualified_context_limit or
        capture_plan.retained_row_limit != qualified_context_limit)
        return error.InvalidReplayCaptureLength;
    var settings = packed_session_mod.Config{
        .plan = capture_plan,
        .layout = layout,
        .max_query_rows = 64,
        .physical_budget_bytes = 0,
        .execution_lease = &execution_lease,
        .optimize_decode = true,
        .optimize_prefill = true,
    };
    settings.physical_budget_bytes = (try packed_session_mod.allocation(settings)).physical_bytes;
    var live = try packed_session_mod.Session.init(a, settings);
    var live_active = true;
    defer if (live_active) live.deinit();

    var budget = memory.RunBudget.init(.{ .host_limit_bytes = 8 * 1024 * 1024 * 1024, .backend_limit_bytes = 8 * 1024 * 1024 * 1024, .combined_limit_bytes = 10 * 1024 * 1024 * 1024 });
    var cb = try factory.getComputeBackendWithBudget(model.session, a, &budget);
    defer cb.deinit();
    var decode_state = generation.NativeDecodeState.initContiguous(a);
    defer decode_state.deinit();
    decode_state.context_session = live.hook();
    var pipeline = generation.NativeGenerationPipeline{
        .allocator = a,
        .io = std.testing.io,
        .cb = cb,
        .session = model.session,
        .load_generation = actual.load_generation,
        .gpt_config = config,
        .kv_dtype = .f16,
        .tokenizer = model.getTokenizer(),
        .chat_template = model.chat_tmpl,
        .add_bos_token = model.manifest.add_bos_token,
        .bos_token = model.manifest.bos_token,
        .decode_state = &decode_state,
        .return_raw_token_ids = true,
    };
    const prefill_config = generation.GenerationConfig{
        .context_storage = .packed_exact,
        .context_budget_bytes = settings.physical_budget_bytes,
        .context_flash_prefill = true,
        .context_split_decode = true,
        .prefill_chunk_size = numerical_capture_inputs.required_prefill_chunk_rows,
        .max_tokens = std.math.cast(i32, inputs.reserved_output_tokens) orelse return error.InvalidReplayCaptureLength,
        .ignore_eos = true,
    };
    try pipeline.runCompactionCapturePrefill(inputs.prompt, &decode_state, prefill_config, inputs.admitted_prefill_chunk_rows);
    try std.testing.expectEqual(@as(u32, @intCast(inputs.prompt.len)), live.logical_tokens);
    if (live.flash_prefill_dispatches == 0) return error.InvalidCampaignCapture;
    var stored = try stored_installed_caller.load(a, std.testing.io, std.mem.span(stored_manifest_path), stored_manifest_sha);
    defer stored.deinit();
    if (stored.parsed.value.native_peak_budget != peak_budget) return error.InvalidReplayCaptureInput;
    var request = try stored.request(actual.load_generation, actual.identity, sources, .{ .capture = .{
        .load_generation = actual.load_generation,
        .loaded_position_limit = config.max_position_embeddings,
        .identity = actual.identity,
        .protocols = authorities.values,
    } });
    if (std.c.getenv("ANTFLY_CONTEXT_STORED_INSTALLED_BASELINE_PREFLIGHT")) |baseline_path| {
        const baseline = try request.measureBaseline(&live, inputs.prompt, actual.load_generation);
        try stored_installed_caller.writeBaselineReceipt(std.testing.io, std.mem.span(baseline_path), stored_manifest_sha, manifest_sha, inputs.source_build_sha256, baseline);
    } else {
        pipeline.runStoredInstalledDiagnostic(&live, inputs.prompt, &request) catch |err| {
            return stored_installed_caller.persistFailureOrReturnOriginal(std.testing.io, std.mem.span(stored_receipt_path), stored_manifest_sha, manifest_sha, inputs.source_build_sha256, err, request.receipt);
        };
        try stored_installed_caller.writeReceipt(std.testing.io, std.mem.span(stored_receipt_path), stored_manifest_sha, manifest_sha, inputs.source_build_sha256, request.receipt);
    }

    decode_state.context_session = null;
    live.deinit();
    live_active = false;
    try execution_lease.release();
    lease_active = false;
    std.debug.print("context_stored_installed: prompt_tokens={d} question_tokens={d} qualified_context_tokens={d} publication=true diagnostic_only=true serving_authority=false release_authority=false\n", .{ inputs.prompt.len, inputs.question.len, inputs.qualified_context_tokens });
}

test "context Metal installed Gemma native coordinator preserves continuation" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    const enabled = std.c.getenv("ANTFLY_CONTEXT_COORDINATOR_PROBE") orelse return error.SkipZigTest;
    if (!std.mem.eql(u8, std.mem.span(enabled), "1")) return error.SkipZigTest;
    const path = std.c.getenv("ANTFLY_CONTEXT_MODEL_DIR") orelse return error.MissingReplayCaptureInput;
    const text_path = std.c.getenv("ANTFLY_CONTEXT_PROBE_TEXT_FILE") orelse return error.MissingReplayCaptureInput;
    const a = std.testing.allocator;
    var backend_manager = backends.SessionManager.initWithIo(a, std.testing.io);
    backend_manager.preferred_backends = &.{.metal};
    backend_manager.required_backend = .metal;
    var manager = manager_mod.ModelManager.init(a, backend_manager);
    defer manager.deinit();
    const model = try manager.loadFromDir(std.mem.span(path));
    const config = factory.getGptConfig(model.session) orelse return error.InvalidModelForGeneration;
    const layout = try policy.Layout.init(config);
    var cb = try factory.getComputeBackend(model.session, a);
    defer cb.deinit();
    const text = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, std.mem.span(text_path), a, .limited(1024 * 1024));
    defer a.free(text);
    const messages = [_]generation.Message{
        .{ .role = "system", .content = "Preserve AZURE-731. Answer only the user's final question." },
        .{ .role = "user", .content = text[0..@min(text.len, 4096)] },
        .{ .role = "user", .content = "Preserve AZURE-731. What is the marker?" },
    };
    var selection = policy.Policy{ .mode = .attention_matching, .max_kv_tokens = 8192, .keep_prefix_tokens = 32, .keep_recent_tokens = 32 };
    try selection.compaction_targets.add(.{ .message_index = 1, .part_index = 0 });
    // Explicit fixture identity: this probe exercises the exact shadow replay,
    // not release registry authorization or an installed lossy profile.
    const identity: compaction_profiles.Identity = .{ .artifact = @splat(1), .tokenizer = @splat(2), .template = @splat(3), .layout = @splat(4), .runtime = @splat(5) };
    var owners: [256]compaction_profiles.Owner = undefined;
    var count: usize = 0;
    const retention: [32]u32 = @splat(256);
    for (layout.layers[0..layout.len], 0..) |l, i| if (l.owner == i and l.sliding_window == 0) {
        owners[count] = .{ .layer = @intCast(i), .retained = retention[0..l.kv_heads] };
        count += 1;
    };
    const profile = compaction_profiles.Profile{ .identity = identity, .owners = owners[0..count], .max_prompt_tokens = 8192, .max_output_tokens = 8, .minimum_saved_bytes = 16384 };
    var state = generation.NativeDecodeState.initContiguous(a);
    defer state.deinit();
    var pipeline = generation.NativeGenerationPipeline{ .allocator = a, .io = std.testing.io, .cb = cb, .session = model.session, .gpt_config = config, .kv_dtype = .f16, .tokenizer = model.getTokenizer(), .chat_template = model.chat_tmpl, .add_bos_token = model.manifest.add_bos_token, .bos_token = model.manifest.bos_token, .decode_state = &state, .return_raw_token_ids = true };
    var request = generation.GenerationConfig{ .context_storage = .packed_exact, .context_budget_bytes = 1024 * 1024 * 1024, .max_tokens = 4, .ignore_eos = true, .enable_thinking = false, .prefill_chunk_size = 64 };
    var baseline = try pipeline.generate(&messages, request);
    defer baseline.deinit();
    request.context_qualification = .{ .selection = selection, .profile = profile, .identity = identity };
    var replayed = try pipeline.generate(&messages, request);
    defer replayed.deinit();
    try std.testing.expectEqualSlices(i32, baseline.token_ids.?, replayed.token_ids.?);
    try std.testing.expectEqualStrings(baseline.text, replayed.text);
    try std.testing.expectEqual(baseline.prompt_tokens, replayed.prompt_tokens);
    try std.testing.expectEqual(@as(usize, 4), replayed.tokens_used);
    const metrics = state.last_context_metrics orelse return error.MissingContextMetrics;
    try std.testing.expectEqual(@as(u64, 1), metrics.epoch);
    try std.testing.expectEqual(replayed.prompt_tokens + replayed.tokens_used - 1, metrics.logical_tokens);
    try std.testing.expect(metrics.replay_status_readback_bytes > 0 and metrics.query_bytes == 0 and metrics.compacted_blocks == 0 and state.context_session == null);
    // A subsequent identity/admission failure cannot reuse old request metrics.
    request.context_qualification.?.identity.template[0] ^= 1;
    try std.testing.expectError(error.CompactionIdentityMismatch, pipeline.generate(&messages, request));
    try std.testing.expect(state.last_context_metrics == null and state.context_session == null);
    std.debug.print("context_native_coordinator: path={s} prompt_tokens={d} generated={d} replay_status_readback_bytes={d} peak_bytes={d} identical_tokens=true identical_text=true hook_restored=true rejected_request_cleared=true lossy_published=false serving_qualified=false\n", .{ std.mem.span(path), replayed.prompt_tokens, replayed.tokens_used, metrics.replay_status_readback_bytes, metrics.peak_bytes });
}
