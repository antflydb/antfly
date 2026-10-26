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

//! Test-only end-to-end synthetic exercise for runStoredCampaignDiagnostic.
const std = @import("std");
const coordinator = @import("compaction_coordinator.zig");
const driver_mod = @import("context_stored_all36_driver.zig");
const installed = @import("context_stored_installed_invocation.zig");
const live_mod = @import("packed_context_session.zig");
const storage = @import("packed_context_storage.zig");
const query_protocol = @import("context_query_protocol.zig");
const study_templates = @import("context_study_templates.zig");
const execution_pool = @import("context_execution_pool.zig");
const contract = @import("../runtime/kv/context_session.zig");
const policy = @import("../runtime/context_policy.zig");
const payload = @import("../runtime/kv/payload.zig");
const reference = @import("../runtime/kv/reference_replay.zig");
const owner_alloc = @import("../runtime/kv/host_allocation_tracker.zig");
const shared = @import("metal_generated_kernels.zig");

const artifact_count = 36;
const prompt_tokens = 8192;
const block_rows = 512;
const retained_rows = 320;
const lossy_blocks = 12;
const head_dim = 64;

fn digest(bytes: []const u8) [32]u8 {
    var result: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &result, .{});
    return result;
}

fn tokenDigest(ids: []const i64) [32]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    for (ids) |id| {
        var bytes: [8]u8 = undefined;
        std.mem.writeInt(i64, &bytes, id, .little);
        hash.update(&bytes);
    }
    return hash.finalResult();
}

fn fixtureConfig() !live_mod.Config {
    var layout = policy.Layout{ .len = 3 };
    for (layout.layers[0..3], 0..) |*layer, index| layer.* = .{
        .owner = @intCast(index),
        .sliding_window = 0,
        .kv_heads = 1,
        .query_heads = 1,
        .head_dim = head_dim,
        .rope_active_dim = head_dim,
        .rope_frequency_dim = head_dim,
        .rope_theta = 10000,
    };
    var config = live_mod.Config{
        .plan = try (policy.Policy{}).admit(12288, null, prompt_tokens, 1, 0),
        .layout = layout,
        .max_query_rows = 64,
        .physical_budget_bytes = 0,
    };
    config.physical_budget_bytes = (try live_mod.allocation(config)).physical_bytes;
    return config;
}

fn device(ref: shared.Ref) contract.DeviceRef {
    return .{ .handle = ref.handle, .offset = ref.offset, .bytes = ref.bytes };
}

const Runner = struct {
    layout: policy.Layout,
    q: shared.Buffer,
    k: shared.Buffer,
    v: shared.Buffer,
    out: shared.Buffer,

    fn step(self: *Runner, hook: contract.Hook, start: u32, rows: u32) !void {
        for (self.layout.layers[0..self.layout.len], 0..) |layer, layer_index| {
            try hook.attention(.{
                .consumer_layer = @intCast(layer_index),
                .owner_layer = layer.owner,
                .query_start = start,
                .query_rows = rows,
                .query_heads = layer.query_heads,
                .kv_heads = layer.kv_heads,
                .head_dim = layer.head_dim,
                .sliding_window = layer.sliding_window,
                .skip_kv_write = layer.owner != layer_index,
                .scale = 1,
            }, device(self.q.ref()), device(self.k.ref()), device(self.v.ref()), device(self.out.ref()));
            const output_count = try std.math.mul(
                usize,
                @intCast(rows),
                try std.math.mul(usize, @intCast(layer.query_heads), @intCast(layer.head_dim)),
            );
            for (self.out.floats()[0..output_count]) |value|
                try std.testing.expectApproxEqAbs(@as(f32, 1), value, 0.002);
        }
    }

    fn replay(ctx: *anyopaque, hook: contract.Hook, start: usize, ids: []const i64) !void {
        const self: *Runner = @ptrCast(@alignCast(ctx));
        try self.step(hook, @intCast(start), @intCast(ids.len));
    }

    fn study(ctx: *anyopaque, _: std.mem.Allocator, hook: contract.Hook, ids: []const i64, response_tokens: u32, _: coordinator.CampaignStudyExecutionOptions, captured: ?[]i64) !coordinator.CampaignStudyReceipt {
        const self: *Runner = @ptrCast(@alignCast(ctx));
        var generated_ids: [study_templates.response_tokens]i64 = undefined;
        const output_count: usize = @intCast(response_tokens);
        if (output_count != generated_ids.len) return error.InvalidStudyGeneration;
        if (captured) |output| if (output.len != output_count) return error.InvalidStudyGeneration;
        const prompt_start: u32 = @intCast(hook.plan.prompt_tokens);
        const prefix_tokens: u32 = @intCast(ids.len);
        var cursor: u32 = 0;
        while (cursor < prefix_tokens) {
            const rows = try hook.nextChunk(prompt_start + cursor, prefix_tokens - cursor);
            try self.step(hook, prompt_start + cursor, rows);
            cursor += rows;
        }
        var generated: u32 = 0;
        while (generated < response_tokens) {
            const start = try std.math.add(u32, try std.math.add(u32, prompt_start, prefix_tokens), generated);
            const rows = try hook.nextChunk(start, response_tokens - generated);
            try self.step(hook, start, rows);
            const generated_start: usize = @intCast(generated);
            const generated_rows: usize = @intCast(rows);
            for (generated_ids[generated_start..][0..generated_rows], generated_start..) |*token, index| token.* = @intCast(index + 1);
            generated += rows;
        }
        if (captured) |output| @memcpy(output, &generated_ids);
        return .{
            .session_identity = hook.identity,
            .prefill_tokens = prefix_tokens,
            .generated_tokens = response_tokens,
            .generated_tokens_sha256 = tokenDigest(&generated_ids),
        };
    }
};

fn writeFile(io: std.Io, dir: std.Io.Dir, name: []const u8, bytes: []const u8) !void {
    const file = try dir.createFile(io, name, .{ .exclusive = true });
    defer file.close(io);
    try file.writeStreamingAll(io, bytes);
    try file.sync(io);
}

fn runCase(precommit_fault: bool) !void {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    const generation: u64 = if (precommit_fault) 92 else 91;
    var pool = try execution_pool.Pool.init(generation, execution_pool.admitted_backend_bytes);
    defer pool.deinit();
    try pool.warm(null);
    var lease = try pool.acquire(generation);
    defer if (lease.pool != null) lease.poisonAndRelease();
    var tracker = owner_alloc.HostAllocationTracker.init(null, 0);
    const tracker_baseline = tracker.snapshot();
    var cancelled = std.atomic.Value(bool).init(false);
    var config = try fixtureConfig();
    config.cancelled = &cancelled;
    config.execution_lease = &lease;
    config.owner_allocation_tracker = &tracker;
    var live = try live_mod.Session.init(allocator, config);
    var live_active = true;
    defer if (live_active) live.deinit();

    var q = try shared.Buffer.init(live.copier.core.runtime, 64 * head_dim * @sizeOf(f32));
    defer q.deinit();
    var k = try shared.Buffer.init(live.copier.core.runtime, 64 * head_dim * @sizeOf(f32));
    defer k.deinit();
    var v = try shared.Buffer.init(live.copier.core.runtime, 64 * head_dim * @sizeOf(f32));
    defer v.deinit();
    var out = try shared.Buffer.init(live.copier.core.runtime, 64 * head_dim * @sizeOf(f32));
    defer out.deinit();
    @memset(q.bytes, 0);
    @memset(k.bytes, 0);
    @memset(v.floats(), 1);
    var runner_state = Runner{ .layout = config.layout, .q = q, .k = k, .v = v, .out = out };
    while (live.logical_tokens < prompt_tokens) {
        const rows = try live.hook().nextChunk(live.logical_tokens, @intCast(prompt_tokens - live.logical_tokens));
        try runner_state.step(live.hook(), live.logical_tokens, rows);
    }
    const baseline = try live.committedContextStorageDiagnostic();
    try std.testing.expectEqual(@as(u32, 3), baseline.global_owner_count);
    try std.testing.expectEqual(baseline.owner_count, baseline.total_owner_count);
    try std.testing.expect(baseline.total_owner_count >= baseline.global_owner_count);
    try std.testing.expectEqual(@as(u64, 0), baseline.replay_control_bytes);

    var prompt: [prompt_tokens]i64 = @splat(1);
    const prompt_sha = tokenDigest(&prompt);
    const response_tokens = study_templates.response_tokens;
    const fit_study = study_templates.authority(.fit);
    const gate_study = study_templates.authority(.gate);
    var authorities = try query_protocol.captureAuthorities(allocator, config.layout, fit_study, gate_study);
    defer authorities.deinit();
    const sources = coordinator.PreparedQuerySources{
        .question = .{ .ids = prompt[100..120], .source_start = 100, .source_end = 120, .prompt_tokens = prompt_tokens, .prompt_tokens_sha256 = prompt_sha },
        .fit = .{ .ids = &.{ 2, 3, 5, 7 }, .template_bytes = study_templates.template(.fit), .generation_contract_bytes = study_templates.generation_contract, .prompt_tokens = prompt_tokens, .prompt_tokens_sha256 = prompt_sha, .response_tokens = response_tokens },
        .gate = .{ .ids = &.{ 11, 13, 17, 19 }, .template_bytes = study_templates.template(.gate), .generation_contract_bytes = study_templates.generation_contract, .prompt_tokens = prompt_tokens, .prompt_tokens_sha256 = prompt_sha, .response_tokens = response_tokens },
        .loaded_private_position_limit = 12288,
    };
    const actual = coordinator.profiles.Identity{ .artifact = @splat(1), .tokenizer = @splat(2), .template = @splat(3), .layout = @splat(4), .runtime = @splat(5) };

    var labels: [prompt_tokens]u32 = @splat(0);
    @memset(labels[512..6656], 1);
    var ranges = [_]reference.Range{.{ .start = 512, .end = 6656 }};
    var blocks: [lossy_blocks]reference.Range = undefined;
    for (&blocks, 0..) |*block, index| {
        const start: u32 = 512 + @as(u32, @intCast(index)) * block_rows;
        block.* = .{ .start = start, .end = start + block_rows };
    }
    const eligible = payload.Eligible{ .allocator = allocator, .labels = &labels, .ranges = &ranges, .blocks = &blocks };
    const exact_prefix = [_]u32{512};
    const compacted = [_]u32{retained_rows};
    const exact_tail = [_]u32{1536};
    var segments: [14]storage.Segment = undefined;
    segments[0] = .{ .logical_count = 512, .retained = &exact_prefix };
    for (1..13) |span| segments[span] = .{ .logical_count = block_rows, .retained = &compacted };
    segments[13] = .{ .logical_count = 1536, .retained = &exact_tail };
    const all_segments = [_][]const storage.Segment{ &segments, &segments, &segments };
    const retained_segment_rows = try std.math.add(u32, 512 + lossy_blocks * retained_rows, 1536);
    const continuation_rows: u32 = @intCast(live.config.plan.logical_token_limit - live.logical_tokens);
    const retained_limit = try std.math.add(u32, retained_segment_rows, continuation_rows);
    try std.testing.expectEqual(@as(u32, 5888), retained_segment_rows);
    try std.testing.expectEqual(@as(u32, 1), continuation_rows);
    try std.testing.expectEqual(@as(u32, 5889), retained_limit);
    const stored_allocation = try storage.allocation(.{
        .head_dim = head_dim,
        .kv_heads = 1,
        .continuation_rows = continuation_rows,
        .max_retained_rows = retained_limit,
    }, &segments);
    try std.testing.expect(stored_allocation.physical_bytes > 0);

    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    var coordinates: [artifact_count]coordinator.StoredDiagnosticCoordinate = undefined;
    var specs: [artifact_count]driver_mod.ArtifactSpec = undefined;
    var budget_artifacts: [artifact_count]live_mod.StoredArtifact = undefined;
    var biases: [artifact_count][retained_rows]f32 = undefined;
    var key_names: [artifact_count][32]u8 = undefined;
    var value_names: [artifact_count][32]u8 = undefined;
    var bias_names: [artifact_count][32]u8 = undefined;
    var artifact_index: usize = 0;
    for (live.globals, 0..) |*owner, owner_index| {
        for (0..lossy_blocks) |block| {
            const logical_start: u32 = 512 + @as(u32, @intCast(block)) * block_rows;
            const offset = @as(usize, logical_start) * head_dim * @sizeOf(f16);
            const bytes = retained_rows * head_dim * @sizeOf(f16);
            const keys = owner.keys.bytes[offset..][0..bytes];
            const values = owner.values.bytes[offset..][0..bytes];
            @memset(biases[artifact_index][0..], @floatCast(@log(@as(f64, block_rows) / retained_rows)));
            const bias = std.mem.sliceAsBytes(biases[artifact_index][0..]);
            coordinates[artifact_index] = .{
                .owner_layer = live.global_layers[owner_index],
                .head = 0,
                .span = @intCast(block),
                .logical_start = logical_start,
                .logical_count = block_rows,
                .retained_rows = retained_rows,
                .head_dim = head_dim,
            };
            const key_name = try std.fmt.bufPrint(&key_names[artifact_index], "k-{d}", .{artifact_index});
            const value_name = try std.fmt.bufPrint(&value_names[artifact_index], "v-{d}", .{artifact_index});
            const bias_name = try std.fmt.bufPrint(&bias_names[artifact_index], "b-{d}", .{artifact_index});
            try writeFile(io, temporary.dir, key_name, keys);
            try writeFile(io, temporary.dir, value_name, values);
            try writeFile(io, temporary.dir, bias_name, bias);
            specs[artifact_index] = .{
                .coordinate = coordinates[artifact_index],
                .keys = .{ .path = key_name, .size_bytes = keys.len, .sha256 = digest(keys) },
                .values = .{ .path = value_name, .size_bytes = values.len, .sha256 = digest(values) },
                .bias = .{ .path = bias_name, .size_bytes = bias.len, .sha256 = digest(bias) },
            };
            budget_artifacts[artifact_index] = .{
                .owner_layer = coordinates[artifact_index].owner_layer,
                .head = 0,
                .span = coordinates[artifact_index].span,
                .logical_start = logical_start,
                .logical_count = block_rows,
                .retained_rows = retained_rows,
                .head_dim = head_dim,
                .keys_f16 = keys,
                .values_f16 = values,
                .bias_f32 = bias,
                .sha256 = @splat(0),
            };
            artifact_index += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, artifact_count), artifact_index);
    const stored_layout = try driver_mod.layoutForCoordinatesDiagnostic(&coordinates, &all_segments);
    for (stored_layout.destination_spans, 0..) |destination_span, index|
        try std.testing.expectEqual(@as(u32, @intCast((index % lossy_blocks) + 1)), destination_span);
    try writeFile(io, temporary.dir, "evidence", "frozen-all36-fixture");
    var evidence = [_]driver_mod.FileSpec{.{ .path = "evidence", .size_bytes = "frozen-all36-fixture".len, .sha256 = digest("frozen-all36-fixture") }};
    const fingerprint = try live.qualificationFingerprint();
    const loader_budget = try live_mod.storedLoaderBudgetDiagnostic(&budget_artifacts);
    var driver_config = driver_mod.Config{
        .artifact_dir = temporary.dir,
        .evidence_dir = temporary.dir,
        .ledger_dir = temporary.dir,
        .reservation_name = if (precommit_fault) "reservation-reject" else "reservation-success",
        .artifacts = &specs,
        .evidence = &evidence,
        .reservation = .{
            .reservation_identity = if (precommit_fault) 902 else 901,
            .algorithm_sha256 = @splat(6),
            .build_sha256 = @splat(7),
            .artifact_manifest_sha256 = @splat(0),
            .profile_sha256 = try coordinator.storedDiagnosticProfileDigestDiagnostic(&coordinates),
            .coordinates = &coordinates,
            .source_descriptor_sha256 = fingerprint.descriptor_sha256,
            .source_payload_sha256 = fingerprint.payload_sha256,
            .loader_budget_bytes = loader_budget.physical_upper_bytes,
            .materialization_host_bytes = 0,
            .artifact_count = artifact_count,
            .expected_baseline_context_bytes = baseline.physical_bytes,
            .expected_owner_count = baseline.global_owner_count,
            .expected_total_owner_count = baseline.total_owner_count,
        },
        .generator_sha256 = @splat(8),
        .corpus_sha256 = @splat(9),
        .ordered_rows_sha256 = @splat(10),
        .partition_sha256 = @splat(11),
        .fault = if (precommit_fault) .content_write else null,
    };
    driver_config.reservation.materialization_host_bytes = try driver_mod.materializationUpperDiagnostic(driver_config);
    driver_config.reservation.artifact_manifest_sha256 = try driver_mod.rawManifestDiagnostic(driver_config);
    var driver = try driver_mod.Driver.init(allocator, io, driver_config);
    defer driver.deinit();
    const old_epoch = live.publication.epoch;
    const old_key_handles = [_]*anyopaque{ live.globals[0].keys.handle, live.globals[1].keys.handle, live.globals[2].keys.handle };
    const old_value_handles = [_]*anyopaque{ live.globals[0].values.handle, live.globals[1].values.handle, live.globals[2].values.handle };
    const authority = coordinator.CampaignAuthority{ .capture = .{ .load_generation = generation, .loaded_position_limit = 12288, .identity = actual, .protocols = authorities.values } };
    const campaign_runner = coordinator.CampaignRunner{ .ctx = &runner_state, .replay_fn = Runner.replay, .study_fn = Runner.study };
    var request = installed.Request{
        .frozen = .{
            .load_generation = generation,
            .model_identity = actual,
            .algorithm_sha256 = driver_config.reservation.algorithm_sha256,
            .build_sha256 = driver_config.reservation.build_sha256,
            .artifact_manifest_sha256 = driver_config.reservation.artifact_manifest_sha256,
            .profile_sha256 = driver_config.reservation.profile_sha256,
            .source_descriptor_sha256 = fingerprint.descriptor_sha256,
            .source_payload_sha256 = fingerprint.payload_sha256,
            .expected_prompt_tokens = @intCast(prompt.len),
            .expected_baseline_context_bytes = baseline.physical_bytes,
            .expected_global_owner_count = baseline.global_owner_count,
            .expected_total_owner_count = baseline.total_owner_count,
        },
        .prompt_tokens_sha256 = try @import("context_teacher_forced.zig").promptTokenDigest(&prompt),
        .sources = sources,
        .authority = authority,
        .actual = actual,
        .provider = driver.provider(),
        .coordinates = &coordinates,
        .eligible = &eligible,
        .parts = &all_segments,
        .retained_limit = retained_limit,
        .native_peak_budget = 1024 * 1024 * 1024,
    };
    if (precommit_fault) {
        try std.testing.expectError(error.InjectedStoredDriverFailure, request.run(&live, &prompt, generation, campaign_runner));
        try std.testing.expectEqual(.failed_before_commit, request.receipt.terminal.state);
        try std.testing.expectEqual(baseline.physical_bytes, request.receipt.baseline_context_physical_bytes);
        try std.testing.expectEqual(baseline.global_owner_count, request.receipt.baseline_global_owner_count);
        try std.testing.expectEqual(baseline.total_owner_count, request.receipt.baseline_total_owner_count);
        try std.testing.expect(request.receipt.terminal.failure != null and request.receipt.terminal.failure.?.durable and request.receipt.terminal.failure.?.nonreuse_committed);
        try std.testing.expectEqual(old_epoch, live.publication.epoch);
        for (live.globals, old_key_handles, old_value_handles) |owner, key_handle, value_handle| {
            try std.testing.expectEqual(key_handle, owner.keys.handle);
            try std.testing.expectEqual(value_handle, owner.values.handle);
        }
    } else {
        try request.run(&live, &prompt, generation, campaign_runner);
        try std.testing.expect(request.receipt.completed);
        try std.testing.expectEqual(.committed, request.receipt.terminal.state);
        try std.testing.expect(request.receipt.terminal.publication != null);
        try std.testing.expectEqual(old_epoch + 1, live.publication.epoch);
        try std.testing.expectEqual(baseline.physical_bytes, request.receipt.baseline_context_physical_bytes);
        try std.testing.expectEqual(baseline.global_owner_count, request.receipt.baseline_global_owner_count);
        try std.testing.expectEqual(baseline.total_owner_count, request.receipt.baseline_total_owner_count);
        try std.testing.expectEqual(baseline.global_owner_count, request.receipt.terminal.stored_global_owner_count);
        try std.testing.expectEqual(baseline.total_owner_count, request.receipt.terminal.stored_owner_count);
        try std.testing.expectEqual(baseline.total_owner_count, request.receipt.terminal.stored_total_owner_count);
        try std.testing.expect(request.receipt.terminal.stored_request_context_physical_bytes * 4 <= request.receipt.terminal.baseline_request_context_physical_bytes * 3);
        for (live.globals, old_key_handles, old_value_handles) |owner, key_handle, value_handle| {
            try std.testing.expect(owner.keys.handle != key_handle);
            try std.testing.expect(owner.values.handle != value_handle);
        }
    }
    const release = live.deinitMeasured();
    live_active = false;
    try std.testing.expect(release.released);
    const after = tracker.snapshot();
    try std.testing.expectEqual(tracker_baseline.tracked_combined_live_bytes, after.tracked_combined_live_bytes);
}

test "context Metal stored all36 coordinator commits synthetic three-owner campaign and rolls back precommit failure" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    try runCase(true);
    try runCase(false);
}

test "context stored all36 synthetic capture digest is deterministic" {
    const expected = [_]i64{ 1, 2, 3, 4 };
    try std.testing.expectEqualSlices(u8, &tokenDigest(&expected), &tokenDigest(&.{ 1, 2, 3, 4 }));
}
