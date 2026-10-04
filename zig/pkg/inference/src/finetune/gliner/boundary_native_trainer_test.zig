// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const build_options = @import("build_options");
const trainer = @import("boundary_native_trainer.zig");
const controller = @import("../seeded_gradient_trainer.zig");
const data = @import("boundary_dataset.zig");
const run = @import("boundary_run.zig");
const step = @import("boundary_train_step.zig");
const helper = @import("boundary_train_step_test.zig");
const objectives_mod = @import("boundary_train_objectives.zig");
const processor = @import("../../pipelines/gliner_boundary_processor.zig");
const native = @import("../../ops/native_compute.zig");
const ml = @import("ml").graph;
const bundle = @import("../../models/gliner_boundary_bundle.zig");
const Tensor = @import("../../backends/tensor.zig").Tensor;
const metal_runtime = @import("../../backends/metal_runtime.zig");
const metal_tensor = @import("../../backends/metal_tensor.zig");

pub fn nextObserved(owner: *trainer.Trainer) !?trainer.Report {
    if (owner.options.execution == .native) return owner.next(null);
    const before = metal_tensor.memoryStatsSnapshot();
    const frozen_uploads = owner.backend.receipt;
    const report = (try owner.next(null)) orelse return null;
    const after = metal_tensor.memoryStatsSnapshot();
    // Detached proposal/loss views use the separately bounded Step interface.
    // Neither its VJPs nor optimizer state may use generic host mirroring.
    try std.testing.expectEqual(before.host_mirror_download_bytes, after.host_mirror_download_bytes);
    try std.testing.expectEqual(before.to_host_calls, after.to_host_calls);
    try std.testing.expectEqualDeep(frozen_uploads, owner.backend.receipt);
    try std.testing.expect(!owner.optimizer.host_mirrors_current);
    try std.testing.expect(report.resident_device_upper_bound_bytes > 0);
    try std.testing.expect(report.resident_device_upper_bound_bytes <= owner.options.limits.max_backend_bytes);
    if (report.terms != null) {
        try std.testing.expect(report.transfers.bytes.upload_bytes > 0);
        try std.testing.expect(try report.transfers.bytes.readbackBytes() > 0);
        try std.testing.expect(report.transfers.upload_calls > 0);
        try std.testing.expect(report.transfers.readback_calls > 0);
    }
    const receipt = owner.optimizer.last_device_receipt orelse return error.MissingDeviceOptimizerReceipt;
    try std.testing.expect(receipt.selected_slots > 0);
    try std.testing.expectEqual(report.optimizer.identity.optimizer_step, receipt.identity.optimizer_step);
    try std.testing.expectEqual(report.optimizer.identity.microbatch_step, receipt.identity.microbatch_step);
    try std.testing.expectEqual(report.optimizer.optimizer_stepped, receipt.optimizer_stepped);
    try std.testing.expectEqual(@as(usize, 0), receipt.scalar_upload_bytes % 4);
    const clipping_bytes: usize = if (owner.optimizer.pytorch_clip_order != null) 4 else 0;
    if (owner.options.execution == .resident_cuda) {
        // CUDA boolean validation reads one flags word per bounded batch.
        try std.testing.expectEqual(@as(usize, 0), receipt.scalar_download_bytes % 4);
    } else try std.testing.expectEqual(clipping_bytes, receipt.scalar_download_bytes % 12);
    try std.testing.expect(receipt.scalar_upload_bytes + receipt.scalar_download_bytes <= receipt.selected_slots * 144 + 64 + clipping_bytes);
    try std.testing.expectEqual(@as(usize, 0), report.resident_gradient_control_bytes % 12);
    return report;
}

pub fn expectSameState(expected: *trainer.Trainer, actual: *trainer.Trainer) !void {
    // Synchronization is deliberate at this diagnostic/checkpoint boundary,
    // never part of a successful per-microbatch observation.
    try expected.optimizer.ensureHostState(null);
    try actual.optimizer.ensureHostState(null);
    try std.testing.expect(expected.optimizer.host_mirrors_current);
    try std.testing.expect(actual.optimizer.host_mirrors_current);
    try std.testing.expectEqual(expected.optimizer.identity(), actual.optimizer.identity());
    try std.testing.expectEqual(expected.optimizer.owner.accum_count, actual.optimizer.owner.accum_count);
    try std.testing.expectEqualSlices(bool, expected.optimizer.present, actual.optimizer.present);
    try std.testing.expectEqual(expected.optimizer.owner.regular_params.items.len, actual.optimizer.owner.regular_params.items.len);
    for (expected.optimizer.owner.regular_params.items, actual.optimizer.owner.regular_params.items) |want, got| {
        try std.testing.expectEqualStrings(want.name, got.name);
        try std.testing.expectEqualSlices(f32, want.weights, got.weights);
        try std.testing.expectEqualSlices(f32, want.grad_accum, got.grad_accum);
        try std.testing.expectEqual(want.adam_step_count, got.adam_step_count);
        const expected_state = expected.optimizer.owner.optimizer_state.param_states.get(want.name).?;
        const actual_state = actual.optimizer.owner.optimizer_state.param_states.get(got.name).?;
        try std.testing.expectEqualSlices(f32, expected_state.m, actual_state.m);
        try std.testing.expectEqualSlices(f32, expected_state.v, actual_state.v);
        try std.testing.expectEqual(expected_state.step_count, actual_state.step_count);
    }
    try std.testing.expectEqual(try expected.optimizer.stateFingerprint(expected.fingerprint, null), try actual.optimizer.stateFingerprint(actual.fingerprint, null));
}

fn expectOrder(a: std.mem.Allocator, owner: *const trainer.Trainer, report: trainer.Report) !void {
    if (report.terms == null) return;
    const order = try owner.run_plan.epochOrder(a, report.epoch, null);
    defer a.free(order);
    try std.testing.expectEqual(report.epoch, owner.order_epoch.?);
    try std.testing.expectEqualSlices(u32, order, owner.order);
}

fn dataset(a: std.mem.Allocator) !data.Dataset {
    var bytes = std.Io.Writer.Allocating.init(a);
    defer bytes.deinit();
    for (0..5) |i| try bytes.writer.print("{{\"version\":1,\"id\":\"{d}\",\"text\":\"Ada Acme\",\"schema\":{{\"entities\":[\"person\"],\"entity_definitions\":{{\"person\":{{\"validators\":[{{\"pattern\":\"Ada\"}}]}}}},\"classifications\":[{{\"name\":\"topic\",\"labels\":[\"meeting\",\"billing\"],\"mode\":\"multi\"}}]}},\"entities\":[{{\"id\":\"a\",\"type\":\"person\",\"span\":{{\"start\":0,\"end\":3}}}}]}}\n", .{i});
    return data.Dataset.fromBytes(a, bytes.written(), .{ .limits = .{ .max_host_bytes = 16 * 1024 * 1024 } }, null, null);
}

fn exercise(a: std.mem.Allocator, execution: controller.Execution, attention_profile: step.AttentionProfile) !void {
    return exerciseWithActivation(a, execution, attention_profile, .retained_v1);
}

fn exerciseWithActivation(a: std.mem.Allocator, execution: controller.Execution, attention_profile: step.AttentionProfile, activation_profile: step.ActivationProfile) !void {
    return exerciseWithBoundaryAttention(a, execution, attention_profile, activation_profile, false);
}

fn exerciseWithBoundaryAttention(a: std.mem.Allocator, execution: controller.Execution, attention_profile: step.AttentionProfile, activation_profile: step.ActivationProfile, fused_boundary: bool) !void {
    var config = helper.config();
    if (fused_boundary) {
        config.head.boundary_dim = 32;
        config.head.boundary_attention_heads = 1;
        config.head.dropout = 0;
    }
    if (activation_profile == .layer_recompute_v1) config.encoder.num_hidden_layers = 2;
    return exerciseConfig(a, execution, attention_profile, activation_profile, fused_boundary, config);
}

/// A two-layer ModernBERT trunk (one global, one local layer) under the same
/// full and heads-only jobs, cancellation, limits, and durable resume.
/// `replay_tiled_v1` selects the fused trunk attention.
fn exerciseModernBert(a: std.mem.Allocator, execution: controller.Execution, attention_profile: step.AttentionProfile) !void {
    var config = helper.config();
    config.backbone = .modern_bert;
    config.encoder = .{ .hidden_size = 4, .intermediate_size = 8, .num_hidden_layers = 2, .num_attention_heads = 2, .vocab_size = 512, .max_position_embeddings = 256, .position_buckets = 0, .layer_norm_eps = 1e-5, .hidden_dropout_prob = 0, .attention_probs_dropout_prob = 0, .pad_token_id = 0, .family = .modern_bert, .global_rope_theta = 160000, .local_rope_theta = 10000, .local_attention_window = 8, .global_attn_every_n_layers = 2 };
    return exerciseConfig(a, execution, attention_profile, .retained_v1, false, config);
}

fn exerciseConfig(a: std.mem.Allocator, execution: controller.Execution, attention_profile: step.AttentionProfile, activation_profile: step.ActivationProfile, fused_boundary: bool, config: @import("../../models/gliner_boundary.zig").Config) !void {
    var samples = try dataset(a);
    defer samples.deinit();
    var tokenizer = helper.TestTokenizer{};
    var first = try samples.sample(0, null, null);
    defer first.deinit();
    var prepared = try processor.prepare(a, tokenizer.tokenizer(), &.{.{ .text = first.row.text, .schema = &first.schema }}, .{});
    defer prepared.deinit();
    var graph = try step.buildWithAttentionProfile(a, config, &prepared, &.{&first.schema}, .{}, .training, attention_profile, .{});
    defer graph.deinit();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var store = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
    defer store.deinitOwned();
    var parameters = std.ArrayListUnmanaged(run.Parameter).empty;
    for (graph.graph.parameters.items) |id| {
        const node = graph.graph.node(id);
        const borrowed_name = graph.graph.parameterName(node);
        if (std.mem.startsWith(u8, borrowed_name, "__")) continue;
        const name = try a.dupe(u8, borrowed_name);
        var enrolled = false;
        errdefer if (!enrolled) a.free(name);
        const shape = node.output_shape;
        const values = try scratch.alloc(f32, @intCast(shape.numElements().?));
        for (values, 0..) |*v, index| v.* = if (std.mem.endsWith(u8, name, ".bias")) 0 else if (std.mem.indexOf(u8, name, "norm") != null or std.mem.indexOf(u8, name, "LayerNorm") != null) 1 else 0.05 * @sin(@as(f32, @floatFromInt(index + @as(usize, id) * 11 + 1)));
        var tensor = try Tensor.initFloat32(a, name, shape.dims[0..shape.rank_], values);
        errdefer if (!enrolled) tensor.deinit();
        try store.resident_weights.put(a, name, .{ .tensor = tensor });
        enrolled = true;
        const dims = try scratch.alloc(i32, shape.rank_);
        for (dims, shape.dims[0..shape.rank_]) |*dim, size| dim.* = @intCast(size);
        // The zero-dropout classifier removes Sequential index two. Preserve
        // the released canonical name, as the real source/benchmark loader does.
        const encoder_name = std.mem.startsWith(u8, name, "embeddings.") or std.mem.startsWith(u8, name, "encoder.") or
            (config.encoder.family == .modern_bert and (std.mem.startsWith(u8, name, "layers.") or std.mem.startsWith(u8, name, "final_norm.")));
        const canonical = if (std.mem.eql(u8, name, "classifier.2.weight")) "classifier.3.weight" else if (std.mem.eql(u8, name, "classifier.2.bias")) "classifier.3.bias" else if (encoder_name) try std.fmt.allocPrint(scratch, "encoder.{s}", .{name}) else name;
        try parameters.append(scratch, .{ .name = name, .canonical_name = canonical, .dimensions = dims, .values = tensor.asFloat32(), .kind = .original });
    }
    const source = bundle.Identity{ .backbone = config.backbone, .precision = .fp32, .weight = bundle.Digest.of("immutable tiny test weights"), .sidecars = .{ bundle.Digest.of("model"), bundle.Digest.of("encoder"), bundle.Digest.of("tokenizer"), bundle.Digest.of("tokenizer config") } };
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/native.safetensors", .{temporary.sub_path});
    defer a.free(path);
    const invalid_path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/invalid-cursor.safetensors", .{temporary.sub_path});
    defer a.free(invalid_path);
    for ([_]run.Mode{ .full, .heads }) |mode| {
        var options = trainer.Options{ .execution = execution, .attention_profile = attention_profile, .activation_profile = activation_profile, .run = .{ .mode = mode, .epochs = 2, .batch_size = 2, .accumulation = 2, .seed = 918, .encoder_lr = 0.001, .task_lr = 0.002 }, .source_reserved_bytes = 16 * 1024 * 1024, .limits = .{ .max_host_bytes = 256 * 1024 * 1024, .max_backend_bytes = 256 * 1024 * 1024, .max_combined_bytes = 1024 * 1024 * 1024 } };
        if (activation_profile == .layer_recompute_v1) {
            options.limits.step.recomputation.max_plan_host_bytes = 32 * 1024 * 1024;
            options.limits.step.max_step_host_bytes = 32 * 1024 * 1024;
            options.limits.max_recomputed_batch_scratch_bytes = 16 * 1024 * 1024;
        }
        var expected = try trainer.Trainer.init(a, &store, tokenizer.tokenizer(), source, config, &samples, parameters.items, options, null);
        defer expected.deinit();
        if (execution != .native) {
            try std.testing.expectEqual(@as(usize, if (mode == .full) 0 else expected.backend.admission.frozen_parameters), expected.backend.receipt.upload_tensors);
            if (mode == .heads) try std.testing.expect(expected.backend.receipt.upload_bytes > 0);
        }
        var reports = std.ArrayListUnmanaged(trainer.Report).empty;
        defer reports.deinit(a);
        while (try nextObserved(expected)) |report| {
            if (fused_boundary and report.terms != null) {
                var found = false;
                for (expected.plan.?.graph.nodes.items) |node| {
                    if (node.op == .fused_boundary_training_attention_v1) found = true;
                }
                try std.testing.expect(found);
            }
            if (execution == .resident_cuda and report.terms != null) {
                var found_scan = false;
                for (expected.plan.?.graph.nodes.items) |node| {
                    if (node.op == .fused_prefix_scan_v1) found_scan = true;
                }
                try std.testing.expect(found_scan);
            }
            try expectOrder(a, expected, report);
            try reports.append(a, report);
        }
        try std.testing.expectEqual(@as(usize, 8), reports.items.len);
        try std.testing.expectEqual(@as(u64, 4), expected.optimizer.identity().optimizer_step);
        try std.testing.expectEqual(@as(u64, 6), expected.optimizer.identity().microbatch_step);
        var actual = try trainer.Trainer.init(a, &store, tokenizer.tokenizer(), source, config, &samples, parameters.items, options, null);
        defer actual.deinit();
        {
            actual.busy.store(true, .release);
            defer actual.busy.store(false, .release);
            try std.testing.expectError(error.BoundaryTrainingJobBusy, actual.next(null));
            try std.testing.expectError(error.BoundaryTrainingJobBusy, actual.save(path, null));
            try std.testing.expectError(error.BoundaryTrainingJobBusy, actual.restore(path, null));
            try std.testing.expectError(error.BoundaryTrainingJobBusy, actual.position());
        }
        const Cancel = struct {
            calls: usize = 0,
            fn check(raw: ?*anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(raw.?));
                self.calls += 1;
                if (self.calls == 20) return error.Cancelled;
            }
        };
        var cancel = Cancel{};
        try std.testing.expectError(error.Cancelled, actual.next(.{ .ptr = &cancel, .check_fn = Cancel.check }));
        try std.testing.expectEqual(@as(u64, 0), actual.optimizer.identity().microbatch_step);
        const state_before_denials = try actual.optimizer.stateFingerprint(actual.fingerprint, null);
        const backend_limit = actual.backend_budget.limit;
        actual.backend_budget.limit = 1;
        try std.testing.expectError(error.BoundaryTrainingBackendMemoryLimitExceeded, actual.next(null));
        try std.testing.expectEqual(@as(u64, 0), actual.optimizer.identity().microbatch_step);
        try std.testing.expectEqual(trainer.MemoryDomain.backend, actual.memory_failures.snapshot().?.domain);
        try std.testing.expectEqual(trainer.MemoryPhase.forward_backward, actual.memory_failures.snapshot().?.phase);
        actual.backend_budget.limit = backend_limit;
        const host_limit = actual.host_budget.limit;
        actual.host_budget.limit = actual.host_budget.live;
        try std.testing.expectError(error.BoundaryTrainingHostMemoryLimitExceeded, actual.next(null));
        try std.testing.expectEqual(trainer.MemoryDomain.host, actual.memory_failures.snapshot().?.domain);
        try std.testing.expectEqual(trainer.MemoryPhase.batch_preparation, actual.memory_failures.snapshot().?.phase);
        actual.host_budget.limit = host_limit;
        // Both sticky denial flags remain set. A new underlying allocation
        // failure must still be OutOfMemory, without advancing optimizer/data.
        {
            const backing = actual.backend_budget.backing;
            var failing = std.testing.FailingAllocator.init(backing, .{ .fail_index = 0 });
            actual.backend_budget.backing = failing.allocator();
            defer actual.backend_budget.backing = backing;
            try std.testing.expectError(error.OutOfMemory, actual.next(null));
            const failure = actual.memory_failures.snapshot().?;
            try std.testing.expectEqual(trainer.MemoryDomain.backend, failure.domain);
            try std.testing.expectEqual(.backing_allocator, failure.allocation.kind);
        }
        try std.testing.expectEqual(state_before_denials, try actual.optimizer.stateFingerprint(actual.fingerprint, null));
        try std.testing.expectEqual(@as(u64, 0), actual.optimizer.identity().microbatch_step);
        try std.testing.expect(!actual.busy.load(.acquire));
        if (activation_profile == .layer_recompute_v1) {
            const scratch_limit = actual.options.limits.max_recomputed_batch_scratch_bytes;
            actual.options.limits.max_recomputed_batch_scratch_bytes = 1;
            try std.testing.expectError(error.BoundaryTrainingHostMemoryLimitExceeded, actual.next(null));
            actual.options.limits.max_recomputed_batch_scratch_bytes = scratch_limit;
            try std.testing.expectEqual(state_before_denials, try actual.optimizer.stateFingerprint(actual.fingerprint, null));
            const host_cap = actual.options.limits.max_host_bytes;
            actual.options.limits.max_host_bytes = actual.host_budget.live + actual.recomputed_future_host_bytes - 1;
            try std.testing.expectError(error.BoundaryTrainingRunLimitExceeded, actual.next(null));
            actual.options.limits.max_host_bytes = host_cap;
            try std.testing.expectEqual(state_before_denials, try actual.optimizer.stateFingerprint(actual.fingerprint, null));
            try std.testing.expect(!actual.plan.?.recomputation.?.graph.regional.active_tape);
        }
        for (reports.items, 0..) |want, index| {
            if (activation_profile == .layer_recompute_v1 and want.terms == null) {
                // The previous partial-window checkpoint restores into a fresh
                // owner; flush must be admitted even with no cached graph.
                const before = try actual.optimizer.stateFingerprint(actual.fingerprint, null);
                const work_cap = actual.options.limits.step.recomputation.max_total_work;
                actual.options.limits.step.recomputation.max_total_work = 1;
                try std.testing.expectError(error.BoundaryTrainingRunLimitExceeded, actual.next(null));
                actual.options.limits.step.recomputation.max_total_work = work_cap;
                try std.testing.expectEqual(before, try actual.optimizer.stateFingerprint(actual.fingerprint, null));
            }
            const got = (try nextObserved(actual)) orelse return error.TestUnexpectedResult;
            try expectOrder(a, actual, got);
            try std.testing.expectEqual(want.epoch, got.epoch);
            try std.testing.expectEqual(want.batch, got.batch);
            try std.testing.expectEqual(want.examples, got.examples);
            try std.testing.expectEqualDeep(want.terms, got.terms);
            try std.testing.expectEqualDeep(want.optimizer, got.optimizer);
            try std.testing.expectEqual(want.decision_fingerprint, got.decision_fingerprint);
            if (index == 0 or index == 2) {
                if (index == 0) {
                    const old_microbatch = actual.optimizer.owner.step_count;
                    actual.optimizer.owner.step_count += 1;
                    actual.optimizer.save(invalid_path, actual.fingerprint, null) catch |err| {
                        actual.optimizer.owner.step_count = old_microbatch;
                        return err;
                    };
                    actual.optimizer.owner.step_count = old_microbatch;
                    const before_restore = actual.optimizer.identity();
                    try std.testing.expectError(error.InvalidBoundaryTrainingProgress, actual.restore(invalid_path, null));
                    try std.testing.expectEqual(before_restore, actual.optimizer.identity());
                }
                try actual.optimizer.ensureHostState(null);
                const checkpoint_state = try actual.optimizer.stateFingerprint(actual.fingerprint, null);
                try actual.save(path, null);
                if (activation_profile == .layer_recompute_v1 and index == 0) {
                    var incompatible_options = options;
                    incompatible_options.activation_profile = .retained_v1;
                    const incompatible = try trainer.Trainer.init(a, &store, tokenizer.tokenizer(), source, config, &samples, parameters.items, incompatible_options, null);
                    defer incompatible.deinit();
                    try std.testing.expect(!std.mem.eql(u8, &actual.fingerprint, &incompatible.fingerprint));
                    const before = try incompatible.optimizer.stateFingerprint(incompatible.fingerprint, null);
                    try std.testing.expectError(error.TrainingStateFingerprintMismatch, incompatible.restore(path, null));
                    try std.testing.expectEqual(before, try incompatible.optimizer.stateFingerprint(incompatible.fingerprint, null));
                }
                if (attention_profile == .replay_tiled_v1 and index == 0) {
                    var incompatible_options = options;
                    incompatible_options.attention_profile = .materialized_v1;
                    const incompatible = try trainer.Trainer.init(a, &store, tokenizer.tokenizer(), source, config, &samples, parameters.items, incompatible_options, null);
                    defer incompatible.deinit();
                    try std.testing.expect(!std.mem.eql(u8, &actual.fingerprint, &incompatible.fingerprint));
                    const before = try incompatible.optimizer.stateFingerprint(incompatible.fingerprint, null);
                    try std.testing.expectError(error.TrainingStateFingerprintMismatch, incompatible.restore(path, null));
                    try std.testing.expectEqual(before, try incompatible.optimizer.stateFingerprint(incompatible.fingerprint, null));
                }
                const resumed = try trainer.Trainer.init(a, &store, tokenizer.tokenizer(), source, config, &samples, parameters.items, options, null);
                errdefer resumed.deinit();
                const restored = try resumed.restorePinned(path, checkpoint_state, null);
                try std.testing.expectEqual(checkpoint_state, restored.state_sha256);
                try expectSameState(actual, resumed);
                actual.deinit();
                actual = resumed;
            }
        }
        try std.testing.expectEqual(@as(?trainer.Report, null), try actual.next(null));
        try expectSameState(expected, actual);
        for (actual.optimizer.owner.regular_params.items) |got| {
            const actual_state = actual.optimizer.owner.optimizer_state.param_states.get(got.name).?;
            if (std.mem.startsWith(u8, got.name, "classifier.")) {
                try std.testing.expectEqual(@as(u32, 0), actual_state.step_count);
                const original = store.resident_weights.get(got.name).?.tensor;
                try std.testing.expectEqualSlices(f32, original.asFloat32(), got.weights);
            }
        }
    }
}

test "boundary native trainer composes ModernBERT full and heads jobs with cancellation and durable partial resume" {
    try exerciseModernBert(std.testing.allocator, .native, .materialized_v1);
}

test "boundary native trainer resident Metal composes ModernBERT full and heads jobs with exact durable resume" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!metal_runtime.metalDeviceAvailable()) return error.SkipZigTest;
    try exerciseModernBert(std.testing.allocator, .resident_metal, .materialized_v1);
}

test "boundary native trainer composes ModernBERT fused-attention full and heads jobs with durable partial resume" {
    try exerciseModernBert(std.testing.allocator, .native, .replay_tiled_v1);
}

test "boundary native trainer resident Metal composes ModernBERT fused-attention full and heads jobs with exact durable resume" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!metal_runtime.metalDeviceAvailable()) return error.SkipZigTest;
    try exerciseModernBert(std.testing.allocator, .resident_metal, .replay_tiled_v1);
}

test "boundary native trainer composes immutable batches full and heads training cancellation and durable partial resume" {
    // Keep leak checks and failure injection; allocation backtraces are opt-in.
    var allocator_state: std.heap.DebugAllocator(.{ .stack_trace_frames = 0, .resize_stack_traces = false }) = .init;
    defer std.debug.assert(allocator_state.deinit() == .ok);
    const test_allocator = if (@import("antfly_platform").env.getenvBool("ANTFLY_TEST_ALLOCATOR_TRACES")) std.testing.allocator else allocator_state.allocator();
    try exercise(test_allocator, .native, .materialized_v1);
}

test "boundary native trainer resident Metal composes tiny full and heads jobs with exact durable resume" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!metal_runtime.metalDeviceAvailable()) return error.SkipZigTest;
    try exercise(std.testing.allocator, .resident_metal, .materialized_v1);
}

test "boundary native trainer replay attention full and heads jobs preserve cancellation and partial resume identity" {
    // Keep leak checks and failure injection; allocation backtraces are opt-in.
    var allocator_state: std.heap.DebugAllocator(.{ .stack_trace_frames = 0, .resize_stack_traces = false }) = .init;
    defer std.debug.assert(allocator_state.deinit() == .ok);
    const test_allocator = if (@import("antfly_platform").env.getenvBool("ANTFLY_TEST_ALLOCATOR_TRACES")) std.testing.allocator else allocator_state.allocator();
    try exercise(test_allocator, .native, .replay_tiled_v1);
}

test "boundary native trainer replay attention resident Metal full and heads jobs preserve partial resume identity" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!metal_runtime.metalDeviceAvailable()) return error.SkipZigTest;
    try exercise(std.testing.allocator, .resident_metal, .replay_tiled_v1);
}

test "boundary native trainer regional recomputation two layers full and heads preserve cancellation and exact partial resume" {
    // Keep leak checks and failure injection; allocation backtraces are opt-in.
    var allocator_state: std.heap.DebugAllocator(.{ .stack_trace_frames = 0, .resize_stack_traces = false }) = .init;
    defer std.debug.assert(allocator_state.deinit() == .ok);
    const test_allocator = if (@import("antfly_platform").env.getenvBool("ANTFLY_TEST_ALLOCATOR_TRACES")) std.testing.allocator else allocator_state.allocator();
    try exerciseWithActivation(test_allocator, .native, .replay_tiled_v1, .layer_recompute_v1);
}

test "boundary native trainer regional recomputation resident Metal two layers full and heads preserve exact partial resume" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!metal_runtime.metalDeviceAvailable()) return error.SkipZigTest;
    try exerciseWithActivation(std.testing.allocator, .resident_metal, .replay_tiled_v1, .layer_recompute_v1);
}

test "boundary native trainer allocation failures distinguish phase limits resize recovery and backing OOM" {
    const Budget = @import("../../runtime/bounded_allocator.zig").BoundedAllocator;
    const a = std.testing.allocator;
    var failing_host = std.testing.FailingAllocator.init(a, .{});
    var failing_backend = std.testing.FailingAllocator.init(a, .{});
    var host = Budget{ .backing = failing_host.allocator(), .limit = 32 };
    var backend = Budget{ .backing = failing_backend.allocator(), .limit = 32 };
    var failures = trainer.MemoryFailures{};
    var host_observer = trainer.MemoryObserver{ .failures = &failures, .domain = .host };
    host_observer.attach(&host);
    var backend_observer = trainer.MemoryObserver{ .failures = &failures, .domain = .backend };
    backend_observer.attach(&backend);
    defer std.debug.assert(host.live == 0 and backend.live == 0);

    failures.begin(.graph_preparation);
    try std.testing.expectError(error.OutOfMemory, host.allocator().alloc(u8, 33));
    try std.testing.expectEqual(error.BoundaryTrainingHostMemoryLimitExceeded, failures.translate(error.OutOfMemory));
    const host_failure = failures.snapshot().?;
    try std.testing.expectEqual(trainer.MemoryPhase.graph_preparation, host_failure.phase);
    try std.testing.expectEqual(trainer.MemoryDomain.host, host_failure.domain);
    try std.testing.expectEqual(@as(usize, 33), host_failure.allocation.requested_bytes);
    try std.testing.expectEqual(@as(usize, 32), host_failure.allocation.limit_bytes);
    try std.testing.expectEqual(error.InvalidShape, failures.translate(error.InvalidShape));
    try std.testing.expectEqual(error.Cancelled, failures.translate(error.Cancelled));

    failures.begin(.forward_backward);
    try std.testing.expectError(error.OutOfMemory, backend.allocator().alloc(u8, 33));
    try std.testing.expectEqual(error.BoundaryTrainingBackendMemoryLimitExceeded, failures.translate(error.OutOfMemory));
    try std.testing.expectEqual(trainer.MemoryPhase.forward_backward, failures.snapshot().?.phase);

    failures.begin(.gradient_readback);
    const bytes = try host.allocator().alloc(u8, 16);
    defer host.allocator().free(bytes);
    @memset(bytes, 7);
    try std.testing.expect(!host.allocator().resize(bytes, 64));
    try std.testing.expect(host.denied);
    try std.testing.expectEqual(@as(?trainer.MemoryFailure, null), failures.snapshot());
    // A smaller replacement can recover after the oversized resize miss.
    const replacement = try host.allocator().alloc(u8, 8);
    host.allocator().free(replacement);
    try std.testing.expectEqual(@as(?trainer.MemoryFailure, null), failures.snapshot());
    failing_host.fail_index = failing_host.alloc_index;
    try std.testing.expectError(error.OutOfMemory, host.allocator().alloc(u8, 8));
    try std.testing.expectEqual(error.OutOfMemory, failures.translate(error.OutOfMemory));
    try std.testing.expectEqual(.backing_allocator, failures.snapshot().?.allocation.kind);
    try std.testing.expectEqual(@as(usize, 16), failures.snapshot().?.allocation.live_bytes);
    try std.testing.expectEqualSlices(u8, &(@as([16]u8, @splat(7))), bytes);

    // A recovered terminal failure in one domain cannot hide a later backing
    // failure in the other: the shared operation record observes actual order.
    try std.testing.expectError(error.OutOfMemory, host.allocator().alloc(u8, 33));
    failing_backend.fail_index = failing_backend.alloc_index;
    try std.testing.expectError(error.OutOfMemory, backend.allocator().alloc(u8, 1));
    try std.testing.expectEqual(error.OutOfMemory, failures.translate(error.OutOfMemory));
    try std.testing.expectEqual(trainer.MemoryDomain.backend, failures.snapshot().?.domain);
    failures.begin(.optimizer);
    failing_host.fail_index = std.math.maxInt(usize);
    failing_backend.fail_index = std.math.maxInt(usize);
    const recovered = try backend.allocator().alloc(u8, 32);
    backend.allocator().free(recovered);
    try std.testing.expectEqual(@as(?trainer.MemoryFailure, null), failures.snapshot());

    var job = Budget{ .backing = a, .limit = 1 };
    var job_observer = trainer.MemoryObserver{ .failures = &failures, .domain = .job };
    job_observer.attach(&job);
    failures.begin(.job);
    try std.testing.expectError(error.OutOfMemory, job.allocator().alloc(u8, 2));
    try std.testing.expectEqual(error.BoundaryTrainingJobMemoryLimitExceeded, failures.translate(error.OutOfMemory));
    try std.testing.expectEqual(@as(usize, 0), job.live);
}

fn restoredFlushAdmission(a: std.mem.Allocator) !void {
    var samples = try dataset(a);
    defer samples.deinit();
    var tokenizer = helper.TestTokenizer{};
    const config = helper.config();
    var store = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
    defer store.deinitOwned();
    // A 2 MiB selected parameter makes the pending optimizer's two live
    // temporaries exceed restore's single upload staging allowance. This
    // isolates the control-plane regression from encoder execution.
    const values = try a.alloc(f32, 1024 * 512);
    defer a.free(values);
    @memset(values, 0.125);
    const name = "boundary_head.admission_projection.weight";
    const dimensions = [_]i32{ 1024, 512 };
    const parameters = [_]run.Parameter{.{ .name = name, .canonical_name = name, .dimensions = &dimensions, .values = values, .kind = .original }};
    const source = bundle.Identity{ .backbone = config.backbone, .precision = .fp32, .weight = bundle.Digest.of("immutable admission test weights"), .sidecars = .{ bundle.Digest.of("model"), bundle.Digest.of("encoder"), bundle.Digest.of("tokenizer"), bundle.Digest.of("tokenizer config") } };
    const options = trainer.Options{
        .execution = .resident_metal,
        .run = .{ .mode = .full, .epochs = 2, .batch_size = 2, .accumulation = 2, .seed = 918, .encoder_lr = 0.001, .task_lr = 0.002 },
        .source_reserved_bytes = 16 * 1024 * 1024,
        .limits = .{ .max_host_bytes = 256 * 1024 * 1024, .max_backend_bytes = 256 * 1024 * 1024, .max_combined_bytes = 1024 * 1024 * 1024 },
    };
    const original = try trainer.Trainer.init(a, &store, tokenizer.tokenizer(), source, config, &samples, &parameters, options, null);
    defer original.deinit();
    {
        const gradient = try original.cb.residentTrainingPrimitive(&.{ .upload_f32 = .{ .values = values, .shape = &dimensions } }, .{});
        defer original.cb.free(gradient);
        // Public submissions produce a valid epoch-end partial window: three
        // batches, one optimizer update, and one unflushed microbatch.
        for (0..3) |_| _ = try original.optimizer.submitResident(original.optimizer.identity(), 1, &.{.{ .name = name, .value = .{ .tensor = gradient } }}, null);
    }
    try std.testing.expectEqual(controller.Identity{ .optimizer_step = 1, .microbatch_step = 3 }, original.optimizer.identity());
    try std.testing.expectEqual(@as(u32, 1), original.optimizer.owner.accum_count);
    try std.testing.expect((try original.position()).requires_flush);
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/epoch-end.safetensors", .{temporary.sub_path});
    defer a.free(path);
    const checkpoint_state = try original.optimizer.stateFingerprint(original.fingerprint, null);
    try original.save(path, null);
    const resumed = try trainer.Trainer.init(a, &store, tokenizer.tokenizer(), source, config, &samples, &parameters, options, null);
    defer resumed.deinit();
    const restored = try resumed.restorePinned(path, checkpoint_state, null);
    try std.testing.expectEqual(checkpoint_state, restored.state_sha256);
    try std.testing.expect(resumed.plan == null);
    try std.testing.expect((try resumed.position()).requires_flush);
    try expectSameState(original, resumed);

    const fixed = resumed.backend.admission.frozen_device_bytes + resumed.optimizer.owner.device_trainable_bytes + resumed.options.limits.max_backend_host_bytes;
    const restore_bound = fixed + resumed.optimizer.owner.device_trainable_bytes + @max(2 * 1024 * 1024, values.len * @sizeOf(f32));
    const transaction = try resumed.optimizer.residentUpdateAdmission(a);
    const transaction_bound = fixed + transaction.device_upper_bound_bytes;
    try std.testing.expect(restore_bound < transaction_bound);
    const original_ceiling = resumed.options.limits.max_backend_bytes;
    resumed.options.limits.max_backend_bytes = transaction_bound - 1;
    try std.testing.expect(restore_bound <= resumed.options.limits.max_backend_bytes);
    try std.testing.expect(resumed.resident_device_upper_bound_bytes <= resumed.options.limits.max_backend_bytes);
    const before_identity = resumed.optimizer.identity();
    const before_device = resumed.optimizer.owner.regular_params.items[0].device.?;
    const before_mirrors = resumed.optimizer.host_mirrors_current;
    const before_receipt = resumed.optimizer.last_device_receipt;
    const before = metal_tensor.memoryStatsSnapshot();
    try std.testing.expectError(error.BoundaryTrainingRunLimitExceeded, resumed.next(null));
    const after = metal_tensor.memoryStatsSnapshot();
    try std.testing.expectEqual(before.device_owned_buffers_created, after.device_owned_buffers_created);
    try std.testing.expectEqual(before.device_owned_live_bytes, after.device_owned_live_bytes);
    try std.testing.expectEqual(before.host_mirror_download_bytes, after.host_mirror_download_bytes);
    try std.testing.expectEqual(before.to_host_calls, after.to_host_calls);
    try std.testing.expectEqual(before_identity, resumed.optimizer.identity());
    try std.testing.expectEqual(before_device, resumed.optimizer.owner.regular_params.items[0].device.?);
    try std.testing.expectEqual(before_mirrors, resumed.optimizer.host_mirrors_current);
    try std.testing.expectEqualDeep(before_receipt, resumed.optimizer.last_device_receipt);
    try std.testing.expectEqual(checkpoint_state, try resumed.optimizer.stateFingerprint(resumed.fingerprint, null));
    try std.testing.expect(resumed.plan == null);
    try std.testing.expect(!resumed.busy.load(.acquire));
    try std.testing.expect((try resumed.position()).requires_flush);

    resumed.options.limits.max_backend_bytes = original_ceiling;
    const flushed = (try nextObserved(resumed)) orelse return error.TestUnexpectedResult;
    try std.testing.expect(flushed.terms == null);
    try std.testing.expect(flushed.optimizer.optimizer_stepped);
    try std.testing.expectEqual(controller.Identity{ .optimizer_step = 2, .microbatch_step = 3 }, flushed.optimizer.identity);
    try std.testing.expectEqual(@as(u32, 0), flushed.optimizer.accumulated_microbatches);
    try std.testing.expectEqual(transaction_bound, flushed.resident_device_upper_bound_bytes);
    try std.testing.expect(!(try resumed.position()).requires_flush);
    _ = (try nextObserved(original)) orelse return error.TestUnexpectedResult;
    try expectSameState(original, resumed);
}

test "boundary native trainer resident restored partial flush enforces combined admission before mutation" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!metal_runtime.metalDeviceAvailable()) return error.SkipZigTest;
    const before = metal_tensor.memoryStatsSnapshot();
    try restoredFlushAdmission(std.testing.allocator);
    try std.testing.expectEqual(before.device_owned_live_bytes, metal_tensor.memoryStatsSnapshot().device_owned_live_bytes);
}

test "boundary native trainer CUDA full and heads materialized and replay preserve durable resume" {
    try @import("../../graph/resident_training_fixture.zig").CudaDevice.requireAvailable();
    try exercise(std.testing.allocator, .resident_cuda, .materialized_v1);
    try exercise(std.testing.allocator, .resident_cuda, .replay_tiled_v1);
}

test "boundary native trainer CUDA layer recomputation preserves durable resume" {
    try @import("../../graph/resident_training_fixture.zig").CudaDevice.requireAvailable();
    try exerciseWithActivation(std.testing.allocator, .resident_cuda, .replay_tiled_v1, .layer_recompute_v1);
}

test "boundary native trainer CUDA fused boundary attention preserves durable resume and recomputation" {
    try @import("../../graph/resident_training_fixture.zig").CudaDevice.requireAvailable();
    try exerciseWithBoundaryAttention(std.testing.allocator, .resident_cuda, .materialized_v1, .retained_v1, true);
    try exerciseWithBoundaryAttention(std.testing.allocator, .resident_cuda, .replay_tiled_v1, .layer_recompute_v1, true);
}

/// Deterministic teacher states sized to each microbatch's student routes.
const SyntheticTeacher = struct {
    hidden: usize,
    calls: usize = 0,

    fn teacher(self: *SyntheticTeacher, salt: u8) trainer.Teacher {
        var identity: [32]u8 = @splat(0);
        identity[0] = salt;
        return .{ .ptr = self, .identity = identity, .encode = encode };
    }

    const Owned = struct {
        allocator: std.mem.Allocator,
        values: [4][]f32,
        fn release(raw: ?*anyopaque) void {
            const self: *Owned = @ptrCast(@alignCast(raw.?));
            for (self.values) |values| self.allocator.free(values);
            self.allocator.destroy(self);
        }
    };

    fn encode(raw: *anyopaque, a: std.mem.Allocator, items: []const processor.Item, student: *const processor.PreparedBatch, _: ?@import("../../execution_control.zig").InferenceExecutionControl) !trainer.TeacherStates {
        const self: *SyntheticTeacher = @ptrCast(@alignCast(raw));
        if (items.len != student.samples.len) return error.TestUnexpectedResult;
        self.calls += 1;
        const owned = try a.create(Owned);
        owned.allocator = a;
        const rows = [_]usize{ student.text_word_mask.len, student.query_marker_mask.len, student.cls_marker_mask.len, student.parent_marker_mask.len };
        for (&owned.values, rows, 0..) |*values, count, route| {
            values.* = try a.alloc(f32, count * self.hidden);
            for (values.*, 0..) |*value, i| value.* = @sin(@as(f32, @floatFromInt(i * 5 + route * 17 + 1)) * 0.41) + 0.25 * @as(f32, @floatFromInt(i % self.hidden));
        }
        return .{ .text = owned.values[0], .queries = owned.values[1], .classifications = owned.values[2], .parents = owned.values[3], .context = owned, .release = Owned.release };
    }
};

const NeckedFixture = struct {
    config: @import("../../models/gliner_boundary.zig").Config,
    samples: data.Dataset,
    tokenizer: helper.TestTokenizer = .{},
    arena: std.heap.ArenaAllocator,
    store: native.WeightStore,
    parameters: std.ArrayListUnmanaged(run.Parameter) = .empty,
    source: bundle.Identity,

    fn init(self: *NeckedFixture, a: std.mem.Allocator, identity_neck: bool) !void {
        var config = modernConfig();
        config.neck = .linear;
        return self.initConfig(a, config, identity_neck);
    }

    fn modernConfig() @import("../../models/gliner_boundary.zig").Config {
        var config = helper.config();
        config.backbone = .modern_bert;
        config.encoder = .{ .hidden_size = 4, .intermediate_size = 8, .num_hidden_layers = 2, .num_attention_heads = 2, .vocab_size = 512, .max_position_embeddings = 256, .position_buckets = 0, .layer_norm_eps = 1e-5, .hidden_dropout_prob = 0, .attention_probs_dropout_prob = 0, .pad_token_id = 0, .family = .modern_bert, .global_rope_theta = 160000, .local_rope_theta = 10000, .local_attention_window = 8, .global_attn_every_n_layers = 2 };
        return config;
    }

    fn initConfig(self: *NeckedFixture, a: std.mem.Allocator, config: @import("../../models/gliner_boundary.zig").Config, identity_neck: bool) !void {
        self.* = .{ .config = config, .samples = try dataset(a), .arena = std.heap.ArenaAllocator.init(a), .store = .{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty }, .source = .{ .backbone = config.backbone, .precision = .fp32, .weight = bundle.Digest.of("immutable tiny test weights"), .sidecars = .{ bundle.Digest.of("model"), bundle.Digest.of("encoder"), bundle.Digest.of("tokenizer"), bundle.Digest.of("tokenizer config") } } };
        errdefer self.deinit();
        var first = try self.samples.sample(0, null, null);
        defer first.deinit();
        var prepared = try processor.prepare(a, self.tokenizer.tokenizer(), &.{.{ .text = first.row.text, .schema = &first.schema }}, .{});
        defer prepared.deinit();
        // Enroll every source tensor, heads included, as a full job's source has them.
        var graph = try step.buildWithAttentionProfile(a, config, &prepared, &.{&first.schema}, .{}, .training, .materialized_v1, .{});
        defer graph.deinit();
        const scratch = self.arena.allocator();
        const h = config.encoder.hidden_size;
        for (graph.graph.parameters.items) |id| {
            const node = graph.graph.node(id);
            const borrowed_name = graph.graph.parameterName(node);
            if (std.mem.startsWith(u8, borrowed_name, "__")) continue;
            const name = try a.dupe(u8, borrowed_name);
            var enrolled = false;
            errdefer if (!enrolled) a.free(name);
            const shape = node.output_shape;
            const values = try scratch.alloc(f32, @intCast(shape.numElements().?));
            for (values, 0..) |*v, index| v.* = if (std.mem.endsWith(u8, name, ".bias")) 0 else if (std.mem.indexOf(u8, name, "norm") != null) 1 else 0.05 * @sin(@as(f32, @floatFromInt(index + @as(usize, id) * 11 + 1)));
            if (identity_neck and std.mem.eql(u8, name, "gliner_neck.weight")) for (values, 0..) |*v, index| {
                v.* = if (index / h == index % h) 1 else 0;
            };
            var tensor = try Tensor.initFloat32(a, name, shape.dims[0..shape.rank_], values);
            errdefer if (!enrolled) tensor.deinit();
            try self.store.resident_weights.put(a, name, .{ .tensor = tensor });
            enrolled = true;
            const dims = try scratch.alloc(i32, shape.rank_);
            for (dims, shape.dims[0..shape.rank_]) |*dim, size| dim.* = @intCast(size);
            const encoder_name = std.mem.startsWith(u8, name, "embeddings.") or std.mem.startsWith(u8, name, "layers.") or std.mem.startsWith(u8, name, "final_norm.") or std.mem.startsWith(u8, name, "encoder.");
            const canonical = if (std.mem.eql(u8, name, "classifier.2.weight")) "classifier.3.weight" else if (std.mem.eql(u8, name, "classifier.2.bias")) "classifier.3.bias" else if (encoder_name) try std.fmt.allocPrint(scratch, "encoder.{s}", .{name}) else name;
            try self.parameters.append(scratch, .{ .name = name, .canonical_name = canonical, .dimensions = dims, .values = tensor.asFloat32(), .kind = .original });
        }
    }

    pub fn deinit(self: *NeckedFixture) void {
        self.store.deinitOwned();
        self.arena.deinit();
        self.samples.deinit();
    }

    fn options(teacher: trainer.Teacher) trainer.Options {
        return .{ .run = .{ .mode = .full, .epochs = 1, .batch_size = 2, .accumulation = 1, .seed = 918, .encoder_lr = 0.001, .task_lr = 0.002 }, .source_reserved_bytes = 16 * 1024 * 1024, .distillation = .{ .teacher = teacher }, .limits = .{ .max_host_bytes = 256 * 1024 * 1024, .max_backend_bytes = 256 * 1024 * 1024, .max_combined_bytes = 1024 * 1024 * 1024 } };
    }
};

test "boundary native trainer distills a necked ModernBERT from a teacher and leaves every head weight frozen" {
    const a = std.testing.allocator;
    var fixture: NeckedFixture = undefined;
    try fixture.init(a, false);
    defer fixture.deinit();
    const config = fixture.config;
    const parameters = fixture.parameters;
    const source = fixture.source;
    var teacher = SyntheticTeacher{ .hidden = config.encoder.hidden_size };
    var options = NeckedFixture.options(teacher.teacher(1));
    var distilled = try trainer.Trainer.init(a, &fixture.store, fixture.tokenizer.tokenizer(), source, config, &fixture.samples, parameters.items, options, null);
    defer distilled.deinit();
    var steps: usize = 0;
    while (try distilled.next(null)) |report| {
        const terms = report.terms orelse continue;
        try std.testing.expect(terms.distillation > 0);
        try std.testing.expectEqual(terms.distillation, terms.total);
        steps += 1;
    }
    try std.testing.expect(steps > 0);
    try std.testing.expectEqual(steps, teacher.calls);
    var moved_trunk = false;
    var moved_neck = false;
    for (distilled.optimizer.owner.regular_params.items) |slot| {
        const initial = for (parameters.items) |parameter| {
            if (std.mem.eql(u8, parameter.name, slot.name)) break parameter.values;
        } else return error.TestUnexpectedResult;
        const changed = !std.mem.eql(f32, initial, slot.weights);
        const trunk = std.mem.startsWith(u8, slot.name, "embeddings.") or std.mem.startsWith(u8, slot.name, "layers.") or std.mem.startsWith(u8, slot.name, "final_norm.");
        if (std.mem.startsWith(u8, slot.name, "gliner_neck.")) {
            moved_neck = moved_neck or changed;
        } else if (trunk) {
            moved_trunk = moved_trunk or changed;
        } else if (changed) {
            std.debug.print("head weight moved under pure distillation: {s}\n", .{slot.name});
            return error.TestUnexpectedResult;
        }
    }
    try std.testing.expect(moved_trunk and moved_neck);
    // The teacher identity and the objective bind into the run fingerprint.
    options.distillation.?.teacher = teacher.teacher(2);
    var other_teacher = try trainer.Trainer.init(a, &fixture.store, fixture.tokenizer.tokenizer(), source, config, &fixture.samples, parameters.items, options, null);
    defer other_teacher.deinit();
    try std.testing.expect(!std.mem.eql(u8, &distilled.fingerprint, &other_teacher.fingerprint));
    options.distillation = null;
    var plain = try trainer.Trainer.init(a, &fixture.store, fixture.tokenizer.tokenizer(), source, config, &fixture.samples, parameters.items, options, null);
    defer plain.deinit();
    try std.testing.expect(!std.mem.eql(u8, &distilled.fingerprint, &plain.fingerprint));
}

test "boundary native trainer prefetches the next microbatch's teacher states without changing the run" {
    const a = std.testing.allocator;
    var fixture: NeckedFixture = undefined;
    try fixture.init(a, false);
    defer fixture.deinit();
    // Two epochs with accumulation over an odd batch count cross an epoch
    // boundary and an end-of-epoch partial flush.
    var runs: [2]struct { losses: std.ArrayListUnmanaged(f32) = .empty, weights: std.ArrayListUnmanaged(f32) = .empty, calls: usize = 0, steps: usize = 0, prefetched: u64 = 0 } = .{ .{}, .{} };
    defer for (&runs) |*result| {
        result.losses.deinit(a);
        result.weights.deinit(a);
    };
    for (&runs, [_]bool{ false, true }) |*result, prefetch| {
        var teacher = SyntheticTeacher{ .hidden = fixture.config.encoder.hidden_size };
        var options = NeckedFixture.options(teacher.teacher(1));
        options.run.epochs = 2;
        options.run.accumulation = 2;
        if (prefetch) options.distillation.?.prefetch = std.testing.io;
        var distilled = try trainer.Trainer.init(a, &fixture.store, fixture.tokenizer.tokenizer(), fixture.source, fixture.config, &fixture.samples, fixture.parameters.items, options, null);
        defer distilled.deinit();
        while (try distilled.next(null)) |report| {
            const terms = report.terms orelse continue;
            try result.losses.append(a, terms.distillation);
            result.steps += 1;
        }
        result.calls = teacher.calls;
        result.prefetched = distilled.prefetched_microbatches;
        for (distilled.optimizer.owner.regular_params.items) |slot| try result.weights.appendSlice(a, slot.weights);
    }
    try std.testing.expect(runs[0].steps > 2);
    try std.testing.expectEqual(runs[0].steps, runs[1].steps);
    // One encode per microbatch either way: never past the run's end.
    try std.testing.expectEqual(runs[0].steps, runs[0].calls);
    try std.testing.expectEqual(runs[1].steps, runs[1].calls);
    try std.testing.expectEqual(@as(u64, 0), runs[0].prefetched);
    try std.testing.expectEqual(@as(u64, runs[1].steps - 1), runs[1].prefetched);
    try std.testing.expectEqualSlices(f32, runs[0].losses.items, runs[1].losses.items);
    try std.testing.expectEqualSlices(f32, runs[0].weights.items, runs[1].weights.items);
}

test "boundary native trainer fits an identity neck to the teacher before the first update" {
    const a = std.testing.allocator;
    var fixture: NeckedFixture = undefined;
    try fixture.init(a, true);
    defer fixture.deinit();
    const h = fixture.config.encoder.hidden_size;
    var teacher = SyntheticTeacher{ .hidden = h };
    var options = NeckedFixture.options(teacher.teacher(1));
    options.distillation.?.fit = .{ .rows = 5 };
    var fitted = try trainer.Trainer.init(a, &fixture.store, fixture.tokenizer.tokenizer(), fixture.source, fixture.config, &fixture.samples, fixture.parameters.items, options, null);
    defer fitted.deinit();
    // The fit runs before training: the teacher saw the fit rows, and the
    // optimizer's neck is no longer the identity.
    try std.testing.expect(teacher.calls > 0);
    var found = false;
    for (fitted.optimizer.owner.regular_params.items) |slot| {
        if (!std.mem.eql(u8, slot.name, "gliner_neck.weight")) continue;
        found = true;
        var identity = true;
        for (slot.weights, 0..) |value, index| {
            try std.testing.expect(std.math.isFinite(value));
            identity = identity and value == @as(f32, if (index / h == index % h) 1 else 0);
        }
        try std.testing.expect(!identity);
    }
    try std.testing.expect(found);
    // The fit settings are part of the run identity.
    options.distillation.?.fit = .{};
    var unfitted = try trainer.Trainer.init(a, &fixture.store, fixture.tokenizer.tokenizer(), fixture.source, fixture.config, &fixture.samples, fixture.parameters.items, options, null);
    defer unfitted.deinit();
    try std.testing.expect(!std.mem.eql(u8, &fitted.fingerprint, &unfitted.fingerprint));
    // A neck that is not the identity cannot be fitted this way.
    var other: NeckedFixture = undefined;
    try other.init(a, false);
    defer other.deinit();
    options.distillation.?.fit = .{ .rows = 5 };
    try std.testing.expectError(error.BoundaryNeckFitNeedsIdentity, trainer.Trainer.init(a, &other.store, other.tokenizer.tokenizer(), other.source, other.config, &other.samples, other.parameters.items, options, null));
}

/// Records the first step's routed student states, concatenated.
const StateRecorder = struct {
    allocator: std.mem.Allocator,
    states: ?[]f32 = null,
    fn observer(self: *StateRecorder) @import("boundary_distillation.zig").Observer {
        return .{ .ptr = self, .observe = observe };
    }
    fn observe(raw: *anyopaque, _: usize, groups: []const @import("boundary_distillation.zig").Group) !void {
        const self: *StateRecorder = @ptrCast(@alignCast(raw));
        if (self.states != null) return;
        var total: usize = 0;
        for (groups) |group| total += group.student.len;
        const out = try self.allocator.alloc(f32, total);
        var at: usize = 0;
        for (groups) |group| {
            @memcpy(out[at..][0..group.student.len], group.student);
            at += group.student.len;
        }
        self.states = out;
    }
};

fn firstStates(a: std.mem.Allocator, fixture: *NeckedFixture, execution: controller.Execution) ![]f32 {
    const gib: usize = 1024 * 1024 * 1024;
    var teacher = SyntheticTeacher{ .hidden = fixture.config.encoder.hidden_size };
    var recorder = StateRecorder{ .allocator = a };
    errdefer if (recorder.states) |states| a.free(states);
    const options = trainer.Options{ .execution = execution, .run = .{ .mode = .full, .epochs = 1, .batch_size = 2, .accumulation = 1, .seed = 918, .encoder_lr = 0.001, .task_lr = 0.002 }, .source_reserved_bytes = 16 * 1024 * 1024, .distillation = .{ .teacher = teacher.teacher(3), .observer = recorder.observer() }, .limits = .{ .max_host_bytes = 4 * gib, .max_backend_bytes = 4 * gib, .max_combined_bytes = 12 * gib } };
    var owner = try trainer.Trainer.init(a, &fixture.store, fixture.tokenizer.tokenizer(), fixture.source, fixture.config, &fixture.samples, fixture.parameters.items, options, null);
    defer owner.deinit();
    while (recorder.states == null) _ = (try owner.next(null)) orelse return error.TestUnexpectedResult;
    return recorder.states.?;
}

test "boundary native trainer resident Metal first step computes the native objective" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!@import("../../backends/metal_runtime.zig").metalDeviceAvailable()) return error.SkipZigTest;
    const a = std.testing.allocator;
    // The published base head (two boundary-attention layers, window 128,
    // pool 384) on the tiny ModernBERT trunk.
    const boundary = @import("../../models/gliner_boundary.zig");
    var published = NeckedFixture.modernConfig();
    {
        const bytes = try @import("../../util/c_file.zig").readFile(a, "testdata/gliner25/models/base/config.json");
        defer a.free(bytes);
        const parsed = try std.json.parseFromSlice(std.json.Value, a, bytes, .{});
        defer parsed.deinit();
        published.head = try boundary.parseHeadConfig(parsed.value.object.get("boundary_head").?.object);
    }
    // Real ModernBERT-base widths with two layers, and a width sweep.
    const Width = struct { hidden: u32, heads: u32, intermediate: u32, layers: u32 = 2, vocab: u32 = 512, global_every: u32 = 3 };
    const widths = [_]Width{.{ .hidden = 64, .heads = 4, .intermediate = 96, .layers = 2, .global_every = 2 }};
    var configs: [3 + widths.len]struct { []const u8, boundary.Config } = undefined;
    var deberta_wide = helper.config();
    deberta_wide.encoder.hidden_size = 64;
    deberta_wide.encoder.num_attention_heads = 4;
    deberta_wide.encoder.intermediate_size = 256;
    configs[0] = .{ "deberta-wide", deberta_wide };
    configs[1] = .{ "modernbert", NeckedFixture.modernConfig() };
    configs[2] = .{ "modernbert-published-head", published };
    for (widths, configs[3..]) |width, *out| {
        var wide = published;
        wide.encoder.hidden_size = width.hidden;
        wide.encoder.num_attention_heads = width.heads;
        wide.encoder.intermediate_size = width.intermediate;
        wide.encoder.num_hidden_layers = width.layers;
        wide.encoder.vocab_size = width.vocab;
        wide.encoder.global_attn_every_n_layers = width.global_every;
        out.* = .{ "width", wide };
    }
    var mismatched = false;
    for (configs) |entry| {
        var fixture: NeckedFixture = undefined;
        try fixture.initConfig(a, entry[1], false);
        defer fixture.deinit();
        const cpu = try firstStates(a, &fixture, .native);
        defer a.free(cpu);
        const metal = try firstStates(a, &fixture, .resident_metal);
        defer a.free(metal);
        var scale: f32 = 0;
        var worst: f32 = 0;
        for (cpu, metal) |want, got| {
            scale = @max(scale, @abs(want));
            worst = @max(worst, @abs(want - got));
        }
        const e = entry[1].encoder;
        std.debug.print("{s} hidden={d} heads={d} layers={d} global_every={d}: routed states max |native - metal|={d} (scale {d})\n", .{ entry[0], e.hidden_size, e.num_attention_heads, e.num_hidden_layers, e.global_attn_every_n_layers, worst, scale });
        if (worst > 1e-4 * @max(1, scale)) mismatched = true;
    }
    try std.testing.expect(!mismatched);
}

test "resident Metal ModernBERT encoder nodes match the interpreter" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!@import("../../backends/metal_runtime.zig").metalDeviceAvailable()) return error.SkipZigTest;
    const a = std.testing.allocator;
    const encoder_graph = @import("boundary_encoder_graph.zig");
    const seeded = @import("../../graph/seeded_training.zig");
    const interpreter = @import("../../graph/interpreter.zig");
    const device_fixture = @import("../../graph/resident_training_fixture.zig");
    var config = NeckedFixture.modernConfig();
    config.encoder.hidden_size = 64;
    config.encoder.num_attention_heads = 4;
    config.encoder.intermediate_size = 96;
    config.encoder.num_hidden_layers = 2;
    config.encoder.global_attn_every_n_layers = 2;
    var samples = try dataset(a);
    defer samples.deinit();
    var tokenizer = helper.TestTokenizer{};
    var first = try samples.sample(0, null, null);
    defer first.deinit();
    var second = try samples.sample(1, null, null);
    defer second.deinit();
    var prepared = try processor.prepare(a, tokenizer.tokenizer(), &.{ .{ .text = first.row.text, .schema = &first.schema }, .{ .text = second.row.text, .schema = &second.schema } }, .{});
    defer prepared.deinit();
    const layout = try encoder_graph.layoutFromPrepared(&config, &prepared, .{});
    var graph = ml.Graph.init(a);
    defer graph.deinit();
    var builder = ml.Builder.init(&graph);
    var built = try encoder_graph.buildWithProfile(&builder, &config, layout, .train, .materialized_v1, .{});
    defer built.deinit();
    var names_list = std.ArrayListUnmanaged([]const u8).empty;
    defer names_list.deinit(a);
    var nodes_list = std.ArrayListUnmanaged(ml.NodeId).empty;
    defer nodes_list.deinit(a);
    for ([_][]const u8{ "embedding", "layer0", "encoder", "text", "queries", "classifications", "parents", "relations" }, [_]ml.NodeId{ built.regions.embedding_output, built.regions.layers[0].output, built.nodes.encoder, built.nodes.text, built.nodes.queries, built.nodes.classifications, built.nodes.parents, built.nodes.relation_queries }) |name, node| {
        if (node == ml.null_node) continue;
        try names_list.append(a, name);
        try nodes_list.append(a, node);
    }
    const names = names_list.items;
    const nodes = nodes_list.items;
    const seeds = try a.alloc(ml.autodiff.Seed, nodes.len);
    defer a.free(seeds);
    for (nodes, seeds, 0..) |node, *seed, i| {
        var name: [64]u8 = undefined;
        seed.* = .{ .output = node, .cotangent = try builder.parameter(try std.fmt.bufPrint(&name, "__probe_cotangent_{d}", .{i}), graph.node(node).output_shape) };
        try graph.markOutput(node);
    }
    var wrt = std.ArrayListUnmanaged(ml.NodeId).empty;
    defer wrt.deinit(a);
    // Deterministic weights and step bindings, as host values.
    var host = std.ArrayListUnmanaged(struct { node: ml.NodeId, dims: []i32, f32s: ?[]f32 = null, i32s: ?[]const i32 = null }).empty;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    for (graph.parameters.items) |id| {
        const node = graph.node(id);
        const name = graph.parameterName(node);
        if (std.mem.startsWith(u8, name, "__")) continue;
        try wrt.append(a, id);
        const shape = node.output_shape;
        const values = try scratch.alloc(f32, @intCast(shape.numElements().?));
        for (values, 0..) |*v, index| v.* = if (std.mem.indexOf(u8, name, "norm") != null) 1 else 0.05 * @sin(@as(f32, @floatFromInt(index + @as(usize, id) * 11 + 1)));
        const dims = try scratch.alloc(i32, shape.rank_);
        for (dims, shape.dims[0..shape.rank_]) |*d, size| d.* = @intCast(size);
        try host.append(scratch, .{ .node = id, .dims = dims, .f32s = values });
    }
    var bound = try encoder_graph.bindPrepared(a, &built, &config, &prepared, .{ .seed = 1, .micro_batch = 0 });
    defer bound.deinit();
    for (bound.bindings) |binding| {
        const dims = try scratch.alloc(i32, binding.shape.rank_);
        for (dims, binding.shape.dims[0..binding.shape.rank_]) |*d, size| d.* = @intCast(size);
        switch (binding.values) {
            .f32 => |values| try host.append(scratch, .{ .node = binding.node, .dims = dims, .f32s = @constCast(values) }),
            .i32 => |values| try host.append(scratch, .{ .node = binding.node, .dims = dims, .i32s = values }),
        }
    }
    // Interpreter on the CPU.
    var store = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
    defer store.deinitOwned();
    var compute = native.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    const cpu = compute.computeBackend();
    var cpu_inputs = std.ArrayListUnmanaged(interpreter.RuntimeInput).empty;
    defer {
        for (cpu_inputs.items) |input| cpu.free(input.value);
        cpu_inputs.deinit(a);
    }
    for (host.items) |item| try cpu_inputs.append(a, .{ .node_id = item.node, .value = if (item.f32s) |v| try cpu.fromFloat32Shape(v, item.dims) else (try cpu.fromInt32Shape(item.i32s.?, item.dims)).? });
    var reference = try interpreter.execute(a, &graph, &cpu, .{ .runtime_inputs = cpu_inputs.items, .strict_integer_constants = true });
    defer reference.deinit(&cpu);
    // The resident Metal training session.
    var device = try device_fixture.Device.init(a);
    defer device.deinit();
    const metal = device.backend.computeBackend();
    var metal_inputs = std.ArrayListUnmanaged(interpreter.RuntimeInput).empty;
    defer {
        for (metal_inputs.items) |input| metal.free(input.value);
        metal_inputs.deinit(a);
    }
    for (host.items) |item| try metal_inputs.append(a, .{ .node_id = item.node, .value = if (item.f32s) |v| try metal.residentTrainingPrimitive(&.{ .upload_f32 = .{ .values = v, .shape = item.dims } }, .{}) else try metal.residentTrainingPrimitive(&.{ .upload_i32 = .{ .values = item.i32s.?, .shape = item.dims } }, .{}) });
    var session = try seeded.Session.init(a, &graph, seeds, wrt.items, .{ .execution = .resident_metal, .allow_no_gradients = true });
    defer session.deinit();
    var tape = try session.forward(&metal, metal_inputs.items, .{ .binding = @splat(3), .optimizer_step = 0, .microbatch = 0 }, null);
    defer tape.deinit();
    var failed = false;
    for (names, 0..) |name, i| {
        const want = try cpu.toFloat32(reference.outputs[i], a);
        defer a.free(want);
        const got = try metal.toFloat32(try tape.logits(i), a);
        defer a.free(got);
        var worst: f32 = 0;
        var scale: f32 = 0;
        for (want, got) |w, g| {
            worst = @max(worst, @abs(w - g));
            scale = @max(scale, @abs(w));
        }
        std.debug.print("resident Metal {s}: max |interpreter - resident|={d} (scale {d})\n", .{ name, worst, scale });
        failed = failed or worst > 1e-4 * @max(1, scale);
    }
    try std.testing.expect(!failed);
}

test "boundary native trainer fits the same neck on Metal as on the CPU" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!@import("../../backends/metal_runtime.zig").metalDeviceAvailable()) return error.SkipZigTest;
    const a = std.testing.allocator;
    var fixture: NeckedFixture = undefined;
    try fixture.init(a, true);
    defer fixture.deinit();
    var teacher = SyntheticTeacher{ .hidden = fixture.config.encoder.hidden_size };
    var necks: [2][]f32 = undefined;
    for ([_]controller.Execution{ .native, .resident_metal }, &necks) |execution, *out| {
        var options = NeckedFixture.options(teacher.teacher(1));
        options.execution = execution;
        options.distillation.?.fit = .{ .rows = 5 };
        var owner = try trainer.Trainer.init(a, &fixture.store, fixture.tokenizer.tokenizer(), fixture.source, fixture.config, &fixture.samples, fixture.parameters.items, options, null);
        defer owner.deinit();
        try owner.optimizer.ensureHostState(null);
        out.* = for (owner.optimizer.owner.regular_params.items) |slot| {
            if (std.mem.eql(u8, slot.name, "gliner_neck.weight")) break try a.dupe(f32, slot.weights);
        } else return error.TestUnexpectedResult;
    }
    defer for (necks) |values| a.free(values);
    var worst: f32 = 0;
    for (necks[0], necks[1]) |cpu, metal| worst = @max(worst, @abs(cpu - metal));
    std.debug.print("neck fit CPU vs Metal: max difference {d}\n", .{worst});
    try std.testing.expect(worst < 1e-3);
}
