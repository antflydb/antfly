// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const build_options = @import("build_options");
const trainer = @import("gliner_boundary_native_trainer.zig");
const controller = @import("seeded_gradient_trainer.zig");
const data = @import("gliner_boundary_dataset.zig");
const run = @import("gliner_boundary_run.zig");
const step = @import("gliner_boundary_train_step.zig");
const helper = @import("gliner_boundary_train_step_test.zig");
const processor = @import("../pipelines/gliner_boundary_processor.zig");
const native = @import("../ops/native_compute.zig");
const bundle = @import("../models/gliner_boundary_bundle.zig");
const Tensor = @import("../backends/tensor.zig").Tensor;
const metal_runtime = @import("../backends/metal_runtime.zig");
const metal_tensor = @import("../backends/metal_tensor.zig");

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
    try std.testing.expectEqual(@as(usize, 0), receipt.scalar_download_bytes % 12);
    try std.testing.expect(receipt.scalar_upload_bytes + receipt.scalar_download_bytes <= receipt.selected_slots * 144 + 64);
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
    var samples = try dataset(a);
    defer samples.deinit();
    var tokenizer = helper.TestTokenizer{};
    var config = helper.config();
    if (activation_profile == .layer_recompute_v1) config.encoder.num_hidden_layers = 2;
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
        const canonical = if (std.mem.startsWith(u8, name, "embeddings.") or std.mem.startsWith(u8, name, "encoder.")) try std.fmt.allocPrint(scratch, "encoder.{s}", .{name}) else name;
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
        if (execution == .resident_metal) {
            try std.testing.expectEqual(@as(usize, if (mode == .full) 0 else expected.backend.admission.frozen_parameters), expected.backend.receipt.upload_tensors);
            if (mode == .heads) try std.testing.expect(expected.backend.receipt.upload_bytes > 0);
        }
        var reports = std.ArrayListUnmanaged(trainer.Report).empty;
        defer reports.deinit(a);
        while (try nextObserved(expected)) |report| {
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

test "boundary native trainer composes immutable batches full and heads training cancellation and durable partial resume" {
    try exercise(std.testing.allocator, .native, .materialized_v1);
}

test "boundary native trainer resident Metal composes tiny full and heads jobs with exact durable resume" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!metal_runtime.metalDeviceAvailable()) return error.SkipZigTest;
    try exercise(std.testing.allocator, .resident_metal, .materialized_v1);
}

test "boundary native trainer replay attention full and heads jobs preserve cancellation and partial resume identity" {
    try exercise(std.testing.allocator, .native, .replay_tiled_v1);
}

test "boundary native trainer replay attention resident Metal full and heads jobs preserve partial resume identity" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!metal_runtime.metalDeviceAvailable()) return error.SkipZigTest;
    try exercise(std.testing.allocator, .resident_metal, .replay_tiled_v1);
}

test "boundary native trainer regional recomputation two layers full and heads preserve cancellation and exact partial resume" {
    try exerciseWithActivation(std.testing.allocator, .native, .replay_tiled_v1, .layer_recompute_v1);
}

test "boundary native trainer regional recomputation resident Metal two layers full and heads preserve exact partial resume" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!metal_runtime.metalDeviceAvailable()) return error.SkipZigTest;
    try exerciseWithActivation(std.testing.allocator, .resident_metal, .replay_tiled_v1, .layer_recompute_v1);
}

test "boundary native trainer allocation failures distinguish phase limits resize recovery and backing OOM" {
    const Budget = @import("../runtime/bounded_allocator.zig").BoundedAllocator;
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
    try std.testing.expectEqualSlices(u8, &([_]u8{7} ** 16), bytes);

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
