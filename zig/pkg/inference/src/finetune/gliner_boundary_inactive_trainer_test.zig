// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const trainer = @import("gliner_boundary_native_trainer.zig");
const previous = @import("gliner_boundary_native_trainer_test.zig");
const controller = @import("seeded_gradient_trainer.zig");
const data = @import("gliner_boundary_dataset.zig");
const run = @import("gliner_boundary_run.zig");
const step = @import("gliner_boundary_train_step.zig");
const helper = @import("gliner_boundary_train_step_test.zig");
const processor = @import("../pipelines/gliner_boundary_processor.zig");
const native = @import("../ops/native_compute.zig");
const bundle = @import("../models/gliner_boundary_bundle.zig");
const Tensor = @import("../backends/tensor.zig").Tensor;
const metal = @import("../backends/metal_runtime.zig");
const mib = 1024 * 1024;

fn dataset(a: std.mem.Allocator) !data.Dataset {
    var bytes = std.Io.Writer.Allocating.init(a);
    defer bytes.deinit();
    for (0..5) |i| {
        const active = i == 0 or i == 2;
        try bytes.writer.print("{{\"version\":1,\"id\":\"{d}\",\"text\":\"Ada Acme\",\"schema\":{{\"entities\":[\"person\"]", .{i});
        if (active) try bytes.writer.writeAll(",\"classifications\":[{\"name\":\"topic\",\"labels\":[\"meeting\",\"billing\"],\"mode\":\"multi\"}]");
        try bytes.writer.writeAll("},\"entities\":[{\"id\":\"a\",\"type\":\"person\",\"span\":{\"start\":0,\"end\":3}}]");
        if (active) try bytes.writer.writeAll(",\"classifications\":[{\"task\":\"topic\",\"labels\":[\"meeting\"]}]");
        try bytes.writer.writeAll("}\n");
    }
    return data.Dataset.fromBytes(a, bytes.written(), .{ .limits = .{ .max_host_bytes = 16 * mib } }, null, null);
}

fn exercise(a: std.mem.Allocator, execution: controller.Execution) !void {
    var samples = try dataset(a);
    defer samples.deinit();
    var tokenizer = helper.TestTokenizer{};
    var config = helper.config();
    // The schema-independent adapter inventory retains the real published
    // classifier dimensions. Keep the synthetic encoder to one short layer;
    // this is not a published checkpoint or a performance qualification.
    config.encoder.hidden_size = 384;
    config.encoder.num_attention_heads = 6;
    config.encoder.intermediate_size = 32;
    // create_mlp includes its dropout module only for positive probability;
    // the published classifier inventory therefore uses index 3, not 2.
    config.head.dropout = 0.125;
    var first = try samples.sample(0, null, null);
    defer first.deinit();
    var prepared = try processor.prepare(a, tokenizer.tokenizer(), &.{.{ .text = first.row.text, .schema = &first.schema }}, .{});
    defer prepared.deinit();
    var graph = try step.build(a, config, &prepared, &.{&first.schema}, .{}, .training, .{});
    defer graph.deinit();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var store = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
    defer store.deinitOwned();
    var parameters = std.ArrayListUnmanaged(run.Parameter).empty;
    for (graph.graph.parameters.items) |id| {
        const node = graph.graph.node(id);
        const borrowed = graph.graph.parameterName(node);
        if (std.mem.startsWith(u8, borrowed, "__")) continue;
        const name = try a.dupe(u8, borrowed);
        var enrolled = false;
        errdefer if (!enrolled) a.free(name);
        const shape = node.output_shape;
        const values = try scratch.alloc(f32, @intCast(shape.numElements().?));
        for (values, 0..) |*value, index| value.* = if (std.mem.endsWith(u8, name, ".bias")) 0 else if (std.mem.indexOf(u8, name, "norm") != null or std.mem.indexOf(u8, name, "LayerNorm") != null) 1 else 0.025 * @sin(@as(f32, @floatFromInt(index + @as(usize, id) * 11 + 1)));
        var tensor = try Tensor.initFloat32(a, name, shape.dims[0..shape.rank_], values);
        errdefer if (!enrolled) tensor.deinit();
        try store.resident_weights.put(a, name, .{ .tensor = tensor });
        enrolled = true;
        const dims = try scratch.alloc(i32, shape.rank_);
        for (dims, shape.dims[0..shape.rank_]) |*dim, size| dim.* = @intCast(size);
        const canonical = if (std.mem.startsWith(u8, name, "embeddings.") or std.mem.startsWith(u8, name, "encoder.")) try std.fmt.allocPrint(scratch, "encoder.{s}", .{name}) else name;
        try parameters.append(scratch, .{ .name = name, .canonical_name = canonical, .dimensions = dims, .values = tensor.asFloat32(), .kind = .original });
    }
    const source = bundle.Identity{ .backbone = config.backbone, .precision = .fp32, .weight = bundle.Digest.of("immutable inactive-classifier test weights"), .sidecars = .{ bundle.Digest.of("model"), bundle.Digest.of("encoder"), bundle.Digest.of("tokenizer"), bundle.Digest.of("tokenizer config") } };
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const checkpoint = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/inactive.safetensors", .{temporary.sub_path});
    defer a.free(checkpoint);
    for ([_]run.Mode{ .lora, .dora }) |mode| {
        const options = trainer.Options{
            .execution = execution,
            .run = .{ .mode = mode, .epochs = 1, .batch_size = 1, .accumulation = 2, .seed = 927, .task_lr = 0.01, .weight_decay = 0.1, .scheduler = .constant, .warmup_ratio = 0, .shuffle = false },
            .peft = .{ .kind = if (mode == .dora) .dora else .lora, .rank = 2, .alpha = 4, .targets = &.{"classification_head"} },
            .source_reserved_bytes = 16 * mib,
            .limits = .{ .max_host_bytes = 128 * mib, .max_backend_bytes = 256 * mib, .max_backend_host_bytes = 16 * mib, .max_combined_bytes = 512 * mib },
        };
        const expected = try trainer.Trainer.init(a, &store, tokenizer.tokenizer(), source, config, &samples, parameters.items, options, null);
        defer expected.deinit();
        var reports: [6]trainer.Report = undefined;
        for (&reports, 0..) |*report, index| {
            report.* = (try previous.nextObserved(expected)) orelse return error.TestUnexpectedResult;
            if (index < 5) {
                const fallback = index == 1 or index == 3 or index == 4;
                try std.testing.expectEqual(fallback, report.zero_loss_fallback);
                try std.testing.expect(report.terms.?.total > 0);
                try std.testing.expectEqual(if (fallback) @as(f32, 0) else report.terms.?.total, report.optimizer.loss.?);
                for (expected.optimizer.present) |present| try std.testing.expectEqual(index % 2 == 0, present);
                if (fallback) try std.testing.expectEqual(@as(usize, 0), expected.plan.?.selected_parameters.len);
            } else {
                try std.testing.expect(report.terms == null);
                try std.testing.expect(report.optimizer.optimizer_stepped);
            }
        }
        try std.testing.expect((try expected.next(null)) == null);
        try expected.optimizer.ensureHostState(null);
        for (expected.optimizer.owner.regular_params.items) |slot| try std.testing.expectEqual(@as(u32, 3), slot.adam_step_count);
        var actual = try trainer.Trainer.init(a, &store, tokenizer.tokenizer(), source, config, &samples, parameters.items, options, null);
        defer actual.deinit();
        for (reports, 0..) |want, index| {
            if (index == 1) {
                const before = try actual.optimizer.stateFingerprint(actual.fingerprint, null);
                const Cancel = struct {
                    fn check(_: ?*anyopaque) !void {
                        return error.Cancelled;
                    }
                };
                try std.testing.expectError(error.Cancelled, actual.next(.{ .check_fn = Cancel.check }));
                const saved_limit = actual.host_budget.limit;
                actual.host_budget.limit = actual.host_budget.live;
                try std.testing.expectError(error.BoundaryTrainingHostMemoryLimitExceeded, actual.next(null));
                actual.host_budget.limit = saved_limit;
                const saved_backing = actual.host_budget.backing;
                var failed = std.testing.FailingAllocator.init(saved_backing, .{ .fail_index = 0 });
                actual.host_budget.backing = failed.allocator();
                defer actual.host_budget.backing = saved_backing;
                try std.testing.expectError(error.OutOfMemory, actual.next(null));
                actual.host_budget.backing = saved_backing;
                try std.testing.expectEqual(before, try actual.optimizer.stateFingerprint(actual.fingerprint, null));
            }
            const got = (try previous.nextObserved(actual)) orelse return error.TestUnexpectedResult;
            try std.testing.expectEqualDeep(want.terms, got.terms);
            try std.testing.expectEqualDeep(want.optimizer, got.optimizer);
            try std.testing.expectEqual(want.zero_loss_fallback, got.zero_loss_fallback);
            try std.testing.expectEqual(want.decision_fingerprint, got.decision_fingerprint);
            if (index == 0 or index == 4) {
                const state = try actual.optimizer.stateFingerprint(actual.fingerprint, null);
                try actual.save(checkpoint, null);
                const resumed = try trainer.Trainer.init(a, &store, tokenizer.tokenizer(), source, config, &samples, parameters.items, options, null);
                errdefer resumed.deinit();
                _ = try resumed.restorePinned(checkpoint, state, null);
                try previous.expectSameState(actual, resumed);
                actual.deinit();
                actual = resumed;
            }
        }
        try previous.expectSameState(expected, actual);
        try std.testing.expectEqual(controller.Identity{ .optimizer_step = 3, .microbatch_step = 5 }, actual.optimizer.identity());
    }
}

test "boundary inactive adapter CPU classifier LoRA DoRA mixed accumulation zero objective and durable partial resume" {
    try exercise(std.testing.allocator, .native);
}

test "boundary inactive adapter Metal classifier LoRA DoRA mixed accumulation zero objective and durable partial resume" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    if (!metal.metalDeviceAvailable()) return error.SkipZigTest;
    try exercise(std.testing.allocator, .resident_metal);
}
