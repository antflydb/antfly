// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const platform = @import("antfly_platform");
const ml = @import("ml").graph;
const train = @import("training.zig");
const objective = @import("objective.zig");
const native = @import("../../ops/native_compute.zig");
const backend = @import("../gliner/boundary_training_backend.zig");
const run = @import("../gliner/boundary_run.zig");
const interpreter = @import("../../graph/interpreter.zig");
const safetensors = @import("../../models/safetensors.zig");
const files = @import("../../util/c_file.zig");
const modern = @import("../../architectures/modern_bert.zig");

test "laya training ReLU uses the PyTorch zero subgradient" {
    const a = std.testing.allocator;
    var graph = ml.Graph.init(a);
    defer graph.deinit();
    var b = ml.Builder.init(&graph);
    const x = try b.parameter("x", ml.Shape.init(.f32, &.{4}));
    const y = try @import("graph.zig").relu(&b, x);
    const loss = try b.reduceSum(y, &.{0});
    try graph.markOutput(loss);
    var gradients = try ml.autodiff.gradient(a, &graph, loss, &.{x});
    defer gradients.deinit();
    gradients.graph.outputs.clearRetainingCapacity();
    try gradients.graph.markOutput(gradients.param_grads[0]);
    var store = native.WeightStore{ .allocator = a, .resident_weights = .{}, .lazy_weights = .{} };
    const values = [_]f32{ -1, -0.0, 0, 1 };
    const original = run.Parameter{ .name = "x", .canonical_name = "x", .dimensions = &.{4}, .values = &values, .kind = .original };
    const selected = train.controller.Parameter{ .name = "x", .dimensions = &.{4}, .values = &values, .group = 0 };
    const owner = try backend.Owner.init(a, &store, &.{original}, &.{selected}, if (platform.env.getenv("ANTFLY_LAYA_METAL") != null) .resident_metal else .native, .{}, null);
    defer owner.deinit();
    const cb = &owner.cb;
    const input = try cb.fromFloat32Shape(&.{ -1, -0.0, 0, 1 }, &.{4});
    defer cb.free(input);
    var result = try interpreter.execute(a, &gradients.graph, cb, .{ .runtime_inputs = &.{.{ .node_id = gradients.id_map[x], .value = input }} });
    defer result.deinit(cb);
    const actual = try cb.toFloat32(result.outputs[0], a);
    defer a.free(actual);
    try std.testing.expectEqualSlices(f32, &.{ 0, 0, 0, 1 }, actual);
}

test "laya training forward objective and every parameter gradient match PyTorch" {
    const root = platform.env.getenv("ANTFLY_LAYA_REFERENCE") orelse return error.SkipZigTest;
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const reference_path = try std.fmt.allocPrint(scratch, "{s}/training_reference.json", .{root});
    const reference = try std.json.parseFromSlice(struct {
        sequences: []const struct { ids: []const i64, markers: []const i64, qtype: u8 },
        logits: []const []const f32,
        targets: []const []const f32,
        noise: []const f32,
        loss: f32,
        ce: f32,
        policy: f32,
        reward: f32,
        cotangent: []const f32,
    }, scratch, try files.readFile(scratch, reference_path), .{});
    const ref = reference.value;
    const examples = try scratch.alloc(train.Example, ref.sequences.len);
    for (examples, ref.sequences, ref.targets) |*dst, seq, target| dst.* = .{ .ids = seq.ids, .markers = seq.markers, .kind = @enumFromInt(seq.qtype), .target = target[0..seq.markers.len] };
    const config_path = try std.fmt.allocPrint(scratch, "{s}/model/config.json", .{root});
    const config = try modern.parseConfig(scratch, try files.readFile(scratch, config_path));
    var program = try train.Program.init(a, config, try train.bucketedLayout(examples, config), 0);
    defer program.deinit();
    var weights = try safetensors.MMapReader.openFileAbsolute(a, try std.fmt.allocPrint(scratch, "{s}/model/model.safetensors", .{root}));
    defer weights.deinit();
    var expected_gradients = try safetensors.MMapReader.openFileAbsolute(a, try std.fmt.allocPrint(scratch, "{s}/gradients.safetensors", .{root}));
    defer expected_gradients.deinit();
    const parameters = try train.parameters(scratch, &program.graph, &weights, 0);
    const originals = try scratch.alloc(run.Parameter, parameters.len);
    for (parameters, originals) |p, *o| o.* = .{ .name = p.name, .canonical_name = p.name, .dimensions = p.dimensions, .values = p.values, .kind = .original };
    var store = native.WeightStore{ .allocator = a, .resident_weights = .{}, .lazy_weights = .{} };
    const execution: train.controller.Execution = if (platform.env.getenv("ANTFLY_LAYA_METAL") != null) .resident_metal else .native;
    const owner = try backend.Owner.init(a, &store, originals, parameters, execution, .{}, null);
    defer owner.deinit();
    var cpu_vtable: @import("../../ops/ops.zig").ComputeBackend.VTable = undefined;
    @import("cpu.zig").install(&owner.cb, &cpu_vtable);
    const cb = &owner.cb;
    var trainer = try train.controller.Trainer.init(a, cb, parameters, .{ .execution = execution, .limits = .{ .max_state_bytes = 16 * 1024 * 1024 * 1024 }, .groups = &.{ .{ .schedule = .{ .constant = 0.000025 } }, .{ .schedule = .{ .constant = 0.0001 } } } });
    defer trainer.deinit();
    var prng = std.Random.DefaultPrng.init(715);
    const runtime = try train.inputs(scratch, cb, &program.graph, program.built, config, examples, prng.random(), false);
    defer for (runtime) |input| cb.free(input.value);
    var binding = try trainer.bind(&program.graph, null);
    defer binding.deinit();
    const combined = try std.mem.concat(scratch, interpreter.RuntimeInput, &.{ binding.inputs, runtime });
    const trace = platform.env.getenv("ANTFLY_LAYA_TRACE") != null;
    if (trace) for (program.built.traces.items) |entry| {
        try program.graph.markOutput(entry.node);
        try program.gradients.graph.markOutput(program.gradients.id_map[entry.node]);
    };
    // The trainer's execution path (a batched command frame on Metal).
    var forward = try train.executeFramed(a, &program.graph, cb, combined);
    defer forward.deinit(cb);
    if (trace) try compareTraces(scratch, root, &program, cb, forward.outputs[1..], "forward");
    const logits = try cb.toFloat32(forward.outputs[0], scratch);
    const l = try train.bucketedLayout(examples, config);
    for (examples, ref.logits, 0..) |e, expected, row| for (0..e.target.len) |k| try std.testing.expectApproxEqAbs(expected[k], logits[row * l.options + k], 2e-4);
    const rows = try scratch.alloc(objective.Row, examples.len);
    for (rows, examples) |*row, e| row.* = .{ .kind = e.kind, .target = e.target };
    // The PyTorch noise has the unpadded option width; the graph's options are
    // bucketed. Score the unpadded logits and pad the cotangent back.
    const width = (try train.layout(examples)).options;
    const compact = try scratch.alloc(f32, examples.len * width);
    for (0..examples.len) |row| @memcpy(compact[row * width ..][0..width], logits[row * l.options ..][0..width]);
    const loss = try objective.evaluate(a, .{}, rows, width, compact, ref.noise);
    defer loss.deinit(a);
    const cotangent = try scratch.alloc(f32, logits.len);
    @memset(cotangent, 0);
    for (0..examples.len) |row| @memcpy(cotangent[row * l.options ..][0..width], loss.gradient[row * width ..][0..width]);
    try std.testing.expectApproxEqAbs(ref.loss, loss.loss, 2e-4);
    try std.testing.expectApproxEqAbs(ref.ce, loss.ce, 2e-4);
    try std.testing.expectApproxEqAbs(ref.policy, loss.policy, 2e-4);
    try std.testing.expectApproxEqAbs(ref.reward, loss.reward, 2e-4);
    for (ref.cotangent, loss.gradient) |expected, actual| try std.testing.expectApproxEqAbs(expected, actual, 2e-4);
    const backward_inputs = try scratch.alloc(interpreter.RuntimeInput, combined.len + 1);
    for (combined, backward_inputs[0..combined.len]) |input, *dst| dst.* = .{ .node_id = program.gradients.id_map[input.node_id], .value = input.value };
    const seed = try cb.fromFloat32Shape(cotangent, &.{ @intCast(l.questions), @intCast(l.options) });
    defer cb.free(seed);
    backward_inputs[combined.len] = .{ .node_id = program.gradients.id_map[program.seed], .value = seed };
    var backward = try train.executeFramed(a, &program.gradients.graph, cb, backward_inputs);
    defer backward.deinit(cb);
    if (trace) try compareTraces(scratch, root, &program, cb, backward.outputs[program.wrt.len..], "backward");
    var worst: f32 = 0;
    var mismatches: usize = 0;
    var worst_relative: f64 = 0;
    for (program.wrt, backward.outputs[0..program.wrt.len]) |id, output| {
        const name = program.graph.parameterName(program.graph.node(id));
        var expected_tensor = try expected_gradients.readTensor(name);
        defer expected_tensor.deinit();
        const actual = try cb.toFloat32(output, scratch);
        const expected = expected_tensor.asFloat32();
        try std.testing.expectEqual(expected.len, actual.len);
        var error_max: f32 = 0;
        var magnitude: f32 = 0;
        var error_sq: f64 = 0;
        var expected_sq: f64 = 0;
        for (expected, actual) |want, got| {
            try std.testing.expect(std.math.isFinite(want) and std.math.isFinite(got));
            error_max = @max(error_max, @abs(want - got));
            magnitude = @max(magnitude, @abs(want));
            error_sq += @as(f64, want - got) * (want - got);
            expected_sq += @as(f64, want) * want;
        }
        worst = @max(worst, error_max);
        worst_relative = @max(worst_relative, @sqrt(error_sq / @max(expected_sq, 1e-30)));
        if (error_max > 5e-5 + magnitude * 0.002) {
            std.debug.print("Laya gradient mismatch {s}: error={d} scale={d} relative_l2={d}\n", .{ name, error_max, magnitude, @sqrt(error_sq / @max(expected_sq, 1e-30)) });
            mismatches += 1;
        }
    }
    std.debug.print("Laya {s}: {d} gradient tensors, max absolute error={d}, max relative L2 error={d}\n", .{ @tagName(execution), program.wrt.len, worst, worst_relative });
    try std.testing.expectEqual(@as(usize, 0), mismatches);
}

fn compareTraces(a: std.mem.Allocator, root: []const u8, program: *const train.Program, cb: *const @import("../../ops/ops.zig").ComputeBackend, outputs: []const @import("../../ops/ops.zig").CT, phase: []const u8) !void {
    var reader = try safetensors.MMapReader.openFileAbsolute(a, try std.fmt.allocPrint(a, "{s}/activations.safetensors", .{root}));
    defer reader.deinit();
    for (program.built.traces.items, outputs) |entry, output| {
        var expected = try reader.readTensor(entry.name);
        defer expected.deinit();
        const actual = try cb.toFloat32(output, a);
        var worst: f32 = 0;
        var scale: f32 = 0;
        var flips: usize = 0;
        for (expected.asFloat32(), actual, 0..) |want, got, i| {
            worst = @max(worst, @abs(want - got));
            scale = @max(scale, @abs(want));
            flips += @intFromBool((want > 0) != (got > 0));
            if (std.mem.endsWith(u8, entry.name, ".linear1") and (want > 0) != (got > 0) and flips <= 8)
                std.debug.print("Laya ReLU crossing {s} {s}[{d}]: reference={d} actual={d}\n", .{ phase, entry.name, i, want, got });
        }
        std.debug.print("Laya {s} {s}: error={d} scale={d} sign_flips={d}\n", .{ phase, entry.name, worst, scale, flips });
    }
}

test "laya training interrupted accumulation resumes to identical serving weights" {
    const root = platform.env.getenv("ANTFLY_LAYA_REFERENCE") orelse return error.SkipZigTest;
    const job = @import("job.zig");
    const a = std.testing.allocator;
    const io = std.testing.io;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var temp = std.testing.tmpDir(.{});
    defer temp.cleanup();
    const directory = try temp.dir.realPathFileAlloc(io, ".", scratch);
    var c = job.Config{
        .model_dir = try std.fs.path.join(scratch, &.{ root, "model" }),
        .train_file = try std.fs.path.join(scratch, &.{ root, "train.jsonl" }),
        .eval_file = try std.fs.path.join(scratch, &.{ root, "eval.jsonl" }),
        .output_dir = try std.fs.path.join(scratch, &.{ directory, "uninterrupted" }),
        .backend = if (platform.env.getenv("ANTFLY_LAYA_METAL") != null) .metal else .cpu,
        .epochs = 2,
        .batch_size = 2,
        .gradient_accumulation = 3,
        .encoder_lr = 0.0001,
        .head_lr = 0.001,
        .head_dropout = 0.1,
    };
    try job.execute(a, io, c);
    const full_weights = try std.fs.path.join(scratch, &.{ c.output_dir, "model", "model.safetensors" });
    c.output_dir = try std.fs.path.join(scratch, &.{ directory, "paused" });
    c.stop_after_microbatches = 1;
    try job.execute(a, io, c);
    c.resume_from = try std.fs.path.join(scratch, &.{ c.output_dir, "latest.safetensors" });
    c.output_dir = try std.fs.path.join(scratch, &.{ directory, "resumed" });
    c.stop_after_microbatches = null;
    try job.execute(a, io, c);
    const resumed_weights = try std.fs.path.join(scratch, &.{ c.output_dir, "model", "model.safetensors" });
    const digest = @import("data.zig").digest;
    try std.testing.expectEqual(digest(try files.readFile(scratch, full_weights)), digest(try files.readFile(scratch, resumed_weights)));
    // Reopen the exported artifact through the real serving factory.
    const factory = @import("../../architectures/session_factory.zig");
    var session = try factory.createNativeSession(a, try std.fs.path.join(scratch, &.{ c.output_dir, "model" }));
    defer session.close();
    const exported = factory.getLayaConfig(session) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(f32, 1), exported.scale(.choice, 3));
}

test "laya training with frozen lower layers keeps them exact and trains the rest" {
    const root = platform.env.getenv("ANTFLY_LAYA_REFERENCE") orelse return error.SkipZigTest;
    const job = @import("job.zig");
    const a = std.testing.allocator;
    const io = std.testing.io;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var temp = std.testing.tmpDir(.{});
    defer temp.cleanup();
    const directory = try temp.dir.realPathFileAlloc(io, ".", scratch);
    const c = job.Config{
        .model_dir = try std.fs.path.join(scratch, &.{ root, "model" }),
        .train_file = try std.fs.path.join(scratch, &.{ root, "train.jsonl" }),
        .eval_file = try std.fs.path.join(scratch, &.{ root, "eval.jsonl" }),
        .output_dir = try std.fs.path.join(scratch, &.{ directory, "frozen" }),
        .backend = if (platform.env.getenv("ANTFLY_LAYA_METAL") != null) .metal else .cpu,
        .epochs = 1,
        .batch_size = 2,
        .encoder_lr = 0.001,
        .head_lr = 0.001,
        .freeze_layers = 1,
    };
    try job.execute(a, io, c);
    var source = try safetensors.MMapReader.openFileAbsolute(a, try std.fs.path.join(scratch, &.{ root, "model", "model.safetensors" }));
    defer source.deinit();
    var exported = try safetensors.MMapReader.openFileAbsolute(a, try std.fs.path.join(scratch, &.{ c.output_dir, "model", "model.safetensors" }));
    defer exported.deinit();
    var frozen_count: usize = 0;
    var trainable_moved = false;
    var names = source.header.tensors.iterator();
    while (names.next()) |entry| {
        const name = entry.key_ptr.*;
        if (!std.mem.startsWith(u8, name, "encoder.")) continue;
        var before = try source.readTensor(name);
        defer before.deinit();
        var after = try exported.readTensor(name);
        defer after.deinit();
        const want = try train.floatValues(scratch, before);
        const got = try train.floatValues(scratch, after);
        if (train.frozen(name, c.freeze_layers)) {
            frozen_count += 1;
            try std.testing.expectEqualSlices(f32, want, got);
        } else if (std.mem.startsWith(u8, name, "encoder.layers.") and !std.mem.eql(f32, want, got)) {
            trainable_moved = true;
        }
    }
    try std.testing.expect(frozen_count > 0);
    try std.testing.expect(trainable_moved);
}

test "laya finetuned export probabilities and tokenization match PyTorch" {
    const root = platform.env.getenv("ANTFLY_LAYA_EXPORT_REFERENCE") orelse return error.SkipZigTest;
    const pipeline = @import("../../pipelines/laya.zig");
    const factory = @import("../../architectures/session_factory.zig");
    const hf = @import("inference_hf_tokenizer");
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const model_path = try std.fs.path.join(scratch, &.{ root, "model" });
    const ref_bytes = try files.readFile(scratch, try std.fs.path.join(scratch, &.{ root, "reference.json" }));
    const reference = try std.json.parseFromSlice(struct {
        states: []const []const u8,
        sequences: []const struct { ids: []const i64, markers: []const i64, qtype: u8 },
        probabilities: []const []const f32,
        act_probabilities: []const f32,
        records: ?[]const @import("data.zig").Record = null,
    }, scratch, ref_bytes, .{ .ignore_unknown_fields = true });
    const ref = reference.value;
    try std.testing.expect(ref.states.len > 0);
    try std.testing.expectEqual(ref.states.len, ref.sequences.len);
    try std.testing.expectEqual(ref.states.len, ref.probabilities.len);
    try std.testing.expectEqual(ref.states.len, ref.act_probabilities.len);
    var session = if (platform.env.getenv("ANTFLY_LAYA_METAL") != null) try factory.createMetalSession(a, model_path) else try factory.createNativeSession(a, model_path);
    defer session.close();
    const cfg = factory.getLayaConfig(session) orelse return error.TestUnexpectedResult;
    const tok_bytes = try files.readFileFromDir(scratch, model_path, "tokenizer.json");
    const tokenizer = try hf.HfTokenizer.loadFromBytes(a, tok_bytes);
    const tok = tokenizer.tokenizer();
    defer tok.deinitTokenizer();
    const questions = [_]pipeline.Question{
        .{ .name = "tool", .kind = .choice, .instruction = "which tool is needed?", .labels = &.{ "search", "fetch", "none" }, .descriptions = &.{ "", "", "" } },
        .{ .name = "urgency", .kind = .score, .instruction = "urgency?", .labels = &.{ "low", "medium", "high" }, .descriptions = &.{ "", "", "" } },
        .{ .name = "needed", .kind = .noul, .instruction = "is search needed?", .labels = &.{ "false", "true" }, .descriptions = &.{ "", "" } },
    };
    const tasks = try scratch.alloc(pipeline.Task, ref.states.len);
    if (ref.records) |records| try std.testing.expectEqual(tasks.len, records.len) else try std.testing.expectEqual(questions.len, tasks.len);
    for (tasks, ref.states, ref.sequences, 0..) |*task, state, seq, i| {
        const q: pipeline.Question = if (ref.records) |records| .{
            .name = records[i].id,
            .kind = records[i].kind,
            .instruction = records[i].instruction,
            .labels = records[i].labels,
            .descriptions = records[i].descriptions orelse blk: {
                const empty = try scratch.alloc([]const u8, records[i].labels.len);
                @memset(empty, "");
                break :blk empty;
            },
        } else questions[i];
        task.* = .{ .text = state, .question = q };
        const prepared = try pipeline.prepare(scratch, tok, cfg, task.*);
        try std.testing.expectEqualSlices(i64, seq.ids, prepared.ids);
        try std.testing.expectEqualSlices(i64, seq.markers, prepared.markers);
    }
    if (platform.env.getenv("ANTFLY_LAYA_BENCH_BATCH")) |batch_text| {
        const batch = try std.fmt.parseInt(usize, batch_text, 10);
        const samples = try std.fmt.parseInt(usize, platform.env.getenv("ANTFLY_LAYA_BENCH_SAMPLES") orelse "100", 10);
        if (batch == 0 or batch > 512 or samples == 0 or samples > 10000) return error.InvalidLayaInputs;
        const mixed = std.mem.eql(u8, platform.env.getenv("ANTFLY_LAYA_BENCH_PROFILE") orelse "fixed", "mixed");
        const batch_tasks = try scratch.alloc(pipeline.Task, batch);
        const times = try scratch.alloc(u64, samples);
        var cold_ns: u64 = 0;
        var resident_before: ?@import("../../ops/laya_metal.zig").Stats = null;
        for (0..samples + 10) |iteration| {
            var request_arena = std.heap.ArenaAllocator.init(a);
            defer request_arena.deinit();
            for (batch_tasks, 0..) |*task, i| task.* = tasks[if (mixed) (iteration * batch + i) % tasks.len else 0];
            const began = platform.time.monotonicNs();
            const result = try pipeline.executeWithScratch(request_arena.allocator(), a, session, tok, cfg, batch_tasks, null, null);
            const ns = platform.time.monotonicNs() - began;
            if (iteration == 0) cold_ns = ns;
            if (iteration == 9) resident_before = factory.layaResidentStats(session);
            if (iteration >= 10) times[iteration - 10] = ns;
            for (result.decisions, 0..) |decision, i| {
                const index = if (mixed) (iteration * batch + i) % tasks.len else 0;
                for (decision.probabilities, ref.probabilities[index]) |actual, expected| try std.testing.expectApproxEqAbs(expected, actual, 5e-5);
                try std.testing.expectApproxEqAbs(ref.act_probabilities[index], decision.act_probability, 5e-5);
            }
        }
        const resident_after = factory.layaResidentStats(session);
        if (resident_before) |before| {
            const after = resident_after.?;
            try std.testing.expectEqual(before.weight_upload_bytes, after.weight_upload_bytes);
            try std.testing.expectEqual(@as(u64, 0), after.activation_host_accesses);
            try std.testing.expectEqual(@as(u64, 0), after.intermediate_readbacks);
            try std.testing.expectEqual(@as(u64, 0), after.host_fallbacks);
            try std.testing.expectEqual(@as(u64, 0), after.cached_activation_bytes);
        }
        const payload = try std.json.Stringify.valueAlloc(scratch, .{ .samples_ns = times, .cold_request_ns = cold_ns, .resident = resident_after, .batch = batch, .mixed = mixed }, .{});
        std.debug.print("LAYA_BENCH_JSON {s}\n", .{payload});
        return;
    }
    var worst: f32 = 0;
    const sizes: []const usize = if (ref.records != null) &.{ 1, 4 } else &.{3};
    for (sizes) |batch_size| {
        var start: usize = 0;
        while (start < tasks.len) : (start += batch_size) {
            const end = @min(start + batch_size, tasks.len);
            var batch_arena = std.heap.ArenaAllocator.init(a);
            defer batch_arena.deinit();
            const result = try pipeline.execute(batch_arena.allocator(), session, tok, cfg, tasks[start..end], null);
            for (result.decisions, ref.probabilities[start..end], ref.act_probabilities[start..end]) |actual, expected, act| {
                try std.testing.expectEqual(expected.len, actual.probabilities.len);
                for (expected, actual.probabilities) |want, got| {
                    worst = @max(worst, @abs(want - got));
                    try std.testing.expectApproxEqAbs(want, got, 5e-5);
                }
                try std.testing.expectApproxEqAbs(act, actual.act_probability, 5e-5);
            }
        }
    }
    std.debug.print("Laya finetuned export backend={s}, max probability error={d}\n", .{ @tagName(session.backend()), worst });
}

test "laya frozen layers cover embeddings and the lowest encoder layers only" {
    try std.testing.expect(!train.frozen("encoder.embeddings.tok_embeddings.weight", 0));
    try std.testing.expect(train.frozen("encoder.embeddings.tok_embeddings.weight", 1));
    try std.testing.expect(train.frozen("encoder.embeddings.norm.weight", 2));
    try std.testing.expect(train.frozen("encoder.layers.1.attn.Wqkv.weight", 2));
    try std.testing.expect(!train.frozen("encoder.layers.2.attn.Wqkv.weight", 2));
    try std.testing.expect(!train.frozen("encoder.layers.12.mlp.Wo.weight", 2));
    try std.testing.expect(!train.frozen("encoder.final_norm.weight", 22));
    try std.testing.expect(!train.frozen("head.layers.0.attn.Wqkv.weight", 22));
}
