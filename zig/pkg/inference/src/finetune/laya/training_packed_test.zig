// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Tree-packed training agrees with tree-packed serving (models/laya/LAYA.md).
const std = @import("std");
const platform = @import("antfly_platform");
const train = @import("training.zig");
const native = @import("../../ops/native_compute.zig");
const backend = @import("../gliner/boundary_training_backend.zig");
const run = @import("../gliner/boundary_run.zig");
const safetensors = @import("../../models/safetensors.zig");
const files = @import("../../util/c_file.zig");
const modern = @import("../../architectures/modern_bert.zig");
const model = @import("../../models/laya.zig");
const pipeline = @import("../../pipelines/laya.zig");
const tree = @import("../../pipelines/laya_tree.zig");
const synthetic = @import("../../util/laya_synthetic.zig");
const factory = @import("../../architectures/session_factory.zig");
const packed_arch = @import("../../architectures/laya_packed.zig");

const states = [_][]const u8{
    "please find the invoice from acme and check whether the payment is late",
    "hello world",
};
const questions = [_]pipeline.Question{
    .{ .name = "tool", .kind = .choice, .instruction = "which tool is needed?", .labels = &.{ "search", "fetch", "none", "escalate" }, .descriptions = &.{ "", "", "", "" } },
    .{ .name = "urgency", .kind = .score, .instruction = "how urgent?", .labels = &.{ "low", "medium", "high" }, .descriptions = &.{ "", "", "" } },
    .{ .name = "late", .kind = .noul, .instruction = "is it late?", .labels = &.{ "false", "true" }, .descriptions = &.{ "", "" } },
};
const targets = [_][]const f32{ &.{ 0.1, 0.6, 0.2, 0.1 }, &.{ 0.2, 0.3, 0.5 }, &.{ 0.25, 0.75 } };

const Harness = struct {
    program: train.Program,
    owner: *backend.Owner,
    trainer: train.controller.Trainer,
    store: *native.WeightStore,
    vtable: *@import("../../ops/ops.zig").ComputeBackend.VTable,

    fn init(a: std.mem.Allocator, scratch: std.mem.Allocator, dir: []const u8, config: modern.Config, examples: []const train.Example, use_fused_attention: bool) !Harness {
        var program = try train.Program.initFrozenFused(a, config, try train.bucketedLayout(examples, config), 0, 0, null, use_fused_attention);
        errdefer program.deinit();
        var weights = try safetensors.MMapReader.openFileAbsolute(a, try std.fs.path.join(scratch, &.{ dir, "model.safetensors" }));
        defer weights.deinit();
        const parameters = try train.parameters(scratch, &program.graph, &weights, 0, null, 0);
        const originals = try scratch.alloc(run.Parameter, parameters.len);
        for (parameters, originals) |p, *o| o.* = .{ .name = p.name, .canonical_name = p.name, .dimensions = p.dimensions, .values = p.values, .kind = .original };
        const store = try scratch.create(native.WeightStore);
        store.* = .{ .allocator = a, .resident_weights = .{}, .lazy_weights = .{} };
        const owner = try backend.Owner.init(a, store, originals, parameters, .native, .{}, null);
        errdefer owner.deinit();
        const vtable = try scratch.create(@import("../../ops/ops.zig").ComputeBackend.VTable);
        @import("cpu.zig").install(&owner.cb, vtable);
        const trainer = try train.controller.Trainer.init(a, &owner.cb, parameters, .{ .execution = .native, .limits = .{ .max_state_bytes = 1024 * 1024 * 1024 }, .groups = &.{ .{ .schedule = .{ .constant = 0 } }, .{ .schedule = .{ .constant = 0 } } } });
        return .{ .program = program, .owner = owner, .trainer = trainer, .store = store, .vtable = vtable };
    }
    fn deinit(self: *Harness) void {
        self.trainer.deinit();
        self.owner.deinit();
        self.program.deinit();
    }
};

fn packedExample(a: std.mem.Allocator, row: tree.Row) !train.Example {
    const kinds = try a.alloc(model.QuestionType, row.questions());
    const row_targets = try a.alloc([]const f32, row.questions());
    for (row.question_index, kinds, row_targets) |index, *kind, *target| {
        kind.* = questions[index].kind;
        target.* = targets[index];
    }
    const packed_row = try a.create(train.Packed);
    packed_row.* = .{ .row = row, .kinds = kinds, .targets = row_targets };
    return .{ .ids = row.ids, .packed_row = packed_row };
}

test "laya packed training graph matches packed serving logits, alone and in a padded batch" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try tmp.dir.realPathFileAlloc(std.testing.io, ".", scratch);
    var words = synthetic.WordTokenizer{};
    const tok = words.tokenizer();
    // `fuse_layers` 3 fuses the head and the top encoder layer.
    const Case = struct { packing: []const u8, extra: []const u8 = "" };
    const pointer = ",\"decision_head\":\"pointer\"";
    for ([_]Case{ .{ .packing = "{\"mode\":\"question\"}" }, .{ .packing = "{\"mode\":\"candidate\"}" }, .{ .packing = "{\"mode\":\"question\",\"fuse_layers\":3}" }, .{ .packing = "{\"mode\":\"question\",\"question_first\":true}" }, .{ .packing = "{\"mode\":\"candidate\",\"question_first\":true}" }, .{ .packing = "{\"mode\":\"question\"}", .extra = pointer }, .{ .packing = "{\"mode\":\"question\",\"question_first\":true}", .extra = pointer } }) |case| for ([_]bool{ false, true }) |use_fused_attention| {
        const packing = case.packing;
        try synthetic.writeModelWith(a, std.testing.io, dir, packing, case.extra, 128, 719);
        const config = try modern.parseConfig(scratch, try files.readFileFromDir(scratch, dir, "config.json"));
        const laya = config.laya.?;
        var examples: [states.len]train.Example = undefined;
        var rows: [states.len]tree.Row = undefined;
        for (&examples, &rows, states) |*e, *row, state| {
            const built = try tree.build(scratch, tok, laya, state, &questions, null);
            try std.testing.expectEqual(@as(usize, 1), built.len);
            row.* = built[0];
            e.* = try packedExample(scratch, row.*);
        }
        var session = try factory.createNativeSession(a, dir);
        defer session.close();
        const cb = try factory.getComputeBackend(session, a);
        defer cb.deinit();
        var worst: f32 = 0;
        // Each row alone, then both rows in one padded batch.
        for ([_][]const train.Example{ examples[0..1], examples[1..2], &examples }) |batch| {
            var harness = try Harness.init(a, scratch, dir, config, batch, use_fused_attention);
            defer harness.deinit();
            const logits = try train.predict(a, &harness.program, &harness.trainer, config, batch);
            defer a.free(logits);
            const l = try train.bucketedLayout(batch, config);
            var decision: usize = 0;
            for (batch) |e| {
                const outputs = try packed_arch.forwardRow(&cb, a, config, laya, e.packed_row.?.row, null);
                defer {
                    for (outputs) |*output| output.deinit();
                    a.free(outputs);
                }
                const served = outputs[0].asFloat32();
                for (0..e.questions()) |qi| {
                    const n = e.question(qi).target.len;
                    for (served[qi * e.packed_row.?.row.width ..][0..n], logits[decision * l.options ..][0..n]) |want, got| worst = @max(worst, @abs(want - got));
                    decision += 1;
                }
            }
            try std.testing.expectEqual(@as(usize, l.questions), decision);
        }
        std.debug.print("Laya packed {s}{s} (fused attention {}): training-graph vs serving max logit error={d}\n", .{ packing, case.extra, use_fused_attention, worst });
        try std.testing.expect(worst < 1e-4);
        try tmp.dir.deleteFile(std.testing.io, "model.safetensors");
    };
}

// Every parameter gradient of a packed pointer-head model agrees between the
// native and the resident Metal training backends. Guards the resident Metal
// gather-from-LayerNorm fault that made every pointer score identical.
test "laya pointer head gradients agree between native and resident Metal" {
    if (!@import("build_options").enable_metal or !@import("../../backends/metal_runtime.zig").metalDeviceAvailable()) return error.SkipZigTest;
    const interpreter = @import("../../graph/interpreter.zig");
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try tmp.dir.realPathFileAlloc(std.testing.io, ".", scratch);
    var words = synthetic.WordTokenizer{};
    const tok = words.tokenizer();
    try synthetic.writeModelWith(a, std.testing.io, dir, "{\"mode\":\"question\"}", ",\"decision_head\":\"pointer\"", 128, 719);
    const config = try modern.parseConfig(scratch, try files.readFileFromDir(scratch, dir, "config.json"));
    var examples: [states.len]train.Example = undefined;
    for (&examples, states) |*e, state| e.* = try packedExample(scratch, (try tree.build(scratch, tok, config.laya.?, state, &questions, null))[0]);
    const l = try train.bucketedLayout(&examples, config);
    var results: [2][][]f32 = undefined;
    var names: [][]const u8 = undefined;
    for ([_]train.controller.Execution{ .native, .resident_metal }, &results) |execution, *out| {
        var program = try train.Program.init(a, config, l, 0);
        defer program.deinit();
        var weights = try safetensors.MMapReader.openFileAbsolute(a, try std.fs.path.join(scratch, &.{ dir, "model.safetensors" }));
        defer weights.deinit();
        const parameters = try train.parameters(scratch, &program.graph, &weights, 0, null, 0);
        const originals = try scratch.alloc(run.Parameter, parameters.len);
        for (parameters, originals) |p, *o| o.* = .{ .name = p.name, .canonical_name = p.name, .dimensions = p.dimensions, .values = p.values, .kind = .original };
        var store = native.WeightStore{ .allocator = a, .resident_weights = .{}, .lazy_weights = .{} };
        const owner = try backend.Owner.init(a, &store, originals, parameters, execution, .{}, null);
        defer owner.deinit();
        var cpu_vtable: @import("../../ops/ops.zig").ComputeBackend.VTable = undefined;
        @import("cpu.zig").install(&owner.cb, &cpu_vtable);
        const cb = &owner.cb;
        var trainer = try train.controller.Trainer.init(a, cb, parameters, .{ .execution = execution, .limits = .{ .max_state_bytes = 1024 * 1024 * 1024 }, .groups = &.{ .{ .schedule = .{ .constant = 0 } }, .{ .schedule = .{ .constant = 0 } } } });
        defer trainer.deinit();
        var prng = std.Random.DefaultPrng.init(715);
        const runtime = try train.inputs(scratch, cb, &program.graph, program.built, config, &examples, prng.random(), false, false);
        defer for (runtime) |input| cb.free(input.value);
        var binding = try trainer.bind(&program.graph, null);
        defer binding.deinit();
        const combined = try std.mem.concat(scratch, interpreter.RuntimeInput, &.{ binding.inputs, runtime });
        const backward_inputs = try scratch.alloc(interpreter.RuntimeInput, combined.len + 1);
        for (combined, backward_inputs[0..combined.len]) |input, *dst| dst.* = .{ .node_id = program.gradients.id_map[input.node_id], .value = input.value };
        const cotangent = try scratch.alloc(f32, l.questions * l.options);
        for (cotangent, 0..) |*v, i| v.* = @as(f32, @floatFromInt(@as(i32, @intCast(i % 7)) - 3)) * 0.25;
        const seed = try cb.fromFloat32Shape(cotangent, &.{ @intCast(l.questions), @intCast(l.options) });
        defer cb.free(seed);
        backward_inputs[combined.len] = .{ .node_id = program.gradients.id_map[program.seed], .value = seed };
        var backward = try train.executeFramed(a, &program.gradients.graph, cb, backward_inputs);
        defer backward.deinit(cb);
        out.* = try scratch.alloc([]f32, program.wrt.len);
        names = try scratch.alloc([]const u8, program.wrt.len);
        for (program.wrt, backward.outputs[0..program.wrt.len], out.*, names) |id, output, *dst, *name| {
            dst.* = try cb.toFloat32(output, scratch);
            name.* = try scratch.dupe(u8, program.graph.parameterName(program.graph.node(id)));
        }
    }
    var mismatches: usize = 0;
    for (names, results[0], results[1]) |name, want, got| {
        var error_sq: f64 = 0;
        var want_sq: f64 = 0;
        for (want, got) |x, y| {
            error_sq += @as(f64, x - y) * (x - y);
            want_sq += @as(f64, x) * x;
        }
        const relative = @sqrt(error_sq / @max(want_sq, 1e-30));
        if (relative > 1e-3) {
            std.debug.print("Laya pointer gradient mismatch {s}: relative L2 {d} (|native|={d})\n", .{ name, relative, @sqrt(want_sq) });
            mismatches += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 0), mismatches);
}

test "laya training converts an unpacked checkpoint into a served packed model" {
    try exerciseUnpackedToPackedParity(false, false, 0);
}

// Question-aware trunk (`packing.trunk_sees: "questions"`): the trainer's
// eval of its final weights must equal serving the exported model, dense
// and fused attention alike, and the export must carry the setting.
test "laya training exports a question-aware trunk that serves exactly" {
    try exerciseUnpackedToPackedParity(false, true, 0);
    try exerciseUnpackedToPackedParity(true, true, 0);
}

// Per-question upper layers (`packing.fuse_layers`): the same exactness, and
// the export carries the setting. The reference model has two encoder and
// two head layers, so 3 fuses the head and the top encoder layer.
test "laya training exports per-question upper layers that serve exactly" {
    try exerciseUnpackedToPackedParity(false, false, 3);
    try exerciseUnpackedToPackedParity(true, false, 3);
}

// Pointer head (`laya.decision_head`) on a scorer checkpoint: the head is
// initialized, trained, exported with the model, and served exactly.
test "laya training exports a new pointer head that serves exactly" {
    try exerciseParity(false, false, 0, .pointer);
    try exerciseParity(true, false, 0, .pointer);
}

// Regression coverage for the fused-attention eval dropout leak: a
// `Program`'s graph (and the fused op's graph-time `dropout_probability`)
// is built once and reused for both training steps and `predict` eval, so
// without a runtime `apply_dropout` toggle the trainer's own eval of its
// final weights applied live seeded dropout that the independently
// implemented serving path never did -- same weights on both sides, so any
// gap here is a real bug, not training drift. `force_fused_attention`
// exercises the fused path directly regardless of whether this small
// fixture would need it on its own.
test "laya training converts an unpacked checkpoint into a served packed model (forced fused attention)" {
    try exerciseUnpackedToPackedParity(true, false, 0);
}

fn exerciseUnpackedToPackedParity(force_fused_attention: bool, trunk_sees_questions: bool, fuse_layers: u32) !void {
    return exerciseParity(force_fused_attention, trunk_sees_questions, fuse_layers, null);
}

fn exerciseParity(force_fused_attention: bool, trunk_sees_questions: bool, fuse_layers: u32, decision_head: ?model.DecisionHead) !void {
    const root = platform.env.getenv("ANTFLY_LAYA_REFERENCE") orelse return error.SkipZigTest;
    const job = @import("job.zig");
    const hf = @import("inference_hf_tokenizer");
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
        .output_dir = try std.fs.path.join(scratch, &.{ directory, "packed" }),
        .epochs = 3,
        .encoder_lr = 0.0001,
        .head_lr = 0.001,
        .objective = .soft_ce,
        .packing = .question,
        .force_fused_attention = force_fused_attention,
        .trunk_sees_questions = trunk_sees_questions,
        .fuse_layers = fuse_layers,
        .decision_head = decision_head,
    };
    var admission = @import("../../runtime/tier/memory.zig").AdmissionController{};
    try job.execute(a, io, c, &admission);
    const model_path = try std.fs.path.join(scratch, &.{ c.output_dir, "model" });
    var session = try factory.createNativeSession(a, model_path);
    defer session.close();
    const cfg = factory.getLayaConfig(session) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(trunk_sees_questions, cfg.packing.trunk_sees_questions);
    try std.testing.expectEqual(fuse_layers, cfg.packing.fuse_layers);
    try std.testing.expectEqual(decision_head orelse .scorer, cfg.decision_head);
    if (decision_head == .pointer) {
        // A new pointer head starts as a uniform decision (zero query
        // projection over normalized rows), not a saturated softmax.
        const Report = struct { initial_eval: struct { soft_ce: f64 } };
        const report = try std.json.parseFromSlice(Report, scratch, try files.readFile(scratch, try std.fs.path.join(scratch, &.{ c.output_dir, "report.json" })), .{ .ignore_unknown_fields = true });
        std.debug.print("Laya new pointer head initial soft CE={d}\n", .{report.value.initial_eval.soft_ce});
        try std.testing.expect(report.value.initial_eval.soft_ce < 5);
        // Training moved the zero-initialized query projection.
        var weights = try safetensors.MMapReader.openFileAbsolute(a, try std.fs.path.join(scratch, &.{ model_path, "model.safetensors" }));
        defer weights.deinit();
        var q = try weights.readTensor("pointer.q.weight");
        defer q.deinit();
        const values = try train.floatValues(scratch, q);
        var largest: f32 = 0;
        for (values) |v| largest = @max(largest, @abs(v));
        std.debug.print("Laya new pointer head trained |pointer.q| max={d}\n", .{largest});
        try std.testing.expect(largest > 0);
    }
    try std.testing.expectEqual(model.PackingMode.question, cfg.packing.mode);
    // Serving the exported packed model reproduces the job's final evaluation.
    const Prediction = struct { kind: model.QuestionType, logits: []const f32, target: []const f32 };
    const predictions = try std.json.parseFromSlice([]const Prediction, scratch, try files.readFile(scratch, try std.fs.path.join(scratch, &.{ c.output_dir, "eval_predictions.json" })), .{ .ignore_unknown_fields = true });
    const records = try std.json.parseFromSlice([]const struct { text: []const u8, kind: model.QuestionType, instruction: []const u8, labels: []const []const u8 }, scratch, blk: {
        // eval.jsonl is newline-delimited; wrap it as one JSON array.
        const raw = try files.readFile(scratch, c.eval_file);
        var list = std.Io.Writer.Allocating.init(scratch);
        try list.writer.writeByte('[');
        var lines = std.mem.tokenizeScalar(u8, raw, '\n');
        var first = true;
        while (lines.next()) |line| {
            if (!first) try list.writer.writeByte(',');
            first = false;
            try list.writer.writeAll(line);
        }
        try list.writer.writeByte(']');
        break :blk list.written();
    }, .{ .ignore_unknown_fields = true });
    const tasks = try scratch.alloc(pipeline.Task, records.value.len);
    for (tasks, records.value) |*task, r| {
        const descriptions = try scratch.alloc([]const u8, r.labels.len);
        @memset(descriptions, "");
        task.* = .{ .text = r.text, .question = .{ .name = r.instruction, .kind = r.kind, .instruction = r.instruction, .labels = r.labels, .descriptions = descriptions } };
    }
    const tokenizer = try hf.HfTokenizer.loadFromBytes(a, try files.readFileFromDir(scratch, model_path, "tokenizer.json"));
    const tok = tokenizer.tokenizer();
    defer tok.deinitTokenizer();
    const result = try pipeline.execute(scratch, session, tok, cfg, tasks, null);
    try std.testing.expectEqual(@as(usize, 1), result.execution_chunks);
    var worst: f32 = 0;
    for (result.decisions, predictions.value) |decision, expected| {
        var max: f32 = -std.math.inf(f32);
        for (expected.logits) |z| max = @max(max, z);
        var sum: f32 = 0;
        for (expected.logits) |z| sum += @exp(z - max);
        for (decision.probabilities, expected.logits) |p, z| worst = @max(worst, @abs(p - @exp(z - max) / sum));
    }
    std.debug.print("Laya packed export serving vs training max probability error={d}\n", .{worst});
    try std.testing.expect(worst < 5e-5);
}
