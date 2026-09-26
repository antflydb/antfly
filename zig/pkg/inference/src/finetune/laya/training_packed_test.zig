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

    fn init(a: std.mem.Allocator, scratch: std.mem.Allocator, dir: []const u8, config: modern.Config, examples: []const train.Example) !Harness {
        var program = try train.Program.init(a, config, try train.bucketedLayout(examples, config), 0);
        errdefer program.deinit();
        var weights = try safetensors.MMapReader.openFileAbsolute(a, try std.fs.path.join(scratch, &.{ dir, "model.safetensors" }));
        defer weights.deinit();
        const parameters = try train.parameters(scratch, &program.graph, &weights, 0);
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
    for ([_][]const u8{ "{\"mode\":\"question\"}", "{\"mode\":\"candidate\"}" }) |packing| {
        try synthetic.writeModel(a, std.testing.io, dir, packing, 128, 719);
        const config = try modern.parseConfig(scratch, try files.readFileFromDir(scratch, dir, "config.json"));
        const laya = config.laya.?;
        var examples: [states.len]train.Example = undefined;
        var rows: [states.len]tree.Row = undefined;
        for (&examples, &rows, states) |*e, *row, state| {
            row.* = (try tree.build(scratch, tok, laya, state, &questions))[0];
            e.* = try packedExample(scratch, row.*);
        }
        var session = try factory.createNativeSession(a, dir);
        defer session.close();
        const cb = try factory.getComputeBackend(session, a);
        defer cb.deinit();
        var worst: f32 = 0;
        // Each row alone, then both rows in one padded batch.
        for ([_][]const train.Example{ examples[0..1], examples[1..2], &examples }) |batch| {
            var harness = try Harness.init(a, scratch, dir, config, batch);
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
        std.debug.print("Laya packed {s}: training-graph vs serving max logit error={d}\n", .{ packing, worst });
        try std.testing.expect(worst < 1e-4);
        try tmp.dir.deleteFile(std.testing.io, "model.safetensors");
    }
}

test "laya training converts an unpacked checkpoint into a served packed model" {
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
    };
    try job.execute(a, io, c);
    const model_path = try std.fs.path.join(scratch, &.{ c.output_dir, "model" });
    var session = try factory.createNativeSession(a, model_path);
    defer session.close();
    const cfg = factory.getLayaConfig(session) orelse return error.TestUnexpectedResult;
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
