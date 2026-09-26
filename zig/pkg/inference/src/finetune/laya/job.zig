// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Native Laya full finetuning, deterministic resume, and serving export.
const std = @import("std");
const platform = @import("antfly_platform");
const ml = @import("ml").graph;
const data = @import("data.zig");
const training = @import("training.zig");
const objective = @import("objective.zig");
const architecture = @import("graph.zig");
const modern = @import("../../architectures/modern_bert.zig");
const hf = @import("inference_hf_tokenizer");
const safetensors = @import("../../models/safetensors.zig");
const checkpoint = @import("../safetensors_checkpoint.zig");
const native = @import("../../ops/native_compute.zig");
const backend = @import("../gliner/boundary_training_backend.zig");
const run = @import("../gliner/boundary_run.zig");
const snapshot = @import("../../runtime/file_snapshot.zig");
const Assets = @import("assets.zig").Assets;
const Budget = @import("../../runtime/bounded_allocator.zig").BoundedAllocator;
const model = @import("../../models/laya.zig");
const memory = @import("../../runtime/tier/memory.zig");
const metal_tensor = @import("../../backends/metal_tensor.zig");

pub const Config = struct {
    version: u32 = 1,
    model_dir: []const u8,
    train_file: []const u8,
    eval_file: []const u8,
    output_dir: []const u8,
    calibration_file: ?[]const u8 = null,
    resume_from: ?[]const u8 = null,
    backend: enum { cpu, metal } = .cpu,
    epochs: u32 = 4,
    batch_size: u32 = 1,
    gradient_accumulation: u32 = 1,
    encoder_lr: f32 = 2.5e-5,
    head_lr: f32 = 1e-4,
    weight_decay: f32 = 0.01,
    max_grad_norm: f32 = 1,
    head_dropout: f32 = 0.1,
    objective: enum { rlcd, soft_ce } = .rlcd,
    group_size: u32 = 4,
    sigma_start: f32 = 0.4,
    sigma_end: f32 = 0.1,
    seed: u64 = 42,
    checkpoint_every_steps: u32 = 100,
    stop_after_microbatches: ?u64 = null,
    max_host_bytes: usize = 24 * 1024 * 1024 * 1024,
    /// Device (GPU/unified) memory ceiling for `backend: metal`. Covers
    /// resident weights and optimizer state, the resident AdamW transaction's
    /// transient snapshot, and the largest admitted layout's activations
    /// (models/laya/LAYA.md, "Training memory"). Ignored on `backend: cpu`,
    /// where the encoder runs in host memory bounded by `max_host_bytes`.
    max_backend_bytes: usize = 24 * 1024 * 1024 * 1024,
    /// Train (and export) a tree-packed layout (models/laya/LAYA.md). Null
    /// keeps the source checkpoint's layout; released checkpoints are unpacked.
    packing: ?model.PackingMode = null,
    max_packed_len: ?u32 = null,
    /// Keep the token embeddings and the lowest N encoder layers at their
    /// source values. Backward work and optimizer state cover only the rest.
    freeze_layers: u32 = 0,
};

pub fn validate(c: Config) !void {
    if (c.version != 1 or c.epochs == 0 or c.epochs > 10000 or c.batch_size == 0 or c.batch_size > 128 or
        c.gradient_accumulation == 0 or c.gradient_accumulation > 65536 or c.group_size < 2 or c.group_size > 64 or
        c.checkpoint_every_steps == 0 or c.max_host_bytes < 64 * 1024 * 1024 or c.max_host_bytes > 128 * 1024 * 1024 * 1024 or
        c.max_backend_bytes < 256 * 1024 * 1024 or c.max_backend_bytes > 64 * 1024 * 1024 * 1024)
        return error.InvalidLayaJob;
    for ([_]f32{ c.encoder_lr, c.head_lr, c.sigma_start, c.sigma_end, c.max_grad_norm }) |v| if (!std.math.isFinite(v) or v <= 0) return error.InvalidLayaJob;
    if (!std.math.isFinite(c.weight_decay) or c.weight_decay < 0 or !std.math.isFinite(c.head_dropout) or c.head_dropout < 0 or c.head_dropout >= 1) return error.InvalidLayaJob;
    for ([_][]const u8{ c.model_dir, c.train_file, c.eval_file, c.output_dir }) |value| if (!std.fs.path.isAbsolute(value)) return error.LayaJobRequiresAbsolutePaths;
    if (c.resume_from) |value| if (!std.fs.path.isAbsolute(value)) return error.LayaJobRequiresAbsolutePaths;
    if (c.calibration_file) |value| if (!std.fs.path.isAbsolute(value)) return error.LayaJobRequiresAbsolutePaths;
    if (c.stop_after_microbatches == 0) return error.InvalidLayaJob;
    if (c.max_packed_len != null and (c.packing == null or c.packing.? == .none)) return error.InvalidLayaJob;
}

/// Apply the job's packing override with the same bounds as `laya.packing`.
fn packing(c: Config, source: model.Config) !model.Packing {
    const mode = c.packing orelse return source.packing;
    if (mode == .none) return .{};
    const length: usize = c.max_packed_len orelse @min(4 * source.max_len, model.max_packed_len_limit);
    if (length < source.max_len or length > model.max_packed_len_limit) return error.InvalidLayaJob;
    return .{ .mode = mode, .max_packed_len = length };
}

fn path(a: std.mem.Allocator, dir: []const u8, name: []const u8) ![]const u8 {
    return std.fs.path.join(a, &.{ dir, name });
}
fn writeJson(io: std.Io, output: []const u8, value: anytype) !void {
    const file = try std.Io.Dir.cwd().createFile(io, output, .{ .exclusive = true });
    defer file.close(io);
    var buffer: [16384]u8 = undefined;
    var w = file.writer(io, &buffer);
    try std.json.Stringify.value(value, .{ .whitespace = .indent_2 }, &w.interface);
    try w.interface.writeByte('\n');
    try w.interface.flush();
    try file.sync(io);
}
fn event(io: std.Io, file: std.Io.File, value: anytype) !void {
    var buffer: [8192]u8 = undefined;
    var w = file.writerStreaming(io, &buffer);
    try std.json.Stringify.value(value, .{}, &w.interface);
    try w.interface.writeByte('\n');
    try w.interface.flush();
}

const Cache = struct {
    allocator: std.mem.Allocator,
    config: modern.Config,
    dropout: f32,
    freeze_layers: u32 = 0,
    frozen: []const training.Frozen = &.{},
    program: ?training.Program = null,
    last: architecture.Layout = .{ .batch = 0, .sequence = 0, .options = 0, .questions = 0 },
    fn get(self: *Cache, examples: []const training.Example) !*training.Program {
        const l = try training.bucketedLayout(examples, self.config);
        if (self.program == null or !std.meta.eql(l, self.last)) {
            if (self.program) |*p| p.deinit();
            self.program = null;
            self.program = try training.Program.initFrozen(self.allocator, self.config, l, self.dropout, self.freeze_layers);
            self.last = l;
        }
        self.program.?.frozen = self.frozen;
        return &self.program.?;
    }
    fn deinit(self: *Cache) void {
        if (self.program) |*p| p.deinit();
    }
};

const Prediction = struct { id: ?[]const u8 = null, kind: model.QuestionType, logits: []const f32, target: []const f32 };
const Metrics = struct { examples: usize, soft_ce: f64, accuracy: f64, ordinal_mae: ?f64 };
/// One prediction per record, in record order.
fn predictions(a: std.mem.Allocator, cache: *Cache, trainer: *training.controller.Trainer, dataset: data.Dataset) ![]Prediction {
    const rows = try a.alloc([]const f32, dataset.examples.len);
    const widths = try a.alloc(usize, dataset.examples.len);
    // Evaluation uses one example at a time, independent of training padding.
    for (dataset.examples, rows, widths) |e, *row, *width| {
        const program = try cache.get(&.{e});
        // Execution scratch must be reclaimable between examples. `a` is the
        // run-lifetime arena: retain only the small prediction in that arena.
        const logits = try training.predict(cache.allocator, program, trainer, cache.config, &.{e});
        defer cache.allocator.free(logits);
        row.* = try a.dupe(f32, logits);
        width.* = cache.last.options;
    }
    const result = try a.alloc(Prediction, dataset.records.len);
    for (dataset.placements, result) |place, *dst| {
        const q = dataset.examples[place.example].question(place.question);
        dst.* = .{ .kind = q.kind, .target = q.target, .logits = rows[place.example][place.question * widths[place.example] ..][0..q.target.len] };
    }
    return result;
}
fn metrics(preds: []const Prediction, temperatures: [3]f32) !Metrics {
    var ce: f64 = 0;
    var correct: usize = 0;
    var ordinals: usize = 0;
    var mae: f64 = 0;
    for (preds) |p| {
        var max: f64 = -std.math.inf(f64);
        var winner: usize = 0;
        var gold: usize = 0;
        for (p.logits, p.target, 0..) |z, t, k| {
            if (!std.math.isFinite(z)) return error.NonFiniteLayaLogits;
            max = @max(max, z / temperatures[@intFromEnum(p.kind)]);
            if (z > p.logits[winner]) winner = k;
            if (t > p.target[gold]) gold = k;
        }
        correct += @intFromBool(winner == gold);
        var sum: f64 = 0;
        var probs: [model.max_packed_options]f64 = undefined;
        for (p.logits, 0..) |z, k| {
            probs[k] = @exp(z / temperatures[@intFromEnum(p.kind)] - max);
            sum += probs[k];
        }
        var expected: f64 = 0;
        var target_expected: f64 = 0;
        for (p.logits, p.target, 0..) |z, t, k| {
            ce -= t * (z / temperatures[@intFromEnum(p.kind)] - max - @log(sum));
            expected += @as(f64, @floatFromInt(k)) * probs[k] / sum;
            target_expected += @as(f64, @floatFromInt(k)) * t;
        }
        if (p.kind == .score) {
            ordinals += 1;
            mae += @abs(expected - target_expected);
        }
    }
    const n: f64 = @floatFromInt(preds.len);
    return .{ .examples = preds.len, .soft_ce = ce / n, .accuracy = @as(f64, @floatFromInt(correct)) / n, .ordinal_mae = if (ordinals > 0) mae / @as(f64, @floatFromInt(ordinals)) else null };
}
fn metricsByKind(a: std.mem.Allocator, preds: []const Prediction, temperatures: [3]f32) ![3]?Metrics {
    var result: [3]?Metrics = .{ null, null, null };
    for (0..3) |kind| {
        var subset: std.ArrayListUnmanaged(Prediction) = .empty;
        defer subset.deinit(a);
        for (preds) |p| if (@intFromEnum(p.kind) == kind) try subset.append(a, p);
        if (subset.items.len > 0) result[kind] = try metrics(subset.items, temperatures);
    }
    return result;
}
fn calibrate(a: std.mem.Allocator, preds: []const Prediction) ![3]f32 {
    var temperatures = [_]f32{ 1, 1, 1 };
    for (0..3) |kind| {
        var subset: std.ArrayListUnmanaged(Prediction) = .empty;
        defer subset.deinit(a);
        for (preds) |p| if (@intFromEnum(p.kind) == kind) try subset.append(a, p);
        if (subset.items.len < 10) continue;
        var best = (try metrics(subset.items, temperatures)).soft_ce;
        // Bounded deterministic log-temperature search. Unit temperature is
        // always a candidate, so calibration never raises calibration-set CE.
        for (0..201) |i| {
            var candidate = temperatures;
            candidate[kind] = @floatCast(@exp(@log(@as(f64, 0.1)) + @as(f64, @floatFromInt(i)) / 200 * @log(@as(f64, 100))));
            const loss = (try metrics(subset.items, candidate)).soft_ce;
            if (loss < best) {
                best = loss;
                temperatures[kind] = candidate[kind];
            }
        }
    }
    return temperatures;
}

fn exportModel(a: std.mem.Allocator, io: std.Io, c: Config, config_json: std.json.Value, layout: model.Packing, assets: Assets, admitted: []const checkpoint.NamedTensor, trainer: *training.controller.Trainer, temperatures: [3]f32) !void {
    try trainer.ensureHostState(null);
    const stage = try path(a, c.output_dir, "model.partial");
    try std.Io.Dir.cwd().createDir(io, stage, .default_dir);
    var entries: std.ArrayListUnmanaged(checkpoint.NamedTensor) = .empty;
    for (admitted) |entry| {
        const values: []const f32 = blk: {
            if (std.mem.eql(u8, entry.name, "temperature")) break :blk &temperatures;
            for (trainer.owner.regular_params.items) |p| if (std.mem.eql(u8, entry.name, p.name)) break :blk p.weights;
            break :blk entry.data;
        };
        try entries.append(a, .{ .name = entry.name, .shape = entry.shape, .data = values });
    }
    try checkpoint.saveControlled(a, try path(a, stage, "model.safetensors"), entries.items, null, true);
    var cfg = config_json;
    var decision = cfg.object.getPtr("laya") orelse return error.InvalidLayaConfig;
    var temp: std.json.Array = .init(a);
    for (temperatures) |t| try temp.append(.{ .float = t });
    try decision.object.put(a, "temperature", .{ .array = temp });
    // A packing override changes the serving layout the weights were trained for.
    if (c.packing != null) {
        _ = decision.object.swapRemove("packing");
        if (layout.enabled()) {
            var object: std.json.ObjectMap = .empty;
            try object.put(a, "mode", .{ .string = @tagName(layout.mode) });
            try object.put(a, "max_packed_len", .{ .integer = @intCast(layout.max_packed_len) });
            try decision.object.put(a, "packing", .{ .object = object });
        }
    }
    // The old option buckets override per-type temperatures in inference.
    // They must never survive a change to the trained weights.
    _ = decision.object.swapRemove("temperature_by_options");
    try writeJson(io, try path(a, stage, "config.json"), cfg);
    try writeJson(io, try path(a, stage, "rl_agent_config.json"), decision.*);
    var stage_dir = try std.Io.Dir.cwd().openDir(io, stage, .{});
    defer stage_dir.close(io);
    try assets.write(io, stage_dir);
    try writeJson(io, try path(a, stage, "model_manifest.json"), .{ .type = "classifier", .tasks = [_][]const u8{"extract"}, .capabilities = [_][]const u8{ "classification", "typed_decisions" }, .inputs = [_][]const u8{"text"}, .source = .{ .repository = c.model_dir, .revision = "antfly-laya-finetune-v1" } });
    const publication = @import("../../gliner_boundary_export.zig");
    try publication.syncDirectory(io, stage);
    try publication.publishDirectory(a, io, stage, try path(a, c.output_dir, "model"));
    try publication.syncDirectory(io, c.output_dir);
}

// A shuffled batch can combine the longest sequence with any other record.
// Compute a conservative worst-case layout for the entire split.
fn combinedLayout(cfg: modern.Config, examples: []const training.Example, batch_size: u32) !architecture.Layout {
    var layout = architecture.Layout{ .batch = @intCast(@min(batch_size, examples.len)), .sequence = 0, .options = 0, .questions = 0 };
    var questions: u32 = 0;
    for (examples) |e| {
        const single = try training.bucketedLayout(&.{e}, cfg);
        layout.sequence = @max(layout.sequence, single.sequence);
        layout.options = @max(layout.options, single.options);
        questions = @max(questions, single.questions);
        for (e.ids) |id| if (id < 0 or id >= cfg.vocab_size) return error.InvalidLayaTrainingToken;
    }
    layout.questions = @min(questions * layout.batch, 512);
    return layout;
}

// Admit a conservative bound for the entire split before backend allocation.
fn admitExamples(cfg: modern.Config, examples: []const training.Example, batch_size: u32, dropout: f32) !void {
    const layout = try combinedLayout(cfg, examples, batch_size);
    try architecture.validate(cfg, layout, dropout);
}

fn mulBytes(a: usize, b: usize) !usize {
    return std.math.mul(usize, a, b) catch error.LayaBackendMemoryLimitExceeded;
}
fn addBytes(a: usize, b: usize) !usize {
    return std.math.add(usize, a, b) catch error.LayaBackendMemoryLimitExceeded;
}

/// Trainable + frozen device weight bytes, f32, at the transaction's peak.
/// The resident AdamW transaction (`seeded_device_transaction.prepare`) keeps
/// weight, m, v, and the gradient accumulator resident (4x trainable bytes),
/// and on every optimizer step additionally snapshots weight/m/v into new
/// buffers before freeing the old ones
/// (`finetune/seeded_gradient_trainer.zig:updateResident`), so the transient
/// peak is 7x trainable bytes. Frozen tensors are uploaded once and carry no
/// optimizer state.
fn weightStateBytes(trainable_elements: usize, frozen_elements: usize) !usize {
    const weight_bytes = try mulBytes(trainable_elements, 4);
    const peak_trainable = try mulBytes(weight_bytes, 7);
    return addBytes(peak_trainable, try mulBytes(frozen_elements, 4));
}

/// Rough upper bound on retained forward/backward activations for one packed
/// row at `layout`. The training graph materializes dense (non-segment)
/// attention and keeps no gradient checkpoint, so every layer's forward
/// tensors are live until its backward pass runs. Per encoder layer this
/// counts about twelve hidden-width buffers (QKV, attention output, two
/// layer norms, residual, and their cotangents), two intermediate-width GeGLU
/// buffers, and eight dense `[batch, heads, seq, seq]` tensors (three
/// tree/padding biases, forward scores and softmax probabilities, and their
/// backward cotangents); each is individually bounded by `graph.validate`'s
/// 64M-element admission, but their sum across layers is not, which is what
/// this estimate is for. Frozen layers still execute forward (`freeze_layers` in
/// LAYA.md); only unfrozen layers additionally retain backward state. This is
/// an admission bound, not a measured footprint: it excludes the decision
/// head, RoPE tables, and dropout masks, which the fixed overhead below folds
/// in from measurement instead of modeling directly.
fn activationBytes(cfg: modern.Config, l: architecture.Layout, freeze_layers: u32) !usize {
    const batch: usize = @intCast(l.batch);
    const sequence: usize = @intCast(l.sequence);
    const hidden: usize = @intCast(cfg.hidden_size);
    const intermediate: usize = @intCast(cfg.intermediate_size);
    const heads: usize = @max(@as(usize, @intCast(cfg.num_attention_heads)), hidden / 64);
    const tokens = try mulBytes(batch, sequence);
    const hidden_bytes = try mulBytes(try mulBytes(tokens, hidden), 12 * 4);
    const mlp_bytes = try mulBytes(try mulBytes(tokens, intermediate), 2 * 4);
    const attn_elements = try mulBytes(try mulBytes(batch, try mulBytes(sequence, sequence)), heads);
    // Eight buffers of this size: three dense biases (encoder_bias, local_bias,
    // head_bias in `architecture.Inputs`), forward scores and softmax
    // probabilities, and their backward cotangents.
    const attn_bytes = try mulBytes(attn_elements, 8 * 4);
    const per_layer = try addBytes(try addBytes(hidden_bytes, mlp_bytes), attn_bytes);
    const forward_layers: usize = @intCast(cfg.num_hidden_layers);
    const backward_layers: usize = @intCast(cfg.num_hidden_layers - @min(freeze_layers, cfg.num_hidden_layers));
    return mulBytes(per_layer, try addBytes(forward_layers, backward_layers));
}

/// Fixed device overhead observed on the Apple M4 Max release trainer beyond
/// weights/optimizer state and modeled activations: MPS kernel/temporary
/// caches, the in-frame buffer reuse pool, RoPE/bias/dropout runtime inputs
/// (uploaded once per step), and Metal driver bookkeeping. See LAYA.md,
/// "Training memory", for the measurement this constant is calibrated
/// against.
const fixed_backend_overhead_bytes: usize = 4 * 1024 * 1024 * 1024;

/// Upper-bound estimate of `backend: metal` device memory for one run: the
/// admission gate in `execute` compares this against `max_backend_bytes`
/// before creating the output directory or allocating any device weights.
fn estimateBackendBytes(cfg: modern.Config, selected: []const training.controller.Parameter, frozen_elements: usize, layouts: []const architecture.Layout, freeze_layers: u32) !usize {
    var trainable_elements: usize = 0;
    for (selected) |p| trainable_elements = try addBytes(trainable_elements, p.values.len);
    var activation: usize = 0;
    for (layouts) |l| activation = @max(activation, try activationBytes(cfg, l, freeze_layers));
    const state = try weightStateBytes(trainable_elements, frozen_elements);
    return addBytes(try addBytes(state, activation), fixed_backend_overhead_bytes);
}

/// The Metal runtime's process-wide peak of live device-owned buffer bytes,
/// reset at admission time so it reflects this run alone. Zero on `cpu` or
/// when the binary has no Metal backend.
fn backendPeakBytes(c: Config) u64 {
    if (comptime @import("build_options").enable_metal) {
        if (c.backend == .metal) return metal_tensor.memoryStatsSnapshot().device_owned_peak_live_bytes;
    }
    return 0;
}

const Cursor = struct {
    batches: u64,
    epochs: u64,
    accumulation: u64,
    stop: ?u64 = null,
    fn validate(raw: ?*const anyopaque, identity: training.controller.Identity, accumulated: u32) !void {
        const self: *const Cursor = @ptrCast(@alignCast(raw.?));
        const completed = identity.microbatch_step;
        if (completed > self.batches * self.epochs) return error.InvalidLayaResumePosition;
        const epochs = completed / self.batches;
        const within = completed % self.batches;
        const per_epoch = std.math.divCeil(u64, self.batches, self.accumulation) catch unreachable;
        if (identity.optimizer_step != epochs * per_epoch + within / self.accumulation or accumulated != within % self.accumulation)
            return error.InvalidLayaResumePosition;
        if (self.stop) |stop| if (stop <= completed or stop > self.batches * self.epochs) return error.InvalidLayaStopPosition;
    }
};

fn validateEncoderMetadata(value: std.json.Value) !void {
    if (value != .object) return error.InvalidLayaConfig;
    // Inference's permissive defaults must not turn malformed training
    // metadata into a different model or normalization recipe.
    for ([_][]const u8{ "vocab_size", "hidden_size", "num_hidden_layers", "num_attention_heads", "intermediate_size", "max_position_embeddings", "global_attn_every_n_layers", "local_attention" }) |key| if (value.object.get(key)) |v| {
        if (v != .integer or v.integer < 0 or v.integer > std.math.maxInt(u32)) return error.InvalidLayaConfig;
    };
    for ([_][]const u8{ "global_rope_theta", "local_rope_theta", "layer_norm_eps" }) |key| if (value.object.get(key)) |v| {
        const number: f64 = switch (v) {
            .integer => @floatFromInt(v.integer),
            .float => v.float,
            else => return error.InvalidLayaConfig,
        };
        if (!std.math.isFinite(number) or number <= 0 or number > std.math.floatMax(f32)) return error.InvalidLayaConfig;
    };
}

/// Own export metadata and frozen values in the caller's run-lifetime arena.
/// Trainable values already have an owned copy and are filled at publication.
fn exportInputs(a: std.mem.Allocator, reader: *const safetensors.MMapReader, selected: []const training.controller.Parameter) ![]checkpoint.NamedTensor {
    var result: std.ArrayListUnmanaged(checkpoint.NamedTensor) = .empty;
    var tensors = reader.header.tensors.iterator();
    while (tensors.next()) |entry| {
        const shape = try a.alloc(usize, entry.value_ptr.shape.len);
        for (shape, entry.value_ptr.shape) |*dst, dim| dst.* = @intCast(dim);
        const values: []const f32 = blk: {
            for (selected) |p| if (std.mem.eql(u8, p.name, entry.key_ptr.*)) break :blk &.{};
            var tensor = try reader.readTensor(entry.key_ptr.*);
            defer tensor.deinit();
            const frozen = try training.floatValues(a, tensor);
            for (frozen) |value| if (!std.math.isFinite(value)) return error.NonFiniteLayaFrozenWeight;
            break :blk frozen;
        };
        try result.append(a, .{ .name = try a.dupe(u8, entry.key_ptr.*), .shape = shape, .data = values });
    }
    return result.toOwnedSlice(a);
}

/// All source/data admission happens before creating a new run directory.
/// Caller supplies the process-wide admission owner; standalone CLI creates
/// exactly one. Library/server integrations share their existing controller.
pub fn execute(gpa: std.mem.Allocator, io: std.Io, c: Config, admission: *memory.AdmissionController) !void {
    try validate(c);
    var budget = Budget{ .backing = gpa, .limit = c.max_host_bytes };
    const a = budget.allocator();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const permanent = arena.allocator();
    var model_dir = try std.Io.Dir.cwd().openDir(io, c.model_dir, .{});
    defer model_dir.close(io);
    const config_bytes = try snapshot.read(permanent, io, model_dir, "config.json", 1024 * 1024, null);
    const config_json = try std.json.parseFromSlice(std.json.Value, permanent, config_bytes, .{ .allocate = .alloc_always });
    try validateEncoderMetadata(config_json.value);
    var encoder = try modern.parseConfig(a, config_bytes);
    const source_laya = encoder.laya orelse return error.InvalidLayaConfig;
    encoder.laya.?.packing = try packing(c, source_laya);
    const laya = encoder.laya.?;
    if (c.freeze_layers > encoder.num_hidden_layers) return error.InvalidLayaJob;
    for ([_][]const u8{ "attention_bias", "mlp_bias", "norm_bias" }) |key| if (config_json.value.object.get(key)) |v| {
        if (v != .bool or v.bool) return error.UnsupportedLayaEncoderBias;
    };
    if (config_json.value.object.get("hidden_activation")) |v| if (v != .string or !std.mem.eql(u8, v.string, "gelu")) return error.UnsupportedLayaActivation;
    // The native encoder training graph currently admits zero encoder dropout.
    for ([_][]const u8{ "attention_dropout", "embedding_dropout", "mlp_dropout" }) |key| if (config_json.value.object.get(key)) |v| {
        if ((v == .float and v.float != 0) or (v == .integer and v.integer != 0) or (v != .float and v != .integer)) return error.UnsupportedLayaEncoderDropout;
    };
    const assets = try Assets.read(permanent, io, model_dir);
    const tokenizer_bytes = assets.bytes[0].?;
    const tokenizer = try hf.HfTokenizer.loadFromBytes(a, tokenizer_bytes);
    const tok = tokenizer.tokenizer();
    defer tok.deinitTokenizer();
    const train = try data.load(permanent, c.train_file, tok, laya);
    const eval = try data.load(permanent, c.eval_file, tok, laya);
    try data.disjoint(a, train, eval);
    const calibration = if (c.calibration_file) |file| try data.load(permanent, file, tok, laya) else null;
    if (calibration) |calib| {
        try data.disjoint(a, train, calib);
        try data.disjoint(a, eval, calib);
    }
    const train_layout = try combinedLayout(encoder, train.examples, c.batch_size);
    try architecture.validate(encoder, train_layout, c.head_dropout);
    const eval_layout = try combinedLayout(encoder, eval.examples, 1);
    try architecture.validate(encoder, eval_layout, c.head_dropout);
    const calib_layout = if (calibration) |calib| try combinedLayout(encoder, calib.examples, 1) else null;
    if (calib_layout) |cl| try architecture.validate(encoder, cl, c.head_dropout);
    var cache = Cache{ .allocator = a, .config = encoder, .dropout = c.head_dropout, .freeze_layers = c.freeze_layers };
    defer cache.deinit();
    // Release the raw source snapshot before optimizer initialization/restore.
    // Only owned trainable values, frozen values, and export metadata survive.
    const admitted = blk: {
        const source_bytes = try snapshot.read(a, io, model_dir, "model.safetensors", 8 * 1024 * 1024 * 1024, null);
        defer a.free(source_bytes);
        var source = try safetensors.MMapReader.fromBorrowedBytesLimited(a, source_bytes, 16 * 1024 * 1024);
        defer source.deinit();
        try @import("../../models/laya.zig").validateReader(&source, laya, encoder);
        if (source.header.tensors.get("temperature")) |meta| if (!std.mem.eql(i64, meta.shape, &.{3})) return error.InvalidLayaWeights;
        const initial = try cache.get(train.examples[0..@min(train.examples.len, c.batch_size)]);
        const selected = try training.parameters(permanent, &initial.graph, &source, c.freeze_layers);
        const export_tensors = try exportInputs(permanent, &source, selected);
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly-laya-training/v1");
        hash.update(config_bytes);
        hash.update(tokenizer_bytes);
        hash.update(source.file_bytes);
        hash.update(&train.sha256);
        hash.update(&eval.sha256);
        if (calibration) |calib| hash.update(&calib.sha256);
        var identity_config = c;
        identity_config.output_dir = "";
        identity_config.resume_from = null;
        identity_config.stop_after_microbatches = null;
        var json = std.Io.Writer.Allocating.init(a);
        defer json.deinit();
        try std.json.Stringify.value(identity_config, .{}, &json.writer);
        hash.update(json.written());
        break :blk .{ .selected = selected, .export_tensors = export_tensors, .identity = hash.finalResult(), .weights_sha256 = data.digest(source_bytes) };
    };
    const selected = admitted.selected;
    // Device memory admission, before any device weight or optimizer-state
    // allocation. `frozen_elements` covers tensors below `freeze_layers`,
    // which are uploaded once but never enter the optimizer.
    var frozen_elements: usize = 0;
    for (admitted.export_tensors) |t| {
        if (training.frozen(t.name, c.freeze_layers)) frozen_elements = try addBytes(frozen_elements, t.data.len);
    }
    var layouts: [3]architecture.Layout = undefined;
    var layout_count: usize = 2;
    layouts[0] = train_layout;
    layouts[1] = eval_layout;
    if (calib_layout) |cl| {
        layouts[2] = cl;
        layout_count = 3;
    }
    const backend_estimate: usize = if (c.backend == .metal)
        try estimateBackendBytes(encoder, selected, frozen_elements, layouts[0..layout_count], c.freeze_layers)
    else
        0;
    if (c.backend == .metal and backend_estimate > c.max_backend_bytes) return error.LayaBackendMemoryLimitExceeded;
    if (comptime @import("build_options").enable_metal) if (c.backend == .metal) metal_tensor.resetMemoryStats();
    // `max_host_bytes` is already a generous safety-net ceiling enforced by
    // `budget` (BoundedAllocator), not actual usage, so it is not also
    // charged to the live-memory sample below: doing so would make a single
    // default job's own declared envelope exceed most machines. The device
    // estimate is the actual new risk (previously unbounded, unlike host
    // memory), so it alone is charged, and only for `backend: metal`, where
    // Metal buffers draw from the same physical pool `tryAcquire`'s live
    // check samples (`memory.zig`'s `liveHostBytes`, unified on macOS).
    var lease = try admission.tryAcquire(
        if (c.backend == .metal) .gpu else .cpu,
        .{
            .host_limit_bytes = c.max_host_bytes,
            .backend_limit_bytes = if (c.backend == .metal) c.max_backend_bytes else 0,
            .combined_limit_bytes = try addBytes(c.max_host_bytes, if (c.backend == .metal) c.max_backend_bytes else 0),
        },
        .{ .backend_scratch_bytes = backend_estimate },
        c.backend == .metal,
    );
    defer lease.release();
    const originals = try permanent.alloc(run.Parameter, selected.len);
    for (selected, originals) |p, *o| o.* = .{ .name = p.name, .canonical_name = p.name, .dimensions = p.dimensions, .values = p.values, .kind = .original };
    var store = native.WeightStore{ .allocator = a, .resident_weights = .{}, .lazy_weights = .{} };
    const execution: training.controller.Execution = if (c.backend == .metal) .resident_metal else .native;
    const owner = try backend.Owner.init(a, &store, originals, selected, execution, .{}, null);
    defer owner.deinit();
    var cpu_vtable: @import("../../ops/ops.zig").ComputeBackend.VTable = undefined;
    @import("cpu.zig").install(&owner.cb, &cpu_vtable);
    // Frozen values come from the source snapshot already kept for export,
    // uploaded once in the trainer's storage (resident on Metal).
    var frozen: std.ArrayListUnmanaged(training.Frozen) = .empty;
    defer {
        for (frozen.items) |f| owner.cb.free(f.value);
        frozen.deinit(a);
    }
    for (admitted.export_tensors) |t| if (training.frozen(t.name, c.freeze_layers)) {
        var dims: [8]i32 = undefined;
        if (t.shape.len > dims.len or t.data.len == 0) return error.InvalidLayaWeights;
        for (t.shape, dims[0..t.shape.len]) |dim, *dst| dst.* = std.math.cast(i32, dim) orelse return error.InvalidLayaWeights;
        const value = if (execution == .native)
            try owner.cb.fromFloat32Shape(t.data, dims[0..t.shape.len])
        else
            try owner.cb.residentTrainingPrimitive(&.{ .upload_f32 = .{ .values = t.data, .shape = dims[0..t.shape.len] } }, .{});
        errdefer owner.cb.free(value);
        try frozen.append(a, .{ .name = t.name, .value = value });
    };
    cache.frozen = frozen.items;
    const batches = std.math.divCeil(usize, train.examples.len, c.batch_size) catch unreachable;
    const updates = std.math.divCeil(usize, batches, c.gradient_accumulation) catch unreachable;
    const steps = std.math.cast(u32, updates * c.epochs) orelse return error.InvalidLayaJob;
    const trainer_config = training.controller.Config{
        .execution = execution,
        .groups = &.{
            .{ .optimizer = .{ .weight_decay = c.weight_decay }, .schedule = .{ .cosine = .{ .initial_lr = c.encoder_lr, .min_lr = @min(c.encoder_lr, 1e-6), .total_steps = steps } } },
            .{ .optimizer = .{ .weight_decay = c.weight_decay }, .schedule = .{ .cosine = .{ .initial_lr = c.head_lr, .min_lr = @min(c.head_lr, 1e-6), .total_steps = steps } } },
        },
        .grad_accum_steps = c.gradient_accumulation,
        .max_grad_norm = c.max_grad_norm,
        .limits = .{ .max_state_bytes = c.max_host_bytes, .max_transaction_bytes = c.max_host_bytes },
    };
    const identity = admitted.identity;
    const cursor = Cursor{ .batches = batches, .epochs = c.epochs, .accumulation = c.gradient_accumulation, .stop = c.stop_after_microbatches };
    var trainer = if (c.resume_from) |resume_path|
        try training.controller.Trainer.initRestoredValidated(a, &owner.cb, selected, trainer_config, resume_path, identity, null, .{ .context = &cursor, .validate = Cursor.validate })
    else
        try training.controller.Trainer.init(a, &owner.cb, selected, trainer_config);
    defer trainer.deinit();
    const completed = trainer.identity().microbatch_step;
    try Cursor.validate(&cursor, trainer.identity(), trainer.owner.accum_count);
    try std.Io.Dir.cwd().createDir(io, c.output_dir, .default_dir);
    const latest = try path(permanent, c.output_dir, "latest.safetensors");
    try writeJson(io, try path(permanent, c.output_dir, "job.json"), c);
    const log = try std.Io.Dir.cwd().createFile(io, try path(permanent, c.output_dir, "metrics.jsonl"), .{ .exclusive = true });
    defer log.close(io);
    const before_predictions = try predictions(permanent, &cache, &trainer, eval);
    for (before_predictions, eval.records) |*p, r| p.id = r.id;
    const before = try metrics(before_predictions, .{ 1, 1, 1 });
    const initial_by_kind = try metricsByKind(a, before_predictions, .{ 1, 1, 1 });
    try writeJson(io, try path(permanent, c.output_dir, "initial_eval_predictions.json"), before_predictions);
    try event(io, log, .{ .event = "initial_eval", .metrics = before });
    try event(io, std.Io.File.stdout(), .{ .event = "initial_eval", .metrics = before });
    const order = try permanent.alloc(usize, train.examples.len);
    const batch = try permanent.alloc(training.Example, c.batch_size);
    // Record ids per example; a packed example holds several records.
    const example_records = try permanent.alloc(std.ArrayListUnmanaged([]const u8), train.examples.len);
    @memset(example_records, .empty);
    for (train.placements, train.records) |place, record| try example_records[place.example].append(permanent, record.id);
    var batch_ids: std.ArrayListUnmanaged([]const u8) = .empty;
    for (0..c.epochs) |epoch| {
        if ((epoch + 1) * batches <= completed) continue;
        for (order, 0..) |*index, i| index.* = i;
        var random = std.Random.DefaultPrng.init(c.seed +% epoch);
        random.random().shuffle(usize, order);
        for (0..batches) |batch_index| {
            const microbatch = epoch * batches + batch_index;
            if (microbatch < completed) continue;
            const start = batch_index * c.batch_size;
            const count = @min(c.batch_size, order.len - start);
            batch_ids.clearRetainingCapacity();
            for (batch[0..count], order[start..][0..count]) |*e, index| {
                e.* = train.examples[index];
                try batch_ids.appendSlice(permanent, example_records[index].items);
            }
            const program = try cache.get(batch[0..count]);
            const progress = @as(f32, @floatFromInt(epoch)) / @as(f32, @floatFromInt(@max(1, c.epochs - 1)));
            const began = platform.time.monotonicNs();
            const report = try training.step(a, program, &trainer, encoder, batch[0..count], .{ .group_size = c.group_size, .sigma = c.sigma_start + (c.sigma_end - c.sigma_start) * progress, .rl_weight = if (c.objective == .rlcd) 1 else 0 }, c.seed +% (microbatch *% 0x9e3779b97f4a7c15));
            // Wall time of this microbatch, including any graph build for a new shape.
            const step_ms = @as(f64, @floatFromInt(platform.time.monotonicNs() - began)) / 1e6;
            try event(io, log, .{ .event = "step", .epoch = epoch + 1, .batch = batch_index + 1, .record_ids = batch_ids.items, .step_ms = step_ms, .report = report });
            try event(io, std.Io.File.stdout(), .{ .event = "step", .epoch = epoch + 1, .batch = batch_index + 1, .step_ms = step_ms, .report = report });
            if (batch_index + 1 == batches) _ = try trainer.flush(trainer.identity(), null);
            if (trainer.identity().microbatch_step % c.checkpoint_every_steps == 0 or batch_index + 1 == batches) try trainer.save(latest, identity, null);
            if (c.stop_after_microbatches) |stop| if (trainer.identity().microbatch_step >= stop) {
                try trainer.save(latest, identity, null);
                const paused = .{ .format = "antfly-laya-finetune/v1", .status = "paused", .optimizer = trainer.identity(), .run_sha256 = std.fmt.bytesToHex(identity, .lower) };
                try writeJson(io, try path(permanent, c.output_dir, "report.json"), paused);
                try event(io, std.Io.File.stdout(), paused);
                return;
            };
        }
    }
    try trainer.save(latest, identity, null);
    const temperatures = if (calibration) |calib| try calibrate(a, try predictions(permanent, &cache, &trainer, calib)) else [3]f32{ 1, 1, 1 };
    const final_predictions = try predictions(permanent, &cache, &trainer, eval);
    for (final_predictions, eval.records) |*p, r| p.id = r.id;
    const after = try metrics(final_predictions, temperatures);
    try writeJson(io, try path(permanent, c.output_dir, "eval_predictions.json"), final_predictions);
    try exportModel(permanent, io, c, config_json.value, laya.packing, assets, admitted.export_tensors, &trainer, temperatures);
    const result = .{ .format = "antfly-laya-finetune/v1", .status = "complete", .backend = c.backend, .objective = c.objective, .source_sha256 = .{ .config = std.fmt.bytesToHex(data.digest(config_bytes), .lower), .tokenizer = std.fmt.bytesToHex(data.digest(tokenizer_bytes), .lower), .weights = std.fmt.bytesToHex(admitted.weights_sha256, .lower) }, .calibration_sha256 = if (calibration) |calib| std.fmt.bytesToHex(calib.sha256, .lower) else null, .resumed_microbatches = completed, .train_examples = train.examples.len, .train_records = train.records.len, .packing = laya.packing.mode, .train_sha256 = std.fmt.bytesToHex(train.sha256, .lower), .eval_sha256 = std.fmt.bytesToHex(eval.sha256, .lower), .run_sha256 = std.fmt.bytesToHex(identity, .lower), .optimizer = trainer.identity(), .initial_eval = before, .initial_by_kind = initial_by_kind, .final_eval = after, .final_by_kind = try metricsByKind(a, final_predictions, temperatures), .final_uncalibrated_eval = try metrics(final_predictions, .{ 1, 1, 1 }), .temperature = temperatures, .calibration_examples = if (calibration) |calib| calib.examples.len else 0, .action_head_trained = false, .host_peak_bytes = budget.peak, .backend_estimated_bytes = backend_estimate, .backend_peak_bytes = backendPeakBytes(c) };
    try writeJson(io, try path(permanent, c.output_dir, "report.json"), result);
    try event(io, std.Io.File.stdout(), result);
}

test "laya training job rejects invalid paths and optimizer settings" {
    var c = Config{ .model_dir = "/model", .train_file = "/train", .eval_file = "/eval", .output_dir = "/output" };
    try validate(c);
    c.encoder_lr = std.math.nan(f32);
    try std.testing.expectError(error.InvalidLayaJob, validate(c));
}

test "laya training calibration fits only sufficiently represented types" {
    const a = std.testing.allocator;
    var preds: [11]Prediction = undefined;
    for (preds[0..10]) |*p| p.* = .{ .kind = .choice, .logits = &.{ 5, -5 }, .target = &.{ 0.5, 0.5 } };
    preds[10] = .{ .kind = .noul, .logits = &.{ 5, -5 }, .target = &.{ 0.5, 0.5 } };
    const temperatures = try calibrate(a, &preds);
    try std.testing.expect(temperatures[0] > 1);
    try std.testing.expectEqual(@as(f32, 1), temperatures[1]);
    try std.testing.expectEqual(@as(f32, 1), temperatures[2]);
    try std.testing.expect((try metrics(&preds, temperatures)).soft_ce < (try metrics(&preds, .{ 1, 1, 1 })).soft_ce);
}

test "laya resume cursor accounts for partial windows and epoch flushes" {
    const cursor = Cursor{ .batches = 5, .epochs = 2, .accumulation = 3 };
    for ([_]u64{ 0, 0, 0, 1, 1, 2, 2, 2, 3, 3, 4 }, 0..) |updates, micro| {
        try Cursor.validate(&cursor, .{ .optimizer_step = updates, .microbatch_step = micro }, @intCast((micro % 5) % 3));
    }
    try std.testing.expectError(error.InvalidLayaResumePosition, Cursor.validate(&cursor, .{ .optimizer_step = 1, .microbatch_step = 5 }, 0));
    try std.testing.expectError(error.InvalidLayaResumePosition, Cursor.validate(&cursor, .{ .optimizer_step = 2, .microbatch_step = 5 }, 2));
    try std.testing.expectError(error.InvalidLayaResumePosition, Cursor.validate(&cursor, .{ .optimizer_step = 4, .microbatch_step = 11 }, 0));
    var stopped = cursor;
    stopped.stop = 5;
    try std.testing.expectError(error.InvalidLayaStopPosition, Cursor.validate(&stopped, .{ .optimizer_step = 2, .microbatch_step = 5 }, 0));
    stopped.stop = 11;
    try std.testing.expectError(error.InvalidLayaStopPosition, Cursor.validate(&stopped, .{ .optimizer_step = 0, .microbatch_step = 0 }, 0));
}

test "laya admission checks late long sequences and token vocabulary before training" {
    const cfg = modern.Config{ .laya = .{ .max_len = 2048 }, .checkpoint_layout = .huggingface_fused_qkv_no_bias, .rope_interleaved = false };
    const ids = [_]i64{0} ** 2048;
    const short = training.Example{ .ids = ids[0..2], .markers = &.{ 0, 1 }, .kind = .noul, .target = &.{ 1, 0 } };
    var long = short;
    long.ids = &ids;
    try admitExamples(cfg, &.{ short, short }, 2, 0);
    try admitExamples(cfg, &.{long}, 1, 0);
    try std.testing.expectError(error.LayaTrainingAttentionLimitExceeded, admitExamples(cfg, &.{ short, short, long }, 2, 0));
    var invalid = short;
    invalid.ids = &.{ 0, cfg.vocab_size };
    try std.testing.expectError(error.InvalidLayaTrainingToken, admitExamples(cfg, &.{ short, invalid }, 1, 0));
}

test "laya job config rejects out-of-range backend memory ceilings" {
    var c = Config{ .model_dir = "/model", .train_file = "/train", .eval_file = "/eval", .output_dir = "/output" };
    try validate(c);
    c.max_backend_bytes = 1024;
    try std.testing.expectError(error.InvalidLayaJob, validate(c));
    c.max_backend_bytes = 128 * 1024 * 1024 * 1024;
    try std.testing.expectError(error.InvalidLayaJob, validate(c));
}

test "laya weight state bytes charge the resident transaction's transient snapshot" {
    // 1000 f32 trainable elements: weight+m+v+grad_accum resident (4x) plus a
    // transient weight/m/v snapshot the transaction takes before the old
    // buffers are freed (+3x). Frozen elements carry no optimizer state.
    try std.testing.expectEqual(@as(usize, 1000 * 4 * 7), try weightStateBytes(1000, 0));
    try std.testing.expectEqual(@as(usize, 1000 * 4 * 7 + 500 * 4), try weightStateBytes(1000, 500));
}

test "laya activation estimate grows with layout and shrinks with frozen layers" {
    const cfg = modern.Config{};
    const small = architecture.Layout{ .batch = 1, .sequence = 64, .options = 2, .questions = 1 };
    const large = architecture.Layout{ .batch = 1, .sequence = 512, .options = 2, .questions = 1 };
    const small_bytes = try activationBytes(cfg, small, 0);
    const large_bytes = try activationBytes(cfg, large, 0);
    try std.testing.expect(large_bytes > small_bytes);
    // Freezing every layer still charges one forward pass, never zero.
    const frozen_bytes = try activationBytes(cfg, large, cfg.num_hidden_layers);
    try std.testing.expect(frozen_bytes > 0 and frozen_bytes < large_bytes);
}

test "laya backend estimate covers weights, activations, and the fixed overhead floor" {
    const cfg = modern.Config{};
    var values: [1024]f32 = undefined;
    const params = [_]training.controller.Parameter{.{ .name = "w", .values = &values, .dimensions = &.{1024}, .group = 0 }};
    const layout = architecture.Layout{ .batch = 1, .sequence = 64, .options = 2, .questions = 1 };
    const estimate = try estimateBackendBytes(cfg, &params, 0, &.{layout}, 0);
    try std.testing.expect(estimate > fixed_backend_overhead_bytes);
    // A larger admitted layout never lowers the estimate.
    const bigger_layout = architecture.Layout{ .batch = 1, .sequence = 512, .options = 2, .questions = 1 };
    const bigger = try estimateBackendBytes(cfg, &params, 0, &.{bigger_layout}, 0);
    try std.testing.expect(bigger > estimate);
}

test "laya training rejects malformed encoder metadata instead of defaulting" {
    for ([_][]const u8{ "{\"hidden_size\":\"768\"}", "{\"num_attention_heads\":-1}", "{\"layer_norm_eps\":\"0.01\"}", "{\"global_rope_theta\":1e999}" }) |json| {
        const parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, json, .{});
        defer parsed.deinit();
        try std.testing.expectError(error.InvalidLayaConfig, validateEncoderMetadata(parsed.value));
    }
}

test "laya admission snapshots frozen export tensors and rejects nonfinite values" {
    const a = std.testing.allocator;
    var temp = std.testing.tmpDir(.{});
    defer temp.cleanup();
    const directory = try temp.dir.realPathFileAlloc(std.testing.io, ".", a);
    defer a.free(directory);
    const file = try path(a, directory, "frozen.safetensors");
    defer a.free(file);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    try checkpoint.saveControlled(a, file, &.{.{ .name = "act_head.0.weight", .shape = &.{1}, .data = &.{7} }}, null, true);
    const admitted = blk: {
        var source = try safetensors.MMapReader.openFileAbsolute(a, file);
        defer source.deinit();
        break :blk try exportInputs(arena.allocator(), &source, &.{});
    };
    try temp.dir.deleteFile(std.testing.io, "frozen.safetensors");
    try checkpoint.saveControlled(a, file, &.{.{ .name = "act_head.0.weight", .shape = &.{1}, .data = &.{std.math.nan(f32)} }}, null, true);
    try std.testing.expectEqualStrings("act_head.0.weight", admitted[0].name);
    try std.testing.expectEqualSlices(usize, &.{1}, admitted[0].shape);
    try std.testing.expectEqualSlices(f32, &.{7}, admitted[0].data);
    var reader = try safetensors.MMapReader.openFileAbsolute(a, file);
    defer reader.deinit();
    try std.testing.expectError(error.NonFiniteLayaFrozenWeight, exportInputs(arena.allocator(), &reader, &.{}));
}
