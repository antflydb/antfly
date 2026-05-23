// Copyright 2026 Antfly, Inc.
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

const std = @import("std");
const ml = @import("ml");
const termite = @import("termite_internal");

const deberta_graph = termite.architectures.deberta_graph;
const native_compute_mod = termite.native_compute.native;
const NativeCompute = native_compute_mod.NativeCompute;
const WeightStore = native_compute_mod.WeightStore;
const real_autodiff = termite.finetune.real_autodiff_trainer;
const gliner2_autodiff = termite.finetune.gliner2_real_autodiff;
const gliner2_bundle = termite.finetune.gliner2;
const gliner2_data = termite.finetune.gliner2_data;
const weight_source_mod = termite.models.weight_source;
const safetensors = termite.models.safetensors;
const compat = termite.io.compat;
const SafetensorsSource = weight_source_mod.SafetensorsSource;
const LoadedWeight = weight_source_mod.LoadedWeight;
const Tensor = termite.backends.Tensor;

const graph_config = deberta_graph.Config{
    .vocab_size = 128011,
    .hidden_size = 768,
    .num_hidden_layers = 12,
    .num_attention_heads = 12,
    .intermediate_size = 3072,
    .max_position_embeddings = 512,
    .position_buckets = 256,
    .layer_norm_eps = 1e-7,
    .use_v3_names = true,
};

const Manifest = struct {
    num_classes: usize,
    hidden_size: usize,
    lora_rank: usize,
    lora_alpha: f64,
    lora_targets: []const []const u8,
    entity_labels: []const []const u8,
    objective: gliner2_autodiff.GlinerObjective = .token,

    fn deinit(self: *Manifest, allocator: std.mem.Allocator) void {
        for (self.lora_targets) |item| allocator.free(item);
        allocator.free(self.lora_targets);
        for (self.entity_labels) |item| allocator.free(item);
        allocator.free(self.entity_labels);
        self.* = undefined;
    }
};

const EvalOptions = struct {
    model_dir: []const u8,
    adapter_dir: []const u8,
    text: []const u8,
    entity_types_csv: ?[]const u8 = null,
    seq_len: usize = 64,
    max_span_width: usize = 4,
    objective_override: ?gliner2_autodiff.GlinerObjective = null,
    expect_text: ?[]const u8 = null,
    expect_label: ?[]const u8 = null,
    min_score: ?f32 = null,
};

const TopEntity = struct {
    text: []const u8,
    label: []const u8,
    start: usize,
    end: usize,
    score: f32,
};

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    var opts = parseArgs(init.minimal.args, allocator) catch |err| {
        if (err == error.HelpRequested) return;
        return err;
    };
    defer opts.deinit(allocator);

    var summary = try evalSavedAdapter(allocator, opts);
    defer summary.deinit(allocator);

    if (opts.value.expect_text) |expected| {
        if (!std.mem.eql(u8, summary.top.text, expected)) return error.SemanticGoldenTextMismatch;
    }
    if (opts.value.expect_label) |expected| {
        if (!std.mem.eql(u8, summary.top.label, expected)) return error.SemanticGoldenLabelMismatch;
    }
    if (opts.value.min_score) |min_score| {
        if (!std.math.isFinite(min_score) or min_score < 0) return error.InvalidScoreThreshold;
        if (summary.top.score < min_score) return error.SemanticGoldenScoreBelowThreshold;
    }

    const stdout = std.Io.File.stdout();
    var buf: [8192]u8 = undefined;
    var writer = stdout.writer(init.io, &buf);
    try std.json.Stringify.value(summary.jsonView(), .{ .whitespace = .indent_2 }, &writer.interface);
    try writer.interface.writeByte('\n');
    try writer.interface.flush();
}

const OwnedOptions = struct {
    value: EvalOptions,
    owned_entity_types_csv: ?[]const u8 = null,

    fn deinit(self: *OwnedOptions, allocator: std.mem.Allocator) void {
        if (self.owned_entity_types_csv) |value| allocator.free(value);
        self.* = undefined;
    }
};

fn parseArgs(args_in: std.process.Args, allocator: std.mem.Allocator) !OwnedOptions {
    var args = try std.process.Args.Iterator.initAllocator(args_in, allocator);
    defer args.deinit();
    _ = args.next();

    const model_dir = args.next() orelse return usageError();
    if (std.mem.eql(u8, model_dir, "--help") or std.mem.eql(u8, model_dir, "-h")) {
        printUsage();
        return error.HelpRequested;
    }
    const adapter_dir = args.next() orelse return usageError();
    const text = args.next() orelse return usageError();
    var opts = EvalOptions{
        .model_dir = model_dir,
        .adapter_dir = adapter_dir,
        .text = text,
    };
    var owned_entity_types_csv: ?[]const u8 = null;

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--entity-types")) {
            opts.entity_types_csv = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--seq-len")) {
            opts.seq_len = try std.fmt.parseUnsigned(usize, args.next() orelse return usageError(), 10);
        } else if (std.mem.eql(u8, arg, "--max-span-width")) {
            opts.max_span_width = try std.fmt.parseUnsigned(usize, args.next() orelse return usageError(), 10);
        } else if (std.mem.eql(u8, arg, "--objective")) {
            opts.objective_override = try parseObjective(args.next() orelse return usageError());
        } else if (std.mem.eql(u8, arg, "--expect-text")) {
            opts.expect_text = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--expect-label")) {
            opts.expect_label = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--min-score")) {
            opts.min_score = try std.fmt.parseFloat(f32, args.next() orelse return usageError());
        } else if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            printUsage();
            return error.HelpRequested;
        } else if (opts.entity_types_csv == null) {
            owned_entity_types_csv = try allocator.dupe(u8, arg);
            opts.entity_types_csv = owned_entity_types_csv.?;
        } else {
            return usageError();
        }
    }

    return .{ .value = opts, .owned_entity_types_csv = owned_entity_types_csv };
}

const EvalSummary = struct {
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    adapter_dir: []const u8,
    text: []const u8,
    entity_types: []const []const u8,
    seq_len: usize,
    num_classes: usize,
    objective: gliner2_autodiff.GlinerObjective,
    lora_rank: usize,
    lora_alpha: f64,
    loaded_base_weight_count: usize,
    top: TopEntity,

    fn deinit(self: *EvalSummary, allocator: std.mem.Allocator) void {
        allocator.free(self.model_dir);
        allocator.free(self.adapter_dir);
        allocator.free(self.text);
        for (self.entity_types) |item| allocator.free(item);
        allocator.free(self.entity_types);
        allocator.free(self.top.text);
        allocator.free(self.top.label);
        self.* = undefined;
    }

    fn jsonView(self: *const EvalSummary) struct {
        model_dir: []const u8,
        adapter_dir: []const u8,
        text: []const u8,
        entity_types: []const []const u8,
        seq_len: usize,
        num_classes: usize,
        objective: []const u8,
        lora_rank: usize,
        lora_alpha: f64,
        loaded_base_weight_count: usize,
        top_entity: TopEntity,
    } {
        return .{
            .model_dir = self.model_dir,
            .adapter_dir = self.adapter_dir,
            .text = self.text,
            .entity_types = self.entity_types,
            .seq_len = self.seq_len,
            .num_classes = self.num_classes,
            .objective = objectiveName(self.objective),
            .lora_rank = self.lora_rank,
            .lora_alpha = self.lora_alpha,
            .loaded_base_weight_count = self.loaded_base_weight_count,
            .top_entity = self.top,
        };
    }
};

fn evalSavedAdapter(allocator: std.mem.Allocator, owned_opts: OwnedOptions) !EvalSummary {
    const opts = owned_opts.value;
    if (opts.seq_len == 0 or opts.seq_len > 4096) return error.InvalidSeqLen;
    if (opts.max_span_width == 0) return error.InvalidMaxSpanWidth;

    var manifest = try loadManifest(allocator, opts.adapter_dir);
    defer manifest.deinit(allocator);
    if (manifest.hidden_size != graph_config.hidden_size) return error.HiddenSizeMismatch;
    const objective = opts.objective_override orelse manifest.objective;

    const entity_types = if (opts.entity_types_csv) |csv|
        try parseCsv(allocator, csv)
    else
        try dupeStringSlice(allocator, manifest.entity_labels);
    errdefer freeStringSlice(allocator, entity_types);
    if (entity_types.len == 0 or entity_types.len + 1 > manifest.num_classes) return error.InvalidEntityTypes;
    if (objective == .span_start and entity_types.len + 1 != manifest.num_classes) return error.InvalidEntityTypes;

    const task_head_path = try std.fs.path.join(allocator, &.{ opts.adapter_dir, gliner2_bundle.task_head_checkpoint_file_name });
    defer allocator.free(task_head_path);
    var task_head = try gliner2_bundle.loadClassifierTaskHead(allocator, task_head_path);
    defer task_head.deinit();
    if (task_head.num_classes != manifest.num_classes or task_head.hidden_size != manifest.hidden_size) return error.TaskHeadShapeMismatch;

    var weight_store = WeightStore{
        .allocator = allocator,
        .resident_weights = .{},
        .lazy_weights = .{},
    };
    var owned_names = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        var it = weight_store.resident_weights.iterator();
        while (it.next()) |entry| entry.value_ptr.deinit();
        weight_store.resident_weights.deinit(allocator);
        for (owned_names.items) |name| allocator.free(name);
        owned_names.deinit(allocator);
    }

    const safetensors_path = try std.fs.path.join(allocator, &.{ opts.model_dir, "model.safetensors" });
    defer allocator.free(safetensors_path);
    const source_ptr = try SafetensorsSource.initAbsolute(allocator, safetensors_path);
    var ws = source_ptr.weightSource();
    defer ws.deinit();
    const loaded_base_weight_count = try loadBaseWeightsFromSource(allocator, &ws, &weight_store, &owned_names);
    try addTaskHeadWeights(allocator, &weight_store, &owned_names, &task_head);

    var native = NativeCompute.init(allocator, &weight_store, null);
    var cb = native.computeBackend();

    var gliner_ctx = gliner2_autodiff.GlinerAutodiffCtx.init(.{
        .graph_config = graph_config,
        .num_classes = @intCast(manifest.num_classes),
        .objective = objective,
    });

    const regular_trainable_params = [_][]const u8{ "classifier.weight", "classifier.bias" };
    var trainer = try real_autodiff.RealAutodiffTrainer.init(
        allocator,
        &cb,
        .{
            .lora = .{
                .rank = @intCast(manifest.lora_rank),
                .alpha = @floatCast(manifest.lora_alpha),
                .target_patterns = manifest.lora_targets,
            },
            .lr_schedule = .{ .constant = 1e-3 },
            .max_grad_norm = 1.0,
            .grad_accum_steps = 1,
            .lora_a_init_std = 0.02,
            .hidden_size_hint = graph_config.hidden_size,
            .num_layers_hint = graph_config.num_hidden_layers,
            .seed = 42,
            .regular_trainable_params = &regular_trainable_params,
        },
    );
    defer trainer.deinit();

    var tokenizer = try gliner2_data.Tokenizer.initGLiNER2HF(allocator, opts.model_dir);
    defer tokenizer.deinit(allocator);

    try ensureGraphBuilt(allocator, &tokenizer, entity_types, opts.text, manifest.num_classes, opts.seq_len, opts.max_span_width, objective, &trainer, &gliner_ctx);

    const adapter_checkpoint_path = try std.fs.path.join(allocator, &.{ opts.adapter_dir, gliner2_bundle.adapter_checkpoint_file_name });
    defer allocator.free(adapter_checkpoint_path);
    try loadPeftAdaptersIntoTrainer(allocator, adapter_checkpoint_path, &trainer);
    try loadTaskHeadIntoTrainer(&task_head, &trainer);

    const top = try decodeTextTopEntity(
        allocator,
        &tokenizer,
        entity_types,
        opts.text,
        manifest.num_classes,
        objective,
        opts.seq_len,
        opts.max_span_width,
        &trainer,
        &gliner_ctx,
    );

    return .{
        .allocator = allocator,
        .model_dir = try allocator.dupe(u8, opts.model_dir),
        .adapter_dir = try allocator.dupe(u8, opts.adapter_dir),
        .text = try allocator.dupe(u8, opts.text),
        .entity_types = entity_types,
        .seq_len = opts.seq_len,
        .num_classes = manifest.num_classes,
        .objective = objective,
        .lora_rank = manifest.lora_rank,
        .lora_alpha = manifest.lora_alpha,
        .loaded_base_weight_count = loaded_base_weight_count,
        .top = .{
            .text = try allocator.dupe(u8, top.text),
            .label = try allocator.dupe(u8, top.label),
            .start = top.start,
            .end = top.end,
            .score = top.score,
        },
    };
}

fn loadManifest(allocator: std.mem.Allocator, adapter_dir: []const u8) !Manifest {
    const manifest_path = try std.fs.path.join(allocator, &.{ adapter_dir, "training_manifest.json" });
    defer allocator.free(manifest_path);
    const data = try compat.cwd().readFileAlloc(compat.io(), manifest_path, allocator, .limited(4 * 1024 * 1024));
    defer allocator.free(data);
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, data, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidTrainingManifest;
    const obj = parsed.value.object;
    return .{
        .num_classes = jsonUsize(obj.get("num_classes")) orelse return error.InvalidTrainingManifest,
        .hidden_size = jsonUsize(obj.get("hidden_size")) orelse return error.InvalidTrainingManifest,
        .lora_rank = jsonUsize(obj.get("lora_rank")) orelse return error.InvalidTrainingManifest,
        .lora_alpha = jsonF64(obj.get("lora_alpha")) orelse return error.InvalidTrainingManifest,
        .lora_targets = try parseCsv(allocator, jsonString(obj.get("lora_targets")) orelse return error.InvalidTrainingManifest),
        .entity_labels = try parseStringArray(allocator, obj.get("entity_labels") orelse return error.InvalidTrainingManifest),
        .objective = try parseObjective(jsonString(obj.get("objective")) orelse "token"),
    };
}

fn loadBaseWeightsFromSource(
    allocator: std.mem.Allocator,
    ws: anytype,
    weight_store: *WeightStore,
    owned_names: *std.ArrayListUnmanaged([]const u8),
) !usize {
    const hf_names = try ws.listNames(allocator);
    defer allocator.free(hf_names);

    var loaded_count: usize = 0;
    for (hf_names) |hf_name| {
        var lw = try ws.getTensor(hf_name);
        const stripped = stripEncoderPrefix(hf_name);
        const owned_name = try allocator.dupe(u8, stripped);
        try owned_names.append(allocator, owned_name);
        lw.tensor.name = owned_name;
        try weight_store.resident_weights.put(allocator, owned_name, lw);
        loaded_count += 1;
    }
    return loaded_count;
}

fn addTaskHeadWeights(
    allocator: std.mem.Allocator,
    weight_store: *WeightStore,
    owned_names: *std.ArrayListUnmanaged([]const u8),
    head: *const gliner2_bundle.ClassifierTaskHead,
) !void {
    {
        const name = try allocator.dupe(u8, "classifier.weight");
        try owned_names.append(allocator, name);
        const shape = [_]i64{ @intCast(head.num_classes), @intCast(head.hidden_size) };
        const tensor = try Tensor.initFloat32(allocator, name, &shape, head.weight);
        try weight_store.resident_weights.put(allocator, name, LoadedWeight{ .tensor = tensor });
    }
    {
        const name = try allocator.dupe(u8, "classifier.bias");
        try owned_names.append(allocator, name);
        const shape = [_]i64{@intCast(head.num_classes)};
        const tensor = try Tensor.initFloat32(allocator, name, &shape, head.bias);
        try weight_store.resident_weights.put(allocator, name, LoadedWeight{ .tensor = tensor });
    }
}

fn ensureGraphBuilt(
    allocator: std.mem.Allocator,
    tokenizer: *const gliner2_data.Tokenizer,
    entity_types: []const []const u8,
    text: []const u8,
    num_classes: usize,
    seq_len: usize,
    max_span_width: usize,
    objective: gliner2_autodiff.GlinerObjective,
    trainer: *real_autodiff.RealAutodiffTrainer,
    gliner_ctx: *gliner2_autodiff.GlinerAutodiffCtx,
) !void {
    switch (objective) {
        .token => {
            const input_ids = try allocator.alloc(i64, seq_len);
            defer allocator.free(input_ids);
            const attention_mask = try allocator.alloc(f32, seq_len);
            defer allocator.free(attention_mask);
            const targets = try allocator.alloc(f32, seq_len * num_classes);
            defer allocator.free(targets);
            try fillInferenceBuffers(allocator, tokenizer, entity_types, text, num_classes, input_ids, attention_mask, targets);
            const trainer_input = gliner2_autodiff.makeTrainerInput(
                gliner_ctx,
                input_ids,
                attention_mask,
                targets,
                gliner2_autodiff.tokenTargetsShape(1, @intCast(seq_len), @intCast(num_classes)),
                1,
                @intCast(seq_len),
            );
            try trainer.ensureGraphBuilt(trainer_input);
        },
        .span_start => {
            const examples = [_]gliner2_data.Example{.{ .text = text, .entities = &.{} }};
            var encoded = try gliner2_data.buildSimpleBatch(allocator, tokenizer, &examples, entity_types, seq_len, max_span_width, 1);
            defer encoded.deinit();
            const input_ids = try allocator.alloc(i64, seq_len);
            defer allocator.free(input_ids);
            const attention_mask = try allocator.alloc(f32, seq_len);
            defer allocator.free(attention_mask);
            try copyEncodedInputs(&encoded, input_ids, attention_mask);
            const target_width = 2 * encoded.num_entity_types + 1;
            const target_len = encoded.batch_size * encoded.max_spans * target_width;
            const targets = try allocator.alloc(f32, target_len);
            defer allocator.free(targets);
            _ = try gliner2_autodiff.fillSpanStartTargetsFromEncodedBatch(&encoded, targets);
            const trainer_input = gliner2_autodiff.makeTrainerInput(
                gliner_ctx,
                input_ids,
                attention_mask,
                targets,
                gliner2_autodiff.spanStartTargetsShape(1, @intCast(encoded.max_spans), @intCast(encoded.num_entity_types)),
                1,
                @intCast(seq_len),
            );
            try trainer.ensureGraphBuilt(trainer_input);
        },
    }
}

fn decodeTextTopEntity(
    allocator: std.mem.Allocator,
    tokenizer: *const gliner2_data.Tokenizer,
    entity_types: []const []const u8,
    text: []const u8,
    num_classes: usize,
    objective: gliner2_autodiff.GlinerObjective,
    seq_len: usize,
    max_span_width: usize,
    trainer: *real_autodiff.RealAutodiffTrainer,
    gliner_ctx: *gliner2_autodiff.GlinerAutodiffCtx,
) !TopEntity {
    return switch (objective) {
        .token => decodeTextTopEntityFromTokenBridge(
            allocator,
            tokenizer,
            entity_types,
            text,
            num_classes,
            seq_len,
            max_span_width,
            trainer,
            gliner_ctx,
        ),
        .span_start => decodeTextTopEntityFromSpanStart(
            allocator,
            tokenizer,
            entity_types,
            text,
            seq_len,
            max_span_width,
            trainer,
            gliner_ctx,
        ),
    };
}

fn decodeTextTopEntityFromTokenBridge(
    allocator: std.mem.Allocator,
    tokenizer: *const gliner2_data.Tokenizer,
    entity_types: []const []const u8,
    text: []const u8,
    num_classes: usize,
    seq_len: usize,
    max_span_width: usize,
    trainer: *real_autodiff.RealAutodiffTrainer,
    gliner_ctx: *gliner2_autodiff.GlinerAutodiffCtx,
) !TopEntity {
    const input_ids = try allocator.alloc(i64, seq_len);
    defer allocator.free(input_ids);
    const attention_mask = try allocator.alloc(f32, seq_len);
    defer allocator.free(attention_mask);
    const targets = try allocator.alloc(f32, seq_len * num_classes);
    defer allocator.free(targets);
    try fillInferenceBuffers(allocator, tokenizer, entity_types, text, num_classes, input_ids, attention_mask, targets);

    const logits = try gliner2_autodiff.tokenLogitsForBatch(
        allocator,
        trainer,
        gliner_ctx,
        input_ids,
        attention_mask,
        1,
        @intCast(seq_len),
    );
    defer allocator.free(logits);
    if (logits.len != seq_len * num_classes) return error.LogitShapeMismatch;
    for (logits) |value| if (!std.math.isFinite(value)) return error.NonFiniteLogit;

    const examples = [_]gliner2_data.Example{.{ .text = text, .entities = &.{} }};
    var decoded_batch = try gliner2_data.buildSimpleBatch(
        allocator,
        tokenizer,
        &examples,
        entity_types,
        seq_len,
        max_span_width,
        1,
    );
    defer decoded_batch.deinit();
    const span_scores = try gliner2_data.tokenLogitsToSpanScoresAlloc(allocator, &decoded_batch, logits, num_classes);
    defer allocator.free(span_scores);

    var max_span_score: f32 = 0.0;
    for (span_scores) |score| {
        if (!std.math.isFinite(score)) return error.NonFiniteSpanScore;
        max_span_score = @max(max_span_score, score);
    }
    if (max_span_score <= 0.0) return error.NoPositiveSpanScores;

    const predictions = try gliner2_data.decodeEntityPredictionsAlloc(
        allocator,
        &decoded_batch,
        &examples,
        entity_types,
        span_scores,
        max_span_score - 1e-6,
    );
    defer allocator.free(predictions);
    if (predictions.len == 0) return error.NoEntityPredictions;
    return .{
        .text = predictions[0].text,
        .label = predictions[0].label,
        .start = predictions[0].start,
        .end = predictions[0].end,
        .score = predictions[0].score,
    };
}

fn decodeTextTopEntityFromSpanStart(
    allocator: std.mem.Allocator,
    tokenizer: *const gliner2_data.Tokenizer,
    entity_types: []const []const u8,
    text: []const u8,
    seq_len: usize,
    max_span_width: usize,
    trainer: *real_autodiff.RealAutodiffTrainer,
    gliner_ctx: *gliner2_autodiff.GlinerAutodiffCtx,
) !TopEntity {
    const examples = [_]gliner2_data.Example{.{ .text = text, .entities = &.{} }};
    var decoded_batch = try gliner2_data.buildSimpleBatch(
        allocator,
        tokenizer,
        &examples,
        entity_types,
        seq_len,
        max_span_width,
        1,
    );
    defer decoded_batch.deinit();

    const input_ids = try allocator.alloc(i64, seq_len);
    defer allocator.free(input_ids);
    const attention_mask = try allocator.alloc(f32, seq_len);
    defer allocator.free(attention_mask);
    try copyEncodedInputs(&decoded_batch, input_ids, attention_mask);

    const target_width = 2 * decoded_batch.num_entity_types + 1;
    const target_len = decoded_batch.batch_size * decoded_batch.max_spans * target_width;
    const targets = try allocator.alloc(f32, target_len);
    defer allocator.free(targets);
    _ = try gliner2_autodiff.fillSpanStartTargetsFromEncodedBatch(&decoded_batch, targets);

    const logits = try gliner2_autodiff.spanStartLogitsForBatch(
        allocator,
        trainer,
        gliner_ctx,
        input_ids,
        attention_mask,
        targets,
        gliner2_autodiff.spanStartTargetsShape(1, @intCast(decoded_batch.max_spans), @intCast(decoded_batch.num_entity_types)),
        1,
        @intCast(seq_len),
    );
    defer allocator.free(logits);

    const expected_scores = decoded_batch.batch_size * decoded_batch.max_spans * decoded_batch.num_entity_types;
    if (logits.len != expected_scores) return error.LogitShapeMismatch;
    const span_scores = try allocator.alloc(f32, expected_scores);
    defer allocator.free(span_scores);
    var max_span_score: f32 = 0.0;
    for (0..decoded_batch.batch_size * decoded_batch.max_spans) |span_idx| {
        const valid = decoded_batch.span_mask[span_idx] > 0.0;
        for (0..decoded_batch.num_entity_types) |entity_idx| {
            const idx = span_idx * decoded_batch.num_entity_types + entity_idx;
            const logit = logits[idx];
            if (!std.math.isFinite(logit)) return error.NonFiniteLogit;
            const score = if (valid) sigmoid(logit) else 0.0;
            if (!std.math.isFinite(score)) return error.NonFiniteSpanScore;
            span_scores[idx] = score;
            if (valid) max_span_score = @max(max_span_score, score);
        }
    }
    if (max_span_score <= 0.0) return error.NoPositiveSpanScores;

    const predictions = try gliner2_data.decodeEntityPredictionsAlloc(
        allocator,
        &decoded_batch,
        &examples,
        entity_types,
        span_scores,
        max_span_score - 1e-6,
    );
    defer allocator.free(predictions);
    if (predictions.len == 0) return error.NoEntityPredictions;
    return .{
        .text = predictions[0].text,
        .label = predictions[0].label,
        .start = predictions[0].start,
        .end = predictions[0].end,
        .score = predictions[0].score,
    };
}

fn fillInferenceBuffers(
    allocator: std.mem.Allocator,
    tokenizer: *const gliner2_data.Tokenizer,
    entity_types: []const []const u8,
    text: []const u8,
    num_classes: usize,
    input_ids: []i64,
    attention_mask: []f32,
    targets: []f32,
) !void {
    const seq_len = input_ids.len;
    if (attention_mask.len != seq_len or targets.len != seq_len * num_classes) return error.InvalidInputShape;
    var tok_ids_buf: [4096]i32 = undefined;
    var tok_mask_buf: [4096]i32 = undefined;
    var words_mask_buf: [4096]i32 = undefined;
    var first_pos_buf: [4096]i32 = undefined;
    var e_tok_pos_buf: [128]i32 = undefined;
    var e_tok_end_buf: [128]i32 = undefined;
    if (seq_len > tok_ids_buf.len or entity_types.len > e_tok_pos_buf.len) return error.InvalidInputShape;

    const tok_ids = tok_ids_buf[0..seq_len];
    const tok_mask = tok_mask_buf[0..seq_len];
    const words_mask = words_mask_buf[0..seq_len];
    const first_pos = first_pos_buf[0..seq_len];
    const e_pos = e_tok_pos_buf[0..entity_types.len];
    const e_end = e_tok_end_buf[0..entity_types.len];
    _ = tokenizer.encodeInto(allocator, text, entity_types, tok_ids, tok_mask, words_mask, first_pos, e_pos, e_end);

    @memset(targets, 0.0);
    for (0..seq_len) |idx| {
        input_ids[idx] = tok_ids[idx];
        attention_mask[idx] = @floatFromInt(tok_mask[idx]);
        const row = idx * num_classes;
        if (tok_mask[idx] != 0) targets[row] = 1.0;
    }
}

fn copyEncodedInputs(
    batch: *const gliner2_data.EncodedBatch,
    input_ids: []i64,
    attention_mask: []f32,
) !void {
    const expected = batch.batch_size * batch.max_length;
    if (batch.batch_size != 1 or input_ids.len != expected or attention_mask.len != expected) return error.InvalidInputShape;
    if (batch.input_ids.len != expected or batch.attention_mask.len != expected) return error.InvalidInputShape;
    for (0..expected) |idx| {
        input_ids[idx] = batch.input_ids[idx];
        attention_mask[idx] = @floatFromInt(batch.attention_mask[idx]);
    }
}

fn sigmoid(x: f32) f32 {
    if (x >= 0.0) {
        const z = @exp(-x);
        return 1.0 / (1.0 + z);
    }
    const z = @exp(x);
    return z / (1.0 + z);
}

fn loadPeftAdaptersIntoTrainer(
    allocator: std.mem.Allocator,
    adapter_checkpoint_path: []const u8,
    trainer: *real_autodiff.RealAutodiffTrainer,
) !void {
    var reader = try safetensors.MMapReader.openFileAbsolute(allocator, adapter_checkpoint_path);
    defer reader.deinit();
    for (trainer.lora_params.items) |*slot| {
        const peft_name = try autodiffSlotNameToPeftName(allocator, slot.name);
        defer allocator.free(peft_name);
        var tensor = try reader.readTensor(peft_name);
        defer tensor.deinit();
        if (tensor.elementCount() != slot.weights.len) return error.AdapterTensorShapeMismatch;
        try copyTensorF32Into(slot.weights, &tensor);
        @memset(slot.grad_accum, 0.0);
    }
}

fn loadTaskHeadIntoTrainer(
    head: *const gliner2_bundle.ClassifierTaskHead,
    trainer: *real_autodiff.RealAutodiffTrainer,
) !void {
    for (trainer.regular_params.items) |*slot| {
        if (std.mem.eql(u8, slot.name, "classifier.weight")) {
            if (slot.weights.len != head.weight.len) return error.TaskHeadShapeMismatch;
            @memcpy(slot.weights, head.weight);
            @memset(slot.grad_accum, 0.0);
        } else if (std.mem.eql(u8, slot.name, "classifier.bias")) {
            if (slot.weights.len != head.bias.len) return error.TaskHeadShapeMismatch;
            @memcpy(slot.weights, head.bias);
            @memset(slot.grad_accum, 0.0);
        }
    }
}

fn copyTensorF32Into(dst: []f32, tensor: *const Tensor) !void {
    if (tensor.dtype != .f32) return error.AdapterTensorDTypeMismatch;
    if (tensor.data.len != dst.len * @sizeOf(f32)) return error.AdapterTensorShapeMismatch;
    for (dst, 0..) |*value, idx| {
        const raw = tensor.data[idx * @sizeOf(f32) ..][0..@sizeOf(f32)];
        value.* = @bitCast(std.mem.readInt(u32, raw, .little));
    }
}

fn autodiffSlotNameToPeftName(allocator: std.mem.Allocator, name: []const u8) ![]const u8 {
    if (std.mem.endsWith(u8, name, ".lora_A")) {
        const base = name[0 .. name.len - ".lora_A".len];
        return autodiffBaseToPeftName(allocator, tensorBaseName(base), "lora_A");
    }
    if (std.mem.endsWith(u8, name, ".lora_B")) {
        const base = name[0 .. name.len - ".lora_B".len];
        return autodiffBaseToPeftName(allocator, tensorBaseName(base), "lora_B");
    }
    return error.InvalidAutodiffAdapterName;
}

fn autodiffBaseToPeftName(allocator: std.mem.Allocator, base_no_weight: []const u8, adapter_name: []const u8) ![]const u8 {
    if (std.mem.startsWith(u8, base_no_weight, "encoder.layer.")) {
        return std.fmt.allocPrint(allocator, "encoder.{s}.{s}.weight", .{ base_no_weight, adapter_name });
    }
    return std.fmt.allocPrint(allocator, "{s}.{s}.weight", .{ base_no_weight, adapter_name });
}

fn tensorBaseName(tensor_name: []const u8) []const u8 {
    if (std.mem.endsWith(u8, tensor_name, ".weight")) return tensor_name[0 .. tensor_name.len - ".weight".len];
    return tensor_name;
}

fn stripEncoderPrefix(name: []const u8) []const u8 {
    const prefix = "encoder.";
    if (std.mem.startsWith(u8, name, prefix)) return name[prefix.len..];
    return name;
}

fn parseCsv(allocator: std.mem.Allocator, value: []const u8) ![][]const u8 {
    var out = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (out.items) |item| allocator.free(item);
        out.deinit(allocator);
    }
    var iter = std.mem.splitScalar(u8, value, ',');
    while (iter.next()) |raw| {
        const item = std.mem.trim(u8, raw, " \t\r\n");
        if (item.len == 0) continue;
        try out.append(allocator, try allocator.dupe(u8, item));
    }
    if (out.items.len == 0) return error.EmptyCsv;
    return try out.toOwnedSlice(allocator);
}

fn parseStringArray(allocator: std.mem.Allocator, value: std.json.Value) ![][]const u8 {
    if (value != .array) return error.InvalidTrainingManifest;
    var out = try allocator.alloc([]const u8, value.array.items.len);
    errdefer {
        for (out) |item| allocator.free(item);
        allocator.free(out);
    }
    for (value.array.items, 0..) |entry, idx| {
        if (entry != .string) return error.InvalidTrainingManifest;
        out[idx] = try allocator.dupe(u8, entry.string);
    }
    return out;
}

fn dupeStringSlice(allocator: std.mem.Allocator, values: []const []const u8) ![][]const u8 {
    var out = try allocator.alloc([]const u8, values.len);
    errdefer {
        for (out) |item| allocator.free(item);
        allocator.free(out);
    }
    for (values, 0..) |value, idx| out[idx] = try allocator.dupe(u8, value);
    return out;
}

fn freeStringSlice(allocator: std.mem.Allocator, values: []const []const u8) void {
    for (values) |value| allocator.free(value);
    allocator.free(values);
}

fn parseObjective(value: []const u8) !gliner2_autodiff.GlinerObjective {
    if (std.mem.eql(u8, value, "token")) return .token;
    if (std.mem.eql(u8, value, "span-start") or std.mem.eql(u8, value, "span_start")) return .span_start;
    return error.InvalidObjective;
}

fn objectiveName(objective: gliner2_autodiff.GlinerObjective) []const u8 {
    return switch (objective) {
        .token => "token",
        .span_start => "span-start",
    };
}

fn jsonString(value: ?std.json.Value) ?[]const u8 {
    const v = value orelse return null;
    return switch (v) {
        .string => |s| s,
        else => null,
    };
}

fn jsonF64(value: ?std.json.Value) ?f64 {
    const v = value orelse return null;
    return switch (v) {
        .integer => |i| @floatFromInt(i),
        .float => |f| f,
        else => null,
    };
}

fn jsonUsize(value: ?std.json.Value) ?usize {
    const v = value orelse return null;
    return switch (v) {
        .integer => |i| if (i >= 0) @intCast(i) else null,
        else => null,
    };
}

fn usageError() error{InvalidArguments} {
    printUsage();
    return error.InvalidArguments;
}

fn printUsage() void {
    std.debug.print(
        \\usage: eval-gliner2-autodiff-adapter <model_dir> <adapter_dir> <text> [entity_types_csv] [options]
        \\example: eval-gliner2-autodiff-adapter /tmp/gliner2 /tmp/gliner2-run "Alice joined Acme in Paris" person,organization,location --expect-label organization --min-score 0.05
        \\
        \\options:
        \\  --entity-types CSV
        \\  --seq-len N
        \\  --max-span-width N
        \\  --objective token|span-start
        \\  --expect-text TEXT
        \\  --expect-label LABEL
        \\  --min-score FLOAT
        \\
    , .{});
}
