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

//! Supervised frozen-encoder ModernBERT span task-head training. Source and
//! datasets are admitted once, train/eval text is disjoint, optimizer resume
//! binds the complete run identity, and final publication is atomic.
const std = @import("std");
const head = @import("head.zig");
const features = @import("features.zig");
const factory = @import("../../architectures/session_factory.zig");
const safetensors = @import("../../models/safetensors.zig");
const checkpoint = @import("../safetensors_checkpoint.zig");
const processor = @import("../../pipelines/gliner_boundary_processor.zig");
const schema = @import("../../pipelines/extraction_schema.zig");
const native = @import("../../ops/native_compute.zig");
const hf = @import("inference_hf_tokenizer");
const qualification = @import("../../models/gliner_decide_qualification.zig");
const file = @import("../../util/c_file.zig");
const publication = @import("../../gliner_boundary_export.zig");
const Budget = @import("../../runtime/bounded_allocator.zig").BoundedAllocator;
const A = std.mem.Allocator;
const Best = @import("../early_stopping.zig").EarlyStoppingState;
const Control = @import("../../execution_control.zig").InferenceExecutionControl;
const prefixes = [_][]const u8{ "classifier.2", "count_pred.2", "span_rep.span_rep_layer.out_project.3" };
const sidecars = [_][]const u8{ "config.json", "encoder_config/config.json", "tokenizer.json", "tokenizer_config.json" };

pub const Config = struct {
    version: u32 = 1,
    timeout_seconds: u32 = 4 * 60 * 60,
    model_dir: []const u8,
    train_file: []const u8,
    eval_file: []const u8,
    output_dir: []const u8,
    resume_from: ?[]const u8 = null,
    epochs: u32 = 20,
    learning_rate: f32 = 1e-4,
    positive_weight: f32 = 10,
    max_grad_norm: f32 = 1,
    seed: u64 = 42,
    checkpoint_every_epochs: u32 = 1,
    stop_after_microbatches: ?u64 = null,
    max_examples: usize = 1024,
    max_sequence_tokens: usize = 1024,
    max_host_bytes: usize = 10 * 1024 * 1024 * 1024,
};
pub const Metrics = struct {
    loss: f64 = 0,
    categorical_correct: usize = 0,
    categorical_tasks: usize = 0,
    span_true_positive: usize = 0,
    span_false_positive: usize = 0,
    span_false_negative: usize = 0,
    span_f1: f64 = 0,
};

fn path(a: A, root: []const u8, name: []const u8) ![]u8 {
    return std.fs.path.join(a, &.{ root, name });
}
fn digest(bytes: []const u8) [32]u8 {
    var result: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &result, .{});
    return result;
}
fn writeJson(a: A, io: std.Io, name: []const u8, value: anytype) !void {
    const data = try std.json.Stringify.valueAlloc(a, value, .{ .whitespace = .indent_2 });
    defer a.free(data);
    var stream = try std.Io.Dir.cwd().createFile(io, name, .{});
    defer stream.close(io);
    try stream.writeStreamingAll(io, data);
    try stream.sync(io);
}
const dataset_mod = @import("../gliner/boundary_dataset.zig");
const targets = @import("../gliner/boundary_targets.zig");
const offsets = @import("../../pipelines/gliner_boundary_decode.zig");
fn sourceSpan(map: *offsets.OffsetMap, source: targets.Source) !features.Span {
    const span = try map.toBytes(.{ .start = source.start, .end = source.end }, source.unit);
    return .{ .start = span.start, .end = span.end };
}
pub fn records(a: A, data: *const dataset_mod.Dataset) ![]features.Record {
    const result = try a.alloc(features.Record, data.index.len);
    for (result, 0..) |*record, index| {
        var sample = try data.sample(index, null, null);
        defer sample.deinit();
        if (sample.schema.schema.structures.len != 0 or sample.schema.schema.entity_attributes.len != 0 or sample.schema.schema.joint_ie != null) return error.UnsupportedSpanTrainingTask;
        var map = try offsets.OffsetMap.init(a, sample.row.text, sample.row.text.len);
        defer map.deinit();
        const entities = try a.alloc(features.Entity, sample.annotations.entities.len);
        for (entities, sample.annotations.entities) |*entity, annotation| {
            if (annotation.attributes.len != 0) return error.UnsupportedSpanTrainingTask;
            const span = try sourceSpan(&map, annotation.source);
            entity.* = .{ .label = try a.dupe(u8, sample.schema.schema.entities[annotation.entity_type].name), .start = span.start, .end = span.end };
        }
        const relations = try a.alloc(features.Relation, sample.annotations.relations.len);
        for (relations, sample.annotations.relations) |*relation, annotation| {
            var ends: [2]features.Span = undefined;
            for ([_]targets.Endpoint{ annotation.head, annotation.tail }, &ends) |endpoint, *span| span.* = switch (endpoint) {
                .document => |source| try sourceSpan(&map, source),
                .entity => |i| .{ .start = entities[i].start, .end = entities[i].end },
            };
            relation.* = .{ .type = try a.dupe(u8, sample.schema.schema.relations[annotation.relation_type].name), .head = ends[0], .tail = ends[1] };
        }
        const classifications = try a.alloc(features.Classification, sample.annotations.classifications.len);
        for (classifications, sample.annotations.classifications) |*classification, annotation| {
            const task = sample.schema.schema.classifications[annotation.task];
            if (task.mode != .single or annotation.labels.len != 1) return error.UnsupportedSpanTrainingTask;
            classification.* = .{ .task = try a.dupe(u8, task.task.name), .label = try a.dupe(u8, task.task.labels[annotation.labels[0]]), .probabilities = if (annotation.probabilities) |values| try a.dupe(f32, values) else null };
        }
        record.* = .{ .id = try a.dupe(u8, sample.row.id), .text = try a.dupe(u8, sample.row.text), .schema_json = try std.json.Stringify.valueAlloc(a, sample.row.schema, .{}), .offset_unit = .utf8_bytes, .entities = entities, .relations = relations, .classifications = classifications };
    }
    return result;
}
fn buildCache(a: A, backing: A, session: @import("../../backends/session.zig").Session, tok: @import("inference_tokenizer").Tokenizer, config: @import("../../architectures/modern_bert.zig").Config, data: []const features.Record, max_tokens: usize, control: Control) ![]features.Cached {
    var result: std.ArrayList(features.Cached) = .empty;
    for (data) |record| {
        try control.check();
        std.debug.print("GLiNER span cache: {s}\n", .{record.id});
        var scratch = std.heap.ArenaAllocator.init(backing);
        defer scratch.deinit();
        const temporary = scratch.allocator();
        var compiled = try schema.compile(temporary, record.schema_json, .{});
        defer compiled.deinit();
        var prepared = try processor.prepare(temporary, tok, &.{.{ .text = record.text, .schema = &compiled }}, .{ .max_sequence_tokens = max_tokens, .max_batch_tokens = max_tokens, .control = control });
        defer prepared.deinit();
        var managed = try factory.getManagedComputeBackend(session, backing, null, control);
        defer managed.deinit();
        const examples = try features.prepare(temporary, &managed.backend, config, prepared.samples[0], &compiled, record);
        for (examples) |example| {
            var retained = example;
            retained.example.features = try a.dupe(f32, example.example.features);
            retained.example.projection = try a.dupe(f32, example.example.projection);
            retained.example.targets = try a.dupe(f32, example.example.targets);
            retained.example.mask = try a.dupe(bool, example.example.mask);
            try result.append(a, retained);
        }
    }
    if (result.items.len == 0) return error.EmptySpanTrainingTargets;
    return result.toOwnedSlice(a);
}
fn evaluate(a: A, trainer: *head.controller.Trainer, data: []const features.Cached, weight: f32) !Metrics {
    var result = Metrics{};
    for (data) |example| {
        var program = try head.Program.init(a, example.prefix, example.layout);
        defer program.deinit();
        const logits = try program.predict(a, trainer, example.example);
        defer a.free(logits);
        const loss = try head.loss(a, example.objective, logits, example.example.targets, example.example.mask, weight);
        defer a.free(loss.gradient);
        result.loss += loss.value;
        if (!std.mem.startsWith(u8, example.prefix, "span_rep.")) {
            var best: usize = 0;
            for (logits, 0..) |z, i| if (z > logits[best]) {
                best = i;
            };
            var gold: usize = 0;
            for (example.example.targets, 0..) |target, i| if (target > example.example.targets[gold]) {
                gold = i;
            };
            if (example.gold_index) |human_gold| gold = human_gold;
            if (gold >= logits.len) return error.InvalidSpanTrainingTarget;
            result.categorical_correct += @intFromBool(best == gold);
            result.categorical_tasks += 1;
        } else for (logits, example.example.targets, 0..) |z, t, i| {
            if (example.example.mask.len != 0 and !example.example.mask[i]) continue;
            if (z >= 0 and t > 0.5) result.span_true_positive += 1;
            if (z >= 0 and t < 0.5) result.span_false_positive += 1;
            if (z < 0 and t > 0.5) result.span_false_negative += 1;
        }
    }
    result.loss /= @floatFromInt(data.len);
    const denominator = 2 * result.span_true_positive + result.span_false_positive + result.span_false_negative;
    if (denominator > 0) result.span_f1 = @as(f64, @floatFromInt(2 * result.span_true_positive)) / @as(f64, @floatFromInt(denominator));
    return result;
}

pub fn execute(backing: A, io: std.Io, c: Config, control: Control) !void {
    try control.check();
    if (c.version != 1 or c.timeout_seconds == 0 or c.timeout_seconds > 7 * 24 * 60 * 60 or c.epochs == 0 or c.max_examples == 0 or c.max_examples > 10000 or c.max_sequence_tokens == 0 or c.max_sequence_tokens > 1024 or c.checkpoint_every_epochs == 0 or !std.math.isFinite(c.learning_rate) or c.learning_rate <= 0 or !std.math.isFinite(c.positive_weight) or c.positive_weight <= 0 or !std.math.isFinite(c.max_grad_norm) or c.max_grad_norm <= 0) return error.InvalidSpanTrainingConfig;
    for ([_][]const u8{ c.model_dir, c.train_file, c.eval_file, c.output_dir }) |name| if (!std.fs.path.isAbsolute(name)) return error.SpanTrainingRequiresAbsolutePaths;
    if (c.resume_from) |resume_path| if (!std.fs.path.isAbsolute(resume_path)) return error.SpanTrainingRequiresAbsolutePaths;
    var budget = Budget{ .backing = backing, .limit = c.max_host_bytes };
    const a = budget.allocator();
    var permanent = std.heap.ArenaAllocator.init(a);
    defer permanent.deinit();
    const p = permanent.allocator();
    const train_bytes = try std.Io.Dir.cwd().readFileAlloc(io, c.train_file, p, .limited(16 * 1024 * 1024));
    const eval_bytes = try std.Io.Dir.cwd().readFileAlloc(io, c.eval_file, p, .limited(16 * 1024 * 1024));
    const dataset_options = dataset_mod.Options{ .limits = .{ .max_file_bytes = 16 * 1024 * 1024, .max_examples = c.max_examples } };
    var train_data = try dataset_mod.Dataset.fromBytes(a, train_bytes, dataset_options, control, null);
    defer train_data.deinit();
    var eval_data = try dataset_mod.Dataset.fromBytes(a, eval_bytes, dataset_options, control, null);
    defer eval_data.deinit();
    try train_data.requireDisjoint(&eval_data, control);
    const train = try records(p, &train_data);
    const eval = try records(p, &eval_data);
    var source = try safetensors.MMapReader.openFileAbsoluteLimited(a, try path(p, c.model_dir, "model.safetensors"), 8 * 1024 * 1024 * 1024, 4 * 1024 * 1024);
    defer source.deinit();
    const source_digest = qualification.Digest.of(source.file_bytes);
    try source_digest.verify(.{ .path = "model.safetensors", .size_bytes = qualification.pinned_fp32_identity.weight.size_bytes, .sha256 = &qualification.pinned_fp32_identity.weight.sha256 });
    var assets: [sidecars.len][]const u8 = undefined;
    for (sidecars, &assets) |name, *bytes| bytes.* = try file.readFileMax(p, try path(p, c.model_dir, name), 64 * 1024 * 1024);
    const tokenizer = try hf.HfTokenizer.loadFromBytes(a, assets[2]);
    defer tokenizer.tokenizer().deinitTokenizer();
    const caches = cache: {
        const session = try factory.createNativeSession(a, c.model_dir);
        defer session.close();
        try factory.sealGlinerDecisionTokenizerDigest(session, qualification.Digest.of(assets[2]));
        const identity = try factory.getGlinerDecisionIdentity(session);
        if (try qualification.require(identity) != .decide_1b or identity.weight_companion != null) return error.UnsupportedSpanTrainingSource;
        const config = try factory.getGlinerSpanConfig(session);
        if (config != .modern_bert) return error.UnsupportedSpanTrainingSource;
        break :cache .{ try buildCache(p, a, session, tokenizer.tokenizer(), config.modern_bert, train, c.max_sequence_tokens, control), try buildCache(p, a, session, tokenizer.tokenizer(), config.modern_bert, eval, c.max_sequence_tokens, control) };
    };
    var store = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
    defer store.deinitOwned();
    var compute = native.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    var cb = compute.computeBackend();
    cb.execution_control = control;
    var parameters: std.ArrayList(head.controller.Parameter) = .empty;
    for (prefixes) |prefix| for ([_][]const u8{ "weight", "bias" }) |suffix| {
        const name = try std.fmt.allocPrint(p, "{s}.{s}", .{ prefix, suffix });
        var tensor = try source.readTensor(name);
        defer tensor.deinit();
        if (tensor.dtype != .f32) return error.UnsupportedSpanTrainingSource;
        const dims = try p.alloc(i32, tensor.shape.len);
        for (tensor.shape, dims) |dim, *out| out.* = @intCast(dim);
        try parameters.append(p, .{ .name = name, .values = try p.dupe(f32, tensor.asFloat32()), .dimensions = dims, .group = 0 });
    };
    var identity_hash = std.crypto.hash.sha2.Sha256.init(.{});
    identity_hash.update("antfly.modernbert.span-head-training/v1");
    identity_hash.update(&source_digest.sha256);
    identity_hash.update(train_bytes);
    identity_hash.update(eval_bytes);
    var identity_config = c;
    identity_config.output_dir = "";
    identity_config.resume_from = null;
    identity_config.stop_after_microbatches = null;
    identity_hash.update(try std.json.Stringify.valueAlloc(p, identity_config, .{}));
    var run: [32]u8 = undefined;
    identity_hash.final(&run);
    const groups = [_]head.controller.Group{.{ .schedule = .{ .constant = c.learning_rate }, .optimizer = .{ .weight_decay = 0 } }};
    const opts = head.controller.Config{ .groups = &groups, .max_grad_norm = c.max_grad_norm, .limits = .{ .max_state_bytes = c.max_host_bytes, .max_transaction_bytes = c.max_host_bytes } };
    var trainer = if (c.resume_from) |resume_path| try head.controller.Trainer.initRestored(a, &cb, parameters.items, opts, resume_path, run, control) else try head.controller.Trainer.init(a, &cb, parameters.items, opts);
    defer trainer.deinit();
    const initial = try evaluate(a, &trainer, caches[1], c.positive_weight);
    try std.Io.Dir.cwd().createDir(io, c.output_dir, .default_dir);
    const checkpoint_path = try path(p, c.output_dir, "latest.safetensors");
    const best_path = try path(p, c.output_dir, "best.safetensors");
    const has_span_targets = for (caches[1]) |example| {
        if (std.mem.startsWith(u8, example.prefix, "span_rep.")) break true;
    } else false;
    var best = Best.init(.{ .direction = .maximize, .patience = std.math.maxInt(u32) });
    const initial_score: f32 = @floatCast(if (has_span_targets) initial.span_f1 else @as(f64, @floatFromInt(initial.categorical_correct)) / @as(f64, @floatFromInt(@max(initial.categorical_tasks, 1))));
    _ = best.observe(initial_score);
    var selected_epoch: usize = 0;
    try trainer.save(best_path, run, control);
    const total_steps = try std.math.mul(u64, c.epochs, caches[0].len);
    const resumed = trainer.identity().microbatch_step;
    if (resumed > total_steps) return error.InvalidSpanTrainingResume;
    const order = try p.alloc(usize, caches[0].len);
    var step: u64 = resumed;
    for (0..c.epochs) |epoch| {
        if (@as(u64, @intCast(epoch + 1)) * caches[0].len <= resumed) continue;
        for (order, 0..) |*index, i| index.* = i;
        var random = std.Random.DefaultPrng.init(c.seed +% epoch);
        random.random().shuffle(usize, order);
        for (order, 0..) |index, position| {
            try control.check();
            const ordinal = epoch * order.len + position;
            if (ordinal < resumed) continue;
            if (c.stop_after_microbatches) |stop| if (step >= stop) {
                try trainer.save(checkpoint_path, run, control);
                try writeJson(a, io, try path(p, c.output_dir, "report.json"), .{ .status = "paused", .run_sha256 = std.fmt.bytesToHex(run, .lower), .optimizer = trainer.identity() });
                return;
            };
            const example = caches[0][index];
            var program = try head.Program.init(a, example.prefix, example.layout);
            defer program.deinit();
            _ = try program.step(a, &trainer, example.example, example.objective, c.positive_weight);
            step += 1;
        }
        if ((epoch + 1) % c.checkpoint_every_epochs == 0) try trainer.save(checkpoint_path, run, control);
        const metrics = try evaluate(a, &trainer, caches[1], c.positive_weight);
        const score: f32 = @floatCast(if (has_span_targets) metrics.span_f1 else @as(f64, @floatFromInt(metrics.categorical_correct)) / @as(f64, @floatFromInt(@max(metrics.categorical_tasks, 1))));
        const previous_best = best.best;
        _ = best.observe(score);
        if (best.best > previous_best) {
            try trainer.save(best_path, run, control);
            selected_epoch = epoch + 1;
        }
        std.debug.print("GLiNER span epoch {d}: eval_loss={d:.6}, span_cell_f1={d:.4}\n", .{ epoch + 1, metrics.loss, metrics.span_f1 });
    }
    try trainer.save(checkpoint_path, run, control);
    const completed_optimizer = trainer.identity();
    const last_eval = try evaluate(a, &trainer, caches[1], c.positive_weight);
    try trainer.restore(best_path, run, control);
    const final = try evaluate(a, &trainer, caches[1], c.positive_weight);
    const stage = try path(p, c.output_dir, "model.partial");
    try std.Io.Dir.cwd().createDir(io, stage, .default_dir);
    var tensors: std.ArrayList(checkpoint.NamedTensor) = .empty;
    var entries = source.header.tensors.iterator();
    while (entries.next()) |entry| {
        try control.check();
        const dims = try p.alloc(usize, entry.value_ptr.shape.len);
        for (entry.value_ptr.shape, dims) |dim, *out| out.* = @intCast(dim);
        var value: ?[]const f32 = null;
        for (trainer.owner.regular_params.items) |parameter| if (std.mem.eql(u8, parameter.name, entry.key_ptr.*)) {
            value = parameter.weights;
        };
        if (value == null) {
            var tensor = try source.readTensor(entry.key_ptr.*);
            defer tensor.deinit();
            value = if (tensor.owns_data) try p.dupe(f32, tensor.asFloat32()) else tensor.asFloat32();
        }
        try tensors.append(p, .{ .name = entry.key_ptr.*, .shape = dims, .data = value.? });
    }
    const weights_path = try path(p, stage, "model.safetensors");
    try checkpoint.saveControlled(a, weights_path, tensors.items, control, true);
    for (sidecars, assets) |name, bytes| {
        const destination = try path(p, stage, name);
        if (std.fs.path.dirname(destination)) |parent| try std.Io.Dir.cwd().createDirPath(io, parent);
        try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = destination, .data = bytes });
    }
    var exported = try file.MmapRegion.init(a, weights_path);
    defer exported.deinit();
    const output_digest = qualification.Digest.of(exported.data);
    const source_after_export = qualification.Digest.of(source.file_bytes);
    if (!std.meta.eql(source_after_export, source_digest)) return error.SpanTrainingSourceChanged;
    const report = .{ .family = "modernbert_span_head_training/v1", .status = "complete", .source_sha256 = source_digest.sha256[0..], .weights_sha256 = output_digest.sha256[0..], .weights_bytes = output_digest.size_bytes, .run_sha256 = std.fmt.bytesToHex(run, .lower), .train_sha256 = std.fmt.bytesToHex(digest(train_bytes), .lower), .eval_sha256 = std.fmt.bytesToHex(digest(eval_bytes), .lower), .train_records = train.len, .eval_records = eval.len, .content_disjoint = true, .resume_microbatch_step = resumed, .completed_optimizer = completed_optimizer, .exported_optimizer = trainer.identity(), .selection_metric = if (has_span_targets) "heldout_span_cell_f1" else "heldout_categorical_accuracy", .selected_epoch = selected_epoch, .selection_start_microbatch_step = resumed, .initial_eval = initial, .last_eval = last_eval, .final_eval = final, .encoder_frozen = true, .selected_layers = prefixes, .host_peak_allocator_bytes = budget.peak, .runtime_qualified = false };
    try writeJson(a, io, try path(p, stage, "antfly_gliner_span_training.json"), report);
    try publication.syncDirectory(io, stage);
    try publication.publishDirectory(a, io, stage, try path(p, c.output_dir, "model"));
    try writeJson(a, io, try path(p, c.output_dir, "report.json"), report);
    try publication.syncDirectory(io, c.output_dir);
}

pub fn testDisjoint() !void {
    const a = std.testing.allocator;
    const bytes = "{\"version\":1,\"id\":\"same\",\"text\":\"Ada\",\"schema\":{\"entities\":[\"person\"]}}\n";
    var train = try dataset_mod.Dataset.fromBytes(a, bytes, .{}, null, null);
    defer train.deinit();
    var eval = try dataset_mod.Dataset.fromBytes(a, bytes, .{}, null, null);
    defer eval.deinit();
    try std.testing.expectError(error.BoundaryTrainingSplitOverlap, train.requireDisjoint(&eval, null));
}

pub fn testSoftTargetGold() !void {
    const a = std.testing.allocator;
    var store = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
    defer store.deinitOwned();
    var compute = native.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    const cb = compute.computeBackend();
    const parameters = [_]head.controller.Parameter{
        .{ .name = "classifier.2.weight", .values = &.{ 1, -1 }, .dimensions = &.{ 1, 2 }, .group = 0 },
        .{ .name = "classifier.2.bias", .values = &.{0}, .dimensions = &.{1}, .group = 0 },
    };
    const config = head.controller.Config{ .groups = &.{.{ .schedule = .{ .constant = 0.01 } }} };
    var trainer = try head.controller.Trainer.init(a, &cb, &parameters, config);
    defer trainer.deinit();
    const examples = [_]features.Cached{.{
        .record_id = "human-disagrees-with-teacher",
        .prefix = "classifier.2",
        .layout = .{ .rows = 2, .features = 2, .outputs = 1 },
        .example = .{ .features = &.{ 1, 0, 0, 1 }, .targets = &.{ 0.6, 0.4 } },
        .objective = .binary,
        .gold_index = 1,
    }};
    const metrics = try evaluate(a, &trainer, &examples, 1);
    try std.testing.expectEqual(@as(usize, 1), metrics.categorical_tasks);
    try std.testing.expectEqual(@as(usize, 0), metrics.categorical_correct);
}
