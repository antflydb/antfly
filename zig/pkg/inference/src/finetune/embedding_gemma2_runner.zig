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

//! Dedicated resident CUDA training. Only token inputs, scalar progress, and
//! explicit checkpoint/qualification snapshots cross the host boundary.
const std = @import("std");
const job = @import("embedding_gemma2_job.zig");
const data = @import("embedding_gemma2_data.zig");
const batch_mod = @import("embedding_gemma2_batch.zig");
const training = @import("embedding_gemma2_training.zig");
const model = @import("../models/embedding_gemma2.zig");
const catalog = @import("../registry/embedding_gemma2_catalog.zig");
const safetensors = @import("../models/safetensors.zig");
const cuda = @import("../ops/cuda/cuda_compute.zig");
const session = @import("embedding_gemma2_cuda_session.zig");
const encoder_mod = @import("embedding_gemma2_cuda_encoder.zig");
const objective_mod = @import("embedding_gemma2_cuda_objective.zig");
const checkpoint = @import("embedding_gemma2_checkpoint.zig");
const bounded = @import("../runtime/bounded_allocator.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const clock = @import("antfly_platform").time;

pub const shuffle_identity = "fisher-yates-splitmix64-v1";
pub const Options = struct {
    resume_from: ?[]const u8 = null,
    stop_after_steps: ?u64 = null,
    dry_run: bool = false,
    pause_context: ?*anyopaque = null,
    pause_requested: ?*const fn (?*anyopaque) bool = null,
};
pub const Result = struct {
    optimizer_steps: u64,
    total_steps: u64,
    paused: bool,
    tracked_peak_device_bytes: usize,
    tracked_peak_host_bytes: usize,
};

pub fn readFile(a: std.mem.Allocator, io: std.Io, path: []const u8, maximum: usize) ![]u8 {
    const f = try std.Io.Dir.cwd().openFile(io, path, .{});
    defer f.close(io);
    const length = try f.length(io);
    if (length == 0 or length > maximum) return error.InvalidTrainingFileSize;
    const bytes = try a.alloc(u8, @intCast(length));
    errdefer a.free(bytes);
    if (try f.readPositionalAll(io, bytes, 0) != bytes.len) return error.UnexpectedEndOfFile;
    return bytes;
}

pub fn loadConfig(a: std.mem.Allocator, io: std.Io, path: []const u8) !std.json.Parsed(job.Config) {
    const bytes = try readFile(a, io, path, 64 * 1024);
    defer a.free(bytes);
    const parsed = try std.json.parseFromSlice(job.Config, a, bytes, .{ .allocate = .alloc_always });
    errdefer parsed.deinit();
    try job.validate(parsed.value);
    // CUDA context initialization currently selects device zero. Refuse an
    // unsupported selection instead of silently training on another GPU.
    if (parsed.value.device_id != 0) return error.UnsupportedTrainingDeviceSelection;
    return parsed;
}

pub fn loadModelConfig(a: std.mem.Allocator, io: std.Io, model_dir: []const u8) !model.Config {
    const path = try std.fs.path.join(a, &.{ model_dir, "config.json" });
    defer a.free(path);
    const bytes = try readFile(a, io, path, 1024 * 1024);
    defer a.free(bytes);
    // Geometry alone does not identify the execution graph: epsilon, RoPE,
    // sliding-window policy, and the projection contract also come from this
    // immutable sidecar. Verify the bytes actually being parsed.
    const pin = comptime blk: {
        for (catalog.bundles[0].artifact_list) |artifact| {
            if (std.mem.eql(u8, artifact.path, "config.json")) break :blk artifact;
        }
        @compileError("EmbeddingGemma 2 catalog is missing config.json");
    };
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
    if (bytes.len != pin.size or !std.mem.eql(u8, &digest, &(try data.decodeDigest(pin.sha256))))
        return error.TrainingBaseIdentityMismatch;
    const config = try model.parseConfig(a, bytes);
    if (!config.isOfficialTextTopology()) return error.UnsupportedEmbeddingGemma2TrainingTopology;
    return config;
}

pub fn verifyBase(a: std.mem.Allocator, io: std.Io, model_dir: []const u8) !void {
    inline for (.{ .{ "model.safetensors", job.base_weight_sha256, @as(u64, 2 * 1024 * 1024 * 1024) }, .{ "tokenizer.json", job.tokenizer_sha256, @as(u64, 64 * 1024 * 1024) } }) |pin| {
        const path = try std.fs.path.join(a, &.{ model_dir, pin[0] });
        defer a.free(path);
        const f = try std.Io.Dir.cwd().openFile(io, path, .{});
        defer f.close(io);
        if (!std.mem.eql(u8, &(try data.hashFile(io, f, pin[2])), &(try data.decodeDigest(pin[1]))))
            return error.TrainingBaseIdentityMismatch;
    }
}

pub fn loadTextWeights(a: std.mem.Allocator, compute: *cuda.CudaCompute, model_dir: []const u8) !void {
    const path = try std.fs.path.join(a, &.{ model_dir, "model.safetensors" });
    defer a.free(path);
    var reader = try safetensors.MMapReader.openFileAbsoluteLimited(a, path, 2 * 1024 * 1024 * 1024, 4 * 1024 * 1024);
    defer reader.deinit();
    const names = try reader.header.tensorNames(a);
    defer a.free(names);
    for (names) |name| {
        if (!std.mem.startsWith(u8, name, "language_model.")) continue;
        var tensor = try reader.readTensor(name);
        defer tensor.deinit();
        if (tensor.dtype != .bf16) return error.TrainingRequiresBf16Base;
        const key = try a.dupe(u8, name);
        // Backend consumes the key, including its error cleanup.
        try compute.insertBf16WeightFromTensor(key, &tensor);
    }
    try compute.synchronizeEmbeddingGemma2Training();
}

pub fn planFor(c: job.Config) training.TrainingPlan {
    return .{ .rank = c.lora.rank, .alpha = c.lora.alpha, .dropout = 0, .max_sequence = @max(c.query_max_tokens, c.document_max_tokens), .mrl_dimensions = &job.dimensions, .checkpoint_every_n_layers = 2 };
}

pub fn run(backing: std.mem.Allocator, io: std.Io, c: job.Config, options: Options, control: ?Control) !Result {
    try job.validate(c);
    try job.validatePathSeparation(backing, io, c);
    if (c.device_id != 0) return error.UnsupportedTrainingDeviceSelection;
    // Reserve mmap residency and library/initialization metadata outside the
    // reclaiming allocator; all job-owned allocations share the remainder.
    const external_host_reserve = 2 * 1024 * 1024 * 1024;
    if (c.max_host_bytes <= external_host_reserve) return error.TrainingHostBudgetExceeded;
    var budget = bounded.BoundedAllocator{ .backing = backing, .limit = @intCast(c.max_host_bytes - external_host_reserve) };
    const a = budget.allocator();
    if (control) |value| try value.check();
    var dataset = try data.Dataset.open(a, io, c.prepared_dir);
    defer dataset.deinit();
    const manifest = dataset.parsed.value;
    if (manifest.train_records == 0 or manifest.query_max_tokens != c.query_max_tokens or
        manifest.document_max_tokens != c.document_max_tokens or manifest.max_negatives != c.max_negatives)
        return error.TrainingDatasetPolicyMismatch;
    const estimate = try job.estimate(c, manifest.records);
    try verifyBase(a, io, c.model_dir);
    const config = try loadModelConfig(a, io, c.model_dir);
    const train_plan = planFor(c);
    try train_plan.validate(config);
    const identity = job.Identity{ .base_sha256 = try data.decodeDigest(job.base_weight_sha256), .tokenizer_sha256 = try data.decodeDigest(job.tokenizer_sha256), .prepared_sha256 = dataset.manifest_sha256, .preprocessing_sha256 = try data.decodeDigest(manifest.preprocessing_sha256) };
    const fingerprint = std.fmt.bytesToHex(try job.fingerprint(c, identity), .lower);
    const dataset_digest = std.fmt.bytesToHex(dataset.manifest_sha256, .lower);
    const steps_per_epoch = try std.math.divCeil(u64, manifest.train_records, c.logical_batch_size);
    const total_steps = try std.math.mul(u64, steps_per_epoch, c.epochs);
    if (options.dry_run) {
        try event(io, .{ .event = "admission", .estimate = estimate, .job_fingerprint = &fingerprint, .total_steps = total_steps });
        return .{ .optimizer_steps = 0, .total_steps = total_steps, .paused = false, .tracked_peak_device_bytes = 0, .tracked_peak_host_bytes = budget.peak + external_host_reserve };
    }
    try event(io, .{ .event = "identity", .job_fingerprint = &fingerprint, .base_weight_sha256 = job.base_weight_sha256, .prepared_manifest_sha256 = &dataset_digest, .total_steps = total_steps, .precision = "bf16", .attention = job.attention_implementation, .compute_profile = job.compute_profile, .padding_policy = job.padding_policy, .rank = c.lora.rank, .alpha = c.lora.alpha, .seed = c.seed, .logical_batch_size = c.logical_batch_size, .microbatch_size = c.microbatch_size, .query_max_tokens = c.query_max_tokens, .document_max_tokens = c.document_max_tokens, .max_negatives = c.max_negatives });
    if (std.Io.Dir.cwd().access(io, c.output_dir, .{})) |_| return error.TrainingOutputAlreadyExists else |err| switch (err) {
        error.FileNotFound => {},
        else => return err,
    }
    if (std.fs.path.dirname(c.output_dir)) |parent| try std.Io.Dir.cwd().createDirPath(io, parent);
    try std.Io.Dir.cwd().createDir(io, c.output_dir, .default_dir);
    // An exclusive directory is retained on failure for diagnostics; durable
    // checkpoints always publish into unique completed-step subdirectories.
    var compute = try cuda.CudaCompute.initWithDeviceMemoryLimit(a, @intCast(c.max_device_bytes - 512 * 1024 * 1024));
    defer compute.deinit();
    compute.capability_profile = .embedding_gemma2;
    compute.resident_training_cache = true;
    try loadTextWeights(a, &compute, c.model_dir);
    const limits = @import("embedding_gemma2_cuda.zig").Limits{ .max_device_bytes = @intCast(c.max_device_bytes), .max_host_bytes = @intCast(c.max_host_bytes), .resident_base_bytes = compute.ctx.device_allocations.snapshot().live };
    var adapters = try session.ResidentAdapterState.init(a, &compute, config, train_plan, c.microbatch_size, @max(c.query_max_tokens, c.document_max_tokens), limits, c.seed);
    defer adapters.deinit();
    var encoder = try encoder_mod.Encoder.init(a, &compute, config, train_plan, &adapters, limits);
    defer encoder.deinit();
    const train_indices = try a.alloc(u64, @intCast(manifest.train_records));
    defer a.free(train_indices);
    var found: usize = 0;
    for (0..@as(usize, @intCast(manifest.records))) |index| if (try dataset.split(index) == .train) {
        if (found == train_indices.len) return error.PreparedSplitCountMismatch;
        train_indices[found] = index;
        found += 1;
    };
    if (found != train_indices.len) return error.PreparedSplitCountMismatch;
    const order = try a.alloc(u64, train_indices.len);
    defer a.free(order);
    var epoch: u64 = 0;
    var cursor: usize = 0;
    if (options.resume_from) |resume_path| {
        var specs_arena = std.heap.ArenaAllocator.init(a);
        defer specs_arena.deinit();
        const specs = try tensorSpecs(specs_arena.allocator(), adapters.layout);
        var loaded = try checkpoint.loadValidated(a, resume_path, .{ .job_fingerprint = &fingerprint, .base_sha256 = job.base_weight_sha256, .dataset_sha256 = &dataset_digest, .tokenizer_sha256 = job.tokenizer_sha256, .preprocessing_sha256 = manifest.preprocessing_sha256, .seed = c.seed, .shuffle_identity = shuffle_identity, .tensors = specs }, c.max_host_bytes - external_host_reserve, control);
        defer loaded.deinit();
        if (loaded.state.total_steps != total_steps or loaded.state.epoch > c.epochs or
            loaded.state.generation != loaded.state.optimizer_step or loaded.state.cursor >= train_indices.len or
            (loaded.state.epoch == c.epochs and loaded.state.cursor != 0) or loaded.state.cursor % c.logical_batch_size != 0 or
            loaded.state.optimizer_step != loaded.state.epoch * steps_per_epoch +
                (try std.math.divCeil(u64, loaded.state.cursor, c.logical_batch_size))) return error.InvalidTrainingResumeCursor;
        try adapters.restore(&loaded);
        epoch = loaded.state.epoch;
        cursor = @intCast(loaded.state.cursor);
    }
    var invocation_steps: u64 = 0;
    var paused = false;
    var last_saved: ?u64 = null;
    while (epoch < c.epochs) {
        job.shuffleEpoch(order, c.seed, epoch);
        while (cursor < order.len) {
            if (control) |value| try value.check();
            if (options.pause_requested) |requested| if (requested(options.pause_context)) {
                paused = true;
                break;
            };
            if (options.stop_after_steps) |stop| if (invocation_steps >= stop) {
                paused = true;
                break;
            };
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const ba = arena.allocator();
            const next = @min(order.len, cursor + c.logical_batch_size);
            const records = try ba.alloc(data.Record, next - cursor);
            for (records, order[cursor..next]) |*record, position| {
                const parsed = try dataset.read(ba, train_indices[@intCast(position)]);
                record.* = parsed.value; // arena owns parsed backing until commit
            }
            var batch = try batch_mod.prepare(ba, records, c.max_negatives);
            defer batch.deinit();
            const candidates = try ba.alloc(batch_mod.Sequence, batch.candidates.len);
            for (candidates, batch.candidates) |*sequence, candidate| sequence.* = .{ .ids = candidate.document.input_ids, .task = candidate.task };
            var objective = try objective_mod.ResidentObjective.init(&compute, @intCast(batch.queries.len), @intCast(candidates.len), c.temperature, batch.validity, batch.positive_indices);
            defer objective.deinit();
            try compute.synchronizeEmbeddingGemma2Training();
            const started = clock.monotonicNs();
            try adapters.beginLogicalBatch(adapters.optimizer_step, adapters.microbatch_step);
            errdefer adapters.abortLogicalBatch();
            try capture(ba, &encoder, &objective, batch.queries, c.query_max_tokens, c.microbatch_size, true, control);
            try capture(ba, &encoder, &objective, candidates, c.document_max_tokens, c.microbatch_size, false, control);
            const loss = try objective.evaluate();
            try replay(ba, &encoder, &objective, batch.queries, c.query_max_tokens, c.microbatch_size, true, control);
            try replay(ba, &encoder, &objective, candidates, c.document_max_tokens, c.microbatch_size, false, control);
            const lr = try job.learningRate(c, adapters.optimizer_step, total_steps);
            _ = try adapters.commitLogicalBatch(.{ .learning_rate = lr, .beta1 = c.optimizer.beta1, .beta2 = c.optimizer.beta2, .epsilon = c.optimizer.epsilon, .weight_decay = c.optimizer.weight_decay, .max_grad_norm = c.optimizer.max_grad_norm }, loss, control);
            try compute.synchronizeEmbeddingGemma2Training();
            const elapsed_ms = @as(f64, @floatFromInt(clock.monotonicNs() - started)) / 1e6;
            cursor = next;
            invocation_steps += 1;
            if (cursor == order.len) {
                epoch += 1;
                cursor = 0;
            }
            try event(io, .{ .event = "step", .step = adapters.optimizer_step - 1, .loss = loss, .learning_rate = lr, .step_ms = elapsed_ms, .queries = batch.queries.len, .candidates = candidates.len, .tracked_device_bytes = compute.ctx.device_allocations.snapshot().live });
            if (adapters.optimizer_step % c.checkpoint_every_steps == 0) {
                try save(a, c, &adapters, epoch, cursor, total_steps, &fingerprint, &dataset_digest, manifest.preprocessing_sha256, control);
                last_saved = adapters.optimizer_step;
            }
            if (cursor == 0) break;
        }
        if (paused) break;
    }
    if (last_saved == null or last_saved.? != adapters.optimizer_step)
        try save(a, c, &adapters, epoch, cursor, total_steps, &fingerprint, &dataset_digest, manifest.preprocessing_sha256, control);
    const result = Result{ .optimizer_steps = adapters.optimizer_step, .total_steps = total_steps, .paused = paused, .tracked_peak_device_bytes = compute.ctx.device_allocations.snapshot().peak, .tracked_peak_host_bytes = budget.peak + external_host_reserve };
    const report_path = try std.fs.path.join(a, &.{ c.output_dir, "result.json" });
    defer a.free(report_path);
    const report = try std.Io.Dir.cwd().createFile(io, report_path, .{ .exclusive = true });
    defer report.close(io);
    var buf: [8192]u8 = undefined;
    var writer = report.writerStreaming(io, &buf);
    try std.json.Stringify.value(result, .{ .whitespace = .indent_2 }, &writer.interface);
    try writer.interface.writeByte('\n');
    try writer.interface.flush();
    try report.sync(io);
    return result;
}

fn input(a: std.mem.Allocator, sequences: []const batch_mod.Sequence, maximum: u32) !encoder_mod.PaddedInput {
    var padded = try batch_mod.pad(a, sequences, maximum);
    defer padded.deinit();
    const ids = try a.alloc(i64, padded.ids.len);
    const mask = try a.alloc(i64, padded.mask.len);
    for (ids, padded.ids) |*dst, src| dst.* = src;
    for (mask, padded.mask) |*dst, src| dst.* = src;
    return .{ .token_ids = ids, .attention_mask = mask, .batch = padded.batch, .sequence = padded.sequence };
}

fn capture(a: std.mem.Allocator, encoder: *encoder_mod.Encoder, objective: *objective_mod.ResidentObjective, sequences: []const batch_mod.Sequence, maximum: u32, micro: u32, queries: bool, control: ?Control) !void {
    var start: usize = 0;
    while (start < sequences.len) {
        const end = @min(sequences.len, start + micro);
        const padded = try input(a, sequences[start..end], maximum);
        defer a.free(padded.token_ids);
        defer a.free(padded.attention_mask);
        var output = try encoder.encode(padded, control);
        defer encoder.releaseOutput(&output);
        if (queries) try objective.captureQueries(@intCast(start), @intCast(end - start), output.buffer) else try objective.captureCandidates(@intCast(start), @intCast(end - start), output.buffer);
        start = end;
    }
}

fn replay(a: std.mem.Allocator, encoder: *encoder_mod.Encoder, objective: *objective_mod.ResidentObjective, sequences: []const batch_mod.Sequence, maximum: u32, micro: u32, queries: bool, control: ?Control) !void {
    var start: usize = 0;
    while (start < sequences.len) {
        const end = @min(sequences.len, start + micro);
        const padded = try input(a, sequences[start..end], maximum);
        defer a.free(padded.token_ids);
        defer a.free(padded.attention_mask);
        const gradient = if (queries) try objective.queryGradient(@intCast(start), @intCast(end - start)) else try objective.candidateGradient(@intCast(start), @intCast(end - start));
        try encoder.replayBackward(padded, gradient, control);
        start = end;
    }
}

pub fn tensorSpecs(a: std.mem.Allocator, layout: @import("embedding_gemma2_adapter_state.zig").Layout) ![]checkpoint.TensorSpec {
    const specs = try a.alloc(checkpoint.TensorSpec, 169 * 6);
    var index: usize = 0;
    var name_buffer: [256]u8 = undefined;
    for (layout.descriptors) |descriptor| {
        const base = try descriptor.writeBaseName(&name_buffer);
        const stem = base[0 .. base.len - ".weight".len];
        inline for (.{ .{ "lora_A.weight", [_]usize{ descriptor.rank, descriptor.input_dim } }, .{ "lora_B.weight", [_]usize{ descriptor.output_dim, descriptor.rank } } }) |parameter| {
            inline for (.{ "", ".exp_avg", ".exp_avg_sq" }) |suffix| {
                specs[index] = .{ .name = try std.fmt.allocPrint(a, "{s}.{s}{s}", .{ stem, parameter[0], suffix }), .shape = try a.dupe(usize, &parameter[1]) };
                index += 1;
            }
        }
    }
    return specs;
}

fn save(a: std.mem.Allocator, c: job.Config, adapters: *session.ResidentAdapterState, epoch: u64, cursor: usize, total: u64, fingerprint: []const u8, dataset_digest: []const u8, preprocessing_digest: []const u8, control: ?Control) !void {
    const path = try std.fmt.allocPrint(a, "{s}/checkpoint-{d:0>8}", .{ c.output_dir, adapters.optimizer_step });
    defer a.free(path);
    var snapshot = try adapters.snapshot(a, c.max_host_bytes - 2 * 1024 * 1024 * 1024);
    defer snapshot.deinit();
    try checkpoint.saveAtomic(a, path, .{ .optimizer_step = adapters.optimizer_step, .generation = adapters.generation, .epoch = epoch, .cursor = cursor, .total_steps = total, .job_fingerprint = fingerprint, .base_sha256 = job.base_weight_sha256, .dataset_sha256 = dataset_digest, .tokenizer_sha256 = job.tokenizer_sha256, .preprocessing_sha256 = preprocessing_digest, .seed = c.seed, .shuffle_identity = shuffle_identity }, snapshot.tensors, c.max_host_bytes - 2 * 1024 * 1024 * 1024, control);
}

pub fn event(io: std.Io, value: anytype) !void {
    var buffer: [8192]u8 = undefined;
    var writer = std.Io.File.stdout().writerStreaming(io, &buffer);
    try std.json.Stringify.value(value, .{}, &writer.interface);
    try writer.interface.writeByte('\n');
    try writer.interface.flush();
}
