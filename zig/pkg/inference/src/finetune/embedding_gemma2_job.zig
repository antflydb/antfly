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

//! Versioned, fail-closed contracts for native EmbeddingGemma 2 LoRA jobs.
//! File locations and operational ceilings do not define the training math.
const std = @import("std");

pub const format = "antfly.embeddinggemma2.lora-job/v1";
pub const base_revision = "914f7f89142e33e77833254d9c9b90c3cef7303b";
pub const base_weight_sha256 = "197a32965d4b1105faf060417baa899e193fb73cd401f42ec9295234d5553d79";
pub const tokenizer_sha256 = "4d777ef5bdc1aa36227abdfb77c3e49e7b9c892d16e1b6bda41c393504828be4";
pub const dimensions = [_]u32{ 128, 256, 512, 768 };
pub const padding_policy = "per-micro-active-v1";
pub const compute_profile = "bf16-edges-fp32-masters-v1";
// This profile is the eager BF16 autograd graph. Fused SDPA is a separate
// performance comparator with different rounding and saved intermediates.
pub const attention_implementation = "eager";
pub const Backend = enum { cuda };
pub const TargetPolicy = enum { encoder_and_embedding_projection };

pub const Lora = struct {
    rank: u32 = 16,
    alpha: f32 = 32,
    dropout: f32 = 0,
    targets: TargetPolicy = .encoder_and_embedding_projection,
};
pub const Optimizer = struct {
    learning_rate: f64 = 1e-4,
    beta1: f64 = 0.9,
    beta2: f64 = 0.999,
    epsilon: f64 = 1e-8,
    weight_decay: f64 = 0.01,
    max_grad_norm: f64 = 1,
    warmup_fraction: f64 = 0.05,
};
pub const Config = struct {
    version: u32 = 1,
    backend: Backend = .cuda,
    model_dir: []const u8,
    prepared_dir: []const u8,
    output_dir: []const u8,
    device_id: u32 = 0,
    lora: Lora = .{},
    optimizer: Optimizer = .{},
    logical_batch_size: u32 = 64,
    microbatch_size: u32 = 2,
    query_max_tokens: u32 = 128,
    document_max_tokens: u32 = 512,
    max_negatives: u32 = 4,
    temperature: f32 = 0.05,
    epochs: u32 = 1,
    seed: u64 = 42,
    checkpoint_every_steps: u32 = 50,
    timeout_seconds: u32 = 86400,
    max_device_bytes: u64 = 20 * 1024 * 1024 * 1024,
    max_host_bytes: u64 = 8 * 1024 * 1024 * 1024,
};

pub fn validate(c: Config) !void {
    if (c.version != 1) return error.UnsupportedTrainingJobVersion;
    for ([_][]const u8{ c.model_dir, c.prepared_dir, c.output_dir }) |p| {
        if (!std.fs.path.isAbsolute(p) or std.mem.indexOfScalar(u8, p, 0) != null)
            return error.TrainingJobRequiresAbsolutePaths;
    }
    if (std.mem.eql(u8, c.model_dir, c.output_dir) or std.mem.eql(u8, c.prepared_dir, c.output_dir))
        return error.TrainingOutputAliasesInput;
    // Only these bounded adapter configurations have an admitted kernel shape.
    if (c.lora.rank != 8 and c.lora.rank != 16 and c.lora.rank != 32) return error.UnsupportedLoraRank;
    if (!std.math.isFinite(c.lora.alpha) or c.lora.alpha <= 0 or c.lora.alpha > 1024)
        return error.InvalidLoraAlpha;
    if (c.lora.dropout != 0) return error.LoraDropoutNotSupported;
    if (c.logical_batch_size == 0 or c.logical_batch_size > 64 or
        c.microbatch_size == 0 or c.microbatch_size > 2 or
        c.microbatch_size > c.logical_batch_size or c.max_negatives > 4)
        return error.InvalidContrastiveBatch;
    if (c.query_max_tokens < 4 or c.query_max_tokens > 512 or
        c.document_max_tokens < 4 or c.document_max_tokens > 512)
        return error.TrainingSequenceTooLong;
    if (!std.math.isFinite(c.temperature) or c.temperature <= 0 or c.temperature > 1 or
        !std.math.isFinite(@as(f32, 1) / c.temperature))
        return error.InvalidContrastiveTemperature;
    if (c.epochs == 0 or c.epochs > 10000 or c.checkpoint_every_steps == 0 or c.device_id > 1024 or
        c.timeout_seconds == 0 or c.timeout_seconds > 7 * 86400)
        return error.InvalidTrainingJob;
    if (c.max_device_bytes < 1024 * 1024 * 1024 or c.max_device_bytes > 256 * 1024 * 1024 * 1024 or
        c.max_host_bytes < 64 * 1024 * 1024 or c.max_host_bytes > 128 * 1024 * 1024 * 1024)
        return error.InvalidTrainingMemoryLimit;
    const o = c.optimizer;
    for ([_]f64{ o.learning_rate, o.epsilon, o.max_grad_norm }) |v|
        if (!std.math.isFinite(v) or v <= 0) return error.InvalidTrainingOptimizer;
    for ([_]f64{ o.beta1, o.beta2 }) |v|
        if (!std.math.isFinite(v) or v < 0 or v >= 1) return error.InvalidTrainingOptimizer;
    if (!std.math.isFinite(o.weight_decay) or o.weight_decay < 0 or
        !std.math.isFinite(o.warmup_fraction) or o.warmup_fraction < 0 or o.warmup_fraction >= 1)
        return error.InvalidTrainingOptimizer;
}

pub fn validatePathSeparation(a: std.mem.Allocator, io: std.Io, c: Config) !void {
    return rejectPathOverlap(a, io, c.output_dir, &.{ c.model_dir, c.prepared_dir });
}

/// Reject an output that aliases, contains, or is contained by any immutable
/// input after resolving existing symlinks and lexical `.`/`..` components.
pub fn rejectPathOverlap(a: std.mem.Allocator, io: std.Io, output: []const u8, inputs: []const []const u8) !void {
    const output_path = try canonicalProspectivePath(a, io, output);
    defer a.free(output_path);
    for (inputs) |input| {
        const input_path = try std.Io.Dir.cwd().realPathFileAlloc(io, input, a);
        defer a.free(input_path);
        if (pathContains(input_path, output_path) or pathContains(output_path, input_path))
            return error.TrainingOutputAliasesInput;
    }
}

fn canonicalProspectivePath(a: std.mem.Allocator, io: std.Io, path: []const u8) ![]u8 {
    const absolute = if (std.fs.path.isAbsolute(path))
        try a.dupe(u8, path)
    else blk: {
        const cwd = try std.Io.Dir.cwd().realPathFileAlloc(io, ".", a);
        defer a.free(cwd);
        break :blk try std.fs.path.join(a, &.{ cwd, path });
    };
    defer a.free(absolute);
    var probe = try a.dupe(u8, absolute);
    defer a.free(probe);
    while (true) {
        const canonical = std.Io.Dir.cwd().realPathFileAlloc(io, probe, a) catch |err| switch (err) {
            error.FileNotFound => {
                const parent = std.fs.path.dirname(probe) orelse return err;
                if (parent.len == probe.len) return err;
                const replacement = try a.dupe(u8, parent);
                a.free(probe);
                probe = replacement;
                continue;
            },
            else => return err,
        };
        defer a.free(canonical);
        var suffix = absolute[probe.len..];
        while (suffix.len > 0 and std.fs.path.isSep(suffix[0])) suffix = suffix[1..];
        return if (suffix.len == 0) a.dupe(u8, canonical) else std.fs.path.resolve(a, &.{ canonical, suffix });
    }
}

fn pathContains(root: []const u8, candidate: []const u8) bool {
    if (std.mem.eql(u8, root, candidate)) return true;
    if (!std.mem.startsWith(u8, candidate, root) or candidate.len <= root.len) return false;
    return root.len == 1 and std.fs.path.isSep(root[0]) or std.fs.path.isSep(candidate[root.len]);
}

pub const Identity = struct {
    base_sha256: [32]u8,
    tokenizer_sha256: [32]u8,
    prepared_sha256: [32]u8,
    /// Includes the rendered prompts and preparation/truncation policy.
    preprocessing_sha256: [32]u8,
};

/// Canonical field order and byte encoding are part of the v1 resume contract.
pub fn fingerprint(c: Config, identity: Identity) ![32]u8 {
    try validate(c);
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update(format ++ "\x00");
    for ([_][32]u8{ identity.base_sha256, identity.tokenizer_sha256, identity.prepared_sha256, identity.preprocessing_sha256 }) |digest| hash.update(&digest);
    const semantic = .{
        .backend = c.backend,
        .lora = c.lora,
        .optimizer = c.optimizer,
        .logical_batch_size = c.logical_batch_size,
        .microbatch_size = c.microbatch_size,
        .query_max_tokens = c.query_max_tokens,
        .document_max_tokens = c.document_max_tokens,
        .max_negatives = c.max_negatives,
        .temperature = c.temperature,
        .epochs = c.epochs,
        .seed = c.seed,
        .dimensions = dimensions,
        .checkpoint_layers = @as(u32, 2),
        .compute_profile = compute_profile,
        .padding_policy = padding_policy,
        .sampler = "fisher-yates-splitmix64-v1",
    };
    var buffer: [4096]u8 = undefined;
    var writer = std.Io.Writer.fixed(&buffer);
    try std.json.Stringify.value(semantic, .{}, &writer);
    hash.update(writer.buffered());
    return hash.finalResult();
}

pub const Estimate = struct {
    adapter_parameters: u64,
    logical_candidates: u64,
    device_upper_bound_bytes: u64,
    host_upper_bound_bytes: u64,
    /// Static estimate; actual allocations must independently enforce ceilings.
    measured: bool = false,
};

pub fn estimate(c: Config, records: u64) !Estimate {
    try validate(c);
    if (records == 0 or records > 100_000_000) return error.InvalidTrainingRecordCount;
    const adapter_parameters = @as(u64, c.lora.rank) * 316_672;
    const candidates = @as(u64, c.logical_batch_size) * (2 + @as(u64, c.max_negatives));
    const tokens = @as(u64, c.microbatch_size) * @max(c.query_max_tokens, c.document_max_tokens);
    const attention = @as(u64, c.microbatch_size) * 4 * @as(u64, c.document_max_tokens) * c.document_max_tokens * 4 * 6;
    // Ten FP32 adapter payloads covers masters, gradients, moments, and private
    // update replacements/mirrors. Two live blocks plus checkpoint boundaries.
    const device = 271_002_624 * 2 + adapter_parameters * 4 * 10 +
        tokens * (512 * 12 + 2048 * 8) * 4 * 2 + tokens * 512 * 2 * 12 +
        attention + candidates * 768 * 4 * 8 +
        @as(u64, c.logical_batch_size) * candidates * 4 * 6 + 1536 * 1024 * 1024;
    const host = records * 72 + adapter_parameters * 4 * 4 + 2 * 1024 * 1024 * 1024;
    if (device > c.max_device_bytes) return error.TrainingDeviceBudgetExceeded;
    if (host > c.max_host_bytes) return error.TrainingHostBudgetExceeded;
    return .{ .adapter_parameters = adapter_parameters, .logical_candidates = candidates, .device_upper_bound_bytes = device, .host_upper_bound_bytes = host };
}

/// Independent counter RNG avoids std.Random version changes on resume.
pub fn splitmix64(state: *u64) u64 {
    state.* +%= 0x9e3779b97f4a7c15;
    var z = state.*;
    z = (z ^ (z >> 30)) *% 0xbf58476d1ce4e5b9;
    z = (z ^ (z >> 27)) *% 0x94d049bb133111eb;
    return z ^ (z >> 31);
}

pub fn shuffleEpoch(indices: []u64, seed: u64, epoch: u64) void {
    for (indices, 0..) |*index, i| index.* = i;
    var state = seed ^ (epoch *% 0xd1342543de82ef95);
    var remaining = indices.len;
    while (remaining > 1) {
        // Rejection sampling keeps the bounded draw unbiased.
        const bound: u64 = remaining;
        const threshold = (0 -% bound) % bound;
        var random = splitmix64(&state);
        while (random < threshold) random = splitmix64(&state);
        const chosen: usize = @intCast(random % bound);
        remaining -= 1;
        std.mem.swap(u64, &indices[remaining], &indices[chosen]);
    }
}

pub fn learningRate(c: Config, completed_steps: u64, total_steps: u64) !f64 {
    try validate(c);
    if (total_steps == 0 or completed_steps >= total_steps) return error.InvalidOptimizerStep;
    const warmup: u64 = @intFromFloat(@floor(@as(f64, @floatFromInt(total_steps)) * c.optimizer.warmup_fraction));
    const step = completed_steps + 1;
    if (warmup > 0 and step <= warmup)
        return c.optimizer.learning_rate * @as(f64, @floatFromInt(step)) / @as(f64, @floatFromInt(warmup));
    if (total_steps == warmup) return c.optimizer.learning_rate;
    // The first post-warmup update uses the peak LR, matching LambdaLR's
    // completed-step convention. The final update still has a nonzero LR.
    return c.optimizer.learning_rate * @as(f64, @floatFromInt(total_steps - completed_steps)) /
        @as(f64, @floatFromInt(total_steps - warmup));
}

fn testConfig() Config {
    return .{ .model_dir = "/models/base", .prepared_dir = "/data/prepared", .output_dir = "/runs/one" };
}

test "EG2 training rejects unsupported math and memory admission" {
    var c = testConfig();
    try validate(c);
    try std.testing.expectEqual(@as(u64, 5_066_752), (try estimate(c, 100_000)).adapter_parameters);
    c.document_max_tokens = 513;
    try std.testing.expectError(error.TrainingSequenceTooLong, validate(c));
    c = testConfig();
    c.lora.dropout = 0.1;
    try std.testing.expectError(error.LoraDropoutNotSupported, validate(c));
    c = testConfig();
    c.temperature = std.math.nan(f32);
    try std.testing.expectError(error.InvalidContrastiveTemperature, validate(c));
    c.temperature = 1e-40;
    try std.testing.expectError(error.InvalidContrastiveTemperature, validate(c));
    c = testConfig();
    c.max_device_bytes = 1024 * 1024 * 1024;
    try std.testing.expectError(error.TrainingDeviceBudgetExceeded, estimate(c, 100_000));
}

test "EG2 resume fingerprint allows relocation but locks training semantics" {
    const identity = Identity{ .base_sha256 = @splat(1), .tokenizer_sha256 = @splat(2), .prepared_sha256 = @splat(3), .preprocessing_sha256 = @splat(4) };
    var c = testConfig();
    const original = try fingerprint(c, identity);
    c.output_dir = "/runs/resumed";
    c.model_dir = "/another/base";
    c.checkpoint_every_steps = 1;
    c.max_device_bytes += 1024;
    try std.testing.expectEqualSlices(u8, &original, &(try fingerprint(c, identity)));
    c.seed += 1;
    try std.testing.expect(!std.mem.eql(u8, &original, &(try fingerprint(c, identity))));
}

test "EG2 training rejects nested and symlinked output aliases" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.createDirPath(io, "model");
    try tmp.dir.createDirPath(io, "model/subdir");
    try tmp.dir.createDirPath(io, "prepared");
    try tmp.dir.symLink(io, "model", "model-link", .{});
    try tmp.dir.symLink(io, "model/subdir", "subdir-link", .{});
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    const model = try std.fs.path.join(a, &.{ root, "model" });
    defer a.free(model);
    const model_link = try std.fs.path.join(a, &.{ root, "model-link" });
    defer a.free(model_link);
    const prepared = try std.fs.path.join(a, &.{ root, "prepared" });
    defer a.free(prepared);
    const nested = try std.fs.path.join(a, &.{ model, "runs", "one" });
    defer a.free(nested);
    const sibling = try std.fs.path.join(a, &.{ root, "runs", "one" });
    defer a.free(sibling);
    const symlink_parent = try std.fs.path.join(a, &.{ root, "subdir-link", "..", "run" });
    defer a.free(symlink_parent);

    var config = Config{ .model_dir = model, .prepared_dir = prepared, .output_dir = nested };
    try std.testing.expectError(error.TrainingOutputAliasesInput, validatePathSeparation(a, io, config));
    config.model_dir = model_link;
    try std.testing.expectError(error.TrainingOutputAliasesInput, validatePathSeparation(a, io, config));
    config.model_dir = model;
    config.output_dir = symlink_parent;
    try std.testing.expectError(error.TrainingOutputAliasesInput, validatePathSeparation(a, io, config));
    config.output_dir = sibling;
    try validatePathSeparation(a, io, config);
}

test "EG2 epoch shuffle is reproducible and a complete permutation" {
    var first: [257]u64 = undefined;
    var replay: [257]u64 = undefined;
    shuffleEpoch(&first, 42, 3);
    shuffleEpoch(&replay, 42, 3);
    try std.testing.expectEqualSlices(u64, &first, &replay);
    var seen: [257]bool = @splat(false);
    for (first) |index| {
        try std.testing.expect(!seen[@intCast(index)]);
        seen[@intCast(index)] = true;
    }
    shuffleEpoch(&replay, 42, 4);
    try std.testing.expect(!std.mem.eql(u64, &first, &replay));
}

test "EG2 schedule uses completed optimizer steps" {
    var c = testConfig();
    c.optimizer.warmup_fraction = 0.1;
    try std.testing.expectApproxEqAbs(@as(f64, 1e-5), try learningRate(c, 0, 100), 1e-12);
    try std.testing.expectApproxEqAbs(@as(f64, 1e-4), try learningRate(c, 9, 100), 1e-12);
    try std.testing.expectApproxEqAbs(@as(f64, 1e-4), try learningRate(c, 10, 100), 1e-12);
    try std.testing.expectApproxEqAbs(@as(f64, 1e-4 / 90.0), try learningRate(c, 99, 100), 1e-12);
}
