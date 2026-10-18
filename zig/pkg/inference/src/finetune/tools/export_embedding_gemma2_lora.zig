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

//! Materialize a validated native EG2 training checkpoint as PEFT + merged BF16.

const std = @import("std");
const internal = @import("inference_internal");
const checkpoint = internal.finetune.embedding_gemma2_checkpoint;
const adapter_state = internal.finetune.embedding_gemma2_adapter_state;
const exporter = internal.finetune.embedding_gemma2_export;
const data = internal.finetune.embedding_gemma2_data;
const job = internal.finetune.embedding_gemma2_job;
const training = internal.finetune.embedding_gemma2_training;

const shuffle_identity = "fisher-yates-splitmix64-v1";

const Inventory = struct {
    specs: []checkpoint.TensorSpec,
    names: [][]u8,
    shapes: [][2]usize,
    bases: [][]u8,

    fn deinit(self: *Inventory, allocator: std.mem.Allocator) void {
        for (self.names) |name| allocator.free(name);
        for (self.bases) |name| allocator.free(name);
        allocator.free(self.names);
        allocator.free(self.shapes);
        allocator.free(self.specs);
        allocator.free(self.bases);
        self.* = undefined;
    }
};

pub fn main(init: std.process.Init) !void {
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, init.gpa);
    defer args.deinit();
    _ = args.next();
    const config_path = args.next() orelse return usage();
    if (std.mem.eql(u8, config_path, "--help")) return help();
    const checkpoint_dir = args.next() orelse return usage();
    const output_dir = args.next() orelse return usage();
    if (args.next() != null) return usage();

    // Bootstrap the bounded job read, then charge the same allocator against
    // the configured ceiling while reserving external mmap residency.
    var budget = internal.runtime.bounded_allocator.BoundedAllocator{ .backing = init.gpa, .limit = 8 * 1024 * 1024 };
    const allocator = budget.allocator();
    const config_bytes = try std.Io.Dir.cwd().readFileAlloc(init.io, config_path, allocator, .limited(1024 * 1024));
    defer allocator.free(config_bytes);
    var parsed = try std.json.parseFromSlice(job.Config, allocator, config_bytes, .{ .allocate = .alloc_always });
    defer parsed.deinit();
    const config = parsed.value;
    try job.validate(config);
    budget.limit = try exportAllocationLimit(config.max_host_bytes);
    if (budget.live > budget.limit) return error.ExportHostBudgetExceeded;
    try job.rejectPathOverlap(allocator, init.io, output_dir, &.{ config.model_dir, config.prepared_dir, checkpoint_dir });

    var dataset = try data.Dataset.open(allocator, init.io, config.prepared_dir);
    defer dataset.deinit();
    const identity = job.Identity{
        .base_sha256 = try data.decodeDigest(job.base_weight_sha256),
        .tokenizer_sha256 = try data.decodeDigest(job.tokenizer_sha256),
        .prepared_sha256 = dataset.manifest_sha256,
        .preprocessing_sha256 = try data.decodeDigest(dataset.parsed.value.preprocessing_sha256),
    };
    const fingerprint = try job.fingerprint(config, identity);
    const fingerprint_hex = try data.digestHex(allocator, fingerprint);
    defer allocator.free(fingerprint_hex);
    const dataset_hex = try data.digestHex(allocator, dataset.manifest_sha256);
    defer allocator.free(dataset_hex);

    const plan = training.TrainingPlan{ .rank = config.lora.rank, .alpha = config.lora.alpha, .dropout = config.lora.dropout, .max_sequence = @max(config.query_max_tokens, config.document_max_tokens), .mrl_dimensions = &job.dimensions };
    const layout = try adapter_state.Layout.init(plan);
    var inventory = try buildInventory(allocator, layout);
    defer inventory.deinit(allocator);
    const expected = checkpoint.Expected{
        .job_fingerprint = fingerprint_hex,
        .base_sha256 = job.base_weight_sha256,
        .dataset_sha256 = dataset_hex,
        .tokenizer_sha256 = job.tokenizer_sha256,
        .preprocessing_sha256 = dataset.parsed.value.preprocessing_sha256,
        .seed = config.seed,
        .shuffle_identity = shuffle_identity,
        .tensors = inventory.specs,
    };
    var loaded = try checkpoint.loadValidated(allocator, checkpoint_dir, expected, budget.limit, null);
    defer loaded.deinit();
    try validateExportState(loaded.state, dataset.parsed.value.train_records, config.logical_batch_size, config.epochs);

    const adapters = try allocator.alloc(exporter.AdapterTensor, adapter_state.adapter_count);
    defer allocator.free(adapters);
    const targets = try allocator.alloc([]const u8, adapter_state.adapter_count);
    defer allocator.free(targets);
    for (layout.descriptors, 0..) |descriptor, index| {
        const a_tensor = loaded.tensors[index * 6];
        const b_tensor = loaded.tensors[index * 6 + 3];
        adapters[index] = .{ .base_weight_name = inventory.bases[index], .A = a_tensor.data, .B = b_tensor.data, .out_features = descriptor.output_dim, .in_features = descriptor.input_dim, .rank = descriptor.rank };
        targets[index] = inventory.bases[index][0 .. inventory.bases[index].len - ".weight".len];
    }
    var provenance: std.Io.Writer.Allocating = .init(allocator);
    defer provenance.deinit();
    try std.json.Stringify.value(.{ .schema = "antfly.embedding_gemma2.native_training.v1", .checkpoint_weight_sha256 = @as([]const u8, &loaded.tensor_file_sha256), .job_fingerprint = fingerprint_hex, .dataset_sha256 = dataset_hex, .public_source_manifest_sha256 = dataset.parsed.value.source_manifest_sha256, .public_source_manifest_bound = dataset.parsed.value.source_manifest_sha256 != null, .optimizer_step = loaded.state.optimizer_step, .generation = loaded.state.generation, .epoch = loaded.state.epoch, .cursor = loaded.state.cursor, .total_steps = loaded.state.total_steps, .seed = config.seed, .shuffle = shuffle_identity }, .{}, &provenance.writer);
    const report = try exporter.exportBundle(allocator, .{ .base_model_dir = config.model_dir, .output_dir = output_dir, .rank = config.lora.rank, .alpha = config.lora.alpha, .target_modules = targets, .training_provenance_json = provenance.written() }, adapters);
    std.debug.print("Exported EG2 LoRA bundle: {s} ({d} merged, {d} copied, SHA256 {s})\n", .{ output_dir, report.merged_tensor_count, report.copied_tensor_count, &report.output_weight_sha256 });
}

fn exportAllocationLimit(max_host_bytes: u64) !usize {
    // Includes the pinned base mmap and the checkpoint source mmap. Owning
    // checkpoint tensors and merge scratch remain in the bounded allocator.
    const external_host_reserve = 2 * 1024 * 1024 * 1024;
    if (max_host_bytes <= external_host_reserve) return error.ExportHostBudgetExceeded;
    return std.math.cast(usize, max_host_bytes - external_host_reserve) orelse error.ExportHostBudgetExceeded;
}

fn buildInventory(allocator: std.mem.Allocator, layout: adapter_state.Layout) !Inventory {
    try layout.validate();
    const count = adapter_state.adapter_count;
    const specs = try allocator.alloc(checkpoint.TensorSpec, count * 6);
    errdefer allocator.free(specs);
    const names = try allocator.alloc([]u8, count * 6);
    errdefer allocator.free(names);
    var name_count: usize = 0;
    errdefer for (names[0..name_count]) |name| allocator.free(name);
    const shapes = try allocator.alloc([2]usize, count * 6);
    errdefer allocator.free(shapes);
    const bases = try allocator.alloc([]u8, count);
    errdefer allocator.free(bases);
    var base_count: usize = 0;
    errdefer for (bases[0..base_count]) |name| allocator.free(name);
    for (layout.descriptors, 0..) |descriptor, index| {
        var buffer: [128]u8 = undefined;
        const base = try allocator.dupe(u8, try descriptor.writeBaseName(&buffer));
        bases[index] = base;
        base_count += 1;
        const module = base[0 .. base.len - ".weight".len];
        const a_shape = [2]usize{ descriptor.rank, descriptor.input_dim };
        const b_shape = [2]usize{ descriptor.output_dim, descriptor.rank };
        const suffixes = [_][]const u8{ ".lora_A.weight", ".lora_A.weight.exp_avg", ".lora_A.weight.exp_avg_sq", ".lora_B.weight", ".lora_B.weight.exp_avg", ".lora_B.weight.exp_avg_sq" };
        for (suffixes, 0..) |suffix, payload| {
            const slot = index * 6 + payload;
            names[slot] = try std.fmt.allocPrint(allocator, "{s}{s}", .{ module, suffix });
            name_count += 1;
            shapes[slot] = if (payload < 3) a_shape else b_shape;
            specs[slot] = .{ .name = names[slot], .shape = &shapes[slot] };
        }
    }
    return .{ .specs = specs, .names = names, .shapes = shapes, .bases = bases };
}

fn help() void {
    std.debug.print("usage: export-embeddinggemma2-lora <job.json> <checkpoint_dir> <new_output_dir>\n", .{});
}

fn validateExportState(state: checkpoint.State, records: u64, batch_size: u64, epochs: u64) !void {
    if (records == 0 or batch_size == 0 or epochs == 0) return error.InvalidCheckpointState;
    const steps_per_epoch = try std.math.divCeil(u64, records, batch_size);
    const total_steps = std.math.mul(u64, steps_per_epoch, epochs) catch return error.InvalidCheckpointState;
    if (state.optimizer_step == 0 or state.total_steps != total_steps or
        state.generation != state.optimizer_step or state.epoch > epochs or state.cursor >= records or
        (state.epoch == epochs and state.cursor != 0) or state.cursor % batch_size != 0 or
        state.optimizer_step != state.epoch * steps_per_epoch + (try std.math.divCeil(u64, state.cursor, batch_size)))
        return error.InvalidCheckpointState;
}
fn usage() error{InvalidArguments} {
    help();
    return error.InvalidArguments;
}

test "EG2 exporter inventory is exact for all 169 adapters" {
    const allocator = std.testing.allocator;
    var inventory = try buildInventory(allocator, try adapter_state.Layout.init(.{ .rank = 16, .alpha = 32 }));
    defer inventory.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 1014), inventory.specs.len);
    try std.testing.expectEqualStrings("language_model.layers.0.self_attn.q_proj.lora_A.weight", inventory.specs[0].name);
    try std.testing.expectEqualSlices(usize, &.{ 16, 512 }, inventory.specs[0].shape);
    try std.testing.expectEqualStrings("language_model.embedding_projection.lora_B.weight.exp_avg_sq", inventory.specs[1013].name);
    try std.testing.expectEqualSlices(usize, &.{ 768, 16 }, inventory.specs[1013].shape);
}

test "EG2 exporter rejects checkpoint progress inconsistent with dataset" {
    const digest = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const base = checkpoint.State{ .optimizer_step = 2, .generation = 2, .epoch = 0, .cursor = 4, .total_steps = 6, .job_fingerprint = digest, .base_sha256 = digest, .dataset_sha256 = digest, .tokenizer_sha256 = digest, .preprocessing_sha256 = digest, .seed = 42, .shuffle_identity = "fisher-yates-splitmix64-v1" };
    try validateExportState(base, 5, 2, 2);
    var altered = base;
    altered.generation = 1;
    try std.testing.expectError(error.InvalidCheckpointState, validateExportState(altered, 5, 2, 2));
    altered = base;
    altered.total_steps = 7;
    try std.testing.expectError(error.InvalidCheckpointState, validateExportState(altered, 5, 2, 2));
    altered = base;
    altered.cursor = 3;
    try std.testing.expectError(error.InvalidCheckpointState, validateExportState(altered, 5, 2, 2));
}

test "EG2 exporter honors configured host ceilings including mmap reserve" {
    try std.testing.expectError(error.ExportHostBudgetExceeded, exportAllocationLimit(64 * 1024 * 1024));
    try std.testing.expectError(error.ExportHostBudgetExceeded, exportAllocationLimit(2 * 1024 * 1024 * 1024));
    try std.testing.expectEqual(@as(usize, 6 * 1024 * 1024 * 1024), try exportAllocationLimit(8 * 1024 * 1024 * 1024));
    try std.testing.expectEqual(@as(usize, 126 * 1024 * 1024 * 1024), try exportAllocationLimit(128 * 1024 * 1024 * 1024));
}
