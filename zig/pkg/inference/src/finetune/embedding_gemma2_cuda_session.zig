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

//! Resident LoRA state for the dedicated EmbeddingGemma 2 CUDA trainer.
//! Activations and optimizer arrays never acquire host mirrors. Checkpoint
//! transfer is deliberately chunked and synchronized at an explicit boundary.

const std = @import("std");
const platform = @import("antfly_platform");
const cuda_compute = @import("../ops/cuda/cuda_compute.zig");
const buffer_mod = @import("../ops/cuda/buffer.zig");
const training = @import("embedding_gemma2_training.zig");
const adapter_state = @import("embedding_gemma2_adapter_state.zig");
const planning = @import("embedding_gemma2_cuda.zig");
const device_transaction = @import("seeded_device_transaction.zig");
const execution = @import("../execution_control.zig");
const ml = @import("ml").graph;
const checkpoint = @import("embedding_gemma2_checkpoint.zig");

const CudaCompute = cuda_compute.CudaCompute;
const DeviceBuffer = buffer_mod.DeviceBuffer;

pub const OptimizerConfig = struct {
    learning_rate: f64 = 1e-4,
    beta1: f64 = 0.9,
    beta2: f64 = 0.999,
    epsilon: f64 = 1e-8,
    weight_decay: f64 = 0.01,
    max_grad_norm: f64 = 1,

    pub fn validate(self: OptimizerConfig) !void {
        if (!std.math.isFinite(self.learning_rate) or self.learning_rate <= 0 or
            !std.math.isFinite(self.beta1) or self.beta1 < 0 or self.beta1 >= 1 or
            !std.math.isFinite(self.beta2) or self.beta2 < 0 or self.beta2 >= 1 or
            !std.math.isFinite(self.epsilon) or self.epsilon <= 0 or
            !std.math.isFinite(self.weight_decay) or self.weight_decay < 0 or
            !std.math.isFinite(self.max_grad_norm) or self.max_grad_norm <= 0)
            return error.InvalidEmbeddingGemma2Optimizer;
    }
};

pub const ResidentAdapterState = struct {
    allocator: std.mem.Allocator,
    compute: *CudaCompute,
    layout: adapter_state.Layout,
    admission: planning.Admission,
    master: DeviceBuffer,
    grad_accum: DeviceBuffer,
    current_grad: DeviceBuffer,
    first_moment: DeviceBuffer,
    second_moment: DeviceBuffer,
    bf16: DeviceBuffer,
    optimizer_step: u64 = 0,
    microbatch_step: u64 = 0,
    generation: u64 = 0,
    accumulated_microbatches: u32 = 0,
    logical_batch_active: bool = false,

    pub fn init(
        allocator: std.mem.Allocator,
        compute: *CudaCompute,
        config: @import("../models/embedding_gemma2.zig").Config,
        plan: training.TrainingPlan,
        microbatch: u32,
        sequence: u32,
        limits: planning.Limits,
        seed: u64,
    ) !ResidentAdapterState {
        // Establish every required resident-training runtime before allocating
        // or uploading mutable state. The fused FP64-scalar AdamW path is
        // provided by training_math and must never fail late at commit.
        try compute.enableResidentTrainingBlas();
        try compute.enableResidentTrainingMath();
        const layout = try adapter_state.Layout.init(plan);
        const admission = try planning.plan(config, plan, microbatch, sequence, limits);
        const count = std.math.cast(usize, layout.total_parameter_elements) orelse return error.EmbeddingGemma2TrainingLimitExceeded;
        if (count != admission.adapter_elements) return error.InvalidEmbeddingGemma2AdapterLayout;
        const f32_bytes = std.math.mul(usize, count, @sizeOf(f32)) catch return error.EmbeddingGemma2TrainingLimitExceeded;
        const bf16_bytes = std.math.mul(usize, count, @sizeOf(u16)) catch return error.EmbeddingGemma2TrainingLimitExceeded;
        var result = ResidentAdapterState{
            .allocator = allocator,
            .compute = compute,
            .layout = layout,
            .admission = admission,
            .master = .{},
            .grad_accum = .{},
            .current_grad = .{},
            .first_moment = .{},
            .second_moment = .{},
            .bf16 = .{},
        };
        errdefer result.deinit();
        result.master = try compute.allocEmbeddingGemma2TrainingBuffer(f32_bytes);
        result.grad_accum = try compute.allocEmbeddingGemma2TrainingBuffer(f32_bytes);
        result.current_grad = try compute.allocEmbeddingGemma2TrainingBuffer(f32_bytes);
        result.first_moment = try compute.allocEmbeddingGemma2TrainingBuffer(f32_bytes);
        result.second_moment = try compute.allocEmbeddingGemma2TrainingBuffer(f32_bytes);
        result.bf16 = try compute.allocEmbeddingGemma2TrainingBuffer(bf16_bytes);
        try compute.fillEmbeddingGemma2TrainingF32(result.master, count, 0);
        try compute.fillEmbeddingGemma2TrainingF32(result.grad_accum, count, 0);
        try compute.fillEmbeddingGemma2TrainingF32(result.current_grad, count, 0);
        try compute.fillEmbeddingGemma2TrainingF32(result.first_moment, count, 0);
        try compute.fillEmbeddingGemma2TrainingF32(result.second_moment, count, 0);
        try result.initializeA(seed);
        try compute.convertEmbeddingGemma2TrainingF32ToBf16(result.bf16, result.master, count);
        try compute.synchronizeEmbeddingGemma2Training();
        return result;
    }

    pub fn deinit(self: *ResidentAdapterState) void {
        self.compute.releaseEmbeddingGemma2TrainingBuffer(&self.bf16);
        self.compute.releaseEmbeddingGemma2TrainingBuffer(&self.second_moment);
        self.compute.releaseEmbeddingGemma2TrainingBuffer(&self.first_moment);
        self.compute.releaseEmbeddingGemma2TrainingBuffer(&self.current_grad);
        self.compute.releaseEmbeddingGemma2TrainingBuffer(&self.grad_accum);
        self.compute.releaseEmbeddingGemma2TrainingBuffer(&self.master);
    }

    /// Deterministic LoRA initialization: A is uniform in
    /// [-1/sqrt(input_dim), +1/sqrt(input_dim)], B remains exactly zero.
    /// Uploads are bounded to one MiB and occur only at session creation.
    fn initializeA(self: *ResidentAdapterState, seed: u64) !void {
        const chunk_elements = 256 * 1024;
        const host = try self.allocator.alloc(f32, chunk_elements);
        defer self.allocator.free(host);
        var random_state = seed;
        for (self.layout.descriptors) |descriptor| {
            const count: usize = @intCast(descriptor.aElements());
            const base: usize = @intCast(descriptor.a_offset);
            const bound = 1.0 / @sqrt(@as(f32, @floatFromInt(descriptor.input_dim)));
            var offset: usize = 0;
            while (offset < count) {
                const len = @min(chunk_elements, count - offset);
                for (host[0..len]) |*value| {
                    const bits = splitMix64(&random_state);
                    const unit: f32 = @as(f32, @floatFromInt(bits >> 40)) * (1.0 / 16777216.0);
                    value.* = (unit * 2.0 - 1.0) * bound;
                }
                const byte_offset = std.math.mul(usize, base + offset, @sizeOf(f32)) catch return error.EmbeddingGemma2TrainingLimitExceeded;
                const byte_len = std.math.mul(usize, len, @sizeOf(f32)) catch return error.EmbeddingGemma2TrainingLimitExceeded;
                const upload_view = DeviceBuffer{ .ptr = self.master.ptr + byte_offset, .len = byte_len };
                try upload_view.copyFromHost(self.compute.embeddingGemma2TrainingContext(), std.mem.sliceAsBytes(host[0..len]));
                offset += len;
            }
        }
    }

    pub fn beginLogicalBatch(self: *ResidentAdapterState, expected_optimizer_step: u64, expected_microbatch_step: u64) !void {
        if (self.optimizer_step != expected_optimizer_step or self.microbatch_step != expected_microbatch_step)
            return error.StaleEmbeddingGemma2TrainingEpoch;
        if (self.accumulated_microbatches != 0 or self.logical_batch_active) return error.EmbeddingGemma2LogicalBatchActive;
        const count = std.math.cast(usize, self.layout.total_parameter_elements) orelse return error.EmbeddingGemma2TrainingLimitExceeded;
        try self.compute.fillEmbeddingGemma2TrainingF32(self.current_grad, count, 0);
        self.logical_batch_active = true;
    }

    pub fn abortLogicalBatch(self: *ResidentAdapterState) void {
        self.compute.synchronizeEmbeddingGemma2Training() catch {};
        const count = std.math.cast(usize, self.layout.total_parameter_elements) orelse {
            self.logical_batch_active = false;
            return;
        };
        self.compute.fillEmbeddingGemma2TrainingF32(self.current_grad, count, 0) catch {};
        self.compute.synchronizeEmbeddingGemma2Training() catch {};
        self.logical_batch_active = false;
    }

    /// Commit exactly one complete logical batch. Encoder replays accumulate
    /// already-averaged microbatch contributions directly into current_grad;
    /// applying another gradient-accumulation divisor here would be wrong.
    pub fn commitLogicalBatch(self: *ResidentAdapterState, optimizer: OptimizerConfig, loss: f32, control: ?execution.InferenceExecutionControl) !device_transaction.Receipt {
        try optimizer.validate();
        if (!self.logical_batch_active or !std.math.isFinite(loss)) return error.InvalidEmbeddingGemma2Optimizer;
        const count = std.math.cast(usize, self.layout.total_parameter_elements) orelse return error.EmbeddingGemma2TrainingLimitExceeded;
        if (count > std.math.maxInt(i32)) return error.EmbeddingGemma2TrainingLimitExceeded;
        var backend = self.compute.computeBackend();
        const weight = try self.compute.borrowEmbeddingGemma2TrainingF32(self.master, count);
        defer backend.free(weight);
        const accumulation = try self.compute.borrowEmbeddingGemma2TrainingF32(self.grad_accum, count);
        defer backend.free(accumulation);
        const first = try self.compute.borrowEmbeddingGemma2TrainingF32(self.first_moment, count);
        defer backend.free(first);
        const second = try self.compute.borrowEmbeddingGemma2TrainingF32(self.second_moment, count);
        defer backend.free(second);
        const gradient = try self.compute.borrowEmbeddingGemma2TrainingF32(self.current_grad, count);
        defer backend.free(gradient);
        const shape = [_]i32{@intCast(count)};
        const slots = [_]device_transaction.Slot{.{
            .weight = weight,
            .grad_accum = accumulation,
            .m = first,
            .v = second,
            .shape = &shape,
            .group = 0,
            .adam_step = @intCast(self.optimizer_step),
            .present = self.accumulated_microbatches != 0,
        }};
        const groups = [_]device_transaction.Group{.{
            // The legacy F32 fields remain populated for the generic
            // transaction validator. CUDA dispatch consumes pytorch_fused so
            // configured job scalars retain their full F64 identity.
            .optimizer = ml.optimizers.AdamWConfig{ .beta1 = @floatCast(optimizer.beta1), .beta2 = @floatCast(optimizer.beta2), .eps = @floatCast(optimizer.epsilon), .weight_decay = @floatCast(optimizer.weight_decay) },
            .schedule = .{ .constant = @floatCast(optimizer.learning_rate) },
            .pytorch_fused = .{
                .optimizer = ml.optimizers.AdamWConfig64{ .beta1 = optimizer.beta1, .beta2 = optimizer.beta2, .eps = optimizer.epsilon, .weight_decay = optimizer.weight_decay },
                .schedule = .{ .constant = optimizer.learning_rate },
            },
        }};
        const gradients = [_]device_transaction.Gradient{.{ .slot = 0, .value = .{ .tensor = gradient } }};
        const pending = try device_transaction.prepare(self.allocator, &backend, .{
            .identity = .{ .optimizer_step = self.optimizer_step, .microbatch_step = self.microbatch_step },
            .accumulated_microbatches = self.accumulated_microbatches,
            .grad_accum_steps = 1,
            // Norm tensors and the clipping kernel are F32 by contract; this
            // is the single explicit precision boundary for this scalar.
            .max_grad_norm = @floatCast(optimizer.max_grad_norm),
            .slots = &slots,
            .groups = &groups,
        }, .{
            .expected = .{ .optimizer_step = self.optimizer_step, .microbatch_step = self.microbatch_step },
            .action = .submit,
            .loss = loss,
            .gradients = &gradients,
        }, .{ .max_device_bytes = self.admission.device_upper_bound_bytes }, control);
        defer pending.deinit();
        var prepared_bf16 = DeviceBuffer{};
        errdefer self.compute.releaseEmbeddingGemma2TrainingBuffer(&prepared_bf16);
        if (pending.replacements[0].weight) |next_weight| {
            prepared_bf16 = try self.compute.allocEmbeddingGemma2TrainingBuffer(try std.math.mul(usize, count, @sizeOf(u16)));
            const next_master_tensor = CudaCompute.embeddingGemma2Tensor(next_weight);
            try self.compute.convertEmbeddingGemma2TrainingF32ToBf16(prepared_bf16, next_master_tensor.buffer, count);
            try self.compute.synchronizeEmbeddingGemma2Training();
        }
        // Complete every fallible device operation before taking replacement
        // ownership or publishing any live optimizer state. Pending.prepare
        // has finished consuming current_grad when it returns.
        try self.compute.fillEmbeddingGemma2TrainingF32(self.current_grad, count, 0);
        try self.compute.synchronizeEmbeddingGemma2Training();
        if (control) |active| try active.check();
        const replacement = pending.take(0);
        const next_accumulation = takeBuffer(&backend, replacement.grad_accum.?);
        var old_accumulation = self.grad_accum;
        self.grad_accum = next_accumulation;
        self.compute.releaseEmbeddingGemma2TrainingBuffer(&old_accumulation);
        if (replacement.weight) |next_weight| {
            const next_master = takeBuffer(&backend, next_weight);
            const next_first = takeBuffer(&backend, replacement.m.?);
            const next_second = takeBuffer(&backend, replacement.v.?);
            var old_master = self.master;
            var old_first = self.first_moment;
            var old_second = self.second_moment;
            var old_bf16 = self.bf16;
            self.master = next_master;
            self.first_moment = next_first;
            self.second_moment = next_second;
            self.bf16 = prepared_bf16;
            prepared_bf16 = .{};
            self.compute.releaseEmbeddingGemma2TrainingBuffer(&old_master);
            self.compute.releaseEmbeddingGemma2TrainingBuffer(&old_first);
            self.compute.releaseEmbeddingGemma2TrainingBuffer(&old_second);
            self.compute.releaseEmbeddingGemma2TrainingBuffer(&old_bf16);
            self.generation +%= 1;
        }
        self.optimizer_step = pending.receipt.identity.optimizer_step;
        self.microbatch_step = pending.receipt.identity.microbatch_step;
        self.accumulated_microbatches = pending.receipt.accumulated_microbatches;
        self.logical_batch_active = false;
        return pending.receipt;
    }

    pub fn adapterSlice(self: *const ResidentAdapterState, index: usize) !struct { a_master: DeviceBuffer, b_master: DeviceBuffer, a_bf16: DeviceBuffer, b_bf16: DeviceBuffer } {
        if (index >= self.layout.descriptors.len) return error.InvalidEmbeddingGemma2AdapterIndex;
        const descriptor = self.layout.descriptors[index];
        return .{
            .a_master = view(self.master, descriptor.a_offset, descriptor.aElements(), @sizeOf(f32)),
            .b_master = view(self.master, descriptor.b_offset, descriptor.bElements(), @sizeOf(f32)),
            .a_bf16 = view(self.bf16, descriptor.a_offset, descriptor.aElements(), @sizeOf(u16)),
            .b_bf16 = view(self.bf16, descriptor.b_offset, descriptor.bElements(), @sizeOf(u16)),
        };
    }

    pub fn gradientSlice(self: *const ResidentAdapterState, index: usize, accumulated: bool) !struct { a: DeviceBuffer, b: DeviceBuffer } {
        if (index >= self.layout.descriptors.len) return error.InvalidEmbeddingGemma2AdapterIndex;
        const descriptor = self.layout.descriptors[index];
        const storage = if (accumulated) self.grad_accum else self.current_grad;
        return .{
            .a = view(storage, descriptor.a_offset, descriptor.aElements(), @sizeOf(f32)),
            .b = view(storage, descriptor.b_offset, descriptor.bElements(), @sizeOf(f32)),
        };
    }

    pub const Snapshot = struct {
        allocator: std.mem.Allocator,
        tensors: []checkpoint.Tensor,
        names: [][]u8,
        shapes: [][]usize,
        values: [][]f32,

        pub fn deinit(self: *Snapshot) void {
            for (self.names) |value| self.allocator.free(value);
            for (self.shapes) |value| self.allocator.free(value);
            for (self.values) |value| self.allocator.free(value);
            self.allocator.free(self.tensors);
            self.allocator.free(self.names);
            self.allocator.free(self.shapes);
            self.allocator.free(self.values);
            self.* = undefined;
        }
    };

    /// Owning exact tensor specification used by checkpoint.loadValidated.
    /// Ordering is identical to snapshot() and restore(): adapter A then B,
    /// each followed by master, first moment, and second moment.
    pub const CheckpointSpecs = struct {
        allocator: std.mem.Allocator,
        specs: []checkpoint.TensorSpec,
        names: [][]u8,
        shapes: [][]usize,

        pub fn deinit(self: *CheckpointSpecs) void {
            for (self.names) |value| self.allocator.free(value);
            for (self.shapes) |value| self.allocator.free(value);
            self.allocator.free(self.specs);
            self.allocator.free(self.names);
            self.allocator.free(self.shapes);
            self.* = undefined;
        }
    };

    pub fn checkpointSpecs(self: *const ResidentAdapterState, allocator: std.mem.Allocator) !CheckpointSpecs {
        const tensor_count = adapter_state.adapter_count * 6;
        var result = CheckpointSpecs{
            .allocator = allocator,
            .specs = try allocator.alloc(checkpoint.TensorSpec, tensor_count),
            .names = &.{},
            .shapes = &.{},
        };
        errdefer result.deinit();
        result.names = try allocator.alloc([]u8, tensor_count);
        for (result.names) |*value| value.* = &.{};
        result.shapes = try allocator.alloc([]usize, tensor_count);
        for (result.shapes) |*value| value.* = &.{};
        var tensor_index: usize = 0;
        var name_buffer: [256]u8 = undefined;
        for (self.layout.descriptors) |descriptor| {
            const base = try descriptor.writeBaseName(&name_buffer);
            if (!std.mem.endsWith(u8, base, ".weight")) return error.InvalidEmbeddingGemma2AdapterLayout;
            const stem = base[0 .. base.len - ".weight".len];
            inline for (.{
                .{ "lora_A.weight", [_]usize{ @intCast(descriptor.rank), @intCast(descriptor.input_dim) } },
                .{ "lora_B.weight", [_]usize{ @intCast(descriptor.output_dim), @intCast(descriptor.rank) } },
            }) |parameter| {
                inline for (.{ "", "exp_avg", "exp_avg_sq" }) |suffix| {
                    const name = if (suffix.len == 0)
                        try std.fmt.allocPrint(allocator, "{s}.{s}", .{ stem, parameter[0] })
                    else
                        try std.fmt.allocPrint(allocator, "{s}.{s}.{s}", .{ stem, parameter[0], suffix });
                    result.names[tensor_index] = name;
                    const shape = try allocator.dupe(usize, &parameter[1]);
                    result.shapes[tensor_index] = shape;
                    result.specs[tensor_index] = .{ .name = name, .shape = shape };
                    tensor_index += 1;
                }
            }
        }
        if (tensor_index != tensor_count) return error.InvalidEmbeddingGemma2AdapterLayout;
        return result;
    }

    /// Checkpoint boundary: no partial logical batch may be serialized. All
    /// D2H copies are queued, followed by one stream drain before publication.
    pub fn snapshot(self: *ResidentAdapterState, allocator: std.mem.Allocator, max_host_bytes: usize) !Snapshot {
        if (self.accumulated_microbatches != 0 or self.logical_batch_active) return error.EmbeddingGemma2LogicalBatchActive;
        const tensor_count = adapter_state.adapter_count * 6;
        var result = Snapshot{
            .allocator = allocator,
            .tensors = try allocator.alloc(checkpoint.Tensor, tensor_count),
            .names = &.{},
            .shapes = &.{},
            .values = &.{},
        };
        errdefer result.deinit();
        result.names = try allocator.alloc([]u8, tensor_count);
        for (result.names) |*value| value.* = &.{};
        result.shapes = try allocator.alloc([]usize, tensor_count);
        for (result.shapes) |*value| value.* = &.{};
        result.values = try allocator.alloc([]f32, tensor_count);
        for (result.values) |*value| value.* = &.{};
        var total_bytes: usize = 0;
        var tensor_index: usize = 0;
        var name_buffer: [256]u8 = undefined;
        for (self.layout.descriptors) |descriptor| {
            const base = try descriptor.writeBaseName(&name_buffer);
            if (!std.mem.endsWith(u8, base, ".weight")) return error.InvalidEmbeddingGemma2AdapterLayout;
            const stem = base[0 .. base.len - ".weight".len];
            inline for (.{
                .{ "lora_A.weight", descriptor.a_offset, descriptor.aElements(), [_]usize{ @intCast(descriptor.rank), @intCast(descriptor.input_dim) } },
                .{ "lora_B.weight", descriptor.b_offset, descriptor.bElements(), [_]usize{ @intCast(descriptor.output_dim), @intCast(descriptor.rank) } },
            }) |parameter| {
                inline for (.{ .{ "", self.master }, .{ "exp_avg", self.first_moment }, .{ "exp_avg_sq", self.second_moment } }) |state| {
                    const name = if (state[0].len == 0)
                        try std.fmt.allocPrint(allocator, "{s}.{s}", .{ stem, parameter[0] })
                    else
                        try std.fmt.allocPrint(allocator, "{s}.{s}.{s}", .{ stem, parameter[0], state[0] });
                    result.names[tensor_index] = name;
                    const shape = try allocator.dupe(usize, &parameter[3]);
                    result.shapes[tensor_index] = shape;
                    const element_count: usize = @intCast(parameter[2]);
                    const byte_count = try std.math.mul(usize, element_count, @sizeOf(f32));
                    total_bytes = try std.math.add(usize, total_bytes, byte_count);
                    if (total_bytes > max_host_bytes) return error.EmbeddingGemma2CheckpointBudgetExceeded;
                    const values = try allocator.alloc(f32, element_count);
                    result.values[tensor_index] = values;
                    const source = view(state[1], parameter[1], parameter[2], @sizeOf(f32));
                    try source.copyToHost(self.compute.embeddingGemma2TrainingContext(), std.mem.sliceAsBytes(values));
                    result.tensors[tensor_index] = .{ .name = name, .data = values, .shape = shape };
                    tensor_index += 1;
                }
            }
        }
        try self.compute.synchronizeEmbeddingGemma2Training();
        for (result.values) |values| for (values) |value| if (!std.math.isFinite(value)) return error.NonFiniteCheckpointTensor;
        return result;
    }

    pub fn restore(self: *ResidentAdapterState, loaded: *const checkpoint.Loaded) !void {
        if (self.accumulated_microbatches != 0 or self.logical_batch_active or loaded.tensors.len != adapter_state.adapter_count * 6)
            return error.InvalidEmbeddingGemma2CheckpointLayout;
        const count = std.math.cast(usize, self.layout.total_parameter_elements) orelse return error.EmbeddingGemma2TrainingLimitExceeded;
        const f32_bytes = try std.math.mul(usize, count, @sizeOf(f32));
        const bf16_bytes = try std.math.mul(usize, count, @sizeOf(u16));
        var next_master = try self.compute.allocEmbeddingGemma2TrainingBuffer(f32_bytes);
        errdefer self.compute.releaseEmbeddingGemma2TrainingBuffer(&next_master);
        var next_first = try self.compute.allocEmbeddingGemma2TrainingBuffer(f32_bytes);
        errdefer self.compute.releaseEmbeddingGemma2TrainingBuffer(&next_first);
        var next_second = try self.compute.allocEmbeddingGemma2TrainingBuffer(f32_bytes);
        errdefer self.compute.releaseEmbeddingGemma2TrainingBuffer(&next_second);
        var next_bf16 = try self.compute.allocEmbeddingGemma2TrainingBuffer(bf16_bytes);
        errdefer self.compute.releaseEmbeddingGemma2TrainingBuffer(&next_bf16);
        var queued = false;
        errdefer if (queued) self.compute.synchronizeEmbeddingGemma2Training() catch {};
        var tensor_index: usize = 0;
        var name_buffer: [256]u8 = undefined;
        for (self.layout.descriptors) |descriptor| {
            const base = try descriptor.writeBaseName(&name_buffer);
            if (!std.mem.endsWith(u8, base, ".weight")) return error.InvalidEmbeddingGemma2AdapterLayout;
            const stem = base[0 .. base.len - ".weight".len];
            inline for (.{
                .{ "lora_A.weight", descriptor.a_offset, descriptor.aElements(), [_]usize{ @intCast(descriptor.rank), @intCast(descriptor.input_dim) } },
                .{ "lora_B.weight", descriptor.b_offset, descriptor.bElements(), [_]usize{ @intCast(descriptor.output_dim), @intCast(descriptor.rank) } },
            }) |parameter| {
                inline for (.{ .{ "", next_master }, .{ "exp_avg", next_first }, .{ "exp_avg_sq", next_second } }) |state| {
                    const tensor = loaded.tensors[tensor_index];
                    var expected_name: [320]u8 = undefined;
                    const expected = if (state[0].len == 0)
                        try std.fmt.bufPrint(&expected_name, "{s}.{s}", .{ stem, parameter[0] })
                    else
                        try std.fmt.bufPrint(&expected_name, "{s}.{s}.{s}", .{ stem, parameter[0], state[0] });
                    if (!std.mem.eql(u8, tensor.name, expected) or !std.mem.eql(usize, tensor.shape, &parameter[3]) or tensor.data.len != @as(usize, @intCast(parameter[2])))
                        return error.InvalidEmbeddingGemma2CheckpointLayout;
                    for (tensor.data) |value| if (!std.math.isFinite(value)) return error.NonFiniteCheckpointTensor;
                    const destination = view(state[1], parameter[1], parameter[2], @sizeOf(f32));
                    try destination.copyFromHost(self.compute.embeddingGemma2TrainingContext(), std.mem.sliceAsBytes(tensor.data));
                    queued = true;
                    tensor_index += 1;
                }
            }
        }
        try self.compute.convertEmbeddingGemma2TrainingF32ToBf16(next_bf16, next_master, count);
        try self.compute.synchronizeEmbeddingGemma2Training();
        queued = false;
        var old_master = self.master;
        var old_first = self.first_moment;
        var old_second = self.second_moment;
        var old_bf16 = self.bf16;
        self.master = next_master;
        self.first_moment = next_first;
        self.second_moment = next_second;
        self.bf16 = next_bf16;
        next_master = .{};
        next_first = .{};
        next_second = .{};
        next_bf16 = .{};
        self.compute.releaseEmbeddingGemma2TrainingBuffer(&old_master);
        self.compute.releaseEmbeddingGemma2TrainingBuffer(&old_first);
        self.compute.releaseEmbeddingGemma2TrainingBuffer(&old_second);
        self.compute.releaseEmbeddingGemma2TrainingBuffer(&old_bf16);
        self.optimizer_step = loaded.state.optimizer_step;
        self.microbatch_step = loaded.state.optimizer_step;
        self.generation = loaded.state.generation;
    }
};

fn takeBuffer(backend: *const @import("../ops/ops.zig").ComputeBackend, tensor: @import("../ops/ops.zig").CT) DeviceBuffer {
    const cuda_tensor = CudaCompute.embeddingGemma2Tensor(tensor);
    const buffer = cuda_tensor.buffer;
    cuda_tensor.owns_buffer = false;
    backend.free(tensor);
    return buffer;
}

fn view(buffer: DeviceBuffer, element_offset: u64, element_count: u64, element_bytes: usize) DeviceBuffer {
    const stride: u64 = @intCast(element_bytes);
    const offset: usize = @intCast(element_offset * stride);
    const len: usize = @intCast(element_count * stride);
    std.debug.assert(offset + len <= buffer.len);
    return .{ .ptr = buffer.ptr + offset, .len = len };
}

fn splitMix64(state: *u64) u64 {
    state.* +%= 0x9e3779b97f4a7c15;
    var value = state.*;
    value = (value ^ (value >> 30)) *% 0xbf58476d1ce4e5b9;
    value = (value ^ (value >> 27)) *% 0x94d049bb133111eb;
    return value ^ (value >> 31);
}

test "EmbeddingGemma2 optimizer parameters reject nonfinite and invalid ranges" {
    try (OptimizerConfig{}).validate();
    try std.testing.expectError(error.InvalidEmbeddingGemma2Optimizer, (OptimizerConfig{ .learning_rate = 0 }).validate());
    try std.testing.expectError(error.InvalidEmbeddingGemma2Optimizer, (OptimizerConfig{ .beta2 = std.math.nan(f64) }).validate());
}

test "EmbeddingGemma2 checkpoint specifications exactly cover optimizer state" {
    const layout = try adapter_state.Layout.init(.{});
    const state = ResidentAdapterState{
        .allocator = std.testing.allocator,
        .compute = undefined,
        .layout = layout,
        .admission = undefined,
        .master = .{},
        .grad_accum = .{},
        .current_grad = .{},
        .first_moment = .{},
        .second_moment = .{},
        .bf16 = .{},
    };
    var specs = try state.checkpointSpecs(std.testing.allocator);
    defer specs.deinit();
    try std.testing.expectEqual(@as(usize, adapter_state.adapter_count * 6), specs.specs.len);
    try std.testing.expectEqualStrings("language_model.layers.0.self_attn.q_proj.lora_A.weight", specs.specs[0].name);
    try std.testing.expectEqualSlices(usize, &.{ 16, 512 }, specs.specs[0].shape);
    try std.testing.expectEqualStrings("language_model.layers.0.self_attn.q_proj.lora_A.weight.exp_avg", specs.specs[1].name);
    try std.testing.expectEqualStrings("language_model.embedding_projection.lora_B.weight.exp_avg_sq", specs.specs[specs.specs.len - 1].name);
    try std.testing.expectEqualSlices(usize, &.{ 768, 16 }, specs.specs[specs.specs.len - 1].shape);
}

test "EmbeddingGemma2 SplitMix initialization is reproducible" {
    var a: u64 = 42;
    var b: u64 = 42;
    for (0..32) |_| try std.testing.expectEqual(splitMix64(&a), splitMix64(&b));
}

pub fn compileProbe() !void {
    // Runtime-false in tests, but not comptime-known: keep lifecycle paths in
    // semantic analysis without requiring a CUDA device.
    if (platform.env.getenvBoolDefault("ANTFLY_TEST_INSTANTIATE_EG2_ADAPTER_STATE", false)) {
        var state = try ResidentAdapterState.init(std.testing.allocator, undefined, undefined, .{}, 2, 512, .{}, 42);
        try state.beginLogicalBatch(0, 0);
        _ = try state.commitLogicalBatch(.{}, 0, null);
        var specs = try state.checkpointSpecs(std.testing.allocator);
        defer specs.deinit();
        var snapshot_value = try state.snapshot(std.testing.allocator, std.math.maxInt(usize));
        defer snapshot_value.deinit();
        try state.restore(undefined);
        state.abortLogicalBatch();
        state.deinit();
    }
}

test "EmbeddingGemma2 resident adapter lifecycle remains fully instantiated" {
    try compileProbe();
}
