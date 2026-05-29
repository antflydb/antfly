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

// Training loop: weight mutation, optimizer integration, checkpointing.
//
// TrainingWeightStore: mutable f32 copies of trainable parameters backed
//   by a read-only ComputeBackend for frozen weights.
// TrainingLoop: forward → loss → gradient → clip → optimize → upload.
// Checkpoint: binary save/load of parameters + optimizer state.

const std = @import("std");
const ml = @import("ml");
const Graph = ml.graph.Graph;
const NodeId = ml.graph.NodeId;
const Shape = ml.graph.Shape;
const checkpoint_mod = ml.graph.checkpoint;
const optimizers = ml.graph.optimizers;
const ops_mod = @import("../ops/ops.zig");
const contracts = @import("backend_contracts.zig");
const CT = contracts.CT;
const ComputeBackend = ops_mod.ComputeBackend;
const training = @import("training.zig");

// ─── TrainingWeightStore ──────────────────────────────────────────────────────

/// Mutable weight store for training. Holds f32 copies of trainable
/// parameters; delegates reads of frozen parameters to the base backend.
pub const TrainingWeightStore = struct {
    trainable_weights: std.StringHashMapUnmanaged([]f32),
    /// Keys that were heap-allocated (and must be freed on deinit/overwrite).
    owned_keys: std.ArrayListUnmanaged([]const u8),
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) TrainingWeightStore {
        return .{
            .trainable_weights = .{},
            .owned_keys = .empty,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *TrainingWeightStore) void {
        var it = self.trainable_weights.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.value_ptr.*);
        }
        self.trainable_weights.deinit(self.allocator);
        for (self.owned_keys.items) |k| {
            self.allocator.free(k);
        }
        self.owned_keys.deinit(self.allocator);
    }

    /// Materialize a parameter as a mutable f32 slice by downloading from
    /// the compute backend. If already materialized, returns the existing copy.
    pub fn materializeTrainable(
        self: *TrainingWeightStore,
        name: []const u8,
        cb: *const ComputeBackend,
    ) ![]f32 {
        const gop = try self.trainable_weights.getOrPut(self.allocator, name);
        if (!gop.found_existing) {
            const ct = try cb.getWeight(name);
            const data = try cb.toFloat32(ct, self.allocator);
            gop.value_ptr.* = data;
        }
        return gop.value_ptr.*;
    }

    /// Materialize from an existing f32 slice (e.g. from runtime inputs).
    pub fn materializeFromSlice(
        self: *TrainingWeightStore,
        name: []const u8,
        data: []const f32,
    ) ![]f32 {
        const gop = try self.trainable_weights.getOrPut(self.allocator, name);
        if (!gop.found_existing) {
            gop.value_ptr.* = try self.allocator.dupe(f32, data);
        }
        return gop.value_ptr.*;
    }

    /// Put with an owned (heap-allocated) key. Takes ownership of both
    /// name_owned and data; frees name_owned if key already exists.
    pub fn putOwned(self: *TrainingWeightStore, name_owned: []u8, data: []f32) !void {
        const gop = try self.trainable_weights.getOrPut(self.allocator, name_owned);
        if (gop.found_existing) {
            self.allocator.free(gop.value_ptr.*);
            self.allocator.free(name_owned); // already keyed
        } else {
            try self.owned_keys.append(self.allocator, name_owned);
        }
        gop.value_ptr.* = data;
    }

    /// Set (overwrite) a trainable weight's data.
    pub fn setWeight(self: *TrainingWeightStore, name: []const u8, data: []const f32) !void {
        const gop = try self.trainable_weights.getOrPut(self.allocator, name);
        if (gop.found_existing) {
            @memcpy(gop.value_ptr.*, data);
        } else {
            gop.value_ptr.* = try self.allocator.dupe(f32, data);
        }
    }

    /// Get the current mutable weight data. Returns null if not materialized.
    pub fn getWeight(self: *const TrainingWeightStore, name: []const u8) ?[]f32 {
        return self.trainable_weights.get(name);
    }

    /// Upload all trainable weights to the compute backend as runtime inputs.
    /// Returns a map of NodeId → CT suitable for passing to trainStep.
    pub fn buildRuntimeInputs(
        self: *const TrainingWeightStore,
        graph: *const Graph,
        cb: *const ComputeBackend,
        allocator: std.mem.Allocator,
    ) !std.AutoHashMapUnmanaged(NodeId, CT) {
        var rt = std.AutoHashMapUnmanaged(NodeId, CT){};
        errdefer {
            var it = rt.iterator();
            while (it.next()) |entry| {
                cb.free(entry.value_ptr.*);
            }
            rt.deinit(allocator);
        }

        for (graph.parameters.items) |param_id| {
            const param_node = graph.node(param_id);
            const name = graph.parameterName(param_node);
            if (self.trainable_weights.get(name)) |data| {
                const ct = try cb.fromFloat32(data);
                try rt.put(allocator, param_id, ct);
            }
        }

        return rt;
    }
};

pub const FlatParamLayout = struct {
    name: []const u8,
    len: usize,
    param_offset: usize,
    grad_offset: usize,
    m_offset: usize,
    v_offset: usize,
};

pub const FlatTrainingState = struct {
    layouts: []FlatParamLayout,
    params: []f32,
    grads: []f32,
    m: []f32,
    v: []f32,
    total_param_elements: usize,
    allocator: std.mem.Allocator,

    pub fn initFromCurrentState(
        allocator: std.mem.Allocator,
        weight_store: *const TrainingWeightStore,
        optimizer_state: *const optimizers.OptimizerState,
        config: optimizers.Optimizer,
    ) !FlatTrainingState {
        var names = std.ArrayListUnmanaged([]const u8).empty;
        defer names.deinit(allocator);

        {
            var it = weight_store.trainable_weights.iterator();
            while (it.next()) |entry| {
                try names.append(allocator, entry.key_ptr.*);
            }
        }

        std.mem.sort([]const u8, names.items, {}, lessThanString);

        const needs_v = optimizerNeedsVariance(config);
        const total_param_elements = countTotalParamElements(weight_store, names.items);
        const layouts = try allocator.alloc(FlatParamLayout, names.items.len);
        errdefer allocator.free(layouts);
        const params = try allocator.alloc(f32, total_param_elements);
        errdefer allocator.free(params);
        const grads = try allocator.alloc(f32, total_param_elements);
        errdefer allocator.free(grads);
        const m = try allocator.alloc(f32, total_param_elements);
        errdefer allocator.free(m);
        const v = if (needs_v) try allocator.alloc(f32, total_param_elements) else try allocator.alloc(f32, 0);
        errdefer allocator.free(v);

        @memset(grads, 0);
        @memset(m, 0);
        if (needs_v) @memset(v, 0);

        var offset: usize = 0;
        for (names.items, 0..) |name, i| {
            const param = weight_store.getWeight(name) orelse return error.MissingTrainableWeight;
            layouts[i] = .{
                .name = name,
                .len = param.len,
                .param_offset = offset,
                .grad_offset = offset,
                .m_offset = offset,
                .v_offset = offset,
            };
            @memcpy(params[offset .. offset + param.len], param);
            if (optimizer_state.param_states.get(name)) |ps| {
                @memcpy(m[offset .. offset + param.len], ps.m);
                if (needs_v and ps.v.len == param.len) {
                    @memcpy(v[offset .. offset + param.len], ps.v);
                }
            }
            offset += param.len;
        }

        return .{
            .layouts = layouts,
            .params = params,
            .grads = grads,
            .m = m,
            .v = v,
            .total_param_elements = total_param_elements,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *FlatTrainingState) void {
        self.allocator.free(self.layouts);
        self.allocator.free(self.params);
        self.allocator.free(self.grads);
        self.allocator.free(self.m);
        self.allocator.free(self.v);
        self.* = undefined;
    }

    pub fn zeroGradients(self: *FlatTrainingState) void {
        @memset(self.grads, 0);
    }

    pub fn buildRuntimeInputs(
        self: *const FlatTrainingState,
        graph: *const Graph,
        cb: *const ComputeBackend,
        allocator: std.mem.Allocator,
    ) !std.AutoHashMapUnmanaged(NodeId, CT) {
        var rt = std.AutoHashMapUnmanaged(NodeId, CT){};
        errdefer {
            var it = rt.iterator();
            while (it.next()) |entry| {
                cb.free(entry.value_ptr.*);
            }
            rt.deinit(allocator);
        }

        for (graph.parameters.items) |param_id| {
            const param_node = graph.node(param_id);
            const name = graph.parameterName(param_node);
            if (self.layoutByName(name)) |layout| {
                const param_slice = self.params[layout.param_offset .. layout.param_offset + layout.len];
                const ct = try cb.fromFloat32(param_slice);
                try rt.put(allocator, param_id, ct);
            }
        }

        return rt;
    }

    pub fn copyGradientsFromResult(self: *FlatTrainingState, result: *const training.TrainStepResult) void {
        self.zeroGradients();
        var it = result.gradients.iterator();
        while (it.next()) |entry| {
            const name = entry.key_ptr.*;
            const grad = entry.value_ptr.*;
            const layout = self.layoutByName(name) orelse continue;
            std.debug.assert(layout.len == grad.len);
            @memcpy(self.grads[layout.grad_offset .. layout.grad_offset + layout.len], grad);
        }
    }

    pub fn syncParamsToWeightStore(self: *const FlatTrainingState, weight_store: *TrainingWeightStore) !void {
        for (self.layouts) |layout| {
            const weight = weight_store.getWeight(layout.name) orelse return error.MissingTrainableWeight;
            const param_slice = self.params[layout.param_offset .. layout.param_offset + layout.len];
            @memcpy(weight, param_slice);
        }
    }

    pub fn syncOptimizerState(
        self: *const FlatTrainingState,
        optimizer_state: *optimizers.OptimizerState,
        needs_v: bool,
    ) !void {
        for (self.layouts) |layout| {
            const ps = try optimizer_state.getOrCreate(layout.name, layout.len, needs_v);
            @memcpy(ps.m, self.m[layout.m_offset .. layout.m_offset + layout.len]);
            if (needs_v) {
                @memcpy(ps.v, self.v[layout.v_offset .. layout.v_offset + layout.len]);
            }
        }
    }

    pub fn gradientSlices(self: *FlatTrainingState, allocator: std.mem.Allocator) ![][]f32 {
        const slices = try allocator.alloc([]f32, self.layouts.len);
        for (self.layouts, 0..) |layout, i| {
            slices[i] = self.grads[layout.grad_offset .. layout.grad_offset + layout.len];
        }
        return slices;
    }

    fn layoutByName(self: *const FlatTrainingState, name: []const u8) ?FlatParamLayout {
        for (self.layouts) |layout| {
            if (std.mem.eql(u8, layout.name, name)) return layout;
        }
        return null;
    }
};

pub const TrainingStepMetrics = struct {
    runtime_input_build_ns: u64 = 0,
    optimizer_ns: u64 = 0,
    total_ns: u64 = 0,
    train_step: training.TrainStepProfile = .{},
    checkpoint_summary: ?training.CheckpointSummary = null,
    flat_param_count: usize = 0,
    flat_param_elements: usize = 0,
    flat_param_bytes: usize = 0,
    flat_optimizer_state_bytes: usize = 0,
};

// ─── TrainingConfig ───────────────────────────────────────────────────────────

pub const TrainingConfig = struct {
    optimizer: optimizers.Optimizer = .{ .adam = .{} },
    lr_schedule: optimizers.LearningRateSchedule = .{ .constant = 0.001 },
    grad_clip: optimizers.GradientClipConfig = .{ .none = {} },
    trainable_params: ?[]const []const u8 = null,
    checkpoint_config: ?checkpoint_mod.CheckpointConfig = null,
    emit_checkpoint_analysis: bool = false,
};

// ─── TrainingLoop ─────────────────────────────────────────────────────────────

pub const TrainingLoop = struct {
    config: TrainingConfig,
    weight_store: TrainingWeightStore,
    optimizer_state: optimizers.OptimizerState,
    flat_state: ?FlatTrainingState = null,
    last_step_metrics: TrainingStepMetrics = .{},
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, config: TrainingConfig) TrainingLoop {
        return .{
            .config = config,
            .weight_store = TrainingWeightStore.init(allocator),
            .optimizer_state = optimizers.OptimizerState.init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *TrainingLoop) void {
        if (self.flat_state) |*flat_state| flat_state.deinit();
        self.weight_store.deinit();
        self.optimizer_state.deinit();
    }

    /// Run one training step: forward + backward + clip + optimize.
    /// Returns the scalar loss value.
    pub fn step(
        self: *TrainingLoop,
        graph: *const Graph,
        loss_node: NodeId,
        cb: *const ComputeBackend,
    ) !f32 {
        const total_start = nowNs();
        try self.refreshFlatState();
        const flat_state = &self.flat_state.?;

        const runtime_input_start = nowNs();
        var rt = try flat_state.buildRuntimeInputs(graph, cb, self.allocator);
        defer {
            var it = rt.iterator();
            while (it.next()) |entry| {
                cb.free(entry.value_ptr.*);
            }
            rt.deinit(self.allocator);
        }
        self.last_step_metrics = .{
            .runtime_input_build_ns = elapsedNs(runtime_input_start),
            .flat_param_count = flat_state.layouts.len,
            .flat_param_elements = flat_state.total_param_elements,
            .flat_param_bytes = flat_state.total_param_elements * @sizeOf(f32),
            .flat_optimizer_state_bytes = flat_state.m.len * @sizeOf(f32) + flat_state.v.len * @sizeOf(f32),
        };

        // Forward + backward.
        var result = try training.trainStep(
            self.allocator,
            graph,
            loss_node,
            cb,
            rt,
            .{
                .trainable_params = self.config.trainable_params,
                .checkpoint_config = self.config.checkpoint_config,
                .emit_checkpoint_analysis = self.config.emit_checkpoint_analysis,
            },
        );
        defer result.deinit();
        self.last_step_metrics.train_step = result.profile;
        self.last_step_metrics.checkpoint_summary = result.checkpoint_summary;

        flat_state.copyGradientsFromResult(&result);
        try self.clipGradients(flat_state);

        // Optimizer step.
        const optimizer_start = nowNs();
        self.optimizer_state.step_count += 1;
        const current_lr = self.config.lr_schedule.lr(self.optimizer_state.step_count);
        const needs_v = optimizerNeedsVariance(self.config.optimizer);
        for (flat_state.layouts) |layout| {
            const param = flat_state.params[layout.param_offset .. layout.param_offset + layout.len];
            const grad = flat_state.grads[layout.grad_offset .. layout.grad_offset + layout.len];
            const m = flat_state.m[layout.m_offset .. layout.m_offset + layout.len];
            const v = if (needs_v)
                flat_state.v[layout.v_offset .. layout.v_offset + layout.len]
            else
                flat_state.v[0..0];
            optimizers.stepSlices(self.config.optimizer, self.optimizer_state.step_count, current_lr, param, grad, m, v);
        }
        self.last_step_metrics.optimizer_ns = elapsedNs(optimizer_start);

        try flat_state.syncParamsToWeightStore(&self.weight_store);
        try flat_state.syncOptimizerState(&self.optimizer_state, needs_v);
        self.last_step_metrics.total_ns = elapsedNs(total_start);

        return result.loss;
    }

    fn clipGradients(self: *TrainingLoop, flat_state: *FlatTrainingState) !void {
        switch (self.config.grad_clip) {
            .none => {},
            else => {
                const grad_slices = try flat_state.gradientSlices(self.allocator);
                defer self.allocator.free(grad_slices);
                optimizers.clipGradients(grad_slices, self.config.grad_clip);
            },
        }
    }

    /// Serialize checkpoint to an allocator-owned byte buffer.
    pub fn serializeCheckpoint(self: *const TrainingLoop, allocator: std.mem.Allocator) ![]u8 {
        var buf = std.ArrayListUnmanaged(u8).empty;
        errdefer buf.deinit(allocator);

        // Header: magic + version + step count + num params.
        try buf.appendSlice(allocator, "TMCK");
        try appendU32(&buf, allocator, 1); // version
        try appendU32(&buf, allocator, self.optimizer_state.step_count);

        // Count trainable params.
        var num_params: u32 = 0;
        {
            var it = self.weight_store.trainable_weights.iterator();
            while (it.next()) |_| num_params += 1;
        }
        try appendU32(&buf, allocator, num_params);

        // Write each parameter.
        var it = self.weight_store.trainable_weights.iterator();
        while (it.next()) |entry| {
            const name = entry.key_ptr.*;
            const data = entry.value_ptr.*;

            // Name.
            try appendU32(&buf, allocator, @intCast(name.len));
            try buf.appendSlice(allocator, name);

            // Weight data.
            try appendU32(&buf, allocator, @intCast(data.len));
            try buf.appendSlice(allocator, std.mem.sliceAsBytes(data));

            // Optimizer state.
            if (self.optimizer_state.param_states.get(name)) |ps| {
                try buf.append(allocator, 1); // has_state
                try buf.appendSlice(allocator, std.mem.sliceAsBytes(ps.m));
                if (ps.v.len > 0) {
                    try buf.append(allocator, 1); // has_v
                    try buf.appendSlice(allocator, std.mem.sliceAsBytes(ps.v));
                } else {
                    try buf.append(allocator, 0);
                }
            } else {
                try buf.append(allocator, 0); // no state
            }
        }

        return buf.toOwnedSlice(allocator);
    }

    /// Deserialize checkpoint from a byte buffer.
    pub fn loadCheckpointFromBytes(self: *TrainingLoop, bytes: []const u8) !void {
        if (bytes.len < 16) return error.InvalidCheckpoint;

        // Header.
        if (!std.mem.eql(u8, bytes[0..4], "TMCK")) return error.InvalidCheckpoint;

        var pos: usize = 4;
        const version = readU32(bytes, &pos);
        if (version != 1) return error.UnsupportedCheckpointVersion;

        self.optimizer_state.step_count = readU32(bytes, &pos);
        const num_params = readU32(bytes, &pos);

        // Read each parameter.
        for (0..num_params) |_| {
            // Name.
            const name_len = try readU32Checked(bytes, &pos);
            if (bytes.len - pos < name_len) return error.InvalidCheckpoint;
            const name_buf = bytes[pos..][0..name_len];
            pos += name_len;

            // Weight data.
            const data_len = try readU32Checked(bytes, &pos);
            if (data_len > (bytes.len - pos) / @sizeOf(f32)) return error.InvalidCheckpoint;
            const data_bytes = bytes[pos..][0 .. data_len * 4];
            pos += data_len * 4;
            const data = try self.allocator.alloc(f32, data_len);
            @memcpy(std.mem.sliceAsBytes(data), data_bytes);

            // Store weight with an owned key copy.
            const name_key = try self.allocator.dupe(u8, name_buf);
            try self.weight_store.putOwned(name_key, data);

            // Optimizer state.
            if (pos >= bytes.len) return error.InvalidCheckpoint;
            const has_state = bytes[pos];
            pos += 1;
            if (has_state == 1) {
                if (data_len > (bytes.len - pos) / @sizeOf(f32)) return error.InvalidCheckpoint;
                const m = try self.allocator.alloc(f32, data_len);
                @memcpy(std.mem.sliceAsBytes(m), bytes[pos..][0 .. data_len * 4]);
                pos += data_len * 4;

                if (pos >= bytes.len) {
                    self.allocator.free(m);
                    return error.InvalidCheckpoint;
                }
                const has_v = bytes[pos];
                pos += 1;
                const v: []f32 = if (has_v == 1) blk: {
                    if (data_len > (bytes.len - pos) / @sizeOf(f32)) {
                        self.allocator.free(m);
                        return error.InvalidCheckpoint;
                    }
                    const vbuf = try self.allocator.alloc(f32, data_len);
                    @memcpy(std.mem.sliceAsBytes(vbuf), bytes[pos..][0 .. data_len * 4]);
                    pos += data_len * 4;
                    break :blk vbuf;
                } else &.{};

                const owned_name = try self.allocator.dupe(u8, name_buf);
                errdefer self.allocator.free(owned_name);
                const ps_gop = try self.optimizer_state.param_states.getOrPut(self.allocator, owned_name);
                if (ps_gop.found_existing) {
                    self.allocator.free(owned_name);
                    ps_gop.value_ptr.deinit();
                }
                ps_gop.value_ptr.* = .{
                    .m = m,
                    .v = v,
                    .allocator = self.allocator,
                };
            }
        }
    }

    fn appendU32(buf: *std.ArrayListUnmanaged(u8), allocator: std.mem.Allocator, val: u32) !void {
        var bytes: [4]u8 = undefined;
        std.mem.writeInt(u32, &bytes, val, .little);
        try buf.appendSlice(allocator, &bytes);
    }

    fn readU32(bytes: []const u8, pos: *usize) u32 {
        const val = std.mem.readInt(u32, bytes[pos.*..][0..4], .little);
        pos.* += 4;
        return val;
    }

    fn readU32Checked(bytes: []const u8, pos: *usize) !u32 {
        if (bytes.len - pos.* < 4) return error.InvalidCheckpoint;
        return readU32(bytes, pos);
    }

    fn refreshFlatState(self: *TrainingLoop) !void {
        if (self.flat_state) |*flat_state| flat_state.deinit();
        self.flat_state = try FlatTrainingState.initFromCurrentState(
            self.allocator,
            &self.weight_store,
            &self.optimizer_state,
            self.config.optimizer,
        );
    }
};

fn countTotalParamElements(weight_store: *const TrainingWeightStore, names: []const []const u8) usize {
    var total: usize = 0;
    for (names) |name| {
        if (weight_store.getWeight(name)) |param| total += param.len;
    }
    return total;
}

fn lessThanString(_: void, lhs: []const u8, rhs: []const u8) bool {
    return std.mem.lessThan(u8, lhs, rhs);
}

fn optimizerNeedsVariance(config: optimizers.Optimizer) bool {
    return switch (config) {
        .adam, .adamw, .schedule_free_adamw => true,
        .sgd => false,
    };
}

fn elapsedNs(start_ns: u64) u64 {
    const end_ns = nowNs();
    return end_ns - start_ns;
}

fn nowNs() u64 {
    var timespec: std.posix.timespec = undefined;
    switch (std.posix.errno(std.posix.system.clock_gettime(std.posix.CLOCK.MONOTONIC, &timespec))) {
        .SUCCESS => return @intCast(@as(i128, timespec.sec) * std.time.ns_per_s + timespec.nsec),
        else => return 0,
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

const Builder = ml.graph.Builder;
const native_mod = @import("../ops/native_compute.zig");
const NativeCompute = native_mod.NativeCompute;
const WeightStore = native_mod.WeightStore;

test "TrainingWeightStore round-trip" {
    const allocator = std.testing.allocator;

    var ws = TrainingWeightStore.init(allocator);
    defer ws.deinit();

    // Materialize from slice.
    const data = &[_]f32{ 1.0, 2.0, 3.0 };
    const w = try ws.materializeFromSlice("w", data);
    try std.testing.expectEqual(@as(usize, 3), w.len);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), w[0], 1e-6);

    // Modify in place.
    w[0] = 42.0;
    const w2 = ws.getWeight("w").?;
    try std.testing.expectApproxEqAbs(@as(f32, 42.0), w2[0], 1e-6);

    // setWeight overwrites.
    const new_data = &[_]f32{ 10.0, 20.0, 30.0 };
    try ws.setWeight("w", new_data);
    const w3 = ws.getWeight("w").?;
    try std.testing.expectApproxEqAbs(@as(f32, 10.0), w3[0], 1e-6);
}

test "TrainingLoop loss decreases" {
    const allocator = std.testing.allocator;

    // Build: y = Wx, loss = reduceSum(y)
    var g = Graph.init(allocator);
    defer g.deinit();
    var bld = Builder.init(&g);

    const x = try bld.parameter("x", Shape.init(.f32, &.{ 2, 3 }));
    const w = try bld.parameter("w", Shape.init(.f32, &.{ 4, 3 }));
    const bias = try bld.parameter("bias", Shape.init(.f32, &.{4}));
    const y = try bld.linear(x, w, bias, 2, 3, 4);
    const loss = try bld.reduceSum(y, &.{ 0, 1 });
    try g.markOutput(loss);

    // Set up native backend.
    var bws = WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    var compute = NativeCompute.init(allocator, &bws, null);
    var cb_val = compute.computeBackend();

    // Initialize training loop.
    var loop = TrainingLoop.init(allocator, .{
        .optimizer = .{ .sgd = .{} },
        .lr_schedule = .{ .constant = 0.01 },
    });
    defer loop.deinit();

    // Materialize initial weights.
    _ = try loop.weight_store.materializeFromSlice("x", &.{ 1, 1, 1, 1, 1, 1 });
    _ = try loop.weight_store.materializeFromSlice("w", &.{ 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2 });
    _ = try loop.weight_store.materializeFromSlice("bias", &.{ 0.01, 0.02, 0.03, 0.04 });

    // Train a few steps — loss should decrease.
    var prev_loss: f32 = std.math.inf(f32);
    for (0..5) |_| {
        const l = try loop.step(&g, loss, &cb_val);
        try std.testing.expect(l < prev_loss);
        prev_loss = l;
    }
}

test "checkpoint serialize and deserialize" {
    const allocator = std.testing.allocator;

    // Create and populate a training loop.
    var loop1 = TrainingLoop.init(allocator, .{
        .optimizer = .{ .sgd = .{} },
        .lr_schedule = .{ .constant = 0.01 },
    });
    defer loop1.deinit();

    _ = try loop1.weight_store.materializeFromSlice("w1", &.{ 1.0, 2.0, 3.0 });
    _ = try loop1.weight_store.materializeFromSlice("w2", &.{ 4.0, 5.0 });
    loop1.optimizer_state.step_count = 42;

    // Serialize to bytes.
    const bytes = try loop1.serializeCheckpoint(allocator);
    defer allocator.free(bytes);

    // Deserialize into a new loop.
    var loop2 = TrainingLoop.init(allocator, .{
        .optimizer = .{ .sgd = .{} },
        .lr_schedule = .{ .constant = 0.01 },
    });
    defer loop2.deinit();

    try loop2.loadCheckpointFromBytes(bytes);

    // Verify step count.
    try std.testing.expectEqual(@as(u32, 42), loop2.optimizer_state.step_count);

    // Verify weights restored.
    const w1 = loop2.weight_store.getWeight("w1");
    const w2 = loop2.weight_store.getWeight("w2");
    try std.testing.expect(w1 != null);
    try std.testing.expect(w2 != null);

    try std.testing.expectEqual(@as(usize, 3), w1.?.len);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), w1.?[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 3.0), w1.?[2], 1e-6);

    try std.testing.expectEqual(@as(usize, 2), w2.?.len);
    try std.testing.expectApproxEqAbs(@as(f32, 4.0), w2.?[0], 1e-6);
}

test "flat training state mirrors weight store and optimizer state" {
    const allocator = std.testing.allocator;

    var loop = TrainingLoop.init(allocator, .{
        .optimizer = .{ .adamw = .{} },
        .lr_schedule = .{ .constant = 0.01 },
    });
    defer loop.deinit();

    _ = try loop.weight_store.materializeFromSlice("b", &.{ 3.0, 4.0 });
    _ = try loop.weight_store.materializeFromSlice("a", &.{ 1.0, 2.0, 5.0 });
    loop.optimizer_state.step_count = 7;
    {
        const ps_a = try loop.optimizer_state.getOrCreate("a", 3, true);
        @memcpy(ps_a.m, &[_]f32{ 0.1, 0.2, 0.3 });
        @memcpy(ps_a.v, &[_]f32{ 0.4, 0.5, 0.6 });
    }

    try loop.refreshFlatState();

    try std.testing.expect(loop.flat_state != null);
    const flat = &loop.flat_state.?;
    try std.testing.expectEqual(@as(usize, 2), flat.layouts.len);
    try std.testing.expectEqualStrings("a", flat.layouts[0].name);
    try std.testing.expectEqualStrings("b", flat.layouts[1].name);
    try std.testing.expectEqual(@as(usize, 5), flat.total_param_elements);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), flat.params[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.1), flat.m[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.6), flat.v[2], 1e-6);
}

test "training loop exposes checkpoint summary and timing metrics" {
    const allocator = std.testing.allocator;

    var g = Graph.init(allocator);
    defer g.deinit();
    var bld = Builder.init(&g);

    const x = try bld.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const w1 = try bld.parameter("w1", Shape.init(.f32, &.{ 4, 4 }));
    const w2 = try bld.parameter("w2", Shape.init(.f32, &.{ 4, 4 }));
    const y1 = try bld.linearNoBias(x, w1, 2, 4, 4);
    const y2 = try bld.linearNoBias(y1, w2, 2, 4, 4);
    const loss = try bld.reduceSum(y2, &.{ 0, 1 });
    try g.markOutput(loss);

    var bws = WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    var compute = NativeCompute.init(allocator, &bws, null);
    var cb_val = compute.computeBackend();

    var loop = TrainingLoop.init(allocator, .{
        .optimizer = .{ .sgd = .{} },
        .lr_schedule = .{ .constant = 0.01 },
        .checkpoint_config = .{ .strategy = .every_n_layers, .layer_interval = 1 },
        .emit_checkpoint_analysis = true,
    });
    defer loop.deinit();

    _ = try loop.weight_store.materializeFromSlice("x", &.{ 1, 1, 1, 1, 1, 1, 1, 1 });
    _ = try loop.weight_store.materializeFromSlice("w1", &.{ 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6 });
    _ = try loop.weight_store.materializeFromSlice("w2", &.{ 0.2, 0.1, 0.4, 0.3, 0.6, 0.5, 0.8, 0.7, 1.0, 0.9, 1.2, 1.1, 1.4, 1.3, 1.6, 1.5 });

    _ = try loop.step(&g, loss, &cb_val);

    try std.testing.expect(loop.last_step_metrics.runtime_input_build_ns > 0);
    try std.testing.expect(loop.last_step_metrics.total_ns > 0);
    try std.testing.expect(loop.last_step_metrics.train_step.checkpoint_ns > 0);
    try std.testing.expect(loop.last_step_metrics.checkpoint_summary != null);
    try std.testing.expect(loop.last_step_metrics.flat_param_count == 3);
}
