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
const compat = @import("../io/compat.zig");
const c_file = @import("../util/c_file.zig");
const safetensors = @import("../models/safetensors.zig");
const weight_source = @import("../models/weight_source.zig");

pub const trainable_token_adapter_config_file_name = "trainable_token_adapter_config.json";
pub const trainable_token_adapter_checkpoint_file_name = "trainable_token_adapter.safetensors";
pub const trainable_token_adapter_tensor_name = "trainable_token_adapter.embedding_delta";
pub const trainable_token_adapter_family_version = "trainable_token_indices/v1alpha1";

pub const TargetPreset = enum {
    all_linear,
    attention_only,
    mlp_only,
    moe_experts,
};

pub const all_linear_patterns: []const []const u8 = &.{
    "q_proj",     "k_proj",   "v_proj",     "o_proj",
    "query_proj", "key_proj", "value_proj", "out_proj",
    "gate_proj",  "up_proj",  "down_proj",  "wi",
    "wo",         "dense",    "fc1",        "fc2",
};

pub const attention_only_patterns: []const []const u8 = &.{
    "q_proj",               "k_proj",             "v_proj",               "o_proj",
    "query_proj",           "key_proj",           "value_proj",           "out_proj",
    "attention.self.query", "attention.self.key", "attention.self.value", "attention.output.dense",
};

pub const mlp_only_patterns: []const []const u8 = &.{
    "gate_proj", "up_proj", "down_proj", "wi", "wo", "intermediate.dense", "output.dense", "fc1", "fc2",
};

pub const moe_expert_patterns: []const []const u8 = &.{
    "experts.", ".experts.", "feed_forward.experts", "block_sparse_moe.experts",
    "experts/", "/experts/", "feed_forward/experts", "block_sparse_moe/experts",
    ".moe.",    ".moe/",     "ffn.experts",          "mlp.experts",
};

pub const moe_expert_weight_suffixes: []const []const u8 = &.{
    ".w1.weight",
    ".w2.weight",
    ".w3.weight",
    ".gate_proj.weight",
    ".up_proj.weight",
    ".down_proj.weight",
    ".gate_up_proj.weight",
    "/w1/weight",
    "/w2/weight",
    "/w3/weight",
    "/gate_proj/weight",
    "/up_proj/weight",
    "/down_proj/weight",
    "/gate_up_proj/weight",
};

pub fn parseTargetPreset(name: []const u8) ?TargetPreset {
    if (std.mem.eql(u8, name, "all-linear")) return .all_linear;
    if (std.mem.eql(u8, name, "attention-only")) return .attention_only;
    if (std.mem.eql(u8, name, "mlp-only")) return .mlp_only;
    if (std.mem.eql(u8, name, "moe-experts")) return .moe_experts;
    if (std.mem.eql(u8, name, "moe")) return .moe_experts;
    return null;
}

pub fn targetPresetPatterns(preset: TargetPreset) []const []const u8 {
    return switch (preset) {
        .all_linear => all_linear_patterns,
        .attention_only => attention_only_patterns,
        .mlp_only => mlp_only_patterns,
        .moe_experts => moe_expert_patterns,
    };
}

pub fn matchesTargetPreset(tensor_name: []const u8, preset: TargetPreset) bool {
    if (preset == .moe_experts) return matchesMoEExpertTensor(tensor_name);
    return matchesAnyPattern(tensor_name, targetPresetPatterns(preset));
}

pub fn matchesAnyPattern(tensor_name: []const u8, patterns: []const []const u8) bool {
    for (patterns) |pattern| {
        if (std.mem.indexOf(u8, tensor_name, pattern) != null) return true;
    }
    return false;
}

pub fn matchesMoEExpertTensor(tensor_name: []const u8) bool {
    if (!matchesAnyPattern(tensor_name, moe_expert_patterns)) return false;
    for (moe_expert_weight_suffixes) |suffix| {
        if (std.mem.endsWith(u8, tensor_name, suffix)) return true;
    }
    return false;
}

pub const CompositionInput = struct {
    path: []const u8,
    weight: f32 = 1.0,
};

pub const CompositionSummary = struct {
    output_path: []const u8,
    adapter_count: usize,
    tensor_count: usize,
    parameter_count: usize,
    eval_exit_code: ?u8 = null,
};

pub const EvalRun = struct {
    phase: []const u8,
    term: []const u8,
    exit_code: ?u8 = null,
    signal: ?u32 = null,
    stdout: []const u8,
    stderr: []const u8,
    success: bool,

    pub fn deinit(self: *EvalRun, allocator: std.mem.Allocator) void {
        allocator.free(self.stdout);
        allocator.free(self.stderr);
        self.* = undefined;
    }
};

pub const AdapterTensor = struct {
    name: []const u8,
    data: []f32,
    shape: []usize,

    pub fn deinit(self: *AdapterTensor, allocator: std.mem.Allocator) void {
        allocator.free(self.name);
        allocator.free(self.data);
        allocator.free(self.shape);
    }
};

pub const WriteTensorF32 = struct {
    name: []const u8,
    data: []const f32,
    shape: []const usize,
};

pub const RuntimeAdapter = struct {
    id: []const u8,
    path: []const u8,
    tensors: []AdapterTensor,

    fn deinit(self: *RuntimeAdapter, allocator: std.mem.Allocator) void {
        allocator.free(self.id);
        allocator.free(self.path);
        for (self.tensors) |*tensor| tensor.deinit(allocator);
        allocator.free(self.tensors);
        self.* = undefined;
    }
};

pub const ActiveAdapter = struct {
    adapter_index: usize,
    weight: f32 = 1.0,
};

pub const CombinedAdapter = struct {
    tensors: []AdapterTensor,
    parameter_count: usize,

    pub fn deinit(self: *CombinedAdapter, allocator: std.mem.Allocator) void {
        for (self.tensors) |*tensor| tensor.deinit(allocator);
        allocator.free(self.tensors);
        self.* = undefined;
    }

    pub fn tensorByName(self: *const CombinedAdapter, name: []const u8) ?*const AdapterTensor {
        for (self.tensors) |*tensor| {
            if (std.mem.eql(u8, tensor.name, name)) return tensor;
        }
        return null;
    }
};

pub const TrainableTokenAdapterConfig = struct {
    artifact_family_version: []const u8 = trainable_token_adapter_family_version,
    base_embedding_tensor_name: []const u8,
    token_indices: []const u32,
    embedding_dim: usize,
    token_count: usize,
};

pub const TrainableTokenAdapterSummary = struct {
    artifact_family_version: []const u8,
    adapter_dir: []const u8,
    config_path: []const u8,
    checkpoint_path: []const u8,
    base_embedding_tensor_name: []const u8,
    token_count: usize,
    embedding_dim: usize,
    parameter_count: usize,
};

pub const LoadedTrainableTokenAdapter = struct {
    allocator: std.mem.Allocator,
    artifact_family_version: []const u8,
    adapter_dir: []const u8,
    config_path: []const u8,
    checkpoint_path: []const u8,
    base_embedding_tensor_name: []const u8,
    token_indices: []u32,
    embedding_dim: usize,
    deltas: []f32,

    pub fn deinit(self: *LoadedTrainableTokenAdapter) void {
        self.allocator.free(self.artifact_family_version);
        self.allocator.free(self.adapter_dir);
        self.allocator.free(self.config_path);
        self.allocator.free(self.checkpoint_path);
        self.allocator.free(self.base_embedding_tensor_name);
        self.allocator.free(self.token_indices);
        self.allocator.free(self.deltas);
        self.* = undefined;
    }

    pub fn tokenCount(self: *const LoadedTrainableTokenAdapter) usize {
        return self.token_indices.len;
    }

    pub fn parameterCount(self: *const LoadedTrainableTokenAdapter) usize {
        return self.deltas.len;
    }
};

pub const RuntimeAdapterRegistry = struct {
    allocator: std.mem.Allocator,
    adapters: std.ArrayListUnmanaged(RuntimeAdapter) = .empty,
    active: std.ArrayListUnmanaged(ActiveAdapter) = .empty,

    pub fn init(allocator: std.mem.Allocator) RuntimeAdapterRegistry {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *RuntimeAdapterRegistry) void {
        for (self.adapters.items) |*adapter| adapter.deinit(self.allocator);
        self.adapters.deinit(self.allocator);
        self.active.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn loadSafetensors(self: *RuntimeAdapterRegistry, id: []const u8, path: []const u8) !void {
        if (self.findAdapterIndex(id) != null) return error.DuplicateAdapterId;
        const tensors = try loadAdapterTensors(self.allocator, path);
        errdefer {
            for (tensors) |*tensor| tensor.deinit(self.allocator);
            self.allocator.free(tensors);
        }
        const owned_id = try self.allocator.dupe(u8, id);
        errdefer self.allocator.free(owned_id);
        const owned_path = try self.allocator.dupe(u8, path);
        errdefer self.allocator.free(owned_path);
        try self.adapters.append(self.allocator, .{
            .id = owned_id,
            .path = owned_path,
            .tensors = tensors,
        });
    }

    pub fn select(self: *RuntimeAdapterRegistry, id: []const u8) !void {
        const idx = self.findAdapterIndex(id) orelse return error.UnknownAdapterId;
        self.active.clearRetainingCapacity();
        try self.active.append(self.allocator, .{ .adapter_index = idx, .weight = 1.0 });
    }

    pub fn disable(self: *RuntimeAdapterRegistry) void {
        self.active.clearRetainingCapacity();
    }

    pub fn setActive(self: *RuntimeAdapterRegistry, id: []const u8, weight: f32) !void {
        const idx = self.findAdapterIndex(id) orelse return error.UnknownAdapterId;
        for (self.active.items) |*active| {
            if (active.adapter_index == idx) {
                active.weight = weight;
                return;
            }
        }
        try self.active.append(self.allocator, .{ .adapter_index = idx, .weight = weight });
    }

    pub fn removeActive(self: *RuntimeAdapterRegistry, id: []const u8) !void {
        const idx = self.findAdapterIndex(id) orelse return error.UnknownAdapterId;
        for (self.active.items, 0..) |active, active_idx| {
            if (active.adapter_index == idx) {
                _ = self.active.swapRemove(active_idx);
                return;
            }
        }
    }

    pub fn activeCount(self: *const RuntimeAdapterRegistry) usize {
        return self.active.items.len;
    }

    pub fn combineActive(self: *const RuntimeAdapterRegistry) !CombinedAdapter {
        if (self.active.items.len == 0) return .{
            .tensors = try self.allocator.alloc(AdapterTensor, 0),
            .parameter_count = 0,
        };

        var index = std.StringHashMapUnmanaged(usize){};
        defer index.deinit(self.allocator);
        var tensors = std.ArrayListUnmanaged(AdapterTensor).empty;
        errdefer {
            for (tensors.items) |*tensor| tensor.deinit(self.allocator);
            tensors.deinit(self.allocator);
        }

        for (self.active.items) |active| {
            const adapter = self.adapters.items[active.adapter_index];
            for (adapter.tensors) |tensor| {
                if (index.get(tensor.name)) |tensor_idx| {
                    const out = &tensors.items[tensor_idx];
                    if (!sameShapeUsize(out.shape, tensor.shape)) return error.AdapterShapeMismatch;
                    if (out.data.len != tensor.data.len) return error.AdapterShapeMismatch;
                    for (tensor.data, 0..) |value, i| out.data[i] += active.weight * value;
                } else {
                    const owned_name = try self.allocator.dupe(u8, tensor.name);
                    errdefer self.allocator.free(owned_name);
                    const owned_shape = try self.allocator.dupe(usize, tensor.shape);
                    errdefer self.allocator.free(owned_shape);
                    const owned_data = try self.allocator.alloc(f32, tensor.data.len);
                    errdefer self.allocator.free(owned_data);
                    for (tensor.data, 0..) |value, i| owned_data[i] = active.weight * value;
                    try index.put(self.allocator, owned_name, tensors.items.len);
                    try tensors.append(self.allocator, .{
                        .name = owned_name,
                        .shape = owned_shape,
                        .data = owned_data,
                    });
                }
            }
        }

        var parameter_count: usize = 0;
        for (tensors.items) |tensor| parameter_count += tensor.data.len;
        return .{
            .tensors = try tensors.toOwnedSlice(self.allocator),
            .parameter_count = parameter_count,
        };
    }

    fn findAdapterIndex(self: *const RuntimeAdapterRegistry, id: []const u8) ?usize {
        for (self.adapters.items, 0..) |adapter, idx| {
            if (std.mem.eql(u8, adapter.id, id)) return idx;
        }
        return null;
    }
};

pub fn saveTrainableTokenAdapter(
    allocator: std.mem.Allocator,
    io: std.Io,
    out_dir: []const u8,
    base_embedding_tensor_name: []const u8,
    token_indices: []const u32,
    embedding_dim: usize,
    deltas: []const f32,
) !TrainableTokenAdapterSummary {
    if (embedding_dim == 0) return error.InvalidAdapterTensorShape;
    if (token_indices.len == 0) return error.NoTokens;
    if (deltas.len != token_indices.len * embedding_dim) return error.InvalidAdapterTensorShape;
    try validateUniqueTokenIndices(token_indices);

    try compat.cwd().createDirPath(io, out_dir);
    const config_path = try std.fs.path.join(allocator, &.{ out_dir, trainable_token_adapter_config_file_name });
    errdefer allocator.free(config_path);
    const checkpoint_path = try std.fs.path.join(allocator, &.{ out_dir, trainable_token_adapter_checkpoint_file_name });
    errdefer allocator.free(checkpoint_path);

    var buffer: std.Io.Writer.Allocating = .init(allocator);
    defer buffer.deinit();
    try std.json.Stringify.value(TrainableTokenAdapterConfig{
        .artifact_family_version = trainable_token_adapter_family_version,
        .base_embedding_tensor_name = base_embedding_tensor_name,
        .token_indices = token_indices,
        .embedding_dim = embedding_dim,
        .token_count = token_indices.len,
    }, .{ .whitespace = .indent_2 }, &buffer.writer);
    try writeFileAtomic(allocator, io, config_path, buffer.written());

    var shape = [_]usize{ token_indices.len, embedding_dim };
    const tensors = [_]WriteTensorF32{.{
        .name = trainable_token_adapter_tensor_name,
        .shape = shape[0..],
        .data = deltas,
    }};
    try saveF32Safetensors(allocator, io, checkpoint_path, &tensors);

    return .{
        .artifact_family_version = try allocator.dupe(u8, trainable_token_adapter_family_version),
        .adapter_dir = try allocator.dupe(u8, out_dir),
        .config_path = config_path,
        .checkpoint_path = checkpoint_path,
        .base_embedding_tensor_name = try allocator.dupe(u8, base_embedding_tensor_name),
        .token_count = token_indices.len,
        .embedding_dim = embedding_dim,
        .parameter_count = deltas.len,
    };
}

pub fn inspectTrainableTokenAdapter(
    allocator: std.mem.Allocator,
    adapter_dir: []const u8,
) !TrainableTokenAdapterSummary {
    var loaded = try loadTrainableTokenAdapter(allocator, adapter_dir);
    defer loaded.deinit();
    return .{
        .artifact_family_version = try allocator.dupe(u8, loaded.artifact_family_version),
        .adapter_dir = try allocator.dupe(u8, loaded.adapter_dir),
        .config_path = try allocator.dupe(u8, loaded.config_path),
        .checkpoint_path = try allocator.dupe(u8, loaded.checkpoint_path),
        .base_embedding_tensor_name = try allocator.dupe(u8, loaded.base_embedding_tensor_name),
        .token_count = loaded.tokenCount(),
        .embedding_dim = loaded.embedding_dim,
        .parameter_count = loaded.parameterCount(),
    };
}

pub fn freeTrainableTokenAdapterSummary(allocator: std.mem.Allocator, summary: *TrainableTokenAdapterSummary) void {
    allocator.free(summary.artifact_family_version);
    allocator.free(summary.adapter_dir);
    allocator.free(summary.config_path);
    allocator.free(summary.checkpoint_path);
    allocator.free(summary.base_embedding_tensor_name);
    summary.* = undefined;
}

pub fn loadTrainableTokenAdapter(
    allocator: std.mem.Allocator,
    adapter_dir: []const u8,
) !LoadedTrainableTokenAdapter {
    const config_path = try std.fs.path.join(allocator, &.{ adapter_dir, trainable_token_adapter_config_file_name });
    errdefer allocator.free(config_path);
    const checkpoint_path = try std.fs.path.join(allocator, &.{ adapter_dir, trainable_token_adapter_checkpoint_file_name });
    errdefer allocator.free(checkpoint_path);

    const config_bytes = try c_file.readFile(allocator, config_path);
    defer allocator.free(config_bytes);
    var parsed = try std.json.parseFromSlice(TrainableTokenAdapterConfig, allocator, config_bytes, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    const cfg = parsed.value;
    if (!std.mem.eql(u8, cfg.artifact_family_version, trainable_token_adapter_family_version)) return error.UnsupportedAdapterFormat;
    if (cfg.embedding_dim == 0 or cfg.token_indices.len == 0 or cfg.token_count != cfg.token_indices.len) return error.InvalidAdapterTensorShape;
    try validateUniqueTokenIndices(cfg.token_indices);

    const tensors = try loadAdapterTensors(allocator, checkpoint_path);
    defer {
        for (tensors) |*tensor| tensor.deinit(allocator);
        allocator.free(tensors);
    }
    if (tensors.len != 1) return error.InvalidAdapterTensorShape;
    const delta_tensor = tensors[0];
    if (!std.mem.eql(u8, delta_tensor.name, trainable_token_adapter_tensor_name)) return error.TensorNotFound;
    if (delta_tensor.shape.len != 2 or delta_tensor.shape[0] != cfg.token_indices.len or delta_tensor.shape[1] != cfg.embedding_dim) return error.InvalidAdapterTensorShape;

    return .{
        .allocator = allocator,
        .artifact_family_version = try allocator.dupe(u8, cfg.artifact_family_version),
        .adapter_dir = try allocator.dupe(u8, adapter_dir),
        .config_path = config_path,
        .checkpoint_path = checkpoint_path,
        .base_embedding_tensor_name = try allocator.dupe(u8, cfg.base_embedding_tensor_name),
        .token_indices = try allocator.dupe(u32, cfg.token_indices),
        .embedding_dim = cfg.embedding_dim,
        .deltas = try allocator.dupe(f32, delta_tensor.data),
    };
}

pub fn applyTrainableTokenAdapter(
    allocator: std.mem.Allocator,
    base_embedding: []const f32,
    vocab_size: usize,
    embedding_dim: usize,
    adapter: *const LoadedTrainableTokenAdapter,
) ![]f32 {
    if (embedding_dim == 0 or base_embedding.len != vocab_size * embedding_dim) return error.InvalidAdapterTensorShape;
    if (adapter.embedding_dim != embedding_dim or adapter.deltas.len != adapter.token_indices.len * embedding_dim) return error.InvalidAdapterTensorShape;
    try validateUniqueTokenIndices(adapter.token_indices);
    const merged = try allocator.dupe(f32, base_embedding);
    errdefer allocator.free(merged);
    for (adapter.token_indices, 0..) |token_index, row_idx| {
        const token_usize: usize = @intCast(token_index);
        if (token_usize >= vocab_size) return error.TokenIndexOutOfRange;
        const dst = merged[token_usize * embedding_dim .. (token_usize + 1) * embedding_dim];
        const src = adapter.deltas[row_idx * embedding_dim .. (row_idx + 1) * embedding_dim];
        for (src, 0..) |delta, dim| dst[dim] += delta;
    }
    return merged;
}

fn loadAdapterTensors(allocator: std.mem.Allocator, path: []const u8) ![]AdapterTensor {
    var reader = try safetensors.MMapReader.openFileAbsolute(allocator, path);
    defer reader.deinit();

    const names = try reader.header.tensorNames(allocator);
    defer allocator.free(names);
    std.mem.sort([]const u8, names, {}, struct {
        fn lessThan(_: void, lhs: []const u8, rhs: []const u8) bool {
            return std.mem.lessThan(u8, lhs, rhs);
        }
    }.lessThan);

    var tensors = std.ArrayListUnmanaged(AdapterTensor).empty;
    errdefer {
        for (tensors.items) |*tensor| tensor.deinit(allocator);
        tensors.deinit(allocator);
    }

    for (names) |name| {
        const meta = reader.header.tensors.get(name) orelse return error.TensorNotFound;
        if (meta.dtype != .f32 and meta.dtype != .f16 and meta.dtype != .bf16) return error.UnsupportedAdapterDType;
        var raw = try reader.readTensor(name);
        defer raw.deinit();
        var tensor = if (raw.dtype == .f32) raw else try weight_source.convertToF32(allocator, &raw);
        defer if (raw.dtype != .f32) tensor.deinit();

        const values = try copyF32TensorData(allocator, tensor.data);
        defer allocator.free(values);
        try tensors.append(allocator, .{
            .name = try allocator.dupe(u8, name),
            .shape = try shapeToUsize(allocator, meta.shape),
            .data = try allocator.dupe(f32, values),
        });
    }

    return try tensors.toOwnedSlice(allocator);
}

fn copyF32TensorData(allocator: std.mem.Allocator, data: []const u8) ![]f32 {
    if (data.len % @sizeOf(f32) != 0) return error.InvalidTensorData;
    const values = try allocator.alloc(f32, data.len / @sizeOf(f32));
    errdefer allocator.free(values);
    for (values, 0..) |*value, idx| {
        const start = idx * @sizeOf(f32);
        const bits = std.mem.readInt(u32, data[start .. start + @sizeOf(f32)][0..4], .little);
        value.* = @bitCast(bits);
    }
    return values;
}

pub fn composeAdapterSafetensors(
    allocator: std.mem.Allocator,
    io: std.Io,
    inputs: []const CompositionInput,
    output_path: []const u8,
    eval_program: ?[]const u8,
) !CompositionSummary {
    if (inputs.len == 0) return error.NoAdapters;

    var composed = std.StringHashMapUnmanaged(usize){};
    defer composed.deinit(allocator);
    var tensors = std.ArrayListUnmanaged(AdapterTensor).empty;
    errdefer {
        for (tensors.items) |*tensor| tensor.deinit(allocator);
        tensors.deinit(allocator);
    }
    defer {
        for (tensors.items) |*tensor| tensor.deinit(allocator);
        tensors.deinit(allocator);
    }

    for (inputs) |input| {
        var reader = try safetensors.MMapReader.openFileAbsolute(allocator, input.path);
        defer reader.deinit();

        const names = try reader.header.tensorNames(allocator);
        defer allocator.free(names);
        std.mem.sort([]const u8, names, {}, struct {
            fn lessThan(_: void, lhs: []const u8, rhs: []const u8) bool {
                return std.mem.lessThan(u8, lhs, rhs);
            }
        }.lessThan);

        for (names) |name| {
            const meta = reader.header.tensors.get(name) orelse return error.TensorNotFound;
            if (meta.dtype != .f32 and meta.dtype != .f16 and meta.dtype != .bf16) return error.UnsupportedAdapterDType;
            var raw = try reader.readTensor(name);
            defer raw.deinit();
            var tensor = if (raw.dtype == .f32) raw else try weight_source.convertToF32(allocator, &raw);
            defer if (raw.dtype != .f32) tensor.deinit();

            const values = try copyF32TensorData(allocator, tensor.data);
            defer allocator.free(values);
            if (composed.get(name)) |idx| {
                if (!sameShape(tensors.items[idx].shape, meta.shape)) return error.AdapterShapeMismatch;
                if (tensors.items[idx].data.len != values.len) return error.AdapterShapeMismatch;
                for (values, 0..) |v, i| tensors.items[idx].data[i] += input.weight * v;
            } else {
                const owned_name = try allocator.dupe(u8, name);
                errdefer allocator.free(owned_name);
                const owned_shape = try shapeToUsize(allocator, meta.shape);
                errdefer allocator.free(owned_shape);
                const owned_data = try allocator.alloc(f32, values.len);
                errdefer allocator.free(owned_data);
                for (values, 0..) |v, i| owned_data[i] = input.weight * v;
                try composed.put(allocator, owned_name, tensors.items.len);
                try tensors.append(allocator, .{
                    .name = owned_name,
                    .data = owned_data,
                    .shape = owned_shape,
                });
            }
        }
    }

    const eval_exit = if (eval_program) |program| try runEvalProgram(allocator, io, program) else null;

    var parameter_count: usize = 0;
    for (tensors.items) |item| {
        parameter_count += item.data.len;
    }
    try saveF32Safetensors(allocator, io, output_path, tensors.items);

    return .{
        .output_path = output_path,
        .adapter_count = inputs.len,
        .tensor_count = tensors.items.len,
        .parameter_count = parameter_count,
        .eval_exit_code = eval_exit,
    };
}

fn saveF32Safetensors(
    allocator: std.mem.Allocator,
    io: std.Io,
    path: []const u8,
    tensors: anytype,
) !void {
    var header_buf: std.Io.Writer.Allocating = .init(allocator);
    defer header_buf.deinit();
    const writer = &header_buf.writer;
    try writer.writeAll("{\"__metadata__\":{\"format\":\"pt\"}");
    var offset: u64 = 0;
    for (tensors) |tensor| {
        const byte_len: u64 = @as(u64, tensor.data.len) * @sizeOf(f32);
        try writer.writeAll(",\"");
        try writeJsonEscaped(writer, tensor.name);
        try writer.writeAll("\":{\"dtype\":\"F32\",\"shape\":[");
        for (tensor.shape, 0..) |dim, dim_idx| {
            if (dim_idx != 0) try writer.writeByte(',');
            try writer.print("{}", .{dim});
        }
        try writer.print("],\"data_offsets\":[{},{}]}}", .{ offset, offset + byte_len });
        offset += byte_len;
    }
    try writer.writeByte('}');

    const tmp_path = try std.fmt.allocPrint(allocator, "{s}.tmp", .{path});
    defer allocator.free(tmp_path);
    std.Io.Dir.cwd().deleteFile(io, tmp_path) catch {};
    var file = try std.Io.Dir.cwd().createFile(io, tmp_path, .{ .truncate = true });
    var closed = false;
    errdefer if (!closed) file.close(io);
    var len_buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &len_buf, header_buf.written().len, .little);
    try file.writeStreamingAll(io, &len_buf);
    try file.writeStreamingAll(io, header_buf.written());
    for (tensors) |tensor| try file.writeStreamingAll(io, std.mem.sliceAsBytes(tensor.data));
    file.close(io);
    closed = true;
    try std.Io.Dir.rename(std.Io.Dir.cwd(), tmp_path, std.Io.Dir.cwd(), path, io);
}

fn writeFileAtomic(allocator: std.mem.Allocator, io: std.Io, path: []const u8, data: []const u8) !void {
    const tmp_path = try std.fmt.allocPrint(allocator, "{s}.tmp", .{path});
    defer allocator.free(tmp_path);
    std.Io.Dir.cwd().deleteFile(io, tmp_path) catch {};
    try compat.cwd().writeFile(io, .{ .sub_path = tmp_path, .data = data });
    try std.Io.Dir.rename(std.Io.Dir.cwd(), tmp_path, std.Io.Dir.cwd(), path, io);
}

fn validateUniqueTokenIndices(token_indices: []const u32) !void {
    for (token_indices, 0..) |token, idx| {
        for (token_indices[0..idx]) |previous| {
            if (previous == token) return error.DuplicateTokenIndex;
        }
    }
}

fn writeJsonEscaped(writer: *std.Io.Writer, text: []const u8) !void {
    for (text) |c| {
        switch (c) {
            '"' => try writer.writeAll("\\\""),
            '\\' => try writer.writeAll("\\\\"),
            else => try writer.writeByte(c),
        }
    }
}

fn runEvalProgram(allocator: std.mem.Allocator, io: std.Io, program: []const u8) !u8 {
    var result = try runEvalCapture(allocator, io, program, "eval");
    defer result.deinit(allocator);
    return if (result.success) result.exit_code orelse 0 else error.EvalFailed;
}

pub fn runEvalCapture(
    allocator: std.mem.Allocator,
    io: std.Io,
    program: []const u8,
    phase: []const u8,
) !EvalRun {
    const result = try std.process.run(allocator, io, .{ .argv = &.{program} });
    errdefer allocator.free(result.stdout);
    errdefer allocator.free(result.stderr);
    return switch (result.term) {
        .exited => |code| .{
            .phase = phase,
            .term = "exited",
            .exit_code = code,
            .stdout = result.stdout,
            .stderr = result.stderr,
            .success = code == 0,
        },
        .signal => |sig| .{
            .phase = phase,
            .term = "signal",
            .signal = @intFromEnum(sig),
            .stdout = result.stdout,
            .stderr = result.stderr,
            .success = false,
        },
        .stopped => |sig| .{
            .phase = phase,
            .term = "stopped",
            .signal = @intFromEnum(sig),
            .stdout = result.stdout,
            .stderr = result.stderr,
            .success = false,
        },
        .unknown => |code| .{
            .phase = phase,
            .term = "unknown",
            .signal = code,
            .stdout = result.stdout,
            .stderr = result.stderr,
            .success = false,
        },
    };
}

fn sameShape(lhs: []const usize, rhs: []const i64) bool {
    if (lhs.len != rhs.len) return false;
    for (lhs, rhs) |l, r| {
        if (l != @as(usize, @intCast(r))) return false;
    }
    return true;
}

fn sameShapeUsize(lhs: []const usize, rhs: []const usize) bool {
    if (lhs.len != rhs.len) return false;
    for (lhs, rhs) |l, r| {
        if (l != r) return false;
    }
    return true;
}

fn shapeToUsize(allocator: std.mem.Allocator, shape: []const i64) ![]usize {
    const out = try allocator.alloc(usize, shape.len);
    for (shape, 0..) |dim, i| out[i] = @intCast(dim);
    return out;
}

test "target presets match common transformer and MoE names" {
    try std.testing.expect(matchesTargetPreset("model.layers.0.self_attn.q_proj.weight", .attention_only));
    try std.testing.expect(matchesTargetPreset("model.layers.0.mlp.down_proj.weight", .mlp_only));
    try std.testing.expect(matchesTargetPreset("model.layers.0.block_sparse_moe.experts.3.w2.weight", .moe_experts));
    try std.testing.expect(matchesTargetPreset("model.layers.1.mlp.experts.12.gate_up_proj.weight", .moe_experts));
    try std.testing.expect(matchesTargetPreset("model.layers.1/feed_forward/experts/12/down_proj/weight", .moe_experts));
    try std.testing.expect(!matchesTargetPreset("model.layers.0.mlp.down_proj.weight", .moe_experts));
    try std.testing.expect(!matchesTargetPreset("model.layers.0.router.weight", .moe_experts));
    try std.testing.expect(!matchesTargetPreset("model.embed_tokens.weight", .all_linear));
}

test "parse target presets" {
    try std.testing.expectEqual(TargetPreset.all_linear, parseTargetPreset("all-linear").?);
    try std.testing.expectEqual(TargetPreset.attention_only, parseTargetPreset("attention-only").?);
    try std.testing.expectEqual(TargetPreset.mlp_only, parseTargetPreset("mlp-only").?);
    try std.testing.expectEqual(TargetPreset.moe_experts, parseTargetPreset("moe-experts").?);
    try std.testing.expect(parseTargetPreset("unknown") == null);
}

test "runtime adapter registry selects disables and weighted-combines adapters" {
    const allocator = std.testing.allocator;
    const root = try std.fmt.allocPrint(allocator, "/tmp/termite_peft_runtime_registry_test_{d}", .{std.posix.system.getpid()});
    defer allocator.free(root);
    compat.cwd().deleteTree(compat.io(), root) catch {};
    try compat.cwd().createDirPath(compat.io(), root);
    defer compat.cwd().deleteTree(compat.io(), root) catch {};

    const adapter_a_path = try std.fs.path.join(allocator, &.{ root, "adapter_a.safetensors" });
    defer allocator.free(adapter_a_path);
    const adapter_b_path = try std.fs.path.join(allocator, &.{ root, "adapter_b.safetensors" });
    defer allocator.free(adapter_b_path);

    var a_shape = [_]usize{2};
    var a_lora_a = [_]f32{ 1.0, 2.0 };
    var a_lora_b = [_]f32{ 3.0, 4.0 };
    const adapter_a_tensors = [_]AdapterTensor{
        .{ .name = "layer.lora_A.weight", .shape = a_shape[0..], .data = a_lora_a[0..] },
        .{ .name = "layer.lora_B.weight", .shape = a_shape[0..], .data = a_lora_b[0..] },
    };
    try saveF32Safetensors(allocator, compat.io(), adapter_a_path, &adapter_a_tensors);

    var b_shape = [_]usize{2};
    var b_lora_a = [_]f32{ 10.0, 20.0 };
    var b_extra = [_]f32{ 5.0, 7.0 };
    const adapter_b_tensors = [_]AdapterTensor{
        .{ .name = "layer.lora_A.weight", .shape = b_shape[0..], .data = b_lora_a[0..] },
        .{ .name = "other.lora_A.weight", .shape = b_shape[0..], .data = b_extra[0..] },
    };
    try saveF32Safetensors(allocator, compat.io(), adapter_b_path, &adapter_b_tensors);

    var registry = RuntimeAdapterRegistry.init(allocator);
    defer registry.deinit();
    try registry.loadSafetensors("a", adapter_a_path);
    try registry.loadSafetensors("b", adapter_b_path);
    try std.testing.expectError(error.DuplicateAdapterId, registry.loadSafetensors("a", adapter_a_path));

    try registry.select("a");
    try std.testing.expectEqual(@as(usize, 1), registry.activeCount());
    var selected = try registry.combineActive();
    defer selected.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 4), selected.parameter_count);
    try std.testing.expectEqualSlices(f32, &.{ 1.0, 2.0 }, selected.tensorByName("layer.lora_A.weight").?.data);

    try registry.setActive("b", 0.5);
    try std.testing.expectEqual(@as(usize, 2), registry.activeCount());
    var combined = try registry.combineActive();
    defer combined.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 6), combined.parameter_count);
    try std.testing.expectEqualSlices(f32, &.{ 6.0, 12.0 }, combined.tensorByName("layer.lora_A.weight").?.data);
    try std.testing.expectEqualSlices(f32, &.{ 3.0, 4.0 }, combined.tensorByName("layer.lora_B.weight").?.data);
    try std.testing.expectEqualSlices(f32, &.{ 2.5, 3.5 }, combined.tensorByName("other.lora_A.weight").?.data);

    try registry.removeActive("a");
    var weighted_b = try registry.combineActive();
    defer weighted_b.deinit(allocator);
    try std.testing.expectEqualSlices(f32, &.{ 5.0, 10.0 }, weighted_b.tensorByName("layer.lora_A.weight").?.data);

    registry.disable();
    try std.testing.expectEqual(@as(usize, 0), registry.activeCount());
    var disabled = try registry.combineActive();
    defer disabled.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 0), disabled.parameter_count);
    try std.testing.expectEqual(@as(usize, 0), disabled.tensors.len);
}

test "trainable token adapter saves loads inspects and applies embedding deltas" {
    const allocator = std.testing.allocator;
    const root = try std.fmt.allocPrint(allocator, "/tmp/termite_trainable_token_adapter_test_{d}", .{std.posix.system.getpid()});
    defer allocator.free(root);
    compat.cwd().deleteTree(compat.io(), root) catch {};
    defer compat.cwd().deleteTree(compat.io(), root) catch {};

    const token_indices = [_]u32{ 1, 3 };
    const deltas = [_]f32{
        0.5, -0.5, 1.0,
        2.0, 0.0,  -1.0,
    };
    var summary = try saveTrainableTokenAdapter(
        allocator,
        compat.io(),
        root,
        "model.embed_tokens.weight",
        &token_indices,
        3,
        &deltas,
    );
    defer freeTrainableTokenAdapterSummary(allocator, &summary);
    try std.testing.expectEqual(@as(usize, 2), summary.token_count);
    try std.testing.expectEqual(@as(usize, 3), summary.embedding_dim);
    try std.testing.expectEqual(@as(usize, 6), summary.parameter_count);

    var inspect = try inspectTrainableTokenAdapter(allocator, root);
    defer freeTrainableTokenAdapterSummary(allocator, &inspect);
    try std.testing.expectEqualStrings(trainable_token_adapter_family_version, inspect.artifact_family_version);
    try std.testing.expectEqualStrings("model.embed_tokens.weight", inspect.base_embedding_tensor_name);
    try std.testing.expectEqual(@as(usize, 2), inspect.token_count);

    var loaded = try loadTrainableTokenAdapter(allocator, root);
    defer loaded.deinit();
    try std.testing.expectEqualSlices(u32, &token_indices, loaded.token_indices);
    try std.testing.expectEqualSlices(f32, &deltas, loaded.deltas);

    const base = [_]f32{
        0, 0, 0,
        1, 1, 1,
        2, 2, 2,
        3, 3, 3,
    };
    const merged = try applyTrainableTokenAdapter(allocator, &base, 4, 3, &loaded);
    defer allocator.free(merged);
    try std.testing.expectEqualSlices(f32, &.{
        0,   0,   0,
        1.5, 0.5, 2.0,
        2,   2,   2,
        5.0, 3.0, 2.0,
    }, merged);
}

test "trainable token adapter rejects out-of-range token indices" {
    const allocator = std.testing.allocator;
    const root = try std.fmt.allocPrint(allocator, "/tmp/termite_trainable_token_adapter_oob_test_{d}", .{std.posix.system.getpid()});
    defer allocator.free(root);
    compat.cwd().deleteTree(compat.io(), root) catch {};
    defer compat.cwd().deleteTree(compat.io(), root) catch {};

    const token_indices = [_]u32{4};
    const deltas = [_]f32{ 1.0, 2.0 };
    var summary = try saveTrainableTokenAdapter(
        allocator,
        compat.io(),
        root,
        "model.embed_tokens.weight",
        &token_indices,
        2,
        &deltas,
    );
    defer freeTrainableTokenAdapterSummary(allocator, &summary);

    var loaded = try loadTrainableTokenAdapter(allocator, root);
    defer loaded.deinit();
    const base = [_]f32{
        0, 0,
        1, 1,
    };
    try std.testing.expectError(error.TokenIndexOutOfRange, applyTrainableTokenAdapter(allocator, &base, 2, 2, &loaded));
}

test "trainable token adapter rejects duplicate token indices" {
    const allocator = std.testing.allocator;
    const root = try std.fmt.allocPrint(allocator, "/tmp/termite_trainable_token_adapter_duplicate_test_{d}", .{std.posix.system.getpid()});
    defer allocator.free(root);
    compat.cwd().deleteTree(compat.io(), root) catch {};
    defer compat.cwd().deleteTree(compat.io(), root) catch {};

    const token_indices = [_]u32{ 1, 1 };
    const deltas = [_]f32{
        1.0, 2.0,
        3.0, 4.0,
    };
    try std.testing.expectError(error.DuplicateTokenIndex, saveTrainableTokenAdapter(
        allocator,
        compat.io(),
        root,
        "model.embed_tokens.weight",
        &token_indices,
        2,
        &deltas,
    ));
}
