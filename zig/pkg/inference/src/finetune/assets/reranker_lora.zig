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

//! Offline checkpoint operations shared by tools and training.

const std = @import("std");
const compat = @import("../../io/compat.zig");
const c_file = @import("../../util/c_file.zig");
const manifest_mod = @import("../../models/manifest.zig");
const safetensors = @import("../../models/safetensors.zig");
const tensor_access = @import("../../models/tensor_access.zig");
const weight_source = @import("../../models/weight_source.zig");
const tensor_mod = @import("../../backends/tensor.zig");

pub const Tensor = tensor_mod.Tensor;

const DType = tensor_mod.DType;

const lora = @import("../lora.zig");

pub const artifact_family_version = "reranker_cross_encoder_lora/v1alpha1";

pub const checkpoint_file_name = "model.safetensors";

pub const config_file_name = "config.json";

pub const adapter_checkpoint_file_name = "adapter_model.safetensors";

pub const adapter_config_file_name = "adapter_config.json";

pub const tokenizer_config_file_name = "tokenizer_config.json";

pub const tokenizer_file_name = "tokenizer.json";

pub const special_tokens_map_file_name = "special_tokens_map.json";

pub const vocab_json_file_name = "vocab.json";

pub const merges_txt_file_name = "merges.txt";

pub const default_lora_target_modules = [_][]const u8{
    "query",
    "key",
    "value",
    "attention.output.dense",
};

pub const AdapterConfig = struct {
    base_model_name_or_path: ?[]const u8 = null,
    peft_type: ?[]const u8 = null,
    task_type: ?[]const u8 = null,
    r: ?usize = null,
    lora_alpha: ?f64 = null,
    target_modules: ?[]const []const u8 = null,
    top_layer_count: ?usize = null,
    use_dora: ?bool = null,
};

pub const LoRATargetTensor = struct {
    tensor_name: []const u8,
    module_name: []const u8,
    input_dim: usize,
    output_dim: usize,
};

pub const BootstrapOptions = struct {
    rank: usize = 16,
    alpha: f32 = 32.0,
    top_layer_count: usize = 1,
    base_model_name_or_path: ?[]const u8 = null,
    target_modules: ?[]const []const u8 = null,
};

pub const BootstrapSummary = struct {
    artifact_family_version: []const u8,
    model_dir: []const u8,
    output_dir: []const u8,
    checkpoint_path: []const u8,
    adapter_checkpoint_path: []const u8,
    adapter_config_path: []const u8,
    base_model_name_or_path: []const u8,
    lora_rank: usize,
    lora_alpha: f32,
    top_layer_count: usize,
    target_modules: []const []const u8,
    resolved_tensors: []LoRATargetTensor,
};

pub const LoRATensorSummary = struct {
    base_tensor_name: []const u8,
    adapter_a_tensor_name: []const u8,
    adapter_b_tensor_name: []const u8,
    dora_magnitude_tensor_name: ?[]const u8 = null,
    module_name: []const u8,
    input_dim: usize,
    output_dim: usize,
    rank: usize,
    adapter_parameter_count: usize,
    dora_magnitude_parameter_count: usize = 0,
};

pub const LoRABundleInspectionSummary = struct {
    artifact_family_version: []const u8,
    base_model_dir: []const u8,
    adapter_model_dir: []const u8,
    base_checkpoint_path: []const u8,
    adapter_checkpoint_path: []const u8,
    adapter_config_path: ?[]const u8 = null,
    base_model_name_or_path: ?[]const u8 = null,
    lora_rank: ?usize = null,
    lora_alpha: ?f64 = null,
    top_layer_count: ?usize = null,
    target_module_count: usize = 0,
    target_modules: ?[]const []const u8 = null,
    use_dora: ?bool = null,
    resolved_tensor_count: usize = 0,
    dora_magnitude_tensor_count: usize = 0,
    dora_magnitude_parameter_count: usize = 0,
    trainable_parameter_count: usize = 0,
    tensors: []LoRATensorSummary,
};

pub const LoadedLoRALayer = struct {
    base_tensor_name: []const u8,
    adapter_a_tensor_name: []const u8,
    adapter_b_tensor_name: []const u8,
    dora_magnitude_tensor_name: ?[]const u8 = null,
    module_name: []const u8,
    input_dim: usize,
    output_dim: usize,
    rank: usize,
    base_weight: []f32,
    adapter_a: []f32,
    adapter_b: []f32,
    dora_magnitude: ?[]f32 = null,
};

pub const LoadedLoRABundle = struct {
    allocator: std.mem.Allocator,
    base_model_dir: []const u8,
    adapter_model_dir: []const u8,
    base_checkpoint_path: []const u8,
    adapter_checkpoint_path: []const u8,
    adapter_config_path: ?[]const u8 = null,
    base_model_name_or_path: ?[]const u8 = null,
    lora_rank: usize,
    lora_alpha: f32,
    top_layer_count: usize = 1,
    target_modules: []const []const u8,
    layers: []LoadedLoRALayer,

    pub fn deinit(self: *LoadedLoRABundle) void {
        self.allocator.free(self.base_model_dir);
        self.allocator.free(self.adapter_model_dir);
        self.allocator.free(self.base_checkpoint_path);
        self.allocator.free(self.adapter_checkpoint_path);
        if (self.adapter_config_path) |value| self.allocator.free(value);
        if (self.base_model_name_or_path) |value| self.allocator.free(value);
        for (self.target_modules) |item| self.allocator.free(item);
        self.allocator.free(self.target_modules);
        for (self.layers) |layer| {
            self.allocator.free(layer.base_tensor_name);
            self.allocator.free(layer.adapter_a_tensor_name);
            self.allocator.free(layer.adapter_b_tensor_name);
            if (layer.dora_magnitude_tensor_name) |name| self.allocator.free(name);
            self.allocator.free(layer.module_name);
            self.allocator.free(layer.base_weight);
            self.allocator.free(layer.adapter_a);
            self.allocator.free(layer.adapter_b);
            if (layer.dora_magnitude) |magnitude| self.allocator.free(magnitude);
        }
        self.allocator.free(self.layers);
        self.* = undefined;
    }
};

pub const MaterializeSummary = struct {
    artifact_family_version: []const u8,
    base_model_dir: []const u8,
    adapter_model_dir: []const u8,
    output_dir: []const u8,
    output_checkpoint_path: []const u8,
    merged_lora_tensor_count: usize,
    merged_dora_tensor_count: usize = 0,
    copied_base_tensor_count: usize,
};

pub fn bootstrapLoRABundle(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    out_dir: []const u8,
    options: BootstrapOptions,
) !BootstrapSummary {
    if (options.rank == 0) return error.InvalidLoRARank;
    if (options.top_layer_count == 0) return error.InvalidTopLayerCount;

    var manifest = try manifest_mod.loadFromDir(allocator, model_dir);
    defer manifest.deinit();
    const checkpoint_path = manifest.safetensors_path orelse return error.MissingMergedCheckpoint;

    const requested_target_modules = options.target_modules orelse default_lora_target_modules[0..];
    const resolved_tensors = try inferLoRATargetTensors(allocator, checkpoint_path, requested_target_modules, options.top_layer_count);
    errdefer freeLoRATargetTensors(allocator, resolved_tensors);
    if (resolved_tensors.len == 0) return error.NoLoRATargetTensorsResolved;

    try compat.cwd().createDirPath(compat.io(), out_dir);
    const adapter_checkpoint_path = try std.fs.path.join(allocator, &.{ out_dir, adapter_checkpoint_file_name });
    errdefer allocator.free(adapter_checkpoint_path);
    const adapter_config_path = try std.fs.path.join(allocator, &.{ out_dir, adapter_config_file_name });
    errdefer allocator.free(adapter_config_path);

    const base_model_name_or_path = if (options.base_model_name_or_path) |value|
        try allocator.dupe(u8, value)
    else
        try allocator.dupe(u8, model_dir);
    errdefer allocator.free(base_model_name_or_path);

    try writeBootstrapAdapterCheckpoint(allocator, adapter_checkpoint_path, resolved_tensors, options.rank);
    try writeAdapterConfigJson(allocator, adapter_config_path, base_model_name_or_path, options.rank, options.alpha, requested_target_modules, options.top_layer_count, false);
    try copySupportingArtifactIfPresent(allocator, model_dir, out_dir, config_file_name);
    try copySupportingArtifactIfPresent(allocator, model_dir, out_dir, tokenizer_config_file_name);
    try copySupportingArtifactIfPresent(allocator, model_dir, out_dir, tokenizer_file_name);
    try copySupportingArtifactIfPresent(allocator, model_dir, out_dir, special_tokens_map_file_name);
    try copySupportingArtifactIfPresent(allocator, model_dir, out_dir, vocab_json_file_name);
    try copySupportingArtifactIfPresent(allocator, model_dir, out_dir, merges_txt_file_name);

    return .{
        .artifact_family_version = try allocator.dupe(u8, artifact_family_version),
        .model_dir = try allocator.dupe(u8, model_dir),
        .output_dir = try allocator.dupe(u8, out_dir),
        .checkpoint_path = try allocator.dupe(u8, checkpoint_path),
        .adapter_checkpoint_path = adapter_checkpoint_path,
        .adapter_config_path = adapter_config_path,
        .base_model_name_or_path = base_model_name_or_path,
        .lora_rank = options.rank,
        .lora_alpha = options.alpha,
        .top_layer_count = options.top_layer_count,
        .target_modules = try dupeStringSlice(allocator, requested_target_modules),
        .resolved_tensors = resolved_tensors,
    };
}

pub fn inspectLoRABundle(
    allocator: std.mem.Allocator,
    base_model_dir: []const u8,
    adapter_model_dir: []const u8,
) !LoRABundleInspectionSummary {
    var base_manifest = try manifest_mod.loadFromDir(allocator, base_model_dir);
    defer base_manifest.deinit();
    const base_checkpoint_path = base_manifest.safetensors_path orelse return error.MissingMergedCheckpoint;
    const adapter_checkpoint_path = try requiredPathInDir(allocator, adapter_model_dir, adapter_checkpoint_file_name);
    defer allocator.free(adapter_checkpoint_path);
    const adapter_config_path = try optionalPathInDir(allocator, adapter_model_dir, adapter_config_file_name);
    defer if (adapter_config_path) |path| allocator.free(path);

    var base_reader = try safetensors.MMapReader.openFileAbsolute(allocator, base_checkpoint_path);
    defer base_reader.deinit();
    var adapter_reader = try safetensors.MMapReader.openFileAbsolute(allocator, adapter_checkpoint_path);
    defer adapter_reader.deinit();

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_alloc = arena.allocator();
    const adapter_config = if (adapter_config_path) |path| try loadOptionalJson(AdapterConfig, arena_alloc, path) else null;

    var tensors: std.ArrayListUnmanaged(LoRATensorSummary) = .empty;
    errdefer {
        for (tensors.items) |*item| freeLoRATensorSummary(allocator, item);
        tensors.deinit(allocator);
    }

    var it = adapter_reader.header.tensors.iterator();
    while (it.next()) |entry| {
        const adapter_a_name = entry.key_ptr.*;
        const parsed = parseLoRAAdapterTensorName(adapter_a_name) orelse continue;
        if (parsed.kind != .a) continue;

        const adapter_a_info = entry.value_ptr.*;
        const base_tensor_name = try std.fmt.allocPrint(allocator, "{s}.weight", .{parsed.base_tensor_base_name});
        defer allocator.free(base_tensor_name);
        const adapter_b_name = try std.fmt.allocPrint(allocator, "{s}.lora_B.weight", .{parsed.base_tensor_base_name});
        defer allocator.free(adapter_b_name);
        const adapter_b_info = adapter_reader.header.tensors.get(adapter_b_name) orelse return error.MissingAdapterPair;
        const base_info = base_reader.header.tensors.get(base_tensor_name) orelse return error.MissingBaseTensorForAdapter;
        if (adapter_a_info.shape.len != 2 or adapter_b_info.shape.len != 2 or base_info.shape.len != 2) return error.InvalidAdapterTensorShape;

        const rank: usize = @intCast(adapter_a_info.shape[0]);
        const input_dim: usize = @intCast(base_info.shape[1]);
        const output_dim: usize = @intCast(base_info.shape[0]);
        if (adapter_a_info.shape[1] != base_info.shape[1]) return error.AdapterInputDimMismatch;
        if (adapter_b_info.shape[0] != base_info.shape[0]) return error.AdapterOutputDimMismatch;
        if (adapter_a_info.shape[0] != adapter_b_info.shape[1]) return error.AdapterRankMismatch;

        const dora_name = try doraMagnitudeTensorName(allocator, parsed.base_tensor_base_name);
        defer allocator.free(dora_name);
        const dora_info = adapter_reader.header.tensors.get(dora_name);
        if (dora_info) |info| {
            if (info.shape.len != 1 or info.shape[0] != base_info.shape[0]) return error.InvalidAdapterTensorShape;
        }
        const dora_parameter_count: usize = if (dora_info != null) output_dim else 0;

        try tensors.append(allocator, .{
            .base_tensor_name = try allocator.dupe(u8, base_tensor_name),
            .adapter_a_tensor_name = try allocator.dupe(u8, adapter_a_name),
            .adapter_b_tensor_name = try allocator.dupe(u8, adapter_b_name),
            .dora_magnitude_tensor_name = if (dora_info != null) try allocator.dupe(u8, dora_name) else null,
            .module_name = try allocator.dupe(u8, parsed.module_name),
            .input_dim = input_dim,
            .output_dim = output_dim,
            .rank = rank,
            .adapter_parameter_count = rank * input_dim + output_dim * rank + dora_parameter_count,
            .dora_magnitude_parameter_count = dora_parameter_count,
        });
    }

    std.mem.sort(LoRATensorSummary, tensors.items, {}, struct {
        fn lessThan(_: void, lhs: LoRATensorSummary, rhs: LoRATensorSummary) bool {
            return std.mem.lessThan(u8, lhs.base_tensor_name, rhs.base_tensor_name);
        }
    }.lessThan);

    var trainable_parameter_count: usize = 0;
    var dora_magnitude_tensor_count: usize = 0;
    var dora_magnitude_parameter_count: usize = 0;
    for (tensors.items) |item| {
        trainable_parameter_count += item.adapter_parameter_count;
        if (item.dora_magnitude_tensor_name != null) dora_magnitude_tensor_count += 1;
        dora_magnitude_parameter_count += item.dora_magnitude_parameter_count;
    }

    return .{
        .artifact_family_version = try allocator.dupe(u8, artifact_family_version),
        .base_model_dir = try allocator.dupe(u8, base_model_dir),
        .adapter_model_dir = try allocator.dupe(u8, adapter_model_dir),
        .base_checkpoint_path = try allocator.dupe(u8, base_checkpoint_path),
        .adapter_checkpoint_path = try allocator.dupe(u8, adapter_checkpoint_path),
        .adapter_config_path = if (adapter_config_path) |path| try allocator.dupe(u8, path) else null,
        .base_model_name_or_path = if (adapter_config) |cfg| try dupeOptionalString(allocator, cfg.base_model_name_or_path) else null,
        .lora_rank = if (adapter_config) |cfg| cfg.r else null,
        .lora_alpha = if (adapter_config) |cfg| cfg.lora_alpha else null,
        .top_layer_count = if (adapter_config) |cfg| cfg.top_layer_count else null,
        .target_module_count = if (adapter_config) |cfg| if (cfg.target_modules) |items| items.len else 0 else 0,
        .target_modules = if (adapter_config) |cfg| try dupeOptionalStringSlice(allocator, cfg.target_modules) else null,
        .use_dora = if (adapter_config) |cfg| cfg.use_dora else null,
        .resolved_tensor_count = tensors.items.len,
        .dora_magnitude_tensor_count = dora_magnitude_tensor_count,
        .dora_magnitude_parameter_count = dora_magnitude_parameter_count,
        .trainable_parameter_count = trainable_parameter_count,
        .tensors = try tensors.toOwnedSlice(allocator),
    };
}

pub fn loadLoRABundle(
    allocator: std.mem.Allocator,
    base_model_dir: []const u8,
    adapter_model_dir: []const u8,
) !LoadedLoRABundle {
    var inspected = try inspectLoRABundle(allocator, base_model_dir, adapter_model_dir);
    defer freeLoRABundleInspectionSummary(allocator, &inspected);
    if (inspected.lora_rank == null or inspected.lora_alpha == null) return error.MissingAdapterConfig;

    const layers = try allocator.alloc(LoadedLoRALayer, inspected.tensors.len);
    var loaded_count: usize = 0;
    errdefer {
        for (layers[0..loaded_count]) |layer| {
            allocator.free(layer.base_tensor_name);
            allocator.free(layer.adapter_a_tensor_name);
            allocator.free(layer.adapter_b_tensor_name);
            if (layer.dora_magnitude_tensor_name) |name| allocator.free(name);
            allocator.free(layer.module_name);
            allocator.free(layer.base_weight);
            allocator.free(layer.adapter_a);
            allocator.free(layer.adapter_b);
            if (layer.dora_magnitude) |magnitude| allocator.free(magnitude);
        }
        allocator.free(layers);
    }

    var base_access = try openTensorAccessForModelDir(allocator, base_model_dir);
    defer base_access.deinit();
    var adapter_access = try openTensorAccessForFile(allocator, inspected.adapter_checkpoint_path);
    defer adapter_access.deinit();

    for (inspected.tensors, 0..) |tensor_summary, idx| {
        var adapter_a_tensor = try loadTensorAsF32(allocator, adapter_access, tensor_summary.adapter_a_tensor_name);
        defer adapter_a_tensor.deinit();
        var adapter_b_tensor = try loadTensorAsF32(allocator, adapter_access, tensor_summary.adapter_b_tensor_name);
        defer adapter_b_tensor.deinit();
        var base_weight_tensor = try loadTensorAsF32(allocator, base_access, tensor_summary.base_tensor_name);
        defer base_weight_tensor.deinit();

        const adapter_a = try allocator.alloc(f32, tensor_summary.input_dim * tensor_summary.rank);
        errdefer allocator.free(adapter_a);
        transpose2DF32(adapter_a, adapter_a_tensor.asFloat32(), tensor_summary.rank, tensor_summary.input_dim);

        const adapter_b = try allocator.alloc(f32, tensor_summary.rank * tensor_summary.output_dim);
        errdefer allocator.free(adapter_b);
        transpose2DF32(adapter_b, adapter_b_tensor.asFloat32(), tensor_summary.output_dim, tensor_summary.rank);

        const base_weight = try allocator.alloc(f32, tensor_summary.input_dim * tensor_summary.output_dim);
        errdefer allocator.free(base_weight);
        transpose2DF32(base_weight, base_weight_tensor.asFloat32(), tensor_summary.output_dim, tensor_summary.input_dim);

        var dora_magnitude_name: ?[]const u8 = null;
        var dora_magnitude: ?[]f32 = null;
        if (tensor_summary.dora_magnitude_tensor_name) |name| {
            var magnitude_tensor = try loadTensorAsF32(allocator, adapter_access, name);
            defer magnitude_tensor.deinit();
            if (magnitude_tensor.shape.len != 1 or magnitude_tensor.shape[0] != @as(i64, @intCast(tensor_summary.output_dim))) return error.InvalidAdapterTensorShape;
            const magnitude = try allocator.dupe(f32, magnitude_tensor.asFloat32());
            errdefer allocator.free(magnitude);
            dora_magnitude_name = try allocator.dupe(u8, name);
            errdefer if (dora_magnitude_name) |owned| allocator.free(owned);
            dora_magnitude = magnitude;
        }

        layers[idx] = .{
            .base_tensor_name = try allocator.dupe(u8, tensor_summary.base_tensor_name),
            .adapter_a_tensor_name = try allocator.dupe(u8, tensor_summary.adapter_a_tensor_name),
            .adapter_b_tensor_name = try allocator.dupe(u8, tensor_summary.adapter_b_tensor_name),
            .dora_magnitude_tensor_name = dora_magnitude_name,
            .module_name = try allocator.dupe(u8, tensor_summary.module_name),
            .input_dim = tensor_summary.input_dim,
            .output_dim = tensor_summary.output_dim,
            .rank = tensor_summary.rank,
            .base_weight = base_weight,
            .adapter_a = adapter_a,
            .adapter_b = adapter_b,
            .dora_magnitude = dora_magnitude,
        };
        loaded_count += 1;
    }

    return .{
        .allocator = allocator,
        .base_model_dir = try allocator.dupe(u8, inspected.base_model_dir),
        .adapter_model_dir = try allocator.dupe(u8, inspected.adapter_model_dir),
        .base_checkpoint_path = try allocator.dupe(u8, inspected.base_checkpoint_path),
        .adapter_checkpoint_path = try allocator.dupe(u8, inspected.adapter_checkpoint_path),
        .adapter_config_path = try dupeOptionalString(allocator, inspected.adapter_config_path),
        .base_model_name_or_path = try dupeOptionalString(allocator, inspected.base_model_name_or_path),
        .lora_rank = inspected.lora_rank.?,
        .lora_alpha = @floatCast(inspected.lora_alpha.?),
        .top_layer_count = inspected.top_layer_count orelse 1,
        .target_modules = if (inspected.target_modules) |items|
            try dupeStringSlice(allocator, items)
        else
            try dupeStringSlice(allocator, default_lora_target_modules[0..]),
        .layers = layers,
    };
}

pub fn saveLoRABundle(bundle: *const LoadedLoRABundle, out_dir: []const u8) !void {
    const allocator = bundle.allocator;
    try compat.cwd().createDirPath(compat.io(), out_dir);
    const checkpoint_path = try std.fs.path.join(allocator, &.{ out_dir, adapter_checkpoint_file_name });
    defer allocator.free(checkpoint_path);
    const config_path = try std.fs.path.join(allocator, &.{ out_dir, adapter_config_file_name });
    defer allocator.free(config_path);

    var tensors = try allocator.alloc(WriteTensorF32, bundle.layers.len * 3);
    defer allocator.free(tensors);
    var owned_shapes: std.ArrayListUnmanaged([]const usize) = .empty;
    defer {
        for (owned_shapes.items) |item| allocator.free(item);
        owned_shapes.deinit(allocator);
    }
    var owned_data: std.ArrayListUnmanaged([]const f32) = .empty;
    defer {
        for (owned_data.items) |item| allocator.free(item);
        owned_data.deinit(allocator);
    }
    var owned_names: std.ArrayListUnmanaged([]const u8) = .empty;
    defer {
        for (owned_names.items) |item| allocator.free(item);
        owned_names.deinit(allocator);
    }

    var tensor_idx: usize = 0;
    for (bundle.layers) |layer| {
        const adapter_a_hf = try allocator.alloc(f32, layer.rank * layer.input_dim);
        const adapter_b_hf = try allocator.alloc(f32, layer.output_dim * layer.rank);
        try owned_data.append(allocator, adapter_a_hf);
        try owned_data.append(allocator, adapter_b_hf);
        transpose2DF32(adapter_a_hf, layer.adapter_a, layer.input_dim, layer.rank);
        transpose2DF32(adapter_b_hf, layer.adapter_b, layer.rank, layer.output_dim);
        const a_shape = try allocator.dupe(usize, &.{ layer.rank, layer.input_dim });
        const b_shape = try allocator.dupe(usize, &.{ layer.output_dim, layer.rank });
        try owned_shapes.append(allocator, a_shape);
        try owned_shapes.append(allocator, b_shape);
        tensors[tensor_idx] = .{ .name = layer.adapter_a_tensor_name, .shape = a_shape, .data = adapter_a_hf };
        tensor_idx += 1;
        tensors[tensor_idx] = .{ .name = layer.adapter_b_tensor_name, .shape = b_shape, .data = adapter_b_hf };
        tensor_idx += 1;
        if (layer.dora_magnitude) |magnitude| {
            const m_shape = try allocator.dupe(usize, &.{layer.output_dim});
            try owned_shapes.append(allocator, m_shape);
            const magnitude_name = if (layer.dora_magnitude_tensor_name) |name|
                name
            else
                try doraMagnitudeTensorName(allocator, tensorBaseName(layer.base_tensor_name));
            if (layer.dora_magnitude_tensor_name == null) try owned_names.append(allocator, magnitude_name);
            tensors[tensor_idx] = .{ .name = magnitude_name, .shape = m_shape, .data = magnitude };
            tensor_idx += 1;
        }
    }

    try writeHeaderAndTensorsF32(allocator, checkpoint_path, tensors[0..tensor_idx]);
    try writeAdapterConfigJson(
        allocator,
        config_path,
        bundle.base_model_name_or_path orelse bundle.base_model_dir,
        bundle.lora_rank,
        bundle.lora_alpha,
        bundle.target_modules,
        bundle.top_layer_count,
        bundleHasDoRA(bundle),
    );
}

pub fn materializeMergedModel(
    allocator: std.mem.Allocator,
    base_model_dir: []const u8,
    adapter_model_dir: []const u8,
    out_dir: []const u8,
) !MaterializeSummary {
    var bundle = try loadLoRABundle(allocator, base_model_dir, adapter_model_dir);
    defer bundle.deinit();

    var base_access = try openTensorAccessForModelDir(allocator, base_model_dir);
    defer base_access.deinit();
    const base_names = try base_access.listNames(allocator);
    defer allocator.free(base_names);

    var merged = std.StringArrayHashMapUnmanaged(Tensor){};
    defer {
        var it = merged.iterator();
        while (it.next()) |entry| {
            allocator.free(entry.key_ptr.*);
            entry.value_ptr.deinit();
        }
        merged.deinit(allocator);
    }

    var merged_dora_tensor_count: usize = 0;
    for (bundle.layers) |layer| {
        const merged_weight = try allocator.alloc(f32, layer.base_weight.len);
        const matrix = lora.Matrix{ .rows = layer.input_dim, .cols = layer.output_dim, .data = layer.base_weight };
        const adapter_a = lora.Matrix{ .rows = layer.input_dim, .cols = layer.rank, .data = layer.adapter_a };
        const adapter_b = lora.Matrix{ .rows = layer.rank, .cols = layer.output_dim, .data = layer.adapter_b };
        if (layer.dora_magnitude) |magnitude| {
            lora.doraMergeInto(.{
                .base = matrix,
                .adapter_a = adapter_a,
                .adapter_b = adapter_b,
                .magnitude = magnitude,
                .alpha = bundle.lora_alpha,
            }, merged_weight);
            merged_dora_tensor_count += 1;
        } else {
            lora.mergeInto(
                matrix,
                adapter_a,
                adapter_b,
                bundle.lora_alpha,
                merged_weight,
            );
        }

        const out_rows = layer.output_dim;
        const out_cols = layer.input_dim;
        const hf_weight = try allocator.alloc(f32, merged_weight.len);
        transpose2DF32(hf_weight, merged_weight, out_cols, out_rows);
        allocator.free(merged_weight);

        const shape = try allocator.dupe(i64, &.{ @as(i64, @intCast(out_rows)), @as(i64, @intCast(out_cols)) });
        defer allocator.free(shape);
        const tensor = try Tensor.initFloat32(allocator, layer.base_tensor_name, shape, hf_weight);
        allocator.free(hf_weight);
        try merged.put(allocator, try allocator.dupe(u8, layer.base_tensor_name), tensor);
    }

    const output_checkpoint_path = try std.fs.path.join(allocator, &.{ out_dir, checkpoint_file_name });
    errdefer allocator.free(output_checkpoint_path);
    try compat.cwd().createDirPath(compat.io(), out_dir);
    const bytes = try buildMergedSafetensorsFile(allocator, base_access, base_names, &merged);
    defer allocator.free(bytes);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = output_checkpoint_path, .data = bytes });

    try copySupportingArtifactIfPresent(allocator, base_model_dir, out_dir, config_file_name);
    try copySupportingArtifactIfPresent(allocator, base_model_dir, out_dir, tokenizer_config_file_name);
    try copySupportingArtifactIfPresent(allocator, base_model_dir, out_dir, tokenizer_file_name);
    try copySupportingArtifactIfPresent(allocator, base_model_dir, out_dir, special_tokens_map_file_name);
    try copySupportingArtifactIfPresent(allocator, base_model_dir, out_dir, vocab_json_file_name);
    try copySupportingArtifactIfPresent(allocator, base_model_dir, out_dir, merges_txt_file_name);

    return .{
        .artifact_family_version = try allocator.dupe(u8, artifact_family_version),
        .base_model_dir = try allocator.dupe(u8, base_model_dir),
        .adapter_model_dir = try allocator.dupe(u8, adapter_model_dir),
        .output_dir = try allocator.dupe(u8, out_dir),
        .output_checkpoint_path = output_checkpoint_path,
        .merged_lora_tensor_count = bundle.layers.len,
        .merged_dora_tensor_count = merged_dora_tensor_count,
        .copied_base_tensor_count = base_names.len - bundle.layers.len,
    };
}

pub fn freeBootstrapSummary(allocator: std.mem.Allocator, summary: *BootstrapSummary) void {
    allocator.free(summary.artifact_family_version);
    allocator.free(summary.model_dir);
    allocator.free(summary.output_dir);
    allocator.free(summary.checkpoint_path);
    allocator.free(summary.adapter_checkpoint_path);
    allocator.free(summary.adapter_config_path);
    allocator.free(summary.base_model_name_or_path);
    for (summary.target_modules) |item| allocator.free(item);
    allocator.free(summary.target_modules);
    freeLoRATargetTensors(allocator, summary.resolved_tensors);
    summary.* = undefined;
}

pub fn freeLoRABundleInspectionSummary(allocator: std.mem.Allocator, summary: *LoRABundleInspectionSummary) void {
    allocator.free(summary.artifact_family_version);
    allocator.free(summary.base_model_dir);
    allocator.free(summary.adapter_model_dir);
    allocator.free(summary.base_checkpoint_path);
    allocator.free(summary.adapter_checkpoint_path);
    if (summary.adapter_config_path) |path| allocator.free(path);
    if (summary.base_model_name_or_path) |path| allocator.free(path);
    if (summary.target_modules) |items| {
        for (items) |item| allocator.free(item);
        allocator.free(items);
    }
    for (summary.tensors) |*item| freeLoRATensorSummary(allocator, item);
    allocator.free(summary.tensors);
    summary.* = undefined;
}

pub fn freeMaterializeSummary(allocator: std.mem.Allocator, summary: *MaterializeSummary) void {
    allocator.free(summary.artifact_family_version);
    allocator.free(summary.base_model_dir);
    allocator.free(summary.adapter_model_dir);
    allocator.free(summary.output_dir);
    allocator.free(summary.output_checkpoint_path);
    summary.* = undefined;
}

fn inferLoRATargetTensors(
    allocator: std.mem.Allocator,
    checkpoint_path: []const u8,
    requested_target_modules: []const []const u8,
    top_layer_count: usize,
) ![]LoRATargetTensor {
    var reader = try safetensors.MMapReader.openFileAbsolute(allocator, checkpoint_path);
    defer reader.deinit();

    var all_targets: std.ArrayListUnmanaged(LoRATargetTensor) = .empty;
    defer {
        if (all_targets.items.len == 0) all_targets.deinit(allocator);
    }
    errdefer {
        for (all_targets.items) |item| {
            allocator.free(item.tensor_name);
            allocator.free(item.module_name);
        }
        all_targets.deinit(allocator);
    }

    var max_layer: ?usize = null;
    var it = reader.header.tensors.iterator();
    while (it.next()) |entry| {
        const tensor_name = entry.key_ptr.*;
        const module_name = moduleNameForTensor(tensor_name) orelse continue;
        if (!stringSliceContains(requested_target_modules, module_name)) continue;
        const layer_idx = parseEncoderLayerIndex(tensor_name) orelse continue;
        max_layer = if (max_layer) |current| @max(current, layer_idx) else layer_idx;
        const info = entry.value_ptr.*;
        if (info.shape.len != 2) continue;
        try all_targets.append(allocator, .{
            .tensor_name = try allocator.dupe(u8, tensor_name),
            .module_name = try allocator.dupe(u8, module_name),
            .output_dim = @intCast(info.shape[0]),
            .input_dim = @intCast(info.shape[1]),
        });
    }
    const max_layer_idx = max_layer orelse return error.NoLoRATargetTensorsResolved;
    const min_layer = if (top_layer_count > max_layer_idx + 1) 0 else (max_layer_idx + 1 - top_layer_count);

    var filtered: std.ArrayListUnmanaged(LoRATargetTensor) = .empty;
    errdefer {
        for (filtered.items) |item| {
            allocator.free(item.tensor_name);
            allocator.free(item.module_name);
        }
        filtered.deinit(allocator);
    }
    for (all_targets.items) |item| {
        const layer_idx = parseEncoderLayerIndex(item.tensor_name) orelse continue;
        if (layer_idx < min_layer) {
            allocator.free(item.tensor_name);
            allocator.free(item.module_name);
            continue;
        }
        try filtered.append(allocator, .{
            .tensor_name = item.tensor_name,
            .module_name = item.module_name,
            .input_dim = item.input_dim,
            .output_dim = item.output_dim,
        });
    }
    allocator.free(all_targets.items);
    std.mem.sort(LoRATargetTensor, filtered.items, {}, struct {
        fn lessThan(_: void, lhs: LoRATargetTensor, rhs: LoRATargetTensor) bool {
            return std.mem.lessThan(u8, lhs.tensor_name, rhs.tensor_name);
        }
    }.lessThan);
    return filtered.toOwnedSlice(allocator);
}

fn moduleNameForTensor(tensor_name: []const u8) ?[]const u8 {
    const normalized = if (std.mem.endsWith(u8, tensor_name, ".weight"))
        tensor_name[0 .. tensor_name.len - ".weight".len]
    else
        tensor_name;
    const ordered_modules = [_][]const u8{
        "attention.output.dense",
        "query",
        "key",
        "value",
    };
    inline for (ordered_modules) |module_name| {
        if (std.mem.eql(u8, normalized, module_name)) return module_name;
        const suffix = "." ++ module_name;
        if (std.mem.endsWith(u8, normalized, suffix)) return module_name;
    }
    return null;
}

const LoRAAdapterTensorKind = enum { a, b };

const ParsedLoRAAdapterTensorName = struct {
    base_tensor_base_name: []const u8,
    module_name: []const u8,
    kind: LoRAAdapterTensorKind,
};

fn parseLoRAAdapterTensorName(tensor_name: []const u8) ?ParsedLoRAAdapterTensorName {
    const parsed = lora.parseLayerTensorName(tensor_name) orelse return null;
    const module = moduleNameForTensor(parsed.layer_name) orelse return null;
    return .{
        .base_tensor_base_name = parsed.layer_name,
        .module_name = module,
        .kind = switch (parsed.kind) {
            .a => .a,
            .b => .b,
        },
    };
}

pub fn parseEncoderLayerIndex(tensor_name: []const u8) ?usize {
    const candidates = [_][]const u8{
        "encoder.layer.",
        "roberta.encoder.layer.",
        "bert.encoder.layer.",
    };
    inline for (candidates) |prefix| {
        if (std.mem.indexOf(u8, tensor_name, prefix)) |start| {
            const digits = tensor_name[start + prefix.len ..];
            var end: usize = 0;
            while (end < digits.len and std.ascii.isDigit(digits[end])) : (end += 1) {}
            if (end == 0) return null;
            return std.fmt.parseUnsigned(usize, digits[0..end], 10) catch null;
        }
    }
    return null;
}

const WriteTensorF32 = struct {
    name: []const u8,
    shape: []const usize,
    data: []const f32,
};

fn writeBootstrapAdapterCheckpoint(
    allocator: std.mem.Allocator,
    output_path: []const u8,
    resolved_tensors: []const LoRATargetTensor,
    rank: usize,
) !void {
    var tensors = try allocator.alloc(WriteTensorF32, resolved_tensors.len * 2);
    defer allocator.free(tensors);
    var owned_names: std.ArrayListUnmanaged([]const u8) = .empty;
    defer {
        for (owned_names.items) |item| allocator.free(item);
        owned_names.deinit(allocator);
    }
    var owned_shapes: std.ArrayListUnmanaged([]const usize) = .empty;
    defer {
        for (owned_shapes.items) |item| allocator.free(item);
        owned_shapes.deinit(allocator);
    }
    var owned_data: std.ArrayListUnmanaged([]const f32) = .empty;
    defer {
        for (owned_data.items) |item| allocator.free(item);
        owned_data.deinit(allocator);
    }
    var tensor_idx: usize = 0;
    for (resolved_tensors) |target| {
        const a_data = try buildDeterministicLoraA(allocator, rank, target.input_dim);
        const b_data = try buildZeroF32(allocator, target.output_dim * rank);
        try owned_data.append(allocator, a_data);
        try owned_data.append(allocator, b_data);
        const a_name = try std.fmt.allocPrint(allocator, "{s}.lora_A.weight", .{tensorBaseName(target.tensor_name)});
        const b_name = try std.fmt.allocPrint(allocator, "{s}.lora_B.weight", .{tensorBaseName(target.tensor_name)});
        try owned_names.append(allocator, a_name);
        try owned_names.append(allocator, b_name);
        const a_shape = try allocator.dupe(usize, &.{ rank, target.input_dim });
        const b_shape = try allocator.dupe(usize, &.{ target.output_dim, rank });
        try owned_shapes.append(allocator, a_shape);
        try owned_shapes.append(allocator, b_shape);
        tensors[tensor_idx] = .{ .name = a_name, .shape = a_shape, .data = a_data };
        tensor_idx += 1;
        tensors[tensor_idx] = .{ .name = b_name, .shape = b_shape, .data = b_data };
        tensor_idx += 1;
    }
    try writeHeaderAndTensorsF32(allocator, output_path, tensors[0..tensor_idx]);
}

pub fn writeHeaderAndTensorsF32(
    allocator: std.mem.Allocator,
    path: []const u8,
    tensors: []const WriteTensorF32,
) !void {
    var header_buf: std.Io.Writer.Allocating = .init(allocator);
    defer header_buf.deinit();
    const writer = &header_buf.writer;
    try writer.writeByte('{');
    var offset: u64 = 0;
    for (tensors, 0..) |tensor, idx| {
        if (idx != 0) try writer.writeByte(',');
        const byte_len = tensor.data.len * @sizeOf(f32);
        try writer.print("\"{s}\":{{\"dtype\":\"F32\",\"shape\":[", .{tensor.name});
        for (tensor.shape, 0..) |dim, dim_idx| {
            if (dim_idx != 0) try writer.writeByte(',');
            try writer.print("{}", .{dim});
        }
        try writer.print("],\"data_offsets\":[{},{}]}}", .{ offset, offset + byte_len });
        offset += byte_len;
    }
    try writer.writeByte('}');
    const io = compat.io();
    var file = try compat.cwd().createFile(io, path, .{ .truncate = true });
    defer file.close(io);
    var len_buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &len_buf, header_buf.written().len, .little);
    try file.writeStreamingAll(io, &len_buf);
    try file.writeStreamingAll(io, header_buf.written());
    for (tensors) |tensor| {
        for (tensor.data) |item| {
            const bits: u32 = @bitCast(item);
            var bits_buf: [4]u8 = undefined;
            std.mem.writeInt(u32, &bits_buf, bits, .little);
            try file.writeStreamingAll(io, &bits_buf);
        }
    }
}

fn writeAdapterConfigJson(
    allocator: std.mem.Allocator,
    path: []const u8,
    base_model_name_or_path: []const u8,
    rank: usize,
    alpha: f32,
    target_modules: []const []const u8,
    top_layer_count: usize,
    use_dora: bool,
) !void {
    var buffer: std.Io.Writer.Allocating = .init(allocator);
    defer buffer.deinit();
    try std.json.Stringify.value(.{
        .base_model_name_or_path = base_model_name_or_path,
        .peft_type = "LORA",
        .task_type = "FEATURE_EXTRACTION",
        .r = rank,
        .lora_alpha = alpha,
        .target_modules = target_modules,
        .top_layer_count = top_layer_count,
        .use_dora = use_dora,
    }, .{ .whitespace = .indent_2 }, &buffer.writer);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = path, .data = buffer.written() });
}

fn buildDeterministicLoraA(allocator: std.mem.Allocator, rows: usize, cols: usize) ![]f32 {
    const data = try allocator.alloc(f32, rows * cols);
    for (0..rows) |row| {
        for (0..cols) |col| {
            const idx = row * cols + col;
            const angle = @as(f32, @floatFromInt((row + 1) * (col + 3)));
            data[idx] = @sin(angle * 0.013) * 0.01;
        }
    }
    return data;
}

fn buildZeroF32(allocator: std.mem.Allocator, len: usize) ![]f32 {
    const data = try allocator.alloc(f32, len);
    @memset(data, 0.0);
    return data;
}

fn tensorBaseName(tensor_name: []const u8) []const u8 {
    if (std.mem.endsWith(u8, tensor_name, ".weight")) return tensor_name[0 .. tensor_name.len - ".weight".len];
    return tensor_name;
}

fn doraMagnitudeTensorName(allocator: std.mem.Allocator, base_tensor_name: []const u8) ![]const u8 {
    return std.fmt.allocPrint(allocator, "{s}.lora_magnitude_vector.weight", .{base_tensor_name});
}

fn bundleHasDoRA(bundle: *const LoadedLoRABundle) bool {
    for (bundle.layers) |layer| {
        if (layer.dora_magnitude != null) return true;
    }
    return false;
}

fn copySupportingArtifactIfPresent(
    allocator: std.mem.Allocator,
    source_dir: []const u8,
    out_dir: []const u8,
    file_name: []const u8,
) !void {
    const src = try optionalPathInDir(allocator, source_dir, file_name);
    defer if (src) |path| allocator.free(path);
    const src_path = src orelse return;
    const bytes = try c_file.readFile(allocator, src_path);
    defer allocator.free(bytes);
    const dst = try std.fs.path.join(allocator, &.{ out_dir, file_name });
    defer allocator.free(dst);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = dst, .data = bytes });
}

fn openTensorAccessForFile(allocator: std.mem.Allocator, path: []const u8) !tensor_access.TensorAccess {
    if (std.mem.endsWith(u8, path, ".index.json")) {
        const access = try tensor_access.ShardedSafetensorsAccess.initAbsolute(allocator, path);
        return access.tensorAccess();
    }
    const access = try tensor_access.SafetensorsAccess.initAbsolute(allocator, path);
    return access.tensorAccess();
}

pub fn openTensorAccessForModelDir(allocator: std.mem.Allocator, model_dir: []const u8) !tensor_access.TensorAccess {
    var manifest = try manifest_mod.loadFromDir(allocator, model_dir);
    defer manifest.deinit();
    return try tensor_access.openFromManifest(allocator, manifest);
}

pub fn loadTensorAsF32(allocator: std.mem.Allocator, access: tensor_access.TensorAccess, name: []const u8) !Tensor {
    var record = try access.getRecord(allocator, name);
    defer record.deinit();
    var tensor = (try record.materializeDense(allocator)) orelse return error.UnsupportedTensorEncoding;
    if (tensor.dtype == .f16 or tensor.dtype == .bf16) {
        const converted = try weight_source.convertToF32(allocator, &tensor);
        tensor.deinit();
        return converted;
    }
    if (tensor.dtype != .f32) {
        tensor.deinit();
        return error.UnsupportedTensorType;
    }
    return tensor;
}

fn buildMergedSafetensorsFile(
    allocator: std.mem.Allocator,
    base_access: tensor_access.TensorAccess,
    base_names: [][]const u8,
    merged: *const std.StringArrayHashMapUnmanaged(Tensor),
) ![]u8 {
    var ordered_names = try allocator.alloc([]const u8, base_names.len + merged.count());
    defer allocator.free(ordered_names);
    var count: usize = 0;
    for (base_names) |name| {
        ordered_names[count] = name;
        count += 1;
    }
    var it_merged = merged.iterator();
    while (it_merged.next()) |entry| {
        if (!stringSliceContains(base_names, entry.key_ptr.*)) {
            ordered_names[count] = entry.key_ptr.*;
            count += 1;
        }
    }
    std.mem.sort([]const u8, ordered_names[0..count], {}, struct {
        fn lessThan(_: void, lhs: []const u8, rhs: []const u8) bool {
            return std.mem.lessThan(u8, lhs, rhs);
        }
    }.lessThan);

    var header_buf: std.Io.Writer.Allocating = .init(allocator);
    defer header_buf.deinit();
    try header_buf.writer.writeByte('{');
    var data_parts = std.ArrayListUnmanaged([]const u8).empty;
    defer data_parts.deinit(allocator);
    var owned_records = std.ArrayListUnmanaged(Tensor).empty;
    defer {
        for (owned_records.items) |*tensor| tensor.deinit();
        owned_records.deinit(allocator);
    }
    var offset: u64 = 0;
    for (ordered_names[0..count], 0..) |name, idx| {
        var tensor: Tensor = undefined;
        if (merged.get(name)) |existing| {
            tensor = existing;
        } else {
            tensor = try loadTensorAsF32(allocator, base_access, name);
            try owned_records.append(allocator, tensor);
        }
        const shape = tensor.shape;
        const byte_len = tensor.data.len;
        if (idx != 0) try header_buf.writer.writeByte(',');
        try header_buf.writer.print("\"{s}\":{{\"dtype\":\"{s}\",\"shape\":[", .{ name, dtypeName(tensor.dtype) });
        for (shape, 0..) |dim, dim_idx| {
            if (dim_idx != 0) try header_buf.writer.writeByte(',');
            try header_buf.writer.print("{}", .{dim});
        }
        try header_buf.writer.print("],\"data_offsets\":[{},{}]}}", .{ offset, offset + byte_len });
        try data_parts.append(allocator, tensor.data);
        offset += byte_len;
    }
    try header_buf.writer.writeByte('}');
    var file = std.ArrayListUnmanaged(u8).empty;
    errdefer file.deinit(allocator);
    var len_buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &len_buf, header_buf.written().len, .little);
    try file.appendSlice(allocator, &len_buf);
    try file.appendSlice(allocator, header_buf.written());
    for (data_parts.items) |part| try file.appendSlice(allocator, part);
    return try file.toOwnedSlice(allocator);
}

fn dtypeName(dtype: DType) []const u8 {
    return switch (dtype) {
        .f32 => "F32",
        .f16 => "F16",
        .bf16 => "BF16",
        .f64 => "F64",
        .i8 => "I8",
        .i16 => "I16",
        .i32 => "I32",
        .i64 => "I64",
        .u8 => "U8",
        .bool_ => "BOOL",
    };
}

fn freeLoRATargetTensors(allocator: std.mem.Allocator, tensors: []LoRATargetTensor) void {
    for (tensors) |item| {
        allocator.free(item.tensor_name);
        allocator.free(item.module_name);
    }
    allocator.free(tensors);
}

fn freeLoRATensorSummary(allocator: std.mem.Allocator, item: *LoRATensorSummary) void {
    allocator.free(item.base_tensor_name);
    allocator.free(item.adapter_a_tensor_name);
    allocator.free(item.adapter_b_tensor_name);
    if (item.dora_magnitude_tensor_name) |name| allocator.free(name);
    allocator.free(item.module_name);
    item.* = undefined;
}

fn requiredPathInDir(allocator: std.mem.Allocator, dir_path: []const u8, basename: []const u8) ![]u8 {
    return (try optionalPathInDir(allocator, dir_path, basename)) orelse error.FileNotFound;
}

fn optionalPathInDir(allocator: std.mem.Allocator, dir_path: []const u8, basename: []const u8) !?[]u8 {
    const path = try std.fs.path.join(allocator, &.{ dir_path, basename });
    errdefer allocator.free(path);
    compat.cwd().access(compat.io(), path, .{}) catch {
        allocator.free(path);
        return null;
    };
    return path;
}

fn loadOptionalJson(comptime T: type, allocator: std.mem.Allocator, path: []const u8) !?T {
    const bytes = c_file.readFile(allocator, path) catch return null;
    defer allocator.free(bytes);
    return try std.json.parseFromSliceLeaky(T, allocator, bytes, .{ .ignore_unknown_fields = true });
}

fn dupeStringSlice(allocator: std.mem.Allocator, value: []const []const u8) ![][]const u8 {
    const out = try allocator.alloc([]const u8, value.len);
    errdefer allocator.free(out);
    var built: usize = 0;
    errdefer for (out[0..built]) |item| allocator.free(item);
    for (value, 0..) |item, idx| {
        out[idx] = try allocator.dupe(u8, item);
        built += 1;
    }
    return out;
}

fn dupeOptionalStringSlice(allocator: std.mem.Allocator, value: ?[]const []const u8) !?[][]const u8 {
    const items = value orelse return null;
    return try dupeStringSlice(allocator, items);
}

fn dupeOptionalString(allocator: std.mem.Allocator, value: ?[]const u8) !?[]const u8 {
    const item = value orelse return null;
    return try allocator.dupe(u8, item);
}

fn stringSliceContains(haystack: []const []const u8, needle: []const u8) bool {
    for (haystack) |item| {
        if (std.mem.eql(u8, item, needle)) return true;
    }
    return false;
}

pub fn transpose2DF32(out: []f32, input: []const f32, rows: usize, cols: usize) void {
    std.debug.assert(out.len == rows * cols);
    std.debug.assert(input.len == rows * cols);
    for (0..rows) |row| {
        for (0..cols) |col| {
            out[col * rows + row] = input[row * cols + col];
        }
    }
}
