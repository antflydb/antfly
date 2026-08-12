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

// Unified session factory for architecture-based models (BERT, T5, etc).
//
// Given a model directory, loads the manifest, detects the architecture,
// creates the appropriate ComputeBackend (native CPU or Metal), and returns a
// Session that runs the model's forward pass.

const std = @import("std");
const build_options = @import("build_options");
const platform = @import("antfly_platform");
const compat = @import("../io/compat.zig");
const Session = @import("../backends/session.zig").Session;
const ResidentOutputs = @import("../backends/session.zig").ResidentOutputs;
const Tensor = @import("../backends/tensor.zig").Tensor;
const TensorInfo = @import("../backends/tensor.zig").TensorInfo;
const BackendType = @import("../backends/backends.zig").BackendType;
const bert = @import("../models/bert.zig");
const t5_mod = @import("../models/t5.zig");
const gpt_mod = @import("../models/gpt.zig");
const safetensors_mod = @import("../models/safetensors.zig");
const whisper_mod = @import("../models/whisper.zig");
const florence_mod = @import("../models/florence.zig");
const clip_mod = @import("../models/clip.zig");
const clap_mod = @import("../models/clap.zig");
const deberta_mod = @import("../models/deberta.zig");
const layoutlmv3_mod = @import("../models/layoutlmv3.zig");
const bert_arch = @import("bert.zig");
const layoutlmv3_arch = @import("layoutlmv3.zig");
const t5_arch = @import("t5.zig");
const gpt_arch = @import("gpt.zig");
const deepseek_v4_arch = @import("deepseek_v4.zig");
const whisper_arch = @import("whisper.zig");
const clip_arch = @import("clip.zig");
const clip_graph = @import("clip_graph.zig");
const clap_arch = @import("clap.zig");
const florence_arch = @import("florence.zig");
const deberta_arch = @import("deberta.zig");
const gliner_head = @import("gliner_head.zig");
const gliner_head_graph = @import("gliner_head_graph.zig");
const kernel_jit = @import("../graph/kernel_jit.zig");
const graph_runtime = @import("../graph/runtime.zig");
const manifest_mod = @import("../models/manifest.zig");
const ops = @import("../ops/ops.zig");
const NativeCompute = @import("../ops/native_compute.zig").NativeCompute;
const metal_compute_mod = @import("../ops/metal_compute.zig");
const MetalCompute = metal_compute_mod.MetalCompute;
const weight_source_mod = @import("../models/weight_source.zig");
const SafetensorsSource = @import("../models/weight_source.zig").SafetensorsSource;
const LoadedWeight = @import("../models/weight_source.zig").LoadedWeight;
const tensor_store_mod = @import("../models/tensor_store.zig");
const export_source_mod = @import("../models/export_source.zig");
const gguf_mod = @import("../gguf/root.zig");
const c_file = @import("../util/c_file.zig");
const runtime = @import("../runtime/root.zig");
const cuda_load_plan = @import("../ops/cuda/load_plan.zig");

const cuda_compute_mod = if (build_options.enable_cuda) @import("../ops/cuda/cuda_compute.zig") else struct {};
pub const CudaRuntimeStats = if (build_options.enable_cuda) cuda_compute_mod.RuntimeStats else void;
const CudaCapabilityProfile = if (build_options.enable_cuda) cuda_compute_mod.CapabilityProfile else enum {
    clipclap,
    bert_encoder,
    deberta_reranker,
    gliner2,
    florence2,
    gemma4,
};
const GpuHostedQuantExecutionMode = @import("../ops/gpu_hosted_store.zig").QuantExecutionMode;
const GpuHostedCompute = void;
const gpu_hosted_mod = struct {
    pub fn arrayFromTensor(_: anytype, _: anytype, _: anytype) !void {
        unreachable;
    }
    pub fn insertWeight(_: anytype, _: anytype, _: anytype, _: anytype) !void {
        unreachable;
    }
    pub fn loadSafetensors(_: anytype, _: anytype, _: anytype) !void {
        unreachable;
    }
    pub fn check(_: anytype) !void {
        unreachable;
    }
};
const gpu_hosted_quant_mod = struct {
    pub const Provider = void;
    pub fn nullProvider() void {
        return {};
    }
    pub fn defaultProvider() void {
        return {};
    }
};
const gpu_hosted_c = struct {
    pub const resident_stream = void;
    pub const resident_map_string_to_array = void;
    pub const resident_array = void;
    pub fn resident_map_string_to_array_iterator_new(_: anytype) void {
        unreachable;
    }
    pub fn resident_map_string_to_array_iterator_free(_: anytype) c_int {
        unreachable;
    }
    pub fn resident_array_new() void {
        unreachable;
    }
    pub fn resident_array_free(_: anytype) c_int {
        unreachable;
    }
    pub fn resident_map_string_to_array_iterator_next(_: anytype, _: anytype, _: anytype) c_int {
        unreachable;
    }
    pub fn resident_transpose(_: anytype, _: anytype, _: anytype) c_int {
        unreachable;
    }
};
const metal_runtime = if (build_options.enable_metal) @import("../backends/metal_runtime.zig") else struct {
    fn metalDeviceAvailable() bool {
        return false;
    }

    fn validateMetalJitLoadContext(
        config: kernel_jit.Config,
        load_context: kernel_jit.LoadContext,
    ) !void {
        if (!load_context.allowsQualification() and
            (config.mode.failClosed() or config.qualified_profile_path != null or config.profile_capture_only))
        {
            return error.KernelJitRequiredDynamicLoad;
        }
    }
};
const MetalJitRouteScope = if (build_options.enable_metal) metal_runtime.MetalJitRouteScope else void;

const pjrt_lib = if (build_options.enable_pjrt) @import("pjrt") else struct {};

fn directQuantEnabled() bool {
    return !platform.env.getenvBool("TERMITE_NATIVE_DISABLE_DIRECT_QUANT");
}

fn gpuHostedQuantExecutionMode(direct_quant_enabled: bool) GpuHostedQuantExecutionMode {
    if (!direct_quant_enabled) return .prefer_backend_dense;

    const slice = platform.env.getenv("TERMITE_METAL_QUANT_MODE") orelse return .device_native;
    if (std.ascii.eqlIgnoreCase(slice, "dense") or std.ascii.eqlIgnoreCase(slice, "prefer_backend_dense")) {
        return .prefer_backend_dense;
    }
    if (std.ascii.eqlIgnoreCase(slice, "wrapper") or std.ascii.eqlIgnoreCase(slice, "wrapper_direct_quant")) {
        return .wrapper_direct_quant;
    }
    if (std.ascii.eqlIgnoreCase(slice, "device") or std.ascii.eqlIgnoreCase(slice, "device_native")) {
        return .device_native;
    }
    return .device_native;
}

fn parseMetalGemmaKvDTypeOverride(value: []const u8) ?runtime.kv.pool.KvDType {
    if (std.ascii.eqlIgnoreCase(value, "f16")) return .f16;
    if (std.ascii.eqlIgnoreCase(value, "f32")) return .f32;
    return null;
}

fn metalGemmaKvDTypeOverride() ?runtime.kv.pool.KvDType {
    const value = platform.env.getenv("TERMITE_METAL_GEMMA_KV_DTYPE") orelse
        platform.env.getenv("ANTFLY_INFERENCE_METAL_GEMMA_KV_DTYPE") orelse
        return null;
    return parseMetalGemmaKvDTypeOverride(value);
}

fn gpuHostedEagerDenseMaxBytes() u64 {
    const mb = platform.env.getenvUsize("TERMITE_METAL_EAGER_DENSE_MAX_MB") orelse return 1024 * 1024 * 1024;
    return mb * 1024 * 1024;
}

fn forceGpuHostedEagerDenseLoadDebug() bool {
    return platform.env.getenvBool("TERMITE_FORCE_METAL_EAGER_DENSE");
}

fn disablePrefetchWorkerDebug() bool {
    return platform.env.getenvBool("TERMITE_DISABLE_PREFETCH_WORKER");
}

fn graphRuntimeStrategyEnabled(strategy: ?graph_runtime.Strategy) bool {
    return if (strategy) |s| s != .interpreter else false;
}

fn shouldUseSharedGpuHostedEagerDenseLoad(allocator: std.mem.Allocator, mf: manifest_mod.ModelManifest, arch_config: ArchConfig) bool {
    if (forceGpuHostedEagerDenseLoadDebug()) return true;
    switch (arch_config) {
        .gpt => |cfg| if (cfg.usesMoe()) return false,
        else => {},
    }
    const total_bytes = estimateNativeWeightBytes(allocator, mf) catch return false;
    return total_bytes > 0 and total_bytes <= gpuHostedEagerDenseMaxBytes();
}

fn shouldUseMetalHostedEagerDenseLoad(allocator: std.mem.Allocator, mf: manifest_mod.ModelManifest, arch_config: ArchConfig) bool {
    _ = allocator;
    _ = mf;
    _ = arch_config;
    return false;
}

fn shouldUseGpuHostedEagerDenseLoad(
    backend_type: BackendType,
    allocator: std.mem.Allocator,
    mf: manifest_mod.ModelManifest,
    arch_config: ArchConfig,
) bool {
    return switch (backend_type) {
        .metal => shouldUseMetalHostedEagerDenseLoad(allocator, mf, arch_config),
        else => unreachable,
    };
}

fn shouldPreferGpuHostedF32DenseTensors(arch_config: ArchConfig) bool {
    return switch (arch_config) {
        .gpt => |cfg| cfg.family == .gemma,
        .layoutlmv3 => true,
        else => false,
    };
}

fn shouldForceGpuHostedF32DenseTensorByName(arch_config: ArchConfig, name: []const u8) bool {
    if (!shouldPreferGpuHostedF32DenseTensors(arch_config)) return false;
    const is_large_non_linear_weight = std.mem.startsWith(u8, name, "vision_tower.") or
        std.mem.startsWith(u8, name, "multi_modal_projector.") or
        std.mem.endsWith(u8, name, "token_embd.weight");
    if (arch_config == .layoutlmv3) return !is_large_non_linear_weight;
    return !is_large_non_linear_weight;
}

fn estimateGpuHostedResidentTensorBytes(tensor: *const Tensor, force_f32: bool) usize {
    if (force_f32 and (tensor.dtype == .f16 or tensor.dtype == .bf16)) {
        var elements: usize = 1;
        for (tensor.shape) |dim| {
            elements = std.math.mul(usize, elements, @intCast(dim)) catch return tensor.data.len;
        }
        return std.math.mul(usize, elements, @sizeOf(f32)) catch return tensor.data.len;
    }
    return tensor.data.len;
}

pub fn estimateNativeWeightBytes(allocator: std.mem.Allocator, mf: manifest_mod.ModelManifest) !u64 {
    return switch (mf.nativeWeightArtifactKind() orelse return 0) {
        .gguf => blk: {
            var total = try c_file.fileSize(allocator, mf.gguf_path.?);
            if (mf.gliner_head_gguf_path) |head_path| {
                total = std.math.add(u64, total, try c_file.fileSize(allocator, head_path)) catch
                    return error.ResourceLimitExceeded;
            }
            if (mf.gliner_head_safetensors_path) |head_path| {
                total = std.math.add(u64, total, try c_file.fileSize(allocator, head_path)) catch
                    return error.ResourceLimitExceeded;
            }
            break :blk total;
        },
        .safetensors => c_file.fileSize(allocator, mf.safetensors_path.?),
        .sharded_safetensors => shardedSafetensorsTotalBytes(
            allocator,
            mf.safetensors_index_path.?,
        ),
    };
}

/// Keep backend-specific materialization policy behind the session factory.
/// ModelManager owns admission orchestration, while each backend owns the
/// physical representation it will create.
pub fn estimateBackendWeightResidencyBytes(
    backend: BackendType,
    encoded_bytes: usize,
) !usize {
    return switch (backend) {
        .cuda => cuda_load_plan.estimateEncodedArtifactDeviceBytes(encoded_bytes),
        .metal => encoded_bytes,
        .native, .onnx, .wasm => 0,
        .pjrt => error.UnsupportedBackend,
    };
}

fn glinerBaseWeightKey(full_name: []const u8) []const u8 {
    if (std.mem.startsWith(u8, full_name, "encoder.embeddings.") or
        std.mem.startsWith(u8, full_name, "encoder.encoder."))
    {
        return full_name["encoder.".len..];
    }
    return full_name;
}

fn shardedSafetensorsTotalBytes(allocator: std.mem.Allocator, index_path: []const u8) !u64 {
    const index_bytes = try c_file.readFile(allocator, index_path);
    defer allocator.free(index_bytes);

    var index = try safetensors_mod.ShardedIndex.load(allocator, index_bytes);
    defer index.deinit();

    const model_dir = std.fs.path.dirname(index_path) orelse return error.InvalidPath;
    var seen = std.StringHashMapUnmanaged(void){};
    defer {
        var it = seen.iterator();
        while (it.next()) |entry| allocator.free(entry.key_ptr.*);
        seen.deinit(allocator);
    }

    var total: u64 = 0;
    var it = index.weight_map.iterator();
    while (it.next()) |entry| {
        const shard_name = entry.value_ptr.*;
        if (seen.contains(shard_name)) continue;
        try seen.put(allocator, try allocator.dupe(u8, shard_name), {});
        const shard_path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ model_dir, shard_name });
        defer allocator.free(shard_path);
        total += try c_file.fileSize(allocator, shard_path);
    }
    return total;
}

/// Supported model architecture families.
const ArchType = enum {
    bert,
    deberta,
    t5,
    gpt,
    whisper,
    florence,
    clip,
    clap,
    gliner,
    layoutlmv3,
};

/// Architecture-specific config, tagged union.
const ArchConfig = union(ArchType) {
    bert: bert.Config,
    deberta: deberta_mod.Config,
    t5: t5_mod.Config,
    gpt: gpt_mod.Config,
    whisper: whisper_mod.Config,
    florence: florence_mod.Config,
    clip: clip_mod.Config,
    clap: clap_mod.Config,
    gliner: deberta_mod.Config,
    layoutlmv3: layoutlmv3_mod.Config,
};

const SessionTask = enum {
    generic,
    classifier,
    recognizer,
};

pub const TaskOverride = enum {
    generic,
    classifier,
    recognizer,
};

pub const GenericEncoderArchConfig = union(enum) {
    bert: bert.Config,
    deberta: deberta_mod.Config,
};

fn sessionTaskForModelType(model_type: manifest_mod.ModelType, override: ?TaskOverride) SessionTask {
    if (override) |value| {
        return switch (value) {
            .generic => .generic,
            .classifier => .classifier,
            .recognizer => .recognizer,
        };
    }
    return switch (model_type) {
        .classifier, .reranker => .classifier,
        .recognizer => .recognizer,
        else => .generic,
    };
}

pub const UnsupportedTensorTypeCount = struct {
    tensor_type: gguf_mod.tensor_types.TensorType,
    count: usize,
};

pub const GgufTensorSample = struct {
    name: []const u8,
    tensor_type: gguf_mod.tensor_types.TensorType,
    byte_len: u64,
};

pub const GgufInspectionReport = struct {
    allocator: std.mem.Allocator,
    architecture: []const u8,
    tensor_count: usize,
    metadata_count: usize,
    gpt_config: ?gpt_mod.Config = null,
    all_tensor_types: []UnsupportedTensorTypeCount = &.{},
    unsupported_tensor_types: []UnsupportedTensorTypeCount = &.{},
    quantized_tensor_samples: [][]const u8 = &.{},
    dense_tensor_samples: []GgufTensorSample = &.{},
    missing_required_tensors: [][]const u8 = &.{},
    unmapped_tensor_names: [][]const u8 = &.{},
    packed_moe_expert_tensors: [][]const u8 = &.{},

    pub fn deinit(self: *GgufInspectionReport) void {
        self.allocator.free(self.architecture);
        self.allocator.free(self.all_tensor_types);
        self.allocator.free(self.unsupported_tensor_types);
        for (self.quantized_tensor_samples) |name| self.allocator.free(name);
        self.allocator.free(self.quantized_tensor_samples);
        for (self.dense_tensor_samples) |sample| self.allocator.free(sample.name);
        self.allocator.free(self.dense_tensor_samples);
        for (self.missing_required_tensors) |name| self.allocator.free(name);
        self.allocator.free(self.missing_required_tensors);
        for (self.unmapped_tensor_names) |name| self.allocator.free(name);
        self.allocator.free(self.unmapped_tensor_names);
        for (self.packed_moe_expert_tensors) |name| self.allocator.free(name);
        self.allocator.free(self.packed_moe_expert_tensors);
    }
};

pub fn inspectGgufModel(allocator: std.mem.Allocator, model_path: []const u8) !?GgufInspectionReport {
    var mf = try manifest_mod.loadFromDir(allocator, model_path);
    defer mf.deinit();
    if (!mf.usesGgufWeights()) return null;

    const arch_config = try detectArchitecture(allocator, model_path, mf);
    var store = try tensor_store_mod.openFromManifest(allocator, mf);
    defer store.deinit();
    return try buildGgufInspectionReport(allocator, arch_config, store);
}

/// Cold-listing inspection reads only GGUF metadata and tensor headers. In
/// particular, tokenizer vocabulary arrays are skipped rather than allocated.
pub fn inspectGgufModelForListing(
    allocator: std.mem.Allocator,
    model_path: []const u8,
    mf: manifest_mod.ModelManifest,
) !?GgufInspectionReport {
    if (!mf.usesGgufWeights()) return null;
    const gguf_path = mf.gguf_path.?;
    var mapped = try c_file.MmapRegion.init(allocator, gguf_path);
    defer mapped.deinit();

    var file = try gguf_mod.format.parseStructure(allocator, mapped.data);
    defer file.deinit(allocator);
    try gguf_mod.format.validateTensorDataRanges(&file, mapped.data.len);
    const parsed_prefix_len = std.math.cast(usize, file.data_region_offset) orelse mapped.data.len;
    mapped.adviseSequentialPrefix(@min(parsed_prefix_len, mapped.data.len));

    const arch_config = try detectArchitectureWithGgufFile(allocator, model_path, mf, &file);
    return @as(?GgufInspectionReport, try buildGgufInspectionReportFromFile(allocator, arch_config, &file));
}

/// Create a native CPU session from a model directory.
pub fn createNativeSession(allocator: std.mem.Allocator, model_path: []const u8) !Session {
    return createNativeSessionWithTaskOverride(allocator, model_path, null);
}

pub fn createNativeSessionWithTaskOverride(allocator: std.mem.Allocator, model_path: []const u8, override: ?TaskOverride) !Session {
    const direct_quant_enabled = directQuantEnabled();
    const cpu_plan_context = defaultPlanContextForBackend(.cpu);
    var mf = try manifest_mod.loadFromDir(allocator, model_path);
    defer mf.deinit();

    // Detect architecture from config.json
    var arch_config = try detectArchitecture(allocator, model_path, mf);
    // Determine weight prefix for the native backend (strip from source tensor names)
    var store = try tensor_store_mod.openFromManifest(allocator, mf);
    if (mf.usesGgufWeights()) {
        if (try buildGgufInspectionReport(allocator, arch_config, store)) |report| {
            defer {
                var r = report;
                r.deinit();
            }
            try ensureGgufInspectionCompatible(report, mf.gguf_path.?);
        }
    }
    const source = (try store.weightSource()) orelse return error.NoDenseWeightSource;

    const prefix = switch (arch_config) {
        .bert => |cfg| cfg.effectivePrefix(),
        .deberta => "deberta",
        .t5 => "", // T5 weights use full names (encoder.block.0.*, decoder.block.0.*)
        .gpt => "", // GPT weights use full names (model.layers.0.*, h.0.*)
        .whisper => "", // Whisper uses full names (encoder.*, model.decoder.*)
        .florence => "", // Florence2 uses full names (davit.*, model.decoder.*)
        .clip => "", // CLIP uses full names (text_model.*, vision_model.*)
        .clap => "", // CLAP uses full names (text_model.*, audio_model.*)
        .gliner => "encoder", // GLiNER wraps DeBERTa encoder; span_rep/count_embed keep full names
        .layoutlmv3 => |cfg| cfg.effectivePrefix(),
    };

    // Detect prefix override from actual weight names
    const is_gliner = arch_config == .gliner;
    const actual_prefix = blk: {
        if (is_gliner) break :blk prefix; // GLiNER uses "encoder" prefix, no auto-detection
        switch (arch_config) {
            .gpt => |cfg| if (cfg.weight_prefix.len != 0) break :blk "",
            else => {},
        }
        const names = try source.listNames(allocator);
        defer allocator.free(names);
        var detected_prefix = prefix;
        for (names) |name| {
            if (std.mem.startsWith(u8, name, "bert.")) {
                detected_prefix = "bert";
                break;
            } else if (std.mem.startsWith(u8, name, "deberta.")) {
                detected_prefix = "deberta";
                break;
            } else if (std.mem.startsWith(u8, name, "roberta.")) {
                detected_prefix = "roberta";
                break;
            } else if (std.mem.startsWith(u8, name, "distilbert.")) {
                detected_prefix = "distilbert";
                break;
            } else if (std.mem.startsWith(u8, name, "layoutlmv3.")) {
                detected_prefix = "layoutlmv3";
                break;
            } else if (arch_config == .gpt and std.mem.startsWith(u8, name, "model.language_model.")) {
                detected_prefix = "model.language_model";
                break;
            } else if (arch_config == .gpt and std.mem.startsWith(u8, name, "language_model.")) {
                detected_prefix = "language_model";
                break;
            }
        }
        break :blk detected_prefix;
    };

    // Load all weights
    const all_names = try source.listNames(allocator);
    defer allocator.free(all_names);
    try maybeInferGptAttentionLayoutFromStore(allocator, store, all_names, &arch_config);

    var resident_weights = std.StringHashMapUnmanaged(LoadedWeight){};
    var lazy_weights = std.StringHashMapUnmanaged(LazyWeightEntry){};
    errdefer {
        var wit = resident_weights.iterator();
        while (wit.next()) |entry| {
            var w = entry.value_ptr.*;
            w.deinit();
            allocator.free(entry.key_ptr.*);
        }
        resident_weights.deinit(allocator);
        var lit = lazy_weights.iterator();
        while (lit.next()) |entry| {
            if (entry.value_ptr.loaded) |*loaded| loaded.deinit();
            entry.value_ptr.tensor_ref.deinit(allocator);
            allocator.free(entry.key_ptr.*);
        }
        lazy_weights.deinit(allocator);
        store.deinit();
    }

    for (all_names) |full_name| {
        if (try appendPackedMoeLazyWeights(allocator, &lazy_weights, store, arch_config, full_name, cpu_plan_context)) {
            continue;
        }

        // For GLiNER: strip "encoder." prefix only from encoder weights,
        // keep span_rep/count_embed/classifier weights with their full names.
        const base_key = if (is_gliner)
            glinerBaseWeightKey(full_name)
        else if (actual_prefix.len > 0 and std.mem.startsWith(u8, full_name, actual_prefix) and full_name.len > actual_prefix.len and full_name[actual_prefix.len] == '.')
            full_name[actual_prefix.len + 1 ..]
        else
            full_name;
        var key_buf: [256]u8 = undefined;
        const key = try normalizeWeightKey(store.kind(), arch_config, base_key, &key_buf);
        const owned_key = try allocator.dupe(u8, key);
        if (shouldLazyLoadWeight(store.kind(), arch_config, key)) {
            if (lazy_weights.contains(key)) {
                allocator.free(owned_key);
                continue;
            }
            const expert_coord = parseMoeExpertCoord(key);
            const tensor_ref = try store.describeTensor(allocator, full_name);
            try lazy_weights.put(allocator, owned_key, .{
                .tensor_ref = tensor_ref,
                .expert_coord = expert_coord,
                .projection_mask = if (expert_coord != null) projectionMaskForWeightKey(key) else 0,
                .placement = runtime.tier.planner.planForContext(cpu_plan_context, key, tensor_ref.byte_len),
            });
            continue;
        }

        if (direct_quant_enabled and try shouldKeepResidentWeightQuantizedOnly(allocator, store, arch_config, key, full_name)) {
            const tensor_ref = try store.describeTensor(allocator, full_name);
            defer {
                var ref = tensor_ref;
                ref.deinit(allocator);
            }
            const storage = (try store.loadQuantizedStorageRef(&tensor_ref)) orelse {
                allocator.free(owned_key);
                return error.UnsupportedTensorType;
            };
            const weight: LoadedWeight = .{
                .tensor = .{
                    .data = &.{},
                    .dtype = .f32,
                    .shape = &.{},
                    .name = owned_key,
                    .allocator = allocator,
                    .owns_data = false,
                    .owns_shape = false,
                },
                .quantized = true,
                .quantized_storage = storage,
            };
            try resident_weights.put(allocator, owned_key, weight);
            continue;
        }

        var tensor_ref = store.describeTensor(allocator, full_name) catch {
            allocator.free(owned_key);
            continue;
        };
        defer {
            var ref = tensor_ref;
            ref.deinit(allocator);
        }
        var weight = store.loadTensorRef(&tensor_ref) catch {
            allocator.free(owned_key);
            continue;
        };
        errdefer weight.deinit();
        if (!direct_quant_enabled) {
            if (weight.quantized_storage) |*storage| {
                storage.deinit();
                weight.quantized_storage = null;
                weight.quantized = false;
            }
        }
        try resident_weights.put(allocator, owned_key, weight);
    }

    if (store.kind() != .gguf) {
        refineArchConfigFromWeights(&arch_config, &resident_weights);
    }

    // GPT-2 safetensors uses Conv1D layout [in_dim, out_dim] for linear
    // weights. Transpose them to the standard [out_dim, in_dim] layout.
    if (store.kind() == .safetensors) {
        if (arch_config == .gpt and arch_config.gpt.family == .gpt2) {
            try transposeGpt2Conv1dWeights(allocator, &resident_weights);
        }
        try applyJinaV5RetrievalAdapterIfPresent(allocator, model_path, mf, &resident_weights);
    }

    const keep_store = shouldRetainTensorStore(store.kind(), lazy_weights.count());
    const resident_store = if (keep_store) store else null;
    if (!keep_store) store.deinit();
    const moe_num_experts = switch (arch_config) {
        .gpt => |cfg| cfg.num_local_experts,
        else => 0,
    };
    const residency = if (lazy_weights.count() > 0 and moe_num_experts > 0)
        runtime.moe.residency.SharedResidency.init(allocator, defaultResidentExpertsPerLayer(arch_config))
    else
        null;
    const tier_cache = if (lazy_weights.count() > 0)
        runtime.tier.cache.SharedCache.init(runtime.tier.cache.defaultBudgetForBackend(.cpu))
    else
        null;
    errdefer {
        if (residency) |value| {
            var v = value;
            v.deinit();
        }
    }

    const impl = try allocator.create(ArchSession);
    impl.* = .{
        .allocator = allocator,
        .arch_config = arch_config,
        .task = sessionTaskForModelType(mf.model_type, override),
        .backend_type = .native,
        .backend_data = .{ .native = .{
            .allocator = allocator,
            .resident_weights = resident_weights,
            .lazy_weights = lazy_weights,
            .tensor_store = resident_store,
            .moe_num_experts = @intCast(moe_num_experts),
            .residency = residency,
            .tier_cache = tier_cache,
            .allow_direct_quant = direct_quant_enabled,
        } },
    };
    errdefer archClose(impl);
    native_mod.initPrefetchQueue(&impl.backend_data.native, allocator);
    {
        var lazy_it = impl.backend_data.native.lazy_weights.iterator();
        while (lazy_it.next()) |entry| {
            entry.value_ptr.guard = impl.backend_data.native.prefetch.mutexPtr();
        }
    }
    if (impl.backend_data.native.lazy_weights.count() > 0) {
        try native_mod.startPrefetchWorker(&impl.backend_data.native);
    }
    return .{ .ptr = impl, .vtable = &arch_vtable };
}

/// Create a PJRT-backed session from a model directory.
///
/// Weights are loaded via the native CPU host backend. A PJRT client is
/// initialized via `Client.initFromEnv` which searches for the plugin in:
///   1. `PJRT_PLUGIN_PATH` env var
///   2. `~/Library/Application Support/go-xla/pjrt_c_api_cpu_plugin.dylib`
///   3. `~/.antfly/inference/pjrt/darwin-arm64/pjrt_c_api_cpu_plugin.dylib`
///
/// If the plugin is not found, the session is created anyway but without a
/// PJRT client (falls back to pure native CPU execution — no XLA-compiled
/// partitions). Callers can check `getPjrtClientPtr` to see whether a client
/// is actually available.
///
/// For graph-mode (generation) workloads the caller should pass the client
/// pointer to `NativeGenerationPipeline.pjrt_client` so that compiled HLO
/// partitions are dispatched through `attachPjrtExecutors`.
pub fn createPjrtSession(allocator: std.mem.Allocator, model_path: []const u8) !Session {
    return createPjrtSessionWithTaskOverride(allocator, model_path, null);
}

pub fn createPjrtSessionWithTaskOverride(allocator: std.mem.Allocator, model_path: []const u8, override: ?TaskOverride) !Session {
    // PJRT weights are served through native CPU — re-use the full native CPU load path.
    const direct_quant_enabled = directQuantEnabled();
    const cpu_plan_context = defaultPlanContextForBackend(.cpu);
    var mf = try manifest_mod.loadFromDir(allocator, model_path);
    defer mf.deinit();

    var arch_config = try detectArchitecture(allocator, model_path, mf);
    var store = try tensor_store_mod.openFromManifest(allocator, mf);
    if (mf.usesGgufWeights()) {
        if (try buildGgufInspectionReport(allocator, arch_config, store)) |report| {
            defer {
                var r = report;
                r.deinit();
            }
            try ensureGgufInspectionCompatible(report, mf.gguf_path.?);
        }
    }
    const source = (try store.weightSource()) orelse return error.NoDenseWeightSource;

    const prefix = switch (arch_config) {
        .bert => |cfg| cfg.effectivePrefix(),
        .deberta => "deberta",
        .t5 => "",
        .gpt => "",
        .whisper => "",
        .florence => "",
        .clip => "",
        .clap => "",
        .gliner => "encoder",
        .layoutlmv3 => |cfg| cfg.effectivePrefix(),
    };

    const is_gliner = arch_config == .gliner;
    const actual_prefix = blk: {
        if (is_gliner) break :blk prefix;
        switch (arch_config) {
            .gpt => |cfg| if (cfg.weight_prefix.len != 0) break :blk "",
            else => {},
        }
        const names = try source.listNames(allocator);
        defer allocator.free(names);
        var detected_prefix = prefix;
        for (names) |name| {
            if (std.mem.startsWith(u8, name, "bert.")) {
                detected_prefix = "bert";
                break;
            } else if (std.mem.startsWith(u8, name, "deberta.")) {
                detected_prefix = "deberta";
                break;
            } else if (std.mem.startsWith(u8, name, "roberta.")) {
                detected_prefix = "roberta";
                break;
            } else if (std.mem.startsWith(u8, name, "distilbert.")) {
                detected_prefix = "distilbert";
                break;
            } else if (std.mem.startsWith(u8, name, "layoutlmv3.")) {
                detected_prefix = "layoutlmv3";
                break;
            } else if (arch_config == .gpt and std.mem.startsWith(u8, name, "model.language_model.")) {
                detected_prefix = "model.language_model";
                break;
            } else if (arch_config == .gpt and std.mem.startsWith(u8, name, "language_model.")) {
                detected_prefix = "language_model";
                break;
            }
        }
        break :blk detected_prefix;
    };

    // Load all weights (same logic as createNativeSessionWithTaskOverride).
    const all_names = try source.listNames(allocator);
    defer allocator.free(all_names);
    try maybeInferGptAttentionLayoutFromStore(allocator, store, all_names, &arch_config);

    var resident_weights = std.StringHashMapUnmanaged(LoadedWeight){};
    var lazy_weights = std.StringHashMapUnmanaged(LazyWeightEntry){};
    errdefer {
        var wit = resident_weights.iterator();
        while (wit.next()) |entry| {
            var w = entry.value_ptr.*;
            w.deinit();
            allocator.free(entry.key_ptr.*);
        }
        resident_weights.deinit(allocator);
        var lit = lazy_weights.iterator();
        while (lit.next()) |entry| {
            if (entry.value_ptr.loaded) |*loaded| loaded.deinit();
            entry.value_ptr.tensor_ref.deinit(allocator);
            allocator.free(entry.key_ptr.*);
        }
        lazy_weights.deinit(allocator);
        store.deinit();
    }

    for (all_names) |full_name| {
        if (try appendPackedMoeLazyWeights(allocator, &lazy_weights, store, arch_config, full_name, cpu_plan_context)) {
            continue;
        }

        const base_key = if (is_gliner)
            glinerBaseWeightKey(full_name)
        else if (actual_prefix.len > 0 and std.mem.startsWith(u8, full_name, actual_prefix) and full_name.len > actual_prefix.len and full_name[actual_prefix.len] == '.')
            full_name[actual_prefix.len + 1 ..]
        else
            full_name;
        var key_buf: [256]u8 = undefined;
        const key = try normalizeWeightKey(store.kind(), arch_config, base_key, &key_buf);
        const owned_key = try allocator.dupe(u8, key);
        if (shouldLazyLoadWeight(store.kind(), arch_config, key)) {
            if (lazy_weights.contains(key)) {
                allocator.free(owned_key);
                continue;
            }
            const expert_coord = parseMoeExpertCoord(key);
            const tensor_ref = try store.describeTensor(allocator, full_name);
            try lazy_weights.put(allocator, owned_key, .{
                .tensor_ref = tensor_ref,
                .expert_coord = expert_coord,
                .projection_mask = if (expert_coord != null) projectionMaskForWeightKey(key) else 0,
                .placement = runtime.tier.planner.planForContext(cpu_plan_context, key, tensor_ref.byte_len),
            });
            continue;
        }

        if (direct_quant_enabled and try shouldKeepResidentWeightQuantizedOnly(allocator, store, arch_config, key, full_name)) {
            const tensor_ref = try store.describeTensor(allocator, full_name);
            defer {
                var ref = tensor_ref;
                ref.deinit(allocator);
            }
            const storage = (try store.loadQuantizedStorageRef(&tensor_ref)) orelse {
                allocator.free(owned_key);
                return error.UnsupportedTensorType;
            };
            const weight: LoadedWeight = .{
                .tensor = .{
                    .data = &.{},
                    .dtype = .f32,
                    .shape = &.{},
                    .name = owned_key,
                    .allocator = allocator,
                    .owns_data = false,
                    .owns_shape = false,
                },
                .quantized = true,
                .quantized_storage = storage,
            };
            try resident_weights.put(allocator, owned_key, weight);
            continue;
        }

        var tensor_ref = store.describeTensor(allocator, full_name) catch {
            allocator.free(owned_key);
            continue;
        };
        defer {
            var ref = tensor_ref;
            ref.deinit(allocator);
        }
        var weight = store.loadTensorRef(&tensor_ref) catch {
            allocator.free(owned_key);
            continue;
        };
        errdefer weight.deinit();
        if (!direct_quant_enabled) {
            if (weight.quantized_storage) |*storage| {
                storage.deinit();
                weight.quantized_storage = null;
                weight.quantized = false;
            }
        }
        try resident_weights.put(allocator, owned_key, weight);
    }

    if (store.kind() != .gguf) {
        refineArchConfigFromWeights(&arch_config, &resident_weights);
    }

    if (store.kind() == .safetensors) {
        try applyJinaV5RetrievalAdapterIfPresent(allocator, model_path, mf, &resident_weights);
    }

    const keep_store = shouldRetainTensorStore(store.kind(), lazy_weights.count());
    const resident_store = if (keep_store) store else null;
    if (!keep_store) store.deinit();
    const moe_num_experts = switch (arch_config) {
        .gpt => |cfg| cfg.num_local_experts,
        else => 0,
    };
    const residency = if (lazy_weights.count() > 0 and moe_num_experts > 0)
        runtime.moe.residency.SharedResidency.init(allocator, defaultResidentExpertsPerLayer(arch_config))
    else
        null;
    const tier_cache = if (lazy_weights.count() > 0)
        runtime.tier.cache.SharedCache.init(runtime.tier.cache.defaultBudgetForBackend(.cpu))
    else
        null;
    errdefer {
        if (residency) |value| {
            var v = value;
            v.deinit();
        }
    }

    // Attempt to initialize the PJRT client. On failure (plugin not found,
    // etc.) we log a warning and proceed with pure native CPU execution. The
    // compiled-partition path in generation.zig simply won't be activated.
    const pjrt_client: if (build_options.enable_pjrt) ?pjrt_lib.pjrt.Client else void = blk: {
        if (!build_options.enable_pjrt) break :blk {};
        const client = pjrt_lib.pjrt.Client.initFromEnv(allocator) catch |err| {
            std.log.warn("PJRT plugin not found ({s}); PJRT session will use native CPU fallback", .{@errorName(err)});
            break :blk null;
        };
        std.log.info("PJRT client initialized via session factory", .{});
        break :blk client;
    };

    const impl = try allocator.create(ArchSession);
    impl.* = .{
        .allocator = allocator,
        .arch_config = arch_config,
        .task = sessionTaskForModelType(mf.model_type, override),
        .backend_type = .pjrt,
        .backend_data = .{ .pjrt = .{
            .native = .{
                .allocator = allocator,
                .resident_weights = resident_weights,
                .lazy_weights = lazy_weights,
                .tensor_store = resident_store,
                .moe_num_experts = @intCast(moe_num_experts),
                .residency = residency,
                .tier_cache = tier_cache,
                .allow_direct_quant = direct_quant_enabled,
            },
            .client = pjrt_client,
        } },
    };
    errdefer archClose(impl);
    native_mod.initPrefetchQueue(&impl.backend_data.pjrt.native, allocator);
    {
        var lazy_it = impl.backend_data.pjrt.native.lazy_weights.iterator();
        while (lazy_it.next()) |entry| {
            entry.value_ptr.guard = impl.backend_data.pjrt.native.prefetch.mutexPtr();
        }
    }
    if (impl.backend_data.pjrt.native.lazy_weights.count() > 0) {
        try native_mod.startPrefetchWorker(&impl.backend_data.pjrt.native);
    }
    return .{ .ptr = impl, .vtable = &arch_vtable };
}

/// Return the PJRT client as a type-erased `*anyopaque` pointer, or null if
/// this is not a PJRT session or the client was not initialized.
///
/// Intended for callers that need to populate
/// `NativeGenerationPipeline.pjrt_client` so that compiled HLO partitions
/// are dispatched through PJRT during graph-mode generation:
///
///   pipeline.pjrt_client = session_factory.getPjrtClientPtr(session);
pub fn getPjrtClientPtr(session: Session) ?*anyopaque {
    if (!build_options.enable_pjrt) return null;
    if (session.vtable != &arch_vtable) return null;
    const self: *ArchSession = @ptrCast(@alignCast(session.ptr));
    if (self.backend_type != .pjrt) return null;
    // client is ?pjrt_lib.pjrt.Client only when enable_pjrt is true (void otherwise).
    // The enable_pjrt check above ensures we only reach here with the real type.
    if (build_options.enable_pjrt) {
        if (self.backend_data.pjrt.client) |*client| {
            return @ptrCast(client);
        }
    }
    return null;
}

pub fn createMetalSession(allocator: std.mem.Allocator, model_path: []const u8) !Session {
    return createMetalSessionWithKernelJit(allocator, model_path, .{});
}

pub fn createMetalSessionWithTaskOverride(allocator: std.mem.Allocator, model_path: []const u8, override: ?TaskOverride) !Session {
    return createMetalSessionWithTaskOverrideAndKernelJit(allocator, model_path, override, .{});
}

pub fn createMetalSessionWithKernelJit(
    allocator: std.mem.Allocator,
    model_path: []const u8,
    config: kernel_jit.Config,
) !Session {
    return createMetalSessionWithKernelJitAndLoadContext(allocator, model_path, config, .dynamic);
}

pub fn createMetalSessionWithKernelJitAndLoadContext(
    allocator: std.mem.Allocator,
    model_path: []const u8,
    config: kernel_jit.Config,
    load_context: kernel_jit.LoadContext,
) !Session {
    return createMetalSessionWithTaskOverrideAndKernelJitAndLoadContext(allocator, model_path, null, config, load_context);
}

pub fn createMetalSessionWithTaskOverrideAndKernelJit(
    allocator: std.mem.Allocator,
    model_path: []const u8,
    override: ?TaskOverride,
    config: kernel_jit.Config,
) !Session {
    return createMetalSessionWithTaskOverrideAndKernelJitAndLoadContext(allocator, model_path, override, config, .dynamic);
}

pub fn createMetalSessionWithTaskOverrideAndKernelJitAndLoadContext(
    allocator: std.mem.Allocator,
    model_path: []const u8,
    override: ?TaskOverride,
    config: kernel_jit.Config,
    load_context: kernel_jit.LoadContext,
) !Session {
    return createGpuHostedSessionWithTaskOverride(allocator, model_path, override, .metal, config, load_context);
}

pub fn createCudaSession(allocator: std.mem.Allocator, model_path: []const u8) !Session {
    return createCudaSessionWithKernelJit(allocator, model_path, .{});
}

pub fn createCudaSessionWithTaskOverride(allocator: std.mem.Allocator, model_path: []const u8, override: ?TaskOverride) !Session {
    return createCudaSessionWithTaskOverrideAndKernelJit(allocator, model_path, override, .{});
}

pub fn createCudaSessionWithKernelJit(
    allocator: std.mem.Allocator,
    model_path: []const u8,
    config: kernel_jit.Config,
) !Session {
    return createCudaSessionWithKernelJitAndLoadContext(allocator, model_path, config, .dynamic);
}

pub fn createCudaSessionWithKernelJitAndLoadContext(
    allocator: std.mem.Allocator,
    model_path: []const u8,
    config: kernel_jit.Config,
    load_context: kernel_jit.LoadContext,
) !Session {
    return createCudaSessionWithTaskOverrideAndKernelJitAndLoadContext(
        allocator,
        model_path,
        null,
        config,
        load_context,
    );
}

pub fn createCudaSessionWithTaskOverrideAndKernelJit(
    allocator: std.mem.Allocator,
    model_path: []const u8,
    override: ?TaskOverride,
    config: kernel_jit.Config,
) !Session {
    return createCudaSessionWithTaskOverrideAndKernelJitAndLoadContext(
        allocator,
        model_path,
        override,
        config,
        .dynamic,
    );
}

pub fn createCudaSessionWithTaskOverrideAndKernelJitAndLoadContext(
    allocator: std.mem.Allocator,
    model_path: []const u8,
    override: ?TaskOverride,
    config: kernel_jit.Config,
    load_context: kernel_jit.LoadContext,
) !Session {
    if (comptime !build_options.enable_cuda) return error.CudaNotEnabled;
    try config.validate();
    if (config.mode.failClosed() and !load_context.allowsQualification()) {
        return error.KernelJitRequiredDynamicLoad;
    }

    const debug_cuda_session = platform.env.getenvBool("ANTFLY_INFERENCE_DEBUG_CUDA_SESSION");
    if (debug_cuda_session) std.log.info("cuda-session: create native session start path={s}", .{model_path});
    var native_session = try createNativeSessionWithTaskOverride(allocator, model_path, override);
    defer native_session.close();
    if (debug_cuda_session) std.log.info("cuda-session: create native session done path={s}", .{model_path});
    const native_impl: *ArchSession = @ptrCast(@alignCast(native_session.ptr));
    if (native_impl.backend_type != .native) return error.InvalidBackend;
    const cuda_profile = cudaProfileForArch(native_impl.arch_config) orelse return error.UnsupportedCudaArchitecture;
    const jit_scope = cuda_compute_mod.kernelJitRouteScopeForLoadedWeights(
        cuda_profile,
        &native_impl.backend_data.native.resident_weights,
    );

    if (debug_cuda_session) std.log.info("cuda-session: init cuda compute start path={s}", .{model_path});
    var cuda_compute = try cuda_compute_mod.CudaCompute.initWithKernelJitForScopeAndLoadContext(
        allocator,
        config,
        cuda_profile,
        jit_scope,
        load_context,
    );
    errdefer cuda_compute.deinit();
    if (debug_cuda_session) std.log.info("cuda-session: init cuda compute done path={s}", .{model_path});

    if (debug_cuda_session) std.log.info("cuda-session: require profile {s}", .{@tagName(cuda_profile)});
    try cuda_compute.requireProfile(cuda_profile);
    var it = native_impl.backend_data.native.resident_weights.iterator();
    var resident_count: usize = 0;
    while (it.next()) |entry| {
        const owned_key = try allocator.dupe(u8, entry.key_ptr.*);
        cuda_compute.insertWeightFromLoaded(owned_key, entry.value_ptr) catch |err| {
            allocator.free(owned_key);
            return err;
        };
        resident_count += 1;
    }
    if (debug_cuda_session) std.log.info("cuda-session: uploaded resident weights count={d}", .{resident_count});
    const upload_stats = cuda_compute.snapshotStats();
    if (upload_stats.bf16_mirror_weight_count > 0) {
        // The default-on prefill mirrors trade device memory for cuBLASLt
        // prefill speed; say so at load time so an OOM investigation can see
        // the cost and the switch without reading source.
        std.log.info(
            "cuda: attached {d} BF16 prefill weight mirrors (+{d} MiB device memory); ANTFLY_INFERENCE_CUDA_BERT_Q4_0_BF16_PREFILL=0 disables",
            .{ upload_stats.bf16_mirror_weight_count, upload_stats.bf16_mirror_weight_bytes / (1024 * 1024) },
        );
    }

    const impl = try allocator.create(ArchSession);
    impl.* = .{
        .allocator = allocator,
        .arch_config = native_impl.arch_config,
        .task = native_impl.task,
        .backend_type = .cuda,
        .kernel_jit_config = config,
        .backend_data = .{ .cuda = .{ .compute = cuda_compute } },
    };
    if (debug_cuda_session) std.log.info("cuda-session: return session path={s}", .{model_path});
    return .{ .ptr = impl, .vtable = &arch_vtable };
}

fn cudaSupportsArch(arch_config: ArchConfig) bool {
    return cudaProfileForArch(arch_config) != null;
}

fn cudaProfileForArch(arch_config: ArchConfig) ?CudaCapabilityProfile {
    return switch (arch_config) {
        .clip, .clap => .clipclap,
        .bert => .bert_encoder,
        .deberta => .deberta_reranker,
        .gliner => .gliner2,
        .florence => .florence2,
        .gpt => |cfg| if (cfg.family == .gemma) .gemma4 else null,
        else => null,
    };
}

test "cuda support gate admits only supported encoder architectures" {
    try std.testing.expect(cudaSupportsArch(.{ .gpt = .{ .family = .gemma } }));
    try std.testing.expect(!cudaSupportsArch(.{ .gpt = .{ .family = .qwen2 } }));
    try std.testing.expect(cudaSupportsArch(.{ .clip = .{} }));
    try std.testing.expect(cudaSupportsArch(.{ .clap = .{} }));
    try std.testing.expect(cudaSupportsArch(.{ .bert = .{} }));
    try std.testing.expect(cudaSupportsArch(.{ .deberta = .{} }));
    try std.testing.expect(cudaSupportsArch(.{ .gliner = .{} }));
    try std.testing.expect(cudaSupportsArch(.{ .florence = .{} }));
    if (comptime build_options.enable_cuda) {
        try std.testing.expectEqual(CudaCapabilityProfile.clipclap, cudaProfileForArch(.{ .clip = .{} }).?);
        try std.testing.expectEqual(CudaCapabilityProfile.bert_encoder, cudaProfileForArch(.{ .bert = .{} }).?);
        try std.testing.expectEqual(CudaCapabilityProfile.deberta_reranker, cudaProfileForArch(.{ .deberta = .{} }).?);
        try std.testing.expectEqual(CudaCapabilityProfile.gliner2, cudaProfileForArch(.{ .gliner = .{} }).?);
        try std.testing.expectEqual(CudaCapabilityProfile.florence2, cudaProfileForArch(.{ .florence = .{} }).?);
        try std.testing.expectEqual(CudaCapabilityProfile.gemma4, cudaProfileForArch(.{ .gpt = .{ .family = .gemma } }).?);
    }
}

test "CUDA runtime JIT required dynamic session rejects before model access" {
    if (comptime !build_options.enable_cuda) return error.SkipZigTest;
    try std.testing.expectError(
        error.KernelJitRequiredDynamicLoad,
        createCudaSessionWithKernelJitAndLoadContext(
            std.testing.allocator,
            "/private/tmp/antfly-runtime-jit-intentionally-missing-model",
            .{ .mode = .required },
            .dynamic,
        ),
    );
}

test "Metal JIT load policy is backend independent" {
    try metal_runtime.validateMetalJitLoadContext(.{ .mode = .on }, .dynamic);
    try std.testing.expectError(
        error.KernelJitRequiredDynamicLoad,
        metal_runtime.validateMetalJitLoadContext(.{ .mode = .required }, .dynamic),
    );
    try std.testing.expectError(
        error.KernelJitRequiredDynamicLoad,
        metal_runtime.validateMetalJitLoadContext(.{
            .mode = .on,
            .qualified_profile_path = "/tmp/qualified-profile.json",
        }, .dynamic),
    );
    try std.testing.expectError(
        error.KernelJitRequiredDynamicLoad,
        metal_runtime.validateMetalJitLoadContext(.{
            .mode = .shadow,
            .profile_capture_only = true,
        }, .dynamic),
    );
    try metal_runtime.validateMetalJitLoadContext(.{
        .mode = .on,
        .qualified_profile_path = "/tmp/qualified-profile.json",
    }, .startup_preload);
}

fn eagerLoadResidentsFromStore(
    allocator: std.mem.Allocator,
    resident_weights: anytype,
    tensor_store: tensor_store_mod.TensorStore,
    mf: manifest_mod.ModelManifest,
    arch_config: *ArchConfig,
    all_names: [][]const u8,
    source: weight_source_mod.WeightSource,
    actual_prefix: []const u8,
    is_gliner: bool,
) !usize {
    if (comptime !false) return error.GpuHostedNotEnabled;
    var resident_weight_estimate_bytes: usize = 0;
    var eager_sharded_source: ?*weight_source_mod.ShardedSafetensorsSource = null;
    defer if (eager_sharded_source) |src| src.weightSource().deinit();
    if (mf.safetensors_index_path) |index_path| {
        eager_sharded_source = try weight_source_mod.ShardedSafetensorsSource.initAbsolute(allocator, index_path);
    }
    for (all_names) |full_name| {
        if (isPackedMoeExpertTensor(full_name)) continue;
        const base_key = if (is_gliner)
            glinerBaseWeightKey(full_name)
        else if (actual_prefix.len > 0 and std.mem.startsWith(u8, full_name, actual_prefix) and full_name.len > actual_prefix.len and full_name[actual_prefix.len] == '.')
            full_name[actual_prefix.len + 1 ..]
        else
            full_name;
        var key_buf: [256]u8 = undefined;
        const key = try normalizeWeightKey(tensor_store.kind(), arch_config.*, base_key, &key_buf);

        const source_copy = source;
        var loaded = if (eager_sharded_source) |src| loaded_blk: {
            const resolved = src.findTensorMeta(full_name) catch break :loaded_blk source_copy.getTensor(full_name) catch continue;
            break :loaded_blk LoadedWeight{
                .tensor = try resolved.reader.readTensor(full_name),
                .quantized = false,
            };
        } else source_copy.getTensor(full_name) catch continue;
        defer loaded.deinit();

        if (arch_config.* == .gpt and arch_config.gpt.family == .gpt2 and tensor_store.kind() == .safetensors) {
            try transposeGpt2Conv1dLoadedWeightInPlace(allocator, key, &loaded);
        }

        const force_f32 = shouldForceGpuHostedF32DenseTensorByName(arch_config.*, full_name);
        resident_weight_estimate_bytes += estimateGpuHostedResidentTensorBytes(&loaded.tensor, force_f32);
        const arr = try gpu_hosted_mod.arrayFromTensor(allocator, &loaded.tensor, force_f32);
        try gpu_hosted_mod.insertWeight(resident_weights.*, allocator, key, arr);
    }
    try refineArchConfigFromStore(allocator, tensor_store, all_names, arch_config);
    return resident_weight_estimate_bytes;
}

fn loadSafetensorsIntoResident(
    allocator: std.mem.Allocator,
    stream: GpuHostedStream,
    resident_weights: anytype,
    st_path: []const u8,
    arch_config: ArchConfig,
) ![]const u8 {
    if (comptime !false) return error.GpuHostedNotEnabled;
    resident_weights.* = try gpu_hosted_mod.loadSafetensors(st_path, allocator, stream);
    if (arch_config == .gpt and arch_config.gpt.family == .gpt2) {
        try transposeGpt2Conv1dResidentGpuHostedWeights(allocator, resident_weights, stream);
    }
    return switch (arch_config) {
        .t5, .gpt, .whisper, .florence, .clip, .clap => "",
        .gliner => "encoder",
        .deberta => "deberta",
        .layoutlmv3 => "layoutlmv3",
        .bert => |cfg| detected: {
            var detected = cfg.effectivePrefix();
            const it = gpu_hosted_c.resident_map_string_to_array_iterator_new(resident_weights.*);
            defer _ = gpu_hosted_c.resident_map_string_to_array_iterator_free(it);
            while (true) {
                var key: [*c]const u8 = null;
                var val = gpu_hosted_c.resident_array_new();
                defer _ = gpu_hosted_c.resident_array_free(val);
                if (gpu_hosted_c.resident_map_string_to_array_iterator_next(&key, &val, it) != 0) break;
                if (key == null) break;
                const name = std.mem.span(key);
                if (std.mem.startsWith(u8, name, "bert.")) {
                    detected = "bert";
                    break;
                } else if (std.mem.startsWith(u8, name, "deberta.")) {
                    detected = "deberta";
                    break;
                } else if (std.mem.startsWith(u8, name, "roberta.")) {
                    detected = "roberta";
                    break;
                } else if (std.mem.startsWith(u8, name, "distilbert.")) {
                    detected = "distilbert";
                    break;
                } else if (arch_config == .gpt and std.mem.startsWith(u8, name, "language_model.")) {
                    detected = "language_model";
                    break;
                }
            }
            break :detected detected;
        },
    };
}

fn createGpuHostedSessionWithTaskOverride(
    allocator: std.mem.Allocator,
    model_path: []const u8,
    override: ?TaskOverride,
    backend_type: BackendType,
    kernel_jit_config: kernel_jit.Config,
    kernel_jit_load_context: kernel_jit.LoadContext,
) !Session {
    try kernel_jit_config.validate();
    try metal_runtime.validateMetalJitLoadContext(kernel_jit_config, kernel_jit_load_context);
    try ensureGpuHostedSessionAvailable(backend_type);
    const direct_quant_enabled = directQuantEnabled();
    const quant_mode = gpuHostedQuantExecutionMode(direct_quant_enabled);

    var mf = try manifest_mod.loadFromDir(allocator, model_path);
    defer mf.deinit();
    var gpu_jina_lora_adapter: ?*gpu_hosted_store_mod.JinaLoraAdapter = null;
    errdefer if (gpu_jina_lora_adapter) |adapter| adapter.destroy();
    const model_weight_bytes = estimateNativeWeightBytes(allocator, mf) catch 0;

    var arch_config = try detectArchitecture(allocator, model_path, mf);
    var metal_jit_scope: MetalJitRouteScope = if (build_options.enable_metal)
        metal_runtime.MetalJitRouteScope.none()
    else {};
    if (mf.usesGgufWeights()) {
        var report_opt = try inspectGgufModel(allocator, model_path);
        defer if (report_opt) |*report| report.deinit();
        if (report_opt) |report| {
            try ensureGgufInspectionCompatible(report, mf.gguf_path.?);
            if (backend_type == .metal) {
                try ensureMetalGgufInspectionCompatible(report, mf.gguf_path.?);
            }
        }
    }

    var lazy_weights = std.StringHashMapUnmanaged(gpu_hosted_store_mod.LazyWeightEntry){};
    var tensor_store: ?tensor_store_mod.TensorStore = null;
    var backend_resources_transferred = false;
    errdefer {
        if (!backend_resources_transferred) {
            var it = lazy_weights.iterator();
            while (it.next()) |entry| {
                if (entry.value_ptr.quantized_storage) |*storage| storage.deinit();
                if (entry.value_ptr.host_loaded) |*host_loaded| host_loaded.deinit();
                entry.value_ptr.tensor_ref.deinit(allocator);
                allocator.free(entry.key_ptr.*);
            }
            lazy_weights.deinit(allocator);
            if (tensor_store) |store| store.deinit();
        }
    }

    var eager_dense = false;
    const resident_weight_bytes_override: usize = 0;
    const budget_policy = gpuHostedBudgetPolicy(backend_type, model_weight_bytes, mf, arch_config, quant_mode);
    const prefer_f32_dense_tensors = budget_policy.prefer_f32_dense_tensors;
    const budget_floor = budget_policy.budget_floor;
    const shared_cache_floor = budget_policy.shared_cache_floor;
    const plan_context = budget_policy.plan_context;

    const resident_prefix: []const u8 = if (mf.safetensors_path != null or mf.safetensors_index_path != null or mf.gguf_path != null) blk: {
        tensor_store = try tensor_store_mod.openFromManifest(allocator, mf);
        const source = (try tensor_store.?.weightSource()) orelse return error.NoDenseWeightSource;
        const all_names = try source.listNames(allocator);
        defer allocator.free(all_names);
        try maybeInferGptAttentionLayoutFromStore(allocator, tensor_store.?, all_names, &arch_config);
        const is_gliner = arch_config == .gliner;
        const actual_prefix = detected: {
            if (is_gliner) break :detected "encoder";
            switch (arch_config) {
                .gpt => |cfg| if (cfg.weight_prefix.len != 0) break :detected "",
                else => {},
            }
            var detected_prefix: []const u8 = switch (arch_config) {
                .bert => |cfg| cfg.effectivePrefix(),
                .deberta => "deberta",
                else => "",
            };
            for (all_names) |name| {
                if (std.mem.startsWith(u8, name, "bert.")) {
                    detected_prefix = "bert";
                    break;
                } else if (std.mem.startsWith(u8, name, "deberta.")) {
                    detected_prefix = "deberta";
                    break;
                } else if (std.mem.startsWith(u8, name, "roberta.")) {
                    detected_prefix = "roberta";
                    break;
                } else if (std.mem.startsWith(u8, name, "distilbert.")) {
                    detected_prefix = "distilbert";
                    break;
                } else if (arch_config == .gpt and std.mem.startsWith(u8, name, "model.language_model.")) {
                    detected_prefix = "model.language_model";
                    break;
                } else if (arch_config == .gpt and std.mem.startsWith(u8, name, "language_model.")) {
                    detected_prefix = "language_model";
                    break;
                }
            }
            break :detected detected_prefix;
        };
        eager_dense = shouldUseGpuHostedEagerDenseLoad(backend_type, allocator, mf, arch_config);
        if (eager_dense) {
            if (comptime !false) {
                std.log.err("metal backend no longer supports eager dense resident loading", .{});
                return error.EagerDenseLoadRequiresGpuHosted;
            }
            std.log.err("metal backend no longer supports eager dense resident loading", .{});
            return error.EagerDenseLoadUnsupported;
        } else {
            for (all_names) |full_name| {
                if (try appendPackedMoeLazyWeights(allocator, &lazy_weights, tensor_store.?, arch_config, full_name, plan_context)) {
                    continue;
                }
                const base_key = if (is_gliner)
                    glinerBaseWeightKey(full_name)
                else if (actual_prefix.len > 0 and std.mem.startsWith(u8, full_name, actual_prefix) and full_name.len > actual_prefix.len and full_name[actual_prefix.len] == '.')
                    full_name[actual_prefix.len + 1 ..]
                else
                    full_name;
                var key_buf: [256]u8 = undefined;
                const key = try normalizeWeightKey(tensor_store.?.kind(), arch_config, base_key, &key_buf);
                if (lazy_weights.contains(key)) continue;
                const expert_coord = parseMoeExpertCoord(key);
                const tensor_ref = try tensor_store.?.describeTensor(allocator, full_name);
                try lazy_weights.put(allocator, try allocator.dupe(u8, key), .{
                    .tensor_ref = tensor_ref,
                    .expert_coord = expert_coord,
                    .projection_mask = if (expert_coord != null) projectionMaskForWeightKey(key) else 0,
                    .placement = runtime.tier.planner.planForContext(plan_context, key, tensor_ref.byte_len),
                    .prefer_dense = shouldKeepGpuHostedLazyWeightDense(backend_type, arch_config, key),
                });
            }
            if (tensor_store.?.kind() != .gguf) {
                try refineArchConfigFromStore(allocator, tensor_store.?, all_names, &arch_config);
            }
            if (backend_type == .metal and std.mem.eql(u8, mf.config_model_arch, "jina_embeddings_v5")) {
                if (try jinaRetrievalAdapterPaths(allocator, model_path)) |paths| {
                    defer allocator.free(paths.config);
                    defer allocator.free(paths.weights);
                    const cfg = try parseJinaLoraConfig(allocator, paths.config);
                    gpu_jina_lora_adapter = try gpu_hosted_store_mod.JinaLoraAdapter.create(allocator, paths.weights, cfg.scale());
                }
            }
        }
        break :blk "";
    } else return error.NoSafetensorsFile;

    const moe_num_experts = switch (arch_config) {
        .gpt => |cfg| cfg.num_local_experts,
        else => 0,
    };
    const residency = if (lazy_weights.count() > 0 and moe_num_experts > 0)
        runtime.moe.residency.SharedResidency.init(allocator, defaultResidentExpertsPerLayer(arch_config))
    else
        null;
    const tier_cache = if (lazy_weights.count() > 0) blk: {
        var budget = runtime.tier.cache.defaultBudgetForBackend(.gpu);
        if (shouldUseLargeGpuHostedLazyQuantBudgets(model_weight_bytes, mf, quant_mode, eager_dense)) {
            const floor = recommendedGpuHostedLazyQuantSharedCacheBudget(model_weight_bytes, quant_mode);
            budget.host_limit_bytes = @max(budget.host_limit_bytes, floor.host_limit_bytes);
            budget.backend_limit_bytes = @max(budget.backend_limit_bytes, floor.backend_limit_bytes);
        }
        budget.host_limit_bytes = @max(budget.host_limit_bytes, shared_cache_floor.host_limit_bytes);
        budget.backend_limit_bytes = @max(budget.backend_limit_bytes, shared_cache_floor.backend_limit_bytes);
        break :blk runtime.tier.cache.SharedCache.init(budget);
    } else null;
    errdefer {
        if (!backend_resources_transferred) {
            if (residency) |value| {
                var v = value;
                v.deinit();
            }
        }
    }
    if (comptime build_options.enable_metal) {
        // CUDA/native sessions do not own a Metal provider, so avoid a GGUF
        // catalog scan that cannot affect their dispatch.
        if (backend_type == .metal) {
            includeMetalJitLinearWeightFormats(
                &metal_jit_scope,
                &lazy_weights,
                tensor_store,
                direct_quant_enabled,
                arch_config,
                metalJitUsesExactProfileScope(kernel_jit_config),
            );
        }
    }

    const impl = try allocator.create(ArchSession);
    impl.* = .{
        .allocator = allocator,
        .arch_config = arch_config,
        .task = sessionTaskForModelType(mf.model_type, override),
        .backend_type = backend_type,
        .kernel_jit_config = kernel_jit_config,
        // Startup authority is a constructor-local capability. Never retain
        // it in a session that may be published and reused post-startup.
        .kernel_jit_load_context = .dynamic,
        .metal_jit_scope = metal_jit_scope,
        .budget_floor = budget_floor,
        .shared_cache_budget_floor = shared_cache_floor,
        .backend_data = makeGpuHostedBackendData(backend_type, .{
            .allocator = allocator,
            .resident_weight_estimate_bytes = resident_weight_bytes_override,
            .prefix = resident_prefix,
            .lazy_weights = lazy_weights,
            .tensor_store = tensor_store,
            .moe_num_experts = @intCast(moe_num_experts),
            .residency = residency,
            .tier_cache = tier_cache,
            .allow_direct_quant = direct_quant_enabled,
            .quant_execution_mode = quant_mode,
            .prefer_f32_dense_tensors = prefer_f32_dense_tensors,
            .jina_lora_adapter = gpu_jina_lora_adapter,
        }),
    };
    backend_resources_transferred = true;
    gpu_jina_lora_adapter = null;
    errdefer archClose(impl);
    // Build and qualify the model-scoped provider before publishing the
    // session. Required mode therefore fails model loading, and subsequent
    // compute wrappers reuse the already-initialized shared provider.
    if (comptime build_options.enable_metal) {
        if (backend_type == .metal and kernel_jit_config.mode.compiles()) {
            var prepared = try MetalCompute.initWithKernelJitScopeAndLoadContext(
                allocator,
                gpuBackendData(impl),
                null,
                kernel_jit_config,
                metal_jit_scope,
                kernel_jit_load_context,
            );
            prepared.deinit();
        }
    }
    try initGpuHostedPrefetch(impl);
    return .{ .ptr = impl, .vtable = &arch_vtable };
}

/// Detect the model architecture from config.json.
fn detectArchitecture(allocator: std.mem.Allocator, model_path: []const u8, mf: manifest_mod.ModelManifest) !ArchConfig {
    return detectArchitectureWithGgufFile(allocator, model_path, mf, null);
}

fn detectArchitectureWithGgufFile(
    allocator: std.mem.Allocator,
    model_path: []const u8,
    mf: manifest_mod.ModelManifest,
    parsed_gguf: ?*const gguf_mod.format.File,
) !ArchConfig {
    // Try to read config.json for model_type
    const config_path = try std.fmt.allocPrint(allocator, "{s}/config.json", .{model_path});
    defer allocator.free(config_path);

    if (c_file.readFile(allocator, config_path)) |config_bytes| {
        defer allocator.free(config_bytes);

        if (try detectModelType(allocator, config_bytes)) |model_type| {
            defer allocator.free(model_type);
            if (mf.gliner_model_type.len > 0) {
                // Split GLiNER bundles keep the DeBERTa encoder config in
                // config.json and use antfly_inference_bundle/gliner_config sidecars
                // to identify the GLiNER wrapper.
                var cfg = try deberta_mod.parseConfig(allocator, config_bytes);
                try applyGlinerLabelTokenIds(allocator, model_path, mf, &cfg);
                return .{ .gliner = cfg };
            }
            if (std.mem.eql(u8, model_type, "extractor")) {
                // GLiNER2: DeBERTa encoder + span classification head
                var cfg = deberta_mod.Config{};

                try applyGlinerLabelTokenIds(allocator, model_path, mf, &cfg);

                return .{ .gliner = cfg };
            }
            if (deberta_mod.isDebertaModel(model_type)) {
                return .{ .deberta = try deberta_mod.parseConfig(allocator, config_bytes) };
            }
            if (t5_mod.isT5Model(model_type)) {
                return .{ .t5 = try t5_mod.parseConfig(allocator, config_bytes) };
            }
            if (gpt_mod.isGenerativeModel(model_type) or
                std.mem.eql(u8, model_type, "colqwen2") or
                std.mem.eql(u8, model_type, "jina_embeddings_v5"))
            {
                var cfg = try gpt_mod.parseConfig(allocator, config_bytes);
                if (mf.usesGgufWeights()) {
                    const gguf_path = mf.gguf_path.?;
                    if (try detectArchitectureFromOptionalGgufFile(allocator, gguf_path, parsed_gguf)) |gguf_config| {
                        switch (gguf_config) {
                            .gpt => |gguf_cfg| overlayGptStructuralConfig(&cfg, gguf_cfg),
                            else => {},
                        }
                    }
                }
                return .{ .gpt = cfg };
            }
            if (whisper_mod.isWhisperModel(model_type)) {
                return .{ .whisper = try whisper_mod.parseConfig(allocator, config_bytes) };
            }
            if (florence_mod.isFlorenceModel(model_type)) {
                return .{ .florence = try florence_mod.parseConfig(allocator, config_bytes) };
            }
            if (clip_mod.isClipModel(model_type)) {
                return .{ .clip = try clip_mod.parseConfig(allocator, config_bytes) };
            }
            if (clap_mod.isClapModel(model_type)) {
                return .{ .clap = try clap_mod.parseConfig(allocator, config_bytes) };
            }
            if (std.mem.eql(u8, model_type, "layoutlmv3")) {
                return .{ .layoutlmv3 = try layoutlmv3_mod.parseConfig(allocator, config_bytes) };
            }
        }
    } else |_| {}

    if (mf.usesGgufWeights()) {
        const gguf_path = mf.gguf_path.?;
        if (try detectArchitectureFromOptionalGgufFile(allocator, gguf_path, parsed_gguf)) |gguf_config| {
            return gguf_config;
        }
    }

    // Default: BERT
    return .{ .bert = makeBertConfig(mf) };
}

fn detectArchitectureFromOptionalGgufFile(
    allocator: std.mem.Allocator,
    gguf_path: []const u8,
    parsed_gguf: ?*const gguf_mod.format.File,
) !?ArchConfig {
    if (parsed_gguf) |file| return detectArchitectureFromGgufFile(file);
    return detectArchitectureFromGguf(allocator, gguf_path);
}

test "architecture detection ignores an unselected colocated GGUF" {
    const allocator = std.testing.allocator;
    var manifest = manifest_mod.ModelManifest{
        .allocator = allocator,
        .safetensors_path = try allocator.dupe(u8, "model.safetensors"),
        // If architecture detection attempts to inspect this optional export,
        // the deliberately missing path makes the regression fail immediately.
        .gguf_path = try allocator.dupe(u8, "missing-export.gguf"),
    };
    defer manifest.deinit();

    const detected = try detectArchitectureWithGgufFile(
        allocator,
        "missing-model-directory",
        manifest,
        null,
    );
    try std.testing.expect(detected == .bert);
}

fn applyGlinerLabelTokenIds(allocator: std.mem.Allocator, model_path: []const u8, mf: manifest_mod.ModelManifest, cfg: *deberta_mod.Config) !void {
    if (mf.gliner_token_c != 0) cfg.classification_token_id = mf.gliner_token_c;
    if (mf.gliner_token_e != 0) cfg.entity_token_id = mf.gliner_token_e;
    if (mf.gliner_token_r != 0) cfg.relation_token_id = mf.gliner_token_r;

    const at_path = try std.fmt.allocPrint(allocator, "{s}/added_tokens.json", .{model_path});
    defer allocator.free(at_path);
    if (c_file.readFile(allocator, at_path)) |at_bytes| {
        defer allocator.free(at_bytes);
        const at_parsed = try std.json.parseFromSlice(std.json.Value, allocator, at_bytes, .{});
        defer at_parsed.deinit();
        if (at_parsed.value.object.get("[C]")) |v| {
            if (v == .integer) cfg.classification_token_id = v.integer;
        }
        if (at_parsed.value.object.get("[E]")) |v| {
            if (v == .integer) cfg.entity_token_id = v.integer;
        }
        if (at_parsed.value.object.get("[R]")) |v| {
            if (v == .integer) cfg.relation_token_id = v.integer;
        }
    } else |_| {}
}

fn detectArchitectureFromGguf(allocator: std.mem.Allocator, gguf_path: []const u8) !?ArchConfig {
    const store = try tensor_store_mod.GgufStore.initAbsolute(allocator, gguf_path);
    defer store.tensorStore().deinit();

    const file = store.tensorStore().ggufFile() orelse return null;
    const detected = (try detectArchitectureFromGgufFile(file)) orelse return null;
    return switch (detected) {
        .gpt => |cfg| blk: {
            var refined = cfg;
            if (store.mmap_region) |region| {
                refineRopeDimFromFreqs(&refined, file, region.data);
            }
            break :blk .{ .gpt = refined };
        },
        else => detected,
    };
}

fn detectArchitectureFromGgufFile(file: *const gguf_mod.format.File) !?ArchConfig {
    const meta = gguf_mod.metadata.View.init(file);
    if (gpt_mod.parseGgufMetadata(meta)) |cfg| {
        var refined = cfg;
        refineGptConfigFromGgufFile(&refined, file);
        return .{ .gpt = refined };
    }
    if (bert.parseGgufMetadata(meta)) |cfg| {
        var refined = cfg;
        refineBertConfigFromGgufTensorInfo(&refined, file);
        return .{ .bert = refined };
    }
    if (t5_mod.parseGgufMetadata(meta)) |cfg| {
        return .{ .t5 = cfg };
    }
    if (whisper_mod.parseGgufMetadata(meta)) |cfg| {
        return .{ .whisper = cfg };
    }
    if (deberta_mod.parseGgufMetadata(meta)) |cfg| {
        return .{ .deberta = cfg };
    }
    if (layoutlmv3_mod.parseGgufMetadata(meta)) |cfg| {
        return .{ .layoutlmv3 = cfg };
    }
    if (florence_mod.parseGgufMetadata(meta)) |cfg| {
        return .{ .florence = cfg };
    }
    if (clip_mod.parseGgufMetadata(meta)) |cfg| {
        return .{ .clip = cfg };
    }
    if (clap_mod.parseGgufMetadata(meta)) |cfg| {
        return .{ .clap = cfg };
    }
    return null;
}

/// Detect effective RoPE dimension from rope_freqs.weight tensor.
/// Gemma 4 uses rope.dimension_count for the frequency domain and may also
/// provide custom frequency factors where a leading prefix is 1.0 and the
/// remaining lanes are ~1e30 (effectively disabling rotation). llama.cpp
/// consumes both contracts. Count that exact mask-shaped prefix to derive the
/// active rotary width while preserving the metadata-derived frequency width.
fn refineRopeDimFromFreqs(config: *gpt_mod.Config, file: *const gguf_mod.format.File, raw_data: []const u8) void {
    const tensor = findGgufTensor(file, "rope_freqs.weight") orelse return;
    if (tensor.dimensions.len < 1) return;
    switch (tensor.tensor_type) {
        .known => |k| if (k != .F32) return,
        .bitnet_tl2 => return,
        .unknown => return,
    }

    const n_vals = std.math.cast(usize, tensor.dimensions[0]) orelse return;
    const byte_offset_u64 = std.math.add(u64, file.data_region_offset, tensor.offset) catch return;
    const byte_offset = std.math.cast(usize, byte_offset_u64) orelse return;
    const byte_len = std.math.mul(usize, n_vals, @sizeOf(f32)) catch return;
    const end = std.math.add(usize, byte_offset, byte_len) catch return;
    if (end > raw_data.len) return;

    const factors = raw_data[byte_offset..end];
    var n_active: u32 = 0;
    for (0..n_vals) |i| {
        const bits = std.mem.readInt(u32, factors[i * @sizeOf(f32) ..][0..4], .little);
        const factor: f32 = @bitCast(bits);
        if (factor == 1.0) {
            n_active += 1;
        } else break;
    }
    if (n_active == 0 or n_active >= n_vals) return;
    // This runtime representation models Gemma's binary active/disabled mask,
    // not arbitrary per-lane scaling. Fail closed for any other factor table.
    for (n_active..n_vals) |i| {
        const bits = std.mem.readInt(u32, factors[i * @sizeOf(f32) ..][0..4], .little);
        const factor: f32 = @bitCast(bits);
        if (!std.math.isFinite(factor) or factor < 1.0e20) return;
    }
    config.rope_dim_override = n_active * 2; // each factor covers a pair of dimensions
    std.log.info("rope_freqs.weight: {d}/{d} active entries -> rope_dim_override={d}", .{ n_active, n_vals, config.rope_dim_override });
}

test "Gemma4 GGUF rope factors refine active lanes despite explicit frequency dimension" {
    var dimensions = [_]u64{256};
    var metadata = [_]gguf_mod.format.MetadataEntry{
        .{ .key = "general.architecture", .value = .{ .string = "gemma4" } },
        .{ .key = "gemma4.rope.dimension_count", .value = .{ .u32 = 512 } },
        .{ .key = "gemma4.rope.dimension_count_swa", .value = .{ .u32 = 256 } },
    };
    var tensors = [_]gguf_mod.format.TensorInfo{.{
        .name = "rope_freqs.weight",
        .dimensions = &dimensions,
        .tensor_type = .{ .known = .F32 },
        .offset = 0,
        .data_offset = 0,
    }};
    const file = gguf_mod.format.File{
        .header = .{ .version = 3, .tensor_count = 1, .metadata_count = metadata.len },
        .metadata = &metadata,
        .tensors = &tensors,
        .alignment = 32,
        .data_region_offset = 0,
    };
    var factors: [256]f32 = undefined;
    @memset(factors[0..64], 1.0);
    @memset(factors[64..], 1.0e30);

    var config = gpt_mod.Config{
        .family = .gemma,
        .attention_head_dim = 256,
        .global_head_dim = 512,
        .sliding_window = 512,
        .sliding_window_pattern = 5,
        .rope_partial_factor = 1.0,
    };
    refineRopeDimFromFreqs(&config, &file, std.mem.sliceAsBytes(factors[0..]));

    try std.testing.expectEqual(@as(u32, 128), config.rope_dim_override);
    try std.testing.expectEqual(@as(u32, 256), config.layerRopeActiveDim(0));
    try std.testing.expectEqual(@as(u32, 128), config.layerRopeActiveDim(4));
    try std.testing.expectEqual(@as(u32, 512), config.layerRopeFrequencyDim(4));
}

test "GGUF rope refinement rejects non-mask frequency factors" {
    var dimensions = [_]u64{4};
    var tensors = [_]gguf_mod.format.TensorInfo{.{
        .name = "rope_freqs.weight",
        .dimensions = &dimensions,
        .tensor_type = .{ .known = .F32 },
        .offset = 0,
        .data_offset = 0,
    }};
    const file = gguf_mod.format.File{
        .header = .{ .version = 3, .tensor_count = 1, .metadata_count = 0 },
        .metadata = &.{},
        .tensors = &tensors,
        .alignment = 32,
        .data_region_offset = 0,
    };
    var factors = [_]f32{ 1.0, 1.0, 2.0, 2.0 };
    var config = gpt_mod.Config{ .family = .gemma, .rope_dim_override = 0 };
    refineRopeDimFromFreqs(&config, &file, std.mem.sliceAsBytes(factors[0..]));
    try std.testing.expectEqual(@as(u32, 0), config.rope_dim_override);
}

fn overlayGptStructuralConfig(target: *gpt_mod.Config, source: gpt_mod.Config) void {
    if (target.family == .other or source.family != .llama or target.family == .llama) {
        target.family = source.family;
    }
    target.hidden_size = source.hidden_size;
    target.num_hidden_layers = source.num_hidden_layers;
    target.num_attention_heads = source.num_attention_heads;
    target.num_key_value_heads = source.num_key_value_heads;
    target.attention_head_dim = source.attention_head_dim;
    target.intermediate_size = source.intermediate_size;
    if (source.vocab_size != 0) {
        target.vocab_size = source.vocab_size;
    }
    target.max_position_embeddings = source.max_position_embeddings;
    if (source.sliding_window != 0 or target.sliding_window == 0) {
        target.sliding_window = source.sliding_window;
    }
    target.num_local_experts = source.num_local_experts;
    target.num_experts_per_tok = source.num_experts_per_tok;
    if (target.family == .other or source.family != .llama or target.family == .llama) {
        target.norm_type = source.norm_type;
        target.position_encoding = source.position_encoding;
        target.activation = source.activation;
        target.norm_eps = source.norm_eps;
        target.weight_prefix = source.weight_prefix;
    }
    target.norm_weight_offset = source.norm_weight_offset;
    if (source.rope_theta != 10000.0 or target.rope_theta == 10000.0) {
        target.rope_theta = source.rope_theta;
    }
    // Gemma 4: overlay per-layer GQA and shared KV fields from GGUF.
    if (source.num_kv_shared_layers > 0) target.num_kv_shared_layers = source.num_kv_shared_layers;
    if (source.global_head_dim > 0) target.global_head_dim = source.global_head_dim;
    if (source.num_global_key_value_heads > 0) target.num_global_key_value_heads = source.num_global_key_value_heads;
    // A scalar GGUF feed_forward_length means every layer uses the same size.
    // Keep zero authoritative here: HF Gemma 4 config.json derives a doubled
    // shared-tail size, but some valid GGUF conversions store uniform 10240-wide
    // FFNs. Retaining the sidecar's 20480 makes quant kernels read past the
    // 10240-wide weight and can crash the Metal backend.
    target.shared_layer_intermediate_size = source.shared_layer_intermediate_size;
    if (source.sliding_window_pattern != 6) target.sliding_window_pattern = source.sliding_window_pattern;
    if (source.rope_local_theta != 10000.0 or target.rope_local_theta == 10000.0) {
        target.rope_local_theta = source.rope_local_theta;
    }
    // Gemma 4: Per-Layer Embeddings (PLE).
    if (source.ple_hidden_size > 0) target.ple_hidden_size = source.ple_hidden_size;
    // Active RoPE lanes refined from a mask-shaped rope_freqs.weight tensor.
    if (source.rope_dim_override > 0) target.rope_dim_override = source.rope_dim_override;

    // Fields below describe the GGUF artifact itself rather than a preference, so the
    // GGUF-derived value wins over config.json. `source` is always a fully GGUF-derived
    // config here; the alternative is silently running weights under a description that
    // does not match them.
    //
    // weight_tying is set by observing whether the file carries a separate output.weight.
    // Gemma 3 GGUFs have no output.weight, and their config.json omits tie_word_embeddings,
    // so without this the runtime looks for a lm_head.weight that does not exist.
    target.weight_tying = source.weight_tying;
    // rope_partial_factor is derived from rope.dimension_count against the head dim, which
    // is how llama.cpp reads the same file. unsloth's Gemma 4 config.json claims 0.25 while
    // its GGUF declares full-width rotation, and honoring config.json produced empty output.
    target.rope_partial_factor = source.rope_partial_factor;
    // Softcapping is often absent from GGUF metadata, so only a positive value is authoritative.
    if (source.final_logit_softcapping != 0.0) target.final_logit_softcapping = source.final_logit_softcapping;
}

pub fn refineGptConfigFromGgufTensorInfo(config: *gpt_mod.Config, file: *const gguf_mod.format.File) void {
    refineGemma4AttentionKEqualVFromGgufTensors(config, file);
    if (findGgufTensor(file, "token_embd.weight")) |tensor| {
        if (tensor.dimensions.len >= 2) {
            config.vocab_size = @intCast(tensor.dimensions[tensor.dimensions.len - 1]);
            config.hidden_size = @intCast(tensor.dimensions[tensor.dimensions.len - 2]);
        }
        // No separate output.weight → lm_head reuses embedding weights.
        if (findGgufTensor(file, "output.weight") == null) {
            config.weight_tying = true;
        }
        return;
    }
    if (findGgufTensor(file, "tok_embeddings.weight")) |tensor| {
        if (tensor.dimensions.len >= 2) {
            config.vocab_size = @intCast(tensor.dimensions[tensor.dimensions.len - 1]);
            config.hidden_size = @intCast(tensor.dimensions[tensor.dimensions.len - 2]);
        }
        if (findGgufTensor(file, "output.weight") == null) {
            config.weight_tying = true;
        }
        return;
    }
    if (findGgufTensor(file, "output.weight")) |tensor| {
        if (tensor.dimensions.len >= 2) {
            config.vocab_size = @intCast(tensor.dimensions[tensor.dimensions.len - 1]);
            config.hidden_size = @intCast(tensor.dimensions[tensor.dimensions.len - 2]);
        }
    }
}

fn refineGptConfigFromGgufFile(config: *gpt_mod.Config, file: *const gguf_mod.format.File) void {
    refineGptConfigFromGgufTensorInfo(config, file);
}

/// BERT-family GGUF conversions (e.g. bge-m3) may omit `bert.vocab_size`;
/// the embedding tensor's row count is authoritative.
fn refineBertConfigFromGgufTensorInfo(config: *bert.Config, file: *const gguf_mod.format.File) void {
    if (findGgufTensor(file, "token_embd.weight")) |tensor| {
        if (tensor.dimensions.len >= 2) {
            config.vocab_size = @intCast(tensor.dimensions[tensor.dimensions.len - 1]);
        }
    }
}

fn findGgufTensor(file: *const gguf_mod.format.File, name: []const u8) ?*const gguf_mod.format.TensorInfo {
    for (file.tensors) |*tensor| {
        if (std.mem.eql(u8, tensor.name, name)) return tensor;
    }
    return null;
}

test "gemma4 gguf explicit shared-tail k weights do not disable shared kv" {
    var emb_dims = [_]u64{ 1536, 262144 };
    var k_dims = [_]u64{ 1536, 256 };
    var tensors = [_]gguf_mod.format.TensorInfo{
        .{
            .name = "token_embd.weight",
            .dimensions = &emb_dims,
            .tensor_type = .{ .known = .F16 },
            .offset = 0,
            .data_offset = 64,
        },
        .{
            .name = "blk.15.attn_k.weight",
            .dimensions = &k_dims,
            .tensor_type = .{ .known = .F16 },
            .offset = 0,
            .data_offset = 64,
        },
    };
    const file = gguf_mod.format.File{
        .header = .{ .version = 3, .tensor_count = tensors.len, .metadata_count = 0 },
        .metadata = &.{},
        .tensors = &tensors,
        .alignment = 32,
        .data_region_offset = 64,
    };

    var config = gpt_mod.Config{
        .family = .gemma,
        .num_hidden_layers = 35,
        .num_attention_heads = 8,
        .num_key_value_heads = 1,
        .attention_head_dim = 256,
        .global_head_dim = 512,
        .sliding_window = 512,
        .sliding_window_pattern = 5,
        .num_kv_shared_layers = 20,
        .shared_layer_intermediate_size = 12288,
    };

    refineGptConfigFromGgufFile(&config, &file);

    try std.testing.expectEqual(@as(u32, 20), config.num_kv_shared_layers);
    try std.testing.expect(config.layerUsesSharedTail(15));
    try std.testing.expect(config.layerSharesKv(15));
    try std.testing.expectEqual(@as(?usize, 13), config.kvDonorLayerIndex(15));
    try std.testing.expectEqual(@as(u32, 12288), config.intermediateSize(15));
}

fn buildGgufInspectionReport(
    allocator: std.mem.Allocator,
    arch_config: ArchConfig,
    store: tensor_store_mod.TensorStore,
) !?GgufInspectionReport {
    if (store.kind() != .gguf) return null;
    const file = store.ggufFile() orelse return null;
    return @as(?GgufInspectionReport, try buildGgufInspectionReportFromFile(allocator, arch_config, file));
}

fn buildGgufInspectionReportFromFile(
    allocator: std.mem.Allocator,
    arch_config: ArchConfig,
    file: *const gguf_mod.format.File,
) !GgufInspectionReport {
    const meta = gguf_mod.metadata.View.init(file);
    const architecture = try allocator.dupe(u8, meta.getString("general.architecture") orelse "unknown");

    var report = GgufInspectionReport{
        .allocator = allocator,
        .architecture = architecture,
        .tensor_count = file.tensors.len,
        .metadata_count = std.math.cast(usize, file.header.metadata_count) orelse file.metadata.len,
        .gpt_config = switch (arch_config) {
            .gpt => |cfg| cfg,
            else => null,
        },
    };
    errdefer report.deinit();

    var all_tensor_types = std.ArrayListUnmanaged(UnsupportedTensorTypeCount).empty;
    defer all_tensor_types.deinit(allocator);
    var unsupported = std.ArrayListUnmanaged(UnsupportedTensorTypeCount).empty;
    defer unsupported.deinit(allocator);
    try collectTensorTypes(allocator, file, &all_tensor_types, &unsupported);
    var quantized_samples = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (quantized_samples.items) |name| allocator.free(name);
        quantized_samples.deinit(allocator);
    }
    try collectQuantizedTensorSamples(allocator, file, &quantized_samples);
    var dense_samples = std.ArrayListUnmanaged(GgufTensorSample).empty;
    defer {
        for (dense_samples.items) |sample| allocator.free(sample.name);
        dense_samples.deinit(allocator);
    }
    try collectLargestDenseTensorSamples(allocator, file, &dense_samples);

    var missing = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (missing.items) |name| allocator.free(name);
        missing.deinit(allocator);
    }
    var unmapped = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (unmapped.items) |name| allocator.free(name);
        unmapped.deinit(allocator);
    }
    var packed_moe = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (packed_moe.items) |name| allocator.free(name);
        packed_moe.deinit(allocator);
    }

    var normalized_names = std.StringHashMapUnmanaged(void){};
    defer {
        var it = normalized_names.iterator();
        while (it.next()) |entry| allocator.free(entry.key_ptr.*);
        normalized_names.deinit(allocator);
    }
    try collectNormalizedGgufNames(allocator, arch_config, file, &normalized_names, &unmapped, &packed_moe);

    switch (arch_config) {
        .gpt => |cfg| try collectMissingRequiredGptWeights(allocator, cfg, &normalized_names, &missing, packed_moe.items.len > 0),
        .deberta => |cfg| try collectMissingRequiredDebertaWeights(allocator, cfg, &normalized_names, &missing),
        else => {},
    }

    report.all_tensor_types = try all_tensor_types.toOwnedSlice(allocator);
    report.unsupported_tensor_types = try unsupported.toOwnedSlice(allocator);
    report.quantized_tensor_samples = try quantized_samples.toOwnedSlice(allocator);
    report.dense_tensor_samples = try dense_samples.toOwnedSlice(allocator);
    report.missing_required_tensors = try missing.toOwnedSlice(allocator);
    report.unmapped_tensor_names = try unmapped.toOwnedSlice(allocator);
    report.packed_moe_expert_tensors = try packed_moe.toOwnedSlice(allocator);
    return report;
}

fn collectLargestDenseTensorSamples(
    allocator: std.mem.Allocator,
    file: *const gguf_mod.format.File,
    out: *std.ArrayListUnmanaged(GgufTensorSample),
) !void {
    const Candidate = struct {
        name: []const u8,
        tensor_type: gguf_mod.tensor_types.TensorType,
        byte_len: u64,
    };

    var candidates = std.ArrayListUnmanaged(Candidate).empty;
    defer candidates.deinit(allocator);
    for (file.tensors) |tensor| {
        if (tensor.tensor_type.isQuantized()) continue;
        if (tensor.dimensions.len < 2) continue;
        const byte_len = gguf_mod.tensor_types.byteLen(tensor.tensor_type, tensor.dimensions) orelse continue;
        try candidates.append(allocator, .{
            .name = tensor.name,
            .tensor_type = tensor.tensor_type,
            .byte_len = byte_len,
        });
    }

    std.mem.sort(Candidate, candidates.items, {}, struct {
        fn lessThan(_: void, lhs: Candidate, rhs: Candidate) bool {
            return lhs.byte_len > rhs.byte_len;
        }
    }.lessThan);

    const limit = @min(candidates.items.len, 16);
    for (candidates.items[0..limit]) |candidate| {
        try out.append(allocator, .{
            .name = try allocator.dupe(u8, candidate.name),
            .tensor_type = candidate.tensor_type,
            .byte_len = candidate.byte_len,
        });
    }
}

fn collectQuantizedTensorSamples(
    allocator: std.mem.Allocator,
    file: *const gguf_mod.format.File,
    out: *std.ArrayListUnmanaged([]const u8),
) !void {
    for (file.tensors) |tensor| {
        if (!tensor.tensor_type.isQuantized()) continue;
        if (out.items.len >= 16) break;
        try out.append(allocator, try allocator.dupe(u8, tensor.name));
    }
}

fn collectTensorTypes(
    allocator: std.mem.Allocator,
    file: *const gguf_mod.format.File,
    all_out: *std.ArrayListUnmanaged(UnsupportedTensorTypeCount),
    out: *std.ArrayListUnmanaged(UnsupportedTensorTypeCount),
) !void {
    for (file.tensors) |tensor| {
        var found_all = false;
        for (all_out.items) |*entry| {
            if (entry.tensor_type.raw() != tensor.tensor_type.raw()) continue;
            entry.count += 1;
            found_all = true;
            break;
        }
        if (!found_all) {
            try all_out.append(allocator, .{
                .tensor_type = tensor.tensor_type,
                .count = 1,
            });
        }

        if (tensorTypeSupported(tensor.tensor_type)) continue;
        var found = false;
        for (out.items) |*entry| {
            if (entry.tensor_type.raw() != tensor.tensor_type.raw()) continue;
            entry.count += 1;
            found = true;
            break;
        }
        if (!found) {
            try out.append(allocator, .{
                .tensor_type = tensor.tensor_type,
                .count = 1,
            });
        }
    }
}

fn collectNormalizedGgufNames(
    allocator: std.mem.Allocator,
    arch_config: ArchConfig,
    file: *const gguf_mod.format.File,
    names: *std.StringHashMapUnmanaged(void),
    unmapped: *std.ArrayListUnmanaged([]const u8),
    packed_moe: *std.ArrayListUnmanaged([]const u8),
) !void {
    for (file.tensors) |tensor| {
        var key_buf: [256]u8 = undefined;
        const normalized = try normalizeWeightKey(.gguf, arch_config, tensor.name, &key_buf);
        try putNameIfAbsent(allocator, names, normalized);
        const packed_moe_expert_tensor = isPackedMoeExpertTensor(tensor.name);
        if (parsePackedMoeTensor(tensor.name)) |packed_tensor| {
            const deepseek_v4 = switch (arch_config) {
                .gpt => |cfg| cfg.family == .deepseek_v4,
                else => false,
            };
            if (deepseek_v4) {
                if (packed_tensor.fused_gate_up) {
                    const packed_name = std.fmt.bufPrint(&key_buf, "model.layers.{d}.mlp.experts.gate_up_proj", .{packed_tensor.layer}) catch return error.NameTooLong;
                    try putNameIfAbsent(allocator, names, packed_name);
                } else {
                    const projs: []const []const u8 = if (packed_tensor.proj2) |p2|
                        &.{ packed_tensor.proj, p2 }
                    else
                        &.{packed_tensor.proj};
                    for (projs) |proj| {
                        const packed_name = std.fmt.bufPrint(&key_buf, "model.layers.{d}.mlp.experts.{s}", .{ packed_tensor.layer, deepseek_v4_arch.moeProjectionName(proj) orelse continue }) catch return error.NameTooLong;
                        try putNameIfAbsent(allocator, names, packed_name);
                    }
                }
            } else {
                const projs: []const []const u8 = if (packed_tensor.proj2) |p2|
                    &.{ packed_tensor.proj, p2 }
                else
                    &.{packed_tensor.proj};
                for (projs) |proj| {
                    const packed_name = std.fmt.bufPrint(&key_buf, "model.layers.{d}.block_sparse_moe.packed.{s}.weight", .{ packed_tensor.layer, proj }) catch return error.NameTooLong;
                    try putNameIfAbsent(allocator, names, packed_name);
                }
            }
        }
        if (packed_moe_expert_tensor and packed_moe.items.len < 16) {
            try packed_moe.append(allocator, try allocator.dupe(u8, tensor.name));
        }
        if (!packed_moe_expert_tensor and shouldRecordUnmappedGgufTensor(arch_config, tensor.name, normalized, unmapped.items.len)) {
            try unmapped.append(allocator, try allocator.dupe(u8, tensor.name));
        }
    }
}

fn putNameIfAbsent(allocator: std.mem.Allocator, names: *std.StringHashMapUnmanaged(void), name: []const u8) !void {
    if (names.contains(name)) return;
    try names.put(allocator, try allocator.dupe(u8, name), {});
}

fn shouldRecordUnmappedGgufTensor(arch_config: ArchConfig, raw_name: []const u8, normalized_name: []const u8, current_count: usize) bool {
    if (current_count >= 16) return false;
    if (!std.mem.eql(u8, raw_name, normalized_name)) return false;
    return switch (arch_config) {
        .gpt => |cfg| switch (cfg.family) {
            .llama, .mistral, .qwen2, .gemma, .bitnet, .phi, .deepseek_v4 => std.mem.startsWith(u8, raw_name, "blk."),
            else => false,
        },
        else => false,
    };
}

fn collectMissingRequiredGptWeights(
    allocator: std.mem.Allocator,
    config: gpt_mod.Config,
    names: *const std.StringHashMapUnmanaged(void),
    missing: *std.ArrayListUnmanaged([]const u8),
    packed_moe_layout_detected: bool,
) !void {
    if (config.family == .gpt2) {
        try appendMissingWeight(allocator, names, missing, "wte.weight");
        try appendMissingWeight(allocator, names, missing, "ln_f.weight");
        try appendMissingWeight(allocator, names, missing, "ln_f.bias");
        if (config.position_encoding == .absolute) {
            try appendMissingWeight(allocator, names, missing, "wpe.weight");
        }
        if (!config.weight_tying) {
            try appendMissingWeight(allocator, names, missing, "lm_head.weight");
        }

        var gpt2_buf: [256]u8 = undefined;
        for (0..config.num_hidden_layers) |layer| {
            try appendMissingFmt(allocator, names, missing, &gpt2_buf, "h.{d}.ln_1.weight", .{layer});
            try appendMissingFmt(allocator, names, missing, &gpt2_buf, "h.{d}.ln_1.bias", .{layer});
            try appendMissingFmt(allocator, names, missing, &gpt2_buf, "h.{d}.attn.c_attn.weight", .{layer});
            try appendMissingFmt(allocator, names, missing, &gpt2_buf, "h.{d}.attn.c_attn.bias", .{layer});
            try appendMissingFmt(allocator, names, missing, &gpt2_buf, "h.{d}.attn.c_proj.weight", .{layer});
            try appendMissingFmt(allocator, names, missing, &gpt2_buf, "h.{d}.attn.c_proj.bias", .{layer});
            try appendMissingFmt(allocator, names, missing, &gpt2_buf, "h.{d}.ln_2.weight", .{layer});
            try appendMissingFmt(allocator, names, missing, &gpt2_buf, "h.{d}.ln_2.bias", .{layer});
            try appendMissingFmt(allocator, names, missing, &gpt2_buf, "h.{d}.mlp.c_fc.weight", .{layer});
            try appendMissingFmt(allocator, names, missing, &gpt2_buf, "h.{d}.mlp.c_fc.bias", .{layer});
            try appendMissingFmt(allocator, names, missing, &gpt2_buf, "h.{d}.mlp.c_proj.weight", .{layer});
            try appendMissingFmt(allocator, names, missing, &gpt2_buf, "h.{d}.mlp.c_proj.bias", .{layer});
        }
        return;
    }

    if (config.family == .gpt_neo) {
        try appendMissingWeight(allocator, names, missing, "wte.weight");
        try appendMissingWeight(allocator, names, missing, "ln_f.weight");
        try appendMissingWeight(allocator, names, missing, "ln_f.bias");
        if (config.position_encoding == .absolute) {
            try appendMissingWeight(allocator, names, missing, "wpe.weight");
        }
        if (!config.weight_tying) {
            try appendMissingWeight(allocator, names, missing, "lm_head.weight");
        }

        var gpt_neo_buf: [256]u8 = undefined;
        for (0..config.num_hidden_layers) |layer| {
            try appendMissingFmt(allocator, names, missing, &gpt_neo_buf, "h.{d}.ln_1.weight", .{layer});
            try appendMissingFmt(allocator, names, missing, &gpt_neo_buf, "h.{d}.ln_1.bias", .{layer});
            try appendMissingFmt(allocator, names, missing, &gpt_neo_buf, "h.{d}.attn.attention.q_proj.weight", .{layer});
            try appendMissingFmt(allocator, names, missing, &gpt_neo_buf, "h.{d}.attn.attention.k_proj.weight", .{layer});
            try appendMissingFmt(allocator, names, missing, &gpt_neo_buf, "h.{d}.attn.attention.v_proj.weight", .{layer});
            try appendMissingFmt(allocator, names, missing, &gpt_neo_buf, "h.{d}.attn.attention.out_proj.weight", .{layer});
            try appendMissingFmt(allocator, names, missing, &gpt_neo_buf, "h.{d}.ln_2.weight", .{layer});
            try appendMissingFmt(allocator, names, missing, &gpt_neo_buf, "h.{d}.ln_2.bias", .{layer});
            try appendMissingFmt(allocator, names, missing, &gpt_neo_buf, "h.{d}.mlp.c_fc.weight", .{layer});
            try appendMissingFmt(allocator, names, missing, &gpt_neo_buf, "h.{d}.mlp.c_fc.bias", .{layer});
            try appendMissingFmt(allocator, names, missing, &gpt_neo_buf, "h.{d}.mlp.c_proj.weight", .{layer});
            try appendMissingFmt(allocator, names, missing, &gpt_neo_buf, "h.{d}.mlp.c_proj.bias", .{layer});
        }
        return;
    }

    if (config.family == .deepseek_v4) {
        try deepseek_v4_arch.appendMissingRequiredWeights(allocator, config, names, missing);
        return;
    }

    try appendMissingWeight(allocator, names, missing, "model.embed_tokens.weight");
    try appendMissingWeight(allocator, names, missing, "model.norm.weight");
    if (config.family == .phi or config.family == .gptj or config.family == .gpt_neox) {
        try appendMissingWeight(allocator, names, missing, "model.norm.bias");
    }
    if (config.position_encoding == .absolute) {
        try appendMissingWeight(allocator, names, missing, "wpe.weight");
    }

    var buf: [256]u8 = undefined;
    for (0..config.num_hidden_layers) |layer| {
        try appendMissingFmt(allocator, names, missing, &buf, "model.layers.{d}.input_layernorm.weight", .{layer});
        if (config.family == .phi or config.family == .gptj or config.family == .gpt_neox) {
            try appendMissingFmt(allocator, names, missing, &buf, "model.layers.{d}.input_layernorm.bias", .{layer});
        }
        try appendMissingFmt(allocator, names, missing, &buf, "model.layers.{d}.self_attn.q_proj.weight", .{layer});
        if (config.family == .qwen2 or config.family == .phi) {
            try appendMissingFmt(allocator, names, missing, &buf, "model.layers.{d}.self_attn.q_proj.bias", .{layer});
        }
        if (!config.layerSharesKv(layer)) {
            try appendMissingFmt(allocator, names, missing, &buf, "model.layers.{d}.self_attn.k_proj.weight", .{layer});
            if (config.family == .qwen2 or config.family == .phi) {
                try appendMissingFmt(allocator, names, missing, &buf, "model.layers.{d}.self_attn.k_proj.bias", .{layer});
            }
            if (!config.layerOmitsVProj(layer)) {
                try appendMissingFmt(allocator, names, missing, &buf, "model.layers.{d}.self_attn.v_proj.weight", .{layer});
                if (config.family == .qwen2 or config.family == .phi) {
                    try appendMissingFmt(allocator, names, missing, &buf, "model.layers.{d}.self_attn.v_proj.bias", .{layer});
                }
            }
        }
        try appendMissingFmt(allocator, names, missing, &buf, "model.layers.{d}.self_attn.o_proj.weight", .{layer});
        try appendMissingFmt(allocator, names, missing, &buf, "model.layers.{d}.post_attention_layernorm.weight", .{layer});
        if (config.family == .phi or config.family == .gptj or config.family == .gpt_neox) {
            try appendMissingFmt(allocator, names, missing, &buf, "model.layers.{d}.post_attention_layernorm.bias", .{layer});
        }

        if (config.usesMoe()) {
            try appendMissingFmt(allocator, names, missing, &buf, "model.layers.{d}.block_sparse_moe.gate.weight", .{layer});
            if (packed_moe_layout_detected) {
                try appendMissingFmt(allocator, names, missing, &buf, "model.layers.{d}.block_sparse_moe.packed.w1.weight", .{layer});
                try appendMissingFmt(allocator, names, missing, &buf, "model.layers.{d}.block_sparse_moe.packed.w2.weight", .{layer});
                try appendMissingFmt(allocator, names, missing, &buf, "model.layers.{d}.block_sparse_moe.packed.w3.weight", .{layer});
            } else {
                for (0..config.num_local_experts) |expert_index| {
                    try appendMissingFmt(allocator, names, missing, &buf, "model.layers.{d}.block_sparse_moe.experts.{d}.w1.weight", .{ layer, expert_index });
                    try appendMissingFmt(allocator, names, missing, &buf, "model.layers.{d}.block_sparse_moe.experts.{d}.w2.weight", .{ layer, expert_index });
                    try appendMissingFmt(allocator, names, missing, &buf, "model.layers.{d}.block_sparse_moe.experts.{d}.w3.weight", .{ layer, expert_index });
                }
            }
        } else {
            if (config.family == .phi or config.family == .gptj or config.family == .gpt_neox) {
                try appendMissingFmt(allocator, names, missing, &buf, "model.layers.{d}.mlp.fc1_proj.weight", .{layer});
                if (config.family == .gptj or config.family == .gpt_neox) {
                    try appendMissingFmt(allocator, names, missing, &buf, "model.layers.{d}.mlp.fc1_proj.bias", .{layer});
                }
                try appendMissingFmt(allocator, names, missing, &buf, "model.layers.{d}.mlp.fc2_proj.weight", .{layer});
                if (config.family == .gptj or config.family == .gpt_neox) {
                    try appendMissingFmt(allocator, names, missing, &buf, "model.layers.{d}.mlp.fc2_proj.bias", .{layer});
                }
            } else {
                try appendMissingFmt(allocator, names, missing, &buf, "model.layers.{d}.mlp.gate_proj.weight", .{layer});
                try appendMissingFmt(allocator, names, missing, &buf, "model.layers.{d}.mlp.up_proj.weight", .{layer});
            }
            if (config.family == .bitnet) {
                try appendMissingFmt(allocator, names, missing, &buf, "model.layers.{d}.mlp.ffn_sub_norm.weight", .{layer});
            }
            if (config.family != .phi and config.family != .gptj and config.family != .gpt_neox) {
                try appendMissingFmt(allocator, names, missing, &buf, "model.layers.{d}.mlp.down_proj.weight", .{layer});
            }
        }
        if (config.family == .bitnet) {
            try appendMissingFmt(allocator, names, missing, &buf, "model.layers.{d}.self_attn.attn_sub_norm.weight", .{layer});
        }
    }
}

fn collectMissingRequiredDebertaWeights(
    allocator: std.mem.Allocator,
    config: deberta_mod.Config,
    names: *const std.StringHashMapUnmanaged(void),
    missing: *std.ArrayListUnmanaged([]const u8),
) !void {
    try appendMissingWeight(allocator, names, missing, "embeddings.word_embeddings.weight");
    try appendMissingWeight(allocator, names, missing, "embeddings.LayerNorm.weight");
    try appendMissingWeight(allocator, names, missing, "embeddings.LayerNorm.bias");
    try appendMissingWeight(allocator, names, missing, "encoder.rel_embeddings.weight");
    try appendMissingWeight(allocator, names, missing, "encoder.LayerNorm.weight");
    try appendMissingWeight(allocator, names, missing, "encoder.LayerNorm.bias");

    var buf: [256]u8 = undefined;
    for (0..config.num_hidden_layers) |layer| {
        try appendMissingFmt(allocator, names, missing, &buf, "encoder.layer.{d}.attention.self.query_proj.weight", .{layer});
        try appendMissingFmt(allocator, names, missing, &buf, "encoder.layer.{d}.attention.self.query_proj.bias", .{layer});
        try appendMissingFmt(allocator, names, missing, &buf, "encoder.layer.{d}.attention.self.key_proj.weight", .{layer});
        try appendMissingFmt(allocator, names, missing, &buf, "encoder.layer.{d}.attention.self.key_proj.bias", .{layer});
        try appendMissingFmt(allocator, names, missing, &buf, "encoder.layer.{d}.attention.self.value_proj.weight", .{layer});
        try appendMissingFmt(allocator, names, missing, &buf, "encoder.layer.{d}.attention.self.value_proj.bias", .{layer});
        try appendMissingFmt(allocator, names, missing, &buf, "encoder.layer.{d}.attention.output.dense.weight", .{layer});
        try appendMissingFmt(allocator, names, missing, &buf, "encoder.layer.{d}.attention.output.dense.bias", .{layer});
        try appendMissingFmt(allocator, names, missing, &buf, "encoder.layer.{d}.attention.output.LayerNorm.weight", .{layer});
        try appendMissingFmt(allocator, names, missing, &buf, "encoder.layer.{d}.attention.output.LayerNorm.bias", .{layer});
        try appendMissingFmt(allocator, names, missing, &buf, "encoder.layer.{d}.intermediate.dense.weight", .{layer});
        try appendMissingFmt(allocator, names, missing, &buf, "encoder.layer.{d}.intermediate.dense.bias", .{layer});
        try appendMissingFmt(allocator, names, missing, &buf, "encoder.layer.{d}.output.dense.weight", .{layer});
        try appendMissingFmt(allocator, names, missing, &buf, "encoder.layer.{d}.output.dense.bias", .{layer});
        try appendMissingFmt(allocator, names, missing, &buf, "encoder.layer.{d}.output.LayerNorm.weight", .{layer});
        try appendMissingFmt(allocator, names, missing, &buf, "encoder.layer.{d}.output.LayerNorm.bias", .{layer});
    }
}

fn isPackedMoeExpertTensor(raw_name: []const u8) bool {
    return std.mem.endsWith(u8, raw_name, ".ffn_gate_exps.weight") or
        std.mem.endsWith(u8, raw_name, ".ffn_down_exps.weight") or
        std.mem.endsWith(u8, raw_name, ".ffn_up_exps.weight") or
        std.mem.endsWith(u8, raw_name, ".ffn_gate_up_exps.weight");
}

fn appendMissingFmt(
    allocator: std.mem.Allocator,
    names: *const std.StringHashMapUnmanaged(void),
    missing: *std.ArrayListUnmanaged([]const u8),
    buf: *[256]u8,
    comptime fmt: []const u8,
    args: anytype,
) !void {
    const name = std.fmt.bufPrint(buf, fmt, args) catch return error.NameTooLong;
    try appendMissingWeight(allocator, names, missing, name);
}

fn appendMissingWeight(
    allocator: std.mem.Allocator,
    names: *const std.StringHashMapUnmanaged(void),
    missing: *std.ArrayListUnmanaged([]const u8),
    name: []const u8,
) !void {
    if (names.contains(name)) return;
    try missing.append(allocator, try allocator.dupe(u8, name));
}

fn tensorTypeSupported(tensor_type: gguf_mod.tensor_types.TensorType) bool {
    return switch (tensor_type) {
        .known => |known| switch (known) {
            .F16,
            .F32,
            .BF16,
            .Q4_0,
            .Q4_1,
            .Q5_0,
            .Q5_1,
            .Q8_0,
            .Q8_1,
            .Q2_K,
            .Q3_K,
            .Q4_K,
            .Q5_K,
            .Q6_K,
            .Q8_K,
            .I2_S,
            .I8_S,
            .TL1,
            .IQ4_NL,
            .IQ4_XS,
            // Integer index tables rather than weights. DeepSeek V4 stores its hash-MoE
            // token-to-expert routing (ffn_gate_tid2eid) as I32, and quant_codec already
            // materializes it via materializePassthrough; only this gate was missing, so
            // inspection rejected the file before the runtime ever saw it.
            .I32,
            => true,
            else => false,
        },
        .bitnet_tl2 => true,
        .unknown => false,
    };
}

/// Whether a backend can consume a tensor encoding after the generic GGUF
/// loader has decoded or retained it. Companion-artifact preflight uses this
/// same gate as whole-model inspection so admission and runtime cannot diverge.
pub fn ggufTensorTypeSupportsBackend(
    tensor_type: gguf_mod.tensor_types.TensorType,
    backend: BackendType,
) bool {
    if (!tensorTypeSupported(tensor_type)) return false;
    return switch (backend) {
        .metal => if (comptime build_options.enable_metal)
            metal_runtime.isMetalNativeSupported(tensor_type)
        else
            true,
        .native, .cuda, .onnx, .wasm => true,
        .pjrt => false,
    };
}

fn ensureGgufInspectionCompatible(report: GgufInspectionReport, gguf_path: []const u8) !void {
    if (report.unsupported_tensor_types.len > 0) {
        std.log.err("GGUF {s} uses unsupported tensor types:", .{gguf_path});
        for (report.unsupported_tensor_types) |entry| {
            std.log.err("  {s}: {d}", .{ entry.tensor_type.name(), entry.count });
        }
        return error.UnsupportedGgufTensorType;
    }
    if (report.missing_required_tensors.len > 0) {
        std.log.err("GGUF {s} is missing required normalized tensors ({d}):", .{ gguf_path, report.missing_required_tensors.len });
        const limit = @min(report.missing_required_tensors.len, 24);
        for (report.missing_required_tensors[0..limit]) |name| {
            std.log.err("  {s}", .{name});
        }
        if (report.missing_required_tensors.len > limit) {
            std.log.err("  ... and {d} more", .{report.missing_required_tensors.len - limit});
        }
        return error.MissingRequiredWeights;
    }
}

fn ensureMetalGgufInspectionCompatible(report: GgufInspectionReport, gguf_path: []const u8) !void {
    if (comptime !build_options.enable_metal) return;
    if (metalGgufIncompatibleTensorType(report)) |entry| {
        std.log.err(
            "GGUF {s} uses {d} {s} tensors that are not release-safe on Metal; rejecting Metal backend",
            .{ gguf_path, entry.count, entry.tensor_type.name() },
        );
        return error.UnsupportedQuantFormatForMetalOnly;
    }
}

fn metalGgufIncompatibleTensorType(report: GgufInspectionReport) ?UnsupportedTensorTypeCount {
    if (comptime !build_options.enable_metal) return null;
    for (report.all_tensor_types) |entry| {
        if (!metal_runtime.isMetalNativeSupported(entry.tensor_type)) return entry;
    }
    return null;
}

pub fn ggufInspectionSupportsBackend(report: GgufInspectionReport, backend: BackendType) bool {
    if (report.unsupported_tensor_types.len > 0 or report.missing_required_tensors.len > 0)
        return false;
    return switch (backend) {
        .metal => metalGgufIncompatibleTensorType(report) == null,
        .native, .cuda, .onnx, .wasm => true,
        .pjrt => false,
    };
}

fn normalizeWeightKey(store_kind: tensor_store_mod.StoreKind, arch_config: ArchConfig, key: []const u8, buf: *[256]u8) ![]const u8 {
    if (store_kind != .gguf) return key;
    return switch (arch_config) {
        .gpt => |cfg| normalizeGgufGptWeightKey(cfg, key, buf) orelse key,
        .bert => bert.normalizeGgufWeightKey(key, buf) orelse key,
        else => key,
    };
}

fn refineGemma4AttentionKEqualVFromGgufTensors(config: *gpt_mod.Config, file: *const gguf_mod.format.File) void {
    if (config.family != .gemma or !config.usesGemmaSlidingAttention()) return;

    const layer = firstFullAttentionLayer(config.*) orelse return;
    if (hasGgufLayerWeight(file, layer, "attn_v.weight", "self_attn.v_proj.weight")) {
        config.attention_k_eq_v = false;
        return;
    }
    if (hasGgufLayerWeight(file, layer, "attn_k.weight", "self_attn.k_proj.weight")) {
        config.attention_k_eq_v = true;
    }
}

fn firstFullAttentionLayer(config: gpt_mod.Config) ?usize {
    if (config.num_hidden_layers == 0) return null;
    for (0..config.num_hidden_layers) |layer| {
        if (!config.layerUsesSlidingAttention(layer)) return layer;
    }
    return null;
}

fn hasGgufLayerWeight(file: *const gguf_mod.format.File, layer: usize, comptime raw_suffix: []const u8, comptime hf_suffix: []const u8) bool {
    var raw_buf: [128]u8 = undefined;
    const raw_name = std.fmt.bufPrint(&raw_buf, "blk.{d}.{s}", .{ layer, raw_suffix }) catch return false;
    if (findGgufTensor(file, raw_name) != null) return true;

    var hf_buf: [160]u8 = undefined;
    const hf_name = std.fmt.bufPrint(&hf_buf, "model.layers.{d}.{s}", .{ layer, hf_suffix }) catch return false;
    return findGgufTensor(file, hf_name) != null;
}

fn maybeInferGptAttentionLayoutFromStore(
    allocator: std.mem.Allocator,
    store: tensor_store_mod.TensorStore,
    all_names: [][]const u8,
    arch_config: *ArchConfig,
) !void {
    var cfg = switch (arch_config.*) {
        .gpt => |value| value,
        else => return,
    };

    const q_proj_name = findTensorNameByPriority(all_names, &.{
        "model.language_model.layers.0.self_attn.q_proj.weight",
        "language_model.model.layers.0.self_attn.q_proj.weight",
        "model.layers.0.self_attn.q_proj.weight",
        "layers.0.self_attn.q_proj.weight",
    }) orelse return;
    const k_proj_name = findTensorNameByPriority(all_names, &.{
        "model.language_model.layers.0.self_attn.k_proj.weight",
        "language_model.model.layers.0.self_attn.k_proj.weight",
        "model.layers.0.self_attn.k_proj.weight",
        "layers.0.self_attn.k_proj.weight",
    }) orelse return;
    const q_norm_name = findTensorNameByPriority(all_names, &.{
        "model.language_model.layers.0.self_attn.q_norm.weight",
        "language_model.model.layers.0.self_attn.q_norm.weight",
        "model.layers.0.self_attn.q_norm.weight",
        "layers.0.self_attn.q_norm.weight",
        "model.language_model.layers.0.self_attn.k_norm.weight",
        "language_model.model.layers.0.self_attn.k_norm.weight",
        "model.layers.0.self_attn.k_norm.weight",
        "layers.0.self_attn.k_norm.weight",
    }) orelse return;

    const q_proj_out = try leadingTensorDim(allocator, store, q_proj_name);
    const k_proj_out = try leadingTensorDim(allocator, store, k_proj_name);
    const head_dim = try leadingTensorDim(allocator, store, q_norm_name);
    if (head_dim == 0 or q_proj_out == 0 or k_proj_out == 0) return;
    if (q_proj_out % head_dim != 0 or k_proj_out % head_dim != 0) return;

    const inferred_heads: u32 = @intCast(q_proj_out / head_dim);
    const inferred_kv_heads: u32 = @intCast(k_proj_out / head_dim);
    const inferred_head_dim: u32 = @intCast(head_dim);
    if (cfg.num_attention_heads == inferred_heads and
        cfg.effectiveKVHeads() == inferred_kv_heads and
        cfg.headDim() == inferred_head_dim) return;

    cfg.num_attention_heads = inferred_heads;
    cfg.num_key_value_heads = inferred_kv_heads;
    cfg.attention_head_dim = inferred_head_dim;
    arch_config.* = .{ .gpt = cfg };
    std.log.info(
        "inferred GPT attention layout from weights: heads={d} kv_heads={d} head_dim={d}",
        .{ inferred_heads, inferred_kv_heads, inferred_head_dim },
    );
}

fn findTensorNameByPriority(all_names: [][]const u8, candidates: []const []const u8) ?[]const u8 {
    for (candidates) |candidate| {
        for (all_names) |name| {
            if (std.mem.eql(u8, name, candidate)) return name;
        }
    }
    return null;
}

fn leadingTensorDim(
    allocator: std.mem.Allocator,
    store: tensor_store_mod.TensorStore,
    name: []const u8,
) !usize {
    var ref = try store.describeTensor(allocator, name);
    defer ref.deinit(allocator);
    var loaded = try store.loadTensorRef(&ref);
    defer loaded.deinit();
    if (loaded.tensor.shape.len == 0) return 0;
    return @intCast(loaded.tensor.shape[0]);
}

fn normalizeGgufGptWeightKey(config: gpt_mod.Config, key: []const u8, buf: *[256]u8) ?[]const u8 {
    if (!gpt_mod.ggufWeightMappingSupported(config.family)) return null;

    if (std.mem.eql(u8, key, "token_embd.weight")) return "model.embed_tokens.weight";
    if (std.mem.eql(u8, key, "output_norm.weight")) return "model.norm.weight";
    if (config.family == .deepseek_v4) {
        if (deepseek_v4_arch.normalizeGgufGlobalWeightKey(key)) |normalized| return normalized;
    }
    if (std.mem.eql(u8, key, "output_norm.bias")) {
        return switch (config.family) {
            .phi => "model.norm.bias",
            else => null,
        };
    }
    if (std.mem.eql(u8, key, "output.weight")) return "lm_head.weight";

    // Gemma 4 PLE: global tensors.
    if (std.mem.eql(u8, key, "per_layer_token_embd.weight")) return "model.per_layer_input.per_layer_token_embd.weight";
    if (std.mem.eql(u8, key, "per_layer_model_proj.weight")) return "model.per_layer_input.per_layer_model_proj.weight";
    if (std.mem.eql(u8, key, "per_layer_proj_norm.weight")) return "model.per_layer_input.per_layer_proj_norm.weight";

    if (!std.mem.startsWith(u8, key, "blk.")) return null;

    var parts = std.mem.splitScalar(u8, key, '.');
    _ = parts.next() orelse return null; // blk
    const layer_str = parts.next() orelse return null;
    const suffix_start = 4 + layer_str.len + 1;
    if (suffix_start >= key.len) return null;
    const suffix = key[suffix_start..];
    const layer = std.fmt.parseInt(usize, layer_str, 10) catch return null;

    if (config.family == .deepseek_v4) {
        return deepseek_v4_arch.normalizeGgufWeightKey(layer, suffix, buf);
    }

    if (std.mem.eql(u8, suffix, "attn_norm.weight")) {
        return std.fmt.bufPrint(buf, "model.layers.{d}.input_layernorm.weight", .{layer}) catch null;
    }
    if (std.mem.eql(u8, suffix, "attn_norm.bias")) {
        return switch (config.family) {
            .phi => std.fmt.bufPrint(buf, "model.layers.{d}.input_layernorm.bias", .{layer}) catch null,
            else => null,
        };
    }
    if (std.mem.eql(u8, suffix, "ffn_norm.weight")) {
        return switch (config.family) {
            .phi => std.fmt.bufPrint(buf, "model.layers.{d}.post_attention_layernorm.weight", .{layer}) catch null,
            .gemma => std.fmt.bufPrint(buf, "model.layers.{d}.pre_feedforward_layernorm.weight", .{layer}) catch null,
            else => std.fmt.bufPrint(buf, "model.layers.{d}.post_attention_layernorm.weight", .{layer}) catch null,
        };
    }
    if (std.mem.eql(u8, suffix, "ffn_norm.bias")) {
        return switch (config.family) {
            .phi => std.fmt.bufPrint(buf, "model.layers.{d}.post_attention_layernorm.bias", .{layer}) catch null,
            else => null,
        };
    }
    if (std.mem.eql(u8, suffix, "attn_q_norm.weight")) {
        return std.fmt.bufPrint(buf, "model.layers.{d}.self_attn.q_norm.weight", .{layer}) catch null;
    }
    if (std.mem.eql(u8, suffix, "attn_k_norm.weight")) {
        return std.fmt.bufPrint(buf, "model.layers.{d}.self_attn.k_norm.weight", .{layer}) catch null;
    }
    if (std.mem.eql(u8, suffix, "attn_sub_norm.weight")) {
        return std.fmt.bufPrint(buf, "model.layers.{d}.self_attn.attn_sub_norm.weight", .{layer}) catch null;
    }
    if (std.mem.eql(u8, suffix, "ffn_sub_norm.weight")) {
        return std.fmt.bufPrint(buf, "model.layers.{d}.mlp.ffn_sub_norm.weight", .{layer}) catch null;
    }
    if (std.mem.eql(u8, suffix, "post_attention_norm.weight")) {
        return std.fmt.bufPrint(buf, "model.layers.{d}.post_attention_layernorm.weight", .{layer}) catch null;
    }
    if (std.mem.eql(u8, suffix, "post_ffw_norm.weight")) {
        return std.fmt.bufPrint(buf, "model.layers.{d}.post_feedforward_layernorm.weight", .{layer}) catch null;
    }
    // Gemma 4: dual-FFN norms (shared expert + MoE routed experts).
    if (std.mem.eql(u8, suffix, "post_ffw_norm_1.weight")) {
        return std.fmt.bufPrint(buf, "model.layers.{d}.post_feedforward_layernorm_1.weight", .{layer}) catch null;
    }
    if (std.mem.eql(u8, suffix, "pre_ffw_norm_2.weight")) {
        return std.fmt.bufPrint(buf, "model.layers.{d}.pre_feedforward_layernorm_2.weight", .{layer}) catch null;
    }
    if (std.mem.eql(u8, suffix, "post_ffw_norm_2.weight")) {
        return std.fmt.bufPrint(buf, "model.layers.{d}.post_feedforward_layernorm_2.weight", .{layer}) catch null;
    }
    // Gemma 4 MoE: router input scale and per-expert output scale.
    if (std.mem.eql(u8, suffix, "ffn_gate_inp.scale")) {
        return std.fmt.bufPrint(buf, "model.layers.{d}.block_sparse_moe.gate.input_scale", .{layer}) catch null;
    }
    if (std.mem.eql(u8, suffix, "ffn_down_exps.scale")) {
        return std.fmt.bufPrint(buf, "model.layers.{d}.block_sparse_moe.expert_output_scale", .{layer}) catch null;
    }
    if (std.mem.eql(u8, suffix, "attn_q.weight")) {
        return std.fmt.bufPrint(buf, "model.layers.{d}.self_attn.q_proj.weight", .{layer}) catch null;
    }
    if (std.mem.eql(u8, suffix, "attn_q.bias")) {
        return switch (config.family) {
            .qwen2, .phi => std.fmt.bufPrint(buf, "model.layers.{d}.self_attn.q_proj.bias", .{layer}) catch null,
            else => null,
        };
    }
    if (std.mem.eql(u8, suffix, "attn_k.weight")) {
        return std.fmt.bufPrint(buf, "model.layers.{d}.self_attn.k_proj.weight", .{layer}) catch null;
    }
    if (std.mem.eql(u8, suffix, "attn_k.bias")) {
        return switch (config.family) {
            .qwen2, .phi => std.fmt.bufPrint(buf, "model.layers.{d}.self_attn.k_proj.bias", .{layer}) catch null,
            else => null,
        };
    }
    if (std.mem.eql(u8, suffix, "attn_v.weight")) {
        return std.fmt.bufPrint(buf, "model.layers.{d}.self_attn.v_proj.weight", .{layer}) catch null;
    }
    if (std.mem.eql(u8, suffix, "attn_v.bias")) {
        return switch (config.family) {
            .qwen2, .phi => std.fmt.bufPrint(buf, "model.layers.{d}.self_attn.v_proj.bias", .{layer}) catch null,
            else => null,
        };
    }
    if (std.mem.eql(u8, suffix, "attn_output.weight")) {
        return std.fmt.bufPrint(buf, "model.layers.{d}.self_attn.o_proj.weight", .{layer}) catch null;
    }
    if (std.mem.eql(u8, suffix, "ffn_gate.weight")) {
        return std.fmt.bufPrint(buf, "model.layers.{d}.mlp.gate_proj.weight", .{layer}) catch null;
    }
    if (std.mem.eql(u8, suffix, "ffn_up.weight")) {
        return switch (config.family) {
            .phi => std.fmt.bufPrint(buf, "model.layers.{d}.mlp.fc1_proj.weight", .{layer}) catch null,
            else => std.fmt.bufPrint(buf, "model.layers.{d}.mlp.up_proj.weight", .{layer}) catch null,
        };
    }
    if (std.mem.eql(u8, suffix, "ffn_down.weight")) {
        return switch (config.family) {
            .phi => std.fmt.bufPrint(buf, "model.layers.{d}.mlp.fc2_proj.weight", .{layer}) catch null,
            else => std.fmt.bufPrint(buf, "model.layers.{d}.mlp.down_proj.weight", .{layer}) catch null,
        };
    }
    if (std.mem.eql(u8, suffix, "ffn_gate_inp.weight")) {
        return std.fmt.bufPrint(buf, "model.layers.{d}.block_sparse_moe.gate.weight", .{layer}) catch null;
    }
    if (normalizeGgufMoeExpertWeight(layer, suffix, "ffn_gate.", "w1", buf)) |name| return name;
    if (normalizeGgufMoeExpertWeight(layer, suffix, "ffn_down.", "w2", buf)) |name| return name;
    if (normalizeGgufMoeExpertWeight(layer, suffix, "ffn_up.", "w3", buf)) |name| return name;
    // Gemma 4: shared expert weights.
    if (std.mem.eql(u8, suffix, "ffn_gate_shexp.weight")) {
        return std.fmt.bufPrint(buf, "model.layers.{d}.block_sparse_moe.shared_expert.gate_proj.weight", .{layer}) catch null;
    }
    if (std.mem.eql(u8, suffix, "ffn_down_shexp.weight")) {
        return std.fmt.bufPrint(buf, "model.layers.{d}.block_sparse_moe.shared_expert.down_proj.weight", .{layer}) catch null;
    }
    if (std.mem.eql(u8, suffix, "ffn_up_shexp.weight")) {
        return std.fmt.bufPrint(buf, "model.layers.{d}.block_sparse_moe.shared_expert.up_proj.weight", .{layer}) catch null;
    }
    // Gemma 4 PLE: per-layer tensors.
    if (std.mem.eql(u8, suffix, "inp_gate.weight")) {
        return std.fmt.bufPrint(buf, "model.layers.{d}.per_layer_input.inp_gate.weight", .{layer}) catch null;
    }
    if (std.mem.eql(u8, suffix, "proj.weight")) {
        return std.fmt.bufPrint(buf, "model.layers.{d}.per_layer_input.proj.weight", .{layer}) catch null;
    }
    if (std.mem.eql(u8, suffix, "layer_output_scale.weight")) {
        return std.fmt.bufPrint(buf, "model.layers.{d}.per_layer_input.layer_output_scale.weight", .{layer}) catch null;
    }
    if (std.mem.eql(u8, suffix, "post_norm.weight")) {
        return std.fmt.bufPrint(buf, "model.layers.{d}.per_layer_input.post_norm.weight", .{layer}) catch null;
    }
    return null;
}

fn appendPackedMoeLazyWeights(
    allocator: std.mem.Allocator,
    lazy_weights: anytype,
    store: tensor_store_mod.TensorStore,
    arch_config: ArchConfig,
    full_name: []const u8,
    plan_context: runtime.tier.planner.PlanContext,
) !bool {
    if (store.kind() != .gguf) return false;
    const packed_tensor = parsePackedMoeTensor(full_name) orelse return false;
    const gpt_cfg = switch (arch_config) {
        .gpt => |cfg| cfg,
        else => return false,
    };
    if (gpt_cfg.family == .deepseek_v4) {
        return appendDeepseekV4MoeLazyWeights(allocator, lazy_weights, store, gpt_cfg, packed_tensor, full_name, plan_context);
    }
    if (!gpt_cfg.usesMoe() or gpt_cfg.num_local_experts == 0) return false;

    var base_ref = try store.describeTensor(allocator, full_name);
    defer base_ref.deinit(allocator);

    // For fused gate+up, register both w1 and w3 projections from the same source tensor.
    const projs: []const []const u8 = if (packed_tensor.proj2) |p2|
        &.{ packed_tensor.proj, p2 }
    else
        &.{packed_tensor.proj};

    for (projs, 0..) |proj, proj_idx| {
        const key = try std.fmt.allocPrint(
            allocator,
            "model.layers.{d}.block_sparse_moe.packed.{s}.weight",
            .{ packed_tensor.layer, proj },
        );
        errdefer allocator.free(key);
        try lazy_weights.put(allocator, key, .{
            .tensor_ref = .{
                .name = try allocator.dupe(u8, key),
                .source_name = try allocator.dupe(u8, base_ref.name),
                .byte_len = base_ref.byte_len,
                .quantized = base_ref.quantized,
                .packed_expert_count = gpt_cfg.num_local_experts,
                .fused_gate_up = packed_tensor.fused_gate_up,
                .fused_gate_up_index = @intCast(proj_idx),
            },
            .projection_mask = projectionMaskForWeightKey(key),
            .placement = runtime.tier.planner.planForContext(plan_context, key, base_ref.byte_len),
        });
    }

    return true;
}

fn appendDeepseekV4MoeLazyWeights(
    allocator: std.mem.Allocator,
    lazy_weights: anytype,
    store: tensor_store_mod.TensorStore,
    gpt_cfg: gpt_mod.Config,
    packed_tensor: PackedMoeTensor,
    full_name: []const u8,
    plan_context: runtime.tier.planner.PlanContext,
) !bool {
    if (!gpt_cfg.usesMoe() or gpt_cfg.num_local_experts == 0) return false;

    var base_ref = try store.describeTensor(allocator, full_name);
    defer base_ref.deinit(allocator);

    const source_byte_len = if (gpt_cfg.num_local_experts > 0)
        base_ref.byte_len / @as(usize, @intCast(gpt_cfg.num_local_experts))
    else
        base_ref.byte_len;
    const projs: []const []const u8 = if (packed_tensor.fused_gate_up)
        &.{"gate_up_proj"}
    else if (packed_tensor.proj2) |p2|
        &.{ packed_tensor.proj, p2 }
    else
        &.{packed_tensor.proj};

    for (0..gpt_cfg.num_local_experts) |expert_index| {
        for (projs) |proj| {
            const canonical_proj = if (packed_tensor.fused_gate_up)
                proj
            else
                deepseek_v4_arch.moeProjectionName(proj) orelse continue;
            const key = try std.fmt.allocPrint(
                allocator,
                "model.layers.{d}.mlp.experts.{d}.{s}",
                .{ packed_tensor.layer, expert_index, canonical_proj },
            );
            errdefer allocator.free(key);
            if (lazy_weights.contains(key)) {
                allocator.free(key);
                continue;
            }
            try lazy_weights.put(allocator, key, .{
                .tensor_ref = .{
                    .name = try allocator.dupe(u8, key),
                    .source_name = try allocator.dupe(u8, base_ref.name),
                    .byte_len = source_byte_len,
                    .quantized = base_ref.quantized,
                    .packed_expert_index = @intCast(expert_index),
                    .packed_expert_count = gpt_cfg.num_local_experts,
                },
                .expert_coord = .{
                    .layer_index = packed_tensor.layer,
                    .expert_index = @intCast(expert_index),
                },
                .projection_mask = projectionMaskForWeightKey(key),
                .placement = runtime.tier.planner.planForContext(plan_context, key, source_byte_len),
            });
        }
    }

    return true;
}

const PackedMoeTensor = struct {
    layer: usize,
    proj: []const u8,
    /// For fused gate+up tensors, the second projection to register.
    proj2: ?[]const u8 = null,
    /// Whether this is a fused gate+up tensor (w1+w3 interleaved in dim 1).
    fused_gate_up: bool = false,
};

fn parsePackedMoeTensor(full_name: []const u8) ?PackedMoeTensor {
    if (!std.mem.startsWith(u8, full_name, "blk.")) return null;
    var parts = std.mem.splitScalar(u8, full_name, '.');
    _ = parts.next() orelse return null;
    const layer_str = parts.next() orelse return null;
    const layer = std.fmt.parseInt(usize, layer_str, 10) catch return null;
    const suffix_start = 4 + layer_str.len + 1;
    if (suffix_start >= full_name.len) return null;
    const suffix = full_name[suffix_start..];

    if (std.mem.eql(u8, suffix, "ffn_gate_exps.weight")) return .{ .layer = layer, .proj = "w1" };
    if (std.mem.eql(u8, suffix, "ffn_down_exps.weight")) return .{ .layer = layer, .proj = "w2" };
    if (std.mem.eql(u8, suffix, "ffn_up_exps.weight")) return .{ .layer = layer, .proj = "w3" };
    if (std.mem.eql(u8, suffix, "ffn_gate_up_exps.weight")) return .{ .layer = layer, .proj = "w1", .proj2 = "w3", .fused_gate_up = true };
    return null;
}

fn normalizeGgufMoeExpertWeight(layer: usize, suffix: []const u8, prefix: []const u8, proj: []const u8, buf: *[256]u8) ?[]const u8 {
    if (!std.mem.startsWith(u8, suffix, prefix) or !std.mem.endsWith(u8, suffix, ".weight")) return null;
    const expert_str = suffix[prefix.len .. suffix.len - ".weight".len];
    if (expert_str.len == 0) return null;
    const expert = std.fmt.parseInt(usize, expert_str, 10) catch return null;
    return std.fmt.bufPrint(buf, "model.layers.{d}.block_sparse_moe.experts.{d}.{s}.weight", .{ layer, expert, proj }) catch null;
}

fn refineArchConfigFromWeights(arch_config: *ArchConfig, weights: *const std.StringHashMapUnmanaged(LoadedWeight)) void {
    switch (arch_config.*) {
        .gpt => |*cfg| refineGptConfigFromWeights(cfg, weights),
        else => {},
    }
}

fn refineArchConfigFromStore(
    allocator: std.mem.Allocator,
    store: tensor_store_mod.TensorStore,
    all_names: [][]const u8,
    arch_config: *ArchConfig,
) !void {
    switch (arch_config.*) {
        .gpt => |*cfg| try refineGptConfigFromStore(allocator, store, all_names, cfg),
        else => {},
    }
}

fn refineGptConfigFromStore(
    allocator: std.mem.Allocator,
    store: tensor_store_mod.TensorStore,
    all_names: [][]const u8,
    config: *gpt_mod.Config,
) !void {
    if (store.kind() == .gguf) {
        deepseek_v4_arch.inferSchedulesFromGgufNames(config, all_names);
    }

    const embed_name = findTensorNameByPriority(all_names, &.{
        "model.language_model.embed_tokens.weight",
        "language_model.model.embed_tokens.weight",
        "model.embed_tokens.weight",
        "embed_tokens.weight",
    });
    const lm_head_name = findTensorNameByPriority(all_names, &.{
        "model.language_model.lm_head.weight",
        "lm_head.weight",
        "language_model.lm_head.weight",
        "output.weight",
    });

    if (embed_name) |name| {
        var ref = try store.describeTensor(allocator, name);
        defer ref.deinit(allocator);
        var loaded = try store.loadTensorRef(&ref);
        defer loaded.deinit();
        if (loaded.tensor.shape.len >= 2) {
            config.vocab_size = @intCast(loaded.tensor.shape[0]);
            config.hidden_size = @intCast(loaded.tensor.shape[1]);
            if (lm_head_name == null) config.weight_tying = true;
            return;
        }
    }

    if (lm_head_name) |name| {
        var ref = try store.describeTensor(allocator, name);
        defer ref.deinit(allocator);
        var loaded = try store.loadTensorRef(&ref);
        defer loaded.deinit();
        if (loaded.tensor.shape.len >= 2) {
            config.vocab_size = @intCast(loaded.tensor.shape[0]);
            config.hidden_size = @intCast(loaded.tensor.shape[1]);
        }
    }
}

fn shouldLazyLoadWeight(store_kind: tensor_store_mod.StoreKind, arch_config: ArchConfig, key: []const u8) bool {
    if (store_kind != .gguf) return false;
    return switch (arch_config) {
        .gpt => |cfg| cfg.usesMoe() and (std.mem.indexOf(u8, key, ".block_sparse_moe.experts.") != null or (cfg.family == .deepseek_v4 and std.mem.indexOf(u8, key, ".mlp.experts.") != null)),
        else => false,
    };
}

fn shouldKeepResidentWeightQuantizedOnly(
    allocator: std.mem.Allocator,
    store: tensor_store_mod.TensorStore,
    arch_config: ArchConfig,
    key: []const u8,
    source_name: []const u8,
) !bool {
    if (store.kind() != .gguf) return false;
    const tensor_ref = try store.describeTensor(allocator, source_name);
    defer {
        var ref = tensor_ref;
        ref.deinit(allocator);
    }
    if (!tensor_ref.quantized) return false;
    const tensor_type = if (store.ggufFile()) |file| blk: {
        const tensor = gguf_mod.tensor_catalog.Catalog.init(file).find(source_name) orelse break :blk null;
        break :blk tensor.tensor_type;
    } else null;

    if (isGptEmbeddingTableKey(key)) {
        var storage = (try store.loadQuantizedStorageRef(&tensor_ref)) orelse return false;
        defer storage.deinit();
        return switch (arch_config) {
            .gpt => |cfg| shouldKeepResidentGptEmbeddingQuantizedOnly(cfg, storage.tensor_type),
            else => false,
        };
    }

    return switch (arch_config) {
        .gpt => |cfg| shouldKeepResidentGptWeightQuantizedOnly(cfg, key, tensor_type),
        .clip, .clap => shouldKeepResidentClipClapWeightQuantizedOnly(key, tensor_type),
        else => false,
    };
}

fn shouldKeepResidentClipClapWeightQuantizedOnly(
    key: []const u8,
    tensor_type: ?gguf_mod.tensor_types.TensorType,
) bool {
    const known = switch (tensor_type orelse return false) {
        .known => |value| value,
        else => return false,
    };
    switch (known) {
        .Q1_0,
        .Q4_0,
        .Q4_1,
        .Q5_0,
        .Q5_1,
        .Q8_0,
        .Q8_1,
        .Q2_K,
        .Q3_K,
        .Q4_K,
        .Q5_K,
        .Q6_K,
        .Q8_K,
        => {},
        else => return false,
    }

    const known_prefix =
        std.mem.startsWith(u8, key, "text_model.") or
        std.mem.startsWith(u8, key, "vision_model.") or
        std.mem.startsWith(u8, key, "audio_model.");
    if (!known_prefix) return false;

    return std.mem.endsWith(u8, key, ".self_attn.q_proj.weight") or
        std.mem.endsWith(u8, key, ".self_attn.k_proj.weight") or
        std.mem.endsWith(u8, key, ".self_attn.v_proj.weight") or
        std.mem.endsWith(u8, key, ".self_attn.out_proj.weight") or
        std.mem.endsWith(u8, key, ".attention.self.query.weight") or
        std.mem.endsWith(u8, key, ".attention.self.key.weight") or
        std.mem.endsWith(u8, key, ".attention.self.value.weight") or
        std.mem.endsWith(u8, key, ".attention.output.dense.weight") or
        std.mem.endsWith(u8, key, ".mlp.fc1.weight") or
        std.mem.endsWith(u8, key, ".mlp.fc2.weight") or
        std.mem.endsWith(u8, key, ".intermediate.dense.weight") or
        std.mem.endsWith(u8, key, ".output.dense.weight");
}

fn shouldKeepGpuHostedLazyWeightDense(backend_type: BackendType, arch_config: ArchConfig, key: []const u8) bool {
    _ = backend_type;
    _ = arch_config;
    _ = key;
    return false;
}

fn isMetalJitTiedEmbeddingWeightKey(key: []const u8) bool {
    return std.mem.eql(u8, key, "embed_tokens.weight") or
        std.mem.endsWith(u8, key, ".embed_tokens.weight") or
        std.mem.eql(u8, key, "wte.weight") or
        std.mem.endsWith(u8, key, ".wte.weight");
}

fn isMetalJitLinearWeightKey(key: []const u8, projection_mask: u8, tied_lm_head: bool) bool {
    if (projection_mask != 0) return true;
    if (!std.mem.endsWith(u8, key, ".weight")) return false;
    // A tied GPT head reaches the token table through ordinary linearNoBias,
    // even though the same storage is also used by embedding lookup.
    if (tied_lm_head and isMetalJitTiedEmbeddingWeightKey(key)) return true;
    // These two-dimensional tensors are lookup tables, not matmul routes.
    const non_linear_tables = [_][]const u8{
        "embed_tokens",
        "embedding",
        "embeddings",
        "position_embeddings",
        "token_type_embeddings",
        "word_embeddings",
        "rope_freqs",
        "wpe.weight",
        "wte.weight",
    };
    for (non_linear_tables) |needle| {
        if (std.mem.indexOf(u8, key, needle) != null) return false;
    }
    return true;
}

fn metalJitUsesExactProfileScope(config: kernel_jit.Config) bool {
    return config.profile_capture_only or config.qualified_profile_path != null;
}

fn metalJitTensorTypeInExactProfileScope(tensor_type: gguf_mod.tensor_types.KnownTensorType) bool {
    // Keep this boundary aligned with the workload tuner's first exact slice.
    // Other formats remain part of the general JIT scope, but cannot affect a
    // Q4_0/Q4_K/Q6_K capture or the later activation of that contract.
    return tensor_type == .Q4_0 or tensor_type == .Q4_K or tensor_type == .Q6_K;
}

/// Derive Metal JIT slots from quantized matrices that the loaded WeightStore
/// can actually feed to linear dispatch. Global GGUF type presence is too
/// broad: embedding tables and dequant-only tensors must not become required
/// matmul routes.
fn includeMetalJitTensorShape(
    scope: *MetalJitRouteScope,
    tensor_type: gguf_mod.tensor_types.KnownTensorType,
    dimensions: []const u64,
    fused_gate_up: bool,
    exact_profile_scope: bool,
) void {
    if (comptime !build_options.enable_metal) return;
    if (exact_profile_scope and !metalJitTensorTypeInExactProfileScope(tensor_type)) return;
    const format = metal_runtime.MetalJitRouteScope.quantFormatForTensorType(tensor_type) orelse return;
    if (dimensions.len < 2) {
        scope.invalidate(.invalid_shape, format, 0, 0);
        return;
    }
    // GGUF stores matrices as [in_dim, out_dim]; the JIT scope uses logical
    // [out_dim, in_dim] so its qualification fixtures match linear dispatch.
    const in_dim = std.math.cast(usize, dimensions[0]) orelse {
        scope.invalidate(.dimension_overflow, format, 0, 0);
        return;
    };
    var out_dim = std.math.cast(usize, dimensions[1]) orelse {
        scope.invalidate(.dimension_overflow, format, 0, in_dim);
        return;
    };
    // Packed GGUF gate+up sources register two logical lazy projections; each
    // runtime dispatch sees one half of the combined output dimension.
    if (fused_gate_up) {
        if (out_dim == 0 or out_dim % 2 != 0) {
            scope.invalidate(.invalid_shape, format, out_dim, in_dim);
            return;
        }
        out_dim /= 2;
    }
    _ = scope.includeQuantShape(format, out_dim, in_dim);
}

/// Composite stores expose their primary GGUF through `ggufFile`, so a linear
/// weight from a secondary GGUF may not be present in that catalog. Recover
/// the logical shape from the store's existing zero-copy quantized view.
fn includeMetalJitStorageShape(
    scope: *MetalJitRouteScope,
    storage: *const weight_source_mod.QuantizedStorage,
    fused_gate_up: bool,
    exact_profile_scope: bool,
) void {
    if (comptime !build_options.enable_metal) return;
    const tensor_type = switch (storage.tensor_type) {
        .known => |known| known,
        .bitnet_tl2 => return,
        .unknown => {
            scope.invalidate(.scope_discovery, .unknown, 0, 0);
            return;
        },
    };
    if (exact_profile_scope and !metalJitTensorTypeInExactProfileScope(tensor_type)) return;
    const format = metal_runtime.MetalJitRouteScope.quantFormatForTensorType(tensor_type) orelse return;
    if (storage.shape.len != 2 or storage.shape[0] <= 0 or storage.shape[1] <= 0) {
        scope.invalidate(.invalid_shape, format, 0, 0);
        return;
    }
    var out_dim = std.math.cast(usize, storage.shape[0]) orelse {
        scope.invalidate(.dimension_overflow, format, 0, 0);
        return;
    };
    const in_dim = std.math.cast(usize, storage.shape[1]) orelse {
        scope.invalidate(.dimension_overflow, format, out_dim, 0);
        return;
    };
    if (fused_gate_up) {
        if (out_dim % 2 != 0) {
            scope.invalidate(.invalid_shape, format, out_dim, in_dim);
            return;
        }
        out_dim /= 2;
    }
    _ = scope.includeQuantShape(format, out_dim, in_dim);
}

fn includeMetalJitLinearWeightFormats(
    scope: *MetalJitRouteScope,
    lazy_weights: *const std.StringHashMapUnmanaged(gpu_hosted_store_mod.LazyWeightEntry),
    tensor_store: ?tensor_store_mod.TensorStore,
    direct_quant_enabled: bool,
    arch_config: ArchConfig,
    exact_profile_scope: bool,
) void {
    if (comptime !build_options.enable_metal) return;
    if (!direct_quant_enabled) return;
    const store = tensor_store orelse return;
    const file = store.ggufFile() orelse return;
    const catalog = gguf_mod.tensor_catalog.Catalog.init(file);
    const tied_lm_head = switch (arch_config) {
        .gpt => |config| blk: {
            if (config.weight_tying) break :blk true;
            var has_separate_head = false;
            var names = lazy_weights.keyIterator();
            while (names.next()) |key| {
                if (isMetalJitOutputHeadKey(key.*)) {
                    has_separate_head = true;
                    break;
                }
            }
            break :blk !has_separate_head;
        },
        else => false,
    };
    var iterator = lazy_weights.iterator();
    while (iterator.next()) |entry| {
        const lazy = entry.value_ptr;
        if (!lazy.tensor_ref.quantized or lazy.prefer_dense or
            !isMetalJitLinearWeightKey(entry.key_ptr.*, lazy.projection_mask, tied_lm_head)) continue;
        const source_name = lazy.tensor_ref.source_name orelse lazy.tensor_ref.name;
        const tensor = catalog.find(source_name) orelse {
            var storage = (store.loadQuantizedStorageRef(&lazy.tensor_ref) catch |err| {
                std.log.warn(
                    "Metal runtime JIT scope discovery failed key={s} source={s}: {s}",
                    .{ entry.key_ptr.*, source_name, @errorName(err) },
                );
                scope.invalidate(.scope_discovery, .unknown, 0, 0);
                continue;
            }) orelse {
                std.log.warn(
                    "Metal runtime JIT scope discovery found no quantized storage key={s} source={s}",
                    .{ entry.key_ptr.*, source_name },
                );
                scope.invalidate(.scope_discovery, .unknown, 0, 0);
                continue;
            };
            defer storage.deinit();
            includeMetalJitStorageShape(scope, &storage, lazy.tensor_ref.fused_gate_up, exact_profile_scope);
            continue;
        };
        switch (tensor.tensor_type) {
            .known => |known| {
                if (exact_profile_scope and !metalJitTensorTypeInExactProfileScope(known)) continue;
                const format = metal_runtime.MetalJitRouteScope.quantFormatForTensorType(known) orelse continue;
                if (tensor.dimensions.len != 2 and lazy.projection_mask == 0) {
                    scope.invalidate(.invalid_shape, format, 0, 0);
                    continue;
                }
                includeMetalJitTensorShape(scope, known, tensor.dimensions, lazy.tensor_ref.fused_gate_up, exact_profile_scope);
            },
            // BitNet TL2 has a bundled Metal route but no generated runtime-JIT
            // artifact in this catalog, so it is outside the JIT scope.
            .bitnet_tl2 => {},
            .unknown => scope.invalidate(.scope_discovery, .unknown, 0, 0),
        }
    }
}

fn isMetalJitOutputHeadKey(key: []const u8) bool {
    return std.mem.eql(u8, key, "output.weight") or
        std.mem.eql(u8, key, "lm_head.weight") or
        std.mem.endsWith(u8, key, ".lm_head.weight");
}

test "Metal JIT scope records GGUF linear dimensions in logical order" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    var scope = metal_runtime.MetalJitRouteScope.none();
    includeMetalJitTensorShape(&scope, .Q2_K, &.{ 512, 256 }, false, false);
    const shapes = scope.observedShapes(.q2_k);
    try std.testing.expectEqual(@as(usize, 1), shapes.len);
    try std.testing.expectEqual([2]usize{ 256, 512 }, shapes[0]);
    try std.testing.expectEqual(@as(usize, 1), metal_runtime.metalJitProductionRouteCount(scope));

    var fused_scope = metal_runtime.MetalJitRouteScope.none();
    includeMetalJitTensorShape(&fused_scope, .Q2_K, &.{ 512, 512, 8 }, true, false);
    try std.testing.expectEqual([2]usize{ 256, 512 }, fused_scope.observedShapes(.q2_k)[0]);
    includeMetalJitTensorShape(&fused_scope, .Q2_K, &.{ 512, 511, 8 }, true, false);
    try std.testing.expect(!fused_scope.conformance_complete);

    // LM-head projections can execute with multiple rows, so they must remain
    // in scope. A synthetic fixture that exceeds the bounded preload footprint
    // stays on the bundled route instead of being silently treated as covered.
    try std.testing.expect(isMetalJitLinearWeightKey("lm_head.weight", 0, false));
    try std.testing.expect(isMetalJitOutputHeadKey("lm_head.weight"));
    try std.testing.expect(isMetalJitOutputHeadKey("model.language_model.lm_head.weight"));
    try std.testing.expect(isMetalJitOutputHeadKey("output.weight"));
    try std.testing.expect(!isMetalJitOutputHeadKey("model.embed_tokens.weight"));
    try std.testing.expect(!isMetalJitLinearWeightKey("model.embed_tokens.weight", 0, false));
    try std.testing.expect(isMetalJitLinearWeightKey("model.embed_tokens.weight", 0, true));
    var large_head_scope = metal_runtime.MetalJitRouteScope.none();
    // A supported body matrix remains eligible even when the same format's
    // tied vocabulary projection cannot fit the bounded live fixture. The
    // exact shape gate keeps only the body on the generated pipeline.
    includeMetalJitTensorShape(&large_head_scope, .Q8_K, &.{ 4096, 4096 }, false, false);
    includeMetalJitTensorShape(&large_head_scope, .Q8_K, &.{ 4096, 262144 }, false, false);
    try std.testing.expect(!large_head_scope.conformance_complete);
    try std.testing.expectEqual(metal_runtime.MetalJitScopeInvalidReason.fixture_resource_limit, large_head_scope.invalid_reason);
    try std.testing.expectEqual(@as(usize, 1), large_head_scope.observedShapes(.q8_k).len);
    try std.testing.expectEqual(@as(usize, 1), metal_runtime.metalJitProductionRouteCount(large_head_scope));
}

test "Metal exact profile scope ignores unrelated oversized formats and keeps tuned formats fail closed" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;

    try std.testing.expect(metalJitUsesExactProfileScope(.{ .mode = .shadow, .profile_capture_only = true }));
    try std.testing.expect(metalJitUsesExactProfileScope(.{ .mode = .required, .qualified_profile_path = "profile.json" }));
    try std.testing.expect(!metalJitUsesExactProfileScope(.{ .mode = .required }));

    var profile_scope = metal_runtime.MetalJitRouteScope.none();
    includeMetalJitTensorShape(&profile_scope, .Q4_0, &.{ 10240, 2560 }, false, true);
    includeMetalJitTensorShape(&profile_scope, .Q6_K, &.{ 4096, 1024 }, false, true);
    includeMetalJitTensorShape(&profile_scope, .Q5_K, &.{ 10752, 262144 }, false, true);
    try std.testing.expect(profile_scope.conformance_complete);
    try std.testing.expectEqualSlices([2]usize, &.{.{ 2560, 10240 }}, profile_scope.observedShapes(.q4_0));
    try std.testing.expectEqualSlices([2]usize, &.{.{ 1024, 4096 }}, profile_scope.observedShapes(.q6_k));

    var relevant_oversized_scope = metal_runtime.MetalJitRouteScope.none();
    includeMetalJitTensorShape(&relevant_oversized_scope, .Q4_0, &.{ 10752, 262144 }, false, true);
    try std.testing.expect(!relevant_oversized_scope.conformance_complete);
    try std.testing.expectEqual(
        metal_runtime.MetalJitScopeInvalidReason.fixture_resource_limit,
        relevant_oversized_scope.invalid_reason,
    );
}

test "Metal JIT scope discovers quantized weights in a secondary GGUF store" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    const allocator = std.testing.allocator;

    const SecondaryGgufStore = struct {
        allocator: std.mem.Allocator,
        encoder_file: gguf_mod.format.File,
        load_calls: usize = 0,

        const vtable = tensor_store_mod.TensorStore.VTable{
            .kind = @ptrCast(&kindImpl),
            .weightSource = @ptrCast(&weightSourceImpl),
            .describeTensor = @ptrCast(&describeTensorImpl),
            .describeTensorRange = @ptrCast(&describeTensorRangeImpl),
            .loadTensorRef = @ptrCast(&loadTensorRefImpl),
            .loadQuantizedStorageRef = @ptrCast(&loadQuantizedStorageRefImpl),
            .ggufFile = @ptrCast(&ggufFileImpl),
            .deinit = @ptrCast(&deinitSelf),
        };

        fn tensorStore(self: *@This()) tensor_store_mod.TensorStore {
            return .{ .ptr = self, .vtable = &vtable };
        }

        fn kindImpl(_: *@This()) tensor_store_mod.StoreKind {
            return .gguf;
        }

        fn weightSourceImpl(_: *@This()) !?weight_source_mod.WeightSource {
            return null;
        }

        fn describeTensorImpl(_: *@This(), _: std.mem.Allocator, _: []const u8) !tensor_store_mod.LazyTensorRef {
            return error.UnsupportedOperation;
        }

        fn describeTensorRangeImpl(_: *@This(), _: std.mem.Allocator, _: []const u8) !?tensor_store_mod.TensorRangeRef {
            return null;
        }

        fn loadTensorRefImpl(_: *@This(), _: *const tensor_store_mod.LazyTensorRef) !weight_source_mod.LoadedWeight {
            return error.UnsupportedOperation;
        }

        fn loadQuantizedStorageRefImpl(
            self: *@This(),
            tensor_ref: *const tensor_store_mod.LazyTensorRef,
        ) !?weight_source_mod.QuantizedStorage {
            const source_name = tensor_ref.source_name orelse tensor_ref.name;
            if (!std.mem.eql(u8, source_name, "span_rep.secondary.weight")) return error.TensorNotFound;
            self.load_calls += 1;
            const shape = try self.allocator.dupe(i64, &.{ 3072, 768 });
            errdefer self.allocator.free(shape);
            const owned_source_name = try self.allocator.dupe(u8, source_name);
            errdefer self.allocator.free(owned_source_name);
            return .{
                .tensor_type = .{ .known = .Q4_K },
                .raw_bytes = &.{},
                .shape = shape,
                .source_name = owned_source_name,
                .raw_owned = false,
                .allocator = self.allocator,
            };
        }

        fn ggufFileImpl(self: *@This()) ?*const gguf_mod.format.File {
            return &self.encoder_file;
        }

        fn deinitSelf(_: *@This()) void {}
    };

    var metadata: [0]gguf_mod.format.MetadataEntry = .{};
    var tensors: [0]gguf_mod.format.TensorInfo = .{};
    var secondary = SecondaryGgufStore{
        .allocator = allocator,
        .encoder_file = .{
            .header = .{ .version = 3, .tensor_count = 0, .metadata_count = 0 },
            .metadata = metadata[0..],
            .tensors = tensors[0..],
            .alignment = gguf_mod.format.default_alignment,
            .data_region_offset = 0,
        },
    };

    var lazy_weights = std.StringHashMapUnmanaged(gpu_hosted_store_mod.LazyWeightEntry){};
    defer {
        var iterator = lazy_weights.iterator();
        while (iterator.next()) |entry| {
            entry.value_ptr.tensor_ref.deinit(allocator);
            allocator.free(entry.key_ptr.*);
        }
        lazy_weights.deinit(allocator);
    }
    const key = try allocator.dupe(u8, "span_rep.secondary.weight");
    var key_owned = true;
    defer if (key_owned) allocator.free(key);
    const ref_name = try allocator.dupe(u8, key);
    var ref_name_owned = true;
    defer if (ref_name_owned) allocator.free(ref_name);
    const ref_source_name = try allocator.dupe(u8, key);
    var ref_source_name_owned = true;
    defer if (ref_source_name_owned) allocator.free(ref_source_name);
    const tensor_ref = tensor_store_mod.LazyTensorRef{
        .name = ref_name,
        .source_name = ref_source_name,
        .quantized = true,
    };
    try lazy_weights.put(allocator, key, .{ .tensor_ref = tensor_ref });
    key_owned = false;
    ref_name_owned = false;
    ref_source_name_owned = false;

    var scope = metal_runtime.MetalJitRouteScope.none();
    includeMetalJitLinearWeightFormats(
        &scope,
        &lazy_weights,
        secondary.tensorStore(),
        true,
        .{ .gliner = .{} },
        false,
    );

    try std.testing.expect(scope.conformance_complete);
    try std.testing.expectEqual(@as(usize, 1), secondary.load_calls);
    try std.testing.expectEqual([2]usize{ 3072, 768 }, scope.observedShapes(.q4_k)[0]);
    try std.testing.expectEqual(@as(usize, 1), metal_runtime.metalJitProductionRouteCount(scope));
}

fn isGptEmbeddingTableKey(key: []const u8) bool {
    return std.mem.eql(u8, key, "model.embed_tokens.weight") or
        std.mem.eql(u8, key, "model.per_layer_input.per_layer_token_embd.weight");
}

fn shouldKeepResidentGptEmbeddingQuantizedOnly(
    config: gpt_mod.Config,
    tensor_type: gguf_mod.tensor_types.TensorType,
) bool {
    return switch (config.family) {
        .gemma => isCudaResidentEmbeddingQuantType(tensor_type),
        .llama, .mistral, .qwen2, .bitnet => std.meta.eql(tensor_type, gguf_mod.tensor_types.TensorType{ .known = .Q8_0 }),
        else => false,
    };
}

fn isCudaResidentEmbeddingQuantType(tensor_type: gguf_mod.tensor_types.TensorType) bool {
    return std.meta.eql(tensor_type, gguf_mod.tensor_types.TensorType{ .known = .Q8_0 }) or
        std.meta.eql(tensor_type, gguf_mod.tensor_types.TensorType{ .known = .Q4_0 }) or
        std.meta.eql(tensor_type, gguf_mod.tensor_types.TensorType{ .known = .Q4_K }) or
        std.meta.eql(tensor_type, gguf_mod.tensor_types.TensorType{ .known = .Q6_K });
}

fn shouldKeepResidentGptWeightQuantizedOnly(
    config: gpt_mod.Config,
    key: []const u8,
    tensor_type: ?gguf_mod.tensor_types.TensorType,
) bool {
    return switch (config.family) {
        .llama, .mistral, .qwen2, .gemma, .bitnet => blk: {
            if (isGptEmbeddingTableKey(key)) {
                const tt = tensor_type orelse break :blk false;
                break :blk shouldKeepResidentGptEmbeddingQuantizedOnly(config, tt);
            }
            if (std.mem.eql(u8, key, "lm_head.weight")) break :blk true;
            if (std.mem.indexOf(u8, key, ".block_sparse_moe.experts.") != null) break :blk false;
            if (std.mem.eql(u8, key, "model.per_layer_input.per_layer_model_proj.weight")) break :blk true;
            break :blk std.mem.endsWith(u8, key, ".self_attn.q_proj.weight") or
                std.mem.endsWith(u8, key, ".self_attn.k_proj.weight") or
                std.mem.endsWith(u8, key, ".self_attn.v_proj.weight") or
                std.mem.endsWith(u8, key, ".self_attn.o_proj.weight") or
                std.mem.endsWith(u8, key, ".mlp.gate_proj.weight") or
                std.mem.endsWith(u8, key, ".mlp.up_proj.weight") or
                std.mem.endsWith(u8, key, ".mlp.down_proj.weight") or
                std.mem.endsWith(u8, key, ".per_layer_input.inp_gate.weight") or
                std.mem.endsWith(u8, key, ".per_layer_input.proj.weight") or
                std.mem.endsWith(u8, key, ".block_sparse_moe.gate.weight");
        },
        else => false,
    };
}

fn defaultResidentExpertsPerLayer(arch_config: ArchConfig) usize {
    return switch (arch_config) {
        .gpt => |cfg| blk: {
            if (!cfg.usesMoe() or cfg.num_local_experts <= 0) break :blk 0;
            const num_experts: usize = @intCast(cfg.num_local_experts);
            const top_k = @max(@as(usize, 1), @as(usize, @intCast(cfg.num_experts_per_tok)));
            // For large expert counts (e.g. 128), keep more experts resident
            // to reduce lazy loading overhead with high top_k.
            const multiplier: usize = if (num_experts >= 64) 3 else 2;
            break :blk @min(num_experts, @max(@as(usize, 4), top_k * multiplier));
        },
        else => 0,
    };
}

fn shouldRetainTensorStore(store_kind: tensor_store_mod.StoreKind, lazy_weight_count: usize) bool {
    // Safetensors stores mmap weights and resident tensors may borrow those
    // buffers directly, so the store must live for the entire session.
    return store_kind == .gguf or store_kind == .safetensors or lazy_weight_count > 0;
}

fn defaultPlanContextForBackend(backend: runtime.tier.planner.BackendClass) runtime.tier.planner.PlanContext {
    const limits = runtime.tier.memory.defaultLimitsForBackend(backend);
    return .{
        .backend = backend,
        .host_budget_bytes = limits.host_limit_bytes,
        .backend_budget_bytes = limits.backend_limit_bytes,
    };
}

fn recommendedGpuHostedLazyQuantBudgetFloor(model_weight_bytes: u64, quant_mode: GpuHostedQuantExecutionMode) runtime.tier.memory.Limits {
    if (model_weight_bytes == 0 or model_weight_bytes <= gpuHostedEagerDenseMaxBytes()) return .{};

    const total_bytes: usize = @intCast(@min(model_weight_bytes, std.math.maxInt(usize)));
    // In device_native mode, quantized weights stay quantized on GPU — no 3× expansion.
    // Only need ~1.5× for quantized weights + activations + KV cache headroom.
    const host_max = if (quant_mode == .device_native) gib(6) else gib(4);
    const host_floor = clampBytes(total_bytes + mib(256), gib(2), host_max);
    const backend_multiplier: usize = if (quant_mode == .device_native) 1 else 3;
    const backend_extra: usize = if (quant_mode == .device_native) mib(512) else gib(1);
    const backend_floor = clampBytes(total_bytes * backend_multiplier + backend_extra, gib(2), gib(14));
    const combined_floor = clampBytes(host_floor + backend_floor + gib(1), gib(4), gib(18));

    return .{
        .host_limit_bytes = host_floor,
        .backend_limit_bytes = backend_floor,
        .combined_limit_bytes = combined_floor,
        .kv_limit_bytes = 0,
        .scratch_limit_bytes = 0,
    };
}

fn recommendedGpuHostedLazyQuantSharedCacheBudget(model_weight_bytes: u64, quant_mode: GpuHostedQuantExecutionMode) runtime.tier.cache.Budget {
    const floor = recommendedGpuHostedLazyQuantBudgetFloor(model_weight_bytes, quant_mode);
    return .{
        .host_limit_bytes = floor.host_limit_bytes,
        .backend_limit_bytes = floor.backend_limit_bytes,
    };
}

fn recommendedGpuHostedLargeMultimodalGemmaBudgetFloor(
    model_weight_bytes: u64,
    prefer_f32_dense_tensors: bool,
) runtime.tier.memory.Limits {
    if (model_weight_bytes == 0 or model_weight_bytes <= gpuHostedEagerDenseMaxBytes()) return .{};

    const total_bytes: usize = @intCast(@min(model_weight_bytes, std.math.maxInt(usize)));
    const promoted_bytes = if (prefer_f32_dense_tensors)
        std.math.mul(usize, total_bytes, 2) catch std.math.maxInt(usize)
    else
        total_bytes;

    const host_floor = clampBytes(total_bytes / 3 + gib(1), gib(2), gib(4));
    const backend_floor = clampBytes((promoted_bytes * 2) / 3 + gib(1), gib(8), gib(12));
    const combined_floor = clampBytes(host_floor + backend_floor + gib(1), gib(12), gib(18));

    return .{
        .host_limit_bytes = host_floor,
        .backend_limit_bytes = backend_floor,
        .combined_limit_bytes = combined_floor,
        .kv_limit_bytes = 0,
        .scratch_limit_bytes = 0,
    };
}

fn recommendedGpuHostedLargeMultimodalGemmaSharedCacheBudget(
    model_weight_bytes: u64,
    prefer_f32_dense_tensors: bool,
) runtime.tier.cache.Budget {
    const floor = recommendedGpuHostedLargeMultimodalGemmaBudgetFloor(model_weight_bytes, prefer_f32_dense_tensors);
    return .{
        .host_limit_bytes = floor.host_limit_bytes,
        .backend_limit_bytes = floor.backend_limit_bytes,
    };
}

fn ensureGpuHostedSessionAvailable(backend_type: BackendType) !void {
    return switch (backend_type) {
        .metal => ensureMetalHostedSessionAvailable(),
        else => unreachable,
    };
}

const GpuHostedStream = if (false) gpu_hosted_c.resident_stream else void;

fn openGpuHostedStream(backend_type: BackendType) !GpuHostedStream {
    return switch (backend_type) {
        .metal => openMetalHostedStream(),
        else => unreachable,
    };
}

fn ensureMetalHostedSessionAvailable() !void {
    if (comptime !build_options.enable_metal) return error.MetalNotEnabled;
    if (!metal_runtime.metalDeviceAvailable()) return error.MetalDeviceUnavailable;
}

test "gpu-hosted Metal availability gate agrees with linked runtime probe" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (metal_runtime.termite_metal_device_available() == 0) return error.SkipZigTest;
    try ensureMetalHostedSessionAvailable();
}

fn openMetalHostedStream() !GpuHostedStream {
    try ensureMetalHostedSessionAvailable();
    if (comptime false) {
        return std.mem.zeroes(GpuHostedStream);
    }
    return {};
}

const GpuHostedBudgetPolicy = struct {
    budget_floor: runtime.tier.memory.Limits,
    shared_cache_floor: runtime.tier.cache.Budget,
    plan_context: runtime.tier.planner.PlanContext,
    prefer_f32_dense_tensors: bool,
};

fn gpuHostedBudgetPolicy(
    backend_type: BackendType,
    model_weight_bytes: u64,
    manifest: manifest_mod.ModelManifest,
    arch_config: ArchConfig,
    quant_mode: GpuHostedQuantExecutionMode,
) GpuHostedBudgetPolicy {
    return switch (backend_type) {
        .metal => metalHostedBudgetPolicy(model_weight_bytes, manifest, arch_config, quant_mode),
        .cuda => sharedGpuHostedBudgetPolicy(model_weight_bytes, manifest, arch_config, quant_mode),
        else => runtimeGpuHostedBudgetPolicyUnsupported(backend_type),
    };
}

fn runtimeGpuHostedBudgetPolicyUnsupported(backend_type: BackendType) GpuHostedBudgetPolicy {
    std.debug.assert(!backend_type.usesGpuHostedSession());
    return .{
        .budget_floor = runtime.tier.memory.Limits{},
        .shared_cache_floor = runtime.tier.cache.Budget{},
        .plan_context = defaultPlanContextForBackend(.cpu),
        .prefer_f32_dense_tensors = false,
    };
}

fn metalHostedBudgetPolicy(
    model_weight_bytes: u64,
    manifest: manifest_mod.ModelManifest,
    arch_config: ArchConfig,
    quant_mode: GpuHostedQuantExecutionMode,
) GpuHostedBudgetPolicy {
    return sharedGpuHostedBudgetPolicy(model_weight_bytes, manifest, arch_config, quant_mode);
}

fn sharedGpuHostedBudgetPolicy(
    model_weight_bytes: u64,
    manifest: manifest_mod.ModelManifest,
    arch_config: ArchConfig,
    quant_mode: GpuHostedQuantExecutionMode,
) GpuHostedBudgetPolicy {
    const prefer_f32_dense_tensors = shouldPreferGpuHostedF32DenseTensors(arch_config);
    const lazy_quant_budget_floor = if (shouldUseLargeGpuHostedLazyQuantBudgets(model_weight_bytes, manifest, quant_mode, false))
        recommendedGpuHostedLazyQuantBudgetFloor(model_weight_bytes, quant_mode)
    else
        runtime.tier.memory.Limits{};
    const lazy_quant_shared_cache_floor = if (shouldUseLargeGpuHostedLazyQuantBudgets(model_weight_bytes, manifest, quant_mode, false))
        recommendedGpuHostedLazyQuantSharedCacheBudget(model_weight_bytes, quant_mode)
    else
        runtime.tier.cache.Budget{};
    const gemma_budget_floor = if (shouldUseLargeGpuHostedMultimodalGemmaBudgets(model_weight_bytes, arch_config))
        recommendedGpuHostedLargeMultimodalGemmaBudgetFloor(model_weight_bytes, prefer_f32_dense_tensors)
    else
        runtime.tier.memory.Limits{};
    const gemma_shared_cache_floor = if (shouldUseLargeGpuHostedMultimodalGemmaBudgets(model_weight_bytes, arch_config))
        recommendedGpuHostedLargeMultimodalGemmaSharedCacheBudget(model_weight_bytes, prefer_f32_dense_tensors)
    else
        runtime.tier.cache.Budget{};
    const budget_floor = widenLimits(lazy_quant_budget_floor, gemma_budget_floor);
    const shared_cache_floor = runtime.tier.cache.Budget{
        .host_limit_bytes = @max(lazy_quant_shared_cache_floor.host_limit_bytes, gemma_shared_cache_floor.host_limit_bytes),
        .backend_limit_bytes = @max(lazy_quant_shared_cache_floor.backend_limit_bytes, gemma_shared_cache_floor.backend_limit_bytes),
    };
    const plan_context: runtime.tier.planner.PlanContext = blk: {
        var ctx = defaultPlanContextForBackend(.gpu);
        ctx.host_budget_bytes = @max(ctx.host_budget_bytes, budget_floor.host_limit_bytes);
        ctx.backend_budget_bytes = @max(ctx.backend_budget_bytes, budget_floor.backend_limit_bytes);
        break :blk ctx;
    };
    return .{
        .budget_floor = budget_floor,
        .shared_cache_floor = shared_cache_floor,
        .plan_context = plan_context,
        .prefer_f32_dense_tensors = prefer_f32_dense_tensors,
    };
}

pub fn widenBudgetLimitsForModelPath(
    allocator: std.mem.Allocator,
    model_path: []const u8,
    limits: runtime.tier.memory.Limits,
    backend_type: BackendType,
) !runtime.tier.memory.Limits {
    if (!backend_type.usesGpuHostedSession()) return limits;
    switch (backend_type) {
        .metal => if (!build_options.enable_metal) return limits,
        .cuda => if (!build_options.enable_cuda) return limits,
        else => return limits,
    }

    const direct_quant_enabled = directQuantEnabled();
    const quant_mode = gpuHostedQuantExecutionMode(direct_quant_enabled);

    var mf = try manifest_mod.loadFromDir(allocator, model_path);
    defer mf.deinit();

    const model_weight_bytes = estimateNativeWeightBytes(allocator, mf) catch 0;
    const arch_config = try detectArchitecture(allocator, model_path, mf);
    const policy = gpuHostedBudgetPolicy(backend_type, model_weight_bytes, mf, arch_config, quant_mode);

    return widenLimits(limits, policy.budget_floor);
}

fn shouldUseLargeGpuHostedLazyQuantBudgets(
    model_weight_bytes: u64,
    manifest: manifest_mod.ModelManifest,
    quant_mode: GpuHostedQuantExecutionMode,
    eager_dense: bool,
) bool {
    return manifest.usesGgufWeights() and
        !eager_dense and
        quant_mode == .device_native and
        model_weight_bytes > gpuHostedEagerDenseMaxBytes();
}

fn shouldUseLargeGpuHostedMultimodalGemmaBudgets(
    model_weight_bytes: u64,
    arch_config: ArchConfig,
) bool {
    if (model_weight_bytes == 0 or model_weight_bytes <= gpuHostedEagerDenseMaxBytes()) return false;
    return switch (arch_config) {
        .gpt => |cfg| cfg.family == .gemma and !cfg.usesMoe() and cfg.isMultimodal(),
        else => false,
    };
}

const GpuHostedBackendInit = struct {
    allocator: std.mem.Allocator,
    resident_weight_estimate_bytes: usize,
    prefix: []const u8,
    lazy_weights: std.StringHashMapUnmanaged(gpu_hosted_store_mod.LazyWeightEntry),
    tensor_store: ?tensor_store_mod.TensorStore,
    moe_num_experts: u32,
    residency: ?runtime.moe.residency.SharedResidency,
    tier_cache: ?runtime.tier.cache.SharedCache,
    allow_direct_quant: bool,
    quant_execution_mode: GpuHostedQuantExecutionMode,
    prefer_f32_dense_tensors: bool,
    jina_lora_adapter: ?*gpu_hosted_store_mod.JinaLoraAdapter = null,
};

fn makeGpuHostedBackendData(
    backend_type: BackendType,
    init: GpuHostedBackendInit,
) BackendData {
    if (comptime !build_options.enable_metal) {
        unreachable;
    }

    const data: GpuHostedData = .{
        .allocator = init.allocator,
        .resident_weight_estimate_bytes = init.resident_weight_estimate_bytes,
        .prefix = init.prefix,
        .lazy_weights = init.lazy_weights,
        .tensor_store = init.tensor_store,
        .moe_num_experts = init.moe_num_experts,
        .residency = init.residency,
        .tier_cache = init.tier_cache,
        .allow_direct_quant = init.allow_direct_quant,
        .quant_execution_mode = init.quant_execution_mode,
        .prefer_f32_dense_tensors = init.prefer_f32_dense_tensors,
        .mirror_kv_to_manager = false,
        .jina_lora_adapter = init.jina_lora_adapter,
    };
    return switch (backend_type) {
        .metal => if (comptime build_options.enable_metal) .{ .metal = data } else unreachable,
        else => unreachable,
    };
}

fn metalHostedQuantProvider() gpu_hosted_quant_mod.Provider {
    if (comptime false) {
        return gpu_hosted_quant_mod.nullProvider();
    }
    return {};
}

fn widenGpuHostedTierCache(self: *ArchSession, budget: *runtime.tier.memory.RunBudget) void {
    if (gpuBackendData(self).tier_cache) |*tier_cache| {
        tier_cache.widenToAtLeast(.{
            .host_limit_bytes = @max(budget.limits.host_limit_bytes, self.shared_cache_budget_floor.host_limit_bytes),
            .backend_limit_bytes = @max(budget.limits.backend_limit_bytes, self.shared_cache_budget_floor.backend_limit_bytes),
        });
    }
}

fn makeGpuHostedComputeBackend(
    self: *ArchSession,
    allocator: std.mem.Allocator,
    run_budget: ?*runtime.tier.memory.RunBudget,
) !ops.ComputeBackend {
    return switch (self.backend_type) {
        .metal => makeMetalHostedComputeBackend(self, allocator, run_budget),
        else => unreachable,
    };
}

fn makeMetalHostedComputeBackend(
    self: *ArchSession,
    allocator: std.mem.Allocator,
    run_budget: ?*runtime.tier.memory.RunBudget,
) !ops.ComputeBackend {
    if (!build_options.enable_metal) return error.MetalNotEnabled;
    const compute = try allocator.create(MetalCompute);
    errdefer allocator.destroy(compute);
    compute.* = if (self.io) |io_handle|
        try MetalCompute.initWithIoAndKernelJitScopeAndLoadContext(
            allocator,
            gpuBackendData(self),
            run_budget,
            io_handle,
            self.kernel_jit_config,
            self.metal_jit_scope,
            self.kernel_jit_load_context,
        )
    else
        try MetalCompute.initWithKernelJitScopeAndLoadContext(
            allocator,
            gpuBackendData(self),
            run_budget,
            self.kernel_jit_config,
            self.metal_jit_scope,
            self.kernel_jit_load_context,
        );
    return compute.computeBackend();
}

fn initGpuHostedPrefetch(self: *ArchSession) !void {
    const gpu_data = gpuBackendData(self);
    switch (self.backend_type) {
        .metal => if (comptime build_options.enable_metal) {
            metal_compute_mod.initPrefetchQueue(gpu_data, self.allocator);
        } else return error.MetalNotEnabled,
        else => return error.InvalidBackendForGpuHosted,
    }
    if (gpu_data.lazy_weights.count() > 0 and !disablePrefetchWorkerDebug()) {
        switch (self.backend_type) {
            .metal => if (comptime build_options.enable_metal) {
                try metal_compute_mod.startPrefetchWorker(gpu_data);
            } else return error.MetalNotEnabled,
            else => return error.InvalidBackendForGpuHosted,
        }
    }
}

fn widenLimits(base: runtime.tier.memory.Limits, floor: runtime.tier.memory.Limits) runtime.tier.memory.Limits {
    return .{
        .host_limit_bytes = @max(base.host_limit_bytes, floor.host_limit_bytes),
        .backend_limit_bytes = @max(base.backend_limit_bytes, floor.backend_limit_bytes),
        .combined_limit_bytes = @max(base.combined_limit_bytes, floor.combined_limit_bytes),
        .kv_limit_bytes = @max(base.kv_limit_bytes, floor.kv_limit_bytes),
        .scratch_limit_bytes = @max(base.scratch_limit_bytes, floor.scratch_limit_bytes),
    };
}

fn packedExpertByteLen(total_byte_len: usize, expert_count: u32) usize {
    if (expert_count == 0) return total_byte_len;
    const count: usize = @intCast(expert_count);
    return @max(@as(usize, 1), total_byte_len / count);
}

fn mib(value: usize) usize {
    return value * 1024 * 1024;
}

fn gib(value: usize) usize {
    return value * 1024 * 1024 * 1024;
}

fn clampBytes(value: usize, min_value: usize, max_value: usize) usize {
    return @min(@max(value, min_value), max_value);
}

fn parseMoeExpertCoord(key: []const u8) ?runtime.moe.residency.ExpertCoord {
    const prefix = "model.layers.";
    const marker = ".block_sparse_moe.experts.";
    const deepseek_marker = ".mlp.experts.";
    if (!std.mem.startsWith(u8, key, prefix)) return null;
    const marker_index = std.mem.indexOf(u8, key, marker) orelse std.mem.indexOf(u8, key, deepseek_marker) orelse return null;
    const marker_len = if (std.mem.startsWith(u8, key[marker_index..], marker)) marker.len else deepseek_marker.len;
    const layer_str = key[prefix.len..marker_index];
    const expert_start = marker_index + marker_len;
    const proj_index = std.mem.indexOfScalarPos(u8, key, expert_start, '.') orelse return null;
    const expert_str = key[expert_start..proj_index];
    const layer_index = std.fmt.parseInt(usize, layer_str, 10) catch return null;
    const expert_index = std.fmt.parseInt(u32, expert_str, 10) catch return null;
    return .{
        .layer_index = layer_index,
        .expert_index = expert_index,
    };
}

fn projectionMaskForWeightKey(key: []const u8) u8 {
    if (std.mem.endsWith(u8, key, ".w1.weight")) return 0x1;
    if (std.mem.endsWith(u8, key, ".w2.weight")) return 0x2;
    if (std.mem.endsWith(u8, key, ".w3.weight")) return 0x4;
    if (std.mem.endsWith(u8, key, ".gate_proj")) return 0x1;
    if (std.mem.endsWith(u8, key, ".down_proj")) return 0x2;
    if (std.mem.endsWith(u8, key, ".up_proj")) return 0x4;
    if (std.mem.endsWith(u8, key, ".gate_up_proj")) return 0x5;
    return 0;
}

fn refineGptConfigFromWeights(config: *gpt_mod.Config, weights: *const std.StringHashMapUnmanaged(LoadedWeight)) void {
    if (weights.get("model.embed_tokens.weight")) |embed| {
        if (embed.tensor.shape.len >= 2) {
            config.vocab_size = @intCast(embed.tensor.shape[0]);
            config.hidden_size = @intCast(embed.tensor.shape[1]);
        }
    } else if (weights.get("lm_head.weight")) |lm_head| {
        if (lm_head.tensor.shape.len >= 2) {
            config.vocab_size = @intCast(lm_head.tensor.shape[0]);
            config.hidden_size = @intCast(lm_head.tensor.shape[1]);
        }
    }

    if (config.position_encoding == .absolute) {
        if (weights.get("wpe.weight")) |wpe| {
            if (wpe.tensor.shape.len >= 2) {
                config.max_position_embeddings = @intCast(wpe.tensor.shape[0]);
            }
        }
    }
}

/// GPT-2 safetensors uses Conv1D for linear layers, storing weights as
/// [in_features, out_features] instead of the standard [out_features, in_features].
/// This function transposes those 2D weight tensors in-place so the rest of
/// the pipeline sees the standard layout.
fn transposeGpt2Conv1dWeights(
    allocator: std.mem.Allocator,
    weights: *std.StringHashMapUnmanaged(LoadedWeight),
) !void {
    var it = weights.iterator();
    while (it.next()) |entry| {
        const key = entry.key_ptr.*;
        // Only transpose 2D .weight tensors in attention and MLP layers.
        if (!isGpt2Conv1dWeight(key)) continue;
        try transposeGpt2Conv1dLoadedWeightInPlace(allocator, key, &entry.value_ptr.*);
    }
}

fn transposeGpt2Conv1dLoadedWeightInPlace(
    allocator: std.mem.Allocator,
    key: []const u8,
    weight: *LoadedWeight,
) !void {
    if (!isGpt2Conv1dWeight(key)) return;

    const w = &weight.tensor;
    if (w.shape.len != 2) return;
    if (w.dtype != .f32) return;

    const rows: usize = @intCast(w.shape[0]);
    const cols: usize = @intCast(w.shape[1]);
    if (rows == 0 or cols == 0) return;

    const src = std.mem.bytesAsSlice(f32, w.data);
    const dst = try allocator.alloc(f32, rows * cols);
    errdefer allocator.free(dst);

    for (0..rows) |r| {
        for (0..cols) |c| {
            dst[c * rows + r] = src[r * cols + c];
        }
    }

    if (w.owns_data) allocator.free(w.data);
    w.data = std.mem.sliceAsBytes(dst);
    w.owns_data = true;

    const new_shape = try allocator.alloc(i64, 2);
    new_shape[0] = @intCast(cols);
    new_shape[1] = @intCast(rows);
    if (w.owns_shape) allocator.free(w.shape);
    w.shape = new_shape;
    w.owns_shape = true;
}

const JinaLoraConfig = struct {
    rank: usize = 32,
    alpha: f32 = 32.0,

    fn scale(self: JinaLoraConfig) f32 {
        return self.alpha / @as(f32, @floatFromInt(self.rank));
    }
};

fn parseJinaLoraConfig(allocator: std.mem.Allocator, path: []const u8) !JinaLoraConfig {
    const bytes = try c_file.readFile(allocator, path);
    defer allocator.free(bytes);
    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, bytes, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidAdapterConfig;
    const obj = parsed.value.object;

    var cfg = JinaLoraConfig{};
    if (obj.get("r")) |value| {
        if (value == .integer and value.integer > 0) cfg.rank = @intCast(value.integer);
    }
    if (obj.get("lora_alpha")) |value| {
        cfg.alpha = switch (value) {
            .integer => |i| @floatFromInt(i),
            .float => |f| @floatCast(f),
            else => cfg.alpha,
        };
    }
    return cfg;
}

fn jinaRetrievalAdapterPaths(
    allocator: std.mem.Allocator,
    model_path: []const u8,
) !?struct { config: []const u8, weights: []const u8 } {
    const config_path = try std.fs.path.join(allocator, &.{ model_path, "adapters", "retrieval", "adapter_config.json" });
    errdefer allocator.free(config_path);
    const weights_path = try std.fs.path.join(allocator, &.{ model_path, "adapters", "retrieval", "adapter_model.safetensors" });
    errdefer allocator.free(weights_path);

    if (!c_file.fileExists(allocator, config_path) and !c_file.fileExists(allocator, weights_path)) {
        allocator.free(config_path);
        allocator.free(weights_path);
        return null;
    }
    if (!c_file.fileExists(allocator, config_path) or !c_file.fileExists(allocator, weights_path)) {
        allocator.free(config_path);
        allocator.free(weights_path);
        return error.IncompleteJinaV5Adapter;
    }

    return .{ .config = config_path, .weights = weights_path };
}

fn applyJinaV5RetrievalAdapterIfPresent(
    allocator: std.mem.Allocator,
    model_path: []const u8,
    mf: manifest_mod.ModelManifest,
    resident_weights: *std.StringHashMapUnmanaged(LoadedWeight),
) !void {
    if (!std.mem.eql(u8, mf.config_model_arch, "jina_embeddings_v5")) return;

    const paths = try jinaRetrievalAdapterPaths(allocator, model_path) orelse return;
    defer allocator.free(paths.config);
    defer allocator.free(paths.weights);

    const cfg = try parseJinaLoraConfig(allocator, paths.config);
    var adapter = try gpu_hosted_store_mod.JinaLoraAdapter.create(allocator, paths.weights, cfg.scale());
    defer adapter.destroy();

    var it = resident_weights.iterator();
    while (it.next()) |entry| {
        try adapter.mergeIntoLoadedWeight(entry.key_ptr.*, entry.value_ptr);
    }
}

fn transposeGpt2Conv1dResidentGpuHostedWeights(
    allocator: std.mem.Allocator,
    weights: anytype,
    stream: GpuHostedStream,
) !void {
    if (comptime !false) return error.GpuHostedNotEnabled;

    const rebuilt = gpu_hosted_c.resident_map_string_to_array_new();
    errdefer _ = gpu_hosted_c.resident_map_string_to_array_free(rebuilt);

    const it = gpu_hosted_c.resident_map_string_to_array_iterator_new(weights.*);
    defer _ = gpu_hosted_c.resident_map_string_to_array_iterator_free(it);

    while (true) {
        var key: [*c]const u8 = null;
        var val = gpu_hosted_c.resident_array_new();
        if (gpu_hosted_c.resident_map_string_to_array_iterator_next(&key, &val, it) != 0) {
            _ = gpu_hosted_c.resident_array_free(val);
            break;
        }
        if (key == null) {
            _ = gpu_hosted_c.resident_array_free(val);
            break;
        }

        const name = std.mem.span(key);
        const name_z = try allocator.dupeZ(u8, name);
        defer allocator.free(name_z);

        if (isGpt2Conv1dWeight(name) and
            gpu_hosted_c.resident_array_ndim(val) == 2 and
            gpu_hosted_c.resident_array_dtype(val) == gpu_hosted_c.GPU_HOSTED_FLOAT32)
        {
            var transposed = gpu_hosted_c.resident_array_new();
            errdefer _ = gpu_hosted_c.resident_array_free(transposed);
            try gpu_hosted_mod.check(gpu_hosted_c.resident_transpose(&transposed, val, stream));
            if (gpu_hosted_c.resident_map_string_to_array_insert(rebuilt, name_z.ptr, transposed) != 0) {
                return error.GpuHostedMapInsertFailed;
            }
            _ = gpu_hosted_c.resident_array_free(transposed);
        } else {
            if (gpu_hosted_c.resident_map_string_to_array_insert(rebuilt, name_z.ptr, val) != 0) {
                return error.GpuHostedMapInsertFailed;
            }
        }
        _ = gpu_hosted_c.resident_array_free(val);
    }

    _ = gpu_hosted_c.resident_map_string_to_array_free(weights.*);
    weights.* = rebuilt;
}

fn isGpt2Conv1dWeight(key: []const u8) bool {
    // GPT-2 Conv1D weight keys:
    //   h.N.attn.c_attn.weight
    //   h.N.attn.c_proj.weight
    //   h.N.mlp.c_fc.weight
    //   h.N.mlp.c_proj.weight
    if (!std.mem.startsWith(u8, key, "h.")) return false;
    if (!std.mem.endsWith(u8, key, ".weight")) return false;
    // Must be an attention or MLP weight (not embedding, not norm).
    if (std.mem.indexOf(u8, key, ".attn.") != null) return true;
    if (std.mem.indexOf(u8, key, ".mlp.") != null) return true;
    return false;
}

test "shouldRetainTensorStore keeps safetensors-backed resident weights alive" {
    try std.testing.expect(shouldRetainTensorStore(.safetensors, 0));
    try std.testing.expect(shouldRetainTensorStore(.gguf, 0));
    try std.testing.expect(shouldRetainTensorStore(.gguf, 2));
    try std.testing.expect(shouldRetainTensorStore(.safetensors, 3));
}

test "overlay gpt structural config carries gguf vocab size" {
    var target = gpt_mod.Config{
        .family = .gemma,
        .hidden_size = 2560,
        .num_hidden_layers = 28,
        .num_attention_heads = 8,
        .num_key_value_heads = 4,
        .intermediate_size = 10240,
        .vocab_size = 50257,
    };
    const source = gpt_mod.Config{
        .family = .gemma,
        .hidden_size = 2560,
        .num_hidden_layers = 28,
        .num_attention_heads = 8,
        .num_key_value_heads = 4,
        .intermediate_size = 10240,
        .vocab_size = 262208,
    };

    overlayGptStructuralConfig(&target, source);

    try std.testing.expectEqual(@as(u32, 262208), target.vocab_size);
}

test "detectModelType reads top-level model_type instead of nested model_type" {
    const json =
        \\{
        \\  "audio_config": { "model_type": "clap_audio_model" },
        \\  "text_config": { "model_type": "clap_text_model" },
        \\  "model_type": "clap"
        \\}
    ;
    const detected = try detectModelType(std.testing.allocator, json);
    defer if (detected) |value| std.testing.allocator.free(value);
    try std.testing.expect(detected != null);
    try std.testing.expectEqualStrings("clap", detected.?);
}

/// Extract the top-level "model_type" string from config.json bytes.
fn detectModelType(allocator: std.mem.Allocator, json: []const u8) !?[]const u8 {
    const parsed = std.json.parseFromSlice(std.json.Value, allocator, json, .{}) catch return null;
    defer parsed.deinit();

    if (parsed.value != .object) return null;
    const value = parsed.value.object.get("model_type") orelse return null;
    if (value != .string) return null;
    return try allocator.dupe(u8, value.string);
}

fn makeBertConfig(mf: manifest_mod.ModelManifest) bert.Config {
    return .{
        .model_type = mf.bert_model_type,
        .vocab_size = mf.bert_vocab_size,
        .hidden_size = mf.hidden_size,
        .num_hidden_layers = mf.num_hidden_layers,
        .num_attention_heads = mf.num_attention_heads,
        .intermediate_size = mf.intermediate_size,
        .max_position_embeddings = mf.max_position_embeddings,
        .type_vocab_size = mf.bert_type_vocab_size,
        .layer_norm_eps = mf.bert_layer_norm_eps,
        .num_labels = mf.num_labels,
        .pad_token_id = mf.bert_pad_token_id,
        .position_id_mode = if (mf.bert_model_type == .roberta) .roberta_padding else .absolute,
    };
}

test "makeBertConfig carries num_labels from manifest" {
    const allocator = std.testing.allocator;
    var mf = manifest_mod.ModelManifest{
        .allocator = allocator,
        .bert_model_type = .roberta,
        .hidden_size = 384,
        .num_hidden_layers = 6,
        .num_attention_heads = 12,
        .intermediate_size = 1536,
        .max_position_embeddings = 256,
        .bert_vocab_size = 250002,
        .bert_type_vocab_size = 1,
        .bert_layer_norm_eps = 0.00001,
        .num_labels = 3,
        .bert_pad_token_id = 1,
    };
    defer mf.deinit();

    const cfg = makeBertConfig(mf);
    try std.testing.expectEqual(@as(bert.ModelType, .roberta), cfg.model_type);
    try std.testing.expectEqual(@as(u32, 250002), cfg.vocab_size);
    try std.testing.expectEqual(@as(u32, 1), cfg.type_vocab_size);
    try std.testing.expectApproxEqAbs(@as(f32, 0.00001), cfg.layer_norm_eps, 0.0000001);
    try std.testing.expectEqual(@as(u32, 3), cfg.num_labels);
    try std.testing.expectEqual(@as(i64, 1), cfg.pad_token_id);
    try std.testing.expectEqual(@as(bert.PositionIdMode, .roberta_padding), cfg.position_id_mode);
}

test "sessionTaskForModelType maps classifier and recognizer tasks" {
    try std.testing.expectEqual(@as(SessionTask, .classifier), sessionTaskForModelType(.classifier, null));
    try std.testing.expectEqual(@as(SessionTask, .classifier), sessionTaskForModelType(.reranker, null));
    try std.testing.expectEqual(@as(SessionTask, .recognizer), sessionTaskForModelType(.recognizer, null));
    try std.testing.expectEqual(@as(SessionTask, .generic), sessionTaskForModelType(.embedder, null));
    try std.testing.expectEqual(@as(SessionTask, .generic), sessionTaskForModelType(.reranker, .generic));
}

test "detectArchitecture recognizes generic deberta classifier configs" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "config.json",
        .data =
        \\{"model_type":"deberta-v3","hidden_size":768,"num_hidden_layers":12,"num_attention_heads":12,"intermediate_size":3072,"num_labels":3}
        ,
    });

    const model_dir = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer allocator.free(model_dir);

    var mf = manifest_mod.ModelManifest{
        .allocator = allocator,
        .model_type = .classifier,
    };
    defer mf.deinit();

    const arch = try detectArchitecture(allocator, model_dir, mf);
    switch (arch) {
        .deberta => |cfg| try std.testing.expectEqual(@as(u32, 3), cfg.num_labels),
        else => return error.TestUnexpectedResult,
    }
}

test "detectArchitecture treats split gliner bundle encoder config as gliner" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "config.json",
        .data =
        \\{"model_type":"deberta-v2","hidden_size":768,"num_hidden_layers":12,"num_attention_heads":12,"intermediate_size":3072,"vocab_size":128011,"position_buckets":256}
        ,
    });
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "added_tokens.json",
        .data = "{\"[C]\":51,\"[E]\":52,\"[R]\":53}",
    });

    const model_dir = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer allocator.free(model_dir);

    var mf = manifest_mod.ModelManifest{
        .allocator = allocator,
        .model_type = .recognizer,
        .gliner_model_type = try allocator.dupe(u8, "gliner2"),
    };
    defer mf.deinit();

    const arch = try detectArchitecture(allocator, model_dir, mf);
    switch (arch) {
        .gliner => |cfg| {
            try std.testing.expectEqual(@as(u32, 128011), cfg.vocab_size);
            try std.testing.expectEqual(@as(i64, 51), cfg.classification_token_id);
            try std.testing.expectEqual(@as(i64, 52), cfg.entity_token_id);
            try std.testing.expectEqual(@as(i64, 53), cfg.relation_token_id);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "detectArchitectureFromGguf recognizes deberta metadata" {
    const allocator = std.testing.allocator;
    var metadata = [_]gguf_mod.format.MetadataEntry{
        .{ .key = "general.architecture", .value = .{ .string = "deberta" } },
        .{ .key = "deberta.vocab_size", .value = .{ .u32 = 32000 } },
        .{ .key = "deberta.embedding_length", .value = .{ .u32 = 384 } },
        .{ .key = "deberta.block_count", .value = .{ .u32 = 6 } },
        .{ .key = "deberta.attention.head_count", .value = .{ .u32 = 6 } },
        .{ .key = "deberta.feed_forward_length", .value = .{ .u32 = 1536 } },
        .{ .key = "deberta.context_length", .value = .{ .u32 = 1024 } },
        .{ .key = "deberta.position_buckets", .value = .{ .u32 = 128 } },
        .{ .key = "deberta.label_count", .value = .{ .u32 = 7 } },
    };
    const tensors = [_]gguf_mod.writer.TensorSpec{};
    var layout = try gguf_mod.writer.buildLayout(allocator, &metadata, &tensors);
    defer layout.deinit(allocator);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "model.gguf", .data = layout.header_bytes });
    const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/model.gguf", .{tmp.sub_path});
    defer allocator.free(path);

    const arch = (try detectArchitectureFromGguf(allocator, path)).?;
    switch (arch) {
        .deberta => |cfg| {
            try std.testing.expectEqual(@as(u32, 384), cfg.hidden_size);
            try std.testing.expectEqual(@as(u32, 7), cfg.num_labels);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "detectArchitectureFromGguf recognizes bert metadata" {
    const allocator = std.testing.allocator;
    const metadata = [_]gguf_mod.format.MetadataEntry{
        .{ .key = "general.architecture", .value = .{ .string = "bert" } },
        .{ .key = "bert.family", .value = .{ .string = "roberta" } },
        .{ .key = "bert.vocab_size", .value = .{ .u32 = 32000 } },
        .{ .key = "bert.embedding_length", .value = .{ .u32 = 384 } },
        .{ .key = "bert.block_count", .value = .{ .u32 = 6 } },
        .{ .key = "bert.attention.head_count", .value = .{ .u32 = 6 } },
        .{ .key = "bert.feed_forward_length", .value = .{ .u32 = 1536 } },
        .{ .key = "bert.context_length", .value = .{ .u32 = 512 } },
        .{ .key = "bert.token_type_count", .value = .{ .u32 = 2 } },
        .{ .key = "bert.label_count", .value = .{ .u32 = 7 } },
        .{ .key = "bert.hidden_act", .value = .{ .string = "gelu" } },
    };
    var layout = try gguf_mod.writer.buildLayout(allocator, &metadata, &.{});
    defer layout.deinit(allocator);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "model.gguf", .data = layout.header_bytes });
    const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/model.gguf", .{tmp.sub_path});
    defer allocator.free(path);

    const detected = (try detectArchitectureFromGguf(allocator, path)).?;
    switch (detected) {
        .bert => |cfg| {
            try std.testing.expectEqual(bert.ModelType.roberta, cfg.model_type);
            try std.testing.expectEqualStrings("", cfg.effectivePrefix());
            try std.testing.expectEqual(@as(u32, 384), cfg.hidden_size);
            try std.testing.expectEqual(@as(u32, 7), cfg.num_labels);
        },
        else => return error.WrongArchitectureDetected,
    }
}

test "detectArchitectureFromGguf recognizes t5 metadata" {
    const allocator = std.testing.allocator;
    const metadata = [_]gguf_mod.format.MetadataEntry{
        .{ .key = "general.architecture", .value = .{ .string = "t5" } },
        .{ .key = "t5.family", .value = .{ .string = "mt5" } },
        .{ .key = "t5.embedding_length", .value = .{ .u32 = 768 } },
        .{ .key = "t5.attention.key_value_length", .value = .{ .u32 = 64 } },
        .{ .key = "t5.feed_forward_length", .value = .{ .u32 = 2048 } },
        .{ .key = "t5.attention.head_count", .value = .{ .u32 = 12 } },
        .{ .key = "t5.encoder.block_count", .value = .{ .u32 = 8 } },
        .{ .key = "t5.decoder.block_count", .value = .{ .u32 = 10 } },
        .{ .key = "t5.attention.relative_buckets", .value = .{ .u32 = 32 } },
        .{ .key = "t5.attention.relative_max_distance", .value = .{ .u32 = 128 } },
        .{ .key = "t5.vocab_size", .value = .{ .u32 = 250112 } },
        .{ .key = "t5.decoder_start_token_id", .value = .{ .i64 = 0 } },
        .{ .key = "t5.eos_token_id", .value = .{ .i64 = 1 } },
        .{ .key = "t5.pad_token_id", .value = .{ .i64 = 0 } },
        .{ .key = "t5.is_gated_act", .value = .{ .bool_ = true } },
    };
    var layout = try gguf_mod.writer.buildLayout(allocator, &metadata, &.{});
    defer layout.deinit(allocator);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "model.gguf", .data = layout.header_bytes });
    const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/model.gguf", .{tmp.sub_path});
    defer allocator.free(path);

    const detected = (try detectArchitectureFromGguf(allocator, path)).?;
    switch (detected) {
        .t5 => |cfg| {
            try std.testing.expectEqual(t5_mod.ModelType.mt5, cfg.model_type);
            try std.testing.expectEqual(@as(u32, 768), cfg.d_model);
            try std.testing.expectEqual(@as(u32, 10), cfg.effectiveDecoderLayers());
            try std.testing.expect(cfg.is_gated_act);
        },
        else => return error.WrongArchitectureDetected,
    }
}

test "detectArchitectureFromGguf recognizes whisper metadata" {
    const allocator = std.testing.allocator;
    const metadata = [_]gguf_mod.format.MetadataEntry{
        .{ .key = "general.architecture", .value = .{ .string = "whisper" } },
        .{ .key = "whisper.embedding_length", .value = .{ .u32 = 384 } },
        .{ .key = "whisper.encoder.block_count", .value = .{ .u32 = 4 } },
        .{ .key = "whisper.decoder.block_count", .value = .{ .u32 = 4 } },
        .{ .key = "whisper.encoder.attention.head_count", .value = .{ .u32 = 6 } },
        .{ .key = "whisper.decoder.attention.head_count", .value = .{ .u32 = 6 } },
        .{ .key = "whisper.encoder.feed_forward_length", .value = .{ .u32 = 1536 } },
        .{ .key = "whisper.decoder.feed_forward_length", .value = .{ .u32 = 1536 } },
        .{ .key = "whisper.num_mel_bins", .value = .{ .u32 = 80 } },
        .{ .key = "whisper.vocab_size", .value = .{ .u32 = 51865 } },
        .{ .key = "whisper.encoder.context_length", .value = .{ .u32 = 1500 } },
        .{ .key = "whisper.decoder.context_length", .value = .{ .u32 = 448 } },
        .{ .key = "whisper.scale_embedding", .value = .{ .bool_ = false } },
        .{ .key = "whisper.bos_token_id", .value = .{ .i64 = 50257 } },
        .{ .key = "whisper.eos_token_id", .value = .{ .i64 = 50257 } },
        .{ .key = "whisper.pad_token_id", .value = .{ .i64 = 50257 } },
        .{ .key = "whisper.decoder_start_token_id", .value = .{ .i64 = 50258 } },
    };
    var layout = try gguf_mod.writer.buildLayout(allocator, &metadata, &.{});
    defer layout.deinit(allocator);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "model.gguf", .data = layout.header_bytes });
    const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/model.gguf", .{tmp.sub_path});
    defer allocator.free(path);

    const detected = (try detectArchitectureFromGguf(allocator, path)).?;
    switch (detected) {
        .whisper => |cfg| {
            try std.testing.expectEqual(@as(u32, 384), cfg.d_model);
            try std.testing.expectEqual(@as(u32, 80), cfg.num_mel_bins);
            try std.testing.expectEqual(@as(i32, 50258), cfg.decoder_start_token_id);
        },
        else => return error.WrongArchitectureDetected,
    }
}

test "detectArchitectureFromGguf recognizes layoutlmv3 metadata" {
    const allocator = std.testing.allocator;
    const metadata = [_]gguf_mod.format.MetadataEntry{
        .{ .key = "general.architecture", .value = .{ .string = "layoutlmv3" } },
        .{ .key = "layoutlmv3.vocab_size", .value = .{ .u32 = 30522 } },
        .{ .key = "layoutlmv3.embedding_length", .value = .{ .u32 = 384 } },
        .{ .key = "layoutlmv3.block_count", .value = .{ .u32 = 6 } },
        .{ .key = "layoutlmv3.attention.head_count", .value = .{ .u32 = 6 } },
        .{ .key = "layoutlmv3.feed_forward_length", .value = .{ .u32 = 1536 } },
        .{ .key = "layoutlmv3.context_length", .value = .{ .u32 = 512 } },
        .{ .key = "layoutlmv3.token_type_count", .value = .{ .u32 = 2 } },
        .{ .key = "layoutlmv3.max_2d_position_embeddings", .value = .{ .u32 = 1024 } },
        .{ .key = "layoutlmv3.coordinate_size", .value = .{ .u32 = 128 } },
        .{ .key = "layoutlmv3.shape_size", .value = .{ .u32 = 128 } },
        .{ .key = "layoutlmv3.input_size", .value = .{ .u32 = 224 } },
        .{ .key = "layoutlmv3.patch_size", .value = .{ .u32 = 16 } },
        .{ .key = "layoutlmv3.num_channels", .value = .{ .u32 = 3 } },
        .{ .key = "layoutlmv3.label_count", .value = .{ .u32 = 7 } },
        .{ .key = "layoutlmv3.pad_token_id", .value = .{ .i64 = 1 } },
        .{ .key = "layoutlmv3.layer_norm_epsilon", .value = .{ .f32 = 1e-5 } },
        .{ .key = "layoutlmv3.has_relative_attention_bias", .value = .{ .bool_ = true } },
        .{ .key = "layoutlmv3.has_spatial_attention_bias", .value = .{ .bool_ = true } },
        .{ .key = "layoutlmv3.rel_pos_bins", .value = .{ .u32 = 32 } },
        .{ .key = "layoutlmv3.max_rel_pos", .value = .{ .u32 = 128 } },
        .{ .key = "layoutlmv3.rel_2d_pos_bins", .value = .{ .u32 = 64 } },
        .{ .key = "layoutlmv3.max_rel_2d_pos", .value = .{ .u32 = 256 } },
        .{ .key = "layoutlmv3.visual_bbox_max_len", .value = .{ .i64 = 1000 } },
    };
    var layout = try gguf_mod.writer.buildLayout(allocator, &metadata, &.{});
    defer layout.deinit(allocator);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "model.gguf", .data = layout.header_bytes });
    const gguf_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/model.gguf", .{tmp.sub_path});
    defer allocator.free(gguf_path);

    const detected = (try detectArchitectureFromGguf(allocator, gguf_path)).?;
    switch (detected) {
        .layoutlmv3 => |cfg| {
            try std.testing.expectEqualStrings("", cfg.weight_prefix);
            try std.testing.expectEqual(@as(u32, 384), cfg.hidden_size);
            try std.testing.expectEqual(@as(u32, 7), cfg.num_labels);
            try std.testing.expect(cfg.has_relative_attention_bias);
            try std.testing.expectEqual(@as(i64, 1000), cfg.visual_bbox_max_len);
        },
        else => return error.WrongArchitectureDetected,
    }
}

test "detectArchitectureFromGguf recognizes clip metadata" {
    const allocator = std.testing.allocator;
    const metadata = [_]gguf_mod.format.MetadataEntry{
        .{ .key = "general.architecture", .value = .{ .string = "clip" } },
        .{ .key = "clip.family", .value = .{ .string = "clip" } },
        .{ .key = "clip.text.embedding_length", .value = .{ .u32 = 64 } },
        .{ .key = "clip.text.block_count", .value = .{ .u32 = 2 } },
        .{ .key = "clip.text.attention.head_count", .value = .{ .u32 = 4 } },
        .{ .key = "clip.text.feed_forward_length", .value = .{ .u32 = 128 } },
        .{ .key = "clip.text.context_length", .value = .{ .u32 = 16 } },
        .{ .key = "clip.text.vocab_size", .value = .{ .u32 = 32 } },
        .{ .key = "clip.vision.embedding_length", .value = .{ .u32 = 96 } },
        .{ .key = "clip.vision.block_count", .value = .{ .u32 = 3 } },
        .{ .key = "clip.vision.attention.head_count", .value = .{ .u32 = 6 } },
        .{ .key = "clip.vision.feed_forward_length", .value = .{ .u32 = 192 } },
        .{ .key = "clip.vision.image_size", .value = .{ .u32 = 32 } },
        .{ .key = "clip.vision.patch_size", .value = .{ .u32 = 16 } },
        .{ .key = "clip.projection_dim", .value = .{ .u32 = 48 } },
    };
    var layout = try gguf_mod.writer.buildLayout(allocator, &metadata, &.{});
    defer layout.deinit(allocator);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "model.gguf", .data = layout.header_bytes });
    const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/model.gguf", .{tmp.sub_path});
    defer allocator.free(path);

    const detected = (try detectArchitectureFromGguf(allocator, path)).?;
    switch (detected) {
        .clip => |cfg| {
            try std.testing.expectEqual(clip_mod.ModelFamily.clip, cfg.family);
            try std.testing.expectEqual(@as(u32, 64), cfg.text_hidden_size);
            try std.testing.expectEqual(@as(u32, 96), cfg.vision_hidden_size);
            try std.testing.expectEqual(@as(u32, 48), cfg.projection_dim);
        },
        else => return error.WrongArchitectureDetected,
    }
}

test "detectArchitectureFromGguf recognizes clap metadata" {
    const allocator = std.testing.allocator;
    var patch_stride = [_]gguf_mod.format.MetadataValue{ .{ .u32 = 4 }, .{ .u32 = 4 } };
    var depths = [_]gguf_mod.format.MetadataValue{ .{ .u32 = 2 }, .{ .u32 = 2 }, .{ .u32 = 6 }, .{ .u32 = 2 } };
    var heads = [_]gguf_mod.format.MetadataValue{ .{ .u32 = 4 }, .{ .u32 = 8 }, .{ .u32 = 16 }, .{ .u32 = 32 } };
    const metadata = [_]gguf_mod.format.MetadataEntry{
        .{ .key = "general.architecture", .value = .{ .string = "clap" } },
        .{ .key = "clap.projection_dim", .value = .{ .u32 = 64 } },
        .{ .key = "clap.projection_hidden_act", .value = .{ .string = "relu" } },
        .{ .key = "clap.logit_scale_init_value", .value = .{ .f32 = 14.0 } },
        .{ .key = "clap.text.vocab_size", .value = .{ .u32 = 32 } },
        .{ .key = "clap.text.embedding_length", .value = .{ .u32 = 48 } },
        .{ .key = "clap.text.block_count", .value = .{ .u32 = 2 } },
        .{ .key = "clap.text.attention.head_count", .value = .{ .u32 = 4 } },
        .{ .key = "clap.text.feed_forward_length", .value = .{ .u32 = 96 } },
        .{ .key = "clap.text.context_length", .value = .{ .u32 = 16 } },
        .{ .key = "clap.text.token_type_count", .value = .{ .u32 = 1 } },
        .{ .key = "clap.text.pad_token_id", .value = .{ .i64 = 1 } },
        .{ .key = "clap.audio.embedding_length", .value = .{ .u32 = 128 } },
        .{ .key = "clap.audio.patch_embeds_hidden_size", .value = .{ .u32 = 16 } },
        .{ .key = "clap.audio.patch_embed_input_channels", .value = .{ .u32 = 1 } },
        .{ .key = "clap.audio.patch_size", .value = .{ .u32 = 4 } },
        .{ .key = "clap.audio.patch_stride", .value = .{ .array = .{ .element_type = .u32, .values = &patch_stride } } },
        .{ .key = "clap.audio.num_mel_bins", .value = .{ .u32 = 64 } },
        .{ .key = "clap.audio.spec_size", .value = .{ .u32 = 64 } },
        .{ .key = "clap.audio.window_size", .value = .{ .u32 = 8 } },
        .{ .key = "clap.audio.depths", .value = .{ .array = .{ .element_type = .u32, .values = &depths } } },
        .{ .key = "clap.audio.attention_head_counts", .value = .{ .array = .{ .element_type = .u32, .values = &heads } } },
        .{ .key = "clap.audio.mlp_ratio", .value = .{ .f32 = 4.0 } },
        .{ .key = "clap.audio.layer_norm_epsilon", .value = .{ .f32 = 1e-5 } },
        .{ .key = "clap.audio.hidden_act", .value = .{ .string = "gelu" } },
        .{ .key = "clap.audio.qkv_bias", .value = .{ .bool_ = true } },
        .{ .key = "clap.audio.enable_fusion", .value = .{ .bool_ = false } },
        .{ .key = "clap.audio.enable_patch_fusion", .value = .{ .bool_ = false } },
        .{ .key = "clap.audio.enable_patch_layer_norm", .value = .{ .bool_ = true } },
        .{ .key = "clap.enable_fusion", .value = .{ .bool_ = false } },
    };
    var layout = try gguf_mod.writer.buildLayout(allocator, &metadata, &.{});
    defer layout.deinit(allocator);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "model.gguf", .data = layout.header_bytes });
    const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/model.gguf", .{tmp.sub_path});
    defer allocator.free(path);

    const detected = (try detectArchitectureFromGguf(allocator, path)).?;
    switch (detected) {
        .clap => |cfg| {
            try std.testing.expectEqual(@as(u32, 64), cfg.projection_dim);
            try std.testing.expectEqual(@as(u32, 48), cfg.text_config.hidden_size);
            try std.testing.expectEqual(@as(u32, 16), cfg.audio_config.patch_embeds_hidden_size);
            try std.testing.expectEqual(@as(u32, 6), cfg.audio_config.depths[2]);
        },
        else => return error.WrongArchitectureDetected,
    }
}

test "detectArchitectureFromGguf recognizes florence metadata" {
    const allocator = std.testing.allocator;
    var patch_size = [_]gguf_mod.format.MetadataValue{ .{ .u32 = 7 }, .{ .u32 = 3 }, .{ .u32 = 3 }, .{ .u32 = 3 } };
    var patch_stride = [_]gguf_mod.format.MetadataValue{ .{ .u32 = 4 }, .{ .u32 = 2 }, .{ .u32 = 2 }, .{ .u32 = 2 } };
    var patch_padding = [_]gguf_mod.format.MetadataValue{ .{ .u32 = 3 }, .{ .u32 = 1 }, .{ .u32 = 1 }, .{ .u32 = 1 } };
    var patch_prenorm = [_]gguf_mod.format.MetadataValue{ .{ .bool_ = false }, .{ .bool_ = true }, .{ .bool_ = true }, .{ .bool_ = true } };
    var dim_embed = [_]gguf_mod.format.MetadataValue{ .{ .u32 = 8 }, .{ .u32 = 16 }, .{ .u32 = 24 }, .{ .u32 = 32 } };
    var num_heads = [_]gguf_mod.format.MetadataValue{ .{ .u32 = 1 }, .{ .u32 = 2 }, .{ .u32 = 3 }, .{ .u32 = 4 } };
    var num_groups = [_]gguf_mod.format.MetadataValue{ .{ .u32 = 1 }, .{ .u32 = 2 }, .{ .u32 = 3 }, .{ .u32 = 4 } };
    var depths = [_]gguf_mod.format.MetadataValue{ .{ .u32 = 1 }, .{ .u32 = 1 }, .{ .u32 = 2 }, .{ .u32 = 1 } };
    var image_feature_source = [_]gguf_mod.format.MetadataValue{ .{ .string = "spatial_avg_pool" }, .{ .string = "last_frame" } };
    const metadata = [_]gguf_mod.format.MetadataEntry{
        .{ .key = "general.architecture", .value = .{ .string = "florence" } },
        .{ .key = "florence.text.d_model", .value = .{ .u32 = 8 } },
        .{ .key = "florence.text.encoder_layers", .value = .{ .u32 = 1 } },
        .{ .key = "florence.text.decoder_layers", .value = .{ .u32 = 1 } },
        .{ .key = "florence.text.encoder_attention_heads", .value = .{ .u32 = 2 } },
        .{ .key = "florence.text.decoder_attention_heads", .value = .{ .u32 = 2 } },
        .{ .key = "florence.text.encoder_ffn_dim", .value = .{ .u32 = 16 } },
        .{ .key = "florence.text.decoder_ffn_dim", .value = .{ .u32 = 16 } },
        .{ .key = "florence.text.vocab_size", .value = .{ .u32 = 32 } },
        .{ .key = "florence.text.max_position_embeddings", .value = .{ .u32 = 16 } },
        .{ .key = "florence.vision.image_size", .value = .{ .u32 = 32 } },
        .{ .key = "florence.vision.hidden_size", .value = .{ .u32 = 8 } },
        .{ .key = "florence.vision.patch_size", .value = .{ .array = .{ .element_type = .u32, .values = &patch_size } } },
        .{ .key = "florence.vision.patch_stride", .value = .{ .array = .{ .element_type = .u32, .values = &patch_stride } } },
        .{ .key = "florence.vision.patch_padding", .value = .{ .array = .{ .element_type = .u32, .values = &patch_padding } } },
        .{ .key = "florence.vision.patch_prenorm", .value = .{ .array = .{ .element_type = .bool_, .values = &patch_prenorm } } },
        .{ .key = "florence.vision.dim_embed", .value = .{ .array = .{ .element_type = .u32, .values = &dim_embed } } },
        .{ .key = "florence.vision.num_heads", .value = .{ .array = .{ .element_type = .u32, .values = &num_heads } } },
        .{ .key = "florence.vision.num_groups", .value = .{ .array = .{ .element_type = .u32, .values = &num_groups } } },
        .{ .key = "florence.vision.depths", .value = .{ .array = .{ .element_type = .u32, .values = &depths } } },
        .{ .key = "florence.vision.window_size", .value = .{ .u32 = 12 } },
        .{ .key = "florence.vision.image_pos_embed_max_pos", .value = .{ .u32 = 50 } },
        .{ .key = "florence.vision.visual_temporal_max_embeddings", .value = .{ .u32 = 100 } },
        .{ .key = "florence.vision.image_feature_source", .value = .{ .array = .{ .element_type = .string, .values = &image_feature_source } } },
        .{ .key = "florence.projection_dim", .value = .{ .u32 = 8 } },
        .{ .key = "florence.image_token_id", .value = .{ .i64 = 31 } },
        .{ .key = "florence.bos_token_id", .value = .{ .i64 = 2 } },
        .{ .key = "florence.eos_token_id", .value = .{ .i64 = 3 } },
        .{ .key = "florence.pad_token_id", .value = .{ .i64 = 1 } },
        .{ .key = "florence.decoder_start_token_id", .value = .{ .i64 = 2 } },
    };
    var layout = try gguf_mod.writer.buildLayout(allocator, &metadata, &.{});
    defer layout.deinit(allocator);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "model.gguf", .data = layout.header_bytes });
    const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/model.gguf", .{tmp.sub_path});
    defer allocator.free(path);

    const detected = (try detectArchitectureFromGguf(allocator, path)).?;
    switch (detected) {
        .florence => |cfg| {
            try std.testing.expectEqual(@as(u32, 8), cfg.d_model);
            try std.testing.expectEqual(@as(u32, 32), cfg.image_size);
            try std.testing.expectEqual(@as(u32, 8), cfg.projection_dim);
            try std.testing.expectEqual(@as(u32, 2), cfg.image_feature_source_count);
            try std.testing.expectEqual(florence_mod.ImageFeatureSource.last_frame, cfg.image_feature_sources[1]);
        },
        else => return error.WrongArchitectureDetected,
    }
}

test "deberta required tensors match exported names" {
    const allocator = std.testing.allocator;
    var names = std.StringHashMapUnmanaged(void){};
    defer {
        var it = names.iterator();
        while (it.next()) |entry| allocator.free(entry.key_ptr.*);
        names.deinit(allocator);
    }
    var missing = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (missing.items) |name| allocator.free(name);
        missing.deinit(allocator);
    }

    const required = [_][]const u8{
        "embeddings.word_embeddings.weight",
        "embeddings.LayerNorm.weight",
        "embeddings.LayerNorm.bias",
        "encoder.rel_embeddings.weight",
        "encoder.LayerNorm.weight",
        "encoder.LayerNorm.bias",
        "encoder.layer.0.attention.self.query_proj.weight",
        "encoder.layer.0.attention.self.query_proj.bias",
        "encoder.layer.0.attention.self.key_proj.weight",
        "encoder.layer.0.attention.self.key_proj.bias",
        "encoder.layer.0.attention.self.value_proj.weight",
        "encoder.layer.0.attention.self.value_proj.bias",
        "encoder.layer.0.attention.output.dense.weight",
        "encoder.layer.0.attention.output.dense.bias",
        "encoder.layer.0.attention.output.LayerNorm.weight",
        "encoder.layer.0.attention.output.LayerNorm.bias",
        "encoder.layer.0.intermediate.dense.weight",
        "encoder.layer.0.intermediate.dense.bias",
        "encoder.layer.0.output.dense.weight",
        "encoder.layer.0.output.dense.bias",
        "encoder.layer.0.output.LayerNorm.weight",
        "encoder.layer.0.output.LayerNorm.bias",
    };
    for (required) |name| {
        try names.put(allocator, try allocator.dupe(u8, name), {});
    }

    try collectMissingRequiredDebertaWeights(allocator, .{
        .hidden_size = 4,
        .num_hidden_layers = 1,
        .num_attention_heads = 2,
        .intermediate_size = 8,
    }, &names, &missing);
    try std.testing.expectEqual(@as(usize, 0), missing.items.len);
}

test "gliner base weight key preserves exported gguf deberta names" {
    try std.testing.expectEqualStrings("embeddings.word_embeddings.weight", glinerBaseWeightKey("encoder.embeddings.word_embeddings.weight"));
    try std.testing.expectEqualStrings("encoder.rel_embeddings.weight", glinerBaseWeightKey("encoder.rel_embeddings.weight"));
    try std.testing.expectEqualStrings("encoder.rel_embeddings.weight", glinerBaseWeightKey("encoder.encoder.rel_embeddings.weight"));
    try std.testing.expectEqualStrings("span_rep.weight", glinerBaseWeightKey("span_rep.weight"));
}

// --- Unified Session implementation ---

const native_mod = @import("../ops/native_compute.zig");
const gpu_hosted_store_mod = @import("../ops/gpu_hosted_store.zig");
const NativeData = native_mod.WeightStore;
const LazyWeightEntry = native_mod.LazyWeightEntry;

const GpuHostedData = if (false or build_options.enable_metal) gpu_hosted_store_mod.WeightStore else void;

/// PJRT backend data: weights are stored and served via the native backend,
/// while the PJRT client is used for compiled partition execution.
const PjrtData = struct {
    /// Native weight store — PJRT uses the native backend for data
    /// conversion (host CTs ↔ PJRT buffers).
    native: NativeData,
    /// Initialized PJRT client. null when the plugin was not found and we
    /// fell back to pure native CPU execution.
    client: if (build_options.enable_pjrt) ?pjrt_lib.pjrt.Client else void =
        if (build_options.enable_pjrt) null else {},
};

const CudaData = struct {
    compute: if (build_options.enable_cuda) cuda_compute_mod.CudaCompute else void,
};

const BackendData = union {
    native: NativeData,
    metal: if (build_options.enable_metal) GpuHostedData else void,
    cuda: if (build_options.enable_cuda) CudaData else void,
    pjrt: PjrtData,
};

const ArchSession = struct {
    allocator: std.mem.Allocator,
    arch_config: ArchConfig,
    task: SessionTask = .generic,
    backend_type: BackendType,
    kernel_jit_config: kernel_jit.Config = .{},
    kernel_jit_load_context: kernel_jit.LoadContext = .dynamic,
    metal_jit_scope: MetalJitRouteScope = if (build_options.enable_metal)
        metal_runtime.MetalJitRouteScope.none()
    else {},
    budget_floor: runtime.tier.memory.Limits = .{},
    shared_cache_budget_floor: runtime.tier.cache.Budget = .{},
    backend_data: BackendData,
    /// Optional Io for parallel GEMM dispatch via lib/linalg's Io variants.
    /// Set by `attachIo` (called from SessionManager when its `io` field is
    /// non-null).  Threaded through `makeComputeBackend` to NativeCompute
    /// /GpuHostedCompute/MetalCompute via their `initWithIo` constructors.
    io: ?std.Io = null,
    /// Optional graph-runtime execution strategy.  Set by
    /// `attachGraphRuntimeStrategy` (called from SessionManager when its
    /// `graph_runtime_strategy` field is non-null).  The eager `runArch`
    /// path consults this on a per-architecture basis: today only the
    /// gliner branch routes through `gliner_head_graph.forwardViaGraph`
    /// when this is set; other architectures fall through to their
    /// existing eager path.
    graph_runtime_strategy: ?graph_runtime.Strategy = null,
};

/// Attach a runtime Io to a Session created by this factory so its
/// compute backend dispatches matmul work through the caller's thread
/// pool.  Safe to call on any Session; no-op when the Session wasn't
/// produced by `createNativeSession` / `createMetalSession`.
pub fn attachIo(session: Session, io: std.Io) void {
    if (session.vtable != &arch_vtable) return;
    const arch_session: *ArchSession = @ptrCast(@alignCast(session.ptr));
    arch_session.io = io;
}

/// Attach a graph-runtime execution strategy to a Session created by this
/// factory.  Mirrors `attachIo`'s lifecycle: SessionManager calls it after
/// `loadFromDir` if its own `graph_runtime_strategy` is non-null.  Today
/// only the gliner branch consults the field (routing through the
/// CT-resident graph path when non-null); other architectures fall
/// through to their existing eager forward.  No-op when called on a
/// Session this factory didn't produce.
pub fn attachGraphRuntimeStrategy(session: Session, strategy: graph_runtime.Strategy) void {
    if (session.vtable != &arch_vtable) return;
    const arch_session: *ArchSession = @ptrCast(@alignCast(session.ptr));
    arch_session.graph_runtime_strategy = strategy;
}

pub const MetalWorkloadProfileExport = if (build_options.enable_metal) metal_runtime.WorkloadProfileExport else void;

/// Starts an explicit pre-serving/benchmark census on an architecture session.
/// Unsupported or non-Metal sessions return false; there is no backend-pointer
/// downcast outside this factory.
pub fn beginMetalWorkloadProfile(session: Session, regime: ops.WorkloadRegime) !bool {
    if (comptime !build_options.enable_metal) return false;
    if (session.vtable != &arch_vtable) return false;
    const arch_session: *ArchSession = @ptrCast(@alignCast(session.ptr));
    if (arch_session.backend_type != .metal) return false;
    const provider = gpuBackendData(arch_session).shared_metal_native_provider orelse
        return error.MetalWorkloadProfileUnavailable;
    try provider.workloadProfileBegin(@enumFromInt(@intFromEnum(regime)));
    return true;
}

/// Stops capture at a command-buffer boundary, optionally calibrates every
/// eligible first-slice signature, and returns an owned backend-neutral export.
pub fn endMetalWorkloadProfile(
    session: Session,
    allocator: std.mem.Allocator,
    calibrate_first_slice: bool,
) !?MetalWorkloadProfileExport {
    if (comptime !build_options.enable_metal) return null;
    if (session.vtable != &arch_vtable) return null;
    const arch_session: *ArchSession = @ptrCast(@alignCast(session.ptr));
    if (arch_session.backend_type != .metal) return null;
    const provider = gpuBackendData(arch_session).shared_metal_native_provider orelse
        return error.MetalWorkloadProfileUnavailable;
    try provider.workloadProfileEnd();
    var snapshot = try provider.workloadProfileSnapshot();
    var tuning: metal_runtime.MetalWorkloadTuningSummary = .{};
    if (calibrate_first_slice and snapshot.eligibleForSelection()) {
        if (!metal_runtime.metalJitHasPersistentQualificationCache(provider)) {
            return error.MetalJitQualificationCacheUnavailable;
        }
        try metal_runtime.calibrateFirstMetalTuningSlice(allocator, provider.raw_decode_runtime, &snapshot);
        tuning = try metal_runtime.tuneFirstMetalWorkloadSlice(provider, &snapshot);
    }
    return try metal_runtime.workloadProfileExportWithTuningAlloc(allocator, snapshot, tuning, &provider.jit_scope);
}

test "attachIo reaches native compute backend" {
    const allocator = std.testing.allocator;
    var arch_session = ArchSession{
        .allocator = allocator,
        .arch_config = .{ .gpt = .{
            .hidden_size = 4,
            .num_hidden_layers = 1,
            .num_attention_heads = 1,
            .intermediate_size = 8,
            .vocab_size = 16,
        } },
        .backend_type = .native,
        .backend_data = .{ .native = .{
            .allocator = allocator,
            .resident_weights = .{},
            .lazy_weights = .{},
        } },
    };
    const session = Session{
        .ptr = &arch_session,
        .vtable = &arch_vtable,
    };

    attachIo(session, std.testing.io);
    var cb = try makeComputeBackend(&arch_session, allocator, null);
    defer cb.deinit();
    try std.testing.expect(cb.getIo() != null);
}

fn gpuBackendData(self: *ArchSession) *GpuHostedData {
    return switch (self.backend_type) {
        .metal => if (comptime build_options.enable_metal) &self.backend_data.metal else unreachable,
        else => unreachable,
    };
}

const arch_vtable = Session.VTable{
    .run = &archRun,
    .runResident = &archRunResident,
    .inputInfo = &archInputInfo,
    .outputInfo = &archOutputInfo,
    .backend = &archBackend,
    .close = &archClose,
};

fn deinitResidentComputeBackend(owner: *anyopaque, allocator: std.mem.Allocator) void {
    const cb: *ops.ComputeBackend = @ptrCast(@alignCast(owner));
    cb.deinit();
    allocator.destroy(cb);
}

const BertRunInputs = struct {
    input_ids: []const i64,
    attention_mask: []const i64,
    token_type_ids: ?[]const i64,
    batch: usize,
    seq_len: usize,
};

fn validateI64Matrix(tensor: Tensor, expected_shape: ?[2]usize) !struct {
    values: []const i64,
    shape: [2]usize,
} {
    if (tensor.dtype != .i64) return error.InvalidInputType;
    if (tensor.shape.len != 2 or tensor.shape[0] <= 0 or tensor.shape[1] <= 0) return error.InvalidInputShape;
    const shape = [2]usize{
        std.math.cast(usize, tensor.shape[0]) orelse return error.InvalidInputShape,
        std.math.cast(usize, tensor.shape[1]) orelse return error.InvalidInputShape,
    };
    if (expected_shape) |expected| {
        if (!std.mem.eql(usize, &shape, &expected)) return error.InvalidInputShape;
    }
    const element_count = std.math.mul(usize, shape[0], shape[1]) catch return error.InvalidInputShape;
    const expected_bytes = std.math.mul(usize, element_count, @sizeOf(i64)) catch return error.InvalidInputShape;
    if (tensor.data.len != expected_bytes or !tensor.isAlignedFor(i64)) return error.InvalidInputShape;
    return .{ .values = tensor.asInt64(), .shape = shape };
}

fn parseBertRunInputs(inputs: []const Tensor) !BertRunInputs {
    if (inputs.len < 2) return error.MissingInputs;
    const ids = try validateI64Matrix(inputs[0], null);
    const mask = try validateI64Matrix(inputs[1], ids.shape);
    const token_type_ids = if (inputs.len > 2)
        (try validateI64Matrix(inputs[2], ids.shape)).values
    else
        null;
    return .{
        .input_ids = ids.values,
        .attention_mask = mask.values,
        .token_type_ids = token_type_ids,
        .batch = ids.shape[0],
        .seq_len = ids.shape[1],
    };
}

test "BERT session inputs require matching aligned i64 matrices" {
    const allocator = std.testing.allocator;
    const shape = [_]i64{ 1, 2 };
    var ids = try Tensor.initInt64(allocator, "input_ids", &shape, &.{ 7, 8 });
    defer ids.deinit();
    var mask = try Tensor.initInt64(allocator, "attention_mask", &shape, &.{ 1, 1 });
    defer mask.deinit();
    const parsed = try parseBertRunInputs(&.{ ids, mask });
    try std.testing.expectEqual(@as(usize, 1), parsed.batch);
    try std.testing.expectEqual(@as(usize, 2), parsed.seq_len);

    var wrong_type = try Tensor.initFloat32(allocator, "attention_mask", &shape, &.{ 1, 1 });
    defer wrong_type.deinit();
    try std.testing.expectError(error.InvalidInputType, parseBertRunInputs(&.{ ids, wrong_type }));

    const wrong_shape = [_]i64{ 2, 1 };
    var mismatched = try Tensor.initInt64(allocator, "attention_mask", &wrong_shape, &.{ 1, 1 });
    defer mismatched.deinit();
    try std.testing.expectError(error.InvalidInputShape, parseBertRunInputs(&.{ ids, mismatched }));
}

test "BERT architecture regression declarations compile" {
    std.testing.refAllDecls(bert_arch);
}

fn archRunResident(ptr: *anyopaque, inputs: []const Tensor, allocator: std.mem.Allocator) !?ResidentOutputs {
    const self: *ArchSession = @ptrCast(@alignCast(ptr));
    const cfg = switch (self.arch_config) {
        .bert => |value| value,
        else => return null,
    };
    if (self.task == .classifier or self.task == .recognizer) return null;
    const bert_inputs = try parseBertRunInputs(inputs);

    const cb = try allocator.create(ops.ComputeBackend);
    errdefer allocator.destroy(cb);
    cb.* = try makeComputeBackend(self, allocator, null);
    errdefer cb.deinit();

    const hidden = try bert_arch.forwardCt(
        cb,
        allocator,
        cfg,
        bert_inputs.input_ids,
        bert_inputs.attention_mask,
        bert_inputs.token_type_ids,
        bert_inputs.batch,
        bert_inputs.seq_len,
    );
    errdefer cb.free(hidden);
    const outputs = try allocator.alloc(ops.CT, 1);
    errdefer allocator.free(outputs);
    outputs[0] = hidden;
    return .{
        .outputs = outputs,
        .backend = cb,
        .allocator = allocator,
        .backend_owner = cb,
        .deinit_backend_owner = &deinitResidentComputeBackend,
    };
}

/// Create a ComputeBackend from an ArchSession. Used internally and by generation pipeline.
fn makeComputeBackend(
    self: *ArchSession,
    allocator: std.mem.Allocator,
    run_budget: ?*runtime.tier.memory.RunBudget,
) !ops.ComputeBackend {
    if (run_budget) |budget| {
        switch (self.backend_type) {
            .native => if (self.backend_data.native.tier_cache) |*tier_cache| {
                tier_cache.widenToAtLeast(.{
                    .host_limit_bytes = @max(budget.limits.host_limit_bytes, self.shared_cache_budget_floor.host_limit_bytes),
                    .backend_limit_bytes = @max(budget.limits.backend_limit_bytes, self.shared_cache_budget_floor.backend_limit_bytes),
                });
            },
            .metal => if (build_options.enable_metal) widenGpuHostedTierCache(self, budget),
            // PJRT: widen the native CPU host-backend tier cache if present.
            .pjrt => if (self.backend_data.pjrt.native.tier_cache) |*tier_cache| {
                tier_cache.widenToAtLeast(.{
                    .host_limit_bytes = @max(budget.limits.host_limit_bytes, self.shared_cache_budget_floor.host_limit_bytes),
                    .backend_limit_bytes = @max(budget.limits.backend_limit_bytes, self.shared_cache_budget_floor.backend_limit_bytes),
                });
            },
            .cuda => {},
            .onnx => {},
            .wasm => {},
        }
    }
    return switch (self.backend_type) {
        .native => blk: {
            const compute = try allocator.create(NativeCompute);
            compute.* = if (self.io) |io_handle|
                NativeCompute.initWithIo(allocator, &self.backend_data.native, run_budget, io_handle)
            else
                NativeCompute.init(allocator, &self.backend_data.native, run_budget);
            break :blk compute.computeBackend();
        },
        .metal => try makeGpuHostedComputeBackend(self, allocator, run_budget),
        // PJRT does not have a generic ComputeBackend VTable of its own.
        // Non-partitioned ops (weight loads, sampling, etc.) run on native CPU.
        // Compiled partitions are executed via PjrtExecutor through the
        // multi_executor pipeline; see generation.zig attachPjrtExecutors.
        .pjrt => blk: {
            const compute = try allocator.create(NativeCompute);
            compute.* = if (self.io) |io_handle|
                NativeCompute.initWithIo(allocator, &self.backend_data.pjrt.native, run_budget, io_handle)
            else
                NativeCompute.init(allocator, &self.backend_data.pjrt.native, run_budget);
            break :blk compute.computeBackend();
        },
        .cuda => if (comptime build_options.enable_cuda) blk: {
            // CudaCompute is shared session state: bind the request-local
            // budget through the scoped handle so the handle's deinit
            // unbinds it instead of the shared compute retaining a pointer
            // into a completed request.
            if (run_budget) |budget| {
                break :blk self.backend_data.cuda.compute.computeBackendWithScopedRunBudget(budget);
            }
            break :blk self.backend_data.cuda.compute.computeBackend();
        } else return error.CudaNotEnabled,
        .onnx => return error.OnnxNotSupportedHere,
        .wasm => return error.WasmNotSupportedHere,
    };
}

/// Get GPT config from a session, if it's a GPT model. Returns null for non-GPT architectures.
pub fn getGptConfig(session: Session) ?gpt_mod.Config {
    if (session.vtable != &arch_vtable) return null;
    const self: *ArchSession = @ptrCast(@alignCast(session.ptr));
    return switch (self.arch_config) {
        .gpt => |cfg| cfg,
        else => null,
    };
}

/// Load GPT architecture metadata without opening a runtime session or weight
/// backend. This is for artifact-backed whole-model runtimes that still need
/// tokenizer/config shape information but must not keep a duplicate native
/// model resident beside the compiled backend.
pub fn loadGptConfigFromModelDir(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    mf: manifest_mod.ModelManifest,
) !gpt_mod.Config {
    return switch (try detectArchitecture(allocator, model_dir, mf)) {
        .gpt => |cfg| cfg,
        else => error.InvalidModelForGeneration,
    };
}

pub fn getWeightExportSource(session: Session) ?export_source_mod.Source {
    if (session.vtable != &arch_vtable) return null;
    const self: *ArchSession = @ptrCast(@alignCast(session.ptr));
    return switch (self.backend_type) {
        .native => export_source_mod.fromNativeWeightStore(&self.backend_data.native),
        else => null,
    };
}

pub fn getCudaRuntimeStats(session: Session) ?CudaRuntimeStats {
    if (comptime !build_options.enable_cuda) return null;
    if (session.vtable != &arch_vtable) return null;
    const self: *ArchSession = @ptrCast(@alignCast(session.ptr));
    return switch (self.backend_type) {
        .cuda => self.backend_data.cuda.compute.snapshotStats(),
        else => null,
    };
}

/// Drain any buffered non-blocking CUDA op-profile events into the compute's
/// stat counters. Call this after a request finishes (before reading stats) so
/// the profiler's single host sync happens off the hot path. No-op when the
/// session is not CUDA-backed or profiling is disabled.
pub fn drainCudaProfile(session: Session) void {
    if (comptime !build_options.enable_cuda) return;
    if (session.vtable != &arch_vtable) return;
    const self: *ArchSession = @ptrCast(@alignCast(session.ptr));
    switch (self.backend_type) {
        .cuda => cuda_compute_mod.drainCudaProfile(&self.backend_data.cuda.compute),
        else => {},
    }
}

/// Shared formatter for the `cuda_prefill_profile_us:` telemetry line so the CLI
/// and the HTTP server emit byte-identical output. Formats into `buf` (768 bytes
/// is ample) and returns the written slice, WITHOUT a trailing newline.
pub fn formatCudaPrefillProfileLine(buf: []u8, stats: CudaRuntimeStats) []const u8 {
    if (comptime build_options.enable_cuda) {
        return std.fmt.bufPrint(
            buf,
            "cuda_prefill_profile_us: events={d} q4_linear={d} q4_qkv={d} q4_pair={d} q4_gated_down={d} bf16_linear={d} bf16_qkv={d} bf16_pair={d} attention={d} ple_dense={d} staging={d} norm={d} rope={d} kv_write={d} elementwise={d} embedding={d}",
            .{
                stats.prefill_profile_events,
                stats.prefill_profile_q4_linear_us,
                stats.prefill_profile_q4_qkv_us,
                stats.prefill_profile_q4_pair_us,
                stats.prefill_profile_q4_gated_down_us,
                stats.prefill_profile_bf16_linear_us,
                stats.prefill_profile_bf16_qkv_us,
                stats.prefill_profile_bf16_pair_us,
                stats.prefill_profile_attention_us,
                stats.prefill_profile_ple_dense_us,
                stats.prefill_profile_staging_us,
                stats.prefill_profile_norm_us,
                stats.prefill_profile_rope_us,
                stats.prefill_profile_kv_write_us,
                stats.prefill_profile_elementwise_us,
                stats.prefill_profile_embedding_us,
            },
        ) catch buf[0..0];
    }
    return buf[0..0];
}

/// True when either CUDA op-profile mode is enabled via env, i.e. there is a
/// profile line worth draining and emitting.
pub fn cudaOpProfileLoggingEnabled() bool {
    if (comptime !build_options.enable_cuda) return false;
    return cuda_compute_mod.cudaPrefillOpProfileEnabled() or cuda_compute_mod.cudaDecodeProfileEnabled();
}

/// Saturating per-field delta of two CUDA runtime-stat snapshots. Shared by the
/// CLI and the server so a request's profile line reflects that request alone.
pub fn cudaStatsDelta(after: CudaRuntimeStats, before: CudaRuntimeStats) CudaRuntimeStats {
    if (comptime !build_options.enable_cuda) return after;
    var delta = after;
    inline for (std.meta.fields(CudaRuntimeStats)) |field| {
        switch (@typeInfo(field.type)) {
            .int => @field(delta, field.name) = @field(after, field.name) -| @field(before, field.name),
            else => {},
        }
    }
    return delta;
}

/// Shared formatter for the `cuda_decode_profile_us:` telemetry line (see
/// formatCudaPrefillProfileLine). Returns the written slice, no trailing newline.
pub fn formatCudaDecodeProfileLine(buf: []u8, stats: CudaRuntimeStats) []const u8 {
    if (comptime build_options.enable_cuda) {
        return std.fmt.bufPrint(
            buf,
            "cuda_decode_profile_us: events={d} qkv={d} gqa_attention={d} attention_output={d} attention_norm_residual={d} ffn_gate_up={d} ffn_gated_down={d} ffn_post_norm={d} lm_head_argmax={d} graph_replay={d}",
            .{
                stats.decode_profile_events,
                stats.decode_profile_qkv_us,
                stats.decode_profile_gqa_attention_us,
                stats.decode_profile_attention_output_us,
                stats.decode_profile_attention_norm_residual_us,
                stats.decode_profile_ffn_gate_up_us,
                stats.decode_profile_ffn_gated_down_us,
                stats.decode_profile_ffn_post_norm_us,
                stats.decode_profile_lm_head_argmax_us,
                stats.decode_profile_graph_replay_us,
            },
        ) catch buf[0..0];
    }
    return buf[0..0];
}

pub const MetalExactJitDispatchStats = struct {
    q4_0_hits: u64 = 0,
    q4_k_hits: u64 = 0,

    pub fn add(self: *MetalExactJitDispatchStats, other: MetalExactJitDispatchStats) void {
        self.q4_0_hits +|= other.q4_0_hits;
        self.q4_k_hits +|= other.q4_k_hits;
    }
};

pub fn getMetalExactJitDispatchStats(session: Session) ?MetalExactJitDispatchStats {
    if (comptime !build_options.enable_metal) return null;
    if (session.vtable != &arch_vtable) return null;
    const self: *ArchSession = @ptrCast(@alignCast(session.ptr));
    if (self.backend_type != .metal) return null;
    const provider = gpuBackendData(self).shared_metal_native_provider orelse return null;
    const stats = metal_runtime.exactJitDispatchStatsSnapshot(provider.raw_decode_runtime) catch return null;
    return .{
        .q4_0_hits = stats.q4_0_hits,
        .q4_k_hits = stats.q4_k_hits,
    };
}

pub fn recommendedKvDTypeForGptConfig(config: gpt_mod.Config, backend_kind: runtime.kv.pool.BackendKind) runtime.kv.pool.KvDType {
    return switch (backend_kind) {
        .native => .f32,
        .cuda => if (shouldDefaultGemmaCudaKvDTypeToPolar4(config)) .polar4 else if (config.family == .gemma) .f32 else .f16,
        .metal => if (config.family == .gemma) metalGemmaKvDTypeOverride() orelse .f16 else .f16,
    };
}

fn shouldDefaultGemmaCudaKvDTypeToPolar4(config: gpt_mod.Config) bool {
    if (config.family != .gemma) return false;
    return config.num_kv_shared_layers > 0 or
        config.global_head_dim > 0 or
        config.ple_hidden_size > 0 or
        config.gemma4_mtp_assistant or
        config.attention_k_eq_v or
        config.rope_dim_override > 0;
}

pub fn recommendedKvDTypeForSession(session: Session, backend_kind: runtime.kv.pool.BackendKind) runtime.kv.pool.KvDType {
    if (getGptConfig(session)) |cfg| return recommendedKvDTypeForGptConfig(cfg, backend_kind);
    return switch (backend_kind) {
        .native => .f32,
        .cuda, .metal => .f16,
    };
}

test "parseMetalGemmaKvDTypeOverride only accepts staged Gemma Metal dtypes" {
    try std.testing.expectEqual(runtime.kv.pool.KvDType.f16, parseMetalGemmaKvDTypeOverride("f16").?);
    try std.testing.expectEqual(runtime.kv.pool.KvDType.f32, parseMetalGemmaKvDTypeOverride("F32").?);
    try std.testing.expect(parseMetalGemmaKvDTypeOverride("int8") == null);
}

test "recommendedKvDTypeForGptConfig keeps backend defaults without a session" {
    const gemma = gpt_mod.Config{ .family = .gemma };
    const gemma4 = gpt_mod.Config{ .family = .gemma, .num_kv_shared_layers = 20 };
    const llama = gpt_mod.Config{ .family = .llama };
    try std.testing.expectEqual(runtime.kv.pool.KvDType.f32, recommendedKvDTypeForGptConfig(gemma, .native));
    try std.testing.expectEqual(runtime.kv.pool.KvDType.f32, recommendedKvDTypeForGptConfig(gemma, .cuda));
    try std.testing.expectEqual(runtime.kv.pool.KvDType.polar4, recommendedKvDTypeForGptConfig(gemma4, .cuda));
    try std.testing.expectEqual(runtime.kv.pool.KvDType.f16, recommendedKvDTypeForGptConfig(llama, .metal));
}

pub fn getClipConfig(session: Session) ?clip_mod.Config {
    if (session.vtable != &arch_vtable) return null;
    const self: *ArchSession = @ptrCast(@alignCast(session.ptr));
    return switch (self.arch_config) {
        .clip => |cfg| cfg,
        else => null,
    };
}

pub fn getClapConfig(session: Session) ?clap_mod.Config {
    if (session.vtable != &arch_vtable) return null;
    const self: *ArchSession = @ptrCast(@alignCast(session.ptr));
    return switch (self.arch_config) {
        .clap => |cfg| cfg,
        else => null,
    };
}

pub fn getWhisperConfig(session: Session) ?whisper_mod.Config {
    if (session.vtable != &arch_vtable) return null;
    const self: *ArchSession = @ptrCast(@alignCast(session.ptr));
    return switch (self.arch_config) {
        .whisper => |cfg| cfg,
        else => null,
    };
}

pub fn getFlorenceConfig(session: Session) ?florence_mod.Config {
    if (session.vtable != &arch_vtable) return null;
    const self: *ArchSession = @ptrCast(@alignCast(session.ptr));
    return switch (self.arch_config) {
        .florence => |cfg| cfg,
        else => null,
    };
}

pub fn runFlorenceEncoderResident(
    session: Session,
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    pixel_values: []const f32,
    batch: usize,
    prompt_input_ids: []const i64,
    prompt_seq_len: usize,
) !?florence_arch.EncoderForwardTensorResult {
    if (session.vtable != &arch_vtable) return null;
    const self: *ArchSession = @ptrCast(@alignCast(session.ptr));
    return switch (self.arch_config) {
        .florence => |cfg| try florence_arch.encoderForwardTensor(
            cb,
            allocator,
            cfg,
            pixel_values,
            batch,
            prompt_input_ids,
            prompt_seq_len,
        ),
        else => null,
    };
}

pub fn runFlorenceDecoderResident(
    session: Session,
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    decoder_input_ids: []const i64,
    encoder_hidden: ops.CT,
    encoder_mask: []const i64,
    batch: usize,
    dec_seq: usize,
    enc_seq: usize,
) !?ops.CT {
    if (session.vtable != &arch_vtable) return null;
    const self: *ArchSession = @ptrCast(@alignCast(session.ptr));
    return switch (self.arch_config) {
        .florence => |cfg| try florence_arch.decoderForwardTensor(
            cb,
            allocator,
            cfg,
            decoder_input_ids,
            encoder_hidden,
            encoder_mask,
            batch,
            dec_seq,
            enc_seq,
        ),
        else => null,
    };
}

/// Get a ComputeBackend from an architecture session.
pub fn getComputeBackend(session: Session, allocator: std.mem.Allocator) !ops.ComputeBackend {
    if (session.vtable != &arch_vtable) return error.NotArchSession;
    const self: *ArchSession = @ptrCast(@alignCast(session.ptr));
    var cb = try makeComputeBackend(self, allocator, null);
    errdefer cb.deinit();
    try cb.beginRequest();
    return cb;
}

pub fn replaceBlasResidentWeight(session: Session, name: []const u8, weight: LoadedWeight) !void {
    if (session.vtable != &arch_vtable) return error.NotArchSession;
    const self: *ArchSession = @ptrCast(@alignCast(session.ptr));
    if (self.backend_type != .native) return error.NotNativeSession;

    if (self.backend_data.native.resident_weights.getPtr(name)) |slot| {
        var old = slot.*;
        old.deinit();
        slot.* = weight;
        return;
    }

    try self.backend_data.native.resident_weights.put(self.allocator, try self.allocator.dupe(u8, name), weight);
}

pub fn getComputeBackendWithBudget(
    session: Session,
    allocator: std.mem.Allocator,
    run_budget: *runtime.tier.memory.RunBudget,
) !ops.ComputeBackend {
    if (session.vtable != &arch_vtable) return error.NotArchSession;
    const self: *ArchSession = @ptrCast(@alignCast(session.ptr));
    var cb = try makeComputeBackend(self, allocator, run_budget);
    errdefer cb.deinit();
    // The CUDA implementation is shared by the loaded session. Establish an
    // unconditional request boundary here, before prompt-cache selection can
    // bypass KV-hook provisioning, so no graph or pinned-address ABI survives
    // from the preceding request.
    try cb.beginRequest();
    return cb;
}

pub fn getGenericEncoderArchConfig(session: Session) !GenericEncoderArchConfig {
    if (session.vtable != &arch_vtable) return error.NotArchSession;
    const self: *ArchSession = @ptrCast(@alignCast(session.ptr));
    return switch (self.arch_config) {
        .bert => |cfg| .{ .bert = cfg },
        .deberta => |cfg| .{ .deberta = cfg },
        .gliner => |cfg| .{ .deberta = cfg },
        else => error.UnsupportedArchitecture,
    };
}

pub fn widenBudgetLimitsForSession(
    session: Session,
    limits: runtime.tier.memory.Limits,
) runtime.tier.memory.Limits {
    if (session.vtable != &arch_vtable) return limits;
    const self: *ArchSession = @ptrCast(@alignCast(session.ptr));
    return widenLimits(limits, self.budget_floor);
}

pub fn memoryBudgetExceededDetail(
    session: Session,
    run_budget: *const runtime.tier.memory.RunBudget,
    buf: []u8,
) ![]const u8 {
    if (run_budget.hasLastDenial()) return run_budget.lastDenialString(buf);
    if (session.vtable != &arch_vtable) {
        return std.fmt.bufPrint(buf, "request exceeds native generation memory budget", .{});
    }
    const self: *ArchSession = @ptrCast(@alignCast(session.ptr));
    return switch (self.backend_type) {
        .native => if (self.backend_data.native.tier_cache) |*tier_cache|
            tier_cache.lastDenialString(buf)
        else
            std.fmt.bufPrint(buf, "request exceeds native generation memory budget", .{}),
        .metal => if (build_options.enable_metal) {
            if (gpuBackendData(self).tier_cache) |*tier_cache| {
                return tier_cache.lastDenialString(buf);
            }
            return std.fmt.bufPrint(buf, "request exceeds native generation memory budget", .{});
        } else std.fmt.bufPrint(buf, "request exceeds native generation memory budget", .{}),
        .pjrt => if (self.backend_data.pjrt.native.tier_cache) |*tier_cache|
            tier_cache.lastDenialString(buf)
        else
            std.fmt.bufPrint(buf, "request exceeds native generation memory budget", .{}),
        .cuda => std.fmt.bufPrint(buf, "request exceeds native generation memory budget", .{}),
        .onnx => std.fmt.bufPrint(buf, "request exceeds native generation memory budget", .{}),
        .wasm => std.fmt.bufPrint(buf, "request exceeds native generation memory budget", .{}),
    };
}

pub fn attachSharedPrefetchState(session: Session, shared_prefetch: *runtime.tier.shared.SharedPrefetchState) !void {
    if (session.vtable != &arch_vtable) return error.NotArchSession;
    const self: *ArchSession = @ptrCast(@alignCast(session.ptr));
    switch (self.backend_type) {
        .native => self.backend_data.native.shared_prefetch = shared_prefetch,
        .metal => if (build_options.enable_metal) {
            gpuBackendData(self).shared_prefetch = shared_prefetch;
        } else {
            return error.MetalNotEnabled;
        },
        .pjrt => self.backend_data.pjrt.native.shared_prefetch = shared_prefetch,
        .cuda => {},
        .onnx => {},
        .wasm => {},
    }
}

fn archRun(ptr: *anyopaque, inputs: []const Tensor, allocator: std.mem.Allocator) ![]Tensor {
    const self: *ArchSession = @ptrCast(@alignCast(ptr));
    const debug_cuda_session = platform.env.getenvBool("ANTFLY_INFERENCE_DEBUG_CUDA_SESSION");
    if (debug_cuda_session) std.log.info("arch-run: start backend={s}", .{@tagName(self.backend_type)});

    // Create the appropriate ComputeBackend
    var cb = try makeComputeBackend(self, allocator, null);
    if (debug_cuda_session) std.log.info("arch-run: compute backend made kind={s}", .{@tagName(cb.kind())});
    defer cb.deinit();

    // Dispatch based on architecture
    switch (self.arch_config) {
        .bert => |cfg| {
            const bert_inputs = try parseBertRunInputs(inputs);
            const batch = bert_inputs.batch;
            const seq_len = bert_inputs.seq_len;
            const hidden = try bert_arch.forward(
                &cb,
                allocator,
                cfg,
                bert_inputs.input_ids,
                bert_inputs.attention_mask,
                bert_inputs.token_type_ids,
                batch,
                seq_len,
            );
            defer allocator.free(hidden);

            if (self.task == .classifier) {
                const logits = try runBertSequenceClassifier(&cb, allocator, cfg, hidden, batch, seq_len);
                defer allocator.free(logits);

                const logits_shape = [_]i64{ @intCast(batch), @intCast(cfg.num_labels) };
                var output_tensor = try Tensor.initFloat32(allocator, "logits", &logits_shape, logits);
                errdefer output_tensor.deinit();

                const result = try allocator.alloc(Tensor, 1);
                result[0] = output_tensor;
                return result;
            }
            if (self.task == .recognizer) {
                const logits = try runTokenClassifier(&cb, allocator, hidden, batch, seq_len, cfg.hidden_size, cfg.num_labels);
                defer allocator.free(logits);

                const logits_shape = [_]i64{ @intCast(batch), @intCast(seq_len), @intCast(cfg.num_labels) };
                var output_tensor = try Tensor.initFloat32(allocator, "logits", &logits_shape, logits);
                errdefer output_tensor.deinit();

                const result = try allocator.alloc(Tensor, 1);
                result[0] = output_tensor;
                return result;
            }

            const H = cfg.hidden_size;
            const shape = [_]i64{ @intCast(batch), @intCast(seq_len), @intCast(H) };
            var output_tensor = try Tensor.initFloat32(allocator, "last_hidden_state", &shape, hidden);
            errdefer output_tensor.deinit();

            const result = try allocator.alloc(Tensor, 1);
            result[0] = output_tensor;
            return result;
        },
        .layoutlmv3 => |cfg| {
            if (inputs.len < 3) return error.MissingInputs;
            const input_ids_tensor = inputs[0];
            if (input_ids_tensor.shape.len != 2) return error.InvalidInputShape;
            const batch: usize = @intCast(input_ids_tensor.shape[0]);
            const seq_len: usize = @intCast(input_ids_tensor.shape[1]);
            const input_ids = input_ids_tensor.asInt64();
            const attention_mask = inputs[1].asInt64();
            const bbox = inputs[2].asInt64();
            const token_type_ids: ?[]const i64 = if (inputs.len > 3 and std.mem.eql(u8, inputs[3].name, "token_type_ids"))
                inputs[3].asInt64()
            else
                null;
            const pixel_values: ?[]const f32 = blk: {
                for (inputs[3..]) |tensor| {
                    if (tensor.dtype == .f32 and std.mem.eql(u8, tensor.name, "pixel_values")) break :blk tensor.asFloat32();
                }
                break :blk null;
            };
            const forward_out = try layoutlmv3_arch.forward(&cb, allocator, cfg, input_ids, attention_mask, token_type_ids, bbox, pixel_values, batch, seq_len);
            defer allocator.free(forward_out.hidden);
            const total_seq_len = forward_out.seq_len;

            if (self.task == .classifier) {
                const cls_embeddings = try extractClsEmbeddings(allocator, forward_out.hidden, batch, total_seq_len, cfg.hidden_size);
                defer allocator.free(cls_embeddings);
                const bert_cfg = bert.Config{
                    .model_type = .roberta,
                    .weight_prefix = "",
                    .vocab_size = cfg.vocab_size,
                    .hidden_size = cfg.hidden_size,
                    .num_hidden_layers = cfg.num_hidden_layers,
                    .num_attention_heads = cfg.num_attention_heads,
                    .intermediate_size = cfg.intermediate_size,
                    .max_position_embeddings = cfg.max_position_embeddings,
                    .type_vocab_size = cfg.type_vocab_size,
                    .num_labels = cfg.num_labels,
                };
                const logits = try runRobertaClassifier(&cb, allocator, bert_cfg, cls_embeddings, batch, cfg.hidden_size);
                defer allocator.free(logits);

                const logits_shape = [_]i64{ @intCast(batch), @intCast(cfg.num_labels) };
                var output_tensor = try Tensor.initFloat32(allocator, "logits", &logits_shape, logits);
                errdefer output_tensor.deinit();

                const result = try allocator.alloc(Tensor, 1);
                result[0] = output_tensor;
                return result;
            }
            if (self.task == .recognizer) {
                const logits = try runTokenClassifier(&cb, allocator, forward_out.hidden, batch, total_seq_len, cfg.hidden_size, cfg.num_labels);
                defer allocator.free(logits);

                const logits_shape = [_]i64{ @intCast(batch), @intCast(total_seq_len), @intCast(cfg.num_labels) };
                var output_tensor = try Tensor.initFloat32(allocator, "logits", &logits_shape, logits);
                errdefer output_tensor.deinit();

                const result = try allocator.alloc(Tensor, 1);
                result[0] = output_tensor;
                return result;
            }

            const H = cfg.hidden_size;
            const shape = [_]i64{ @intCast(batch), @intCast(total_seq_len), @intCast(H) };
            var output_tensor = try Tensor.initFloat32(allocator, "last_hidden_state", &shape, forward_out.hidden);
            errdefer output_tensor.deinit();

            const result = try allocator.alloc(Tensor, 1);
            result[0] = output_tensor;
            return result;
        },
        .deberta => |cfg| {
            if (inputs.len < 2) return error.MissingInputs;
            const input_ids_tensor = inputs[0];
            if (input_ids_tensor.shape.len != 2) return error.InvalidInputShape;
            const batch: usize = @intCast(input_ids_tensor.shape[0]);
            const seq_len: usize = @intCast(input_ids_tensor.shape[1]);
            const input_ids = input_ids_tensor.asInt64();
            const attention_mask = inputs[1].asInt64();

            if (self.task == .classifier) {
                const hidden_ct = try deberta_arch.forwardCt(&cb, allocator, cfg, input_ids, attention_mask, batch, seq_len, false);
                defer cb.free(hidden_ct);

                const logits = try runDebertaSequenceClassifierCt(&cb, allocator, cfg, hidden_ct, batch, seq_len);
                defer allocator.free(logits);

                const logits_shape = [_]i64{ @intCast(batch), @intCast(cfg.num_labels) };
                var output_tensor = try Tensor.initFloat32(allocator, "logits", &logits_shape, logits);
                errdefer output_tensor.deinit();

                const result = try allocator.alloc(Tensor, 1);
                result[0] = output_tensor;
                return result;
            }

            const hidden = try deberta_arch.forward(&cb, allocator, cfg, input_ids, attention_mask, batch, seq_len);
            defer allocator.free(hidden);

            if (self.task == .recognizer) {
                const logits = try runTokenClassifier(&cb, allocator, hidden, batch, seq_len, cfg.hidden_size, cfg.num_labels);
                defer allocator.free(logits);

                const logits_shape = [_]i64{ @intCast(batch), @intCast(seq_len), @intCast(cfg.num_labels) };
                var output_tensor = try Tensor.initFloat32(allocator, "logits", &logits_shape, logits);
                errdefer output_tensor.deinit();

                const result = try allocator.alloc(Tensor, 1);
                result[0] = output_tensor;
                return result;
            }

            const H = cfg.hidden_size;
            const shape = [_]i64{ @intCast(batch), @intCast(seq_len), @intCast(H) };
            var output_tensor = try Tensor.initFloat32(allocator, "last_hidden_state", &shape, hidden);
            errdefer output_tensor.deinit();

            const result = try allocator.alloc(Tensor, 1);
            result[0] = output_tensor;
            return result;
        },
        .t5 => |cfg| {
            if (inputs.len < 2) return error.MissingInputs;
            const input_ids_tensor = inputs[0];
            if (input_ids_tensor.shape.len != 2) return error.InvalidInputShape;
            const batch: usize = @intCast(input_ids_tensor.shape[0]);
            const seq_len: usize = @intCast(input_ids_tensor.shape[1]);
            const input_ids = input_ids_tensor.asInt64();
            const attention_mask = inputs[1].asInt64();
            // For T5, the session factory creates separate encoder and decoder sessions.
            // When called as a single session, run the encoder.
            const hidden = try t5_arch.encoderForward(&cb, allocator, cfg, input_ids, attention_mask, batch, seq_len);
            defer allocator.free(hidden);

            const d_model = cfg.d_model;
            const shape = [_]i64{ @intCast(batch), @intCast(seq_len), @intCast(d_model) };
            var output_tensor = try Tensor.initFloat32(allocator, "last_hidden_state", &shape, hidden);
            errdefer output_tensor.deinit();

            const result = try allocator.alloc(Tensor, 1);
            result[0] = output_tensor;
            return result;
        },
        .gpt => |cfg| {
            if (inputs.len < 1) return error.MissingInputs;
            const input_ids_tensor = inputs[0];
            if (input_ids_tensor.shape.len != 2) return error.InvalidInputShape;
            const batch: usize = @intCast(input_ids_tensor.shape[0]);
            const seq_len: usize = @intCast(input_ids_tensor.shape[1]);
            const input_ids = input_ids_tensor.asInt64();

            // Return hidden states (not logits) — used for embedding extraction.
            const hidden = try gpt_arch.hiddenForward(&cb, allocator, cfg, input_ids, batch, seq_len, null);
            defer allocator.free(hidden);

            const H = cfg.hidden_size;
            const shape = [_]i64{ @intCast(batch), @intCast(seq_len), @intCast(H) };
            var output_tensor = try Tensor.initFloat32(allocator, "last_hidden_state", &shape, hidden);
            errdefer output_tensor.deinit();

            const result = try allocator.alloc(Tensor, 1);
            result[0] = output_tensor;
            return result;
        },
        .whisper => |cfg| {
            if (inputs.len < 1) return error.MissingInputs;
            const first = inputs[0];

            if (first.dtype == .f32 and std.mem.eql(u8, first.name, "input_features")) {
                if (first.shape.len != 3) return error.InvalidInputShape;
                const batch: usize = @intCast(first.shape[0]);
                const n_mels: usize = @intCast(first.shape[1]);
                const time_steps: usize = @intCast(first.shape[2]);
                if (n_mels != cfg.num_mel_bins) return error.InvalidInputShape;

                const mel_shape = [_]i32{
                    @intCast(batch),
                    @intCast(n_mels),
                    @intCast(time_steps),
                };
                const mel_ct = try cb.fromFloat32Shape(first.asFloat32(), &mel_shape);
                defer cb.free(mel_ct);

                const hidden = try whisper_arch.encoderForward(&cb, allocator, cfg, mel_ct, batch, time_steps);
                defer allocator.free(hidden);

                const enc_seq = (time_steps + 2 * 1 - 3) / 2 + 1;
                const shape = [_]i64{ @intCast(batch), @intCast(enc_seq), @intCast(cfg.d_model) };
                var output_tensor = try Tensor.initFloat32(allocator, "last_hidden_state", &shape, hidden);
                errdefer output_tensor.deinit();

                const result = try allocator.alloc(Tensor, 1);
                result[0] = output_tensor;
                return result;
            }

            if (inputs.len < 2) return error.MissingInputs;
            const input_ids_tensor = first;
            if (input_ids_tensor.shape.len != 2) return error.InvalidInputShape;
            const encoder_hidden_tensor = inputs[1];
            if (encoder_hidden_tensor.dtype != .f32 or encoder_hidden_tensor.shape.len != 3) return error.InvalidInputShape;

            const batch: usize = @intCast(input_ids_tensor.shape[0]);
            const dec_seq: usize = @intCast(input_ids_tensor.shape[1]);
            const enc_batch: usize = @intCast(encoder_hidden_tensor.shape[0]);
            const enc_seq: usize = @intCast(encoder_hidden_tensor.shape[1]);
            const enc_hidden_size: usize = @intCast(encoder_hidden_tensor.shape[2]);
            if (enc_batch != batch or enc_hidden_size != cfg.d_model) return error.InvalidInputShape;

            const encoder_hidden_shape = [_]i32{
                @intCast(batch),
                @intCast(enc_seq),
                @intCast(enc_hidden_size),
            };
            const encoder_hidden = try cb.fromFloat32Shape(encoder_hidden_tensor.asFloat32(), &encoder_hidden_shape);
            defer cb.free(encoder_hidden);

            const encoder_mask = try allocator.alloc(i64, batch * enc_seq);
            defer allocator.free(encoder_mask);
            @memset(encoder_mask, 1);

            const logits = try whisper_arch.decoderForward(
                &cb,
                allocator,
                cfg,
                input_ids_tensor.asInt64(),
                encoder_hidden,
                encoder_mask,
                batch,
                dec_seq,
                enc_seq,
            );
            defer allocator.free(logits);

            const shape = [_]i64{ @intCast(batch), @intCast(dec_seq), @intCast(cfg.vocab_size) };
            var output_tensor = try Tensor.initFloat32(allocator, "logits", &shape, logits);
            errdefer output_tensor.deinit();

            const result = try allocator.alloc(Tensor, 1);
            result[0] = output_tensor;
            return result;
        },
        .florence => |cfg| {
            if (debug_cuda_session) std.log.info("arch-run: florence branch inputs={d}", .{inputs.len});
            if (inputs.len < 1) return error.MissingInputs;
            const first = inputs[0];

            if (first.dtype == .f32 and std.mem.eql(u8, first.name, "pixel_values")) {
                if (first.shape.len != 4) return error.InvalidInputShape;
                const batch: usize = @intCast(first.shape[0]);
                const channels: usize = @intCast(first.shape[1]);
                const height: usize = @intCast(first.shape[2]);
                const width: usize = @intCast(first.shape[3]);
                if (channels != 3 or height != cfg.image_size or width != cfg.image_size) return error.InvalidInputShape;

                var prompt_ids: []const i64 = &.{};
                var prompt_seq_len: usize = 0;
                if (inputs.len > 1) {
                    const prompt_tensor = inputs[1];
                    if (prompt_tensor.dtype != .i64 or prompt_tensor.shape.len != 2) return error.InvalidInputShape;
                    const prompt_batch: usize = @intCast(prompt_tensor.shape[0]);
                    if (prompt_batch != batch) return error.InvalidInputShape;
                    prompt_seq_len = @intCast(prompt_tensor.shape[1]);
                    prompt_ids = prompt_tensor.asInt64();
                }

                if (debug_cuda_session) std.log.info("arch-run: florence encoder start batch={d} prompt_seq={d}", .{ batch, prompt_seq_len });
                const encoder = try florence_arch.encoderForward(
                    &cb,
                    allocator,
                    cfg,
                    first.asFloat32(),
                    batch,
                    prompt_ids,
                    prompt_seq_len,
                );
                if (debug_cuda_session) std.log.info("arch-run: florence encoder done seq={d}", .{encoder.seq_len});
                defer allocator.free(encoder.hidden);

                const shape = [_]i64{ @intCast(batch), @intCast(encoder.seq_len), @intCast(cfg.d_model) };
                var output_tensor = try Tensor.initFloat32(allocator, "last_hidden_state", &shape, encoder.hidden);
                errdefer output_tensor.deinit();

                const result = try allocator.alloc(Tensor, 1);
                result[0] = output_tensor;
                return result;
            }

            const input_ids_tensor = first;
            if (input_ids_tensor.shape.len != 2) return error.InvalidInputShape;
            const batch: usize = @intCast(input_ids_tensor.shape[0]);
            const seq_len: usize = @intCast(input_ids_tensor.shape[1]);
            const input_ids = input_ids_tensor.asInt64();

            var encoder_hidden_tensor: ?Tensor = null;
            var encoder_mask_tensor: ?Tensor = null;
            for (inputs[1..]) |tensor| {
                if (tensor.dtype == .f32 and tensor.shape.len == 3 and
                    (std.mem.eql(u8, tensor.name, "encoder_hidden_states") or encoder_hidden_tensor == null))
                {
                    encoder_hidden_tensor = tensor;
                } else if (tensor.dtype == .i64 and tensor.shape.len == 2 and
                    (std.mem.eql(u8, tensor.name, "encoder_attention_mask") or encoder_mask_tensor == null))
                {
                    encoder_mask_tensor = tensor;
                }
            }

            if (encoder_hidden_tensor) |enc_tensor| {
                const enc_batch: usize = @intCast(enc_tensor.shape[0]);
                const enc_seq: usize = @intCast(enc_tensor.shape[1]);
                const enc_hidden_size: usize = @intCast(enc_tensor.shape[2]);
                if (enc_batch != batch or enc_hidden_size != cfg.d_model) return error.InvalidInputShape;

                const encoder_hidden_shape = [_]i32{
                    @intCast(batch),
                    @intCast(enc_seq),
                    @intCast(enc_hidden_size),
                };
                const encoder_hidden = try cb.fromFloat32Shape(enc_tensor.asFloat32(), &encoder_hidden_shape);
                defer cb.free(encoder_hidden);

                const encoder_mask = try if (encoder_mask_tensor) |mask_tensor|
                    allocator.dupe(i64, mask_tensor.asInt64())
                else blk: {
                    const mask = try allocator.alloc(i64, batch * enc_seq);
                    @memset(mask, 1);
                    break :blk mask;
                };
                defer allocator.free(encoder_mask);

                const logits = try florence_arch.decoderForward(
                    &cb,
                    allocator,
                    cfg,
                    input_ids,
                    encoder_hidden,
                    encoder_mask,
                    batch,
                    seq_len,
                    enc_seq,
                );
                defer allocator.free(logits);

                const shape = [_]i64{ @intCast(batch), @intCast(seq_len), @intCast(cfg.vocab_size) };
                var output_tensor = try Tensor.initFloat32(allocator, "logits", &shape, logits);
                errdefer output_tensor.deinit();

                const result = try allocator.alloc(Tensor, 1);
                result[0] = output_tensor;
                return result;
            }

            // When called as a text-only session, run the BART decoder in
            // hidden-state mode for embedding-style use.
            const hidden = try florence_arch.decoderHiddenForward(&cb, allocator, cfg, input_ids, batch, seq_len);
            defer allocator.free(hidden);

            const d_model = cfg.d_model;
            const shape = [_]i64{ @intCast(batch), @intCast(seq_len), @intCast(d_model) };
            var output_tensor = try Tensor.initFloat32(allocator, "last_hidden_state", &shape, hidden);
            errdefer output_tensor.deinit();

            const result = try allocator.alloc(Tensor, 1);
            result[0] = output_tensor;
            return result;
        },
        .clip => |cfg| {
            if (inputs.len < 1) return error.MissingInputs;
            const first = inputs[0];

            if (first.dtype == .f32 and std.mem.eql(u8, first.name, "pixel_values")) {
                if (first.shape.len != 4) return error.InvalidInputShape;
                const batch: usize = @intCast(first.shape[0]);
                const use_graph_runtime = graphRuntimeStrategyEnabled(self.graph_runtime_strategy);
                const embeddings = if (use_graph_runtime)
                    try clip_graph.runVisionGraph(&cb, allocator, cfg, first.asFloat32(), batch, self.graph_runtime_strategy.?)
                else
                    try clip_arch.visionEncoderForward(&cb, allocator, cfg, first.asFloat32(), batch);
                defer allocator.free(embeddings);

                const proj_dim = cfg.projection_dim;
                const shape = [_]i64{ @intCast(batch), @intCast(proj_dim) };
                var output_tensor = try Tensor.initFloat32(allocator, "image_embeds", &shape, embeddings);
                errdefer output_tensor.deinit();

                const result = try allocator.alloc(Tensor, 1);
                result[0] = output_tensor;
                return result;
            }

            if (inputs.len < 2) return error.MissingInputs;
            const input_ids_tensor = first;
            if (input_ids_tensor.shape.len != 2) return error.InvalidInputShape;
            const batch: usize = @intCast(input_ids_tensor.shape[0]);
            const seq_len: usize = @intCast(input_ids_tensor.shape[1]);
            const input_ids = input_ids_tensor.asInt64();

            const use_graph_runtime = graphRuntimeStrategyEnabled(self.graph_runtime_strategy);
            const embeddings = if (use_graph_runtime)
                try clip_graph.runTextGraph(&cb, allocator, cfg, input_ids, batch, seq_len, self.graph_runtime_strategy.?)
            else
                try clip_arch.textEncoderForward(&cb, allocator, cfg, input_ids, batch, seq_len);
            defer allocator.free(embeddings);

            const proj_dim = cfg.projection_dim;
            const shape = [_]i64{ @intCast(batch), @intCast(proj_dim) };
            var output_tensor = try Tensor.initFloat32(allocator, "text_embeds", &shape, embeddings);
            errdefer output_tensor.deinit();

            const result = try allocator.alloc(Tensor, 1);
            result[0] = output_tensor;
            return result;
        },
        .clap => |cfg| {
            if (inputs.len < 1) return error.MissingInputs;
            const first = inputs[0];
            if (first.dtype == .f32 and std.mem.eql(u8, first.name, "input_features")) {
                if (first.shape.len != 4) return error.InvalidInputShape;
                const batch: usize = @intCast(first.shape[0]);
                const channels: usize = @intCast(first.shape[1]);
                const time_frames: usize = @intCast(first.shape[2]);
                const mel_bins: usize = @intCast(first.shape[3]);
                const is_longer = if (inputs.len > 1 and inputs[1].dtype == .bool_) inputs[1].data else &[_]u8{};

                const use_graph_runtime = graphRuntimeStrategyEnabled(self.graph_runtime_strategy);
                const embeddings = if (use_graph_runtime)
                    try clap_arch.audioEncoderForwardGraphTail(&cb, allocator, cfg, first.asFloat32(), batch, channels, time_frames, mel_bins, is_longer, self.graph_runtime_strategy.?)
                else
                    try clap_arch.audioEncoderForward(&cb, allocator, cfg, first.asFloat32(), batch, channels, time_frames, mel_bins, is_longer);
                defer allocator.free(embeddings);

                const shape = [_]i64{ @intCast(batch), @intCast(cfg.projection_dim) };
                var output_tensor = try Tensor.initFloat32(allocator, "audio_embeds", &shape, embeddings);
                errdefer output_tensor.deinit();

                const result = try allocator.alloc(Tensor, 1);
                result[0] = output_tensor;
                return result;
            }
            if (inputs.len < 2) return error.MissingInputs;
            if (first.shape.len != 2) return error.InvalidInputShape;

            const batch: usize = @intCast(first.shape[0]);
            const seq_len: usize = @intCast(first.shape[1]);
            const input_ids = first.asInt64();
            const attention_mask = inputs[1].asInt64();
            const token_type_ids: ?[]const i64 = if (inputs.len > 2) inputs[2].asInt64() else null;

            const embeddings = try clap_arch.textEncoderForward(&cb, allocator, cfg, input_ids, attention_mask, token_type_ids, batch, seq_len);
            defer allocator.free(embeddings);

            const shape = [_]i64{ @intCast(batch), @intCast(cfg.projection_dim) };
            var output_tensor = try Tensor.initFloat32(allocator, "text_embeds", &shape, embeddings);
            errdefer output_tensor.deinit();

            const result = try allocator.alloc(Tensor, 1);
            result[0] = output_tensor;
            return result;
        },
        .gliner => |cfg| {
            // GLiNER2: DeBERTa encoder + span classification head
            // Inputs: input_ids, attention_mask, words_mask, span_idx
            if (inputs.len < 4) return error.MissingInputs;
            const input_ids_tensor = inputs[0];
            if (input_ids_tensor.shape.len != 2) return error.InvalidInputShape;
            const batch: usize = @intCast(input_ids_tensor.shape[0]);
            const seq_len: usize = @intCast(input_ids_tensor.shape[1]);
            const input_ids = input_ids_tensor.asInt64();
            const attention_mask = inputs[1].asInt64();
            const words_mask = inputs[2].asInt64();
            const span_idx = inputs[3].asInt64();

            const use_graph_runtime = graphRuntimeStrategyEnabled(self.graph_runtime_strategy);
            if (use_graph_runtime) {
                const head_cfg = gliner_head_graph.Config{
                    .hidden_size = cfg.hidden_size,
                    .classification_token_id = cfg.classification_token_id,
                    .entity_token_id = cfg.entity_token_id,
                    .relation_token_id = cfg.relation_token_id,
                };
                const graph_result = try gliner_head_graph.runFullGraph(
                    &cb,
                    allocator,
                    cfg,
                    head_cfg,
                    input_ids,
                    attention_mask,
                    words_mask,
                    span_idx,
                    batch,
                    seq_len,
                    self.graph_runtime_strategy.?,
                );
                defer allocator.free(graph_result.logits);

                // Output shape mirrors the eager path: [batch, num_words,
                // max_width, num_labels].  num_words / num_labels come from
                // the graph result; max_width is derived the same way the
                // eager `getSpanInfo` does (num_spans / num_words, default 8
                // when num_words == 0).
                const max_width: usize = if (graph_result.num_words == 0)
                    8
                else
                    @as(usize, @intCast(graph_result.num_spans)) / @as(usize, @intCast(graph_result.num_words));
                const shape = [_]i64{
                    @intCast(batch),
                    @intCast(graph_result.num_words),
                    @intCast(max_width),
                    @intCast(graph_result.num_labels),
                };
                var output_tensor = try Tensor.initFloat32(allocator, "logits", &shape, graph_result.logits);
                errdefer output_tensor.deinit();

                const result = try allocator.alloc(Tensor, 1);
                result[0] = output_tensor;
                return result;
            }

            // The GLiNER span head and DeBERTa encoder run encoder-shaped
            // quantized GEMMs. Bounded f16 mirrors avoid per-row quantized
            // setup while leaving other model sessions unchanged.
            cb.preferEagerQuantMirrors(true);
            const hidden = try deberta_arch.forwardCt(&cb, allocator, cfg, input_ids, attention_mask, batch, seq_len, true);
            defer cb.free(hidden);

            // Eager head path -- keeps the encoder/head boundary on the
            // backend (CT) so we skip the toFloat32 + fromFloat32Shape
            // round-trip the legacy []f32-typed APIs do. One Metal frame
            // amortizes command-buffer commits across the resident head;
            // host-reading ops such as the label GRU drain and reopen it.
            var head_frame_active = false;
            if (cb.kind() == .metal and !cb.decoderRuntimeHasActiveFrame()) {
                head_frame_active = cb.decoderRuntimeBeginFrame() catch false;
            }
            errdefer if (head_frame_active) cb.decoderRuntimeCancelFrame() catch {};
            const head_result = try gliner_head.forwardCtWithLabelMarkers(&cb, allocator, hidden, input_ids, words_mask, span_idx, batch, seq_len, cfg.hidden_size, .{
                .classification = cfg.classification_token_id,
                .entity = cfg.entity_token_id,
                .relation = cfg.relation_token_id,
            });
            defer cb.free(head_result.logits);
            if (head_frame_active) {
                try cb.decoderRuntimeSubmitAndWaitFrame();
                head_frame_active = false;
            }

            const logits_f32 = if (head_result.num_labels == 0)
                try allocator.alloc(f32, 0)
            else
                try cb.toFloat32(head_result.logits, allocator);

            const shape = [_]i64{
                @intCast(batch),
                @intCast(head_result.num_words),
                @intCast(head_result.max_width),
                @intCast(head_result.num_labels),
            };
            var output_tensor = try Tensor.initFloat32(allocator, "logits", &shape, logits_f32);
            allocator.free(logits_f32);
            errdefer output_tensor.deinit();

            const result = try allocator.alloc(Tensor, 1);
            result[0] = output_tensor;
            return result;
        },
    }
}

fn runBertSequenceClassifier(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    cfg: bert.Config,
    hidden: []const f32,
    batch: usize,
    seq_len: usize,
) ![]f32 {
    const H: usize = @intCast(cfg.hidden_size);
    const cls_embeddings = try extractClsEmbeddings(allocator, hidden, batch, seq_len, H);
    defer allocator.free(cls_embeddings);

    return switch (cfg.model_type) {
        .distilbert => runDistilBertClassifier(cb, allocator, cfg, cls_embeddings, batch, H),
        .roberta => runRobertaClassifier(cb, allocator, cfg, cls_embeddings, batch, H),
        .bert => runBertClassifier(cb, allocator, cfg, cls_embeddings, batch, H),
    };
}

fn extractClsEmbeddings(
    allocator: std.mem.Allocator,
    hidden: []const f32,
    batch: usize,
    seq_len: usize,
    hidden_size: usize,
) ![]f32 {
    const out = try allocator.alloc(f32, batch * hidden_size);
    for (0..batch) |b| {
        const src_start = b * seq_len * hidden_size;
        const dst_start = b * hidden_size;
        @memcpy(out[dst_start .. dst_start + hidden_size], hidden[src_start .. src_start + hidden_size]);
    }
    return out;
}

fn runBertClassifier(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    cfg: bert.Config,
    cls_embeddings: []const f32,
    batch: usize,
    hidden_size: usize,
) ![]f32 {
    const pooled = try maybeApplyPooler(cb, allocator, cls_embeddings, batch, hidden_size, "pooler.dense.weight", "pooler.dense.bias");
    defer allocator.free(pooled);
    return runLinearHead(allocator, cb, pooled, batch, hidden_size, cfg.num_labels, "classifier.weight", "classifier.bias");
}

fn runDebertaSequenceClassifier(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    cfg: deberta_mod.Config,
    hidden: []const f32,
    batch: usize,
    seq_len: usize,
) ![]f32 {
    const H: usize = @intCast(cfg.hidden_size);
    const cls_embeddings = try extractClsEmbeddings(allocator, hidden, batch, seq_len, H);
    defer allocator.free(cls_embeddings);

    const pooled = try maybeApplyPooler(cb, allocator, cls_embeddings, batch, H, "pooler.dense.weight", "pooler.dense.bias");
    defer allocator.free(pooled);

    return runLinearHead(allocator, cb, pooled, batch, H, cfg.num_labels, "classifier.weight", "classifier.bias");
}

fn runDebertaSequenceClassifierCt(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    cfg: deberta_mod.Config,
    hidden: ops.CT,
    batch: usize,
    seq_len: usize,
) ![]f32 {
    const H: usize = @intCast(cfg.hidden_size);
    const cls_embeddings = try extractClsEmbeddingsCt(cb, allocator, hidden, batch, seq_len, H);
    defer cb.free(cls_embeddings);

    const pooled = try maybeApplyPoolerCt(cb, allocator, cls_embeddings, batch, H, "pooler.dense.weight", "pooler.dense.bias");
    defer cb.free(pooled);

    const logits = try runLinearHeadCt(cb, pooled, batch, H, cfg.num_labels, "classifier.weight", "classifier.bias");
    defer cb.free(logits);
    return try cb.toFloat32(logits, allocator);
}

fn extractClsEmbeddingsCt(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    hidden: ops.CT,
    batch: usize,
    seq_len: usize,
    hidden_size: usize,
) !ops.CT {
    const row_ids = try allocator.alloc(u32, batch);
    defer allocator.free(row_ids);
    for (0..batch) |b| row_ids[b] = @intCast(b * seq_len);

    if (try cb.takeRows(hidden, row_ids, batch, hidden_size)) |cls| {
        return cls;
    }

    const hidden_f32 = try cb.toFloat32(hidden, allocator);
    defer allocator.free(hidden_f32);
    const cls_f32 = try extractClsEmbeddings(allocator, hidden_f32, batch, seq_len, hidden_size);
    defer allocator.free(cls_f32);
    const shape = [_]i32{ @intCast(batch), @intCast(hidden_size) };
    return try cb.fromFloat32Shape(cls_f32, &shape);
}

fn maybeApplyPoolerCt(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    cls_embeddings: ops.CT,
    batch: usize,
    hidden_size: usize,
    weight_name: []const u8,
    bias_name: []const u8,
) !ops.CT {
    const pool_w = cb.getWeight(weight_name) catch |err| switch (err) {
        error.WeightNotFound => {
            const shape = [_]i32{ @intCast(batch), @intCast(hidden_size) };
            if (try cb.cloneTensorShape(cls_embeddings, &shape)) |clone| return clone;
            const cls_f32 = try cb.toFloat32(cls_embeddings, allocator);
            defer allocator.free(cls_f32);
            return try cb.fromFloat32Shape(cls_f32, &shape);
        },
        else => return err,
    };
    defer cb.free(pool_w);
    const pool_b = try cb.getWeight(bias_name);
    defer cb.free(pool_b);

    const pooled_ct = try cb.linear(cls_embeddings, pool_w, pool_b, batch, hidden_size, hidden_size);
    defer cb.free(pooled_ct);

    return try cb.tanh_act(pooled_ct);
}

fn runLinearHeadCt(
    cb: *const ops.ComputeBackend,
    input: ops.CT,
    batch: usize,
    input_dim: usize,
    output_dim_u32: u32,
    weight_name: []const u8,
    bias_name: []const u8,
) !ops.CT {
    const output_dim: usize = @intCast(output_dim_u32);
    const weight = try cb.getWeight(weight_name);
    defer cb.free(weight);
    const bias = try cb.getWeight(bias_name);
    defer cb.free(bias);
    return try cb.linear(input, weight, bias, batch, input_dim, output_dim);
}

fn runTokenClassifier(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    hidden: []const f32,
    batch: usize,
    seq_len: usize,
    hidden_size_u32: u32,
    num_labels_u32: u32,
) ![]f32 {
    const hidden_size: usize = @intCast(hidden_size_u32);
    const rows = batch * seq_len;
    return runLinearHead(allocator, cb, hidden, rows, hidden_size, num_labels_u32, "classifier.weight", "classifier.bias");
}

fn runRobertaClassifier(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    cfg: bert.Config,
    cls_embeddings: []const f32,
    batch: usize,
    hidden_size: usize,
) ![]f32 {
    if (runTwoLayerHead(
        allocator,
        cb,
        cls_embeddings,
        batch,
        hidden_size,
        hidden_size,
        cfg.num_labels,
        "classifier.dense.weight",
        "classifier.dense.bias",
        "classifier.out_proj.weight",
        "classifier.out_proj.bias",
        .tanh,
    )) |logits| {
        return logits;
    } else |err| switch (err) {
        error.MissingHead => return runLinearHead(allocator, cb, cls_embeddings, batch, hidden_size, cfg.num_labels, "classifier.weight", "classifier.bias"),
        else => return err,
    }
}

fn runDistilBertClassifier(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    cfg: bert.Config,
    cls_embeddings: []const f32,
    batch: usize,
    hidden_size: usize,
) ![]f32 {
    return runTwoLayerHead(
        allocator,
        cb,
        cls_embeddings,
        batch,
        hidden_size,
        hidden_size,
        cfg.num_labels,
        "pre_classifier.weight",
        "pre_classifier.bias",
        "classifier.weight",
        "classifier.bias",
        .relu,
    );
}

const HeadActivation = enum {
    relu,
    tanh,
};

fn runTwoLayerHead(
    allocator: std.mem.Allocator,
    cb: *const ops.ComputeBackend,
    input: []const f32,
    batch: usize,
    input_dim: usize,
    hidden_dim: usize,
    output_dim_u32: u32,
    first_weight: []const u8,
    first_bias: []const u8,
    second_weight: []const u8,
    second_bias: []const u8,
    activation: HeadActivation,
) ![]f32 {
    const first_w = cb.getWeight(first_weight) catch |err| switch (err) {
        error.WeightNotFound => return error.MissingHead,
        else => return err,
    };
    defer cb.free(first_w);
    const first_b = cb.getWeight(first_bias) catch |err| switch (err) {
        error.WeightNotFound => return error.MissingHead,
        else => return err,
    };
    defer cb.free(first_b);
    const output_dim: usize = @intCast(output_dim_u32);
    const input_shape = [_]i32{ @intCast(batch), @intCast(input_dim) };
    const input_ct = try cb.fromFloat32Shape(input, &input_shape);
    defer cb.free(input_ct);

    const first_ct = try cb.linear(input_ct, first_w, first_b, batch, input_dim, hidden_dim);
    defer cb.free(first_ct);

    const activated_ct = switch (activation) {
        .relu => try cb.relu(first_ct),
        .tanh => try cb.tanh_act(first_ct),
    };
    defer cb.free(activated_ct);

    const second_w = try cb.getWeight(second_weight);
    defer cb.free(second_w);
    const second_b = try cb.getWeight(second_bias);
    defer cb.free(second_b);
    const logits_ct = try cb.linear(activated_ct, second_w, second_b, batch, hidden_dim, output_dim);
    defer cb.free(logits_ct);
    return try cb.toFloat32(logits_ct, allocator);
}

fn runLinearHead(
    allocator: std.mem.Allocator,
    cb: *const ops.ComputeBackend,
    input: []const f32,
    batch: usize,
    input_dim: usize,
    output_dim_u32: u32,
    weight_name: []const u8,
    bias_name: []const u8,
) ![]f32 {
    const output_dim: usize = @intCast(output_dim_u32);
    const input_shape = [_]i32{ @intCast(batch), @intCast(input_dim) };
    const input_ct = try cb.fromFloat32Shape(input, &input_shape);
    defer cb.free(input_ct);

    const weight = try cb.getWeight(weight_name);
    defer cb.free(weight);
    const bias = try cb.getWeight(bias_name);
    defer cb.free(bias);
    const logits_ct = try cb.linear(input_ct, weight, bias, batch, input_dim, output_dim);
    defer cb.free(logits_ct);
    return try cb.toFloat32(logits_ct, allocator);
}

fn maybeApplyPooler(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    cls_embeddings: []const f32,
    batch: usize,
    hidden_size: usize,
    weight_name: []const u8,
    bias_name: []const u8,
) ![]f32 {
    const pool_w = cb.getWeight(weight_name) catch |err| switch (err) {
        error.WeightNotFound => return try allocator.dupe(f32, cls_embeddings),
        else => return err,
    };
    defer cb.free(pool_w);
    const pool_b = try cb.getWeight(bias_name);
    defer cb.free(pool_b);
    const input_shape = [_]i32{ @intCast(batch), @intCast(hidden_size) };
    const cls_ct = try cb.fromFloat32Shape(cls_embeddings, &input_shape);
    defer cb.free(cls_ct);

    const pooled_ct = try cb.linear(cls_ct, pool_w, pool_b, batch, hidden_size, hidden_size);
    defer cb.free(pooled_ct);

    const activated_ct = try cb.tanh_act(pooled_ct);
    defer cb.free(activated_ct);
    return try cb.toFloat32(activated_ct, allocator);
}

fn archInputInfo(ptr: *anyopaque) []const TensorInfo {
    const self: *ArchSession = @ptrCast(@alignCast(ptr));
    return switch (self.arch_config) {
        .clip => &.{
            .{ .name = "input_ids", .dtype = .i64, .shape = &.{ -1, -1 } },
            .{ .name = "attention_mask", .dtype = .i64, .shape = &.{ -1, -1 } },
            .{ .name = "pixel_values", .dtype = .f32, .shape = &.{ -1, 3, -1, -1 } },
        },
        .clap => &.{
            .{ .name = "input_ids", .dtype = .i64, .shape = &.{ -1, -1 } },
            .{ .name = "attention_mask", .dtype = .i64, .shape = &.{ -1, -1 } },
            .{ .name = "input_features", .dtype = .f32, .shape = &.{ -1, -1, -1, -1 } },
            .{ .name = "is_longer", .dtype = .bool_, .shape = &.{ -1, 1 } },
        },
        .layoutlmv3 => &.{
            .{ .name = "input_ids", .dtype = .i64, .shape = &.{ -1, -1 } },
            .{ .name = "attention_mask", .dtype = .i64, .shape = &.{ -1, -1 } },
            .{ .name = "bbox", .dtype = .i64, .shape = &.{ -1, -1, 4 } },
            .{ .name = "token_type_ids", .dtype = .i64, .shape = &.{ -1, -1 } },
            .{ .name = "pixel_values", .dtype = .f32, .shape = &.{ -1, 3, -1, -1 } },
        },
        else => &.{
            .{ .name = "input_ids", .dtype = .i64, .shape = &.{ -1, -1 } },
            .{ .name = "attention_mask", .dtype = .i64, .shape = &.{ -1, -1 } },
        },
    };
}

fn archOutputInfo(ptr: *anyopaque) []const TensorInfo {
    const self: *ArchSession = @ptrCast(@alignCast(ptr));
    if (self.task == .classifier and (self.arch_config == .bert or self.arch_config == .deberta or self.arch_config == .layoutlmv3)) {
        return &.{
            .{ .name = "logits", .dtype = .f32, .shape = &.{ -1, -1 } },
        };
    }
    if (self.task == .recognizer and (self.arch_config == .bert or self.arch_config == .deberta or self.arch_config == .layoutlmv3)) {
        return &.{
            .{ .name = "logits", .dtype = .f32, .shape = &.{ -1, -1, -1 } },
        };
    }
    return switch (self.arch_config) {
        .clap => &.{
            .{ .name = "text_embeds", .dtype = .f32, .shape = &.{ -1, -1 } },
        },
        else => &.{
            .{ .name = "last_hidden_state", .dtype = .f32, .shape = &.{ -1, -1, -1 } },
        },
    };
}

fn archBackend(ptr: *anyopaque) BackendType {
    const self: *ArchSession = @ptrCast(@alignCast(ptr));
    return self.backend_type;
}

fn archClose(ptr: *anyopaque) void {
    const self: *ArchSession = @ptrCast(@alignCast(ptr));
    switch (self.backend_type) {
        .native => {
            native_mod.stopPrefetchWorker(&self.backend_data.native);
            var it = self.backend_data.native.resident_weights.iterator();
            while (it.next()) |entry| {
                var w = entry.value_ptr.*;
                w.deinit();
                self.allocator.free(entry.key_ptr.*);
            }
            self.backend_data.native.resident_weights.deinit(self.allocator);

            var lazy_it = self.backend_data.native.lazy_weights.iterator();
            while (lazy_it.next()) |entry| {
                if (entry.value_ptr.loaded) |*loaded| loaded.deinit();
                entry.value_ptr.tensor_ref.deinit(self.allocator);
                self.allocator.free(entry.key_ptr.*);
            }
            self.backend_data.native.lazy_weights.deinit(self.allocator);
            native_mod.deinitPrefetchQueue(&self.backend_data.native);
            if (self.backend_data.native.residency) |*residency| residency.deinit();
            if (self.backend_data.native.tensor_store) |tensor_store| tensor_store.deinit();
        },
        .metal => {
            if (comptime build_options.enable_metal) {
                const gpu_data = gpuBackendData(self);
                metal_compute_mod.stopPrefetchWorker(gpu_data);
                metal_compute_mod.deinitSharedNativeProvider(gpu_data);
                var it = gpu_data.lazy_weights.iterator();
                while (it.next()) |entry| {
                    if (entry.value_ptr.quantized_storage) |*storage| storage.deinit();
                    if (entry.value_ptr.host_loaded) |*host_loaded| host_loaded.deinit();
                    entry.value_ptr.tensor_ref.deinit(self.allocator);
                    self.allocator.free(entry.key_ptr.*);
                }
                gpu_data.lazy_weights.deinit(self.allocator);
                metal_compute_mod.deinitPackedExpertViews(gpu_data, self.allocator);
                metal_compute_mod.deinitPrefetchQueue(gpu_data);
                if (gpu_data.residency) |*residency| residency.deinit();
                if (gpu_data.jina_lora_adapter) |adapter| adapter.destroy();
                if (gpu_data.tensor_store) |store| store.deinit();
            }
        },
        .pjrt => {
            // Deinit the PJRT client (if one was successfully initialized).
            if (build_options.enable_pjrt) {
                if (self.backend_data.pjrt.client) |*client| {
                    client.deinit();
                }
            }
            // Clean up the native CPU host-backend weight store.
            native_mod.stopPrefetchWorker(&self.backend_data.pjrt.native);
            var it = self.backend_data.pjrt.native.resident_weights.iterator();
            while (it.next()) |entry| {
                var w = entry.value_ptr.*;
                w.deinit();
                self.allocator.free(entry.key_ptr.*);
            }
            self.backend_data.pjrt.native.resident_weights.deinit(self.allocator);
            var lazy_it = self.backend_data.pjrt.native.lazy_weights.iterator();
            while (lazy_it.next()) |entry| {
                if (entry.value_ptr.loaded) |*loaded| loaded.deinit();
                entry.value_ptr.tensor_ref.deinit(self.allocator);
                self.allocator.free(entry.key_ptr.*);
            }
            self.backend_data.pjrt.native.lazy_weights.deinit(self.allocator);
            native_mod.deinitPrefetchQueue(&self.backend_data.pjrt.native);
            if (self.backend_data.pjrt.native.residency) |*residency| residency.deinit();
            if (self.backend_data.pjrt.native.tensor_store) |tensor_store| tensor_store.deinit();
        },
        .cuda => {
            if (comptime build_options.enable_cuda) {
                self.backend_data.cuda.compute.deinit();
            }
        },
        .onnx => {},
        .wasm => {},
    }
    self.allocator.destroy(self);
}

test "gemma gguf ffn norm maps to pre-feedforward layernorm" {
    var buf: [256]u8 = undefined;
    const mapped = normalizeGgufGptWeightKey(.{
        .family = .gemma,
    }, "blk.0.ffn_norm.weight", &buf).?;
    try std.testing.expectEqualStrings("model.layers.0.pre_feedforward_layernorm.weight", mapped);
}

test "large GPU-hosted lazy quant budget floor widens host and backend limits" {
    const floor = recommendedGpuHostedLazyQuantBudgetFloor(3 * 1024 * 1024 * 1024, .prefer_backend_dense);
    try std.testing.expect(floor.host_limit_bytes >= 3 * 1024 * 1024 * 1024);
    try std.testing.expect(floor.backend_limit_bytes >= 6 * 1024 * 1024 * 1024);
    try std.testing.expect(floor.combined_limit_bytes >= floor.host_limit_bytes + floor.backend_limit_bytes);
}

test "device-native GPU-hosted lazy quant budget covers E2B Q8 host cache" {
    const e2b_q8_bytes = 4700 * 1024 * 1024;
    const floor = recommendedGpuHostedLazyQuantBudgetFloor(e2b_q8_bytes, .device_native);
    try std.testing.expect(floor.host_limit_bytes >= e2b_q8_bytes + 256 * 1024 * 1024);
    try std.testing.expect(floor.host_limit_bytes <= gib(6));
    try std.testing.expect(floor.combined_limit_bytes >= floor.host_limit_bytes + floor.backend_limit_bytes);
}

test "large multimodal gemma gpu_hosted budget floor widens dense limits" {
    const floor = recommendedGpuHostedLargeMultimodalGemmaBudgetFloor(8 * 1024 * 1024 * 1024, true);
    try std.testing.expect(floor.host_limit_bytes >= 2 * 1024 * 1024 * 1024);
    try std.testing.expect(floor.backend_limit_bytes >= 6 * 1024 * 1024 * 1024);
    try std.testing.expect(floor.combined_limit_bytes >= floor.backend_limit_bytes);
}

test "session budget widening preserves higher explicit limits" {
    const widened = widenLimits(.{
        .host_limit_bytes = gib(1),
        .backend_limit_bytes = gib(4),
        .combined_limit_bytes = gib(6),
        .kv_limit_bytes = mib(512),
        .scratch_limit_bytes = mib(256),
    }, .{
        .host_limit_bytes = gib(4),
        .backend_limit_bytes = gib(8),
        .combined_limit_bytes = gib(12),
        .kv_limit_bytes = 0,
        .scratch_limit_bytes = 0,
    });
    try std.testing.expectEqual(gib(4), widened.host_limit_bytes);
    try std.testing.expectEqual(gib(8), widened.backend_limit_bytes);
    try std.testing.expectEqual(gib(12), widened.combined_limit_bytes);
    try std.testing.expectEqual(mib(512), widened.kv_limit_bytes);
    try std.testing.expectEqual(mib(256), widened.scratch_limit_bytes);
}

test "gemma gguf norm aliases stay distinct" {
    const cfg: gpt_mod.Config = .{ .family = .gemma };

    var buf0: [256]u8 = undefined;
    var buf1: [256]u8 = undefined;
    var buf2: [256]u8 = undefined;
    var buf3: [256]u8 = undefined;
    var buf4: [256]u8 = undefined;
    var buf5: [256]u8 = undefined;

    const attn = normalizeGgufGptWeightKey(cfg, "blk.0.attn_norm.weight", &buf0).?;
    const q = normalizeGgufGptWeightKey(cfg, "blk.0.attn_q_norm.weight", &buf1).?;
    const k = normalizeGgufGptWeightKey(cfg, "blk.0.attn_k_norm.weight", &buf2).?;
    const ffn = normalizeGgufGptWeightKey(cfg, "blk.0.ffn_norm.weight", &buf3).?;
    const post_attn = normalizeGgufGptWeightKey(cfg, "blk.0.post_attention_norm.weight", &buf4).?;
    const post_ffn = normalizeGgufGptWeightKey(cfg, "blk.0.post_ffw_norm.weight", &buf5).?;

    try std.testing.expectEqualStrings("model.layers.0.input_layernorm.weight", attn);
    try std.testing.expectEqualStrings("model.layers.0.self_attn.q_norm.weight", q);
    try std.testing.expectEqualStrings("model.layers.0.self_attn.k_norm.weight", k);
    try std.testing.expectEqualStrings("model.layers.0.pre_feedforward_layernorm.weight", ffn);
    try std.testing.expectEqualStrings("model.layers.0.post_attention_layernorm.weight", post_attn);
    try std.testing.expectEqualStrings("model.layers.0.post_feedforward_layernorm.weight", post_ffn);
}

test "overlay gpt structural config keeps gguf gemma norm offset" {
    var target: gpt_mod.Config = .{
        .family = .gemma,
        .norm_weight_offset = 1.0,
        .rope_theta = 1_000_000.0,
    };
    const source: gpt_mod.Config = .{
        .family = .gemma,
        .norm_weight_offset = 0.0,
        .rope_theta = 1_000_000.0,
    };
    overlayGptStructuralConfig(&target, source);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), target.norm_weight_offset, 1e-6);
}

test "overlay gpt structural config takes weight tying from gguf" {
    // Gemma 3 config.json omits tie_word_embeddings, so the HF-derived config says false
    // while the GGUF (which has no output.weight tensor) says true. Honoring config.json
    // sent the runtime looking for a lm_head.weight that is not in the file.
    var target: gpt_mod.Config = .{ .family = .gemma, .weight_tying = false };
    const source: gpt_mod.Config = .{ .family = .gemma, .weight_tying = true };

    overlayGptStructuralConfig(&target, source);

    try std.testing.expect(target.weight_tying);
}

test "overlay gpt structural config takes rope partial factor from gguf" {
    // unsloth's Gemma 4 config.json claims partial_rotary_factor 0.25 while its GGUF
    // declares rope.dimension_count equal to the head dim (full rotation). Following
    // config.json produced empty completions from an otherwise-identical artifact.
    var target: gpt_mod.Config = .{ .family = .gemma, .rope_partial_factor = 0.25 };
    const source: gpt_mod.Config = .{ .family = .gemma, .rope_partial_factor = 1.0 };

    overlayGptStructuralConfig(&target, source);

    try std.testing.expectApproxEqAbs(@as(f32, 1.0), target.rope_partial_factor, 1e-6);
}

test "overlay gpt structural config clears sidecar-only shared tail ffn size" {
    var target: gpt_mod.Config = .{
        .family = .gemma,
        .intermediate_size = 10240,
        .shared_layer_intermediate_size = 20480,
    };
    const source: gpt_mod.Config = .{
        .family = .gemma,
        .intermediate_size = 10240,
        .shared_layer_intermediate_size = 0,
    };

    overlayGptStructuralConfig(&target, source);

    try std.testing.expectEqual(@as(u32, 0), target.shared_layer_intermediate_size);
    try std.testing.expectEqual(@as(u32, 10240), target.intermediateSize(41));
}

test "metal gguf preflight rejects release-unsafe iq4 xs tensors" {
    var tensor_types = [_]UnsupportedTensorTypeCount{
        .{ .tensor_type = .{ .known = .IQ4_XS }, .count = 15 },
    };
    const report = GgufInspectionReport{
        .allocator = std.testing.allocator,
        .architecture = "gemma4",
        .tensor_count = 15,
        .metadata_count = 0,
        .all_tensor_types = tensor_types[0..],
    };

    if (comptime build_options.enable_metal) {
        const incompatible = metalGgufIncompatibleTensorType(report) orelse return error.TestExpectedEqual;
        try std.testing.expectEqual(gguf_mod.tensor_types.KnownTensorType.IQ4_XS, incompatible.tensor_type.known);
        try std.testing.expectEqual(@as(usize, 15), incompatible.count);
    } else try std.testing.expect(metalGgufIncompatibleTensorType(report) == null);
}

test "overlay gpt structural config keeps config softcapping when gguf omits it" {
    var target: gpt_mod.Config = .{ .family = .gemma, .final_logit_softcapping = 30.0 };
    const source: gpt_mod.Config = .{ .family = .gemma, .final_logit_softcapping = 0.0 };

    overlayGptStructuralConfig(&target, source);

    try std.testing.expectApproxEqAbs(@as(f32, 30.0), target.final_logit_softcapping, 1e-6);
}

test "qwen3 gguf weights normalize onto hf names" {
    var buf: [256]u8 = undefined;
    const config = gpt_mod.Config{ .family = .qwen3 };

    try std.testing.expectEqualStrings(
        "model.layers.7.self_attn.q_norm.weight",
        normalizeGgufGptWeightKey(config, "blk.7.attn_q_norm.weight", &buf).?,
    );
    try std.testing.expectEqualStrings(
        "model.layers.7.self_attn.k_norm.weight",
        normalizeGgufGptWeightKey(config, "blk.7.attn_k_norm.weight", &buf).?,
    );
    try std.testing.expectEqualStrings(
        "model.layers.0.self_attn.q_proj.weight",
        normalizeGgufGptWeightKey(config, "blk.0.attn_q.weight", &buf).?,
    );
    // Qwen3 has no qkv bias, so ffn_norm must land on post_attention_layernorm rather
    // than the Gemma-style pre_feedforward_layernorm.
    try std.testing.expectEqualStrings(
        "model.layers.0.post_attention_layernorm.weight",
        normalizeGgufGptWeightKey(config, "blk.0.ffn_norm.weight", &buf).?,
    );
    try std.testing.expectEqualStrings(
        "model.embed_tokens.weight",
        normalizeGgufGptWeightKey(config, "token_embd.weight", &buf).?,
    );
}

test "qwen3_5 gguf weights are not claimed as mappable" {
    // qwen3_5 carries ssm_*/attn_qkv/attn_gate tensors with no mapping to the
    // linear-attention runtime. Claiming support would fail late with a wall of
    // missing tensors instead of a clear unsupported-architecture error.
    var buf: [256]u8 = undefined;
    try std.testing.expect(!gpt_mod.ggufWeightMappingSupported(.qwen3_5));
    try std.testing.expect(normalizeGgufGptWeightKey(
        .{ .family = .qwen3_5 },
        "blk.0.attn_q.weight",
        &buf,
    ) == null);
}

test "mistral gguf ffn norm maps to post-attention layernorm" {
    var buf: [256]u8 = undefined;
    const mapped = normalizeGgufGptWeightKey(.{
        .family = .mistral,
    }, "blk.0.ffn_norm.weight", &buf).?;
    try std.testing.expectEqualStrings("model.layers.0.post_attention_layernorm.weight", mapped);
}

test "qwen2 gguf attention bias tensors map to architecture weights" {
    const cfg: gpt_mod.Config = .{ .family = .qwen2 };

    var buf0: [256]u8 = undefined;
    const q_bias = normalizeGgufGptWeightKey(cfg, "blk.0.attn_q.bias", &buf0).?;
    try std.testing.expectEqualStrings("model.layers.0.self_attn.q_proj.bias", q_bias);

    var buf1: [256]u8 = undefined;
    const k_bias = normalizeGgufGptWeightKey(cfg, "blk.0.attn_k.bias", &buf1).?;
    try std.testing.expectEqualStrings("model.layers.0.self_attn.k_proj.bias", k_bias);

    var buf2: [256]u8 = undefined;
    const v_bias = normalizeGgufGptWeightKey(cfg, "blk.0.attn_v.bias", &buf2).?;
    try std.testing.expectEqualStrings("model.layers.0.self_attn.v_proj.bias", v_bias);
}

test "gpt neo required tensors match native gguf names" {
    const allocator = std.testing.allocator;
    var names = std.StringHashMapUnmanaged(void){};
    defer {
        var it = names.keyIterator();
        while (it.next()) |key| allocator.free(key.*);
        names.deinit(allocator);
    }

    const present = [_][]const u8{
        "wte.weight",
        "wpe.weight",
        "ln_f.weight",
        "ln_f.bias",
        "lm_head.weight",
        "h.0.ln_1.weight",
        "h.0.ln_1.bias",
        "h.0.attn.attention.q_proj.weight",
        "h.0.attn.attention.k_proj.weight",
        "h.0.attn.attention.v_proj.weight",
        "h.0.attn.attention.out_proj.weight",
        "h.0.ln_2.weight",
        "h.0.ln_2.bias",
        "h.0.mlp.c_fc.weight",
        "h.0.mlp.c_fc.bias",
        "h.0.mlp.c_proj.weight",
        "h.0.mlp.c_proj.bias",
    };
    for (present) |name| {
        try names.put(allocator, try allocator.dupe(u8, name), {});
    }

    var missing = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (missing.items) |name| allocator.free(name);
        missing.deinit(allocator);
    }

    try collectMissingRequiredGptWeights(allocator, .{
        .family = .gpt_neo,
        .num_hidden_layers = 1,
        .position_encoding = .absolute,
        .weight_tying = false,
    }, &names, &missing, false);

    try std.testing.expectEqual(@as(usize, 0), missing.items.len);
}

test "gptj required tensors match generic exported names" {
    const allocator = std.testing.allocator;
    var names = std.StringHashMapUnmanaged(void){};
    defer {
        var it = names.keyIterator();
        while (it.next()) |key| allocator.free(key.*);
        names.deinit(allocator);
    }

    const present = [_][]const u8{
        "model.embed_tokens.weight",
        "wpe.weight",
        "model.norm.weight",
        "model.norm.bias",
        "lm_head.weight",
        "model.layers.0.input_layernorm.weight",
        "model.layers.0.input_layernorm.bias",
        "model.layers.0.self_attn.q_proj.weight",
        "model.layers.0.self_attn.k_proj.weight",
        "model.layers.0.self_attn.v_proj.weight",
        "model.layers.0.self_attn.o_proj.weight",
        "model.layers.0.post_attention_layernorm.weight",
        "model.layers.0.post_attention_layernorm.bias",
        "model.layers.0.mlp.fc1_proj.weight",
        "model.layers.0.mlp.fc1_proj.bias",
        "model.layers.0.mlp.fc2_proj.weight",
        "model.layers.0.mlp.fc2_proj.bias",
    };
    for (present) |name| {
        try names.put(allocator, try allocator.dupe(u8, name), {});
    }

    var missing = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (missing.items) |name| allocator.free(name);
        missing.deinit(allocator);
    }

    try collectMissingRequiredGptWeights(allocator, .{
        .family = .gptj,
        .num_hidden_layers = 1,
        .position_encoding = .absolute,
        .weight_tying = false,
    }, &names, &missing, false);

    try std.testing.expectEqual(@as(usize, 0), missing.items.len);
}

test "gpt neox required tensors match generic exported names" {
    const allocator = std.testing.allocator;
    var names = std.StringHashMapUnmanaged(void){};
    defer {
        var it = names.keyIterator();
        while (it.next()) |key| allocator.free(key.*);
        names.deinit(allocator);
    }

    const present = [_][]const u8{
        "model.embed_tokens.weight",
        "model.norm.weight",
        "model.norm.bias",
        "lm_head.weight",
        "model.layers.0.input_layernorm.weight",
        "model.layers.0.input_layernorm.bias",
        "model.layers.0.self_attn.q_proj.weight",
        "model.layers.0.self_attn.q_proj.bias",
        "model.layers.0.self_attn.k_proj.weight",
        "model.layers.0.self_attn.k_proj.bias",
        "model.layers.0.self_attn.v_proj.weight",
        "model.layers.0.self_attn.v_proj.bias",
        "model.layers.0.self_attn.o_proj.weight",
        "model.layers.0.post_attention_layernorm.weight",
        "model.layers.0.post_attention_layernorm.bias",
        "model.layers.0.mlp.fc1_proj.weight",
        "model.layers.0.mlp.fc1_proj.bias",
        "model.layers.0.mlp.fc2_proj.weight",
        "model.layers.0.mlp.fc2_proj.bias",
    };
    for (present) |name| {
        try names.put(allocator, try allocator.dupe(u8, name), {});
    }

    var missing = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (missing.items) |name| allocator.free(name);
        missing.deinit(allocator);
    }

    try collectMissingRequiredGptWeights(allocator, .{
        .family = .gpt_neox,
        .num_hidden_layers = 1,
        .position_encoding = .rope,
        .weight_tying = false,
    }, &names, &missing, false);

    try std.testing.expectEqual(@as(usize, 0), missing.items.len);
}

test "gemma4 shared kv tail does not require per-layer k/v tensors" {
    const allocator = std.testing.allocator;
    var names = std.StringHashMapUnmanaged(void){};
    defer {
        var it = names.keyIterator();
        while (it.next()) |key| allocator.free(key.*);
        names.deinit(allocator);
    }

    const present = [_][]const u8{
        "model.embed_tokens.weight",
        "model.norm.weight",
        "model.layers.0.input_layernorm.weight",
        "model.layers.0.self_attn.q_proj.weight",
        "model.layers.0.self_attn.k_proj.weight",
        "model.layers.0.self_attn.v_proj.weight",
        "model.layers.0.self_attn.o_proj.weight",
        "model.layers.0.post_attention_layernorm.weight",
        "model.layers.0.mlp.gate_proj.weight",
        "model.layers.0.mlp.up_proj.weight",
        "model.layers.0.mlp.down_proj.weight",
        "model.layers.1.input_layernorm.weight",
        "model.layers.1.self_attn.q_proj.weight",
        "model.layers.1.self_attn.o_proj.weight",
        "model.layers.1.post_attention_layernorm.weight",
        "model.layers.1.mlp.gate_proj.weight",
        "model.layers.1.mlp.up_proj.weight",
        "model.layers.1.mlp.down_proj.weight",
    };
    for (present) |name| {
        try names.put(allocator, try allocator.dupe(u8, name), {});
    }

    var missing = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (missing.items) |name| allocator.free(name);
        missing.deinit(allocator);
    }

    try collectMissingRequiredGptWeights(allocator, .{
        .family = .gemma,
        .num_hidden_layers = 2,
        .num_kv_shared_layers = 1,
        .position_encoding = .rope,
        .weight_tying = true,
    }, &names, &missing, false);

    try std.testing.expectEqual(@as(usize, 0), missing.items.len);
}

test "deepseek v4 required tensors use canonical hf names" {
    const allocator = std.testing.allocator;
    var names = std.StringHashMapUnmanaged(void){};
    defer {
        var it = names.keyIterator();
        while (it.next()) |key| allocator.free(key.*);
        names.deinit(allocator);
    }

    var attention_schedule = [_]gpt_mod.DeepseekV4AttentionKind{.sliding_attention} ** gpt_mod.deepseek_v4_max_layers;
    attention_schedule[1] = .compressed_sparse_attention;
    attention_schedule[2] = .heavily_compressed_attention;

    var mlp_schedule = [_]gpt_mod.DeepseekV4MlpKind{.moe} ** gpt_mod.deepseek_v4_max_layers;
    mlp_schedule[0] = .hash_moe;

    var missing = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (missing.items) |name| allocator.free(name);
        missing.deinit(allocator);
    }

    try collectMissingRequiredGptWeights(allocator, .{
        .family = .deepseek_v4,
        .num_hidden_layers = 3,
        .deepseek_v4_attention_schedule_len = 3,
        .deepseek_v4_attention_schedule = attention_schedule,
        .deepseek_v4_mlp_schedule_len = 3,
        .deepseek_v4_mlp_schedule = mlp_schedule,
    }, &names, &missing, false);

    try std.testing.expectEqual(@as(usize, 91), missing.items.len);
    try std.testing.expect(missingContains(missing.items, "model.embed_tokens.weight"));
    try std.testing.expect(missingContains(missing.items, "model.norm.weight"));
    try std.testing.expect(missingContains(missing.items, "model.hc_head.hc_fn"));
    try std.testing.expect(missingContains(missing.items, "model.hc_head.hc_base"));
    try std.testing.expect(missingContains(missing.items, "model.hc_head.hc_scale"));

    try std.testing.expect(missingContains(missing.items, "model.layers.0.self_attn.q_a_proj.weight"));
    try std.testing.expect(missingContains(missing.items, "model.layers.0.self_attn.q_a_norm.weight"));
    try std.testing.expect(missingContains(missing.items, "model.layers.0.self_attn.q_b_proj.weight"));
    try std.testing.expect(missingContains(missing.items, "model.layers.0.self_attn.kv_proj.weight"));
    try std.testing.expect(missingContains(missing.items, "model.layers.0.self_attn.kv_norm.weight"));
    try std.testing.expect(missingContains(missing.items, "model.layers.0.self_attn.o_a_proj.weight"));
    try std.testing.expect(missingContains(missing.items, "model.layers.0.self_attn.o_b_proj.weight"));
    try std.testing.expect(missingContains(missing.items, "model.layers.0.self_attn.sinks"));
    try std.testing.expect(missingContains(missing.items, "model.layers.0.attn_hc.fn"));
    try std.testing.expect(missingContains(missing.items, "model.layers.0.attn_hc.base"));
    try std.testing.expect(missingContains(missing.items, "model.layers.0.attn_hc.scale"));
    try std.testing.expect(missingContains(missing.items, "model.layers.0.ffn_hc.fn"));
    try std.testing.expect(missingContains(missing.items, "model.layers.0.ffn_hc.base"));
    try std.testing.expect(missingContains(missing.items, "model.layers.0.ffn_hc.scale"));
    try std.testing.expect(missingContains(missing.items, "model.layers.0.mlp.gate.weight"));
    try std.testing.expect(missingContains(missing.items, "model.layers.0.mlp.gate.tid2eid"));
    try std.testing.expect(!missingContains(missing.items, "model.layers.0.mlp.gate.e_score_correction_bias"));

    try std.testing.expect(missingContains(missing.items, "model.layers.1.self_attn.compressor.kv_proj.weight"));
    try std.testing.expect(missingContains(missing.items, "model.layers.1.self_attn.compressor.gate_proj.weight"));
    try std.testing.expect(missingContains(missing.items, "model.layers.1.self_attn.compressor.position_bias"));
    try std.testing.expect(missingContains(missing.items, "model.layers.1.self_attn.compressor.kv_norm.weight"));
    try std.testing.expect(missingContains(missing.items, "model.layers.1.self_attn.compressor.indexer.kv_proj.weight"));
    try std.testing.expect(missingContains(missing.items, "model.layers.1.self_attn.compressor.indexer.gate_proj.weight"));
    try std.testing.expect(missingContains(missing.items, "model.layers.1.self_attn.compressor.indexer.position_bias"));
    try std.testing.expect(missingContains(missing.items, "model.layers.1.self_attn.compressor.indexer.kv_norm.weight"));
    try std.testing.expect(missingContains(missing.items, "model.layers.1.self_attn.compressor.indexer.q_b_proj.weight"));
    try std.testing.expect(missingContains(missing.items, "model.layers.1.self_attn.compressor.indexer.weights_proj.weight"));
    try std.testing.expect(missingContains(missing.items, "model.layers.1.mlp.gate.e_score_correction_bias"));
    try std.testing.expect(!missingContains(missing.items, "model.layers.1.mlp.gate.tid2eid"));

    try std.testing.expect(missingContains(missing.items, "model.layers.2.self_attn.compressor.kv_proj.weight"));
    try std.testing.expect(!missingContains(missing.items, "model.layers.2.self_attn.compressor.indexer.kv_proj.weight"));
    try std.testing.expect(missingContains(missing.items, "model.layers.2.mlp.experts.gate_proj"));
    try std.testing.expect(missingContains(missing.items, "model.layers.2.mlp.experts.up_proj"));
    try std.testing.expect(missingContains(missing.items, "model.layers.2.mlp.experts.down_proj"));
    try std.testing.expect(missingContains(missing.items, "model.layers.2.mlp.shared_experts.gate_proj.weight"));
    try std.testing.expect(missingContains(missing.items, "model.layers.2.mlp.shared_experts.up_proj.weight"));
    try std.testing.expect(missingContains(missing.items, "model.layers.2.mlp.shared_experts.down_proj.weight"));

    try std.testing.expect(!missingContains(missing.items, "model.layers.0.self_attn.q_proj.weight"));
    try std.testing.expect(!missingContains(missing.items, "model.layers.0.mlp.down_proj.weight"));
}

test "deepseek v4 gguf tensor names map to canonical runtime names" {
    var buf0: [256]u8 = undefined;
    var buf1: [256]u8 = undefined;
    var buf2: [256]u8 = undefined;
    var buf3: [256]u8 = undefined;
    var buf4: [256]u8 = undefined;
    var buf5: [256]u8 = undefined;
    const cfg = gpt_mod.Config{ .family = .deepseek_v4 };

    try std.testing.expectEqualStrings("model.layers.0.self_attn.q_a_proj.weight", normalizeGgufGptWeightKey(cfg, "blk.0.attn_q_a.weight", &buf0).?);
    try std.testing.expectEqualStrings("model.layers.0.self_attn.q_a_norm.weight", normalizeGgufGptWeightKey(cfg, "blk.0.attn_q_a_norm.weight", &buf1).?);
    try std.testing.expectEqualStrings("model.layers.0.self_attn.kv_proj.weight", normalizeGgufGptWeightKey(cfg, "blk.0.attn_kv_a_mqa.weight", &buf2).?);
    try std.testing.expectEqualStrings("model.layers.0.self_attn.o_b_proj.weight", normalizeGgufGptWeightKey(cfg, "blk.0.attn_o_b.weight", &buf3).?);
    try std.testing.expectEqualStrings("model.layers.0.mlp.gate.weight", normalizeGgufGptWeightKey(cfg, "blk.0.ffn_gate_inp.weight", &buf4).?);
    try std.testing.expectEqualStrings("model.layers.0.mlp.shared_experts.up_proj.weight", normalizeGgufGptWeightKey(cfg, "blk.0.ffn_up_shexp.weight", &buf5).?);
}

test "deepseek v4 flash gguf tensor dump aliases map to canonical runtime names" {
    const cfg = gpt_mod.Config{ .family = .deepseek_v4 };
    var buf0: [256]u8 = undefined;
    var buf1: [256]u8 = undefined;
    var buf2: [256]u8 = undefined;
    var buf3: [256]u8 = undefined;
    var buf4: [256]u8 = undefined;
    var buf5: [256]u8 = undefined;
    var buf6: [256]u8 = undefined;
    var buf7: [256]u8 = undefined;
    var buf8: [256]u8 = undefined;
    var buf9: [256]u8 = undefined;
    var buf10: [256]u8 = undefined;
    var buf11: [256]u8 = undefined;
    var buf12: [256]u8 = undefined;
    var buf13: [256]u8 = undefined;
    var buf14: [256]u8 = undefined;
    var buf15: [256]u8 = undefined;
    var buf16: [256]u8 = undefined;
    var buf17: [256]u8 = undefined;
    var buf18: [256]u8 = undefined;

    try std.testing.expectEqualStrings("model.layers.2.self_attn.kv_proj.weight", normalizeGgufGptWeightKey(cfg, "blk.2.attn_kv_latent.weight", &buf0).?);
    try std.testing.expectEqualStrings("model.layers.2.self_attn.o_a_proj.weight", normalizeGgufGptWeightKey(cfg, "blk.2.attn_output_a.weight", &buf1).?);
    try std.testing.expectEqualStrings("model.layers.2.self_attn.o_b_proj.weight", normalizeGgufGptWeightKey(cfg, "blk.2.attn_output_b.weight", &buf2).?);
    try std.testing.expectEqualStrings("model.layers.2.mlp.gate.tid2eid", normalizeGgufGptWeightKey(cfg, "blk.2.ffn_gate_tid2eid", &buf3).?);
    try std.testing.expectEqualStrings("model.layers.2.mlp.gate.e_score_correction_bias", normalizeGgufGptWeightKey(cfg, "blk.2.exp_probs_b", &buf4).?);
    try std.testing.expectEqualStrings("model.layers.2.attn_hc.fn", normalizeGgufGptWeightKey(cfg, "blk.2.hc_attn_fn", &buf5).?);
    try std.testing.expectEqualStrings("model.layers.2.attn_hc.base", normalizeGgufGptWeightKey(cfg, "blk.2.hc_attn_base", &buf6).?);
    try std.testing.expectEqualStrings("model.layers.2.attn_hc.scale", normalizeGgufGptWeightKey(cfg, "blk.2.hc_attn_scale", &buf7).?);
    try std.testing.expectEqualStrings("model.layers.2.ffn_hc.fn", normalizeGgufGptWeightKey(cfg, "blk.2.hc_ffn_fn", &buf8).?);
    try std.testing.expectEqualStrings("model.layers.2.self_attn.compressor.kv_proj.weight", normalizeGgufGptWeightKey(cfg, "blk.2.attn_compress_kv.weight", &buf9).?);
    try std.testing.expectEqualStrings("model.layers.2.self_attn.compressor.gate_proj.weight", normalizeGgufGptWeightKey(cfg, "blk.2.attn_compress_gate.weight", &buf10).?);
    try std.testing.expectEqualStrings("model.layers.2.self_attn.compressor.position_bias", normalizeGgufGptWeightKey(cfg, "blk.2.attn_compress_ape", &buf11).?);
    try std.testing.expectEqualStrings("model.layers.2.self_attn.compressor.kv_norm.weight", normalizeGgufGptWeightKey(cfg, "blk.2.attn_compress_norm.weight", &buf12).?);
    try std.testing.expectEqualStrings("model.layers.2.self_attn.compressor.indexer.q_b_proj.weight", normalizeGgufGptWeightKey(cfg, "blk.2.indexer.attn_q_b.weight", &buf13).?);
    try std.testing.expectEqualStrings("model.layers.2.self_attn.compressor.indexer.weights_proj.weight", normalizeGgufGptWeightKey(cfg, "blk.2.indexer.proj.weight", &buf14).?);
    try std.testing.expectEqualStrings("model.layers.2.self_attn.compressor.indexer.position_bias", normalizeGgufGptWeightKey(cfg, "blk.2.indexer.compress_ape", &buf15).?);
    try std.testing.expectEqualStrings("model.layers.2.self_attn.compressor.indexer.kv_proj.weight", normalizeGgufGptWeightKey(cfg, "blk.2.indexer.compress_kv.weight", &buf16).?);
    try std.testing.expectEqualStrings("model.layers.2.self_attn.compressor.indexer.gate_proj.weight", normalizeGgufGptWeightKey(cfg, "blk.2.indexer.compress_gate.weight", &buf17).?);
    try std.testing.expectEqualStrings("model.layers.2.self_attn.compressor.indexer.kv_norm.weight", normalizeGgufGptWeightKey(cfg, "blk.2.indexer.compress_norm.weight", &buf18).?);
}

test "deepseek v4 gguf packed moe names map to routed expert lazy keys" {
    const gate = parsePackedMoeTensor("blk.2.ffn_gate_exps.weight").?;
    try std.testing.expectEqual(@as(usize, 2), gate.layer);
    try std.testing.expectEqualStrings("gate_proj", deepseek_v4_arch.moeProjectionName(gate.proj).?);
    try std.testing.expect(gate.proj2 == null);
    try std.testing.expect(!gate.fused_gate_up);

    const up = parsePackedMoeTensor("blk.2.ffn_up_exps.weight").?;
    try std.testing.expectEqualStrings("up_proj", deepseek_v4_arch.moeProjectionName(up.proj).?);
    const down = parsePackedMoeTensor("blk.2.ffn_down_exps.weight").?;
    try std.testing.expectEqualStrings("down_proj", deepseek_v4_arch.moeProjectionName(down.proj).?);

    const fused = parsePackedMoeTensor("blk.2.ffn_gate_up_exps.weight").?;
    try std.testing.expect(fused.fused_gate_up);
    try std.testing.expectEqualStrings("w1", fused.proj);
    try std.testing.expectEqualStrings("w3", fused.proj2.?);

    const coord = parseMoeExpertCoord("model.layers.2.mlp.experts.7.gate_proj").?;
    try std.testing.expectEqual(@as(usize, 2), coord.layer_index);
    try std.testing.expectEqual(@as(u32, 7), coord.expert_index);
    try std.testing.expectEqual(@as(u8, 0x1), projectionMaskForWeightKey("model.layers.2.mlp.experts.7.gate_proj"));
    try std.testing.expectEqual(@as(u8, 0x2), projectionMaskForWeightKey("model.layers.2.mlp.experts.7.down_proj"));
    try std.testing.expectEqual(@as(u8, 0x4), projectionMaskForWeightKey("model.layers.2.mlp.experts.7.up_proj"));
    try std.testing.expectEqual(@as(u8, 0x5), projectionMaskForWeightKey("model.layers.2.mlp.experts.7.gate_up_proj"));
    try std.testing.expect(shouldLazyLoadWeight(.gguf, .{ .gpt = .{ .family = .deepseek_v4, .num_local_experts = 8, .num_experts_per_tok = 2 } }, "model.layers.2.mlp.experts.7.gate_proj"));
}

fn missingContains(missing: []const []const u8, expected: []const u8) bool {
    for (missing) |name| {
        if (std.mem.eql(u8, name, expected)) return true;
    }
    return false;
}

test "phi gguf tensors map to architecture weights" {
    const cfg: gpt_mod.Config = .{ .family = .phi };

    var buf0: [256]u8 = undefined;
    try std.testing.expectEqualStrings("model.norm.bias", normalizeGgufGptWeightKey(cfg, "output_norm.bias", &buf0).?);

    var buf1: [256]u8 = undefined;
    try std.testing.expectEqualStrings("model.layers.0.input_layernorm.bias", normalizeGgufGptWeightKey(cfg, "blk.0.attn_norm.bias", &buf1).?);

    var buf2: [256]u8 = undefined;
    try std.testing.expectEqualStrings("model.layers.0.post_attention_layernorm.bias", normalizeGgufGptWeightKey(cfg, "blk.0.ffn_norm.bias", &buf2).?);

    var buf3: [256]u8 = undefined;
    try std.testing.expectEqualStrings("model.layers.0.self_attn.q_proj.bias", normalizeGgufGptWeightKey(cfg, "blk.0.attn_q.bias", &buf3).?);

    var buf4: [256]u8 = undefined;
    try std.testing.expectEqualStrings("model.layers.0.mlp.fc1_proj.weight", normalizeGgufGptWeightKey(cfg, "blk.0.ffn_up.weight", &buf4).?);

    var buf5: [256]u8 = undefined;
    try std.testing.expectEqualStrings("model.layers.0.mlp.fc2_proj.weight", normalizeGgufGptWeightKey(cfg, "blk.0.ffn_down.weight", &buf5).?);
}

test "bitnet gguf tensor names map to architecture weights" {
    const cfg: gpt_mod.Config = .{ .family = .bitnet };

    var buf0: [256]u8 = undefined;
    const token = normalizeGgufGptWeightKey(cfg, "token_embd.weight", &buf0).?;
    try std.testing.expectEqualStrings("model.embed_tokens.weight", token);

    var buf1: [256]u8 = undefined;
    const attn_norm = normalizeGgufGptWeightKey(cfg, "blk.0.attn_norm.weight", &buf1).?;
    try std.testing.expectEqualStrings("model.layers.0.input_layernorm.weight", attn_norm);

    var buf2: [256]u8 = undefined;
    const ffn_norm = normalizeGgufGptWeightKey(cfg, "blk.0.ffn_norm.weight", &buf2).?;
    try std.testing.expectEqualStrings("model.layers.0.post_attention_layernorm.weight", ffn_norm);

    var buf3: [256]u8 = undefined;
    const attn_sub_norm = normalizeGgufGptWeightKey(cfg, "blk.0.attn_sub_norm.weight", &buf3).?;
    try std.testing.expectEqualStrings("model.layers.0.self_attn.attn_sub_norm.weight", attn_sub_norm);

    var buf4: [256]u8 = undefined;
    const ffn_sub_norm = normalizeGgufGptWeightKey(cfg, "blk.0.ffn_sub_norm.weight", &buf4).?;
    try std.testing.expectEqualStrings("model.layers.0.mlp.ffn_sub_norm.weight", ffn_sub_norm);

    var buf5: [256]u8 = undefined;
    const q_proj = normalizeGgufGptWeightKey(cfg, "blk.0.attn_q.weight", &buf5).?;
    try std.testing.expectEqualStrings("model.layers.0.self_attn.q_proj.weight", q_proj);

    var buf6: [256]u8 = undefined;
    const down_proj = normalizeGgufGptWeightKey(cfg, "blk.0.ffn_down.weight", &buf6).?;
    try std.testing.expectEqualStrings("model.layers.0.mlp.down_proj.weight", down_proj);
}

test "bitnet i2_s tensor type is supported for gguf inspection" {
    try std.testing.expect(tensorTypeSupported(.{ .known = .I2_S }));
}

test "gemma4 gguf shared expert weight names map correctly" {
    var buf: [256]u8 = undefined;

    const gate = normalizeGgufGptWeightKey(.{ .family = .gemma }, "blk.5.ffn_gate_shexp.weight", &buf).?;
    try std.testing.expectEqualStrings("model.layers.5.block_sparse_moe.shared_expert.gate_proj.weight", gate);

    var buf2: [256]u8 = undefined;
    const down = normalizeGgufGptWeightKey(.{ .family = .gemma }, "blk.0.ffn_down_shexp.weight", &buf2).?;
    try std.testing.expectEqualStrings("model.layers.0.block_sparse_moe.shared_expert.down_proj.weight", down);

    var buf3: [256]u8 = undefined;
    const up = normalizeGgufGptWeightKey(.{ .family = .gemma }, "blk.12.ffn_up_shexp.weight", &buf3).?;
    try std.testing.expectEqualStrings("model.layers.12.block_sparse_moe.shared_expert.up_proj.weight", up);
}

test "gemma4 packed moe tensors are tracked but not reported as unmapped" {
    const allocator = std.testing.allocator;
    var tensors = [_]gguf_mod.format.TensorInfo{
        .{
            .name = "blk.0.ffn_down_exps.weight",
            .dimensions = &.{},
            .tensor_type = .{ .known = .Q5_1 },
            .offset = 0,
            .data_offset = 0,
        },
    };
    const file = gguf_mod.format.File{
        .header = .{ .version = 3, .tensor_count = tensors.len, .metadata_count = 0 },
        .metadata = &.{},
        .tensors = tensors[0..],
        .alignment = gguf_mod.format.default_alignment,
        .data_region_offset = 0,
    };

    var names = std.StringHashMapUnmanaged(void).empty;
    defer {
        var it = names.keyIterator();
        while (it.next()) |key| allocator.free(key.*);
        names.deinit(allocator);
    }
    var unmapped = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (unmapped.items) |name| allocator.free(name);
        unmapped.deinit(allocator);
    }
    var packed_moe = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (packed_moe.items) |name| allocator.free(name);
        packed_moe.deinit(allocator);
    }

    try collectNormalizedGgufNames(allocator, .{ .gpt = .{ .family = .gemma } }, &file, &names, &unmapped, &packed_moe);

    try std.testing.expectEqual(@as(usize, 1), packed_moe.items.len);
    try std.testing.expectEqual(@as(usize, 0), unmapped.items.len);
}

test "defaultResidentExpertsPerLayer returns 24 for 128-expert top_k=8 model" {
    const cfg: gpt_mod.Config = .{
        .num_local_experts = 128,
        .num_experts_per_tok = 8,
    };
    const resident = defaultResidentExpertsPerLayer(.{ .gpt = cfg });
    // 128 experts, top_k=8, multiplier=3 (>=64 experts) → min(128, max(4, 24)) = 24
    try std.testing.expectEqual(@as(usize, 24), resident);
}

test "defaultResidentExpertsPerLayer returns 4 for 8-expert top_k=2 model" {
    const cfg: gpt_mod.Config = .{
        .num_local_experts = 8,
        .num_experts_per_tok = 2,
    };
    const resident = defaultResidentExpertsPerLayer(.{ .gpt = cfg });
    // 8 experts, top_k=2, multiplier=2 (<64 experts) → min(8, max(4, 4)) = 4
    try std.testing.expectEqual(@as(usize, 4), resident);
}

test "defaultResidentExpertsPerLayer returns 0 for non-MoE model" {
    const cfg: gpt_mod.Config = .{};
    const resident = defaultResidentExpertsPerLayer(.{ .gpt = cfg });
    try std.testing.expectEqual(@as(usize, 0), resident);
}

test "Gemma resident embeddings retain CUDA-supported quantized formats" {
    const gemma_cfg: gpt_mod.Config = .{ .family = .gemma };
    try std.testing.expect(shouldKeepResidentGptEmbeddingQuantizedOnly(gemma_cfg, .{ .known = .Q8_0 }));
    try std.testing.expect(shouldKeepResidentGptEmbeddingQuantizedOnly(gemma_cfg, .{ .known = .Q4_0 }));
    try std.testing.expect(shouldKeepResidentGptEmbeddingQuantizedOnly(gemma_cfg, .{ .known = .Q4_K }));
    try std.testing.expect(shouldKeepResidentGptEmbeddingQuantizedOnly(gemma_cfg, .{ .known = .Q6_K }));

    const llama_cfg: gpt_mod.Config = .{ .family = .llama };
    try std.testing.expect(!shouldKeepResidentGptEmbeddingQuantizedOnly(llama_cfg, .{ .known = .Q6_K }));
}

test "serving policy does not disable existing gguf weight mappings" {
    var buf: [256]u8 = undefined;
    try std.testing.expectEqualStrings(
        "model.layers.3.self_attn.q_proj.weight",
        normalizeGgufGptWeightKey(.{ .family = .mistral }, "blk.3.attn_q.weight", &buf).?,
    );
    try std.testing.expect(normalizeGgufGptWeightKey(
        .{ .family = .falcon },
        "blk.3.attn_q.weight",
        &buf,
    ) == null);
}

test "deepseek v4 gguf architecture name is recognized" {
    // llama.cpp writes "deepseek4", not any of the HF config.json spellings. Without
    // this the whole deepseek_v4 implementation is unreachable from GGUF.
    try std.testing.expectEqual(gpt_mod.ModelFamily.deepseek_v4, gpt_mod.detectFamily("deepseek4"));
    try std.testing.expect(gpt_mod.isGenerativeModel("deepseek4"));
}

test "i32 index tables pass gguf tensor type inspection" {
    // DeepSeek V4 stores hash-MoE routing tables as I32. They are indices, not weights,
    // and quant_codec materializes them; inspection must not reject the file.
    try std.testing.expect(tensorTypeSupported(.{ .known = .I32 }));
}

test "unsupported families keep their gguf weight mapping" {
    // The mapping predicate is independent of the support tier. deepseek_v4 is blocked
    // for generation but its tensor names must still resolve, both because its mapping
    // tests depend on them and because blocking a family should not silently change what
    // the loader can parse.
    var buf: [256]u8 = undefined;
    try std.testing.expect(gpt_mod.ggufWeightMappingSupported(.deepseek_v4));
    try std.testing.expectEqualStrings(
        "model.layers.0.self_attn.q_a_proj.weight",
        normalizeGgufGptWeightKey(.{ .family = .deepseek_v4 }, "blk.0.attn_q_a.weight", &buf).?,
    );
}
