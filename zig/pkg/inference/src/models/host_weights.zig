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

//! Artifact-to-host preparation shared by native execution, PJRT and CUDA
//! upload. Owns normalization, lazy/mapped weights, packed representations and
//! failure rollback. No serving or session implementation dependency.
const std = @import("std");
const platform = @import("antfly_platform");
const bert = @import("bert.zig");
const gpt_mod = @import("gpt.zig");
const deberta_mod = @import("deberta.zig");
const boundary_bundle = @import("gliner_boundary_bundle.zig");
const modern_bert_arch = @import("../architectures/modern_bert.zig");
const embedding_gemma2_arch = @import("../architectures/embedding_gemma2.zig");
const deepseek_v4_arch = @import("../architectures/deepseek_v4.zig");
const gliner_encoder = @import("gliner_encoder.zig");
const gliner_boundary_model = @import("gliner_boundary.zig");
const gliner_decide_qualification = @import("gliner_decide_qualification.zig");
const manifest_mod = @import("manifest.zig");
const weight_source_mod = @import("weight_source.zig");
const LoadedWeight = @import("weight_source.zig").LoadedWeight;
const tensor_store_mod = @import("tensor_store.zig");
const gguf_mod = @import("../gguf/root.zig");
const c_file = @import("../util/c_file.zig");
const runtime = @import("../runtime/root.zig");
const backend_contracts = @import("../graph/backend_contracts.zig");
const gemma4_runtime = @import("../architectures/gemma4_runtime.zig");

pub fn directQuantEnabled() bool {
    return !platform.env.getenvBool("TERMITE_NATIVE_DISABLE_DIRECT_QUANT");
}

pub fn glinerBaseWeightKey(full_name: []const u8) []const u8 {
    if (std.mem.startsWith(u8, full_name, "encoder.embeddings.") or
        std.mem.startsWith(u8, full_name, "encoder.encoder."))
    {
        return full_name["encoder.".len..];
    }
    return full_name;
}

pub fn validateNativeBoundaryWeights(allocator: std.mem.Allocator, mf: manifest_mod.ModelManifest, config: gliner_boundary_model.Config, store: tensor_store_mod.TensorStore) !void {
    if (mf.usesGgufWeights()) {
        const receipt = mf.gliner_boundary_bundle orelse return error.UnsupportedGlinerBoundaryBundle;
        const file = store.ggufFile() orelse return error.InvalidGlinerBoundaryBundle;
        const bytes = store.ggufArtifactBytes() orelse return error.InvalidGlinerBoundaryBundle;
        _ = try @import("gliner_boundary_bundle.zig").validateLoadedGguf(allocator, receipt.value, config, file, bytes, null);
        return;
    }
    const tensor_access = @import("tensor_access.zig");
    const reader = store.singleSafetensorsReader() orelse return error.UnsupportedGlinerBoundaryBundle;
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const descriptors = try a.alloc(tensor_access.Descriptor, reader.header.tensors.count());
    var entries = reader.header.tensors.iterator();
    var index: usize = 0;
    while (entries.next()) |entry| : (index += 1) {
        const meta = entry.value_ptr.*;
        descriptors[index] = .{
            .name = entry.key_ptr.*,
            .shape = meta.shape,
            .encoding = .{ .dense = meta.dtype },
            .byte_len = @intCast(meta.data_end - meta.data_start),
            .quantized = false,
        };
    }
    _ = try @import("gliner_boundary_artifact.zig").validate(a, config.backbone, .fp32, descriptors, null);
}

pub fn captureBoundaryIdentity(mf: *const manifest_mod.ModelManifest, store: tensor_store_mod.TensorStore) !boundary_bundle.Identity {
    const config = mf.gliner_boundary_config orelse return error.InvalidGlinerBoundaryConfig;
    const sidecars = try mf.boundarySidecarDigests();
    if (mf.gliner_boundary_bundle) |receipt| {
        // validateNativeBoundaryWeights already checked the opened mapping.
        const pin = try boundary_bundle.pinFor(receipt.value.files, boundary_bundle.model_name);
        var digest = boundary_bundle.Digest{ .size_bytes = pin.size_bytes, .sha256 = undefined };
        @memcpy(&digest.sha256, pin.sha256);
        return .{ .backbone = config.backbone, .precision = receipt.value.precision, .weight = digest, .sidecars = sidecars };
    }
    const reader = store.singleSafetensorsReader() orelse return error.UnsupportedGlinerBoundaryBundle;
    return .{ .backbone = config.backbone, .precision = .fp32, .weight = boundary_bundle.Digest.ofMapped(reader.file_bytes), .sidecars = sidecars };
}

pub fn captureGlinerDecisionIdentity(mf: *const manifest_mod.ModelManifest, store: tensor_store_mod.TensorStore) !?gliner_decide_qualification.Identity {
    if (mf.gliner_architecture != .span or !mf.gliner_span_declared) return null;
    const family: gliner_decide_qualification.EncoderFamily = switch (mf.gliner_span_encoder_family) {
        .deberta => .deberta,
        .modern_bert => .modern_bert,
        .unknown => return null,
    };
    const config = mf.gliner_span_config_digest orelse return null;
    const encoder = mf.gliner_span_encoder_digest orelse return null;
    const tokenizer = mf.gliner_span_tokenizer_digest orelse return null;
    const tokenizer_config = mf.gliner_span_tokenizer_config_digest orelse return null;
    var inventory: gliner_decide_qualification.TensorInventory = undefined;
    var weight: gliner_decide_qualification.Digest = undefined;
    var companion: ?gliner_decide_qualification.Digest = null;
    if (store.singleSafetensorsReader()) |reader| {
        inventory = .{ .count = reader.header.tensors.count(), .all_f32 = true };
        var entries = reader.header.tensors.iterator();
        while (entries.next()) |entry| if (entry.value_ptr.dtype != .f32) {
            inventory.all_f32 = false;
            break;
        };
        weight = gliner_decide_qualification.Digest.ofMapped(reader.file_bytes);
    } else if (store.glinerGgufSnapshot()) |snapshot| {
        inventory = .{ .count = snapshot.tensor_count, .all_f32 = snapshot.all_f32 };
        // GGUF stores start with random-access advice for sparse tensor loads.
        // Authenticating both complete files is sequential; enable readahead
        // rather than faulting each 4 KiB page independently.
        c_file.MmapRegion.adviseBytesSequential(snapshot.encoder);
        c_file.MmapRegion.adviseBytesSequential(snapshot.head);
        weight = gliner_decide_qualification.Digest.of(snapshot.encoder);
        companion = gliner_decide_qualification.Digest.of(snapshot.head);
    } else return null;
    return .{
        .encoder_family = family,
        .geometry = .{
            .hidden_size = mf.hidden_size,
            .intermediate_size = mf.intermediate_size,
            .num_hidden_layers = mf.num_hidden_layers,
            .num_attention_heads = mf.num_attention_heads,
            .vocab_size = mf.bert_vocab_size,
            .max_position_embeddings = mf.max_position_embeddings,
        },
        .markers = .{
            .p = mf.gliner_token_p,
            .c = mf.gliner_token_c,
            .e = mf.gliner_token_e,
            .r = mf.gliner_token_r,
            .l = mf.gliner_token_l,
            .sep_struct = mf.gliner_token_sep_struct,
            .sep_text = mf.gliner_token_sep_text,
        },
        .inventory = inventory,
        .weight = weight,
        .weight_companion = companion,
        .sidecars = .{
            .config = config,
            .encoder_config = encoder,
            .tokenizer = tokenizer,
            .tokenizer_config = tokenizer_config,
            .special_tokens_map = mf.gliner_span_special_tokens_digest,
        },
    };
}

const model_definition = @import("../architectures/model_definition.zig");
const ArchConfig = model_definition.ArchConfig;

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
    packed_moe_expert_tensor_count: usize = 0,
    packed_moe_q4_0_tensor_count: usize = 0,
    a4b_packed_expert_layout_qualified: bool = false,

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

pub fn requiresModernBertSpanClassifier(manifest: manifest_mod.ModelManifest) bool {
    return manifest.gliner_architecture == .span and
        manifest.gliner_span_encoder_family == .modern_bert;
}

/// Whether a Laya checkpoint serves its linear weights as Q8_0 (LAYA.md 1d).
pub fn layaQuantizesWeights(arch_config: ArchConfig) !bool {
    if (arch_config != .modern_bert) return false;
    const config = arch_config.modern_bert.laya orelse return false;
    return try config.effectiveWeightQuantization() == .q8_0;
}

/// Load one dense Laya linear weight and quantize it to Q8_0 storage.
pub fn layaQ8Weight(allocator: std.mem.Allocator, store: tensor_store_mod.TensorStore, full_name: []const u8, name: []const u8) !LoadedWeight {
    var tensor_ref = try store.describeTensor(allocator, full_name);
    defer tensor_ref.deinit(allocator);
    var dense = try store.loadTensorRef(&tensor_ref);
    defer dense.deinit();
    const storage = try weight_source_mod.quantizeDenseQ8_0(allocator, &dense.tensor);
    return .{
        .tensor = .{ .data = &.{}, .dtype = .f32, .shape = &.{}, .name = name, .allocator = allocator, .owns_data = false, .owns_shape = false },
        .quantized = true,
        .quantized_storage = storage,
    };
}

/// Host preparation selects the final owner before installing prefetch guards.
/// CUDA upload retains packed encoder storage and materializes weights that
/// would otherwise be lazy; ordinary execution retains its CPU policy.
pub const HostPreparation = struct {
    backend: enum { native, pjrt } = .native,
    intent: enum { execute, device_upload } = .execute,
};

/// Detect the model architecture from config.json.
pub fn buildGgufInspectionReport(
    allocator: std.mem.Allocator,
    arch_config: ArchConfig,
    store: tensor_store_mod.TensorStore,
    require_modern_span_classifier: bool,
) !?GgufInspectionReport {
    if (store.kind() != .gguf) return null;
    const file = store.ggufFile() orelse return null;
    return @as(?GgufInspectionReport, try buildGgufInspectionReportFromFile(allocator, arch_config, file, require_modern_span_classifier));
}

pub fn buildGgufInspectionReportFromFile(
    allocator: std.mem.Allocator,
    arch_config: ArchConfig,
    file: *const gguf_mod.format.File,
    require_modern_span_classifier: bool,
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
    for (file.tensors) |tensor| {
        if (!isPackedMoeExpertTensor(tensor.name)) continue;
        report.packed_moe_expert_tensor_count += 1;
        if (std.meta.eql(tensor.tensor_type, gguf_mod.tensor_types.TensorType{ .known = .Q4_0 })) {
            report.packed_moe_q4_0_tensor_count += 1;
        }
    }
    report.a4b_packed_expert_layout_qualified = qualifiedA4bPackedExpertLayout(arch_config, file);

    switch (arch_config) {
        .gpt => |cfg| try collectMissingRequiredGptWeights(allocator, cfg, &normalized_names, &missing, packed_moe.items.len > 0),
        .deberta => |cfg| try collectMissingRequiredDebertaWeights(allocator, cfg, &normalized_names, &missing),
        .modern_bert => |cfg| {
            try collectMissingRequiredModernBertWeights(allocator, cfg, &normalized_names, &missing);
            if (require_modern_span_classifier)
                try collectMissingRequiredGlinerModernBertClassifier(allocator, &normalized_names, &missing);
        },
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

pub fn collectLargestDenseTensorSamples(
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

pub fn collectQuantizedTensorSamples(
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

pub fn collectTensorTypes(
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

pub fn collectNormalizedGgufNames(
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

pub fn putNameIfAbsent(allocator: std.mem.Allocator, names: *std.StringHashMapUnmanaged(void), name: []const u8) !void {
    if (names.contains(name)) return;
    try names.put(allocator, try allocator.dupe(u8, name), {});
}

pub fn shouldRecordUnmappedGgufTensor(arch_config: ArchConfig, raw_name: []const u8, normalized_name: []const u8, current_count: usize) bool {
    if (current_count >= 16) return false;
    if (!std.mem.eql(u8, raw_name, normalized_name)) return false;
    return switch (arch_config) {
        .gpt => |cfg| switch (cfg.family) {
            .llama, .mistral, .qwen2, .qwen3, .qwen3_vl, .gemma, .bitnet, .phi, .deepseek_v4 => std.mem.startsWith(u8, raw_name, "blk."),
            else => false,
        },
        else => false,
    };
}

pub fn collectMissingRequiredGptWeights(
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
        if (config.family == .qwen3 or config.family == .qwen3_vl) {
            try appendMissingFmt(allocator, names, missing, &buf, "model.layers.{d}.self_attn.q_norm.weight", .{layer});
            try appendMissingFmt(allocator, names, missing, &buf, "model.layers.{d}.self_attn.k_norm.weight", .{layer});
        }
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

pub fn collectMissingRequiredDebertaWeights(
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

pub fn collectMissingRequiredModernBertWeights(
    allocator: std.mem.Allocator,
    config: modern_bert_arch.Config,
    names: *const std.StringHashMapUnmanaged(void),
    missing: *std.ArrayListUnmanaged([]const u8),
) !void {
    try appendMissingWeight(allocator, names, missing, "model.embeddings.tok_embeddings.weight");
    try appendMissingWeight(allocator, names, missing, "model.embeddings.norm.weight");
    try appendMissingWeight(allocator, names, missing, "model.final_norm.weight");
    const fused_bias_free = config.checkpoint_layout == .huggingface_fused_qkv_no_bias;
    if (!fused_bias_free) {
        try appendMissingWeight(allocator, names, missing, "model.embeddings.norm.bias");
        try appendMissingWeight(allocator, names, missing, "model.final_norm.bias");
    }

    var buf: [256]u8 = undefined;
    for (0..config.num_hidden_layers) |layer| {
        if (!fused_bias_free or layer != 0) {
            try appendMissingFmt(allocator, names, missing, &buf, "model.layers.{d}.attn_norm.weight", .{layer});
            if (!fused_bias_free)
                try appendMissingFmt(allocator, names, missing, &buf, "model.layers.{d}.attn_norm.bias", .{layer});
        }
        try appendMissingFmt(allocator, names, missing, &buf, "model.layers.{d}.mlp_norm.weight", .{layer});
        if (!fused_bias_free)
            try appendMissingFmt(allocator, names, missing, &buf, "model.layers.{d}.mlp_norm.bias", .{layer});

        if (fused_bias_free) {
            try appendMissingFmt(allocator, names, missing, &buf, "model.layers.{d}.attn.Wqkv.weight", .{layer});
        } else {
            inline for (&.{ "query_proj", "key_proj", "value_proj" }) |projection| {
                try appendMissingFmt(allocator, names, missing, &buf, "model.layers.{d}.attn.{s}.weight", .{ layer, projection });
                try appendMissingFmt(allocator, names, missing, &buf, "model.layers.{d}.attn.{s}.bias", .{ layer, projection });
            }
        }
        try appendMissingFmt(allocator, names, missing, &buf, "model.layers.{d}.attn.Wo.weight", .{layer});
        if (!fused_bias_free)
            try appendMissingFmt(allocator, names, missing, &buf, "model.layers.{d}.attn.Wo.bias", .{layer});
        try appendMissingFmt(allocator, names, missing, &buf, "model.layers.{d}.mlp.Wi.weight", .{layer});
        try appendMissingFmt(allocator, names, missing, &buf, "model.layers.{d}.mlp.Wo.weight", .{layer});
    }
}

pub fn collectMissingRequiredGlinerModernBertClassifier(
    allocator: std.mem.Allocator,
    names: *const std.StringHashMapUnmanaged(void),
    missing: *std.ArrayListUnmanaged([]const u8),
) !void {
    inline for (&.{
        "classifier.0.weight",
        "classifier.0.bias",
        "classifier.2.weight",
        "classifier.2.bias",
    }) |name| try appendMissingWeight(allocator, names, missing, name);
}

pub fn isPackedMoeExpertTensor(raw_name: []const u8) bool {
    return std.mem.endsWith(u8, raw_name, ".ffn_gate_exps.weight") or
        std.mem.endsWith(u8, raw_name, ".ffn_down_exps.weight") or
        std.mem.endsWith(u8, raw_name, ".ffn_up_exps.weight") or
        std.mem.endsWith(u8, raw_name, ".ffn_gate_up_exps.weight");
}

/// Require one unambiguous Q4_0 routed-expert layout per A4B layer. GGUF may
/// encode gate/up as two tensors or as one interleaved gate_up tensor; mixing
/// both representations in a layer would make lazy weight registration order
/// dependent and is rejected.
pub fn qualifiedA4bPackedExpertLayout(arch_config: ArchConfig, file: *const gguf_mod.format.File) bool {
    const config = switch (arch_config) {
        .gpt => |cfg| cfg,
        else => return false,
    };
    if (!gemma4_runtime.isQualifiedA4bArchitecture(config)) return false;

    const layer_count = backend_contracts.qualified_a4b_geometries[0].moe_layer_count;
    var layer_masks: [layer_count]u8 = @splat(0);
    for (file.tensors) |tensor| {
        const packed_tensor = parsePackedMoeTensor(tensor.name) orelse continue;
        if (packed_tensor.layer >= layer_masks.len) return false;
        if (!std.meta.eql(tensor.tensor_type, gguf_mod.tensor_types.TensorType{ .known = .Q4_0 })) return false;
        if (tensor.dimensions.len != 3 or
            tensor.dimensions[2] != @as(u64, @intCast(config.num_local_experts))) return false;
        const bit: u8 = if (packed_tensor.fused_gate_up) blk: {
            if (tensor.dimensions[0] != config.hidden_size or
                tensor.dimensions[1] != @as(u64, config.expertIntermediateSize()) * 2) return false;
            break :blk 1 << 3;
        } else if (std.mem.eql(u8, packed_tensor.proj, "w1")) blk: {
            if (tensor.dimensions[0] != config.hidden_size or
                tensor.dimensions[1] != config.expertIntermediateSize()) return false;
            break :blk 1 << 0;
        } else if (std.mem.eql(u8, packed_tensor.proj, "w2")) blk: {
            if (tensor.dimensions[0] != config.expertIntermediateSize() or
                tensor.dimensions[1] != config.hidden_size) return false;
            break :blk 1 << 1;
        } else if (std.mem.eql(u8, packed_tensor.proj, "w3")) blk: {
            if (tensor.dimensions[0] != config.hidden_size or
                tensor.dimensions[1] != config.expertIntermediateSize()) return false;
            break :blk 1 << 2;
        } else return false;
        if (layer_masks[packed_tensor.layer] & bit != 0) return false;
        layer_masks[packed_tensor.layer] |= bit;
    }

    const separate_gate_up: u8 = (1 << 0) | (1 << 1) | (1 << 2);
    const fused_gate_up: u8 = (1 << 1) | (1 << 3);
    for (layer_masks) |mask| {
        if (mask != separate_gate_up and mask != fused_gate_up) return false;
    }
    return true;
}

pub fn appendMissingFmt(
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

pub fn appendMissingWeight(
    allocator: std.mem.Allocator,
    names: *const std.StringHashMapUnmanaged(void),
    missing: *std.ArrayListUnmanaged([]const u8),
    name: []const u8,
) !void {
    if (names.contains(name)) return;
    try missing.append(allocator, try allocator.dupe(u8, name));
}

pub fn tensorTypeSupported(tensor_type: gguf_mod.tensor_types.TensorType) bool {
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

pub fn ensureGgufInspectionCompatible(report: GgufInspectionReport, gguf_path: []const u8) !void {
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

pub fn normalizeWeightKey(store_kind: tensor_store_mod.StoreKind, arch_config: ArchConfig, key: []const u8, buf: *[256]u8) ![]const u8 {
    if (arch_config == .gliner_modern_bert) {
        if (std.mem.startsWith(u8, key, "encoder."))
            return std.fmt.bufPrint(buf, "model.{s}", .{key["encoder.".len..]}) catch return error.NameTooLong;
        return key;
    }
    if (arch_config == .modern_bert) {
        if (std.mem.startsWith(u8, key, "encoder."))
            return std.fmt.bufPrint(buf, "model.{s}", .{key["encoder.".len..]}) catch return error.NameTooLong;
        if (std.mem.startsWith(u8, key, "model.")) return key;
        // Laya's encoder and question heads share the runtime model namespace.
        // GLiNER's composite heads instead retain their top-level names.
        if (arch_config.modern_bert.laya != null or
            std.mem.startsWith(u8, key, "embeddings.") or
            std.mem.startsWith(u8, key, "layers.") or
            std.mem.startsWith(u8, key, "final_norm."))
        {
            return std.fmt.bufPrint(buf, "model.{s}", .{key}) catch return error.NameTooLong;
        }
        // Composite GLiNER heads share the same source artifact but remain
        // outside the nested encoder namespace.
        return key;
    }
    if (store_kind != .gguf) return key;
    return switch (arch_config) {
        .gpt => |cfg| normalizeGgufGptWeightKey(cfg, key, buf) orelse key,
        .bert => bert.normalizeGgufWeightKey(key, buf) orelse key,
        else => key,
    };
}

pub fn maybeInferGptAttentionLayoutFromStore(
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

pub fn findTensorNameByPriority(all_names: [][]const u8, candidates: []const []const u8) ?[]const u8 {
    for (candidates) |candidate| {
        for (all_names) |name| {
            if (std.mem.eql(u8, name, candidate)) return name;
        }
    }
    return null;
}

pub fn leadingTensorDim(
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

pub fn normalizeGgufGptWeightKey(config: gpt_mod.Config, key: []const u8, buf: *[256]u8) ?[]const u8 {
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

pub fn appendPackedMoeLazyWeights(
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

pub fn appendDeepseekV4MoeLazyWeights(
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

pub const PackedMoeTensor = struct {
    layer: usize,
    proj: []const u8,
    /// For fused gate+up tensors, the second projection to register.
    proj2: ?[]const u8 = null,
    /// Whether this is a fused gate+up tensor (w1+w3 interleaved in dim 1).
    fused_gate_up: bool = false,
};

pub fn parsePackedMoeTensor(full_name: []const u8) ?PackedMoeTensor {
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

pub fn normalizeGgufMoeExpertWeight(layer: usize, suffix: []const u8, prefix: []const u8, proj: []const u8, buf: *[256]u8) ?[]const u8 {
    if (!std.mem.startsWith(u8, suffix, prefix) or !std.mem.endsWith(u8, suffix, ".weight")) return null;
    const expert_str = suffix[prefix.len .. suffix.len - ".weight".len];
    if (expert_str.len == 0) return null;
    const expert = std.fmt.parseInt(usize, expert_str, 10) catch return null;
    return std.fmt.bufPrint(buf, "model.layers.{d}.block_sparse_moe.experts.{d}.{s}.weight", .{ layer, expert, proj }) catch null;
}

pub fn refineArchConfigFromWeights(arch_config: *ArchConfig, weights: *const std.StringHashMapUnmanaged(LoadedWeight)) void {
    switch (arch_config.*) {
        .gpt => |*cfg| refineGptConfigFromWeights(cfg, weights),
        else => {},
    }
}

pub fn shouldLazyLoadWeight(store_kind: tensor_store_mod.StoreKind, arch_config: ArchConfig, key: []const u8) bool {
    if (arch_config == .embedding_gemma2) return true;
    if (store_kind != .gguf) return false;
    return switch (arch_config) {
        .gpt => |cfg| cfg.usesMoe() and (std.mem.indexOf(u8, key, ".block_sparse_moe.experts.") != null or (cfg.family == .deepseek_v4 and std.mem.indexOf(u8, key, ".mlp.experts.") != null)),
        else => false,
    };
}

pub fn storeTensorIsQuantized(allocator: std.mem.Allocator, store: tensor_store_mod.TensorStore, name: []const u8) !bool {
    var ref = try store.describeTensor(allocator, name);
    defer ref.deinit(allocator);
    return ref.quantized;
}

pub fn ggufEncoderHasQuantizedTensors(store: tensor_store_mod.TensorStore) bool {
    const file = store.ggufFile() orelse return false;
    for (file.tensors) |tensor| if (tensor.tensor_type.isQuantized()) return true;
    return false;
}

pub fn shouldKeepResidentWeightQuantizedOnly(
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

    if (arch_config == .gliner_boundary) {
        const policy = @import("gliner_boundary_artifact.zig");
        for (policy.specs(arch_config.gliner_boundary.backbone)) |spec| {
            if (!std.mem.eql(u8, spec.name, source_name)) continue;
            if (policy.role(spec) != .encoder_matrix) return error.InvalidGlinerBoundaryTensorType;
            return switch (tensor_type orelse return error.InvalidGlinerBoundaryTensorType) {
                .known => |known| switch (known) {
                    .Q8_0, .Q4_0, .Q4_K => true,
                    else => return error.UnsupportedGlinerBoundaryPrecision,
                },
                else => return error.UnsupportedGlinerBoundaryPrecision,
            };
        }
        return error.InvalidGlinerBoundaryTensorName;
    }

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

pub fn shouldKeepResidentClipClapWeightQuantizedOnly(
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

pub fn isGptEmbeddingTableKey(key: []const u8) bool {
    return std.mem.eql(u8, key, "model.embed_tokens.weight") or
        std.mem.eql(u8, key, "model.per_layer_input.per_layer_token_embd.weight");
}

pub fn shouldKeepResidentGptEmbeddingQuantizedOnly(
    config: gpt_mod.Config,
    tensor_type: gguf_mod.tensor_types.TensorType,
) bool {
    return switch (config.family) {
        .gemma => isCudaResidentEmbeddingQuantType(tensor_type),
        .llama, .mistral, .qwen2, .qwen3, .bitnet => std.meta.eql(tensor_type, gguf_mod.tensor_types.TensorType{ .known = .Q8_0 }),
        else => false,
    };
}

pub fn isCudaResidentEmbeddingQuantType(tensor_type: gguf_mod.tensor_types.TensorType) bool {
    return std.meta.eql(tensor_type, gguf_mod.tensor_types.TensorType{ .known = .Q8_0 }) or
        std.meta.eql(tensor_type, gguf_mod.tensor_types.TensorType{ .known = .Q4_0 }) or
        std.meta.eql(tensor_type, gguf_mod.tensor_types.TensorType{ .known = .Q4_K }) or
        std.meta.eql(tensor_type, gguf_mod.tensor_types.TensorType{ .known = .Q6_K });
}

pub fn shouldKeepResidentGptWeightQuantizedOnly(
    config: gpt_mod.Config,
    key: []const u8,
    tensor_type: ?gguf_mod.tensor_types.TensorType,
) bool {
    // Qwen3's resident CPU/GPU paths can consume these Q8 matrices directly.
    // Keep other formats and auxiliary tensors on their existing load policy
    // until their quantized-only execution paths have been qualified.
    if (config.family == .qwen3) {
        const tt = tensor_type orelse return false;
        if (!std.meta.eql(tt, gguf_mod.tensor_types.TensorType{ .known = .Q8_0 })) return false;
        if (std.mem.eql(u8, key, "model.embed_tokens.weight") or
            std.mem.eql(u8, key, "lm_head.weight") or
            std.mem.eql(u8, key, "cls.output.weight")) return true;
        if (!std.mem.startsWith(u8, key, "model.layers.")) return false;
        return std.mem.endsWith(u8, key, ".self_attn.q_proj.weight") or
            std.mem.endsWith(u8, key, ".self_attn.k_proj.weight") or
            std.mem.endsWith(u8, key, ".self_attn.v_proj.weight") or
            std.mem.endsWith(u8, key, ".self_attn.o_proj.weight") or
            std.mem.endsWith(u8, key, ".mlp.gate_proj.weight") or
            std.mem.endsWith(u8, key, ".mlp.up_proj.weight") or
            std.mem.endsWith(u8, key, ".mlp.down_proj.weight");
    }
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

pub fn defaultResidentExpertsPerLayer(arch_config: ArchConfig) usize {
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

pub fn shouldRetainTensorStore(store_kind: tensor_store_mod.StoreKind, lazy_weight_count: usize) bool {
    // Safetensors stores mmap weights and resident tensors may borrow those
    // buffers directly, so the store must live for the entire session.
    return store_kind == .gguf or store_kind == .safetensors or lazy_weight_count > 0;
}

pub fn defaultPlanContextForBackend(backend: runtime.tier.planner.BackendClass) runtime.tier.planner.PlanContext {
    const limits = runtime.tier.memory.defaultLimitsForBackend(backend);
    return .{
        .backend = backend,
        .host_budget_bytes = limits.host_limit_bytes,
        .backend_budget_bytes = limits.backend_limit_bytes,
    };
}

pub fn parseMoeExpertCoord(key: []const u8) ?runtime.moe.residency.ExpertCoord {
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

pub fn projectionMaskForWeightKey(key: []const u8) u8 {
    if (std.mem.endsWith(u8, key, ".w1.weight")) return 0x1;
    if (std.mem.endsWith(u8, key, ".w2.weight")) return 0x2;
    if (std.mem.endsWith(u8, key, ".w3.weight")) return 0x4;
    if (std.mem.endsWith(u8, key, ".gate_proj")) return 0x1;
    if (std.mem.endsWith(u8, key, ".down_proj")) return 0x2;
    if (std.mem.endsWith(u8, key, ".up_proj")) return 0x4;
    if (std.mem.endsWith(u8, key, ".gate_up_proj")) return 0x5;
    return 0;
}

pub fn refineGptConfigFromWeights(config: *gpt_mod.Config, weights: *const std.StringHashMapUnmanaged(LoadedWeight)) void {
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
pub fn transposeGpt2Conv1dWeights(
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

pub fn transposeGpt2Conv1dLoadedWeightInPlace(
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

    // Prepare every fallible allocation before replacing either owned buffer.
    const new_shape = try allocator.alloc(i64, 2);
    new_shape[0] = @intCast(cols);
    new_shape[1] = @intCast(rows);
    if (w.owns_data and w.data.len > 0) allocator.rawFree(w.data, w.data_alignment, @returnAddress());
    w.data = std.mem.sliceAsBytes(dst);
    w.owns_data = true;
    w.data_alignment = .of(f32);

    if (w.owns_shape) allocator.free(w.shape);
    w.shape = new_shape;
    w.owns_shape = true;
}

pub const JinaLoraConfig = struct {
    rank: usize = 32,
    alpha: f32 = 32.0,

    pub fn scale(self: JinaLoraConfig) f32 {
        return self.alpha / @as(f32, @floatFromInt(self.rank));
    }
};

pub fn parseJinaLoraConfig(allocator: std.mem.Allocator, path: []const u8) !JinaLoraConfig {
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

pub fn jinaRetrievalAdapterPaths(
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

pub fn applyJinaV5RetrievalAdapterIfPresent(
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

pub fn isGpt2Conv1dWeight(key: []const u8) bool {
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

const native_mod = @import("../ops/native_compute.zig");
const gpu_hosted_store_mod = @import("../ops/gpu_hosted_store.zig");
const NativeData = native_mod.WeightStore;
const LazyWeightEntry = native_mod.LazyWeightEntry;

pub const PreparedWeights = struct {
    architecture: ArchConfig,
    weights: NativeData,
    boundary_identity: ?boundary_bundle.Identity,
    decision_identity: ?gliner_decide_qualification.Identity,
    owns_weights: bool = true,

    pub fn deinit(self: *PreparedWeights) void {
        if (!self.owns_weights) return;
        self.owns_weights = false;
        self.weights.deinitOwned();
    }

    pub fn takeWeights(self: *PreparedWeights) NativeData {
        std.debug.assert(self.owns_weights);
        self.owns_weights = false;
        return self.weights;
    }
};

pub fn prepare(allocator: std.mem.Allocator, model_path: []const u8, loaded_model: *model_definition.LoadedModel, preparation: HostPreparation) !PreparedWeights {
    var direct_quant_enabled = directQuantEnabled();
    const cpu_plan_context = defaultPlanContextForBackend(.cpu);
    if (preparation.backend == .pjrt) switch (loaded_model.architecture) {
        .embedding_gemma2 => return error.UnsupportedBackend,
        .gliner_boundary => return error.UnsupportedGlinerBoundaryBackend,
        else => {},
    };
    var mf = loaded_model.manifest;

    // Detect architecture from config.json
    var arch_config = loaded_model.architecture;
    // Determine weight prefix for the native backend (strip from source tensor names)
    var store = loaded_model.takeWeights();
    var store_owned = true;
    errdefer if (store_owned) store.deinit();
    if (arch_config == .modern_bert) {
        if (arch_config.modern_bert.laya) |config| try @import("laya.zig").validateWeights(store, config, arch_config.modern_bert);
        if (requiresModernBertSpanClassifier(mf) and store.singleSafetensorsReader() != null) {
            try gliner_encoder.validateDecisionWeights(store, arch_config.modern_bert);
            direct_quant_enabled = false;
        }
    }
    if (arch_config == .embedding_gemma2) {
        direct_quant_enabled = false;
        try embedding_gemma2_arch.validateWeights(allocator, store, arch_config.embedding_gemma2);
    }
    const laya_q8 = try layaQuantizesWeights(arch_config);
    if (arch_config == .gliner_modern_bert) {
        try gliner_encoder.validateDecisionWeights(store, arch_config.gliner_modern_bert);
        direct_quant_enabled = false;
    }
    if (arch_config == .gliner_boundary) {
        try validateNativeBoundaryWeights(allocator, mf, arch_config.gliner_boundary, store);
        // Reduced bundles retain their declared quantized storage. FP32
        // checkpoints never acquire an implicit quantization profile.
        direct_quant_enabled = if (mf.gliner_boundary_bundle) |receipt| switch (receipt.value.precision) {
            .q8_0, .q4_k, .q4_0 => true,
            else => false,
        } else false;
    }
    const boundary_identity = if (arch_config == .gliner_boundary) try captureBoundaryIdentity(&mf, store) else null;
    const gliner_decision_identity = try captureGlinerDecisionIdentity(&mf, store);
    const keep_gliner_q8 = preparation.intent == .device_upload and requiresModernBertSpanClassifier(mf) and ggufEncoderHasQuantizedTensors(store);
    if (keep_gliner_q8) {
        const identity = gliner_decision_identity orelse return error.UnsupportedGlinerDecisionArtifact;
        if (try gliner_decide_qualification.require(identity) != .decide_1b) return error.UnsupportedGlinerDecisionArtifact;
        direct_quant_enabled = true;
    }
    if (mf.usesGgufWeights()) {
        if (try buildGgufInspectionReport(allocator, arch_config, store, requiresModernBertSpanClassifier(mf))) |report| {
            defer {
                var r = report;
                r.deinit();
            }
            try ensureGgufInspectionCompatible(report, mf.gguf_path.?);
        }
    }
    const source = (try store.weightSource()) orelse return error.NoDenseWeightSource;
    const is_gliner_span_modern = mf.gliner_architecture == .span and mf.gliner_span_encoder_family == .modern_bert;

    const prefix = switch (arch_config) {
        .bert => |cfg| cfg.effectivePrefix(),
        .modern_bert => if (is_gliner_span_modern) "encoder" else "",
        .gliner_modern_bert => "",
        .nomic_bert, .embedding_gemma2 => "",
        .deberta => "deberta",
        .t5 => "", // T5 weights use full names (encoder.block.0.*, decoder.block.0.*)
        .gpt => "", // GPT weights use full names (model.layers.0.*, h.0.*)
        .whisper => "", // Whisper uses full names (encoder.*, model.decoder.*)
        .florence => "", // Florence2 uses full names (davit.*, model.decoder.*)
        .clip => "", // CLIP uses full names (text_model.*, vision_model.*)
        .clap => "", // CLAP uses full names (text_model.*, audio_model.*)
        .gliner, .gliner_boundary => "encoder", // GLiNER wraps DeBERTa encoder; span_rep/count_embed keep full names
        .layoutlmv3 => |cfg| cfg.effectivePrefix(),
    };

    // Detect prefix override from actual weight names
    const is_gliner = arch_config == .gliner or arch_config == .gliner_boundary or is_gliner_span_modern;
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
    var maps_owned = true;
    errdefer if (maps_owned) {
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
    };

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
        var key_owned = true;
        defer if (key_owned) allocator.free(owned_key);
        // CUDA transfers the resident inventory and closes this host session.
        // EmbeddingGemma 2's CPU lazy weights must be materialized here so its
        // encoder and media towers remain available after that handoff.
        const cuda_embedding_gemma2_upload = preparation.intent == .device_upload and arch_config == .embedding_gemma2;
        if (!cuda_embedding_gemma2_upload and shouldLazyLoadWeight(store.kind(), arch_config, key)) {
            if (lazy_weights.contains(key)) {
                continue;
            }
            const expert_coord = parseMoeExpertCoord(key);
            var tensor_ref = try store.describeTensor(allocator, full_name);
            errdefer tensor_ref.deinit(allocator);
            try lazy_weights.put(allocator, owned_key, .{
                .tensor_ref = tensor_ref,
                .expert_coord = expert_coord,
                .projection_mask = if (expert_coord != null) projectionMaskForWeightKey(key) else 0,
                .placement = runtime.tier.planner.planForContext(cpu_plan_context, key, tensor_ref.byte_len),
            });
            key_owned = false;
            continue;
        }

        if (laya_q8 and @import("laya.zig").quantizedLinear(full_name)) {
            var weight = try layaQ8Weight(allocator, store, full_name, owned_key);
            errdefer weight.deinit();
            try resident_weights.put(allocator, owned_key, weight);
            key_owned = false;
            continue;
        }

        if (direct_quant_enabled and ((keep_gliner_q8 and try storeTensorIsQuantized(allocator, store, full_name)) or try shouldKeepResidentWeightQuantizedOnly(allocator, store, arch_config, key, full_name))) {
            const tensor_ref = try store.describeTensor(allocator, full_name);
            defer {
                var ref = tensor_ref;
                ref.deinit(allocator);
            }
            const storage = (try store.loadQuantizedStorageRef(&tensor_ref)) orelse {
                return error.UnsupportedTensorType;
            };
            var weight: LoadedWeight = .{
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
            errdefer weight.deinit();
            try resident_weights.put(allocator, owned_key, weight);
            key_owned = false;
            continue;
        }

        var tensor_ref = store.describeTensor(allocator, full_name) catch |err| {
            if (arch_config == .gliner_boundary or err == error.OutOfMemory) return err;
            continue;
        };
        defer {
            var ref = tensor_ref;
            ref.deinit(allocator);
        }
        var weight = store.loadTensorRef(&tensor_ref) catch |err| {
            if (arch_config == .gliner_boundary or err == error.OutOfMemory) return err;
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
        key_owned = false;
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
    if (!keep_store) {
        store.deinit();
        store_owned = false;
    }
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
    var residency_owned = true;
    errdefer if (residency_owned) {
        if (residency) |value| {
            var v = value;
            v.deinit();
        }
    };

    const native_data: NativeData = .{
        .allocator = allocator,
        .resident_weights = resident_weights,
        .lazy_weights = lazy_weights,
        .tensor_store = resident_store,
        .moe_num_experts = @intCast(moe_num_experts),
        .residency = residency,
        .tier_cache = tier_cache,
        .allow_direct_quant = direct_quant_enabled,
    };
    maps_owned = false;
    store_owned = false;
    residency_owned = false;
    return .{ .architecture = arch_config, .weights = native_data, .boundary_identity = boundary_identity, .decision_identity = gliner_decision_identity };
}

pub fn inspectGgufModelForListing(
    allocator: std.mem.Allocator,
    model_path: []const u8,
    mf: manifest_mod.ModelManifest,
) !?GgufInspectionReport {
    if (!mf.usesGgufWeights()) return null;
    const gguf_path = mf.gguf_path.?;
    var mapped = try c_file.MmapRegion.init(allocator, gguf_path);
    defer mapped.deinit();
    // Listing inspection touches metadata only. Evicting the entire backing
    // file on close would destroy an intentionally warmed checkpoint before
    // the production CUDA loader gets to consume it.
    mapped.preserveFileCacheOnDeinit();

    var file = try gguf_mod.format.parseStructure(allocator, mapped.data);
    defer file.deinit(allocator);
    try gguf_mod.format.validateTensorDataRanges(&file, mapped.data.len);
    const parsed_prefix_len = std.math.cast(usize, file.data_region_offset) orelse mapped.data.len;
    mapped.adviseSequentialPrefix(@min(parsed_prefix_len, mapped.data.len));

    const arch_config = try model_definition.detectArchitectureWithGgufFile(allocator, model_path, mf, &file);
    return @as(?GgufInspectionReport, try buildGgufInspectionReportFromFile(allocator, arch_config, &file, requiresModernBertSpanClassifier(mf)));
}
