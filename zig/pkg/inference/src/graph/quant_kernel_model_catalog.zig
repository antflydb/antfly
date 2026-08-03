// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Read-only GGUF inventory for the quant-kernel compiler catalog.
//!
//! This module deliberately stops at catalog coverage. A handwritten production
//! route is still covered; promotion evidence remains the responsibility of the
//! compiler manifests and device benchmarks.

const std = @import("std");
const c_file = @import("../util/c_file.zig");
const gguf_format = @import("../gguf/format.zig");
const gguf_metadata = @import("../gguf/metadata.zig");
const tensor_types = @import("../gguf/tensor_types.zig");
const gpt = @import("../models/gpt.zig");
const quant_matmul = @import("quant_matmul.zig");
const compiler = @import("quant_kernel_compiler.zig");
const cuda_renderer = @import("quant_kernel_cuda_renderer.zig");
const aot_catalog = @import("quant_kernel_catalog.zig");
const quant_kernel_op = @import("quant_kernel_op.zig");

pub const schema = "antfly.quant_kernel.model_inventory.v1";

fn fingerprintHex(allocator: std.mem.Allocator, fingerprint: u64) ![]u8 {
    return std.fmt.allocPrint(allocator, "{x:0>16}", .{fingerprint});
}

pub const ModelDimensions = struct {
    hidden_size: u32,
    layer_count: u32,
    query_heads: u32,
    base_kv_heads: u32,
    base_head_dim: u32,
    intermediate_size: u32,
    vocabulary_size: u32,
    context_length: u32,
    local_expert_count: u32,
    experts_per_token: u32,
    shared_expert_count: u32,
    per_layer_embedding_size: u32,
};

pub const QuantFormatInventory = struct {
    gguf_tensor_type: []const u8,
    raw_tensor_type: u32,
    normalized_format: []const u8,
    tensor_count: usize,
    decode_operation_count: usize,
    quantized: bool,
    compiler_catalog_covered: bool,
    cuda_catalog_covered: bool,
    metal_catalog_covered: bool,
};

pub const AotResolutionStatus = enum {
    semantic_unsupported,
    aot_resolved,
    aot_missing,
};

pub const AotResolution = struct {
    backend: []const u8,
    architecture: []const u8,
    target_fingerprint: u64,
    target_fingerprint_hex: []const u8,
    status: AotResolutionStatus,
    kernel_id: []const u8,
    entry_fingerprint: u64,
    entry_fingerprint_hex: []const u8,
    source_fingerprint: u64,
    source_fingerprint_hex: []const u8,
    production_enabled: bool,
};

pub const OperationSignature = struct {
    signature: []const u8,
    semantic_fingerprint: u64,
    semantic_fingerprint_hex: []const u8,
    operation: []const u8,
    kernel_operation: []const u8,
    gguf_tensor_type: []const u8,
    raw_tensor_type: u32,
    normalized_format: []const u8,
    input_dim: u64,
    output_dim: u64,
    instances_per_tensor: u64,
    tensor_count: usize,
    active_decode_count: usize,
    derived: bool,
    quantized: bool,
    catalog_applicable: bool,
    catalog_required: bool,
    compiler_catalog_covered: bool,
    cuda_catalog_covered: bool,
    cuda_production_route: []const u8,
    cuda_candidate_route: []const u8,
    cuda_fallback_reason: []const u8,
    metal_catalog_covered: bool,
    metal_production_route: []const u8,
    metal_candidate_route: []const u8,
    metal_fallback_reason: []const u8,
    aot_resolutions: []AotResolution,
};

pub const AttentionTopology = struct {
    signature: []const u8,
    semantic_fingerprint: u64,
    semantic_fingerprint_hex: []const u8,
    kind: []const u8,
    layer_count: usize,
    query_heads: u32,
    kv_heads: u32,
    query_heads_per_kv_head: u32,
    head_dim: u32,
    rope_dim: u32,
    rope_active_dim: u32,
    sliding_window: u32,
    shares_kv: bool,
    compiler_catalog_covered: bool,
    cuda_catalog_covered: bool,
    cuda_candidate_kernel: []const u8,
    metal_catalog_covered: bool,
    metal_candidate_kernel: []const u8,
    aot_query_sequence_length: u32,
    aot_resolutions: []AotResolution,
};

pub const AotTargetCoverage = struct {
    backend: []const u8,
    architecture: []const u8,
    target_fingerprint: u64,
    target_fingerprint_hex: []const u8,
    operation_signature_count: usize,
    resolved_operation_signature_count: usize,
    missing_operation_signature_count: usize,
    semantic_unsupported_operation_signature_count: usize,
    operation_instance_count: u64,
    resolved_operation_instance_count: u64,
    production_operation_instance_count: u64,
    required_operation_signature_count: usize,
    resolved_required_operation_signature_count: usize,
    missing_required_operation_signature_count: usize,
    semantic_unsupported_required_operation_signature_count: usize,
    optional_operation_signature_count: usize,
    resolved_optional_operation_signature_count: usize,
    missing_optional_operation_signature_count: usize,
    semantic_unsupported_optional_operation_signature_count: usize,
    attention_topology_count: usize,
    resolved_attention_topology_count: usize,
    missing_attention_topology_count: usize,
    semantic_unsupported_attention_topology_count: usize,
    production_attention_topology_count: usize,
    all_signatures_resolved: bool,
    all_signatures_production_enabled: bool,
    all_required_signatures_resolved: bool,
    all_optional_signatures_resolved: bool,
};

pub const CoverageSummary = struct {
    complete: bool,
    normalized_operation_count: usize,
    present_quantized_operation_count: usize,
    quantized_operation_count: usize,
    unsupported_quantized_operation_count: usize,
    unsupported_attention_topology_count: usize,
    unsupported_cuda_quantized_operation_count: usize,
    unsupported_metal_quantized_operation_count: usize,
    attention_topology_count: usize,
    unsupported_cuda_attention_topology_count: usize,
    unsupported_metal_attention_topology_count: usize,
    cuda_catalog_complete: bool,
    metal_catalog_complete: bool,
    cuda_route_complete: bool,
    metal_route_complete: bool,
};

fn deinitAotResolutions(allocator: std.mem.Allocator, resolutions: []AotResolution) void {
    for (resolutions) |resolution| {
        allocator.free(resolution.target_fingerprint_hex);
        allocator.free(resolution.entry_fingerprint_hex);
        allocator.free(resolution.source_fingerprint_hex);
    }
    allocator.free(resolutions);
}

pub const ModelInventory = struct {
    schema: []const u8 = schema,
    model_path: []const u8,
    architecture: []const u8,
    family: []const u8,
    dimensions: ModelDimensions,
    quant_formats: []QuantFormatInventory,
    attention_topologies: []AttentionTopology,
    operations: []OperationSignature,
    aot_targets: []AotTargetCoverage,
    coverage: CoverageSummary,

    allocator: std.mem.Allocator = undefined,

    pub fn deinit(self: *ModelInventory) void {
        self.allocator.free(self.model_path);
        self.allocator.free(self.architecture);
        for (self.operations) |operation| {
            self.allocator.free(operation.signature);
            self.allocator.free(operation.semantic_fingerprint_hex);
            deinitAotResolutions(self.allocator, operation.aot_resolutions);
        }
        self.allocator.free(self.operations);
        for (self.attention_topologies) |topology| {
            self.allocator.free(topology.signature);
            self.allocator.free(topology.semantic_fingerprint_hex);
            deinitAotResolutions(self.allocator, topology.aot_resolutions);
        }
        self.allocator.free(self.attention_topologies);
        self.allocator.free(self.quant_formats);
        for (self.aot_targets) |target| self.allocator.free(target.target_fingerprint_hex);
        self.allocator.free(self.aot_targets);
    }
};

pub fn inspectModel(allocator: std.mem.Allocator, model_path: []const u8) !ModelInventory {
    var mapped = try c_file.MmapRegion.init(allocator, model_path);
    defer mapped.deinit();

    var parsed = try gguf_format.parse(allocator, mapped.data);
    defer parsed.deinit(allocator);
    mapped.adviseSequentialPrefix(@intCast(parsed.data_region_offset));

    const view = gguf_metadata.View.init(&parsed);
    const config = gpt.parseGgufMetadata(view) orelse return error.UnsupportedModelArchitecture;
    const architecture = view.getString("general.architecture") orelse return error.MissingModelArchitecture;
    return buildInventory(allocator, model_path, architecture, config, &parsed);
}

pub fn writeJson(io: std.Io, inventory: ModelInventory) !void {
    const stdout = std.Io.File.stdout();
    var buffer: [16 * 1024]u8 = undefined;
    var writer = stdout.writer(io, &buffer);
    const report = .{
        .schema = inventory.schema,
        .model_path = inventory.model_path,
        .architecture = inventory.architecture,
        .family = inventory.family,
        .dimensions = inventory.dimensions,
        .quant_formats = inventory.quant_formats,
        .attention_topologies = inventory.attention_topologies,
        .operations = inventory.operations,
        .aot_targets = inventory.aot_targets,
        .coverage = inventory.coverage,
    };
    try std.json.Stringify.value(report, .{ .whitespace = .indent_2 }, &writer.interface);
    try writer.interface.writeByte('\n');
    try writer.interface.flush();
}

pub fn buildInventory(
    allocator: std.mem.Allocator,
    model_path: []const u8,
    architecture: []const u8,
    config: gpt.Config,
    file: *const gguf_format.File,
) !ModelInventory {
    // Catalog ambiguity is a correctness failure. Exact target misses are
    // reported below, but must not be confused with an invalid specialization
    // registry.
    try aot_catalog.catalog.validate();
    const aot_targets = try collectAotTargets(allocator);
    defer allocator.free(aot_targets);

    var formats = std.ArrayListUnmanaged(QuantFormatInventory).empty;
    errdefer formats.deinit(allocator);
    var operations = std.ArrayListUnmanaged(OperationSignature).empty;
    errdefer {
        for (operations.items) |operation| {
            allocator.free(operation.signature);
            allocator.free(operation.semantic_fingerprint_hex);
            deinitAotResolutions(allocator, operation.aot_resolutions);
        }
        operations.deinit(allocator);
    }

    for (file.tensors) |tensor| {
        const decode_role = decodeOperationRole(tensor.name, tensor.dimensions.len, tensor.tensor_type.isQuantized());
        const operation_index = if (decode_role) |role|
            try appendOrAccumulateOperation(allocator, &operations, role, tensor, config, aot_targets)
        else
            null;
        try appendOrAccumulateFormat(&formats, tensor.tensor_type, operation_index != null, allocator);
    }
    var has_explicit_lm_head = false;
    for (operations.items) |operation| {
        if (std.mem.eql(u8, operation.operation, "lm_head")) {
            has_explicit_lm_head = true;
            break;
        }
    }
    if (config.weight_tying or !has_explicit_lm_head) {
        try appendTiedLmHeadOperation(allocator, &operations, aot_targets);
    } else {
        try appendExplicitLmHeadArgmaxOperation(allocator, &operations, aot_targets);
    }
    try appendDerivedFfnOperations(allocator, &operations, config, aot_targets);

    std.mem.sort(OperationSignature, operations.items, {}, struct {
        fn lessThan(_: void, lhs: OperationSignature, rhs: OperationSignature) bool {
            return std.mem.lessThan(u8, lhs.signature, rhs.signature);
        }
    }.lessThan);
    std.mem.sort(QuantFormatInventory, formats.items, {}, struct {
        fn lessThan(_: void, lhs: QuantFormatInventory, rhs: QuantFormatInventory) bool {
            return lhs.raw_tensor_type < rhs.raw_tensor_type;
        }
    }.lessThan);

    const topologies = try buildAttentionTopologies(allocator, config, aot_targets);
    errdefer {
        for (topologies) |topology| {
            allocator.free(topology.signature);
            allocator.free(topology.semantic_fingerprint_hex);
            deinitAotResolutions(allocator, topology.aot_resolutions);
        }
        allocator.free(topologies);
    }

    var present_quantized_operation_count: usize = 0;
    var quantized_operation_count: usize = 0;
    var unsupported_quantized_operation_count: usize = 0;
    var unsupported_cuda_quantized_operation_count: usize = 0;
    var unsupported_metal_quantized_operation_count: usize = 0;
    for (operations.items) |operation| {
        if (!operation.catalog_required) continue;
        present_quantized_operation_count += operation.tensor_count;
        quantized_operation_count += operation.active_decode_count;
        if (!operation.compiler_catalog_covered) unsupported_quantized_operation_count += operation.active_decode_count;
        if (!operation.cuda_catalog_covered) unsupported_cuda_quantized_operation_count += operation.active_decode_count;
        if (!operation.metal_catalog_covered) unsupported_metal_quantized_operation_count += operation.active_decode_count;
    }

    var unsupported_attention: usize = 0;
    var unsupported_cuda_attention: usize = 0;
    var unsupported_metal_attention: usize = 0;
    for (topologies) |topology| {
        if (!topology.compiler_catalog_covered) unsupported_attention += 1;
        if (!topology.cuda_catalog_covered) unsupported_cuda_attention += 1;
        if (!topology.metal_catalog_covered) unsupported_metal_attention += 1;
    }
    const semantic_catalog_complete = unsupported_quantized_operation_count == 0 and unsupported_attention == 0;
    const cuda_complete = semantic_catalog_complete and unsupported_cuda_quantized_operation_count == 0 and unsupported_cuda_attention == 0;
    const metal_complete = semantic_catalog_complete and unsupported_metal_quantized_operation_count == 0 and unsupported_metal_attention == 0;

    const target_coverage = try summarizeAotTargets(allocator, aot_targets, operations.items, topologies);
    errdefer {
        for (target_coverage) |target| allocator.free(target.target_fingerprint_hex);
        allocator.free(target_coverage);
    }

    const normalized_operation_count = operations.items.len;
    const owned_model_path = try allocator.dupe(u8, model_path);
    errdefer allocator.free(owned_model_path);
    const owned_architecture = try allocator.dupe(u8, architecture);
    errdefer allocator.free(owned_architecture);
    const owned_formats = try formats.toOwnedSlice(allocator);
    errdefer allocator.free(owned_formats);
    const owned_operations = try operations.toOwnedSlice(allocator);
    errdefer {
        for (owned_operations) |operation| {
            allocator.free(operation.signature);
            allocator.free(operation.semantic_fingerprint_hex);
            deinitAotResolutions(allocator, operation.aot_resolutions);
        }
        allocator.free(owned_operations);
    }

    return .{
        .model_path = owned_model_path,
        .architecture = owned_architecture,
        .family = @tagName(config.family),
        .dimensions = .{
            .hidden_size = config.hidden_size,
            .layer_count = config.num_hidden_layers,
            .query_heads = config.num_attention_heads,
            .base_kv_heads = config.effectiveKVHeads(),
            .base_head_dim = config.headDim(),
            .intermediate_size = config.intermediate_size,
            .vocabulary_size = derivedVocabularySize(config, file),
            .context_length = config.max_position_embeddings,
            .local_expert_count = config.num_local_experts,
            .experts_per_token = config.num_experts_per_tok,
            .shared_expert_count = config.num_shared_experts,
            .per_layer_embedding_size = config.ple_hidden_size,
        },
        .quant_formats = owned_formats,
        .attention_topologies = topologies,
        .operations = owned_operations,
        .aot_targets = target_coverage,
        .coverage = .{
            .complete = semantic_catalog_complete,
            .normalized_operation_count = normalized_operation_count,
            .present_quantized_operation_count = present_quantized_operation_count,
            .quantized_operation_count = quantized_operation_count,
            .unsupported_quantized_operation_count = unsupported_quantized_operation_count,
            .unsupported_attention_topology_count = unsupported_attention,
            .unsupported_cuda_quantized_operation_count = unsupported_cuda_quantized_operation_count,
            .unsupported_metal_quantized_operation_count = unsupported_metal_quantized_operation_count,
            .attention_topology_count = topologies.len,
            .unsupported_cuda_attention_topology_count = unsupported_cuda_attention,
            .unsupported_metal_attention_topology_count = unsupported_metal_attention,
            .cuda_catalog_complete = cuda_complete,
            .metal_catalog_complete = metal_complete,
            .cuda_route_complete = cuda_complete,
            .metal_route_complete = metal_complete,
        },
        .allocator = allocator,
    };
}

fn appendOrAccumulateFormat(
    formats: *std.ArrayListUnmanaged(QuantFormatInventory),
    tensor_type: tensor_types.TensorType,
    decode_operation: bool,
    allocator: std.mem.Allocator,
) !void {
    const raw = tensor_type.raw();
    for (formats.items) |*entry| {
        if (entry.raw_tensor_type != raw) continue;
        entry.tensor_count += 1;
        if (decode_operation) entry.decode_operation_count += 1;
        return;
    }

    const format = quantFormatFromTensorType(tensor_type);
    const spec = if (format) |value| compiler.specFor(value) else null;
    try formats.append(allocator, .{
        .gguf_tensor_type = tensor_type.name(),
        .raw_tensor_type = raw,
        .normalized_format = if (format) |value| @tagName(value) else "",
        .tensor_count = 1,
        .decode_operation_count = @intFromBool(decode_operation),
        .quantized = tensor_type.isQuantized(),
        .compiler_catalog_covered = !tensor_type.isQuantized() or spec != null,
        .cuda_catalog_covered = !tensor_type.isQuantized() or (spec != null and spec.?.supportsBackend(.cuda)),
        .metal_catalog_covered = !tensor_type.isQuantized() or (spec != null and spec.?.supportsBackend(.metal)),
    });
}

fn collectAotTargets(allocator: std.mem.Allocator) ![]compiler.Target {
    var targets = std.ArrayListUnmanaged(compiler.Target).empty;
    errdefer targets.deinit(allocator);
    for (aot_catalog.entries) |entry| {
        const fingerprint = compiler.targetFingerprint(entry.target);
        for (targets.items) |target| {
            if (compiler.targetFingerprint(target) == fingerprint) break;
        } else {
            try targets.append(allocator, entry.target);
        }
    }
    return targets.toOwnedSlice(allocator);
}

fn resolveAotTargets(
    allocator: std.mem.Allocator,
    semantic_signature: ?quant_kernel_op.SpecializationSignature,
    runtime: ?quant_kernel_op.RuntimeShape,
    targets: []const compiler.Target,
) ![]AotResolution {
    const resolutions = try allocator.alloc(AotResolution, targets.len);
    var initialized: usize = 0;
    errdefer {
        for (resolutions[0..initialized]) |resolution| {
            allocator.free(resolution.target_fingerprint_hex);
            allocator.free(resolution.entry_fingerprint_hex);
            allocator.free(resolution.source_fingerprint_hex);
        }
        allocator.free(resolutions);
    }

    if (semantic_signature) |signature| try signature.validate();
    for (targets, resolutions) |target, *resolution| {
        const target_fingerprint = compiler.targetFingerprint(target);
        var status: AotResolutionStatus = if (semantic_signature == null) .semantic_unsupported else .aot_missing;
        var kernel_id: []const u8 = "";
        var entry_fingerprint: u64 = 0;
        var source_fingerprint: u64 = 0;
        var production_enabled = false;
        if (semantic_signature != null and runtime != null) {
            if (try aot_catalog.catalog.resolve(.{
                .signature = semantic_signature.?,
                .runtime = runtime.?,
                .target = target,
            })) |entry| {
                status = .aot_resolved;
                kernel_id = entry.kernel_id;
                entry_fingerprint = entry.fingerprint();
                source_fingerprint = entry.source_fingerprint;
                production_enabled = entry.production_enabled;
            }
        }

        const target_fingerprint_hex = try fingerprintHex(allocator, target_fingerprint);
        const entry_fingerprint_hex = fingerprintHex(allocator, entry_fingerprint) catch |err| {
            allocator.free(target_fingerprint_hex);
            return err;
        };
        const source_fingerprint_hex = fingerprintHex(allocator, source_fingerprint) catch |err| {
            allocator.free(entry_fingerprint_hex);
            allocator.free(target_fingerprint_hex);
            return err;
        };
        resolution.* = .{
            .backend = @tagName(target.backend),
            .architecture = target.architecture,
            .target_fingerprint = target_fingerprint,
            .target_fingerprint_hex = target_fingerprint_hex,
            .status = status,
            .kernel_id = kernel_id,
            .entry_fingerprint = entry_fingerprint,
            .entry_fingerprint_hex = entry_fingerprint_hex,
            .source_fingerprint = source_fingerprint,
            .source_fingerprint_hex = source_fingerprint_hex,
            .production_enabled = production_enabled,
        };
        initialized += 1;
    }
    return resolutions;
}

fn resolutionForTarget(resolutions: []const AotResolution, target_fingerprint: u64) ?AotResolution {
    for (resolutions) |resolution| {
        if (resolution.target_fingerprint == target_fingerprint) return resolution;
    }
    return null;
}

fn summarizeAotTargets(
    allocator: std.mem.Allocator,
    targets: []const compiler.Target,
    operations: []const OperationSignature,
    topologies: []const AttentionTopology,
) ![]AotTargetCoverage {
    const summaries = try allocator.alloc(AotTargetCoverage, targets.len);
    var initialized: usize = 0;
    errdefer {
        for (summaries[0..initialized]) |summary| allocator.free(summary.target_fingerprint_hex);
        allocator.free(summaries);
    }
    for (targets, summaries) |target, *summary| {
        const target_fingerprint = compiler.targetFingerprint(target);
        const target_fingerprint_hex = try fingerprintHex(allocator, target_fingerprint);
        summary.* = .{
            .backend = @tagName(target.backend),
            .architecture = target.architecture,
            .target_fingerprint = target_fingerprint,
            .target_fingerprint_hex = target_fingerprint_hex,
            .operation_signature_count = 0,
            .resolved_operation_signature_count = 0,
            .missing_operation_signature_count = 0,
            .semantic_unsupported_operation_signature_count = 0,
            .operation_instance_count = 0,
            .resolved_operation_instance_count = 0,
            .production_operation_instance_count = 0,
            .required_operation_signature_count = 0,
            .resolved_required_operation_signature_count = 0,
            .missing_required_operation_signature_count = 0,
            .semantic_unsupported_required_operation_signature_count = 0,
            .optional_operation_signature_count = 0,
            .resolved_optional_operation_signature_count = 0,
            .missing_optional_operation_signature_count = 0,
            .semantic_unsupported_optional_operation_signature_count = 0,
            .attention_topology_count = topologies.len,
            .resolved_attention_topology_count = 0,
            .missing_attention_topology_count = 0,
            .semantic_unsupported_attention_topology_count = 0,
            .production_attention_topology_count = 0,
            .all_signatures_resolved = false,
            .all_signatures_production_enabled = false,
            .all_required_signatures_resolved = false,
            .all_optional_signatures_resolved = false,
        };
        initialized += 1;

        for (operations) |operation| {
            if (operation.aot_resolutions.len == 0 or operation.active_decode_count == 0) continue;
            const resolution = resolutionForTarget(operation.aot_resolutions, target_fingerprint) orelse return error.MissingModelAotResolution;
            const instances = try std.math.mul(
                u64,
                operation.instances_per_tensor,
                std.math.cast(u64, operation.active_decode_count) orelse return error.ModelOperationCountOverflow,
            );
            summary.operation_signature_count += 1;
            summary.operation_instance_count = try std.math.add(u64, summary.operation_instance_count, instances);
            if (operation.catalog_required) {
                summary.required_operation_signature_count += 1;
            } else {
                summary.optional_operation_signature_count += 1;
            }
            switch (resolution.status) {
                .semantic_unsupported => {
                    summary.semantic_unsupported_operation_signature_count += 1;
                    if (operation.catalog_required) {
                        summary.semantic_unsupported_required_operation_signature_count += 1;
                    } else {
                        summary.semantic_unsupported_optional_operation_signature_count += 1;
                    }
                },
                .aot_missing => {
                    summary.missing_operation_signature_count += 1;
                    if (operation.catalog_required) {
                        summary.missing_required_operation_signature_count += 1;
                    } else {
                        summary.missing_optional_operation_signature_count += 1;
                    }
                },
                .aot_resolved => {
                    summary.resolved_operation_signature_count += 1;
                    if (operation.catalog_required) {
                        summary.resolved_required_operation_signature_count += 1;
                    } else {
                        summary.resolved_optional_operation_signature_count += 1;
                    }
                    summary.resolved_operation_instance_count = try std.math.add(u64, summary.resolved_operation_instance_count, instances);
                    if (resolution.production_enabled) {
                        summary.production_operation_instance_count = try std.math.add(u64, summary.production_operation_instance_count, instances);
                    }
                },
            }
        }

        for (topologies) |topology| {
            const resolution = resolutionForTarget(topology.aot_resolutions, target_fingerprint) orelse return error.MissingModelAotResolution;
            switch (resolution.status) {
                .semantic_unsupported => summary.semantic_unsupported_attention_topology_count += 1,
                .aot_missing => summary.missing_attention_topology_count += 1,
                .aot_resolved => {
                    summary.resolved_attention_topology_count += 1;
                    if (resolution.production_enabled) summary.production_attention_topology_count += 1;
                },
            }
        }

        summary.all_signatures_resolved = summary.missing_operation_signature_count == 0 and
            summary.semantic_unsupported_operation_signature_count == 0 and
            summary.missing_attention_topology_count == 0 and
            summary.semantic_unsupported_attention_topology_count == 0;
        summary.all_signatures_production_enabled = summary.all_signatures_resolved and
            summary.production_operation_instance_count == summary.operation_instance_count and
            summary.production_attention_topology_count == summary.attention_topology_count;
        summary.all_required_signatures_resolved = summary.missing_required_operation_signature_count == 0 and
            summary.semantic_unsupported_required_operation_signature_count == 0 and
            summary.missing_attention_topology_count == 0 and
            summary.semantic_unsupported_attention_topology_count == 0;
        summary.all_optional_signatures_resolved = summary.missing_optional_operation_signature_count == 0 and
            summary.semantic_unsupported_optional_operation_signature_count == 0;
    }
    return summaries;
}

fn appendOrAccumulateOperation(
    allocator: std.mem.Allocator,
    operations: *std.ArrayListUnmanaged(OperationSignature),
    role: []const u8,
    tensor: gguf_format.TensorInfo,
    config: gpt.Config,
    aot_targets: []const compiler.Target,
) !?usize {
    if (tensor.dimensions.len < 2) return null;
    const input_dim = tensor.dimensions[0];
    const output_dim = tensor.dimensions[1];
    var instances: u64 = 1;
    for (tensor.dimensions[2..]) |dim| instances = std.math.mul(u64, instances, dim) catch return error.ModelTensorShapeOverflow;

    const format = quantFormatFromTensorType(tensor.tensor_type);
    const matmul_semantics = roleHasMatmulSemantics(role);
    const embedding_lookup = std.mem.eql(u8, role, "token_embedding") or std.mem.eql(u8, role, "per_layer_embedding");
    const catalog_required = tensor.tensor_type.isQuantized() and !embedding_lookup;
    const active_decode_count: usize = @intFromBool(decodeTensorIsActive(config, tensor.name, role));
    const normalized_format = if (format) |value| @tagName(value) else "";
    const signature = try std.fmt.allocPrint(
        allocator,
        "{s}|weight={s}|rows=1|in={d}|out={d}|instances={d}|epilogue=none",
        .{ role, if (normalized_format.len != 0) normalized_format else tensor.tensor_type.name(), input_dim, output_dim, instances },
    );
    errdefer allocator.free(signature);

    for (operations.items, 0..) |*entry, index| {
        if (!std.mem.eql(u8, entry.signature, signature)) continue;
        entry.tensor_count += 1;
        entry.active_decode_count += active_decode_count;
        allocator.free(signature);
        return index;
    }

    const quantized = tensor.tensor_type.isQuantized();
    const plan = if (format != null and matmul_semantics) quant_matmul.plan(.{
        .rows = 1,
        .in_dim = std.math.cast(usize, input_dim) orelse 0,
        .out_dim = std.math.cast(usize, output_dim) orelse 0,
        .format = format.?,
    }) else null;
    const compiler_covered = catalog_required and matmul_semantics and format != null and compiler.specFor(format.?) != null;
    const cuda = if (plan) |value| compiler.registryLoweringForPlan(.cuda, value, .none) else null;
    const metal = if (plan) |value| compiler.registryLoweringForPlan(.metal, value, .none) else null;
    const cuda_covered = catalog_required and cuda != null and cuda.?.production_route != .unsupported;
    const metal_covered = catalog_required and metal != null and metal.?.production_route != .unsupported;
    const semantic_signature: ?quant_kernel_op.SpecializationSignature = if (format != null and matmul_semantics) .{
        .small_batch_matmul = .{
            .format = format.?,
            .row_bucket = .rows_1,
            .dispatch = .mmv,
            .epilogue = .none,
        },
    } else null;
    const semantic_fingerprint = if (semantic_signature) |value|
        quant_kernel_op.specializationSignatureFingerprint(value)
    else
        0;
    const semantic_fingerprint_hex = try fingerprintHex(allocator, semantic_fingerprint);
    errdefer allocator.free(semantic_fingerprint_hex);
    const runtime: ?quant_kernel_op.RuntimeShape = if (std.math.cast(u32, input_dim)) |input|
        if (std.math.cast(u32, output_dim)) |output|
            .{ .rows = 1, .input_dim = input, .output_dim = output }
        else
            null
    else
        null;
    const aot_resolutions = if (catalog_required)
        try resolveAotTargets(allocator, if (compiler_covered) semantic_signature else null, runtime, aot_targets)
    else
        try allocator.alloc(AotResolution, 0);
    errdefer deinitAotResolutions(allocator, aot_resolutions);

    try operations.append(allocator, .{
        .signature = signature,
        .semantic_fingerprint = semantic_fingerprint,
        .semantic_fingerprint_hex = semantic_fingerprint_hex,
        .operation = role,
        .kernel_operation = if (matmul_semantics) "small_batch_matmul" else if (embedding_lookup) "embedding_lookup" else "unclassified",
        .gguf_tensor_type = tensor.tensor_type.name(),
        .raw_tensor_type = tensor.tensor_type.raw(),
        .normalized_format = normalized_format,
        .input_dim = input_dim,
        .output_dim = output_dim,
        .instances_per_tensor = instances,
        .tensor_count = 1,
        .active_decode_count = active_decode_count,
        .derived = false,
        .quantized = quantized,
        .catalog_applicable = catalog_required,
        .catalog_required = catalog_required,
        .compiler_catalog_covered = compiler_covered,
        .cuda_catalog_covered = cuda_covered,
        .cuda_production_route = if (cuda) |value| compiler.loweringRouteName(value.production_route) else "not_applicable",
        .cuda_candidate_route = if (cuda) |value| compiler.loweringRouteName(value.candidate_route) else "not_applicable",
        .cuda_fallback_reason = if (cuda) |value| compiler.fallbackReasonName(value.fallback_reason) else "not_applicable",
        .metal_catalog_covered = metal_covered,
        .metal_production_route = if (metal) |value| compiler.loweringRouteName(value.production_route) else "not_applicable",
        .metal_candidate_route = if (metal) |value| compiler.loweringRouteName(value.candidate_route) else "not_applicable",
        .metal_fallback_reason = if (metal) |value| compiler.fallbackReasonName(value.fallback_reason) else "not_applicable",
        .aot_resolutions = aot_resolutions,
    });
    return operations.items.len - 1;
}

fn activationFunction(activation: gpt.ActivationType) quant_kernel_op.ActivationFunction {
    return switch (activation) {
        .gelu => .gelu,
        .gelu_new => .gelu_new,
        .silu => .silu,
        .relu => .relu,
        .relu_squared => .relu_squared,
    };
}

fn sameFfnProjectionShape(lhs: OperationSignature, rhs: OperationSignature) bool {
    return lhs.catalog_required and rhs.catalog_required and
        lhs.raw_tensor_type == rhs.raw_tensor_type and
        lhs.input_dim == rhs.input_dim and
        lhs.output_dim == rhs.output_dim and
        lhs.instances_per_tensor == rhs.instances_per_tensor;
}

fn appendDerivedFfnOperations(
    allocator: std.mem.Allocator,
    operations: *std.ArrayListUnmanaged(OperationSignature),
    config: gpt.Config,
    aot_targets: []const compiler.Target,
) !void {
    const raw_operation_count = operations.items.len;
    for (0..raw_operation_count) |gate_index| {
        const gate = operations.items[gate_index];
        if (!std.mem.eql(u8, gate.operation, "ffn_gate_projection") or !gate.catalog_required) continue;

        var up: ?OperationSignature = null;
        for (operations.items[0..raw_operation_count]) |candidate| {
            if (std.mem.eql(u8, candidate.operation, "ffn_up_projection") and sameFfnProjectionShape(gate, candidate)) {
                up = candidate;
                break;
            }
        }
        const matched_up = up orelse continue;
        const pair_count = @min(gate.tensor_count, matched_up.tensor_count);
        const pair_active_count = @min(gate.active_decode_count, matched_up.active_decode_count);
        if (pair_count == 0) continue;
        try appendDerivedMatmulOperation(
            allocator,
            operations,
            gate,
            "ffn_gate_up_pair_activation",
            .pair_activation,
            activationFunction(config.activation),
            .q8_1,
            .q8_1,
            pair_count,
            pair_active_count,
            false,
            aot_targets,
        );
        try appendDerivedMatmulOperation(
            allocator,
            operations,
            gate,
            "ffn_gate_up_pair_activation_f32_exact",
            .pair_activation,
            activationFunction(config.activation),
            .f32,
            .f32,
            pair_count,
            pair_active_count,
            false,
            aot_targets,
        );

        for (0..raw_operation_count) |down_index| {
            const down = operations.items[down_index];
            if (!std.mem.eql(u8, down.operation, "ffn_down_projection") or
                !down.catalog_required or
                down.input_dim != gate.output_dim or
                down.output_dim != gate.input_dim or
                down.instances_per_tensor != gate.instances_per_tensor) continue;
            const down_count = @min(pair_count, down.tensor_count);
            const down_active_count = @min(pair_active_count, down.active_decode_count);
            if (down_count == 0) continue;
            try appendDerivedMatmulOperation(
                allocator,
                operations,
                down,
                "ffn_gated_down",
                .gated_down,
                .none,
                .q8_1,
                .f32,
                down_count,
                down_active_count,
                false,
                aot_targets,
            );
            try appendDerivedMatmulOperation(
                allocator,
                operations,
                down,
                "ffn_gated_down_f32_exact",
                .gated_down,
                .none,
                .f32,
                .f32,
                down_count,
                down_active_count,
                false,
                aot_targets,
            );
        }
    }
}

fn appendTiedLmHeadOperation(
    allocator: std.mem.Allocator,
    operations: *std.ArrayListUnmanaged(OperationSignature),
    aot_targets: []const compiler.Target,
) !void {
    const raw_operation_count = operations.items.len;
    for (0..raw_operation_count) |embedding_index| {
        const embedding = operations.items[embedding_index];
        if (!std.mem.eql(u8, embedding.operation, "token_embedding") or !embedding.quantized) continue;
        try appendDerivedMatmulOperation(
            allocator,
            operations,
            embedding,
            "tied_lm_head",
            .none,
            .none,
            .f32,
            .f32,
            embedding.tensor_count,
            embedding.active_decode_count,
            true,
            aot_targets,
        );
        try appendDerivedMatmulOperation(
            allocator,
            operations,
            embedding,
            "tied_lm_head_argmax",
            .argmax,
            .none,
            .q8_1,
            .i32,
            embedding.tensor_count,
            embedding.active_decode_count,
            false,
            aot_targets,
        );
    }
}

fn appendExplicitLmHeadArgmaxOperation(
    allocator: std.mem.Allocator,
    operations: *std.ArrayListUnmanaged(OperationSignature),
    aot_targets: []const compiler.Target,
) !void {
    const raw_operation_count = operations.items.len;
    for (0..raw_operation_count) |head_index| {
        const head = operations.items[head_index];
        if (!std.mem.eql(u8, head.operation, "lm_head") or !head.quantized or head.active_decode_count == 0) continue;
        try appendDerivedMatmulOperation(
            allocator,
            operations,
            head,
            "lm_head_argmax",
            .argmax,
            .none,
            .q8_1,
            .i32,
            head.tensor_count,
            head.active_decode_count,
            false,
            aot_targets,
        );
    }
}

fn appendDerivedMatmulOperation(
    allocator: std.mem.Allocator,
    operations: *std.ArrayListUnmanaged(OperationSignature),
    source: OperationSignature,
    role: []const u8,
    epilogue: quant_kernel_op.Epilogue,
    function: quant_kernel_op.ActivationFunction,
    activation: quant_kernel_op.ActivationEncoding,
    output: quant_kernel_op.OutputEncoding,
    tensor_count: usize,
    active_decode_count: usize,
    catalog_required: bool,
    aot_targets: []const compiler.Target,
) !void {
    const format = std.meta.stringToEnum(quant_matmul.Format, source.normalized_format) orelse return error.InvalidNormalizedQuantFormat;
    const semantic_signature = quant_kernel_op.SpecializationSignature{ .small_batch_matmul = .{
        .format = format,
        .row_bucket = .rows_1,
        .dispatch = .mmv,
        .epilogue = epilogue,
        .activation = activation,
        .function = function,
        .output = output,
    } };
    try semantic_signature.validate();
    const signature = try std.fmt.allocPrint(
        allocator,
        "{s}|weight={s}|rows=1|in={d}|out={d}|instances={d}|epilogue={s}|activation={s}|function={s}|output={s}",
        .{
            role,
            source.normalized_format,
            source.input_dim,
            source.output_dim,
            source.instances_per_tensor,
            @tagName(epilogue),
            @tagName(activation),
            @tagName(function),
            @tagName(output),
        },
    );
    errdefer allocator.free(signature);
    for (operations.items) |*entry| {
        if (!std.mem.eql(u8, entry.signature, signature)) continue;
        entry.tensor_count = try std.math.add(usize, entry.tensor_count, tensor_count);
        entry.active_decode_count = try std.math.add(usize, entry.active_decode_count, active_decode_count);
        allocator.free(signature);
        return;
    }

    const compiler_covered = compiler.buildIr(format, .rows_1, epilogue) != null;
    const plan = quant_matmul.plan(.{
        .rows = 1,
        .in_dim = std.math.cast(usize, source.input_dim) orelse 0,
        .out_dim = std.math.cast(usize, source.output_dim) orelse 0,
        .format = format,
    });
    const cuda = compiler.registryLoweringForPlan(.cuda, plan, epilogue);
    const metal = compiler.registryLoweringForPlan(.metal, plan, epilogue);
    const runtime: ?quant_kernel_op.RuntimeShape = if (std.math.cast(u32, source.input_dim)) |input_dim|
        if (std.math.cast(u32, source.output_dim)) |output_dim|
            .{ .rows = 1, .input_dim = input_dim, .output_dim = output_dim }
        else
            null
    else
        null;
    const aot_resolutions = try resolveAotTargets(
        allocator,
        if (compiler_covered) semantic_signature else null,
        runtime,
        aot_targets,
    );
    errdefer deinitAotResolutions(allocator, aot_resolutions);
    const semantic_fingerprint = quant_kernel_op.specializationSignatureFingerprint(semantic_signature);
    const semantic_fingerprint_hex = try fingerprintHex(allocator, semantic_fingerprint);
    errdefer allocator.free(semantic_fingerprint_hex);

    try operations.append(allocator, .{
        .signature = signature,
        .semantic_fingerprint = semantic_fingerprint,
        .semantic_fingerprint_hex = semantic_fingerprint_hex,
        .operation = role,
        .kernel_operation = "small_batch_matmul",
        .gguf_tensor_type = source.gguf_tensor_type,
        .raw_tensor_type = source.raw_tensor_type,
        .normalized_format = source.normalized_format,
        .input_dim = source.input_dim,
        .output_dim = source.output_dim,
        .instances_per_tensor = source.instances_per_tensor,
        .tensor_count = tensor_count,
        .active_decode_count = active_decode_count,
        .derived = true,
        .quantized = true,
        .catalog_applicable = true,
        .catalog_required = catalog_required,
        .compiler_catalog_covered = compiler_covered,
        .cuda_catalog_covered = cuda.production_route != .unsupported,
        .cuda_production_route = compiler.loweringRouteName(cuda.production_route),
        .cuda_candidate_route = compiler.loweringRouteName(cuda.candidate_route),
        .cuda_fallback_reason = compiler.fallbackReasonName(cuda.fallback_reason),
        .metal_catalog_covered = metal.production_route != .unsupported,
        .metal_production_route = compiler.loweringRouteName(metal.production_route),
        .metal_candidate_route = compiler.loweringRouteName(metal.candidate_route),
        .metal_fallback_reason = compiler.fallbackReasonName(metal.fallback_reason),
        .aot_resolutions = aot_resolutions,
    });
}

fn derivedVocabularySize(config: gpt.Config, file: *const gguf_format.File) u32 {
    var result = config.vocab_size;
    for (file.tensors) |tensor| {
        if (tensor.dimensions.len < 2) continue;
        if (!std.mem.eql(u8, tensor.name, "output.weight") and
            !std.mem.eql(u8, tensor.name, "token_embd.weight") and
            !std.mem.endsWith(u8, tensor.name, ".lm_head.weight") and
            !std.mem.endsWith(u8, tensor.name, ".embed_tokens.weight")) continue;
        const candidate = std.math.cast(u32, tensor.dimensions[1]) orelse continue;
        result = @max(result, candidate);
    }
    return result;
}

fn buildAttentionTopologies(
    allocator: std.mem.Allocator,
    config: gpt.Config,
    aot_targets: []const compiler.Target,
) ![]AttentionTopology {
    var topologies = std.ArrayListUnmanaged(AttentionTopology).empty;
    errdefer {
        for (topologies.items) |topology| {
            allocator.free(topology.signature);
            allocator.free(topology.semantic_fingerprint_hex);
            deinitAotResolutions(allocator, topology.aot_resolutions);
        }
        topologies.deinit(allocator);
    }

    for (0..config.num_hidden_layers) |layer| {
        const kind = attentionKindForLayer(config, layer);
        const query_heads = config.num_attention_heads;
        const kv_heads = config.effectiveKVHeadsForLayer(layer);
        const head_dim = config.effectiveHeadDimForLayer(layer);
        const heads_per_kv = if (kv_heads != 0 and query_heads % kv_heads == 0) query_heads / kv_heads else 0;
        const rope_dim = config.layerRopeDim(layer);
        const rope_active_dim = config.layerRopeActiveDim(layer);
        const sliding_window = if (std.mem.eql(u8, kind, "sliding")) config.sliding_window else 0;
        const shares_kv = config.layerSharesKv(layer);

        const signature = try std.fmt.allocPrint(
            allocator,
            "decode_attention|kind={s}|q_heads={d}|kv_heads={d}|gqa={d}|head_dim={d}|rope_dim={d}|rope_active={d}|window={d}|shared_kv={}",
            .{ kind, query_heads, kv_heads, heads_per_kv, head_dim, rope_dim, rope_active_dim, sliding_window, shares_kv },
        );
        errdefer allocator.free(signature);
        for (topologies.items) |*entry| {
            if (!std.mem.eql(u8, entry.signature, signature)) continue;
            entry.layer_count += 1;
            allocator.free(signature);
            break;
        } else {
            const cuda_kernel = attentionCandidateKernel(.cuda, kind, query_heads, head_dim, heads_per_kv);
            const metal_kernel = attentionCandidateKernel(.metal, kind, query_heads, head_dim, heads_per_kv);
            const semantic_supported = std.mem.eql(u8, kind, "global") or std.mem.eql(u8, kind, "sliding");
            const semantic_signature: ?quant_kernel_op.SpecializationSignature = if (semantic_supported)
                .{ .attention = .{ .kind = .decode_1x } }
            else
                null;
            const query_sequence_length = @max(@as(u32, 1), if (sliding_window != 0) sliding_window else config.max_position_embeddings);
            const runtime: ?quant_kernel_op.RuntimeShape = if (head_dim != 0 and query_heads != 0 and kv_heads != 0)
                .{
                    .rows = 1,
                    .head_dim = head_dim,
                    .query_heads = query_heads,
                    .kv_heads = kv_heads,
                    .sequence_length = query_sequence_length,
                }
            else
                null;
            const aot_resolutions = try resolveAotTargets(allocator, semantic_signature, runtime, aot_targets);
            errdefer deinitAotResolutions(allocator, aot_resolutions);
            const semantic_fingerprint = if (semantic_supported)
                quant_kernel_op.specializationSignatureFingerprint(.{ .attention = .{ .kind = .decode_1x } })
            else
                0;
            const semantic_fingerprint_hex = try fingerprintHex(allocator, semantic_fingerprint);
            errdefer allocator.free(semantic_fingerprint_hex);
            try topologies.append(allocator, .{
                .signature = signature,
                .semantic_fingerprint = semantic_fingerprint,
                .semantic_fingerprint_hex = semantic_fingerprint_hex,
                .kind = kind,
                .layer_count = 1,
                .query_heads = query_heads,
                .kv_heads = kv_heads,
                .query_heads_per_kv_head = heads_per_kv,
                .head_dim = head_dim,
                .rope_dim = rope_dim,
                .rope_active_dim = rope_active_dim,
                .sliding_window = sliding_window,
                .shares_kv = shares_kv,
                .compiler_catalog_covered = semantic_supported,
                .cuda_catalog_covered = cuda_kernel != null,
                .cuda_candidate_kernel = cuda_kernel orelse "",
                .metal_catalog_covered = metal_kernel != null,
                .metal_candidate_kernel = metal_kernel orelse "",
                .aot_query_sequence_length = query_sequence_length,
                .aot_resolutions = aot_resolutions,
            });
        }
    }

    std.mem.sort(AttentionTopology, topologies.items, {}, struct {
        fn lessThan(_: void, lhs: AttentionTopology, rhs: AttentionTopology) bool {
            return std.mem.lessThan(u8, lhs.signature, rhs.signature);
        }
    }.lessThan);
    return topologies.toOwnedSlice(allocator);
}

fn attentionKindForLayer(config: gpt.Config, layer: usize) []const u8 {
    if (config.layerUsesQwen35LinearAttention(layer)) return "linear";
    if (config.family == .deepseek_v4) {
        return switch (config.deepseekV4AttentionKind(layer)) {
            .sliding_attention => "sliding",
            .compressed_sparse_attention => "compressed_sparse",
            .heavily_compressed_attention => "heavily_compressed",
            .unknown => "global",
        };
    }
    return if (config.layerUsesSlidingAttention(layer)) "sliding" else "global";
}

fn attentionCandidateKernel(backend: compiler.Backend, kind: []const u8, query_heads: u32, head_dim: u32, heads_per_kv: u32) ?[]const u8 {
    if (!std.mem.eql(u8, kind, "global") and !std.mem.eql(u8, kind, "sliding")) return null;
    if (backend == .cuda and
        (query_heads == 0 or query_heads > @as(u32, cuda_renderer.generated_attention_workspace_max_query_heads))) return null;
    for (compiler.first_generated_attention_artifacts) |artifact| {
        if (artifact.backend != backend) continue;
        const op = artifact.attentionOp() orelse continue;
        if (op.kind != .decode_1x) continue;
        if (op.head_dim != 0 and @as(u32, op.head_dim) != head_dim) continue;
        if (backend == .cuda and
            (heads_per_kv == 0 or heads_per_kv > @as(u32, op.schedule.attention_query_heads_per_kv_head))) continue;
        return artifact.kernel_id;
    }
    return null;
}

fn roleHasMatmulSemantics(role: []const u8) bool {
    return std.mem.eql(u8, role, "lm_head") or
        std.mem.eql(u8, role, "attention_qkv_projection") or
        std.mem.eql(u8, role, "attention_query_projection") or
        std.mem.eql(u8, role, "attention_key_projection") or
        std.mem.eql(u8, role, "attention_value_projection") or
        std.mem.eql(u8, role, "attention_output_projection") or
        std.mem.eql(u8, role, "moe_router") or
        std.mem.eql(u8, role, "ffn_gate_up_projection") or
        std.mem.eql(u8, role, "ffn_gate_projection") or
        std.mem.eql(u8, role, "ffn_up_projection") or
        std.mem.eql(u8, role, "ffn_down_projection") or
        std.mem.eql(u8, role, "ffn_projection") or
        std.mem.eql(u8, role, "per_layer_context_projection") or
        std.mem.eql(u8, role, "per_layer_gate_projection") or
        std.mem.eql(u8, role, "per_layer_output_projection");
}

fn indexedBlockLayer(name: []const u8) ?usize {
    if (!std.mem.startsWith(u8, name, "blk.")) return null;
    const suffix = name["blk.".len..];
    const separator = std.mem.indexOfScalar(u8, suffix, '.') orelse return null;
    if (separator == 0) return null;
    return std.fmt.parseUnsigned(usize, suffix[0..separator], 10) catch null;
}

fn isIndexedBlockLeaf(name: []const u8, leaf: []const u8) bool {
    _ = indexedBlockLayer(name) orelse return false;
    const suffix = name["blk.".len..];
    const separator = std.mem.indexOfScalar(u8, suffix, '.') orelse return false;
    return std.mem.eql(u8, suffix[separator + 1 ..], leaf);
}

fn decodeTensorIsActive(config: gpt.Config, name: []const u8, role: []const u8) bool {
    if (std.mem.eql(u8, role, "lm_head") and config.weight_tying) return false;
    const layer = indexedBlockLayer(name) orelse return true;
    if (layer >= config.num_hidden_layers) return true;
    const key_projection = std.mem.eql(u8, role, "attention_key_projection");
    const value_projection = std.mem.eql(u8, role, "attention_value_projection");
    if ((key_projection or value_projection) and config.layerSharesKv(layer)) return false;
    if (value_projection and config.layerOmitsVProj(layer)) return false;
    return true;
}

fn decodeOperationRole(name: []const u8, rank: usize, quantized: bool) ?[]const u8 {
    if (rank < 2) return null;
    if (std.mem.eql(u8, name, "output.weight") or std.mem.endsWith(u8, name, ".lm_head.weight")) return "lm_head";
    if (std.mem.eql(u8, name, "token_embd.weight") or std.mem.endsWith(u8, name, ".embed_tokens.weight")) return "token_embedding";
    if (std.mem.indexOf(u8, name, "per_layer_token_embd") != null) return "per_layer_embedding";
    if (std.mem.indexOf(u8, name, "per_layer_model_proj") != null or
        std.mem.indexOf(u8, name, "per_layer_model_projection") != null) return "per_layer_context_projection";
    if (isIndexedBlockLeaf(name, "inp_gate.weight") or
        std.mem.indexOf(u8, name, ".per_layer_input.inp_gate.") != null) return "per_layer_gate_projection";
    if (isIndexedBlockLeaf(name, "proj.weight") or
        std.mem.indexOf(u8, name, ".per_layer_input.proj.") != null) return "per_layer_output_projection";
    if (std.mem.indexOf(u8, name, ".attn_qkv.") != null) return "attention_qkv_projection";
    if (std.mem.indexOf(u8, name, ".attn_q.") != null) return "attention_query_projection";
    if (std.mem.indexOf(u8, name, ".attn_k.") != null) return "attention_key_projection";
    if (std.mem.indexOf(u8, name, ".attn_v.") != null) return "attention_value_projection";
    if (std.mem.indexOf(u8, name, ".attn_output.") != null or std.mem.indexOf(u8, name, ".attn_o.") != null) return "attention_output_projection";
    if (std.mem.indexOf(u8, name, ".ffn_gate_inp.") != null or std.mem.indexOf(u8, name, ".ffn_router.") != null) return "moe_router";
    if (std.mem.indexOf(u8, name, ".ffn_gate_up.") != null) return "ffn_gate_up_projection";
    if (std.mem.indexOf(u8, name, ".ffn_gate") != null) return "ffn_gate_projection";
    if (std.mem.indexOf(u8, name, ".ffn_up") != null) return "ffn_up_projection";
    if (std.mem.indexOf(u8, name, ".ffn_down") != null) return "ffn_down_projection";
    if (std.mem.indexOf(u8, name, ".ffn_") != null) return "ffn_projection";
    if (quantized) return "unclassified_quantized_tensor";
    return null;
}

fn quantFormatFromTensorType(tensor_type: tensor_types.TensorType) ?quant_matmul.Format {
    return switch (tensor_type) {
        .known => |known| switch (known) {
            .F32 => .f32,
            .Q4_0 => .q4_0,
            .Q4_1 => .q4_1,
            .Q5_0 => .q5_0,
            .Q5_1 => .q5_1,
            .Q8_0 => .q8_0,
            .Q8_1 => .q8_1,
            .Q2_K => .q2_k,
            .Q3_K => .q3_k,
            .Q4_K => .q4_k,
            .Q5_K => .q5_k,
            .Q6_K => .q6_k,
            .Q8_K => .q8_k,
            .IQ1_S => .iq1_s,
            .IQ1_M => .iq1_m,
            .IQ2_XXS => .iq2_xxs,
            .IQ2_XS => .iq2_xs,
            .IQ2_S => .iq2_s,
            .IQ3_XXS => .iq3_xxs,
            .IQ3_S => .iq3_s,
            .IQ4_NL => .iq4_nl,
            .IQ4_XS => .iq4_xs,
            .TQ1_0 => .tq1_0,
            .TQ2_0 => .tq2_0,
            .I2_S => .i2_s,
            .I8_S => .i8_s,
            .TL1 => .tl1,
            .MXFP4 => .mxfp4,
            .NVFP4 => .nvfp4,
            .Q1_0 => .q1_0,
            .F16, .BF16, .I8, .I16, .I32, .I64, .F64 => null,
        },
        .bitnet_tl2 => .tl2,
        .unknown => null,
    };
}

fn expectFingerprintHex(value: u64, encoded: []const u8) !void {
    var buffer: [16]u8 = undefined;
    const expected = try std.fmt.bufPrint(&buffer, "{x:0>16}", .{value});
    try std.testing.expectEqualStrings(expected, encoded);
}

test "quant kernel model catalog normalizes decode operations and attention topologies" {
    const allocator = std.testing.allocator;
    var q_dims = [_]u64{ 1536, 2048 };
    var down_dims = [_]u64{ 6144, 1536 };
    var tensors = [_]gguf_format.TensorInfo{
        .{ .name = "blk.0.attn_q.weight", .dimensions = &q_dims, .tensor_type = .{ .known = .Q4_0 }, .offset = 0, .data_offset = 0 },
        .{ .name = "blk.1.attn_q.weight", .dimensions = &q_dims, .tensor_type = .{ .known = .Q4_0 }, .offset = 0, .data_offset = 0 },
        .{ .name = "blk.0.ffn_down.weight", .dimensions = &down_dims, .tensor_type = .{ .known = .Q4_0 }, .offset = 0, .data_offset = 0 },
    };
    const file = gguf_format.File{
        .header = .{ .version = 3, .tensor_count = tensors.len, .metadata_count = 0 },
        .metadata = &.{},
        .tensors = &tensors,
        .alignment = gguf_format.default_alignment,
        .data_region_offset = 0,
    };
    const config = gpt.Config{
        .family = .gemma,
        .hidden_size = 1536,
        .num_hidden_layers = 4,
        .num_attention_heads = 8,
        .num_key_value_heads = 1,
        .attention_head_dim = 256,
        .global_head_dim = 512,
        .intermediate_size = 6144,
        .sliding_window = 512,
        .sliding_window_pattern = 2,
        .rope_partial_factor = 1.0,
    };

    var inventory = try buildInventory(allocator, "model.gguf", "gemma4", config, &file);
    defer inventory.deinit();
    try std.testing.expectEqual(@as(usize, 2), inventory.operations.len);
    try std.testing.expectEqualStrings("attention_query_projection", inventory.operations[0].operation);
    try std.testing.expectEqual(@as(usize, 2), inventory.operations[0].tensor_count);
    try std.testing.expect(inventory.operations[0].compiler_catalog_covered);
    try std.testing.expect(inventory.operations[0].semantic_fingerprint != 0);
    try std.testing.expectEqual(@as(usize, 16), inventory.operations[0].semantic_fingerprint_hex.len);
    try expectFingerprintHex(inventory.operations[0].semantic_fingerprint, inventory.operations[0].semantic_fingerprint_hex);
    try std.testing.expectEqual(@as(usize, 2), inventory.attention_topologies.len);
    try std.testing.expect(inventory.coverage.complete);
    try std.testing.expect(inventory.attention_topologies[0].semantic_fingerprint != 0);
    try std.testing.expectEqual(@as(usize, 16), inventory.attention_topologies[0].semantic_fingerprint_hex.len);
    try std.testing.expectEqual(@as(usize, 16), inventory.aot_targets[0].target_fingerprint_hex.len);
    try expectFingerprintHex(inventory.aot_targets[0].target_fingerprint, inventory.aot_targets[0].target_fingerprint_hex);
}

test "quant kernel model catalog emits canonical fingerprint hex" {
    const encoded = try fingerprintHex(std.testing.allocator, 0xfedc_ba98_7654_3210);
    defer std.testing.allocator.free(encoded);
    try std.testing.expectEqualStrings("fedcba9876543210", encoded);
}

test "quant kernel model catalog owns empty AOT resolution slices" {
    const resolutions = try resolveAotTargets(std.testing.allocator, null, null, &.{});
    defer deinitAotResolutions(std.testing.allocator, resolutions);
    try std.testing.expectEqual(@as(usize, 0), resolutions.len);
}

test "quant kernel model catalog rejects unknown quantized formats from coverage" {
    const allocator = std.testing.allocator;
    var dims = [_]u64{ 64, 64 };
    var tensors = [_]gguf_format.TensorInfo{.{
        .name = "blk.0.attn_q.weight",
        .dimensions = &dims,
        .tensor_type = .{ .unknown = 999 },
        .offset = 0,
        .data_offset = 0,
    }};
    const file = gguf_format.File{
        .header = .{ .version = 3, .tensor_count = 1, .metadata_count = 0 },
        .metadata = &.{},
        .tensors = &tensors,
        .alignment = gguf_format.default_alignment,
        .data_region_offset = 0,
    };
    const config = gpt.Config{
        .family = .llama,
        .hidden_size = 512,
        .num_hidden_layers = 1,
        .num_attention_heads = 8,
        .num_key_value_heads = 1,
        .attention_head_dim = 64,
    };

    var inventory = try buildInventory(allocator, "unknown.gguf", "llama", config, &file);
    defer inventory.deinit();
    try std.testing.expect(!inventory.coverage.complete);
    try std.testing.expectEqual(@as(usize, 1), inventory.coverage.unsupported_quantized_operation_count);
    try std.testing.expect(!inventory.operations[0].compiler_catalog_covered);
}

test "quant kernel model catalog reports exact AOT and derived E2B FFN signatures" {
    const allocator = std.testing.allocator;
    var output_dims = [_]u64{ 2048, 1536 };
    var ffn_dims = [_]u64{ 1536, 6144 };
    var down_dims = [_]u64{ 6144, 1536 };
    var tensors = [_]gguf_format.TensorInfo{
        .{ .name = "blk.0.attn_output.weight", .dimensions = &output_dims, .tensor_type = .{ .known = .Q4_0 }, .offset = 0, .data_offset = 0 },
        .{ .name = "blk.0.ffn_gate.weight", .dimensions = &ffn_dims, .tensor_type = .{ .known = .Q4_0 }, .offset = 0, .data_offset = 0 },
        .{ .name = "blk.0.ffn_up.weight", .dimensions = &ffn_dims, .tensor_type = .{ .known = .Q4_0 }, .offset = 0, .data_offset = 0 },
        .{ .name = "blk.0.ffn_down.weight", .dimensions = &down_dims, .tensor_type = .{ .known = .Q4_0 }, .offset = 0, .data_offset = 0 },
    };
    const file = gguf_format.File{
        .header = .{ .version = 3, .tensor_count = tensors.len, .metadata_count = 0 },
        .metadata = &.{},
        .tensors = &tensors,
        .alignment = gguf_format.default_alignment,
        .data_region_offset = 0,
    };
    const config = gpt.Config{
        .family = .gemma,
        .hidden_size = 1536,
        .num_hidden_layers = 1,
        .num_attention_heads = 8,
        .num_key_value_heads = 1,
        .attention_head_dim = 256,
        .intermediate_size = 6144,
        .max_position_embeddings = 4096,
        .activation = .gelu_new,
    };
    var model_path = "model.gguf".*;
    var inventory = try buildInventory(allocator, &model_path, "gemma4", config, &file);
    defer inventory.deinit();
    model_path[0] = 'X';
    try std.testing.expectEqualStrings("model.gguf", inventory.model_path);
    try std.testing.expect(inventory.coverage.complete);
    try std.testing.expectEqual(@as(usize, 1), inventory.aot_targets.len);
    try std.testing.expectEqualStrings("cuda", inventory.aot_targets[0].backend);
    try std.testing.expectEqualStrings("sm_89", inventory.aot_targets[0].architecture);

    var saw_production_projection = false;
    var saw_pair = false;
    var saw_down = false;
    var saw_exact_pair = false;
    var saw_exact_down = false;
    for (inventory.operations) |operation| {
        try expectFingerprintHex(operation.semantic_fingerprint, operation.semantic_fingerprint_hex);
        if (operation.aot_resolutions.len != 0) {
            const resolution = operation.aot_resolutions[0];
            try expectFingerprintHex(resolution.target_fingerprint, resolution.target_fingerprint_hex);
            try expectFingerprintHex(resolution.entry_fingerprint, resolution.entry_fingerprint_hex);
            try expectFingerprintHex(resolution.source_fingerprint, resolution.source_fingerprint_hex);
        }
        if (std.mem.eql(u8, operation.operation, "attention_output_projection")) {
            try std.testing.expectEqual(AotResolutionStatus.aot_resolved, operation.aot_resolutions[0].status);
            try std.testing.expect(operation.aot_resolutions[0].production_enabled);
            saw_production_projection = true;
        } else if (std.mem.eql(u8, operation.operation, "ffn_gate_up_pair_activation")) {
            try std.testing.expect(operation.derived);
            try std.testing.expectEqual(AotResolutionStatus.aot_resolved, operation.aot_resolutions[0].status);
            try std.testing.expect(!operation.aot_resolutions[0].production_enabled);
            saw_pair = true;
        } else if (std.mem.eql(u8, operation.operation, "ffn_gated_down")) {
            try std.testing.expect(operation.derived);
            try std.testing.expectEqual(AotResolutionStatus.aot_resolved, operation.aot_resolutions[0].status);
            try std.testing.expect(!operation.aot_resolutions[0].production_enabled);
            saw_down = true;
        } else if (std.mem.eql(u8, operation.operation, "ffn_gate_up_pair_activation_f32_exact")) {
            try std.testing.expect(operation.derived);
            try std.testing.expect(!operation.catalog_required);
            try std.testing.expectEqual(AotResolutionStatus.aot_resolved, operation.aot_resolutions[0].status);
            try std.testing.expect(!operation.aot_resolutions[0].production_enabled);
            try std.testing.expectEqualStrings(
                compiler.first_e2b_cuda_q4_0_pair_f32_6144_exact_kernel_id,
                operation.aot_resolutions[0].kernel_id,
            );
            saw_exact_pair = true;
        } else if (std.mem.eql(u8, operation.operation, "ffn_gated_down_f32_exact")) {
            try std.testing.expect(operation.derived);
            try std.testing.expect(!operation.catalog_required);
            try std.testing.expectEqual(AotResolutionStatus.aot_resolved, operation.aot_resolutions[0].status);
            try std.testing.expect(!operation.aot_resolutions[0].production_enabled);
            try std.testing.expectEqualStrings(
                compiler.first_e2b_cuda_q4_0_down_f32_6144_exact_kernel_id,
                operation.aot_resolutions[0].kernel_id,
            );
            saw_exact_down = true;
        }
    }
    try std.testing.expect(saw_production_projection);
    try std.testing.expect(saw_pair);
    try std.testing.expect(saw_down);
    try std.testing.expect(saw_exact_pair);
    try std.testing.expect(saw_exact_down);
    try std.testing.expectEqual(AotResolutionStatus.aot_resolved, inventory.attention_topologies[0].aot_resolutions[0].status);
    try expectFingerprintHex(
        inventory.attention_topologies[0].aot_resolutions[0].source_fingerprint,
        inventory.attention_topologies[0].aot_resolutions[0].source_fingerprint_hex,
    );
    try std.testing.expectEqual(@as(u32, 4096), inventory.attention_topologies[0].aot_query_sequence_length);
}

test "quant kernel model catalog recognizes common GGUF decode tensor roles" {
    try std.testing.expectEqualStrings("lm_head", decodeOperationRole("output.weight", 2, true).?);
    try std.testing.expectEqualStrings("attention_qkv_projection", decodeOperationRole("blk.3.attn_qkv.weight", 2, true).?);
    try std.testing.expectEqualStrings("ffn_gate_projection", decodeOperationRole("blk.2.ffn_gate_shexp.weight", 2, true).?);
    try std.testing.expectEqualStrings("per_layer_context_projection", decodeOperationRole("per_layer_model_proj.weight", 2, true).?);
    try std.testing.expectEqualStrings("per_layer_gate_projection", decodeOperationRole("blk.12.inp_gate.weight", 2, true).?);
    try std.testing.expectEqualStrings("per_layer_output_projection", decodeOperationRole("blk.12.proj.weight", 2, true).?);
    try std.testing.expectEqualStrings("unclassified_quantized_tensor", decodeOperationRole("blk.12.indexer.proj.weight", 2, true).?);
    try std.testing.expectEqualStrings("unclassified_quantized_tensor", decodeOperationRole("custom.weight", 2, true).?);
    try std.testing.expect(decodeOperationRole("blk.0.attn_norm.weight", 1, false) == null);
}

test "quant kernel model catalog excludes shared KV projections from active work" {
    const config = gpt.Config{
        .family = .gemma,
        .num_hidden_layers = 4,
        .num_kv_shared_layers = 2,
    };
    try std.testing.expect(decodeTensorIsActive(config, "blk.1.attn_k.weight", "attention_key_projection"));
    try std.testing.expect(!decodeTensorIsActive(config, "blk.2.attn_k.weight", "attention_key_projection"));
    try std.testing.expect(!decodeTensorIsActive(config, "blk.3.attn_v.weight", "attention_value_projection"));
    try std.testing.expect(decodeTensorIsActive(config, "blk.3.attn_q.weight", "attention_query_projection"));
    try std.testing.expect(decodeTensorIsActive(config, "blk.bad.attn_k.weight", "attention_key_projection"));
    var tied_config = config;
    tied_config.weight_tying = true;
    try std.testing.expect(!decodeTensorIsActive(tied_config, "output.weight", "lm_head"));
}

test "quant kernel model catalog preserves mixed-format gated-down candidates" {
    const allocator = std.testing.allocator;
    var ffn_dims = [_]u64{ 2560, 10_240 };
    var down_dims = [_]u64{ 10_240, 2560 };
    var tensors = [_]gguf_format.TensorInfo{
        .{ .name = "blk.0.ffn_gate.weight", .dimensions = &ffn_dims, .tensor_type = .{ .known = .Q4_K }, .offset = 0, .data_offset = 0 },
        .{ .name = "blk.1.ffn_gate.weight", .dimensions = &ffn_dims, .tensor_type = .{ .known = .Q4_K }, .offset = 0, .data_offset = 0 },
        .{ .name = "blk.0.ffn_up.weight", .dimensions = &ffn_dims, .tensor_type = .{ .known = .Q4_K }, .offset = 0, .data_offset = 0 },
        .{ .name = "blk.1.ffn_up.weight", .dimensions = &ffn_dims, .tensor_type = .{ .known = .Q4_K }, .offset = 0, .data_offset = 0 },
        .{ .name = "blk.0.ffn_down.weight", .dimensions = &down_dims, .tensor_type = .{ .known = .Q4_K }, .offset = 0, .data_offset = 0 },
        .{ .name = "blk.1.ffn_down.weight", .dimensions = &down_dims, .tensor_type = .{ .known = .Q6_K }, .offset = 0, .data_offset = 0 },
    };
    const file = gguf_format.File{
        .header = .{ .version = 3, .tensor_count = tensors.len, .metadata_count = 0 },
        .metadata = &.{},
        .tensors = &tensors,
        .alignment = gguf_format.default_alignment,
        .data_region_offset = 0,
    };
    const config = gpt.Config{
        .family = .gemma,
        .hidden_size = 2560,
        .num_hidden_layers = 2,
        .num_attention_heads = 8,
        .num_key_value_heads = 2,
        .attention_head_dim = 256,
        .intermediate_size = 10_240,
        .activation = .gelu_new,
    };

    var inventory = try buildInventory(allocator, "mixed-ffn.gguf", "gemma4", config, &file);
    defer inventory.deinit();
    var saw_q4_down = false;
    var saw_q6_down = false;
    for (inventory.operations) |operation| {
        if (!std.mem.eql(u8, operation.operation, "ffn_gated_down")) continue;
        try std.testing.expectEqual(@as(usize, 1), operation.tensor_count);
        try std.testing.expectEqual(@as(usize, 1), operation.active_decode_count);
        if (std.mem.eql(u8, operation.normalized_format, "q4_k")) saw_q4_down = true;
        if (std.mem.eql(u8, operation.normalized_format, "q6_k")) saw_q6_down = true;
    }
    try std.testing.expect(saw_q4_down);
    try std.testing.expect(saw_q6_down);
}

test "quant kernel model catalog infers tied Q6 LM head and greedy candidate" {
    const allocator = std.testing.allocator;
    var embedding_dims = [_]u64{ 2560, 262_144 };
    var tensors = [_]gguf_format.TensorInfo{.{
        .name = "token_embd.weight",
        .dimensions = &embedding_dims,
        .tensor_type = .{ .known = .Q6_K },
        .offset = 0,
        .data_offset = 0,
    }};
    const file = gguf_format.File{
        .header = .{ .version = 3, .tensor_count = tensors.len, .metadata_count = 0 },
        .metadata = &.{},
        .tensors = &tensors,
        .alignment = gguf_format.default_alignment,
        .data_region_offset = 0,
    };
    const config = gpt.Config{
        .family = .gemma,
        .hidden_size = 2560,
        .vocab_size = 262_144,
        .weight_tying = false,
    };

    var inventory = try buildInventory(allocator, "tied-q6.gguf", "gemma4", config, &file);
    defer inventory.deinit();
    var saw_logits = false;
    var saw_argmax = false;
    for (inventory.operations) |operation| {
        if (std.mem.eql(u8, operation.operation, "tied_lm_head")) {
            try std.testing.expectEqual(AotResolutionStatus.aot_missing, operation.aot_resolutions[0].status);
            saw_logits = true;
        } else if (std.mem.eql(u8, operation.operation, "tied_lm_head_argmax")) {
            try std.testing.expectEqual(AotResolutionStatus.aot_resolved, operation.aot_resolutions[0].status);
            try std.testing.expectEqualStrings(
                compiler.first_cuda_q6_k_q8_1_argmax_k2560_kernel_id,
                operation.aot_resolutions[0].kernel_id,
            );
            try std.testing.expect(!operation.aot_resolutions[0].production_enabled);
            saw_argmax = true;
        }
    }
    try std.testing.expect(saw_logits);
    try std.testing.expect(saw_argmax);
    try std.testing.expect(inventory.coverage.complete);
}

test "quant kernel model catalog derives tied heads across operation list growth" {
    const allocator = std.testing.allocator;
    var embedding_dims = [_]u64{ 2560, 262_144 };
    var projection_dims = [_]u64{ 2560, 2048 };
    const embedding = gguf_format.TensorInfo{
        .name = "token_embd.weight",
        .dimensions = &embedding_dims,
        .tensor_type = .{ .known = .Q6_K },
        .offset = 0,
        .data_offset = 0,
    };
    const projection = gguf_format.TensorInfo{
        .name = "blk.0.attn_q.weight",
        .dimensions = &projection_dims,
        .tensor_type = .{ .known = .Q4_K },
        .offset = 0,
        .data_offset = 0,
    };
    const config = gpt.Config{ .family = .gemma, .hidden_size = 2560 };

    var operations = std.ArrayListUnmanaged(OperationSignature).empty;
    defer {
        for (operations.items) |operation| {
            allocator.free(operation.signature);
            allocator.free(operation.semantic_fingerprint_hex);
            deinitAotResolutions(allocator, operation.aot_resolutions);
        }
        operations.deinit(allocator);
    }
    try operations.ensureTotalCapacityPrecise(allocator, 2);
    _ = try appendOrAccumulateOperation(allocator, &operations, "token_embedding", embedding, config, &.{});
    _ = try appendOrAccumulateOperation(allocator, &operations, "attention_query_projection", projection, config, &.{});
    try std.testing.expectEqual(operations.items.len, operations.capacity);

    try appendTiedLmHeadOperation(allocator, &operations, &.{});
    try std.testing.expectEqual(@as(usize, 4), operations.items.len);
    try std.testing.expectEqualStrings("attention_query_projection", operations.items[1].operation);
    try std.testing.expectEqualStrings("tied_lm_head", operations.items[2].operation);
    try std.testing.expectEqualStrings("tied_lm_head_argmax", operations.items[3].operation);
}

test "quant kernel model catalog derives greedy candidate from explicit LM head" {
    const allocator = std.testing.allocator;
    var head_dims = [_]u64{ 2560, 262_144 };
    var tensors = [_]gguf_format.TensorInfo{.{
        .name = "output.weight",
        .dimensions = &head_dims,
        .tensor_type = .{ .known = .Q6_K },
        .offset = 0,
        .data_offset = 0,
    }};
    const file = gguf_format.File{
        .header = .{ .version = 3, .tensor_count = tensors.len, .metadata_count = 0 },
        .metadata = &.{},
        .tensors = &tensors,
        .alignment = gguf_format.default_alignment,
        .data_region_offset = 0,
    };
    const config = gpt.Config{
        .family = .gemma,
        .hidden_size = 2560,
        .vocab_size = 262_144,
        .weight_tying = false,
    };

    var inventory = try buildInventory(allocator, "explicit-q6.gguf", "gemma4", config, &file);
    defer inventory.deinit();
    var saw_explicit_head = false;
    var saw_argmax = false;
    for (inventory.operations) |operation| {
        if (std.mem.eql(u8, operation.operation, "lm_head")) {
            try std.testing.expectEqual(@as(usize, 1), operation.active_decode_count);
            saw_explicit_head = true;
        } else if (std.mem.eql(u8, operation.operation, "lm_head_argmax")) {
            try std.testing.expect(operation.derived);
            try std.testing.expectEqual(AotResolutionStatus.aot_resolved, operation.aot_resolutions[0].status);
            try std.testing.expectEqualStrings(
                compiler.first_cuda_q6_k_q8_1_argmax_k2560_kernel_id,
                operation.aot_resolutions[0].kernel_id,
            );
            saw_argmax = true;
        }
    }
    try std.testing.expect(saw_explicit_head);
    try std.testing.expect(saw_argmax);
}

test "quant kernel model catalog sees typed CUDA attention artifacts" {
    try std.testing.expect(attentionCandidateKernel(.cuda, "sliding", 32, 256, 8) != null);
    try std.testing.expect(attentionCandidateKernel(.cuda, "global", 32, 512, 8) != null);
    try std.testing.expect(attentionCandidateKernel(.cuda, "global", 33, 512, 8) == null);
    try std.testing.expect(attentionCandidateKernel(.metal, "global", 40, 256, 8) != null);
    try std.testing.expect(attentionCandidateKernel(.cuda, "linear", 8, 256, 8) == null);
}
