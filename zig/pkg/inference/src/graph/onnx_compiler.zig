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

//! ONNX partition compiler.
//!
//! Builds an exportable partition-local graph, lowers fused ops to standard
//! ONNX primitives, and serializes the result for ONNX Runtime execution.

const std = @import("std");
const ml = @import("ml");
const onnx_graph = @import("onnx_graph");
const platform = @import("antfly_platform");

const Allocator = std.mem.Allocator;
const Builder = ml.graph.Builder;
const Graph = ml.graph.Graph;
const NodeId = ml.graph.NodeId;

const ops_mod = @import("../ops/ops.zig");
const contracts = @import("backend_contracts.zig");
const ComputeBackend = ops_mod.ComputeBackend;
const ExportTensorData = ops_mod.ExportTensorData;
const partition_mod = @import("partition.zig");
const Partition = partition_mod.Partition;
const partition_export = @import("partition_export.zig");
const export_source_mod = @import("../models/export_source.zig");
const c_file = @import("../util/c_file.zig");
const ParameterInitializer = onnx_graph.ParameterInitializer;
const ParameterInitializerProvider = onnx_graph.ParameterInitializerProvider;
const ParameterInitializerData = @TypeOf(@as(ParameterInitializer, undefined).data);
const BuiltInitializer = struct {
    shape: ml.graph.Shape,
    data: ParameterInitializerData,
};

pub const QuantExportMode = enum {
    dense,
    q8_0_weight_only,
};

pub const WeightExportRule = struct {
    substring: []const u8,
    mode: QuantExportMode,
};

pub const WeightExportPolicy = struct {
    default_mode: QuantExportMode = .dense,
    rules: []const WeightExportRule = &.{},

    pub fn modeForName(self: WeightExportPolicy, name: []const u8) QuantExportMode {
        var mode = self.default_mode;
        for (self.rules) |rule| {
            if (std.mem.indexOf(u8, name, rule.substring) != null) mode = rule.mode;
        }
        return mode;
    }
};

fn onnxExportProfileEnabled() bool {
    return platform.env.getenv("TERMITE_ONNX_EXPORT_PROFILE") != null;
}

fn onnxExportEstimateOnlyEnabled() bool {
    return platform.env.getenv("TERMITE_ONNX_EXPORT_ESTIMATE_ONLY") != null;
}

fn nowNs() u64 {
    var ts: std.posix.timespec = undefined;
    switch (std.posix.errno(std.posix.system.clock_gettime(std.posix.CLOCK.MONOTONIC, &ts))) {
        .SUCCESS => return @as(u64, @intCast(ts.sec)) * std.time.ns_per_s + @as(u64, @intCast(ts.nsec)),
        else => return 0,
    }
}

fn serializedTensorBytes(shape: ml.graph.Shape, element_count: usize) usize {
    return switch (shape.dtype) {
        .f16, .bf16 => element_count * 2,
        .f32 => element_count * 4,
        .f64 => element_count * 8,
        .i8, .i16 => element_count * shape.dtype.byteSize(),
        .i32 => element_count * 4,
        .i64 => element_count * 8,
        .u8, .bool_ => element_count,
    };
}

pub const CompileResult = struct {
    onnx_bytes: []u8,
    external_data_path: ?[]u8 = null,
    external_data_bytes: []u8 = &.{},
    input_node_ids: []NodeId,
    input_names: [][]u8 = &.{},
    output_node_ids: []NodeId,
    output_names: [][]u8 = &.{},
    allocator: Allocator,

    pub fn deinit(self: *CompileResult) void {
        self.allocator.free(self.onnx_bytes);
        if (self.external_data_path) |path| self.allocator.free(path);
        if (self.external_data_bytes.len > 0) self.allocator.free(self.external_data_bytes);
        self.allocator.free(self.input_node_ids);
        for (self.input_names) |name| self.allocator.free(name);
        if (self.input_names.len > 0) self.allocator.free(self.input_names);
        self.allocator.free(self.output_node_ids);
        for (self.output_names) |name| self.allocator.free(name);
        if (self.output_names.len > 0) self.allocator.free(self.output_names);
    }
};

pub const CompileOptions = struct {
    /// Build a decoder-style ONNX ABI from a single-token decode graph:
    /// `input_ids`, `logits`, and synthetic past/present K/V around each GQA.
    semantic_decoder_entrypoint: bool = false,
    /// Overrides keyed by node ids in the final sorted export graph. Callers
    /// that start from traced graph ids must map through subgraph extraction,
    /// lowering, and topological sort before populating this list.
    node_name_overrides: []const onnx_graph.NodeNameOverride = &.{},
    /// Semantic decoder bindings keyed by final sorted export graph node ids.
    semantic_decoder_gqa_bindings: []const onnx_graph.SemanticDecoderGqaBinding = &.{},
};

pub const ExportFootprint = struct {
    parameter_count: usize = 0,
    skipped_parameter_count: usize = 0,
    dense_policy_parameter_count: usize = 0,
    q8_0_policy_parameter_count: usize = 0,
    dense_source_parameter_count: usize = 0,
    quantized_source_parameter_count: usize = 0,
    q8_0_candidate_parameter_count: usize = 0,
    q8_0_candidate_serialized_bytes: usize = 0,
    loaded_bytes: usize = 0,
    serialized_bytes: usize = 0,
    policy_rule_match_counts: []usize = &.{},
    skipped_parameter_names: std.ArrayListUnmanaged([]const u8) = .empty,

    pub fn deinit(self: *ExportFootprint, allocator: Allocator) void {
        if (self.policy_rule_match_counts.len > 0) allocator.free(self.policy_rule_match_counts);
        for (self.skipped_parameter_names.items) |name| allocator.free(name);
        self.skipped_parameter_names.deinit(allocator);
    }
};

pub fn compilePartition(
    allocator: Allocator,
    graph: *const Graph,
    part: *const Partition,
    cb: *const ComputeBackend,
    weight_export_source: ?export_source_mod.Source,
    weight_export_policy: WeightExportPolicy,
    external_data_name: ?[]const u8,
    external_data_absolute_path: ?[]const u8,
    reuse_initializers_from_onnx_path: ?[]const u8,
    extra_output_node_ids: []const NodeId,
    options: CompileOptions,
) !CompileResult {
    const profile_enabled = onnxExportProfileEnabled();
    const estimate_only = onnxExportEstimateOnlyEnabled();
    const started_ns = if (profile_enabled) nowNs() else 0;
    var subgraph = try partition_export.buildExportableSubgraph(
        allocator,
        graph,
        part,
        cb,
        true,
        extra_output_node_ids,
    );
    defer subgraph.deinit();
    const extracted_ns = if (profile_enabled) nowNs() else 0;

    try synthesizeLinearDecompositions(&subgraph.graph, .{ .preserve_gqa = options.semantic_decoder_entrypoint });
    const synthesized_ns = if (profile_enabled) nowNs() else 0;

    var lowered_export = try ml.graph.lower.lower(allocator, &subgraph.graph);
    defer lowered_export.deinit();
    const lowered_ns = if (profile_enabled) nowNs() else 0;

    var sorted_export = try topologicallySortGraph(allocator, &lowered_export.graph);
    defer sorted_export.deinit();
    const sorted_ns = if (profile_enabled) nowNs() else 0;

    var export_runtime_input_node_ids = std.ArrayListUnmanaged(NodeId).empty;
    defer export_runtime_input_node_ids.deinit(allocator);
    var compile_input_node_ids = std.ArrayListUnmanaged(NodeId).empty;
    defer compile_input_node_ids.deinit(allocator);
    for (subgraph.input_node_ids, subgraph.runtime_input_parameter_node_ids) |old_node_id, subgraph_input_node_id| {
        const lowered_node_id = lowered_export.id_map[subgraph_input_node_id];
        if (lowered_node_id == ml.graph.null_node) continue;
        const sorted_node_id = sorted_export.id_map[lowered_node_id];
        if (sorted_node_id == ml.graph.null_node) continue;
        try export_runtime_input_node_ids.append(allocator, sorted_node_id);
        try compile_input_node_ids.append(allocator, old_node_id);
    }

    const compile_output_node_ids = try reorderCompileOutputNodeIds(
        allocator,
        &subgraph,
        &lowered_export,
        &sorted_export,
    );
    defer allocator.free(compile_output_node_ids);

    const effective_node_name_overrides = try buildEffectiveNodeNameOverrides(
        allocator,
        &sorted_export.graph,
        export_runtime_input_node_ids.items,
        options,
    );
    defer allocator.free(effective_node_name_overrides);
    const effective_semantic_gqa_bindings = try buildEffectiveSemanticDecoderGqaBindings(
        allocator,
        &sorted_export.graph,
        options,
    );
    defer allocator.free(effective_semantic_gqa_bindings);

    const compile_input_names = try buildCompileNodeNames(allocator, &sorted_export.graph, export_runtime_input_node_ids.items, effective_node_name_overrides);
    defer {
        for (compile_input_names) |name| allocator.free(name);
        allocator.free(compile_input_names);
    }
    const compile_output_names = try buildCompileOutputNames(allocator, &sorted_export, effective_node_name_overrides);
    defer {
        for (compile_output_names) |name| allocator.free(name);
        allocator.free(compile_output_names);
    }

    var estimated_footprint_storage: ?ExportFootprint = null;
    defer if (estimated_footprint_storage) |*estimate| estimate.deinit(allocator);
    const estimated_footprint = if (profile_enabled) blk: {
        estimated_footprint_storage = try estimateParameterExportFootprint(
            allocator,
            &sorted_export.graph,
            export_runtime_input_node_ids.items,
            cb,
            weight_export_source,
            weight_export_policy,
        );
        break :blk &estimated_footprint_storage.?;
    } else null;

    if (estimate_only) {
        var estimate = try estimateParameterExportFootprint(
            allocator,
            &sorted_export.graph,
            export_runtime_input_node_ids.items,
            cb,
            weight_export_source,
            weight_export_policy,
        );
        defer estimate.deinit(allocator);
        std.debug.print(
            "onnx_export_estimate: nodes={d} runtime_inputs={d} outputs={d} parameters={d} skipped_parameters={d} estimated_loaded_bytes={d} estimated_serialized_bytes={d}\n",
            .{
                sorted_export.graph.nodeCount(),
                export_runtime_input_node_ids.items.len,
                subgraph.output_node_ids.len,
                estimate.parameter_count,
                estimate.skipped_parameter_count,
                estimate.loaded_bytes,
                estimate.serialized_bytes,
            },
        );
        std.debug.print(
            "onnx_export_estimate_detail: dense_source_parameters={d} quantized_source_parameters={d}\n",
            .{ estimate.dense_source_parameter_count, estimate.quantized_source_parameter_count },
        );
        std.debug.print(
            "onnx_export_estimate_policy: dense_parameters={d} q8_0_parameters={d}\n",
            .{ estimate.dense_policy_parameter_count, estimate.q8_0_policy_parameter_count },
        );
        for (weight_export_policy.rules, 0..) |rule, idx| {
            const match_count = if (idx < estimate.policy_rule_match_counts.len) estimate.policy_rule_match_counts[idx] else 0;
            std.debug.print(
                "onnx_export_estimate_policy_rule: index={d} substring={s} mode={s} matches={d}\n",
                .{ idx, rule.substring, @tagName(rule.mode), match_count },
            );
        }
        std.debug.print(
            "onnx_export_estimate_q8_0: candidate_parameters={d} estimated_serialized_bytes={d}\n",
            .{ estimate.q8_0_candidate_parameter_count, estimate.q8_0_candidate_serialized_bytes },
        );
        if (estimate.skipped_parameter_names.items.len > 0) {
            const limit = @min(estimate.skipped_parameter_names.items.len, 8);
            for (estimate.skipped_parameter_names.items[0..limit]) |name| {
                std.debug.print("onnx_export_estimate_skipped: {s}\n", .{name});
            }
        }
        return error.ExportEstimateOnly;
    }

    var parameter_initializers: []ParameterInitializer = &.{};
    defer freeParameterInitializers(allocator, parameter_initializers);

    var export_opts: onnx_graph.ExportOptions = .{
        .graph_name = "termite_partition",
        .lower_fused = false,
        .node_name_overrides = effective_node_name_overrides,
        .semantic_decoder_gqa_bindings = effective_semantic_gqa_bindings,
    };
    if (reuse_initializers_from_onnx_path) |reuse_path| {
        var reference_ctx = try ExistingOnnxInitializerReferenceContext.init(allocator, reuse_path, weight_export_policy);
        defer reference_ctx.deinit();
        export_opts.parameter_initializer_reference_provider = .{
            .context = &reference_ctx,
            .load = loadExistingInitializerReference,
        };
        const exported_bytes = try onnx_graph.exportGraph(allocator, &sorted_export.graph, export_opts);
        errdefer allocator.free(exported_bytes);
        return .{
            .onnx_bytes = exported_bytes,
            .external_data_path = null,
            .input_node_ids = try allocator.dupe(NodeId, compile_input_node_ids.items),
            .input_names = try cloneStringSliceList(allocator, compile_input_names),
            .output_node_ids = try allocator.dupe(NodeId, compile_output_node_ids),
            .output_names = try cloneStringSliceList(allocator, compile_output_names),
            .allocator = allocator,
        };
    }
    if (external_data_name != null and external_data_absolute_path != null) {
        var provider_ctx = ParameterInitializerProviderContext{
            .graph = &sorted_export.graph,
            .runtime_input_node_ids = export_runtime_input_node_ids.items,
            .cb = cb,
            .weight_export_source = weight_export_source,
            .weight_export_policy = weight_export_policy,
            .profile_enabled = profile_enabled,
        };
        export_opts.parameter_initializer_provider = .{
            .context = &provider_ctx,
            .load = loadParameterInitializerOnDemand,
            .free = freeParameterInitializerOnDemand,
        };
        var streamed = try onnx_graph.exportGraphWithExternalDataToPath(
            allocator,
            &sorted_export.graph,
            export_opts,
            external_data_name.?,
            external_data_absolute_path.?,
        );
        defer streamed.deinit(allocator);
        if (profile_enabled) {
            const exported_ns = nowNs();
            std.debug.print(
                "onnx_export_profile: nodes={d} runtime_inputs={d} outputs={d} extracted_ms={d} synthesize_ms={d} lower_ms={d} sort_ms={d} export_ms={d} estimated_params={d} estimated_skipped_params={d} estimated_loaded_bytes={d} estimated_serialized_bytes={d} lazy_inits={d}\n",
                .{
                    sorted_export.graph.nodeCount(),
                    export_runtime_input_node_ids.items.len,
                    subgraph.output_node_ids.len,
                    @divTrunc(extracted_ns - started_ns, std.time.ns_per_ms),
                    @divTrunc(synthesized_ns - extracted_ns, std.time.ns_per_ms),
                    @divTrunc(lowered_ns - synthesized_ns, std.time.ns_per_ms),
                    @divTrunc(sorted_ns - lowered_ns, std.time.ns_per_ms),
                    @divTrunc(exported_ns - sorted_ns, std.time.ns_per_ms),
                    estimated_footprint.?.parameter_count,
                    estimated_footprint.?.skipped_parameter_count,
                    estimated_footprint.?.loaded_bytes,
                    estimated_footprint.?.serialized_bytes,
                    provider_ctx.loads,
                },
            );
            std.debug.print(
                "onnx_export_profile_detail: lazy_raw_inits={d} lazy_streamed_inits={d} lazy_streamed_dense_inits={d} lazy_streamed_quantized_inits={d} lazy_q8_0_inits={d} lazy_f32_inits={d} actual_loaded_bytes={d} actual_serialized_bytes={d}\n",
                .{
                    provider_ctx.raw_byte_loads,
                    provider_ctx.streamed_loads,
                    provider_ctx.streamed_dense_loads,
                    provider_ctx.streamed_quantized_loads,
                    provider_ctx.q8_0_loads,
                    provider_ctx.f32_loads,
                    provider_ctx.loaded_bytes,
                    provider_ctx.serialized_bytes,
                },
            );
        }
        return .{
            .onnx_bytes = try allocator.dupe(u8, streamed.model_bytes),
            .external_data_path = try allocator.dupe(u8, streamed.relative_external_path),
            .input_node_ids = try allocator.dupe(NodeId, compile_input_node_ids.items),
            .input_names = try cloneStringSliceList(allocator, compile_input_names),
            .output_node_ids = try allocator.dupe(NodeId, compile_output_node_ids),
            .output_names = try cloneStringSliceList(allocator, compile_output_names),
            .allocator = allocator,
        };
    }

    parameter_initializers = try collectParameterInitializers(allocator, &sorted_export.graph, export_runtime_input_node_ids.items, cb, weight_export_source, weight_export_policy);
    export_opts.parameter_initializers = parameter_initializers;

    var exported: onnx_graph.ExportResult = blk: {
        if (external_data_name) |name| {
            break :blk try onnx_graph.exportGraphWithExternalData(allocator, &sorted_export.graph, export_opts, name);
        }
        const inline_bytes = try onnx_graph.exportGraph(allocator, &sorted_export.graph, export_opts);
        break :blk .{ .model_bytes = inline_bytes };
    };
    defer exported.deinit(allocator);
    if (profile_enabled) {
        const exported_ns = nowNs();
        std.debug.print(
            "onnx_export_profile: nodes={d} runtime_inputs={d} outputs={d} extracted_ms={d} synthesize_ms={d} lower_ms={d} sort_ms={d} export_ms={d} estimated_params={d} estimated_skipped_params={d} estimated_loaded_bytes={d} estimated_serialized_bytes={d} eager_inits={d}\n",
            .{
                sorted_export.graph.nodeCount(),
                export_runtime_input_node_ids.items.len,
                subgraph.output_node_ids.len,
                @divTrunc(extracted_ns - started_ns, std.time.ns_per_ms),
                @divTrunc(synthesized_ns - extracted_ns, std.time.ns_per_ms),
                @divTrunc(lowered_ns - synthesized_ns, std.time.ns_per_ms),
                @divTrunc(sorted_ns - lowered_ns, std.time.ns_per_ms),
                @divTrunc(exported_ns - sorted_ns, std.time.ns_per_ms),
                estimated_footprint.?.parameter_count,
                estimated_footprint.?.skipped_parameter_count,
                estimated_footprint.?.loaded_bytes,
                estimated_footprint.?.serialized_bytes,
                parameter_initializers.len,
            },
        );
    }

    return .{
        .onnx_bytes = try allocator.dupe(u8, exported.model_bytes),
        .external_data_path = if (exported.external_data) |data| try allocator.dupe(u8, data.relative_path) else null,
        .external_data_bytes = if (exported.external_data) |data| try allocator.dupe(u8, data.bytes) else &.{},
        .input_node_ids = try allocator.dupe(NodeId, compile_input_node_ids.items),
        .input_names = try cloneStringSliceList(allocator, compile_input_names),
        .output_node_ids = try allocator.dupe(NodeId, compile_output_node_ids),
        .output_names = try cloneStringSliceList(allocator, compile_output_names),
        .allocator = allocator,
    };
}

fn buildEffectiveNodeNameOverrides(
    allocator: Allocator,
    graph: *const Graph,
    runtime_input_node_ids: []const NodeId,
    options: CompileOptions,
) ![]onnx_graph.NodeNameOverride {
    var out = std.ArrayListUnmanaged(onnx_graph.NodeNameOverride).empty;
    errdefer out.deinit(allocator);
    if (options.semantic_decoder_entrypoint) {
        if (runtime_input_node_ids.len == 0) return error.UnsupportedArtifactInputs;
        try out.append(allocator, .{
            .node_id = semanticInputIdsNodeId(graph, runtime_input_node_ids),
            .name = "input_ids",
        });
        if (graph.outputs.items.len == 0) return error.InvalidArtifactOutput;
        try out.append(allocator, .{
            .node_id = graph.outputs.items[0],
            .name = "logits",
        });
    }
    try out.appendSlice(allocator, options.node_name_overrides);
    return out.toOwnedSlice(allocator);
}

fn semanticInputIdsNodeId(graph: *const Graph, runtime_input_node_ids: []const NodeId) NodeId {
    for (runtime_input_node_ids) |node_id| {
        const shape = graph.node(node_id).output_shape;
        switch (shape.dtype) {
            .i64, .i32 => return node_id,
            else => {},
        }
    }
    return runtime_input_node_ids[0];
}

fn buildEffectiveSemanticDecoderGqaBindings(
    allocator: Allocator,
    graph: *const Graph,
    options: CompileOptions,
) ![]onnx_graph.SemanticDecoderGqaBinding {
    var out = std.ArrayListUnmanaged(onnx_graph.SemanticDecoderGqaBinding).empty;
    errdefer out.deinit(allocator);
    if (options.semantic_decoder_entrypoint) {
        for (0..graph.nodeCount()) |i| {
            const node_id: NodeId = @intCast(i);
            switch (graph.node(node_id).op) {
                .fused_gqa_causal_attention => |attrs| {
                    if (attrs.seq_len == 0) continue;
                    try out.append(allocator, .{
                        .node_id = node_id,
                        .layer_index = attrs.layer_index,
                        .skip_kv_write = attrs.skip_kv_write,
                    });
                },
                else => {},
            }
        }
        if (out.items.len == 0) return error.UnsupportedArtifactInputs;
    }
    try out.appendSlice(allocator, options.semantic_decoder_gqa_bindings);
    return out.toOwnedSlice(allocator);
}

fn buildCompileNodeNames(
    allocator: Allocator,
    graph: *const Graph,
    node_ids: []const NodeId,
    node_name_overrides: []const onnx_graph.NodeNameOverride,
) ![][]u8 {
    const out = try allocator.alloc([]u8, node_ids.len);
    errdefer allocator.free(out);
    for (node_ids, 0..) |node_id, idx| {
        const node = graph.node(node_id);
        out[idx] = if (findNodeNameOverride(node_name_overrides, node_id)) |override_name|
            try allocator.dupe(u8, override_name)
        else switch (node.op) {
            .parameter => |p| try allocator.dupe(u8, graph.string_table.items[p.name_offset..][0..p.name_len]),
            .constant => try std.fmt.allocPrint(allocator, "const_{d}", .{node_id}),
            else => try std.fmt.allocPrint(allocator, "node_{d}", .{node_id}),
        };
    }
    return out;
}

fn buildCompileOutputNames(
    allocator: Allocator,
    sorted_export: *const TopologicalSortResult,
    node_name_overrides: []const onnx_graph.NodeNameOverride,
) ![][]u8 {
    return buildCompileNodeNames(allocator, &sorted_export.graph, sorted_export.graph.outputs.items, node_name_overrides);
}

fn findNodeNameOverride(overrides: []const onnx_graph.NodeNameOverride, node_id: NodeId) ?[]const u8 {
    for (overrides) |override| {
        if (override.node_id == node_id) return override.name;
    }
    return null;
}

fn cloneStringSliceList(allocator: Allocator, src: []const []u8) ![][]u8 {
    const out = try allocator.alloc([]u8, src.len);
    errdefer allocator.free(out);
    for (src, 0..) |name, idx| {
        out[idx] = try allocator.dupe(u8, name);
    }
    return out;
}

const ExistingQ8Attrs = struct {
    axis: i64,
    block_size: i64,
};

const ExistingOnnxInitializerReferenceContext = struct {
    allocator: Allocator,
    model_bytes: []u8,
    lazy_model: onnx_graph.LazyModelProto,
    weight_export_policy: WeightExportPolicy,

    fn init(allocator: Allocator, path: []const u8, weight_export_policy: WeightExportPolicy) !@This() {
        const model_bytes = try readFileAllocC(allocator, path);
        errdefer allocator.free(model_bytes);
        var lazy_model = try onnx_graph.parseLazy(allocator, model_bytes);
        errdefer lazy_model.deinit(allocator);
        return .{
            .allocator = allocator,
            .model_bytes = model_bytes,
            .lazy_model = lazy_model,
            .weight_export_policy = weight_export_policy,
        };
    }

    fn deinit(self: *@This()) void {
        self.lazy_model.deinit(self.allocator);
        self.allocator.free(self.model_bytes);
        self.* = undefined;
    }

    fn cloneInitializerByName(
        self: *@This(),
        allocator: Allocator,
        name: []const u8,
    ) !?onnx_graph.TensorProto {
        const graph = if (self.lazy_model.graph) |*g| g else return null;
        for (0..graph.initializerCount()) |idx| {
            const init_name = try graph.initializerName(idx);
            if (!std.mem.eql(u8, init_name, name)) continue;
            var parsed = try graph.parseInitializer(allocator, idx);
            defer parsed.deinit(allocator);
            if (!parsed.isExternal()) return error.ReusedOnnxInitializerMustBeExternal;
            if (parsed.raw_data.len != 0 or parsed.float_data.len != 0 or parsed.int32_data.len != 0 or parsed.int64_data.len != 0 or parsed.double_data.len != 0) {
                return error.ReusedOnnxInitializerMustBeExternal;
            }
            return try cloneTensorProto(allocator, &parsed);
        }
        return null;
    }

    fn findQ8Attrs(self: *@This(), parameter_name: []const u8) ?ExistingQ8Attrs {
        const graph = if (self.lazy_model.graph) |*g| g else return null;
        for (graph.nodes) |node| {
            if (!std.mem.eql(u8, node.op_type, "DequantizeLinear")) continue;
            if (node.outputs.len == 0 or !std.mem.eql(u8, node.outputs[0], parameter_name)) continue;
            var axis: ?i64 = null;
            var block_size: ?i64 = null;
            for (node.attributes) |attr| {
                if (std.mem.eql(u8, attr.name, "axis")) axis = attr.i;
                if (std.mem.eql(u8, attr.name, "block_size")) block_size = attr.i;
            }
            return .{
                .axis = axis orelse return null,
                .block_size = block_size orelse return null,
            };
        }
        return null;
    }
};

fn readFileAllocC(allocator: Allocator, path: []const u8) ![]u8 {
    return c_file.readFileMax(allocator, path, std.math.maxInt(usize));
}

fn loadExistingInitializerReference(
    raw_context: ?*anyopaque,
    allocator: Allocator,
    name: []const u8,
) !?onnx_graph.ParameterInitializerReference {
    const raw = raw_context orelse return null;
    const ctx: *ExistingOnnxInitializerReferenceContext = @ptrCast(@alignCast(raw));
    if (ctx.weight_export_policy.modeForName(name) == .q8_0_weight_only) {
        const values_name = try std.fmt.allocPrint(allocator, "{s}__q8_values", .{name});
        defer allocator.free(values_name);
        if (try ctx.cloneInitializerByName(allocator, values_name)) |values| {
            var owned_values = values;
            errdefer owned_values.deinit(allocator);

            const scales_name = try std.fmt.allocPrint(allocator, "{s}__q8_scales", .{name});
            defer allocator.free(scales_name);
            var scales = (try ctx.cloneInitializerByName(allocator, scales_name)) orelse return error.MissingExternalInitializerReference;
            errdefer scales.deinit(allocator);

            const zero_point_name = try std.fmt.allocPrint(allocator, "{s}__q8_zero_point", .{name});
            defer allocator.free(zero_point_name);
            var zero_point = (try ctx.cloneInitializerByName(allocator, zero_point_name)) orelse return error.MissingExternalInitializerReference;
            errdefer zero_point.deinit(allocator);

            const attrs = ctx.findQ8Attrs(name) orelse return error.MissingExternalInitializerReference;
            return .{
                .q8_0_block = .{
                    .values = owned_values,
                    .scales = scales,
                    .zero_point = zero_point,
                    .axis = attrs.axis,
                    .block_size = attrs.block_size,
                },
            };
        }
    }
    if (try ctx.cloneInitializerByName(allocator, name)) |tensor| {
        return .{ .tensor = tensor };
    }
    if (std.mem.startsWith(u8, name, "input_")) return null;
    std.log.err("missing reused ONNX initializer reference for parameter '{s}'", .{name});
    return error.MissingExternalInitializerReference;
}

fn cloneTensorProto(allocator: Allocator, src: *const onnx_graph.TensorProto) !onnx_graph.TensorProto {
    const dims = try allocator.dupe(i64, src.dims);
    errdefer allocator.free(dims);
    const external_data = try allocator.alloc(onnx_graph.proto.ExternalDataEntry, src.external_data.len);
    errdefer allocator.free(external_data);
    var external_data_init: usize = 0;
    errdefer {
        for (external_data[0..external_data_init]) |entry| {
            if (entry.key.len > 0) allocator.free(entry.key);
            if (entry.value.len > 0) allocator.free(entry.value);
        }
    }
    for (src.external_data, 0..) |entry, idx| {
        external_data[idx] = .{
            .key = try allocator.dupe(u8, entry.key),
            .value = try allocator.dupe(u8, entry.value),
        };
        external_data_init += 1;
    }
    return .{
        .dims = dims,
        .data_type = src.data_type,
        .float_data = try allocator.dupe(u8, src.float_data),
        .int32_data = try allocator.dupe(u8, src.int32_data),
        .int64_data = try allocator.dupe(u8, src.int64_data),
        .name = try allocator.dupe(u8, src.name),
        .raw_data = try allocator.dupe(u8, src.raw_data),
        .double_data = try allocator.dupe(u8, src.double_data),
        .external_data = external_data,
        .data_location = src.data_location,
    };
}

fn reorderCompileOutputNodeIds(
    allocator: Allocator,
    subgraph: *const partition_export.ExportedSubgraph,
    lowered_export: *const ml.graph.lower.LowerResult,
    sorted_export: *const TopologicalSortResult,
) ![]NodeId {
    const ordered = try allocator.alloc(NodeId, sorted_export.graph.outputs.items.len);
    errdefer allocator.free(ordered);

    const sorted_output_ids = sorted_export.graph.outputs.items;
    if (sorted_output_ids.len != subgraph.output_node_ids.len) return error.InvalidArtifactOutput;

    if (subgraph.graph.outputs.items.len != subgraph.output_node_ids.len) return error.InvalidArtifactOutput;

    for (sorted_output_ids, 0..) |sorted_output_id, out_idx| {
        var matched: ?NodeId = null;
        for (subgraph.graph.outputs.items, subgraph.output_node_ids) |subgraph_output_id, old_output_id| {
            const output_node = subgraph.graph.node(subgraph_output_id);
            const redirected_output_id = if (output_node.op.isFused() and output_node.vjp_alternate != ml.graph.null_node)
                output_node.vjp_alternate
            else
                subgraph_output_id;
            const lowered_output_id = lowered_export.id_map[redirected_output_id];
            if (lowered_output_id == ml.graph.null_node) continue;
            const remapped_sorted_id = sorted_export.id_map[lowered_output_id];
            if (remapped_sorted_id == sorted_output_id) {
                matched = old_output_id;
                break;
            }
        }
        ordered[out_idx] = matched orelse return error.MissingOutputMapping;
    }

    return ordered;
}

const TopologicalSortResult = struct {
    graph: Graph,
    id_map: []NodeId,

    fn deinit(self: *@This()) void {
        const allocator = self.graph.allocator;
        self.graph.deinit();
        allocator.free(self.id_map);
    }
};

fn topologicallySortGraph(allocator: Allocator, graph: *const Graph) !TopologicalSortResult {
    const count = graph.nodeCount();
    const in_degree = try allocator.alloc(u32, count);
    defer allocator.free(in_degree);
    @memset(in_degree, 0);

    for (0..count) |i| {
        const node = graph.node(@intCast(i));
        for (node.getInputs()) |input_id| {
            if (input_id != ml.graph.null_node) in_degree[i] += 1;
        }
    }

    var queue = std.ArrayListUnmanaged(NodeId).empty;
    defer queue.deinit(allocator);
    for (0..count) |i| {
        if (in_degree[i] == 0) try queue.append(allocator, @intCast(i));
    }

    const id_map = try allocator.alloc(NodeId, count);
    errdefer allocator.free(id_map);
    @memset(id_map, ml.graph.null_node);

    var sorted_nodes = try std.ArrayListUnmanaged(ml.graph.Node).initCapacity(allocator, count);
    errdefer sorted_nodes.deinit(allocator);

    var head: usize = 0;
    while (head < queue.items.len) {
        const old_id = queue.items[head];
        head += 1;

        id_map[old_id] = @intCast(sorted_nodes.items.len);
        sorted_nodes.appendAssumeCapacity(graph.node(old_id).*);

        for (0..count) |j| {
            const node = graph.node(@intCast(j));
            for (node.getInputs()) |input_id| {
                if (input_id == old_id) {
                    in_degree[j] -= 1;
                    if (in_degree[j] == 0) try queue.append(allocator, @intCast(j));
                }
            }
        }
    }

    if (sorted_nodes.items.len != count) return error.CycleDetected;

    for (sorted_nodes.items) |*node| {
        for (&node.inputs, 0..) |*input_id, j| {
            if (j >= node.num_inputs) break;
            if (input_id.* != ml.graph.null_node) input_id.* = id_map[input_id.*];
        }
        if (node.vjp_alternate != ml.graph.null_node) {
            node.vjp_alternate = id_map[node.vjp_alternate];
        }
    }

    var sorted_graph = Graph.init(allocator);
    errdefer sorted_graph.deinit();
    try sorted_graph.string_table.appendSlice(allocator, graph.string_table.items);
    try sorted_graph.constant_pool.appendSlice(allocator, graph.constant_pool.items);
    try sorted_graph.nodes.appendSlice(allocator, sorted_nodes.items);
    for (graph.outputs.items) |output_id| try sorted_graph.outputs.append(allocator, id_map[output_id]);
    for (graph.parameters.items) |parameter_id| {
        if (id_map[parameter_id] != ml.graph.null_node) {
            try sorted_graph.parameters.append(allocator, id_map[parameter_id]);
        }
    }
    sorted_nodes.deinit(allocator);

    return .{
        .graph = sorted_graph,
        .id_map = id_map,
    };
}

fn collectParameterInitializers(
    allocator: Allocator,
    graph: *const Graph,
    runtime_input_node_ids: []const NodeId,
    cb: *const ComputeBackend,
    weight_export_source: ?export_source_mod.Source,
    weight_export_policy: WeightExportPolicy,
) ![]ParameterInitializer {
    var result = std.ArrayListUnmanaged(ParameterInitializer).empty;
    errdefer freeParameterInitializers(allocator, result.items);
    for (0..graph.nodeCount()) |i| {
        const node_id: NodeId = @intCast(i);
        if (isRuntimeInputNode(runtime_input_node_ids, node_id)) continue;
        const node = graph.node(node_id);
        if (node.op != .parameter) continue;
        const name = graph.parameterName(node);
        if (weight_export_source) |source| {
            if (try buildParameterInitializerFromExportSource(allocator, source, name, node.output_shape, weight_export_policy)) |init| {
                errdefer freeParameterInitializerData(allocator, init.data);
                try result.append(allocator, .{
                    .name = try allocator.dupe(u8, name),
                    .shape = init.shape,
                    .data = init.data,
                });
                continue;
            }
        }
        const ct = try cb.getWeight(name);
        defer cb.free(ct);
        const shape = try resolveParameterInitializerShape(allocator, graph, node_id, node, cb, ct);
        const init = try buildParameterInitializer(allocator, cb, ct, shape);
        errdefer freeParameterInitializerData(allocator, init.data);
        try result.append(allocator, .{
            .name = try allocator.dupe(u8, name),
            .shape = init.shape,
            .data = init.data,
        });
    }
    return result.toOwnedSlice(allocator);
}

fn estimateParameterExportFootprint(
    allocator: Allocator,
    graph: *const Graph,
    runtime_input_node_ids: []const NodeId,
    cb: *const ComputeBackend,
    weight_export_source: ?export_source_mod.Source,
    weight_export_policy: WeightExportPolicy,
) !ExportFootprint {
    var footprint: ExportFootprint = .{};
    if (weight_export_policy.rules.len > 0) {
        footprint.policy_rule_match_counts = try allocator.alloc(usize, weight_export_policy.rules.len);
        @memset(footprint.policy_rule_match_counts, 0);
    }
    for (0..graph.nodeCount()) |i| {
        const node_id: NodeId = @intCast(i);
        if (isRuntimeInputNode(runtime_input_node_ids, node_id)) continue;
        const node = graph.node(node_id);
        if (node.op != .parameter) continue;
        const name = graph.parameterName(node);
        footprint.parameter_count += 1;
        for (weight_export_policy.rules, 0..) |rule, rule_idx| {
            if (std.mem.indexOf(u8, name, rule.substring) != null) footprint.policy_rule_match_counts[rule_idx] += 1;
        }
        const selected_mode = weight_export_policy.modeForName(name);
        switch (selected_mode) {
            .dense => footprint.dense_policy_parameter_count += 1,
            .q8_0_weight_only => footprint.q8_0_policy_parameter_count += 1,
        }
        if (weight_export_source) |source| {
            if (selected_mode == .q8_0_weight_only and onnxQ8ExportEnabledForName(name)) {
                if (try source.openQ8_0BlockTensor(allocator, name)) |q8| {
                    defer {
                        var owned = q8;
                        owned.deinit(allocator);
                    }
                    footprint.q8_0_candidate_parameter_count += 1;
                    footprint.q8_0_candidate_serialized_bytes += q8.values_u8.len + (q8.scales_f32.len * @sizeOf(f32)) + 1;
                    footprint.quantized_source_parameter_count += 1;
                    footprint.loaded_bytes += q8.source_byte_len;
                    footprint.serialized_bytes += q8.values_u8.len + (q8.scales_f32.len * @sizeOf(f32)) + 1;
                    continue;
                }
            }
            const target_dtype = graphDTypeToTensorDType(node.output_shape.dtype);
            if (try source.openTensor(allocator, name, target_dtype)) |stream| {
                defer stream.deinit(stream.context, allocator);
                const shape = ml.graph.Shape.init(node.output_shape.dtype, stream.shape);
                _ = shapeElementCount(shape) catch |err| switch (err) {
                    error.UnsupportedShape => {
                        footprint.skipped_parameter_count += 1;
                        try footprint.skipped_parameter_names.append(allocator, try allocator.dupe(u8, name));
                        continue;
                    },
                    else => return err,
                };
                switch (stream.storage_kind) {
                    .dense_native => footprint.dense_source_parameter_count += 1,
                    .quantized_dequantized_f32 => footprint.quantized_source_parameter_count += 1,
                }
                footprint.loaded_bytes += stream.source_byte_len;
                footprint.serialized_bytes += stream.byte_len;
                continue;
            }
        }
        const ct = try cb.getWeight(name);
        defer cb.free(ct);
        const shape = try resolveParameterInitializerShape(allocator, graph, node_id, node, cb, ct);
        const element_count = shapeElementCount(shape) catch |err| switch (err) {
            error.UnsupportedShape => {
                footprint.skipped_parameter_count += 1;
                try footprint.skipped_parameter_names.append(allocator, try allocator.dupe(u8, name));
                continue;
            },
            else => return err,
        };
        footprint.loaded_bytes += estimateLoadedTensorBytes(cb, ct, shape, element_count);
        footprint.serialized_bytes += serializedTensorBytes(shape, element_count);
    }
    return footprint;
}

fn estimateLoadedTensorBytes(
    cb: *const ComputeBackend,
    tensor: contracts.CT,
    shape: ml.graph.Shape,
    element_count: usize,
) usize {
    const backend_dtype = cb.tensorDType(tensor) catch null;
    if (backend_dtype) |dtype| {
        const graph_dtype = exportTensorDTypeToGraphDType(dtype) orelse return element_count * @sizeOf(f32);
        if (graph_dtype == shape.dtype) return exportTensorDTypeByteSize(dtype) * element_count;
    }
    return element_count * @sizeOf(f32);
}

const ParameterInitializerProviderContext = struct {
    graph: *const Graph,
    runtime_input_node_ids: []const NodeId,
    cb: *const ComputeBackend,
    weight_export_source: ?export_source_mod.Source = null,
    weight_export_policy: WeightExportPolicy = .{},
    profile_enabled: bool = false,
    loads: usize = 0,
    raw_byte_loads: usize = 0,
    streamed_loads: usize = 0,
    streamed_dense_loads: usize = 0,
    streamed_quantized_loads: usize = 0,
    q8_0_loads: usize = 0,
    f32_loads: usize = 0,
    loaded_bytes: usize = 0,
    serialized_bytes: usize = 0,
};

fn loadParameterInitializerOnDemand(
    raw_context: ?*anyopaque,
    allocator: Allocator,
    name: []const u8,
) !?ParameterInitializer {
    const context = raw_context orelse return null;
    const ctx: *ParameterInitializerProviderContext = @ptrCast(@alignCast(context));
    for (0..ctx.graph.nodeCount()) |i| {
        const node_id: NodeId = @intCast(i);
        if (isRuntimeInputNode(ctx.runtime_input_node_ids, node_id)) continue;
        const node = ctx.graph.node(node_id);
        if (node.op != .parameter) continue;
        if (!std.mem.eql(u8, ctx.graph.parameterName(node), name)) continue;
        if (ctx.weight_export_source) |source| {
            if (try buildParameterInitializerFromExportSource(allocator, source, name, node.output_shape, ctx.weight_export_policy)) |init| {
                if (ctx.profile_enabled) {
                    ctx.loads += 1;
                    switch (init.data) {
                        .f32 => |data| {
                            ctx.f32_loads += 1;
                            ctx.loaded_bytes += data.len * @sizeOf(f32);
                            ctx.serialized_bytes += serializedTensorBytes(init.shape, data.len);
                        },
                        .raw_bytes => |data| {
                            ctx.raw_byte_loads += 1;
                            ctx.loaded_bytes += data.len;
                            ctx.serialized_bytes += data.len;
                        },
                        .streamed => |data| {
                            ctx.streamed_loads += 1;
                            switch (data.storage_kind_tag) {
                                .dense_native => ctx.streamed_dense_loads += 1,
                                .quantized_dequantized_f32 => ctx.streamed_quantized_loads += 1,
                            }
                            ctx.loaded_bytes += data.source_byte_len;
                            ctx.serialized_bytes += data.byte_len;
                        },
                        .q8_0_block => |data| {
                            ctx.q8_0_loads += 1;
                            ctx.loaded_bytes += data.source_byte_len;
                            ctx.serialized_bytes += data.values_u8.len + (data.scales_f32.len * @sizeOf(f32)) + 1;
                        },
                    }
                }
                return .{ .name = name, .shape = init.shape, .data = init.data };
            }
        }
        const ct = try ctx.cb.getWeight(name);
        defer ctx.cb.free(ct);
        const shape = try resolveParameterInitializerShape(allocator, ctx.graph, node_id, node, ctx.cb, ct);
        const init = try buildParameterInitializer(allocator, ctx.cb, ct, shape);
        if (ctx.profile_enabled) {
            ctx.loads += 1;
            switch (init.data) {
                .f32 => |data| {
                    ctx.f32_loads += 1;
                    ctx.loaded_bytes += data.len * @sizeOf(f32);
                    ctx.serialized_bytes += serializedTensorBytes(init.shape, data.len);
                },
                .raw_bytes => |data| {
                    ctx.raw_byte_loads += 1;
                    ctx.loaded_bytes += data.len;
                    ctx.serialized_bytes += data.len;
                },
                .streamed => |data| {
                    ctx.streamed_loads += 1;
                    ctx.loaded_bytes += data.source_byte_len;
                    ctx.serialized_bytes += data.byte_len;
                },
                .q8_0_block => |data| {
                    ctx.q8_0_loads += 1;
                    ctx.loaded_bytes += data.source_byte_len;
                    ctx.serialized_bytes += data.values_u8.len + (data.scales_f32.len * @sizeOf(f32)) + 1;
                },
            }
        }
        return .{ .name = name, .shape = init.shape, .data = init.data };
    }
    return null;
}

fn freeParameterInitializerOnDemand(
    _: ?*anyopaque,
    allocator: Allocator,
    init: *const ParameterInitializer,
) void {
    freeParameterInitializerData(allocator, init.data);
}

fn freeParameterInitializers(allocator: Allocator, inits: []ParameterInitializer) void {
    for (inits) |init| {
        allocator.free(init.name);
        freeParameterInitializerData(allocator, init.data);
    }
    if (inits.len > 0) allocator.free(inits);
}

fn freeParameterInitializerData(allocator: Allocator, init_data: ParameterInitializerData) void {
    switch (init_data) {
        .f32 => |slice| allocator.free(slice),
        .raw_bytes => |slice| allocator.free(slice),
        .streamed => |stream| onnx_graph.freeStreamedTensorData(allocator, stream),
        .q8_0_block => |data| {
            allocator.free(data.values_u8);
            allocator.free(data.scales_f32);
        },
    }
}

fn buildParameterInitializer(
    allocator: Allocator,
    cb: *const ComputeBackend,
    tensor: contracts.CT,
    shape: ml.graph.Shape,
) !BuiltInitializer {
    if (try cb.exportTensorData(tensor, allocator)) |exported| {
        switch (exported.payload) {
            .bytes => |bytes| {
                errdefer allocator.free(bytes);
                if (exportTensorDTypeToGraphDType(exported.dtype)) |export_dtype| {
                    if (export_dtype == shape.dtype) {
                        const elem_size = exportTensorDTypeByteSize(exported.dtype);
                        if (elem_size == 0 or bytes.len % elem_size != 0) return error.InvalidTensorBytes;
                        const element_count = bytes.len / elem_size;
                        return .{
                            .shape = try resolveDynamicShapeFromElementCount(shape, element_count),
                            .data = .{ .raw_bytes = bytes },
                        };
                    }
                }
                allocator.free(bytes);
            },
            .quantized_f32 => |streamed_quantized| {
                allocator.free(streamed_quantized.raw_bytes);
                allocator.free(streamed_quantized.shape);
            },
        }
    }

    const data = try cb.toFloat32(tensor, allocator);
    errdefer allocator.free(data);
    return .{
        .shape = try resolveDynamicShapeFromElementCount(shape, data.len),
        .data = .{ .f32 = data },
    };
}

fn buildParameterInitializerFromExportSource(
    allocator: Allocator,
    source: export_source_mod.Source,
    name: []const u8,
    shape: ml.graph.Shape,
    weight_export_policy: WeightExportPolicy,
) !?BuiltInitializer {
    if (weight_export_policy.modeForName(name) == .q8_0_weight_only and onnxQ8ExportEnabledForName(name)) {
        if (try source.openQ8_0BlockTensor(allocator, name)) |q8| {
            const resolved_shape = ml.graph.Shape.init(shape.dtype, q8.shape);
            const scale_shape = ml.graph.Shape.init(.f32, q8.scale_shape);
            allocator.free(q8.shape);
            allocator.free(q8.scale_shape);
            return .{
                .shape = resolved_shape,
                .data = .{
                    .q8_0_block = .{
                        .scale_shape = scale_shape,
                        .values_u8 = q8.values_u8,
                        .scales_f32 = q8.scales_f32,
                        .zero_point_u8 = q8.zero_point_u8,
                        .axis = q8.axis,
                        .block_size = q8.block_size,
                        .source_byte_len = q8.source_byte_len,
                    },
                },
            };
        }
    }
    const target_dtype = graphDTypeToTensorDType(shape.dtype);
    const stream = (try source.openTensor(allocator, name, target_dtype)) orelse return null;
    const resolved_shape = ml.graph.Shape.init(shape.dtype, stream.shape);
    const Adapter = struct {
        stream: export_source_mod.Stream,

        fn writeAll(raw_context: ?*anyopaque, alloc: Allocator, sink: onnx_graph.ByteSink) anyerror!void {
            const context = raw_context orelse return error.InvalidState;
            const self: *@This() = @ptrCast(@alignCast(context));
            const SinkAdapter = struct {
                sink: onnx_graph.ByteSink,

                fn write(adapter_context: ?*anyopaque, bytes: []const u8) anyerror!void {
                    const adapter = adapter_context orelse return error.InvalidState;
                    const typed: *@This() = @ptrCast(@alignCast(adapter));
                    return typed.sink.write(typed.sink.context, bytes);
                }
            };

            var sink_adapter = SinkAdapter{ .sink = sink };
            return self.stream.write_all(
                self.stream.context,
                alloc,
                .{
                    .context = &sink_adapter,
                    .write = &SinkAdapter.write,
                },
            );
        }

        fn deinit(raw_context: ?*anyopaque, alloc: Allocator) void {
            const context = raw_context orelse return;
            const self: *@This() = @ptrCast(@alignCast(context));
            self.stream.deinit(self.stream.context, alloc);
            alloc.destroy(self);
        }
    };
    const adapter = try allocator.create(Adapter);
    adapter.* = .{ .stream = stream };
    return .{
        .shape = resolved_shape,
        .data = .{
            .streamed = .{
                .storage_kind_tag = switch (stream.storage_kind) {
                    .dense_native => .dense_native,
                    .quantized_dequantized_f32 => .quantized_dequantized_f32,
                },
                .source_byte_len = stream.source_byte_len,
                .byte_len = stream.byte_len,
                .context = adapter,
                .write_all = &Adapter.writeAll,
                .deinit = &Adapter.deinit,
            },
        },
    };
}

fn onnxQ8ExportEnabledForName(name: []const u8) bool {
    if (platform.env.getenv("TERMITE_ONNX_Q8_INCLUDE")) |include| {
        if (include.len > 0 and !matchesCommaSeparatedSubstringList(include, name)) return false;
    }
    if (platform.env.getenv("TERMITE_ONNX_Q8_EXCLUDE")) |exclude| {
        if (exclude.len > 0 and matchesCommaSeparatedSubstringList(exclude, name)) return false;
    }
    return true;
}

fn matchesCommaSeparatedSubstringList(list: []const u8, haystack: []const u8) bool {
    var it = std.mem.splitScalar(u8, list, ',');
    while (it.next()) |raw_item| {
        const item = std.mem.trim(u8, raw_item, " \t\r\n");
        if (item.len == 0) continue;
        if (std.mem.indexOf(u8, haystack, item) != null) return true;
    }
    return false;
}

test "WeightExportPolicy applies ordered substring overrides" {
    const rules = [_]WeightExportRule{
        .{ .substring = "model.layers.", .mode = .q8_0_weight_only },
        .{ .substring = "model.layers.0.", .mode = .dense },
    };
    const policy = WeightExportPolicy{
        .default_mode = .dense,
        .rules = &rules,
    };
    try std.testing.expectEqual(QuantExportMode.dense, policy.modeForName("model.embed_tokens.weight"));
    try std.testing.expectEqual(QuantExportMode.dense, policy.modeForName("model.layers.0.mlp.down_proj.weight"));
    try std.testing.expectEqual(QuantExportMode.q8_0_weight_only, policy.modeForName("model.layers.1.mlp.down_proj.weight"));
}

fn exportTensorDTypeToGraphDType(dtype: @TypeOf(@as(ExportTensorData, undefined).dtype)) ?ml.graph.DType {
    return switch (dtype) {
        .f32 => .f32,
        .f16 => .f16,
        .bf16 => .bf16,
        .f64 => .f64,
        .i8 => .i8,
        .i16 => .i16,
        .i32 => .i32,
        .i64 => .i64,
        .u8 => .u8,
        .bool_ => .bool_,
    };
}

fn graphDTypeToTensorDType(dtype: ml.graph.DType) @TypeOf(@as(ExportTensorData, undefined).dtype) {
    return switch (dtype) {
        .f32 => .f32,
        .f16 => .f16,
        .bf16 => .bf16,
        .f64 => .f64,
        .i8 => .i8,
        .i16 => .i16,
        .i32 => .i32,
        .i64 => .i64,
        .u8 => .u8,
        .bool_ => .bool_,
    };
}

fn exportTensorDTypeByteSize(dtype: @TypeOf(@as(ExportTensorData, undefined).dtype)) usize {
    return switch (dtype) {
        .f32, .i32 => 4,
        .f16, .bf16, .i16 => 2,
        .f64, .i64 => 8,
        .i8, .u8, .bool_ => 1,
    };
}

fn isRuntimeInputNode(runtime_input_node_ids: []const NodeId, node_id: NodeId) bool {
    for (runtime_input_node_ids) |runtime_input_node_id| {
        if (runtime_input_node_id == node_id) return true;
    }
    return false;
}

fn resolveParameterInitializerShape(
    allocator: Allocator,
    graph: *const Graph,
    node_id: NodeId,
    node: *const ml.graph.Node,
    cb: *const ComputeBackend,
    tensor: contracts.CT,
) !ml.graph.Shape {
    _ = graph;
    _ = node_id;
    if (cb.tensorShape(tensor, allocator)) |dims| {
        defer allocator.free(dims);
        return ml.graph.Shape.init(node.output_shape.dtype, dims);
    } else |err| switch (err) {
        error.UnsupportedShape => return node.output_shape,
        else => return err,
    }
}

fn resolveDynamicShapeFromElementCount(
    shape: ml.graph.Shape,
    element_count: usize,
) !ml.graph.Shape {
    var resolved = shape;
    var unknown_axis: ?u8 = null;
    var known_product: usize = 1;
    for (0..shape.rank()) |i| {
        const dim = shape.dim(@intCast(i));
        if (dim <= 0) {
            if (unknown_axis != null) return shape;
            unknown_axis = @intCast(i);
            continue;
        }
        known_product *= @as(usize, @intCast(dim));
    }
    if (unknown_axis == null or known_product == 0 or element_count % known_product != 0) return shape;
    resolved.dims[unknown_axis.?] = @intCast(@divExact(element_count, known_product));
    return resolved;
}

fn shapeElementCount(shape: ml.graph.Shape) !usize {
    var total: usize = 1;
    for (0..shape.rank()) |i| {
        const dim = shape.dim(@intCast(i));
        if (dim <= 0) return error.UnsupportedShape;
        total = try std.math.mul(usize, total, @as(usize, @intCast(dim)));
    }
    return total;
}

fn isWholeGraphPartition(graph: *const Graph, part: *const Partition) bool {
    if (part.external_inputs.len != 0) return false;
    if (part.node_ids.len != graph.nodeCount()) return false;
    for (part.node_ids, 0..) |nid, idx| {
        if (nid != @as(NodeId, @intCast(idx))) return false;
    }
    return true;
}

const SynthesizeOptions = struct {
    preserve_gqa: bool = false,
};

fn synthesizeLinearDecompositions(graph: *Graph, options: SynthesizeOptions) !void {
    const initial_count = graph.nodeCount();
    var b = Builder.init(graph);
    for (0..initial_count) |i| {
        const node_id: NodeId = @intCast(i);
        const node = graph.node(node_id);
        if (node.vjp_alternate != ml.graph.null_node) continue;

        switch (node.op) {
            .fused_linear => |attrs| {
                const decomposed = try synthesizeLinear(&b, graph, node, attrs.rows, attrs.in_dim, attrs.out_dim, true);
                graph.nodeMut(node_id).vjp_alternate = decomposed;
            },
            .fused_linear_no_bias => |attrs| {
                const decomposed = try synthesizeLinear(&b, graph, node, attrs.rows, attrs.in_dim, attrs.out_dim, false);
                graph.nodeMut(node_id).vjp_alternate = decomposed;
            },
            .fused_gqa_causal_attention => |attrs| {
                if (options.preserve_gqa) continue;
                const num_kv_heads = if (attrs.num_kv_heads > 0) attrs.num_kv_heads else attrs.num_heads;
                if (num_kv_heads > 0 and attrs.num_heads % num_kv_heads == 0) {
                    const decomposed = try synthesizeCausalSelfAttention(&b, graph, node_id, node, attrs);
                    graph.nodeMut(node_id).vjp_alternate = decomposed;
                }
            },
            else => {},
        }
    }
}

fn synthesizeLinear(
    b: *Builder,
    graph: *Graph,
    node: *const ml.graph.Node,
    rows: u32,
    in_dim: u32,
    out_dim: u32,
    has_bias: bool,
) !NodeId {
    const input_id = node.inputs[0];
    const weight_id = node.inputs[1];
    const bias_id = node.inputs[2];
    const input_shape = graph.node(input_id).output_shape;
    const weight_shape = graph.node(weight_id).output_shape;
    const matmul_input = if (input_shape.rank() == 1)
        try b.reshape(input_id, ml.graph.Shape.init(input_shape.dtype, &.{ @intCast(rows), @intCast(in_dim) }))
    else
        input_id;
    const matmul_weight = if (weight_shape.rank() == 2)
        weight_id
    else
        try b.reshape(weight_id, ml.graph.Shape.init(weight_shape.dtype, &.{ @intCast(out_dim), @intCast(in_dim) }));
    const wt = try b.transpose(matmul_weight, &.{ 1, 0 });
    const mm = try b.matmul(matmul_input, wt);
    return if (has_bias) try b.add(mm, bias_id) else mm;
}

fn synthesizeCausalSelfAttention(
    b: *Builder,
    graph: *Graph,
    node_id: NodeId,
    node: *const ml.graph.Node,
    attrs: anytype,
) !NodeId {
    const q_id = node.inputs[0];
    const k_id = node.inputs[1];
    const v_id = node.inputs[2];
    const bias_id = if (node.num_inputs > 3) node.inputs[3] else ml.graph.null_node;
    const output_shape = node.output_shape;

    const batch: i64 = @intCast(attrs.batch);
    const seq_len: i64 = @intCast(attrs.seq_len);
    const num_heads: i64 = @intCast(attrs.num_heads);
    const num_kv_heads: i64 = if (attrs.num_kv_heads > 0) @intCast(attrs.num_kv_heads) else num_heads;
    const head_dim: i64 = @intCast(attrs.head_dim);
    const bh: i64 = batch * num_heads;
    const q_shape = graph.node(q_id).output_shape;

    const q4 = try b.reshape(q_id, ml.graph.Shape.init(q_shape.dtype, &.{ batch, seq_len, num_heads, head_dim }));
    const q_t = try b.transpose(q4, &.{ 0, 2, 1, 3 });
    const q_bh = try b.reshape(q_t, ml.graph.Shape.init(q_shape.dtype, &.{ bh, seq_len, head_dim }));

    const k4_base = try b.reshape(k_id, ml.graph.Shape.init(q_shape.dtype, &.{ batch, seq_len, num_kv_heads, head_dim }));
    const k4 = if (num_kv_heads == num_heads)
        k4_base
    else
        try synthesizeRepeatGqaKvHeads(b, graph, node_id, k4_base, batch, seq_len, num_kv_heads, num_heads, head_dim);
    const k_t_heads = try b.transpose(k4, &.{ 0, 2, 1, 3 });
    const k_bh = try b.reshape(k_t_heads, ml.graph.Shape.init(q_shape.dtype, &.{ bh, seq_len, head_dim }));

    const v4_base = try b.reshape(v_id, ml.graph.Shape.init(q_shape.dtype, &.{ batch, seq_len, num_kv_heads, head_dim }));
    const v4 = if (num_kv_heads == num_heads)
        v4_base
    else
        try synthesizeRepeatGqaKvHeads(b, graph, node_id, v4_base, batch, seq_len, num_kv_heads, num_heads, head_dim);
    const v_t_heads = try b.transpose(v4, &.{ 0, 2, 1, 3 });
    const v_bh = try b.reshape(v_t_heads, ml.graph.Shape.init(q_shape.dtype, &.{ bh, seq_len, head_dim }));

    const k_t = try b.transpose(k_bh, &.{ 0, 2, 1 });
    const scores = try graph.addNode(.{
        .op = .{ .dot_general = .{
            .lhs_contracting = .{ 2, 0, 0, 0, 0, 0, 0, 0 },
            .rhs_contracting = .{ 1, 0, 0, 0, 0, 0, 0, 0 },
            .lhs_batch = .{ 0, 0, 0, 0, 0, 0, 0, 0 },
            .rhs_batch = .{ 0, 0, 0, 0, 0, 0, 0, 0 },
            .num_contracting = 1,
            .num_batch = 1,
        } },
        .output_shape = ml.graph.Shape.init(q_shape.dtype, &.{ bh, seq_len, seq_len }),
        .inputs = .{ q_bh, k_t, ml.graph.null_node, ml.graph.null_node },
        .num_inputs = 2,
    });

    const scale = try b.scalarConst(q_shape.dtype, 1.0 / @sqrt(@as(f32, @floatFromInt(attrs.head_dim))));
    var masked_scores = try b.mul(scores, scale);
    const causal_mask = try emitCausalMask(graph, q_shape.dtype, node_id, bh, seq_len);
    masked_scores = try b.add(masked_scores, causal_mask);
    if (bias_id != ml.graph.null_node) {
        masked_scores = try b.add(masked_scores, bias_id);
    }

    const probs = try b.softmax(masked_scores);
    const attended = try graph.addNode(.{
        .op = .{ .dot_general = .{
            .lhs_contracting = .{ 2, 0, 0, 0, 0, 0, 0, 0 },
            .rhs_contracting = .{ 1, 0, 0, 0, 0, 0, 0, 0 },
            .lhs_batch = .{ 0, 0, 0, 0, 0, 0, 0, 0 },
            .rhs_batch = .{ 0, 0, 0, 0, 0, 0, 0, 0 },
            .num_contracting = 1,
            .num_batch = 1,
        } },
        .output_shape = ml.graph.Shape.init(q_shape.dtype, &.{ bh, seq_len, head_dim }),
        .inputs = .{ probs, v_bh, ml.graph.null_node, ml.graph.null_node },
        .num_inputs = 2,
    });
    const attended_heads = try b.reshape(attended, ml.graph.Shape.init(q_shape.dtype, &.{ batch, num_heads, seq_len, head_dim }));
    const attended_t = try b.transpose(attended_heads, &.{ 0, 2, 1, 3 });
    return b.reshape(attended_t, output_shape);
}

fn synthesizeRepeatGqaKvHeads(
    b: *Builder,
    graph: *Graph,
    node_id: NodeId,
    value4: NodeId,
    batch: i64,
    seq_len: i64,
    num_kv_heads: i64,
    num_heads: i64,
    head_dim: i64,
) !NodeId {
    const repeat_factor = std.math.divExact(i64, num_heads, num_kv_heads) catch return error.UnsupportedShape;
    const value_shape = graph.node(value4).output_shape;
    const transposed = try b.transpose(value4, &.{ 0, 1, 3, 2 });
    const flat_rows = try std.math.mul(i64, try std.math.mul(i64, batch, seq_len), head_dim);
    const flat = try b.reshape(transposed, ml.graph.Shape.init(value_shape.dtype, &.{ flat_rows, num_kv_heads }));

    const repeat_len: usize = @intCast(num_kv_heads * num_heads);
    const repeat_data = try graph.allocator.alloc(f32, repeat_len);
    defer graph.allocator.free(repeat_data);
    @memset(repeat_data, 0.0);
    for (0..@as(usize, @intCast(num_kv_heads))) |kv_head| {
        for (0..@as(usize, @intCast(repeat_factor))) |rep| {
            repeat_data[kv_head * @as(usize, @intCast(num_heads)) + kv_head * @as(usize, @intCast(repeat_factor)) + rep] = 1.0;
        }
    }
    _ = node_id;
    const repeat = try b.tensorConst(
        repeat_data,
        ml.graph.Shape.init(value_shape.dtype, &.{ num_kv_heads, num_heads }),
    );
    const expanded_flat = try b.matmul(flat, repeat);
    const expanded = try b.reshape(expanded_flat, ml.graph.Shape.init(value_shape.dtype, &.{ batch, seq_len, head_dim, num_heads }));
    return b.transpose(expanded, &.{ 0, 1, 3, 2 });
}

fn emitCausalMask(
    graph: *Graph,
    dtype: ml.graph.DType,
    node_id: NodeId,
    batch_heads: i64,
    seq_len: i64,
) !NodeId {
    const elem_count: usize = @intCast(batch_heads * seq_len * seq_len);
    const data = try graph.allocator.alloc(f32, elem_count);
    defer graph.allocator.free(data);
    var idx: usize = 0;
    for (0..@as(usize, @intCast(batch_heads))) |_| {
        for (0..@as(usize, @intCast(seq_len))) |q| {
            for (0..@as(usize, @intCast(seq_len))) |k| {
                data[idx] = if (k <= q) 0.0 else -1.0e9;
                idx += 1;
            }
        }
    }
    const loc = try graph.internConstant(data);
    _ = node_id;
    return graph.addNode(.{
        .op = .{ .constant = .{ .data_offset = loc.offset, .data_len = loc.len } },
        .output_shape = ml.graph.Shape.init(dtype, &.{ batch_heads, seq_len, seq_len }),
    });
}

test "synthesizeLinearDecompositions lowers grouped-query attention to portable ONNX ops" {
    const allocator = std.testing.allocator;

    var graph = Graph.init(allocator);
    defer graph.deinit();
    var b = Builder.init(&graph);

    const q = try b.parameter("q", ml.graph.Shape.init(.f32, &.{ 3, 8 }));
    const k = try b.parameter("k", ml.graph.Shape.init(.f32, &.{ 3, 4 }));
    const v = try b.parameter("v", ml.graph.Shape.init(.f32, &.{ 3, 4 }));
    const gqa = try graph.addNode(.{
        .op = .{ .fused_gqa_causal_attention = .{
            .batch = 1,
            .seq_len = 3,
            .num_heads = 4,
            .num_kv_heads = 2,
            .head_dim = 2,
        } },
        .output_shape = ml.graph.Shape.init(.f32, &.{ 3, 8 }),
        .inputs = .{ q, k, v, ml.graph.null_node },
        .num_inputs = 3,
    });
    try graph.markOutput(gqa);

    try synthesizeLinearDecompositions(&graph, .{});
    try std.testing.expect(graph.node(gqa).vjp_alternate != ml.graph.null_node);

    var lowered = try ml.graph.lower.lower(allocator, &graph);
    defer lowered.deinit();
    const bytes = try onnx_graph.exportGraph(allocator, &lowered.graph, .{ .lower_fused = false });
    defer allocator.free(bytes);

    try std.testing.expect(std.mem.indexOf(u8, bytes, "GroupQueryAttention") == null);
    try std.testing.expect(std.mem.indexOf(u8, bytes, "MatMul") != null);
}

test "semantic decoder bindings mark shared-KV attention reads" {
    const allocator = std.testing.allocator;

    var graph = Graph.init(allocator);
    defer graph.deinit();
    var b = Builder.init(&graph);

    const q0 = try b.parameter("q0", ml.graph.Shape.init(.f32, &.{ 1, 8 }));
    const k0 = try b.parameter("k0", ml.graph.Shape.init(.f32, &.{ 1, 4 }));
    const v0 = try b.parameter("v0", ml.graph.Shape.init(.f32, &.{ 1, 4 }));
    const write_gqa = try graph.addNode(.{
        .op = .{ .fused_gqa_causal_attention = .{
            .batch = 1,
            .seq_len = 1,
            .num_heads = 4,
            .num_kv_heads = 2,
            .head_dim = 2,
            .layer_index = 13,
            .skip_kv_write = false,
        } },
        .output_shape = ml.graph.Shape.init(.f32, &.{ 1, 8 }),
        .inputs = .{ q0, k0, v0, ml.graph.null_node },
        .num_inputs = 3,
    });

    const q1 = try b.parameter("q1", ml.graph.Shape.init(.f32, &.{ 1, 8 }));
    const k1 = try b.parameter("k1", ml.graph.Shape.init(.f32, &.{ 1, 4 }));
    const v1 = try b.parameter("v1", ml.graph.Shape.init(.f32, &.{ 1, 4 }));
    _ = try graph.addNode(.{
        .op = .{ .fused_gqa_causal_attention = .{
            .batch = 1,
            .seq_len = 1,
            .num_heads = 4,
            .num_kv_heads = 2,
            .head_dim = 2,
            .layer_index = 13,
            .skip_kv_write = true,
        } },
        .output_shape = ml.graph.Shape.init(.f32, &.{ 1, 8 }),
        .inputs = .{ q1, k1, v1, ml.graph.null_node },
        .num_inputs = 3,
    });
    try graph.markOutput(write_gqa);

    const bindings = try buildEffectiveSemanticDecoderGqaBindings(allocator, &graph, .{ .semantic_decoder_entrypoint = true });
    defer allocator.free(bindings);

    try std.testing.expectEqual(@as(usize, 2), bindings.len);
    try std.testing.expectEqual(write_gqa, bindings[0].node_id);
    try std.testing.expectEqual(@as(u32, 13), bindings[0].layer_index);
    try std.testing.expect(!bindings[0].skip_kv_write);
    try std.testing.expectEqual(@as(u32, 13), bindings[1].layer_index);
    try std.testing.expect(bindings[1].skip_kv_write);
}

test "semantic input_ids override prefers integer runtime input" {
    const allocator = std.testing.allocator;

    var graph = Graph.init(allocator);
    defer graph.deinit();
    var b = Builder.init(&graph);

    const skip_k = try b.parameter("skip_k", ml.graph.Shape.init(.f32, &.{ 1, 2, 1, 4 }));
    const token_ids = try b.parameter("token_ids", ml.graph.Shape.init(.i64, &.{1}));
    const skip_v = try b.parameter("skip_v", ml.graph.Shape.init(.f32, &.{ 1, 2, 1, 4 }));
    const runtime_inputs = [_]NodeId{ skip_k, skip_v, token_ids };

    try std.testing.expectEqual(token_ids, semanticInputIdsNodeId(&graph, &runtime_inputs));
}
