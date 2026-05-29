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
const platform = @import("antfly_platform");
const build_options = @import("build_options");
const backends = @import("backends/backends.zig");
const session_factory = @import("architectures/session_factory.zig");
const generation = @import("pipelines/generation.zig");
const graph_mod = @import("graph/root.zig");
const model_manager_mod = @import("server/model_manager.zig");
const runtime = @import("runtime/root.zig");
const native_backend_choice = @import("native_backend_choice.zig");
const ops = @import("ops/ops.zig");
const export_source_mod = @import("models/export_source.zig");
const compiled_artifact = @import("compiled_artifact.zig");

const print = std.debug.print;
const BackendChoice = native_backend_choice.Choice;
const Graph = @import("ml").graph.Graph;
const NodeId = @import("ml").graph.NodeId;
const max_debug_output_nodes = 32;
const OnnxWeightExportMode = graph_mod.onnx_compiler.QuantExportMode;
const OnnxWeightExportRule = graph_mod.onnx_compiler.WeightExportRule;
const PjrtInputBinding = if (build_options.enable_pjrt)
    graph_mod.pjrt_compiler.InputBinding
else
    union(enum) {
        graph_node: NodeId,
        embedding_ids: NodeId,
        semantic_past_graph_node: NodeId,
    };
const max_weight_export_rules = 32;

const PjrtArtifactExportKind = enum {
    hlo,
    executable,
};

const PjrtParameterExportMode = enum {
    embedded,
    inputs,
};

const Options = struct {
    model_dir: []const u8,
    prompt: []const u8,
    backend: BackendChoice,
    attention_mode: CompileAttentionMode = .auto,
    onnx_weight_mode: OnnxWeightExportMode = .dense,
    onnx_weight_export_rules: [max_weight_export_rules]OnnxWeightExportRule = undefined,
    onnx_weight_export_rule_count: usize = 0,
    onnx_reuse_initializers_from: ?[]const u8 = null,
    onnx_import_from: ?[]const u8 = null,
    onnx_semantic_entrypoint: bool = false,
    artifact_role: ?[]const u8 = null,
    xla_artifact_kind: PjrtArtifactExportKind = .hlo,
    xla_artifact_kind_explicit: bool = false,
    xla_parameter_mode: PjrtParameterExportMode = .embedded,
    xla_package_decode_max_seq_len: ?usize = null,
    output_path: ?[]const u8 = null,
    list_op_nodes: ?[]const u8 = null,
    list_node_window: ?u32 = null,
    list_node_window_radius: usize = 8,
    node_index: ?u32 = null,
    node_range_start: ?u32 = null,
    node_range_end: ?u32 = null,
    debug_output_nodes: [max_debug_output_nodes]u32 = [_]u32{0} ** max_debug_output_nodes,
    debug_output_node_count: usize = 0,
    node_neighborhood: usize = 0,
    node_closure: bool = false,
    partition_index: ?u32 = null,
    list_partitions: bool = false,
    best_partition: bool = false,
    seq_len: ?usize = null,
    query_seq_len: ?usize = null,
    no_chat_template: bool = false,
    raw_prompt: bool = false,
};

const CompileAttentionMode = enum {
    auto,
    full_recompute,
    paged_prefill,
    paged_decode,
};

const SelectedPartition = struct {
    part: graph_mod.partition.Partition,
    selected_index: ?u32 = null,
    selected_signature: ?[]u8 = null,
    output_label: ?[]u8 = null,
    is_partial_artifact: bool = false,
};

const CompiledArtifactWriteResult = struct {
    xla_artifact_kind: PjrtArtifactExportKind,
};

const RefreshedPackageManifest = struct {
    package_path: []u8,
    artifact_count: usize,
    prefill_count: usize,
    decode_count: usize,

    fn deinit(self: @This(), allocator: std.mem.Allocator) void {
        allocator.free(self.package_path);
    }
};

fn printCompiledPackageSummary(
    backend: []const u8,
    kind: []const u8,
    package_path: []const u8,
    artifact_count: usize,
    prefill_count: usize,
    decode_count: usize,
) void {
    print(
        "compiled package backend={s} kind={s} package={s} artifacts={d} prefill={d} decode={d}\n",
        .{ backend, kind, package_path, artifact_count, prefill_count, decode_count },
    );
}

pub fn main(allocator: std.mem.Allocator, io: std.Io, args: []const []const u8) !void {
    const opts = try parseArgs(args);
    try validateCompileBackend(opts.backend);
    if (opts.onnx_import_from != null) {
        return writeImportedOnnxArtifactManifest(allocator, io, opts);
    }

    var session_manager = backends.SessionManager.initWithIo(allocator, io);
    session_manager.preferred_backends = &.{backends.BackendType.native};
    var model_manager = model_manager_mod.ModelManager.init(allocator, session_manager);
    defer model_manager.deinit();

    const model = try model_manager.loadFromDirWithPreferredBackends(
        opts.model_dir,
        &.{backends.BackendType.native},
        true,
    );
    const gpt_config = session_factory.getGptConfig(model.session) orelse return error.InvalidModelForGeneration;
    const tokenizer = model.getTokenizer();

    const messages = [_]generation.Message{
        .{ .role = "user", .content = opts.prompt },
    };
    const apply_chat_template = !opts.raw_prompt and !opts.no_chat_template and model.chat_tmpl != null;
    const rendered_prompt = if (opts.raw_prompt)
        try allocator.dupe(u8, opts.prompt)
    else if (apply_chat_template)
        try model.chat_tmpl.?.apply(allocator, &messages, true)
    else
        try generation.formatMessages(allocator, &messages);
    defer allocator.free(rendered_prompt);

    var prompt_encoded = try generation.encodePromptForGeneration(
        tokenizer,
        allocator,
        rendered_prompt,
        4096,
        model.manifest.add_bos_token,
        model.manifest.bos_token,
    );
    defer prompt_encoded.deinit();
    const prompt_tokens = countPromptTokens(prompt_encoded.attention_mask);
    const seq_len = opts.seq_len orelse prompt_tokens;
    if (seq_len == 0 or seq_len > prompt_tokens) return error.InvalidSequenceLength;
    const query_seq_len = opts.query_seq_len orelse seq_len;
    if (query_seq_len == 0 or query_seq_len > seq_len) return error.InvalidQuerySequenceLength;
    const input_start = seq_len - query_seq_len;

    const graph_input_ids = try allocator.alloc(i64, query_seq_len);
    defer allocator.free(graph_input_ids);
    for (0..query_seq_len) |i| graph_input_ids[i] = prompt_encoded.ids[input_start + i];

    var cb = try session_factory.getComputeBackend(model.session, allocator);
    defer cb.deinit();
    const weight_export_source = session_factory.getWeightExportSource(model.session);

    var kv_manager = runtime.kv.manager.KvManager.init(allocator);
    defer kv_manager.deinit();

    const backend_kind: runtime.kv.pool.BackendKind = switch (model.session.backend()) {
        .native => .native,
        .metal => .metal,
        .mlx => .mlx,
        .cuda => .cuda,
        .pjrt => return error.UnexpectedPjrtBackend,
        .onnx => return error.UnexpectedOnnxBackend,
        .wasm => return error.UnexpectedWasmBackend,
    };
    const kv_dtype = session_factory.recommendedKvDTypeForSession(model.session, backend_kind);
    const sliding_window_size: ?u32 = if (gpt_config.position_encoding == .absolute)
        null
    else if (gpt_config.sliding_window > 0)
        gpt_config.sliding_window
    else if (gpt_config.max_position_embeddings > 0)
        gpt_config.max_position_embeddings
    else
        null;

    const pool_id = try kv_manager.addPool(.{
        .backend = backend_kind,
        .dtype = kv_dtype,
        .page_size_tokens = 16,
        .num_layers_packed = @intCast(gpt_config.num_hidden_layers),
        .num_kv_heads = gpt_config.maxKvHeads(),
        .head_dim = gpt_config.maxHeadDim(),
        .sliding_window_size = sliding_window_size,
    });

    var decode_state = generation.NativeDecodeState.initPaged(allocator, &kv_manager, pool_id, model.shared_moe_cache);
    defer decode_state.deinit();
    var decode_context = try buildCompileDecodeContext(opts.attention_mode, &decode_state, seq_len, query_seq_len);

    var graph_cache = graph_mod.cache.GraphCache.init(allocator);
    defer graph_cache.deinit();

    const pipeline_stub = .{
        .allocator = allocator,
        .gpt_config = gpt_config,
        .cb = &cb,
    };
    const entry = try graph_mod.execution.ensureGraphEntry(
        &pipeline_stub,
        &graph_cache,
        graph_input_ids,
        1,
        seq_len,
        &decode_context,
    );

    const graph_attention_mode = toGraphAttentionMode(decode_context.attention_mode);
    var selection = if (opts.partition_index != null or opts.list_partitions or opts.best_partition)
        try buildSelectedOrPlannedPartition(
            allocator,
            &entry.graph,
            &cb,
            opts.backend,
            opts.model_dir,
            graph_attention_mode,
            opts.partition_index,
            opts.best_partition,
            opts.list_partitions,
        )
    else if (opts.node_index != null or opts.node_range_start != null or opts.list_op_nodes != null or opts.list_node_window != null)
        try buildDebugNodeSelection(
            allocator,
            &entry.graph,
            opts.backend,
            opts.list_op_nodes,
            opts.list_node_window,
            opts.list_node_window_radius,
            opts.node_index,
            opts.node_range_start,
            opts.node_range_end,
            opts.node_neighborhood,
            opts.node_closure,
        )
    else
        SelectedPartition{ .part = try buildWholeGraphPartition(
            allocator,
            &entry.graph,
            compileBackendKind(opts.backend),
            opts.backend == .onnx and graph_attention_mode != .full_recompute,
        ) };
    defer {
        deinitOwnedPartition(allocator, &selection.part);
        if (selection.selected_signature) |sig| allocator.free(sig);
        if (selection.output_label) |label| allocator.free(label);
    }
    if (opts.list_partitions or opts.list_op_nodes != null or opts.list_node_window != null) return;

    if (opts.xla_package_decode_max_seq_len) |max_decode_seq_len| {
        try writeWholeModelPjrtPackageArtifacts(
            allocator,
            io,
            opts,
            model,
            &cb,
            weight_export_source,
            prompt_tokens,
            seq_len,
            max_decode_seq_len,
            apply_chat_template,
            &entry.graph,
            selection,
            graph_input_ids[query_seq_len - 1],
        );
        return;
    }

    _ = try writeCompiledArtifact(
        allocator,
        io,
        opts,
        &entry.graph,
        &cb,
        weight_export_source,
        selection,
        prompt_tokens,
        seq_len,
        query_seq_len,
        @tagName(decode_context.attention_mode),
        apply_chat_template,
    );
}

fn parseEnvUsize(name: [:0]const u8, default_value: usize) usize {
    const slice = platform.env.getenvSlice(name) orelse return default_value;
    if (slice.len == 0) return default_value;
    return std.fmt.parseInt(usize, slice, 10) catch default_value;
}

fn pjrtMaxWholeModelExecutableExportHloBytes() usize {
    return parseEnvUsize("ANTFLY_INFERENCE_PJRT_MAX_EXECUTABLE_EXPORT_HLO_BYTES", 256 * 1024 * 1024);
}

fn artifactRole(attention_mode: []const u8) []const u8 {
    if (std.mem.eql(u8, attention_mode, "paged_decode")) return compiled_artifact.artifact_role_decode;
    return compiled_artifact.artifact_role_prefill;
}

fn onnxExportEstimateOnlyEnabled() bool {
    return platform.env.getenv("ANTFLY_INFERENCE_ONNX_EXPORT_ESTIMATE_ONLY") != null;
}

fn shouldUseSemanticOnnxEntrypoint(
    explicit: bool,
    is_partial_artifact: bool,
    attention_mode: []const u8,
    seq_len: usize,
    query_seq_len: usize,
    debug_output_count: usize,
) bool {
    if (explicit) return true;
    if (std.mem.eql(u8, attention_mode, "paged_decode")) return true;
    if (!std.mem.eql(u8, attention_mode, "paged_prefill")) return false;
    if (seq_len != query_seq_len) return false;
    if (is_partial_artifact) return false;
    if (debug_output_count != 0) return false;
    return true;
}

fn writeImportedOnnxArtifactManifest(
    allocator: std.mem.Allocator,
    io: std.Io,
    opts: Options,
) !void {
    if (opts.backend != .onnx) return error.InvalidArgs;
    const import_path = opts.onnx_import_from orelse return error.InvalidArgs;
    const seq_len = opts.seq_len orelse return error.InvalidArgs;
    const query_seq_len = opts.query_seq_len orelse return error.InvalidArgs;
    const attention_mode = switch (opts.attention_mode) {
        .auto => return error.InvalidArgs,
        .full_recompute => "full_recompute",
        .paged_prefill => "paged_prefill",
        .paged_decode => "paged_decode",
    };
    const role = opts.artifact_role orelse artifactRole(attention_mode);
    if (!std.mem.eql(u8, role, compiled_artifact.artifact_role_prefill) and
        !std.mem.eql(u8, role, compiled_artifact.artifact_role_decode))
    {
        return error.InvalidArgs;
    }

    const output_path_is_default = opts.output_path == null;
    const output_path = if (opts.output_path) |path|
        path
    else
        try defaultOutputPath(
            allocator,
            opts.model_dir,
            .onnx,
            opts.xla_artifact_kind,
            null,
            role,
            seq_len,
            query_seq_len,
            attention_mode,
        );
    defer if (output_path_is_default) allocator.free(output_path);

    try ensureParentDir(io, output_path);
    const manifest_path = try compiled_artifact.artifactManifestPath(allocator, output_path);
    defer allocator.free(manifest_path);

    try compiled_artifact.writeManifest(allocator, io, manifest_path, .{
        .kind = artifactKind(.onnx, false, opts.xla_artifact_kind),
        .artifact_role = role,
        .backend = "onnx",
        .model_dir = opts.model_dir,
        .artifact_path = import_path,
        .source_path = import_path,
        .prompt_tokens = seq_len,
        .seq_len = seq_len,
        .query_seq_len = query_seq_len,
        .attention_mode = attention_mode,
        .raw_prompt = opts.raw_prompt,
        .chat_template_applied = false,
    });
    const refreshed_package = try refreshWholeModelPackageManifest(
        allocator,
        io,
        std.fs.path.dirname(manifest_path) orelse ".",
        "onnx",
        opts.model_dir,
        artifactKind(.onnx, false, opts.xla_artifact_kind),
        null,
    );
    defer refreshed_package.deinit(allocator);
    print(
        "imported backend=onnx artifact_role={s} seq_len={d} query_seq_len={d} attention_mode={s} artifact={s} manifest={s}\n",
        .{ role, seq_len, query_seq_len, attention_mode, import_path, manifest_path },
    );
    printCompiledPackageSummary(
        "onnx",
        artifactKind(.onnx, false, opts.xla_artifact_kind),
        refreshed_package.package_path,
        refreshed_package.artifact_count,
        refreshed_package.prefill_count,
        refreshed_package.decode_count,
    );
}

fn writeCompiledArtifact(
    allocator: std.mem.Allocator,
    io: std.Io,
    opts: Options,
    graph: *const Graph,
    cb: *const ops.ComputeBackend,
    weight_export_source: ?export_source_mod.Source,
    selection: SelectedPartition,
    prompt_tokens: usize,
    seq_len: usize,
    query_seq_len: usize,
    attention_mode: []const u8,
    apply_chat_template: bool,
) !CompiledArtifactWriteResult {
    var resolved_xla_artifact_kind = try resolveXlaArtifactKind(
        allocator,
        opts.backend,
        opts.xla_artifact_kind,
        opts.xla_artifact_kind_explicit,
        selection.is_partial_artifact,
    );
    const debug_output_label = if (opts.backend == .onnx and opts.debug_output_node_count > 0 and selection.output_label == null)
        try buildOnnxDebugOutputLabel(allocator, opts.debug_output_nodes[0..opts.debug_output_node_count])
    else
        null;
    defer if (debug_output_label) |label| allocator.free(label);

    const output_path_is_default = opts.output_path == null;
    var output_path = if (opts.output_path) |path|
        path
    else
        try defaultOutputPath(
            allocator,
            opts.model_dir,
            opts.backend,
            resolved_xla_artifact_kind,
            selection.selected_index,
            selection.output_label orelse debug_output_label,
            seq_len,
            query_seq_len,
            attention_mode,
        );
    defer if (output_path_is_default) allocator.free(output_path);

    try ensureParentDir(io, output_path);

    var artifact_path = output_path;
    var onnx_input_node_ids: []u32 = &.{};
    var onnx_input_names: [][]u8 = &.{};
    var onnx_output_node_ids: []u32 = &.{};
    var onnx_output_names: [][]u8 = &.{};
    var pjrt_input_bindings: []compiled_artifact.PjrtInputBindingMeta = &.{};
    var pjrt_output_node_ids: []u32 = &.{};
    var pjrt_output_bindings: []compiled_artifact.PjrtOutputBindingMeta = &.{};
    var pjrt_input_shapes: [][]i64 = &.{};
    var pjrt_output_shapes: [][]i64 = &.{};
    const attention_bindings = try buildAttentionBindingMeta(allocator, graph, &selection.part);
    defer allocator.free(attention_bindings);
    defer {
        if (onnx_input_node_ids.len > 0) allocator.free(onnx_input_node_ids);
        for (onnx_input_names) |name| allocator.free(name);
        if (onnx_input_names.len > 0) allocator.free(onnx_input_names);
        if (onnx_output_node_ids.len > 0) allocator.free(onnx_output_node_ids);
        for (onnx_output_names) |name| allocator.free(name);
        if (onnx_output_names.len > 0) allocator.free(onnx_output_names);
        freePjrtInputBindingMeta(allocator, pjrt_input_bindings);
        if (pjrt_output_node_ids.len > 0) allocator.free(pjrt_output_node_ids);
        freePjrtOutputBindingMeta(allocator, pjrt_output_bindings);
        freeShapeSlices(allocator, pjrt_input_shapes);
        freeShapeSlices(allocator, pjrt_output_shapes);
    }

    const implicit_whole_model_xla_executable = opts.backend == .xla and
        !selection.is_partial_artifact and
        !opts.xla_artifact_kind_explicit and
        opts.xla_artifact_kind == .hlo and
        resolved_xla_artifact_kind == .executable;

    switch (opts.backend) {
        .onnx => {
            if (opts.node_closure and opts.onnx_reuse_initializers_from == null and !onnxExportEstimateOnlyEnabled()) {
                std.log.err(
                    "ONNX node-closure exports duplicate large external weight blobs; use --onnx-reuse-initializers-from <existing.onnx> or --debug-output-node for lightweight probes",
                    .{},
                );
                return error.InvalidArguments;
            }
            if (opts.onnx_reuse_initializers_from) |reuse_path| {
                try guardOnnxInitializerReuseOutput(output_path, reuse_path);
            }
            var extra_output_storage: [max_debug_output_nodes]NodeId = undefined;
            for (opts.debug_output_nodes[0..opts.debug_output_node_count], 0..) |node_id, idx| {
                extra_output_storage[idx] = @intCast(node_id);
            }
            const extra_output_node_ids = extra_output_storage[0..opts.debug_output_node_count];
            const external_data_name = try std.fmt.allocPrint(allocator, "{s}.weights.bin", .{std.fs.path.basename(output_path)});
            defer allocator.free(external_data_name);
            const output_dir = std.fs.path.dirname(output_path) orelse ".";
            const external_output_path = try std.fs.path.join(allocator, &.{ output_dir, external_data_name });
            defer allocator.free(external_output_path);
            const semantic_decoder_entrypoint = shouldUseSemanticOnnxEntrypoint(
                opts.onnx_semantic_entrypoint,
                selection.is_partial_artifact,
                attention_mode,
                seq_len,
                query_seq_len,
                opts.debug_output_node_count,
            );
            var result = graph_mod.onnx_compiler.compilePartition(
                allocator,
                graph,
                &selection.part,
                cb,
                weight_export_source,
                .{
                    .default_mode = opts.onnx_weight_mode,
                    .rules = opts.onnx_weight_export_rules[0..opts.onnx_weight_export_rule_count],
                },
                external_data_name,
                external_output_path,
                opts.onnx_reuse_initializers_from,
                extra_output_node_ids,
                .{ .semantic_decoder_entrypoint = semantic_decoder_entrypoint },
            ) catch |err| switch (err) {
                error.ConstantPoolTooLarge => {
                    std.log.err(
                        "ONNX full-model export requires external-data weight export for this graph shape; the Antfly inference graph constant pool exceeds 4 GiB",
                        .{},
                    );
                    return err;
                },
                error.ExportEstimateOnly => return .{ .xla_artifact_kind = resolved_xla_artifact_kind },
                else => return err,
            };
            defer result.deinit();
            onnx_input_node_ids = try cloneNodeIdsAsU32(allocator, result.input_node_ids);
            onnx_input_names = try cloneStringSlices(allocator, result.input_names);
            onnx_output_node_ids = try cloneNodeIdsAsU32(allocator, result.output_node_ids);
            onnx_output_names = try cloneStringSlices(allocator, result.output_names);
            try std.Io.Dir.cwd().writeFile(io, .{
                .sub_path = output_path,
                .data = result.onnx_bytes,
            });
            if (result.external_data_path != null and result.external_data_bytes.len > 0) {
                try std.Io.Dir.cwd().writeFile(io, .{
                    .sub_path = external_output_path,
                    .data = result.external_data_bytes,
                });
            }
        },
        .xla => {
            if (comptime build_options.enable_pjrt) {
                const semantic_kv_bindings = !selection.is_partial_artifact and !std.mem.eql(u8, attention_mode, "full_recompute");
                const semantic_kv_inputs = semantic_kv_bindings and std.mem.eql(u8, attention_mode, "paged_decode");
                var result = try graph_mod.pjrt_compiler.compilePartitionWithOptions(
                    allocator,
                    graph,
                    &selection.part,
                    cb,
                    .{
                        .parameter_inputs = opts.xla_parameter_mode == .inputs,
                        .semantic_kv_bindings = semantic_kv_bindings,
                        .semantic_kv_inputs = semantic_kv_inputs,
                    },
                );
                defer result.deinit();
                if (result.hlo_bytes.len == 0) return error.BackendUnavailable;
                pjrt_input_bindings = try buildPjrtInputBindingMeta(allocator, result.input_bindings, attention_bindings);
                pjrt_input_shapes = try cloneShapeSlices(allocator, result.input_shapes);
                pjrt_output_node_ids = try cloneNodeIdsAsU32(allocator, result.output_node_ids);
                pjrt_output_bindings = try buildPjrtOutputBindingMeta(allocator, result.output_node_ids, attention_bindings);
                pjrt_output_shapes = try cloneShapeSlices(allocator, result.output_shapes);
                switch (resolved_xla_artifact_kind) {
                    .hlo => try std.Io.Dir.cwd().writeFile(io, .{
                        .sub_path = output_path,
                        .data = result.hlo_bytes,
                    }),
                    .executable => writePjrtExecutableArtifact(
                        allocator,
                        io,
                        output_path,
                        result.hlo_bytes,
                        result.output_node_ids.len,
                        selection.is_partial_artifact,
                    ) catch |err| {
                        if (!implicit_whole_model_xla_executable) return err;
                        switch (err) {
                            error.OutOfMemory, error.MissingPjrtPluginPath => {
                                std.log.info(
                                    "PJRT whole-model export default fell back to HLO: err={s}",
                                    .{@errorName(err)},
                                );
                                resolved_xla_artifact_kind = .hlo;
                                try updateDefaultXlaOutputPathForResolvedKind(
                                    allocator,
                                    &output_path,
                                    output_path_is_default,
                                    opts.model_dir,
                                    resolved_xla_artifact_kind,
                                    selection.selected_index,
                                    selection.output_label orelse debug_output_label,
                                    seq_len,
                                    query_seq_len,
                                    attention_mode,
                                );
                                artifact_path = output_path;
                                try std.Io.Dir.cwd().writeFile(io, .{
                                    .sub_path = output_path,
                                    .data = result.hlo_bytes,
                                });
                            },
                            else => return err,
                        }
                    },
                }
            } else {
                return error.BackendUnavailable;
            }
        },
        else => return error.UnsupportedCompileBackend,
    }

    const manifest_path = try compiled_artifact.artifactManifestPath(allocator, artifact_path);
    defer allocator.free(manifest_path);
    try compiled_artifact.writeManifest(
        allocator,
        io,
        manifest_path,
        .{
            .kind = artifactKind(opts.backend, selection.is_partial_artifact, resolved_xla_artifact_kind),
            .artifact_role = opts.artifact_role orelse artifactRole(attention_mode),
            .backend = @tagName(opts.backend),
            .model_dir = opts.model_dir,
            .artifact_path = artifact_path,
            .source_path = "",
            .partition_signature = if (selection.selected_signature) |sig| sig else "",
            .prompt_tokens = prompt_tokens,
            .seq_len = seq_len,
            .query_seq_len = query_seq_len,
            .attention_mode = attention_mode,
            .raw_prompt = opts.raw_prompt,
            .chat_template_applied = apply_chat_template,
            .attention_bindings = attention_bindings,
            .onnx_input_node_ids = onnx_input_node_ids,
            .onnx_input_names = onnx_input_names,
            .onnx_output_node_ids = onnx_output_node_ids,
            .onnx_output_names = onnx_output_names,
            .pjrt_parameter_mode = switch (opts.xla_parameter_mode) {
                .embedded => compiled_artifact.pjrt_parameter_mode_embedded,
                .inputs => compiled_artifact.pjrt_parameter_mode_inputs,
            },
            .pjrt_input_bindings = pjrt_input_bindings,
            .pjrt_output_node_ids = pjrt_output_node_ids,
            .pjrt_output_bindings = pjrt_output_bindings,
            .pjrt_input_shapes = pjrt_input_shapes,
            .pjrt_output_shapes = pjrt_output_shapes,
        },
    );
    const refreshed_package = if (!selection.is_partial_artifact and (opts.backend == .xla or opts.backend == .onnx))
        try refreshWholeModelPackageManifest(
            allocator,
            io,
            std.fs.path.dirname(manifest_path) orelse ".",
            @tagName(opts.backend),
            opts.model_dir,
            artifactKind(opts.backend, selection.is_partial_artifact, resolved_xla_artifact_kind),
            if (opts.backend == .xla) switch (opts.xla_parameter_mode) {
                .embedded => compiled_artifact.pjrt_parameter_mode_embedded,
                .inputs => compiled_artifact.pjrt_parameter_mode_inputs,
            } else null,
        )
    else
        null;
    defer if (refreshed_package) |pkg| pkg.deinit(allocator);

    print(
        "compiled backend={s} prompt_tokens={d} seq_len={d} query_seq_len={d} attention_mode={s} output={s} manifest={s}\n",
        .{
            @tagName(opts.backend),
            prompt_tokens,
            seq_len,
            query_seq_len,
            attention_mode,
            artifact_path,
            manifest_path,
        },
    );
    if (refreshed_package) |pkg| {
        if (opts.xla_package_decode_max_seq_len == null) {
            printCompiledPackageSummary(
                @tagName(opts.backend),
                artifactKind(opts.backend, selection.is_partial_artifact, resolved_xla_artifact_kind),
                pkg.package_path,
                pkg.artifact_count,
                pkg.prefill_count,
                pkg.decode_count,
            );
        }
    }
    return .{ .xla_artifact_kind = resolved_xla_artifact_kind };
}

fn writeWholeModelPjrtPackageArtifacts(
    allocator: std.mem.Allocator,
    io: std.Io,
    opts: Options,
    model: *model_manager_mod.LoadedModel,
    cb: *const ops.ComputeBackend,
    weight_export_source: ?export_source_mod.Source,
    prompt_tokens: usize,
    prefill_seq_len: usize,
    max_decode_seq_len: usize,
    apply_chat_template: bool,
    prefill_graph: *const Graph,
    prefill_selection: SelectedPartition,
    decode_token_id: i64,
) !void {
    try validateWholeModelPjrtPackageRequest(opts, prefill_selection.is_partial_artifact, prefill_seq_len, max_decode_seq_len);

    var package_opts = opts;
    package_opts.query_seq_len = prefill_seq_len;
    package_opts.seq_len = prefill_seq_len;
    package_opts.attention_mode = .paged_prefill;
    package_opts.artifact_role = compiled_artifact.artifact_role_prefill;

    const prefill_result = try writeCompiledArtifact(
        allocator,
        io,
        package_opts,
        prefill_graph,
        cb,
        weight_export_source,
        prefill_selection,
        prompt_tokens,
        prefill_seq_len,
        prefill_seq_len,
        "paged_prefill",
        apply_chat_template,
    );

    package_opts.xla_artifact_kind = prefill_result.xla_artifact_kind;
    package_opts.xla_artifact_kind_explicit = true;
    package_opts.attention_mode = .paged_decode;
    package_opts.artifact_role = compiled_artifact.artifact_role_decode;

    var decode_seq_len = prefill_seq_len + 1;
    while (decode_seq_len <= max_decode_seq_len) : (decode_seq_len += 1) {
        package_opts.seq_len = decode_seq_len;
        package_opts.query_seq_len = 1;

        _ = try writeWholeModelPjrtArtifactForShape(
            allocator,
            io,
            package_opts,
            model,
            cb,
            weight_export_source,
            prompt_tokens,
            decode_seq_len,
            1,
            decode_token_id,
            apply_chat_template,
        );
    }

    const package_path = try wholeModelPjrtPackageManifestPath(
        allocator,
        opts.model_dir,
        prefill_result.xla_artifact_kind,
        opts.xla_parameter_mode,
    );
    defer allocator.free(package_path);
    printCompiledPackageSummary(
        "xla",
        artifactKind(.xla, false, prefill_result.xla_artifact_kind),
        package_path,
        1 + (max_decode_seq_len - prefill_seq_len),
        1,
        max_decode_seq_len - prefill_seq_len,
    );
}

fn validateWholeModelPjrtPackageRequest(
    opts: Options,
    is_partial_artifact: bool,
    prefill_seq_len: usize,
    max_decode_seq_len: usize,
) !void {
    if (opts.backend != .xla) return error.UnsupportedCompileBackend;
    if (opts.output_path != null) return error.InvalidArguments;
    if (is_partial_artifact) return error.InvalidArguments;
    if (opts.artifact_role != null and !std.mem.eql(u8, opts.artifact_role.?, compiled_artifact.artifact_role_prefill)) {
        return error.InvalidArguments;
    }
    if (opts.partition_index != null or opts.best_partition) {
        return error.InvalidArguments;
    }
    if (opts.node_index != null or
        opts.node_range_start != null or
        opts.list_op_nodes != null or
        opts.list_node_window != null or
        opts.debug_output_node_count > 0 or
        opts.node_closure or
        opts.node_neighborhood != 0)
    {
        return error.InvalidArguments;
    }
    if (max_decode_seq_len <= prefill_seq_len) return error.InvalidSequenceLength;
    if (opts.query_seq_len != null and opts.query_seq_len.? != prefill_seq_len) return error.InvalidQuerySequenceLength;
    switch (opts.attention_mode) {
        .auto, .paged_prefill => {},
        else => return error.InvalidArguments,
    }
}

fn wholeModelPjrtPackageManifestPath(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    xla_artifact_kind: PjrtArtifactExportKind,
    parameter_mode: PjrtParameterExportMode,
) ![]u8 {
    const artifact_dir = try compiled_artifact.defaultArtifactDirForModel(allocator, model_dir, "xla");
    defer allocator.free(artifact_dir);
    return compiled_artifact.packageManifestPath(
        allocator,
        artifact_dir,
        "xla",
        model_dir,
        artifactKind(.xla, false, xla_artifact_kind),
        switch (parameter_mode) {
            .embedded => compiled_artifact.pjrt_parameter_mode_embedded,
            .inputs => compiled_artifact.pjrt_parameter_mode_inputs,
        },
    );
}

fn writeWholeModelPjrtArtifactForShape(
    allocator: std.mem.Allocator,
    io: std.Io,
    opts: Options,
    model: *model_manager_mod.LoadedModel,
    cb: *const ops.ComputeBackend,
    weight_export_source: ?export_source_mod.Source,
    prompt_tokens: usize,
    seq_len: usize,
    query_seq_len: usize,
    token_id: i64,
    apply_chat_template: bool,
) !CompiledArtifactWriteResult {
    if (query_seq_len != 1) return error.InvalidQuerySequenceLength;
    const gpt_config = session_factory.getGptConfig(model.session) orelse return error.InvalidModelForGeneration;
    var kv_manager = runtime.kv.manager.KvManager.init(allocator);
    defer kv_manager.deinit();

    const backend_kind: runtime.kv.pool.BackendKind = switch (model.session.backend()) {
        .native => .native,
        .metal => .metal,
        .mlx => .mlx,
        .cuda => .cuda,
        .pjrt => return error.UnexpectedPjrtBackend,
        .onnx => return error.UnexpectedOnnxBackend,
        .wasm => return error.UnexpectedWasmBackend,
    };
    const kv_dtype = session_factory.recommendedKvDTypeForSession(model.session, backend_kind);
    const sliding_window_size: ?u32 = if (gpt_config.position_encoding == .absolute)
        null
    else if (gpt_config.sliding_window > 0)
        gpt_config.sliding_window
    else if (gpt_config.max_position_embeddings > 0)
        gpt_config.max_position_embeddings
    else
        null;

    const pool_id = try kv_manager.addPool(.{
        .backend = backend_kind,
        .dtype = kv_dtype,
        .page_size_tokens = 16,
        .num_layers_packed = @intCast(gpt_config.num_hidden_layers),
        .num_kv_heads = gpt_config.maxKvHeads(),
        .head_dim = gpt_config.maxHeadDim(),
        .sliding_window_size = sliding_window_size,
    });

    var decode_state = generation.NativeDecodeState.initPaged(allocator, &kv_manager, pool_id, model.shared_moe_cache);
    defer decode_state.deinit();
    var decode_context = try buildCompileDecodeContext(opts.attention_mode, &decode_state, seq_len, query_seq_len);

    var graph_cache = graph_mod.cache.GraphCache.init(allocator);
    defer graph_cache.deinit();

    const graph_input_ids = [_]i64{token_id};
    const pipeline_stub = .{
        .allocator = allocator,
        .gpt_config = gpt_config,
        .cb = cb,
    };
    const entry = try graph_mod.execution.ensureGraphEntry(
        &pipeline_stub,
        &graph_cache,
        graph_input_ids[0..query_seq_len],
        1,
        seq_len,
        &decode_context,
    );

    const graph_attention_mode = toGraphAttentionMode(decode_context.attention_mode);
    const selection = SelectedPartition{
        .part = try buildWholeGraphPartition(
            allocator,
            &entry.graph,
            compileBackendKind(opts.backend),
            opts.backend == .onnx and graph_attention_mode != .full_recompute,
        ),
    };
    defer deinitOwnedPartition(allocator, &selection.part);

    return writeCompiledArtifact(
        allocator,
        io,
        opts,
        &entry.graph,
        cb,
        weight_export_source,
        selection,
        prompt_tokens,
        seq_len,
        query_seq_len,
        @tagName(decode_context.attention_mode),
        apply_chat_template,
    );
}

fn refreshWholeModelPackageManifest(
    allocator: std.mem.Allocator,
    io: std.Io,
    artifact_dir: []const u8,
    backend: []const u8,
    model_dir: []const u8,
    kind: []const u8,
    parameter_mode: ?[]const u8,
) !RefreshedPackageManifest {
    const package_path = try compiled_artifact.packageManifestPath(
        allocator,
        artifact_dir,
        backend,
        model_dir,
        kind,
        parameter_mode,
    );
    defer allocator.free(package_path);

    var dir = if (std.fs.path.isAbsolute(artifact_dir))
        try std.Io.Dir.openDirAbsolute(io, artifact_dir, .{ .iterate = true })
    else
        try std.Io.Dir.cwd().openDir(io, artifact_dir, .{ .iterate = true });
    defer dir.close(io);

    var entries = std.ArrayListUnmanaged(compiled_artifact.PackageArtifactEntry).empty;
    defer entries.deinit(allocator);

    var iter = dir.iterate();
    while (try iter.next(io)) |entry| {
        if (entry.kind != .file) continue;
        if (!std.mem.endsWith(u8, entry.name, ".inference.json")) continue;

        const candidate_path = try std.fs.path.join(allocator, &.{ artifact_dir, entry.name });
        defer allocator.free(candidate_path);
        var parsed = compiled_artifact.readManifest(allocator, io, candidate_path) catch |err| switch (err) {
            error.FileNotFound, error.InvalidCharacter, error.SyntaxError, error.UnexpectedToken, error.UnknownField => continue,
            else => return err,
        };
        defer parsed.deinit();

        const manifest = parsed.value;
        if (!std.mem.eql(u8, manifest.backend, backend) or
            !std.mem.eql(u8, manifest.model_dir, model_dir) or
            !std.mem.eql(u8, manifest.kind, kind))
        {
            continue;
        }
        if (parameter_mode) |mode| {
            if (!std.mem.eql(u8, manifest.pjrt_parameter_mode, mode)) continue;
        }
        if (!std.mem.eql(u8, manifest.artifact_role, compiled_artifact.artifact_role_prefill) and
            !std.mem.eql(u8, manifest.artifact_role, compiled_artifact.artifact_role_decode))
        {
            continue;
        }

        try entries.append(allocator, .{
            .manifest_path = try allocator.dupe(u8, candidate_path),
            .artifact_path = try allocator.dupe(u8, manifest.artifact_path),
            .artifact_role = try allocator.dupe(u8, manifest.artifact_role),
            .seq_len = manifest.seq_len,
            .query_seq_len = manifest.query_seq_len,
            .attention_mode = try allocator.dupe(u8, manifest.attention_mode),
        });
    }
    defer {
        for (entries.items) |item| {
            allocator.free(item.manifest_path);
            allocator.free(item.artifact_path);
            allocator.free(item.artifact_role);
            allocator.free(item.attention_mode);
        }
    }

    std.mem.sort(compiled_artifact.PackageArtifactEntry, entries.items, {}, struct {
        fn lessThan(_: void, a: compiled_artifact.PackageArtifactEntry, b: compiled_artifact.PackageArtifactEntry) bool {
            if (!std.mem.eql(u8, a.artifact_role, b.artifact_role)) {
                return std.mem.eql(u8, a.artifact_role, compiled_artifact.artifact_role_prefill);
            }
            if (a.seq_len != b.seq_len) return a.seq_len < b.seq_len;
            if (a.query_seq_len != b.query_seq_len) return a.query_seq_len < b.query_seq_len;
            return std.mem.lessThan(u8, a.manifest_path, b.manifest_path);
        }
    }.lessThan);

    var prefill_count: usize = 0;
    var decode_count: usize = 0;
    for (entries.items) |item| {
        if (std.mem.eql(u8, item.artifact_role, compiled_artifact.artifact_role_prefill)) {
            prefill_count += 1;
        } else if (std.mem.eql(u8, item.artifact_role, compiled_artifact.artifact_role_decode)) {
            decode_count += 1;
        }
    }

    try compiled_artifact.writePackageManifest(allocator, io, package_path, .{
        .backend = backend,
        .model_dir = model_dir,
        .kind = kind,
        .pjrt_parameter_mode = parameter_mode orelse compiled_artifact.pjrt_parameter_mode_embedded,
        .artifacts = entries.items,
    });
    return .{
        .package_path = try allocator.dupe(u8, package_path),
        .artifact_count = entries.items.len,
        .prefill_count = prefill_count,
        .decode_count = decode_count,
    };
}

fn guardPjrtExecutableExportBudget(is_partial_artifact: bool, hlo_bytes_len: usize) !void {
    return guardPjrtExecutableExportBudgetWithMax(
        is_partial_artifact,
        hlo_bytes_len,
        pjrtMaxWholeModelExecutableExportHloBytes(),
    );
}

fn guardPjrtExecutableExportBudgetWithMax(is_partial_artifact: bool, hlo_bytes_len: usize, max_bytes: usize) !void {
    if (is_partial_artifact) return;
    if (hlo_bytes_len <= max_bytes) return;
    std.log.warn(
        "PJRT executable export refused: hlo_bytes={d} max_bytes={d}",
        .{ hlo_bytes_len, max_bytes },
    );
    std.log.info(
        "PJRT executable export guidance: use --best-partition for a bounded proof, export HLO for validation, or raise ANTFLY_INFERENCE_PJRT_MAX_EXECUTABLE_EXPORT_HLO_BYTES explicitly",
        .{},
    );
    return error.OutOfMemory;
}

fn shapeApproxByteSize(shape: @import("ml").graph.Shape) usize {
    const numel = shape.numElements() orelse return 0;
    return @as(usize, @intCast(numel)) * shape.dtype.byteSize();
}

fn deinitOwnedPartition(allocator: std.mem.Allocator, part: *const graph_mod.partition.Partition) void {
    allocator.free(part.node_ids);
    allocator.free(part.external_inputs);
}

fn compileBackendKind(choice: BackendChoice) ops.BackendKind {
    return switch (choice) {
        .onnx => .onnx,
        .xla => .pjrt,
        else => unreachable,
    };
}

fn buildWholeGraphPartition(
    allocator: std.mem.Allocator,
    graph: *const @import("ml").graph.Graph,
    backend: ops.BackendKind,
    explicit_skip_kv_inputs: bool,
) !graph_mod.partition.Partition {
    const node_ids = try allocator.alloc(@import("ml").graph.NodeId, graph.nodeCount());
    for (0..graph.nodeCount()) |i| node_ids[i] = @intCast(i);
    var external_inputs = std.ArrayListUnmanaged(graph_mod.partition.ExternalInput).empty;
    errdefer external_inputs.deinit(allocator);
    if (explicit_skip_kv_inputs) {
        try appendSkipKvAttentionExternalInputs(allocator, graph, &external_inputs);
    }
    return .{
        .backend = backend,
        .node_ids = node_ids,
        .external_inputs = try external_inputs.toOwnedSlice(allocator),
    };
}

fn appendSkipKvAttentionExternalInputs(
    allocator: std.mem.Allocator,
    graph: *const Graph,
    external_inputs: *std.ArrayListUnmanaged(graph_mod.partition.ExternalInput),
) !void {
    for (0..graph.nodeCount()) |i| {
        const node_id: NodeId = @intCast(i);
        const node = graph.node(node_id);
        switch (node.op) {
            .fused_gqa_causal_attention => |attrs| {
                if (!attrs.skip_kv_write) continue;
                const inputs = node.getInputs();
                if (inputs.len < 3) return error.UnsupportedShape;
                try appendUniqueExternalInput(allocator, external_inputs, inputs[1]);
                try appendUniqueExternalInput(allocator, external_inputs, inputs[2]);
            },
            else => {},
        }
    }
}

fn appendUniqueExternalInput(
    allocator: std.mem.Allocator,
    external_inputs: *std.ArrayListUnmanaged(graph_mod.partition.ExternalInput),
    node_id: NodeId,
) !void {
    for (external_inputs.items) |existing| {
        if (existing.node_id == node_id) return;
    }
    try external_inputs.append(allocator, .{
        .node_id = node_id,
        .source_partition = 0,
    });
}

fn buildDebugNodeSelection(
    allocator: std.mem.Allocator,
    graph: *const Graph,
    backend: BackendChoice,
    list_op_nodes: ?[]const u8,
    list_node_window: ?u32,
    list_node_window_radius: usize,
    node_index: ?u32,
    node_range_start: ?u32,
    node_range_end: ?u32,
    node_neighborhood: usize,
    node_closure: bool,
) !SelectedPartition {
    if (list_op_nodes) |op_name| {
        printMatchingOpNodes(graph, op_name);
        return .{
            .part = .{
                .backend = compileBackendKind(backend),
                .node_ids = try allocator.dupe(NodeId, &.{}),
                .external_inputs = try allocator.dupe(graph_mod.partition.ExternalInput, &.{}),
            },
        };
    }

    if (list_node_window) |center| {
        if (center >= graph.nodeCount()) return error.InvalidPartitionIndex;
        printNodeWindow(graph, center, list_node_window_radius);
        return .{
            .part = .{
                .backend = compileBackendKind(backend),
                .node_ids = try allocator.dupe(NodeId, &.{}),
                .external_inputs = try allocator.dupe(graph_mod.partition.ExternalInput, &.{}),
            },
        };
    }

    if (node_range_start != null or node_range_end != null) {
        const start_u32 = node_range_start orelse return error.InvalidArgs;
        const end_u32 = node_range_end orelse return error.InvalidArgs;
        if (start_u32 > end_u32 or end_u32 >= graph.nodeCount()) return error.InvalidPartitionIndex;
        if (node_index != null or node_closure or node_neighborhood != 0) return error.InvalidArgs;
        const start: NodeId = @intCast(start_u32);
        const end: NodeId = @intCast(end_u32);
        printSelectedNodeRangeSummary(graph, start, end);
        return .{
            .output_label = try std.fmt.allocPrint(allocator, "nodes{d}_{d}", .{ start, end }),
            .is_partial_artifact = true,
            .part = try buildNodeRangePartition(allocator, graph, start, end, compileBackendKind(backend)),
        };
    }

    const nid = node_index orelse return error.InvalidPartitionIndex;
    if (nid >= graph.nodeCount()) return error.InvalidPartitionIndex;
    if (node_closure and node_neighborhood != 0) return error.InvalidArgs;
    printSelectedNodeSummary(graph, nid, node_neighborhood, node_closure);
    return .{
        .selected_index = nid,
        .output_label = if (node_closure)
            try std.fmt.allocPrint(allocator, "node{d}.closure.{s}", .{ nid, @tagName(graph.node(nid).op) })
        else if (node_neighborhood == 0)
            try std.fmt.allocPrint(allocator, "node{d}.{s}", .{ nid, @tagName(graph.node(nid).op) })
        else
            try std.fmt.allocPrint(allocator, "node{d}.nb{d}.{s}", .{ nid, node_neighborhood, @tagName(graph.node(nid).op) }),
        .is_partial_artifact = true,
        .part = if (node_closure)
            try buildNodeClosurePartition(allocator, graph, nid, compileBackendKind(backend))
        else
            try buildNodeNeighborhoodPartition(allocator, graph, nid, node_neighborhood, compileBackendKind(backend)),
    };
}

fn buildNodeClosurePartition(
    allocator: std.mem.Allocator,
    graph: *const Graph,
    node_index: NodeId,
    backend: ops.BackendKind,
) !graph_mod.partition.Partition {
    const count = graph.nodeCount();
    var included = try allocator.alloc(bool, count);
    defer allocator.free(included);
    @memset(included, false);

    var stack = std.ArrayListUnmanaged(NodeId).empty;
    defer stack.deinit(allocator);
    try stack.append(allocator, node_index);

    while (stack.pop()) |nid| {
        if (included[nid]) continue;
        included[nid] = true;
        const node = graph.node(nid);
        for (node.getInputs(), 0..) |input_id, input_idx| {
            if (input_id == @import("ml").graph.null_node) continue;
            if (isSkipKvAttentionCacheInput(node, input_idx)) continue;
            const input = graph.node(input_id);
            switch (input.op) {
                .parameter, .fused_from_float32 => {},
                else => try stack.append(allocator, input_id),
            }
        }
    }

    var node_ids_list = std.ArrayListUnmanaged(NodeId).empty;
    defer node_ids_list.deinit(allocator);
    for (0..count) |i| {
        if (included[i]) try node_ids_list.append(allocator, @intCast(i));
    }

    var external_inputs = std.ArrayListUnmanaged(graph_mod.partition.ExternalInput).empty;
    errdefer external_inputs.deinit(allocator);
    for (node_ids_list.items) |nid| {
        const node = graph.node(nid);
        for (node.getInputs()) |input_id| {
            if (input_id == @import("ml").graph.null_node) continue;
            if (input_id < count and included[input_id]) continue;
            var found = false;
            for (external_inputs.items) |existing| {
                if (existing.node_id == input_id) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                try external_inputs.append(allocator, .{
                    .node_id = input_id,
                    .source_partition = 0,
                });
            }
        }
    }

    return .{
        .backend = backend,
        .node_ids = try node_ids_list.toOwnedSlice(allocator),
        .external_inputs = try external_inputs.toOwnedSlice(allocator),
    };
}

fn isSkipKvAttentionCacheInput(node: *const @import("ml").graph.Node, input_idx: usize) bool {
    return switch (node.op) {
        .fused_gqa_causal_attention => |attrs| attrs.skip_kv_write and (input_idx == 1 or input_idx == 2),
        else => false,
    };
}

fn buildNodeNeighborhoodPartition(
    allocator: std.mem.Allocator,
    graph: *const Graph,
    node_index: NodeId,
    node_neighborhood: usize,
    backend: ops.BackendKind,
) !graph_mod.partition.Partition {
    const start_idx = node_index -| @as(NodeId, @intCast(node_neighborhood));
    const last_graph_idx: NodeId = @intCast(graph.nodeCount() - 1);
    const end_idx = @min(last_graph_idx, node_index + @as(NodeId, @intCast(node_neighborhood)));
    const node_count: usize = @intCast(end_idx - start_idx + 1);
    const node_ids = try allocator.alloc(NodeId, node_count);
    for (0..node_count) |offset| node_ids[offset] = start_idx + @as(NodeId, @intCast(offset));

    var external_inputs = std.ArrayListUnmanaged(graph_mod.partition.ExternalInput).empty;
    errdefer external_inputs.deinit(allocator);
    for (node_ids) |nid| {
        const node = graph.node(nid);
        for (node.getInputs()) |input_id| {
            if (input_id == @import("ml").graph.null_node) continue;
            if (input_id >= start_idx and input_id <= end_idx) continue;
            var found = false;
            for (external_inputs.items) |existing| {
                if (existing.node_id == input_id) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                try external_inputs.append(allocator, .{
                    .node_id = input_id,
                    .source_partition = 0,
                });
            }
        }
    }

    return .{
        .backend = backend,
        .node_ids = node_ids,
        .external_inputs = try external_inputs.toOwnedSlice(allocator),
    };
}

fn buildNodeRangePartition(
    allocator: std.mem.Allocator,
    graph: *const Graph,
    start_idx: NodeId,
    end_idx: NodeId,
    backend: ops.BackendKind,
) !graph_mod.partition.Partition {
    const node_count: usize = @intCast(end_idx - start_idx + 1);
    const node_ids = try allocator.alloc(NodeId, node_count);
    for (0..node_count) |offset| node_ids[offset] = start_idx + @as(NodeId, @intCast(offset));

    var external_inputs = std.ArrayListUnmanaged(graph_mod.partition.ExternalInput).empty;
    errdefer external_inputs.deinit(allocator);
    for (node_ids) |nid| {
        const node = graph.node(nid);
        for (node.getInputs()) |input_id| {
            if (input_id == @import("ml").graph.null_node) continue;
            if (input_id >= start_idx and input_id <= end_idx) continue;
            var found = false;
            for (external_inputs.items) |existing| {
                if (existing.node_id == input_id) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                try external_inputs.append(allocator, .{
                    .node_id = input_id,
                    .source_partition = 0,
                });
            }
        }
    }

    return .{
        .backend = backend,
        .node_ids = node_ids,
        .external_inputs = try external_inputs.toOwnedSlice(allocator),
    };
}

fn buildSelectedOrPlannedPartition(
    allocator: std.mem.Allocator,
    graph: *const Graph,
    cb: *const ops.ComputeBackend,
    backend: BackendChoice,
    model_dir: []const u8,
    attention_mode: graph_mod.cache.AttentionMode,
    maybe_part_idx: ?u32,
    choose_best: bool,
    list_only: bool,
) !SelectedPartition {
    const backend_kind = compileBackendKind(backend);
    const backend_def = graph_mod.compiled_registry.find(backend_kind) orelse return error.UnsupportedCompileBackend;
    const attach_context: graph_mod.compiled_backend.AttachContext = .{
        .cb = cb,
        .requested_backend = backend_kind,
        .model_dir = model_dir,
        .attention_mode = attention_mode,
    };
    const capabilities = [_]graph_mod.partition.Capability{
        backend_def.capability(attach_context, .single_device),
        .{ .backend = cb.kind(), .priority = 1, .supports = &graph_mod.partition.supportsAll },
    };
    var plan = try graph_mod.partition.partition(allocator, graph, &capabilities);
    defer plan.deinit();
    if (list_only) {
        printPartitionList(graph, &plan, backend_kind);
        return .{
            .part = .{
                .backend = backend_kind,
                .node_ids = try allocator.dupe(NodeId, &.{}),
                .external_inputs = try allocator.dupe(graph_mod.partition.ExternalInput, &.{}),
            },
        };
    }
    const part_idx = if (maybe_part_idx) |idx|
        idx
    else if (choose_best)
        try chooseBestPartitionIndex(graph, &plan, attention_mode, backend_kind)
    else
        return error.InvalidPartitionIndex;
    if (part_idx >= plan.partitions.len) return error.InvalidPartitionIndex;
    const part = plan.partitions[part_idx];
    if (part.backend != backend_kind) return error.PartitionBackendMismatch;
    printSelectedPartitionSummary(graph, &plan, attention_mode, backend_kind, @intCast(part_idx));
    return .{
        .selected_index = part_idx,
        .is_partial_artifact = true,
        .part = .{
            .backend = part.backend,
            .device_id = part.device_id,
            .node_ids = try allocator.dupe(NodeId, part.node_ids),
            .external_inputs = try allocator.dupe(graph_mod.partition.ExternalInput, part.external_inputs),
            .executor = null,
        },
    };
}

fn chooseBestPartitionIndex(
    graph: *const Graph,
    plan: *const graph_mod.partition.PartitionPlan,
    attention_mode: graph_mod.cache.AttentionMode,
    backend_kind: ops.BackendKind,
) !u32 {
    _ = attention_mode;

    return switch (backend_kind) {
        .pjrt => if (comptime build_options.enable_pjrt) blk: {
            var best_idx: ?u32 = null;
            var best_nodes: usize = 0;
            for (plan.partitions, 0..) |part, idx_usize| {
                if (part.backend != .pjrt) continue;
                if (!graph_mod.compiled_pjrt.isPartitionPjrtEligible(graph, part)) continue;
                if (best_idx == null or part.node_ids.len > best_nodes) {
                    best_idx = @intCast(idx_usize);
                    best_nodes = part.node_ids.len;
                }
            }
            break :blk best_idx orelse error.NoCompilablePartition;
        } else error.BackendUnavailable,
        else => blk: {
            var best_idx: ?u32 = null;
            var best_nodes: usize = 0;
            for (plan.partitions, 0..) |part, idx_usize| {
                if (part.backend != backend_kind) continue;
                if (best_idx == null or part.node_ids.len > best_nodes) {
                    best_idx = @intCast(idx_usize);
                    best_nodes = part.node_ids.len;
                }
            }
            break :blk best_idx orelse error.NoCompilablePartition;
        },
    };
}

fn toGraphAttentionMode(mode: @import("architectures/gpt.zig").DecodeContext.AttentionMode) graph_mod.cache.AttentionMode {
    return switch (mode) {
        .full_recompute => .full_recompute,
        .paged_prefill => .paged_prefill,
        .paged_decode => .paged_decode,
    };
}

fn buildCompileDecodeContext(
    mode: CompileAttentionMode,
    decode_state: *generation.NativeDecodeState,
    seq_len: usize,
    query_seq_len: usize,
) !@import("architectures/gpt.zig").DecodeContext {
    switch (mode) {
        .auto => {
            try decode_state.notePrefill(seq_len);
            return decode_state.gptDecodeContext(seq_len, query_seq_len);
        },
        .full_recompute => return .{
            .attention_mode = .full_recompute,
            .total_sequence_len = seq_len,
            .query_sequence_len = query_seq_len,
            .kv_sequence_len = seq_len,
            .kv_position_offset = 0,
            .moe_runtime = &decode_state.moe_runtime,
        },
        .paged_prefill => {
            if (query_seq_len != seq_len) return error.InvalidQuerySequenceLength;
            try decode_state.notePrefill(seq_len);
            const ctx = decode_state.gptDecodeContext(seq_len, query_seq_len);
            if (ctx.attention_mode != .paged_prefill) return error.InvalidArguments;
            return ctx;
        },
        .paged_decode => {
            if (query_seq_len != 1 or seq_len <= query_seq_len) return error.InvalidQuerySequenceLength;
            try decode_state.notePrefill(seq_len);
            const ctx = decode_state.gptDecodeContext(seq_len, query_seq_len);
            if (ctx.attention_mode != .paged_decode) return error.InvalidArguments;
            return ctx;
        },
    }
}

fn buildWholeModelSelectionForShape(
    allocator: std.mem.Allocator,
    model: *model_manager_mod.Model,
    cb: *const ops.ComputeBackend,
    seq_len: usize,
    query_seq_len: usize,
    attention_mode: CompileAttentionMode,
) !SelectedPartition {
    const gpt_config = session_factory.getGptConfig(model.session) orelse return error.InvalidModelForGeneration;
    const backend_kind: runtime.kv.pool.BackendKind = switch (model.session.backend()) {
        .native => .native,
        .metal => .metal,
        .mlx => .mlx,
        .pjrt => return error.UnexpectedPjrtBackend,
        .onnx => return error.UnexpectedOnnxBackend,
        .wasm => return error.UnexpectedWasmBackend,
    };
    const kv_dtype = session_factory.recommendedKvDTypeForSession(model.session, backend_kind);
    const sliding_window_size: ?u32 = if (gpt_config.position_encoding == .absolute)
        null
    else if (gpt_config.sliding_window > 0)
        gpt_config.sliding_window
    else if (gpt_config.max_position_embeddings > 0)
        gpt_config.max_position_embeddings
    else
        null;

    var kv_manager = runtime.kv.manager.KvManager.init(allocator);
    defer kv_manager.deinit();
    const pool_id = try kv_manager.addPool(.{
        .backend = backend_kind,
        .dtype = kv_dtype,
        .page_size_tokens = 16,
        .num_layers_packed = @intCast(gpt_config.num_hidden_layers),
        .num_kv_heads = gpt_config.maxKvHeads(),
        .head_dim = gpt_config.maxHeadDim(),
        .sliding_window_size = sliding_window_size,
    });

    var decode_state = generation.NativeDecodeState.initPaged(allocator, &kv_manager, pool_id, model.shared_moe_cache);
    defer decode_state.deinit();
    var decode_context = try buildCompileDecodeContext(attention_mode, &decode_state, seq_len, query_seq_len);

    var graph_cache = graph_mod.cache.GraphCache.init(allocator);
    defer graph_cache.deinit();

    const graph_input_ids = try allocator.alloc(i64, query_seq_len);
    defer allocator.free(graph_input_ids);
    @memset(graph_input_ids, 0);

    const pipeline_stub = .{
        .allocator = allocator,
        .gpt_config = gpt_config,
        .cb = cb,
    };
    const entry = try graph_mod.execution.ensureGraphEntry(
        &pipeline_stub,
        &graph_cache,
        graph_input_ids,
        1,
        seq_len,
        &decode_context,
    );

    return .{
        .part = try buildWholeGraphPartition(
            allocator,
            &entry.graph,
            compileBackendKind(.xla),
            false,
        ),
    };
}

fn buildAttentionBindingMeta(
    allocator: std.mem.Allocator,
    graph: *const Graph,
    part: *const graph_mod.partition.Partition,
) ![]compiled_artifact.AttentionBindingMeta {
    var out = std.ArrayListUnmanaged(compiled_artifact.AttentionBindingMeta).empty;
    defer out.deinit(allocator);
    for (part.node_ids) |node_id| {
        const node = graph.node(node_id);
        switch (node.op) {
            .fused_gqa_causal_attention => |attrs| {
                const inputs = node.getInputs();
                if (inputs.len < 3) return error.UnsupportedShape;
                try out.append(allocator, .{
                    .node_id = @intCast(node_id),
                    .k_node_id = @intCast(inputs[1]),
                    .v_node_id = @intCast(inputs[2]),
                    .layer_index = attrs.layer_index,
                    .skip_kv_write = attrs.skip_kv_write,
                });
            },
            else => {},
        }
    }
    return out.toOwnedSlice(allocator);
}

fn printPartitionList(
    graph: *const Graph,
    plan: *const graph_mod.partition.PartitionPlan,
    backend_kind: ops.BackendKind,
) void {
    for (plan.partitions, 0..) |part, idx| {
        if (part.backend != backend_kind) continue;
        const first_op = if (part.node_ids.len > 0) @tagName(graph.node(part.node_ids[0]).op) else "none";
        const last_op = if (part.node_ids.len > 0) @tagName(graph.node(part.node_ids[part.node_ids.len - 1]).op) else "none";
        print(
            "partition index={d} backend={s} nodes={d} external_inputs={d} first_op={s} last_op={s}\n",
            .{ idx, @tagName(part.backend), part.node_ids.len, part.external_inputs.len, first_op, last_op },
        );
    }
}

fn printSelectedPartitionSummary(
    graph: *const Graph,
    plan: *const graph_mod.partition.PartitionPlan,
    attention_mode: graph_mod.cache.AttentionMode,
    backend_kind: ops.BackendKind,
    part_idx: u32,
) void {
    _ = attention_mode;
    const idx_usize: usize = @intCast(part_idx);
    if (idx_usize >= plan.partitions.len) return;
    const part = plan.partitions[idx_usize];
    if (part.backend != backend_kind) return;

    const first_op = if (part.node_ids.len > 0) @tagName(graph.node(part.node_ids[0]).op) else "none";
    const last_op = if (part.node_ids.len > 0) @tagName(graph.node(part.node_ids[part.node_ids.len - 1]).op) else "none";

    if (backend_kind == .pjrt) {
        if (comptime build_options.enable_pjrt) {
            print(
                "selected partition index={d} backend=pjrt eligible={} nodes={d} external_inputs={d} first_op={s} last_op={s}\n",
                .{
                    part_idx,
                    graph_mod.compiled_pjrt.isPartitionPjrtEligible(graph, part),
                    part.node_ids.len,
                    part.external_inputs.len,
                    first_op,
                    last_op,
                },
            );
            return;
        }
    }

    print(
        "selected partition index={d} backend={s} nodes={d} external_inputs={d} first_op={s} last_op={s}\n",
        .{ part_idx, @tagName(part.backend), part.node_ids.len, part.external_inputs.len, first_op, last_op },
    );
}

fn printMatchingOpNodes(graph: *const Graph, op_name: []const u8) void {
    for (0..graph.nodeCount()) |idx| {
        const nid: NodeId = @intCast(idx);
        const node = graph.node(nid);
        const tag = @tagName(node.op);
        if (!std.mem.eql(u8, tag, op_name)) continue;
        print("node index={d} op={s} shape=", .{ idx, tag });
        printShape(node.output_shape);
        print(" inputs={d}\n", .{node.num_inputs});
    }
}

fn printNodeWindow(graph: *const Graph, center: u32, radius: usize) void {
    const center_id: NodeId = @intCast(center);
    const start_idx = center_id -| @as(NodeId, @intCast(radius));
    const last_graph_idx: NodeId = @intCast(graph.nodeCount() - 1);
    const end_idx = @min(last_graph_idx, center_id + @as(NodeId, @intCast(radius)));
    for (@as(usize, @intCast(start_idx))..@as(usize, @intCast(end_idx)) + 1) |idx| {
        const nid: NodeId = @intCast(idx);
        const node = graph.node(nid);
        print("node index={d} op={s} shape=", .{ idx, @tagName(node.op) });
        printShape(node.output_shape);
        print(" inputs=[", .{});
        var first = true;
        for (0..node.num_inputs) |input_idx| {
            const input_id = node.inputs[input_idx];
            if (!first) print(",", .{});
            first = false;
            print("{d}", .{input_id});
        }
        print("]\n", .{});
    }
}

fn printSelectedNodeSummary(graph: *const Graph, node_index: NodeId, node_neighborhood: usize, node_closure: bool) void {
    const node = graph.node(node_index);
    if (node_closure) {
        print("selected node index={d} mode=closure op={s} shape=", .{ node_index, @tagName(node.op) });
    } else if (node_neighborhood == 0) {
        print("selected node index={d} op={s} shape=", .{ node_index, @tagName(node.op) });
    } else {
        print("selected node neighborhood center={d} radius={d} op={s} shape=", .{
            node_index,
            node_neighborhood,
            @tagName(node.op),
        });
    }
    printShape(node.output_shape);
    print(" external_inputs={d}\n", .{node.num_inputs});
}

fn printSelectedNodeRangeSummary(graph: *const Graph, start: NodeId, end: NodeId) void {
    const first = graph.node(start);
    const last = graph.node(end);
    print(
        "selected node range start={d} end={d} nodes={d} first_op={s} last_op={s}\n",
        .{
            start,
            end,
            end - start + 1,
            @tagName(first.op),
            @tagName(last.op),
        },
    );
}

fn printShape(shape: @import("ml").graph.Shape) void {
    print("[", .{});
    for (0..shape.rank()) |axis| {
        if (axis != 0) print("x", .{});
        print("{d}", .{shape.dim(@intCast(axis))});
    }
    print("]", .{});
}

fn defaultOutputPath(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    backend: BackendChoice,
    xla_artifact_kind: PjrtArtifactExportKind,
    partition_index: ?u32,
    output_label: ?[]const u8,
    seq_len: usize,
    query_seq_len: usize,
    attention_mode: []const u8,
) ![]const u8 {
    const stem = std.fs.path.basename(model_dir);
    const ext = switch (backend) {
        .onnx => "onnx",
        .xla => switch (xla_artifact_kind) {
            .hlo => "hlo",
            .executable => "pjrt_exec",
        },
        else => return error.UnsupportedCompileBackend,
    };
    const artifact_dir = try compiled_artifact.defaultArtifactDirForModel(allocator, model_dir, @tagName(backend));
    defer allocator.free(artifact_dir);
    const prefix = if (output_label) |label|
        try std.fmt.allocPrint(allocator, "{s}.{s}", .{ stem, label })
    else if (partition_index) |idx|
        try std.fmt.allocPrint(allocator, "{s}.part{d}", .{ stem, idx })
    else
        try allocator.dupe(u8, stem);
    defer allocator.free(prefix);
    const filename = try std.fmt.allocPrint(allocator, "{s}.{s}.s{d}.q{d}.{s}", .{
        prefix,
        attention_mode,
        seq_len,
        query_seq_len,
        ext,
    });
    defer allocator.free(filename);
    return std.fs.path.join(allocator, &.{ artifact_dir, filename });
}

fn updateDefaultXlaOutputPathForResolvedKind(
    allocator: std.mem.Allocator,
    output_path: *[]const u8,
    output_path_is_default: bool,
    model_dir: []const u8,
    xla_artifact_kind: PjrtArtifactExportKind,
    partition_index: ?u32,
    output_label: ?[]const u8,
    seq_len: usize,
    query_seq_len: usize,
    attention_mode: []const u8,
) !void {
    if (!output_path_is_default) return;
    const next_path = try defaultOutputPath(
        allocator,
        model_dir,
        .xla,
        xla_artifact_kind,
        partition_index,
        output_label,
        seq_len,
        query_seq_len,
        attention_mode,
    );
    allocator.free(output_path.*);
    output_path.* = next_path;
}

fn buildOnnxDebugOutputLabel(allocator: std.mem.Allocator, node_ids: []const u32) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(allocator);
    try out.appendSlice(allocator, "debug");
    for (node_ids) |node_id| {
        const suffix = try std.fmt.allocPrint(allocator, "_{d}", .{node_id});
        defer allocator.free(suffix);
        try out.appendSlice(allocator, suffix);
    }
    return out.toOwnedSlice(allocator);
}

fn guardOnnxInitializerReuseOutput(output_path: []const u8, reuse_path: []const u8) !void {
    const output_dir = std.fs.path.dirname(output_path) orelse ".";
    const reuse_dir = std.fs.path.dirname(reuse_path) orelse ".";
    if (std.mem.eql(u8, output_dir, reuse_dir)) return;
    std.log.err(
        "ONNX initializer reuse requires output beside the source artifact so relative external-data paths still resolve: output_dir={s} reuse_dir={s}",
        .{ output_dir, reuse_dir },
    );
    return error.InvalidArguments;
}

fn xlaArtifactKind(artifact_kind: PjrtArtifactExportKind, is_partition: bool) []const u8 {
    return switch (artifact_kind) {
        .hlo => if (is_partition) "pjrt_partition_hlo" else "pjrt_hlo",
        .executable => if (is_partition) "pjrt_partition_executable" else "pjrt_executable",
    };
}

fn defaultWholeModelXlaArtifactKind(
    requested_kind: PjrtArtifactExportKind,
    explicit_kind: bool,
    is_partial_artifact: bool,
    load_only_executable_supported: bool,
) PjrtArtifactExportKind {
    if (explicit_kind or is_partial_artifact) return requested_kind;
    if (requested_kind != .hlo) return requested_kind;
    return if (load_only_executable_supported) .executable else .hlo;
}

fn pjrtLoadOnlyExecutableArtifactsSupported(allocator: std.mem.Allocator) !bool {
    if (!build_options.enable_pjrt) return false;
    const plugin_path = try native_backend_choice.pjrtPluginPathFromEnv(allocator) orelse return false;
    defer allocator.free(plugin_path);

    const pjrt_lib = @import("pjrt");
    var client = pjrt_lib.pjrt.Client.init(plugin_path) catch return false;
    defer client.deinit();
    return client.executableArtifactSupport().loadOnlyExecutableArtifacts();
}

fn writePjrtExecutableArtifact(
    allocator: std.mem.Allocator,
    io: std.Io,
    output_path: []const u8,
    hlo_bytes: []const u8,
    output_count: usize,
    is_partial_artifact: bool,
) !void {
    try guardPjrtExecutableExportBudget(is_partial_artifact, hlo_bytes.len);
    const pjrt_lib = @import("pjrt");
    const plugin_path = try native_backend_choice.pjrtPluginPathFromEnv(allocator) orelse return error.MissingPjrtPluginPath;
    defer allocator.free(plugin_path);
    var client = try pjrt_lib.pjrt.Client.init(plugin_path);
    defer client.deinit();
    var executable = try client.compile(hlo_bytes, output_count);
    defer executable.deinit();
    const serialized = try executable.serialize(allocator);
    defer allocator.free(serialized);
    try std.Io.Dir.cwd().writeFile(io, .{
        .sub_path = output_path,
        .data = serialized,
    });
}

fn resolveXlaArtifactKind(
    allocator: std.mem.Allocator,
    backend: BackendChoice,
    requested_kind: PjrtArtifactExportKind,
    explicit_kind: bool,
    is_partial_artifact: bool,
) !PjrtArtifactExportKind {
    if (backend != .xla) return requested_kind;
    if (explicit_kind or is_partial_artifact or requested_kind != .hlo) return requested_kind;
    const executable_supported = try pjrtLoadOnlyExecutableArtifactsSupported(allocator);
    return defaultWholeModelXlaArtifactKind(
        requested_kind,
        explicit_kind,
        is_partial_artifact,
        executable_supported,
    );
}

fn artifactKind(backend: BackendChoice, is_partition: bool, xla_artifact_kind: PjrtArtifactExportKind) []const u8 {
    return switch (backend) {
        .onnx => if (is_partition) "onnx_partition_graph" else "onnx_graph",
        .xla => xlaArtifactKind(xla_artifact_kind, is_partition),
        else => unreachable,
    };
}

fn cloneStringSlices(allocator: std.mem.Allocator, src: []const []u8) ![][]u8 {
    const out = try allocator.alloc([]u8, src.len);
    errdefer allocator.free(out);
    for (src, 0..) |value, i| {
        out[i] = try allocator.dupe(u8, value);
    }
    return out;
}

fn cloneShapeSlices(allocator: std.mem.Allocator, src: []const []i64) ![][]i64 {
    const out = try allocator.alloc([]i64, src.len);
    errdefer allocator.free(out);
    for (src, 0..) |value, i| {
        out[i] = try allocator.dupe(i64, value);
    }
    return out;
}

fn freeShapeSlices(allocator: std.mem.Allocator, shapes: [][]i64) void {
    for (shapes) |shape| allocator.free(shape);
    if (shapes.len > 0) allocator.free(shapes);
}

fn cloneNodeIdsAsU32(allocator: std.mem.Allocator, src: []const NodeId) ![]u32 {
    const out = try allocator.alloc(u32, src.len);
    errdefer allocator.free(out);
    for (src, 0..) |value, i| out[i] = @intCast(value);
    return out;
}

fn buildPjrtInputBindingMeta(
    allocator: std.mem.Allocator,
    bindings: []const PjrtInputBinding,
    attention_bindings: []const compiled_artifact.AttentionBindingMeta,
) ![]compiled_artifact.PjrtInputBindingMeta {
    const out = try allocator.alloc(compiled_artifact.PjrtInputBindingMeta, bindings.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |binding| {
            if (binding.name.len > 0) allocator.free(binding.name);
        }
        allocator.free(out);
    }
    for (bindings, 0..) |binding, idx| {
        out[idx] = switch (binding) {
            .graph_node => |nid| if (findPjrtAttentionCacheBinding(attention_bindings, nid)) |cache| .{
                .kind = if (cache.kind == .key) compiled_artifact.pjrt_binding_past_key else compiled_artifact.pjrt_binding_past_value,
                .node_id = @intCast(nid),
                .name = try pjrtKvBindingName(allocator, "past_key_values", cache.layer_index, cache.kind),
                .layer_index = cache.layer_index,
            } else .{
                .kind = compiled_artifact.pjrt_binding_graph_node,
                .node_id = @intCast(nid),
            },
            .embedding_ids => |nid| .{
                .kind = compiled_artifact.pjrt_binding_input_ids,
                .node_id = @intCast(nid),
                .name = try allocator.dupe(u8, "input_ids"),
            },
            .semantic_past_graph_node => |nid| if (findPjrtAttentionCacheBinding(attention_bindings, nid)) |cache| .{
                .kind = if (cache.kind == .key) compiled_artifact.pjrt_binding_past_key else compiled_artifact.pjrt_binding_past_value,
                .node_id = @intCast(nid),
                .name = try pjrtKvBindingName(allocator, "past_key_values", cache.layer_index, cache.kind),
                .layer_index = cache.layer_index,
            } else .{
                .kind = compiled_artifact.pjrt_binding_graph_node,
                .node_id = @intCast(nid),
            },
        };
        initialized += 1;
    }
    return out;
}

fn buildPjrtOutputBindingMeta(
    allocator: std.mem.Allocator,
    node_ids: []const NodeId,
    attention_bindings: []const compiled_artifact.AttentionBindingMeta,
) ![]compiled_artifact.PjrtOutputBindingMeta {
    const out = try allocator.alloc(compiled_artifact.PjrtOutputBindingMeta, node_ids.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |binding| {
            if (binding.name.len > 0) allocator.free(binding.name);
        }
        allocator.free(out);
    }
    for (node_ids, 0..) |node_id, i| {
        out[i] = if (findPjrtAttentionCacheBinding(attention_bindings, node_id)) |cache| .{
            .kind = if (cache.kind == .key) compiled_artifact.pjrt_binding_present_key else compiled_artifact.pjrt_binding_present_value,
            .node_id = @intCast(node_id),
            .name = try pjrtKvBindingName(allocator, "present", cache.layer_index, cache.kind),
            .layer_index = cache.layer_index,
        } else .{
            .kind = compiled_artifact.pjrt_binding_graph_node,
            .node_id = @intCast(node_id),
        };
        initialized += 1;
    }
    return out;
}

const PjrtAttentionCacheBinding = struct {
    layer_index: u32,
    kind: PjrtKvKind,
};

const PjrtKvKind = enum { key, value };

fn findPjrtAttentionCacheBinding(
    attention_bindings: []const compiled_artifact.AttentionBindingMeta,
    node_id: NodeId,
) ?PjrtAttentionCacheBinding {
    for (attention_bindings) |binding| {
        if (binding.k_node_id == node_id) return .{ .layer_index = binding.layer_index, .kind = .key };
        if (binding.v_node_id == node_id) return .{ .layer_index = binding.layer_index, .kind = .value };
    }
    return null;
}

fn pjrtKvBindingName(
    allocator: std.mem.Allocator,
    prefix: []const u8,
    layer_index: u32,
    kind: PjrtKvKind,
) ![]u8 {
    return std.fmt.allocPrint(allocator, "{s}.{d}.{s}", .{ prefix, layer_index, @tagName(kind) });
}

fn freePjrtInputBindingMeta(
    allocator: std.mem.Allocator,
    bindings: []compiled_artifact.PjrtInputBindingMeta,
) void {
    for (bindings) |binding| {
        if (binding.name.len > 0) allocator.free(binding.name);
    }
    if (bindings.len > 0) allocator.free(bindings);
}

fn freePjrtOutputBindingMeta(
    allocator: std.mem.Allocator,
    bindings: []compiled_artifact.PjrtOutputBindingMeta,
) void {
    for (bindings) |binding| {
        if (binding.name.len > 0) allocator.free(binding.name);
    }
    if (bindings.len > 0) allocator.free(bindings);
}

fn ensureParentDir(io: std.Io, path: []const u8) !void {
    const parent = std.fs.path.dirname(path) orelse return;
    if (std.Io.Dir.cwd().access(io, parent, .{})) |_| return else |_| {}
    try std.Io.Dir.cwd().createDirPath(io, parent);
}

fn validateCompileBackend(choice: BackendChoice) !void {
    try native_backend_choice.validate(choice);
    switch (choice) {
        .onnx => {},
        .xla => if (!build_options.enable_pjrt) return error.BackendUnavailable,
        else => return error.UnsupportedCompileBackend,
    }
}

fn parseArgs(args: []const []const u8) !Options {
    if (args.len < 2) return error.InvalidArgs;

    var opts = Options{
        .model_dir = args[0],
        .prompt = args[1],
        .backend = .onnx,
    };

    var i: usize = 2;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], "--backend") and i + 1 < args.len) {
            opts.backend = native_backend_choice.parse(args[i + 1]) orelse return error.InvalidBackend;
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--attention-mode") and i + 1 < args.len) {
            opts.attention_mode = parseCompileAttentionMode(args[i + 1]) orelse return error.InvalidArgs;
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--onnx-weight-mode") and i + 1 < args.len) {
            opts.onnx_weight_mode = parseOnnxWeightMode(args[i + 1]) orelse return error.InvalidArgs;
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--onnx-weight-policy") and i + 1 < args.len) {
            const raw_rule = args[i + 1];
            const eq = std.mem.indexOfScalar(u8, raw_rule, '=') orelse return error.InvalidArgs;
            const lhs = std.mem.trim(u8, raw_rule[0..eq], " \t\r\n");
            const rhs = std.mem.trim(u8, raw_rule[eq + 1 ..], " \t\r\n");
            const mode = parseOnnxWeightMode(rhs) orelse return error.InvalidArgs;
            if (std.mem.eql(u8, lhs, "default")) {
                opts.onnx_weight_mode = mode;
            } else {
                if (lhs.len == 0) return error.InvalidArgs;
                if (opts.onnx_weight_export_rule_count >= max_weight_export_rules) return error.InvalidArgs;
                opts.onnx_weight_export_rules[opts.onnx_weight_export_rule_count] = .{
                    .substring = lhs,
                    .mode = mode,
                };
                opts.onnx_weight_export_rule_count += 1;
            }
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--onnx-reuse-initializers-from") and i + 1 < args.len) {
            opts.onnx_reuse_initializers_from = args[i + 1];
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--onnx-import-from") and i + 1 < args.len) {
            opts.onnx_import_from = args[i + 1];
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--onnx-semantic-entrypoint")) {
            opts.onnx_semantic_entrypoint = true;
        } else if (std.mem.eql(u8, args[i], "--artifact-role") and i + 1 < args.len) {
            if (!std.mem.eql(u8, args[i + 1], compiled_artifact.artifact_role_prefill) and
                !std.mem.eql(u8, args[i + 1], compiled_artifact.artifact_role_decode))
            {
                return error.InvalidArgs;
            }
            opts.artifact_role = args[i + 1];
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--xla-artifact-kind") and i + 1 < args.len) {
            opts.xla_artifact_kind = parsePjrtArtifactExportKind(args[i + 1]) orelse return error.InvalidArgs;
            opts.xla_artifact_kind_explicit = true;
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--xla-parameter-mode") and i + 1 < args.len) {
            opts.xla_parameter_mode = parsePjrtParameterExportMode(args[i + 1]) orelse return error.InvalidArgs;
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--xla-package-decode-max-seq-len") and i + 1 < args.len) {
            opts.xla_package_decode_max_seq_len = try std.fmt.parseInt(usize, args[i + 1], 10);
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--list-op-nodes") and i + 1 < args.len) {
            opts.list_op_nodes = args[i + 1];
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--list-node-window") and i + 1 < args.len) {
            opts.list_node_window = try std.fmt.parseInt(u32, args[i + 1], 10);
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--list-node-window-radius") and i + 1 < args.len) {
            opts.list_node_window_radius = try std.fmt.parseInt(usize, args[i + 1], 10);
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--node-index") and i + 1 < args.len) {
            opts.node_index = try std.fmt.parseInt(u32, args[i + 1], 10);
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--node-range") and i + 2 < args.len) {
            opts.node_range_start = try std.fmt.parseInt(u32, args[i + 1], 10);
            opts.node_range_end = try std.fmt.parseInt(u32, args[i + 2], 10);
            i += 2;
        } else if (std.mem.eql(u8, args[i], "--debug-output-node") and i + 1 < args.len) {
            if (opts.debug_output_node_count >= max_debug_output_nodes) return error.InvalidArgs;
            opts.debug_output_nodes[opts.debug_output_node_count] = try std.fmt.parseInt(u32, args[i + 1], 10);
            opts.debug_output_node_count += 1;
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--node-closure")) {
            opts.node_closure = true;
        } else if (std.mem.eql(u8, args[i], "--node-neighborhood") and i + 1 < args.len) {
            opts.node_neighborhood = try std.fmt.parseInt(usize, args[i + 1], 10);
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--list-partitions")) {
            opts.list_partitions = true;
        } else if (std.mem.eql(u8, args[i], "--best-partition")) {
            opts.best_partition = true;
        } else if (std.mem.eql(u8, args[i], "--partition-index") and i + 1 < args.len) {
            opts.partition_index = try std.fmt.parseInt(u32, args[i + 1], 10);
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--output") and i + 1 < args.len) {
            opts.output_path = args[i + 1];
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--seq-len") and i + 1 < args.len) {
            opts.seq_len = try std.fmt.parseInt(usize, args[i + 1], 10);
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--query-seq-len") and i + 1 < args.len) {
            opts.query_seq_len = try std.fmt.parseInt(usize, args[i + 1], 10);
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--no-chat-template")) {
            opts.no_chat_template = true;
        } else if (std.mem.eql(u8, args[i], "--raw-prompt")) {
            opts.raw_prompt = true;
        } else {
            return error.InvalidArgs;
        }
    }

    return opts;
}

fn countPromptTokens(attention_mask: anytype) usize {
    var count: usize = 0;
    while (count < attention_mask.len and attention_mask[count] != 0) : (count += 1) {}
    return count;
}

fn parseOnnxWeightMode(raw: []const u8) ?OnnxWeightExportMode {
    if (std.mem.eql(u8, raw, "dense")) return .dense;
    if (std.mem.eql(u8, raw, "q8_0_weight_only")) return .q8_0_weight_only;
    return null;
}

fn parseCompileAttentionMode(raw: []const u8) ?CompileAttentionMode {
    if (std.mem.eql(u8, raw, "auto")) return .auto;
    if (std.mem.eql(u8, raw, "full_recompute")) return .full_recompute;
    if (std.mem.eql(u8, raw, "paged_prefill")) return .paged_prefill;
    if (std.mem.eql(u8, raw, "paged_decode")) return .paged_decode;
    return null;
}

fn parsePjrtArtifactExportKind(raw: []const u8) ?PjrtArtifactExportKind {
    if (std.mem.eql(u8, raw, "hlo")) return .hlo;
    if (std.mem.eql(u8, raw, "executable")) return .executable;
    return null;
}

fn parsePjrtParameterExportMode(raw: []const u8) ?PjrtParameterExportMode {
    if (std.mem.eql(u8, raw, "embedded")) return .embedded;
    if (std.mem.eql(u8, raw, "inputs")) return .inputs;
    return null;
}

pub fn printUsage() void {
    print(
        \\usage: antfly inference compile-artifact <model-dir> <prompt> [--backend onnx|xla] [--attention-mode auto|full_recompute|paged_prefill|paged_decode] [--onnx-weight-mode dense|q8_0_weight_only] [--onnx-weight-policy SUBSTRING=MODE] [--onnx-reuse-initializers-from <artifact.onnx>] [--onnx-import-from <semantic-decoder.onnx>] [--onnx-semantic-entrypoint] [--artifact-role prefill|decode] [--xla-artifact-kind hlo|executable] [--xla-parameter-mode embedded|inputs] [--xla-package-decode-max-seq-len N] [--debug-output-node N] [--output <path>] [--list-partitions] [--list-op-nodes <op>] [--list-node-window N] [--list-node-window-radius N] [--best-partition] [--partition-index N] [--node-index N] [--node-range START END] [--node-closure] [--node-neighborhood N] [--seq-len N] [--query-seq-len N] [--no-chat-template] [--raw-prompt]
        \\
        \\Compiles a traced generation graph into an offline artifact for a concrete shape.
        \\  default artifact dir mirrors model layout: ~/.antfly/inference/artifacts/<owner>/<model>/<backend>/...
        \\  backend=onnx   emits a self-contained ONNX file
        \\  attention-mode selects the traced attention ABI; auto preserves the generation path
        \\  onnx-weight-mode controls the default ONNX initializer export mode
        \\  onnx-weight-policy overrides export mode for matching parameter-name substrings; repeat as needed, or use default=MODE
        \\  onnx-reuse-initializers-from regenerates only the ONNX protobuf and points at an existing external-data blob
        \\  onnx-import-from writes an Antfly inference manifest for an existing semantic ONNX artifact, refreshes the package manifest, and requires explicit attention-mode, seq-len, and query-seq-len
        \\  artifact-role tags imported or compiled whole-model artifacts as prefill or decode
        \\  xla-artifact-kind selects serialized HLO or a plugin-native executable artifact
        \\  xla-parameter-mode=inputs emits model weights as PJRT inputs instead of embedding dense constants in HLO
        \\  xla-package-decode-max-seq-len compiles one whole-model PJRT prefill artifact plus contiguous decode buckets through seq_len=N
        \\  whole-model ONNX/XLA compiles refresh and print a package manifest beside the emitted artifacts
        \\  debug-output-node marks one traced node as an additional ONNX graph output; repeat for multiple nodes
        \\  backend=xla    emits serialized HLO or a plugin-native executable plus PJRT binding metadata
        \\  list-partitions prints traced backend partitions for the selected shape and exits
        \\  list-op-nodes prints traced node IDs matching an op tag and exits
        \\  list-node-window prints a traced node window around one node ID and exits
        \\  best-partition chooses the strongest traced partition automatically
        \\  partition-index compiles one traced partition instead of the whole graph
        \\  node-index compiles one traced node as a debug partition artifact
        \\  node-range compiles an exact contiguous traced node range as a debug partition artifact
        \\  node-closure compiles the full dependency closure for node-index
        \\  node-neighborhood widens node-index into a bounded topological window
        \\  seq_len/query_seq_len control the traced graph shape
        \\    default: seq_len=prompt_tokens, query_seq_len=seq_len
        \\
    , .{});
}

test "parseArgs accepts explicit output and backend" {
    const opts = try parseArgs(&.{
        "/tmp/model",
        "hello",
        "--backend",
        "onnx",
        "--attention-mode",
        "full_recompute",
        "--onnx-weight-mode",
        "q8_0_weight_only",
        "--onnx-weight-policy",
        "model.embed_tokens=dense",
        "--onnx-weight-policy",
        "lm_head=q8_0_weight_only",
        "--onnx-reuse-initializers-from",
        "/tmp/base.onnx",
        "--onnx-import-from",
        "/tmp/decoder.onnx",
        "--onnx-semantic-entrypoint",
        "--artifact-role",
        "decode",
        "--xla-artifact-kind",
        "executable",
        "--xla-parameter-mode",
        "inputs",
        "--xla-package-decode-max-seq-len",
        "16",
        "--list-op-nodes",
        "fused_gqa_causal_attention",
        "--list-node-window",
        "11",
        "--list-node-window-radius",
        "4",
        "--node-index",
        "9",
        "--node-range",
        "11",
        "19",
        "--debug-output-node",
        "13",
        "--debug-output-node",
        "17",
        "--node-closure",
        "--node-neighborhood",
        "2",
        "--best-partition",
        "--partition-index",
        "7",
        "--output",
        "/tmp/out.onnx",
        "--seq-len",
        "128",
        "--query-seq-len",
        "1",
        "--raw-prompt",
    });
    try std.testing.expectEqualStrings("/tmp/model", opts.model_dir);
    try std.testing.expectEqual(BackendChoice.onnx, opts.backend);
    try std.testing.expectEqual(CompileAttentionMode.full_recompute, opts.attention_mode);
    try std.testing.expectEqual(OnnxWeightExportMode.q8_0_weight_only, opts.onnx_weight_mode);
    try std.testing.expectEqual(@as(usize, 2), opts.onnx_weight_export_rule_count);
    try std.testing.expectEqualStrings("model.embed_tokens", opts.onnx_weight_export_rules[0].substring);
    try std.testing.expectEqual(OnnxWeightExportMode.dense, opts.onnx_weight_export_rules[0].mode);
    try std.testing.expectEqualStrings("lm_head", opts.onnx_weight_export_rules[1].substring);
    try std.testing.expectEqual(OnnxWeightExportMode.q8_0_weight_only, opts.onnx_weight_export_rules[1].mode);
    try std.testing.expectEqualStrings("/tmp/base.onnx", opts.onnx_reuse_initializers_from.?);
    try std.testing.expectEqualStrings("/tmp/decoder.onnx", opts.onnx_import_from.?);
    try std.testing.expect(opts.onnx_semantic_entrypoint);
    try std.testing.expectEqualStrings("decode", opts.artifact_role.?);
    try std.testing.expectEqual(PjrtArtifactExportKind.executable, opts.xla_artifact_kind);
    try std.testing.expectEqual(PjrtParameterExportMode.inputs, opts.xla_parameter_mode);
    try std.testing.expectEqual(@as(?usize, 16), opts.xla_package_decode_max_seq_len);
    try std.testing.expectEqualStrings("fused_gqa_causal_attention", opts.list_op_nodes.?);
    try std.testing.expectEqual(@as(?u32, 11), opts.list_node_window);
    try std.testing.expectEqual(@as(usize, 4), opts.list_node_window_radius);
    try std.testing.expectEqual(@as(?u32, 9), opts.node_index);
    try std.testing.expectEqual(@as(?u32, 11), opts.node_range_start);
    try std.testing.expectEqual(@as(?u32, 19), opts.node_range_end);
    try std.testing.expectEqual(@as(usize, 2), opts.debug_output_node_count);
    try std.testing.expectEqual(@as(u32, 13), opts.debug_output_nodes[0]);
    try std.testing.expectEqual(@as(u32, 17), opts.debug_output_nodes[1]);
    try std.testing.expect(opts.node_closure);
    try std.testing.expectEqual(@as(usize, 2), opts.node_neighborhood);
    try std.testing.expect(opts.best_partition);
    try std.testing.expectEqual(@as(?u32, 7), opts.partition_index);
    try std.testing.expectEqualStrings("/tmp/out.onnx", opts.output_path.?);
    try std.testing.expectEqual(@as(?usize, 128), opts.seq_len);
    try std.testing.expectEqual(@as(?usize, 1), opts.query_seq_len);
    try std.testing.expect(opts.raw_prompt);
}

test "semantic ONNX entrypoint selection keeps partial artifacts node-oriented" {
    try std.testing.expect(shouldUseSemanticOnnxEntrypoint(
        false,
        false,
        "paged_prefill",
        3,
        3,
        0,
    ));
    try std.testing.expect(!shouldUseSemanticOnnxEntrypoint(
        false,
        true,
        "paged_prefill",
        3,
        3,
        0,
    ));
    try std.testing.expect(!shouldUseSemanticOnnxEntrypoint(
        false,
        false,
        "paged_prefill",
        3,
        3,
        1,
    ));
    try std.testing.expect(!shouldUseSemanticOnnxEntrypoint(
        false,
        false,
        "paged_prefill",
        4,
        1,
        0,
    ));
    try std.testing.expect(!shouldUseSemanticOnnxEntrypoint(
        false,
        false,
        "full_recompute",
        3,
        3,
        0,
    ));
    try std.testing.expect(shouldUseSemanticOnnxEntrypoint(
        false,
        true,
        "paged_decode",
        4,
        1,
        1,
    ));
    try std.testing.expect(shouldUseSemanticOnnxEntrypoint(
        true,
        true,
        "paged_prefill",
        4,
        1,
        1,
    ));
}

test "defaultWholeModelXlaArtifactKind prefers executable for implicit whole-model exports" {
    try std.testing.expectEqual(
        PjrtArtifactExportKind.executable,
        defaultWholeModelXlaArtifactKind(.hlo, false, false, true),
    );
    try std.testing.expectEqual(
        PjrtArtifactExportKind.hlo,
        defaultWholeModelXlaArtifactKind(.hlo, false, false, false),
    );
    try std.testing.expectEqual(
        PjrtArtifactExportKind.hlo,
        defaultWholeModelXlaArtifactKind(.hlo, false, true, true),
    );
    try std.testing.expectEqual(
        PjrtArtifactExportKind.hlo,
        defaultWholeModelXlaArtifactKind(.hlo, true, false, true),
    );
    try std.testing.expectEqual(
        PjrtArtifactExportKind.executable,
        defaultWholeModelXlaArtifactKind(.executable, true, false, false),
    );
}

test "validateWholeModelPjrtPackageRequest accepts whole-model prefill package request" {
    var opts = Options{
        .model_dir = "/tmp/model",
        .prompt = "hello",
        .backend = .xla,
        .attention_mode = .paged_prefill,
        .query_seq_len = 8,
        .seq_len = 8,
    };
    try validateWholeModelPjrtPackageRequest(opts, false, 8, 12);

    opts.attention_mode = .auto;
    try validateWholeModelPjrtPackageRequest(opts, false, 8, 12);
}

test "validateWholeModelPjrtPackageRequest rejects invalid package requests" {
    const base = Options{
        .model_dir = "/tmp/model",
        .prompt = "hello",
        .backend = .xla,
        .attention_mode = .paged_prefill,
        .query_seq_len = 8,
        .seq_len = 8,
    };

    try std.testing.expectError(error.InvalidArguments, validateWholeModelPjrtPackageRequest(.{
        .model_dir = base.model_dir,
        .prompt = base.prompt,
        .backend = base.backend,
        .attention_mode = base.attention_mode,
        .query_seq_len = base.query_seq_len,
        .seq_len = base.seq_len,
        .output_path = "/tmp/out",
    }, false, 8, 12));
    try std.testing.expectError(error.InvalidArguments, validateWholeModelPjrtPackageRequest(.{
        .model_dir = base.model_dir,
        .prompt = base.prompt,
        .backend = base.backend,
        .attention_mode = base.attention_mode,
        .query_seq_len = base.query_seq_len,
        .seq_len = base.seq_len,
        .debug_output_node_count = 1,
    }, false, 8, 12));
    try std.testing.expectError(error.InvalidArguments, validateWholeModelPjrtPackageRequest(.{
        .model_dir = base.model_dir,
        .prompt = base.prompt,
        .backend = base.backend,
        .attention_mode = .paged_decode,
        .query_seq_len = base.query_seq_len,
        .seq_len = base.seq_len,
    }, false, 8, 12));
    try std.testing.expectError(error.InvalidArguments, validateWholeModelPjrtPackageRequest(base, true, 8, 12));
    try std.testing.expectError(error.InvalidSequenceLength, validateWholeModelPjrtPackageRequest(base, false, 8, 7));
    try std.testing.expectError(error.InvalidQuerySequenceLength, validateWholeModelPjrtPackageRequest(.{
        .model_dir = base.model_dir,
        .prompt = base.prompt,
        .backend = base.backend,
        .attention_mode = base.attention_mode,
        .query_seq_len = 1,
        .seq_len = base.seq_len,
    }, false, 8, 12));
}

test "updateDefaultXlaOutputPathForResolvedKind swaps XLA default extension" {
    var path = try defaultOutputPath(std.testing.allocator, "/tmp/model-dir", .xla, .executable, null, null, 4, 1, "paged_decode");
    defer std.testing.allocator.free(path);
    try updateDefaultXlaOutputPathForResolvedKind(
        std.testing.allocator,
        &path,
        true,
        "/tmp/model-dir",
        .hlo,
        null,
        null,
        4,
        1,
        "paged_decode",
    );
    try std.testing.expect(std.mem.endsWith(u8, path, "tmp/model-dir/xla/model-dir.paged_decode.s4.q1.hlo"));
}

test "wholeModelPjrtPackageManifestPath tracks resolved artifact kind" {
    const executable_path = try wholeModelPjrtPackageManifestPath(
        std.testing.allocator,
        "/tmp/model-dir",
        .executable,
        .embedded,
    );
    defer std.testing.allocator.free(executable_path);
    try std.testing.expect(std.mem.endsWith(u8, executable_path, "tmp/model-dir/xla/model-dir.xla.pjrt_executable.embedded.antfly-inference-package.json"));

    const hlo_path = try wholeModelPjrtPackageManifestPath(
        std.testing.allocator,
        "/tmp/model-dir",
        .hlo,
        .inputs,
    );
    defer std.testing.allocator.free(hlo_path);
    try std.testing.expect(std.mem.endsWith(u8, hlo_path, "tmp/model-dir/xla/model-dir.xla.pjrt_hlo.inputs.antfly-inference-package.json"));
}

test "defaultOutputPath matches backend extension" {
    const onnx_path = try defaultOutputPath(std.testing.allocator, "/tmp/model-dir", .onnx, .hlo, null, null, 128, 128, "paged_prefill");
    defer std.testing.allocator.free(onnx_path);
    try std.testing.expect(std.mem.endsWith(u8, onnx_path, "model-dir/onnx/model-dir.paged_prefill.s128.q128.onnx"));

    const node_path = try defaultOutputPath(std.testing.allocator, "/tmp/model-dir", .onnx, .hlo, 9, "node9.fused_gqa_causal_attention", 1, 1, "paged_prefill");
    defer std.testing.allocator.free(node_path);
    try std.testing.expect(std.mem.endsWith(u8, node_path, "model-dir/onnx/model-dir.node9.fused_gqa_causal_attention.paged_prefill.s1.q1.onnx"));

    const xla_path = try defaultOutputPath(std.testing.allocator, "/tmp/model-dir", .xla, .hlo, null, null, 1, 1, "paged_decode");
    defer std.testing.allocator.free(xla_path);
    try std.testing.expect(std.mem.endsWith(u8, xla_path, "tmp/model-dir/xla/model-dir.paged_decode.s1.q1.hlo"));

    const xla_exec_path = try defaultOutputPath(std.testing.allocator, "/tmp/model-dir", .xla, .executable, null, null, 1, 1, "paged_decode");
    defer std.testing.allocator.free(xla_exec_path);
    try std.testing.expect(std.mem.endsWith(u8, xla_exec_path, "tmp/model-dir/xla/model-dir.paged_decode.s1.q1.pjrt_exec"));
}

test "PJRT artifact kind distinguishes HLO and executable partitions" {
    try std.testing.expectEqualStrings("pjrt_hlo", artifactKind(.xla, false, .hlo));
    try std.testing.expectEqualStrings("pjrt_partition_hlo", artifactKind(.xla, true, .hlo));
    try std.testing.expectEqualStrings("pjrt_executable", artifactKind(.xla, false, .executable));
    try std.testing.expectEqualStrings("pjrt_partition_executable", artifactKind(.xla, true, .executable));
}

test "PJRT executable export budget only gates whole-model artifacts" {
    try guardPjrtExecutableExportBudgetWithMax(false, 1024, 1024);
    try guardPjrtExecutableExportBudgetWithMax(true, 4096, 1024);
    try std.testing.expectError(error.OutOfMemory, guardPjrtExecutableExportBudgetWithMax(false, 4096, 1024));
}

test "PJRT output binding metadata defaults to graph nodes" {
    const bindings = try buildPjrtOutputBindingMeta(std.testing.allocator, &.{ 12, 34 }, &.{});
    defer std.testing.allocator.free(bindings);

    try std.testing.expectEqual(@as(usize, 2), bindings.len);
    try std.testing.expectEqualStrings(compiled_artifact.pjrt_binding_graph_node, bindings[0].kind);
    try std.testing.expectEqual(@as(u32, 12), bindings[0].node_id);
    try std.testing.expectEqualStrings(compiled_artifact.pjrt_binding_graph_node, bindings[1].kind);
    try std.testing.expectEqual(@as(u32, 34), bindings[1].node_id);
}

test "PJRT binding metadata marks semantic KV cache nodes" {
    const attention_bindings = [_]compiled_artifact.AttentionBindingMeta{
        .{ .node_id = 10, .k_node_id = 12, .v_node_id = 13, .layer_index = 2, .skip_kv_write = true },
    };
    const input_bindings = [_]PjrtInputBinding{
        .{ .embedding_ids = 7 },
        .{ .graph_node = 12 },
        .{ .graph_node = 99 },
    };
    const inputs = try buildPjrtInputBindingMeta(std.testing.allocator, &input_bindings, &attention_bindings);
    defer freePjrtInputBindingMeta(std.testing.allocator, inputs);

    try std.testing.expectEqualStrings(compiled_artifact.pjrt_binding_input_ids, inputs[0].kind);
    try std.testing.expectEqualStrings("input_ids", inputs[0].name);
    try std.testing.expectEqualStrings(compiled_artifact.pjrt_binding_past_key, inputs[1].kind);
    try std.testing.expectEqualStrings("past_key_values.2.key", inputs[1].name);
    try std.testing.expectEqualStrings(compiled_artifact.pjrt_binding_graph_node, inputs[2].kind);

    const outputs = try buildPjrtOutputBindingMeta(std.testing.allocator, &.{ 10, 12, 13 }, &attention_bindings);
    defer freePjrtOutputBindingMeta(std.testing.allocator, outputs);
    try std.testing.expectEqualStrings(compiled_artifact.pjrt_binding_graph_node, outputs[0].kind);
    try std.testing.expectEqualStrings(compiled_artifact.pjrt_binding_present_key, outputs[1].kind);
    try std.testing.expectEqualStrings("present.2.key", outputs[1].name);
    try std.testing.expectEqualStrings(compiled_artifact.pjrt_binding_present_value, outputs[2].kind);
    try std.testing.expectEqualStrings("present.2.value", outputs[2].name);
}
