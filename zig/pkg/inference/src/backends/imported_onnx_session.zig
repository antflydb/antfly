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

const std = @import("std");
const build_options = @import("build_options");
const ml = @import("ml");
const onnx_graph = @import("onnx_graph");
const c_file = @import("../util/c_file.zig");
const native_mod = if (build_options.enable_native) @import("../ops/native_compute.zig") else struct {};
const metal_compute_mod = if (build_options.enable_metal) @import("../ops/metal_compute.zig") else struct {};
const graph_interpreter = @import("../graph/interpreter.zig");
const graph_partition = @import("../graph/partition.zig");
const metal_capabilities = @import("../graph/metal_capabilities.zig");
const graph_contracts = @import("../graph/backend_contracts.zig");
const operator_plan = @import("../graph/operator_plan.zig");
const session_mod = @import("session.zig");
const Session = session_mod.Session;
const ResidentInput = session_mod.ResidentInput;
const ResidentOutputs = session_mod.ResidentOutputs;
const Tensor = @import("tensor.zig").Tensor;
const TensorInfo = @import("tensor.zig").TensorInfo;
const DType = @import("tensor.zig").DType;
const BackendType = @import("backends.zig").BackendType;
const InferenceExecutionControl = @import("../execution_control.zig").InferenceExecutionControl;
const ops_mod = @import("../ops/ops.zig");
const executable_mod = @import("../execution/executable.zig");
const graph_runtime_mod = executable_mod.runtime_module;
const ModelProgram = @import("../execution/program.zig").ModelProgram;
const ProgramBinding = @import("../execution/program.zig").Binding;

const NativeCompute = if (build_options.enable_native) native_mod.NativeCompute else opaque {};
const WeightStore = if (build_options.enable_native) native_mod.WeightStore else opaque {};
const RuntimeInput = graph_interpreter.RuntimeInput;
const Graph = ml.graph.Graph;
const Shape = ml.graph.Shape;
const NodeId = ml.graph.NodeId;
const Pipeline = ml.graph.passes.pipeline.Pipeline;
const max_onnx_model_bytes = 2 * 1024 * 1024 * 1024;
const MetalCompute = if (build_options.enable_metal) metal_compute_mod.MetalCompute else opaque {};
const clipclap_audio_input_frames: i64 = 1001;
const clipclap_text_batch: i64 = 1;
const clipclap_text_sequence_length: i64 = 77;

fn shouldSpecializeClipclapAudioInputTime(path: []const u8) bool {
    const base = std.fs.path.basename(path);
    return std.mem.eql(u8, base, "audio_model.onnx");
}

fn shouldSpecializeClipclapTextInputShape(path: []const u8) bool {
    const base = std.fs.path.basename(path);
    return std.mem.eql(u8, base, "text_model.onnx");
}

// Compatibility export for callers retaining shared contexts across stages.
pub const SharedBackendContext = @import("compute_context.zig").SharedBackendContext;
const tensorShapeI32 = @import("compute_context.zig").tensorShapeI32;
const importMetalTensor = @import("compute_context.zig").importMetalTensor;

pub const ImportedOnnxSession = struct {
    allocator: std.mem.Allocator,
    program: ModelProgram,
    input_node_ids: []NodeId,
    input_info: []TensorInfo,
    output_info: []TensorInfo,
    static_runtime_inputs: []RuntimeInput,
    static_names: [][]const u8 = &.{},
    shared_backend_ctx: *SharedBackendContext,
    cb: @import("../ops/ops.zig").ComputeBackend,
    executable: executable_mod.Executable,
    source_path: ?[]u8 = null,
    shape_template: ?[]u8 = null,
    requested_strategy: graph_runtime_mod.Strategy = .interpreter,
    io: ?std.Io = null,
    specialization_mutex: std.atomic.Mutex = .unlocked,
    execution_count: std.atomic.Value(u64) = .init(0),
    last_batch: std.atomic.Value(u64) = .init(0),
    specialization_builds: u64 = 0,
    specialization_hits: u64 = 0,
    specializations: std.ArrayListUnmanaged(Specialization) = .empty,
    const Specialization = struct { shapes: []i64, session: *ImportedOnnxSession };
    const max_specializations = 4;

    pub fn deinit(self: *ImportedOnnxSession) void {
        for (self.specializations.items) |entry| {
            self.allocator.free(entry.shapes);
            entry.session.deinit();
        }
        self.specializations.deinit(self.allocator);
        if (self.source_path) |path| self.allocator.free(path);
        if (self.shape_template) |bytes| self.allocator.free(bytes);
        for (self.input_info) |info| {
            self.allocator.free(info.name);
            self.allocator.free(info.shape);
        }
        self.allocator.free(self.input_info);

        for (self.output_info) |info| {
            self.allocator.free(info.name);
            self.allocator.free(info.shape);
        }
        self.allocator.free(self.output_info);

        for (self.static_runtime_inputs) |ri| self.cb.free(ri.value);
        self.allocator.free(self.static_runtime_inputs);
        for (self.static_names) |name| self.allocator.free(name);
        self.allocator.free(self.static_names);
        self.allocator.free(self.input_node_ids);
        self.executable.deinit();
        self.program.deinit();

        self.shared_backend_ctx.release();
        self.allocator.destroy(self);
    }
};

pub fn createSession(allocator: std.mem.Allocator, onnx_path: []const u8, requested_backend: BackendType) !Session {
    return createSessionWithOptions(allocator, onnx_path, requested_backend, .{});
}

/// Parse and normalize an ONNX graph without constructing backend buffers.
///
/// Discovery uses this as a cached compatibility preflight when no external ONNX
/// Runtime backend is compiled in. It stops before optimization and backend allocation:
/// malformed external-data references, unsupported operators, and invalid graph topology
/// fail early without constant-folding a potentially large model during listing.
pub fn inspectGraphCompatibility(allocator: std.mem.Allocator, onnx_path: []const u8) !void {
    var mapped = try c_file.MmapRegion.init(allocator, onnx_path);
    defer mapped.deinit();
    if (mapped.data.len > max_onnx_model_bytes) return error.FileTooLarge;
    mapped.adviseSequentialPrefix(@min(mapped.data.len, 16 * 1024 * 1024));

    const model_dir = std.fs.path.dirname(onnx_path) orelse ".";
    var model = try onnx_graph.parseLazyAsModelWithBaseDir(allocator, mapped.data, model_dir);
    defer model.deinit();

    var clipclap_dim_overrides: onnx_graph.DimOverrides = .empty;
    defer clipclap_dim_overrides.deinit(allocator);
    var use_clipclap_dim_overrides = false;
    if (shouldSpecializeClipclapAudioInputTime(onnx_path)) {
        try clipclap_dim_overrides.put(allocator, "time", clipclap_audio_input_frames);
        use_clipclap_dim_overrides = true;
    }
    if (shouldSpecializeClipclapTextInputShape(onnx_path)) {
        try clipclap_dim_overrides.put(allocator, "batch_size", clipclap_text_batch);
        try clipclap_dim_overrides.put(allocator, "sequence_length", clipclap_text_sequence_length);
        use_clipclap_dim_overrides = true;
    }
    const dim_overrides: ?*const onnx_graph.DimOverrides = if (use_clipclap_dim_overrides) &clipclap_dim_overrides else null;

    var converted = try model.convertToGraphWithDims(allocator, dim_overrides);
    defer converted.deinit(allocator);
    try validateImportedGraph(&converted.graph, "convert");
}

/// Files that participate in one ONNX model load. ONNX stores large tensors in
/// external files, so treating only the protobuf as the model artifact can
/// under-account residency by gigabytes and leaves dependency changes invisible
/// to compatibility caches.
pub const OnnxArtifactSet = struct {
    allocator: std.mem.Allocator,
    external_paths: [][]u8,
    encoded_bytes: usize,

    pub fn deinit(self: *OnnxArtifactSet) void {
        for (self.external_paths) |path| self.allocator.free(path);
        self.allocator.free(self.external_paths);
        self.* = undefined;
    }
};

/// Inspect only declared graph I/O. No tensor data or backend session is loaded.
/// The caller provides an arena so all descriptors have one short-lived owner.
pub fn inspectSignature(arena: std.mem.Allocator, onnx_path: []const u8) !struct { inputs: []TensorInfo, outputs: []TensorInfo } {
    var mapped = try c_file.MmapRegion.init(arena, onnx_path);
    defer mapped.deinit();
    if (mapped.data.len > max_onnx_model_bytes) return error.FileTooLarge;
    var model = try onnx_graph.parseLazyAsModelWithBaseDir(arena, mapped.data, std.fs.path.dirname(onnx_path) orelse ".");
    defer model.deinit();
    const graph = model.graph() orelse return error.InvalidOnnxGraph;
    var inputs = std.ArrayList(TensorInfo).empty;
    for (graph.inputs) |input| {
        if (input.name.len == 0 or !model.input_set.contains(input.name)) continue;
        if (input.type_proto == null or input.type_proto.?.tensor_type == null) return error.IncompatibleModel;
        if (input.type_proto.?.tensor_type.?.shape == null) return error.IncompatibleModel;
        try inputs.append(arena, try tensorInfoFromValueOrShape(arena, input.name, input.type_proto, Shape.init(.f32, &.{})));
    }
    const outputs = try arena.alloc(TensorInfo, graph.outputs.len);
    for (graph.outputs, outputs) |output, *info| {
        if (output.type_proto == null or output.type_proto.?.tensor_type == null) return error.IncompatibleModel;
        if (output.type_proto.?.tensor_type.?.shape == null) return error.IncompatibleModel;
        info.* = try tensorInfoFromValueOrShape(arena, output.name, output.type_proto, Shape.init(.f32, &.{}));
    }
    return .{ .inputs = inputs.items, .outputs = outputs };
}

/// Parse only initializer metadata and return the complete, validated artifact
/// set. External files are deduplicated and conservatively charged at their
/// full file size: runtimes may mmap or cache the complete backing file even
/// when individual tensors reference disjoint ranges.
pub fn inspectArtifactSet(
    allocator: std.mem.Allocator,
    onnx_path: []const u8,
) !OnnxArtifactSet {
    var mapped = try c_file.MmapRegion.init(allocator, onnx_path);
    defer mapped.deinit();
    if (mapped.data.len > max_onnx_model_bytes) return error.FileTooLarge;
    mapped.adviseSequentialPrefix(@min(mapped.data.len, 16 * 1024 * 1024));

    const model_dir = std.fs.path.dirname(onnx_path) orelse ".";
    var model = try onnx_graph.parseLazyAsModelWithBaseDir(allocator, mapped.data, model_dir);
    defer model.deinit();

    var paths = std.ArrayListUnmanaged([]u8).empty;
    errdefer {
        for (paths.items) |path| allocator.free(path);
        paths.deinit(allocator);
    }
    var encoded_bytes = mapped.data.len;

    var initializer_it = model.initializer_map.iterator();
    while (initializer_it.next()) |entry| {
        const tensor = try model.initializerAt(entry.value_ptr.*);
        if (!tensor.isExternal()) continue;
        const external = tensor.externalDataInfo();
        if (external.location.len == 0) return error.InvalidExternalPath;
        if (std.fs.path.isAbsolute(external.location)) return error.InvalidExternalPath;
        if (std.mem.indexOfScalar(u8, external.location, 0) != null) return error.InvalidExternalPath;
        if (std.mem.indexOfScalar(u8, external.location, '\\') != null) return error.InvalidExternalPath;
        var component_it = std.mem.splitScalar(u8, external.location, std.fs.path.sep);
        while (component_it.next()) |component| {
            if (std.mem.eql(u8, component, "..")) return error.InvalidExternalPath;
        }
        if (external.offset < 0) return error.InvalidExternalOffset;

        const full_path = try std.fs.path.join(allocator, &.{ model_dir, external.location });
        var duplicate = false;
        for (paths.items) |existing| {
            if (std.mem.eql(u8, existing, full_path)) {
                duplicate = true;
                break;
            }
        }

        const file_size = c_file.fileSize(allocator, full_path) catch |err| {
            allocator.free(full_path);
            return err;
        };
        const offset: u64 = @intCast(external.offset);
        if (offset > file_size) {
            allocator.free(full_path);
            return error.ExternalRegionOutOfBounds;
        }
        if (external.length >= 0) {
            const end = std.math.add(u64, offset, @as(u64, @intCast(external.length))) catch {
                allocator.free(full_path);
                return error.ExternalRegionOutOfBounds;
            };
            if (end > file_size) {
                allocator.free(full_path);
                return error.ExternalRegionOutOfBounds;
            }
        }

        if (duplicate) {
            allocator.free(full_path);
            continue;
        }
        const size = std.math.cast(usize, file_size) orelse {
            allocator.free(full_path);
            return error.ResourceLimitExceeded;
        };
        encoded_bytes = std.math.add(usize, encoded_bytes, size) catch {
            allocator.free(full_path);
            return error.ResourceLimitExceeded;
        };
        paths.append(allocator, full_path) catch |err| {
            allocator.free(full_path);
            return err;
        };
    }

    return .{
        .allocator = allocator,
        .external_paths = try paths.toOwnedSlice(allocator),
        .encoded_bytes = encoded_bytes,
    };
}

/// Keep graph structure and shape constants immutable across cache misses, without
/// retaining a second host copy of the model's large weights. All large weights
/// are bound through the parent's resident handles. In particular, replacing a
/// managed model directory must not mix new graph metadata with old weights.
fn snapshotShapeTemplate(allocator: std.mem.Allocator, model: *onnx_graph.Model) ![]u8 {
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const scratch = arena.allocator();
    var graph = (model.graph() orelse return error.InvalidOnnxGraph).*;
    graph.initializers = try scratch.alloc(onnx_graph.proto.TensorProto, model.initializer_map.count());
    var names = model.initializer_map.keyIterator();
    var i: usize = 0;
    while (names.next()) |name| : (i += 1) {
        const data = model.getInitializer(name.*) orelse return error.InvalidOnnxGraph;
        const tensor = data.tensor;
        const small_constant = onnx_graph.tensor.numElements(tensor.dims) <= 16 and
            (tensor.data_type == .float32 or tensor.data_type == .int32 or tensor.data_type == .int64);
        graph.initializers[i] = .{ .name = tensor.name, .dims = tensor.dims, .data_type = tensor.data_type };
        if (small_constant) {
            if (!tensor.isExternal()) {
                graph.initializers[i] = tensor.*;
            } else {
                // An unresolved/unused external initializer must remain a
                // parameter, just as it was in the original conversion.
                const bytes = onnx_graph.tensor.extractNativeBytesWithExternal(scratch, tensor, model.base_dir) catch {
                    graph.initializers[i].data_location = .external;
                    continue;
                };
                graph.initializers[i].raw_data = bytes;
            }
        }
    }
    var snapshot = model.onnx;
    snapshot.graph = graph;
    return onnx_graph.serializeModel(allocator, &snapshot);
}

pub const ImportedOnnxSessionOptions = struct {
    graph_runtime_strategy: ?graph_runtime_mod.Strategy = null,
    shared_backend_ctx: ?*SharedBackendContext = null,
    dim_overrides: ?*const onnx_graph.DimOverrides = null,
    /// Internal shape-plan construction. Weight handles are retained from the
    /// source session, so cached shapes never duplicate the model's weights.
    input_shapes: ?[]const Tensor = null,
    static_source: ?*const ImportedOnnxSession = null,
    /// Caller's Io runtime, used by the compute backend for parallel GEMM
    /// dispatch via `linalg.sgemm*Io`.  When null, backends fall back to
    /// the void linalg API which uses a process-wide futex pool internally.
    io: ?std.Io = null,
};

pub fn createSessionWithOptions(
    allocator: std.mem.Allocator,
    onnx_path: []const u8,
    requested_backend: BackendType,
    options: ImportedOnnxSessionOptions,
) !Session {
    if (options.shared_backend_ctx) |shared| {
        if (shared.backendType() != requested_backend) return error.SharedBackendContextMismatch;
    }

    if (options.static_source) |source| {
        if (options.shared_backend_ctx != source.shared_backend_ctx) return error.SharedBackendContextMismatch;
    }

    const model_bytes = if (options.static_source) |source|
        source.shape_template orelse return error.MissingOnnxShapeTemplate
    else
        try c_file.readFileMax(allocator, onnx_path, max_onnx_model_bytes);
    defer if (options.static_source == null) allocator.free(model_bytes);

    const model_dir = std.fs.path.dirname(onnx_path) orelse ".";
    var model = try onnx_graph.parseLazyAsModelWithBaseDir(allocator, model_bytes, if (options.static_source == null) model_dir else null);
    defer model.deinit();

    var clipclap_dim_overrides: onnx_graph.DimOverrides = .empty;
    defer clipclap_dim_overrides.deinit(allocator);
    var use_clipclap_dim_overrides = false;
    if (options.dim_overrides == null and shouldSpecializeClipclapAudioInputTime(onnx_path)) {
        try clipclap_dim_overrides.put(allocator, "time", clipclap_audio_input_frames);
        use_clipclap_dim_overrides = true;
    }
    if (options.dim_overrides == null and shouldSpecializeClipclapTextInputShape(onnx_path)) {
        try clipclap_dim_overrides.put(allocator, "batch_size", clipclap_text_batch);
        try clipclap_dim_overrides.put(allocator, "sequence_length", clipclap_text_sequence_length);
        use_clipclap_dim_overrides = true;
    }
    const dim_overrides = options.dim_overrides orelse if (use_clipclap_dim_overrides) &clipclap_dim_overrides else null;

    var concrete_inputs: std.StringHashMapUnmanaged(Shape) = .empty;
    defer concrete_inputs.deinit(allocator);
    if (options.input_shapes) |inputs| {
        for (inputs) |input| {
            try concrete_inputs.put(allocator, input.name, Shape.init(std.meta.stringToEnum(ml.graph.DType, @tagName(input.dtype)) orelse return error.UnsupportedTensorType, input.shape));
        }
    }
    var converted = try model.convertToGraphWithInputShapes(allocator, dim_overrides, if (options.input_shapes != null) &concrete_inputs else null);
    errdefer converted.deinit(allocator);

    var bindings: std.ArrayListUnmanaged(ProgramBinding) = .empty;
    defer bindings.deinit(allocator);
    for (converted.parameter_names, converted.graph.parameters.items) |name, node_id| {
        try bindings.append(allocator, .{ .name = name, .node_id = node_id, .kind = if (model.input_set.contains(name)) .input else .weight });
    }
    const model_graph = model.graph() orelse return error.InvalidOnnxGraph;
    for (model_graph.outputs, converted.output_ids) |output, node_id| {
        try bindings.append(allocator, .{ .name = output.name, .node_id = node_id, .kind = .output });
    }
    var program = try ModelProgram.init(allocator, &converted.graph, bindings.items);
    var program_owned = true;
    errdefer if (program_owned) program.deinit();
    try validateImportedGraph(&program.graph, "shared_preparation");
    try validateRuntimeShapeBackend(&program.graph, requested_backend);

    const remapped_params = try remapReachableParameters(
        allocator,
        converted.parameter_names,
        converted.graph.parameters.items,
        program.id_map,
    );
    defer {
        allocator.free(remapped_params.names);
        allocator.free(remapped_params.node_ids);
    }

    converted.graph.deinit();
    converted.graph = Graph.init(allocator);

    const graph = program.graph;
    const parameter_names = remapped_params.names;
    const parameter_node_ids = remapped_params.node_ids;
    if (parameter_names.len != parameter_node_ids.len) {
        std.log.err("onnx graph parameter mismatch names={} nodes={}", .{ parameter_names.len, parameter_node_ids.len });
        for (parameter_node_ids, 0..) |node_id, i| {
            const node = graph.node(node_id);
            const graph_name = if (std.meta.activeTag(node.op) == .parameter) graph.parameterName(node) else "<non-parameter>";
            const converted_name = if (i < parameter_names.len) parameter_names[i] else "<missing>";
            std.log.err("onnx graph parameter[{}] graph={s} converted={s}", .{ i, graph_name, converted_name });
        }
        return error.InvalidOnnxGraph;
    }

    const input_node_ids = try remapModelInputBindings(
        allocator,
        &model,
        parameter_names,
        parameter_node_ids,
    );
    defer allocator.free(input_node_ids);

    const static_params = try filterStaticParameters(allocator, &model, parameter_names, parameter_node_ids);
    defer {
        allocator.free(static_params.names);
        allocator.free(static_params.node_ids);
    }

    const self = try allocator.create(ImportedOnnxSession);
    errdefer allocator.destroy(self);

    // Visibility on whether the GEMM backend is composing with the caller's
    // runtime or falling back to the process-wide futex pool.  The latter is
    // correct but bypasses any cooperative scheduling -- worth flagging once
    // per session so misconfigurations are visible without needing a debugger.
    if (options.io == null) {
        std.log.debug(
            "imported_onnx_session: created without Io; matmul will use lib/linalg's *Sync futex pool",
            .{},
        );
    } else {
        std.log.debug(
            "imported_onnx_session: created with Io; matmul will dispatch through caller's runtime",
            .{},
        );
    }

    const owned_input_ids = try allocator.dupe(NodeId, input_node_ids);
    errdefer allocator.free(owned_input_ids);
    const shared_backend = if (options.shared_backend_ctx) |shared| shared.retain() else try SharedBackendContext.init(allocator, requested_backend, options.io);
    errdefer shared_backend.release();
    self.* = .{
        .allocator = allocator,
        .program = program,
        .input_node_ids = owned_input_ids,
        .input_info = undefined,
        .output_info = undefined,
        .static_runtime_inputs = undefined,
        .shared_backend_ctx = shared_backend,
        .cb = undefined,
        .executable = undefined,
    };
    program_owned = false;
    errdefer self.program.deinit();

    self.cb = self.shared_backend_ctx.computeBackend();
    self.io = options.io;
    self.requested_strategy = options.graph_runtime_strategy orelse graph_runtime_mod.strategyFromEnv();
    if ((requested_backend == .native or requested_backend == .metal or requested_backend == .cuda) and options.graph_runtime_strategy == null and
        @import("antfly_platform").env.getenv("TERMITE_GRAPH_RUNTIME") == null and
        @import("antfly_platform").env.getenv("TERMITE_ONNX_GRAPH_RUNTIME") == null)
        self.requested_strategy = .compiled_preferred;
    if ((requested_backend == .native or requested_backend == .metal or requested_backend == .cuda) and options.input_shapes == null) {
        for (self.input_node_ids) |node_id| {
            if (node_id == ml.graph.null_node) continue;
            const shape = self.program.graph.node(node_id).output_shape;
            for (shape.dims[0..shape.rank()]) |dim| {
                if (dim < 0) {
                    self.source_path = try allocator.dupe(u8, onnx_path);
                    break;
                }
            }
            if (self.source_path != null) break;
        }
    }
    errdefer if (self.source_path) |path| allocator.free(path);
    if (requested_backend == .cuda and self.requested_strategy == .interpreter and self.source_path == null) {
        for (self.program.graph.nodes.items, 0..) |node, id| {
            if (!@import("../graph/cuda_capabilities.zig").decide(.{ .graph = &self.program.graph, .node_id = @intCast(id), .op = node.op }).can_execute)
                return error.UnsupportedOnnxGraphBackendOperation;
        }
    }
    self.input_info = buildInputInfo(allocator, &model, &self.program.graph, self.input_node_ids) catch |err| {
        std.log.err("buildInputInfo failed: {}", .{err});
        return err;
    };
    errdefer freeTensorInfoList(allocator, self.input_info);

    self.output_info = buildOutputInfo(allocator, &model, &self.program.graph, self.program.graph.outputs.items) catch |err| {
        std.log.err("buildOutputInfo failed: {} outputs={} converted_outputs={}", .{
            err,
            if (model.graph()) |g| g.outputs.len else 0,
            self.program.graph.outputs.items.len,
        });
        return err;
    };
    errdefer freeTensorInfoList(allocator, self.output_info);

    self.static_names = try allocator.alloc([]const u8, static_params.names.len);
    var initialized_names: usize = 0;
    errdefer {
        for (self.static_names[0..initialized_names]) |name| allocator.free(name);
        allocator.free(self.static_names);
    }
    for (static_params.names, self.static_names) |name, *owned| {
        owned.* = try allocator.dupe(u8, name);
        initialized_names += 1;
    }
    self.static_runtime_inputs = buildStaticRuntimeInputs(
        allocator,
        &model,
        self.shared_backend_ctx,
        static_params.names,
        static_params.node_ids,
        options.static_source,
    ) catch |err| {
        std.log.err("buildStaticRuntimeInputs failed: {}", .{err});
        return err;
    };
    errdefer {
        for (self.static_runtime_inputs) |ri| self.cb.free(ri.value);
        allocator.free(self.static_runtime_inputs);
    }

    self.executable = try executable_mod.prepareWithOptions(
        allocator,
        &self.program,
        &self.cb,
        if (self.source_path != null) .interpreter else self.requested_strategy,
        .{ .input_bindings = self.static_runtime_inputs },
    );
    errdefer self.executable.deinit();

    if (self.source_path != null) self.shape_template = try snapshotShapeTemplate(allocator, &model);
    errdefer if (self.shape_template) |bytes| allocator.free(bytes);

    logImportedGraphBindings(&self.program.graph, self.input_node_ids, self.program.graph.outputs.items);

    allocator.free(converted.output_ids);
    allocator.free(converted.parameter_names);

    return .{
        .ptr = self,
        .vtable = &imported_session_vtable,
        .shared_compute_context = self.shared_backend_ctx,
    };
}

fn configureExecutionDiagnostics(ptr: *anyopaque, enabled: bool) void {
    const self: *ImportedOnnxSession = @ptrCast(@alignCast(ptr));
    self.executable.diagnostics.configure(enabled);
}
fn executionDiagnostics(ptr: *anyopaque) ?@import("../execution/diagnostics.zig").Snapshot {
    const self: *ImportedOnnxSession = @ptrCast(@alignCast(ptr));
    return self.executable.diagnostics.snapshot();
}

const imported_session_vtable = Session.VTable{
    .configureExecutionDiagnostics = configureExecutionDiagnostics,
    .executionDiagnostics = executionDiagnostics,
    .run = run,
    .runWithControl = runWithControl,
    .inputInfo = inputInfo,
    .outputInfo = outputInfo,
    .backend = backend,
    .close = close,
    .runResident = runResident,
    .runResidentInputs = runResidentInputs,
    .runResidentWithControl = runResidentWithControl,
    .runResidentInputsWithControl = runResidentInputsWithControl,
};

fn runWithControl(
    ptr: *anyopaque,
    inputs: []const Tensor,
    allocator: std.mem.Allocator,
    control: InferenceExecutionControl,
) ![]Tensor {
    const self: *ImportedOnnxSession = @ptrCast(@alignCast(ptr));
    return runImpl(self, inputs, allocator, control);
}

pub fn sharedBackendContext(session: Session) ?*SharedBackendContext {
    return session.shared_compute_context;
}

fn releaseResidentContext(owner: *anyopaque, _: std.mem.Allocator) void {
    const context: *SharedBackendContext = @ptrCast(@alignCast(owner));
    context.release();
}

pub const ExecutionStats = struct {
    strategy: graph_runtime_mod.Strategy,
    executions: u64,
    last_batch: u64,
    cached_plans: usize,
    plan_builds: u64,
    plan_hits: u64,
    shared_initializers: usize,
};

pub fn executionStats(session: Session) ?ExecutionStats {
    if (session.vtable != &imported_session_vtable) return null;
    const self: *ImportedOnnxSession = @ptrCast(@alignCast(session.ptr));
    @import("antfly_platform").sync.lockYielding(&self.specialization_mutex);
    defer self.specialization_mutex.unlock();
    return .{
        .strategy = self.requested_strategy,
        .executions = self.execution_count.load(.monotonic),
        .last_batch = self.last_batch.load(.monotonic),
        .cached_plans = self.specializations.items.len,
        .plan_builds = self.specialization_builds,
        .plan_hits = self.specialization_hits,
        .shared_initializers = self.static_runtime_inputs.len,
    };
}

fn importedOnnxTraceEnabled() bool {
    if (comptime build_options.enable_wasm or !build_options.link_libc) return false;
    const value = std.c.getenv("TERMITE_ONNX_GRAPH_TRACE") orelse return false;
    return value[0] != 0 and value[0] != '0';
}

fn logImportedGraphBindings(graph: *const Graph, input_node_ids: []const NodeId, output_node_ids: []const NodeId) void {
    if (!importedOnnxTraceEnabled()) return;
    for (input_node_ids, 0..) |input_id, input_idx| {
        std.log.info("termite onnx graph input index={} node_id={d}", .{ input_idx, input_id });
        if (input_id == ml.graph.null_node) continue;
        var consumer_count: usize = 0;
        for (graph.nodes.items, 0..) |node, node_idx| {
            for (node.getInputs()) |dep| {
                if (dep != input_id) continue;
                std.log.info("termite onnx graph input consumer input_index={} input_node={d} consumer_node={d} op={s} shape={any}", .{
                    input_idx,
                    input_id,
                    node_idx,
                    @tagName(std.meta.activeTag(node.op)),
                    node.output_shape,
                });
                consumer_count += 1;
            }
        }
        if (consumer_count == 0) {
            std.log.warn("termite onnx graph input index={} node_id={d} has no direct consumers", .{ input_idx, input_id });
        }
    }
    for (output_node_ids, 0..) |output_id, output_idx| {
        const node = graph.node(output_id);
        std.log.info("termite onnx graph output index={} node_id={d} op={s} inputs={any} shape={any}", .{
            output_idx,
            output_id,
            @tagName(std.meta.activeTag(node.op)),
            node.getInputs(),
            node.output_shape,
        });
    }
}

fn run(ptr: *anyopaque, inputs: []const Tensor, allocator: std.mem.Allocator) anyerror![]Tensor {
    const self: *ImportedOnnxSession = @ptrCast(@alignCast(ptr));
    return runImpl(self, inputs, allocator, null);
}

fn runImpl(
    self: *ImportedOnnxSession,
    inputs: []const Tensor,
    allocator: std.mem.Allocator,
    control: ?InferenceExecutionControl,
) ![]Tensor {
    return runWithOutputs(self, inputs, null, allocator, control, false);
}

fn exportOutputs(self: *ImportedOnnxSession, result: *graph_runtime_mod.Result, allocator: std.mem.Allocator, control: ?InferenceExecutionControl) ![]Tensor {
    const outputs = try allocator.alloc(Tensor, result.outputs.len);
    var output_count: usize = 0;
    errdefer {
        for (outputs[0..output_count]) |*tensor| tensor.deinit();
        allocator.free(outputs);
    }
    for (result.outputs, 0..) |output_ct, i| {
        if (control) |active| try active.check();
        const owner = result.backendFor(&self.executable.runtime, i);
        const info = self.output_info[i];
        const shape = owner.tensorShape(output_ct, allocator) catch try allocator.dupe(i64, info.shape);
        defer allocator.free(shape);
        if (try owner.exportTensorData(output_ct, allocator)) |exported| {
            outputs[i] = try tensorFromExportedData(allocator, info.name, shape, exported);
        } else {
            const values = try owner.toFloat32(output_ct, allocator);
            defer allocator.free(values);
            outputs[i] = try tensorFromNumericValues(allocator, info.name, info.dtype, shape, values);
        }
        output_count += 1;
    }
    return outputs;
}

fn runResident(ptr: *anyopaque, inputs: []const Tensor, allocator: std.mem.Allocator) anyerror!?ResidentOutputs {
    const self: *ImportedOnnxSession = @ptrCast(@alignCast(ptr));
    return try runResidentImpl(self, inputs, null, allocator, null);
}

fn runResidentInputs(ptr: *anyopaque, inputs: []const ResidentInput, allocator: std.mem.Allocator) anyerror!?ResidentOutputs {
    const self: *ImportedOnnxSession = @ptrCast(@alignCast(ptr));
    return try runResidentImpl(self, null, inputs, allocator, null);
}

fn runResidentWithControl(
    ptr: *anyopaque,
    inputs: []const Tensor,
    allocator: std.mem.Allocator,
    control: InferenceExecutionControl,
) anyerror!?ResidentOutputs {
    const self: *ImportedOnnxSession = @ptrCast(@alignCast(ptr));
    return try runResidentImpl(self, inputs, null, allocator, control);
}

fn runResidentInputsWithControl(
    ptr: *anyopaque,
    inputs: []const ResidentInput,
    allocator: std.mem.Allocator,
    control: InferenceExecutionControl,
) anyerror!?ResidentOutputs {
    const self: *ImportedOnnxSession = @ptrCast(@alignCast(ptr));
    return try runResidentImpl(self, null, inputs, allocator, control);
}

fn specializeForInputs(self: *ImportedOnnxSession, inputs: []const Tensor) !*ImportedOnnxSession {
    if (inputs.len != self.input_info.len) return error.InputArityMismatch;
    var key: std.ArrayListUnmanaged(i64) = .empty;
    defer key.deinit(self.allocator);
    for (inputs, self.input_info) |input, info| {
        if (!std.mem.eql(u8, input.name, info.name) or input.dtype != info.dtype or input.shape.len != info.shape.len or input.shape.len > ml.graph.shape.max_rank)
            return error.InvalidTensorShape;
        try key.append(self.allocator, @intCast(input.shape.len));
        for (input.shape, info.shape) |actual, declared| {
            if (actual <= 0 or (declared > 0 and declared != actual)) return error.InvalidTensorShape;
            try key.append(self.allocator, actual);
        }
    }
    for (self.specializations.items, 0..) |entry, i| {
        if (!std.mem.eql(i64, entry.shapes, key.items)) continue;
        // Move a hit to the MRU end without reallocating or copying tensors.
        self.specialization_hits += 1;
        const hit = self.specializations.orderedRemove(i);
        self.specializations.appendAssumeCapacity(hit);
        return hit.session;
    }
    const session = try createSessionWithOptions(self.allocator, self.source_path.?, self.shared_backend_ctx.backendType(), .{
        .graph_runtime_strategy = self.requested_strategy,
        .shared_backend_ctx = self.shared_backend_ctx,
        .input_shapes = inputs,
        .static_source = self,
        .io = self.io,
    });
    const specialized: *ImportedOnnxSession = @ptrCast(@alignCast(session.ptr));
    errdefer specialized.deinit();
    const owned_key = try self.allocator.dupe(i64, key.items);
    errdefer self.allocator.free(owned_key);
    try self.specializations.ensureUnusedCapacity(self.allocator, 1);
    if (self.specializations.items.len == ImportedOnnxSession.max_specializations) {
        const oldest = self.specializations.orderedRemove(0);
        oldest.session.deinit();
        self.allocator.free(oldest.shapes);
    }
    self.specialization_builds += 1;
    self.specializations.appendAssumeCapacity(.{ .shapes = owned_key, .session = specialized });
    return specialized;
}

fn runResidentImpl(
    self: *ImportedOnnxSession,
    host_inputs: ?[]const Tensor,
    resident_inputs: ?[]const ResidentInput,
    allocator: std.mem.Allocator,
    control: ?InferenceExecutionControl,
) !ResidentOutputs {
    return runWithOutputs(self, host_inputs, resident_inputs, allocator, control, true);
}

fn runWithOutputs(
    self: *ImportedOnnxSession,
    host_inputs: ?[]const Tensor,
    resident_inputs: ?[]const ResidentInput,
    allocator: std.mem.Allocator,
    control: ?InferenceExecutionControl,
    comptime resident: bool,
) !if (resident) ResidentOutputs else []Tensor {
    _ = self.execution_count.fetchAdd(1, .monotonic);
    if (host_inputs) |inputs| {
        if (inputs.len > 0 and inputs[0].shape.len > 0 and inputs[0].shape[0] > 0)
            self.last_batch.store(@intCast(inputs[0].shape[0]), .monotonic);
    } else if (resident_inputs) |inputs| {
        if (inputs.len > 0) {
            const shape = try inputs[0].backend.tensorShape(inputs[0].value, allocator);
            defer allocator.free(shape);
            if (shape.len > 0 and shape[0] > 0) self.last_batch.store(@intCast(shape[0]), .monotonic);
        }
    }
    if (control) |active| try active.check();
    if (self.source_path != null) {
        @import("antfly_platform").sync.lockYielding(&self.specialization_mutex);
        defer self.specialization_mutex.unlock();
        // Shape metadata is sufficient to select a plan for device inputs;
        // never download resident tensors merely to bind dynamic dimensions.
        const metadata = if (host_inputs == null) try allocator.alloc(Tensor, if (resident_inputs) |inputs| inputs.len else 0) else null;
        var initialized: usize = 0;
        defer if (metadata) |inputs| {
            for (inputs[0..initialized]) |input| allocator.free(input.shape);
            allocator.free(inputs);
        };
        if (metadata) |inputs| {
            if (inputs.len != self.input_info.len) return error.InputArityMismatch;
            for (resident_inputs.?, self.input_info, inputs) |input, info, *meta| {
                try validateResidentInput(self, input);
                meta.* = .{
                    .name = info.name,
                    .dtype = info.dtype,
                    .shape = try input.backend.tensorShape(input.value, allocator),
                    .data = &.{},
                    .allocator = allocator,
                    .owns_data = false,
                    .owns_shape = false,
                };
                initialized += 1;
            }
        }
        const specialized = try specializeForInputs(self, host_inputs orelse metadata.?);
        const collect_diagnostics = self.executable.diagnostics.isEnabled();
        specialized.executable.diagnostics.configure(collect_diagnostics);
        defer if (collect_diagnostics) {
            if (specialized.executable.diagnostics.snapshot()) |snapshot| self.executable.diagnostics.publish(snapshot);
        };
        if (resident) {
            var outputs = try runWithOutputs(specialized, host_inputs, resident_inputs, allocator, control, true);
            // Normalized resident values use the shared context and survive
            // eviction. Producer-owned host exports finish inside this lock.
            outputs.backend = self.shared_backend_ctx.stableBackend();
            return outputs;
        } else return runWithOutputs(specialized, host_inputs, resident_inputs, allocator, control, false);
    }
    const input_len = if (host_inputs) |inputs| inputs.len else if (resident_inputs) |inputs| inputs.len else 0;
    if (input_len != self.input_info.len) return error.InputArityMismatch;

    var live_dynamic_count: usize = 0;
    for (self.input_node_ids) |node_id| {
        if (node_id != ml.graph.null_node) live_dynamic_count += 1;
    }

    const dynamic_inputs = try allocator.alloc(RuntimeInput, live_dynamic_count);
    defer allocator.free(dynamic_inputs);

    var imported_count: usize = 0;
    errdefer {
        if (resident_inputs == null) {
            for (dynamic_inputs[0..imported_count]) |ri| self.cb.free(ri.value);
        }
    }

    for (self.input_node_ids, 0..) |node_id, i| {
        if (control) |active| try active.check();
        if (node_id == ml.graph.null_node) continue;
        const value = if (host_inputs) |inputs|
            try self.shared_backend_ctx.importHostTensor(allocator, &inputs[i])
        else if (resident_inputs) |inputs| blk: {
            try validateResidentInput(self, inputs[i]);
            break :blk inputs[i].value;
        } else unreachable;
        dynamic_inputs[imported_count] = .{
            .node_id = node_id,
            .value = value,
        };
        imported_count += 1;
    }

    const runtime_inputs = try allocator.alloc(RuntimeInput, self.static_runtime_inputs.len + dynamic_inputs.len);
    defer allocator.free(runtime_inputs);
    @memcpy(runtime_inputs[0..self.static_runtime_inputs.len], self.static_runtime_inputs);
    @memcpy(runtime_inputs[self.static_runtime_inputs.len..], dynamic_inputs);
    const sdpa_mask = if (host_inputs) |inputs| findSdpaMask(inputs) else null;

    var exec_result = try self.executable.run(allocator, .{
        .runtime_inputs = runtime_inputs,
        .sdpa_mask = sdpa_mask,
        .execution_control = control,
        .output_placement = if (resident) .default_backend else .producer,
        .collect_partition_stats = self.executable.diagnostics.isEnabled(),
    });
    errdefer exec_result.deinit(&self.executable.runtime);
    if (control) |active| try active.check();

    if (!resident) {
        const outputs = try exportOutputs(self, &exec_result, allocator, control);
        self.executable.diagnostics.record(exec_result.stats, &self.cb);
        exec_result.deinit(&self.executable.runtime);
        for (dynamic_inputs[0..imported_count]) |ri| {
            if (resident_inputs == null) self.cb.free(ri.value);
        }
        return outputs;
    }
    const outputs = exec_result.outputs;
    exec_result.outputs = &.{};
    for (dynamic_inputs[0..imported_count]) |ri| {
        if (resident_inputs == null) self.cb.free(ri.value);
    }
    return .{
        .outputs = outputs,
        .backend = self.shared_backend_ctx.stableBackend(),
        .backend_owner = self.shared_backend_ctx.retain(),
        .deinit_backend_owner = releaseResidentContext,
        .allocator = exec_result.allocator,
    };
}

fn validateResidentInput(self: *ImportedOnnxSession, input: ResidentInput) !void {
    if (input.backend.ptr == self.cb.ptr and input.backend.vtable == self.cb.vtable) return;
    if (input.backend.kind() == .native and self.cb.kind() == .native) return;
    return error.UnsupportedResidentInputBackend;
}

fn findSdpaMask(inputs: []const Tensor) ?[]const i64 {
    for (inputs) |*input| {
        if (!std.mem.eql(u8, input.name, "attention_mask")) continue;
        if (input.dtype != .i64) return null;
        return input.asInt64();
    }
    return null;
}

fn inputInfo(ptr: *anyopaque) []const TensorInfo {
    const self: *ImportedOnnxSession = @ptrCast(@alignCast(ptr));
    return self.input_info;
}

fn outputInfo(ptr: *anyopaque) []const TensorInfo {
    const self: *ImportedOnnxSession = @ptrCast(@alignCast(ptr));
    return self.output_info;
}

fn backend(ptr: *anyopaque) BackendType {
    const self: *ImportedOnnxSession = @ptrCast(@alignCast(ptr));
    return self.shared_backend_ctx.backendType();
}

fn close(ptr: *anyopaque) void {
    const self: *ImportedOnnxSession = @ptrCast(@alignCast(ptr));
    self.deinit();
}

fn validateImportedGraph(graph: *const Graph, stage: []const u8) !void {
    for (graph.nodes.items, 0..) |node, node_id| {
        for (0..node.num_inputs) |input_idx| {
            const input_id = node.inputs[input_idx];
            if (input_id == ml.graph.null_node) {
                std.log.err("onnx graph invalid after {s}: node_id={} op={s} has null input at index {}", .{
                    stage,
                    node_id,
                    @tagName(std.meta.activeTag(node.op)),
                    input_idx,
                });
                return error.InvalidOnnxGraph;
            }
            if (input_id >= node_id) {
                std.log.err("onnx graph invalid after {s}: node_id={} op={s} input_id={} is not topologically earlier", .{
                    stage,
                    node_id,
                    @tagName(std.meta.activeTag(node.op)),
                    input_id,
                });
                return error.InvalidOnnxGraph;
            }
        }
    }
}

fn validateRuntimeShapeBackend(graph: *const Graph, backend_type: BackendType) !void {
    if (backend_type != .native) {
        for (graph.nodes.items, 0..) |node, node_id| {
            const unsupported = switch (node.op) {
                .conv_general => |attrs| attrs.transposed,
                .average_pool => true,
                else => false,
            };
            if (!unsupported) continue;
            std.log.info(
                "imported onnx backend {s} does not support {s} at node_id={}; falling back",
                .{ @tagName(backend_type), @tagName(node.op), node_id },
            );
            return error.UnsupportedOnnxGraphBackendOperation;
        }
    }

    // Native and Metal execute imported Slice and its consumers using runtime
    // dimensions. Other graph backends still require static downstream shapes.
    if (backend_type == .native or backend_type == .metal) return;

    for (graph.nodes.items, 0..) |node, node_id| {
        const attrs = switch (node.op) {
            .slice => |slice_attrs| slice_attrs,
            else => continue,
        };
        if (!attrs.runtime_starts and !attrs.runtime_limits) continue;

        std.log.info(
            "imported onnx backend {s} does not support runtime-bound Slice at node_id={}; falling back",
            .{ @tagName(backend_type), node_id },
        );
        return error.UnsupportedDynamicOnnxGraphBackend;
    }
}

test "runtime-bound ONNX slices admit native and Metal imported graph backends" {
    var graph = Graph.init(std.testing.allocator);
    defer graph.deinit();

    var attrs: ml.graph.node.SliceAttrs = .{};
    attrs.runtime_limits = true;
    _ = try graph.addNode(.{
        .op = .{ .slice = attrs },
        .output_shape = Shape.init(.f32, &.{ 1, -1 }),
    });

    try validateRuntimeShapeBackend(&graph, .native);
    try validateRuntimeShapeBackend(&graph, .metal);
    try std.testing.expectError(error.UnsupportedDynamicOnnxGraphBackend, validateRuntimeShapeBackend(&graph, .wasm));
}
test "imported ConvTranspose selects only the native graph backend" {
    var graph = Graph.init(std.testing.allocator);
    defer graph.deinit();

    var attrs: ml.graph.node.ConvAttrs = .{};
    attrs.transposed = true;
    attrs.num_spatial = 2;
    _ = try graph.addNode(.{
        .op = .{ .conv_general = attrs },
        .output_shape = Shape.init(.f32, &.{ 1, 1, 4, 4 }),
    });

    try validateRuntimeShapeBackend(&graph, .native);
    try std.testing.expectError(error.UnsupportedOnnxGraphBackendOperation, validateRuntimeShapeBackend(&graph, .metal));
    try std.testing.expectError(error.UnsupportedOnnxGraphBackendOperation, validateRuntimeShapeBackend(&graph, .cuda));
    try std.testing.expectError(error.UnsupportedOnnxGraphBackendOperation, validateRuntimeShapeBackend(&graph, .wasm));
}

fn buildInputInfo(
    allocator: std.mem.Allocator,
    model: *onnx_graph.Model,
    graph: *const Graph,
    input_node_ids: []const NodeId,
) ![]TensorInfo {
    const onnx_model_graph = model.graph() orelse return error.InvalidOnnxGraph;
    const infos = try allocator.alloc(TensorInfo, input_node_ids.len);
    var initialized: usize = 0;
    errdefer {
        for (infos[0..initialized]) |info| {
            allocator.free(info.name);
            allocator.free(info.shape);
        }
        allocator.free(infos);
    }

    var out_idx: usize = 0;
    for (onnx_model_graph.inputs) |input| {
        if (input.name.len == 0 or !model.input_set.contains(input.name)) continue;
        const node_id = input_node_ids[out_idx];
        const shape = if (node_id != ml.graph.null_node) graph.node(node_id).output_shape else Shape.init(.f32, &.{});
        infos[out_idx] = try tensorInfoFromValueOrShape(
            allocator,
            input.name,
            input.type_proto,
            shape,
        );
        initialized += 1;
        out_idx += 1;
    }
    if (out_idx != infos.len) return error.InvalidOnnxGraph;
    return infos;
}

fn buildOutputInfo(
    allocator: std.mem.Allocator,
    model: *onnx_graph.Model,
    graph: *const Graph,
    output_node_ids: []const NodeId,
) ![]TensorInfo {
    const onnx_model_graph = model.graph() orelse return error.InvalidOnnxGraph;
    if (onnx_model_graph.outputs.len != output_node_ids.len) return error.InvalidOnnxGraph;

    const infos = try allocator.alloc(TensorInfo, output_node_ids.len);
    var initialized: usize = 0;
    errdefer {
        for (infos[0..initialized]) |info| {
            allocator.free(info.name);
            allocator.free(info.shape);
        }
        allocator.free(infos);
    }

    for (onnx_model_graph.outputs, output_node_ids, 0..) |output, node_id, i| {
        infos[i] = try tensorInfoFromValueOrShape(
            allocator,
            output.name,
            output.type_proto,
            graph.node(node_id).output_shape,
        );
        initialized += 1;
    }
    return infos;
}

fn buildStaticRuntimeInputs(
    allocator: std.mem.Allocator,
    model: *onnx_graph.Model,
    backend_ctx: *SharedBackendContext,
    parameter_names: [][]const u8,
    parameter_node_ids: []const NodeId,
    source: ?*const ImportedOnnxSession,
) ![]RuntimeInput {
    if (parameter_names.len != parameter_node_ids.len) return error.InvalidOnnxGraph;
    const runtime_inputs = try allocator.alloc(RuntimeInput, parameter_names.len);
    var initialized: usize = 0;
    errdefer {
        for (runtime_inputs[0..initialized]) |ri| backend_ctx.computeBackend().free(ri.value);
        allocator.free(runtime_inputs);
    }

    for (parameter_names, parameter_node_ids, 0..) |name, node_id, i| {
        if (source) |parent| {
            var retained: ?ops_mod.CT = null;
            for (parent.static_runtime_inputs, parent.static_names) |input, original_name| {
                if (!std.mem.eql(u8, name, original_name)) continue;
                retained = try parent.cb.retainImmutableTensor(input.value, allocator);
                break;
            }
            if (retained) |value| {
                runtime_inputs[i] = .{ .node_id = node_id, .value = value };
                initialized += 1;
                continue;
            }
            return error.MissingSharedOnnxInitializer;
        }
        const init = model.getInitializer(name) orelse return error.MissingWeight;
        if (try maybeDirectStaticTensor(allocator, model, init)) |tensor| {
            runtime_inputs[i] = .{
                .node_id = node_id,
                .value = try backend_ctx.importStaticTensor(allocator, tensor),
            };
            initialized += 1;
            continue;
        }

        const init_dtype = dtypeFromShape(init.shape);
        const init_shape = try shapeSliceFromOwnedShape(allocator, init.shape);
        defer allocator.free(init_shape);
        const values = try model.loadInitializerData(allocator, name);
        const imported = backend_ctx.importDenseStaticF32(
            allocator,
            init_dtype,
            init_shape,
            values,
        ) catch |err| {
            std.log.err("importDenseStaticF32 failed name={s} dtype={s} shape={any} err={}", .{
                name,
                @tagName(init_dtype),
                init_shape,
                err,
            });
            return err;
        };
        runtime_inputs[i] = .{
            .node_id = node_id,
            .value = imported,
        };
        initialized += 1;
    }

    return runtime_inputs;
}

const RemappedParameters = struct {
    names: [][]const u8,
    node_ids: []NodeId,
};

fn remapModelInputBindings(
    allocator: std.mem.Allocator,
    model: *onnx_graph.Model,
    remapped_parameter_names: [][]const u8,
    remapped_parameter_node_ids: []const NodeId,
) ![]NodeId {
    const onnx_model_graph = model.graph() orelse return error.InvalidOnnxGraph;
    const input_count = model.inputCount();
    const input_node_ids = try allocator.alloc(NodeId, input_count);
    var out_idx: usize = 0;

    for (onnx_model_graph.inputs) |input| {
        if (input.name.len == 0 or !model.input_set.contains(input.name)) continue;
        input_node_ids[out_idx] = ml.graph.null_node;

        for (remapped_parameter_names, remapped_parameter_node_ids) |remapped_name, remapped_node_id| {
            if (std.mem.eql(u8, remapped_name, input.name)) {
                input_node_ids[out_idx] = remapped_node_id;
                break;
            }
        }

        out_idx += 1;
    }

    if (out_idx != input_count) {
        allocator.free(input_node_ids);
        return error.InvalidOnnxGraph;
    }
    return input_node_ids;
}

fn filterStaticParameters(
    allocator: std.mem.Allocator,
    model: *onnx_graph.Model,
    parameter_names: [][]const u8,
    parameter_node_ids: []const NodeId,
) !RemappedParameters {
    if (parameter_names.len != parameter_node_ids.len) return error.InvalidOnnxGraph;

    var count: usize = 0;
    for (parameter_names) |name| {
        if (!model.input_set.contains(name)) count += 1;
    }

    const names = try allocator.alloc([]const u8, count);
    errdefer allocator.free(names);
    const node_ids = try allocator.alloc(NodeId, count);
    errdefer allocator.free(node_ids);

    var out_idx: usize = 0;
    for (parameter_names, parameter_node_ids) |name, node_id| {
        if (model.input_set.contains(name)) continue;
        names[out_idx] = name;
        node_ids[out_idx] = node_id;
        out_idx += 1;
    }

    return .{ .names = names, .node_ids = node_ids };
}

fn remapReachableParameters(
    allocator: std.mem.Allocator,
    parameter_names: [][]const u8,
    parameter_node_ids: []const NodeId,
    id_map: []const NodeId,
) !RemappedParameters {
    if (parameter_names.len != parameter_node_ids.len) return error.InvalidOnnxGraph;

    var count: usize = 0;
    for (parameter_node_ids) |old_id| {
        if (old_id < id_map.len and id_map[old_id] != ml.graph.null_node) count += 1;
    }

    const names = try allocator.alloc([]const u8, count);
    errdefer allocator.free(names);
    const node_ids = try allocator.alloc(NodeId, count);
    errdefer allocator.free(node_ids);

    var out_idx: usize = 0;
    for (parameter_names, parameter_node_ids) |name, old_id| {
        if (old_id >= id_map.len) continue;
        const new_id = id_map[old_id];
        if (new_id == ml.graph.null_node) continue;
        names[out_idx] = name;
        node_ids[out_idx] = new_id;
        out_idx += 1;
    }

    return .{ .names = names, .node_ids = node_ids };
}

fn maybeDirectStaticTensor(
    allocator: std.mem.Allocator,
    model: *onnx_graph.Model,
    init: onnx_graph.InitializerData,
) !?Tensor {
    const dtype = dtypeFromShape(init.shape);
    if (init.shape.rank() != 2) return null;
    if (dtype != .f32 and dtype != .f16 and dtype != .bf16) return null;

    if (init.tensor.raw_data.len > 0) {
        return .{
            .data = try allocator.dupe(u8, init.tensor.raw_data),
            .dtype = dtype,
            .shape = try shapeSliceFromOwnedShape(allocator, init.shape),
            .name = init.name,
            .allocator = allocator,
            .owns_data = true,
            .owns_shape = true,
        };
    }

    if (init.external) |external| {
        const base_dir = model.base_dir orelse return null;
        if (external.location.len == 0) return null;
        if (std.fs.path.isAbsolute(external.location)) return error.InvalidExternalPath;
        if (std.mem.indexOf(u8, external.location, "..") != null) return error.InvalidExternalPath;

        const full_path = try std.fs.path.join(allocator, &.{ base_dir, external.location });
        defer allocator.free(full_path);

        if (external.offset < 0) return error.InvalidExternalOffset;
        const offset: u64 = @intCast(external.offset);
        const len: usize = if (external.length < 0) blk: {
            const size = try c_file.fileSize(allocator, full_path);
            if (offset > size) return error.ExternalRegionOutOfBounds;
            break :blk @intCast(size - offset);
        } else @intCast(external.length);

        return .{
            .data = try c_file.readRegion(allocator, full_path, offset, len),
            .dtype = dtype,
            .shape = try shapeSliceFromOwnedShape(allocator, init.shape),
            .name = init.name,
            .allocator = allocator,
            .owns_data = true,
            .owns_shape = true,
        };
    }

    return null;
}

fn tensorInfoFromValueOrShape(
    allocator: std.mem.Allocator,
    name: []const u8,
    type_proto: ?onnx_graph.proto.TypeProto,
    fallback_shape: Shape,
) !TensorInfo {
    const owned_name = try allocator.dupe(u8, name);
    errdefer allocator.free(owned_name);

    if (type_proto) |tp| {
        if (tp.tensor_type) |tt| {
            const dtype = try onnxDataTypeToBackend(tt.elem_type);
            const dims = if (tt.shape) |shape| try dimsFromTensorShapeProto(allocator, shape) else try allocator.dupe(i64, &.{});
            return .{ .name = owned_name, .dtype = dtype, .shape = dims };
        }
    }

    return .{
        .name = owned_name,
        .dtype = dtypeFromShape(fallback_shape),
        .shape = try shapeSliceFromOwnedShape(allocator, fallback_shape),
    };
}

fn dimsFromTensorShapeProto(
    allocator: std.mem.Allocator,
    shape: onnx_graph.proto.TensorShapeProto,
) ![]i64 {
    const dims = try allocator.alloc(i64, shape.dims.len);
    for (shape.dims, 0..) |dim, i| {
        dims[i] = dim.dim_value orelse -1;
    }
    return dims;
}

fn shapeSliceFromOwnedShape(allocator: std.mem.Allocator, shape: Shape) ![]i64 {
    const rank = shape.rank();
    const dims = try allocator.alloc(i64, rank);
    for (0..rank) |i| dims[i] = shape.dim(@intCast(i));
    return dims;
}

fn shapeSliceFromShape(shape: Shape) []const i64 {
    return shape.dims[0..shape.rank()];
}

fn dtypeFromShape(shape: Shape) DType {
    return switch (shape.dtype) {
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

fn onnxDataTypeToBackend(dtype: onnx_graph.proto.DataType) !DType {
    return switch (dtype) {
        .float32 => .f32,
        .float16 => .f16,
        .bfloat16 => .bf16,
        .float64 => .f64,
        .int8 => .i8,
        .int16 => .i16,
        .int32 => .i32,
        .int64 => .i64,
        .uint8 => .u8,
        .bool_ => .bool_,
        else => error.UnsupportedDType,
    };
}

fn tensorFromNumericValues(
    allocator: std.mem.Allocator,
    name: []const u8,
    dtype: DType,
    shape: []const i64,
    values: []const f32,
) !Tensor {
    return switch (dtype) {
        .f32 => Tensor.initFloat32(allocator, name, shape, values),
        .f64 => blk: {
            const data = try allocator.alloc(f64, values.len);
            defer allocator.free(data);
            for (values, 0..) |value, i| data[i] = value;
            break :blk Tensor.initFloat64(allocator, name, shape, data);
        },
        .i64 => blk: {
            const data = try allocator.alloc(i64, values.len);
            defer allocator.free(data);
            for (values, 0..) |value, i| data[i] = @intFromFloat(@round(value));
            break :blk Tensor.initInt64(allocator, name, shape, data);
        },
        .i16 => blk: {
            const data = try allocator.alloc(i16, values.len);
            defer allocator.free(data);
            for (values, 0..) |value, i| data[i] = @intFromFloat(@round(value));
            break :blk Tensor.initInt16(allocator, name, shape, data);
        },
        .i8 => blk: {
            const data = try allocator.alloc(i8, values.len);
            defer allocator.free(data);
            for (values, 0..) |value, i| data[i] = @intFromFloat(@round(value));
            break :blk Tensor.initInt8(allocator, name, shape, data);
        },
        .i32 => initInt32Tensor(allocator, name, shape, values),
        .u8 => initU8Tensor(allocator, name, shape, values),
        .bool_ => initBoolTensor(allocator, name, shape, values),
        .f16 => initF16Tensor(allocator, name, shape, values),
        .bf16 => initBf16Tensor(allocator, name, shape, values),
    };
}

fn tensorFromExportedData(
    allocator: std.mem.Allocator,
    name: []const u8,
    shape: []const i64,
    exported: @import("../ops/ops.zig").ExportTensorData,
) !Tensor {
    errdefer freeExportedTensorData(allocator, exported);

    return switch (exported.payload) {
        .bytes => |bytes| .{
            .data = bytes,
            .dtype = exported.dtype,
            .shape = try allocator.dupe(i64, shape),
            .name = name,
            .allocator = allocator,
            .owns_data = true,
            .owns_shape = true,
        },
        .quantized_f32 => |quantized| blk: {
            const values = try allocator.alloc(f32, countElements(shape));
            errdefer allocator.free(values);
            try @import("../gguf/quant_codec.zig").dequantizeToFloat32(
                quantized.tensor_type,
                quantized.raw_bytes,
                values,
            );
            defer allocator.free(values);
            defer allocator.free(quantized.raw_bytes);
            defer allocator.free(quantized.shape);
            break :blk Tensor.initFloat32(allocator, name, shape, values);
        },
    };
}

fn freeExportedTensorData(
    allocator: std.mem.Allocator,
    exported: @import("../ops/ops.zig").ExportTensorData,
) void {
    switch (exported.payload) {
        .bytes => |bytes| allocator.free(bytes),
        .quantized_f32 => |quantized| {
            allocator.free(quantized.raw_bytes);
            allocator.free(quantized.shape);
        },
    }
}

fn countElements(shape: []const i64) usize {
    var total: usize = 1;
    for (shape) |dim| total *= @intCast(dim);
    return total;
}

fn initInt32Tensor(
    allocator: std.mem.Allocator,
    name: []const u8,
    shape: []const i64,
    values: []const f32,
) !Tensor {
    const bytes = try allocator.alloc(u8, values.len * @sizeOf(i32));
    errdefer allocator.free(bytes);
    for (values, 0..) |value, i| {
        const casted: i32 = @intFromFloat(@round(value));
        std.mem.writeInt(i32, bytes[i * 4 ..][0..4], casted, .little);
    }
    const owned_shape = try allocator.dupe(i64, shape);
    return .{
        .data = bytes,
        .dtype = .i32,
        .shape = owned_shape,
        .name = name,
        .allocator = allocator,
        .owns_data = true,
        .owns_shape = true,
    };
}

fn initU8Tensor(
    allocator: std.mem.Allocator,
    name: []const u8,
    shape: []const i64,
    values: []const f32,
) !Tensor {
    const bytes = try allocator.alloc(u8, values.len);
    errdefer allocator.free(bytes);
    for (values, 0..) |value, i| {
        const rounded = @round(value);
        const clamped = @max(@as(f32, 0), @min(@as(f32, 255), rounded));
        bytes[i] = @intFromFloat(clamped);
    }
    const owned_shape = try allocator.dupe(i64, shape);
    return .{
        .data = bytes,
        .dtype = .u8,
        .shape = owned_shape,
        .name = name,
        .allocator = allocator,
        .owns_data = true,
        .owns_shape = true,
    };
}

fn initBoolTensor(
    allocator: std.mem.Allocator,
    name: []const u8,
    shape: []const i64,
    values: []const f32,
) !Tensor {
    const bytes = try allocator.alloc(u8, values.len);
    errdefer allocator.free(bytes);
    for (values, 0..) |value, i| bytes[i] = if (@round(value) == 0) 0 else 1;
    const owned_shape = try allocator.dupe(i64, shape);
    return .{
        .data = bytes,
        .dtype = .bool_,
        .shape = owned_shape,
        .name = name,
        .allocator = allocator,
        .owns_data = true,
        .owns_shape = true,
    };
}

fn initF16Tensor(
    allocator: std.mem.Allocator,
    name: []const u8,
    shape: []const i64,
    values: []const f32,
) !Tensor {
    const bytes = try allocator.alloc(u8, values.len * @sizeOf(f16));
    errdefer allocator.free(bytes);
    for (values, 0..) |value, i| {
        const casted: f16 = @floatCast(value);
        const bits: u16 = @bitCast(casted);
        std.mem.writeInt(u16, bytes[i * 2 ..][0..2], bits, .little);
    }
    const owned_shape = try allocator.dupe(i64, shape);
    return .{
        .data = bytes,
        .dtype = .f16,
        .shape = owned_shape,
        .name = name,
        .allocator = allocator,
        .owns_data = true,
        .owns_shape = true,
    };
}

fn initBf16Tensor(
    allocator: std.mem.Allocator,
    name: []const u8,
    shape: []const i64,
    values: []const f32,
) !Tensor {
    const bytes = try allocator.alloc(u8, values.len * @sizeOf(u16));
    errdefer allocator.free(bytes);
    for (values, 0..) |value, i| {
        const bits: u32 = @bitCast(value);
        const bf16_bits: u16 = @truncate(bits >> 16);
        std.mem.writeInt(u16, bytes[i * 2 ..][0..2], bf16_bits, .little);
    }
    const owned_shape = try allocator.dupe(i64, shape);
    return .{
        .data = bytes,
        .dtype = .bf16,
        .shape = owned_shape,
        .name = name,
        .allocator = allocator,
        .owns_data = true,
        .owns_shape = true,
    };
}

fn freeTensorInfoList(allocator: std.mem.Allocator, infos: []TensorInfo) void {
    for (infos) |info| {
        allocator.free(info.name);
        allocator.free(info.shape);
    }
    allocator.free(infos);
}

test "imported onnx session runs simple add model" {
    const allocator = std.testing.allocator;

    var graph = Graph.init(allocator);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);
    const x = try builder.parameter("x", Shape.init(.f32, &.{4}));
    const bias = try builder.tensorConst(&.{ 0.1, 0.2, 0.3, 0.4 }, Shape.init(.f32, &.{4}));
    const sum = try builder.add(x, bias);
    try graph.markOutput(sum);

    const exported_bytes = try onnx_graph.exportGraph(allocator, &graph, .{});
    defer allocator.free(exported_bytes);
    var model = try onnx_graph.parse(allocator, exported_bytes);
    defer model.deinit();
    var snapshot = model.onnx;
    var abi_graph = snapshot.graph.?;
    var declared_inputs = [_]onnx_graph.proto.ValueInfoProto{ abi_graph.inputs[0], abi_graph.inputs[0] };
    declared_inputs[1].name = "unused";
    abi_graph.inputs = &declared_inputs;
    snapshot.graph = abi_graph;
    const model_bytes = try onnx_graph.serializeModel(allocator, &snapshot);
    defer allocator.free(model_bytes);

    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    try dir.dir.writeFile(std.testing.io, .{ .sub_path = "model.onnx", .data = model_bytes });

    const path = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", dir.sub_path[0..], "model.onnx" });
    defer allocator.free(path);

    var session = try createSessionWithOptions(allocator, path, .native, .{
        .graph_runtime_strategy = .partitioned,
    });
    defer session.close();

    const input = [_]f32{ 1.0, 2.0, 3.0, 4.0 };
    var input_tensor = try Tensor.initFloat32(allocator, "x", &.{4}, &input);
    defer input_tensor.deinit();

    var unused = try Tensor.initFloat32(allocator, "unused", &.{4}, &input);
    defer unused.deinit();
    var outputs = try session.run(&.{ input_tensor, unused }, allocator);
    defer {
        for (outputs) |*tensor| tensor.deinit();
        allocator.free(outputs);
    }

    try std.testing.expectEqual(@as(usize, 1), outputs.len);
    try std.testing.expectEqual(DType.f32, outputs[0].dtype);
    try std.testing.expectEqualSlices(i64, &.{4}, outputs[0].shape);
    const actual = outputs[0].asFloat32();
    const expected = [_]f32{ 1.1, 2.2, 3.3, 4.4 };
    for (expected, actual) |want, got| {
        try std.testing.expectApproxEqAbs(want, got, 1e-5);
    }
}

test "imported Reshape copies dynamic axes before inferring and transposing" {
    const allocator = std.testing.allocator;
    const proto = onnx_graph.proto;
    var input_dims = [_]proto.TensorShapeProto.Dimension{
        .{ .dim_param = "batch" }, .{ .dim_param = "sequence" }, .{ .dim_value = 12 },
    };
    var output_dims = [_]proto.TensorShapeProto.Dimension{
        .{ .dim_value = 2 },          .{ .dim_param = "batch" }, .{ .dim_value = 2 },
        .{ .dim_param = "sequence" }, .{ .dim_value = 3 },
    };
    var inputs = [_]proto.ValueInfoProto{.{
        .name = "x",
        .type_proto = .{ .tensor_type = .{
            .elem_type = .float32,
            .shape = .{ .dims = &input_dims },
        } },
    }};
    var outputs = [_]proto.ValueInfoProto{.{
        .name = "y",
        .type_proto = .{ .tensor_type = .{
            .elem_type = .float32,
            .shape = .{ .dims = &output_dims },
        } },
    }};
    var target_values = [_]i64{ 0, -1, 2, 2, 3 };
    for (&target_values) |*value| value.* = std.mem.nativeToLittle(i64, value.*);
    var target_dims = [_]i64{5};
    var initializers = [_]proto.TensorProto{.{
        .name = "target",
        .dims = &target_dims,
        .data_type = .int64,
        .raw_data = std.mem.sliceAsBytes(&target_values),
    }};
    var reshape_inputs = [_][]const u8{ "x", "target" };
    var reshape_outputs = [_][]const u8{"reshaped"};
    var transpose_inputs = [_][]const u8{"reshaped"};
    var transpose_outputs = [_][]const u8{"y"};
    var perm = [_]i64{ 2, 0, 3, 1, 4 };
    var transpose_attributes = [_]proto.AttributeProto{
        .{ .name = "perm", .ints = &perm, .attr_type = .ints },
    };
    var nodes = [_]proto.NodeProto{
        .{ .op_type = "Reshape", .inputs = &reshape_inputs, .outputs = &reshape_outputs },
        .{ .op_type = "Transpose", .inputs = &transpose_inputs, .outputs = &transpose_outputs, .attributes = &transpose_attributes },
    };
    var opsets = [_]proto.OpsetImport{.{ .domain = "", .version = 14 }};
    const model = proto.ModelProto{
        .ir_version = 7,
        .opset_import = &opsets,
        .graph = .{ .name = "dynamic_reshape", .nodes = &nodes, .inputs = &inputs, .outputs = &outputs, .initializers = &initializers },
    };
    const model_bytes = try onnx_graph.serializeModel(allocator, &model);
    defer allocator.free(model_bytes);
    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    try dir.dir.writeFile(std.testing.io, .{ .sub_path = "model.onnx", .data = model_bytes });
    const path = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", dir.sub_path[0..], "model.onnx" });
    defer allocator.free(path);
    var session = try createSessionWithOptions(allocator, path, .native, .{ .graph_runtime_strategy = .partitioned });
    defer session.close();
    for ([_][2]usize{ .{ 1, 5 }, .{ 2, 3 } }) |case| {
        const batch, const sequence = case;
        const count = batch * sequence * 12;
        const values = try allocator.alloc(f32, count);
        defer allocator.free(values);
        for (values, 0..) |*value, index| value.* = @floatFromInt(index);
        const shape = [_]i64{ @intCast(batch), @intCast(sequence), 12 };
        var input = try Tensor.initFloat32(allocator, "x", &shape, values);
        defer input.deinit();
        const actual = try session.run(&.{input}, allocator);
        defer {
            for (actual) |*tensor| tensor.deinit();
            allocator.free(actual);
        }
        try std.testing.expectEqualSlices(i64, &.{ 2, @intCast(batch), 2, @intCast(sequence), 3 }, actual[0].shape);
        for (0..2) |group| {
            for (0..batch) |b| {
                for (0..2) |head| {
                    for (0..sequence) |s| {
                        for (0..3) |d| {
                            const source = (b * sequence + s) * 12 + (group * 2 + head) * 3 + d;
                            const destination = (((group * batch + b) * 2 + head) * sequence + s) * 3 + d;
                            try std.testing.expectEqual(@as(f32, @floatFromInt(source)), actual[0].asFloat32()[destination]);
                        }
                    }
                }
            }
        }
    }
}

test "imported AveragePool preserves local windows and dynamic image dimensions" {
    const allocator = std.testing.allocator;
    const proto = onnx_graph.proto;
    var input_dims = [_]proto.TensorShapeProto.Dimension{
        .{ .dim_param = "batch" },
        .{ .dim_value = 2 },
        .{ .dim_param = "height" },
        .{ .dim_param = "width" },
    };
    var output_dims = [_]proto.TensorShapeProto.Dimension{
        .{ .dim_param = "batch" },
        .{ .dim_value = 2 },
        .{ .dim_param = "pooled_height" },
        .{ .dim_param = "pooled_width" },
    };
    var inputs = [_]proto.ValueInfoProto{.{
        .name = "x",
        .type_proto = .{ .tensor_type = .{
            .elem_type = .float32,
            .shape = .{ .dims = &input_dims },
        } },
    }};
    var outputs = [_]proto.ValueInfoProto{.{
        .name = "y",
        .type_proto = .{ .tensor_type = .{
            .elem_type = .float32,
            .shape = .{ .dims = &output_dims },
        } },
    }};
    var kernel = [_]i64{ 3, 2 };
    var strides = [_]i64{ 3, 2 };
    var attributes = [_]proto.AttributeProto{
        .{ .name = "kernel_shape", .ints = &kernel, .attr_type = .ints },
        .{ .name = "strides", .ints = &strides, .attr_type = .ints },
    };
    var node_inputs = [_][]const u8{"x"};
    var node_outputs = [_][]const u8{"y"};
    var nodes = [_]proto.NodeProto{.{
        .op_type = "AveragePool",
        .inputs = &node_inputs,
        .outputs = &node_outputs,
        .attributes = &attributes,
    }};
    var opsets = [_]proto.OpsetImport{.{ .domain = "", .version = 12 }};
    const model = proto.ModelProto{
        .ir_version = 7,
        .opset_import = &opsets,
        .graph = .{ .name = "local_average_pool", .nodes = &nodes, .inputs = &inputs, .outputs = &outputs },
    };
    const model_bytes = try onnx_graph.serializeModel(allocator, &model);
    defer allocator.free(model_bytes);
    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    try dir.dir.writeFile(std.testing.io, .{ .sub_path = "model.onnx", .data = model_bytes });
    const path = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", dir.sub_path[0..], "model.onnx" });
    defer allocator.free(path);
    var session = try createSessionWithOptions(allocator, path, .native, .{ .graph_runtime_strategy = .partitioned });
    defer session.close();

    const cases = .{
        .{ .shape = [_]i64{ 1, 2, 3, 4 }, .pooled_shape = [_]i64{ 1, 2, 1, 2 }, .expected = [_]f32{ 4.5, 6.5, 16.5, 18.5 } },
        .{ .shape = [_]i64{ 2, 2, 6, 6 }, .pooled_shape = [_]i64{ 2, 2, 2, 3 }, .expected = [_]f32{
            6.5,   8.5,   10.5,  24.5,  26.5,  28.5,
            42.5,  44.5,  46.5,  60.5,  62.5,  64.5,
            78.5,  80.5,  82.5,  96.5,  98.5,  100.5,
            114.5, 116.5, 118.5, 132.5, 134.5, 136.5,
        } },
    };
    inline for (cases) |case| {
        const input_shape = case.shape;
        const pooled_shape = case.pooled_shape;
        const expected = case.expected;
        const count = case.shape[0] * case.shape[1] * case.shape[2] * case.shape[3];
        var values: [count]f32 = undefined;
        for (&values, 0..) |*value, index| value.* = @floatFromInt(index);
        var input = try Tensor.initFloat32(allocator, "x", &input_shape, &values);
        defer input.deinit();
        var actual = try session.run(&.{input}, allocator);
        defer {
            for (actual) |*tensor| tensor.deinit();
            allocator.free(actual);
        }
        try std.testing.expectEqual(@as(usize, 1), actual.len);
        try std.testing.expectEqualSlices(i64, &pooled_shape, actual[0].shape);
        try std.testing.expectEqualSlices(f32, &expected, actual[0].asFloat32());
    }
}

test "imported AveragePool roundtrip preserves padding divisors alignment and dilation" {
    const allocator = std.testing.allocator;
    const PoolAttrs = ml.graph.node.AveragePoolAttrs;
    var excluded = PoolAttrs{ .num_spatial = 1 };
    excluded.kernel[0] = 3;
    excluded.strides[0] = 2;
    excluded.padding[0][0] = 1;
    var included = excluded;
    included.count_include_pad = true;
    var upper = excluded;
    upper.auto_pad = .same_upper;
    upper.padding[0] = .{ 0, 0 };
    var lower = upper;
    lower.auto_pad = .same_lower;
    var dilated = PoolAttrs{ .num_spatial = 3 };
    for (0..3) |axis| {
        dilated.kernel[axis] = 2;
        dilated.dilations[axis] = 2;
    }
    var volume = @as([27]f32, @splat(0));
    volume[0] = 8;
    volume[26] = 16;
    volume[13] = 999; // A dilated window must not sample its center.
    const Case = struct {
        attrs: PoolAttrs,
        shape: []const i64,
        values: []const f32,
        output_shape: []const i64,
        expected: []const f32,
    };
    const cases = [_]Case{
        .{ .attrs = excluded, .shape = &.{ 1, 1, 4 }, .values = &.{ 1, 3, 5, 7 }, .output_shape = &.{ 1, 1, 2 }, .expected = &.{ 2, 5 } },
        .{ .attrs = included, .shape = &.{ 1, 1, 4 }, .values = &.{ 1, 3, 5, 7 }, .output_shape = &.{ 1, 1, 2 }, .expected = &.{ 4.0 / 3.0, 5 } },
        .{ .attrs = upper, .shape = &.{ 1, 1, 4 }, .values = &.{ 1, 3, 5, 7 }, .output_shape = &.{ 1, 1, 2 }, .expected = &.{ 3, 6 } },
        .{ .attrs = lower, .shape = &.{ 1, 1, 4 }, .values = &.{ 1, 3, 5, 7 }, .output_shape = &.{ 1, 1, 2 }, .expected = &.{ 2, 5 } },
        .{ .attrs = dilated, .shape = &.{ 1, 1, 3, 3, 3 }, .values = &volume, .output_shape = &.{ 1, 1, 1, 1, 1 }, .expected = &.{3} },
    };
    for (cases) |case| {
        var graph = Graph.init(allocator);
        defer graph.deinit();
        var builder = ml.graph.Builder.init(&graph);
        const x = try builder.parameter("x", Shape.init(.f32, case.shape));
        const pooled = try graph.addNode(.{
            .op = .{ .average_pool = case.attrs },
            .output_shape = Shape.init(.f32, case.output_shape),
            .inputs = .{ x, ml.graph.null_node, ml.graph.null_node, ml.graph.null_node },
            .num_inputs = 1,
        });
        try graph.markOutput(pooled);
        const bytes = try onnx_graph.exportGraph(allocator, &graph, .{});
        defer allocator.free(bytes);
        var dir = std.testing.tmpDir(.{});
        defer dir.cleanup();
        try dir.dir.writeFile(std.testing.io, .{ .sub_path = "model.onnx", .data = bytes });
        const path = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", dir.sub_path[0..], "model.onnx" });
        defer allocator.free(path);
        var session = try createSessionWithOptions(allocator, path, .native, .{ .graph_runtime_strategy = .partitioned });
        defer session.close();
        var input = try Tensor.initFloat32(allocator, "x", case.shape, case.values);
        defer input.deinit();
        var actual = try session.run(&.{input}, allocator);
        defer {
            for (actual) |*tensor| tensor.deinit();
            allocator.free(actual);
        }
        try std.testing.expectEqual(@as(usize, 1), actual.len);
        try std.testing.expectEqualSlices(i64, case.output_shape, actual[0].shape);
        for (case.expected, actual[0].asFloat32()) |expected, value| try std.testing.expectApproxEqAbs(expected, value, 1e-6);
    }
}

test "imported onnx session matches dynamic quantized integer matmul semantics" {
    const allocator = std.testing.allocator;
    const proto = onnx_graph.proto;

    var matrix_dims = [_]proto.TensorShapeProto.Dimension{
        .{ .dim_value = 2 },
        .{ .dim_value = 2 },
    };
    var input_infos = [_]proto.ValueInfoProto{
        .{
            .name = "x",
            .type_proto = .{ .tensor_type = .{
                .elem_type = .float32,
                .shape = .{ .dims = &matrix_dims },
            } },
        },
    };
    var output_infos = [_]proto.ValueInfoProto{
        .{
            .name = "q",
            .type_proto = .{ .tensor_type = .{
                .elem_type = .uint8,
                .shape = .{ .dims = &matrix_dims },
            } },
        },
        .{
            .name = "scale",
            .type_proto = .{ .tensor_type = .{
                .elem_type = .float32,
                .shape = .{ .dims = &.{} },
            } },
        },
        .{
            .name = "zp",
            .type_proto = .{ .tensor_type = .{
                .elem_type = .uint8,
                .shape = .{ .dims = &.{} },
            } },
        },
        .{
            .name = "out",
            .type_proto = .{ .tensor_type = .{
                .elem_type = .float32,
                .shape = .{ .dims = &matrix_dims },
            } },
        },
    };

    const weight_values = [_]i8{ 1, -2, 3, 4 };
    const weight_zero_points = [_]i8{ 0, 0 };
    var weight_dims = [_]i64{ 2, 2 };
    var zero_point_dims = [_]i64{2};
    var initializers = [_]proto.TensorProto{
        .{
            .name = "w",
            .dims = &weight_dims,
            .data_type = .int8,
            .raw_data = std.mem.sliceAsBytes(&weight_values),
        },
        .{
            .name = "wzp",
            .dims = &zero_point_dims,
            .data_type = .int8,
            .raw_data = std.mem.sliceAsBytes(&weight_zero_points),
        },
    };

    var dql_inputs = [_][]const u8{"x"};
    var dql_outputs = [_][]const u8{ "q", "scale", "zp" };
    var mm_inputs = [_][]const u8{ "q", "w", "zp", "wzp" };
    var mm_outputs = [_][]const u8{"mm"};
    var cast_inputs = [_][]const u8{"mm"};
    var cast_outputs = [_][]const u8{"out"};
    var cast_attrs = [_]proto.AttributeProto{
        .{ .name = "to", .i = @backingInt(proto.DataType.float32), .attr_type = .int },
    };
    var nodes = [_]proto.NodeProto{
        .{ .op_type = "DynamicQuantizeLinear", .inputs = &dql_inputs, .outputs = &dql_outputs },
        .{ .op_type = "MatMulInteger", .inputs = &mm_inputs, .outputs = &mm_outputs },
        .{ .op_type = "Cast", .inputs = &cast_inputs, .outputs = &cast_outputs, .attributes = &cast_attrs },
    };
    var opsets = [_]proto.OpsetImport{.{ .domain = "", .version = 12 }};
    const graph_proto = proto.GraphProto{
        .name = "dynamic_quantized_matmul",
        .nodes = &nodes,
        .initializers = &initializers,
        .inputs = &input_infos,
        .outputs = &output_infos,
    };
    const model = proto.ModelProto{
        .ir_version = 7,
        .opset_import = &opsets,
        .graph = graph_proto,
    };
    const model_bytes = try onnx_graph.serializeModel(allocator, &model);
    defer allocator.free(model_bytes);

    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    try dir.dir.writeFile(std.testing.io, .{ .sub_path = "model.onnx", .data = model_bytes });
    const path = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", dir.sub_path[0..], "model.onnx" });
    defer allocator.free(path);

    var session = try createSessionWithOptions(allocator, path, .native, .{
        .graph_runtime_strategy = .partitioned,
    });
    defer session.close();

    const input = [_]f32{ -2.0, -1.0, 0.0, 1.0 };
    var input_tensor = try Tensor.initFloat32(allocator, "x", &.{ 2, 2 }, &input);
    defer input_tensor.deinit();

    var outputs = try session.run(&.{input_tensor}, allocator);
    defer {
        for (outputs) |*tensor| tensor.deinit();
        allocator.free(outputs);
    }

    try std.testing.expectEqual(@as(usize, 4), outputs.len);
    // Graph outputs preserve their declared integer storage dtype, matching
    // ONNX's DynamicQuantizeLinear contract and the typed intermediate values.
    try std.testing.expectEqual(DType.u8, outputs[0].dtype);
    try std.testing.expectEqualSlices(u8, &.{ 0, 85, 170, 255 }, outputs[0].data);
    try std.testing.expectEqual(DType.f32, outputs[1].dtype);
    try std.testing.expectApproxEqAbs(@as(f32, 0.011764706), outputs[1].asFloat32()[0], 1e-8);
    try std.testing.expectEqual(DType.u8, outputs[2].dtype);
    try std.testing.expectEqualSlices(u8, &.{170}, outputs[2].data);
    try std.testing.expectEqual(DType.f32, outputs[3].dtype);
    try std.testing.expectEqualSlices(f32, &.{ -425.0, 0.0, 255.0, 340.0 }, outputs[3].asFloat32());
}

test "ONNX artifact inspection includes and validates external tensor files" {
    const allocator = std.testing.allocator;

    var graph = Graph.init(allocator);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);
    const weight = try builder.parameter("weight", Shape.init(.f32, &.{4}));
    try graph.markOutput(weight);
    const values = [_]f32{ 1, 2, 3, 4 };
    const initializer = onnx_graph.ParameterInitializer{
        .name = "weight",
        .shape = Shape.init(.f32, &.{4}),
        .data = .{ .raw_bytes = std.mem.asBytes(&values) },
    };
    var exported = try onnx_graph.exportGraphWithExternalData(
        allocator,
        &graph,
        .{ .parameter_initializers = &.{initializer} },
        "model.data",
    );
    defer exported.deinit(allocator);

    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    try dir.dir.writeFile(std.testing.io, .{
        .sub_path = "model.onnx",
        .data = exported.model_bytes,
    });
    try dir.dir.writeFile(std.testing.io, .{
        .sub_path = "model.data",
        .data = exported.external_data.?.bytes,
    });
    const path = try std.fs.path.join(
        allocator,
        &.{ ".zig-cache", "tmp", dir.sub_path[0..], "model.onnx" },
    );
    defer allocator.free(path);

    var artifacts = try inspectArtifactSet(allocator, path);
    defer artifacts.deinit();
    try std.testing.expectEqual(@as(usize, 1), artifacts.external_paths.len);
    try std.testing.expectEqual(
        exported.model_bytes.len + exported.external_data.?.bytes.len,
        artifacts.encoded_bytes,
    );

    try dir.dir.deleteFile(std.testing.io, "model.data");
    try std.testing.expectError(error.FileNotFound, inspectArtifactSet(allocator, path));
}

test "imported onnx l2 normalize tail plans to metal without device" {
    const allocator = std.testing.allocator;

    var graph = Graph.init(allocator);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);

    const batch: i64 = 4;
    const dim: i64 = 768;
    const x = try builder.parameter("projected", Shape.init(.f32, &.{ batch, dim }));
    const squared = try builder.mul(x, x);
    const sum_sq = try builder.reduceSum(squared, &.{1});
    const norm = try builder.sqrt(sum_sq);
    const norm_bc = try graph.addNode(.{
        .op = .{ .broadcast_in_dim = .{
            .target_shape = Shape.init(.f32, &.{ batch, dim }),
            .broadcast_axes = .{ 0, 1, 0, 0, 0, 0, 0, 0 },
            .num_axes = 2,
        } },
        .output_shape = Shape.init(.f32, &.{ batch, dim }),
        .inputs = .{ norm, ml.graph.null_node, ml.graph.null_node, ml.graph.null_node },
        .num_inputs = 1,
    });
    const normalized = try builder.div(x, norm_bc);
    try graph.markOutput(normalized);

    const model_bytes = try onnx_graph.exportGraph(allocator, &graph, .{});
    defer allocator.free(model_bytes);

    var model = try onnx_graph.parseLazyAsModel(allocator, model_bytes);
    defer model.deinit();
    var converted = try model.convertToGraph(allocator);
    defer converted.deinit(allocator);
    var optimized = try Pipeline.default.run(allocator, &converted.graph);
    defer optimized.deinit();

    try expectNormalizeTailPlansForMetal(&optimized.graph);
}

fn defaultClipclapTextModelPath(allocator: std.mem.Allocator) ![]u8 {
    return defaultClipclapModelPath(allocator, "text_model.onnx");
}

fn defaultClipclapModelPath(allocator: std.mem.Allocator, file_name: []const u8) ![]u8 {
    if (std.c.getenv("ANTFLY_TEST_CLIPCLAP_ONNX_DIR")) |dir| {
        return std.fs.path.join(allocator, &.{ std.mem.span(dir), file_name });
    }
    const home = std.c.getenv("HOME") orelse return error.SkipZigTest;
    const preferred = try std.fs.path.join(allocator, &.{ std.mem.span(home), ".antfly", "models", "antflydb", "clipclap", file_name });
    if (c_file.fileExists(allocator, preferred)) return preferred;
    allocator.free(preferred);
    return std.fs.path.join(allocator, &.{ std.mem.span(home), ".antfly", "inference", "models", "antflydb", "clipclap", file_name });
}

test "imported onnx session executes clipclap text model natively with padded mask" {
    try expectClipclapTextExecution(.native, null);
}

test "imported onnx session executes clipclap text model on metal across graph runtimes" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!@import("metal_runtime.zig").metalDeviceAvailable()) return error.SkipZigTest;
    for ([_]graph_runtime_mod.Strategy{ .interpreter, .partitioned, .compiled_preferred }) |strategy| {
        try expectClipclapTextExecution(.metal, strategy);
    }
}

fn expectClipclapTextExecution(requested_backend: BackendType, strategy: ?graph_runtime_mod.Strategy) !void {
    const allocator = std.testing.allocator;
    const path = try defaultClipclapTextModelPath(allocator);
    defer allocator.free(path);

    var session = createSessionWithOptions(allocator, path, requested_backend, .{ .graph_runtime_strategy = strategy }) catch |err| switch (err) {
        error.FileNotFound => return error.SkipZigTest,
        else => return err,
    };
    defer session.close();

    const seq_len = 77;
    var input_ids_data: [seq_len]i64 = undefined;
    var attention_mask_data: [seq_len]i64 = undefined;
    @memset(input_ids_data[0..], 1);
    // Pool from a nonzero EOS position, as real CLIP-tokenized text does.
    input_ids_data[0] = 49406;
    input_ids_data[8] = 49407;
    @memset(attention_mask_data[0..], 0);
    @memset(attention_mask_data[0..9], 1);

    var compact_ids: [seq_len]i32 = undefined;
    for (input_ids_data, &compact_ids) |id, *compact| compact.* = @intCast(id);
    // The embedding pipeline uploads compact i32 token IDs. Exercise its
    // identity cast and cross-partition lifetime, alongside native i64 input.
    var input_ids = if (requested_backend == .metal)
        try Tensor.initInt32(allocator, "input_ids", &.{ 1, seq_len }, &compact_ids)
    else
        try Tensor.initInt64(allocator, "input_ids", &.{ 1, seq_len }, &input_ids_data);
    defer input_ids.deinit();
    var attention_mask = try Tensor.initInt64(allocator, "attention_mask", &.{ 1, seq_len }, &attention_mask_data);
    defer attention_mask.deinit();

    var outputs = try session.run(&.{ input_ids, attention_mask }, allocator);
    defer {
        for (outputs) |*tensor| tensor.deinit();
        allocator.free(outputs);
    }

    try std.testing.expect(outputs.len > 0);
    if (std.c.getenv("ANTFLY_TEST_DUMP_CLIPCLAP_OUTPUTS") != null) {
        std.debug.print("clipclap outputs={d}\n", .{outputs.len});
        for (outputs, 0..) |*output, idx| {
            const values = output.asFloat32();
            std.debug.print("  output[{d}] name={s} shape={any} first=", .{ idx, output.name, output.shape });
            for (values[0..@min(values.len, 8)]) |value| {
                std.debug.print("{d:.6} ", .{value});
            }
            std.debug.print("\n", .{});
        }
    }
    try std.testing.expectEqual(DType.f32, outputs[0].dtype);
    try std.testing.expect(outputs[0].elementCount() > 0);
    for (outputs[0].asFloat32()) |value| {
        try std.testing.expect(std.math.isFinite(value));
    }
}

test "imported onnx session loads clipclap audio model natively with cubic resize" {
    const allocator = std.testing.allocator;
    const path = try defaultClipclapModelPath(allocator, "audio_model.onnx");
    defer allocator.free(path);

    var session = createSession(allocator, path, .native) catch |err| switch (err) {
        error.FileNotFound => return error.SkipZigTest,
        else => return err,
    };
    defer session.close();

    const imported: *ImportedOnnxSession = @ptrCast(@alignCast(session.ptr));
    var found_input_features = false;
    for (session.inputInfo(), 0..) |info, i| {
        if (!std.mem.eql(u8, info.name, "input_features")) continue;
        found_input_features = true;
        const input_shape = imported.program.graph.node(imported.input_node_ids[i]).output_shape;
        try std.testing.expect(input_shape.rank() >= 3);
        try std.testing.expectEqual(@as(i64, clipclap_audio_input_frames), input_shape.dim(2));
    }
    try std.testing.expect(found_input_features);
}

test "imported onnx session plans local clipclap text projections and transposes for metal without device" {
    try expectClipclapOnnxPlansForMetal("text_model.onnx", .{
        .require_sdpa = true,
        .require_transpose = true,
        .require_matmul = true,
    });
    try expectClipclapOnnxPlansForMetal("text_projection.onnx", .{
        .require_matmul = true,
    });
}

test "imported onnx session plans local clipclap visual model and projection for metal without device" {
    try expectClipclapOnnxPlansForMetal("visual_model.onnx", .{
        .require_transpose = true,
        .require_matmul = true,
    });
    try expectClipclapOnnxPlansForMetal("visual_projection.onnx", .{
        .require_matmul = true,
    });
}

test "imported onnx session plans local clipclap audio model and projection for metal without device" {
    try expectClipclapOnnxPlansForMetal("audio_model.onnx", .{
        .require_transpose = true,
        .require_matmul = true,
    });
    try expectClipclapOnnxPlansForMetal("audio_projection.onnx", .{
        .require_matmul = true,
    });
}

const ClipclapPlannerExpectations = struct {
    require_sdpa: bool = false,
    require_transpose: bool = false,
    require_matmul: bool = false,
};

fn expectNormalizeTailPlansForMetal(graph: *const Graph) !void {
    const allocator = std.testing.allocator;
    const seeds = try graph_partition.allocTensorDescriptorSeeds(allocator, graph);
    defer allocator.free(seeds);
    try seedF32GraphResidencyForMetal(seeds, graph);

    const caps = [_]graph_partition.Capability{
        .{ .backend = .metal, .priority = 20, .decide = &metal_capabilities.decideMetalEagerGraph },
        .{ .backend = .native, .priority = 0, .decide = &graph_partition.decideNative },
    };
    var diagnostics = graph_partition.CapabilityDiagnostics{};
    var plan = try graph_partition.partitionWithOptions(allocator, graph, &caps, .{
        .tensor_descs = seeds,
        .diagnostics = &diagnostics,
    });
    defer plan.deinit();

    var reduce_count: usize = 0;
    var metal_reduce_count: usize = 0;
    var unary_count: usize = 0;
    var metal_unary_count: usize = 0;
    var broadcast_count: usize = 0;
    var metal_broadcast_count: usize = 0;
    var normalize_binary_count: usize = 0;
    var metal_normalize_binary_count: usize = 0;

    for (0..graph.nodeCount()) |idx| {
        const node_id: NodeId = @intCast(idx);
        const node = graph.node(node_id);
        const planned_backend = plan.partitions[plan.node_assignment[node_id]].backend;
        switch (node.op) {
            .reduce_sum, .reduce_mean, .reduce_max => {
                if (!clipclapNodeIsLastDimF32Reduce(graph, node_id)) continue;
                reduce_count += 1;
                if (planned_backend == .metal) metal_reduce_count += 1;
            },
            .sqrt, .rsqrt => {
                if (node.output_shape.dtype != .f32) continue;
                unary_count += 1;
                if (planned_backend == .metal) metal_unary_count += 1;
            },
            .broadcast_in_dim => {
                if (!clipclapNodeIsNormalizeBroadcastTail(graph, node_id)) continue;
                broadcast_count += 1;
                if (planned_backend == .metal) metal_broadcast_count += 1;
            },
            .mul, .div => {
                if (!clipclapNodeConsumesNormalizeBroadcastTail(graph, node_id)) continue;
                normalize_binary_count += 1;
                if (planned_backend == .metal) metal_normalize_binary_count += 1;
            },
            else => {},
        }
    }

    if (reduce_count != metal_reduce_count or
        unary_count != metal_unary_count or
        broadcast_count != metal_broadcast_count or
        normalize_binary_count != metal_normalize_binary_count)
    {
        dumpNormalizePlannerMismatches(graph, &plan, seeds);
    }
    try std.testing.expect(reduce_count > 0);
    try std.testing.expect(unary_count > 0);
    try std.testing.expect(broadcast_count > 0);
    try std.testing.expect(normalize_binary_count > 0);
    try std.testing.expectEqual(reduce_count, metal_reduce_count);
    try std.testing.expectEqual(unary_count, metal_unary_count);
    try std.testing.expectEqual(broadcast_count, metal_broadcast_count);
    try std.testing.expectEqual(normalize_binary_count, metal_normalize_binary_count);
    try std.testing.expectEqual(@as(usize, 0), diagnostics.count(.missing_quant_kernel));
}

fn expectClipclapOnnxPlansForMetal(file_name: []const u8, expectations: ClipclapPlannerExpectations) !void {
    const allocator = std.testing.allocator;
    const path = try defaultClipclapModelPath(allocator, file_name);
    defer allocator.free(path);

    const model_bytes = c_file.readFileMax(allocator, path, max_onnx_model_bytes) catch |err| switch (err) {
        error.FileNotFound => return error.SkipZigTest,
        else => return err,
    };
    defer allocator.free(model_bytes);

    const model_dir = std.fs.path.dirname(path) orelse ".";
    var model = try onnx_graph.parseLazyAsModelWithBaseDir(allocator, model_bytes, model_dir);
    defer model.deinit();

    var clipclap_dim_overrides: onnx_graph.DimOverrides = .empty;
    defer clipclap_dim_overrides.deinit(allocator);
    var use_clipclap_dim_overrides = false;
    if (shouldSpecializeClipclapAudioInputTime(path)) {
        try clipclap_dim_overrides.put(allocator, "time", clipclap_audio_input_frames);
        use_clipclap_dim_overrides = true;
    }
    if (shouldSpecializeClipclapTextInputShape(path)) {
        try clipclap_dim_overrides.put(allocator, "batch_size", clipclap_text_batch);
        try clipclap_dim_overrides.put(allocator, "sequence_length", clipclap_text_sequence_length);
        use_clipclap_dim_overrides = true;
    }
    const dim_overrides: ?*const onnx_graph.DimOverrides = if (use_clipclap_dim_overrides) &clipclap_dim_overrides else null;

    var converted = try model.convertToGraphWithDims(allocator, dim_overrides);
    defer converted.deinit(allocator);
    var optimized = try Pipeline.default.run(allocator, &converted.graph);
    defer optimized.deinit();
    specializeGraphDynamicDimsForPlanner(&optimized.graph, 1);

    const graph = &optimized.graph;
    const seeds = try graph_partition.allocTensorDescriptorSeeds(allocator, graph);
    defer allocator.free(seeds);
    try seedF32GraphResidencyForMetal(seeds, graph);

    const caps = [_]graph_partition.Capability{
        .{ .backend = .metal, .priority = 20, .decide = &metal_capabilities.decideMetalEagerGraph },
        .{ .backend = .native, .priority = 0, .decide = &graph_partition.decideNative },
    };
    var diagnostics = graph_partition.CapabilityDiagnostics{};
    var plan = try graph_partition.partitionWithOptions(allocator, graph, &caps, .{
        .tensor_descs = seeds,
        .diagnostics = &diagnostics,
    });
    defer plan.deinit();

    var sdpa_count: usize = 0;
    var metal_sdpa_count: usize = 0;
    var transpose_count: usize = 0;
    var metal_transpose_count: usize = 0;
    var matmul_count: usize = 0;
    var metal_matmul_count: usize = 0;
    var equal_binary_count: usize = 0;
    var metal_equal_binary_count: usize = 0;
    for (0..graph.nodeCount()) |idx| {
        const node_id: NodeId = @intCast(idx);
        const node = graph.node(node_id);
        const planned_backend = plan.partitions[plan.node_assignment[node_id]].backend;
        switch (node.op) {
            .fused_sdpa => {
                sdpa_count += 1;
                if (planned_backend == .metal) {
                    metal_sdpa_count += 1;
                    try std.testing.expectEqual(operator_plan.Operator.attention_flash, plan.operatorPlanForNode(node_id).?.operator());
                }
            },
            .transpose => {
                if (node.output_shape.dtype != .f32) continue;
                transpose_count += 1;
                if (planned_backend == .metal) metal_transpose_count += 1;
            },
            .dot_general, .fused_linear, .fused_linear_no_bias => {
                if (node.output_shape.dtype != .f32) continue;
                matmul_count += 1;
                if (planned_backend == .metal) metal_matmul_count += 1;
            },
            .add, .mul, .sub, .div, .less_than, .fused_elem_add, .fused_elem_multiply => {
                if (!clipclapNodeHasEqualSizeF32BinaryInputs(graph, node_id)) continue;
                equal_binary_count += 1;
                if (planned_backend == .metal) metal_equal_binary_count += 1;
            },
            else => {},
        }
    }

    if (expectations.require_sdpa) try std.testing.expect(sdpa_count > 0);
    if (expectations.require_transpose) try std.testing.expect(transpose_count > 0);
    if (expectations.require_matmul) try std.testing.expect(matmul_count > 0);
    if (sdpa_count != metal_sdpa_count or
        transpose_count != metal_transpose_count or
        matmul_count != metal_matmul_count or
        equal_binary_count != metal_equal_binary_count)
    {
        dumpClipclapPlannerMismatches(file_name, graph, &plan, seeds);
    }
    try std.testing.expectEqual(sdpa_count, metal_sdpa_count);
    try std.testing.expectEqual(transpose_count, metal_transpose_count);
    try std.testing.expectEqual(matmul_count, metal_matmul_count);
    try std.testing.expectEqual(equal_binary_count, metal_equal_binary_count);
    try std.testing.expectEqual(@as(usize, 0), diagnostics.count(.missing_quant_kernel));
}

fn clipclapNodeHasEqualSizeF32BinaryInputs(graph: *const Graph, node_id: NodeId) bool {
    const node = graph.node(node_id);
    if (node.output_shape.dtype != .f32) return false;
    const inputs = node.getInputs();
    if (inputs.len < 2) return false;
    if (inputs[0] == ml.graph.null_node or inputs[1] == ml.graph.null_node) return false;
    const lhs_shape = graph.node(inputs[0]).output_shape;
    const rhs_shape = graph.node(inputs[1]).output_shape;
    if (lhs_shape.dtype != .f32 or rhs_shape.dtype != .f32) return false;
    const lhs_elems = lhs_shape.numElements() orelse return false;
    const rhs_elems = rhs_shape.numElements() orelse return false;
    const out_elems = node.output_shape.numElements() orelse return false;
    return lhs_elems > 0 and lhs_elems == rhs_elems and lhs_elems == out_elems;
}

fn clipclapNodeIsLastDimF32Reduce(graph: *const Graph, node_id: NodeId) bool {
    const node = graph.node(node_id);
    if (node.output_shape.dtype != .f32) return false;
    const attrs = switch (node.op) {
        .reduce_sum, .reduce_mean, .reduce_max => |attrs| attrs,
        else => return false,
    };
    if (attrs.num_axes != 1) return false;
    const inputs = node.getInputs();
    if (inputs.len == 0 or inputs[0] == ml.graph.null_node) return false;
    const input_shape = graph.node(inputs[0]).output_shape;
    if (input_shape.dtype != .f32 or input_shape.rank() == 0) return false;
    return attrs.axes[0] == input_shape.rank() - 1;
}

fn clipclapNodeIsNormalizeBroadcastTail(graph: *const Graph, node_id: NodeId) bool {
    const node = graph.node(node_id);
    const attrs = switch (node.op) {
        .broadcast_in_dim => |attrs| attrs,
        else => return false,
    };
    if (node.output_shape.dtype != .f32) return false;
    const inputs = node.getInputs();
    if (inputs.len == 0 or inputs[0] == ml.graph.null_node) return false;
    const input_shape = graph.node(inputs[0]).output_shape;
    const output_shape = node.output_shape;
    const rank = output_shape.rank();
    if (rank == 0 or input_shape.rank() != rank or attrs.num_axes != rank) return false;
    for (0..rank) |axis| {
        if (attrs.broadcast_axes[axis] != axis) return false;
        const in_dim = input_shape.dim(@intCast(axis));
        const out_dim = output_shape.dim(@intCast(axis));
        if (in_dim <= 0 or out_dim <= 0) return false;
        if (axis == rank - 1) {
            if (in_dim != 1 or out_dim <= 1) return false;
        } else if (in_dim != out_dim) return false;
    }
    return true;
}

fn clipclapNodeConsumesNormalizeBroadcastTail(graph: *const Graph, node_id: NodeId) bool {
    const node = graph.node(node_id);
    if (node.output_shape.dtype != .f32) return false;
    switch (node.op) {
        .mul, .div => {},
        else => return false,
    }
    for (node.getInputs()) |input_id| {
        if (input_id == ml.graph.null_node) continue;
        if (clipclapNodeIsNormalizeBroadcastTail(graph, input_id)) return true;
    }
    return false;
}

fn dumpClipclapPlannerMismatches(
    file_name: []const u8,
    graph: *const Graph,
    plan: *const graph_partition.PartitionPlan,
    seeds: []const ?graph_contracts.TensorDesc,
) void {
    std.debug.print("clipclap metal planner mismatches for {s}\n", .{file_name});
    var printed: usize = 0;
    for (0..graph.nodeCount()) |idx| {
        const node_id: NodeId = @intCast(idx);
        const node = graph.node(node_id);
        const planned_backend = plan.partitions[plan.node_assignment[node_id]].backend;
        const interesting = switch (node.op) {
            .fused_sdpa, .transpose, .dot_general, .fused_linear, .fused_linear_no_bias => node.output_shape.dtype == .f32,
            .reduce_sum, .reduce_mean, .reduce_max => clipclapNodeIsLastDimF32Reduce(graph, node_id),
            .sqrt, .rsqrt => node.output_shape.dtype == .f32,
            .broadcast_in_dim => clipclapNodeIsNormalizeBroadcastTail(graph, node_id),
            .mul, .div => clipclapNodeConsumesNormalizeBroadcastTail(graph, node_id) or clipclapNodeHasEqualSizeF32BinaryInputs(graph, node_id),
            .add, .sub, .less_than, .fused_elem_add, .fused_elem_multiply => clipclapNodeHasEqualSizeF32BinaryInputs(graph, node_id),
            else => false,
        };
        if (!interesting or planned_backend == .metal) continue;
        const decision = metal_capabilities.decideMetalEagerGraph(.{
            .graph = graph,
            .node_id = node_id,
            .op = node.op,
            .tensor_descs = seeds,
        });
        std.debug.print("  node={d} op={s} backend={s} reason={s} shape={any}\n", .{
            node_id,
            @tagName(std.meta.activeTag(node.op)),
            @tagName(planned_backend),
            @tagName(decision.reason),
            node.output_shape,
        });
        printed += 1;
        if (printed >= 16) break;
    }
}

fn dumpNormalizePlannerMismatches(
    graph: *const Graph,
    plan: *const graph_partition.PartitionPlan,
    seeds: []const ?graph_contracts.TensorDesc,
) void {
    std.debug.print("normalize tail metal planner mismatches\n", .{});
    var printed: usize = 0;
    for (0..graph.nodeCount()) |idx| {
        const node_id: NodeId = @intCast(idx);
        const node = graph.node(node_id);
        const planned_backend = plan.partitions[plan.node_assignment[node_id]].backend;
        const interesting = switch (node.op) {
            .reduce_sum, .reduce_mean, .reduce_max => clipclapNodeIsLastDimF32Reduce(graph, node_id),
            .sqrt, .rsqrt => node.output_shape.dtype == .f32,
            .broadcast_in_dim => clipclapNodeIsNormalizeBroadcastTail(graph, node_id),
            .mul, .div => clipclapNodeConsumesNormalizeBroadcastTail(graph, node_id),
            else => false,
        };
        if (!interesting or planned_backend == .metal) continue;
        const decision = metal_capabilities.decideMetalEagerGraph(.{
            .graph = graph,
            .node_id = node_id,
            .op = node.op,
            .tensor_descs = seeds,
        });
        std.debug.print("  node={d} op={s} backend={s} reason={s} shape={any}\n", .{
            node_id,
            @tagName(std.meta.activeTag(node.op)),
            @tagName(planned_backend),
            @tagName(decision.reason),
            node.output_shape,
        });
        printed += 1;
        if (printed >= 16) break;
    }
}

fn seedF32GraphResidencyForMetal(seeds: []?graph_contracts.TensorDesc, graph: *const Graph) !void {
    for (0..graph.nodeCount()) |idx| {
        const node_id: NodeId = @intCast(idx);
        const shape = graph.node(node_id).output_shape;
        if (shape.dtype != .f32) continue;
        var desc = graph_contracts.TensorDesc.init(shape, .metal_buffer);
        desc.resident_backend = .metal;
        try graph_partition.seedTensorDescriptor(seeds, graph, node_id, desc);
    }
}

fn specializeGraphDynamicDimsForPlanner(graph: *Graph, fallback_dim: i64) void {
    std.debug.assert(fallback_dim > 0);
    for (0..graph.nodeCount()) |idx| {
        const node_id: NodeId = @intCast(idx);
        const node = graph.nodeMut(node_id);
        specializeShapeDynamicDimsForPlanner(&node.output_shape, fallback_dim);
        switch (node.op) {
            .reshape => |*attrs| specializeShapeDynamicDimsForPlanner(&attrs.new_shape, fallback_dim),
            .broadcast_in_dim => |*attrs| specializeShapeDynamicDimsForPlanner(&attrs.target_shape, fallback_dim),
            .fused_sdpa => |*attrs| specializeSdpaAttrsForPlanner(graph, node_id, attrs),
            else => {},
        }
    }
    reconcileDotGeneralPlannerShapes(graph);
}

fn specializeShapeDynamicDimsForPlanner(shape: *ml.graph.Shape, fallback_dim: i64) void {
    for (0..shape.rank()) |axis| {
        if (shape.dims[axis] >= 0) continue;
        shape.dims[axis] = if (shape.bounds[axis] > 0) @min(shape.bounds[axis], fallback_dim) else fallback_dim;
        shape.bounds[axis] = 0;
    }
}

fn specializeSdpaAttrsForPlanner(graph: *const Graph, node_id: NodeId, attrs: *ml.graph.node.AttentionAttrs) void {
    const node = graph.node(node_id);
    if (node.num_inputs < 3) return;
    const q_shape = graph.node(node.inputs[0]).output_shape;
    const k_shape = graph.node(node.inputs[1]).output_shape;

    if (q_shape.rank() == 4) {
        const batch = positivePlannerDim(q_shape, 0) orelse return;
        const num_heads = positivePlannerDim(q_shape, 1) orelse return;
        const seq_len = positivePlannerDim(q_shape, 2) orelse return;
        const head_dim = positivePlannerDim(q_shape, 3) orelse return;
        attrs.batch = @intCast(batch);
        attrs.num_heads = @intCast(num_heads);
        attrs.seq_len = @intCast(seq_len);
        attrs.head_dim = @intCast(head_dim);
        if (k_shape.rank() == 4) {
            if (positivePlannerDim(k_shape, 1)) |num_kv_heads| attrs.num_kv_heads = @intCast(num_kv_heads);
            if (positivePlannerDim(k_shape, 2)) |kv_seq_len| attrs.kv_seq_len = @intCast(kv_seq_len);
        }
        return;
    }

    if (q_shape.rank() == 3) {
        const bh = positivePlannerDim(q_shape, 0) orelse return;
        const seq_len = positivePlannerDim(q_shape, 1) orelse return;
        const head_dim = positivePlannerDim(q_shape, 2) orelse return;
        const num_heads = if (attrs.num_heads > 0) attrs.num_heads else 1;
        attrs.num_heads = num_heads;
        attrs.batch = @intCast(@max(@as(usize, 1), bh / num_heads));
        attrs.seq_len = @intCast(seq_len);
        attrs.head_dim = @intCast(head_dim);
        if (k_shape.rank() == 3) {
            if (positivePlannerDim(k_shape, 1)) |kv_seq_len| attrs.kv_seq_len = @intCast(kv_seq_len);
        }
    }
}

fn positivePlannerDim(shape: ml.graph.Shape, axis: u8) ?usize {
    if (axis >= shape.rank()) return null;
    const dim = shape.dim(axis);
    if (dim <= 0) return null;
    return @intCast(dim);
}

fn reconcileDotGeneralPlannerShapes(graph: *Graph) void {
    for (0..graph.nodeCount()) |idx| {
        const node_id: NodeId = @intCast(idx);
        const node = graph.node(node_id);
        const attrs = switch (node.op) {
            .dot_general => |attrs| attrs,
            else => continue,
        };
        if (attrs.num_batch != 0 or attrs.num_contracting != 1 or node.num_inputs < 2) continue;

        const lhs_id = node.inputs[0];
        const rhs_id = node.inputs[1];
        if (lhs_id == ml.graph.null_node or rhs_id == ml.graph.null_node) continue;
        const lhs_shape = graph.node(lhs_id).output_shape;
        const rhs_shape = graph.node(rhs_id).output_shape;
        if (lhs_shape.rank() != 2 or rhs_shape.rank() != 2) continue;

        const lhs_axis = attrs.lhs_contracting[0];
        const rhs_axis = attrs.rhs_contracting[0];
        if (lhs_axis >= lhs_shape.rank() or rhs_axis >= rhs_shape.rank()) continue;
        const rhs_dim = rhs_shape.dim(rhs_axis);
        if (rhs_dim <= 0 or lhs_shape.dim(lhs_axis) == rhs_dim) continue;
        setPlannerShapeDim(graph, lhs_id, lhs_axis, rhs_dim);
    }
}

fn setPlannerShapeDim(graph: *Graph, node_id: NodeId, axis: u8, dim: i64) void {
    const node = graph.nodeMut(node_id);
    if (axis >= node.output_shape.rank()) return;
    node.output_shape.dims[axis] = dim;
    switch (node.op) {
        .reshape => |*attrs| {
            if (axis < attrs.new_shape.rank()) attrs.new_shape.dims[axis] = dim;
        },
        .broadcast_in_dim => |*attrs| {
            if (axis < attrs.target_shape.rank()) attrs.target_shape.dims[axis] = dim;
        },
        else => {},
    }
}

test "findSdpaMask returns borrowed attention_mask input" {
    const allocator = std.testing.allocator;
    const mask = [_]i64{ 1, 1, 0, 0 };
    var attention_mask = try Tensor.initInt64(allocator, "attention_mask", &.{ 1, 4 }, &mask);
    defer attention_mask.deinit();
    const token_ids = [_]i64{ 10, 11, 0, 0 };
    var input_ids = try Tensor.initInt64(allocator, "input_ids", &.{ 1, 4 }, &token_ids);
    defer input_ids.deinit();

    const found = findSdpaMask(&.{ input_ids, attention_mask }).?;
    try std.testing.expectEqualSlices(i64, &mask, found);
}

test "imported ONNX preserves integer input storage on CPU and Metal" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!@import("metal_runtime.zig").metalDeviceAvailable()) return error.SkipZigTest;
    const a = std.testing.allocator;
    var weights = @import("../ops/gpu_hosted_store.zig").WeightStore{ .allocator = a, .prefix = "", .lazy_weights = .empty };
    defer weights.lazy_weights.deinit(a);
    var metal = try MetalCompute.init(a, &weights, null);
    defer metal.deinit();
    var native_weights = WeightStore{ .allocator = a, .resident_weights = .{}, .lazy_weights = .{} };
    var native = NativeCompute.init(a, &native_weights, null);
    defer native.deinit();
    inline for (.{ i8, i16, i32, i64, u8 }) |T| {
        const values = [_]T{ if (T == i64) 9007199254740993 else std.math.maxInt(T), std.math.minInt(T) };
        const tensor = Tensor{ .name = "input", .data = @constCast(std.mem.sliceAsBytes(&values)), .dtype = @field(@import("tensor.zig").DType, @typeName(T)), .shape = &.{ 1, 2 }, .allocator = a, .owns_data = false, .owns_shape = false };
        for ([_]bool{ false, true }) |gpu| {
            const cb = if (gpu) metal.computeBackend() else native.computeBackend();
            const ct = if (gpu) try importMetalTensor(a, &metal, &tensor) else try native.importHostTensor(&tensor);
            defer cb.free(ct);
            const raw = (try cb.exportTensorData(ct, a)).?;
            defer a.free(raw.payload.bytes);
            try std.testing.expectEqual(tensor.dtype, raw.dtype);
            try std.testing.expectEqualSlices(u8, tensor.data, raw.payload.bytes);
        }
    }
}

test "imported ONNX CPU shape cache shares weights and survives eviction" {
    try expectShapeCacheReuse(.native);
}

test "imported ONNX Metal shape cache shares weights and survives eviction" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!@import("metal_runtime.zig").metalDeviceAvailable()) return error.SkipZigTest;
    try expectShapeCacheReuse(.metal);
}

test "imported ONNX CUDA shape cache shares weights and survives eviction" {
    if (comptime !build_options.enable_cuda) return error.SkipZigTest;
    var context = @import("compute_context.zig").BackendContext.init(std.testing.allocator, .cuda, null) catch |err| switch (err) {
        error.CudaUnavailable, error.NoCudaDevices => return error.SkipZigTest,
        else => return err,
    };
    defer context.deinit(std.testing.allocator);
    try expectShapeCacheReuse(.cuda);
}

fn expectShapeCacheReuse(kind: BackendType) !void {
    const allocator = std.testing.allocator;
    var graph = Graph.init(allocator);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);
    const x = try builder.parameter("x", Shape.init(.f32, &.{ -1, 32 }));
    const bias = try builder.parameter("bias", Shape.init(.f32, &.{ 1, 32 }));
    const sum = try builder.add(x, bias);
    try graph.markOutput(sum);
    const bias_values = @as([32]f32, @splat(2));
    const bytes = try onnx_graph.exportGraph(allocator, &graph, .{ .parameter_initializers = &.{.{ .name = "bias", .shape = Shape.init(.f32, &.{ 1, 32 }), .data = .{ .f32 = &bias_values } }} });
    defer allocator.free(bytes);
    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    try dir.dir.writeFile(std.testing.io, .{ .sub_path = "model.onnx", .data = bytes });
    const path = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", dir.sub_path[0..], "model.onnx" });
    defer allocator.free(path);
    var session = try createSessionWithOptions(allocator, path, kind, .{ .graph_runtime_strategy = .partitioned });
    defer session.close();
    const parent: *ImportedOnnxSession = @ptrCast(@alignCast(session.ptr));
    try std.testing.expect(parent.source_path != null);
    // A new shape must keep working even after an atomic model replacement.
    // The cache owns a compact immutable graph, not a path to reload later.
    try dir.dir.writeFile(std.testing.io, .{ .sub_path = "model.onnx", .data = "replaced model" });
    var template = try onnx_graph.parseLazyAsModel(allocator, parent.shape_template.?);
    defer template.deinit();
    try std.testing.expectEqual(@as(usize, 0), template.getInitializer("bias").?.tensor.raw_data.len);
    try std.testing.expectEqual(@as(usize, 1), parent.static_runtime_inputs.len);
    // Revisit a cached shape, exceed capacity, then revisit the evicted shape.
    for ([_]usize{ 1, 2, 1, 3, 4, 5, 1 }, 0..) |batch, iteration| {
        const values = try allocator.alloc(f32, batch * 32);
        defer allocator.free(values);
        @memset(values, @floatFromInt(iteration));
        var input = try Tensor.initFloat32(allocator, "x", &.{ @intCast(batch), 32 }, values);
        defer input.deinit();
        var outputs = try session.run(&.{input}, allocator);
        defer {
            for (outputs) |*output| output.deinit();
            allocator.free(outputs);
        }
        for (outputs[0].asFloat32()) |value| try std.testing.expectEqual(@as(f32, @floatFromInt(iteration)) + 2, value);
        try std.testing.expect(parent.specializations.items.len <= ImportedOnnxSession.max_specializations);
        const child = parent.specializations.items[parent.specializations.items.len - 1].session;
        try std.testing.expectEqual(parent.shared_backend_ctx, child.shared_backend_ctx);
        try std.testing.expectEqual(@as(usize, 1), child.static_runtime_inputs.len);
        const fallback = if (kind == .native) &child.cb else &child.executable.runtime.fallback_native.?.backend;
        if (kind != .native) try std.testing.expectEqual(fallback, child.executable.runtime.mesh.?.device(1).?.backend);
        if (kind == .native or kind == .cuda) try std.testing.expectEqual(parent.static_runtime_inputs[0].value, child.static_runtime_inputs[0].value);
        const transfer = @import("../graph/multi_executor.zig").transferTensor;
        const exact = (try parent.cb.fromInt32Shape(&.{ 16777217, -16777217 }, &.{2})) orelse return error.TestUnexpectedResult;
        defer parent.cb.free(exact);
        const host = try transfer(allocator, exact, &parent.cb, fallback);
        defer fallback.free(host);
        const roundtrip = try transfer(allocator, host, fallback, &parent.cb);
        defer parent.cb.free(roundtrip);
        const exported = (try parent.cb.exportTensorData(roundtrip, allocator)) orelse return error.TestUnexpectedResult;
        defer allocator.free(exported.payload.bytes);
        try std.testing.expectEqualSlices(u8, std.mem.sliceAsBytes(&[_]i32{ 16777217, -16777217 }), exported.payload.bytes);
        const resident = try parent.shared_backend_ctx.importHostTensor(allocator, &input);
        defer parent.cb.free(resident);
        var resident_outputs = try runResidentImpl(parent, null, &.{.{ .value = resident, .backend = &parent.cb }}, allocator, null);
        defer resident_outputs.deinit();
        const result = try parent.cb.toFloat32(resident_outputs.outputs[0], allocator);
        defer allocator.free(result);
        try std.testing.expectEqualSlices(f32, outputs[0].asFloat32(), result);
    }
}

test "ONNX host outputs export directly from a final CPU fallback" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!@import("metal_runtime.zig").metalDeviceAvailable()) return error.SkipZigTest;
    const a = std.testing.allocator;
    var graph = Graph.init(a);
    defer graph.deinit();
    var b = ml.graph.Builder.init(&graph);
    const table = try b.parameter("table", Shape.init(.f32, &.{ 2, 3 }));
    const indices = try b.parameter("indices", Shape.init(.i64, &.{2}));
    const gathered = try b.gather(table, indices, Shape.init(.f32, &.{ 2, 2 }));
    // The resident Metal gather accepts axis 0 only. Axis 1 exercises an
    // actual native fallback, with the final output already on the host.
    graph.nodeMut(gathered).op.gather.axis = 1;
    const activated = try b.relu(gathered);
    const final = try b.gather(activated, indices, Shape.init(.f32, &.{ 2, 2 }));
    graph.nodeMut(final).op.gather.axis = 1;
    try graph.markOutput(final);
    const bytes = try onnx_graph.exportGraph(a, &graph, .{});
    defer a.free(bytes);
    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    try dir.dir.writeFile(std.testing.io, .{ .sub_path = "model.onnx", .data = bytes });
    const path = try std.fs.path.join(a, &.{ ".zig-cache", "tmp", dir.sub_path[0..], "model.onnx" });
    defer a.free(path);
    var session = try createSessionWithOptions(a, path, .metal, .{ .graph_runtime_strategy = .partitioned });
    defer session.close();
    try std.testing.expect(session.configureExecutionDiagnostics(true));
    const impl: *ImportedOnnxSession = @ptrCast(@alignCast(session.ptr));
    var saw_activation = false;
    for (impl.program.graph.nodes.items, 0..) |node, id| {
        if (node.op != .fused_relu) continue;
        saw_activation = true;
        const plan = &impl.executable.runtime.plan.?.base;
        try std.testing.expectEqual(graph_contracts.BackendKind.native, plan.partitions[plan.node_assignment[id]].backend);
    }
    try std.testing.expect(saw_activation);
    const Probe = struct {
        var original: @FieldType(ops_mod.ComputeBackend.VTable, "copyTensorFromBackend") = null;
        var copies: usize = 0;
        var native_relu_calls: usize = 0;
        fn nativeAssigned(ctx: *anyopaque, request: ops_mod.kernel_descriptor.Invocation) anyerror!bool {
            const accepted = try native_mod.vtable_impl.kernels.?.runAssignedKernel.?(ctx, request);
            if (accepted and request.output != null and request.descriptor.operation == .unary and request.descriptor.operation.unary == .relu) native_relu_calls += 1;
            return accepted;
        }
        fn nativeRelu(ctx: *anyopaque, input: ops_mod.CT) anyerror!ops_mod.CT {
            native_relu_calls += 1;
            return native_mod.vtable_impl.relu(ctx, input);
        }

        fn copy(ctx: *anyopaque, src: *anyopaque, kind: ops_mod.BackendKind, tensor: ops_mod.CT) anyerror!?ops_mod.CT {
            copies += 1;
            return original.?(ctx, src, kind, tensor);
        }
    };
    const fallback = impl.executable.runtime.fallback_native.?;
    var native_vtable = fallback.backend.vtable.*;
    native_vtable.relu = &Probe.nativeRelu;
    var functions = native_vtable.kernels.?.*;
    functions.runAssignedKernel = &Probe.nativeAssigned;
    native_vtable.kernels = &functions;
    fallback.backend.vtable = &native_vtable;
    // The instrumentation preserves the implementation, so bind its vtable
    // identity to the already-prepared physical plan too.
    for (impl.executable.runtime.physical_pool.?.backends) |*bound_backend| {
        if (bound_backend.ptr == fallback.backend.ptr) bound_backend.* = fallback.backend;
    }
    Probe.native_relu_calls = 0;
    var vtable = impl.cb.vtable.*;
    Probe.original = vtable.copyTensorFromBackend;
    Probe.copies = 0;
    vtable.copyTensorFromBackend = &Probe.copy;
    impl.cb.vtable = &vtable;
    var values = try Tensor.initFloat32(a, "table", &.{ 2, 3 }, &.{ -1, 2, -3, 4, -5, 6 });
    defer values.deinit();
    var index_values = try Tensor.initInt64(a, "indices", &.{2}, &.{ 1, 0 });
    defer index_values.deinit();
    var outputs = try session.run(&.{ values, index_values }, a);
    defer {
        for (outputs) |*tensor| tensor.deinit();
        a.free(outputs);
    }
    try std.testing.expectEqualSlices(f32, &.{ 0, 2, 4, 0 }, outputs[0].asFloat32());
    try std.testing.expectEqual(@as(usize, 0), Probe.copies);
    try std.testing.expectEqual(@as(usize, 1), Probe.native_relu_calls);
    const first_stats = session.executionDiagnostics().?.execution;
    try std.testing.expectEqual(@as(u64, 1), first_stats.physical_transfer_dispatches);
    try std.testing.expectEqual(@as(u64, 24), first_stats.physical_transfer_bytes);
    var resident = (try session.runResident(&.{ values, index_values }, a)).?;
    defer resident.deinit();
    try std.testing.expect(Probe.copies > 0);
    try std.testing.expectEqual(@as(usize, 2), Probe.native_relu_calls);
    const warm_stats = session.executionDiagnostics().?.execution;
    try std.testing.expectEqual(@as(u64, 1), warm_stats.physical_transfer_dispatches);
    try std.testing.expectEqual(@as(u64, 0), warm_stats.physical_storage_allocations);
}

test "imported onnx session preserves exported contractions and reports warm physical reuse" {
    const a = std.testing.allocator;
    for ([_]bool{ false, true }) |lower_fused| {
        var graph = Graph.init(a);
        defer graph.deinit();
        var b = ml.graph.Builder.init(&graph);
        const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 3 }));
        const weights = [_]f32{ 1, 0, -1, 0, 2, 0, 1, 1, 1, -2, 0, 1 };
        var initializers: [3]onnx_graph.ParameterInitializer = undefined;
        for ([_][]const u8{ "w0", "w1", "w2" }, &initializers) |name, *initializer| {
            const w = try b.parameter(name, Shape.init(.f32, &.{ 4, 3 }));
            const output = try b.linearNoBias(x, w, 2, 3, 4);
            try graph.markOutput(output);
            initializer.* = .{ .name = name, .shape = graph.node(w).output_shape, .data = .{ .f32 = &weights } };
        }
        const bytes = try onnx_graph.exportGraph(a, &graph, .{ .lower_fused = lower_fused, .parameter_initializers = &initializers });
        defer a.free(bytes);
        var directory = std.testing.tmpDir(.{});
        defer directory.cleanup();
        try directory.dir.writeFile(std.testing.io, .{ .sub_path = "model.onnx", .data = bytes });
        const path = try std.fs.path.join(a, &.{ ".zig-cache", "tmp", directory.sub_path[0..], "model.onnx" });
        defer a.free(path);
        var session = try createSessionWithOptions(a, path, .native, .{ .graph_runtime_strategy = .compiled_preferred });
        defer session.close();
        try std.testing.expect(session.configureExecutionDiagnostics(true));
        var input = try Tensor.initFloat32(a, "x", &.{ 2, 3 }, &.{ 1, 2, 3, 4, 5, 6 });
        defer input.deinit();
        for (0..3) |iteration| {
            const outputs = try session.run(&.{input}, a);
            defer {
                for (outputs) |*tensor| tensor.deinit();
                a.free(outputs);
            }
            try std.testing.expectEqual(@as(usize, 3), outputs.len);
            for (outputs) |tensor| try std.testing.expectEqualSlices(f32, &.{ -2, 4, 6, 1, -2, 10, 15, -2 }, tensor.asFloat32());
            const stats = session.executionDiagnostics().?.execution;
            try std.testing.expect(stats.assigned_kernel_dispatches > 0);
            if (iteration != 0) {
                try std.testing.expectEqual(@as(u64, 0), stats.physical_storage_allocations);
                try std.testing.expect(stats.physical_storage_reuses > 0);
            }
        }
        try std.testing.expect(session.configureExecutionDiagnostics(false));
        try std.testing.expect(session.executionDiagnostics() == null);
    }
}

test "resident staged execution retains its common context after publishing sessions close" {
    const a = std.testing.allocator;
    for ([_]BackendType{ .native, .metal }) |kind| {
        if (kind == .metal) {
            if (comptime !build_options.enable_metal) continue;
            if (!@import("metal_runtime.zig").metalDeviceAvailable()) continue;
        }
        var directory = std.testing.tmpDir(.{});
        defer directory.cleanup();
        for ([_][]const u8{ "encoder.onnx", "projection.onnx" }, 0..) |name, index| {
            var graph = Graph.init(a);
            defer graph.deinit();
            var b = ml.graph.Builder.init(&graph);
            const x = try b.parameter("x", Shape.init(.f32, &.{ 1, 4 }));
            const out = if (index == 0) try b.relu(x) else try b.neg(x);
            try graph.markOutput(out);
            const bytes = try onnx_graph.exportGraph(a, &graph, .{});
            defer a.free(bytes);
            try directory.dir.writeFile(std.testing.io, .{ .sub_path = name, .data = bytes });
        }
        const encoder_path = try std.fs.path.join(a, &.{ ".zig-cache", "tmp", directory.sub_path[0..], "encoder.onnx" });
        defer a.free(encoder_path);
        const projection_path = try std.fs.path.join(a, &.{ ".zig-cache", "tmp", directory.sub_path[0..], "projection.onnx" });
        defer a.free(projection_path);
        var encoder = try createSessionWithOptions(a, encoder_path, kind, .{ .graph_runtime_strategy = .compiled_preferred });
        var encoder_open = true;
        defer if (encoder_open) encoder.close();
        const common = sharedBackendContext(encoder).?;
        var input = try Tensor.initFloat32(a, "x", &.{ 1, 4 }, &.{ -2, 1, 2, -1 });
        defer input.deinit();
        var encoded = (try encoder.runResident(&.{input}, a)).?;
        var encoded_open = true;
        defer if (encoded_open) encoded.deinit();
        var next_input = try Tensor.initFloat32(a, "x", &.{ 1, 4 }, &.{ 7, 8, -9, 10 });
        defer next_input.deinit();
        var next_encoded = (try encoder.runResident(&.{next_input}, a)).?;
        defer next_encoded.deinit();
        const original_values = try encoded.backend.toFloat32(encoded.outputs[0], a);
        defer a.free(original_values);
        const next_values = try next_encoded.backend.toFloat32(next_encoded.outputs[0], a);
        defer a.free(next_values);
        try std.testing.expectEqualSlices(f32, &.{ 0, 1, 2, 0 }, original_values);
        try std.testing.expectEqualSlices(f32, &.{ 7, 8, 0, 10 }, next_values);
        encoder.close();
        encoder_open = false;
        // The output retains both storage and a stable backend handle, so a
        // subsequent stage can be prepared after its producer has retired.
        var projection = try createSessionWithOptions(a, projection_path, kind, .{ .graph_runtime_strategy = .compiled_preferred, .shared_backend_ctx = common });
        var projection_open = true;
        defer if (projection_open) projection.close();
        var projected = (try projection.runResidentInputs(&.{.{ .value = encoded.outputs[0], .backend = encoded.backend, .shape = &.{ 1, 4 }, .estimated_bytes = 16 }}, a)).?;
        defer projected.deinit();
        projection.close();
        projection_open = false;
        encoded.deinit();
        encoded_open = false;
        const values = try projected.backend.toFloat32(projected.outputs[0], a);
        defer a.free(values);
        try std.testing.expectEqualSlices(f32, &.{ 0, -1, -2, 0 }, values);
        if (kind == .metal) try std.testing.expect(@import("../graph/metal_partition_executor.zig").isMetalDeviceResident(projected.backend, projected.outputs[0]));
    }
}
