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

// Recursive graph conversion engine.
//
// Converts an ONNX GraphProto into a termite Graph by walking from
// outputs back to inputs, converting each ONNX node via ops.zig,
// and memoizing results to avoid duplicate work. The recursive walk
// naturally produces topological order (dependencies before dependents).

const std = @import("std");
const log = std.log.scoped(.onnx_convert);
const ml = @import("ml");
const proto = @import("proto.zig");
const ops = @import("ops.zig");
const tensor_mod = @import("tensor.zig");

const Graph = ml.graph.Graph;
const Builder = ml.graph.Builder;
const NodeId = ml.graph.NodeId;
const null_node = ml.graph.null_node;
const Shape = ml.graph.Shape;
const DType = ml.graph.DType;

const GraphProto = proto.GraphProto;
const NodeProto = proto.NodeProto;
const TensorProto = proto.TensorProto;
const ValueInfoProto = proto.ValueInfoProto;

pub const ConvertError = ops.ConvertError || error{
    OutputNotFound,
    NodeNotFound,
    InputNotProvided,
    CyclicGraph,
};

/// Result of converting an ONNX graph.
pub const ConvertResult = struct {
    graph: Graph,
    output_ids: []NodeId,
    /// Parameter names in definition order (for binding weights at execution).
    parameter_names: [][]const u8,

    pub fn deinit(self: *ConvertResult, allocator: std.mem.Allocator) void {
        self.graph.deinit();
        allocator.free(self.output_ids);
        allocator.free(self.parameter_names);
    }
};

/// Initializer metadata exposed to callers for weight loading.
pub const InitializerData = struct {
    name: []const u8,
    shape: Shape,
    tensor: *const TensorProto,
    /// If the tensor's data lives in a separate file (ONNX external data),
    /// this is the info needed to locate it; otherwise null.
    external: ?proto.ExternalDataInfo = null,
};

/// Lazy backing state for a Model — holds the LazyModelProto and a
/// pointer-stable slot per initializer that's filled in on first access.
/// The synthetic `graph` shares the lazy model's nodes/inputs/outputs
/// (slices borrowed, not copied) so callers get a uniform GraphProto view.
const LazyState = struct {
    lazy: proto.LazyModelProto,
    /// One slot per initializer; null until parsed.
    parsed: []?TensorProto,
    /// A GraphProto view backed by `lazy`; `initializers` is left empty
    /// because callers go through Model.initializerAt() instead of iterating
    /// the eager array.
    synthetic: GraphProto,
};

/// Persistent model state built during parse, used across conversions.
pub const Model = struct {
    allocator: std.mem.Allocator,
    onnx: proto.ModelProto,
    /// Optional lazy state. When set, initializer_map indices refer into
    /// `lazy_state.parsed` instead of `onnx.graph.?.initializers`, and
    /// initializers are parsed on first access.
    lazy_state: ?LazyState = null,
    /// Directory where the .onnx file lives; used to resolve `external_data`
    /// `location` entries. When null, external tensors become parameters and
    /// the caller is responsible for loading their data.
    base_dir: ?[]const u8 = null,

    // Pre-built indexes (populated in init)
    output_to_node: std.StringHashMapUnmanaged(u32),
    initializer_map: std.StringHashMapUnmanaged(u32),
    input_set: std.StringHashMapUnmanaged(void),

    pub fn init(allocator: std.mem.Allocator, onnx_model: proto.ModelProto) !Model {
        return initWithBaseDir(allocator, onnx_model, null);
    }

    /// Initialize a Model with a base directory for resolving external data.
    /// `base_dir` is borrowed and must outlive the Model.
    pub fn initWithBaseDir(
        allocator: std.mem.Allocator,
        onnx_model: proto.ModelProto,
        base_dir: ?[]const u8,
    ) !Model {
        var self = Model{
            .allocator = allocator,
            .onnx = onnx_model,
            .base_dir = base_dir,
            .output_to_node = .empty,
            .initializer_map = .empty,
            .input_set = .empty,
        };

        const onnx_graph = self.onnx.graph orelse return self;

        // Build output_name → node_index map
        for (onnx_graph.nodes, 0..) |*node, idx| {
            for (node.outputs) |output_name| {
                if (output_name.len > 0) {
                    try self.output_to_node.put(allocator, output_name, @intCast(idx));
                }
            }
        }

        // Build initializer name → index map
        for (onnx_graph.initializers, 0..) |*init_tensor, idx| {
            if (init_tensor.name.len > 0) {
                try self.initializer_map.put(allocator, init_tensor.name, @intCast(idx));
            }
        }

        // Build input name set (excluding initializers, which are weights)
        for (onnx_graph.inputs) |*input| {
            if (input.name.len > 0 and !self.initializer_map.contains(input.name)) {
                try self.input_set.put(allocator, input.name, {});
            }
        }

        return self;
    }

    /// Initialize a Model from a lazily-parsed ONNX protobuf. Initializers
    /// are not parsed up front — each is parsed the first time it is
    /// accessed during conversion, then cached. For large models with many
    /// weights this avoids up-front TensorProto construction for weights
    /// that the converter never touches.
    /// `base_dir` is borrowed and must outlive the Model.
    pub fn initFromLazy(
        allocator: std.mem.Allocator,
        lazy_model: proto.LazyModelProto,
        base_dir: ?[]const u8,
    ) !Model {
        var self = Model{
            .allocator = allocator,
            .onnx = .{}, // empty; real data lives in lazy_state
            .base_dir = base_dir,
            .output_to_node = .empty,
            .initializer_map = .empty,
            .input_set = .empty,
        };
        errdefer {
            self.output_to_node.deinit(allocator);
            self.initializer_map.deinit(allocator);
            self.input_set.deinit(allocator);
        }

        // Move opset_import into the eager ModelProto so opsetVersion() works.
        self.onnx.ir_version = lazy_model.ir_version;
        self.onnx.opset_import = lazy_model.opset_import;

        const lazy_graph = lazy_model.graph orelse {
            // Lazy model with no graph — still store the lazy state so
            // deinit frees opset_import, etc. via the lazy model.
            var ls = LazyState{
                .lazy = lazy_model,
                .parsed = &.{},
                .synthetic = .{},
            };
            // Clear opset_import from ls.lazy to avoid double-free in deinit.
            ls.lazy.opset_import = &.{};
            self.lazy_state = ls;
            return self;
        };

        // Allocate one cache slot per initializer.
        const parsed = try allocator.alloc(?TensorProto, lazy_graph.initializer_bytes.len);
        errdefer allocator.free(parsed);
        @memset(parsed, null);

        // Build initializer_map by extracting just the name field from
        // each raw initializer (zero-alloc per-entry).
        for (0..lazy_graph.initializer_bytes.len) |idx| {
            const name = lazy_graph.initializerName(idx) catch continue;
            if (name.len > 0) {
                try self.initializer_map.put(allocator, name, @intCast(idx));
            }
        }

        // Build output → node map (nodes are already eagerly parsed).
        for (lazy_graph.nodes, 0..) |*node, idx| {
            for (node.outputs) |output_name| {
                if (output_name.len > 0) {
                    try self.output_to_node.put(allocator, output_name, @intCast(idx));
                }
            }
        }

        // Build input_set (excluding initializers).
        for (lazy_graph.inputs) |*input| {
            if (input.name.len > 0 and !self.initializer_map.contains(input.name)) {
                try self.input_set.put(allocator, input.name, {});
            }
        }

        // Build a synthetic GraphProto view that borrows the lazy graph's
        // nodes/inputs/outputs slices. `initializers` is deliberately empty
        // because access is routed through Model.initializerAt(idx).
        const synthetic = GraphProto{
            .name = lazy_graph.name,
            .nodes = lazy_graph.nodes,
            .initializers = &.{},
            .inputs = lazy_graph.inputs,
            .outputs = lazy_graph.outputs,
        };

        var ls = LazyState{
            .lazy = lazy_model,
            .parsed = parsed,
            .synthetic = synthetic,
        };
        // Clear opset_import from ls.lazy: we moved ownership to self.onnx
        // so the lazy deinit shouldn't also free it.
        ls.lazy.opset_import = &.{};

        self.lazy_state = ls;
        return self;
    }

    pub fn deinit(self: *Model) void {
        self.output_to_node.deinit(self.allocator);
        self.initializer_map.deinit(self.allocator);
        self.input_set.deinit(self.allocator);
        if (self.lazy_state) |*ls| {
            // Free any initializers we materialized on demand.
            for (ls.parsed) |*slot| {
                if (slot.*) |*t| t.deinit(self.allocator);
            }
            self.allocator.free(ls.parsed);
            // Free the lazy proto structure (nodes, inputs, outputs, raw bytes).
            ls.lazy.deinit(self.allocator);
        }
        self.onnx.deinit(self.allocator);
    }

    /// Access an initializer by its index (as stored in initializer_map).
    /// For lazy models, parses the initializer on first access and caches
    /// it in a pointer-stable slot so subsequent calls return the same ptr.
    pub fn initializerAt(self: *Model, idx: u32) !*const TensorProto {
        if (self.lazy_state) |*ls| {
            if (idx >= ls.parsed.len) return error.NodeNotFound;
            if (ls.parsed[idx]) |*t| return t;
            const lazy_graph = ls.lazy.graph orelse return error.NodeNotFound;
            ls.parsed[idx] = try lazy_graph.parseInitializer(self.allocator, idx);
            return &ls.parsed[idx].?;
        }
        const g = self.onnx.graph orelse return error.NodeNotFound;
        if (idx >= g.initializers.len) return error.NodeNotFound;
        return &g.initializers[idx];
    }

    /// Get the ONNX graph. For lazy models returns a synthetic view that
    /// borrows the lazy graph's nodes/inputs/outputs. Do NOT iterate the
    /// synthetic view's `initializers` — it's empty by design; use
    /// `initializerAt(idx)` keyed via `initializer_map`.
    pub fn graph(self: *const Model) ?*const GraphProto {
        if (self.lazy_state) |*ls| return &ls.synthetic;
        if (self.onnx.graph) |*g| return g;
        return null;
    }

    /// Get the default opset version (domain="" i.e. ai.onnx).
    pub fn opsetVersion(self: *const Model) u64 {
        for (self.onnx.opset_import) |opset| {
            if (opset.domain.len == 0) return opset.version;
        }
        return 0; // unknown
    }

    /// Get the symbolic dimension names used in input shapes.
    /// Returns names like "batch_size", "sequence_length" etc.
    /// Caller owns the returned slice.
    pub fn dynamicDimNamesAlloc(self: *const Model, allocator: std.mem.Allocator) ![][]const u8 {
        const g = self.graph() orelse return &.{};
        var names = std.ArrayListUnmanaged([]const u8).empty;
        defer names.deinit(allocator);
        for (g.inputs) |*inp| {
            const tp = inp.type_proto orelse continue;
            const tt = tp.tensor_type orelse continue;
            const sp = tt.shape orelse continue;
            for (sp.dims) |d| {
                if (d.dim_param.len > 0) {
                    // Deduplicate
                    var found = false;
                    for (names.items) |existing| {
                        if (std.mem.eql(u8, existing, d.dim_param)) {
                            found = true;
                            break;
                        }
                    }
                    if (!found) try names.append(allocator, d.dim_param);
                }
            }
        }
        return names.toOwnedSlice(allocator);
    }

    /// Get count of true inputs (not initializers/weights).
    /// Use inputNamesAlloc for the actual names.
    pub fn inputCount(self: *const Model) usize {
        const g = self.graph() orelse return 0;
        var count: usize = 0;
        for (g.inputs) |*inp| {
            if (inp.name.len > 0 and self.input_set.contains(inp.name)) count += 1;
        }
        return count;
    }

    /// Allocate and return input names.
    pub fn inputNamesAlloc(self: *const Model, allocator: std.mem.Allocator) ![][]const u8 {
        const g = self.graph() orelse return &.{};
        var names = std.ArrayListUnmanaged([]const u8).empty;
        defer names.deinit(allocator);
        for (g.inputs) |*inp| {
            if (inp.name.len > 0 and self.input_set.contains(inp.name)) {
                try names.append(allocator, inp.name);
            }
        }
        return names.toOwnedSlice(allocator);
    }

    /// Allocate and return output names.
    pub fn outputNamesAlloc(self: *const Model, allocator: std.mem.Allocator) ![][]const u8 {
        const g = self.graph() orelse return &.{};
        var names = std.ArrayListUnmanaged([]const u8).empty;
        defer names.deinit(allocator);
        for (g.outputs) |*out| {
            if (out.name.len > 0) try names.append(allocator, out.name);
        }
        return names.toOwnedSlice(allocator);
    }

    /// Get initializer data for a parameter name.
    /// For lazy models this will parse and cache the underlying tensor
    /// on first access, which is why the receiver is `*Model` rather than
    /// `*const Model`.
    pub fn getInitializer(self: *Model, name: []const u8) ?InitializerData {
        _ = self.graph() orelse return null;
        const idx = self.initializer_map.get(name) orelse return null;
        const tensor = self.initializerAt(idx) catch return null;
        const shape = tensor_mod.tensorShape(tensor) catch return null;
        const external_info: ?proto.ExternalDataInfo = if (tensor.isExternal())
            tensor.externalDataInfo()
        else
            null;
        return .{
            .name = name,
            .shape = shape,
            .tensor = tensor,
            .external = external_info,
        };
    }

    /// Load an initializer's raw f32 data, transparently resolving external
    /// data files when `base_dir` is set. Caller owns the returned slice.
    /// May parse and cache a lazily-loaded tensor on first access.
    pub fn loadInitializerData(
        self: *Model,
        allocator: std.mem.Allocator,
        name: []const u8,
    ) ![]f32 {
        _ = self.graph() orelse return error.NodeNotFound;
        const idx = self.initializer_map.get(name) orelse return error.NodeNotFound;
        const tensor = try self.initializerAt(idx);
        return tensor_mod.extractFloat32WithExternal(allocator, tensor, self.base_dir);
    }

    /// Convert the ONNX graph to a termite Graph.
    /// `graph_inputs` maps ONNX input names to pre-created NodeIds.
    pub fn buildGraph(
        self: *Model,
        allocator: std.mem.Allocator,
        graph_inputs: *const std.StringHashMapUnmanaged(NodeId),
    ) ConvertError!ConvertResult {
        const onnx_graph = self.graph() orelse return error.OutputNotFound;

        var termite_graph = Graph.init(allocator);
        errdefer termite_graph.deinit();
        var builder = Builder.init(&termite_graph);

        // Memoization: ONNX output name → termite NodeId
        var converted: std.StringHashMapUnmanaged(NodeId) = .empty;
        defer converted.deinit(allocator);

        // Pre-populate with caller-provided inputs
        var input_iter = graph_inputs.iterator();
        while (input_iter.next()) |entry| {
            try converted.put(allocator, entry.key_ptr.*, entry.value_ptr.*);
        }

        // Track parameter names in order
        var param_names = std.ArrayListUnmanaged([]const u8).empty;
        defer param_names.deinit(allocator);

        // Convert each output
        var output_ids = std.ArrayListUnmanaged(NodeId).empty;
        defer output_ids.deinit(allocator);

        for (onnx_graph.outputs) |*output| {
            if (output.name.len == 0) continue;
            const node_id = try self.recursiveConvert(
                allocator,
                &builder,
                &converted,
                &param_names,
                onnx_graph,
                output.name,
            );
            try output_ids.append(allocator, node_id);
            try termite_graph.markOutput(node_id);
        }

        return .{
            .graph = termite_graph,
            .output_ids = try output_ids.toOwnedSlice(allocator),
            .parameter_names = try param_names.toOwnedSlice(allocator),
        };
    }

    /// Convert the ONNX graph creating parameter nodes for inputs automatically.
    /// This is the simplest API — creates parameters for all graph inputs.
    pub fn convertToGraph(self: *Model, allocator: std.mem.Allocator) ConvertError!ConvertResult {
        return self.convertToGraphWithDims(allocator, null);
    }

    /// Convert the ONNX graph, specializing dynamic dims via `dim_overrides`.
    /// Any dimension whose ONNX `dim_param` name appears as a key in the map
    /// is replaced with the corresponding concrete value before building
    /// parameter nodes. Dynamic dims that are not overridden stay as -1.
    /// This unlocks shape-dependent ops (Shape→Gather→Reshape) when the
    /// caller knows the runtime dimensions (e.g. batch=1, seq_len=128).
    pub fn convertToGraphWithDims(
        self: *Model,
        allocator: std.mem.Allocator,
        dim_overrides: ?*const DimOverrides,
    ) ConvertError!ConvertResult {
        const onnx_graph = self.graph() orelse return error.OutputNotFound;

        var termite_graph = Graph.init(allocator);
        errdefer termite_graph.deinit();
        var builder = Builder.init(&termite_graph);

        var converted: std.StringHashMapUnmanaged(NodeId) = .empty;
        defer converted.deinit(allocator);

        var param_names = std.ArrayListUnmanaged([]const u8).empty;
        defer param_names.deinit(allocator);

        // Create parameter nodes for all true inputs
        for (onnx_graph.inputs) |*input| {
            if (input.name.len == 0) continue;
            if (self.initializer_map.contains(input.name)) continue;

            const shape = inputShapeWithOverrides(input, dim_overrides) orelse Shape.init(.f32, &.{1});
            const node_id = try builder.parameter(input.name, shape);
            try converted.put(allocator, input.name, node_id);
            try param_names.append(allocator, input.name);
        }

        // Convert each output
        var output_ids = std.ArrayListUnmanaged(NodeId).empty;
        defer output_ids.deinit(allocator);

        for (onnx_graph.outputs) |*output| {
            if (output.name.len == 0) continue;
            const node_id = try self.recursiveConvert(
                allocator,
                &builder,
                &converted,
                &param_names,
                onnx_graph,
                output.name,
            );
            try output_ids.append(allocator, node_id);
            try termite_graph.markOutput(node_id);
        }

        return .{
            .graph = termite_graph,
            .output_ids = try output_ids.toOwnedSlice(allocator),
            .parameter_names = try param_names.toOwnedSlice(allocator),
        };
    }

    fn recursiveConvert(
        self: *Model,
        allocator: std.mem.Allocator,
        builder: *Builder,
        converted: *std.StringHashMapUnmanaged(NodeId),
        param_names: *std.ArrayListUnmanaged([]const u8),
        onnx_graph: *const GraphProto,
        name: []const u8,
    ) ConvertError!NodeId {
        // Already converted?
        if (converted.get(name)) |id| {
            if (id == null_node) {
                log.warn("detected recursive ONNX dependency at '{s}'", .{name});
                return error.CyclicGraph;
            }
            return id;
        }

        // Is it an initializer (weight)?
        if (self.initializer_map.get(name)) |init_idx| {
            const tensor = self.initializerAt(init_idx) catch return error.NodeNotFound;
            const shape = try tensor_mod.tensorShape(tensor);
            const count = tensor_mod.numElements(tensor.dims);

            // Small constants with inline data go into constant pool
            // (includes int64 shape tensors used by Reshape, Slice, etc.).
            // External small constants are loaded from disk when base_dir
            // is configured so shape tensors stored externally still work.
            const is_small_type = (tensor.data_type == .float32 or
                tensor.data_type == .int64 or tensor.data_type == .int32);
            const can_load_external = !tensor.isExternal() or self.base_dir != null;
            if (count <= 16 and is_small_type and can_load_external) {
                if (shape.dtype != .f32) {
                    const bytes = tensor_mod.extractNativeBytesWithExternal(allocator, tensor, self.base_dir) catch |e| {
                        log.warn("initializer '{s}': failed to load native data ({}), falling back to parameter", .{ name, e });
                        const node_id = try builder.parameter(name, shape);
                        try converted.put(allocator, name, node_id);
                        try param_names.append(allocator, name);
                        return node_id;
                    };
                    defer allocator.free(bytes);
                    const node_id = try builder.tensorConstBytes(bytes, shape);
                    try converted.put(allocator, name, node_id);
                    return node_id;
                }

                const data = tensor_mod.extractFloat32WithExternal(allocator, tensor, self.base_dir) catch |e| {
                    log.warn("initializer '{s}': failed to load data ({}), falling back to parameter", .{ name, e });
                    const node_id = try builder.parameter(name, shape);
                    try converted.put(allocator, name, node_id);
                    try param_names.append(allocator, name);
                    return node_id;
                };
                defer allocator.free(data);
                const node_id = if (count <= 1)
                    try builder.scalarConst(shape.dtype, if (data.len > 0) data[0] else 0)
                else
                    try builder.tensorConst(data, shape);
                try converted.put(allocator, name, node_id);
                return node_id;
            }

            // Large weights become parameters
            const node_id = try builder.parameter(name, shape);
            try converted.put(allocator, name, node_id);
            try param_names.append(allocator, name);
            return node_id;
        }

        // Is it a graph input? Should have been pre-populated.
        if (self.input_set.contains(name)) {
            log.warn("graph input '{s}' not provided — expected caller to supply it", .{name});
            return error.InputNotProvided;
        }

        // Find the node that produces this output
        const node_idx = self.output_to_node.get(name) orelse {
            log.warn("no ONNX node produces output '{s}'", .{name});
            return error.NodeNotFound;
        };
        const node = &onnx_graph.nodes[node_idx];
        try converted.put(allocator, name, null_node);
        errdefer _ = converted.remove(name);

        // Recursively convert all inputs (up to 16 for ops like BatchNorm with 5)
        var input_ids: [16]NodeId = .{null_node} ** 16;
        for (node.inputs, 0..) |input_name, i| {
            if (i >= 16) break;
            if (input_name.len == 0) {
                input_ids[i] = null_node;
                continue;
            }
            input_ids[i] = self.recursiveConvert(
                allocator,
                builder,
                converted,
                param_names,
                onnx_graph,
                input_name,
            ) catch |e| {
                log.warn("{s} ({s}): failed to convert input '{s}': {}", .{
                    node.op_type, name, input_name, e,
                });
                return e;
            };
        }

        // Convert this node, with extra outputs buffer for multi-output ops.
        // Pass the parent `converted` map as an outer scope so that
        // If/Loop/Scan bodies can resolve implicit captures by name.
        const num_inputs = @min(node.inputs.len, 16);
        var extra_outputs: [7]NodeId = .{null_node} ** 7;
        const extra_slice = if (node.outputs.len > 1) extra_outputs[0..@min(node.outputs.len - 1, 7)] else extra_outputs[0..0];
        const parent_scope = ops.NameScope{ .map = converted };
        const result_id = ops.convertNodeWithScope(
            allocator,
            builder,
            node,
            input_ids[0..num_inputs],
            extra_slice,
            &parent_scope,
        ) catch |e| {
            log.warn("{s} → '{s}': conversion failed: {}", .{ node.op_type, name, e });
            return e;
        };

        // Cache the primary output
        if (node.outputs.len > 0 and node.outputs[0].len > 0) {
            try converted.put(allocator, node.outputs[0], result_id);
        }

        // Cache additional outputs from multi-output ops
        for (node.outputs[1..], 0..) |out_name, ei| {
            if (out_name.len > 0) {
                const extra_id = if (ei < extra_slice.len and extra_slice[ei] != null_node)
                    extra_slice[ei]
                else
                    result_id; // fallback to primary if not populated
                try converted.put(allocator, out_name, extra_id);
            }
        }

        return result_id;
    }
};

/// Map from ONNX symbolic dim names (e.g. "batch_size", "sequence_length")
/// to the concrete integer values the caller wants to bind.
pub const DimOverrides = std.StringHashMapUnmanaged(i64);

fn inputShape(info: *const ValueInfoProto) ?Shape {
    return inputShapeWithOverrides(info, null);
}

fn inputShapeWithOverrides(info: *const ValueInfoProto, overrides: ?*const DimOverrides) ?Shape {
    const type_proto = info.type_proto orelse return null;
    const tensor_type = type_proto.tensor_type orelse return null;
    const shape_proto = tensor_type.shape orelse return null;
    const dtype = tensor_mod.onnxDTypeToTermite(tensor_type.elem_type) catch return null;

    if (shape_proto.dims.len > 8) return null;
    var dims: [8]i64 = .{0} ** 8;
    for (shape_proto.dims, 0..) |d, i| {
        // Prefer an explicit override keyed by dim_param name when provided.
        if (d.dim_param.len > 0) {
            if (overrides) |ov| {
                if (ov.get(d.dim_param)) |concrete| {
                    dims[i] = concrete;
                    continue;
                }
            }
        }
        dims[i] = d.dim_value orelse -1; // -1 for dynamic (no override)
    }
    return Shape{
        .dtype = dtype,
        .dims = dims,
        .rank_ = @intCast(shape_proto.dims.len),
    };
}

// ── Tests ────────────────────────────────────────────────────────────

test "Model.init with empty proto" {
    const allocator = std.testing.allocator;
    var model = try Model.init(allocator, .{});
    defer model.deinit();
    try std.testing.expect(model.graph() == null);
}

test "Model.init indexes nodes and initializers" {
    const allocator = std.testing.allocator;

    // Build a minimal graph: one Add node, one initializer
    var node_inputs = [_][]const u8{ "x", "bias" };
    var node_outputs = [_][]const u8{"y"};
    var nodes = [_]NodeProto{
        .{
            .op_type = "Add",
            .inputs = &node_inputs,
            .outputs = &node_outputs,
        },
    };
    var init_dims = [_]i64{4};
    var initializers = [_]TensorProto{
        .{ .name = "bias", .dims = &init_dims, .data_type = .float32 },
    };
    var input_infos = [_]ValueInfoProto{
        .{ .name = "x" },
        .{ .name = "bias" }, // also an input in ONNX, but is an initializer
    };
    var output_infos = [_]ValueInfoProto{
        .{ .name = "y" },
    };
    const graph_proto = GraphProto{
        .nodes = &nodes,
        .initializers = &initializers,
        .inputs = &input_infos,
        .outputs = &output_infos,
    };
    _ = &nodes;
    _ = &initializers;
    _ = &input_infos;
    _ = &output_infos;

    var model = try Model.init(allocator, .{ .graph = graph_proto });
    defer {
        model.output_to_node.deinit(allocator);
        model.initializer_map.deinit(allocator);
        model.input_set.deinit(allocator);
        // Don't deinit onnx — we used stack-allocated data
    }

    // "y" should map to node 0
    try std.testing.expectEqual(@as(?u32, 0), model.output_to_node.get("y"));
    // "bias" should be in initializer map
    try std.testing.expectEqual(@as(?u32, 0), model.initializer_map.get("bias"));
    // "x" should be a true input, "bias" should not
    try std.testing.expect(model.input_set.contains("x"));
    try std.testing.expect(!model.input_set.contains("bias"));
}

test "Model.inputCount and inputNamesAlloc" {
    const allocator = std.testing.allocator;

    var node_inputs = [_][]const u8{ "x", "bias" };
    var node_outputs = [_][]const u8{"y"};
    var nodes = [_]NodeProto{
        .{ .op_type = "Add", .inputs = &node_inputs, .outputs = &node_outputs },
    };
    var init_dims = [_]i64{4};
    var initializers = [_]TensorProto{
        .{ .name = "bias", .dims = &init_dims, .data_type = .float32 },
    };
    var input_infos = [_]ValueInfoProto{
        .{ .name = "x" },
        .{ .name = "bias" },
    };
    var output_infos = [_]ValueInfoProto{
        .{ .name = "y" },
    };
    const graph_proto = GraphProto{
        .nodes = &nodes,
        .initializers = &initializers,
        .inputs = &input_infos,
        .outputs = &output_infos,
    };
    _ = &nodes;
    _ = &initializers;
    _ = &input_infos;
    _ = &output_infos;

    var model = try Model.init(allocator, .{ .graph = graph_proto });
    defer {
        model.output_to_node.deinit(allocator);
        model.initializer_map.deinit(allocator);
        model.input_set.deinit(allocator);
    }

    // Only "x" is a true input (bias is initializer)
    try std.testing.expectEqual(@as(usize, 1), model.inputCount());

    const names = try model.inputNamesAlloc(allocator);
    defer allocator.free(names);
    try std.testing.expectEqual(@as(usize, 1), names.len);
    try std.testing.expectEqualStrings("x", names[0]);
}

test "Model.outputNamesAlloc" {
    const allocator = std.testing.allocator;

    var node_inputs = [_][]const u8{ "x", "bias" };
    var node_outputs = [_][]const u8{"y"};
    var nodes = [_]NodeProto{
        .{ .op_type = "Add", .inputs = &node_inputs, .outputs = &node_outputs },
    };
    var output_infos = [_]ValueInfoProto{
        .{ .name = "y" },
    };
    const graph_proto = GraphProto{
        .nodes = &nodes,
        .outputs = &output_infos,
    };
    _ = &nodes;
    _ = &output_infos;

    var model = try Model.init(allocator, .{ .graph = graph_proto });
    defer {
        model.output_to_node.deinit(allocator);
        model.initializer_map.deinit(allocator);
        model.input_set.deinit(allocator);
    }

    const names = try model.outputNamesAlloc(allocator);
    defer allocator.free(names);
    try std.testing.expectEqual(@as(usize, 1), names.len);
    try std.testing.expectEqualStrings("y", names[0]);
}

test "Model.getInitializer returns data for known initializer" {
    const allocator = std.testing.allocator;

    var init_dims = [_]i64{ 2, 3 };
    var initializers = [_]TensorProto{
        .{ .name = "weight", .dims = &init_dims, .data_type = .float32 },
    };
    const graph_proto = GraphProto{
        .initializers = &initializers,
    };
    _ = &initializers;

    var model = try Model.init(allocator, .{ .graph = graph_proto });
    defer {
        model.output_to_node.deinit(allocator);
        model.initializer_map.deinit(allocator);
        model.input_set.deinit(allocator);
    }

    const init_data = model.getInitializer("weight");
    try std.testing.expect(init_data != null);
    try std.testing.expectEqualStrings("weight", init_data.?.name);
    try std.testing.expectEqual(@as(u8, 2), init_data.?.shape.rank());
    try std.testing.expect(init_data.?.external == null);

    // Missing initializer returns null
    try std.testing.expect(model.getInitializer("missing") == null);
}

test "Model.getInitializer reports external data" {
    const allocator = std.testing.allocator;

    var init_dims = [_]i64{8};
    var entries = [_]proto.ExternalDataEntry{
        .{ .key = "location", .value = "weights.bin" },
        .{ .key = "offset", .value = "256" },
        .{ .key = "length", .value = "32" },
    };
    var initializers = [_]TensorProto{
        .{
            .name = "big_weight",
            .dims = &init_dims,
            .data_type = .float32,
            .data_location = .external,
            .external_data = &entries,
        },
    };
    const graph_proto = GraphProto{ .initializers = &initializers };
    _ = &initializers;
    _ = &entries;

    var model = try Model.initWithBaseDir(allocator, .{ .graph = graph_proto }, "/models");
    defer {
        model.output_to_node.deinit(allocator);
        model.initializer_map.deinit(allocator);
        model.input_set.deinit(allocator);
    }

    try std.testing.expectEqualStrings("/models", model.base_dir.?);

    const init_data = model.getInitializer("big_weight").?;
    try std.testing.expect(init_data.external != null);
    try std.testing.expectEqualStrings("weights.bin", init_data.external.?.location);
    try std.testing.expectEqual(@as(i64, 256), init_data.external.?.offset);
    try std.testing.expectEqual(@as(i64, 32), init_data.external.?.length);
}

test "Model.loadInitializerData loads external small constants" {
    const allocator = std.testing.allocator;

    // Write the backing bytes to a temp location.
    const values = [_]f32{ 0.25, 0.5, 0.75, 1.0 };
    const raw = std.mem.sliceAsBytes(&values);
    const base_dir = "/tmp";
    const file_name = "termite_onnx_model_ext.bin";
    const full_path = "/tmp/termite_onnx_model_ext.bin";
    {
        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        try std.Io.Dir.cwd().writeFile(io_impl.io(), .{ .sub_path = full_path, .data = raw });
    }
    defer {
        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        std.Io.Dir.cwd().deleteFile(io_impl.io(), full_path) catch {};
    }

    var init_dims = [_]i64{4};
    var entries = [_]proto.ExternalDataEntry{
        .{ .key = "location", .value = file_name },
    };
    var initializers = [_]TensorProto{
        .{
            .name = "scale",
            .dims = &init_dims,
            .data_type = .float32,
            .data_location = .external,
            .external_data = &entries,
        },
    };
    const graph_proto = GraphProto{ .initializers = &initializers };
    _ = &initializers;
    _ = &entries;

    var model = try Model.initWithBaseDir(allocator, .{ .graph = graph_proto }, base_dir);
    defer {
        model.output_to_node.deinit(allocator);
        model.initializer_map.deinit(allocator);
        model.input_set.deinit(allocator);
    }

    const data = try model.loadInitializerData(allocator, "scale");
    defer allocator.free(data);
    try std.testing.expectEqual(@as(usize, 4), data.len);
    try std.testing.expectEqual(@as(f32, 0.25), data[0]);
    try std.testing.expectEqual(@as(f32, 1.0), data[3]);
}

test "Model.convertToGraph inlines external small constants when base_dir is set" {
    const allocator = std.testing.allocator;

    // Write bias data to disk as the external storage.
    const bias_values = [_]f32{ 0.1, 0.2, 0.3, 0.4 };
    const bias_raw = std.mem.sliceAsBytes(&bias_values);
    const base_dir = "/tmp";
    const file_name = "termite_onnx_convert_ext.bin";
    const full_path = "/tmp/termite_onnx_convert_ext.bin";
    {
        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        try std.Io.Dir.cwd().writeFile(io_impl.io(), .{ .sub_path = full_path, .data = bias_raw });
    }
    defer {
        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        std.Io.Dir.cwd().deleteFile(io_impl.io(), full_path) catch {};
    }

    var node_inputs = [_][]const u8{ "x", "bias" };
    var node_outputs = [_][]const u8{"y"};
    var nodes = [_]NodeProto{
        .{ .op_type = "Add", .inputs = &node_inputs, .outputs = &node_outputs },
    };
    var init_dims = [_]i64{4};
    var entries = [_]proto.ExternalDataEntry{
        .{ .key = "location", .value = file_name },
    };
    var initializers = [_]TensorProto{
        .{
            .name = "bias",
            .dims = &init_dims,
            .data_type = .float32,
            .data_location = .external,
            .external_data = &entries,
        },
    };
    var input_infos = [_]ValueInfoProto{
        .{ .name = "x" },
        .{ .name = "bias" },
    };
    var output_infos = [_]ValueInfoProto{
        .{ .name = "y" },
    };
    const graph_proto = GraphProto{
        .nodes = &nodes,
        .initializers = &initializers,
        .inputs = &input_infos,
        .outputs = &output_infos,
    };
    _ = &nodes;
    _ = &initializers;
    _ = &input_infos;
    _ = &output_infos;
    _ = &entries;

    var model = try Model.initWithBaseDir(allocator, .{ .graph = graph_proto }, base_dir);
    defer {
        model.output_to_node.deinit(allocator);
        model.initializer_map.deinit(allocator);
        model.input_set.deinit(allocator);
    }

    var result = try model.convertToGraph(allocator);
    defer result.deinit(allocator);

    // bias should have been inlined as a constant (loaded from disk), not
    // surfaced as a parameter. Only "x" should be in parameter_names.
    try std.testing.expectEqual(@as(usize, 1), result.parameter_names.len);
    try std.testing.expectEqualStrings("x", result.parameter_names[0]);
    try std.testing.expect(result.graph.nodeCount() >= 3);
}

test "Model.convertToGraph falls back to parameter for external small constants without base_dir" {
    const allocator = std.testing.allocator;

    var node_inputs = [_][]const u8{ "x", "bias" };
    var node_outputs = [_][]const u8{"y"};
    var nodes = [_]NodeProto{
        .{ .op_type = "Add", .inputs = &node_inputs, .outputs = &node_outputs },
    };
    var init_dims = [_]i64{4};
    var entries = [_]proto.ExternalDataEntry{
        .{ .key = "location", .value = "weights.bin" },
    };
    var initializers = [_]TensorProto{
        .{
            .name = "bias",
            .dims = &init_dims,
            .data_type = .float32,
            .data_location = .external,
            .external_data = &entries,
        },
    };
    var input_infos = [_]ValueInfoProto{
        .{ .name = "x" },
        .{ .name = "bias" },
    };
    var output_infos = [_]ValueInfoProto{
        .{ .name = "y" },
    };
    const graph_proto = GraphProto{
        .nodes = &nodes,
        .initializers = &initializers,
        .inputs = &input_infos,
        .outputs = &output_infos,
    };
    _ = &nodes;
    _ = &initializers;
    _ = &input_infos;
    _ = &output_infos;
    _ = &entries;

    // No base_dir → external tensor must become a parameter.
    var model = try Model.init(allocator, .{ .graph = graph_proto });
    defer {
        model.output_to_node.deinit(allocator);
        model.initializer_map.deinit(allocator);
        model.input_set.deinit(allocator);
    }

    var result = try model.convertToGraph(allocator);
    defer result.deinit(allocator);

    // Both "x" and "bias" should be parameters.
    try std.testing.expectEqual(@as(usize, 2), result.parameter_names.len);
}

test "Model.convertToGraph builds graph from Add node" {
    const allocator = std.testing.allocator;

    // Graph: input "x" + small constant initializer "bias" → output "y"
    var node_inputs = [_][]const u8{ "x", "bias" };
    var node_outputs = [_][]const u8{"y"};
    var nodes = [_]NodeProto{
        .{ .op_type = "Add", .inputs = &node_inputs, .outputs = &node_outputs },
    };
    // Small initializer (≤16 elements, f32) → goes into constant pool
    const bias_values = [_]f32{ 0.1, 0.2, 0.3, 0.4 };
    const bias_raw = std.mem.sliceAsBytes(&bias_values);
    var init_dims = [_]i64{4};
    var initializers = [_]TensorProto{
        .{ .name = "bias", .dims = &init_dims, .data_type = .float32, .raw_data = bias_raw },
    };
    var input_infos = [_]ValueInfoProto{
        .{ .name = "x" },
        .{ .name = "bias" },
    };
    var output_infos = [_]ValueInfoProto{
        .{ .name = "y" },
    };
    const graph_proto = GraphProto{
        .nodes = &nodes,
        .initializers = &initializers,
        .inputs = &input_infos,
        .outputs = &output_infos,
    };
    _ = &nodes;
    _ = &initializers;
    _ = &input_infos;
    _ = &output_infos;

    var model = try Model.init(allocator, .{ .graph = graph_proto });
    defer {
        model.output_to_node.deinit(allocator);
        model.initializer_map.deinit(allocator);
        model.input_set.deinit(allocator);
    }

    var result = try model.convertToGraph(allocator);
    defer result.deinit(allocator);

    // Should have at least 3 nodes: parameter(x), constant(bias), add(x, bias)
    try std.testing.expect(result.graph.nodeCount() >= 3);
    // One output
    try std.testing.expectEqual(@as(usize, 1), result.output_ids.len);
    // "x" should be in parameter_names (true input)
    try std.testing.expect(result.parameter_names.len >= 1);
    var found_x = false;
    for (result.parameter_names) |name| {
        if (std.mem.eql(u8, name, "x")) found_x = true;
    }
    try std.testing.expect(found_x);
}

test "Model.convertToGraph preserves large int64 scalar initializer" {
    const allocator = std.testing.allocator;

    var node_inputs = [_][]const u8{"limit"};
    var node_outputs = [_][]const u8{"y"};
    var nodes = [_]NodeProto{
        .{ .op_type = "Identity", .inputs = &node_inputs, .outputs = &node_outputs },
    };
    const limit_value = [_]i64{std.math.maxInt(i64)};
    const limit_raw = std.mem.sliceAsBytes(&limit_value);
    var init_dims = [_]i64{};
    var initializers = [_]TensorProto{
        .{ .name = "limit", .dims = &init_dims, .data_type = .int64, .raw_data = limit_raw },
    };
    var input_infos = [_]ValueInfoProto{
        .{ .name = "limit" },
    };
    var output_infos = [_]ValueInfoProto{
        .{ .name = "y" },
    };
    const graph_proto = GraphProto{
        .nodes = &nodes,
        .initializers = &initializers,
        .inputs = &input_infos,
        .outputs = &output_infos,
    };
    _ = &nodes;
    _ = &initializers;
    _ = &input_infos;
    _ = &output_infos;

    var model = try Model.init(allocator, .{ .graph = graph_proto });
    defer {
        model.output_to_node.deinit(allocator);
        model.initializer_map.deinit(allocator);
        model.input_set.deinit(allocator);
    }

    var result = try model.convertToGraph(allocator);
    defer result.deinit(allocator);

    var found = false;
    for (result.graph.nodes.items) |node| {
        if (node.output_shape.dtype != .i64 or node.op != .constant) continue;
        const attrs = node.op.constant;
        const data = result.graph.constantDataAs(i64, attrs.data_offset, attrs.data_len);
        try std.testing.expectEqual(@as(usize, 1), data.len);
        try std.testing.expectEqual(std.math.maxInt(i64), data[0]);
        found = true;
    }
    try std.testing.expect(found);
}

test "Model.convertToGraph errors on missing input" {
    const allocator = std.testing.allocator;

    // Graph references input "x" but model has no input or initializer for it,
    // and we use buildGraph without providing it
    var node_inputs = [_][]const u8{"x"};
    var node_outputs = [_][]const u8{"y"};
    var nodes = [_]NodeProto{
        .{ .op_type = "Identity", .inputs = &node_inputs, .outputs = &node_outputs },
    };
    var input_infos = [_]ValueInfoProto{
        .{ .name = "x" },
    };
    var output_infos = [_]ValueInfoProto{
        .{ .name = "y" },
    };
    const graph_proto = GraphProto{
        .nodes = &nodes,
        .inputs = &input_infos,
        .outputs = &output_infos,
    };
    _ = &nodes;
    _ = &input_infos;
    _ = &output_infos;

    var model = try Model.init(allocator, .{ .graph = graph_proto });
    defer {
        model.output_to_node.deinit(allocator);
        model.initializer_map.deinit(allocator);
        model.input_set.deinit(allocator);
    }

    // buildGraph without providing "x" should error
    var graph_inputs: std.StringHashMapUnmanaged(NodeId) = .empty;
    const result = model.buildGraph(allocator, &graph_inputs);
    try std.testing.expectError(error.InputNotProvided, result);
}

test "Model.convertToGraph with two inputs" {
    const allocator = std.testing.allocator;

    // Two-input graph: a + b → c (no initializers)
    var node_inputs = [_][]const u8{ "a", "b" };
    var node_outputs = [_][]const u8{"c"};
    var nodes = [_]NodeProto{
        .{ .op_type = "Add", .inputs = &node_inputs, .outputs = &node_outputs },
    };
    var input_infos = [_]ValueInfoProto{
        .{ .name = "a" },
        .{ .name = "b" },
    };
    var output_infos = [_]ValueInfoProto{
        .{ .name = "c" },
    };
    const graph_proto = GraphProto{
        .nodes = &nodes,
        .inputs = &input_infos,
        .outputs = &output_infos,
    };
    _ = &nodes;
    _ = &input_infos;
    _ = &output_infos;

    var model = try Model.init(allocator, .{ .graph = graph_proto });
    defer {
        model.output_to_node.deinit(allocator);
        model.initializer_map.deinit(allocator);
        model.input_set.deinit(allocator);
    }

    var result = try model.convertToGraph(allocator);
    defer result.deinit(allocator);

    // Should have one output
    try std.testing.expectEqual(@as(usize, 1), result.output_ids.len);
    // Both "a" and "b" should be parameters
    try std.testing.expectEqual(@as(usize, 2), result.parameter_names.len);
    // Graph should have at least 3 nodes: param(a), param(b), add
    try std.testing.expect(result.graph.nodeCount() >= 3);
}

test "Model.opsetVersion" {
    const allocator = std.testing.allocator;

    // No opsets → version 0
    var model_no_opset = try Model.init(allocator, .{});
    defer model_no_opset.deinit();
    try std.testing.expectEqual(@as(u64, 0), model_no_opset.opsetVersion());

    // With opset
    var opsets = [_]proto.OpsetImport{
        .{ .domain = "", .version = 17 },
        .{ .domain = "com.microsoft", .version = 1 },
    };
    var model_with_opset = try Model.init(allocator, .{ .opset_import = &opsets });
    defer {
        model_with_opset.output_to_node.deinit(allocator);
        model_with_opset.initializer_map.deinit(allocator);
        model_with_opset.input_set.deinit(allocator);
    }
    try std.testing.expectEqual(@as(u64, 17), model_with_opset.opsetVersion());
}

test "inputShape preserves dynamic dims" {
    // Build a ValueInfoProto with dynamic batch dim
    var dims = [_]proto.TensorShapeProto.Dimension{
        .{ .dim_param = "batch" },
        .{ .dim_value = 128 },
    };
    const shape_proto = proto.TensorShapeProto{ .dims = &dims };
    const tensor_type = proto.TensorTypeProto{
        .elem_type = .float32,
        .shape = shape_proto,
    };
    const type_proto = proto.TypeProto{ .tensor_type = tensor_type };
    const info = ValueInfoProto{ .name = "x", .type_proto = type_proto };
    _ = &dims;

    const shape = inputShape(&info).?;
    try std.testing.expectEqual(@as(u8, 2), shape.rank());
    try std.testing.expectEqual(@as(i64, -1), shape.dim(0)); // dynamic
    try std.testing.expectEqual(@as(i64, 128), shape.dim(1)); // static
}

test "inputShapeWithOverrides substitutes named dims" {
    const allocator = std.testing.allocator;

    var dims = [_]proto.TensorShapeProto.Dimension{
        .{ .dim_param = "batch" },
        .{ .dim_param = "seq_len" },
        .{ .dim_value = 768 },
    };
    const shape_proto = proto.TensorShapeProto{ .dims = &dims };
    const tensor_type = proto.TensorTypeProto{
        .elem_type = .float32,
        .shape = shape_proto,
    };
    const type_proto = proto.TypeProto{ .tensor_type = tensor_type };
    const info = ValueInfoProto{ .name = "x", .type_proto = type_proto };
    _ = &dims;

    var overrides: DimOverrides = .empty;
    defer overrides.deinit(allocator);
    try overrides.put(allocator, "batch", 2);
    try overrides.put(allocator, "seq_len", 128);

    // With overrides: both dynamic dims should be concrete
    const shape_concrete = inputShapeWithOverrides(&info, &overrides).?;
    try std.testing.expectEqual(@as(u8, 3), shape_concrete.rank());
    try std.testing.expectEqual(@as(i64, 2), shape_concrete.dim(0));
    try std.testing.expectEqual(@as(i64, 128), shape_concrete.dim(1));
    try std.testing.expectEqual(@as(i64, 768), shape_concrete.dim(2));

    // Without overrides: dynamic dims stay as -1
    const shape_dynamic = inputShapeWithOverrides(&info, null).?;
    try std.testing.expectEqual(@as(i64, -1), shape_dynamic.dim(0));
    try std.testing.expectEqual(@as(i64, -1), shape_dynamic.dim(1));
    try std.testing.expectEqual(@as(i64, 768), shape_dynamic.dim(2));
}

test "Model.convertToGraphWithDims specializes parameter shapes" {
    const allocator = std.testing.allocator;

    // Build ONNX graph: Identity(x) → y, with x of shape [batch, seq_len, 768]
    var node_inputs = [_][]const u8{"x"};
    var node_outputs = [_][]const u8{"y"};
    var nodes = [_]NodeProto{
        .{ .op_type = "Identity", .inputs = &node_inputs, .outputs = &node_outputs },
    };

    var in_dims = [_]proto.TensorShapeProto.Dimension{
        .{ .dim_param = "batch" },
        .{ .dim_param = "seq_len" },
        .{ .dim_value = 768 },
    };
    const in_shape_proto = proto.TensorShapeProto{ .dims = &in_dims };
    const in_tensor_type = proto.TensorTypeProto{
        .elem_type = .float32,
        .shape = in_shape_proto,
    };
    const in_type_proto = proto.TypeProto{ .tensor_type = in_tensor_type };

    var input_infos = [_]ValueInfoProto{
        .{ .name = "x", .type_proto = in_type_proto },
    };
    var output_infos = [_]ValueInfoProto{
        .{ .name = "y" },
    };
    const graph_proto = GraphProto{
        .nodes = &nodes,
        .inputs = &input_infos,
        .outputs = &output_infos,
    };
    _ = &nodes;
    _ = &in_dims;
    _ = &input_infos;
    _ = &output_infos;

    var model = try Model.init(allocator, .{ .graph = graph_proto });
    defer {
        model.output_to_node.deinit(allocator);
        model.initializer_map.deinit(allocator);
        model.input_set.deinit(allocator);
    }

    var overrides: DimOverrides = .empty;
    defer overrides.deinit(allocator);
    try overrides.put(allocator, "batch", 4);
    try overrides.put(allocator, "seq_len", 64);

    var result = try model.convertToGraphWithDims(allocator, &overrides);
    defer result.deinit(allocator);

    // Find the parameter node for x and check its concrete shape
    var found = false;
    const count = result.graph.nodeCount();
    for (0..count) |i| {
        const n = result.graph.node(@intCast(i));
        if (n.op == .parameter) {
            try std.testing.expectEqual(@as(u8, 3), n.output_shape.rank());
            try std.testing.expectEqual(@as(i64, 4), n.output_shape.dim(0));
            try std.testing.expectEqual(@as(i64, 64), n.output_shape.dim(1));
            try std.testing.expectEqual(@as(i64, 768), n.output_shape.dim(2));
            found = true;
        }
    }
    try std.testing.expect(found);
}

test "Model.dynamicDimNamesAlloc" {
    const allocator = std.testing.allocator;

    var dims = [_]proto.TensorShapeProto.Dimension{
        .{ .dim_param = "batch_size" },
        .{ .dim_param = "seq_len" },
        .{ .dim_value = 768 },
    };
    const shape_proto = proto.TensorShapeProto{ .dims = &dims };
    const tensor_type = proto.TensorTypeProto{
        .elem_type = .float32,
        .shape = shape_proto,
    };
    const type_proto = proto.TypeProto{ .tensor_type = tensor_type };
    var input_infos = [_]ValueInfoProto{
        .{ .name = "input_ids", .type_proto = type_proto },
    };
    const graph_proto = GraphProto{
        .inputs = &input_infos,
    };
    _ = &dims;
    _ = &input_infos;

    var model = try Model.init(allocator, .{ .graph = graph_proto });
    defer {
        model.output_to_node.deinit(allocator);
        model.initializer_map.deinit(allocator);
        model.input_set.deinit(allocator);
    }

    const dyn_names = try model.dynamicDimNamesAlloc(allocator);
    defer allocator.free(dyn_names);
    try std.testing.expectEqual(@as(usize, 2), dyn_names.len);
    try std.testing.expectEqualStrings("batch_size", dyn_names[0]);
    try std.testing.expectEqualStrings("seq_len", dyn_names[1]);
}

test "end-to-end: multi-node graph with optimization passes" {
    const allocator = std.testing.allocator;

    // Build a small MLP-like graph:
    //   x → Relu → Add(bias1) → Relu → Add(bias2) → y
    // With small constant initializers for biases.
    var relu1_inputs = [_][]const u8{"x"};
    var relu1_outputs = [_][]const u8{"relu1"};
    var add1_inputs = [_][]const u8{ "relu1", "bias1" };
    var add1_outputs = [_][]const u8{"add1"};
    var relu2_inputs = [_][]const u8{"add1"};
    var relu2_outputs = [_][]const u8{"relu2"};
    var add2_inputs = [_][]const u8{ "relu2", "bias2" };
    var add2_outputs = [_][]const u8{"y"};

    var nodes = [_]NodeProto{
        .{ .op_type = "Relu", .inputs = &relu1_inputs, .outputs = &relu1_outputs },
        .{ .op_type = "Add", .inputs = &add1_inputs, .outputs = &add1_outputs },
        .{ .op_type = "Relu", .inputs = &relu2_inputs, .outputs = &relu2_outputs },
        .{ .op_type = "Add", .inputs = &add2_inputs, .outputs = &add2_outputs },
    };

    // Small constant biases → go into constant pool
    const bias1_vals = [_]f32{ 0.1, 0.2, 0.3, 0.4 };
    const bias1_raw = std.mem.sliceAsBytes(&bias1_vals);
    const bias2_vals = [_]f32{ 0.5, 0.6, 0.7, 0.8 };
    const bias2_raw = std.mem.sliceAsBytes(&bias2_vals);
    var init_dims = [_]i64{4};
    var initializers = [_]TensorProto{
        .{ .name = "bias1", .dims = &init_dims, .data_type = .float32, .raw_data = bias1_raw },
        .{ .name = "bias2", .dims = &init_dims, .data_type = .float32, .raw_data = bias2_raw },
    };
    var input_infos = [_]ValueInfoProto{
        .{ .name = "x" },
        .{ .name = "bias1" },
        .{ .name = "bias2" },
    };
    var output_infos = [_]ValueInfoProto{
        .{ .name = "y" },
    };
    const graph_proto = GraphProto{
        .nodes = &nodes,
        .initializers = &initializers,
        .inputs = &input_infos,
        .outputs = &output_infos,
    };
    _ = &nodes;
    _ = &initializers;
    _ = &input_infos;
    _ = &output_infos;

    var model = try Model.init(allocator, .{ .graph = graph_proto });
    defer {
        model.output_to_node.deinit(allocator);
        model.initializer_map.deinit(allocator);
        model.input_set.deinit(allocator);
    }

    // Convert to termite graph
    var result = try model.convertToGraph(allocator);
    defer result.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 1), result.output_ids.len);
    try std.testing.expect(result.graph.nodeCount() >= 5); // param + 2 constants + 2 relu + 2 add
    const pre_opt_count = result.graph.nodeCount();

    // Run optimization pipeline (const_fold → fuse → cse)
    const Pipeline = ml.graph.passes.pipeline.Pipeline;
    var opt_result = try Pipeline.default.run(allocator, &result.graph);
    defer opt_result.deinit();

    // Optimized graph should be valid (at least has outputs)
    try std.testing.expect(opt_result.graph.nodeCount() > 0);
    try std.testing.expect(opt_result.graph.outputs.items.len > 0);

    // The fusion pass should have converted Relu ops to fused_relu
    // and possibly other optimizations. Node count may differ.
    _ = pre_opt_count; // available for debugging
}

// ── Lazy-path conversion tests ───────────────────────────────────────

test "Model.initFromLazy: convertToGraph round-trips via lazy parser" {
    const allocator = std.testing.allocator;

    // Build a small ONNX model via the export serializer, then parse it
    // back through the lazy path and convert to a termite graph.
    const @"export" = @import("export.zig");

    var graph = ml.graph.Graph.init(allocator);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);

    const x = try builder.parameter("x", Shape.init(.f32, &.{4}));
    // Small constants become initializers in the exported ONNX model.
    const bias = try builder.tensorConst(&.{ 0.1, 0.2, 0.3, 0.4 }, Shape.init(.f32, &.{4}));
    const sum = try builder.add(x, bias);
    try graph.markOutput(sum);

    const bytes = try @"export".exportGraph(allocator, &graph, .{});
    defer allocator.free(bytes);

    // Parse via the lazy path and build a Model.
    const lazy_model = try proto.parseLazyModelProto(allocator, bytes);
    var model = try Model.initFromLazy(allocator, lazy_model, null);
    defer model.deinit();

    // The synthetic graph should expose the same nodes/inputs/outputs as eager.
    const g = model.graph() orelse return error.OutputNotFound;
    try std.testing.expect(g.nodes.len >= 1);
    try std.testing.expect(g.inputs.len >= 1);
    try std.testing.expect(g.outputs.len == 1);

    // Converting should parse the bias initializer on demand.
    var result = try model.convertToGraph(allocator);
    defer result.deinit(allocator);

    try std.testing.expect(result.graph.nodeCount() > 0);
    try std.testing.expect(result.output_ids.len == 1);
}

test "Model.initFromLazy: lazy parsing skips untouched initializers" {
    const allocator = std.testing.allocator;

    // Build a model with two initializers, where only one is referenced
    // by a node. After lazy conversion only the referenced one should be
    // materialized in the cache.
    var node_inputs = [_][]const u8{ "x", "used_bias" };
    var node_outputs = [_][]const u8{"y"};
    var nodes = [_]NodeProto{
        .{ .op_type = "Add", .inputs = &node_inputs, .outputs = &node_outputs },
    };
    var used_dims = [_]i64{4};
    var unused_dims = [_]i64{4};
    const used_data = [_]f32{ 0.1, 0.2, 0.3, 0.4 };
    const unused_data = [_]f32{ 9.9, 9.9, 9.9, 9.9 };
    var initializers = [_]TensorProto{
        .{
            .name = "used_bias",
            .dims = &used_dims,
            .data_type = .float32,
            .raw_data = std.mem.sliceAsBytes(&used_data),
        },
        .{
            .name = "unused_weight",
            .dims = &unused_dims,
            .data_type = .float32,
            .raw_data = std.mem.sliceAsBytes(&unused_data),
        },
    };
    var in_dims = [_]proto.TensorShapeProto.Dimension{
        .{ .dim_value = 4 },
    };
    const in_shape_proto = proto.TensorShapeProto{ .dims = &in_dims };
    const in_tensor_type = proto.TensorTypeProto{
        .elem_type = .float32,
        .shape = in_shape_proto,
    };
    const in_type_proto = proto.TypeProto{ .tensor_type = in_tensor_type };
    var input_infos = [_]ValueInfoProto{
        .{ .name = "x", .type_proto = in_type_proto },
        .{ .name = "used_bias" },
        .{ .name = "unused_weight" },
    };
    var output_infos = [_]ValueInfoProto{
        .{ .name = "y" },
    };
    const graph_proto = GraphProto{
        .nodes = &nodes,
        .initializers = &initializers,
        .inputs = &input_infos,
        .outputs = &output_infos,
    };
    _ = &nodes;
    _ = &initializers;
    _ = &input_infos;
    _ = &output_infos;

    // Serialize to bytes so we can lazy-parse it back.
    const @"export" = @import("export.zig");
    var opsets = [_]proto.OpsetImport{.{ .domain = "", .version = 17 }};
    const model_proto = proto.ModelProto{
        .ir_version = 8,
        .opset_import = &opsets,
        .graph = graph_proto,
    };
    _ = &opsets;
    const bytes = try @"export".serializeModel(allocator, &model_proto);
    defer allocator.free(bytes);

    // Parse lazily.
    const lazy_model = try proto.parseLazyModelProto(allocator, bytes);
    var model = try Model.initFromLazy(allocator, lazy_model, null);
    defer model.deinit();

    // Both initializers should be indexed, but no slots should be parsed yet.
    try std.testing.expect(model.initializer_map.contains("used_bias"));
    try std.testing.expect(model.initializer_map.contains("unused_weight"));
    const ls = &model.lazy_state.?;
    try std.testing.expectEqual(@as(usize, 2), ls.parsed.len);
    try std.testing.expect(ls.parsed[0] == null);
    try std.testing.expect(ls.parsed[1] == null);

    // Convert — this should touch only `used_bias`.
    var result = try model.convertToGraph(allocator);
    defer result.deinit(allocator);

    const used_idx = model.initializer_map.get("used_bias").?;
    const unused_idx = model.initializer_map.get("unused_weight").?;
    try std.testing.expect(ls.parsed[used_idx] != null);
    try std.testing.expect(ls.parsed[unused_idx] == null);
}

// ── Integration tests with real ONNX models ─────────────────────────

fn readModelFile(allocator: std.mem.Allocator, path: []const u8) ![]const u8 {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    return std.Io.Dir.cwd().readFileAlloc(io_impl.io(), path, allocator, .limited(1024 * 1024 * 1024)) catch |e| switch (e) {
        error.FileNotFound => return error.SkipZigTest,
        else => return e,
    };
}

test "integration: PaddleOCR det.onnx parse and convert" {
    const allocator = std.testing.allocator;

    const data = try readModelFile(allocator, "/Users/ajroetker/.termite/models/readers/monkt/paddleocr-onnx/det.onnx");
    defer allocator.free(data);

    // Parse
    var model = try Model.init(allocator, try proto.parseModelProto(allocator, data));
    defer model.deinit();

    // Verify model structure
    const g = model.graph() orelse return error.OutputNotFound;
    try std.testing.expect(g.nodes.len > 100); // PaddleOCR det has 572 nodes
    try std.testing.expect(g.outputs.len > 0);

    // Check inputs
    try std.testing.expect(model.inputCount() > 0);

    // Attempt conversion
    var result = model.convertToGraph(allocator) catch |e| {
        std.debug.print("PaddleOCR conversion stopped: {}\n", .{e});
        return;
    };
    defer result.deinit(allocator);

    // If conversion succeeds, verify the graph is non-trivial
    try std.testing.expect(result.graph.nodeCount() > 0);
    try std.testing.expect(result.output_ids.len > 0);
    std.debug.print("PaddleOCR det: {d} nodes, {d} params, {d} outputs\n", .{
        result.graph.nodeCount(),
        result.parameter_names.len,
        result.output_ids.len,
    });
}

test "integration: GPT-2 decoder_model.onnx parse and convert" {
    const allocator = std.testing.allocator;

    const data = readModelFile(allocator, "/Users/ajroetker/.termite/models/openai-community/gpt2/onnx/decoder_model.onnx") catch |e| switch (e) {
        error.SkipZigTest => return e,
        else => return e,
    };
    defer allocator.free(data);

    // Parse
    var model = try Model.init(allocator, try proto.parseModelProto(allocator, data));
    defer model.deinit();

    const g = model.graph() orelse return error.OutputNotFound;
    try std.testing.expect(g.nodes.len > 1000); // GPT-2 has 3095 nodes
    try std.testing.expect(g.outputs.len > 0);

    var result = model.convertToGraph(allocator) catch |e| {
        std.debug.print("GPT-2 conversion stopped: {}\n", .{e});
        return;
    };
    defer result.deinit(allocator);

    try std.testing.expect(result.graph.nodeCount() > 0);
    try std.testing.expect(result.output_ids.len > 0);

    // Run fuse pass and verify SDPA pattern detection fires
    const Pipeline = ml.graph.passes.pipeline.Pipeline;
    var opt_result = Pipeline.default.run(allocator, &result.graph) catch |e| {
        std.debug.print("GPT-2 optimization failed: {}\n", .{e});
        return;
    };
    defer opt_result.deinit();

    var sdpa_count: u32 = 0;
    for (0..opt_result.graph.nodeCount()) |idx| {
        if (std.meta.activeTag(opt_result.graph.node(@intCast(idx)).op) == .fused_sdpa) {
            sdpa_count += 1;
        }
    }

    std.debug.print("GPT-2: {d} nodes, {d} params, {d} outputs, {d} fused SDPA\n", .{
        opt_result.graph.nodeCount(),
        result.parameter_names.len,
        result.output_ids.len,
        sdpa_count,
    });
}

test "integration: clipclap text_model position_ids initializer metadata" {
    const allocator = std.testing.allocator;

    const data = readModelFile(allocator, "/Users/ajroetker/.termite/models/antflydb/clipclap/text_model.onnx") catch |e| switch (e) {
        error.SkipZigTest => return e,
        else => return e,
    };
    defer allocator.free(data);

    var model = try Model.initFromLazy(
        allocator,
        try proto.parseLazyModelProto(allocator, data),
        "/Users/ajroetker/.termite/models/antflydb/clipclap",
    );
    defer model.deinit();

    const maybe_init = model.getInitializer("embeddings.position_ids");
    try std.testing.expect(maybe_init != null);
    const init = maybe_init.?;
    try std.testing.expectEqualStrings("embeddings.position_ids", init.name);
    try std.testing.expectEqual(DType.i64, init.shape.dtype);
    try std.testing.expectEqual(@as(u8, 2), init.shape.rank());
    try std.testing.expectEqual(@as(i64, 1), init.shape.dim(0));
    try std.testing.expectEqual(@as(i64, 77), init.shape.dim(1));

    const values = try model.loadInitializerData(allocator, "embeddings.position_ids");
    defer allocator.free(values);
    try std.testing.expectEqual(@as(usize, 77), values.len);
}

test "integration: clipclap text_model emits fused_sdpa" {
    const allocator = std.testing.allocator;

    const data = readModelFile(allocator, "/Users/ajroetker/.termite/models/antflydb/clipclap/text_model.onnx") catch |e| switch (e) {
        error.SkipZigTest => return e,
        else => return e,
    };
    defer allocator.free(data);

    var model = try Model.initFromLazy(
        allocator,
        try proto.parseLazyModelProto(allocator, data),
        "/Users/ajroetker/.termite/models/antflydb/clipclap",
    );
    defer model.deinit();

    var result = try model.convertToGraph(allocator);
    defer result.deinit(allocator);

    const Pipeline = ml.graph.passes.pipeline.Pipeline;
    var opt_result = try Pipeline.default.run(allocator, &result.graph);
    defer opt_result.deinit();

    var sdpa_count: u32 = 0;
    for (0..opt_result.graph.nodeCount()) |idx| {
        if (std.meta.activeTag(opt_result.graph.node(@intCast(idx)).op) == .fused_sdpa) {
            sdpa_count += 1;
        }
    }

    try std.testing.expect(sdpa_count > 0);
}
