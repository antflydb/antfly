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

// ONNX export — serialize termite Graph IR to ONNX protobuf format.
//
// Two layers:
//   1. Proto serialization: ONNX proto structs → protobuf wire bytes
//   2. Graph conversion:    termite Graph → ONNX proto structs → bytes
//
// Uses the shared protobuf wire primitives.

const std = @import("std");
const ml = @import("ml");
const message = @import("protobuf").message;
const proto = @import("proto.zig");

const Allocator = std.mem.Allocator;

const Graph = ml.graph.Graph;
const Node = ml.graph.Node;
const NodeId = ml.graph.NodeId;
const null_node = ml.graph.null_node;
const OpCode = ml.graph.Node.OpCode;
const Shape = ml.graph.Shape;
const DType = ml.graph.DType;

const ModelProto = proto.ModelProto;
const GraphProto = proto.GraphProto;
const NodeProto = proto.NodeProto;
const TensorProto = proto.TensorProto;
const AttributeProto = proto.AttributeProto;
const ValueInfoProto = proto.ValueInfoProto;
const TensorShapeProto = proto.TensorShapeProto;
const DataType = proto.DataType;

// ── Proto Serialization ─────────────────────────────────────────────
//
// All proto encode/decode goes through `lib/protobuf/src/message.zig`:
// each ONNX struct declares a `_pb_field_map` in proto.zig, and the
// comptime runtime handles the wire-format details (packed-or-repeated
// detection, sub-message length prefixes, zero-default skipping, etc.).
//
// `exportGraph` below still assembles the ModelProto struct tree and
// owns the allocations for names/dims/shapes; serializeModel is just a
// thin wrapper that hands the assembled struct to the runtime encoder.

/// Serialize a ModelProto to protobuf bytes.
pub fn serializeModel(alloc: Allocator, model: *const ModelProto) ![]u8 {
    return message.encode(ModelProto, alloc, model);
}

// ── Graph IR → ONNX Proto ───────────────────────────────────────────

/// Options for graph export.
pub const ExportOptions = struct {
    /// ONNX IR version (default 8 = ONNX 1.13+).
    ir_version: u64 = 8,
    /// Default opset version (default 17).
    opset_version: u64 = 17,
    /// Graph name.
    graph_name: []const u8 = "inference_graph",
    /// When true, decompose fused ops (gelu, silu, rms_norm, linear, etc.)
    /// into standard ONNX primitives via the lowering pass before export.
    /// This produces more portable ONNX models at the cost of larger graphs.
    lower_fused: bool = false,
    /// Optional weight parameter materialization. Parameter nodes whose names
    /// match one of these entries are exported as initializers instead of graph
    /// inputs.
    parameter_initializers: ?[]const ParameterInitializer = null,
    /// Optional lazy parameter initializer provider. Used to avoid holding
    /// all exported parameter weights in memory at once during large exports.
    parameter_initializer_provider: ?ParameterInitializerProvider = null,
    /// Optional initializer reference provider. Used when regenerating an ONNX
    /// model protobuf that should point at an existing external-data artifact
    /// instead of writing initializer payload bytes again.
    parameter_initializer_reference_provider: ?ParameterInitializerReferenceProvider = null,
    /// Optional relative filename for external initializer data.
    /// When set, constant initializers are emitted via TensorProto.external_data
    /// into a single sidecar blob instead of inline raw_data.
    external_data_location: ?[]const u8 = null,
    /// Optional node-output name overrides, keyed by the graph node id after
    /// any requested lowering. Overrides rename the value everywhere it is
    /// referenced, including graph inputs, node inputs, node outputs, and graph
    /// outputs.
    node_name_overrides: []const NodeNameOverride = &.{},
    /// Optional decoder ABI bindings for semantic causal attention nodes.
    /// These synthesize `past_key_values.*` graph inputs and `present.*` graph
    /// outputs around the traced current-token K/V projections.
    semantic_decoder_gqa_bindings: []const SemanticDecoderGqaBinding = &.{},
};

pub const NodeNameOverride = struct {
    node_id: NodeId,
    name: []const u8,
};

pub const SemanticDecoderGqaBinding = struct {
    node_id: NodeId,
    layer_index: u32,
    skip_kv_write: bool = false,
};

pub const ParameterInitializer = struct {
    name: []const u8,
    shape: Shape,
    data: union(enum) {
        f32: []const f32,
        raw_bytes: []const u8,
        streamed: StreamedTensorData,
        q8_0_block: Q8_0BlockData,
    },
};

pub const Q8_0BlockData = struct {
    scale_shape: Shape,
    values_u8: []const u8,
    scales_f32: []const f32,
    zero_point_u8: u8,
    axis: i64,
    block_size: i64,
    source_byte_len: usize,
};

pub const ByteSink = struct {
    context: ?*anyopaque = null,
    write: *const fn (context: ?*anyopaque, bytes: []const u8) anyerror!void,
};

pub const StreamedTensorData = struct {
    storage_kind_tag: enum { dense_native, quantized_dequantized_f32 },
    source_byte_len: usize,
    byte_len: usize,
    context: ?*anyopaque = null,
    write_all: *const fn (context: ?*anyopaque, allocator: Allocator, sink: ByteSink) anyerror!void,
    deinit: *const fn (context: ?*anyopaque, allocator: Allocator) void,
};

pub fn freeStreamedTensorData(alloc: Allocator, data: StreamedTensorData) void {
    data.deinit(data.context, alloc);
}

fn freeQ8_0BlockData(alloc: Allocator, data: Q8_0BlockData) void {
    alloc.free(data.values_u8);
    alloc.free(data.scales_f32);
}

pub const ParameterInitializerProvider = struct {
    context: ?*anyopaque = null,
    load: *const fn (context: ?*anyopaque, allocator: Allocator, name: []const u8) anyerror!?ParameterInitializer,
    free: *const fn (context: ?*anyopaque, allocator: Allocator, init: *const ParameterInitializer) void,
};

pub const Q8_0BlockInitializerReference = struct {
    values: TensorProto,
    scales: TensorProto,
    zero_point: TensorProto,
    axis: i64,
    block_size: i64,
};

pub const ParameterInitializerReference = union(enum) {
    tensor: TensorProto,
    q8_0_block: Q8_0BlockInitializerReference,
};

pub const ParameterInitializerReferenceProvider = struct {
    context: ?*anyopaque = null,
    load: *const fn (context: ?*anyopaque, allocator: Allocator, name: []const u8) anyerror!?ParameterInitializerReference,
};

pub const ExternalDataFile = struct {
    relative_path: []u8,
    bytes: []u8,
};

pub const ExportResult = struct {
    model_bytes: []u8,
    external_data: ?ExternalDataFile = null,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.model_bytes);
        if (self.external_data) |data| {
            alloc.free(data.relative_path);
            alloc.free(data.bytes);
        }
        self.* = undefined;
    }
};

pub const StreamedExportResult = struct {
    model_bytes: []u8,
    relative_external_path: []u8,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.model_bytes);
        alloc.free(self.relative_external_path);
        self.* = undefined;
    }
};

/// Map termite DType to ONNX DataType.
fn termiteDTypeToOnnx(dtype: DType) DataType {
    return switch (dtype) {
        .f32 => .float32,
        .f16 => .float16,
        .bf16 => .bfloat16,
        .f64 => .float64,
        .i8 => .int8,
        .i16 => .int16,
        .i32 => .int32,
        .i64 => .int64,
        .u8 => .uint8,
        .bool_ => .bool_,
    };
}

/// Build a ValueInfoProto from a name and shape.
/// The returned dims slice is allocated and must be freed by the caller.
fn makeValueInfo(alloc: Allocator, name: []const u8, shape: Shape) !ValueInfoProto {
    const rank = shape.rank();
    const dims = try alloc.alloc(TensorShapeProto.Dimension, rank);
    for (0..rank) |i| {
        const d = shape.dim(@intCast(i));
        if (d < 0) {
            // Dynamic dimension — use dim_param with synthetic name, dim_value unset.
            dims[i] = .{ .dim_value = null, .dim_param = try std.fmt.allocPrint(alloc, "d_{s}_{d}", .{ name, i }) };
        } else {
            dims[i] = .{ .dim_value = d };
        }
    }
    return .{
        .name = name,
        .type_proto = .{
            .tensor_type = .{
                .elem_type = termiteDTypeToOnnx(shape.dtype),
                .shape = .{ .dims = dims },
            },
        },
    };
}

/// Build a TensorProto initializer from constant f32 data.
fn makeConstantTensor(alloc: Allocator, name: []const u8, shape: Shape, data: []const f32) !TensorProto {
    const dims = try alloc.alloc(i64, shape.rank());
    for (0..shape.rank()) |i| dims[i] = shape.dim(@intCast(i));
    return .{
        .name = name,
        .dims = dims,
        .data_type = termiteDTypeToOnnx(shape.dtype),
        .raw_data = std.mem.sliceAsBytes(data),
    };
}

fn makeRawConstantTensor(alloc: Allocator, name: []const u8, shape: Shape, data: []const u8) !TensorProto {
    const dims = try alloc.alloc(i64, shape.rank());
    for (0..shape.rank()) |i| dims[i] = shape.dim(@intCast(i));
    return .{
        .name = name,
        .dims = dims,
        .data_type = termiteDTypeToOnnx(shape.dtype),
        .raw_data = data,
    };
}

fn graphConstantByteLen(dtype: DType, elem_count: u32) !u32 {
    const bytes = std.math.mul(usize, @intCast(elem_count), dtype.byteSize()) catch return error.UnsupportedShape;
    if (bytes > std.math.maxInt(u32)) return error.UnsupportedShape;
    return @intCast(bytes);
}

fn f32SliceToF16Bytes(alloc: Allocator, data: []const f32) ![]u8 {
    const out = try alloc.alloc(u8, data.len * @sizeOf(u16));
    errdefer alloc.free(out);
    for (data, 0..) |value, i| {
        const half_bits: u16 = @bitCast(@as(f16, @floatCast(value)));
        out[i * 2] = @truncate(half_bits);
        out[i * 2 + 1] = @truncate(half_bits >> 8);
    }
    return out;
}

fn f32SliceToBf16Bytes(alloc: Allocator, data: []const f32) ![]u8 {
    const out = try alloc.alloc(u8, data.len * @sizeOf(u16));
    errdefer alloc.free(out);
    for (data, 0..) |value, i| {
        const bits: u32 = @bitCast(value);
        const bf16_bits: u16 = @truncate(bits >> 16);
        out[i * 2] = @truncate(bf16_bits);
        out[i * 2 + 1] = @truncate(bf16_bits >> 8);
    }
    return out;
}

fn writeExternalTensorData(
    alloc: Allocator,
    ext: *ExternalDataBuilder,
    shape: Shape,
    data: []const f32,
) !usize {
    switch (shape.dtype) {
        .f16 => {
            const raw = try f32SliceToF16Bytes(alloc, data);
            defer alloc.free(raw);
            try writeExternalBytes(alloc, ext, raw);
            return raw.len;
        },
        .bf16 => {
            const raw = try f32SliceToBf16Bytes(alloc, data);
            defer alloc.free(raw);
            try writeExternalBytes(alloc, ext, raw);
            return raw.len;
        },
        else => {
            const raw = std.mem.sliceAsBytes(data);
            try writeExternalBytes(alloc, ext, raw);
            return raw.len;
        },
    }
}

fn findParameterInitializer(opts: ExportOptions, name: []const u8) ?ParameterInitializer {
    const inits = opts.parameter_initializers orelse return null;
    for (inits) |init| {
        if (std.mem.eql(u8, init.name, name)) return init;
    }
    return null;
}

fn findNodeNameOverride(opts: ExportOptions, node_id: NodeId) ?[]const u8 {
    for (opts.node_name_overrides) |override| {
        if (override.node_id == node_id) return override.name;
    }
    return null;
}

fn findSemanticDecoderGqaBinding(opts: ExportOptions, node_id: NodeId) ?SemanticDecoderGqaBinding {
    for (opts.semantic_decoder_gqa_bindings) |binding| {
        if (binding.node_id == node_id) return binding;
    }
    return null;
}

fn shapeElementCount(shape: Shape) !usize {
    var count: usize = 1;
    for (0..shape.rank()) |i| {
        const dim = shape.dim(@intCast(i));
        if (dim <= 0) return error.UnsupportedShape;
        count *= @intCast(dim);
    }
    return count;
}

const ExternalDataBuilder = struct {
    relative_path: []u8,
    bytes: std.ArrayListUnmanaged(u8) = .empty,
    file: ?std.Io.File = null,
    offset: usize = 0,

    fn deinit(self: *@This(), alloc: Allocator) void {
        if (self.file) |*file| file.close(std.Io.Threaded.global_single_threaded.io());
        alloc.free(self.relative_path);
        self.bytes.deinit(alloc);
        self.* = undefined;
    }
};

fn createExternalDataFile(path: []const u8) !std.Io.File {
    const io = std.Io.Threaded.global_single_threaded.io();
    if (std.fs.path.isAbsolute(path)) {
        return std.Io.Dir.createFileAbsolute(io, path, .{ .truncate = true });
    }
    return std.Io.Dir.cwd().createFile(io, path, .{ .truncate = true });
}

fn writeExternalBytes(
    alloc: Allocator,
    ext: *ExternalDataBuilder,
    bytes: []const u8,
) !void {
    if (ext.file) |*file| {
        try file.writeStreamingAll(std.Io.Threaded.global_single_threaded.io(), bytes);
        return;
    }
    try ext.bytes.appendSlice(alloc, bytes);
}

fn makeExternalConstantTensor(
    alloc: Allocator,
    ext: *ExternalDataBuilder,
    name: []const u8,
    shape: Shape,
    data: []const f32,
) !TensorProto {
    const dims = try alloc.alloc(i64, shape.rank());
    errdefer alloc.free(dims);
    for (0..shape.rank()) |i| dims[i] = shape.dim(@intCast(i));

    const offset = ext.offset;
    const length = try writeExternalTensorData(alloc, ext, shape, data);
    ext.offset += length;

    const entries = try alloc.alloc(proto.ExternalDataEntry, 3);
    errdefer alloc.free(entries);
    entries[0] = .{
        .key = try alloc.dupe(u8, "location"),
        .value = try alloc.dupe(u8, ext.relative_path),
    };
    entries[1] = .{
        .key = try alloc.dupe(u8, "offset"),
        .value = try std.fmt.allocPrint(alloc, "{d}", .{offset}),
    };
    entries[2] = .{
        .key = try alloc.dupe(u8, "length"),
        .value = try std.fmt.allocPrint(alloc, "{d}", .{length}),
    };

    return .{
        .name = name,
        .dims = dims,
        .data_type = termiteDTypeToOnnx(shape.dtype),
        .external_data = entries,
        .data_location = .external,
    };
}

fn makeExternalRawConstantTensor(
    alloc: Allocator,
    ext: *ExternalDataBuilder,
    name: []const u8,
    shape: Shape,
    data: []const u8,
) !TensorProto {
    const dims = try alloc.alloc(i64, shape.rank());
    errdefer alloc.free(dims);
    for (0..shape.rank()) |i| dims[i] = shape.dim(@intCast(i));

    const offset = ext.offset;
    try writeExternalBytes(alloc, ext, data);
    ext.offset += data.len;

    const entries = try alloc.alloc(proto.ExternalDataEntry, 3);
    errdefer alloc.free(entries);
    entries[0] = .{
        .key = try alloc.dupe(u8, "location"),
        .value = try alloc.dupe(u8, ext.relative_path),
    };
    entries[1] = .{
        .key = try alloc.dupe(u8, "offset"),
        .value = try std.fmt.allocPrint(alloc, "{d}", .{offset}),
    };
    entries[2] = .{
        .key = try alloc.dupe(u8, "length"),
        .value = try std.fmt.allocPrint(alloc, "{d}", .{data.len}),
    };

    return .{
        .name = name,
        .dims = dims,
        .data_type = termiteDTypeToOnnx(shape.dtype),
        .external_data = entries,
        .data_location = .external,
    };
}

fn makeExternalStreamedConstantTensor(
    alloc: Allocator,
    ext: *ExternalDataBuilder,
    name: []const u8,
    shape: Shape,
    stream: StreamedTensorData,
) !TensorProto {
    const dims = try alloc.alloc(i64, shape.rank());
    errdefer alloc.free(dims);
    for (0..shape.rank()) |i| dims[i] = shape.dim(@intCast(i));

    const offset = ext.offset;
    var bytes_written: usize = 0;
    const SinkContext = struct {
        ext: *ExternalDataBuilder,
        io_alloc: Allocator,
        bytes_written: *usize,

        fn write(raw_context: ?*anyopaque, bytes: []const u8) anyerror!void {
            const context = raw_context orelse return error.InvalidState;
            const self: *@This() = @ptrCast(@alignCast(context));
            try writeExternalBytes(self.io_alloc, self.ext, bytes);
            self.bytes_written.* += bytes.len;
        }
    };

    var sink_context = SinkContext{
        .ext = ext,
        .io_alloc = alloc,
        .bytes_written = &bytes_written,
    };
    const sink: ByteSink = .{
        .context = &sink_context,
        .write = &SinkContext.write,
    };
    try stream.write_all(stream.context, alloc, sink);
    if (bytes_written != stream.byte_len) return error.InvalidTensorBytes;
    ext.offset += bytes_written;

    const entries = try alloc.alloc(proto.ExternalDataEntry, 3);
    errdefer alloc.free(entries);
    entries[0] = .{
        .key = try alloc.dupe(u8, "location"),
        .value = try alloc.dupe(u8, ext.relative_path),
    };
    entries[1] = .{
        .key = try alloc.dupe(u8, "offset"),
        .value = try std.fmt.allocPrint(alloc, "{d}", .{offset}),
    };
    entries[2] = .{
        .key = try alloc.dupe(u8, "length"),
        .value = try std.fmt.allocPrint(alloc, "{d}", .{bytes_written}),
    };

    return .{
        .name = name,
        .dims = dims,
        .data_type = termiteDTypeToOnnx(shape.dtype),
        .external_data = entries,
        .data_location = .external,
    };
}

fn appendQ8_0BlockParameter(
    alloc: Allocator,
    external_data_builder: ?*ExternalDataBuilder,
    initializers: *std.ArrayListUnmanaged(TensorProto),
    onnx_nodes: *std.ArrayListUnmanaged(NodeProto),
    extra_names: *std.ArrayListUnmanaged([]u8),
    parameter_name: []const u8,
    output_shape: Shape,
    data: Q8_0BlockData,
) !void {
    const values_name = try std.fmt.allocPrint(alloc, "{s}__q8_values", .{parameter_name});
    errdefer alloc.free(values_name);
    const scales_name = try std.fmt.allocPrint(alloc, "{s}__q8_scales", .{parameter_name});
    errdefer alloc.free(scales_name);
    const zp_name = try std.fmt.allocPrint(alloc, "{s}__q8_zero_point", .{parameter_name});
    errdefer alloc.free(zp_name);
    try extra_names.append(alloc, values_name);
    try extra_names.append(alloc, scales_name);
    try extra_names.append(alloc, zp_name);

    const values_shape = Shape.init(.u8, output_shape.dims[0..output_shape.rank()]);
    const values_tensor = if (external_data_builder) |builder|
        try makeExternalRawConstantTensor(alloc, builder, values_name, values_shape, data.values_u8)
    else
        try makeRawConstantTensor(alloc, values_name, values_shape, data.values_u8);
    try initializers.append(alloc, values_tensor);

    const scales_bytes = std.mem.sliceAsBytes(data.scales_f32);
    const scales_tensor = if (external_data_builder) |builder|
        try makeExternalRawConstantTensor(alloc, builder, scales_name, data.scale_shape, scales_bytes)
    else
        try makeRawConstantTensor(alloc, scales_name, data.scale_shape, scales_bytes);
    try initializers.append(alloc, scales_tensor);

    const zp_elems = try shapeElementCount(data.scale_shape);
    const zero_points = try alloc.alloc(u8, zp_elems);
    defer alloc.free(zero_points);
    @memset(zero_points, data.zero_point_u8);
    const zp_tensor = if (external_data_builder) |builder|
        try makeExternalRawConstantTensor(alloc, builder, zp_name, Shape.init(.u8, data.scale_shape.dims[0..data.scale_shape.rank()]), zero_points)
    else
        try makeRawConstantTensor(alloc, zp_name, Shape.init(.u8, data.scale_shape.dims[0..data.scale_shape.rank()]), zero_points);
    try initializers.append(alloc, zp_tensor);

    const inp_names = try alloc.alloc([]const u8, 3);
    errdefer alloc.free(inp_names);
    inp_names[0] = values_name;
    inp_names[1] = scales_name;
    inp_names[2] = zp_name;

    const out_names = try alloc.alloc([]const u8, 1);
    errdefer alloc.free(out_names);
    out_names[0] = parameter_name;

    const attrs = try alloc.alloc(AttributeProto, 2);
    errdefer alloc.free(attrs);
    attrs[0] = .{ .name = "axis", .i = data.axis, .attr_type = .int };
    attrs[1] = .{ .name = "block_size", .i = data.block_size, .attr_type = .int };

    try onnx_nodes.append(alloc, .{
        .inputs = inp_names,
        .outputs = out_names,
        .name = parameter_name,
        .op_type = "DequantizeLinear",
        .attributes = attrs,
    });
}

fn appendQ8_0BlockParameterReference(
    alloc: Allocator,
    initializers: *std.ArrayListUnmanaged(TensorProto),
    onnx_nodes: *std.ArrayListUnmanaged(NodeProto),
    parameter_name: []const u8,
    reference: Q8_0BlockInitializerReference,
) !void {
    var values = reference.values;
    var values_moved = false;
    defer if (!values_moved) freeTensorProto(alloc, &values);
    try initializers.append(alloc, values);
    values_moved = true;

    var scales = reference.scales;
    var scales_moved = false;
    defer if (!scales_moved) freeTensorProto(alloc, &scales);
    try initializers.append(alloc, scales);
    scales_moved = true;

    var zero_point = reference.zero_point;
    var zero_point_moved = false;
    defer if (!zero_point_moved) freeTensorProto(alloc, &zero_point);
    try initializers.append(alloc, zero_point);
    zero_point_moved = true;

    const inp_names = try alloc.alloc([]const u8, 3);
    errdefer alloc.free(inp_names);
    inp_names[0] = initializers.items[initializers.items.len - 3].name;
    inp_names[1] = initializers.items[initializers.items.len - 2].name;
    inp_names[2] = initializers.items[initializers.items.len - 1].name;

    const out_names = try alloc.alloc([]const u8, 1);
    errdefer alloc.free(out_names);
    out_names[0] = parameter_name;

    const attrs = try alloc.alloc(AttributeProto, 2);
    errdefer alloc.free(attrs);
    attrs[0] = .{ .name = "axis", .i = reference.axis, .attr_type = .int };
    attrs[1] = .{ .name = "block_size", .i = reference.block_size, .attr_type = .int };

    try onnx_nodes.append(alloc, .{
        .inputs = inp_names,
        .outputs = out_names,
        .name = parameter_name,
        .op_type = "DequantizeLinear",
        .attributes = attrs,
    });
}

/// Export a termite Graph to ONNX ModelProto protobuf bytes.
/// Caller owns the returned slice.
pub fn exportGraph(alloc: Allocator, graph: *const Graph, opts: ExportOptions) ![]u8 {
    var result = try exportGraphResult(alloc, graph, opts);
    if (result.external_data != null) {
        result.deinit(alloc);
        return error.UnsupportedShape;
    }
    const model_bytes = result.model_bytes;
    result.model_bytes = &.{};
    return model_bytes;
}

pub fn exportGraphWithExternalData(
    alloc: Allocator,
    graph: *const Graph,
    opts: ExportOptions,
    relative_path: []const u8,
) !ExportResult {
    var effective_opts = opts;
    effective_opts.external_data_location = relative_path;
    return exportGraphResult(alloc, graph, effective_opts);
}

pub fn exportGraphWithExternalDataToPath(
    alloc: Allocator,
    graph: *const Graph,
    opts: ExportOptions,
    relative_path: []const u8,
    absolute_path: []const u8,
) !StreamedExportResult {
    var effective_opts = opts;
    effective_opts.external_data_location = relative_path;
    var result = try exportGraphResultToPath(alloc, graph, effective_opts, absolute_path);
    errdefer result.deinit(alloc);
    if (result.external_data != null) return error.UnsupportedShape;
    return .{
        .model_bytes = result.model_bytes,
        .relative_external_path = try alloc.dupe(u8, relative_path),
    };
}

fn exportGraphResult(alloc: Allocator, graph: *const Graph, opts: ExportOptions) !ExportResult {
    return exportGraphResultMaybeStream(alloc, graph, opts, null);
}

fn exportGraphResultToPath(
    alloc: Allocator,
    graph: *const Graph,
    opts: ExportOptions,
    absolute_path: []const u8,
) !ExportResult {
    return exportGraphResultMaybeStream(alloc, graph, opts, absolute_path);
}

fn exportGraphResultMaybeStream(
    alloc: Allocator,
    graph: *const Graph,
    opts: ExportOptions,
    external_absolute_path: ?[]const u8,
) !ExportResult {
    // If lower_fused is requested, decompose fused ops into primitives first.
    var lowered: ?ml.graph.lower.LowerResult = null;
    defer if (lowered) |*l| l.deinit();

    const effective_graph: *const Graph = if (opts.lower_fused) blk: {
        lowered = ml.graph.lower.lower(alloc, graph) catch |e| switch (e) {
            error.OutOfMemory => return error.OutOfMemory,
        };
        break :blk &lowered.?.graph;
    } else graph;

    // Phase 1: Name every node output.
    //   - parameters → their stored name
    //   - constants  → "const_<id>"
    //   - ops        → "node_<id>"
    const count = effective_graph.nodeCount();
    var names = try alloc.alloc([]const u8, count);
    defer {
        for (names) |n| alloc.free(n);
        alloc.free(names);
    }

    for (0..count) |i| {
        const n = effective_graph.node(@intCast(i));
        names[i] = if (findNodeNameOverride(opts, @intCast(i))) |override_name|
            try alloc.dupe(u8, override_name)
        else switch (n.op) {
            .parameter => |p| try alloc.dupe(u8, effective_graph.string_table.items[p.name_offset..][0..p.name_len]),
            .constant => try std.fmt.allocPrint(alloc, "const_{d}", .{i}),
            else => try std.fmt.allocPrint(alloc, "node_{d}", .{i}),
        };
    }

    // Phase 2: Build ONNX nodes, inputs, outputs, initializers.
    var effective_opset_version = opts.opset_version;

    var onnx_nodes = std.ArrayListUnmanaged(NodeProto).empty;
    defer {
        for (onnx_nodes.items) |*n| freeNodeProto(alloc, n);
        onnx_nodes.deinit(alloc);
    }

    var initializers = std.ArrayListUnmanaged(TensorProto).empty;
    defer {
        for (initializers.items) |*t| freeTensorProto(alloc, t);
        initializers.deinit(alloc);
    }

    var input_infos = std.ArrayListUnmanaged(ValueInfoProto).empty;
    defer {
        for (input_infos.items) |*vi| freeValueInfoDims(alloc, vi);
        input_infos.deinit(alloc);
    }

    var semantic_output_infos = std.ArrayListUnmanaged(ValueInfoProto).empty;
    defer {
        for (semantic_output_infos.items) |*vi| freeValueInfoDims(alloc, vi);
        semantic_output_infos.deinit(alloc);
    }

    // Extra allocations emitted by convertOpToNode for ops whose parameters
    // ONNX expects as tensor inputs (Reshape/Expand shape tensors). These
    // aren't covered by the generic initializer cleanup above because their
    // name and raw_data backing buffer are freshly allocated here rather
    // than borrowed from the Graph.
    var shape_extras = std.ArrayListUnmanaged(ShapeExtra).empty;
    defer {
        for (shape_extras.items) |e| {
            alloc.free(e.name);
            alloc.free(e.raw_data);
        }
        shape_extras.deinit(alloc);
    }

    var quant_extras = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (quant_extras.items) |name| alloc.free(name);
        quant_extras.deinit(alloc);
    }

    var zero_tensor_extras = std.ArrayListUnmanaged([]f32).empty;
    defer {
        for (zero_tensor_extras.items) |zeros| alloc.free(zeros);
        zero_tensor_extras.deinit(alloc);
    }

    var tensor_data_extras = std.ArrayListUnmanaged(TensorDataExtra).empty;
    defer {
        for (tensor_data_extras.items) |extra| {
            alloc.free(extra.name);
            alloc.free(extra.raw_data);
        }
        tensor_data_extras.deinit(alloc);
    }

    var emitted_parameter_names = std.StringHashMapUnmanaged(void).empty;
    defer emitted_parameter_names.deinit(alloc);

    var pair_second_outputs = std.AutoHashMapUnmanaged(NodeId, []const u8).empty;
    defer pair_second_outputs.deinit(alloc);

    var external_data_builder: ?ExternalDataBuilder = if (opts.external_data_location) |path| blk: {
        var builder = ExternalDataBuilder{ .relative_path = try alloc.dupe(u8, path) };
        if (external_absolute_path) |abs_path| {
            builder.file = try createExternalDataFile(abs_path);
        }
        break :blk builder;
    } else null;
    defer if (external_data_builder) |*builder| builder.deinit(alloc);

    for (0..count) |i| {
        const n = effective_graph.node(@intCast(i));
        switch (n.op) {
            .reduce_sum, .reduce_max, .reduce_mean => effective_opset_version = @max(effective_opset_version, 21),
            else => {},
        }
        switch (n.op) {
            .parameter, .fused_from_float32 => {
                const gop = try emitted_parameter_names.getOrPut(alloc, names[i]);
                if (gop.found_existing) continue;
                gop.value_ptr.* = {};
                if (opts.parameter_initializer_reference_provider) |provider| {
                    if (try provider.load(provider.context, alloc, names[i])) |reference| {
                        switch (reference) {
                            .tensor => |tensor_ref| {
                                var tensor = tensor_ref;
                                var moved = false;
                                defer if (!moved) freeTensorProto(alloc, &tensor);
                                try initializers.append(alloc, tensor);
                                moved = true;
                            },
                            .q8_0_block => |q8_ref| {
                                try appendQ8_0BlockParameterReference(
                                    alloc,
                                    &initializers,
                                    &onnx_nodes,
                                    names[i],
                                    q8_ref,
                                );
                                effective_opset_version = @max(effective_opset_version, 21);
                            },
                        }
                    } else {
                        try input_infos.append(alloc, try makeValueInfo(alloc, names[i], n.output_shape));
                    }
                } else if (findParameterInitializer(opts, names[i])) |init| {
                    switch (init.data) {
                        .q8_0_block => |data| try appendQ8_0BlockParameter(
                            alloc,
                            if (external_data_builder) |*builder| builder else return error.UnsupportedShape,
                            &initializers,
                            &onnx_nodes,
                            &quant_extras,
                            names[i],
                            init.shape,
                            data,
                        ),
                        else => {
                            const tensor = if (external_data_builder) |*builder| switch (init.data) {
                                .f32 => |data| try makeExternalConstantTensor(alloc, builder, names[i], init.shape, data),
                                .raw_bytes => |data| try makeExternalRawConstantTensor(alloc, builder, names[i], init.shape, data),
                                .streamed => |data| try makeExternalStreamedConstantTensor(alloc, builder, names[i], init.shape, data),
                                .q8_0_block => unreachable,
                            } else switch (init.data) {
                                .f32 => |data| try makeConstantTensor(alloc, names[i], init.shape, data),
                                .raw_bytes => |data| try makeRawConstantTensor(alloc, names[i], init.shape, data),
                                .streamed => return error.UnsupportedShape,
                                .q8_0_block => unreachable,
                            };
                            try initializers.append(alloc, tensor);
                        },
                    }
                    if (std.meta.activeTag(init.data) == .q8_0_block) effective_opset_version = @max(effective_opset_version, 21);
                } else if (opts.parameter_initializer_provider) |provider| {
                    if (try provider.load(provider.context, alloc, names[i])) |init| {
                        defer provider.free(provider.context, alloc, &init);
                        switch (init.data) {
                            .q8_0_block => |data| try appendQ8_0BlockParameter(
                                alloc,
                                if (external_data_builder) |*builder| builder else return error.UnsupportedShape,
                                &initializers,
                                &onnx_nodes,
                                &quant_extras,
                                names[i],
                                init.shape,
                                data,
                            ),
                            else => {
                                const tensor = if (external_data_builder) |*builder| switch (init.data) {
                                    .f32 => |data| try makeExternalConstantTensor(alloc, builder, names[i], init.shape, data),
                                    .raw_bytes => |data| try makeExternalRawConstantTensor(alloc, builder, names[i], init.shape, data),
                                    .streamed => |data| try makeExternalStreamedConstantTensor(alloc, builder, names[i], init.shape, data),
                                    .q8_0_block => unreachable,
                                } else switch (init.data) {
                                    .f32 => |data| try makeConstantTensor(alloc, names[i], init.shape, data),
                                    .raw_bytes => |data| try makeRawConstantTensor(alloc, names[i], init.shape, data),
                                    .streamed => return error.UnsupportedShape,
                                    .q8_0_block => unreachable,
                                };
                                try initializers.append(alloc, tensor);
                            },
                        }
                        if (std.meta.activeTag(init.data) == .q8_0_block) effective_opset_version = @max(effective_opset_version, 21);
                    } else {
                        try input_infos.append(alloc, try makeValueInfo(alloc, names[i], n.output_shape));
                    }
                } else {
                    try input_infos.append(alloc, try makeValueInfo(alloc, names[i], n.output_shape));
                }
            },
            .constant => |c| {
                // Create initializer tensor
                const byte_len = try graphConstantByteLen(n.output_shape.dtype, c.data_len);
                const data = effective_graph.constantBytes(c.data_offset, byte_len);
                const tensor = if (external_data_builder) |*builder|
                    try makeExternalRawConstantTensor(alloc, builder, names[i], n.output_shape, data)
                else
                    try makeRawConstantTensor(alloc, names[i], n.output_shape, data);
                try initializers.append(alloc, tensor);
            },
            .fused_zero_tensor => {
                const elem_count = try shapeElementCount(n.output_shape);
                const zeros = try alloc.alloc(f32, elem_count);
                errdefer alloc.free(zeros);
                @memset(zeros, 0.0);
                const tensor = if (external_data_builder) |*builder|
                    try makeExternalConstantTensor(alloc, builder, names[i], n.output_shape, zeros)
                else
                    try makeConstantTensor(alloc, names[i], n.output_shape, zeros);
                if (external_data_builder == null) try zero_tensor_extras.append(alloc, zeros);
                try initializers.append(alloc, tensor);
            },
            .fused_linear_no_bias_pair => |attrs| try appendFusedLinearNoBiasPairSubgraph(
                alloc,
                @intCast(i),
                effective_graph,
                n,
                names,
                &onnx_nodes,
                &pair_second_outputs,
                &quant_extras,
                attrs,
            ),
            .fused_linear => try appendFusedLinearSubgraph(
                alloc,
                @intCast(i),
                effective_graph,
                n,
                names,
                &onnx_nodes,
                &quant_extras,
                true,
            ),
            .fused_linear_no_bias => try appendFusedLinearSubgraph(
                alloc,
                @intCast(i),
                effective_graph,
                n,
                names,
                &onnx_nodes,
                &quant_extras,
                false,
            ),
            .dot_general => try appendDotGeneralSubgraph(
                alloc,
                @intCast(i),
                effective_graph,
                n,
                names,
                &onnx_nodes,
                &quant_extras,
            ),
            .fused_gqa_causal_attention => |attrs| {
                if (findSemanticDecoderGqaBinding(opts, @intCast(i))) |binding| {
                    try appendSemanticGqaSubgraph(
                        alloc,
                        @intCast(i),
                        effective_graph,
                        n,
                        names,
                        &input_infos,
                        &semantic_output_infos,
                        &initializers,
                        &onnx_nodes,
                        &shape_extras,
                        &tensor_data_extras,
                        &quant_extras,
                        attrs,
                        binding,
                    );
                } else if (attrs.seq_len == 1) {
                    try appendDegenerateSingleStepGqaSubgraph(
                        alloc,
                        @intCast(i),
                        n,
                        names,
                        &initializers,
                        &onnx_nodes,
                        &shape_extras,
                        &quant_extras,
                        attrs,
                    );
                } else {
                    const node_proto = try convertOpToNode(
                        alloc,
                        effective_graph,
                        @intCast(i),
                        n,
                        names,
                        &initializers,
                        &input_infos,
                        &shape_extras,
                    );
                    try onnx_nodes.append(alloc, node_proto);
                }
            },
            .fused_rope => |attrs| try appendFusedRopeSubgraph(
                alloc,
                @intCast(i),
                n,
                names,
                &initializers,
                &onnx_nodes,
                &shape_extras,
                &quant_extras,
                &tensor_data_extras,
                if (external_data_builder) |*builder| builder else null,
                attrs,
            ),
            .fused_to_float32 => {
                const ins = n.getInputs();
                if (ins.len == 1 and ins[0] != null_node) {
                    if (pair_second_outputs.get(ins[0])) |second_name| {
                        try appendSimpleNode(alloc, &onnx_nodes, "Identity", names[i], &.{second_name}, &.{names[i]});
                        continue;
                    }
                }
                const node_proto = try convertOpToNode(
                    alloc,
                    effective_graph,
                    @intCast(i),
                    n,
                    names,
                    &initializers,
                    &input_infos,
                    &shape_extras,
                );
                try onnx_nodes.append(alloc, node_proto);
            },
            else => {
                // Convert to ONNX NodeProto. Some ops (Reshape, Expand, …) need
                // their shape-like parameters as extra tensor inputs in ONNX.
                // convertOpToNode emits those as new initializers on demand.
                const node_proto = try convertOpToNode(
                    alloc,
                    effective_graph,
                    @intCast(i),
                    n,
                    names,
                    &initializers,
                    &input_infos,
                    &shape_extras,
                );
                try onnx_nodes.append(alloc, node_proto);
            },
        }
    }

    // Graph outputs
    var output_infos = std.ArrayListUnmanaged(ValueInfoProto).empty;
    defer {
        for (output_infos.items) |*vi| freeValueInfoDims(alloc, vi);
        output_infos.deinit(alloc);
    }
    for (effective_graph.outputs.items) |out_id| {
        const n = effective_graph.node(out_id);
        try output_infos.append(alloc, try makeValueInfo(alloc, names[out_id], n.output_shape));
    }
    try output_infos.appendSlice(alloc, semantic_output_infos.items);
    semantic_output_infos.clearRetainingCapacity();
    pruneUnusedGraphInputs(alloc, &input_infos, onnx_nodes.items, output_infos.items);

    // Phase 3: Assemble ModelProto and serialize.
    var opsets = [_]proto.OpsetImport{.{ .domain = "", .version = effective_opset_version }};
    const graph_proto = GraphProto{
        .name = opts.graph_name,
        .nodes = onnx_nodes.items,
        .initializers = initializers.items,
        .inputs = input_infos.items,
        .outputs = output_infos.items,
    };
    const model = ModelProto{
        .ir_version = opts.ir_version,
        .opset_import = &opsets,
        .graph = graph_proto,
    };

    const model_bytes = try serializeModel(alloc, &model);
    var result = ExportResult{ .model_bytes = model_bytes };
    if (external_data_builder) |*builder| {
        if (builder.file != null) return result;
        result.external_data = .{
            .relative_path = try alloc.dupe(u8, builder.relative_path),
            .bytes = try builder.bytes.toOwnedSlice(alloc),
        };
    }
    return result;
}

fn pruneUnusedGraphInputs(
    alloc: Allocator,
    input_infos: *std.ArrayListUnmanaged(ValueInfoProto),
    nodes: []const NodeProto,
    outputs: []const ValueInfoProto,
) void {
    var write_idx: usize = 0;
    for (input_infos.items) |*info| {
        if (graphInputIsReferenced(info.name, nodes, outputs)) {
            input_infos.items[write_idx] = info.*;
            write_idx += 1;
        } else {
            freeValueInfoDims(alloc, info);
        }
    }
    input_infos.shrinkRetainingCapacity(write_idx);
}

fn graphInputIsReferenced(
    name: []const u8,
    nodes: []const NodeProto,
    outputs: []const ValueInfoProto,
) bool {
    for (nodes) |node| {
        for (node.inputs) |input_name| {
            if (std.mem.eql(u8, input_name, name)) return true;
        }
    }
    for (outputs) |output_info| {
        if (std.mem.eql(u8, output_info.name, name)) return true;
    }
    return false;
}

/// Tracks allocations for extra shape initializers emitted by convertOpToNode,
/// so exportGraph can free them on return. `raw_data` is the i64 backing
/// buffer that the initializer's raw_data slice points into.
const ShapeExtra = struct {
    name: []u8,
    raw_data: []i64,
};

const TensorDataExtra = struct {
    name: []u8,
    raw_data: []u8,
};

const RopeTensorData = struct {
    name: []u8,
    raw_data: []u8,
};

const RopeTrigKind = enum { cos, sin };

fn appendFusedRopeSubgraph(
    alloc: Allocator,
    node_id: NodeId,
    n: *const Node,
    names: []const []const u8,
    initializers: *std.ArrayListUnmanaged(TensorProto),
    onnx_nodes: *std.ArrayListUnmanaged(NodeProto),
    shape_extras: *std.ArrayListUnmanaged(ShapeExtra),
    extra_names: *std.ArrayListUnmanaged([]u8),
    tensor_data_extras: *std.ArrayListUnmanaged(TensorDataExtra),
    external_data_builder: ?*ExternalDataBuilder,
    attrs: ml.graph.node.RopeAttrs,
) !void {
    const inputs = n.getInputs();
    if (inputs.len != 1 or inputs[0] == null_node) return error.UnsupportedShape;
    if (n.output_shape.rank() != 2 or attrs.seq_len == 0 or attrs.head_dim == 0) return error.UnsupportedShape;

    const rows_i64 = n.output_shape.dim(0);
    const hidden_i64 = n.output_shape.dim(1);
    if (rows_i64 <= 0 or hidden_i64 <= 0) return error.UnsupportedShape;

    const rows: usize = @intCast(rows_i64);
    const hidden: usize = @intCast(hidden_i64);
    const head_dim: usize = @intCast(attrs.head_dim);
    if (head_dim == 0 or hidden % head_dim != 0) return error.UnsupportedShape;
    const num_heads = hidden / head_dim;
    if (num_heads == 0) return error.UnsupportedShape;

    const rope_dim: usize = if (attrs.rope_dim > 0) @intCast(attrs.rope_dim) else head_dim;
    if (rope_dim == 0 or rope_dim > head_dim or rope_dim % 2 != 0) return error.UnsupportedShape;

    const seq_len: usize = @intCast(attrs.seq_len);
    if (rows > seq_len) return error.UnsupportedShape;

    const dtype = n.output_shape.dtype;
    switch (dtype) {
        .f32, .f16, .bf16 => {},
        else => return error.UnsupportedShape,
    }

    const output_name = names[node_id];
    const input_name = names[inputs[0]];
    const reshape_in_shape = [_]i64{ @intCast(rows), @intCast(num_heads), @intCast(head_dim) };
    const reshape_out_shape = [_]i64{ @intCast(rows), @intCast(hidden) };

    const shape_in_name = try std.fmt.allocPrint(alloc, "{s}__rope_shape_in", .{output_name});
    errdefer alloc.free(shape_in_name);
    const shape_out_name = try std.fmt.allocPrint(alloc, "{s}__rope_shape_out", .{output_name});
    errdefer alloc.free(shape_out_name);
    try appendOwnedInt64Initializer(alloc, initializers, shape_extras, shape_in_name, &reshape_in_shape);
    try appendOwnedInt64Initializer(alloc, initializers, shape_extras, shape_out_name, &reshape_out_shape);

    const perm_data = try buildRopePermutationMatrixData(alloc, dtype, head_dim, rope_dim, attrs.consecutive_pairs, output_name);
    errdefer {
        alloc.free(perm_data.name);
        alloc.free(perm_data.raw_data);
    }
    try appendOwnedRawTensorInitializer(
        alloc,
        initializers,
        tensor_data_extras,
        external_data_builder,
        perm_data.name,
        Shape.init(dtype, &.{ @intCast(head_dim), @intCast(head_dim) }),
        perm_data.raw_data,
    );

    const trig_shape = Shape.init(dtype, &.{ @intCast(rows), @intCast(num_heads), @intCast(head_dim) });
    const cos_data = try buildRopeTrigTensorData(alloc, dtype, rows, num_heads, head_dim, rope_dim, seq_len, attrs.theta, attrs.freq_scale, attrs.position_offset, attrs.consecutive_pairs, .cos, output_name);
    errdefer {
        alloc.free(cos_data.name);
        alloc.free(cos_data.raw_data);
    }
    try appendOwnedRawTensorInitializer(alloc, initializers, tensor_data_extras, external_data_builder, cos_data.name, trig_shape, cos_data.raw_data);

    const sin_data = try buildRopeTrigTensorData(alloc, dtype, rows, num_heads, head_dim, rope_dim, seq_len, attrs.theta, attrs.freq_scale, attrs.position_offset, attrs.consecutive_pairs, .sin, output_name);
    errdefer {
        alloc.free(sin_data.name);
        alloc.free(sin_data.raw_data);
    }
    try appendOwnedRawTensorInitializer(alloc, initializers, tensor_data_extras, external_data_builder, sin_data.name, trig_shape, sin_data.raw_data);

    const reshape_name = try std.fmt.allocPrint(alloc, "{s}__rope_reshape", .{output_name});
    errdefer alloc.free(reshape_name);
    const rotated_name = try std.fmt.allocPrint(alloc, "{s}__rope_rotated", .{output_name});
    errdefer alloc.free(rotated_name);
    const mul_cos_name = try std.fmt.allocPrint(alloc, "{s}__rope_mul_cos", .{output_name});
    errdefer alloc.free(mul_cos_name);
    const mul_sin_name = try std.fmt.allocPrint(alloc, "{s}__rope_mul_sin", .{output_name});
    errdefer alloc.free(mul_sin_name);
    const add_name = try std.fmt.allocPrint(alloc, "{s}__rope_add", .{output_name});
    errdefer alloc.free(add_name);
    try extra_names.append(alloc, reshape_name);
    try extra_names.append(alloc, rotated_name);
    try extra_names.append(alloc, mul_cos_name);
    try extra_names.append(alloc, mul_sin_name);
    try extra_names.append(alloc, add_name);

    try appendSimpleNode(alloc, onnx_nodes, "Reshape", reshape_name, &.{ input_name, shape_in_name }, &.{reshape_name});
    try appendSimpleNode(alloc, onnx_nodes, "MatMul", rotated_name, &.{ reshape_name, perm_data.name }, &.{rotated_name});
    try appendSimpleNode(alloc, onnx_nodes, "Mul", mul_cos_name, &.{ reshape_name, cos_data.name }, &.{mul_cos_name});
    try appendSimpleNode(alloc, onnx_nodes, "Mul", mul_sin_name, &.{ rotated_name, sin_data.name }, &.{mul_sin_name});
    try appendSimpleNode(alloc, onnx_nodes, "Add", add_name, &.{ mul_cos_name, mul_sin_name }, &.{add_name});
    try appendSimpleNode(alloc, onnx_nodes, "Reshape", output_name, &.{ add_name, shape_out_name }, &.{output_name});
}

fn appendFusedLinearNoBiasPairSubgraph(
    alloc: Allocator,
    node_id: NodeId,
    graph: *const Graph,
    n: *const Node,
    names: []const []const u8,
    onnx_nodes: *std.ArrayListUnmanaged(NodeProto),
    pair_second_outputs: *std.AutoHashMapUnmanaged(NodeId, []const u8),
    extra_names: *std.ArrayListUnmanaged([]u8),
    _: ml.graph.node.LinearAttrs,
) !void {
    const inputs = n.getInputs();
    if (inputs.len != 3) return error.UnsupportedShape;

    const target_dtype = n.output_shape.dtype;
    const input_name = try ensureNodeAsDType(alloc, onnx_nodes, extra_names, names[inputs[0]], "pair_input", node_id, graph.node(inputs[0]).output_shape.dtype, target_dtype);
    const weight_a_name = try ensureNodeAsDType(alloc, onnx_nodes, extra_names, names[inputs[1]], "pair_weight_a", node_id, graph.node(inputs[1]).output_shape.dtype, target_dtype);
    const weight_b_name = try ensureNodeAsDType(alloc, onnx_nodes, extra_names, names[inputs[2]], "pair_weight_b", node_id, graph.node(inputs[2]).output_shape.dtype, target_dtype);
    const first_output_name = names[node_id];
    const second_output_name = try std.fmt.allocPrint(alloc, "{s}__pair_second", .{first_output_name});
    errdefer alloc.free(second_output_name);
    try extra_names.append(alloc, second_output_name);
    try pair_second_outputs.put(alloc, node_id, second_output_name);

    try appendGemmNode(alloc, onnx_nodes, first_output_name, &.{ input_name, weight_a_name }, first_output_name);
    try appendGemmNode(alloc, onnx_nodes, second_output_name, &.{ input_name, weight_b_name }, second_output_name);
}

fn appendFusedLinearSubgraph(
    alloc: Allocator,
    node_id: NodeId,
    graph: *const Graph,
    n: *const Node,
    names: []const []const u8,
    onnx_nodes: *std.ArrayListUnmanaged(NodeProto),
    extra_names: *std.ArrayListUnmanaged([]u8),
    has_bias: bool,
) !void {
    const inputs = n.getInputs();
    if ((has_bias and inputs.len != 3) or (!has_bias and inputs.len != 2)) return error.UnsupportedShape;

    const target_dtype = n.output_shape.dtype;
    const input_name = try ensureNodeAsDType(alloc, onnx_nodes, extra_names, names[inputs[0]], "linear_input", node_id, graph.node(inputs[0]).output_shape.dtype, target_dtype);
    const weight_name = try ensureNodeAsDType(alloc, onnx_nodes, extra_names, names[inputs[1]], "linear_weight", node_id, graph.node(inputs[1]).output_shape.dtype, target_dtype);

    if (has_bias) {
        const bias_name = try ensureNodeAsDType(alloc, onnx_nodes, extra_names, names[inputs[2]], "linear_bias", node_id, graph.node(inputs[2]).output_shape.dtype, target_dtype);
        try appendGemmNode(alloc, onnx_nodes, names[node_id], &.{ input_name, weight_name, bias_name }, names[node_id]);
    } else {
        try appendGemmNode(alloc, onnx_nodes, names[node_id], &.{ input_name, weight_name }, names[node_id]);
    }
}

fn appendDotGeneralSubgraph(
    alloc: Allocator,
    node_id: NodeId,
    graph: *const Graph,
    n: *const Node,
    names: []const []const u8,
    onnx_nodes: *std.ArrayListUnmanaged(NodeProto),
    extra_names: *std.ArrayListUnmanaged([]u8),
) !void {
    const inputs = n.getInputs();
    if (inputs.len != 2) return error.UnsupportedShape;
    const target_dtype = n.output_shape.dtype;
    const lhs_name = try ensureNodeAsDType(alloc, onnx_nodes, extra_names, names[inputs[0]], "dot_lhs", node_id, graph.node(inputs[0]).output_shape.dtype, target_dtype);
    const rhs_name = try ensureNodeAsDType(alloc, onnx_nodes, extra_names, names[inputs[1]], "dot_rhs", node_id, graph.node(inputs[1]).output_shape.dtype, target_dtype);
    try appendSimpleNode(alloc, onnx_nodes, "MatMul", names[node_id], &.{ lhs_name, rhs_name }, &.{names[node_id]});
}

fn ensureNodeAsDType(
    alloc: Allocator,
    onnx_nodes: *std.ArrayListUnmanaged(NodeProto),
    extra_names: *std.ArrayListUnmanaged([]u8),
    input_name: []const u8,
    role: []const u8,
    node_id: NodeId,
    actual_dtype: DType,
    target_dtype: DType,
) ![]const u8 {
    if (actual_dtype == target_dtype) return input_name;
    const cast_name = try std.fmt.allocPrint(alloc, "node_{d}__{s}_cast", .{ node_id, role });
    errdefer alloc.free(cast_name);
    try extra_names.append(alloc, cast_name);
    const inps = [_][]const u8{input_name};
    const outs = [_][]const u8{cast_name};
    const attrs = try alloc.alloc(AttributeProto, 1);
    errdefer alloc.free(attrs);
    attrs[0] = .{
        .name = "to",
        .i = @intCast(@intFromEnum(termiteDTypeToOnnx(target_dtype))),
        .attr_type = .int,
    };
    const node_inputs = try alloc.alloc([]const u8, inps.len);
    errdefer alloc.free(node_inputs);
    @memcpy(node_inputs, &inps);
    const node_outputs = try alloc.alloc([]const u8, outs.len);
    errdefer alloc.free(node_outputs);
    @memcpy(node_outputs, &outs);
    try onnx_nodes.append(alloc, .{
        .inputs = node_inputs,
        .outputs = node_outputs,
        .name = cast_name,
        .op_type = "Cast",
        .attributes = attrs,
    });
    return cast_name;
}

fn appendDegenerateSingleStepGqaSubgraph(
    alloc: Allocator,
    node_id: NodeId,
    n: *const Node,
    names: []const []const u8,
    initializers: *std.ArrayListUnmanaged(TensorProto),
    onnx_nodes: *std.ArrayListUnmanaged(NodeProto),
    shape_extras: *std.ArrayListUnmanaged(ShapeExtra),
    extra_names: *std.ArrayListUnmanaged([]u8),
    attrs: ml.graph.node.AttentionAttrs,
) !void {
    const inputs = n.getInputs();
    if (inputs.len < 3) return error.UnsupportedShape;
    if (n.output_shape.rank() != 2) return error.UnsupportedShape;

    const rows_i64 = n.output_shape.dim(0);
    const hidden_i64 = n.output_shape.dim(1);
    if (rows_i64 <= 0 or hidden_i64 <= 0) return error.UnsupportedShape;
    if (attrs.batch == 0 or attrs.num_heads == 0 or attrs.num_kv_heads == 0 or attrs.head_dim == 0) return error.UnsupportedShape;
    if (attrs.num_heads % attrs.num_kv_heads != 0) return error.UnsupportedShape;

    const rows: i64 = rows_i64;
    const num_heads: i64 = @intCast(attrs.num_heads);
    const num_kv_heads: i64 = @intCast(attrs.num_kv_heads);
    const head_dim: i64 = @intCast(attrs.head_dim);
    const heads_per_group: i64 = @divExact(num_heads, num_kv_heads);
    if (hidden_i64 != num_heads * head_dim) return error.UnsupportedShape;

    const output_name = names[node_id];
    const v_name = names[inputs[2]];
    const shape_v3 = [_]i64{ rows, num_kv_heads, head_dim };
    const shape_v4 = [_]i64{ rows, num_kv_heads, 1, head_dim };
    const shape_expand = [_]i64{ rows, num_kv_heads, heads_per_group, head_dim };
    const shape_heads = [_]i64{ rows, num_heads, head_dim };
    const shape_out = [_]i64{ rows, hidden_i64 };

    const shape_v3_name = try std.fmt.allocPrint(alloc, "{s}__gqa_shape_v3", .{output_name});
    errdefer alloc.free(shape_v3_name);
    const shape_v4_name = try std.fmt.allocPrint(alloc, "{s}__gqa_shape_v4", .{output_name});
    errdefer alloc.free(shape_v4_name);
    const shape_expand_name = try std.fmt.allocPrint(alloc, "{s}__gqa_shape_expand", .{output_name});
    errdefer alloc.free(shape_expand_name);
    const shape_heads_name = try std.fmt.allocPrint(alloc, "{s}__gqa_shape_heads", .{output_name});
    errdefer alloc.free(shape_heads_name);
    const shape_out_name = try std.fmt.allocPrint(alloc, "{s}__gqa_shape_out", .{output_name});
    errdefer alloc.free(shape_out_name);
    try appendOwnedInt64Initializer(alloc, initializers, shape_extras, shape_v3_name, &shape_v3);
    try appendOwnedInt64Initializer(alloc, initializers, shape_extras, shape_v4_name, &shape_v4);
    try appendOwnedInt64Initializer(alloc, initializers, shape_extras, shape_expand_name, &shape_expand);
    try appendOwnedInt64Initializer(alloc, initializers, shape_extras, shape_heads_name, &shape_heads);
    try appendOwnedInt64Initializer(alloc, initializers, shape_extras, shape_out_name, &shape_out);

    const v3_name = try std.fmt.allocPrint(alloc, "{s}__gqa_v3", .{output_name});
    errdefer alloc.free(v3_name);
    const v4_name = try std.fmt.allocPrint(alloc, "{s}__gqa_v4", .{output_name});
    errdefer alloc.free(v4_name);
    const expanded_name = try std.fmt.allocPrint(alloc, "{s}__gqa_expanded", .{output_name});
    errdefer alloc.free(expanded_name);
    const heads_name = try std.fmt.allocPrint(alloc, "{s}__gqa_heads", .{output_name});
    errdefer alloc.free(heads_name);
    try extra_names.append(alloc, v3_name);
    try extra_names.append(alloc, v4_name);
    try extra_names.append(alloc, expanded_name);
    try extra_names.append(alloc, heads_name);

    try appendSimpleNode(alloc, onnx_nodes, "Reshape", v3_name, &.{ v_name, shape_v3_name }, &.{v3_name});
    try appendSimpleNode(alloc, onnx_nodes, "Reshape", v4_name, &.{ v3_name, shape_v4_name }, &.{v4_name});
    try appendSimpleNode(alloc, onnx_nodes, "Expand", expanded_name, &.{ v4_name, shape_expand_name }, &.{expanded_name});
    try appendSimpleNode(alloc, onnx_nodes, "Reshape", heads_name, &.{ expanded_name, shape_heads_name }, &.{heads_name});
    try appendSimpleNode(alloc, onnx_nodes, "Reshape", output_name, &.{ heads_name, shape_out_name }, &.{output_name});
}

fn appendSemanticGqaSubgraph(
    alloc: Allocator,
    node_id: NodeId,
    graph: *const Graph,
    n: *const Node,
    names: []const []const u8,
    input_infos: *std.ArrayListUnmanaged(ValueInfoProto),
    output_infos: *std.ArrayListUnmanaged(ValueInfoProto),
    initializers: *std.ArrayListUnmanaged(TensorProto),
    onnx_nodes: *std.ArrayListUnmanaged(NodeProto),
    shape_extras: *std.ArrayListUnmanaged(ShapeExtra),
    tensor_data_extras: *std.ArrayListUnmanaged(TensorDataExtra),
    extra_names: *std.ArrayListUnmanaged([]u8),
    attrs: ml.graph.node.AttentionAttrs,
    binding: SemanticDecoderGqaBinding,
) !void {
    const inputs = n.getInputs();
    if (inputs.len < 3) return error.UnsupportedShape;
    if (n.output_shape.rank() != 2) return error.UnsupportedShape;

    const rows_i64 = n.output_shape.dim(0);
    const hidden_i64 = n.output_shape.dim(1);
    if (rows_i64 <= 0 or hidden_i64 <= 0) return error.UnsupportedShape;
    if (attrs.batch != 1 or attrs.seq_len == 0 or attrs.num_heads == 0 or attrs.num_kv_heads == 0 or attrs.head_dim == 0) return error.UnsupportedShape;
    if (rows_i64 != @as(i64, @intCast(attrs.seq_len))) return error.UnsupportedShape;
    if (attrs.num_heads % attrs.num_kv_heads != 0) return error.UnsupportedShape;

    const num_heads: i64 = @intCast(attrs.num_heads);
    const num_kv_heads: i64 = @intCast(attrs.num_kv_heads);
    const head_dim: i64 = @intCast(attrs.head_dim);
    const seq_len: i64 = @intCast(attrs.seq_len);
    const heads_per_group: i64 = @divExact(num_heads, num_kv_heads);
    if (hidden_i64 != num_heads * head_dim) return error.UnsupportedShape;

    const k_shape = graph.node(inputs[1]).output_shape;
    const v_shape = graph.node(inputs[2]).output_shape;
    const cache_k_shape = Shape.init(k_shape.dtype, &.{ 1, num_kv_heads, -1, head_dim });
    const cache_v_shape = Shape.init(v_shape.dtype, &.{ 1, num_kv_heads, -1, head_dim });

    const past_k_name = try std.fmt.allocPrint(alloc, "past_key_values.{d}.key", .{binding.layer_index});
    errdefer alloc.free(past_k_name);
    const past_v_name = try std.fmt.allocPrint(alloc, "past_key_values.{d}.value", .{binding.layer_index});
    errdefer alloc.free(past_v_name);
    const present_k_name = try std.fmt.allocPrint(alloc, "present.{d}.key", .{binding.layer_index});
    errdefer alloc.free(present_k_name);
    const present_v_name = try std.fmt.allocPrint(alloc, "present.{d}.value", .{binding.layer_index});
    errdefer alloc.free(present_v_name);
    try extra_names.append(alloc, past_k_name);
    try extra_names.append(alloc, past_v_name);
    try extra_names.append(alloc, present_k_name);
    try extra_names.append(alloc, present_v_name);

    if (!binding.skip_kv_write) {
        try input_infos.append(alloc, try makeValueInfo(alloc, past_k_name, cache_k_shape));
        try input_infos.append(alloc, try makeValueInfo(alloc, past_v_name, cache_v_shape));
        try output_infos.append(alloc, try makeValueInfo(alloc, present_k_name, cache_k_shape));
        try output_infos.append(alloc, try makeValueInfo(alloc, present_v_name, cache_v_shape));
    }

    const output_name = names[node_id];
    const q_name = names[inputs[0]];
    const k_name = names[inputs[1]];
    const v_name = names[inputs[2]];

    const q4_shape = [_]i64{ 1, num_heads, seq_len, head_dim };
    const kv4_shape = [_]i64{ 1, num_kv_heads, seq_len, head_dim };
    const grouped_kv_shape = [_]i64{ 1, num_kv_heads, 1, -1, head_dim };
    const expanded_kv_shape = [_]i64{ 1, num_heads, -1, head_dim };
    const tile_repeats = [_]i64{ 1, 1, heads_per_group, 1, 1 };
    const k_t_perm = [_]i64{ 0, 1, 3, 2 };
    const out_t_perm = [_]i64{ 0, 2, 1, 3 };
    const out_shape = [_]i64{ seq_len, hidden_i64 };

    const q4_shape_name = try std.fmt.allocPrint(alloc, "{s}__semantic_q4_shape", .{output_name});
    errdefer alloc.free(q4_shape_name);
    const kv4_shape_name = try std.fmt.allocPrint(alloc, "{s}__semantic_kv4_shape", .{output_name});
    errdefer alloc.free(kv4_shape_name);
    const grouped_kv_shape_name = try std.fmt.allocPrint(alloc, "{s}__semantic_grouped_kv_shape", .{output_name});
    errdefer alloc.free(grouped_kv_shape_name);
    const expanded_kv_shape_name = try std.fmt.allocPrint(alloc, "{s}__semantic_expanded_kv_shape", .{output_name});
    errdefer alloc.free(expanded_kv_shape_name);
    const tile_repeats_name = try std.fmt.allocPrint(alloc, "{s}__semantic_gqa_tile_repeats", .{output_name});
    errdefer alloc.free(tile_repeats_name);
    const out_shape_name = try std.fmt.allocPrint(alloc, "{s}__semantic_out_shape", .{output_name});
    errdefer alloc.free(out_shape_name);
    try appendOwnedInt64Initializer(alloc, initializers, shape_extras, q4_shape_name, &q4_shape);
    if (binding.skip_kv_write) {
        try extra_names.append(alloc, kv4_shape_name);
    } else {
        try appendOwnedInt64Initializer(alloc, initializers, shape_extras, kv4_shape_name, &kv4_shape);
    }
    if (heads_per_group > 1) {
        try appendOwnedInt64Initializer(alloc, initializers, shape_extras, grouped_kv_shape_name, &grouped_kv_shape);
        try appendOwnedInt64Initializer(alloc, initializers, shape_extras, expanded_kv_shape_name, &expanded_kv_shape);
        try appendOwnedInt64Initializer(alloc, initializers, shape_extras, tile_repeats_name, &tile_repeats);
    } else {
        try extra_names.append(alloc, grouped_kv_shape_name);
        try extra_names.append(alloc, expanded_kv_shape_name);
        try extra_names.append(alloc, tile_repeats_name);
    }
    try appendOwnedInt64Initializer(alloc, initializers, shape_extras, out_shape_name, &out_shape);

    const scale_name = try std.fmt.allocPrint(alloc, "{s}__semantic_scale", .{output_name});
    errdefer alloc.free(scale_name);
    const scale = 1.0 / @sqrt(@as(f32, @floatFromInt(head_dim)));
    try appendOwnedF32Initializer(alloc, initializers, tensor_data_extras, scale_name, Shape.init(.f32, &.{1}), &.{scale});

    const q4_name = try std.fmt.allocPrint(alloc, "{s}__semantic_q4", .{output_name});
    errdefer alloc.free(q4_name);
    const k4_name = try std.fmt.allocPrint(alloc, "{s}__semantic_k4", .{output_name});
    errdefer alloc.free(k4_name);
    const v4_name = try std.fmt.allocPrint(alloc, "{s}__semantic_v4", .{output_name});
    errdefer alloc.free(v4_name);
    const k_grouped_name = try std.fmt.allocPrint(alloc, "{s}__semantic_k_grouped", .{output_name});
    errdefer alloc.free(k_grouped_name);
    const k_tiled_name = try std.fmt.allocPrint(alloc, "{s}__semantic_k_tiled", .{output_name});
    errdefer alloc.free(k_tiled_name);
    const k_expanded_name = try std.fmt.allocPrint(alloc, "{s}__semantic_k_expanded", .{output_name});
    errdefer alloc.free(k_expanded_name);
    const v_grouped_name = try std.fmt.allocPrint(alloc, "{s}__semantic_v_grouped", .{output_name});
    errdefer alloc.free(v_grouped_name);
    const v_tiled_name = try std.fmt.allocPrint(alloc, "{s}__semantic_v_tiled", .{output_name});
    errdefer alloc.free(v_tiled_name);
    const v_expanded_name = try std.fmt.allocPrint(alloc, "{s}__semantic_v_expanded", .{output_name});
    errdefer alloc.free(v_expanded_name);
    const k_t_name = try std.fmt.allocPrint(alloc, "{s}__semantic_k_t", .{output_name});
    errdefer alloc.free(k_t_name);
    const scores_name = try std.fmt.allocPrint(alloc, "{s}__semantic_scores", .{output_name});
    errdefer alloc.free(scores_name);
    const scaled_name = try std.fmt.allocPrint(alloc, "{s}__semantic_scaled", .{output_name});
    errdefer alloc.free(scaled_name);
    const masked_name = try std.fmt.allocPrint(alloc, "{s}__semantic_masked", .{output_name});
    errdefer alloc.free(masked_name);
    const probs_name = try std.fmt.allocPrint(alloc, "{s}__semantic_probs", .{output_name});
    errdefer alloc.free(probs_name);
    const ctx_name = try std.fmt.allocPrint(alloc, "{s}__semantic_ctx", .{output_name});
    errdefer alloc.free(ctx_name);
    const ctx_t_name = try std.fmt.allocPrint(alloc, "{s}__semantic_ctx_t", .{output_name});
    errdefer alloc.free(ctx_t_name);
    try extra_names.append(alloc, q4_name);
    try extra_names.append(alloc, k4_name);
    try extra_names.append(alloc, v4_name);
    try extra_names.append(alloc, k_grouped_name);
    try extra_names.append(alloc, k_tiled_name);
    try extra_names.append(alloc, k_expanded_name);
    try extra_names.append(alloc, v_grouped_name);
    try extra_names.append(alloc, v_tiled_name);
    try extra_names.append(alloc, v_expanded_name);
    try extra_names.append(alloc, k_t_name);
    try extra_names.append(alloc, scores_name);
    try extra_names.append(alloc, scaled_name);
    try extra_names.append(alloc, masked_name);
    try extra_names.append(alloc, probs_name);
    try extra_names.append(alloc, ctx_name);
    try extra_names.append(alloc, ctx_t_name);

    try appendSimpleNode(alloc, onnx_nodes, "Reshape", q4_name, &.{ q_name, q4_shape_name }, &.{q4_name});
    if (!binding.skip_kv_write) {
        try appendSimpleNode(alloc, onnx_nodes, "Reshape", k4_name, &.{ k_name, kv4_shape_name }, &.{k4_name});
        try appendSimpleNode(alloc, onnx_nodes, "Reshape", v4_name, &.{ v_name, kv4_shape_name }, &.{v4_name});
        try appendSimpleNode(alloc, onnx_nodes, "Concat", present_k_name, &.{ past_k_name, k4_name }, &.{present_k_name});
        onnx_nodes.items[onnx_nodes.items.len - 1].attributes = try singleIntAttr(alloc, "axis", 2);
        try appendSimpleNode(alloc, onnx_nodes, "Concat", present_v_name, &.{ past_v_name, v4_name }, &.{present_v_name});
        onnx_nodes.items[onnx_nodes.items.len - 1].attributes = try singleIntAttr(alloc, "axis", 2);
    }

    const semantic_kv_k_name = present_k_name;
    const semantic_kv_v_name = present_v_name;
    const attention_k_name = if (heads_per_group > 1) k_expanded_name else semantic_kv_k_name;
    const attention_v_name = if (heads_per_group > 1) v_expanded_name else semantic_kv_v_name;
    if (heads_per_group > 1) {
        try appendSimpleNode(alloc, onnx_nodes, "Reshape", k_grouped_name, &.{ semantic_kv_k_name, grouped_kv_shape_name }, &.{k_grouped_name});
        try appendSimpleNode(alloc, onnx_nodes, "Tile", k_tiled_name, &.{ k_grouped_name, tile_repeats_name }, &.{k_tiled_name});
        try appendSimpleNode(alloc, onnx_nodes, "Reshape", k_expanded_name, &.{ k_tiled_name, expanded_kv_shape_name }, &.{k_expanded_name});
        try appendSimpleNode(alloc, onnx_nodes, "Reshape", v_grouped_name, &.{ semantic_kv_v_name, grouped_kv_shape_name }, &.{v_grouped_name});
        try appendSimpleNode(alloc, onnx_nodes, "Tile", v_tiled_name, &.{ v_grouped_name, tile_repeats_name }, &.{v_tiled_name});
        try appendSimpleNode(alloc, onnx_nodes, "Reshape", v_expanded_name, &.{ v_tiled_name, expanded_kv_shape_name }, &.{v_expanded_name});
    }

    try appendSimpleNode(alloc, onnx_nodes, "Transpose", k_t_name, &.{attention_k_name}, &.{k_t_name});
    onnx_nodes.items[onnx_nodes.items.len - 1].attributes = try singleIntsAttr(alloc, "perm", &k_t_perm);
    try appendSimpleNode(alloc, onnx_nodes, "MatMul", scores_name, &.{ q4_name, k_t_name }, &.{scores_name});
    try appendSimpleNode(alloc, onnx_nodes, "Mul", scaled_name, &.{ scores_name, scale_name }, &.{scaled_name});
    const probs_input_name = if (attrs.seq_len > 1) blk: {
        const mask_name = try std.fmt.allocPrint(alloc, "{s}__semantic_causal_mask", .{output_name});
        errdefer alloc.free(mask_name);
        const mask_len = try std.math.mul(usize, @intCast(attrs.seq_len), @intCast(attrs.seq_len));
        const mask_values = try alloc.alloc(f32, mask_len);
        defer alloc.free(mask_values);
        for (0..@as(usize, @intCast(attrs.seq_len))) |q_idx| {
            for (0..@as(usize, @intCast(attrs.seq_len))) |k_idx| {
                mask_values[q_idx * @as(usize, @intCast(attrs.seq_len)) + k_idx] = if (k_idx <= q_idx) 0 else -1.0e9;
            }
        }
        try appendOwnedF32Initializer(
            alloc,
            initializers,
            tensor_data_extras,
            mask_name,
            Shape.init(.f32, &.{ 1, 1, seq_len, seq_len }),
            mask_values,
        );
        try appendSimpleNode(alloc, onnx_nodes, "Add", masked_name, &.{ scaled_name, mask_name }, &.{masked_name});
        break :blk masked_name;
    } else scaled_name;
    try appendSimpleNode(alloc, onnx_nodes, "Softmax", probs_name, &.{probs_input_name}, &.{probs_name});
    onnx_nodes.items[onnx_nodes.items.len - 1].attributes = try singleIntAttr(alloc, "axis", 3);
    try appendSimpleNode(alloc, onnx_nodes, "MatMul", ctx_name, &.{ probs_name, attention_v_name }, &.{ctx_name});
    try appendSimpleNode(alloc, onnx_nodes, "Transpose", ctx_t_name, &.{ctx_name}, &.{ctx_t_name});
    onnx_nodes.items[onnx_nodes.items.len - 1].attributes = try singleIntsAttr(alloc, "perm", &out_t_perm);
    try appendSimpleNode(alloc, onnx_nodes, "Reshape", output_name, &.{ ctx_t_name, out_shape_name }, &.{output_name});
}

fn appendSimpleNode(
    alloc: Allocator,
    onnx_nodes: *std.ArrayListUnmanaged(NodeProto),
    op_type: []const u8,
    node_name: []const u8,
    inputs: []const []const u8,
    outputs: []const []const u8,
) !void {
    const inps = try alloc.alloc([]const u8, inputs.len);
    errdefer alloc.free(inps);
    @memcpy(inps, inputs);
    const outs = try alloc.alloc([]const u8, outputs.len);
    errdefer alloc.free(outs);
    @memcpy(outs, outputs);
    try onnx_nodes.append(alloc, .{
        .inputs = inps,
        .outputs = outs,
        .name = node_name,
        .op_type = op_type,
        .attributes = &.{},
    });
}

fn singleIntAttr(alloc: Allocator, name: []const u8, value: i64) ![]AttributeProto {
    const attrs = try alloc.alloc(AttributeProto, 1);
    attrs[0] = .{ .name = name, .i = value, .attr_type = .int };
    return attrs;
}

fn singleIntsAttr(alloc: Allocator, name: []const u8, values: []const i64) ![]AttributeProto {
    const attrs = try alloc.alloc(AttributeProto, 1);
    errdefer alloc.free(attrs);
    const ints = try alloc.alloc(i64, values.len);
    errdefer alloc.free(ints);
    @memcpy(ints, values);
    attrs[0] = .{ .name = name, .ints = ints, .attr_type = .ints };
    return attrs;
}

fn appendGemmNode(
    alloc: Allocator,
    onnx_nodes: *std.ArrayListUnmanaged(NodeProto),
    node_name: []const u8,
    inputs: []const []const u8,
    output_name: []const u8,
) !void {
    const inps = try alloc.alloc([]const u8, inputs.len);
    errdefer alloc.free(inps);
    @memcpy(inps, inputs);
    const outs = try alloc.alloc([]const u8, 1);
    errdefer alloc.free(outs);
    outs[0] = output_name;
    const attrs = try alloc.alloc(AttributeProto, 1);
    errdefer alloc.free(attrs);
    attrs[0] = .{ .name = "transB", .i = 1, .attr_type = .int };
    try onnx_nodes.append(alloc, .{
        .inputs = inps,
        .outputs = outs,
        .name = node_name,
        .op_type = "Gemm",
        .attributes = attrs,
    });
}

fn appendOwnedInt64Initializer(
    alloc: Allocator,
    initializers: *std.ArrayListUnmanaged(TensorProto),
    shape_extras: *std.ArrayListUnmanaged(ShapeExtra),
    name: []u8,
    values: []const i64,
) !void {
    try shape_extras.ensureUnusedCapacity(alloc, 1);
    const raw = try alloc.alloc(i64, values.len);
    errdefer alloc.free(raw);
    @memcpy(raw, values);
    const dims = try alloc.alloc(i64, 1);
    errdefer alloc.free(dims);
    dims[0] = @intCast(values.len);
    try initializers.append(alloc, .{
        .name = name,
        .dims = dims,
        .data_type = .int64,
        .raw_data = std.mem.sliceAsBytes(raw),
    });
    shape_extras.appendAssumeCapacity(.{ .name = name, .raw_data = raw });
}

fn appendOwnedF32Initializer(
    alloc: Allocator,
    initializers: *std.ArrayListUnmanaged(TensorProto),
    tensor_data_extras: *std.ArrayListUnmanaged(TensorDataExtra),
    name: []u8,
    shape: Shape,
    values: []const f32,
) !void {
    try tensor_data_extras.ensureUnusedCapacity(alloc, 1);
    const raw = try allocTensorBytesForDType(alloc, .f32, values);
    errdefer alloc.free(raw);
    var tensor = try makeRawConstantTensor(alloc, name, shape, raw);
    errdefer freeTensorProto(alloc, &tensor);
    try initializers.append(alloc, tensor);
    tensor_data_extras.appendAssumeCapacity(.{ .name = name, .raw_data = raw });
}

fn appendOwnedRawTensorInitializer(
    alloc: Allocator,
    initializers: *std.ArrayListUnmanaged(TensorProto),
    tensor_data_extras: *std.ArrayListUnmanaged(TensorDataExtra),
    external_data_builder: ?*ExternalDataBuilder,
    name: []u8,
    shape: Shape,
    raw_data: []u8,
) !void {
    const tensor = if (external_data_builder) |builder|
        try makeExternalRawConstantTensor(alloc, builder, name, shape, raw_data)
    else
        try makeRawConstantTensor(alloc, name, shape, raw_data);
    try initializers.append(alloc, tensor);
    try tensor_data_extras.append(alloc, .{ .name = name, .raw_data = raw_data });
}

fn allocTensorBytesForDType(alloc: Allocator, dtype: DType, data: []const f32) ![]u8 {
    return switch (dtype) {
        .f32 => blk: {
            const out = try alloc.alloc(u8, data.len * @sizeOf(f32));
            @memcpy(out, std.mem.sliceAsBytes(data));
            break :blk out;
        },
        .f16 => try f32SliceToF16Bytes(alloc, data),
        .bf16 => try f32SliceToBf16Bytes(alloc, data),
        else => error.UnsupportedShape,
    };
}

fn buildRopeTrigTensorData(
    alloc: Allocator,
    dtype: DType,
    rows: usize,
    num_heads: usize,
    head_dim: usize,
    rope_dim: usize,
    seq_len: usize,
    theta: f32,
    freq_scale: f32,
    position_offset: u32,
    consecutive_pairs: bool,
    kind: RopeTrigKind,
    output_name: []const u8,
) !RopeTensorData {
    const name = try std.fmt.allocPrint(alloc, "{s}__rope_{s}", .{ output_name, @tagName(kind) });
    errdefer alloc.free(name);
    const values = try alloc.alloc(f32, rows * num_heads * head_dim);
    defer alloc.free(values);
    switch (kind) {
        .cos => @memset(values, 1.0),
        .sin => @memset(values, 0.0),
    }

    const half = rope_dim / 2;
    const head_half = head_dim / 2;
    const row_position_offset: usize = if (rows < seq_len) seq_len - rows else 0;
    for (0..rows) |row| {
        const pos_f: f32 = @floatFromInt(@as(usize, position_offset) + row_position_offset + row);
        for (0..num_heads) |head| {
            const base = (row * num_heads + head) * head_dim;
            for (0..half) |j| {
                const freq = 1.0 / std.math.pow(f32, theta, @as(f32, @floatFromInt(2 * j)) / @as(f32, @floatFromInt(rope_dim)));
                const angle = pos_f * freq_scale * freq;
                const trig = switch (kind) {
                    .cos => @cos(angle),
                    .sin => @sin(angle),
                };
                const idx0 = if (consecutive_pairs) 2 * j else j;
                const idx1 = if (consecutive_pairs) 2 * j + 1 else j + head_half;
                values[base + idx0] = trig;
                values[base + idx1] = trig;
            }
        }
    }

    return .{
        .name = name,
        .raw_data = try allocTensorBytesForDType(alloc, dtype, values),
    };
}

fn buildRopePermutationMatrixData(
    alloc: Allocator,
    dtype: DType,
    head_dim: usize,
    rope_dim: usize,
    consecutive_pairs: bool,
    output_name: []const u8,
) !RopeTensorData {
    const name = try std.fmt.allocPrint(alloc, "{s}__rope_perm", .{output_name});
    errdefer alloc.free(name);
    const matrix = try alloc.alloc(f32, head_dim * head_dim);
    defer alloc.free(matrix);
    @memset(matrix, 0.0);
    const half = rope_dim / 2;
    const head_half = head_dim / 2;
    for (0..half) |j| {
        const idx0 = if (consecutive_pairs) 2 * j else j;
        const idx1 = if (consecutive_pairs) 2 * j + 1 else j + head_half;
        matrix[idx1 * head_dim + idx0] = -1.0;
        matrix[idx0 * head_dim + idx1] = 1.0;
    }
    return .{
        .name = name,
        .raw_data = try allocTensorBytesForDType(alloc, dtype, matrix),
    };
}

/// Convert a single termite op node to an ONNX NodeProto. May append extra
/// shape-like initializers (and corresponding ValueInfoProto entries) for
/// ops whose parameters ONNX expects as tensor inputs (Reshape, Expand, …).
fn convertOpToNode(
    alloc: Allocator,
    graph: *const Graph,
    node_id: NodeId,
    n: *const Node,
    names: []const []const u8,
    initializers: *std.ArrayListUnmanaged(TensorProto),
    _: *std.ArrayListUnmanaged(ValueInfoProto),
    shape_extras: *std.ArrayListUnmanaged(ShapeExtra),
) !NodeProto {
    // Determine if this op needs an extra shape input appended to its
    // ONNX input list. For Reshape / Expand the target shape lives in the
    // termite node's attributes; ONNX expects it as a second tensor input.
    const shape_attr: ?Shape = switch (n.op) {
        .reshape => |a| a.new_shape,
        .broadcast_in_dim => |a| a.target_shape,
        else => null,
    };
    const slice_attr: ?ml.graph.node.SliceAttrs = switch (n.op) {
        .slice => |a| a,
        else => null,
    };
    const reduce_axes_attr: ?[]const u8 = switch (n.op) {
        .reduce_sum => |a| a.axes[0..a.num_axes],
        .reduce_max => |a| a.axes[0..a.num_axes],
        .reduce_mean => |a| a.axes[0..a.num_axes],
        else => null,
    };

    const inputs_slice = n.getInputs();
    const extra_inputs: usize = @intFromBool(shape_attr != null) + @intFromBool(reduce_axes_attr != null) + (if (slice_attr != null) @as(usize, 4) else @as(usize, 0));
    var inp_names = try alloc.alloc([]const u8, inputs_slice.len + extra_inputs);
    errdefer alloc.free(inp_names);
    for (inputs_slice, 0..) |inp_id, i| {
        inp_names[i] = if (inp_id == null_node) "" else names[inp_id];
    }

    var extra_input_idx = inputs_slice.len;

    if (shape_attr) |shape| {
        if (!reshapeShapeInitializerIsOnnxSafe(shape)) return error.UnsupportedShape;

        // Reserve list capacity up front so the actual append below can't
        // fail — this keeps ownership transfer of shape_name/dims_i64 into
        // shape_extras atomic, avoiding double-free vs errdefer paths.
        try shape_extras.ensureUnusedCapacity(alloc, 1);

        const shape_name = try std.fmt.allocPrint(alloc, "shape_{d}", .{node_id});
        errdefer alloc.free(shape_name);
        const rank = shape.rank();
        const dims_i64 = try alloc.alloc(i64, rank);
        errdefer alloc.free(dims_i64);
        for (0..rank) |i| dims_i64[i] = shape.dim(@intCast(i));

        // Initializer: 1-D i64 tensor of length `rank` holding the shape.
        const init_dims = try alloc.alloc(i64, 1);
        errdefer alloc.free(init_dims);
        init_dims[0] = @intCast(rank);

        try initializers.append(alloc, .{
            .name = shape_name,
            .dims = init_dims,
            .data_type = .int64,
            .raw_data = std.mem.sliceAsBytes(dims_i64),
        });
        // At this point, `init_dims` is owned by the initializers cleanup.
        // shape_name / dims_i64 still need an owner if subsequent steps fail.

        // Transfer ownership of shape_name / dims_i64 to the caller via
        // shape_extras. This append cannot fail due to the ensureUnusedCapacity
        // call above, so it's safe to not re-free them on any later error.
        shape_extras.appendAssumeCapacity(.{ .name = shape_name, .raw_data = dims_i64 });

        inp_names[extra_input_idx] = shape_name;
        extra_input_idx += 1;
    }

    if (reduce_axes_attr) |axes| {
        try shape_extras.ensureUnusedCapacity(alloc, 1);

        const axes_name = try std.fmt.allocPrint(alloc, "axes_{d}", .{node_id});
        errdefer alloc.free(axes_name);
        const axes_i64 = try alloc.alloc(i64, axes.len);
        errdefer alloc.free(axes_i64);
        for (axes, 0..) |axis, i| axes_i64[i] = @intCast(axis);

        const init_dims = try alloc.alloc(i64, 1);
        errdefer alloc.free(init_dims);
        init_dims[0] = @intCast(axes.len);

        try initializers.append(alloc, .{
            .name = axes_name,
            .dims = init_dims,
            .data_type = .int64,
            .raw_data = std.mem.sliceAsBytes(axes_i64),
        });
        shape_extras.appendAssumeCapacity(.{ .name = axes_name, .raw_data = axes_i64 });
        inp_names[extra_input_idx] = axes_name;
        extra_input_idx += 1;
    }

    if (slice_attr) |a| {
        try shape_extras.ensureUnusedCapacity(alloc, 4);

        const starts_name = try std.fmt.allocPrint(alloc, "slice_starts_{d}", .{node_id});
        errdefer alloc.free(starts_name);
        const starts_i64 = try alloc.alloc(i64, a.num_axes);
        errdefer alloc.free(starts_i64);
        for (0..a.num_axes) |i| starts_i64[i] = a.starts[i];
        const starts_dims = try alloc.alloc(i64, 1);
        errdefer alloc.free(starts_dims);
        starts_dims[0] = @intCast(a.num_axes);
        try initializers.append(alloc, .{
            .name = starts_name,
            .dims = starts_dims,
            .data_type = .int64,
            .raw_data = std.mem.sliceAsBytes(starts_i64),
        });
        shape_extras.appendAssumeCapacity(.{ .name = starts_name, .raw_data = starts_i64 });
        inp_names[extra_input_idx] = starts_name;
        extra_input_idx += 1;

        const ends_name = try std.fmt.allocPrint(alloc, "slice_ends_{d}", .{node_id});
        errdefer alloc.free(ends_name);
        const ends_i64 = try alloc.alloc(i64, a.num_axes);
        errdefer alloc.free(ends_i64);
        for (0..a.num_axes) |i| ends_i64[i] = a.limits[i];
        const ends_dims = try alloc.alloc(i64, 1);
        errdefer alloc.free(ends_dims);
        ends_dims[0] = @intCast(a.num_axes);
        try initializers.append(alloc, .{
            .name = ends_name,
            .dims = ends_dims,
            .data_type = .int64,
            .raw_data = std.mem.sliceAsBytes(ends_i64),
        });
        shape_extras.appendAssumeCapacity(.{ .name = ends_name, .raw_data = ends_i64 });
        inp_names[extra_input_idx] = ends_name;
        extra_input_idx += 1;

        const axes_name = try std.fmt.allocPrint(alloc, "slice_axes_{d}", .{node_id});
        errdefer alloc.free(axes_name);
        const axes_i64 = try alloc.alloc(i64, a.num_axes);
        errdefer alloc.free(axes_i64);
        for (0..a.num_axes) |i| axes_i64[i] = @intCast(i);
        const axes_dims = try alloc.alloc(i64, 1);
        errdefer alloc.free(axes_dims);
        axes_dims[0] = @intCast(a.num_axes);
        try initializers.append(alloc, .{
            .name = axes_name,
            .dims = axes_dims,
            .data_type = .int64,
            .raw_data = std.mem.sliceAsBytes(axes_i64),
        });
        shape_extras.appendAssumeCapacity(.{ .name = axes_name, .raw_data = axes_i64 });
        inp_names[extra_input_idx] = axes_name;
        extra_input_idx += 1;

        const steps_name = try std.fmt.allocPrint(alloc, "slice_steps_{d}", .{node_id});
        errdefer alloc.free(steps_name);
        const steps_i64 = try alloc.alloc(i64, a.num_axes);
        errdefer alloc.free(steps_i64);
        for (0..a.num_axes) |i| steps_i64[i] = a.strides[i];
        const steps_dims = try alloc.alloc(i64, 1);
        errdefer alloc.free(steps_dims);
        steps_dims[0] = @intCast(a.num_axes);
        try initializers.append(alloc, .{
            .name = steps_name,
            .dims = steps_dims,
            .data_type = .int64,
            .raw_data = std.mem.sliceAsBytes(steps_i64),
        });
        shape_extras.appendAssumeCapacity(.{ .name = steps_name, .raw_data = steps_i64 });
        inp_names[extra_input_idx] = steps_name;
    }

    // Single output name
    var out_names = try alloc.alloc([]const u8, 1);
    out_names[0] = names[node_id];

    // Map OpCode → ONNX op_type + attributes
    const mapping = try mapOp(alloc, graph, n);

    return .{
        .inputs = inp_names,
        .outputs = out_names,
        .name = names[node_id],
        .op_type = mapping.op_type,
        .attributes = mapping.attrs,
    };
}

fn reshapeShapeInitializerIsOnnxSafe(shape: Shape) bool {
    var inferred_dims: usize = 0;
    for (0..shape.rank()) |axis| {
        const dim = shape.dim(@intCast(axis));
        if (dim < -1) return false;
        if (dim == -1) inferred_dims += 1;
        if (inferred_dims > 1) return false;
    }
    return true;
}

const OpMapping = struct {
    op_type: []const u8,
    attrs: []AttributeProto,
};

fn mapOp(alloc: Allocator, graph: *const Graph, n: *const Node) !OpMapping {
    _ = graph;
    return switch (n.op) {
        // Elementwise unary
        .neg => simpleOp("Neg"),
        .sqrt => simpleOp("Sqrt"),
        // ONNX has no Rsqrt; emit as a termite-domain op to preserve semantics.
        // Consumers should either handle the custom op or run the lowering
        // pass before export (which decomposes rsqrt via its vjp_alternate).
        .rsqrt => .{ .op_type = "Rsqrt", .attrs = &.{} },
        .exp => simpleOp("Exp"),
        .log => simpleOp("Log"),
        .sin => simpleOp("Sin"),
        .cos => simpleOp("Cos"),
        .tanh => simpleOp("Tanh"),
        .erf => simpleOp("Erf"),
        .abs => simpleOp("Abs"),

        // Elementwise binary
        .add => simpleOp("Add"),
        .mul => simpleOp("Mul"),
        .sub => simpleOp("Sub"),
        .div => simpleOp("Div"),

        // Comparison
        .less_than => simpleOp("Less"),
        .where_select => simpleOp("Where"),

        // Reduction
        .reduce_sum => simpleOp("ReduceSum"),
        .reduce_max => simpleOp("ReduceMax"),
        .reduce_mean => simpleOp("ReduceMean"),
        .argmax => |a| try argReduceOp(alloc, "ArgMax", a),

        // Shape manipulation
        .reshape => simpleOp("Reshape"),
        .transpose => |a| try transposeOp(alloc, a),
        .broadcast_in_dim => simpleOp("Expand"),
        .slice => simpleOp("Slice"),
        .concat_prim => |a| try intAttrOp(alloc, "Concat", "axis", a.axis),
        .range => simpleOp("Range"),
        .shape_of => |a| try shapeOfOp(alloc, a),
        .gather => |a| try intAttrOp(alloc, "Gather", "axis", a.axis),
        .scatter_add => simpleOp("ScatterElements"),

        // Contraction
        .dot_general => simpleOp("MatMul"),

        // Convolution
        .conv_general => |a| try convOp(alloc, a),

        // Type conversion
        .convert_dtype => |a| try castOp(alloc, a),

        // Fused activations
        .fused_relu => simpleOp("Relu"),
        .fused_gelu => simpleOp("Gelu"),
        .fused_sigmoid => simpleOp("Sigmoid"),
        .fused_tanh_act => simpleOp("Tanh"),
        .fused_silu => simpleOp("Silu"),

        // Fused elementwise
        .fused_elem_add => simpleOp("Add"),
        .fused_elem_multiply => simpleOp("Mul"),

        // Fused softmax
        .fused_softmax => |a| try intAttrOp(alloc, "Softmax", "axis", @as(i64, @intCast(a.dim))),
        .fused_log_softmax => |a| try intAttrOp(alloc, "LogSoftmax", "axis", @as(i64, @intCast(a.dim))),

        // Fused norms
        .fused_layer_norm => |a| try normOp(alloc, "LayerNormalization", a),
        .fused_rms_norm => |a| try normOp(alloc, "SimplifiedLayerNormalization", a),

        // Fused linear layers can map directly to Gemm when they survive
        // lowering, as long as the weight input remains in [out_dim, in_dim].
        .fused_linear, .fused_linear_no_bias => try intAttrOp(alloc, "Gemm", "transB", @as(i64, 1)),

        // Type conversion fused: emit Cast with the correct `to` attribute.
        // fused_from_float32: f32 → n.output_shape.dtype.
        // fused_to_float32:   input dtype → f32 (always float32).
        .fused_from_float32 => try intAttrOp(
            alloc,
            "Cast",
            "to",
            @as(i64, @intCast(@intFromEnum(termiteDTypeToOnnx(n.output_shape.dtype)))),
        ),
        .fused_to_float32 => try intAttrOp(
            alloc,
            "Cast",
            "to",
            @as(i64, @intCast(@intFromEnum(DataType.float32))),
        ),

        // Fused attention
        .fused_sdpa => |a| try attentionOp(alloc, "MultiHeadAttention", a.num_heads, false),
        .fused_causal_self_attention => |a| try attentionOp(alloc, "MultiHeadAttention", a.num_heads, true),
        .fused_cross_attention => |a| try intAttrOp(alloc, "MultiHeadAttention", "num_heads", a.num_heads),
        .fused_gqa_causal_attention => |a| try gqaOp(alloc, a),

        // Fused embedding / misc
        .fused_quick_gelu => simpleOp("FastGelu"),
        .fused_concat => simpleOp("Concat"),
        .fused_embedding_lookup => simpleOp("Gather"),
        .fused_take_rows => simpleOp("Gather"),

        // Constants and parameters
        .parameter => simpleOp("Identity"),
        .constant => simpleOp("Constant"),

        // Everything else → custom op with termite domain
        else => .{ .op_type = @tagName(n.op), .attrs = &.{} },
    };
}

fn simpleOp(op_type: []const u8) OpMapping {
    return .{ .op_type = op_type, .attrs = &.{} };
}

fn shapeOfOp(alloc: Allocator, attrs_in: ml.graph.node.ShapeOfAttrs) !OpMapping {
    var attrs = std.ArrayListUnmanaged(AttributeProto).empty;
    errdefer attrs.deinit(alloc);
    if (attrs_in.start != 0) {
        try attrs.append(alloc, .{ .name = "start", .i = attrs_in.start, .attr_type = .int });
    }
    if (attrs_in.end != 0) {
        try attrs.append(alloc, .{ .name = "end", .i = attrs_in.end, .attr_type = .int });
    }
    return .{ .op_type = "Shape", .attrs = try attrs.toOwnedSlice(alloc) };
}

fn argReduceOp(alloc: Allocator, op_type: []const u8, attrs_in: ml.graph.node.ArgReduceAttrs) !OpMapping {
    const attrs = try alloc.alloc(AttributeProto, 2);
    attrs[0] = .{ .name = "axis", .i = attrs_in.axis, .attr_type = .int };
    attrs[1] = .{ .name = "keepdims", .i = if (attrs_in.keepdims) 1 else 0, .attr_type = .int };
    return .{ .op_type = op_type, .attrs = attrs };
}

fn intAttrOp(alloc: Allocator, op_type: []const u8, name: []const u8, value: anytype) !OpMapping {
    const attrs = try alloc.alloc(AttributeProto, 1);
    attrs[0] = .{ .name = name, .i = @as(i64, @intCast(value)), .attr_type = .int };
    return .{ .op_type = op_type, .attrs = attrs };
}

fn reduceOp(alloc: Allocator, op_type: []const u8, a: ml.graph.node.ReduceAttrs, n: *const Node) !OpMapping {
    _ = n;
    var axes = try alloc.alloc(i64, a.num_axes);
    for (0..a.num_axes) |i| axes[i] = @intCast(a.axes[i]);
    const attrs = try alloc.alloc(AttributeProto, 1);
    attrs[0] = .{ .name = "axes", .ints = axes, .attr_type = .ints };
    return .{ .op_type = op_type, .attrs = attrs };
}

fn transposeOp(alloc: Allocator, a: ml.graph.node.TransposeAttrs) !OpMapping {
    var perm = try alloc.alloc(i64, a.num_axes);
    for (0..a.num_axes) |i| perm[i] = @intCast(a.perm[i]);
    const attrs = try alloc.alloc(AttributeProto, 1);
    attrs[0] = .{ .name = "perm", .ints = perm, .attr_type = .ints };
    return .{ .op_type = "Transpose", .attrs = attrs };
}

fn sliceOp(alloc: Allocator, a: ml.graph.node.SliceAttrs) !OpMapping {
    // ONNX Slice takes starts/ends/axes/steps as inputs, but we encode as attributes
    // for simplicity. Consumer tools can handle either form.
    var attrs_list = std.ArrayListUnmanaged(AttributeProto).empty;
    errdefer attrs_list.deinit(alloc);

    var starts = try alloc.alloc(i64, a.num_axes);
    for (0..a.num_axes) |i| starts[i] = a.starts[i];
    try attrs_list.append(alloc, .{ .name = "starts", .ints = starts, .attr_type = .ints });

    var ends = try alloc.alloc(i64, a.num_axes);
    for (0..a.num_axes) |i| ends[i] = a.limits[i];
    try attrs_list.append(alloc, .{ .name = "ends", .ints = ends, .attr_type = .ints });

    var axes = try alloc.alloc(i64, a.num_axes);
    for (0..a.num_axes) |i| axes[i] = @intCast(i);
    try attrs_list.append(alloc, .{ .name = "axes", .ints = axes, .attr_type = .ints });

    return .{ .op_type = "Slice", .attrs = try attrs_list.toOwnedSlice(alloc) };
}

fn convOp(alloc: Allocator, a: ml.graph.node.ConvAttrs) !OpMapping {
    var attrs_list = std.ArrayListUnmanaged(AttributeProto).empty;
    errdefer attrs_list.deinit(alloc);

    // kernel_shape not stored directly — omit (consumers infer from weight shape)

    // strides
    var strides = try alloc.alloc(i64, a.num_spatial);
    for (0..a.num_spatial) |i| strides[i] = @intCast(a.strides[i]);
    try attrs_list.append(alloc, .{ .name = "strides", .ints = strides, .attr_type = .ints });

    // pads (ONNX uses [begin_0, begin_1, ..., end_0, end_1, ...])
    var pads = try alloc.alloc(i64, @as(usize, a.num_spatial) * 2);
    for (0..a.num_spatial) |i| pads[i] = @intCast(a.padding[i][0]);
    for (0..a.num_spatial) |i| pads[a.num_spatial + i] = @intCast(a.padding[i][1]);
    try attrs_list.append(alloc, .{ .name = "pads", .ints = pads, .attr_type = .ints });

    // group
    if (a.groups > 1) {
        try attrs_list.append(alloc, .{ .name = "group", .i = @intCast(a.groups), .attr_type = .int });
    }

    return .{ .op_type = "Conv", .attrs = try attrs_list.toOwnedSlice(alloc) };
}

fn castOp(alloc: Allocator, a: ml.graph.node.ConvertDTypeAttrs) !OpMapping {
    const onnx_dt = termiteDTypeToOnnx(a.target);
    return intAttrOp(alloc, "Cast", "to", @as(i64, @intCast(@intFromEnum(onnx_dt))));
}

fn normOp(alloc: Allocator, op_type: []const u8, a: ml.graph.node.NormAttrs) !OpMapping {
    const attrs = try alloc.alloc(AttributeProto, 2);
    attrs[0] = .{ .name = "axis", .i = -1, .attr_type = .int };
    attrs[1] = .{ .name = "epsilon", .f = a.eps, .attr_type = .float };
    return .{ .op_type = op_type, .attrs = attrs };
}

fn attentionOp(alloc: Allocator, op_type: []const u8, num_heads: u32, unidirectional: bool) !OpMapping {
    const num_attrs: usize = if (unidirectional) 2 else 1;
    const attrs = try alloc.alloc(AttributeProto, num_attrs);
    attrs[0] = .{ .name = "num_heads", .i = @intCast(num_heads), .attr_type = .int };
    if (unidirectional) {
        attrs[1] = .{ .name = "unidirectional", .i = 1, .attr_type = .int };
    }
    return .{ .op_type = op_type, .attrs = attrs };
}

fn gqaOp(alloc: Allocator, a: ml.graph.node.AttentionAttrs) !OpMapping {
    const attrs = try alloc.alloc(AttributeProto, 2);
    attrs[0] = .{ .name = "num_heads", .i = @intCast(a.num_heads), .attr_type = .int };
    attrs[1] = .{ .name = "kv_num_heads", .i = @intCast(a.num_kv_heads), .attr_type = .int };
    return .{ .op_type = "GroupQueryAttention", .attrs = attrs };
}

fn freeValueInfoDims(alloc: Allocator, vi: *ValueInfoProto) void {
    if (vi.type_proto) |*tp| {
        if (tp.tensor_type) |*tt| {
            if (tt.shape) |*sp| {
                for (sp.dims) |*dim| {
                    if (dim.dim_param.len > 0) alloc.free(dim.dim_param);
                }
                if (sp.dims.len > 0) alloc.free(sp.dims);
            }
        }
    }
}

fn freeTensorProto(alloc: Allocator, tensor: *TensorProto) void {
    if (tensor.dims.len > 0) alloc.free(tensor.dims);
    for (tensor.external_data) |entry| {
        if (entry.key.len > 0) alloc.free(entry.key);
        if (entry.value.len > 0) alloc.free(entry.value);
    }
    if (tensor.external_data.len > 0) alloc.free(tensor.external_data);
}

fn freeNodeProto(alloc: Allocator, node: *NodeProto) void {
    // Free allocated attribute data
    for (node.attributes) |*attr| {
        if (attr.ints.len > 0) alloc.free(attr.ints);
        if (attr.floats.len > 0) alloc.free(attr.floats);
    }
    alloc.free(node.attributes);
    alloc.free(node.inputs);
    alloc.free(node.outputs);
}

// ── Tests ───────────────────────────────────────────────────────────

test "serializeModel roundtrip — empty model" {
    const alloc = std.testing.allocator;
    const model = ModelProto{ .ir_version = 8 };
    const bytes = try serializeModel(alloc, &model);
    defer alloc.free(bytes);

    // Parse back
    var parsed = try proto.parseModelProto(alloc, bytes);
    defer parsed.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 8), parsed.ir_version);
    try std.testing.expect(parsed.graph == null);
}

test "serializeModel roundtrip — model with graph" {
    const alloc = std.testing.allocator;

    var inp_names = [_][]const u8{ "x", "y" };
    var out_names = [_][]const u8{"z"};
    var nodes = [_]NodeProto{
        .{ .op_type = "Add", .inputs = &inp_names, .outputs = &out_names, .name = "add_0" },
    };
    var input_infos = [_]ValueInfoProto{
        .{ .name = "x" },
        .{ .name = "y" },
    };
    var output_infos = [_]ValueInfoProto{
        .{ .name = "z" },
    };
    const graph_proto = GraphProto{
        .name = "test_graph",
        .nodes = &nodes,
        .inputs = &input_infos,
        .outputs = &output_infos,
    };
    _ = &nodes;
    _ = &input_infos;
    _ = &output_infos;
    var opsets = [_]proto.OpsetImport{.{ .domain = "", .version = 17 }};
    const model = ModelProto{
        .ir_version = 8,
        .graph = graph_proto,
        .opset_import = &opsets,
    };

    const bytes = try serializeModel(alloc, &model);
    defer alloc.free(bytes);
    try std.testing.expect(bytes.len > 0);

    // Parse back
    var parsed = try proto.parseModelProto(alloc, bytes);
    defer parsed.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 8), parsed.ir_version);
    try std.testing.expect(parsed.graph != null);

    const g = parsed.graph.?;
    try std.testing.expectEqualStrings("test_graph", g.name);
    try std.testing.expectEqual(@as(usize, 1), g.nodes.len);
    try std.testing.expectEqualStrings("Add", g.nodes[0].op_type);
    try std.testing.expectEqual(@as(usize, 2), g.nodes[0].inputs.len);
    try std.testing.expectEqualStrings("x", g.nodes[0].inputs[0]);
    try std.testing.expectEqualStrings("y", g.nodes[0].inputs[1]);
    try std.testing.expectEqual(@as(usize, 1), g.nodes[0].outputs.len);
    try std.testing.expectEqualStrings("z", g.nodes[0].outputs[0]);
    try std.testing.expectEqual(@as(usize, 2), g.inputs.len);
    try std.testing.expectEqual(@as(usize, 1), g.outputs.len);
    try std.testing.expectEqual(@as(usize, 1), parsed.opset_import.len);
    try std.testing.expectEqual(@as(u64, 17), parsed.opset_import[0].version);
}

test "serializeModel roundtrip — tensor initializer" {
    const alloc = std.testing.allocator;

    var dims = [_]i64{ 2, 3 };
    const float_vals = [_]f32{ 1.0, 2.0, 3.0, 4.0, 5.0, 6.0 };
    const raw = std.mem.sliceAsBytes(&float_vals);
    var initializers = [_]TensorProto{
        .{ .name = "weight", .dims = &dims, .data_type = .float32, .raw_data = raw },
    };
    const graph_proto = GraphProto{
        .initializers = &initializers,
    };
    _ = &initializers;
    const model = ModelProto{ .graph = graph_proto };

    const bytes = try serializeModel(alloc, &model);
    defer alloc.free(bytes);

    var parsed = try proto.parseModelProto(alloc, bytes);
    defer parsed.deinit(alloc);
    const g = parsed.graph.?;
    try std.testing.expectEqual(@as(usize, 1), g.initializers.len);
    try std.testing.expectEqualStrings("weight", g.initializers[0].name);
    try std.testing.expectEqual(@as(usize, 2), g.initializers[0].dims.len);
    try std.testing.expectEqual(@as(i64, 2), g.initializers[0].dims[0]);
    try std.testing.expectEqual(@as(i64, 3), g.initializers[0].dims[1]);
    try std.testing.expectEqual(@as(usize, 24), g.initializers[0].raw_data.len);
}

test "serializeModel roundtrip — node with int attribute" {
    const alloc = std.testing.allocator;

    var ints = [_]i64{ 0, 2, 1 };
    var attrs = [_]AttributeProto{
        .{ .name = "perm", .ints = &ints, .attr_type = .ints },
    };
    var inp = [_][]const u8{"x"};
    var out = [_][]const u8{"y"};
    var nodes = [_]NodeProto{
        .{ .op_type = "Transpose", .inputs = &inp, .outputs = &out, .attributes = &attrs },
    };
    const graph_proto = GraphProto{ .nodes = &nodes };
    _ = &attrs;
    _ = &nodes;
    const model = ModelProto{ .graph = graph_proto };

    const bytes = try serializeModel(alloc, &model);
    defer alloc.free(bytes);

    var parsed = try proto.parseModelProto(alloc, bytes);
    defer parsed.deinit(alloc);
    const g = parsed.graph.?;
    try std.testing.expectEqual(@as(usize, 1), g.nodes.len);
    try std.testing.expectEqual(@as(usize, 1), g.nodes[0].attributes.len);
    try std.testing.expectEqualStrings("perm", g.nodes[0].attributes[0].name);
    try std.testing.expectEqual(@as(usize, 3), g.nodes[0].attributes[0].ints.len);
    try std.testing.expectEqual(@as(i64, 0), g.nodes[0].attributes[0].ints[0]);
    try std.testing.expectEqual(@as(i64, 2), g.nodes[0].attributes[0].ints[1]);
    try std.testing.expectEqual(@as(i64, 1), g.nodes[0].attributes[0].ints[2]);
}

test "serializeModel roundtrip — value info with shape" {
    const alloc = std.testing.allocator;

    var dims = [_]TensorShapeProto.Dimension{
        .{ .dim_param = "batch" },
        .{ .dim_value = 128 },
    };
    var input_infos = [_]ValueInfoProto{
        .{
            .name = "input",
            .type_proto = .{
                .tensor_type = .{
                    .elem_type = .float32,
                    .shape = .{ .dims = &dims },
                },
            },
        },
    };
    const graph_proto = GraphProto{ .inputs = &input_infos };
    _ = &input_infos;
    const model = ModelProto{ .graph = graph_proto };

    const bytes = try serializeModel(alloc, &model);
    defer alloc.free(bytes);

    var parsed = try proto.parseModelProto(alloc, bytes);
    defer parsed.deinit(alloc);
    const g = parsed.graph.?;
    try std.testing.expectEqual(@as(usize, 1), g.inputs.len);
    try std.testing.expectEqualStrings("input", g.inputs[0].name);
    const tp = g.inputs[0].type_proto.?;
    const tt = tp.tensor_type.?;
    try std.testing.expectEqual(DataType.float32, tt.elem_type);
    const sp = tt.shape.?;
    try std.testing.expectEqual(@as(usize, 2), sp.dims.len);
    try std.testing.expectEqualStrings("batch", sp.dims[0].dim_param);
    try std.testing.expectEqual(@as(?i64, 128), sp.dims[1].dim_value);
}

test "exportGraph — simple add graph" {
    const alloc = std.testing.allocator;

    // Build a simple termite graph: param(a) + param(b) → output
    var graph = Graph.init(alloc);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);

    const a = try builder.parameter("a", Shape.init(.f32, &.{4}));
    const b = try builder.parameter("b", Shape.init(.f32, &.{4}));
    const sum = try builder.add(a, b);
    try graph.markOutput(sum);

    // Export
    const bytes = try exportGraph(alloc, &graph, .{});
    defer alloc.free(bytes);
    try std.testing.expect(bytes.len > 0);

    // Parse back and verify
    var parsed = try proto.parseModelProto(alloc, bytes);
    defer parsed.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 8), parsed.ir_version);

    const g = parsed.graph.?;
    try std.testing.expectEqualStrings("inference_graph", g.name);
    // Should have one Add node
    try std.testing.expectEqual(@as(usize, 1), g.nodes.len);
    try std.testing.expectEqualStrings("Add", g.nodes[0].op_type);
    // Two inputs (parameters a, b)
    try std.testing.expectEqual(@as(usize, 2), g.inputs.len);
    // One output
    try std.testing.expectEqual(@as(usize, 1), g.outputs.len);
}

test "exportGraph applies node name overrides to graph ABI" {
    const alloc = std.testing.allocator;

    var graph = Graph.init(alloc);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);

    const a = try builder.parameter("a", Shape.init(.f32, &.{4}));
    const b = try builder.parameter("b", Shape.init(.f32, &.{4}));
    const sum = try builder.add(a, b);
    try graph.markOutput(sum);

    const overrides = [_]NodeNameOverride{
        .{ .node_id = a, .name = "input_ids" },
        .{ .node_id = sum, .name = "logits" },
    };
    const bytes = try exportGraph(alloc, &graph, .{ .node_name_overrides = &overrides });
    defer alloc.free(bytes);

    var parsed = try proto.parseModelProto(alloc, bytes);
    defer parsed.deinit(alloc);

    const g = parsed.graph.?;
    try std.testing.expectEqual(@as(usize, 2), g.inputs.len);
    try std.testing.expectEqualStrings("input_ids", g.inputs[0].name);
    try std.testing.expectEqualStrings("b", g.inputs[1].name);
    try std.testing.expectEqual(@as(usize, 1), g.nodes.len);
    try std.testing.expectEqualStrings("input_ids", g.nodes[0].inputs[0]);
    try std.testing.expectEqualStrings("b", g.nodes[0].inputs[1]);
    try std.testing.expectEqualStrings("logits", g.nodes[0].outputs[0]);
    try std.testing.expectEqual(@as(usize, 1), g.outputs.len);
    try std.testing.expectEqualStrings("logits", g.outputs[0].name);
}

test "exportGraph emits semantic single-step decoder MHA ABI" {
    const alloc = std.testing.allocator;

    var graph = Graph.init(alloc);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);

    const q = try builder.parameter("q", Shape.init(.f32, &.{ 1, 4 }));
    const k = try builder.parameter("k", Shape.init(.f32, &.{ 1, 4 }));
    const v = try builder.parameter("v", Shape.init(.f32, &.{ 1, 4 }));
    const gqa = try graph.addNode(.{
        .op = .{ .fused_gqa_causal_attention = .{
            .batch = 1,
            .seq_len = 1,
            .num_heads = 2,
            .num_kv_heads = 2,
            .head_dim = 2,
            .layer_index = 3,
        } },
        .output_shape = Shape.init(.f32, &.{ 1, 4 }),
        .inputs = .{ q, k, v, null_node },
        .num_inputs = 3,
    });
    try graph.markOutput(gqa);

    const overrides = [_]NodeNameOverride{
        .{ .node_id = q, .name = "input_ids" },
        .{ .node_id = gqa, .name = "logits" },
    };
    const bindings = [_]SemanticDecoderGqaBinding{
        .{ .node_id = gqa, .layer_index = 3 },
    };
    const bytes = try exportGraph(alloc, &graph, .{
        .node_name_overrides = &overrides,
        .semantic_decoder_gqa_bindings = &bindings,
    });
    defer alloc.free(bytes);

    var parsed = try proto.parseModelProto(alloc, bytes);
    defer parsed.deinit(alloc);

    const g = parsed.graph.?;
    try std.testing.expectEqual(@as(usize, 5), g.inputs.len);
    try std.testing.expectEqualStrings("input_ids", g.inputs[0].name);
    try std.testing.expectEqualStrings("k", g.inputs[1].name);
    try std.testing.expectEqualStrings("v", g.inputs[2].name);
    try std.testing.expectEqualStrings("past_key_values.3.key", g.inputs[3].name);
    try std.testing.expectEqualStrings("past_key_values.3.value", g.inputs[4].name);
    try std.testing.expectEqual(@as(usize, 3), g.outputs.len);
    try std.testing.expectEqualStrings("logits", g.outputs[0].name);
    try std.testing.expectEqualStrings("present.3.key", g.outputs[1].name);
    try std.testing.expectEqualStrings("present.3.value", g.outputs[2].name);

    var saw_concat = false;
    var saw_softmax = false;
    for (g.nodes) |node| {
        if (std.mem.eql(u8, node.op_type, "Concat")) saw_concat = true;
        if (std.mem.eql(u8, node.op_type, "Softmax")) saw_softmax = true;
    }
    try std.testing.expect(saw_concat);
    try std.testing.expect(saw_softmax);
}

test "exportGraph emits semantic single-step decoder GQA ABI" {
    const alloc = std.testing.allocator;

    var graph = Graph.init(alloc);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);

    const q = try builder.parameter("q", Shape.init(.f32, &.{ 1, 8 }));
    const k = try builder.parameter("k", Shape.init(.f32, &.{ 1, 4 }));
    const v = try builder.parameter("v", Shape.init(.f32, &.{ 1, 4 }));
    const gqa = try graph.addNode(.{
        .op = .{ .fused_gqa_causal_attention = .{
            .batch = 1,
            .seq_len = 1,
            .num_heads = 4,
            .num_kv_heads = 2,
            .head_dim = 2,
            .layer_index = 7,
        } },
        .output_shape = Shape.init(.f32, &.{ 1, 8 }),
        .inputs = .{ q, k, v, null_node },
        .num_inputs = 3,
    });
    try graph.markOutput(gqa);

    const overrides = [_]NodeNameOverride{
        .{ .node_id = q, .name = "input_ids" },
        .{ .node_id = gqa, .name = "logits" },
    };
    const bindings = [_]SemanticDecoderGqaBinding{
        .{ .node_id = gqa, .layer_index = 7 },
    };
    const bytes = try exportGraph(alloc, &graph, .{
        .node_name_overrides = &overrides,
        .semantic_decoder_gqa_bindings = &bindings,
    });
    defer alloc.free(bytes);

    var parsed = try proto.parseModelProto(alloc, bytes);
    defer parsed.deinit(alloc);

    const g = parsed.graph.?;
    try std.testing.expectEqual(@as(usize, 5), g.inputs.len);
    try std.testing.expectEqualStrings("input_ids", g.inputs[0].name);
    try std.testing.expectEqualStrings("k", g.inputs[1].name);
    try std.testing.expectEqualStrings("v", g.inputs[2].name);
    try std.testing.expectEqualStrings("past_key_values.7.key", g.inputs[3].name);
    try std.testing.expectEqualStrings("past_key_values.7.value", g.inputs[4].name);
    try std.testing.expectEqual(@as(usize, 3), g.outputs.len);
    try std.testing.expectEqualStrings("logits", g.outputs[0].name);
    try std.testing.expectEqualStrings("present.7.key", g.outputs[1].name);
    try std.testing.expectEqualStrings("present.7.value", g.outputs[2].name);

    var tile_count: usize = 0;
    var saw_softmax = false;
    for (g.nodes) |node| {
        if (std.mem.eql(u8, node.op_type, "Tile")) tile_count += 1;
        if (std.mem.eql(u8, node.op_type, "Softmax")) saw_softmax = true;
    }
    try std.testing.expectEqual(@as(usize, 2), tile_count);
    try std.testing.expect(saw_softmax);
}

test "exportGraph emits semantic multi-token prefill GQA ABI with causal mask" {
    const alloc = std.testing.allocator;

    var graph = Graph.init(alloc);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);

    const q = try builder.parameter("q", Shape.init(.f32, &.{ 2, 8 }));
    const k = try builder.parameter("k", Shape.init(.f32, &.{ 2, 4 }));
    const v = try builder.parameter("v", Shape.init(.f32, &.{ 2, 4 }));
    const gqa = try graph.addNode(.{
        .op = .{ .fused_gqa_causal_attention = .{
            .batch = 1,
            .seq_len = 2,
            .num_heads = 4,
            .num_kv_heads = 2,
            .head_dim = 2,
            .layer_index = 9,
        } },
        .output_shape = Shape.init(.f32, &.{ 2, 8 }),
        .inputs = .{ q, k, v, null_node },
        .num_inputs = 3,
    });
    try graph.markOutput(gqa);

    const overrides = [_]NodeNameOverride{
        .{ .node_id = q, .name = "input_ids" },
        .{ .node_id = gqa, .name = "logits" },
    };
    const bindings = [_]SemanticDecoderGqaBinding{
        .{ .node_id = gqa, .layer_index = 9 },
    };
    const bytes = try exportGraph(alloc, &graph, .{
        .node_name_overrides = &overrides,
        .semantic_decoder_gqa_bindings = &bindings,
    });
    defer alloc.free(bytes);

    var parsed = try proto.parseModelProto(alloc, bytes);
    defer parsed.deinit(alloc);

    const g = parsed.graph.?;
    try std.testing.expectEqual(@as(usize, 5), g.inputs.len);
    try std.testing.expectEqualStrings("past_key_values.9.key", g.inputs[3].name);
    try std.testing.expectEqualStrings("past_key_values.9.value", g.inputs[4].name);
    try std.testing.expectEqual(@as(usize, 3), g.outputs.len);
    try std.testing.expectEqualStrings("present.9.key", g.outputs[1].name);
    try std.testing.expectEqualStrings("present.9.value", g.outputs[2].name);

    var saw_add_mask = false;
    var saw_softmax = false;
    for (g.nodes) |node| {
        if (std.mem.eql(u8, node.op_type, "Add") and std.mem.indexOf(u8, node.name, "__semantic_masked") != null) saw_add_mask = true;
        if (std.mem.eql(u8, node.op_type, "Softmax")) saw_softmax = true;
    }
    try std.testing.expect(saw_add_mask);
    try std.testing.expect(saw_softmax);
}

test "exportGraph — graph with constant" {
    const alloc = std.testing.allocator;

    var graph = Graph.init(alloc);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);

    const x = try builder.parameter("x", Shape.init(.f32, &.{4}));
    const bias = try builder.tensorConst(&.{ 0.1, 0.2, 0.3, 0.4 }, Shape.init(.f32, &.{4}));
    const sum = try builder.add(x, bias);
    try graph.markOutput(sum);

    const bytes = try exportGraph(alloc, &graph, .{});
    defer alloc.free(bytes);

    var parsed = try proto.parseModelProto(alloc, bytes);
    defer parsed.deinit(alloc);
    const g = parsed.graph.?;

    // One Add node, one initializer (the constant)
    try std.testing.expectEqual(@as(usize, 1), g.nodes.len);
    try std.testing.expectEqual(@as(usize, 1), g.initializers.len);
    // Only the parameter should be a graph input; constants stay as initializers.
    try std.testing.expectEqual(@as(usize, 1), g.inputs.len);
    try std.testing.expectEqualStrings("x", g.inputs[0].name);
}

test "exportGraph preserves typed graph constant raw bytes" {
    const alloc = std.testing.allocator;

    var graph = Graph.init(alloc);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);

    const c = try builder.tensorConst(&.{ -1.0, 64.0 }, Shape.init(.i64, &.{2}));
    try graph.markOutput(c);

    const bytes = try exportGraph(alloc, &graph, .{});
    defer alloc.free(bytes);

    var parsed = try proto.parseModelProto(alloc, bytes);
    defer parsed.deinit(alloc);
    const g = parsed.graph.?;

    try std.testing.expectEqual(@as(usize, 1), g.initializers.len);
    try std.testing.expectEqual(DataType.int64, g.initializers[0].data_type);
    try std.testing.expectEqual(@as(usize, 2 * @sizeOf(i64)), g.initializers[0].raw_data.len);
    const values: [*]align(1) const i64 = @ptrCast(g.initializers[0].raw_data.ptr);
    try std.testing.expectEqual(@as(i64, -1), values[0]);
    try std.testing.expectEqual(@as(i64, 64), values[1]);
}

test "exportGraph — transpose with perm" {
    const alloc = std.testing.allocator;

    var graph = Graph.init(alloc);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);

    const x = try builder.parameter("x", Shape.init(.f32, &.{ 2, 3, 4 }));
    const t = try builder.transpose(x, &.{ 0, 2, 1 });
    try graph.markOutput(t);

    const bytes = try exportGraph(alloc, &graph, .{});
    defer alloc.free(bytes);

    var parsed = try proto.parseModelProto(alloc, bytes);
    defer parsed.deinit(alloc);
    const g = parsed.graph.?;

    try std.testing.expectEqual(@as(usize, 1), g.nodes.len);
    try std.testing.expectEqualStrings("Transpose", g.nodes[0].op_type);
    try std.testing.expectEqual(@as(usize, 1), g.nodes[0].attributes.len);
    try std.testing.expectEqualStrings("perm", g.nodes[0].attributes[0].name);
    const perm = g.nodes[0].attributes[0].ints;
    try std.testing.expectEqual(@as(usize, 3), perm.len);
    try std.testing.expectEqual(@as(i64, 0), perm[0]);
    try std.testing.expectEqual(@as(i64, 2), perm[1]);
    try std.testing.expectEqual(@as(i64, 1), perm[2]);
}

test "exportGraph roundtrip — export then import" {
    const alloc = std.testing.allocator;

    // Build graph: relu(x + bias)
    var graph = Graph.init(alloc);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);

    const x = try builder.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const bias = try builder.tensorConst(&.{ 0.1, 0.2, 0.3, 0.4 }, Shape.init(.f32, &.{4}));
    const sum = try builder.add(x, bias);
    const relu = try builder.relu(sum);
    try graph.markOutput(relu);

    // Export to ONNX bytes
    const bytes = try exportGraph(alloc, &graph, .{});
    defer alloc.free(bytes);

    // Import back as ONNX model
    const onnx = @import("root.zig");
    var model = try onnx.parse(alloc, bytes);
    defer model.deinit();

    try std.testing.expect(model.graph() != null);
    try std.testing.expectEqual(@as(u64, 17), model.opsetVersion());
}

test "exportGraph with lower_fused decomposes fused ops" {
    const alloc = std.testing.allocator;

    // Build graph with fused relu (has vjp_alternate decomposition)
    var graph = Graph.init(alloc);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);

    const x = try builder.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const relu = try builder.relu(x);
    try graph.markOutput(relu);

    // Export without lowering — should have "Relu" node
    const bytes_fused = try exportGraph(alloc, &graph, .{});
    defer alloc.free(bytes_fused);
    var parsed_fused = try proto.parseModelProto(alloc, bytes_fused);
    defer parsed_fused.deinit(alloc);
    const gf = parsed_fused.graph.?;
    // Find the Relu op
    var has_relu = false;
    for (gf.nodes) |*n| {
        if (std.mem.eql(u8, n.op_type, "Relu")) has_relu = true;
    }
    try std.testing.expect(has_relu);

    // Export with lowering — fused relu should be decomposed to primitives
    const bytes_lowered = try exportGraph(alloc, &graph, .{ .lower_fused = true });
    defer alloc.free(bytes_lowered);
    var parsed_lowered = try proto.parseModelProto(alloc, bytes_lowered);
    defer parsed_lowered.deinit(alloc);
    const gl = parsed_lowered.graph.?;
    // Should NOT have "Relu" — it should be decomposed into Where + Less etc.
    var has_relu_lowered = false;
    var has_where = false;
    for (gl.nodes) |*n| {
        if (std.mem.eql(u8, n.op_type, "Relu")) has_relu_lowered = true;
        if (std.mem.eql(u8, n.op_type, "Where")) has_where = true;
    }
    try std.testing.expect(!has_relu_lowered);
    // Lowered graph should contain the decomposed Where primitive
    try std.testing.expect(has_where);
}

test "exportGraph — reshape emits shape initializer" {
    const alloc = std.testing.allocator;

    // Build graph: reshape(x, [3, 4]) where x has shape [2, 6]
    var graph = Graph.init(alloc);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);

    const x = try builder.parameter("x", Shape.init(.f32, &.{ 2, 6 }));
    const r = try builder.reshape(x, Shape.init(.f32, &.{ 3, 4 }));
    try graph.markOutput(r);

    // Export
    const bytes = try exportGraph(alloc, &graph, .{});
    defer alloc.free(bytes);

    // Parse and verify structure
    var parsed = try proto.parseModelProto(alloc, bytes);
    defer parsed.deinit(alloc);
    const g = parsed.graph.?;

    // Should contain one Reshape node with two inputs (data + shape)
    try std.testing.expectEqual(@as(usize, 1), g.nodes.len);
    try std.testing.expectEqualStrings("Reshape", g.nodes[0].op_type);
    try std.testing.expectEqual(@as(usize, 2), g.nodes[0].inputs.len);

    // The second input should reference our shape initializer
    const shape_input_name = g.nodes[0].inputs[1];
    try std.testing.expect(std.mem.startsWith(u8, shape_input_name, "shape_"));

    // A matching i64 initializer should exist with dims [2]
    var found_shape_init = false;
    for (g.initializers) |*init| {
        if (std.mem.eql(u8, init.name, shape_input_name)) {
            try std.testing.expectEqual(DataType.int64, init.data_type);
            try std.testing.expectEqual(@as(usize, 1), init.dims.len);
            try std.testing.expectEqual(@as(i64, 2), init.dims[0]);
            // raw_data is 2 × i64 = 16 bytes, encoding [3, 4]
            try std.testing.expectEqual(@as(usize, 16), init.raw_data.len);
            const shape_vals: [*]align(1) const i64 = @ptrCast(init.raw_data.ptr);
            try std.testing.expectEqual(@as(i64, 3), shape_vals[0]);
            try std.testing.expectEqual(@as(i64, 4), shape_vals[1]);
            found_shape_init = true;
            break;
        }
    }
    try std.testing.expect(found_shape_init);
}

test "exportGraph rejects reshape with multiple inferred dims" {
    const alloc = std.testing.allocator;

    var graph = Graph.init(alloc);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);

    const x = try builder.parameter("x", Shape.init(.f32, &.{ 2, 6 }));
    const r = try builder.reshape(x, Shape.init(.f32, &.{ -1, -1 }));
    try graph.markOutput(r);

    try std.testing.expectError(error.UnsupportedShape, exportGraph(alloc, &graph, .{}));
}

test "exportGraph roundtrip — reshape re-imports to termite graph" {
    const alloc = std.testing.allocator;

    // Build graph: reshape(x + bias, [3, 4]) where x is [2, 6]
    var graph = Graph.init(alloc);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);

    const x = try builder.parameter("x", Shape.init(.f32, &.{ 2, 6 }));
    const bias = try builder.tensorConst(
        &.{ 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2 },
        Shape.init(.f32, &.{ 2, 6 }),
    );
    const sum = try builder.add(x, bias);
    const r = try builder.reshape(sum, Shape.init(.f32, &.{ 3, 4 }));
    try graph.markOutput(r);

    // Export to ONNX bytes
    const bytes = try exportGraph(alloc, &graph, .{});
    defer alloc.free(bytes);

    // Import back and convert to a termite graph
    const onnx = @import("root.zig");
    var model = try onnx.parse(alloc, bytes);
    defer model.deinit();

    var result = try model.convertToGraph(alloc);
    defer result.deinit(alloc);

    // Walk the converted graph — find a reshape node with new_shape [3, 4]
    var found_reshape = false;
    const count = result.graph.nodeCount();
    for (0..count) |i| {
        const n = result.graph.node(@intCast(i));
        if (n.op == .reshape) {
            const attrs = n.op.reshape;
            try std.testing.expectEqual(@as(u8, 2), attrs.new_shape.rank());
            try std.testing.expectEqual(@as(i64, 3), attrs.new_shape.dim(0));
            try std.testing.expectEqual(@as(i64, 4), attrs.new_shape.dim(1));
            found_reshape = true;
        }
    }
    try std.testing.expect(found_reshape);
}

test "termiteDTypeToOnnx mapping" {
    try std.testing.expectEqual(DataType.float32, termiteDTypeToOnnx(.f32));
    try std.testing.expectEqual(DataType.float16, termiteDTypeToOnnx(.f16));
    try std.testing.expectEqual(DataType.bfloat16, termiteDTypeToOnnx(.bf16));
    try std.testing.expectEqual(DataType.int32, termiteDTypeToOnnx(.i32));
    try std.testing.expectEqual(DataType.int64, termiteDTypeToOnnx(.i64));
    try std.testing.expectEqual(DataType.uint8, termiteDTypeToOnnx(.u8));
    try std.testing.expectEqual(DataType.bool_, termiteDTypeToOnnx(.bool_));
}

test "makeExternalConstantTensor writes f16-sized payload" {
    const alloc = std.testing.allocator;
    var ext = ExternalDataBuilder{
        .relative_path = try alloc.dupe(u8, "weights.bin"),
    };
    defer ext.deinit(alloc);

    const shape = Shape.init(.f16, &.{ 2, 2 });
    const data = [_]f32{ 1.0, 2.0, 3.0, 4.0 };
    var tensor = try makeExternalConstantTensor(alloc, &ext, "weight", shape, &data);
    defer freeTensorProto(alloc, &tensor);

    try std.testing.expectEqual(@as(usize, 8), ext.bytes.items.len);
    try std.testing.expectEqual(DataType.float16, tensor.data_type);
}

test "makeExternalConstantTensor writes bf16-sized payload" {
    const alloc = std.testing.allocator;
    var ext = ExternalDataBuilder{
        .relative_path = try alloc.dupe(u8, "weights.bin"),
    };
    defer ext.deinit(alloc);

    const shape = Shape.init(.bf16, &.{ 2, 2 });
    const data = [_]f32{ 1.0, 2.0, 3.0, 4.0 };
    var tensor = try makeExternalConstantTensor(alloc, &ext, "weight", shape, &data);
    defer freeTensorProto(alloc, &tensor);

    try std.testing.expectEqual(@as(usize, 8), ext.bytes.items.len);
    try std.testing.expectEqual(DataType.bfloat16, tensor.data_type);
}

test "exportGraph uses raw-byte parameter initializer payload" {
    const alloc = std.testing.allocator;

    var graph = Graph.init(alloc);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);

    const weight = try builder.parameter("weight", Shape.init(.f16, &.{2}));
    try graph.markOutput(weight);

    const raw = [_]u8{ 0x00, 0x3C, 0x00, 0x40 };
    const init = ParameterInitializer{
        .name = "weight",
        .shape = Shape.init(.f16, &.{2}),
        .data = .{ .raw_bytes = &raw },
    };

    const bytes = try exportGraph(alloc, &graph, .{ .parameter_initializers = &.{init} });
    defer alloc.free(bytes);

    var parsed = try proto.parseModelProto(alloc, bytes);
    defer parsed.deinit(alloc);
    const g = parsed.graph.?;
    try std.testing.expectEqual(@as(usize, 1), g.initializers.len);
    try std.testing.expectEqual(DataType.float16, g.initializers[0].data_type);
    try std.testing.expectEqual(@as(usize, raw.len), g.initializers[0].raw_data.len);
    try std.testing.expectEqualSlices(u8, &raw, g.initializers[0].raw_data);
}

test "exportGraphWithExternalData emits q8_0 block parameter via DequantizeLinear" {
    const alloc = std.testing.allocator;

    var graph = Graph.init(alloc);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);

    const weight = try builder.parameter("weight", Shape.init(.f32, &.{ 2, 32 }));
    try graph.markOutput(weight);

    const values = @as([64]u8, @splat(128));
    const scales = [_]f32{ 0.25, 0.5 };
    const init = ParameterInitializer{
        .name = "weight",
        .shape = Shape.init(.f32, &.{ 2, 32 }),
        .data = .{
            .q8_0_block = .{
                .scale_shape = Shape.init(.f32, &.{ 2, 1 }),
                .values_u8 = &values,
                .scales_f32 = &scales,
                .zero_point_u8 = 128,
                .axis = 1,
                .block_size = 32,
                .source_byte_len = 66,
            },
        },
    };

    var result = try exportGraphWithExternalData(alloc, &graph, .{
        .opset_version = 21,
        .parameter_initializers = &.{init},
    }, "weights.bin");
    defer result.deinit(alloc);

    try std.testing.expect(result.external_data != null);
    try std.testing.expectEqualStrings("weights.bin", result.external_data.?.relative_path);
    try std.testing.expectEqual(@as(usize, values.len + (scales.len * @sizeOf(f32)) + scales.len), result.external_data.?.bytes.len);

    var parsed = try proto.parseModelProto(alloc, result.model_bytes);
    defer parsed.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), parsed.opset_import.len);
    try std.testing.expectEqual(@as(u64, 21), parsed.opset_import[0].version);

    const g = parsed.graph.?;
    try std.testing.expectEqual(@as(usize, 3), g.initializers.len);
    try std.testing.expectEqual(@as(usize, 1), g.nodes.len);
    try std.testing.expectEqualStrings("DequantizeLinear", g.nodes[0].op_type);
    try std.testing.expectEqual(@as(usize, 3), g.nodes[0].inputs.len);
    try std.testing.expectEqualStrings("weight", g.nodes[0].outputs[0]);
    try std.testing.expectEqual(@as(usize, 2), g.nodes[0].attributes.len);
    try std.testing.expectEqual(proto.AttributeType.int, g.nodes[0].attributes[0].attr_type);
    try std.testing.expectEqual(proto.AttributeType.int, g.nodes[0].attributes[1].attr_type);

    try std.testing.expect(g.initializers[0].isExternal());
    try std.testing.expect(g.initializers[1].isExternal());
    try std.testing.expect(g.initializers[2].isExternal());
    try std.testing.expectEqual(DataType.uint8, g.initializers[0].data_type);
    try std.testing.expectEqual(DataType.float32, g.initializers[1].data_type);
    try std.testing.expectEqual(DataType.uint8, g.initializers[2].data_type);
    try std.testing.expectEqual(@as(i64, @intCast(values.len)), g.initializers[0].externalDataInfo().length);
    try std.testing.expectEqual(@as(i64, @intCast(scales.len * @sizeOf(f32))), g.initializers[1].externalDataInfo().length);
    try std.testing.expectEqual(@as(i64, @intCast(scales.len)), g.initializers[2].externalDataInfo().length);
}

test "exportGraph rejects inline q8_0 block parameter initializers" {
    const alloc = std.testing.allocator;

    var graph = Graph.init(alloc);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);

    const weight = try builder.parameter("weight", Shape.init(.f32, &.{ 1, 32 }));
    try graph.markOutput(weight);

    const values = @as([32]u8, @splat(128));
    const scales = [_]f32{0.25};
    const init = ParameterInitializer{
        .name = "weight",
        .shape = Shape.init(.f32, &.{ 1, 32 }),
        .data = .{
            .q8_0_block = .{
                .scale_shape = Shape.init(.f32, &.{ 1, 1 }),
                .values_u8 = &values,
                .scales_f32 = &scales,
                .zero_point_u8 = 128,
                .axis = 1,
                .block_size = 32,
                .source_byte_len = 34,
            },
        },
    };

    try std.testing.expectError(error.UnsupportedShape, exportGraph(alloc, &graph, .{
        .opset_version = 21,
        .parameter_initializers = &.{init},
    }));
}

test "exportGraph lowers fused_rope to standard ONNX ops" {
    const alloc = std.testing.allocator;

    var graph = Graph.init(alloc);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);

    const x = try builder.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const rope = try graph.addNode(.{
        .op = .{ .fused_rope = .{
            .seq_len = 2,
            .head_dim = 4,
            .rope_dim = 4,
            .theta = 10000.0,
            .freq_scale = 1.0,
            .position_offset = 0,
            .consecutive_pairs = false,
        } },
        .output_shape = Shape.init(.f32, &.{ 2, 4 }),
        .inputs = .{ x, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    try graph.markOutput(rope);

    const bytes = try exportGraph(alloc, &graph, .{});
    defer alloc.free(bytes);

    var parsed = try proto.parseModelProto(alloc, bytes);
    defer parsed.deinit(alloc);
    const g = parsed.graph.?;

    var saw_matmul = false;
    var saw_mul = false;
    var saw_add = false;
    var saw_custom_rope = false;
    for (g.nodes) |node| {
        if (std.mem.eql(u8, node.op_type, "MatMul")) saw_matmul = true;
        if (std.mem.eql(u8, node.op_type, "Mul")) saw_mul = true;
        if (std.mem.eql(u8, node.op_type, "Add")) saw_add = true;
        if (std.mem.eql(u8, node.op_type, "fused_rope")) saw_custom_rope = true;
    }
    try std.testing.expect(saw_matmul);
    try std.testing.expect(saw_mul);
    try std.testing.expect(saw_add);
    try std.testing.expect(!saw_custom_rope);
}

test "exportGraph emits reduce axes as tensor inputs for opset 21" {
    const alloc = std.testing.allocator;

    var graph = Graph.init(alloc);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);

    const x = try builder.parameter("x", Shape.init(.f32, &.{ 2, 3, 4 }));
    const reduced = try builder.reduceMax(x, &.{2});
    try graph.markOutput(reduced);

    const bytes = try exportGraph(alloc, &graph, .{ .opset_version = 21 });
    defer alloc.free(bytes);

    var parsed = try proto.parseModelProto(alloc, bytes);
    defer parsed.deinit(alloc);
    const g = parsed.graph.?;

    var reduce_node: ?*const NodeProto = null;
    for (g.nodes) |*node| {
        if (std.mem.eql(u8, node.op_type, "ReduceMax")) {
            reduce_node = node;
            break;
        }
    }
    const node = reduce_node orelse return error.MissingValue;
    try std.testing.expectEqual(@as(usize, 2), node.inputs.len);
    try std.testing.expectEqualStrings("axes_", node.inputs[1][0..5]);
    for (node.attributes) |attr| {
        try std.testing.expect(!std.mem.eql(u8, attr.name, "axes"));
    }
}

test "fused_rope permutation does not add identity on partial half-split partner lanes" {
    const alloc = std.testing.allocator;

    const data = try buildRopePermutationMatrixData(alloc, .f32, 8, 4, false, "rope");
    defer alloc.free(data.name);
    defer alloc.free(data.raw_data);

    const readF32 = struct {
        fn at(bytes: []const u8, idx: usize) f32 {
            return @bitCast(std.mem.readInt(u32, bytes[idx * @sizeOf(f32) ..][0..@sizeOf(f32)], .little));
        }
    }.at;

    const head_dim = 8;
    try std.testing.expectEqual(@as(f32, -1.0), readF32(data.raw_data, 4 * head_dim + 0));
    try std.testing.expectEqual(@as(f32, 1.0), readF32(data.raw_data, 0 * head_dim + 4));
    try std.testing.expectEqual(@as(f32, -1.0), readF32(data.raw_data, 5 * head_dim + 1));
    try std.testing.expectEqual(@as(f32, 1.0), readF32(data.raw_data, 1 * head_dim + 5));
    try std.testing.expectEqual(@as(f32, 0.0), readF32(data.raw_data, 4 * head_dim + 4));
    try std.testing.expectEqual(@as(f32, 0.0), readF32(data.raw_data, 5 * head_dim + 5));
}
