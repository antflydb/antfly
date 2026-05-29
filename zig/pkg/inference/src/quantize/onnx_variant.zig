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
const ml = @import("ml");
const onnx_graph = @import("onnx_graph");

const compat = @import("../io/compat.zig");
const c_file = @import("../util/c_file.zig");
const tensor_mod = @import("../backends/tensor.zig");
const export_source_mod = @import("../models/export_source.zig");
const constants = @import("constants.zig");
const options = @import("options.zig");
const variants_manifest = @import("variants_manifest.zig");

const print = std.debug.print;
const Allocator = std.mem.Allocator;

pub const default_min_elements: usize = constants.default_min_elements;
const q8_block_size: usize = 32;

const QuantizeStats = struct {
    onnx_files: usize = 0,
    copied_files: usize = 0,
    skipped_external_data_files: usize = 0,
};

const Manifest = struct {
    source_model_dir: []const u8,
    format: []const u8,
    min_elements: usize,
    onnx_files: usize,
    copied_files: usize,
    skipped_external_data_files: usize,
};

pub fn run(allocator: Allocator, io: std.Io, opts: options.Options) !void {
    if (!std.mem.eql(u8, opts.format, "q8_0")) return error.UnsupportedQuantizationFormat;
    if (opts.quantize_include_prefixes != null or
        opts.quantize_exclude_prefixes != null or
        opts.projector_output_path != null or
        opts.projector_format != null or
        opts.dry_run)
    {
        return error.InvalidArguments;
    }

    var owned_output_dir: ?[]u8 = null;
    const output_dir = if (opts.output_path) |out|
        out
    else if (isClipclapSourceDir(allocator, opts.model_dir))
        opts.model_dir
    else blk: {
        owned_output_dir = try defaultVariantDir(allocator, opts.model_dir, opts.format);
        break :blk owned_output_dir.?;
    };
    defer if (owned_output_dir) |path| allocator.free(path);

    try compat.cwd().createDirPath(io, output_dir);

    var stats: QuantizeStats = .{};
    try createVariantDir(allocator, io, opts, output_dir, &stats);
    try writeManifest(allocator, io, opts, output_dir, stats);
    try variants_manifest.writeClipclapVariantsManifest(allocator, io, output_dir);

    print(
        "quantized model format={s} source={s} output={s} onnx_files={d} copied_files={d} skipped_external_data={d}\n",
        .{
            opts.format,
            opts.model_dir,
            output_dir,
            stats.onnx_files,
            stats.copied_files,
            stats.skipped_external_data_files,
        },
    );
}

pub fn defaultVariantDir(allocator: Allocator, source_dir: []const u8, format: []const u8) ![]u8 {
    const trimmed = trimRightSlash(source_dir);
    const base = std.fs.path.basename(trimmed);
    const parent = std.fs.path.dirname(trimmed) orelse ".";
    const variant_base = try std.fmt.allocPrint(allocator, "{s}-{s}", .{ base, format });
    defer allocator.free(variant_base);
    return std.fs.path.join(allocator, &.{ parent, variant_base });
}

fn trimRightSlash(value: []const u8) []const u8 {
    var end = value.len;
    while (end > 0 and value[end - 1] == '/') : (end -= 1) {}
    return value[0..end];
}

fn createVariantDir(
    allocator: Allocator,
    io: std.Io,
    opts: options.Options,
    output_dir: []const u8,
    stats: *QuantizeStats,
) !void {
    const same_dir = try pathsReferToSameDirectory(allocator, io, opts.model_dir, output_dir);
    var dir = try compat.cwd().openDir(io, opts.model_dir, .{ .iterate = true });
    defer dir.close(io);

    var names = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (names.items) |name| allocator.free(name);
        names.deinit(allocator);
    }
    var iter = dir.iterate();
    while (try iter.next(io)) |entry| {
        if (entry.kind != .file) continue;
        try names.append(allocator, try allocator.dupe(u8, entry.name));
    }

    for (names.items) |name| {
        if (std.mem.endsWith(u8, name, ".onnx")) {
            if (!same_dir) {
                try copyModelFile(allocator, io, opts.model_dir, output_dir, name);
                stats.copied_files += 1;
            }
            if (!variants_manifest.isVariantOnnxName(name)) {
                try rewriteOnnxFile(allocator, io, opts, output_dir, name);
                stats.onnx_files += 1;
            }
        } else {
            if (!same_dir and !std.mem.eql(u8, name, "quantization_manifest.json")) {
                try copyModelFile(allocator, io, opts.model_dir, output_dir, name);
                stats.copied_files += 1;
            } else if (std.mem.endsWith(u8, name, ".onnx.data")) {
                stats.skipped_external_data_files += 1;
            }
        }
    }
}

fn pathsEqualLexically(a: []const u8, b: []const u8) bool {
    return std.mem.eql(u8, trimRightSlash(a), trimRightSlash(b));
}

fn isClipclapSourceDir(allocator: Allocator, model_dir: []const u8) bool {
    const required = [_][]const u8{
        "text_model.onnx",
        "visual_model.onnx",
        "audio_model.onnx",
        "text_projection.onnx",
        "visual_projection.onnx",
        "audio_projection.onnx",
    };
    for (required) |name| {
        if (!c_file.fileExistsInDir(allocator, model_dir, name)) return false;
    }
    return true;
}

fn copyModelFile(
    allocator: Allocator,
    io: std.Io,
    source_dir: []const u8,
    output_dir: []const u8,
    name: []const u8,
) !void {
    const source_path = try std.fs.path.join(allocator, &.{ source_dir, name });
    defer allocator.free(source_path);
    const target_path = try std.fs.path.join(allocator, &.{ output_dir, name });
    defer allocator.free(target_path);

    try copyFileStreaming(allocator, io, source_path, target_path);
}

fn copyFileStreaming(allocator: Allocator, io: std.Io, source_path: []const u8, target_path: []const u8) !void {
    if (try pathsReferToSameFile(allocator, io, source_path, target_path)) return;

    var src = try compat.cwd().openFile(io, source_path, .{});
    defer src.close(io);
    var dst = try compat.cwd().createFile(io, target_path, .{ .truncate = true });
    defer dst.close(io);

    var buf: [1024 * 1024]u8 = undefined;
    while (true) {
        const n = src.readStreaming(io, &.{buf[0..]}) catch |err| switch (err) {
            error.EndOfStream => 0,
            else => return err,
        };
        if (n == 0) break;
        try dst.writeStreamingAll(io, buf[0..n]);
    }
}

fn pathsReferToSameDirectory(allocator: Allocator, io: std.Io, a: []const u8, b: []const u8) !bool {
    if (pathsEqualLexically(a, b)) return true;
    const a_real = try compat.cwd().realPathFileAlloc(io, a, allocator);
    defer allocator.free(a_real);
    const b_real = compat.cwd().realPathFileAlloc(io, b, allocator) catch |err| switch (err) {
        error.FileNotFound => return false,
        else => return err,
    };
    defer allocator.free(b_real);
    return std.mem.eql(u8, a_real, b_real);
}

fn pathsReferToSameFile(allocator: Allocator, io: std.Io, source_path: []const u8, target_path: []const u8) !bool {
    if (pathsEqualLexically(source_path, target_path)) return true;
    const source_real = try compat.cwd().realPathFileAlloc(io, source_path, allocator);
    defer allocator.free(source_real);
    const target_real = compat.cwd().realPathFileAlloc(io, target_path, allocator) catch |err| switch (err) {
        error.FileNotFound => return false,
        else => return err,
    };
    defer allocator.free(target_real);
    return std.mem.eql(u8, source_real, target_real);
}

fn rewriteOnnxFile(
    allocator: Allocator,
    io: std.Io,
    opts: options.Options,
    output_dir: []const u8,
    name: []const u8,
) !void {
    const source_path = try std.fs.path.join(allocator, &.{ opts.model_dir, name });
    defer allocator.free(source_path);
    const output_name = try variants_manifest.variantOnnxName(allocator, name, opts.format);
    defer allocator.free(output_name);
    const output_path = try std.fs.path.join(allocator, &.{ output_dir, output_name });
    defer allocator.free(output_path);
    const external_name = try variants_manifest.variantOnnxDataName(allocator, output_name);
    defer allocator.free(external_name);
    const external_path = try std.fs.path.join(allocator, &.{ output_dir, external_name });
    defer allocator.free(external_path);

    const bytes = try c_file.readFileMax(allocator, source_path, std.math.maxInt(usize));
    defer allocator.free(bytes);

    var model = try onnx_graph.parseWithBaseDir(allocator, bytes, opts.model_dir);
    defer model.deinit();

    var proto_string_arena = std.heap.ArenaAllocator.init(allocator);
    defer proto_string_arena.deinit();

    try rewriteInitializersAsQ8_0(allocator, proto_string_arena.allocator(), &model, opts.min_elements, external_name, external_path);
    const model_bytes = try onnx_graph.serializeModel(allocator, &model.onnx);
    defer allocator.free(model_bytes);
    try compat.cwd().writeFile(io, .{ .sub_path = output_path, .data = model_bytes });
}

fn writeManifest(
    allocator: Allocator,
    io: std.Io,
    opts: options.Options,
    output_dir: []const u8,
    stats: QuantizeStats,
) !void {
    const manifest = Manifest{
        .source_model_dir = opts.model_dir,
        .format = opts.format,
        .min_elements = opts.min_elements,
        .onnx_files = stats.onnx_files,
        .copied_files = stats.copied_files,
        .skipped_external_data_files = stats.skipped_external_data_files,
    };
    const json = try std.json.Stringify.valueAlloc(allocator, manifest, .{ .whitespace = .indent_2 });
    defer allocator.free(json);

    const path = try std.fs.path.join(allocator, &.{ output_dir, "quantization_manifest.json" });
    defer allocator.free(path);
    try compat.cwd().writeFile(io, .{ .sub_path = path, .data = json });
}

const ExternalDataWriter = struct {
    file: std.Io.File,
    relative_path: []const u8,
    offset: usize = 0,

    fn init(allocator: Allocator, relative_path: []const u8, absolute_path: []const u8) !ExternalDataWriter {
        _ = allocator;
        const file = try compat.cwd().createFile(compat.io(), absolute_path, .{ .truncate = true });
        return .{ .file = file, .relative_path = relative_path };
    }

    fn deinit(self: *ExternalDataWriter) void {
        self.file.close(compat.io());
    }

    fn append(self: *ExternalDataWriter, bytes: []const u8) !ExternalSlice {
        const offset = self.offset;
        try self.file.writeStreamingAll(compat.io(), bytes);
        self.offset += bytes.len;
        return .{ .offset = offset, .length = bytes.len };
    }
};

const ExternalSlice = struct {
    offset: usize,
    length: usize,
};

fn rewriteInitializersAsQ8_0(
    allocator: Allocator,
    string_allocator: Allocator,
    model: *onnx_graph.Model,
    min_elements: usize,
    external_name: []const u8,
    external_path: []const u8,
) !void {
    if (model.onnx.graph == null) return error.OutputNotFound;
    var graph = &model.onnx.graph.?;
    const original_initializers = graph.initializers;
    const original_nodes = graph.nodes;
    var writer = try ExternalDataWriter.init(allocator, external_name, external_path);
    defer writer.deinit();

    var source_ctx = OnnxInitializerSource{
        .model = model,
        .min_elements = min_elements,
    };

    var initializers = std.ArrayListUnmanaged(onnx_graph.TensorProto).empty;
    errdefer {
        for (initializers.items) |*tensor| tensor.deinit(allocator);
        initializers.deinit(allocator);
    }
    var dq_nodes = std.ArrayListUnmanaged(onnx_graph.NodeProto).empty;
    errdefer {
        for (dq_nodes.items) |*node| freeGeneratedNodeContainers(allocator, node);
        dq_nodes.deinit(allocator);
    }
    var used_q8 = false;

    for (original_initializers) |*init| {
        if (try source_ctx.openQ8_0BlockTensorRaw(allocator, .{
            .name = init.name,
            .shape = try onnx_graph.tensor.tensorShape(init),
            .tensor = init,
            .external = if (init.isExternal()) init.externalDataInfo() else null,
        })) |q8| {
            defer {
                var owned = q8;
                owned.deinit(allocator);
            }
            try appendQ8DequantizedInitializer(allocator, string_allocator, &writer, &initializers, &dq_nodes, init.name, q8);
            used_q8 = true;
        } else {
            const raw = try onnx_graph.tensor.extractNativeBytesWithExternal(allocator, init, model.base_dir);
            defer allocator.free(raw);
            try initializers.append(allocator, try makeExternalTensor(
                allocator,
                string_allocator,
                &writer,
                init.name,
                init.dims,
                init.data_type,
                raw,
            ));
        }
    }

    if (used_q8) try ensureDefaultOpsetAtLeast(model, 21);

    const replacement_initializers = try initializers.toOwnedSlice(allocator);
    initializers = .empty;
    errdefer {
        for (replacement_initializers) |*tensor| tensor.deinit(allocator);
        if (replacement_initializers.len > 0) allocator.free(replacement_initializers);
    }

    const nodes = try allocator.alloc(onnx_graph.NodeProto, dq_nodes.items.len + original_nodes.len);
    for (dq_nodes.items, 0..) |node, i| nodes[i] = node;
    for (original_nodes, 0..) |node, i| nodes[dq_nodes.items.len + i] = node;
    dq_nodes.clearRetainingCapacity();
    dq_nodes.deinit(allocator);

    for (original_initializers) |*tensor| tensor.deinit(allocator);
    if (original_initializers.len > 0) allocator.free(original_initializers);
    if (original_nodes.len > 0) allocator.free(original_nodes);

    graph.initializers = replacement_initializers;
    graph.nodes = nodes;
}

fn appendQ8DequantizedInitializer(
    allocator: Allocator,
    string_allocator: Allocator,
    writer: *ExternalDataWriter,
    initializers: *std.ArrayListUnmanaged(onnx_graph.TensorProto),
    nodes: *std.ArrayListUnmanaged(onnx_graph.NodeProto),
    parameter_name: []const u8,
    q8: export_source_mod.Q8_0BlockTensor,
) !void {
    const values_name = try std.fmt.allocPrint(string_allocator, "{s}__q8_values", .{parameter_name});
    const scales_name = try std.fmt.allocPrint(string_allocator, "{s}__q8_scales", .{parameter_name});
    const zp_name = try std.fmt.allocPrint(string_allocator, "{s}__q8_zero_point", .{parameter_name});

    try initializers.append(allocator, try makeExternalTensor(
        allocator,
        string_allocator,
        writer,
        values_name,
        q8.shape,
        .uint8,
        q8.values_u8,
    ));
    try initializers.append(allocator, try makeExternalTensor(
        allocator,
        string_allocator,
        writer,
        scales_name,
        q8.scale_shape,
        .float32,
        std.mem.sliceAsBytes(q8.scales_f32),
    ));

    const zp_count = try shapeElementCount(q8.scale_shape);
    const zero_points = try allocator.alloc(u8, zp_count);
    defer allocator.free(zero_points);
    @memset(zero_points, q8.zero_point_u8);
    try initializers.append(allocator, try makeExternalTensor(
        allocator,
        string_allocator,
        writer,
        zp_name,
        q8.scale_shape,
        .uint8,
        zero_points,
    ));

    const inputs = try allocator.alloc([]const u8, 3);
    errdefer allocator.free(inputs);
    inputs[0] = values_name;
    inputs[1] = scales_name;
    inputs[2] = zp_name;

    const outputs = try allocator.alloc([]const u8, 1);
    errdefer allocator.free(outputs);
    outputs[0] = try string_allocator.dupe(u8, parameter_name);

    const attrs = try allocator.alloc(onnx_graph.proto.AttributeProto, 2);
    errdefer allocator.free(attrs);
    attrs[0] = .{ .name = "axis", .i = q8.axis, .attr_type = .int };
    attrs[1] = .{ .name = "block_size", .i = q8.block_size, .attr_type = .int };

    try nodes.append(allocator, .{
        .inputs = inputs,
        .outputs = outputs,
        .name = try string_allocator.dupe(u8, parameter_name),
        .op_type = "DequantizeLinear",
        .attributes = attrs,
    });
}

fn makeExternalTensor(
    allocator: Allocator,
    string_allocator: Allocator,
    writer: *ExternalDataWriter,
    name: []const u8,
    dims: []const i64,
    dtype: onnx_graph.DataType,
    bytes: []const u8,
) !onnx_graph.TensorProto {
    const slice = try writer.append(bytes);
    const external = try allocator.alloc(onnx_graph.proto.ExternalDataEntry, 3);
    errdefer allocator.free(external);
    external[0] = .{
        .key = "location",
        .value = try string_allocator.dupe(u8, writer.relative_path),
    };
    external[1] = .{
        .key = "offset",
        .value = try std.fmt.allocPrint(string_allocator, "{d}", .{slice.offset}),
    };
    external[2] = .{
        .key = "length",
        .value = try std.fmt.allocPrint(string_allocator, "{d}", .{slice.length}),
    };
    return .{
        .dims = try allocator.dupe(i64, dims),
        .data_type = dtype,
        .name = try string_allocator.dupe(u8, name),
        .external_data = external,
        .data_location = .external,
    };
}

fn ensureDefaultOpsetAtLeast(model: *onnx_graph.Model, version: u64) !void {
    for (model.onnx.opset_import) |*opset| {
        if (opset.domain.len == 0) {
            if (opset.version < version) opset.version = version;
            return;
        }
    }
    const old = model.onnx.opset_import;
    const next = try model.allocator.alloc(onnx_graph.proto.OpsetImport, old.len + 1);
    errdefer model.allocator.free(next);
    @memcpy(next[0..old.len], old);
    const domain = try model.allocator.dupe(u8, "");
    next[old.len] = .{ .domain = domain, .version = version };
    model.onnx.opset_import = next;
    if (old.len > 0) model.allocator.free(old);
}

fn freeGeneratedNodeContainers(allocator: Allocator, node: *onnx_graph.NodeProto) void {
    if (node.inputs.len > 0) allocator.free(node.inputs);
    if (node.outputs.len > 0) allocator.free(node.outputs);
    if (node.attributes.len > 0) allocator.free(node.attributes);
    node.* = .{};
}

const RawStreamContext = struct {
    shape: []i64,
    dtype: tensor_mod.DType,
    bytes: []u8,
};

const OnnxInitializerSource = struct {
    model: *onnx_graph.Model,
    min_elements: usize,

    fn source(self: *OnnxInitializerSource) export_source_mod.Source {
        return .{
            .context = self,
            .open = &openTensor,
            .open_q8_0_block = &openQ8_0BlockTensor,
        };
    }

    fn provider(self: *OnnxInitializerSource) onnx_graph.@"export".ParameterInitializerProvider {
        return .{
            .context = self,
            .load = &loadInitializer,
            .free = &freeInitializer,
        };
    }

    fn loadInitializer(
        raw_context: ?*anyopaque,
        allocator: Allocator,
        name: []const u8,
    ) !?onnx_graph.@"export".ParameterInitializer {
        const context = raw_context orelse return null;
        const self: *OnnxInitializerSource = @ptrCast(@alignCast(context));
        const init = self.model.getInitializer(name) orelse return null;

        if (try self.openQ8_0BlockTensorRaw(allocator, init)) |q8| {
            const shape = onnx_graph.tensor.tensorShape(init.tensor) catch return error.UnsupportedShape;
            const scale_shape = ml.graph.Shape.init(.f32, q8.scale_shape);
            allocator.free(q8.shape);
            allocator.free(q8.scale_shape);
            return .{
                .name = name,
                .shape = shape,
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

        const bytes = try onnx_graph.tensor.extractNativeBytesWithExternal(
            allocator,
            init.tensor,
            self.model.base_dir,
        );
        errdefer allocator.free(bytes);
        return .{
            .name = name,
            .shape = try onnx_graph.tensor.tensorShape(init.tensor),
            .data = .{ .raw_bytes = bytes },
        };
    }

    fn freeInitializer(
        raw_context: ?*anyopaque,
        allocator: Allocator,
        init: *const onnx_graph.@"export".ParameterInitializer,
    ) void {
        _ = raw_context;
        switch (init.data) {
            .f32 => |data| allocator.free(data),
            .raw_bytes => |data| allocator.free(data),
            .streamed => |stream| onnx_graph.freeStreamedTensorData(allocator, stream),
            .q8_0_block => |data| {
                allocator.free(data.values_u8);
                allocator.free(data.scales_f32);
            },
        }
    }

    fn openTensor(
        raw_context: ?*anyopaque,
        allocator: Allocator,
        name: []const u8,
        target_dtype: tensor_mod.DType,
    ) !?export_source_mod.Stream {
        const context = raw_context orelse return null;
        const self: *OnnxInitializerSource = @ptrCast(@alignCast(context));
        const init = self.model.getInitializer(name) orelse return null;
        const dtype = try tensorDType(init.tensor.data_type);
        if (dtype != target_dtype) return error.UnsupportedTensorType;

        const bytes = try onnx_graph.tensor.extractNativeBytesWithExternal(
            allocator,
            init.tensor,
            self.model.base_dir,
        );
        errdefer allocator.free(bytes);

        const shape = try allocator.dupe(i64, init.tensor.dims);
        errdefer allocator.free(shape);

        const ctx = try allocator.create(RawStreamContext);
        errdefer allocator.destroy(ctx);
        ctx.* = .{
            .shape = shape,
            .dtype = dtype,
            .bytes = bytes,
        };

        return .{
            .shape = shape,
            .dtype = dtype,
            .storage_kind = .dense_native,
            .source_byte_len = bytes.len,
            .byte_len = bytes.len,
            .context = ctx,
            .write_all = &writeRawStream,
            .deinit = &deinitRawStream,
        };
    }

    fn openQ8_0BlockTensor(
        raw_context: ?*anyopaque,
        allocator: Allocator,
        name: []const u8,
    ) !?export_source_mod.Q8_0BlockTensor {
        const context = raw_context orelse return null;
        const self: *OnnxInitializerSource = @ptrCast(@alignCast(context));
        const init = self.model.getInitializer(name) orelse return null;
        return try self.openQ8_0BlockTensorRaw(allocator, init);
    }

    fn openQ8_0BlockTensorRaw(
        self: *OnnxInitializerSource,
        allocator: Allocator,
        init: onnx_graph.InitializerData,
    ) !?export_source_mod.Q8_0BlockTensor {
        switch (init.tensor.data_type) {
            .float32, .float16, .bfloat16 => {},
            else => return null,
        }
        if (init.tensor.dims.len < 2) return null;

        const element_count = shapeElementCount(init.tensor.dims) catch return null;
        if (element_count < self.min_elements) return null;

        const axis = init.tensor.dims.len - 1;
        const last_dim: usize = @intCast(init.tensor.dims[axis]);
        if (last_dim == 0 or last_dim % q8_block_size != 0) return null;

        const values = try self.model.loadInitializerData(allocator, init.name);
        defer allocator.free(values);
        if (values.len != element_count) return error.InvalidTensorShape;

        return try quantizeF32ToQ8_0Blocks(allocator, init.tensor.dims, values);
    }
};

fn tensorDType(dtype: onnx_graph.DataType) !tensor_mod.DType {
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

fn writeRawStream(raw_context: ?*anyopaque, allocator: Allocator, sink: export_source_mod.ByteSink) !void {
    _ = allocator;
    const context = raw_context orelse return error.InvalidState;
    const ctx: *RawStreamContext = @ptrCast(@alignCast(context));
    try sink.write(sink.context, ctx.bytes);
}

fn deinitRawStream(raw_context: ?*anyopaque, allocator: Allocator) void {
    const context = raw_context orelse return;
    const ctx: *RawStreamContext = @ptrCast(@alignCast(context));
    allocator.free(ctx.shape);
    allocator.free(ctx.bytes);
    allocator.destroy(ctx);
}

fn shapeElementCount(shape: []const i64) !usize {
    var count: usize = 1;
    for (shape) |dim| {
        if (dim <= 0) return error.UnsupportedShape;
        count = try std.math.mul(usize, count, @intCast(dim));
    }
    return count;
}

fn quantizeF32ToQ8_0Blocks(
    allocator: Allocator,
    input_shape: []const i64,
    values: []const f32,
) !export_source_mod.Q8_0BlockTensor {
    if (input_shape.len == 0) return error.UnsupportedShape;
    const axis = input_shape.len - 1;
    const last_dim: usize = @intCast(input_shape[axis]);
    if (last_dim == 0 or last_dim % q8_block_size != 0) return error.UnsupportedShape;
    if (values.len != try shapeElementCount(input_shape)) return error.InvalidTensorShape;

    const block_count = values.len / q8_block_size;
    const shape = try allocator.dupe(i64, input_shape);
    errdefer allocator.free(shape);
    const scale_shape = try allocator.dupe(i64, input_shape);
    errdefer allocator.free(scale_shape);
    scale_shape[axis] = @intCast(last_dim / q8_block_size);

    const values_u8 = try allocator.alloc(u8, values.len);
    errdefer allocator.free(values_u8);
    const scales_f32 = try allocator.alloc(f32, block_count);
    errdefer allocator.free(scales_f32);

    for (0..block_count) |block_idx| {
        const start = block_idx * q8_block_size;
        const block = values[start .. start + q8_block_size];
        var max_abs: f32 = 0.0;
        for (block) |v| {
            const abs_v = @abs(v);
            if (abs_v > max_abs) max_abs = abs_v;
        }
        const scale = if (max_abs == 0.0) 1.0 else max_abs / 127.0;
        scales_f32[block_idx] = scale;
        for (block, 0..) |v, i| {
            const rounded = @round(v / scale);
            const clamped = @min(@max(rounded, -127.0), 127.0);
            const q: i32 = @intFromFloat(clamped);
            values_u8[start + i] = @intCast(q + 128);
        }
    }

    return .{
        .shape = shape,
        .scale_shape = scale_shape,
        .values_u8 = values_u8,
        .scales_f32 = scales_f32,
        .axis = @intCast(axis),
        .block_size = q8_block_size,
        .zero_point_u8 = 128,
        .source_byte_len = values.len * @sizeOf(f32),
    };
}

test "quantizeF32ToQ8_0Blocks uses per-32-value scales and zero point" {
    var values: [32]f32 = undefined;
    for (&values, 0..) |*v, i| v.* = @as(f32, @floatFromInt(@as(i32, @intCast(i)) - 16));

    var q8 = try quantizeF32ToQ8_0Blocks(std.testing.allocator, &.{ 1, 32 }, &values);
    defer q8.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 32), q8.values_u8.len);
    try std.testing.expectEqual(@as(usize, 1), q8.scales_f32.len);
    try std.testing.expectEqual(@as(u8, 128), q8.zero_point_u8);
    try std.testing.expect(q8.scales_f32[0] > 0.0);
    for (values, 0..) |want, i| {
        const got = q8.scales_f32[0] * @as(f32, @floatFromInt(@as(i32, q8.values_u8[i]) - 128));
        try std.testing.expectApproxEqAbs(want, got, q8.scales_f32[0]);
    }
}

test "defaultVariantDir appends quantization format beside source for non-ClipClap models" {
    const out = try defaultVariantDir(std.testing.allocator, "/tmp/models/antflydb/model", "q8_0");
    defer std.testing.allocator.free(out);
    try std.testing.expectEqualStrings("/tmp/models/antflydb/model-q8_0", out);
}

test "copyFileStreaming skips same file through canonical alias" {
    const allocator = std.testing.allocator;
    const dir_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/onnx-copy-same-file-{d}", .{std.posix.system.getpid()});
    defer allocator.free(dir_path);
    defer compat.cwd().deleteTree(compat.io(), dir_path) catch {};
    try compat.cwd().createDirPath(compat.io(), dir_path);

    const source_path = try std.fs.path.join(allocator, &.{ dir_path, "payload.bin" });
    defer allocator.free(source_path);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = source_path, .data = "payload-data" });

    const alias_path = try std.fmt.allocPrint(allocator, "{s}/./payload.bin", .{dir_path});
    defer allocator.free(alias_path);
    try copyFileStreaming(allocator, compat.io(), source_path, alias_path);

    const after = try c_file.readFile(allocator, source_path);
    defer allocator.free(after);
    try std.testing.expectEqualStrings("payload-data", after);
}

test "ClipClap ONNX quantize writes suffixed variants beside defaults" {
    const allocator = std.testing.allocator;
    const dir_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/clipclap-onnx-single-repo-{d}", .{std.posix.system.getpid()});
    defer allocator.free(dir_path);
    defer compat.cwd().deleteTree(compat.io(), dir_path) catch {};
    try compat.cwd().createDirPath(compat.io(), dir_path);

    const onnx_files = [_][]const u8{
        "text_model.onnx",
        "visual_model.onnx",
        "audio_model.onnx",
        "text_projection.onnx",
        "visual_projection.onnx",
        "audio_projection.onnx",
    };
    for (onnx_files) |file_name| {
        try writeTinyOnnxModel(allocator, dir_path, file_name);
    }

    try run(allocator, compat.io(), .{
        .model_dir = dir_path,
        .target = .onnx,
        .format = "q8_0",
        .min_elements = 1,
    });

    for (onnx_files) |file_name| {
        const variant_name = try variants_manifest.variantOnnxName(allocator, file_name, "q8_0");
        defer allocator.free(variant_name);
        const variant_path = try std.fs.path.join(allocator, &.{ dir_path, variant_name });
        defer allocator.free(variant_path);
        try std.testing.expect(c_file.fileExists(allocator, variant_path));

        const data_name = try variants_manifest.variantOnnxDataName(allocator, variant_name);
        defer allocator.free(data_name);
        const data_path = try std.fs.path.join(allocator, &.{ dir_path, data_name });
        defer allocator.free(data_path);
        try std.testing.expect(c_file.fileExists(allocator, data_path));

        const bytes = try c_file.readFile(allocator, variant_path);
        defer allocator.free(bytes);
        var model = try onnx_graph.parseWithBaseDir(allocator, bytes, dir_path);
        defer model.deinit();
        const graph = model.onnx.graph orelse return error.TestExpectedGraph;
        try std.testing.expect(graph.initializers.len > 0);
        const external = graph.initializers[0].externalDataInfo();
        try std.testing.expectEqualStrings(data_name, external.location);
    }

    const variants_path = try std.fs.path.join(allocator, &.{ dir_path, "antfly_inference_variants.json" });
    defer allocator.free(variants_path);
    const variants = try c_file.readFile(allocator, variants_path);
    defer allocator.free(variants);
    try std.testing.expect(std.mem.indexOf(u8, variants, "\"id\": \"onnx-Q8_0\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, variants, "\"text_model\": \"text_model.Q8_0.onnx\"") != null);
}

test "ClipClap ONNX quantize output stages default external data" {
    const allocator = std.testing.allocator;
    const source_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/clipclap-onnx-source-{d}", .{std.posix.system.getpid()});
    defer allocator.free(source_path);
    const output_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/clipclap-onnx-output-{d}", .{std.posix.system.getpid()});
    defer allocator.free(output_path);
    defer compat.cwd().deleteTree(compat.io(), source_path) catch {};
    defer compat.cwd().deleteTree(compat.io(), output_path) catch {};
    try compat.cwd().createDirPath(compat.io(), source_path);

    const onnx_files = [_][]const u8{
        "text_model.onnx",
        "visual_model.onnx",
        "audio_model.onnx",
        "text_projection.onnx",
        "visual_projection.onnx",
        "audio_projection.onnx",
    };
    for (onnx_files) |file_name| {
        try writeTinyOnnxModel(allocator, source_path, file_name);
        const data_name = try variants_manifest.variantOnnxDataName(allocator, file_name);
        defer allocator.free(data_name);
        const data_path = try std.fs.path.join(allocator, &.{ source_path, data_name });
        defer allocator.free(data_path);
        try compat.cwd().writeFile(compat.io(), .{ .sub_path = data_path, .data = "default-data" });
    }

    try run(allocator, compat.io(), .{
        .model_dir = source_path,
        .target = .onnx,
        .format = "q8_0",
        .output_path = output_path,
        .min_elements = 1,
    });

    const copied_data_path = try std.fs.path.join(allocator, &.{ output_path, "text_model.onnx.data" });
    defer allocator.free(copied_data_path);
    const copied_data = try c_file.readFile(allocator, copied_data_path);
    defer allocator.free(copied_data);
    try std.testing.expectEqualStrings("default-data", copied_data);

    const variant_data_path = try std.fs.path.join(allocator, &.{ output_path, "text_model.Q8_0.onnx.data" });
    defer allocator.free(variant_data_path);
    try std.testing.expect(c_file.fileExists(allocator, variant_data_path));
}

fn writeTinyOnnxModel(allocator: Allocator, dir_path: []const u8, file_name: []const u8) !void {
    var dims = [_]i64{ 1, 32 };
    var values: [32]f32 = undefined;
    for (&values, 0..) |*value, i| value.* = @floatFromInt(i);
    const raw = std.mem.sliceAsBytes(&values);
    var initializers = [_]onnx_graph.TensorProto{
        .{ .name = "weight", .dims = &dims, .data_type = .float32, .raw_data = raw },
    };
    const graph_proto = onnx_graph.GraphProto{ .initializers = &initializers };
    const model_proto = onnx_graph.ModelProto{ .graph = graph_proto };
    const model_bytes = try onnx_graph.serializeModel(allocator, &model_proto);
    defer allocator.free(model_bytes);

    const path = try std.fs.path.join(allocator, &.{ dir_path, file_name });
    defer allocator.free(path);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = path, .data = model_bytes });
}
