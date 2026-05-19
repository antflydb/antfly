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
const Tensor = @import("../backends/tensor.zig").Tensor;
const DType = @import("../backends/tensor.zig").DType;
const compat = @import("../io/compat.zig");
const gguf_mod = @import("../gguf/root.zig");
const safetensors = @import("safetensors.zig");
const weight_source_mod = @import("weight_source.zig");
const manifest_mod = @import("manifest.zig");
const tensor_store_mod = @import("tensor_store.zig");
const c_file = @import("../util/c_file.zig");
const onnx_graph = @import("onnx_graph");

pub const Encoding = union(enum) {
    dense: DType,
    gguf: gguf_mod.tensor_types.TensorType,
};

pub const Descriptor = struct {
    name: []const u8,
    shape: []const i64,
    encoding: Encoding,
    byte_len: usize,
    quantized: bool,
};

const OnnxSpec = struct {
    path: []const u8,
    prefix: []const u8,
};

pub const Record = struct {
    descriptor: Descriptor,
    raw_bytes: []const u8,
    allocator: ?std.mem.Allocator = null,
    owns_bytes: bool = false,
    owns_shape: bool = false,

    pub fn deinit(self: *Record) void {
        if (self.allocator) |allocator| {
            if (self.owns_bytes) allocator.free(@constCast(self.raw_bytes));
            if (self.owns_shape) allocator.free(@constCast(self.descriptor.shape));
        }
    }

    pub fn materializeDense(self: *const Record, allocator: std.mem.Allocator) !?Tensor {
        return switch (self.descriptor.encoding) {
            .dense => |value| blk: {
                const owned_bytes = try allocator.dupe(u8, self.raw_bytes);
                errdefer allocator.free(owned_bytes);
                const owned_shape = try allocator.dupe(i64, self.descriptor.shape);
                break :blk .{
                    .data = owned_bytes,
                    .dtype = value,
                    .shape = owned_shape,
                    .name = self.descriptor.name,
                    .allocator = allocator,
                    .owns_data = true,
                    .owns_shape = true,
                };
            },
            .gguf => |tensor_type| try gguf_mod.quant_codec.materializeDense(
                allocator,
                self.descriptor.name,
                tensor_type,
                self.descriptor.shape,
                self.raw_bytes,
            ),
        };
    }
};

pub const TensorAccess = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        getRecord: *const fn (*anyopaque, allocator: std.mem.Allocator, name: []const u8) anyerror!Record,
        listNames: *const fn (*anyopaque, allocator: std.mem.Allocator) anyerror![][]const u8,
        deinit: *const fn (*anyopaque) void,
    };

    pub fn getRecord(self: TensorAccess, allocator: std.mem.Allocator, name: []const u8) !Record {
        return self.vtable.getRecord(self.ptr, allocator, name);
    }

    pub fn listNames(self: TensorAccess, allocator: std.mem.Allocator) ![][]const u8 {
        return self.vtable.listNames(self.ptr, allocator);
    }

    pub fn deinit(self: TensorAccess) void {
        self.vtable.deinit(self.ptr);
    }
};

pub const SafetensorsAccess = struct {
    allocator: std.mem.Allocator,
    source: *weight_source_mod.SafetensorsSource,

    const vtable = TensorAccess.VTable{
        .getRecord = @ptrCast(&getRecordImpl),
        .listNames = @ptrCast(&listNamesImpl),
        .deinit = @ptrCast(&deinitSelf),
    };

    pub fn initAbsolute(allocator: std.mem.Allocator, path: []const u8) !*SafetensorsAccess {
        const self = try allocator.create(SafetensorsAccess);
        errdefer allocator.destroy(self);
        self.* = .{
            .allocator = allocator,
            .source = try weight_source_mod.SafetensorsSource.initAbsolute(allocator, path),
        };
        return self;
    }

    pub fn tensorAccess(self: *SafetensorsAccess) TensorAccess {
        return .{ .ptr = self, .vtable = &vtable };
    }

    fn getRecordImpl(self: *SafetensorsAccess, allocator: std.mem.Allocator, name: []const u8) !Record {
        _ = allocator;
        const meta = self.source.reader.header.tensors.get(name) orelse return error.TensorNotFound;
        const abs_start = self.source.reader.data_offset + meta.data_start;
        const abs_end = self.source.reader.data_offset + meta.data_end;
        if (abs_end > self.source.reader.file_bytes.len) return error.DataOutOfBounds;

        return .{
            .descriptor = .{
                .name = name,
                .shape = meta.shape,
                .encoding = .{ .dense = meta.dtype },
                .byte_len = @intCast(meta.data_end - meta.data_start),
                .quantized = false,
            },
            .raw_bytes = self.source.reader.file_bytes[@intCast(abs_start)..@intCast(abs_end)],
        };
    }

    fn listNamesImpl(self: *SafetensorsAccess, allocator: std.mem.Allocator) ![][]const u8 {
        return self.source.reader.header.tensorNames(allocator);
    }

    fn deinitSelf(self: *SafetensorsAccess) void {
        self.source.weightSource().deinit();
        self.allocator.destroy(self);
    }
};

pub const ShardedSafetensorsAccess = struct {
    allocator: std.mem.Allocator,
    source: *weight_source_mod.ShardedSafetensorsSource,

    const vtable = TensorAccess.VTable{
        .getRecord = @ptrCast(&getRecordImpl),
        .listNames = @ptrCast(&listNamesImpl),
        .deinit = @ptrCast(&deinitSelf),
    };

    pub fn initAbsolute(allocator: std.mem.Allocator, index_path: []const u8) !*ShardedSafetensorsAccess {
        const self = try allocator.create(ShardedSafetensorsAccess);
        errdefer allocator.destroy(self);
        self.* = .{
            .allocator = allocator,
            .source = try weight_source_mod.ShardedSafetensorsSource.initAbsolute(allocator, index_path),
        };
        return self;
    }

    pub fn tensorAccess(self: *ShardedSafetensorsAccess) TensorAccess {
        return .{ .ptr = self, .vtable = &vtable };
    }

    fn getRecordImpl(self: *ShardedSafetensorsAccess, allocator: std.mem.Allocator, name: []const u8) !Record {
        _ = allocator;
        const resolved = try self.source.findTensorMeta(name);
        const abs_start = resolved.reader.data_offset + resolved.meta.data_start;
        const abs_end = resolved.reader.data_offset + resolved.meta.data_end;
        if (abs_end > resolved.reader.file_bytes.len) return error.DataOutOfBounds;

        return .{
            .descriptor = .{
                .name = name,
                .shape = resolved.meta.shape,
                .encoding = .{ .dense = resolved.meta.dtype },
                .byte_len = @intCast(resolved.meta.data_end - resolved.meta.data_start),
                .quantized = false,
            },
            .raw_bytes = resolved.reader.file_bytes[@intCast(abs_start)..@intCast(abs_end)],
        };
    }

    fn listNamesImpl(self: *ShardedSafetensorsAccess, allocator: std.mem.Allocator) ![][]const u8 {
        return self.source.weightSource().listNames(allocator);
    }

    fn deinitSelf(self: *ShardedSafetensorsAccess) void {
        self.source.weightSource().deinit();
        self.allocator.destroy(self);
    }
};

pub const GgufAccess = struct {
    allocator: std.mem.Allocator,
    store: *tensor_store_mod.GgufStore,

    const vtable = TensorAccess.VTable{
        .getRecord = @ptrCast(&getRecordImpl),
        .listNames = @ptrCast(&listNamesImpl),
        .deinit = @ptrCast(&deinitSelf),
    };

    pub fn initAbsolute(allocator: std.mem.Allocator, path: []const u8) !*GgufAccess {
        const self = try allocator.create(GgufAccess);
        errdefer allocator.destroy(self);
        self.* = .{
            .allocator = allocator,
            .store = try tensor_store_mod.GgufStore.initAbsolute(allocator, path),
        };
        return self;
    }

    pub fn tensorAccess(self: *GgufAccess) TensorAccess {
        return .{ .ptr = self, .vtable = &vtable };
    }

    fn getRecordImpl(self: *GgufAccess, allocator: std.mem.Allocator, name: []const u8) !Record {
        const tensor = gguf_mod.tensor_catalog.Catalog.init(&self.store.parsed).find(name) orelse return error.TensorNotFound;
        const byte_len_u64 = gguf_mod.tensor_types.byteLen(tensor.tensor_type, tensor.dimensions) orelse return error.UnsupportedTensorType;
        const byte_len: usize = @intCast(byte_len_u64);
        const path = self.store.path orelse return error.MissingTensorStorePath;
        const raw_bytes = try c_file.readRegion(allocator, path, tensor.data_offset, byte_len);
        errdefer allocator.free(raw_bytes);

        const shape = try allocator.alloc(i64, tensor.dimensions.len);
        errdefer allocator.free(shape);
        for (0..tensor.dimensions.len) |i| shape[i] = @intCast(tensor.dimensions[tensor.dimensions.len - 1 - i]);

        return .{
            .descriptor = .{
                .name = tensor.name,
                .shape = shape,
                .encoding = .{ .gguf = tensor.tensor_type },
                .byte_len = byte_len,
                .quantized = tensor.tensor_type.isQuantized(),
            },
            .raw_bytes = @constCast(raw_bytes),
            .allocator = allocator,
            .owns_bytes = true,
            .owns_shape = true,
        };
    }

    fn listNamesImpl(self: *GgufAccess, allocator: std.mem.Allocator) ![][]const u8 {
        const names = try allocator.alloc([]const u8, self.store.parsed.tensors.len);
        for (self.store.parsed.tensors, 0..) |tensor, i| names[i] = tensor.name;
        return names;
    }

    fn deinitSelf(self: *GgufAccess) void {
        self.store.tensorStore().deinit();
        self.allocator.destroy(self);
    }
};

pub const OnnxInitializerAccess = struct {
    allocator: std.mem.Allocator,
    files: []OnnxFile,
    entries: []Entry,

    const OnnxFile = struct {
        path: []const u8,
        base_dir: []const u8,
        prefix: []const u8,
        bytes: []u8,
        model: onnx_graph.Model,

        fn deinit(self: *OnnxFile, allocator: std.mem.Allocator) void {
            self.model.deinit();
            allocator.free(self.bytes);
            allocator.free(self.prefix);
            allocator.free(self.base_dir);
            allocator.free(self.path);
        }
    };

    const Entry = struct {
        name: []const u8,
        file_index: usize,
        initializer_index: usize,

        fn deinit(self: *Entry, allocator: std.mem.Allocator) void {
            allocator.free(self.name);
        }
    };

    const vtable = TensorAccess.VTable{
        .getRecord = @ptrCast(&getRecordImpl),
        .listNames = @ptrCast(&listNamesImpl),
        .deinit = @ptrCast(&deinitSelf),
    };

    pub fn initFromManifest(allocator: std.mem.Allocator, manifest: manifest_mod.ModelManifest) !*OnnxInitializerAccess {
        var specs = std.ArrayListUnmanaged(OnnxSpec).empty;
        defer specs.deinit(allocator);

        try appendUniqueOnnxSpec(allocator, &specs, manifest.onnx_path);
        try appendUniqueOnnxSpecWithPrefix(allocator, &specs, manifest.visual_model_path, "vision_model.");
        try appendUniqueOnnxSpecWithPrefix(allocator, &specs, manifest.audio_model_path, "audio_model.");
        try appendUniqueOnnxSpecWithPrefix(allocator, &specs, manifest.text_projection_path, "text_projection.");
        try appendUniqueOnnxSpecWithPrefix(allocator, &specs, manifest.visual_projection_path, "visual_projection.");
        try appendUniqueOnnxSpecWithPrefix(allocator, &specs, manifest.audio_projection_path, "audio_projection.");
        if (specs.items.len == 0) return error.NoTensorStoreFound;

        const self = try allocator.create(OnnxInitializerAccess);
        errdefer allocator.destroy(self);
        self.* = .{
            .allocator = allocator,
            .files = try allocator.alloc(OnnxFile, specs.items.len),
            .entries = &.{},
        };
        var initialized_files: usize = 0;
        errdefer {
            for (self.files[0..initialized_files]) |*file| file.deinit(allocator);
            allocator.free(self.files);
        }

        var entries = std.ArrayListUnmanaged(Entry).empty;
        errdefer {
            for (entries.items) |*entry| entry.deinit(allocator);
            entries.deinit(allocator);
        }

        for (specs.items, 0..) |spec, file_index| {
            const path = try allocator.dupe(u8, spec.path);
            errdefer allocator.free(path);
            const base_dir = try allocator.dupe(u8, std.fs.path.dirname(spec.path) orelse ".");
            errdefer allocator.free(base_dir);
            const prefix = try allocator.dupe(u8, spec.prefix);
            errdefer allocator.free(prefix);
            const bytes = try c_file.readFileMax(allocator, spec.path, std.math.maxInt(usize));
            errdefer allocator.free(bytes);
            const model = try onnx_graph.parseWithBaseDir(allocator, bytes, base_dir);
            errdefer {
                var owned = model;
                owned.deinit();
            }

            self.files[file_index] = .{
                .path = path,
                .base_dir = base_dir,
                .prefix = prefix,
                .bytes = bytes,
                .model = model,
            };
            initialized_files += 1;

            const graph = self.files[file_index].model.onnx.graph orelse continue;
            for (graph.initializers, 0..) |initializer, initializer_index| {
                const local_name = try inferInitializerExportName(allocator, &graph, initializer.name) orelse try allocator.dupe(u8, initializer.name);
                defer allocator.free(local_name);
                const entry_name = if (prefix.len == 0)
                    try allocator.dupe(u8, local_name)
                else
                    try std.fmt.allocPrint(allocator, "{s}{s}", .{ prefix, local_name });
                errdefer allocator.free(entry_name);
                try entries.append(allocator, .{
                    .name = entry_name,
                    .file_index = file_index,
                    .initializer_index = initializer_index,
                });
            }
        }

        self.entries = try entries.toOwnedSlice(allocator);
        return self;
    }

    pub fn tensorAccess(self: *OnnxInitializerAccess) TensorAccess {
        return .{ .ptr = self, .vtable = &vtable };
    }

    fn getRecordImpl(self: *OnnxInitializerAccess, allocator: std.mem.Allocator, name: []const u8) !Record {
        const entry = self.findEntry(name) orelse return error.TensorNotFound;
        const file = &self.files[entry.file_index];
        const graph = file.model.onnx.graph orelse return error.TensorNotFound;
        const initializer = &graph.initializers[entry.initializer_index];
        const raw = try onnx_graph.tensor.extractNativeBytesWithExternal(allocator, initializer, file.model.base_dir);
        errdefer allocator.free(raw);
        const shape = try allocator.dupe(i64, initializer.dims);
        errdefer allocator.free(shape);

        return .{
            .descriptor = .{
                .name = entry.name,
                .shape = shape,
                .encoding = .{ .dense = try onnxDType(initializer.data_type) },
                .byte_len = raw.len,
                .quantized = false,
            },
            .raw_bytes = raw,
            .allocator = allocator,
            .owns_bytes = true,
            .owns_shape = true,
        };
    }

    fn listNamesImpl(self: *OnnxInitializerAccess, allocator: std.mem.Allocator) ![][]const u8 {
        const names = try allocator.alloc([]const u8, self.entries.len);
        for (self.entries, 0..) |entry, i| names[i] = entry.name;
        return names;
    }

    fn deinitSelf(self: *OnnxInitializerAccess) void {
        for (self.entries) |*entry| entry.deinit(self.allocator);
        self.allocator.free(self.entries);
        for (self.files) |*file| file.deinit(self.allocator);
        self.allocator.free(self.files);
        self.allocator.destroy(self);
    }

    fn findEntry(self: *const OnnxInitializerAccess, name: []const u8) ?Entry {
        for (self.entries) |entry| {
            if (std.mem.eql(u8, entry.name, name)) return entry;
        }
        return null;
    }
};

fn inferInitializerExportName(
    allocator: std.mem.Allocator,
    graph: *const onnx_graph.proto.GraphProto,
    initializer_name: []const u8,
) !?[]const u8 {
    if (try inferPyTorchParameterExportName(allocator, initializer_name)) |name| return name;
    if (try inferDownsampleReductionExportName(allocator, graph, initializer_name)) |name| return name;
    const matmul_output = findMatMulOutputForInitializer(graph, initializer_name) orelse return null;
    const bias_name = findAddBiasForMatMulOutput(graph, matmul_output) orelse return null;
    if (!std.mem.endsWith(u8, bias_name, ".bias")) return null;
    return try std.fmt.allocPrint(allocator, "{s}.weight", .{bias_name[0 .. bias_name.len - ".bias".len]});
}

fn inferPyTorchParameterExportName(
    allocator: std.mem.Allocator,
    initializer_name: []const u8,
) !?[]const u8 {
    const prefix = "p_audio_encoder_layers_";
    if (!std.mem.startsWith(u8, initializer_name, prefix)) return null;

    const suffix = initializer_name[prefix.len..];
    var parts = std.mem.splitScalar(u8, suffix, '_');
    const stage = parts.next() orelse return null;
    if (stage.len == 0) return null;

    var out: std.Io.Writer.Allocating = .init(allocator);
    errdefer out.deinit();
    const writer = &out.writer;
    try writer.print("audio_encoder.layers.{s}", .{stage});
    while (parts.next()) |part| {
        if (part.len == 0) continue;
        try writer.writeByte('.');
        try writer.writeAll(part);
    }
    const result = try out.toOwnedSlice();
    return result;
}

fn inferDownsampleReductionExportName(
    allocator: std.mem.Allocator,
    graph: *const onnx_graph.proto.GraphProto,
    initializer_name: []const u8,
) !?[]const u8 {
    const matmul_output = findMatMulOutputForInitializer(graph, initializer_name) orelse return null;
    const next_stage = findLayerNormBeforeStageForInput(graph, matmul_output) orelse return null;
    if (next_stage == 0) return null;
    return try std.fmt.allocPrint(allocator, "audio_encoder.layers.{d}.downsample.reduction.weight", .{next_stage - 1});
}

fn findLayerNormBeforeStageForInput(graph: *const onnx_graph.proto.GraphProto, input_name: []const u8) ?usize {
    for (graph.nodes) |node| {
        if (!std.mem.eql(u8, node.op_type, "LayerNormalization")) continue;
        if (node.inputs.len < 2) continue;
        if (!std.mem.eql(u8, node.inputs[0], input_name)) continue;
        return parseAudioEncoderLayerNormBeforeStage(node.inputs[1]);
    }
    return null;
}

fn parseAudioEncoderLayerNormBeforeStage(name: []const u8) ?usize {
    const prefix = "audio_encoder.layers.";
    const suffix = ".blocks.0.layernorm_before.weight";
    if (!std.mem.startsWith(u8, name, prefix) or !std.mem.endsWith(u8, name, suffix)) return null;
    const stage_text = name[prefix.len .. name.len - suffix.len];
    return std.fmt.parseUnsigned(usize, stage_text, 10) catch null;
}

test "infers PyTorch audio encoder parameter names" {
    const allocator = std.testing.allocator;
    const name = try inferPyTorchParameterExportName(allocator, "p_audio_encoder_layers_0_downsample_reduction_weight") orelse return error.TestUnexpectedResult;
    defer allocator.free(name);
    try std.testing.expectEqualStrings("audio_encoder.layers.0.downsample.reduction.weight", name);
}

test "infers no-bias CLAP downsample reduction MatMul names" {
    const allocator = std.testing.allocator;
    var matmul_inputs = [_][]const u8{ "layer_norm_5", "val_984" };
    var matmul_outputs = [_][]const u8{"linear_12"};
    var norm_inputs = [_][]const u8{
        "linear_12",
        "audio_encoder.layers.1.blocks.0.layernorm_before.weight",
        "audio_encoder.layers.1.blocks.0.layernorm_before.bias",
    };
    var norm_outputs = [_][]const u8{"layer_norm_6"};
    var nodes = [_]onnx_graph.proto.NodeProto{
        .{ .inputs = &matmul_inputs, .outputs = &matmul_outputs, .op_type = "MatMul" },
        .{ .inputs = &norm_inputs, .outputs = &norm_outputs, .op_type = "LayerNormalization" },
    };
    const graph = onnx_graph.proto.GraphProto{ .nodes = &nodes };
    const name = try inferDownsampleReductionExportName(allocator, &graph, "val_984") orelse return error.TestUnexpectedResult;
    defer allocator.free(name);
    try std.testing.expectEqualStrings("audio_encoder.layers.0.downsample.reduction.weight", name);
}

fn findMatMulOutputForInitializer(graph: *const onnx_graph.proto.GraphProto, initializer_name: []const u8) ?[]const u8 {
    for (graph.nodes) |node| {
        if (!std.mem.eql(u8, node.op_type, "MatMul")) continue;
        if (node.outputs.len == 0) continue;
        for (node.inputs) |input| {
            if (std.mem.eql(u8, input, initializer_name)) return node.outputs[0];
        }
    }
    return null;
}

fn findAddBiasForMatMulOutput(graph: *const onnx_graph.proto.GraphProto, matmul_output: []const u8) ?[]const u8 {
    for (graph.nodes) |node| {
        if (!std.mem.eql(u8, node.op_type, "Add")) continue;
        if (node.inputs.len < 2) continue;
        if (std.mem.eql(u8, node.inputs[0], matmul_output) and std.mem.endsWith(u8, node.inputs[1], ".bias")) return node.inputs[1];
        if (std.mem.eql(u8, node.inputs[1], matmul_output) and std.mem.endsWith(u8, node.inputs[0], ".bias")) return node.inputs[0];
    }
    return null;
}

pub fn openFromManifest(allocator: std.mem.Allocator, manifest: manifest_mod.ModelManifest) !TensorAccess {
    if (manifest.safetensors_path) |path| {
        const access = try SafetensorsAccess.initAbsolute(allocator, path);
        return access.tensorAccess();
    }
    if (manifest.safetensors_index_path) |path| {
        const access = try ShardedSafetensorsAccess.initAbsolute(allocator, path);
        return access.tensorAccess();
    }
    if (manifest.gguf_path) |path| {
        const access = try GgufAccess.initAbsolute(allocator, path);
        return access.tensorAccess();
    }
    if (manifest.onnx_path != null or
        manifest.visual_model_path != null or
        manifest.audio_model_path != null or
        manifest.text_projection_path != null or
        manifest.visual_projection_path != null or
        manifest.audio_projection_path != null)
    {
        const access = try OnnxInitializerAccess.initFromManifest(allocator, manifest);
        return access.tensorAccess();
    }
    return error.NoTensorStoreFound;
}

fn appendUniqueOnnxSpec(
    allocator: std.mem.Allocator,
    specs: *std.ArrayListUnmanaged(OnnxSpec),
    maybe_path: ?[]const u8,
) !void {
    const path = maybe_path orelse return;
    try appendUniqueOnnxSpecWithPrefix(allocator, specs, path, defaultOnnxPrefix(path));
}

fn appendUniqueOnnxSpecWithPrefix(
    allocator: std.mem.Allocator,
    specs: *std.ArrayListUnmanaged(OnnxSpec),
    maybe_path: ?[]const u8,
    prefix: []const u8,
) !void {
    const path = maybe_path orelse return;
    for (specs.items) |spec| {
        if (std.mem.eql(u8, spec.path, path)) return;
    }
    try specs.append(allocator, .{ .path = path, .prefix = prefix });
}

fn defaultOnnxPrefix(path: []const u8) []const u8 {
    const base = std.fs.path.basename(path);
    if (std.mem.startsWith(u8, base, "text_model")) return "text_model.";
    if (std.mem.startsWith(u8, base, "visual_model") or std.mem.startsWith(u8, base, "vision_model")) return "vision_model.";
    if (std.mem.startsWith(u8, base, "audio_model") or std.mem.startsWith(u8, base, "audio_encoder")) return "audio_model.";
    if (std.mem.startsWith(u8, base, "text_projection")) return "text_projection.";
    if (std.mem.startsWith(u8, base, "visual_projection")) return "visual_projection.";
    if (std.mem.startsWith(u8, base, "audio_projection")) return "audio_projection.";
    return "";
}

fn onnxDType(dtype: onnx_graph.DataType) !DType {
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

test "safetensors access reads descriptor and dense bytes" {
    const allocator = std.testing.allocator;

    const json_str =
        \\{"weights": {"dtype": "F32", "shape": [4], "data_offsets": [0, 16]}}
    ;

    var file = std.ArrayListUnmanaged(u8).empty;
    defer file.deinit(allocator);
    try appendLe(u64, allocator, &file, json_str.len);
    try file.appendSlice(allocator, json_str);
    try file.appendSlice(allocator, std.mem.asBytes(&[_]f32{ 1.0, 2.0, 3.0, 4.0 }));

    const dir_path = try testScratchDir(allocator, "tensor-access-safetensors");
    defer {
        compat.cwd().deleteTree(compat.io(), dir_path) catch {};
        allocator.free(dir_path);
    }
    const path = try std.fs.path.join(allocator, &.{ dir_path, "model.safetensors" });
    defer allocator.free(path);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = path, .data = file.items });

    var manifest = manifest_mod.ModelManifest{ .allocator = allocator, .safetensors_path = try allocator.dupe(u8, path) };
    defer manifest.deinit();

    const access = try openFromManifest(allocator, manifest);
    defer access.deinit();

    var record = try access.getRecord(allocator, "weights");
    defer record.deinit();
    try std.testing.expectEqual(@as(usize, 16), record.descriptor.byte_len);
    try std.testing.expect(!record.descriptor.quantized);

    var tensor = (try record.materializeDense(allocator)).?;
    defer tensor.deinit();
    try std.testing.expectEqual(DType.f32, tensor.dtype);
    try std.testing.expectApproxEqAbs(@as(f32, 4.0), tensor.asFloat32()[3], 1e-6);
}

test "gguf access reads descriptor and raw tensor bytes" {
    const allocator = std.testing.allocator;

    var data = std.ArrayListUnmanaged(u8).empty;
    defer data.deinit(allocator);

    try data.appendSlice(allocator, "GGUF");
    try appendLe(u32, allocator, &data, 3);
    try appendLe(u64, allocator, &data, 1);
    try appendLe(u64, allocator, &data, 0);
    try appendString(allocator, &data, "tok_embeddings.weight");
    try appendLe(u32, allocator, &data, 1);
    try appendLe(u64, allocator, &data, 4);
    try appendLe(u32, allocator, &data, @intFromEnum(gguf_mod.tensor_types.KnownTensorType.F16));
    try appendLe(u64, allocator, &data, 0);
    try padToAlignment(allocator, &data, gguf_mod.format.default_alignment);
    try data.appendSlice(allocator, &[_]u8{ 0x00, 0x3C, 0x00, 0x40, 0x00, 0x42, 0x00, 0x44 });

    const dir_path = try testScratchDir(allocator, "tensor-access-gguf");
    defer {
        compat.cwd().deleteTree(compat.io(), dir_path) catch {};
        allocator.free(dir_path);
    }
    const path = try std.fs.path.join(allocator, &.{ dir_path, "model.gguf" });
    defer allocator.free(path);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = path, .data = data.items });

    var manifest = manifest_mod.ModelManifest{ .allocator = allocator, .gguf_path = try allocator.dupe(u8, path) };
    defer manifest.deinit();

    const access = try openFromManifest(allocator, manifest);
    defer access.deinit();

    var record = try access.getRecord(allocator, "tok_embeddings.weight");
    defer record.deinit();
    try std.testing.expectEqual(@as(usize, 8), record.descriptor.byte_len);
    try std.testing.expect(!record.descriptor.quantized);

    var tensor = (try record.materializeDense(allocator)).?;
    defer tensor.deinit();
    try std.testing.expectEqual(DType.f16, tensor.dtype);
    try std.testing.expectEqual(@as(usize, 4), tensor.elementCount());
}

test "onnx initializer access reads prefixed dense bytes" {
    const allocator = std.testing.allocator;

    const dir_path = try testScratchDir(allocator, "tensor-access-onnx");
    defer {
        compat.cwd().deleteTree(compat.io(), dir_path) catch {};
        allocator.free(dir_path);
    }

    var dims = [_]i64{ 2, 2 };
    const values = [_]f32{ 1.0, 2.0, 3.0, 4.0 };
    const raw = std.mem.sliceAsBytes(&values);
    var initializers = [_]onnx_graph.TensorProto{
        .{ .name = "embeddings.weight", .dims = &dims, .data_type = .float32, .raw_data = raw },
    };
    const graph_proto = onnx_graph.GraphProto{ .initializers = &initializers };
    const model_proto = onnx_graph.ModelProto{ .graph = graph_proto };
    const model_bytes = try onnx_graph.serializeModel(allocator, &model_proto);
    defer allocator.free(model_bytes);

    const onnx_path = try std.fs.path.join(allocator, &.{ dir_path, "text_model.onnx" });
    defer allocator.free(onnx_path);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = onnx_path, .data = model_bytes });

    var manifest = manifest_mod.ModelManifest{
        .allocator = allocator,
        .onnx_path = try allocator.dupe(u8, onnx_path),
    };
    defer manifest.deinit();

    const access = try openFromManifest(allocator, manifest);
    defer access.deinit();

    const names = try access.listNames(allocator);
    defer allocator.free(names);
    try std.testing.expectEqual(@as(usize, 1), names.len);
    try std.testing.expectEqualStrings("text_model.embeddings.weight", names[0]);

    var record = try access.getRecord(allocator, "text_model.embeddings.weight");
    defer record.deinit();
    try std.testing.expectEqual(DType.f32, switch (record.descriptor.encoding) {
        .dense => |dtype| dtype,
        .gguf => return error.UnexpectedQuantizedEncoding,
    });
    try std.testing.expectEqual(@as(i64, 2), record.descriptor.shape[0]);
    try std.testing.expectEqual(@as(i64, 2), record.descriptor.shape[1]);
    try std.testing.expectEqualSlices(u8, raw, record.raw_bytes);
}

fn appendLe(comptime T: type, allocator: std.mem.Allocator, data: *std.ArrayListUnmanaged(u8), value: T) !void {
    var buf: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &buf, value, .little);
    try data.appendSlice(allocator, &buf);
}

fn appendString(allocator: std.mem.Allocator, data: *std.ArrayListUnmanaged(u8), value: []const u8) !void {
    try appendLe(u64, allocator, data, value.len);
    try data.appendSlice(allocator, value);
}

fn testScratchDir(allocator: std.mem.Allocator, name: []const u8) ![]u8 {
    const root = try std.fmt.allocPrint(allocator, "termite-model-tests-{d}", .{std.posix.system.getpid()});
    defer allocator.free(root);
    const dir_path = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", root, name });
    errdefer allocator.free(dir_path);
    compat.cwd().deleteTree(compat.io(), dir_path) catch {};
    try compat.cwd().createDirPath(compat.io(), dir_path);
    return dir_path;
}

fn padToAlignment(allocator: std.mem.Allocator, data: *std.ArrayListUnmanaged(u8), alignment: u64) !void {
    const rem = data.items.len % alignment;
    if (rem == 0) return;
    try data.appendNTimes(allocator, 0, alignment - rem);
}
