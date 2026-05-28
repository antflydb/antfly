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

// ONNX Runtime backend.
//
// Links against libonnxruntime via the ONNX Runtime C API.
// Only compiled when -Donnx=true is passed to the build.

const std = @import("std");
const builtin = @import("builtin");
const Session = @import("session.zig").Session;
const Tensor = @import("tensor.zig").Tensor;
const TensorInfo = @import("tensor.zig").TensorInfo;
const DType = @import("tensor.zig").DType;
const BackendType = @import("backends.zig").BackendType;

const c = @cImport({
    @cInclude("onnxruntime_c_api.h");
    @cInclude("onnxruntime_session_options_config_keys.h");
});

const OrtApi = c.OrtApi;

var global_api: ?*const OrtApi = null;
var global_env: ?*c.OrtEnv = null;

fn getApi() *const OrtApi {
    if (global_api) |api| return api;
    global_api = c.OrtGetApiBase().*.GetApi.?(c.ORT_API_VERSION);
    return global_api.?;
}

fn initEnv() !void {
    if (global_env != null) return;
    const api = getApi();
    const status = api.CreateEnv.?(c.ORT_LOGGING_LEVEL_WARNING, "termite", &global_env);
    if (status) |s| {
        defer api.ReleaseStatus.?(s);
        return error.OrtEnvCreationFailed;
    }
}

/// Check an ORT status and return an error if non-null.
fn checkStatus(api: *const OrtApi, status: ?*c.OrtStatus) !void {
    if (status) |s| {
        if (api.GetErrorMessage) |get_error_message| {
            if (get_error_message(s)) |msg_ptr| {
                const msg = std.mem.span(msg_ptr);
                std.debug.print("onnxruntime: {s}\n", .{msg});
            }
        }
        defer api.ReleaseStatus.?(s);
        return error.OrtApiFailed;
    }
}

/// ORT element type to our DType.
fn ortElementTypeToDType(ort_type: c.ONNXTensorElementDataType) DType {
    return switch (ort_type) {
        c.ONNX_TENSOR_ELEMENT_DATA_TYPE_FLOAT => .f32,
        c.ONNX_TENSOR_ELEMENT_DATA_TYPE_DOUBLE => .f64,
        c.ONNX_TENSOR_ELEMENT_DATA_TYPE_INT8 => .i8,
        c.ONNX_TENSOR_ELEMENT_DATA_TYPE_INT16 => .i16,
        c.ONNX_TENSOR_ELEMENT_DATA_TYPE_INT64 => .i64,
        c.ONNX_TENSOR_ELEMENT_DATA_TYPE_INT32 => .i32,
        c.ONNX_TENSOR_ELEMENT_DATA_TYPE_FLOAT16 => .f16,
        c.ONNX_TENSOR_ELEMENT_DATA_TYPE_BFLOAT16 => .bf16,
        c.ONNX_TENSOR_ELEMENT_DATA_TYPE_UINT8 => .u8,
        c.ONNX_TENSOR_ELEMENT_DATA_TYPE_BOOL => .bool_,
        else => .f32,
    };
}

/// Our DType to ORT element type.
fn dtypeToOrtElementType(dtype: DType) c.ONNXTensorElementDataType {
    return switch (dtype) {
        .f32 => c.ONNX_TENSOR_ELEMENT_DATA_TYPE_FLOAT,
        .f64 => c.ONNX_TENSOR_ELEMENT_DATA_TYPE_DOUBLE,
        .i8 => c.ONNX_TENSOR_ELEMENT_DATA_TYPE_INT8,
        .i16 => c.ONNX_TENSOR_ELEMENT_DATA_TYPE_INT16,
        .i64 => c.ONNX_TENSOR_ELEMENT_DATA_TYPE_INT64,
        .i32 => c.ONNX_TENSOR_ELEMENT_DATA_TYPE_INT32,
        .f16 => c.ONNX_TENSOR_ELEMENT_DATA_TYPE_FLOAT16,
        .bf16 => c.ONNX_TENSOR_ELEMENT_DATA_TYPE_BFLOAT16,
        .u8 => c.ONNX_TENSOR_ELEMENT_DATA_TYPE_UINT8,
        .bool_ => c.ONNX_TENSOR_ELEMENT_DATA_TYPE_BOOL,
    };
}

pub const OnnxSession = struct {
    allocator: std.mem.Allocator,
    session: *c.OrtSession,
    input_names: std.ArrayListUnmanaged([:0]const u8),
    output_names: std.ArrayListUnmanaged([:0]const u8),
    input_info: std.ArrayListUnmanaged(TensorInfo),
    output_info: std.ArrayListUnmanaged(TensorInfo),

    pub fn deinit(self: *OnnxSession) void {
        const api = getApi();
        api.ReleaseSession.?(self.session);

        // Free ORT-allocated name strings
        const ort_allocator = getDefaultAllocator(api) catch null;
        if (ort_allocator) |alloc| {
            for (self.input_names.items) |name| {
                alloc.*.Free.?(alloc, @ptrCast(@constCast(name.ptr)));
            }
            for (self.output_names.items) |name| {
                alloc.*.Free.?(alloc, @ptrCast(@constCast(name.ptr)));
            }
        }

        self.input_names.deinit(self.allocator);
        self.output_names.deinit(self.allocator);

        // Free shape slices in TensorInfo
        for (self.input_info.items) |info| {
            self.allocator.free(info.shape);
        }
        for (self.output_info.items) |info| {
            self.allocator.free(info.shape);
        }

        self.input_info.deinit(self.allocator);
        self.output_info.deinit(self.allocator);
    }
};

pub const SessionOptions = struct {
    low_memory: bool = false,
};

pub const RetainedInput = struct {
    name: []const u8,
    value: *c.OrtValue,
};

pub const RetainedOutput = struct {
    allocator: std.mem.Allocator,
    name: []const u8,
    dtype: DType,
    shape: []i64,
    value: *c.OrtValue,

    pub fn deinit(self: *RetainedOutput) void {
        const api = getApi();
        api.ReleaseValue.?(self.value);
        self.allocator.free(self.shape);
        self.* = undefined;
    }

    pub fn seqLen(self: *const RetainedOutput) usize {
        if (self.shape.len == 4 and self.shape[2] > 0) return @intCast(self.shape[2]);
        return 0;
    }
};

pub const BoundRunResult = struct {
    allocator: std.mem.Allocator,
    tensors: []Tensor = &.{},
    retained_outputs: []RetainedOutput = &.{},

    pub fn deinit(self: *BoundRunResult) void {
        for (self.tensors) |*tensor| tensor.deinit();
        if (self.tensors.len > 0) self.allocator.free(self.tensors);
        for (self.retained_outputs) |*output| output.deinit();
        if (self.retained_outputs.len > 0) self.allocator.free(self.retained_outputs);
        self.tensors = &.{};
        self.retained_outputs = &.{};
    }
};

pub const RetainedValueCache = struct {
    allocator: std.mem.Allocator,
    values: []RetainedOutput = &.{},

    pub fn init(allocator: std.mem.Allocator) RetainedValueCache {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *RetainedValueCache) void {
        for (self.values) |*value| value.deinit();
        if (self.values.len > 0) self.allocator.free(self.values);
        self.values = &.{};
    }

    pub fn replace(self: *RetainedValueCache, outputs: []RetainedOutput) void {
        self.deinit();
        self.values = outputs;
    }

    pub fn find(self: *const RetainedValueCache, name: []const u8) ?RetainedInput {
        for (self.values) |*value| {
            if (std.mem.eql(u8, value.name, name)) {
                return .{ .name = name, .value = value.value };
            }
        }
        return null;
    }

    pub fn seqLen(self: *const RetainedValueCache) usize {
        for (self.values) |*value| {
            const len = value.seqLen();
            if (len > 0) return len;
        }
        return 0;
    }
};

fn getDefaultAllocator(api: *const OrtApi) !*c.OrtAllocator {
    var allocator: ?*c.OrtAllocator = null;
    try checkStatus(api, api.GetAllocatorWithDefaultOptions.?(&allocator));
    return allocator.?;
}

/// Introspect session inputs or outputs and populate names + info lists.
fn introspectIO(
    api: *const OrtApi,
    session: *c.OrtSession,
    allocator: std.mem.Allocator,
    ort_allocator: *c.OrtAllocator,
    names: *std.ArrayListUnmanaged([:0]const u8),
    infos: *std.ArrayListUnmanaged(TensorInfo),
    comptime is_input: bool,
) !void {
    var count: usize = 0;
    if (is_input) {
        try checkStatus(api, api.SessionGetInputCount.?(session, &count));
    } else {
        try checkStatus(api, api.SessionGetOutputCount.?(session, &count));
    }

    for (0..count) |i| {
        // Get name (ORT allocates the string, we must free it later via ort_allocator)
        var name_ptr: [*c]u8 = undefined;
        if (is_input) {
            try checkStatus(api, api.SessionGetInputName.?(session, i, ort_allocator, &name_ptr));
        } else {
            try checkStatus(api, api.SessionGetOutputName.?(session, i, ort_allocator, &name_ptr));
        }
        const name: [:0]const u8 = std.mem.span(@as([*:0]const u8, @ptrCast(name_ptr)));
        try names.append(allocator, name);

        // Get type info
        var type_info: ?*c.OrtTypeInfo = null;
        if (is_input) {
            try checkStatus(api, api.SessionGetInputTypeInfo.?(session, i, &type_info));
        } else {
            try checkStatus(api, api.SessionGetOutputTypeInfo.?(session, i, &type_info));
        }
        defer api.ReleaseTypeInfo.?(type_info.?);

        // Cast to tensor type info (non-owning pointer)
        var tensor_info: ?*const c.OrtTensorTypeAndShapeInfo = null;
        try checkStatus(api, api.CastTypeInfoToTensorInfo.?(type_info.?, &tensor_info));

        // Get element type
        var element_type: c.ONNXTensorElementDataType = c.ONNX_TENSOR_ELEMENT_DATA_TYPE_UNDEFINED;
        try checkStatus(api, api.GetTensorElementType.?(tensor_info.?, &element_type));

        // Get dimensions
        var dim_count: usize = 0;
        try checkStatus(api, api.GetDimensionsCount.?(tensor_info.?, &dim_count));

        const dims = try allocator.alloc(i64, dim_count);
        try checkStatus(api, api.GetDimensions.?(tensor_info.?, dims.ptr, dim_count));

        try infos.append(allocator, .{
            .name = name,
            .dtype = ortElementTypeToDType(element_type),
            .shape = dims,
        });
    }
}

pub fn createSession(allocator: std.mem.Allocator, model_path: []const u8) !Session {
    return createSessionWithOptions(allocator, model_path, .{});
}

pub fn createSessionWithOptions(
    allocator: std.mem.Allocator,
    model_path: []const u8,
    options: SessionOptions,
) !Session {
    try initEnv();
    const api = getApi();

    // Create session options
    var session_options: ?*c.OrtSessionOptions = null;
    {
        const status = api.CreateSessionOptions.?(&session_options);
        if (status) |s| {
            defer api.ReleaseStatus.?(s);
            return error.OrtSessionOptionsFailed;
        }
    }
    defer api.ReleaseSessionOptions.?(session_options.?);

    // Set thread count
    _ = api.SetIntraOpNumThreads.?(session_options.?, 4);
    if (options.low_memory) {
        try checkStatus(api, api.SetSessionGraphOptimizationLevel.?(session_options.?, c.ORT_DISABLE_ALL));
        try checkStatus(api, api.SetSessionExecutionMode.?(session_options.?, c.ORT_SEQUENTIAL));
        try checkStatus(api, api.DisableMemPattern.?(session_options.?));
        try checkStatus(api, api.DisableCpuMemArena.?(session_options.?));
        try checkStatus(api, api.AddSessionConfigEntry.?(
            session_options.?,
            c.kOrtSessionOptionsConfigDisablePrepacking,
            "1",
        ));
    }

    // Only probe CUDA on Linux builds where a CUDA-enabled ORT is plausible.
    // On macOS this only produces noisy provider-library failures before ORT
    // falls back to CPU anyway.
    if (builtin.os.tag == .linux) {
        var cuda_opts: c.OrtCUDAProviderOptions = std.mem.zeroes(c.OrtCUDAProviderOptions);
        cuda_opts.device_id = 0;
        const cuda_status = api.SessionOptionsAppendExecutionProvider_CUDA.?(session_options.?, &cuda_opts);
        if (cuda_status) |s| {
            defer api.ReleaseStatus.?(s);
            std.log.info("CUDA provider unavailable, falling back to CPU", .{});
        } else {
            std.log.info("CUDA execution provider enabled (device 0)", .{});
        }
    }

    // Ensure the model path is null-terminated for the C API.
    // model_path comes from Zig so may not have a sentinel.
    const path_z = try allocator.dupeZ(u8, model_path);
    defer allocator.free(path_z);

    // Create session from model file
    var ort_session: ?*c.OrtSession = null;
    try checkStatus(api, api.CreateSession.?(global_env.?, path_z.ptr, session_options.?, &ort_session));

    const impl = try allocator.create(OnnxSession);
    impl.* = .{
        .allocator = allocator,
        .session = ort_session.?,
        .input_names = std.ArrayListUnmanaged([:0]const u8).empty,
        .output_names = std.ArrayListUnmanaged([:0]const u8).empty,
        .input_info = std.ArrayListUnmanaged(TensorInfo).empty,
        .output_info = std.ArrayListUnmanaged(TensorInfo).empty,
    };

    // Introspect model inputs and outputs
    const ort_allocator = try getDefaultAllocator(api);
    try introspectIO(api, ort_session.?, allocator, ort_allocator, &impl.input_names, &impl.input_info, true);
    try introspectIO(api, ort_session.?, allocator, ort_allocator, &impl.output_names, &impl.output_info, false);

    return .{
        .ptr = impl,
        .vtable = &onnx_vtable,
    };
}

const onnx_vtable = Session.VTable{
    .run = &onnxRun,
    .inputInfo = &onnxInputInfo,
    .outputInfo = &onnxOutputInfo,
    .backend = &onnxBackend,
    .close = &onnxClose,
};

fn onnxRun(ptr: *anyopaque, inputs: []const Tensor, allocator: std.mem.Allocator) ![]Tensor {
    const self: *OnnxSession = @ptrCast(@alignCast(ptr));
    const api = getApi();

    // Create memory info for CPU tensors
    var memory_info: ?*c.OrtMemoryInfo = null;
    try checkStatus(api, api.CreateCpuMemoryInfo.?(
        c.OrtArenaAllocator,
        c.OrtMemTypeDefault,
        &memory_info,
    ));
    defer api.ReleaseMemoryInfo.?(memory_info.?);

    const num_inputs = self.input_names.items.len;
    const num_outputs = self.output_names.items.len;

    // Build input OrtValues.
    // The ORT C API's CreateTensorWithDataAsOrtValue does NOT copy data,
    // so input Tensor data must stay alive until after Run().
    const input_values = try allocator.alloc(?*c.OrtValue, num_inputs);
    defer allocator.free(input_values);
    @memset(input_values, null);

    defer {
        for (input_values) |v| {
            if (v) |val| api.ReleaseValue.?(val);
        }
    }

    // Match input tensors to session inputs by name
    for (self.input_names.items, 0..) |expected_name, i| {
        var found = false;
        for (inputs) |input| {
            if (std.mem.eql(u8, input.name, expected_name)) {
                try createInputOrtValue(api, memory_info.?, input, &input_values[i]);
                found = true;
                break;
            }
        }
        if (!found) return error.MissingInputTensor;
    }

    // Prepare input/output name arrays as [*c]const [*c]const u8 for C API
    const input_name_ptrs = try allocator.alloc([*c]const u8, num_inputs);
    defer allocator.free(input_name_ptrs);
    for (self.input_names.items, 0..) |name, i| {
        input_name_ptrs[i] = name.ptr;
    }

    const output_name_ptrs = try allocator.alloc([*c]const u8, num_outputs);
    defer allocator.free(output_name_ptrs);
    for (self.output_names.items, 0..) |name, i| {
        output_name_ptrs[i] = name.ptr;
    }

    // Allocate output value array (ORT allocates the actual tensors)
    const output_values = try allocator.alloc(?*c.OrtValue, num_outputs);
    defer allocator.free(output_values);
    @memset(output_values, null);

    defer {
        for (output_values) |v| {
            if (v) |val| api.ReleaseValue.?(val);
        }
    }

    // Run inference
    try checkStatus(api, api.Run.?(
        self.session,
        null, // run options
        input_name_ptrs.ptr,
        @ptrCast(input_values.ptr),
        num_inputs,
        output_name_ptrs.ptr,
        num_outputs,
        @ptrCast(output_values.ptr),
    ));

    // Convert output OrtValues to Zig Tensors
    const results = try allocator.alloc(Tensor, num_outputs);
    var result_count: usize = 0;
    errdefer {
        for (results[0..result_count]) |*t| {
            t.deinit();
        }
        allocator.free(results);
    }

    for (output_values, 0..) |ort_val, i| {
        const val = ort_val orelse continue;

        const output_name = if (i < self.output_names.items.len)
            self.output_names.items[i]
        else
            "";
        results[result_count] = try copyOrtValueToTensor(api, val, output_name, allocator);
        result_count += 1;
    }

    // If we got fewer than num_outputs (some were null), shrink
    if (result_count < num_outputs) {
        const shrunk = try allocator.realloc(results, result_count);
        return shrunk;
    }

    return results;
}

pub fn runWithBoundValues(
    session: Session,
    tensor_inputs: []const Tensor,
    retained_inputs: []const RetainedInput,
    retain_output_names: []const []const u8,
    allocator: std.mem.Allocator,
) !BoundRunResult {
    const self: *OnnxSession = @ptrCast(@alignCast(session.ptr));
    const api = getApi();

    var memory_info: ?*c.OrtMemoryInfo = null;
    try checkStatus(api, api.CreateCpuMemoryInfo.?(
        c.OrtArenaAllocator,
        c.OrtMemTypeDefault,
        &memory_info,
    ));
    defer api.ReleaseMemoryInfo.?(memory_info.?);

    var binding: ?*c.OrtIoBinding = null;
    try checkStatus(api, api.CreateIoBinding.?(self.session, &binding));
    defer api.ReleaseIoBinding.?(binding.?);

    const input_values = try allocator.alloc(?*c.OrtValue, tensor_inputs.len);
    defer allocator.free(input_values);
    @memset(input_values, null);
    defer {
        for (input_values) |value| {
            if (value) |v| api.ReleaseValue.?(v);
        }
    }

    for (tensor_inputs, 0..) |input, idx| {
        try createInputOrtValue(api, memory_info.?, input, &input_values[idx]);
        try checkStatus(api, api.BindInput.?(binding.?, input.name.ptr, input_values[idx].?));
    }

    for (retained_inputs) |input| {
        try checkStatus(api, api.BindInput.?(binding.?, input.name.ptr, input.value));
    }

    for (self.output_names.items) |name| {
        try checkStatus(api, api.BindOutputToDevice.?(binding.?, name.ptr, memory_info.?));
    }

    try checkStatus(api, api.RunWithBinding.?(self.session, null, binding.?));

    const ort_allocator = try getDefaultAllocator(api);
    var output_values: [*c]?*c.OrtValue = null;
    var output_count: usize = 0;
    try checkStatus(api, api.GetBoundOutputValues.?(binding.?, ort_allocator, &output_values, &output_count));
    defer if (output_values != null) {
        ort_allocator.*.Free.?(ort_allocator, @ptrCast(output_values));
    };
    defer {
        for (0..output_count) |idx| {
            if (output_values[idx]) |val| api.ReleaseValue.?(val);
        }
    }

    var tensors = std.ArrayListUnmanaged(Tensor).empty;
    errdefer {
        for (tensors.items) |*tensor| tensor.deinit();
        tensors.deinit(allocator);
    }

    var retained = std.ArrayListUnmanaged(RetainedOutput).empty;
    errdefer {
        for (retained.items) |*value| value.deinit();
        retained.deinit(allocator);
    }

    for (0..output_count) |idx| {
        const val = output_values[idx] orelse continue;
        const output_name = if (idx < self.output_names.items.len) self.output_names.items[idx] else "";
        if (nameInList(output_name, retain_output_names)) {
            try retained.append(allocator, try retainOrtValue(api, val, output_name, allocator));
            output_values[idx] = null;
        } else {
            try tensors.append(allocator, try copyOrtValueToTensor(api, val, output_name, allocator));
        }
    }

    const tensor_slice = try tensors.toOwnedSlice(allocator);
    tensors = .empty;
    errdefer {
        for (tensor_slice) |*tensor| tensor.deinit();
        allocator.free(tensor_slice);
    }
    const retained_slice = try retained.toOwnedSlice(allocator);
    retained = .empty;
    return .{
        .allocator = allocator,
        .tensors = tensor_slice,
        .retained_outputs = retained_slice,
    };
}

fn createInputOrtValue(
    api: *const OrtApi,
    memory_info: *c.OrtMemoryInfo,
    input: Tensor,
    output_value: *?*c.OrtValue,
) !void {
    const ort_type = dtypeToOrtElementType(input.dtype);
    const elem_count = input.elementCount();
    const byte_len = elem_count * input.dtype.byteSize();
    try checkStatus(api, api.CreateTensorWithDataAsOrtValue.?(
        memory_info,
        input.data.ptr,
        byte_len,
        input.shape.ptr,
        input.shape.len,
        ort_type,
        output_value,
    ));
}

fn copyOrtValueToTensor(
    api: *const OrtApi,
    value: *c.OrtValue,
    name: []const u8,
    allocator: std.mem.Allocator,
) !Tensor {
    const meta = try readOrtValueMetadata(api, value, allocator);
    errdefer allocator.free(meta.shape);

    var data_ptr: ?*anyopaque = null;
    try checkStatus(api, api.GetTensorMutableData.?(value, &data_ptr));

    const byte_len = elementCountFromShape(meta.shape) * meta.dtype.byteSize();
    const src_bytes: [*]const u8 = @ptrCast(data_ptr.?);
    const owned_bytes = try allocator.alloc(u8, byte_len);
    errdefer allocator.free(owned_bytes);
    @memcpy(owned_bytes, src_bytes[0..byte_len]);

    return .{
        .data = owned_bytes,
        .dtype = meta.dtype,
        .shape = meta.shape,
        .name = name,
        .allocator = allocator,
        .owns_data = true,
        .owns_shape = true,
    };
}

fn retainOrtValue(
    api: *const OrtApi,
    value: *c.OrtValue,
    name: []const u8,
    allocator: std.mem.Allocator,
) !RetainedOutput {
    const meta = try readOrtValueMetadata(api, value, allocator);
    errdefer allocator.free(meta.shape);
    return .{
        .allocator = allocator,
        .name = name,
        .dtype = meta.dtype,
        .shape = meta.shape,
        .value = value,
    };
}

const OrtValueMetadata = struct {
    dtype: DType,
    shape: []i64,
};

fn readOrtValueMetadata(
    api: *const OrtApi,
    value: *c.OrtValue,
    allocator: std.mem.Allocator,
) !OrtValueMetadata {
    var type_and_shape: ?*c.OrtTensorTypeAndShapeInfo = null;
    try checkStatus(api, api.GetTensorTypeAndShape.?(value, &type_and_shape));
    defer api.ReleaseTensorTypeAndShapeInfo.?(type_and_shape.?);

    var element_type: c.ONNXTensorElementDataType = c.ONNX_TENSOR_ELEMENT_DATA_TYPE_UNDEFINED;
    try checkStatus(api, api.GetTensorElementType.?(type_and_shape.?, &element_type));

    var dim_count: usize = 0;
    try checkStatus(api, api.GetDimensionsCount.?(type_and_shape.?, &dim_count));

    const shape = try allocator.alloc(i64, dim_count);
    errdefer allocator.free(shape);
    try checkStatus(api, api.GetDimensions.?(type_and_shape.?, shape.ptr, dim_count));

    return .{
        .dtype = ortElementTypeToDType(element_type),
        .shape = shape,
    };
}

fn elementCountFromShape(shape: []const i64) usize {
    var count: usize = 1;
    for (shape) |dim| {
        count *= @intCast(dim);
    }
    return count;
}

fn nameInList(name: []const u8, names: []const []const u8) bool {
    for (names) |candidate| {
        if (std.mem.eql(u8, name, candidate)) return true;
    }
    return false;
}

fn onnxInputInfo(ptr: *anyopaque) []const TensorInfo {
    const self: *OnnxSession = @ptrCast(@alignCast(ptr));
    return self.input_info.items;
}

fn onnxOutputInfo(ptr: *anyopaque) []const TensorInfo {
    const self: *OnnxSession = @ptrCast(@alignCast(ptr));
    return self.output_info.items;
}

fn onnxBackend(_: *anyopaque) BackendType {
    return .onnx;
}

fn onnxClose(ptr: *anyopaque) void {
    const self: *OnnxSession = @ptrCast(@alignCast(ptr));
    self.deinit();
    self.allocator.destroy(self);
}
