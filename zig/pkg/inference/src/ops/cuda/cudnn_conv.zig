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

//! Bounded NCHW cuDNN convolutions with explicit storage, accumulation and bias contracts.
const std = @import("std");
const backend = @import("cudnn_backend.zig");
const Desc = ?*anyopaque;
const Status = c_int;
const Perf = extern struct {
    algo: c_int,
    status: Status,
    time: f32,
    memory: usize,
    determinism: c_int,
    math_type: c_int,
    reserved: [3]c_int,
};
const Table = struct {
    cudnnCreate: *const fn (*Desc) callconv(.c) Status,
    cudnnDestroy: *const fn (Desc) callconv(.c) Status,
    cudnnSetStream: *const fn (Desc, ?*anyopaque) callconv(.c) Status,
    cudnnCreateTensorDescriptor: *const fn (*Desc) callconv(.c) Status,
    cudnnDestroyTensorDescriptor: *const fn (Desc) callconv(.c) Status,
    cudnnSetTensor4dDescriptor: *const fn (Desc, c_int, c_int, c_int, c_int, c_int, c_int) callconv(.c) Status,
    cudnnCreateFilterDescriptor: *const fn (*Desc) callconv(.c) Status,
    cudnnDestroyFilterDescriptor: *const fn (Desc) callconv(.c) Status,
    cudnnSetFilter4dDescriptor: *const fn (Desc, c_int, c_int, c_int, c_int, c_int, c_int) callconv(.c) Status,
    cudnnCreateConvolutionDescriptor: *const fn (*Desc) callconv(.c) Status,
    cudnnDestroyConvolutionDescriptor: *const fn (Desc) callconv(.c) Status,
    cudnnSetConvolution2dDescriptor: *const fn (Desc, c_int, c_int, c_int, c_int, c_int, c_int, c_int, c_int) callconv(.c) Status,
    cudnnSetConvolutionMathType: *const fn (Desc, c_int) callconv(.c) Status,
    cudnnGetConvolutionForwardAlgorithm_v7: *const fn (Desc, Desc, Desc, Desc, Desc, c_int, *c_int, [*]Perf) callconv(.c) Status,
    cudnnConvolutionForward: *const fn (Desc, *const f32, Desc, ?*const anyopaque, Desc, ?*const anyopaque, Desc, c_int, ?*anyopaque, usize, *const f32, Desc, ?*anyopaque) callconv(.c) Status,
    cudnnAddTensor: *const fn (Desc, *const f32, Desc, ?*const anyopaque, *const f32, Desc, ?*anyopaque) callconv(.c) Status,
};

pub const Storage = enum { f16, bf16 };
pub const Layout = enum { nchw };
pub const Accumulation = enum { f32 };
pub const Bias = enum { add, none };
pub const Route = enum { legacy, backend_graph };
pub const Heuristic = enum(c_int) { instant = 0, mode_b = 1 };

pub const Key = struct {
    route: Route = .legacy,
    /// Non-benchmark eager source policy: instantaneous heuristics, no forced
    /// engine index. A deterministic caller may filter nondeterministic plans.
    heuristic: Heuristic = .instant,
    deterministic: bool = false,
    alignment: usize = 32,
    storage: Storage = .f16,
    layout: Layout = .nchw,
    accumulation: Accumulation = .f32,
    bias: Bias = .add,
    batch: usize,
    inputs: usize,
    outputs: usize,
    height: usize,
    width: usize,
    kernel: usize,
    stride: usize,
    padding: usize,

    pub fn eligible(k: Key) bool {
        if (k.route == .backend_graph and (k.storage != .bf16 or !std.math.isPowerOfTwo(k.alignment) or k.alignment < 2 or k.alignment > 32)) return false;
        if (k.batch == 0 or k.batch > 64 or k.height == 0 or k.width == 0 or k.height > 4096 or k.width > 4096) return false;
        if (k.inputs == 0 or k.outputs == 0 or k.inputs > 65536 or k.outputs > 65536) return false;
        if (k.storage == .bf16) return k.kernel == 3 and k.stride == 2 and k.padding == 1;
        return (k.inputs == 3 and k.outputs == 128 and k.kernel == 7 and k.stride == 4 and k.padding == 3) or
            ((k.inputs == 128 or k.inputs == 256 or k.inputs == 512) and k.outputs == k.inputs * 2 and k.kernel == 3 and k.stride == 2 and k.padding == 1);
    }
    pub fn outHeight(k: Key) usize {
        return (k.height + 2 * k.padding - k.kernel) / k.stride + 1;
    }
    pub fn outWidth(k: Key) usize {
        return (k.width + 2 * k.padding - k.kernel) / k.stride + 1;
    }
};

const GraphPlan = struct {
    x: Desc = null,
    weight: Desc = null,
    y: Desc = null,
    convolution: Desc = null,
    operation: Desc = null,
    graph: Desc = null,
    config: Desc = null,
    execution: Desc = null,
    variant: Desc = null,
    bindings: ?Bindings = null,
    workspace_bytes: usize = 0,

    fn deinit(p: *GraphPlan, api: *const backend.Api) void {
        // Keep dependency descriptors and the loaded SDK alive through variant
        // and execution-plan retirement, including failed cold construction.
        inline for (.{ "variant", "execution", "config", "graph", "operation", "convolution", "y", "weight", "x" }) |name| api.destroy(&@field(p, name));
        p.* = .{};
    }
    fn matches(p: *const GraphPlan, b: Bindings) bool {
        return p.variant != null and p.bindings != null and std.meta.eql(p.bindings.?, b);
    }
    fn tensor(api: *const backend.Api, out: *Desc, dims: [4]i64, id: i64, alignment: usize) !void {
        const strides = try nchwStrides(dims);
        try check(api.create_desc(.tensor, out));
        const dtype: c_int = 9; // BF16 storage. Convolution accumulation stays F32.
        const align_bytes: i64 = @intCast(alignment);
        try check(api.set(out.*, 900, .int64, 1, @ptrCast(&align_bytes)));
        try check(api.set(out.*, 901, .data_type, 1, @ptrCast(&dtype)));
        try check(api.set(out.*, 902, .int64, 4, @ptrCast(&dims)));
        try check(api.set(out.*, 903, .int64, 4, @ptrCast(&strides)));
        try check(api.set(out.*, 906, .int64, 1, @ptrCast(&id)));
        try check(api.finalize(out.*));
    }
    fn prepare(p: *GraphPlan, api: *const backend.Api, handle: Desc, key: Key) !void {
        const x = [_]i64{ @intCast(key.batch), @intCast(key.inputs), @intCast(key.height), @intCast(key.width) };
        const w = [_]i64{ @intCast(key.outputs), @intCast(key.inputs), @intCast(key.kernel), @intCast(key.kernel) };
        const y = [_]i64{ @intCast(key.batch), @intCast(key.outputs), @intCast(key.outHeight()), @intCast(key.outWidth()) };
        try tensor(api, &p.x, x, 1, key.alignment);
        try tensor(api, &p.weight, w, 2, key.alignment);
        try tensor(api, &p.y, y, 3, key.alignment);
        try check(api.create_desc(.convolution, &p.convolution));
        const compute: c_int = 0;
        const cross_correlation: c_int = 1;
        const spatial: i64 = 2;
        const dilation = [_]i64{ 1, 1 };
        const stride = [_]i64{ @intCast(key.stride), @intCast(key.stride) };
        const padding = [_]i64{ @intCast(key.padding), @intCast(key.padding) };
        try check(api.set(p.convolution, 100, .data_type, 1, @ptrCast(&compute)));
        try check(api.set(p.convolution, 101, .convolution_mode, 1, @ptrCast(&cross_correlation)));
        try check(api.set(p.convolution, 106, .int64, 1, @ptrCast(&spatial)));
        try check(api.set(p.convolution, 102, .int64, 2, @ptrCast(&dilation)));
        try check(api.set(p.convolution, 103, .int64, 2, @ptrCast(&stride)));
        try check(api.set(p.convolution, 104, .int64, 2, @ptrCast(&padding)));
        try check(api.set(p.convolution, 105, .int64, 2, @ptrCast(&padding)));
        try check(api.finalize(p.convolution));
        try check(api.create_desc(.convolution_fwd, &p.operation));
        const alpha: f32 = 1;
        const beta: f32 = 0;
        try check(api.set(p.operation, 700, .float, 1, @ptrCast(&alpha)));
        try check(api.set(p.operation, 701, .float, 1, @ptrCast(&beta)));
        inline for (.{ "convolution", "weight", "x", "y" }, .{ 702, 703, 704, 705 }) |name, attr| try check(api.set(p.operation, attr, .backend_descriptor, 1, @ptrCast(&@field(p, name))));
        try check(api.finalize(p.operation));
        try check(api.create_desc(.operation_graph, &p.graph));
        try check(api.set(p.graph, 800, .handle, 1, @ptrCast(&handle)));
        try check(api.set(p.graph, 801, .backend_descriptor, 1, @ptrCast(&p.operation)));
        try check(api.finalize(p.graph));
        var heur: Desc = null;
        defer api.destroy(&heur);
        try check(api.create_desc(.engineheur, &heur));
        const mode: c_int = @backingInt(key.heuristic);
        try check(api.set(heur, 200, .heur_mode, 1, @ptrCast(&mode)));
        try check(api.set(heur, 201, .backend_descriptor, 1, @ptrCast(&p.graph)));
        try check(api.finalize(heur));
        var available: i64 = 0;
        try check(api.get(heur, 202, .backend_descriptor, 0, &available, null));
        if (available <= 0) return error.CudnnUnsupported;
        var configs: [32]Desc = @splat(null);
        defer for (&configs) |*config| {
            if (config.* != p.config) api.destroy(config);
        };
        const requested: usize = @min(@as(usize, @intCast(available)), configs.len);
        for (configs[0..requested]) |*config| try check(api.create_desc(.enginecfg, config));
        var count: i64 = 0;
        try check(api.get(heur, 202, .backend_descriptor, @intCast(requested), &count, @ptrCast(&configs)));
        if (count <= 0 or count > @as(i64, @intCast(requested))) return error.CudnnUnsupported;
        for (configs[0..@intCast(count)]) |config| {
            if (!try notesAllowed(api, config, key.deterministic)) continue;
            var execution: Desc = null;
            defer api.destroy(&execution);
            try check(api.create_desc(.execution_plan, &execution));
            try check(api.set(execution, 400, .handle, 1, @ptrCast(&handle)));
            try check(api.set(execution, 401, .backend_descriptor, 1, @ptrCast(&config)));
            const status = api.finalize(execution);
            if (status == 9) continue; // Unsupported candidate, before any math.
            try check(status);
            var bytes: i64 = 0;
            count = 0;
            try check(api.get(execution, 402, .int64, 1, &count, @ptrCast(&bytes)));
            if (count != 1 or bytes < 0) return error.CudnnFailure;
            if (bytes > Module.workspace_limit) continue;
            p.config = config;
            p.execution = execution;
            execution = null;
            p.workspace_bytes = @intCast(bytes);
            return;
        }
        return error.CudnnUnsupported;
    }
};

/// Literal dense NCHW/OIHW layout. Singleton dimensions retain their physical
/// strides; no hidden channels-last reinterpretation or virtual conversion.
pub fn nchwStrides(dims: [4]i64) ![4]i64 {
    for (dims) |dim| if (dim <= 0) return error.InvalidCudnnBuffer;
    const hw = std.math.mul(i64, dims[2], dims[3]) catch return error.InvalidCudnnBuffer;
    return .{ std.math.mul(i64, dims[1], hw) catch return error.InvalidCudnnBuffer, hw, dims[3], 1 };
}
pub fn numericalNotesAllowed(notes: []const c_int, deterministic: bool) bool {
    for (notes) |note| {
        // Match BF16 eager source filtering, not a forced engine/algorithm ID:
        // reject input downconversion; reject nondeterminism only if requested.
        if (note == 1 or (deterministic and note == 4)) return false;
    }
    return true;
}
fn notesAllowed(api: *const backend.Api, config: Desc, deterministic: bool) !bool {
    var engine: Desc = null;
    defer api.destroy(&engine);
    try check(api.create_desc(.engine, &engine));
    var count: i64 = 0;
    try check(api.get(config, 300, .backend_descriptor, 1, &count, @ptrCast(&engine)));
    if (count != 1) return error.CudnnFailure;
    var available: i64 = 0;
    try check(api.get(engine, 1303, .numerical_note, 0, &available, null));
    var notes: [16]c_int = undefined;
    if (available < 0 or available > notes.len) return error.CudnnUnsupported;
    if (available == 0) return true;
    try check(api.get(engine, 1303, .numerical_note, available, &count, @ptrCast(&notes)));
    if (count != available) return error.CudnnFailure;
    return numericalNotesAllowed(notes[0..@intCast(count)], deterministic);
}

pub const Plan = struct {
    key: Key = undefined,
    x: Desc = null,
    y: Desc = null,
    bias: Desc = null,
    weight: Desc = null,
    convolution: Desc = null,
    algorithm: c_int = 0,
    workspace_bytes: usize = 0,
    stamp: u64 = 0,
    graph: GraphPlan = .{},

    fn deinit(p: *Plan, f: *const Table, api: ?*const backend.Api) void {
        if (api) |owned| p.graph.deinit(owned);
        inline for (.{ "x", "y", "bias" }) |name| {
            if (@field(p, name) != null) _ = f.cudnnDestroyTensorDescriptor(@field(p, name));
        }
        if (p.weight != null) _ = f.cudnnDestroyFilterDescriptor(p.weight);
        if (p.convolution != null) _ = f.cudnnDestroyConvolutionDescriptor(p.convolution);
        p.* = .{};
    }
};

pub const Buffer = struct { ptr: usize = 0, bytes: usize = 0 };
pub const Bindings = struct {
    input: Buffer,
    weight: Buffer,
    bias: Buffer = .{},
    output: Buffer,
    workspace: Buffer = .{},
    fn checked(buffer: Buffer, bytes: usize, alignment: usize) !struct { begin: usize, end: usize } {
        if (bytes == 0) return .{ .begin = 0, .end = 0 };
        if (buffer.ptr == 0 or buffer.ptr % alignment != 0 or buffer.bytes < bytes) return error.InvalidCudnnBuffer;
        return .{ .begin = buffer.ptr, .end = std.math.add(usize, buffer.ptr, bytes) catch return error.InvalidCudnnBuffer };
    }
    pub fn validate(b: Bindings, key: Key, workspace_bytes: usize) !void {
        if (!key.eligible() or workspace_bytes > Module.workspace_limit) return error.InvalidCudnnBuffer;
        const alignment = if (key.route == .backend_graph) key.alignment else 2;
        const input = try checked(b.input, try product(&.{ key.batch, key.inputs, key.height, key.width, 2 }), alignment);
        const weight = try checked(b.weight, try product(&.{ key.outputs, key.inputs, key.kernel, key.kernel, 2 }), alignment);
        const output = try checked(b.output, try product(&.{ key.batch, key.outputs, key.outHeight(), key.outWidth(), 2 }), alignment);
        const bias = try checked(b.bias, if (key.bias == .add) try product(&.{ key.outputs, 2 }) else 0, 2);
        const workspace = try checked(b.workspace, workspace_bytes, 256);
        for ([_]@TypeOf(input){ input, weight, bias }) |operand| {
            if (operand.begin < output.end and output.begin < operand.end) return error.InvalidCudnnBuffer;
            if (operand.begin < workspace.end and workspace.begin < operand.end) return error.InvalidCudnnBuffer;
        }
        if (workspace.begin < output.end and output.begin < workspace.end) return error.InvalidCudnnBuffer;
    }
    fn product(factors: []const usize) !usize {
        var value: usize = 1;
        for (factors) |factor| value = std.math.mul(usize, value, factor) catch return error.InvalidCudnnBuffer;
        return value;
    }
};

pub const Module = struct {
    lib: std.DynLib,
    f: Table,
    handle: Desc,
    backend_api: ?backend.Api = null,
    plans: [32]Plan = @splat(.{}),
    count: usize = 0,
    /// Successful forward SDK submissions, not completed GPU kernel launches.
    calls: usize = 0,
    f16_calls: usize = 0,
    bf16_calls: usize = 0,
    bias_calls: usize = 0,
    /// Actual graph SDK submissions; a cuDNN operation is not a GPU kernel count.
    graph_calls: usize = 0,
    /// Peak explicit workspace used by a successful forward submission.
    workspace_peak_bytes: usize = 0,
    serial: u64 = 0,
    pub const workspace_limit = 64 * 1024 * 1024;

    pub fn init(stream: ?*anyopaque) !Module {
        var lib = try @import("libraries.zig").openCudnnLibrary();
        errdefer lib.close();
        var f: Table = undefined;
        inline for (@typeInfo(Table).@"struct".field_names) |name| {
            @field(f, name) = lib.lookup(@FieldType(Table, name), name) orelse return error.CudnnUnavailable;
        }
        var handle: Desc = null;
        try check(f.cudnnCreate(&handle));
        errdefer _ = f.cudnnDestroy(handle);
        try check(f.cudnnSetStream(handle, stream));
        return .{ .lib = lib, .f = f, .handle = handle, .backend_api = backend.Api.lookup(&lib) };
    }
    pub fn deinit(m: *Module) void {
        for (m.plans[0..m.count]) |*p| p.deinit(&m.f, if (m.backend_api) |*api| api else null);
        _ = m.f.cudnnDestroy(m.handle);
        m.lib.close();
        m.* = undefined;
    }
    pub fn cachedPlan(m: *Module, key: Key) ?*const Plan {
        for (m.plans[0..m.count]) |*p| if (std.meta.eql(key, p.key)) {
            m.serial +|= 1;
            p.stamp = m.serial;
            return p;
        };
        return null;
    }
    pub fn needsRetirement(m: *Module, key: Key) bool {
        return m.count == m.plans.len and m.cachedPlan(key) == null;
    }
    /// Cold caller must first synchronize this module's stream. Host Plan
    /// views never escape a call; queued SDK use has completed before destroy.
    /// Captured host bodies use cachedPlan only and cannot replace a slot.
    pub fn retireOldestAfterCompletion(m: *Module) void {
        if (m.count == 0) return;
        var oldest: usize = 0;
        for (m.plans[0..m.count], 0..) |p, i| if (p.stamp < m.plans[oldest].stamp) {
            oldest = i;
        };
        m.plans[oldest].deinit(&m.f, if (m.backend_api) |*api| api else null);
        m.count -= 1;
        if (oldest != m.count) m.plans[oldest] = m.plans[m.count];
        m.plans[m.count] = .{};
    }
    pub fn bindStream(m: *Module, stream: ?*anyopaque) !void {
        try check(m.f.cudnnSetStream(m.handle, stream));
    }
    pub fn needsBindingRetirement(_: *Module, p: *const Plan, bindings: Bindings) bool {
        return p.key.route == .backend_graph and p.graph.variant != null and !p.graph.matches(bindings);
    }
    /// Caller must drain this stream before replacing a published variant pack;
    /// captured bodies never call this or evict descriptor/address owners.
    pub fn retireBindingsAfterCompletion(m: *Module, p: *const Plan) void {
        if (p.key.route != .backend_graph) return;
        const api = if (m.backend_api) |*value| value else unreachable;
        const owned = &@constCast(p).graph;
        api.destroy(&owned.variant);
        owned.bindings = null;
    }
    /// Cold caller invokes this after bank admission and BEFORE the first cast
    /// or numerical submission. No SDK allocation occurs for a prepared replay.
    /// This method never guesses stream-capture state or retires queued owners.
    pub fn prepareBindings(m: *Module, p: *const Plan, bindings: Bindings, actual_capture: bool) !void {
        try bindings.validate(p.key, p.workspace_bytes);
        if (p.key.route != .backend_graph or p.graph.matches(bindings)) return;
        if (actual_capture) return error.CudaGraphCaptureUnsafeTempAlloc;
        if (p.graph.variant != null) return error.CudnnVariantPackNeedsRetirement;
        const api = if (m.backend_api) |*value| value else return error.CudnnUnavailable;
        const owned = &@constCast(p).graph;
        var variant: Desc = null;
        errdefer api.destroy(&variant);
        try check(api.create_desc(.variant_pack, &variant));
        const ids = [_]i64{ 1, 2, 3 };
        const pointers = [_]?*anyopaque{ @ptrFromInt(bindings.input.ptr), @ptrFromInt(bindings.weight.ptr), @ptrFromInt(bindings.output.ptr) };
        var workspace: ?*anyopaque = if (p.workspace_bytes == 0) null else @ptrFromInt(bindings.workspace.ptr);
        try check(api.set(variant, 1000, .int64, ids.len, @ptrCast(&ids)));
        try check(api.set(variant, 1001, .void_ptr, pointers.len, @ptrCast(&pointers)));
        try check(api.set(variant, 1003, .void_ptr, 1, @ptrCast(&workspace)));
        try check(api.finalize(variant));
        owned.variant = variant;
        owned.bindings = bindings;
    }
    pub fn plan(m: *Module, key: Key) !?*const Plan {
        return m.planForCapture(key, false);
    }
    /// Actual capture state must be queried by the stream owner. A cold miss
    /// during capture rejects before any descriptor creation or SDK planning.
    pub fn planForCapture(m: *Module, key: Key, actual_capture: bool) !?*const Plan {
        if (!key.eligible()) return null;
        if (m.cachedPlan(key)) |cached| {
            if (actual_capture and key.route == .backend_graph and cached.graph.variant == null) return error.CudaGraphCaptureUnsafeTempAlloc;
            return cached;
        }
        if (actual_capture) return error.CudaGraphCaptureUnsafeTempAlloc;
        if (m.count == m.plans.len) return null;
        m.serial +|= 1;
        var p = Plan{ .key = key, .stamp = m.serial };
        errdefer p.deinit(&m.f, if (m.backend_api) |*api| api else null);
        if (key.route == .backend_graph) {
            const api = if (m.backend_api) |*value| value else return error.CudnnUnavailable;
            try p.graph.prepare(api, m.handle, key);
            p.workspace_bytes = p.graph.workspace_bytes;
            if (key.bias == .add) {
                try check(m.f.cudnnCreateTensorDescriptor(&p.y));
                try check(m.f.cudnnCreateTensorDescriptor(&p.bias));
                try check(m.f.cudnnSetTensor4dDescriptor(p.y, 0, 9, @intCast(key.batch), @intCast(key.outputs), @intCast(key.outHeight()), @intCast(key.outWidth())));
                try check(m.f.cudnnSetTensor4dDescriptor(p.bias, 0, 9, 1, @intCast(key.outputs), 1, 1));
            }
            m.plans[m.count] = p;
            m.count += 1;
            return &m.plans[m.count - 1];
        }
        try check(m.f.cudnnCreateTensorDescriptor(&p.x));
        try check(m.f.cudnnCreateTensorDescriptor(&p.y));
        try check(m.f.cudnnCreateTensorDescriptor(&p.bias));
        try check(m.f.cudnnCreateFilterDescriptor(&p.weight));
        try check(m.f.cudnnCreateConvolutionDescriptor(&p.convolution));
        // NCHW storage, F32 accumulation and cross-correlation are cache-key contracts.
        const dtype: c_int = if (key.storage == .bf16) 9 else 2;
        try check(m.f.cudnnSetTensor4dDescriptor(p.x, 0, dtype, @intCast(key.batch), @intCast(key.inputs), @intCast(key.height), @intCast(key.width)));
        try check(m.f.cudnnSetTensor4dDescriptor(p.y, 0, dtype, @intCast(key.batch), @intCast(key.outputs), @intCast(key.outHeight()), @intCast(key.outWidth())));
        try check(m.f.cudnnSetTensor4dDescriptor(p.bias, 0, dtype, 1, @intCast(key.outputs), 1, 1));
        try check(m.f.cudnnSetFilter4dDescriptor(p.weight, dtype, 0, @intCast(key.outputs), @intCast(key.inputs), @intCast(key.kernel), @intCast(key.kernel)));
        try check(m.f.cudnnSetConvolution2dDescriptor(p.convolution, @intCast(key.padding), @intCast(key.padding), @intCast(key.stride), @intCast(key.stride), 1, 1, 1, 0));
        try check(m.f.cudnnSetConvolutionMathType(p.convolution, 1));
        var candidates: [8]Perf = undefined;
        var count: c_int = 0;
        try check(m.f.cudnnGetConvolutionForwardAlgorithm_v7(m.handle, p.x, p.weight, p.convolution, p.y, candidates.len, &count, &candidates));
        if (count < 0 or count > candidates.len) return error.CudnnFailure;
        var found = false;
        for (candidates[0..@intCast(count)]) |candidate| {
            if (candidate.status != 0 or candidate.memory > workspace_limit or candidate.determinism != 1) continue;
            p.algorithm = candidate.algo;
            p.workspace_bytes = candidate.memory;
            try check(m.f.cudnnSetConvolutionMathType(p.convolution, candidate.math_type));
            found = true;
            break;
        }
        if (!found) {
            p.deinit(&m.f, null);
            return null;
        }
        m.plans[m.count] = p;
        m.count += 1;
        return &m.plans[m.count - 1];
    }
    pub fn run(m: *Module, p: *const Plan, bindings: Bindings) !void {
        try bindings.validate(p.key, p.workspace_bytes);
        const x = bindings.input.ptr;
        const weight = bindings.weight.ptr;
        const bias = bindings.bias.ptr;
        const output = bindings.output.ptr;
        const workspace = bindings.workspace.ptr;
        const one: f32 = 1;
        const zero: f32 = 0;
        if (p.key.route == .backend_graph) {
            if (!p.graph.matches(bindings)) return error.CudnnVariantPackNotPrepared;
            const api = if (m.backend_api) |*value| value else return error.CudnnUnavailable;
            try check(api.execute(m.handle, p.graph.execution, p.graph.variant));
            m.graph_calls += 1;
        } else {
            try check(m.f.cudnnConvolutionForward(m.handle, &one, p.x, @ptrFromInt(x), p.weight, @ptrFromInt(weight), p.convolution, p.algorithm, if (p.workspace_bytes == 0) null else @ptrFromInt(workspace), p.workspace_bytes, &zero, p.y, @ptrFromInt(output)));
        }
        m.calls += 1;
        switch (p.key.storage) {
            .f16 => m.f16_calls += 1,
            .bf16 => m.bf16_calls += 1,
        }
        m.workspace_peak_bytes = @max(m.workspace_peak_bytes, p.workspace_bytes);
        if (p.key.bias == .add) {
            try check(m.f.cudnnAddTensor(m.handle, &one, p.bias, @ptrFromInt(bias), &one, p.y, @ptrFromInt(output)));
            m.bias_calls += 1;
        }
    }
};
fn check(status: Status) !void {
    if (status != 0) return error.CudnnFailure;
}

test "LightOnOCR3 CUDA Florence convolution eligibility excludes unrelated and oversized geometry" {
    var key = Key{ .batch = 16, .inputs = 3, .outputs = 128, .height = 768, .width = 768, .kernel = 7, .stride = 4, .padding = 3 };
    try std.testing.expect(key.eligible());
    try std.testing.expectEqual(@as(usize, 192), key.outHeight());
    key.batch = 65;
    try std.testing.expect(!key.eligible());
    key.batch = 16;
    key.stride = 2;
    try std.testing.expect(!key.eligible());
    try std.testing.expectEqual(@as(usize, 48), @sizeOf(Perf));
}

test "NCHW vendor convolution cache separates source storage and bias contracts" {
    const source = Key{ .storage = .bf16, .bias = .none, .batch = 1, .inputs = 1, .outputs = 128, .height = 99, .width = 128, .kernel = 3, .stride = 2, .padding = 1 };
    try std.testing.expect(source.eligible());
    try std.testing.expectEqual(@as(usize, 50), source.outHeight());
    try std.testing.expectEqual(@as(usize, 64), source.outWidth());
    var second = source;
    second.inputs = 128;
    second.outputs = 32;
    second.height = 50;
    second.width = 64;
    try std.testing.expect(second.eligible());
    try std.testing.expectEqual(@as(usize, 25), second.outHeight());
    try std.testing.expectEqual(@as(usize, 32), second.outWidth());
    var with_bias = source;
    with_bias.bias = .add;
    var fp16 = source;
    fp16.storage = .f16;
    var module = Module{ .lib = undefined, .f = undefined, .handle = null };
    module.plans[0] = .{ .key = source };
    module.count = 1;
    try std.testing.expect(module.cachedPlan(source) != null);
    try std.testing.expect(module.cachedPlan(with_bias) == null);
    try std.testing.expect(module.cachedPlan(fp16) == null);
    try std.testing.expect(!fp16.eligible()); // Existing FP16 shape gate stays exact.
    second.stride = 1;
    try std.testing.expect(!second.eligible());
    second.stride = 2;
    second.inputs = 0;
    try std.testing.expect(!second.eligible());
}

test "vendor convolution checks actual used extents and rejects output or workspace aliases" {
    const key = Key{ .storage = .bf16, .bias = .none, .batch = 1, .inputs = 1, .outputs = 2, .height = 3, .width = 3, .kernel = 3, .stride = 2, .padding = 1 };
    const good = Bindings{ .input = .{ .ptr = 0x1000, .bytes = 18 }, .weight = .{ .ptr = 0x2000, .bytes = 36 }, .output = .{ .ptr = 0x3000, .bytes = 16 }, .workspace = .{ .ptr = 0x4000, .bytes = 256 } };
    try good.validate(key, 256);
    var invalid = good;
    invalid.input.bytes -= 1;
    try std.testing.expectError(error.InvalidCudnnBuffer, invalid.validate(key, 256));
    invalid = good;
    invalid.output.ptr = good.input.ptr;
    try std.testing.expectError(error.InvalidCudnnBuffer, invalid.validate(key, 256));
    invalid = good;
    invalid.workspace.ptr = good.weight.ptr;
    try std.testing.expectError(error.InvalidCudnnBuffer, invalid.validate(key, 256));
    invalid = good;
    invalid.workspace.ptr += 2;
    try std.testing.expectError(error.InvalidCudnnBuffer, invalid.validate(key, 256));
    invalid = good;
    invalid.weight.ptr = std.math.maxInt(usize) - 1;
    try std.testing.expectError(error.InvalidCudnnBuffer, invalid.validate(key, 256));
    var needs_bias = key;
    needs_bias.bias = .add;
    try std.testing.expectError(error.InvalidCudnnBuffer, good.validate(needs_bias, 256));
    // Oversized available capacities do not create a false alias: only the
    // actual source operand footprint is read by this convolution.
    var capacity = good;
    capacity.input.bytes = 0x3000;
    try capacity.validate(key, 256);
}

test "bounded convolution cache retires cold LRU descriptors beyond thirty two shapes" {
    var module = Module{ .lib = undefined, .f = undefined, .handle = null };
    const key = Key{ .storage = .bf16, .bias = .none, .batch = 1, .inputs = 1, .outputs = 128, .height = 1, .width = 128, .kernel = 3, .stride = 2, .padding = 1 };
    // Null descriptors model independently retired CPU metadata: no SDK call.
    for (0..96) |shape| {
        var next = key;
        next.height = shape + 1;
        if (module.needsRetirement(next)) module.retireOldestAfterCompletion();
        try std.testing.expect(module.count < module.plans.len);
        module.serial += 1;
        module.plans[module.count] = .{ .key = next, .stamp = module.serial };
        module.count += 1;
        try std.testing.expect(module.cachedPlan(next) != null);
    }
    try std.testing.expectEqual(@as(usize, 32), module.count);
    try std.testing.expect(module.cachedPlan(key) == null);
}

test "vendor convolution reports actual successful SDK submissions and explicit workspace" {
    const Fake = struct {
        var forward_status: Status = 0;
        var bias_status: Status = 0;
        fn forward(_: Desc, _: *const f32, _: Desc, _: ?*const anyopaque, _: Desc, _: ?*const anyopaque, _: Desc, _: c_int, _: ?*anyopaque, _: usize, _: *const f32, _: Desc, _: ?*anyopaque) callconv(.c) Status {
            return forward_status;
        }
        fn bias(_: Desc, _: *const f32, _: Desc, _: ?*const anyopaque, _: *const f32, _: Desc, _: ?*anyopaque) callconv(.c) Status {
            return bias_status;
        }
    };
    Fake.forward_status = 0;
    Fake.bias_status = 0;
    var module = Module{ .lib = undefined, .f = undefined, .handle = null };
    module.f.cudnnConvolutionForward = Fake.forward;
    module.f.cudnnAddTensor = Fake.bias;
    const plan = Plan{ .key = .{ .storage = .bf16, .bias = .add, .batch = 1, .inputs = 1, .outputs = 2, .height = 3, .width = 3, .kernel = 3, .stride = 2, .padding = 1 }, .workspace_bytes = 256 };
    const buffers = Bindings{ .input = .{ .ptr = 0x1000, .bytes = 18 }, .weight = .{ .ptr = 0x2000, .bytes = 36 }, .bias = .{ .ptr = 0x2800, .bytes = 4 }, .output = .{ .ptr = 0x3000, .bytes = 16 }, .workspace = .{ .ptr = 0x4000, .bytes = 256 } };
    try module.run(&plan, buffers);
    try std.testing.expectEqual(@as(usize, 1), module.calls);
    try std.testing.expectEqual(@as(usize, 1), module.bf16_calls);
    try std.testing.expectEqual(@as(usize, 0), module.f16_calls);
    try std.testing.expectEqual(@as(usize, 1), module.bias_calls);
    try std.testing.expectEqual(@as(usize, 256), module.workspace_peak_bytes);
    // A failed bias must not hide an already submitted forward operation.
    Fake.bias_status = 1;
    try std.testing.expectError(error.CudnnFailure, module.run(&plan, buffers));
    try std.testing.expectEqual(@as(usize, 2), module.calls);
    try std.testing.expectEqual(@as(usize, 2), module.bf16_calls);
    try std.testing.expectEqual(@as(usize, 1), module.bias_calls);
    Fake.forward_status = 1;
    try std.testing.expectError(error.CudnnFailure, module.run(&plan, buffers));
    try std.testing.expectEqual(@as(usize, 2), module.calls);
    Fake.forward_status = 0;
    const fp16_plan = Plan{ .key = .{ .storage = .f16, .bias = .none, .batch = 1, .inputs = 128, .outputs = 256, .height = 3, .width = 3, .kernel = 3, .stride = 2, .padding = 1 } };
    try module.run(&fp16_plan, .{ .input = .{ .ptr = 0x1000, .bytes = 2304 }, .weight = .{ .ptr = 0x100000, .bytes = 589824 }, .output = .{ .ptr = 0x200000, .bytes = 2048 } });
    try std.testing.expectEqual(@as(usize, 3), module.calls);
    try std.testing.expectEqual(@as(usize, 1), module.f16_calls);
    try std.testing.expectEqual(@as(usize, 2), module.bf16_calls);
    try std.testing.expectEqual(@as(usize, 1), module.bias_calls);
    try std.testing.expectEqual(@as(usize, 256), module.workspace_peak_bytes);
    var invalid = buffers;
    invalid.output.ptr = invalid.input.ptr;
    try std.testing.expectError(error.InvalidCudnnBuffer, module.run(&plan, invalid));
    try std.testing.expectEqual(@as(usize, 3), module.calls);
}

test "graph convolution keeps literal layouts source numerical filtering and distinct cache identities" {
    try std.testing.expectEqual([4]i64{ 384, 384, 128, 1 }, try nchwStrides(.{ 1, 1, 3, 128 }));
    try std.testing.expectEqual([4]i64{ 1152, 9, 3, 1 }, try nchwStrides(.{ 32, 128, 3, 3 }));
    try std.testing.expectError(error.InvalidCudnnBuffer, nchwStrides(.{ 1, 1, 0, 1 }));
    try std.testing.expectError(error.InvalidCudnnBuffer, nchwStrides(.{ 1, std.math.maxInt(i64), 2, 2 }));
    try std.testing.expect(numericalNotesAllowed(&.{ 0, 2, 4 }, false));
    try std.testing.expect(!numericalNotesAllowed(&.{4}, true));
    try std.testing.expect(!numericalNotesAllowed(&.{1}, false));
    var key = Key{ .storage = .bf16, .bias = .none, .batch = 1, .inputs = 1, .outputs = 2, .height = 3, .width = 3, .kernel = 3, .stride = 2, .padding = 1 };
    var module = Module{ .lib = undefined, .f = undefined, .handle = null };
    module.plans[0] = .{ .key = key };
    module.count = 1;
    key.route = .backend_graph;
    try std.testing.expect(module.cachedPlan(key) == null);
    module.plans[1] = .{ .key = key };
    module.count = 2;
    key.deterministic = true;
    try std.testing.expect(module.cachedPlan(key) == null);
    key.deterministic = false;
    key.heuristic = .mode_b;
    try std.testing.expect(module.cachedPlan(key) == null);
    key.heuristic = .instant;
    key.alignment = 16;
    try std.testing.expect(module.cachedPlan(key) == null);
}

test "graph convolution capture misses and malformed buffers reject before any SDK owner access" {
    var module = Module{ .lib = undefined, .f = undefined, .handle = undefined };
    const key = Key{ .route = .backend_graph, .storage = .bf16, .bias = .none, .batch = 1, .inputs = 1, .outputs = 2, .height = 3, .width = 3, .kernel = 3, .stride = 2, .padding = 1 };
    try std.testing.expectError(error.CudaGraphCaptureUnsafeTempAlloc, module.planForCapture(key, true));
    var invalid = key;
    invalid.inputs = 0;
    try std.testing.expect((try module.planForCapture(invalid, true)) == null);
    const plan = Plan{ .key = key };
    module.plans[0] = plan;
    module.count = 1;
    try std.testing.expectError(error.CudaGraphCaptureUnsafeTempAlloc, module.planForCapture(key, true));
    try std.testing.expectError(error.InvalidCudnnBuffer, module.prepareBindings(&plan, .{ .input = .{}, .weight = .{}, .output = .{} }, true));
    const buffers = Bindings{ .input = .{ .ptr = 0x1000, .bytes = 18 }, .weight = .{ .ptr = 0x2000, .bytes = 36 }, .output = .{ .ptr = 0x3000, .bytes = 16 } };
    try std.testing.expectError(error.CudaGraphCaptureUnsafeTempAlloc, module.prepareBindings(&plan, buffers, true));
    var misaligned = buffers;
    misaligned.input.ptr += 2;
    try std.testing.expectError(error.InvalidCudnnBuffer, module.prepareBindings(&plan, misaligned, false));
    try std.testing.expectEqual(@as(usize, 0), module.calls);
}

// A deterministic host-only SDK double: no CUDA driver, allocator or real
// descriptor is reachable. Failed creation may publish a partial owner.
const TestBackend = struct {
    var calls: usize = 0;
    var fail_call: usize = 0;
    var active: usize = 0;
    var next: usize = 0;
    var executions: usize = 0;
    var execution_candidates: usize = 0;
    var descriptor_kinds: [256]backend.DescriptorType = undefined;
    var fail_execute: bool = false;
    fn reset() void {
        calls = 0;
        fail_call = 0;
        active = 0;
        next = 0;
        executions = 0;
        execution_candidates = 0;
        fail_execute = false;
    }
    fn status() Status {
        calls += 1;
        return if (calls == fail_call) 1 else 0;
    }
    fn create(kind: backend.DescriptorType, out: *Desc) callconv(.c) Status {
        next += 1;
        descriptor_kinds[next] = kind;
        if (kind == .execution_plan) execution_candidates += 1;
        out.* = @ptrFromInt(next * 0x1000);
        active += 1;
        return status();
    }
    fn destroy(_: Desc) callconv(.c) Status {
        active -= 1;
        return 0;
    }
    fn set(_: Desc, _: c_int, _: backend.AttributeType, _: i64, _: ?*const anyopaque) callconv(.c) Status {
        return status();
    }
    fn get(_: Desc, attr: c_int, _: backend.AttributeType, requested: i64, count: *i64, output: ?*anyopaque) callconv(.c) Status {
        const result = status();
        if (result != 0) return result;
        switch (attr) {
            202 => count.* = if (requested == 0) 2 else requested,
            300 => count.* = 1, // Keep caller-created engine descriptor owner.
            1303 => count.* = 0,
            402 => {
                count.* = 1;
                const bytes: *i64 = @ptrCast(@alignCast(output.?));
                bytes.* = if (execution_candidates == 1) Module.workspace_limit + 1 else 4096;
            },
            else => return 1,
        }
        return 0;
    }
    fn finalize(_: Desc) callconv(.c) Status {
        return status();
    }
    fn execute(_: Desc, _: Desc, _: Desc) callconv(.c) Status {
        executions += 1;
        return if (fail_execute) 1 else 0;
    }
    const api = backend.Api{ .create_desc = create, .destroy_desc = destroy, .set = set, .get = get, .finalize = finalize, .execute = execute };
};

test "graph convolution cold construction unwinds every SDK failure and honors existing workspace cap" {
    const key = Key{ .route = .backend_graph, .storage = .bf16, .bias = .none, .batch = 1, .inputs = 1, .outputs = 2, .height = 3, .width = 3, .kernel = 3, .stride = 2, .padding = 1 };
    TestBackend.reset();
    var module = Module{ .lib = undefined, .f = undefined, .handle = null, .backend_api = TestBackend.api };
    const plan = (try module.planForCapture(key, false)).?;
    try std.testing.expectEqual(@as(usize, 4096), plan.workspace_bytes);
    try std.testing.expectEqual(@as(usize, 2), TestBackend.execution_candidates);
    const successful_calls = TestBackend.calls;
    @constCast(plan).deinit(&module.f, &module.backend_api.?);
    try std.testing.expectEqual(@as(usize, 0), TestBackend.active);
    for (1..successful_calls + 1) |failure| {
        TestBackend.reset();
        TestBackend.fail_call = failure;
        module = .{ .lib = undefined, .f = undefined, .handle = null, .backend_api = TestBackend.api };
        try std.testing.expectError(error.CudnnFailure, module.planForCapture(key, false));
        try std.testing.expectEqual(@as(usize, 0), module.count);
        try std.testing.expectEqual(@as(usize, 0), TestBackend.active);
    }
}

test "graph convolution prepared replay allocates nothing rejects address changes and never retries submission" {
    TestBackend.reset();
    var module = Module{ .lib = undefined, .f = undefined, .handle = null, .backend_api = TestBackend.api };
    const key = Key{ .route = .backend_graph, .storage = .bf16, .bias = .none, .batch = 1, .inputs = 1, .outputs = 2, .height = 3, .width = 3, .kernel = 3, .stride = 2, .padding = 1 };
    const plan = (try module.planForCapture(key, false)).?;
    defer @constCast(plan).deinit(&module.f, &module.backend_api.?);
    const b = Bindings{ .input = .{ .ptr = 0x1000, .bytes = 18 }, .weight = .{ .ptr = 0x2000, .bytes = 36 }, .output = .{ .ptr = 0x3000, .bytes = 16 }, .workspace = .{ .ptr = 0x4000, .bytes = 4096 } };
    const active = TestBackend.active;
    for (1..6) |failure| {
        TestBackend.fail_call = TestBackend.calls + failure;
        try std.testing.expectError(error.CudnnFailure, module.prepareBindings(plan, b, false));
        try std.testing.expect(plan.graph.variant == null and plan.graph.bindings == null);
        try std.testing.expectEqual(active, TestBackend.active);
    }
    TestBackend.fail_call = 0;
    try module.prepareBindings(plan, b, false);
    const calls = TestBackend.calls;
    try std.testing.expect((try module.planForCapture(key, true)).? == plan);
    try module.prepareBindings(plan, b, true);
    try std.testing.expectEqual(calls, TestBackend.calls);
    try module.run(plan, b);
    try std.testing.expectEqual(@as(usize, 1), module.calls);
    try std.testing.expectEqual(@as(usize, 1), module.graph_calls);
    var moved = b;
    moved.output.ptr = 0x6000;
    try std.testing.expect(module.needsBindingRetirement(plan, moved));
    try std.testing.expectError(error.CudaGraphCaptureUnsafeTempAlloc, module.prepareBindings(plan, moved, true));
    try std.testing.expectError(error.CudnnVariantPackNeedsRetirement, module.prepareBindings(plan, moved, false));
    try std.testing.expectError(error.CudnnVariantPackNotPrepared, module.run(plan, moved));
    try std.testing.expectEqual(@as(usize, 1), TestBackend.executions);
    TestBackend.fail_execute = true;
    try std.testing.expectError(error.CudnnFailure, module.run(plan, b));
    try std.testing.expectEqual(@as(usize, 2), TestBackend.executions);
    try std.testing.expectEqual(@as(usize, 1), module.graph_calls);
    // Explicit completion authority is a caller contract, never inferred by run.
    module.retireBindingsAfterCompletion(plan);
    try module.prepareBindings(plan, moved, false);
}

test "CUDA backend convolution BF16 NCHW primitive preserves center filters tails and separate bias publication" {
    if (comptime !@import("build_options").enable_cuda) return error.SkipZigTest;
    const Context = @import("context.zig").CudaContext;
    const DeviceBuffer = @import("buffer.zig").DeviceBuffer;
    var ctx = Context.initDefault() catch |err| switch (err) {
        error.CudaUnavailable, error.NoCudaDevices => return error.SkipZigTest,
        else => return err,
    };
    defer ctx.deinit();
    if (ctx.info.compute_major < 8) return error.SkipZigTest;
    var module = Module.init(ctx.stream) catch |err| switch (err) {
        error.CudnnUnavailable => return error.SkipZigTest,
        else => return err,
    };
    defer module.deinit();
    if (module.backend_api == null) return error.SkipZigTest;
    const key = Key{ .route = .backend_graph, .storage = .bf16, .bias = .add, .batch = 2, .inputs = 8, .outputs = 8, .height = 5, .width = 7, .kernel = 3, .stride = 2, .padding = 1 };
    const plan = (try module.planForCapture(key, false)) orelse return error.CudnnUnsupported;
    const Fixture = struct {
        fn bits(value: f32) u16 {
            return @truncate(@as(u32, @bitCast(value)) >> 16);
        }
        fn input(b: usize, c: usize, h: usize, w: usize) f32 {
            return @as(f32, @floatFromInt(b * 64 + c * 16 + h * 2 + w)) / 8;
        }
    };
    var host_x: [2 * 8 * 5 * 7]u16 = undefined;
    var host_w: [8 * 8 * 3 * 3]u16 = @splat(0);
    var host_bias: [8]u16 = undefined;
    var expected: [2 * 8 * 3 * 4]u16 = undefined;
    var actual: [2 * 8 * 3 * 4]u16 = @splat(0x7fc1);
    for (0..key.batch) |b| for (0..key.inputs) |c| for (0..key.height) |h| for (0..key.width) |w| {
        host_x[((b * key.inputs + c) * key.height + h) * key.width + w] = Fixture.bits(Fixture.input(b, c, h, w));
    };
    for (0..key.outputs) |o| {
        const bias: f32 = if (o % 2 == 0) 0.5 else -0.5;
        host_bias[o] = Fixture.bits(bias);
        host_w[(o * key.inputs + (o * 3) % key.inputs) * 9 + 4] = Fixture.bits(1);
        for (0..key.batch) |b| for (0..key.outHeight()) |h| for (0..key.outWidth()) |w| {
            expected[((b * key.outputs + o) * key.outHeight() + h) * key.outWidth() + w] = Fixture.bits(Fixture.input(b, (o * 3) % key.inputs, h * 2, w * 2) + bias);
        };
    }
    var x = try DeviceBuffer.alloc(&ctx, @sizeOf(@TypeOf(host_x)));
    defer x.free(&ctx);
    var weight = try DeviceBuffer.alloc(&ctx, @sizeOf(@TypeOf(host_w)));
    defer weight.free(&ctx);
    var bias = try DeviceBuffer.alloc(&ctx, @sizeOf(@TypeOf(host_bias)));
    defer bias.free(&ctx);
    var y = try DeviceBuffer.alloc(&ctx, @sizeOf(@TypeOf(actual)));
    defer y.free(&ctx);
    var workspace = try DeviceBuffer.alloc(&ctx, plan.workspace_bytes);
    defer workspace.free(&ctx);
    errdefer ctx.synchronize() catch @import("antfly_platform").inference_process_supervisor.restartWorker();
    const bindings = Bindings{ .input = .{ .ptr = x.ptr, .bytes = x.len }, .weight = .{ .ptr = weight.ptr, .bytes = weight.len }, .bias = .{ .ptr = bias.ptr, .bytes = bias.len }, .output = .{ .ptr = y.ptr, .bytes = y.len }, .workspace = .{ .ptr = workspace.ptr, .bytes = workspace.len } };
    try module.prepareBindings(plan, bindings, false);
    try x.copyFromHost(&ctx, std.mem.sliceAsBytes(&host_x));
    try weight.copyFromHost(&ctx, std.mem.sliceAsBytes(&host_w));
    try bias.copyFromHost(&ctx, std.mem.sliceAsBytes(&host_bias));
    try y.copyFromHost(&ctx, std.mem.sliceAsBytes(&actual));
    try module.run(plan, bindings);
    try y.copyToHost(&ctx, std.mem.sliceAsBytes(&actual));
    try ctx.synchronize();
    try std.testing.expectEqualSlices(u16, &expected, &actual);
    try std.testing.expectEqual(@as(usize, 1), module.graph_calls);
    try std.testing.expectEqual(@as(usize, 1), module.bf16_calls);
    try std.testing.expectEqual(@as(usize, 1), module.bias_calls);
}
