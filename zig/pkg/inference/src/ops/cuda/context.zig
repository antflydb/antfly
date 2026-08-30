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
const driver_mod = @import("driver.zig");

const CudaDriver = driver_mod.CudaDriver;
const CUcontext = driver_mod.CUcontext;
const CUdevice = driver_mod.CUdevice;
const CUgraph = driver_mod.CUgraph;
const CUgraphExec = driver_mod.CUgraphExec;
const CUgraphExecUpdateResult = driver_mod.CUgraphExecUpdateResult;
const CUgraphInstantiateResult = driver_mod.CUgraphInstantiateResult;
const CUgraphNode = driver_mod.CUgraphNode;
const CUevent = driver_mod.CUevent;
const CUstream = driver_mod.CUstream;

pub const DeviceInfo = struct {
    driver_version: i32 = 0,
    device_count: i32 = 0,
    selected_device: i32 = 0,
    name: [256]u8 = @splat(0),
    name_len: usize = 0,
    compute_major: i32 = 0,
    compute_minor: i32 = 0,

    pub fn nameSlice(self: *const DeviceInfo) []const u8 {
        return self.name[0..self.name_len];
    }
};

pub const RuntimeStats = struct {
    kernel_launches: usize = 0,
    stream_syncs: usize = 0,
};

pub const DeviceMemoryInfo = struct {
    free_bytes: usize,
    total_bytes: usize,
};

fn graphInstantiateResultName(result: CUgraphInstantiateResult) []const u8 {
    return switch (result) {
        driver_mod.CUDA_GRAPH_INSTANTIATE_SUCCESS => "success",
        driver_mod.CUDA_GRAPH_INSTANTIATE_ERROR => "error",
        driver_mod.CUDA_GRAPH_INSTANTIATE_INVALID_STRUCTURE => "invalid_structure",
        driver_mod.CUDA_GRAPH_INSTANTIATE_NODE_OPERATION_NOT_SUPPORTED => "node_operation_not_supported",
        driver_mod.CUDA_GRAPH_INSTANTIATE_MULTIPLE_CTXS_NOT_SUPPORTED => "multiple_contexts_not_supported",
        driver_mod.CUDA_GRAPH_INSTANTIATE_CONDITIONAL_HANDLE_UNUSED => "conditional_handle_unused",
        else => "unknown",
    };
}

pub const ProfileEventPair = struct {
    start: CUevent = null,
    end: CUevent = null,
};

pub const GraphExecUpdateOutcome = struct {
    cuda_result: driver_mod.CUresult,
    update_result: CUgraphExecUpdateResult = driver_mod.CU_GRAPH_EXEC_UPDATE_ERROR,
    error_node: CUgraphNode = null,
    error_from_node: CUgraphNode = null,

    pub fn success(self: GraphExecUpdateOutcome) bool {
        return self.cuda_result == driver_mod.CUDA_SUCCESS and
            self.update_result == driver_mod.CU_GRAPH_EXEC_UPDATE_SUCCESS;
    }
};

pub const CudaContext = struct {
    driver: CudaDriver,
    device: CUdevice,
    ctx: CUcontext,
    stream: CUstream,
    info: DeviceInfo,
    stats: RuntimeStats = .{},
    debug_graph_capture_active: bool = false,
    debug_graph_capture_id: usize = 0,
    debug_graph_capture_param_trace_count: usize = 0,
    debug_graph_capture_tensor_trace_count: usize = 0,

    pub fn initDefault() driver_mod.Error!CudaContext {
        var driver = try CudaDriver.open();
        errdefer driver.deinit();

        checkStep(&driver, "cuInit", driver.fns.cuInit(0)) catch |err| return err;

        var info = DeviceInfo{};
        checkStep(&driver, "cuDriverGetVersion", driver.fns.cuDriverGetVersion(&info.driver_version)) catch |err| return err;
        checkStep(&driver, "cuDeviceGetCount", driver.fns.cuDeviceGetCount(&info.device_count)) catch |err| return err;
        if (info.device_count <= 0) return error.NoCudaDevices;

        var device: CUdevice = 0;
        checkStep(&driver, "cuDeviceGet", driver.fns.cuDeviceGet(&device, 0)) catch |err| return err;
        info.selected_device = device;

        var name_buf: [256]u8 = @splat(0);
        checkStep(&driver, "cuDeviceGetName", driver.fns.cuDeviceGetName(&name_buf, name_buf.len, device)) catch |err| return err;
        info.name = name_buf;
        info.name_len = std.mem.indexOfScalar(u8, &info.name, 0) orelse info.name.len;

        checkStep(&driver, "cuDeviceComputeCapability", driver.fns.cuDeviceComputeCapability(&info.compute_major, &info.compute_minor, device)) catch |err| return err;

        var ctx: CUcontext = null;
        checkStep(&driver, "cuDevicePrimaryCtxRetain", driver.fns.cuDevicePrimaryCtxRetain(&ctx, device)) catch |err| return err;
        errdefer _ = driver.fns.cuDevicePrimaryCtxRelease(device);
        checkStep(&driver, "cuCtxSetCurrent", driver.fns.cuCtxSetCurrent(ctx)) catch |err| return err;

        var stream: CUstream = null;
        checkStep(&driver, "cuStreamCreate", driver.fns.cuStreamCreate(&stream, 0)) catch |err| return err;
        errdefer _ = driver.fns.cuStreamDestroy(stream);

        return .{
            .driver = driver,
            .device = device,
            .ctx = ctx,
            .stream = stream,
            .info = info,
        };
    }

    pub fn deinit(self: *CudaContext) void {
        if (self.ctx != null) {
            _ = self.driver.fns.cuCtxSetCurrent(self.ctx);
        }
        if (self.stream != null) {
            _ = self.driver.fns.cuStreamDestroy(self.stream);
            self.stream = null;
        }
        if (self.ctx != null) {
            _ = self.driver.fns.cuCtxSetCurrent(null);
            _ = self.driver.fns.cuDevicePrimaryCtxRelease(self.device);
            self.ctx = null;
        }
        self.driver.deinit();
    }

    pub fn makeCurrent(self: *CudaContext) driver_mod.Error!void {
        if (self.ctx == null) return error.InvalidCudaState;
        try self.driver.check(self.driver.fns.cuCtxSetCurrent(self.ctx));
    }

    pub fn synchronize(self: *CudaContext) driver_mod.Error!void {
        try self.makeCurrent();
        try self.driver.check(self.driver.fns.cuStreamSynchronize(self.stream));
        self.stats.stream_syncs += 1;
    }

    pub fn memoryInfo(self: *CudaContext) driver_mod.Error!DeviceMemoryInfo {
        try self.makeCurrent();
        var free_bytes: usize = 0;
        var total_bytes: usize = 0;
        try self.driver.check(self.driver.fns.cuMemGetInfo(&free_bytes, &total_bytes));
        if (total_bytes == 0 or free_bytes > total_bytes) return error.InvalidCudaState;
        return .{ .free_bytes = free_bytes, .total_bytes = total_bytes };
    }

    pub fn createStreamEvent(self: *CudaContext) driver_mod.Error!CUevent {
        try self.makeCurrent();
        var event: CUevent = null;
        try self.driver.check(self.driver.fns.cuEventCreate(&event, driver_mod.CU_EVENT_DISABLE_TIMING));
        if (event == null) return error.InvalidCudaState;
        return event;
    }

    pub fn recordEvent(self: *CudaContext, event: CUevent) driver_mod.Error!void {
        if (event == null) return error.InvalidCudaState;
        try self.makeCurrent();
        try self.driver.check(self.driver.fns.cuEventRecord(event, self.stream));
    }

    pub fn waitEvent(self: *CudaContext, event: CUevent) driver_mod.Error!void {
        if (event == null) return error.InvalidCudaState;
        try self.makeCurrent();
        try self.driver.check(self.driver.fns.cuStreamWaitEvent(self.stream, event, 0));
    }

    pub fn beginStreamCapture(self: *CudaContext, mode: driver_mod.CUstreamCaptureMode) driver_mod.Error!void {
        try self.makeCurrent();
        try self.driver.check(self.driver.fns.cuStreamBeginCapture(self.stream, mode));
        self.debug_graph_capture_active = true;
        self.debug_graph_capture_id += 1;
        self.debug_graph_capture_param_trace_count = 0;
        self.debug_graph_capture_tensor_trace_count = 0;
    }

    pub fn endStreamCapture(self: *CudaContext) driver_mod.Error!CUgraph {
        try self.makeCurrent();
        var graph: CUgraph = null;
        self.driver.check(self.driver.fns.cuStreamEndCapture(self.stream, &graph)) catch |err| {
            self.debug_graph_capture_active = false;
            return err;
        };
        self.debug_graph_capture_active = false;
        if (graph == null) return error.InvalidCudaState;
        return graph;
    }

    pub fn instantiateGraph(self: *CudaContext, graph: CUgraph) driver_mod.Error!CUgraphExec {
        try self.makeCurrent();
        var exec: CUgraphExec = null;
        if (self.driver.fns.cuGraphInstantiateWithParams) |instantiate_with_params| {
            var params = driver_mod.CUDA_GRAPH_INSTANTIATE_PARAMS{};
            const result = instantiate_with_params(&exec, graph, &params);
            if (result != driver_mod.CUDA_SUCCESS or params.result_out != driver_mod.CUDA_GRAPH_INSTANTIATE_SUCCESS) {
                std.log.warn("cuda_graph_capture_probe: instantiate_with_params_failed cuda={s} result={s} err_node=0x{x}", .{
                    self.driver.errorName(result),
                    graphInstantiateResultName(params.result_out),
                    @intFromPtr(params.hErrNode_out),
                });
                try self.driver.check(result);
                return error.CudaDriverError;
            }
        } else {
            try self.driver.check(self.driver.fns.cuGraphInstantiate(&exec, graph, 0));
        }
        if (exec == null) return error.InvalidCudaState;
        return exec;
    }

    pub fn updateGraphExec(self: *CudaContext, exec: CUgraphExec, graph: CUgraph) driver_mod.Error!GraphExecUpdateOutcome {
        const update_fn = self.driver.fns.cuGraphExecUpdate orelse return error.CudaSymbolMissing;
        try self.makeCurrent();
        var outcome = GraphExecUpdateOutcome{ .cuda_result = driver_mod.CUDA_SUCCESS };
        var result_info = driver_mod.CUgraphExecUpdateResultInfo{};
        outcome.cuda_result = update_fn(exec, graph, &result_info);
        outcome.update_result = result_info.result;
        outcome.error_node = result_info.errorNode;
        outcome.error_from_node = result_info.errorFromNode;
        return outcome;
    }

    pub fn launchGraph(self: *CudaContext, exec: CUgraphExec) driver_mod.Error!void {
        try self.makeCurrent();
        try self.driver.check(self.driver.fns.cuGraphLaunch(exec, self.stream));
        self.noteKernelLaunch();
    }

    /// Return the driver's authoritative stream-capture state when the
    /// installed CUDA driver exposes it. The local flag remains the fallback
    /// for older drivers and for unit-test contexts. Treat an invalidated
    /// capture as active: descriptor creation, event timing, and other
    /// capture-unsafe first-use work must still be rejected on that stream.
    pub fn streamCaptureActive(self: *CudaContext) driver_mod.Error!bool {
        const query = self.driver.fns.cuStreamIsCapturing orelse
            return self.debug_graph_capture_active;
        try self.makeCurrent();
        var status: driver_mod.CUstreamCaptureStatus = driver_mod.CU_STREAM_CAPTURE_STATUS_NONE;
        try self.driver.check(query(self.stream, &status));
        return status != driver_mod.CU_STREAM_CAPTURE_STATUS_NONE;
    }

    /// Create a timing event pair without recording either event. Used by the
    /// pooled, non-blocking profiler so the two `cuEventCreate` calls happen once
    /// per pool slot rather than once per profiled op.
    pub fn createProfileEventPair(self: *CudaContext) driver_mod.Error!ProfileEventPair {
        try self.makeCurrent();
        var pair = ProfileEventPair{};
        errdefer self.destroyProfileEventPair(pair);
        try self.driver.check(self.driver.fns.cuEventCreate(&pair.start, driver_mod.CU_EVENT_DEFAULT));
        try self.driver.check(self.driver.fns.cuEventCreate(&pair.end, driver_mod.CU_EVENT_DEFAULT));
        return pair;
    }

    /// Record the start event on the compute stream. Returns immediately (async).
    pub fn recordProfileStart(self: *CudaContext, pair: ProfileEventPair) driver_mod.Error!void {
        try self.makeCurrent();
        try self.driver.check(self.driver.fns.cuEventRecord(pair.start, self.stream));
    }

    /// Record the end event on the compute stream. Returns immediately (async):
    /// unlike `endProfileEventPairUs` this performs NO host synchronization, so a
    /// caller timing many ops can defer a single sync until it drains the batch.
    pub fn recordProfileEnd(self: *CudaContext, pair: ProfileEventPair) driver_mod.Error!void {
        try self.makeCurrent();
        try self.driver.check(self.driver.fns.cuEventRecord(pair.end, self.stream));
    }

    /// Read the elapsed time between an already-recorded, already-completed event
    /// pair. The caller MUST have synchronized (e.g. one `cuStreamSynchronize`
    /// covering the whole batch) so both events have fired; this neither
    /// synchronizes nor destroys the events (the pool reuses them).
    pub fn readProfileEventPairUs(self: *CudaContext, pair: ProfileEventPair) driver_mod.Error!u64 {
        var elapsed_ms: f32 = 0;
        try self.driver.check(self.driver.fns.cuEventElapsedTime(&elapsed_ms, pair.start, pair.end));
        if (elapsed_ms <= 0) return 0;
        return @intFromFloat(@as(f64, elapsed_ms) * 1000.0);
    }

    pub fn beginProfileEventPair(self: *CudaContext) driver_mod.Error!ProfileEventPair {
        const pair = try self.createProfileEventPair();
        errdefer self.destroyProfileEventPair(pair);
        try self.recordProfileStart(pair);
        return pair;
    }

    pub fn endProfileEventPairUs(self: *CudaContext, pair: ProfileEventPair) driver_mod.Error!u64 {
        defer self.destroyProfileEventPair(pair);
        try self.recordProfileEnd(pair);
        try self.driver.check(self.driver.fns.cuEventSynchronize(pair.end));
        return self.readProfileEventPairUs(pair);
    }

    pub fn destroyGraphExec(self: *CudaContext, exec: CUgraphExec) void {
        if (exec == null) return;
        self.makeCurrent() catch {};
        _ = self.driver.fns.cuGraphExecDestroy(exec);
    }

    pub fn destroyGraph(self: *CudaContext, graph: CUgraph) void {
        if (graph == null) return;
        self.makeCurrent() catch {};
        _ = self.driver.fns.cuGraphDestroy(graph);
    }

    pub fn destroyProfileEventPair(self: *CudaContext, pair: ProfileEventPair) void {
        if (pair.start != null) {
            _ = self.driver.fns.cuEventDestroy(pair.start);
        }
        if (pair.end != null) {
            _ = self.driver.fns.cuEventDestroy(pair.end);
        }
    }

    pub fn destroyEvent(self: *CudaContext, event: CUevent) void {
        if (event == null) return;
        self.makeCurrent() catch {};
        _ = self.driver.fns.cuEventDestroy(event);
    }

    pub fn noteKernelLaunch(self: *CudaContext) void {
        self.stats.kernel_launches += 1;
    }
};

fn checkStep(driver: *const CudaDriver, step: []const u8, result: driver_mod.CUresult) driver_mod.Error!void {
    if (result != driver_mod.CUDA_SUCCESS) {
        std.log.err("CUDA context init failed at {s}: {s} ({s})", .{ step, driver.errorName(result), driver.errorString(result) });
        return error.CudaDriverError;
    }
}

pub fn probeDefault() driver_mod.Error!DeviceInfo {
    var ctx = try CudaContext.initDefault();
    defer ctx.deinit();
    return ctx.info;
}
