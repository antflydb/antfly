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

pub const CUresult = c_int;
pub const CUdevice = c_int;
pub const CUdeviceptr = u64;
pub const CUcontext = ?*anyopaque;
pub const CUstream = ?*anyopaque;
pub const CUmodule = ?*anyopaque;
pub const CUfunction = ?*anyopaque;
pub const CUgraph = ?*anyopaque;
pub const CUgraphExec = ?*anyopaque;
pub const CUgraphNode = ?*anyopaque;
pub const CUevent = ?*anyopaque;
pub const CUjit_option = c_uint;
pub const CUstreamCaptureMode = c_uint;
pub const CUgraphExecUpdateResult = c_uint;
pub const CUgraphInstantiateResult = c_uint;
pub const CUDA_SUCCESS: CUresult = 0;
pub const CU_EVENT_DEFAULT: c_uint = 0;
pub const CU_EVENT_DISABLE_TIMING: c_uint = 2;

pub const CU_STREAM_CAPTURE_MODE_GLOBAL: CUstreamCaptureMode = 0;
pub const CU_STREAM_CAPTURE_MODE_THREAD_LOCAL: CUstreamCaptureMode = 1;
pub const CU_STREAM_CAPTURE_MODE_RELAXED: CUstreamCaptureMode = 2;

pub const CU_GRAPH_EXEC_UPDATE_SUCCESS: CUgraphExecUpdateResult = 0;
pub const CU_GRAPH_EXEC_UPDATE_ERROR: CUgraphExecUpdateResult = 1;
pub const CU_GRAPH_EXEC_UPDATE_ERROR_TOPOLOGY_CHANGED: CUgraphExecUpdateResult = 2;
pub const CU_GRAPH_EXEC_UPDATE_ERROR_NODE_TYPE_CHANGED: CUgraphExecUpdateResult = 3;
pub const CU_GRAPH_EXEC_UPDATE_ERROR_FUNCTION_CHANGED: CUgraphExecUpdateResult = 4;
pub const CU_GRAPH_EXEC_UPDATE_ERROR_PARAMETERS_CHANGED: CUgraphExecUpdateResult = 5;
pub const CU_GRAPH_EXEC_UPDATE_ERROR_NOT_SUPPORTED: CUgraphExecUpdateResult = 6;
pub const CU_GRAPH_EXEC_UPDATE_ERROR_UNSUPPORTED_FUNCTION_CHANGE: CUgraphExecUpdateResult = 7;
pub const CU_GRAPH_EXEC_UPDATE_ERROR_ATTRIBUTES_CHANGED: CUgraphExecUpdateResult = 8;

pub const CUDA_GRAPH_INSTANTIATE_SUCCESS: CUgraphInstantiateResult = 0;
pub const CUDA_GRAPH_INSTANTIATE_ERROR: CUgraphInstantiateResult = 1;
pub const CUDA_GRAPH_INSTANTIATE_INVALID_STRUCTURE: CUgraphInstantiateResult = 2;
pub const CUDA_GRAPH_INSTANTIATE_NODE_OPERATION_NOT_SUPPORTED: CUgraphInstantiateResult = 3;
pub const CUDA_GRAPH_INSTANTIATE_MULTIPLE_CTXS_NOT_SUPPORTED: CUgraphInstantiateResult = 4;
pub const CUDA_GRAPH_INSTANTIATE_CONDITIONAL_HANDLE_UNUSED: CUgraphInstantiateResult = 5;

pub const CUgraphExecUpdateResultInfo = extern struct {
    result: CUgraphExecUpdateResult = CU_GRAPH_EXEC_UPDATE_ERROR,
    errorNode: CUgraphNode = null,
    errorFromNode: CUgraphNode = null,
};

pub const CUgraphExecUpdateFn = *const fn (
    hGraphExec: CUgraphExec,
    hGraph: CUgraph,
    resultInfo: *CUgraphExecUpdateResultInfo,
) callconv(.c) CUresult;

pub const CUDA_GRAPH_INSTANTIATE_PARAMS = extern struct {
    flags: c_ulonglong = 0,
    hUploadStream: CUstream = null,
    hErrNode_out: CUgraphNode = null,
    result_out: CUgraphInstantiateResult = CUDA_GRAPH_INSTANTIATE_SUCCESS,
};

pub const CU_JIT_INFO_LOG_BUFFER: CUjit_option = 3;
pub const CU_JIT_INFO_LOG_BUFFER_SIZE_BYTES: CUjit_option = 4;
pub const CU_JIT_ERROR_LOG_BUFFER: CUjit_option = 5;
pub const CU_JIT_ERROR_LOG_BUFFER_SIZE_BYTES: CUjit_option = 6;

pub const Error = error{
    CudaUnavailable,
    CudaSymbolMissing,
    CudaDriverError,
    CudaKernelUnavailable,
    NoCudaDevices,
    InvalidCudaState,
};

pub const CudaDriver = struct {
    lib: std.DynLib,
    fns: Table,

    pub const Table = struct {
        cuInit: *const fn (flags: c_uint) callconv(.c) CUresult,
        cuDriverGetVersion: *const fn (driverVersion: *c_int) callconv(.c) CUresult,
        cuDeviceGetCount: *const fn (count: *c_int) callconv(.c) CUresult,
        cuDeviceGet: *const fn (device: *CUdevice, ordinal: c_int) callconv(.c) CUresult,
        cuDeviceGetName: *const fn (name: [*]u8, len: c_int, dev: CUdevice) callconv(.c) CUresult,
        cuDeviceComputeCapability: *const fn (major: *c_int, minor: *c_int, dev: CUdevice) callconv(.c) CUresult,
        cuDevicePrimaryCtxRetain: *const fn (pctx: *CUcontext, dev: CUdevice) callconv(.c) CUresult,
        cuDevicePrimaryCtxRelease: *const fn (dev: CUdevice) callconv(.c) CUresult,
        cuCtxSetCurrent: *const fn (ctx: CUcontext) callconv(.c) CUresult,
        cuStreamCreate: *const fn (phStream: *CUstream, flags: c_uint) callconv(.c) CUresult,
        cuStreamBeginCapture: *const fn (hStream: CUstream, mode: CUstreamCaptureMode) callconv(.c) CUresult,
        cuStreamEndCapture: *const fn (hStream: CUstream, phGraph: *CUgraph) callconv(.c) CUresult,
        cuStreamSynchronize: *const fn (hStream: CUstream) callconv(.c) CUresult,
        cuStreamWaitEvent: *const fn (hStream: CUstream, hEvent: CUevent, flags: c_uint) callconv(.c) CUresult,
        cuStreamDestroy: *const fn (hStream: CUstream) callconv(.c) CUresult,
        cuEventCreate: *const fn (phEvent: *CUevent, flags: c_uint) callconv(.c) CUresult,
        cuEventRecord: *const fn (hEvent: CUevent, hStream: CUstream) callconv(.c) CUresult,
        cuEventSynchronize: *const fn (hEvent: CUevent) callconv(.c) CUresult,
        cuEventElapsedTime: *const fn (pMilliseconds: *f32, hStart: CUevent, hEnd: CUevent) callconv(.c) CUresult,
        cuEventDestroy: *const fn (hEvent: CUevent) callconv(.c) CUresult,
        cuMemAlloc: *const fn (dptr: *CUdeviceptr, bytesize: usize) callconv(.c) CUresult,
        cuMemFree: *const fn (dptr: CUdeviceptr) callconv(.c) CUresult,
        cuMemAllocHost: ?*const fn (pp: *?*anyopaque, bytesize: usize) callconv(.c) CUresult,
        cuMemFreeHost: ?*const fn (p: ?*anyopaque) callconv(.c) CUresult,
        cuMemcpyHtoDAsync: *const fn (dstDevice: CUdeviceptr, srcHost: ?*const anyopaque, ByteCount: usize, hStream: CUstream) callconv(.c) CUresult,
        cuMemcpyDtoHAsync: *const fn (dstHost: ?*anyopaque, srcDevice: CUdeviceptr, ByteCount: usize, hStream: CUstream) callconv(.c) CUresult,
        cuMemcpyDtoDAsync: *const fn (dstDevice: CUdeviceptr, srcDevice: CUdeviceptr, ByteCount: usize, hStream: CUstream) callconv(.c) CUresult,
        cuModuleLoadDataEx: *const fn (module: *CUmodule, image: ?*const anyopaque, numOptions: c_uint, options: ?[*]CUjit_option, optionValues: ?[*]?*anyopaque) callconv(.c) CUresult,
        cuModuleUnload: *const fn (hmod: CUmodule) callconv(.c) CUresult,
        cuModuleGetFunction: *const fn (hfunc: *CUfunction, hmod: CUmodule, name: [*:0]const u8) callconv(.c) CUresult,
        cuFuncSetAttribute: ?*const fn (hfunc: CUfunction, attrib: c_int, value: c_int) callconv(.c) CUresult,
        cuLaunchKernel: *const fn (
            f: CUfunction,
            gridDimX: c_uint,
            gridDimY: c_uint,
            gridDimZ: c_uint,
            blockDimX: c_uint,
            blockDimY: c_uint,
            blockDimZ: c_uint,
            sharedMemBytes: c_uint,
            hStream: CUstream,
            kernelParams: ?[*]?*anyopaque,
            extra: ?[*]?*anyopaque,
        ) callconv(.c) CUresult,
        cuGraphInstantiate: *const fn (phGraphExec: *CUgraphExec, hGraph: CUgraph, flags: c_ulonglong) callconv(.c) CUresult,
        cuGraphInstantiateWithParams: ?*const fn (phGraphExec: *CUgraphExec, hGraph: CUgraph, instantiateParams: *CUDA_GRAPH_INSTANTIATE_PARAMS) callconv(.c) CUresult,
        cuGraphExecUpdate: ?CUgraphExecUpdateFn,
        cuGraphLaunch: *const fn (hGraphExec: CUgraphExec, hStream: CUstream) callconv(.c) CUresult,
        cuGraphExecDestroy: *const fn (hGraphExec: CUgraphExec) callconv(.c) CUresult,
        cuGraphDestroy: *const fn (hGraph: CUgraph) callconv(.c) CUresult,
        cuGetErrorName: *const fn (error_: CUresult, pStr: *?[*:0]const u8) callconv(.c) CUresult,
        cuGetErrorString: *const fn (error_: CUresult, pStr: *?[*:0]const u8) callconv(.c) CUresult,
    };

    pub fn open() Error!CudaDriver {
        var lib = std.DynLib.open("libcuda.so.1") catch return error.CudaUnavailable;
        errdefer lib.close();
        return .{
            .lib = lib,
            .fns = .{
                .cuInit = lookup(&lib, @TypeOf(@as(Table, undefined).cuInit), "cuInit") catch return error.CudaSymbolMissing,
                .cuDriverGetVersion = lookup(&lib, @TypeOf(@as(Table, undefined).cuDriverGetVersion), "cuDriverGetVersion") catch return error.CudaSymbolMissing,
                .cuDeviceGetCount = lookup(&lib, @TypeOf(@as(Table, undefined).cuDeviceGetCount), "cuDeviceGetCount") catch return error.CudaSymbolMissing,
                .cuDeviceGet = lookup(&lib, @TypeOf(@as(Table, undefined).cuDeviceGet), "cuDeviceGet") catch return error.CudaSymbolMissing,
                .cuDeviceGetName = lookup(&lib, @TypeOf(@as(Table, undefined).cuDeviceGetName), "cuDeviceGetName") catch return error.CudaSymbolMissing,
                .cuDeviceComputeCapability = lookup(&lib, @TypeOf(@as(Table, undefined).cuDeviceComputeCapability), "cuDeviceComputeCapability") catch return error.CudaSymbolMissing,
                .cuDevicePrimaryCtxRetain = lookup(&lib, @TypeOf(@as(Table, undefined).cuDevicePrimaryCtxRetain), "cuDevicePrimaryCtxRetain") catch return error.CudaSymbolMissing,
                .cuDevicePrimaryCtxRelease = lookup(&lib, @TypeOf(@as(Table, undefined).cuDevicePrimaryCtxRelease), "cuDevicePrimaryCtxRelease") catch return error.CudaSymbolMissing,
                .cuCtxSetCurrent = lookup(&lib, @TypeOf(@as(Table, undefined).cuCtxSetCurrent), "cuCtxSetCurrent") catch return error.CudaSymbolMissing,
                .cuStreamCreate = lookup(&lib, @TypeOf(@as(Table, undefined).cuStreamCreate), "cuStreamCreate") catch return error.CudaSymbolMissing,
                .cuStreamBeginCapture = lookup(&lib, @TypeOf(@as(Table, undefined).cuStreamBeginCapture), "cuStreamBeginCapture_v2") catch return error.CudaSymbolMissing,
                .cuStreamEndCapture = lookup(&lib, @TypeOf(@as(Table, undefined).cuStreamEndCapture), "cuStreamEndCapture") catch return error.CudaSymbolMissing,
                .cuStreamSynchronize = lookup(&lib, @TypeOf(@as(Table, undefined).cuStreamSynchronize), "cuStreamSynchronize") catch return error.CudaSymbolMissing,
                .cuStreamWaitEvent = lookup(&lib, @TypeOf(@as(Table, undefined).cuStreamWaitEvent), "cuStreamWaitEvent") catch return error.CudaSymbolMissing,
                .cuStreamDestroy = lookup(&lib, @TypeOf(@as(Table, undefined).cuStreamDestroy), "cuStreamDestroy") catch return error.CudaSymbolMissing,
                .cuEventCreate = lookup(&lib, @TypeOf(@as(Table, undefined).cuEventCreate), "cuEventCreate") catch return error.CudaSymbolMissing,
                .cuEventRecord = lookup(&lib, @TypeOf(@as(Table, undefined).cuEventRecord), "cuEventRecord") catch return error.CudaSymbolMissing,
                .cuEventSynchronize = lookup(&lib, @TypeOf(@as(Table, undefined).cuEventSynchronize), "cuEventSynchronize") catch return error.CudaSymbolMissing,
                .cuEventElapsedTime = lookup(&lib, @TypeOf(@as(Table, undefined).cuEventElapsedTime), "cuEventElapsedTime") catch return error.CudaSymbolMissing,
                .cuEventDestroy = lookup(&lib, @TypeOf(@as(Table, undefined).cuEventDestroy), "cuEventDestroy") catch return error.CudaSymbolMissing,
                .cuMemAlloc = lookup(&lib, @TypeOf(@as(Table, undefined).cuMemAlloc), "cuMemAlloc_v2") catch return error.CudaSymbolMissing,
                .cuMemFree = lookup(&lib, @TypeOf(@as(Table, undefined).cuMemFree), "cuMemFree_v2") catch return error.CudaSymbolMissing,
                .cuMemAllocHost = lookupOptional(&lib, *const fn (pp: *?*anyopaque, bytesize: usize) callconv(.c) CUresult, "cuMemAllocHost_v2"),
                .cuMemFreeHost = lookupOptional(&lib, *const fn (p: ?*anyopaque) callconv(.c) CUresult, "cuMemFreeHost"),
                .cuMemcpyHtoDAsync = lookup(&lib, @TypeOf(@as(Table, undefined).cuMemcpyHtoDAsync), "cuMemcpyHtoDAsync_v2") catch return error.CudaSymbolMissing,
                .cuMemcpyDtoHAsync = lookup(&lib, @TypeOf(@as(Table, undefined).cuMemcpyDtoHAsync), "cuMemcpyDtoHAsync_v2") catch return error.CudaSymbolMissing,
                .cuMemcpyDtoDAsync = lookup(&lib, @TypeOf(@as(Table, undefined).cuMemcpyDtoDAsync), "cuMemcpyDtoDAsync_v2") catch return error.CudaSymbolMissing,
                .cuModuleLoadDataEx = lookup(&lib, @TypeOf(@as(Table, undefined).cuModuleLoadDataEx), "cuModuleLoadDataEx") catch return error.CudaSymbolMissing,
                .cuModuleUnload = lookup(&lib, @TypeOf(@as(Table, undefined).cuModuleUnload), "cuModuleUnload") catch return error.CudaSymbolMissing,
                .cuModuleGetFunction = lookup(&lib, @TypeOf(@as(Table, undefined).cuModuleGetFunction), "cuModuleGetFunction") catch return error.CudaSymbolMissing,
                .cuFuncSetAttribute = lookupOptional(&lib, *const fn (hfunc: CUfunction, attrib: c_int, value: c_int) callconv(.c) CUresult, "cuFuncSetAttribute"),
                .cuLaunchKernel = lookup(&lib, @TypeOf(@as(Table, undefined).cuLaunchKernel), "cuLaunchKernel") catch return error.CudaSymbolMissing,
                .cuGraphInstantiate = lookup(&lib, @TypeOf(@as(Table, undefined).cuGraphInstantiate), "cuGraphInstantiateWithFlags") catch return error.CudaSymbolMissing,
                .cuGraphInstantiateWithParams = lookupOptional(&lib, *const fn (phGraphExec: *CUgraphExec, hGraph: CUgraph, instantiateParams: *CUDA_GRAPH_INSTANTIATE_PARAMS) callconv(.c) CUresult, "cuGraphInstantiateWithParams"),
                .cuGraphExecUpdate = lookupGraphExecUpdate(&lib),
                .cuGraphLaunch = lookup(&lib, @TypeOf(@as(Table, undefined).cuGraphLaunch), "cuGraphLaunch") catch return error.CudaSymbolMissing,
                .cuGraphExecDestroy = lookup(&lib, @TypeOf(@as(Table, undefined).cuGraphExecDestroy), "cuGraphExecDestroy") catch return error.CudaSymbolMissing,
                .cuGraphDestroy = lookup(&lib, @TypeOf(@as(Table, undefined).cuGraphDestroy), "cuGraphDestroy") catch return error.CudaSymbolMissing,
                .cuGetErrorName = lookup(&lib, @TypeOf(@as(Table, undefined).cuGetErrorName), "cuGetErrorName") catch return error.CudaSymbolMissing,
                .cuGetErrorString = lookup(&lib, @TypeOf(@as(Table, undefined).cuGetErrorString), "cuGetErrorString") catch return error.CudaSymbolMissing,
            },
        };
    }

    pub fn deinit(self: *CudaDriver) void {
        self.lib.close();
    }

    pub fn check(self: *const CudaDriver, result: CUresult) Error!void {
        if (result != CUDA_SUCCESS) {
            std.log.err("CUDA driver call failed: {s} ({s})", .{ self.errorName(result), self.errorString(result) });
            return error.CudaDriverError;
        }
    }

    pub fn errorName(self: *const CudaDriver, result: CUresult) []const u8 {
        var raw: ?[*:0]const u8 = null;
        if (self.fns.cuGetErrorName(result, &raw) == CUDA_SUCCESS) {
            if (raw) |ptr| return std.mem.span(ptr);
        }
        return "CUDA_ERROR_UNKNOWN";
    }

    pub fn errorString(self: *const CudaDriver, result: CUresult) []const u8 {
        var raw: ?[*:0]const u8 = null;
        if (self.fns.cuGetErrorString(result, &raw) == CUDA_SUCCESS) {
            if (raw) |ptr| return std.mem.span(ptr);
        }
        return "";
    }
};

fn lookup(lib: *std.DynLib, comptime T: type, name: [:0]const u8) Error!T {
    return lib.lookup(T, name) orelse error.CudaSymbolMissing;
}

fn lookupOptional(lib: *std.DynLib, comptime T: type, name: [:0]const u8) ?T {
    return lib.lookup(T, name);
}

// Some CUDA 12 drivers expose the unversioned dlsym entry with the legacy
// four-argument ABI. Prefer the explicit three-argument symbol we bind here.
fn lookupGraphExecUpdate(lib: *std.DynLib) ?CUgraphExecUpdateFn {
    return lookupOptional(lib, CUgraphExecUpdateFn, "cuGraphExecUpdate_v2") orelse
        lookupOptional(lib, CUgraphExecUpdateFn, "cuGraphExecUpdate");
}

test "cuda driver unavailable probe does not crash" {
    var driver = CudaDriver.open() catch |err| {
        try std.testing.expect(err == error.CudaUnavailable or err == error.CudaSymbolMissing);
        return;
    };
    defer driver.deinit();
}
