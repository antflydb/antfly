// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Compiled owner of the inference runtime entry points.

pub const antfly_sources = @import("source_owner_common.zig");

const std = @import("std");

const bridge = @import("runtime_bridge.zig");

const process = @import("runtime_process.zig");

const runtimeEntry = process.runtimeEntry;

const exportInternal = process.exportInternal;

const runtimeAllocator = process.runtimeAllocator;

const inference_runtime = @import("inference_runtime/runtime.zig");

const standalone_inference_host = @import("standalone/inference_host.zig");

const standalone_inference_bridge = @import("standalone/inference_bridge.zig");

fn runInference(init: std.process.Init, _: []const u8, args: *std.process.Args.Iterator) !void {
    return inference_runtime.runFromIterator(init, "antfly", args);
}

fn inferenceEntry(context: *const bridge.Context) callconv(.c) c_int {
    return runtimeEntry(context, "inference", runInference);
}

fn standaloneInferenceCreate(context: *const standalone_inference_bridge.CreateContext) callconv(.c) standalone_inference_bridge.Status {
    if (!standalone_inference_bridge.validContext(
        standalone_inference_bridge.CreateContext,
        context.abi_version,
        context.struct_size,
    ))
        return standalone_inference_bridge.statusFromError(error.UnsupportedVersion);
    context.out_handle.* = standalone_inference_host.linkedInferenceCreate(context) catch |err| {
        return reportStandaloneInferenceFailure("create", err);
    };
    return .ok;
}

fn standaloneInferenceConfigure(context: *const standalone_inference_bridge.ConfigureContext) callconv(.c) standalone_inference_bridge.Status {
    if (!standalone_inference_bridge.validContext(
        standalone_inference_bridge.ConfigureContext,
        context.abi_version,
        context.struct_size,
    ))
        return standalone_inference_bridge.statusFromError(error.UnsupportedVersion);
    standalone_inference_host.linkedInferenceConfigure(context) catch |err| {
        return reportStandaloneInferenceFailure("configure", err);
    };
    return .ok;
}

fn standaloneInferenceInvokeProvider(context: *const standalone_inference_bridge.ProviderInvokeContext) callconv(.c) standalone_inference_bridge.Status {
    if (!standalone_inference_bridge.validContext(
        standalone_inference_bridge.ProviderInvokeContext,
        context.abi_version,
        context.struct_size,
    ))
        return standalone_inference_bridge.statusFromError(error.UnsupportedVersion);
    standalone_inference_host.linkedInferenceInvokeProvider(context) catch |err| {
        return @import("standalone/provider_failure.zig").status(
            context.operation,
            context.request_json.slice(),
            context.has_deadline != 0,
            err,
        );
    };
    return .ok;
}

fn standaloneInferenceDestroyProviderResponse(handle: *anyopaque) callconv(.c) void {
    standalone_inference_host.linkedInferenceDestroyProviderResponse(handle);
}

fn standaloneInferenceRouteManifest(context: *const standalone_inference_bridge.RouteManifestContext) callconv(.c) standalone_inference_bridge.Status {
    if (!standalone_inference_bridge.validContext(
        standalone_inference_bridge.RouteManifestContext,
        context.abi_version,
        context.struct_size,
    ))
        return standalone_inference_bridge.statusFromError(error.UnsupportedVersion);
    standalone_inference_host.linkedInferenceRouteManifest(context) catch |err| {
        return reportStandaloneInferenceFailure("route_manifest", err);
    };
    return .ok;
}

fn standaloneInferenceHandleHttp(context: *const standalone_inference_bridge.HttpHandleContext) callconv(.c) standalone_inference_bridge.Status {
    if (!standalone_inference_bridge.validContext(
        standalone_inference_bridge.HttpHandleContext,
        context.abi_version,
        context.struct_size,
    ))
        return standalone_inference_bridge.statusFromError(error.UnsupportedVersion);
    standalone_inference_host.linkedInferenceHandleHttp(context) catch |err| {
        return reportStandaloneInferenceFailure("handle_http", err);
    };
    return .ok;
}

fn standaloneInferenceDestroyHttpResponse(handle: *anyopaque) callconv(.c) void {
    standalone_inference_host.linkedInferenceDestroyHttpResponse(handle);
}

fn standaloneInferenceTryAcquireRequest(handle: *anyopaque) callconv(.c) u8 {
    return @intFromBool(standalone_inference_host.linkedInferenceTryAcquireRequest(handle));
}

fn standaloneInferenceReleaseRequest(handle: *anyopaque) callconv(.c) void {
    standalone_inference_host.linkedInferenceReleaseRequest(handle);
}

fn standaloneInferenceRequestAdmissionStats(
    handle: *anyopaque,
    out: *standalone_inference_bridge.RequestAdmissionStats,
) callconv(.c) void {
    out.* = standalone_inference_host.linkedInferenceRequestAdmissionStats(handle);
}

fn standaloneInferenceDestroy(handle: *anyopaque) callconv(.c) void {
    standalone_inference_host.linkedInferenceDestroy(handle);
}

const standalone_inference_function_table: standalone_inference_bridge.FunctionTable = .{
    .abi_version = standalone_inference_bridge.abi_version,
    .struct_size = @sizeOf(standalone_inference_bridge.FunctionTable),
    .capabilities = standalone_inference_bridge.Capability.provider |
        standalone_inference_bridge.Capability.route_manifest |
        standalone_inference_bridge.Capability.resource_budget |
        standalone_inference_bridge.Capability.request_admission,
    .create = &standaloneInferenceCreate,
    .configure = &standaloneInferenceConfigure,
    .invoke_provider = &standaloneInferenceInvokeProvider,
    .destroy_provider_response = &standaloneInferenceDestroyProviderResponse,
    .route_manifest = &standaloneInferenceRouteManifest,
    .handle_http = &standaloneInferenceHandleHttp,
    .destroy_http_response = &standaloneInferenceDestroyHttpResponse,
    .try_acquire_request = &standaloneInferenceTryAcquireRequest,
    .release_request = &standaloneInferenceReleaseRequest,
    .request_admission_stats = &standaloneInferenceRequestAdmissionStats,
    .destroy = &standaloneInferenceDestroy,
};

fn standaloneInferenceGetFunctionTable() callconv(.c) *const standalone_inference_bridge.FunctionTable {
    return &standalone_inference_function_table;
}

fn reportStandaloneInferenceFailure(comptime operation: []const u8, err: anyerror) standalone_inference_bridge.Status {
    std.log.err("standalone inference bridge failed operation={s} err={}", .{ operation, err });
    return standalone_inference_bridge.statusFromError(err);
}

comptime {
    exportInternal(&standaloneInferenceGetFunctionTable, "antfly_standalone_inference_get_function_table");
    exportInternal(&inferenceEntry, "antfly_runtime_inference");
}
