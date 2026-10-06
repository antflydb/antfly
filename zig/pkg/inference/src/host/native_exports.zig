// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Native host function table shared by the server and embedded library.
const std = @import("std");
const standalone_inference_host = @import("antfly_inference_host");
const standalone_inference_bridge = @import("antfly_inference_bridge");

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
        return @import("antfly_inference_provider_failure").status(
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

fn standaloneInferencePullModel(context: *const standalone_inference_bridge.PullModelContext) callconv(.c) standalone_inference_bridge.Status {
    if (!standalone_inference_bridge.validContext(
        standalone_inference_bridge.PullModelContext,
        context.abi_version,
        context.struct_size,
    ))
        return standalone_inference_bridge.statusFromError(error.UnsupportedVersion);
    standalone_inference_host.linkedInferencePullModel(context) catch |err| {
        return standalone_inference_bridge.statusFromError(err);
    };
    return .ok;
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
        standalone_inference_bridge.Capability.request_admission |
        standalone_inference_bridge.Capability.model_pull,
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
    .pull_model = &standaloneInferencePullModel,
};

fn standaloneInferenceGetFunctionTable() callconv(.c) *const standalone_inference_bridge.FunctionTable {
    return &standalone_inference_function_table;
}

fn reportStandaloneInferenceFailure(comptime operation: []const u8, err: anyerror) standalone_inference_bridge.Status {
    std.log.err("standalone inference bridge failed operation={s} err={}", .{ operation, err });
    return standalone_inference_bridge.statusFromError(err);
}

comptime {
    @export(&standaloneInferenceGetFunctionTable, .{ .name = "antfly_standalone_inference_get_function_table", .visibility = .hidden });
}
