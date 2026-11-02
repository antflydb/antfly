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

//! Whole native lake builds. Only catalog control callbacks cross owners;
//! row scans, writers, artifact clients and cleanup belong to the provider.
const std = @import("std");
const local = @import("antfly_local_sources");
pub const native = @import("antfly_runtime_abi").native_abi;
pub const values = @import("lake_execution_values.zig");
pub const BuildLimits = @import("../serverless/build/lake_build_limits.zig").Limits;
pub const SourceOptions = @import("../serverless/configured_object_store_support.zig").BindingObjectStoreOpenOptions;
const catalog = local.metadata_lake_index_catalog;
const A = std.mem.Allocator;
const Embedder = local.inference_managed_embedder;
const execution = @import("antfly_inference_execution_context");
pub const LimitsRegistry = @typeInfo(@FieldType(Embedder.ProviderRuntime, "limits")).pointer.child;
var error_domain: u8 = 0;
/// Foreign error integers are opaque. Only the originating compilation unit
/// decodes its token; every other receiver uses the error name.
pub const Failure = extern struct {
    ptr: ?[*]const u8 = null,
    len: usize = 0,
    origin: ?*const anyopaque = null,
    token: usize = 0,
    pub fn fromError(err: anyerror) Failure {
        const name = @errorName(err);
        return .{ .ptr = name.ptr, .len = name.len, .origin = &error_domain, .token = @intFromError(err) };
    }
    pub fn decode(self: Failure) ?anyerror {
        const ptr = self.ptr orelse return null;
        if (self.origin == @as(?*const anyopaque, &error_domain)) return @errorFromInt(@as(u16, @intCast(self.token)));
        return @import("lake_execution_errors.zig").fromName(ptr[0..self.len]);
    }
};
pub fn Outcome(comptime T: type) type {
    return struct { value: T = undefined, failure: Failure = .{} };
}
pub const Control = struct {
    ptr: *anyopaque,
    check: *const fn (*anyopaque) Outcome(void),
    now: *const fn (*anyopaque) Outcome(u64),
    snapshot: *const fn (*anyopaque, A) Outcome(local.common_topology_records.TableRecord),
    save_checkpoint: *const fn (*anyopaque, catalog.Publication) Outcome(void),
    discard_checkpoint: *const fn (*anyopaque) Outcome(void),
};
pub const Request = struct {
    allocator: A,
    table: local.common_topology_records.TableRecord,
    store: values.StoreDescriptor,
    source: SourceOptions,
    io: native.IoBorrow,
    cancellation: @import("antfly_cancellation").CancellationToken,
    control: Control,
    embedding_options: ?Embedder.InitOptions,
    /// Preserve process-wide quotas while recreating owner-local HTTP pools.
    provider_limits: ?*LimitsRegistry = null,
    policy: values.BuildPolicy,
    build_limits: BuildLimits,
};
pub const Response = struct {
    /// Uses Request.allocator; the caller frees this after publication CAS.
    bytes: ?[]u8 = null,
    failure: Failure = .{},
    /// Distinguishes a missing/corrupt derived artifact from a source failure.
    artifact_cause: Failure = .{},
};
const Function = *const fn (Request) Response;
pub fn signature() native.CallContract {
    @setEvalBranchQuota(100_000);
    return .of("antfly_lake_execute_v2", Function, Request, Response);
}
const layout_types = .{ Failure, Control, Request, Response, values.StoreDescriptor, SourceOptions, values.BuildPolicy, BuildLimits, @import("antfly_cancellation").CancellationToken, execution.ProgressSink, execution.Progress, execution.Phase, Outcome(void), Outcome(u64), Outcome(local.common_topology_records.TableRecord), A.VTable, local.common_topology_records.TableRecord, catalog.Publication, catalog.StoreLocator, catalog.StoreLocator.Protocol, local.common_config.Config, local.common_secrets.FileStore, Embedder.InitOptions, Embedder.AntflyProvider, LimitsRegistry, @typeInfo(@FieldType(SourceOptions, "retained_catalog_metadata")).optional.child, @typeInfo(@FieldType(values.StoreDescriptor, "cloud_vacuum")).optional.child };
pub const layouts = blk: {
    @setEvalBranchQuota(100_000);
    var result: [layout_types.len]native.TypeContract = undefined;
    for (layout_types, 0..) |T, i| result[i] = .of(T);
    break :blk result;
};
pub const endpoint_version: u32 = 2;
pub const Endpoint = extern struct {
    version: u32 = endpoint_version,
    contract: native.CallContract,
    layout_count: usize = layouts.len,
    layout: *const [layouts.len]native.TypeContract = &layouts,
    invoke: *const fn (*const native.CallContract, *const anyopaque, *anyopaque) callconv(.c) void,
};
pub fn validate(endpoint: *const Endpoint) !void {
    if (endpoint.version != endpoint_version or !endpoint.contract.matches(signature())) return error.InvalidArgument;
    if (endpoint.layout_count != layouts.len) return error.InvalidArgument;
    for (endpoint.layout.*, layouts) |actual, expected| if (!actual.matches(expected)) return error.InvalidArgument;
}
pub fn execute(request: Request) !Response {
    const get = @extern(*const fn () callconv(.c) *const Endpoint, .{ .name = "antfly_lake_execute_v2" });
    const endpoint = get();
    try validate(endpoint);
    var response: Response = undefined;
    endpoint.invoke(&signature(), &request, &response);
    return response;
}
