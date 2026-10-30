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

//! Production-mode conformance probe for the independently code-generated
//! inference archive. This executable deliberately imports only the ABI
//! declarations and resolves the exported function table from the linked
//! archive; it cannot inline or directly call pkg/inference/src/host/host.zig.

const std = @import("std");
const bridge = @import("antfly_inference_bridge");
const http = @import("antfly_runtime_abi").http_abi;

pub fn main() !void {
    const table = bridge.antfly_standalone_inference_get_function_table();
    if (!bridge.validFunctionTable(table, bridge.Capability.provider | bridge.Capability.route_manifest))
        return error.InvalidLinkedInferenceFunctionTable;

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    // The exported wrapper, rather than the host implementation, owns context
    // version validation and stable Status conversion.
    var ignored_handle: ?*anyopaque = null;
    var invalid_create = createContext(&io, &ignored_handle);
    invalid_create.abi_version -= 1;
    const invalid_status = table.create(&invalid_create);
    if (bridge.errorFromStatus(invalid_status) != error.UnsupportedVersion)
        return error.InvalidLinkedInferenceStatusMapping;

    var handle: ?*anyopaque = null;
    const create_context = createContext(&io, &handle);
    const create_status = table.create(&create_context);
    if (!create_status.isOk()) return bridge.errorFromStatus(create_status);
    defer table.destroy(handle.?);
    try checkNativeCheckpointRoutes(table, handle.?);

    const image_bytes = [_]u8{0};
    const payloads = [_]bridge.ProviderBinaryPayload{.{
        .bytes = bridge.String.init(&image_bytes),
        .content_type = bridge.String.init("application/pdf"),
    }};
    const refs = [_]bridge.ProviderAttachmentRef{.{ .attachment_index = 0, .item_index = 0 }};
    const request_json =
        \\{"model":"unused","image_count":1}
    ;
    var response_handle: ?*anyopaque = null;
    var response_json = bridge.String.init("");
    const invoke_context = bridge.ProviderInvokeContext{
        .abi_version = bridge.abi_version,
        .handle = handle.?,
        .operation = @backingInt(bridge.ProviderOperation.read_encoded_images),
        .request_json = bridge.String.init(request_json),
        .deadline_ns = 0,
        .has_deadline = 0,
        .out_response_handle = &response_handle,
        .out_response_json = &response_json,
        .binary_payloads = &payloads,
        .binary_payloads_len = payloads.len,
        .attachment_refs = &refs,
        .attachment_refs_len = refs.len,
    };
    const invoke_status = table.invoke_provider(&invoke_context);
    if (bridge.errorFromStatus(invoke_status) != error.InvalidArguments)
        return error.InvalidLinkedInferenceBinaryContract;
    if (response_handle != null) return error.UnexpectedLinkedInferenceResponse;

    const embedding_request_json =
        \\{"model":"unused","parts":[{"binary":{"mime_type":"image/png","data":[]}}],"attachment_count":2}
    ;
    const embedding_context = bridge.ProviderInvokeContext{
        .abi_version = bridge.abi_version,
        .handle = handle.?,
        .operation = @backingInt(bridge.ProviderOperation.embed_dense_parts),
        .request_json = bridge.String.init(embedding_request_json),
        .deadline_ns = 0,
        .has_deadline = 0,
        .out_response_handle = &response_handle,
        .out_response_json = &response_json,
        .binary_payloads = &payloads,
        .binary_payloads_len = payloads.len,
        .attachment_refs = &refs,
        .attachment_refs_len = refs.len,
    };
    const embedding_status = table.invoke_provider(&embedding_context);
    if (bridge.errorFromStatus(embedding_status) != error.InvalidArguments)
        return error.InvalidLinkedInferenceEmbeddingBinaryContract;
    if (response_handle != null) return error.UnexpectedLinkedInferenceResponse;

    const chunk_request_json =
        \\{"model":"fixed","input":{"binary":{"mime_type":"application/pdf","data":[]}},"config":{"provider":"antfly","model":"fixed"},"attachment_count":2}
    ;
    const chunk_context = bridge.ProviderInvokeContext{
        .abi_version = bridge.abi_version,
        .handle = handle.?,
        .operation = @backingInt(bridge.ProviderOperation.chunk_input),
        .request_json = bridge.String.init(chunk_request_json),
        .deadline_ns = 0,
        .has_deadline = 0,
        .out_response_handle = &response_handle,
        .out_response_json = &response_json,
        .binary_payloads = &payloads,
        .binary_payloads_len = payloads.len,
        .attachment_refs = &refs,
        .attachment_refs_len = refs.len,
    };
    const chunk_status = table.invoke_provider(&chunk_context);
    if (bridge.errorFromStatus(chunk_status) != error.InvalidArguments)
        return error.InvalidLinkedInferenceChunkBinaryContract;
    if (response_handle != null) return error.UnexpectedLinkedInferenceResponse;
}

fn createContext(io: *const std.Io, out_handle: *?*anyopaque) bridge.CreateContext {
    return .{
        .abi_version = bridge.abi_version,
        .data_dir_ptr = ".".ptr,
        .data_dir_len = ".".len,
        .models_dir = bridge.OptionalString.init("."),
        .ml_dir = bridge.OptionalString.init("."),
        .host_limit_bytes = 0,
        .backend_limit_bytes = 0,
        .combined_limit_bytes = 0,
        .kv_limit_bytes = 0,
        .scratch_limit_bytes = 0,
        .process_memory_limit_bytes = std.math.maxInt(usize),
        .preload_ptr = null,
        .preload_len = 0,
        .keep_alive = .{},
        .max_loaded_models = 0,
        .has_max_loaded_models = 0,
        .content_security_json = .{},
        .s3_credentials_json = .{},
        // This probe validates malformed requests at the production archive
        // boundary, not model execution. It has no worker CLI entry point or
        // resource owner; do not launch an isolated model worker recursively.
        .runtime_config_json = bridge.String.init("{\"embedded_enabled\":false}"),
        .executor = .init(io),
        .out_handle = out_handle,
    };
}

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};

fn checkNativeCheckpointRoutes(table: *const bridge.FunctionTable, handle: *anyopaque) !void {
    var entries: ?[*]const bridge.RouteManifestEntry = null;
    var count: usize = 0;
    const status = table.route_manifest(&.{ .abi_version = bridge.abi_version, .handle = handle, .out_entries = &entries, .out_len = &count });
    if (!status.isOk()) return bridge.errorFromStatus(status);
    const manifest = (entries orelse return error.MissingInferenceRoutes)[0..count];
    inline for (.{ bridge.ai_api_prefix, bridge.public_api_prefix }) |prefix| {
        var resume_routes: [2]?bridge.RouteManifestEntry = @splat(null);
        var prepare_seen = false;
        for (manifest) |entry| {
            if (entry.method != .post) continue;
            if (std.mem.eql(u8, entry.path.slice(), prefix ++ "/generate/context/checkpoint")) {
                if (entry.request_body != .buffered or entry.streaming_response == 0) return error.InvalidCheckpointRouteCapabilities;
                prepare_seen = true;
            }
            inline for (.{ "/generate/context/resume", "/generate/context/resume/batch" }, 0..) |suffix, index| {
                if (std.mem.eql(u8, entry.path.slice(), prefix ++ suffix)) {
                    // Uploads are incremental for both endpoints; only the
                    // single continuation can return SSE. Batch returns JSON.
                    if (entry.request_body != .buffered or entry.streaming_response != @intFromBool(index == 0))
                        return error.InvalidCheckpointRouteCapabilities;
                    resume_routes[index] = entry;
                }
            }
        }
        if (!prepare_seen or resume_routes[0] == null or resume_routes[1] == null) return error.MissingCheckpointRoute;
        const Source = struct {
            bytes: [40]u8 = @splat(0),
            offset: usize = 0,
            calls: usize = 0,
            fn read(raw: ?*anyopaque, destination: [*]u8, capacity: usize, out: *usize) callconv(.c) http.CallbackStatus {
                const self: *@This() = @ptrCast(@alignCast(raw orelse return .failed));
                self.calls += 1;
                out.* = @min(capacity, self.bytes.len - self.offset, 3);
                @memcpy(destination[0..out.*], self.bytes[self.offset..][0..out.*]);
                self.offset += out.*;
                return .ok;
            }
        };
        inline for (.{ "/generate/context/resume", "/generate/context/resume/batch" }, 0..) |suffix, index| {
            for ([_]u16{ 400, 413, 400 }) |expected| {
                var source: Source = .{};
                if (expected == 413) {
                    @memcpy(source.bytes[0..8], "AFATT001");
                    std.mem.writeInt(u64, source.bytes[8..16], 2 * 1024 * 1024 + 1, .little);
                    std.mem.writeInt(u32, source.bytes[16..20], 1, .little);
                    std.mem.writeInt(u32, source.bytes[24..28], "application/vnd.antfly.kv-checkpoint".len, .little);
                    std.mem.writeInt(u64, source.bytes[32..40], 192, .little);
                }
                const headers = [_]http.HeaderView{.{ .name = .init("Content-Type"), .value = .init("application/vnd.antfly.attachments.v1") }};
                const request: http.HttpRequestView = .{ .method = .post, .path = .init(prefix ++ suffix), .headers_ptr = &headers, .headers_len = headers.len };
                var response_handle: ?*anyopaque = null;
                var response: http.HttpResponseView = .{ .body = .init("") };
                defer if (response_handle) |owned| table.destroy_http_response(owned);
                const result = table.handle_http(&.{ .abi_version = bridge.abi_version, .route_handle = resume_routes[index].?.route_handle, .request = &request, .body_source = .{ .context = &source, .read = Source.read, .streaming = 1 }, .out_response_handle = &response_handle, .out_response = &response });
                if (!result.isOk()) return bridge.errorFromStatus(result);
                if (response.status != expected or source.offset != 40 or source.calls <= 1)
                    return error.InvalidCheckpointIncrementalAbi;
            }
        }
    }
    var admission: bridge.RequestAdmissionStats = undefined;
    table.request_admission_stats(handle, &admission);
    if (admission.in_flight != 0) return error.LeakedCheckpointAdmission;
}
