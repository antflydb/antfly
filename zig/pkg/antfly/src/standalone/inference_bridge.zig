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

//! Opaque internal ABI between the standalone core and the independently
//! code-generated inference runtime. These are intermediate link boundaries,
//! not a public or stable C API.

const error_abi = @import("../runtime_error_abi.zig");
const http_abi = @import("../runtime_http_abi.zig");

pub const abi_version: u32 = 2;
pub const ai_api_prefix = "/ai/v1";
pub const public_api_prefix = "/ml/v1";
pub const Status = error_abi.Status;
pub const statusFromError = error_abi.statusFromError;
pub const errorFromStatus = error_abi.errorFromStatus;

pub const String = extern struct {
    ptr: [*]const u8,
    len: usize,

    pub fn init(value: []const u8) String {
        return .{ .ptr = value.ptr, .len = value.len };
    }

    pub fn slice(self: String) []const u8 {
        return self.ptr[0..self.len];
    }
};

pub const OptionalString = extern struct {
    ptr: ?[*]const u8 = null,
    len: usize = 0,

    pub fn init(value: ?[]const u8) OptionalString {
        const present = value orelse return .{};
        return .{ .ptr = present.ptr, .len = present.len };
    }

    pub fn slice(self: OptionalString) ?[]const u8 {
        const ptr = self.ptr orelse return null;
        return ptr[0..self.len];
    }
};

pub const WarmModel = extern struct {
    kind: String,
    name: String,
    backend: OptionalString = .{},
    format: OptionalString = .{},
    quantization: OptionalString = .{},
};

pub const CreateContext = extern struct {
    abi_version: u32,
    data_dir_ptr: [*]const u8,
    data_dir_len: usize,
    models_dir: OptionalString,
    ml_dir: OptionalString,
    host_limit_bytes: usize,
    backend_limit_bytes: usize,
    combined_limit_bytes: usize,
    kv_limit_bytes: usize,
    scratch_limit_bytes: usize,
    preload_ptr: ?[*]const WarmModel,
    preload_len: usize,
    keep_alive: OptionalString,
    max_loaded_models: i64,
    has_max_loaded_models: u8,
    content_security_json: OptionalString,
    s3_credentials_json: OptionalString,
    runtime_config_json: String,
    out_handle: *?*anyopaque,
};

pub const ConfigureContext = extern struct {
    abi_version: u32,
    handle: *anyopaque,
    resource_budget: *const ResourceBudget,
};

pub const AdmissionAmounts = extern struct {
    host_weight_bytes: usize,
    backend_weight_bytes: usize,
    host_kv_bytes: usize,
    backend_kv_bytes: usize,
    host_scratch_bytes: usize,
    backend_scratch_bytes: usize,
};

pub const ResourceBudget = extern struct {
    abi_version: u32,
    context: *anyopaque,
    reserve_admission: *const fn (*anyopaque, *const AdmissionAmounts) callconv(.c) Status,
    release_admission: *const fn (*anyopaque, *const AdmissionAmounts) callconv(.c) void,
    observe_prompt_cache: *const fn (*anyopaque, u64, u64) callconv(.c) void,
    reserve_tokenizer_cache: *const fn (*anyopaque, usize) callconv(.c) u8,
    release_tokenizer_cache: *const fn (*anyopaque, usize) callconv(.c) void,
};

/// Stable operation identifiers for the embedded inference service. Requests
/// and responses are UTF-8 JSON owned by the caller and inference unit,
/// respectively. This keeps Zig allocators, error unions, tagged unions, and
/// function signatures out of the archive boundary.
pub const ProviderOperation = enum(c_int) {
    embed_dense_texts = 1,
    embed_dense_texts_with_context = 2,
    embed_sparse_texts = 3,
    embed_dense_parts = 4,
    embed_dense_parts_with_context = 5,
    rerank_texts = 6,
    generate_text = 7,
    generate_messages = 8,
    read_images = 9,
    transcribe_audio = 10,
    extract = 11,
    list_models_json = 12,
};

pub const ProviderInvokeContext = extern struct {
    abi_version: u32,
    handle: *anyopaque,
    operation: c_int,
    request_json: String,
    deadline_ns: u64,
    has_deadline: u8,
    out_response_handle: *?*anyopaque,
    out_response_json: *String,
};

pub const RoutesContext = extern struct {
    abi_version: u32,
    handle: *anyopaque,
    registrar_handle: *anyopaque,
};

pub const RouteContext = extern struct {
    abi_version: u32,
    registrar_handle: *anyopaque,
    route_handle: *anyopaque,
    method: http_abi.HttpMethod,
    path: http_abi.Bytes,
};

pub const HttpHandleContext = extern struct {
    abi_version: u32,
    route_handle: *anyopaque,
    request: *const http_abi.HttpRequestView,
    out_response_handle: *?*anyopaque,
    out_response: *http_abi.HttpResponseView,
};

pub extern fn antfly_standalone_inference_create(context: *const CreateContext) callconv(.c) Status;
pub extern fn antfly_standalone_inference_configure(context: *const ConfigureContext) callconv(.c) Status;
pub extern fn antfly_standalone_inference_invoke_provider(context: *const ProviderInvokeContext) callconv(.c) Status;
pub extern fn antfly_standalone_inference_destroy_provider_response(handle: *anyopaque) callconv(.c) void;
pub extern fn antfly_standalone_inference_register_routes(context: *const RoutesContext) callconv(.c) Status;
pub extern fn antfly_standalone_inference_handle_http(context: *const HttpHandleContext) callconv(.c) Status;
pub extern fn antfly_standalone_inference_destroy_http_response(handle: *anyopaque) callconv(.c) void;
pub extern fn antfly_standalone_inference_destroy(handle: *anyopaque) callconv(.c) void;
