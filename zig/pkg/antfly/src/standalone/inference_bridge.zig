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

const failure_abi = @import("runtime_failure_abi");

pub const abi_version = failure_abi.abi_version;
pub const Status = failure_abi.Status;
pub const FailureIdentity = failure_abi.FailureIdentity;

/// Append-only stage identity for inference lifecycle failures.
pub const Operation = enum(u32) {
    create = 1,
    configure = 2,
    register_routes = 3,
    embed_dense_texts = 4,
    embed_sparse_texts = 5,
    rerank_texts = 6,
    list_models_json = 7,
    generate_text = 8,
    generate_messages = 9,
    embed_dense_parts = 10,
};

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
    init: *const anyopaque,
    loaded_config: ?*const anyopaque,
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
    out_handle: *?*anyopaque,
    out_failure: *FailureIdentity,
};

pub const ConfigureContext = extern struct {
    handle: *anyopaque,
    resource_manager: *anyopaque,
    io: ?*const anyopaque,
    out_failure: *FailureIdentity,
};

pub const ProviderContext = extern struct {
    handle: *anyopaque,
    out_provider: *anyopaque,
};

pub const CancellationProbeFn = *const fn (?*const anyopaque) callconv(.c) u8;

/// One coarse dense-embedding batch. All strings are borrowed for the
/// synchronous call. Cancellation is queried through a consumer-local C ABI
/// callback rather than passing a Zig atomic or `std.Io` across the boundary.
pub const DenseEmbeddingRequest = extern struct {
    version: u32 = abi_version,
    has_deadline: u8 = 0,
    _reserved0: [3]u8 = @splat(0),
    handle: ?*anyopaque = null,
    model: String,
    texts: ?[*]const String = null,
    text_count: usize = 0,
    deadline_ns: u64 = 0,
    cancellation_ctx: ?*const anyopaque = null,
    cancellation_probe: ?CancellationProbeFn = null,
};

pub const DenseVector = extern struct {
    values: ?[*]const f32 = null,
    value_count: usize = 0,

    pub fn slice(self: DenseVector) []const f32 {
        const ptr = self.values orelse return &.{};
        return ptr[0..self.value_count];
    }
};

/// Provider-owned descriptors and vectors. The consumer copies the result
/// into its own allocator and always calls the matching destroy function.
pub const DenseEmbeddingResult = extern struct {
    version: u32 = abi_version,
    _reserved0: u32 = 0,
    owner: ?*anyopaque = null,
    vectors: ?[*]const DenseVector = null,
    vector_count: usize = 0,
};

pub const SparseVector = extern struct {
    indices: ?[*]const u32 = null,
    values: ?[*]const f32 = null,
    value_count: usize = 0,
};

pub const SparseEmbeddingResult = extern struct {
    version: u32 = abi_version,
    _reserved0: u32 = 0,
    owner: ?*anyopaque = null,
    vectors: ?[*]const SparseVector = null,
    vector_count: usize = 0,
};

pub const RerankRequest = extern struct {
    version: u32 = abi_version,
    _reserved0: u32 = 0,
    handle: ?*anyopaque = null,
    model: String,
    query: String,
    documents: ?[*]const String = null,
    document_count: usize = 0,
};

pub const FloatResult = extern struct {
    version: u32 = abi_version,
    _reserved0: u32 = 0,
    owner: ?*anyopaque = null,
    values: ?[*]const f32 = null,
    value_count: usize = 0,
};

pub const HandleRequest = extern struct {
    version: u32 = abi_version,
    _reserved0: u32 = 0,
    handle: ?*anyopaque = null,
};

pub const BytesResult = extern struct {
    version: u32 = abi_version,
    _reserved0: u32 = 0,
    owner: ?*anyopaque = null,
    bytes: ?[*]const u8 = null,
    byte_count: usize = 0,
};

pub const GenerateTextRequest = extern struct {
    version: u32 = abi_version,
    _reserved0: u32 = 0,
    handle: ?*anyopaque = null,
    model: String,
    roles: ?[*]const String = null,
    contents: ?[*]const String = null,
    message_count: usize = 0,
};

pub const JsonOperationRequest = extern struct {
    version: u32 = abi_version,
    _reserved0: u32 = 0,
    handle: ?*anyopaque = null,
    model: String,
    payload_json: String,
};

pub const ContentPartTag = enum(u32) {
    text = 1,
    media_url = 2,
    binary = 3,
};

pub const ContentPart = extern struct {
    tag: ContentPartTag,
    _reserved0: u32 = 0,
    value: String,
    mime_type: String,
};

pub const DensePartsRequest = extern struct {
    version: u32 = abi_version,
    has_deadline: u8 = 0,
    _reserved0: [3]u8 = @splat(0),
    handle: ?*anyopaque = null,
    model: String,
    parts: ?[*]const ContentPart = null,
    part_count: usize = 0,
    deadline_ns: u64 = 0,
    cancellation_ctx: ?*const anyopaque = null,
    cancellation_probe: ?CancellationProbeFn = null,
};

pub const RoutesContext = extern struct {
    handle: *anyopaque,
    server: *anyopaque,
    out_failure: *FailureIdentity,
};

pub extern fn antfly_standalone_inference_create(context: *const CreateContext) callconv(.c) Status;
pub extern fn antfly_standalone_inference_configure(context: *const ConfigureContext) callconv(.c) Status;
pub extern fn antfly_standalone_inference_provider(context: *const ProviderContext) callconv(.c) void;
pub extern fn antfly_standalone_inference_embed_dense(
    request: *const DenseEmbeddingRequest,
    out_result: *DenseEmbeddingResult,
    out_failure: *FailureIdentity,
) callconv(.c) Status;
pub extern fn antfly_standalone_inference_dense_result_destroy(
    result: *DenseEmbeddingResult,
) callconv(.c) void;
pub extern fn antfly_standalone_inference_embed_sparse(
    request: *const DenseEmbeddingRequest,
    out_result: *SparseEmbeddingResult,
    out_failure: *FailureIdentity,
) callconv(.c) Status;
pub extern fn antfly_standalone_inference_sparse_result_destroy(
    result: *SparseEmbeddingResult,
) callconv(.c) void;
pub extern fn antfly_standalone_inference_rerank(
    request: *const RerankRequest,
    out_result: *FloatResult,
    out_failure: *FailureIdentity,
) callconv(.c) Status;
pub extern fn antfly_standalone_inference_float_result_destroy(
    result: *FloatResult,
) callconv(.c) void;
pub extern fn antfly_standalone_inference_list_models(
    request: *const HandleRequest,
    out_result: *BytesResult,
    out_failure: *FailureIdentity,
) callconv(.c) Status;
pub extern fn antfly_standalone_inference_bytes_result_destroy(
    result: *BytesResult,
) callconv(.c) void;
pub extern fn antfly_standalone_inference_generate_text(
    request: *const GenerateTextRequest,
    out_result: *BytesResult,
    out_failure: *FailureIdentity,
) callconv(.c) Status;
pub extern fn antfly_standalone_inference_generate_messages(
    request: *const JsonOperationRequest,
    out_result: *BytesResult,
    out_failure: *FailureIdentity,
) callconv(.c) Status;
pub extern fn antfly_standalone_inference_embed_dense_parts(
    request: *const DensePartsRequest,
    out_result: *DenseEmbeddingResult,
    out_failure: *FailureIdentity,
) callconv(.c) Status;
pub extern fn antfly_standalone_inference_register_routes(context: *const RoutesContext) callconv(.c) Status;
pub extern fn antfly_standalone_inference_destroy(handle: *anyopaque) callconv(.c) void;
