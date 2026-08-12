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

pub const RoutesContext = extern struct {
    handle: *anyopaque,
    server: *anyopaque,
    out_failure: *FailureIdentity,
};

pub extern fn antfly_standalone_inference_create(context: *const CreateContext) callconv(.c) Status;
pub extern fn antfly_standalone_inference_configure(context: *const ConfigureContext) callconv(.c) Status;
pub extern fn antfly_standalone_inference_provider(context: *const ProviderContext) callconv(.c) void;
pub extern fn antfly_standalone_inference_register_routes(context: *const RoutesContext) callconv(.c) Status;
pub extern fn antfly_standalone_inference_destroy(handle: *anyopaque) callconv(.c) void;
