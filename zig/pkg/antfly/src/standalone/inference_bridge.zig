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

pub const CreateContext = extern struct {
    init: *const anyopaque,
    cli: *const anyopaque,
    loaded_config: ?*const anyopaque,
    data_dir_ptr: [*]const u8,
    data_dir_len: usize,
    out_handle: *?*anyopaque,
};

pub const ConfigureContext = extern struct {
    handle: *anyopaque,
    resource_manager: *anyopaque,
    io: ?*const anyopaque,
};

pub const ProviderContext = extern struct {
    handle: *anyopaque,
    out_provider: *anyopaque,
};

pub const RoutesContext = extern struct {
    handle: *anyopaque,
    server: *anyopaque,
};

pub extern fn antfly_standalone_inference_create(context: *const CreateContext) callconv(.c) c_int;
pub extern fn antfly_standalone_inference_configure(context: *const ConfigureContext) callconv(.c) c_int;
pub extern fn antfly_standalone_inference_provider(context: *const ProviderContext) callconv(.c) void;
pub extern fn antfly_standalone_inference_register_routes(context: *const RoutesContext) callconv(.c) c_int;
pub extern fn antfly_standalone_inference_destroy(handle: *anyopaque) callconv(.c) void;
