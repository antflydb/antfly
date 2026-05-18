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

//! PJRT C API type definitions for Zig.
//!
//! Matches pjrt_c_api.h ABI layout (major version 0). PJRT plugins
//! are loaded via dlopen; this module defines the types needed to
//! call through the PJRT_Api function pointer table.

const std = @import("std");

// ── Opaque handle types (forward declarations in pjrt_c_api.h) ─────

pub const PjrtClient = opaque {};
pub const PjrtDevice = opaque {};
pub const PjrtBuffer = opaque {};
pub const PjrtLoadedExecutable = opaque {};
pub const PjrtExecutable = opaque {};
pub const PjrtSerializedExecutable = opaque {};
pub const PjrtEvent = opaque {};
pub const PjrtError = opaque {};
pub const PjrtMemory = opaque {};

// ── Enums ──────────────────────────────────────────────────────────

/// PJRT_Buffer_Type element types (values match C enum).
pub const BufferType = enum(c_uint) {
    invalid = 0,
    pred = 1,
    int8 = 2,
    int16 = 3,
    int32 = 4,
    int64 = 5,
    uint8 = 6,
    uint16 = 7,
    uint32 = 8,
    uint64 = 9,
    float16 = 10,
    float32 = 11,
    float64 = 12,
    bfloat16 = 13,
    complex64 = 14,
    complex128 = 15,
    _,
};

/// PJRT_HostBufferSemantics — how host memory is handled during transfer.
pub const HostBufferSemantics = enum(c_uint) {
    immutable_only_during_call = 0,
    immutable_until_transfer_completes = 1,
    immutable_zero_copy = 2,
    mutable_zero_copy = 3,
};

// ── Argument structs ───────────────────────────────────────────────

pub const ErrorDestroyArgs = extern struct {
    struct_size: usize,
    extension_start: ?*anyopaque,
    err: ?*PjrtError,
};

pub const ErrorMessageArgs = extern struct {
    struct_size: usize,
    extension_start: ?*anyopaque,
    err: ?*const PjrtError,
    message: ?[*]const u8, // out
    message_size: usize, // out
};

pub const PluginInitializeArgs = extern struct {
    struct_size: usize,
    extension_start: ?*anyopaque,
};

pub const ClientCreateArgs = extern struct {
    struct_size: usize,
    extension_start: ?*anyopaque,
    create_options: ?*const anyopaque,
    num_options: usize,
    kv_get_callback: ?*const anyopaque,
    kv_get_user_arg: ?*anyopaque,
    kv_put_callback: ?*const anyopaque,
    kv_put_user_arg: ?*anyopaque,
    client: ?*PjrtClient, // out
    kv_try_get_callback: ?*const anyopaque,
    kv_try_get_user_arg: ?*anyopaque,
};

pub const ClientDestroyArgs = extern struct {
    struct_size: usize,
    extension_start: ?*anyopaque,
    client: ?*PjrtClient,
};

pub const ClientAddressableDevicesArgs = extern struct {
    struct_size: usize,
    extension_start: ?*anyopaque,
    client: ?*PjrtClient,
    addressable_devices: ?[*]?*PjrtDevice, // out
    num_addressable_devices: usize, // out
};

pub const ClientCompileArgs = extern struct {
    struct_size: usize,
    extension_start: ?*anyopaque,
    client: ?*PjrtClient,
    program: ?*const Program,
    compile_options: ?[*]const u8,
    compile_options_size: usize,
    executable: ?*PjrtLoadedExecutable, // out
};

pub const ClientBufferFromHostBufferArgs = extern struct {
    struct_size: usize,
    extension_start: ?*anyopaque,
    client: ?*PjrtClient,
    data: ?*const anyopaque,
    buffer_type: BufferType,
    // 4 bytes auto-padding (C ABI aligns next pointer to 8)
    dims: ?[*]const i64,
    num_dims: usize,
    byte_strides: ?[*]const i64,
    num_byte_strides: usize,
    host_buffer_semantics: HostBufferSemantics,
    // 4 bytes auto-padding
    device: ?*PjrtDevice,
    memory: ?*PjrtMemory,
    device_layout: ?*anyopaque,
    done_with_host_buffer: ?*PjrtEvent, // out
    buffer: ?*PjrtBuffer, // out
};

pub const LoadedExecutableDestroyArgs = extern struct {
    struct_size: usize,
    extension_start: ?*anyopaque,
    executable: ?*PjrtLoadedExecutable,
};

pub const ExecutableSerializeArgs = extern struct {
    struct_size: usize,
    extension_start: ?*anyopaque,
    executable: ?*PjrtExecutable,
    serialized_bytes: ?[*]const u8, // out
    serialized_bytes_size: usize, // out
    serialized_executable: ?*PjrtSerializedExecutable, // out
    serialized_executable_deleter: ?*const fn (?*PjrtSerializedExecutable) callconv(.c) void, // out
};

pub const ExecutableDeserializeAndLoadArgs = extern struct {
    struct_size: usize,
    extension_start: ?*anyopaque,
    client: ?*PjrtClient,
    serialized_executable: ?[*]const u8,
    serialized_executable_size: usize,
    loaded_executable: ?*PjrtLoadedExecutable, // out
    overridden_serialized_compile_options: ?[*]const u8,
    overridden_serialized_compile_options_size: usize,
};

pub const LoadedExecutableGetExecutableArgs = extern struct {
    struct_size: usize,
    extension_start: ?*anyopaque,
    loaded_executable: ?*PjrtLoadedExecutable,
    executable: ?*PjrtExecutable, // out, not owned
};

pub const LoadedExecutableExecuteArgs = extern struct {
    struct_size: usize,
    extension_start: ?*anyopaque,
    executable: ?*PjrtLoadedExecutable,
    options: ?*ExecuteOptions,
    argument_lists: ?*anyopaque, // PJRT_Buffer* const* const*
    num_devices: usize,
    num_args: usize,
    output_lists: ?*anyopaque, // PJRT_Buffer** const*
    device_complete_events: ?[*]?*PjrtEvent,
    execute_device: ?*PjrtDevice,
};

pub const BufferDestroyArgs = extern struct {
    struct_size: usize,
    extension_start: ?*anyopaque,
    buffer: ?*PjrtBuffer,
};

pub const BufferToHostBufferArgs = extern struct {
    struct_size: usize,
    extension_start: ?*anyopaque,
    src: ?*PjrtBuffer,
    host_layout: ?*anyopaque,
    dst: ?*anyopaque,
    dst_size: usize,
    event: ?*PjrtEvent, // out
};

pub const BufferOnDeviceSizeInBytesArgs = extern struct {
    struct_size: usize,
    extension_start: ?*anyopaque,
    buffer: ?*PjrtBuffer,
    on_device_size_in_bytes: usize, // out
};

pub const EventAwaitArgs = extern struct {
    struct_size: usize,
    extension_start: ?*anyopaque,
    event: ?*PjrtEvent,
};

pub const EventDestroyArgs = extern struct {
    struct_size: usize,
    extension_start: ?*anyopaque,
    event: ?*PjrtEvent,
};

pub const Program = extern struct {
    struct_size: usize,
    extension_start: ?*anyopaque,
    code: ?[*]u8,
    code_size: usize,
    format: ?[*]const u8,
    format_size: usize,
};

pub const ExecuteOptions = extern struct {
    struct_size: usize,
    extension_start: ?*anyopaque,
    send_callbacks: ?*anyopaque,
    recv_callbacks: ?*anyopaque,
    num_send_ops: usize,
    num_recv_ops: usize,
    launch_id: c_int,
    // 4 bytes auto-padding
    non_donatable_input_indices: ?*const i64,
    num_non_donatable_input_indices: usize,
    // Fields added in PJRT API 0.83+
    context: ?*anyopaque,
    _reserved1: ?*anyopaque,
    _reserved2: ?*anyopaque,
    _reserved3: ?*anyopaque,
    _reserved4: ?*anyopaque,
};

// ── API version (embedded in Api struct by value) ──────────────────

pub const ApiVersion = extern struct {
    struct_size: usize,
    extension_start: ?*anyopaque,
    major_version: c_int,
    minor_version: c_int,
};

// ── PJRT_Api function pointer table ────────────────────────────────

/// The PJRT_Api struct returned by GetPjrtApi(). Contains a version
/// header followed by a table of function pointers. Indices into
/// `_fns` match the field order in pjrt_c_api.h.
pub const Api = extern struct {
    struct_size: usize,
    extension_start: ?*anyopaque,
    pjrt_api_version: ApiVersion,
    _fns: [NUM_FNS]?*const anyopaque,

    const NUM_FNS = 132;

    // ── Function pointer indices (pjrt_c_api.h field order) ────────
    // Only the functions we actually call are named.

    pub const FN_ERROR_DESTROY = 0;
    pub const FN_ERROR_MESSAGE = 1;
    pub const FN_PLUGIN_INITIALIZE = 3;
    const FN_EVENT_DESTROY = 5;
    const FN_EVENT_AWAIT = 8;
    const FN_CLIENT_CREATE = 10;
    const FN_CLIENT_DESTROY = 11;
    const FN_CLIENT_ADDRESSABLE_DEVICES = 16;
    const FN_CLIENT_COMPILE = 20;
    const FN_CLIENT_BUFFER_FROM_HOST = 22;
    const FN_EXEC_SERIALIZE = 49;
    const FN_LOADED_EXEC_DESTROY = 50;
    const FN_LOADED_EXEC_GET_EXECUTABLE = 51;
    const FN_LOADED_EXEC_EXECUTE = 55;
    const FN_EXEC_DESERIALIZE_AND_LOAD = 56;
    const FN_BUFFER_DESTROY = 58;
    const FN_BUFFER_ON_DEVICE_SIZE = 64;
    const FN_BUFFER_TO_HOST = 70;

    // ── Typed accessors ────────────────────────────────────────────

    fn castFn(comptime T: type, ptr: *const anyopaque) T {
        return @ptrCast(@alignCast(ptr));
    }

    pub fn errorDestroy(self: *const Api, args: *ErrorDestroyArgs) void {
        const f = castFn(*const fn (*ErrorDestroyArgs) callconv(.c) void, self._fns[FN_ERROR_DESTROY].?);
        f(args);
    }

    pub fn errorMessage(self: *const Api, args: *ErrorMessageArgs) void {
        const f = castFn(*const fn (*ErrorMessageArgs) callconv(.c) void, self._fns[FN_ERROR_MESSAGE].?);
        f(args);
    }

    pub fn pluginInitialize(self: *const Api, args: *PluginInitializeArgs) ?*PjrtError {
        const f = castFn(*const fn (*PluginInitializeArgs) callconv(.c) ?*PjrtError, self._fns[FN_PLUGIN_INITIALIZE].?);
        return f(args);
    }

    pub fn eventDestroy(self: *const Api, args: *EventDestroyArgs) ?*PjrtError {
        const f = castFn(*const fn (*EventDestroyArgs) callconv(.c) ?*PjrtError, self._fns[FN_EVENT_DESTROY].?);
        return f(args);
    }

    pub fn eventAwait(self: *const Api, args: *EventAwaitArgs) ?*PjrtError {
        const f = castFn(*const fn (*EventAwaitArgs) callconv(.c) ?*PjrtError, self._fns[FN_EVENT_AWAIT].?);
        return f(args);
    }

    pub fn clientCreate(self: *const Api, args: *ClientCreateArgs) ?*PjrtError {
        const f = castFn(*const fn (*ClientCreateArgs) callconv(.c) ?*PjrtError, self._fns[FN_CLIENT_CREATE].?);
        return f(args);
    }

    pub fn clientDestroy(self: *const Api, args: *ClientDestroyArgs) ?*PjrtError {
        const f = castFn(*const fn (*ClientDestroyArgs) callconv(.c) ?*PjrtError, self._fns[FN_CLIENT_DESTROY].?);
        return f(args);
    }

    pub fn clientAddressableDevices(self: *const Api, args: *ClientAddressableDevicesArgs) ?*PjrtError {
        const f = castFn(*const fn (*ClientAddressableDevicesArgs) callconv(.c) ?*PjrtError, self._fns[FN_CLIENT_ADDRESSABLE_DEVICES].?);
        return f(args);
    }

    pub fn clientCompile(self: *const Api, args: *ClientCompileArgs) ?*PjrtError {
        const f = castFn(*const fn (*ClientCompileArgs) callconv(.c) ?*PjrtError, self._fns[FN_CLIENT_COMPILE].?);
        return f(args);
    }

    pub fn clientBufferFromHostBuffer(self: *const Api, args: *ClientBufferFromHostBufferArgs) ?*PjrtError {
        const f = castFn(*const fn (*ClientBufferFromHostBufferArgs) callconv(.c) ?*PjrtError, self._fns[FN_CLIENT_BUFFER_FROM_HOST].?);
        return f(args);
    }

    pub fn hasExecutableSerialization(self: *const Api) bool {
        return self._fns[FN_EXEC_SERIALIZE] != null and
            self._fns[FN_LOADED_EXEC_GET_EXECUTABLE] != null and
            self._fns[FN_EXEC_DESERIALIZE_AND_LOAD] != null;
    }

    pub fn loadedExecutableDestroy(self: *const Api, args: *LoadedExecutableDestroyArgs) ?*PjrtError {
        const f = castFn(*const fn (*LoadedExecutableDestroyArgs) callconv(.c) ?*PjrtError, self._fns[FN_LOADED_EXEC_DESTROY].?);
        return f(args);
    }

    pub fn executableSerialize(self: *const Api, args: *ExecutableSerializeArgs) ?*PjrtError {
        const f = castFn(*const fn (*ExecutableSerializeArgs) callconv(.c) ?*PjrtError, self._fns[FN_EXEC_SERIALIZE].?);
        return f(args);
    }

    pub fn executableDeserializeAndLoad(self: *const Api, args: *ExecutableDeserializeAndLoadArgs) ?*PjrtError {
        const f = castFn(*const fn (*ExecutableDeserializeAndLoadArgs) callconv(.c) ?*PjrtError, self._fns[FN_EXEC_DESERIALIZE_AND_LOAD].?);
        return f(args);
    }

    pub fn loadedExecutableGetExecutable(self: *const Api, args: *LoadedExecutableGetExecutableArgs) ?*PjrtError {
        const f = castFn(*const fn (*LoadedExecutableGetExecutableArgs) callconv(.c) ?*PjrtError, self._fns[FN_LOADED_EXEC_GET_EXECUTABLE].?);
        return f(args);
    }

    pub fn loadedExecutableExecute(self: *const Api, args: *LoadedExecutableExecuteArgs) ?*PjrtError {
        const f = castFn(*const fn (*LoadedExecutableExecuteArgs) callconv(.c) ?*PjrtError, self._fns[FN_LOADED_EXEC_EXECUTE].?);
        return f(args);
    }

    pub fn bufferDestroy(self: *const Api, args: *BufferDestroyArgs) ?*PjrtError {
        const f = castFn(*const fn (*BufferDestroyArgs) callconv(.c) ?*PjrtError, self._fns[FN_BUFFER_DESTROY].?);
        return f(args);
    }

    pub fn bufferOnDeviceSizeInBytes(self: *const Api, args: *BufferOnDeviceSizeInBytesArgs) ?*PjrtError {
        const f = castFn(*const fn (*BufferOnDeviceSizeInBytesArgs) callconv(.c) ?*PjrtError, self._fns[FN_BUFFER_ON_DEVICE_SIZE].?);
        return f(args);
    }

    pub fn bufferToHostBuffer(self: *const Api, args: *BufferToHostBufferArgs) ?*PjrtError {
        const f = castFn(*const fn (*BufferToHostBufferArgs) callconv(.c) ?*PjrtError, self._fns[FN_BUFFER_TO_HOST].?);
        return f(args);
    }
};

/// Entry point function exported by PJRT plugins.
pub const GetPjrtApiFn = *const fn () callconv(.c) *const Api;
