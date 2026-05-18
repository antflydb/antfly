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

//! High-level PJRT client for compiling and executing HLO programs.
//!
//! Loads a PJRT plugin (e.g., CPU or TPU) via dlopen and provides
//! Zig-idiomatic wrappers for compilation and execution.

const std = @import("std");
const c = @import("pjrt_c_types.zig");
const hlo = @import("hlo.zig");

pub const PjrtError = error{
    PjrtApiError,
    PluginLoadFailed,
    SymbolNotFound,
    NoDevices,
    UnsupportedPjrtFeature,
};

pub const ExecutableArtifactSupport = struct {
    /// The local wrapper can compile serialized HLO artifacts through
    /// PJRT_Client_Compile.
    hlo_compile_from_artifact: bool = true,
    /// The local C API binding exposes PJRT executable
    /// serialize/deserialize entrypoints. A plugin may still reject
    /// either call for a particular executable.
    loaded_executable_serialize: bool = false,
    loaded_executable_deserialize: bool = false,

    pub fn loadOnlyExecutableArtifacts(self: ExecutableArtifactSupport) bool {
        return self.loaded_executable_serialize and self.loaded_executable_deserialize;
    }
};

// ── Error handling ─────────────────────────────────────────────────

fn getErrorMessage(api: *const c.Api, err: *c.PjrtError) []const u8 {
    var msg_args = c.ErrorMessageArgs{
        .struct_size = @sizeOf(c.ErrorMessageArgs),
        .extension_start = null,
        .err = err,
        .message = null,
        .message_size = 0,
    };
    api.errorMessage(&msg_args);
    if (msg_args.message) |msg| {
        return msg[0..msg_args.message_size];
    }
    return "unknown PJRT error";
}

fn destroyError(api: *const c.Api, err: *c.PjrtError) void {
    var args = c.ErrorDestroyArgs{
        .struct_size = @sizeOf(c.ErrorDestroyArgs),
        .extension_start = null,
        .err = err,
    };
    api.errorDestroy(&args);
}

fn checkError(api: *const c.Api, err: ?*c.PjrtError) PjrtError!void {
    if (err) |e| {
        const msg = getErrorMessage(api, e);
        std.log.err("PJRT error: {s}", .{msg});
        destroyError(api, e);
        return PjrtError.PjrtApiError;
    }
}

fn destroyEvent(api: *const c.Api, event: *c.PjrtEvent) void {
    var args = c.EventDestroyArgs{
        .struct_size = @sizeOf(c.EventDestroyArgs),
        .extension_start = null,
        .event = event,
    };
    _ = api.eventDestroy(&args);
}

fn awaitAndDestroyEvent(api: *const c.Api, event: *c.PjrtEvent) PjrtError!void {
    var await_args = c.EventAwaitArgs{
        .struct_size = @sizeOf(c.EventAwaitArgs),
        .extension_start = null,
        .event = event,
    };
    try checkError(api, api.eventAwait(&await_args));
    destroyEvent(api, event);
}

// ── Buffer ─────────────────────────────────────────────────────────

pub const Buffer = struct {
    api: *const c.Api,
    handle: *c.PjrtBuffer,

    pub fn deinit(self: *Buffer) void {
        var args = c.BufferDestroyArgs{
            .struct_size = @sizeOf(c.BufferDestroyArgs),
            .extension_start = null,
            .buffer = self.handle,
        };
        _ = self.api.bufferDestroy(&args);
    }

    pub fn onDeviceSizeInBytes(self: *const Buffer) PjrtError!usize {
        var args = c.BufferOnDeviceSizeInBytesArgs{
            .struct_size = @sizeOf(c.BufferOnDeviceSizeInBytesArgs),
            .extension_start = null,
            .buffer = self.handle,
            .on_device_size_in_bytes = 0,
        };
        try checkError(self.api, self.api.bufferOnDeviceSizeInBytes(&args));
        return args.on_device_size_in_bytes;
    }

    pub fn toFloat32(self: *const Buffer, allocator: std.mem.Allocator) ![]f32 {
        const size = try self.onDeviceSizeInBytes();
        const num_floats = size / @sizeOf(f32);
        const result = try allocator.alloc(f32, num_floats);
        errdefer allocator.free(result);

        var args = c.BufferToHostBufferArgs{
            .struct_size = @sizeOf(c.BufferToHostBufferArgs),
            .extension_start = null,
            .src = self.handle,
            .host_layout = null,
            .dst = @ptrCast(result.ptr),
            .dst_size = size,
            .event = null,
        };
        try checkError(self.api, self.api.bufferToHostBuffer(&args));

        if (args.event) |event| {
            try awaitAndDestroyEvent(self.api, event);
        }

        return result;
    }
};

// ── LoadedExecutable ───────────────────────────────────────────────

pub const LoadedExecutable = struct {
    api: *const c.Api,
    handle: *c.PjrtLoadedExecutable,
    device: *c.PjrtDevice,
    num_outputs: usize,

    pub fn deinit(self: *LoadedExecutable) void {
        var args = c.LoadedExecutableDestroyArgs{
            .struct_size = @sizeOf(c.LoadedExecutableDestroyArgs),
            .extension_start = null,
            .executable = self.handle,
        };
        _ = self.api.loadedExecutableDestroy(&args);
    }

    /// Serialize the plugin-native executable representation, if the
    /// loaded PJRT plugin supports it. Caller owns the returned bytes.
    pub fn serialize(self: *const LoadedExecutable, allocator: std.mem.Allocator) ![]u8 {
        if (!self.api.hasExecutableSerialization()) return PjrtError.UnsupportedPjrtFeature;

        var get_args = c.LoadedExecutableGetExecutableArgs{
            .struct_size = @sizeOf(c.LoadedExecutableGetExecutableArgs),
            .extension_start = null,
            .loaded_executable = self.handle,
            .executable = null,
        };
        try checkError(self.api, self.api.loadedExecutableGetExecutable(&get_args));

        var serialize_args = c.ExecutableSerializeArgs{
            .struct_size = @sizeOf(c.ExecutableSerializeArgs),
            .extension_start = null,
            .executable = get_args.executable orelse return PjrtError.PjrtApiError,
            .serialized_bytes = null,
            .serialized_bytes_size = 0,
            .serialized_executable = null,
            .serialized_executable_deleter = null,
        };
        try checkError(self.api, self.api.executableSerialize(&serialize_args));
        defer {
            if (serialize_args.serialized_executable) |serialized| {
                if (serialize_args.serialized_executable_deleter) |deleter| deleter(serialized);
            }
        }

        const bytes = serialize_args.serialized_bytes orelse return PjrtError.PjrtApiError;
        return allocator.dupe(u8, bytes[0..serialize_args.serialized_bytes_size]);
    }

    /// Execute with the given input buffers. Returns output buffers.
    /// Caller must deinit each returned Buffer and free the slice.
    pub fn execute(self: *const LoadedExecutable, inputs: []const Buffer, allocator: std.mem.Allocator) ![]Buffer {
        const num_outputs = self.num_outputs;

        // Build argument list for single-device execution
        const input_ptrs = try allocator.alloc(?*c.PjrtBuffer, inputs.len);
        defer allocator.free(input_ptrs);
        for (inputs, 0..) |buf, i| {
            input_ptrs[i] = buf.handle;
        }
        var arg_list_ptrs = [1][*]?*c.PjrtBuffer{input_ptrs.ptr};

        // Output buffer slots (filled by execute) — dynamically sized
        const output_ptrs = try allocator.alloc(?*c.PjrtBuffer, num_outputs);
        defer allocator.free(output_ptrs);
        @memset(output_ptrs, null);
        var output_list_ptrs = [1][*]?*c.PjrtBuffer{output_ptrs.ptr};

        // Completion event slot
        var events: [1]?*c.PjrtEvent = .{null};

        // Execute options (zeroed = defaults)
        var options: c.ExecuteOptions = std.mem.zeroes(c.ExecuteOptions);
        options.struct_size = @sizeOf(c.ExecuteOptions);

        var args = c.LoadedExecutableExecuteArgs{
            .struct_size = @sizeOf(c.LoadedExecutableExecuteArgs),
            .extension_start = null,
            .executable = self.handle,
            .options = &options,
            .argument_lists = @ptrCast(&arg_list_ptrs),
            .num_devices = 1,
            .num_args = inputs.len,
            .output_lists = @ptrCast(&output_list_ptrs),
            .device_complete_events = &events,
            .execute_device = self.device,
        };

        try checkError(self.api, self.api.loadedExecutableExecute(&args));

        // Wait for completion
        if (events[0]) |event| {
            try awaitAndDestroyEvent(self.api, event);
        }

        // Wrap output buffers
        const results = try allocator.alloc(Buffer, num_outputs);
        for (0..num_outputs) |i| {
            results[i] = .{ .api = self.api, .handle = output_ptrs[i].? };
        }
        return results;
    }
};

// ── Client ─────────────────────────────────────────────────────────

pub const Client = struct {
    api: *const c.Api,
    handle: *c.PjrtClient,
    device: *c.PjrtDevice,
    lib: std.DynLib,

    pub fn init(plugin_path: [:0]const u8) PjrtError!Client {
        // Load plugin shared library
        var lib = std.DynLib.open(plugin_path) catch return PjrtError.PluginLoadFailed;
        errdefer lib.close();

        // Get API function pointer table
        const getApi = lib.lookup(c.GetPjrtApiFn, "GetPjrtApi") orelse
            return PjrtError.SymbolNotFound;
        const api = getApi();

        // Initialize plugin (if the function is available)
        if (api._fns[c.Api.FN_PLUGIN_INITIALIZE]) |_| {
            var init_args = c.PluginInitializeArgs{
                .struct_size = @sizeOf(c.PluginInitializeArgs),
                .extension_start = null,
            };
            try checkError(api, api.pluginInitialize(&init_args));
        }

        // Create client (empty options = defaults)
        var create_args: c.ClientCreateArgs = std.mem.zeroes(c.ClientCreateArgs);
        create_args.struct_size = @sizeOf(c.ClientCreateArgs);
        try checkError(api, api.clientCreate(&create_args));

        const client_handle = create_args.client orelse return PjrtError.PjrtApiError;

        // Get first addressable device
        var devices_args = c.ClientAddressableDevicesArgs{
            .struct_size = @sizeOf(c.ClientAddressableDevicesArgs),
            .extension_start = null,
            .client = client_handle,
            .addressable_devices = null,
            .num_addressable_devices = 0,
        };
        try checkError(api, api.clientAddressableDevices(&devices_args));

        if (devices_args.num_addressable_devices == 0) return PjrtError.NoDevices;
        const device = devices_args.addressable_devices.?[0] orelse return PjrtError.NoDevices;

        return .{
            .api = api,
            .handle = client_handle,
            .device = device,
            .lib = lib,
        };
    }

    pub fn deinit(self: *Client) void {
        var args = c.ClientDestroyArgs{
            .struct_size = @sizeOf(c.ClientDestroyArgs),
            .extension_start = null,
            .client = self.handle,
        };
        _ = self.api.clientDestroy(&args);
        self.lib.close();
    }

    pub fn executableArtifactSupport(self: *const Client) ExecutableArtifactSupport {
        const has_serialization = self.api.hasExecutableSerialization();
        return .{
            .loaded_executable_serialize = has_serialization,
            .loaded_executable_deserialize = has_serialization,
        };
    }

    /// Return all addressable devices. The returned slice references
    /// PJRT-internal memory and must not be freed.
    pub fn addressableDevices(self: *const Client) PjrtError![]const *c.PjrtDevice {
        var args = c.ClientAddressableDevicesArgs{
            .struct_size = @sizeOf(c.ClientAddressableDevicesArgs),
            .extension_start = null,
            .client = self.handle,
            .addressable_devices = null,
            .num_addressable_devices = 0,
        };
        try checkError(self.api, self.api.clientAddressableDevices(&args));

        const n = args.num_addressable_devices;
        if (n == 0) return PjrtError.NoDevices;
        const raw_ptr = args.addressable_devices orelse return PjrtError.NoDevices;
        // Cast the nullable-element array to a non-null slice. Each entry
        // was validated by PJRT to be non-null.
        const typed: [*]const *c.PjrtDevice = @ptrCast(raw_ptr);
        return typed[0..n];
    }

    /// Compile an HLO program (serialized HloModuleProto bytes).
    /// `num_outputs` is the number of output buffers the program produces
    /// (1 for single-output, N for tuple-root programs).
    pub fn compile(self: *const Client, hlo_bytes: []const u8, num_outputs: usize) PjrtError!LoadedExecutable {
        const format = "hlo";
        var program = c.Program{
            .struct_size = @sizeOf(c.Program),
            .extension_start = null,
            .code = @constCast(hlo_bytes.ptr),
            .code_size = hlo_bytes.len,
            .format = format,
            .format_size = format.len,
        };

        // Build CompileOptionsProto: { executable_build_options(3): { num_replicas(4): 1, num_partitions(5): 1 } }
        const compile_options = comptime blk: {
            // ExecutableBuildOptionsProto
            const num_replicas = [_]u8{ 0x20, 0x01 }; // field 4, varint 1
            const num_partitions = [_]u8{ 0x28, 0x01 }; // field 5, varint 1
            const build_opts = num_replicas ++ num_partitions;
            // CompileOptionsProto field 3 (message)
            const tag = [_]u8{0x1a}; // (3 << 3) | 2 = 26 = 0x1a
            const len = [_]u8{build_opts.len};
            break :blk tag ++ len ++ build_opts;
        };

        var args = c.ClientCompileArgs{
            .struct_size = @sizeOf(c.ClientCompileArgs),
            .extension_start = null,
            .client = self.handle,
            .program = &program,
            .compile_options = &compile_options,
            .compile_options_size = compile_options.len,
            .executable = null,
        };
        try checkError(self.api, self.api.clientCompile(&args));

        return .{
            .api = self.api,
            .handle = args.executable orelse return PjrtError.PjrtApiError,
            .device = self.device,
            .num_outputs = num_outputs,
        };
    }

    /// Compile an HLO program targeting a specific device.
    /// `num_outputs` is the number of output buffers the program produces.
    pub fn compileForDevice(self: *const Client, hlo_bytes: []const u8, num_outputs: usize, target_device: *c.PjrtDevice) PjrtError!LoadedExecutable {
        const format = "hlo";
        var program = c.Program{
            .struct_size = @sizeOf(c.Program),
            .extension_start = null,
            .code = @constCast(hlo_bytes.ptr),
            .code_size = hlo_bytes.len,
            .format = format,
            .format_size = format.len,
        };

        const compile_options = comptime blk: {
            const num_replicas = [_]u8{ 0x20, 0x01 };
            const num_partitions = [_]u8{ 0x28, 0x01 };
            const build_opts = num_replicas ++ num_partitions;
            const tag = [_]u8{0x1a};
            const len = [_]u8{build_opts.len};
            break :blk tag ++ len ++ build_opts;
        };

        var args = c.ClientCompileArgs{
            .struct_size = @sizeOf(c.ClientCompileArgs),
            .extension_start = null,
            .client = self.handle,
            .program = &program,
            .compile_options = &compile_options,
            .compile_options_size = compile_options.len,
            .executable = null,
        };
        try checkError(self.api, self.api.clientCompile(&args));

        return .{
            .api = self.api,
            .handle = args.executable orelse return PjrtError.PjrtApiError,
            .device = target_device,
            .num_outputs = num_outputs,
        };
    }

    /// Load a plugin-native serialized executable without compiling HLO.
    pub fn deserializeExecutable(
        self: *const Client,
        serialized_executable: []const u8,
        num_outputs: usize,
    ) PjrtError!LoadedExecutable {
        if (!self.api.hasExecutableSerialization()) return PjrtError.UnsupportedPjrtFeature;

        var args = c.ExecutableDeserializeAndLoadArgs{
            .struct_size = @sizeOf(c.ExecutableDeserializeAndLoadArgs),
            .extension_start = null,
            .client = self.handle,
            .serialized_executable = serialized_executable.ptr,
            .serialized_executable_size = serialized_executable.len,
            .loaded_executable = null,
            .overridden_serialized_compile_options = null,
            .overridden_serialized_compile_options_size = 0,
        };
        try checkError(self.api, self.api.executableDeserializeAndLoad(&args));

        return .{
            .api = self.api,
            .handle = args.loaded_executable orelse return PjrtError.PjrtApiError,
            .device = self.device,
            .num_outputs = num_outputs,
        };
    }

    /// Upload f32 data from host to device, returning a Buffer handle.
    pub fn bufferFromHostFloat32(
        self: *const Client,
        data: []const f32,
        dims: []const i64,
    ) PjrtError!Buffer {
        var args = c.ClientBufferFromHostBufferArgs{
            .struct_size = @sizeOf(c.ClientBufferFromHostBufferArgs),
            .extension_start = null,
            .client = self.handle,
            .data = @ptrCast(data.ptr),
            .buffer_type = .float32,
            .dims = dims.ptr,
            .num_dims = dims.len,
            .byte_strides = null,
            .num_byte_strides = 0,
            .host_buffer_semantics = .immutable_only_during_call,
            .device = self.device,
            .memory = null,
            .device_layout = null,
            .done_with_host_buffer = null,
            .buffer = null,
        };
        try checkError(self.api, self.api.clientBufferFromHostBuffer(&args));

        // Wait for host→device transfer to complete so caller can free data
        if (args.done_with_host_buffer) |event| {
            try awaitAndDestroyEvent(self.api, event);
        }

        return .{
            .api = self.api,
            .handle = args.buffer orelse return PjrtError.PjrtApiError,
        };
    }

    /// Upload i64 data from host to device, returning a Buffer handle.
    pub fn bufferFromHostInt64(
        self: *const Client,
        data: []const i64,
        dims: []const i64,
    ) PjrtError!Buffer {
        var args = c.ClientBufferFromHostBufferArgs{
            .struct_size = @sizeOf(c.ClientBufferFromHostBufferArgs),
            .extension_start = null,
            .client = self.handle,
            .data = @ptrCast(data.ptr),
            .buffer_type = .int64,
            .dims = dims.ptr,
            .num_dims = dims.len,
            .byte_strides = null,
            .num_byte_strides = 0,
            .host_buffer_semantics = .immutable_only_during_call,
            .device = self.device,
            .memory = null,
            .device_layout = null,
            .done_with_host_buffer = null,
            .buffer = null,
        };
        try checkError(self.api, self.api.clientBufferFromHostBuffer(&args));

        if (args.done_with_host_buffer) |event| {
            try awaitAndDestroyEvent(self.api, event);
        }

        return .{
            .api = self.api,
            .handle = args.buffer orelse return PjrtError.PjrtApiError,
        };
    }

    /// Initialize PJRT client by searching for the plugin in standard locations.
    /// Search order:
    ///   1. TERMITE_XLA_PLUGIN, TERMITE_PJRT_PLUGIN, PJRT_PLUGIN_PATH, or PJRT_PLUGIN
    ///   2. ~/.termite/pjrt/<os>-<arch>/pjrt_c_api_cpu_plugin.dylib (or .so)
    ///   3. If neither resolves to an existing file, returns error.PjrtPluginNotFound
    pub fn initFromEnv(allocator: std.mem.Allocator) !Client {
        const plugin_path = try resolvePjrtPluginPath(allocator);
        defer allocator.free(plugin_path);
        const plugin_path_z = try allocator.dupeZ(u8, plugin_path);
        defer allocator.free(plugin_path_z);
        return Client.init(plugin_path_z);
    }

    /// Upload f32 data to a specific device, returning a Buffer handle.
    pub fn bufferFromHostFloat32ForDevice(
        self: *const Client,
        data: []const f32,
        dims: []const i64,
        target_device: *c.PjrtDevice,
    ) PjrtError!Buffer {
        var args = c.ClientBufferFromHostBufferArgs{
            .struct_size = @sizeOf(c.ClientBufferFromHostBufferArgs),
            .extension_start = null,
            .client = self.handle,
            .data = @ptrCast(data.ptr),
            .buffer_type = .float32,
            .dims = dims.ptr,
            .num_dims = dims.len,
            .byte_strides = null,
            .num_byte_strides = 0,
            .host_buffer_semantics = .immutable_only_during_call,
            .device = target_device,
            .memory = null,
            .device_layout = null,
            .done_with_host_buffer = null,
            .buffer = null,
        };
        try checkError(self.api, self.api.clientBufferFromHostBuffer(&args));

        if (args.done_with_host_buffer) |event| {
            try awaitAndDestroyEvent(self.api, event);
        }

        return .{
            .api = self.api,
            .handle = args.buffer orelse return PjrtError.PjrtApiError,
        };
    }
};

// ── Plugin path resolution ─────────────────────────────────────────

fn getEnvVarOwned(allocator: std.mem.Allocator, comptime name: [:0]const u8) !?[]u8 {
    const value = std.c.getenv(name) orelse return null;
    return try allocator.dupe(u8, std.mem.span(value));
}

fn pathExists(allocator: std.mem.Allocator, path: []const u8) !bool {
    const path_z = try allocator.dupeZ(u8, path);
    defer allocator.free(path_z);
    return std.c.access(path_z.ptr, std.c.F_OK) == 0;
}

fn resolvePjrtPluginPath(allocator: std.mem.Allocator) ![]u8 {
    // 1. Check environment variable
    if (try getEnvVarOwned(allocator, "TERMITE_XLA_PLUGIN")) |env_path| return env_path;
    if (try getEnvVarOwned(allocator, "TERMITE_PJRT_PLUGIN")) |env_path| return env_path;
    if (try getEnvVarOwned(allocator, "PJRT_PLUGIN_PATH")) |env_path| return env_path;
    if (try getEnvVarOwned(allocator, "PJRT_PLUGIN")) |env_path| return env_path;

    // Determine platform-specific subdirectory and extension
    const platform = switch (@import("builtin").target.os.tag) {
        .macos => "darwin",
        .linux => "linux",
        else => return error.PjrtPluginNotFound,
    };
    const arch = switch (@import("builtin").target.cpu.arch) {
        .aarch64 => "arm64",
        .x86_64 => "x86_64",
        else => return error.PjrtPluginNotFound,
    };
    const ext = switch (@import("builtin").target.os.tag) {
        .macos => "dylib",
        else => "so",
    };

    const plat_dir = try std.fmt.allocPrint(allocator, "{s}-{s}", .{ platform, arch });
    defer allocator.free(plat_dir);
    const lib_name = try std.fmt.allocPrint(allocator, "pjrt_c_api_cpu_plugin.{s}", .{ext});
    defer allocator.free(lib_name);

    if (try getEnvVarOwned(allocator, "HOME")) |home| {
        defer allocator.free(home);

        // 2. ~/Library/Application Support/go-xla/pjrt_c_api_cpu_plugin.<ext>
        //    Installed by go-xla / GoMLX (already present on this system).
        if (@import("builtin").target.os.tag == .macos) {
            const goxla_path = try std.fs.path.join(allocator, &.{ home, "Library", "Application Support", "go-xla", lib_name });
            errdefer allocator.free(goxla_path);
            if (try pathExists(allocator, goxla_path)) {
                return goxla_path;
            } else {
                allocator.free(goxla_path);
            }
        }

        // 3. ~/.termite/pjrt/<os>-<arch>/pjrt_c_api_cpu_plugin.<ext>
        const termite_path = try std.fs.path.join(allocator, &.{ home, ".termite", "pjrt", plat_dir, lib_name });
        errdefer allocator.free(termite_path);
        if (try pathExists(allocator, termite_path)) {
            return termite_path;
        } else {
            allocator.free(termite_path);
        }
    }

    return error.PjrtPluginNotFound;
}

// ── Tests ──────────────────────────────────────────────────────────

test "end-to-end: compile and execute HLO add via PJRT CPU plugin" {
    const alloc = std.testing.allocator;

    // Load CPU plugin — skip test gracefully if not found
    var client = Client.initFromEnv(alloc) catch |err| {
        std.debug.print("Skipping PJRT test (plugin not found: {})\n", .{err});
        return;
    };
    defer client.deinit();

    std.debug.print("PJRT API version: {}.{}\n", .{
        client.api.pjrt_api_version.major_version,
        client.api.pjrt_api_version.minor_version,
    });

    // Build HLO program: add(x, y)
    var builder = hlo.Builder.init(alloc, "add_computation");
    defer builder.deinit();

    const shape = hlo.Shape.init(.f32, &.{4});
    const p0 = try builder.parameter(0, shape, "x");
    const p1 = try builder.parameter(1, shape, "y");
    _ = try builder.add(p0, p1);

    const module = hlo.Module.init("test_add", builder.build());
    const hlo_bytes = try module.serialize(alloc);
    defer alloc.free(hlo_bytes);

    // Compile
    var executable = try client.compile(hlo_bytes, 1);
    defer executable.deinit();

    // Create input buffers on device
    const x_data = [_]f32{ 1.0, 2.0, 3.0, 4.0 };
    const y_data = [_]f32{ 10.0, 20.0, 30.0, 40.0 };
    var buf_x = try client.bufferFromHostFloat32(&x_data, &.{4});
    defer buf_x.deinit();
    var buf_y = try client.bufferFromHostFloat32(&y_data, &.{4});
    defer buf_y.deinit();

    // Execute
    const inputs = [_]Buffer{ buf_x, buf_y };
    var outputs = try executable.execute(&inputs, alloc);
    defer {
        for (outputs) |*buf| buf.deinit();
        alloc.free(outputs);
    }

    // Copy result to host and verify
    const result = try outputs[0].toFloat32(alloc);
    defer alloc.free(result);

    try std.testing.expectEqual(@as(usize, 4), result.len);
    try std.testing.expectApproxEqAbs(@as(f32, 11.0), result[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 22.0), result[1], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 33.0), result[2], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 44.0), result[3], 1e-6);
}

test "end-to-end: serialize and deserialize HLO add executable when PJRT plugin supports it" {
    const alloc = std.testing.allocator;

    var client = Client.initFromEnv(alloc) catch |err| {
        std.debug.print("Skipping PJRT executable serialization test (plugin not found: {})\n", .{err});
        return;
    };
    defer client.deinit();

    const support = client.executableArtifactSupport();
    if (!support.loadOnlyExecutableArtifacts()) {
        std.debug.print("Skipping PJRT executable serialization test (C API entrypoints unavailable)\n", .{});
        return;
    }

    var builder = hlo.Builder.init(alloc, "add_computation_serialized");
    defer builder.deinit();

    const shape = hlo.Shape.init(.f32, &.{4});
    const p0 = try builder.parameter(0, shape, "x");
    const p1 = try builder.parameter(1, shape, "y");
    _ = try builder.add(p0, p1);

    const module = hlo.Module.init("test_add_serialized", builder.build());
    const hlo_bytes = try module.serialize(alloc);
    defer alloc.free(hlo_bytes);

    var executable = try client.compile(hlo_bytes, 1);
    defer executable.deinit();

    const serialized = executable.serialize(alloc) catch |err| {
        std.debug.print("Skipping PJRT executable serialization test (serialize unsupported by plugin: {})\n", .{err});
        return;
    };
    defer alloc.free(serialized);

    var reloaded = client.deserializeExecutable(serialized, 1) catch |err| {
        std.debug.print("Skipping PJRT executable serialization test (deserialize unsupported by plugin: {})\n", .{err});
        return;
    };
    defer reloaded.deinit();

    const x_data = [_]f32{ 1.0, 2.0, 3.0, 4.0 };
    const y_data = [_]f32{ 10.0, 20.0, 30.0, 40.0 };
    var buf_x = try client.bufferFromHostFloat32(&x_data, &.{4});
    defer buf_x.deinit();
    var buf_y = try client.bufferFromHostFloat32(&y_data, &.{4});
    defer buf_y.deinit();

    const inputs = [_]Buffer{ buf_x, buf_y };
    var outputs = try reloaded.execute(&inputs, alloc);
    defer {
        for (outputs) |*buf| buf.deinit();
        alloc.free(outputs);
    }

    const result = try outputs[0].toFloat32(alloc);
    defer alloc.free(result);

    try std.testing.expectEqual(@as(usize, 4), result.len);
    try std.testing.expectApproxEqAbs(@as(f32, 11.0), result[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 22.0), result[1], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 33.0), result[2], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 44.0), result[3], 1e-6);
}

test "executable artifact support default reports HLO compile only" {
    const support = ExecutableArtifactSupport{};
    try std.testing.expect(support.hlo_compile_from_artifact);
    try std.testing.expect(!support.loaded_executable_serialize);
    try std.testing.expect(!support.loaded_executable_deserialize);
    try std.testing.expect(!support.loadOnlyExecutableArtifacts());
}
