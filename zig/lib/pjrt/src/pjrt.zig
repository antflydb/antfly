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
    InvalidMemoryStats,
    OutputCountMismatch,
};

/// Compiler-reported single-device execution footprint. Keep inputs and
/// outputs separate: our runtime retains inputs while outputs are returned,
/// so donated/aliased bytes must not be subtracted from this upper bound.
pub const CompiledMemoryStats = struct {
    code: usize,
    arguments: usize,
    outputs: usize,
    temporary: usize,
    host_code: usize,
    host_arguments: usize,
    host_outputs: usize,
    host_temporary: usize,
    /// Optional aggregate statistics added by newer plugins. Older plugins
    /// leave these zero; negative sentinels mean unavailable, not huge sizes.
    peak: usize = 0,
    total: usize = 0,
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
    defer destroyEvent(api, event);
    try checkError(api, api.eventAwait(&await_args));
}

/// OpenXLA runtime flags can override Client_Create's allocator and fraction,
/// and host preallocation is controlled independently of device preallocation.
/// A bounded client cannot inherit those uncontrolled process-wide policies.
pub fn validateBoundedCudaEnvironment() PjrtError!void {
    if (std.c.getenv("XLA_FLAGS")) |raw| if (std.mem.trim(u8, std.mem.span(raw), " \t\r\n").len != 0) return error.UnsupportedPjrtFeature;
    for ([_][:0]const u8{ "XLA_PJRT_GPU_HOST_MEMORY_PREALLOCATE", "TF_FORCE_UNIFIED_MEMORY" }) |name| {
        if (std.c.getenv(name)) |raw| {
            const value = std.mem.trim(u8, std.mem.span(raw), " \t\r\n");
            if (value.len != 0 and !std.mem.eql(u8, value, "0") and !std.ascii.eqlIgnoreCase(value, "false")) return error.UnsupportedPjrtFeature;
        }
    }
}

fn cudaClientOptions(capacity: usize, device: usize, visible: []const i64) PjrtError![6]c.NamedValue {
    const quantum: usize = 2 * 1024 * 1024;
    if (capacity < 2 * quantum or device == 0 or capacity > device) return error.InvalidMemoryStats;
    // OpenXLA rounds BFC limits upward to 2 MiB. Round our default pool down
    // first, and explicitly bound the separate collective pool: zero means an
    // automatic pool covering the rest of VRAM, not a disabled allocator.
    const default_capacity = (capacity - quantum) & ~(quantum - 1);
    const ratio = @as(f64, @floatFromInt(default_capacity)) / @as(f64, @floatFromInt(device));
    // Round down: converting to f32 must never widen an operator's byte cap.
    var fraction: f32 = @floatCast(ratio);
    if (@as(f64, fraction) > ratio) fraction = @bitCast(@as(u32, @bitCast(fraction)) - 1);
    if (fraction <= 0) return error.InvalidMemoryStats;
    return .{
        .{ .name = "allocator", .name_size = "allocator".len, .type = .string, .value = .{ .string_value = "bfc" }, .value_size = 3 },
        .{ .name = "memory_fraction", .name_size = "memory_fraction".len, .type = .float, .value = .{ .float_value = fraction } },
        .{ .name = "preallocate", .name_size = "preallocate".len, .type = .boolean, .value = .{ .bool_value = false } },
        .{ .name = "collective_memory_size", .name_size = "collective_memory_size".len, .type = .int64, .value = .{ .int64_value = quantum } },
        .{ .name = "visible_devices", .name_size = "visible_devices".len, .type = .int64_list, .value = .{ .int64_array_value = visible.ptr }, .value_size = visible.len },
        .{ .name = "should_stage_host_to_device_transfers", .name_size = "should_stage_host_to_device_transfers".len, .type = .boolean, .value = .{ .bool_value = false } },
    };
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

    /// Logical dimensions are independent of device padding and tiling.
    pub fn matchesFloat32Shape(self: *const Buffer, expected: []const i64) !bool {
        if (!self.api.hasBufferShape()) return error.UnsupportedPjrtFeature;
        var kind = std.mem.zeroes(c.BufferElementTypeArgs);
        kind.struct_size = @sizeOf(@TypeOf(kind));
        kind.buffer = self.handle;
        try checkError(self.api, self.api.bufferElementType(&kind));
        if (kind.buffer_type != .float32) return false;
        var dims = std.mem.zeroes(c.BufferDimensionsArgs);
        dims.struct_size = @sizeOf(@TypeOf(dims));
        dims.buffer = self.handle;
        try checkError(self.api, self.api.bufferDimensions(&dims));
        if (dims.num_dims != expected.len) return false;
        if (expected.len != 0) {
            for ((dims.dims orelse return error.PjrtApiError)[0..expected.len], expected) |actual, wanted| {
                if (actual < 0 or (wanted >= 0 and actual != wanted)) return false;
            }
        }
        return true;
    }

    pub fn toFloat32(self: *const Buffer, allocator: std.mem.Allocator) ![]f32 {
        return self.downloadFloat32(allocator, allocator, null);
    }

    pub fn toFloat32WithShape(self: *const Buffer, allocator: std.mem.Allocator, expected: []const i64) ![]f32 {
        return self.downloadFloat32(allocator, allocator, expected);
    }

    pub fn toFloat32WithShapeAndScratch(self: *const Buffer, allocator: std.mem.Allocator, scratch: std.mem.Allocator, expected: []const i64) ![]f32 {
        return self.downloadFloat32(allocator, scratch, expected);
    }

    fn downloadFloat32(self: *const Buffer, allocator: std.mem.Allocator, scratch: std.mem.Allocator, expected: ?[]const i64) ![]f32 {
        if (!self.api.hasBufferReadback()) return error.UnsupportedPjrtFeature;
        var kind = c.BufferElementTypeArgs{ .struct_size = @sizeOf(c.BufferElementTypeArgs), .extension_start = null, .buffer = self.handle, .buffer_type = .invalid };
        try checkError(self.api, self.api.bufferElementType(&kind));
        if (kind.buffer_type != .float32) return error.PjrtApiError;
        var dims = std.mem.zeroes(c.BufferDimensionsArgs);
        dims.struct_size = @sizeOf(@TypeOf(dims));
        dims.buffer = self.handle;
        try checkError(self.api, self.api.bufferDimensions(&dims));
        if (expected) |shape| {
            if (dims.num_dims != shape.len) return error.PjrtApiError;
            if (shape.len != 0 and !std.mem.eql(i64, (dims.dims orelse return error.PjrtApiError)[0..shape.len], shape)) return error.PjrtApiError;
        }
        var elements: usize = 1;
        if (dims.num_dims != 0) {
            for ((dims.dims orelse return error.PjrtApiError)[0..dims.num_dims]) |dim| {
                const n = std.math.cast(usize, dim) orelse return error.PjrtApiError;
                elements = try std.math.mul(usize, elements, n);
            }
        }
        // Ask for dense row-major host data, regardless of native layout.
        var inline_order: [8]i64 = undefined;
        const order = if (dims.num_dims <= inline_order.len) inline_order[0..dims.num_dims] else try scratch.alloc(i64, dims.num_dims);
        defer if (dims.num_dims > inline_order.len) scratch.free(order);
        for (order, 0..) |*dim, i| dim.* = @intCast(order.len - i - 1);
        var layout = std.mem.zeroes(c.BufferMemoryLayout);
        layout.struct_size = @sizeOf(@TypeOf(layout));
        layout.layout_type = 0; // PJRT_Buffer_MemoryLayout_Type_Tiled
        layout.layout.tiled = std.mem.zeroes(c.BufferMemoryLayoutTiled);
        layout.layout.tiled.struct_size = @sizeOf(c.BufferMemoryLayoutTiled);
        layout.layout.tiled.minor_to_major = order.ptr;
        layout.layout.tiled.minor_to_major_size = order.len;
        var query = c.BufferToHostBufferArgs{ .struct_size = @sizeOf(c.BufferToHostBufferArgs), .extension_start = null, .src = self.handle, .host_layout = &layout, .dst = null, .dst_size = 0, .event = null };
        const query_error = self.api.bufferToHostBuffer(&query);
        if (query_error != null) {
            if (query.event) |event| awaitAndDestroyEvent(self.api, event) catch {};
            try checkError(self.api, query_error);
        }
        if (query.event) |event| try awaitAndDestroyEvent(self.api, event);
        const size = query.dst_size;
        if (size != try std.math.mul(usize, elements, @sizeOf(f32))) return error.PjrtApiError;
        const num_floats = size / @sizeOf(f32);
        const result = try allocator.alloc(f32, num_floats);
        errdefer allocator.free(result);

        var args = c.BufferToHostBufferArgs{
            .struct_size = @sizeOf(c.BufferToHostBufferArgs),
            .extension_start = null,
            .src = self.handle,
            .host_layout = &layout,
            .dst = @ptrCast(result.ptr),
            .dst_size = size,
            .event = null,
        };
        const args_error = self.api.bufferToHostBuffer(&args);
        if (args_error != null) {
            if (args.event) |event| awaitAndDestroyEvent(self.api, event) catch {};
            try checkError(self.api, args_error);
        }

        if (args.event) |event| {
            try awaitAndDestroyEvent(self.api, event);
        }

        return result;
    }
};

// ── LoadedExecutable ───────────────────────────────────────────────

/// GetExecutable constructs an independently owned wrapper, including when
/// the loaded executable remains alive. Always release it before plugin close.
const Executable = struct {
    api: *const c.Api,
    handle: *c.PjrtExecutable,

    fn deinit(self: *@This()) void {
        var args = c.ExecutableDestroyArgs{ .struct_size = @sizeOf(c.ExecutableDestroyArgs), .extension_start = null, .executable = self.handle };
        _ = self.api.executableDestroy(&args);
    }
};

pub const LoadedExecutable = struct {
    api: *const c.Api,
    handle: *c.PjrtLoadedExecutable,
    device: *c.PjrtDevice,
    num_outputs: usize,

    fn executable(self: *const LoadedExecutable) PjrtError!Executable {
        if (!self.api.hasOwnedExecutableAccess()) return error.UnsupportedPjrtFeature;
        var args = std.mem.zeroes(c.LoadedExecutableGetExecutableArgs);
        args.struct_size = @sizeOf(@TypeOf(args));
        args.loaded_executable = self.handle;
        try checkError(self.api, self.api.loadedExecutableGetExecutable(&args));
        return .{ .api = self.api, .handle = args.executable orelse return error.PjrtApiError };
    }

    /// Native executable metadata is authoritative; sidecar shapes must match
    /// before any execution or retained-state allocation can occur.
    pub fn validateOutputShapes(self: *const LoadedExecutable, expected: []const []const i64) !void {
        if (!self.api.hasOutputShapes()) return error.UnsupportedPjrtFeature;
        var owned = try self.executable();
        defer owned.deinit();
        var types = std.mem.zeroes(c.ExecutableOutputElementTypesArgs);
        types.struct_size = @sizeOf(@TypeOf(types));
        types.executable = owned.handle;
        try checkError(self.api, self.api.executableOutputTypes(&types));
        var dims = std.mem.zeroes(c.ExecutableOutputDimensionsArgs);
        dims.struct_size = @sizeOf(@TypeOf(dims));
        dims.executable = owned.handle;
        try checkError(self.api, self.api.executableOutputDimensions(&dims));
        if (types.num_output_types != expected.len or dims.num_outputs != expected.len) return error.OutputCountMismatch;
        var offset: usize = 0;
        for (expected, 0..) |shape, i| {
            if ((types.output_types orelse return error.PjrtApiError)[i] != .float32) return error.PjrtApiError;
            const rank = (dims.dim_sizes orelse return error.PjrtApiError)[i];
            if (rank != shape.len) return error.PjrtApiError;
            const end = try std.math.add(usize, offset, rank);
            if (rank != 0 and !std.mem.eql(i64, (dims.dims orelse return error.PjrtApiError)[offset..end], shape)) return error.PjrtApiError;
            offset = end;
        }
    }

    pub fn compiledMemoryStats(self: *const LoadedExecutable) PjrtError!CompiledMemoryStats {
        if (!self.api.hasCompiledMemoryStats()) return error.UnsupportedPjrtFeature;
        var owned = try self.executable();
        defer owned.deinit();
        var stats = std.mem.zeroes(c.ExecutableGetCompiledMemoryStatsArgs);
        stats.struct_size = @sizeOf(@TypeOf(stats));
        stats.executable = owned.handle;
        try checkError(self.api, self.api.executableGetCompiledMemoryStats(&stats));
        return .{
            .code = std.math.cast(usize, stats.generated_code_size_in_bytes) orelse return error.InvalidMemoryStats,
            .arguments = std.math.cast(usize, stats.argument_size_in_bytes) orelse return error.InvalidMemoryStats,
            .outputs = std.math.cast(usize, stats.output_size_in_bytes) orelse return error.InvalidMemoryStats,
            .temporary = std.math.cast(usize, stats.temp_size_in_bytes) orelse return error.InvalidMemoryStats,
            .host_code = std.math.cast(usize, stats.host_generated_code_size_in_bytes) orelse return error.InvalidMemoryStats,
            .host_arguments = std.math.cast(usize, stats.host_argument_size_in_bytes) orelse return error.InvalidMemoryStats,
            .host_outputs = std.math.cast(usize, stats.host_output_size_in_bytes) orelse return error.InvalidMemoryStats,
            .host_temporary = std.math.cast(usize, stats.host_temp_size_in_bytes) orelse return error.InvalidMemoryStats,
            .peak = std.math.cast(usize, stats.peak_memory_in_bytes) orelse 0,
            .total = std.math.cast(usize, stats.total_size_in_bytes) orelse 0,
        };
    }

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

        var owned = try self.executable();
        defer owned.deinit();

        var serialize_args = c.ExecutableSerializeArgs{
            .struct_size = @sizeOf(c.ExecutableSerializeArgs),
            .extension_start = null,
            .executable = owned.handle,
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
        const results = try allocator.alloc(Buffer, num_outputs);
        errdefer allocator.free(results);
        errdefer for (output_ptrs) |maybe_buffer| {
            if (maybe_buffer) |handle| {
                var buffer = Buffer{ .api = self.api, .handle = handle };
                buffer.deinit();
            }
        };

        // Completion event slot
        var events: [1]?*c.PjrtEvent = .{null};
        defer if (events[0]) |event| destroyEvent(self.api, event);

        // Inputs remain owned by the caller, including retained KV needed if
        // readback fails. Native may-alias executables must not donate them.
        const non_donatable = try allocator.alloc(i64, inputs.len);
        defer allocator.free(non_donatable);
        for (non_donatable, 0..) |*index, i| index.* = @intCast(i);

        // Execute options (zeroed = defaults)
        var options: c.ExecuteOptions = std.mem.zeroes(c.ExecuteOptions);
        options.struct_size = @sizeOf(c.ExecuteOptions);
        options.non_donatable_input_indices = non_donatable.ptr;
        options.num_non_donatable_input_indices = non_donatable.len;

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
            events[0] = null;
            try awaitAndDestroyEvent(self.api, event);
        }

        // Wrap output buffers
        for (0..num_outputs) |i| {
            results[i] = .{ .api = self.api, .handle = output_ptrs[i] orelse return error.PjrtApiError };
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

    pub fn platformName(self: *const Client) PjrtError![]const u8 {
        var args = std.mem.zeroes(c.ClientPlatformNameArgs);
        args.struct_size = @sizeOf(@TypeOf(args));
        args.client = self.handle;
        try checkError(self.api, self.api.clientPlatformName(&args));
        const name = (args.platform_name orelse return error.PjrtApiError)[0..args.platform_name_size];
        return name;
    }

    pub fn isCpu(self: *const Client) PjrtError!bool {
        const name = try self.platformName();
        return std.ascii.eqlIgnoreCase(name, "cpu") or std.ascii.eqlIgnoreCase(name, "host");
    }

    pub fn init(plugin_path: [:0]const u8) PjrtError!Client {
        return initWithMemoryPolicy(plugin_path, null);
    }

    /// Load-only inference supplies a verified platform and an already leased
    /// allocator capacity. Never let CUDA's default preallocation policy spend
    /// unrelated VRAM before the executable's first budget check.
    pub const MemoryPolicy = union(enum) {
        cpu,
        cuda: struct { capacity_bytes: usize, device_bytes: usize },
    };

    pub fn initWithMemoryPolicy(plugin_path: [:0]const u8, policy: ?MemoryPolicy) PjrtError!Client {
        if (policy) |bounded| if (bounded == .cuda) try validateBoundedCudaEnvironment();
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
        var visible = [_]i64{0};
        var options: [6]c.NamedValue = undefined;
        if (policy) |bounded| switch (bounded) {
            .cpu => {},
            .cuda => |limit| {
                options = try cudaClientOptions(limit.capacity_bytes, limit.device_bytes, &visible);
                create_args.create_options = @ptrCast(&options);
                create_args.num_options = options.len;
            },
        };
        try checkError(api, api.clientCreate(&create_args));

        const client_handle = create_args.client orelse return PjrtError.PjrtApiError;
        errdefer {
            var destroy_args = c.ClientDestroyArgs{ .struct_size = @sizeOf(c.ClientDestroyArgs), .extension_start = null, .client = client_handle };
            _ = api.clientDestroy(&destroy_args);
        }

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

        return self.validateLoadedOutputs(args.executable orelse return PjrtError.PjrtApiError, self.device, num_outputs);
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

        return self.validateLoadedOutputs(args.executable orelse return PjrtError.PjrtApiError, target_device, num_outputs);
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

        return self.validateLoadedOutputs(args.loaded_executable orelse return PjrtError.PjrtApiError, self.device, num_outputs);
    }

    fn validateLoadedOutputs(self: *const Client, handle: *c.PjrtLoadedExecutable, device: *c.PjrtDevice, num_outputs: usize) PjrtError!LoadedExecutable {
        var loaded = LoadedExecutable{ .api = self.api, .handle = handle, .device = device, .num_outputs = num_outputs };
        errdefer loaded.deinit();
        if (!self.api.hasOutputCount()) return error.UnsupportedPjrtFeature;
        var owned = try loaded.executable();
        defer owned.deinit();
        var count = c.ExecutableNumOutputsArgs{ .struct_size = @sizeOf(c.ExecutableNumOutputsArgs), .extension_start = null, .executable = owned.handle, .num_outputs = 0 };
        try checkError(self.api, self.api.executableNumOutputs(&count));
        if (count.num_outputs != num_outputs) return error.OutputCountMismatch;
        return loaded;
    }

    /// Upload f32 data from host to device, returning a Buffer handle.
    pub fn bufferFromHostFloat32(
        self: *const Client,
        data: []const f32,
        dims: []const i64,
    ) PjrtError!Buffer {
        return self.uploadBuffer(@ptrCast(data.ptr), .float32, dims, self.device);
    }

    /// Upload i64 data from host to device, returning a Buffer handle.
    pub fn bufferFromHostInt64(
        self: *const Client,
        data: []const i64,
        dims: []const i64,
    ) PjrtError!Buffer {
        return self.uploadBuffer(@ptrCast(data.ptr), .int64, dims, self.device);
    }

    /// Initialize PJRT client by searching for the plugin in standard locations.
    /// Search order:
    ///   1. ANTFLY_INFERENCE_XLA_PLUGIN, ANTFLY_INFERENCE_PJRT_PLUGIN,
    ///      TERMITE_XLA_PLUGIN, TERMITE_PJRT_PLUGIN, PJRT_PLUGIN_PATH, or PJRT_PLUGIN
    ///   2. ~/.termite/pjrt/<os>-<arch>/pjrt_c_api_cpu_plugin.dylib (or .so)
    ///   3. If neither resolves to an existing file, returns error.PjrtPluginNotFound
    pub fn initFromEnv(allocator: std.mem.Allocator) !Client {
        const plugin_path = try resolvePjrtPluginPath(allocator);
        defer allocator.free(plugin_path);
        const plugin_path_z = try allocator.dupeSentinel(u8, plugin_path, 0);
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
        return self.uploadBuffer(@ptrCast(data.ptr), .float32, dims, target_device);
    }

    /// Acquire output ownership after a successful native call. Completion
    /// can fail after a valid buffer has been returned, and callers cannot
    /// release a buffer that this helper never returns to them.
    fn uploadBuffer(self: *const Client, data: *const anyopaque, kind: c.BufferType, dims: []const i64, device: *c.PjrtDevice) PjrtError!Buffer {
        var args = std.mem.zeroes(c.ClientBufferFromHostBufferArgs);
        args.struct_size = @sizeOf(@TypeOf(args));
        args.client = self.handle;
        args.data = data;
        args.buffer_type = kind;
        args.dims = dims.ptr;
        args.num_dims = dims.len;
        args.host_buffer_semantics = .immutable_only_during_call;
        args.device = device;
        try checkError(self.api, self.api.clientBufferFromHostBuffer(&args));
        defer if (args.done_with_host_buffer) |event| destroyEvent(self.api, event);
        errdefer if (args.buffer) |handle| {
            var buffer = Buffer{ .api = self.api, .handle = handle };
            buffer.deinit();
        };
        if (args.done_with_host_buffer) |event| {
            args.done_with_host_buffer = null;
            try awaitAndDestroyEvent(self.api, event);
        }
        return .{ .api = self.api, .handle = args.buffer orelse return error.PjrtApiError };
    }
};

// ── Plugin path resolution ─────────────────────────────────────────

fn getEnvVarOwned(allocator: std.mem.Allocator, comptime name: [:0]const u8) !?[]u8 {
    const value = std.c.getenv(name) orelse return null;
    return try allocator.dupe(u8, std.mem.span(value));
}

fn pathExists(allocator: std.mem.Allocator, path: []const u8) !bool {
    const path_z = try allocator.dupeSentinel(u8, path, 0);
    defer allocator.free(path_z);
    return std.c.access(path_z.ptr, std.c.F_OK) == 0;
}

fn resolvePjrtPluginPath(allocator: std.mem.Allocator) ![]u8 {
    // 1. Check environment variable
    if (try getEnvVarOwned(allocator, "ANTFLY_INFERENCE_XLA_PLUGIN")) |env_path| return env_path;
    if (try getEnvVarOwned(allocator, "ANTFLY_INFERENCE_PJRT_PLUGIN")) |env_path| return env_path;
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

test "compiled memory statistics honor API extent and reject unknown sizes" {
    const Fake = struct {
        var unknown: bool = false;
        var destroyed: usize = 0;
        fn destroy(_: *c.ExecutableDestroyArgs) callconv(.c) ?*c.PjrtError {
            destroyed += 1;
            return null;
        }
        fn get(args: *c.LoadedExecutableGetExecutableArgs) callconv(.c) ?*c.PjrtError {
            args.executable = @ptrFromInt(1);
            return null;
        }
        fn stats(args: *c.ExecutableGetCompiledMemoryStatsArgs) callconv(.c) ?*c.PjrtError {
            std.debug.assert(args.struct_size == 144);
            args.generated_code_size_in_bytes = 16;
            args.argument_size_in_bytes = 32;
            args.output_size_in_bytes = 64;
            args.alias_size_in_bytes = 32;
            args.temp_size_in_bytes = if (unknown) -1 else 128;
            args.host_temp_size_in_bytes = 256;
            args.peak_memory_in_bytes = 512;
            return null;
        }
    };
    var api = std.mem.zeroes(c.Api);
    api.struct_size = @sizeOf(c.Api);
    api._fns[40] = @ptrCast(&Fake.destroy);
    api._fns[51] = @ptrCast(&Fake.get);
    api._fns[96] = @ptrCast(&Fake.stats);
    const executable = LoadedExecutable{ .api = &api, .handle = @ptrFromInt(1), .device = @ptrFromInt(1), .num_outputs = 1 };
    Fake.unknown = false;
    Fake.destroyed = 0;
    const stats = try executable.compiledMemoryStats();
    try std.testing.expectEqual(@as(usize, 32), stats.arguments);
    try std.testing.expectEqual(@as(usize, 128), stats.temporary);
    try std.testing.expectEqual(@as(usize, 256), stats.host_temporary);
    try std.testing.expectEqual(@as(usize, 512), stats.peak);
    try std.testing.expectEqual(@as(usize, 1), Fake.destroyed);
    Fake.unknown = true;
    try std.testing.expectError(error.InvalidMemoryStats, executable.compiledMemoryStats());
    try std.testing.expectEqual(@as(usize, 2), Fake.destroyed);
    Fake.unknown = false;
    // A non-null slot outside the advertised ABI extent must not be read.
    api.struct_size = @offsetOf(c.Api, "_fns") + 96 * @sizeOf(?*const anyopaque);
    try std.testing.expectError(error.UnsupportedPjrtFeature, executable.compiledMemoryStats());
    api.struct_size = @sizeOf(c.Api);
    api._fns[96] = null;
    try std.testing.expectError(error.UnsupportedPjrtFeature, executable.compiledMemoryStats());
}

test "compiled memory CPU placement follows the PJRT platform" {
    const Fake = struct {
        var cpu: bool = true;
        fn name(args: *c.ClientPlatformNameArgs) callconv(.c) ?*c.PjrtError {
            const value: []const u8 = if (cpu) "cpu" else "cuda";
            args.platform_name = value.ptr;
            args.platform_name_size = value.len;
            return null;
        }
    };
    var api = std.mem.zeroes(c.Api);
    api.struct_size = @sizeOf(c.Api);
    api._fns[12] = @ptrCast(&Fake.name);
    const client = Client{ .api = &api, .handle = @ptrFromInt(1), .device = @ptrFromInt(1), .lib = undefined };
    Fake.cpu = true;
    try std.testing.expect(try client.isCpu());
    Fake.cpu = false;
    try std.testing.expect(!try client.isCpu());
}

test "bounded CUDA client options never widen the leased device capacity" {
    const visible = [_]i64{0};
    for ([_]usize{ 4194304, 4194305, 12345678, 8000000001 }) |capacity| {
        const device: usize = 16000000003;
        const options = try cudaClientOptions(capacity, device, &visible);
        try std.testing.expectEqualStrings("bfc", options[0].value.string_value[0..options[0].value_size]);
        const fraction = options[1].value.float_value;
        const quantum: usize = 2 * 1024 * 1024;
        const default_bytes: usize = @intFromFloat(@as(f64, fraction) * @as(f64, @floatFromInt(device)));
        const rounded_default = (default_bytes + quantum - 1) & ~(quantum - 1);
        try std.testing.expect(rounded_default + quantum <= capacity);
        try std.testing.expect(!options[2].value.bool_value);
        try std.testing.expectEqual(@as(i64, 2 * 1024 * 1024), options[3].value.int64_value);
        try std.testing.expectEqual(@as(usize, 1), options[4].value_size);
        try std.testing.expectEqual(@as(i64, 0), options[4].value.int64_array_value[0]);
        try std.testing.expect(!options[5].value.bool_value);
    }
    try std.testing.expectError(error.InvalidMemoryStats, cudaClientOptions(0, 1, &visible));
    try std.testing.expectError(error.InvalidMemoryStats, cudaClientOptions(2, 1, &visible));
}

test "serialize releases both caller-owned PJRT handles on allocation failure" {
    const Fake = struct {
        var owned: usize = 0;
        var serialized: usize = 0;
        fn destroy(_: *c.ExecutableDestroyArgs) callconv(.c) ?*c.PjrtError {
            owned -= 1;
            return null;
        }
        fn get(args: *c.LoadedExecutableGetExecutableArgs) callconv(.c) ?*c.PjrtError {
            owned += 1;
            args.executable = @ptrFromInt(1);
            return null;
        }
        fn delete(_: ?*c.PjrtSerializedExecutable) callconv(.c) void {
            serialized -= 1;
        }
        fn serialize(args: *c.ExecutableSerializeArgs) callconv(.c) ?*c.PjrtError {
            serialized += 1;
            args.serialized_bytes = "executable";
            args.serialized_bytes_size = 10;
            args.serialized_executable = @ptrFromInt(1);
            args.serialized_executable_deleter = &delete;
            return null;
        }
        fn deserialize(_: *c.ExecutableDeserializeAndLoadArgs) callconv(.c) ?*c.PjrtError {
            return null;
        }
        fn scenario(a: std.mem.Allocator, executable: LoadedExecutable) !void {
            const bytes = try executable.serialize(a);
            defer a.free(bytes);
            try std.testing.expectEqualStrings("executable", bytes);
        }
    };
    var api = std.mem.zeroes(c.Api);
    api.struct_size = @sizeOf(c.Api);
    api._fns[40] = @ptrCast(&Fake.destroy);
    api._fns[51] = @ptrCast(&Fake.get);
    api._fns[49] = @ptrCast(&Fake.serialize);
    api._fns[56] = @ptrCast(&Fake.deserialize);
    const executable = LoadedExecutable{ .api = &api, .handle = @ptrFromInt(1), .device = @ptrFromInt(1), .num_outputs = 1 };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fake.scenario, .{executable});
    try std.testing.expectEqual(@as(usize, 0), Fake.owned);
    try std.testing.expectEqual(@as(usize, 0), Fake.serialized);
    api._fns[40] = null;
    try std.testing.expectError(error.UnsupportedPjrtFeature, executable.serialize(std.testing.allocator));
}

test "bounded CUDA clients reject allocator environment overrides before plugin initialization" {
    const Env = struct {
        extern "c" fn setenv([*:0]const u8, [*:0]const u8, c_int) c_int;
        extern "c" fn unsetenv([*:0]const u8) c_int;
    };
    const a = std.testing.allocator;
    const names = [_][:0]const u8{ "XLA_FLAGS", "XLA_PJRT_GPU_HOST_MEMORY_PREALLOCATE", "TF_FORCE_UNIFIED_MEMORY" };
    var saved: [3]?[:0]u8 = @splat(null);
    var initialized: usize = 0;
    defer {
        for (names[0..initialized], saved[0..initialized]) |name, value| {
            if (value) |original| {
                _ = Env.setenv(name, original, 1);
                a.free(original);
            } else _ = Env.unsetenv(name);
        }
    }
    for (names, 0..) |name, i| {
        if (std.c.getenv(name)) |value| saved[i] = try a.dupeSentinel(u8, std.mem.span(value), 0);
        initialized += 1;
        try std.testing.expectEqual(@as(c_int, 0), Env.unsetenv(name));
    }
    try validateBoundedCudaEnvironment();
    for (names, [_][:0]const u8{ "--xla_gpu_memory_fraction_policy=0.9", "true", "1" }) |name, value| {
        try std.testing.expectEqual(@as(c_int, 0), Env.setenv(name, value, 1));
        try std.testing.expectError(error.UnsupportedPjrtFeature, Client.initWithMemoryPolicy("missing-provider", .{ .cuda = .{ .capacity_bytes = 1, .device_bytes = 2 } }));
        try std.testing.expectEqual(@as(c_int, 0), Env.unsetenv(name));
    }
    try std.testing.expectEqual(@as(c_int, 0), Env.setenv(names[1], "false", 1));
    try std.testing.expectEqual(@as(c_int, 0), Env.setenv(names[2], "0", 1));
    try validateBoundedCudaEnvironment();
}

test "execution owns all native outputs and completion events across host allocation failures" {
    const Fake = struct {
        var live_buffers: usize = 0;
        var live_events: usize = 0;
        var missing_output: bool = false;
        fn execute(args: *c.LoadedExecutableExecuteArgs) callconv(.c) ?*c.PjrtError {
            const lists: [*][*]?*c.PjrtBuffer = @ptrCast(@alignCast(args.output_lists.?));
            lists[0][0] = @ptrFromInt(1);
            live_buffers += 1;
            if (!missing_output) {
                lists[0][1] = @ptrFromInt(2);
                live_buffers += 1;
            }
            args.device_complete_events.?[0] = @ptrFromInt(1);
            live_events += 1;
            return null;
        }
        fn destroyBuffer(_: *c.BufferDestroyArgs) callconv(.c) ?*c.PjrtError {
            live_buffers -= 1;
            return null;
        }
        fn destroyEvent_(_: *c.EventDestroyArgs) callconv(.c) ?*c.PjrtError {
            live_events -= 1;
            return null;
        }
        fn awaitEvent(_: *c.EventAwaitArgs) callconv(.c) ?*c.PjrtError {
            return null;
        }
        fn scenario(a: std.mem.Allocator, executable: LoadedExecutable) !void {
            defer std.debug.assert(live_buffers == 0 and live_events == 0);
            const outputs = try executable.execute(&.{}, a);
            defer a.free(outputs);
            defer for (outputs) |*buffer| buffer.deinit();
            try std.testing.expectEqual(@as(usize, 2), outputs.len);
        }
    };
    var api = std.mem.zeroes(c.Api);
    api.struct_size = @sizeOf(c.Api);
    api._fns[55] = @ptrCast(&Fake.execute);
    api._fns[58] = @ptrCast(&Fake.destroyBuffer);
    api._fns[5] = @ptrCast(&Fake.destroyEvent_);
    api._fns[8] = @ptrCast(&Fake.awaitEvent);
    const executable = LoadedExecutable{ .api = &api, .handle = @ptrFromInt(1), .device = @ptrFromInt(1), .num_outputs = 2 };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fake.scenario, .{executable});
    Fake.missing_output = true;
    defer Fake.missing_output = false;
    try std.testing.expectError(error.PjrtApiError, Fake.scenario(std.testing.allocator, executable));
}

test "compile and deserialize reject native output arity with balanced ownership" {
    const Fake = struct {
        var loaded_destroyed: usize = 0;
        var executable_destroyed: usize = 0;
        fn compile(args: *c.ClientCompileArgs) callconv(.c) ?*c.PjrtError {
            args.executable = @ptrFromInt(1);
            return null;
        }
        fn deserialize(args: *c.ExecutableDeserializeAndLoadArgs) callconv(.c) ?*c.PjrtError {
            args.loaded_executable = @ptrFromInt(1);
            return null;
        }
        fn get(args: *c.LoadedExecutableGetExecutableArgs) callconv(.c) ?*c.PjrtError {
            args.executable = @ptrFromInt(2);
            return null;
        }
        fn count(args: *c.ExecutableNumOutputsArgs) callconv(.c) ?*c.PjrtError {
            args.num_outputs = 3;
            return null;
        }
        fn destroyLoaded(_: *c.LoadedExecutableDestroyArgs) callconv(.c) ?*c.PjrtError {
            loaded_destroyed += 1;
            return null;
        }
        fn destroyExecutable(_: *c.ExecutableDestroyArgs) callconv(.c) ?*c.PjrtError {
            executable_destroyed += 1;
            return null;
        }
    };
    var api = std.mem.zeroes(c.Api);
    api.struct_size = @sizeOf(c.Api);
    api._fns[c.Api.FN_CLIENT_COMPILE] = @ptrCast(&Fake.compile);
    api._fns[c.Api.FN_EXEC_DESERIALIZE_AND_LOAD] = @ptrCast(&Fake.deserialize);
    api._fns[c.Api.FN_EXEC_SERIALIZE] = @ptrCast(&Fake.compile);
    api._fns[c.Api.FN_LOADED_EXEC_GET_EXECUTABLE] = @ptrCast(&Fake.get);
    api._fns[c.Api.FN_EXEC_NUM_OUTPUTS] = @ptrCast(&Fake.count);
    api._fns[c.Api.FN_LOADED_EXEC_DESTROY] = @ptrCast(&Fake.destroyLoaded);
    api._fns[c.Api.FN_EXEC_DESTROY] = @ptrCast(&Fake.destroyExecutable);
    const client = Client{ .lib = undefined, .api = &api, .handle = @ptrFromInt(1), .device = @ptrFromInt(1) };
    Fake.loaded_destroyed = 0;
    Fake.executable_destroyed = 0;
    try std.testing.expectError(error.OutputCountMismatch, client.compile("hlo", 2));
    try std.testing.expectError(error.OutputCountMismatch, client.compileForDevice("hlo", 2, @ptrFromInt(1)));
    try std.testing.expectError(error.OutputCountMismatch, client.deserializeExecutable("serialized", 2));
    try std.testing.expectEqual(@as(usize, 3), Fake.loaded_destroyed);
    try std.testing.expectEqual(@as(usize, 3), Fake.executable_destroyed);
}

test "float32 downloads use logical host bytes and reject invalid type size and shape" {
    const Fake = struct {
        var kind: c.BufferType = .float32;
        var size: usize = 8;
        var transfers: usize = 0;
        var events_destroyed: usize = 0;
        var events_awaited: usize = 0;
        const dims = [_]i64{2};
        fn elementType(args: *c.BufferElementTypeArgs) callconv(.c) ?*c.PjrtError {
            args.buffer_type = kind;
            return null;
        }
        fn dimensions(args: *c.BufferDimensionsArgs) callconv(.c) ?*c.PjrtError {
            args.dims = &dims;
            args.num_dims = 1;
            return null;
        }
        fn awaitEvent(_: *c.EventAwaitArgs) callconv(.c) ?*c.PjrtError {
            events_awaited += 1;
            return null;
        }
        fn destroyEvent_(_: *c.EventDestroyArgs) callconv(.c) ?*c.PjrtError {
            events_destroyed += 1;
            return null;
        }
        fn download(args: *c.BufferToHostBufferArgs) callconv(.c) ?*c.PjrtError {
            args.event = @ptrFromInt(1);
            if (args.dst == null) {
                args.dst_size = size;
            } else {
                transfers += 1;
                std.debug.assert(args.dst_size == 8);
                @memcpy(@as([*]u8, @ptrCast(args.dst.?))[0..8], std.mem.sliceAsBytes(&[_]f32{ 0, 1 }));
            }
            return null;
        }
    };
    var api = std.mem.zeroes(c.Api);
    api.struct_size = @sizeOf(c.Api);
    api._fns[c.Api.FN_BUFFER_ELEMENT_TYPE] = @ptrCast(&Fake.elementType);
    api._fns[c.Api.FN_BUFFER_DIMENSIONS] = @ptrCast(&Fake.dimensions);
    api._fns[c.Api.FN_BUFFER_TO_HOST] = @ptrCast(&Fake.download);
    api._fns[c.Api.FN_EVENT_AWAIT] = @ptrCast(&Fake.awaitEvent);
    api._fns[c.Api.FN_EVENT_DESTROY] = @ptrCast(&Fake.destroyEvent_);
    // No device-size callback: physical padding must never control host size.
    const buffer = Buffer{ .api = &api, .handle = @ptrFromInt(1) };
    Fake.kind = .float32;
    Fake.size = 8;
    Fake.transfers = 0;
    Fake.events_destroyed = 0;
    Fake.events_awaited = 0;
    const values = try buffer.toFloat32WithShape(std.testing.allocator, &.{2});
    defer std.testing.allocator.free(values);
    try std.testing.expectEqualSlices(f32, &.{ 0, 1 }, values);
    try std.testing.expectError(error.PjrtApiError, buffer.toFloat32WithShape(std.testing.allocator, &.{ 1, 2 }));
    var failure = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    try std.testing.expectError(error.OutOfMemory, buffer.toFloat32(failure.allocator()));
    Fake.size = 6;
    try std.testing.expectError(error.PjrtApiError, buffer.toFloat32(std.testing.allocator));
    Fake.size = 4;
    try std.testing.expectError(error.PjrtApiError, buffer.toFloat32(std.testing.allocator));
    Fake.kind = .int64;
    try std.testing.expectError(error.PjrtApiError, buffer.toFloat32(std.testing.allocator));
    try std.testing.expectEqual(@as(usize, 1), Fake.transfers);
    try std.testing.expectEqual(@as(usize, 5), Fake.events_destroyed);
    try std.testing.expectEqual(Fake.events_destroyed, Fake.events_awaited);
    api.struct_size = @offsetOf(c.Api, "_fns") + c.Api.FN_BUFFER_TO_HOST * @sizeOf(?*const anyopaque);
    try std.testing.expectError(error.UnsupportedPjrtFeature, buffer.toFloat32(std.testing.allocator));
}

test "native output metadata rejects sidecar dimensions and types before execution" {
    const Fake = struct {
        var destroyed: usize = 0;
        var kind = [_]c.BufferType{.float32};
        const dims = [_]i64{2};
        const ranks = [_]usize{1};
        fn get(args: *c.LoadedExecutableGetExecutableArgs) callconv(.c) ?*c.PjrtError {
            args.executable = @ptrFromInt(2);
            return null;
        }
        fn destroy(_: *c.ExecutableDestroyArgs) callconv(.c) ?*c.PjrtError {
            destroyed += 1;
            return null;
        }
        fn types(args: *c.ExecutableOutputElementTypesArgs) callconv(.c) ?*c.PjrtError {
            args.output_types = &kind;
            args.num_output_types = 1;
            return null;
        }
        fn dimensions(args: *c.ExecutableOutputDimensionsArgs) callconv(.c) ?*c.PjrtError {
            args.num_outputs = 1;
            args.dims = &dims;
            args.dim_sizes = &ranks;
            return null;
        }
    };
    var api = std.mem.zeroes(c.Api);
    api.struct_size = @sizeOf(c.Api);
    api._fns[c.Api.FN_LOADED_EXEC_GET_EXECUTABLE] = @ptrCast(&Fake.get);
    api._fns[c.Api.FN_EXEC_DESTROY] = @ptrCast(&Fake.destroy);
    api._fns[c.Api.FN_EXEC_OUTPUT_TYPES] = @ptrCast(&Fake.types);
    api._fns[c.Api.FN_EXEC_OUTPUT_DIMENSIONS] = @ptrCast(&Fake.dimensions);
    const executable = LoadedExecutable{ .api = &api, .handle = @ptrFromInt(1), .device = @ptrFromInt(1), .num_outputs = 1 };
    Fake.destroyed = 0;
    Fake.kind[0] = .float32;
    try executable.validateOutputShapes(&.{&.{2}});
    try std.testing.expectError(error.PjrtApiError, executable.validateOutputShapes(&.{&.{ 1, 2 }}));
    try std.testing.expectError(error.PjrtApiError, executable.validateOutputShapes(&.{&.{1}}));
    Fake.kind[0] = .int64;
    try std.testing.expectError(error.PjrtApiError, executable.validateOutputShapes(&.{&.{2}}));
    try std.testing.expectEqual(@as(usize, 4), Fake.destroyed);
    api.struct_size = @offsetOf(c.Api, "_fns") + c.Api.FN_EXEC_OUTPUT_DIMENSIONS * @sizeOf(?*const anyopaque);
    try std.testing.expectError(error.UnsupportedPjrtFeature, executable.validateOutputShapes(&.{&.{2}}));
    try std.testing.expectEqual(@as(usize, 4), Fake.destroyed);
}

test "row major readback canonicalizes native layout and KV shape ignores padding" {
    const Fake = struct {
        const dims = [_]i64{ 2, 2 };
        var kind: c.BufferType = .float32;
        fn elementType(args: *c.BufferElementTypeArgs) callconv(.c) ?*c.PjrtError {
            args.buffer_type = kind;
            return null;
        }
        fn dimensions(args: *c.BufferDimensionsArgs) callconv(.c) ?*c.PjrtError {
            args.dims = &dims;
            args.num_dims = dims.len;
            return null;
        }
        fn download(args: *c.BufferToHostBufferArgs) callconv(.c) ?*c.PjrtError {
            const layout = args.host_layout.?;
            std.debug.assert(layout.layout_type == 0);
            std.debug.assert(layout.layout.tiled.num_tiles == 0);
            std.debug.assert(std.mem.eql(i64, layout.layout.tiled.minor_to_major.?[0..2], &.{ 1, 0 }));
            if (args.dst == null) {
                args.dst_size = 16;
                return null;
            }
            // Native column-major storage would be [10,20,0,1]. The requested
            // host layout must expose logical rows [10,0],[20,1] instead.
            @memcpy(@as([*]u8, @ptrCast(args.dst.?))[0..16], std.mem.sliceAsBytes(&[_]f32{ 10, 0, 20, 1 }));
            return null;
        }
    };
    var api = std.mem.zeroes(c.Api);
    api.struct_size = @sizeOf(c.Api);
    api._fns[c.Api.FN_BUFFER_ELEMENT_TYPE] = @ptrCast(&Fake.elementType);
    api._fns[c.Api.FN_BUFFER_DIMENSIONS] = @ptrCast(&Fake.dimensions);
    api._fns[c.Api.FN_BUFFER_TO_HOST] = @ptrCast(&Fake.download);
    // No physical-size callback: padding cannot decide logical compatibility.
    const buffer = Buffer{ .api = &api, .handle = @ptrFromInt(1) };
    Fake.kind = .float32;
    try std.testing.expect(try buffer.matchesFloat32Shape(&.{ 2, 2 }));
    try std.testing.expect(!try buffer.matchesFloat32Shape(&.{4}));
    try std.testing.expect(try buffer.matchesFloat32Shape(&.{ -1, 2 }));
    try std.testing.expect(!try buffer.matchesFloat32Shape(&.{ -1, 3 }));
    Fake.kind = .int64;
    try std.testing.expect(!try buffer.matchesFloat32Shape(&.{ 2, 2 }));
    Fake.kind = .float32;
    const values = try buffer.toFloat32WithShape(std.testing.allocator, &.{ 2, 2 });
    defer std.testing.allocator.free(values);
    try std.testing.expectEqualSlices(f32, &.{ 10, 0, 20, 1 }, values);
    try std.testing.expect(values[2] > values[3]);
}
