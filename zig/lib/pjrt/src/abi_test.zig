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

//! Verify our hand-written ABI against the independently vendored upstream C
//! header. The dynamic fixture alone cannot catch a shared slot-number mistake.
const std = @import("std");
const c = @import("pjrt_c_types.zig");
extern const antfly_pjrt_api_offsets: [22]usize;
extern const antfly_pjrt_arg_layouts: [14]usize;

test "PJRT function slots match the upstream C header" {
    const Slot = struct { index: usize, name: []const u8 };
    const slots = [_]Slot{
        .{ .index = c.Api.FN_ERROR_DESTROY, .name = "PJRT_Error_Destroy" },
        .{ .index = c.Api.FN_ERROR_MESSAGE, .name = "PJRT_Error_Message" },
        .{ .index = c.Api.FN_PLUGIN_INITIALIZE, .name = "PJRT_Plugin_Initialize" },
        .{ .index = c.Api.FN_EVENT_DESTROY, .name = "PJRT_Event_Destroy" },
        .{ .index = c.Api.FN_EVENT_AWAIT, .name = "PJRT_Event_Await" },
        .{ .index = c.Api.FN_CLIENT_CREATE, .name = "PJRT_Client_Create" },
        .{ .index = c.Api.FN_CLIENT_DESTROY, .name = "PJRT_Client_Destroy" },
        .{ .index = c.Api.FN_CLIENT_PLATFORM_NAME, .name = "PJRT_Client_PlatformName" },
        .{ .index = c.Api.FN_CLIENT_ADDRESSABLE_DEVICES, .name = "PJRT_Client_AddressableDevices" },
        .{ .index = c.Api.FN_CLIENT_COMPILE, .name = "PJRT_Client_Compile" },
        .{ .index = c.Api.FN_CLIENT_BUFFER_FROM_HOST, .name = "PJRT_Client_BufferFromHostBuffer" },
        .{ .index = c.Api.FN_EXEC_DESTROY, .name = "PJRT_Executable_Destroy" },
        .{ .index = c.Api.FN_EXEC_NUM_OUTPUTS, .name = "PJRT_Executable_NumOutputs" },
        .{ .index = c.Api.FN_EXEC_SERIALIZE, .name = "PJRT_Executable_Serialize" },
        .{ .index = c.Api.FN_LOADED_EXEC_DESTROY, .name = "PJRT_LoadedExecutable_Destroy" },
        .{ .index = c.Api.FN_LOADED_EXEC_GET_EXECUTABLE, .name = "PJRT_LoadedExecutable_GetExecutable" },
        .{ .index = c.Api.FN_LOADED_EXEC_EXECUTE, .name = "PJRT_LoadedExecutable_Execute" },
        .{ .index = c.Api.FN_EXEC_DESERIALIZE_AND_LOAD, .name = "PJRT_Executable_DeserializeAndLoad" },
        .{ .index = c.Api.FN_BUFFER_DESTROY, .name = "PJRT_Buffer_Destroy" },
        .{ .index = c.Api.FN_BUFFER_ON_DEVICE_SIZE, .name = "PJRT_Buffer_OnDeviceSizeInBytes" },
        .{ .index = c.Api.FN_BUFFER_TO_HOST, .name = "PJRT_Buffer_ToHostBuffer" },
        .{ .index = c.Api.FN_EXEC_COMPILED_MEMORY_STATS, .name = "PJRT_Executable_GetCompiledMemoryStats" },
    };
    // Adding another wrapper entry point must also add its upstream field here.
    inline for (comptime std.meta.declarations(c.Api)) |decl| {
        if (comptime std.mem.startsWith(u8, decl, "FN_")) {
            var covered = false;
            inline for (slots) |slot| {
                if (slot.index == @field(c.Api, decl)) covered = true;
            }
            try std.testing.expect(covered);
        }
    }
    inline for (slots, 0..) |slot, i| {
        errdefer std.debug.print("PJRT ABI mismatch: {s}\n", .{slot.name});
        try std.testing.expectEqual(antfly_pjrt_api_offsets[i], @offsetOf(c.Api, "_fns") + slot.index * @sizeOf(?*const anyopaque));
    }
}

test "PJRT executable ownership and arity argument layouts match upstream" {
    const layouts = [_]usize{
        @sizeOf(c.ExecutableDestroyArgs),
        @offsetOf(c.ExecutableDestroyArgs, "struct_size"),
        @offsetOf(c.ExecutableDestroyArgs, "extension_start"),
        @offsetOf(c.ExecutableDestroyArgs, "executable"),
        @sizeOf(c.LoadedExecutableGetExecutableArgs),
        @offsetOf(c.LoadedExecutableGetExecutableArgs, "struct_size"),
        @offsetOf(c.LoadedExecutableGetExecutableArgs, "extension_start"),
        @offsetOf(c.LoadedExecutableGetExecutableArgs, "loaded_executable"),
        @offsetOf(c.LoadedExecutableGetExecutableArgs, "executable"),
        @sizeOf(c.ExecutableNumOutputsArgs),
        @offsetOf(c.ExecutableNumOutputsArgs, "struct_size"),
        @offsetOf(c.ExecutableNumOutputsArgs, "extension_start"),
        @offsetOf(c.ExecutableNumOutputsArgs, "executable"),
        @offsetOf(c.ExecutableNumOutputsArgs, "num_outputs"),
    };
    try std.testing.expectEqualSlices(usize, &antfly_pjrt_arg_layouts, &layouts);
}
