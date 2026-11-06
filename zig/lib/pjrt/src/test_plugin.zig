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

//! Deterministic provider for artifact admission tests. Handles really allocate
//! and free; exported counters verify constructor ordering and C API ownership.
const std = @import("std");
const c = @import("pjrt_c_types.zig");
var counters: [8]usize = @splat(0);
var device: u8 = 0;
var devices = [_]?*c.PjrtDevice{@ptrCast(&device)};
const serialized = "fixture executable";
fn handle(comptime T: type) *T {
    const value = std.heap.c_allocator.create(u8) catch @panic("fixture allocation");
    value.* = 0;
    return @ptrCast(value);
}
fn free(value: anytype) void {
    std.heap.c_allocator.destroy(@as(*u8, @ptrCast(value.?)));
}
pub export fn AntflyTestReset() void {
    counters = @splat(0);
}
pub export fn AntflyTestRead(index: usize) usize {
    return counters[index];
}
fn initialize(_: *c.PluginInitializeArgs) callconv(.c) ?*c.PjrtError {
    counters[0] += 1;
    return null;
}
fn create(args: *c.ClientCreateArgs) callconv(.c) ?*c.PjrtError {
    counters[1] += 1;
    args.client = handle(c.PjrtClient);
    return null;
}
fn destroy(args: *c.ClientDestroyArgs) callconv(.c) ?*c.PjrtError {
    counters[2] += 1;
    free(args.client);
    return null;
}
fn name(args: *c.ClientPlatformNameArgs) callconv(.c) ?*c.PjrtError {
    args.platform_name = "cpu";
    args.platform_name_size = 3;
    return null;
}
fn addressable(args: *c.ClientAddressableDevicesArgs) callconv(.c) ?*c.PjrtError {
    args.addressable_devices = &devices;
    args.num_addressable_devices = 1;
    return null;
}
fn compile(args: *c.ClientCompileArgs) callconv(.c) ?*c.PjrtError {
    counters[3] += 1;
    args.executable = handle(c.PjrtLoadedExecutable);
    return null;
}
fn deserialize(args: *c.ExecutableDeserializeAndLoadArgs) callconv(.c) ?*c.PjrtError {
    std.debug.assert(std.mem.eql(u8, args.serialized_executable.?[0..args.serialized_executable_size], serialized));
    counters[3] += 1;
    args.loaded_executable = handle(c.PjrtLoadedExecutable);
    return null;
}
fn destroyLoaded(args: *c.LoadedExecutableDestroyArgs) callconv(.c) ?*c.PjrtError {
    counters[4] += 1;
    free(args.executable);
    return null;
}
fn executable(args: *c.LoadedExecutableGetExecutableArgs) callconv(.c) ?*c.PjrtError {
    counters[5] += 1;
    args.executable = handle(c.PjrtExecutable);
    return null;
}
fn destroyExecutable(args: *c.ExecutableDestroyArgs) callconv(.c) ?*c.PjrtError {
    counters[6] += 1;
    free(args.executable);
    return null;
}
fn deleteSerialized(value: ?*c.PjrtSerializedExecutable) callconv(.c) void {
    counters[7] += 1;
    free(value);
}
fn serialize(args: *c.ExecutableSerializeArgs) callconv(.c) ?*c.PjrtError {
    args.serialized_bytes = serialized;
    args.serialized_bytes_size = serialized.len;
    args.serialized_executable = handle(c.PjrtSerializedExecutable);
    args.serialized_executable_deleter = &deleteSerialized;
    return null;
}
fn stats(args: *c.ExecutableGetCompiledMemoryStatsArgs) callconv(.c) ?*c.PjrtError {
    args.generated_code_size_in_bytes = 64;
    args.argument_size_in_bytes = 4096;
    args.output_size_in_bytes = 8;
    args.temp_size_in_bytes = 128;
    return null;
}
var api: c.Api = blk: {
    var value = std.mem.zeroes(c.Api);
    value.struct_size = @sizeOf(c.Api);
    value._fns[3] = @ptrCast(&initialize);
    value._fns[10] = @ptrCast(&create);
    value._fns[11] = @ptrCast(&destroy);
    value._fns[12] = @ptrCast(&name);
    value._fns[16] = @ptrCast(&addressable);
    value._fns[20] = @ptrCast(&compile);
    value._fns[41] = @ptrCast(&destroyExecutable);
    value._fns[49] = @ptrCast(&serialize);
    value._fns[50] = @ptrCast(&destroyLoaded);
    value._fns[51] = @ptrCast(&executable);
    value._fns[56] = @ptrCast(&deserialize);
    value._fns[96] = @ptrCast(&stats);
    break :blk value;
};
pub export fn GetPjrtApi() *const c.Api {
    return &api;
}
