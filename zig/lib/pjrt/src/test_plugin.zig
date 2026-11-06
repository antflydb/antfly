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
// Slots 11/12 track live/peak bytes of the fixture's scalar F32 KV outputs.
// Token IDs and logits use different sizes, so these measure KV specifically.
var counters: [13]usize = @splat(0);
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
    const bytes = args.serialized_executable.?[0..args.serialized_executable_size];
    const kind: u8 = if (std.mem.eql(u8, bytes, serialized)) 0 else if (std.mem.eql(u8, bytes, "fixture paged prefill")) 1 else if (std.mem.eql(u8, bytes, "fixture paged decode")) 2 else @panic("invalid fixture executable");
    counters[3] += 1;
    args.loaded_executable = handle(c.PjrtLoadedExecutable);
    @as(*u8, @ptrCast(args.loaded_executable.?)).* = kind;
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
    @as(*u8, @ptrCast(args.executable.?)).* = @as(*const u8, @ptrCast(args.loaded_executable.?)).*;
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
fn numOutputs(args: *c.ExecutableNumOutputsArgs) callconv(.c) ?*c.PjrtError {
    args.num_outputs = if (@as(*const u8, @ptrCast(args.executable)).* == 0) 1 else 3;
    return null;
}
fn stats(args: *c.ExecutableGetCompiledMemoryStatsArgs) callconv(.c) ?*c.PjrtError {
    args.generated_code_size_in_bytes = 64;
    args.argument_size_in_bytes = 4096;
    args.output_size_in_bytes = if (@as(*const u8, @ptrCast(args.executable.?)).* == 0) 8 else 16;
    args.temp_size_in_bytes = 128;
    return null;
}
const DataBuffer = struct { bytes: []u8 };
fn dataBuffer(raw: ?*c.PjrtBuffer) *DataBuffer {
    return @ptrCast(@alignCast(raw.?));
}
fn newBuffer(bytes: []const u8) *c.PjrtBuffer {
    const buffer = std.heap.c_allocator.create(DataBuffer) catch @panic("fixture allocation");
    buffer.bytes = std.heap.c_allocator.dupe(u8, bytes) catch @panic("fixture allocation");
    counters[8] += 1;
    if (bytes.len == @sizeOf(f32)) {
        counters[11] += bytes.len;
        counters[12] = @max(counters[12], counters[11]);
    }
    return @ptrCast(buffer);
}
fn upload(args: *c.ClientBufferFromHostBufferArgs) callconv(.c) ?*c.PjrtError {
    var bytes: usize = if (args.buffer_type == .int64) 8 else 4;
    for (args.dims.?[0..args.num_dims]) |dim| bytes *= @intCast(dim);
    args.buffer = newBuffer(@as([*]const u8, @ptrCast(args.data.?))[0..bytes]);
    args.done_with_host_buffer = null;
    return null;
}
fn destroyBuffer(args: *c.BufferDestroyArgs) callconv(.c) ?*c.PjrtError {
    const buffer = dataBuffer(args.buffer);
    if (buffer.bytes.len == @sizeOf(f32)) counters[11] -= buffer.bytes.len;
    std.heap.c_allocator.free(buffer.bytes);
    std.heap.c_allocator.destroy(buffer);
    counters[9] += 1;
    return null;
}
fn bufferSize(args: *c.BufferOnDeviceSizeInBytesArgs) callconv(.c) ?*c.PjrtError {
    args.on_device_size_in_bytes = dataBuffer(args.buffer).bytes.len;
    return null;
}
fn download(args: *c.BufferToHostBufferArgs) callconv(.c) ?*c.PjrtError {
    const data = dataBuffer(args.src).bytes;
    std.debug.assert(args.dst_size >= data.len);
    @memcpy(@as([*]u8, @ptrCast(args.dst.?))[0..data.len], data);
    args.event = null;
    return null;
}
fn execute(args: *c.LoadedExecutableExecuteArgs) callconv(.c) ?*c.PjrtError {
    const kind = @as(*const u8, @ptrCast(args.executable.?)).*;
    const lists: [*][*]?*c.PjrtBuffer = @ptrCast(@alignCast(args.output_lists.?));
    if (kind == 2) {
        std.debug.assert(args.num_args == 3);
        const inputs: [*][*]?*c.PjrtBuffer = @ptrCast(@alignCast(args.argument_lists.?));
        std.debug.assert(std.mem.bytesToValue(f32, dataBuffer(inputs[0][1]).bytes[0..4]) == 1);
        std.debug.assert(std.mem.bytesToValue(f32, dataBuffer(inputs[0][2]).bytes[0..4]) == 1);
    }
    const logits = if (kind == 1) [_]f32{ 0, 1 } else [_]f32{ 1, 0 };
    lists[0][0] = newBuffer(std.mem.sliceAsBytes(&logits));
    if (kind != 0) {
        const kv = [_]f32{@floatFromInt(kind)};
        lists[0][1] = newBuffer(std.mem.sliceAsBytes(&kv));
        lists[0][2] = newBuffer(std.mem.sliceAsBytes(&kv));
    }
    counters[10] += 1;
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
    value._fns[22] = @ptrCast(&upload);
    value._fns[44] = @ptrCast(&numOutputs);
    value._fns[40] = @ptrCast(&destroyExecutable);
    value._fns[49] = @ptrCast(&serialize);
    value._fns[50] = @ptrCast(&destroyLoaded);
    value._fns[51] = @ptrCast(&executable);
    value._fns[55] = @ptrCast(&execute);
    value._fns[56] = @ptrCast(&deserialize);
    value._fns[58] = @ptrCast(&destroyBuffer);
    value._fns[64] = @ptrCast(&bufferSize);
    value._fns[70] = @ptrCast(&download);
    value._fns[96] = @ptrCast(&stats);
    break :blk value;
};
pub export fn GetPjrtApi() *const c.Api {
    return &api;
}
