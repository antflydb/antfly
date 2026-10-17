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

//! Load Antfly's native, database-free inference runtime through its stable
//! C ABI. No Rust, subprocess, HTTP listener, or database is involved.
const std = @import("std");
const Options = @import("options.zig").Options;
const Slice = extern struct { ptr: ?[*]const u8 = null, len: usize = 0 };
const Buffer = extern struct { ptr: ?[*]u8 = null, len: usize = 0 };
pub const InferenceOptions = extern struct {
    abi_size: u32 = 0,
    flags: u32 = 0,
    models_dir: Slice = .{},
    host_budget_mb: u32 = 0,
    backend_budget_mb: u32 = 0,
    process_memory_budget_mb: u32 = 0,
    combined_budget_mb: u32 = 0,
    kv_budget_mb: u32 = 0,
    scratch_budget_mb: u32 = 0,
    call_timeout_ms: u64 = 0,
    reserved: [8]u64 = @splat(0),
};
pub const Local = struct {
    library: @import("native_library.zig").Library,
    handle: ?*anyopaque,
    close_fn: *const fn (?*anyopaque) callconv(.c) void,
    decide_fn: *const fn (?*anyopaque, Slice, *Buffer) callconv(.c) c_int,
    free_fn: *const fn (*Buffer) callconv(.c) void,
    pub fn init(o: *const Options) !Local {
        const target = @import("builtin");
        // Zig's static-musl ElfDynLib maps symbols but does not relocate a
        // general shared library. Refuse it before calling unrelocated code;
        // native inference requires the operating system's dynamic loader.
        if (comptime target.os.tag == .linux and target.abi == .musl and target.link_mode == .static) return error.DynamicInferenceRequiresDynamicLibc;
        const path = o.library_path orelse (if (@import("builtin").os.tag == .macos) "libantfly.dylib" else if (@import("builtin").os.tag == .windows) "antfly.dll" else "libantfly.so");
        var library = try @import("native_library.zig").Library.open(path);
        errdefer library.close();
        const abi = library.lookup(*const fn () callconv(.c) u32, "antfly_abi_version") orelse return error.IncompatibleInferenceLibrary;
        const size = library.lookup(*const fn () callconv(.c) u32, "antfly_inference_options_size") orelse return error.IncompatibleInferenceLibrary;
        if (abi() != 3 or size() != @sizeOf(InferenceOptions)) return error.IncompatibleInferenceLibrary;
        const initialize = library.lookup(*const fn (*InferenceOptions) callconv(.c) c_int, "antfly_inference_options_init") orelse return error.IncompatibleInferenceLibrary;
        const open = library.lookup(*const fn (*const InferenceOptions, *?*anyopaque) callconv(.c) c_int, "antfly_inference_open") orelse return error.IncompatibleInferenceLibrary;
        const close = library.lookup(*const fn (?*anyopaque) callconv(.c) void, "antfly_inference_close") orelse return error.IncompatibleInferenceLibrary;
        const invoke_decide = library.lookup(*const fn (?*anyopaque, Slice, *Buffer) callconv(.c) c_int, "antfly_inference_decide_json") orelse return error.IncompatibleInferenceLibrary;
        const free = library.lookup(*const fn (*Buffer) callconv(.c) void, "antfly_buffer_free") orelse return error.IncompatibleInferenceLibrary;
        var config: InferenceOptions = undefined;
        if (initialize(&config) != 0) return error.IncompatibleInferenceLibrary;
        if (o.models_dir) |dir| config.models_dir = .{ .ptr = dir.ptr, .len = dir.len };
        config.call_timeout_ms = o.timeout_ms;
        var handle: ?*anyopaque = null;
        if (open(&config, &handle) != 0 or handle == null) return error.LocalInferenceStartupFailed;
        return .{ .library = library, .handle = handle, .close_fn = close, .decide_fn = invoke_decide, .free_fn = free };
    }
    pub fn deinit(self: *Local) void {
        self.close_fn(self.handle);
        self.library.close();
    }
    pub fn decide(self: *Local, a: std.mem.Allocator, body: []const u8) ![]u8 {
        var buffer: Buffer = .{};
        defer self.free_fn(&buffer);
        const code = self.decide_fn(self.handle, .{ .ptr = body.ptr, .len = body.len }, &buffer);
        if (code != 0) return error.LocalDecisionFailed;
        const ptr = buffer.ptr orelse return error.EmptyDecisionResponse;
        if (buffer.len > 4 * 1024 * 1024) return error.DecisionResponseTooLarge;
        return a.dupe(u8, ptr[0..buffer.len]);
    }
};
test "inference option ABI layout matches antfly.h v3" {
    try std.testing.expectEqual(@as(usize, 120), @sizeOf(InferenceOptions));
}
