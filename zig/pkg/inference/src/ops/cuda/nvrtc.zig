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

const std = @import("std");
const builtin = @import("builtin");

pub const max_source_bytes = 16 * 1024 * 1024;
pub const max_ptx_bytes = 64 * 1024 * 1024;
pub const max_log_bytes = 64 * 1024;
pub const max_extra_options = 63;
pub const max_option_bytes = 1024;

pub const NvrtcResult = c_int;
pub const NVRTC_SUCCESS: NvrtcResult = 0;
const NvrtcProgram = ?*anyopaque;

pub const Error = error{
    NvrtcUnavailable,
    NvrtcSymbolMissing,
    NvrtcCallFailed,
    InvalidComputeCapability,
    InvalidSource,
    SourceTooLarge,
    TooManyOptions,
    InvalidOption,
    OptionTooLong,
    PtxTooLarge,
    InvalidPtx,
};

pub const ComputeCapability = struct {
    major: u8,
    minor: u8,

    pub fn option(self: ComputeCapability, buffer: []u8) Error![:0]u8 {
        if (self.major == 0 or self.minor > 9) return error.InvalidComputeCapability;
        return std.fmt.bufPrintSentinel(buffer, "--gpu-architecture=compute_{d}{d}", .{ self.major, self.minor }, 0) catch
            return error.InvalidComputeCapability;
    }
};

pub const Compilation = struct {
    status: NvrtcResult,
    /// NVRTC includes the trailing NUL required by cuModuleLoadDataEx.
    ptx: ?[]u8,
    log: []u8,
    message: []u8,

    pub fn succeeded(self: Compilation) bool {
        return self.status == NVRTC_SUCCESS and self.ptx != null;
    }

    pub fn deinit(self: *Compilation, allocator: std.mem.Allocator) void {
        if (self.ptx) |ptx| allocator.free(ptx);
        allocator.free(self.log);
        allocator.free(self.message);
        self.* = undefined;
    }
};

const Table = struct {
    version: *const fn (*c_int, *c_int) callconv(.c) NvrtcResult,
    createProgram: *const fn (
        *NvrtcProgram,
        [*:0]const u8,
        [*:0]const u8,
        c_int,
        ?[*]const [*:0]const u8,
        ?[*]const [*:0]const u8,
    ) callconv(.c) NvrtcResult,
    destroyProgram: *const fn (*NvrtcProgram) callconv(.c) NvrtcResult,
    compileProgram: *const fn (NvrtcProgram, c_int, ?[*]const [*:0]const u8) callconv(.c) NvrtcResult,
    getPtxSize: *const fn (NvrtcProgram, *usize) callconv(.c) NvrtcResult,
    getPtx: *const fn (NvrtcProgram, [*]u8) callconv(.c) NvrtcResult,
    getProgramLogSize: *const fn (NvrtcProgram, *usize) callconv(.c) NvrtcResult,
    getProgramLog: *const fn (NvrtcProgram, [*]u8) callconv(.c) NvrtcResult,
    getErrorString: *const fn (NvrtcResult) callconv(.c) ?[*:0]const u8,
};

pub const Nvrtc = struct {
    lib: std.DynLib,
    fns: Table,
    version_major: u16,
    version_minor: u16,

    pub fn open() Error!Nvrtc {
        var lib = try openAny(libraryNames());
        errdefer lib.close();

        const fns: Table = .{
            .version = try lookup(&lib, @TypeOf(@as(Table, undefined).version), "nvrtcVersion"),
            .createProgram = try lookup(&lib, @TypeOf(@as(Table, undefined).createProgram), "nvrtcCreateProgram"),
            .destroyProgram = try lookup(&lib, @TypeOf(@as(Table, undefined).destroyProgram), "nvrtcDestroyProgram"),
            .compileProgram = try lookup(&lib, @TypeOf(@as(Table, undefined).compileProgram), "nvrtcCompileProgram"),
            .getPtxSize = try lookup(&lib, @TypeOf(@as(Table, undefined).getPtxSize), "nvrtcGetPTXSize"),
            .getPtx = try lookup(&lib, @TypeOf(@as(Table, undefined).getPtx), "nvrtcGetPTX"),
            .getProgramLogSize = try lookup(&lib, @TypeOf(@as(Table, undefined).getProgramLogSize), "nvrtcGetProgramLogSize"),
            .getProgramLog = try lookup(&lib, @TypeOf(@as(Table, undefined).getProgramLog), "nvrtcGetProgramLog"),
            .getErrorString = try lookup(&lib, @TypeOf(@as(Table, undefined).getErrorString), "nvrtcGetErrorString"),
        };
        var major: c_int = 0;
        var minor: c_int = 0;
        if (fns.version(&major, &minor) != NVRTC_SUCCESS) return error.NvrtcCallFailed;
        return .{
            .lib = lib,
            .fns = fns,
            .version_major = std.math.cast(u16, major) orelse return error.NvrtcCallFailed,
            .version_minor = std.math.cast(u16, minor) orelse return error.NvrtcCallFailed,
        };
    }

    pub fn deinit(self: *Nvrtc) void {
        self.lib.close();
        self.* = undefined;
    }

    pub fn compile(
        self: *const Nvrtc,
        allocator: std.mem.Allocator,
        name: [:0]const u8,
        source: []const u8,
        capability: ComputeCapability,
        extra_options: []const [:0]const u8,
    ) (Error || std.mem.Allocator.Error)!Compilation {
        return compileWithTable(&self.fns, allocator, name, source, capability, extra_options);
    }
};

fn compileWithTable(
    fns: *const Table,
    allocator: std.mem.Allocator,
    name: [:0]const u8,
    source: []const u8,
    capability: ComputeCapability,
    extra_options: []const [:0]const u8,
) (Error || std.mem.Allocator.Error)!Compilation {
    if (source.len > max_source_bytes) return error.SourceTooLarge;
    if (std.mem.indexOfScalar(u8, source, 0) != null) return error.InvalidSource;
    if (extra_options.len > max_extra_options) return error.TooManyOptions;
    for (extra_options) |option| {
        if (option.len > max_option_bytes) return error.OptionTooLong;
        if (std.mem.indexOfScalar(u8, option, 0) != null) return error.InvalidOption;
    }

    var arch_buffer: [48]u8 = undefined;
    const arch_option = try capability.option(&arch_buffer);
    var option_ptrs: [max_extra_options + 1][*:0]const u8 = undefined;
    option_ptrs[0] = arch_option.ptr;
    for (extra_options, 1..) |option, index| option_ptrs[index] = option.ptr;

    const source_z = try allocator.dupeSentinel(u8, source, 0);
    defer allocator.free(source_z);
    var program: NvrtcProgram = null;
    const create_status = fns.createProgram(&program, source_z.ptr, name.ptr, 0, null, null);
    if (create_status != NVRTC_SUCCESS) {
        if (program != null) _ = fns.destroyProgram(&program);
        return failure(fns, allocator, create_status, try allocator.dupe(u8, ""));
    }
    defer _ = fns.destroyProgram(&program);

    const compile_status = fns.compileProgram(program, @intCast(extra_options.len + 1), &option_ptrs);
    const log_result = try readLog(fns, allocator, program);
    if (log_result.status != NVRTC_SUCCESS) return failure(fns, allocator, log_result.status, log_result.bytes);
    if (compile_status != NVRTC_SUCCESS) return failure(fns, allocator, compile_status, log_result.bytes);

    var ptx_size: usize = 0;
    const size_status = fns.getPtxSize(program, &ptx_size);
    if (size_status != NVRTC_SUCCESS) return failure(fns, allocator, size_status, log_result.bytes);
    if (ptx_size == 0) {
        allocator.free(log_result.bytes);
        return error.InvalidPtx;
    }
    if (ptx_size > max_ptx_bytes) {
        allocator.free(log_result.bytes);
        return error.PtxTooLarge;
    }

    const ptx = allocator.alloc(u8, ptx_size) catch |err| {
        allocator.free(log_result.bytes);
        return err;
    };
    const ptx_status = fns.getPtx(program, ptx.ptr);
    if (ptx_status != NVRTC_SUCCESS) {
        allocator.free(ptx);
        return failure(fns, allocator, ptx_status, log_result.bytes);
    }
    if (ptx[ptx.len - 1] != 0) {
        allocator.free(ptx);
        allocator.free(log_result.bytes);
        return error.InvalidPtx;
    }
    const message = allocator.dupe(u8, "") catch |err| {
        allocator.free(ptx);
        allocator.free(log_result.bytes);
        return err;
    };
    return .{
        .status = NVRTC_SUCCESS,
        .ptx = ptx,
        .log = log_result.bytes,
        .message = message,
    };
}

const LogResult = struct {
    status: NvrtcResult,
    bytes: []u8,
};

fn readLog(fns: *const Table, allocator: std.mem.Allocator, program: NvrtcProgram) !LogResult {
    var size: usize = 0;
    const size_status = fns.getProgramLogSize(program, &size);
    if (size_status != NVRTC_SUCCESS) return .{
        .status = size_status,
        .bytes = try allocator.dupe(u8, ""),
    };
    if (size == 0) return .{
        .status = NVRTC_SUCCESS,
        .bytes = try allocator.dupe(u8, ""),
    };
    if (size > max_log_bytes) return .{
        .status = NVRTC_SUCCESS,
        .bytes = try allocator.dupe(u8, "NVRTC log omitted: exceeds 64 KiB"),
    };

    const buffer = try allocator.alloc(u8, size);
    const status = fns.getProgramLog(program, buffer.ptr);
    if (status != NVRTC_SUCCESS) {
        allocator.free(buffer);
        return .{ .status = status, .bytes = try allocator.dupe(u8, "") };
    }
    const end = std.mem.indexOfScalar(u8, buffer, 0) orelse buffer.len;
    if (end == buffer.len) return .{ .status = NVRTC_SUCCESS, .bytes = buffer };
    const trimmed = allocator.dupe(u8, buffer[0..end]) catch |err| {
        allocator.free(buffer);
        return err;
    };
    allocator.free(buffer);
    return .{ .status = NVRTC_SUCCESS, .bytes = trimmed };
}

fn failure(fns: *const Table, allocator: std.mem.Allocator, status: NvrtcResult, log: []u8) !Compilation {
    errdefer allocator.free(log);
    const raw = if (fns.getErrorString(status)) |message| std.mem.span(message) else "unknown NVRTC error";
    const message = try allocator.dupe(u8, raw[0..@min(raw.len, 1024)]);
    return .{ .status = status, .ptx = null, .log = log, .message = message };
}

fn libraryNames() []const []const u8 {
    return switch (builtin.os.tag) {
        .windows => &.{
            "nvrtc64_130_0.dll",
            "nvrtc64_120_0.dll",
            "nvrtc64_112_0.dll",
        },
        else => &.{
            "libnvrtc.so.13",
            "libnvrtc.so.12",
            "libnvrtc.so.11.2",
            "libnvrtc.so",
        },
    };
}

fn openAny(names: []const []const u8) Error!std.DynLib {
    for (names) |name| {
        if (std.DynLib.open(name)) |lib| return lib else |_| {}
    }
    return error.NvrtcUnavailable;
}

fn lookup(lib: *std.DynLib, comptime T: type, name: [:0]const u8) Error!T {
    return lib.lookup(T, name) orelse error.NvrtcSymbolMissing;
}

test "nvrtc: compute capability option" {
    var buffer: [48]u8 = undefined;
    try std.testing.expectEqualStrings(
        "--gpu-architecture=compute_89",
        try (ComputeCapability{ .major = 8, .minor = 9 }).option(&buffer),
    );
    try std.testing.expectError(
        error.InvalidComputeCapability,
        (ComputeCapability{ .major = 9, .minor = 10 }).option(&buffer),
    );
}

test "nvrtc: unavailable library is explicit" {
    try std.testing.expectError(
        error.NvrtcUnavailable,
        openAny(&.{"libantfly_nvrtc_intentionally_missing.so"}),
    );
}

test "nvrtc: rejects unsafe compiler options before invoking NVRTC" {
    const unused: Table = undefined;
    const interior_nul: [:0]const u8 = "--bad\x00option";
    try std.testing.expectError(
        error.InvalidOption,
        compileWithTable(
            &unused,
            std.testing.allocator,
            "safe.cu",
            "extern \"C\" __global__ void safe() {}",
            .{ .major = 8, .minor = 9 },
            &.{interior_nul},
        ),
    );
    const too_long: [:0]const u8 = &@as([max_option_bytes + 1:0]u8, @splat('x'));
    try std.testing.expectError(
        error.OptionTooLong,
        compileWithTable(
            &unused,
            std.testing.allocator,
            "safe.cu",
            "extern \"C\" __global__ void safe() {}",
            .{ .major = 8, .minor = 9 },
            &.{too_long},
        ),
    );
}

test "nvrtc: compiler failures retain bounded diagnostics" {
    const Fake = struct {
        fn version(major: *c_int, minor: *c_int) callconv(.c) NvrtcResult {
            major.* = 12;
            minor.* = 0;
            return NVRTC_SUCCESS;
        }
        fn createProgram(program: *NvrtcProgram, _: [*:0]const u8, _: [*:0]const u8, _: c_int, _: ?[*]const [*:0]const u8, _: ?[*]const [*:0]const u8) callconv(.c) NvrtcResult {
            program.* = @ptrFromInt(1);
            return NVRTC_SUCCESS;
        }
        fn destroyProgram(program: *NvrtcProgram) callconv(.c) NvrtcResult {
            program.* = null;
            return NVRTC_SUCCESS;
        }
        fn compileProgram(_: NvrtcProgram, _: c_int, _: ?[*]const [*:0]const u8) callconv(.c) NvrtcResult {
            return 6;
        }
        fn getPtxSize(_: NvrtcProgram, size: *usize) callconv(.c) NvrtcResult {
            size.* = 0;
            return NVRTC_SUCCESS;
        }
        fn getPtx(_: NvrtcProgram, _: [*]u8) callconv(.c) NvrtcResult {
            return NVRTC_SUCCESS;
        }
        fn getProgramLogSize(_: NvrtcProgram, size: *usize) callconv(.c) NvrtcResult {
            size.* = 5;
            return NVRTC_SUCCESS;
        }
        fn getProgramLog(_: NvrtcProgram, output: [*]u8) callconv(.c) NvrtcResult {
            @memcpy(output[0..5], "oops\x00");
            return NVRTC_SUCCESS;
        }
        fn getErrorString(_: NvrtcResult) callconv(.c) ?[*:0]const u8 {
            return "fake NVRTC compilation error";
        }
    };
    const fake: Table = .{
        .version = &Fake.version,
        .createProgram = &Fake.createProgram,
        .destroyProgram = &Fake.destroyProgram,
        .compileProgram = &Fake.compileProgram,
        .getPtxSize = &Fake.getPtxSize,
        .getPtx = &Fake.getPtx,
        .getProgramLogSize = &Fake.getProgramLogSize,
        .getProgramLog = &Fake.getProgramLog,
        .getErrorString = &Fake.getErrorString,
    };

    var result = try compileWithTable(
        &fake,
        std.testing.allocator,
        "broken.cu",
        "extern \"C\" __global__ void broken() {}",
        .{ .major = 8, .minor = 9 },
        &.{},
    );
    defer result.deinit(std.testing.allocator);
    try std.testing.expect(!result.succeeded());
    try std.testing.expectEqual(@as(NvrtcResult, 6), result.status);
    try std.testing.expectEqualStrings("oops", result.log);
    try std.testing.expectEqualStrings("fake NVRTC compilation error", result.message);
}
