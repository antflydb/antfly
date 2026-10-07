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

//! Optional Linux x86 GNU acceleration. Keep the library resident for the
//! process lifetime: published function pointers must never outlive it.
const platform = @import("antfly_platform");
const std = @import("std");

const builtin = @import("builtin");
const options = @import("build_options");
const linalg = @import("inference_linalg");

pub const enabled = builtin.os.tag == .linux and builtin.cpu.arch == .x86_64 and
    builtin.abi == .gnu and builtin.link_libc and !options.enable_system_blas and
    @hasDecl(options, "enable_runtime_openblas") and options.enable_runtime_openblas;

pub const Sgemm = *const fn (c_int, c_int, c_int, c_int, c_int, c_int, f32, [*]const f32, c_int, [*]const f32, c_int, f32, [*]f32, c_int) callconv(.c) void;
const Api = struct { lib: platform.DynLib, sgemm: Sgemm, threads: usize };
var api: ?Api = null;
var initialized: std.atomic.Value(bool) = .init(false);
var mutex: std.Io.Mutex = .init;

fn env(name: [*:0]const u8) ?[]const u8 {
    return if (std.c.getenv(name)) |value| std.mem.span(value) else null;
}

fn threadCount(budget: usize, requested: ?[]const u8) !usize {
    const count = if (requested) |value| try std.fmt.parseInt(usize, value, 10) else @min(budget, 2);
    if (count == 0) return error.InvalidOpenBlasThreads;
    return @max(1, @min(count, budget));
}

fn load() !Api {
    var lib = platform.DynLib.open(env("ANTFLY_OPENBLAS_LIBRARY") orelse "libopenblas.so.0") catch return error.OpenBlasNotFound;
    errdefer lib.close();
    const get_config = lib.lookup(*const fn () callconv(.c) [*:0]const u8, "openblas_get_config") orelse return error.InvalidOpenBlasLibrary;
    // CBLAS uses c_int dimensions here. An ILP64 library has a different ABI.
    if (std.mem.indexOf(u8, std.mem.span(get_config()), "USE64BITINT") != null) return error.OpenBlasIntegerAbiMismatch;
    const get_parallel = lib.lookup(*const fn () callconv(.c) c_int, "openblas_get_parallel") orelse return error.InvalidOpenBlasLibrary;
    // Support the pthread build we qualify and ship. Sequential builds need
    // separate locking guarantees; OpenMP has a different scheduling policy.
    if (get_parallel() != 1) return error.UnsupportedOpenBlasThreading;
    const sgemm_fn = lib.lookup(Sgemm, "cblas_sgemm") orelse return error.InvalidOpenBlasLibrary;
    const set_threads = lib.lookup(*const fn (c_int) callconv(.c) void, "openblas_set_num_threads") orelse return error.InvalidOpenBlasLibrary;
    const get_threads = lib.lookup(*const fn () callconv(.c) c_int, "openblas_get_num_threads") orelse return error.InvalidOpenBlasLibrary;
    const threads = try threadCount(linalg.pool.cachedCpuCount(), env("OPENBLAS_NUM_THREADS"));
    set_threads(@intCast(threads));
    const actual = get_threads();
    if (actual < 1 or actual > threads) return error.OpenBlasThreadLimitFailed;
    return .{ .lib = lib, .sgemm = sgemm_fn, .threads = @intCast(actual) };
}

pub fn available() bool {
    if (comptime !enabled) return false;
    if (!initialized.load(.acquire)) {
        const io = platform.Io.Threaded.global_single_threaded.io();
        mutex.lockUncancelable(io);
        defer mutex.unlock(io);
        if (!initialized.load(.monotonic)) {
            const policy = env("ANTFLY_INFERENCE_BLAS") orelse "auto";
            const required = std.mem.eql(u8, policy, "openblas");
            if (!std.mem.eql(u8, policy, "off")) {
                if (!required and !std.mem.eql(u8, policy, "auto"))
                    std.debug.panic("invalid ANTFLY_INFERENCE_BLAS={s} (expected auto, off, openblas)", .{policy});
                api = load() catch |err| blk: {
                    if (required) std.debug.panic("OpenBLAS required: {s}", .{@errorName(err)});
                    if (err != error.OpenBlasNotFound) std.log.warn("OpenBLAS unavailable ({s}); using native CPU kernels", .{@errorName(err)});
                    break :blk null;
                };
                if (api) |loaded| std.log.info("native CPU acceleration: OpenBLAS ({d} threads)", .{loaded.threads});
            }
            initialized.store(true, .release);
        }
    }
    return api != null;
}

pub fn threadLimit() ?usize {
    return if (available()) api.?.threads else null;
}

pub fn sgemm(layout: c_int, transa: c_int, transb: c_int, m: c_int, n: c_int, k: c_int, alpha: f32, a: [*]const f32, lda: c_int, b: [*]const f32, ldb: c_int, beta: f32, c: [*]f32, ldc: c_int) void {
    std.debug.assert(available());
    // BLAS requires positive leading dimensions even for empty matrices or
    // k == 0. Native callers legitimately pass zero strides in those cases.
    api.?.sgemm(layout, transa, transb, m, n, k, alpha, a, @max(lda, 1), b, @max(ldb, 1), beta, c, @max(ldc, 1));
}

test "OpenBLAS thread budget defaults to two and honors tighter limits" {
    try std.testing.expectEqual(@as(usize, 2), try threadCount(8, null));
    try std.testing.expectEqual(@as(usize, 1), try threadCount(1, null));
    try std.testing.expectEqual(@as(usize, 1), try threadCount(8, "1"));
    try std.testing.expectEqual(@as(usize, 3), try threadCount(3, "32"));
    try std.testing.expectError(error.InvalidOpenBlasThreads, threadCount(8, "0"));
    try std.testing.expectError(error.InvalidCharacter, threadCount(8, "invalid"));
}
