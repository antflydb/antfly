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
const build_options = @import("build_options");

const print = std.debug.print;

const cuda_context = if (build_options.enable_cuda) @import("ops/cuda/context.zig") else struct {};
const cuda_compute = if (build_options.enable_cuda) @import("ops/cuda/cuda_compute.zig") else struct {};
const cuda_kernels = if (build_options.enable_cuda) @import("ops/cuda/kernels.zig") else struct {};

pub fn main(allocator: std.mem.Allocator, _: std.Io, args: []const []const u8) !void {
    var smoke = false;
    for (args) |arg| {
        if (std.mem.eql(u8, arg, "--smoke")) {
            smoke = true;
        } else if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            printUsage();
            return;
        } else {
            print("unknown cuda-info option: {s}\n", .{arg});
            printUsage();
            std.process.exit(1);
        }
    }

    if (!build_options.enable_cuda) {
        print("cuda: unavailable\nreason: backend not built; rebuild with -Dcuda=true\n", .{});
        std.process.exit(1);
    }

    if (comptime build_options.enable_cuda) {
        const info = cuda_context.probeDefault() catch |err| {
            print("cuda: unavailable\nreason: {s}\n", .{@errorName(err)});
            std.process.exit(1);
        };

        print("cuda: available\n", .{});
        print("driver_version: {d}\n", .{info.driver_version});
        print("device_count: {d}\n", .{info.device_count});
        print("selected_device: {d}\n", .{info.selected_device});
        print("device_name: {s}\n", .{info.nameSlice()});
        print("compute_capability: sm_{d}{d}\n", .{ info.compute_major, info.compute_minor });
        print("artifacts: {s}\n", .{build_options.cuda_artifacts});
        print("cuda_libraries: {s}\n", .{build_options.cuda_libraries});
        var compute = cuda_compute.CudaCompute.init(allocator) catch |err| {
            print("capabilities: unavailable\nreason: {s}\n", .{@errorName(err)});
            std.process.exit(1);
        };
        defer compute.deinit();
        print("cublas_available: {}\n", .{compute.hasCublas()});
        print("cublaslt_available: {}\n", .{compute.hasCublasLt()});
        print("dense_library_acceleration: {}\n", .{compute.hasDenseLibraryAcceleration()});
        print("capability_clipclap: {}\n", .{compute.supportsProfile(.clipclap)});
        print("capability_deberta_reranker: {}\n", .{compute.supportsProfile(.deberta_reranker)});
        print("capability_gliner2: {}\n", .{compute.supportsProfile(.gliner2)});

        if (smoke) {
            cuda_kernels.smokeFill(allocator) catch |err| {
                print("smoke: fill_f32 failed\nreason: {s}\n", .{@errorName(err)});
                std.process.exit(1);
            };
            print("smoke: fill_f32 ok\n", .{});
            cuda_kernels.smokeDenseF32(allocator) catch |err| {
                print("smoke: dense_f32 failed\nreason: {s}\n", .{@errorName(err)});
                std.process.exit(1);
            };
            print("smoke: dense_f32 ok\n", .{});
            cuda_kernels.smokeDense16(allocator) catch |err| {
                print("smoke: dense16_f32 failed\nreason: {s}\n", .{@errorName(err)});
                std.process.exit(1);
            };
            print("smoke: dense16_f32 ok\n", .{});
            if (compute.hasDenseLibraryAcceleration()) {
                compute.smokeDenseLt(allocator) catch |err| {
                    print("smoke: cublaslt_dense16 failed\nreason: {s}\n", .{@errorName(err)});
                    std.process.exit(1);
                };
                print("smoke: cublaslt_dense16 ok\n", .{});
            } else {
                print("smoke: cublaslt_dense16 skipped\n", .{});
            }
            cuda_kernels.smokeQ8_0(allocator) catch |err| {
                print("smoke: q8_0_f32 failed\nreason: {s}\n", .{@errorName(err)});
                std.process.exit(1);
            };
            print("smoke: q8_0_f32 ok\n", .{});
            cuda_kernels.smokeQ4_0(allocator) catch |err| {
                print("smoke: q4_0_f32 failed\nreason: {s}\n", .{@errorName(err)});
                std.process.exit(1);
            };
            print("smoke: q4_0_f32 ok\n", .{});
            cuda_kernels.smokeQ4_K(allocator) catch |err| {
                print("smoke: q4_k_f32 failed\nreason: {s}\n", .{@errorName(err)});
                std.process.exit(1);
            };
            print("smoke: q4_k_f32 ok\n", .{});
        }

        const dense_stats = compute.denseLtStats();
        print("cublaslt_matmul_enabled: {}\n", .{dense_stats.enabled});
        print("cublaslt_workspace_bytes: {d}\n", .{dense_stats.workspace_bytes});
        print("cublaslt_min_ops: {d}\n", .{dense_stats.min_ops});
        print("cublaslt_plan_cache_entries: {d}\n", .{dense_stats.plan_cache_entries});
        print("cublaslt_matmul_attempts: {d}\n", .{dense_stats.attempts});
        print("cublaslt_matmul_successes: {d}\n", .{dense_stats.successes});
        print("cublaslt_matmul_fallbacks: {d}\n", .{dense_stats.fallbacks});
        print("cublaslt_heuristic_misses: {d}\n", .{dense_stats.heuristic_misses});
    }
}

fn printUsage() void {
    print(
        \\usage: antfly inference cuda-info [--smoke]
        \\
        \\  --smoke   Run embedded CUDA artifact smoke checks, including dense16 and quantized kernels.
        \\
    , .{});
}
