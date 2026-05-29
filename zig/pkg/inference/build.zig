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
const runtime_build = @import("build/runtime.zig");
const finetune_common = @import("build/finetune/common.zig");
const finetune_tests = @import("build/finetune/tests.zig");
const finetune_tools = @import("build/finetune/tools.zig");
const finetune_workflows = @import("build/finetune/workflows.zig");

fn resolveSharedLibRoot(b: *std.Build) []const u8 {
    return b.option(
        []const u8,
        "shared-lib-root",
        "Path to the monorepo root that provides shared generic Zig libraries used by Antfly inference (defaults to ../..)",
    ) orelse b.option(
        []const u8,
        "antfly-root",
        "Deprecated alias for -Dshared-lib-root",
    ) orelse "../..";
}

fn selectTestFilters(b: *std.Build, default_filters: []const []const u8) []const []const u8 {
    const args = b.args orelse return default_filters;
    if (args.len == 0) return default_filters;
    if (std.mem.eql(u8, args[0], "--test-filter")) {
        if (args.len <= 1) return default_filters;
        return args[1..];
    }
    return args;
}

fn defaultOnnxRuntimeRoot(b: *std.Build, target: std.Build.ResolvedTarget) []const u8 {
    const platform_str = switch (target.result.os.tag) {
        .macos => "darwin",
        .linux => "linux",
        else => "unknown",
    };
    const arch_str = switch (target.result.cpu.arch) {
        .aarch64 => "arm64",
        .x86_64 => "amd64",
        else => "unknown",
    };
    return b.fmt("onnxruntime/{s}-{s}", .{ platform_str, arch_str });
}

fn mlxRootAvailable(b: *std.Build, target: std.Build.ResolvedTarget, root: []const u8) bool {
    if (target.result.os.tag != .macos) return false;
    const header = b.fmt("{s}/include/mlx/c/mlx.h", .{root});
    const library = b.fmt("{s}/lib/libmlxc.dylib", .{root});
    return pathExists(b, header) and pathExists(b, library);
}

fn defaultMlxRoot(b: *std.Build, target: std.Build.ResolvedTarget) ?[]const u8 {
    const roots = [_][]const u8{
        "/opt/homebrew",
        "/opt/homebrew/opt/mlx-c",
        "/usr/local",
        "/usr/local/opt/mlx-c",
        "/usr",
    };
    for (roots) |root| {
        if (mlxRootAvailable(b, target, root)) return root;
    }
    return null;
}

fn pathExists(b: *std.Build, path: []const u8) bool {
    const io = b.graph.io;
    std.Io.Dir.cwd().access(io, path, .{}) catch return false;
    return true;
}

fn addRootedLibraryPaths(b: *std.Build, module: *std.Build.Module, root: []const u8) void {
    module.addIncludePath(.{ .cwd_relative = b.fmt("{s}/include", .{root}) });
    module.addLibraryPath(.{ .cwd_relative = b.fmt("{s}/lib", .{root}) });
    module.addRPath(.{ .cwd_relative = b.fmt("{s}/lib", .{root}) });
}

fn configureSystemBlas(
    b: *std.Build,
    module: *std.Build.Module,
    target: std.Build.ResolvedTarget,
    blas_root: ?[]const u8,
) void {
    if (target.result.os.tag == .macos) {
        module.linkFramework("Accelerate", .{});
        return;
    }
    if (blas_root) |root| {
        addRootedLibraryPaths(b, module, root);
    }
    module.linkSystemLibrary("openblas", .{});
}

/// Link the Metal framework and compile the standalone Metal kernels.
/// No MLX dependency — the `.m` file uses Foundation + Metal/MPS.
fn configureMetal(
    b: *std.Build,
    module: *std.Build.Module,
    target: std.Build.ResolvedTarget,
    enable_metal: bool,
) void {
    if (!enable_metal or target.result.os.tag != .macos) return;

    module.linkFramework("Foundation", .{});
    module.linkFramework("Metal", .{});
    module.linkFramework("MetalPerformanceShaders", .{});
    module.linkFramework("MetalPerformanceShadersGraph", .{});
    module.addCSourceFile(.{ .file = b.path("src/backends/metal_kernels.m"), .flags = &.{"-fobjc-arc"} });
}

/// Link libmlxc for the MLX numerics backend. Independent of Metal.
fn configureMlx(
    b: *std.Build,
    module: *std.Build.Module,
    target: std.Build.ResolvedTarget,
    enable_mlx: bool,
    mlx_root: ?[]const u8,
) void {
    if (!enable_mlx or target.result.os.tag != .macos) return;
    if (mlx_root) |root| {
        module.addLibraryPath(.{ .cwd_relative = b.fmt("{s}/lib", .{root}) });
        module.addRPath(.{ .cwd_relative = b.fmt("{s}/lib", .{root}) });
    }
    module.linkSystemLibrary("mlxc", .{});
}

fn configureOnnxRuntime(
    b: *std.Build,
    module: *std.Build.Module,
    enable_onnx: bool,
    onnx_root: []const u8,
) void {
    if (!enable_onnx) return;
    module.addIncludePath(.{ .cwd_relative = b.fmt("{s}/include", .{onnx_root}) });
    module.addLibraryPath(.{ .cwd_relative = b.fmt("{s}/lib", .{onnx_root}) });
    module.addRPath(.{ .cwd_relative = b.fmt("{s}/lib", .{onnx_root}) });
    module.linkSystemLibrary("onnxruntime", .{});
    module.linkSystemLibrary("onnxruntime-genai", .{});
}

fn configureNativeTool(
    b: *std.Build,
    artifact: *std.Build.Step.Compile,
    target: std.Build.ResolvedTarget,
    enable_system_blas: bool,
    blas_root: ?[]const u8,
    enable_mlx: bool,
    mlx_root: ?[]const u8,
    enable_metal: bool,
) void {
    if (enable_system_blas) {
        configureSystemBlas(b, artifact.root_module, target, blas_root);
    }
    configureMetal(b, artifact.root_module, target, enable_metal);
    configureMlx(b, artifact.root_module, target, enable_mlx, mlx_root);
    artifact.root_module.link_libc = true;
}

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});
    const shared_lib_root = resolveSharedLibRoot(b);

    // Backend options
    const enable_wasm = b.option(bool, "wasm", "Build WASM+SIMD module for browser inference") orelse false;
    const enable_webgpu = b.option(bool, "webgpu", "Enable WebGPU acceleration for WASM builds") orelse false;
    const link_libc = b.option(bool, "link-libc", "Link Antfly inference targets against libc") orelse !enable_wasm;
    const wasm_memory_model = b.option([]const u8, "wasm-memory-model", "WASM linear memory model for browser inference: wasm32 or wasm64") orelse "wasm32";
    if (!std.mem.eql(u8, wasm_memory_model, "wasm32") and !std.mem.eql(u8, wasm_memory_model, "wasm64")) {
        @panic("invalid -Dwasm-memory-model (expected wasm32 or wasm64)");
    }
    const onnx_option = b.option(bool, "onnx", "Enable ONNX Runtime backend");
    const enable_onnx = if (enable_wasm or !link_libc) false else (onnx_option orelse false);
    const onnx_root_opt = b.option([]const u8, "onnx-root", "Path to ONNX Runtime root (default: ./onnxruntime/<platform>)");
    const effective_onnx_root = onnx_root_opt orelse defaultOnnxRuntimeRoot(b, target);
    const mlx_root_opt = b.option([]const u8, "mlx-root", "Path to MLX C root with lib/libmlxc.dylib");
    const mlx_option = b.option(bool, "mlx", "Enable MLX backend (macOS only)");
    const mlx_requested = if (enable_wasm or !link_libc) false else (mlx_option orelse false);
    // Metal kernels are independent of MLX, but MLX decoder paths currently
    // dispatch through Metal kernels. Disabling Metal therefore disables MLX.
    const enable_metal = if (enable_wasm or !link_libc) false else (b.option(bool, "metal", "Enable Apple Metal kernels (macOS only)") orelse (mlx_requested or target.result.os.tag == .macos));
    const enable_mlx = enable_metal and mlx_requested;
    const effective_mlx_root = if (enable_mlx)
        mlx_root_opt orelse defaultMlxRoot(b, target)
    else
        mlx_root_opt;
    if (enable_onnx) {
        const onnx_runtime_available = pathExists(b, b.fmt("{s}/include/onnxruntime_c_api.h", .{effective_onnx_root})) and
            pathExists(b, b.fmt("{s}/lib", .{effective_onnx_root}));
        if (!onnx_runtime_available) {
            @panic("-Donnx=true requires an ONNX Runtime install; pass -Donnx-root=<path>");
        }
    }
    if (enable_mlx) {
        const root = effective_mlx_root orelse @panic("-Dmlx=true requires an MLX C install; pass -Dmlx-root=<path>");
        if (!mlxRootAvailable(b, target, root)) {
            @panic("-Dmlx=true requires an MLX C install with include/mlx/c/mlx.h and lib/libmlxc.dylib");
        }
    }
    const enable_cuda = if (enable_wasm or !link_libc) false else (b.option(bool, "cuda", "Enable CUDA backend through the NVIDIA Driver API") orelse false);
    const cuda_artifacts = b.option([]const u8, "cuda-artifacts", "CUDA artifact bundle: portable PTX; fatbin is not implemented yet") orelse "portable";
    if (!std.mem.eql(u8, cuda_artifacts, "portable")) {
        @panic("invalid -Dcuda-artifacts (expected portable; fatbin is not implemented yet)");
    }
    const enable_pjrt = if (enable_wasm or !link_libc) false else (b.option(bool, "pjrt", "Enable PJRT backend (TPU/CPU via dlopen)") orelse false);
    const blas_root_opt = b.option([]const u8, "blas-root", "Path to system BLAS root with include/ and lib/ for non-macOS native acceleration");
    const skip_openapi = b.option(bool, "skip-openapi", "Skip OpenAPI codegen for core-only builds that do not import inference_api") orelse false;
    const enable_native = !enable_wasm;
    // The native CPU backend is always available on native builds. System BLAS
    // remains an optional acceleration layer for hot kernels.
    const system_blas_available = target.result.os.tag == .macos or blas_root_opt != null;
    const enable_system_blas = if (enable_wasm or !link_libc)
        false
    else
        (b.option(bool, "system-blas", "Enable system BLAS acceleration for native CPU math") orelse system_blas_available);
    const blas_root = if (enable_wasm or !enable_system_blas or target.result.os.tag == .macos)
        null
    else
        blas_root_opt;
    const antfly_version = b.option([]const u8, "antfly-version", "Antfly version string") orelse "dev";
    const enable_native_quant_dispatch_stats = b.option(bool, "enable-native-quant-dispatch-stats", "Enable native quant dispatch counters for benchmark diagnostics") orelse false;

    const runtime_graph = runtime_build.create(.{
        .b = b,
        .target = target,
        .optimize = optimize,
        .paths = .{
            .inference_root = "",
            .shared_lib_root = shared_lib_root,
        },
        .register_public_modules = true,
        .backend = .{
            .enable_onnx = enable_onnx,
            .onnx_root = effective_onnx_root,
            .enable_mlx = enable_mlx,
            .mlx_root = effective_mlx_root,
            .enable_metal = enable_metal,
            .enable_cuda = enable_cuda,
            .cuda_artifacts = cuda_artifacts,
            .enable_pjrt = enable_pjrt,
            .enable_native = enable_native,
            .enable_system_blas = enable_system_blas,
            .blas_root = blas_root,
            .enable_wasm = enable_wasm,
            .enable_webgpu = enable_webgpu,
            .wasm_memory_model = wasm_memory_model,
            .link_libc = link_libc,
            .skip_openapi = skip_openapi,
            .inference_version = antfly_version,
            .enable_native_quant_dispatch_stats = enable_native_quant_dispatch_stats,
        },
    });
    const build_options_mod = runtime_graph.build_options_mod;
    const audio_open_corpus_build_options_mod = runtime_graph.audio_open_corpus_build_options_mod;
    const jinja_mod = runtime_graph.jinja_mod;
    const protobuf_mod = runtime_graph.protobuf_mod;
    const ml_mod = runtime_graph.ml_mod;
    const sentencepiece_proto_mod = runtime_graph.sentencepiece_proto_mod;
    const onnx_graph_mod = runtime_graph.onnx_graph_mod;
    const pjrt_mod = runtime_graph.pjrt_mod;
    const httpx_mod = runtime_graph.httpx_mod;
    const platform_mod = runtime_graph.platform_mod;
    const antfly_scraping_mod = runtime_graph.scraping_mod;
    const antfly_jsonschema_mod = runtime_graph.jsonschema_mod;
    const antfly_image_mod = runtime_graph.image_mod;
    const prometheus_mod = runtime_graph.prometheus_mod;
    const structlog_mod = runtime_graph.structlog_mod;
    const inference_api_mod = runtime_graph.inference_api_mod;
    const inference_tokenizer_mod = runtime_graph.inference_tokenizer_mod;
    const inference_hf_tokenizer_mod = runtime_graph.inference_hf_tokenizer_mod;
    const inference_linalg_mod = runtime_graph.inference_linalg_mod;
    const inference_fixed_tokenizer_data_mod = runtime_graph.inference_fixed_tokenizer_data_mod;
    const inference_audio_mod = runtime_graph.inference_audio_mod;
    const inference_chunker_mod = runtime_graph.inference_chunker_mod;
    const generating_openapi_mod = runtime_graph.generating_openapi_mod;
    const client_mod = runtime_graph.inference_client_mod;
    const inference_internal_mod = runtime_graph.inference_internal_mod;

    const exe = runtime_build.addStandaloneExecutable(b, runtime_graph, target, optimize, "", link_libc);

    const run_exe = b.addRunArtifact(exe);
    run_exe.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_exe.addArgs(args);
    }
    const run_step = b.step("run", "Run the Antfly inference server");
    run_step.dependOn(&run_exe.step);

    const metal_gemma4_prefill_frame_test = b.addSystemCommand(&.{
        "bash",
        "scripts/test_metal_gemma4_prefill_frame.sh",
    });
    metal_gemma4_prefill_frame_test.step.dependOn(b.getInstallStep());
    const metal_gemma4_prefill_frame_test_step = b.step(
        "test-metal-gemma4-prefill-frame",
        "Run the local Metal Gemma4 prefill-frame token-anchor regression test",
    );
    metal_gemma4_prefill_frame_test_step.dependOn(&metal_gemma4_prefill_frame_test.step);

    const metal_gemma4_prefill_block_parity_test = b.addSystemCommand(&.{
        "bash",
        "scripts/test_metal_gemma4_prefill_block_parity.sh",
    });
    metal_gemma4_prefill_block_parity_test.step.dependOn(b.getInstallStep());
    const metal_gemma4_prefill_block_parity_test_step = b.step(
        "test-metal-gemma4-prefill-block-parity",
        "Run the local Metal Gemma4 staged-vs-full-prefill-block parity diagnostic",
    );
    metal_gemma4_prefill_block_parity_test_step.dependOn(&metal_gemma4_prefill_block_parity_test.step);

    const metal_prefill_bucket_bench_exe = b.addExecutable(.{
        .name = "antfly-inference-metal-prefill-buckets-bench",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/bench/metal_prefill_buckets.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_metal_prefill_bucket_bench = b.addRunArtifact(metal_prefill_bucket_bench_exe);
    run_metal_prefill_bucket_bench.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_metal_prefill_bucket_bench.addArgs(args);
    }
    const metal_prefill_bucket_bench_step = b.step(
        "bench-metal-prefill-buckets",
        "Run Metal Gemma4 pp10/pp128/pp512 prefill plus tg16 decode bucket benchmarks",
    );
    metal_prefill_bucket_bench_step.dependOn(&run_metal_prefill_bucket_bench.step);

    const run_finetune = b.addRunArtifact(exe);
    run_finetune.step.dependOn(b.getInstallStep());
    run_finetune.addArg("finetune");
    if (b.args) |args| {
        run_finetune.addArgs(args);
    }
    const finetune_step = b.step("finetune", "Run Antfly inference finetune");
    finetune_step.dependOn(&run_finetune.step);

    const bench_exe = b.addExecutable(.{
        .name = "antfly-inference-paged-attention-bench",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/paged_attention_bench.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    bench_exe.root_module.addImport("build_options", build_options_mod);
    bench_exe.root_module.addImport("inference_linalg", inference_linalg_mod);
    if (enable_system_blas) {
        configureSystemBlas(b, bench_exe.root_module, target, blas_root);
    }
    configureMetal(b, bench_exe.root_module, target, enable_metal);
    configureMlx(b, bench_exe.root_module, target, enable_mlx, effective_mlx_root);
    bench_exe.root_module.link_libc = true;

    const run_bench = b.addRunArtifact(bench_exe);
    if (b.args) |args| {
        run_bench.addArgs(args);
    }
    const bench_step = b.step("bench-paged-attention", "Run the native paged-attention benchmark");
    bench_step.dependOn(&run_bench.step);

    const turboquant_distortion_bench_exe = b.addExecutable(.{
        .name = "antfly-inference-turboquant-distortion-bench",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/turboquant_distortion_bench.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_turboquant_distortion_bench = b.addRunArtifact(turboquant_distortion_bench_exe);
    if (b.args) |args| {
        run_turboquant_distortion_bench.addArgs(args);
    }
    const turboquant_distortion_bench_step = b.step("bench-turboquant-distortion", "Run TurboQuant dot-product distortion benchmark");
    turboquant_distortion_bench_step.dependOn(&run_turboquant_distortion_bench.step);

    const training_bench_exe = b.addExecutable(.{
        .name = "antfly-inference-training-bench",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/bench/training_bench.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const linalg_bench_exe = b.addExecutable(.{
        .name = "antfly-inference-linalg-bench",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/linalg_bench.zig"),
            .target = target,
            .optimize = .ReleaseFast,
        }),
    });
    training_bench_exe.root_module.addImport("build_options", build_options_mod);
    training_bench_exe.root_module.addImport("ml", ml_mod);
    configureNativeTool(b, training_bench_exe, target, enable_system_blas, blas_root, enable_mlx, effective_mlx_root, enable_metal);
    const run_training_bench = b.addRunArtifact(training_bench_exe);
    if (b.args) |args| {
        run_training_bench.addArgs(args);
    }
    const training_bench_step = b.step("bench-training", "Run the native training benchmark");
    training_bench_step.dependOn(&run_training_bench.step);
    linalg_bench_exe.root_module.addImport("inference_linalg", inference_linalg_mod);
    const run_linalg_bench = b.addRunArtifact(linalg_bench_exe);
    if (b.args) |args| {
        run_linalg_bench.addArgs(args);
    }
    const linalg_bench_step = b.step("bench-linalg", "Run the shared linalg benchmark");
    linalg_bench_step.dependOn(&run_linalg_bench.step);

    const clipclap_bench_exe = b.addExecutable(.{
        .name = "antfly-inference-clipclap-kernels-bench",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/clipclap_kernels_bench.zig"),
            .target = target,
            .optimize = .ReleaseFast,
        }),
    });
    clipclap_bench_exe.root_module.addImport("build_options", build_options_mod);
    clipclap_bench_exe.root_module.addImport("inference_linalg", inference_linalg_mod);
    if (enable_system_blas) {
        configureSystemBlas(b, clipclap_bench_exe.root_module, target, blas_root);
    }
    clipclap_bench_exe.root_module.link_libc = true;
    const run_clipclap_bench = b.addRunArtifact(clipclap_bench_exe);
    if (b.args) |args| {
        run_clipclap_bench.addArgs(args);
    }
    const clipclap_bench_step = b.step("bench-clipclap-kernels", "Run the CLIPCLAP native kernel microbenchmark (baseline vs optimized)");
    clipclap_bench_step.dependOn(&run_clipclap_bench.step);

    // GLiNER2 end-to-end native bench: random weights, real eager forward.
    const gliner2_bench_exe = b.addExecutable(.{
        .name = "antfly-inference-gliner2-native-bench",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/bench/gliner2_native.zig"),
            .target = target,
            .optimize = .ReleaseFast,
        }),
    });
    gliner2_bench_exe.root_module.addImport("build_options", build_options_mod);
    gliner2_bench_exe.root_module.addImport("ml", ml_mod);
    gliner2_bench_exe.root_module.addImport("pjrt", pjrt_mod);
    gliner2_bench_exe.root_module.addImport("inference_linalg", inference_linalg_mod);
    gliner2_bench_exe.root_module.addImport("inference_hf_tokenizer", inference_hf_tokenizer_mod);
    gliner2_bench_exe.root_module.addImport("antfly_image", antfly_image_mod);
    gliner2_bench_exe.root_module.addImport("inference_audio", inference_audio_mod);
    gliner2_bench_exe.root_module.addImport("protobuf", protobuf_mod);
    gliner2_bench_exe.root_module.addImport("onnx_graph", onnx_graph_mod);
    gliner2_bench_exe.root_module.addImport("inference_internal", inference_internal_mod);
    configureNativeTool(b, gliner2_bench_exe, target, enable_system_blas, blas_root, enable_mlx, effective_mlx_root, enable_metal);
    configureOnnxRuntime(b, gliner2_bench_exe.root_module, enable_onnx, effective_onnx_root);
    const run_gliner2_bench = b.addRunArtifact(gliner2_bench_exe);
    if (b.args) |args| {
        run_gliner2_bench.addArgs(args);
    }
    const gliner2_bench_step = b.step("bench-gliner2-native", "Run an end-to-end GLiNER2 bench against the native backend with random weights");
    gliner2_bench_step.dependOn(&run_gliner2_bench.step);

    const gliner2_e2e_bench_exe = b.addExecutable(.{
        .name = "antfly-inference-gliner2-e2e-bench",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/bench/gliner2_e2e.zig"),
            .target = target,
            .optimize = .ReleaseFast,
        }),
    });
    gliner2_e2e_bench_exe.root_module.addImport("build_options", build_options_mod);
    gliner2_e2e_bench_exe.root_module.addImport("ml", ml_mod);
    gliner2_e2e_bench_exe.root_module.addImport("pjrt", pjrt_mod);
    gliner2_e2e_bench_exe.root_module.addImport("inference_linalg", inference_linalg_mod);
    gliner2_e2e_bench_exe.root_module.addImport("inference_hf_tokenizer", inference_hf_tokenizer_mod);
    gliner2_e2e_bench_exe.root_module.addImport("antfly_image", antfly_image_mod);
    gliner2_e2e_bench_exe.root_module.addImport("inference_audio", inference_audio_mod);
    gliner2_e2e_bench_exe.root_module.addImport("protobuf", protobuf_mod);
    gliner2_e2e_bench_exe.root_module.addImport("onnx_graph", onnx_graph_mod);
    gliner2_e2e_bench_exe.root_module.addImport("inference_internal", inference_internal_mod);
    configureNativeTool(b, gliner2_e2e_bench_exe, target, enable_system_blas, blas_root, enable_mlx, effective_mlx_root, enable_metal);
    configureOnnxRuntime(b, gliner2_e2e_bench_exe.root_module, enable_onnx, effective_onnx_root);
    const run_gliner2_e2e_bench = b.addRunArtifact(gliner2_e2e_bench_exe);
    if (b.args) |args| {
        run_gliner2_e2e_bench.addArgs(args);
    }
    const gliner2_e2e_bench_step = b.step("bench-gliner2-e2e", "Run real-bundle GLiNER2 recognition E2E benchmarks");
    gliner2_e2e_bench_step.dependOn(&run_gliner2_e2e_bench.step);

    const clipclap_native_bench_exe = b.addExecutable(.{
        .name = "antfly-inference-clipclap-native-bench",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/bench/clipclap_native.zig"),
            .target = target,
            .optimize = .ReleaseFast,
        }),
    });
    clipclap_native_bench_exe.root_module.addImport("build_options", build_options_mod);
    clipclap_native_bench_exe.root_module.addImport("ml", ml_mod);
    clipclap_native_bench_exe.root_module.addImport("pjrt", pjrt_mod);
    clipclap_native_bench_exe.root_module.addImport("inference_linalg", inference_linalg_mod);
    clipclap_native_bench_exe.root_module.addImport("inference_hf_tokenizer", inference_hf_tokenizer_mod);
    clipclap_native_bench_exe.root_module.addImport("antfly_image", antfly_image_mod);
    clipclap_native_bench_exe.root_module.addImport("inference_audio", inference_audio_mod);
    clipclap_native_bench_exe.root_module.addImport("protobuf", protobuf_mod);
    clipclap_native_bench_exe.root_module.addImport("onnx_graph", onnx_graph_mod);
    clipclap_native_bench_exe.root_module.addImport("inference_internal", inference_internal_mod);
    configureNativeTool(b, clipclap_native_bench_exe, target, enable_system_blas, blas_root, enable_mlx, effective_mlx_root, enable_metal);
    configureOnnxRuntime(b, clipclap_native_bench_exe.root_module, enable_onnx, effective_onnx_root);
    const run_clipclap_native_bench = b.addRunArtifact(clipclap_native_bench_exe);
    if (b.args) |args| {
        run_clipclap_native_bench.addArgs(args);
    }
    const clipclap_native_bench_step = b.step("bench-clipclap-native", "Run end-to-end CLIP/CLAP native encoder benches with random quantized weights");
    clipclap_native_bench_step.dependOn(&run_clipclap_native_bench.step);

    const clipclap_e2e_bench_exe = b.addExecutable(.{
        .name = "antfly-inference-clipclap-e2e-bench",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/bench/clipclap_e2e.zig"),
            .target = target,
            .optimize = .ReleaseFast,
        }),
    });
    clipclap_e2e_bench_exe.root_module.addImport("build_options", build_options_mod);
    clipclap_e2e_bench_exe.root_module.addImport("ml", ml_mod);
    clipclap_e2e_bench_exe.root_module.addImport("pjrt", pjrt_mod);
    clipclap_e2e_bench_exe.root_module.addImport("inference_linalg", inference_linalg_mod);
    clipclap_e2e_bench_exe.root_module.addImport("inference_hf_tokenizer", inference_hf_tokenizer_mod);
    clipclap_e2e_bench_exe.root_module.addImport("antfly_image", antfly_image_mod);
    clipclap_e2e_bench_exe.root_module.addImport("inference_audio", inference_audio_mod);
    clipclap_e2e_bench_exe.root_module.addImport("protobuf", protobuf_mod);
    clipclap_e2e_bench_exe.root_module.addImport("onnx_graph", onnx_graph_mod);
    clipclap_e2e_bench_exe.root_module.addImport("inference_internal", inference_internal_mod);
    configureNativeTool(b, clipclap_e2e_bench_exe, target, enable_system_blas, blas_root, enable_mlx, effective_mlx_root, enable_metal);
    configureOnnxRuntime(b, clipclap_e2e_bench_exe.root_module, enable_onnx, effective_onnx_root);
    const run_clipclap_e2e_bench = b.addRunArtifact(clipclap_e2e_bench_exe);
    if (b.args) |args| {
        run_clipclap_e2e_bench.addArgs(args);
    }
    const clipclap_e2e_bench_step = b.step("bench-clipclap-e2e", "Run real-bundle CLIP/CLAP embedding E2E benchmarks");
    clipclap_e2e_bench_step.dependOn(&run_clipclap_e2e_bench.step);

    const audio_bench_exe = b.addExecutable(.{
        .name = "antfly-inference-audio-bench",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/audio_bench.zig"),
            .target = target,
            .optimize = .ReleaseFast,
        }),
    });
    audio_bench_exe.root_module.addImport("build_options", build_options_mod);
    audio_bench_exe.root_module.addImport("inference_audio", inference_audio_mod);
    audio_bench_exe.root_module.link_libc = true;
    const run_audio_bench = b.addRunArtifact(audio_bench_exe);
    if (b.args) |args| {
        run_audio_bench.addArgs(args);
    }
    const audio_bench_step = b.step("bench-audio", "Run the checked-in audio decode and synthesis benchmark");
    audio_bench_step.dependOn(&run_audio_bench.step);

    // Tests
    const runtime_test_filter = b.option(bool, "runtime-test-filter", "Build unit tests with a simple runtime-filtering test runner") orelse false;
    const main_test_filters = if (runtime_test_filter) &.{} else selectTestFilters(b, &.{});
    const runtime_filter_test_runner: ?std.Build.Step.Compile.TestRunner = if (runtime_test_filter) .{
        .path = b.path("src/test_runner_filter.zig"),
        .mode = .simple,
    } else null;
    const tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/inference.zig"),
            .target = target,
            .optimize = optimize,
        }),
        .filters = main_test_filters,
        .test_runner = runtime_filter_test_runner,
    });
    tests.root_module.addImport("build_options", build_options_mod);
    tests.root_module.addImport("httpx", httpx_mod);
    tests.root_module.addImport("inference_api", inference_api_mod);
    tests.root_module.addImport("antfly_generating_openapi", generating_openapi_mod);
    tests.root_module.addImport("inference_audio", inference_audio_mod);
    tests.root_module.addImport("inference_chunker", inference_chunker_mod);
    tests.root_module.addImport("jinja", jinja_mod);
    tests.root_module.addImport("inference_tokenizer", inference_tokenizer_mod);
    tests.root_module.addImport("inference_hf_tokenizer", inference_hf_tokenizer_mod);
    tests.root_module.addImport("inference_linalg", inference_linalg_mod);
    tests.root_module.addImport("inference_fixed_tokenizer_data", inference_fixed_tokenizer_data_mod);
    tests.root_module.addImport("antfly_jsonschema", antfly_jsonschema_mod);
    tests.root_module.addImport("antfly_scraping", antfly_scraping_mod);
    tests.root_module.addImport("antfly_image", antfly_image_mod);
    tests.root_module.addImport("ml", ml_mod);
    tests.root_module.addImport("onnx_graph", onnx_graph_mod);
    tests.root_module.addImport("pjrt", pjrt_mod);
    tests.root_module.addImport("prometheus", prometheus_mod);
    tests.root_module.addImport("structlog", structlog_mod);
    tests.root_module.addImport("antfly_platform", platform_mod);
    tests.root_module.addImport("inference_internal", tests.root_module);
    if (client_mod) |mod| {
        tests.root_module.addImport("inference_client", mod);
    }
    if (enable_system_blas) {
        configureSystemBlas(b, tests.root_module, target, blas_root);
    }
    configureMetal(b, tests.root_module, target, enable_metal);
    configureMlx(b, tests.root_module, target, enable_mlx, effective_mlx_root);
    configureOnnxRuntime(b, tests.root_module, enable_onnx, effective_onnx_root);
    tests.root_module.link_libc = link_libc;

    const finetune_ctx = finetune_common.Context{
        .b = b,
        .target = target,
        .optimize = optimize,
        .build_options_mod = build_options_mod,
        .jinja_mod = jinja_mod,
        .ml_mod = ml_mod,
        .onnx_graph_mod = onnx_graph_mod,
        .inference_internal_mod = inference_internal_mod,
        .inference_tokenizer_mod = inference_tokenizer_mod,
        .inference_hf_tokenizer_mod = inference_hf_tokenizer_mod,
        .antfly_image_mod = antfly_image_mod,
        .pjrt_mod = pjrt_mod,
        .protobuf_mod = protobuf_mod,
        .inference_linalg_mod = inference_linalg_mod,
        .antfly_platform_mod = platform_mod,
        .enable_system_blas = enable_system_blas,
        .blas_root = blas_root,
        .enable_mlx = enable_mlx,
        .mlx_root = effective_mlx_root,
        .enable_metal = enable_metal,
    };
    finetune_tools.register(finetune_ctx);
    finetune_workflows.register(finetune_ctx);
    finetune_tests.register(finetune_ctx);

    const run_tests = b.addRunArtifact(tests);
    if (runtime_test_filter) {
        if (b.args) |args| run_tests.addArgs(args);
    }
    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_tests.step);
    const install_tests = b.addInstallArtifact(tests, .{
        .dest_sub_path = "antfly-inference-tests",
    });
    const test_bin_step = b.step("test-bin", "Build unit test binary without running");
    test_bin_step.dependOn(&install_tests.step);

    const wasm_compute_tests = b.addTest(.{
        .name = "wasm-compute-tests",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/inference.zig"),
            .target = target,
            .optimize = optimize,
        }),
        .filters = &.{"wasm_compute:"},
    });
    wasm_compute_tests.root_module.addImport("build_options", build_options_mod);
    wasm_compute_tests.root_module.addImport("httpx", httpx_mod);
    wasm_compute_tests.root_module.addImport("inference_api", inference_api_mod);
    wasm_compute_tests.root_module.addImport("inference_audio", inference_audio_mod);
    wasm_compute_tests.root_module.addImport("inference_chunker", inference_chunker_mod);
    wasm_compute_tests.root_module.addImport("jinja", jinja_mod);
    wasm_compute_tests.root_module.addImport("inference_tokenizer", inference_tokenizer_mod);
    wasm_compute_tests.root_module.addImport("inference_hf_tokenizer", inference_hf_tokenizer_mod);
    wasm_compute_tests.root_module.addImport("inference_linalg", inference_linalg_mod);
    wasm_compute_tests.root_module.addImport("inference_fixed_tokenizer_data", inference_fixed_tokenizer_data_mod);
    wasm_compute_tests.root_module.addImport("antfly_jsonschema", antfly_jsonschema_mod);
    wasm_compute_tests.root_module.addImport("antfly_scraping", antfly_scraping_mod);
    wasm_compute_tests.root_module.addImport("antfly_image", antfly_image_mod);
    wasm_compute_tests.root_module.addImport("ml", ml_mod);
    wasm_compute_tests.root_module.addImport("onnx_graph", onnx_graph_mod);
    wasm_compute_tests.root_module.addImport("pjrt", pjrt_mod);
    wasm_compute_tests.root_module.addImport("prometheus", prometheus_mod);
    wasm_compute_tests.root_module.addImport("structlog", structlog_mod);
    if (client_mod) |mod| {
        wasm_compute_tests.root_module.addImport("inference_client", mod);
    }
    if (enable_system_blas) {
        configureSystemBlas(b, wasm_compute_tests.root_module, target, blas_root);
    }
    configureMetal(b, wasm_compute_tests.root_module, target, enable_metal);
    configureMlx(b, wasm_compute_tests.root_module, target, enable_mlx, effective_mlx_root);
    configureOnnxRuntime(b, wasm_compute_tests.root_module, enable_onnx, effective_onnx_root);
    wasm_compute_tests.root_module.link_libc = true;
    const run_wasm_compute_tests = b.addRunArtifact(wasm_compute_tests);
    const test_wasm_compute_step = b.step("test-wasm-compute", "Run focused WasmCompute backend tests");
    test_wasm_compute_step.dependOn(&run_wasm_compute_tests.step);

    // Tiny helper binary that exposes gguf.quant_codec.dequantizeToFloat32 to
    // out-of-process test harnesses (web/test-quant-kernels-webgpu.cjs).
    const dequant_cli = b.addExecutable(.{
        .name = "dequant_cli",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/dequant_cli.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
        }),
    });
    const install_dequant_cli = b.addInstallArtifact(dequant_cli, .{});
    const dequant_cli_step = b.step("dequant-cli", "Build the GGUF dequant helper used by the WebGPU shader tests");
    dequant_cli_step.dependOn(&install_dequant_cli.step);

    const web_projector_tests = b.addTest(.{
        .name = "web-projector-tests",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/inference.zig"),
            .target = target,
            .optimize = optimize,
        }),
        .filters = &.{"projector"},
    });
    web_projector_tests.root_module.addImport("build_options", build_options_mod);
    web_projector_tests.root_module.addImport("httpx", httpx_mod);
    web_projector_tests.root_module.addImport("inference_api", inference_api_mod);
    web_projector_tests.root_module.addImport("inference_audio", inference_audio_mod);
    web_projector_tests.root_module.addImport("inference_chunker", inference_chunker_mod);
    web_projector_tests.root_module.addImport("jinja", jinja_mod);
    web_projector_tests.root_module.addImport("inference_tokenizer", inference_tokenizer_mod);
    web_projector_tests.root_module.addImport("inference_hf_tokenizer", inference_hf_tokenizer_mod);
    web_projector_tests.root_module.addImport("inference_linalg", inference_linalg_mod);
    web_projector_tests.root_module.addImport("inference_fixed_tokenizer_data", inference_fixed_tokenizer_data_mod);
    web_projector_tests.root_module.addImport("antfly_jsonschema", antfly_jsonschema_mod);
    web_projector_tests.root_module.addImport("antfly_scraping", antfly_scraping_mod);
    web_projector_tests.root_module.addImport("antfly_image", antfly_image_mod);
    web_projector_tests.root_module.addImport("ml", ml_mod);
    web_projector_tests.root_module.addImport("onnx_graph", onnx_graph_mod);
    web_projector_tests.root_module.addImport("pjrt", pjrt_mod);
    web_projector_tests.root_module.addImport("prometheus", prometheus_mod);
    web_projector_tests.root_module.addImport("structlog", structlog_mod);
    if (client_mod) |mod| {
        web_projector_tests.root_module.addImport("inference_client", mod);
    }
    if (enable_system_blas) {
        configureSystemBlas(b, web_projector_tests.root_module, target, blas_root);
    }
    configureMetal(b, web_projector_tests.root_module, target, enable_metal);
    configureMlx(b, web_projector_tests.root_module, target, enable_mlx, effective_mlx_root);
    configureOnnxRuntime(b, web_projector_tests.root_module, enable_onnx, effective_onnx_root);
    web_projector_tests.root_module.link_libc = true;
    const run_web_projector_tests = b.addRunArtifact(web_projector_tests);
    const test_web_projector_step = b.step("test-web-projector", "Run focused web projector/runtime tests");
    test_web_projector_step.dependOn(&run_web_projector_tests.step);

    const run_webgpu_browser_smoke = b.addSystemCommand(&.{ "node", "web/test-webgpu-shader-smoke.mjs" });
    const test_webgpu_browser_step = b.step("test-webgpu-browser", "Run Chromium WebGPU shader-family browser smoke");
    test_webgpu_browser_step.dependOn(&run_webgpu_browser_smoke.step);

    const linalg_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path(b.fmt("{s}/lib/linalg/src/mod.zig", .{shared_lib_root})),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_linalg_tests = b.addRunArtifact(linalg_tests);
    const linalg_test_step = b.step("test-linalg", "Run linalg tests");
    linalg_test_step.dependOn(&run_linalg_tests.step);

    // Tokenizer-only tests
    const tok_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path(b.fmt("{s}/lib/tokenizer/src/sentencepiece.zig", .{shared_lib_root})),
            .target = target,
            .optimize = optimize,
        }),
    });
    tok_tests.root_module.addImport("sentencepiece_proto", sentencepiece_proto_mod);
    const run_tok_tests = b.addRunArtifact(tok_tests);
    const tok_test_step = b.step("test-tokenizer", "Run tokenizer tests");
    tok_test_step.dependOn(&run_tok_tests.step);

    const audio_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path(b.fmt("{s}/lib/audio/audio_test_root.zig", .{shared_lib_root})),
            .target = target,
            .optimize = optimize,
        }),
    });
    audio_tests.root_module.addImport("build_options", build_options_mod);
    audio_tests.root_module.addImport("inference_audio", inference_audio_mod);
    audio_tests.root_module.link_libc = true;
    const run_audio_tests = b.addRunArtifact(audio_tests);
    const audio_test_step = b.step("test-audio", "Run shared audio tests");
    audio_test_step.dependOn(&run_audio_tests.step);

    const audio_open_corpus = b.addExecutable(.{
        .name = "audio_open_corpus",
        .root_module = b.createModule(.{
            .root_source_file = b.path(b.fmt("{s}/lib/audio/open_corpus_root.zig", .{shared_lib_root})),
            .target = target,
            .optimize = .ReleaseSafe,
        }),
    });
    audio_open_corpus.root_module.addImport("build_options", audio_open_corpus_build_options_mod);
    audio_open_corpus.root_module.link_libc = true;
    const run_audio_open_corpus = b.addRunArtifact(audio_open_corpus);
    if (b.args) |args| run_audio_open_corpus.addArgs(args);
    const audio_open_corpus_step = b.step("audio-open-corpus", "Run the non-MP3 audio open corpus runner");
    audio_open_corpus_step.dependOn(&run_audio_open_corpus.step);

    const audio_xiph_corpora_e2e = b.addExecutable(.{
        .name = "audio_xiph_corpora_e2e",
        .root_module = b.createModule(.{
            .root_source_file = b.path(b.fmt("{s}/lib/audio/audio_xiph_corpora_e2e.zig", .{shared_lib_root})),
            .target = target,
            .optimize = .ReleaseFast,
        }),
    });
    audio_xiph_corpora_e2e.root_module.addImport("build_options", audio_open_corpus_build_options_mod);
    audio_xiph_corpora_e2e.root_module.link_libc = true;
    const audio_xiph_corpora_e2e_step = b.step("audio-xiph-corpora-e2e", "Build the lib/audio upstream Xiph corpora e2e runner");
    audio_xiph_corpora_e2e_step.dependOn(&audio_xiph_corpora_e2e.step);

    const fetch_audio_xiph_corpora_e2e = b.addRunArtifact(audio_xiph_corpora_e2e);
    fetch_audio_xiph_corpora_e2e.addArg("fetch");
    fetch_audio_xiph_corpora_e2e.addArg("/tmp/antfly-inference-audio-xiph-corpora");
    const audio_xiph_corpora_e2e_fetch_step = b.step("audio-xiph-corpora-e2e-fetch", "Fetch or refresh the upstream lib/audio Xiph-family corpora checkouts");
    audio_xiph_corpora_e2e_fetch_step.dependOn(&fetch_audio_xiph_corpora_e2e.step);

    const run_audio_xiph_corpora_e2e = b.addRunArtifact(audio_xiph_corpora_e2e);
    run_audio_xiph_corpora_e2e.addArg("run");
    run_audio_xiph_corpora_e2e.addArg("/tmp/antfly-inference-audio-xiph-corpora");
    run_audio_xiph_corpora_e2e.addArg("--no-fetch");
    const audio_xiph_corpora_e2e_run_step = b.step("audio-xiph-corpora-e2e-run", "Run the lib/audio upstream Xiph-family corpora e2e runner");
    audio_xiph_corpora_e2e_run_step.dependOn(&run_audio_xiph_corpora_e2e.step);

    const audio_misc_corpora_e2e = b.addExecutable(.{
        .name = "audio_misc_corpora_e2e",
        .root_module = b.createModule(.{
            .root_source_file = b.path(b.fmt("{s}/lib/audio/audio_misc_corpora_e2e.zig", .{shared_lib_root})),
            .target = target,
            .optimize = .ReleaseFast,
        }),
    });
    audio_misc_corpora_e2e.root_module.addImport("build_options", audio_open_corpus_build_options_mod);
    audio_misc_corpora_e2e.root_module.link_libc = true;
    const audio_misc_corpora_e2e_step = b.step("audio-misc-corpora-e2e", "Build the lib/audio external MP3/AAC/MP4 corpora e2e runner");
    audio_misc_corpora_e2e_step.dependOn(&audio_misc_corpora_e2e.step);

    const fetch_audio_misc_corpora_e2e = b.addRunArtifact(audio_misc_corpora_e2e);
    fetch_audio_misc_corpora_e2e.addArg("fetch");
    fetch_audio_misc_corpora_e2e.addArg("/tmp/antfly-inference-audio-misc-corpora");
    const audio_misc_corpora_e2e_fetch_step = b.step("audio-misc-corpora-e2e-fetch", "Fetch or refresh the external lib/audio MP3/AAC/MP4 corpora");
    audio_misc_corpora_e2e_fetch_step.dependOn(&fetch_audio_misc_corpora_e2e.step);

    const run_audio_misc_corpora_e2e = b.addRunArtifact(audio_misc_corpora_e2e);
    run_audio_misc_corpora_e2e.addArg("run");
    run_audio_misc_corpora_e2e.addArg("/tmp/antfly-inference-audio-misc-corpora");
    run_audio_misc_corpora_e2e.addArg("--no-fetch");
    const audio_misc_corpora_e2e_run_step = b.step("audio-misc-corpora-e2e-run", "Run the lib/audio external MP3/AAC/MP4 corpora e2e runner");
    audio_misc_corpora_e2e_run_step.dependOn(&run_audio_misc_corpora_e2e.step);

    const audio_module_test_step = b.step("test-audio-internals", "Run selected stable internal audio module tests");
    const audio_module_test_filters = [_][]const u8{
        "decode synthetic channel-pair parses tns data before gain-control flag",
        "parse adts header rejects nonzero layer bits",
        "parse synthetic long-window ics info rejects predictor data",
        "decode synthetic mono block skips leading pce element",
        "decode synthetic mono block skips leading dse element",
        "decode synthetic mono block skips leading fil element",
        "decode synthetic mono block skips leading fil element with zero escape count",
        "decode mono pcm sequence skips supported trailing metadata elements",
        "decode mono pcm sequence rejects trailing unexpected channel element",
        "decode mono pcm sequence rejects leading channel-pair element",
        "decode synthetic channel-pair skips supported trailing metadata elements",
        "decode synthetic channel-pair rejects trailing unexpected channel element",
        "aac lc mp4 access unit config accepts lc extension flag",
        "aac lc mp4 access unit config rejects sbr sync extension",
        "aac real low-bitrate m4a fixture exposes sbr sync extension",
        "aac real low-bitrate m4a fixture lc core decodes at the packet layer",
        "aac ps mp4 access unit config rejects explicit ps object type",
        "aac lc mp4 access unit config ignores sbr sync pattern inside pce comment",
        "aac main mp4 access unit config decodes mono access unit",
        "aac main mp4 access unit config decodes stereo access unit",
        "aac lc mp4 access unit config infers mono from explicit pce",
        "aac lc mp4 access unit config skips metadata-only mono access unit",
        "aac lc mp4 access unit config rejects metadata-only mono access units",
        "aac lc adts explicit pce channel config decodes mono access unit",
        "aac lc adts explicit pce channel config reuses first-frame mono layout",
        "aac lc adts explicit pce channel config skips metadata-only leading mono frame",
        "aac lc adts explicit pce channel config skips metadata-only pre-layout mono frame",
        "aac lc adts explicit pce channel config rejects metadata-only mono stream",
        "aac lc adts explicit pce channel config rejects missing initial layout",
        "aac lc adts explicit pce channel config rejects later mono tag mismatch",
        "aac lc adts explicit pce channel config rejects mixed sample rate frames",
        "aac lc mp4 explicit pce mono lfe decodes access unit",
        "aac lc adts explicit pce channel config decodes mono lfe access unit",
        "aac lc mp4 explicit pce mono side sce tag must match access unit",
        "aac lc mp4 explicit pce rejects mismatched metadata-only mono access unit",
        "aac lc mp4 explicit pce rejects mismatched in-band mono pce before matching audio",
        "aac lc mp4 explicit pce rejects conflicting repeated in-band mono pce",
        "aac lc adts explicit pce channel config decodes mono back sce access unit",
        "aac lc mp4 access unit config infers stereo from explicit pce",
        "aac lc mp4 explicit pce skips metadata-only stereo access unit",
        "aac lc mp4 explicit pce rejects mismatched metadata-only stereo access unit",
        "aac lc mp4 explicit pce rejects metadata-only stereo access units",
        "aac lc mp4 explicit pce rejects mismatched in-band stereo pce before matching audio",
        "aac lc mp4 explicit pce rejects conflicting repeated in-band stereo pce",
        "aac lc adts explicit pce channel config decodes stereo access unit",
        "aac lc adts explicit pce channel config reuses first-frame stereo cpe layout",
        "aac lc adts explicit pce channel config reuses first-frame stereo sce layout",
        "aac lc adts explicit pce channel config reuses first raw-data-block stereo layout",
        "aac lc adts explicit pce channel config skips metadata-only first raw-data-block",
        "aac lc adts explicit pce channel config skips metadata-only pre-layout raw-data-block",
        "aac lc adts explicit pce channel config rejects later raw-data-block stereo tag mismatch",
        "aac lc adts explicit pce channel config rejects conflicting repeated layout in same raw-data-block",
        "aac lc adts explicit pce channel config rejects conflicting repeated mono layout in same raw-data-block",
        "aac lc adts explicit pce channel config rejects conflicting repeated layout in later raw-data-block",
        "aac lc adts explicit pce channel config rejects conflicting repeated mono layout in later raw-data-block",
        "aac lc adts explicit pce channel config reuses crc-protected raw-data-block stereo layout",
        "aac lc adts explicit pce channel config skips crc-protected metadata-only pre-layout raw-data-block",
        "aac lc adts explicit pce channel config rejects crc-protected audio before layout raw-data-block",
        "aac lc adts explicit pce channel config rejects conflicting repeated layout in crc-protected raw-data-block",
        "aac lc adts explicit pce channel config rejects conflicting repeated mono layout in crc-protected raw-data-block",
        "aac lc adts explicit pce channel config rejects later stereo tag mismatch",
        "aac lc adts explicit pce channel config rejects conflicting repeated stereo layout",
        "aac lc mp4 explicit pce stereo cpe tag must match access unit",
        "aac lc adts explicit pce stereo cpe tag must match access unit",
        "aac lc mp4 explicit pce stereo side cpe tag must match access unit",
        "aac lc adts explicit pce channel config decodes stereo back cpe access unit",
        "aac lc mp4 explicit pce stereo sce pair decodes access unit",
        "aac lc adts explicit pce channel config decodes stereo sce pair access unit",
        "aac lc mp4 explicit pce stereo sce pair tags must match access unit",
        "aac lc mp4 explicit pce stereo front back sce tags must match access unit",
        "aac lc mp4 explicit pce rejects sce plus lfe as stereo",
        "aac lc adts explicit pce rejects sce plus lfe as stereo",
        "scan adts frames skips leading id3v2 tag",
        "scan adts frames skips sized leading id3v2 tag with footer",
        "scan adts frames skips interstitial id3v2 tag",
        "scan adts frames skips trailing id3v1 tag",
        "decode adts crc-protected frame skips crc bytes",
        "decode adts frame with two crc-absent raw data blocks",
        "decode adts crc-protected frame with two raw data blocks skips block crcs",
        "aac main adts fixed channel config decodes mono access unit",
        "aac main adts fixed channel config decodes stereo access unit",
        "aac main mp4 access unit config decodes mono predictor data",
        "aac lc mp4 access unit config decodes mono 960-sample access unit",
        "aac lc mp4 access unit config decodes stereo 960-sample access unit",
        "aac he-aac mp4 access unit config decodes explicit sbr object type",
        "aac ps mp4 access unit config decodes explicit ps object type",
        "aac ps mp4 access unit config decodes explicit ps mono-core stereo output",
        "aac explicit ps mono-core stereo carries payload profile across later no-fill access units",
        "aac explicit ps mono-core stereo delays activation until first payload access unit",
        "aac explicit ps mono-core stereo ignores sbr-only payload until first ps payload",
        "aac lc mp4 sync-extension ps fill payload decodes mono-core stereo output",
        "aac sync-extension ps mono-core stereo output rejects sbr-only fill payload",
        "aac lc mp4 sync-extension sbr fill payload upsamples stereo access unit",
        "aac stereo cpe with gain control and sbr fill payload decodes",
        "aac stereo intensity cpe with tns gain and sbr fill decodes",
        "aac lc adts fixed channel config skips metadata-only leading mono frame",
        "aac lc adts fixed channel config rejects metadata-only mono stream",
        "aac lc adts fixed channel config skips metadata-only first stereo raw-data-block",
        "aac lc adts fixed channel config skips metadata-only crc-protected first stereo raw-data-block",
        "aac gain control parser consumes ffmpeg-compatible payload shape",
        "aac intensity stereo reconstructs right channel band",
        "aac sbr enhancement synthesis adds detail beyond linear upsample",
        "aac sbr enhancement synthesis responds to tail detail hints",
        "aac fill element parser captures payload stats",
        "aac fill element parser distinguishes ps payload marker",
        "aac fill element parser captures tail detail hints",
        "aac access unit trailing info aggregates payload structure",
        "aac access unit trailing info prefers latest ps payload structure",
        "aac access unit trailing info keeps latest plain sbr across later ps-only access unit",
        "aac sync-extension sbr carries forward last enhancement payload across access units",
        "aac sync-extension sbr carried enhancement decays across repeated no-fill access units",
        "aac sync-extension sbr refresh keeps prior unrefreshed subfields across access units",
        "aac sync-extension sbr carries forward last plain sbr payload across later ps-only access units",
        "aac sync-extension ps carries forward ps payload across later sbr-only access units",
        "aac sync-extension ps carried stereoization decays across repeated sbr-only access units",
        "aac sync-extension ps refresh keeps prior unrefreshed ps subfields across access units",
        "aac sync-extension ps-only refresh keeps carried sbr shaping profile",
        "aac trailing info scans past leading non-sbr fill to later sbr fill",
        "aac sync-extension sbr decode honors trailing fill after leading non-sbr fill",
        "aac trailing info scans stereo sce pair with trailing sbr fill",
        "aac trailing info prefers latest sbr fill in same access unit",
        "aac trailing info prefers latest ps fill in same access unit",
        "aac trailing info preserves prior sbr subfields on shorter latest same access unit fill",
        "aac trailing info preserves prior ps subfields on shorter latest same access unit fill",
        "aac sync-extension sbr decode prefers latest fill in same access unit",
        "aac sync-extension sbr decode preserves prior subfields on shorter latest fill",
        "aac sync-extension sbr decode preserves prior subfields on shorter later access unit fill",
        "aac sync-extension ps decode preserves prior subfields on shorter later access unit fill",
        "aac sync-extension sbr stereo sce pair decodes with trailing fill",
        "aac sync-extension sbr enhancement varies with payload structure",
        "aac sync-extension sbr enhancement is applied per access unit",
        "aac ps stereoization varies with payload structure",
        "aac sync-extension ps mono-core stereo output tolerates delayed first ps payload",
        "aac main prediction carries state and honors reset groups",
        "parse synthetic long-window ics info decodes predictor data when sample rate is known",
        "extract checked-in caf alac magic cookie",
        "extract checked-in 24bit caf alac magic cookie",
        "parse checked-in mp4 alac decoder config",
        "parse checked-in 24bit mp4 alac decoder config",
        "parse checked-in generic mp4 alac decoder config",
        "parse checked-in 24bit generic mp4 alac decoder config",
        "decode checked-in caf alac fixture to interleaved pcm",
        "decode checked-in 24bit caf alac fixture to interleaved pcm",
        "decode checked-in m4a alac fixture to interleaved pcm",
        "decode checked-in 24bit m4a alac fixture to interleaved pcm",
        "decode checked-in generic mp4 alac fixture to interleaved pcm",
        "decode checked-in 24bit generic mp4 alac fixture to interleaved pcm",
        "extract synthetic caf alac magic cookie from eof-sized kuki chunk",
        "parse checked-in flac streaminfo",
        "decode checked-in flac fixture to interleaved pcm",
        "decode checked-in 24bit flac fixture to interleaved pcm",
        "sniff checked-in ogg codecs",
        "parse checked-in ogg packet metadata",
        "reconstruct checked-in ogg flac stream",
        "decode checked-in ogg flac fixture to interleaved pcm",
        "parse checked-in opus head",
        "parse checked-in opus toc and frame packing",
        "demux checked-in opus fixtures exposes trim and packet counts",
        "classify checked-in opus frame shapes",
        "opus output gain scale follows q8 db units",
        "opus range decoder reads raw tail bits from end of frame",
        "opus range decoder decodes binary symbol with logp shortcut",
        "decode checked-in opus coarse energy stays aligned between .opus and .ogg alias",
        "decode checked-in opus coarse energy sequences remain finite",
        "decode checked-in mono opus residual bands stay finite and non-zero",
        "decode checked-in stereo opus residual bands stay finite and non-zero",
        "checked-in stereo opus aliases keep celt residual plan parity",
        "checked-in stereo opus corpus exercises widened stereo plan shapes",
        "classify real mono celt 5ms packet shape",
        "classify real mono celt 120ms packet shape",
        "classify real stereo celt 2.5ms packet shape",
        "classify real stereo celt 60ms packet shape",
        "classify real stereo celt 40ms packet shape",
        "decode checked-in mono opus to interleaved pcm on narrow pure-zig lane",
        "decode real mono celt 5ms opus fixture to interleaved pcm",
        "decode real mono celt 120ms opus fixture to interleaved pcm",
        "decode real stereo celt 2.5ms opus fixture to interleaved pcm",
        "decode real stereo celt 60ms opus fixture to interleaved pcm",
        "decode real stereo celt 40ms opus fixture to interleaved pcm",
        "decode checked-in stereo opus aliases to interleaved pcm on widened pure-zig lane",
        "synthesize stereo celt frame handles intensity-shared tail bands",
        "decode coupled stereo celt band keeps coefficients finite",
        "decode and synthesize coupled stereo celt low bands below intensity",
        "decode generated silk packet header flags",
        "decode generated hybrid packet header flags",
        "decode generated silk packet front exposes indices and pulses",
        "decode generated hybrid packet front exposes stereo silk state and pulses",
        "decode generated silk packet parameters expose gains lpc and excitation",
        "silk gain dequant saturates instead of overflowing i32",
        "silk round shift saturates instead of overflowing i32",
        "silk clamp32 saturates i64 extremes",
        "decode real mono silk fec packet header exposes lbrr",
        "decode real mono silk fec 10ms packet header exposes lbrr",
        "decode real mono silk fec 40ms packet header exposes lbrr and two internal frames",
        "decode real mono silk fec 60ms packet header exposes lbrr and three internal frames",
        "decode real stereo silk fec packet header exposes lbrr",
        "decode real stereo silk fec 10ms packet header exposes lbrr",
        "decode real stereo silk fec 40ms packet header exposes lbrr and two internal frames",
        "decode real stereo silk fec 60ms packet header exposes lbrr and three internal frames",
        "synthesize real mono silk fec packet to 16 khz pcm",
        "synthesize real mono silk fec 10ms packet to 16 khz pcm",
        "synthesize real mono silk fec 40ms packet to 16 khz pcm",
        "synthesize real mono silk fec 60ms packet to 16 khz pcm",
        "synthesize real stereo silk fec packet to 16 khz pcm",
        "synthesize real stereo silk fec 10ms packet to 16 khz pcm",
        "synthesize real stereo silk fec 40ms packet to 16 khz pcm",
        "synthesize real stereo silk fec 60ms packet to 16 khz pcm",
        "decode generated hybrid packet parameters expose stereo silk decode state",
        "synthesize real mono silk 10ms packet to 16 khz pcm",
        "synthesize real stereo silk 10ms packet to 16 khz pcm",
        "decode real stereo silk 40ms packet front exposes two internal frames",
        "decode real mono silk 60ms packet parameters expose three internal frames",
        "synthesize generated silk packet to mono 16 khz pcm",
        "synthesize real mono silk 60ms packet to 16 khz pcm",
        "integrate generated hybrid silk lowband into 48 khz stereo pcm",
        "decode generated hybrid packet celt highband residual after silk front",
        "decode generated hybrid packet integrates celt highband into stereo 48 khz pcm",
        "decode real mono hybrid fec packet header exposes lbrr",
        "decode real mono hybrid fec 10ms packet header exposes lbrr",
        "decode real mono hybrid 10ms packet integrates to 48 khz pcm",
        "decode real mono hybrid fec packet integrates to 48 khz pcm",
        "decode real mono hybrid fec 10ms packet integrates to 48 khz pcm",
        "decode real stereo hybrid fec packet header exposes lbrr",
        "decode real stereo hybrid fec 10ms packet header exposes lbrr",
        "decode real stereo hybrid 10ms packet integrates to 48 khz pcm",
        "decode real stereo hybrid fec packet integrates to 48 khz pcm",
        "decode real stereo hybrid fec 10ms packet integrates to 48 khz pcm",
        "decode synthetic ogg opus silk probe to interleaved pcm",
        "decode synthetic ogg opus hybrid probe to interleaved pcm",
        "parse checked-in vorbis identification header",
        "parse checked-in vorbis headers",
        "parse checked-in vorbis setup exposes codebook floor residue metadata",
        "parse checked-in vorbis audio packet headers",
        "demux checked-in vorbis fixtures exposes packet schedule and trim",
        "decode checked-in vorbis fixtures to interleaved pcm",
        "decodeInterleaved handles checked-in aiff pcm16 fixture",
        "decodeInterleaved handles checked-in flac fixtures",
        "decodeInterleaved handles checked-in ogg flac fixture",
        "decode synthetic aiff pcm8 and pcm24 fixtures",
        "decode synthetic aifc none twos and sowt pcm fixtures",
        "decode synthetic aifc raw unsigned pcm8 fixture",
        "decode synthetic aifc in24 and in32 pcm aliases",
        "decode synthetic aifc twos signed pcm64 fixture",
        "decode synthetic aifc fl32 and fl64 fixtures",
        "decode synthetic aifc ulaw and alaw fixtures",
        "decode synthetic au pcm and float fixtures",
        "decode synthetic au wide pcm and float fixtures",
        "decode synthetic au g711 fixtures",
        "decodeInterleaved handles synthetic au pcm16 fixture",
        "decode synthetic caf lpcm pcm16 and float32 fixtures",
        "decode synthetic caf wide lpcm fixtures",
        "decode synthetic caf g711 fixtures",
        "demux caf alac fixture accepts data chunk sized to end of file",
        "decodeInterleaved handles synthetic caf lpcm pcm16 fixture",
        "decodeInterleaved normalizes finite PCM range at public boundary",
        "mp4 edit-list trim helper removes priming and remainder frames",
        "parse edit list accepts leading empty edit before media edit",
        "parse edit list accepts contiguous media edits",
        "parse edit list rejects discontiguous media edits",
        "parse audio sample entry accepts version 1 child boxes",
        "parse audio sample entry accepts version 2 quicktime extension",
        "parse audio sample entry finds esds in wave wrapper",
        "parse media accepts minf before audio handler",
        "parse movie accepts movie header after audio track",
        "parse movie skips unsupported audio track before supported track",
        "parse sample-to-chunk table accepts monotonic first-description entries",
        "parse sample description table uses selected entry",
        "parse sample-to-chunk table rejects sample-description switch",
        "parse sample-to-chunk table rejects nonmonotonic chunks",
        "parse compact sample size table handles 4-bit entries",
        "parse compact sample size table handles 16-bit entries",
        "parse compact sample size table rejects unsupported field size",
        "decodeInterleaved handles wav common pcm formats",
        "decode wav rejects partial sample frame data",
        "decode interleaved wav handles g711 alaw and mulaw formats",
        "decode interleaved wav handles extensible g711 formats",
        "decode rf64 and bw64 wav use ds64 data size",
        "decode mono wav fast path handles pcm32 mono and stereo",
        "decode mono wav fast path handles pcm64 mono and stereo",
        "decode mono wav fast path handles pcm8 and g711 stereo",
        "checked-in mp3 demux rejection corpus handles raw fallback outcomes",
    };
    for (audio_module_test_filters, 0..) |filter, filter_index| {
        const audio_module_tests = b.addTest(.{
            .name = b.fmt("audio-internal-{d}", .{filter_index}),
            .root_module = b.createModule(.{
                .root_source_file = b.path(b.fmt("{s}/lib/audio/audio_module_test_root.zig", .{shared_lib_root})),
                .target = target,
                .optimize = optimize,
            }),
            .filters = &.{filter},
        });
        audio_module_tests.root_module.addImport("build_options", build_options_mod);
        audio_module_tests.root_module.link_libc = true;
        const run_audio_module_tests = b.addRunArtifact(audio_module_tests);
        audio_module_test_step.dependOn(&run_audio_module_tests.step);
    }

    const chunker_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path(b.fmt("{s}/lib/chunker/src/mod.zig", .{shared_lib_root})),
            .target = target,
            .optimize = optimize,
        }),
    });
    chunker_tests.root_module.addImport("build_options", build_options_mod);
    chunker_tests.root_module.addImport("inference_hf_tokenizer", inference_hf_tokenizer_mod);
    chunker_tests.root_module.addImport("inference_audio", inference_audio_mod);
    chunker_tests.root_module.addImport("inference_fixed_tokenizer_data", inference_fixed_tokenizer_data_mod);
    chunker_tests.root_module.addImport("antfly_image", antfly_image_mod);
    chunker_tests.root_module.link_libc = true;
    const run_chunker_tests = b.addRunArtifact(chunker_tests);
    const chunker_test_step = b.step("test-chunker", "Run chunker tests");
    chunker_test_step.dependOn(&run_chunker_tests.step);

    // ONNX graph converter tests
    const onnx_graph_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path(b.fmt("{s}/lib/onnx/src/root.zig", .{shared_lib_root})),
            .target = target,
            .optimize = optimize,
        }),
    });
    onnx_graph_tests.root_module.addImport("protobuf", protobuf_mod);
    onnx_graph_tests.root_module.addImport("ml", ml_mod);
    const run_onnx_graph_tests = b.addRunArtifact(onnx_graph_tests);
    const onnx_graph_test_step = b.step("test-onnx-graph", "Run ONNX graph converter tests");
    onnx_graph_test_step.dependOn(&run_onnx_graph_tests.step);

    // WASM library target for browser inference
    if (enable_wasm) {
        const is_wasm64 = std.mem.eql(u8, wasm_memory_model, "wasm64");
        const wasm_target = b.resolveTargetQuery(.{
            .cpu_arch = if (is_wasm64) .wasm64 else .wasm32,
            .os_tag = .freestanding,
            .cpu_features_add = std.Target.wasm.featureSet(&.{ .atomics, .bulk_memory, .simd128 }),
        });
        const wasm_root = if (is_wasm64)
            "src/wasm_entry_wasm64.zig"
        else
            "src/wasm_entry_wasm32.zig";
        const wasm_install_name = if (is_wasm64) "antfly-inference-wasm64.wasm" else "antfly-inference-wasm32.wasm";
        const wasm_jinja_dep = b.dependency("jinja", .{
            .target = wasm_target,
            .optimize = .ReleaseSafe,
        });

        const wasm_lib = b.addExecutable(.{
            .name = if (is_wasm64) "antfly-inference-wasm64" else "antfly-inference-wasm32",
            .root_module = b.createModule(.{
                .root_source_file = b.path(wasm_root),
                .target = wasm_target,
                .optimize = .ReleaseSafe,
                .single_threaded = true,
            }),
        });
        wasm_lib.root_module.addImport("build_options", build_options_mod);
        wasm_lib.entry = .disabled;
        wasm_lib.rdynamic = true;
        // ReleaseSafe: works around LLVM WASM backend miscompilation at -Os/-O3
        // that produces NaN in BERT encoder FFN linear ops. 1.2 MB binary.

        // Tokenizer modules for WASM target (pure Zig, no C deps)
        const wasm_tokenizer_mod = b.createModule(.{
            .root_source_file = b.path(b.fmt("{s}/lib/tokenizer/src/tokenizer.zig", .{shared_lib_root})),
            .target = wasm_target,
            .optimize = .ReleaseSafe,
            .single_threaded = true,
        });
        const wasm_hf_tokenizer_mod = b.createModule(.{
            .root_source_file = b.path(b.fmt("{s}/lib/tokenizer/src/hf_root.zig", .{shared_lib_root})),
            .target = wasm_target,
            .optimize = .ReleaseSafe,
            .single_threaded = true,
        });
        const wasm_audio_mod = b.createModule(.{
            .root_source_file = b.path(b.fmt("{s}/lib/audio/src/mod.zig", .{shared_lib_root})),
            .target = wasm_target,
            .optimize = .ReleaseSafe,
            .single_threaded = true,
        });
        const wasm_image_mod = b.createModule(.{
            .root_source_file = b.path(b.fmt("{s}/lib/image/src/mod.zig", .{shared_lib_root})),
            .target = wasm_target,
            .optimize = .ReleaseSafe,
            .single_threaded = true,
        });
        const wasm_platform_mod = b.createModule(.{
            .root_source_file = b.path(b.fmt("{s}/lib/platform/src/root.zig", .{shared_lib_root})),
            .target = wasm_target,
            .optimize = .ReleaseSafe,
            .single_threaded = true,
        });
        const wasm_linalg_mod = b.createModule(.{
            .root_source_file = b.path(b.fmt("{s}/lib/linalg/src/mod.zig", .{shared_lib_root})),
            .target = wasm_target,
            .optimize = .ReleaseSafe,
            .single_threaded = true,
        });
        const wasm_ml_mod = b.createModule(.{
            .root_source_file = b.path(b.fmt("{s}/lib/ml/src/root.zig", .{shared_lib_root})),
            .target = wasm_target,
            .optimize = .ReleaseSafe,
            .single_threaded = true,
        });
        const wasm_onnx_graph_mod = b.createModule(.{
            .root_source_file = b.path(b.fmt("{s}/lib/onnx/src/root.zig", .{shared_lib_root})),
            .target = wasm_target,
            .optimize = .ReleaseSafe,
            .single_threaded = true,
        });
        wasm_onnx_graph_mod.addImport("protobuf", protobuf_mod);
        wasm_onnx_graph_mod.addImport("ml", wasm_ml_mod);
        wasm_tokenizer_mod.addImport("sentencepiece_proto", sentencepiece_proto_mod);
        wasm_hf_tokenizer_mod.addImport("inference_tokenizer", wasm_tokenizer_mod);
        wasm_lib.root_module.addImport("jinja", wasm_jinja_dep.module("jinja"));
        wasm_lib.root_module.addImport("inference_audio", wasm_audio_mod);
        wasm_lib.root_module.addImport("inference_tokenizer", wasm_tokenizer_mod);
        wasm_lib.root_module.addImport("inference_hf_tokenizer", wasm_hf_tokenizer_mod);
        wasm_lib.root_module.addImport("inference_linalg", wasm_linalg_mod);
        wasm_lib.root_module.addImport("antfly_image", wasm_image_mod);
        wasm_lib.root_module.addImport("antfly_platform", wasm_platform_mod);
        wasm_lib.root_module.addImport("ml", wasm_ml_mod);
        wasm_lib.root_module.addImport("onnx_graph", wasm_onnx_graph_mod);

        const wasm_install = b.addInstallArtifact(wasm_lib, .{
            .dest_sub_path = wasm_install_name,
        });

        const wasm_step = b.step("wasm", "Build WASM module for browser inference");
        wasm_step.dependOn(&wasm_install.step);
        if (!is_wasm64) {
            const wasm_compat_install = b.addInstallFile(wasm_lib.getEmittedBin(), "antfly-inference.wasm");
            wasm_step.dependOn(&wasm_compat_install.step);
        }
    }
}
