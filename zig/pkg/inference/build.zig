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
const build_test_filters = @import("build/test_filters.zig");
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
    return build_test_filters.select(
        b.allocator,
        b.args orelse &.{},
        default_filters,
    );
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

fn pathExists(b: *std.Build, path: []const u8) bool {
    const io = b.graph.io;
    std.Io.Dir.cwd().access(io, path, .{}) catch return false;
    return true;
}

fn targetRunsOnBuildHost(b: *std.Build, target: std.Build.ResolvedTarget) bool {
    const host = b.graph.host.result;
    return target.result.os.tag == host.os.tag and
        target.result.cpu.arch == host.cpu.arch and
        target.result.abi == host.abi;
}

fn addMacosSdkPaths(b: *std.Build, module: *std.Build.Module, target: std.Build.ResolvedTarget) void {
    if (target.result.os.tag != .macos) return;
    const sdk_root = b.sysroot orelse
        b.graph.environ_map.get("SDK_PATH") orelse
        std.zig.system.darwin.getSdk(b.allocator, b.graph.io, &target.result) orelse
        return;
    module.addSystemIncludePath(.{ .cwd_relative = b.fmt("{s}/usr/include", .{sdk_root}) });
    module.addLibraryPath(.{ .cwd_relative = b.fmt("{s}/usr/lib", .{sdk_root}) });
    module.addFrameworkPath(.{ .cwd_relative = b.fmt("{s}/System/Library/Frameworks", .{sdk_root}) });
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
        addMacosSdkPaths(b, module, target);
        module.linkFramework("Accelerate", .{});
        return;
    }
    if (blas_root) |root| {
        addRootedLibraryPaths(b, module, root);
    }
    module.linkSystemLibrary("openblas", .{});
}

/// Link the Metal framework and compile the standalone Metal kernels.
fn configureMetal(
    b: *std.Build,
    module: *std.Build.Module,
    target: std.Build.ResolvedTarget,
    enable_metal: bool,
) void {
    if (!enable_metal or target.result.os.tag != .macos) return;

    addMacosSdkPaths(b, module, target);
    module.linkFramework("Foundation", .{});
    module.linkFramework("Metal", .{});
    module.linkFramework("MetalPerformanceShaders", .{});
    module.addCSourceFile(.{ .file = b.path("src/backends/metal_kernels.m"), .flags = &.{"-fobjc-arc"} });
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
    enable_metal: bool,
) void {
    if (enable_system_blas) {
        configureSystemBlas(b, artifact.root_module, target, blas_root);
    }
    configureMetal(b, artifact.root_module, target, enable_metal);
    artifact.root_module.link_libc = true;
}

pub fn build(b: *std.Build) void {
    // On Linux, an implicit native target can cause Zig 0.16.0 to discover and
    // link against the host distro's crt startup objects. Newer glibc/binutils
    // builds may include .sframe sections with relocation types that Zig's
    // linker cannot yet handle. Defaulting Linux builds to an explicit GNU
    // target keeps user-supplied -Dtarget overrides intact while making the
    // no-argument path use Zig's bundled libc startup objects.
    const default_target: std.Target.Query = if (builtin.os.tag == .linux)
        .{ .cpu_arch = builtin.cpu.arch, .os_tag = .linux, .abi = .gnu }
    else
        .{};
    const target = b.standardTargetOptions(.{ .default_target = default_target });
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
    const enable_metal = if (enable_wasm or !link_libc) false else (b.option(bool, "metal", "Enable Apple Metal kernels (macOS only)") orelse (target.result.os.tag == .macos));
    if (enable_onnx) {
        const onnx_runtime_available = pathExists(b, b.fmt("{s}/include/onnxruntime_c_api.h", .{effective_onnx_root})) and
            pathExists(b, b.fmt("{s}/lib", .{effective_onnx_root}));
        if (!onnx_runtime_available) {
            @panic("-Donnx=true requires an ONNX Runtime install; pass -Donnx-root=<path>");
        }
    }
    const enable_cuda = if (enable_wasm or !link_libc) false else (b.option(bool, "cuda", "Enable CUDA backend through the NVIDIA Driver API") orelse false);
    const cuda_artifacts = b.option([]const u8, "cuda-artifacts", "CUDA artifact bundle: fatbin SASS+PTX, portable PTX, or sm89 cubin") orelse "fatbin";
    if (!std.mem.eql(u8, cuda_artifacts, "portable") and !std.mem.eql(u8, cuda_artifacts, "fatbin") and !std.mem.eql(u8, cuda_artifacts, "sm89")) {
        @panic("invalid -Dcuda-artifacts (expected portable, fatbin, or sm89)");
    }
    const cuda_libraries = b.option([]const u8, "cuda-libs", "CUDA library acceleration policy: auto, off, or required") orelse "auto";
    if (!std.mem.eql(u8, cuda_libraries, "auto") and !std.mem.eql(u8, cuda_libraries, "off") and !std.mem.eql(u8, cuda_libraries, "required")) {
        @panic("invalid -Dcuda-libs (expected auto, off, or required)");
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
    const configured_platform_mod = b.dependency("antfly_platform", .{
        .target = target,
        .optimize = optimize,
        .link_libc = link_libc,
    }).module("antfly_platform");

    const runtime_graph = runtime_build.create(.{
        .b = b,
        .target = target,
        .optimize = optimize,
        .paths = .{
            .inference_root = "",
            .shared_lib_root = shared_lib_root,
        },
        .register_public_modules = true,
        .shared = .{
            .platform = configured_platform_mod,
        },
        .backend = .{
            .enable_onnx = enable_onnx,
            .onnx_root = effective_onnx_root,
            .enable_metal = enable_metal,
            .enable_cuda = enable_cuda,
            .cuda_artifacts = cuda_artifacts,
            .cuda_libraries = cuda_libraries,
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
    const extraction_openapi_mod = runtime_graph.extraction_openapi_mod;
    const extracting_mod = runtime_graph.extracting_mod;
    const client_mod = runtime_graph.inference_client_mod;
    const inference_internal_mod = runtime_graph.inference_internal_mod;

    const exe = runtime_build.addStandaloneExecutable(b, runtime_graph, target, optimize, "", link_libc);
    const install_exe = b.addInstallArtifact(exe, .{
        .dest_sub_path = "antfly-inference",
    });
    b.getInstallStep().dependOn(&install_exe.step);

    const run_exe = b.addRunArtifact(exe);
    run_exe.step.dependOn(&install_exe.step);
    if (b.args) |args| {
        run_exe.addArgs(args);
    }
    const run_step = b.step("run", "Run the Antfly inference server");
    run_step.dependOn(&run_exe.step);

    const kernel_jit_package_exe = b.addExecutable(.{
        .name = "antfly-kernel-jit-package",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/kernel_jit_package_main.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    kernel_jit_package_exe.root_module.link_libc = true;
    const install_kernel_jit_package = b.addInstallArtifact(kernel_jit_package_exe, .{});
    b.getInstallStep().dependOn(&install_kernel_jit_package.step);
    const kernel_jit_package_runner = if (targetRunsOnBuildHost(b, target))
        kernel_jit_package_exe
    else blk: {
        const host_exe = b.addExecutable(.{
            .name = "antfly-kernel-jit-package-host",
            .root_module = b.createModule(.{
                .root_source_file = b.path("src/kernel_jit_package_main.zig"),
                .target = b.graph.host,
                .optimize = optimize,
            }),
        });
        host_exe.root_module.link_libc = true;
        break :blk host_exe;
    };
    const run_kernel_jit_package = b.addRunArtifact(kernel_jit_package_runner);
    if (b.args) |args| run_kernel_jit_package.addArgs(args);
    const kernel_jit_package_step = b.step(
        "kernel-jit-package",
        "Export, verify, or import exact-match JIT qualification packages",
    );
    kernel_jit_package_step.dependOn(&kernel_jit_package_exe.step);
    kernel_jit_package_step.dependOn(&run_kernel_jit_package.step);

    const quant_kernel_codegen_exe = b.addExecutable(.{
        .name = "antfly-quant-kernel-codegen",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/quant_kernel_codegen_main.zig"),
            .target = b.graph.host,
            .optimize = optimize,
        }),
    });
    quant_kernel_codegen_exe.root_module.addImport("build_options", build_options_mod);
    quant_kernel_codegen_exe.root_module.link_libc = link_libc;
    const quant_kernel_codegen_check = b.addRunArtifact(quant_kernel_codegen_exe);
    if (b.args) |args| {
        quant_kernel_codegen_check.addArgs(args);
    } else {
        quant_kernel_codegen_check.addArg("--check");
    }
    const quant_kernel_codegen_test_check = b.addRunArtifact(quant_kernel_codegen_exe);
    quant_kernel_codegen_test_check.addArg("--check");
    const quant_kernel_codegen_step = b.step("quant-kernel-codegen", "Verify dev-generated quant kernel sources are fresh");
    quant_kernel_codegen_step.dependOn(&quant_kernel_codegen_check.step);

    const metal_unavailable_message = "Metal quant kernel evidence targets require a macOS target with xcrun/Metal; no Metal runtime evidence was run.";
    const quant_kernel_metal_check_step = b.step("quant-kernel-metal-check", "Compile generated and promoted Metal quant kernels");
    var quant_kernel_metal_artifact_check_step: ?*std.Build.Step = null;
    var quant_kernel_metal_unavailable_step: ?*std.Build.Step = null;
    if (target.result.os.tag == .macos) {
        const quant_kernel_metal_artifact_check = b.addRunArtifact(quant_kernel_codegen_exe);
        quant_kernel_metal_artifact_check.addArg("--check-metal");
        quant_kernel_metal_artifact_check.step.dependOn(&quant_kernel_codegen_test_check.step);
        quant_kernel_metal_artifact_check_step = &quant_kernel_metal_artifact_check.step;
        quant_kernel_metal_check_step.dependOn(&quant_kernel_metal_artifact_check.step);
    } else {
        const quant_kernel_metal_unavailable = b.addFail(metal_unavailable_message);
        quant_kernel_metal_unavailable_step = &quant_kernel_metal_unavailable.step;
        quant_kernel_metal_check_step.dependOn(&quant_kernel_metal_unavailable.step);
    }

    const quant_kernel_metal_runtime_check_step = b.step("quant-kernel-metal-runtime-check", "Run dev-only generated Metal quant kernel correctness check");
    const metal_decode_gqa_split_routes_step = b.step("test-metal-decode-gqa-split-routes", "Run only the Metal split-GQA production-route tensor oracle");
    const quant_kernel_metal_v2_conformance_step = b.step("quant-kernel-metal-v2-conformance", "Run rendered v2 Metal quant kernels against the CPU reference on device");
    const quant_kernel_metal_sweep_step = b.step("quant-kernel-metal-sweep", "Sweep rendered Metal quant kernel schedule variants and report per-route winners");
    const quant_kernel_metal_runtime_route_all_step = b.step("quant-kernel-metal-runtime-route-all", "Run dev-only generated Metal route-all evidence check");
    const quant_kernel_metal_production_regression_step = b.step("quant-kernel-metal-production-regression-check", "Run promoted Metal quant kernel production regression gate");
    const quant_kernel_metal_blocker_evidence_refresh_step = b.step("quant-kernel-metal-blocker-evidence-refresh", "Refresh local Metal promotion blocker evidence");
    const quant_kernel_metal_blocker_evidence_step = b.step("quant-kernel-metal-blocker-evidence-check", "Check saved Metal promotion blocker evidence");
    const quant_kernel_metal_blocker_strict_step = b.step("quant-kernel-metal-blocker-strict-check", "Fail if saved Metal blocker evidence clears and needs promotion review");
    var quant_kernel_metal_production_regression_run_step: ?*std.Build.Step = null;
    if (target.result.os.tag == .macos) {
        const quant_kernel_metal_runtime_check_exe = b.addExecutable(.{
            .name = "antfly-quant-kernel-metal-runtime-check",
            .root_module = b.createModule(.{
                .root_source_file = b.path("src/quant_kernel_metal_runtime_check.zig"),
                .target = target,
                .optimize = optimize,
            }),
        });
        configureMetal(b, quant_kernel_metal_runtime_check_exe.root_module, target, true);
        quant_kernel_metal_runtime_check_exe.root_module.link_libc = true;
        const run_quant_kernel_metal_runtime_check = b.addRunArtifact(quant_kernel_metal_runtime_check_exe);
        if (b.args) |args| {
            run_quant_kernel_metal_runtime_check.addArgs(args);
        }
        if (quant_kernel_metal_artifact_check_step) |metal_artifact_check_step| {
            run_quant_kernel_metal_runtime_check.step.dependOn(metal_artifact_check_step);
        }
        quant_kernel_metal_runtime_check_step.dependOn(&run_quant_kernel_metal_runtime_check.step);

        const run_metal_decode_gqa_split_routes = b.addRunArtifact(quant_kernel_metal_runtime_check_exe);
        run_metal_decode_gqa_split_routes.addArg("--split-gqa-only");
        if (quant_kernel_metal_artifact_check_step) |metal_artifact_check_step| {
            run_metal_decode_gqa_split_routes.step.dependOn(metal_artifact_check_step);
        }
        metal_decode_gqa_split_routes_step.dependOn(&run_metal_decode_gqa_split_routes.step);

        const run_quant_kernel_metal_v2_conformance = b.addRunArtifact(quant_kernel_metal_runtime_check_exe);
        run_quant_kernel_metal_v2_conformance.addArg("--v2-conformance");
        if (quant_kernel_metal_artifact_check_step) |metal_artifact_check_step| {
            run_quant_kernel_metal_v2_conformance.step.dependOn(metal_artifact_check_step);
        }
        quant_kernel_metal_v2_conformance_step.dependOn(&run_quant_kernel_metal_v2_conformance.step);

        const run_quant_kernel_metal_sweep = b.addRunArtifact(quant_kernel_metal_runtime_check_exe);
        run_quant_kernel_metal_sweep.addArg("--sweep");
        if (b.args) |args| {
            run_quant_kernel_metal_sweep.addArgs(args);
        }
        if (quant_kernel_metal_artifact_check_step) |metal_artifact_check_step| {
            run_quant_kernel_metal_sweep.step.dependOn(metal_artifact_check_step);
        }
        quant_kernel_metal_sweep_step.dependOn(&run_quant_kernel_metal_sweep.step);

        const run_quant_kernel_metal_runtime_route_all = b.addRunArtifact(quant_kernel_metal_runtime_check_exe);
        run_quant_kernel_metal_runtime_route_all.has_side_effects = true;
        run_quant_kernel_metal_runtime_route_all.addArg("--evidence-out");
        const route_all_evidence_name = b.fmt("antfly-quant-metal-runtime-route-all-evidence-{x}.json", .{b.graph.random_seed});
        const route_all_evidence = run_quant_kernel_metal_runtime_route_all.addOutputFileArg(route_all_evidence_name);
        run_quant_kernel_metal_runtime_route_all.addArg("--runtime-route-all");
        const check_quant_kernel_metal_runtime_route_all = b.addRunArtifact(quant_kernel_metal_runtime_check_exe);
        check_quant_kernel_metal_runtime_route_all.addArg("--check-evidence");
        check_quant_kernel_metal_runtime_route_all.addFileArg(route_all_evidence);
        check_quant_kernel_metal_runtime_route_all.addArg("--require-runtime-route-all");
        if (quant_kernel_metal_artifact_check_step) |metal_artifact_check_step| {
            run_quant_kernel_metal_runtime_route_all.step.dependOn(metal_artifact_check_step);
            check_quant_kernel_metal_runtime_route_all.step.dependOn(metal_artifact_check_step);
        }
        check_quant_kernel_metal_runtime_route_all.step.dependOn(&run_quant_kernel_metal_runtime_route_all.step);
        quant_kernel_metal_runtime_route_all_step.dependOn(&check_quant_kernel_metal_runtime_route_all.step);

        const run_quant_kernel_metal_production_regression = b.addRunArtifact(quant_kernel_metal_runtime_check_exe);
        run_quant_kernel_metal_production_regression.has_side_effects = true;
        run_quant_kernel_metal_production_regression.addArg("--evidence-out");
        const production_regression_evidence_name = b.fmt("antfly-quant-metal-production-regression-evidence-{x}.json", .{b.graph.random_seed});
        _ = run_quant_kernel_metal_production_regression.addOutputFileArg(production_regression_evidence_name);
        run_quant_kernel_metal_production_regression.addArgs(&.{
            "--repeat-runs",
            "5",
            "--measure-iters",
            "500",
            "--production-regression-check",
        });
        if (quant_kernel_metal_artifact_check_step) |metal_artifact_check_step| {
            run_quant_kernel_metal_production_regression.step.dependOn(metal_artifact_check_step);
        }
        quant_kernel_metal_production_regression_run_step = &run_quant_kernel_metal_production_regression.step;
        quant_kernel_metal_production_regression_step.dependOn(&run_quant_kernel_metal_production_regression.step);

        const refresh_quant_kernel_metal_blocker_evidence = b.addRunArtifact(quant_kernel_metal_runtime_check_exe);
        refresh_quant_kernel_metal_blocker_evidence.has_side_effects = true;
        refresh_quant_kernel_metal_blocker_evidence.addArgs(&.{
            "--refresh-blocker-evidence",
            "--blocker-evidence-dir",
        });
        const blocker_evidence_dir_name = b.fmt("antfly-quant-metal-blocker-evidence-{x}", .{b.graph.random_seed});
        const blocker_evidence_dir = refresh_quant_kernel_metal_blocker_evidence.addOutputDirectoryArg(blocker_evidence_dir_name);
        if (quant_kernel_metal_artifact_check_step) |metal_artifact_check_step| {
            refresh_quant_kernel_metal_blocker_evidence.step.dependOn(metal_artifact_check_step);
        }
        refresh_quant_kernel_metal_blocker_evidence.step.dependOn(&check_quant_kernel_metal_runtime_route_all.step);
        quant_kernel_metal_blocker_evidence_refresh_step.dependOn(&refresh_quant_kernel_metal_blocker_evidence.step);

        const check_quant_kernel_metal_blocker_evidence = b.addRunArtifact(quant_kernel_metal_runtime_check_exe);
        check_quant_kernel_metal_blocker_evidence.addArgs(&.{
            "--check-blocker-evidence",
            "--blocker-evidence-dir",
        });
        check_quant_kernel_metal_blocker_evidence.addDirectoryArg(blocker_evidence_dir);
        if (quant_kernel_metal_artifact_check_step) |metal_artifact_check_step| {
            check_quant_kernel_metal_blocker_evidence.step.dependOn(metal_artifact_check_step);
        }
        check_quant_kernel_metal_blocker_evidence.step.dependOn(&refresh_quant_kernel_metal_blocker_evidence.step);
        quant_kernel_metal_blocker_evidence_step.dependOn(&check_quant_kernel_metal_blocker_evidence.step);

        const strict_quant_kernel_metal_blocker_evidence = b.addRunArtifact(quant_kernel_metal_runtime_check_exe);
        strict_quant_kernel_metal_blocker_evidence.addArgs(&.{
            "--check-blocker-evidence",
            "--confirm-cleared-blockers",
            "--fail-on-cleared-blocker",
            "--blocker-evidence-dir",
        });
        strict_quant_kernel_metal_blocker_evidence.addDirectoryArg(blocker_evidence_dir);
        if (quant_kernel_metal_artifact_check_step) |metal_artifact_check_step| {
            strict_quant_kernel_metal_blocker_evidence.step.dependOn(metal_artifact_check_step);
        }
        strict_quant_kernel_metal_blocker_evidence.step.dependOn(&refresh_quant_kernel_metal_blocker_evidence.step);
        quant_kernel_metal_blocker_strict_step.dependOn(&strict_quant_kernel_metal_blocker_evidence.step);
    } else {
        const metal_unavailable_step = quant_kernel_metal_unavailable_step orelse unreachable;
        quant_kernel_metal_runtime_check_step.dependOn(metal_unavailable_step);
        metal_decode_gqa_split_routes_step.dependOn(metal_unavailable_step);
        quant_kernel_metal_v2_conformance_step.dependOn(metal_unavailable_step);
        quant_kernel_metal_sweep_step.dependOn(metal_unavailable_step);
        quant_kernel_metal_runtime_route_all_step.dependOn(metal_unavailable_step);
        quant_kernel_metal_production_regression_step.dependOn(metal_unavailable_step);
        quant_kernel_metal_blocker_evidence_refresh_step.dependOn(metal_unavailable_step);
        quant_kernel_metal_blocker_evidence_step.dependOn(metal_unavailable_step);
        quant_kernel_metal_blocker_strict_step.dependOn(metal_unavailable_step);
    }
    const quant_kernel_metal_runtime_check_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/quant_kernel_metal_runtime_check.zig"),
            .target = target,
            .optimize = optimize,
        }),
        .filters = &.{"quant kernel metal runtime"},
    });
    const run_quant_kernel_metal_runtime_check_tests = b.addRunArtifact(quant_kernel_metal_runtime_check_tests);
    if (target.result.os.tag == .macos and targetRunsOnBuildHost(b, target)) {
        if (quant_kernel_metal_production_regression_run_step) |production_regression_step| {
            production_regression_step.dependOn(&run_quant_kernel_metal_runtime_check_tests.step);
        }
        quant_kernel_metal_runtime_check_step.dependOn(&run_quant_kernel_metal_runtime_check_tests.step);
    }

    const cuda_artifact_source_policy_check = b.addSystemCommand(&.{
        "bash",
        "scripts/regen-cuda-artifacts.sh",
        "--check-source-policy",
    });
    const cuda_artifacts_freshness_check = b.addSystemCommand(&.{
        "bash",
        "scripts/regen-cuda-artifacts.sh",
        "--check",
        "--all",
    });
    const cuda_artifacts_check_step = b.step("cuda-artifacts-check", "Verify checked-in CUDA artifacts with CUDA 13.2");
    cuda_artifacts_check_step.dependOn(&cuda_artifacts_freshness_check.step);

    const quant_kernel_cuda_attention_diff_step = b.step(
        "quant-kernel-cuda-attention-diff",
        "Run raw handwritten-vs-generated CUDA decode-attention differential checks",
    );
    if (enable_cuda and targetRunsOnBuildHost(b, target)) {
        const quant_kernel_cuda_attention_diff_exe = b.addExecutable(.{
            .name = "antfly-quant-kernel-cuda-attention-diff",
            .root_module = b.createModule(.{
                .root_source_file = b.path("src/quant_kernel_cuda_attention_diff.zig"),
                .target = target,
                .optimize = optimize,
            }),
        });
        quant_kernel_cuda_attention_diff_exe.root_module.addImport("build_options", build_options_mod);
        quant_kernel_cuda_attention_diff_exe.root_module.link_libc = true;
        const run_quant_kernel_cuda_attention_diff = b.addRunArtifact(quant_kernel_cuda_attention_diff_exe);
        if (b.args) |args| run_quant_kernel_cuda_attention_diff.addArgs(args);
        run_quant_kernel_cuda_attention_diff.step.dependOn(&cuda_artifact_source_policy_check.step);
        quant_kernel_cuda_attention_diff_step.dependOn(&run_quant_kernel_cuda_attention_diff.step);
    } else {
        const cuda_attention_diff_unavailable = b.addFail(
            "quant-kernel-cuda-attention-diff requires a host CUDA build; pass -Dcuda=true",
        );
        quant_kernel_cuda_attention_diff_step.dependOn(&cuda_attention_diff_unavailable.step);
    }

    const quant_kernel_cuda_paged_attention_diff_step = b.step(
        "quant-kernel-cuda-paged-attention-diff",
        "Run raw production-vs-generated paged CUDA decode-attention differential checks",
    );
    if (enable_cuda and targetRunsOnBuildHost(b, target)) {
        const compile_score_prework_hd256 = b.addSystemCommand(&.{
            "bash",
            "scripts/compile-generated-cuda-candidate.sh",
        });
        compile_score_prework_hd256.addFileArg(b.path("src/ops/cuda/generated/attention_decode_score_prework_hd256.cu"));
        const score_prework_hd256_cubin = compile_score_prework_hd256.addOutputFileArg("attention_decode_score_prework_hd256.sm89.cubin");

        const compile_score_prework_hd512 = b.addSystemCommand(&.{
            "bash",
            "scripts/compile-generated-cuda-candidate.sh",
        });
        compile_score_prework_hd512.addFileArg(b.path("src/ops/cuda/generated/attention_decode_score_prework_hd512.cu"));
        const score_prework_hd512_cubin = compile_score_prework_hd512.addOutputFileArg("attention_decode_score_prework_hd512.sm89.cubin");

        const quant_kernel_cuda_paged_attention_diff_exe = b.addExecutable(.{
            .name = "antfly-quant-kernel-cuda-paged-attention-diff",
            .root_module = b.createModule(.{
                .root_source_file = b.path("src/quant_kernel_cuda_paged_attention_diff.zig"),
                .target = target,
                .optimize = optimize,
            }),
        });
        quant_kernel_cuda_paged_attention_diff_exe.root_module.addImport("build_options", build_options_mod);
        quant_kernel_cuda_paged_attention_diff_exe.root_module.link_libc = true;
        const quant_kernel_cuda_paged_attention_diff_tests = b.addTest(.{
            .root_module = b.createModule(.{
                .root_source_file = b.path("src/quant_kernel_cuda_paged_attention_diff.zig"),
                .target = target,
                .optimize = optimize,
            }),
        });
        quant_kernel_cuda_paged_attention_diff_tests.root_module.addImport("build_options", build_options_mod);
        quant_kernel_cuda_paged_attention_diff_tests.root_module.link_libc = true;
        const run_quant_kernel_cuda_paged_attention_diff_tests = b.addRunArtifact(quant_kernel_cuda_paged_attention_diff_tests);
        const run_quant_kernel_cuda_paged_attention_diff = b.addRunArtifact(quant_kernel_cuda_paged_attention_diff_exe);
        run_quant_kernel_cuda_paged_attention_diff.addArg("--candidate-hd256");
        run_quant_kernel_cuda_paged_attention_diff.addFileArg(score_prework_hd256_cubin);
        run_quant_kernel_cuda_paged_attention_diff.addArg("--candidate-hd512");
        run_quant_kernel_cuda_paged_attention_diff.addFileArg(score_prework_hd512_cubin);
        if (b.args) |args| run_quant_kernel_cuda_paged_attention_diff.addArgs(args);
        run_quant_kernel_cuda_paged_attention_diff.step.dependOn(&cuda_artifact_source_policy_check.step);
        run_quant_kernel_cuda_paged_attention_diff.step.dependOn(&run_quant_kernel_cuda_paged_attention_diff_tests.step);
        quant_kernel_cuda_paged_attention_diff_step.dependOn(&run_quant_kernel_cuda_paged_attention_diff.step);
    } else {
        const cuda_paged_attention_diff_unavailable = b.addFail(
            "quant-kernel-cuda-paged-attention-diff requires a host CUDA build; pass -Dcuda=true",
        );
        quant_kernel_cuda_paged_attention_diff_step.dependOn(&cuda_paged_attention_diff_unavailable.step);
    }

    const quant_kernel_cuda_paged_prefill_diff_step = b.step(
        "quant-kernel-cuda-paged-prefill-diff",
        "Run embedded CUDA paged-prefill attention differential and timing checks",
    );
    if (enable_cuda and targetRunsOnBuildHost(b, target)) {
        const quant_kernel_cuda_paged_prefill_diff_exe = b.addExecutable(.{
            .name = "antfly-quant-kernel-cuda-paged-prefill-diff",
            .root_module = b.createModule(.{
                .root_source_file = b.path("src/quant_kernel_cuda_paged_prefill_diff.zig"),
                .target = target,
                .optimize = optimize,
            }),
        });
        quant_kernel_cuda_paged_prefill_diff_exe.root_module.addImport("build_options", build_options_mod);
        quant_kernel_cuda_paged_prefill_diff_exe.root_module.link_libc = true;
        const quant_kernel_cuda_paged_prefill_diff_tests = b.addTest(.{
            .root_module = b.createModule(.{
                .root_source_file = b.path("src/quant_kernel_cuda_paged_prefill_diff.zig"),
                .target = target,
                .optimize = optimize,
            }),
        });
        quant_kernel_cuda_paged_prefill_diff_tests.root_module.addImport("build_options", build_options_mod);
        quant_kernel_cuda_paged_prefill_diff_tests.root_module.link_libc = true;
        const run_quant_kernel_cuda_paged_prefill_diff_tests = b.addRunArtifact(quant_kernel_cuda_paged_prefill_diff_tests);
        const run_quant_kernel_cuda_paged_prefill_diff = b.addRunArtifact(quant_kernel_cuda_paged_prefill_diff_exe);
        if (b.args) |args| run_quant_kernel_cuda_paged_prefill_diff.addArgs(args);
        run_quant_kernel_cuda_paged_prefill_diff.step.dependOn(&cuda_artifact_source_policy_check.step);
        run_quant_kernel_cuda_paged_prefill_diff.step.dependOn(&cuda_artifacts_freshness_check.step);
        run_quant_kernel_cuda_paged_prefill_diff.step.dependOn(&run_quant_kernel_cuda_paged_prefill_diff_tests.step);
        quant_kernel_cuda_paged_prefill_diff_step.dependOn(&run_quant_kernel_cuda_paged_prefill_diff.step);
    } else {
        const cuda_paged_prefill_diff_unavailable = b.addFail(
            "quant-kernel-cuda-paged-prefill-diff requires a host CUDA build; pass -Dcuda=true",
        );
        quant_kernel_cuda_paged_prefill_diff_step.dependOn(&cuda_paged_prefill_diff_unavailable.step);
    }

    const quant_kernel_cuda_ffn_diff_step = b.step(
        "quant-kernel-cuda-ffn-diff",
        "Run raw handwritten-vs-generated CUDA exact F32 E2B FFN differential checks",
    );
    if (enable_cuda and targetRunsOnBuildHost(b, target)) {
        const quant_kernel_cuda_ffn_diff_exe = b.addExecutable(.{
            .name = "antfly-quant-kernel-cuda-ffn-diff",
            .root_module = b.createModule(.{
                .root_source_file = b.path("src/quant_kernel_cuda_ffn_diff.zig"),
                .target = target,
                .optimize = optimize,
            }),
        });
        quant_kernel_cuda_ffn_diff_exe.root_module.addImport("build_options", build_options_mod);
        quant_kernel_cuda_ffn_diff_exe.root_module.link_libc = true;
        const run_quant_kernel_cuda_ffn_diff = b.addRunArtifact(quant_kernel_cuda_ffn_diff_exe);
        if (b.args) |args| run_quant_kernel_cuda_ffn_diff.addArgs(args);
        run_quant_kernel_cuda_ffn_diff.step.dependOn(&cuda_artifact_source_policy_check.step);
        quant_kernel_cuda_ffn_diff_step.dependOn(&run_quant_kernel_cuda_ffn_diff.step);
    } else {
        const cuda_ffn_diff_unavailable = b.addFail(
            "quant-kernel-cuda-ffn-diff requires a host CUDA build; pass -Dcuda=true",
        );
        quant_kernel_cuda_ffn_diff_step.dependOn(&cuda_ffn_diff_unavailable.step);
    }

    const quant_kernel_local_check_step = b.step("quant-kernel-local-check", "Run local quant kernel compiler readiness checks");
    const quant_kernel_metal_local_check_step = b.step("quant-kernel-metal-local-check", "Run local Metal quant kernel compiler readiness checks");
    quant_kernel_metal_local_check_step.dependOn(quant_kernel_metal_check_step);
    if (target.result.os.tag == .macos and targetRunsOnBuildHost(b, target)) {
        quant_kernel_metal_local_check_step.dependOn(&run_quant_kernel_metal_runtime_check_tests.step);
    }
    quant_kernel_metal_local_check_step.dependOn(quant_kernel_metal_runtime_route_all_step);
    quant_kernel_metal_local_check_step.dependOn(quant_kernel_metal_production_regression_step);
    quant_kernel_metal_local_check_step.dependOn(quant_kernel_metal_blocker_evidence_step);
    quant_kernel_metal_local_check_step.dependOn(quant_kernel_metal_blocker_strict_step);
    quant_kernel_local_check_step.dependOn(&quant_kernel_codegen_test_check.step);
    quant_kernel_local_check_step.dependOn(&cuda_artifact_source_policy_check.step);
    if (enable_metal and target.result.os.tag == .macos and targetRunsOnBuildHost(b, target)) {
        quant_kernel_local_check_step.dependOn(quant_kernel_metal_local_check_step);
    }

    const metal_gemma4_prefill_frame_script_self_test = b.addSystemCommand(&.{
        "bash",
        "scripts/test_metal_gemma4_prefill_frame.sh",
        "--self-test",
    });
    const metal_gemma4_prefill_frame_script_self_test_step = b.step(
        "test-metal-gemma4-prefill-frame-script",
        "Run the Metal Gemma4 prefill-frame smoke script self-test",
    );
    metal_gemma4_prefill_frame_script_self_test_step.dependOn(&metal_gemma4_prefill_frame_script_self_test.step);
    if (target.result.os.tag == .macos and targetRunsOnBuildHost(b, target)) {
        quant_kernel_metal_local_check_step.dependOn(&metal_gemma4_prefill_frame_script_self_test.step);
    }

    const metal_quant_summary_check_self_test = b.addSystemCommand(&.{
        "python3",
        "scripts/check_metal_quant_summary.py",
        "--self-test",
    });
    const metal_quant_summary_check_self_test_step = b.step(
        "test-metal-quant-summary-check",
        "Run the Metal quant summary checker self-test",
    );
    metal_quant_summary_check_self_test_step.dependOn(&metal_quant_summary_check_self_test.step);
    if (target.result.os.tag == .macos and targetRunsOnBuildHost(b, target)) {
        quant_kernel_metal_local_check_step.dependOn(&metal_quant_summary_check_self_test.step);
    }

    const metal_gemma4_bench_script_self_test = b.addSystemCommand(&.{
        "bash",
        "scripts/bench_metal_gemma4_e2b.sh",
        "--self-test",
    });
    const metal_gemma4_bench_script_self_test_step = b.step(
        "test-metal-gemma4-bench-script",
        "Run the Metal Gemma4 bench script self-test",
    );
    metal_gemma4_bench_script_self_test_step.dependOn(&metal_gemma4_bench_script_self_test.step);
    if (target.result.os.tag == .macos and targetRunsOnBuildHost(b, target)) {
        quant_kernel_metal_local_check_step.dependOn(&metal_gemma4_bench_script_self_test.step);
    }

    const metal_gemma4_prefill_frame_test = b.addSystemCommand(&.{
        "bash",
        "scripts/test_metal_gemma4_prefill_frame.sh",
        "--antfly-bin",
    });
    metal_gemma4_prefill_frame_test.addFileArg(exe.getEmittedBin());
    const metal_gemma4_prefill_frame_test_step = b.step(
        "test-metal-gemma4-prefill-frame",
        "Run the local Metal Gemma4 handwritten/generated stage-sync parity smoke test",
    );
    metal_gemma4_prefill_frame_test_step.dependOn(&metal_gemma4_prefill_frame_test.step);

    const metal_gemma4_prefill_frame_generated_q8_test = b.addSystemCommand(&.{
        "bash",
        "scripts/test_metal_gemma4_prefill_frame.sh",
        "--antfly-bin",
    });
    metal_gemma4_prefill_frame_generated_q8_test.addFileArg(exe.getEmittedBin());
    metal_gemma4_prefill_frame_generated_q8_test.addArg(
        "--generated-q8-smoke",
    );
    const metal_gemma4_prefill_frame_generated_q8_test_step = b.step(
        "test-metal-gemma4-prefill-frame-generated-q8",
        "Run the local Metal Gemma4 generated-Q8_0 parity smoke against handwritten",
    );
    metal_gemma4_prefill_frame_generated_q8_test_step.dependOn(&metal_gemma4_prefill_frame_generated_q8_test.step);

    const metal_gemma4_prefill_frame_e4b_test = b.addSystemCommand(&.{
        "bash",
        "scripts/test_metal_gemma4_prefill_frame.sh",
        "--antfly-bin",
    });
    metal_gemma4_prefill_frame_e4b_test.addFileArg(exe.getEmittedBin());
    metal_gemma4_prefill_frame_e4b_test.addArg(
        "--e4b-smoke",
    );
    const metal_gemma4_prefill_frame_e4b_test_step = b.step(
        "test-metal-gemma4-prefill-frame-e4b",
        "Run the local Metal Gemma4 E4B handwritten/generated stage-sync parity smoke",
    );
    metal_gemma4_prefill_frame_e4b_test_step.dependOn(&metal_gemma4_prefill_frame_e4b_test.step);

    const metal_gemma4_prefill_frame_e4b_generated_q8_test = b.addSystemCommand(&.{
        "bash",
        "scripts/test_metal_gemma4_prefill_frame.sh",
        "--e4b-smoke",
        "--generated-q8-smoke",
        "--antfly-bin",
    });
    metal_gemma4_prefill_frame_e4b_generated_q8_test.addFileArg(exe.getEmittedBin());
    const metal_gemma4_prefill_frame_e4b_generated_q8_test_step = b.step(
        "test-metal-gemma4-prefill-frame-e4b-generated-q8",
        "Run the local Metal Gemma4 E4B generated-Q8_0 parity smoke against handwritten",
    );
    metal_gemma4_prefill_frame_e4b_generated_q8_test_step.dependOn(&metal_gemma4_prefill_frame_e4b_generated_q8_test.step);

    const metal_gemma4_prefill_frame_e4b_generated_q8_q4_0_test = b.addSystemCommand(&.{
        "env",
        "ANTFLY_INFERENCE_GEMMA4_MIN_GENERATED_Q4_0_SMALL_BATCH=1",
        "ANTFLY_INFERENCE_GEMMA4_MIN_GENERATED_FAMILY_COUNT=2",
        "ANTFLY_INFERENCE_GEMMA4_EXPECTED_GENERATED_TOP_FAMILY=q4_0",
        "ANTFLY_INFERENCE_GEMMA4_MIN_GENERATED_TOP_COUNT=1",
        "bash",
        "scripts/test_metal_gemma4_prefill_frame.sh",
        "--e4b-smoke",
        "--generated-q8-smoke",
        "--antfly-bin",
    });
    metal_gemma4_prefill_frame_e4b_generated_q8_q4_0_test.addFileArg(exe.getEmittedBin());
    const metal_gemma4_prefill_frame_e4b_generated_q8_q4_0_test_step = b.step(
        "test-metal-gemma4-prefill-frame-e4b-generated-q8-q4-0",
        "Run the local Metal Gemma4 E4B generated-Q8_0/Q4_0 parity smoke against handwritten",
    );
    metal_gemma4_prefill_frame_e4b_generated_q8_q4_0_test_step.dependOn(&metal_gemma4_prefill_frame_e4b_generated_q8_q4_0_test.step);

    const quant_kernel_metal_model_local_check_step = b.step(
        "quant-kernel-metal-model-local-check",
        "Run local model-level Metal quant kernel smoke checks",
    );
    quant_kernel_metal_model_local_check_step.dependOn(quant_kernel_metal_local_check_step);
    if (target.result.os.tag == .macos and targetRunsOnBuildHost(b, target)) {
        quant_kernel_metal_model_local_check_step.dependOn(&metal_gemma4_prefill_frame_e4b_generated_q8_q4_0_test.step);
    }

    const quant_kernel_metal_industry_local_check_step = b.step(
        "quant-kernel-metal-industry-local-check",
        "Run local Metal quant kernel readiness checks plus model smoke checks",
    );
    quant_kernel_metal_industry_local_check_step.dependOn(quant_kernel_metal_local_check_step);
    quant_kernel_metal_industry_local_check_step.dependOn(quant_kernel_metal_model_local_check_step);

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

    const metal_gemma4_mtp_long_context_test = b.addSystemCommand(&.{
        "bash",
        "scripts/test_metal_gemma4_mtp_long_context.sh",
    });
    metal_gemma4_mtp_long_context_test.step.dependOn(b.getInstallStep());
    const metal_gemma4_mtp_long_context_test_step = b.step(
        "test-metal-gemma4-mtp-long-context",
        "Run the 2,003-token Metal Gemma4 target/MTP exact-token regression gate",
    );
    metal_gemma4_mtp_long_context_test_step.dependOn(&metal_gemma4_mtp_long_context_test.step);
    if (target.result.os.tag == .macos and targetRunsOnBuildHost(b, target)) {
        quant_kernel_metal_model_local_check_step.dependOn(&metal_gemma4_mtp_long_context_test.step);
    }

    const metal_gemma4_long_output_benchmark_contract_test = b.addSystemCommand(&.{
        "python3",
        "scripts/test_benchmark_metal_gemma4_long_output.py",
    });
    const metal_gemma4_long_output_benchmark_contract_test_step = b.step(
        "test-metal-gemma4-long-output-benchmark",
        "Test the paired Gemma4 2K-prompt/300-output benchmark contract",
    );
    metal_gemma4_long_output_benchmark_contract_test_step.dependOn(&metal_gemma4_long_output_benchmark_contract_test.step);

    const metal_gemma4_ab_benchmark_contract_test = b.addSystemCommand(&.{
        "python3",
        "scripts/test_benchmark_metal_gemma4_ab.py",
    });
    const metal_gemma4_ab_benchmark_contract_test_step = b.step(
        "test-metal-gemma4-ab-benchmark",
        "Test the fail-closed Gemma4 Metal baseline/candidate benchmark contract",
    );
    metal_gemma4_ab_benchmark_contract_test_step.dependOn(&metal_gemma4_ab_benchmark_contract_test.step);

    const metal_gemma4_benchmark_contracts_test_step = b.step(
        "test-metal-gemma4-benchmark-contracts",
        "Test the Gemma4 Metal comparator and baseline/candidate benchmark contracts",
    );
    metal_gemma4_benchmark_contracts_test_step.dependOn(metal_gemma4_long_output_benchmark_contract_test_step);
    metal_gemma4_benchmark_contracts_test_step.dependOn(metal_gemma4_ab_benchmark_contract_test_step);

    const metal_gemma4_tool_calling_test = b.addSystemCommand(&.{
        "bash",
        "scripts/test_metal_gemma4_tool_calling.sh",
    });
    metal_gemma4_tool_calling_test.step.dependOn(b.getInstallStep());
    const metal_gemma4_tool_calling_test_step = b.step(
        "test-metal-gemma4-tool-calling",
        "Run the local Metal Gemma4 server tool-calling smoke test",
    );
    metal_gemma4_tool_calling_test_step.dependOn(&metal_gemma4_tool_calling_test.step);

    const metal_gemma4_cli_tool_calling_test = b.addSystemCommand(&.{
        "bash",
        "scripts/test_metal_gemma4_cli_tool_calling.sh",
    });
    metal_gemma4_cli_tool_calling_test.step.dependOn(b.getInstallStep());
    const metal_gemma4_cli_tool_calling_test_step = b.step(
        "diagnose-metal-gemma4-cli-tool-calling",
        "Diagnose the optional Metal Gemma4 CLI tool-calling surface; server coverage lives in test-metal-gemma4-tool-calling",
    );
    metal_gemma4_cli_tool_calling_test_step.dependOn(&metal_gemma4_cli_tool_calling_test.step);

    const metal_gemma4_e2b_bench = b.addSystemCommand(&.{
        "bash",
        "scripts/bench_metal_gemma4_e2b.sh",
    });
    metal_gemma4_e2b_bench.step.dependOn(b.getInstallStep());
    const metal_gemma4_e2b_bench_step = b.step(
        "bench-metal-gemma4-e2b",
        "Run the canonical local Metal Gemma4 E2B Q8_0 throughput benchmark",
    );
    metal_gemma4_e2b_bench_step.dependOn(&metal_gemma4_e2b_bench.step);

    const metal_gemma4_e4b_bench = b.addSystemCommand(&.{
        "bash",
        "scripts/bench_metal_gemma4_e4b.sh",
    });
    metal_gemma4_e4b_bench.step.dependOn(b.getInstallStep());
    const metal_gemma4_e4b_bench_step = b.step(
        "bench-metal-gemma4-e4b",
        "Run the canonical local Metal Gemma4 E4B Q4_K/Q6_K throughput benchmark",
    );
    metal_gemma4_e4b_bench_step.dependOn(&metal_gemma4_e4b_bench.step);

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

    const metal_bench_exe = b.addExecutable(.{
        .name = "antfly-inference-metal-bench",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/bench/metal_q4_0_linear.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    metal_bench_exe.root_module.addImport("build_options", build_options_mod);
    metal_bench_exe.root_module.addImport("inference_internal", inference_internal_mod);
    // inference_internal already owns the native backend links. Configuring
    // Metal again here compiles metal_kernels.m twice into this executable.
    metal_bench_exe.root_module.link_libc = true;
    const run_metal_bench = b.addRunArtifact(metal_bench_exe);
    if (b.args) |args| {
        run_metal_bench.addArgs(args);
    }
    const metal_bench_step = b.step(
        "inference-metal-bench",
        "Run the focused Metal kernel benchmark; pass --mode and shape filters after --",
    );
    metal_bench_step.dependOn(&run_metal_bench.step);

    const q4_mm_aligned_route_check = b.addRunArtifact(metal_bench_exe);
    q4_mm_aligned_route_check.addArgs(&.{ "--mode", "linear", "--rows", "32", "--in", "2560", "--out", "4096", "--warmup", "1", "--iters", "1", "--expect-q4-route", "aligned", "--expect-output-hash", "cdaed85c5db03f09" });
    const q4_mm_aligned_tail_route_check = b.addRunArtifact(metal_bench_exe);
    q4_mm_aligned_tail_route_check.addArgs(&.{ "--mode", "linear", "--rows", "31", "--in", "2560", "--out", "512", "--warmup", "1", "--iters", "1", "--expect-q4-route", "aligned-tail", "--expect-output-hash", "eb66cbc058bfed68" });
    q4_mm_aligned_tail_route_check.step.dependOn(&q4_mm_aligned_route_check.step);
    const q4_mm_ragged_tail_route_check = b.addRunArtifact(metal_bench_exe);
    q4_mm_ragged_tail_route_check.addArgs(&.{ "--mode", "linear", "--rows", "2003", "--in", "2560", "--out", "10240", "--warmup", "1", "--iters", "1", "--expect-q4-route", "aligned-tail", "--expect-output-hash", "96347637560a304a" });
    q4_mm_ragged_tail_route_check.step.dependOn(&q4_mm_aligned_tail_route_check.step);
    const q4_mm_ragged_tail_down_route_check = b.addRunArtifact(metal_bench_exe);
    q4_mm_ragged_tail_down_route_check.addArgs(&.{ "--mode", "linear", "--rows", "2003", "--in", "10240", "--out", "2560", "--warmup", "1", "--iters", "1", "--expect-q4-route", "aligned-tail", "--expect-output-hash", "56067182fae278a" });
    q4_mm_ragged_tail_down_route_check.step.dependOn(&q4_mm_ragged_tail_route_check.step);
    const q4_mm_unrolled_route_check = b.addRunArtifact(metal_bench_exe);
    q4_mm_unrolled_route_check.addArgs(&.{ "--mode", "linear", "--rows", "32", "--in", "2048", "--out", "2560", "--warmup", "1", "--iters", "1", "--expect-q4-route", "unrolled", "--expect-output-hash", "cb1571d002d961f3" });
    q4_mm_unrolled_route_check.step.dependOn(&q4_mm_ragged_tail_down_route_check.step);
    const q4_mm_route_check_step = b.step(
        "test-metal-q4-0-mm-routes",
        "Run on-device assertions for aligned, ragged-tail, and unrolled Q4_0 MM routes",
    );
    q4_mm_route_check_step.dependOn(&q4_mm_unrolled_route_check.step);
    if (target.result.os.tag == .macos and targetRunsOnBuildHost(b, target)) {
        quant_kernel_metal_local_check_step.dependOn(&q4_mm_unrolled_route_check.step);
    }

    const q4_mmv_nr4_nsg2_route_check = b.addRunArtifact(metal_bench_exe);
    q4_mmv_nr4_nsg2_route_check.setEnvironmentVariable("TERMITE_METAL_Q4_0_MMV_VARIANT", "nr4-nsg2");
    q4_mmv_nr4_nsg2_route_check.setEnvironmentVariable("TERMITE_METAL_DISABLE_Q4_0_MMV_PORTFOLIO", "0");
    q4_mmv_nr4_nsg2_route_check.setEnvironmentVariable("TERMITE_METAL_DISABLE_Q4_0_SMALL_REDUCE", "0");
    q4_mmv_nr4_nsg2_route_check.addArgs(&.{ "--mode", "linear", "--rows", "1", "--in", "256", "--out", "256", "--warmup", "1", "--iters", "1", "--expect-q4-mmv-variant", "nr4-nsg2", "--expect-output-hash", "72bd43f1b2aa7f8e" });
    const q4_mmv_nr8_nsg2_route_check = b.addRunArtifact(metal_bench_exe);
    q4_mmv_nr8_nsg2_route_check.setEnvironmentVariable("TERMITE_METAL_Q4_0_MMV_VARIANT", "nr8-nsg2");
    q4_mmv_nr8_nsg2_route_check.setEnvironmentVariable("TERMITE_METAL_DISABLE_Q4_0_MMV_PORTFOLIO", "0");
    q4_mmv_nr8_nsg2_route_check.setEnvironmentVariable("TERMITE_METAL_DISABLE_Q4_0_SMALL_REDUCE", "0");
    q4_mmv_nr8_nsg2_route_check.addArgs(&.{ "--mode", "linear", "--rows", "1", "--in", "256", "--out", "256", "--warmup", "1", "--iters", "1", "--expect-q4-mmv-variant", "nr8-nsg2", "--expect-output-hash", "72bd43f1b2aa7f8e" });
    q4_mmv_nr8_nsg2_route_check.step.dependOn(&q4_mmv_nr4_nsg2_route_check.step);
    const q4_mmv_nr4_nsg4_route_check = b.addRunArtifact(metal_bench_exe);
    q4_mmv_nr4_nsg4_route_check.setEnvironmentVariable("TERMITE_METAL_Q4_0_MMV_VARIANT", "nr4-nsg4");
    q4_mmv_nr4_nsg4_route_check.setEnvironmentVariable("TERMITE_METAL_DISABLE_Q4_0_MMV_PORTFOLIO", "0");
    q4_mmv_nr4_nsg4_route_check.setEnvironmentVariable("TERMITE_METAL_DISABLE_Q4_0_SMALL_REDUCE", "0");
    q4_mmv_nr4_nsg4_route_check.addArgs(&.{ "--mode", "linear", "--rows", "1", "--in", "256", "--out", "256", "--warmup", "1", "--iters", "1", "--expect-q4-mmv-variant", "nr4-nsg4", "--expect-output-hash", "72bd43f1b2aa7f8e" });
    q4_mmv_nr4_nsg4_route_check.step.dependOn(&q4_mmv_nr8_nsg2_route_check.step);
    const q4_mmv_nr8_nsg4_route_check = b.addRunArtifact(metal_bench_exe);
    q4_mmv_nr8_nsg4_route_check.setEnvironmentVariable("TERMITE_METAL_Q4_0_MMV_VARIANT", "nr8-nsg4");
    q4_mmv_nr8_nsg4_route_check.setEnvironmentVariable("TERMITE_METAL_DISABLE_Q4_0_MMV_PORTFOLIO", "0");
    q4_mmv_nr8_nsg4_route_check.setEnvironmentVariable("TERMITE_METAL_DISABLE_Q4_0_SMALL_REDUCE", "0");
    q4_mmv_nr8_nsg4_route_check.addArgs(&.{ "--mode", "linear", "--rows", "1", "--in", "256", "--out", "256", "--warmup", "1", "--iters", "1", "--expect-q4-mmv-variant", "nr8-nsg4", "--expect-output-hash", "72bd43f1b2aa7f8e" });
    q4_mmv_nr8_nsg4_route_check.step.dependOn(&q4_mmv_nr4_nsg4_route_check.step);
    const q4_mmv_auto_gate_route_check = b.addRunArtifact(metal_bench_exe);
    q4_mmv_auto_gate_route_check.setEnvironmentVariable("TERMITE_METAL_Q4_0_MMV_VARIANT", "auto");
    q4_mmv_auto_gate_route_check.setEnvironmentVariable("TERMITE_METAL_DISABLE_Q4_0_MMV_PORTFOLIO", "0");
    q4_mmv_auto_gate_route_check.setEnvironmentVariable("TERMITE_METAL_DISABLE_Q4_0_SMALL_REDUCE", "0");
    q4_mmv_auto_gate_route_check.addArgs(&.{ "--mode", "linear", "--rows", "1", "--in", "2560", "--out", "10240", "--warmup", "1", "--iters", "1", "--expect-q4-mmv-auto", "--expect-output-hash", "b4826b6e363a4695" });
    q4_mmv_auto_gate_route_check.step.dependOn(&q4_mmv_nr8_nsg4_route_check.step);
    const q4_mmv_auto_down_route_check = b.addRunArtifact(metal_bench_exe);
    q4_mmv_auto_down_route_check.setEnvironmentVariable("TERMITE_METAL_Q4_0_MMV_VARIANT", "auto");
    q4_mmv_auto_down_route_check.setEnvironmentVariable("TERMITE_METAL_DISABLE_Q4_0_MMV_PORTFOLIO", "0");
    q4_mmv_auto_down_route_check.setEnvironmentVariable("TERMITE_METAL_DISABLE_Q4_0_SMALL_REDUCE", "0");
    q4_mmv_auto_down_route_check.addArgs(&.{ "--mode", "linear", "--rows", "1", "--in", "10240", "--out", "2560", "--warmup", "1", "--iters", "1", "--expect-q4-mmv-auto", "--expect-output-hash", "2b8a37cc63efcfec" });
    q4_mmv_auto_down_route_check.step.dependOn(&q4_mmv_auto_gate_route_check.step);
    const q4_mmv_kill_switch_route_check = b.addRunArtifact(metal_bench_exe);
    q4_mmv_kill_switch_route_check.setEnvironmentVariable("TERMITE_METAL_Q4_0_MMV_VARIANT", "nr8-nsg4");
    q4_mmv_kill_switch_route_check.setEnvironmentVariable("TERMITE_METAL_DISABLE_Q4_0_MMV_PORTFOLIO", "1");
    q4_mmv_kill_switch_route_check.setEnvironmentVariable("TERMITE_METAL_DISABLE_Q4_0_SMALL_REDUCE", "0");
    q4_mmv_kill_switch_route_check.addArgs(&.{ "--mode", "linear", "--rows", "1", "--in", "256", "--out", "256", "--warmup", "1", "--iters", "1", "--expect-q4-mmv-variant", "nr4-nsg2", "--expect-output-hash", "72bd43f1b2aa7f8e" });
    q4_mmv_kill_switch_route_check.step.dependOn(&q4_mmv_auto_down_route_check.step);
    const q4_mmv_fallback_route_check = b.addRunArtifact(metal_bench_exe);
    q4_mmv_fallback_route_check.setEnvironmentVariable("TERMITE_METAL_Q4_0_MMV_VARIANT", "nr4-nsg4");
    q4_mmv_fallback_route_check.setEnvironmentVariable("TERMITE_METAL_DISABLE_Q4_0_MMV_PORTFOLIO", "0");
    q4_mmv_fallback_route_check.setEnvironmentVariable("TERMITE_METAL_DISABLE_Q4_0_SMALL_REDUCE", "1");
    q4_mmv_fallback_route_check.addArgs(&.{ "--mode", "linear", "--rows", "1", "--in", "256", "--out", "256", "--warmup", "1", "--iters", "1", "--expect-q4-mmv-variant", "nr8-nsg2", "--expect-q4-mmv-fallbacks", "1", "--expect-output-hash", "72bd43f1b2aa7f8e" });
    q4_mmv_fallback_route_check.step.dependOn(&q4_mmv_kill_switch_route_check.step);
    const q4_mmv_ffn_workload_route_check = b.addRunArtifact(metal_bench_exe);
    q4_mmv_ffn_workload_route_check.setEnvironmentVariable("TERMITE_METAL_Q4_0_MMV_VARIANT", "auto");
    q4_mmv_ffn_workload_route_check.setEnvironmentVariable("TERMITE_METAL_Q4_0_MMV_FFN_GATE_UP_VARIANT", "nr8-nsg4");
    q4_mmv_ffn_workload_route_check.setEnvironmentVariable("TERMITE_METAL_Q4_0_MMV_FFN_DOWN_VARIANT", "nr8-nsg4");
    q4_mmv_ffn_workload_route_check.setEnvironmentVariable("TERMITE_METAL_DISABLE_Q4_0_MMV_PORTFOLIO", "0");
    q4_mmv_ffn_workload_route_check.setEnvironmentVariable("TERMITE_METAL_DISABLE_Q4_0_SMALL_REDUCE", "0");
    q4_mmv_ffn_workload_route_check.setEnvironmentVariable("TERMITE_METAL_DISABLE_Q4_0_PAIR_ACTIVATION_FUSION", "1");
    q4_mmv_ffn_workload_route_check.addArgs(&.{ "--mode", "ffn", "--rows", "1", "--in", "2560", "--out", "10240", "--warmup", "1", "--iters", "1", "--expect-q4-mmv-variant", "nr8-nsg4", "--expect-output-hash", "e48cf991af265cc9" });
    q4_mmv_ffn_workload_route_check.step.dependOn(&q4_mmv_fallback_route_check.step);
    const q4_mmv_route_check_step = b.step(
        "test-metal-q4-0-mmv-routes",
        "Run on-device assertions for the Q4_0 row-one MMV portfolio, workload policy, kill switch, and fallback",
    );
    q4_mmv_route_check_step.dependOn(&q4_mmv_ffn_workload_route_check.step);
    if (target.result.os.tag == .macos and targetRunsOnBuildHost(b, target)) {
        quant_kernel_metal_local_check_step.dependOn(&q4_mmv_ffn_workload_route_check.step);
    }

    const q4_pair_mmv_variants = [_]struct { name: []const u8, hash: []const u8 }{
        .{ .name = "nr4-nsg2", .hash = "1b8993f8509b5da" },
        .{ .name = "nr8-nsg2", .hash = "1b8993f8509b5da" },
        .{ .name = "nr4-nsg4", .hash = "1b8993f8509b5da" },
        .{ .name = "nr8-nsg4", .hash = "1b8993f8509b5da" },
    };
    var q4_pair_route_tail: *std.Build.Step = undefined;
    for (q4_pair_mmv_variants, 0..) |variant, variant_index| {
        const check = b.addRunArtifact(metal_bench_exe);
        check.setEnvironmentVariable("TERMITE_METAL_ENABLE_Q4_0_PAIR_ACTIVATION_FUSION", "1");
        check.setEnvironmentVariable("TERMITE_METAL_DISABLE_Q4_0_PAIR_ACTIVATION_FUSION", "0");
        check.setEnvironmentVariable("TERMITE_METAL_Q4_0_PAIR_ACTIVATION_MMV_VARIANT", variant.name);
        check.addArg("--skip-unless-apple-m4");
        check.addArgs(&.{ "--mode", "ffn", "--rows", "1", "--in", "2560", "--out", "10240", "--warmup", "1", "--iters", "1", "--expect-q4-pair-mmv-variant", variant.name, "--expect-output-hash", variant.hash });
        if (variant_index != 0) check.step.dependOn(q4_pair_route_tail);
        q4_pair_route_tail = &check.step;
    }
    const q4_pair_mmv_auto_route_check = b.addRunArtifact(metal_bench_exe);
    q4_pair_mmv_auto_route_check.setEnvironmentVariable("TERMITE_METAL_ENABLE_Q4_0_PAIR_ACTIVATION_FUSION", "1");
    q4_pair_mmv_auto_route_check.setEnvironmentVariable("TERMITE_METAL_DISABLE_Q4_0_PAIR_ACTIVATION_FUSION", "0");
    q4_pair_mmv_auto_route_check.setEnvironmentVariable("TERMITE_METAL_Q4_0_PAIR_ACTIVATION_MMV_VARIANT", "auto");
    q4_pair_mmv_auto_route_check.addArg("--skip-unless-apple-m4");
    q4_pair_mmv_auto_route_check.addArgs(&.{ "--mode", "ffn", "--rows", "1", "--in", "2560", "--out", "10240", "--warmup", "1", "--iters", "1", "--expect-q4-pair-mmv-variant", "nr4-nsg2", "--expect-output-hash", "1b8993f8509b5da" });
    q4_pair_mmv_auto_route_check.step.dependOn(q4_pair_route_tail);

    const q4_pair_mmv_kill_switch_check = b.addRunArtifact(metal_bench_exe);
    q4_pair_mmv_kill_switch_check.setEnvironmentVariable("TERMITE_METAL_ENABLE_Q4_0_PAIR_ACTIVATION_FUSION", "1");
    q4_pair_mmv_kill_switch_check.setEnvironmentVariable("TERMITE_METAL_DISABLE_Q4_0_PAIR_ACTIVATION_FUSION", "1");
    q4_pair_mmv_kill_switch_check.setEnvironmentVariable("TERMITE_METAL_Q4_0_PAIR_ACTIVATION_MMV_VARIANT", "nr8-nsg2");
    q4_pair_mmv_kill_switch_check.addArg("--skip-unless-apple-m4");
    q4_pair_mmv_kill_switch_check.addArgs(&.{ "--mode", "ffn", "--rows", "1", "--in", "2560", "--out", "10240", "--warmup", "1", "--iters", "1", "--expect-output-hash", "e48cf991af265cc9" });
    q4_pair_mmv_kill_switch_check.step.dependOn(&q4_pair_mmv_auto_route_check.step);

    const q4_pair_mm_cases = [_]struct { variant: []const u8, rows: []const u8, route: []const u8, hash: []const u8 }{
        .{ .variant = "m32-n64", .rows = "32", .route = "m32-n64-aligned", .hash = "e1941e94e89fba88" },
        .{ .variant = "m32-n32", .rows = "32", .route = "m32-n32-aligned", .hash = "e1941e94e89fba88" },
        .{ .variant = "m32-n64", .rows = "31", .route = "m32-n64-tail", .hash = "74bf31c81f60ad14" },
        .{ .variant = "m32-n32", .rows = "31", .route = "m32-n32-tail", .hash = "74bf31c81f60ad14" },
        .{ .variant = "m32-n64", .rows = "2003", .route = "m32-n64-tail", .hash = "34e1a1753b4c4dfe" },
    };
    q4_pair_route_tail = &q4_pair_mmv_kill_switch_check.step;
    for (q4_pair_mm_cases) |case| {
        const check = b.addRunArtifact(metal_bench_exe);
        check.setEnvironmentVariable("TERMITE_METAL_ENABLE_Q4_0_PAIR_ACTIVATION_MM", "1");
        check.setEnvironmentVariable("TERMITE_METAL_DISABLE_Q4_0_PAIR_ACTIVATION_MM", "0");
        check.setEnvironmentVariable("TERMITE_METAL_Q4_0_PAIR_ACTIVATION_MM_VARIANT", case.variant);
        check.addArg("--skip-unless-apple-m4");
        check.addArgs(&.{ "--mode", "ffn", "--rows", case.rows, "--in", "2560", "--out", "10240", "--warmup", "1", "--iters", "1", "--expect-q4-pair-mm-route", case.route, "--expect-output-hash", case.hash });
        check.step.dependOn(q4_pair_route_tail);
        q4_pair_route_tail = &check.step;
    }
    const q4_pair_route_check_step = b.step(
        "test-metal-q4-0-pair-activation-routes",
        "Run exact-output Metal assertions for the M4-qualified Q4_0 pair activation portfolios",
    );
    q4_pair_route_check_step.dependOn(q4_pair_route_tail);
    if (target.result.os.tag == .macos and targetRunsOnBuildHost(b, target)) {
        quant_kernel_metal_local_check_step.dependOn(q4_pair_route_tail);
    }

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
    configureNativeTool(b, training_bench_exe, target, enable_system_blas, blas_root, enable_metal);
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
    gliner2_bench_exe.root_module.addImport("antfly_platform", platform_mod);
    gliner2_bench_exe.root_module.addImport("inference_internal", inference_internal_mod);
    // inference_internal already owns the native backend linkage, including
    // metal_kernels.m. Linking it again at the executable root produces
    // duplicate Metal symbols in these standalone benchmark tools.
    gliner2_bench_exe.root_module.link_libc = true;
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
    gliner2_e2e_bench_exe.root_module.link_libc = true;
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
    // inference_internal already owns the Metal source and frameworks.
    configureNativeTool(b, clipclap_native_bench_exe, target, enable_system_blas, blas_root, false);
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
    // inference_internal already owns the Metal source and frameworks.
    configureNativeTool(b, clipclap_e2e_bench_exe, target, enable_system_blas, blas_root, false);
    configureOnnxRuntime(b, clipclap_e2e_bench_exe.root_module, enable_onnx, effective_onnx_root);
    const run_clipclap_e2e_bench = b.addRunArtifact(clipclap_e2e_bench_exe);
    if (b.args) |args| {
        run_clipclap_e2e_bench.addArgs(args);
    }
    const clipclap_e2e_bench_step = b.step("bench-clipclap-e2e", "Run real-bundle CLIP/CLAP embedding E2E benchmarks");
    clipclap_e2e_bench_step.dependOn(&run_clipclap_e2e_bench.step);

    const bge_m3_e2e_bench_exe = b.addExecutable(.{
        .name = "antfly-inference-bge-m3-e2e-bench",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/bench/bge_m3_e2e.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    bge_m3_e2e_bench_exe.root_module.addImport("build_options", build_options_mod);
    bge_m3_e2e_bench_exe.root_module.addImport("ml", ml_mod);
    bge_m3_e2e_bench_exe.root_module.addImport("pjrt", pjrt_mod);
    bge_m3_e2e_bench_exe.root_module.addImport("inference_linalg", inference_linalg_mod);
    bge_m3_e2e_bench_exe.root_module.addImport("inference_hf_tokenizer", inference_hf_tokenizer_mod);
    bge_m3_e2e_bench_exe.root_module.addImport("antfly_image", antfly_image_mod);
    bge_m3_e2e_bench_exe.root_module.addImport("inference_audio", inference_audio_mod);
    bge_m3_e2e_bench_exe.root_module.addImport("protobuf", protobuf_mod);
    bge_m3_e2e_bench_exe.root_module.addImport("onnx_graph", onnx_graph_mod);
    bge_m3_e2e_bench_exe.root_module.addImport("inference_internal", inference_internal_mod);
    // inference_internal already owns the Metal source and frameworks.
    configureNativeTool(b, bge_m3_e2e_bench_exe, target, enable_system_blas, blas_root, false);
    configureOnnxRuntime(b, bge_m3_e2e_bench_exe.root_module, enable_onnx, effective_onnx_root);
    const run_bge_m3_e2e_bench = b.addRunArtifact(bge_m3_e2e_bench_exe);
    if (b.args) |args| {
        run_bge_m3_e2e_bench.addArgs(args);
    }
    const bge_m3_e2e_bench_step = b.step("bench-bge-m3-e2e", "Run pretokenized BGE-M3 encoder E2E benchmarks");
    bge_m3_e2e_bench_step.dependOn(&run_bge_m3_e2e_bench.step);

    const reranker_e2e_bench_exe = b.addExecutable(.{
        .name = "antfly-inference-reranker-e2e-bench",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/bench/reranker_e2e.zig"),
            .target = target,
            .optimize = .ReleaseFast,
        }),
    });
    reranker_e2e_bench_exe.root_module.addImport("build_options", build_options_mod);
    reranker_e2e_bench_exe.root_module.addImport("ml", ml_mod);
    reranker_e2e_bench_exe.root_module.addImport("pjrt", pjrt_mod);
    reranker_e2e_bench_exe.root_module.addImport("inference_linalg", inference_linalg_mod);
    reranker_e2e_bench_exe.root_module.addImport("inference_hf_tokenizer", inference_hf_tokenizer_mod);
    reranker_e2e_bench_exe.root_module.addImport("antfly_image", antfly_image_mod);
    reranker_e2e_bench_exe.root_module.addImport("inference_audio", inference_audio_mod);
    reranker_e2e_bench_exe.root_module.addImport("protobuf", protobuf_mod);
    reranker_e2e_bench_exe.root_module.addImport("onnx_graph", onnx_graph_mod);
    reranker_e2e_bench_exe.root_module.addImport("inference_internal", inference_internal_mod);
    reranker_e2e_bench_exe.root_module.link_libc = true;
    configureOnnxRuntime(b, reranker_e2e_bench_exe.root_module, enable_onnx, effective_onnx_root);
    const run_reranker_e2e_bench = b.addRunArtifact(reranker_e2e_bench_exe);
    if (b.args) |args| {
        run_reranker_e2e_bench.addArgs(args);
    }
    const reranker_e2e_bench_step = b.step("bench-reranker-e2e", "Run real-bundle text reranker E2E benchmarks");
    reranker_e2e_bench_step.dependOn(&run_reranker_e2e_bench.step);

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
    const selected_test_filters = selectTestFilters(b, &.{});
    const main_test_filters = if (runtime_test_filter) &.{} else selected_test_filters;
    const runtime_filter_test_runner: std.Build.Step.Compile.TestRunner = .{
        .path = b.path("src/test_runner_filter.zig"),
        .mode = .simple,
    };
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
    tests.root_module.addImport("antfly_extraction_openapi", extraction_openapi_mod);
    tests.root_module.addImport("antfly_extracting", extracting_mod);
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
    tests.root_module.addImport("ml_tabular", runtime_graph.ml_tabular_mod);
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
        .enable_metal = enable_metal,
    };
    finetune_tools.register(finetune_ctx);
    finetune_workflows.register(finetune_ctx);
    finetune_tests.register(finetune_ctx);

    const run_tests = b.addRunArtifact(tests);
    for (selected_test_filters) |filter| {
        run_tests.addArgs(&.{ "--test-filter", filter });
    }
    build_test_filters.addRuntimeControls(run_tests, b.args orelse &.{});
    const run_quant_kernel_compiler_tests = b.addRunArtifact(tests);
    run_quant_kernel_compiler_tests.addArg("--test-filter");
    run_quant_kernel_compiler_tests.addArg("quant kernel compiler");
    if (targetRunsOnBuildHost(b, target)) {
        quant_kernel_local_check_step.dependOn(&run_quant_kernel_compiler_tests.step);
        quant_kernel_metal_local_check_step.dependOn(&run_quant_kernel_compiler_tests.step);
    }
    const run_quant_kernel_metal_renderer_tests = b.addRunArtifact(tests);
    run_quant_kernel_metal_renderer_tests.addArg("--test-filter");
    run_quant_kernel_metal_renderer_tests.addArg("metal renderer");
    if (targetRunsOnBuildHost(b, target)) {
        quant_kernel_local_check_step.dependOn(&run_quant_kernel_metal_renderer_tests.step);
        quant_kernel_metal_local_check_step.dependOn(&run_quant_kernel_metal_renderer_tests.step);
    }
    const run_quant_kernel_cuda_microbench_tests = b.addRunArtifact(tests);
    run_quant_kernel_cuda_microbench_tests.addArg("--test-filter");
    run_quant_kernel_cuda_microbench_tests.addArg("cuda microbench");
    if (targetRunsOnBuildHost(b, target)) {
        quant_kernel_local_check_step.dependOn(&run_quant_kernel_cuda_microbench_tests.step);
    }
    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&quant_kernel_codegen_test_check.step);
    test_step.dependOn(&cuda_artifact_source_policy_check.step);
    test_step.dependOn(&run_quant_kernel_metal_runtime_check_tests.step);
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

    const hf_tok_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path(b.fmt("{s}/lib/tokenizer/src/hf_tokenizer.zig", .{shared_lib_root})),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
        }),
    });
    hf_tok_tests.root_module.addImport("sentencepiece_proto", sentencepiece_proto_mod);
    const run_hf_tok_tests = b.addRunArtifact(hf_tok_tests);

    const tok_test_step = b.step("test-tokenizer", "Run tokenizer tests");
    tok_test_step.dependOn(&run_tok_tests.step);
    tok_test_step.dependOn(&run_hf_tok_tests.step);

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
        const wasm_platform_mod = b.dependency("antfly_platform", .{
            .target = wasm_target,
            .optimize = .ReleaseSafe,
            .link_libc = false,
        }).module("antfly_platform");
        wasm_platform_mod.single_threaded = true;
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
        wasm_ml_mod.addImport("antfly_platform", wasm_platform_mod);
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
