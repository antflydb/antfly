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
const antfly_benches_build = @import("pkg/antfly/build/benches.zig");
const antfly_capi_build = @import("pkg/antfly/build/capi.zig");
const antfly_conformance_build = @import("pkg/antfly/build/conformance.zig");
const antfly_embedded_build = @import("pkg/antfly/build/embedded.zig");
const antfly_generated_build = @import("pkg/antfly/build/generated.zig");
const antfly_inference_build = @import("pkg/antfly/build/inference.zig");
const antfly_lite_build = @import("pkg/antfly/build/lite.zig");
const antfly_storage_build = @import("pkg/antfly/build/storage.zig");
const antfly_tests_build = @import("pkg/antfly/build/tests.zig");
const antfly_tools_build = @import("pkg/antfly/build/tools.zig");
const inference_runtime_build = @import("pkg/inference/build/runtime.zig");
const platform_build = @import("lib/platform/build_support.zig");

const LmdbBackend = antfly_storage_build.LmdbBackend;
const addFilteredTestRunArtifact = antfly_tests_build.addFilteredTestRunArtifact;
const addFilteredTestRunArtifactWithRuntimeFilters = antfly_tests_build.addFilteredTestRunArtifactWithRuntimeFilters;
const addRuntimeTestFilters = antfly_tests_build.addRuntimeTestFilters;
const chainLabeledRun = antfly_tests_build.chainLabeledRun;
const chainLabeledRunStep = antfly_tests_build.chainLabeledRunStep;
const configureEmbeddedModule = antfly_embedded_build.configureModule;
const lmdb_c_flags = antfly_storage_build.lmdb_c_flags;
const makeLmdbBuildOptions = antfly_storage_build.makeLmdbBuildOptions;
const makeLmdbEngineModule = antfly_storage_build.makeLmdbEngineModule;
const makeLmdbModule = antfly_storage_build.makeLmdbModule;
const makeRootBuildOptions = antfly_storage_build.makeRootBuildOptions;
const selectTestFilters = antfly_tests_build.selectTestFilters;

const BuildEdition = enum {
    full,
    inference,
};

fn dependOnAll(step: *std.Build.Step, dependencies: []const *std.Build.Step) void {
    for (dependencies) |dependency| {
        step.dependOn(dependency);
    }
}

fn expectQuietSuccess(run: *std.Build.Step.Run) *std.Build.Step {
    run.has_side_effects = true;
    run.expectExitCode(0);
    run.expectStdErrMatch("");
    return &run.step;
}

const FfmpegPaths = struct {
    include_dir: []const u8,
    lib_dir: []const u8,
};

fn pathExists(b: *std.Build, path: []const u8) bool {
    const io = b.graph.io;
    std.Io.Dir.cwd().access(io, path, .{}) catch return false;
    return true;
}

fn addMacosSdkPaths(b: *std.Build, module: *std.Build.Module, target: std.Build.ResolvedTarget) void {
    if (target.result.os.tag != .macos) return;
    const sdk_root = b.sysroot orelse
        std.zig.system.darwin.getSdk(b.allocator, b.graph.io, &target.result) orelse
        b.graph.environ_map.get("SDK_PATH") orelse
        return;
    module.addSystemIncludePath(.{ .cwd_relative = b.fmt("{s}/usr/include", .{sdk_root}) });
    module.addLibraryPath(.{ .cwd_relative = b.fmt("{s}/usr/lib", .{sdk_root}) });
    module.addFrameworkPath(.{ .cwd_relative = b.fmt("{s}/System/Library/Frameworks", .{sdk_root}) });
}

const SpngPaths = struct {
    include_dir: []const u8,
    lib_dir: []const u8,
};

fn defaultInferenceOnnxRoot(b: *std.Build, target: std.Build.ResolvedTarget) []const u8 {
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
    return b.fmt("pkg/inference/onnxruntime/{s}-{s}", .{ platform_str, arch_str });
}

fn detectFfmpegPaths(b: *std.Build, target: std.Build.ResolvedTarget) ?FfmpegPaths {
    const macos_candidates = [_]FfmpegPaths{
        .{ .include_dir = "/opt/homebrew/include", .lib_dir = "/opt/homebrew/lib" },
        .{ .include_dir = "/opt/homebrew/opt/ffmpeg/include", .lib_dir = "/opt/homebrew/opt/ffmpeg/lib" },
        .{ .include_dir = "/usr/local/include", .lib_dir = "/usr/local/lib" },
        .{ .include_dir = "/usr/local/opt/ffmpeg/include", .lib_dir = "/usr/local/opt/ffmpeg/lib" },
    };
    const linux_candidates = [_]FfmpegPaths{
        .{ .include_dir = "/usr/include", .lib_dir = "/usr/lib/x86_64-linux-gnu" },
        .{ .include_dir = "/usr/include", .lib_dir = "/usr/lib/aarch64-linux-gnu" },
        .{ .include_dir = "/usr/include", .lib_dir = "/usr/lib64" },
        .{ .include_dir = "/usr/include", .lib_dir = "/usr/lib" },
        .{ .include_dir = "/usr/local/include", .lib_dir = "/usr/local/lib64" },
        .{ .include_dir = "/usr/local/include", .lib_dir = "/usr/local/lib" },
    };
    const candidates: []const FfmpegPaths = switch (target.result.os.tag) {
        .macos => macos_candidates[0..],
        .linux => linux_candidates[0..],
        else => return null,
    };

    for (candidates) |candidate| {
        const header = b.fmt("{s}/libavformat/avformat.h", .{candidate.include_dir});
        const dylib = b.fmt("{s}/libavformat.dylib", .{candidate.lib_dir});
        const so = b.fmt("{s}/libavformat.so", .{candidate.lib_dir});
        if (pathExists(b, header) and (pathExists(b, dylib) or pathExists(b, so))) return candidate;
    }
    return null;
}

fn detectSpngPaths(b: *std.Build, target: std.Build.ResolvedTarget) ?SpngPaths {
    const macos_candidates = [_]SpngPaths{
        .{ .include_dir = "/opt/homebrew/include", .lib_dir = "/opt/homebrew/lib" },
        .{ .include_dir = "/usr/local/include", .lib_dir = "/usr/local/lib" },
    };
    const linux_candidates = [_]SpngPaths{
        .{ .include_dir = "/usr/include", .lib_dir = "/usr/lib/x86_64-linux-gnu" },
        .{ .include_dir = "/usr/include", .lib_dir = "/usr/lib/aarch64-linux-gnu" },
        .{ .include_dir = "/usr/include", .lib_dir = "/usr/lib64" },
        .{ .include_dir = "/usr/include", .lib_dir = "/usr/lib" },
        .{ .include_dir = "/usr/local/include", .lib_dir = "/usr/local/lib64" },
        .{ .include_dir = "/usr/local/include", .lib_dir = "/usr/local/lib" },
    };
    const candidates: []const SpngPaths = switch (target.result.os.tag) {
        .macos => macos_candidates[0..],
        .linux => linux_candidates[0..],
        else => return null,
    };

    for (candidates) |candidate| {
        const header = b.fmt("{s}/spng.h", .{candidate.include_dir});
        const dylib = b.fmt("{s}/libspng.dylib", .{candidate.lib_dir});
        const so = b.fmt("{s}/libspng.so", .{candidate.lib_dir});
        const static_lib = b.fmt("{s}/libspng.a", .{candidate.lib_dir});
        if (pathExists(b, header) and (pathExists(b, dylib) or pathExists(b, so) or pathExists(b, static_lib))) return candidate;
    }
    return null;
}

fn addLocalHttpxModule(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
) *std.Build.Module {
    return b.createModule(.{
        .root_source_file = b.path("lib/httpx/src/httpx.zig"),
        .target = target,
        .optimize = optimize,
    });
}

fn setStripRecursively(module: *std.Build.Module, visited: *std.AutoHashMap(*std.Build.Module, void)) void {
    const result = visited.getOrPut(module) catch @panic("OOM");
    if (result.found_existing) return;

    module.strip = true;
    for (module.import_table.values()) |imported_module| {
        setStripRecursively(imported_module, visited);
    }
}

const AntflyRootImports = struct {
    build_options: *std.Build.Step.Options,
    lmdb_engine: *std.Build.Module,
    raft_engine: *std.Build.Module,
    public_openapi: *std.Build.Module,
    client_openapi: *std.Build.Module,
    schema_openapi: *std.Build.Module,
    indexes_openapi: *std.Build.Module,
    sort_openapi: *std.Build.Module,
    generating_api_openapi: *std.Build.Module,
    eval_openapi: *std.Build.Module,
    query_openapi: *std.Build.Module,
    admin_openapi: *std.Build.Module,
    internal_openapi: *std.Build.Module,
    metadata_openapi: *std.Build.Module,
    usermgr_openapi: *std.Build.Module,
    logging_openapi: *std.Build.Module,
    audio_openapi: *std.Build.Module,
    middleware_openapi: *std.Build.Module,
    scraping_openapi: *std.Build.Module,
    scraping: *std.Build.Module,
    s3_openapi: *std.Build.Module,
    inference_config_openapi: *std.Build.Module,
    chunking_api_openapi: *std.Build.Module,
    chunking_openapi: *std.Build.Module,
    chunking: *std.Build.Module,
    embeddings_openapi: *std.Build.Module,
    embeddings: *std.Build.Module,
    common_openapi: *std.Build.Module,
    generating_openapi: *std.Build.Module,
    reranking_openapi: *std.Build.Module,
    extraction_openapi: *std.Build.Module,
    transcribing: *std.Build.Module,
    readers: *std.Build.Module,
    extracting: *std.Build.Module,
    synthesizing: *std.Build.Module,
    httpx: *std.Build.Module,
    google: *std.Build.Module,
    objectstore: *std.Build.Module,
    bloom: *std.Build.Module,
    vector: *std.Build.Module,
    vectorindex: *std.Build.Module,
    matcher: *std.Build.Module,
    resolver: *std.Build.Module,
    casbin: *std.Build.Module,
    vellum: *std.Build.Module,
    regex: *std.Build.Module,
    json: *std.Build.Module,
    jsonschema: *std.Build.Module,
    mcp: *std.Build.Module,
    a2a: *std.Build.Module,
    generating: *std.Build.Module,
    reranking: *std.Build.Module,
    inference_api: *std.Build.Module,
    inference_hf_tokenizer: *std.Build.Module,
    inference_fixed_tokenizer_data: *std.Build.Module,
    inference_chunker: *std.Build.Module,
    image: *std.Build.Module,
    font: *std.Build.Module,
    pdf: *std.Build.Module,
    openai_api: *std.Build.Module,
    handlebars: *std.Build.Module,
    inference_server: *std.Build.Module,
    prometheus: *std.Build.Module,
    structlog: *std.Build.Module,
    platform: *std.Build.Module,
    platform_link_libc: bool,
    platform_target: std.Build.ResolvedTarget,
    filesystem_capacity_source_file: std.Build.LazyPath,

    const import_table = [_]struct { name: []const u8, field: []const u8 }{
        .{ .name = "lmdb_engine", .field = "lmdb_engine" },
        .{ .name = "raft_engine", .field = "raft_engine" },
        .{ .name = "antfly_public_openapi", .field = "public_openapi" },
        .{ .name = "antfly_client_openapi", .field = "client_openapi" },
        .{ .name = "antfly_schema_openapi", .field = "schema_openapi" },
        .{ .name = "antfly_indexes_openapi", .field = "indexes_openapi" },
        .{ .name = "antfly_sort_openapi", .field = "sort_openapi" },
        .{ .name = "antfly_generating_api_openapi", .field = "generating_api_openapi" },
        .{ .name = "antfly_eval_openapi", .field = "eval_openapi" },
        .{ .name = "antfly_query_openapi", .field = "query_openapi" },
        .{ .name = "antfly_admin_openapi", .field = "admin_openapi" },
        .{ .name = "antfly_internal_openapi", .field = "internal_openapi" },
        .{ .name = "antfly_metadata_openapi", .field = "metadata_openapi" },
        .{ .name = "antfly_usermgr_openapi", .field = "usermgr_openapi" },
        .{ .name = "antfly_logging_openapi", .field = "logging_openapi" },
        .{ .name = "antfly_audio_openapi", .field = "audio_openapi" },
        .{ .name = "antfly_middleware_openapi", .field = "middleware_openapi" },
        .{ .name = "antfly_scraping_openapi", .field = "scraping_openapi" },
        .{ .name = "antfly_scraping", .field = "scraping" },
        .{ .name = "antfly_s3_openapi", .field = "s3_openapi" },
        .{ .name = "antfly_inference_config_openapi", .field = "inference_config_openapi" },
        .{ .name = "antfly_chunking_api_openapi", .field = "chunking_api_openapi" },
        .{ .name = "antfly_chunking_openapi", .field = "chunking_openapi" },
        .{ .name = "antfly_chunking", .field = "chunking" },
        .{ .name = "antfly_embeddings_openapi", .field = "embeddings_openapi" },
        .{ .name = "antfly_embeddings", .field = "embeddings" },
        .{ .name = "antfly_common_openapi", .field = "common_openapi" },
        .{ .name = "antfly_generating_openapi", .field = "generating_openapi" },
        .{ .name = "antfly_reranking_openapi", .field = "reranking_openapi" },
        .{ .name = "antfly_extraction_openapi", .field = "extraction_openapi" },
        .{ .name = "antfly_transcribing", .field = "transcribing" },
        .{ .name = "antfly_readers", .field = "readers" },
        .{ .name = "antfly_extracting", .field = "extracting" },
        .{ .name = "antfly_synthesizing", .field = "synthesizing" },
        .{ .name = "httpx", .field = "httpx" },
        .{ .name = "antfly_google", .field = "google" },
        .{ .name = "objectstore", .field = "objectstore" },
        .{ .name = "bloom", .field = "bloom" },
        .{ .name = "antfly_vector", .field = "vector" },
        .{ .name = "antfly_vectorindex", .field = "vectorindex" },
        .{ .name = "antfly_matcher", .field = "matcher" },
        .{ .name = "antfly_resolver", .field = "resolver" },
        .{ .name = "antfly_casbin", .field = "casbin" },
        .{ .name = "antfly_vellum", .field = "vellum" },
        .{ .name = "antfly_regex", .field = "regex" },
        .{ .name = "antfly-json", .field = "json" },
        .{ .name = "antfly_jsonschema", .field = "jsonschema" },
        .{ .name = "antfly_mcp", .field = "mcp" },
        .{ .name = "antfly_a2a", .field = "a2a" },
        .{ .name = "antfly_generating", .field = "generating" },
        .{ .name = "antfly_reranking", .field = "reranking" },
        .{ .name = "inference_api", .field = "inference_api" },
        .{ .name = "inference_hf_tokenizer", .field = "inference_hf_tokenizer" },
        .{ .name = "inference_fixed_tokenizer_data", .field = "inference_fixed_tokenizer_data" },
        .{ .name = "inference_chunker", .field = "inference_chunker" },
        .{ .name = "antfly_image", .field = "image" },
        .{ .name = "antfly_font", .field = "font" },
        .{ .name = "antfly_pdf", .field = "pdf" },
        .{ .name = "openai_api", .field = "openai_api" },
        .{ .name = "handlebars", .field = "handlebars" },
        .{ .name = "inference_server", .field = "inference_server" },
        .{ .name = "prometheus", .field = "prometheus" },
        .{ .name = "structlog", .field = "structlog" },
    };

    pub fn configure(self: @This(), b: *std.Build, mod: *std.Build.Module, include_lmdb_c: bool, link_libc: bool) void {
        mod.addOptions("build_options", self.build_options);
        inline for (import_table) |entry| {
            mod.addImport(entry.name, @field(self, entry.field));
        }
        mod.addImport("antfly_platform", self.platform);
        if (link_libc and !self.platform_link_libc) {
            platform_build.addFilesystemCapacitySource(
                mod,
                self.filesystem_capacity_source_file,
                self.platform_target,
            );
        }
        mod.addIncludePath(b.path("lib/lmdb"));
        if (include_lmdb_c) {
            mod.addCSourceFiles(.{
                .files = &.{ "lib/lmdb/mdb.c", "lib/lmdb/midl.c" },
                .flags = &lmdb_c_flags,
            });
        }
        mod.link_libc = link_libc;
        addSnowballModule(b, mod);
    }
};

fn addSnowballModule(b: *std.Build, lib_mod: *std.Build.Module) void {
    const snowball_mod = b.addModule("snowball", .{
        .root_source_file = b.path(antfly_generated_build.snowball_generated_root ++ "/root.zig"),
    });

    lib_mod.addImport("snowball", snowball_mod);
}

pub fn build(b: *std.Build) void {
    // Keep focused test inventories in pkg/antfly/build/tests.zig. build.zig
    // wires durable suites and modules; it should not grow exact test-title
    // lists as API/storage refactors move coverage closer to implementation.
    antfly_tests_build.assertBuildZigDoesNotOwnTestInventory(b);
    antfly_tests_build.assertDBRefactorBoundary(b);

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
    const strip = b.option(bool, "strip", "Omit debug information from release artifacts") orelse false;
    const wasm_target = b.resolveTargetQuery(.{
        .cpu_arch = .wasm32,
        .os_tag = .freestanding,
        .cpu_features_add = std.Target.wasm.featureSet(&.{ .atomics, .bulk_memory, .simd128 }),
    });
    const lmdb_backend = b.option(LmdbBackend, "lmdb_backend", "Select the LMDB backend scaffold (c or zig)") orelse .zig;
    const lmdb_evented_async_io = b.option(bool, "lmdb_evented_async_io", "Use std.Io.Evented for the Zig LMDB async_io backend") orelse false;
    const with_tla = b.option(bool, "with_tla", "Enable TLA+ trace instrumentation (ndjson event logging)") orelse false;
    const link_libc = b.option(bool, "link-libc", "Link Antfly runtime modules against libc") orelse true;
    const sanitize_thread = b.option(bool, "sanitize-thread", "Enable ThreadSanitizer for the Antfly runtime") orelse false;
    const include_ha_tests_in_aggregates = b.option(bool, "ha-tests", "Include hot-standby HA suites in aggregate test steps") orelse true;
    const edition = b.option(BuildEdition, "edition", "Build edition: full or inference") orelse .full;
    const antfly_bin_name = b.option([]const u8, "antfly-bin-name", "Installed filename for the top-level Antfly CLI") orelse "antfly";
    antfly_tests_build.selected_test_filter = b.option([]const u8, "test-filter", "Run selectable Zig test steps with a single test-name filter");
    if (antfly_bin_name.len == 0 or std.mem.indexOfAny(u8, antfly_bin_name, "/\\") != null) {
        @panic("-Dantfly-bin-name must be a non-empty filename, not a path");
    }
    if (!link_libc and lmdb_backend == .c) {
        @panic("-Dlink-libc=false requires -Dlmdb_backend=zig");
    }
    const termite_onnx_option = b.option(bool, "onnx", "Enable ONNX Runtime support for embedded inference");
    const termite_enable_onnx = if (link_libc)
        termite_onnx_option orelse false
    else
        false;
    const termite_onnx_root_opt = b.option([]const u8, "onnx-root", "Path to ONNX Runtime root for embedded inference");
    const termite_onnx_root = termite_onnx_root_opt orelse defaultInferenceOnnxRoot(b, target);
    const termite_enable_metal = if (link_libc)
        b.option(bool, "metal", "Enable Apple Metal kernels for embedded inference") orelse (target.result.os.tag == .macos)
    else
        false;
    const termite_enable_cuda = b.option(bool, "cuda", "Enable CUDA inference support through the NVIDIA Driver API") orelse false;
    const termite_cuda_artifacts = b.option([]const u8, "cuda-artifacts", "CUDA artifact bundle: portable PTX; fatbin is not implemented yet") orelse "portable";
    if (!std.mem.eql(u8, termite_cuda_artifacts, "portable")) {
        @panic("invalid -Dcuda-artifacts (expected portable; fatbin is not implemented yet)");
    }
    const termite_blas_root_opt = b.option([]const u8, "blas-root", "Path to system BLAS root with include/ and lib/ for non-macOS native acceleration");
    const termite_system_blas_available = link_libc and (target.result.os.tag == .macos or termite_blas_root_opt != null);
    const termite_enable_system_blas = if (link_libc)
        b.option(bool, "system-blas", "Enable system BLAS acceleration for native CPU math") orelse termite_system_blas_available
    else
        false;
    const termite_blas_root = if (termite_enable_system_blas and target.result.os.tag != .macos)
        termite_blas_root_opt
    else
        null;
    const antfly_version = b.option([]const u8, "antfly-version", "Antfly version string") orelse "dev";
    const lite_local_inference_runtime = b.option(bool, "lite-local-inference-runtime", "Advertise an embedded local inference runtime in Antfly Lite status") orelse false;
    if (termite_enable_onnx) {
        const termite_onnx_available = pathExists(b, b.fmt("{s}/include/onnxruntime_c_api.h", .{termite_onnx_root})) and
            pathExists(b, b.fmt("{s}/lib", .{termite_onnx_root}));
        if (!termite_onnx_available) {
            @panic("-Donnx=true requires an ONNX Runtime install; pass -Donnx-root=<path>");
        }
    }
    const delegated_inference_steps = antfly_inference_build.addDelegatedBuildSteps(.{
        .b = b,
        .enable_metal = termite_enable_metal,
        .enable_onnx = termite_enable_onnx,
        .onnx_root = termite_onnx_root,
        .enable_cuda = termite_enable_cuda,
        .cuda_artifacts = termite_cuda_artifacts,
        .enable_system_blas = termite_enable_system_blas,
        .blas_root = termite_blas_root,
    });

    const lmdb_build_options = makeLmdbBuildOptions(b, lmdb_backend, lmdb_evented_async_io, false);
    const build_options = makeRootBuildOptions(b, lmdb_backend, lmdb_evented_async_io, false, with_tla, link_libc, false, lite_local_inference_runtime, antfly_version);
    const standalone_runtime_build_options = makeRootBuildOptions(b, lmdb_backend, lmdb_evented_async_io, false, with_tla, link_libc, true, lite_local_inference_runtime, antfly_version);
    const lmdb_engine_mod = makeLmdbEngineModule(b, target, optimize, link_libc, lmdb_build_options);
    const lmdb_engine_wasm_mod = makeLmdbEngineModule(b, wasm_target, optimize, false, lmdb_build_options);
    const raft_engine_mod = b.createModule(.{
        .root_source_file = b.path("lib/raft/src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    const httpx_mod = addLocalHttpxModule(b, target, optimize);
    const prometheus_mod = b.createModule(.{
        .root_source_file = b.path("lib/prometheus/src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    const structlog_mod = b.createModule(.{
        .root_source_file = b.path("lib/structlog/src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    // Protobuf wire format
    const protobuf_dep = b.dependency("protobuf", .{});
    const protobuf_mod = protobuf_dep.module("protobuf");
    const generated_steps = antfly_generated_build.addGeneratedArtifactSteps(.{
        .b = b,
        .target = target,
        .optimize = optimize,
        .httpx_mod = httpx_mod,
        .protobuf_dep = protobuf_dep,
    });
    const openapi_modules = antfly_generated_build.addCommittedOpenApiModules(.{
        .b = b,
        .target = target,
        .optimize = optimize,
        .httpx_mod = httpx_mod,
    });
    const public_openapi_mod = openapi_modules.public;
    const client_openapi_mod = openapi_modules.client;
    const schema_openapi_mod = openapi_modules.schema;
    const indexes_openapi_mod = openapi_modules.indexes;
    const sort_openapi_mod = openapi_modules.sort;
    const eval_openapi_mod = openapi_modules.eval;
    const query_openapi_mod = openapi_modules.query;
    const admin_openapi_mod = openapi_modules.admin;
    const internal_openapi_mod = openapi_modules.internal;
    const usermgr_openapi_mod = openapi_modules.usermgr;
    const metadata_openapi_mod = openapi_modules.metadata;
    const logging_openapi_mod = openapi_modules.logging;
    const audio_openapi_mod = openapi_modules.audio;
    const middleware_openapi_mod = openapi_modules.middleware;
    const scraping_openapi_mod = openapi_modules.scraping;
    const s3_openapi_mod = openapi_modules.s3;
    const inference_config_openapi_mod = openapi_modules.inference_config;
    const chunking_api_openapi_mod = openapi_modules.chunking_api;
    const chunking_openapi_mod = openapi_modules.chunking;
    const embeddings_openapi_mod = openapi_modules.embeddings;
    const common_openapi_mod = openapi_modules.common;
    const generating_openapi_mod = openapi_modules.generating;
    const reranking_openapi_mod = openapi_modules.reranking;
    const generating_api_openapi_mod = openapi_modules.generating_api;
    const extraction_openapi_mod = openapi_modules.extraction;
    const openai_api_mod = openapi_modules.openai;

    // Handlebars template engine
    const handlebars_dep = b.dependency("handlebars", .{});
    const handlebars_mod = handlebars_dep.module("handlebars");

    const platform_mod = platform_build.createModule(b, .{
        .root_source_file = b.path("lib/platform/src/root.zig"),
        .filesystem_capacity_source_file = b.path("lib/platform/src/filesystem_capacity.c"),
        .target = target,
        .optimize = optimize,
        .link_libc = link_libc,
    });
    const wasm_platform_mod = platform_build.createModule(b, .{
        .root_source_file = b.path("lib/platform/src/root.zig"),
        .filesystem_capacity_source_file = b.path("lib/platform/src/filesystem_capacity.c"),
        .target = wasm_target,
        .optimize = optimize,
        .link_libc = false,
    });
    const objectstore_mod = b.createModule(.{
        .root_source_file = b.path("lib/objectstore/src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    const google_mod = b.createModule(.{
        .root_source_file = b.path("lib/google/src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    google_mod.addImport("httpx", httpx_mod);
    google_mod.addImport("antfly_platform", platform_mod);
    objectstore_mod.addImport("httpx", httpx_mod);
    objectstore_mod.addImport("antfly_platform", platform_mod);
    objectstore_mod.addImport("antfly_google", google_mod);
    const wasm_objectstore_mod = b.createModule(.{
        .root_source_file = b.path("lib/objectstore/src/root.zig"),
        .target = wasm_target,
        .optimize = optimize,
    });
    const wasm_google_mod = b.createModule(.{
        .root_source_file = b.path("lib/google/src/root.zig"),
        .target = wasm_target,
        .optimize = optimize,
    });
    wasm_google_mod.addImport("httpx", httpx_mod);
    wasm_google_mod.addImport("antfly_platform", wasm_platform_mod);
    wasm_objectstore_mod.addImport("httpx", httpx_mod);
    wasm_objectstore_mod.addImport("antfly_platform", wasm_platform_mod);
    wasm_objectstore_mod.addImport("antfly_google", wasm_google_mod);
    const bloom_mod = b.createModule(.{
        .root_source_file = b.path("lib/bloom/src/mod.zig"),
        .target = target,
        .optimize = optimize,
    });
    const vector_mod = b.createModule(.{
        .root_source_file = b.path("lib/vector/src/mod.zig"),
        .target = target,
        .optimize = optimize,
    });
    vector_mod.addImport("protobuf", protobuf_mod);
    const wasm_vector_mod = b.createModule(.{
        .root_source_file = b.path("lib/vector/src/mod.zig"),
        .target = wasm_target,
        .optimize = optimize,
    });
    wasm_vector_mod.addImport("protobuf", protobuf_mod);
    const vectorindex_mod = b.createModule(.{
        .root_source_file = b.path("lib/vectorindex/src/mod.zig"),
        .target = target,
        .optimize = optimize,
    });
    vectorindex_mod.addImport("antfly_vector", vector_mod);
    vectorindex_mod.addImport("antfly_platform", platform_mod);
    if (target.result.os.tag == .macos) {
        addMacosSdkPaths(b, vectorindex_mod, target);
        vectorindex_mod.linkFramework("Foundation", .{});
        vectorindex_mod.linkFramework("Metal", .{});
        vectorindex_mod.addCSourceFile(.{ .file = b.path("lib/vectorindex/src/kmeans_metal.m"), .flags = &.{"-fobjc-arc"} });
    }
    const wasm_vectorindex_mod = b.createModule(.{
        .root_source_file = b.path("lib/vectorindex/src/mod.zig"),
        .target = wasm_target,
        .optimize = optimize,
    });
    wasm_vectorindex_mod.addImport("antfly_vector", wasm_vector_mod);
    wasm_vectorindex_mod.addImport("antfly_platform", wasm_platform_mod);
    const casbin_mod = b.createModule(.{
        .root_source_file = b.path("lib/casbin/src/mod.zig"),
        .target = target,
        .optimize = optimize,
    });
    const usermgr_build_options = b.addOptions();
    usermgr_build_options.addOption(bool, "usermgr_storage_adapter", false);
    const usermgr_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/usermgr/mod.zig"),
        .target = target,
        .optimize = optimize,
    });
    usermgr_mod.addOptions("build_options", usermgr_build_options);
    usermgr_mod.link_libc = link_libc;
    usermgr_mod.addImport("antfly_casbin", casbin_mod);
    const wasm_bloom_mod = b.createModule(.{
        .root_source_file = b.path("lib/bloom/src/mod.zig"),
        .target = wasm_target,
        .optimize = optimize,
    });
    const vellum_mod = b.createModule(.{
        .root_source_file = b.path("lib/vellum/src/mod.zig"),
        .target = target,
        .optimize = optimize,
    });
    const regex_mod = b.createModule(.{
        .root_source_file = b.path("lib/regex/src/mod.zig"),
        .target = target,
        .optimize = optimize,
    });
    regex_mod.addImport("antfly_vellum", vellum_mod);
    const jsonschema_mod = b.createModule(.{
        .root_source_file = b.path("lib/jsonschema/src/mod.zig"),
        .target = target,
        .optimize = optimize,
    });
    const json_mod = b.addModule("antfly-json", .{
        .root_source_file = b.path("lib/json/src/mod.zig"),
        .target = target,
        .optimize = optimize,
    });
    const toon_mod = b.addModule("antfly_toon", .{
        .root_source_file = b.path("lib/toon/src/mod.zig"),
        .target = target,
        .optimize = optimize,
    });
    const mcp_mod = b.addModule("antfly_mcp", .{
        .root_source_file = b.path("lib/mcp/src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    const a2a_mod = b.addModule("antfly_a2a", .{
        .root_source_file = b.path("lib/a2a/src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    const matcher_mod = b.addModule("antfly_matcher", .{
        .root_source_file = b.path("lib/matcher/src/mod.zig"),
        .target = target,
        .optimize = optimize,
    });
    const resolver_mod = b.addModule("antfly_resolver", .{
        .root_source_file = b.path("lib/resolver/src/mod.zig"),
        .target = target,
        .optimize = optimize,
    });
    resolver_mod.addImport("antfly_matcher", matcher_mod);
    httpx_mod.addImport("antfly-json", json_mod);
    jsonschema_mod.addImport("antfly_regex", regex_mod);
    jsonschema_mod.addImport("antfly-json", json_mod);
    const generating_mod = b.createModule(.{
        .root_source_file = b.path("lib/generating/src/mod.zig"),
        .target = target,
        .optimize = optimize,
    });
    generating_mod.addImport("antfly-json", json_mod);
    generating_mod.addImport("antfly_generating_openapi", generating_openapi_mod);
    const chunking_mod = b.createModule(.{
        .root_source_file = b.path("lib/chunking/src/mod.zig"),
        .target = target,
        .optimize = optimize,
    });
    chunking_mod.addImport("antfly-json", json_mod);
    chunking_mod.addImport("antfly_chunking_api_openapi", chunking_api_openapi_mod);
    chunking_mod.addImport("antfly_chunking_openapi", chunking_openapi_mod);
    const embeddings_mod = b.createModule(.{
        .root_source_file = b.path("lib/embeddings/src/mod.zig"),
        .target = target,
        .optimize = optimize,
    });
    embeddings_mod.addImport("antfly-json", json_mod);
    embeddings_mod.addImport("antfly_embeddings_openapi", embeddings_openapi_mod);
    const scraping_mod = b.createModule(.{
        .root_source_file = b.path("lib/scraping/src/mod.zig"),
        .target = target,
        .optimize = optimize,
    });
    scraping_mod.addImport("objectstore", objectstore_mod);
    const reranking_mod = b.createModule(.{
        .root_source_file = b.path("lib/reranking/src/mod.zig"),
        .target = target,
        .optimize = optimize,
    });
    reranking_mod.addImport("antfly-json", json_mod);
    reranking_mod.addImport("antfly_reranking_openapi", reranking_openapi_mod);
    const extracting_mod = b.createModule(.{
        .root_source_file = b.path("lib/extracting/src/mod.zig"),
        .target = target,
        .optimize = optimize,
    });
    extracting_mod.addImport("httpx", httpx_mod);
    extracting_mod.addImport("antfly_extraction_openapi", extraction_openapi_mod);

    // --- Inference backend detection (must precede module creation) ---
    const termite_ffmpeg_paths = if (link_libc) detectFfmpegPaths(b, target) else null;
    const image_mod = b.createModule(.{
        .root_source_file = b.path("lib/image/src/mod.zig"),
        .target = target,
        .optimize = optimize,
    });
    const pdf_mod = b.createModule(.{
        .root_source_file = b.path("lib/pdf/src/mod.zig"),
        .target = target,
        .optimize = optimize,
    });
    const font_mod = b.createModule(.{
        .root_source_file = b.path("lib/font/src/mod.zig"),
        .target = target,
        .optimize = optimize,
    });
    pdf_mod.addImport("antfly_image", image_mod);
    pdf_mod.addImport("antfly_font", font_mod);
    const wasm_image_mod = b.createModule(.{
        .root_source_file = b.path("lib/image/src/mod.zig"),
        .target = wasm_target,
        .optimize = optimize,
    });
    const wasm_pdf_mod = b.createModule(.{
        .root_source_file = b.path("lib/pdf/src/mod.zig"),
        .target = wasm_target,
        .optimize = optimize,
    });
    const wasm_font_mod = b.createModule(.{
        .root_source_file = b.path("lib/font/src/mod.zig"),
        .target = wasm_target,
        .optimize = optimize,
    });
    wasm_pdf_mod.addImport("antfly_image", wasm_image_mod);
    wasm_pdf_mod.addImport("antfly_font", wasm_font_mod);

    const sentencepiece_proto_mod = antfly_generated_build.addSentencePieceProtoModule(b, protobuf_dep);
    const termite_jinja_mod = b.createModule(.{
        .root_source_file = b.path("lib/jinja/src/jinja.zig"),
        .target = target,
        .optimize = optimize,
    });
    const termite_ml_mod = b.createModule(.{
        .root_source_file = b.path("lib/ml/src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    termite_ml_mod.addImport("antfly_platform", platform_mod);
    const ml_tabular_mod = b.addModule("ml_tabular", .{
        .root_source_file = b.path("lib/ml/tabular/src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    const termite_onnx_graph_mod = b.addModule("termite_onnx_graph", .{
        .root_source_file = b.path("lib/onnx/src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    termite_onnx_graph_mod.addImport("protobuf", protobuf_mod);
    termite_onnx_graph_mod.addImport("ml", termite_ml_mod);
    termite_onnx_graph_mod.addImport("structlog", structlog_mod);
    const termite_pjrt_xla_proto_mod = antfly_generated_build.addXlaProtoModule(b, protobuf_dep);
    const termite_pjrt_mod = b.createModule(.{
        .root_source_file = b.path("lib/pjrt/src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    termite_pjrt_mod.addImport("protobuf", protobuf_mod);
    termite_pjrt_mod.addImport("xla_proto", termite_pjrt_xla_proto_mod);

    const inference_graph = inference_runtime_build.create(.{
        .b = b,
        .target = target,
        .optimize = optimize,
        .paths = .{
            .inference_root = "pkg/inference",
            .shared_lib_root = "",
        },
        .backend = .{
            .enable_onnx = termite_enable_onnx,
            .onnx_root = termite_onnx_root,
            .enable_metal = termite_enable_metal,
            .enable_cuda = termite_enable_cuda,
            .cuda_artifacts = termite_cuda_artifacts,
            .enable_pjrt = false,
            .enable_native = true,
            .enable_system_blas = termite_enable_system_blas,
            .blas_root = termite_blas_root,
            .enable_ffmpeg_audio = termite_ffmpeg_paths != null,
            .ffmpeg_paths = if (termite_ffmpeg_paths) |paths| .{
                .include_dir = paths.include_dir,
                .lib_dir = paths.lib_dir,
            } else null,
            .link_libc = link_libc,
            .skip_openapi = false,
            .inference_version = antfly_version,
        },
        .shared = .{
            .json = json_mod,
            .httpx = httpx_mod,
            .platform = platform_mod,
            .vellum = vellum_mod,
            .scraping = scraping_mod,
            .google = google_mod,
            .objectstore = objectstore_mod,
            .regex = regex_mod,
            .jsonschema = jsonschema_mod,
            .image = image_mod,
            .prometheus = prometheus_mod,
            .structlog = structlog_mod,
            .jinja = termite_jinja_mod,
            .protobuf = protobuf_mod,
            .sentencepiece_proto = sentencepiece_proto_mod,
            .ml = termite_ml_mod,
            .ml_tabular = ml_tabular_mod,
            .onnx_graph = termite_onnx_graph_mod,
            .pjrt = termite_pjrt_mod,
            .generating_openapi = generating_openapi_mod,
            .extraction_openapi = extraction_openapi_mod,
            .extracting = extracting_mod,
        },
    });
    const inference_build_options_mod = inference_graph.build_options_mod;
    const inference_api_mod = inference_graph.inference_api_mod;
    inference_api_mod.addImport("antfly_generating_openapi", generating_openapi_mod);
    inference_api_mod.addImport("antfly_extraction_openapi", extraction_openapi_mod);
    inference_api_mod.addImport("antfly_chunking_api_openapi", chunking_api_openapi_mod);
    const inference_hf_tokenizer_mod = inference_graph.inference_hf_tokenizer_mod;
    const inference_fixed_tokenizer_data_mod = inference_graph.inference_fixed_tokenizer_data_mod;
    const inference_chunker_mod = inference_graph.inference_chunker_mod;
    const inference_server_mod = inference_graph.inference_mod;

    const transcribing_mod = b.createModule(.{
        .root_source_file = b.path("lib/transcribing/src/mod.zig"),
        .target = target,
        .optimize = optimize,
    });
    transcribing_mod.addImport("antfly_audio_openapi", audio_openapi_mod);
    transcribing_mod.addImport("httpx", httpx_mod);
    transcribing_mod.addImport("inference_api", inference_api_mod);
    transcribing_mod.addImport("antfly_scraping", scraping_mod);
    transcribing_mod.addImport("antfly_google", google_mod);
    const readers_mod = b.createModule(.{
        .root_source_file = b.path("lib/readers/src/mod.zig"),
        .target = target,
        .optimize = optimize,
    });
    readers_mod.addImport("httpx", httpx_mod);
    readers_mod.addImport("inference_api", inference_api_mod);
    readers_mod.addImport("antfly_google", google_mod);
    inference_server_mod.addImport("antfly_readers", readers_mod);
    inference_server_mod.addImport("antfly_transcribing", transcribing_mod);
    inference_server_mod.addImport("antfly_extracting", extracting_mod);
    const synthesizing_mod = b.createModule(.{
        .root_source_file = b.path("lib/synthesizing/src/mod.zig"),
        .target = target,
        .optimize = optimize,
    });
    synthesizing_mod.addImport("antfly_audio_openapi", audio_openapi_mod);
    synthesizing_mod.addImport("httpx", httpx_mod);

    const antfly_imports = AntflyRootImports{
        .build_options = build_options,
        .lmdb_engine = lmdb_engine_mod,
        .raft_engine = raft_engine_mod,
        .public_openapi = public_openapi_mod,
        .client_openapi = client_openapi_mod,
        .schema_openapi = schema_openapi_mod,
        .indexes_openapi = indexes_openapi_mod,
        .sort_openapi = sort_openapi_mod,
        .generating_api_openapi = generating_api_openapi_mod,
        .eval_openapi = eval_openapi_mod,
        .query_openapi = query_openapi_mod,
        .admin_openapi = admin_openapi_mod,
        .internal_openapi = internal_openapi_mod,
        .metadata_openapi = metadata_openapi_mod,
        .usermgr_openapi = usermgr_openapi_mod,
        .logging_openapi = logging_openapi_mod,
        .audio_openapi = audio_openapi_mod,
        .middleware_openapi = middleware_openapi_mod,
        .scraping_openapi = scraping_openapi_mod,
        .scraping = scraping_mod,
        .s3_openapi = s3_openapi_mod,
        .inference_config_openapi = inference_config_openapi_mod,
        .chunking_api_openapi = chunking_api_openapi_mod,
        .chunking_openapi = chunking_openapi_mod,
        .chunking = chunking_mod,
        .embeddings_openapi = embeddings_openapi_mod,
        .embeddings = embeddings_mod,
        .common_openapi = common_openapi_mod,
        .generating_openapi = generating_openapi_mod,
        .reranking_openapi = reranking_openapi_mod,
        .extraction_openapi = extraction_openapi_mod,
        .transcribing = transcribing_mod,
        .readers = readers_mod,
        .extracting = extracting_mod,
        .synthesizing = synthesizing_mod,
        .httpx = httpx_mod,
        .google = google_mod,
        .objectstore = objectstore_mod,
        .bloom = bloom_mod,
        .vector = vector_mod,
        .vectorindex = vectorindex_mod,
        .matcher = matcher_mod,
        .resolver = resolver_mod,
        .casbin = casbin_mod,
        .vellum = vellum_mod,
        .regex = regex_mod,
        .json = json_mod,
        .jsonschema = jsonschema_mod,
        .mcp = mcp_mod,
        .a2a = a2a_mod,
        .generating = generating_mod,
        .reranking = reranking_mod,
        .inference_api = inference_api_mod,
        .inference_hf_tokenizer = inference_hf_tokenizer_mod,
        .inference_fixed_tokenizer_data = inference_fixed_tokenizer_data_mod,
        .inference_chunker = inference_chunker_mod,
        .image = image_mod,
        .font = font_mod,
        .pdf = pdf_mod,
        .openai_api = openai_api_mod,
        .handlebars = handlebars_mod,
        .inference_server = inference_server_mod,
        .prometheus = prometheus_mod,
        .structlog = structlog_mod,
        .platform = platform_mod,
        .platform_link_libc = link_libc,
        .platform_target = target,
        .filesystem_capacity_source_file = b.path("lib/platform/src/filesystem_capacity.c"),
    };

    // Library module
    const lib_mod = b.addModule("antfly-zig", .{
        .root_source_file = b.path("pkg/antfly/src/root.zig"),
        .target = target,
        .optimize = optimize,
        .sanitize_thread = sanitize_thread,
    });
    antfly_imports.configure(b, lib_mod, false, link_libc);

    const lib_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    antfly_imports.configure(b, lib_test_mod, true, true);

    const raft_sim_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/raft_sim_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    antfly_imports.configure(b, raft_sim_test_mod, true, true);
    const raft_runtime_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/raft_runtime_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    antfly_imports.configure(b, raft_runtime_test_mod, true, true);
    const raft_restore_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/raft_restore_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    antfly_imports.configure(b, raft_restore_test_mod, true, true);
    const filesystem_capacity_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/filesystem_capacity_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    antfly_imports.configure(b, filesystem_capacity_test_mod, true, true);

    const api_query_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/api_query_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    antfly_imports.configure(b, api_query_test_mod, true, true);

    const data_runtime_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/data_runtime_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    antfly_imports.configure(b, data_runtime_test_mod, true, true);

    const data_storage_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/data_storage_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    antfly_imports.configure(b, data_storage_test_mod, true, true);

    const usermgr_storage_lib_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/usermgr/storage_imports.zig"),
        .target = target,
        .optimize = optimize,
    });
    usermgr_storage_lib_mod.addImport("antfly_root", lib_mod);
    usermgr_storage_lib_mod.addImport("antfly_platform", platform_mod);
    lib_mod.addImport("usermgr_storage", usermgr_storage_lib_mod);

    const usermgr_storage_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/usermgr/storage_imports.zig"),
        .target = target,
        .optimize = optimize,
    });
    usermgr_storage_test_mod.addImport("antfly_root", lib_test_mod);
    usermgr_storage_test_mod.addImport("antfly_platform", platform_mod);
    lib_test_mod.addImport("usermgr_storage", usermgr_storage_test_mod);

    const embedded_deps = .{
        build_options,
        lmdb_engine_mod,
        json_mod,
        public_openapi_mod,
        query_openapi_mod,
        indexes_openapi_mod,
        sort_openapi_mod,
        metadata_openapi_mod,
        reranking_mod,
        objectstore_mod,
        platform_mod,
        chunking_mod,
        bloom_mod,
        vector_mod,
        vectorindex_mod,
        vellum_mod,
        regex_mod,
        image_mod,
        font_mod,
        pdf_mod,
        handlebars_mod,
    };

    const embedded_support_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/embedded_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    @call(.auto, configureEmbeddedModule, .{ b, embedded_support_mod } ++ embedded_deps ++ .{addSnowballModule});
    embedded_support_mod.addImport("antfly_scraping", scraping_mod);

    const embedded_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/embedded/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    embedded_mod.addImport("embedded_support", embedded_support_mod);

    const embedded_db_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/embedded/db.zig"),
        .target = target,
        .optimize = optimize,
    });
    embedded_db_mod.addImport("embedded_support", embedded_support_mod);

    const embedded_api_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/embedded/api.zig"),
        .target = target,
        .optimize = optimize,
    });
    embedded_api_mod.addImport("embedded_support", embedded_support_mod);
    embedded_api_mod.addImport("embedded_db_surface", embedded_db_mod);
    embedded_mod.addImport("embedded_db_surface", embedded_db_mod);
    embedded_mod.addImport("embedded_api_surface", embedded_api_mod);

    const antfly_embedded_pkg_mod = b.addModule("antfly-embedded", .{
        .root_source_file = b.path("pkg/antfly-embedded/src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    antfly_embedded_pkg_mod.addImport("embedded_surface", embedded_mod);

    const antfly_embedded_db_pkg_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/db.zig"),
        .target = target,
        .optimize = optimize,
    });
    antfly_embedded_db_pkg_mod.addImport("embedded_db_surface", embedded_db_mod);

    const antfly_embedded_api_pkg_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/api.zig"),
        .target = target,
        .optimize = optimize,
    });
    antfly_embedded_api_pkg_mod.addImport("embedded_api_surface", embedded_api_mod);

    const antfly_client_pkg_mod = b.addModule("antfly-client", .{
        .root_source_file = b.path("pkg/antfly-client/src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    antfly_client_pkg_mod.addImport("antfly_client_openapi", client_openapi_mod);
    antfly_client_pkg_mod.addImport("httpx", httpx_mod);

    const embedded_wasm_deps = .{
        build_options,
        lmdb_engine_wasm_mod,
        json_mod,
        public_openapi_mod,
        query_openapi_mod,
        indexes_openapi_mod,
        sort_openapi_mod,
        metadata_openapi_mod,
        reranking_mod,
        wasm_objectstore_mod,
        wasm_platform_mod,
        chunking_mod,
        wasm_bloom_mod,
        wasm_vector_mod,
        wasm_vectorindex_mod,
        vellum_mod,
        regex_mod,
        wasm_image_mod,
        wasm_font_mod,
        wasm_pdf_mod,
        handlebars_mod,
    };

    const embedded_support_wasm_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/embedded_root.zig"),
        .target = wasm_target,
        .optimize = optimize,
    });
    @call(.auto, configureEmbeddedModule, .{ b, embedded_support_wasm_mod } ++ embedded_wasm_deps ++ .{addSnowballModule});

    const embedded_wasm_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/embedded/root.zig"),
        .target = wasm_target,
        .optimize = optimize,
    });
    embedded_wasm_mod.addImport("embedded_support", embedded_support_wasm_mod);

    const embedded_db_wasm_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/embedded/db.zig"),
        .target = wasm_target,
        .optimize = optimize,
    });
    embedded_db_wasm_mod.addImport("embedded_support", embedded_support_wasm_mod);

    const embedded_api_wasm_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/embedded/api.zig"),
        .target = wasm_target,
        .optimize = optimize,
    });
    embedded_api_wasm_mod.addImport("embedded_support", embedded_support_wasm_mod);
    embedded_api_wasm_mod.addImport("embedded_db_surface", embedded_db_wasm_mod);
    embedded_wasm_mod.addImport("embedded_db_surface", embedded_db_wasm_mod);
    embedded_wasm_mod.addImport("embedded_api_surface", embedded_api_wasm_mod);

    const antfly_embedded_db_pkg_wasm_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/db.zig"),
        .target = wasm_target,
        .optimize = optimize,
    });
    antfly_embedded_db_pkg_wasm_mod.addImport("embedded_db_surface", embedded_db_wasm_mod);

    const antfly_embedded_api_pkg_wasm_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/api.zig"),
        .target = wasm_target,
        .optimize = optimize,
    });
    antfly_embedded_api_pkg_wasm_mod.addImport("embedded_api_surface", embedded_api_wasm_mod);

    const antfly_embedded_pkg_wasm_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/root.zig"),
        .target = wasm_target,
        .optimize = optimize,
    });
    antfly_embedded_pkg_wasm_mod.addImport("embedded_surface", embedded_wasm_mod);

    antfly_embedded_build.addUnifiedWasmSteps(.{
        .b = b,
        .wasm_target = wasm_target,
        .antfly_version = antfly_version,
        .sentencepiece_proto_mod = sentencepiece_proto_mod,
        .protobuf_mod = protobuf_mod,
        .wasm_image_mod = wasm_image_mod,
        .wasm_platform_mod = wasm_platform_mod,
        .antfly_embedded_db_pkg_wasm_mod = antfly_embedded_db_pkg_wasm_mod,
        .antfly_embedded_api_pkg_wasm_mod = antfly_embedded_api_pkg_wasm_mod,
    });

    // Static library
    const lib = b.addLibrary(.{
        .linkage = .static,
        .name = "antfly-zig",
        .root_module = lib_mod,
    });
    _ = lib;

    const capi_steps = antfly_capi_build.addCApiSteps(.{
        .b = b,
        .target = target,
        .optimize = optimize,
        .strip = strip,
        .lib_mod = lib_mod,
        .structlog_mod = structlog_mod,
    });

    const fuzz_tabular_loader_mod = b.createModule(.{
        .root_source_file = b.path("lib/ml/tabular/src/fuzz_loader.zig"),
        .target = target,
        .optimize = optimize,
    });

    const run_lib_toon_conformance_after_fetch_quiet_step = antfly_conformance_build.addToonConformanceSteps(.{
        .b = b,
        .target = target,
        .optimize = optimize,
        .toon_mod = toon_mod,
    });

    const httpx_json_test_mod = b.createModule(.{
        .root_source_file = b.path("lib/httpx/src/util/json.zig"),
        .target = target,
        .optimize = optimize,
    });
    httpx_json_test_mod.addImport("antfly-json", json_mod);

    const api_json_helpers_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/api_json_helpers_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    api_json_helpers_test_mod.addImport("antfly-json", json_mod);

    const api_artifact_reprocess_jobs_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/api_artifact_reprocess_jobs_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    antfly_imports.configure(b, api_artifact_reprocess_jobs_test_mod, true, true);
    const run_api_query_tests = antfly_tests_build.addModuleTestStep(
        b,
        api_query_test_mod,
        "api-query-test",
        "Run focused API query merge and response tests",
        .{
            .filters = &antfly_tests_build.APIQueryTestFilters.core,
            .select_filters = false,
            .simple_runner = true,
        },
    ).run;
    const run_api_artifact_reprocess_jobs_tests = antfly_tests_build.addModuleTestStep(
        b,
        api_artifact_reprocess_jobs_test_mod,
        "lib-api-artifact-reprocess-jobs-test",
        "Run artifact reprocess job store tests",
        .{
            .filters = &antfly_tests_build.ArtifactReprocessJobTestFilters.store,
            .select_filters = false,
            .simple_runner = true,
        },
    ).run;

    const image_test_mod = b.createModule(.{
        .root_source_file = b.path("lib/image/image_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    image_test_mod.addImport("antfly_image", image_mod);

    const lib_image_spng_paths = detectSpngPaths(b, target);
    const lib_image_enable_spng = lib_image_spng_paths != null;
    antfly_benches_build.addLibraryBenchSteps(.{
        .b = b,
        .target = target,
        .lib_image_spng_paths = lib_image_spng_paths,
        .lib_image_enable_spng = lib_image_enable_spng,
        .inference_linalg_mod = inference_graph.inference_linalg_mod,
        .inference_audio_mod = inference_graph.inference_audio_mod,
    });

    const image_conformance_steps = antfly_conformance_build.addImageConformanceSteps(.{
        .b = b,
        .target = target,
        .optimize = optimize,
        .image_mod = image_mod,
        .spng_paths = lib_image_spng_paths,
        .enable_spng = lib_image_enable_spng,
    });

    const run_lib_generating_runtime_tests = antfly_tests_build.addModuleTestStep(
        b,
        lib_test_mod,
        "lib-generating-runtime-test",
        "Run generating backend adapter tests",
        .{ .filters = &antfly_tests_build.PackageTestFilters.generating_runtime },
    ).run;

    const run_lib_reranking_runtime_tests = antfly_tests_build.addModuleTestStep(
        b,
        lib_test_mod,
        "lib-reranking-runtime-test",
        "Run reranking backend adapter tests",
        .{ .filters = &antfly_tests_build.PackageTestFilters.reranking_runtime },
    ).run;

    const run_lib_common_tests = antfly_tests_build.addModuleTestStep(
        b,
        lib_test_mod,
        "lib-common-test",
        "Run common/provider registry tests",
        .{ .filters = &antfly_tests_build.PackageTestFilters.common },
    ).run;

    const run_lib_common_config_tests = antfly_tests_build.addModuleTestStep(
        b,
        lib_test_mod,
        "lib-common-config-test",
        "Run common/config tests",
        .{
            .filters = &antfly_tests_build.PackageTestFilters.common_config,
            .simple_runner = true,
        },
    ).run;

    const embedded_test_run = antfly_tests_build.addModuleTestStep(
        b,
        embedded_mod,
        "embedded-test",
        "Run embedded API tests",
        .{ .filters = &antfly_tests_build.PackageTestFilters.embedded },
    );
    const run_embedded_tests = embedded_test_run.run;

    const antfly_embedded_pkg_tests = b.addTest(.{
        .root_module = antfly_embedded_pkg_mod,
        .filters = selectTestFilters(b, &antfly_tests_build.PackageTestFilters.antfly_embedded_root),
    });
    const run_antfly_embedded_pkg_tests = b.addRunArtifact(antfly_embedded_pkg_tests);
    const antfly_embedded_db_pkg_tests = b.addTest(.{
        .root_module = antfly_embedded_db_pkg_mod,
        .filters = selectTestFilters(b, &antfly_tests_build.PackageTestFilters.antfly_embedded_db),
    });
    const run_antfly_embedded_db_pkg_tests = b.addRunArtifact(antfly_embedded_db_pkg_tests);
    const antfly_embedded_api_pkg_tests = b.addTest(.{
        .root_module = antfly_embedded_api_pkg_mod,
        .filters = selectTestFilters(b, &antfly_tests_build.PackageTestFilters.antfly_embedded_api),
    });
    const run_antfly_embedded_api_pkg_tests = b.addRunArtifact(antfly_embedded_api_pkg_tests);
    embedded_test_run.step.dependOn(&run_antfly_embedded_pkg_tests.step);
    embedded_test_run.step.dependOn(&run_antfly_embedded_db_pkg_tests.step);
    embedded_test_run.step.dependOn(&run_antfly_embedded_api_pkg_tests.step);

    const run_antfly_client_pkg_tests = antfly_tests_build.addModuleTestStep(
        b,
        antfly_client_pkg_mod,
        "antfly-client-test",
        "Run the standalone antfly-client package compile test",
        .{ .filters = &antfly_tests_build.PackageTestFilters.antfly_client },
    ).run;

    const root_test_skip_filters = antfly_tests_build.RootTestFilters.skip;
    const unit_progress_skip_filters = antfly_tests_build.RootTestFilters.unit_progress_skip;
    const root_module_tests = antfly_tests_build.addRootTestStep(b, lib_test_mod);
    const root_test_step = root_module_tests.step;

    const lake_scaffold_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/lake_scaffold_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    const lake_scaffold_transcribing_stub_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/testing/transcribing_stub.zig"),
        .target = target,
        .optimize = optimize,
    });
    lake_scaffold_transcribing_stub_mod.addImport("httpx", httpx_mod);
    lake_scaffold_test_mod.addOptions("build_options", build_options);
    lake_scaffold_test_mod.addImport("objectstore", objectstore_mod);
    lake_scaffold_test_mod.addImport("antfly_vector", vector_mod);
    lake_scaffold_test_mod.addImport("raft_engine", raft_engine_mod);
    lake_scaffold_test_mod.addImport("antfly_pdf", pdf_mod);
    lake_scaffold_test_mod.addImport("antfly_scraping", scraping_mod);
    lake_scaffold_test_mod.addImport("antfly_platform", platform_mod);
    lake_scaffold_test_mod.addImport("handlebars", handlebars_mod);
    lake_scaffold_test_mod.addImport("antfly_transcribing", lake_scaffold_transcribing_stub_mod);
    const lake_scaffold_tests = b.addTest(.{
        .root_module = lake_scaffold_test_mod,
    });
    const run_lake_scaffold_tests = b.addRunArtifact(lake_scaffold_tests);
    const lake_test_step = b.step("lake-test", "Run Antfly lake-native tests");
    lake_test_step.dependOn(&run_lake_scaffold_tests.step);
    root_test_step.dependOn(&run_lake_scaffold_tests.step);

    const graph_metric_tests = antfly_tests_build.addGraphMetricTestSteps(b, .{
        .root = lib_test_mod,
        .query_fan_in = api_query_test_mod,
    }, &root_test_skip_filters);
    const lite_native_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/lite_native_test.zig"),
        .target = target,
        .optimize = optimize,
    });
    antfly_imports.configure(b, lite_native_test_mod, true, true);
    const lite_native_tests = b.addTest(.{
        .root_module = lite_native_test_mod,
        .filters = antfly_tests_build.selectTestFilters(b, &antfly_tests_build.PackageTestFilters.lite_native),
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lite_native_tests = b.addRunArtifact(lite_native_tests);
    const lite_native_test_step = b.step("lite-native-test", "Run Antfly Lite native tests");
    lite_native_test_step.dependOn(&run_lite_native_tests.step);

    const lite_cli_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/lite_cli_test.zig"),
        .target = target,
        .optimize = optimize,
    });
    lite_cli_test_mod.addImport("antfly-zig", lib_mod);
    lite_cli_test_mod.addImport("antfly-client", antfly_client_pkg_mod);
    lite_cli_test_mod.addImport("httpx", httpx_mod);
    lite_cli_test_mod.addImport("antfly_vellum", vellum_mod);
    lite_cli_test_mod.addImport("raft_engine", raft_engine_mod);
    lite_cli_test_mod.addImport("structlog", structlog_mod);
    lite_cli_test_mod.addImport("antfly_platform", platform_mod);
    lite_cli_test_mod.addImport("handlebars", handlebars_mod);
    lite_cli_test_mod.addOptions("build_options", build_options);
    const lite_cli_tests = b.addTest(.{
        .root_module = lite_cli_test_mod,
        .filters = antfly_tests_build.selectTestFilters(b, &antfly_tests_build.PackageTestFilters.lite_cli),
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lite_cli_tests = b.addRunArtifact(lite_cli_tests);

    const recall_test_run = antfly_tests_build.addModuleTestStep(
        b,
        lib_test_mod,
        "recall-test",
        "Run HBC vector recall quality tests",
        .{
            .filters = &antfly_tests_build.RecallTestFilters.hbc,
            .simple_runner = true,
        },
    );
    const recall_test_step = recall_test_run.step;

    const raft_unit_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = selectTestFilters(b, &antfly_tests_build.RaftTestFilters.root),
    });
    const run_raft_unit_tests = b.addRunArtifact(raft_unit_tests);

    const raft_transport_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = &antfly_tests_build.RaftTestFilters.transport,
    });
    const run_raft_transport_tests = b.addRunArtifact(raft_transport_tests);

    const http_low_fd_ratchet_tests = b.addTest(.{
        .root_module = lib_test_mod,
        // Compile the shared HTTP module for declaration reachability, but
        // execute only the process-level regression below. The wider common
        // and transport buckets contain high-cardinality socket tests that do
        // not fit inside this target's 256-descriptor process limit.
        .filters = &antfly_tests_build.HTTPTestFilters.low_fd_ratchet_compile,
    });
    const run_http_low_fd_ratchet_tests = addFilteredTestRunArtifactWithRuntimeFilters(
        b,
        http_low_fd_ratchet_tests,
        &antfly_tests_build.HTTPTestFilters.low_fd_ratchet_runtime,
    );
    const http_low_fd_ratchet_test_step = b.step(
        "http-low-fd-ratchet-test",
        "Run the process-level low-FD HTTP worker ratchet regression",
    );
    http_low_fd_ratchet_test_step.dependOn(&run_http_low_fd_ratchet_tests.step);

    const lib_raft_sim_tests = b.addTest(.{
        .root_module = raft_sim_test_mod,
        .filters = &antfly_tests_build.RaftTestFilters.sim,
    });
    const run_lib_raft_sim_tests = b.addRunArtifact(lib_raft_sim_tests);
    const lib_raft_sim_test_step = b.step("lib-raft-sim-test", "Run raft simulation harness tests");
    lib_raft_sim_test_step.dependOn(&run_lib_raft_sim_tests.step);

    const lib_raft_chaos_tests = b.addTest(.{
        .root_module = raft_sim_test_mod,
        .filters = &antfly_tests_build.RaftTestFilters.chaos,
    });
    const run_lib_raft_chaos_tests = b.addRunArtifact(lib_raft_chaos_tests);
    const lib_raft_chaos_test_step = b.step("lib-raft-chaos-test", "Run longer raft restart/HTTP simulation campaigns");
    lib_raft_chaos_test_step.dependOn(&run_lib_raft_chaos_tests.step);

    const run_lib_lsm_backend_sim_tests = antfly_tests_build.addModuleTestStep(
        b,
        lib_test_mod,
        "lib-lsm-backend-sim-test",
        "Run LSM backend storage workload simulation tests",
        .{ .filters = &antfly_tests_build.PackageTestFilters.lsm_backend_sim },
    ).run;

    const lib_lsm_backend_chaos_test_run = antfly_tests_build.addModuleTestStep(
        b,
        lib_test_mod,
        "lib-lsm-backend-chaos-test",
        "Run longer LSM backend compaction chaos campaigns",
        .{ .filters = &antfly_tests_build.PackageTestFilters.lsm_backend_chaos },
    );
    const lib_lsm_backend_chaos_tests = lib_lsm_backend_chaos_test_run.tests;
    _ = lib_lsm_backend_chaos_test_run.step;
    const ha_chaos_test_run = antfly_tests_build.addModuleTestStep(
        b,
        lib_test_mod,
        "ha-chaos-test",
        "Run HA hot-standby crash and partition hardening tests",
        .{ .filters = &antfly_tests_build.HATestFilters.chaos },
    );
    const lib_ha_chaos_tests = ha_chaos_test_run.tests;
    const lib_ha_compat_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = &antfly_tests_build.HATestFilters.compat,
    });
    const run_lib_ha_compat_tests = b.addRunArtifact(lib_ha_compat_tests);

    const test_step = b.step("test", "Run default package test aggregates");
    const antfly_test_step = b.step("antfly-test", "Run default Antfly unit, simulation, integration, chaos, and recall checks");
    const conformance_test_step = b.step("conformance-test", "Fetch and run conformance suites");
    const promotion_test_step = b.step("promotion-test", "Run explicit promotion and release qualification suites");
    const soak_test_step = b.step("soak-test", "Run long-running soak test aggregates");

    dependOnAll(conformance_test_step, &.{
        run_lib_toon_conformance_after_fetch_quiet_step,
        &image_conformance_steps.tests_after_fetch_quiet.step,
        image_conformance_steps.corpus_verify_jpeg_quiet,
        image_conformance_steps.corpus_verify_png_quiet,
        image_conformance_steps.corpus_verify_png_spng_quiet,
        image_conformance_steps.corpus_verify_gif_quiet,
        image_conformance_steps.jpeg_seed_corpora_after_fetch_quiet,
    });

    const unit_test_step = b.step("unit-test", "Run hermetic unit and focused integration test buckets without metadata chaos simulations");
    const unit_test_progress_step = b.step("unit-test-progress", "Run labeled major unit test suites to expose slow or stuck phases");

    const lib_db_test = antfly_tests_build.addDBRootTestStep(b, lib_test_mod);

    const serverless_test_run = antfly_tests_build.addModuleTestStep(
        b,
        lib_test_mod,
        "serverless-test",
        "Run serverless and serverless transport tests",
        .{
            .filters = &antfly_tests_build.PackageTestFilters.serverless,
            .simple_runner = true,
        },
    );
    const serverless_tests = serverless_test_run.tests;
    const run_serverless_tests = serverless_test_run.run;

    const data_runtime_tests = b.addTest(.{
        .root_module = data_runtime_test_mod,
        .filters = &antfly_tests_build.DataTestFilters.runtime,
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_data_runtime_tests = b.addRunArtifact(data_runtime_tests);

    const run_lib_data_storage_tests = antfly_tests_build.addModuleTestStep(
        b,
        data_storage_test_mod,
        "lib-data-storage-test",
        "Run focused data storage tests",
        .{
            .filters = &antfly_tests_build.DataTestFilters.storage,
            .simple_runner = true,
        },
    ).run;

    const lib_db_module_tests = antfly_tests_build.addDBRootModuleTestSteps(b, lib_test_mod);

    const metadata_fk_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/metadata_fk_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    antfly_imports.configure(b, metadata_fk_test_mod, true, true);
    const metadata_tests = antfly_tests_build.addMetadataTestSteps(b, lib_test_mod, metadata_fk_test_mod);
    const metadata_root_tests = metadata_tests.root.tests;
    const run_lib_metadata_sim_smoke_tests = metadata_tests.sim_smoke.run;
    const run_lib_metadata_vopr_tests = metadata_tests.vopr.run;
    const run_lib_metadata_vopr_chaos_tests = metadata_tests.vopr_chaos.run;
    const lib_metadata_public_chaos_test_step = metadata_tests.chaos.public;
    const run_lib_metadata_sim_public_tests = metadata_tests.sim_public.run;

    const api_focused_test_modules = antfly_tests_build.makeAPIFocusedTestModules(b, lib_test_mod, target, optimize, antfly_imports);
    const api_focused_tests = antfly_tests_build.addAPIFocusedTestSteps(b, api_focused_test_modules, &generated_steps.openapi_root_check.step);
    const run_public_api_parity_tests = api_focused_tests.public_api_parity.run;
    const run_public_api_parity_aggregate_tests = run_public_api_parity_tests;
    run_public_api_parity_aggregate_tests.step.dependOn(&generated_steps.openapi_root_check.step);
    const run_public_api_graph_metric_e2e_tests = api_focused_tests.public_api_graph_metric_e2e.run;
    const run_lib_api_auth_tests = api_focused_tests.auth.run;
    const run_lib_api_logic_tests = api_focused_tests.logic.run;

    // Keep API tests wired at stable suite granularity. Leaf implementation
    // tests should join these roots via pkg/antfly/build/tests.zig filters,
    // not by adding one top-level build step per regression.
    const api_table_tests = antfly_tests_build.addAPITableTestRootSteps(b, .{
        .root = lib_test_mod,
        .target = target,
        .optimize = optimize,
    }, antfly_imports);
    api_table_tests.api_table_writes_production_regression_unit.step.dependOn(&run_public_api_parity_aggregate_tests.step);
    api_table_tests.provisioned_query_visibility_unit.step.dependOn(&api_table_tests.api_table_writes_production_regression_unit.step);
    _ = antfly_tests_build.addDocIdTestStep(
        b,
        api_focused_tests.docid_lifecycle,
        api_table_tests,
        lib_db_module_tests.result_shape,
    );

    _ = antfly_tests_build.addAPITableAggregateTestStep(b, api_table_tests, .{
        .data_storage = run_lib_data_storage_tests,
        .data_runtime = run_data_runtime_tests,
        .metadata_sim_smoke = run_lib_metadata_sim_smoke_tests,
        .metadata_sim_public = run_lib_metadata_sim_public_tests,
        .metadata_vopr = run_lib_metadata_vopr_tests,
        .metadata_vopr_chaos = run_lib_metadata_vopr_chaos_tests,
        .metadata_public_chaos = lib_metadata_public_chaos_test_step,
        .db_result_shape = lib_db_module_tests.result_shape,
    });

    antfly_tests_build.dependOnAPIGeneratedChecks(generated_steps.generated_check, api_table_tests);

    const run_lib_metadata_sim_forward_tests = metadata_tests.sim_forward.run;

    const storage_tests = antfly_tests_build.addStorageTestSteps(b, lib_test_mod, &unit_progress_skip_filters);
    const run_lib_storage_tests = storage_tests.root.run;
    const run_ha_tests = storage_tests.ha.run;
    const lib_storage_progress_tests = storage_tests.progress.tests;
    const run_lsm_backend_tests = storage_tests.lsm_backend.run;
    const run_resource_budget_tests = storage_tests.resource_budget.run;

    const main_capture_tests = antfly_tests_build.addMainCaptureTestSteps(.{
        .b = b,
        .target = target,
        .optimize = optimize,
        .antfly_imports = antfly_imports,
        .lib_test_mod = lib_test_mod,
        .lib_mod = lib_mod,
        .antfly_client_pkg_mod = antfly_client_pkg_mod,
        .httpx_mod = httpx_mod,
        .vellum_mod = vellum_mod,
        .raft_engine_mod = raft_engine_mod,
        .structlog_mod = structlog_mod,
        .platform_mod = platform_mod,
        .handlebars_mod = handlebars_mod,
        .build_options = build_options,
        .sentencepiece_proto_mod = sentencepiece_proto_mod,
        .pdf_mod = pdf_mod,
        .raft_runtime_test_mod = raft_runtime_test_mod,
        .raft_restore_test_mod = raft_restore_test_mod,
        .filesystem_capacity_test_mod = filesystem_capacity_test_mod,
        .resource_budget_run = run_resource_budget_tests,
        .api_artifact_reprocess_jobs_test_mod = api_artifact_reprocess_jobs_test_mod,
        .data_runtime_test_mod = data_runtime_test_mod,
        .openapi_root_check = &generated_steps.openapi_root_check.step,
    });

    const ha_test_step = b.step("ha-test", "Run HA hot-standby storage and replication compatibility tests");
    ha_test_step.dependOn(&run_ha_tests.step);
    ha_test_step.dependOn(&run_lib_ha_compat_tests.step);

    const sim_test_step = b.step("sim-test", "Run mocked-time Antfly simulation suites");
    sim_test_step.dependOn(metadata_tests.sim_all);
    sim_test_step.dependOn(&run_lib_raft_sim_tests.step);

    const integration_test_step = b.step("integration-test", "Run focused real HTTP and public API integration suites");
    integration_test_step.dependOn(&run_lib_metadata_sim_public_tests.step);
    integration_test_step.dependOn(&run_lib_metadata_sim_forward_tests.step);
    integration_test_step.dependOn(&run_public_api_parity_aggregate_tests.step);
    integration_test_step.dependOn(graph_metric_tests.integration);
    integration_test_step.dependOn(&run_public_api_graph_metric_e2e_tests.step);

    const chaos_test_step = b.step("chaos-test", "Run bounded generated chaos campaigns with labeled progress");
    var chaos_progress_tail: ?*std.Build.Step = null;
    chaos_test_step.dependOn(metadata_tests.chaos.all);
    chaos_progress_tail = chainLabeledRun(b, lib_lsm_backend_chaos_tests, "lib-lsm-backend-chaos-test", chaos_progress_tail);
    if (include_ha_tests_in_aggregates) {
        chaos_progress_tail = chainLabeledRun(b, lib_ha_chaos_tests, "ha-chaos-test", chaos_progress_tail);
    }
    chaos_test_step.dependOn(chaos_progress_tail.?);

    const chaos_soak_test_step = b.step("chaos-soak-test", "Run broad legacy metadata and raft chaos simulation soaks");
    var chaos_soak_progress_tail: ?*std.Build.Step = null;
    chaos_soak_progress_tail = antfly_tests_build.chainMetadataChaosSoakTests(b, lib_test_mod, chaos_soak_progress_tail);
    chaos_soak_progress_tail = chainLabeledRun(b, lib_raft_chaos_tests, "lib-raft-chaos-test", chaos_soak_progress_tail);
    chaos_soak_test_step.dependOn(chaos_soak_progress_tail.?);
    soak_test_step.dependOn(chaos_soak_test_step);

    const template_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/template_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    antfly_imports.configure(b, template_test_mod, false, true);
    const standalone_module_tests = antfly_tests_build.addStandaloneModuleTestSteps(b, .{
        .regex = regex_mod,
        .jsonschema = jsonschema_mod,
        .json = json_mod,
        .ml_tabular = ml_tabular_mod,
        .fuzz_tabular_loader = fuzz_tabular_loader_mod,
        .toon = toon_mod,
        .mcp = mcp_mod,
        .a2a = a2a_mod,
        .matcher = matcher_mod,
        .resolver = resolver_mod,
        .httpx_json = httpx_json_test_mod,
        .httpx = httpx_mod,
        .api_json_helpers = api_json_helpers_test_mod,
        .generating = generating_mod,
        .embeddings = embeddings_mod,
        .vectorindex = vectorindex_mod,
        .chunking = chunking_mod,
        .readers = readers_mod,
        .extracting = extracting_mod,
        .image = image_test_mod,
        .reranking = reranking_mod,
        .casbin = casbin_mod,
        .usermgr = usermgr_mod,
        .template = template_test_mod,
    });
    const httpx_transport_regression = antfly_tests_build.addModuleTestStep(
        b,
        httpx_mod,
        "lib-httpx-transport-regression-test",
        "Run focused HTTP transport response serialization regressions",
        .{
            .filters = &antfly_tests_build.PackageTestFilters.httpx_transport_regression,
            .simple_runner = true,
        },
    );
    unit_test_step.dependOn(&httpx_transport_regression.run.step);

    const audio_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/audio_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    antfly_imports.configure(b, audio_test_mod, false, true);
    const lib_audio_tests = b.addTest(.{
        .root_module = audio_test_mod,
    });
    const run_lib_audio_tests = b.addRunArtifact(lib_audio_tests);
    const lib_transcribing_tests = b.addTest(.{
        .root_module = transcribing_mod,
    });
    const run_lib_transcribing_tests = b.addRunArtifact(lib_transcribing_tests);
    const lib_audio_test_step = b.step("lib-audio-test", "Run audio transcribing and synthesizing runtime tests");
    lib_audio_test_step.dependOn(&run_lib_audio_tests.step);
    lib_audio_test_step.dependOn(&run_lib_transcribing_tests.step);

    antfly_conformance_build.addAudioConformanceSteps(.{
        .b = b,
        .target = target,
        .ffmpeg_paths = termite_ffmpeg_paths,
        .inference_build_options_mod = inference_build_options_mod,
        .conformance_test_step = conformance_test_step,
    });

    const standalone_runtime_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/standalone_runtime_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    var standalone_runtime_imports = antfly_imports;
    standalone_runtime_imports.build_options = standalone_runtime_build_options;
    standalone_runtime_imports.configure(b, standalone_runtime_test_mod, true, true);
    const usermgr_storage_standalone_runtime_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/usermgr/storage_imports.zig"),
        .target = target,
        .optimize = optimize,
    });
    usermgr_storage_standalone_runtime_test_mod.addImport("antfly_root", standalone_runtime_test_mod);
    usermgr_storage_standalone_runtime_test_mod.addImport("antfly_platform", platform_mod);
    standalone_runtime_test_mod.addImport("usermgr_storage", usermgr_storage_standalone_runtime_test_mod);
    const standalone_tests = b.addTest(.{
        .root_module = standalone_runtime_test_mod,
        .filters = &antfly_tests_build.StandaloneRuntimeTestFilters.focused,
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lib_standalone_runtime_tests = antfly_tests_build.addFilteredTestRunArtifact(b, standalone_tests);
    const lib_standalone_runtime_test_step = b.step("lib-standalone-runtime-test", "Run focused standalone runtime tests");
    lib_standalone_runtime_test_step.dependOn(&run_lib_standalone_runtime_tests.step);

    const runtime_test_step = b.step("runtime-test", "Run focused data and standalone runtime tests");
    runtime_test_step.dependOn(&run_data_runtime_tests.step);
    runtime_test_step.dependOn(&run_lib_standalone_runtime_tests.step);

    const raft_test_step = b.step("raft-test", "Run raft unit and transport tests");
    raft_test_step.dependOn(&run_raft_unit_tests.step);
    raft_test_step.dependOn(&run_raft_transport_tests.step);
    raft_test_step.dependOn(&main_capture_tests.raft_runtime.step);
    raft_test_step.dependOn(&main_capture_tests.raft_restore.step);
    raft_test_step.dependOn(&main_capture_tests.raft_library.step);
    raft_test_step.dependOn(&main_capture_tests.raft_ready_continuation.step);
    raft_test_step.dependOn(&api_table_tests.raft_transition_runtime_docid.step);

    unit_test_step.dependOn(&standalone_module_tests.regex.run.step);
    unit_test_step.dependOn(&standalone_module_tests.jsonschema.run.step);
    unit_test_step.dependOn(&standalone_module_tests.generating.run.step);
    unit_test_step.dependOn(&standalone_module_tests.embeddings.run.step);
    unit_test_step.dependOn(&standalone_module_tests.vectorindex.run.step);
    unit_test_step.dependOn(&standalone_module_tests.chunking.run.step);
    unit_test_step.dependOn(&run_lib_generating_runtime_tests.step);
    unit_test_step.dependOn(&standalone_module_tests.reranking.run.step);
    unit_test_step.dependOn(&run_lib_reranking_runtime_tests.step);
    unit_test_step.dependOn(&run_lib_common_tests.step);
    unit_test_step.dependOn(&run_lib_common_config_tests.step);
    unit_test_step.dependOn(&standalone_module_tests.casbin.run.step);
    unit_test_step.dependOn(&standalone_module_tests.usermgr.run.step);
    unit_test_step.dependOn(&run_embedded_tests.step);
    unit_test_step.dependOn(&run_antfly_embedded_pkg_tests.step);
    unit_test_step.dependOn(&capi_steps.run_capi_tests.step);
    unit_test_step.dependOn(&run_lite_native_tests.step);
    unit_test_step.dependOn(&run_lite_cli_tests.step);
    unit_test_step.dependOn(&main_capture_tests.cmd.step);
    unit_test_step.dependOn(&lib_db_test.run.step);
    unit_test_step.dependOn(&lib_db_module_tests.result_shape.step);
    unit_test_step.dependOn(&run_serverless_tests.step);
    unit_test_step.dependOn(&run_lib_data_storage_tests.step);
    unit_test_step.dependOn(metadata_tests.root.step);
    antfly_tests_build.dependOnAPITableUnitTestRuns(unit_test_step, api_table_tests);
    unit_test_step.dependOn(&api_table_tests.provisioned_query_visibility_unit.step);
    unit_test_step.dependOn(&api_table_tests.api_table_writes_production_regression_unit.step);
    unit_test_step.dependOn(&run_lib_api_auth_tests.step);
    unit_test_step.dependOn(&run_lib_api_logic_tests.step);
    unit_test_step.dependOn(&run_api_query_tests.step);
    unit_test_step.dependOn(&run_api_artifact_reprocess_jobs_tests.step);
    unit_test_step.dependOn(&main_capture_tests.api_restore_jobs.step);
    unit_test_step.dependOn(&main_capture_tests.portable_backup.step);
    unit_test_step.dependOn(&main_capture_tests.common_secrets.step);
    unit_test_step.dependOn(&main_capture_tests.introducer.step);
    const run_lib_api_derived_coverage_tests = main_capture_tests.api_derived_coverage;
    unit_test_step.dependOn(&run_lib_api_derived_coverage_tests.step);
    unit_test_step.dependOn(&run_public_api_parity_aggregate_tests.step);
    unit_test_step.dependOn(&standalone_module_tests.template.run.step);
    unit_test_step.dependOn(&standalone_module_tests.toon.run.step);
    unit_test_step.dependOn(&standalone_module_tests.mcp.run.step);
    unit_test_step.dependOn(&standalone_module_tests.a2a.run.step);
    unit_test_step.dependOn(&standalone_module_tests.image.run.step);
    unit_test_step.dependOn(&run_lib_audio_tests.step);
    unit_test_step.dependOn(&main_capture_tests.hf_tokenizer.step);
    unit_test_step.dependOn(delegated_inference_steps.inference_test);
    unit_test_step.dependOn(delegated_inference_steps.inference_finetune_test);
    unit_test_step.dependOn(delegated_inference_steps.inference_onnx_test);
    unit_test_step.dependOn(&main_capture_tests.raft_runtime.step);
    unit_test_step.dependOn(&main_capture_tests.raft_restore.step);
    unit_test_step.dependOn(&main_capture_tests.raft_ready_continuation.step);
    unit_test_step.dependOn(&main_capture_tests.raft_library.step);
    unit_test_step.dependOn(runtime_test_step);
    unit_test_step.dependOn(lib_standalone_runtime_test_step);
    if (include_ha_tests_in_aggregates) {
        unit_test_step.dependOn(ha_test_step);
    }
    unit_test_step.dependOn(&run_raft_unit_tests.step);
    unit_test_step.dependOn(&run_raft_transport_tests.step);

    var unit_progress_tail: ?*std.Build.Step = null;
    unit_progress_tail = chainLabeledRun(b, lib_storage_progress_tests, "lib-storage-test", unit_progress_tail);
    if (include_ha_tests_in_aggregates) {
        unit_progress_tail = chainLabeledRun(b, storage_tests.ha.tests, "ha-test", unit_progress_tail);
    }
    unit_progress_tail = chainLabeledRunStep(b, lib_db_test.run, antfly_tests_build.db_root_step_name, unit_progress_tail);
    unit_progress_tail = chainLabeledRun(b, metadata_root_tests, "metadata-test", unit_progress_tail);
    unit_progress_tail = chainLabeledRun(b, raft_unit_tests, "raft-test", unit_progress_tail);
    unit_progress_tail = chainLabeledRun(b, raft_transport_tests, "raft.transport", unit_progress_tail);
    unit_progress_tail = chainLabeledRun(b, serverless_tests, "serverless-test", unit_progress_tail);
    unit_progress_tail = chainLabeledRun(b, standalone_module_tests.template.tests, "lib-template-test", unit_progress_tail);
    unit_test_progress_step.dependOn(unit_progress_tail.?);

    const storage_lmdb_test_mod = makeLmdbModule(b, "pkg/antfly/src/storage/lmdb.zig", target, optimize, build_options, lmdb_engine_mod, platform_mod);

    const storage_sim_runtime_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/storage_sim_runtime_root.zig"),
        .target = target,
        .optimize = optimize,
    });

    const storage_lmdb_soak_build_options = makeLmdbBuildOptions(b, lmdb_backend, lmdb_evented_async_io, true);
    const storage_lmdb_soak_engine_mod = makeLmdbEngineModule(b, target, optimize, true, storage_lmdb_soak_build_options);
    const storage_lmdb_soak_test_mod = makeLmdbModule(b, "pkg/antfly/src/storage/lmdb.zig", target, optimize, storage_lmdb_soak_build_options, storage_lmdb_soak_engine_mod, platform_mod);

    const docstore_test_mod = makeLmdbModule(b, "pkg/antfly/src/docstore_test_root.zig", target, optimize, build_options, lmdb_engine_mod, platform_mod);
    docstore_test_mod.addImport("bloom", bloom_mod);
    docstore_test_mod.addImport("antfly_vellum", vellum_mod);
    docstore_test_mod.addImport("antfly_regex", regex_mod);
    docstore_test_mod.addImport("antfly_reranking", reranking_mod);

    const shard_test_mod = makeLmdbModule(b, "pkg/antfly/src/shard_test_root.zig", target, optimize, build_options, lmdb_engine_mod, platform_mod);
    shard_test_mod.addImport("bloom", bloom_mod);
    shard_test_mod.addImport("antfly_vellum", vellum_mod);
    shard_test_mod.addImport("antfly_regex", regex_mod);
    shard_test_mod.addImport("antfly_reranking", reranking_mod);

    const wal_test_mod = makeLmdbModule(b, "pkg/antfly/src/wal_test_root.zig", target, optimize, build_options, lmdb_engine_mod, platform_mod);
    wal_test_mod.addImport("bloom", bloom_mod);
    wal_test_mod.addImport("antfly_vellum", vellum_mod);
    wal_test_mod.addImport("antfly_regex", regex_mod);
    wal_test_mod.addImport("antfly_reranking", reranking_mod);
    wal_test_mod.addImport("structlog", structlog_mod);

    const wal_soak_build_options = makeLmdbBuildOptions(b, lmdb_backend, lmdb_evented_async_io, true);
    const wal_soak_engine_mod = makeLmdbEngineModule(b, target, optimize, true, wal_soak_build_options);
    const wal_soak_test_mod = makeLmdbModule(b, "pkg/antfly/src/wal_test_root.zig", target, optimize, wal_soak_build_options, wal_soak_engine_mod, platform_mod);
    wal_soak_test_mod.addImport("bloom", bloom_mod);
    wal_soak_test_mod.addImport("antfly_vellum", vellum_mod);
    wal_soak_test_mod.addImport("antfly_regex", regex_mod);
    wal_soak_test_mod.addImport("antfly_reranking", reranking_mod);

    const persistent_test_mod = makeLmdbModule(b, "pkg/antfly/src/persistent_test_root.zig", target, optimize, build_options, lmdb_engine_mod, platform_mod);
    persistent_test_mod.addImport("bloom", bloom_mod);
    persistent_test_mod.addImport("antfly_vellum", vellum_mod);
    persistent_test_mod.addImport("antfly_regex", regex_mod);
    persistent_test_mod.addImport("antfly_vector", vector_mod);
    persistent_test_mod.addImport("antfly_vectorindex", vectorindex_mod);
    persistent_test_mod.addImport("antfly_reranking", reranking_mod);
    persistent_test_mod.addImport("structlog", structlog_mod);

    const persistent_soak_build_options = makeLmdbBuildOptions(b, lmdb_backend, lmdb_evented_async_io, true);
    const persistent_soak_engine_mod = makeLmdbEngineModule(b, target, optimize, true, persistent_soak_build_options);
    const persistent_soak_test_mod = makeLmdbModule(b, "pkg/antfly/src/persistent_test_root.zig", target, optimize, persistent_soak_build_options, persistent_soak_engine_mod, platform_mod);
    persistent_soak_test_mod.addImport("bloom", bloom_mod);
    persistent_soak_test_mod.addImport("antfly_vellum", vellum_mod);
    persistent_soak_test_mod.addImport("antfly_regex", regex_mod);
    persistent_soak_test_mod.addImport("antfly_vector", vector_mod);
    persistent_soak_test_mod.addImport("antfly_vectorindex", vectorindex_mod);
    persistent_soak_test_mod.addImport("antfly_reranking", reranking_mod);

    const index_manager_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/index_manager_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    antfly_imports.configure(b, index_manager_test_mod, true, true);
    const usermgr_storage_index_manager_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/usermgr/storage_imports.zig"),
        .target = target,
        .optimize = optimize,
    });
    usermgr_storage_index_manager_test_mod.addImport("antfly_root", index_manager_test_mod);
    usermgr_storage_index_manager_test_mod.addImport("antfly_platform", platform_mod);
    index_manager_test_mod.addImport("usermgr_storage", usermgr_storage_index_manager_test_mod);

    const db_test_mod = makeLmdbModule(b, "pkg/antfly/src/db_test_root.zig", target, optimize, build_options, lmdb_engine_mod, platform_mod);
    const transcribing_db_test_stub_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/testing/transcribing_stub.zig"),
        .target = target,
        .optimize = optimize,
    });
    transcribing_db_test_stub_mod.addImport("httpx", httpx_mod);
    antfly_imports.configure(b, db_test_mod, false, link_libc);
    db_test_mod.addImport("antfly_transcribing", transcribing_db_test_stub_mod);
    const usermgr_storage_db_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/usermgr/storage_imports.zig"),
        .target = target,
        .optimize = optimize,
    });
    usermgr_storage_db_test_mod.addImport("antfly_root", db_test_mod);
    usermgr_storage_db_test_mod.addImport("antfly_platform", platform_mod);
    db_test_mod.addImport("usermgr_storage", usermgr_storage_db_test_mod);

    const db_storage_tests = antfly_tests_build.addDBStorageTestSteps(b, db_test_mod);
    sim_test_step.dependOn(&db_storage_tests.sim.step);

    const release_blocker_regression_tests = b.addTest(.{
        .root_module = db_test_mod,
        .filters = antfly_tests_build.compileFiltersWithAnchors(
            b,
            &antfly_tests_build.release_blocker_compile_anchors,
            &antfly_tests_build.release_blocker_regression_filters,
        ),
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_release_blocker_regression_tests = addFilteredTestRunArtifactWithRuntimeFilters(
        b,
        release_blocker_regression_tests,
        &antfly_tests_build.release_blocker_regression_filters,
    );
    const release_blocker_regression_step = b.step(
        "release-blocker-regression-test",
        "Run selective ANN and post-delete full-text release-blocker regressions",
    );
    release_blocker_regression_step.dependOn(&run_release_blocker_regression_tests.step);
    unit_test_step.dependOn(&run_release_blocker_regression_tests.step);

    const release_scale_tests = b.addTest(.{
        .root_module = db_test_mod,
        .filters = &antfly_tests_build.release_scale_test_filters,
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_release_scale_tests = addFilteredTestRunArtifact(b, release_scale_tests);
    const release_scale_test_step = b.step(
        "release-scale-test",
        "Run corpus-scale ANN and full-text release regressions",
    );
    release_scale_test_step.dependOn(&run_release_scale_tests.step);

    const sparse_test_mod = makeLmdbModule(b, "pkg/antfly/src/sparse_test_root.zig", target, optimize, build_options, lmdb_engine_mod, platform_mod);
    sparse_test_mod.addImport("bloom", bloom_mod);
    sparse_test_mod.addImport("antfly_vellum", vellum_mod);
    sparse_test_mod.addImport("antfly_regex", regex_mod);
    sparse_test_mod.addImport("antfly_reranking", reranking_mod);

    const derived_log_test_mod = makeLmdbModule(b, "pkg/antfly/src/derived_log_test_root.zig", target, optimize, build_options, lmdb_engine_mod, platform_mod);
    derived_log_test_mod.addImport("bloom", bloom_mod);
    derived_log_test_mod.addImport("antfly_vellum", vellum_mod);
    derived_log_test_mod.addImport("antfly_regex", regex_mod);
    derived_log_test_mod.addImport("antfly_reranking", reranking_mod);
    const storage_backend_tests = antfly_tests_build.addStorageBackendTestSteps(b, .{
        .lmdb_engine = lmdb_engine_mod,
        .storage_lmdb = storage_lmdb_test_mod,
        .storage_lmdb_soak = storage_lmdb_soak_test_mod,
        .storage_sim_runtime = storage_sim_runtime_test_mod,
        .docstore = docstore_test_mod,
        .shard = shard_test_mod,
        .wal = wal_test_mod,
        .wal_soak = wal_soak_test_mod,
        .persistent = persistent_test_mod,
        .persistent_soak = persistent_soak_test_mod,
        .index_manager = index_manager_test_mod,
        .sparse = sparse_test_mod,
        .derived_log = derived_log_test_mod,
    }, .{
        .lsm_backend_sim = run_lib_lsm_backend_sim_tests,
    });
    sim_test_step.dependOn(storage_backend_tests.storage_vopr);
    soak_test_step.dependOn(storage_backend_tests.storage_sim_soak);

    // Default Antfly unit coverage is hermetic: no network fetchers, no
    // benchmarks, and no soak/conformance suites that require external corpora.
    // Focused local test steps stay available separately; broader module suites
    // are wired here once.
    dependOnAll(unit_test_step, &.{
        &standalone_module_tests.json.run.step,
        &standalone_module_tests.httpx_json.run.step,
        &standalone_module_tests.httpx.run.step,
        &standalone_module_tests.api_json_helpers.run.step,
        &run_antfly_client_pkg_tests.step,
        &root_module_tests.run.step,
        metadata_tests.root.step,
        &run_lib_storage_tests.step,
        &run_lsm_backend_tests.step,
        &run_resource_budget_tests.step,
        &storage_backend_tests.lmdb.run.step,
        &storage_backend_tests.storage_lmdb.run.step,
        &storage_backend_tests.docstore.run.step,
        &storage_backend_tests.shard.run.step,
        &storage_backend_tests.wal.run.step,
        &storage_backend_tests.persistent.run.step,
        &storage_backend_tests.index_manager.run.step,
        &db_storage_tests.all.step,
        &storage_backend_tests.sparse.run.step,
        &storage_backend_tests.derived_log.run.step,
        graph_metric_tests.unit,
        &graph_metric_tests.smoke.step,
    });

    const bench_steps = antfly_benches_build.addBenchSteps(.{
        .b = b,
        .target = target,
        .optimize = optimize,
        .lmdb_backend = lmdb_backend,
        .lmdb_evented_async_io = lmdb_evented_async_io,
        .lite_local_inference_runtime = lite_local_inference_runtime,
        .antfly_version = antfly_version,
        .with_tla = with_tla,
        .lib_mod = lib_mod,
        .platform_mod = platform_mod,
        .lmdb_engine_mod = lmdb_engine_mod,
        .json_mod = json_mod,
        .inference_tokenizer_mod = inference_graph.inference_tokenizer_mod,
        .indexes_openapi_mod = indexes_openapi_mod,
        .metadata_openapi_mod = metadata_openapi_mod,
        .query_openapi_mod = query_openapi_mod,
        .bloom_mod = bloom_mod,
        .vector_mod = vector_mod,
        .vectorindex_mod = vectorindex_mod,
        .matcher_mod = matcher_mod,
        .resolver_mod = resolver_mod,
        .vellum_mod = vellum_mod,
        .regex_mod = regex_mod,
        .reranking_mod = reranking_mod,
        .scraping_mod = scraping_mod,
        .raft_engine_mod = raft_engine_mod,
        .run_lib_ha_compat_tests = run_lib_ha_compat_tests,
        .include_ha_tests_in_aggregates = include_ha_tests_in_aggregates,
        .makeLmdbBuildOptions = makeLmdbBuildOptions,
        .makeRootBuildOptions = makeRootBuildOptions,
        .makeLmdbEngineModule = makeLmdbEngineModule,
        .makeLmdbModule = makeLmdbModule,
        .addSnowballModule = addSnowballModule,
    });

    const antfly_main_mod = if (edition == .full) blk: {
        const mod = b.createModule(.{
            .root_source_file = b.path("pkg/antfly/src/main.zig"),
            .target = target,
            .optimize = optimize,
            .sanitize_thread = sanitize_thread,
        });
        mod.addImport("antfly-zig", lib_mod);
        mod.addImport("antfly-client", antfly_client_pkg_mod);
        mod.addImport("httpx", httpx_mod);
        mod.addImport("antfly_vellum", vellum_mod);
        mod.addImport("raft_engine", raft_engine_mod);
        mod.addImport("structlog", structlog_mod);
        mod.addImport("antfly_platform", platform_mod);
        mod.addImport("handlebars", handlebars_mod);
        mod.addOptions("build_options", build_options);
        break :blk mod;
    } else blk: {
        const inference_cli_mod = b.createModule(.{
            .root_source_file = b.path("pkg/inference/src/main.zig"),
            .target = target,
            .optimize = optimize,
        });
        inference_cli_mod.addImport("inference", inference_server_mod);
        inference_cli_mod.addImport("build_options", inference_build_options_mod);
        inference_cli_mod.addImport("antfly_platform", platform_mod);
        inference_cli_mod.addImport("structlog", structlog_mod);

        const mod = b.createModule(.{
            .root_source_file = b.path("pkg/antfly/src/inference_main.zig"),
            .target = target,
            .optimize = optimize,
            .sanitize_thread = sanitize_thread,
        });
        mod.addImport("inference_cli", inference_cli_mod);
        mod.addImport("antfly_platform", platform_mod);
        mod.addImport("structlog", structlog_mod);
        mod.addOptions("build_options", build_options);
        break :blk mod;
    };

    if (strip) {
        var visited = std.AutoHashMap(*std.Build.Module, void).init(b.allocator);
        defer visited.deinit();
        setStripRecursively(antfly_main_mod, &visited);
    }
    const antfly_main = b.addExecutable(.{
        .name = "antfly",
        .root_module = antfly_main_mod,
    });
    const graph_metric_maintenance_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/cmd_graph_metric_maintenance_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    graph_metric_maintenance_test_mod.addImport("antfly-zig", lib_mod);
    graph_metric_maintenance_test_mod.addImport("antfly-client", antfly_client_pkg_mod);
    graph_metric_maintenance_test_mod.addImport("httpx", httpx_mod);
    graph_metric_maintenance_test_mod.addImport("antfly_vellum", vellum_mod);
    graph_metric_maintenance_test_mod.addImport("raft_engine", raft_engine_mod);
    graph_metric_maintenance_test_mod.addImport("structlog", structlog_mod);
    graph_metric_maintenance_test_mod.addImport("antfly_platform", platform_mod);
    graph_metric_maintenance_test_mod.addImport("handlebars", handlebars_mod);
    graph_metric_maintenance_test_mod.addOptions("build_options", build_options);
    const antfly_main_tests = b.addTest(.{
        .root_module = antfly_main_mod,
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_antfly_main_tests = b.addRunArtifact(antfly_main_tests);
    unit_test_step.dependOn(&run_antfly_main_tests.step);

    const graph_metric_operations_command_tests = b.addTest(.{
        .root_module = graph_metric_maintenance_test_mod,
        .filters = &antfly_tests_build.GraphMetricCommandTestFilters.operations,
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_graph_metric_operations_command_tests = b.addRunArtifact(graph_metric_operations_command_tests);
    const cli_test_step = b.step("cli-test", "Run Antfly command-line tests");
    cli_test_step.dependOn(&run_antfly_main_tests.step);
    cli_test_step.dependOn(&run_lite_cli_tests.step);
    cli_test_step.dependOn(&run_graph_metric_operations_command_tests.step);
    unit_test_step.dependOn(&run_graph_metric_operations_command_tests.step);

    if (edition == .full) {
        const graph_metric_process_harness_mod = b.createModule(.{
            .root_source_file = b.path("pkg/antfly/src/cmd/graph_metric_process_harness.zig"),
            .target = target,
            .optimize = optimize,
        });
        graph_metric_process_harness_mod.addImport("antfly-zig", lib_mod);
        graph_metric_process_harness_mod.addImport("antfly_platform", platform_mod);
        const graph_metric_process_harness = b.addExecutable(.{
            .name = "graph-metric-process-harness",
            .root_module = graph_metric_process_harness_mod,
        });
        graph_metric_process_harness.step.dependOn(&antfly_main.step);
        const run_graph_metric_process_harness = b.addRunArtifact(graph_metric_process_harness);
        run_graph_metric_process_harness.addArtifactArg(antfly_main);
        run_graph_metric_process_harness.addArgs(&.{ "--profile", "promotion" });
        run_graph_metric_process_harness.has_side_effects = true;
        promotion_test_step.dependOn(&run_graph_metric_process_harness.step);
    }

    const install_antfly = b.addInstallArtifact(antfly_main, .{ .dest_sub_path = antfly_bin_name });
    const install_antfarm_assets = b.addInstallDirectory(.{
        .source_dir = b.path("../go/pkg/antfly/src/metadata/antfarm"),
        .install_dir = .prefix,
        .install_subdir = "share/antfly/antfarm",
    });
    b.getInstallStep().dependOn(&install_antfly.step);
    if (edition == .full) {
        b.getInstallStep().dependOn(&install_antfarm_assets.step);
    }

    antfly_lite_build.addCliSteps(.{
        .b = b,
        .target = target,
        .wasm_target = wasm_target,
        .optimize = optimize,
        .lib_mod = lib_mod,
        .antfly_client_pkg_mod = antfly_client_pkg_mod,
        .httpx_mod = httpx_mod,
        .vellum_mod = vellum_mod,
        .raft_engine_mod = raft_engine_mod,
        .structlog_mod = structlog_mod,
        .platform_mod = platform_mod,
        .handlebars_mod = handlebars_mod,
        .build_options = build_options,
        .antfly_main = antfly_main,
        .antfly_bin_name = antfly_bin_name,
        .install_antfly = install_antfly,
        .lite_local_inference_runtime = lite_local_inference_runtime,
        .capi_steps = capi_steps,
        .run_antfly_main_tests = run_antfly_main_tests,
        .run_lite_cli_tests = run_lite_cli_tests,
        .run_lite_cmd_tests = main_capture_tests.lite_cmd,
        .run_lite_native_tests = run_lite_native_tests,
        .run_antfly_embedded_pkg_tests = run_antfly_embedded_pkg_tests,
    });

    const run_antfly = b.addRunArtifact(antfly_main);
    if (b.args) |args| {
        run_antfly.addArgs(args);
    }
    const antfly_step = b.step("antfly", "Run the top-level Antfly CLI");
    antfly_step.dependOn(&run_antfly.step);

    const run_recall_harness_default = b.addRunArtifact(bench_steps.recall_harness);
    run_recall_harness_default.stdio = .inherit;
    run_recall_harness_default.addArgs(&.{
        "--dataset-dir",
        "testdata/vectorsets",
    });
    dependOnAll(antfly_test_step, &.{
        unit_test_step,
        sim_test_step,
        integration_test_step,
        recall_test_step,
        &run_recall_harness_default.step,
        chaos_test_step,
    });

    dependOnAll(test_step, &.{
        antfly_test_step,
        delegated_inference_steps.inference_test,
    });

    antfly_tools_build.addToolSteps(.{
        .b = b,
        .target = target,
        .lib_mod = lib_mod,
    });
}
