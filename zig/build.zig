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
const audio_build = @import("lib/audio/build_support.zig");

const pdf_build = @import("lib/pdf/build_support.zig");
const image_build = @import("lib/image/build_support.zig");

const antfly_runtime_build = @import("pkg/antfly/build/runtime.zig");
const lib_sql_build_support = @import("lib/sql/build_support.zig");
const yacc_build = @import("lib/yacc/build_support.zig");
const tools_build = @import("tools/build_support.zig");

const pkg_antfly_build_codegen = @import("pkg/antfly/build/codegen.zig");
const addOpenApiRootCheckStep = pkg_antfly_build_codegen.addOpenApiRootCheckStep;
const addOpenApiSourceSteps = pkg_antfly_build_codegen.addOpenApiSourceSteps;

const pkg_antfly_build_runtime = @import("pkg/antfly/build/runtime.zig");
const RuntimeArtifactRole = pkg_antfly_build_runtime.RuntimeArtifactRole;
const RuntimeLibraryUnit = pkg_antfly_build_runtime.RuntimeLibraryUnit;

const lib_platform_build_support = @import("lib/platform/build_support.zig");
const addMacosSdkPaths = lib_platform_build_support.addMacosSdkPaths;

const pkg_antfly_build_tests = @import("pkg/antfly/build/tests.zig");

const addRuntimeTestFilters = pkg_antfly_build_tests.addRuntimeTestFilters;
const addFilteredTestRunArtifact = pkg_antfly_build_tests.addFilteredTestRunArtifact;
const dependOnAll = pkg_antfly_build_tests.dependOnAll;
const assignDefaultAggregateMaxRss = pkg_antfly_build_tests.assignDefaultAggregateMaxRss;

const pkg_antfly_build_imports = @import("pkg/antfly/build/imports.zig");
const AntflyRootImports = pkg_antfly_build_imports.AntflyRootImports;

const pkg_antfly_build_snowball = @import("pkg/antfly/build/snowball.zig");

const builtin = @import("builtin");
const antfly_benches_build = @import("pkg/antfly/build/benches.zig");
const antfly_embedded_build = @import("pkg/antfly/build/embedded.zig");
const antfly_storage_build = @import("pkg/antfly/build/storage.zig");
const antfly_tests_build = @import("pkg/antfly/build/tests.zig");
const inference_runtime_build = @import("pkg/inference/build/runtime.zig");
const platform_build = @import("lib/platform/build_support.zig");

const LmdbBackend = antfly_storage_build.LmdbBackend;
const makeLmdbBuildOptions = antfly_storage_build.makeLmdbBuildOptions;
const makeLmdbEngineModule = antfly_storage_build.makeLmdbEngineModule;
const makeRootBuildOptions = antfly_storage_build.makeRootBuildOptions;
const selectTestFilters = antfly_tests_build.selectTestFilters;

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

pub fn build(b: *std.Build) void {
    _ = create(b);
}

pub const Artifacts = struct {
    runtime: antfly_runtime_build.AddRuntimeResult,
    inference: inference_runtime_build.Graph,
    wasm: *std.Build.Step.Compile,
};

/// Compose owners once. Consumers of this constructor can inspect the same
/// artifacts used by public targets without maintaining a second build graph.
pub fn create(b: *std.Build) ?Artifacts {
    const api_bench_standalone = b.option(bool, "api-bench-standalone", "Build only the API benchmark for an existing server process") orelse false;
    const conformance_fetch = b.option(bool, "conformance-fetch", "Fetch missing external conformance fixtures") orelse true;
    const conformance_fixtures = b.option([]const u8, "conformance-fixtures", "Cache directory for external conformance fixtures") orelse "/tmp";
    // On Linux, an implicit native target can cause Zig 0.16.0 to discover and
    // link against the host distro's crt startup objects. Newer glibc/binutils
    // builds may include .sframe sections with relocation types that Zig's
    // linker cannot yet handle. Defaulting Linux builds to an explicit GNU
    // target keeps user-supplied -Dtarget overrides intact while making the
    // no-argument path use Zig's bundled libc startup objects.
    const default_target: std.Target.Query = if (builtin.os.tag == .linux)
        .{
            .cpu_arch = builtin.cpu.arch,
            .os_tag = .linux,
            .abi = .gnu,
        }
    else
        .{};
    const target = b.standardTargetOptions(.{ .default_target = default_target });
    const optimize = b.standardOptimizeOption(.{});
    const vopr_dep = b.dependency("vopr", .{ .target = target, .optimize = optimize });
    const vopr_mod = vopr_dep.module("vopr");
    const strip = b.option(bool, "strip", "Omit debug information from release artifacts") orelse false;
    const lmdb_backend = b.option(LmdbBackend, "lmdb_backend", "Select the LMDB backend scaffold (c or zig)") orelse .zig;
    const lmdb_evented_async_io = b.option(bool, "lmdb_evented_async_io", "Use std.Io.Evented for the Zig LMDB async_io backend") orelse false;
    const with_tla = b.option(bool, "with_tla", "Enable TLA+ trace instrumentation (ndjson event logging)") orelse false;
    const link_libc = b.option(bool, "link-libc", "Link Antfly runtime modules against libc") orelse true;
    const sanitize_thread = b.option(bool, "sanitize-thread", "Enable ThreadSanitizer for the Antfly runtime") orelse false;
    const runtime_artifact_role = b.option(RuntimeArtifactRole, "runtime-artifact-role", "Build one focused runtime artifact: cli, data, inference, metadata, or standalone");
    const antfly_bin_name = b.option([]const u8, "antfly-bin-name", "Installed filename for the top-level Antfly CLI") orelse "antfly";
    if (antfly_bin_name.len == 0 or std.mem.indexOfAny(u8, antfly_bin_name, "/\\") != null) {
        @panic("-Dantfly-bin-name must be a non-empty filename, not a path");
    }
    if (!link_libc and lmdb_backend == .c) {
        @panic("-Dlink-libc=false requires -Dlmdb_backend=zig");
    }
    const inference_onnx_option = b.option(bool, "onnx", "Enable ONNX Runtime support for embedded inference");
    const inference_enable_onnx = if (link_libc)
        inference_onnx_option orelse false
    else
        false;
    const inference_onnx_root_opt = b.option([]const u8, "onnx-root", "Path to ONNX Runtime root for embedded inference");
    const inference_onnx_root = inference_onnx_root_opt orelse defaultInferenceOnnxRoot(b, target);
    const inference_enable_metal = if (link_libc)
        b.option(bool, "metal", "Enable Apple Metal kernels for embedded inference") orelse (target.result.os.tag == .macos)
    else
        false;
    const inference_enable_cuda = b.option(bool, "cuda", "Enable CUDA inference support through the NVIDIA Driver API") orelse false;
    const inference_cuda_artifacts = b.option([]const u8, "cuda-artifacts", "CUDA artifact bundle: fatbin SASS+PTX, portable PTX, or sm89 cubin") orelse "fatbin";
    if (!std.mem.eql(u8, inference_cuda_artifacts, "portable") and !std.mem.eql(u8, inference_cuda_artifacts, "fatbin") and !std.mem.eql(u8, inference_cuda_artifacts, "sm89")) {
        @panic("invalid -Dcuda-artifacts (expected portable, fatbin, or sm89)");
    }
    const inference_enable_pjrt = if (link_libc)
        b.option(bool, "pjrt", "Enable PJRT inference support through runtime-loaded plugins") orelse false
    else
        false;
    const inference_blas_root_opt = b.option([]const u8, "blas-root", "Path to system BLAS root with include/ and lib/ for non-macOS native acceleration");
    const inference_system_blas_available = link_libc and (target.result.os.tag == .macos or inference_blas_root_opt != null);
    const inference_enable_system_blas = if (link_libc)
        b.option(bool, "system-blas", "Enable system BLAS acceleration for native CPU math") orelse inference_system_blas_available
    else
        false;
    const inference_blas_root = if (inference_enable_system_blas and target.result.os.tag != .macos)
        inference_blas_root_opt
    else
        null;
    const antfly_version = b.option([]const u8, "antfly-version", "Antfly version string") orelse "dev";
    const build_info = @import("lib/build_info/build_support.zig").create(b, .{
        .root = b.path("lib/build_info"),
        .target = target,
        .optimize = optimize,
        .version = antfly_version,
    });
    const lite_local_inference_runtime = b.option(bool, "lite-local-inference-runtime", "Advertise an embedded local inference runtime in Antfly Lite status") orelse false;
    const platform_tests = platform_build.addTests(b, .{
        .root = b.path("lib/platform"),
        .target = target,
        .optimize = optimize,
        .link_libc = link_libc,
    });

    const platform_test_step = b.step("lib-platform-test", "Run supervisor unit and process-lifecycle tests (Python 3 on POSIX)");
    platform_test_step.dependOn(&platform_tests.unit.step);
    if (platform_tests.process) |process| platform_test_step.dependOn(process);

    const lmdb_build_options = makeLmdbBuildOptions(b, lmdb_backend, lmdb_evented_async_io, false);
    const build_options = makeRootBuildOptions(b, lmdb_backend, lmdb_evented_async_io, false, with_tla, link_libc, false, true);
    const standalone_runtime_build_options = makeRootBuildOptions(b, lmdb_backend, lmdb_evented_async_io, false, with_tla, link_libc, true, true);
    const production_build_options = makeRootBuildOptions(b, lmdb_backend, lmdb_evented_async_io, false, with_tla, link_libc, false, false);
    const lmdb_engine_mod = makeLmdbEngineModule(b, target, optimize, link_libc, lmdb_build_options);
    const raft_engine_mod = b.createModule(.{
        .root_source_file = b.path("lib/raft/src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    const json_mod = b.addModule("antfly-json", .{
        .root_source_file = b.path("lib/json/src/mod.zig"),
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
    const snowball_steps = pkg_antfly_build_snowball.addSteps(b);
    b.step("regen-snowball", "Regenerate checked-in Zig Snowball stemmers").dependOn(&snowball_steps.regen.step);
    b.step("check-snowball", "Check checked-in Zig Snowball stemmers are current").dependOn(&snowball_steps.compare.step);
    const openapi_build = b.lazyImport(@This(), "openapi") orelse return null;
    const openapi_codegen = openapi_build.addCompiler(b, b.path("lib/openapi"), b.graph.host, .ReleaseSafe);
    const openapi_sources = addOpenApiSourceSteps(b, openapi_build, openapi_codegen);
    const update_public_openapi = b.addUpdateSourceFiles();
    update_public_openapi.addCopyFileToSource(openapi_sources.public_spec, "../openapi.yaml");
    const openapi_regen_step = b.step("regen-openapi", "Regenerate checked-in OpenAPI sources");
    openapi_regen_step.dependOn(&openapi_sources.regen.step);
    openapi_regen_step.dependOn(&update_public_openapi.step);
    const openapi_check_step = b.step("check-openapi", "Compare checked-in OpenAPI sources without modifying them");
    openapi_check_step.dependOn(&openapi_sources.check.step);
    const yacc_codegen = yacc_build.addCompiler(b, b.path("lib/yacc"), target, optimize);
    b.step("yacc-zig", "Build and install the standalone Zig yacc generator").dependOn(&b.addInstallArtifact(yacc_codegen, .{}).step);
    const yacc_tests = b.addTest(.{ .root_module = b.createModule(.{
        .root_source_file = b.path("lib/yacc/src/root.zig"),
        .target = target,
        .optimize = optimize,
    }) });
    const run_yacc_tests = b.addRunArtifact(yacc_tests);
    b.step("lib-yacc-test", "Run standalone lib/yacc parser generator tests").dependOn(&run_yacc_tests.step);
    const yacc_steps = lib_sql_build_support.addSteps(b, .{
        .root = b.path("lib/sql"),
        .target = target,
        .optimize = optimize,
        .codegen = yacc_build.addCompiler(b, b.path("lib/yacc"), b.graph.host, .ReleaseSafe),
        .compare_tool = tools_build.addFileCompareTool(b, b.path("tools")),
        .grammar_label = "lib/sql/grammar/antfly_sql.y",
    });
    b.step("regen-sql-grammar", "Regenerate checked-in Antfly SQL grammar metadata").dependOn(&yacc_steps.regen.step);
    const sql_generated_check = b.step("sql-grammar-generated-check", "Check and compile the generated Antfly SQL grammar metadata");
    sql_generated_check.dependOn(&yacc_steps.compare.step);
    sql_generated_check.dependOn(&yacc_steps.run_generated.step);
    b.step("lib-sql-parser-test", "Run the storage-independent SQL lexer and parser tests").dependOn(&yacc_steps.run_parser_tests.step);
    b.step("lib-sql-parser-bench", "Build and install lib-sql-parser-bench").dependOn(&b.addInstallArtifact(yacc_steps.benchmark, .{}).step);
    const openapi_root_check = addOpenApiRootCheckStep(b);
    openapi_check_step.dependOn(&openapi_root_check.step);
    const openapi_modules = pkg_antfly_build_codegen.createCommittedModules(b, .{
        .root = b.path("pkg/antfly/src/openapi/generated"),
        .target = target,
        .optimize = optimize,
        .httpx = httpx_mod,
        .json = json_mod,
        .export_modules = true,
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
    const openai_api_mod = openapi_modules.openai_api;

    // Handlebars template engine
    const handlebars_dep = b.dependency("handlebars", .{ .target = target, .optimize = optimize });
    const handlebars_mod = handlebars_dep.module("handlebars");

    // Protobuf wire format
    const protobuf_dep = b.dependency("protobuf", .{ .target = target, .optimize = optimize });
    const protobuf_mod = protobuf_dep.module("protobuf");
    const platform_mod = platform_build.createModule(b, .{
        .root_source_file = b.path("lib/platform/src/root.zig"),
        .filesystem_capacity_source_file = b.path("lib/platform/src/filesystem_capacity.c"),
        .target = target,
        .optimize = optimize,
        .link_libc = link_libc,
    });
    const objectstore_mod = b.createModule(.{
        .root_source_file = b.path("lib/objectstore/src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    const credentials_mod = b.createModule(.{
        .root_source_file = b.path("lib/credentials/src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    const google_mod = b.createModule(.{
        .root_source_file = b.path("lib/google/src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    google_mod.addImport("httpx", httpx_mod);
    google_mod.addImport("antfly_credentials", credentials_mod);
    google_mod.addImport("antfly_platform", platform_mod);
    objectstore_mod.addImport("httpx", httpx_mod);
    objectstore_mod.addImport("antfly_platform", platform_mod);
    objectstore_mod.addImport("antfly_google", google_mod);
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
    const hash_mod = b.createModule(.{
        .root_source_file = b.path("lib/hash/src/mod.zig"),
        .target = target,
        .optimize = optimize,
    });
    // Isolated image/hash benchmarks own a fixed ReleaseFast profile.
    const hash_bench_mod = if (optimize == .ReleaseFast) hash_mod else b.createModule(.{
        .root_source_file = b.path("lib/hash/src/mod.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    const vectorindex_mod = b.createModule(.{
        .root_source_file = b.path("lib/vectorindex/src/mod.zig"),
        .target = target,
        .optimize = optimize,
    });
    vectorindex_mod.addImport("antfly_vector", vector_mod);
    vectorindex_mod.addImport("antfly_platform", platform_mod);
    vectorindex_mod.addImport("antfly_hash", hash_mod);
    if (target.result.os.tag == .macos) {
        addMacosSdkPaths(b, vectorindex_mod, target);
        vectorindex_mod.linkFramework("Foundation", .{});
        vectorindex_mod.linkFramework("Metal", .{});
        vectorindex_mod.addCSourceFile(.{ .file = b.path("lib/vectorindex/src/kmeans_metal.m"), .flags = &.{"-fobjc-arc"} });
    }
    const casbin_mod = b.createModule(.{
        .root_source_file = b.path("lib/casbin/src/mod.zig"),
        .target = target,
        .optimize = optimize,
    });
    const storage_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/storage_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    storage_mod.addImport("bloom", bloom_mod);
    storage_mod.addImport("antfly_platform", platform_mod);
    storage_mod.addImport("antfly_hash", hash_mod);
    const usermgr_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/usermgr_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    usermgr_mod.link_libc = link_libc;
    usermgr_mod.addImport("antfly_casbin", casbin_mod);
    usermgr_mod.addImport("bloom", bloom_mod);
    usermgr_mod.addImport("antfly_platform", platform_mod);
    usermgr_mod.addImport("antfly_hash", hash_mod);
    const usermgr_test_storage_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/usermgr/storage_imports.zig"),
        .target = target,
        .optimize = optimize,
    });
    usermgr_test_storage_mod.addImport("antfly_root", usermgr_mod);
    usermgr_test_storage_mod.addImport("antfly_platform", platform_mod);
    usermgr_mod.addImport("usermgr_storage", usermgr_test_storage_mod);
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
    mcp_mod.addImport("antfly-json", json_mod);
    const a2a_mod = b.addModule("antfly_a2a", .{
        .root_source_file = b.path("lib/a2a/src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    a2a_mod.addImport("antfly-json", json_mod);
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
    scraping_mod.addImport("httpx", httpx_mod);
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
    const image_mod = image_build.createModule(b, b.path("lib/image"), target, optimize, hash_mod);
    const pdf_standard_fonts_mod = b.createModule(.{
        .root_source_file = b.path("pdf_standard_fonts.zig"),
        .target = target,
        .optimize = optimize,
    });
    const font_mod = b.createModule(.{
        .root_source_file = b.path("lib/font/src/mod.zig"),
        .target = target,
        .optimize = optimize,
    });
    const pdf_mod = pdf_build.createModule(b, b.path("lib/pdf"), target, optimize, image_mod, hash_mod, font_mod, pdf_standard_fonts_mod);

    const tokenizer_build = @import("lib/tokenizer/build_support.zig");
    const sentencepiece_proto_source = tokenizer_build.generateSentencePieceProto(b, protobuf_dep.artifact("protoc-zig"), b.path("lib/tokenizer"));
    const sentencepiece_proto_mod = tokenizer_build.createSentencePieceProtoModule(b, sentencepiece_proto_source, protobuf_mod);
    const inference_jinja_mod = b.createModule(.{
        .root_source_file = b.path("lib/jinja/src/jinja.zig"),
        .target = target,
        .optimize = optimize,
    });
    const inference_ml_mod = b.createModule(.{
        .root_source_file = b.path("lib/ml/src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    inference_ml_mod.addImport("antfly_platform", platform_mod);
    const ml_tabular_mod = b.addModule("ml_tabular", .{
        .root_source_file = b.path("lib/ml/tabular/src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    const onnx_build = @import("onnx_graph").support;
    const inference_onnx = onnx_build.create(b, .{
        .root = b.path("lib/onnx"),
        .target = target,
        .optimize = optimize,
        .protobuf = protobuf_mod,
        .ml = inference_ml_mod,
    });
    b.modules.put(b.allocator, b.dupe("inference_onnx_graph"), inference_onnx.graph) catch @panic("OOM");
    const inference_pjrt_xla_proto_mod = b.createModule(.{
        .root_source_file = b.path("lib/pjrt/proto/xla_proto_stub.zig"),
        .target = target,
        .optimize = optimize,
    });
    inference_pjrt_xla_proto_mod.addImport("protobuf", protobuf_mod);
    const inference_pjrt_mod = b.createModule(.{
        .root_source_file = b.path("lib/pjrt/src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    inference_pjrt_mod.addImport("protobuf", protobuf_mod);
    inference_pjrt_mod.addImport("xla_proto", inference_pjrt_xla_proto_mod);

    const tokenizer = @import("lib/tokenizer/build_support.zig").create(b, .{
        .root = b.path("lib/tokenizer"),
        .target = target,
        .optimize = optimize,
        .protobuf = protobuf_mod,
        .sentencepiece_proto = sentencepiece_proto_mod,
    });

    const inference_api_source = inference_runtime_build.addInferenceApiOverride(b, openapi_build, b.path("../scripts"), openapi_codegen);
    const inference_config: inference_runtime_build.Config = .{
        .b = b,
        .target = target,
        .optimize = optimize,
        .paths = .{
            .inference_root = "pkg/inference",
            .shared_lib_root = "",
        },
        .backend = .{
            .enable_onnx = inference_enable_onnx,
            .onnx_root = inference_onnx_root,
            .enable_metal = inference_enable_metal,
            .enable_cuda = inference_enable_cuda,
            .cuda_artifacts = inference_cuda_artifacts,
            .enable_pjrt = inference_enable_pjrt,
            .enable_native = true,
            .wasm_memory_model = b.option([]const u8, "wasm-memory-model", "Inference WASM memory model: wasm32 or wasm64") orelse "wasm32",
            .enable_webgpu = b.option(bool, "webgpu", "Enable WebGPU for inference WASM") orelse false,
            .enable_system_blas = inference_enable_system_blas,
            .blas_root = inference_blas_root,
            .link_libc = link_libc,
            .skip_openapi = false,
        },
        .shared = .{
            .build_info_mod = build_info.module,
            .build_info_object = build_info.object,
            .tokenizer_mod = tokenizer.tokenizer,
            .hf_tokenizer_mod = tokenizer.huggingface,
            .fixed_tokenizer_data_mod = tokenizer.fixed_data,
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
            .hash = hash_mod,
            .prometheus = prometheus_mod,
            .structlog = structlog_mod,
            .jinja = inference_jinja_mod,
            .inference_api_source = inference_api_source,
            .protobuf = protobuf_mod,
            .sentencepiece_proto = sentencepiece_proto_mod,
            .ml = inference_ml_mod,
            .ml_tabular = ml_tabular_mod,
            .onnx = inference_onnx,
            .pjrt = inference_pjrt_mod,
            .audio_openapi = audio_openapi_mod,
            .s3_openapi = s3_openapi_mod,
            .generating_openapi = generating_openapi_mod,
            .extraction_openapi = extraction_openapi_mod,
            .extracting = extracting_mod,
        },
    };
    const inference_graph = inference_runtime_build.create(inference_config);
    const inference_api_mod = inference_graph.inference_api_mod;
    inference_api_mod.addImport("antfly_generating_openapi", generating_openapi_mod);
    inference_api_mod.addImport("antfly_chunking_api_openapi", chunking_api_openapi_mod);
    inference_api_mod.addImport("antfly_extraction_openapi", extraction_openapi_mod);
    const inference_hf_tokenizer_mod = inference_graph.inference_hf_tokenizer_mod;
    const inference_fixed_tokenizer_data_mod = inference_graph.inference_fixed_tokenizer_data_mod;
    const inference_chunker_mod = inference_graph.inference_chunker_mod;
    const inference_server_mod = inference_graph.inference_mod;
    const hf_tokenizer_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("lib/tokenizer/src/hf_tokenizer.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
        }),
    });
    hf_tokenizer_tests.root_module.addImport(
        "sentencepiece_proto",
        sentencepiece_proto_mod,
    );
    const run_hf_tokenizer_tests = b.addRunArtifact(hf_tokenizer_tests);
    const hf_tokenizer_test_step = b.step(
        "lib-tokenizer-test",
        "Run Hugging Face tokenizer tests",
    );
    hf_tokenizer_test_step.dependOn(&run_hf_tokenizer_tests.step);

    const transcribing_mod = inference_graph.transcribing_mod;
    const reader_config_mod = inference_graph.reader_config_mod;
    const readers_mod = b.createModule(.{
        .root_source_file = b.path("lib/readers/src/mod.zig"),
        .target = target,
        .optimize = optimize,
    });
    readers_mod.addImport("httpx", httpx_mod);
    readers_mod.addImport("inference_api", inference_api_mod);
    readers_mod.addImport("antfly_google", google_mod);
    readers_mod.addImport("antfly_reader_config", reader_config_mod);
    readers_mod.addImport("antfly_scraping", scraping_mod);
    readers_mod.addImport("antfly_image", image_mod);
    inference_server_mod.addImport("antfly_readers", readers_mod);
    inference_server_mod.addImport("antfly_reader_config", reader_config_mod);
    inference_server_mod.addImport("antfly_extracting", extracting_mod);
    const synthesizing_mod = b.createModule(.{
        .root_source_file = b.path("lib/synthesizing/src/mod.zig"),
        .target = target,
        .optimize = optimize,
    });
    synthesizing_mod.addImport("antfly_audio_openapi", audio_openapi_mod);
    synthesizing_mod.addImport("httpx", httpx_mod);

    const inference_workflow = @import("pkg/inference/build/context.zig").Context{
        .b = b,
        .target = target,
        .optimize = optimize,
        .paths = inference_config.paths,
        .backend = inference_config.backend,
        .graph = inference_graph,
        .args = b.args,
        .step_prefix = "inference-",
        .add_native_process_test = platform_build.addNativeProcessTest,
        .runtime_test_filter = b.option(bool, "runtime-test-filter", "Build inference tests once and filter them at runtime") orelse false,
    };
    const inference_wasm_target = @import("pkg/inference/build/wasm.zig").resolveTarget(inference_workflow);
    const inference_wasm_jinja = b.createModule(.{
        .root_source_file = b.path("lib/jinja/src/jinja.zig"),
        .target = inference_wasm_target,
        .optimize = .ReleaseSafe,
    });
    const inference_wasm_platform = platform_build.createModule(b, .{
        .root_source_file = b.path("lib/platform/src/root.zig"),
        .filesystem_capacity_source_file = b.path("lib/platform/src/filesystem_capacity.c"),
        .target = inference_wasm_target,
        .optimize = .ReleaseSafe,
        .link_libc = false,
    });
    const inference_steps = @import("pkg/inference/build/integration.zig").add(inference_workflow, inference_wasm_jinja, inference_wasm_platform);

    const antfly_imports = AntflyRootImports{
        .build_info = build_info,
        .build_options = build_options,
        .lite_options = antfly_storage_build.createLiteOptions(b, lite_local_inference_runtime),
        .embedded_openapi = pkg_antfly_build_codegen.addEmbeddedSpecs(b, .{
            .root_source_file = b.path("pkg/antfly/src/openapi/embedded_specs.zig"),
            .schema_root = b.path("../specs/openapi"),
            .public_spec = b.path("../openapi.yaml"),
        }),
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
        .reader_config = reader_config_mod,
        .readers = readers_mod,
        .extracting = extracting_mod,
        .synthesizing = synthesizing_mod,
        .httpx = httpx_mod,
        .credentials = credentials_mod,
        .google = google_mod,
        .objectstore = objectstore_mod,
        .bloom = bloom_mod,
        .vector = vector_mod,
        .vectorindex = vectorindex_mod,
        .hash = hash_mod,
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
    var production_antfly_imports = antfly_imports;
    production_antfly_imports.build_options = production_build_options;

    // Library module
    const antfly_mod = b.addModule("antfly-zig", .{
        .root_source_file = b.path("pkg/antfly/src/root.zig"),
        .target = target,
        .optimize = optimize,
        .sanitize_thread = sanitize_thread,
    });
    // The full package exports simulation APIs as well as its runtime surface.
    antfly_imports.configure(b, antfly_mod, link_libc);
    antfly_storage_build.configureLmdb(b, antfly_mod, lmdb_engine_mod, false);
    antfly_mod.addImport("vopr", vopr_mod);
    antfly_mod.addImport("antfly_openapi_specs", antfly_imports.embedded_openapi);

    const wasm = @import("pkg/antfly/build/wasm.zig").add(b, sentencepiece_proto_source);
    const wasm_step = b.step("wasm", "Build and install the unified Antfly WASM bundle");
    dependOnAll(wasm_step, wasm.install);
    wasm.smoke.step.dependOn(wasm_step);
    b.step("wasm-test", "Build the Antfly WASM bundle and run its Node smoke test").dependOn(&wasm.smoke.step);
    const embedded = antfly_embedded_build.addEmbedded(b, .{
        .lmdb_engine = lmdb_engine_mod,
        .vopr = vopr_mod,
        .optimize = optimize,
        .strip = strip,
        .antfly_imports = antfly_imports,
        .antfly_mod = antfly_mod,
    });
    const embedded_mod = embedded.embedded_mod;
    const embedded_api_mod = embedded.embedded_api_mod;
    const antfly_embedded_pkg_mod = embedded.antfly_embedded_pkg_mod;
    const antfly_embedded_db_pkg_mod = embedded.antfly_embedded_db_pkg_mod;
    const antfly_embedded_api_pkg_mod = embedded.antfly_embedded_api_pkg_mod;
    const antfly_client_pkg_mod = embedded.antfly_client_pkg_mod;
    const capi_mod = embedded.capi_mod;
    const libantfly_link_mod = embedded.libantfly_link_mod;
    const install_libantfly = embedded.install_libantfly;
    const install_capi_header = embedded.install_capi_header;
    const run_capi_smoke = embedded.run_capi_smoke;
    const run_lite_go_tests = embedded.run_lite_go_tests;
    const run_lite_go_example = embedded.run_lite_go_example;
    const run_lite_go_retrieval_template = embedded.run_lite_go_retrieval_template;
    const run_cabi_packaging_tests = embedded.run_cabi_packaging_tests;
    const run_capi_tests = embedded.run_capi_tests;

    const lib_regex_tests = b.addTest(.{
        .root_module = regex_mod,
    });
    const run_lib_regex_tests = b.addRunArtifact(lib_regex_tests);
    const lib_regex_test_step = b.step("lib-regex-test", "Run standalone lib/regex tests");
    lib_regex_test_step.dependOn(&run_lib_regex_tests.step);

    const lib_scraping_tests = b.addTest(.{
        .root_module = scraping_mod,
    });
    const run_lib_scraping_tests = b.addRunArtifact(lib_scraping_tests);
    const lib_scraping_test_step = b.step("lib-scraping-test", "Run standalone lib/scraping tests");
    lib_scraping_test_step.dependOn(&run_lib_scraping_tests.step);

    const lib_jsonschema_tests = b.addTest(.{
        .root_module = jsonschema_mod,
    });
    const run_lib_jsonschema_tests = b.addRunArtifact(lib_jsonschema_tests);
    const lib_jsonschema_test_step = b.step("lib-jsonschema-test", "Run standalone lib/jsonschema tests");
    lib_jsonschema_test_step.dependOn(&run_lib_jsonschema_tests.step);

    const lib_json_tests = b.addTest(.{
        .root_module = json_mod,
    });
    const run_lib_json_tests = b.addRunArtifact(lib_json_tests);
    const lib_json_test_step = b.step("lib-json-test", "Run standalone lib/json tests");
    lib_json_test_step.dependOn(&run_lib_json_tests.step);

    const lib_ml_tabular_tests = b.addTest(.{
        .root_module = ml_tabular_mod,
    });
    const run_lib_ml_tabular_tests = b.addRunArtifact(lib_ml_tabular_tests);
    const lib_ml_tabular_test_step = b.step("lib-ml-tabular-test", "Run standalone lib/ml/tabular tests");
    lib_ml_tabular_test_step.dependOn(&run_lib_ml_tabular_tests.step);

    const onnx_tests = onnx_build.createTests(b, inference_onnx, .{
        .path = b.path("pkg/antfly/src/test_runner.zig"),
        .mode = .simple,
    });
    onnx_tests.graph.setEnvironmentVariable("ANTFLY_TEST_FAIL_ON_ERROR_LOGS", "0");
    const lib_onnx_test_step = b.step("lib-onnx-test", "Run standalone lib/onnx tests");
    lib_onnx_test_step.dependOn(&onnx_tests.data.step);
    lib_onnx_test_step.dependOn(&onnx_tests.graph.step);

    const fuzz_tabular_loader = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("lib/ml/tabular/src/fuzz_loader.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_fuzz_tabular_loader = b.addRunArtifact(fuzz_tabular_loader);
    const fuzz_tabular_loader_step = b.step("lib-ml-tabular-fuzz-test", "Fuzz the tabular_model.json loader (--fuzz to keep running)");
    fuzz_tabular_loader_step.dependOn(&run_fuzz_tabular_loader.step);

    const lib_toon_tests = b.addTest(.{
        .root_module = toon_mod,
    });
    const run_lib_toon_tests = b.addRunArtifact(lib_toon_tests);
    const lib_toon_test_step = b.step("lib-toon-test", "Run standalone lib/toon tests");
    lib_toon_test_step.dependOn(&run_lib_toon_tests.step);

    const lib_mcp_tests = b.addTest(.{
        .root_module = mcp_mod,
    });
    const run_lib_mcp_tests = b.addRunArtifact(lib_mcp_tests);
    const lib_mcp_test_step = b.step("lib-mcp-test", "Run standalone lib/mcp tests");
    lib_mcp_test_step.dependOn(&run_lib_mcp_tests.step);

    const lib_a2a_tests = b.addTest(.{
        .root_module = a2a_mod,
    });
    const run_lib_a2a_tests = b.addRunArtifact(lib_a2a_tests);
    const lib_a2a_test_step = b.step("lib-a2a-test", "Run standalone lib/a2a tests");
    lib_a2a_test_step.dependOn(&run_lib_a2a_tests.step);

    const lib_matcher_tests = b.addTest(.{
        .root_module = matcher_mod,
    });
    const run_lib_matcher_tests = b.addRunArtifact(lib_matcher_tests);
    const lib_matcher_test_step = b.step("lib-matcher-test", "Run standalone lib/matcher tests");
    lib_matcher_test_step.dependOn(&run_lib_matcher_tests.step);

    const lib_resolver_tests = b.addTest(.{
        .root_module = resolver_mod,
    });
    const run_lib_resolver_tests = b.addRunArtifact(lib_resolver_tests);
    const lib_resolver_test_step = b.step("lib-resolver-test", "Run standalone lib/resolver tests");
    lib_resolver_test_step.dependOn(&run_lib_resolver_tests.step);

    const lib_toon_conformance = b.addExecutable(.{
        .name = "lib-toon-conformance",
        .root_module = b.createModule(.{
            .root_source_file = b.path("lib/toon/toon_conformance.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    lib_toon_conformance.root_module.addImport("antfly_toon", toon_mod);

    const run_lib_toon_conformance = b.addRunArtifact(lib_toon_conformance);
    run_lib_toon_conformance.addArgs(&.{ "run", b.pathJoin(&.{ conformance_fixtures, "toon-format-spec" }) });
    if (!conformance_fetch) run_lib_toon_conformance.addArg("--no-fetch");
    const lib_toon_conformance_step = b.step("lib-toon-conformance", "Run lib/toon conformance (fetch missing fixtures)");
    lib_toon_conformance_step.dependOn(&run_lib_toon_conformance.step);

    const httpx_json_test_mod = b.createModule(.{
        .root_source_file = b.path("lib/httpx/src/util/json.zig"),
        .target = target,
        .optimize = optimize,
    });
    httpx_json_test_mod.addImport("antfly-json", json_mod);
    const httpx_json_tests = b.addTest(.{
        .root_module = httpx_json_test_mod,
    });
    const run_httpx_json_tests = b.addRunArtifact(httpx_json_tests);
    const lib_httpx_json_test_step = b.step("lib-httpx-json-test", "Run standalone lib/httpx JSON helper tests");
    lib_httpx_json_test_step.dependOn(&run_httpx_json_tests.step);

    const httpx_tests = b.addTest(.{
        .root_module = httpx_mod,
        .filters = selectTestFilters(b, &.{}),
    });
    const run_httpx_tests = b.addRunArtifact(httpx_tests);
    const lib_httpx_test_step = b.step("lib-httpx-test", "Run standalone lib/httpx tests");
    lib_httpx_test_step.dependOn(&run_httpx_tests.step);

    const httpx_client_lifecycle_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("lib/httpx/src/client_test_root.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
        }),
        .filters = &.{ "request gate", "request watchdog" },
    });
    const run_httpx_client_lifecycle_tests = b.addRunArtifact(httpx_client_lifecycle_tests);
    b.step("lib-httpx-client-lifecycle-test", "Run HTTP client admission, release, and shutdown contracts").dependOn(&run_httpx_client_lifecycle_tests.step);
    lib_httpx_test_step.dependOn(&run_httpx_client_lifecycle_tests.step);

    const objectstore_tests = b.addTest(.{
        .root_module = objectstore_mod,
        .filters = selectTestFilters(b, &.{}),
    });
    const run_objectstore_tests = b.addRunArtifact(objectstore_tests);
    const lib_objectstore_test_step = b.step("lib-objectstore-test", "Run standalone lib/objectstore tests");
    lib_objectstore_test_step.dependOn(&run_objectstore_tests.step);

    const httpx_transport_regression_tests = b.addTest(.{
        .root_module = httpx_mod,
        .filters = &.{
            "H2 response serialization strips connection-specific headers",
            "HTTP streaming headers and automatic preflight preserve middleware policy",
        },
    });
    const run_httpx_transport_regression_tests = b.addRunArtifact(httpx_transport_regression_tests);

    const lib_generating_tests = b.addTest(.{
        .root_module = generating_mod,
    });
    const run_lib_generating_tests = b.addRunArtifact(lib_generating_tests);
    const lib_generating_test_step = b.step("lib-generating-test", "Run standalone lib/generating tests");
    lib_generating_test_step.dependOn(&run_lib_generating_tests.step);

    const lib_embeddings_tests = b.addTest(.{
        .root_module = embeddings_mod,
    });
    const run_lib_embeddings_tests = b.addRunArtifact(lib_embeddings_tests);
    const lib_embeddings_test_step = b.step("lib-embeddings-test", "Run standalone lib/embeddings tests");
    lib_embeddings_test_step.dependOn(&run_lib_embeddings_tests.step);

    const lib_hash_tests = b.addTest(.{
        .root_module = hash_mod,
        .filters = b.args orelse &.{},
    });
    const run_lib_hash_tests = b.addRunArtifact(lib_hash_tests);
    const lib_hash_test_step = b.step("lib-hash-test", "Run standalone lib/hash tests");
    lib_hash_test_step.dependOn(&run_lib_hash_tests.step);

    const lib_vectorindex_tests = b.addTest(.{
        .root_module = vectorindex_mod,
        .filters = b.args orelse &.{},
    });
    const run_lib_vectorindex_tests = b.addRunArtifact(lib_vectorindex_tests);
    const lib_vectorindex_test_step = b.step("lib-vectorindex-test", "Run standalone lib/vectorindex tests");
    lib_vectorindex_test_step.dependOn(&run_lib_vectorindex_tests.step);

    const vector_kernel_mod = b.createModule(.{ .root_source_file = b.path("lib/vector/src/quantizer.zig"), .target = target, .optimize = optimize });
    vector_kernel_mod.addImport("protobuf", protobuf_mod);
    const vector_kernel_tests = b.addTest(.{ .root_module = vector_kernel_mod, .filters = b.args orelse &.{} });
    const run_vector_kernel_tests = b.addRunArtifact(vector_kernel_tests);
    b.step("lib-vector-kernel-test", "Run standalone quantizer kernel tests (no external recall fixtures)").dependOn(&run_vector_kernel_tests.step);

    const subgroup_scan_mod = b.createModule(.{ .root_source_file = b.path("tools/bench_subgroup_scan.zig"), .target = target, .optimize = optimize });
    subgroup_scan_mod.addImport("antfly_vector", vector_mod);
    subgroup_scan_mod.addImport("antfly_vector_index", vectorindex_mod);
    const subgroup_scan_bench = b.addExecutable(.{ .name = "bench-subgroup-scan", .root_module = subgroup_scan_mod });
    const install_subgroup_scan_bench = b.addInstallArtifact(subgroup_scan_bench, .{});
    b.step("bench-subgroup-scan", "Build offline weighted selection and native range scan benchmark").dependOn(&install_subgroup_scan_bench.step);

    const vector_projection_bounds_mod = b.createModule(.{
        .root_source_file = b.path("bench/vectors/vector_projection_bounds_bench.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    vector_projection_bounds_mod.addImport("antfly_vector", vector_mod);
    vector_projection_bounds_mod.addImport("antfly_vector_index", vectorindex_mod);
    vector_projection_bounds_mod.addImport("antfly_platform", platform_mod);
    const vector_projection_bounds_bench = b.addExecutable(.{
        .name = "vector_projection_bounds_bench",
        .root_module = vector_projection_bounds_mod,
    });
    b.step("vector-projection-bounds-bench", "Build and install persisted projection bounds benchmark")
        .dependOn(&b.addInstallArtifact(vector_projection_bounds_bench, .{}).step);

    const vector_cancellation_tests = b.addTest(.{
        .root_module = vector_mod,
        .filters = &.{"RaBitQuantizer checks cancellation inside distance scans"},
    });
    const run_vector_cancellation_tests = b.addRunArtifact(vector_cancellation_tests);
    const vector_cancellation_test_step = b.step("lib-vector-cancellation-test", "Run bounded vector-kernel cancellation tests");
    vector_cancellation_test_step.dependOn(&run_vector_cancellation_tests.step);

    const lib_chunking_tests = b.addTest(.{
        .root_module = chunking_mod,
    });
    const run_lib_chunking_tests = b.addRunArtifact(lib_chunking_tests);
    const lib_chunking_test_step = b.step("lib-chunking-test", "Run standalone lib/chunking tests");
    lib_chunking_test_step.dependOn(&run_lib_chunking_tests.step);

    const lib_readers_tests = b.addTest(.{
        .root_module = readers_mod,
    });
    const run_lib_readers_tests = b.addRunArtifact(lib_readers_tests);
    const lib_readers_test_step = b.step("lib-readers-test", "Run standalone lib/readers tests");
    lib_readers_test_step.dependOn(&run_lib_readers_tests.step);

    const lib_extracting_tests = b.addTest(.{
        .root_module = extracting_mod,
    });
    const run_lib_extracting_tests = b.addRunArtifact(lib_extracting_tests);
    const lib_extracting_test_step = b.step("lib-extracting-test", "Run standalone lib/extracting tests");
    lib_extracting_test_step.dependOn(&run_lib_extracting_tests.step);

    const image_tests = image_build.addTests(b, .{
        .root = b.path("lib/image"),
        .target = target,
        .optimize = optimize,
        .hash_mod = hash_mod,
        .image_mod = image_mod,
    });
    const run_lib_image_tests = image_tests.run_lib_image_tests;
    const run_png_tests = image_tests.run_png_tests;
    const run_jpeg2000_decode_tests = image_tests.run_jpeg2000_decode_tests;
    b.step("lib-image-png-test", "Run PNG codec and checksum compatibility tests").dependOn(&run_png_tests.step);
    b.step("lib-image-jpeg2000-test", "Run direct JPEG 2000 decoder tests").dependOn(&run_jpeg2000_decode_tests.step);
    const image_test_step = b.step("lib-image-test", "Run shared image tests");
    image_test_step.dependOn(&run_lib_image_tests.step);
    image_test_step.dependOn(&run_png_tests.step);
    image_test_step.dependOn(&run_jpeg2000_decode_tests.step);

    const pdf_tests = pdf_build.addTests(b, .{
        .root = b.path("lib/pdf"),
        .target = target,
        .optimize = optimize,
        .image_mod = image_mod,
        .hash_mod = hash_mod,
        .pdf_standard_fonts_mod = pdf_standard_fonts_mod,
        .font_mod = font_mod,
    });
    const run_lib_pdf_tests = pdf_tests.run_lib_pdf_tests;
    const pdf_integration = antfly_tests_build.createPdfIntegration(b, .{
        .root = b.path("pkg/antfly"),
        .fixture = b.path("lib/pdf/integration_fixture.zig"),
        .imports = production_antfly_imports,
        .optimize = optimize,
    });
    const pdf_test_step = b.step("lib-pdf-test", "Run shared PDF tests");
    pdf_test_step.dependOn(&run_lib_pdf_tests.step);
    pdf_test_step.dependOn(&pdf_integration.run.step);
    b.step("pdf-ocr-integration-test", "Run native PDF rendering and encoded reader batching through the OCR coordinator").dependOn(&pdf_integration.run.step);
    b.step("pdf-model-qualification-test", "Run opt-in real Florence/Gemma4/ClipClap PDF qualification against ANTFLY_PDF_QUALIFICATION_URL").dependOn(&pdf_integration.qualification.step);

    const lib_image_spng_paths = image_build.detectSpngPaths(b, target);
    const image_benchmark = image_build.addBenchmark(b, .{
        .root = b.path("lib/image"),
        .target = target,
        .hash_bench_mod = hash_bench_mod,
        .spng_paths = lib_image_spng_paths,
    });
    b.step("lib-image-bench", "Build and install lib-image-bench").dependOn(&b.addInstallArtifact(image_benchmark, .{}).step);

    const pdf_bench_optimize = b.option(std.builtin.OptimizeMode, "pdf-optimize", "Optimization for the isolated PDF executable") orelse .ReleaseFast;
    const pdf_bench_hash = if (pdf_bench_optimize == .ReleaseFast) hash_bench_mod else if (pdf_bench_optimize == optimize) hash_mod else b.createModule(.{
        .root_source_file = b.path("lib/hash/src/mod.zig"),
        .target = target,
        .optimize = pdf_bench_optimize,
    });
    const pdf_bench_image = image_build.createModule(b, b.path("lib/image"), target, pdf_bench_optimize, pdf_bench_hash);
    const pdf_bench_font = b.createModule(.{
        .root_source_file = b.path("lib/font/src/mod.zig"),
        .target = target,
        .optimize = pdf_bench_optimize,
    });
    const pdf_bench_fonts = b.createModule(.{
        .root_source_file = b.path("pdf_standard_fonts.zig"),
        .target = target,
        .optimize = pdf_bench_optimize,
    });
    const pdf_bench_pdf = pdf_build.createModule(b, b.path("lib/pdf"), target, pdf_bench_optimize, pdf_bench_image, pdf_bench_hash, pdf_bench_font, pdf_bench_fonts);
    const pdf_bench = pdf_build.addBenchmark(b, .{
        .root = b.path("lib/pdf"),
        .target = target,
        .optimize = pdf_bench_optimize,
        .pdf_mod = pdf_bench_pdf,
    });
    b.step("lib-pdf-bench", "Build and install lib-pdf-bench").dependOn(&b.addInstallArtifact(pdf_bench, .{}).step);
    const pdf_safety = addFilteredTestRunArtifact(b, pdf_build.addSafetyTests(b, pdf_mod));
    b.step("lib-pdf-safety-test", "Run focused PDF OCR rendering and parser safety tests").dependOn(&pdf_safety.step);

    const image_conformance = image_build.addConformance(b, .{
        .root = b.path("lib/image"),
        .add_test_run = antfly_tests_build.addFilteredTestRunArtifact,
        .conformance_fetch = conformance_fetch,
        .conformance_fixtures = conformance_fixtures,
        .target = target,
        .optimize = optimize,
        .hash_mod = hash_mod,
        .image_mod = image_mod,
        .spng_paths = lib_image_spng_paths,
    });
    const lib_image_conformance_run_step = b.step("lib-image-conformance", "Run lib/image conformance (fetch missing fixtures)");
    for (image_conformance.runs) |run| lib_image_conformance_run_step.dependOn(&run.step);
    b.step("image-jpeg-seed-corpora-e2e", "Build the lib/image upstream JPEG seed-corpora e2e runner").dependOn(&b.addInstallArtifact(image_conformance.jpeg_seed_corpora, .{}).step);
    b.step("image-jpeg2000-fuzz", "Build the JPEG 2000 fuzz runner").dependOn(&b.addInstallArtifact(image_conformance.jpeg2000_fuzz, .{}).step);

    const lib_google_tests = b.addTest(.{ .root_module = google_mod });
    const run_lib_google_tests = addFilteredTestRunArtifact(b, lib_google_tests);
    const lib_google_test_step = b.step("lib-google-test", "Run Google credential cache and transport tests");
    lib_google_test_step.dependOn(&run_lib_google_tests.step);

    const lib_reranking_tests = b.addTest(.{
        .root_module = reranking_mod,
    });
    const run_lib_reranking_tests = b.addRunArtifact(lib_reranking_tests);
    const lib_reranking_test_step = b.step("lib-reranking-test", "Run standalone lib/reranking tests");
    lib_reranking_test_step.dependOn(&run_lib_reranking_tests.step);

    const lib_casbin_tests = b.addTest(.{
        .root_module = casbin_mod,
    });
    const run_lib_casbin_tests = b.addRunArtifact(lib_casbin_tests);
    const lib_casbin_test_step = b.step("lib-casbin-test", "Run standalone lib/casbin tests");
    lib_casbin_test_step.dependOn(&run_lib_casbin_tests.step);

    const raft_library_tests = b.addTest(.{
        .root_module = raft_engine_mod,
        .filters = selectTestFilters(b, &.{}),
    });
    const run_raft_library_tests = addFilteredTestRunArtifact(b, raft_library_tests);
    const raft_library_test_step = b.step("lib-raft-test", "Run standalone raft library tests");
    raft_library_test_step.dependOn(&run_raft_library_tests.step);

    const owner_tests = antfly_tests_build.addTests(b, .{
        .lmdb_engine = lmdb_engine_mod,
        .vopr = vopr_mod,
        .optimize = optimize,
        .lmdb_backend = lmdb_backend,
        .lmdb_evented_async_io = lmdb_evented_async_io,
        .standalone_runtime_build_options = standalone_runtime_build_options,
        .openapi_root_check = openapi_root_check,
        .usermgr_mod = usermgr_mod,
        .antfly_imports = antfly_imports,
        .antfly_mod = antfly_mod,
        .embedded_mod = embedded_mod,
        .embedded_api_mod = embedded_api_mod,
        .antfly_embedded_pkg_mod = antfly_embedded_pkg_mod,
        .antfly_embedded_db_pkg_mod = antfly_embedded_db_pkg_mod,
        .antfly_embedded_api_pkg_mod = antfly_embedded_api_pkg_mod,
        .antfly_client_pkg_mod = antfly_client_pkg_mod,
        .embedded_db_mod = embedded.embedded_db_mod,
        .embedded_support_mod = embedded.embedded_support_mod,
        .capi_root_mod = embedded.capi_root_mod,
        .capi_mod = embedded.capi_mod,
        .run_capi_tests = run_capi_tests,
        .run_raft_library_tests = run_raft_library_tests,
    });
    const antfly_test_mod = owner_tests.antfly_test_mod;
    const run_antfly_embedded_pkg_tests = owner_tests.run_antfly_embedded_pkg_tests;
    const run_lite_native_tests = owner_tests.run_lite_native_tests;
    const run_lite_cmd_tests = owner_tests.run_lite_cmd_tests;
    const run_lib_ha_compat_tests = owner_tests.run_lib_ha_compat_tests;
    const antfly_test_step = owner_tests.antfly_test_step;
    const unit_test_step = owner_tests.unit_test_step;
    unit_test_step.dependOn(&pdf_integration.run.step);
    unit_test_step.dependOn(&run_httpx_client_lifecycle_tests.step);
    const vopr_test_step = owner_tests.vopr_test_step;
    const integration_test_step = owner_tests.integration_test_step;
    const chaos_test_step = owner_tests.chaos_test_step;
    const compiled_recall_tests = owner_tests.compiled_recall_tests;

    const test_step = b.step("test", "Run default package test aggregates");
    const conformance_test_step = b.step("conformance-test", "Fetch and run conformance suites");
    const soak_test_step = b.step("soak-test", "Run long-running soak test aggregates");
    const lib_test_step = b.step("lib-test", "Run default standalone library tests");
    dependOnAll(conformance_test_step, &.{ lib_toon_conformance_step, lib_image_conformance_run_step });
    lib_test_step.dependOn(&run_yacc_tests.step);
    lib_test_step.dependOn(&yacc_steps.run_parser_tests.step);
    lib_test_step.dependOn(&run_lib_regex_tests.step);
    lib_test_step.dependOn(&run_raft_library_tests.step);
    lib_test_step.dependOn(&run_lib_jsonschema_tests.step);
    lib_test_step.dependOn(&run_lib_generating_tests.step);
    lib_test_step.dependOn(&run_lib_embeddings_tests.step);
    lib_test_step.dependOn(&run_lib_vectorindex_tests.step);
    lib_test_step.dependOn(&run_lib_hash_tests.step);
    lib_test_step.dependOn(&run_vector_cancellation_tests.step);
    lib_test_step.dependOn(&run_lib_chunking_tests.step);
    lib_test_step.dependOn(&run_lib_google_tests.step);
    lib_test_step.dependOn(&run_lib_reranking_tests.step);
    lib_test_step.dependOn(&run_httpx_transport_regression_tests.step);
    lib_test_step.dependOn(&run_lib_casbin_tests.step);
    lib_test_step.dependOn(&run_lib_toon_tests.step);
    lib_test_step.dependOn(&run_lib_mcp_tests.step);
    lib_test_step.dependOn(&run_lib_a2a_tests.step);
    lib_test_step.dependOn(&run_lib_image_tests.step);
    lib_test_step.dependOn(&run_png_tests.step);
    lib_test_step.dependOn(&run_jpeg2000_decode_tests.step);
    lib_test_step.dependOn(&run_lib_pdf_tests.step);
    lib_test_step.dependOn(&run_lib_scraping_tests.step);
    lib_test_step.dependOn(&run_hf_tokenizer_tests.step);
    lib_test_step.dependOn(platform_test_step);
    dependOnAll(lib_test_step, &.{
        &run_lib_json_tests.step,
        lib_onnx_test_step,
        &run_objectstore_tests.step,
        &run_httpx_json_tests.step,
        &run_httpx_tests.step,
    });
    soak_test_step.dependOn(owner_tests.chaos_soak_test_step);
    soak_test_step.dependOn(owner_tests.storage_workload_soak_step);
    const lib_transcribing_tests = b.addTest(.{
        .root_module = transcribing_mod,
    });
    const run_lib_transcribing_tests = b.addRunArtifact(lib_transcribing_tests);
    const lib_transcribing_test_step = b.step("lib-transcribing-test", "Run standalone lib/transcribing tests");
    lib_transcribing_test_step.dependOn(&run_lib_transcribing_tests.step);

    const audio_conformance = audio_build.addConformance(b, .{
        .root = b.path("lib/audio"),
        .conformance_fetch = conformance_fetch,
        .conformance_fixtures = conformance_fixtures,
        .target = target,
    });
    const audio_conformance_step = b.step("lib-audio-conformance", "Run lib/audio conformance (fetch missing fixtures)");
    for (audio_conformance) |run| audio_conformance_step.dependOn(&run.step);
    conformance_test_step.dependOn(audio_conformance_step);

    const regex_bench_mod = b.createModule(.{
        .root_source_file = b.path("lib/regex/bench/regex_bench.zig"),
        .target = target,
        .optimize = optimize,
    });
    regex_bench_mod.addImport("antfly_regex", regex_mod);
    regex_bench_mod.addImport("antfly_vellum", vellum_mod);
    const regex_bench = b.addExecutable(.{
        .name = "regex_bench",
        .root_module = regex_bench_mod,
    });

    const regex_bench_step = b.step("regex-bench", "Build and install regex_bench");
    regex_bench_step.dependOn(&b.addInstallArtifact(regex_bench, .{}).step);

    const json_bench_mod = b.createModule(.{
        .root_source_file = b.path("lib/json/bench/json_bench.zig"),
        .target = target,
        .optimize = optimize,
    });
    json_bench_mod.addImport("antfly-json", json_mod);

    const json_bench = b.addExecutable(.{
        .name = "json_bench",
        .root_module = json_bench_mod,
    });

    const json_bench_step = b.step("json-bench", "Build and install json_bench");
    json_bench_step.dependOn(&b.addInstallArtifact(json_bench, .{}).step);

    const tokenizer_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/tokenizer_benchmark.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    tokenizer_bench_mod.addImport("inference_tokenizer", inference_graph.inference_tokenizer_mod);
    const tokenizer_bench = b.addExecutable(.{
        .name = "tokenizer_benchmark",
        .root_module = tokenizer_bench_mod,
    });
    const install_tokenizer_bench = b.addInstallArtifact(tokenizer_bench, .{});
    const tokenizer_bench_build_step = b.step(
        "bench-tokenizer-build",
        "Build the native Zig HuggingFace tokenizer benchmark binary",
    );
    tokenizer_bench_build_step.dependOn(&install_tokenizer_bench.step);

    const tokenizer_bench_step = b.step("bench-tokenizer", "Build and install tokenizer_benchmark");
    tokenizer_bench_step.dependOn(&b.addInstallArtifact(tokenizer_bench, .{}).step);

    const benchmarks = antfly_benches_build.addBenchmarks(b, .{
        .lmdb_engine = lmdb_engine_mod,
        .api_bench_standalone = api_bench_standalone,
        .optimize = optimize,
        .lmdb_backend = lmdb_backend,
        .lmdb_evented_async_io = lmdb_evented_async_io,
        .with_tla = with_tla,
        .antfly_imports = antfly_imports,
        .antfly_mod = antfly_mod,
        .antfly_test_mod = antfly_test_mod,
        .run_lib_ha_compat_tests = run_lib_ha_compat_tests,
        .compiled_recall_tests = compiled_recall_tests,
    });
    const recall_ci_test_step = benchmarks.recall_ci_test_step;

    const runtime = antfly_runtime_build.addRuntime(b, .{
        .target = target,
        .optimize = optimize,
        .strip = strip,
        .link_libc = link_libc,
        .sanitize_thread = sanitize_thread,
        .runtime_artifact_role = runtime_artifact_role,
        .structlog_mod = structlog_mod,
        .platform_mod = platform_mod,
        .hash_mod = hash_mod,
        .production_antfly_imports = production_antfly_imports,
        .antfly_client_pkg_mod = antfly_client_pkg_mod,
        .capi_mod = capi_mod,
        .libantfly_link_mod = libantfly_link_mod,
    });
    b.step("linked-inference-abi-integration-test", "Run the production-linked inference function-table and binary-payload ABI probe").dependOn(&runtime.run_linked_inference_abi_integration.step);
    owner_tests.standalone_runtime_test_step.dependOn(&runtime.run_linked_inference_abi_integration.step);
    const antfly_main = runtime.antfly_main;
    const runtime_library_artifacts = runtime.runtime_library_artifacts;

    const antfly_main_tests = runtime.antfly_main_tests;
    const run_antfly_main_tests = b.addRunArtifact(antfly_main_tests);
    addRuntimeTestFilters(b, run_antfly_main_tests, selectTestFilters(b, &.{}));
    const antfly_main_test_step = b.step("antfly-main-test", "Run top-level Antfly CLI tests");
    antfly_main_test_step.dependOn(&run_antfly_main_tests.step);
    unit_test_step.dependOn(&run_antfly_main_tests.step);

    // The aggregate intentionally runs with normal CPU concurrency. Give every
    // compile step a conservative scheduler claim unless it already has a
    // measured, domain-specific claim above; CI supplies the cgroup-aware
    // aggregate budget through --maxrss.
    assignDefaultAggregateMaxRss(
        b,
        unit_test_step,
        @as(usize, if (target.result.os.tag == .macos) 10 else 7) * 1024 * 1024 * 1024,
        6 * 1024 * 1024 * 1024,
    );

    const install_antfly = b.addInstallArtifact(antfly_main, .{ .dest_sub_path = antfly_bin_name });
    const install_antfarm_assets = b.addInstallDirectory(.{
        .source_dir = b.path("pkg/antfly/antfarm"),
        .install_dir = .prefix,
        .install_subdir = "share/antfly/antfarm",
    });
    b.getInstallStep().dependOn(&install_antfly.step);
    b.getInstallStep().dependOn(&install_antfarm_assets.step);
    const antfly_step = b.step("antfly", "Build and install the top-level Antfly CLI");
    antfly_step.dependOn(&install_antfly.step);
    antfly_step.dependOn(&install_antfarm_assets.step);

    const lite_module_options: std.Build.Module.CreateOptions = .{
        .root_source_file = b.path("pkg/antfly/src/lite_main.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "build_info", .module = build_info.module },
            .{ .name = "build_options", .module = production_build_options.createModule() },
            .{ .name = "structlog", .module = structlog_mod },
            .{ .name = "antfly_platform", .module = platform_mod },
            .{ .name = "antfly_hash", .module = hash_mod },
        },
    };
    const lite_main_mod = b.createModule(lite_module_options);
    build_info.link(lite_main_mod);
    const lite_main = b.addExecutable(.{
        .name = "antfly-lite",
        .root_module = lite_main_mod,
    });
    // Lite commands are owned by the standalone runtime, including lite serve.
    for ([_]RuntimeLibraryUnit{ .distributed, .api_kernel, .inference }) |unit| {
        lite_main.root_module.linkLibrary(runtime_library_artifacts[@intFromEnum(unit)].?);
    }
    const lite_cli_smoke = b.addExecutable(.{
        .name = "antfly-lite-cli-smoke",
        .root_module = b.createModule(.{
            .root_source_file = b.path("examples/antfly_lite_cli_smoke.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_lite_cli_smoke = b.addRunArtifact(lite_cli_smoke);
    run_lite_cli_smoke.addArtifactArg(lite_main);
    const run_antfly_lite_cli_smoke = b.addRunArtifact(lite_cli_smoke);
    run_antfly_lite_cli_smoke.addArtifactArg(antfly_main);
    const lite_main_tests = b.addTest(.{
        .root_module = b.createModule(lite_module_options),
        .filters = &.{"lite main compiles"},
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lite_main_tests = addFilteredTestRunArtifact(b, lite_main_tests);
    const install_lite_main = b.addInstallArtifact(lite_main, .{ .dest_sub_path = antfly_bin_name });

    const lite_step = b.step("lite", "Build and install the Antfly Lite CLI and libantfly C ABI");
    lite_step.dependOn(&install_lite_main.step);
    lite_step.dependOn(&install_libantfly.step);
    lite_step.dependOn(&install_capi_header.step);

    const lite_test_step = b.step("lite-test", "Run Lite backend, CLI, bindings, examples, and C ABI packaging checks");
    lite_test_step.dependOn(&run_antfly_main_tests.step);
    lite_test_step.dependOn(&run_lite_main_tests.step);
    lite_test_step.dependOn(&run_lite_cmd_tests.step);
    lite_test_step.dependOn(&run_lite_native_tests.step);
    lite_test_step.dependOn(&run_capi_smoke.step);
    lite_test_step.dependOn(&run_lite_go_tests.step);
    lite_test_step.dependOn(&run_lite_go_example.step);
    lite_test_step.dependOn(&run_lite_go_retrieval_template.step);
    lite_test_step.dependOn(&run_lite_cli_smoke.step);
    lite_test_step.dependOn(&run_antfly_lite_cli_smoke.step);
    lite_test_step.dependOn(&run_cabi_packaging_tests.step);
    lite_test_step.dependOn(&run_capi_tests.step);
    lite_test_step.dependOn(&run_antfly_embedded_pkg_tests.step);

    dependOnAll(antfly_test_step, &.{
        unit_test_step,
        vopr_test_step,
        integration_test_step,
        recall_ci_test_step,
        chaos_test_step,
    });

    dependOnAll(test_step, &.{
        lib_test_step,
        antfly_test_step,
        inference_steps.inference_test,
        inference_steps.inference_finetune_test,
    });

    // `test` owns more than the Antfly unit-test subgraph. Fill in claims for
    // simulation, integration, recall, chaos, and inference steps
    // too so --maxrss bounds the complete aggregate instead of only one arm.
    assignDefaultAggregateMaxRss(
        b,
        test_step,
        @as(usize, if (target.result.os.tag == .macos) 10 else 7) * 1024 * 1024 * 1024,
        6 * 1024 * 1024 * 1024,
    );

    const hbc_trace_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/tools/hbc_trace.zig"),
        .target = target,
        .optimize = optimize,
    });
    hbc_trace_mod.addImport("antfly-zig", antfly_mod);
    const recall_common_mod = b.createModule(.{
        .root_source_file = b.path("bench/vectors/recall_common.zig"),
        .target = target,
        .optimize = optimize,
    });
    recall_common_mod.addImport("antfly-zig", antfly_mod);
    hbc_trace_mod.addImport("recall_common", recall_common_mod);

    const hbc_trace = b.addExecutable(.{
        .name = "hbc_trace",
        .root_module = hbc_trace_mod,
    });

    const hbc_trace_step = b.step("hbc-trace", "Build and install hbc_trace");
    hbc_trace_step.dependOn(&b.addInstallArtifact(hbc_trace, .{}).step);

    const hbc_leaf_debug_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/tools/hbc_leaf_debug.zig"),
        .target = target,
        .optimize = optimize,
    });
    hbc_leaf_debug_mod.addImport("antfly-zig", antfly_mod);
    hbc_leaf_debug_mod.addImport("recall_common", recall_common_mod);

    const hbc_leaf_debug = b.addExecutable(.{
        .name = "hbc_leaf_debug",
        .root_module = hbc_leaf_debug_mod,
    });

    const hbc_leaf_debug_step = b.step("hbc-leaf-debug", "Build and install hbc_leaf_debug");
    hbc_leaf_debug_step.dependOn(&b.addInstallArtifact(hbc_leaf_debug, .{}).step);
    if (b.option(bool, "test-progress", "Label existing antfly-unit-test run nodes without changing test selection or scheduling") orelse false) {
        antfly_tests_build.labelTestRuns(b, unit_test_step);
        antfly_tests_build.labelTestRuns(b, lib_test_step);
    }
    return .{ .runtime = runtime, .inference = inference_graph, .wasm = wasm.artifact };
}
