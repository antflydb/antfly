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

const std = @import("std");
const builtin = @import("builtin");
const audio_build = @import("../../lib/audio/build_support.zig");
const pdf_build = @import("../../lib/pdf/build_support.zig");
const image_build = @import("../../lib/image/build_support.zig");
const lib_sql_build_support = @import("../../lib/sql/build_support.zig");
const yacc_build = @import("../../lib/yacc/build_support.zig");
const tools_build = @import("../../tools/build_support.zig");
const pkg_antfly_build_codegen = @import("../openapi.zig");
const addOpenApiRootCheckStep = pkg_antfly_build_codegen.addOpenApiRootCheckStep;
const addOpenApiSourceSteps = pkg_antfly_build_codegen.addOpenApiSourceSteps;
const lib_platform_build_support = @import("antfly_platform");
const platform_build = lib_platform_build_support;
const addMacosSdkPaths = lib_platform_build_support.addMacosSdkPaths;
const pkg_antfly_build_imports = @import("imports.zig");
const AntflyRootImports = pkg_antfly_build_imports.AntflyRootImports;
const pkg_antfly_build_snowball = @import("../embedded/snowball.zig");
const antfly_storage_build = @import("../embedded/storage.zig");
const inference_runtime_build = @import("../../pkg/inference/build/runtime.zig");
const antfly_tests_build = @import("test_support.zig");
const LmdbBackend = antfly_storage_build.LmdbBackend;
const RuntimeArtifactRole = @import("runtime_roles.zig").RuntimeArtifactRole;
const makeLmdbBuildOptions = antfly_storage_build.makeLmdbBuildOptions;
const makeLmdbEngineModule = antfly_storage_build.makeLmdbEngineModule;
const makeRootBuildOptions = antfly_storage_build.makeRootBuildOptions;
const selectTestFilters = antfly_tests_build.selectTestFilters;
const addFilteredTestRunArtifact = antfly_tests_build.addFilteredTestRunArtifact;
const addRuntimeTestFilters = antfly_tests_build.addRuntimeTestFilters;
const dependOnAll = antfly_tests_build.dependOnAll;

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
    optimize: std.lang.Optimize,
) *std.Build.Module {
    return b.createModule(.{
        .root_source_file = b.path("lib/httpx/src/httpx.zig"),
        .target = target,
        .optimize = optimize,
    });
}

pub const Shared = struct {
    api_bench_standalone: bool,
    conformance_fetch: bool,
    conformance_fixtures: []const u8,
    target: std.Build.ResolvedTarget,
    optimize: std.lang.Optimize,
    vopr_mod: *std.Build.Module,
    strip: bool,
    lmdb_backend: LmdbBackend,
    lmdb_evented_async_io: bool,
    with_tla: bool,
    link_libc: bool,
    sanitize_thread: bool,
    runtime_artifact_role: ?RuntimeArtifactRole,
    antfly_bin_name: []const u8,
    inference_enable_onnx: bool,
    inference_enable_metal: bool,
    inference_enable_cuda: bool,
    antfly_version: []const u8,
    build_info: @import("../../lib/build_info/build_support.zig").BuildInfo,
    platform_test_step: *std.Build.Step,
    build_options: *std.Build.Step.Options,
    standalone_runtime_build_options: *std.Build.Step.Options,
    production_build_options: *std.Build.Step.Options,
    lmdb_engine_mod: *std.Build.Module,
    raft_engine_mod: *std.Build.Module,
    json_mod: *std.Build.Module,
    httpx_mod: *std.Build.Module,
    structlog_mod: *std.Build.Module,
    run_yacc_tests: *std.Build.Step.Run,
    yacc_steps: lib_sql_build_support.Steps,
    run_sql_tests: *std.Build.Step.Run,
    run_pgwire_tests: *std.Build.Step.Run,
    openapi_root_check: *std.Build.Step.Run,
    openapi_docs_test: *std.Build.Step.Compile,
    protobuf_mod: *std.Build.Module,
    platform_mod: *std.Build.Module,
    objectstore_mod: *std.Build.Module,
    google_mod: *std.Build.Module,
    vector_mod: *std.Build.Module,
    hash_mod: *std.Build.Module,
    hash_bench_mod: *std.Build.Module,
    vectorindex_mod: *std.Build.Module,
    casbin_mod: *std.Build.Module,
    usermgr_mod: *std.Build.Module,
    fst_mod: *std.Build.Module,
    regex_mod: *std.Build.Module,
    jsonschema_mod: *std.Build.Module,
    toon_mod: *std.Build.Module,
    mcp_mod: *std.Build.Module,
    a2a_mod: *std.Build.Module,
    matcher_mod: *std.Build.Module,
    resolver_mod: *std.Build.Module,
    generating_mod: *std.Build.Module,
    chunking_mod: *std.Build.Module,
    embeddings_mod: *std.Build.Module,
    scraping_mod: *std.Build.Module,
    reranking_mod: *std.Build.Module,
    extracting_mod: *std.Build.Module,
    image_mod: *std.Build.Module,
    pdf_standard_fonts_mod: *std.Build.Module,
    font_mod: *std.Build.Module,
    pdf_mod: *std.Build.Module,
    sentencepiece_proto_source: std.Build.LazyPath,
    inference_ml_mod: *std.Build.Module,
    ml_tabular_mod: *std.Build.Module,
    inference_onnx: @import("onnx_graph").support.Modules,
    tokenizer: @import("../../lib/tokenizer/build_support.zig").Modules,
    inference_graph: inference_runtime_build.Graph,
    run_hf_tokenizer_tests: *std.Build.Step.Run,
    transcribing_mod: *std.Build.Module,
    readers_mod: *std.Build.Module,
    inference_steps: @import("../../pkg/inference/build/integration.zig").Steps,
    antfly_imports: AntflyRootImports,
    production_antfly_imports: AntflyRootImports,
};

/// Shared dependency composition; product owners select their own roots.
pub fn create(b: *std.Build, comptime asking_build_zig: type) ?Shared {
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
    const lmdb_backend = b.option(LmdbBackend, "lmdb_backend", "Select the LMDB implementation for test and benchmark fixtures (c or zig)") orelse .zig;
    const lmdb_evented_async_io = b.option(bool, "lmdb_evented_async_io", "Use std.Io.Evented for standalone Zig LMDB test and benchmark fixtures") orelse false;
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
    const inference_blas = @import("../../pkg/inference/build/blas.zig").configure(b, link_libc, target.result.os.tag == .macos, inference_blas_root_opt != null);
    const inference_enable_system_blas = inference_blas.system;
    const inference_blas_root = if (inference_enable_system_blas and target.result.os.tag != .macos)
        inference_blas_root_opt
    else
        null;
    const antfly_version = b.option([]const u8, "antfly-version", "Antfly version string") orelse "dev";
    const build_info = @import("../../lib/build_info/build_support.zig").create(b, .{
        .root = b.path("lib/build_info"),
        .target = target,
        .optimize = optimize,
        .version = antfly_version,
    });
    // Antfly Lite always links and advertises the embedded local inference
    // runtime, matching the `antfly` executable (see COMPILATION.md's "C API
    // composition" section and LITE.md's "Local Embedded Inference" section).
    // This remains a build option so a caller can still opt out of
    // advertising the capability; freestanding/wasm builds always disable it
    // regardless of this flag (see storage/lite/capabilities.zig).
    // Antfly Lite always links and advertises the embedded local inference
    // runtime, matching the `antfly` executable (see COMPILATION.md's "C API
    // composition" section and LITE.md's "Local Embedded Inference" section).
    // This remains a build option so a caller can still opt out of
    // advertising the capability; freestanding/wasm builds always disable it
    // regardless of this flag (see storage/lite/capabilities.zig).
    const lite_local_inference_runtime = b.option(bool, "lite-local-inference-runtime", "Advertise an embedded local inference runtime in Antfly Lite status") orelse true;
    const platform_tests = platform_build.addTests(b, .{
        .root = b.path("lib/platform"),
        .target = target,
        .optimize = optimize,
        .link_libc = link_libc,
    });

    const platform_test_step = b.step("lib-platform-test", "Run supervisor unit and process-lifecycle tests (Python 3 on POSIX)");
    platform_test_step.dependOn(&platform_tests.unit.step);
    if (platform_tests.process) |process| platform_test_step.dependOn(process);
    platform_test_step.dependOn(&platform_tests.one_shot_unit.step);
    if (platform_tests.one_shot_process) |process| platform_test_step.dependOn(process);

    const platform_mod = platform_build.createModule(b, .{
        .root_source_file = b.path("lib/platform/src/root.zig"),
        .filesystem_capacity_source_file = b.path("lib/platform/src/filesystem_capacity.c"),
        .target = target,
        .optimize = optimize,
        .link_libc = link_libc,
    });
    const lmdb_build_options = makeLmdbBuildOptions(b, lmdb_backend, lmdb_evented_async_io, false);
    const build_options = makeRootBuildOptions(b, false, with_tla, link_libc, false, false);
    const standalone_runtime_build_options = makeRootBuildOptions(b, false, with_tla, link_libc, true, false);
    const production_build_options = makeRootBuildOptions(b, false, with_tla, link_libc, false, true);
    const lmdb_engine_mod = makeLmdbEngineModule(b, target, optimize, link_libc, lmdb_build_options, platform_mod);
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
    const openapi_build = b.lazyImport(asking_build_zig, "openapi") orelse return null;
    const openapi_codegen = openapi_build.addCompiler(b, b.path("lib/openapi"), b.graph.host, .safe);
    const openapi_sources = addOpenApiSourceSteps(b, openapi_build, openapi_codegen);
    const update_public_openapi = b.addUpdateSourceFiles();
    update_public_openapi.addCopyFileToSource(openapi_sources.public_spec, "../openapi.yaml");
    const openapi_regen_step = b.step("regen-openapi", "Regenerate checked-in OpenAPI sources");
    openapi_regen_step.dependOn(&openapi_sources.regen.step);
    openapi_regen_step.dependOn(&update_public_openapi.step);
    const openapi_check_step = b.step("check-openapi", "Compare checked-in OpenAPI sources without modifying them");
    openapi_check_step.dependOn(&openapi_sources.check.step);
    const openapi_docs_test_mod = b.createModule(.{
        .root_source_file = b.path("build_support/openapi_exact_sort_test.zig"),
        .target = b.graph.host,
        .optimize = optimize,
    });
    openapi_docs_test_mod.addAnonymousImport("public_types.zig", .{ .root_source_file = b.path("pkg/antfly-embedded/src/openapi/generated/antfly_public_openapi/types.zig") });
    openapi_docs_test_mod.addAnonymousImport("metadata_types.zig", .{ .root_source_file = b.path("pkg/antfly-embedded/src/openapi/generated/antfly_metadata_openapi/types.zig") });
    openapi_docs_test_mod.addAnonymousImport("client_types.zig", .{ .root_source_file = b.path("pkg/antfly-client/src/openapi/generated/antfly_client_openapi/types.zig") });
    const openapi_docs_test = b.addTest(.{ .root_module = openapi_docs_test_mod });
    openapi_check_step.dependOn(&b.addRunArtifact(openapi_docs_test).step);
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
        .codegen = yacc_build.addCompiler(b, b.path("lib/yacc"), b.graph.host, .safe),
        .compare_tool = tools_build.addFileCompareTool(b, b.path("tools")),
        .grammar_label = "lib/sql/grammar/antfly_sql.y",
    });
    b.step("regen-sql-grammar", "Regenerate checked-in Antfly SQL grammar metadata").dependOn(&yacc_steps.regen.step);
    const sql_generated_check = b.step("sql-grammar-generated-check", "Check and compile the generated Antfly SQL grammar metadata");
    sql_generated_check.dependOn(&yacc_steps.compare.step);
    sql_generated_check.dependOn(&yacc_steps.run_generated.step);
    b.step("lib-sql-parser-test", "Run the storage-independent SQL lexer and parser tests").dependOn(&yacc_steps.run_parser_tests.step);
    b.step("lib-sql-parser-bench", "Build and install lib-sql-parser-bench").dependOn(&b.addInstallArtifact(yacc_steps.benchmark, .{}).step);
    const sql_parser_mod = b.createModule(.{
        .root_source_file = b.path("lib/sql/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    const sql_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/sql_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    // Native SQL fixtures import storage and API contracts whose unrelated
    // transitive tests require different owner roots. Select the SQL and
    // row-policy contract namespaces at compile time, while allowing explicit
    // caller filters.
    const sql_tests = b.addTest(.{ .root_module = sql_test_mod, .filters = selectTestFilters(b, &.{ "sql.", "system_catalog.policies" }) });
    const run_sql_tests = b.addRunArtifact(sql_tests);
    // The native SQL contract corpus is larger than the parser-only owner but
    // remains below the full database compilation and integration test roots.
    sql_tests.step.max_rss = 1536 * 1024 * 1024;
    // The complete compiler/executor corpus includes exhaustive allocation-fault
    // runs and parallel partition lifecycle checks. This scheduling estimate
    // is independent of the executor's per-statement memory admission tests.
    run_sql_tests.step.max_rss = 384 * 1024 * 1024;
    b.step("sql-test", "Run SQL compilation, catalog binding, and native execution contract tests").dependOn(&run_sql_tests.step);
    const pgwire_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/pgwire_test_root.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = link_libc,
    });
    @import("../embedded/source_owner.zig").attach(pgwire_test_mod);
    pgwire_test_mod.addImport("sql_parser", sql_parser_mod);
    // This intentionally remains a storage-independent test root. Every
    // pgwire-owned test has the pgwire prefix; without a compile filter Zig
    // also discovers transitive storage tests through the SQL binder and
    // requires unrelated native imports this module does not provide.
    const pgwire_tests = b.addTest(.{
        .root_module = pgwire_test_mod,
        .filters = buildArguments(b) orelse &.{"pgwire"},
    });
    const run_pgwire_tests = b.addRunArtifact(pgwire_tests);
    pgwire_tests.step.max_rss = 1024 * 1024 * 1024;
    run_pgwire_tests.step.max_rss = 64 * 1024 * 1024;
    b.step("pgwire-test", "Run PostgreSQL wire framing, session, and lifecycle tests").dependOn(&run_pgwire_tests.step);
    const openapi_root_check = addOpenApiRootCheckStep(b);
    openapi_check_step.dependOn(&openapi_root_check.step);
    const openapi_modules = pkg_antfly_build_codegen.createCommittedModules(b, .{
        .root = b.path("pkg/antfly-embedded/src/openapi/generated"),
        .client_root = b.path("pkg/antfly-client/src/openapi/generated"),
        .server_root = b.path("pkg/antfly-server-api/src/openapi/generated"),
        .target = target,
        .optimize = optimize,
        .httpx = httpx_mod,
        .json = json_mod,
        .export_modules = true,
    });
    const public_openapi_mod = openapi_modules.public;
    const public_server_openapi_mod = openapi_modules.public_server;
    const client_openapi_mod = openapi_modules.client;
    const schema_openapi_mod = openapi_modules.schema;
    const indexes_openapi_mod = openapi_modules.indexes;
    const sort_openapi_mod = openapi_modules.sort;
    const eval_openapi_mod = openapi_modules.eval;
    const query_openapi_mod = openapi_modules.query;
    const admin_openapi_mod = openapi_modules.admin;
    const internal_openapi_mod = openapi_modules.internal;
    const usermgr_openapi_mod = openapi_modules.usermgr;
    const usermgr_server_openapi_mod = openapi_modules.usermgr_server;
    const metadata_openapi_mod = openapi_modules.metadata;
    const metadata_server_openapi_mod = openapi_modules.metadata_server;
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
    const exa_api_mod = openapi_modules.exa_api;
    const tavily_api_mod = openapi_modules.tavily_api;

    // Schema checks execute on the build host even during cross compilation.
    const openapi_check_json = b.createModule(.{
        .root_source_file = b.path("lib/json/src/mod.zig"),
        .target = b.graph.host,
        .optimize = optimize,
    });
    const openapi_check_httpx = b.createModule(.{
        .root_source_file = b.path("lib/httpx/src/httpx.zig"),
        .target = b.graph.host,
        .optimize = optimize,
    });
    openapi_check_httpx.addImport("antfly-json", openapi_check_json);
    const openapi_check_modules = pkg_antfly_build_codegen.createCommittedModules(b, .{
        .root = b.path("pkg/antfly-embedded/src/openapi/generated"),
        .client_root = b.path("pkg/antfly-client/src/openapi/generated"),
        .server_root = b.path("pkg/antfly-server-api/src/openapi/generated"),
        .target = b.graph.host,
        .optimize = optimize,
        .httpx = openapi_check_httpx,
        .json = openapi_check_json,
    });
    const openapi_split_test_mod = b.createModule(.{
        .root_source_file = b.path("build_support/openapi_split_test.zig"),
        .target = b.graph.host,
        .optimize = optimize,
    });
    openapi_split_test_mod.addImport("antfly_public_openapi", openapi_check_modules.public);
    openapi_split_test_mod.addImport("antfly_public_server_openapi", openapi_check_modules.public_server);
    openapi_split_test_mod.addImport("antfly_metadata_openapi", openapi_check_modules.metadata);
    openapi_split_test_mod.addImport("antfly_metadata_server_openapi", openapi_check_modules.metadata_server);
    openapi_split_test_mod.addImport("antfly_usermgr_openapi", openapi_check_modules.usermgr);
    openapi_split_test_mod.addImport("antfly_usermgr_server_openapi", openapi_check_modules.usermgr_server);
    openapi_check_step.dependOn(&b.addRunArtifact(b.addTest(.{ .root_module = openapi_split_test_mod })).step);

    // Handlebars template engine
    const handlebars_dep = b.dependency("handlebars", .{ .target = target, .optimize = optimize });
    const handlebars_mod = handlebars_dep.module("handlebars");

    // Protobuf wire format
    const protobuf_dep = b.dependency("protobuf", .{ .target = target, .optimize = optimize });
    const protobuf_mod = protobuf_dep.module("protobuf");
    const evented_enrichment_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/raft/enrichment_executor_test.zig"),
        .target = target,
        .optimize = optimize,
    });
    evented_enrichment_test_mod.addImport("antfly_platform", platform_mod);
    const evented_enrichment_tests = b.addTest(.{ .root_module = evented_enrichment_test_mod });
    const evented_enrichment_step = b.step("evented-enrichment-test", "Test enrichment Evented lifetime, concurrent tasks, cancellation, and file I/O");
    evented_enrichment_step.dependOn(&b.addRunArtifact(evented_enrichment_tests).step);
    if (target.result.os.tag == .macos and
        (target.result.cpu.arch == .aarch64 or target.result.cpu.arch == .x86_64))
    {
        const dispatch_tests = b.addTest(.{ .root_module = b.createModule(.{
            .root_source_file = b.path("lib/platform/src/dispatch_compat.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
        }) });
        evented_enrichment_step.dependOn(&b.addRunArtifact(dispatch_tests).step);
    }

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
    const hash_bench_mod = if (optimize == .fast) hash_mod else b.createModule(.{
        .root_source_file = b.path("lib/hash/src/mod.zig"),
        .target = target,
        .optimize = .fast,
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
    usermgr_mod.addImport("antfly_source_root", usermgr_mod);
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
    const fst_mod = b.createModule(.{
        .root_source_file = b.path("lib/fst/src/mod.zig"),
        .target = target,
        .optimize = optimize,
    });
    const regex_mod = b.createModule(.{
        .root_source_file = b.path("lib/regex/src/mod.zig"),
        .target = target,
        .optimize = optimize,
    });
    regex_mod.addImport("antfly_fst", fst_mod);
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
    const pdf_standard_fonts_mod = @import("fonts.zig").create(b, target, optimize);
    const font_mod = b.createModule(.{
        .root_source_file = b.path("lib/font/src/mod.zig"),
        .target = target,
        .optimize = optimize,
    });
    const pdf_mod = pdf_build.createModule(b, b.path("lib/pdf"), target, optimize, image_mod, hash_mod, font_mod, pdf_standard_fonts_mod, platform_mod);

    const tokenizer_build = @import("../../lib/tokenizer/build_support.zig");
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

    const tokenizer = @import("../../lib/tokenizer/build_support.zig").create(b, .{
        .platform = platform_mod,
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
            .enable_runtime_openblas = inference_blas.runtime,
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
            .fst = fst_mod,
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
    hf_tokenizer_tests.root_module.addImport("antfly_platform", platform_mod);
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

    const inference_workflow = @import("../../pkg/inference/build/context.zig").Context{
        .b = b,
        .target = target,
        .optimize = optimize,
        .paths = inference_config.paths,
        .backend = inference_config.backend,
        .graph = inference_graph,
        .args = buildArguments(b),
        .step_prefix = "inference-",
        .install_apache_licenses = @import("../../lib/product_licenses/build.zig").installApache,
        .add_native_process_test = platform_build.addNativeProcessTest,
        .runtime_test_filter = b.option(bool, "runtime-test-filter", "Build inference tests once and filter them at runtime") orelse false,
    };
    const inference_wasm_target = @import("../../pkg/inference/build/wasm.zig").resolveTarget(inference_workflow);
    const inference_wasm_jinja = b.createModule(.{
        .root_source_file = b.path("lib/jinja/src/jinja.zig"),
        .target = inference_wasm_target,
        .optimize = .safe,
    });
    const inference_wasm_platform = platform_build.createModule(b, .{
        .root_source_file = b.path("lib/platform/src/root.zig"),
        .filesystem_capacity_source_file = b.path("lib/platform/src/filesystem_capacity.c"),
        .target = inference_wasm_target,
        .optimize = .safe,
        .link_libc = false,
    });
    const inference_steps = @import("../../pkg/inference/build/integration.zig").add(inference_workflow, inference_wasm_jinja, inference_wasm_platform);

    const cancellation_mod = b.createModule(.{
        .root_source_file = b.path("lib/runtime/src/cancellation.zig"),
        .target = target,
        .optimize = optimize,
    });
    credentials_mod.addImport("httpx", httpx_mod);
    credentials_mod.addImport("antfly_cancellation", cancellation_mod);
    credentials_mod.link_libc = link_libc;
    const aws_tests_mod = b.createModule(.{
        .root_source_file = b.path("lib/credentials/src/aws.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = link_libc,
    });
    aws_tests_mod.addImport("httpx", httpx_mod);
    aws_tests_mod.addImport("antfly_cancellation", cancellation_mod);
    const aws_tests = b.addTest(.{ .root_module = aws_tests_mod });
    b.step("aws-credentials-test", "Test shared AWS discovery and credential cache ownership")
        .dependOn(&b.addRunArtifact(aws_tests).step);
    const cache_budget_mod = b.createModule(.{
        .root_source_file = b.path("lib/runtime/src/cache_budget.zig"),
        .target = target,
        .optimize = optimize,
    });
    cache_budget_mod.addImport("antfly_platform", platform_mod);
    const runtime_abi_mod = b.createModule(.{
        .root_source_file = b.path("lib/runtime/src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    runtime_abi_mod.addImport("httpx", httpx_mod);
    runtime_abi_mod.addImport("antfly_platform", platform_mod);
    const runtime_fs_mod = b.createModule(.{
        .root_source_file = b.path("lib/runtime/src/fs.zig"),
        .target = target,
        .optimize = optimize,
    });
    runtime_fs_mod.addImport("antfly_platform", platform_mod);
    const provision_contract_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/metadata/provision_contract.zig"),
        .target = target,
        .optimize = optimize,
    });
    const read_state_observer_mod = b.createModule(.{
        .root_source_file = b.path("lib/raft/src/read_state_observer.zig"),
        .target = target,
        .optimize = optimize,
    });
    read_state_observer_mod.addImport("raft_engine", raft_engine_mod);
    const private_error_diagnostics_mod = b.createModule(.{
        .root_source_file = b.path("lib/runtime/src/private_error_diagnostics.zig"),
        .target = target,
        .optimize = optimize,
    });
    private_error_diagnostics_mod.addImport("antfly_platform", platform_mod);
    const inference_bridge_mod = b.createModule(.{
        .root_source_file = b.path("pkg/inference/src/host/bridge.zig"),
        .target = target,
        .optimize = optimize,
    });
    inference_bridge_mod.addImport("antfly_runtime_abi", runtime_abi_mod);
    inference_bridge_mod.addImport("antfly_image", image_mod);
    const inference_provider_failure_mod = b.createModule(.{
        .root_source_file = b.path("pkg/inference/src/host/provider_failure.zig"),
        .target = target,
        .optimize = optimize,
    });
    inference_provider_failure_mod.addImport("antfly_inference_bridge", inference_bridge_mod);
    inference_provider_failure_mod.addImport("antfly_private_error_diagnostics", private_error_diagnostics_mod);
    inference_provider_failure_mod.addImport("antfly_platform", platform_mod);
    const public_limits_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/api/public_limits.zig"),
        .target = target,
        .optimize = optimize,
    });
    const template_content_mod = b.createModule(.{
        .root_source_file = b.path("lib/template/src/content_part.zig"),
        .target = target,
        .optimize = optimize,
    });
    const sparse_embedding_mod = b.createModule(.{
        .root_source_file = b.path("pkg/inference/src/host/sparse_embedding.zig"),
        .target = target,
        .optimize = optimize,
    });
    const inference_worker_rpc_mod = b.createModule(.{
        .root_source_file = b.path("pkg/inference/src/host/worker_rpc.zig"),
        .target = target,
        .optimize = optimize,
    });
    inference_worker_rpc_mod.addImport("httpx", httpx_mod);
    const inference_embedding_wire_mod = b.createModule(.{
        .root_source_file = b.path("pkg/inference/src/host/embedding_wire.zig"),
        .target = target,
        .optimize = optimize,
    });
    inference_embedding_wire_mod.addImport("httpx", httpx_mod);
    const inference_types_mod = b.createModule(.{
        .root_source_file = b.path("pkg/inference/src/host/types.zig"),
        .target = target,
        .optimize = optimize,
    });
    inference_types_mod.addImport("antfly_generating", generating_mod);
    const inference_work_mod = b.createModule(.{
        .root_source_file = b.path("pkg/inference/src/host/work.zig"),
        .target = target,
        .optimize = optimize,
    });
    inference_work_mod.addImport("antfly_scraping", scraping_mod);
    inference_work_mod.addImport("antfly_image", image_mod);
    const inference_worker_wire_mod = b.createModule(.{
        .root_source_file = b.path("pkg/inference/src/host/worker_wire.zig"),
        .target = target,
        .optimize = optimize,
    });
    inference_worker_wire_mod.addImport("antfly_inference_bridge", inference_bridge_mod);
    inference_worker_wire_mod.addImport("antfly_runtime_abi", runtime_abi_mod);
    inference_worker_wire_mod.addImport("httpx", httpx_mod);
    inference_worker_wire_mod.addImport("antfly_inference_worker_rpc", inference_worker_rpc_mod);
    inference_worker_wire_mod.addImport("antfly_inference_work", inference_work_mod);
    inference_worker_wire_mod.addImport("antfly_public_limits", public_limits_mod);
    const inference_openai_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/inference/providers/openai.zig"),
        .target = target,
        .optimize = optimize,
    });
    inference_openai_mod.addImport("httpx", httpx_mod);
    inference_openai_mod.addImport("openai_api", openai_api_mod);
    inference_openai_mod.addImport("antfly_inference_types", inference_types_mod);
    const inference_provider_defaults_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/inference/providers/provider_defaults.zig"),
        .target = target,
        .optimize = optimize,
    });
    const inference_bedrock_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/inference/providers/bedrock.zig"),
        .target = target,
        .optimize = optimize,
    });
    inference_bedrock_mod.addImport("antfly_cancellation", cancellation_mod);
    inference_bedrock_mod.addImport("httpx", httpx_mod);
    inference_bedrock_mod.addImport("antfly_inference_types", inference_types_mod);
    inference_bedrock_mod.addImport("antfly_inference_work", inference_work_mod);
    inference_bedrock_mod.addImport("antfly_credentials", credentials_mod);
    inference_bedrock_mod.addImport("antfly_inference_provider_defaults", inference_provider_defaults_mod);
    inference_bedrock_mod.addImport("antfly_template_content", template_content_mod);
    const inference_local_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/inference/providers/local.zig"),
        .target = target,
        .optimize = optimize,
    });
    inference_local_mod.addImport("antfly_cancellation", cancellation_mod);
    inference_local_mod.addImport("httpx", httpx_mod);
    inference_local_mod.addImport("inference_api", inference_api_mod);
    inference_local_mod.addImport("antfly_inference_types", inference_types_mod);
    inference_local_mod.addImport("antfly_inference_work", inference_work_mod);
    inference_local_mod.addImport("antfly_template_content", template_content_mod);
    const inference_vertex_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/inference/providers/vertex.zig"),
        .target = target,
        .optimize = optimize,
    });
    inference_vertex_mod.addImport("antfly_scraping", scraping_mod);
    inference_vertex_mod.addImport("httpx", httpx_mod);
    inference_vertex_mod.addImport("antfly_google", google_mod);
    inference_vertex_mod.addImport("antfly_inference_types", inference_types_mod);
    inference_vertex_mod.addImport("antfly_inference_provider_defaults", inference_provider_defaults_mod);
    const inference_list_models_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/inference/providers/list_models.zig"),
        .target = target,
        .optimize = optimize,
    });
    inference_list_models_mod.addImport("httpx", httpx_mod);
    inference_list_models_mod.addImport("antfly_inference_bedrock", inference_bedrock_mod);
    inference_list_models_mod.addImport("antfly_inference_vertex", inference_vertex_mod);
    inference_list_models_mod.addImport("antfly_inference_provider_defaults", inference_provider_defaults_mod);
    const inference_remote_capabilities_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/inference/remote_capabilities.zig"),
        .target = target,
        .optimize = optimize,
    });
    inference_remote_capabilities_mod.addImport("antfly_platform", platform_mod);
    inference_remote_capabilities_mod.addImport("httpx", httpx_mod);
    inference_remote_capabilities_mod.addImport("antfly_inference_work", inference_work_mod);
    inference_remote_capabilities_mod.addImport("antfly_cancellation", cancellation_mod);
    const inference_execution_control_mod = b.createModule(.{
        .root_source_file = b.path("pkg/inference/src/host/execution_control.zig"),
        .target = target,
        .optimize = optimize,
    });
    inference_execution_control_mod.addImport("antfly_platform", platform_mod);
    inference_execution_control_mod.addImport("antfly_cancellation", cancellation_mod);
    const inference_execution_context_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/inference/execution_context.zig"),
        .target = target,
        .optimize = optimize,
    });
    inference_execution_context_mod.addImport("antfly_platform", platform_mod);
    inference_execution_context_mod.addImport("httpx", httpx_mod);
    inference_execution_context_mod.addImport("antfly_inference_remote_capabilities", inference_remote_capabilities_mod);
    inference_execution_context_mod.addImport("antfly_cancellation", cancellation_mod);
    inference_execution_context_mod.addImport("antfly_inference_execution_control", inference_execution_control_mod);
    const inference_request_types_mod = b.createModule(.{
        .root_source_file = b.path("pkg/inference/src/host/request_types.zig"),
        .target = target,
        .optimize = optimize,
    });
    inference_request_types_mod.addImport("antfly_inference_execution_control", inference_execution_control_mod);
    const inference_runtime_paths_mod = b.createModule(.{
        .root_source_file = b.path("pkg/inference/src/host/runtime_paths.zig"),
        .target = target,
        .optimize = optimize,
    });
    inference_runtime_paths_mod.addImport("antfly_platform", platform_mod);
    const inference_query_embedding_cache_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/inference/providers/query_embedding_cache.zig"),
        .target = target,
        .optimize = optimize,
    });
    inference_query_embedding_cache_mod.addImport("antfly_cache_budget", cache_budget_mod);
    inference_query_embedding_cache_mod.addImport("antfly_platform", platform_mod);
    const inference_host_mod = b.createModule(.{
        .root_source_file = b.path("pkg/inference/src/host/host.zig"),
        .target = target,
        .optimize = optimize,
    });
    inference_host_mod.addImport("httpx", httpx_mod);
    readers_mod.addImport("apple_reader_options", inference_graph.apple_native_mod.import_table.get("apple_native_options").?);
    readers_mod.addImport("antfly_inference_work", inference_work_mod);
    readers_mod.addImport("antfly_platform", platform_mod);
    generating_mod.addImport("antfly_apple_native", inference_graph.apple_native_mod);
    inference_host_mod.addImport("antfly_readers", readers_mod);
    inference_host_mod.addImport("antfly_transcribing", transcribing_mod);
    inference_host_mod.addImport("antfly_extracting", extracting_mod);
    inference_host_mod.addImport("antfly_scraping", scraping_mod);
    inference_host_mod.addImport("antfly_template_content", template_content_mod);
    inference_host_mod.addImport("antfly_sparse_embedding", sparse_embedding_mod);
    inference_host_mod.addImport("antfly_inference_types", inference_types_mod);
    inference_host_mod.addImport("antfly_inference_work", inference_work_mod);
    inference_host_mod.addImport("antfly_inference_request_types", inference_request_types_mod);
    inference_host_mod.addImport("antfly_inference_execution_control", inference_execution_control_mod);
    inference_host_mod.addImport("antfly_inference_runtime_paths", inference_runtime_paths_mod);
    inference_host_mod.addImport("inference_server", inference_server_mod);
    inference_host_mod.addImport("antfly_inference_bridge", inference_bridge_mod);
    inference_host_mod.addImport("antfly_runtime_abi", runtime_abi_mod);
    inference_host_mod.addImport("antfly_platform", platform_mod);
    inference_host_mod.addImport("inference_api", inference_api_mod);
    inference_host_mod.addImport("inference_chunker", inference_chunker_mod);
    inference_host_mod.addImport("antfly_chunking", chunking_mod);
    inference_host_mod.addImport("antfly_inference_worker_rpc", inference_worker_rpc_mod);
    inference_host_mod.addImport("antfly_inference_worker_wire", inference_worker_wire_mod);
    inference_host_mod.addImport("antfly_inference_provider_failure", inference_provider_failure_mod);
    const inference_openai_tests = b.addTest(.{ .root_module = inference_openai_mod });
    b.step("antfly-embedded-inference-openai-test", "Run embedded inference OpenAI provider tests")
        .dependOn(&b.addRunArtifact(inference_openai_tests).step);
    const embedded_inference_providers_test_step = b.step("antfly-embedded-inference-providers-test", "Run embedded inference provider and discovery tests");
    inline for (.{
        inference_provider_defaults_mod,
        inference_bedrock_mod,
        inference_local_mod,
        inference_vertex_mod,
        inference_list_models_mod,
        inference_remote_capabilities_mod,
        inference_execution_context_mod,
    }) |module| {
        embedded_inference_providers_test_step.dependOn(&b.addRunArtifact(b.addTest(.{ .root_module = module })).step);
    }
    const antfly_imports = AntflyRootImports{
        .sql_parser = sql_parser_mod,
        .storage_boundary = @import("../embedded/storage_boundary.zig").create(b, target, optimize),
        .cancellation = cancellation_mod,
        .cache_budget = cache_budget_mod,
        .runtime_abi = runtime_abi_mod,
        .runtime_fs = runtime_fs_mod,
        .provision_contract = provision_contract_mod,
        .read_state_observer = read_state_observer_mod,
        .private_error_diagnostics = private_error_diagnostics_mod,
        .inference_bridge = inference_bridge_mod,
        .inference_provider_failure = inference_provider_failure_mod,
        .public_limits = public_limits_mod,
        .template_content = template_content_mod,
        .sparse_embedding = sparse_embedding_mod,
        .inference_worker_wire = inference_worker_wire_mod,
        .inference_worker_rpc = inference_worker_rpc_mod,
        .inference_embedding_wire = inference_embedding_wire_mod,
        .inference_types = inference_types_mod,
        .inference_work = inference_work_mod,
        .inference_openai = inference_openai_mod,
        .inference_provider_defaults = inference_provider_defaults_mod,
        .inference_bedrock = inference_bedrock_mod,
        .inference_local = inference_local_mod,
        .inference_list_models = inference_list_models_mod,
        .inference_vertex = inference_vertex_mod,
        .inference_remote_capabilities = inference_remote_capabilities_mod,
        .inference_execution_context = inference_execution_context_mod,
        .inference_request_types = inference_request_types_mod,
        .inference_runtime_paths = inference_runtime_paths_mod,
        .inference_query_embedding_cache = inference_query_embedding_cache_mod,
        .inference_host = inference_host_mod,
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
        .public_server_openapi = public_server_openapi_mod,
        .client_openapi = client_openapi_mod,
        .schema_openapi = schema_openapi_mod,
        .indexes_openapi = indexes_openapi_mod,
        .sort_openapi = sort_openapi_mod,
        .generating_api_openapi = generating_api_openapi_mod,
        .websearch_openapi = openapi_modules.websearch,
        .eval_openapi = eval_openapi_mod,
        .query_openapi = query_openapi_mod,
        .admin_openapi = admin_openapi_mod,
        .internal_openapi = internal_openapi_mod,
        .metadata_openapi = metadata_openapi_mod,
        .metadata_server_openapi = metadata_server_openapi_mod,
        .usermgr_openapi = usermgr_openapi_mod,
        .usermgr_server_openapi = usermgr_server_openapi_mod,
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
        .fst = fst_mod,
        .regex = regex_mod,
        .json = json_mod,
        .jsonschema = jsonschema_mod,
        .mcp = mcp_mod,
        .toon = toon_mod,
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
        .exa_api = exa_api_mod,
        .tavily_api = tavily_api_mod,
        .handlebars = handlebars_mod,
        .inference_server = inference_server_mod,
        .prometheus = prometheus_mod,
        .structlog = structlog_mod,
        .platform = platform_mod,
        .platform_link_libc = link_libc,
        .platform_target = target,
        .filesystem_capacity_source_file = b.path("lib/platform/src/filesystem_capacity.c"),
    };
    // SQL shape fixtures reach native schema and storage contracts, but do not
    // need the inference/API module graph of a full storage owner.
    antfly_imports.configureRuntimeContracts(usermgr_mod);
    antfly_imports.storage_boundary.configureSources(sql_test_mod, false, false);
    sql_test_mod.addImport("sql_parser", sql_parser_mod);
    sql_test_mod.addImport("antfly_platform", platform_mod);
    sql_test_mod.addImport("antfly_schema_openapi", schema_openapi_mod);
    sql_test_mod.addImport("antfly_regex", regex_mod);
    sql_test_mod.addImport("antfly_hash", hash_mod);
    sql_test_mod.addImport("bloom", bloom_mod);
    sql_test_mod.link_libc = link_libc;
    const refinement_bench_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/sql_refinement_bench_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    antfly_imports.storage_boundary.configureSources(refinement_bench_mod, false, false);
    refinement_bench_mod.addImport("sql_parser", sql_parser_mod);
    refinement_bench_mod.addImport("antfly_platform", platform_mod);
    refinement_bench_mod.addImport("antfly_schema_openapi", schema_openapi_mod);
    refinement_bench_mod.addImport("antfly_regex", regex_mod);
    refinement_bench_mod.addImport("antfly_hash", hash_mod);
    refinement_bench_mod.addImport("bloom", bloom_mod);
    refinement_bench_mod.link_libc = link_libc;
    const refinement_bench = b.addTest(.{ .root_module = refinement_bench_mod, .filters = &.{"native refinements benchmark"} });
    b.step("sql-native-refinement-bench", "Compare native expression, aggregate and window spill refinements").dependOn(&b.addRunArtifact(refinement_bench).step);
    const pipeline_bench = b.addTest(.{ .root_module = refinement_bench_mod, .filters = &.{"native pipeline refinements benchmark"} });
    b.step("sql-native-pipeline-bench", "Compare shared typed DAGs and block result delivery").dependOn(&b.addRunArtifact(pipeline_bench).step);

    antfly_imports.storage_boundary.configureSources(storage_mod, false, false);
    var production_antfly_imports = antfly_imports;
    production_antfly_imports.build_options = production_build_options;

    return .{
        .api_bench_standalone = api_bench_standalone,
        .conformance_fetch = conformance_fetch,
        .conformance_fixtures = conformance_fixtures,
        .target = target,
        .optimize = optimize,
        .vopr_mod = vopr_mod,
        .strip = strip,
        .lmdb_backend = lmdb_backend,
        .lmdb_evented_async_io = lmdb_evented_async_io,
        .with_tla = with_tla,
        .link_libc = link_libc,
        .sanitize_thread = sanitize_thread,
        .runtime_artifact_role = runtime_artifact_role,
        .antfly_bin_name = antfly_bin_name,
        .inference_enable_onnx = inference_enable_onnx,
        .inference_enable_metal = inference_enable_metal,
        .inference_enable_cuda = inference_enable_cuda,
        .build_info = build_info,
        .antfly_version = antfly_version,
        .platform_test_step = platform_test_step,
        .build_options = build_options,
        .standalone_runtime_build_options = standalone_runtime_build_options,
        .production_build_options = production_build_options,
        .lmdb_engine_mod = lmdb_engine_mod,
        .raft_engine_mod = raft_engine_mod,
        .json_mod = json_mod,
        .httpx_mod = httpx_mod,
        .structlog_mod = structlog_mod,
        .run_yacc_tests = run_yacc_tests,
        .yacc_steps = yacc_steps,
        .run_sql_tests = run_sql_tests,
        .run_pgwire_tests = run_pgwire_tests,
        .openapi_root_check = openapi_root_check,
        .openapi_docs_test = openapi_docs_test,
        .protobuf_mod = protobuf_mod,
        .platform_mod = platform_mod,
        .objectstore_mod = objectstore_mod,
        .google_mod = google_mod,
        .vector_mod = vector_mod,
        .hash_mod = hash_mod,
        .hash_bench_mod = hash_bench_mod,
        .vectorindex_mod = vectorindex_mod,
        .casbin_mod = casbin_mod,
        .usermgr_mod = usermgr_mod,
        .fst_mod = fst_mod,
        .regex_mod = regex_mod,
        .jsonschema_mod = jsonschema_mod,
        .toon_mod = toon_mod,
        .mcp_mod = mcp_mod,
        .a2a_mod = a2a_mod,
        .matcher_mod = matcher_mod,
        .resolver_mod = resolver_mod,
        .generating_mod = generating_mod,
        .chunking_mod = chunking_mod,
        .embeddings_mod = embeddings_mod,
        .scraping_mod = scraping_mod,
        .reranking_mod = reranking_mod,
        .extracting_mod = extracting_mod,
        .image_mod = image_mod,
        .pdf_standard_fonts_mod = pdf_standard_fonts_mod,
        .font_mod = font_mod,
        .pdf_mod = pdf_mod,
        .sentencepiece_proto_source = sentencepiece_proto_source,
        .inference_ml_mod = inference_ml_mod,
        .ml_tabular_mod = ml_tabular_mod,
        .inference_onnx = inference_onnx,
        .tokenizer = tokenizer,
        .inference_graph = inference_graph,
        .run_hf_tokenizer_tests = run_hf_tokenizer_tests,
        .transcribing_mod = transcribing_mod,
        .readers_mod = readers_mod,
        .inference_steps = inference_steps,
        .antfly_imports = antfly_imports,
        .production_antfly_imports = production_antfly_imports,
    };
}

fn buildArguments(b: *std.Build) ?[]const []const u8 {
    if (!b.available_options_map.contains("test-filter"))
        return b.option([]const []const u8, "test-filter", "Compile-time test filters (runtime filters follow --)");
    const input = b.user_input_options.get("test-filter") orelse return null;
    return switch (input) {
        .scalar => |value| blk: {
            const values = b.allocator.alloc([]const u8, 1) catch @panic("OOM");
            values[0] = value;
            break :blk values;
        },
        .list => |values| values.items,
        else => null,
    };
}
