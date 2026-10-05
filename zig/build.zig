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

const pkg_antfly_build_codegen = @import("build_support/openapi.zig");
const addOpenApiRootCheckStep = pkg_antfly_build_codegen.addOpenApiRootCheckStep;
const addOpenApiSourceSteps = pkg_antfly_build_codegen.addOpenApiSourceSteps;

const pkg_antfly_build_runtime = @import("pkg/antfly/build/runtime.zig");
const RuntimeArtifactRole = @import("build_support/antfly/runtime_roles.zig").RuntimeArtifactRole;
const RuntimeLibraryUnit = pkg_antfly_build_runtime.RuntimeLibraryUnit;

const lib_platform_build_support = @import("lib/platform/build_support.zig");
const addMacosSdkPaths = lib_platform_build_support.addMacosSdkPaths;

const pkg_antfly_build_tests = @import("pkg/antfly/build/tests.zig");

const addRuntimeTestFilters = pkg_antfly_build_tests.addRuntimeTestFilters;
const addFilteredTestRunArtifact = pkg_antfly_build_tests.addFilteredTestRunArtifact;
const dependOnAll = pkg_antfly_build_tests.dependOnAll;
const assignDefaultAggregateMaxRss = pkg_antfly_build_tests.assignDefaultAggregateMaxRss;

const pkg_antfly_build_imports = @import("build_support/antfly/imports.zig");
const AntflyRootImports = pkg_antfly_build_imports.AntflyRootImports;

const pkg_antfly_build_snowball = @import("pkg/antfly-embedded/build/snowball.zig");

const builtin = @import("builtin");
const antfly_benches_build = @import("pkg/antfly/build/benches.zig");
const antfly_embedded_build = @import("pkg/antfly-embedded/build/embedded.zig");
const antfly_storage_build = @import("pkg/antfly-embedded/build/storage.zig");
const antfly_tests_build = @import("pkg/antfly/build/tests.zig");
const inference_runtime_build = @import("pkg/inference/build/runtime.zig");
const platform_build = @import("lib/platform/build_support.zig");

const LmdbBackend = antfly_storage_build.LmdbBackend;
const makeLmdbBuildOptions = antfly_storage_build.makeLmdbBuildOptions;
const makeLmdbEngineModule = antfly_storage_build.makeLmdbEngineModule;
const makeRootBuildOptions = antfly_storage_build.makeRootBuildOptions;
const selectTestFilters = antfly_tests_build.selectTestFilters;

pub fn build(b: *std.Build) void {
    _ = create(b);
}

pub const Artifacts = struct {
    inference_steps: @import("pkg/inference/build/integration.zig").Steps,
    runtime: antfly_runtime_build.AddRuntimeResult,
    inference: inference_runtime_build.Graph,
    wasm: *std.Build.Step.Compile,
};

/// Compose owners once. Consumers of this constructor can inspect the same
/// artifacts used by public targets without maintaining a second build graph.
pub fn create(b: *std.Build) ?Artifacts {
    defer @import("pkg/antfly-embedded/build/source_owner.zig").finalize(b);
    const shared = @import("build_support/antfly/dependencies.zig").create(b) orelse return null;
    const api_bench_standalone = shared.api_bench_standalone;
    const conformance_fetch = shared.conformance_fetch;
    const conformance_fixtures = shared.conformance_fixtures;
    const target = shared.target;
    const optimize = shared.optimize;
    const vopr_mod = shared.vopr_mod;
    const strip = shared.strip;
    const lmdb_backend = shared.lmdb_backend;
    const lmdb_evented_async_io = shared.lmdb_evented_async_io;
    const with_tla = shared.with_tla;
    const link_libc = shared.link_libc;
    const sanitize_thread = shared.sanitize_thread;
    const runtime_artifact_role = shared.runtime_artifact_role;
    const antfly_bin_name = shared.antfly_bin_name;
    const inference_enable_onnx = shared.inference_enable_onnx;
    const inference_enable_metal = shared.inference_enable_metal;
    const inference_enable_cuda = shared.inference_enable_cuda;
    const build_info = shared.build_info;
    const platform_test_step = shared.platform_test_step;
    const standalone_runtime_build_options = shared.standalone_runtime_build_options;
    const production_build_options = shared.production_build_options;
    const lmdb_engine_mod = shared.lmdb_engine_mod;
    const raft_engine_mod = shared.raft_engine_mod;
    const json_mod = shared.json_mod;
    const httpx_mod = shared.httpx_mod;
    const structlog_mod = shared.structlog_mod;
    const run_yacc_tests = shared.run_yacc_tests;
    const yacc_steps = shared.yacc_steps;
    const run_sql_tests = shared.run_sql_tests;
    const run_pgwire_tests = shared.run_pgwire_tests;
    const openapi_root_check = shared.openapi_root_check;
    const openapi_docs_test = shared.openapi_docs_test;
    const protobuf_mod = shared.protobuf_mod;
    const platform_mod = shared.platform_mod;
    const objectstore_mod = shared.objectstore_mod;
    const google_mod = shared.google_mod;
    const vector_mod = shared.vector_mod;
    const hash_mod = shared.hash_mod;
    const hash_bench_mod = shared.hash_bench_mod;
    const vectorindex_mod = shared.vectorindex_mod;
    const casbin_mod = shared.casbin_mod;
    const usermgr_mod = shared.usermgr_mod;
    const fst_mod = shared.fst_mod;
    const regex_mod = shared.regex_mod;
    const jsonschema_mod = shared.jsonschema_mod;
    const toon_mod = shared.toon_mod;
    const mcp_mod = shared.mcp_mod;
    const a2a_mod = shared.a2a_mod;
    const matcher_mod = shared.matcher_mod;
    const resolver_mod = shared.resolver_mod;
    const generating_mod = shared.generating_mod;
    const chunking_mod = shared.chunking_mod;
    const embeddings_mod = shared.embeddings_mod;
    const scraping_mod = shared.scraping_mod;
    const reranking_mod = shared.reranking_mod;
    const extracting_mod = shared.extracting_mod;
    const image_mod = shared.image_mod;
    const pdf_standard_fonts_mod = shared.pdf_standard_fonts_mod;
    const font_mod = shared.font_mod;
    const pdf_mod = shared.pdf_mod;
    const sentencepiece_proto_source = shared.sentencepiece_proto_source;
    const inference_ml_mod = shared.inference_ml_mod;
    const ml_tabular_mod = shared.ml_tabular_mod;
    const inference_onnx = shared.inference_onnx;
    const inference_graph = shared.inference_graph;
    const run_hf_tokenizer_tests = shared.run_hf_tokenizer_tests;
    const transcribing_mod = shared.transcribing_mod;
    const readers_mod = shared.readers_mod;
    const inference_steps = shared.inference_steps;
    const antfly_imports = shared.antfly_imports;
    const production_antfly_imports = shared.production_antfly_imports;
    production_antfly_imports.configureRuntimeContracts(usermgr_mod);
    production_antfly_imports.storage_boundary.configureSources(usermgr_mod, false, false);
    const onnx_build = @import("onnx_graph").support;

    // The public package has the same storage boundary as the linked server:
    // LMDB is retained only by explicitly configured test/benchmark modules.
    const antfly_mod = b.addModule("antfly-zig", .{
        .root_source_file = b.path("pkg/antfly/src/root.zig"),
        .target = target,
        .optimize = optimize,
        .sanitize_thread = sanitize_thread,
    });
    production_antfly_imports.configure(b, antfly_mod, link_libc);
    antfly_mod.addImport("vopr", vopr_mod);
    antfly_mod.addImport("antfly_openapi_specs", antfly_imports.embedded_openapi);

    const wasm = @import("pkg/antfly-embedded/build/wasm.zig").add(b, sentencepiece_proto_source);
    const wasm_step = b.step("wasm", "Build and install the unified Antfly WASM bundle");
    dependOnAll(wasm_step, wasm.install);
    wasm.smoke.step.dependOn(wasm_step);
    b.step("wasm-test", "Build the Antfly WASM bundle and run its Node smoke test").dependOn(&wasm.smoke.step);
    const embedded = antfly_embedded_build.addEmbedded(b, .{
        .server_integration_tests = true,
        .lmdb_engine = lmdb_engine_mod,
        .vopr = vopr_mod,
        .optimize = optimize,
        .strip = strip,
        .antfly_imports = production_antfly_imports,
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
    const run_capi_conformance = embedded.run_capi_conformance;
    const run_lite_go_tests = embedded.run_lite_go_tests;
    const run_lite_py_tests = embedded.run_lite_py_tests;
    const run_lite_rs_tests = embedded.run_lite_rs_tests;
    const run_lite_ts_tests = embedded.run_lite_ts_tests;
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

    const lib_ml_tests = b.addTest(.{ .root_module = inference_ml_mod });
    const run_lib_ml_tests = b.addRunArtifact(lib_ml_tests);
    const lib_ml_test_step = b.step("lib-ml-test", "Run standalone lib/ml graph and optimizer tests");
    lib_ml_test_step.dependOn(&run_lib_ml_tests.step);

    const lib_ml_tabular_tests = b.addTest(.{
        .root_module = ml_tabular_mod,
    });
    const run_lib_ml_tabular_tests = b.addRunArtifact(lib_ml_tabular_tests);
    const lib_ml_tabular_test_step = b.step("lib-ml-tabular-test", "Run standalone lib/ml/tabular tests");
    lib_ml_tabular_test_step.dependOn(&run_lib_ml_tabular_tests.step);

    const onnx_tests = onnx_build.createTests(b, inference_onnx, .{
        .path = b.path("pkg/antfly-embedded/src/local/test_runner.zig"),
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
        .filters = &.{ "request gate", "request watchdog", "request task admission", "request cancellation before socket publication", "successful H1 requests do not wait" },
    });
    const run_httpx_client_lifecycle_tests = b.addRunArtifact(httpx_client_lifecycle_tests);
    b.step("lib-httpx-client-lifecycle-test", "Run HTTP client admission, release, and shutdown contracts").dependOn(&run_httpx_client_lifecycle_tests.step);
    // The complete HTTP library artifact already owns these lifecycle tests.

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
        .filters = buildArguments(b) orelse &.{},
    });
    const run_lib_hash_tests = b.addRunArtifact(lib_hash_tests);
    const lib_hash_test_step = b.step("lib-hash-test", "Run standalone lib/hash tests");
    lib_hash_test_step.dependOn(&run_lib_hash_tests.step);

    const lib_vectorindex_tests = b.addTest(.{
        .root_module = vectorindex_mod,
        .filters = buildArguments(b) orelse &.{},
    });
    const run_lib_vectorindex_tests = b.addRunArtifact(lib_vectorindex_tests);
    const lib_vectorindex_test_step = b.step("lib-vectorindex-test", "Run standalone lib/vectorindex tests");
    lib_vectorindex_test_step.dependOn(&run_lib_vectorindex_tests.step);

    const vector_kernel_mod = b.createModule(.{ .root_source_file = b.path("lib/vector/src/quantizer.zig"), .target = target, .optimize = optimize });
    vector_kernel_mod.addImport("protobuf", protobuf_mod);
    const vector_kernel_tests = b.addTest(.{ .root_module = vector_kernel_mod, .filters = buildArguments(b) orelse &.{} });
    const run_vector_kernel_tests = b.addRunArtifact(vector_kernel_tests);
    b.step("lib-vector-kernel-test", "Run standalone quantizer kernel tests (no external recall fixtures)").dependOn(&run_vector_kernel_tests.step);

    const packing_bench_mod = b.createModule(.{ .root_source_file = b.path("tools/bench_query_packing.zig"), .target = target, .optimize = optimize });
    packing_bench_mod.addImport("antfly_vector", vector_mod);
    const packing_bench = b.addExecutable(.{ .name = "bench-query-packing", .root_module = packing_bench_mod });
    const run_packing_bench = b.addRunArtifact(packing_bench);
    b.step("bench-query-packing", "Compare exact query bitplane packing and complete scoring kernels").dependOn(&run_packing_bench.step);

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

    const pdf_bench_optimize = b.option(std.lang.Optimize, "pdf-optimize", "Optimization for the isolated PDF executable") orelse .fast;
    const pdf_bench_hash = if (pdf_bench_optimize == .fast) hash_bench_mod else if (pdf_bench_optimize == optimize) hash_mod else b.createModule(.{
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
    const pdf_bench_fonts = @import("build_support/antfly/fonts.zig").create(b, target, pdf_bench_optimize);
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
    unit_test_step.dependOn(&b.addRunArtifact(openapi_docs_test).step);
    unit_test_step.dependOn(&run_sql_tests.step);
    unit_test_step.dependOn(&run_pgwire_tests.step);
    unit_test_step.dependOn(&pdf_integration.run.step);
    // HTTP client lifecycle tests belong to lib-test; keep their focused target.
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
    soak_test_step.dependOn(owner_tests.vopr_soak_test_step);
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
    regex_bench_mod.addImport("antfly_fst", fst_mod);
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
        .vopr = vopr_mod,
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
        .cpu_inference = !inference_enable_cuda and !inference_enable_metal and !inference_enable_onnx,
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
    const consumer_test_metadata = @import("lib/build_info/build_support.zig").create(b, .{
        .root = b.path("lib/build_info"),
        .target = target,
        .optimize = optimize,
        .version = "test",
    });
    // Filtered executables can share a root module. Link inputs belong to the
    // module, so attach them once even when several compile steps consume it.
    var linked_consumer_modules: std.AutoHashMapUnmanaged(*std.Build.Module, void) = .empty;
    defer linked_consumer_modules.deinit(b.allocator);
    for (owner_tests.linked_consumer_tests) |tests| {
        const entry = linked_consumer_modules.getOrPut(b.allocator, tests.root_module) catch @panic("OOM");
        if (entry.found_existing) continue;
        tests.root_module.addObject(consumer_test_metadata.object);
        inline for (.{ .storage_kernel, .enrichment_compute, .inference }) |unit|
            tests.root_module.linkLibrary(runtime_library_artifacts[@backingInt(@as(@import("pkg/antfly/build/runtime.zig").RuntimeLibraryUnit, unit))].?);
    }
    const standalone_initial_fk_tests = owner_tests.standalone_initial_fk_tests;
    standalone_initial_fk_tests.root_module.addObject(consumer_test_metadata.object);
    inline for (.{ .storage_kernel, .enrichment_compute, .inference }) |unit|
        standalone_initial_fk_tests.root_module.linkLibrary(runtime_library_artifacts[@backingInt(@as(@import("pkg/antfly/build/runtime.zig").RuntimeLibraryUnit, unit))].?);
    const run_standalone_initial_fk_tests = antfly_tests_build.addFilteredTestRunArtifact(b, standalone_initial_fk_tests);
    b.step("antfly-standalone-initial-fk-test", "Run linked native standalone initial-FK owner publication tests").dependOn(&run_standalone_initial_fk_tests.step);
    const graph_transfer_tests = b.addTest(.{ .root_module = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/graph_transfer_test.zig"),
        .target = target,
        .optimize = optimize,
    }) });
    production_antfly_imports.configureRuntimeContracts(graph_transfer_tests.root_module);
    graph_transfer_tests.root_module.addImport("antfly_hash", hash_mod);
    production_antfly_imports.storage_boundary.configureSources(graph_transfer_tests.root_module, false, false);
    const run_graph_transfer_tests = b.addRunArtifact(graph_transfer_tests);
    b.step("antfly-graph-transfer-test", "Validate certified graph artifact generation transfer").dependOn(&run_graph_transfer_tests.step);
    // The storage lanes already own every named graph-transfer contract.
    // Keep this focused target without compiling a duplicate aggregate image.
    const standalone_policy_ha_tests = owner_tests.standalone_policy_ha_tests;
    standalone_policy_ha_tests.root_module.addObject(consumer_test_metadata.object);
    inline for (.{ .storage_kernel, .enrichment_compute, .inference }) |unit|
        standalone_policy_ha_tests.root_module.linkLibrary(runtime_library_artifacts[@backingInt(@as(@import("pkg/antfly/build/runtime.zig").RuntimeLibraryUnit, unit))].?);
    const run_standalone_policy_ha_tests = antfly_tests_build.addFilteredTestRunArtifact(b, standalone_policy_ha_tests);
    b.step("antfly-standalone-policy-ha-test", "Run native standalone and HA row-policy publication regressions").dependOn(&run_standalone_policy_ha_tests.step);
    // Native activation must remain covered by the existing physical-owner
    // CI gates, not only by developer-invoked focused targets.
    for ([_]*std.Build.Step.Run{ run_standalone_initial_fk_tests, run_standalone_policy_ha_tests }) |run| {
        owner_tests.storage_test_step.dependOn(&run.step);
        owner_tests.integration_test_step.dependOn(&run.step);
    }

    // These three compile real CLI/server boot paths (Lite command tests,
    // the standalone runtime's HA/hot-standby/Lite surface, and the public
    // API parity e2e suite) that reach the real storage-kernel owner through
    // api/kernel_owner_source.zig the same way production does, independent
    // of the control-only source selection most unit tests use. Each already
    // compiles from its own dedicated module (not the shared antfly_test_mod
    // or standalone_runtime_test_mod), so linking the owner archive here
    // reaches only this one compile per fixture.
    for ([_]*std.Build.Step.Compile{
        owner_tests.lite_cmd_tests,
        owner_tests.lib_standalone_runtime_tests,
        owner_tests.public_api_parity_tests,
    }) |tests| {
        const entry = linked_consumer_modules.getOrPut(b.allocator, tests.root_module) catch @panic("OOM");
        if (entry.found_existing) continue;
        tests.root_module.addObject(consumer_test_metadata.object);
        inline for (.{ .storage_kernel, .enrichment_compute, .inference }) |unit|
            tests.root_module.linkLibrary(runtime_library_artifacts[@backingInt(@as(@import("pkg/antfly/build/runtime.zig").RuntimeLibraryUnit, unit))].?);
    }

    const storage_owner_runs = @import("pkg/antfly/build/storage_owner_tests.zig").add(b, target, optimize, production_antfly_imports, vopr_mod, lmdb_engine_mod, runtime_library_artifacts);
    b.step("antfly-storage-owner-test", "Run real compiled storage owner ABI regressions").dependOn(&storage_owner_runs.runs[0].step);
    b.step("antfly-storage-owner-source-test", "Run compiled owner source and callback regressions").dependOn(&storage_owner_runs.runs[1].step);
    for (storage_owner_runs.runs) |run| {
        owner_tests.storage_test_step.dependOn(&run.step);
        owner_tests.integration_test_step.dependOn(&run.step);
    }

    b.top_level_steps.get("antfly-storage-bench").?.step.dependOn(&b.addInstallArtifact(storage_owner_runs.benchmark, .{}).step);

    const antfly_main_tests = runtime.antfly_main_tests;
    const run_antfly_main_tests = b.addRunArtifact(antfly_main_tests);
    addRuntimeTestFilters(b, run_antfly_main_tests, selectTestFilters(b, &.{}));
    const antfly_main_test_step = b.step("antfly-main-test", "Run top-level Antfly CLI tests");
    antfly_main_test_step.dependOn(&run_antfly_main_tests.step);
    unit_test_step.dependOn(&run_antfly_main_tests.step);

    const maintenance_process_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/maintenance_process_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    antfly_imports.configure(b, maintenance_process_mod, link_libc);
    @import("pkg/antfly-embedded/build/storage.zig").configureLmdb(b, maintenance_process_mod, lmdb_engine_mod, true);
    maintenance_process_mod.addImport("vopr", vopr_mod);
    maintenance_process_mod.addImport("antfly_openapi_specs", antfly_imports.embedded_openapi);
    maintenance_process_mod.addImport("antfly_platform", platform_mod);
    maintenance_process_mod.addImport("httpx", httpx_mod);
    const maintenance_process = b.addExecutable(.{
        .name = "maintenance-process-tests",
        .root_module = maintenance_process_mod,
    });
    maintenance_process.root_module.linkLibrary(
        runtime_library_artifacts[@backingInt(RuntimeLibraryUnit.api_kernel)].?,
    );
    const run_maintenance_process = b.addRunArtifact(maintenance_process);
    run_maintenance_process.has_side_effects = true;
    if (@import("lib/platform/build_support.zig").canRunNativeProcess(b, maintenance_process)) {
        integration_test_step.dependOn(&run_maintenance_process.step);
    } else {
        // Child processes execute this same target directly. Keep cross-build
        // coverage without attempting to spawn foreign binaries from a fixture.
        integration_test_step.dependOn(&maintenance_process.step);
    }

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
    // Lite administration shares storage; serving shares the server runtime.
    for ([_]RuntimeLibraryUnit{ .storage_kernel, .distributed, .api_kernel, .enrichment_compute, .inference }) |unit| {
        lite_main.root_module.linkLibrary(runtime_library_artifacts[@backingInt(unit)].?);
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
    run_lite_cli_smoke.addArtifactArg2(lite_main, .{ .make_absolute = true });
    const run_antfly_lite_cli_smoke = b.addRunArtifact(lite_cli_smoke);
    run_antfly_lite_cli_smoke.addArtifactArg2(antfly_main, .{ .make_absolute = true });
    const lite_main_tests = b.addTest(.{
        .root_module = b.createModule(lite_module_options),
        .filters = &.{"lite main compiles"},
        .test_runner = .{
            .path = b.path("pkg/antfly-embedded/src/local/test_runner.zig"),
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
    lite_test_step.dependOn(&run_capi_conformance.step);
    lite_test_step.dependOn(&run_lite_go_tests.step);
    lite_test_step.dependOn(&run_lite_py_tests.step);
    lite_test_step.dependOn(&run_lite_rs_tests.step);
    lite_test_step.dependOn(&run_lite_ts_tests.step);
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

    // The VOPR workflow also selects focused and build-only roots that need
    // not be reachable from `test`. Account for every compile/run before the
    // cgroup-aware wrapper admits parallel work; preserve measured claims.
    for ([_][]const u8{
        "antfly-raft-transport-test",  "standby-vopr-test",                "vopr-runtime-test",
        "restore-admission-vopr-test", "vopr-determinism-audit",           "vopr-build",
        "antfly",                      "antfly-storage-owner-source-test", "antfly-api-hosted-recovery-test",
    }) |name| {
        assignDefaultAggregateMaxRss(
            b,
            &b.top_level_steps.get(name).?.step,
            @as(usize, if (target.result.os.tag == .macos) 10 else 7) * 1024 * 1024 * 1024,
            6 * 1024 * 1024 * 1024,
        );
    }

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
    @import("build_support/antfly/test_support.zig").configureSimpleTestRuns(b, test_step);
    @import("pkg/antfly-embedded/build/source_owner.zig").finalize(b);
    const unit_ownership_baseline = @import("pkg/antfly/build/unit_test_ownership.zig").applyWithSourceOwners(b, unit_test_step, @import("build_support/antfly/test_partitions.zig").consumerFor);
    @import("pkg/antfly/build/unit_test_inventory.zig").add(b, unit_test_step, unit_ownership_baseline, &.{
        lib_test_step,
        &b.top_level_steps.get("inference-test").?.step,
        &b.top_level_steps.get("inference-finetune-test").?.step,
    });
    return .{ .runtime = runtime, .inference = inference_graph, .wasm = wasm.artifact, .inference_steps = inference_steps };
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
