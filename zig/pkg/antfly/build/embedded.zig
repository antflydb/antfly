// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");
const AntflyRootImports = @import("imports.zig").AntflyRootImports;

pub fn configureModule(
    b: *std.Build,
    mod: *std.Build.Module,
    build_options: *std.Build.Step.Options,
    lmdb_engine_mod: *std.Build.Module,
    json_mod: *std.Build.Module,
    public_openapi_mod: *std.Build.Module,
    query_openapi_mod: *std.Build.Module,
    indexes_openapi_mod: *std.Build.Module,
    sort_openapi_mod: *std.Build.Module,
    metadata_openapi_mod: *std.Build.Module,
    reranking_mod: *std.Build.Module,
    objectstore_mod: *std.Build.Module,
    platform_mod: *std.Build.Module,
    chunking_mod: *std.Build.Module,
    bloom_mod: *std.Build.Module,
    vector_mod: *std.Build.Module,
    vectorindex_mod: *std.Build.Module,
    hash_mod: *std.Build.Module,
    vellum_mod: *std.Build.Module,
    regex_mod: *std.Build.Module,
    image_mod: *std.Build.Module,
    font_mod: *std.Build.Module,
    pdf_mod: *std.Build.Module,
    handlebars_mod: *std.Build.Module,
    add_snowball_module: *const fn (*std.Build, *std.Build.Module) void,
) void {
    mod.addOptions("build_options", build_options);
    mod.addImport("lmdb_engine", lmdb_engine_mod);
    mod.addImport("antfly-json", json_mod);
    mod.addImport("antfly_public_openapi", public_openapi_mod);
    mod.addImport("antfly_query_openapi", query_openapi_mod);
    mod.addImport("antfly_indexes_openapi", indexes_openapi_mod);
    mod.addImport("antfly_sort_openapi", sort_openapi_mod);
    mod.addImport("antfly_metadata_openapi", metadata_openapi_mod);
    mod.addImport("antfly_reranking", reranking_mod);
    mod.addImport("objectstore", objectstore_mod);
    mod.addImport("antfly_platform", platform_mod);
    mod.addImport("antfly_chunking", chunking_mod);
    mod.addImport("bloom", bloom_mod);
    mod.addImport("antfly_vector", vector_mod);
    mod.addImport("antfly_vectorindex", vectorindex_mod);
    mod.addImport("antfly_hash", hash_mod);
    mod.addImport("antfly_vellum", vellum_mod);
    mod.addImport("antfly_regex", regex_mod);
    mod.addImport("antfly_image", image_mod);
    mod.addImport("antfly_font", font_mod);
    mod.addImport("antfly_pdf", pdf_mod);
    mod.addImport("handlebars", handlebars_mod);
    add_snowball_module(b, mod);
}
const addMacosSdkPaths = @import("../../../lib/platform/build_support.zig").addMacosSdkPaths;
const addFilteredTestRunArtifact = @import("tests.zig").addFilteredTestRunArtifact;
const addSnowballModule = @import("snowball.zig").addSnowballModule;
const configureEmbeddedModule = @import("embedded.zig").configureModule;
const selectTestFilters = @import("tests.zig").selectTestFilters;

pub const AddEmbeddedOptions = struct {
    optimize: std.builtin.OptimizeMode,
    strip: bool,
    wasm_target: std.Build.ResolvedTarget,
    antfly_version: []const u8,
    lmdb_engine_wasm_mod: *std.Build.Module,
    protobuf_mod: *std.Build.Module,
    wasm_platform_mod: *std.Build.Module,
    wasm_objectstore_mod: *std.Build.Module,
    wasm_vector_mod: *std.Build.Module,
    wasm_hash_mod: *std.Build.Module,
    wasm_vectorindex_mod: *std.Build.Module,
    wasm_bloom_mod: *std.Build.Module,
    wasm_image_mod: *std.Build.Module,
    wasm_pdf_mod: *std.Build.Module,
    wasm_font_mod: *std.Build.Module,
    sentencepiece_proto_mod: *std.Build.Module,
    antfly_imports: AntflyRootImports,
    antfly_mod: *std.Build.Module,
};
pub const AddEmbeddedResult = struct {
    embedded_mod: *std.Build.Module,
    embedded_api_mod: *std.Build.Module,
    antfly_embedded_pkg_mod: *std.Build.Module,
    antfly_embedded_db_pkg_mod: *std.Build.Module,
    antfly_embedded_api_pkg_mod: *std.Build.Module,
    antfly_client_pkg_mod: *std.Build.Module,
    capi_mod: *std.Build.Module,
    libantfly_link_mod: *std.Build.Module,
    install_libantfly: *std.Build.Step.InstallArtifact,
    install_capi_header: *std.Build.Step.InstallFile,
    run_capi_smoke: *std.Build.Step.Run,
    run_lite_go_tests: *std.Build.Step.Run,
    run_lite_go_example: *std.Build.Step.Run,
    run_lite_go_retrieval_template: *std.Build.Step.Run,
    run_cabi_packaging_tests: *std.Build.Step.Run,
    run_capi_tests: *std.Build.Step.Run,
};

pub fn addEmbedded(b: *std.Build, options: AddEmbeddedOptions) AddEmbeddedResult {
    const target = options.antfly_imports.platform_target;
    const optimize = options.optimize;
    const strip = options.strip;
    const wasm_target = options.wasm_target;
    const link_libc = options.antfly_imports.platform_link_libc;
    const antfly_version = options.antfly_version;
    const build_options = options.antfly_imports.build_options;
    const lmdb_engine_mod = options.antfly_imports.lmdb_engine;
    const lmdb_engine_wasm_mod = options.lmdb_engine_wasm_mod;
    const httpx_mod = options.antfly_imports.httpx;
    const structlog_mod = options.antfly_imports.structlog;
    const public_openapi_mod = options.antfly_imports.public_openapi;
    const client_openapi_mod = options.antfly_imports.client_openapi;
    const indexes_openapi_mod = options.antfly_imports.indexes_openapi;
    const sort_openapi_mod = options.antfly_imports.sort_openapi;
    const query_openapi_mod = options.antfly_imports.query_openapi;
    const metadata_openapi_mod = options.antfly_imports.metadata_openapi;
    const handlebars_mod = options.antfly_imports.handlebars;
    const protobuf_mod = options.protobuf_mod;
    const platform_mod = options.antfly_imports.platform;
    const wasm_platform_mod = options.wasm_platform_mod;
    const objectstore_mod = options.antfly_imports.objectstore;
    const wasm_objectstore_mod = options.wasm_objectstore_mod;
    const bloom_mod = options.antfly_imports.bloom;
    const vector_mod = options.antfly_imports.vector;
    const wasm_vector_mod = options.wasm_vector_mod;
    const hash_mod = options.antfly_imports.hash;
    const wasm_hash_mod = options.wasm_hash_mod;
    const vectorindex_mod = options.antfly_imports.vectorindex;
    const wasm_vectorindex_mod = options.wasm_vectorindex_mod;
    const wasm_bloom_mod = options.wasm_bloom_mod;
    const vellum_mod = options.antfly_imports.vellum;
    const regex_mod = options.antfly_imports.regex;
    const json_mod = options.antfly_imports.json;
    const matcher_mod = options.antfly_imports.matcher;
    const resolver_mod = options.antfly_imports.resolver;
    const chunking_mod = options.antfly_imports.chunking;
    const scraping_mod = options.antfly_imports.scraping;
    const reranking_mod = options.antfly_imports.reranking;
    const image_mod = options.antfly_imports.image;
    const pdf_mod = options.antfly_imports.pdf;
    const font_mod = options.antfly_imports.font;
    const wasm_image_mod = options.wasm_image_mod;
    const wasm_pdf_mod = options.wasm_pdf_mod;
    const wasm_font_mod = options.wasm_font_mod;
    const sentencepiece_proto_mod = options.sentencepiece_proto_mod;
    const transcribing_mod = options.antfly_imports.transcribing;
    const reader_config_mod = options.antfly_imports.reader_config;
    const antfly_imports = options.antfly_imports;
    const antfly_mod = options.antfly_mod;
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
        hash_mod,
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
    embedded_support_mod.addImport("antfly_resolver", resolver_mod);
    embedded_support_mod.addImport("antfly_matcher", matcher_mod);
    embedded_support_mod.addImport("antfly_reader_config", reader_config_mod);
    embedded_support_mod.addImport("antfly_transcribing", transcribing_mod);

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
        wasm_hash_mod,
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

    // --- Inference WASM modules for unified antfly.wasm ---
    const inference_wasm_build_options = b.addOptions();
    inference_wasm_build_options.addOption(bool, "enable_onnx", false);
    inference_wasm_build_options.addOption(bool, "enable_pjrt", false);
    inference_wasm_build_options.addOption(bool, "enable_cuda", false);
    inference_wasm_build_options.addOption([]const u8, "cuda_artifacts", "portable");
    inference_wasm_build_options.addOption(bool, "enable_metal", false);
    inference_wasm_build_options.addOption(bool, "enable_native", false);
    inference_wasm_build_options.addOption(bool, "enable_system_blas", false);
    inference_wasm_build_options.addOption(bool, "enable_wasm", true);
    inference_wasm_build_options.addOption(bool, "enable_webgpu", true);
    inference_wasm_build_options.addOption(bool, "enable_ffmpeg_audio", false);
    inference_wasm_build_options.addOption(bool, "link_libc", false);
    inference_wasm_build_options.addOption(bool, "skip_openapi", false);
    inference_wasm_build_options.addOption([]const u8, "inference_version", antfly_version);
    inference_wasm_build_options.addOption([]const u8, "wasm_memory_model", "wasm32");
    const inference_wasm_build_options_mod = inference_wasm_build_options.createModule();

    const wasm_inference_jinja_mod = b.createModule(.{
        .root_source_file = b.path("lib/jinja/src/jinja.zig"),
        .target = wasm_target,
        .optimize = .ReleaseSafe,
        .single_threaded = true,
    });
    const wasm_inference_tokenizer_mod = b.createModule(.{
        .root_source_file = b.path("lib/tokenizer/src/tokenizer.zig"),
        .target = wasm_target,
        .optimize = .ReleaseSafe,
    });
    wasm_inference_tokenizer_mod.addImport("sentencepiece_proto", sentencepiece_proto_mod);
    const wasm_inference_hf_tokenizer_mod = b.createModule(.{
        .root_source_file = b.path("lib/tokenizer/src/hf_root.zig"),
        .target = wasm_target,
        .optimize = .ReleaseSafe,
    });
    wasm_inference_hf_tokenizer_mod.addImport("inference_tokenizer", wasm_inference_tokenizer_mod);
    const wasm_inference_linalg_mod = b.createModule(.{
        .root_source_file = b.path("lib/linalg/src/mod.zig"),
        .target = wasm_target,
        .optimize = .ReleaseSafe,
    });
    const wasm_inference_ml_mod = b.createModule(.{
        .root_source_file = b.path("lib/ml/src/root.zig"),
        .target = wasm_target,
        .optimize = .ReleaseSafe,
        .single_threaded = true,
    });
    wasm_inference_ml_mod.addImport("antfly_platform", wasm_platform_mod);
    const wasm_inference_onnx_graph_mod = b.createModule(.{
        .root_source_file = b.path("lib/onnx/src/root.zig"),
        .target = wasm_target,
        .optimize = .ReleaseSafe,
        .single_threaded = true,
    });
    wasm_inference_onnx_graph_mod.addImport("protobuf", protobuf_mod);
    wasm_inference_onnx_graph_mod.addImport("ml", wasm_inference_ml_mod);
    const wasm_inference_audio_mod = b.createModule(.{
        .root_source_file = b.path("lib/audio/src/mod.zig"),
        .target = wasm_target,
        .optimize = .ReleaseSafe,
    });
    const inference_wasm_inference_mod = b.createModule(.{
        .root_source_file = b.path("pkg/inference/src/wasm_entry.zig"),
        .target = wasm_target,
        .optimize = .ReleaseSafe,
    });
    inference_wasm_inference_mod.addImport("build_options", inference_wasm_build_options_mod);
    inference_wasm_inference_mod.addImport("inference_audio", wasm_inference_audio_mod);
    inference_wasm_inference_mod.addImport("inference_linalg", wasm_inference_linalg_mod);
    inference_wasm_inference_mod.addImport("inference_tokenizer", wasm_inference_tokenizer_mod);
    inference_wasm_inference_mod.addImport("inference_hf_tokenizer", wasm_inference_hf_tokenizer_mod);
    inference_wasm_inference_mod.addImport("antfly_image", wasm_image_mod);
    inference_wasm_inference_mod.addImport("antfly_platform", wasm_platform_mod);
    inference_wasm_inference_mod.addImport("jinja", wasm_inference_jinja_mod);
    inference_wasm_inference_mod.addImport("ml", wasm_inference_ml_mod);
    inference_wasm_inference_mod.addImport("onnx_graph", wasm_inference_onnx_graph_mod);

    const antfly_wasm_mod = b.createModule(.{
        .root_source_file = b.path("examples/antfly_wasm.zig"),
        .target = wasm_target,
        .optimize = .ReleaseSafe,
    });
    antfly_wasm_mod.addImport("antfly_embedded_db", antfly_embedded_db_pkg_wasm_mod);
    antfly_wasm_mod.addImport("antfly_embedded_api", antfly_embedded_api_pkg_wasm_mod);
    antfly_wasm_mod.addImport("inference_runtime", inference_wasm_inference_mod);

    const antfly_wasm = b.addExecutable(.{
        .name = "antfly_wasm",
        .root_module = antfly_wasm_mod,
    });
    antfly_wasm.entry = .disabled;
    antfly_wasm.rdynamic = true;
    antfly_wasm.export_memory = true;
    const install_antfly_wasm = b.addInstallArtifact(antfly_wasm, .{
        .dest_dir = .{ .override = .prefix },
        .dest_sub_path = "antfly-wasm/antfly.wasm",
    });
    const install_antfly_wasm_smoke_run = b.addInstallFile(
        b.path("pkg/antfly-embedded/wasm_smoke_run.mjs"),
        "antfly-wasm/run.mjs",
    );
    const install_antfly_wasm_client = b.addInstallFile(
        b.path("pkg/antfly-embedded/wasm_client.mjs"),
        "antfly-wasm/antfly_embedded_wasm_client.mjs",
    );
    const install_antfly_wasm_browser = b.addInstallFile(
        b.path("pkg/antfly-embedded/wasm_smoke_browser.mjs"),
        "antfly-wasm/browser.mjs",
    );
    const install_antfly_wasm_index = b.addInstallFile(
        b.path("pkg/antfly-embedded/wasm_smoke_index.html"),
        "antfly-wasm/index.html",
    );
    const install_antfly_wasm_readme = b.addInstallFile(
        b.path("pkg/antfly-embedded/WASM.md"),
        "antfly-wasm/README.md",
    );

    const install_antfly_wasm_webgpu_ops = b.addInstallFile(
        b.path("pkg/antfly-embedded/webgpu_ops.mjs"),
        "antfly-wasm/webgpu_ops.mjs",
    );
    const shader_names = [_][]const u8{
        "attention",            "causal_attention",     "cross_attention",
        "gqa_cached_attention", "gqa_causal_attention", "layer_norm",
        "matmul",               "matmul_transb",        "matmul_transb_q4_0",
        "matmul_transb_q4_1",   "matmul_transb_q5_0",   "matmul_transb_q5_1",
        "matmul_transb_q8_0",   "matmul_transb_q8_1",   "matmul_transb_iq4_nl",
        "matmul_transb_iq4_xs", "matmul_transb_q2_k",   "matmul_transb_q3_k",
        "matmul_transb_q4_k",   "matmul_transb_q5_k",   "matmul_transb_q6_k",
        "matmul_transb_q8_k",   "rms_norm",
    };
    var install_shader_steps: [shader_names.len]*std.Build.Step = undefined;
    for (shader_names, 0..) |name, i| {
        const install_shader = b.addInstallFile(
            b.path(b.fmt("pkg/antfly-embedded/shaders/{s}.wgsl", .{name})),
            b.fmt("antfly-wasm/shaders/{s}.wgsl", .{name}),
        );
        install_shader_steps[i] = &install_shader.step;
    }

    const wasm_step = b.step("wasm", "Build and install the unified antfly wasm target (antfly-embedded + inference runtime)");
    wasm_step.dependOn(&install_antfly_wasm.step);
    wasm_step.dependOn(&install_antfly_wasm_smoke_run.step);
    wasm_step.dependOn(&install_antfly_wasm_client.step);
    wasm_step.dependOn(&install_antfly_wasm_browser.step);
    wasm_step.dependOn(&install_antfly_wasm_index.step);
    wasm_step.dependOn(&install_antfly_wasm_readme.step);
    wasm_step.dependOn(&install_antfly_wasm_webgpu_ops.step);
    for (&install_shader_steps) |step| {
        wasm_step.dependOn(step);
    }

    const run_antfly_wasm_smoke = b.addSystemCommand(&.{
        "node",
        b.getInstallPath(.prefix, "antfly-wasm/run.mjs"),
    });
    run_antfly_wasm_smoke.step.dependOn(wasm_step);

    const wasm_test_step = b.step("wasm-test", "Build the Antfly WASM bundle and run its Node smoke test");
    wasm_test_step.dependOn(&run_antfly_wasm_smoke.step);

    // Static library
    const lib = b.addLibrary(.{
        .linkage = .static,
        .name = "antfly-zig",
        .root_module = antfly_mod,
    });
    _ = lib;

    const capi_root_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/capi_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    antfly_imports.configure(b, capi_root_mod, false, link_libc);
    const capi_usermgr_storage_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/usermgr/storage_imports.zig"),
        .target = target,
        .optimize = optimize,
    });
    capi_usermgr_storage_mod.addImport("antfly_root", capi_root_mod);
    capi_usermgr_storage_mod.addImport("antfly_platform", platform_mod);
    capi_root_mod.addImport("usermgr_storage", capi_usermgr_storage_mod);

    const capi_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/capi/db.zig"),
        .target = target,
        .optimize = optimize,
        .pic = true,
    });
    capi_mod.addImport("antfly_storage_root", capi_root_mod);
    capi_mod.addImport("antfly_vector", vector_mod);
    capi_mod.addImport("structlog", structlog_mod);

    // The public C ABI and executable reuse the distributed PIC storage
    // archive, so production builds analyze and optimize that graph once.
    const libantfly_link_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/capi/link_anchor.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = link_libc,
        .strip = strip,
    });
    addMacosSdkPaths(b, libantfly_link_mod, target);
    const libantfly = b.addLibrary(.{
        .linkage = .dynamic,
        .name = "antfly",
        .root_module = libantfly_link_mod,
        .max_rss = 2 * 1024 * 1024 * 1024,
    });
    libantfly.link_gc_sections = true;
    // Homebrew rewrites the dylib ID to its absolute opt/lib path on install.
    if (target.result.os.tag == .macos) {
        libantfly.headerpad_max_install_names = true;
    }
    const install_libantfly = b.addInstallArtifact(libantfly, .{});
    const install_capi_header = b.addInstallFileWithDir(
        b.path("pkg/antfly/include/antfly.h"),
        .header,
        "antfly.h",
    );

    const capi_step = b.step("capi", "Build the public libantfly C ABI shared library");
    capi_step.dependOn(&install_libantfly.step);
    capi_step.dependOn(&install_capi_header.step);

    const capi_smoke_mod = b.createModule(.{
        .target = target,
        .optimize = optimize,
    });
    capi_smoke_mod.link_libc = true;
    capi_smoke_mod.addIncludePath(b.path("pkg/antfly/include"));
    capi_smoke_mod.addCSourceFile(.{
        .file = b.path("examples/antfly_c_smoke.c"),
        .flags = &.{ "-std=c11", "-Wall", "-Wextra", "-Werror" },
    });
    const capi_smoke = b.addExecutable(.{
        .name = "antfly-c-smoke",
        .root_module = capi_smoke_mod,
    });
    capi_smoke.root_module.linkLibrary(libantfly);
    const run_capi_smoke = b.addRunArtifact(capi_smoke);
    const capi_smoke_step = b.step("capi-smoke", "Compile and run a C consumer smoke test for libantfly");
    capi_smoke_step.dependOn(&run_capi_smoke.step);

    const run_lite_go_tests = b.addSystemCommand(&.{
        "env",
        "GOWORK=off",
        "go",
        "test",
        "-tags",
        "antflylite_capi",
        "-count=1",
        "./...",
    });
    run_lite_go_tests.setCwd(b.path("../go/pkg/antflylite"));
    run_lite_go_tests.step.dependOn(&install_libantfly.step);
    run_lite_go_tests.step.dependOn(&install_capi_header.step);
    const lite_go_test_step = b.step("lite-go-test", "Run Go Antfly Lite binding tests against libantfly");
    lite_go_test_step.dependOn(&run_lite_go_tests.step);

    const run_lite_go_example = b.addSystemCommand(&.{
        "env",
        "GOWORK=off",
        "go",
        "run",
        ".",
        "--reset",
        "--db",
        "../../zig/.zig-cache/antfly-lite-go-example.aflite",
        "--backup",
        "../../zig/.zig-cache/antfly-lite-go-example.afb",
    });
    run_lite_go_example.setCwd(b.path("../examples/antfly-lite-go"));
    run_lite_go_example.step.dependOn(&install_libantfly.step);
    run_lite_go_example.step.dependOn(&install_capi_header.step);
    const lite_go_example_step = b.step("lite-go-example", "Run the embedded Go Antfly Lite example app");
    lite_go_example_step.dependOn(&run_lite_go_example.step);

    const run_lite_go_retrieval_template = b.addSystemCommand(&.{
        "env",
        "GOWORK=off",
        "go",
        "run",
        ".",
        "--reset",
        "--db",
        "../../zig/.zig-cache/antfly-lite-retrieval-go.aflite",
        "--backup",
        "../../zig/.zig-cache/antfly-lite-retrieval-go.afb",
    });
    run_lite_go_retrieval_template.setCwd(b.path("../examples/antfly-lite-retrieval-go"));
    run_lite_go_retrieval_template.step.dependOn(&install_libantfly.step);
    run_lite_go_retrieval_template.step.dependOn(&install_capi_header.step);
    const lite_go_retrieval_template_step = b.step("lite-go-retrieval-template", "Run the embedded Go Antfly Lite retrieval template");
    lite_go_retrieval_template_step.dependOn(&run_lite_go_retrieval_template.step);

    const run_cabi_packaging_tests = b.addSystemCommand(&.{
        "env",
        "PYTHONPYCACHEPREFIX=/tmp/antfly-pycache",
        "python3",
        "scripts/packaging/test_cabi_packaging.py",
    });
    run_cabi_packaging_tests.setCwd(b.path(".."));
    const capi_package_test_step = b.step("capi-package-test", "Run Antfly C ABI release packaging regression tests");
    capi_package_test_step.dependOn(&run_cabi_packaging_tests.step);

    const capi_default_filters = [_][]const u8{
        "capi artifact decode and lookup json",
        "capi lite opens exports imports checks and vacuums aflite",
        "capi zero buffer helper wipes bytes before free",
        "capi lite exposes hosted and status-only profiles",
        "capi lite open options validate and configure ttl cleanup",
        "capi execute graph queries honors identity read generation",
        "capi search rejects stale identity generation before readable lease hook",
        "capi search json returns stamped identity generation",
        "packed dense response exposes public ids not doc ordinals",
        "dense response identity generation footer",
        "capi aggregate hits rejects stale identity generation before aggregation materialization",
    };
    const capi_tests = b.addTest(.{
        .root_module = capi_mod,
        .filters = selectTestFilters(b, &capi_default_filters),
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_capi_tests = addFilteredTestRunArtifact(b, capi_tests);
    const capi_test_step = b.step("capi-test", "Run C API tests");
    capi_test_step.dependOn(&run_capi_tests.step);

    return .{
        .embedded_mod = embedded_mod,
        .embedded_api_mod = embedded_api_mod,
        .antfly_embedded_pkg_mod = antfly_embedded_pkg_mod,
        .antfly_embedded_db_pkg_mod = antfly_embedded_db_pkg_mod,
        .antfly_embedded_api_pkg_mod = antfly_embedded_api_pkg_mod,
        .antfly_client_pkg_mod = antfly_client_pkg_mod,
        .capi_mod = capi_mod,
        .libantfly_link_mod = libantfly_link_mod,
        .install_libantfly = install_libantfly,
        .install_capi_header = install_capi_header,
        .run_capi_smoke = run_capi_smoke,
        .run_lite_go_tests = run_lite_go_tests,
        .run_lite_go_example = run_lite_go_example,
        .run_lite_go_retrieval_template = run_lite_go_retrieval_template,
        .run_cabi_packaging_tests = run_cabi_packaging_tests,
        .run_capi_tests = run_capi_tests,
    };
}
