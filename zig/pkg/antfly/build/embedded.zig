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
    storage_boundary: @import("storage_boundary.zig").Modules,
    mod: *std.Build.Module,
    build_options: *std.Build.Step.Options,
    lite_options: *std.Build.Module,
    lmdb_engine_mod: *std.Build.Module,
    json_mod: *std.Build.Module,
    public_openapi_mod: *std.Build.Module,
    query_openapi_mod: *std.Build.Module,
    indexes_openapi_mod: *std.Build.Module,
    sort_openapi_mod: *std.Build.Module,
    metadata_openapi_mod: *std.Build.Module,
    schema_openapi_mod: *std.Build.Module,
    reranking_mod: *std.Build.Module,
    objectstore_mod: *std.Build.Module,
    httpx_mod: *std.Build.Module,
    platform_mod: *std.Build.Module,
    chunking_mod: *std.Build.Module,
    bloom_mod: *std.Build.Module,
    vector_mod: *std.Build.Module,
    vectorindex_mod: *std.Build.Module,
    hash_mod: *std.Build.Module,
    fst_mod: *std.Build.Module,
    regex_mod: *std.Build.Module,
    image_mod: *std.Build.Module,
    font_mod: *std.Build.Module,
    pdf_mod: *std.Build.Module,
    handlebars_mod: *std.Build.Module,
    add_snowball_module: *const fn (*std.Build, *std.Build.Module) void,
) void {
    storage_boundary.configure(mod, false, false);
    mod.addOptions("build_options", build_options);
    mod.addImport("antfly_lite_options", lite_options);
    mod.addImport("lmdb_engine", lmdb_engine_mod);
    mod.addImport("antfly-json", json_mod);
    mod.addImport("antfly_public_openapi", public_openapi_mod);
    mod.addImport("antfly_query_openapi", query_openapi_mod);
    mod.addImport("antfly_indexes_openapi", indexes_openapi_mod);
    mod.addImport("antfly_sort_openapi", sort_openapi_mod);
    mod.addImport("antfly_metadata_openapi", metadata_openapi_mod);
    // schema/table_schema_impl.zig takes the public table storage mode and
    // the relational wire types from the schema OpenAPI module (#784).
    mod.addImport("antfly_schema_openapi", schema_openapi_mod);
    mod.addImport("antfly_reranking", reranking_mod);
    mod.addImport("objectstore", objectstore_mod);
    mod.addImport("httpx", httpx_mod);
    mod.addImport("antfly_platform", platform_mod);
    mod.addImport("antfly_chunking", chunking_mod);
    mod.addImport("bloom", bloom_mod);
    mod.addImport("antfly_vector", vector_mod);
    mod.addImport("antfly_vectorindex", vectorindex_mod);
    mod.addImport("antfly_hash", hash_mod);
    mod.addImport("antfly_fst", fst_mod);
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
    vopr: *std.Build.Module,
    lmdb_engine: *std.Build.Module,
    optimize: std.builtin.OptimizeMode,
    strip: bool,
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
    embedded_db_mod: *std.Build.Module,
    embedded_support_mod: *std.Build.Module,
    capi_root_mod: *std.Build.Module,
    capi_mod: *std.Build.Module,
    libantfly_link_mod: *std.Build.Module,
    install_libantfly: *std.Build.Step.InstallArtifact,
    install_capi_header: *std.Build.Step.InstallFile,
    run_capi_smoke: *std.Build.Step.Run,
    run_capi_conformance: *std.Build.Step.Run,
    run_lite_go_tests: *std.Build.Step.Run,
    run_lite_py_tests: *std.Build.Step.Run,
    run_lite_rs_tests: *std.Build.Step.Run,
    run_lite_ts_tests: *std.Build.Step.Run,
    run_lite_go_example: *std.Build.Step.Run,
    run_lite_go_retrieval_template: *std.Build.Step.Run,
    run_cabi_packaging_tests: *std.Build.Step.Run,
    run_capi_tests: *std.Build.Step.Run,
};

pub fn addEmbedded(b: *std.Build, options: AddEmbeddedOptions) AddEmbeddedResult {
    const target = options.antfly_imports.platform_target;
    const optimize = options.optimize;
    const strip = options.strip;
    const link_libc = options.antfly_imports.platform_link_libc;
    const build_options = options.antfly_imports.build_options;
    const lmdb_engine_mod = options.lmdb_engine;
    const httpx_mod = options.antfly_imports.httpx;
    const structlog_mod = options.antfly_imports.structlog;
    const public_openapi_mod = options.antfly_imports.public_openapi;
    const client_openapi_mod = options.antfly_imports.client_openapi;
    const indexes_openapi_mod = options.antfly_imports.indexes_openapi;
    const sort_openapi_mod = options.antfly_imports.sort_openapi;
    const query_openapi_mod = options.antfly_imports.query_openapi;
    const metadata_openapi_mod = options.antfly_imports.metadata_openapi;
    const schema_openapi_mod = options.antfly_imports.schema_openapi;
    const handlebars_mod = options.antfly_imports.handlebars;
    const platform_mod = options.antfly_imports.platform;
    const objectstore_mod = options.antfly_imports.objectstore;
    const bloom_mod = options.antfly_imports.bloom;
    const vector_mod = options.antfly_imports.vector;
    const hash_mod = options.antfly_imports.hash;
    const vectorindex_mod = options.antfly_imports.vectorindex;
    const fst_mod = options.antfly_imports.fst;
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
    const transcribing_mod = options.antfly_imports.transcribing;
    const reader_config_mod = options.antfly_imports.reader_config;
    const antfly_imports = options.antfly_imports;
    const antfly_mod = options.antfly_mod;
    const embedded_deps = .{
        build_options,
        antfly_imports.lite_options,
        lmdb_engine_mod,
        json_mod,
        public_openapi_mod,
        query_openapi_mod,
        indexes_openapi_mod,
        sort_openapi_mod,
        metadata_openapi_mod,
        schema_openapi_mod,
        reranking_mod,
        objectstore_mod,
        httpx_mod,
        platform_mod,
        chunking_mod,
        bloom_mod,
        vector_mod,
        vectorindex_mod,
        hash_mod,
        fst_mod,
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
    @call(.auto, configureEmbeddedModule, .{ b, antfly_imports.storage_boundary, embedded_support_mod } ++ embedded_deps ++ .{addSnowballModule});
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
    // This source root supports C API unit tests. The installed library uses
    // libantfly_link_mod and the production storage archive below.
    const test_imports = @import("test_support.zig").Imports{ .runtime = antfly_imports, .vopr = options.vopr, .lmdb_engine = options.lmdb_engine };
    test_imports.configure(b, capi_root_mod, false, link_libc);
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
    antfly_imports.storage_boundary.configure(capi_mod, false, false);
    capi_mod.addImport("antfly_source_root", capi_root_mod);
    const capi_options = b.addOptions();
    capi_options.addOption(bool, "linked_storage", false);
    // The inference runtime is always linked into libantfly (see
    // link_anchor.zig and addRuntime's storage_kernel unit), so this is
    // unconditionally true. Unit tests compile the inference call path
    // directly (no archive/trap boundary either way).
    capi_options.addOption(bool, "inference_enabled", true);
    capi_mod.addOptions("capi_build_options", capi_options);
    capi_mod.addImport("antfly_storage_root", capi_root_mod);
    capi_mod.addImport("antfly_vector", vector_mod);
    capi_mod.addImport("structlog", structlog_mod);

    // The public C ABI and executable reuse the distributed PIC storage
    // archive, so production builds analyze and optimize that graph once.
    // libantfly embeds the standalone inference runtime in-process, the same
    // as the `antfly` executable (see link_anchor.zig and addRuntime's
    // storage_kernel unit): this is a deliberate product decision
    // (2026-09-17) so Lite hosts get local inference without a separate
    // runtime, at the cost of a much larger shared library (see
    // COMPILATION.md's "C API composition" section).
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
        .max_rss = 4 * 1024 * 1024 * 1024,
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

    // Reference runner for the shared conformance cases every binding runs
    // (pkg/antfly/capi-conformance/README.md). It calls libantfly only
    // through the public header, so a case that fails here is an ABI bug
    // rather than a binding bug.
    const capi_conformance_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/capi/conformance_runner.zig"),
        .target = target,
        .optimize = optimize,
    });
    capi_conformance_mod.link_libc = true;
    capi_conformance_mod.addIncludePath(b.path("pkg/antfly/include"));
    const capi_conformance = b.addExecutable(.{
        .name = "antfly-capi-conformance",
        .root_module = capi_conformance_mod,
    });
    capi_conformance.root_module.linkLibrary(libantfly);
    const run_capi_conformance = b.addRunArtifact(capi_conformance);
    run_capi_conformance.addDirectoryArg(b.path("pkg/antfly/capi-conformance/cases"));
    _ = run_capi_conformance.addOutputDirectoryArg("capi-conformance-work");
    const capi_conformance_step = b.step("capi-conformance", "Run the shared libantfly conformance cases against the C ABI");
    capi_conformance_step.dependOn(&run_capi_conformance.step);

    // The Go test binary is not the `antfly` executable, so it cannot re-exec
    // itself the way the CLI does to spawn the sandboxed inference worker
    // (see inference_worker.zig's `resolveWorkerExecutable`). Point it at the
    // `antfly` binary this same build tree produces so it never falls
    // through to an unrelated `antfly` a developer happens to have on PATH.
    const lite_go_worker_env = b.fmt("ANTFLY_INFERENCE_WORKER={s}", .{b.getInstallPath(.bin, "antfly")});
    const run_lite_go_tests = b.addSystemCommand(&.{
        "env",
        "GOWORK=off",
        lite_go_worker_env,
        "go",
        "test",
        "-tags",
        "libantfly",
        "-count=1",
        "./...",
    });
    run_lite_go_tests.setCwd(b.path("../go/pkg/lite"));
    run_lite_go_tests.step.dependOn(&install_libantfly.step);
    run_lite_go_tests.step.dependOn(&install_capi_header.step);
    const lite_go_test_step = b.step("lite-go-test", "Run Go Antfly Lite binding tests against libantfly");
    lite_go_test_step.dependOn(&run_lite_go_tests.step);

    // The Python, Rust, and TypeScript bindings skip their native tests
    // when libantfly is absent; ANTFLY_LITE_REQUIRE_LIBRARY turns that into
    // a failure here, and ANTFLY_LIB_DIR points them at this build's copy.
    const lite_lib_dir_env = b.fmt("ANTFLY_LIB_DIR={s}", .{b.getInstallPath(.lib, "")});
    const run_lite_py_tests = b.addSystemCommand(&.{
        "env",
        "ANTFLY_LITE_REQUIRE_LIBRARY=1",
        lite_lib_dir_env,
        lite_go_worker_env,
        "uv",
        "run",
        "--locked",
        "pytest",
        "-q",
    });
    run_lite_py_tests.setCwd(b.path("../py/packages/lite"));
    run_lite_py_tests.step.dependOn(&install_libantfly.step);
    const lite_py_test_step = b.step("lite-py-test", "Run Python Antfly Lite binding tests against libantfly");
    lite_py_test_step.dependOn(&run_lite_py_tests.step);

    const run_lite_rs_tests = b.addSystemCommand(&.{
        "env",
        lite_lib_dir_env,
        lite_go_worker_env,
        "cargo",
        "test",
        "--locked",
        "--manifest-path",
        "../rs/Cargo.toml",
        "--package",
        "antfly-lite",
        "--features",
        "libantfly",
    });
    run_lite_rs_tests.setCwd(b.path("."));
    run_lite_rs_tests.step.dependOn(&install_libantfly.step);
    const lite_rs_test_step = b.step("lite-rs-test", "Run Rust Antfly Lite binding tests against libantfly");
    lite_rs_test_step.dependOn(&run_lite_rs_tests.step);

    const run_lite_ts_tests = b.addSystemCommand(&.{
        "env",
        "ANTFLY_LITE_REQUIRE_LIBRARY=1",
        lite_lib_dir_env,
        lite_go_worker_env,
        "pnpm",
        "run",
        "test",
    });
    run_lite_ts_tests.setCwd(b.path("../ts/packages/lite"));
    run_lite_ts_tests.step.dependOn(&install_libantfly.step);
    const lite_ts_test_step = b.step("lite-ts-test", "Run TypeScript Antfly Lite binding tests against libantfly (needs pnpm install in ts/)");
    lite_ts_test_step.dependOn(&run_lite_ts_tests.step);

    const run_lite_go_example = b.addSystemCommand(&.{
        "env",
        "GOWORK=off",
        lite_go_worker_env,
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
        lite_go_worker_env,
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
        "capi SQL",
        "storage owner runtime status",
        "capi relational expression errors preserve public status semantics",
        "capi artifact decode and lookup json",
        "capi lite opens exports imports checks and vacuums aflite",
        "capi zero buffer helper wipes bytes before free",
        "capi system write cursor",
        "capi lite exposes hosted and status-only profiles",
        "capi lite open options validate and configure ttl cleanup",
        "capi handle ids are safe to use after close and across slot reuse",
        "capi handle registry retires a slot instead of wrapping its generation",
        "capi concurrent calls and closes on one handle never touch freed memory",
        "capi text and dense searches succeed while writes commit",
        "capi execute graph queries honors identity read generation",
        "capi search rejects stale identity generation before readable lease hook",
        "capi search json returns stamped identity generation",
        "packed dense response exposes public ids not doc ordinals",
        "dense response identity generation footer",
        "capi aggregate hits rejects stale identity generation before aggregation materialization",
        "capi lite local-runtime-configured flag reports local_embedded only when the build links inference",
        "capi lite explicit resource budget overrides are reported in status",
        "capi lite defaults embedded generation budgets when no override is given",
        "capi lite drains an antfly embedder with no api_url through the embedded inference provider",
        "capi get edges json does not double free a non-empty edge slice",
        "run until idle no-progress error maps to a dedicated stalled ABI code, not internal",
        "capi lite merged indexes JSON discovers a standalone asset extractor and chunk enrichment with no owning index",
        "capi lite run until idle drains a standalone chunk enrichment with no owning index",
        "capi lite AddIndexJSON registers a graph config's nested resolvers",
        "capi lite AddIndexJSON restores the enrichment catalog when admission rejects the index",
    };
    const capi_tests = b.addTest(.{
        .root_module = capi_mod,
        // Storage-backed Mach-O ReleaseSafe codegen needs 12 GiB headroom.
        .max_rss = @as(usize, if (target.result.os.tag == .macos) 12 else 7) * 1024 * 1024 * 1024,
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
        .embedded_db_mod = embedded_db_mod,
        .embedded_support_mod = embedded_support_mod,
        .capi_root_mod = capi_root_mod,
        .capi_mod = capi_mod,
        .libantfly_link_mod = libantfly_link_mod,
        .install_libantfly = install_libantfly,
        .install_capi_header = install_capi_header,
        .run_capi_smoke = run_capi_smoke,
        .run_capi_conformance = run_capi_conformance,
        .run_lite_go_tests = run_lite_go_tests,
        .run_lite_py_tests = run_lite_py_tests,
        .run_lite_rs_tests = run_lite_rs_tests,
        .run_lite_ts_tests = run_lite_ts_tests,
        .run_lite_go_example = run_lite_go_example,
        .run_lite_go_retrieval_template = run_lite_go_retrieval_template,
        .run_cabi_packaging_tests = run_cabi_packaging_tests,
        .run_capi_tests = run_capi_tests,
    };
}
