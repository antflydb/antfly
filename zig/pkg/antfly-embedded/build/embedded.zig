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
const AntflyRootImports = @import("../../../build_support/antfly/imports.zig").AntflyRootImports;

/// Freestanding composition has no native cloud authentication or lake I/O.
pub fn configureBrowserModule(
    b: *std.Build,
    storage_boundary: @import("storage_boundary.zig").Modules,
    mod: *std.Build.Module,
    build_options: *std.Build.Step.Options,
    lite_options: *std.Build.Module,
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
const addFilteredTestRunArtifact = @import("../../../build_support/antfly/test_support.zig").addFilteredTestRunArtifact;
const addSnowballModule = @import("snowball.zig").addSnowballModule;
const selectTestFilters = @import("../../../build_support/antfly/test_support.zig").selectTestFilters;

pub const AddEmbeddedOptions = struct {
    server_integration_tests: bool = false,
    vopr: *std.Build.Module,
    lmdb_engine: *std.Build.Module,
    optimize: std.lang.Optimize,
    strip: bool,
    antfly_imports: AntflyRootImports,
    antfly_mod: *std.Build.Module,
};
pub const AddEmbeddedResult = struct {
    native_inference: *std.Build.Step.Compile,
    native_enrichment: *std.Build.Step.Compile,
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
    const httpx_mod = options.antfly_imports.httpx;
    const structlog_mod = options.antfly_imports.structlog;
    const client_openapi_mod = options.antfly_imports.client_openapi;
    const platform_mod = options.antfly_imports.platform;
    const vector_mod = options.antfly_imports.vector;
    const antfly_imports = options.antfly_imports;
    const antfly_mod = options.antfly_mod;
    const embedded_support_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/local/embedded_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    // The public package and independent products use one native dependency
    // composer. Browser composition remains a separate freestanding profile.
    antfly_imports.configureEmbedded(b, embedded_support_mod, link_libc);

    const embedded_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/engine/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    embedded_mod.addImport("embedded_support", embedded_support_mod);

    const embedded_db_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/engine/db.zig"),
        .target = target,
        .optimize = optimize,
    });
    embedded_db_mod.addImport("embedded_support", embedded_support_mod);

    const embedded_api_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/engine/api.zig"),
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
        .root_source_file = b.path(if (options.server_integration_tests) "pkg/antfly/src/capi_test_root.zig" else "pkg/antfly-embedded/src/local/public_capi_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    // Server integration tests supply their own root. Independent tests and
    // the installed library own only local public C API sources.
    const test_imports = @import("../../../build_support/antfly/test_support.zig").Imports{ .runtime = antfly_imports, .vopr = options.vopr, .lmdb_engine = options.lmdb_engine };
    test_imports.configure(b, capi_root_mod, false, link_libc);
    if (options.server_integration_tests) {
        const capi_usermgr_storage_mod = b.createModule(.{
            .root_source_file = b.path("pkg/antfly/src/usermgr/storage_imports.zig"),
            .target = target,
            .optimize = optimize,
        });
        capi_usermgr_storage_mod.addImport("antfly_root", capi_root_mod);
        capi_usermgr_storage_mod.addImport("antfly_platform", platform_mod);
        capi_root_mod.addImport("usermgr_storage", capi_usermgr_storage_mod);
    }

    const capi_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/local/public_capi_root.zig"),
        .target = target,
        .optimize = optimize,
        .pic = true,
    });
    antfly_imports.storage_boundary.configure(capi_mod, false, false);
    capi_mod.addImport("antfly_platform", platform_mod);
    const capi_options = b.addOptions();
    capi_options.addOption(bool, "linked_storage", false);
    // Every native libantfly includes the local inference provider archive.
    // Unit tests exercise the same host directly.
    capi_options.addOption(bool, "inference_enabled", true);
    capi_mod.addOptions("capi_build_options", capi_options);
    capi_root_mod.addOptions("capi_build_options", capi_options);
    capi_root_mod.addImport("antfly_storage_root", capi_root_mod);
    capi_mod.addImport("antfly_vector", vector_mod);
    capi_mod.addImport("structlog", structlog_mod);

    // Public C API compilation has its own physical/local source owner. No
    // private storage-provider archive is needed to analyze this object.
    antfly_imports.configureEmbedded(b, capi_mod, link_libc);
    capi_mod.addImport("antfly_storage_root", capi_mod);
    const capi_native_object = b.addObject(.{
        .name = "antfly-embedded-capi",
        .root_module = capi_mod,
        .max_rss = @as(usize, if (target.result.os.tag == .macos) 12 else 7) * 1024 * 1024 * 1024,
    });
    const capi_native_check = b.step("embedded-capi-check", "Compile the public C API with its independent local source owner");
    capi_native_check.dependOn(&capi_native_object.step);
    const capi_boundary = @import("embedded_boundary.zig").add(b, capi_mod);
    b.step("embedded-native-module-boundary-check", "Resolve the native public C API source and module boundary").dependOn(&capi_boundary.step);

    // Public C ABI and native inference have independent compilation owners.
    // The server storage archive keeps its private operations and handle owner;
    // the embedded library never links that server archive.
    const native_inference_mod = b.createModule(.{
        .root_source_file = b.path("pkg/inference/src/host/native_exports.zig"),
        .target = target,
        .optimize = optimize,
        .pic = true,
    });
    antfly_imports.configureInference(b, native_inference_mod, link_libc);
    const native_inference = b.addLibrary(.{
        .name = "antfly-embedded-inference",
        .linkage = .static,
        .root_module = native_inference_mod,
        .max_rss = 12 * 1024 * 1024 * 1024,
    });
    const native_enrichment_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/local/enrichment_compute_root.zig"),
        .target = target,
        .optimize = optimize,
        .pic = true,
    });
    antfly_imports.configureEnrichment(b, native_enrichment_mod, link_libc);
    const native_enrichment = b.addLibrary(.{
        .name = "antfly-embedded-enrichment",
        .linkage = .static,
        .root_module = native_enrichment_mod,
        .max_rss = 8 * 1024 * 1024 * 1024,
    });
    const libantfly_link_mod = capi_mod;
    libantfly_link_mod.strip = strip;
    libantfly_link_mod.linkLibrary(native_inference);
    libantfly_link_mod.linkLibrary(native_enrichment);
    addMacosSdkPaths(b, libantfly_link_mod, target);
    const libantfly = b.addLibrary(.{
        .linkage = .dynamic,
        .name = "antfly",
        .root_module = libantfly_link_mod,
        .max_rss = 12 * 1024 * 1024 * 1024,
    });
    libantfly.link_gc_sections = true;
    // Homebrew rewrites the dylib ID to its absolute opt/lib path on install.
    if (target.result.os.tag == .macos) {
        libantfly.headerpad_max_install_names = true;
    }
    const install_libantfly = b.addInstallArtifact(libantfly, .{});
    const install_capi_header = b.addInstallFileWithDir(
        b.path("pkg/antfly-embedded/include/antfly.h"),
        .header,
        "antfly.h",
    );

    const public_consumer_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/tests/lake.zig"),
        .target = target,
        .optimize = optimize,
    });
    public_consumer_mod.addImport("antfly-embedded", antfly_embedded_pkg_mod);
    const public_consumer_tests = b.addTest(.{ .root_module = public_consumer_mod });
    b.step("embedded-package-test", "Exercise lake readers and SQL through the public Zig package")
        .dependOn(&b.addRunArtifact(public_consumer_tests).step);
    const package_boundary = @import("embedded_boundary.zig").add(b, antfly_embedded_pkg_mod);
    b.top_level_steps.get("embedded-native-module-boundary-check").?.step.dependOn(&package_boundary.step);

    const capi_step = b.step("capi", "Build the public libantfly C ABI shared library");
    capi_step.dependOn(&install_libantfly.step);
    capi_step.dependOn(&install_capi_header.step);

    const capi_smoke_mod = b.createModule(.{
        .target = target,
        .optimize = optimize,
    });
    capi_smoke_mod.link_libc = true;
    capi_smoke_mod.addIncludePath(b.path("pkg/antfly-embedded/include"));
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
    // (pkg/antfly-embedded/capi-conformance/README.md). It calls libantfly only
    // through the public header, so a case that fails here is an ABI bug
    // rather than a binding bug.
    const capi_conformance_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/local/capi/conformance_runner.zig"),
        .target = target,
        .optimize = optimize,
    });
    capi_conformance_mod.link_libc = true;
    const capi_header = b.addTranslateC(.{
        .root_source_file = b.path("pkg/antfly-embedded/include/antfly.h"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    capi_conformance_mod.addImport("antfly_c", capi_header.createModule());
    const capi_conformance = b.addExecutable(.{
        .name = "antfly-capi-conformance",
        .root_module = capi_conformance_mod,
    });
    capi_conformance.root_module.linkLibrary(libantfly);
    const run_capi_conformance = b.addRunArtifact(capi_conformance);
    run_capi_conformance.addDirectoryArg2(b.path("pkg/antfly-embedded/capi-conformance/cases"), .{ .make_absolute = true });
    _ = run_capi_conformance.addOutputDirectoryArg2("capi-conformance-work", .{ .make_absolute = true });
    const capi_conformance_step = b.step("capi-conformance", "Run the shared libantfly conformance cases against the C ABI");
    capi_conformance_step.dependOn(&run_capi_conformance.step);

    const run_lite_go_tests = b.addSystemCommand(&.{
        "env",
        "GOWORK=off",
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
    const run_lite_py_tests = b.addSystemCommand(&.{
        "env",
        "ANTFLY_LITE_REQUIRE_LIBRARY=1",
        "uv",
        "run",
        "--locked",
        "pytest",
        "-q",
    });
    run_lite_py_tests.argv.insert(b.allocator, 2, .{ .decorated_directory = .{ .lazy_path = b.graph.path(.install_lib, ""), .prefix = "ANTFLY_LIB_DIR=", .suffix = "", .make_absolute = true } }) catch @panic("OOM");
    run_lite_py_tests.setCwd(b.path("../py/packages/lite"));
    run_lite_py_tests.step.dependOn(&install_libantfly.step);
    const lite_py_test_step = b.step("lite-py-test", "Run Python Antfly Lite binding tests against libantfly");
    lite_py_test_step.dependOn(&run_lite_py_tests.step);

    const run_lite_rs_tests = b.addSystemCommand(&.{
        "env",
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
    run_lite_rs_tests.argv.insert(b.allocator, 1, .{ .decorated_directory = .{ .lazy_path = b.graph.path(.install_lib, ""), .prefix = "ANTFLY_LIB_DIR=", .suffix = "", .make_absolute = true } }) catch @panic("OOM");
    run_lite_rs_tests.setCwd(b.path("."));
    run_lite_rs_tests.step.dependOn(&install_libantfly.step);
    const lite_rs_test_step = b.step("lite-rs-test", "Run Rust Antfly Lite binding tests against libantfly");
    lite_rs_test_step.dependOn(&run_lite_rs_tests.step);

    const run_lite_ts_tests = b.addSystemCommand(&.{
        "env",
        "ANTFLY_LITE_REQUIRE_LIBRARY=1",
        "pnpm",
        "run",
        "test",
    });
    run_lite_ts_tests.argv.insert(b.allocator, 2, .{ .decorated_directory = .{ .lazy_path = b.graph.path(.install_lib, ""), .prefix = "ANTFLY_LIB_DIR=", .suffix = "", .make_absolute = true } }) catch @panic("OOM");
    run_lite_ts_tests.setCwd(b.path("../ts/packages/lite"));
    run_lite_ts_tests.step.dependOn(&install_libantfly.step);
    const lite_ts_test_step = b.step("lite-ts-test", "Run TypeScript Antfly Lite binding tests against libantfly (needs pnpm install in ts/)");
    lite_ts_test_step.dependOn(&run_lite_ts_tests.step);

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
        "capi SQL",
        "storage owner runtime status",
        "capi relational expression errors preserve public status semantics",
        "capi artifact decode and lookup json",
        "capi lite opens exports imports checks and vacuums aflite",
        "capi zero buffer helper wipes bytes before free",
        "capi system write cursor",
        "capi lite exposes hosted and status-only profiles",
        "capi lite open options validate and configure ttl cleanup",
        "capi directory restore coordinates with open handles and publishes atomically",
        "capi directory restore publishes with derived work drained",
        "capi handle ids are safe to use after close and across slot reuse",
        "capi handle registry retires a slot instead of wrapping its generation",
        "capi concurrent calls and closes on one handle never touch freed memory",
        "capi text and dense searches succeed while writes commit",
        "capi execute graph queries honors identity read generation",
        "capi fact relationships preserve identities and filter before ranking",
        "capi fact path serialization and parsing release partial allocations",
        "capi fact algebraic paths retain provenance and respect frontier limits",
        "capi fact edge cleanup releases the owned array exactly once",
        "capi search rejects stale identity generation before readable lease hook",
        "capi search json returns stamped identity generation",
        "packed dense response exposes public ids not doc ordinals",
        "dense response identity generation footer",
        "capi aggregate hits rejects stale identity generation before aggregation materialization",
        "capi lite local-runtime-configured flag reports local_embedded only when the build links inference",
        "capi lite explicit resource budget overrides are reported in status",
        "capi lite defaults embedded generation budgets when no override is given",
        "capi lite drains an antfly embedder with no api_url through the embedded inference provider",
        "capi inference options are prefix compatible and reject unknown flags and reserved bits",
        "capi inference calls reject null, closed, and database handles",
        "capi inference lists models and reports route errors",
        "capi inference embeds text with a local model",
        "capi inference reranks documents with a local model",
        "capi inference chunks text without a model",
        "capi inference generates text with a local model and rejects streaming",
        "capi inference pull rejects invalid requests with a JSON error",
        "capi inference pulls a model with progress",
        "capi inference streaming reports request errors without a model",
        "capi get edges json does not double free a non-empty edge slice",
        "run until idle no-progress error maps to a dedicated stalled ABI code, not internal",
        "capi lite merged indexes JSON discovers a standalone asset extractor and chunk enrichment with no owning index",
        "capi lite run until idle drains a standalone chunk enrichment with no owning index",
        "capi lite AddIndexJSON registers a graph config",
        "capi lite AddIndexJSON restores the enrichment catalog when admission rejects the index",
    };
    const capi_tests = b.addTest(.{
        .root_module = capi_root_mod,
        // Storage-backed Mach-O Debug codegen measured 13.51 GB.
        .max_rss = @as(usize, if (target.result.os.tag == .macos) 14 else 7) * 1024 * 1024 * 1024,
        .filters = selectTestFilters(b, &capi_default_filters),
        .test_runner = .{
            .path = b.path("pkg/antfly-embedded/src/local/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_capi_tests = addFilteredTestRunArtifact(b, capi_tests);
    const lake_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/local/lake_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    antfly_imports.configureEmbedded(b, lake_test_mod, link_libc);
    const lake_tests = b.addTest(.{
        .name = "embedded-lake",
        .root_module = lake_test_mod,
        // Compile reader/cursor regressions, not unrelated server fixture tests
        // reachable through shared schema and credential modules.
        .filters = selectTestFilters(b, &.{ "lake SQL", "Parquet ", "parquet ", "Iceberg ", "iceberg ", "external source inventory", "external lake identities", "external lake pruning", "lake host resolver" }),
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/local/test_runner.zig"), .mode = .simple },
    });
    b.step("embedded-lake-test", "Test lake readers and SQL cursors without the server")
        .dependOn(&addFilteredTestRunArtifact(b, lake_tests).step);

    const capi_test_step = b.step("capi-test", "Run C API tests");
    capi_test_step.dependOn(&run_capi_tests.step);

    return .{
        .native_inference = native_inference,
        .native_enrichment = native_enrichment,
        .embedded_mod = embedded_mod,
        .embedded_api_mod = embedded_api_mod,
        .antfly_embedded_pkg_mod = antfly_embedded_pkg_mod,
        .antfly_embedded_db_pkg_mod = antfly_embedded_db_pkg_mod,
        .antfly_embedded_api_pkg_mod = antfly_embedded_api_pkg_mod,
        .antfly_client_pkg_mod = antfly_client_pkg_mod,
        .embedded_db_mod = embedded_db_mod,
        .embedded_support_mod = embedded_support_mod,
        .capi_root_mod = capi_root_mod,
        .capi_mod = capi_root_mod,
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
