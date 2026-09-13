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
const Context = @import("context.zig").Context;
const runtime_build = @import("runtime.zig");
const build_test_filters = @import("test_filters.zig");

pub const Suite = struct {
    tests: *std.Build.Step.Compile,
    selected_test_filters: []const []const u8,
    run_tests: *std.Build.Step.Run,
    run_cli_tests: *std.Build.Step.Run,
    run_gliner25_fuzz_tests: *std.Build.Step.Run,
};

pub fn create(ctx: Context) Suite {
    const b = ctx.b;
    const selected_test_filters = build_test_filters.select(b.allocator, ctx.args orelse &.{}, &.{});
    const main_test_filters = if (ctx.runtime_test_filter) &.{} else selected_test_filters;
    const runtime_filter_test_runner: std.Build.Step.Compile.TestRunner = .{
        .path = ctx.path("src/test_runner_filter.zig"),
        .mode = .simple,
    };
    // Full CPU inference tests measured about 6 GiB to compile.
    const tests = b.addTest(.{
        .max_rss = 7 * 1024 * 1024 * 1024,
        .root_module = b.createModule(.{
            .root_source_file = ctx.path("src/inference.zig"),
            .target = ctx.target,
            .optimize = ctx.optimize,
        }),
        .filters = main_test_filters,
        .test_runner = runtime_filter_test_runner,
    });
    tests.root_module.addImport("build_info", ctx.graph.build_info_mod);
    ctx.graph.identities.addImports(tests.root_module);
    tests.root_module.addImport("build_options", ctx.graph.qualification_build_options_mod);
    tests.root_module.addImport("antfly-json", ctx.graph.json_mod);
    tests.root_module.addImport("httpx", ctx.graph.httpx_mod);
    tests.root_module.addImport("inference_api", ctx.graph.inference_api_mod);
    tests.root_module.addImport("antfly_generating_openapi", ctx.graph.generating_openapi_mod);
    tests.root_module.addImport("antfly_extraction_openapi", ctx.graph.extraction_openapi_mod);
    tests.root_module.addImport("antfly_extracting", ctx.graph.extracting_mod);
    tests.root_module.addImport("antfly_transcribing", ctx.graph.transcribing_mod);
    tests.root_module.addImport("inference_audio", ctx.graph.inference_audio_mod);
    tests.root_module.addImport("inference_chunker", ctx.graph.inference_chunker_mod);
    tests.root_module.addImport("jinja", ctx.graph.jinja_mod);
    tests.root_module.addImport("inference_tokenizer", ctx.graph.inference_tokenizer_mod);
    tests.root_module.addImport("inference_hf_tokenizer", ctx.graph.inference_hf_tokenizer_mod);
    tests.root_module.addImport("inference_linalg", ctx.graph.inference_linalg_mod);
    tests.root_module.addImport("inference_fixed_tokenizer_data", ctx.graph.inference_fixed_tokenizer_data_mod);
    tests.root_module.addImport("antfly_jsonschema", ctx.graph.jsonschema_mod);
    tests.root_module.addImport("antfly_scraping", ctx.graph.scraping_mod);
    tests.root_module.addImport("antfly_image", ctx.graph.image_mod);
    tests.root_module.addImport("ml", ctx.graph.ml_mod);
    tests.root_module.addImport("ml_tabular", ctx.graph.ml_tabular_mod);
    tests.root_module.addImport("onnx_graph", ctx.graph.onnx.graph);
    tests.root_module.addImport("onnx_data", ctx.graph.onnx.data);
    tests.root_module.addImport("pjrt", ctx.graph.qualification_pjrt_mod);
    tests.root_module.addImport("prometheus", ctx.graph.prometheus_mod);
    tests.root_module.addImport("structlog", ctx.graph.structlog_mod);
    tests.root_module.addImport("antfly_platform", ctx.graph.platform_mod);
    tests.root_module.addImport("antfly_reader_config", ctx.graph.reader_config_mod);
    tests.root_module.addImport("inference_internal", tests.root_module);
    if (ctx.graph.inference_client_mod) |mod| {
        tests.root_module.addImport("inference_client", mod);
    }
    if (ctx.backend.enable_system_blas) {
        runtime_build.configureSystemBlas(b, tests.root_module, ctx.target, ctx.backend.blas_root);
    }
    runtime_build.configureMetal(b, tests.root_module, ctx.target, ctx.backend.enable_metal, ctx.paths);
    runtime_build.configureOnnxRuntime(b, tests.root_module, ctx.backend.enable_onnx, ctx.backend.onnx_root);
    tests.root_module.link_libc = ctx.backend.link_libc;

    // Keep the standalone server's CLI and configuration parsing covered too.
    // `src/inference.zig` does not import `src/main.zig`, so tests declared by
    // the executable root otherwise compile only when invoked manually.
    // The CLI test root measured about 5 GiB; retain accelerator headroom.
    const cli_tests = b.addTest(.{
        .max_rss = @as(usize, if (ctx.hasAccelerator()) 7 else 6) * 1024 * 1024 * 1024,
        .root_module = b.createModule(.{
            .root_source_file = ctx.path("src/main.zig"),
            .target = ctx.target,
            .optimize = ctx.optimize,
        }),
        .filters = main_test_filters,
        .test_runner = runtime_filter_test_runner,
    });
    cli_tests.root_module.addImport("build_info", ctx.graph.build_info_mod);
    cli_tests.root_module.addImport("inference", ctx.graph.inference_mod);
    cli_tests.root_module.addImport("build_options", ctx.graph.build_options_mod);
    cli_tests.root_module.addImport("structlog", ctx.graph.structlog_mod);
    cli_tests.root_module.addImport("antfly_platform", ctx.graph.platform_mod);
    cli_tests.root_module.link_libc = ctx.backend.link_libc;

    const run_cli_tests = ctx.addRunArtifact(cli_tests);
    for (selected_test_filters) |filter| {
        run_cli_tests.addArgs(&.{ "--test-filter", filter });
    }
    build_test_filters.addRuntimeControls(run_cli_tests, ctx.args orelse &.{});
    const run_tests = ctx.addRunArtifact(tests);
    for (selected_test_filters) |filter| {
        run_tests.addArgs(&.{ "--test-filter", filter });
    }
    build_test_filters.addRuntimeControls(run_tests, ctx.args orelse &.{});
    return .{
        .tests = tests,
        .selected_test_filters = selected_test_filters,
        .run_tests = run_tests,
        .run_cli_tests = run_cli_tests,
        .run_gliner25_fuzz_tests = addGliner25Fuzz(ctx),
    };
}

pub const Checks = struct {
    codegen: *std.Build.Step.Run,
    cuda_source: *std.Build.Step.Run,
    metal_runtime: *std.Build.Step.Run,
    bge_benchmark: *std.Build.Step.Run,
};

pub fn addDefault(ctx: Context, suite: Suite, checks: Checks) *std.Build.Step {
    const b = ctx.b;
    const selected_test_filters = suite.selected_test_filters;
    const run_tests = suite.run_tests;
    const run_cli_tests = suite.run_cli_tests;
    const quant_kernel_codegen_test_check = checks.codegen;
    const cuda_artifact_source_policy_check = checks.cuda_source;
    const run_quant_kernel_metal_runtime_check_tests = checks.metal_runtime;
    const run_bge_m3_e2e_bench_tests = checks.bge_benchmark;
    const test_step = ctx.step("test", "Run unit tests");
    const cancellation_e2e_step = ctx.step("test-cancellation-e2e", "Run HTTP inference cancellation and worker recovery E2E tests");
    if (ctx.target.result.os.tag == .linux or ctx.target.result.os.tag == .macos) {
        const fixture = b.addExecutable(.{
            .name = "inference-cancellation-fixture",
            // The CPU fixture reached 4.74 GB after the runtime I/O migration.
            // Keep headroom so parallel builds reserve its observed footprint.
            .max_rss = @as(usize, if (ctx.hasAccelerator()) 7 else 6) * 1024 * 1024 * 1024,
            .root_module = b.createModule(.{
                .root_source_file = ctx.path("tests/cancellation_fixture.zig"),
                .target = ctx.target,
                .optimize = ctx.optimize,
            }),
        });
        fixture.root_module.addImport("inference", ctx.graph.inference_mod);
        fixture.root_module.addImport("httpx", ctx.graph.httpx_mod);
        fixture.root_module.addImport("antfly_platform", ctx.graph.platform_mod);
        fixture.root_module.link_libc = true;
        const integration = ctx.add_native_process_test(b, fixture, ctx.path("tests/test_cancellation_e2e.py"));
        cancellation_e2e_step.dependOn(integration);
        if (selected_test_filters.len == 0) test_step.dependOn(cancellation_e2e_step);
    }
    test_step.dependOn(&quant_kernel_codegen_test_check.step);
    test_step.dependOn(&cuda_artifact_source_policy_check.step);
    test_step.dependOn(&run_quant_kernel_metal_runtime_check_tests.step);
    test_step.dependOn(&run_tests.step);
    // A focused server/library filter need not match an executable-root test.
    // The default aggregate still owns the complete executable-root suites.
    if (selected_test_filters.len == 0) {
        test_step.dependOn(&run_cli_tests.step);
        test_step.dependOn(&run_bge_m3_e2e_bench_tests.step);
        test_step.dependOn(&suite.run_gliner25_fuzz_tests.step);
    }
    return test_step;
}

pub fn addGliner25Trained(ctx: Context, gliner25_trained_check_module: *std.Build.Module) void {
    const b = ctx.b;
    const gliner25_trained_tests = b.addTest(.{
        .root_module = gliner25_trained_check_module,
        .filters = build_test_filters.select(b.allocator, ctx.args orelse &.{}, &.{"trained execution"}),
    });
    const run_gliner25_trained_tests = ctx.addRunArtifact(gliner25_trained_tests);
    ctx.step("gliner25-trained-check-test", "Test trained GLiNER2.5 execution envelopes, identity, admission and lifetimes without a model").dependOn(&run_gliner25_trained_tests.step);
}

fn addGliner25Fuzz(ctx: Context) *std.Build.Step.Run {
    const b = ctx.b;
    const no_error_tracing = b.option(bool, "gliner25-fuzz-no-error-tracing", "Work around Zig 0.16.0 fuzz runner error-trace mismatch for GLiNER25 only (use with --fuzz)") orelse false;
    // Keep the standard test runner for deterministic corpus and --fuzz runs.
    // This pure-Zig platform instance deliberately omits filesystem_capacity.c:
    // Zig 0.16 cannot instrument that C helper with its fuzz sanitizer profile.
    // Construct it from shared paths so root and package builds need no nested
    // package dependency and the ML imports share exactly this module identity.
    const platform = b.createModule(.{
        .root_source_file = b.path(b.pathJoin(&.{ ctx.paths.shared_lib_root, "lib/platform/src/root.zig" })),
        .target = ctx.target,
        .optimize = ctx.optimize,
        .link_libc = false,
    });
    const ml = b.createModule(.{
        .root_source_file = b.path(b.pathJoin(&.{ ctx.paths.shared_lib_root, "lib/ml/src/root.zig" })),
        .target = ctx.target,
        .optimize = ctx.optimize,
    });
    ml.addImport("antfly_platform", platform);
    const tests = b.addTest(.{
        .name = "gliner25-parser-properties",
        .root_module = b.createModule(.{
            .root_source_file = ctx.path("src/gliner25_fuzz.zig"),
            .target = ctx.target,
            .optimize = ctx.optimize,
            .link_libc = ctx.backend.link_libc,
            .error_tracing = if (no_error_tracing) false else null,
        }),
        .filters = &.{"GLiNER25 fuzz"},
    });
    tests.root_module.addImport("build_options", ctx.graph.build_options_mod);
    tests.root_module.addImport("antfly_platform", platform);
    tests.root_module.addImport("antfly_image", ctx.graph.image_mod);
    tests.root_module.addImport("inference_tokenizer", ctx.graph.inference_tokenizer_mod);
    tests.root_module.addImport("inference_hf_tokenizer", ctx.graph.inference_hf_tokenizer_mod);
    tests.root_module.addImport("inference_linalg", ctx.graph.inference_linalg_mod);
    tests.root_module.addImport("ml", ml);
    const run = ctx.addRunArtifact(tests);
    ctx.step("test-gliner25-fuzz", "Run bounded GLiNER2.5 parser, schema and document property tests").dependOn(&run.step);
    return run;
}
