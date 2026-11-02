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
    // Native CPU Debug peaked at 12,070,764,544 bytes with cold compiler
    // caches. 12,672 MiB retains 10% headroom rounded up to 128 MiB.
    // Accelerator and other-host reservations retain their existing profiles.
    const native_cpu = b.graph.host.result.os.tag == .macos and b.graph.host.result.cpu.arch == .aarch64 and
        ctx.target.result.os.tag == .macos and ctx.target.result.cpu.arch == .aarch64 and
        !ctx.backend.enable_metal and !ctx.backend.enable_cuda and !ctx.backend.enable_pjrt;
    const tests = b.addTest(.{
        // Zig 0.17's focused CUDA serving suite reached 19.57 GB on aarch64
        // Linux. Reserve 24 GiB so the scheduler admits its measured compiler
        // footprint and leaves room for the socket/reference coverage.
        // The focused native CPU ReleaseFast suite peaked at 10,032,644,096
        // bytes. Its old 9 GiB declaration rejected a successful compilation.
        .max_rss = if (native_cpu and ctx.optimize == .debug) 12_672 * 1024 * 1024 else @as(usize, if (native_cpu) 11 else if (ctx.backend.enable_cuda) 24 else if (ctx.target.result.os.tag == .macos and ctx.backend.enable_metal) 14 else 9) * 1024 * 1024 * 1024,
        .root_module = b.createModule(.{
            .root_source_file = ctx.path("src/inference.zig"),
            .target = ctx.target,
            .optimize = ctx.optimize,
        }),
        .filters = main_test_filters,
        .test_runner = runtime_filter_test_runner,
    });
    tests.root_module.strip = ctx.strip;
    tests.root_module.addImport("build_info", ctx.graph.build_info_mod);
    tests.root_module.addImport("antfly_cloudflare", ctx.graph.inference_mod.import_table.get("antfly_cloudflare").?);
    tests.root_module.addImport("antfly_decisions", ctx.graph.inference_mod.import_table.get("antfly_decisions").?);
    tests.root_module.addImport("antfly_structured", ctx.graph.inference_mod.import_table.get("antfly_structured").?);
    ctx.graph.identities.addImports(tests.root_module);
    tests.root_module.addImport("build_options", ctx.graph.qualification_build_options_mod);
    tests.root_module.addImport("antfly-json", ctx.graph.json_mod);
    tests.root_module.addImport("httpx", ctx.graph.httpx_mod);
    tests.root_module.addImport("protobuf", ctx.graph.protobuf_mod);
    tests.root_module.addImport("inference_api", ctx.graph.inference_api_mod);
    tests.root_module.addImport("antfly_generating_openapi", ctx.graph.generating_openapi_mod);
    tests.root_module.addImport("antfly_extraction_openapi", ctx.graph.extraction_openapi_mod);
    tests.root_module.addImport("antfly_extracting", ctx.graph.extracting_mod);
    // Direct reader API tests share the runtime's request and result types.
    // Reuse the root owner so native reader options, imports, and framework
    // linkage stay consistent with the runtime under test.
    const readers_mod = ctx.graph.inference_mod.import_table.get("antfly_readers") orelse blk: {
        const standalone_readers_mod = b.createModule(.{
            .root_source_file = b.path(b.pathJoin(&.{ ctx.paths.shared_lib_root, "lib/readers/src/mod.zig" })),
            .target = ctx.target,
            .optimize = ctx.optimize,
        });
        standalone_readers_mod.addImport("antfly_document_runs", ctx.graph.document_runs_mod);
        standalone_readers_mod.addImport("httpx", ctx.graph.httpx_mod);
        standalone_readers_mod.addImport("inference_api", ctx.graph.inference_api_mod);
        standalone_readers_mod.addImport("antfly_google", ctx.graph.google_mod);
        standalone_readers_mod.addImport("antfly_reader_config", ctx.graph.reader_config_mod);
        standalone_readers_mod.addImport("antfly_scraping", ctx.graph.scraping_mod);
        standalone_readers_mod.addImport("antfly_image", ctx.graph.image_mod);
        break :blk standalone_readers_mod;
    };
    tests.root_module.addImport("antfly_readers", readers_mod);
    tests.root_module.addImport("antfly_document_runs", ctx.graph.document_runs_mod);
    tests.root_module.addImport("antfly_transcribing", ctx.graph.transcribing_mod);
    tests.root_module.addImport("inference_audio", ctx.graph.inference_audio_mod);
    tests.root_module.addImport("antfly_media", ctx.graph.inference_mod.import_table.get("antfly_media").?);
    tests.root_module.addImport("antfly_pdf", ctx.graph.inference_mod.import_table.get("antfly_pdf").?);
    tests.root_module.addImport("antfly_documents", ctx.graph.inference_mod.import_table.get("antfly_documents").?);
    tests.root_module.addImport("antfly_video", ctx.graph.inference_mod.import_table.get("antfly_video").?);
    tests.root_module.addImport("antfly_pdf", ctx.graph.inference_mod.import_table.get("antfly_pdf").?);
    tests.root_module.addImport("inference_chunker", ctx.graph.inference_chunker_mod);
    tests.root_module.addImport("antfly_pdf", ctx.graph.inference_mod.import_table.get("antfly_pdf").?);
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
    tests.root_module.addImport("antfly_runtime_abi", ctx.graph.runtime_abi_mod);
    tests.root_module.addImport("antfly_reader_config", ctx.graph.reader_config_mod);
    tests.root_module.addImport("inference_internal", tests.root_module);
    if (ctx.graph.inference_client_mod) |mod| {
        tests.root_module.addImport("inference_client", mod);
    }
    if (ctx.backend.enable_system_blas) {
        runtime_build.configureSystemBlas(b, tests.root_module, ctx.target, ctx.backend.blas_root);
    }
    // No-copy host buffer wrapping can succeed even for misaligned pointers
    // on some devices. Exercise the copied-output fallback deterministically.
    if (ctx.backend.enable_metal) tests.root_module.addCMacro("ANTFLY_METAL_TEST_FORCE_HOST_OUTPUT_COPY", "1");
    runtime_build.configureMetal(b, tests.root_module, ctx.target, ctx.backend.enable_metal, ctx.paths);
    runtime_build.configureCudaVideo(b, tests.root_module, ctx.target, ctx.backend.enable_cuda, ctx.paths);
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
    cli_tests.root_module.strip = ctx.strip;
    cli_tests.root_module.addImport("build_info", ctx.graph.build_info_mod);
    cli_tests.root_module.addImport("inference", ctx.graph.inference_mod);
    cli_tests.root_module.addImport("build_options", ctx.graph.build_options_mod);
    cli_tests.root_module.addImport("structlog", ctx.graph.structlog_mod);
    cli_tests.root_module.addImport("antfly_platform", ctx.graph.platform_mod);
    cli_tests.root_module.link_libc = ctx.backend.link_libc;

    const run_cli_tests = ctx.addRunArtifact(cli_tests);
    // Runtime passthrough arguments arrive after Zig 0.17's configure phase.
    // The library runner remains strict; its default-only CLI companion may
    // be empty only when an explicit include selector matches no CLI test.
    run_cli_tests.addArg("--allow-empty-filtered-suite");
    for (selected_test_filters) |filter| {
        run_cli_tests.addArgs(&.{ "--test-filter", filter });
    }
    build_test_filters.addRuntimeControls(run_cli_tests, ctx.args orelse &.{});
    const run_tests = ctx.addRunArtifact(tests);
    const evidence_module = b.createModule(.{
        .root_source_file = ctx.path("src/pipelines/grammar.zig"),
        .target = ctx.target,
        .optimize = ctx.optimize,
    });
    evidence_module.addImport("inference_tokenizer", ctx.graph.inference_tokenizer_mod);
    const evidence_tests = b.addTest(.{ .root_module = evidence_module, .filters = &.{ "canonical evidence", "gbnf", "json schema grammar" } });
    const run_evidence_tests = ctx.addRunArtifact(evidence_tests);
    run_tests.step.dependOn(&run_evidence_tests.step);
    ctx.step("test-structured-token-evidence", "Check canonical finite-value token admission and masking").dependOn(&run_evidence_tests.step);
    const structured_module = ctx.graph.inference_mod.import_table.get("antfly_structured").?;
    const attribution_module = b.createModule(.{ .root_source_file = structured_module.root_source_file, .target = ctx.target, .optimize = ctx.optimize });
    for (structured_module.import_table.keys(), structured_module.import_table.values()) |name, module| attribution_module.addImport(name, module);
    const attribution_tests = b.addTest(.{ .root_module = attribution_module, .filters = &.{"gemma native"} });
    const run_attribution_tests = ctx.addRunArtifact(attribution_tests);
    ctx.step("test-structured-attribution", "Check real native finite-value evidence attribution").dependOn(&run_attribution_tests.step);
    const token_bytes_step = ctx.step("test-token-bytes", "Check additive tokenizer bytes without sequence cleanup");
    for ([_]*std.Build.Module{ ctx.graph.inference_tokenizer_mod, ctx.graph.inference_hf_tokenizer_mod }, 0..) |source, i| {
        const root = if (i == 0) b.path(b.pathJoin(&.{ ctx.paths.shared_lib_root, "lib/tokenizer/src/sentencepiece.zig" })) else source.root_source_file;
        const module = b.createModule(.{ .root_source_file = root, .target = ctx.target, .optimize = ctx.optimize });
        for (source.import_table.keys(), source.import_table.values()) |name, dependency| module.addImport(name, dependency);
        const token_tests = b.addTest(.{ .root_module = module, .filters = &.{"raw token bytes"} });
        const run = ctx.addRunArtifact(token_tests);
        token_bytes_step.dependOn(&run.step);
        run_tests.step.dependOn(&run.step);
    }
    // Keep backend packaging covered even when CUDA implementation tests are
    // excluded. A CPU/Metal test product must not retain a device image.
    const artifact_module = b.createModule(.{
        .root_source_file = ctx.path("src/ops/cuda/artifact.zig"),
        .target = ctx.target,
        .optimize = ctx.optimize,
    });
    artifact_module.addImport("build_options", ctx.graph.qualification_build_options_mod);
    const artifact_tests = b.addTest(.{ .root_module = artifact_module });
    const run_artifact_tests = ctx.addRunArtifact(artifact_tests);
    run_tests.step.dependOn(&run_artifact_tests.step);
    ctx.step("test-cuda-artifact", "Check that CUDA device images match the enabled backend").dependOn(&run_artifact_tests.step);
    for (selected_test_filters) |filter| {
        run_tests.addArgs(&.{ "--test-filter", filter });
    }
    build_test_filters.addRuntimeControls(run_tests, ctx.args orelse &.{});
    // Focused inference invocations retain their historical reachability;
    // the default gates assign these tests to the shared finetuning owner.
    if (selected_test_filters.len == 0) {
        for (@import("finetune/tests.zig").inference_overlap_filters) |filter|
            run_tests.addArgs(&.{ "--skip-test-filter", filter });
    }
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
    const media_tests = b.addTest(.{ .root_module = b.createModule(.{
        .root_source_file = b.path(b.pathJoin(&.{ ctx.paths.shared_lib_root, "lib/media/src/inference.zig" })),
        .target = ctx.target,
        .optimize = ctx.optimize,
    }) });
    const media_step = ctx.step("test-video-media", "Run bounded media and NVDEC callback tests without a GPU");
    media_step.dependOn(&ctx.addRunArtifact(media_tests).step);
    if (ctx.target.result.os.tag == .linux) {
        const callbacks = b.addExecutable(.{ .name = "video-decode-callback-tests", .root_module = b.createModule(.{
            .target = ctx.target,
            .optimize = ctx.optimize,
            .link_libc = true,
        }) });
        callbacks.root_module.addCSourceFile(.{ .file = ctx.path("tests/video_decode_test.c"), .flags = &.{ "-std=c11", "-Wall", "-Wextra", "-Werror", "-UNDEBUG" } });
        media_step.dependOn(&ctx.addRunArtifact(callbacks).step);
    }
    test_step.dependOn(media_step);
    ctx.step("test-inference-unit", "Run inference unit tests with optional filters, without independent library suites").dependOn(&run_tests.step);
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
        ctx.graph.linkBuildInfo(fixture.root_module);
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
    const laya_cuda_tests = b.addTest(.{
        .name = "laya-cuda-tests",
        .max_rss = 9 * 1024 * 1024 * 1024,
        .root_module = suite.tests.root_module,
        .filters = &.{ "laya ", "cuda support gate", "readiness inventory" },
        .test_runner = .{ .path = ctx.path("src/test_runner_filter.zig"), .mode = .simple },
    });
    const install_laya_cuda_tests = b.addInstallArtifact(laya_cuda_tests, .{});
    ctx.step("laya-cuda-test-build", "Build Laya CUDA qualification executable without running it").dependOn(&install_laya_cuda_tests.step);
    const run_laya_cuda_tests = ctx.addRunArtifact(laya_cuda_tests);
    run_laya_cuda_tests.setEnvironmentVariable("ANTFLY_LAYA_REQUIRE_TESTS", "1");
    run_laya_cuda_tests.setEnvironmentVariable("ANTFLY_LAYA_BACKEND", "cuda");
    ctx.step("laya-cuda-test", "Require CUDA and pinned Laya fixtures").dependOn(&run_laya_cuda_tests.step);
    // Dedicated hardware gate: reuse the inference test module and frozen
    // CPU/Metal exercises, but missing CUDA must never become a successful skip.
    const gliner25_cuda_tests = b.addTest(.{
        .name = "gliner25-cuda-tests",
        .max_rss = 7 * 1024 * 1024 * 1024,
        .root_module = suite.tests.root_module,
        .filters = &.{ "CUDA boundary", "deberta training CUDA", "trainer CUDA", "adapter CUDA" },
        .test_runner = .{ .path = ctx.path("src/test_runner_filter.zig"), .mode = .simple },
    });
    const run_gliner25_cuda_tests = ctx.addRunArtifact(gliner25_cuda_tests);
    run_gliner25_cuda_tests.setEnvironmentVariable("TERMITE_REQUIRE_CUDA_TESTS", "1");
    const cuda_step = ctx.step("test-gliner25-cuda", "Run required-hardware GLiNER2.5 CUDA parity, optimizer and lifecycle tests");
    cuda_step.dependOn(&run_gliner25_cuda_tests.step);
    cuda_step.dependOn(&quant_kernel_codegen_test_check.step);
    cuda_step.dependOn(&cuda_artifact_source_policy_check.step);
    // Serving qualification is separate from the optimizer/kernel gate above.
    // A passing run requires real pinned artifacts and the production policy;
    // opt-in fixtures must not silently turn a release gate into skips.
    const production_cuda_filters = [_][]const u8{
        "production CUDA required artifacts",
        "CUDA pinned base full inference",
        "CUDA pinned multi full inference",
        "GLiNER bounded window composition pinned CUDA",
        "frozen CUDA family profiles",
        "CUDA boundary multi profiles",
        "CUDA boundary batch rows",
        "GLiNER production CUDA distinct input batches",
        "Decide published checkpoint",
        "Decide Q8 published bundle",
        "Decide Q8 CUDA classification oracle",
        "GLiNER2.5-Decide native parity",
        "Decide Q8 CUDA qualification binds",
        "Decide CUDA legacy geometry",
        "exact DeBERTa CUDA qualification",
        "GLiNER2.5-Decide CUDA classification oracle parity",
        "GLiNER production CUDA typed decisions",
        "GLiNER production CUDA extraction through concurrent HTTP",
        "pinned CUDA extraction direct and HTTP",
    };
    const gliner25_production_cuda_tests = b.addTest(.{
        .name = "gliner25-production-cuda-tests",
        .max_rss = 24 * 1024 * 1024 * 1024,
        .root_module = suite.tests.root_module,
        .filters = &production_cuda_filters,
        .test_runner = .{ .path = ctx.path("src/test_runner_filter.zig"), .mode = .simple },
    });
    ctx.step("gliner25-production-cuda-test-build", "Build pinned GLiNER2.5 production CUDA serving qualification").dependOn(&b.addInstallArtifact(gliner25_production_cuda_tests, .{}).step);
    const run_gliner25_production_cuda_tests = ctx.addRunArtifact(gliner25_production_cuda_tests);
    for (production_cuda_filters) |filter| run_gliner25_production_cuda_tests.addArgs(&.{ "--test-filter", filter });
    run_gliner25_production_cuda_tests.setEnvironmentVariable("TERMITE_REQUIRE_CUDA_TESTS", "1");
    run_gliner25_production_cuda_tests.setEnvironmentVariable("ANTFLY_GLINER25_REQUIRE_PRODUCTION_MODELS", "1");
    run_gliner25_production_cuda_tests.setEnvironmentVariable("ANTFLY_GLINER_DECIDE_BACKEND", "cuda");
    const production_cuda_step = ctx.step("test-gliner25-production-cuda", "Require pinned GLiNER2.5 CUDA parity and production serving checks");
    if (ctx.backend.enable_cuda) {
        production_cuda_step.dependOn(&run_gliner25_production_cuda_tests.step);
    } else {
        production_cuda_step.dependOn(&b.addFail("GLiNER2.5 production CUDA qualification requires -Dcuda=true; skipped GPU tests cannot qualify serving").step);
    }
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
    // Reuse the runtime graph's platform module so tokenizer sources have one
    // module owner even when this test imports them through multiple paths.
    const platform = ctx.graph.platform_mod;
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
    tests.root_module.addImport("antfly_runtime_abi", ctx.graph.runtime_abi_mod);
    tests.root_module.addImport("antfly_document_runs", ctx.graph.document_runs_mod);
    tests.root_module.addImport("build_options", ctx.graph.build_options_mod);
    tests.root_module.addImport("antfly_platform", platform);
    tests.root_module.addImport("antfly_runtime_abi", ctx.graph.runtime_abi_mod);
    tests.root_module.addImport("antfly_image", ctx.graph.image_mod);
    tests.root_module.addImport("antfly_extracting", ctx.graph.extracting_mod);
    tests.root_module.addImport("inference_tokenizer", ctx.graph.inference_tokenizer_mod);
    tests.root_module.addImport("inference_hf_tokenizer", ctx.graph.inference_hf_tokenizer_mod);
    tests.root_module.addImport("inference_linalg", ctx.graph.inference_linalg_mod);
    tests.root_module.addImport("ml", ml);
    const run = ctx.addRunArtifact(tests);
    ctx.step("test-gliner25-fuzz", "Run bounded GLiNER2.5 parser, schema and document property tests").dependOn(&run.step);
    return run;
}
