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
const antfly_tests_build = @import("tests.zig");

pub fn addCliSteps(ctx: anytype) void {
    const b = ctx.b;
    const target = ctx.target;
    const wasm_target = ctx.wasm_target;
    const optimize = ctx.optimize;
    const lib_mod = ctx.lib_mod;
    const antfly_client_pkg_mod = ctx.antfly_client_pkg_mod;
    const httpx_mod = ctx.httpx_mod;
    const vellum_mod = ctx.vellum_mod;
    const raft_engine_mod = ctx.raft_engine_mod;
    const structlog_mod = ctx.structlog_mod;
    const platform_mod = ctx.platform_mod;
    const handlebars_mod = ctx.handlebars_mod;
    const build_options = ctx.build_options;
    const antfly_main = ctx.antfly_main;
    const antfly_bin_name = ctx.antfly_bin_name;
    const install_antfly = ctx.install_antfly;
    const lite_local_inference_runtime = ctx.lite_local_inference_runtime;
    const capi_steps = ctx.capi_steps;
    const run_antfly_main_tests = ctx.run_antfly_main_tests;
    const run_lite_cmd_tests = ctx.run_lite_cmd_tests;
    const run_lite_native_tests = ctx.run_lite_native_tests;
    const run_antfly_embedded_pkg_tests = ctx.run_antfly_embedded_pkg_tests;

    const lite_core_main_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/lite_core_main.zig"),
        .target = target,
        .optimize = optimize,
    });
    lite_core_main_mod.addImport("antfly-zig", lib_mod);
    lite_core_main_mod.addImport("antfly-client", antfly_client_pkg_mod);
    lite_core_main_mod.addImport("httpx", httpx_mod);
    lite_core_main_mod.addImport("antfly_vellum", vellum_mod);
    lite_core_main_mod.addImport("raft_engine", raft_engine_mod);
    lite_core_main_mod.addImport("structlog", structlog_mod);
    lite_core_main_mod.addImport("antfly_platform", platform_mod);
    lite_core_main_mod.addImport("handlebars", handlebars_mod);
    const lite_core_main = b.addExecutable(.{
        .name = "antfly-lite-core",
        .root_module = lite_core_main_mod,
    });
    const lite_cli_smoke = b.addExecutable(.{
        .name = "antfly-lite-cli-smoke",
        .root_module = b.createModule(.{
            .root_source_file = b.path("examples/antfly_lite_cli_smoke.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_lite_core_cli_smoke = b.addRunArtifact(lite_cli_smoke);
    run_lite_core_cli_smoke.addArtifactArg(lite_core_main);
    const run_lite_full_cli_smoke = b.addRunArtifact(lite_cli_smoke);
    run_lite_full_cli_smoke.addArtifactArg(antfly_main);
    const lite_core_main_tests = b.addTest(.{
        .root_module = lite_core_main_mod,
        .filters = antfly_tests_build.selectTestFilters(b, &antfly_tests_build.PackageTestFilters.lite_core_main),
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lite_core_main_tests = b.addRunArtifact(lite_core_main_tests);
    const install_lite_core_main = b.addInstallArtifact(lite_core_main, .{ .dest_sub_path = antfly_bin_name });

    const lite_core_step = b.step("lite-core", "Build Antfly Lite core CLI, embedded package check, and libantfly C ABI");
    lite_core_step.dependOn(&install_lite_core_main.step);
    lite_core_step.dependOn(&capi_steps.install_antfly_capi_lib.step);
    lite_core_step.dependOn(&capi_steps.install_capi_header.step);
    lite_core_step.dependOn(&run_lite_core_main_tests.step);
    lite_core_step.dependOn(&capi_steps.run_lite_capi_smoke.step);
    lite_core_step.dependOn(&capi_steps.run_lite_go_tests.step);
    lite_core_step.dependOn(&capi_steps.run_lite_go_example.step);
    lite_core_step.dependOn(&capi_steps.run_lite_go_retrieval_template.step);
    lite_core_step.dependOn(&run_lite_core_cli_smoke.step);
    lite_core_step.dependOn(&run_antfly_embedded_pkg_tests.step);

    const lite_full_step = b.step("lite-full", "Build the full Antfly CLI with Lite commands, local inference runtime capability, embedded package check, and libantfly C ABI");
    if (!lite_local_inference_runtime) {
        lite_full_step.dependOn(&b.addFail("lite-full requires -Dlite-local-inference-runtime=true so Lite status and bindings advertise the local inference runtime").step);
    }
    lite_full_step.dependOn(&install_antfly.step);
    lite_full_step.dependOn(&capi_steps.install_antfly_capi_lib.step);
    lite_full_step.dependOn(&capi_steps.install_capi_header.step);
    lite_full_step.dependOn(&run_antfly_main_tests.step);
    lite_full_step.dependOn(&run_lite_cmd_tests.step);
    lite_full_step.dependOn(&run_lite_native_tests.step);
    lite_full_step.dependOn(&capi_steps.run_lite_capi_smoke.step);
    lite_full_step.dependOn(&capi_steps.run_lite_go_tests.step);
    lite_full_step.dependOn(&capi_steps.run_lite_go_example.step);
    lite_full_step.dependOn(&capi_steps.run_lite_go_retrieval_template.step);
    lite_full_step.dependOn(&run_lite_full_cli_smoke.step);
    lite_full_step.dependOn(&run_antfly_embedded_pkg_tests.step);

    const lite_wasm_profile_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/lite_wasm_profile.zig"),
        .target = wasm_target,
        .optimize = optimize,
    });
    lite_wasm_profile_mod.addOptions("build_options", build_options);
    const lite_wasm_profile = b.addExecutable(.{
        .name = "antfly_lite_wasm_profile",
        .root_module = lite_wasm_profile_mod,
    });
    lite_wasm_profile.entry = .disabled;
    lite_wasm_profile.rdynamic = true;
    lite_wasm_profile.export_memory = true;
    const install_lite_wasm_profile = b.addInstallArtifact(lite_wasm_profile, .{
        .dest_sub_path = "antfly-lite-wasm/antfly_lite_wasm_profile.wasm",
    });
    const lite_wasm_profile_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("pkg/antfly/src/lite_wasm_profile.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    lite_wasm_profile_tests.root_module.addOptions("build_options", build_options);
    const run_lite_wasm_profile_tests = b.addRunArtifact(lite_wasm_profile_tests);

    const lite_test_step = b.step("lite-test", "Run Antfly Lite CLI, native, C ABI, Go binding, packaging, and WASM profile checks");
    lite_test_step.dependOn(&run_lite_core_main_tests.step);
    lite_test_step.dependOn(&run_lite_cmd_tests.step);
    lite_test_step.dependOn(&run_lite_native_tests.step);
    lite_test_step.dependOn(&capi_steps.run_lite_capi_smoke.step);
    lite_test_step.dependOn(&capi_steps.run_lite_go_tests.step);
    lite_test_step.dependOn(&capi_steps.run_lite_go_example.step);
    lite_test_step.dependOn(&capi_steps.run_lite_go_retrieval_template.step);
    lite_test_step.dependOn(&run_lite_core_cli_smoke.step);
    lite_test_step.dependOn(&run_lite_full_cli_smoke.step);
    lite_test_step.dependOn(&install_lite_wasm_profile.step);
    lite_test_step.dependOn(&run_lite_wasm_profile_tests.step);
    lite_test_step.dependOn(&capi_steps.run_cabi_packaging_tests.step);
    lite_test_step.dependOn(&run_antfly_embedded_pkg_tests.step);

    const lite_dev_step = b.step("lite-dev", "Build the Antfly Lite development profile with CLI diagnostics and C ABI checks");
    lite_dev_step.dependOn(&install_antfly.step);
    lite_dev_step.dependOn(&capi_steps.install_antfly_capi_lib.step);
    lite_dev_step.dependOn(&capi_steps.install_capi_header.step);
    lite_dev_step.dependOn(&run_antfly_main_tests.step);
    lite_dev_step.dependOn(&run_lite_core_main_tests.step);
    lite_dev_step.dependOn(&run_lite_cmd_tests.step);
    lite_dev_step.dependOn(&run_lite_native_tests.step);
    lite_dev_step.dependOn(&capi_steps.run_lite_capi_smoke.step);
    lite_dev_step.dependOn(&capi_steps.run_lite_go_tests.step);
    lite_dev_step.dependOn(&capi_steps.run_lite_go_example.step);
    lite_dev_step.dependOn(&capi_steps.run_lite_go_retrieval_template.step);
    lite_dev_step.dependOn(&run_lite_core_cli_smoke.step);
    lite_dev_step.dependOn(&run_lite_full_cli_smoke.step);
    lite_dev_step.dependOn(&install_lite_wasm_profile.step);
    lite_dev_step.dependOn(&run_lite_wasm_profile_tests.step);
    lite_dev_step.dependOn(&capi_steps.run_cabi_packaging_tests.step);
    lite_dev_step.dependOn(&capi_steps.run_capi_tests.step);
    lite_dev_step.dependOn(&run_antfly_embedded_pkg_tests.step);
}
