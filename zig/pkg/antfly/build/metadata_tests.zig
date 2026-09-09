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
const chainLabeledFilteredTests = @import("test_support.zig").chainLabeledFilteredTests;
const selectTestFilters = @import("test_support.zig").selectTestFilters;
const addFilteredTestRunArtifact = @import("test_support.zig").addFilteredTestRunArtifact;

pub const AddTestsOptions = struct {
    target: std.Build.ResolvedTarget,
    antfly_test_mod: *std.Build.Module,
};
pub const AddTestsResult = struct {
    lib_metadata_runtime_filters: []const []const u8,
    lib_metadata_test_step: *std.Build.Step,
    run_lib_metadata_sim_smoke_tests: *std.Build.Step.Run,
    run_lib_metadata_vopr_tests: *std.Build.Step.Run,
    lib_metadata_vopr_chaos_tests: *std.Build.Step.Compile,
    lib_metadata_transition_chaos_filters: []const []const u8,
    lib_metadata_public_chaos_filters: []const []const u8,
    lib_metadata_placement_chaos_filters: []const []const u8,
    run_lib_metadata_sim_public_tests: *std.Build.Step.Run,
};

pub fn addTests(b: *std.Build, options: AddTestsOptions) AddTestsResult {
    const target = options.target;
    const antfly_test_mod = options.antfly_test_mod;
    const lib_metadata_runtime_filters = selectTestFilters(b, &.{"metadata."});
    const lib_metadata_test_step = b.step("antfly-metadata-test", "Run root-module metadata tests only");

    const lib_metadata_table_workflow_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{
            "table workflow can drive real metadata service topology and split setup",
            "table workflow can drive placement intents through the real metadata control loop",
        },
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lib_metadata_table_workflow_tests = addFilteredTestRunArtifact(b, lib_metadata_table_workflow_tests);
    const lib_metadata_table_workflow_test_step = b.step("antfly-metadata-table-workflow-test", "Run focused metadata table workflow tests");
    lib_metadata_table_workflow_test_step.dependOn(&run_lib_metadata_table_workflow_tests.step);

    const lib_metadata_sim_default_filters = [_][]const u8{"metadata http cluster simulation"};
    const lib_metadata_sim_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = selectTestFilters(b, &lib_metadata_sim_default_filters),
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lib_metadata_sim_tests = addFilteredTestRunArtifact(b, lib_metadata_sim_tests);
    const lib_metadata_sim_test_step = b.step("antfly-metadata-sim-test", "Run metadata real-HTTP simulation tests only");
    lib_metadata_sim_test_step.dependOn(&run_lib_metadata_sim_tests.step);

    const lib_metadata_sim_core_default_filters = [_][]const u8{
        "metadata http cluster simulation drives table placement convergence",
        "metadata http cluster simulation converges placement after candidate churn",
        "metadata http cluster simulation drives split intent through the control loop",
        "metadata http cluster simulation drives merge intent through the control loop",
        "metadata http cluster simulation drives automatic split through the control loop",
        "metadata http cluster simulation drives automatic merge through the control loop",
        "metadata http cluster simulation uses live median key for automatic split planning",
        "metadata http cluster simulation uses remote live median key when metadata leader is not a shard replica",
        "metadata http cluster simulation publishes split topology after finalize",
        "metadata http cluster simulation publishes merge topology after finalize",
        "metadata http cluster simulation provisions split destination replicas across nodes",
        "metadata http cluster simulation retires merge donor replicas across nodes",
    };
    const lib_metadata_sim_core_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = selectTestFilters(b, &lib_metadata_sim_core_default_filters),
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lib_metadata_sim_core_tests = addFilteredTestRunArtifact(b, lib_metadata_sim_core_tests);
    const lib_metadata_sim_core_test_step = b.step("antfly-metadata-sim-core-test", "Run deterministic metadata virtual-transport simulation tests without public API or chaos");
    lib_metadata_sim_core_test_step.dependOn(&run_lib_metadata_sim_core_tests.step);

    const lib_metadata_sim_smoke_default_filters = [_][]const u8{
        "metadata sim split runtime preserves source identity namespace",
        "metadata sim merge runtime records doc identity reassignment opt-in",
        "metadata http cluster simulation drives table placement convergence",
        "metadata http cluster simulation drives split intent through the control loop",
    };
    const lib_metadata_sim_smoke_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = selectTestFilters(b, &lib_metadata_sim_smoke_default_filters),
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lib_metadata_sim_smoke_tests = addFilteredTestRunArtifact(b, lib_metadata_sim_smoke_tests);
    const lib_metadata_sim_smoke_test_step = b.step("antfly-metadata-sim-smoke-test", "Run fast metadata virtual-transport simulation smoke tests");
    lib_metadata_sim_smoke_test_step.dependOn(&run_lib_metadata_sim_smoke_tests.step);

    const lib_metadata_vopr_default_filters = [_][]const u8{
        "metadata VOPR seeded smoke campaign",
    };
    const lib_metadata_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = selectTestFilters(b, &lib_metadata_vopr_default_filters),
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lib_metadata_vopr_tests = addFilteredTestRunArtifact(b, lib_metadata_vopr_tests);
    const lib_metadata_vopr_test_step = b.step("antfly-metadata-vopr-test", "Run seeded metadata virtual-operation campaign tests");
    lib_metadata_vopr_test_step.dependOn(&run_lib_metadata_vopr_tests.step);

    const lib_metadata_vopr_chaos_default_filters = [_][]const u8{
        "metadata VOPR expanded generated workload campaign",
    };
    const lib_metadata_vopr_chaos_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = selectTestFilters(b, &lib_metadata_vopr_chaos_default_filters),
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lib_metadata_vopr_chaos_tests = addFilteredTestRunArtifact(b, lib_metadata_vopr_chaos_tests);
    const lib_metadata_vopr_chaos_test_step = b.step("antfly-metadata-vopr-chaos-test", "Run expanded metadata VOPR generated workload campaigns");
    lib_metadata_vopr_chaos_test_step.dependOn(&run_lib_metadata_vopr_chaos_tests.step);

    const lib_metadata_transition_chaos_default_filters = [_][]const u8{
        "metadata http cluster simulation completes automatic split after metadata leader restart",
        "metadata http cluster simulation completes automatic split after metadata leader partition",
        "metadata http cluster simulation completes automatic split under delayed raft transport",
        "metadata http cluster simulation completes automatic split after leader restart under delayed raft transport",
        "metadata http cluster simulation completes automatic split after source group leader restart",
        "metadata http cluster simulation completes automatic split after destination group leader restart",
        "metadata http cluster simulation completes automatic split after leader partition under delayed raft transport",
        "metadata http cluster simulation completes automatic merge after metadata leader restart",
        "metadata http cluster simulation completes automatic merge after donor group leader restart",
        "metadata http cluster simulation completes automatic merge after receiver group leader restart",
        "metadata http cluster simulation completes automatic merge after metadata leader partition",
        "metadata http cluster simulation completes automatic merge under delayed raft transport",
        "metadata http cluster simulation completes automatic merge after leader restart under delayed raft transport",
        "metadata http cluster simulation completes automatic merge after leader partition under delayed raft transport",
        "metadata http cluster simulation survives leader restart before forced automatic split reconcile",
    };
    const lib_metadata_public_chaos_default_filters = [_][]const u8{
        "metadata http cluster simulation serves public traffic across automatic split under delayed raft transport",
        "metadata http cluster simulation serves public traffic across automatic split after leader restart under delayed raft transport",
        "metadata http cluster simulation serves public traffic across automatic split after source leader restart under delayed raft transport",
        "metadata http cluster simulation serves public traffic across automatic split after leader partition under delayed raft transport",
        "metadata http cluster simulation serves public traffic across automatic split after metadata leader partition",
        "metadata http cluster simulation serves public traffic across automatic merge under delayed raft transport",
        "metadata http cluster simulation serves public traffic across automatic merge after leader restart under delayed raft transport",
        "metadata http cluster simulation serves public traffic across automatic merge after donor leader restart under delayed raft transport",
        "metadata http cluster simulation serves public traffic across automatic merge after leader partition under delayed raft transport",
        "metadata http cluster simulation serves public traffic across automatic merge after metadata leader partition",
    };
    const lib_metadata_placement_chaos_default_filters = [_][]const u8{
        "metadata http cluster simulation survives metadata leader restart during placement reconcile",
        "metadata http cluster simulation drops table topology across leader restart",
    };
    const lib_metadata_transition_chaos_filters = selectTestFilters(b, &lib_metadata_transition_chaos_default_filters);
    const lib_metadata_public_chaos_filters = selectTestFilters(b, &lib_metadata_public_chaos_default_filters);
    const lib_metadata_placement_chaos_filters = selectTestFilters(b, &lib_metadata_placement_chaos_default_filters);

    const lib_metadata_transition_chaos_test_step = b.step("antfly-metadata-transition-chaos-test", "Run metadata split/merge transition restart and partition chaos simulations");
    var metadata_transition_chaos_progress_tail: ?*std.Build.Step = null;
    metadata_transition_chaos_progress_tail = chainLabeledFilteredTests(b, antfly_test_mod, "antfly-metadata-transition-chaos-test", lib_metadata_transition_chaos_filters, metadata_transition_chaos_progress_tail);
    lib_metadata_transition_chaos_test_step.dependOn(metadata_transition_chaos_progress_tail.?);

    const lib_metadata_public_chaos_test_step = b.step("antfly-metadata-public-chaos-test", "Run metadata public traffic split/merge chaos simulations");
    var metadata_public_chaos_progress_tail: ?*std.Build.Step = null;
    metadata_public_chaos_progress_tail = chainLabeledFilteredTests(b, antfly_test_mod, "antfly-metadata-public-chaos-test", lib_metadata_public_chaos_filters, metadata_public_chaos_progress_tail);
    lib_metadata_public_chaos_test_step.dependOn(metadata_public_chaos_progress_tail.?);

    const lib_metadata_placement_chaos_test_step = b.step("antfly-metadata-placement-chaos-test", "Run metadata placement restart chaos simulations");
    var metadata_placement_chaos_progress_tail: ?*std.Build.Step = null;
    metadata_placement_chaos_progress_tail = chainLabeledFilteredTests(b, antfly_test_mod, "antfly-metadata-placement-chaos-test", lib_metadata_placement_chaos_filters, metadata_placement_chaos_progress_tail);
    lib_metadata_placement_chaos_test_step.dependOn(metadata_placement_chaos_progress_tail.?);

    const lib_metadata_chaos_test_step = b.step("antfly-metadata-chaos-test", "Run metadata delayed/restart/partition chaos simulations");
    var metadata_chaos_progress_tail: ?*std.Build.Step = null;
    metadata_chaos_progress_tail = chainLabeledFilteredTests(b, antfly_test_mod, "antfly-metadata-transition-chaos-test", lib_metadata_transition_chaos_filters, metadata_chaos_progress_tail);
    metadata_chaos_progress_tail = chainLabeledFilteredTests(b, antfly_test_mod, "antfly-metadata-public-chaos-test", lib_metadata_public_chaos_filters, metadata_chaos_progress_tail);
    metadata_chaos_progress_tail = chainLabeledFilteredTests(b, antfly_test_mod, "antfly-metadata-placement-chaos-test", lib_metadata_placement_chaos_filters, metadata_chaos_progress_tail);
    lib_metadata_chaos_test_step.dependOn(metadata_chaos_progress_tail.?);

    const lib_metadata_sim_public_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{
            "public api linearizable read driver ignores a delayed earlier generation",
            "metadata http cluster simulation serves public lifecycle from a non-host node after public create",
            "metadata http cluster simulation seeds default admin for auth-enabled public api",
            "metadata http cluster simulation forwards public split flow from a non-host node after public create",
            "metadata http cluster simulation forwards public merge flow from a non-host node after public create",
        },
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
        // This broad macOS ReleaseFast simulation root has measured above
        // 12 GiB. Reserve its observed class without serializing the suite.
        .max_rss = @as(usize, if (target.result.os.tag == .macos) 14 else 7) * 1024 * 1024 * 1024,
    });
    const run_lib_metadata_sim_public_tests = addFilteredTestRunArtifact(b, lib_metadata_sim_public_tests);
    const lib_metadata_sim_public_test_step = b.step("antfly-metadata-sim-public-test", "Run metadata public lifecycle/split/merge simulation tests");
    lib_metadata_sim_public_test_step.dependOn(&run_lib_metadata_sim_public_tests.step);

    return .{
        .lib_metadata_runtime_filters = lib_metadata_runtime_filters,
        .lib_metadata_test_step = lib_metadata_test_step,
        .run_lib_metadata_sim_smoke_tests = run_lib_metadata_sim_smoke_tests,
        .run_lib_metadata_vopr_tests = run_lib_metadata_vopr_tests,
        .lib_metadata_vopr_chaos_tests = lib_metadata_vopr_chaos_tests,
        .lib_metadata_transition_chaos_filters = lib_metadata_transition_chaos_filters,
        .lib_metadata_public_chaos_filters = lib_metadata_public_chaos_filters,
        .lib_metadata_placement_chaos_filters = lib_metadata_placement_chaos_filters,
        .run_lib_metadata_sim_public_tests = run_lib_metadata_sim_public_tests,
    };
}
