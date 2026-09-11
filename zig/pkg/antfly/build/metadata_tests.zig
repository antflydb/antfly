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
    run_lib_metadata_vopr_data_tests: *std.Build.Step.Run,
    lib_metadata_runtime_filters: []const []const u8,
    lib_metadata_test_step: *std.Build.Step,
    run_lib_metadata_vopr_virtual_smoke_tests: *std.Build.Step.Run,
    run_lib_metadata_vopr_tests: *std.Build.Step.Run,
    lib_metadata_vopr_chaos_tests: *std.Build.Step.Compile,
    lib_metadata_vopr_transition_chaos_filters: []const []const u8,
    lib_metadata_vopr_public_chaos_filters: []const []const u8,
    lib_metadata_vopr_placement_chaos_filters: []const []const u8,
    run_lib_metadata_vopr_public_integration_tests: *std.Build.Step.Run,
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

    const lib_metadata_vopr_http_integration_default_filters = [_][]const u8{"metadata VOPR http cluster"};
    const lib_metadata_vopr_http_integration_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = selectTestFilters(b, &lib_metadata_vopr_http_integration_default_filters),
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lib_metadata_vopr_http_integration_tests = addFilteredTestRunArtifact(b, lib_metadata_vopr_http_integration_tests);
    const lib_metadata_vopr_http_integration_test_step = b.step("lib-metadata-vopr-http-integration-test", "Run metadata VOPR HTTP cluster fixtures, including native HTTP integration");
    lib_metadata_vopr_http_integration_test_step.dependOn(&run_lib_metadata_vopr_http_integration_tests.step);

    const lib_metadata_vopr_virtual_transport_default_filters = [_][]const u8{
        "metadata VOPR http cluster drives table placement convergence",
        "metadata VOPR http cluster converges placement after candidate churn",
        "metadata VOPR http cluster drives split intent through the control loop",
        "metadata VOPR http cluster drives merge intent through the control loop",
        "metadata VOPR http cluster drives automatic split through the control loop",
        "metadata VOPR http cluster drives automatic merge through the control loop",
        "metadata VOPR http cluster uses live median key for automatic split planning",
        "metadata VOPR http cluster uses remote live median key when metadata leader is not a shard replica",
        "metadata VOPR http cluster publishes split topology after finalize",
        "metadata VOPR http cluster publishes merge topology after finalize",
        "metadata VOPR http cluster provisions split destination replicas across nodes",
        "metadata VOPR http cluster retires merge donor replicas across nodes",
    };
    const lib_metadata_vopr_virtual_transport_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = selectTestFilters(b, &lib_metadata_vopr_virtual_transport_default_filters),
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lib_metadata_vopr_virtual_transport_tests = addFilteredTestRunArtifact(b, lib_metadata_vopr_virtual_transport_tests);
    const lib_metadata_vopr_virtual_transport_test_step = b.step("lib-metadata-vopr-virtual-transport-test", "Run metadata virtual-Raft-transport convergence tests; median-key fixtures also use native HTTP");
    lib_metadata_vopr_virtual_transport_test_step.dependOn(&run_lib_metadata_vopr_virtual_transport_tests.step);

    const lib_metadata_vopr_virtual_smoke_default_filters = [_][]const u8{
        "metadata VOPR candidate status marks explicitly supplied disk sizes known",
        "metadata VOPR split runtime preserves source identity namespace",
        "metadata VOPR source seeding preserves arbitrary keys and open range bounds",
        "metadata VOPR merge runtime records doc identity reassignment opt-in",
        "metadata VOPR http cluster drives table placement convergence",
        "metadata VOPR http cluster drives split intent through the control loop",
    };
    const lib_metadata_vopr_virtual_smoke_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = selectTestFilters(b, &lib_metadata_vopr_virtual_smoke_default_filters),
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lib_metadata_vopr_virtual_smoke_tests = addFilteredTestRunArtifact(b, lib_metadata_vopr_virtual_smoke_tests);
    const lib_metadata_vopr_virtual_smoke_test_step = b.step("lib-metadata-vopr-virtual-smoke-test", "Run fast metadata virtual-transport smoke tests");
    lib_metadata_vopr_virtual_smoke_test_step.dependOn(&run_lib_metadata_vopr_virtual_smoke_tests.step);

    const lib_metadata_vopr_default_filters = [_][]const u8{
        "metadata VOPR seeded smoke campaign",
        "metadata VOPR records crash interval and durable-state restart lifecycle",
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

    const lib_metadata_vopr_replay_stability_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"metadata VOPR trace exactly replays 100 consecutive times"},
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lib_metadata_vopr_replay_stability_tests = addFilteredTestRunArtifact(b, lib_metadata_vopr_replay_stability_tests);
    const lib_metadata_vopr_replay_stability_step = b.step(
        "metadata-vopr-replay-stability-test",
        "Exact-replay one metadata VOPR trace 100 consecutive times",
    );
    lib_metadata_vopr_replay_stability_step.dependOn(&run_lib_metadata_vopr_replay_stability_tests.step);

    // Production API/DataServer fixtures share the large runtime root. macOS
    // ReleaseSafe compiles measured 16.55 GB for DataServer and 16.15 GB for
    // metadata public-data tests; reserve the same 18 GiB as full-cluster
    // histories. Keep Linux's separately measured 7 GiB reservation.
    const production_vopr_compile_max_rss = @import("test_support.zig").productionVoprCompileMaxRss(target);
    const lib_metadata_vopr_data_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .max_rss = production_vopr_compile_max_rss,
        .filters = &.{"metadata VOPR distributed data survives split partition node restart and modeled storage crash"},
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lib_metadata_vopr_data_tests = addFilteredTestRunArtifact(b, lib_metadata_vopr_data_tests);
    const lib_metadata_vopr_data_test_step = b.step("lib-metadata-vopr-data-test", "Run the distributed public-data VOPR durability scenario");
    lib_metadata_vopr_data_test_step.dependOn(&run_lib_metadata_vopr_data_tests.step);

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

    const lib_metadata_vopr_transition_chaos_default_filters = [_][]const u8{
        "metadata VOPR http cluster completes automatic split after metadata leader restart",
        "metadata VOPR http cluster completes automatic split after metadata leader partition",
        "metadata VOPR http cluster completes automatic split under delayed raft transport",
        "metadata VOPR http cluster completes automatic split after leader restart under delayed raft transport",
        "metadata VOPR http cluster completes automatic split after source group leader restart",
        "metadata VOPR http cluster completes automatic split after destination group leader restart",
        "metadata VOPR http cluster completes automatic split after leader partition under delayed raft transport",
        "metadata VOPR http cluster completes automatic merge after metadata leader restart",
        "metadata VOPR http cluster completes automatic merge after donor group leader restart",
        "metadata VOPR http cluster completes automatic merge after receiver group leader restart",
        "metadata VOPR http cluster completes automatic merge after metadata leader partition",
        "metadata VOPR http cluster completes automatic merge under delayed raft transport",
        "metadata VOPR http cluster completes automatic merge after leader restart under delayed raft transport",
        "metadata VOPR http cluster completes automatic merge after leader partition under delayed raft transport",
        "metadata VOPR http cluster survives leader restart before forced automatic split reconcile",
    };
    const lib_metadata_vopr_public_chaos_default_filters = [_][]const u8{
        "metadata VOPR http cluster serves public traffic across automatic split under delayed raft transport",
        "metadata VOPR http cluster serves public traffic across automatic split after leader restart under delayed raft transport",
        "metadata VOPR http cluster serves public traffic across automatic split after source leader restart under delayed raft transport",
        "metadata VOPR http cluster serves public traffic across automatic split after leader partition under delayed raft transport",
        "metadata VOPR http cluster serves public traffic across automatic split after metadata leader partition",
        "metadata VOPR http cluster serves public traffic across automatic merge under delayed raft transport",
        "metadata VOPR http cluster serves public traffic across automatic merge after leader restart under delayed raft transport",
        "metadata VOPR http cluster serves public traffic across automatic merge after donor leader restart under delayed raft transport",
        "metadata VOPR http cluster serves public traffic across automatic merge after leader partition under delayed raft transport",
        "metadata VOPR http cluster serves public traffic across automatic merge after metadata leader partition",
    };
    const lib_metadata_vopr_placement_chaos_default_filters = [_][]const u8{
        "metadata VOPR http cluster survives metadata leader restart during placement reconcile",
        "metadata VOPR http cluster drops table topology across leader restart",
    };
    const lib_metadata_vopr_transition_chaos_filters = selectTestFilters(b, &lib_metadata_vopr_transition_chaos_default_filters);
    const lib_metadata_vopr_public_chaos_filters = selectTestFilters(b, &lib_metadata_vopr_public_chaos_default_filters);
    const lib_metadata_vopr_placement_chaos_filters = selectTestFilters(b, &lib_metadata_vopr_placement_chaos_default_filters);

    const lib_metadata_vopr_transition_chaos_test_step = b.step("lib-metadata-vopr-transition-chaos-test", "Run metadata VOPR split/merge transition restart and partition chaos tests");
    var metadata_transition_chaos_progress_tail: ?*std.Build.Step = null;
    metadata_transition_chaos_progress_tail = chainLabeledFilteredTests(b, antfly_test_mod, "lib-metadata-vopr-transition-chaos-test", lib_metadata_vopr_transition_chaos_filters, metadata_transition_chaos_progress_tail);
    lib_metadata_vopr_transition_chaos_test_step.dependOn(metadata_transition_chaos_progress_tail.?);

    const lib_metadata_vopr_public_chaos_test_step = b.step("lib-metadata-vopr-public-chaos-test", "Run metadata VOPR public traffic split/merge chaos tests");
    var metadata_public_chaos_progress_tail: ?*std.Build.Step = null;
    metadata_public_chaos_progress_tail = chainLabeledFilteredTests(b, antfly_test_mod, "lib-metadata-vopr-public-chaos-test", lib_metadata_vopr_public_chaos_filters, metadata_public_chaos_progress_tail);
    lib_metadata_vopr_public_chaos_test_step.dependOn(metadata_public_chaos_progress_tail.?);

    const lib_metadata_vopr_placement_chaos_test_step = b.step("lib-metadata-vopr-placement-chaos-test", "Run metadata VOPR placement restart chaos tests");
    var metadata_placement_chaos_progress_tail: ?*std.Build.Step = null;
    metadata_placement_chaos_progress_tail = chainLabeledFilteredTests(b, antfly_test_mod, "lib-metadata-vopr-placement-chaos-test", lib_metadata_vopr_placement_chaos_filters, metadata_placement_chaos_progress_tail);
    lib_metadata_vopr_placement_chaos_test_step.dependOn(metadata_placement_chaos_progress_tail.?);

    const lib_metadata_vopr_chaos_soak_test_step = b.step("lib-metadata-vopr-chaos-soak-test", "Run metadata VOPR delayed/restart/partition chaos tests");
    var metadata_chaos_progress_tail: ?*std.Build.Step = null;
    metadata_chaos_progress_tail = chainLabeledFilteredTests(b, antfly_test_mod, "lib-metadata-vopr-transition-chaos-test", lib_metadata_vopr_transition_chaos_filters, metadata_chaos_progress_tail);
    metadata_chaos_progress_tail = chainLabeledFilteredTests(b, antfly_test_mod, "lib-metadata-vopr-public-chaos-test", lib_metadata_vopr_public_chaos_filters, metadata_chaos_progress_tail);
    metadata_chaos_progress_tail = chainLabeledFilteredTests(b, antfly_test_mod, "lib-metadata-vopr-placement-chaos-test", lib_metadata_vopr_placement_chaos_filters, metadata_chaos_progress_tail);
    lib_metadata_vopr_chaos_soak_test_step.dependOn(metadata_chaos_progress_tail.?);

    const lib_metadata_vopr_public_integration_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{
            "public api linearizable read driver ignores a delayed earlier generation",
            "metadata VOPR http cluster serves public lifecycle from a non-host node after public create",
            "metadata VOPR http cluster seeds default admin for auth-enabled public api",
            "metadata VOPR http cluster forwards public split flow from a non-host node after public create",
            "metadata VOPR http cluster forwards public merge flow from a non-host node after public create",
        },
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
        // This broad macOS ReleaseFast simulation root has measured above
        // 12 GiB. Reserve its observed class without serializing the suite.
        .max_rss = @as(usize, if (target.result.os.tag == .macos) 14 else 7) * 1024 * 1024 * 1024,
    });
    const run_lib_metadata_vopr_public_integration_tests = addFilteredTestRunArtifact(b, lib_metadata_vopr_public_integration_tests);
    const lib_metadata_vopr_public_integration_test_step = b.step("lib-metadata-vopr-public-integration-test", "Run metadata public lifecycle/split/merge integration tests");
    lib_metadata_vopr_public_integration_test_step.dependOn(&run_lib_metadata_vopr_public_integration_tests.step);

    return .{
        .run_lib_metadata_vopr_data_tests = run_lib_metadata_vopr_data_tests,
        .lib_metadata_runtime_filters = lib_metadata_runtime_filters,
        .lib_metadata_test_step = lib_metadata_test_step,
        .run_lib_metadata_vopr_virtual_smoke_tests = run_lib_metadata_vopr_virtual_smoke_tests,
        .run_lib_metadata_vopr_tests = run_lib_metadata_vopr_tests,
        .lib_metadata_vopr_chaos_tests = lib_metadata_vopr_chaos_tests,
        .lib_metadata_vopr_transition_chaos_filters = lib_metadata_vopr_transition_chaos_filters,
        .lib_metadata_vopr_public_chaos_filters = lib_metadata_vopr_public_chaos_filters,
        .lib_metadata_vopr_placement_chaos_filters = lib_metadata_vopr_placement_chaos_filters,
        .run_lib_metadata_vopr_public_integration_tests = run_lib_metadata_vopr_public_integration_tests,
    };
}
