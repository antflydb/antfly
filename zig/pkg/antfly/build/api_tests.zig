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
const AntflyRootImports = @import("../../../build_support/antfly/imports.zig").AntflyRootImports;
const selectTestFilters = @import("../../../build_support/antfly/test_support.zig").selectTestFilters;
const compileFiltersWithAnchors = @import("../../../build_support/antfly/test_support.zig").compileFiltersWithAnchors;
const addFilteredTestRunArtifactWithRuntimeFilters = @import("../../../build_support/antfly/test_support.zig").addFilteredTestRunArtifactWithRuntimeFilters;
const addFilteredTestRunArtifact = @import("../../../build_support/antfly/test_support.zig").addFilteredTestRunArtifact;
const addCuratedTestRunArtifact = @import("../../../build_support/antfly/test_support.zig").addCuratedTestRunArtifact;

pub const AddTestsOptions = struct {
    api_http_runtime_test_mod: *std.Build.Module,
    vopr: *std.Build.Module,
    lmdb_engine: *std.Build.Module,
    optimize: std.lang.Optimize,
    openapi_root_check: *std.Build.Step.Run,
    antfly_imports: AntflyRootImports,
    antfly_test_mod: *std.Build.Module,
    run_lib_usermgr_tests: *std.Build.Step.Run,
};
pub const AddTestsResult = struct {
    process_generation_fence_step: *std.Build.Step,
    linked_consumer_tests: []const *std.Build.Step.Compile,
    hosted_recovery_tests: [3]*std.Build.Step.Compile,
    hosted_recovery_step: *std.Build.Step,
    run_lib_api_standalone_backup_restore_tests: *std.Build.Step.Run,
    distributed_query_availability_step: *std.Build.Step,
    run_public_api_parity_aggregate_tests: *std.Build.Step.Run,
    run_lib_api_auth_tests: *std.Build.Step.Run,
    run_algebraic_dynamic_template_tests: *std.Build.Step.Run,
    run_lib_api_connections_tests: *std.Build.Step.Run,
    run_lib_api_storage_authority_tests: *std.Build.Step.Run,
    api_table_writes_docid_test_mod: *std.Build.Module,
    write_implementation_tests: *std.Build.Step.Compile,
    public_api_parity_tests: *std.Build.Step.Compile,
    api_table_reads_docid_test_mod: *std.Build.Module,
    run_lib_api_docid_tests: *std.Build.Step.Run,
    api_derived_coverage_test_mod: *std.Build.Module,
    run_lib_api_derived_coverage_tests: *std.Build.Step.Run,
    run_lib_serverless_docid_tests: *std.Build.Step.Run,
    run_api_transactions_docid_tests: *std.Build.Step.Run,
    run_api_table_writes_docid_tests: *std.Build.Step.Run,
    run_api_table_reads_docid_tests: *std.Build.Step.Run,
    run_api_public_table_http_docid_tests: *std.Build.Step.Run,
    run_raft_transition_runtime_docid_tests: *std.Build.Step.Run,
    run_api_table_writes_production_regression_unit_tests: *std.Build.Step.Run,
    run_lib_docid_lifecycle_tests: *std.Build.Step.Run,
};

pub fn addTests(b: *std.Build, options: AddTestsOptions) AddTestsResult {
    const target = options.antfly_imports.platform_target;
    const optimize = options.optimize;
    const openapi_root_check = options.openapi_root_check;
    const antfly_imports = options.antfly_imports;
    const test_imports = @import("../../../build_support/antfly/test_support.zig").Imports{ .runtime = antfly_imports, .vopr = options.vopr, .lmdb_engine = options.lmdb_engine };
    const antfly_test_mod = options.antfly_test_mod;
    const run_lib_usermgr_tests = options.run_lib_usermgr_tests;
    const table_write_execution_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/table_write_execution_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configureConsumer(b, table_write_execution_root);
    table_write_execution_root.addImport("antfly_source_root", table_write_execution_root);
    const table_write_execution_tests = b.addTest(.{
        .name = "table-write-execution-contract-tests",
        .root_module = table_write_execution_root,
        .filters = &.{"compiled table write boundary"},
        .max_rss = 2 * 1024 * 1024 * 1024,
    });
    b.step("antfly-table-write-execution-contract-test", "Verify retained table-write teams across private callback dispatch")
        .dependOn(&b.addRunArtifact(table_write_execution_tests).step);
    const lake_store_opening_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/lake_store_opening_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configureConsumer(b, lake_store_opening_root);
    lake_store_opening_root.addImport("antfly_source_root", lake_store_opening_root);
    const lake_store_opening_tests = b.addTest(.{
        .name = "lake-store-opening-tests",
        .root_module = lake_store_opening_root,
        .filters = &.{"lake store opening"},
        .max_rss = 3 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    const lake_store_opening_run = b.addRunArtifact(lake_store_opening_tests);
    lake_store_opening_run.addArg("--require-no-skips");
    b.step("antfly-lake-store-opening-test", "Verify lake provider and partial allocation ownership on opening failure")
        .dependOn(&lake_store_opening_run.step);
    const lake_text_retirement_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/lake_text_retirement_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configureConsumer(b, lake_text_retirement_root);
    lake_text_retirement_root.addImport("antfly_source_root", lake_text_retirement_root);
    const lake_text_retirement_tests = b.addTest(.{
        .name = "lake-text-retirement-tests",
        .root_module = lake_text_retirement_root,
        .filters = &.{"external lake native text owns required and speculative children through physical retirement"},
        .max_rss = 3 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    const lake_text_retirement_run = b.addRunArtifact(lake_text_retirement_tests);
    lake_text_retirement_run.addArg("--require-no-skips");
    b.step("antfly-lake-text-retirement-test", "Verify real packed text IO and physical speculative joins with independent control progress")
        .dependOn(&lake_text_retirement_run.step);
    const ndjson_read_owner_tests = b.addTest(.{
        .name = "ndjson-read-owner-tests",
        .root_module = options.api_http_runtime_test_mod,
        .filters = &.{
            "system catalog NDJSON reuses one query definition without administrative snapshots",
            "global multi-query rechecks live permission before each table result",
            "workload deadline survives NDJSON lines and MCP dispatch",
            "api http server maps cancelled NDJSON multi-query to client closed response",
        },
        .max_rss = @as(usize, if (target.result.os.tag == .macos) 17 else 7) * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    const ndjson_read_owner_run = b.addRunArtifact(ndjson_read_owner_tests);
    ndjson_read_owner_run.addArg("--require-no-skips");
    b.step("antfly-ndjson-read-owner-test", "Verify one NDJSON execution owner preserves catalog, permission and original deadline semantics")
        .dependOn(&ndjson_read_owner_run.step);
    const retrieval_read_owner_tests = b.addTest(.{
        .name = "retrieval-read-owner-tests",
        .root_module = options.api_http_runtime_test_mod,
        .filters = &.{
            "retrieval catalog read owns or borrows one original query execution",
            "retrieval key scans retain original ownership through storage waits and denied reentry",
            "retrieval incoming probe retains original native or borrowed authority through callback retirement",
            "semantic query planning reuses equivalent embeddings across tables and isolates principals",
            "api http server serves retrieval agent response envelope",
        },
        // Native qualification measured 19.61 GB on macOS; keep this
        // compiler within the serialized 32 GiB build envelope.
        .max_rss = @as(usize, if (target.result.os.tag == .macos) 22 else 7) * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    const retrieval_read_owner_run = b.addRunArtifact(retrieval_read_owner_tests);
    retrieval_read_owner_run.addArg("--require-no-skips");
    b.step("antfly-retrieval-read-owner-test", "Verify agent read ownership and synchronous semantic inference grant propagation")
        .dependOn(&retrieval_read_owner_run.step);
    const physical_metadata_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/physical_resource_metadata_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configureConsumer(b, physical_metadata_root);
    physical_metadata_root.addImport("antfly_source_root", physical_metadata_root);
    const physical_metadata_tests = b.addTest(.{
        .name = "physical-resource-metadata-tests",
        .root_module = physical_metadata_root,
        .filters = &.{"physical cache metadata"},
        .max_rss = 3 * 1024 * 1024 * 1024,
    });
    b.step("antfly-physical-resource-metadata-test", "Verify fixed cache metadata admission and movable teardown ownership")
        .dependOn(&b.addRunArtifact(physical_metadata_tests).step);
    const writer_retirement_tests = b.addTest(.{
        .name = "bounded-writer-retirement-tests",
        .root_module = physical_metadata_root,
        .filters = &.{"physical cache metadata bounded writer retirement"},
        .max_rss = 4 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    const writer_retirement_run = b.addRunArtifact(writer_retirement_tests);
    writer_retirement_run.addArg("--require-no-skips");
    b.step("antfly-bounded-writer-retirement-test", "Verify admitted writer version pins, staged inputs and parent retirement")
        .dependOn(&writer_retirement_run.step);

    const legacy_retirement_tests = b.addTest(.{
        .name = "bounded-legacy-retirement-tests",
        .root_module = physical_metadata_root,
        .filters = &.{"physical cache metadata bounded legacy retirement"},
        .max_rss = 4 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    const legacy_retirement_run = b.addRunArtifact(legacy_retirement_tests);
    legacy_retirement_run.addArg("--require-no-skips");
    b.step("antfly-bounded-legacy-retirement-test", "Verify admitted legacy directory/path retirement and saved-cursor abort")
        .dependOn(&legacy_retirement_run.step);

    const legacy_migration_tests = b.addTest(.{
        .name = "registered-legacy-migration-tests",
        .root_module = physical_metadata_root,
        .filters = &.{"physical cache metadata legacy root migration"},
        .max_rss = 3 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    const legacy_migration_run = b.addRunArtifact(legacy_migration_tests);
    legacy_migration_run.addArg("--require-no-skips");
    b.step("antfly-registered-legacy-migration-test", "Verify nonempty legacy root installation, refusal and joined ownership")
        .dependOn(&legacy_migration_run.step);

    const checkpoint_join_tests = b.addTest(.{
        .name = "checkpoint-physical-join-tests",
        .root_module = physical_metadata_root,
        .filters = &.{"physical cache metadata checkpoint physical join"},
        .max_rss = 3 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    const checkpoint_join_run = b.addRunArtifact(checkpoint_join_tests);
    checkpoint_join_run.addArg("--require-no-skips");
    b.step("antfly-checkpoint-physical-join-test", "Verify issuer-bound checkpoint physical join and metadata independence")
        .dependOn(&checkpoint_join_run.step);

    const memory_flush_tests = b.addTest(.{
        .name = "registered-memory-flush-tests",
        .root_module = physical_metadata_root,
        .filters = &.{"physical cache metadata registered memory flush"},
        .max_rss = 3 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    const memory_flush_run = b.addRunArtifact(memory_flush_tests);
    memory_flush_run.addArg("--require-no-skips");
    b.step("antfly-registered-memory-flush-test", "Verify retained memory-run output, visibility, original refusal and durable reopen")
        .dependOn(&memory_flush_run.step);

    const file_snapshot_tests = b.addTest(.{
        .name = "registered-file-snapshot-tests",
        .root_module = physical_metadata_root,
        .filters = &.{"physical cache metadata registered file snapshot"},
        .max_rss = 3 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    const file_snapshot_run = b.addRunArtifact(file_snapshot_tests);
    file_snapshot_run.addArg("--require-no-skips");
    b.step("antfly-registered-file-snapshot-test", "Verify existing SST metadata ownership, original refusal and producer exit")
        .dependOn(&file_snapshot_run.step);

    const memory_snapshot_tests = b.addTest(.{
        .name = "registered-memory-snapshot-tests",
        .root_module = physical_metadata_root,
        .filters = &.{"physical cache metadata registered memory snapshot"},
        .max_rss = 3 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    const memory_snapshot_run = b.addRunArtifact(memory_snapshot_tests);
    memory_snapshot_run.addArg("--require-no-skips");
    b.step("antfly-registered-memory-snapshot-test", "Verify independently funded memory root snapshots and producer exit")
        .dependOn(&memory_snapshot_run.step);

    const flush_builder_tests = b.addTest(.{
        .name = "registered-flush-builder-tests",
        .root_module = physical_metadata_root,
        .filters = &.{"physical cache metadata flush builder"},
        .max_rss = 3 * 1024 * 1024 * 1024,
    });
    b.step("antfly-registered-flush-builder-test", "Verify retained multi-file flush construction, refusal and cleanup")
        .dependOn(&b.addRunArtifact(flush_builder_tests).step);

    const flush_publication_tests = b.addTest(.{
        .name = "registered-flush-publication-tests",
        .root_module = physical_metadata_root,
        .filters = &.{"physical cache metadata registered flush publication"},
        .max_rss = 3 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    const flush_publication_run = b.addRunArtifact(flush_publication_tests);
    flush_publication_run.addArg("--require-no-skips");
    b.step("antfly-registered-flush-publication-test", "Verify exclusive flush handoff, coupled publication coverage and cancellation-safe file ownership")
        .dependOn(&flush_publication_run.step);

    const coupled_ledger_tests = b.addTest(.{
        .name = "coupled-ledger-publication-tests",
        .root_module = physical_metadata_root,
        .filters = &.{"physical cache metadata coupled ledger removal"},
        .max_rss = 3 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    const coupled_ledger_run = b.addRunArtifact(coupled_ledger_tests);
    coupled_ledger_run.addArg("--require-no-skips");
    b.step("antfly-coupled-ledger-publication-test", "Verify independent obsolete intent removal preserves coupled file roots and physical readers")
        .dependOn(&coupled_ledger_run.step);

    const manifest_intent_tests = b.addTest(.{
        .name = "manifest-retirement-intent-tests",
        .root_module = physical_metadata_root,
        .filters = &.{"physical cache metadata manifest retirement"},
        .max_rss = 3 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    const manifest_intent_run = b.addRunArtifact(manifest_intent_tests);
    manifest_intent_run.addArg("--require-no-skips");
    b.step("antfly-manifest-retirement-intent-test", "Verify coupled manifest retirement intent retains lineage paths and original refusal ownership")
        .dependOn(&manifest_intent_run.step);

    const registered_checkpoint_tests = b.addTest(.{
        .name = "registered-manifest-checkpoint-tests",
        .root_module = physical_metadata_root,
        .filters = &.{"physical cache metadata registered checkpoint"},
        .max_rss = 3 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    const registered_checkpoint_run = b.addRunArtifact(registered_checkpoint_tests);
    registered_checkpoint_run.addArg("--require-no-skips");
    b.step("antfly-registered-manifest-checkpoint-test", "Verify registered durable checkpoint completion, recovery fencing and retained physical ownership")
        .dependOn(&registered_checkpoint_run.step);

    const durable_receipt_tests = b.addTest(.{
        .name = "durable-checkpoint-receipt-tests",
        .root_module = physical_metadata_root,
        .filters = &.{"physical cache metadata durable checkpoint receipt"},
        .max_rss = 3 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    const durable_receipt_run = b.addRunArtifact(durable_receipt_tests);
    durable_receipt_run.addArg("--require-no-skips");
    b.step("antfly-durable-checkpoint-receipt-test", "Verify durable registered metadata receipt, replacement and exact logical coverage")
        .dependOn(&durable_receipt_run.step);

    const immutable_retirement_tests = b.addTest(.{
        .name = "immutable-durable-retirement-tests",
        .root_module = physical_metadata_root,
        .filters = &.{"physical cache metadata immutable durable retirement"},
        .max_rss = 3 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    const immutable_retirement_run = b.addRunArtifact(immutable_retirement_tests);
    immutable_retirement_run.addArg("--require-no-skips");
    b.step("antfly-immutable-durable-retirement-test", "Verify durable immutable input retirement, cancellation and retained readers")
        .dependOn(&immutable_retirement_run.step);

    const immutable_pin_tests = b.addTest(.{
        .name = "immutable-pin-index-tests",
        .root_module = physical_metadata_root,
        .filters = &.{"physical cache metadata immutable pin indexes"},
        .max_rss = 3 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    const immutable_pin_run = b.addRunArtifact(immutable_pin_tests);
    immutable_pin_run.addArg("--require-no-skips");
    b.step("antfly-immutable-pin-index-test", "Verify indexed immutable reader ownership across flush, reordered release and allocation failure")
        .dependOn(&immutable_pin_run.step);

    const wal_retirement_tests = b.addTest(.{
        .name = "admitted-wal-retirement-tests",
        .root_module = physical_metadata_root,
        .filters = &.{"physical cache metadata admitted WAL retirement"},
        .max_rss = 3 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    const wal_retirement_run = b.addRunArtifact(wal_retirement_tests);
    wal_retirement_run.addArg("--require-no-skips");
    b.step("antfly-admitted-wal-retirement-test", "Verify admitted WAL rotation, covered deletion and retained replay/recovery authority")
        .dependOn(&wal_retirement_run.step);

    const socket_child_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/socket_child_cancellation_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configureConsumer(b, socket_child_root);
    socket_child_root.addImport("antfly_source_root", socket_child_root);
    const socket_child_tests = b.addTest(.{
        .name = "socket-child-cancellation-tests",
        .root_module = socket_child_root,
        .filters = &.{"socket child cancellation"},
        .max_rss = 2 * 1024 * 1024 * 1024,
    });
    b.step("antfly-socket-child-cancellation-test", "Verify socket child cancellation cannot rearm its parent")
        .dependOn(&b.addRunArtifact(socket_child_tests).step);

    const transaction_budget_port_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/transaction_routing_budget_port_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configureConsumer(b, transaction_budget_port_root);
    transaction_budget_port_root.addImport("antfly_source_root", transaction_budget_port_root);
    const transaction_budget_port_tests = b.addTest(.{
        .name = "transaction-routing-budget-port-tests",
        .root_module = transaction_budget_port_root,
        .filters = &.{ "transaction routing control port", "backend atomic admission" },
        .max_rss = 2 * 1024 * 1024 * 1024,
    });
    b.step("antfly-transaction-routing-budget-port-test", "Verify checked original transaction budget callbacks")
        .dependOn(&b.addRunArtifact(transaction_budget_port_tests).step);

    const cache_read_admission_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/cache_read_admission_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configureConsumer(b, cache_read_admission_root);
    cache_read_admission_root.addImport("antfly_source_root", cache_read_admission_root);
    const parent_reclaim_foreign_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/parent_reclaim_foreign_fixture.zig"),
        .target = target,
        .optimize = optimize,
    });
    parent_reclaim_foreign_root.addImport("antfly_runtime_abi", antfly_imports.runtime_abi);
    const parent_reclaim_foreign_library = b.addLibrary(.{
        .name = "parent-reclaim-foreign-fixture",
        .linkage = .static,
        .root_module = parent_reclaim_foreign_root,
    });
    cache_read_admission_root.linkLibrary(parent_reclaim_foreign_library);
    const cache_read_admission_tests = b.addTest(.{
        .name = "cache-read-admission-tests",
        .root_module = cache_read_admission_root,
        .filters = &.{
            "cache read admission",
            "shared LSM cache yields to foreground aggregate admission",
            "shared LSM resource reclaimer never waits for active accounting",
        },
        .max_rss = 2 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    const cache_read_admission_run = b.addRunArtifact(cache_read_admission_tests);
    cache_read_admission_run.addArg("--require-no-skips");
    b.step("antfly-cache-read-admission-test", "Require actual cache loader and lookup original admission ownership")
        .dependOn(&cache_read_admission_run.step);

    // This contract gate owns only the API execution capsule and inference
    // control/admission boundary; it needs no resident storage/model archive.
    const inference_owner_sources = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/source_catalog.zig"),
        .target = target,
        .optimize = optimize,
    });
    inference_owner_sources.addImport("antfly_local_test_sources", b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/api/runnable_inference_test_sources.zig"),
        .target = target,
        .optimize = optimize,
    }));
    inference_owner_sources.addImport("antfly_cancellation", antfly_imports.cancellation);
    inference_owner_sources.addImport("antfly_platform", antfly_imports.platform);
    inference_owner_sources.addImport("antfly_runtime_abi", antfly_imports.runtime_abi);
    const resource_forward_sources = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/source_catalog.zig"),
        .target = target,
        .optimize = optimize,
    });
    resource_forward_sources.addImport("antfly_local_test_sources", b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/api/runnable_resource_forwarding_test_sources.zig"),
        .target = target,
        .optimize = optimize,
    }));
    resource_forward_sources.addImport("antfly_cancellation", antfly_imports.cancellation);
    resource_forward_sources.addImport("antfly_platform", antfly_imports.platform);
    resource_forward_sources.addImport("antfly_runtime_abi", antfly_imports.runtime_abi);
    resource_forward_sources.addImport("antfly_cache_budget", antfly_imports.cache_budget);
    resource_forward_sources.addImport("runtime_memory_abi", antfly_imports.storage_boundary.memory);
    const resource_forward_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/api/runnable_resource_forwarding_test_root.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    resource_forward_root.addImport("antfly_runtime_abi", antfly_imports.runtime_abi);
    resource_forward_root.addImport("antfly_local_sources", resource_forward_sources);
    const resource_forward_tests = b.addTest(.{
        .name = "runnable-resource-forwarding-tests",
        .root_module = resource_forward_root,
        .filters = &.{"runnable resource forwarding"},
        .max_rss = 2 * 1024 * 1024 * 1024,
    });
    const resource_forward_run = b.addRunArtifact(resource_forward_tests);
    resource_forward_run.has_side_effects = true;
    b.step("antfly-runnable-resource-forwarding-test", "Verify resource callbacks and physical credit through opaque owner and native read bridges")
        .dependOn(&resource_forward_run.step);
    const fanout_owner_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/api/runnable_fanout_owner.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    fanout_owner_root.addImport("antfly_runtime_abi", antfly_imports.runtime_abi);
    fanout_owner_root.addImport("antfly_local_sources", inference_owner_sources);
    const fanout_owner_tests = b.addTest(.{
        .name = "runnable-fanout-owner-tests",
        .root_module = fanout_owner_root,
        .filters = &.{ "fanout owner retains joined result slots", "remote wait restores the original request phase", "owner transition sleep releases CPU", "algebraic partial ownership survives failure" },
        .max_rss = 3 * 1024 * 1024 * 1024,
    });
    b.step("antfly-runnable-fanout-owner-test", "Verify independent child result ownership and original phase through joining and rejected resumption")
        .dependOn(&b.addRunArtifact(fanout_owner_tests).step);
    const graph_fanout_owner_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/graph_fanout_owner_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configureConsumer(b, graph_fanout_owner_root);
    graph_fanout_owner_root.addImport("antfly_source_root", graph_fanout_owner_root);
    const graph_fanout_owner_tests = b.addTest(.{
        .name = "graph-fanout-owner-tests",
        .root_module = graph_fanout_owner_root,
        .filters = &.{ "distributed graph fans out per-group expand and hydrate with worker io", "storage local graph controls preserve borrowed authority" },
        .max_rss = 3 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    const graph_fanout_owner_run = b.addRunArtifact(graph_fanout_owner_tests);
    graph_fanout_owner_run.addArg("--require-no-skips");
    b.step("antfly-graph-fanout-owner-test", "Verify production graph expand and hydrate fanout under one or two foreground CPU slots")
        .dependOn(&graph_fanout_owner_run.step);
    const http_response_owner_tests = b.addTest(.{
        .name = "http-response-owner-tests",
        .root_module = options.api_http_runtime_test_mod,
        .filters = &.{"HTTP response owner"},
        .max_rss = 6 * 1024 * 1024 * 1024,
    });
    b.step("antfly-http-response-owner-test", "Verify buffered response owners retain original authority until physical context retirement")
        .dependOn(&b.addRunArtifact(http_response_owner_tests).step);
    const completion_task_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/api/runnable_completion_task_test_root.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    completion_task_root.addImport("antfly_runtime_abi", antfly_imports.runtime_abi);
    completion_task_root.addImport("antfly_local_sources", inference_owner_sources);
    completion_task_root.addImport("antfly_request_allocation_budget", b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/common/allocation_budget.zig"),
        .target = target,
        .optimize = optimize,
    }));
    const completion_budget_abi_tests = b.addTest(.{
        .name = "completion-budget-abi-tests",
        .root_module = b.createModule(.{
            .root_source_file = b.path("lib/runtime/src/runtime_runnable_abi.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
        }),
        .filters = &.{"completion ABI"},
        .max_rss = 2 * 1024 * 1024 * 1024,
    });
    b.step("antfly-completion-budget-abi-test", "Verify completion budget ABI version and authority checks")
        .dependOn(&b.addRunArtifact(completion_budget_abi_tests).step);
    const runnable_child_abi_tests = b.addTest(.{
        .name = "runnable-child-abi-tests",
        .root_module = b.createModule(.{
            .root_source_file = b.path("lib/runtime/src/runtime_runnable_abi.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
        }),
        .filters = &.{"runnable child ABI"},
        .max_rss = 2 * 1024 * 1024 * 1024,
    });
    b.step("antfly-runnable-child-abi-test", "Verify child reservation ABI prefixes and inherited authority")
        .dependOn(&b.addRunArtifact(runnable_child_abi_tests).step);
    const runnable_lifetime_abi_tests = b.addTest(.{
        .name = "runnable-lifetime-abi-tests",
        .root_module = b.createModule(.{
            .root_source_file = b.path("lib/runtime/src/runtime_runnable_abi.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
        }),
        .filters = &.{"runnable lifetime ABI"},
        .max_rss = 2 * 1024 * 1024 * 1024,
    });
    b.step("antfly-runnable-lifetime-abi-test", "Verify parked borrowed lifetime without acquiring CPU")
        .dependOn(&b.addRunArtifact(runnable_lifetime_abi_tests).step);
    const provider_lifetime_root = b.createModule(.{
        .root_source_file = b.path("pkg/inference/src/host/execution_control.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    provider_lifetime_root.addImport("antfly_runtime_abi", antfly_imports.runtime_abi);
    provider_lifetime_root.addImport("antfly_platform", antfly_imports.platform);
    provider_lifetime_root.addImport("antfly_cancellation", antfly_imports.cancellation);
    const provider_lifetime_tests = b.addTest(.{
        .name = "provider-lifetime-tests",
        .root_module = provider_lifetime_root,
        .filters = &.{"provider request lifetime polling"},
        .max_rss = 2 * 1024 * 1024 * 1024,
    });
    b.step("antfly-provider-lifetime-test", "Verify provider polling observes owner cancellation expiry and shutdown without CPU resumption")
        .dependOn(&b.addRunArtifact(provider_lifetime_tests).step);
    const decision_budget_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/api/decision_recovery_budget.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    decision_budget_root.addImport("antfly_runtime_abi", antfly_imports.runtime_abi);
    const decision_budget_tests = b.addTest(.{
        .name = "decision-recovery-budget-tests",
        .root_module = decision_budget_root,
        .filters = &.{"decision recovery budget"},
        .max_rss = 2 * 1024 * 1024 * 1024,
    });
    b.step("antfly-decision-recovery-budget-test", "Verify accepted completion deadlines cannot be renewed by recovery")
        .dependOn(&b.addRunArtifact(decision_budget_tests).step);
    const completion_task_tests = b.addTest(.{
        .name = "completion-task-tests",
        .root_module = completion_task_root,
        .filters = &.{ "completion task", "runnable execution completion ABI", "completion ABI" },
        .max_rss = 2 * 1024 * 1024 * 1024,
    });
    b.step("antfly-completion-task-test", "Verify preauthorized joined completion tasks")
        .dependOn(&b.addRunArtifact(completion_task_tests).step);
    const runnable_bundle_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/runnable_resource_bundle_test_root.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    test_imports.configureConsumer(b, runnable_bundle_root);
    const runnable_bundle_tests = b.addTest(.{
        .name = "runnable-resource-bundle-tests",
        .root_module = runnable_bundle_root,
        .filters = &.{ "runnable resource bundle", "dense search bandwidth" },
        .max_rss = 2 * 1024 * 1024 * 1024,
    });
    const runnable_bundle_run = b.addRunArtifact(runnable_bundle_tests);
    runnable_bundle_run.has_side_effects = true;
    b.step("antfly-runnable-resource-bundle-test", "Verify joint CPU memory and dense bandwidth grants and continuation progress")
        .dependOn(&runnable_bundle_run.step);
    const prepared_memory_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/prepared_resource_bundle_test_root.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    test_imports.configureConsumer(b, prepared_memory_root);
    const prepared_memory_tests = b.addTest(.{
        .name = "prepared-memory-batch-tests",
        .root_module = prepared_memory_root,
        .filters = &.{ "prepared memory batch", "batch reservation", "batch release accounting", "resource identity ledgers", "identity allocation failure" },
        .max_rss = 2 * 1024 * 1024 * 1024,
    });
    const prepared_memory_run = b.addRunArtifact(prepared_memory_tests);
    // Exercise physical admission/retirement on every requested qualification
    // run, including when the test executable itself is cached.
    prepared_memory_run.has_side_effects = true;
    b.step("antfly-prepared-memory-batch-test", "Verify allocation-free atomic memory admission with prepared ownership")
        .dependOn(&prepared_memory_run.step);
    const control_metadata_tests = b.addTest(.{
        .name = "shared-control-metadata-tests",
        .root_module = prepared_memory_root,
        .filters = &.{ "shared control metadata", "prepared memory batch declaration reachability" },
        .max_rss = 2 * 1024 * 1024 * 1024,
    });
    const control_metadata_run = b.addRunArtifact(control_metadata_tests);
    control_metadata_run.has_side_effects = true;
    b.step("antfly-control-metadata-test", "Verify common identity metadata stays inside physical memory admission")
        .dependOn(&control_metadata_run.step);
    const resource_accounting_tests = b.addTest(.{
        .name = "shared-resource-accounting-tests",
        .root_module = prepared_memory_root,
        .filters = &.{ "storage.resource_manager.test.", "prepared memory batch declaration reachability" },
        .max_rss = 2 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    const resource_accounting_run = b.addRunArtifact(resource_accounting_tests);
    resource_accounting_run.has_side_effects = true;
    resource_accounting_run.addArg("--timeout-ms=10000");
    b.step("antfly-resource-accounting-test", "Verify common physical resource accounting and admission")
        .dependOn(&resource_accounting_run.step);
    const cleanup_budget_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/common/runnable_scheduler.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    cleanup_budget_root.addImport("antfly_platform", antfly_imports.platform);
    const cleanup_budget_tests = b.addTest(.{
        .name = "cleanup-budget-tests",
        .root_module = cleanup_budget_root,
        .max_rss = 2 * 1024 * 1024 * 1024,
    });
    b.step("antfly-cleanup-budget-test", "Verify cleanup activation budgets and runnable scheduler contracts")
        .dependOn(&b.addRunArtifact(cleanup_budget_tests).step);

    const status_refresh_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/api/runnable_status_refresh.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    status_refresh_root.addImport("antfly_runtime_abi", antfly_imports.runtime_abi);
    status_refresh_root.addImport("antfly_platform", antfly_imports.platform);
    status_refresh_root.addImport("antfly_local_sources", inference_owner_sources);
    const status_refresh_tests = b.addTest(.{
        .name = "status-refresh-execution-tests",
        .root_module = status_refresh_root,
        .filters = &.{"runnable status refresh"},
        .max_rss = 2 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    const status_refresh_run = b.addRunArtifact(status_refresh_tests);
    status_refresh_run.has_side_effects = true;
    status_refresh_run.addArg("--require-no-skips");
    b.step("antfly-status-refresh-execution-test", "Verify independently admitted refresh passes preserve queued deadlines and physical ownership")
        .dependOn(&status_refresh_run.step);
    const maintenance_runnable_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/api/runnable_maintenance_test_root.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    maintenance_runnable_root.addImport("antfly_runtime_abi", antfly_imports.runtime_abi);
    maintenance_runnable_root.addImport("antfly_local_sources", inference_owner_sources);
    const maintenance_runnable_tests = b.addTest(.{
        .name = "maintenance-runnable-tests",
        .root_module = maintenance_runnable_root,
        .filters = &.{"background execution recurring"},
        .max_rss = 2 * 1024 * 1024 * 1024,
    });
    b.step("antfly-background-execution-test", "Verify recurring background passes share node CPU and retain original authority through waits and joins")
        .dependOn(&b.addRunArtifact(maintenance_runnable_tests).step);

    const cleanup_task_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/api/runnable_cleanup_task_test_root.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    cleanup_task_root.addImport("antfly_runtime_abi", antfly_imports.runtime_abi);
    cleanup_task_root.addImport("antfly_local_sources", inference_owner_sources);
    cleanup_task_root.addImport("antfly_request_allocation_budget", b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/common/allocation_budget.zig"),
        .target = target,
        .optimize = optimize,
    }));
    const cleanup_task_tests = b.addTest(.{
        .name = "cleanup-task-tests",
        .root_module = cleanup_task_root,
        .filters = &.{"cleanup task"},
        .max_rss = 2 * 1024 * 1024 * 1024,
    });
    b.step("antfly-cleanup-task-test", "Verify preauthorized joined cleanup tasks")
        .dependOn(&b.addRunArtifact(cleanup_task_tests).step);
    const sql_write_team_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/api/runnable_write_team_test_root.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    sql_write_team_root.addImport("antfly_runtime_abi", antfly_imports.runtime_abi);
    sql_write_team_root.addImport("antfly_local_sources", inference_owner_sources);
    sql_write_team_root.addImport("antfly_request_allocation_budget", b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/common/allocation_budget.zig"),
        .target = target,
        .optimize = optimize,
    }));
    const sql_write_team_tests = b.addTest(.{
        .name = "sql-write-team-tests",
        .root_module = sql_write_team_root,
        .filters = &.{ "SQL write team", "SQL protected commit team" },
        .max_rss = 2 * 1024 * 1024 * 1024,
    });
    b.step("antfly-sql-write-team-test", "Run retained SQL write and protected commit CPU team contracts")
        .dependOn(&b.addRunArtifact(sql_write_team_tests).step);
    const inference_owner_admission = b.createModule(.{
        .root_source_file = b.path("pkg/inference/src/server/runnable_execution.zig"),
        .target = target,
        .optimize = optimize,
    });
    inference_owner_admission.addImport("antfly_runtime_abi", antfly_imports.runtime_abi);
    inference_owner_admission.addImport("inference_linalg", antfly_imports.inference_server.import_table.get("inference_linalg").?);
    const inference_owner_control = b.createModule(.{
        .root_source_file = b.path("pkg/inference/src/execution_control.zig"),
        .target = target,
        .optimize = optimize,
    });
    inference_owner_control.addImport("antfly_platform", antfly_imports.platform);
    inference_owner_control.addImport("antfly_runtime_abi", antfly_imports.runtime_abi);
    inference_owner_admission.addImport("inference_execution_control", inference_owner_control);
    const local_child_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/api/runnable_local_child.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    local_child_root.addImport("antfly_runtime_abi", antfly_imports.runtime_abi);
    local_child_root.addImport("antfly_local_sources", inference_owner_sources);
    const local_child_tests = b.addTest(.{
        .name = "local-child-owner-tests",
        .root_module = local_child_root,
        .filters = &.{"local child receiver"},
        .max_rss = 2 * 1024 * 1024 * 1024,
    });
    b.step("antfly-local-child-owner-test", "Verify local SQL child receiver lifetime and independent worker execution")
        .dependOn(&b.addRunArtifact(local_child_tests).step);
    const cpu_borrow_abi_tests = b.addTest(.{
        .name = "cpu-borrow-abi-tests",
        .root_module = b.createModule(.{
            .root_source_file = b.path("lib/runtime/src/runtime_runnable_abi.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
        }),
        .max_rss = 2 * 1024 * 1024 * 1024,
    });
    b.step("antfly-runnable-cpu-borrow-abi-test", "Verify runnable CPU-loan ABI prefixes and optional capabilities")
        .dependOn(&b.addRunArtifact(cpu_borrow_abi_tests).step);
    const reserved_policy_tests = b.addTest(.{
        .name = "read-policy-memory-tests",
        .root_module = b.createModule(.{
            .root_source_file = b.path("pkg/antfly/src/api/read_policy_memory.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
        }),
        .max_rss = 1024 * 1024 * 1024,
    });
    b.step("antfly-read-policy-memory-test", "Verify retained policy scratch bounds and refusal without allocator fallback")
        .dependOn(&b.addRunArtifact(reserved_policy_tests).step);

    const policy_memory_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/usermgr_policy_memory_test_root.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    policy_memory_root.addImport("antfly_runtime_abi", antfly_imports.runtime_abi);
    policy_memory_root.addImport("antfly_casbin", antfly_imports.casbin);
    const policy_memory_tests = b.addTest(.{
        .name = "usermgr-policy-memory-tests",
        .root_module = policy_memory_root,
        .filters = &.{"user_manager.test."},
        .max_rss = 2 * 1024 * 1024 * 1024,
    });
    b.step("antfly-usermgr-policy-memory-test", "Verify UserManager policy ownership and caller-bounded snapshot allocations")
        .dependOn(&b.addRunArtifact(policy_memory_tests).step);

    const runnable_owner_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/api/runnable_owner_execution.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    runnable_owner_root.addImport("antfly_runtime_abi", antfly_imports.runtime_abi);
    runnable_owner_root.addImport("antfly_local_sources", inference_owner_sources);
    const runnable_owner_tests = b.addTest(.{
        .name = "runnable-owner-port-tests",
        .root_module = runnable_owner_root,
        .filters = &.{"runnable execution"},
        .max_rss = 2 * 1024 * 1024 * 1024,
    });
    b.step("antfly-runnable-owner-port-test", "Verify checked API-owned execution capsules and CPU-loan transitions")
        .dependOn(&b.addRunArtifact(runnable_owner_tests).step);
    const native_borrow_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/api/runnable_native_borrow.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    native_borrow_root.addImport("antfly_runtime_abi", antfly_imports.runtime_abi);
    native_borrow_root.addImport("antfly_local_sources", inference_owner_sources);
    const native_borrow_tests = b.addTest(.{
        .name = "native-execution-borrow-tests",
        .root_module = native_borrow_root,
        .filters = &.{"native execution borrow"},
        .max_rss = 2 * 1024 * 1024 * 1024,
    });
    b.step("antfly-runnable-native-borrow-test", "Verify native read adapters retain opaque root and child execution ownership through physical joins")
        .dependOn(&b.addRunArtifact(native_borrow_tests).step);
    const inference_owner_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/api/runnable_inference_test_root.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    inference_owner_root.addImport("antfly_runtime_abi", antfly_imports.runtime_abi);
    inference_owner_root.addImport("antfly_local_sources", inference_owner_sources);
    inference_owner_root.addImport("inference_runnable_execution", inference_owner_admission);
    inference_owner_root.addImport("inference_execution_control", inference_owner_control);
    const inference_owner_tests = b.addTest(.{
        .name = "inference-owner-handoff-tests",
        .root_module = inference_owner_root,
        .max_rss = 2 * 1024 * 1024 * 1024,
    });
    b.step("antfly-inference-owner-handoff-test", "Run real shared-owner inference handoff and model-lock control progress contracts")
        .dependOn(&b.addRunArtifact(inference_owner_tests).step);
    const http_cleanup_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/api/runnable_http_cleanup_test_root.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    http_cleanup_root.addImport("antfly_runtime_abi", antfly_imports.runtime_abi);
    http_cleanup_root.addImport("httpx", antfly_imports.httpx);
    http_cleanup_root.addImport("antfly_local_sources", inference_owner_sources);
    const http_cleanup_tests = b.addTest(.{
        .name = "http-cleanup-owner-tests",
        .root_module = http_cleanup_root,
        .filters = &.{"runnable HTTP cleanup"},
        .max_rss = 2 * 1024 * 1024 * 1024,
    });
    b.step("antfly-http-cleanup-owner-test", "Run actual listener output through shared protected cleanup ownership")
        .dependOn(&b.addRunArtifact(http_cleanup_tests).step);
    const generation_fixture_root = b.createModule(.{
        .root_source_file = b.path("lib/platform/tests/process_generation_fence_fixture.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    generation_fixture_root.addImport("platform", antfly_imports.platform);
    const generation_fixture = b.addExecutable(.{ .name = "process-generation-fence-fixture", .root_module = generation_fixture_root });
    const generation_run = b.addSystemCommand(&.{"python3"});
    generation_run.addFileArg(b.path("lib/platform/tests/test_process_generation_fence.py"));
    generation_run.addArtifactArg(generation_fixture);
    generation_run.setCwd(b.path("."));
    generation_run.addArg(".zig-cache/process-generation-fence-tests");
    generation_run.has_side_effects = true;
    const generation_step = b.step("antfly-process-generation-fence-test", "Verify inherited process leases across parent death and partial startup");
    const lifetime_fixture_root = b.createModule(.{
        .root_source_file = b.path("pkg/inference/src/host/worker_lifetime_fixture.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    lifetime_fixture_root.addImport("antfly_platform", antfly_imports.platform);
    lifetime_fixture_root.addImport("antfly_inference_bridge", antfly_imports.inference_host.import_table.get("antfly_inference_bridge").?);
    lifetime_fixture_root.addImport("antfly_inference_worker_wire", antfly_imports.inference_host.import_table.get("antfly_inference_worker_wire").?);
    lifetime_fixture_root.addImport("inference_server", antfly_imports.inference_host.import_table.get("inference_server").?);
    lifetime_fixture_root.addImport("antfly_inference_worker_rpc", antfly_imports.inference_host.import_table.get("antfly_inference_worker_rpc").?);
    const lifetime_fixture = b.addExecutable(.{ .name = "inference-worker-lifetime-fixture", .root_module = lifetime_fixture_root });
    const lifetime_run = b.addSystemCommand(&.{"python3"});
    lifetime_run.addFileArg(b.path("lib/platform/tests/test_worker_lifetime.py"));
    lifetime_run.addArtifactArg(generation_fixture);
    lifetime_run.addArtifactArg(lifetime_fixture);
    lifetime_run.setCwd(b.path("."));
    lifetime_run.addArg(".zig-cache/process-generation-fence-tests");
    lifetime_run.has_side_effects = true;
    generation_step.dependOn(&lifetime_run.step);

    generation_step.dependOn(&generation_run.step);
    const generation_worker_root = b.createModule(.{
        .root_source_file = b.path("pkg/inference/src/host/worker.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    for (antfly_imports.inference_host.import_table.keys(), antfly_imports.inference_host.import_table.values()) |name, module| {
        generation_worker_root.addImport(name, module);
    }
    const provider_check_root = b.createModule(.{
        .root_source_file = b.path("pkg/inference/src/host/worker_provider_check_root.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    for (antfly_imports.inference_host.import_table.keys(), antfly_imports.inference_host.import_table.values()) |name, module| {
        provider_check_root.addImport(name, module);
    }
    const provider_check = b.addObject(.{
        .name = "inference-worker-provider-entry-check",
        .root_module = provider_check_root,
        .max_rss = 8 * 1024 * 1024 * 1024,
    });
    generation_step.dependOn(&provider_check.step);
    const generation_worker_tests = b.addTest(.{
        .name = "inference-worker-generation-tests",
        .root_module = generation_worker_root,
        .filters = &.{ "inference worker", "runnable execution worker budget" },
        .max_rss = 12 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    const generation_worker_run = b.addRunArtifact(generation_worker_tests);
    generation_worker_run.addArg("--require-no-skips");
    generation_worker_run.addArgs(&.{ "--test-filter", "inference worker", "--test-filter", "runnable execution worker budget" });
    generation_worker_run.has_side_effects = true;
    generation_step.dependOn(&generation_worker_run.step);
    const generation_rpc_tests = b.addTest(.{
        .name = "inference-worker-payload-budget-tests",
        .root_module = antfly_imports.inference_host.import_table.get("antfly_inference_worker_rpc").?,
        .filters = &.{"inference worker"},
        .max_rss = 2 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    const generation_rpc_run = b.addRunArtifact(generation_rpc_tests);
    generation_rpc_run.addArgs(&.{ "--require-no-skips", "--test-filter", "inference worker" });
    generation_rpc_run.has_side_effects = true;
    generation_step.dependOn(&generation_rpc_run.step);
    const prompt_cache_root = b.createModule(.{
        .root_source_file = b.path("pkg/inference/src/runtime/kv/prompt_cache.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    const prompt_cache_tests = b.addTest(.{
        .name = "inference-prompt-cache-ownership-tests",
        .root_module = prompt_cache_root,
        .max_rss = 2 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    const prompt_cache_run = b.addRunArtifact(prompt_cache_tests);
    prompt_cache_run.addArg("--require-no-skips");
    prompt_cache_run.has_side_effects = true;
    b.step("antfly-prompt-cache-ownership-test", "Run prompt cache, pool and sequence ownership contracts").dependOn(&prompt_cache_run.step);
    const cuda_credit_root = b.createModule(.{
        .root_source_file = b.path("pkg/inference/src/cuda_device_credit_test_root.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    cuda_credit_root.addImport("antfly_platform", antfly_imports.platform);
    const cuda_credit_tests = b.addTest(.{
        .name = "inference-cuda-physical-credit-tests",
        .root_module = cuda_credit_root,
        .filters = &.{ "CUDA physical", "CUDA failed physical", "CUDA graph suballocation" },
        .max_rss = 2 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    const cuda_credit_run = b.addRunArtifact(cuda_credit_tests);
    cuda_credit_run.addArg("--require-no-skips");
    cuda_credit_run.setEnvironmentVariable("ANTFLY_TEST_FAIL_ON_ERROR_LOGS", "1");
    cuda_credit_run.has_side_effects = true;
    b.step("antfly-cuda-physical-credit-test", "Run physical CUDA allocation credit contracts with a mock driver").dependOn(&cuda_credit_run.step);
    const node_memory_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/worker_node_memory_test_root.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    test_imports.configureConsumer(b, node_memory_root);
    // Exercise embedded creation through the production native function table,
    // as well as the local-codegen variant below.
    const node_memory_exports = b.createModule(.{
        .root_source_file = b.path("pkg/inference/src/host/native_exports.zig"),
        .target = target,
        .optimize = optimize,
    });
    antfly_imports.configureInference(b, node_memory_exports, true);
    node_memory_root.addImport("antfly_inference_native_exports", node_memory_exports);
    node_memory_root.addImport("inference_worker_rpc", antfly_imports.inference_host.import_table.get("antfly_inference_worker_rpc").?);
    node_memory_root.addImport("antfly_inference_host", antfly_imports.inference_host);
    node_memory_root.addImport("inference_worker_wire", antfly_imports.inference_host.import_table.get("antfly_inference_worker_wire").?);
    node_memory_root.addImport("inference_worker_bridge", antfly_imports.inference_host.import_table.get("antfly_inference_bridge").?);
    const node_memory_tests = b.addTest(.{
        .name = "inference-worker-node-memory-tests",
        .root_module = node_memory_root,
        .filters = &.{"worker node memory"},
        .max_rss = 12 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    const node_memory_run = b.addRunArtifact(node_memory_tests);
    node_memory_run.addArgs(&.{ "--require-no-skips", "--timeout-ms=5000", "--test-filter", "worker node memory" });
    node_memory_run.has_side_effects = true;
    generation_step.dependOn(&node_memory_run.step);
    const worker_progress_root = b.createModule(.{
        .root_source_file = b.path("pkg/inference/src/host/worker_progress.zig"),
        .target = target,
        .optimize = optimize,
    });
    const worker_progress_tests = b.addTest(.{
        .name = "inference-worker-progress-tests",
        .root_module = worker_progress_root,
        .filters = &.{"inference worker progress"},
        .max_rss = 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    const worker_progress_run = b.addRunArtifact(worker_progress_tests);
    worker_progress_run.addArg("--require-no-skips");
    b.step("antfly-inference-worker-progress-test", "Verify fixed worker telemetry scratch and non-waiting per-request gates")
        .dependOn(&worker_progress_run.step);
    const worker_owner_rpc = b.createModule(.{
        .root_source_file = b.path("pkg/inference/src/host/worker_rpc.zig"),
        .target = target,
        .optimize = optimize,
    });
    worker_owner_rpc.addImport("httpx", antfly_imports.httpx);
    const worker_rpc_control_tests = b.addTest(.{
        .name = "inference-worker-rpc-control-tests",
        .root_module = worker_owner_rpc,
        .filters = &.{"inference worker"},
        .max_rss = 2 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    const worker_rpc_control_run = b.addRunArtifact(worker_rpc_control_tests);
    worker_rpc_control_run.addArg("--require-no-skips");
    b.step("antfly-inference-worker-rpc-control-test", "Verify acknowledged worker event lifetime controls over real pipes")
        .dependOn(&worker_rpc_control_run.step);
    const worker_http_control_root = b.createModule(.{
        .root_source_file = b.path("pkg/inference/src/host/worker_http_control.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    worker_http_control_root.addImport("antfly_runtime_abi", antfly_imports.runtime_abi);
    worker_http_control_root.addImport("antfly_inference_worker_rpc", worker_owner_rpc);
    worker_http_control_root.addImport("antfly_platform", antfly_imports.platform);
    worker_http_control_root.addImport("httpx", antfly_imports.httpx);
    const worker_http_control_tests = b.addTest(.{
        .name = "inference-worker-http-control-tests",
        .root_module = worker_http_control_root,
        .filters = &.{"inference worker HTTP control"},
        .max_rss = 2 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    const worker_http_control_run = b.addRunArtifact(worker_http_control_tests);
    worker_http_control_run.addArg("--require-no-skips");
    b.step("antfly-inference-worker-http-control-test", "Verify native deadline projection and controlled worker output retirement")
        .dependOn(&worker_http_control_run.step);
    const worker_owner_phases = b.createModule(.{
        .root_source_file = b.path("pkg/inference/src/host/worker_execution_phase.zig"),
        .target = target,
        .optimize = optimize,
    });
    const worker_owner_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/api/runnable_worker_test_root.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    worker_owner_root.addImport("antfly_runtime_abi", antfly_imports.runtime_abi);
    worker_owner_root.addImport("httpx", antfly_imports.httpx);
    worker_owner_root.addImport("antfly_local_sources", inference_owner_sources);
    worker_owner_root.addImport("inference_execution_control", inference_owner_control);
    worker_owner_root.addImport("inference_worker_rpc", worker_owner_rpc);
    worker_owner_root.addImport("inference_worker_phases", worker_owner_phases);
    const worker_owner_tests = b.addTest(.{
        .name = "inference-worker-owner-handoff-tests",
        .root_module = worker_owner_root,
        .filters = &.{"inference worker physical handoff"},
        .max_rss = 2 * 1024 * 1024 * 1024,
    });
    b.step("antfly-inference-worker-owner-handoff-test", "Run real owner capsules through the worker pipe phase transport")
        .dependOn(&b.addRunArtifact(worker_owner_tests).step);
    const worker_process_fixture_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/api/runnable_worker_process_fixture.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    worker_process_fixture_root.addImport("inference_worker_rpc", worker_owner_rpc);
    const worker_process_fixture = b.addExecutable(.{ .name = "inference-worker-output-fixture", .root_module = worker_process_fixture_root });
    const worker_process_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/api/runnable_worker_process_test_root.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    worker_process_root.addImport("antfly_runtime_abi", antfly_imports.runtime_abi);
    worker_process_root.addImport("httpx", antfly_imports.httpx);
    worker_process_root.addImport("antfly_local_sources", inference_owner_sources);
    worker_process_root.addImport("inference_worker_rpc", worker_owner_rpc);
    const worker_process_tests = b.addTest(.{
        .name = "inference-worker-process-output-tests",
        .root_module = worker_process_root,
        .filters = &.{"inference worker subprocess output"},
        .max_rss = 2 * 1024 * 1024 * 1024,
    });
    const run_worker_process_tests = b.addRunArtifact(worker_process_tests);
    run_worker_process_tests.setCwd(worker_process_fixture.getEmittedBin().dirname());
    run_worker_process_tests.setEnvironmentVariable("ANTFLY_TEST_WORKER_OUTPUT_FIXTURE", b.fmt("./{s}", .{worker_process_fixture.out_filename}));
    b.step("antfly-inference-worker-process-output-test", "Run actual worker subprocess output through API ownership, TCP waits and reaping")
        .dependOn(&run_worker_process_tests.step);
    const worker_output_error_root = b.createModule(.{
        .root_source_file = b.path("pkg/inference/src/host/worker_output_error.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    worker_output_error_root.addImport("antfly_runtime_abi", antfly_imports.runtime_abi);
    const worker_output_error_tests = b.addTest(.{
        .name = "inference-worker-output-error-tests",
        .root_module = worker_output_error_root,
        .max_rss = 1024 * 1024 * 1024,
    });
    b.step("antfly-inference-worker-output-error-test", "Run caller-owned scheduler failures through output callback statuses")
        .dependOn(&b.addRunArtifact(worker_output_error_tests).step);
    const inference_error_names_tests = b.addTest(.{
        .name = "inference-error-names-tests",
        .root_module = b.createModule(.{
            .root_source_file = b.path("pkg/inference/src/host/engine_errors.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
        }),
    });
    b.step("antfly-inference-error-names-test", "Verify every private inference error name round-trips without numeric wire identities")
        .dependOn(&b.addRunArtifact(inference_error_names_tests).step);
    const inference_http_deadline_tests = b.addTest(.{
        .name = "inference-http-deadline-tests",
        .root_module = antfly_imports.inference_server,
        .filters = &.{"runnable execution HTTP native deadline"},
        .max_rss = 4 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    const inference_http_deadline_run = b.addRunArtifact(inference_http_deadline_tests);
    inference_http_deadline_run.addArg("--require-no-skips");
    b.step("antfly-inference-http-deadline-test", "Verify inference HTTP native clock conversion and borrowed lifetime intersection")
        .dependOn(&inference_http_deadline_run.step);
    const inference_phase_filters = [_][]const u8{ "runnable execution local provider", "runnable execution worker budget", "native checkpoint worker upload", "inference worker checkpoint upload", "inference worker document capabilities" };
    const inference_phase_tests = b.addTest(.{
        .name = "inference-parent-phase-tests",
        .root_module = antfly_imports.inference_host,
        .filters = &inference_phase_filters,
        .max_rss = 4 * 1024 * 1024 * 1024,
    });
    const inference_phase_step = b.step("antfly-inference-parent-phase-test", "Run provider grant transfer, worker demand and owned-result cleanup contracts");
    inference_phase_step.dependOn(&addFilteredTestRunArtifactWithRuntimeFilters(b, inference_phase_tests, &inference_phase_filters).step);
    const inference_wire_filters = [_][]const u8{ "inference worker", "native checkpoint worker streaming" };
    const inference_wire_tests = b.addTest(.{
        .name = "inference-parent-wire-tests",
        .root_module = antfly_imports.inference_worker_wire,
        .filters = &inference_wire_filters,
        .max_rss = 2 * 1024 * 1024 * 1024,
    });
    inference_phase_step.dependOn(&addFilteredTestRunArtifactWithRuntimeFilters(b, inference_wire_tests, &inference_wire_filters).step);
    const pooling_filters = [_][]const u8{
        "HA runnable ingress",
        "bounded admission",
        "runnable scheduler",
        "runnable execution",
        "decision functions remote transport",
        "runnable metrics",
        "workload envelope",
        "workload deadline",
        "config workload envelope",
        "pgwire retained budget",
        "pgwire shared session budget",
        "request admission bounds",
        "request admission lease",
        "request admission metrics",
        "config query waiting",
        "config runnable scheduler",
        "httpx workload policy",
        "httpx query waiting",
        "SQL query waiting",
        "httpx query admission",
        "httpx write admission",
        "SQL protected executor",
        "SQL protected control",
        "SQL prepared completion",
        "SQL stateless completion",
        "httpx stable transaction commit",
        "httpx SQL executes one relational page",
        "httpx SQL connection routes",
        "httpx SQL durable session settings",
        "SQL pgwire credential snapshot",
        "SQL preparation",
        "SQL setup",
        "SQL pgwire controlled datetime normalization",
        "SQL read resume fences",
        "read fence validates each",
        "read fence owns bounded",
        "read fence invalidates",
    };
    // Keep the entire curated selection, but compile it in serial batches.
    // The previous monolithic Linux test compiler was killed with a 22 GiB
    // container limit. Batching changes compilation, not runtime resource limits.
    const pooling_batch_size = 8;
    const pooling_batch_count = (pooling_filters.len + pooling_batch_size - 1) / pooling_batch_size;
    const test_support = @import("../../../build_support/antfly/test_support.zig");
    var pooling_batches: [pooling_batch_count]test_support.OwnerTests = undefined;
    var previous_pooling_compile: ?*std.Build.Step.Compile = null;
    for (&pooling_batches, 0..) |*batch, index| {
        const start = index * pooling_batch_size;
        const filters = pooling_filters[start..@min(start + pooling_batch_size, pooling_filters.len)];
        const tests = b.addTest(.{
            .name = b.fmt("default-workload-pooling-tests-{d}", .{index}),
            .root_module = options.api_http_runtime_test_mod,
            .filters = filters,
            .max_rss = @as(usize, if (target.result.os.tag == .macos) 22 else 23) * 1024 * 1024 * 1024,
            .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
        });
        // Inventory auditing needs every batch compiled first. Chain compiler
        // steps (not runs) to avoid an audit/compile cycle and concurrent peaks.
        if (previous_pooling_compile) |previous| tests.step.dependOn(&previous.step);
        previous_pooling_compile = tests;
        // Filters can overlap (e.g. "runnable scheduler" also matches
        // "config runnable scheduler"). Give each test to its first batch;
        // otherwise the combined inventory correctly rejects duplicate owners.
        batch.* = .{ .artifact = tests, .filters = filters, .skip_filters = pooling_filters[0..start] };
    }
    const pooling_step = b.step("antfly-workload-pooling-test", "Run all bounded waiting and shared HTTP/SQL admission contracts in serial compiler batches");
    // Combined inventory validates caller selections across all batches; runs
    // stay serial and every original default filter remains selected.
    test_support.addOwnerTestRuns(b, pooling_step, &pooling_batches, &.{});

    const remote_attempt_wire_tests = b.addTest(.{
        .name = "remote-attempt-wire-tests",
        .root_module = options.api_http_runtime_test_mod,
        .filters = &.{ "remote attempt wire", "remote attempt service credential", "remote worker attempt cannot", "api.internal_service_auth.", "remote attempt membership", "remote attempt client" },
        .max_rss = 4 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    b.step("antfly-remote-attempt-wire-test", "Verify signed remote attempt request bindings and finite duration forwarding")
        .dependOn(&addFilteredTestRunArtifact(b, remote_attempt_wire_tests).step);

    const retained_cursor_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/retained_cursor_admission_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, retained_cursor_root, true, true);
    retained_cursor_root.addImport("antfly_source_root", retained_cursor_root);
    const retained_cursor_tests = b.addTest(.{
        .name = "retained-cursor-admission-tests",
        .root_module = retained_cursor_root,
        .filters = &.{ "retained relational page admission", "retained document page admission", "retained metadata read admission", "retained capture admission", "retained normalization admission" },
        .max_rss = 6 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    b.step("antfly-retained-cursor-admission-test", "Verify native cursor pages borrow original admission only through physical read completion")
        .dependOn(&b.addRunArtifact(retained_cursor_tests).step);

    const retained_read_lifetime_tests = b.addTest(.{
        .name = "retained-read-lifetime-tests",
        .root_module = options.api_http_runtime_test_mod,
        .filters = &.{ "retained read registry", "retained read owner", "retained read RPC", "retained read client" },
        .max_rss = 6 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    b.step("antfly-retained-read-lifetime-test", "Verify physical snapshot and cursor retirement retains admission through cleanup")
        .dependOn(&addFilteredTestRunArtifact(b, retained_read_lifetime_tests).step);

    const composed_query_contract_tests = b.addTest(.{
        .name = "composed-query-contract-tests",
        .root_module = options.api_http_runtime_test_mod,
        .filters = &.{ "api.composed_query.", "api.lake_retained_cut.", "serverless objectstore verification caches" },
        .max_rss = 6 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    b.step("antfly-composed-query-contract-test", "Verify composed query ordering, continuation and exact result contracts")
        .dependOn(&addFilteredTestRunArtifact(b, composed_query_contract_tests).step);

    const internal_query_owner_tests = b.addTest(.{
        .name = "internal-query-owner-tests",
        .root_module = options.api_http_runtime_test_mod,
        .filters = &.{"internal query owner"},
        .max_rss = 14 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    b.step("antfly-internal-query-owner-test", "Verify internal query CPU, allocation and buffered response lifetimes")
        .dependOn(&addFilteredTestRunArtifact(b, internal_query_owner_tests).step);

    const signed_retained_read_tests = b.addTest(.{
        .name = "signed-retained-read-tests",
        .root_module = options.api_http_runtime_test_mod,
        .filters = &.{ "internal query owner signed ingress", "retained read RPC" },
        .max_rss = 8 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    b.step("antfly-signed-retained-read-test", "Verify authenticated retained RPC admission, signed routing and physical worker retirement")
        .dependOn(&addFilteredTestRunArtifact(b, signed_retained_read_tests).step);

    const remote_attempt_owner_tests = b.addTest(.{
        .name = "remote-attempt-owner-tests",
        .root_module = options.api_http_runtime_test_mod,
        .filters = &.{"remote attempt owner"},
        .max_rss = 16 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    b.step("antfly-remote-attempt-owner-test", "Verify owned remote bootstrap through direct and opaque API lifetimes")
        .dependOn(&addFilteredTestRunArtifact(b, remote_attempt_owner_tests).step);

    const pgwire_operation_error_test = b.addTest(.{
        .name = "pgwire-operation-error-tests",
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
        .root_module = b.createModule(.{ .root_source_file = b.path("pkg/antfly/src/pgwire/operation_error.zig"), .target = target, .optimize = optimize }),
        .max_rss = 1024 * 1024 * 1024,
    });
    pgwire_operation_error_test.root_module.addImport("antfly_test_error_logs", options.antfly_imports.storage_boundary.test_error_logs);
    b.step("antfly-pgwire-operation-error-test", "Verify backend cancellation preserves healthy sessions and actual frontend shutdown")
        .dependOn(&b.addRunArtifact(pgwire_operation_error_test).step);

    const execution_wait_test = b.addTest(.{
        .name = "execution-wait-tests",
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
        .root_module = b.createModule(.{ .root_source_file = b.path("lib/runtime/src/execution_wait.zig"), .target = target, .optimize = optimize }),
        .max_rss = 1024 * 1024 * 1024,
    });
    execution_wait_test.root_module.addImport("antfly_test_error_logs", options.antfly_imports.storage_boundary.test_error_logs);
    b.step("antfly-execution-wait-test", "Verify shared wait loans and physical discard after failed reentry")
        .dependOn(&b.addRunArtifact(execution_wait_test).step);

    const remote_reply_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/common/remote_attempt_reply.zig"),
        .target = target,
        .optimize = optimize,
    });
    remote_reply_mod.addImport("antfly_runtime_fs", options.antfly_imports.runtime_fs);
    remote_reply_mod.addImport("antfly_runtime_abi", options.antfly_imports.runtime_abi);
    const remote_reply_tests = b.addTest(.{
        .name = "remote-reply-tests",
        .root_module = remote_reply_mod,
        .max_rss = 1024 * 1024 * 1024,
    });
    b.step("antfly-remote-reply-test", "Verify root-signed quiescence replies before uncertainty reconciliation")
        .dependOn(&b.addRunArtifact(remote_reply_tests).step);

    const remote_worker_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/common/remote_worker.zig"),
        .target = target,
        .optimize = optimize,
    });
    remote_worker_mod.addImport("antfly_runtime_fs", options.antfly_imports.runtime_fs);
    remote_worker_mod.addImport("antfly_runtime_abi", options.antfly_imports.runtime_abi);
    const remote_worker_tests = b.addTest(.{
        .name = "remote-worker-tests",
        .root_module = remote_worker_mod,
        .max_rss = 1024 * 1024 * 1024,
    });
    b.step("antfly-remote-worker-test", "Verify bounded remote resource ownership through physical retirement and fencing")
        .dependOn(&b.addRunArtifact(remote_worker_tests).step);

    const remote_fence_store_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/common/remote_fence_store.zig"),
        .target = target,
        .optimize = optimize,
    });
    remote_fence_store_mod.addImport("antfly_runtime_fs", options.antfly_imports.runtime_fs);
    remote_fence_store_mod.addImport("antfly_runtime_abi", options.antfly_imports.runtime_abi);
    const remote_fence_store_tests = b.addTest(.{
        .name = "remote-fence-store-tests",
        .root_module = remote_fence_store_mod,
        .max_rss = 1024 * 1024 * 1024,
    });
    b.step("antfly-remote-fence-store-test", "Verify durable worker fence floors before acknowledgements")
        .dependOn(&b.addRunArtifact(remote_fence_store_tests).step);

    const remote_credit_wait_tests = b.addTest(.{
        .name = "remote-credit-wait-tests",
        .root_module = options.api_http_runtime_test_mod,
        .filters = &.{"remote attempt credit wait"},
        .max_rss = 3 * 1024 * 1024 * 1024,
    });
    b.step("antfly-remote-credit-wait-test", "Verify bounded remote credit waits release CPU and never publish on cancellation")
        .dependOn(&addFilteredTestRunArtifact(b, remote_credit_wait_tests).step);

    const remote_attempt_tests = b.addTest(.{
        .name = "remote-attempt-tests",
        .root_module = b.createModule(.{
            .root_source_file = b.path("pkg/antfly/src/common/remote_attempts.zig"),
            .target = target,
            .optimize = optimize,
        }),
        .max_rss = 1024 * 1024 * 1024,
    });
    b.step("antfly-remote-attempt-test", "Verify remote uncertainty accounting, duplicate rejection and generation gate state machines")
        .dependOn(&b.addRunArtifact(remote_attempt_tests).step);

    const sql_protocol_pooling_tests = b.addTest(.{
        .name = "sql-protocol-pooling-tests",
        .root_module = options.api_http_runtime_test_mod,
        .filters = &.{ "httpx SQL executes one relational page with exact integer parameters", "SQL pgwire dispatch preserves imported executor authority including unavailable views" },
        .max_rss = 12 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    b.step("antfly-sql-protocol-pooling-test", "Verify native mixed HTTP/pgwire execution, sessions and protected control progress")
        .dependOn(&addFilteredTestRunArtifact(b, sql_protocol_pooling_tests).step);

    const sql_lake_admission_tests = b.addTest(.{
        .name = "sql-lake-admission-tests",
        .root_module = options.api_http_runtime_test_mod,
        .filters = &.{ "SQL lake request admission", "lake SQL accepted capability", "external lake snapshot retirement", "external lake segmented WAL", "external lake WAL collection" },
        .max_rss = 6 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    b.step("antfly-sql-lake-admission-test", "Verify original SQL execution across retained cuts, catalog/WAL reads and snapshot pins")
        .dependOn(&addFilteredTestRunArtifact(b, sql_lake_admission_tests).step);

    const sql_scan_startup_tests = b.addTest(.{
        .name = "sql-scan-startup-tests",
        .root_module = options.api_http_runtime_test_mod,
        .filters = &.{
            "SQL scan startup wait releases runnable capacity and preserves original authority",
            "SQL retained cursor admission",
            "SQL scoped ordered scans own cursor callbacks preserve ordered quotas and join original failures",
        },
        .max_rss = 3 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    b.step("antfly-sql-scan-startup-test", "Verify scan startup lends CPU and preserves cancellation, deadline and shutdown authority")
        .dependOn(&addFilteredTestRunArtifact(b, sql_scan_startup_tests).step);

    const runnable_lane_tests = b.addTest(.{
        .name = "runnable-lane-tests",
        .root_module = options.api_http_runtime_test_mod,
        .filters = &.{ "runnable scheduler", "runnable execution", "runnable read lanes", "runnable CPU loan" },
        .max_rss = 8 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    b.step("antfly-runnable-lanes-test", "Verify protected read lanes and existing scheduler ownership contracts")
        .dependOn(&addFilteredTestRunArtifact(b, runnable_lane_tests).step);

    const read_resource_tests = b.addTest(.{
        .name = "runnable-read-resource-tests",
        .root_module = options.api_http_runtime_test_mod,
        .filters = &.{"runnable read resources"},
        .max_rss = 3 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    b.step("antfly-runnable-read-resource-test", "Verify coordinated physical heap and scheduler demotion ownership")
        .dependOn(&addFilteredTestRunArtifact(b, read_resource_tests).step);

    const point_lookup_tests = b.addTest(.{
        .name = "point-lookup-admission-tests",
        .root_module = options.api_http_runtime_test_mod,
        .filters = &.{ "runnable point lookup", "runnable dependency wait", "api http point lookup retries bounded local readiness races", "httpx antfly reads preserve availability and terminal failures" },
        .max_rss = 4 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    b.step("antfly-point-lookup-admission-test", "Verify public point lookup ownership, physical output retirement and checked readiness waits")
        .dependOn(&addFilteredTestRunArtifact(b, point_lookup_tests).step);

    const read_policy_tests = b.addTest(.{
        .name = "runnable-read-policy-tests",
        .root_module = options.api_http_runtime_test_mod,
        .filters = &.{ "runnable read lanes", "SQL read lane", "runnable metrics", "API kernel ABI", "API imported runtime capability" },
        .max_rss = 3 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    b.step("antfly-runnable-read-policy-test", "Verify read classification, lane isolation and telemetry")
        .dependOn(&addFilteredTestRunArtifact(b, read_policy_tests).step);

    const admission_ownership_tests = b.addTest(.{
        .name = "admission-ownership-tests",
        .root_module = options.api_http_runtime_test_mod,
        .filters = &.{
            "bounded admission ",
            "request admission bounds positive capacity and preserves unlimited mode",
            "request admission lease releases exactly once",
            "request admission metrics use the shared admission namespace",
        },
        .max_rss = 3 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    b.step("antfly-admission-ownership-test", "Verify operation grant metrics and fail-fast, queued, cancellation and drain ownership")
        .dependOn(&addFilteredTestRunArtifact(b, admission_ownership_tests).step);

    const pgwire_read_pool_tests = b.addTest(.{
        .name = "pgwire-read-pool-tests",
        .root_module = options.api_http_runtime_test_mod,
        .filters = &.{ "SQL pgwire supervised cleanup", "SQL pgwire retained lease", "SQL pgwire materialized read pool", "remote-only owner" },
        .max_rss = 3 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    b.step("antfly-pgwire-read-pool-test", "Verify portal read heap ownership through page retirement and joined cleanup")
        .dependOn(&addFilteredTestRunArtifact(b, pgwire_read_pool_tests).step);

    const read_pool_startup_tests = b.addTest(.{
        .name = "read-pool-startup-tests",
        .root_module = options.api_http_runtime_test_mod,
        .filters = &.{"workload read pools startup"},
        .max_rss = 3 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    b.step("antfly-read-pool-startup-test", "Verify production read pool publication, rollback and total node envelope")
        .dependOn(&addFilteredTestRunArtifact(b, read_pool_startup_tests).step);

    const startup_transport_tests = b.addTest(.{
        .name = "startup-transport-tests",
        .root_module = options.api_http_runtime_test_mod,
        .filters = &.{"workload public transport envelope fits retained roots and honors smaller operator limits"},
        .max_rss = 3 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    b.step("antfly-startup-transport-test", "Verify effective body and application budgets fit their stable startup owner")
        .dependOn(&addFilteredTestRunArtifact(b, startup_transport_tests).step);

    // No emitted binary is requested: type the production HTTP entry point,
    // worker and backend calls without compiling/linking the full provider
    // archives. Runtime behavior remains covered by the corresponding suites.
    const http_deadline_composition_tests = b.addTest(.{
        .name = "http-deadline-composition-tests",
        .root_module = options.api_http_runtime_test_mod,
        .filters = &.{ "HTTP ingress deadline composition", "internal transaction ingress establishes and validates pre-decision deadline", "API kernel HTTP original deadline", "linked API dispatch preserves kernel-owned ingress policy", "HTTP request lifetime canonical transport", "common config HTTP lifetime", "local inference connection ABI rejects old and truncated" },
        .max_rss = 4 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    b.step("antfly-http-deadline-composition-test", "Verify forwarded budgets tighten original HTTP lifetimes without changing clock authority")
        .dependOn(&addFilteredTestRunArtifact(b, http_deadline_composition_tests).step);

    const http_ingress_authorization_tests = b.addTest(.{
        .name = "http-ingress-authorization-tests",
        .root_module = options.api_http_runtime_test_mod,
        .filters = &.{ "online merge ingress bounds owner work using the transport clock", "httpx schema patch merges at the authority and accepts version zero ETag", "httpx antfly routes require auth and enforce admin middleware" },
        .max_rss = 4 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    b.step("antfly-http-ingress-authorization-test", "Verify internal merge deadlines and public schema method authorization")
        .dependOn(&addFilteredTestRunArtifact(b, http_ingress_authorization_tests).step);

    const sql_http_ingress_check = b.addTest(.{
        .name = "sql-http-ingress-check",
        .root_module = options.api_http_runtime_test_mod,
        .filters = &.{
            "httpx SQL dispatch preserves imported executor authority",
            "httpx SQL rejects unsupported shapes and releases dynamic admission",
            "httpx SQL executes one relational page with exact integer parameters",
        },
        .max_rss = 3 * 1024 * 1024 * 1024,
    });
    b.step("antfly-sql-http-ingress-check", "Type-check production SQL HTTP decode and execution handoffs without emitting a binary")
        .dependOn(&sql_http_ingress_check.step);

    const sql_http_response_worker_tests = b.addTest(.{
        .name = "sql-http-response-worker-tests",
        .root_module = options.api_http_runtime_test_mod,
        .filters = &.{"HTTP SQL worker response"},
        .max_rss = 3 * 1024 * 1024 * 1024,
    });
    b.step("antfly-sql-http-response-worker-test", "Verify original SQL root ownership and physical result retirement during response encoding")
        .dependOn(&b.addRunArtifact(sql_http_response_worker_tests).step);

    const sql_commit_completion_tests = b.addTest(.{
        .name = "sql-commit-completion-tests",
        .root_module = options.api_http_runtime_test_mod,
        .filters = &.{"SQL prepared completion"},
        .max_rss = @as(usize, if (target.result.os.tag == .macos) 22 else 23) * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    b.step("antfly-sql-commit-completion-test", "Run production SQL prepared commit handoff, durable seal and original-fence contracts")
        .dependOn(&addFilteredTestRunArtifact(b, sql_commit_completion_tests).step);

    const sql_stateless_completion_tests = b.addTest(.{
        .name = "sql-stateless-completion-tests",
        .root_module = options.api_http_runtime_test_mod,
        .filters = &.{"SQL stateless completion"},
        .max_rss = @as(usize, if (target.result.os.tag == .macos) 22 else 23) * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    b.step("antfly-sql-stateless-completion-test", "Run stateless SQL native handoff, original fences and response ownership contracts")
        .dependOn(&addFilteredTestRunArtifact(b, sql_stateless_completion_tests).step);

    const pgwire_array_page_tests = b.addTest(.{
        .name = "pgwire-array-page-tests",
        .root_module = options.api_http_runtime_test_mod,
        .filters = &.{"pgwire native array page delivery"},
        .max_rss = 3 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    const pgwire_array_page_run = addFilteredTestRunArtifact(b, pgwire_array_page_tests);
    pgwire_array_page_run.addArg("--require-no-skips");
    b.step("antfly-pgwire-array-page-test", "Verify native pgwire array page conversion and allocation-failure cleanup")
        .dependOn(&pgwire_array_page_run.step);

    const pgwire_read_pool_check = b.addTest(.{
        .name = "pgwire-read-pool-check",
        .root_module = options.api_http_runtime_test_mod,
        .filters = &.{ "api.sql_pgwire", "remote-only owner" },
        .max_rss = 3 * 1024 * 1024 * 1024,
    });
    b.step("antfly-pgwire-read-pool-check", "Type-check production pgwire portal allocation and retirement paths")
        .dependOn(&pgwire_read_pool_check.step);

    const pgwire_retirement_tests = b.addTest(.{
        .name = "pgwire-retirement-tests",
        .root_module = options.api_http_runtime_test_mod,
        .filters = &.{"api.sql_pgwire"},
        .max_rss = @as(usize, if (target.result.os.tag == .macos) 22 else 23) * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    const pgwire_retirement_run = addFilteredTestRunArtifact(b, pgwire_retirement_tests);
    pgwire_retirement_run.addArg("--require-no-skips");
    b.step("antfly-pgwire-retirement-test", "Run native pgwire authority, session and completion regressions")
        .dependOn(&pgwire_retirement_run.step);

    const read_authority_filters = [_][]const u8{ "SQL pgwire credential snapshot", "SQL read resume fences", "read fence validates each", "read fence owns bounded", "read fence invalidates" };
    const read_authority_tests = b.addTest(.{
        .name = "sql-read-authority-tests",
        .root_module = options.api_http_runtime_test_mod,
        .filters = &read_authority_filters,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    b.step("antfly-read-authority-test", "Run current SQL credential and retained read-policy contracts")
        .dependOn(&addFilteredTestRunArtifactWithRuntimeFilters(b, read_authority_tests, &read_authority_filters).step);

    const public_api_parity_default_filters = [_][]const u8{
        "empty activation page validates source catalog without probing external parents",
        "SQL API cross-table MERGE retains both source and target range proofs",
        "api http server authenticates bounded online merge owner routes",
        "online merge ingress bounds owner work using the transport clock",
        "online merge private standalone rewrite port pins authority and never fabricates Raft coordinates",
        "online merge private port fences owners cancellation and deadlines before dispatch",
        "online merge private port preserves source recovery errors through foreign runtime dispatch",
        "join planning",
        "public openapi contract module is generated and wired",
        "public enrollment encoding preserves a signed u128 root without JSON rounding",
        "store-root enrollment requires cluster admin and forwards only valid possession proof",
        "admin openapi contract module is generated and wired",
        "internal openapi contract module is generated and wired",
        "metadata openapi module generates extractor surface for routed endpoints",
        "usermgr openapi module generates extractor surface for routed endpoints",
        "client openapi module resolves shared refs through owner modules",
        "batch parser accepts supported Go transform op spelling",
        "batch parser accepts pull for exact array values",
        "batch parser rejects every recognized but unsupported transform operator",
        "public table batch handler forwards pull transforms",
        "public table contract exposes migration metadata",
        "api http client round-trips public table management routes",
        "api http server serves status",
        "api http server returns json eval and query builder validation errors",
        "api http server returns json not found for missing query builder table",
        "api http server serves eval response envelope",
        "api http server serves query builder response envelope",
        "api http server query builder infers semantic indexes from table metadata",
        "api http server query builder handles tree graph indexes",
        "api http server query builder replays clarification decisions",
        "api http server serves secrets crud when backed by a local store",
        "api http server lists secrets status without a local secret store",
        "api http server rejects secret writes without a local secret store",
        "api http server serves table lookup with version header",
        "api http server serves table scan as ndjson",
        "api http server routes table query through read schema full text index",
        "api http server serves table query response envelope",
        "api http server query string boolean controls survive reopen",
        "api http server executes public Query filter roots and compositions",
        "public table query handler preserves structured filter and hierarchy diagnostics",
        "query dependency errors expose a stable JSON retry contract",
        "api http server serves retrieval agent response envelope",
        "api http server serves table batch writes",
        "api http server routes table batches through the batch commit hook",
        "api http server coordinated batch outcomes retain prepared names and conflict keys",
        "api http server rewrite job authorization follows the complete cohort",
        "auto bulk max-window request waits for idle finish",
        "auto bulk group writes release leases so idle finish can publish",
        "auto bulk background finish skips entries with active foreground leases",
        "provisioned table write source seeds doc identity namespace from table range",
        "provisioned table write source cached runtime status does not fetch catalog coverage",
        "managed startup catch-up uses provided indexes json without catalog fetch",
        "managed startup catch-up marks FileNotFound index open terminal degraded",
        "managed startup catch-up preserves restore repair debt while index load is terminal",
        "managed startup catch-up defers while shared bulk ingest state is active",
        "idle startup runtime status preserves live empty cached status",
        "idle startup completion cannot downgrade a superseding live index status",
        "api http server serves table batch transforms",
        "api http server updates local table schema through bound write source",
        "api http server serves public transaction commit route",
        "api http server surfaces structured participant diagnostics for unavailable transaction commits",
        "api http server surfaces structured decision conflicts for transaction commits",
        "api http server surfaces structured torn-state conflicts when txn record is missing",
        "api http server surfaces structured torn-state conflicts when txn record is corrupted",
        "api http server serves transaction session cleanup route",
        "api http server serves table metadata list and detail",
        "api http server serves runtime schema debug on table and index detail",
        "api http server serves table index metadata routes",
        "api runtime status upsert keeps one authoritative observation per group",
        "api index status uses read runtime status without consulting write source",
        "api index status refreshes synthetic configured index status from write source",
        "api index status refreshes writer when read snapshot omits requested index",
        "api index status prefers current same-name incarnation from write source",
        "api index status uses propagated remote store runtime status",
        "api index status ignores propagated runtime status from removed owner",
        "api index status reports missing remote shard as not ready",
        "single embeddings index encoder keeps backfill active while enrichment replay lags",
        "api http server serves local index runtime status",
        "api http server join planner uses complete fresh local stats before metadata publication",
        "api http server serves provisioned index runtime backfill status across shards",
        "table contract rejects unsupported index kinds before catalog admission",
        "table contract keeps operational create request failures on the internal error path",
        "api http server rejects unsupported table index before metadata publication",
        "api http server serves table create and drop",
        "api http server serves table metadata routes against real metadata service",
        "api http server create table with replication sources returns encoded table detail",
        "api http server lists cluster backups through public route",
        "api http server returns retryable not leader when cluster backup read barrier times out",
        "api http server fails closed when backup fences are unsupported",
        "api http server cluster backup succeeds after load balanced metadata timeout retry",
        "api http server does not advertise a retry after cluster backup side effects begin",
        "cluster backup retains its fenced attempt after an ambiguous table outcome",
        "table backup retry preserves the retained ambiguous generation",
        "public metadata mutation retries transient authority loss only within its deadline",
        "api http server rejects restore before persistence without an asynchronous worker",
        "configured api http server attaches durable restore job persistence",
        "restore job list paginates after authorization filtering",
        "restore job list bounds authorization scans with an empty continuation page",
        "api http server backs up and restores a table through public routes",
        "api http server cluster overwrite restores from read-only repository without dropping live table",
        "api http server durability-pending restore preserves committed metadata",
        "api http server cluster restore rehydrates extension metadata",
        "api http server prefers metadata-owned restore over inline write-source restore",
        "api http server does not retry authoritative metadata table-exists conflict",
        "api http server retries interrupted metadata restore publication",
        "api query contract parses direct JSON-pointer path aliases",
        "api query contract serializes derived hierarchy ancestry",
        "api query contract serializes mention evidence hierarchy",
        "api query contract validates canonical hierarchy controls",
        "query max score preserves negative relevance scores",
        "query hit exposes relevance score and raw vector distance separately",
        "query merge orders pure dense results by descending relevance score",
        "query parser accepts graph pattern searches",
        "query parser treats explicit graph document fields as a projection",
        "api http server serves fielded full-text search through mcp tools",
        "api query contract canonicalizes public Query filter roots and compositions",
        "api query contract keeps filter carriers out of vector fusion",
        "api query contract accepts multi_match bool_prefix full text",
        "api query contract bounds public fuzzy integers without narrowing traps",
        "api query contract preserves supported match options and rejects semantic loss",
        "api query contract preserves nested direct boosts and rejects ambiguous scoring roots",
        "api query contract accepts explicit empty public boolean branches",
        "api query contract preserves schema-dependent canonical ranges",
        "api query contract preserves text native public filter variants",
        "api query contract accepts text-index queries in canonical boolean filters",
        "api query contract expands text-index document filter bindings",
        "api query contract keeps text-index bindings in bool must non-scoring",
        "api query contract keeps full text binding references non-scoring",
        "api query contract retains structured bindings beside text bindings",
        "api query contract retains structured dependencies of text bindings",
        "api query contract classifies transitive text binding dependencies",
        "api query contract rejects unused bindings with unknown syntax",
        "api query contract splits mixed structured and text binding conjunctions",
        "api query contract binding expansion observes zero timeout",
        "api query contract honors caller absolute deadline during normalization",
        "api query contract expansion budget checks its absolute deadline",
        "api query contract final binding validation observes caller deadline",
        "api query contract limits binding expansion growth not input size",
        "api query contract bounds expanded binding output bytes",
        "api query contract combines public and internal filter representations losslessly",
        "structured filter grammar validates ranges without a runtime schema",
        "api query contract preserves canonical structured compounds without speculative parsing",
        "api query contract cleans up partially parsed direct query arrays",
        "api query contract reports the failing nested filter node",
        "api query contract classifies typed filter errors as validation errors",
        "api query contract applies one inclusive depth limit to filter dependencies",
        "api query contract distinguishes explicit zero from implicit pure should minimum",
        "api query contract orders forward document filter dependencies",
        "api query contract rejects invalid document filter dependency graphs",
        "api query contract keeps compact ref fields distinct from binding references",
        "api query contract keeps should optional beside required filters",
        "encode query request losslessly carries optional should and named filter bindings",
        "encode query request rejects duplicate named filter bindings",
        "encode query request round-trips all public phrase geo and ip queries",
        "encode query request round-trips every scalar Query text variant",
        "encode query request round-trips schema valid multi match boosts",
        "encode query request rejects invalid public phrase geo and ip values",
        "optional pure should preserves zero baseline and text scores",
        "remote query preserves optional should and named filter bindings",
        "api query contract graph evaluation preserves base hit paging and shard windows",
        "distributed reranking widens retrieval and stays coordinator owned",
        "reranker candidate and output windows have distinct bounds",
        "reranker admission precedes candidate rendering",
        "reranker component paging includes the post-rerank offset",
        "reranker paging preserves the underlying retrieval total",
        "distributed join context forwards one absolute deadline to every query callback",
        "distributed join ownership and transport failures remain retryable and fail closed",
        "distributed join search hit JSON normalizes non-finite scores",
        "distributed join unmatched worker returns only unmatched synthetic hits",
        "distributed join applies auth row filter to right table filter query",
        "distributed join preserves native public filters when adding join predicates",
        "scan request errors map to stable client responses",
        "httpx antfly reads preserve availability and terminal failures",
        "httpx lookup revalidates missing catalog bindings across restore",
        "httpx antfly scan honors optional body and documented bad requests",
        "httpx antfly scan reports a stale owner descriptor as temporarily unavailable",
        "httpx multi batch route uses the batch commit hook and public response contract",
        "httpx stable transaction commit durably hands off recovery before acknowledgement",
        "public transaction preparation generation transition is retryable without outcome claims",
        "httpx shared registrar keeps root probes and rejects removed data aliases",
        "httpx storage maintenance routes call typed operations directly",
        "httpx antfly routes require auth and enforce admin middleware",
        "httpx relational row query mutation endpoints enforce exact versions and schema epochs",
        "httpx SQL",
        "httpx retained read",
        "httpx hidden handoff receipt rejects public caller even in legacy internal mode",
        "httpx FK source control rejects missing service token in legacy internal mode",
        "hidden generation handoff receipt requires exact scoped read-index request",
        "hidden generation handoff identity requires exact scoped read-index request",
        "api.sql_execution",
        "api.sql_pgwire",
        "api.sql_session",
        "api.sql_prepared",
        "HTTP connection record",
        "HTTP connection DISCARD",
        "HTTP connection recovers",
        "HTTP connection keeps",
        "HTTP connection reconciles",
        "HTTP connection publishes",
        "direct public transaction routes exclude connection-owned sessions",
        "api.sql_catalog",
        "api.sql_truncate",
        "api.sql_schema_cache",
        "SQL typed mutation boundary",
        "SQL JSON null metadata",
        "SQL session metadata",
        "SQL staged statements",
        "relational row query coordinator",
        "httpx antfly schema update returns full table status after projection",
        "httpx antfly schema update owns self partial support and requires coordinated publication",
        "httpx schema patch merges at the authority and accepts version zero ETag",
        "schema ETags are strong and preserve version zero",
        "request context observes cancellation before deadline",
        "admission reservation releases exactly once",
        "probe operations distinguish health from readiness",
        "unsupported maintenance operations fail before transport adaptation",
        "storage maintenance typed operations run without an HTTP request",
        "httpx owned response preserves retryable JSON metadata",
        "httpx query admission rejects saturated queries without blocking control routes",
        "httpx query admission releases a cancelled query slot",
        "httpx query admission treats zero capacity as unlimited",
        "httpx write admission rejects saturated table mutations",
        "httpx inference connection uses the configured shared admission owner",
        "local inference connection admission is owned exactly once by its target",
        "httpx inference connection preserves upstream retry guidance",
        "request admission bounds positive capacity and preserves unlimited mode",
        "shared application admission covers MCP query and write operations",
        "compiled stored filters honor canonical JSON pointer fields and escapes",
        "jsonDocMatchesPatternFilter supports stored structured filters",
        "stored term filters preserve JSON scalar kinds",
        "api http invalid filter query response names the offending node",
        "api http unsupported filter query response names the offending node",
        "api http server drop table observes metadata absence before local cleanup",
        "public api smoke integration creates table inserts and queries documents",
        "provisioned table write source routes batch writes across ranges",
        "public api integration recreates managed embeddings index after corrupt artifact",
        "public api split integration uses distributed global text stats for bm25 and significant_terms",
        "public api multi-node integration routes CRUD from a non-host node",
    };
    const public_api_parity_runtime_filters = selectTestFilters(b, &public_api_parity_default_filters);
    const public_api_parity = @import("linked_tests.zig").add(b, .{
        .name = "public-api-parity",
        .root_module = antfly_test_mod,
        .filters = compileFiltersWithAnchors(b, &.{"api module compiles"}, public_api_parity_runtime_filters),
        // The macOS debug root includes the complete public transport,
        // generated-contract, and native-index surface. Debug test
        // compilation has a measured native floor in testCompileMaxRss;
        // retain this authored fallback for unmeasured host profiles.
        .max_rss = @as(usize, if (target.result.os.tag == .macos) 15 else 7) * 1024 * 1024 * 1024,
        .test_runner = .{
            .path = b.path("pkg/antfly-embedded/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const public_api_parity_tests = public_api_parity.executable;
    const run_public_api_parity_tests = addFilteredTestRunArtifactWithRuntimeFilters(
        b,
        public_api_parity_tests,
        public_api_parity_runtime_filters,
    );
    const run_public_api_parity_aggregate_tests = addFilteredTestRunArtifactWithRuntimeFilters(
        b,
        public_api_parity_tests,
        public_api_parity_runtime_filters,
    );
    run_public_api_parity_tests.step.dependOn(&openapi_root_check.step);
    run_public_api_parity_aggregate_tests.step.dependOn(&openapi_root_check.step);
    const public_api_parity_test_step = b.step("public-api-parity-test", "Run focused stateful public API parity tests");
    public_api_parity_test_step.dependOn(&run_public_api_parity_tests.step);

    const lib_api_auth_default_filters = [_][]const u8{
        "trusted principal table operations",
        "storage migration job observation preserves admitted and unpublished catalog state",
        "storage migration command admission fences delayed starts across handlers",
        "api http server requires auth on public routes when enabled",
        "continuous HA rejects non-replicated public mutations before handlers",
        "HA mutation middleware fails closed for unregistered HTTP methods",
        "continuous HA freezes pre-existing restore workers and resumption",
        "continuous HA allows a configured RemoteApply batch write",
        "api http server document scan requires table read permission",
        "api http server does not replay create when local reconcile lease is lost",
        "api http server returns retryable not leader when metadata proposal is dropped",
        "api http server returns retryable not leader through public table adapter mutation",
        "api http server returns retryable not leader when cluster backup read barrier times out",
        "api http server fails closed when backup fences are unsupported",
        "api http server cluster backup succeeds after load balanced metadata timeout retry",
        "api http server does not advertise a retry after cluster backup side effects begin",
        "cluster backup retains its fenced attempt after an ambiguous table outcome",
        "table backup retry preserves the retained ambiguous generation",
        "typed HA route operation dispatches admin and internal executors",
        "typed HA route operation requires exact bearer token for internal replication routes",
        "api http server forbids non-admin secret access when auth is enabled",
        "api http server query builder requires table read permission when auth is enabled",
        "global multi-query rechecks live permission before each table result",
        "api http server restricts runtime schema debug to admins when auth is enabled",
        "api http server serves user management routes when auth is enabled",
        "api http server serves api key and row filter routes",
        "api http server returns json user auth errors",
        "document artifact routes declare read and admin permissions",
        "api http server durably retries table repair job when background submit is closing",
        "api http server serves MCP and hides A2A by default",
        "api http server serves MCP and opted-in A2A protocol surfaces",
        "api http server serves ARD catalogs with public bootstrap and authenticated tenant entries",
        "api http server requires auth for ARD tenant catalog when auth is enabled",
        "api http server serves ARD OpenAPI, skill, resource, and registry endpoints",
        "api http server filters extension mcp tools by trusted principal table permissions",
        "api http server serves document lookup through mcp tool",
        "ARD search filters scoped catalog entries",
        "ARD search requires text while explore accepts filter-only requests",
        "ARD search supports publisher and metadata filters",
        "ARD search validates federation and returns referral envelope",
        "ARD explore returns requested facet buckets over scoped entries",
        "ARD catalog entries contain required value or reference fields",
        "ARD catalog omits A2A discovery when disabled",
        "ARD catalog resolves artifact urls against configured base url",
        "ARD MCP descriptors resolve endpoints against configured base url",
        "ARD catalog hides admin-only built-in skills from non-admin entries",
        "ARD catalog applies declared table permissions to built-in skills",
        "ARD extension package entries use trust provenance for artifact digests",
        "ARD search supports extension metadata filters",
        "ARD profile filter keeps only profile-compatible skills",
        "auth row filter resolver expands username references",
        "auth row filter resolver does not parse unused malformed metadata",
        "auth row filter resolver expands metadata references",
        "auth row filter resolver shares one bounded metadata expansion budget",
        "auth row filter validator accepts username references",
        "auth row filter admission requires executable Zig filter syntax",
        "auth row filter resolver rejects unsupported auth paths",
        "auth row filter validator rejects malformed auth node",
        "effective resolved row filter prefers table filter before wildcard",
        "artifact operations apply source document row filter visibility",
        "scan line key uses reserved _id document identity",
    };
    const lib_api_auth_runtime_filters = selectTestFilters(
        b,
        &lib_api_auth_default_filters,
    );
    // Large native Debug auth objects need the same relocatable-object and
    // native final-link path as the public API suite.
    const lib_api_auth = @import("linked_tests.zig").add(b, .{
        .name = "api-auth-tests",
        .max_rss = @import("runtime_memory.zig").testCompileMaxRss("api-auth-tests", b.graph.host.result, target.result, optimize, if (target.result.os.tag == .macos) 12 * 1024 * 1024 * 1024 else 0),
        .root_module = antfly_test_mod,
        .filters = lib_api_auth_runtime_filters,
        .test_runner = .{
            .path = b.path("pkg/antfly-embedded/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const lib_api_auth_tests = lib_api_auth.executable;
    const run_lib_api_auth_tests = addFilteredTestRunArtifactWithRuntimeFilters(
        b,
        lib_api_auth_tests,
        lib_api_auth_runtime_filters,
    );
    run_lib_api_auth_tests.step.dependOn(&openapi_root_check.step);
    const lib_api_auth_test_step = b.step("antfly-api-auth-test", "Run focused API auth/usermgr HTTP tests");
    lib_api_auth_test_step.dependOn(&run_lib_api_auth_tests.step);

    // The real MCP transport/database security regression does not need the
    // monolithic library discovery root or its full-suite memory reservation.
    const mcp_auth_filters = &.{ "api http server serves document lookup through mcp tool", "trusted principal table operations" };
    const mcp_auth_tests = b.addTest(.{
        .name = "api-mcp-auth-tests",
        .root_module = options.api_http_runtime_test_mod,
        .filters = mcp_auth_filters,
        .test_runner = .{
            .path = b.path("pkg/antfly-embedded/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    b.step("antfly-api-mcp-auth-test", "Run real MCP trusted-principal authorization regressions")
        .dependOn(&addFilteredTestRunArtifactWithRuntimeFilters(b, mcp_auth_tests, mcp_auth_filters).step);

    const source_merge_filters = &.{"linear merge"};
    const source_merge_tests = b.addTest(.{
        .name = "api-source-merge-tests",
        .root_module = options.api_http_runtime_test_mod,
        .filters = source_merge_filters,
        .test_runner = .{
            .path = b.path("pkg/antfly-embedded/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    b.step("antfly-api-source-merge-test", "Run bounded source LinearMerge regressions")
        .dependOn(&addFilteredTestRunArtifactWithRuntimeFilters(b, source_merge_tests, source_merge_filters).step);

    const authorization_sink_filters = [_][]const u8{
        "api http server document scan requires table read permission",
        "global multi-query rechecks live permission before each table result",
        "transaction principals bind sessions to credential identity",
        "api transaction sessions enforce principal permissions and row filters",
        "stored destination admission requires write permission on every eventual sink",
        "query builder runtime preflight injects mandatory row filter",
        "stored destination envelopes cannot be forged and validate on resume",
        "stored destination grants bind credential source and live permissions",
        "api http client forwards bounded raft batch routing context without allocation",
        "internal service request signing uses the transport clock authority",
        "api http client authenticates only the internal API namespace",
        "MCP document sampling pushes mandatory row filters into storage scans",
        "internal service credentials cannot authorize public inference routes",
        "legacy restore jobs resume only when backed-up definitions are sink free",
        "failed destination authorization refresh reuses the idempotent restore job",
        "destination authorization adoption requires source table admin",
        "legacy stored destinations can be adopted idempotently",
        "api http server cluster restore",
        "cluster restore repository errors preserve operational failure semantics",
        "internal namespace requires a service principal except hot standby",
        "usermgr api key permission intersection narrows owner and key wildcards",
        "httpx internal control routes call typed operations directly",
    };
    const authorization_sink_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &authorization_sink_filters,
        .test_runner = .{
            .path = b.path("pkg/antfly-embedded/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_authorization_sink_tests = addFilteredTestRunArtifact(b, authorization_sink_tests);
    run_authorization_sink_tests.step.dependOn(&openapi_root_check.step);
    const authorization_sink_test_step = b.step(
        "antfly-api-authorization-sink-test",
        "Run exploit regressions for API authorization sinks",
    );
    authorization_sink_test_step.dependOn(&run_authorization_sink_tests.step);
    authorization_sink_test_step.dependOn(&run_lib_usermgr_tests.step);

    const authorization_audit_step = b.step(
        "authorization-audit",
        "Run the authorization sink audit and broader API auth suite",
    );
    authorization_audit_step.dependOn(authorization_sink_test_step);
    authorization_audit_step.dependOn(lib_api_auth_test_step);

    const algebraic_dynamic_template_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{
            "dynamic template",
            "child cardinality cache",
            "public algebraic index definitions",
            "metadata.algebraic schema regeneration",
            "db managed algebraic admission builds and reopens",
        },
        .test_runner = .{
            .path = b.path("pkg/antfly-embedded/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_algebraic_dynamic_template_tests = b.addRunArtifact(algebraic_dynamic_template_tests);
    run_algebraic_dynamic_template_tests.step.dependOn(&openapi_root_check.step);
    const lib_storage_maintenance_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{
            "storage maintenance requires an asynchronous backend runtime",
            "storage maintenance coordinator is idempotent and single flight",
            "storage maintenance job ids are namespaced by server boot",
            "storage maintenance cancellation reaches a cooperative engine",
            "storage maintenance shutdown fences and drains its backend runtime owner",
            "storage maintenance snapshots remain valid after retention pruning",
            "storage maintenance append allocation failure does not wedge coordinator",
        },
        .test_runner = .{
            .path = b.path("pkg/antfly-embedded/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lib_storage_maintenance_tests = addFilteredTestRunArtifact(b, lib_storage_maintenance_tests);
    const lib_storage_maintenance_test_step = b.step("antfly-storage-maintenance-test", "Run storage maintenance coordinator ownership and concurrency tests");
    lib_storage_maintenance_test_step.dependOn(&run_lib_storage_maintenance_tests.step);

    const api_connections_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/api_connections_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, api_connections_test_mod, true, true);
    api_connections_test_mod.addImport("antfly_openapi_specs", antfly_imports.embedded_openapi);
    const lib_api_connections_tests = b.addTest(.{
        .root_module = api_connections_test_mod,
        .filters = &.{
            "object probe cache identity covers every bucket and credential source",
            "connection filesystem probes use the explicit filesystem authority",
            "connection cache remains valid across every allocation failure",
            "build response exposes embedded inference as a local connection",
            "inference connection operations are allowlisted",
            "inference admission ownership uses explicit connection identity",
            "inference connection URLs require an HTTP origin",
            "build response reports mock connected and types filter",
            "build response reports configured external io connections",
            "build response reports configured web search connections",
            "build response includes cdc replication sources with generic cdc kind",
            "include param parsing",
        },
        .test_runner = .{
            .path = b.path("pkg/antfly-embedded/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lib_api_connections_tests = addFilteredTestRunArtifact(b, lib_api_connections_tests);
    const lib_api_connections_test_step = b.step("antfly-api-connections-test", "Run connection inventory and probe-admission tests");
    lib_api_connections_test_step.dependOn(&run_lib_api_connections_tests.step);

    const api_storage_authority_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/api_storage_authority_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, api_storage_authority_test_mod, true, true);
    api_storage_authority_test_mod.addImport("antfly_openapi_specs", antfly_imports.embedded_openapi);
    const lib_api_storage_authority_tests = b.addTest(.{
        .name = @import("test_owner_names.zig").api_storage_authority,
        .max_rss = @import("runtime_memory.zig").testCompileMaxRss(@import("test_owner_names.zig").api_storage_authority, b.graph.host.result, target.result, optimize, @as(usize, if (target.result.os.tag == .macos) 10 else 7) * 1024 * 1024 * 1024),
        .root_module = api_storage_authority_test_mod,
        .filters = selectTestFilters(b, &.{
            "portable backup manifest rejects partial per-shard accepted generation proof",
            "public table backup and restore require named connections",
            "public table backup handler rejects an existing backup id",
            "public table backup handler exposes non-retryable fenced outcomes",
            "backup fence headers require a complete canonical fence",
            "fenced backup forwarding treats post-send transport failure as ambiguous",
            "cluster backup APIs require named connections",
            "cluster backup vtable preserves request context and canceled ingress stops before parsing",
            "cluster backup format defaults portable and preserves explicit native",
            "cluster backup format one-table adapter keeps artifact IDs isolated",
            "cluster backup and restore reject duplicate table selectors",
            "backup API requests reject unknown operational fields",
            "backup manifest round trips through metadata path",
            "current Go portable metadata envelope materializes into a verified Zig manifest",
            "current Go portable metadata parsing is allocation failure safe",
            "current Go portable cluster envelope resolves table metadata ids",
            "restore admission validates the stable newest current Go attempt",
            "current Go table metadata identity matches the producer derivation",
            "current Go attempt parser is strict in one pass",
            "current Go migration head is strict",
            "current Go migration head selects only the digest-pinned marker",
            "current Go migration head rejects marker digest mismatch",
            "portable artifact availability rejects empty and non-regular local payloads",
            "remote uri canonicalizes object prefix separators",
            "remote backup metadata reads are size bounded",
            "remote backup key joins canonicalize only the prefix boundary",
            "cluster backup list uses top-level remote manifests without recursing into payloads",
            "incomplete cluster backup attempts do not hide committed backups",
            "cluster backup list defers artifact validation to restore admission",
            "cluster backup list canonicalizes trailing prefix through s3 protocol",
            "cluster backup list canonicalizes trailing remote prefix slash",
            "remote backup reservations fence duplicate execution and can be released after cleanup",
            "cluster repository reservation serializes distinct backup ids and owners",
            "attempt head ordering ignores producer wall clocks and journal scans",
            "attempt head generation detects publication and retirement ABA",
            "newest attempt exact verification detects corruption and receipts revalidate identity",
            "unpublished remote cleanup preserves a conflicting manifest",
            "forwarded backup envelope retirement preserves canonical payload",
            "table backup reservation durably binds logical and artifact ids",
            "table backup writer lease fences cleanup until the storage owner expires",
            "standalone table backup stale reclamation fences delayed writers",
            "standalone table backup stale reclamation preserves committed manifests and legacy missing leases",
            "cluster writer lease reclamation persists bounded scan progress",
            "table backup cleanup removes the forwarded artifact envelope before payload",
            "cluster backup retains its fenced attempt after an ambiguous table outcome",
            "table backup retry preserves the retained ambiguous generation",
            "table backup retry reclaims an eligible reservation and admits the new generation",
            "cluster backup attempt markers reject overlapping cleanup identities",
            "stale owned cluster backup attempt retains generation fences and retires authoritative head",
            "expired recovery preserves an oversized remote commit record",
            "cluster backup reservation heartbeat fences premature and stale recovery",
            "filesystem cluster backup lease supports the maximum owner identity",
            "filesystem stale attempt reclamation index prevents directory-order starvation",
            "filesystem completed attempt tickets are deleted instead of durably rotated",
            "filesystem attempt publication tolerates concurrent bounded maintenance",
            "filesystem attempt maintenance removes only stale staged tickets",
            "filesystem reclaim removes invalid POSIX tickets containing backslashes",
            "local reclaim stays bound to its opened repository after a root swap",
            "filesystem stale attempt reclamation recovers an abandoned claim",
            "remote stale attempt reclamation cursor prevents prefix starvation",
            "legacy quarantine equivalence rejects a borrowed marker digest",
            "stale cluster backup attempt preserves aggregate referenced artifacts",
            "filesystem backup listing is bounded and cursor stable",
            "native backup directory copy preserves nested files",
            "native artifact verification consistently rejects empty directories",
            "native verification receipt rejects membership changes after exact pass",
            "remote portable file transfer uses objectstore file paths",
            "remote backup directory download paginates and enforces segment prefix",
            "api http server lists cluster backups through public route",
            "cluster backup maintenance queue retains distinct locations with bounded deduplication",
            "cluster backup maintenance queue rotates repositories without allocation",
            "cluster backup maintenance queue expires inactive repositories",
            "restore repository contention backoff is bounded and increasing",
            "restore retry deadline wakeup is interruptible without polling",
            "owned backup runtime has a finite worker ceiling",
            "remote backup connection retains network io instead of filesystem io",
            "dynamic gcs credentials borrow distinct network and filesystem authorities",
            "dynamic s3 profile and web identity retain explicit credential authorities",
            "backup staging uses configured storage authority and exclusive generations",
            "remote backup staging keeps native filesystem io separate from repository transport",
            "owned restore verifies declared artifact identity instead of accepting staged bytes",
            "cluster restore repository errors preserve operational failure semantics",
        }),
        .test_runner = .{
            .path = b.path("pkg/antfly-embedded/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lib_api_storage_authority_tests = addFilteredTestRunArtifact(b, lib_api_storage_authority_tests);
    const lib_api_storage_authority_test_step = b.step("antfly-api-storage-authority-test", "Run remote backup credential-boundary tests");
    lib_api_storage_authority_test_step.dependOn(&run_lib_api_storage_authority_tests.step);

    const api_session_maintenance_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/api_session_maintenance_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, api_session_maintenance_test_mod, true, true);
    api_session_maintenance_test_mod.addImport("antfly_openapi_specs", antfly_imports.embedded_openapi);
    const retained_quota_tests = b.addTest(.{
        .root_module = api_session_maintenance_test_mod,
        .filters = &.{ "retained quota", "api http server routes table batches through the batch commit hook", "api http client preserves public batch retry safety classifications", "internal routed batch preserves typed validation across the HTTP forwarding hop", "typed routed batch preserves forwarding cancellation and identity conflicts", "httpx antfly reads preserve availability and terminal failures" },
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    b.step("antfly-api-retained-quota-test", "Run retained-effects pressure outcome and retry contracts").dependOn(&addFilteredTestRunArtifact(b, retained_quota_tests).step);
    const lib_api_session_maintenance_tests = b.addTest(.{
        .root_module = api_session_maintenance_test_mod,
        .filters = &.{
            "SQL prepared completion",
            "httpx stable transaction commit durably hands off recovery before acknowledgement",
            "durable session mutations publish only after persistence succeeds",
            "durable SQL LOCAL DEFAULT preserves committed setting across restart and savepoint",
            "durable transaction sessions retain terminal commit coordinator handoff",
            "repair-required transaction sessions replay propagation once then release coordination",
            "committed repair session without coordinator replays a write handoff",
            "terminal commit response preserves live debt ahead of repair",
            "transaction session commit request is sealed across retries",
            "durable recovery index tracks only validated commit execution and terminal handoff",
            "durable recovery scan rotates fairly beyond one maintenance batch",
            "in-memory recovery scan rotates fairly when the first page remains pending",
            "background recovery adopts an expired shared-store owner lease",
            "api http server retries stable terminal commits without replaying writes",
            "api session maintenance recovers crash window after durable 2pc commit",
            "api session maintenance skips live commit execution and acknowledgement",
            "session maintenance activation follows current range leadership without follower RPCs",
            "api http server serves status using control metadata without diagnostic transfers",
            "transaction commit execution yields through caller io and preserves ownership on cancellation",
            "transaction session registry adopts durable session ownership",
            "transaction session registry only adopts durable sessions after lease expiry",
            "transaction session registry reports status and cleans expired durable sessions",
            "transaction session registry enforces savepoint limits and reports remaining capacity",
            "transaction session registry can renew owned leases opportunistically",
            "api http server keeps session maintenance off public request paths",
            "api http server can renew owned session leases via explicit maintenance hook",
            "api http server keeps session maintenance off internal request paths",
        },
        .test_runner = .{
            .path = b.path("pkg/antfly-embedded/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lib_api_session_maintenance_tests = addFilteredTestRunArtifact(b, lib_api_session_maintenance_tests);
    const lib_api_session_maintenance_test_step = b.step("antfly-api-session-maintenance-test", "Run durable session concurrency and background-maintenance tests");
    lib_api_session_maintenance_test_step.dependOn(&run_lib_api_session_maintenance_tests.step);

    const lib_api_docid_tests = b.addTest(.{
        .name = "api-docid-tests",
        .max_rss = @import("runtime_memory.zig").testCompileMaxRss("api-docid-tests", b.graph.host.result, target.result, optimize, if (target.result.os.tag == .macos) 12 * 1024 * 1024 * 1024 else 0),
        .root_module = antfly_test_mod,
        .filters = &.{
            "api table reads reject stale doc identity before multigroup fanout",
            "distributed table reads reject stale doc identity before multigroup fanout",
            "api public table query rejects only top-level internal fields",
            "single embeddings index encoder scopes isolated enrichment failure to one index",
            "empty embeddings index status is ready without dense artifact visibility",
            "index encoders report missing and stale topology groups without probing databases",
            "api query contract public parser rejects internal shard doc identity controls",
            "api distributed graph hydrate carries identity generation and clears cross-range ordinals",
            "distributed graph rejects doc identity rebuild before cross-range fanout",
            "distributed graph rejects unstamped result refs before cross-range fanout",
            "api distributed graph preserves per-shard snapshots across result refs expansion and hydration",
            "graph hydrate response wire flattens index identity",
            "distributed graph edge reader routes outgoing and fans out incoming adjacency",
            "distributed graph edges response round trips owned edges",
            "query merge preserves common identity read generation",
            "query merge applies distributed typed sort ordering and cursor paging",
            "match_all index sort uses doc values collector for selective native filters",
            "text index sort uses doc values collector for selective term filters",
            "query merge applies runtime schema to distributed date cursors",
            "query merge rejects sorted shards without complete sort tuples",
            "query merge rejects sorted shards whose id tiebreaker mismatches hit id",
            "query merge rejects sorted shards with mixed sort value domains",
            "query merge rejects score ordered hits without finite scores",
            "query merge orders non score bearing hits by id without requiring scores",
            "query parser records approximate source diagnostic for semantic exact sort",
            "query parser rejects semantic cursor-only pagination as approximate source",
            "query parser rejects semantic search_before pagination as approximate source",
            "query parser rejects semantic score sort as approximate source",
            "query encoder does not expose internal doc ordinals",
            "query builder preflight validates score sort source",
            "query builder preflight validates cursor values against mapped sort field types",
            "query builder preflight reports sort tuple shape diagnostics directly",
            "api query contract serializes sort profile diagnostics",
            "api query contract maps public exact sort rejection diagnostics",
            "api query contract serializes ordered hit sort tuple",
            "api query contract rejects ordered hits without complete sort tuple",
            "api query contract rejects ordered hits with non replayable sort tuple",
            "api query contract defaults cursor pagination without sort to id order",
            "api query contract preflight rejects cursor pagination without sort when cursor is not id arity",
            "create table parser rejects schemas that cannot derive runtime mappings",
            "metadata.schema update rejects schemas that cannot derive runtime mappings",
            "api query contract preflight rejects cursor pagination over approximate vector source",
            "api query contract preflight rejects search_before pagination over approximate vector source",
            "api query contract preflight rejects score sort over approximate vector source",
            "api query contract preflight rejects score sort without score-bearing source",
            "api query contract appends stable id sort tiebreaker for cursors",
            "api query contract rejects cursor width that omits stable id tiebreaker",
            "api query contract records cursor arity diagnostic without sort",
            "api query contract rejects non replayable search_after cursor values",
            "api query contract rejects non replayable search_before cursor values",
            "api query contract rejects ambiguous explicit id sort tiebreaker",
            "graph edge local read rejects stale identity generation",
            "catalog doc identity readiness checks table range health",
            "catalog resolved filter validation accepts preserved split identity domains",
            "metadata merge request validation rejects incompatible doc identity namespaces",
            "metadata merge validation handles rolling mixed-version doc identity status fixtures",
            "metadata split request validation rejects stale doc identity namespace",
            "metadata reconciler does not automatically split ordinal exhausted doc identity",
            "metadata state classifies mixed-version doc identity lifecycle reports",
            "metadata state marks doc identity rebuild required on range namespace mismatch",
            "metadata http server rejects split and merge during active doc identity reassignment before source mutation",
            "metadata http server replaces a table definition through compare-and-swap",
            "metadata http server serves status and filtered admin routes",
            "metadata http server maps source split merge doc identity conflicts",
            "metadata http client preserves split merge doc identity conflicts",
            "metadata http client parses legacy range records without doc identity fields",
            "metadata http client round-trips range doc identity fields",
            "metadata http client round-trips server endpoints",
            "table workflow doc identity guards reject active transition intents",
            "table workflow doc identity lifecycle handles mixed-version transition status",
            "metadata reconciler doc identity guards block new planning during active reassignment",
            "metadata reconciler does not upsert desired split with stale doc identity namespace",
            "metadata reconciler allows explicit merge with doc identity reassignment opt-in",
            "replay batcher tuple map keys preserve embedded delimiters",
            "db chunk cache keys preserve embedded separators",
            "enrichment worker chunk cache keys preserve embedded separators",
            "search request text stats keys preserve embedded separators",
            "merge distributed background text stats keys preserve embedded separators",
            "significant_terms local background stats use postings without stored index source",
            "graph edge local read rejects stale identity namespace",
            "dense metadata keys preserve embedded index separators",
            "dense metadata lookups read legacy textual rows",
            "distributed txn participant ids preserve embedded group markers",
            "distributed join unmatched worker pages group-local right hits",
            "distributed join follow-up pagination requires stamped identity request",
            "distributed join group-local hit pagination reuses structured search generation",
            "distributed right join unmatched tracking uses ordinal identity keys",
            "distributed join unmatched worker prefers local search results over query envelopes",
            "distributed join rejects doc identity rebuild before right-table fanout",
            "distributed join stateful shuffle rejects doc identity rebuild before worker dispatch",
            "internal worker doc identity exchange audit covers every boundary",
            "typed internal HTTP errors preserve conflict semantics",
            "internal transaction HTTP responses prove not-proposed only before decision",
            "internal transaction ingress establishes and validates pre-decision deadline",
            "api http client preserves group doc identity conflicts",
            "aggregation context rejects non-current identity generation",
            "aggregation full-result capture survives a later primary write",
            "aggregation selection and rerun share a read lease without repeating the raft barrier",
            "local query provider returns complete aggregations and preserves the requested hit page",
            "aggregation domain",
            "aggregation full-result rerun counts text matches and each vector window without changing ranked page",
            "provisioned single-group queries retain coordinator-owned finalization",
            "provisioned query delegates single-group physical execution to local read source",
            "aggregation full-result rerun can reuse snapped result identity generation",
            "explicit text stats requests preserve identity generation",
            "explicit text stats requests carry resolved doc filters and apply exact projection",
            "explicit text stats requests reject stale identity generation",
            "algebraic partial request fails closed when lifecycle is stale",
            "algebraic partial request accepts current identity generation and rejects stale",
            "provisioned distributed aggregations collect path terms nested cardinality",
            "algebraic distributed planner selects identity-stamped derived join tensor program",
            "algebraic derived join tensor reads subtract identity tombstones at generation",
            "planner rejects rebuild-required schema lifecycle state",
            "algebraic adaptive progress marks rebuild required on schema drift",
            "db vector symbolic filters fail closed when algebraic lifecycle is stale",
            "remote simple vector query uses vector worker route",
            "encode query request serializes internal resolved doc filters with wire context",
            "simple vector shard request carries serializable resolved doc filter",
            "api http server preserves public query availability errors",
            "api http server maps retrieval agent doc identity mismatch to unavailable",
            "api http server query builder maps doc identity mismatch to unavailable",
            "api http server surfaces structured doc identity conflicts for transaction commits",
            "distributed graph expand request preserves algebraic semiring planning flag",
            "batch identity metadata delete observes buffered resurrection state",
            "identity validation accepts missing canonical rows but rejects conflicts",
            "identity allocation rejects canonical row conflicts before reserving ordinal",
            "batch identity metadata fails closed at ordinal capacity",
            "identity namespace reassignment preserves snapshot generations and rejects stale writers",
            "near-u32 ordinal pressure preserves sparse high ordinal state through reassignment",
            "db stats flag document identity ordinal capacity exhaustion",
            "db stats expose document identity coverage and tombstones",
            "db allocates final document ordinal with all index families present",
            "db lsm primary compaction preserves doc identity ordinals",
            "db rejects new document writes at ordinal exhaustion for every sync level",
            "db transaction intent writes reject new documents at ordinal exhaustion",
            "db restore snapshot rejects invalid doc identity metadata",
            "db deferred restore rejects strict doc identity namespace mismatch",
            "db explicit restore runtime repair repairs managed chunked dense embeddings once for restored shard",
            "db incomplete deferred restore import recovers before runtime repair",
            "export and import preserves doc identity metadata",
            "import rejects doc identity metadata with invalid canonical ids",
            "import rejects doc identity namespace mismatch unless preserving existing namespace",
            "db resolved doc-set projection honors identity read generation",
            "db doc set planning stats record ordinal bitmap promotion",
            "db search requests default to current identity generation snapshot",
            "db validates internal resolved doc filter wire namespace and generation",
            "db explicit doc-id filter resolution honors identity generation",
            "doc filter wire round-trips ordinal and doc-key filters",
            "doc filter wire rejects old required-field fixtures but tolerates additive fields",
            "doc filter wire rejects invalid ordinal fixtures from mixed-version senders",
            "dense vector id ignores ordinal metadata for a different doc",
            "dense metadata prefetch includes legacy ordinal vector ids",
            "db dense index stores stable vector ids with ordinal filter mappings",
            "db dense artifact rebuild preserves stable vector ids distinct from ordinals",
            "db sparse index uses identity ordinals as physical doc nums for primary docs",
            "db sparse hits resolve doc ordinals through identity not sparse doc nums",
            "native dense constraints fail closed without ordinal vector mapping",
            "native constraints fail closed when resolved ordinals cannot be represented",
            "native sparse constraints fail closed without ordinal doc num mapper",
            "native sparse constraints map resolved ordinals to physical doc nums",
            "match_all candidate ordinal lookup uses identity read generation",
            "match_all consumes resolved ordinal filters without doc id projection",
            "native constraints pass identity generation to doc-set id projection",
            "native constraints pass identity read generation to live doc filtering",
            "native constraints treat resolved all-doc exclusion as empty candidates",
            "native sparse constraints keep explicit doc ids when identity coverage is incomplete",
            "text resolved doc filter projection passes identity generation to live filtering",
            "text native constraints fall back for mixed ordinal sidecar coverage",
            "text native constraints fail closed when resolved ordinals cannot be projected",
            "text native constraints treat resolved all-doc exclusion as empty candidates",
            "sort cursor contract classifies arity separately from type",
            "segment doc ordinal sidecar roundtrip and merge preserve live order",
            "segment index sort metadata roundtrip",
            "segment merge drops index sort metadata until physical sort is preserved",
            "segment sorted merge normalizes legacy mixed numeric index sort domains",
            "segment sorted merge preserves index sort and remaps doc addressed sections",
            "segment sorted merge rejects non-finite f64 index sort values",
            "match_all sorted segment seek merges sorted segments and applies cursors",
            "match_all sorted segment seek uses cursor seek within each segment",
            "match_all sorted segment seek enforces scan budget",
            "match_all sorted segment seek checks deadline while scanning",
            "match_all sorted segment seek zero limit returns profile without scanning",
            "match_all sorted segment seek rejects cursor when segment bounds are unavailable",
            "match_all projected source load rejects expired deadline before batch load",
            "dense projected source load rejects expired deadline before load",
            "match_all unordered source loads selected hits through projected batch",
            "text projected source load rejects expired deadline before stored load",
            "text projected source batch preserves selection and cleans up failed hydration",
            "native sort zero limit avoids generic collector decoration",
            "text doc values sort zero limit avoids budget and decoration",
            "match_all native candidate sort zero limit avoids decoration",
            "match_all native ordinal doc values zero limit avoids budget and decoration",
            "match_all native stream sort zero limit counts without decoration",
            "match_all id seek zero limit exposes internal sort profile when sampled",
            "match_all id seek zero limit respects cursor bounds exactly",
            "match_all native candidate sort applies cursor before admission",
            "document mapper flushes schema index_sort segments in physical sort order",
            "document mapper validates schema index_sort field capabilities",
            "merge preserves common sorted segment index_sort metadata",
            "db text compaction preserves ordinal filters across reopen",
            "structured filter doc set cache returns owned clones",
            "structured filter doc set cache separates shared namespace generation keys",
            "cache invalidates ownership move prefix without reviving pinned generations",
            "applyGraphUnion deduplicates by ordinals when hit pages are complete",
            "applyGraphIntersection uses ordinals when hit pages are complete",
            "query merge preserves single-result doc ordinals",
            "fuseNamedSets deduplicates aliases by ordinal when complete",
            "graph result_ref fails closed when unbounded resolved doc-set cannot project",
            "graph result_ref uses complete node doc-set when hits are paged",
            "graph query result doc-set resolution receives identity generation",
            "provisioned direct read db opens reject stale identity namespace",
            "provisioned query runtime db rejects stale identity namespace",
        },
        .test_runner = .{
            .path = b.path("pkg/antfly-embedded/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const api_transactions_docid_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/api_transactions_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, api_transactions_docid_test_mod, true, true);
    api_transactions_docid_test_mod.addImport("antfly_openapi_specs", antfly_imports.embedded_openapi);
    const api_table_writes_docid_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/api_table_writes_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configureConsumer(b, api_table_writes_docid_test_mod);
    api_table_writes_docid_test_mod.addImport("antfly_openapi_specs", antfly_imports.embedded_openapi);
    const write_implementation_module = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/api_table_writes_implementation_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, write_implementation_module, true, true);
    write_implementation_module.addImport("antfly_openapi_specs", antfly_imports.embedded_openapi);
    const api_table_reads_docid_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/api_table_reads_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configureConsumer(b, api_table_reads_docid_test_mod);
    api_table_reads_docid_test_mod.addImport("antfly_openapi_specs", antfly_imports.embedded_openapi);
    const retained_capture_admission_tests = b.addTest(.{
        .name = "retained-capture-admission-tests",
        .root_module = api_table_reads_docid_test_mod,
        .filters = &.{ "retained read registry", "retained read owner", "retained read RPC", "retained read client" },
        .max_rss = 6 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    const retained_capture_admission_run = b.addRunArtifact(retained_capture_admission_tests);
    retained_capture_admission_run.addArg("--require-no-skips");
    b.step("antfly-retained-capture-admission-test", "Verify fenced RPC owner admission, capability retirement and transport lifetimes")
        .dependOn(&retained_capture_admission_run.step);

    const retained_worker_parent_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/retained_worker_parent_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configureConsumer(b, retained_worker_parent_mod);
    retained_worker_parent_mod.addImport("antfly_openapi_specs", antfly_imports.embedded_openapi);
    const retained_worker_parent_tests = b.addTest(.{
        .name = "retained-worker-parent-tests",
        .root_module = retained_worker_parent_mod,
        .filters = &.{ "internal query owner retained receiver", "retained read owner worker parent", "internal query owner trusted dispatch" },
        .max_rss = 3 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    const retained_worker_parent_run = b.addRunArtifact(retained_worker_parent_tests);
    retained_worker_parent_run.addArg("--require-no-skips");
    b.step("antfly-retained-worker-parent-test", "Verify receiver tokens pin admitted worker, query heap and runnable through physical close")
        .dependOn(&retained_worker_parent_run.step);

    const route_admission_tests = b.addTest(.{
        .name = "route-admission-tests",
        .root_module = api_table_reads_docid_test_mod,
        .filters = &.{"metadata route admission"},
        .max_rss = 3 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    b.step("antfly-route-admission-test", "Verify original execution through fresh route and metadata runtime wait admission")
        .dependOn(&addFilteredTestRunArtifact(b, route_admission_tests).step);

    const statement_admission_tests = b.addTest(.{
        .name = "statement-admission-tests",
        .root_module = api_table_reads_docid_test_mod,
        .filters = &.{ "relational statement retries unavailable capture and releases fences before pages or deadline", "relational row query full-key index proof requires every routed owner", "dynamic statement admission" },
        .max_rss = 6 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    b.step("antfly-statement-admission-test", "Verify original execution across statement capture, retry, open and proof retrieval")
        .dependOn(&addFilteredTestRunArtifact(b, statement_admission_tests).step);

    const api_public_table_http_docid_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/api_public_table_http_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, api_public_table_http_docid_test_mod, true, true);
    api_public_table_http_docid_test_mod.addImport("antfly_openapi_specs", antfly_imports.embedded_openapi);
    const raft_transition_runtime_docid_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/raft_transition_runtime_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, raft_transition_runtime_docid_test_mod, true, true);
    raft_transition_runtime_docid_test_mod.addImport("antfly_openapi_specs", antfly_imports.embedded_openapi);
    const api_transactions_docid_tests = b.addTest(.{
        .root_module = api_transactions_docid_test_mod,
        .filters = &.{
            "transaction request parsers reject invalid unsigned integers and accept legacy epochs",
            "transaction read snapshot map keys preserve embedded delimiters",
            "transaction session commit response includes retry hints for doc identity availability conflicts",
            "hosted participant attempt deadline preserves the server outcome window",
            "hosted participant rediscovery retries only pre-decision leader unavailability",
            "distributed txn coordinator aborts only participants that may have begun",
            "DistributedEntitySink deletes an old pinned copy while moving a key",
            "DistributedEntitySink live replay preserves curator redirects and alias union",
            "DistributedEntitySink upserts a merge transform per entity",
            "DistributedEntitySink overwrites the redirect for a merged tombstone",
            "DistributedEntitySink records a cross-table redirect",
            "DistributedEntitySink atomic promotion batch prefers stateless batch commit",
            "DistributedEntitySink commits a re-key across pinned physical tables atomically",
            "DistributedEntitySink batch commit remains compatible with transaction-only sources",
            "DistributedEntitySink atomic mode fails closed when unsupported",
            "api http client preserves retryable group transaction unavailability",
            "internal transaction operations preserve pre-decision leader unavailability",
        },
        .test_runner = .{
            .path = b.path("pkg/antfly-embedded/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const api_table_writes_docid_tests = @import("linked_tests.zig").add(b, .{
        .name = "api-table-write-tests",
        .root_module = api_table_writes_docid_test_mod,
        .filters = selectTestFilters(b, &.{
            "auto bulk group writes release leases so idle finish can publish",
            "provisioned table write source has a finite worker ceiling",
            "provisioned native storage metrics bypass an empty busy write cache",
            "provisioned table write source rejects stale doc identity namespace before write",
            "writer identity resolution rejects stale eventual routes",
            "replicated split destination seeds inherited doc identity before range publication",
            "internal batch parser rejects mixed split transition commands",
            "internal batch parser requires source acknowledgements to be metadata-only",
            "internal batch codec preserves timestamps and rejects public injection",
            "internal batch topology control preserves binary fences and refuses public injection",
            "internal batch split identity round trips the full u64 id space",
            "internal batch codec round trips replicated transaction phases",
            "txn resolve codec preserves sync level and accepts legacy requests",
            "distributed txn classifies local and transported visibility outcomes identically",
            "distributed txn coordinator groups by range and commits all participants",
            "stable distributed transaction retry resumes a durable commit decision",
            "distributed txn retries an ambiguous coordinator decision under the same id",
            "distributed txn bounds unresolved coordinator decision retries",
            "distributed txn propagates one absolute deadline through ambiguous decision recovery",
            "distributed txn participant fanout is bounded and concurrent",
            "distributed txn acknowledgement windows batch only proven followers and preserve recovery debt",
            "hosted participant bulk acknowledgements use typed transport and only definite legacy fallback",
            "distributed txn bulk acknowledgement codec owns bounded participant identities and restore authority",
            "distributed txn abort fanout preserves durable ordering contact evidence and bounded recovery",
            "distributed txn coordinator never aborts after durable commit decision",
            "distributed txn coordinator never restarts a transaction id on topology change",
            "db transaction recovery runtime resolves table-group participants through distributed txn resolver",
            "compiled table write boundary transports cancellation and committed failure identity",
            "compiled table write boundary forwards retained execution teams without legacy fallback",
            "pre-decision context deadline has typed admission provenance",
            "bound stable single-group transaction retry does not reapply transforms",
            "bound single-group batch reports prepared intent conflicts",
            "provisioned predicate-only batch validates matching and stale versions",
            "provisioned single-group commit preserves transaction graph transform contract",
            "raft single-group fast path excludes graph projection transforms only",
            "resident writer repair state distinguishes clean and metadata-pending writers",
            "api http client preserves group doc identity conflicts",
            "api http client transports txn resolve cancellation and visibility reason",
            "resolve group routes uses one router-owned snapshot callback for fanout",
            "system catalog parallel hosted candidate fanout sends only owned keys",
            "api http client preserves public batch retry safety classifications",
            "api http client forwards bounded raft batch routing context without allocation",
            "api http client preserves committed visibility outcomes for forwarded raft batches",
            "api http client rejects unsupported routed batch protocol without legacy replay",
            "api http client requires explicit not-proposed marker and tracks delivery phase",
            "online merge catalog rejection requires a not-proposed receipt before cancellation",
            "raft batch aggregation makes failures after an accepted group non-retryable",
            "prepared raft apply reclassifies every transient pre-mutation writer conflict",
            "stateless batch retries are bounded and exclude explicit OCC",
            "provisioned stateless batch retries definite aborts to the production bound",
            "bound table write source backs up and restores a local table",
            "bound table write source backs up and restores a portable local table",
            "provisioned table write source backs up and restores a local table",
            "provisioned table write source backs up a portable local table",
            "provisioned table write source backs up and restores a local table",
            "provisioned native backup restore repeats through shared read and write owners",
            "provisioned table restore rejects multi-range manifests before opening storage",
            "provisioned table restore rejects mismatched doc identity namespace",
            "provisioned table write source path invalidation clears shared vector read cache",
            "provisioned table restore retry repairs exact incomplete restore state through active writer",
            "provisioned restore repair source deinit cancels sleeping retry worker",
            "provisioned restore repair worker retries transient step failures to completion",
            "provisioned table write source restore repair completion retires cached vector read state",
            "provisioned restore repair open rejects stale doc identity namespace",
            "write cache blocks same-root generation replacement while stale lease stays live",
            "provisioned transition writer fences exact supplied table metadata",
            "provisioned create index enqueues target-fenced cached-writer activation",
            "write cache metadata refresh preserves inactive adoptable seed",
            "write cache adopts active just-created db across generation bump",
            "write cache local mutation reuses live stale-generation writer",
            "write cache structural local mutation finishes auto bulk before reuse",
            "write cache local mutation preempts stale startup writer",
            "hosted runtime status reads owner snapshot without inspecting live writer",
            "HA seed preflight drains writer released after promotion cache clear before capture freeze",
            "HA seed cache transition original lifetime unwinds every acquired fence",
            "runtime status collection leaves active stale write lease live",
            "resident DB lease adopts seeded write cache across visible generation bump",
            "provisioned write cache close detaches promotion leadership callback before stats",
            "provisioned table write source coalesces same-group waiters",
            "provisioned table write coalescer hands off after owner completes",
            "provisioned table write source preserves same-key delete then write across coalesced waiters",
            "provisioned table write coalescer isolates invalid waiter on same-key overlap",
            "provisioned table write coalescer isolates failed waiters",
            "provisioned table write source consistent visibility hook does not block on busy apply lock",
            "provisioned table write source consistent visibility refreshes stale dense status",
            "provisioned table write source visibility hook defers status sampling to runtime owner",
            "provisioned table write source status visibility does not invalidate read cache",
            "provisioned table write source metrics serve cached snapshot while write cache lock is busy",
            "provisioned table restore lifecycle reserves forwarded owner and caller sources",
            "provisioned startup catch-up enters through forwarded write owner",
            "provisioned runtime status inspection remains available during structural transition",
            "provisioned read admission enters through forwarded write owner",
            "provisioned table write source drop table waits for active read cache lease",
            "provisioned table write source drop table closes schema-bearing cached writer once",
            "provisioned table write source backup releases read cache exclusive before native snapshot copy",
            "live managed repair leaves resident replay and status reads nonblocking",
            "managed startup catch-up open constructs bounded enrichment runtime without workers",
            "provisioned group storage wires remote content to writer caches",
            "startup runtime status snapshot publishes live db when active cache is empty",
            "best effort startup runtime status publishes live db when cache is empty",
            "idle startup runtime status publish is live when startup flag is still set",
            "idle startup completion cannot downgrade a superseding live index status",
            "managed startup catch-up quarantines repeated zero progress with bounded backoff",
            "managed startup catch-up uses provided indexes json without catalog fetch",
            "managed startup catch-up marks FileNotFound index open terminal degraded",
            "managed startup catch-up preserves restore repair debt while index load is terminal",
            "managed startup catch-up defers while shared bulk ingest state is active",
            "clean generated startup inspection does not retain a resident writer",
            "table runtime snapshot cache clones stored status",
            "table runtime snapshot cache rejects a late stale live observation",
            "table runtime snapshot cache replacement preserves a newer live observation",
            "structural reconcile reconfigures retained writer before managed dense writes",
            "provisioned managed replay tails converge and publish without later traffic",
            "provisioned owner publication advances exact index replay target",
            "provisioned owner publication fills cold dense visibility",
            "provisioned owner publication clears ambiguous replay-only backfill",
            "provisioned owner publication replaces stale cached backfill",
            "managed source status-only open drains stale pending close before retry",
            "hosted status-only open drains stale pending close before retry",
            "write cache HA gate clear drains inactive pending closes before returning",
            "write cache retires shared HA generation stale entries before reuse",
        }),
        .test_runner = .{
            .path = b.path("pkg/antfly-embedded/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const api_table_reads_linked_tests = @import("linked_tests.zig").add(b, .{
        .name = "api-table-read-tests",
        .root_module = api_table_reads_docid_test_mod,
        .filters = &.{
            "retained read owner",
            "retained read RPC",
            "retained read client",
            "api http client preserves public batch retry safety classifications",
            "relational row query response budget",
            "relational statement retries unavailable capture and releases fences before pages or deadline",
            "relational row query full-key index proof requires every routed owner",
            "partially applied two-owner decision cannot enter retained SQL cut",
            "relational row query executes typed projection",
            "owner-local delayed statement scans",
            "relational row query retained owner holds admission and releases failed opens",
            "table reads translate request deadlines into the routing clock",
            "api query contract graph evaluation preserves base hit paging and shard windows",
            "distributed reranking widens retrieval and stays coordinator owned",
            "reranker candidate and output windows have distinct bounds",
            "reranker paging preserves the underlying retrieval total",
            "coordinator prunes the final score domain before paging",
            "profiled composed dense query preserves exact route telemetry",
            "aggregation completeness requires exact total relation",
            "aggregation-only collection preserves controls and clears only internal hits",
            "aggregation full-result rerun includes newly published text documents at the same identity generation",
            "aggregation full-result rerun preserves the graph reranked hit page",
            "aggregation context rejects non-current identity generation",
            "aggregation full-result capture survives a later primary write",
            "aggregation selection and rerun share a read lease without repeating the raft barrier",
            "local query provider returns complete aggregations and preserves the requested hit page",
            "aggregation domain",
            "aggregation full-result rerun counts text matches and each vector window without changing ranked page",
            "provisioned single-group queries retain coordinator-owned finalization",
            "provisioned query delegates single-group physical execution to local read source",
            "aggregation text analysis selects the named full text index",
            "collect significant terms field requests gathers unique field terms from hits",
            "distributed grouped hierarchy expands only the globally merged page",
            "distributed grouped unit expansion rejects a cross-revision result",
            "distributed grouped unit expansion rejects a missing selected group",
            "distributed query shard request preserves sorted cursor contract",
            "distributed unit grouping round trips deferred shard hydration",
            "distributed unit group hydration routes selected units and deduplicates sources",
            "distributed unit hydration preserves exclusion-only projections",
            "distributed unit group hydration rejects a cross-revision unit payload",
            "identity-only distributed unit groups consume envelopes without routed reads",
            "hosted distributed grouped hierarchy expands the globally selected shard page",
            "hosted hierarchy navigation routes projection-safe hydration and advances cursors",
            "query merge treats hierarchy navigation positions as opaque cursor values",
            "query merge treats conflicting hierarchy navigation plans as retryable",
            "query merge treats malformed hierarchy navigation shard tuples as retryable",
            "query merge releases hierarchy navigation candidates once at the global budget",
            "query merge globally coalesces hierarchy unit groups and bounded chunks",
            "query merge treats conflicting hierarchy unit identities as retryable",
            "query merge treats malformed hierarchy unit shard ranking as retryable",
            "query merge reports an honest lower bound for a partial hierarchy unit union",
            "query merge rejects exact sorting for hierarchy unit groups",
            "query merge bounds hierarchy unit selection by page instead of shard fanout",
            "encode query request preserves hierarchy unit navigation contract",
            "hierarchy navigation hydration validates the planned unit fingerprint",
            "hosted hierarchy navigation routes projection-safe hydration and advances cursors",
            "routed internal reads require an explicit peer fence acknowledgement",
            "generation handoff receipt local forwarding certifies the strict read index before admission",
            "routing sessions reserve authoritative snapshots for cross-table plans",
            "encode query request preserves unit grouping ancestor projections",
            "parseRemoteSearchResult",
            "remote query returns the shard-selected identity generation",
            "remote simple vector query uses vector worker route",
            "remote scan fails closed without streaming and honors cancellation before transport",
            "remote scan prefers one snapshot-stable streaming request",
            "simple vector shard request lowers to vector worker envelope",
            "api http client forwards internal query controls and maps remote timeout",
            "api http client encodes lookup route and query components",
            "api http client preserves remote storage read contention",
            "api http client preserves stale hierarchy cursor conflicts",
            "api http client preserves storage read contention across group read endpoints",
            "typed internal group reads preserve retryable resident storage failures",
            "typed routed batch preserves forwarding cancellation and identity conflicts",
            "typed internal query workers preserve identity generation validation",
            "remote shard query phases propagate deadline and request cancellation",
            "provisioned table read cache has a finite worker ceiling",
            "provisioned read cache invalidates repeated ownership moves with pinned leases",
            "provisioned read cache exclusive access drains active read leases",
            "provisioned read cache group exclusive drains only the published group",
            "provisioned storage inspection uses table read admission",
            "provisioned distributed aggregations collect path terms nested cardinality",
            "distributed significant terms candidates use configured analyzers and bounded memory",
            "api query contract serializes derived hierarchy ancestry",
            "api query contract preserves the internal grouped unit revision envelope",
            "api query contract serializes hydrated unit ancestor for direct unit hits",
            "table read distributed sorted merge uses catalog runtime schema and rejects incomplete shard windows",
            "provisioned standby read gate permits stale reads and routes non-stale reads to primary",
            "provisioned local query reuses resident generation without readonly open",
            "provisioned auxiliary reads publish resident databases outside read admission",
            "provisioned graph hydrate completes consistency before resident read admission",
            "provisioned consistency read reroutes after topology changes before admission",
            "route-pinned catalog prevents a stale admin namespace from replacing routing identity",
            "routing session validates a pinned selection against current topology",
            "routing topology epoch fences identity-only changes",
            "authoritative write routing pins keys and identity in one compact snapshot",
            "catalog route fence dispatch is strict and fail closed",
            "provisioned stale read admits before routing without a redundant catalog validation",
            "distributed graph source read rejects topology change before aggregation",
            "hosted cross-range graph query expands explicit local start keys",
            "provisioned reads reject a group removed from the table topology",
            "provisioned table read source rejects uncertified point reads and permits explicit stale reads",
            "catalog backed router skips non-serving relocation placements",
            "resolve group routes uses one router-owned snapshot callback for fanout",
            "system catalog parallel hosted candidate fanout sends only owned keys",
        },
        .test_runner = .{
            .path = b.path("pkg/antfly-embedded/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const api_public_table_http_docid_tests = b.addTest(.{
        .root_module = api_public_table_http_docid_test_mod,
        .filters = &.{
            "public table batch handler maps doc identity unavailable errors",
            "public table batch handler maps write unavailable errors",
            "public table batch mandatory responses reserve before native acceptance and survive allocator exhaustion",
            "public table batch handler exposes pending HA durability without claiming rollback",
            "public table batch handler preserves ambiguous write outcomes",
            "public table batch handler maps HA write gate errors",
            "public table batch handler returns concise dense repair backpressure",
            "public table api carries borrowed cancellation into batch execution",
            "public create index exposes retryable storage descriptor exhaustion",
            "public create index exposes unsupported deployment capability",
            "public create index returns normalized created resource",
            "public table query handler maps doc identity unavailable errors",
            "public table query reports the selected reranker candidate ceiling",
            "public table query handler maps exact graph execution failures",
            "graph path weight error body fails closed without its diagnostic",
            "public table query handler preserves structured filter and hierarchy diagnostics",
            "public table query handler preserves retryable failure status",
            "public table query handler maps HA read gate errors",
            "public table query handler rejects unknown sort tuple properties before dispatch",
            "public table query handler maps candidate budget exhaustion",
            "public table query handler maps unsupported exact sort",
            "public table query handler exposes stable count-only sort rejection reason",
            "public table query handler surfaces exact sort rejection diagnostics",
            "public table query view handler maps doc identity unavailable errors",
            "public table backup handler accepts portable format",
            "public table backup handler exposes non-retryable fenced outcomes",
            "public table restore handler maps unsupported multi-range error",
            "public table restore handler reports artifact integrity failures",
            "public table restore handler reports committed durability pending",
            "public table restore handler reports confirmed durability",
            "public table query view handler maps HA read gate errors",
            "public document artifact manifest handlers map HA read gate errors",
            "public document artifact manifest handler returns summary and raw state",
            "public document artifact reprocess handler returns accepted",
        },
        .test_runner = .{
            .path = b.path("pkg/antfly-embedded/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const raft_transition_runtime_docid_tests = b.addTest(.{
        .root_module = raft_transition_runtime_docid_test_mod,
        .filters = &.{
            "transition runtime fails closed when doc identity reassignment callback is missing",
        },
        .test_runner = .{
            .path = b.path("pkg/antfly-embedded/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const lib_serverless_docid_runtime_filters = &.{
        "serverless query module compiles",
        "search plan rejects internal doc identity controls",
        "serverless graph plans reject internal doc identity controls",
    };
    const lib_serverless_docid_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = compileFiltersWithAnchors(b, &.{"serverless module compiles"}, lib_serverless_docid_runtime_filters),
        .test_runner = .{
            .path = b.path("pkg/antfly-embedded/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lib_api_docid_tests = addFilteredTestRunArtifact(b, lib_api_docid_tests);
    b.step("antfly-api-docid-test", "Run public API document identity and restore contracts").dependOn(&run_lib_api_docid_tests.step);
    const lib_api_graph_snapshot_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = compileFiltersWithAnchors(b, &.{"api module compiles"}, &.{
            "api distributed graph preserves per-shard snapshots across result refs expansion and hydration",
            "distributed graph edge reader routes outgoing and fans out incoming adjacency",
            "distributed graph edges response round trips owned edges",
            "distributed graph retries once on topology change and succeeds",
            "distributed K path identity preserves same type fact ids",
            "distributed graph identity hashing",
            "distributed weighted fact paths",
            "distributed graph expand request preserves algebraic semiring planning flag",
            "canonical relationship predicates",
            "projected graph endpoint routing recognizes every source form",
            "distributed graph edge reader finds fact owners outside the source endpoint shard",
            "distributed graph edges response round trips owned edges",
            "internal batch parser owns and round trips graph mutations",
            "internal batch graph endpoint cleanup command",
            "graph cleanup owner replay afterimages",
            "Yen scratch reservations fail before allocation and release exactly",
            "graph workers report retired ranges as topology unavailability",
            "distributed graph incoming probe expands only positive source shards",
            "graph hydrate incoming probe wire carries pinned clock and physical allowance",
            "incoming graph route cache is exact and generation fenced",
            "incoming graph route durable hint coalescer is byte bounded",
            "incoming graph route durable writes leave the query path",
            "incoming graph route durable persistence retries without request traffic",
            "incoming graph route durable failures retry boundedly and retain accepted hints",
            "incoming graph route directory survives cache restart and replaces stale fences",
            "distributed graph per-key authoritative incoming routes avoid shard probes",
            "distributed graph root probe retires resolved keys between shard waves",
            "distributed graph supports cross-range traverse target selectors",
            "distributed graph traverse routes cross-table frontier by table generation",
            "distributed graph metrics reject personalized reads before transport",
            "distributed path queries reject metric clauses before path search",
            "legacy graph expansion wire omits new controls and remains budget fenced",
            "graph expand remote retries legacy wire only with a full scan ceiling",
            "legacy graph hydrate wire omits metric fields in both directions",
            "legacy graph edges request requires the full physical scan ceiling",
            "legacy graph wire admits direct indexes and fences contributor indexes",
            "graph hydrate and edges remote retry legacy wire safely",
            "distributed metric postprocessing enforces the complete candidate ceiling",
        }),
        .test_runner = .{
            .path = b.path("pkg/antfly-embedded/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lib_api_graph_snapshot_tests = addFilteredTestRunArtifact(b, lib_api_graph_snapshot_tests);
    const lib_api_graph_snapshot_test_step = b.step("antfly-api-graph-snapshot-test", "Run distributed graph snapshot-vector regression tests");
    lib_api_graph_snapshot_test_step.dependOn(&run_lib_api_graph_snapshot_tests.step);
    const lib_api_graph_wire_runtime_filters = &.{"graph hydrate response wire flattens index identity"};
    const lib_api_graph_wire_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = compileFiltersWithAnchors(b, &.{"api module compiles"}, lib_api_graph_wire_runtime_filters),
        .test_runner = .{
            .path = b.path("pkg/antfly-embedded/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lib_api_graph_wire_tests = addFilteredTestRunArtifactWithRuntimeFilters(
        b,
        lib_api_graph_wire_tests,
        lib_api_graph_wire_runtime_filters,
    );
    const lib_api_graph_wire_test_step = b.step("lib-api-graph-wire-test", "Run canonical internal graph wire-contract regressions");
    lib_api_graph_wire_test_step.dependOn(&run_lib_api_graph_wire_tests.step);
    const lib_api_distributed_query_availability_runtime_filters = &.{
        "distributed query transport failures become one retryable availability condition",
        "remote lookup transport failures preserve read availability without retrying",
    };
    const lib_api_distributed_query_availability_tests = @import("linked_tests.zig").add(b, .{
        .name = "api-distributed-query-availability-tests",
        .root_module = api_table_reads_docid_test_mod,
        .filters = lib_api_distributed_query_availability_runtime_filters,
        .test_runner = .{
            .path = b.path("pkg/antfly-embedded/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lib_api_distributed_query_availability_tests = lib_api_distributed_query_availability_tests.run(b);
    const lib_api_distributed_query_availability_test_step = b.step(
        "lib-api-distributed-query-availability-test",
        "Run retryable distributed-query transport classification regressions",
    );
    lib_api_distributed_query_availability_test_step.dependOn(&run_lib_api_distributed_query_availability_tests.step);
    const api_internal_read_availability_tests = @import("linked_tests.zig").add(b, .{
        .name = "api-internal-read-availability-tests",
        .root_module = api_table_reads_docid_test_mod,
        .filters = &.{
            "typed internal group reads preserve retryable resident storage failures",
            "api http client preserves remote storage read contention",
        },
        .test_runner = .{
            .path = b.path("pkg/antfly-embedded/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    b.step("antfly-api-internal-read-availability-test", "Run internal scan leadership availability and wire classification regressions").dependOn(&api_internal_read_availability_tests.run(b).step);
    const api_internal_routed_batch_tests = @import("linked_tests.zig").add(b, .{
        .name = "api-internal-routed-batch-tests",
        .root_module = api_table_reads_docid_test_mod,
        .filters = &.{"typed routed batch preserves forwarding cancellation and identity conflicts"},
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    b.step("antfly-api-internal-routed-batch-test", "Run exact private batch routing and rollback authority regressions").dependOn(&api_internal_routed_batch_tests.run(b).step);
    const api_internal_batch_outcome_tests = @import("linked_tests.zig").add(b, .{
        .name = "api-internal-batch-outcome-tests",
        .root_module = api_table_writes_docid_test_mod,
        .filters = &.{"api http client requires explicit not-proposed marker and tracks delivery phase"},
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    b.step("antfly-api-internal-batch-outcome-test", "Run forwarded batch outcome and admission classification regressions")
        .dependOn(&api_internal_batch_outcome_tests.run(b).step);
    const api_aborted_prepare_unavailability_tests = b.addTest(.{
        .root_module = api_table_writes_docid_test_mod,
        .filters = &.{"distributed txn coordinator aborts only participants that may have begun"},
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    b.step("antfly-api-aborted-prepare-unavailability-test", "Run durable-abort retryability classification regression")
        .dependOn(&addFilteredTestRunArtifact(b, api_aborted_prepare_unavailability_tests).step);
    const api_derived_coverage_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/api_derived_coverage_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, api_derived_coverage_test_mod, true, true);
    api_derived_coverage_test_mod.addImport("antfly_openapi_specs", antfly_imports.embedded_openapi);
    const status_paging_cache_tests = b.addTest(.{
        .name = "status-refresh-paging-cache-tests",
        .root_module = api_derived_coverage_test_mod,
        .filters = &.{
            "runtime status paged catalog",
            "group read payload preparation and retirement stay off the global cache mutex",
            "runtime status cache stable absence removal retires the old table epoch",
            "table runtime snapshot cache live publication does not starve structural refresh",
            "runtime status group batches reject duplicate group ids before publication",
        },
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
        .max_rss = 7 * 1024 * 1024 * 1024,
    });
    const status_paging_cache_run = addFilteredTestRunArtifact(b, status_paging_cache_tests);
    status_paging_cache_run.has_side_effects = true;
    b.step("antfly-api-status-refresh-paging-test", "Verify premetadata publication fences and independent catalog pruning")
        .dependOn(&status_paging_cache_run.step);
    const lib_api_derived_coverage_tests = b.addTest(.{
        .name = "api-derived-coverage-tests",
        .root_module = api_derived_coverage_test_mod,
        .max_rss = @import("runtime_memory.zig").testCompileMaxRss("api-derived-coverage-tests", b.graph.host.result, target.result, optimize, @as(usize, if (target.result.os.tag == .macos) 13 else 7) * 1024 * 1024 * 1024),
        .filters = &.{
            "live repair admission supersedes cached vector serviceability",
            // Own the complete status contract suite; table-write integration
            // borrows these contracts without collecting them a second time.
            "api.runtime_status.consumerTests.Suite.test.",
            "coverage policy accepts only the public embeddings contract",
            "index configs receive persistent private incarnations across index kinds",
            "create table parser preserves supported metadata fields",
            "create table rejects caller-managed schema versions",
            "metadata.schema update versions only the active primary full-text index with a fresh incarnation",
            "restore manifest preserves trusted coverage incarnation metadata",
            "public index config encoders redact coverage incarnation",
            "public index config encoders redact nested credentials",
            "public index config encoders preserve enrichment objects on read",
            "public index config encoders omit root write-only producer documents",
            "created index configs normalize single-source input forms",
            "table contract rejects unknown fields for every public index variant",
            "identical index mutation retries preserve coverage incarnation",
            "derived coverage evaluation is policy exact and observation gated",
            "settled terminal enrichment debt is degraded rather than rebuilding",
            "derived coverage source totals ignore derived index fan out",
            "dense publication target requires every expected shard observation",
            "derived coverage aggregation rejects mixed config observations",
            "derived coverage embedding activity aggregation is order independent and phase authoritative",
            "derived coverage ready full text status reports complete progress",
            "derived coverage artifact projections use source readiness without vector counters",
            "readiness observation completion requires convergence and full topology",
            "late source target notification cannot revoke an already observed target",
            "late exact index notification preserves completed observation and reduction authority",
            "late source notification cannot reuse an observation from before a catalog fence",
            "readiness evaluation cannot complete while convergence work remains",
            "readiness completion fences include every observation dimension",
            "chunked dense completion follows the physical publication target",
            "runtime status best effort overlay cannot clear readiness under apply contention",
            "db source commit publishes exact target observation sequence",
            "db generated downstream indexes are exact convergence targets at source commit",
            "db chunked dense enrichment skips unchanged chunks and deletes stale chunk artifacts",
            "db chunk deletion replay targets only matching vector sources",
            "db chunk retirement removes dense and sparse members from generated and artifact projections",
            "db reopened chunked dense HBC deletes stale vectors through artifact loader",
            "embedding artifact replay deletes only the consuming vector projection",
            "collectManagedSyncTargets includes graph index for graph artifact journal changes",
            "visibility targets include graph replay blocked by a missing dependency",
            "index status exposes compact repair state without internal diagnostics",
            "index status aggregation preserves actionable repair diagnostics for the requested incarnation",
            "rebuild quarantine remains an explicit failed public index status",
            "index repair aggregation exposes a waiting shard over rebuilding shards",
            "derived coverage aggregation rejects stale index incarnations",
            "derived coverage reasons expose counter mismatch",
            "derived coverage rejects unknown freshness for aggregate and shard views",
            "cached all-skipped coverage observation is a runtime fact",
            "live writer artifact regression keeps authoritative source deletions",
            "same owner stale serving revision cannot regress an exact incarnation",
            "owner replacement cannot regress serving facts at the same accepted source target",
            "owner replacement cannot regress serving facts but a new non-vector incarnation may reset them",
            "irrelevant broad replay cursor cannot regress exact index serving or coverage facts",
            "exact additive target advance preserves serving facts while delete authority permits reduction",
            "publication stamps order revisions and require recovery across owners",
            "db vector status revalidates stale repair admission without a query",
            "owner publication stamps order all index kinds independently of counts and callbacks",
            "source coverage observation is independent of artifact replay progress",
            "retained source coverage is fenced after every publication merge",
            "target and reducing watermarks commute across every callback permutation",
            "runtime status group batches reject duplicate group ids before publication",
            "table runtime snapshot cache clones stored status",
            "projection continuity preserves lifecycle classification as one bundle",
            "table runtime snapshot cache summarizes replay debt",
            "table runtime snapshot cache batch publication is table epoch atomic",
            "table runtime snapshot cache publication fence preserves the last snapshot",
            "targeted publication fence preserves only untouched siblings during catch up",
            "new targeted transition supersedes delayed controls from an older owner",
            "targeted catch up hands off same incarnation serving authority",
            "target authority settles only after every group acknowledges the exact incarnation",
            "target authority uses bound catalog groups instead of cached group count",
            "resident serving acknowledgement hands off a separate coordinator cache",
            "failed exact handoff accepts identity without fabricating serving authority",
            "targeted publication rejects a completed stale incarnation until structural acknowledgement",
            "settled target authority preserves the accepted incarnation against late publishers",
            "accepted authority requires identity containment not equal cardinality",
            "accepted group identity survives late predecessor before global handoff",
            "catalog identity rejects a wrong first structural observation",
            "exact target advances fence only their index incarnation",
            "exact target advance before first status remains index scoped",
            "exact target advance allocation failure remains fail closed",
            "batched target advance stops after fallback invalidates cached state",
            "per-index delta merge stays bounded across many accepted authorities",
            "bulk target handoff reduces per-group acknowledgements linearly",
            "incremental group acknowledgement sync is linear with one global handoff",
            "multi-group delta allocation failure is atomic and leak free",
            "multi-group commit uses preflight group capacity without allocation",
            "prepared index delta commit performs no allocation",
            "targeted authority binding is monotonic under reversed publication order",
            "wholly stale targeted publication cannot bind unknown authority",
            "targeted deletion hands off only after authoritative absence",
            "synthetic refresh cannot outrank targeted structural owner observation",
            "background refresh completes exact multi-group target handoff incrementally",
            "background refresh exact target allocation failure is atomic and leak free",
            "background refresh completes absent multi-group target handoff incrementally",
            "synthetic refresh preserves post-fence target facts before serving handoff",
            "table runtime snapshot cache lifecycle transition replaces and fences observations",
            "table runtime snapshot cache batch preserves newer group observations",
            "consistent boundary publication supersedes later-reserved stale observation",
            "table runtime snapshot cache missed refresh retains counts without renewing freshness",
            "runtime status cache stable absence removal retires the old table epoch",
            "runtime status snapshots never wait for mutable cache ownership",
            "group read payload preparation and retirement stay off the global cache mutex",
            "blocked read payload preparation does not convoy an unrelated table",
            "blocked table refresh preparation does not convoy unrelated publication",
            "activation failure projection preserves every stable failure code",
            "terminal activation failure precedes and binds its exact catalog row",
            "terminal drop failure never acknowledges absence and remains actionable",
            "terminal failure result distinguishes supersession and reserved storage",
            "target invalidation retires only its table read view",
            "lifecycle mirror failure cannot expose the retired generation",
            "partial coverage embeddings readiness counts skipped source units",
            "partial coverage embeddings readiness does not mask pending enrichment",
            "complete partial embeddings coverage is ready after active generation proof",
            "runnable repair owns its load error without becoming a terminal aggregate failure",
            "actionable repair remains visible while retained generation stays queryable",
            "serviceable full text replacement remains queryable while rebuilding",
            "progressive embeddings readiness exposes a queryable partial generation",
            "missing target observation preserves serving snapshot and blocks only completion",
            "stale in-place status preserves an incarnation-scoped serviceability proof",
            "identity-proven embeddings stay current during sibling startup catch-up",
            "opening embeddings observation requires explicit serviceability authority",
            "cached owner observation preserves serving authority without convergence authority",
            "index-local convergence fence does not revoke a completed sibling",
            "accepted target observation survives late snapshots but not new commit fences",
            "single group synthetic publication preserves owner runtime authority",
            "target-scoped stale full text observation cannot publish old readiness",
            "targeted full text sibling remains authoritative during table catch up",
            "managed embedder preserves atomic publication policy",
            "derived coverage reasons deduplicate overlapping freshness signals",
            "managed embeddings readiness ignores finalizing catch-up after rate-limit recovery",
            "single embeddings index encoder keeps retrying coverage gaps catch-up coherent",
            "single embeddings index encoder scopes isolated enrichment failure to one index",
            "published embeddings snapshot remains queryable after isolated source failure",
            "multi-source embedding enrichments receive a shared semantic producer identity",
            "transcriber enrichment shorthand expands into a document extraction producer",
            "asset enrichment may consume another asset artifact",
            "source readiness isolates terminal enrichment failures",
            "source readiness distinguishes durable repair debt from runtime enrichment failure",
            "managed embeddings skipped terminal sources complete backfill without fabricating replay debt",
            "repair-free embeddings aggregate retains live dense catch-up",
            "serviceable repair preserves sibling shard dense catch-up fallback",
            "serviceable repair cannot mask sibling shard load failure",
            "index encoders preserve sibling replay debt during serviceable repair",
            "enrichment aggregation preserves telemetry and fences mixed checkpoint identity",
            "table storage status indexes one distributed snapshot by table and owner",
            "distributed join uses row estimates when byte statistics are unavailable",
            "distributed join preserves no-stat strategy when one side is unknown",
            "api http server join planner uses complete fresh local stats before metadata publication",
            "external embeddings index readiness does not require table doc coverage",
            "api http server preserves public query availability errors",
            "api http maps missing physical index only for rebuilding lifecycle",
            "api http classifies catalog-to-serving index convergence without runtime status",
            "remote rebuild quarantine preserves its source and index failure",
            "api http server create index installs exact visible config and defers lagging projection",
            "api http server create index expands schema-derived algebraic config",
            "hosted index lifecycle callback hands exact target to persistent control plane",
            "committed activation receipt rejects delayed drop after newer create",
            "committed activation receipt rejects delayed create after newer drop and coalesces exact duplicate",
            "committed activation watermark stays bounded across completed scope churn",
            "index activation client preserves progress and transport classifications",
            "resident activation journal rejects a delayed lower metadata epoch",
            "resident activation requeue cannot displace a newer queued epoch",
            "activation scheduler key includes complete storage identity",
            "whole table reconcile cannot absorb exact resident activation",
            "active structural key coalesces duplicate and requeues allocation free",
            "all structural workers retain allocation-free requeue capacity during admission bursts",
            "structural scheduler burst uses keyed coalescing and FIFO deadlines",
            "structural scheduler capacity includes active ownership",
            "yielded structural work advances under sustained new admission",
            "exact activation dual worker admission failure leaves no orphan",
            "duplicate activation cannot observe accepted before admission commits",
            "completed activation ring evicts oldest and records completion once",
            "discard retires unreferenced scope revisions under churn",
            "newer completed activation makes retained older result stale",
            "discarded newer activation cannot resurrect an older terminal result",
            "activation progress preserves every terminal failure class",
            "hosted index lifecycle owner retries transient activation failure",
            "structural activation dispatch does not convoy unrelated groups",
            "hosted index lifecycle backs off repeated stale resident observation",
            "hosted activation revalidates topology after final resident RPC",
            "hosted index lifecycle durable wake repairs worker admission failure",
            "structural reconcile retains ordered constant-time repair wake membership across plan resets",
            "startup configured indexes separate physical indexes from reserved metadata",
        },
        .test_runner = .{
            .path = b.path("pkg/antfly-embedded/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lib_api_derived_coverage_tests = addFilteredTestRunArtifact(b, lib_api_derived_coverage_tests);
    run_lib_api_derived_coverage_tests.step.dependOn(&openapi_root_check.step);
    const lib_api_derived_coverage_test_step = b.step("antfly-api-derived-coverage-test", "Run focused public derived coverage tests");
    lib_api_derived_coverage_test_step.dependOn(&run_lib_api_derived_coverage_tests.step);
    const run_lib_serverless_docid_tests = addFilteredTestRunArtifactWithRuntimeFilters(
        b,
        lib_serverless_docid_tests,
        lib_serverless_docid_runtime_filters,
    );
    const run_api_transactions_docid_tests = addFilteredTestRunArtifact(b, api_transactions_docid_tests);
    b.step("antfly-api-transaction-commit-test", "Run routed transaction commit and entity promotion contracts").dependOn(&run_api_transactions_docid_tests.step);
    const write_implementation_tests = b.addTest(.{
        .name = "api-table-write-implementation-tests",
        .root_module = write_implementation_module,
        .filters = &.{"api.table_reads.implementationTests."},
        .max_rss = @import("runtime_memory.zig").testCompileMaxRss("api-table-write-implementation-tests", b.graph.host.result, target.result, optimize, @import("runtime_memory.zig").routedImplementationCompileMaxRss(.{
            .host = b.graph.host.result,
            .target = target.result,
            .optimize = optimize,
            .strip = write_implementation_module.strip orelse false,
            .cpu_inference = false,
            .sanitize_thread = write_implementation_module.sanitize_thread orelse false,
        })),
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    const run_api_table_writes_docid_tests = @import("linked_tests.zig").runPair(b, api_table_writes_docid_tests, write_implementation_tests);
    const decision_recovery_run = b.addRunArtifact(api_table_writes_docid_tests.executable);
    @import("../../../build_support/antfly/test_support.zig").configureTestRun(decision_recovery_run);
    decision_recovery_run.addArgs(&.{ "--require-no-skips", "--test-filter", "stable distributed transaction retry resumes a durable commit decision", "--test-filter", "distributed txn retries an ambiguous coordinator decision under the same id", "--test-filter", "distributed txn bounds unresolved coordinator decision retries", "--test-filter", "distributed txn propagates one absolute deadline through ambiguous decision recovery" });
    b.step("antfly-decision-recovery-completion-test", "Verify finite same-ID coordinator recovery after ambiguous submission")
        .dependOn(&decision_recovery_run.step);
    const run_api_table_reads_docid_tests = @import("linked_tests.zig").runPair(b, api_table_reads_linked_tests, write_implementation_tests);
    b.step("antfly-api-table-read-test", "Run table-read routing and internal group contracts").dependOn(&run_api_table_reads_docid_tests.step);
    const statement_capture_retry_run = b.addRunArtifact(api_table_reads_linked_tests.executable);
    @import("../../../build_support/antfly/test_support.zig").configureTestRun(statement_capture_retry_run);
    statement_capture_retry_run.addArgs(&.{ "--test-filter", "relational statement retries unavailable capture and releases fences before pages or deadline" });
    b.step("antfly-api-statement-capture-retry-test", "Run bounded SQL statement capture retry regression")
        .dependOn(&statement_capture_retry_run.step);
    const api_aggregation_tests = @import("linked_tests.zig").addPair(b, .{
        .name = "api-aggregation-tests",
        .root_module = api_table_reads_docid_test_mod,
        .filters = &.{ "aggregation-only collection", "aggregation completeness", "aggregation context", "aggregation full-result rerun", "aggregation domain", "encode vector-only query does not create a match-all text component", "api query contract keeps filter carriers out of vector fusion" },
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    }, write_implementation_tests);
    b.step("antfly-api-aggregation-test", "Run captured aggregation collection, completeness and generation regressions").dependOn(&api_aggregation_tests.run(b).step);
    const rewrite_admission_contract_tests = b.addTest(.{
        .root_module = api_transactions_docid_test_mod,
        .filters = &.{ "distributed txn rewrite admission", "relational integrity restore staging rewrite intent" },
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    b.step("antfly-rewrite-admission-contract-test", "Verify scoped rewrite admission and durable intent binding")
        .dependOn(&addFilteredTestRunArtifact(b, rewrite_admission_contract_tests).step);
    const row_semantics_codec_tests = b.addTest(.{
        .root_module = api_transactions_docid_test_mod,
        .filters = &.{ "distributed txn prepare preserves JSON null", "distributed txn prepare JSON null", "internal batch JSON null", "distributed txn range guard wire" },
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    b.step("antfly-row-semantics-codec-test", "Verify fail-closed row semantics envelopes and allocation failure cleanup")
        .dependOn(&addFilteredTestRunArtifact(b, row_semantics_codec_tests).step);
    const session_publication_tests = b.addTest(.{
        .name = "durable-session-publication-tests",
        .root_module = api_transactions_docid_test_mod,
        .filters = &.{ "transaction session", "durable session", "durable transaction sessions", "SQL session" },
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    b.step("antfly-durable-session-publication-test", "Verify pending durable BEGIN publication capacity across every registry insertion")
        .dependOn(&addFilteredTestRunArtifact(b, session_publication_tests).step);
    const api_transaction_contract_tests = b.addTest(.{
        .root_module = api_transactions_docid_test_mod,
        .filters = &.{ "compiled table write boundary", "distributed txn", "hosted participant", "stable distributed transaction retry", "internal batch parser owns binary staged restore controls", "merge page internal codec", "online merge private", "durable SQL session rejects duplicate savepoint ids" },
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    b.step("antfly-api-transactions-test", "Run transaction coordinator and participant contracts").dependOn(&addFilteredTestRunArtifact(b, api_transaction_contract_tests).step);
    const participant_fanout_control_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/participant_fanout_control_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configureConsumer(b, participant_fanout_control_root);
    participant_fanout_control_root.addImport("antfly_source_root", participant_fanout_control_root);
    participant_fanout_control_root.addImport("antfly_openapi_specs", antfly_imports.embedded_openapi);
    const participant_fanout_control_tests = b.addTest(.{
        .name = "participant-fanout-control-tests",
        .root_module = participant_fanout_control_root,
        .filters = &.{
            "distributed txn independently scheduled",
            "group admission wait",
            "hosted participant bulk acknowledgements use typed transport and only definite legacy fallback",
            "distributed txn retries an ambiguous coordinator decision under the same id",
            "distributed txn propagates one absolute deadline through ambiguous decision recovery",
        },
        .max_rss = 3 * 1024 * 1024 * 1024,
    });
    b.step("antfly-participant-fanout-control-test", "Verify independent participant scopes, CPU-free physical joins and original completion authority")
        .dependOn(&b.addRunArtifact(participant_fanout_control_tests).step);
    const relational_source_contract_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/relational_source_contract_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configureConsumer(b, relational_source_contract_root);
    relational_source_contract_root.addImport("antfly_source_root", relational_source_contract_root);
    relational_source_contract_root.addImport("antfly_openapi_specs", antfly_imports.embedded_openapi);
    const relational_source_contract_tests = b.addTest(.{
        .name = "relational-source-contract-tests",
        .root_module = relational_source_contract_root,
        .filters = &.{"distributed txn"},
        .max_rss = 3 * 1024 * 1024 * 1024,
    });
    b.step("antfly-relational-source-contract-test", "Verify relational activation and recovery consumer contracts without importing a physical DB")
        .dependOn(&b.addRunArtifact(relational_source_contract_tests).step);
    const local_commit_control_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/api_local_commit_control_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, local_commit_control_root, true, true);
    local_commit_control_root.addImport("antfly_openapi_specs", antfly_imports.embedded_openapi);
    const local_commit_control_tests = b.addTest(.{
        .name = "local-commit-control-tests",
        .root_module = local_commit_control_root,
        .filters = &.{ "bound batch startup recovery preserves original lifetime and pending durability", "bound commit controls reject expired admission and durably abort canceled preparation", "shared stateless commit controls retain the original deadline through conflict retries", "bound batch controls retain original owner through native snapshot admission", "bound batch controls retain CPU through snapshot-owned apply rejection", "bound BEGIN and prepare retain CPU through policy-owned apply rejection", "bound batch controls reject non-lending replication capture contention and retry the same root", "bound batch predecision pressure rejects blocked producers and preserves soft admission", "bound batch startup lends original CPU before acquiring policy ownership", "bound batch startup pending records survive retryable rejection and reopen", "bound transaction original lifetime rejection after BEGIN durably aborts", "bound resolution original lifetime rejection preserves prepared intents for retry" },
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    const snapshot_predecision_tests = b.addTest(.{
        .name = "snapshot-predecision-batch-tests",
        .root_module = local_commit_control_root,
        .filters = &.{ "bound batch controls retain original owner through native snapshot admission", "bound batch controls retain CPU through snapshot-owned apply rejection", "bound BEGIN and prepare retain CPU through policy-owned apply rejection", "bound batch controls reject non-lending replication capture contention and retry the same root", "bound batch predecision pressure rejects blocked producers and preserves soft admission", "bound batch startup recovery preserves original lifetime and pending durability", "bound batch startup lends original CPU before acquiring policy ownership", "bound batch startup pending records survive retryable rejection and reopen" },
        .max_rss = 12 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    const snapshot_predecision_run = addFilteredTestRunArtifact(b, snapshot_predecision_tests);
    snapshot_predecision_run.addArg("--require-no-skips");
    b.step("antfly-snapshot-predecision-batch-test", "Verify physical pre-decision startup/pressure/replication/snapshot/apply admission and durable retries")
        .dependOn(&snapshot_predecision_run.step);

    const local_commit_control_run = addFilteredTestRunArtifact(b, local_commit_control_tests);
    local_commit_control_run.addArg("--require-no-skips");
    b.step("antfly-local-commit-control-test", "Require physical bound commit cancellation cleanup and original stateless budgets")
        .dependOn(&local_commit_control_run.step);

    const transaction_capture_tests = b.addTest(.{
        .name = "transaction-capture-budget-tests",
        .root_module = local_commit_control_root,
        .filters = &.{ "distributed txn routing budget rejects legacy capture and releases expired captures", "distributed txn routing releases catalog ownership on missing topology and rejected admission", "transaction routing capture retains original payload policy wait budget and releases reader admission" },
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    const transaction_capture_run = addFilteredTestRunArtifact(b, transaction_capture_tests);
    transaction_capture_run.addArg("--require-no-skips");
    b.step("antfly-transaction-capture-budget-test", "Require original transaction routing controls and capture cleanup")
        .dependOn(&transaction_capture_run.step);

    const mutation_service_admission_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/mutation_service_admission_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, mutation_service_admission_root, true, true);
    mutation_service_admission_root.addImport("antfly_source_root", mutation_service_admission_root);
    const mutation_service_admission_tests = b.addTest(.{
        .name = "mutation-service-admission-tests",
        .root_module = mutation_service_admission_root,
        .filters = &.{"db controlled mutation admission"},
        .max_rss = @as(usize, if (target.result.os.tag == .macos) 19 else 7) * 1024 * 1024 * 1024,
    });
    b.step("antfly-mutation-service-admission-test", "Verify original mutation admission rejects before effects and releases real DB fences")
        .dependOn(&b.addRunArtifact(mutation_service_admission_tests).step);

    const mutation_service_semantic = b.addTest(.{
        .name = "mutation-service-admission-check",
        .root_module = mutation_service_admission_root,
        .filters = &.{"db controlled mutation admission"},
        .max_rss = 3 * 1024 * 1024 * 1024,
    });
    b.step("antfly-mutation-service-admission-check", "Type-check physical controlled DB mutation and transaction bodies without emitting a binary")
        .dependOn(&mutation_service_semantic.step);

    const ha_table_request_admission_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/ha_table_request_admission_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configureConsumer(b, ha_table_request_admission_root);
    ha_table_request_admission_root.addImport("antfly_source_root", ha_table_request_admission_root);
    ha_table_request_admission_root.addImport("antfly_openapi_specs", antfly_imports.embedded_openapi);
    const ha_table_request_admission_tests = b.addTest(.{
        .name = "ha-table-request-admission-tests",
        .root_module = ha_table_request_admission_root,
        .filters = &.{ "HA controlled table request admission", "routed write admission", "controlled CPU reentry", "HA seed maintenance lifetime" },
        .max_rss = 3 * 1024 * 1024 * 1024,
    });
    b.step("antfly-ha-table-request-admission-test", "Verify actual HA table request drain and closed admission with C1 CPU progress")
        .dependOn(&b.addRunArtifact(ha_table_request_admission_tests).step);

    const group_batch_context_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/group_batch_context_check_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configureConsumer(b, group_batch_context_root);
    group_batch_context_root.addImport("antfly_source_root", group_batch_context_root);
    const group_batch_context_check = b.addTest(.{
        .name = "group-batch-context-check",
        .root_module = group_batch_context_root,
        .filters = &.{ "controlled group batch context hosted route", "controlled range tracking lifetime", "controlled range tracking context preserves" },
        .max_rss = 3 * 1024 * 1024 * 1024,
    });
    b.step("antfly-group-batch-context-check", "Analyze actual hosted control-only batch context forwarding without emitting a binary")
        .dependOn(&group_batch_context_check.step);

    const owner_preflight_context_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/owner_preflight_context_check_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, owner_preflight_context_root, true, true);
    owner_preflight_context_root.addImport("antfly_source_root", owner_preflight_context_root);
    const owner_preflight_capi_options = b.addOptions();
    owner_preflight_capi_options.addOption(bool, "linked_storage", false);
    owner_preflight_capi_options.addOption(bool, "inference_enabled", true);
    const owner_preflight_sources = owner_preflight_context_root.import_table.get("antfly_local_sources").?;
    owner_preflight_sources.addOptions("capi_build_options", owner_preflight_capi_options);
    owner_preflight_sources.addImport("antfly_storage_root", owner_preflight_context_root);
    const preparation_status_owner_check = b.addTest(.{
        .name = "preparation-status-owner-check",
        .root_module = owner_preflight_context_root,
        .filters = &.{"distributed txn typed native status"},
        .max_rss = 3 * 1024 * 1024 * 1024,
    });
    b.step("antfly-preparation-status-owner-check", "Analyze resident status leases and quorum admission without the full storage archive")
        .dependOn(&preparation_status_owner_check.step);
    const online_owner_queue_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/api/owner_preparation_queue.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    const online_owner_queue_tests = b.addTest(.{
        .root_module = online_owner_queue_root,
        .filters = &.{"online owner preparation queue"},
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    const online_owner_queue_run = b.addRunArtifact(online_owner_queue_tests);
    online_owner_queue_run.has_side_effects = true;
    online_owner_queue_run.addArg("--require-no-skips");
    b.step("antfly-online-owner-preparation-test", "Verify bounded online owner preparation metadata, expiry, coalescing and adaptive workspace")
        .dependOn(&online_owner_queue_run.step);
    const online_owner_source_tests = b.addTest(.{
        .root_module = owner_preflight_context_root,
        .filters = &.{"online owner preparation source"},
        .max_rss = 3 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    const online_owner_source_run = b.addRunArtifact(online_owner_source_tests);
    online_owner_source_run.has_side_effects = true;
    online_owner_source_run.addArg("--require-no-skips");
    b.step("antfly-online-owner-preparation-source-test", "Exercise actual source scheduling, pending admission and controlled descriptor confirmation")
        .dependOn(&online_owner_source_run.step);
    const request_execution_admission_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/request_execution_admission_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    const request_execution_admission_sources = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/source_catalog.zig"),
        .target = target,
        .optimize = optimize,
    });
    request_execution_admission_sources.addImport("antfly_local_test_sources", b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/api/runnable_inference_test_sources.zig"),
        .target = target,
        .optimize = optimize,
    }));
    request_execution_admission_sources.addImport("antfly_cancellation", antfly_imports.cancellation);
    request_execution_admission_sources.addImport("antfly_platform", antfly_imports.platform);
    request_execution_admission_sources.addImport("antfly_runtime_abi", antfly_imports.runtime_abi);
    request_execution_admission_sources.addImport("storage_engine_errors", antfly_imports.storage_boundary.engine_errors);
    request_execution_admission_sources.addImport("antfly_source_root", request_execution_admission_root);
    request_execution_admission_root.link_libc = true;
    request_execution_admission_root.addImport("antfly_runtime_abi", antfly_imports.runtime_abi);
    request_execution_admission_root.addImport("antfly_platform", antfly_imports.platform);
    request_execution_admission_root.addImport("antfly_local_sources", request_execution_admission_sources);
    request_execution_admission_root.addImport("antfly_source_root", request_execution_admission_root);
    const request_execution_archive_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/batch_visibility_archive_fixture.zig"),
        .target = target,
        .optimize = optimize,
    });
    request_execution_archive_root.link_libc = true;
    request_execution_archive_root.addImport("antfly_runtime_abi", antfly_imports.runtime_abi);
    request_execution_archive_root.addImport("antfly_cancellation", antfly_imports.cancellation);
    request_execution_archive_root.addImport("antfly_platform", antfly_imports.platform);
    request_execution_archive_root.addImport("antfly_source_root", request_execution_archive_root);
    const request_execution_archive = b.addLibrary(.{
        .name = "request-execution-admission-fixture",
        .linkage = .static,
        .root_module = request_execution_archive_root,
        .max_rss = 3 * 1024 * 1024 * 1024,
    });
    request_execution_admission_root.linkLibrary(request_execution_archive);
    const request_execution_admission_tests = b.addTest(.{
        .name = "request-execution-admission-tests",
        .root_module = request_execution_admission_root,
        .filters = &.{"controlled owner preflight"},
        .max_rss = 3 * 1024 * 1024 * 1024,
    });
    b.step("antfly-request-execution-admission-test", "Verify original executor, CPU and holder retirement through native visibility contracts")
        .dependOn(&b.addRunArtifact(request_execution_admission_tests).step);

    const request_execution_io_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/api/request_execution_io.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configureConsumer(b, request_execution_io_root);
    const request_execution_io_tests = b.addTest(.{ .root_module = request_execution_io_root });
    b.step("antfly-request-execution-io-test", "Verify borrowed execution IO without a per-request pool or clock fallback")
        .dependOn(&b.addRunArtifact(request_execution_io_tests).step);

    const native_begin_source_check = b.addTest(.{
        .root_module = owner_preflight_context_root,
        .filters = &.{ "controlled native BEGIN source", "controlled native completion source", "controlled native PREPARE source", "controlled native ACK source" },
        .max_rss = 3 * 1024 * 1024 * 1024,
    });
    b.step("antfly-native-begin-source-check", "Analyze controlled native BEGIN and completion owner acquisition")
        .dependOn(&native_begin_source_check.step);

    const owner_close_preflight_tests = b.addTest(.{
        .name = "owner-close-preflight-tests",
        .root_module = owner_preflight_context_root,
        .filters = &.{ "controlled owner close preflight C entry", "owned db reconciles published schema indexes", "db close retires runtime owners for memory primary backend", "controlled close status" },
        .max_rss = @import("runtime_memory.zig").testCompileMaxRss("owner-close-preflight-tests", b.graph.host.result, target.result, optimize, @as(usize, if (target.result.os.tag == .macos) 12 else 8) * 1024 * 1024 * 1024),
    });
    b.step("antfly-owner-close-preflight-test", "Exercise controlled close preflight with a real DB and original cleanup capsule")
        .dependOn(&b.addRunArtifact(owner_close_preflight_tests).step);

    const source_close_admission_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/source_close_admission_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, source_close_admission_root, true, true);
    source_close_admission_root.addImport("antfly_source_root", source_close_admission_root);
    const source_close_capi_options = b.addOptions();
    source_close_capi_options.addOption(bool, "linked_storage", false);
    source_close_capi_options.addOption(bool, "inference_enabled", true);
    const source_close_sources = source_close_admission_root.import_table.get("antfly_local_sources").?;
    source_close_sources.addOptions("capi_build_options", source_close_capi_options);
    source_close_sources.addImport("antfly_storage_root", source_close_admission_root);
    const source_close_admission_tests = b.addTest(.{
        .name = "source-close-admission-tests",
        .root_module = source_close_admission_root,
        .filters = &.{ "controlled owner source publication stop", "controlled owner reader cache close", "relational index system source publication job survives" },
        .max_rss = @import("runtime_memory.zig").testCompileMaxRss("source-close-admission-tests", b.graph.host.result, target.result, optimize, @as(usize, if (target.result.os.tag == .macos) 12 else 8) * 1024 * 1024 * 1024),
    });
    b.step("antfly-source-close-admission-test", "Verify exporter, polling and reader state across bounded cleanup waits")
        .dependOn(&b.addRunArtifact(source_close_admission_tests).step);

    const owner_control_funding_check = b.addTest(.{
        .name = "owner-control-funding-check",
        .root_module = owner_preflight_context_root,
        .filters = &.{ "committed owner apply never opens or closes storage under the raft apply lock", "funded owner control startup", "online owner preparation admission" },
        .max_rss = 3 * 1024 * 1024 * 1024,
    });
    b.step("antfly-owner-control-funding-check", "Analyze production cold apply opening with independently funded execution")
        .dependOn(&owner_control_funding_check.step);
    const owner_preflight_context_check = b.addTest(.{
        .name = "owner-preflight-context-check",
        .root_module = owner_preflight_context_root,
        .filters = &.{ "source owner restore context rejects before resource access", "controlled owner batch admission C entry", "request execution lifetime ingress", "controlled transaction dispatch" },
        .max_rss = 3 * 1024 * 1024 * 1024,
    });
    b.step("antfly-owner-preflight-context-check", "Analyze actual restore owner batch context transport without emitting a binary")
        .dependOn(&owner_preflight_context_check.step);

    const data_restore_recovery_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/data_restore_recovery_check_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, data_restore_recovery_root, true, true);
    data_restore_recovery_root.addImport("antfly_source_root", data_restore_recovery_root);
    const data_restore_recovery_sources = data_restore_recovery_root.import_table.get("antfly_local_sources").?;
    data_restore_recovery_sources.addOptions("capi_build_options", owner_preflight_capi_options);
    data_restore_recovery_sources.addImport("antfly_storage_root", data_restore_recovery_root);
    const data_restore_recovery_check = b.addTest(.{
        .name = "data-restore-recovery-check",
        .root_module = data_restore_recovery_root,
        .filters = &.{ "data restore recovery original admission rejects before metadata access", "data HA capture activity original admission rejects before owner access" },
        .max_rss = 3 * 1024 * 1024 * 1024,
    });
    b.step("antfly-data-restore-recovery-check", "Analyze actual DataServer restore recovery admission without emitting a binary")
        .dependOn(&data_restore_recovery_check.step);

    const remote_bootstrap_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/data_remote_attempt_bootstrap_check_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, remote_bootstrap_root, true, true);
    remote_bootstrap_root.addImport("antfly_source_root", remote_bootstrap_root);
    const remote_bootstrap_sources = remote_bootstrap_root.import_table.get("antfly_local_sources").?;
    remote_bootstrap_sources.addOptions("capi_build_options", owner_preflight_capi_options);
    remote_bootstrap_sources.addImport("antfly_storage_root", remote_bootstrap_root);
    const remote_bootstrap_check = b.addObject(.{
        .name = "data-remote-attempt-bootstrap-check",
        .root_module = remote_bootstrap_root,
        .max_rss = 3 * 1024 * 1024 * 1024,
    });
    b.step("antfly-data-remote-attempt-bootstrap-check", "Analyze the production node reopen hook through the opaque API boundary without emitting a binary")
        .dependOn(&remote_bootstrap_check.step);

    const storage_parent_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/storage_context_memory_parent_check_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, storage_parent_root, true, true);
    storage_parent_root.addImport("antfly_source_root", storage_parent_root);
    const storage_parent_sources = storage_parent_root.import_table.get("antfly_local_sources").?;
    storage_parent_sources.addOptions("capi_build_options", owner_preflight_capi_options);
    storage_parent_sources.addImport("antfly_storage_root", storage_parent_root);
    const storage_parent_check = b.addObject(.{
        .name = "storage-context-memory-parent-check",
        .root_module = storage_parent_root,
        .max_rss = 4 * 1024 * 1024 * 1024,
    });
    b.step("antfly-storage-context-memory-parent-check", "Analyze actual storage context parent ingress, construction and physical destruction")
        .dependOn(&storage_parent_check.step);

    const execution_context_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/storage_execution_context_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, execution_context_root, true, true);
    execution_context_root.addImport("antfly_source_root", execution_context_root);
    const execution_context_sources = execution_context_root.import_table.get("antfly_local_sources").?;
    execution_context_sources.addOptions("capi_build_options", owner_preflight_capi_options);
    execution_context_sources.addImport("antfly_storage_root", execution_context_root);
    const execution_context_tests = b.addTest(.{
        .name = "storage-execution-context-tests",
        .root_module = execution_context_root,
        .filters = &.{"shared execution storage context"},
        .max_rss = 8 * 1024 * 1024 * 1024,
    });
    b.step("antfly-storage-execution-context-test", "Run actual storage context shared execution, rollback and physical teardown")
        .dependOn(&b.addRunArtifact(execution_context_tests).step);

    const worker_memory_bootstrap_step = b.step("antfly-node-worker-memory-bootstrap-check", "Analyze standalone, data and metadata startup ownership in compiled and legacy storage profiles");
    const bounded_atomic_lock_tests = b.addTest(.{
        .name = "bounded-atomic-lock-tests",
        .root_module = b.createModule(.{
            .root_source_file = b.path("pkg/antfly-embedded/src/common/bounded_mutex.zig"),
            .target = target,
            .optimize = optimize,
        }),
        .filters = &.{"backend atomic admission"},
        .max_rss = 1024 * 1024 * 1024,
    });
    b.step("antfly-bounded-atomic-lock-test", "Verify backend/publication mutexes preserve original wait authority and roll back failed acquisition")
        .dependOn(&b.addRunArtifact(bounded_atomic_lock_tests).step);
    for ([_]bool{ true, false }) |compiled_storage| {
        const worker_memory_bootstrap_root = b.createModule(.{
            .root_source_file = b.path("pkg/antfly/src/node_worker_memory_bootstrap_check_root.zig"),
            .target = target,
            .optimize = optimize,
        });
        if (compiled_storage) test_imports.configureConsumer(b, worker_memory_bootstrap_root) else test_imports.configure(b, worker_memory_bootstrap_root, true, true);
        const worker_profile = b.addOptions();
        worker_profile.addOption(bool, "compiled_storage", compiled_storage);
        worker_memory_bootstrap_root.addOptions("worker_memory_bootstrap_options", worker_profile);
        worker_memory_bootstrap_root.addImport("antfly_source_root", worker_memory_bootstrap_root);
        worker_memory_bootstrap_root.addImport("antfly_admin_openapi", antfly_imports.admin_openapi);
        worker_memory_bootstrap_root.addImport("antfly_internal_openapi", antfly_imports.internal_openapi);
        const worker_memory_usermgr_storage = b.createModule(.{
            .root_source_file = b.path("pkg/antfly/src/usermgr/storage_imports.zig"),
            .target = target,
            .optimize = optimize,
        });
        worker_memory_usermgr_storage.addImport("antfly_root", worker_memory_bootstrap_root);
        worker_memory_usermgr_storage.addImport("antfly_platform", antfly_imports.platform);
        worker_memory_bootstrap_root.addImport("usermgr_storage", worker_memory_usermgr_storage);
        const worker_memory_bootstrap_check = b.addObject(.{
            .name = if (compiled_storage) "node-worker-memory-bootstrap-compiled-check" else "node-worker-memory-bootstrap-legacy-check",
            .root_module = worker_memory_bootstrap_root,
            .max_rss = 12 * 1024 * 1024 * 1024,
        });
        worker_memory_bootstrap_step.dependOn(&worker_memory_bootstrap_check.step);
    }

    const retained_read_provider_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/retained_read_provider_check_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, retained_read_provider_root, true, true);
    retained_read_provider_root.addImport("antfly_source_root", retained_read_provider_root);
    const retained_read_provider_sources = retained_read_provider_root.import_table.get("antfly_local_sources").?;
    retained_read_provider_sources.addOptions("capi_build_options", owner_preflight_capi_options);
    retained_read_provider_sources.addImport("antfly_storage_root", retained_read_provider_root);
    const retained_read_provider_check = b.addObject(.{
        .name = "retained-read-provider-check",
        .root_module = retained_read_provider_root,
        .max_rss = 3 * 1024 * 1024 * 1024,
    });
    b.step("antfly-retained-read-provider-check", "Analyze native retained read creation and page callbacks without emitting a binary")
        .dependOn(&retained_read_provider_check.step);

    const portable_publication_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/portable_publication_provider_check_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, portable_publication_root, true, true);
    portable_publication_root.addImport("antfly_source_root", portable_publication_root);
    const portable_publication_sources = portable_publication_root.import_table.get("antfly_local_sources").?;
    portable_publication_sources.addOptions("capi_build_options", owner_preflight_capi_options);
    portable_publication_sources.addImport("antfly_storage_root", portable_publication_root);
    const portable_publication_check = b.addObject(.{
        .name = "portable-publication-provider-check",
        .root_module = portable_publication_root,
        .max_rss = 3 * 1024 * 1024 * 1024,
    });
    b.step("antfly-portable-publication-provider-check", "Analyze directory and Lite import publication admission without emitting a binary")
        .dependOn(&portable_publication_check.step);

    const metadata_route_provider_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/metadata_route_provider_check_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, metadata_route_provider_root, true, true);
    metadata_route_provider_root.addImport("antfly_source_root", metadata_route_provider_root);
    const metadata_route_provider_sources = metadata_route_provider_root.import_table.get("antfly_local_sources").?;
    metadata_route_provider_sources.addOptions("capi_build_options", owner_preflight_capi_options);
    metadata_route_provider_sources.addImport("antfly_storage_root", metadata_route_provider_root);
    const metadata_route_check = b.addObject(.{
        .name = "metadata-route-provider-check",
        .root_module = metadata_route_provider_root,
        .max_rss = 3 * 1024 * 1024 * 1024,
    });
    b.step("antfly-metadata-route-provider-check", "Analyze actual metadata route and read-index admission providers without emitting a binary")
        .dependOn(&metadata_route_check.step);

    const metadata_read_provider_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/metadata_read_provider_check_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, metadata_read_provider_root, true, true);
    metadata_read_provider_root.addImport("antfly_source_root", metadata_read_provider_root);
    const metadata_read_provider_sources = metadata_read_provider_root.import_table.get("antfly_local_sources").?;
    metadata_read_provider_sources.addOptions("capi_build_options", owner_preflight_capi_options);
    metadata_read_provider_sources.addImport("antfly_storage_root", metadata_read_provider_root);
    const metadata_read_provider_check = b.addTest(.{
        .name = "metadata-read-provider-check",
        .root_module = metadata_read_provider_root,
        .filters = &.{"controlled metadata read provider rejects expiry before standalone state access"},
        .max_rss = 3 * 1024 * 1024 * 1024,
    });
    b.step("antfly-metadata-read-provider-check", "Analyze actual standalone metadata read provider admission without emitting a binary")
        .dependOn(&metadata_read_provider_check.step);

    const transaction_begin_storage_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/transaction_begin_storage_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, transaction_begin_storage_root, true, true);
    transaction_begin_storage_root.addImport("antfly_source_root", transaction_begin_storage_root);
    const transaction_begin_storage_sources = transaction_begin_storage_root.import_table.get("antfly_local_sources").?;
    transaction_begin_storage_sources.addOptions("capi_build_options", owner_preflight_capi_options);
    transaction_begin_storage_sources.addImport("antfly_storage_root", transaction_begin_storage_root);
    const transaction_begin_storage_tests = b.addTest(.{
        .name = "transaction-begin-storage-tests",
        .root_module = transaction_begin_storage_root,
        .filters = &.{ "controlled transaction persisted BEGIN", "controlled transaction persisted PREPARE", "controlled transaction persisted indexed resolution", "controlled transaction persisted coordinator deferral" },
        .max_rss = 3 * 1024 * 1024 * 1024,
    });
    b.step("antfly-transaction-begin-storage-test", "Verify actual transaction BEGIN rejection and accepted WAL replay on LSM")
        .dependOn(&b.addRunArtifact(transaction_begin_storage_tests).step);

    const read_memory_tests = b.addTest(.{
        .name = "read-memory-tests",
        .root_module = b.createModule(.{
            .root_source_file = b.path("pkg/antfly/src/common/read_memory.zig"),
            .target = target,
            .optimize = optimize,
        }),
        .filters = &.{"workload read memory"},
        .max_rss = 1024 * 1024 * 1024,
    });
    b.step("antfly-read-memory-test", "Verify physical read allocation ownership through lane transfer and retirement")
        .dependOn(&b.addRunArtifact(read_memory_tests).step);

    const cleanup_memory_tests = b.addTest(.{
        .name = "cleanup-memory-isolation-tests",
        .root_module = options.api_http_runtime_test_mod,
        .filters = &.{ "workload envelope cleanup", "remote attempt client API sender owns idle baseline", "workload envelope preserves control allocation", "workload envelope pins HTTP transport and control", "workload read pools startup", "workload public transport envelope" },
        .max_rss = 16 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    b.step("antfly-cleanup-memory-test", "Verify independent cleanup consumers under physical foreground and control exhaustion")
        .dependOn(&addFilteredTestRunArtifact(b, cleanup_memory_tests).step);

    const allocation_budget_tests = b.addTest(.{
        .name = "allocation-budget-tests",
        .root_module = b.createModule(.{
            .root_source_file = b.path("pkg/antfly/src/common/allocation_budget.zig"),
            .target = target,
            .optimize = optimize,
        }),
        .filters = &.{"workload envelope"},
        .max_rss = 1024 * 1024 * 1024,
    });
    b.step("antfly-allocation-budget-test", "Verify shared allocation accounting and cross-partition ownership")
        .dependOn(&b.addRunArtifact(allocation_budget_tests).step);

    const serverless_execution_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/serverless_execution_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, serverless_execution_root, true, true);
    serverless_execution_root.addImport("antfly_source_root", serverless_execution_root);
    const serverless_execution_tests = b.addTest(.{
        .name = "serverless-execution-tests",
        .root_module = serverless_execution_root,
        .filters = &.{ "serverless execution", "serverless request deadlines preserve accepted WAL", "fs wal", "fs WAL", "serverless server module compiles", "serverless server starts managed runtime and serves listener requests", "serverless main process memory budget" },
        .max_rss = 8 * 1024 * 1024 * 1024,
    });
    b.step("antfly-serverless-execution-test", "Verify serverless shared scheduler policy, protected health and owning release")
        .dependOn(&b.addRunArtifact(serverless_execution_tests).step);

    const wal_admission_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/wal_admission_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, wal_admission_root, true, true);
    wal_admission_root.addImport("antfly_source_root", wal_admission_root);
    const batch_visibility_archive_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/batch_visibility_archive_fixture.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, batch_visibility_archive_root, false, true);
    batch_visibility_archive_root.addImport("antfly_source_root", batch_visibility_archive_root);
    const batch_visibility_archive = b.addLibrary(.{
        .name = "batch-visibility-archive-fixture",
        .linkage = .static,
        .root_module = batch_visibility_archive_root,
        .max_rss = 3 * 1024 * 1024 * 1024,
    });
    wal_admission_root.linkLibrary(batch_visibility_archive);
    const wal_admission_tests = b.addTest(.{
        .name = "wal-admission-tests",
        .root_module = wal_admission_root,
        .filters = &.{ "controlled WAL snapshot", "controlled publisher preflight", "controlled owner preflight", "controlled restore registry", "controlled restore plan codec", "request context execution lifetime", "controlled range tracking context", "request metadata read lifetime", "controlled metadata read mutex", "controlled transaction execution admission" },
        .max_rss = 3 * 1024 * 1024 * 1024,
    });
    b.step("antfly-wal-admission-test", "Verify actual native and opaque WAL snapshot admission and original error relay")
        .dependOn(&b.addRunArtifact(wal_admission_tests).step);

    const ha_capture_activity_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/ha_capture_activity_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configureConsumer(b, ha_capture_activity_root);
    ha_capture_activity_root.addImport("antfly_source_root", ha_capture_activity_root);
    const ha_capture_activity_tests = b.addTest(.{
        .name = "ha-capture-activity-tests",
        .root_module = ha_capture_activity_root,
        .filters = &.{"controlled HA capture activity"},
        .max_rss = 3 * 1024 * 1024 * 1024,
    });
    b.step("antfly-ha-capture-activity-test", "Verify original HA capture lifetime through retained activity/source holders")
        .dependOn(&b.addRunArtifact(ha_capture_activity_tests).step);

    const ha_operation_admission_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/ha_operation_admission_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, ha_operation_admission_root, true, true);
    ha_operation_admission_root.addImport("antfly_source_root", ha_operation_admission_root);
    const ha_operation_admission_tests = b.addTest(.{
        .name = "ha-operation-admission-tests",
        .root_module = ha_operation_admission_root,
        .filters = &.{"HA original admission"},
        .max_rss = 3 * 1024 * 1024 * 1024,
    });
    b.step("antfly-ha-operation-admission-test", "Verify original HA capture admission, C1 holder/control progress and rejection retirement")
        .dependOn(&b.addRunArtifact(ha_operation_admission_tests).step);

    const controlled_service_lock_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/controlled_service_lock_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, controlled_service_lock_root, true, true);
    controlled_service_lock_root.addImport("antfly_source_root", controlled_service_lock_root);
    const controlled_service_lock_tests = b.addTest(.{
        .name = "controlled-service-lock-tests",
        .root_module = controlled_service_lock_root,
        .filters = &.{ "controlled apply lock", "controlled text analysis acquisition", "apply rw lock", "snapshot admission", "mutation admission controls", "mutation apply admission", "mutation batch root", "controlled pressure admission", "controlled replication preflight", "controlled owner batch admission", "controlled group batch context", "runnable scheduler", "runnable execution", "runnable fail-fast" },
        .max_rss = 3 * 1024 * 1024 * 1024,
    });
    b.step("antfly-controlled-service-lock-test", "Verify service lock waits release CPU and preserve original lifetime and lock ownership")
        .dependOn(&b.addRunArtifact(controlled_service_lock_tests).step);

    const metadata_codec_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/metadata_codec_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, metadata_codec_root, true, true);
    metadata_codec_root.addImport("antfly_source_root", metadata_codec_root);
    const metadata_codec_tests = b.addTest(.{
        .name = "metadata-wire-codec-tests",
        .root_module = metadata_codec_root,
        .filters = &.{"metadata raft apply store transition codec keeps published secrets distinct from restore staging"},
        .max_rss = 4 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    const metadata_codec_run = b.addRunArtifact(metadata_codec_tests);
    metadata_codec_run.addArg("--require-no-skips");
    b.step("antfly-metadata-wire-codec-test", "Verify published wire tags remain distinct from pooling extensions")
        .dependOn(&metadata_codec_run.step);
    const maintenance_selection_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/maintenance_selection_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, maintenance_selection_root, true, true);
    maintenance_selection_root.addImport("antfly_source_root", maintenance_selection_root);
    const maintenance_selection_tests = b.addTest(.{
        .name = "maintenance-selection-tests",
        .root_module = maintenance_selection_root,
        .filters = &.{ "maintenance selection bounds", "maintenance selection new owners", "maintenance selection production owner" },
        .max_rss = 3 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    const maintenance_selection_run = b.addRunArtifact(maintenance_selection_tests);
    maintenance_selection_run.addArg("--require-no-skips");
    b.step("antfly-maintenance-selection-test", "Verify due and debt work share bounded fair turns through the owner registry")
        .dependOn(&maintenance_selection_run.step);

    const maintenance_selection_local_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/maintenance_selection_local_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, maintenance_selection_local_root, true, true);
    maintenance_selection_local_root.addImport("antfly_openapi_specs", antfly_imports.embedded_openapi);
    maintenance_selection_local_root.addImport("antfly_source_root", maintenance_selection_local_root);
    const maintenance_selection_local_tests = b.addTest(.{
        .name = "maintenance-selection-local-tests",
        .root_module = maintenance_selection_local_root,
        .filters = &.{"maintenance selection local cache"},
        .max_rss = @as(usize, 12) * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    const maintenance_selection_local_run = b.addRunArtifact(maintenance_selection_local_tests);
    maintenance_selection_local_run.addArg("--require-no-skips");
    b.step("antfly-maintenance-selection-local-test", "Verify due/debt fairness and lease accounting against real local DB probes")
        .dependOn(&maintenance_selection_local_run.step);

    const owner_close_retirement_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/owner_close_retirement_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, owner_close_retirement_root, true, true);
    owner_close_retirement_root.addImport("antfly_source_root", owner_close_retirement_root);
    const owner_close_retirement_tests = b.addTest(.{
        .name = "owner-close-retirement-tests",
        .root_module = owner_close_retirement_root,
        .filters = &.{ "controlled owner lease retirement", "runnable execution retained consumer" },
        .max_rss = 3 * 1024 * 1024 * 1024,
    });
    b.step("antfly-owner-close-retirement-test", "Verify last owner lease transfers close to published control instead of stopped foreground")
        .dependOn(&b.addRunArtifact(owner_close_retirement_tests).step);

    const retained_snapshot_authority_tests = b.addTest(.{
        .name = "retained-snapshot-authority-tests",
        .root_module = owner_close_retirement_root,
        .filters = &.{"retained routed snapshot preserves certified cut"},
        .max_rss = 3 * 1024 * 1024 * 1024,
    });
    b.step("antfly-retained-snapshot-authority-test", "Verify delayed routed snapshots retain their cut after later writes and reject lost serving authority")
        .dependOn(&b.addRunArtifact(retained_snapshot_authority_tests).step);

    const derived_visibility_wait_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/derived_visibility_wait_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, derived_visibility_wait_root, true, true);
    derived_visibility_wait_root.addImport("antfly_source_root", derived_visibility_wait_root);
    const derived_visibility_wait_tests = b.addTest(.{
        .name = "derived-visibility-wait-tests",
        .root_module = derived_visibility_wait_root,
        .filters = &.{ "controlled propagation shutdown", "incremental resolution replay atomically", "PromotionRuntime", "controlled enrichment shutdown", "enrichment owner", "uninstalled enrichment config", "enrichment runtime construction unwinds", "enrichment replay passes are single flight", "enrichment inline backoff wakes for teardown", "controlled enrichment visibility", "enrichment visibility wait observes borrowed request cancellation", "enrichment visibility wait has a hard liveness timeout", "enrichment visibility wait is cancelable", "controlled derived quiescence", "controlled derived finalization", "controlled managed index", "controlled dense finalization", "controlled text merge producer close", "controlled text merge shutdown", "controlled derived visibility", "derived visibility wait", "derived enrichment visibility guard", "issue1015 visibility" },
        .max_rss = 4 * 1024 * 1024 * 1024,
    });
    b.step("antfly-derived-visibility-wait-test", "Verify original derived visibility rejection and callbacks outside worker locks")
        .dependOn(&b.addRunArtifact(derived_visibility_wait_tests).step);

    const dense_finalization_db_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/dense_finalization_db_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, dense_finalization_db_root, true, true);
    dense_finalization_db_root.addImport("antfly_source_root", dense_finalization_db_root);
    const dense_finalization_owner = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/api/runnable_owner_execution.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    dense_finalization_owner.addImport("antfly_runtime_abi", antfly_imports.runtime_abi);
    dense_finalization_owner.addImport("antfly_local_sources", dense_finalization_db_root);
    dense_finalization_db_root.addImport("runnable_test_owner", dense_finalization_owner);
    const dense_finalization_db_tests = b.addTest(.{
        .name = "dense-finalization-db-tests",
        .root_module = dense_finalization_db_root,
        .filters = &.{ "controlled dense finalization DB callback", "controlled dense finalization session claim", "dense session owner", "maintenance runtime cache close", "native publication finalization forwards raced source completion", "controlled durable close DB dependency", "maintenance producer close", "controlled index retirement" },
        .max_rss = 16 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    b.step("antfly-dense-finalization-test", "Verify controlled preparation retains a real dense capture until acceptance")
        .dependOn(&b.addRunArtifact(dense_finalization_db_tests).step);

    const graph_maintenance_runtime_tests = b.addTest(.{
        .name = "graph-maintenance-runtime-tests",
        .root_module = dense_finalization_db_root,
        .filters = &.{ "graph metric runtime background", "graph metric runtime lease", "graph metric runtime role", "graph metric runtime config", "graph metric runtime requires", "graph metric runtime worker pool identity", "graph metric runtime boundary tick", "graph metric runtime retirement", "graph metric shared maintenance" },
        .max_rss = 16 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    const graph_maintenance_runtime_run = b.addRunArtifact(graph_maintenance_runtime_tests);
    // Runtime filters reject missing test discovery instead of accepting only
    // anonymous semantic-analysis roots as a successful graph qualification.
    for (graph_maintenance_runtime_tests.filters) |filter| graph_maintenance_runtime_run.addArgs(&.{ "--test-filter", filter });
    graph_maintenance_runtime_run.addArg("--require-no-skips");
    b.step("antfly-graph-maintenance-runtime-test", "Verify shared graph metric cadence, lease ownership, wakeups and controlled retirement")
        .dependOn(&graph_maintenance_runtime_run.step);

    const enrichment_progress_tests = b.addTest(.{
        .name = "enrichment-progress-mailbox-tests",
        .root_module = derived_visibility_wait_root,
        .filters = &.{ "enrichment progress mailbox", "enrichment provider deadlines and progress cross native clock boundaries" },
        .max_rss = 4 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    const enrichment_progress_run = b.addRunArtifact(enrichment_progress_tests);
    enrichment_progress_run.addArg("--require-no-skips");
    b.step("antfly-enrichment-progress-mailbox-test", "Verify non-waiting progress publication and owning lifecycle consumption")
        .dependOn(&enrichment_progress_run.step);

    const outbox_read_admission_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/outbox_read_admission_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, outbox_read_admission_root, true, true);
    outbox_read_admission_root.addImport("antfly_source_root", outbox_read_admission_root);
    const outbox_read_admission_tests = b.addTest(.{
        .name = "outbox-read-admission-tests",
        .root_module = outbox_read_admission_root,
        .filters = &.{"durable outbox storage"},
        .max_rss = 3 * 1024 * 1024 * 1024,
    });
    b.step("antfly-outbox-read-admission-test", "Verify original lifetime through physical outbox page reads and finite copy quanta")
        .dependOn(&b.addRunArtifact(outbox_read_admission_tests).step);

    const replication_startup_admission_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/replication_startup_admission_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, replication_startup_admission_root, true, true);
    replication_startup_admission_root.addImport("antfly_source_root", replication_startup_admission_root);
    const replication_startup_admission_tests = b.addTest(.{
        .name = "replication-startup-admission-tests",
        .root_module = replication_startup_admission_root,
        .filters = &.{"replication startup original lifetime"},
        .max_rss = 3 * 1024 * 1024 * 1024,
    });
    b.step("antfly-replication-startup-admission-test", "Verify original lifetime through startup flush and apply admission")
        .dependOn(&b.addRunArtifact(replication_startup_admission_tests).step);

    const sql_compiler_preparation_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/sql_compiler_preparation_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    sql_compiler_preparation_root.addImport("sql_parser", antfly_imports.sql_parser);
    sql_compiler_preparation_root.addImport("antfly_platform", antfly_imports.platform);
    sql_compiler_preparation_root.addImport("antfly_sql_regex", antfly_imports.sql_regex);
    sql_compiler_preparation_root.addImport("antfly_hash", antfly_imports.hash);
    const sql_compiler_preparation_tests = b.addTest(.{
        .name = "sql-compiler-preparation-tests",
        .root_module = sql_compiler_preparation_root,
        .filters = &.{
            "sql.compiler.",
            "sql.builtin_cast.",
            "controlled preparation subquery scans",
        },
        .max_rss = 3 * 1024 * 1024 * 1024,
    });
    b.step("antfly-sql-compiler-preparation-test", "Verify SQL preparation semantics, bounded checkpoints and partial allocation cleanup")
        .dependOn(&b.addRunArtifact(sql_compiler_preparation_tests).step);

    const sql_memory_budget_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/sql_memory_budget_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    sql_memory_budget_root.addImport("antfly_runtime_abi", antfly_imports.runtime_abi);
    sql_memory_budget_root.addImport("antfly_platform", antfly_imports.platform);
    const sql_memory_budget_tests = b.addTest(.{
        .name = "sql-memory-budget-tests",
        .root_module = sql_memory_budget_root,
        .filters = &.{"sql.memory_budget."},
        .max_rss = 1024 * 1024 * 1024,
    });
    b.step("antfly-sql-memory-budget-test", "Verify SQL allocation causes, arena headroom and exact quota release")
        .dependOn(&b.addRunArtifact(sql_memory_budget_tests).step);

    const sql_controlled_array_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/sql_controlled_array_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    sql_controlled_array_root.addImport("sql_parser", antfly_imports.sql_parser);
    sql_controlled_array_root.addImport("antfly_platform", antfly_imports.platform);
    sql_controlled_array_root.addImport("antfly_sql_regex", antfly_imports.sql_regex);
    sql_controlled_array_root.addImport("antfly_hash", antfly_imports.hash);
    const sql_controlled_array_tests = b.addTest(.{
        .name = "sql-controlled-array-tests",
        .root_module = sql_controlled_array_root,
        .filters = &.{ "sql.array_wire.", "sql.array_value.", "sql.json_order.", "sql.numeric_value.", "controlled datum byte" },
        .max_rss = 3 * 1024 * 1024 * 1024,
    });
    b.step("antfly-sql-controlled-array-test", "Verify native array validation, JSON hashes and numeric semantics under original work authority")
        .dependOn(&b.addRunArtifact(sql_controlled_array_tests).step);

    const sql_scalar_lifetime_tests = b.addTest(.{
        .name = "sql-scalar-lifetime-tests",
        .root_module = options.api_http_runtime_test_mod,
        .filters = &.{ "SQL scalar lifetime callbacks", "SQL CPU loan policy validation", "SQL pgwire credential snapshot", "SQL read resume fences", "SQL schema cache" },
        .max_rss = 4 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    b.step("antfly-sql-scalar-lifetime-test", "Verify scalar lifetime polls and SQL policy boundaries preserve checked CPU ownership")
        .dependOn(&addFilteredTestRunArtifact(b, sql_scalar_lifetime_tests).step);

    const sql_prepared_numeric_owner_tests = b.addTest(.{
        .name = "sql-prepared-numeric-owner-tests",
        .root_module = options.api_http_runtime_test_mod,
        .filters = &.{ "SQL prepared numeric serial owner", "SQL grouped coordinator scratch" },
        .max_rss = 4 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    b.step("antfly-sql-prepared-numeric-owner-test", "Verify serial SQL coordinator scope and node-charged scratch")
        .dependOn(&addFilteredTestRunArtifact(b, sql_prepared_numeric_owner_tests).step);

    const sql_prepared_numeric_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/sql_prepared_numeric_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    sql_prepared_numeric_root.addImport("sql_parser", antfly_imports.sql_parser);
    sql_prepared_numeric_root.addImport("antfly_decisions", @import("../../../lib/decisions/build.zig").create(b, b.path("lib/decisions/root.zig")));
    sql_prepared_numeric_root.addImport("antfly_runtime_abi", antfly_imports.runtime_abi);
    sql_prepared_numeric_root.addImport("antfly_platform", antfly_imports.platform);
    sql_prepared_numeric_root.addImport("antfly_sql_regex", antfly_imports.sql_regex);
    sql_prepared_numeric_root.addImport("antfly_hash", antfly_imports.hash);
    const sql_prepared_numeric_tests = b.addTest(.{
        .name = "sql-prepared-numeric-tests",
        .root_module = sql_prepared_numeric_root,
        .filters = &.{ "SQL prepared numeric", "SQL scoped vector", "SQL vector kernels", "SQL vector arithmetic", "SQL scoped ordered scans", "SQL row predicate native cancellation prevents mutation and releases resources", "SQL grouped", "SQL encoded grouping", "SQL global batch reductions", "SQL grouping work", "SQL JSONB", "structural JSON ordering", "primitive numeric hashes", "SQL exact NUMERIC retained", "SQL typed array retained", "SQL typed store", "SQL typed dictionaries", "SQL retained column", "SQL retained numeric dictionaries", "SQL dictionary handoff", "SQL hash join", "SQL typed hash join", "SQL typed array physical keys", "SQL typed aggregate vectors", "SQL NUMERIC accumulator", "SQL NUMERIC aggregate", "SQL regex", "SQL monotonic region budget", "SQL nested memory budget", "SQL memory budget", "SQL checked budget" },
        .max_rss = 3 * 1024 * 1024 * 1024,
    });
    b.step("antfly-sql-prepared-numeric-test", "Verify prepared numeric fixed scratch and SQL semantics")
        .dependOn(&b.addRunArtifact(sql_prepared_numeric_tests).step);

    const sql_scalar_input_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/sql_scalar_input_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    sql_scalar_input_root.addImport("sql_parser", antfly_imports.sql_parser);
    sql_scalar_input_root.addImport("antfly_runtime_abi", antfly_imports.runtime_abi);
    sql_scalar_input_root.addImport("antfly_platform", antfly_imports.platform);
    sql_scalar_input_root.addImport("antfly_sql_regex", antfly_imports.sql_regex);
    sql_scalar_input_root.addImport("antfly_hash", antfly_imports.hash);
    const sql_scalar_input_tests = b.addTest(.{
        .name = "sql-scalar-input-tests",
        .root_module = sql_scalar_input_root,
        .filters = &.{ "sql.scalar_input.", "sql.controlled_float.", "SQL builtin casts preserve", "controlled floating input", "controlled cast input", "SQL scalar input", "SQL shared scalar work owner", "SQL scalar JSON output" },
        .max_rss = 3 * 1024 * 1024 * 1024,
    });
    b.step("antfly-sql-scalar-input-test", "Verify bounded scalar input scans, legacy grammar and retained work/cancellation")
        .dependOn(&b.addRunArtifact(sql_scalar_input_tests).step);

    const sql_plan_cache_retirement_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/sql_plan_cache_retirement_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    const sql_plan_cache_sources = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/source_catalog.zig"),
        .target = target,
        .optimize = optimize,
    });
    const sql_plan_cache_inventory = b.addOptions();
    sql_plan_cache_inventory.addOption([]const []const u8, "names", &.{ "sql_compiler", "sql_memory_budget" });
    sql_plan_cache_sources.addOptions("antfly_local_test_sources", sql_plan_cache_inventory);
    sql_plan_cache_sources.addImport("antfly_test_source_root", sql_plan_cache_retirement_root);
    sql_plan_cache_sources.addImport("sql_parser", antfly_imports.sql_parser);
    sql_plan_cache_sources.addImport("antfly_platform", antfly_imports.platform);
    sql_plan_cache_sources.addImport("antfly_sql_regex", antfly_imports.sql_regex);
    sql_plan_cache_sources.addImport("antfly_hash", antfly_imports.hash);
    sql_plan_cache_retirement_root.addImport("antfly_local_sources", sql_plan_cache_sources);
    const sql_plan_cache_retirement_tests = b.addTest(.{
        .name = "sql-plan-cache-retirement-tests",
        .root_module = sql_plan_cache_retirement_root,
        .filters = &.{"sql.plan_cache."},
        .max_rss = 2 * 1024 * 1024 * 1024,
    });
    b.step("antfly-sql-plan-cache-retirement-test", "Verify contended plan loan returns, bounded reconciliation and physical cache accounting")
        .dependOn(&b.addRunArtifact(sql_plan_cache_retirement_tests).step);

    const sql_input_owner_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/sql_input_owner_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    const sql_input_owner_sources = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/source_catalog.zig"),
        .target = target,
        .optimize = optimize,
    });
    const sql_input_owner_inventory = b.addOptions();
    sql_input_owner_inventory.addOption([]const []const u8, "names", &.{ "common_bounded_admission", "common_request_admission", "sql_memory_budget", "sql_cooperative_json_output", "sql_cooperative_json_source" });
    sql_input_owner_sources.addOptions("antfly_local_test_sources", sql_input_owner_inventory);
    sql_input_owner_sources.addImport("antfly_test_source_root", sql_input_owner_root);
    sql_input_owner_root.addImport("antfly_local_sources", sql_input_owner_sources);
    const sql_input_owner_tests = b.addTest(.{
        .name = "sql-input-owner-tests",
        .root_module = sql_input_owner_root,
        .filters = &.{ "SQL JSON source", "api.sql_parameter_accounting.", "api.sql_input_capsule." },
        .max_rss = 2 * 1024 * 1024 * 1024,
    });
    b.step("antfly-sql-input-owner-test", "Verify original authority through SQL JSON scanning and iterative input accounting")
        .dependOn(&b.addRunArtifact(sql_input_owner_tests).step);
    const sql_json_output_tests = b.addTest(.{
        .name = "sql-json-output-tests",
        .root_module = sql_input_owner_root,
        .filters = &.{"api.sql_output_control."},
        .max_rss = 2 * 1024 * 1024 * 1024,
    });
    const sql_cooperative_json_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/sql_cooperative_json_test_root.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    const sql_cooperative_json_tests = b.addTest(.{
        .name = "sql-cooperative-json-output-tests",
        .root_module = sql_cooperative_json_root,
        .filters = &.{"sql.cooperative_json_output."},
        .max_rss = 2 * 1024 * 1024 * 1024,
    });
    const sql_json_output_step = b.step("antfly-sql-json-output-test", "Verify shared controlled JSON output parity, original read authority and partial-output retirement");
    sql_json_output_step.dependOn(&b.addRunArtifact(sql_json_output_tests).step);
    sql_json_output_step.dependOn(&b.addRunArtifact(sql_cooperative_json_tests).step);

    const sql_http_lifetime_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/api/sql_http_lifetime_test_root.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    sql_http_lifetime_root.addImport("httpx", antfly_imports.httpx);
    const sql_http_lifetime_sources = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/source_catalog.zig"),
        .target = target,
        .optimize = optimize,
    });
    sql_http_lifetime_sources.addOptions("antfly_local_test_sources", sql_input_owner_inventory);
    sql_http_lifetime_sources.addImport("antfly_test_source_root", sql_http_lifetime_root);
    sql_http_lifetime_root.addImport("antfly_local_sources", sql_http_lifetime_sources);
    const sql_http_lifetime_tests = b.addTest(.{
        .name = "sql-http-lifetime-tests",
        .root_module = sql_http_lifetime_root,
        .filters = &.{"SQL HTTP lifetime"},
        .max_rss = 3 * 1024 * 1024 * 1024,
    });
    b.step("antfly-sql-http-lifetime-test", "Verify original SQL input/root and heap cancellation frame survive through physical HTTP context retirement")
        .dependOn(&b.addRunArtifact(sql_http_lifetime_tests).step);

    const sql_pgwire_input_options_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/sql_pgwire_input_options_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    const sql_pgwire_input_options_sources = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/source_catalog.zig"),
        .target = target,
        .optimize = optimize,
    });
    const sql_pgwire_input_options_inventory = b.addOptions();
    sql_pgwire_input_options_inventory.addOption([]const []const u8, "names", &.{ "common_bounded_admission", "sql_scalar" });
    sql_pgwire_input_options_sources.addOptions("antfly_local_test_sources", sql_pgwire_input_options_inventory);
    sql_pgwire_input_options_sources.addImport("antfly_test_source_root", sql_pgwire_input_options_root);
    sql_pgwire_input_options_sources.addImport("sql_parser", antfly_imports.sql_parser);
    sql_pgwire_input_options_sources.addImport("antfly_platform", antfly_imports.platform);
    sql_pgwire_input_options_sources.addImport("antfly_sql_regex", antfly_imports.sql_regex);
    sql_pgwire_input_options_sources.addImport("antfly_hash", antfly_imports.hash);
    sql_pgwire_input_options_sources.addImport("antfly_runtime_abi", antfly_imports.runtime_abi);
    sql_pgwire_input_options_root.addImport("antfly_local_sources", sql_pgwire_input_options_sources);
    sql_pgwire_input_options_root.addImport("antfly_runtime_abi", antfly_imports.runtime_abi);
    sql_pgwire_input_options_root.addImport("sql_parser", antfly_imports.sql_parser);
    const sql_pgwire_input_options_tests = b.addTest(.{
        .name = "sql-pgwire-input-options-tests",
        .root_module = sql_pgwire_input_options_root,
        .filters = &.{ "SQL pgwire retained input charge", "SQL query waiting pgwire preserves deadline", "SQL pgwire controlled datetime" },
        .max_rss = 2 * 1024 * 1024 * 1024,
    });
    b.step("antfly-sql-pgwire-input-options-test", "Verify captured pgwire input charges preserve original deadline and cancellation restrictions")
        .dependOn(&b.addRunArtifact(sql_pgwire_input_options_tests).step);

    const sql_spill_retirement_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/sql_spill_retirement_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, sql_spill_retirement_root, true, true);
    sql_spill_retirement_root.addImport("antfly_source_root", sql_spill_retirement_root);
    const sql_spill_retirement_tests = b.addTest(.{
        .name = "sql-spill-retirement-tests",
        .root_module = sql_spill_retirement_root,
        .filters = &.{ "SQL spill", "SQL buffered spill" },
        .max_rss = 3 * 1024 * 1024 * 1024,
    });
    b.step("antfly-sql-spill-retirement-test", "Verify physical SQL spill admission, quotas and owned cleanup")
        .dependOn(&b.addRunArtifact(sql_spill_retirement_tests).step);

    const bounded_sql_worker_tests = b.addTest(.{
        .name = "bounded-sql-worker-api-tests",
        .root_module = options.api_http_runtime_test_mod,
        .filters = &.{ "SQL bounded read workers", "SQL protected executor" },
        .max_rss = 4 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    b.step("antfly-bounded-sql-worker-test", "Verify default bounded SQL executor publication and private bounded-read worker progress")
        .dependOn(&addFilteredTestRunArtifact(b, bounded_sql_worker_tests).step);

    const shared_sql_worker_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/shared_sql_workers_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, shared_sql_worker_root, true, true);
    shared_sql_worker_root.addImport("antfly_source_root", shared_sql_worker_root);
    const shared_sql_worker_tests = b.addTest(.{
        .name = "shared-sql-worker-tests",
        .root_module = shared_sql_worker_root,
        .filters = &.{ "SQL imported worker", "SQL shared worker" },
        .max_rss = 3 * 1024 * 1024 * 1024,
    });
    b.step("antfly-shared-sql-worker-test", "Verify SQL instances share node worker roles and retire leases after owned joins")
        .dependOn(&b.addRunArtifact(shared_sql_worker_tests).step);

    const maintenance_shutdown_admission_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/maintenance_shutdown_admission_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, maintenance_shutdown_admission_root, true, true);
    maintenance_shutdown_admission_root.addImport("antfly_source_root", maintenance_shutdown_admission_root);
    const maintenance_shutdown_admission_tests = b.addTest(.{
        .name = "maintenance-shutdown-admission-tests",
        .root_module = maintenance_shutdown_admission_root,
        .filters = &.{ "independent maintenance", "maintenance scheduler", "maintenance runtime", "graph cleanup owner", "quarantine recovery", "external lake native warming", "publication recovery registration", "schema reconciliation", "portable activation finite recovery", "native projection owner" },
        .max_rss = 3 * 1024 * 1024 * 1024,
    });
    b.step("antfly-maintenance-shutdown-admission-test", "Verify recurring maintenance stops admitting stages and physically joins current callbacks")
        .dependOn(&b.addRunArtifact(maintenance_shutdown_admission_tests).step);

    const recovery_notification_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/recovery_notification_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, recovery_notification_root, true, true);
    recovery_notification_root.addImport("antfly_source_root", recovery_notification_root);
    const recovery_notification_tests = b.addTest(.{
        .name = "recovery-notification-tests",
        .root_module = recovery_notification_root,
        .filters = &.{"transaction recovery notification"},
        .max_rss = 3 * 1024 * 1024 * 1024,
    });
    b.step("antfly-recovery-notification-test", "Verify recovery releases capture admission during participant notification and retains durable debt")
        .dependOn(&b.addRunArtifact(recovery_notification_tests).step);

    const mutation_root_lease_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/mutation_root_lease_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, mutation_root_lease_root, true, true);
    mutation_root_lease_root.addImport("antfly_source_root", mutation_root_lease_root);
    const mutation_root_lease_tests = b.addTest(.{
        .name = "mutation-root-lease-tests",
        .root_module = mutation_root_lease_root,
        .filters = &.{"mutation root lease"},
        .max_rss = 3 * 1024 * 1024 * 1024,
    });
    b.step("antfly-mutation-root-lease-test", "Verify independent mutation root leases preserve original C1 ownership and writer priority")
        .dependOn(&b.addRunArtifact(mutation_root_lease_tests).step);

    const text_scoring_authority_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/text_scoring_authority_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, text_scoring_authority_root, true, true);
    text_scoring_authority_root.addImport("antfly_source_root", text_scoring_authority_root);
    const text_scoring_authority_tests = b.addTest(.{
        .name = "text-scoring-authority-tests",
        .root_module = text_scoring_authority_root,
        .filters = &.{ "native text deletion wait", "streaming boolean", "controlled WAND scoring", "native text lifetime", "native text search propagates lifetime", "native dictionary search entry", "controlled dictionary filters", "controlled dictionary iterators", "prefix filter seeks late range", "wildcard filter seeks by literal prefix", "fuzzy filter pruning matches", "regexp filter prunes", "controlled count filters", "native text count authority", "controlled term filter checkpoints", "scorer", "single-term", "WAND", "shared top-k" },
        .max_rss = 3 * 1024 * 1024 * 1024,
    });
    b.step("antfly-text-scoring-authority-test", "Verify original lifetime failures retire native scoring state")
        .dependOn(&b.addRunArtifact(text_scoring_authority_tests).step);

    const text_filter_controls_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/text_filter_controls_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, text_filter_controls_root, true, true);
    text_filter_controls_root.addImport("antfly_source_root", text_filter_controls_root);
    const text_filter_controls_tests = b.addTest(.{
        .name = "text-filter-controls-tests",
        .root_module = text_filter_controls_root,
        .filters = &.{ "controlled term filter checkpoints", "term filter with synonym expansion" },
        .max_rss = 3 * 1024 * 1024 * 1024,
    });
    b.step("antfly-text-filter-controls-test", "Verify controlled postings scans preserve exact results and retire rejected partial scans")
        .dependOn(&b.addRunArtifact(text_filter_controls_tests).step);

    const intent_collection_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/intent_collection_controls_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, intent_collection_root, true, true);
    intent_collection_root.addImport("antfly_source_root", intent_collection_root);
    const intent_collection_tests = b.addTest(.{
        .name = "intent-collection-controls-tests",
        .root_module = intent_collection_root,
        .filters = &.{ "transaction intent collection original admission", "transaction intent snapshot owns one payload copy" },
        .max_rss = 3 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    const intent_collection_run = b.addRunArtifact(intent_collection_tests);
    intent_collection_run.addArg("--require-no-skips");
    b.step("antfly-intent-collection-controls-test", "Verify physical transaction snapshot original admission and rejection retirement")
        .dependOn(&intent_collection_run.step);

    const capture_dependency_admission_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/capture_dependency_admission_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, capture_dependency_admission_root, true, true);
    capture_dependency_admission_root.addImport("antfly_source_root", capture_dependency_admission_root);
    const capture_dependency_admission_tests = b.addTest(.{
        .name = "capture-dependency-admission-tests",
        .root_module = capture_dependency_admission_root,
        .filters = &.{"capture dependency admission"},
        .max_rss = 4 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    const capture_dependency_admission_run = b.addRunArtifact(capture_dependency_admission_tests);
    capture_dependency_admission_run.addArg("--require-no-skips");
    b.step("antfly-capture-dependency-admission-test", "Verify capture dependency retirement before original waits and publication contention")
        .dependOn(&capture_dependency_admission_run.step);

    const local_decoder_admission_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/local_decoder_admission_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, local_decoder_admission_root, true, true);
    local_decoder_admission_root.addImport("antfly_source_root", local_decoder_admission_root);
    const local_decoder_admission_tests = b.addTest(.{
        .name = "local-decoder-admission-tests",
        .root_module = local_decoder_admission_root,
        .filters = &.{ "lsm local decoder", "lsm local waiter" },
        .max_rss = 3 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    const local_decoder_admission_run = b.addRunArtifact(local_decoder_admission_tests);
    local_decoder_admission_run.addArg("--require-no-skips");
    b.step("antfly-local-decoder-admission-test", "Verify bounded local decode sharing and native sole-slot producer progress")
        .dependOn(&local_decoder_admission_run.step);

    const physical_sync_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/physical_sync_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, physical_sync_root, true, true);
    physical_sync_root.addImport("antfly_source_root", physical_sync_root);
    const physical_sync_tests = b.addTest(.{
        .name = "physical-sync-tests",
        .root_module = physical_sync_root,
        .filters = &.{"controlled physical sync"},
        .max_rss = 4 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    const physical_sync_run = b.addRunArtifact(physical_sync_tests);
    physical_sync_run.addArgs(&.{ "--test-filter", "controlled physical sync", "--require-no-skips" });
    b.step("antfly-physical-sync-test", "Verify owning physical durability operations and controlled retry without synchronous fallback")
        .dependOn(&physical_sync_run.step);

    const state_merge_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/state_merge_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, state_merge_root, true, true);
    state_merge_root.addImport("antfly_source_root", state_merge_root);
    const state_merge_filters = &.{
        "controlled physical sync flush state merge",
        "lsm backend background flush waits for and merges a complete immutable window",
        "lsm backend idle maintenance publishes a partial immutable window",
        "lsm backend size maintenance preserves a partial immutable window",
        "lsm backend byte flush window coalesces hot overwrites before run publication",
    };
    const state_merge_tests = b.addTest(.{
        .name = "flush-state-merge-tests",
        .root_module = state_merge_root,
        .filters = state_merge_filters,
        .max_rss = 4 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    const state_merge_run = b.addRunArtifact(state_merge_tests);
    state_merge_run.addArg("--require-no-skips");
    b.step("antfly-flush-state-merge-test", "Verify retained flush merging and normal immutable-window publication")
        .dependOn(&state_merge_run.step);

    const builder_output_tests = b.addTest(.{
        .name = "builder-output-tests",
        .root_module = physical_sync_root,
        .filters = &.{"controlled physical sync prepaid run"},
        .max_rss = 4 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    const builder_output_run = b.addRunArtifact(builder_output_tests);
    builder_output_run.addArgs(&.{ "--test-filter", "controlled physical sync prepaid run", "--require-no-skips" });
    b.step("antfly-builder-output-test", "Verify owned builder bounds, output accounting and rejected handoff lifecycle")
        .dependOn(&builder_output_run.step);

    const output_retirement_tests = b.addTest(.{
        .name = "output-retirement-tests",
        .root_module = physical_sync_root,
        .filters = &.{ "controlled physical sync output retirement", "compaction publication stages", "compaction publication uses" },
        .max_rss = 4 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    const output_retirement_run = b.addRunArtifact(output_retirement_tests);
    output_retirement_run.addArgs(&.{ "--test-filter", "controlled physical sync output retirement", "--test-filter", "compaction publication stages", "--test-filter", "compaction publication uses", "--require-no-skips" });
    b.step("antfly-output-retirement-test", "Verify yieldable physical output and cached table-index destruction")
        .dependOn(&output_retirement_run.step);

    const read_metadata_tests = b.addTest(.{
        .name = "read-metadata-retirement-tests",
        .root_module = physical_sync_root,
        .filters = &.{"controlled physical sync read metadata"},
        .max_rss = 4 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    const read_metadata_run = b.addRunArtifact(read_metadata_tests);
    read_metadata_run.addArgs(&.{ "--test-filter", "controlled physical sync read metadata", "--require-no-skips" });
    b.step("antfly-read-metadata-retirement-test", "Verify constant-work read cache invalidation and bounded physical metadata retirement")
        .dependOn(&read_metadata_run.step);

    const resumable_payload_tests = b.addTest(.{
        .name = "resumable-payload-retirement-tests",
        .root_module = physical_sync_root,
        .filters = &.{ "controlled physical sync resumable payload", "controlled physical sync registered entry" },
        .max_rss = 4 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    const resumable_payload_run = b.addRunArtifact(resumable_payload_tests);
    resumable_payload_run.addArgs(&.{ "--test-filter", "controlled physical sync", "--require-no-skips" });
    b.step("antfly-resumable-payload-retirement-test", "Verify registered resumable cleanup retains funding and independent progress")
        .dependOn(&resumable_payload_run.step);

    const directory_entry_filters = &.{ "controlled physical sync funded directory entry", "controlled physical sync private put" };
    const directory_entry_tests = b.addTest(.{
        .name = "directory-entry-preparation-tests",
        .root_module = physical_sync_root,
        .filters = directory_entry_filters,
        .max_rss = 4 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    const directory_entry_run = b.addRunArtifact(directory_entry_tests);
    directory_entry_run.addArgs(&.{ "--test-filter", "controlled physical sync", "--require-no-skips" });
    b.step("antfly-directory-entry-preparation-test", "Verify funded retained directory entry construction and retirement")
        .dependOn(&directory_entry_run.step);

    const storage_read_admission_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/storage_read_admission_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, storage_read_admission_root, true, true);
    storage_read_admission_root.addImport("antfly_source_root", storage_read_admission_root);
    const storage_read_admission_tests = b.addTest(.{
        .name = "storage-read-admission-tests",
        .root_module = storage_read_admission_root,
        .filters = &.{ "lsm source read admission", "lsm local cold source", "lsm local decoder admission", "lsm routing cursor local decoder admission", "lite snapshot admission preserves original cancellation while the backend lock is held", "lite routing cursor checks original admission and clears failed operation borrows", "lsm routing cursor interrupts internal tombstone scans with original admission", "lsm routing cursor carries the original deadline into a held cache loader", "lsm routing cursor cancels cold index decoding and resumes a clean loader", "lsm backend loader ownership admits key bytes before storage and retries correct data", "lsm backend fallback reads interrupt internal decoding with original scope and retry", "lsm admitted successful cursor close transfers pins under held cache shards without borrowing expired scope", "native fd admission try", "weighted native fd admission preserves FIFO progress", "shared native fd admission wait is cancellation aware" },
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    const storage_read_admission_run = b.addRunArtifact(storage_read_admission_tests);
    storage_read_admission_run.addArg("--require-no-skips");
    b.step("antfly-storage-read-admission-test", "Require physical LSM and Lite original read admission and cursor cleanup")
        .dependOn(&storage_read_admission_run.step);

    const background_dispatch_root = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/background_maintenance_dispatch_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, background_dispatch_root, true, true);
    background_dispatch_root.addImport("antfly_source_root", background_dispatch_root);
    const background_dispatch_tests = b.addTest(.{
        .name = "background-maintenance-dispatch-tests",
        .root_module = background_dispatch_root,
        .filters = &.{
            "lsm backend bounded background",
            "lsm backend schedules deferred immutable flush on configured background executor",
            "lsm backend background flush waits for and merges a complete immutable window",
            "lsm backend detached no-op maintenance does not resubmit forever",
            "lsm backend schedules detached maintenance job for compaction debt",
            "lsm backend background io budget",
        },
        .max_rss = 4 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    const background_dispatch_run = b.addRunArtifact(background_dispatch_tests);
    background_dispatch_run.addArg("--require-no-skips");
    b.step("antfly-background-maintenance-dispatch-test", "Verify finite normal flush/maintenance dispatches, continuation ownership and interleaved reads")
        .dependOn(&background_dispatch_run.step);

    const maintenance_lifetime_tests = b.addTest(.{
        .name = "maintenance-original-lifetime-tests",
        .root_module = storage_read_admission_root,
        .filters = &.{"maintenance original lifetime"},
        .max_rss = 4 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    const maintenance_lifetime_run = b.addRunArtifact(maintenance_lifetime_tests);
    maintenance_lifetime_run.addArg("--require-no-skips");
    b.step("antfly-maintenance-original-lifetime-test", "Verify original lifetime rejection around physical flush, publication, cleanup and reopen")
        .dependOn(&maintenance_lifetime_run.step);

    const lite_recovery_capture_tests = b.addTest(.{
        .name = "lite-recovery-capture-tests",
        .root_module = storage_read_admission_root,
        .filters = &.{
            "lite coordinated",
            "lite controlled reader fence",
            "lite allocator verification follows only identical certain owner checkpoints",
            "lite allocator detached verification cancels and refuses stale checkpoints",
            "lite allocator v4 rejects a durable free bit for a live page",
        },
        .max_rss = 4 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    const lite_recovery_capture_run = b.addRunArtifact(lite_recovery_capture_tests);
    lite_recovery_capture_run.addArg("--require-no-skips");
    b.step("antfly-lite-recovery-capture-test", "Verify detached Lite allocator proofs, cancellation and corrupt-ledger refusal")
        .dependOn(&lite_recovery_capture_run.step);

    const routing_owner_tests = b.addTest(.{
        .name = "transaction-routing-owner-tests",
        .root_module = local_commit_control_root,
        .filters = &.{"distributed txn routing releases catalog ownership on missing topology and rejected admission"},
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    const routing_owner_run = addFilteredTestRunArtifact(b, routing_owner_tests);
    routing_owner_run.addArg("--require-no-skips");
    b.step("antfly-transaction-routing-owner-test", "Require transaction routing snapshot ownership on every admission exit")
        .dependOn(&routing_owner_run.step);

    const commit_control_tests = b.addTest(.{
        .name = "commit-control-contract-tests",
        .root_module = local_commit_control_root,
        .filters = &.{"shared stateless commit controls retain the original deadline through conflict retries"},
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    const commit_control_run = addFilteredTestRunArtifact(b, commit_control_tests);
    commit_control_run.addArg("--require-no-skips");
    b.step("antfly-commit-control-test", "Require original commit budgets through stateless retries and preserve unknown outcomes")
        .dependOn(&commit_control_run.step);

    const predecision_budget_tests = b.addTest(.{
        .name = "predecision-budget-contract-tests",
        .root_module = api_transactions_docid_test_mod,
        .filters = &.{"hosted participant shares one pre-decision budget across begin prepare and fanout"},
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    const predecision_budget_run = addFilteredTestRunArtifact(b, predecision_budget_tests);
    predecision_budget_run.addArg("--require-no-skips");
    b.step("antfly-predecision-budget-test", "Require shared hosted participant deadlines and pre-contact cancellation")
        .dependOn(&predecision_budget_run.step);

    const participant_team_tests = b.addTest(.{
        .name = "participant-team-contract-tests",
        .root_module = api_transactions_docid_test_mod,
        .filters = &.{"distributed txn participant team bounds CPU and child allocations"},
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    const participant_team_run = addFilteredTestRunArtifact(b, participant_team_tests);
    participant_team_run.addArg("--require-no-skips");
    b.step("antfly-participant-team-test", "Require the actual coordinator participant team and bounded child allocation regression")
        .dependOn(&participant_team_run.step);

    const run_api_public_table_http_docid_tests = addFilteredTestRunArtifact(b, api_public_table_http_docid_tests);
    run_api_public_table_http_docid_tests.addArg("--require-no-skips");
    b.step("antfly-api-public-table-http-test", "Run public table HTTP response contracts").dependOn(&run_api_public_table_http_docid_tests.step);
    const mandatory_batch_response_tests = b.addTest(.{
        .name = "api-mandatory-batch-response-tests",
        .root_module = api_public_table_http_docid_test_mod,
        .filters = &.{ "public table batch mandatory responses", "public table batch handler maps write unavailable errors" },
        .max_rss = 2 * 1024 * 1024 * 1024,
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    b.step("antfly-api-mandatory-batch-response-test", "Run response admission and allocation-free post-acceptance batch contracts")
        .dependOn(&addFilteredTestRunArtifact(b, mandatory_batch_response_tests).step);

    const api_relational_row_contract_tests = b.addTest(.{
        .root_module = api_public_table_http_docid_test_mod,
        .filters = &.{ "relational mutation", "relational row query", "relational declarations", "ordinary schema update rejects unacknowledged FK generation", "FK generation publication coordinator", "FK initial create coordinator", "SQL TRUNCATE" },
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    b.step("antfly-api-relational-rows-test", "Run generated relational row and schema boundary contracts").dependOn(&addFilteredTestRunArtifact(b, api_relational_row_contract_tests).step);
    b.step("antfly-schema-expression-test", "Run durable expression semantics and bounded preparation contracts")
        .dependOn(&addCuratedTestRunArtifact(b, api_relational_row_contract_tests, &.{"relational declarations"}).step);
    const restore_lookup_authority_tests = b.addTest(.{
        .root_module = api_public_table_http_docid_test_mod,
        .filters = &.{ "private restore lookup plan identity", "compiled lookup wire preserves binary scope", "ancestors-only hierarchy survives the internal wire re-encode without a stray group_by" },
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    b.step("antfly-api-restore-lookup-authority-test", "Run private restore lookup authority parsing and compiled wire regressions").dependOn(&addFilteredTestRunArtifact(b, restore_lookup_authority_tests).step);
    const api_index_maintenance_tests = b.addTest(.{
        .root_module = api_public_table_http_docid_test_mod,
        .filters = &.{ "index maintenance", "hot-standby mutation" },
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    b.step("antfly-api-index-maintenance-test", "Run generation-fenced index maintenance admission and recovery contracts").dependOn(&addFilteredTestRunArtifact(b, api_index_maintenance_tests).step);
    const api_storage_capability_tests = b.addTest(.{
        .root_module = api_public_table_http_docid_test_mod,
        .filters = &.{"serverless capabilities"},
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    b.step("antfly-api-storage-capabilities-test", "Run backend relational capability admission contracts").dependOn(&addFilteredTestRunArtifact(b, api_storage_capability_tests).step);
    const api_relational_index_status_tests = b.addTest(.{
        .root_module = api_public_table_http_docid_test_mod,
        .filters = &.{"relational index status"},
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    b.step("antfly-api-relational-index-status-test", "Run unified relational index readiness and owner coverage contracts").dependOn(&addFilteredTestRunArtifact(b, api_relational_index_status_tests).step);
    const api_relational_topology_contract_tests = @import("linked_tests.zig").addPair(b, .{
        .name = "api-relational-topology-tests",
        .root_module = api_table_writes_docid_test_mod,
        .filters = &.{ "internal batch topology control", "relational backup cohort" },
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    }, write_implementation_tests);
    b.step("antfly-api-relational-topology-test", "Run private lifecycle control and backup cohort contracts").dependOn(&api_relational_topology_contract_tests.run(b).step);
    const run_raft_transition_runtime_docid_tests = addFilteredTestRunArtifact(b, raft_transition_runtime_docid_tests);
    const api_table_writes_production_regression_tests = @import("linked_tests.zig").addPair(b, .{
        .name = "api-table-write-lifecycle-tests",
        .root_module = api_table_writes_docid_test_mod,
        .max_rss = @as(usize, if (target.result.os.tag == .macos) 12 else 7) * 1024 * 1024 * 1024,
        .filters = &.{
            "restore staging private provisioning",
            "provisioned table write source drop table",
            "provisioned table drop cache drain",
            "provisioned writer cache starts DB workers after stable entry installation",
            "table write source restore acquires lifecycle unless caller reserves it",
            "provisioned native backup restore repeats through shared read and write owners",
            "native backup coordinator quiescence retry is bounded",
            "native backup reclaims crash-left snapshot attempts from durable markers",
            "native backup reclaims a crash marker before snapshot root creation",
            "native backup never reclaims an old attempt with a live lease",
            "provisioned table write source create table provisions local indexes and schema",
            "provisioned create succeeds when post-commit runtime status is fenced",
            "provisioned create retries a retired cache lease before structural publication",
            "provisioned create reuses a generation opened by startup reconciliation",
            "provisioned owner clone snapshot preserves retired runtime counters",
            "provisioned create installs managed enrichment despite a matching stale fingerprint",
            "provisioned create index enqueues target-fenced cached-writer activation",
            "cached runtime status is independent of writer lock and target observation",
            "provisioned table write source runtime status serves cached snapshot during active same-table work",
            "provisioned table write source runtime status still serves unrelated table snapshot while source mutex is busy",
            "provisioned table write source best effort publish does not advertise lock contention as an empty table",
            "hosted backup forwarding preserves external io authority",
            "backup storage resolution rejects a reused table name from another incarnation",
            "provisioned table restore retry repairs exact incomplete restore state through active writer",
            "provisioned table restore preparation blocks writes and competing structural mutation",
            "provisioned table restore preparation blocks writes while allowing reads",
            "resident group write releases queued reads before remote completion",
            "provisioned table transition activity excludes writers but preserves reads",
            "provisioned table transition waiter queues ahead of later writers",
            "provisioned table transition waiter queues ahead of later readers",
            "provisioned table write source read request permits replicated apply activity",
            "provisioned table group operation waiter queues ahead of later readers",
            "provisioned table write source drop table cancels index repair before structural admission",
            "dropped table quarantine path keeps valid API names in one portable component",
            "malformed recovery intent is durably removed from the active queue and counted",
            "transient recovery intent read failure retains active work and retries successfully",
            "dropped table recovery drains a wake coalesced during the active scan",
            "dropped table recovery watchdog repairs a failed durable enqueue",
            "provisioned table drop persists cleanup intent before filesystem failure and recovers after restart",
            "provisioned table drop retains repair intent until catalog ownership clears",
            "replica retirement journal distinguishes active retained and committed removal",
            "replica retirement journal batches preserve every group phase",
            "replica retirement batch identity is canonical and rejects duplicate groups",
            "replica retirement recovery discards a legacy orphan whose catalog removal aborted",
            "hosted source publication recovers durable dropped-table intent",
            "hosted background writes carry the selected catalog fence through routed Raft admission",
            "provisioned table write source drop table retires old publication authority",
            "provisioned table write source drop table does not hold local db mutex during background delete",
            "provisioned table write request queues structural reconcile ahead of later writes",
            "structural reconcile reservation defers metadata group refresh without blocking admitted work",
            "queued structural reconcile reserves write admission before its worker starts",
            "queued index catch-up does not reserve table write admission",
            "targeted index reconciliation stales only the named cached index",
            "targeted repair visibility edge preserves exact sibling cache authority",
            "unrelated repair visibility edge invalidates during targeted reconciliation",
            "repair edge after target release retains target authority until handoff",
            "initial build edge after target authority release preserves runtime observation",
            "fenced blocking status publication preserves accepted serving snapshot",
            "ownerless blocking publication preserves serving facts and fences convergence",
            "targeted index cache update retains the published sibling snapshot through handoff",
            "index reconciliation request enqueues a catalog-deleted target without create admission",
            "structural reconcile retry backoff is bounded",
            "structural reconcile productive quantum yields immediately while blocked work backs off",
            "structural reconcile retries a transient worker failure",
            "db structural mutation autonomously retries transient maintenance restart failures",
            "structural reconcile returns a bounded pending quantum while a group is busy",
            "structural reconcile publishes durable index repair debt once per group",
            "structural repair handoff keeps status fenced through final shard visibility",
            "repair handoff status settles after authoritative cached publication",
            "live repair final audit excludes concurrent group mutation through publication",
            "live repair validates resident writer against current catalog not queued metadata",
            "terminal repair publication settles handoff or retains one fenced retry",
            "managed create publication handoff releases on converged owner publication",
            "managed dense publication handoff releases when its incarnation is superseded",
            "hosted index lifecycle callback hands exact target to persistent control plane",
            "committed activation receipt rejects delayed drop after newer create",
            "committed activation receipt rejects delayed create after newer drop and coalesces exact duplicate",
            "committed activation watermark stays bounded across completed scope churn",
            "resident activation journal rejects a delayed lower metadata epoch",
            "resident activation requeue cannot displace a newer queued epoch",
            "activation scheduler key includes complete storage identity",
            "whole table reconcile cannot absorb exact resident activation",
            "active structural key coalesces duplicate and requeues allocation free",
            "structural scheduler burst uses keyed coalescing and FIFO deadlines",
            "structural scheduler capacity includes active ownership",
            "yielded structural work advances under sustained new admission",
            "exact activation dual worker admission failure leaves no orphan",
            "duplicate activation cannot observe accepted before admission commits",
            "completed activation ring evicts oldest and records completion once",
            "discard retires unreferenced scope revisions under churn",
            "newer completed activation makes retained older result stale",
            "discarded newer activation cannot resurrect an older terminal result",
            "activation progress preserves every terminal failure class",
            "hosted index lifecycle owner retries transient activation failure",
            "structural activation dispatch does not convoy unrelated groups",
            "hosted index lifecycle backs off repeated stale resident observation",
            "hosted activation revalidates topology after final resident RPC",
            "hosted index lifecycle durable wake repairs worker admission failure",
            "structural reconcile retains ordered constant-time repair wake membership across plan resets",
            "resident DB retry preparation waits outside admission for writer publication",
            "resident DB retry preparation does not block a borrowed std.Io scheduler",
            "write cache pending closes yield to the borrowed scheduler",
            "admitted resident DB lease never waits for an in-flight writer publication",
            "write cache local mutation preempts stale startup writer",
            "structural reconcile pending set never revisits completed groups",
            "structural reconcile completion rejects ranges added after contract capture",
            "structural reconcile production topology fence rejects ranges added after capture",
            "structural reconcile production catalog fails closed without table publication fence",
            "structural reconcile fences incarnation initialization and discards empty topology",
            "provisioned structural reconcile blocks table write admission",
            "provisioned source quiesce closes cleanup admission and drains accepted owner jobs",
            "provisioned source quiesce retains activity until borrowed transaction callbacks drain",
            "provisioned schema reconcile keeps reads and status available",
            "busy startup open preserves fresh writer runtime status",
            "managed startup catch-up marks FileNotFound index open terminal degraded",
            "managed startup catch-up preserves restore repair debt while index load is terminal",
            "managed startup catch-up defers while shared bulk ingest state is active",
            "clean generated startup inspection does not retain a resident writer",
            "managed startup catch-up retains resolver-only recovery after reopen",
            "managed startup catch-up retains cursor-only resolver recovery on workerless reopen",
            "managed startup catch-up allocation failure preserves bounded retry",
            "managed startup catch-up quarantines repeated zero progress with bounded backoff",
            "standby HA replay reconciles managed indexes without opening the public write gate",
            "cold replicated apply preserves declared full text projection across retained reopen",
            "managed structural catch-up delegates durable generation repair without rebuilding inline",
            "managed structural catch-up leaves pending enrichment with the asynchronous owner",
            "managed structural catch-up does not delegate an empty producer handoff",
            "standalone managed structural catch-up owns admitted enrichment progress",
            "managed catch-up reaches durable generation repair when dense replay needs an artifact rebuild",
            "managed create publication handoff ignores unrelated index debt",
            "db managed vector admission captures writes while durable repair is pending",
            "db managed repair scheduler defers canonical worker until shadow activation",
            "index repair inspection window is bounded and rotates fairly",
            "resident index repair scheduler skips deferred prefixes with bounded fair quanta",
            "resident index repair scheduler maintains exact aggregate wake precedence",
            "resident index repair progress waits are revision scoped and event driven",
            "index repair intent string replacement is allocation failure safe",
            "quarantine binding reconciliation serializes with terminal transition",
            "rollback predecessor retry policy preserves automatic recovery boundaries",
            "db missing activation certification rolls back to serviceable dense predecessor",
            "db missing activation certification exposes predecessor action required",
            "db failed activated dense generation rolls back to retained predecessor",
            "repair admission revisions stay fail closed and reject delayed publishers",
            "db progressive managed admission serves a checkpointed partial generation",
            "db completed managed admission emits an initial build clear edge",
            "db removing one repair pin preserves pressure gate for another index",
            "db dense replay failure upgrades a preflight validation intent",
            "db durable repair classification emits exact admission and action edges",
            "db managed algebraic admission builds and reopens",
            "db algebraic generation build yields and resumes from its durable source cursor",
            "db forced algebraic repair persists an operator generation intent before execution",
            "db algebraic post-commit activation crash recovers through generation repair",
            "table provisioner admits algebraic index on a non-empty table through generation repair",
            "structural reconcile reconfigures retained writer before managed dense writes",
            "target index reconciliation never mutates sibling indexes",
            "target index reconciliation does not wait for sibling storage maintenance",
            "target index reconciliation retires orphaned inline enrichments after deletion retry",
            "replica root reconcile enqueues newly admitted managed full text repair",
            "managed repair visibility edges retire cached readers and runtime status",
            "repair visibility progress does not churn readers without an admission edge",
            "table runtime snapshot cache invalidation fences a stale observed publisher",
            "maintenance runtime status preserves source observation without inventing targets",
            "targeted structural publication cannot regress an untouched sibling generation",
            "runtime status hook orders completed observation without crossing invalidation",
            "provisioned owner publication advances exact index replay target",
            "structural repair publication advances the table lifecycle epoch",
            "targeted repair publication preserves sibling authority fence",
            "status publication fence does not suppress structural work re-drive",
            "table runtime snapshot cache live publication does not starve structural refresh",
            "runtime owner retirement preserves serving snapshot and fences convergence",
            "failed exact handoff accepts identity without fabricating serving authority",
            "target authority uses bound catalog groups instead of cached group count",
            "synthetic refresh preserves post-fence target facts before serving handoff",
            "read mirror allocation failure cannot attach replacement authority to predecessor identity",
            "background refresh completes exact multi-group target handoff incrementally",
            "background refresh exact target allocation failure is atomic and leak free",
            "background refresh completes absent multi-group target handoff incrementally",
            "table runtime snapshot cache preserves live completion over regressing persisted projection",
            "catching up observation preserves same-incarnation published visibility",
            "catching up observation cannot preserve a same-config replacement incarnation",
            "catching up observation cannot preserve across an lsm root change",
            "unpublished embeddings incarnation cannot mint catch up serviceability",
            "empty embeddings incarnation preserves serviceability during catch up",
            "synthetic relabel cannot reuse cached catch up serviceability",
            "all-skipped embeddings incarnation preserves logical publication during catch up",
            "table runtime snapshot cache table fences isolate unrelated invalidations",
            "runtime status cache publishes unaffected tables and retries only invalidated tables",
            "runtime status cache stable absence removal retires the old table epoch",
            "group read payload preparation and retirement stay off the global cache mutex",
            "blocked read payload preparation does not convoy an unrelated table",
            "blocked table refresh preparation does not convoy unrelated publication",
            "activation failure projection preserves every stable failure code",
            "terminal activation failure precedes and binds its exact catalog row",
            "terminal drop failure never acknowledges absence and remains actionable",
            "terminal failure result distinguishes supersession and reserved storage",
            "incremental group acknowledgement sync is linear with one global handoff",
            "provisioned named index repair keeps group queued for aggregate debt audit",
            "dirty table tracking stays bounded to writer cache ownership",
            "writer cache eviction retires dirty ownership after the last cache owner",
            "prepared writer open evicts an inactive sibling group before retrying descriptor pressure",
            "prepared writer open reclaims descriptor capacity from startup cache",
            "forwarded write sources use the local writer owner dirty lifecycle",
            "writer cache distinguishes borrowed publisher callbacks and synchronization",
            "HA ownership transition invalidates cached visibility and dirty identities",
            "HA ownership transition serializes with active writer cache mutation",
            "HA seed request admission drains accepted writes and closes the preflight race",
            "startup cache clear retires dirty identity without a serving owner",
            "dirty auto bulk writer publishes runtime status without closing the cached writer",
            "auto bulk max-window request waits for idle finish",
            "split transition auto bulk publication retries while a writer lease is active",
            "median key lookup reuses startup writer instead of reopening its root",
            "write cache retirement is allocation-free after entry installation",
            "write cache transition locks use stable cache roles instead of addresses",
            "provider shutdown barrier closes cached dbs and remains idempotent",
            "provider shutdown barrier joins an in-flight generated embedding call",
            "writer cache metric pin batch release compacts retired entries once",
            "writer cache bulk transition fences only its table",
            "db runtime relabel cannot reuse cached index serviceability",
            "provisioned read cache retirement is allocation-free after entry installation",
            "provisioned group storage prunes stale visible root generations",
            "provisioned Raft snapshot install publishes a fenced group generation",
            "provisioned Raft snapshot install rejects a changed catalog contract",
            "prepared generation publication rolls back before serving admission",
            "prepared generation reconciliation rolls back an exchanged candidate",
            "prepared first generation reconciliation removes an unvalidated candidate",
            "committed generation reconciliation preserves the validated candidate",
            "generation publication marker parsing preserves allocator exhaustion",
            "manual generation runtime uses an explicit filesystem io authority",
            "provisioned table write source borrows runtime IO without native fallback",
        },
    }, write_implementation_tests);
    const run_api_table_writes_production_regression_tests = api_table_writes_production_regression_tests.run(b);
    const restore_provisioning_tests = @import("linked_tests.zig").addPair(b, .{
        .name = "api-restore-provisioning-tests",
        .root_module = api_table_writes_docid_test_mod,
        .filters = &.{"restore staging private provisioning"},
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    }, write_implementation_tests);
    b.step("antfly-api-restore-provisioning-test", "Run private hidden owner reservation and writer-cache admission").dependOn(&restore_provisioning_tests.run(b).step);
    const hosted_batch_tests = @import("linked_tests.zig").addPair(b, .{
        .name = "api-hosted-batch-tests",
        .root_module = api_table_writes_docid_test_mod,
        .filters = &.{
            "hosted remote batch prefers routed Raft protocol with safe legacy fallback",
            "hosted ambiguous batch preserves the remote transaction identity in the public response",
            "range tracking activation uses a fenced owner-routed Raft command",
        },
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    }, write_implementation_tests);
    // The public response fixture includes the HTTP server. Compile its
    // consumer after the implementation owner to keep the two broad closures
    // from occupying the qualification container concurrently.
    hosted_batch_tests.consumer.object.step.dependOn(&write_implementation_tests.step);
    b.step("antfly-api-hosted-batch-test", "Run hosted batch forwarding authority and canonical wire regressions").dependOn(&hosted_batch_tests.run(b).step);
    const run_api_table_writes_production_regression_unit_tests = api_table_writes_production_regression_tests.run(b);
    // These stateful suites each open several DB/index runtimes. Keep their
    // aggregate-gate runs on one lane so bounded CI hosts do not convert
    // aggregate memory pressure into allocator failures. Focused aliases use
    // the independent run artifacts above.
    run_api_table_writes_production_regression_unit_tests.step.dependOn(&run_public_api_parity_aggregate_tests.step);
    const api_table_writes_production_regression_step = b.step("antfly-api-table-writes-lifecycle-test", "Run focused restore and writer-cache lifecycle regressions");
    api_table_writes_production_regression_step.dependOn(&run_api_table_writes_production_regression_tests.step);
    const replica_retirement_batch_tests = @import("linked_tests.zig").addPair(b, .{
        .name = "api-replica-retirement-batch-tests",
        .root_module = api_table_writes_docid_test_mod,
        .filters = &.{
            "replica retirement journal batches preserve every group phase",
            "replica retirement journal distinguishes active retained and committed removal",
            "private initial child retirement requires canceled local publication",
        },
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    }, write_implementation_tests);
    b.step("antfly-api-replica-retirement-batch-test", "Run the durable replica-retirement batch decoder regression").dependOn(&replica_retirement_batch_tests.run(b).step);
    const api_create_structural_retry_tests = @import("linked_tests.zig").addPair(b, .{
        .name = "api-table-create-retry-tests",
        .root_module = api_table_writes_docid_test_mod,
        .filters = &.{
            "provisioned table write source create table provisions local indexes and schema",
            "provisioned create retries a retired cache lease before structural publication",
        },
        .test_runner = .{
            .path = b.path("pkg/antfly-embedded/src/test_runner.zig"),
            .mode = .simple,
        },
    }, write_implementation_tests);
    const run_api_create_structural_retry_tests = api_create_structural_retry_tests.run(b);
    const api_create_structural_retry_step = b.step(
        "antfly-api-create-structural-retry-test",
        "Run isolated create metadata and structural-publication regressions",
    );
    api_create_structural_retry_step.dependOn(&run_api_create_structural_retry_tests.step);
    const api_table_writes_restore_repeat_tests = @import("linked_tests.zig").addPair(b, .{
        .name = "api-table-restore-repeat-tests",
        .root_module = api_table_writes_docid_test_mod,
        .filters = &.{
            "provisioned native backup restore repeats through shared read and write owners",
            "provisioned table restore retry repairs exact incomplete restore state through active writer",
            "managed native restore repair retains target backend admission for staged open",
            "native backup reclaims crash-left snapshot attempts from durable markers",
            "native backup reclaims a crash marker before snapshot root creation",
            "native backup never reclaims an old attempt with a live lease",
        },
        .test_runner = .{
            .path = b.path("pkg/antfly-embedded/src/test_runner.zig"),
            .mode = .simple,
        },
    }, write_implementation_tests);
    const run_api_table_writes_restore_repeat_tests = api_table_writes_restore_repeat_tests.run(b);
    const api_table_writes_restore_repeat_step = b.step("antfly-api-table-writes-restore-repeat-test", "Run focused native restore identity and shared-owner regressions");
    api_table_writes_restore_repeat_step.dependOn(&run_api_table_writes_restore_repeat_tests.step);
    const lib_docid_lifecycle_runtime_filters: []const []const u8 = &.{
        "metadata reconciler does not automatically split ordinal exhausted doc identity",
        "metadata state classifies mixed-version doc identity lifecycle reports",
        "metadata state marks doc identity rebuild required on range namespace mismatch",
        "metadata merge validation handles rolling mixed-version doc identity status fixtures",
        "metadata split request validation rejects stale doc identity namespace",
        "metadata http server rejects split and merge during active doc identity reassignment before source mutation",
        "table workflow doc identity guards reject active transition intents",
        "metadata reconciler doc identity guards block new planning during active reassignment",
        "metadata reconciler does not upsert desired split with stale doc identity namespace",
        "metadata reconciler allows explicit merge with doc identity reassignment opt-in",
        "distributed join follow-up pagination requires stamped identity request",
        "distributed join group-local hit pagination reuses structured search generation",
        "distributed join rejects doc identity rebuild before right-table fanout",
        "distributed join stateful shuffle rejects doc identity rebuild before worker dispatch",
        "distributed graph rejects doc identity rebuild before cross-range fanout",
        "distributed graph rejects unstamped result refs before cross-range fanout",
        "api distributed graph hydrate carries identity generation and clears cross-range ordinals",
        "internal worker doc identity exchange audit covers every boundary",
        "aggregation context rejects non-current identity generation",
        "aggregation full-result rerun can reuse snapped result identity generation",
        "explicit text stats requests preserve identity generation",
        "explicit text stats requests reject stale identity generation",
        "structured filter doc set cache separates shared namespace generation keys",
        "cache invalidates ownership move prefix without reviving pinned generations",
        "db text compaction preserves ordinal filters across reopen",
        "db lsm primary compaction preserves doc identity ordinals",
        "db allocates final document ordinal with all index families present",
        "identity namespace reassignment preserves snapshot generations and rejects stale writers",
        "near-u32 ordinal pressure preserves sparse high ordinal state through reassignment",
        "index manager split handoff preserves interleaved write and query summaries",
        "db stats flag document identity ordinal capacity exhaustion",
        "db rejects new document writes at ordinal exhaustion for every sync level",
        "db transaction intent writes reject new documents at ordinal exhaustion",
        "db search requests default to current identity generation snapshot",
        "db validates internal resolved doc filter wire namespace and generation",
        "db resolved doc-set projection honors identity read generation",
        "doc filter wire rejects old required-field fixtures but tolerates additive fields",
        "doc filter wire rejects invalid ordinal fixtures from mixed-version senders",
    };
    const lib_docid_lifecycle_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        // The API anchor exposes lazily imported join and table-read tests.
        // Keep it compile-only so the lifecycle selection remains unchanged.
        .filters = compileFiltersWithAnchors(b, &.{"api module compiles"}, lib_docid_lifecycle_runtime_filters),
        .test_runner = .{
            .path = b.path("pkg/antfly-embedded/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lib_docid_lifecycle_tests = addFilteredTestRunArtifactWithRuntimeFilters(
        b,
        lib_docid_lifecycle_tests,
        lib_docid_lifecycle_runtime_filters,
    );

    const api_backup_restore_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/api_backup_restore_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, api_backup_restore_test_mod, true, true);
    api_backup_restore_test_mod.addImport("antfly_openapi_specs", antfly_imports.embedded_openapi);
    const hosted_fk_placement_tests = b.addTest(.{
        .root_module = api_backup_restore_test_mod,
        .filters = &.{"hosted relational parent placement opens a real Raft owner"},
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    b.step("antfly-api-hosted-fk-placement-test", "Run mounted metadata and relational parent owner placement regression")
        .dependOn(&addFilteredTestRunArtifact(b, hosted_fk_placement_tests).step);
    const sql_rewrite_receipt_tests = b.addTest(.{
        .root_module = api_backup_restore_test_mod,
        .filters = &.{ "SQL DDL readiness retries transient observations without replaying the declaration", "SQL rewrite response retains admitted and uncertain restore handles", "SQL DDL receipt status distinguishes unknown admission", "SQL UUID prepared CREATE TABLE commits and replays catalog topology only on execute", "relational UUID preparation canonicalizes before durable row and index extraction", "relational UUID ingress shares canonical typed bytes and semantic hash", "initial foreign-key create distinguishes unknown admission from accepted work", "primary key schema lowering rejects deferred timing and empty keys", "relational primary keys cannot defer enforcement", "restore immutable rewrite failures are terminal but transport pressure is retryable", "httpx restore owner accepts bounded rewrite source chunks above legacy control limit" },
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    b.step("antfly-api-sql-rewrite-receipt-test", "Run SQL rewrite receipt and unknown-admission response regression")
        .dependOn(&addFilteredTestRunArtifact(b, sql_rewrite_receipt_tests).step);
    const sql_uuid_catalog_tests = b.addTest(.{
        .root_module = api_backup_restore_test_mod,
        .filters = &.{"SQL UUID prepared CREATE TABLE commits and replays catalog topology only on execute"},
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    b.step("antfly-api-sql-uuid-catalog-test", "Run prepared SQL UUID catalog and owner publication regression")
        .dependOn(&addFilteredTestRunArtifact(b, sql_uuid_catalog_tests).step);
    const pk_failure_abi_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/runtime_failure_abi.zig"),
        .target = target,
        .optimize = optimize,
    });
    const pk_failure_identity_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/runtime_failure_identity.zig"),
        .target = target,
        .optimize = optimize,
    });
    pk_failure_identity_mod.addImport("runtime_failure_abi", pk_failure_abi_mod);
    const pk_failure_identity_tests = b.addTest(.{
        .root_module = pk_failure_identity_mod,
        .filters = &.{"registered storage-kernel errors are unique and round trip without losing identity"},
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    b.step("antfly-api-pk-restore-error-identity-test", "Run exact native invalid-row restore error identity regression")
        .dependOn(&addFilteredTestRunArtifact(b, pk_failure_identity_tests).step);
    const sql_primary_key_rewrite_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/api_sql_primary_key_rewrite_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configureConsumer(b, sql_primary_key_rewrite_test_mod);
    @import("../../../build_support/embedded/storage.zig").configureLmdb(b, sql_primary_key_rewrite_test_mod, options.lmdb_engine, true);
    sql_primary_key_rewrite_test_mod.addImport("antfly_admin_openapi", antfly_imports.admin_openapi);
    sql_primary_key_rewrite_test_mod.addImport("antfly_internal_openapi", antfly_imports.internal_openapi);
    sql_primary_key_rewrite_test_mod.addImport("antfly_openapi_specs", antfly_imports.embedded_openapi);
    const pk_usermgr_storage = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/usermgr/storage_imports.zig"),
        .target = target,
        .optimize = optimize,
    });
    pk_usermgr_storage.addImport("antfly_root", sql_primary_key_rewrite_test_mod);
    pk_usermgr_storage.addImport("antfly_platform", antfly_imports.platform);
    sql_primary_key_rewrite_test_mod.addImport("usermgr_storage", pk_usermgr_storage);
    const sql_primary_key_rewrite_tests = b.addTest(.{
        .root_module = sql_primary_key_rewrite_test_mod,
        .filters = &.{"mounted SQL ADD PRIMARY KEY publishes only validated fresh generation"},
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    b.step("antfly-api-sql-primary-key-rewrite-test", "Run mounted SQL primary-key rewrite publication and failure cases")
        .dependOn(&addFilteredTestRunArtifact(b, sql_primary_key_rewrite_tests).step);
    const hosted_initial_fk_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/api_hosted_initial_fk_test_root.zig"),
        .target = target,
        .optimize = optimize,
        .strip = false,
    });
    test_imports.configureConsumer(b, hosted_initial_fk_test_mod);
    @import("../../../build_support/embedded/storage.zig").configureLmdb(b, hosted_initial_fk_test_mod, options.lmdb_engine, true);
    hosted_initial_fk_test_mod.addImport("antfly_admin_openapi", antfly_imports.admin_openapi);
    hosted_initial_fk_test_mod.addImport("antfly_internal_openapi", antfly_imports.internal_openapi);
    hosted_initial_fk_test_mod.addImport("antfly_openapi_specs", antfly_imports.embedded_openapi);
    const hosted_fk_usermgr_storage = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/usermgr/storage_imports.zig"),
        .target = target,
        .optimize = optimize,
        .strip = false,
    });
    hosted_fk_usermgr_storage.addImport("antfly_root", hosted_initial_fk_test_mod);
    hosted_fk_usermgr_storage.addImport("antfly_platform", antfly_imports.platform);
    hosted_initial_fk_test_mod.addImport("usermgr_storage", hosted_fk_usermgr_storage);
    const hosted_initial_fk_tests = b.addTest(.{
        .root_module = hosted_initial_fk_test_mod,
        .filters = &.{ "FK plan table and range descriptors survive snapshot release", "initial FK plan waits for all external parent schema migrations", "mounted initial MATCH PARTIAL publication enforces constraints across restart", "hosted initial FK real services and facade retain all required capabilities" },
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    b.step("antfly-api-hosted-initial-fk-test", "Run mounted hosted initial-FK owner publication regression")
        .dependOn(&addFilteredTestRunArtifact(b, hosted_initial_fk_tests).step);
    const hosted_initial_fk_fault_tests = b.addTest(.{
        .root_module = hosted_initial_fk_test_mod,
        .filters = &.{ "mounted initial FK lost replies and cold owner restart before release", "hosted initial FK real services and facade retain all required capabilities" },
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    b.step("antfly-api-hosted-initial-fk-fault-test", "Run hosted initial CREATE lost-reply and cold owner release recovery")
        .dependOn(&addFilteredTestRunArtifact(b, hosted_initial_fk_fault_tests).step);
    const hosted_initial_fk_transfer_tests = b.addTest(.{
        .root_module = hosted_initial_fk_test_mod,
        .filters = &.{ "mounted initial FK hidden three voter owner transfers before release", "hosted initial FK real services and facade retain all required capabilities" },
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    b.step("antfly-api-hosted-initial-fk-transfer-test", "Run hidden initial-child three-voter Raft leadership transfer")
        .dependOn(&addFilteredTestRunArtifact(b, hosted_initial_fk_transfer_tests).step);
    const hosted_initial_fk_offline_tests = b.addTest(.{
        .root_module = hosted_initial_fk_test_mod,
        .filters = &.{ "mounted initial FK canceled offline replica returns and signs exact unlink ACK", "mounted initial FK published obsolete released replica retires without canceling current owner", "hosted initial FK real services and facade retain all required capabilities" },
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    b.step("antfly-api-hosted-initial-fk-offline-test", "Run canceled and published obsolete initial-child offline physical retirement")
        .dependOn(&addFilteredTestRunArtifact(b, hosted_initial_fk_offline_tests).step);
    // Recovery selections belong to the native Zig test inventory.
    const hosted_initial_fk_recovery_tests = b.addTest(.{
        .name = "antfly-hosted-initial-fk-recovery",
        .max_rss = @import("runtime_memory.zig").testCompileMaxRss("antfly-hosted-initial-fk-recovery", b.graph.host.result, target.result, optimize, 0),
        .root_module = hosted_initial_fk_test_mod,
        .filters = &.{ "mounted initial", "hosted initial FK real services and facade retain all required capabilities", "FK plan table and range descriptors survive snapshot release", "initial FK plan waits for all external parent schema migrations" },
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    const hosted_fk_drop_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/api_hosted_fk_drop_test_root.zig"),
        .target = target,
        .optimize = optimize,
        .strip = false,
    });
    test_imports.configureConsumer(b, hosted_fk_drop_test_mod);
    @import("../../../build_support/embedded/storage.zig").configureLmdb(b, hosted_fk_drop_test_mod, options.lmdb_engine, true);
    hosted_fk_drop_test_mod.addImport("antfly_admin_openapi", antfly_imports.admin_openapi);
    hosted_fk_drop_test_mod.addImport("antfly_internal_openapi", antfly_imports.internal_openapi);
    hosted_fk_drop_test_mod.addImport("antfly_openapi_specs", antfly_imports.embedded_openapi);
    const hosted_fk_drop_usermgr_storage = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/usermgr/storage_imports.zig"),
        .target = target,
        .optimize = optimize,
        .strip = false,
    });
    hosted_fk_drop_usermgr_storage.addImport("antfly_root", hosted_fk_drop_test_mod);
    hosted_fk_drop_usermgr_storage.addImport("antfly_platform", antfly_imports.platform);
    hosted_fk_drop_test_mod.addImport("usermgr_storage", hosted_fk_drop_usermgr_storage);
    const hosted_fk_drop_tests = b.addTest(.{
        .root_module = hosted_fk_drop_test_mod,
        .filters = &.{ "mounted hosted external-parent FK DROP publishes after parent ACK", "mounted hosted external-parent FK TRUNCATE", "owner catalog busy without index debt keeps provisioning retry until full reconciliation" },
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    b.step("antfly-api-hosted-fk-drop-test", "Run mounted external-parent FK DROP retirement and restart")
        .dependOn(&addFilteredTestRunArtifact(b, hosted_fk_drop_tests).step);
    b.step("antfly-api-hosted-fk-truncate-test", "Run mounted external-parent FK generation retirement and restart")
        .dependOn(&addFilteredTestRunArtifact(b, hosted_fk_drop_tests).step);
    const hosted_graph_truncate_tests = b.addTest(.{
        .root_module = hosted_fk_drop_test_mod,
        .filters = &.{"mounted hosted graph TRUNCATE"},
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    b.step("antfly-api-hosted-graph-truncate-test", "Run public graph TRUNCATE seal and cold-restart recovery")
        .dependOn(&addFilteredTestRunArtifact(b, hosted_graph_truncate_tests).step);
    const hosted_truncate_fk_recovery_tests = b.addTest(.{
        .name = "antfly-hosted-truncate-fk-recovery",
        .max_rss = @import("runtime_memory.zig").testCompileMaxRss("antfly-hosted-truncate-fk-recovery", b.graph.host.result, target.result, optimize, 0),
        .root_module = hosted_fk_drop_test_mod,
        .filters = &.{ "mounted hosted external-parent FK DROP publishes after parent ACK", "mounted hosted external-parent FK TRUNCATE", "mounted hosted graph TRUNCATE", "owner catalog busy without index debt keeps provisioning retry until full reconciliation" },
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    const hosted_self_fk_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/api_hosted_self_fk_test_root.zig"),
        .target = target,
        .optimize = optimize,
        .strip = false,
    });
    test_imports.configureConsumer(b, hosted_self_fk_test_mod);
    @import("../../../build_support/embedded/storage.zig").configureLmdb(b, hosted_self_fk_test_mod, options.lmdb_engine, true);
    hosted_self_fk_test_mod.addImport("antfly_admin_openapi", antfly_imports.admin_openapi);
    hosted_self_fk_test_mod.addImport("antfly_internal_openapi", antfly_imports.internal_openapi);
    hosted_self_fk_test_mod.addImport("antfly_openapi_specs", antfly_imports.embedded_openapi);
    const hosted_self_fk_usermgr_storage = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/usermgr/storage_imports.zig"),
        .target = target,
        .optimize = optimize,
        .strip = false,
    });
    hosted_self_fk_usermgr_storage.addImport("antfly_root", hosted_self_fk_test_mod);
    hosted_self_fk_usermgr_storage.addImport("antfly_platform", antfly_imports.platform);
    hosted_self_fk_test_mod.addImport("usermgr_storage", hosted_self_fk_usermgr_storage);
    const hosted_self_fk_retry_tests = b.addTest(.{
        .root_module = hosted_self_fk_test_mod,
        .filters = &.{"self-FK retries only a proven durable precommit abort"},
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    b.step("antfly-api-hosted-self-fk-retry-test", "Run self-FK proven durable precommit retry regression")
        .dependOn(&addFilteredTestRunArtifact(b, hosted_self_fk_retry_tests).step);
    const hosted_self_fk_diagnostic_tests = b.addTest(.{
        .root_module = hosted_self_fk_test_mod,
        .filters = &.{"mounted hosted self-FK ADD DROP restart"},
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    b.step("antfly-api-hosted-self-fk-test", "Run mounted public self-FK ADD/DROP/restart")
        .dependOn(&addFilteredTestRunArtifact(b, hosted_self_fk_diagnostic_tests).step);
    const hosted_cte_runtime_tests = b.addTest(.{
        .root_module = hosted_self_fk_test_mod,
        .filters = &.{ "mounted hosted prepared CTE mutations and recursive read retain owner fences", "hosted CTE MERGE retries only proven precommit read unavailability", "SQL pgwire MERGE completion reports committed affected rows", "SQL unknown mutation keeps native reconciliation receipt without allocation", "public table batch handler maps write unavailable errors" },
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    b.step("antfly-api-hosted-cte-runtime-test", "Run linked hosted prepared CTE and recursive owner-snapshot regression")
        .dependOn(&addFilteredTestRunArtifact(b, hosted_cte_runtime_tests).step);
    const hosted_self_fk_fault_tests = b.addTest(.{
        .root_module = hosted_self_fk_test_mod,
        .filters = &.{"mounted hosted self-FK publication resumes after lost owner and metadata replies"},
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    b.step("antfly-api-hosted-self-fk-fault-test", "Run mounted self-FK publication lost-reply recovery")
        .dependOn(&addFilteredTestRunArtifact(b, hosted_self_fk_fault_tests).step);
    const hosted_self_fk_metadata_restart_tests = b.addTest(.{
        .root_module = hosted_self_fk_test_mod,
        .filters = &.{"mounted hosted self-FK resumes after metadata and owner cold restart at parent ACK"},
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    b.step("antfly-api-hosted-self-fk-metadata-restart-test", "Run mounted self-FK metadata and owner cold restart at parent ACK")
        .dependOn(&addFilteredTestRunArtifact(b, hosted_self_fk_metadata_restart_tests).step);
    const hosted_self_fk_leader_transfer_tests = b.addTest(.{
        .root_module = hosted_self_fk_test_mod,
        .filters = &.{"mounted hosted self-FK survives three-voter owner leadership transfer at ADD and DROP ACK"},
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    b.step("antfly-api-hosted-self-fk-leader-transfer-test", "Run mounted three-voter self-FK ADD/DROP owner leadership transfer")
        .dependOn(&addFilteredTestRunArtifact(b, hosted_self_fk_leader_transfer_tests).step);
    const hosted_self_fk_recovery_tests = b.addTest(.{
        .name = "antfly-hosted-self-fk-recovery",
        .max_rss = @import("runtime_memory.zig").testCompileMaxRss("antfly-hosted-self-fk-recovery", b.graph.host.result, target.result, optimize, 0),
        .root_module = hosted_self_fk_test_mod,
        // Exact positive proofs: never include the old guard-on assertions.
        .filters = &.{ "mounted hosted self-FK ADD DROP restart", "mounted hosted self-FK publication resumes after lost owner and metadata replies", "mounted hosted self-FK resumes after metadata and owner cold restart at parent ACK", "mounted hosted self-FK survives three-voter owner leadership transfer at ADD and DROP ACK" },
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    const hosted_recovery_tests = [_]*std.Build.Step.Compile{ hosted_initial_fk_recovery_tests, hosted_self_fk_recovery_tests, hosted_truncate_fk_recovery_tests };
    const hosted_recovery_step = b.step("antfly-api-hosted-recovery-test", "Run native hosted owner recovery integration tests");
    var previous_recovery: ?*std.Build.Step = null;
    for (hosted_recovery_tests) |tests| {
        const run = @import("../../../build_support/antfly/test_support.zig").addRequiredTestRunArtifact(b, tests, 15 * 60 * 1000);
        if (previous_recovery) |previous| run.step.dependOn(previous);
        previous_recovery = &run.step;
    }
    hosted_recovery_step.dependOn(previous_recovery.?);
    const lib_api_standalone_backup_restore_tests = b.addTest(.{
        .root_module = api_backup_restore_test_mod,
        .filters = &.{
            "public api standalone-like integration backs up drops and restores a table",
            "public table backup handler exposes non-retryable fenced outcomes",
            "api restore rollback preserves a concurrently replaced table definition",
            "api http server cluster overwrite restores from read-only repository without dropping live table",
            "api http server rejects an empty cluster backup without publishing a manifest",
            "backup manifest validation rejects ambiguous or unbound artifacts",
            "canonical repository file adapter streams blobs and compare-and-swaps refs",
            "remote repository coordinator fences a resumed stale owner",
            "repository remote verification accepts only full-object SHA-256 proofs",
            "backup manifest cancellation prevents late publication",
            "backup root publication cancellation leaves no visible control record",
            "unpublished table cleanup retains its retry address until writer state retires",
            "unpublished table cleanup preserves its reservation on writer owner mismatch",
            "unpublished table cleanup exposes bounded resumable progress",
            "stale table reclaim reports a concurrently replaced generation",
            "stale table reclaim honors cancellation before storage mutation",
            "table backup collision commit check is bounded and exact",
            "standalone stale reclaim bounds foreground native artifact deletion",
            "table backup retry preserves the retained ambiguous generation",
            "table backup retry reclaims an eligible reservation and admits the new generation",
            "table backup lease conflict retains the retry address and live writer fence",
            "backup maintenance target coalesces exact table reclaim intent",
            "table backup reclaim retry uses exact future eligibility",
            "cluster backup manifest rejects incomplete coverage",
            "restore source identities are bounded and canonical",
            "filesystem backup location returns the canonical authorized identity",
            "portable backup integrity rejects changed staged bytes",
            "native artifact copy observes cancellation between io chunks",
            "native backup directory copy preserves nested files",
            "db explicit restore runtime repair repairs managed chunked dense embeddings once for restored shard",
            "db incomplete deferred restore import recovers before runtime repair",
            "db restore state uses strict structured content identity markers",
            "restore job list paginates after authorization filtering",
            "restore job list bounds authorization scans with an empty continuation page",
            "restore job ownership failures remain retryable",
            "restore retry wakeup admission failure cannot escape as execution failure",
            "staged restore published metadata wins cancellation only after every owner opens",
            "staged restore worker publishes a dependency complete mixed native cohort",
            "staged restore worker rebuilds a dependency complete mixed portable cohort",
            "staged restore worker rewrites retained acknowledged writes and reopens hidden mixed owners",
            "restore admission unknown response preserves recovery",
            "restore worker authority is fenced across leadership reacquisition",
            "restore ownership backoff is interruptible without polling",
        },
        .test_runner = .{
            .path = b.path("pkg/antfly-embedded/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lib_api_standalone_backup_restore_tests = addFilteredTestRunArtifact(b, lib_api_standalone_backup_restore_tests);
    const staged_restore_driver_tests = b.addTest(.{
        .root_module = api_backup_restore_test_mod,
        .filters = &.{ "busy staged restore owner retains its pinned attempt", "restore cutover lost begin reply waits for the same fence to drain", "staged restore published metadata wins cancellation only after every owner opens", "staged restore worker publishes a dependency complete mixed native cohort", "staged restore worker rebuilds a dependency complete mixed portable cohort", "staged restore worker recovers lost acknowledgements across owner restart matrix", "staged restore worker preserves generated mixed cohorts across HTTP owner faults", "staged table restore worker uses shared dependency complete cohort", "staged restore worker preserves migration mappings and indexes after restart", "staged restore worker measures bounded LSM native and portable work" },
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    b.step("antfly-api-staged-restore-driver-test", "Run bounded staged restore publication and cancellation driver regressions").dependOn(&addFilteredTestRunArtifact(b, staged_restore_driver_tests).step);
    const staged_restore_proof_tests = b.addTest(.{
        .root_module = api_backup_restore_test_mod,
        .filters = &.{ "staged restore worker rebuilds a dependency complete mixed portable cohort", "staged table restore worker uses shared dependency complete cohort" },
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    b.step("antfly-api-staged-restore-proof-test", "Run portable accepted-generation and table-cohort restore regressions").dependOn(&addFilteredTestRunArtifact(b, staged_restore_proof_tests).step);
    const staged_restore_portable_fk_tests = b.addTest(.{
        .root_module = api_backup_restore_test_mod,
        .filters = &.{"staged restore worker rebuilds a dependency complete mixed portable cohort"},
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    b.step("antfly-api-staged-restore-portable-fk-test", "Run portable accepted-generation restore regression").dependOn(&addFilteredTestRunArtifact(b, staged_restore_portable_fk_tests).step);
    const portable_fk_restore_admission_tests = b.addTest(.{
        .root_module = api_backup_restore_test_mod,
        .filters = &.{ "public portable foreign-key restore rejects a new job without losing an exact retry", "portable foreign-key worker guard is terminal and not a corrupt archive" },
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    b.step("antfly-api-portable-fk-restore-admission-test", "Run fail-closed public portable FK restore admission regression").dependOn(&addFilteredTestRunArtifact(b, portable_fk_restore_admission_tests).step);
    const staged_restore_benchmark_tests = b.addTest(.{
        .root_module = api_backup_restore_test_mod,
        .filters = &.{"staged restore worker measures bounded LSM native and portable work"},
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    b.step("antfly-api-staged-restore-benchmark-test", "Measure bounded native and portable LSM restore work").dependOn(&addFilteredTestRunArtifact(b, staged_restore_benchmark_tests).step);
    const staged_restore_portable_terminal_tests = b.addTest(.{
        .root_module = api_backup_restore_test_mod,
        .filters = &.{"staged portable restore proves terminal correctness with a bounded extended budget"},
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    b.step("antfly-api-staged-restore-portable-terminal-test", "Prove large portable staged restore reaches durable publication").dependOn(&addFilteredTestRunArtifact(b, staged_restore_portable_terminal_tests).step);
    const rewrite_driver_tests = b.addTest(.{
        .root_module = api_backup_restore_test_mod,
        .filters = &.{ "rewrite shared job", "staged restore native handoff", "staged restore worker rewrites retained acknowledged writes and reopens hidden mixed owners", "restore immutable rewrite failures are terminal", "restore cutover readiness waits without exponential retry" },
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    b.step("antfly-api-rewrite-driver-test", "Run real retained-source rewrite, hidden owner recovery and failed-tail cancellation").dependOn(&addFilteredTestRunArtifact(b, rewrite_driver_tests).step);
    const generated_restore_tests = b.addTest(.{
        .root_module = api_backup_restore_test_mod,
        .filters = &.{"staged restore worker preserves generated mixed cohorts across HTTP owner faults"},
        .test_runner = .{ .path = b.path("pkg/antfly-embedded/src/test_runner.zig"), .mode = .simple },
    });
    b.step("antfly-api-generated-restore-test", "Run generated mixed-table restore through HTTP owner faults").dependOn(&addFilteredTestRunArtifact(b, generated_restore_tests).step);
    const lib_api_standalone_backup_restore_test_step = b.step("antfly-api-standalone-backup-restore-test", "Run the focused standalone-like backup/restore e2e test");
    lib_api_standalone_backup_restore_test_step.dependOn(&run_lib_api_standalone_backup_restore_tests.step);

    return .{
        .process_generation_fence_step = generation_step,
        .run_lib_api_standalone_backup_restore_tests = run_lib_api_standalone_backup_restore_tests,
        .distributed_query_availability_step = lib_api_distributed_query_availability_test_step,
        .run_public_api_parity_aggregate_tests = run_public_api_parity_aggregate_tests,
        .run_lib_api_auth_tests = run_lib_api_auth_tests,
        .run_algebraic_dynamic_template_tests = run_algebraic_dynamic_template_tests,
        .run_lib_api_connections_tests = run_lib_api_connections_tests,
        .run_lib_api_storage_authority_tests = run_lib_api_storage_authority_tests,
        .api_table_writes_docid_test_mod = api_table_writes_docid_test_mod,
        .write_implementation_tests = write_implementation_tests,
        .public_api_parity_tests = public_api_parity_tests,
        .api_table_reads_docid_test_mod = api_table_reads_docid_test_mod,
        .run_lib_api_docid_tests = run_lib_api_docid_tests,
        .api_derived_coverage_test_mod = api_derived_coverage_test_mod,
        .run_lib_api_derived_coverage_tests = run_lib_api_derived_coverage_tests,
        .run_lib_serverless_docid_tests = run_lib_serverless_docid_tests,
        .run_api_transactions_docid_tests = run_api_transactions_docid_tests,
        .run_api_table_writes_docid_tests = run_api_table_writes_docid_tests,
        .run_api_table_reads_docid_tests = run_api_table_reads_docid_tests,
        .hosted_recovery_tests = hosted_recovery_tests,
        .hosted_recovery_step = hosted_recovery_step,
        .linked_consumer_tests = b.allocator.dupe(*std.Build.Step.Compile, &.{ public_api_parity_tests, lib_api_auth_tests, api_table_reads_linked_tests.executable, lib_api_distributed_query_availability_tests.executable, api_table_writes_docid_tests.executable, api_relational_topology_contract_tests.consumer.executable, api_table_writes_production_regression_tests.consumer.executable, hosted_batch_tests.consumer.executable, api_create_structural_retry_tests.consumer.executable, api_table_writes_restore_repeat_tests.consumer.executable, hosted_fk_placement_tests, sql_primary_key_rewrite_tests, hosted_initial_fk_tests, hosted_initial_fk_fault_tests, hosted_initial_fk_transfer_tests, hosted_initial_fk_offline_tests, hosted_initial_fk_recovery_tests, hosted_fk_drop_tests, hosted_graph_truncate_tests, hosted_truncate_fk_recovery_tests, hosted_self_fk_retry_tests, hosted_self_fk_diagnostic_tests, hosted_self_fk_fault_tests, hosted_self_fk_metadata_restart_tests, hosted_self_fk_leader_transfer_tests, hosted_self_fk_recovery_tests }) catch @panic("OOM"),
        .run_api_public_table_http_docid_tests = run_api_public_table_http_docid_tests,
        .run_raft_transition_runtime_docid_tests = run_raft_transition_runtime_docid_tests,
        .run_api_table_writes_production_regression_unit_tests = run_api_table_writes_production_regression_unit_tests,
        .run_lib_docid_lifecycle_tests = run_lib_docid_lifecycle_tests,
    };
}
