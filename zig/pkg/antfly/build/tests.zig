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
const api_tests = @import("api_tests.zig");
const metadata_tests = @import("metadata_tests.zig");
const db_tests = @import("db_tests.zig");
const data_tests = @import("data_tests.zig");
const build_test_filters = @import("../../../build_test_filters.zig");
pub const chainLabeledRun = @import("test_support.zig").chainLabeledRun;
pub const chainLabeledRunStep = @import("test_support.zig").chainLabeledRunStep;
pub const chainLabeledFilteredTests = @import("test_support.zig").chainLabeledFilteredTests;
pub const selectTestFilters = @import("test_support.zig").selectTestFilters;
pub const labelTestRuns = @import("test_support.zig").labelTestRuns;
pub const dependOnAll = @import("test_support.zig").dependOnAll;
pub const assignDefaultAggregateMaxRss = @import("test_support.zig").assignDefaultAggregateMaxRss;
pub const assignDefaultAggregateMaxRssRecursive = @import("test_support.zig").assignDefaultAggregateMaxRssRecursive;
pub const addRuntimeTestFilters = @import("test_support.zig").addRuntimeTestFilters;
pub const addRuntimeSkipTestFilters = @import("test_support.zig").addRuntimeSkipTestFilters;
pub const configureUnitStorageTestRun = @import("test_support.zig").configureUnitStorageTestRun;
pub const compileFiltersWithAnchors = @import("test_support.zig").compileFiltersWithAnchors;
pub const addAntflyTestRunArtifact = @import("test_support.zig").addAntflyTestRunArtifact;
pub const addFilteredTestRunArtifactWithRuntimeFilters = @import("test_support.zig").addFilteredTestRunArtifactWithRuntimeFilters;
pub const addFilteredTestRunArtifact = @import("test_support.zig").addFilteredTestRunArtifact;
pub const addCuratedTestRunArtifact = @import("test_support.zig").addCuratedTestRunArtifact;
pub const expectQuietSuccess = @import("test_support.zig").expectQuietSuccess;
pub const release_scale_test_filters = @import("test_support.zig").release_scale_test_filters;
const addSnowballModule = @import("snowball.zig").addSnowballModule;
const makeLmdbBuildOptions = @import("storage.zig").makeLmdbBuildOptions;
const makeLmdbEngineModule = @import("storage.zig").makeLmdbEngineModule;
const makeLmdbModule = @import("storage.zig").makeLmdbModule;

const AntflyRootImports = @import("imports.zig").AntflyRootImports;
const LmdbBackend = @import("storage.zig").LmdbBackend;

pub const AddTestsOptions = struct {
    vopr: *std.Build.Module,
    lmdb_engine: *std.Build.Module,
    optimize: std.builtin.OptimizeMode,
    lmdb_backend: LmdbBackend,
    lmdb_evented_async_io: bool,
    standalone_runtime_build_options: *std.Build.Step.Options,
    openapi_root_check: *std.Build.Step.Run,
    usermgr_mod: *std.Build.Module,
    antfly_imports: AntflyRootImports,
    antfly_mod: *std.Build.Module,
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
    run_capi_tests: *std.Build.Step.Run,
    run_raft_library_tests: *std.Build.Step.Run,
};
pub const AddTestsResult = struct {
    vopr_soak_test_step: *std.Build.Step,
    storage_workload_soak_step: *std.Build.Step,
    antfly_test_mod: *std.Build.Module,
    run_antfly_embedded_pkg_tests: *std.Build.Step.Run,
    run_lite_native_tests: *std.Build.Step.Run,
    run_lite_cmd_tests: *std.Build.Step.Run,
    run_lib_ha_compat_tests: *std.Build.Step.Run,
    antfly_test_step: *std.Build.Step,
    unit_test_step: *std.Build.Step,
    standalone_runtime_test_step: *std.Build.Step,
    vopr_test_step: *std.Build.Step,
    integration_test_step: *std.Build.Step,
    chaos_test_step: *std.Build.Step,
    compiled_recall_tests: *std.Build.Step.Compile,
};

pub fn addTests(b: *std.Build, options: AddTestsOptions) AddTestsResult {
    const target = options.antfly_imports.platform_target;
    const optimize = options.optimize;
    const production_vopr_compile_max_rss = @import("test_support.zig").productionVoprCompileMaxRss(target);
    const lmdb_backend = options.lmdb_backend;
    const lmdb_evented_async_io = options.lmdb_evented_async_io;
    const build_options = options.antfly_imports.build_options;
    const standalone_runtime_build_options = options.standalone_runtime_build_options;
    const lmdb_engine_mod = options.lmdb_engine;
    const raft_engine_mod = options.antfly_imports.raft_engine;
    const httpx_mod = options.antfly_imports.httpx;
    const structlog_mod = options.antfly_imports.structlog;
    const openapi_root_check = options.openapi_root_check;
    const handlebars_mod = options.antfly_imports.handlebars;
    const platform_mod = options.antfly_imports.platform;
    const bloom_mod = options.antfly_imports.bloom;
    const vector_mod = options.antfly_imports.vector;
    const hash_mod = options.antfly_imports.hash;
    const vectorindex_mod = options.antfly_imports.vectorindex;
    const usermgr_mod = options.usermgr_mod;
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
    const inference_api_mod = options.antfly_imports.inference_api;
    const inference_chunker_mod = options.antfly_imports.inference_chunker;
    const reader_config_mod = options.antfly_imports.reader_config;
    const antfly_imports = options.antfly_imports;
    const test_imports = @import("test_support.zig").Imports{ .runtime = antfly_imports, .vopr = options.vopr, .lmdb_engine = options.lmdb_engine };
    const vopr_mod = options.vopr;
    const casbin_mod = antfly_imports.casbin;
    const antfly_mod = options.antfly_mod;
    const embedded_mod = options.embedded_mod;
    const embedded_api_mod = options.embedded_api_mod;
    const antfly_embedded_pkg_mod = options.antfly_embedded_pkg_mod;
    const antfly_embedded_db_pkg_mod = options.antfly_embedded_db_pkg_mod;
    const antfly_embedded_api_pkg_mod = options.antfly_embedded_api_pkg_mod;
    const antfly_client_pkg_mod = options.antfly_client_pkg_mod;
    const embedded_db_mod = options.embedded_db_mod;
    const embedded_support_mod = options.embedded_support_mod;
    const capi_root_mod = options.capi_root_mod;
    const capi_mod = options.capi_mod;
    const run_capi_tests = options.run_capi_tests;
    const run_raft_library_tests = options.run_raft_library_tests;
    // Tests
    const antfly_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, antfly_test_mod, true, true);
    antfly_test_mod.addImport("antfly_openapi_specs", antfly_imports.embedded_openapi);

    const api_http_runtime_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/api_http_runtime_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, api_http_runtime_test_mod, true, true);
    api_http_runtime_test_mod.addImport("antfly_openapi_specs", antfly_imports.embedded_openapi);

    const metadata_unit_baseline_root_paths = [_][]const u8{
        "pkg/antfly/src/metadata_reconciler_test_root.zig",
        "pkg/antfly/src/metadata_service_http_test_root.zig",
        "pkg/antfly/src/metadata_core_test_root.zig",
        "pkg/antfly/src/metadata_api_admin_test_root.zig",
        "pkg/antfly/src/metadata_server_test_root.zig",
        "pkg/antfly/src/metadata_planning_transition_test_root.zig",
        "pkg/antfly/src/metadata_table_provisioner_test_root.zig",
        "pkg/antfly/src/metadata_replication_backfill_test_root.zig",
        "pkg/antfly/src/metadata_storage_test_root.zig",
    };
    var metadata_unit_baseline_mods: [metadata_unit_baseline_root_paths.len]*std.Build.Module = undefined;
    for (metadata_unit_baseline_root_paths, &metadata_unit_baseline_mods) |root_path, *test_mod| {
        test_mod.* = b.createModule(.{
            .root_source_file = b.path(root_path),
            .target = target,
            .optimize = optimize,
        });
        test_imports.configure(b, test_mod.*, true, true);
        test_mod.*.addImport("antfly_openapi_specs", antfly_imports.embedded_openapi);
    }

    const system_catalog_tests = b.addTest(.{
        .root_module = metadata_unit_baseline_mods[8],
        .filters = &.{ "system catalog", "catalog rename", "catalog names", "metadata raft apply store projects backup restore bootstrap source in placement intents" },
    });
    const system_catalog_store_tests = b.addTest(.{
        .root_module = metadata_unit_baseline_mods[8],
        .filters = &.{ "metadata raft apply store", "system catalog" },
    });
    const system_catalog_store_step = b.step("antfly-system-catalog-store-test", "Run catalog report persistence, snapshot, drain, and migration regressions");
    system_catalog_store_step.dependOn(&b.addRunArtifact(system_catalog_store_tests).step);
    const report_bench_tests = b.addTest(.{
        .root_module = metadata_unit_baseline_mods[8],
        .filters = &.{"store report workload benchmark"},
    });
    b.step("antfly-system-catalog-report-bench", "Run opt-in report apply and hydration workloads").dependOn(&b.addRunArtifact(report_bench_tests).step);
    const system_catalog_api_tests = b.addTest(.{
        .root_module = api_http_runtime_test_mod,
        .filters = &.{ "system catalog", "prepared query routing", "routing session pins every table", "metadata.table status encoder", "metadata.table detail encoder" },
    });
    const system_catalog_api_step = b.step("antfly-system-catalog-api-test", "Run qualified catalog HTTP authorization and protocol tests");
    system_catalog_api_step.dependOn(&b.addRunArtifact(system_catalog_api_tests).step);
    const system_catalog_test_step = b.step("antfly-system-catalog-test", "Run system catalog durability, identity, and routing tests");
    system_catalog_test_step.dependOn(&b.addRunArtifact(system_catalog_tests).step);
    const system_catalog_transport_tests = b.addTest(.{ .root_module = metadata_unit_baseline_mods[1], .filters = &.{"system catalog"} });
    system_catalog_test_step.dependOn(&b.addRunArtifact(system_catalog_transport_tests).step);

    const metadata_unit_test_root_paths = [_][]const u8{
        "pkg/antfly/src/metadata_unit_lane_a_test_root.zig",
        "pkg/antfly/src/metadata_unit_lane_b_test_root.zig",
    };
    var metadata_unit_test_mods: [metadata_unit_test_root_paths.len]*std.Build.Module = undefined;
    for (metadata_unit_test_root_paths, &metadata_unit_test_mods) |root_path, *test_mod| {
        test_mod.* = b.createModule(.{
            .root_source_file = b.path(root_path),
            .target = target,
            .optimize = optimize,
        });
        test_imports.configure(b, test_mod.*, true, true);
        test_mod.*.addImport("antfly_openapi_specs", antfly_imports.embedded_openapi);
    }

    const raft_harness_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/raft_harness_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, raft_harness_test_mod, true, true);
    raft_harness_test_mod.addImport("antfly_openapi_specs", antfly_imports.embedded_openapi);

    const introducer_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/introducer.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, introducer_test_mod, true, true);

    const data_runtime_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/data_runtime_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, data_runtime_test_mod, true, true);
    data_runtime_test_mod.addImport("antfly_openapi_specs", antfly_imports.embedded_openapi);

    const raft_runtime_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/raft_runtime_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, raft_runtime_test_mod, true, true);
    raft_runtime_test_mod.addImport("antfly_openapi_specs", antfly_imports.embedded_openapi);

    const raft_restore_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/raft_restore_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, raft_restore_test_mod, true, true);
    raft_restore_test_mod.addImport("antfly_openapi_specs", antfly_imports.embedded_openapi);

    const filesystem_capacity_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/filesystem_capacity_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, filesystem_capacity_test_mod, true, true);

    const data_storage_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/data_storage_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, data_storage_test_mod, true, true);
    data_storage_test_mod.addImport("antfly_openapi_specs", antfly_imports.embedded_openapi);

    const usermgr_storage_lib_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/usermgr/storage_imports.zig"),
        .target = target,
        .optimize = optimize,
    });
    usermgr_storage_lib_mod.addImport("antfly_root", antfly_mod);
    usermgr_storage_lib_mod.addImport("antfly_platform", platform_mod);
    antfly_mod.addImport("usermgr_storage", usermgr_storage_lib_mod);

    const usermgr_storage_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/usermgr/storage_imports.zig"),
        .target = target,
        .optimize = optimize,
    });
    usermgr_storage_test_mod.addImport("antfly_root", antfly_test_mod);
    usermgr_storage_test_mod.addImport("antfly_platform", platform_mod);
    antfly_test_mod.addImport("usermgr_storage", usermgr_storage_test_mod);

    const usermgr_storage_data_runtime_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/usermgr/storage_imports.zig"),
        .target = target,
        .optimize = optimize,
    });
    usermgr_storage_data_runtime_test_mod.addImport("antfly_root", data_runtime_test_mod);
    usermgr_storage_data_runtime_test_mod.addImport("antfly_platform", platform_mod);
    data_runtime_test_mod.addImport("usermgr_storage", usermgr_storage_data_runtime_test_mod);

    const common_http_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/common_http_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    common_http_test_mod.addImport("raft_engine", raft_engine_mod);
    common_http_test_mod.addImport("antfly_platform", platform_mod);
    common_http_test_mod.addImport("antfly_hash", hash_mod);
    common_http_test_mod.addImport("httpx", httpx_mod);
    common_http_test_mod.addImport("vopr", vopr_mod);
    const common_http_tests = b.addTest(.{
        .root_module = common_http_test_mod,
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    common_http_tests.root_module.link_libc = true;
    const run_common_http_tests = addFilteredTestRunArtifactWithRuntimeFilters(b, common_http_tests, &.{});
    const common_http_test_step = b.step("common-http-test", "Run common HTTP listener and client tests");
    common_http_test_step.dependOn(&run_common_http_tests.step);

    const api_json_helpers_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/api/json_helpers.zig"),
        .target = target,
        .optimize = optimize,
    });
    api_json_helpers_test_mod.addImport("antfly-json", json_mod);
    const api_json_helpers_tests = b.addTest(.{
        .root_module = api_json_helpers_test_mod,
    });
    const run_api_json_helpers_tests = b.addRunArtifact(api_json_helpers_tests);
    const lib_api_json_helpers_test_step = b.step("antfly-api-json-helpers-test", "Run standalone api/json_helpers tests");
    lib_api_json_helpers_test_step.dependOn(&run_api_json_helpers_tests.step);

    const api_artifact_reprocess_jobs_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/api_artifact_reprocess_jobs_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, api_artifact_reprocess_jobs_test_mod, true, true);
    api_artifact_reprocess_jobs_test_mod.addImport("antfly_openapi_specs", antfly_imports.embedded_openapi);
    const api_artifact_reprocess_jobs_tests = b.addTest(.{
        .root_module = api_artifact_reprocess_jobs_test_mod,
        .filters = &.{
            "artifact reprocess job store starts and updates a job",
            "artifact reprocess job store recovers durable jobs and reseeds ids",
            "artifact reprocess job store persists monotonic next id across stale durable writes",
            "artifact reprocess job cleanup removes recovered durable expired jobs",
            "artifact reprocess job store applies running cancellation at pass boundary",
            "artifact reprocess job store records cancel requested across stale queued token",
            "repair job store starts and records a pass",
            "repair job store applies running cancellation at pass boundary",
            "repair job store records cancel requested across stale queued token",
            "repair job store does not expire future live running heartbeat",
            "table repair job store persists monotonic next id across stale durable writes",
            "table repair job cleanup pages durable expired jobs",
            "forced index repair job dispatches force only once",
            "index repair job keeps degradation gauges as snapshots across retries",
            "named index repair cancellation remains nonterminal until durable controls finish",
            "named index repair cancellation restarts its durable traversal after job store recovery",
            "durable cancellation retries transient failures with backoff",
            "durable cancellation scan rotates past a backed off head window",
            "table repair job recovery quarantines corrupt primary without blocking service",
            "active repair job recovery quarantines malformed secondary entries",
            "api http client maps remote repair cancel unavailable",
            "api http client encodes table name for repair cancel callback",
            "public api routes compile",
            "table repair job records bounded pass and continuation",
        },
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_api_artifact_reprocess_jobs_tests = addFilteredTestRunArtifact(b, api_artifact_reprocess_jobs_tests);
    const lib_api_artifact_reprocess_jobs_test_step = b.step("antfly-api-artifact-reprocess-jobs-test", "Run artifact reprocess job store tests");
    lib_api_artifact_reprocess_jobs_test_step.dependOn(&run_api_artifact_reprocess_jobs_tests.step);

    const api_restore_jobs_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/api_restore_jobs_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, api_restore_jobs_test_mod, true, true);
    const api_restore_jobs_tests = b.addTest(.{
        .root_module = api_restore_jobs_test_mod,
        .filters = &.{
            "replicated restore persistence maps private callback errors to stable unavailability",
            "failed destination authorization refresh reuses the idempotent restore job",
            "delayed replicated restore refresh cannot regress a running job",
            "restore job store is idempotent and fenced",
            "restore idempotency keys are scoped by principal and resource",
            "successful restore completion wins a racing cancellation",
            "retryable restore contention durably requeues progress and honors cancellation",
            "restore ownership loss requeues only the exact running attempt",
            "replicated restore mutations are rejected after leadership term changes",
            "restore dispatch recovery retains worker ownership when begin fails",
            "restore retry jitter is stable and honors production bounds",
            "delayed restore contention yields FIFO capacity to unrelated jobs",
            "restore job runnable queue drains incrementally and preserves insertion order",
            "replicated restore leadership rebuild preserves FIFO and recovers running attempts",
            "replicated restore leadership terminalizes cancellation of a running attempt",
            "replicated restore expiry deletion preserves foreign boundary failure",
            "restore requests without idempotency keys create independent opaque jobs",
            "restore runtime store persists checkpoints and requeues interrupted work",
            "restore progress ordinals remain bounded at maximum table count",
            "restore progress ranges bound maximally fragmented cluster state",
            "cluster restore summaries are truthful and bounded",
            "restore job store rejects oversized request state",
        },
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_api_restore_jobs_tests = addFilteredTestRunArtifact(b, api_restore_jobs_tests);
    const lib_api_restore_jobs_test_step = b.step("antfly-api-restore-jobs-test", "Run durable restore job store tests");
    lib_api_restore_jobs_test_step.dependOn(&run_api_restore_jobs_tests.step);

    const portable_backup_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/portable_backup_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, portable_backup_test_mod, true, true);
    const portable_backup_tests = b.addTest(.{
        .root_module = portable_backup_test_mod,
        .filters = &.{
            "export and import documents round trip",
            "portable AFB2 delta resolves exact base and deduplicates physical blobs",
            "file import restores Go cross-backend portable fixture",
            "file import restores production Go portable fixture",
            "file import rejects oversized portable blocks before allocation",
            "import preflights full portable envelope before mutating destination",
            "export and import documents preserve timestamps",
            "export and import chunk artifacts round trip with public artifact ids",
            "export and import asset artifacts round trip with public artifact ids",
            "export and import resolution artifacts round trip with public artifact ids",
            "portable graph conversion accepts generation-less v1 edge artifacts",
            "document batch round-trip",
            "AFB2 manifest separates representation from snapshot mode",
            "AFB2 manifest rejects ambiguous delta and traversal paths",
            "AFB2 delta base binds inventory and identity to one canonical manifest",
            "AFB2 readers fail closed on declared unsupported payload features",
            "AFB2 trailer locates the footer without scanning payloads",
            "AFB2 native directory round trips through staged extraction",
            "AFB2 native delta requires and resolves the exact parent manifest",
            "repository manifest is a complete canonical materialized inventory",
            "repository inventory preserves logical paths while deduplicating bytes",
            "repository manifest parsing is bounded before allocation",
            "repository ref publication is compare and swap",
            "incremental plan uploads only blobs absent from complete parent",
            "repository incremental upload streams only blobs absent from exact parent",
            "repository publishes resolves and materializes one complete deduplicated snapshot",
            "repository epoch fences GC and active publication leases retain candidates",
            "repository reachability fails closed while an active lease manifest is missing",
        },
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_portable_backup_tests = addFilteredTestRunArtifact(b, portable_backup_tests);
    const lib_portable_backup_test_step = b.step("antfly-storage-portable-backup-test", "Run bounded portable backup tests");
    lib_portable_backup_test_step.dependOn(&run_portable_backup_tests.step);

    const lib_generating_runtime_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{
            "generating backend",
            "local generation budgets",
            "local generation bridge",
            "generating backend factory executes fallback chain across providers",
            "asset producer runtime",
            "asset producer raw raster selection requires local physical capability and borrows pixels",
            "asset producer destroys returned values",
            "asset producer media invocation memory fails closed",
            "asset producer enforces media allocator and result contracts",
            "asset producer enforces invocation contracts",
            "asset producer bounds invocation contract resolution",
            "encoded reader chunks obey model item and byte limits",
            "media part item embedding",
            "managed embedder",
            "inference capabilities",
            "attachment transport",
            "remote generator batch streams attachments",
            "batch capabilities",
            "work identity and execution reports",
            "bounded invocation allocator",
            "remote Antfly",
            "antfly chunk request frames borrowed binary input",
            "capability lease HTTP fields own storage",
            "provider quotas",
            "vertex provider",
        },
    });
    const run_lib_generating_runtime_tests = addFilteredTestRunArtifact(b, lib_generating_runtime_tests);
    const lib_generating_runtime_test_step = b.step("antfly-generating-test", "Run generating backend adapter tests");
    lib_generating_runtime_test_step.dependOn(&run_lib_generating_runtime_tests.step);

    const lib_managed_embedder_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{ "managed embedder", "antfly embed request", "antfly embed round trip", "antfly sparse embed round trip", "antfly numeric", "antfly provider preserves explicit distributed admission denial", "legacy numeric" },
    });
    const run_lib_managed_embedder_tests = addFilteredTestRunArtifact(b, lib_managed_embedder_tests);
    const lib_managed_embedder_test_step = b.step("antfly-inference-managed-embedder-test", "Run managed embedder contract and provider tests");
    lib_managed_embedder_test_step.dependOn(&run_lib_managed_embedder_tests.step);

    const lib_reranking_runtime_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"reranking runtime"},
    });
    const run_lib_reranking_runtime_tests = addFilteredTestRunArtifact(b, lib_reranking_runtime_tests);
    const lib_reranking_runtime_test_step = b.step("antfly-reranking-test", "Run reranking backend adapter tests");
    lib_reranking_runtime_test_step.dependOn(&run_lib_reranking_runtime_tests.step);

    const lib_common_default_filters = [_][]const u8{ "provider registry", "std http listener", "std http executor", "threaded connector", "health server", "runtime lifecycle" };
    const lib_common_runtime_filters = selectTestFilters(b, &lib_common_default_filters);
    const lib_common_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = compileFiltersWithAnchors(
            b,
            &.{"common."},
            lib_common_runtime_filters,
        ),
    });
    const run_lib_common_tests = addFilteredTestRunArtifactWithRuntimeFilters(
        b,
        lib_common_tests,
        lib_common_runtime_filters,
    );
    const lib_common_test_step = b.step("antfly-common-test", "Run common/provider registry tests");
    lib_common_test_step.dependOn(&run_lib_common_tests.step);

    const lib_common_config_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"common config"},
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lib_common_config_tests = addFilteredTestRunArtifact(b, lib_common_config_tests);
    run_lib_common_config_tests.setEnvironmentVariable("ANTFLY_TEST_FAIL_ON_ERROR_LOGS", "0");
    const lib_common_config_test_step = b.step("antfly-common-config-test", "Run common/config tests");
    lib_common_config_test_step.dependOn(&run_lib_common_config_tests.step);

    const lib_preload_model_spec_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{
            "preload model spec parser categorizes registry variants and backends",
            "inference runtime preload parser preserves registry variants and explicit backends",
            "inference run config",
            "inference list accepts models directory before or after flags",
        },
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lib_preload_model_spec_tests = addFilteredTestRunArtifact(b, lib_preload_model_spec_tests);
    const lib_preload_model_spec_test_step = b.step("antfly-common-preload-model-spec-test", "Run preload model CLI parser tests");
    lib_preload_model_spec_test_step.dependOn(&run_lib_preload_model_spec_tests.step);

    const lib_common_secrets_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{ "file secret store", "remote content runtime" },
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lib_common_secrets_tests = addFilteredTestRunArtifact(b, lib_common_secrets_tests);
    const lib_common_secrets_test_step = b.step("antfly-common-secrets-test", "Run common secret and remote-content reload tests");
    lib_common_secrets_test_step.dependOn(&run_lib_common_secrets_tests.step);

    const secret_store_abi_provider_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/secret_store_abi_test_provider.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    secret_store_abi_provider_mod.addImport("antfly_platform", platform_mod);
    const secret_store_abi_provider = b.addLibrary(.{
        .name = "secret-store-abi-test-provider",
        .root_module = secret_store_abi_provider_mod,
        .linkage = .static,
    });
    const secret_store_abi_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/secret_store_abi_test.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    secret_store_abi_test_mod.addImport("antfly_platform", platform_mod);
    secret_store_abi_test_mod.linkLibrary(secret_store_abi_provider);
    const secret_store_abi_tests = b.addTest(.{
        .root_module = secret_store_abi_test_mod,
        .filters = &.{ "secret store operations retain their IO owner across runtime archives", "secret store archive boundary" },
    });
    const run_secret_store_abi_tests = b.addRunArtifact(secret_store_abi_tests);
    const secret_store_abi_test_step = b.step("lib-common-secrets-abi-test", "Run secret store tests across independently compiled runtime archives");
    secret_store_abi_test_step.dependOn(&run_secret_store_abi_tests.step);
    lib_common_secrets_test_step.dependOn(&run_secret_store_abi_tests.step);

    const runtime_io_abi_provider = b.addLibrary(.{
        .name = "runtime-io-abi-test-provider",
        .linkage = .static,
        .root_module = b.createModule(.{
            .root_source_file = b.path("pkg/antfly/src/runtime_io_abi_test_provider.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
        }),
    });
    const runtime_io_abi_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/runtime_io_abi_test.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    runtime_io_abi_test_mod.linkLibrary(runtime_io_abi_provider);
    const runtime_io_abi_tests = b.addTest(.{
        .root_module = runtime_io_abi_test_mod,
        .filters = &.{"executor archive boundary"},
    });
    const run_runtime_io_abi_tests = b.addRunArtifact(runtime_io_abi_tests);
    b.step("runtime-io-abi-test", "Run executor contracts across independent error domains").dependOn(&run_runtime_io_abi_tests.step);

    const api_cluster_secret_status_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/api_cluster_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, api_cluster_secret_status_test_mod, true, true);
    api_cluster_secret_status_test_mod.addImport("antfly_openapi_specs", antfly_imports.embedded_openapi);
    const api_cluster_secret_status_tests = b.addTest(.{
        .root_module = api_cluster_secret_status_test_mod,
        .filters = &.{ "cluster status carries non-secret", "cluster topology owns snapshot data" },
    });
    const run_api_cluster_secret_status_tests = addFilteredTestRunArtifact(b, api_cluster_secret_status_tests);
    lib_common_secrets_test_step.dependOn(&run_api_cluster_secret_status_tests.step);

    const lib_usermgr_tests = b.addTest(.{
        .root_module = usermgr_mod,
        .filters = &.{"usermgr."},
        .test_runner = .{ .path = b.path("pkg/antfly/src/test_runner.zig"), .mode = .simple },
    });
    const run_lib_usermgr_tests = b.addRunArtifact(lib_usermgr_tests);
    const lib_usermgr_test_step = b.step("antfly-usermgr-test", "Run standalone pkg/antfly/src/usermgr tests");
    lib_usermgr_test_step.dependOn(&run_lib_usermgr_tests.step);

    const usermgr_abi_provider_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/usermgr_abi_test_provider.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    usermgr_abi_provider_mod.addImport("antfly_casbin", casbin_mod);
    const usermgr_abi_provider = b.addLibrary(.{
        .name = "usermgr-abi-test-provider",
        .root_module = usermgr_abi_provider_mod,
        .linkage = .static,
    });
    const usermgr_abi_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/usermgr_abi_test.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    usermgr_abi_test_mod.addImport("antfly_casbin", casbin_mod);
    usermgr_abi_test_mod.linkLibrary(usermgr_abi_provider);
    const usermgr_abi_tests = b.addTest(.{
        .root_module = usermgr_abi_test_mod,
        .filters = &.{"usermgr archive boundary"},
    });
    const run_usermgr_abi_tests = b.addRunArtifact(usermgr_abi_tests);
    b.step("lib-usermgr-abi-test", "Run UserManager contracts across independent error domains").dependOn(&run_usermgr_abi_tests.step);
    lib_usermgr_test_step.dependOn(&run_usermgr_abi_tests.step);

    const embedded_tests = b.addTest(.{
        .root_module = embedded_mod,
        .filters = &.{"embedded"},
    });
    const run_embedded_tests = addFilteredTestRunArtifact(b, embedded_tests);
    const embedded_test_step = b.step("embedded-test", "Run embedded API tests");
    embedded_test_step.dependOn(&run_embedded_tests.step);
    // Imported module roots do not collect their own tests through the package
    // surface test, so run the API fixtures in their owning module as well.
    const embedded_api_tests = b.addTest(.{
        .root_module = embedded_api_mod,
        .filters = &.{
            "embedded api round-trips batch lookup scan and search over memory-backed durable lsm",
            "embedded api hosted profile drains derived indexing without native runtimes",
            "embedded api hosted profile persists text index across reopen over storage",
        },
    });
    const run_embedded_api_tests = addFilteredTestRunArtifact(b, embedded_api_tests);
    embedded_test_step.dependOn(&run_embedded_api_tests.step);

    const antfly_embedded_pkg_tests = b.addTest(.{
        .root_module = antfly_embedded_pkg_mod,
        .filters = &.{"pkg antfly embedded root"},
    });
    const run_antfly_embedded_pkg_tests = addFilteredTestRunArtifact(b, antfly_embedded_pkg_tests);
    const antfly_embedded_db_pkg_tests = b.addTest(.{
        .root_module = antfly_embedded_db_pkg_mod,
        .filters = &.{"pkg antfly embedded db"},
    });
    const run_antfly_embedded_db_pkg_tests = addFilteredTestRunArtifact(b, antfly_embedded_db_pkg_tests);
    const antfly_embedded_api_pkg_tests = b.addTest(.{
        .root_module = antfly_embedded_api_pkg_mod,
        .filters = &.{"pkg antfly embedded api"},
    });
    const run_antfly_embedded_api_pkg_tests = addFilteredTestRunArtifact(b, antfly_embedded_api_pkg_tests);
    const antfly_embedded_pkg_test_step = b.step("antfly-embedded-test", "Run the standalone antfly-embedded package compile test");
    antfly_embedded_pkg_test_step.dependOn(&run_antfly_embedded_pkg_tests.step);
    antfly_embedded_pkg_test_step.dependOn(&run_antfly_embedded_db_pkg_tests.step);
    antfly_embedded_pkg_test_step.dependOn(&run_antfly_embedded_api_pkg_tests.step);

    const antfly_client_pkg_tests = b.addTest(.{
        .root_module = antfly_client_pkg_mod,
        .filters = &.{
            "antfly client pkg compiles",
            "get index response timeout bounds the complete HTTP request",
            "list indexes response timeout bounds readiness preflight",
        },
    });
    const run_antfly_client_pkg_tests = addFilteredTestRunArtifact(b, antfly_client_pkg_tests);
    const antfly_client_pkg_test_step = b.step("antfly-client-test", "Run the standalone antfly-client package compile test");
    antfly_client_pkg_test_step.dependOn(&run_antfly_client_pkg_tests.step);

    const root_test_skip_filters = [_][]const u8{
        "managed host simulation",
        "managed http host simulation",
        "managed http cluster simulation",
        "cluster simulation",
        "http host simulation",
        "simulation harness module compiles",
        "lsm backend simulation",
        "persistent sim ",
        "wal sim ",
        "index manager sim ",
        "db split sim ",
        "metadata VOPR",
        "chaos",
        "soak",
        "storage sim ",
        "modeled device",
        "wal group commit uses injected virtual clock",
        "wal can reopen on modeled storage device",
        "wal modeled ",
        "persistent modeled ",
        "index manager modeled replay fixtures stay green",
        "index manager modeled crash fixtures stay green",
        "db split modeled ",
        "serverless",
        "raft.",
        "storage.ha",
        "HBC recall",
    };
    const lib_unit_default_filters = [_][]const u8{
        // Regressions previously selected only by unit-test-progress.
        "api.tables.test.metadata.table lsm status exposes wal retry and publication debt",
        "api.tables.test.metadata.table status encoder emits antfly-style shard map",
        "api.tables.test.metadata.table detail encoder includes replication source status and action hint",
        "api.tables.test.metadata.table status encoder canonicalizes embeddings indexes independent of JSON key order",
        "api.tables.test.metadata.table status includes observed dynamic field capabilities",
        "api.tables.test.metadata.table status merges query modes conservatively",
        "api.tables.test.metadata.table status encoder preserves enrichment summaries without producer configuration",
        "api.tables.test.metadata.table list projection redacts only enrichment producer configuration",
        "api.tables.test.metadata.schema update preserves read schema and adds versioned full-text index",
        "api.tables.test.metadata.schema update versions template-only changes",
        "api.tables.test.metadata.schema update avoids a generation for semantically identical JSON",
        "api.tables.test.metadata.schema update avoids a generation for explicit runtime defaults",
        "api.tables.test.metadata.schema update versions dynamic rule precedence changes",
        "api.tables.test.metadata.schema update versions inferred dynamic path changes",
        "api.tables.test.metadata.schema update versions validation-only changes",
        "api.tables.test.metadata.schema update rejects generation overflow",
        "api.tables.test.metadata.schema update can repair a legacy non-derivable schema",
        "api.tables.test.metadata.schema update regenerates algebraic config and preserves user knobs",
        "api.tables.test.metadata.schema update preserves compatible HLL definitions and rejects removed fields",
        "api.tables.test.metadata.query routing selects read schema full text index",
        "api.tables.test.metadata.query routing leaves hierarchy child traversal index free",
        "api.tables.test.metadata.query routing selects current versioned full text index",
        "api.tables.test.metadata.query routing preserves vector index and records read schema text index for filters",
        "api.tables.test.metadata.query routing selects read schema text index for vector-only structured filters",
        "metadata.reallocation_request.test.reallocation request membership contract is canonical and fail closed for legacy records",
        "metadata.topology_protocol.test.range membership is order independent and table scoped",
        "metadata.runtime_status_protocol.test.runtime status exposes only released compatibility profiles",
        "metadata.table_topology_mutations.test.restore preserves implicit source document identity across a new physical incarnation",

        "boundary dispatcher preserves local calls and maps cross-unit calls",
        "bedrock provider request helpers",
        "embedding provider request helpers",
        "restore job store is idempotent and fenced",
        "restore requests without idempotency keys create independent opaque jobs",
        "restore runtime store persists checkpoints and requeues interrupted work",
        "restore job store rejects oversized request state",
        "restore filesystem scope containment handles filesystem roots and component boundaries",
        ".test_0",
        "module compiles",
        "internal join maps resource and ownership failures to unavailable",
        "postgres libpq global permits are atomic and bounded",
        "postgres libpq permit saturation preserves zero-connection pools",
        "postgres libpq async reader services input while flushing and between results",
        "postgres libpq async reader rejects and clears additional results",
        "postgres libpq async reader observes cancellation after bounded wait before consuming input",
        "postgres libpq async reader returns cancelled before waiting or consuming input",
        "postgres libpq result decoding observes cancellation at periodic checkpoints",
        "postgres libpq global permit wait observes cancellation without a deadline",
        "postgres libpq pool wait observes cancellation without a deadline",
        "postgres libpq cancellation during connect polling closes the fresh connection",
        "managed embedder cancels an in-flight remote embedding request",
        "postgres libpq created replication snapshot cloning is allocation-failure safe",
        "postgres libpq weighted FIFO preserves a queued two-permit cutover",
        "postgres libpq timed out weighted head hands released capacity to follower",
        "postgres libpq cancelled FIFO head hands capacity to next waiter",
        "postgres libpq idle reclamation transfers only missing capacity",
        "postgres libpq reclamation leaves permit scheduling responsive",
        "cache budget atomically enforces its hard limit",
        "query embedding cache owns results and coalesces misses",
        "query embedding cache keys isolate security domains",
        "managed embedder deadlines bound provider pacing and transport",
        "remote capability invalidation fences active discovery",
        "managed embedder dimension probe validation modes",
        "managed embedding request overlays borrow capability cache synchronization",
        "managed embedder rejects malformed provider vectors",
        "managed embedder rejects unsupported execution namespaces",
        "managed embedder separates index and artifact lookup namespaces",
        "managed embedder validates sparse config with probe during normalization",
        "managed embedder routes antfly without api_url to local provider",
        "managed embedder artifact backed embedding translation",
        "managed embedder binds execution to catalog semantic producer identity",
        "managed embedder reuses an executable owner for producerless artifact consumers",
        "managed embedder catalog ownership rejects orphaned semantic producers",
        "catalog ownership rejects duplicate executable owners and endpoint mismatches",
        "metadata http client preserves artifact dependency conflicts",
        "managed embedder preserves coverage policy in storage config",
        "managed embedder admission follows the selected attachment transport",
        "managed embedder metadata",
        "managed embedder partitions and validates inline image data URIs",
        "attachment transport separates wire and peak resident representations",
        "bounded invocation allocator",
        "inline data URI parser validates canonical metadata",
        "antfly embed request",
        "antfly embed round trip",
        "antfly sparse embed round trip",
        "antfly embed parts uses the framed attachment transport",
        "antfly embed parts request sizing is exact for escaped strings",
        "antfly dense JSON response cleanup is allocation-failure safe",
        "remote generator batch streams attachments into one exact JSON body",
        "capability lease HTTP fields own storage",
        "asset producer runtime rejects empty borrowed media",
        "asset producer runtime derives coherent logical and wire result ceilings",
        "asset producer runtime applies result ceilings to non-model producers",
        "remote chunk runtime services do not require a local callback provider",
        "asset producer media invocation memory fails closed",
        "asset producer enforces media allocator and result contracts",
        "asset producer enforces invocation contracts",
        "asset producer bounds invocation contract resolution",
        "media part item embedding",
        "PDF render budget reserves the complete invocation peak",
        "semantic query planning reuses equivalent embeddings",
        "batch parser preserves oversized value errors",
        "batch parser accepts raw payload value under public request cap",
        "public batch parser rejects non-object documents while internal replay remains opaque",
        "batch parser safely rejects unsupported transform after initialized operations",
        "linear merge request parser accepts raw payload value under public request cap",
        "linear merge request parser rejects non-object records",
        "linear merge uses one ordered hash scan and delegates mutations to the HA batch source",
        "internal scan content hash mode round trips without public document fields",
        "http response uses its owning allocator",
        "public index contract exposes runtime status metadata",
        "public index config encoders redact coverage incarnation",
        "created nested response allowlists cover generated schemas",
        "public index config encoders redact nested credentials",
        "public index config encoders retain credential-free provider urls",
        "public index config encoders omit root write-only producer documents",
        "created graph index response projects closed nested schemas",
        "enrichment index status encodes worker lifecycle diagnostics",
        "compact index repair status keeps corrupt terminal state actionable",
        "data runtime report preserves compact managed repair admission state",
        "metadata status JSON preserves compact managed index admission state",
        "catalog sources without compact routing fail closed",
        "span routing uses compact catalog snapshot when available",
        "span routing confirms eventual misses with a linearizable compact snapshot",
        "route resolver confirms a table-present range miss linearly",
        "eventual span routing distinguishes snapshot timeout",
        "await route observes delayed publication without a polling sleep",
        "await route distinguishes persistent absence from capture timeout",
        "await route reports an expired pre-capture deadline as timed out",
        "route projection preserves order and remains bounded after capture",
        "pinned fanout rejects mismatched fence identity",
        "metadata routing server converts relative budget to local deadline",
        "metadata routing change client forwards an authority-scoped long poll",
        "remote runtime status reports replay debt separately from active catch-up",
        "table storage status sums complete fresh shard disk usage",
        "metadata.table status encoder honors storage status overrides",
        "public openapi documents stable exact sort diagnostics",
        "artifact enrichment request permits asset full text routing",
        "provisioned read cache retirement is allocation-free after entry installation",
        "provisioned read cache exclusive access drains active read leases",
        "provisioned group storage wires remote content to writer caches",
        "provisioned table write source drop table waits for active read cache lease",
        "provisioned table write source backup releases read cache exclusive before native snapshot copy",
        "write cache retirement is allocation-free after entry installation",
        "write cache transition locks use stable cache roles instead of addresses",
        "backend runtime durable lane runs inline jobs",
        "backend runtime durable lane leaves inline failed jobs owned by caller",
        "backend runtime threaded durable lane rejects jobs after owner close",
        "backend runtime API lane leases expose and release the interface",
        "backend runtime rejects API lane leases after shutdown begins",
        "backend runtime control lane leases are isolated from API leases",
        "backend runtime inference lane has an isolated bounded executor",
        "backend runtime separates native operation IO from outbound network IO",
        "backend runtime native API lane preserves filesystem errors across executors",
        "backend runtime exposes native API filesystem IO separately",
        "backend runtime rejects control lane leases after shutdown begins",
        "provisioned table write cache retires stale db when index metadata changes",
        "table runtime snapshot cache preserves active managed admission proof",
        "managed startup catch-up advances counterless incomplete dense repair",
        "db completed partial managed admission serves and retires redundant repair",
        "db status cannot reopen a quarantined generation from an older publication certificate",
        "db status retains certified canonical admission during shadow build handoff",
        "db initial replay repair retains certified canonical admission during shadow reconstruction",
        "db empty managed index does not invent generated coverage recovery debt",
        "db repair preflight retains a canonical generation completed after scheduler selection",
        "db coverage recovery admits a published generation after its admission marker retires",
        "provisioned leader admission rejects uncommitted writes under dense repair pressure",
        "api maintenance resumes recovered durable named index cancellation without client advance",
        "embeddings index status ignores inactive stale catch-up progress once dense coverage is visible",
        "managed embeddings readiness ignores finalizing catch-up after rate-limit recovery",
        "managed embedder sends antfly media parts when local provider is configured",
        "managed embedder normalizes local admission overload across embedding modes",
        "partial coverage embeddings readiness counts skipped source units",
        "partial coverage embeddings readiness does not mask pending enrichment",
        "complete partial embeddings coverage is ready after active generation proof",
        "actionable repair remains visible while retained generation stays queryable",
        "serviceable full text replacement remains queryable while rebuilding",
        "progressive embeddings readiness exposes a queryable partial generation",
        "readiness evaluation cannot complete while convergence work remains",
        "readiness completion fences include every observation dimension",
        "missing target observation preserves serving snapshot and blocks only completion",
        "create table raw parser merges default full text with quickstart embedding index",
        "create table raw parser accepts its canonical full text output",
        "table contract rejects unsupported index kinds before admission",
        "table contract rejects graph configs the runtime cannot materialize",
        "table graph validation rejects runtime-invalid configs before catalog admission",
        "table contract preserves typed artifact-backed graph configuration",
        "table contract rejects unknown fields in closed nested index objects",
        "table contract treats nullable nested index fields as omitted",
        "table contract preserves artifact-backed public full text indexes",
        "table contract rejects invalid inline artifact enrichments before admission",
        "table contract normalizes public artifact enrichment request",
        "restore admission rejects an embedding artifact catalog without an executable producer",
        "extension lifecycle rejects artifact embedding consumers without executable producers",
        "extension lifecycle rejects duplicate executable artifact owners",
        "extension lifecycle requires stable identity for executable artifact owners",
        "managed embedding catalog normalization persists stable producer identity",
        "exact replacement protects only changed extension-owned state",
        "authoritative catalog mutation boundaries reject orphaned semantic producers",
        "public enrichment validation rejects invalid execution and producer config",
        "provisioned primary lookup lease fails on identity namespace mismatch",
        "inference pull recognizes help before model resolution",
        "inference run recognizes help before server startup",
        "inference pull classifies order independent value flags",
        "inference pull rejects flags from the other model domain",
        "inference runtime preserves effective process envelope provenance",
        "metadata.table generated field capabilities include schema dynamic templates",
        "metadata.table status exposes stable field capabilities",
        "metadata.table status promotes schema capability when runtime coverage is complete",
        "metadata.table status promotes schema geo capability when runtime coverage is complete",
        "metadata.table status does not promote mismatched index sort runtime capability",
        "metadata.table status does not advertise changed index sort direction before rebuild",
        "metadata.table status merges observed capabilities conservatively",
        "metadata.table debug encoder emits runtime schemas and index bindings",
        "api query builder preflight describes missing physical sort coverage with public sortable wording",
        "api query builder prompt exposes native sort capabilities",
        "api query contract preflight preserves a named full text index",
        "query builder preflight plan preserves exact named full text selection",
        "distributed query shard request preserves sorted cursor contract",
        "distributed sorted hit merge uses typed sort tuple ordering and cursors",
        "distributed shard validation rejects mixed scalar sort domains",
        "distributed merge rejects provably incomplete exact shard windows",
        "distributed merge rejects oversized shard windows",
        "distributed merge uses runtime schema for typed date cursors",
        "segment index sort metadata roundtrip",
        "segment merge drops index sort metadata until physical sort is preserved",
        "segment sorted merge normalizes legacy mixed numeric index sort domains",
        "segment sorted merge preserves index sort and remaps doc addressed sections",
        "dynamic template selector and mapping-option resolution",
        "parse document field mapping contract",
        "runtime schema derives internal doc values from sortable scalar mappings",
        "schema rejects sortable non-scalar dynamic mappings",
        "runtime schema derives and validates index sort metadata",
        "runtime schema lowers document field mappings to exact declared fields",
        "explicit document field mappings take precedence over dynamic templates",
        "document field mappings deduplicate compatible paths and reject conflicts",
        "write validation enforces table-wide exact mappings across document types",
        "composed schemas lower only unconditional equivalent exact mappings",
        "runtime schema retains shorthand exact scalar declarations as non-sortable capabilities",
        "metadata.schema update ignores shorthand capability declaration order",
        "schema rejects sortable non-scalar document field mappings",
        "parse rejects document field mappings incompatible with their schema value domain",
        "write validation rejects values that cannot populate explicit physical mappings",
        "table schema parses canonical ttl policy and explicit removal",
        "schema merge patch preserves unrelated fields and removes ttl",
        "runtime schema field capability helpers classify mapped sortability",
        "schema serialization rejects unsorted or duplicate exact fields",
        "sorted exact fields resolve before wildcard templates and find subfields without allocation",
        "document mapper accepts match-mapping-type dynamic template index_sort field",
        "document mapper emits mapped keyword subfield postings and typed doc values",
        "exact document mappings do not leak through dynamic leaf-name fallback",
        "nested exact mappings do not consume their parent value as a multi-field",
        "distributed merge accepts cursors across the logical numeric domain",
        "sorted segment bounds compare across the logical numeric domain",
        "native sort execution accepts cursors across the logical numeric domain",
        "document mapper emits schema-derived mapped keyword subfield coverage",
        "document mapper omits multi-valued mapped keyword subfield typed doc values",
        "document mapper flushes schema index_sort segments in physical sort order",
        "document mapper validates schema index_sort field capabilities",
        "document mapper emits schema geo point typed doc values",
        "typed doc values bytes round-trip",
        "typed doc values exact numeric domain round-trip and comparison",
        "typed doc values coverage admission honors cancellation deadline and contention",
        "cover bounding box enforces budget with hashed deduplication",
        "cover bounding box rejects invalid bounds",
        "geo distance filter",
        "geo bbox filter refines indexed geohash candidates",
        "geo filter candidate precision adapts to selective boxes",
        "geo bbox coarse candidates expand max precision geohash terms",
        "geo bbox dense coarse candidates fall back to exact doc values",
        "geo bbox filter supports antimeridian wrapped longitude ranges",
        "geo distance filter uses indexed candidates across antimeridian",
        "geo shape filter point in polygon",
        "document mapper preserves unsigned numeric doc values beyond i64 as u64",
        "schema-derived keyword subfield backs native sort execution",
        "sort value comparison defines canonical scalar order",
        "sort execution plan dimension names are stable for profiles",
        "sort cursor contract classifies arity separately from type",
        "json sort values reject non-replayable numeric values at API boundaries",
        "stored json debug sort honors runtime missing null policy",
        "stored json debug sort normalizes runtime datetime values",
        "score sort source detection rejects non-scoring text queries",
        "vector score order helper is limited to internal score tuple decoration",
        "score sort rejects hits without finite scores",
        "native sort zero limit avoids generic collector decoration",
        "text doc values sort zero limit avoids budget and decoration",
        "match_all candidate sort rejects direct score sort execution",
        "match_all native candidate sort zero limit avoids decoration",
        "match_all native ordinal doc values zero limit avoids budget and decoration",
        "match_all native stream sort zero limit counts without decoration",
        "match_all id seek zero limit exposes internal sort profile when sampled",
        "match_all id seek zero limit respects cursor bounds exactly",
        "match_all native candidate sort applies cursor before admission",
        "match_all unordered source loads selected hits through projected batch",
        "vector score top k sort profile uses common sort vocabulary",
        "native sort planner classifies mapping and cursor rejection reasons",
        "text score query exposes score top k sort profile",
        "native text sort planner requires live segment index sort coverage for sorted executor",
        "native text sort planner ignores fully deleted legacy segments for index sort coverage",
        "text field sort uses sorted segment membership path when index sort matches",
        "text projected source load rejects expired deadline before stored load",
        "native numeric sort rejects non-finite doc values",
        "mixed numeric concrete sort keys share one cursor domain",
        "native sort coverage diagnostics classify physical doc value failures",
        "native sort cold coverage validation observes request deadline and cancellation",
        "sort uses native text doc values without stored json fallback",
        "required native sort does not fall back to stored json on doc value miss",
        "native doc values plan enforces native values even with non-requiring loader",
        "native doc values plan rejects runtime value kind mismatch",
        "required native sort fails on absent physical doc value section",
        "required native sort fails on sparse doc value entry miss",
        "pattern typed structured filters accept explicit path alias",
        "pattern typed structured filters reject ambiguous field and path aliases",
        "pattern typed structured filters reject malformed and unbounded ranges",
        "pattern geo structured filters reject invalid coordinates",
        "exact structured ID filters resolve without a secondary index",
        "dense and sparse search reject unsupported exact sort page options",
        "dense projected source load rejects expired deadline before load",
        "match_all sorted segment seek merges sorted segments and applies cursors",
        "match_all sorted segment seek honors deleted old sort values after upsert",
        "match_all sorted segment seek uses cursor seek within each segment",
        "match_all sorted segment seek enforces scan budget",
        "match_all sorted segment seek checks deadline while scanning",
        "match_all sorted segment seek zero limit returns profile without scanning",
        "match_all projected source load rejects expired deadline before batch load",
        "match_all rejects sorted pages with unresolved stored pattern filters",
        "match_all rejects cursor pages with unresolved stored pattern filters",
        "match_all rejects field sort without native doc values",
        "match_all rejects score sort without score-bearing source",
        "composed search rejects exact field sort across embedding sources",
        "composed text exact sort preserves native component profile",
        "composed exact sort validates component sort tuples",
        "composed text exact sort surfaces missing component profile",
        "declared runtime sortable field capability reports covered queryable state",
        "declared runtime geo field capability reports covered filterable state",
        "retrieval agent treats aggregations as first-class tool capability",
        "retrieval agent requires filter and aggregate tools for filtered aggregations",
        "retrieval agent ignores empty map-valued tool fields for policy and strategy",
        "retrieval agent supports roots tree search",
        "annotate tree document prefers graph path branch metadata",
        "retrieval agent isolates query predicates while applying accumulated filters",
        "retrieval agent installs canonical mandatory predicates once",
        "retrieval agent generation uses the canonical generator and chain contract",
        "retrieval agent generation preserves canonical chain order and retry policy",
        "retrieval agent generation requires a canonical generator when the step is present",
        "retrieval agent authenticated row filter conjoins generated filter",
        "query builder infers graph multi hop pattern from intent",
        "query builder maps canonical graph queries and ignores legacy expansion",
        "retrieval root scan pushes row inclusion and exclusion predicates into one filter",
        "retrieval contains filter treats wildcard operators as literals",
        "distributed reranking widens retrieval and stays coordinator owned",
        "reranker candidate and output windows have distinct bounds",
        "reranker admission precedes candidate rendering",
        "reranker component paging includes the post-rerank offset",
        "reranker paging preserves the underlying retrieval total",
        "query dependency errors expose a stable JSON retry contract",
        "wildcard matching distinguishes operators from escaped literals",
        "wildcard literal escaping round trips metacharacters",
        "wildcard search plans preserve escaped exact literals and prefixes",
        "algebraic wildcard helpers preserve escaped literals",
        "algebraic traversal intersects query-scoped node admission",
        "traverse preserves table-scoped identities across result dedup and algebraic fallback",
        "traverse counts only target-admitted nodes toward result limit",
        "graph query engine shares traversal work across start nodes",
        "stored graph weights are finite and non-negative",
        "canonical graph admission preserves and validates weight bounds",
        "graph edge type policy is byte-bounded UTF-8",
        "graph durable writes reject invalid edge types before mutation",
        "graph edge encoding round-trip",
        "canonical graph result node path is self-consistent",
        "canonical path weight sum rejects non-finite accumulation",
        "joining paths is allocation-failure safe",
        "path weight overflow has a stable public diagnostic",
        "anchor scans have an independent request-wide budget",
        "retained expansion state has an explicit byte ceiling",
        "retained lease accounts allocation replacement peak",
        "retained lease rejects allocation replacement peak without leaking",
        "traversal preflights live frontier admission before ownership transfer",
        "traversal ancestry and returned paths share retained state budget",
        "projected MATCH rows reserve and release retained output bytes",
        "shortest path preflights live frontier admission",
        "shortest path retained payloads use the shared request budget",
        "consumed path state detaches its request-scoped release hook",
        "distributed bounded paths retain non-dominated cost and depth labels",
        "distributed frontier reservations precede allocation and release on deinit",
        "anchor-only aggregate fails closed at the shared anchor scan ceiling",
        "k shortest paths preserve parallel typed edge identities",
        "k shortest paths share one cumulative work budget across spur searches",
        "conjunctive fixed edges preserve self loops while variable paths remain node simple",
        "conjunctive match supports branches anti joins inequality and optional nulls",
        "conjunctive validation rejects disconnected and unused aliases",
        "conjunctive validation bounds total recursive pattern shape",
        "exact conjunctive aggregate does not inherit row expansion window",
        "exact distinct aggregates share a fail-closed identity and byte budget",
        "conjunctive matcher admits anchors before alias evaluation",
        "prevalidated conjunctive anchors skip duplicate checks but reached nodes remain guarded",
        "conjunctive anchor selection prefers filters and ignores declaration order",
        "bounded conjunctive matches stream complete rows before the intermediate-state budget",
        "variable length conjunctive edge preserves simple path multiplicity",
        "conjunctive cycle closure survives node admission deduplication",
        "conjunctive reverse expansion uses the declared cross-table source alias",
        "cross-table reverse variable expansion fails closed before reading adjacency",
        "cross-table both preflights every physical source before streaming",
        "conjunctive cross-table directions use physical source routing in every execution mode",
        "complete graph match anchors discard retrieval shaping",
        "complete graph match anchor scan is independent per named operation",
        "complete graph match anchor scan reports native filter coverage failures",
        "qualified graph endpoint requires coordination for a single source group",
        "exact two-edge pattern uses typed batch probes without paths",
        "exact two-edge probe plan is equivalent to generic expansion",
        "exact two-edge probe honors incoming final direction",
        "exact two-edge probe preserves fixed-edge self loops",
        "exact endpoint constrains the final pattern step before limiting",
        "exact pattern targets preserve table identity",
        "inapplicable exact plan does not consume generic fallback budget",
        "graph exact edge probes stay aligned and preserve payloads",
        "graph bounded adjacency pages preserve order and fail before budget overflow",
        "api http client preserves remote graph edge budget exhaustion",
        // Own the complete fast API query module as one stable lane. Exact
        // per-test entries let new admission and ownership regressions compile
        // out of CI until somebody remembered to extend this list.
        "api.query.test.",
        "graph operation execution order is independent of declaration order",
        "graph operation execution order rejects cycles",
        "graph query dependency sorting enforces request-wide operation bounds",
        "graph query dependency sorting accepts path result endpoints",
        "stateful path results materialize endpoint nodes for result refs",
        "pattern response omits paths unless requested",
        "canonical graph binding responses require exact projected alias sets",
        "graph aggregate response preserves exact decimal counts",
        "graph aggregate response fails closed on missing or inexact results",
        "graph response encoding requires exactly one result per traversal operation",
        "canonical path responses require one terminal node per path",
        "canonical traversal responses keep paths on bounded result nodes",
        "canonical graph paths preserve table-qualified node identities",
        "canonical graph path objective exposes max weight product",
        "canonical graph path metadata safely reads legacy non-object records",
        "canonical graph result nodes fail closed outside the public contract",
        "canonical graph path edges enforce durable type policy",
        "remote canonical graph nodes reject invalid identity and depth domains",
        "remote canonical graph result stats and aggregate exactness fail closed",
        "api query contract preserves algebraic graph path provenance",
        "api query contract owns the admitted graph wire for exact proxying",
        "api query contract preserves opaque legacy graph operation names",
        "graph wire envelope capture normalizes nulls and escaped dialect names",
        "graph wire envelope validates dialect and exact operation set once",
        "graph wire envelope preserves allocator failures",
        "graph response format uses admitted metadata and fails closed on plan drift",
        "deprecated graph search preserves its response envelope",
        "admitted graph dialect drives the owned deprecation signal",
        "canonical graph contract rejects modes without exact public execution",
        "generated stateful graph result union decodes pre-discriminator legacy responses",
        "api query contract preflight summarizes query lanes and result refs",
        "parse supported graph queries accepts pattern requests",
        "graph node filters reject analyzer-backed text clauses",
        "canonical graph document filter variants cross the public storage boundary",
        "canonical graph boolean field filter has one unambiguous root",
        "raw graph admission rejects recursive edge shapes above the contract budget",
        "parse supported graph queries accepts branches predicates optional groups and counts",
        "parse supported graph queries rejects distinct field on count all",
        "graph query dependencies require compatible explicit outputs",
        "resolve graph selector fails closed for unbounded paged result refs",
        "distributed graph edges request preserves typed graph edge access path",
        "distributed graph edge reader routes outgoing and fans out incoming adjacency",
        "distributed graph expand request preserves algebraic semiring planning flag",
        "distributed graph complete anchors require the source snapshot",
        "distributed graph complete anchor pages require strict cursor order",
        "distributed graph paged anchors use page completion instead of cursor-relative totals",
        "distributed graph paged execution trusts only source-filtered anchors across cursor pages",
        "distributed graph retries once on topology change and succeeds",
        "distributed graph stops after single retry on repeated topology churn",
        "distributed graph duplicate distinct aggregates share one result payload",
        "distributed graph exact distinct stream budget spans cursor pages",
        "distributed graph exact distinct budget spans named operations",
        "distributed graph canonical MATCH admission excludes retrieval predicates",
        "distributed graph target refs are table exact while raw keys remain wildcard",
        "distributed graph MATCH binding refs preserve table identity and deduplicate",
        "distributed graph executes result dependencies before declaration order",
        "distributed graph path materialization preserves table provenance",
        "distributed canonical path weight is the checked raw edge sum",
        "distributed K path identity preserves parallel typed edges",
        "distributed Yen edge exclusions preserve table-qualified path identity",
        "distributed graph supports legacy pattern step reverse directions exactly",
        "pattern hit shaping is lazy but preserves graph dependencies",
        "graph result refs select one MATCH binding without duplicate seeds",
        "distinct graph aggregates include table identity",
        "db unfiltered graph search retains algebraic execution",
        "db preflightSearchRequest validates live lane bindings",
        "db graph search filters result nodes and hidden traversal intermediates",
        "db graph shortest path searches through admitted alternatives",
        "db graph artifact external node targets return ids without document hydration",
        "db graph hydration rejects table-qualified entity nodes in local snapshots",
        "db index repair streams graph artifact rebuild in batches",
        "api distributed graph cross-table hydrate enforces target authorization",
        "public table query handler maps exact graph execution failures",
        "unsupported graph diagnostics identify the rejected operation feature",
        "authenticated single-group graph queries require distributed coordination",
        "graph table queries have one fresh-topology retry",
        "generic shard query wire preserves admitted canonical graph operations without reparsing",
        "generic shard query wire fails closed without an admitted graph fragment",
        "generic shard query wire never drops graph table authorization",
        "graph edge metadata accepts only the public object shape",
        "unsupported graph query modes fail closed",
        "parseRemoteSearchResult preserves typed graph rows and hydrated documents",
        "parseRemoteSearchResult preserves canonical graph path table identities",
    };
    const lib_unit_filters = selectTestFilters(b, &lib_unit_default_filters);
    const lib_unit_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = compileFiltersWithAnchors(b, &.{ "api module compiles", "metadata module compiles" }, lib_unit_filters),
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lib_unit_tests = b.addRunArtifact(lib_unit_tests);
    addRuntimeTestFilters(b, run_lib_unit_tests, lib_unit_filters);
    for (root_test_skip_filters) |filter| {
        run_lib_unit_tests.addArgs(&.{ "--skip-test-filter", filter });
    }
    const root_test_step = b.step("root-test", "Run fast root-module compile smoke tests");
    root_test_step.dependOn(&run_lib_unit_tests.step);

    const lib_bedrock_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"bedrock provider request helpers"},
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lib_bedrock_tests = addFilteredTestRunArtifact(b, lib_bedrock_tests);
    const lib_bedrock_test_step = b.step("antfly-inference-bedrock-test", "Run focused Bedrock provider tests");
    lib_bedrock_test_step.dependOn(&run_lib_bedrock_tests.step);

    const api_http_runtime_default_filters = [_][]const u8{
        "system catalog",
        "model-directed",
        "tool query builder",
        "agent conversation",
        "embedded canonical generation",
        "table contract admits and preserves multi-source index requests",
        "table contract enforces stable graph source identities and numeric targets",
        "table contract admits and projects explicit embedding vector space",
        "create index request defers upstream artifact resolution to merged catalog",
        "table contract rejects malformed multi-source members",
        "table contract rejects ambiguous index source spellings",
        "table contract preserves typed artifact-backed graph configuration",
        "created index configs normalize single-source input forms",
        "merged index metadata validates artifact consumer references",
        "graph config accepts canonical single-source mappings without a discriminator",
        "created graph index response projects closed nested schemas",
        "index encoders expose graph sources once in normalized config",
        "api http client round-trips public status and internal capability routes",
        "api http client requires explicit not-proposed marker and tracks delivery phase",
        "index activation client preserves progress and transport classifications",
        "api http retryable embedding failures provide retry guidance",
        "api http server obtains query embedding policy from resource manager",
        "api http query budget rejection response exposes stable sort reason",
        "api query contract enforces provider-specific reranker candidate limits",
        "api query contract targets named full text retrieval without changing primary filters",
        "metadata.query routing validates named full text retrieval and keeps schema filters separate",
        "encode query request preserves the singular named full text selector across shard forwarding",
        "api http stale hierarchy cursor response is actionable and machine readable",
        "api http server preserves public query availability errors",
        "public table query handler preserves retryable failure status",
        "api http unsupported unsorted query response is machine readable",
        "api http unsupported hierarchy grouping response uses the public contract",
        "api http point lookup retries bounded local readiness races",
        "api http retry clock translates native query deadlines",
        "api http retry sleep is bounded by request deadline",
        "api http transient read retry honors expired request deadline before source query",
        "api http transient read retry stops before source query when client cancellation is signaled",
        "api http hierarchy traversal preserves policy and cursor across remote hydration seam",
        "api http public sort gate accepts synthetic hierarchy child positions",
        "api http public sort capability gate validates mapped sortable fields",
        "api http public sort capability gate fails closed for uncovered observed dynamic fields",
        "api http server create table with local writes waits for projected presence without lifecycle",
        "api http server rejects oversized table definitions before parsing across public and MCP",
        "api http server reports exhausted table mutation authority consistently",
        "api http server marks every proven table mutation pre-admission failure",
        "api http server retries only pre-admission public table drop failures",
        "ambiguous mutation response is explicitly non-retryable",
        "authoritative catalog mutation boundaries reject orphaned semantic producers",
        "stamped catalog mutations retain their commit stamp when the post-commit round fails",
        "public index mutations preserve an explicit outcome-unknown contract",
        "api http server exposes ambiguous index mutations without a replay signal",
        "routed table mutation preserves hop budget for provably unsent request",
        "api http server create index installs exact visible config and defers lagging projection",
        "table-wide native vector work does not block an independently ready index",
        "api http server drop table observes metadata absence before local cleanup",
        "status source reports an absent linearizable read capability without failing",
        "status source rejects every partial routing capability",
        "table read source distinguishes unavailable physical capability observation",
        "generated route policy inventory is unique and describes wire modes",
        "linked API dispatch preserves kernel-owned ingress policy",
        "opaque host middleware protects direct internal routes across the kernel ABI",
        "linked transport projects the universal request cancellation callback",
        "linked transport admits a streaming body before the kernel pulls it",
        "linked callbacks preserve streaming and cancellation semantics",
        "outbound stream callbacks preserve terminal status classes",
        "outbound callbacks prefer cancellation that arrives during transport IO",
        "linked request bodies remain lazy and transport neutral",
        "native executor borrows validate before reconstructing std.Io",
        "httpx production path sheds 128 abandoned queries and preserves control recovery",
        "httpx write admission rejects saturated table mutations",
        "httpx request lifecycle hook suspends after admission without leaking capacity",
        "httpx owned response preserves retryable JSON metadata",
        "httpx inference connection uses the configured shared admission owner",
        "local inference connection admission is owned exactly once by its target",
        "httpx inference connection requires inference write permission",
        "httpx inference connection propagates failures after stream commit",
        "httpx retrieval SSE",
        "retrieval agent sse",
        "retrieval agent streaming emits go-shaped tree",
        "inference connection invocation forwards streaming and deadline through stable target ABI",
        "inference invocation remaining deadline rounds up and expires",
        "inference connection ABI reclaims partial responses on target failure",
        "inference connection ABI rejects malformed responses without dereferencing invalid ownership",
        "local inference connection ABI retains C layout and validates capabilities",
        "local inference response validation contains malformed ownership",
        "inference connection invocation requires inference write permission",
        "httpx inference connection preserves upstream retry guidance",
        "api http client preserves exact-group join unavailability and absence",
        "distributed join translates native and borrowed deadline boundaries",
        "distributed join context forwards one absolute deadline to every query callback",
        "distributed graph translates native worker and catalog deadline boundaries",
        "query embedding cache translates native query deadlines",
        "typed internal HTTP errors preserve conflict semantics",
        "internal transaction HTTP responses prove not-proposed only before decision",
        "internal transaction ingress establishes and validates pre-decision deadline",
        "request admission bounds positive capacity and preserves unlimited mode",
        "request admission lease releases exactly once",
        "request admission metrics use the shared admission namespace",
        "gzip request completes with combined encoded and decoded budget",
        "shared application admission covers MCP query and write operations",
        "API kernel ABI rejects mismatched context and function-table prefixes",
        "runtime HTTP values retain C layout",
    };
    const api_http_runtime_filters = selectTestFilters(b, &api_http_runtime_default_filters);
    const api_http_runtime_tests = b.addTest(.{
        .root_module = api_http_runtime_test_mod,
        .filters = api_http_runtime_filters,
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_api_http_runtime_tests = addFilteredTestRunArtifactWithRuntimeFilters(
        b,
        api_http_runtime_tests,
        api_http_runtime_filters,
    );
    const api_http_runtime_test_step = b.step("antfly-api-http-runtime-test", "Run focused API HTTP and linked-boundary tests");
    api_http_runtime_test_step.dependOn(&run_api_http_runtime_tests.step);
    root_test_step.dependOn(&run_api_http_runtime_tests.step);

    const introducer_tests = b.addTest(.{
        .root_module = introducer_test_mod,
        .filters = selectTestFilters(b, &.{}),
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_introducer_tests = addFilteredTestRunArtifact(b, introducer_tests);
    const introducer_test_step = b.step("introducer-test", "Run segment introducer unit tests");
    introducer_test_step.dependOn(&run_introducer_tests.step);

    const lite_native_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/lite_native_test.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, lite_native_test_mod, true, true);
    lite_native_test_mod.addImport("antfly_openapi_specs", antfly_imports.embedded_openapi);
    const lite_native_tests = b.addTest(.{
        .root_module = lite_native_test_mod,
        .filters = &.{"storage.lite."},
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lite_native_tests = addFilteredTestRunArtifact(b, lite_native_tests);
    const lite_native_test_step = b.step("lite-native-test", "Run Lite native backend tests");
    lite_native_test_step.dependOn(&run_lite_native_tests.step);

    const cmd_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/cmd_test.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, cmd_test_mod, true, true);
    cmd_test_mod.addImport("antfly_openapi_specs", antfly_imports.embedded_openapi);
    const cmd_usermgr_storage_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/usermgr/storage_imports.zig"),
        .target = target,
        .optimize = optimize,
    });
    cmd_usermgr_storage_mod.addImport("antfly_root", cmd_test_mod);
    cmd_usermgr_storage_mod.addImport("antfly_platform", platform_mod);
    cmd_test_mod.addImport("usermgr_storage", cmd_usermgr_storage_mod);
    cmd_test_mod.addImport("antfly-zig", antfly_mod);
    cmd_test_mod.addImport("antfly-client", antfly_client_pkg_mod);
    const cmd_tests = b.addTest(.{
        .root_module = cmd_test_mod,
        .filters = &.{
            "cmd.lite",
            "cmd.serverless",
            "cmd.cli.backup",
            "cmd.cli.index",
            "cmd.cli.query",
            "cmd.cli.agents",
            "cmd.cli.table",
            "cmd.cli.mod",
            "cmd.cli.data.test.mutation parser",
            "cmd.cli.data.test.load parser",
            "cmd.cli.data.test.checkpoint validation rejects changed source and load config",
        },
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
        // Mach-O debug codegen currently peaks a little above 12 GiB for this
        // intentionally broad command root. Linux remains within the
        // aggregate's 7 GiB claim.
        .max_rss = @as(usize, if (target.result.os.tag == .macos) 13 else 7) * 1024 * 1024 * 1024,
    });
    const run_cmd_tests = addFilteredTestRunArtifact(b, cmd_tests);
    const cmd_test_step = b.step("cmd-test", "Run Antfly command and client CLI tests");
    cmd_test_step.dependOn(&run_cmd_tests.step);

    const lite_cmd_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/lite_cmd_test.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, lite_cmd_test_mod, true, true);
    lite_cmd_test_mod.addImport("antfly_openapi_specs", antfly_imports.embedded_openapi);
    const lite_cmd_usermgr_storage_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/usermgr/storage_imports.zig"),
        .target = target,
        .optimize = optimize,
    });
    lite_cmd_usermgr_storage_mod.addImport("antfly_root", lite_cmd_test_mod);
    lite_cmd_usermgr_storage_mod.addImport("antfly_platform", platform_mod);
    lite_cmd_test_mod.addImport("usermgr_storage", lite_cmd_usermgr_storage_mod);
    lite_cmd_test_mod.addImport("antfly-zig", antfly_mod);
    lite_cmd_test_mod.addImport("antfly-client", antfly_client_pkg_mod);
    const lite_cmd_tests = b.addTest(.{
        .root_module = lite_cmd_test_mod,
        .filters = &.{ "cmd.lite", "cmd.cli.backup", "cmd.cli.index", "cmd.cli.query", "cmd.cli.agents", "cmd.cli.table", "cmd.cli.mod" },
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lite_cmd_tests = addFilteredTestRunArtifact(b, lite_cmd_tests);
    const lite_cmd_test_step = b.step("lite-cmd-test", "Run command tests owned by Antfly Lite profiles");
    lite_cmd_test_step.dependOn(&run_lite_cmd_tests.step);

    const recall_test_step = b.step("antfly-storage-vectorindex-recall-test", "Run HBC vector recall quality tests");

    const raft_unit_default_filters = [_][]const u8{"raft."};
    const raft_unit_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = selectTestFilters(b, &raft_unit_default_filters),
    });
    const run_raft_unit_tests = addFilteredTestRunArtifact(b, raft_unit_tests);
    const checkpoint_host_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"managed host default"},
    });
    b.step("antfly-system-catalog-host-test", "Run metadata checkpoint host persistence regressions").dependOn(&b.addRunArtifact(checkpoint_host_tests).step);

    const raft_read_gate_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/raft_read_gate_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, raft_read_gate_test_mod, true, true);
    const raft_read_gate_tests = b.addTest(.{
        .root_module = raft_read_gate_test_mod,
        .filters = &.{"raft.read_gate."},
    });
    const run_raft_read_gate_tests = b.addRunArtifact(raft_read_gate_tests);
    b.step("raft-read-gate-test", "Run synchronous read ownership and restart identity contracts").dependOn(&run_raft_read_gate_tests.step);

    // The Antfly-rooted Raft tests below cover integration call sites but do
    // not collect tests declared by the raft library's own root module.
    const raft_runtime_default_filters = [_][]const u8{
        "http host reserves service workers through its runtime and rolls back overcommit",
        "managed raft progress driver advances independently and joins on stop",
        "managed raft progress driver publishes source failure",
        "managed raft progress driver reports a wedged round unhealthy",
        "managed raft progress driver ignores a completed observed generation",
        "managed raft progress driver stop interrupts a long cadence wait",
        "managed host service preserves leader-routed observation roles from transition ops",
        "managed host service seeds queued transitions from projected metadata store",
        "raft runtime cadence validates independent intervals",
        "hosted shard db adapter rediscovers median key after stale leader route",
        "shard operation adapter metadata runtime dispatches actions",
        "transition destination requires a stable healthy voter set",
        "transition retry jitter is bounded and desynchronizes services",
        "transition service",
        "raft scheduler ready priority cannot starve consensus ticks",
    };
    const raft_runtime_tests = b.addTest(.{
        .root_module = raft_runtime_test_mod,
        .filters = selectTestFilters(b, &raft_runtime_default_filters),
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_raft_runtime_tests = addFilteredTestRunArtifact(b, raft_runtime_tests);

    const raft_restore_tests = b.addTest(.{
        .root_module = raft_restore_test_mod,
        .filters = &.{
            "host restores through an explicitly authorized bootstrap owner",
            "host restores backup bootstrap replicas from file-backed catalog on restart",
            "managed host restores backup bootstrap replicas from file-backed catalog on restart",
            "host does not perform path restore without a bootstrap authority owner",
            "host records backup restore bootstrap failure when no handler is available",
            "file replica catalog persists backup restore bootstrap records across reopen",
            "replica catalog rejects invalid backup restore authority and integrity bindings",
            "restore binding pins the authenticated native generation manifest",
            "prepared native restore repair reuses target backend admission",
            "backup restore bootstrap adopts an exact imported generation while repair holds a reader",
        },
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_raft_restore_tests = b.addRunArtifact(raft_restore_tests);

    const raft_ready_continuation_tests = b.addTest(.{
        .root_module = raft_engine_mod,
        .filters = &.{
            "multi raft drainReady continues async pipeline without starving peer",
            "multi raft drainReady does not retry a no-progress frontier",
            "multi raft drainReady reserves continuations for productive groups",
            "multi raft empty drain remains allocation free after group admission",
            "multi raft backpressure rejects async ready before cloning messages",
            "multi raft routes outbound snapshots through snapshot transport",
        },
    });
    const run_raft_ready_continuation_tests = addFilteredTestRunArtifact(b, raft_ready_continuation_tests);

    const raft_transport_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{ "raft integration module compiles", "raft.transport." },
    });
    const run_raft_transport_tests = addFilteredTestRunArtifact(b, raft_transport_tests);

    // Snapshot artifact storage has its own root because Zig does not collect
    // tests from the implementation behind the transport compatibility alias.
    // Keep the target component-wide rather than naming an individual policy
    // regression so new storage contracts are discovered automatically.
    const raft_storage_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/raft_storage_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, raft_storage_test_mod, true, true);
    const raft_storage_tests = b.addTest(.{
        .root_module = raft_storage_test_mod,
        .filters = selectTestFilters(b, &.{}),
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_raft_storage_tests = addFilteredTestRunArtifact(b, raft_storage_tests);
    const raft_snapshot_maintenance_vopr_tests = b.addTest(.{
        .root_module = raft_storage_test_mod,
        .filters = &.{
            "raft snapshot storage tests are reachable",
            "file snapshot maintenance uses borrowed scheduling",
        },
    });
    const run_raft_snapshot_maintenance_vopr_tests = b.addRunArtifact(raft_snapshot_maintenance_vopr_tests);
    const raft_snapshot_maintenance_vopr_step = b.step(
        "raft-snapshot-maintenance-vopr-test",
        "Run snapshot maintenance scheduling, wakeup, and shutdown contracts on VoprIo",
    );
    raft_snapshot_maintenance_vopr_step.dependOn(&run_raft_snapshot_maintenance_vopr_tests.step);

    // Keep this as the stable behavioral suffix of the declaration rather
    // than duplicating its descriptive worker-model prefix. The exact-filter
    // runner still fails when the regression is no longer declared.
    const http_low_fd_ratchet_filter = "recovers descriptors after cancellation storms";
    const http_low_fd_ratchet_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        // Compile the shared HTTP module for declaration reachability, but
        // execute only the process-level regression below. The wider common
        // and transport buckets contain high-cardinality socket tests that do
        // not fit inside this target's 256-descriptor process limit.
        .filters = &.{ "common.", http_low_fd_ratchet_filter },
    });
    const run_http_low_fd_ratchet_tests = addFilteredTestRunArtifactWithRuntimeFilters(
        b,
        http_low_fd_ratchet_tests,
        &.{http_low_fd_ratchet_filter},
    );
    const http_low_fd_ratchet_test_step = b.step(
        "http-low-fd-ratchet-test",
        "Run the process-level low-FD HTTP worker ratchet regression",
    );
    http_low_fd_ratchet_test_step.dependOn(&run_http_low_fd_ratchet_tests.step);

    const lib_raft_harness_default_filters = [_][]const u8{
        "virtual http network exposes selected message transitions",
        "managed host simulation drives add and peer refresh through deterministic steps",
        "managed host simulation restores through both raft state backends",
        "managed host simulation keeps WAL replay debt bounded across repeated proposals",
        "managed host simulation removes routes and replicas across deterministic steps",
        "simulation harness module compiles",
        "cluster simulation validates mirrored merge pair invariants",
        "cluster simulation validates split transition enrichment invariants",
        "cluster simulation validates merge transition enrichment invariants",
        "cluster simulation drives split transition actions deterministically",
        "cluster simulation drives merge transition actions deterministically",
    };
    const lib_raft_harness_tests = b.addTest(.{
        .root_module = raft_harness_test_mod,
        .filters = &lib_raft_harness_default_filters,
    });
    const run_lib_raft_harness_tests = addFilteredTestRunArtifact(b, lib_raft_harness_tests);
    const lib_raft_harness_test_step = b.step("lib-raft-harness-test", "Run the legacy Raft deterministic harness tests");
    lib_raft_harness_test_step.dependOn(&run_lib_raft_harness_tests.step);

    const lib_raft_chaos_default_filters = [_][]const u8{
        "managed host simulation restores through both raft state backends",
        "managed host simulation persists replica removal across restart for both raft state backends",
        "managed host simulation drops queued metadata updates across restart for both raft state backends",
        "managed host simulation does not persist proposals before a runtime round across both raft state backends",
        "managed http host simulation starts listener and applies deterministic metadata updates",
        "managed http host simulations elect and replicate over real HTTP",
        "managed http host simulation can remove and rejoin from HTTP snapshot fetch",
        "managed http cluster simulation",
        "http host simulation drives queued split transitions through the service lane",
        "http host simulation rolls back and retries queued split transitions through the service lane",
        "http host simulation removes queued split transition mid-flight",
        "http host simulation updates split transition to rollback mid-flight",
        "cluster simulation drives queued split transitions through service-owned metadata updates",
        "cluster simulation resumes queued split transitions after node restart",
        "cluster simulation ignores active split removal and rolls back explicitly across restart",
        "cluster simulation rolls back queued split transition mid-flight across node restart",
        "cluster simulation survives repeated same-id split overwrites across restart",
        "cluster simulation drives queued merge transitions through service-owned metadata updates",
        "http host simulation drives queued merge transitions through the service lane",
        "http host simulation rolls back and retries queued merge transitions through the service lane",
        "http host simulation removes queued merge transition mid-flight",
        "http host simulation updates merge transition to rollback mid-flight",
        "cluster simulation resumes queued merge transitions after node restart",
        "cluster simulation rolls back queued merge transition mid-flight across node restart",
        "cluster simulation survives repeated same-id merge overwrites across restart",
        "cluster simulation isolates concurrent",
        "cluster simulation drives multiple concurrent real transition ids through multiplexed runtime",
        "cluster simulation isolates overlapping same-id split overwrites while other transitions complete",
        "cluster simulation ignores active merge removal and rolls back explicitly across restart",
    };
    const lib_raft_chaos_tests = b.addTest(.{
        .root_module = raft_harness_test_mod,
        .filters = &lib_raft_chaos_default_filters,
    });
    const run_lib_raft_chaos_tests = addFilteredTestRunArtifact(b, lib_raft_chaos_tests);
    const lib_raft_chaos_test_step = b.step("antfly-raft-chaos-test", "Run longer raft restart/HTTP simulation campaigns");
    lib_raft_chaos_test_step.dependOn(&run_lib_raft_chaos_tests.step);

    const lib_raft_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"raft VOPR"},
    });
    const run_lib_raft_vopr_tests = addFilteredTestRunArtifact(b, lib_raft_vopr_tests);
    const lib_raft_vopr_test_step = b.step("raft-vopr-test", "Run replayable per-group Raft VOPR campaigns");
    lib_raft_vopr_test_step.dependOn(&run_lib_raft_vopr_tests.step);

    const lib_lsm_backend_workload_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"lsm backend simulation"},
    });
    const run_lib_lsm_backend_workload_tests = addFilteredTestRunArtifact(b, lib_lsm_backend_workload_tests);
    const lib_lsm_backend_workload_test_step = b.step("lib-lsm-backend-workload-test", "Run legacy LSM backend storage workload tests");
    lib_lsm_backend_workload_test_step.dependOn(&run_lib_lsm_backend_workload_tests.step);

    const lib_lsm_backend_chaos_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"lsm backend compaction chaos campaign"},
    });
    const run_lib_lsm_backend_chaos_tests = addFilteredTestRunArtifact(b, lib_lsm_backend_chaos_tests);
    const lib_lsm_backend_chaos_test_step = b.step("antfly-storage-lsm-backend-chaos-test", "Run longer LSM backend compaction chaos campaigns");
    lib_lsm_backend_chaos_test_step.dependOn(&run_lib_lsm_backend_chaos_tests.step);

    const lib_lsm_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"LSM VOPR"},
    });
    const run_lib_lsm_vopr_tests = addFilteredTestRunArtifact(b, lib_lsm_vopr_tests);
    const lib_lsm_vopr_test_step = b.step("lsm-vopr-test", "Run replayable real-backend LSM VOPR campaigns");
    lib_lsm_vopr_test_step.dependOn(&run_lib_lsm_vopr_tests.step);
    const lib_ha_chaos_default_filters = [_][]const u8{
        "storage.ha chaos crash during base backup preserves slot pin and catch-up boundary",
        "storage.ha chaos crash after receive replays durable WAL before streaming resumes",
        "storage.ha chaos rejects noncontiguous records and follows timeline switch across restart",
        "storage.ha chaos crash during apply preserves remote write and blocks remote apply",
        "storage.ha chaos crash after apply before ack reports durable progress on resume",
        "storage.ha chaos primary restart preserves synchronous acknowledgement boundaries",
        "storage.ha chaos lag retention forces reseed and former primary cannot rewind expired WAL",
        "storage.ha chaos network partition requires fence before standby promotion",
    };
    const lib_ha_chaos_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = selectTestFilters(b, &lib_ha_chaos_default_filters),
    });
    const run_lib_ha_chaos_tests = addFilteredTestRunArtifact(b, lib_ha_chaos_tests);
    const lib_ha_chaos_test_step = b.step("antfly-storage-ha-chaos-test", "Run HA hot-standby crash and partition hardening tests");
    lib_ha_chaos_test_step.dependOn(&run_lib_ha_chaos_tests.step);
    const lib_ha_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"HA VOPR"},
    });
    const run_lib_ha_vopr_tests = addFilteredTestRunArtifact(b, lib_ha_vopr_tests);
    const lib_ha_vopr_test_step = b.step("ha-vopr-test", "Run replayable HA lifecycle VOPR campaigns");
    lib_ha_vopr_test_step.dependOn(&run_lib_ha_vopr_tests.step);
    const lib_ha_compat_default_filters = [_][]const u8{
        "storage.ha compat decodes v1 replication record fixture",
        "storage.ha compat keeps v1 replication record encoding stable",
        "storage.ha compat decodes v1 timeline switch record fixture",
        "storage.ha compat keeps v1 timeline switch encoding stable",
        "storage.ha compat decodes v1 base backup and checkpoint record fixtures",
        "storage.ha compat keeps v1 base backup and checkpoint encodings stable",
        "storage.ha compat decodes v1 backup manifest fixture",
        "storage.ha compat keeps v1 backup manifest encoding stable",
        "storage.ha compat keeps v1 backup manifest file kind tags stable",
        "storage.ha compat keeps v1 record kind tags stable",
    };
    const lib_ha_compat_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = selectTestFilters(b, &lib_ha_compat_default_filters),
    });
    const run_lib_ha_compat_tests = addFilteredTestRunArtifact(b, lib_ha_compat_tests);
    const lib_ha_compat_test_step = b.step("antfly-storage-ha-compat-test", "Run HA replication format compatibility tests");
    lib_ha_compat_test_step.dependOn(&run_lib_ha_compat_tests.step);

    const antfly_test_step = b.step("antfly-test", "Run default Antfly unit, VOPR, integration, chaos, and recall checks");

    const unit_test_step = b.step("antfly-unit-test", "Run hermetic unit and focused integration test buckets without metadata chaos simulations");

    const serverless_default_filters = [_][]const u8{"serverless"};
    const serverless_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &serverless_default_filters,
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_serverless_tests = addFilteredTestRunArtifact(b, serverless_tests);
    const serverless_test_step = b.step("antfly-serverless-test", "Run serverless and serverless transport tests");
    serverless_test_step.dependOn(&run_serverless_tests.step);

    const serverless_manifest_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/serverless_manifest_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, serverless_manifest_test_mod, true, true);
    serverless_manifest_test_mod.addImport("antfly_openapi_specs", antfly_imports.embedded_openapi);
    const serverless_manifest_tests = b.addTest(.{
        .root_module = serverless_manifest_test_mod,
        .filters = &.{
            "objectstore-backed manifest store supports publish and list",
            "serverless retention",
            "serverless manifest GC floor",
            "manifest head CAS verifies a stat ETag when GET omits it",
            "objectstore-backed manifest store resolves conditional create races by content",
            "host object storage delegates through callbacks",
        },
        .test_runner = .{ .path = b.path("pkg/antfly/src/test_runner.zig"), .mode = .simple },
    });
    const run_serverless_manifest_tests = addFilteredTestRunArtifact(b, serverless_manifest_tests);
    const serverless_manifest_test_step = b.step("antfly-serverless-manifest-test", "Run focused serverless manifest object-store tests");
    serverless_manifest_test_step.dependOn(&run_serverless_manifest_tests.step);

    const lake_scaffold_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/lake_scaffold_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, lake_scaffold_test_mod, true, true);
    lake_scaffold_test_mod.addImport("antfly_openapi_specs", antfly_imports.embedded_openapi);
    const lake_scaffold_tests = b.addTest(.{
        .root_module = lake_scaffold_test_mod,
        .filters = &.{ "lake", "parquet", "iceberg", "external source", "row fragment", "sidecar" },
        .test_runner = .{ .path = b.path("pkg/antfly/src/test_runner.zig"), .mode = .simple },
    });
    const run_lake_scaffold_tests = addFilteredTestRunArtifact(b, lake_scaffold_tests);
    const lake_test_step = b.step("lake-test", "Run Antfly lake-native tests");
    lake_test_step.dependOn(&run_lake_scaffold_tests.step);
    unit_test_step.dependOn(&run_lake_scaffold_tests.step);

    const data_tests_addTests_result = data_tests.addTests(b, .{
        .target = target,
        .data_runtime_test_mod = data_runtime_test_mod,
        .data_storage_test_mod = data_storage_test_mod,
    });
    const run_lib_data_runtime_tests = data_tests_addTests_result.run_lib_data_runtime_tests;
    const run_lib_data_storage_tests = data_tests_addTests_result.run_lib_data_storage_tests;

    const db_tests_addTests_result = db_tests.addTests(b, .{
        .antfly_test_mod = antfly_test_mod,
    });
    const run_lib_db_txn_tests = db_tests_addTests_result.run_lib_db_txn_tests;
    const run_lib_db_enrichment_tests = db_tests_addTests_result.run_lib_db_enrichment_tests;
    const run_lib_db_result_shape_tests = db_tests_addTests_result.run_lib_db_result_shape_tests;

    const metadata_tests_addTests_result = metadata_tests.addTests(b, .{
        .target = target,
        .antfly_test_mod = antfly_test_mod,
    });
    const run_lib_metadata_vopr_data_tests = metadata_tests_addTests_result.run_lib_metadata_vopr_data_tests;
    const lib_metadata_runtime_filters = metadata_tests_addTests_result.lib_metadata_runtime_filters;
    const lib_metadata_test_step = metadata_tests_addTests_result.lib_metadata_test_step;
    const run_lib_metadata_vopr_virtual_smoke_tests = metadata_tests_addTests_result.run_lib_metadata_vopr_virtual_smoke_tests;
    const run_lib_metadata_vopr_tests = metadata_tests_addTests_result.run_lib_metadata_vopr_tests;
    const lib_metadata_vopr_chaos_tests = metadata_tests_addTests_result.lib_metadata_vopr_chaos_tests;
    const lib_metadata_vopr_transition_chaos_filters = metadata_tests_addTests_result.lib_metadata_vopr_transition_chaos_filters;
    const lib_metadata_vopr_public_chaos_filters = metadata_tests_addTests_result.lib_metadata_vopr_public_chaos_filters;
    const lib_metadata_vopr_placement_chaos_filters = metadata_tests_addTests_result.lib_metadata_vopr_placement_chaos_filters;
    const run_lib_metadata_vopr_public_integration_tests = metadata_tests_addTests_result.run_lib_metadata_vopr_public_integration_tests;

    const api_tests_addTests_result = api_tests.addTests(b, .{
        .api_http_runtime_test_mod = api_http_runtime_test_mod,
        .lmdb_engine = lmdb_engine_mod,
        .vopr = vopr_mod,
        .optimize = optimize,
        .openapi_root_check = openapi_root_check,
        .antfly_imports = antfly_imports,
        .antfly_test_mod = antfly_test_mod,
        .run_lib_usermgr_tests = run_lib_usermgr_tests,
    });
    root_test_step.dependOn(api_tests_addTests_result.distributed_query_availability_step);
    const run_lib_api_standalone_backup_restore_tests = api_tests_addTests_result.run_lib_api_standalone_backup_restore_tests;
    const run_public_api_parity_aggregate_tests = api_tests_addTests_result.run_public_api_parity_aggregate_tests;
    const run_lib_api_auth_tests = api_tests_addTests_result.run_lib_api_auth_tests;
    const run_algebraic_dynamic_template_tests = api_tests_addTests_result.run_algebraic_dynamic_template_tests;
    const run_lib_api_connections_tests = api_tests_addTests_result.run_lib_api_connections_tests;
    const run_lib_api_storage_authority_tests = api_tests_addTests_result.run_lib_api_storage_authority_tests;
    const api_table_writes_docid_test_mod = api_tests_addTests_result.api_table_writes_docid_test_mod;
    const run_lib_api_docid_tests = api_tests_addTests_result.run_lib_api_docid_tests;
    const api_derived_coverage_test_mod = api_tests_addTests_result.api_derived_coverage_test_mod;
    const run_lib_api_derived_coverage_tests = api_tests_addTests_result.run_lib_api_derived_coverage_tests;
    const run_lib_serverless_docid_tests = api_tests_addTests_result.run_lib_serverless_docid_tests;
    const run_api_transactions_docid_tests = api_tests_addTests_result.run_api_transactions_docid_tests;
    const run_api_table_writes_docid_tests = api_tests_addTests_result.run_api_table_writes_docid_tests;
    const run_api_table_reads_docid_tests = api_tests_addTests_result.run_api_table_reads_docid_tests;
    const run_api_public_table_http_docid_tests = api_tests_addTests_result.run_api_public_table_http_docid_tests;
    const run_raft_transition_runtime_docid_tests = api_tests_addTests_result.run_raft_transition_runtime_docid_tests;
    const run_api_table_writes_production_regression_unit_tests = api_tests_addTests_result.run_api_table_writes_production_regression_unit_tests;
    const run_lib_docid_lifecycle_tests = api_tests_addTests_result.run_lib_docid_lifecycle_tests;

    const openapi_root_check_step = b.step("openapi-root-check", "Check that the bundled root OpenAPI spec matches the modular Zig specs");
    openapi_root_check_step.dependOn(&openapi_root_check.step);

    const lib_metadata_vopr_forwarding_integration_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"forwards public table io"},
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lib_metadata_vopr_forwarding_integration_tests = addFilteredTestRunArtifact(b, lib_metadata_vopr_forwarding_integration_tests);
    const lib_metadata_vopr_forwarding_integration_test_step = b.step("lib-metadata-vopr-forwarding-integration-test", "Run public table I/O forwarding integration tests only");
    lib_metadata_vopr_forwarding_integration_test_step.dependOn(&run_lib_metadata_vopr_forwarding_integration_tests.step);

    const lib_metadata_service_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{
            "metadata service ",
            "cdc work permit ",
            "metadata proposal receipt ",
            "metadata reconciliation plan uses one terminal receipt for ordered apply",
            "table workflow cancellation stops before reconciliation lease work",
            "table workflow can drive real metadata service topology and split setup",
            "table workflow can drive placement intents through the real metadata control loop",
            "metadata http service catalog cache is independent from volatile projection traffic",
            "lifecycle listener detach drains callbacks and preserves unrelated listeners",
            "metadata.table mutation routing forwards only to a routable remote leader",
            "metadata http client forwards table create and drop to the internal route",
            "metadata http client rejects invalid forwarded table names before I/O",
            "metadata http client surfaces typed rejection for forwarded table mutations only with non-admission proof",
            "metadata http client preserves transport ambiguity for forwarded table mutations",
            "metadata http client preserves extension ownership across forwarding",
            "metadata http client preserves unrecognized server outcomes for forwarded table mutations",
            "metadata http client does not replay unmarked table mutation rejection proof",
            "metadata http client round-trips server endpoints",
            "stamped definition replacement falls back to v0.2 text route",
            "definition replacement does not replay an ambiguous admitted request",
            "routed table mutation",
            "forwarded create body limit",
            "table mutation names preserve the public contract",
            "stored create table encoding",
            "raft mutation ",
            "table topology mutation ",
            "metadata http server preserves extension-owned table drop conflicts",
            "metadata http server replaces a table definition through compare-and-swap",
            "extension lifecycle proposal",
        },
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lib_metadata_service_tests = addFilteredTestRunArtifact(b, lib_metadata_service_tests);
    const lib_metadata_service_test_step = b.step("antfly-metadata-service-test", "Run metadata service/control-loop integration tests");
    lib_metadata_service_test_step.dependOn(&run_lib_metadata_service_tests.step);

    const lib_metadata_logic_default_filters = [_][]const u8{
        "metadata reconciler",
        "transition state",
        "metadata server ",
        "metadata admin maps retryable authority loss to service unavailable",
        "metadata merge request validation rejects incompatible doc identity namespaces",
        "metadata split request validation rejects stale doc identity namespace",
        "transition actions",
        "placement planner",
        "metadata control loop proposes desired transitions through the service seam",
        "metadata control loop plans placement intents",
        "metadata control loop installs service median key lookup for automatic split planning",
        "table manager ",
        "metadata state ",
        "transition controller ",
        "metadata module compiles",
        "metadata transition driver ",
        "metadata storage module compiles",
        "metadata cluster incarnation has one canonical JSON representation",
        "metadata authority retry classification is fail closed",
        "table workflow can build desired topology through the control loop seam",
        "table workflow doc identity guards reject active transition intents",
        "table workflow can remove a table topology from desired state",
        "table workflow can reconcile projected local placement intents",
        "metadata raft apply store ",
        "metadata transition decoders reject unknown enum values",
        "metadata store observer ",
        "metadata state machine projects transitions through metadata apply store",
        "table provisioner restores local shard data from metadata restore intent",
        "table provisioner restore rejects mismatched doc identity namespace",
        "table provisioner replaces embedding index when metadata incarnation changes",
        "table provisioner can admit resolver backfill without draining corpus work",
        "runtime schema progress requires every hosted range",
        "table provisioner accepts target schema index when retained read index has inflated doc count",
        "table provisioner runtime schema progress requires authoritative O(1) identity coverage",
        "catalog table topology is order independent and detects range mutation",
        "metadata route wire conversion preserves its absolute deadline",
        "metadata http server serves status and filtered admin routes",
        "metadata admin linearizable snapshot propagates request context",
        "metadata linearizable snapshot fences and frees one owned response",
        "metadata linearizable snapshot detects concurrent projection changes",
        "coherent linearizable snapshot retries a torn capture and preserves request context",
        "metadata http client signs internal routes without leaking authority to public routes",
        "metadata http client fetches one bounded linearizable snapshot",
        "stamped definition replacement falls back to v0.2 text route",
        "definition replacement does not replay an ambiguous admitted request",
        "metadata http client treats missing linearizable snapshot route as unsupported",
        "metadata http server accepts internal reallocate and split merge routes",
        "metadata http server returns 400 for invalid internal restore backup locations",
        "metadata http server returns retryable authority response when reconcile lease is not held",
    };
    const lib_metadata_logic_runtime_filters = selectTestFilters(b, &lib_metadata_logic_default_filters);
    const lib_metadata_logic_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = compileFiltersWithAnchors(
            b,
            &.{"metadata."},
            lib_metadata_logic_runtime_filters,
        ),
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lib_metadata_logic_tests = addFilteredTestRunArtifactWithRuntimeFilters(
        b,
        lib_metadata_logic_tests,
        lib_metadata_logic_runtime_filters,
    );
    const lib_metadata_logic_test_step = b.step("antfly-metadata-logic-test", "Run metadata logic/state/planner tests");
    lib_metadata_logic_test_step.dependOn(&run_lib_metadata_logic_tests.step);

    const lib_storage_default_filters = [_][]const u8{
        "storage.",
    };
    const lib_storage_runtime_filters = selectTestFilters(b, &lib_storage_default_filters);
    const lib_storage_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = lib_storage_runtime_filters,
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lib_storage_tests = addFilteredTestRunArtifact(b, lib_storage_tests);
    addRuntimeSkipTestFilters(run_lib_storage_tests, &release_scale_test_filters);
    const lib_storage_test_step = b.step("antfly-storage-test", "Run root-module storage tests only");
    lib_storage_test_step.dependOn(&run_lib_storage_tests.step);

    const ha_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"storage.ha"},
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_ha_tests = addFilteredTestRunArtifact(b, ha_tests);
    const ha_test_step = b.step("antfly-storage-ha-test", "Run hot-standby HA storage tests");
    ha_test_step.dependOn(&run_ha_tests.step);

    // cmd/ha.zig is owned by the distributed runtime unit. Keep its focused
    // parser root inside pkg/antfly/src so relative imports stay within the Zig
    // module boundary, without pulling the command back into the CLI unit.
    const ha_cli_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/ha_cmd_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    ha_cli_test_mod.addImport("antfly-zig", antfly_mod);
    const ha_cli_tests = b.addTest(.{
        .root_module = ha_cli_test_mod,
        .filters = &.{"ha cmd artifact"},
    });
    const run_ha_cli_tests = b.addRunArtifact(ha_cli_tests);
    ha_test_step.dependOn(&run_ha_cli_tests.step);

    const lsm_backend_runtime_filters = selectTestFilters(b, &.{"storage.lsm_backend."});
    const lsm_backend_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = compileFiltersWithAnchors(
            b,
            &.{"lsm backend module tests are reachable"},
            lsm_backend_runtime_filters,
        ),
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lsm_backend_tests = addFilteredTestRunArtifactWithRuntimeFilters(
        b,
        lsm_backend_tests,
        lsm_backend_runtime_filters,
    );
    const lsm_backend_test_step = b.step("lsm-backend-test", "Run LSM backend unit tests only");
    lsm_backend_test_step.dependOn(&run_lsm_backend_tests.step);

    const resource_budget_runtime_filters = [_][]const u8{
        "default tokenizer cache budget is aligned with its resource slice",
        "default lake range cache queue budget is aligned with its terminal resource slice",
        "identity allocation failure rolls back every memory ledger",
        "manager teardown retires live observer snapshots",
        "batch reservation is atomic across inference resource slices",
        "classified batch reservation distinguishes size from contention",
        "aggregate host memory admission is atomic across slices",
        "bounded observer growth grants aggregate slice and host capacity atomically",
        "owned split reservations prevent concurrent headroom theft",
        "owned split reservations reclaim before partial fallback",
        "owned split secondary credit transfers into a budgeted allocator",
        "pinned split credit survives idle allocation windows",
        "logical inference slices can charge only physical host memory",
        "batch release accounting errors fail closed",
        "single release and observer mismatch cannot debit unrelated memory",
        "bounded oversized progress cannot bypass aggregate host memory",
        "resource manager observes over-budget external usage",
        "identity-aware cache admission rejects growth and always permits shrink",
        "resource manager evaluates projected admission with configured action",
        "resource manager bounds soft write throttling without waiting for compaction publication",
        "resource manager records index repair activation pause separately from cleanup",
        "catchUpIndex refuses to open an apply window after its deadline",
        "cache reports shared byte usage to resource manager",
        "cache falls back to a transient handle when retention exceeds the resource envelope",
        "cache transfers existing usage when resource manager changes",
        "shared LSM cache yields to foreground aggregate admission",
        "shared LSM resource reclaimer never waits for active accounting",
        "lsm backend resource manager throttles projected immutable state",
        "lsm backend resource manager reclaims local durable state before rejecting",
        "derived backlog tracker accounts payload and sequence ownership",
        "derived backlog tracker fails closed when sequence accounting allocation fails",
        "derived backlog tracker bounds sequence-only admission drain window",
        "hbc shared cache namespaces entries",
        "hbc index reports shared cache ownership",
        "hbc shared cache evicts across namespaces under one resource budget",
        "hbc shared cache CLOCK refreshes recency on borrowed vector hits",
        "hbc shared vector replacement cannot return an older external value",
        "hbc vector fill captured before a committed mutation cannot repopulate stale data",
        "hbc shared detached leases remain physically accounted until release",
        "hbc standalone detached leases remain physically accounted until release",
        "hbc standalone cache yields to foreground aggregate admission",
        "hbc resource reclaimer never waits for an active cache owner",
        "hbc concurrent vector admission samples at a full steady target",
        "hbc exact-route vector admission samples outside the search epoch",
        "hbc decoded residency lease reserves a complete query and bypasses mid-query sampling",
        "hbc sampled decoded residency evolves a full resident set within its byte target",
        "hbc decoded residency fails closed when pinned entries prevent precharge",
        "hbc route observation counts external distance timing once",
        "dense vector load session switches to retained LSM ownership before reservation overrun",
        "production external vector session evolves a saturated decoded resident set",
        "hbc shared cache reclaims exact vectors before protected routing nodes",
        "hbc shared cache reclaims an over-quota namespace for a borrowing peer",
        "hbc shared vector cache warms during concurrent search",
        "hbc external rerank loads metadata only for decoded vector misses",
        "hbc shared vector publication coalesces concurrent duplicate fills",
        "hbc retained node and quantized handles survive threaded eviction",
        "hbc vector artifact reads avoid duplicate LSM block residency only with retained vectors",
        "searchWithRequest applies filter prefix and distance bounds",
        "hbc cache reports byte usage to resource manager",
        "hbc search charges estimated quantized scan bytes to node admission",
        "dense search bandwidth admission is FIFO and work weighted",
        "dense search bandwidth admission removes cancelled waiters",
        "hbc resource manager reattachment is idempotent and transfers local cache usage",
        "hbc cache shrinks to resource budget under pressure",
        "resource manager derives elastic HBC cache-class policy from pressure",
        "resource manager bounds adaptive HBC benefit-per-byte targets",
        "adaptive HBC benefit retains miss cost through all-hit samples",
        "resource manager apportions reclaim across weighted cache owners",
        "resource manager invokes reclaimers without holding registry mutex",
        "foreground admission reclaims cache bytes and retries atomically",
        "classified batch chooses foreground requester when cache slice is first",
        "resource-managed mapped residency evicts cold segments and preserves hot mappings",
        "provisioned group storage derives all resource budgets",
        "provisioned dense native authority gate is fail-closed and monotonic",
        "provisioned lsm cache is an elastic share of the node envelope",
        "provisioned HBC cache is an elastic share of the node envelope",
        "standalone resource manager derives elastic storage cache envelopes",
        "effective process memory limit preserves source and clamps explicit requests",
        "resource manager capacity source is immutable after composition",
        "capacity percentage safety floor is capped on large volumes",
        "capacity reservation revalidation fails closed when available space falls",
        "resource manager background deferral follows slice policy",
        "budgeted allocator admits before allocation and releases exact live bytes",
        "budgeted allocator reclamation is opt in and retries without raising limits",
        "budgeted allocator reclaim denial does not retry a busy cache",
        "budgeted allocator allows concurrent operations within the shared hard limit",
        "budgeted allocator amortizes manager reservations and releases idle credit",
    };
    // Retain the API declaration walk that owns provisioned_storage. Zig
    // compile filters otherwise prune that module before the exact runtime
    // filter can select its resource-budget test.
    const resource_budget_compile_filters = [_][]const u8{"api module compiles"} ++ resource_budget_runtime_filters;
    const resource_budget_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &resource_budget_compile_filters,
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_resource_budget_tests = addFilteredTestRunArtifactWithRuntimeFilters(
        b,
        resource_budget_tests,
        &resource_budget_runtime_filters,
    );
    const filesystem_capacity_tests = b.addTest(.{
        .root_module = filesystem_capacity_test_mod,
        .filters = &.{"filesystem capacity probe reports the test volume"},
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_filesystem_capacity_tests = addFilteredTestRunArtifact(b, filesystem_capacity_tests);
    // Keep the filesystem probe in the default resource-budget lane instead of
    // adding another concurrently runnable unit-test process. The default unit
    // graph is intentionally broad, and an extra process here can turn short
    // listener/storage timing tests into load-dependent failures.
    run_resource_budget_tests.step.dependOn(&run_filesystem_capacity_tests.step);
    const resource_budget_test_step = b.step("resource-budget-test", "Run storage resource-manager accounting tests");
    resource_budget_test_step.dependOn(&run_resource_budget_tests.step);

    const dense_index_lifecycle_regression_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{
            "posting WAL capacity",
            "native posting initial acceleration",
            "db online vector publication",
            "db multi-source dense target",
            "index repair state root-generation reset atomically rebinds replacement debt",
            "index repair state persists through backend storage",
            "index repair state persists intent and provisional replay pin atomically",
            "index generation manifest is durable and fenced by identity",
            "db managed operator repair persists intent without running reconstruction inline",
            "index repair advance lease covers cancellation and deletion",
            "db restart reconciles activated dense repair without rebuilding",
            "db restart clears stale dense generation intent after clean checkpoint",
            "db missing activation certification rolls back to serviceable dense predecessor",
            "db missing activation certification exposes predecessor action required",
            "db failed activated dense generation rolls back to retained predecessor",
            "db root generation rollover preserves activated repair debt fail closed",
            "rollback action required atomically retires activation certification",
            "db repair capacity converts materialized shadow bytes into consumed reservation",
            "db automatic dense repair bootstraps missing coverage metadata",
            "db dense artifact rebuild bootstraps missing counter metadata",
            "db dense artifact rebuild force-resets corrupt external dense structure",
            "db asynchronous dense replay lag is not classified as repair debt",
            "db dense artifact rebuild rejects clean checkpoint for stale config identity",
            "db forced repair attaches to automatic generation intent idempotently",
            "db forced repair preserves missing-counter fail-closed classification",
            "db forced dense repair stays fail closed until background health proof",
            "db forced dense repair keeps structurally invalid generation fail closed",
            "db quarantined dense bootstrap tracks concurrent insert update and delete",
            "db inline dense generation remains rebuilding until outcomes cover the live corpus",
            "db dense shadow activation rejects surplus candidate coverage",
            "db document artifact child range batch atomically tracks dense artifact counters",
            "db ttl delete callback atomically removes dense artifacts and updates repair counters",
            "db replay skips a missing dense artifact after its source document was deleted",
            "db replay blocks dense embedding writes when artifact payload is missing",
            "db replay blocks and preserves corrupt dense embedding artifacts",
            "db repeated replay preserves nonblocking dense artifact repair intent",
            "db dense artifact surplus uses quarantined generation replacement",
            "db dense artifact planner does not let stale status override authoritative counter",
            "db dense artifact counter bootstrap combines snapshot with concurrent write delta",
            "db dense artifact counter bootstrap restarts from a fresh snapshot",
            "db dense artifact counter bootstrap fences stale concurrent attempt",
            "db malformed quarantined dense config does not block healthy artifact counters",
            "db query repair gate revalidates stale debt",
            "db status cannot reopen a quarantined generation from an older publication certificate",
            "db status retains certified canonical admission during shadow build handoff",
            "db initial replay repair retains certified canonical admission during shadow reconstruction",
            "db empty managed index does not invent generated coverage recovery debt",
            "db repair preflight retains a canonical generation completed after scheduler selection",
            "db coverage recovery admits a published generation after its admission marker retires",
            "db dense repair working set scales batch to resource budget",
            "db dense counter bootstrap admission respects soft background budget",
            "managed startup catch-up advances counterless incomplete dense repair",
            "provisioned leader admission rejects uncommitted writes under dense repair pressure",
            "api maintenance resumes recovered durable named index cancellation without client advance",
            "bulk publication revalidates admission before every publish window",
        },
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_dense_index_lifecycle_regression_tests = addFilteredTestRunArtifact(b, dense_index_lifecycle_regression_tests);
    const dense_index_repair_job_tests = b.addTest(.{
        .root_module = api_artifact_reprocess_jobs_test_mod,
        .filters = &.{
            "forced index repair job dispatches force only once",
            "index repair job keeps degradation gauges as snapshots across retries",
            "named index repair cancellation remains nonterminal until durable controls finish",
            "named index repair cancellation restarts its durable traversal after job store recovery",
            "durable cancellation scan rotates past a backed off head window",
            "table repair job recovery quarantines corrupt primary without blocking service",
            "active repair job recovery quarantines malformed secondary entries",
        },
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_dense_index_repair_job_tests = addFilteredTestRunArtifact(b, dense_index_repair_job_tests);
    const dense_index_repair_status_tests = b.addTest(.{
        .root_module = api_derived_coverage_test_mod,
        .filters = &.{
            "index status exposes compact repair state without internal diagnostics",
            "index status aggregation preserves actionable repair diagnostics for the requested incarnation",
            "actionable repair remains visible while retained generation stays queryable",
            "serviceable full text replacement remains queryable while rebuilding",
            "serviceable repair preserves sibling shard dense catch-up fallback",
            "serviceable repair cannot mask sibling shard load failure",
            "index encoders preserve sibling replay debt during serviceable repair",
        },
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_dense_index_repair_status_tests = addFilteredTestRunArtifact(b, dense_index_repair_status_tests);
    const dense_index_repair_runtime_tests = b.addTest(.{
        .root_module = data_runtime_test_mod,
        .filters = &.{
            "data runtime repair debt hook targets the affected group queue",
            "data runtime repair failures preserve durable backoff and increase retry delay",
            "index repair fallback backoff never blocks an exact durable wake",
        },
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_dense_index_repair_runtime_tests = addFilteredTestRunArtifact(b, dense_index_repair_runtime_tests);
    const dense_index_lifecycle_regression_step = b.step(
        "dense-index-lifecycle-regression-test",
        "Run focused durable dense-index repair and admission regressions",
    );
    dense_index_lifecycle_regression_step.dependOn(&run_dense_index_lifecycle_regression_tests.step);
    dense_index_lifecycle_regression_step.dependOn(&run_dense_index_repair_job_tests.step);
    dense_index_lifecycle_regression_step.dependOn(&run_dense_index_repair_status_tests.step);
    dense_index_lifecycle_regression_step.dependOn(&run_dense_index_repair_runtime_tests.step);

    const vopr_contract_test_mod = b.createModule(.{
        .root_source_file = b.path("lib/vopr/src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    const vopr_contract_tests = b.addTest(.{ .root_module = vopr_contract_test_mod });
    const run_vopr_contract_tests = b.addRunArtifact(vopr_contract_tests);
    const vopr_engine_test_step = b.step("vopr-engine-test", "Run the standalone VOPR engine and replay tests");
    vopr_engine_test_step.dependOn(&run_vopr_contract_tests.step);
    const vopr_contract_test_step = b.step("vopr-contract-test", "Run deterministic VOPR contract and replay-equivalence tests");
    vopr_contract_test_step.dependOn(&run_vopr_contract_tests.step);

    const vopr_benchmark_mod = b.createModule(.{
        .root_source_file = b.path("lib/vopr/src/benchmark_main.zig"),
        .target = target,
        .optimize = .ReleaseSafe,
    });
    vopr_benchmark_mod.addImport("vopr", b.dependency("vopr", .{ .target = target, .optimize = .ReleaseSafe }).module("vopr"));
    const vopr_benchmark = b.addExecutable(.{ .name = "vopr-benchmark", .root_module = vopr_benchmark_mod });
    const vopr_benchmark_step = b.step("vopr-benchmark", "Run deterministic VOPR search-efficiency benchmarks");
    vopr_benchmark_step.dependOn(&b.addRunArtifact(vopr_benchmark).step);

    const vopr_cli_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/vopr/cli.zig"),
        .target = target,
        .optimize = optimize,
    });
    vopr_cli_mod.addImport("antfly", antfly_test_mod);
    vopr_cli_mod.addImport("vopr", vopr_mod);
    vopr_cli_mod.link_libc = true;
    // Antfly's VOPR scenarios deliberately use std.testing facilities.
    // A custom runner makes the test artifact behave as a normal command-line
    // program while retaining the harness-only compilation contract.
    const vopr_cli = b.addTest(.{
        .name = "vopr",
        .root_module = vopr_cli_mod,
        .test_runner = .{
            .path = b.path("pkg/antfly/src/vopr/cli_runner.zig"),
            .mode = .simple,
        },
    });
    const vopr_cli_meta_tests = b.addTest(.{
        .root_module = vopr_cli_mod,
        .filters = &.{"Antfly injected bug is discovered replayed reduced and promoted"},
        .max_rss = @as(usize, if (target.result.os.tag == .macos) 16 else 7) * 1024 * 1024 * 1024,
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_vopr_cli_meta_tests = b.addRunArtifact(vopr_cli_meta_tests);
    const vopr_meta_test_step = b.step("vopr-meta-test", "Prove Antfly VOPR discovery, replay, reduction, and promotion end to end");
    vopr_meta_test_step.dependOn(&run_vopr_cli_meta_tests.step);
    const vopr_cli_registry_tests = b.addTest(.{
        .root_module = vopr_cli_mod,
        .filters = &.{"VOPR scenario registry"},
        .max_rss = @as(usize, if (target.result.os.tag == .macos) 16 else 7) * 1024 * 1024 * 1024,
    });
    const run_vopr_cli_registry_tests = b.addRunArtifact(vopr_cli_registry_tests);
    const vopr_registry_test_step = b.step("vopr-registry-test", "Record and exact-replay every context-free VOPR scenario through the CLI registry");
    vopr_registry_test_step.dependOn(&run_vopr_cli_registry_tests.step);

    const transaction_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"transaction VOPR exactly replays and emits a formal sidecar"},
    });
    const run_transaction_vopr_tests = b.addRunArtifact(transaction_vopr_tests);
    const transaction_vopr_test_step = b.step("transaction-vopr-test", "Run deterministic transaction VOPR and formal trace export tests");
    transaction_vopr_test_step.dependOn(&run_transaction_vopr_tests.step);

    const distributed_transaction_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"distributed transaction lifecycle VOPR records and exact replays"},
    });
    const run_distributed_transaction_vopr_tests = b.addRunArtifact(distributed_transaction_vopr_tests);
    const distributed_transaction_vopr_test_step = b.step("distributed-transaction-vopr-test", "Run distributed transaction lifecycle VOPR campaigns");
    distributed_transaction_vopr_test_step.dependOn(&run_distributed_transaction_vopr_tests.step);
    distributed_transaction_vopr_test_step.dependOn(&run_transaction_vopr_tests.step);
    distributed_transaction_vopr_test_step.dependOn(&run_lib_db_txn_tests.step);
    distributed_transaction_vopr_test_step.dependOn(&run_api_transactions_docid_tests.step);

    const data_plane_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"data plane microstep VOPR records and exact replays"},
    });
    const run_data_plane_vopr_tests = b.addRunArtifact(data_plane_vopr_tests);
    const data_plane_vopr_test_step = b.step("data-plane-vopr-test", "Run data-plane routing, persistence, apply, split, and read VOPR campaigns");
    data_plane_vopr_test_step.dependOn(&run_data_plane_vopr_tests.step);
    data_plane_vopr_test_step.dependOn(&run_lib_metadata_vopr_data_tests.step);
    data_plane_vopr_test_step.dependOn(&run_lib_raft_vopr_tests.step);

    const request_lifecycle_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"request lifecycle adapter records stable VoprIo safepoints"},
    });
    const run_request_lifecycle_vopr_tests = b.addRunArtifact(request_lifecycle_vopr_tests);
    const request_lifecycle_vopr_test_step = b.step(
        "request-lifecycle-vopr-test",
        "Run production request lifecycle suspension points on VoprIo",
    );
    request_lifecycle_vopr_test_step.dependOn(&run_request_lifecycle_vopr_tests.step);
    request_lifecycle_vopr_test_step.dependOn(&run_api_http_runtime_tests.step);
    data_plane_vopr_test_step.dependOn(&run_request_lifecycle_vopr_tests.step);

    const replication_backfill_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{
            "replication lifecycle adapter records stable VoprIo safepoints",
            "replication backfill service rates compose and heal across production snapshot and stream",
            "replication backfill VOPR exact replays every production recovery mode",
        },
    });
    const run_replication_backfill_vopr_tests = b.addRunArtifact(replication_backfill_vopr_tests);
    const replication_backfill_vopr_test_step = b.step(
        "replication-backfill-vopr-test",
        "Run production replication lifecycle suspension points and VOPR campaigns",
    );
    replication_backfill_vopr_test_step.dependOn(&run_replication_backfill_vopr_tests.step);

    const supervision_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"standalone serverless supervision VOPR exact replays lifecycle failures"},
    });
    const run_supervision_vopr_tests = b.addRunArtifact(supervision_vopr_tests);
    const supervision_vopr_test_step = b.step(
        "supervision-vopr-test",
        "Run standalone and serverless startup, failure, shutdown, deadline, and restart campaigns",
    );
    supervision_vopr_test_step.dependOn(&run_supervision_vopr_tests.step);

    const auth_lifecycle_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"user auth lifecycle VOPR exact replays rotate revoke reload and crash recovery"},
    });
    const run_auth_lifecycle_vopr_tests = b.addRunArtifact(auth_lifecycle_vopr_tests);
    const auth_lifecycle_vopr_test_step = b.step(
        "auth-lifecycle-vopr-test",
        "Run password, API-key, permission, row-filter, seed, revoke, reload, and crash campaigns",
    );
    auth_lifecycle_vopr_test_step.dependOn(&run_auth_lifecycle_vopr_tests.step);

    const data_server_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .max_rss = production_vopr_compile_max_rss,
        .filters = &.{
            "production DataServer public HTTP",
            "production HTTP lifecycle runs chunked keep-alive pipeline and stream on VoprIo",
            "DataServer LSM maintenance owner runs on borrowed VoprIo",
            "DataServer LSM maintenance cost port composes and heals on borrowed VoprIo",
            "DataServer VOPR background owner executes and cancels maintenance on VoprIo",
            "production DataServer replicated merge actions run on VoprIo",
        },
    });
    const run_data_server_vopr_tests = b.addRunArtifact(data_server_vopr_tests);
    const data_server_transition_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{
            "three production DataServers compose replicated merge and split across public writes failover and restart on VoprIo",
            "inline replicated split action failure releases its transition lane exactly once",
        },
    });
    const run_data_server_transition_vopr_tests = b.addRunArtifact(data_server_transition_vopr_tests);
    const data_server_transition_vopr_test_step = b.step(
        "data-server-transition-vopr-test",
        "Run the isolated three-owner replicated merge/split VOPR history",
    );
    data_server_transition_vopr_test_step.dependOn(&run_data_server_transition_vopr_tests.step);
    const data_server_vopr_test_step = b.step(
        "data-server-vopr-test",
        "Run production DataServer HTTP, ownership, and replicated merge/split actions on VoprIo",
    );
    data_server_vopr_test_step.dependOn(&run_data_server_vopr_tests.step);
    data_server_vopr_test_step.dependOn(&run_data_server_transition_vopr_tests.step);
    data_server_vopr_test_step.dependOn(&run_request_lifecycle_vopr_tests.step);
    data_plane_vopr_test_step.dependOn(&run_data_server_vopr_tests.step);

    const serverless_object_store_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"serverless object store VOPR"},
    });
    const run_serverless_object_store_vopr_tests = b.addRunArtifact(serverless_object_store_vopr_tests);
    const serverless_object_store_vopr_test_step = b.step(
        "serverless-object-store-vopr-test",
        "Run real serverless object-store protocols with deterministic faults",
    );
    serverless_object_store_vopr_test_step.dependOn(&run_serverless_object_store_vopr_tests.step);

    const serverless_workflow_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{
            "serverless workflow service rates compose and heal across publish and compaction",
            "complete serverless workflow VOPR exact replays",
        },
    });
    const run_serverless_workflow_vopr_tests = b.addRunArtifact(serverless_workflow_vopr_tests);
    const serverless_workflow_vopr_test_step = b.step(
        "serverless-workflow-vopr-test",
        "Run claim, build, compaction, publication, visibility, and recovery histories",
    );
    serverless_workflow_vopr_test_step.dependOn(&run_serverless_workflow_vopr_tests.step);

    const db_index_race_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"DB index request races VOPR exact replays"},
    });
    const run_db_index_race_vopr_tests = b.addRunArtifact(db_index_race_vopr_tests);
    const db_index_race_vopr_test_step = b.step(
        "db-index-race-vopr-test",
        "Run DB/index delete, materialization, capture, admission, cancellation, and shutdown races",
    );
    db_index_race_vopr_test_step.dependOn(&run_db_index_race_vopr_tests.step);

    const admission_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{
            "resource admission VOPR",
            "cross-service resource pressure VOPR",
        },
    });
    const run_admission_vopr_tests = b.addRunArtifact(admission_vopr_tests);
    const admission_vopr_test_step = b.step(
        "admission-vopr-test",
        "Run shared production resource admission, quota, cancellation, and recovery under deterministic VOPR contention",
    );
    admission_vopr_test_step.dependOn(&run_admission_vopr_tests.step);

    const provider_boundary_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"provider boundary VOPR exact replays"},
    });
    const run_provider_boundary_vopr_tests = b.addRunArtifact(provider_boundary_vopr_tests);
    const provider_boundary_vopr_test_step = b.step(
        "provider-boundary-vopr-test",
        "Run inference and PostgreSQL response-boundary fault campaigns",
    );
    provider_boundary_vopr_test_step.dependOn(&run_provider_boundary_vopr_tests.step);

    const composed_query_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"composed query lifecycle VOPR exact replays"},
    });
    const run_composed_query_vopr_tests = b.addRunArtifact(composed_query_vopr_tests);
    const composed_query_vopr_test_step = b.step(
        "composed-query-vopr-test",
        "Run vector, text, graph, and global-query assembly fault campaigns",
    );
    composed_query_vopr_test_step.dependOn(&run_composed_query_vopr_tests.step);

    const query_embedding_cache_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"query embedding cache VOPR exact replays"},
    });
    const run_query_embedding_cache_vopr_tests = b.addRunArtifact(query_embedding_cache_vopr_tests);
    const query_embedding_cache_vopr_test_step = b.step(
        "query-embedding-cache-vopr-test",
        "Run query embedding coalescing, cancellation, timeout, admission, TTL, LRU, pin, and service-rate races on VoprIo",
    );
    query_embedding_cache_vopr_test_step.dependOn(&run_query_embedding_cache_vopr_tests.step);

    // Every filtered full-cluster gate analyzes the same production-heavy
    // root, so use the shared reservation rather than per-mode estimates.
    const full_cluster_vopr_max_rss = production_vopr_compile_max_rss;
    const transaction_runtime_filters: []const []const u8 = &.{
        "table transaction identities borrow runtime entropy and realtime",
        "table transaction recovery preserves fresh transactions on the runtime clock",
        "shared stateless batch retries borrow IO and preserve unknown outcomes",
        "provisioned stateless batch retries definite aborts to the production bound",
    };
    const transaction_runtime_regressions = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = compileFiltersWithAnchors(b, &.{"api module compiles"}, transaction_runtime_filters),
        .test_runner = .{ .path = b.path("pkg/antfly/src/test_runner.zig"), .mode = .simple },
    });
    b.step("transaction-runtime-regression-test", "Check transaction identity, recovery clocks, and safe stateless retries").dependOn(&addCuratedTestRunArtifact(b, transaction_runtime_regressions, transaction_runtime_filters).step);
    const vopr_runtime_regression_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{
            "metadata VOPR distributed data survives split partition node restart and modeled storage crash",
            "metadata VOPR split runtime preserves source identity namespace",
            "metadata VOPR source seeding preserves arbitrary keys and open range bounds",
            "public api linearizable read driver ignores a delayed earlier generation",
            "table transaction identities borrow runtime entropy and realtime",
            "table transaction recovery preserves fresh transactions on the runtime clock",
            "transaction attempt budgets follow the borrowed transport clock",
            "pre-decision context deadline has typed admission provenance",
            "internal transaction ingress establishes and validates pre-decision deadline",
            "table reads translate request deadlines into the routing clock",
            "catalog route fence dispatch is strict and fail closed",
            "metadata raft apply store catalog projection uses storage snapshot independently from apply mutex",
            "db modeled index repair adopts replacements with the serving allocator",
            "db implicit batch timestamps use the borrowed runtime clock",
            "graph workers report retired ranges as topology unavailability",
            "full cluster production data plane VOPR bounded cutoff exact replay",
            "full cluster VOPR exact replays the composed deployment and recovery",
            "full cluster VOPR exact replays resource pressure recovery",
            "shared stateless batch retries borrow IO and preserve unknown outcomes",
            "provisioned stateless batch retries definite aborts to the production bound",
        },
        .max_rss = full_cluster_vopr_max_rss,
        .test_runner = .{ .path = b.path("pkg/antfly/src/test_runner.zig"), .mode = .simple },
    });
    const run_vopr_runtime_regressions = b.addRunArtifact(vopr_runtime_regression_tests);
    if (b.args) |args| run_vopr_runtime_regressions.addArgs(args);
    b.step("vopr-runtime-regression-test", "Run VOPR runtime ownership, clock, snapshot, and replay regressions").dependOn(&run_vopr_runtime_regressions.step);

    const ha_production_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"production HA owners"},
        .max_rss = full_cluster_vopr_max_rss,
    });
    const run_ha_production_vopr_tests = b.addRunArtifact(ha_production_vopr_tests);
    b.step("ha-production-vopr-test", "Exercise production HA public writes, standby reads, and promotion on VoprIo").dependOn(&run_ha_production_vopr_tests.step);

    const ha_scaling_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"production HA scaling VOPR exact replays"},
        .max_rss = full_cluster_vopr_max_rss,
    });
    const run_ha_scaling_vopr_tests = b.addRunArtifact(ha_scaling_vopr_tests);
    run_ha_scaling_vopr_tests.step.dependOn(&run_ha_production_vopr_tests.step);
    b.step("ha-scaling-vopr-test", "Replay production standby promotion with automatic split/merge and replica scale-out/drain").dependOn(&run_ha_scaling_vopr_tests.step);

    const full_cluster_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"full cluster VOPR exact replays"},
        .max_rss = full_cluster_vopr_max_rss,
    });
    const run_full_cluster_vopr_tests = b.addRunArtifact(full_cluster_vopr_tests);
    const full_cluster_vopr_test_step = b.step(
        "full-cluster-vopr-test",
        "Run one shared-scheduler metadata, data, serverless, HTTP, and client deployment history",
    );
    full_cluster_vopr_test_step.dependOn(&run_full_cluster_vopr_tests.step);

    const production_cluster_service_rate_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"full cluster production service rates compose heal and exact replay"},
        .max_rss = full_cluster_vopr_max_rss,
    });
    const run_production_cluster_service_rate_vopr_tests = b.addRunArtifact(production_cluster_service_rate_vopr_tests);
    const production_cluster_service_rate_vopr_test_step = b.step(
        "production-cluster-service-rate-vopr-test",
        "Run composed DataServer, graph, and serverless reversible service-rate history",
    );
    production_cluster_service_rate_vopr_test_step.dependOn(&run_production_cluster_service_rate_vopr_tests.step);

    const production_cluster_query_cache_service_rate_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"full cluster production query embedding cache deadline owner restart and exact replay"},
        .max_rss = full_cluster_vopr_max_rss,
    });
    const run_production_cluster_query_cache_service_rate_vopr_tests = b.addRunArtifact(production_cluster_query_cache_service_rate_vopr_tests);
    const production_cluster_query_cache_service_rate_vopr_test_step = b.step(
        "production-cluster-query-cache-deadline-restart-vopr-test",
        "Run the production ApiHttpServer cache through slowdown, deadline, DataServer restart, recomputation, and durable recovery",
    );
    production_cluster_query_cache_service_rate_vopr_test_step.dependOn(&run_production_cluster_query_cache_service_rate_vopr_tests.step);

    const production_cluster_serverless_fencing_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"full cluster production generation progress conflict exact replay"},
        .max_rss = full_cluster_vopr_max_rss,
    });
    const run_production_cluster_serverless_fencing_vopr_tests = b.addRunArtifact(production_cluster_serverless_fencing_vopr_tests);
    const production_cluster_serverless_fencing_vopr_test_step = b.step(
        "production-cluster-serverless-fencing-vopr-test",
        "Run a stale enrichment generation and losing publication CAS beside live production metadata, DataServers, and public clients",
    );
    production_cluster_serverless_fencing_vopr_test_step.dependOn(&run_production_cluster_serverless_fencing_vopr_tests.step);

    const production_cluster_authenticated_tenant_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"full cluster production authenticated tenant isolation exact replay"},
        .max_rss = full_cluster_vopr_max_rss,
    });
    const run_production_cluster_authenticated_tenant_vopr_tests = b.addRunArtifact(production_cluster_authenticated_tenant_vopr_tests);
    const production_cluster_authenticated_tenant_vopr_test_step = b.step(
        "production-cluster-authenticated-tenant-vopr-test",
        "Run two table-scoped identities through concurrent public writes, reads, bidirectional denials, and exact replay",
    );
    production_cluster_authenticated_tenant_vopr_test_step.dependOn(&run_production_cluster_authenticated_tenant_vopr_tests.step);

    const production_cluster_disk_capacity_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"full cluster production disk capacity pressure exact replay"},
        .max_rss = full_cluster_vopr_max_rss,
    });
    const run_production_cluster_disk_capacity_vopr_tests = b.addRunArtifact(production_cluster_disk_capacity_vopr_tests);
    const production_cluster_disk_capacity_vopr_test_step = b.step(
        "production-cluster-disk-capacity-vopr-test",
        "Run a live DataServer capacity source through persistent-cache denial, healing, retry, and public-read continuity",
    );
    production_cluster_disk_capacity_vopr_test_step.dependOn(&run_production_cluster_disk_capacity_vopr_tests.step);

    const production_cluster_managed_index_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"full cluster production managed index publication recovery exact replay"},
        .max_rss = full_cluster_vopr_max_rss,
    });
    const run_production_cluster_managed_index_vopr_tests = b.addRunArtifact(production_cluster_managed_index_vopr_tests);
    const production_cluster_managed_index_vopr_test_step = b.step(
        "production-cluster-managed-index-vopr-test",
        "Run public managed-index pending readiness through DataServer reconstruction, durable repair, and all-node semantic recovery",
    );
    production_cluster_managed_index_vopr_test_step.dependOn(&run_production_cluster_managed_index_vopr_tests.step);

    const production_cluster_replication_backfill_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"full cluster production replication backfill crosses public data raft and exact replays"},
        .max_rss = full_cluster_vopr_max_rss,
    });
    const run_production_cluster_replication_backfill_vopr_tests = b.addRunArtifact(production_cluster_replication_backfill_vopr_tests);
    const production_cluster_replication_backfill_vopr_test_step = b.step(
        "production-cluster-replication-backfill-vopr-test",
        "Run production snapshot and CDC batches through public HTTP, DataServer Raft, shared slowdown, and exact replay",
    );
    production_cluster_replication_backfill_vopr_test_step.dependOn(&run_production_cluster_replication_backfill_vopr_tests.step);

    const production_cluster_replication_schema_change_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"full cluster production replication schema change resumes through public data raft and exact replays"},
        .max_rss = full_cluster_vopr_max_rss,
    });
    const run_production_cluster_replication_schema_change_vopr_tests = b.addRunArtifact(production_cluster_replication_schema_change_vopr_tests);
    const production_cluster_replication_schema_change_vopr_test_step = b.step(
        "production-cluster-replication-schema-change-vopr-test",
        "Run interrupted schema-change backfill, durable resume, public DataServer Raft visibility, and exact replay",
    );
    production_cluster_replication_schema_change_vopr_test_step.dependOn(&run_production_cluster_replication_schema_change_vopr_tests.step);

    const production_cluster_replication_owner_restart_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"full cluster production replication target owner restarts resumes and exact replays"},
        .max_rss = full_cluster_vopr_max_rss,
    });
    const run_production_cluster_replication_owner_restart_vopr_tests = b.addRunArtifact(production_cluster_replication_owner_restart_vopr_tests);
    const production_cluster_replication_owner_restart_vopr_test_step = b.step(
        "production-cluster-replication-owner-restart-vopr-test",
        "Run replication through a stopped/reconstructed production DataServer owner and exact replay",
    );
    production_cluster_replication_owner_restart_vopr_test_step.dependOn(&run_production_cluster_replication_owner_restart_vopr_tests.step);

    const production_cluster_replication_source_crash_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"full cluster production replication source session crashes resumes and exact replays"},
        .max_rss = full_cluster_vopr_max_rss,
    });
    const run_production_cluster_replication_source_crash_vopr_tests = b.addRunArtifact(production_cluster_replication_source_crash_vopr_tests);
    const production_cluster_replication_source_crash_vopr_test_step = b.step(
        "production-cluster-replication-source-crash-vopr-test",
        "Run replication through a failed/replaced source session and exact replay",
    );
    production_cluster_replication_source_crash_vopr_test_step.dependOn(&run_production_cluster_replication_source_crash_vopr_tests.step);

    const production_cluster_replication_cancellation_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"full cluster production replication durable cancellation resumes and exact replays"},
        .max_rss = full_cluster_vopr_max_rss,
    });
    const run_production_cluster_replication_cancellation_vopr_tests = b.addRunArtifact(production_cluster_replication_cancellation_vopr_tests);
    const production_cluster_replication_cancellation_vopr_test_step = b.step(
        "production-cluster-replication-cancellation-vopr-test",
        "Run replication through durable checkpoint lease cancellation and exact replay",
    );
    production_cluster_replication_cancellation_vopr_test_step.dependOn(&run_production_cluster_replication_cancellation_vopr_tests.step);

    const production_cluster_replication_stale_owner_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"full cluster production replication stale owner replays undurable batch and exact replays"},
        .max_rss = full_cluster_vopr_max_rss,
    });
    const run_production_cluster_replication_stale_owner_vopr_tests = b.addRunArtifact(production_cluster_replication_stale_owner_vopr_tests);
    const production_cluster_replication_stale_owner_vopr_test_step = b.step(
        "production-cluster-replication-stale-owner-vopr-test",
        "Reject a stale replication owner before checkpoint publication, replay idempotently, and exact replay",
    );
    production_cluster_replication_stale_owner_vopr_test_step.dependOn(&run_production_cluster_replication_stale_owner_vopr_tests.step);

    const production_cluster_replication_topology_change_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"full cluster production replication metadata topology rotates cutover authority and exact replays"},
        .max_rss = full_cluster_vopr_max_rss,
    });
    const run_production_cluster_replication_topology_change_vopr_tests = b.addRunArtifact(production_cluster_replication_topology_change_vopr_tests);
    const production_cluster_replication_topology_change_vopr_test_step = b.step(
        "production-cluster-replication-topology-change-vopr-test",
        "Rotate exact-cutover authority through metadata Raft after a replicated source-catalog change and exact replay",
    );
    production_cluster_replication_topology_change_vopr_test_step.dependOn(&run_production_cluster_replication_topology_change_vopr_tests.step);

    const production_cluster_graph_hydration_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"full cluster production public graph hydration exact replay"},
        .max_rss = full_cluster_vopr_max_rss,
    });
    const run_production_cluster_graph_hydration_vopr_tests = b.addRunArtifact(production_cluster_graph_hydration_vopr_tests);
    const production_cluster_graph_hydration_vopr_test_step = b.step(
        "production-cluster-graph-hydration-vopr-test",
        "Run public production-owner graph expansion, document hydration, and exact replay",
    );
    production_cluster_graph_hydration_vopr_test_step.dependOn(&run_production_cluster_graph_hydration_vopr_tests.step);

    const production_cluster_graph_cancellation_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"full cluster production public graph cancellation exact replay"},
        .max_rss = full_cluster_vopr_max_rss,
    });
    const run_production_cluster_graph_cancellation_vopr_tests = b.addRunArtifact(production_cluster_graph_cancellation_vopr_tests);
    const production_cluster_graph_cancellation_vopr_test_step = b.step(
        "production-cluster-graph-cancellation-vopr-test",
        "Run public production-owner graph cancellation, recovery, and exact replay",
    );
    production_cluster_graph_cancellation_vopr_test_step.dependOn(&run_production_cluster_graph_cancellation_vopr_tests.step);

    const production_cluster_graph_cancellation_transport_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"full cluster production public graph cancellation under transport fault exact replay"},
        .max_rss = full_cluster_vopr_max_rss,
    });
    const run_production_cluster_graph_cancellation_transport_vopr_tests = b.addRunArtifact(production_cluster_graph_cancellation_transport_vopr_tests);
    const production_cluster_graph_cancellation_transport_vopr_test_step = b.step(
        "production-cluster-graph-cancellation-transport-fault-vopr-test",
        "Run public graph cancellation with outstanding hydration under a scoped transport outage",
    );
    production_cluster_graph_cancellation_transport_vopr_test_step.dependOn(&run_production_cluster_graph_cancellation_transport_vopr_tests.step);

    const production_cluster_graph_inflight_authorization_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"full cluster production public graph inflight authorization revocation exact replay"},
        .max_rss = full_cluster_vopr_max_rss,
    });
    const run_production_cluster_graph_inflight_authorization_vopr_tests = b.addRunArtifact(production_cluster_graph_inflight_authorization_vopr_tests);
    const production_cluster_graph_inflight_authorization_vopr_test_step = b.step(
        "production-cluster-graph-inflight-authorization-vopr-test",
        "Run in-flight authenticated public cross-table graph revocation, recovery, and exact replay",
    );
    production_cluster_graph_inflight_authorization_vopr_test_step.dependOn(&run_production_cluster_graph_inflight_authorization_vopr_tests.step);

    const production_cluster_graph_stale_snapshot_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"full cluster production public graph stale snapshot retry exhaustion exact replay"},
        .max_rss = full_cluster_vopr_max_rss,
    });
    const run_production_cluster_graph_stale_snapshot_vopr_tests = b.addRunArtifact(production_cluster_graph_stale_snapshot_vopr_tests);
    const production_cluster_graph_stale_snapshot_vopr_test_step = b.step(
        "production-cluster-graph-stale-snapshot-vopr-test",
        "Run public graph stale-snapshot retry exhaustion, recovery, and exact replay",
    );
    production_cluster_graph_stale_snapshot_vopr_test_step.dependOn(&run_production_cluster_graph_stale_snapshot_vopr_tests.step);

    const production_cluster_global_query_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"full cluster production public global query exact replay"},
        .max_rss = full_cluster_vopr_max_rss,
    });
    const run_production_cluster_global_query_vopr_tests = b.addRunArtifact(production_cluster_global_query_vopr_tests);
    const production_cluster_global_query_vopr_test_step = b.step(
        "production-cluster-global-query-vopr-test",
        "Run ordered, table-isolated public global NDJSON query dispatch through production owners",
    );
    production_cluster_global_query_vopr_test_step.dependOn(&run_production_cluster_global_query_vopr_tests.step);

    const production_cluster_global_query_cancellation_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"full cluster production public global query cancellation exact replay"},
        .max_rss = full_cluster_vopr_max_rss,
    });
    const run_production_cluster_global_query_cancellation_vopr_tests = b.addRunArtifact(production_cluster_global_query_cancellation_vopr_tests);
    const production_cluster_global_query_cancellation_vopr_test_step = b.step(
        "production-cluster-global-query-cancellation-vopr-test",
        "Cancel global NDJSON dispatch after its first result and prove no-partial recovery",
    );
    production_cluster_global_query_cancellation_vopr_test_step.dependOn(&run_production_cluster_global_query_cancellation_vopr_tests.step);

    const production_cluster_global_query_authorization_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"full cluster production public global query inflight authorization revocation exact replay"},
        .max_rss = full_cluster_vopr_max_rss,
    });
    const run_production_cluster_global_query_authorization_vopr_tests = b.addRunArtifact(production_cluster_global_query_authorization_vopr_tests);
    const production_cluster_global_query_authorization_vopr_test_step = b.step(
        "production-cluster-global-query-authorization-vopr-test",
        "Revoke live authority between global NDJSON results and prove fail-closed recovery",
    );
    production_cluster_global_query_authorization_vopr_test_step.dependOn(&run_production_cluster_global_query_authorization_vopr_tests.step);

    const production_cluster_global_query_transport_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"full cluster production public global query transport failure exact replay"},
        .max_rss = full_cluster_vopr_max_rss,
    });
    const run_production_cluster_global_query_transport_vopr_tests = b.addRunArtifact(production_cluster_global_query_transport_vopr_tests);
    const production_cluster_global_query_transport_vopr_test_step = b.step(
        "production-cluster-global-query-transport-vopr-test",
        "Cut the second table's production query link after the first result and prove fail-closed recovery",
    );
    production_cluster_global_query_transport_vopr_test_step.dependOn(&run_production_cluster_global_query_transport_vopr_tests.step);

    const production_cluster_global_query_owner_restart_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"full cluster production public global query owner restart exact replay"},
        .max_rss = full_cluster_vopr_max_rss,
    });
    const run_production_cluster_global_query_owner_restart_vopr_tests = b.addRunArtifact(production_cluster_global_query_owner_restart_vopr_tests);
    const production_cluster_global_query_owner_restart_vopr_test_step = b.step(
        "production-cluster-global-query-owner-restart-vopr-test",
        "Destroy the second table's production owner after the first result and prove reconstruction recovery",
    );
    production_cluster_global_query_owner_restart_vopr_test_step.dependOn(&run_production_cluster_global_query_owner_restart_vopr_tests.step);

    const production_cluster_baseline_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"full cluster production data plane baseline exact replay"},
        .max_rss = full_cluster_vopr_max_rss,
    });
    const run_production_cluster_baseline_vopr_tests = b.addRunArtifact(production_cluster_baseline_vopr_tests);
    const production_cluster_bounded_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"full cluster production data plane VOPR bounded cutoff exact replay"},
        .max_rss = full_cluster_vopr_max_rss,
    });
    const run_production_cluster_bounded_vopr_tests = b.addRunArtifact(production_cluster_bounded_vopr_tests);
    // Each production composition may use most of its large RSS allowance.
    // Keep the smoke aggregate deterministic under constrained CI hosts by
    // running the two fresh-world replay processes serially.
    run_production_cluster_bounded_vopr_tests.step.dependOn(&run_production_cluster_baseline_vopr_tests.step);
    const production_cluster_deep_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"full cluster production data plane VOPR active split exact replay"},
        .max_rss = full_cluster_vopr_max_rss,
    });
    const run_production_cluster_deep_vopr_tests = b.addRunArtifact(production_cluster_deep_vopr_tests);
    const production_cluster_graph_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"full cluster production data plane graph exact replay"},
        .max_rss = full_cluster_vopr_max_rss,
    });
    const run_production_cluster_graph_vopr_tests = b.addRunArtifact(production_cluster_graph_vopr_tests);
    const production_cluster_graph_split_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"full cluster production data plane graph active split exact replay"},
        .max_rss = full_cluster_vopr_max_rss,
    });
    const run_production_cluster_graph_split_vopr_tests = b.addRunArtifact(production_cluster_graph_split_vopr_tests);
    const production_cluster_graph_split_transport_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"full cluster production data plane graph active split transport failure exact replay"},
        .max_rss = full_cluster_vopr_max_rss,
    });
    const run_production_cluster_graph_split_transport_vopr_tests = b.addRunArtifact(production_cluster_graph_split_transport_vopr_tests);
    const production_cluster_graph_split_owner_restart_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"full cluster production data plane graph active split owner restart exact replay"},
        .max_rss = full_cluster_vopr_max_rss,
    });
    const run_production_cluster_graph_split_owner_restart_vopr_tests = b.addRunArtifact(production_cluster_graph_split_owner_restart_vopr_tests);
    const production_cluster_graph_split_partial_write_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"full cluster production data plane graph active split partial write exact replay"},
        .max_rss = full_cluster_vopr_max_rss,
    });
    const run_production_cluster_graph_split_partial_write_vopr_tests = b.addRunArtifact(production_cluster_graph_split_partial_write_vopr_tests);
    const production_cluster_graph_split_resource_pressure_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"full cluster production data plane graph active split resource pressure exact replay"},
        .max_rss = full_cluster_vopr_max_rss,
    });
    const run_production_cluster_graph_split_resource_pressure_vopr_tests = b.addRunArtifact(production_cluster_graph_split_resource_pressure_vopr_tests);
    const production_cluster_join_split_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{
            "full cluster production data plane distributed join active split exact replay",
            "production distributed join oracle accepts broadcast without a shuffle ledger",
        },
        .max_rss = full_cluster_vopr_max_rss,
    });
    const run_production_cluster_join_split_vopr_tests = b.addRunArtifact(production_cluster_join_split_vopr_tests);
    const production_cluster_durable_join_takeover_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"full cluster production data plane durable shuffle join finalizer takeover exact replay"},
        .max_rss = full_cluster_vopr_max_rss,
    });
    const run_production_cluster_durable_join_takeover_vopr_tests = b.addRunArtifact(production_cluster_durable_join_takeover_vopr_tests);
    const production_cluster_durable_join_cancellation_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"full cluster production durable shuffle join cancellation exact replay"},
        .max_rss = full_cluster_vopr_max_rss,
    });
    const run_production_cluster_durable_join_cancellation_vopr_tests = b.addRunArtifact(production_cluster_durable_join_cancellation_vopr_tests);
    const production_cluster_durable_join_worker_retry_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"full cluster production durable shuffle partition worker failover exact replay"},
        .max_rss = full_cluster_vopr_max_rss,
    });
    const run_production_cluster_durable_join_worker_retry_vopr_tests = b.addRunArtifact(production_cluster_durable_join_worker_retry_vopr_tests);
    const production_cluster_durable_join_owner_restart_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"full cluster production durable shuffle partition owner reconstruction exact replay"},
        .max_rss = full_cluster_vopr_max_rss,
    });
    const run_production_cluster_durable_join_owner_restart_vopr_tests = b.addRunArtifact(production_cluster_durable_join_owner_restart_vopr_tests);
    const production_cluster_durable_join_retry_exhaustion_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"full cluster production durable shuffle overlapping fault retry exhaustion exact replay"},
        .max_rss = full_cluster_vopr_max_rss,
    });
    const run_production_cluster_durable_join_retry_exhaustion_vopr_tests = b.addRunArtifact(production_cluster_durable_join_retry_exhaustion_vopr_tests);
    const production_cluster_durable_join_cancellation_overlap_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"full cluster production durable shuffle cancellation under overlapping faults exact replay"},
        .max_rss = full_cluster_vopr_max_rss,
    });
    const run_production_cluster_durable_join_cancellation_overlap_vopr_tests = b.addRunArtifact(production_cluster_durable_join_cancellation_overlap_vopr_tests);
    const production_cluster_durable_join_cancellation_owner_restart_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"full cluster production durable shuffle cancellation with owner reconstruction exact replay"},
        .max_rss = full_cluster_vopr_max_rss,
    });
    const run_production_cluster_durable_join_cancellation_owner_restart_vopr_tests = b.addRunArtifact(production_cluster_durable_join_cancellation_owner_restart_vopr_tests);
    const production_cluster_graph_split_overlapping_faults_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"full cluster production data plane graph active split overlapping link resource faults exact replay"},
        .max_rss = full_cluster_vopr_max_rss,
    });
    const run_production_cluster_graph_split_overlapping_faults_vopr_tests = b.addRunArtifact(production_cluster_graph_split_overlapping_faults_vopr_tests);
    const production_cluster_graph_split_socket_pressure_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"full cluster production data plane graph active split socket pressure exact replay"},
        .max_rss = full_cluster_vopr_max_rss,
    });
    const run_production_cluster_graph_split_socket_pressure_vopr_tests = b.addRunArtifact(production_cluster_graph_split_socket_pressure_vopr_tests);
    // Stackful VoprIo fibers do not use Zig's persistent std.zig.Server test
    // protocol: after a successful test body the server runner can report a
    // spurious subprocess failure, while the same binary and seed pass in
    // normal test mode. Run these production-sized gates as ordinary
    // exit-code-checked subprocesses. `.inherit` also takes the build graph's
    // global stdio lock, preventing two large fresh-world replays from sharing
    // the host at once.
    inline for (.{
        run_production_cluster_baseline_vopr_tests,
        run_production_cluster_bounded_vopr_tests,
        run_production_cluster_deep_vopr_tests,
        run_production_cluster_graph_vopr_tests,
        run_production_cluster_graph_split_vopr_tests,
        run_production_cluster_graph_split_transport_vopr_tests,
        run_production_cluster_graph_split_owner_restart_vopr_tests,
        run_production_cluster_graph_split_partial_write_vopr_tests,
        run_production_cluster_graph_split_resource_pressure_vopr_tests,
        run_production_cluster_join_split_vopr_tests,
        run_production_cluster_durable_join_takeover_vopr_tests,
        run_production_cluster_durable_join_cancellation_vopr_tests,
        run_production_cluster_durable_join_worker_retry_vopr_tests,
        run_production_cluster_durable_join_owner_restart_vopr_tests,
        run_production_cluster_durable_join_retry_exhaustion_vopr_tests,
        run_production_cluster_durable_join_cancellation_overlap_vopr_tests,
        run_production_cluster_durable_join_cancellation_owner_restart_vopr_tests,
        run_production_cluster_graph_split_overlapping_faults_vopr_tests,
        run_production_cluster_graph_split_socket_pressure_vopr_tests,
        run_production_cluster_service_rate_vopr_tests,
        run_production_cluster_query_cache_service_rate_vopr_tests,
        run_production_cluster_serverless_fencing_vopr_tests,
        run_production_cluster_authenticated_tenant_vopr_tests,
        run_production_cluster_replication_backfill_vopr_tests,
        run_production_cluster_replication_schema_change_vopr_tests,
        run_production_cluster_replication_owner_restart_vopr_tests,
        run_production_cluster_replication_source_crash_vopr_tests,
        run_production_cluster_replication_cancellation_vopr_tests,
        run_production_cluster_replication_stale_owner_vopr_tests,
        run_production_cluster_replication_topology_change_vopr_tests,
        run_production_cluster_graph_hydration_vopr_tests,
        run_production_cluster_graph_cancellation_vopr_tests,
        run_production_cluster_graph_cancellation_transport_vopr_tests,
        run_production_cluster_graph_inflight_authorization_vopr_tests,
        run_production_cluster_graph_stale_snapshot_vopr_tests,
        run_production_cluster_global_query_vopr_tests,
        run_production_cluster_global_query_cancellation_vopr_tests,
        run_production_cluster_global_query_authorization_vopr_tests,
        run_production_cluster_global_query_transport_vopr_tests,
        run_production_cluster_global_query_owner_restart_vopr_tests,
    }) |run_production_cluster_test| {
        // addRunArtifact appends cache-dir, seed, and --listen arguments after
        // the artifact; simple mode needs only the artifact itself.
        run_production_cluster_test.argv.shrinkRetainingCapacity(1);
        run_production_cluster_test.stdio = .inherit;
    }
    const production_cluster_vopr_smoke_test_step = b.step(
        "production-cluster-vopr-smoke-test",
        "Exact-replay the production DataServer deployment baseline and bounded lifecycle",
    );
    production_cluster_vopr_smoke_test_step.dependOn(&run_production_cluster_baseline_vopr_tests.step);
    production_cluster_vopr_smoke_test_step.dependOn(&run_production_cluster_bounded_vopr_tests.step);
    const production_cluster_vopr_deep_test_step = b.step(
        "production-cluster-vopr-deep-test",
        "Exact-replay the complete metadata-driven production DataServer split history",
    );
    production_cluster_vopr_deep_test_step.dependOn(&run_production_cluster_deep_vopr_tests.step);
    const production_cluster_graph_vopr_test_step = b.step(
        "production-cluster-graph-vopr-test",
        "Exact-replay a depth-two public graph across production DataServer owners",
    );
    production_cluster_graph_vopr_test_step.dependOn(&run_production_cluster_graph_vopr_tests.step);
    const production_cluster_graph_split_vopr_test_step = b.step(
        "production-cluster-graph-split-vopr-test",
        "Exact-replay public graph queries before, during, and after a production DataServer active split",
    );
    production_cluster_graph_split_vopr_test_step.dependOn(&run_production_cluster_graph_split_vopr_tests.step);
    const production_cluster_graph_split_transport_vopr_test_step = b.step(
        "production-cluster-graph-split-transport-vopr-test",
        "Exact-replay a fail-closed public graph transport cut during a production DataServer active split",
    );
    production_cluster_graph_split_transport_vopr_test_step.dependOn(&run_production_cluster_graph_split_transport_vopr_tests.step);
    const production_cluster_graph_split_owner_restart_vopr_test_step = b.step(
        "production-cluster-graph-split-owner-restart-vopr-test",
        "Exact-replay a fail-closed remote production DataServer restart during a public graph active split",
    );
    production_cluster_graph_split_owner_restart_vopr_test_step.dependOn(&run_production_cluster_graph_split_owner_restart_vopr_tests.step);
    const production_cluster_graph_split_partial_write_vopr_test_step = b.step(
        "production-cluster-graph-split-partial-write-vopr-test",
        "Exact-replay a scoped short graph HTTP write during a production DataServer active split",
    );
    production_cluster_graph_split_partial_write_vopr_test_step.dependOn(&run_production_cluster_graph_split_partial_write_vopr_tests.step);
    const production_cluster_graph_split_resource_pressure_vopr_test_step = b.step(
        "production-cluster-graph-split-resource-pressure-vopr-test",
        "Exact-replay production DataServer memory denial and recovery during a public graph active split",
    );
    production_cluster_graph_split_resource_pressure_vopr_test_step.dependOn(&run_production_cluster_graph_split_resource_pressure_vopr_tests.step);
    const production_cluster_join_split_vopr_test_step = b.step(
        "production-cluster-join-split-vopr-test",
        "Exact-replay a public distributed join before, during, and after a production DataServer active split",
    );
    production_cluster_join_split_vopr_test_step.dependOn(&run_production_cluster_join_split_vopr_tests.step);
    const production_cluster_durable_join_takeover_vopr_test_step = b.step(
        "production-cluster-durable-join-takeover-vopr-test",
        "Exact-replay durable shuffle finalizer takeover after an unacknowledged persisted result",
    );
    production_cluster_durable_join_takeover_vopr_test_step.dependOn(&run_production_cluster_durable_join_takeover_vopr_tests.step);
    const production_cluster_durable_join_cancellation_vopr_test_step = b.step(
        "production-cluster-durable-join-cancellation-vopr-test",
        "Exact-replay public cancellation propagating into an outstanding durable-shuffle partition worker",
    );
    production_cluster_durable_join_cancellation_vopr_test_step.dependOn(&run_production_cluster_durable_join_cancellation_vopr_tests.step);
    const production_cluster_durable_join_worker_retry_vopr_test_step = b.step(
        "production-cluster-durable-join-worker-retry-vopr-test",
        "Exact-replay durable-shuffle partition failover across production worker groups",
    );
    production_cluster_durable_join_worker_retry_vopr_test_step.dependOn(&run_production_cluster_durable_join_worker_retry_vopr_tests.step);
    const production_cluster_durable_join_owner_restart_vopr_test_step = b.step(
        "production-cluster-durable-join-owner-restart-vopr-test",
        "Exact-replay durable partition-owner process destruction, reconstruction, and failover",
    );
    production_cluster_durable_join_owner_restart_vopr_test_step.dependOn(&run_production_cluster_durable_join_owner_restart_vopr_tests.step);
    const production_cluster_durable_join_retry_exhaustion_vopr_test_step = b.step(
        "production-cluster-durable-join-retry-exhaustion-vopr-test",
        "Exact-replay durable join retry exhaustion under overlapping resource and network faults",
    );
    production_cluster_durable_join_retry_exhaustion_vopr_test_step.dependOn(&run_production_cluster_durable_join_retry_exhaustion_vopr_tests.step);
    const production_cluster_durable_join_cancellation_overlap_vopr_test_step = b.step(
        "production-cluster-durable-join-cancellation-overlap-vopr-test",
        "Exact-replay durable join cancellation under overlapping resource and network faults",
    );
    production_cluster_durable_join_cancellation_overlap_vopr_test_step.dependOn(&run_production_cluster_durable_join_cancellation_overlap_vopr_tests.step);
    const production_cluster_durable_join_cancellation_owner_restart_vopr_test_step = b.step(
        "production-cluster-durable-join-cancellation-owner-restart-vopr-test",
        "Exact-replay durable join cancellation followed by production owner destruction and reconstruction",
    );
    production_cluster_durable_join_cancellation_owner_restart_vopr_test_step.dependOn(&run_production_cluster_durable_join_cancellation_owner_restart_vopr_tests.step);
    const production_cluster_graph_split_overlapping_faults_vopr_test_step = b.step(
        "production-cluster-graph-split-overlapping-faults-vopr-test",
        "Exact-replay overlapping graph transport and all-owner memory faults during an active split",
    );
    production_cluster_graph_split_overlapping_faults_vopr_test_step.dependOn(&run_production_cluster_graph_split_overlapping_faults_vopr_tests.step);
    const production_cluster_graph_split_socket_pressure_vopr_test_step = b.step(
        "production-cluster-graph-split-socket-pressure-vopr-test",
        "Exact-replay a selected production listener socket denial and recovery during an active split",
    );
    production_cluster_graph_split_socket_pressure_vopr_test_step.dependOn(&run_production_cluster_graph_split_socket_pressure_vopr_tests.step);
    const production_cluster_vopr_test_step = b.step(
        "production-cluster-vopr-test",
        "Run every focused production DataServer cluster history through v53",
    );
    production_cluster_vopr_test_step.dependOn(production_cluster_vopr_smoke_test_step);
    production_cluster_vopr_test_step.dependOn(production_cluster_vopr_deep_test_step);
    production_cluster_vopr_test_step.dependOn(production_cluster_graph_vopr_test_step);
    production_cluster_vopr_test_step.dependOn(production_cluster_graph_split_vopr_test_step);
    production_cluster_vopr_test_step.dependOn(production_cluster_graph_split_transport_vopr_test_step);
    production_cluster_vopr_test_step.dependOn(production_cluster_graph_split_owner_restart_vopr_test_step);
    production_cluster_vopr_test_step.dependOn(production_cluster_graph_split_partial_write_vopr_test_step);
    production_cluster_vopr_test_step.dependOn(production_cluster_graph_split_resource_pressure_vopr_test_step);
    production_cluster_vopr_test_step.dependOn(production_cluster_disk_capacity_vopr_test_step);
    production_cluster_vopr_test_step.dependOn(production_cluster_managed_index_vopr_test_step);
    production_cluster_vopr_test_step.dependOn(production_cluster_join_split_vopr_test_step);
    production_cluster_vopr_test_step.dependOn(production_cluster_durable_join_takeover_vopr_test_step);
    production_cluster_vopr_test_step.dependOn(production_cluster_durable_join_cancellation_vopr_test_step);
    production_cluster_vopr_test_step.dependOn(production_cluster_durable_join_worker_retry_vopr_test_step);
    production_cluster_vopr_test_step.dependOn(production_cluster_durable_join_owner_restart_vopr_test_step);
    production_cluster_vopr_test_step.dependOn(production_cluster_durable_join_retry_exhaustion_vopr_test_step);
    production_cluster_vopr_test_step.dependOn(production_cluster_durable_join_cancellation_overlap_vopr_test_step);
    production_cluster_vopr_test_step.dependOn(production_cluster_durable_join_cancellation_owner_restart_vopr_test_step);
    production_cluster_vopr_test_step.dependOn(production_cluster_graph_split_overlapping_faults_vopr_test_step);
    production_cluster_vopr_test_step.dependOn(production_cluster_graph_split_socket_pressure_vopr_test_step);
    production_cluster_vopr_test_step.dependOn(production_cluster_service_rate_vopr_test_step);
    production_cluster_vopr_test_step.dependOn(production_cluster_query_cache_service_rate_vopr_test_step);
    production_cluster_vopr_test_step.dependOn(production_cluster_serverless_fencing_vopr_test_step);
    production_cluster_vopr_test_step.dependOn(production_cluster_authenticated_tenant_vopr_test_step);
    production_cluster_vopr_test_step.dependOn(production_cluster_replication_backfill_vopr_test_step);
    production_cluster_vopr_test_step.dependOn(production_cluster_replication_schema_change_vopr_test_step);
    production_cluster_vopr_test_step.dependOn(production_cluster_replication_owner_restart_vopr_test_step);
    production_cluster_vopr_test_step.dependOn(production_cluster_replication_source_crash_vopr_test_step);
    production_cluster_vopr_test_step.dependOn(production_cluster_replication_cancellation_vopr_test_step);
    production_cluster_vopr_test_step.dependOn(production_cluster_replication_stale_owner_vopr_test_step);
    production_cluster_vopr_test_step.dependOn(production_cluster_replication_topology_change_vopr_test_step);
    production_cluster_vopr_test_step.dependOn(production_cluster_graph_hydration_vopr_test_step);
    production_cluster_vopr_test_step.dependOn(production_cluster_graph_cancellation_vopr_test_step);
    production_cluster_vopr_test_step.dependOn(production_cluster_graph_cancellation_transport_vopr_test_step);
    production_cluster_vopr_test_step.dependOn(production_cluster_graph_inflight_authorization_vopr_test_step);
    production_cluster_vopr_test_step.dependOn(production_cluster_graph_stale_snapshot_vopr_test_step);
    production_cluster_vopr_test_step.dependOn(production_cluster_global_query_vopr_test_step);
    production_cluster_vopr_test_step.dependOn(production_cluster_global_query_cancellation_vopr_test_step);
    production_cluster_vopr_test_step.dependOn(production_cluster_global_query_authorization_vopr_test_step);
    production_cluster_vopr_test_step.dependOn(production_cluster_global_query_transport_vopr_test_step);
    production_cluster_vopr_test_step.dependOn(production_cluster_global_query_owner_restart_vopr_test_step);

    const generation_reranking_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"generation and reranking chain VOPR exact replays"},
    });
    const run_generation_reranking_vopr_tests = b.addRunArtifact(generation_reranking_vopr_tests);
    const generation_reranking_vopr_test_step = b.step(
        "generation-reranking-vopr-test",
        "Run local/remote generation and reranking fallback, replacement, validation, timeout, and cancellation histories on VoprIo",
    );
    generation_reranking_vopr_test_step.dependOn(&run_generation_reranking_vopr_tests.step);

    const distributed_query_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"distributed query VOPR exact replays"},
    });
    const run_distributed_query_vopr_tests = b.addRunArtifact(distributed_query_vopr_tests);
    const distributed_query_vopr_test_step = b.step(
        "distributed-query-vopr-test",
        "Run distributed graph planning, fanout, hydration, topology, snapshot, and cancellation histories on VoprIo",
    );
    distributed_query_vopr_test_step.dependOn(&run_distributed_query_vopr_tests.step);

    const parquet_cache_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"persistent Parquet cache VOPR exact replays"},
    });
    const run_parquet_cache_vopr_tests = b.addRunArtifact(parquet_cache_vopr_tests);
    const parquet_cache_vopr_test_step = b.step("parquet-cache-vopr-test", "Run persistent Parquet cache faults and crash recovery on VoprIo");
    parquet_cache_vopr_test_step.dependOn(&run_parquet_cache_vopr_tests.step);

    const provisioning_startup_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"provisioning startup VOPR exact replays"},
    });
    const run_provisioning_startup_vopr_tests = b.addRunArtifact(provisioning_startup_vopr_tests);
    const provisioning_startup_vopr_test_step = b.step("provisioning-startup-vopr-test", "Run startup admission, provisioning, retry, and crash histories on VoprIo");
    provisioning_startup_vopr_test_step.dependOn(&run_provisioning_startup_vopr_tests.step);

    const generation_lifecycle_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"generation lifecycle VOPR exact replays"},
    });
    const run_generation_lifecycle_vopr_tests = b.addRunArtifact(generation_lifecycle_vopr_tests);
    const generation_lifecycle_vopr_test_step = b.step(
        "generation-lifecycle-vopr-test",
        "Run generation publication, rollback, recovery, cleanup, and lock histories on VoprIo",
    );
    generation_lifecycle_vopr_test_step.dependOn(&run_generation_lifecycle_vopr_tests.step);

    const backfill_marker_discovery_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"backfill marker discovery VOPR exact replays"},
    });
    const run_backfill_marker_discovery_vopr_tests = b.addRunArtifact(backfill_marker_discovery_vopr_tests);
    const backfill_marker_discovery_vopr_test_step = b.step(
        "backfill-marker-discovery-vopr-test",
        "Run metadata marker discovery, ownership, corruption, recheck, and throttle histories on VoprIo",
    );
    backfill_marker_discovery_vopr_test_step.dependOn(&run_backfill_marker_discovery_vopr_tests.step);

    const config_extension_lifecycle_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"config extension lifecycle VOPR exact replays"},
    });
    const run_config_extension_lifecycle_vopr_tests = b.addRunArtifact(config_extension_lifecycle_vopr_tests);
    const config_extension_lifecycle_vopr_test_step = b.step(
        "config-extension-lifecycle-vopr-test",
        "Run cold config, secret rotation, refresh rollback, and extension activation histories on VoprIo",
    );
    config_extension_lifecycle_vopr_test_step.dependOn(&run_config_extension_lifecycle_vopr_tests.step);

    const embedded_lite_lifecycle_vopr_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/vopr/embedded_lite_lifecycle.zig"),
        .target = target,
        .optimize = optimize,
    });
    embedded_lite_lifecycle_vopr_mod.link_libc = true;
    embedded_lite_lifecycle_vopr_mod.addImport("vopr", vopr_mod);
    embedded_lite_lifecycle_vopr_mod.addImport("embedded_db_surface", embedded_db_mod);
    embedded_lite_lifecycle_vopr_mod.addImport("embedded_support", embedded_support_mod);
    const embedded_lite_lifecycle_vopr_tests = b.addTest(.{
        .root_module = embedded_lite_lifecycle_vopr_mod,
        .filters = &.{
            "embedded and Lite lifecycle exact replay",
            "Lite native and VoprIo produce the same logical checkpoint",
        },
    });
    const run_embedded_lite_lifecycle_vopr_tests = b.addRunArtifact(embedded_lite_lifecycle_vopr_tests);

    const capi_lite_lifecycle_vopr_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/vopr/capi_lite_lifecycle.zig"),
        .target = target,
        .optimize = optimize,
    });
    capi_lite_lifecycle_vopr_mod.link_libc = true;
    capi_lite_lifecycle_vopr_mod.addImport("vopr", vopr_mod);
    capi_lite_lifecycle_vopr_mod.addImport("antfly_capi", capi_mod);
    capi_lite_lifecycle_vopr_mod.addImport("antfly_capi_storage_root", capi_root_mod);
    const capi_lite_lifecycle_vopr_tests = b.addTest(.{
        .root_module = capi_lite_lifecycle_vopr_mod,
        .filters = &.{"C API Lite lifecycle exact replay"},
    });
    const run_capi_lite_lifecycle_vopr_tests = b.addRunArtifact(capi_lite_lifecycle_vopr_tests);
    const embedded_lite_lifecycle_vopr_test_step = b.step(
        "embedded-lite-lifecycle-vopr-test",
        "Run embedded, C ABI, and native Lite lifecycle, restore, callback, crash, and differential histories",
    );
    embedded_lite_lifecycle_vopr_test_step.dependOn(&run_embedded_lite_lifecycle_vopr_tests.step);
    embedded_lite_lifecycle_vopr_test_step.dependOn(&run_capi_lite_lifecycle_vopr_tests.step);

    const vopr_determinism_audit_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{
            "replayable Antfly VOPR sources pass the fail-closed determinism audit",
            "determinism manifest covers every exported Antfly VOPR source",
        },
    });
    const run_vopr_determinism_audit_tests = b.addRunArtifact(vopr_determinism_audit_tests);
    const vopr_determinism_audit_step = b.step(
        "vopr-determinism-audit",
        "Reject uncontrolled entropy, clocks, host I/O, iteration, native libraries, and unstable identities in replayable VOPR adapters",
    );
    vopr_determinism_audit_step.dependOn(&run_vopr_determinism_audit_tests.step);

    const external_lake_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"external lake VOPR exact replays"},
    });
    const run_external_lake_vopr_tests = b.addRunArtifact(external_lake_vopr_tests);
    const external_lake_vopr_test_step = b.step("external-lake-vopr-test", "Run composed Iceberg discovery, Parquet query, cache, version, deletion, retry, eviction, and restart histories");
    external_lake_vopr_test_step.dependOn(&run_external_lake_vopr_tests.step);

    const media_runtime_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"media provider VOPR exact replays"},
    });
    const run_media_runtime_vopr_tests = b.addRunArtifact(media_runtime_vopr_tests);
    const media_runtime_vopr_test_step = b.step("media-runtime-vopr-test", "Run production media HTTP, retry, timeout, cancellation, replacement, and cleanup histories on VoprIo");
    media_runtime_vopr_test_step.dependOn(&run_media_runtime_vopr_tests.step);

    const upgrade_compatibility_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"upgrade compatibility VOPR exact replays"},
    });
    const run_upgrade_compatibility_vopr_tests = b.addRunArtifact(upgrade_compatibility_vopr_tests);
    const upgrade_compatibility_vopr_test_step = b.step("upgrade-compatibility-vopr-test", "Run explicit Antfly product storage and serverless artifact compatibility histories");
    upgrade_compatibility_vopr_test_step.dependOn(&run_upgrade_compatibility_vopr_tests.step);

    const derived_workflow_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"derived workflow VOPR records and exact replays"},
    });
    const run_derived_workflow_vopr_tests = b.addRunArtifact(derived_workflow_vopr_tests);
    const derived_workflow_vopr_test_step = b.step("derived-workflow-vopr-test", "Run enrichment, indexing, repair, and compaction VOPR campaigns");
    derived_workflow_vopr_test_step.dependOn(&run_derived_workflow_vopr_tests.step);
    derived_workflow_vopr_test_step.dependOn(&run_lib_db_enrichment_tests.step);
    derived_workflow_vopr_test_step.dependOn(&run_dense_index_lifecycle_regression_tests.step);
    derived_workflow_vopr_test_step.dependOn(&run_dense_index_repair_job_tests.step);
    derived_workflow_vopr_test_step.dependOn(&run_dense_index_repair_runtime_tests.step);

    const backup_restore_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"backup restore lifecycle VOPR records and exact replays"},
    });
    const run_backup_restore_vopr_tests = b.addRunArtifact(backup_restore_vopr_tests);
    const backup_restore_vopr_test_step = b.step("backup-restore-vopr-test", "Run backup publication, retention, restore, activation, and GC VOPR campaigns");
    backup_restore_vopr_test_step.dependOn(&run_backup_restore_vopr_tests.step);
    backup_restore_vopr_test_step.dependOn(&run_api_restore_jobs_tests.step);
    backup_restore_vopr_test_step.dependOn(&run_portable_backup_tests.step);
    backup_restore_vopr_test_step.dependOn(&run_raft_restore_tests.step);
    backup_restore_vopr_test_step.dependOn(&run_lib_api_standalone_backup_restore_tests.step);
    backup_restore_vopr_test_step.dependOn(&run_lib_ha_vopr_tests.step);

    const clock_fault_vopr_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{"clock lease TTL fault VOPR records and exact replays"},
    });
    const run_clock_fault_vopr_tests = b.addRunArtifact(clock_fault_vopr_tests);
    const clock_fault_vopr_test_step = b.step("clock-fault-vopr-test", "Run wall-clock, monotonic-clock, lease, retention, and TTL fault VOPR campaigns");
    clock_fault_vopr_test_step.dependOn(&run_clock_fault_vopr_tests.step);
    clock_fault_vopr_test_step.dependOn(&run_lib_db_txn_tests.step);
    clock_fault_vopr_test_step.dependOn(&run_lib_ha_vopr_tests.step);

    const domain_vopr_test_step = b.step("domain-vopr-test", "Run all cross-domain Antfly VOPR protocol campaigns");
    domain_vopr_test_step.dependOn(&run_distributed_transaction_vopr_tests.step);
    domain_vopr_test_step.dependOn(&run_data_plane_vopr_tests.step);
    domain_vopr_test_step.dependOn(&run_derived_workflow_vopr_tests.step);
    domain_vopr_test_step.dependOn(&run_backup_restore_vopr_tests.step);
    domain_vopr_test_step.dependOn(&run_clock_fault_vopr_tests.step);

    const vopr_runtime_adapter_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{
            "VOPR durable job",
            "backend runtime durable owner lifecycle",
            "backend runtime borrows backend-agnostic std.Io lanes",
            "ttl runtime executes production pass on borrowed VoprIo",
            "transaction recovery executes production pass on borrowed VoprIo",
            "background maintenance services lifecycle runs on borrowed VoprIo",
        },
    });
    const run_vopr_runtime_adapter_tests = b.addRunArtifact(vopr_runtime_adapter_tests);
    const vopr_runtime_adapter_test_step = b.step("vopr-runtime-test", "Run Antfly background-service adapters on the deterministic VOPR runtime");
    vopr_runtime_adapter_test_step.dependOn(&run_vopr_runtime_adapter_tests.step);
    derived_workflow_vopr_test_step.dependOn(&run_vopr_runtime_adapter_tests.step);

    b.step("vopr-build", "Install the VOPR campaign and replay executable").dependOn(&b.addInstallArtifact(vopr_cli, .{}).step);

    const run_vopr_cli = b.addRunArtifact(vopr_cli);
    run_vopr_cli.addArg("run");
    if (b.args) |args| run_vopr_cli.addArgs(args);
    const vopr_run_step = b.step("vopr-run", "Run one deterministic generated VOPR history");
    vopr_run_step.dependOn(&run_vopr_cli.step);

    const replay_vopr_cli = b.addRunArtifact(vopr_cli);
    replay_vopr_cli.addArg("replay");
    if (b.args) |args| replay_vopr_cli.addArgs(args);
    const vopr_replay_step = b.step("vopr-replay", "Replay one exact VOPR artifact");
    vopr_replay_step.dependOn(&replay_vopr_cli.step);

    const campaign_vopr_cli = b.addRunArtifact(vopr_cli);
    campaign_vopr_cli.addArg("campaign");
    if (b.args) |args| campaign_vopr_cli.addArgs(args);
    const vopr_campaign_step = b.step("vopr-campaign", "Run a bounded parallel VOPR campaign");
    vopr_campaign_step.dependOn(&campaign_vopr_cli.step);

    const reduce_vopr_cli = b.addRunArtifact(vopr_cli);
    reduce_vopr_cli.addArg("reduce");
    if (b.args) |args| reduce_vopr_cli.addArgs(args);
    const vopr_reduce_step = b.step("vopr-reduce", "Reduce a VOPR failure while preserving its fingerprint");
    vopr_reduce_step.dependOn(&reduce_vopr_cli.step);

    const promote_vopr_cli = b.addRunArtifact(vopr_cli);
    promote_vopr_cli.addArg("promote");
    if (b.args) |args| promote_vopr_cli.addArgs(args);
    const vopr_promote_step = b.step("vopr-promote", "Promote a reviewed reduced VOPR failure fixture");
    vopr_promote_step.dependOn(&promote_vopr_cli.step);

    const tla_vopr_cli = b.addRunArtifact(vopr_cli);
    tla_vopr_cli.addArg("tla");
    if (b.args) |args| tla_vopr_cli.addArgs(args);
    const vopr_tla_step = b.step("vopr-tla", "Exact-replay a VOPR artifact and export TLA+ Raft NDJSON");
    vopr_tla_step.dependOn(&tla_vopr_cli.step);

    const explain_vopr_cli = b.addRunArtifact(vopr_cli);
    explain_vopr_cli.addArg("explain");
    if (b.args) |args| explain_vopr_cli.addArgs(args);
    const vopr_explain_step = b.step("vopr-explain", "Exact-replay a failing VOPR artifact and render its semantic causal slice");
    vopr_explain_step.dependOn(&explain_vopr_cli.step);

    const debug_vopr_cli = b.addRunArtifact(vopr_cli);
    debug_vopr_cli.addArg("debug");
    if (b.args) |args| debug_vopr_cli.addArgs(args);
    const vopr_debug_step = b.step("vopr-debug", "Inspect a replay-validated VOPR artifact at a choice prefix");
    vopr_debug_step.dependOn(&debug_vopr_cli.step);

    const results_vopr_cli = b.addRunArtifact(vopr_cli);
    results_vopr_cli.addArg("results");
    if (b.args) |args| results_vopr_cli.addArgs(args);
    const vopr_results_step = b.step("vopr-results", "Render exact-replayed VOPR results as stable JSON and static HTML");
    vopr_results_step.dependOn(&results_vopr_cli.step);

    const events_vopr_cli = b.addRunArtifact(vopr_cli);
    events_vopr_cli.addArg("events");
    if (b.args) |args| events_vopr_cli.addArgs(args);
    const vopr_events_step = b.step("vopr-events", "Validate or run a saved event-set query over exact-replayed VOPR histories");
    vopr_events_step.dependOn(&events_vopr_cli.step);

    const recipe_vopr_cli = b.addRunArtifact(vopr_cli);
    recipe_vopr_cli.addArg("recipe");
    if (b.args) |args| recipe_vopr_cli.addArgs(args);
    const vopr_recipe_step = b.step("vopr-recipe", "Build a reduction, causal, counterfactual, query, and collector debug package");
    vopr_recipe_step.dependOn(&recipe_vopr_cli.step);

    const index_vopr_cli = b.addRunArtifact(vopr_cli);
    index_vopr_cli.addArg("index");
    if (b.args) |args| index_vopr_cli.addArgs(args);
    const vopr_index_step = b.step("vopr-index", "Update and query the deterministic local VOPR run/results index");
    vopr_index_step.dependOn(&index_vopr_cli.step);

    const corpus_merge_vopr_cli = b.addRunArtifact(vopr_cli);
    corpus_merge_vopr_cli.addArg("corpus-merge");
    if (b.args) |args| corpus_merge_vopr_cli.addArgs(args);
    const vopr_corpus_merge_step = b.step("vopr-corpus-merge", "Exact-replay and deterministically merge local, CI, and nightly VOPR corpora");
    vopr_corpus_merge_step.dependOn(&corpus_merge_vopr_cli.step);

    const vopr_test_step = b.step("vopr-test", "Run the fast deterministic Antfly VOPR suites");
    vopr_test_step.dependOn(&run_raft_snapshot_maintenance_vopr_tests.step);
    vopr_test_step.dependOn(&run_vopr_contract_tests.step);
    vopr_test_step.dependOn(&run_transaction_vopr_tests.step);
    vopr_test_step.dependOn(&run_distributed_transaction_vopr_tests.step);
    vopr_test_step.dependOn(&run_data_plane_vopr_tests.step);
    vopr_test_step.dependOn(&run_request_lifecycle_vopr_tests.step);
    vopr_test_step.dependOn(&run_replication_backfill_vopr_tests.step);
    vopr_test_step.dependOn(&run_supervision_vopr_tests.step);
    vopr_test_step.dependOn(&run_auth_lifecycle_vopr_tests.step);
    vopr_test_step.dependOn(&run_data_server_vopr_tests.step);
    vopr_test_step.dependOn(&run_serverless_object_store_vopr_tests.step);
    vopr_test_step.dependOn(&run_serverless_workflow_vopr_tests.step);
    vopr_test_step.dependOn(&run_db_index_race_vopr_tests.step);
    vopr_test_step.dependOn(&run_admission_vopr_tests.step);
    vopr_test_step.dependOn(&run_provider_boundary_vopr_tests.step);
    vopr_test_step.dependOn(&run_composed_query_vopr_tests.step);
    vopr_test_step.dependOn(&run_query_embedding_cache_vopr_tests.step);
    vopr_test_step.dependOn(&run_full_cluster_vopr_tests.step);
    vopr_test_step.dependOn(&run_ha_scaling_vopr_tests.step);
    vopr_test_step.dependOn(production_cluster_vopr_smoke_test_step);
    vopr_test_step.dependOn(&run_generation_reranking_vopr_tests.step);
    vopr_test_step.dependOn(&run_distributed_query_vopr_tests.step);
    vopr_test_step.dependOn(&run_parquet_cache_vopr_tests.step);
    vopr_test_step.dependOn(&run_provisioning_startup_vopr_tests.step);
    vopr_test_step.dependOn(&run_generation_lifecycle_vopr_tests.step);
    vopr_test_step.dependOn(&run_backfill_marker_discovery_vopr_tests.step);
    vopr_test_step.dependOn(&run_config_extension_lifecycle_vopr_tests.step);
    vopr_test_step.dependOn(&run_embedded_lite_lifecycle_vopr_tests.step);
    vopr_test_step.dependOn(&run_capi_lite_lifecycle_vopr_tests.step);
    vopr_test_step.dependOn(&run_vopr_determinism_audit_tests.step);
    vopr_test_step.dependOn(&run_external_lake_vopr_tests.step);
    vopr_test_step.dependOn(&run_media_runtime_vopr_tests.step);
    vopr_test_step.dependOn(&run_upgrade_compatibility_vopr_tests.step);
    vopr_test_step.dependOn(&run_derived_workflow_vopr_tests.step);
    vopr_test_step.dependOn(&run_backup_restore_vopr_tests.step);
    vopr_test_step.dependOn(&run_clock_fault_vopr_tests.step);
    vopr_test_step.dependOn(&run_vopr_runtime_adapter_tests.step);
    vopr_test_step.dependOn(&run_lib_metadata_vopr_virtual_smoke_tests.step);
    vopr_test_step.dependOn(&run_lib_metadata_vopr_tests.step);
    vopr_test_step.dependOn(&run_lib_metadata_vopr_data_tests.step);
    vopr_test_step.dependOn(&run_lib_raft_vopr_tests.step);
    vopr_test_step.dependOn(&run_lib_ha_vopr_tests.step);
    vopr_test_step.dependOn(&run_lib_raft_harness_tests.step);
    vopr_test_step.dependOn(&run_vopr_cli_meta_tests.step);
    vopr_test_step.dependOn(&run_vopr_cli_registry_tests.step);

    const integration_test_step = b.step("antfly-integration-test", "Run focused real HTTP and public API integration suites");
    integration_test_step.dependOn(&run_lib_metadata_vopr_public_integration_tests.step);
    integration_test_step.dependOn(&run_lib_metadata_vopr_forwarding_integration_tests.step);
    // Both aggregates share this run node, so the default test DAG executes
    // the stateful parity suite once. The focused alias remains independent.
    integration_test_step.dependOn(&run_public_api_parity_aggregate_tests.step);
    // Keep document-identity regressions in the owning integration suite.
    // The mixed lifecycle artifact remains intact so removing its public
    // shortcut does not discard metadata, cache, or distributed-query cases.
    integration_test_step.dependOn(&run_lib_docid_lifecycle_tests.step);
    integration_test_step.dependOn(&run_lib_serverless_docid_tests.step);
    integration_test_step.dependOn(&run_api_transactions_docid_tests.step);
    integration_test_step.dependOn(&run_api_table_reads_docid_tests.step);
    integration_test_step.dependOn(&run_api_table_writes_docid_tests.step);
    integration_test_step.dependOn(&run_api_public_table_http_docid_tests.step);

    const chaos_test_step = b.step("antfly-chaos-test", "Run bounded generated chaos campaigns with labeled progress");
    var chaos_progress_tail: ?*std.Build.Step = null;
    chaos_progress_tail = chainLabeledRun(b, distributed_transaction_vopr_tests, "distributed-transaction-vopr-test", chaos_progress_tail);
    chaos_progress_tail = chainLabeledRun(b, data_plane_vopr_tests, "data-plane-vopr-test", chaos_progress_tail);
    chaos_progress_tail = chainLabeledRun(b, derived_workflow_vopr_tests, "derived-workflow-vopr-test", chaos_progress_tail);
    chaos_progress_tail = chainLabeledRun(b, backup_restore_vopr_tests, "backup-restore-vopr-test", chaos_progress_tail);
    chaos_progress_tail = chainLabeledRun(b, clock_fault_vopr_tests, "clock-fault-vopr-test", chaos_progress_tail);
    chaos_progress_tail = chainLabeledRun(b, lib_metadata_vopr_chaos_tests, "lib-metadata-vopr-chaos-test", chaos_progress_tail);
    chaos_progress_tail = chainLabeledRun(b, lib_raft_vopr_tests, "raft-vopr-test", chaos_progress_tail);
    chaos_progress_tail = chainLabeledRun(b, lib_lsm_backend_chaos_tests, "lib-lsm-backend-chaos-test", chaos_progress_tail);
    chaos_progress_tail = chainLabeledRun(b, lib_ha_chaos_tests, "ha-chaos-test", chaos_progress_tail);
    chaos_progress_tail = chainLabeledRun(b, lib_ha_vopr_tests, "ha-vopr-test", chaos_progress_tail);
    chaos_test_step.dependOn(chaos_progress_tail.?);

    const vopr_soak_test_step = b.step("vopr-soak-test", "Run HA/scaling/Raft/data VOPR search campaigns and metadata/Raft native differentials");
    const vopr_soak_histories = b.option(u64, "vopr-soak-histories", "Histories per VOPR soak campaign") orelse 100;
    const vopr_soak_production_histories = b.option(u64, "vopr-soak-production-histories", "Histories in the production HA/scaling soak campaign") orelse 2;
    const vopr_soak_seed = b.option(u64, "vopr-soak-seed", "Base seed for VOPR soak campaigns") orelse 0xa17f_5500;
    const vopr_soak_artifacts = b.option([]const u8, "vopr-soak-artifacts", "Persistent directory for VOPR soak reports and replay corpus") orelse "zig-out/vopr-soak";
    var vopr_soak_progress_tail: ?*std.Build.Step = null;
    // One worker makes corpus-guided selection reproducible as a campaign,
    // in addition to each history's independent exact-replay guarantee.
    for ([_][]const u8{ "ha", "raft", "distributed-data", "ha-scaling" }) |scenario| {
        const histories = if (std.mem.eql(u8, scenario, "ha-scaling")) vopr_soak_production_histories else vopr_soak_histories;
        const campaign = b.addRunArtifact(vopr_cli);
        campaign.addArgs(&.{
            "campaign",                      "--scenario",               scenario,
            "--histories",                   b.fmt("{d}", .{histories}), "--seed",
            b.fmt("{d}", .{vopr_soak_seed}), "--workers",                "1",
            "--fail-on-findings",            "--artifact-dir",           b.pathJoin(&.{ vopr_soak_artifacts, scenario }),
            "--defer-diagnostics",
        });
        if (vopr_soak_progress_tail) |previous| campaign.step.dependOn(previous);
        vopr_soak_progress_tail = &campaign.step;
    }
    // Retain the existing broad differential coverage during consolidation.
    // Native HTTP/storage tests do not acquire exact replay by being in this tier.
    vopr_soak_progress_tail = chainLabeledFilteredTests(b, antfly_test_mod, "lib-metadata-vopr-transition-chaos-test", lib_metadata_vopr_transition_chaos_filters, vopr_soak_progress_tail);
    vopr_soak_progress_tail = chainLabeledFilteredTests(b, antfly_test_mod, "lib-metadata-vopr-public-chaos-test", lib_metadata_vopr_public_chaos_filters, vopr_soak_progress_tail);
    vopr_soak_progress_tail = chainLabeledFilteredTests(b, antfly_test_mod, "lib-metadata-vopr-placement-chaos-test", lib_metadata_vopr_placement_chaos_filters, vopr_soak_progress_tail);
    vopr_soak_progress_tail = chainLabeledRun(b, lib_raft_chaos_tests, "lib-raft-chaos-test", vopr_soak_progress_tail);
    vopr_soak_test_step.dependOn(vopr_soak_progress_tail.?);

    const template_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/template_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, template_test_mod, false, true);
    const lib_template_tests = b.addTest(.{
        .root_module = template_test_mod,
    });
    // template_remote imports Antfly runtime ABI tests, whose intentional
    // error paths use the repository runner's expected-log accounting.
    const run_lib_template_tests = addAntflyTestRunArtifact(b, lib_template_tests);
    const lib_template_test_step = b.step("antfly-template-test", "Run template rendering tests");
    lib_template_test_step.dependOn(&run_lib_template_tests.step);

    const audio_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/audio_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_imports.configure(b, audio_test_mod, false, true);
    const lib_audio_tests = b.addTest(.{
        .root_module = audio_test_mod,
    });
    const run_lib_audio_tests = b.addRunArtifact(lib_audio_tests);
    const lib_audio_test_step = b.step("antfly-audio-test", "Run audio transcribing and synthesizing runtime tests");
    lib_audio_test_step.dependOn(&run_lib_audio_tests.step);
    const standalone_runtime_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/standalone_runtime_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    var standalone_runtime_imports = test_imports;
    standalone_runtime_imports.runtime.build_options = standalone_runtime_build_options;
    standalone_runtime_imports.configure(b, standalone_runtime_test_mod, true, true);
    standalone_runtime_test_mod.addImport("antfly_openapi_specs", standalone_runtime_imports.runtime.embedded_openapi);
    const usermgr_storage_standalone_runtime_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/usermgr/storage_imports.zig"),
        .target = target,
        .optimize = optimize,
    });
    usermgr_storage_standalone_runtime_test_mod.addImport("antfly_root", standalone_runtime_test_mod);
    usermgr_storage_standalone_runtime_test_mod.addImport("antfly_platform", platform_mod);
    standalone_runtime_test_mod.addImport("usermgr_storage", usermgr_storage_standalone_runtime_test_mod);
    const system_catalog_standalone_tests = b.addTest(.{
        .root_module = standalone_runtime_test_mod,
        .filters = &.{"system catalog"},
    });
    const system_catalog_standalone_step = b.step("antfly-system-catalog-standalone-test", "Run standalone catalog checkpoint and rollback tests");
    system_catalog_standalone_step.dependOn(&b.addRunArtifact(system_catalog_standalone_tests).step);
    const lib_standalone_runtime_tests = b.addTest(.{
        .root_module = standalone_runtime_test_mod,
        .filters = &.{
            "standalone runtime module compiles",
            "standalone.runtime.test.system catalog",
            "catalog.domain.",
            "standalone runtime local generator accepts media url data uris",
            "local generate message conversion preserves tool history and admission",
            "inference worker",
            "provider failure logging",
            "provider owner logs private cause",
            "standalone runtime local dense embed preserves borrowed binary media",
            "standalone numeric result ABI",
            "standalone raster embedding control",
            "standalone encoded reader ABI round trips borrowed payloads",
            "standalone raster reader ABI preserves borrowed strided pages and identity",
            "encoded reader ABI enforces resolved model capabilities",
            "standalone runtime local generator preflights mixed resident media exactly",
            "standalone runtime local generator refuses decode allocation beyond preflight",
            "linked generator validates concrete MIME and decoded pixels",
            "standalone inference middleware reuses public API authentication",
            "standalone CORS middleware",
            "standalone runtime local replica reconcile permit blocks only active startup catch-up",
            "standalone runtime parses experimental flag",
            "standalone runtime antfarm path guards keep api routes reserved",
            "standalone startup checkpoint readiness requires applied and safe-read progress",
            "standalone activated seed bootstraps exact standby checkpoint and rejects older progress",
            "parse cli accepts config path",
            "parse cli accepts secret store path",
            "parse cli accepts ARD identity flags",
            "parse cli accepts canonical host port and models dir flags",
            "parse cli preserves registry variants and recognizes explicit preload backends",
            "parse cli accepts HA primary runtime flags",
            "parse cli accepts HA primary sync policy flags",
            "promoted HA primary retains exact predecessor startup provenance",
            "parse cli accepts HA standby runtime flags",
            "standalone HA standby replication flags require upstream and slot",
            "standalone HA string classifier distinguishes missing padded and valid values",
            "standalone HA runtime rejects ambiguous role flags",
            "standalone continuous HA mutation guard follows role lifecycle",
            "antfly config uses cli override before common config",
            "standalone memory budget conversion rejects overflow",
            "standalone public api caps keep alive request reuse",
            "standalone public api body limit matches common http listener",
            "standalone public ready endpoint fails closed before API initialization",
            "standalone public HTTP server is restart-safe and uses public API request body limit",
            "standalone rejects configured server TLS instead of serving plaintext",
            "standalone Lite transaction sessions survive file reopen",
            "durable session mutations publish only after persistence succeeds",
            "durable session limits bound count and encoded record size",
            "common config rejects removed top-level storage backend fields",
            "common config parses bounded transaction session policy",
            "parse cli accepts inference budget overrides",
            "standalone preserves effective process envelope provenance for inference",
            "standalone kernel JIT mode precedence is CLI then environment then config",
            "inference config falls back to common config",
            "standalone prompt cache detaches resource observer before owner teardown",
            "inference admission bridge charges combined native residency to resource manager",
            "standalone tokenizer bridge enforces growth and permits exact teardown",
            "standalone inference keep alive parses compound durations and zero",
            "standalone preload bridge preserves A4B residency controls",
            "standalone data directory does not change the default models directory",
            "standalone linked inference ABI validates the supported function-table prefix",
            "linked inference ABI rejects mismatched context and function-table prefixes",
            "standalone local inference lifetime distinguishes deadline from upstream cancellation",
            "standalone resolves the default secret store before full config parsing",
            "embedded provider lifetime rejects new calls and joins admitted calls",
            "standalone runtime resolves paths from common storage base dir",
            "standalone runtime resolves extension package store env before local default",
            "standalone Lite enforces one shard and one replica",
            "standalone Lite adoption preserves deterministic embedded document identity",
            "standalone validates effective Lite CLI and config settings",
            "standalone metadata rolls back an undurable catalog mutation",
            "standalone metadata advertises a linearizable owned snapshot",
            "standalone schema mutation supports atomic merge patch and version CAS",
            "standalone routing watch does not report absence after one probe",
            "standalone metadata catalog source provides compact routing",
            "standalone metadata rejects corrupt catalog without double-freeing owned paths",
            "standalone metadata finalizes schema migration from resident runtime evidence",
            "standalone unified server lifecycle propagates startup failure",
            "runtime lease watchdog publishes active self-fenced proof from exact expired lease",
            "runtime lease watchdog fetch and validation failures publish no bootstrap capability",
            "runtime lease watchdog retains a bounded Kubernetes response budget",
            "runtime lease watchdog prefers a DNS-verified Kubernetes API host and retains the injected port",
            "Lease executor rejects unscoped request shapes",
            "Lease executor accepts optional CertificateRequest with projected CA and verified hostname",
            "Lease executor accepts TLS 1.2 optional CertificateRequest",
            "Lease executor rejects optional CertificateRequest hostname mismatch",
        },
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
        // This root intentionally links the complete standalone runtime and
        // embedded inference ABI. macOS codegen peaked near 7.6 GiB in Debug
        // and 9.93 GB in ReleaseFast; reserve headroom for safe parallel builds.
        // This is a compile-memory scheduling claim, not a service limit.
        // Linux retains the measured aggregate default.
        .max_rss = @as(usize, if (target.result.os.tag == .macos) 11 else 7) * 1024 * 1024 * 1024,
    });
    const lib_standalone_runtime_test_step = b.step("antfly-standalone-runtime-test", "Run focused standalone runtime tests");
    const run_lib_standalone_runtime_tests = addFilteredTestRunArtifact(b, lib_standalone_runtime_tests);
    lib_standalone_runtime_test_step.dependOn(&run_lib_standalone_runtime_tests.step);

    const raft_test_step = b.step("antfly-raft-test", "Run raft integration unit tests");
    raft_test_step.dependOn(&run_raft_unit_tests.step);
    raft_test_step.dependOn(&run_raft_read_gate_tests.step);
    raft_test_step.dependOn(&run_raft_runtime_tests.step);
    raft_test_step.dependOn(&run_raft_restore_tests.step);
    raft_test_step.dependOn(&run_raft_library_tests.step);
    raft_test_step.dependOn(&run_raft_ready_continuation_tests.step);
    raft_test_step.dependOn(&run_raft_storage_tests.step);
    raft_test_step.dependOn(&run_raft_transition_runtime_docid_tests.step);

    const raft_runtime_test_step = b.step("antfly-raft-runtime-test", "Run focused managed Raft runtime tests");
    raft_runtime_test_step.dependOn(&run_raft_runtime_tests.step);
    raft_runtime_test_step.dependOn(&run_raft_ready_continuation_tests.step);

    const raft_restore_test_step = b.step("antfly-raft-restore-test", "Run focused Raft restore authority and restart tests");
    raft_restore_test_step.dependOn(&run_raft_restore_tests.step);

    const raft_transport_test_step = b.step("antfly-raft-transport-test", "Run raft transport unit tests");
    raft_transport_test_step.dependOn(&run_raft_transport_tests.step);

    const raft_storage_test_step = b.step("antfly-raft-storage-test", "Run Raft snapshot artifact storage tests");
    raft_storage_test_step.dependOn(&run_raft_storage_tests.step);

    unit_test_step.dependOn(&run_lib_generating_runtime_tests.step);
    unit_test_step.dependOn(&run_lib_reranking_runtime_tests.step);
    unit_test_step.dependOn(&run_lib_common_tests.step);
    unit_test_step.dependOn(&run_lib_common_config_tests.step);
    unit_test_step.dependOn(&run_lib_preload_model_spec_tests.step);
    unit_test_step.dependOn(&run_lib_common_secrets_tests.step);
    unit_test_step.dependOn(&run_secret_store_abi_tests.step);
    unit_test_step.dependOn(&run_runtime_io_abi_tests.step);

    unit_test_step.dependOn(&run_api_http_runtime_tests.step);
    unit_test_step.dependOn(&run_lib_usermgr_tests.step);
    unit_test_step.dependOn(&run_raft_read_gate_tests.step);
    unit_test_step.dependOn(&run_usermgr_abi_tests.step);

    unit_test_step.dependOn(&run_embedded_tests.step);
    unit_test_step.dependOn(&run_antfly_embedded_pkg_tests.step);
    unit_test_step.dependOn(&run_capi_tests.step);
    unit_test_step.dependOn(&run_lite_native_tests.step);
    unit_test_step.dependOn(&run_cmd_tests.step);
    unit_test_step.dependOn(&run_introducer_tests.step);
    unit_test_step.dependOn(&run_serverless_tests.step);
    unit_test_step.dependOn(&run_lib_data_runtime_tests.step);
    // Data storage has its own root module, so the root-module `storage.` union
    // cannot discover these split, snapshot, and replica-state contracts. Share
    // the focused artifact with the aggregate to run the curated bucket once.
    unit_test_step.dependOn(&run_lib_data_storage_tests.step);
    unit_test_step.dependOn(&run_lib_api_docid_tests.step);
    unit_test_step.dependOn(&run_lib_db_result_shape_tests.step);
    unit_test_step.dependOn(&run_raft_transition_runtime_docid_tests.step);
    unit_test_step.dependOn(&run_lib_api_auth_tests.step);
    unit_test_step.dependOn(&run_algebraic_dynamic_template_tests.step);
    unit_test_step.dependOn(&run_api_artifact_reprocess_jobs_tests.step);
    unit_test_step.dependOn(&run_api_restore_jobs_tests.step);
    unit_test_step.dependOn(&run_portable_backup_tests.step);
    unit_test_step.dependOn(&run_public_api_parity_aggregate_tests.step);
    unit_test_step.dependOn(&run_lib_template_tests.step);
    unit_test_step.dependOn(&run_lib_audio_tests.step);
    unit_test_step.dependOn(lib_standalone_runtime_test_step);
    // The aggregate's storage HA shard owns the library tests. Keep only the
    // command-root coverage that the shard cannot discover; `antfly-storage-ha-test` remains
    // available as the convenient focused target containing both artifacts.
    unit_test_step.dependOn(&run_ha_cli_tests.step);
    unit_test_step.dependOn(&run_raft_unit_tests.step);
    unit_test_step.dependOn(&run_raft_snapshot_maintenance_vopr_tests.step);
    unit_test_step.dependOn(&run_raft_runtime_tests.step);
    unit_test_step.dependOn(&run_raft_restore_tests.step);
    // The standalone Raft library and Antfly-rooted Raft artifacts already
    // contain the ready-continuation and transport selections, respectively.
    // Preserve their focused targets without executing them twice in `antfly-unit-test`.

    const lmdb_unit_tests = b.addTest(.{
        .root_module = lmdb_engine_mod,
    });
    const run_lmdb_unit_tests = b.addRunArtifact(lmdb_unit_tests);

    const lmdb_test_step = b.step("lmdb-test", "Run Zig LMDB port unit tests");
    lmdb_test_step.dependOn(&run_lmdb_unit_tests.step);

    const storage_lmdb_test_mod = makeLmdbModule(b, "pkg/antfly/src/storage/lmdb.zig", target, optimize, build_options, lmdb_engine_mod, platform_mod, hash_mod);
    const storage_lmdb_unit_tests = b.addTest(.{
        .root_module = storage_lmdb_test_mod,
    });
    const run_storage_lmdb_unit_tests = b.addRunArtifact(storage_lmdb_unit_tests);

    const storage_lmdb_test_step = b.step("antfly-storage-lmdb-test", "Run storage/lmdb wrapper unit tests");
    storage_lmdb_test_step.dependOn(&run_storage_lmdb_unit_tests.step);

    const storage_lmdb_replay_tests = b.addTest(.{
        .root_module = storage_lmdb_test_mod,
        .filters = &.{"LMDB replay fixtures stay green"},
    });
    const run_storage_lmdb_replay_tests = addFilteredTestRunArtifact(b, storage_lmdb_replay_tests);
    const storage_lmdb_replay_step = b.step("lmdb-replay-fixtures", "Run only the LMDB replay fixture test");
    storage_lmdb_replay_step.dependOn(&run_storage_lmdb_replay_tests.step);

    const lmdb_vopr_test_mod = makeLmdbModule(b, "pkg/antfly/src/storage/lmdb_vopr.zig", target, optimize, build_options, lmdb_engine_mod, platform_mod, hash_mod);
    lmdb_vopr_test_mod.addImport("vopr", vopr_mod);
    const lmdb_vopr_tests = b.addTest(.{
        .root_module = lmdb_vopr_test_mod,
        .filters = &.{"LMDB VOPR"},
    });
    const run_lmdb_vopr_tests = addFilteredTestRunArtifact(b, lmdb_vopr_tests);
    const lmdb_vopr_test_step = b.step("lmdb-vopr-test", "Run replayable C-versus-Zig LMDB VOPR campaigns");
    lmdb_vopr_test_step.dependOn(&run_lmdb_vopr_tests.step);

    const storage_vopr_runtime_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/storage_sim_runtime_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    storage_vopr_runtime_test_mod.addImport("antfly_platform", platform_mod);
    storage_vopr_runtime_test_mod.addImport("antfly_hash", hash_mod);
    const storage_vopr_runtime_tests = b.addTest(.{
        .root_module = storage_vopr_runtime_test_mod,
    });
    const run_storage_vopr_runtime_tests = b.addRunArtifact(storage_vopr_runtime_tests);
    const storage_vopr_runtime_test_step = b.step("storage-vopr-runtime-test", "Run storage VOPR runtime and modeled-device tests");
    storage_vopr_runtime_test_step.dependOn(&run_storage_vopr_runtime_tests.step);

    const storage_lmdb_soak_build_options = makeLmdbBuildOptions(b, lmdb_backend, lmdb_evented_async_io, true);
    const storage_lmdb_soak_engine_mod = makeLmdbEngineModule(b, target, optimize, true, storage_lmdb_soak_build_options);
    const storage_lmdb_soak_test_mod = makeLmdbModule(b, "pkg/antfly/src/storage/lmdb.zig", target, optimize, storage_lmdb_soak_build_options, storage_lmdb_soak_engine_mod, platform_mod, hash_mod);
    const storage_lmdb_soak_tests = b.addTest(.{
        .root_module = storage_lmdb_soak_test_mod,
        .filters = &.{"LMDB sim soak stays green"},
    });
    const run_storage_lmdb_soak_tests = addFilteredTestRunArtifact(b, storage_lmdb_soak_tests);
    const storage_lmdb_soak_step = b.step("lmdb-workload-soak", "Run only the legacy LMDB randomized workload soak");
    storage_lmdb_soak_step.dependOn(&run_storage_lmdb_soak_tests.step);

    const docstore_test_mod = makeLmdbModule(b, "pkg/antfly/src/docstore_test_root.zig", target, optimize, build_options, lmdb_engine_mod, platform_mod, hash_mod);
    docstore_test_mod.addImport("bloom", bloom_mod);
    const docstore_unit_tests = b.addTest(.{
        .root_module = docstore_test_mod,
        .test_runner = .{ .path = b.path("pkg/antfly/src/test_runner.zig"), .mode = .simple },
    });
    const run_docstore_unit_tests = b.addRunArtifact(docstore_unit_tests);

    const docstore_test_step = b.step("docstore-test", "Run storage/docstore unit tests");
    docstore_test_step.dependOn(&run_docstore_unit_tests.step);

    const vector_payload_test_mod = makeLmdbModule(b, "pkg/antfly/src/vector_payload_store_test_root.zig", target, optimize, build_options, lmdb_engine_mod, platform_mod, hash_mod);
    vector_payload_test_mod.addImport("bloom", bloom_mod);
    vector_payload_test_mod.addImport("antfly_vectorindex", vectorindex_mod);
    vector_payload_test_mod.addImport("antfly-json", json_mod);
    vector_payload_test_mod.addImport("structlog", structlog_mod);
    const vector_payload_tests = b.addTest(.{
        .root_module = vector_payload_test_mod,
        .test_runner = .{ .path = b.path("pkg/antfly/src/test_runner.zig"), .mode = .simple },
        .filters = &.{ "source vector payloads", "vector references", "table storage settings" },
    });
    const run_vector_payload_tests = b.addRunArtifact(vector_payload_tests);
    const vector_payload_test_step = b.step("vector-payload-test", "Run source vector payload ownership and recovery tests");
    vector_payload_test_step.dependOn(&run_vector_payload_tests.step);
    unit_test_step.dependOn(&run_vector_payload_tests.step);

    const native_vector_store_test_mod = makeLmdbModule(b, "pkg/antfly/src/native_vector_store_test_root.zig", target, optimize, build_options, lmdb_engine_mod, platform_mod, hash_mod);
    native_vector_store_test_mod.addImport("bloom", bloom_mod);
    native_vector_store_test_mod.addImport("antfly_vectorindex", vectorindex_mod);
    native_vector_store_test_mod.addImport("antfly-json", json_mod);
    native_vector_store_test_mod.addImport("structlog", structlog_mod);
    const native_vector_store_tests = b.addTest(.{
        .root_module = native_vector_store_test_mod,
        .test_runner = .{ .path = b.path("pkg/antfly/src/test_runner.zig"), .mode = .simple },
        .filters = &.{ "storage.vector_block_store", "vector block", "vector WAL", "native compaction", "sealed vector", "empty vector authority", "owned staged base", "cold projection workers", "source vector payloads adaptive" },
    });
    const run_native_vector_store_tests = b.addRunArtifact(native_vector_store_tests);
    const native_vector_store_test_step = b.step("vector-block-store-test", "Run native vector segment publication and recovery tests");
    native_vector_store_test_step.dependOn(&run_native_vector_store_tests.step);
    const shard_test_mod = makeLmdbModule(b, "pkg/antfly/src/shard_test_root.zig", target, optimize, build_options, lmdb_engine_mod, platform_mod, hash_mod);
    shard_test_mod.addImport("bloom", bloom_mod);
    const shard_unit_tests = b.addTest(.{
        .root_module = shard_test_mod,
    });
    const run_shard_unit_tests = b.addRunArtifact(shard_unit_tests);

    const shard_test_step = b.step("shard-test", "Run storage/shard unit tests");
    shard_test_step.dependOn(&run_shard_unit_tests.step);

    const wal_test_mod = makeLmdbModule(b, "pkg/antfly/src/wal_test_root.zig", target, optimize, build_options, lmdb_engine_mod, platform_mod, hash_mod);
    wal_test_mod.addImport("bloom", bloom_mod);
    wal_test_mod.addImport("structlog", structlog_mod);
    wal_test_mod.addImport("vopr", vopr_mod);
    const wal_unit_tests = b.addTest(.{
        .root_module = wal_test_mod,
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_wal_unit_tests = b.addRunArtifact(wal_unit_tests);

    const wal_test_step = b.step("wal-test", "Run storage/wal unit tests");
    wal_test_step.dependOn(&run_wal_unit_tests.step);

    const wal_workload_tests = b.addTest(.{
        .root_module = wal_test_mod,
        .filters = &.{"wal sim"},
    });
    const run_wal_workload_tests = addFilteredTestRunArtifact(b, wal_workload_tests);
    const wal_workload_test_step = b.step("wal-workload-test", "Run only the legacy WAL randomized workload tests");
    wal_workload_test_step.dependOn(&run_wal_workload_tests.step);

    const wal_vopr_tests = b.addTest(.{
        .root_module = wal_test_mod,
        .filters = &.{
            "wal group commit uses injected virtual clock",
            "wal can reopen on modeled storage device",
            "wal modeled storage survives crash before close after acknowledged append",
            "modeled device exposes torn writes and acknowledged dropped syncs",
            "wal modeled replay runner uses virtual storage and time",
            "wal modeled crash runner preserves acknowledged public append",
            "wal modeled VOPR campaign stays green",
            "modeled WAL campaign records and exactly replays VOPR traces",
            "modeled WAL VOPR classifies injected write and sync outcomes",
            "modeled WAL VOPR constrains partial-write and dropped-sync recovery outcomes",
            "wal modeled replay fixtures stay green",
            "wal modeled crash fixtures stay green",
            "wal modeled commit backend completion uses scheduled virtual time",
            "wal modeled storage commit delay uses injected virtual clock",
        },
    });
    const run_wal_vopr_tests = addFilteredTestRunArtifact(b, wal_vopr_tests);
    const wal_vopr_test_step = b.step("wal-vopr-test", "Run WAL modeled-time VOPR smoke tests");
    wal_vopr_test_step.dependOn(&run_wal_vopr_tests.step);

    const wal_replay_tests = b.addTest(.{
        .root_module = wal_test_mod,
        .filters = &.{"wal replay fixtures stay green"},
    });
    const run_wal_replay_tests = addFilteredTestRunArtifact(b, wal_replay_tests);
    const wal_replay_step = b.step("wal-replay-fixtures", "Run only the WAL replay fixture tests");
    wal_replay_step.dependOn(&run_wal_replay_tests.step);

    const wal_soak_build_options = makeLmdbBuildOptions(b, lmdb_backend, lmdb_evented_async_io, true);
    const wal_soak_engine_mod = makeLmdbEngineModule(b, target, optimize, true, wal_soak_build_options);
    const wal_soak_test_mod = makeLmdbModule(b, "pkg/antfly/src/wal_test_root.zig", target, optimize, wal_soak_build_options, wal_soak_engine_mod, platform_mod, hash_mod);
    wal_soak_test_mod.addImport("bloom", bloom_mod);
    wal_soak_test_mod.addImport("vopr", vopr_mod);
    const wal_soak_tests = b.addTest(.{
        .root_module = wal_soak_test_mod,
        .filters = &.{"wal sim soak stays green"},
    });
    const run_wal_soak_tests = addFilteredTestRunArtifact(b, wal_soak_tests);
    const wal_soak_step = b.step("wal-workload-soak", "Run only the legacy WAL randomized workload soak");
    wal_soak_step.dependOn(&run_wal_soak_tests.step);

    const storage_workload_soak_step = b.step("storage-workload-soak", "Run the legacy LMDB and WAL randomized workload soaks");
    storage_workload_soak_step.dependOn(&run_storage_lmdb_soak_tests.step);
    storage_workload_soak_step.dependOn(&run_wal_soak_tests.step);

    const persistent_test_mod = makeLmdbModule(b, "pkg/antfly/src/persistent_test_root.zig", target, optimize, build_options, lmdb_engine_mod, platform_mod, hash_mod);
    persistent_test_mod.addImport("bloom", bloom_mod);
    persistent_test_mod.addImport("antfly_vellum", vellum_mod);
    persistent_test_mod.addImport("antfly_regex", regex_mod);
    persistent_test_mod.addImport("antfly_vector", vector_mod);
    persistent_test_mod.addImport("antfly_vectorindex", vectorindex_mod);
    persistent_test_mod.addImport("antfly_reranking", reranking_mod);
    persistent_test_mod.addImport("structlog", structlog_mod);
    persistent_test_mod.addImport("vopr", vopr_mod);
    const persistent_unit_tests = b.addTest(.{
        .root_module = persistent_test_mod,
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_persistent_unit_tests = b.addRunArtifact(persistent_unit_tests);

    const persistent_test_step = b.step("persistent-test", "Run storage/persistent unit tests");
    persistent_test_step.dependOn(&run_persistent_unit_tests.step);

    const persistent_delete_regression_tests = b.addTest(.{
        .root_module = persistent_test_mod,
        .filters = &.{
            "deleteById removes every live duplicate across historical segments",
            "tracked multi-segment deletion can roll back before persistence",
            "persistent index preserves deletion of repeated document versions across reopen",
        },
    });
    const run_persistent_delete_regression_tests = addFilteredTestRunArtifact(b, persistent_delete_regression_tests);
    const persistent_delete_regression_step = b.step("persistent-delete-regression-test", "Run atomic multi-segment deletion regressions");
    persistent_delete_regression_step.dependOn(&run_persistent_delete_regression_tests.step);

    const persistent_workload_tests = b.addTest(.{
        .root_module = persistent_test_mod,
        .filters = &.{"persistent sim workloads stay green"},
    });
    const run_persistent_workload_tests = addFilteredTestRunArtifact(b, persistent_workload_tests);
    const persistent_workload_step = b.step("persistent-workload-test", "Run only the legacy persistent randomized workload tests");
    persistent_workload_step.dependOn(&run_persistent_workload_tests.step);

    const persistent_replay_tests = b.addTest(.{
        .root_module = persistent_test_mod,
        .filters = &.{"persistent replay fixtures stay green"},
    });
    const run_persistent_replay_tests = addFilteredTestRunArtifact(b, persistent_replay_tests);
    const persistent_replay_step = b.step("persistent-replay-fixtures", "Run only the persistent replay fixture tests");
    persistent_replay_step.dependOn(&run_persistent_replay_tests.step);

    const persistent_vopr_tests = b.addTest(.{
        .root_module = persistent_test_mod,
        .filters = &.{
            "persistent modeled replay fixtures stay green",
            "persistent modeled sim workload stays green",
            "persistent modeled full-text compaction publish faults stay green",
            "persistent VOPR",
        },
    });
    const run_persistent_vopr_tests = addFilteredTestRunArtifact(b, persistent_vopr_tests);
    const persistent_vopr_step = b.step("persistent-vopr-test", "Run persistent modeled-storage VOPR smoke tests");
    persistent_vopr_step.dependOn(&run_persistent_vopr_tests.step);

    const persistent_soak_build_options = makeLmdbBuildOptions(b, lmdb_backend, lmdb_evented_async_io, true);
    const persistent_soak_engine_mod = makeLmdbEngineModule(b, target, optimize, true, persistent_soak_build_options);
    const persistent_soak_test_mod = makeLmdbModule(b, "pkg/antfly/src/persistent_test_root.zig", target, optimize, persistent_soak_build_options, persistent_soak_engine_mod, platform_mod, hash_mod);
    persistent_soak_test_mod.addImport("bloom", bloom_mod);
    persistent_soak_test_mod.addImport("antfly_vellum", vellum_mod);
    persistent_soak_test_mod.addImport("antfly_regex", regex_mod);
    persistent_soak_test_mod.addImport("antfly_vector", vector_mod);
    persistent_soak_test_mod.addImport("antfly_vectorindex", vectorindex_mod);
    persistent_soak_test_mod.addImport("antfly_reranking", reranking_mod);
    const persistent_soak_tests = b.addTest(.{
        .root_module = persistent_soak_test_mod,
        .filters = &.{"persistent sim soak stays green"},
    });
    const run_persistent_soak_tests = addFilteredTestRunArtifact(b, persistent_soak_tests);
    const persistent_soak_step = b.step("persistent-workload-soak", "Run only the legacy persistent randomized workload soak");
    persistent_soak_step.dependOn(&run_persistent_soak_tests.step);

    storage_workload_soak_step.dependOn(&run_persistent_soak_tests.step);

    const index_manager_test_mod = makeLmdbModule(b, "pkg/antfly/src/index_manager_test_root.zig", target, optimize, build_options, lmdb_engine_mod, platform_mod, hash_mod);
    addSnowballModule(b, index_manager_test_mod);
    index_manager_test_mod.addImport("bloom", bloom_mod);
    index_manager_test_mod.addImport("antfly_vellum", vellum_mod);
    index_manager_test_mod.addImport("antfly_vector", vector_mod);
    index_manager_test_mod.addImport("antfly_vectorindex", vectorindex_mod);
    index_manager_test_mod.addImport("antfly_matcher", matcher_mod);
    index_manager_test_mod.addImport("antfly_resolver", resolver_mod);
    index_manager_test_mod.addImport("antfly_chunking", chunking_mod);
    index_manager_test_mod.addImport("antfly-json", json_mod);
    index_manager_test_mod.addImport("antfly_scraping", scraping_mod);
    index_manager_test_mod.addImport("antfly_image", image_mod);
    index_manager_test_mod.addImport("antfly_regex", regex_mod);
    index_manager_test_mod.addImport("antfly_reader_config", reader_config_mod);
    index_manager_test_mod.addImport("structlog", structlog_mod);
    index_manager_test_mod.addImport("vopr", vopr_mod);
    const index_manager_unit_tests = b.addTest(.{
        .root_module = index_manager_test_mod,
        .filters = selectTestFilters(b, &.{}),
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_index_manager_unit_tests = addFilteredTestRunArtifact(b, index_manager_unit_tests);

    const index_manager_test_step = b.step("index-manager-test", "Run storage/db/catalog/index_manager unit tests");
    index_manager_test_step.dependOn(&run_index_manager_unit_tests.step);

    const index_manager_resource_tests = b.addTest(.{
        .root_module = index_manager_test_mod,
        .filters = &.{"text merge resource manager accounts pending bytes and active buffers"},
    });
    const run_index_manager_resource_tests = addFilteredTestRunArtifact(b, index_manager_resource_tests);
    const index_manager_resource_step = b.step("index-manager-resource-test", "Run index manager resource-manager accounting tests");
    index_manager_resource_step.dependOn(&run_index_manager_resource_tests.step);

    const index_manager_workload_tests = b.addTest(.{
        .root_module = index_manager_test_mod,
        .filters = &.{"index manager sim workloads stay green"},
    });
    const run_index_manager_workload_tests = addFilteredTestRunArtifact(b, index_manager_workload_tests);
    const index_manager_workload_step = b.step("index-manager-workload-test", "Run only the legacy index-manager randomized workload tests");
    index_manager_workload_step.dependOn(&run_index_manager_workload_tests.step);

    const index_manager_replay_tests = b.addTest(.{
        .root_module = index_manager_test_mod,
        .filters = &.{"index manager replay fixtures stay green"},
    });
    const run_index_manager_replay_tests = addFilteredTestRunArtifact(b, index_manager_replay_tests);
    const index_manager_replay_step = b.step("index-manager-replay-fixtures", "Run only the index manager replay fixture tests");
    index_manager_replay_step.dependOn(&run_index_manager_replay_tests.step);

    const index_manager_vopr_tests = b.addTest(.{
        .root_module = index_manager_test_mod,
        .filters = &.{
            "index manager modeled replay fixtures stay green",
            "index manager modeled crash fixtures stay green",
            "index manager VOPR",
        },
    });
    const run_index_manager_vopr_tests = addFilteredTestRunArtifact(b, index_manager_vopr_tests);
    const index_manager_vopr_step = b.step("index-manager-vopr-test", "Run index manager modeled-storage VOPR smoke tests");
    index_manager_vopr_step.dependOn(&run_index_manager_vopr_tests.step);

    const db_test_mod = makeLmdbModule(b, "pkg/antfly/src/db_test_root.zig", target, optimize, build_options, lmdb_engine_mod, platform_mod, hash_mod);
    const transcribing_db_test_stub_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/testing/transcribing_stub.zig"),
        .target = target,
        .optimize = optimize,
    });
    transcribing_db_test_stub_mod.addImport("httpx", httpx_mod);
    addSnowballModule(b, db_test_mod);
    db_test_mod.addImport("bloom", bloom_mod);
    db_test_mod.addImport("handlebars", handlebars_mod);
    db_test_mod.addImport("antfly_vellum", vellum_mod);
    db_test_mod.addImport("antfly_vector", vector_mod);
    db_test_mod.addImport("antfly_vectorindex", vectorindex_mod);
    db_test_mod.addImport("antfly_matcher", matcher_mod);
    db_test_mod.addImport("antfly_resolver", resolver_mod);
    db_test_mod.addImport("antfly_chunking", chunking_mod);
    db_test_mod.addImport("antfly_regex", regex_mod);
    db_test_mod.addImport("antfly-json", json_mod);
    db_test_mod.addImport("raft_engine", raft_engine_mod);
    db_test_mod.addImport("inference_chunker", inference_chunker_mod);
    db_test_mod.addImport("inference_api", inference_api_mod);
    db_test_mod.addImport("antfly_reranking", reranking_mod);
    db_test_mod.addImport("antfly_scraping", scraping_mod);
    db_test_mod.addImport("antfly_reader_config", reader_config_mod);
    db_test_mod.addImport("antfly_transcribing", transcribing_db_test_stub_mod);
    db_test_mod.addImport("httpx", httpx_mod);
    db_test_mod.addImport("antfly_pdf", pdf_mod);
    db_test_mod.addImport("antfly_image", image_mod);
    db_test_mod.addImport("antfly_font", font_mod);
    db_test_mod.addImport("structlog", structlog_mod);
    db_test_mod.addImport("vopr", vopr_mod);

    const db_split_workload_default_filters = [_][]const u8{
        "db split sim default workload stays green",
        "db split sim reopen-heavy workload stays green",
    };
    const db_split_workload_tests = b.addTest(.{
        .root_module = db_test_mod,
        .filters = selectTestFilters(b, &db_split_workload_default_filters),
    });
    const run_db_split_workload_tests = addFilteredTestRunArtifact(b, db_split_workload_tests);
    const db_split_workload_step = b.step("db-split-workload-test", "Run only the legacy DB split randomized workload tests");
    db_split_workload_step.dependOn(&run_db_split_workload_tests.step);

    const db_split_vopr_tests = b.addTest(.{
        .root_module = db_test_mod,
        .filters = &.{
            "db split modeled replay fixtures stay green",
            "db split modeled sim workloads stay green",
            "DB split VOPR",
        },
    });
    const run_db_split_vopr_tests = addFilteredTestRunArtifact(b, db_split_vopr_tests);
    const db_split_vopr_step = b.step("antfly-storage-db-split-vopr-test", "Run only the DB split modeled-storage replay fixture tests");
    db_split_vopr_step.dependOn(&run_db_split_vopr_tests.step);

    const storage_workload_test_step = b.step("storage-workload-test", "Run legacy deterministic storage workloads that still use real storage I/O");
    storage_workload_test_step.dependOn(&run_wal_workload_tests.step);
    storage_workload_test_step.dependOn(&run_persistent_workload_tests.step);
    storage_workload_test_step.dependOn(&run_index_manager_workload_tests.step);

    const storage_vopr_step = b.step("storage-vopr-test", "Run deterministic storage modeled-time/model-I/O VOPR checks");
    storage_vopr_step.dependOn(&run_storage_vopr_runtime_tests.step);
    storage_vopr_step.dependOn(&run_lib_lsm_vopr_tests.step);
    storage_vopr_step.dependOn(&run_lmdb_vopr_tests.step);
    storage_vopr_step.dependOn(&run_wal_vopr_tests.step);
    storage_vopr_step.dependOn(&run_persistent_vopr_tests.step);
    storage_vopr_step.dependOn(&run_index_manager_vopr_tests.step);
    storage_vopr_step.dependOn(&run_db_split_vopr_tests.step);
    vopr_test_step.dependOn(storage_vopr_step);
    chaos_test_step.dependOn(storage_vopr_step);

    const db_unit_tests = b.addTest(.{
        .root_module = db_test_mod,
        .filters = selectTestFilters(b, &.{}),
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_db_unit_tests = addFilteredTestRunArtifactWithRuntimeFilters(b, db_unit_tests, selectTestFilters(b, &.{}));
    addRuntimeSkipTestFilters(run_db_unit_tests, &release_scale_test_filters);
    // This root contains the complete former full-root DB and result-shape
    // inventory as well as its own unique cases. Run that union once.
    const db_test_step = b.step("antfly-storage-db-test", "Run storage/db tests");
    db_test_step.dependOn(&run_db_unit_tests.step);

    // Keep the small, deterministic release-blocker primitives in the PR/base
    // unit gate. The corpus-scale fixtures below protect thresholds that only
    // appear at thousands of documents and run in the zig-full gate instead.
    const release_blocker_regression_filters = [_][]const u8{
        "non-visible doc set complements visibility per generation",
        "built-in exact dense scorer filters metadata before vector reads",
        "dense search route reports exact native filter budget decisions",
        "dense search route uses measured per-index costs pressure and hysteresis",
        "one percent filtered route preserves exact recall with candidate-linear IO",
        "dense index manager accepts external embedding indexes without enrichments",
        "production external scorers use bounded cache-first artifact batches",
        "progressive filtered l2 traversal preserves exact top k without bound stops",
        "flat rabitq filtered traversal advances then stops on a certified bound",
        "sorted unique vector id subtraction handles sparse and dense exclusions",
    };
    const release_blocker_regression_tests = b.addTest(.{
        .root_module = db_test_mod,
        // A root DB test keeps query/search_exec and dense_exact reachable to
        // Zig's compile-time test discovery. Runtime filters below execute
        // only the fast primitives, never this corpus-scale anchor.
        .filters = compileFiltersWithAnchors(
            b,
            &.{"db dense default dynamic 0.2 percent numeric filter exact scores bounded candidates"},
            &release_blocker_regression_filters,
        ),
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_release_blocker_regression_tests = addFilteredTestRunArtifactWithRuntimeFilters(
        b,
        release_blocker_regression_tests,
        &release_blocker_regression_filters,
    );
    const release_blocker_regression_step = b.step(
        "release-blocker-regression-test",
        "Run selective ANN and post-delete full-text release-blocker regressions",
    );
    release_blocker_regression_step.dependOn(&run_release_blocker_regression_tests.step);
    unit_test_step.dependOn(&run_release_blocker_regression_tests.step);

    const release_scale_tests = b.addTest(.{
        .root_module = db_test_mod,
        .filters = &release_scale_test_filters,
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_release_scale_tests = addFilteredTestRunArtifact(b, release_scale_tests);
    const release_scale_test_step = b.step(
        "release-scale-test",
        "Run corpus-scale ANN and full-text release regressions",
    );
    release_scale_test_step.dependOn(&run_release_scale_tests.step);

    const db_restore_identity_tests = b.addTest(.{
        .root_module = db_test_mod,
        .filters = &.{"db restore snapshot repeatedly validates run-backed doc identity metadata"},
    });
    const run_db_restore_identity_tests = addFilteredTestRunArtifact(b, db_restore_identity_tests);
    const db_restore_identity_step = b.step("antfly-storage-db-restore-identity-test", "Run the focused run-backed identity restore regression");
    db_restore_identity_step.dependOn(&run_db_restore_identity_tests.step);

    // These focused regressions protect production paths introduced by this
    // branch. Keep them in the PR/base gate instead of defining orphan steps
    // that run only when invoked manually.
    unit_test_step.dependOn(&run_lib_api_derived_coverage_tests.step);
    unit_test_step.dependOn(&run_lib_api_storage_authority_tests.step);
    unit_test_step.dependOn(&run_lib_api_connections_tests.step);
    unit_test_step.dependOn(&run_api_table_writes_production_regression_unit_tests.step);

    const db_restore_managed_tests = b.addTest(.{
        .root_module = db_test_mod,
        .filters = &.{
            "db restore snapshot replays managed chunked dense embeddings",
            "native restore backend configuration is resolved exactly once",
            "native restore filesystem publication rejects non-publishable storage capabilities",
        },
    });
    const run_db_restore_managed_tests = addFilteredTestRunArtifact(b, db_restore_managed_tests);
    const db_restore_managed_step = b.step("antfly-storage-db-restore-managed-test", "Run focused managed native restore DB tests");
    db_restore_managed_step.dependOn(&run_db_restore_managed_tests.step);

    const provisioned_write_cache_failed_close_tests = b.addTest(.{
        .root_module = db_test_mod,
        .filters = &.{"provisioned write cache invalidation closes failed managed enrichment db without aborting"},
    });
    const run_provisioned_write_cache_failed_close_tests = addFilteredTestRunArtifact(b, provisioned_write_cache_failed_close_tests);
    const provisioned_write_cache_failed_close_step = b.step(
        "provisioned-write-cache-failed-close-test",
        "Run the focused provisioned write-cache failed-enrichment close regression",
    );
    provisioned_write_cache_failed_close_step.dependOn(&run_provisioned_write_cache_failed_close_tests.step);

    const provisioned_query_visibility_tests = b.addTest(.{
        .root_module = api_table_writes_docid_test_mod,
        .filters = &.{
            "provisioned table write source invalidates cached query db after managed dense replay becomes visible",
            "managed visibility publish hook updates runtime status cache from live writer",
            "provisioned read preparation invalidates readers without closing dirty writer cache",
            "provisioned read preparation does not block on same-table batch after early dirty publication",
            "provisioned table write source runtime status does not inspect read cache hbc stats when dirty",
            "provisioned table write source read cache overlay preserves live replay status",
            "read preparation keeps write cache dirty while auto bulk ingest is active",
            "runtime status request does not finish expired auto bulk ingest",
            "managed startup catch-up ignores stale dirty bit after writer cache entry is gone",
            "provisioned table write source deinit drains restore repair work group",
        },
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_provisioned_query_visibility_tests = addFilteredTestRunArtifact(b, provisioned_query_visibility_tests);
    const run_provisioned_query_visibility_unit_tests = addFilteredTestRunArtifact(b, provisioned_query_visibility_tests);
    run_provisioned_query_visibility_unit_tests.step.dependOn(&run_api_table_writes_production_regression_unit_tests.step);
    const provisioned_query_visibility_step = b.step(
        "provisioned-query-visibility-test",
        "Run the focused managed dense query-visibility cache invalidation regression",
    );
    provisioned_query_visibility_step.dependOn(&run_provisioned_query_visibility_tests.step);
    unit_test_step.dependOn(&run_provisioned_query_visibility_unit_tests.step);

    const db_embeddings_update_tests = b.addTest(.{
        .root_module = db_test_mod,
        .filters = &.{"db document _embeddings update vector index and strip stored special fields"},
    });
    const run_db_embeddings_update_tests = addFilteredTestRunArtifact(b, db_embeddings_update_tests);
    const db_embeddings_update_step = b.step("antfly-storage-db-embeddings-update-test", "Run the explicit _embeddings update DB test");
    db_embeddings_update_step.dependOn(&run_db_embeddings_update_tests.step);

    const db_merge_cutover_tests = b.addTest(.{
        .root_module = db_test_mod,
        .filters = &.{"db merge-style cutover preserves enrichment resume and fencing across reopen"},
    });
    const run_db_merge_cutover_tests = addFilteredTestRunArtifact(b, db_merge_cutover_tests);
    const db_merge_cutover_step = b.step("antfly-storage-db-merge-cutover-test", "Run the merge cutover enrichment reopen DB test");
    db_merge_cutover_step.dependOn(&run_db_merge_cutover_tests.step);

    const db_shared_embedding_tests = b.addTest(.{
        .root_module = db_test_mod,
        .filters = &.{"db shared embedding enrichment feeds multiple dense indexes"},
    });
    const run_db_shared_embedding_tests = addFilteredTestRunArtifact(b, db_shared_embedding_tests);
    const db_shared_embedding_step = b.step("antfly-storage-db-shared-embedding-test", "Run the shared embedding enrichment DB test");
    db_shared_embedding_step.dependOn(&run_db_shared_embedding_tests.step);

    const db_dense_parent_paging_tests = b.addTest(.{
        .root_module = db_test_mod,
        .filters = &.{"db dense parent paging fetches enough chunk hits before grouping"},
    });
    const run_db_dense_parent_paging_tests = addFilteredTestRunArtifact(b, db_dense_parent_paging_tests);
    const db_dense_parent_paging_step = b.step("antfly-storage-db-dense-parent-paging-test", "Run the dense parent paging enrichment DB test");
    db_dense_parent_paging_step.dependOn(&run_db_dense_parent_paging_tests.step);

    const db_split_replay_tests = b.addTest(.{
        .root_module = db_test_mod,
        .filters = &.{"db split replay fixtures stay green"},
    });
    const run_db_split_replay_tests = addFilteredTestRunArtifact(b, db_split_replay_tests);
    const db_split_replay_step = b.step("db-split-replay-fixtures", "Run only the DB split replay fixture tests");
    db_split_replay_step.dependOn(&run_db_split_replay_tests.step);

    const sparse_test_mod = makeLmdbModule(b, "pkg/antfly/src/sparse_test_root.zig", target, optimize, build_options, lmdb_engine_mod, platform_mod, hash_mod);
    sparse_test_mod.addImport("bloom", bloom_mod);
    // Sparse lifecycle tests reach BackendRuntime through shared storage code;
    // its lazy PDF lane is part of that module graph even when the test itself
    // does not render a document.
    sparse_test_mod.addImport("antfly_pdf", pdf_mod);
    const sparse_unit_tests = b.addTest(.{
        .root_module = sparse_test_mod,
    });
    // This aggregate exercises long-running storage lifecycle tests. Use the
    // simple runner so failures retain per-test/cleanup attribution instead of
    // collapsing into an opaque test-server process exit.
    const run_sparse_unit_tests = addFilteredTestRunArtifact(b, sparse_unit_tests);

    const sparse_test_step = b.step("sparse-test", "Run sparse index unit tests");
    sparse_test_step.dependOn(&run_sparse_unit_tests.step);

    const derived_log_test_mod = makeLmdbModule(b, "pkg/antfly/src/derived_log_test_root.zig", target, optimize, build_options, lmdb_engine_mod, platform_mod, hash_mod);
    derived_log_test_mod.addImport("bloom", bloom_mod);
    const derived_log_unit_tests = b.addTest(.{
        .root_module = derived_log_test_mod,
    });
    const run_derived_log_unit_tests = b.addRunArtifact(derived_log_unit_tests);

    const derived_log_test_step = b.step("derived-log-test", "Run storage/db/derived/derived_log unit tests");
    derived_log_test_step.dependOn(&run_derived_log_unit_tests.step);

    // The root storage filter used to produce one roughly 2,400-test codegen
    // unit. Split it by implementation domain so CI never has to retain the
    // complete DB, HA, LSM, Lite, and storage utility test graph in one compiler
    // process. These compile-time prefixes are disjoint; the existing runtime
    // filters below continue to own overlap and caller-selected filtering.
    const unit_storage_shard_filters = [_][]const []const u8{
        &.{"storage.db.algebraic."},
        &.{
            "storage.db.derived.",
            "storage.db.enrichment.",
        },
        &.{
            "storage.db.catalog.",
            "storage.db.maintenance.",
            "storage.db.query.",
        },
        &.{
            "storage.db.aggregations.",
            "storage.db.apply_rw_lock.",
            "storage.db.artifact_ids.",
            "storage.db.backfill_state.",
            "storage.db.batcher.",
            "storage.db.config.",
            "storage.db.db.",
            "storage.db.dense_exact.",
            "storage.db.doc_filter_wire.",
            "storage.db.doc_identity.",
            "storage.db.doc_set.",
            "storage.db.document_content_hash.",
            "storage.db.document_mapper.",
            "storage.db.document_query.",
            "storage.db.generation_lifecycle.",
            "storage.db.graph_asset_state.",
            "storage.db.graph_edge_contender.",
            "storage.db.graph_state_name.",
            "storage.db.lease.",
            "storage.db.merge_state.",
            "storage.db.mod.",
            "storage.db.native_backup.",
            "storage.db.ownership.",
            "storage.db.planning_stats.",
            "storage.db.promotion_runtime.",
            "storage.db.publication.",
            "storage.db.query_metrics.",
            "storage.db.range_state.",
            "storage.db.resolution_handoff.",
            "storage.db.resolution_runtime.",
            "storage.db.root_identity.",
            "storage.db.snapshot_admission.",
            "storage.db.template_remote_stub.",
            "storage.db.template_stub.",
            "storage.db.transform.",
            "storage.db.typed_doc_values_coverage.",
            "storage.db.types.",
        },
        &.{"storage.ha."},
        &.{
            "storage.lite.",
            "storage.lsm.",
            "storage.lsm_backend.",
            "storage.lsm_backend_sim_test.",
            "storage.lsm_vopr.",
        },
        &.{
            "storage.backend_adapter.",
            "storage.artifact_payload.",
            "storage.admission_waiter.",
            "storage.dense_work_admission.",
            "storage.maintenance_signal.",
            "storage.projection_page_cache.",
            "storage.projection_read_trace.",
            "storage.vector_payload_store.",
            "storage.vector_wal_view.",
            "storage.backend_conformance_test.",
            "storage.backend_erased.",
            "storage.backend_types.",
            "storage.background_runtime.",
            "storage.backup_bundle.",
            "storage.backup_bundle_io.",
            "storage.backup_codec.",
            "storage.backup_repository.",
            "storage.coverage_identity.",
            "storage.db_split_vopr.",
            "storage.derived_log_test_root.",
            "storage.docstore.",
            "storage.enrichment.",
            "storage.filesystem_capacity.",
            "storage.generation_publication.",
            "storage.hbc_adapter.",
            "storage.hierarchy_navigation.",
            "storage.index_manager_vopr.",
            "storage.internal_keys.",
            "storage.lmdb.",
            "storage.lmdb_backend.",
            "storage.lmdb_vopr.",
            "storage.maintenance.",
            "storage.mem_backend.",
            "storage.mem_ordered.",
            "storage.object_storage.",
            "storage.persistent.",
            "storage.persistent_vopr.",
            "storage.portable_backup.",
            "storage.posting_segment_store.",
            "storage.resource_manager.",
            "storage.rowsource.",
            "storage.schema.",
            "storage.shard.",
            "storage.sim_runtime.",
            "storage.transactions.",
            "storage.transaction_vopr.",
            "storage.ttl.",
            "storage.vector_block_store.",
            "storage.vopr_durable_job_lane.",
            "storage.wal.",
            "storage.wal_vopr.",
        },
    };
    const unit_storage_db_core_shard_index = 3;
    // Recent CI timings put these DB categories at 279 seconds and the
    // complement at 297 seconds. Run the two halves from one compiled DB-core
    // artifact so the dominant shard gets parallel runtime without duplicating
    // its expensive semantic analysis and code generation.
    const unit_storage_db_core_lane_filters = [_][]const u8{
        "db restore",
        "db explicit doc-id",
        "db artifact repair",
        "db document",
        "db dense",
        "db split",
    };
    const unit_storage_recall_filters = [_][]const u8{"HBC recall"};
    const unit_storage_sharded_test_step = b.step(
        "unit-storage-test",
        "Run the storage portion of the default antfly-unit-test target in bounded codegen shards",
    );
    const unit_storage_shard_audit = b.addSystemCommand(&.{"python3"});
    unit_storage_shard_audit.addFileArg(b.path("tools/audit_storage_test_shards.py"));
    unit_storage_shard_audit.addArg("--root");
    unit_storage_shard_audit.addDirectoryArg(b.path("pkg/antfly/src/storage"));
    unit_storage_shard_audit.addArg("--manifest");
    unit_storage_shard_audit.addFileArg(b.path("pkg/antfly/src/storage/test_manifest.zig"));
    for (unit_storage_shard_filters) |shard_filters| {
        for (shard_filters) |shard_filter| {
            unit_storage_shard_audit.addArgs(&.{ "--filter", shard_filter });
        }
    }
    unit_storage_shard_audit.addArg("--runtime-partition-source");
    unit_storage_shard_audit.addFileArg(b.path("pkg/antfly/src/storage/db/db.zig"));
    for (unit_storage_db_core_lane_filters) |lane_filter| {
        unit_storage_shard_audit.addArgs(&.{ "--runtime-partition-filter", lane_filter });
    }
    const unit_storage_shard_audit_step = b.step(
        "unit-storage-test-audit",
        "Verify every test-bearing storage module belongs to a bounded codegen shard",
    );
    unit_storage_shard_audit_step.dependOn(&unit_storage_shard_audit.step);
    const storage_runtime_filter_is_default =
        lib_storage_runtime_filters.len == 1 and
        std.mem.eql(u8, lib_storage_runtime_filters[0], "storage.");
    // Preserve three independent compiler processes while removing four
    // repeated semantic-analysis/code-generation passes through the broad
    // Antfly test root. DB core remains isolated, the engine artifact owns HA
    // and LSM/Lite, and the support artifact owns the other four logical
    // ownership groups plus the reusable recall tests.
    var unit_storage_support_compile_filters: []const []const u8 = &.{};
    for ([_]usize{ 0, 1, 2, 6 }) |shard_index| {
        unit_storage_support_compile_filters = compileFiltersWithAnchors(
            b,
            unit_storage_support_compile_filters,
            unit_storage_shard_filters[shard_index],
        );
    }
    unit_storage_support_compile_filters = compileFiltersWithAnchors(
        b,
        unit_storage_support_compile_filters,
        &unit_storage_recall_filters,
    );
    var unit_storage_engine_compile_filters: []const []const u8 = &.{};
    for ([_]usize{ 4, 5 }) |shard_index| {
        unit_storage_engine_compile_filters = compileFiltersWithAnchors(
            b,
            unit_storage_engine_compile_filters,
            unit_storage_shard_filters[shard_index],
        );
    }

    const unit_storage_support_tests = b.addTest(.{
        .name = "storage-support-tests",
        .root_module = antfly_test_mod,
        .filters = unit_storage_support_compile_filters,
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
        // The consolidated ReleaseFast support artifact now peaks just over
        // 10 GiB on macOS Zig 0.16. This is a compiler scheduler reservation,
        // not a runtime memory allowance for Antfly.
        .max_rss = 12 * 1024 * 1024 * 1024,
    });
    unit_storage_support_tests.step.dependOn(&unit_storage_shard_audit.step);
    const run_unit_storage_support_tests = b.addRunArtifact(unit_storage_support_tests);
    configureUnitStorageTestRun(
        b,
        run_unit_storage_support_tests,
        lib_storage_runtime_filters,
        !storage_runtime_filter_is_default,
        lib_unit_filters,
        &root_test_skip_filters,
        &.{},
        false,
    );
    unit_test_step.dependOn(&run_unit_storage_support_tests.step);
    unit_storage_sharded_test_step.dependOn(&run_unit_storage_support_tests.step);

    const unit_storage_engine_tests = b.addTest(.{
        .name = "storage-engine-tests",
        .root_module = antfly_test_mod,
        .filters = unit_storage_engine_compile_filters,
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
        // The consolidated ReleaseFast engine artifact reaches about 9.1 GiB
        // on current macOS Zig 0.16 builds. Reserve the measured envelope so
        // the scheduler can keep independent artifacts parallel without
        // rejecting this compiler after it crosses the stale 8 GiB estimate.
        .max_rss = 10 * 1024 * 1024 * 1024,
    });
    unit_storage_engine_tests.step.dependOn(&unit_storage_shard_audit.step);
    const run_unit_storage_engine_tests = b.addRunArtifact(unit_storage_engine_tests);
    configureUnitStorageTestRun(
        b,
        run_unit_storage_engine_tests,
        lib_storage_runtime_filters,
        !storage_runtime_filter_is_default,
        lib_unit_filters,
        &root_test_skip_filters,
        &.{},
        // This artifact owns HA, so do not apply the broad-root HA skip.
        true,
    );
    // Keep runtime memory bounded to the existing two lanes. This dependency
    // orders only the Run steps; all three artifacts remain free to compile in
    // parallel.
    run_unit_storage_engine_tests.step.dependOn(&run_unit_storage_support_tests.step);
    unit_test_step.dependOn(&run_unit_storage_engine_tests.step);
    unit_storage_sharded_test_step.dependOn(&run_unit_storage_engine_tests.step);

    const unit_storage_db_core_tests = b.addTest(.{
        .name = "storage-db-core-tests",
        .root_module = antfly_test_mod,
        .filters = unit_storage_shard_filters[unit_storage_db_core_shard_index],
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
        // The consolidated ReleaseFast DB-core artifact now peaks just over
        // 10 GiB on macOS Zig 0.16. This reserves compiler scheduling capacity;
        // it does not raise Antfly's runtime memory budget.
        .max_rss = 12 * 1024 * 1024 * 1024,
    });
    unit_storage_db_core_tests.step.dependOn(&unit_storage_shard_audit.step);
    const unit_storage_compile_step = b.step(
        "unit-storage-compile",
        "Compile the three storage antfly-unit-test artifacts without running them",
    );
    unit_storage_compile_step.dependOn(&unit_storage_support_tests.step);
    unit_storage_compile_step.dependOn(&unit_storage_engine_tests.step);
    unit_storage_compile_step.dependOn(&unit_storage_db_core_tests.step);

    // Keep an explicit, opt-in equivalence check for future changes to these
    // groupings. It compiles the former seven-artifact layout and compares the
    // union of declared named tests with the default three-artifact layout.
    // Neither the baseline artifacts nor this comparison are dependencies of
    // antfly-unit-test, unit-storage-test, or antfly-storage-vectorindex-recall-test.
    const unit_storage_baseline_names = [_][]const u8{
        "storage-algebraic-baseline-tests",
        "storage-derived-enrichment-baseline-tests",
        "storage-query-catalog-baseline-tests",
        "storage-db-core-baseline-tests",
        "storage-ha-baseline-tests",
        "storage-lsm-lite-baseline-tests",
        "storage-utility-baseline-tests",
    };
    const unit_storage_baseline_max_rss = [_]usize{
        4 * 1024 * 1024 * 1024,
        5 * 1024 * 1024 * 1024,
        5 * 1024 * 1024 * 1024,
        8 * 1024 * 1024 * 1024,
        5 * 1024 * 1024 * 1024,
        5 * 1024 * 1024 * 1024,
        5 * 1024 * 1024 * 1024,
    };
    var unit_storage_baseline_tests: [unit_storage_shard_filters.len]*std.Build.Step.Compile = undefined;
    for (
        unit_storage_shard_filters,
        unit_storage_baseline_names,
        unit_storage_baseline_max_rss,
        0..,
    ) |shard_filters, shard_name, shard_max_rss, shard_index| {
        const compile_filters = if (shard_index == 6)
            compileFiltersWithAnchors(b, shard_filters, &unit_storage_recall_filters)
        else
            shard_filters;
        unit_storage_baseline_tests[shard_index] = b.addTest(.{
            .name = shard_name,
            .root_module = antfly_test_mod,
            .filters = compile_filters,
            .test_runner = .{
                .path = b.path("pkg/antfly/src/test_runner.zig"),
                .mode = .simple,
            },
            .max_rss = shard_max_rss,
        });
        unit_storage_baseline_tests[shard_index].step.dependOn(&unit_storage_shard_audit.step);
    }
    const compare_unit_storage_inventory = b.addSystemCommand(&.{"python3"});
    compare_unit_storage_inventory.setName("compare consolidated storage test inventory");
    compare_unit_storage_inventory.addFileArg(b.path("tools/compare_test_inventories.py"));
    compare_unit_storage_inventory.addArg("--baseline");
    for (unit_storage_baseline_tests) |baseline_tests| {
        compare_unit_storage_inventory.addArtifactArg(baseline_tests);
    }
    compare_unit_storage_inventory.addArg("--candidate");
    compare_unit_storage_inventory.addArtifactArg(unit_storage_support_tests);
    compare_unit_storage_inventory.addArtifactArg(unit_storage_engine_tests);
    compare_unit_storage_inventory.addArtifactArg(unit_storage_db_core_tests);
    compare_unit_storage_inventory.addArgs(&.{ "--label", "storage consolidation" });
    const unit_storage_inventory_step = b.step(
        "unit-storage-test-inventory",
        "Verify the consolidated artifacts preserve the seven-shard named test inventory",
    );
    unit_storage_inventory_step.dependOn(&compare_unit_storage_inventory.step);

    if (storage_runtime_filter_is_default) {
        // Zig serializes independent checked Run steps for one artifact. Use
        // one scheduler-visible step that launches both filtered DB processes,
        // preserving one compile while realizing the overlap.
        const run_db_core_partitioned_tests = b.addSystemCommand(&.{"python3"});
        run_db_core_partitioned_tests.setName("run test storage-db-core-tests partitioned");
        run_db_core_partitioned_tests.addFileArg(b.path("tools/run_test_partitions.py"));
        run_db_core_partitioned_tests.addArg("--executable");
        run_db_core_partitioned_tests.addArtifactArg(unit_storage_db_core_tests);
        for (unit_storage_db_core_lane_filters) |lane_filter| {
            run_db_core_partitioned_tests.addArgs(&.{ "--partition-filter", lane_filter });
        }
        for (lib_unit_filters) |filter| {
            run_db_core_partitioned_tests.addArgs(&.{ "--common-skip-filter", filter });
        }
        for (root_test_skip_filters) |filter| {
            run_db_core_partitioned_tests.addArgs(&.{ "--common-skip-filter", filter });
        }
        for (release_scale_test_filters) |filter| {
            run_db_core_partitioned_tests.addArgs(&.{ "--common-skip-filter", filter });
        }
        if (b.args) |runtime_args| {
            if (runtime_args.len != 0) {
                run_db_core_partitioned_tests.addArg("--");
                run_db_core_partitioned_tests.addArgs(runtime_args);
            }
        }
        run_db_core_partitioned_tests.stdio = .inherit;
        run_db_core_partitioned_tests.step.max_rss = 12 * 1024 * 1024 * 1024;
        unit_test_step.dependOn(&run_db_core_partitioned_tests.step);
        unit_storage_sharded_test_step.dependOn(&run_db_core_partitioned_tests.step);
    } else {
        const run_unit_storage_db_core_tests = b.addRunArtifact(unit_storage_db_core_tests);
        configureUnitStorageTestRun(
            b,
            run_unit_storage_db_core_tests,
            lib_storage_runtime_filters,
            true,
            lib_unit_filters,
            &root_test_skip_filters,
            &.{},
            false,
        );
        unit_test_step.dependOn(&run_unit_storage_db_core_tests.step);
        unit_storage_sharded_test_step.dependOn(&run_unit_storage_db_core_tests.step);
    }

    // Reuse the storage utility executable instead of compiling another copy
    // of the broad Antfly root solely for the two corpus recall tests. Its
    // ordinary unit run selects `storage.` and therefore never executes these
    // additional compile-time tests.
    const compiled_recall_tests = unit_storage_support_tests;
    const run_compiled_recall_tests = addFilteredTestRunArtifactWithRuntimeFilters(
        b,
        compiled_recall_tests,
        &unit_storage_recall_filters,
    );
    recall_test_step.dependOn(&run_compiled_recall_tests.step);

    // The complete metadata namespace pulls in the VOPR harness and a
    // large amount of control-plane code even though the default unit target
    // excludes simulations at runtime. Compile the production metadata tests
    // in module-owned shards so no individual Linux test image has to load the
    // entire namespace. An explicit, flat production test root gives every
    // shard a stable compile-time ownership prefix without traversing the
    // public metadata namespace or its VOPR imports. The sets are
    // disjoint, and the default runtime selection uses the same ownership
    // prefixes so an accidental empty shard is a hard failure.
    const unit_metadata_shard_filters = [_][]const []const u8{
        &.{"metadata.reconciler."},
        &.{
            "metadata.service.",
            "metadata.catalog_projection_reader.",
            "metadata.admin_read_operations.",
            "metadata.admin_mutation_operations.",
            "metadata.extension_operations.",
            "metadata.node_operations.",
            "metadata.table_operations.",
            "metadata.http_client.",
            "metadata.http_routes.",
            "metadata.http_server.",
        },
        &.{
            "metadata.state.",
            "metadata.runtime.",
            "metadata.authority.",
            "metadata.incarnation.",
            "metadata.reconcile_lease.",
            "metadata.store_observer.",
        },
        &.{
            "metadata.api.",
            "metadata.admin.",
        },
        &.{"metadata.server."},
        &.{
            "metadata.placement_planner.",
            "metadata.control_loop.",
            "metadata.table_manager.",
            "metadata.table_workflow.",
            "metadata.transition_state.",
            "metadata.transition_actions.",
            "metadata.transition_controller.",
            "metadata.transition_driver.",
        },
        &.{"metadata.table_provisioner."},
        &.{"metadata.replication_backfill."},
        &.{"metadata.storage."},
    };
    const metadata_runtime_filter_is_default =
        lib_metadata_runtime_filters.len == 1 and
        std.mem.eql(u8, lib_metadata_runtime_filters[0], "metadata.");
    // Preserve the former two compile lanes while compiling each lane's
    // production metadata ownership groups into one executable. This removes
    // seven repeated semantic-analysis/code-generation passes without
    // constructing the public metadata barrel that also owns VOPR fixtures.
    const unit_metadata_artifact_shard_indices = [_][]const usize{
        &.{ 0, 2, 4, 6, 8 },
        &.{ 1, 3, 5, 7 },
    };
    const unit_metadata_artifact_names = [_][]const u8{
        "metadata-unit-lane-a-tests",
        "metadata-unit-lane-b-tests",
    };
    var unit_metadata_compile_filters: [unit_metadata_artifact_shard_indices.len][]const []const u8 = .{
        &.{},
        &.{},
    };
    for (unit_metadata_artifact_shard_indices, &unit_metadata_compile_filters) |shard_indices, *compile_filters| {
        for (shard_indices) |shard_index| {
            compile_filters.* = compileFiltersWithAnchors(
                b,
                compile_filters.*,
                unit_metadata_shard_filters[shard_index],
            );
        }
    }

    var unit_metadata_tests: [metadata_unit_test_mods.len]*std.Build.Step.Compile = undefined;
    for (
        unit_metadata_artifact_names,
        metadata_unit_test_mods,
        unit_metadata_compile_filters,
        &unit_metadata_tests,
    ) |artifact_name, test_mod, compile_filters, *tests| {
        tests.* = b.addTest(.{
            .name = artifact_name,
            .root_module = test_mod,
            .filters = compile_filters,
            .test_runner = .{
                .path = b.path("pkg/antfly/src/test_runner.zig"),
                .mode = .simple,
            },
            // The consolidated service/HTTP lane reaches roughly 9.3 GiB on
            // macOS Zig 0.16. This is compiler scheduling capacity, not an
            // Antfly runtime budget; retain headroom for codegen variance.
            .max_rss = 12 * 1024 * 1024 * 1024,
        });
    }
    const unit_metadata_compile_step = b.step(
        "unit-metadata-compile",
        "Compile the two consolidated metadata antfly-unit-test artifacts without running them",
    );
    for (unit_metadata_tests) |tests| unit_metadata_compile_step.dependOn(&tests.step);

    // Keep the prior nine-artifact layout as an opt-in coverage oracle. It is
    // deliberately absent from antfly-unit-test and antfly-metadata-test.
    const unit_metadata_baseline_names = [_][]const u8{
        "metadata-reconciler-tests",
        "metadata-service-http-tests",
        "metadata-core-tests",
        "metadata-api-admin-tests",
        "metadata-server-tests",
        "metadata-planning-transition-tests",
        "metadata-table-provisioner-tests",
        "metadata-replication-backfill-tests",
        "metadata-storage-tests",
    };
    const unit_metadata_baseline_max_rss = [_]usize{
        5 * 1024 * 1024 * 1024,
        5 * 1024 * 1024 * 1024,
        7 * 1024 * 1024 * 1024,
        5 * 1024 * 1024 * 1024,
        7 * 1024 * 1024 * 1024,
        5 * 1024 * 1024 * 1024,
        5 * 1024 * 1024 * 1024,
        7 * 1024 * 1024 * 1024,
        6 * 1024 * 1024 * 1024,
    };
    const unit_metadata_baseline_lanes = [_]usize{ 0, 1, 0, 1, 0, 1, 0, 1, 0 };
    var unit_metadata_baseline_compile_tails = [_]?*std.Build.Step{ null, null };
    var unit_metadata_baseline_tests: [metadata_unit_baseline_mods.len]*std.Build.Step.Compile = undefined;
    for (
        unit_metadata_shard_filters,
        unit_metadata_baseline_names,
        metadata_unit_baseline_mods,
        unit_metadata_baseline_max_rss,
        unit_metadata_baseline_lanes,
        &unit_metadata_baseline_tests,
    ) |shard_filters, artifact_name, test_mod, max_rss, lane, *tests| {
        tests.* = b.addTest(.{
            .name = artifact_name,
            .root_module = test_mod,
            .filters = shard_filters,
            .test_runner = .{
                .path = b.path("pkg/antfly/src/test_runner.zig"),
                .mode = .simple,
            },
            .max_rss = max_rss,
        });
        if (unit_metadata_baseline_compile_tails[lane]) |previous| {
            tests.*.step.dependOn(previous);
        } else {
            for (unit_metadata_tests) |candidate| tests.*.step.dependOn(&candidate.step);
        }
        unit_metadata_baseline_compile_tails[lane] = &tests.*.step;
    }
    const compare_unit_metadata_inventory = b.addSystemCommand(&.{"python3"});
    compare_unit_metadata_inventory.setName("compare consolidated metadata test inventory");
    compare_unit_metadata_inventory.addFileArg(b.path("tools/compare_test_inventories.py"));
    compare_unit_metadata_inventory.addArg("--baseline");
    for (unit_metadata_baseline_tests) |tests| compare_unit_metadata_inventory.addArtifactArg(tests);
    compare_unit_metadata_inventory.addArg("--candidate");
    for (unit_metadata_tests) |tests| compare_unit_metadata_inventory.addArtifactArg(tests);
    compare_unit_metadata_inventory.addArgs(&.{ "--label", "metadata consolidation" });
    const unit_metadata_inventory_step = b.step(
        "unit-metadata-test-inventory",
        "Verify the two consolidated artifacts preserve the nine-shard named test inventory",
    );
    unit_metadata_inventory_step.dependOn(&compare_unit_metadata_inventory.step);

    var unit_metadata_lane_a_pre_filters: []const []const u8 = &.{};
    for ([_]usize{ 0, 2, 4, 6 }) |shard_index| {
        unit_metadata_lane_a_pre_filters = compileFiltersWithAnchors(
            b,
            unit_metadata_lane_a_pre_filters,
            unit_metadata_shard_filters[shard_index],
        );
    }
    var unit_metadata_lane_b_pre_filters: []const []const u8 = &.{};
    for ([_]usize{ 1, 3, 5 }) |shard_index| {
        unit_metadata_lane_b_pre_filters = compileFiltersWithAnchors(
            b,
            unit_metadata_lane_b_pre_filters,
            unit_metadata_shard_filters[shard_index],
        );
    }
    const MetadataRuntimePartition = struct {
        name: []const u8,
        artifact_index: usize,
        filters: []const []const u8,
        other_filters: []const []const u8,
    };
    const unit_metadata_runtime_partitions = [_]MetadataRuntimePartition{
        .{
            .name = "metadata-unit-lane-a-pre-tests",
            .artifact_index = 0,
            .filters = unit_metadata_lane_a_pre_filters,
            .other_filters = unit_metadata_shard_filters[8],
        },
        .{
            .name = "metadata-unit-lane-b-pre-tests",
            .artifact_index = 1,
            .filters = unit_metadata_lane_b_pre_filters,
            .other_filters = unit_metadata_shard_filters[7],
        },
        .{
            .name = "metadata-replication-backfill-tests",
            .artifact_index = 1,
            .filters = unit_metadata_shard_filters[7],
            .other_filters = unit_metadata_lane_b_pre_filters,
        },
        .{
            .name = "metadata-storage-tests",
            .artifact_index = 0,
            .filters = unit_metadata_shard_filters[8],
            .other_filters = unit_metadata_lane_a_pre_filters,
        },
    };
    var unit_metadata_aggregate_runs: [unit_metadata_runtime_partitions.len]*std.Build.Step.Run = undefined;
    var unit_metadata_focused_runs: [unit_metadata_runtime_partitions.len]*std.Build.Step.Run = undefined;
    for (
        unit_metadata_runtime_partitions,
        &unit_metadata_aggregate_runs,
        &unit_metadata_focused_runs,
    ) |partition, *aggregate_run, *focused_run| {
        const runtime_filters = if (metadata_runtime_filter_is_default)
            partition.filters
        else
            lib_metadata_runtime_filters;

        aggregate_run.* = b.addRunArtifact(unit_metadata_tests[partition.artifact_index]);
        aggregate_run.*.setName(b.fmt("run test {s}", .{partition.name}));
        addRuntimeTestFilters(b, aggregate_run.*, runtime_filters);
        addRuntimeSkipTestFilters(aggregate_run.*, lib_unit_filters);
        for (root_test_skip_filters) |filter| {
            aggregate_run.*.addArgs(&.{ "--skip-test-filter", filter });
        }
        if (!metadata_runtime_filter_is_default) {
            aggregate_run.*.addArg("--allow-empty-test-filter");
            addRuntimeSkipTestFilters(aggregate_run.*, partition.other_filters);
        }
        unit_test_step.dependOn(&aggregate_run.*.step);

        // The standalone metadata step owns its selected metadata tests. Use a
        // separate run policy so a caller filter is not mistaken for the root
        // aggregate's overlap exclusion and skipped everywhere.
        focused_run.* = b.addRunArtifact(unit_metadata_tests[partition.artifact_index]);
        focused_run.*.setName(b.fmt("run focused test {s}", .{partition.name}));
        addRuntimeTestFilters(b, focused_run.*, runtime_filters);
        if (!metadata_runtime_filter_is_default) {
            focused_run.*.addArg("--allow-empty-test-filter");
            addRuntimeSkipTestFilters(focused_run.*, partition.other_filters);
        }
        for (root_test_skip_filters) |filter| {
            focused_run.*.addArgs(&.{ "--skip-test-filter", filter });
        }
        lib_metadata_test_step.dependOn(&focused_run.*.step);
    }

    // Preserve the existing runtime isolation: ordinary lane work overlaps,
    // replication backfill runs alone, and metadata storage follows it.
    unit_metadata_aggregate_runs[2].step.dependOn(&unit_metadata_aggregate_runs[0].step);
    unit_metadata_aggregate_runs[2].step.dependOn(&unit_metadata_aggregate_runs[1].step);
    unit_metadata_aggregate_runs[3].step.dependOn(&unit_metadata_aggregate_runs[2].step);
    unit_metadata_focused_runs[2].step.dependOn(&unit_metadata_focused_runs[0].step);
    unit_metadata_focused_runs[2].step.dependOn(&unit_metadata_focused_runs[1].step);
    unit_metadata_focused_runs[3].step.dependOn(&unit_metadata_focused_runs[2].step);

    // Default Antfly unit coverage is hermetic: no network fetchers, no
    // benchmarks, and no soak/conformance suites that require external corpora.
    // Runtime exclusions above give explicit API filters first ownership,
    // storage second ownership, and module-sharded metadata third ownership.
    dependOnAll(unit_test_step, &.{
        &run_common_http_tests.step,
        &run_api_json_helpers_tests.step,
        &run_antfly_client_pkg_tests.step,
        &run_lib_unit_tests.step,
        &run_sparse_unit_tests.step,
    });

    return .{
        .vopr_soak_test_step = vopr_soak_test_step,
        .storage_workload_soak_step = storage_workload_soak_step,
        .antfly_test_mod = antfly_test_mod,
        .run_antfly_embedded_pkg_tests = run_antfly_embedded_pkg_tests,
        .run_lite_native_tests = run_lite_native_tests,
        .run_lite_cmd_tests = run_lite_cmd_tests,
        .run_lib_ha_compat_tests = run_lib_ha_compat_tests,
        .antfly_test_step = antfly_test_step,
        .unit_test_step = unit_test_step,
        .standalone_runtime_test_step = lib_standalone_runtime_test_step,
        .vopr_test_step = vopr_test_step,
        .integration_test_step = integration_test_step,
        .chaos_test_step = chaos_test_step,
        .compiled_recall_tests = compiled_recall_tests,
    };
}

/// Exercise native PDF decoding without pulling its implementation into every
/// unit-test root. The entrypoint owns the public targets and aggregates.
pub fn createPdfIntegration(b: *std.Build, options: struct {
    root: std.Build.LazyPath,
    fixture: std.Build.LazyPath,
    imports: AntflyRootImports,
    optimize: std.builtin.OptimizeMode,
}) struct { run: *std.Build.Step.Run, qualification: *std.Build.Step.Run } {
    const target = options.imports.platform_target;
    const module = b.createModule(.{
        .root_source_file = options.root.path(b, "src/pdf_ocr_integration.zig"),
        .target = target,
        .optimize = options.optimize,
    });
    module.addImport("pdf_integration_fixture", b.createModule(.{
        .root_source_file = options.fixture,
        .target = target,
        .optimize = options.optimize,
    }));
    options.imports.configure(b, module, options.imports.platform_link_libc);
    const executable = b.addExecutable(.{ .name = "pdf-ocr-integration", .root_module = module });
    const run = b.addRunArtifact(executable);
    const qualification = b.addRunArtifact(executable);
    qualification.addArg("--qualify-real");
    return .{ .run = run, .qualification = qualification };
}
