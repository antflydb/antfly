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

pub const split_bench_root = "pkg/antfly/src/split_bench_root.zig";
pub const wal_bench_root = "pkg/antfly/src/wal_bench_root.zig";
pub const derived_log_bench_root = "pkg/antfly/src/derived_log_bench_root.zig";
pub const storage_bench_root = "pkg/antfly/src/storage_bench_root.zig";
const addFilteredTestRunArtifact = @import("tests.zig").addFilteredTestRunArtifact;
const addSnowballModule = @import("snowball.zig").addSnowballModule;
const makeLmdbBuildOptions = @import("storage.zig").makeLmdbBuildOptions;
const makeLmdbEngineModule = @import("storage.zig").makeLmdbEngineModule;
const makeLmdbModule = @import("storage.zig").makeLmdbModule;
const makeRootBuildOptions = @import("storage.zig").makeRootBuildOptions;

const std = @import("std");
const AntflyRootImports = @import("imports.zig").AntflyRootImports;
const LmdbBackend = @import("storage.zig").LmdbBackend;

pub const AddBenchmarksOptions = struct {
    vopr: *std.Build.Module,
    lmdb_engine: *std.Build.Module,
    api_bench_standalone: bool,
    optimize: std.builtin.OptimizeMode,
    lmdb_backend: LmdbBackend,
    lmdb_evented_async_io: bool,
    with_tla: bool,
    antfly_imports: AntflyRootImports,
    antfly_mod: *std.Build.Module,
    antfly_test_mod: *std.Build.Module,
    run_lib_ha_compat_tests: *std.Build.Step.Run,
    compiled_recall_tests: *std.Build.Step.Compile,
};
pub const AddBenchmarksResult = struct {
    recall_ci_test_step: *std.Build.Step,
};

pub fn addBenchmarks(b: *std.Build, options: AddBenchmarksOptions) AddBenchmarksResult {
    const api_bench_standalone = options.api_bench_standalone;
    const target = options.antfly_imports.platform_target;
    const optimize = options.optimize;
    const lmdb_backend = options.lmdb_backend;
    const lmdb_evented_async_io = options.lmdb_evented_async_io;
    const with_tla = options.with_tla;
    const lmdb_engine_mod = options.lmdb_engine;
    const raft_engine_mod = options.antfly_imports.raft_engine;
    const httpx_mod = options.antfly_imports.httpx;
    const platform_mod = options.antfly_imports.platform;
    const bloom_mod = options.antfly_imports.bloom;
    const vector_mod = options.antfly_imports.vector;
    const structlog_mod = options.antfly_imports.structlog;
    const hash_mod = options.antfly_imports.hash;
    const vectorindex_mod = options.antfly_imports.vectorindex;
    const vellum_mod = options.antfly_imports.vellum;
    const antfly_imports = options.antfly_imports;
    const antfly_mod = options.antfly_mod;
    const antfly_test_mod = options.antfly_test_mod;
    const run_lib_ha_compat_tests = options.run_lib_ha_compat_tests;
    const compiled_recall_tests = options.compiled_recall_tests;
    const system_catalog_bench = b.addExecutable(.{
        .name = "antfly-system-catalog-bench",
        .root_module = b.createModule(.{
            .root_source_file = b.path("pkg/antfly/benchmarks/system_catalog.zig"),
            .target = target,
            .optimize = .ReleaseFast,
        }),
    });
    system_catalog_bench.root_module.addImport("system_catalog", b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/system_catalog/domain.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    }));
    b.step("antfly-system-catalog-bench", "Benchmark indexed catalog lookups and mutation planning").dependOn(&b.addRunArtifact(system_catalog_bench).step);
    const system_catalog_routing_bench = b.addExecutable(.{
        .name = "antfly-system-catalog-routing-bench",
        .root_module = b.createModule(.{
            .root_source_file = b.path("pkg/antfly/src/system_catalog_routing_bench.zig"),
            .target = target,
            .optimize = .ReleaseFast,
        }),
    });
    const catalog_bench_imports = @import("test_support.zig").Imports{ .runtime = antfly_imports, .vopr = options.vopr, .lmdb_engine = options.lmdb_engine };
    catalog_bench_imports.configure(b, system_catalog_routing_bench.root_module, true, true);
    b.step("antfly-system-catalog-routing-bench", "Benchmark rebuilt and retained indexed routing generations").dependOn(&b.addRunArtifact(system_catalog_routing_bench).step);
    const lmdb_bench_engine_options_c = makeLmdbBuildOptions(b, .c, false, false);
    const lmdb_bench_build_options_c = makeRootBuildOptions(b, .c, false, false, false, true, false, true);
    const lmdb_bench_engine_mod_c = makeLmdbEngineModule(b, target, optimize, true, lmdb_bench_engine_options_c);
    const lmdb_bench_wrapper_mod_c = makeLmdbModule(b, "pkg/antfly/src/storage/lmdb.zig", target, optimize, lmdb_bench_build_options_c, lmdb_bench_engine_mod_c, platform_mod, hash_mod);
    const lmstorage_bench_mod_c = b.createModule(.{
        .root_source_file = b.path("bench/storage/lmdb_bench.zig"),
        .target = target,
        .optimize = optimize,
    });
    lmstorage_bench_mod_c.addImport("lmdb", lmdb_bench_wrapper_mod_c);
    lmstorage_bench_mod_c.addImport("lmdb_engine", lmdb_bench_engine_mod_c);

    const lmdb_bench_c = b.addExecutable(.{
        .name = "lmdb_bench_c",
        .root_module = lmstorage_bench_mod_c,
    });

    const lmdb_bench_engine_options_zig = makeLmdbBuildOptions(b, .zig, lmdb_evented_async_io, false);
    const lmdb_bench_build_options_zig = makeRootBuildOptions(b, .zig, lmdb_evented_async_io, false, false, true, false, true);
    const lmdb_bench_engine_mod_zig = makeLmdbEngineModule(b, target, optimize, true, lmdb_bench_engine_options_zig);
    const lmdb_bench_wrapper_mod_zig = makeLmdbModule(b, "pkg/antfly/src/storage/lmdb.zig", target, optimize, lmdb_bench_build_options_zig, lmdb_bench_engine_mod_zig, platform_mod, hash_mod);
    const lmstorage_bench_mod_zig = b.createModule(.{
        .root_source_file = b.path("bench/storage/lmdb_bench.zig"),
        .target = target,
        .optimize = optimize,
    });
    lmstorage_bench_mod_zig.addImport("lmdb", lmdb_bench_wrapper_mod_zig);
    lmstorage_bench_mod_zig.addImport("lmdb_engine", lmdb_bench_engine_mod_zig);

    const lmdb_bench_zig = b.addExecutable(.{
        .name = "lmdb_bench_zig",
        .root_module = lmstorage_bench_mod_zig,
    });

    const lmstorage_bench_step = b.step("lmdb-bench", "Build and install both C and Zig LMDB benchmark binaries");
    lmstorage_bench_step.dependOn(&b.addInstallArtifact(lmdb_bench_c, .{}).step);
    lmstorage_bench_step.dependOn(&b.addInstallArtifact(lmdb_bench_zig, .{}).step);

    const split_bench_engine_options = makeLmdbBuildOptions(b, lmdb_backend, lmdb_evented_async_io, false);
    const split_bench_build_options = makeRootBuildOptions(b, lmdb_backend, lmdb_evented_async_io, false, false, true, false, true);
    const split_bench_engine_mod = makeLmdbEngineModule(b, target, optimize, true, split_bench_engine_options);
    const split_bench_root_mod = makeLmdbModule(b, split_bench_root, target, optimize, split_bench_build_options, split_bench_engine_mod, platform_mod, hash_mod);
    const split_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/storage/split_bench.zig"),
        .target = target,
        .optimize = optimize,
    });
    split_bench_mod.addImport("split_storage", split_bench_root_mod);

    const split_bench = b.addExecutable(.{
        .name = "split_bench",
        .root_module = split_bench_mod,
    });

    const split_bench_step = b.step("split-bench", "Build and install split_bench");
    split_bench_step.dependOn(&b.addInstallArtifact(split_bench, .{}).step);

    const db_split_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/storage/db_split_bench.zig"),
        .target = target,
        .optimize = optimize,
    });
    db_split_bench_mod.addImport("antfly-zig", antfly_mod);

    const db_split_bench = b.addExecutable(.{
        .name = "db_split_bench",
        .root_module = db_split_bench_mod,
    });

    const db_split_bench_step = b.step("db-split-bench", "Build and install db_split_bench");
    db_split_bench_step.dependOn(&b.addInstallArtifact(db_split_bench, .{}).step);

    const storage_bench_step = b.step("antfly-storage-bench", "Build and install storage benchmarks and comparisons");

    const backend_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/storage/backend_bench.zig"),
        .target = target,
        .optimize = optimize,
    });
    backend_bench_mod.addImport("antfly_zig", antfly_mod);
    const backend_bench = b.addExecutable(.{
        .name = "backend_bench",
        .root_module = backend_bench_mod,
    });

    const backend_bench_step = b.step("backend-bench", "Build and install backend_bench");
    backend_bench_step.dependOn(&b.addInstallArtifact(backend_bench, .{}).step);

    const graph_metric_prepare_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/graph/metric_preparation_bench.zig"),
        .target = target,
        .optimize = optimize,
    });
    graph_metric_prepare_bench_mod.addImport("antfly_zig", antfly_mod);
    const graph_metric_prepare_bench = b.addExecutable(.{
        .name = "graph_metric_preparation_bench",
        .root_module = graph_metric_prepare_bench_mod,
    });
    const run_graph_metric_prepare_bench = b.addRunArtifact(graph_metric_prepare_bench);
    if (b.args) |args| run_graph_metric_prepare_bench.addArgs(args);
    b.step("graph-metric-preparation-bench", "Compare unpack/hash and packed ordinal graph-metric preparation").dependOn(&run_graph_metric_prepare_bench.step);

    const graph_pattern_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/graph/pattern_query_bench.zig"),
        .target = target,
        .optimize = optimize,
    });
    graph_pattern_bench_mod.addImport("antfly_zig", antfly_mod);
    const graph_pattern_bench = b.addExecutable(.{
        .name = "graph_pattern_query_bench",
        .root_module = graph_pattern_bench_mod,
    });

    const graph_pattern_bench_step = b.step("graph-pattern-bench", "Build and install graph_pattern_query_bench");
    graph_pattern_bench_step.dependOn(&b.addInstallArtifact(graph_pattern_bench, .{}).step);

    const lsm_backend_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/storage/lsm_backend_bench.zig"),
        .target = target,
        .optimize = optimize,
    });
    lsm_backend_bench_mod.addImport("antfly_zig", antfly_mod);
    const lsm_backend_bench = b.addExecutable(.{
        .name = "lsm_backend_bench",
        .root_module = lsm_backend_bench_mod,
    });

    const lsm_backend_bench_step = b.step("lsm-backend-bench", "Build and install lsm_backend_bench");
    lsm_backend_bench_step.dependOn(&b.addInstallArtifact(lsm_backend_bench, .{}).step);

    const hbc_storage_read_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/vectors/hbc_storage_read_bench.zig"),
        .target = target,
        .optimize = optimize,
    });
    hbc_storage_read_bench_mod.addImport("antfly-zig", antfly_mod);
    const hbc_storage_read_bench = b.addExecutable(.{
        .name = "hbc_storage_read_bench",
        .root_module = hbc_storage_read_bench_mod,
    });

    const hbc_storage_read_bench_step = b.step("hbc-storage-read-bench", "Build and install hbc_storage_read_bench");
    hbc_storage_read_bench_step.dependOn(&b.addInstallArtifact(hbc_storage_read_bench, .{}).step);

    const lsm_write_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/storage/lsm_write_bench.zig"),
        .target = target,
        .optimize = optimize,
    });
    const lsm_write_bench_root_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/lsm_write_bench_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    lsm_write_bench_root_mod.addImport("bloom", bloom_mod);
    lsm_write_bench_root_mod.addImport("antfly_platform", platform_mod);
    lsm_write_bench_root_mod.addImport("antfly_hash", hash_mod);
    lsm_write_bench_mod.addImport("antfly_zig", lsm_write_bench_root_mod);
    const lsm_write_bench = b.addExecutable(.{
        .name = "lsm_write_bench",
        .root_module = lsm_write_bench_mod,
    });

    const lsm_write_bench_step = b.step("lsm-write-bench", "Build and install lsm_write_bench");
    lsm_write_bench_step.dependOn(&b.addInstallArtifact(lsm_write_bench, .{}).step);

    const lsm_write_bench_compare_mod = b.createModule(.{
        .root_source_file = b.path("bench/storage/lsm_write_bench_compare.zig"),
        .target = target,
        .optimize = optimize,
    });
    const lsm_write_bench_compare = b.addExecutable(.{
        .name = "lsm_write_bench_compare",
        .root_module = lsm_write_bench_compare_mod,
    });

    const lsm_write_bench_compare_step = b.step("lsm-write-bench-compare", "Build and install lsm_write_bench_compare");
    lsm_write_bench_compare_step.dependOn(&b.addInstallArtifact(lsm_write_bench_compare, .{}).step);

    const text_segment_write_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/full_text/text_segment_write_bench.zig"),
        .target = target,
        .optimize = optimize,
    });
    const text_segment_bench_root_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/text_segment_bench_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    text_segment_bench_root_mod.addImport("bloom", bloom_mod);
    text_segment_bench_root_mod.addImport("antfly_vellum", vellum_mod);
    text_segment_bench_root_mod.addImport("antfly_platform", platform_mod);
    text_segment_bench_root_mod.addImport("antfly_hash", hash_mod);
    text_segment_write_bench_mod.addImport("antfly_text_bench", text_segment_bench_root_mod);
    const text_segment_write_bench = b.addExecutable(.{
        .name = "text_segment_write_bench",
        .root_module = text_segment_write_bench_mod,
    });

    const text_segment_write_bench_step = b.step("text-segment-write-bench", "Build and install text_segment_write_bench");
    text_segment_write_bench_step.dependOn(&b.addInstallArtifact(text_segment_write_bench, .{}).step);

    const lsm_backend_bench_compare_mod = b.createModule(.{
        .root_source_file = b.path("bench/storage/lsm_backend_bench_compare.zig"),
        .target = target,
        .optimize = optimize,
    });
    const lsm_backend_bench_compare = b.addExecutable(.{
        .name = "lsm_backend_bench_compare",
        .root_module = lsm_backend_bench_compare_mod,
    });

    const lsm_backend_bench_compare_step = b.step("lsm-backend-bench-compare", "Build and install lsm_backend_bench_compare");
    lsm_backend_bench_compare_step.dependOn(&b.addInstallArtifact(lsm_backend_bench_compare, .{}).step);

    const wal_bench_engine_options = makeLmdbBuildOptions(b, lmdb_backend, lmdb_evented_async_io, false);
    const wal_bench_build_options = makeRootBuildOptions(b, lmdb_backend, lmdb_evented_async_io, false, false, true, false, true);
    const wal_bench_engine_mod = makeLmdbEngineModule(b, target, optimize, true, wal_bench_engine_options);
    const wal_bench_wal_mod = makeLmdbModule(b, wal_bench_root, target, optimize, wal_bench_build_options, wal_bench_engine_mod, platform_mod, hash_mod);
    const wal_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/storage/wal_bench.zig"),
        .target = target,
        .optimize = optimize,
    });
    wal_bench_mod.addImport("wal", wal_bench_wal_mod);
    wal_bench_wal_mod.addImport("bloom", bloom_mod);

    const wal_bench = b.addExecutable(.{
        .name = "wal_bench",
        .root_module = wal_bench_mod,
    });

    const benchmark_io_test_step = b.step("benchmark-io-test", "Check benchmark partial-start cleanup under Io capacity exhaustion");
    const wal_bench_io_tests = b.addTest(.{
        .root_module = wal_bench_mod,
        .filters = &.{"benchmark partial startup"},
    });
    benchmark_io_test_step.dependOn(&b.addRunArtifact(wal_bench_io_tests).step);

    const wal_bench_step = b.step("antfly-storage-wal-bench", "Build and install wal_bench");
    wal_bench_step.dependOn(&b.addInstallArtifact(wal_bench, .{}).step);

    const derived_log_bench_engine_options = makeLmdbBuildOptions(b, lmdb_backend, lmdb_evented_async_io, false);
    const derived_log_bench_build_options = makeRootBuildOptions(b, lmdb_backend, lmdb_evented_async_io, false, false, true, false, true);
    const derived_log_bench_engine_mod = makeLmdbEngineModule(b, target, optimize, true, derived_log_bench_engine_options);
    const derived_log_bench_root_mod = b.createModule(.{
        .root_source_file = b.path(derived_log_bench_root),
        .target = target,
        .optimize = optimize,
    });
    derived_log_bench_root_mod.addOptions("build_options", derived_log_bench_build_options);
    derived_log_bench_root_mod.addImport("lmdb_engine", derived_log_bench_engine_mod);
    derived_log_bench_root_mod.addImport("bloom", bloom_mod);
    derived_log_bench_root_mod.addImport("antfly_platform", platform_mod);
    derived_log_bench_root_mod.addImport("antfly_hash", hash_mod);
    derived_log_bench_root_mod.link_libc = true;
    const derived_log_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/storage/derived_log_bench.zig"),
        .target = target,
        .optimize = optimize,
    });
    derived_log_bench_mod.addImport("derived_log", derived_log_bench_root_mod);

    const derived_log_bench = b.addExecutable(.{
        .name = "derived_log_bench",
        .root_module = derived_log_bench_mod,
    });

    const derived_log_bench_io_tests = b.addTest(.{
        .root_module = derived_log_bench_mod,
        .filters = &.{"benchmark partial startup"},
    });
    benchmark_io_test_step.dependOn(&b.addRunArtifact(derived_log_bench_io_tests).step);

    const derived_log_bench_step = b.step("antfly-storage-db-derived-bench", "Build and install derived_log_bench");
    derived_log_bench_step.dependOn(&b.addInstallArtifact(derived_log_bench, .{}).step);

    // Benchmark executable
    const bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/bench.zig"),
        .target = target,
        .optimize = optimize,
    });
    bench_mod.addImport("antfly-zig", antfly_mod);

    const bench = b.addExecutable(.{
        .name = "bench",
        .root_module = bench_mod,
    });

    const bench_step = b.step("bench", "Build and install bench");
    bench_step.dependOn(&b.addInstallArtifact(bench, .{}).step);

    // Quickstart-shaped benchmark: mirrors the workload of
    // `test_text_quickstart_and_document_artifact` (e2e/antfly/test_quickstart.py)
    // so the per-iteration cost can be compared against the per-primitive
    // numbers reported by `bench`. Uses a slim root module so it only depends
    // on text/search code (and skips OpenAPI codegen).
    const quickstart_bench_root_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/quickstart_bench_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    quickstart_bench_root_mod.addImport("antfly_vellum", vellum_mod);
    quickstart_bench_root_mod.addImport("bloom", bloom_mod);
    quickstart_bench_root_mod.addImport("antfly_platform", platform_mod);
    quickstart_bench_root_mod.addImport("antfly_hash", hash_mod);
    addSnowballModule(b, quickstart_bench_root_mod);

    const quickstart_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/quickstart_bench.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    quickstart_bench_mod.addImport("antfly_quickstart_bench", quickstart_bench_root_mod);

    const quickstart_bench = b.addExecutable(.{
        .name = "quickstart_bench",
        .root_module = quickstart_bench_mod,
    });

    const quickstart_bench_step = b.step("quickstart-bench", "Build and install quickstart_bench");
    quickstart_bench_step.dependOn(&b.addInstallArtifact(quickstart_bench, .{}).step);

    const compat_mod = b.createModule(.{
        .root_source_file = b.path("bench/compat_runner.zig"),
        .target = target,
        .optimize = optimize,
    });
    compat_mod.addImport("antfly-zig", antfly_mod);

    const compat = b.addExecutable(.{
        .name = "compat_runner",
        .root_module = compat_mod,
    });

    const run_compat = b.addRunArtifact(compat);
    run_compat.addArg("compat/cases");
    const compat_step = b.step("compat", "Run the shared compatibility corpus");
    compat_step.dependOn(&run_compat.step);
    compat_step.dependOn(&run_lib_ha_compat_tests.step);

    const search_benchmark_index_mod = b.createModule(.{
        .root_source_file = b.path("bench/full_text/search_benchmark_index.zig"),
        .target = target,
        .optimize = optimize,
    });
    search_benchmark_index_mod.addImport("antfly-zig", antfly_mod);
    const search_benchmark_index = b.addExecutable(.{
        .name = "search_benchmark_index",
        .root_module = search_benchmark_index_mod,
    });
    const install_search_benchmark_index = b.addInstallArtifact(search_benchmark_index, .{});

    const search_benchmark_query_mod = b.createModule(.{
        .root_source_file = b.path("bench/full_text/search_benchmark_query.zig"),
        .target = target,
        .optimize = optimize,
    });
    search_benchmark_query_mod.addImport("antfly-zig", antfly_mod);
    const search_benchmark_query = b.addExecutable(.{
        .name = "search_benchmark_query",
        .root_module = search_benchmark_query_mod,
    });
    const install_search_benchmark_query = b.addInstallArtifact(search_benchmark_query, .{});

    const search_benchmark_common_test_mod = b.createModule(.{
        .root_source_file = b.path("bench/full_text/search_benchmark_common.zig"),
        .target = target,
        .optimize = optimize,
    });
    search_benchmark_common_test_mod.addImport("antfly-zig", antfly_mod);
    const search_benchmark_common_tests = b.addTest(.{
        .root_module = search_benchmark_common_test_mod,
    });
    const run_search_benchmark_common_tests = b.addRunArtifact(search_benchmark_common_tests);
    const search_bench_test_step = b.step("search-bench-test", "Run search benchmark grammar and protocol tests");
    search_bench_test_step.dependOn(&run_search_benchmark_common_tests.step);

    const search_performance_tests = b.addTest(.{
        .root_module = antfly_test_mod,
        .filters = &.{
            "search bool conjunction query",
            "search bool should-only query",
            "streaming boolean scorer matches all-hit reference on randomized corpus",
            "search pure bool should uses WAND top-k",
            "search term phrase uses exact positional BM25 top-k",
            "streaming phrase scorer matches randomized positional reference",
            "phrase filter exact adjacency",
            "phrase filter with slop",
            "prefix filter seeks late range in large term dictionary",
            "prefix filter uses a materialized companion with old-segment fallback",
            "exact inclusive term range preserves prefix constant scores",
            "multi_match bool_prefix preserves root semantics and bounds shingle prefixes",
            "segment term statistics stay immutable while tombstones mask hits",
            "retained snapshots share tombstones and immutable BM25 statistics",
            "concurrent searches safely observe in-place deletion publication",
            "resolved ordinal filters subtract non-visible complement without live probes",
            "db one real delete keeps filtered full text on complement path across restart",
            "PostingsIterator positional seek decodes only selected records",
            "PostingsIterator deferred positional seek decodes only accepted candidates",
            "PostingsIterator streams deferred grouped positions without scratch arrays",
            "production reader rejects branch-only v24-v37 formats",
            "v12 positions are bit-packed smaller than raw u32",
            "v12 reads back positions with wide packed deltas",
            "current one posting block retains one global impact bound",
            "v29 impact frequency escape remains a conservative upper bound",
            "v29 adaptive impact IDs use runs and round-trip",
            "v29 one-payload-block postings omit sparse impact range IDs",
            "v30 contiguous grouped positions retain direct document round-trip",
            "v31 inline single-document postings retain frequency positions and direct iteration",
            "v32 posting-count metadata derives chunk ordinal and document count",
            "v33 constant-frequency blocks omit packed frequency payload",
            "v34 five-bit impact frequencies are conservative upper bounds",
            "v35 full posting blocks use portable vertical BP128 for docs and frequencies",
            "portable vertical BP128 round-trips every bit width",
            "portable vertical BP128 fuses document delta prefix sums",
            "PostingsIterator advanceTo uses sparse skip data for long postings",
            "current reader reopens origin-main v23 postings and block-max layout",
            "index-only stored fields preserve ordinals key ranges and merges",
            "v25 field norms match Tantivy quantization",
            "BM25 term scorer retains query-invariant arithmetic",
            "BM25 bound table matches packed impact and norm domains",
            "snapshot BM25 bound table cache is reused and bounded",
            "v25 norm table uses one byte per document and reads legacy packed norms",
            "v22 term dictionary block values compact one-hit terms and delta postings offsets",
            "v23 term dictionary stores front-coded blocks indexed by block ceiling",
            "WAND pivot bound remains conservative across later high-impact blocks",
            "single-term block scan preserves a later higher-impact chunk",
            "single-term equality pruning retains earliest cutoff ties",
            "pure conjunction block pruning retains earliest cutoff ties",
            "pure conjunction metadata scan preserves later competitive block",
            "multi-segment filter execution",
            "multi-segment search merges per-segment top-k globally",
            "fragmented snapshot retains segment bound pruning",
            "bool fallback applies native doc number constraints",
            "db text kernel search matches projected search without stored bodies",
            "split preserves postings when text segments omit source bodies",
            "text score query exposes score top k sort profile",
        },
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_search_performance_tests = addFilteredTestRunArtifact(b, search_performance_tests);
    const search_performance_test_step = b.step("search-performance-test", "Run focused full-text scorer regression tests");
    search_performance_test_step.dependOn(&run_search_performance_tests.step);

    const search_benchmark_codec_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/full_text/search_benchmark_codec_bench.zig"),
        .target = target,
        .optimize = optimize,
    });
    search_benchmark_codec_bench_mod.addImport("antfly-zig", antfly_mod);
    const search_benchmark_codec_bench = b.addExecutable(.{
        .name = "search_benchmark_codec_bench",
        .root_module = search_benchmark_codec_bench_mod,
    });
    const install_search_benchmark_codec_bench = b.addInstallArtifact(search_benchmark_codec_bench, .{});

    const search_bench_codec_step = b.step("search-bench-codec-bench", "Build and install search_benchmark_codec_bench");
    search_bench_codec_step.dependOn(&b.addInstallArtifact(search_benchmark_codec_bench, .{}).step);

    const search_benchmark_bitpack_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/full_text/search_benchmark_bitpack_bench.zig"),
        .target = target,
        .optimize = optimize,
    });
    search_benchmark_bitpack_bench_mod.addImport("antfly-zig", antfly_mod);
    const search_benchmark_bitpack_bench = b.addExecutable(.{
        .name = "search_benchmark_bitpack_bench",
        .root_module = search_benchmark_bitpack_bench_mod,
    });
    const install_search_benchmark_bitpack_bench = b.addInstallArtifact(search_benchmark_bitpack_bench, .{});

    const search_bench_bitpack_step = b.step("search-bench-bitpack-bench", "Build and install search_benchmark_bitpack_bench");
    search_bench_bitpack_step.dependOn(&b.addInstallArtifact(search_benchmark_bitpack_bench, .{}).step);

    const search_impact_layout_analyze_mod = b.createModule(.{
        .root_source_file = b.path("bench/full_text/search_impact_layout_analyze.zig"),
        .target = target,
        .optimize = optimize,
    });
    search_impact_layout_analyze_mod.addImport("antfly-zig", antfly_mod);
    const search_impact_layout_analyze = b.addExecutable(.{
        .name = "search_impact_layout_analyze",
        .root_module = search_impact_layout_analyze_mod,
    });

    const search_impact_layout_analyze_step = b.step("search-impact-layout-analyze", "Build and install search_impact_layout_analyze");
    search_impact_layout_analyze_step.dependOn(&b.addInstallArtifact(search_impact_layout_analyze, .{}).step);

    const wand_skip_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/full_text/wand_skip_bench.zig"),
        .target = target,
        .optimize = optimize,
    });
    wand_skip_bench_mod.addImport("antfly-zig", antfly_mod);
    const wand_skip_bench = b.addExecutable(.{
        .name = "wand_skip_bench",
        .root_module = wand_skip_bench_mod,
    });

    const wand_skip_bench_step = b.step("wand-skip-bench", "Build and install wand_skip_bench");
    wand_skip_bench_step.dependOn(&b.addInstallArtifact(wand_skip_bench, .{}).step);

    const search_bench_build_step = b.step("search-bench", "Build search-benchmark-game antfly-zig adapter and search codec benchmark binaries");
    search_bench_build_step.dependOn(&install_search_benchmark_index.step);
    search_bench_build_step.dependOn(&install_search_benchmark_query.step);
    search_bench_build_step.dependOn(&install_search_benchmark_codec_bench.step);
    search_bench_build_step.dependOn(&install_search_benchmark_bitpack_bench.step);

    const storage_fixture_promote_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/storage_fixture_promote.zig"),
        .target = target,
        .optimize = optimize,
    });
    const storage_fixture_promote = b.addExecutable(.{
        .name = "storage_fixture_promote",
        .root_module = storage_fixture_promote_mod,
    });

    const storage_fixture_promote_step = b.step("storage-fixture-promote", "Build and install storage_fixture_promote");
    storage_fixture_promote_step.dependOn(&b.addInstallArtifact(storage_fixture_promote, .{}).step);

    const merge_cycle_mod = b.createModule(.{
        .root_source_file = b.path("bench/full_text/merge_cycle_bench.zig"),
        .target = target,
        .optimize = optimize,
    });
    merge_cycle_mod.addImport("antfly-zig", antfly_mod);

    const merge_cycle = b.addExecutable(.{
        .name = "merge_cycle_bench",
        .root_module = merge_cycle_mod,
    });

    const merge_cycle_step = b.step("merge-cycle", "Build and install merge_cycle_bench");
    merge_cycle_step.dependOn(&b.addInstallArtifact(merge_cycle, .{}).step);

    const merge_cost_mod = b.createModule(.{
        .root_source_file = b.path("bench/full_text/merge_cost_bench.zig"),
        .target = target,
        .optimize = optimize,
    });
    merge_cost_mod.addImport("antfly-zig", antfly_mod);

    const merge_cost = b.addExecutable(.{
        .name = "merge_cost_bench",
        .root_module = merge_cost_mod,
    });

    const merge_cost_step = b.step("merge-cost", "Build and install merge_cost_bench");
    merge_cost_step.dependOn(&b.addInstallArtifact(merge_cost, .{}).step);

    const hbc_parity_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/tools/hbc_parity.zig"),
        .target = target,
        .optimize = optimize,
    });
    hbc_parity_mod.addImport("antfly-zig", antfly_mod);

    const hbc_parity = b.addExecutable(.{
        .name = "hbc_parity",
        .root_module = hbc_parity_mod,
    });

    const hbc_parity_step = b.step("hbc-parity", "Build and install hbc_parity");
    hbc_parity_step.dependOn(&b.addInstallArtifact(hbc_parity, .{}).step);

    const hbc_isolate_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/tools/hbc_isolate.zig"),
        .target = target,
        .optimize = optimize,
    });
    const hbc_isolate_root_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/hbc_isolate_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    const hbc_isolate_build_options = b.addOptions();
    hbc_isolate_build_options.addOption([]const u8, "lmdb_backend", @tagName(lmdb_backend));
    hbc_isolate_build_options.addOption(bool, "lmdb_evented_async_io", lmdb_evented_async_io);
    hbc_isolate_build_options.addOption(bool, "storage_sim_soak", false);
    hbc_isolate_build_options.addOption(bool, "with_tla", with_tla);
    hbc_isolate_build_options.addOption(bool, "link_libc", true);
    hbc_isolate_build_options.addOption(bool, "standalone_runtime_focused_test", false);
    hbc_isolate_build_options.addOption(bool, "lmdb_enabled", false);
    hbc_isolate_build_options.addOption(bool, "bench_minimal_deps", true);
    hbc_isolate_root_mod.addOptions("build_options", hbc_isolate_build_options);
    hbc_isolate_root_mod.addImport("lmdb_engine", lmdb_engine_mod);
    hbc_isolate_root_mod.addImport("bloom", bloom_mod);
    hbc_isolate_root_mod.addImport("antfly_vector", vector_mod);
    hbc_isolate_root_mod.addImport("antfly_vectorindex", vectorindex_mod);
    hbc_isolate_root_mod.addImport("antfly_platform", platform_mod);
    hbc_isolate_root_mod.addImport("antfly_hash", hash_mod);
    hbc_isolate_mod.addImport("antfly_hbc_isolate_root", hbc_isolate_root_mod);

    const hbc_isolate = b.addExecutable(.{
        .name = "hbc_isolate",
        .root_module = hbc_isolate_mod,
    });

    const hbc_isolate_step = b.step("hbc-isolate", "Build and install hbc_isolate");
    hbc_isolate_step.dependOn(&b.addInstallArtifact(hbc_isolate, .{}).step);

    const dense_stack_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/vectors/dense_stack_bench.zig"),
        .target = target,
        .optimize = optimize,
    });
    dense_stack_bench_mod.addImport("antfly-zig", antfly_mod);
    const capi_bench_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/capi/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    capi_bench_mod.addImport("antfly_storage_root", antfly_mod);
    capi_bench_mod.addImport("antfly_vector", vector_mod);
    capi_bench_mod.addImport("structlog", structlog_mod);
    dense_stack_bench_mod.addImport("antfly_capi", capi_bench_mod);

    const dense_stack_bench = b.addExecutable(.{
        .name = "dense_stack_bench",
        .root_module = dense_stack_bench_mod,
    });

    const dense_stack_bench_step = b.step("dense-stack-bench", "Build and install dense_stack_bench");
    dense_stack_bench_step.dependOn(&b.addInstallArtifact(dense_stack_bench, .{}).step);

    const storage_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/storage_bench.zig"),
        .target = target,
        .optimize = optimize,
    });
    const storage_bench_root_mod = b.createModule(.{
        .root_source_file = b.path(storage_bench_root),
        .target = target,
        .optimize = optimize,
    });
    antfly_imports.configureStorageBenchmark(b, storage_bench_root_mod);
    @import("storage.zig").configureLmdb(b, storage_bench_root_mod, lmdb_engine_mod, false);
    storage_bench_mod.addImport("antfly-zig", storage_bench_root_mod);
    storage_bench_mod.addImport("antfly_platform", platform_mod);

    const storage_bench = b.addExecutable(.{
        .name = "storage_bench",
        .root_module = storage_bench_mod,
        .max_rss = 14 * 1024 * 1024 * 1024,
    });

    storage_bench_step.dependOn(&b.addInstallArtifact(storage_bench, .{}).step);

    // Keep focused DB workloads in separate compile artifacts under the same
    // owner target. Combining them with the API/HBC driver made a batch-only
    // edit rebuild the entire driver; see the build measurements in BENCHMARKS.md.
    // Share the production module configuration, including generated inputs;
    // the workloads explicitly supply their own deterministic providers.
    const db_workloads = .{
        .{ "batch_bench", "bench/storage/batch_bench.zig" },
        .{ "replay_bench", "bench/storage/replay_bench.zig" },
        .{ "open_bench", "bench/storage/open_bench.zig" },
        .{ "artifact_rebuild_bench", "bench/storage/artifact_rebuild_bench.zig" },
    };
    inline for (db_workloads) |workload| {
        const module = b.createModule(.{
            .root_source_file = b.path(workload[1]),
            .target = target,
            .optimize = optimize,
        });
        module.addImport("antfly-zig", storage_bench_root_mod);
        const artifact = b.addExecutable(.{
            .name = workload[0],
            .root_module = module,
            // Normalized standalone workloads measured up to 6 GiB; leave
            // headroom for compiler/platform variation in the shared scheduler.
            .max_rss = 8 * 1024 * 1024 * 1024,
        });
        storage_bench_step.dependOn(&b.addInstallArtifact(artifact, .{}).step);
    }

    const rw_lock_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/storage/rw_lock_bench.zig"),
        .target = target,
        .optimize = optimize,
    });
    rw_lock_bench_mod.addImport("antfly-zig", antfly_mod);

    const rw_lock_bench = b.addExecutable(.{
        .name = "rw_lock_bench",
        .root_module = rw_lock_bench_mod,
    });

    const rw_lock_bench_step = b.step("rw-lock-bench", "Build and install rw_lock_bench");
    rw_lock_bench_step.dependOn(&b.addInstallArtifact(rw_lock_bench, .{}).step);

    const provisioned_warmup_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/storage/provisioned_warmup_bench.zig"),
        .target = target,
        .optimize = optimize,
    });
    provisioned_warmup_bench_mod.addImport("antfly-zig", antfly_mod);

    const provisioned_warmup_bench = b.addExecutable(.{
        .name = "provisioned_warmup_bench",
        .root_module = provisioned_warmup_bench_mod,
    });

    const provisioned_warmup_bench_step = b.step("provisioned-warmup-bench", "Build and install provisioned_warmup_bench");
    provisioned_warmup_bench_step.dependOn(&b.addInstallArtifact(provisioned_warmup_bench, .{}).step);

    const public_query_guardrail_mod = b.createModule(.{
        .root_source_file = b.path("bench/storage/public_query_guardrail.zig"),
        .target = target,
        .optimize = optimize,
    });
    public_query_guardrail_mod.addImport("antfly-zig", antfly_mod);
    public_query_guardrail_mod.addImport("httpx", httpx_mod);
    const public_query_guardrail_build_options = b.addOptions();
    public_query_guardrail_build_options.addOption(bool, "standalone_only", false);
    public_query_guardrail_mod.addOptions("public_query_guardrail_build_options", public_query_guardrail_build_options);

    const public_query_guardrail = b.addExecutable(.{
        .name = "api_bench",
        .root_module = public_query_guardrail_mod,
    });

    const api_bench_step = b.step("antfly-api-bench", "Build and install API benchmarks");
    if (!api_bench_standalone) api_bench_step.dependOn(&b.addInstallArtifact(public_query_guardrail, .{}).step);

    // Internal stage measurements own a concrete server. The standalone driver
    // reuses the production executable and its compiled runtime kernels, so
    // it omits in-process handler code.
    const public_query_standalone_guardrail_mod = b.createModule(.{
        .root_source_file = b.path("bench/storage/public_query_guardrail.zig"),
        .target = target,
        .optimize = optimize,
    });
    public_query_standalone_guardrail_mod.addImport("antfly-zig", antfly_mod);
    public_query_standalone_guardrail_mod.addImport("httpx", httpx_mod);
    const public_query_standalone_guardrail_build_options = b.addOptions();
    public_query_standalone_guardrail_build_options.addOption(bool, "standalone_only", true);
    public_query_standalone_guardrail_mod.addOptions("public_query_guardrail_build_options", public_query_standalone_guardrail_build_options);
    const public_query_standalone_guardrail = b.addExecutable(.{
        .name = "api_standalone_bench",
        .root_module = public_query_standalone_guardrail_mod,
    });

    if (api_bench_standalone) api_bench_step.dependOn(&b.addInstallArtifact(public_query_standalone_guardrail, .{}).step);
    const raft_apply_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/storage/raft_apply_bench.zig"),
        .target = target,
        .optimize = optimize,
    });
    raft_apply_bench_mod.addImport("antfly-zig", antfly_mod);
    raft_apply_bench_mod.addImport("raft_engine", raft_engine_mod);

    const raft_apply_bench = b.addExecutable(.{
        .name = "raft_apply_bench",
        .root_module = raft_apply_bench_mod,
    });

    const raft_apply_bench_step = b.step("raft-apply-bench", "Build and install raft_apply_bench");
    raft_apply_bench_step.dependOn(&b.addInstallArtifact(raft_apply_bench, .{}).step);

    const managed_host_wal_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/storage/managed_host_wal_bench.zig"),
        .target = target,
        .optimize = optimize,
    });
    managed_host_wal_bench_mod.addImport("antfly-zig", antfly_mod);
    managed_host_wal_bench_mod.addImport("raft_engine", raft_engine_mod);

    const managed_host_wal_bench = b.addExecutable(.{
        .name = "managed_host_wal_bench",
        .root_module = managed_host_wal_bench_mod,
    });

    const managed_host_wal_bench_step = b.step("managed-host-wal-bench", "Build and install managed_host_wal_bench");
    managed_host_wal_bench_step.dependOn(&b.addInstallArtifact(managed_host_wal_bench, .{}).step);

    const dense_profile_summary = b.addExecutable(.{
        .name = "dense_profile_summary",
        .root_module = b.createModule(.{
            .root_source_file = b.path("bench/vectors/dense_profile_summary.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    const dense_profile_summary_step = b.step("dense-profile-summary", "Build and install dense_profile_summary");
    dense_profile_summary_step.dependOn(&b.addInstallArtifact(dense_profile_summary, .{}).step);

    const lmdb_commit_compare_mod = b.createModule(.{
        .root_source_file = b.path("bench/storage/lmdb_commit_compare.zig"),
        .target = target,
        .optimize = optimize,
    });
    lmdb_commit_compare_mod.addImport("antfly-zig", antfly_mod);

    const lmdb_commit_compare = b.addExecutable(.{
        .name = "lmdb_commit_compare",
        .root_module = lmdb_commit_compare_mod,
    });

    const lmdb_commit_compare_step = b.step("lmdb-commit-compare", "Build and install lmdb_commit_compare");
    lmdb_commit_compare_step.dependOn(&b.addInstallArtifact(lmdb_commit_compare, .{}).step);

    const sparse_split_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/vectors/sparse_split_bench.zig"),
        .target = target,
        .optimize = optimize,
    });
    sparse_split_bench_mod.addImport("antfly-zig", antfly_mod);

    const sparse_split_bench = b.addExecutable(.{
        .name = "sparse_split_bench",
        .root_module = sparse_split_bench_mod,
    });

    const sparse_split_bench_step = b.step("sparse-split-bench", "Build and install sparse_split_bench");
    sparse_split_bench_step.dependOn(&b.addInstallArtifact(sparse_split_bench, .{}).step);

    const rabitq_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/vectors/rabitq_bench.zig"),
        .target = target,
        .optimize = optimize,
    });
    rabitq_bench_mod.addImport("antfly-zig", antfly_mod);
    rabitq_bench_mod.addImport("antfly_vector", vector_mod);

    const rabitq_bench = b.addExecutable(.{
        .name = "rabitq_bench",
        .root_module = rabitq_bench_mod,
    });

    const rabitq_bench_step = b.step("rabitq-bench", "Build and install rabitq_bench");
    rabitq_bench_step.dependOn(&b.addInstallArtifact(rabitq_bench, .{}).step);

    const recall_harness_mod = b.createModule(.{
        .root_source_file = b.path("bench/vectors/recall_harness.zig"),
        .target = target,
        .optimize = optimize,
    });
    recall_harness_mod.addImport("antfly-zig", antfly_mod);

    const recall_harness = b.addExecutable(.{
        .name = "recall_harness",
        .root_module = recall_harness_mod,
    });

    const recall_harness_step = b.step("recall-harness", "Build and install recall_harness");
    recall_harness_step.dependOn(&b.addInstallArtifact(recall_harness, .{}).step);

    // Allow full CI to compile this ReleaseFast executable alongside the
    // release-scale regressions, then reuse the cached artifact for the recall
    // phase instead of paying its compile cost on the recall critical path.

    const run_recall_checks = b.addSystemCommand(&.{"python3"});
    run_recall_checks.setName("run storage and per-metric recall checks concurrently");
    run_recall_checks.addFileArg(b.path("tools/run_recall_checks.py"));
    run_recall_checks.addArg("--test-executable");
    run_recall_checks.addArtifactArg(compiled_recall_tests);
    run_recall_checks.addArg("--harness-executable");
    run_recall_checks.addArtifactArg(recall_harness);
    run_recall_checks.addArg("--dataset-dir");
    run_recall_checks.addDirectoryArg(b.path("testdata/vectorsets"));
    run_recall_checks.stdio = .inherit;
    run_recall_checks.step.max_rss = 12 * 1024 * 1024 * 1024;
    const recall_ci_test_step = b.step(
        "antfly-recall-test",
        "Run storage-backed and per-metric recall checks concurrently",
    );
    recall_ci_test_step.dependOn(&run_recall_checks.step);

    return .{
        .recall_ci_test_step = recall_ci_test_step,
    };
}
