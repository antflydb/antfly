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

const max_build_zig_bytes = 2 * 1024 * 1024;
const max_db_zig_bytes = 1024 * 1024;
const max_db_module_zig_bytes = 64 * 1024 * 1024;
const db_source_root = "pkg/antfly/src/storage/db";
const db_test_root_path = "pkg/antfly/src/db_test_root.zig";

pub const no_default_filters = [_][]const u8{};

pub const DBTestStep = struct {
    name: []const u8,
    description: []const u8,
    filters: []const []const u8,
    simple_runner: bool = false,
};

pub const DBRootTestStep = struct {
    tests: *std.Build.Step.Compile,
    run: *std.Build.Step.Run,
};

pub const ModuleTestRun = struct {
    tests: *std.Build.Step.Compile,
    run: *std.Build.Step.Run,
    step: *std.Build.Step,
};

pub const StandaloneModuleTestStep = struct {
    name: []const u8,
    description: []const u8,
};

pub const StorageBackendTestStep = struct {
    name: []const u8,
    description: []const u8,
    filters: ?[]const []const u8 = null,
    select_filters: bool = false,
    simple_runner: bool = false,
};

pub const StandaloneModuleTestModules = struct {
    regex: *std.Build.Module,
    jsonschema: *std.Build.Module,
    json: *std.Build.Module,
    ml_tabular: *std.Build.Module,
    fuzz_tabular_loader: *std.Build.Module,
    toon: *std.Build.Module,
    mcp: *std.Build.Module,
    a2a: *std.Build.Module,
    matcher: *std.Build.Module,
    resolver: *std.Build.Module,
    httpx_json: *std.Build.Module,
    httpx: *std.Build.Module,
    api_json_helpers: *std.Build.Module,
    generating: *std.Build.Module,
    embeddings: *std.Build.Module,
    vectorindex: *std.Build.Module,
    chunking: *std.Build.Module,
    readers: *std.Build.Module,
    extracting: *std.Build.Module,
    image: *std.Build.Module,
    reranking: *std.Build.Module,
    casbin: *std.Build.Module,
    usermgr: *std.Build.Module,
    template: *std.Build.Module,
};

pub const StandaloneModuleTestRuns = struct {
    regex: ModuleTestRun,
    jsonschema: ModuleTestRun,
    json: ModuleTestRun,
    ml_tabular: ModuleTestRun,
    fuzz_tabular_loader: ModuleTestRun,
    toon: ModuleTestRun,
    mcp: ModuleTestRun,
    a2a: ModuleTestRun,
    matcher: ModuleTestRun,
    resolver: ModuleTestRun,
    httpx_json: ModuleTestRun,
    httpx: ModuleTestRun,
    api_json_helpers: ModuleTestRun,
    generating: ModuleTestRun,
    embeddings: ModuleTestRun,
    vectorindex: ModuleTestRun,
    chunking: ModuleTestRun,
    readers: ModuleTestRun,
    extracting: ModuleTestRun,
    image: ModuleTestRun,
    reranking: ModuleTestRun,
    casbin: ModuleTestRun,
    usermgr: ModuleTestRun,
    template: ModuleTestRun,
};

pub const StorageBackendTestModules = struct {
    lmdb_engine: *std.Build.Module,
    storage_lmdb: *std.Build.Module,
    storage_lmdb_soak: *std.Build.Module,
    storage_sim_runtime: *std.Build.Module,
    docstore: *std.Build.Module,
    shard: *std.Build.Module,
    wal: *std.Build.Module,
    wal_soak: *std.Build.Module,
    persistent: *std.Build.Module,
    persistent_soak: *std.Build.Module,
    index_manager: *std.Build.Module,
    sparse: *std.Build.Module,
    derived_log: *std.Build.Module,
};

pub const StorageBackendTestDependencies = struct {
    lsm_backend_sim: *std.Build.Step.Run,
};

pub const StorageBackendTestRuns = struct {
    lmdb: ModuleTestRun,
    storage_lmdb: ModuleTestRun,
    storage_lmdb_replay: ModuleTestRun,
    storage_sim_runtime: ModuleTestRun,
    storage_lmdb_soak: ModuleTestRun,
    docstore: ModuleTestRun,
    shard: ModuleTestRun,
    wal: ModuleTestRun,
    wal_sim: ModuleTestRun,
    wal_vopr: ModuleTestRun,
    wal_replay: ModuleTestRun,
    wal_soak: ModuleTestRun,
    persistent: ModuleTestRun,
    persistent_sim: ModuleTestRun,
    persistent_replay: ModuleTestRun,
    persistent_vopr: ModuleTestRun,
    persistent_soak: ModuleTestRun,
    index_manager: ModuleTestRun,
    index_manager_resource: ModuleTestRun,
    index_manager_sim: ModuleTestRun,
    index_manager_replay: ModuleTestRun,
    index_manager_vopr: ModuleTestRun,
    sparse: ModuleTestRun,
    derived_log: ModuleTestRun,
    storage_sim: *std.Build.Step,
    storage_vopr: *std.Build.Step,
    storage_sim_soak: *std.Build.Step,
};

pub const ModuleTestOptions = struct {
    filters: ?[]const []const u8 = null,
    select_filters: bool = true,
    simple_runner: bool = false,
};

pub const DBRootModuleTestSteps = struct {
    result_shape: *std.Build.Step.Run,
};

pub const DBStorageTestSteps = struct {
    all: *std.Build.Step.Run,
    sim: *std.Build.Step.Run,
};

pub const APITableTestModules = struct {
    root: *std.Build.Module,
    transactions_docid: *std.Build.Module,
    table_writes: *std.Build.Module,
    table_reads: *std.Build.Module,
    public_table_http_docid: *std.Build.Module,
    rows: *std.Build.Module,
    internal_group_write_routes: *std.Build.Module,
    raft_transition_runtime_docid: *std.Build.Module,
};

pub const APITableTestRootOptions = struct {
    root: *std.Build.Module,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
};

pub const APITableTestRuns = struct {
    docid: *std.Build.Step.Run,
    serverless_docid: *std.Build.Step.Run,
    transactions_docid: *std.Build.Step.Run,
    table_writes: *std.Build.Step.Run,
    provisioned_query_visibility: *std.Build.Step.Run,
    table_reads: *std.Build.Step.Run,
    table_reads_graph_metric: *std.Build.Step.Run,
    public_table_http_docid: *std.Build.Step.Run,
    rows: *std.Build.Step.Run,
    sql_api_parity: *std.Build.Step.Run,
    sql_api_parity_fixture_promote: *std.Build.Step.Run,
    sql_api_parity_fixture_check: *std.Build.Step.Run,
    internal_group_write_routes: *std.Build.Step.Run,
    raft_transition_runtime_docid: *std.Build.Step.Run,
};

pub const APITableAggregateDependencies = struct {
    data_storage: *std.Build.Step.Run,
    data_runtime: *std.Build.Step.Run,
    metadata_sim_smoke: *std.Build.Step.Run,
    metadata_sim_public: *std.Build.Step.Run,
    metadata_vopr: *std.Build.Step.Run,
    metadata_vopr_chaos: *std.Build.Step.Run,
    metadata_public_chaos: *std.Build.Step,
    db_result_shape: *std.Build.Step.Run,
};

pub const APIFocusedTestRun = struct {
    tests: *std.Build.Step.Compile,
    run: *std.Build.Step.Run,
    step: ?*std.Build.Step = null,
};

pub const APIFocusedTestRuns = struct {
    public_api_parity: APIFocusedTestRun,
    public_api_graph_metric_e2e: APIFocusedTestRun,
    resolution_source: APIFocusedTestRun,
    auth: APIFocusedTestRun,
    logic: APIFocusedTestRun,
    docid_lifecycle: APIFocusedTestRun,
    swarm_backup_restore: APIFocusedTestRun,
};

pub const GraphMetricTestModules = struct {
    root: *std.Build.Module,
    query_fan_in: *std.Build.Module,
};

pub const GraphMetricTestRuns = struct {
    unit: *std.Build.Step,
    smoke: *std.Build.Step.Run,
    lifecycle: *std.Build.Step.Run,
    query_fan_in: *std.Build.Step.Run,
    operations: *std.Build.Step.Run,
    runtime_operations: *std.Build.Step.Run,
    cleanup: *std.Build.Step.Run,
    degree_canary: *std.Build.Step.Run,
    default_gate: *std.Build.Step.Run,
    integration: *std.Build.Step,
};

pub const RootTestStep = struct {
    tests: *std.Build.Step.Compile,
    run: *std.Build.Step.Run,
    step: *std.Build.Step,
};

pub const MetadataTestRun = struct {
    tests: *std.Build.Step.Compile,
    run: *std.Build.Step.Run,
    step: *std.Build.Step,
};

pub const MetadataChaosTestSteps = struct {
    transition: *std.Build.Step,
    public: *std.Build.Step,
    relational_public: *std.Build.Step,
    placement: *std.Build.Step,
    all: *std.Build.Step,
};

pub const MetadataTestSteps = struct {
    root: MetadataTestRun,
    table_workflow: MetadataTestRun,
    sim: MetadataTestRun,
    sim_core: MetadataTestRun,
    sim_smoke: MetadataTestRun,
    vopr: MetadataTestRun,
    vopr_chaos: MetadataTestRun,
    sim_public: MetadataTestRun,
    sim_forward: MetadataTestRun,
    sim_all: *std.Build.Step,
    service: MetadataTestRun,
    logic: MetadataTestRun,
    chaos: MetadataChaosTestSteps,
};

pub const StorageTestRun = struct {
    tests: *std.Build.Step.Compile,
    run: *std.Build.Step.Run,
    step: ?*std.Build.Step = null,
};

pub const StorageTestSteps = struct {
    root: StorageTestRun,
    ha: StorageTestRun,
    progress: StorageTestRun,
    lsm_backend: StorageTestRun,
    resource_budget: StorageTestRun,
};

pub const db_root_step_name = "lib-db-test";
pub const db_result_shape_step_name = "lib-db-result-shape-test";
pub const db_storage_step_name = "db-test";
pub const db_sim_step_name = "db-sim-test";

fn readBuildRootFileAlloc(b: *std.Build, path: []const u8, max_bytes: usize, context: []const u8) []const u8 {
    return b.build_root.handle.readFileAlloc(b.graph.io, path, b.allocator, .limited(max_bytes)) catch |err| {
        std.debug.panic("failed to read {s} for {s}: {}", .{ path, context, err });
    };
}

fn readBuildSourceAlloc(b: *std.Build) []const u8 {
    return readBuildRootFileAlloc(b, "build.zig", max_build_zig_bytes, "test inventory guardrail");
}

fn lineNumberForOffset(source: []const u8, offset: usize) usize {
    var line: usize = 1;
    for (source[0..@min(offset, source.len)]) |byte| {
        if (byte == '\n') line += 1;
    }
    return line;
}

fn assertBuildZigDoesNotInlineTestFilters(source: []const u8) void {
    const needle = ".filters = &." ++ "{";
    var search_index: usize = 0;
    while (std.mem.indexOfPos(u8, source, search_index, needle)) |start| {
        const list_start = start + needle.len;
        const list_end = std.mem.indexOfScalarPos(u8, source, list_start, '}') orelse
            std.debug.panic("unterminated inline test filter list in build.zig at line {}", .{lineNumberForOffset(source, start)});
        const string_count = std.mem.count(u8, source[list_start..list_end], "\"") / 2;
        if (string_count > 0) {
            std.debug.panic(
                "build.zig has inline test filters at line {}; move exact test-title lists to pkg/antfly/build/tests.zig",
                .{lineNumberForOffset(source, start)},
            );
        }
        search_index = list_end + 1;
    }
}

fn assertBuildZigTestFiltersReferenceManifest(source: []const u8) void {
    const needle = ".filters =";
    var search_index: usize = 0;
    while (std.mem.indexOfPos(u8, source, search_index, needle)) |start| {
        const assignment_end = std.mem.indexOfScalarPos(u8, source, start, '\n') orelse
            std.debug.panic("unterminated test filter assignment in build.zig at line {}", .{lineNumberForOffset(source, start)});
        if (std.mem.indexOf(u8, source[start..assignment_end], "antfly_tests_build.") == null) {
            std.debug.panic(
                "build.zig test filters at line {} must reference pkg/antfly/build/tests.zig",
                .{lineNumberForOffset(source, start)},
            );
        }
        search_index = assignment_end + 1;
    }
}

fn assertBuildZigDoesNotPassDirectTestFilterArgs(source: []const u8) void {
    const needles = .{
        "--test-filter",
        "--skip-test-filter",
    };
    inline for (needles) |needle| {
        if (std.mem.indexOf(u8, source, needle)) |start| {
            std.debug.panic(
                "build.zig passes direct test filter arg '{s}' at line {}; keep exact test-title filters in pkg/antfly/build/tests.zig",
                .{ needle, lineNumberForOffset(source, start) },
            );
        }
    }
}

fn assertBuildZigDoesNotOwnManifestTestRoots(source: []const u8) void {
    inline for (api_table_test_roots) |root| {
        if (std.mem.indexOf(u8, source, root.path)) |start| {
            std.debug.panic(
                "build.zig references manifest-owned test root '{s}' at line {}; move focused API test roots to pkg/antfly/build/tests.zig",
                .{ root.path, lineNumberForOffset(source, start) },
            );
        }
    }
}

fn assertBuildZigDoesNotOwnAPISplitImplementationInventory(source: []const u8) void {
    const api_split_implementation_paths = .{
        "pkg/antfly/src/api/table_reads/",
        "pkg/antfly/src/api/table_writes/",
    };
    inline for (api_split_implementation_paths) |path| {
        if (std.mem.indexOf(u8, source, path)) |start| {
            std.debug.panic(
                "build.zig references API split implementation path '{s}' at line {}; aggregate leaf tests through the API test roots and pkg/antfly/build/tests.zig",
                .{ path, lineNumberForOffset(source, start) },
            );
        }
    }
}

pub fn assertBuildZigDoesNotOwnTestInventory(b: *std.Build) void {
    const source = readBuildSourceAlloc(b);
    assertBuildZigDoesNotInlineTestFilters(source);
    assertBuildZigTestFiltersReferenceManifest(source);
    assertBuildZigDoesNotPassDirectTestFilterArgs(source);
    assertBuildZigDoesNotOwnManifestTestRoots(source);
    assertBuildZigDoesNotOwnAPISplitImplementationInventory(source);
}

fn assertDBRootDoesNotOwnInlineTests(source: []const u8) void {
    const starts_with_test = std.mem.startsWith(u8, source, "test \"");
    const start = if (starts_with_test) 0 else std.mem.indexOf(u8, source, "\ntest \"") orelse return;
    std.debug.panic(
        "storage/db/db.zig declares an inline test at line {}; move implementation-local tests to the owning DB module",
        .{lineNumberForOffset(source, if (starts_with_test) start else start + 1)},
    );
}

fn assertDBRootDoesNotOwnPrivateHelpers(source: []const u8) void {
    var line_start: usize = 0;
    while (line_start < source.len) {
        const line_end = std.mem.indexOfScalarPos(u8, source, line_start, '\n') orelse source.len;
        const line = source[line_start..line_end];
        const trimmed_line = std.mem.trim(u8, line, " \t");
        if (std.mem.startsWith(u8, trimmed_line, "fn ")) {
            std.debug.panic(
                "storage/db/db.zig declares private helper function at line {}; move implementation details to a coarse DB module",
                .{lineNumberForOffset(source, line_start)},
            );
        }
        line_start = if (line_end == source.len) source.len else line_end + 1;
    }
}

fn assertDBRootForwardersDoNotAccessSelfMembers(source: []const u8) void {
    if (std.mem.indexOf(u8, source, "self.")) |start| {
        std.debug.panic(
            "storage/db/db.zig accesses a DB member at line {}; public methods should forward through an owning implementation module",
            .{lineNumberForOffset(source, start)},
        );
    }
}

fn enclosingPublicFunctionStart(source: []const u8, offset: usize) ?usize {
    var search_end = @min(offset, source.len);
    while (std.mem.lastIndexOf(u8, source[0..search_end], "\n    pub fn ")) |line_start| {
        const fn_start = line_start + 1;
        if (fn_start < offset) return fn_start;
        search_end = line_start;
    }
    return null;
}

fn assertDBRootAfterGateForwardersHaveDurableGate(source: []const u8) void {
    var search_index: usize = 0;
    while (std.mem.indexOfPos(u8, source, search_index, "AfterGate(")) |after_gate_start| {
        const fn_start = enclosingPublicFunctionStart(source, after_gate_start) orelse {
            std.debug.panic(
                "storage/db/db.zig calls an AfterGate implementation helper at line {} outside a public DB forwarding method",
                .{lineNumberForOffset(source, after_gate_start)},
            );
        };
        if (std.mem.indexOf(u8, source[fn_start..after_gate_start], "enforceDurableMutationGate(self)") == null) {
            std.debug.panic(
                "storage/db/db.zig calls an AfterGate implementation helper at line {} without first enforcing the DB durable mutation gate in the same public method",
                .{lineNumberForOffset(source, after_gate_start)},
            );
        }
        search_index = after_gate_start + "AfterGate(".len;
    }
}

fn assertDBRootMetadataForwardersHaveSyncPreflight(source: []const u8) void {
    const metadata_mutation_calls = [_][]const u8{
        "schema_runtime_impl.setSchemaAfterGate(",
        "schema_runtime_impl.applyTableSchemaJsonAfterGate(",
        "schema_runtime_impl.reloadAlgebraicSchemaConfigsAfterGate(",
        "schema_runtime_impl.stageAlgebraicSchemaConfigsPending(",
        "schema_runtime_impl.applyLiteSqlTableRecordAfterGate(",
    };
    inline for (metadata_mutation_calls) |needle| {
        var search_index: usize = 0;
        while (std.mem.indexOfPos(u8, source, search_index, needle)) |call_start| {
            const fn_start = enclosingPublicFunctionStart(source, call_start) orelse {
                std.debug.panic(
                    "storage/db/db.zig calls metadata mutation helper '{s}' at line {} outside a public DB forwarding method",
                    .{ needle, lineNumberForOffset(source, call_start) },
                );
            };
            if (std.mem.indexOf(u8, source[fn_start..call_start], "preflightDBMetadataSyncCommit(self)") == null) {
                std.debug.panic(
                    "storage/db/db.zig calls metadata mutation helper '{s}' at line {} without first preflighting the HA metadata sync gate in the same public method",
                    .{ needle, lineNumberForOffset(source, call_start) },
                );
            }
            search_index = call_start + needle.len;
        }
    }
}

fn assertDBSourceDoesNotUseMixins(path: []const u8, source: []const u8) void {
    if (std.mem.indexOf(u8, source, "usingnamespace")) |start| {
        std.debug.panic(
            "storage/db/{s} uses usingnamespace at line {}; DB refactor uses explicit forwarding methods, not mixin injection",
            .{ path, lineNumberForOffset(source, start) },
        );
    }
}

fn assertDBTestRootImportsDBAggregateRoot(source: []const u8) void {
    const import_path = "storage/db/mod.zig";
    const needle = "_ = @import(\"" ++ import_path ++ "\");";
    if (std.mem.indexOf(u8, source, needle) == null) {
        std.debug.panic(
            "{s} must explicitly import {s} so DB aggregate tests stay reachable from lib-db-test/db-test",
            .{ db_test_root_path, import_path },
        );
    }
}

fn assertDBStandaloneTestFilesReachable(b: *std.Build, test_root_source: []const u8) void {
    var dir = b.build_root.handle.openDir(b.graph.io, db_source_root, .{ .iterate = true }) catch |err| {
        std.debug.panic("failed to open {s} for DB standalone test reachability guardrail: {}", .{ db_source_root, err });
    };
    defer dir.close(b.graph.io);

    var walker = dir.walk(b.allocator) catch |err| {
        std.debug.panic("failed to walk {s} for DB standalone test reachability guardrail: {}", .{ db_source_root, err });
    };
    defer walker.deinit();

    while (walker.next(b.graph.io) catch |err| {
        std.debug.panic("failed to scan {s} for DB standalone test reachability guardrail: {}", .{ db_source_root, err });
    }) |entry| {
        if (entry.kind != .file) continue;
        if (!std.mem.endsWith(u8, entry.path, "_test.zig")) continue;

        const import_path = std.fmt.allocPrint(b.allocator, "storage/db/{s}", .{entry.path}) catch |err| {
            std.debug.panic("failed to build DB standalone test import path for {s}: {}", .{ entry.path, err });
        };
        if (std.mem.indexOf(u8, test_root_source, import_path) != null) continue;
        std.debug.panic(
            "{s} must explicitly import {s} so standalone DB test files stay reachable from lib-db-test/db-test",
            .{ db_test_root_path, import_path },
        );
    }
}

fn dbSourceMayImportDBRoot(path: []const u8) bool {
    return std.mem.eql(u8, path, "db.zig") or std.mem.eql(u8, path, "mod.zig");
}

fn assertDBSourceDoesNotImportDBRoot(path: []const u8, source: []const u8) void {
    if (dbSourceMayImportDBRoot(path)) return;
    const needles = [_][]const u8{
        "@import(\"db.zig\")",
        "@import(\"../db.zig\")",
    };
    inline for (needles) |needle| {
        if (std.mem.indexOf(u8, source, needle)) |start| {
            std.debug.panic(
                "storage/db/{s} imports db.zig at line {}; implementation modules must use Impl(comptime DB: type)",
                .{ path, lineNumberForOffset(source, start) },
            );
        }
    }
}

fn assertDBSourceDoesNotInstantiateSiblingImpl(path: []const u8, source: []const u8) void {
    if (std.mem.eql(u8, path, "db.zig")) return;
    if (std.mem.indexOf(u8, source, ".Impl(")) |start| {
        std.debug.panic(
            "storage/db/{s} instantiates a sibling Impl at line {}; cross-module DB behavior must go through DB forwarding methods or shared helpers",
            .{ path, lineNumberForOffset(source, start) },
        );
    }
}

fn assertDBImplementationModuleContract(b: *std.Build) void {
    var dir = b.build_root.handle.openDir(b.graph.io, db_source_root, .{ .iterate = true }) catch |err| {
        std.debug.panic("failed to open {s} for DB implementation module guardrail: {}", .{ db_source_root, err });
    };
    defer dir.close(b.graph.io);

    var walker = dir.walk(b.allocator) catch |err| {
        std.debug.panic("failed to walk {s} for DB implementation module guardrail: {}", .{ db_source_root, err });
    };
    defer walker.deinit();

    while (walker.next(b.graph.io) catch |err| {
        std.debug.panic("failed to scan {s} for DB implementation module guardrail: {}", .{ db_source_root, err });
    }) |entry| {
        if (entry.kind != .file) continue;
        if (!std.mem.endsWith(u8, entry.path, ".zig")) continue;
        const source = dir.readFileAlloc(
            b.graph.io,
            entry.path,
            b.allocator,
            .limited(max_db_module_zig_bytes),
        ) catch |err| {
            std.debug.panic(
                "failed to read storage/db/{s} for DB implementation module guardrail: {}",
                .{ entry.path, err },
            );
        };
        assertDBSourceDoesNotUseMixins(entry.path, source);
        assertDBSourceDoesNotImportDBRoot(entry.path, source);
        assertDBSourceDoesNotInstantiateSiblingImpl(entry.path, source);
    }
}

pub fn assertDBRefactorBoundary(b: *std.Build) void {
    const source = readBuildRootFileAlloc(
        b,
        "pkg/antfly/src/storage/db/db.zig",
        max_db_zig_bytes,
        "DB refactor boundary guardrail",
    );
    const test_root_source = readBuildRootFileAlloc(
        b,
        db_test_root_path,
        max_build_zig_bytes,
        "DB test root aggregation guardrail",
    );
    assertDBRootDoesNotOwnInlineTests(source);
    assertDBRootDoesNotOwnPrivateHelpers(source);
    assertDBRootForwardersDoNotAccessSelfMembers(source);
    assertDBRootAfterGateForwardersHaveDurableGate(source);
    assertDBRootMetadataForwardersHaveSyncPreflight(source);
    assertDBTestRootImportsDBAggregateRoot(test_root_source);
    assertDBStandaloneTestFilesReachable(b, test_root_source);
    assertDBImplementationModuleContract(b);
}

pub const capi_default_filters = [_][]const u8{
    "capi lite opens exports imports checks and vacuums aflite",
    "capi zero buffer helper wipes bytes before free",
    "capi lite exposes hosted and status-only profiles",
    "capi lite open options validate and configure ttl cleanup",
    "capi execute graph queries honors identity read generation",
    "capi search rejects stale identity generation before readable lease hook",
    "capi search json returns stamped identity generation",
    "capi schema json uses table schema lifecycle",
    "packed dense response exposes public ids not doc ordinals",
    "dense response identity generation footer",
    "capi aggregate hits rejects stale identity generation before aggregation materialization",
};

pub const DBTestFilters = struct {
    // Keep DB buckets at module/category granularity. build.zig wires coarse
    // steps and aggregates only, so a DB regression should normally join an
    // owning module or stable prefix instead of adding a one-off title.
    pub const root = [_][]const u8{
        "storage.db.db.test.",
    };

    pub const enrichment = [_][]const u8{
        "storage.db.enrichment.enrichment_runtime.test.db enrichment runtime ",
        "storage.db.derived_async.test.db derived async ",
        "storage.db.split_restore_test.test.db split cutover",
        "storage.db.split_restore_test.test.db merge-style cutover",
    };

    pub const enrichment_worker = [_][]const u8{
        "storage.db.enrichment.enrichment_runtime.test.db enrichment runtime ",
    };

    pub const enrichment_replay = [_][]const u8{
        "storage.db.derived_async.test.db derived async ",
    };

    pub const enrichment_cutover = [_][]const u8{
        "storage.db.split_restore_test.test.db split cutover",
        "storage.db.split_restore_test.test.db merge-style cutover",
    };

    pub const enrichment_split_cutover = [_][]const u8{
        "storage.db.split_restore_test.test.db split cutover enrichment ",
    };

    pub const enrichment_merge_cutover = [_][]const u8{
        "storage.db.split_restore_test.test.db merge-style cutover enrichment ",
    };

    pub const enrichment_split_cutover_reopen = [_][]const u8{
        "storage.db.split_restore_test.test.db split cutover enrichment resume ",
    };

    pub const enrichment_merge_cutover_reopen = [_][]const u8{
        "storage.db.split_restore_test.test.db merge-style cutover enrichment resume ",
    };

    pub const query = [_][]const u8{
        "storage.db.db.test.db full-text",
        "storage.db.db.test.db dense ",
        "storage.db.db.test.db sparse ",
        "storage.db.db.test.db search ",
        "storage.db.db.test.db document _edges",
        "storage.db.db.test.db document _embeddings",
        "storage.db.graph_runtime.test.",
        "storage.db.search_runtime.test.db search runtime indexing ",
        "storage.db.search_runtime.test.db search runtime graph composition ",
        "storage.db.search_runtime.test.db search runtime text schema ",
        "storage.db.search_runtime.test.db search runtime identity ",
    };

    pub const result_shape = [_][]const u8{
        "db query result shape ",
    };

    pub const txn = [_][]const u8{
        "storage.db.transactions.test.",
        "storage.db.relational_integrity.test.db relational integrity transaction ",
        "storage.db.relational_integrity.test.db relational integrity constraints ",
        "storage.db.write_path.test.db write path ",
        "storage.db.maintenance.ttl_runtime.test.",
    };

    pub const sim = [_][]const u8{
        "storage.db.db_sim_test.test.",
    };

    pub const split_replay_fixtures = [_][]const u8{
        "storage.db.db_sim_test.test.db split replay ",
    };

    pub const split_restore_lifecycle = [_][]const u8{
        "storage.db.split_restore_test.test.",
    };

    pub const schema = [_][]const u8{
        "storage.db.schema_runtime.test.",
        "storage.db.relational_integrity.test.",
    };

    pub const write_path = [_][]const u8{
        "storage.db.write_path.test.",
    };

    pub const ha_replication = [_][]const u8{
        "storage.ha db ",
    };

    pub const foreign_key = [_][]const u8{
        "foreign key",
    };

    pub const temporal = [_][]const u8{
        "storage.db.relational_integrity.test.db relational temporal",
    };

    pub const relational_rows = [_][]const u8{
        "storage.db.relational_rows.test.",
    };

    pub const search_runtime = [_][]const u8{
        "storage.db.search_runtime.test.",
    };

    pub const graph_runtime = [_][]const u8{
        "storage.db.graph_runtime.test.",
    };

    pub const resolution_runtime = [_][]const u8{
        "storage.db.resolution_runtime.test.db resolution runtime ",
    };

    pub const derived_async = [_][]const u8{
        "storage.db.derived_async.test.",
    };

    pub const lifecycle = [_][]const u8{
        "storage.db.lifecycle.test.",
    };

    pub const reopen = [_][]const u8{
        "storage.db.search_runtime.test.db search runtime reopen ",
    };

    pub const ttl_runtime = [_][]const u8{
        "storage.db.maintenance.ttl_runtime.test.",
    };

    pub const enrichment_any = [_][]const u8{
        "enrichment",
    };
};

pub const APITestFilters = struct {
    pub const public_api_parity = [_][]const u8{
        "public openapi contract module is generated and wired",
        "admin openapi contract module is generated and wired",
        "internal openapi contract module is generated and wired",
        "metadata openapi module generates extractor surface for routed endpoints",
        "usermgr openapi module generates extractor surface for routed endpoints",
        "client openapi module resolves shared refs through owner modules",
        "public api routes compile",
        "mcp table tools expose catalog fields and route supported catalog lifecycle targets",
        "internal group write routes expose unique integrity",
        "internal group write routes expose foreign key action job requeue",
        "batch parser accepts Go transform op spelling",
        "public table contract exposes migration metadata",
        "table contract accepts public field scoped full text create index",
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
        "api http server serves retrieval agent response envelope",
        "api http server serves table batch writes",
        "api http server exposes relational foreign key integrity repair",
        "api http server exposes relational unique integrity repair",
        "auto bulk max-window session rolls without a following write",
        "auto bulk group writes release leases so idle finish can publish",
        "auto bulk max-window rolls publish all threshold aligned docs",
        "provisioned table write source seeds doc identity namespace from table range",
        "provisioned table write source cached runtime status does not fetch catalog coverage",
        "managed startup catch-up uses provided indexes json without catalog fetch",
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
        "api index status prefers best-effort write runtime status",
        "api index status prefers cached read runtime status before write status",
        "api index status does not fall through to write runtime status when read cache is empty",
        "api index status uses propagated remote store runtime status",
        "api index status ignores propagated runtime status from removed owner",
        "api index status reports missing remote shard as not ready",
        "single embeddings index encoder keeps backfill active while enrichment replay lags",
        "api http server serves local index runtime backfill status",
        "api http server graph metric action endpoint returns updated status",
        "api http server serves provisioned index runtime backfill status across shards",
        "api http server derives public SQL DDL command tags from parsed statements",
        "optional pgwire listener rejects host without port",
        "api http server serves database and namespace catalog routes",
        "explicit catalog routes declare qualified namespace and table permissions",
        "api http server serves table create and drop",
        "api http server serves table metadata routes against real metadata service",
        "api http server create table with replication sources returns encoded table detail",
        "api http server exposes relational foreign key integrity repair",
        "api http server exposes relational unique integrity repair",
        "api http server lists cluster backups through public route",
        "api http server backs up and restores a table through public routes",
        "api http server prefers metadata-owned restore over inline write-source restore",
        "public API request body limit matches Go linear merge contract",
        "public api smoke e2e creates table inserts and queries documents",
        "public api e2e supports graph queries",
        "public api e2e recreates managed embeddings index after corrupt artifact",
        "public api split e2e uses distributed global text stats for bm25 and significant_terms",
        "public api multi-node e2e routes CRUD from a non-host node",
    };

    pub const public_api_graph_metric_e2e = [_][]const u8{
        "public index contract exposes runtime status metadata",
        "indexes openapi parses graph metric runtime summary",
        "client openapi parses graph metric runtime summary",
        "client openapi module resolves shared refs through owner modules",
        "api http server graph metric action endpoint returns updated status",
        "public api e2e supports graph queries",
    };

    pub const resolution_source = [_][]const u8{
        "DistributedCandidateSource",
        "prefixUpperBoundAlloc",
        "DistributedEntitySink",
    };

    pub const swarm_backup_restore = [_][]const u8{
        "public api swarm-like e2e backs up drops and restores a table",
    };

    pub const auth = [_][]const u8{
        "api http server requires auth on public routes when enabled",
        "api http server dispatches HA admin and internal executors",
        "api http server protects HA admin routes while exempting HA internal routes",
        "api http server forbids non-admin secret access when auth is enabled",
        "api http server query builder requires table read permission when auth is enabled",
        "api http server restricts runtime schema debug to admins when auth is enabled",
        "api http server serves user management routes when auth is enabled",
        "api http server applies authorization SQL DDL through user manager",
        "api http server applies SQL DDL with explicit catalog session",
        "api http server persists SQL catalog session state through transaction session routes",
        "api http server enforces SQL statement timeout on session-backed DDL",
        "api http server routes prepared transaction SQL DDL to coordinator recovery",
        "api http server executes SQL notification channel plans through native runtime",
        "api http server applies SQL routine catalog plans through native runtime",
        "api http server exposes SQL routine bindings to catalog read planning",
        "api http server passes SQL routine bindings to source-backed schema DDL",
        "api http server refreshes SQL routine hooks from ready extension query functions",
        "catalog jobs schedules typed schema rewrite jobs from applied SQL DDL work",
        "catalog jobs schedules durable schema validation jobs from applied SQL DDL work",
        "api http server applies SQL ALTER COLUMN USING through durable schema rewrite jobs",
        "api http server executes SQL COPY FROM STDIN through catalog rows batch",
        "api http server maps runtime role settings into SQL catalog session defaults",
        "sql auth adapter creates roles and applies table grants through user manager",
        "sql auth adapter resolves role setting conflicts deterministically",
        "user manager applies permission change batches atomically",
        "usermgr effective runtime role settings merge direct database overrides and inherited conflicts",
        "usermgr SQL row security policy targets follow role membership durably",
        "usermgr SQL row security policy target replacement updates effective filters",
        "sql auth adapter grants directly to existing antfly users",
        "sql auth adapter applies row security policies through user manager",
        "sql auth adapter maps row security policy targets to Antfly role subjects",
        "sql row security policies are inert until row security is enabled",
        "api http server serves api key and row filter routes",
        "api http server returns json user auth errors",
        "document artifact routes declare ",
        "api http server serves mcp and a2a protocol surfaces",
        "api http server hydrates trusted principal role settings from antfly user manager",
        "api http server resolves row filter role settings for target database",
        "api http server resolves document SQL row filters through catalog target resources",
        "api http server derives public SQL DDL command tags from parsed statements",
        "optional pgwire listener rejects host without port",
        "explicit catalog routes declare qualified namespace and table permissions",
        "api http server serves ARD catalogs with public bootstrap and authenticated tenant entries",
        "api http server requires auth for ARD tenant catalog when auth is enabled",
        "api http server serves ARD OpenAPI, skill, resource, and registry endpoints",
        "api http server filters extension mcp tools by trusted principal table permissions",
        "ARD search filters scoped catalog entries",
        "ARD search requires text while explore accepts filter-only requests",
        "ARD search supports publisher and metadata filters",
        "ARD search validates federation and returns referral envelope",
        "ARD explore returns requested facet buckets over scoped entries",
        "ARD catalog entries contain required value or reference fields",
        "ARD catalog resolves artifact urls against configured base url",
        "ARD MCP descriptors resolve endpoints against configured base url",
        "ARD catalog hides admin-only built-in skills from non-admin entries",
        "ARD catalog applies declared table permissions to built-in skills",
        "ARD extension package entries use trust provenance for artifact digests",
        "ARD search supports extension metadata filters",
        "ARD profile filter keeps only profile-compatible skills",
        "auth row filter resolver expands username references",
        "auth row filter resolver expands metadata references",
        "auth row filter validator accepts username references",
        "auth row filter resolver rejects unsupported auth paths",
        "auth row filter validator rejects malformed auth node",
        "effective resolved row filter prefers table filter before wildcard",
        "auth row filter expression predicates apply to typed row query",
        "auth row filter disjuncts preserve mixed scalar and expression policy branches",
        "artifact operations apply source document row filter visibility",
    };

    pub const logic = [_][]const u8{
        // api/tables.zig: status/detail/debug encoders, parsers, schema
        // update, and query-routing logic.
        "metadata.table status encoder",
        "metadata.table detail encoder",
        "metadata.table debug encoder",
        "create table parser",
        "schema-derived algebraic indexes",
        "single schema-derived algebraic index",
        "public algebraic index definitions",
        "schema update parser",
        "validated table schema parses",
        "table schema write validation",
        "metadata.schema update",
        "metadata.query routing",
        "api query contract parses typed row claim request",
        "api query contract parses typed json filters",
        "sql adapter parsed sql exposes raw statement source spans",
        "sql adapter parsed sql owns typed statement variants",
        "sql adapter binder resolves catalog prebind table names from shared tokens",
        "sql adapter binder validates relational catalog lookups",
        "sql adapter binder resolves join projection bindings",
        "sql adapter lowering context classifies read sql into typed plan families",
        "sql adapter lowering context lowers catalog-backed equality join read plans",
        "sql adapter lowering context lowers catalog-backed bounded left join lateral read plans",
        "sql adapter diagnostics accept only stable known classification reasons",
        "sql adapter diagnostics map unsupported classifications to native requirements",
        "sql adapter corpus owns fixture family policies",
        "sql adapter corpus cte coverage tokens are exact",
        "sql adapter source corpus covers required native requirement classifications",
        "sql adapter source corpus covers resolved native requirements with positive typed plans",
        "sql adapter native requirement manifests classify every stable requirement",
        "sql adapter corpus validates resolved native requirement manifest",
        "postgres sql adapter validates app parity fixture metadata with applied schema context",
        "sql adapter ddl plan lowers create table ddl into typed schema plan",
        "sql adapter ddl plan lowers application-time temporal table constraints",
        "catalog apply creates clones and replaces public schema json",
        "catalog apply applies create table ddl plan to owned runtime schema",
        "catalog apply applies additive alter table ddl plan to runtime schema",
        "catalog apply applies create index ddl plan to runtime schema",
        "catalog apply applies updated-at trigger ddl plan to runtime schema",
        "sql adapter lower dml detects dotted path conflicts",
        "sql adapter lower dml detects json set path conflicts",
        "sql adapter lower dml routes write sql through typed plan families",
        "sql adapter lower dml rejects catalog writes to document tables",
        "sql adapter lower dml detects merge target row usage",
        "sql adapter lower dml appends joined mutation in predicates",
        "sql adapter lower dml resolves joined mutation CTE source",
        "sql adapter lower dml lowers joined mutation source with separate target and source schemas",
        "sql adapter lower dml lowers recursive cte joined mutation sources",
        "sql adapter lower dml rejects insert without primary key",
        "sql adapter lower dml lowers insert default values into defaulted row batch",
        "sql adapter lower dml lowers on conflict primary do nothing and update",
        "sql adapter lower dml lowers on conflict primary do update with excluded values",
        "sql adapter lower dml lowers on conflict unique do update",
        "sql adapter lower dml lowers on conflict arithmetic update",
        "sql adapter lower dml lowers on conflict boolean expression update",
        "sql adapter lower dml lowers on conflict date_bin expression update",
        "sql adapter lower dml lowers on conflict array transforms from excluded scalar",
        "sql adapter lower dml lowers on conflict jsonb concat update",
        "sql adapter lower dml lowers on conflict jsonb_build_object update",
        "sql adapter lower dml lowers partial unique conflict target predicates",
        "sql adapter lower dml lowers lower expression unique conflict target",
        "sql adapter lower dml lowers upper expression unique conflict target",
        "sql adapter lower dml lowers md5 expression unique conflict target",
        "sql adapter lower dml lowers mixed column expression unique conflict targets",
        "sql adapter lower dml lowers excluded explicit default values",
        "sql adapter lower dml lowers conflict update explicit default values",
        "sql adapter lower dml lowers cross-column excluded conflict values",
        "sql adapter lower dml lowers uuid generation insert values",
        "sql adapter lower dml lowers insert source write plans",
        "sql adapter lower dml lowers insert values returning into row batch",
        "sql adapter lower dml lowers insert jsonb literal",
        "sql adapter lower dml lowers jsonb_build_object insert values",
        "sql adapter lower dml lowers convert_from jsonb insert values",
        "sql adapter lower dml lowers now and current_timestamp insert values",
        "sql adapter lower dml lowers range insert values into period columns",
        "sql adapter lower dml lowers explicit default insert values",
        "sql adapter lower dml rejects default without column default",
        "sql adapter lower dml lowers explicit default update values",
        "sql adapter lower dml lowers jsonb_build_object update value",
        "sql adapter lower dml lowers convert_from jsonb update value",
        "sql adapter lower dml lowers update jsonb_set returning through row batch",
        "sql adapter lower dml lowers update jsonb concat into json set operations",
        "sql adapter lower dml evaluates concat returning expression from typed plan",
        "sql adapter lower dml evaluates now returning expression from typed plan",
        "sql adapter lower dml lowers arithmetic updates into typed increments",
        "sql adapter lower dml lowers array updates into typed transforms",
        "sql adapter lower dml lowers uuid generation update values and conflict actions",
        "sql adapter lower dml lowers now and current_timestamp update values",
        "sql adapter lower dml lowers interval arithmetic update values",
        "sql adapter lower dml applies server update policies",
        "sql adapter lower dml lowers update patch with explicit version predicate",
        "sql adapter lower dml lowers partial unique point selectors",
        "sql adapter lower dml lowers delete with explicit version predicate",
        "sql adapter lower dml lowers truncate into claimed table-emptying mutation source",
        "sql adapter lower dml lowers mutation source pagination fetch forms",
        "sql adapter lower dml lowers temporal portion mutation sources",
        "sql adapter lower dml lowers qualified generated mutation-source predicates",
        "sql adapter lower dml lowers claimed update mutation source",
        "sql adapter lower dml lowers claimed delete mutation source",
        "sql adapter lower expr lowers uuid generation projections",
        "sql adapter lower expr lowers routine expression bindings into row expressions",
        "sql adapter lower expr lowers pipe concat operator expressions",
        "sql adapter lower expr lowers concat projections",
        "sql adapter lower expr lowers boolean projection operators",
        "sql adapter lower expr lowers nullif projections",
        "sql adapter lower expr lowers numeric function projections",
        "sql adapter lower expr lowers unary minus projections",
        "sql adapter lower expr lowers current time projections",
        "sql adapter lower expr lowers arithmetic projections",
        "sql adapter lower expr lowers interval arithmetic projections",
        "sql adapter lower expr lowers case projections",
        "sql adapter lower expr lowers cast projections",
        "sql adapter lower expr ignores harmless identifier casts",
        "sql adapter lower expr lowers arithmetic predicates",
        "sql adapter lower expr lowers coalesce predicates",
        "sql adapter lower expr lowers array length predicates",
        "sql adapter lower expr lowers grouped aggregate queries",
        "sql adapter lower expr lowers select distinct to group-only aggregate",
        "sql adapter lower expr lowers filtered aggregate predicates",
        "sql adapter lower expr lowers distinct aggregate specs",
        "sql adapter lower expr lowers aggregate expression inputs",
        "sql adapter lower expr lowers bounded array aggregate specs",
        "sql adapter lower expr lowers exact percentile continuous aggregates",
        "sql adapter lower expr lowers global aggregate queries",
        "sql adapter lower expr lowers pagination limit all and fetch forms",
        "sql adapter lower expr lowers row claim query plans",
        "sql adapter lower expr treats direct graph table functions as relation sources",
        "sql adapter lower expr lowers select all with named extra projections",
        "sql adapter lower expr lowers json extraction predicates",
        "sql adapter lower expr lowers jsonb containment existence and extraction projections",
        "sql adapter lower expr accepts casted jsonb document literals",
        "sql adapter lower expr lowers qualified single table select outputs",
        "sql adapter lower expr lowers array containment and equality predicates",
        "sql adapter lower expr lowers array function projections",
        "sql adapter lower expr lowers scalar any predicates",
        "sql adapter lower expr lowers boolean is predicates",
        "sql adapter lower expr lowers bare boolean where expressions",
        "sql adapter lower expr lowers text pattern predicates",
        "sql adapter lower expr lowers scalar in predicates",
        "sql adapter lower expr lowers scalar between predicates",
        "sql adapter lower expr lowers null-safe distinct predicates",
        "sql adapter lower expr lowers scalar or predicates",
        "sql adapter lower expr lowers temporal range predicates and bound projections",
        "sql adapter lower expr lowers scalar not predicate groups",
        "sql adapter lower expr lowers null-test order expressions",
        "sql adapter lower expr lowers now in scalar predicates",
        "sql adapter lower expr lowers string_to_array projections",
        "sql adapter lower expr lowers string_to_array containment predicates",
        "sql adapter lower expr lowers string_to_array equality predicates",
        "sql adapter lower expr lowers coalesce projections",
        "sql adapter lower expr lowers scalar expression order keys",
        "sql adapter lower expr lowers row query output order ordinals",
        "sql adapter lower expr lowers row query output order aliases",
        "sql adapter lower expr lowers generated case-fold query pushdown",
        "sql adapter lower expr lowers generated concat query pushdown",
        "sql adapter lower expr lowers qualified generated read-source predicates",
        "sql adapter lower expr lowers scalar function expressions",
        "sql adapter lower expr lowers case-fold expression predicates",
        "sql adapter lower expr lowers direct select set operation query plans",
        "sql adapter lower expr reconciles set operation output shape",
        "sql adapter lower expr assembles boolean predicate groups",
        "sql adapter lower expr names every row expression kind",
        "sql adapter lower expr compares row expressions",
        "sql adapter lower expr compares aggregate specs",
        "sql adapter lower expr detects catalog expression references",
        "sql adapter lower expr compares query projection and set operation surfaces",
        "sql adapter lower expr proves simple predicate disjointness",
        "sql adapter lower expr validates catalog check expression types",
        "sql adapter lower expr validates DDL expression catalog constraints",
        "sql adapter lower expr lowers non recursive cte query plans",
        "sql adapter lower expr lowers equality join queries",
        "sql adapter lower expr lowers bounded left join lateral queries",
        "sql adapter lower expr lowers non recursive cte aggregate plans",
        "sql adapter lower expr lowers non recursive cte join and lateral plans",
        "sql adapter lower expr lowers row_number window query plans",
        "sql adapter lower expr detects deterministic row expressions",
        "sql adapter lower expr validates unique expression lists",
        "sql adapter plan clones and frees row expressions",
        "sql adapter plan frees predicate and window ownership containers",
        "sql adapter plan counts merge arm surfaces",
        "sql adapter plan clones query predicates",
        "sql adapter plan owns projection helpers",
        "sql adapter plan parses relation aliases and qualified projections",
        "sql adapter lowered read plans own nested storage plan memory",
        "sql adapter plan resolves CTE and base table sources",
        "sql adapter plan resolves join CTE sides to physical base table",
        "sql adapter plan clones query check without catalog name",
        "sql adapter lower expr lowers recursive cte stream contract",
        "document SQL lowers id lookup projection",
        "document SQL expands star projection with document virtual columns",
        "document SQL expands qualified star projection with document virtual columns",
        "document SQL rejects unsupported tail keywords as source aliases",
        "document SQL rejects unsupported compound and multi-source shapes",
        "document SQL lowers id in lookup projection",
        "document SQL rejects select projection modifiers",
        "document SQL lowers id lookup with scalar residual filter",
        "document SQL lowers id lookup null membership to empty lookup",
        "document SQL rejects id lookup with full text residual",
        "document SQL lowers qualified single table references",
        "document SQL matches single table qualifiers case insensitively",
        "document SQL rejects unknown single table qualifier",
        "document SQL lowers bounded order by over id lookup",
        "document SQL lowers ordered indexed query as bounded candidate producer",
        "document SQL lowers algebraic grouped count over indexed facts",
        "document SQL lowers qualified aggregate group by",
        "document SQL rejects aggregate projection modifiers",
        "document SQL requires algebraic materialization for catalog aggregate plan",
        "document SQL keeps filtered aggregate as native candidate producer",
        "document SQL lowers aggregate id lookup with scalar residual candidate",
        "document SQL lowers grouped aggregate id lookup with scalar residual candidate",
        "document SQL capability-aware aggregate requires full text producer",
        "document SQL capability-aware aggregate keeps full text candidate with scalar residual",
        "document SQL capability-aware aggregate keeps scalar candidate with scalar residual",
        "document SQL keeps catalog aggregate plan without algebraic materialization",
        "document SQL aggregate group by accepts index-backed virtual fields",
        "document SQL prefers algebraic materialization over bounded aggregate scan fallback",
        "document SQL lowers aggregate to policy bounded scan when no index can answer",
        "document SQL rejects algebraic group by without indexed facts",
        "document SQL lowers json path projection",
        "document SQL lowers full text producer",
        "document SQL lowers qualified full text producer",
        "document SQL rejects ranked antfly functions as scalar predicates",
        "document SQL capability-aware lowering requires full text producer",
        "document SQL capability-aware lowering keeps full text candidate with scalar residual",
        "document SQL lowers scalar equality to indexed filter producer",
        "document SQL lowers scalar inequality to native exclusion filter producer",
        "document SQL lowers scalar null predicates to native existence filters",
        "document SQL lowers null equality comparisons to match none",
        "document SQL lowers null range and pattern predicates to match none",
        "document SQL lowers like predicates to native prefix and wildcard filters",
        "document SQL rejects unsupported ilike predicates",
        "document SQL lowers json path equality to indexed filter producer",
        "document SQL lowers scalar range predicates to indexed filter producer",
        "document SQL lowers between predicates to native ranges",
        "document SQL lowers declared json path range predicates to indexed filter producer",
        "document SQL rejects untyped json subtree range predicates",
        "document SQL lowers unindexed scalar predicate to policy bounded residual scan",
        "document SQL scans scalar predicates when only generic full text index exists",
        "document SQL external row filters constrain every read producer",
        "document SQL external row filters constrain aggregate candidate producers",
        "document SQL lowers between predicate to bounded residual scan",
        "document SQL lowers scalar inequality to policy bounded residual scan",
        "document SQL lowers scalar null predicate to policy bounded residual scan",
        "document SQL lowers null equality comparisons to policy bounded residual scan",
        "document SQL lowers null range and pattern predicates to policy bounded residual scan",
        "document SQL keeps indexed scalar producer when bounded scan policy is present",
        "document SQL requires explicit limit for policy-backed indexed reads",
        "document SQL capability-aware lowering scans when scalar index capability is absent",
        "document SQL capability-aware lowering pushes only proven scalar paths",
        "document SQL filters on index-backed virtual scalar fields",
        "document SQL combines typed virtual scalar fields with independent index readiness",
        "document SQL lowers explicit array unnest over bounded scan",
        "document SQL treats field-scoped full text index as scalar-capable for that path",
        "document SQL selects compatible full text producer by query field",
        "document SQL scalar index capability still requires field-level readiness",
        "document SQL rejects scalar predicates over array fields without explicit unnest",
        "document SQL ordered scan requires explicit bounded scan policy",
        "document SQL lowers scalar in and conjunction to indexed filter producer",
        "document SQL lowers scalar in null membership with SQL semantics",
        "document SQL keeps separate scalar filter residual on full text producer",
        "document SQL requires bounded scan without id predicate",
        "source binding classifies relational document and lake schemas",
        "sql adapter lowering context derives document scalar capabilities from catalog indexes",
        "sql runtime rejects document joins with document diagnostic",
        "sql runtime non catalog document reads use conservative capabilities",
        "sql adapter ddl plan lowers create index ddl",
        "sql adapter query function dispatch uses token keyword metadata",
        "sql adapter query function lowers antfly query functions into native search requests",
        "sql adapter query function read accepts projected hit columns",
        "sql adapter query function read keeps projected columns for derived search functions",
        "sql adapter ddl plan lowers alter table ddl",
        "sql adapter ddl plan lowers alter table constraint validation ddl",
        "sql adapter ddl plan lowers additive inline foreign key column ddl",
        "sql adapter ddl plan lowers updated-at trigger ddl into typed update policy",
        "sql adapter ddl plan lowers routine expression bindings into ddl plans",
        "sql adapter ddl plan lowers catalog-only ddl plans",
        "sql adapter ddl plan lowers routine catalog ddl plans",
        "sql adapter ddl plan lowers authorization catalog ddl plans",
        "sql adapter ddl plan lowers partition and row security catalog ddl plans",
        "sql adapter ddl plan lowers namespace database and tablespace catalog ddl plans",
        "sql adapter ddl plan lowers notification and logical replication catalog ddl plans",
        "sql adapter ddl plan lowers type system catalog ddl plans",
        "sql adapter ddl plan lowers maintenance job ddl plans",
        "sql adapter ddl plan lowers bulk io ddl plans",
        "sql adapter ddl plan lowers session catalog ddl plans",
        "sql adapter ddl plan lowers transaction control and protocol ddl plans",
        "sql adapter ddl plan lowers prepared transaction ddl plans",
        "sql adapter ddl plan lowers prepared statement cursor and savepoint ddl plans",
        "sql adapter ddl plan rejects unsupported ddl shapes explicitly",
        "sql adapter ddl plan rejects mistyped expression checks during catalog validation",
        "catalog apply executes prepared transaction recovery intents",
        "sql adapter ddl plan lowers application inline foreign key ddl",
        "sql adapter ddl plan preserves named inline create table constraints",
        "sql adapter ddl plan lowers computed check constraints into native expression checks",
        "SQL adapter DDL syntax conversions map grammar enums to plan enums",
        "catalog jobs schedules typed schema rewrite jobs from applied SQL DDL work",
        "catalog jobs schedules durable schema validation jobs from applied SQL DDL work",
        "catalog jobs schedules row-plan schema rewrite jobs from applied SQL DDL work",
        "catalog jobs schedules full row schema rewrite jobs from applied SQL DDL work",
        "catalog jobs rejects schema rewrite jobs without typed row operation",
        "catalog jobs detects schema rewrite wakeable applied DDL work",
        "catalog jobs builds deterministic table emptying jobs for ranges",
        "catalog jobs schedules table emptying jobs from snapshot ranges",
        "catalog jobs admits session qualified table emptying barrier with cascade",
        "catalog jobs table emptying barrier waits for every affected table range",
        "catalog jobs table emptying barrier rejects invalid affected range job",
        "catalog jobs repairs table emptying barrier jobs after range topology changes",
        "catalog jobs promotes completed table emptying barrier by removing durable jobs",
        "catalog jobs rejects restart identity promotion without allocator reset owner",
        "catalog jobs promotes restart identity barrier through catalog source reset hook",
        "catalog jobs promotes one completed table emptying barrier per metadata mutation",
        "catalog jobs promotes completed table emptying barrier by table id",
        "catalog jobs snapshot scheduler does not require HTTP service surface",
        "sql adapter value parses timestamp literals",
        "sql adapter value parses scalar json literals",
        "sql adapter value validates json values and defaults",
        "sql adapter value parses interval literals",
        "sql adapter grammar parses row security catalog tails",
        "sql adapter grammar parses update policy trigger catalog tails",
        "sql adapter grammar parses relation population syntax",
        "sql adapter grammar parses authorization catalog tails",
        "sql adapter grammar parses logical replication catalog tails",
        "sql adapter grammar parses type system catalog tails",
        "sql adapter grammar parses relation lifetime prefixes",
        "sql adapter grammar parses routine catalog tails",
        "sql adapter grammar parses sequence catalog tails",
        "sql adapter grammar parses enum type catalog tails",
        "sql adapter grammar parses domain catalog tails",
        "sql adapter grammar parses comment metadata catalog tails",
        "sql adapter grammar parses drop table and index catalog tails",
        "sql adapter grammar parses create index headers",
        "sql adapter grammar validates identifier lists",
        "sql adapter grammar parses alter table headers",
        "sql adapter grammar parses identity allocator table headers",
        "sql adapter grammar parses create table definition headers",
        "sql adapter grammar parses table clone catalog tails",
        "sql adapter grammar parses table partition catalog tails",
        "sql adapter grammar parses view catalog tails",
        "sql adapter grammar parses truncate mutation-source syntax",
        "sql adapter bulk io lowers COPY execution routes",
        "sql adapter bulk io imports COPY rows into row batches",
        "sql adapter bulk io imports and exports COPY text csv and binary codecs",
        "sql adapter ddl fingerprint owns catalog-only ddl surfaces",
        "sql adapter ddl fingerprint owns routine catalog ddl surfaces",
        "sql adapter ddl fingerprint owns authorization catalog ddl surfaces",
        "sql adapter ddl fingerprint owns partition and row security catalog ddl surfaces",
        "sql adapter ddl fingerprint owns namespace database and tablespace catalog ddl surfaces",
        "sql adapter ddl fingerprint owns notification and logical replication ddl surfaces",
        "sql adapter ddl fingerprint owns type system ddl surfaces",
        "sql adapter ddl fingerprint owns maintenance job ddl surfaces",
        "sql adapter ddl fingerprint owns bulk io ddl surfaces",
        "sql adapter ddl fingerprint owns session catalog ddl surfaces",
        "sql adapter ddl fingerprint owns transaction protocol ddl surfaces",
        "sql adapter ddl fingerprint owns prepared transaction ddl surfaces",
        "sql adapter ddl fingerprint owns prepared statement cursor and savepoint ddl surfaces",
        "sql cursor runtime",
        "sql notification runtime",
        "sql prepared statement runtime",
        "sql routine runtime",
        "sql savepoint runtime",
        "sql session helpers",
        "api http server recovers durable SQL routine catalog",
        "api http server routes routine-backed SQL trigger DDL through routine runtime",
        "api http server keeps updated-at trigger DDL on table source path",
        "api http server applies safe before insert SQL triggers to rows batch",
        "api http server executes SQL reads through typed row plan ingress",
        "api http server executes document SQL reads through typed document plan ingress",
        "document sql filter-only index query requests include match_all base query",
        "document sql native filter rewrite only maps field identifiers",
        "document sql native filter rewrite canonicalizes row filter conjunctions",
        "document SQL bounded aggregate scan admits only lookup-backed document keys",
        "document SQL residual filter supports bool must not",
        "document SQL residual filter supports null and existence predicates",
        "document SQL residual filter supports match all and match none",
        "document SQL residual filter supports native conjuncts and disjuncts",
        "api http server executes Antfly SQL query functions through native query path",
        "api http server executes SQL point writes through typed row batch ingress",
        "api http server applies SQL row triggers to public SQL writes",
        "api http server exposes SQL routine bindings to catalog read planning",
        "api http server enforces SQL row security WITH CHECK on row writes",
        "api http server applies SQL derived index DDL to catalog index metadata",
        "api http server wakes durable schema rewrite worker after SQL ALTER rewrite DDL",
        "api http server wakes durable schema worker after SQL ALTER validation DDL",
        "api schema rewrite wake continues after unclaimed terminal progress",
        "api schema rewrite wake stops on busy-only pass",
        "api session maintenance runs schema rewrite catalog catch-up",
        "api session maintenance repairs table emptying topology jobs and wakes table",
        "api schema rewrite catalog catch-up dedupes in-flight table wakes",
        "derive initial ranges",
        "table catalog identity",
        // api/indexes.zig: index status/config encoders and aggregation.
        "index encoders expose",
        "index encoders aggregate",
        "index encoders report missing",
        "index config map encoder",
        "single index config encoder",
        "single index helpers use default",
        "index metadata helpers",
        "index status aggregation",
        "index status keeps",
        "single embeddings index encoder",
        "external embeddings index readiness",
        "embeddings index status",
        "embeddings index replay completion",
        "managed embeddings readiness prefers replay completion once docs are indexed",
        "managed embeddings readiness does not require table doc count once replay is complete",
        "managed embedder translates managed embeddings config into db generator config",
        "managed embedder translates managed embeddings config with probed dimension",
    };

    pub const docid = [_][]const u8{
        "api table reads reject stale doc identity before multigroup fanout",
        "distributed table reads reject stale doc identity before multigroup fanout",
        "api public table query rejects only top-level internal fields",
        "single embeddings index encoder scopes isolated enrichment failure to one index",
        "api query contract rejects doc identity control fields when with relaxes schema",
        "api query contract public parser rejects internal shard doc identity controls",
        "api distributed graph hydrate carries identity generation and clears cross-range ordinals",
        "distributed graph metric status merge validates metadata compatibility",
        "distributed graph rejects doc identity rebuild before cross-range fanout",
        "distributed graph rejects unstamped result refs before cross-range fanout",
        "distributed graph edge reader carries identity generation",
        "query merge preserves common identity read generation",
        "query encoder does not expose internal doc ordinals",
        "catalog doc identity readiness checks table range health",
        "catalog resolved filter validation accepts preserved split identity domains",
        "metadata merge request validation rejects incompatible doc identity namespaces",
        "metadata merge validation handles rolling mixed-version doc identity status fixtures",
        "metadata split request validation rejects stale doc identity namespace",
        "metadata reconciler does not automatically split ordinal exhausted doc identity",
        "metadata state classifies mixed-version doc identity lifecycle reports",
        "metadata state marks doc identity rebuild required on range namespace mismatch",
        "metadata http server rejects split and merge during active doc identity reassignment before source mutation",
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
        "internal group write routes map shard doc identity mismatch to conflict",
        "internal group join routes map doc identity mismatch to conflict",
        "internal group read routes map doc identity mismatch to conflict",
        "api http client preserves group doc identity conflicts",
        "aggregation context rejects non-current identity generation",
        "aggregation full-result rerun can reuse snapped result identity generation",
        "provisioned distributed aggregations collect path terms nested cardinality",
        "algebraic distributed planner selects identity-stamped derived join tensor program",
        "algebraic derived join tensor reads subtract identity tombstones at generation",
        "planner rejects rebuild-required schema lifecycle state",
        "algebraic adaptive progress marks rebuild required on schema drift",
        "remote simple vector query uses vector worker route",
        "api.table_reads.remote_wire.test.remote wire doc identity ",
        "api http server maps public query doc identity mismatch to unavailable",
        "api http server maps retrieval agent doc identity mismatch to unavailable",
        "api http server query builder maps doc identity mismatch to unavailable",
        "api http server surfaces structured doc identity conflicts for transaction commits",
        "internal group vector worker rejects unsupported identity generation",
        "internal group graph expand rejects unsupported identity generation",
        "distributed graph expand request preserves algebraic semiring planning flag",
        "batch identity metadata delete observes buffered resurrection state",
        "identity validation accepts missing canonical rows but rejects conflicts",
        "identity allocation rejects canonical row conflicts before reserving ordinal",
        "batch identity metadata fails closed at ordinal capacity",
        "identity namespace reassignment preserves snapshot generations and rejects stale writers",
        "near-u32 ordinal pressure preserves sparse high ordinal state through reassignment",
        "storage.db.lifecycle.test.db lifecycle doc identity ",
        "storage.db.write_path.test.db write path doc identity ",
        "storage.db.transactions.test.db transactions doc identity ",
        "storage.db.split_restore_test.test.db split restore doc identity ",
        "export and import preserves doc identity metadata",
        "import rejects doc identity metadata with invalid canonical ids",
        "import rejects doc identity namespace mismatch unless preserving existing namespace",
        "storage.db.search_runtime.test.db search runtime identity ",
        "storage.db.doc_filter_wire.test.doc filter wire ",
        "dense vector id ignores ordinal metadata for a different doc",
        "dense metadata prefetch includes legacy ordinal vector ids",
        "db dense artifact rebuild preserves stable vector ids distinct from ordinals",
        "db sparse index keeps physical doc nums distinct from doc identity ordinals",
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
        "segment doc ordinal sidecar roundtrip and merge preserve live order",
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
    };

    pub const transactions_docid = [_][]const u8{
        "transaction read snapshot map keys preserve embedded delimiters",
        "transaction session commit response includes retry hints for doc identity availability conflicts",
        "transaction session registry persists SQL catalog session state and savepoints restore it",
        "catalog source resolves foreign key ref owner groups",
        "catalog source resolves unique constraint owner groups",
        "catalog source promotes unique constraint with table schema compare and swap",
        "txn prepare parser round-trips constraint participant intents",
        "foreign key ref children request and response round-trip cursors",
        "foreign key action schedule ids include the mutating action",
        "distributed txn coordinator registers foreign key parent participants",
        "distributed txn coordinator externalizes deferred foreign key parent checks exactly",
        "single-table distributed txn coordinator registers foreign key parent groups",
        "distributed txn coordinator routes foreign key child writes through ref owners when configured",
        "distributed txn coordinator fails closed for transitional foreign key ref owner ranges",
        "distributed txn coordinator routes old and new foreign key refs with versioned child rows",
        "distributed txn coordinator routes unique-touching transforms with row proofs",
        "distributed txn coordinator routes unique constraint writes through owner ranges",
        "distributed txn coordinator routes unique owner handoff with row version proofs",
        "distributed txn coordinator allows non-unique transforms on multi-range unique tables",
        "distributed txn coordinator allows single-range unique writes to use local enforcement",
        "distributed txn coordinator rejects non-primary foreign key parent writes without unique owner topology",
        "distributed txn coordinator rejects partial match full composite foreign key writes before prepare",
        "distributed txn coordinator routes foreign key checks through unique owner ranges",
        "distributed txn coordinator routes cross-table foreign key checks through parent unique owner ranges",
        "distributed txn coordinator routes unique foreign key parent updates through ref owners",
        "distributed txn coordinator schedules mutating unique foreign key parent updates through ref owners",
        "distributed txn coordinator routes cross-table composite foreign key checks through parent unique owner ranges",
        "distributed txn coordinator routes cross-table composite foreign key checks through parent primary key owner ranges",
        "distributed txn coordinator routes unique foreign key parent deletes through ref owners",
        "distributed txn coordinator routes cross-table unique foreign key parent deletes through ref owners",
        "distributed txn coordinator routes unique foreign key set-null parent deletes through ref owners",
        "distributed txn coordinator routes unique foreign key cascade parent deletes through ref owners",
        "distributed txn explain routes restrict parent deletes through ref owners",
        "distributed txn explain fails closed on incomplete routed ref owner scans",
        "distributed txn coordinator routes foreign key reference transforms with final-value planning",
        "distributed txn coordinator allows non-reference transforms on foreign key tables",
        "distributed txn coordinator fails closed without foreign key ref owner parent delete ranges",
        "distributed txn coordinator routes foreign key parent deletes through ref owners when configured",
        "distributed txn coordinator routes deferred foreign key parent deletes through ref owners when configured",
        "distributed txn coordinator fails closed for transitional foreign key ref owner parent deletes",
        "distributed txn coordinator ignores unrelated foreign key child tables for parent delete planning",
        "distributed txn coordinator routes distributed foreign key set-null actions across child ranges",
        "foreign key action page executes owner ref cleanup and child mutation through routed participants",
        "foreign key action page fails closed for transitional ref owner topology",
        "foreign key action page routes update cascade child mutations with replacement parent key",
        "foreign key action page schedules recursive cascade work for deleted children",
        "foreign key action page accepts same table runtime parent identity for durable schedules",
        "distributed txn relational identity workload mixes owner topology churn and actions",
        "distributed txn coordinator routes distributed foreign key cascade actions across child ranges",
        "distributed txn coordinator rejects distributed foreign key cascade actions without ref owner topology",
    };

    const table_writes_docid_prefix = "api.table_writes.docid ";
    const table_reads_docid_prefix = "api.table_reads.docid ";
    const table_writes_query_visibility_prefix = "api.table_writes.query_visibility ";

    pub const table_writes = [_][]const u8{
        // Prefer owner-module and suite-name prefixes. Exact test names should
        // move with their owner module instead of accumulating here.
        "api.table_writes.backup_restore.test.",
        "api.table_writes.bulk_ingest.test.",
        "api.table_writes.cache.test.",
        "api.table_writes.core.test.",
        "api.table_writes.index_config.test.",
        "api.table_writes.integrity.test.",
        "api.table_writes.managed_db.test.",
        "api.table_writes.relational_mutation.test.",
        "api.table_writes.remote_wire.test.",
        "api.table_writes.schema_jobs.test.",
        "api.table_writes.sources.test.",
        table_writes_docid_prefix,
    };

    pub const provisioned_query_visibility = [_][]const u8{
        table_writes_query_visibility_prefix,
    };

    pub const table_reads = [_][]const u8{
        // Prefer owner-module and suite-name prefixes. Exact test names should
        // move with their owner module instead of accumulating here.
        "api.table_reads.cache.test.",
        "api.table_reads.core.test.",
        "api.table_reads.document_sql.test.",
        "api.table_reads.external_lake.test.",
        "api.table_reads.fanout.test.",
        "api.table_reads.graph.test.",
        "api.table_reads.relational_rows.test.",
        "api.table_reads.remote_wire.test.",
        "api.table_reads.sources.test.",
        "api.http_internal_group_read_routes.test.",
        table_reads_docid_prefix,
    };

    pub const table_reads_graph_metric = [_][]const u8{
        "hosted cross-range graph metric fan-in merges compatible hits pair",
        "hosted cross-range graph metric fan-in rejects unpublished or incompatible shard generations",
        "hosted cross-range graph metric fan-in rejects incompatible remote hits pair",
        "hosted cross-range graph metric fan-in rejects missing remote hits status",
        "hosted cross-range graph metric fan-in merges compatible published shard generations",
        "hosted cross-range graph metric fan-in merges active stale shard for published",
        "hosted cross-range graph metric fan-in merges nonuniform promotion shard layout",
        "encode query request includes graph metric read and rerank",
        "graph metric fan-in shard request carries internal status without mutating public request",
    };

    pub const public_table_http_docid = [_][]const u8{
        "public table batch handler maps doc identity unavailable errors",
        "public table query handler maps doc identity unavailable errors",
        "public table query view handler maps doc identity unavailable errors",
    };

    pub const rows = [_][]const u8{
        "relational rows unique selector",
        "relational rows conflict target upsert",
        "relational rows batch returning",
        "relational rows materializes server defaults",
        "relational rows json_set",
        "relational rows window contract",
        "relational rows join contract",
        "relational rows lateral contract",
        "relational rows cte plan contract",
        "relational rows read plan output metadata",
        "relational rows cross-table join and lateral plans execute with side schemas",
        "relational rows query contract projects coalesce",
        "relational rows query contract projects generic expression",
        "relational rows query contract parses public expression operator surface",
        "relational rows query contract projects date_trunc",
        "relational rows query contract projects string_to_array",
        "relational rows query contract supports scalar or",
        "relational rows lake bridge",
        "postgres sql adapter",
        "api http server resolves relational rows by unique selector",
        "api http server executes public relational row plan endpoints",
        "api http server exposes psql-style SQL session endpoint",
        "api http server executes PostgreSQL sequence compatibility functions through metadata source",
        "api http server applies SQL routine catalog plans through native runtime",
        "api http server exposes SQL routine bindings to catalog read planning",
        "api http server passes SQL routine bindings to source-backed schema DDL",
        "api http server refreshes SQL routine hooks from ready extension query functions",
        "api http server persists prepared transaction SQL DDL through durable session store fallback",
        "api http server routes public external lake row queries through configured resolver",
        "api http server resolves credentialed external lake rows from node config",
        "relational rows query projects typed expression outputs",
        "provisioned relational row plans fail closed when range topology moves during collection",
        "hosted table read source executes relational row query plans across local and remote owners",
        "hosted table read source coordinates relational row plans without local owner ranges",
        "hosted relational row plans fail closed when remote range topology moves during collection",
        "hosted relational lateral plans fail closed when remote range topology moves during right collection",
        "bound table read source executes SQL system-time as-of by commit sequence",
        "relational rows mutation source updates claimed base rows transactionally",
        "relational rows mutation source plans across injected owner ranges",
        "relational joined mutation source stages target-side updates from source rows",
        "relational joined mutation source plans across injected owner ranges",
        "relational joined mutation source stages target updates with separate source schema",
        "db row claim lease expiry aborts stale owner and lets next claimer proceed",
        "db row claim lease expiry lets direct mutation reclaim stale owner",
        "catalog source resolves groups by key and span",
    };

    pub const sql_api_parity = [_][]const u8{
        "postgres sql adapter classifies application parity corpus",
        "postgres sql adapter rejects data-driven application edge cases explicitly",
        "catalog apply applies incremental ddl plans to public schema json",
        "api http server executes public relational row plan endpoints",
        "api http server routes public external lake row queries through configured resolver",
        "api http server resolves credentialed external lake rows from node config",
        "relational rows joined mutation source contract parses lockable join plans",
        "relational rows joined mutation source validates target and source schemas independently",
        "relational rows cross-table join and lateral plans execute with side schemas",
        "postgres sql adapter typed read plans execute through relational storage",
        "postgres sql adapter typed write plans execute through relational storage",
    };

    pub const sql_api_parity_fixture = [_][]const u8{
        "postgres sql adapter checks application parity fixture freshness",
    };

    pub const internal_group_write_routes = [_][]const u8{
        "internal group write routes",
    };

    pub const raft_transition_runtime_docid = [_][]const u8{
        "transition runtime fails closed when doc identity reassignment callback is missing",
    };

    pub const serverless_docid = [_][]const u8{
        "serverless query module compiles",
        "search plan rejects internal doc identity controls",
        "serverless graph plans reject internal doc identity controls",
    };

    pub const docid_lifecycle = [_][]const u8{
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
        "storage.db.lifecycle.test.db lifecycle doc identity ",
        "storage.db.write_path.test.db write path doc identity ",
        "identity namespace reassignment preserves snapshot generations and rejects stale writers",
        "near-u32 ordinal pressure preserves sparse high ordinal state through reassignment",
        "index manager split handoff preserves interleaved write and query summaries",
        "storage.db.transactions.test.db transactions doc identity ",
        "storage.db.split_restore_test.test.db split restore doc identity ",
        "storage.db.search_runtime.test.db search runtime identity ",
        "doc filter wire rejects old required-field fixtures but tolerates additive fields",
        "doc filter wire rejects invalid ordinal fixtures from mixed-version senders",
    };
};

pub const RootTestFilters = struct {
    pub const skip = [_][]const u8{
        "metadata http cluster simulation",
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
        "HBC recall",
    };

    pub const unit_progress_skip = skip ++ [_][]const u8{
        "lsm backend compaction chaos campaign",
    };

    pub const fast = [_][]const u8{
        ".test_0",
        "module compiles",
        "batch parser preserves oversized value errors",
        "batch parser accepts raw payload value under public request cap",
        "linear merge request parser accepts raw payload value under public request cap",
        "query parser accepts direct graph metric reads",
        "query parser accepts graph metric rerank",
        "query parser accepts public generated match helper shape with explicit nulls",
        "query parser accepts public query string full text",
        "query encoder emits graph metric results",
        "query encoder supports count-only and profile responses",
        "query profile reports failed graph metric status across read surfaces",
        "query merge applies deterministic graph metric top-k across shards",
        "query merge rejects missing or unpublished graph metric shard results",
        "query merge rejects duplicate direct graph metric score nodes",
        "query merge rejects non-finite direct graph metric scores",
        "query merge rejects duplicate direct graph metric shard results",
        "query merge rejects mismatched direct graph metric shard identity",
        "query merge rejects inconsistent graph metric fan-in status state",
        "query merge rejects non-finite graph metric fan-in status numbers",
        "query merge rejects out-of-range graph metric fan-in progress",
        "query merge rejects incompatible graph metric fan-in metadata",
        "query merge rejects unsolicited graph score surfaces",
        "query merge rejects unsolicited graph search metric status",
        "query merge validates included graph search metric status list",
        "query merge rejects malformed graph search metric payloads",
        "query merge rejects malformed graph search traversal payloads",
        "query merge rejects malformed graph search hit payloads",
        "query merge preserves failed graph metric status across shard fan-in",
        "query merge requires comparable graph search metric generations across shards",
        "query merge allows unpublished projected graph search metric status",
        "query merge rejects ambiguous graph search fan-in metric status",
        "query merge preserves failed graph search metric status across shards",
        "query merge enforces graph search order and filter metric generations across shards",
        "query profile reports merged graph search metric generation",
        "query merge requires comparable graph metric rerank generations across shards",
        "query merge rejects malformed graph metric rerank score details",
        "query merge rejects missing or unpublished graph metric rerank shard status",
        "query encoder emits graph metric rerank score details",
        "storage.db.maintenance.graph_metric_runtime.test.db graph metric runtime background ",
        "storage.db.maintenance.graph_metric_runtime.test.db graph metric runtime planned ",
        "storage.db.maintenance.graph_metric_runtime.test.db graph metric runtime query ",
        "storage.db.maintenance.graph_metric_runtime.test.db graph metric runtime role ",
        "graph metric order and filter dependencies attach status without projection",
        "storage.db.maintenance.graph_metric_runtime.test.db graph metric runtime lease ",
        "graph metric failed planned build cleans abandoned scores and job namespace",
        "graph metric failed planned build retains bounded diagnostics",
        "graph metric repeated failed planned builds bound diagnostics and cleanup abandoned namespaces",
        "graph metric repeated failed iterative builds bound diagnostics and cleanup abandoned namespaces",
        "graph metric repeated failed hits builds bound diagnostics and cleanup abandoned namespaces",
        "provisioned read cache keeps leased entry cleanup reachable when retirement bookkeeping allocation fails",
        "write cache keeps leased entry cleanup reachable when retirement bookkeeping allocation fails",
        "provisioned table write cache retires stale db when index metadata changes",
        "primary lookup adopts seeded write cache across visible generation bump",
        "pgwire cancel registry matches backend key data",
        "pgwire parse infers text parameter oids outside literals and comments",
        "pgwire text parameters decode to typed sql values without rewriting sql",
        "pgwire binary parameters decode to typed sql values",
        "pgwire binary result encoders use postgres wire layouts",
        "pgwire relational column descriptions use postgres-compatible text types",
        "pgwire sqlstate mapping preserves postgres error classes",
        "retrieval agent treats aggregations as first-class tool capability",
        "retrieval agent requires filter and aggregate tools for filtered aggregations",
        "retrieval agent ignores empty map-valued tool fields for policy and strategy",
    };
};

pub const RecallTestFilters = struct {
    pub const hbc = [_][]const u8{
        "HBC recall",
    };
};

pub const RaftTestFilters = struct {
    pub const root = [_][]const u8{
        "raft.",
    };

    pub const transport = [_][]const u8{
        "raft.transport.",
    };

    pub const sim = [_][]const u8{
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

    pub const chaos = [_][]const u8{
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
        "cluster simulation removes queued split transition mid-flight across node restart",
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
        "cluster simulation removes queued merge transition mid-flight across node restart",
    };
};

pub const HATestFilters = struct {
    pub const chaos = [_][]const u8{
        "storage.ha chaos crash during base backup preserves slot pin and catch-up boundary",
        "storage.ha chaos crash after receive replays durable WAL before streaming resumes",
        "storage.ha chaos rejects noncontiguous records and follows timeline switch across restart",
        "storage.ha chaos rejects out-of-order WAL without poisoning receive cursor",
        "storage.ha chaos crash during apply preserves remote write and blocks remote apply",
        "storage.ha chaos crash after apply before ack reports durable progress on resume",
        "storage.ha chaos primary restart preserves synchronous acknowledgement boundaries",
        "storage.ha chaos lag retention forces reseed and former primary cannot rewind expired WAL",
        "storage.ha chaos network partition requires fence before standby promotion",
    };

    pub const compat = [_][]const u8{
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
};

const ha_storage_default_skip_filters = HATestFilters.chaos ++ HATestFilters.compat;

pub const PackageTestFilters = struct {
    pub const image_conformance = [_][]const u8{
        "conformance corpus",
    };

    pub const generating_runtime = [_][]const u8{
        "generating backend factory executes fallback chain across providers",
        "asset producer runtime",
    };

    pub const reranking_runtime = [_][]const u8{
        "reranking runtime",
    };

    pub const common = [_][]const u8{
        "provider registry",
    };

    pub const common_config = [_][]const u8{
        "common config",
    };

    pub const embedded = [_][]const u8{
        "embedded",
    };

    pub const antfly_embedded_root = [_][]const u8{
        "pkg antfly embedded root",
    };

    pub const antfly_embedded_db = [_][]const u8{
        "pkg antfly embedded db",
    };

    pub const antfly_embedded_api = [_][]const u8{
        "pkg antfly embedded api",
    };

    pub const antfly_client = [_][]const u8{
        "antfly client pkg compiles",
    };

    pub const lite_native = [_][]const u8{
        "storage.lite.",
    };

    pub const lite_cli = [_][]const u8{
        "cmd.lite",
        "cmd.cli.backup",
    };

    pub const lsm_backend_sim = [_][]const u8{
        "lsm backend simulation",
    };

    pub const lsm_backend_chaos = [_][]const u8{
        "lsm backend compaction chaos campaign",
    };

    pub const serverless = [_][]const u8{
        "serverless",
    };

    pub const lite_core_main = [_][]const u8{
        "lite core main compiles",
    };
};

pub const DataTestFilters = struct {
    pub const runtime = [_][]const u8{
        "data runtime status refresh publishes synthetic missing status for absent local group db",
        "data runtime status refresh budget reuses cached group status instead of opening db",
        "data runtime status refresh reuses managed writer snapshot instead of reopening table db",
        "data runtime keeps status refresh dirty for non-startup async index work",
        "data runtime runRound does not refresh provisioned replica root inline while worker is active",
        "data runtime data changes mark provisioned startup catch-up dirty",
        "data runtime structural changes preserve writer-published runtime status",
        "data runtime startup catch-up prefers cached admin snapshot",
        "data runtime startup catch-up clears no-debt busy writer groups",
        "data runtime provisioned root refresh spawn failure preserves retry bookkeeping",
        "data runtime background maintenance is due for dense posting cadence without lsm debt",
        "data runtime local split fallback preserves source identity namespace",
        "data runtime local merge fallback derives receiver identity namespace from catalog",
        "data runtime resolves extension package store env before local default",
        "data runtime cli accepts ARD identity flags",
        "data public API listener uses public API request body limit",
        "data server can register a store without enabling data raft",
        "data server registered data raft uses wal state backend by default",
        "data server wires configured HA executors into API server",
        "data server mirrors managed primary writes into HA replication log",
        "data server fail-closed sync policy rejects primary writes before local commit",
        "data server block sync policy waits for standby acknowledgement before commit returns",
        "data server propagates standby HA write gate into provisioned write sources",
        "storage.ha data server rejects writes and owner jobs after primary promotion fence",
        "data server applies routed HA replication records through standby write gate",
        "data server pulls and applies HA standby replication through internal HTTP client",
        "data server resumes HA standby replication from durable progress after restart",
        "data runtime records HA standby replication round failures",
        "data runtime records HA standby apply failures without stopping run round",
    };

    pub const storage = [_][]const u8{
        "db split sync coordinator allocates destination identity namespace",
        "db split status rejects stale destination identity namespace",
        "db merge coordinator opt-in applies configured receiver identity namespace",
        "db merge coordinator reapplies target namespace for persisted reassignment opt-in",
        "db merge coordinator bootstraps relational rows and column entries",
        "db merge coordinator rollback reapplies target namespace for persisted reassignment opt-in",
    };
};

pub const MetadataTestFilters = struct {
    pub const root = [_][]const u8{
        "metadata.mod.test.",
        "metadata.api.test.",
        "metadata.admin.test.",
        "metadata.http_routes.test.",
        "metadata.http_server.test.",
        "metadata.http_client.test.",
        "metadata.state.test.",
        "metadata.storage.",
        "metadata.service.test.",
        "metadata.server.test.",
        "metadata.runtime.test.",
        "metadata.reconciler.test.",
        "metadata.control_loop.test.",
        "metadata.transition_driver.test.",
        "metadata.replication_backfill.test.",
    };

    pub const foreign_key = [_][]const u8{
        "metadata raft apply store preserves projected tables and ranges across reopen",
        "metadata reconciler converges foreign key reference owner ranges",
        "metadata reconciler derives foreign key reference owner ranges from table schemas",
        "metadata reconciler derives primary key and unique owner ranges from table schemas",
        "metadata reconciler derives secondary index rebuild ranges from building relational indexes",
        "metadata reconciler preserves split foreign key reference owner ranges for active schema foreign keys",
        "metadata reconciler converges unique constraint owner ranges",
        "metadata raft apply store persists secondary index rebuild work ranges across reopen",
        "placement planner places foreign key reference owner ranges",
        "placement planner places unique constraint owner ranges",
        "placement planner places secondary index rebuild ranges",
        "table manager applies foreign key reference range lifecycle operations",
        "table manager applies secondary index rebuild lifecycle operations",
        "table manager applies unique constraint range lifecycle operations",
        "table manager owns foreign key reference owner ranges",
        "table manager owns secondary index rebuild work ranges",
        "table manager owns unique constraint owner ranges",
    };

    pub const table_workflow = [_][]const u8{
        "table workflow can reconcile foreign key reference owner ranges",
        "table workflow drives foreign key reference range lifecycle commands",
        "metadata http server accepts internal foreign key reference range lifecycle routes",
        "table workflow can drive real metadata service topology and split setup",
        "table workflow can drive placement intents through the real metadata control loop",
    };

    pub const sim = [_][]const u8{
        "metadata http cluster simulation",
    };

    pub const sim_core = [_][]const u8{
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

    pub const sim_smoke = [_][]const u8{
        "metadata sim split runtime preserves source identity namespace",
        "metadata sim merge runtime records doc identity reassignment opt-in",
        "metadata http cluster simulation drives table placement convergence",
        "metadata http cluster simulation drives split intent through the control loop",
    };

    pub const vopr = [_][]const u8{
        "metadata VOPR seeded smoke campaign",
    };

    pub const vopr_chaos = [_][]const u8{
        "metadata VOPR expanded generated workload campaign",
        "metadata VOPR relational identity owner topology campaign",
    };

    pub const transition_chaos = [_][]const u8{
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

    pub const public_chaos = [_][]const u8{
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
        "metadata http cluster simulation resolves relational unique selectors across hosted storage restart",
    };

    pub const relational_public_chaos = [_][]const u8{
        "metadata http cluster simulation resolves relational unique selectors across hosted storage restart",
    };

    pub const placement_chaos = [_][]const u8{
        "metadata http cluster simulation survives metadata leader restart during placement reconcile",
        "metadata http cluster simulation drops table topology across leader restart",
    };

    pub const sim_public = [_][]const u8{
        "metadata http cluster simulation serves public lifecycle from a non-host node after public create",
        "metadata http cluster simulation seeds default admin for auth-enabled public api",
        "metadata http cluster simulation forwards public split flow from a non-host node after public create",
        "metadata http cluster simulation forwards public merge flow from a non-host node after public create",
    };

    pub const sim_forward = [_][]const u8{
        "forwards public table io",
    };

    pub const service = [_][]const u8{
        "metadata service ",
        "metadata control loop can drive the real metadata service",
        "table workflow can drive real metadata service topology and split setup",
        "table workflow can drive placement intents through the real metadata control loop",
    };

    pub const logic = [_][]const u8{
        "metadata reconciler",
        "metadata transition state",
        "metadata server module compiles",
        "metadata catalog validation requires cross-table foreign keys to reference parent unique columns",
        "metadata catalog validation rejects relational parent table drop while referenced",
        "metadata catalog validation applies sql drop table cascade through child schema updates",
        "metadata catalog validation treats missing drop table if exists as ddl noop",
        "metadata admin source advances foreign key validation state through schema update",
        "metadata fk schema controller config builds bounded maintenance options",
        "metadata merge request validation rejects incompatible doc identity namespaces",
        "metadata split request validation rejects stale doc identity namespace",
        "metadata transition actions",
        "placement planner",
        "metadata control loop proposes desired transitions through the service seam",
        "metadata control loop plans placement intents",
        "table manager ",
        "metadata state ",
        "transition controller ",
        "metadata module compiles",
        "metadata transition driver ",
        "metadata storage module compiles",
        "table workflow can build desired topology through the control loop seam",
        "table workflow doc identity guards reject active transition intents",
        "table workflow can remove a table topology from desired state",
        "table workflow can reconcile projected local placement intents",
        "metadata raft apply store ",
        "metadata.table record decoder",
        "metadata catalog identity",
        "metadata state machine projects transitions through metadata apply store",
        "table provisioner restore rejects mismatched doc identity namespace",
    };
};

pub const StorageTestFilters = struct {
    pub const root = [_][]const u8{
        "storage.",
    };

    pub const ha = [_][]const u8{
        "storage.ha",
    };

    pub const lsm_backend = [_][]const u8{
        "storage.lsm_backend.",
    };

    pub const resource_budget = [_][]const u8{
        "resource manager observes over-budget external usage",
        "cache reports shared byte usage to resource manager",
        "derived backlog tracker accounts and releases payload bytes",
        "hbc shared cache namespaces entries",
        "hbc shared cache evicts across namespaces under one resource budget",
        "hbc cache reports byte usage to resource manager",
        "hbc cache shrinks to resource budget under pressure",
        "provisioned group storage derives all resource budgets",
    };
};

pub const ArtifactReprocessJobTestFilters = struct {
    pub const store = [_][]const u8{
        "artifact reprocess job store starts and updates a job",
        "artifact reprocess job store recovers durable jobs and reseeds ids",
        "artifact reprocess job cleanup removes recovered durable expired jobs",
    };
};

pub const SwarmRuntimeTestFilters = struct {
    pub const focused = [_][]const u8{
        "swarm runtime module compiles",
        "swarm runtime local replica reconcile permit stays blocked while startup debt is unresolved",
        "swarm runtime registers internal group routes explicitly",
        "swarm runtime registers mcp routes before antfarm catch-all",
        "parse cli accepts config path",
        "parse cli accepts secret store path",
        "parse cli accepts ARD identity flags",
        "parse cli accepts canonical host port and models dir flags",
        "parse cli accepts HA primary runtime flags",
        "parse cli accepts HA primary sync policy flags",
        "parse cli accepts HA standby runtime flags",
        "swarm HA standby replication flags require upstream and slot",
        "swarm HA string classifier distinguishes missing padded and valid values",
        "swarm HA runtime rejects ambiguous role flags",
        "antfly config uses cli override before common config",
        "swarm public api caps keep alive request reuse",
        "swarm public api body limit matches common http listener",
        "swarm public HTTP server uses public API request body limit",
        "parse cli accepts inference budget overrides",
        "inference config falls back to common config",
        "swarm runtime resolves paths from common storage base dir",
        "swarm local metadata drop table cascade removes child foreign keys",
        "swarm runtime resolves extension package store env before local default",
    };
};

pub const StorageBackendTestFilters = struct {
    pub const lmdb_replay = [_][]const u8{
        "LMDB replay fixtures stay green",
    };

    pub const lmdb_soak = [_][]const u8{
        "LMDB sim soak stays green",
    };

    pub const wal_sim = [_][]const u8{
        "wal sim",
    };

    pub const wal_vopr = [_][]const u8{
        "wal group commit uses injected virtual clock",
        "wal can reopen on modeled storage device",
        "wal modeled storage survives crash before close after acknowledged append",
        "wal modeled replay runner uses virtual storage and time",
        "wal modeled crash runner preserves acknowledged public append",
        "wal modeled VOPR campaign stays green",
        "wal modeled replay fixtures stay green",
        "wal modeled crash fixtures stay green",
        "wal modeled commit backend completion uses scheduled virtual time",
        "wal modeled storage commit delay uses injected virtual clock",
    };

    pub const wal_replay = [_][]const u8{
        "wal replay fixtures stay green",
    };

    pub const wal_soak = [_][]const u8{
        "wal sim soak stays green",
    };

    pub const persistent_sim = [_][]const u8{
        "persistent sim workloads stay green",
    };

    pub const persistent_replay = [_][]const u8{
        "persistent replay fixtures stay green",
    };

    pub const persistent_vopr = [_][]const u8{
        "persistent modeled replay fixtures stay green",
        "persistent modeled sim workload stays green",
        "persistent modeled full-text compaction publish faults stay green",
    };

    pub const persistent_soak = [_][]const u8{
        "persistent sim soak stays green",
    };

    pub const index_manager_resource = [_][]const u8{
        "text merge resource manager accounts pending bytes and active buffers",
    };

    pub const index_manager_sim = [_][]const u8{
        "index manager sim workloads stay green",
    };

    pub const index_manager_replay = [_][]const u8{
        "index manager replay fixtures stay green",
    };

    pub const index_manager_vopr = [_][]const u8{
        "index manager modeled replay fixtures stay green",
        "index manager modeled crash fixtures stay green",
    };
};

pub const GraphMetricTestFilters = struct {
    pub const unit_runtime = [_][]const u8{
        "graph metric runtime config rejects zero lease and maintenance budgets when enabled",
        "graph metric runtime config rejects worker id lists for single-owner roles",
        "graph metric runtime role gates apply without durable lease ownership",
        "graph metric runtime worker pool identity is order independent",
        "graph metric runtime boundary tick preserves worker pool operation",
    };

    pub const smoke = [_][]const u8{
        "graph degree planned build publishes scores matching local runner",
    };

    pub const lifecycle = [_][]const u8{
        "graph pagerank planned build publishes scores matching local runner",
        "graph pagerank planned build drains partitioned pages across workers",
        "graph pagerank later iteration exhausted page fails build and preserves prior generation",
        "storage.db.maintenance.graph_metric_runtime.test.db graph metric runtime planned ",
        "graph eigenvector planned build publishes scores matching local runner",
        "graph eigenvector later iteration exhausted page fails build and preserves prior generation",
        "graph eigenvector reclaimed scan page overwrites stale partial output",
        "graph eigenvector reclaimed initialize page overwrites stale rank output",
        "graph eigenvector reclaimed contribution and reduce pages overwrite stale output",
        "graph eigenvector convergence page reclaim recomputes without stale partial summary",
        "graph eigenvector failed planned build preserves prior published generation",
        "graph eigenvector coordinator publish failure preserves prior published generation after reopen",
        "graph hits planned build publishes paired scores matching local runner",
        "graph hits active planned rebuild keeps prior published pair visible",
        "graph hits reopened coordinators do not duplicate paired publish",
        "graph hits later iteration exhausted page fails pair and preserves prior published pair",
        "graph hits reclaimed scan page overwrites stale partial output",
        "graph hits reclaimed initialize page overwrites stale rank output",
        "graph hits reclaimed contribution and reduce pages overwrite stale output",
        "graph hits convergence page reclaim recomputes without stale partial summary",
        "graph hits failed planned build preserves prior published pair",
        "graph hits coordinator publish failure preserves prior published pair after reopen",
        "graph hits planned build drains partitioned paired pages across workers",
        "graph hits larger manifest resumes across reopen boundaries",
    };

    pub const query_fan_in = [_][]const u8{
        "query profile reports failed graph metric status across read surfaces",
        "query merge applies deterministic graph metric top-k across shards",
        "query merge rejects missing or unpublished graph metric shard results",
        "query merge rejects duplicate direct graph metric score nodes",
        "query merge rejects non-finite direct graph metric scores",
        "query merge rejects duplicate direct graph metric shard results",
        "query merge rejects mismatched direct graph metric shard identity",
        "query merge rejects inconsistent graph metric fan-in status state",
        "query merge rejects non-finite graph metric fan-in status numbers",
        "query merge rejects out-of-range graph metric fan-in progress",
        "query merge rejects incompatible graph metric fan-in metadata",
        "query merge rejects unsolicited graph search metric status",
        "query merge validates included graph search metric status list",
        "query merge rejects malformed graph search metric payloads",
        "query merge rejects malformed graph search traversal payloads",
        "query merge rejects malformed graph search hit payloads",
        "query merge preserves failed graph metric status across shard fan-in",
        "query merge requires comparable graph search metric generations across shards",
        "query merge allows unpublished projected graph search metric status",
        "query merge rejects ambiguous graph search fan-in metric status",
        "query merge preserves failed graph search metric status across shards",
        "query merge enforces graph search order and filter metric generations across shards",
        "query profile reports merged graph search metric generation",
        "query merge requires comparable graph metric rerank generations across shards",
        "query merge rejects malformed graph metric rerank score details",
        "query merge rejects missing or unpublished graph metric rerank shard status",
        "query encoder emits graph metric rerank score details",
    };

    pub const operations = [_][]const u8{
        "graph metric status summarizes multiple active build pages with cap",
        "table runtime snapshot cache preserves graph metric runtime ownership telemetry",
        "index encoders expose graph metric runtime ownership summary",
        "index encoders expose mixed graph metric runtime roles without aggregate role",
        "graph metric status encoder exposes active build pages",
        "distributed graph expand request defers worker result limit for metric post processing",
        "indexes openapi parses graph metric runtime summary",
        "client openapi parses graph metric runtime summary",
        "internal group write routes expose graph metric maintenance boundary",
        "internal group write routes graph metric maintenance fences service runtime owners",
        "internal group write routes graph metric maintenance releases only current service owner",
    };

    pub const runtime_operations = [_][]const u8{
        "storage.db.maintenance.graph_metric_runtime.test.db graph metric runtime background ",
        "storage.db.maintenance.graph_metric_runtime.test.db graph metric runtime role ",
        "storage.db.maintenance.graph_metric_runtime.test.db graph metric runtime operations ",
    };

    pub const cleanup = [_][]const u8{
        "storage.db.maintenance.graph_metric_runtime.test.db graph metric runtime lease ",
        "graph pagerank planned cleanup resumes after non-final cleanup page reopen",
        "graph pagerank cleanup page resumes from durable cursor after reopen",
        "graph eigenvector cleanup page resumes after reopen with published scores visible",
        "graph hits cleanup page resumes after reopen with published pair visible",
        "graph metric failed planned build cleans abandoned scores and job namespace",
        "graph metric failed planned build retains bounded diagnostics",
        "graph metric repeated failed planned builds bound diagnostics and cleanup abandoned namespaces",
        "graph metric repeated failed iterative builds bound diagnostics and cleanup abandoned namespaces",
        "graph metric repeated failed hits builds bound diagnostics and cleanup abandoned namespaces",
        "graph metric build job cleanup refuses active job namespace",
    };

    pub const degree_canary = [_][]const u8{
        "storage.db.maintenance.graph_metric_runtime.test.db graph metric runtime degree canary ",
    };

    pub const default_gate = [_][]const u8{
        "storage.db.maintenance.graph_metric_runtime.test.db graph metric runtime default gate ",
    };
};

pub const GraphMetricCommandTestFilters = struct {
    pub const operations = [_][]const u8{
        "graph metric maintenance command parses service target config",
        "graph metric maintenance service request stays owner and budget scoped",
        "graph metric maintenance service runner aggregates remote ticks",
        "graph metric maintenance service boundary preserves worker pool owner request",
        "graph metric maintenance supervisor parses config and defaults workers",
        "graph metric maintenance supervisor parses service target config",
        "graph metric maintenance supervisor builds coordinator and worker pool argv",
        "graph metric maintenance supervisor builds service child argv without local writer guard",
        "graph metric maintenance launched child argv stays owner and budget scoped",
        "graph metric maintenance supervisor restart policy is bounded",
        "graph metric maintenance supervisor parses child runtime telemetry",
        "graph metric maintenance command exits after configured idle streak",
        "graph metric maintenance command summary exposes ownership telemetry",
    };
};

// DB focused targets are registered from these private inventories so build.zig
// depends on aggregate handles instead of growing a field per regression.
const db_root_module_steps = [_]DBTestStep{
    .{
        .name = "lib-db-enrichment-test",
        .description = "Run root-module DB enrichment/replay/cutover tests",
        .filters = &DBTestFilters.enrichment,
    },
    .{
        .name = "lib-db-enrichment-worker-test",
        .description = "Run root-module DB enrichment worker tests",
        .filters = &DBTestFilters.enrichment_worker,
    },
    .{
        .name = "lib-db-enrichment-replay-test",
        .description = "Run root-module DB enrichment replay tests",
        .filters = &DBTestFilters.enrichment_replay,
    },
    .{
        .name = "lib-db-enrichment-cutover-test",
        .description = "Run root-module DB enrichment cutover tests",
        .filters = &DBTestFilters.enrichment_cutover,
    },
    .{
        .name = "lib-db-enrichment-split-cutover-test",
        .description = "Run root-module DB enrichment split cutover tests",
        .filters = &DBTestFilters.enrichment_split_cutover,
    },
    .{
        .name = "lib-db-enrichment-merge-cutover-test",
        .description = "Run root-module DB enrichment merge cutover tests",
        .filters = &DBTestFilters.enrichment_merge_cutover,
    },
    .{
        .name = "lib-db-enrichment-split-cutover-reopen-test",
        .description = "Run root-module DB split cutover reopen test",
        .filters = &DBTestFilters.enrichment_split_cutover_reopen,
    },
    .{
        .name = "lib-db-enrichment-merge-cutover-reopen-test",
        .description = "Run root-module DB merge cutover reopen test",
        .filters = &DBTestFilters.enrichment_merge_cutover_reopen,
    },
    .{
        .name = "lib-db-query-test",
        .description = "Run root-module DB query/indexing tests",
        .filters = &DBTestFilters.query,
    },
    .{
        .name = db_result_shape_step_name,
        .description = "Run focused DB query doc id boundary tests",
        .filters = &DBTestFilters.result_shape,
        .simple_runner = true,
    },
    .{
        .name = "lib-db-txn-test",
        .description = "Run focused DB write/TTL/transaction tests",
        .filters = &DBTestFilters.txn,
    },
};

const db_storage_module_steps = [_]DBTestStep{
    .{
        .name = db_sim_step_name,
        .description = "Run DB simulation and replay tests",
        .filters = &DBTestFilters.sim,
    },
    .{
        .name = "db-split-replay-fixtures",
        .description = "Run DB split replay fixture tests",
        .filters = &DBTestFilters.split_replay_fixtures,
    },
    .{
        .name = "db-split-restore-lifecycle-test",
        .description = "Run DB split/restore lifecycle regression tests",
        .filters = &DBTestFilters.split_restore_lifecycle,
    },
    .{
        .name = "db-schema-test",
        .description = "Run focused storage/db schema validation tests",
        .filters = &DBTestFilters.schema,
    },
    .{
        .name = "db-write-path-test",
        .description = "Run focused storage/db write-path tests",
        .filters = &DBTestFilters.write_path,
    },
    .{
        .name = "db-ha-replication-test",
        .description = "Run focused storage/db HA replication tests",
        .filters = &DBTestFilters.ha_replication,
    },
    .{
        .name = "db-foreign-key-test",
        .description = "Run focused storage/db foreign-key unit tests",
        .filters = &DBTestFilters.foreign_key,
    },
    .{
        .name = "db-temporal-test",
        .description = "Run focused storage/db application-time temporal unit tests",
        .filters = &DBTestFilters.temporal,
    },
    .{
        .name = "db-relational-rows-test",
        .description = "Run focused storage/db relational row tests",
        .filters = &DBTestFilters.relational_rows,
    },
    .{
        .name = "db-search-runtime-test",
        .description = "Run focused storage/db search runtime tests",
        .filters = &DBTestFilters.search_runtime,
    },
    .{
        .name = "db-graph-runtime-test",
        .description = "Run focused storage/db graph runtime tests",
        .filters = &DBTestFilters.graph_runtime,
    },
    .{
        .name = "db-resolution-runtime-test",
        .description = "Run focused storage/db resolution workflow tests",
        .filters = &DBTestFilters.resolution_runtime,
    },
    .{
        .name = "db-derived-async-test",
        .description = "Run focused storage/db derived async tests",
        .filters = &DBTestFilters.derived_async,
    },
    .{
        .name = "db-lifecycle-test",
        .description = "Run focused storage/db lifecycle tests",
        .filters = &DBTestFilters.lifecycle,
    },
    .{
        .name = "lib-db-reopen-test",
        .description = "Run root-module DB reopen/compaction tests",
        .filters = &DBTestFilters.reopen,
    },
    .{
        .name = "db-ttl-runtime-test",
        .description = "Run focused storage/db TTL runtime tests",
        .filters = &DBTestFilters.ttl_runtime,
    },
    .{
        .name = "db-enrichment-test",
        .description = "Run storage/db enrichment-related unit tests",
        .filters = &DBTestFilters.enrichment_any,
    },
};

const standalone_module_test_steps = .{
    .regex = StandaloneModuleTestStep{
        .name = "lib-regex-test",
        .description = "Run standalone lib/regex tests",
    },
    .jsonschema = StandaloneModuleTestStep{
        .name = "lib-jsonschema-test",
        .description = "Run standalone lib/jsonschema tests",
    },
    .json = StandaloneModuleTestStep{
        .name = "lib-json-test",
        .description = "Run standalone lib/json tests",
    },
    .ml_tabular = StandaloneModuleTestStep{
        .name = "lib-ml-tabular-test",
        .description = "Run standalone lib/ml/tabular tests",
    },
    .fuzz_tabular_loader = StandaloneModuleTestStep{
        .name = "fuzz-tabular-loader",
        .description = "Fuzz the tabular_model.json loader (--fuzz to keep running)",
    },
    .toon = StandaloneModuleTestStep{
        .name = "lib-toon-test",
        .description = "Run standalone lib/toon tests",
    },
    .mcp = StandaloneModuleTestStep{
        .name = "lib-mcp-test",
        .description = "Run standalone lib/mcp tests",
    },
    .a2a = StandaloneModuleTestStep{
        .name = "lib-a2a-test",
        .description = "Run standalone lib/a2a tests",
    },
    .matcher = StandaloneModuleTestStep{
        .name = "lib-matcher-test",
        .description = "Run standalone lib/matcher tests",
    },
    .resolver = StandaloneModuleTestStep{
        .name = "lib-resolver-test",
        .description = "Run standalone lib/resolver tests",
    },
    .httpx_json = StandaloneModuleTestStep{
        .name = "lib-httpx-json-test",
        .description = "Run standalone lib/httpx JSON helper tests",
    },
    .httpx = StandaloneModuleTestStep{
        .name = "lib-httpx-test",
        .description = "Run standalone lib/httpx tests",
    },
    .api_json_helpers = StandaloneModuleTestStep{
        .name = "lib-api-json-helpers-test",
        .description = "Run standalone api/json_helpers tests",
    },
    .generating = StandaloneModuleTestStep{
        .name = "lib-generating-test",
        .description = "Run standalone lib/generating tests",
    },
    .embeddings = StandaloneModuleTestStep{
        .name = "lib-embeddings-test",
        .description = "Run standalone lib/embeddings tests",
    },
    .vectorindex = StandaloneModuleTestStep{
        .name = "lib-vectorindex-test",
        .description = "Run standalone lib/vectorindex tests",
    },
    .chunking = StandaloneModuleTestStep{
        .name = "lib-chunking-test",
        .description = "Run standalone lib/chunking tests",
    },
    .readers = StandaloneModuleTestStep{
        .name = "lib-readers-test",
        .description = "Run standalone lib/readers tests",
    },
    .extracting = StandaloneModuleTestStep{
        .name = "lib-extracting-test",
        .description = "Run standalone lib/extracting tests",
    },
    .image = StandaloneModuleTestStep{
        .name = "lib-image-test",
        .description = "Run shared image tests",
    },
    .reranking = StandaloneModuleTestStep{
        .name = "lib-reranking-test",
        .description = "Run standalone lib/reranking tests",
    },
    .casbin = StandaloneModuleTestStep{
        .name = "lib-casbin-test",
        .description = "Run standalone lib/casbin tests",
    },
    .usermgr = StandaloneModuleTestStep{
        .name = "lib-usermgr-test",
        .description = "Run standalone pkg/antfly/src/usermgr tests",
    },
    .template = StandaloneModuleTestStep{
        .name = "lib-template-test",
        .description = "Run template rendering tests",
    },
};

const storage_backend_test_steps = .{
    .lmdb = StorageBackendTestStep{
        .name = "lmdb-test",
        .description = "Run Zig LMDB port unit tests",
    },
    .storage_lmdb = StorageBackendTestStep{
        .name = "storage-lmdb-test",
        .description = "Run storage/lmdb wrapper unit tests",
    },
    .storage_lmdb_replay = StorageBackendTestStep{
        .name = "lmdb-replay-fixtures",
        .description = "Run only the LMDB replay fixture test",
        .filters = &StorageBackendTestFilters.lmdb_replay,
    },
    .storage_sim_runtime = StorageBackendTestStep{
        .name = "storage-sim-runtime-test",
        .description = "Run storage simulation runtime and modeled device tests",
    },
    .storage_lmdb_soak = StorageBackendTestStep{
        .name = "lmdb-sim-soak",
        .description = "Run only the LMDB simulation soak test",
        .filters = &StorageBackendTestFilters.lmdb_soak,
    },
    .docstore = StorageBackendTestStep{
        .name = "docstore-test",
        .description = "Run storage/docstore unit tests",
    },
    .shard = StorageBackendTestStep{
        .name = "shard-test",
        .description = "Run storage/shard unit tests",
    },
    .wal = StorageBackendTestStep{
        .name = "wal-test",
        .description = "Run storage/wal unit tests",
        .simple_runner = true,
    },
    .wal_sim = StorageBackendTestStep{
        .name = "wal-sim-test",
        .description = "Run only the WAL simulation workload tests",
        .filters = &StorageBackendTestFilters.wal_sim,
    },
    .wal_vopr = StorageBackendTestStep{
        .name = "wal-vopr-test",
        .description = "Run WAL modeled-time VOPR smoke tests",
        .filters = &StorageBackendTestFilters.wal_vopr,
    },
    .wal_replay = StorageBackendTestStep{
        .name = "wal-replay-fixtures",
        .description = "Run only the WAL replay fixture tests",
        .filters = &StorageBackendTestFilters.wal_replay,
    },
    .wal_soak = StorageBackendTestStep{
        .name = "wal-sim-soak",
        .description = "Run only the WAL simulation soak test",
        .filters = &StorageBackendTestFilters.wal_soak,
    },
    .persistent = StorageBackendTestStep{
        .name = "persistent-test",
        .description = "Run storage/persistent unit tests",
        .simple_runner = true,
    },
    .persistent_sim = StorageBackendTestStep{
        .name = "persistent-sim-test",
        .description = "Run only the persistent simulation workload tests",
        .filters = &StorageBackendTestFilters.persistent_sim,
    },
    .persistent_replay = StorageBackendTestStep{
        .name = "persistent-replay-fixtures",
        .description = "Run only the persistent replay fixture tests",
        .filters = &StorageBackendTestFilters.persistent_replay,
    },
    .persistent_vopr = StorageBackendTestStep{
        .name = "persistent-vopr-test",
        .description = "Run persistent modeled-storage VOPR smoke tests",
        .filters = &StorageBackendTestFilters.persistent_vopr,
    },
    .persistent_soak = StorageBackendTestStep{
        .name = "persistent-sim-soak",
        .description = "Run only the persistent simulation soak test",
        .filters = &StorageBackendTestFilters.persistent_soak,
    },
    .index_manager = StorageBackendTestStep{
        .name = "index-manager-test",
        .description = "Run storage/db/catalog/index_manager unit tests",
        .filters = &no_default_filters,
        .select_filters = true,
        .simple_runner = true,
    },
    .index_manager_resource = StorageBackendTestStep{
        .name = "index-manager-resource-test",
        .description = "Run index manager resource-manager accounting tests",
        .filters = &StorageBackendTestFilters.index_manager_resource,
    },
    .index_manager_sim = StorageBackendTestStep{
        .name = "index-manager-sim-test",
        .description = "Run only the index manager simulation workload tests",
        .filters = &StorageBackendTestFilters.index_manager_sim,
    },
    .index_manager_replay = StorageBackendTestStep{
        .name = "index-manager-replay-fixtures",
        .description = "Run only the index manager replay fixture tests",
        .filters = &StorageBackendTestFilters.index_manager_replay,
    },
    .index_manager_vopr = StorageBackendTestStep{
        .name = "index-manager-vopr-test",
        .description = "Run index manager modeled-storage VOPR smoke tests",
        .filters = &StorageBackendTestFilters.index_manager_vopr,
    },
    .sparse = StorageBackendTestStep{
        .name = "sparse-test",
        .description = "Run sparse index unit tests",
    },
    .derived_log = StorageBackendTestStep{
        .name = "derived-log-test",
        .description = "Run storage/db/derived/derived_log unit tests",
    },
    .storage_sim = StorageBackendTestStep{
        .name = "storage-sim-test",
        .description = "Run legacy deterministic storage workload simulations that still use real storage I/O",
    },
    .storage_vopr = StorageBackendTestStep{
        .name = "storage-vopr-test",
        .description = "Run storage modeled-time/model-I/O VOPR smoke and simulation checks",
    },
    .storage_sim_soak = StorageBackendTestStep{
        .name = "storage-sim-soak",
        .description = "Run the LMDB and WAL simulation soak tests",
    },
};

fn addProgressBanner(b: *std.Build, label: []const u8) *std.Build.Step.Run {
    return b.addSystemCommand(&.{
        "sh",
        "-c",
        b.fmt("printf '\\n==== {s} ====\\n'", .{label}),
    });
}

pub fn chainLabeledRun(
    b: *std.Build,
    artifact: *std.Build.Step.Compile,
    label: []const u8,
    previous: ?*std.Build.Step,
) *std.Build.Step {
    const banner = addProgressBanner(b, label);
    if (previous) |step| banner.step.dependOn(step);
    const run = b.addRunArtifact(artifact);
    run.step.dependOn(&banner.step);
    return &run.step;
}

fn singleTestFilter(b: *std.Build, filter: []const u8) []const []const u8 {
    const filters = b.allocator.alloc([]const u8, 1) catch @panic("OOM");
    filters[0] = filter;
    return filters;
}

fn chainLabeledFilteredTest(
    b: *std.Build,
    root_module: *std.Build.Module,
    phase: []const u8,
    filter: []const u8,
    previous: ?*std.Build.Step,
) *std.Build.Step {
    const tests = b.addTest(.{
        .root_module = root_module,
        .filters = singleTestFilter(b, filter),
    });
    return chainLabeledRun(b, tests, b.fmt("{s}: {s}", .{ phase, filter }), previous);
}

pub fn chainLabeledFilteredTests(
    b: *std.Build,
    root_module: *std.Build.Module,
    phase: []const u8,
    filters: []const []const u8,
    previous: ?*std.Build.Step,
) *std.Build.Step {
    var tail = previous;
    for (filters) |filter| {
        tail = chainLabeledFilteredTest(b, root_module, phase, filter, tail);
    }
    return tail.?;
}

fn addTestArtifact(
    b: *std.Build,
    root_module: *std.Build.Module,
    default_filters: []const []const u8,
    simple_runner: bool,
) *std.Build.Step.Compile {
    return if (simple_runner) b.addTest(.{
        .root_module = root_module,
        .filters = selectTestFilters(b, default_filters),
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    }) else b.addTest(.{
        .root_module = root_module,
        .filters = selectTestFilters(b, default_filters),
    });
}

pub fn addModuleTestStep(
    b: *std.Build,
    root_module: *std.Build.Module,
    step_name: []const u8,
    description: []const u8,
    options: ModuleTestOptions,
) ModuleTestRun {
    const filters: []const []const u8 = if (options.filters) |default_filters|
        if (options.select_filters) selectTestFilters(b, default_filters) else default_filters
    else
        &.{};
    const tests = if (options.simple_runner) b.addTest(.{
        .root_module = root_module,
        .filters = filters,
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    }) else b.addTest(.{
        .root_module = root_module,
        .filters = filters,
    });
    const run = b.addRunArtifact(tests);
    const step = b.step(step_name, description);
    step.dependOn(&run.step);
    return .{
        .tests = tests,
        .run = run,
        .step = step,
    };
}

fn addStandaloneModuleTestStep(
    b: *std.Build,
    root_module: *std.Build.Module,
    test_step: StandaloneModuleTestStep,
) ModuleTestRun {
    const tests = b.addTest(.{
        .root_module = root_module,
    });
    const run = b.addRunArtifact(tests);
    const step = b.step(test_step.name, test_step.description);
    step.dependOn(&run.step);
    return .{
        .tests = tests,
        .run = run,
        .step = step,
    };
}

pub fn addStandaloneModuleTestSteps(
    b: *std.Build,
    modules: StandaloneModuleTestModules,
) StandaloneModuleTestRuns {
    return .{
        .regex = addStandaloneModuleTestStep(b, modules.regex, standalone_module_test_steps.regex),
        .jsonschema = addStandaloneModuleTestStep(b, modules.jsonschema, standalone_module_test_steps.jsonschema),
        .json = addStandaloneModuleTestStep(b, modules.json, standalone_module_test_steps.json),
        .ml_tabular = addStandaloneModuleTestStep(b, modules.ml_tabular, standalone_module_test_steps.ml_tabular),
        .fuzz_tabular_loader = addStandaloneModuleTestStep(b, modules.fuzz_tabular_loader, standalone_module_test_steps.fuzz_tabular_loader),
        .toon = addStandaloneModuleTestStep(b, modules.toon, standalone_module_test_steps.toon),
        .mcp = addStandaloneModuleTestStep(b, modules.mcp, standalone_module_test_steps.mcp),
        .a2a = addStandaloneModuleTestStep(b, modules.a2a, standalone_module_test_steps.a2a),
        .matcher = addStandaloneModuleTestStep(b, modules.matcher, standalone_module_test_steps.matcher),
        .resolver = addStandaloneModuleTestStep(b, modules.resolver, standalone_module_test_steps.resolver),
        .httpx_json = addStandaloneModuleTestStep(b, modules.httpx_json, standalone_module_test_steps.httpx_json),
        .httpx = addStandaloneModuleTestStep(b, modules.httpx, standalone_module_test_steps.httpx),
        .api_json_helpers = addStandaloneModuleTestStep(b, modules.api_json_helpers, standalone_module_test_steps.api_json_helpers),
        .generating = addStandaloneModuleTestStep(b, modules.generating, standalone_module_test_steps.generating),
        .embeddings = addStandaloneModuleTestStep(b, modules.embeddings, standalone_module_test_steps.embeddings),
        .vectorindex = addStandaloneModuleTestStep(b, modules.vectorindex, standalone_module_test_steps.vectorindex),
        .chunking = addStandaloneModuleTestStep(b, modules.chunking, standalone_module_test_steps.chunking),
        .readers = addStandaloneModuleTestStep(b, modules.readers, standalone_module_test_steps.readers),
        .extracting = addStandaloneModuleTestStep(b, modules.extracting, standalone_module_test_steps.extracting),
        .image = addStandaloneModuleTestStep(b, modules.image, standalone_module_test_steps.image),
        .reranking = addStandaloneModuleTestStep(b, modules.reranking, standalone_module_test_steps.reranking),
        .casbin = addStandaloneModuleTestStep(b, modules.casbin, standalone_module_test_steps.casbin),
        .usermgr = addStandaloneModuleTestStep(b, modules.usermgr, standalone_module_test_steps.usermgr),
        .template = addStandaloneModuleTestStep(b, modules.template, standalone_module_test_steps.template),
    };
}

fn addStorageBackendTestStep(
    b: *std.Build,
    root_module: *std.Build.Module,
    test_step: StorageBackendTestStep,
) ModuleTestRun {
    return addModuleTestStep(b, root_module, test_step.name, test_step.description, .{
        .filters = test_step.filters,
        .select_filters = test_step.select_filters,
        .simple_runner = test_step.simple_runner,
    });
}

pub fn addStorageBackendTestSteps(
    b: *std.Build,
    modules: StorageBackendTestModules,
    deps: StorageBackendTestDependencies,
) StorageBackendTestRuns {
    const lmdb = addStorageBackendTestStep(b, modules.lmdb_engine, storage_backend_test_steps.lmdb);
    const storage_lmdb = addStorageBackendTestStep(b, modules.storage_lmdb, storage_backend_test_steps.storage_lmdb);
    const storage_lmdb_replay = addStorageBackendTestStep(b, modules.storage_lmdb, storage_backend_test_steps.storage_lmdb_replay);
    const storage_sim_runtime = addStorageBackendTestStep(b, modules.storage_sim_runtime, storage_backend_test_steps.storage_sim_runtime);
    const storage_lmdb_soak = addStorageBackendTestStep(b, modules.storage_lmdb_soak, storage_backend_test_steps.storage_lmdb_soak);
    const docstore = addStorageBackendTestStep(b, modules.docstore, storage_backend_test_steps.docstore);
    const shard = addStorageBackendTestStep(b, modules.shard, storage_backend_test_steps.shard);
    const wal = addStorageBackendTestStep(b, modules.wal, storage_backend_test_steps.wal);
    const wal_sim = addStorageBackendTestStep(b, modules.wal, storage_backend_test_steps.wal_sim);
    const wal_vopr = addStorageBackendTestStep(b, modules.wal, storage_backend_test_steps.wal_vopr);
    const wal_replay = addStorageBackendTestStep(b, modules.wal, storage_backend_test_steps.wal_replay);
    const wal_soak = addStorageBackendTestStep(b, modules.wal_soak, storage_backend_test_steps.wal_soak);
    const persistent = addStorageBackendTestStep(b, modules.persistent, storage_backend_test_steps.persistent);
    const persistent_sim = addStorageBackendTestStep(b, modules.persistent, storage_backend_test_steps.persistent_sim);
    const persistent_replay = addStorageBackendTestStep(b, modules.persistent, storage_backend_test_steps.persistent_replay);
    const persistent_vopr = addStorageBackendTestStep(b, modules.persistent, storage_backend_test_steps.persistent_vopr);
    const persistent_soak = addStorageBackendTestStep(b, modules.persistent_soak, storage_backend_test_steps.persistent_soak);
    const index_manager = addStorageBackendTestStep(b, modules.index_manager, storage_backend_test_steps.index_manager);
    const index_manager_resource = addStorageBackendTestStep(b, modules.index_manager, storage_backend_test_steps.index_manager_resource);
    const index_manager_sim = addStorageBackendTestStep(b, modules.index_manager, storage_backend_test_steps.index_manager_sim);
    const index_manager_replay = addStorageBackendTestStep(b, modules.index_manager, storage_backend_test_steps.index_manager_replay);
    const index_manager_vopr = addStorageBackendTestStep(b, modules.index_manager, storage_backend_test_steps.index_manager_vopr);
    const sparse = addStorageBackendTestStep(b, modules.sparse, storage_backend_test_steps.sparse);
    const derived_log = addStorageBackendTestStep(b, modules.derived_log, storage_backend_test_steps.derived_log);

    const storage_sim_soak = b.step(storage_backend_test_steps.storage_sim_soak.name, storage_backend_test_steps.storage_sim_soak.description);
    storage_sim_soak.dependOn(&storage_lmdb_soak.run.step);
    storage_sim_soak.dependOn(&wal_soak.run.step);
    storage_sim_soak.dependOn(&persistent_soak.run.step);

    const storage_sim = b.step(storage_backend_test_steps.storage_sim.name, storage_backend_test_steps.storage_sim.description);
    storage_sim.dependOn(&wal_sim.run.step);
    storage_sim.dependOn(&persistent_sim.run.step);
    storage_sim.dependOn(&index_manager_sim.run.step);

    const storage_vopr = b.step(storage_backend_test_steps.storage_vopr.name, storage_backend_test_steps.storage_vopr.description);
    storage_vopr.dependOn(&storage_sim_runtime.run.step);
    storage_vopr.dependOn(&deps.lsm_backend_sim.step);
    storage_vopr.dependOn(&wal_vopr.run.step);
    storage_vopr.dependOn(&persistent_vopr.run.step);
    storage_vopr.dependOn(&index_manager_vopr.run.step);

    return .{
        .lmdb = lmdb,
        .storage_lmdb = storage_lmdb,
        .storage_lmdb_replay = storage_lmdb_replay,
        .storage_sim_runtime = storage_sim_runtime,
        .storage_lmdb_soak = storage_lmdb_soak,
        .docstore = docstore,
        .shard = shard,
        .wal = wal,
        .wal_sim = wal_sim,
        .wal_vopr = wal_vopr,
        .wal_replay = wal_replay,
        .wal_soak = wal_soak,
        .persistent = persistent,
        .persistent_sim = persistent_sim,
        .persistent_replay = persistent_replay,
        .persistent_vopr = persistent_vopr,
        .persistent_soak = persistent_soak,
        .index_manager = index_manager,
        .index_manager_resource = index_manager_resource,
        .index_manager_sim = index_manager_sim,
        .index_manager_replay = index_manager_replay,
        .index_manager_vopr = index_manager_vopr,
        .sparse = sparse,
        .derived_log = derived_log,
        .storage_sim = storage_sim,
        .storage_vopr = storage_vopr,
        .storage_sim_soak = storage_sim_soak,
    };
}

fn addSimpleAPITestRun(
    b: *std.Build,
    root_module: *std.Build.Module,
    default_filters: []const []const u8,
    select_filters: bool,
) *std.Build.Step.Run {
    const filters = if (select_filters) selectTestFilters(b, default_filters) else default_filters;
    const tests = b.addTest(.{
        .root_module = root_module,
        .filters = filters,
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    return b.addRunArtifact(tests);
}

fn addFocusedAPITestStep(
    b: *std.Build,
    name: []const u8,
    description: []const u8,
    run: *std.Build.Step.Run,
) void {
    const step = b.step(name, description);
    step.dependOn(&run.step);
}

fn addAPIFocusedTestRun(
    b: *std.Build,
    root_module: *std.Build.Module,
    name: ?[]const u8,
    description: []const u8,
    default_filters: []const []const u8,
    simple_runner: bool,
    select_filters: bool,
    dependency: ?*std.Build.Step,
) APIFocusedTestRun {
    const filters = if (select_filters) selectTestFilters(b, default_filters) else default_filters;
    const tests = if (simple_runner) b.addTest(.{
        .root_module = root_module,
        .filters = filters,
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    }) else b.addTest(.{
        .root_module = root_module,
        .filters = filters,
    });
    const run = b.addRunArtifact(tests);
    if (dependency) |dep| run.step.dependOn(dep);
    const step = if (name) |step_name| blk: {
        const focused_step = b.step(step_name, description);
        focused_step.dependOn(&run.step);
        break :blk focused_step;
    } else null;
    return .{
        .tests = tests,
        .run = run,
        .step = step,
    };
}

pub fn addAPIFocusedTestSteps(
    b: *std.Build,
    root_module: *std.Build.Module,
    openapi_root_check_step: *std.Build.Step,
) APIFocusedTestRuns {
    return .{
        .public_api_parity = addAPIFocusedTestRun(b, root_module, "public-api-parity-test", "Run focused stateful public API parity tests", &APITestFilters.public_api_parity, true, true, openapi_root_check_step),
        .public_api_graph_metric_e2e = addAPIFocusedTestRun(b, root_module, null, "Run focused public API graph metric e2e tests", &APITestFilters.public_api_graph_metric_e2e, false, false, openapi_root_check_step),
        .resolution_source = addAPIFocusedTestRun(b, root_module, "lib-resolution-source-test", "Run focused cross-shard resolution candidate-source and entity-sink tests", &APITestFilters.resolution_source, false, false, null),
        .auth = addAPIFocusedTestRun(b, root_module, "lib-api-auth-test", "Run focused API auth/usermgr HTTP tests", &APITestFilters.auth, true, false, openapi_root_check_step),
        .logic = addAPIFocusedTestRun(b, root_module, "lib-api-logic-test", "Run focused API table/index encoder, parser, and schema-update logic tests", &APITestFilters.logic, true, false, openapi_root_check_step),
        .docid_lifecycle = addAPIFocusedTestRun(b, root_module, null, "Run focused DOCID lifecycle and distributed snapshot hardening tests", &APITestFilters.docid_lifecycle, true, false, null),
        .swarm_backup_restore = addAPIFocusedTestRun(b, root_module, "lib-api-swarm-backup-restore-test", "Run the focused swarm-like backup/restore e2e test", &APITestFilters.swarm_backup_restore, false, false, null),
    };
}

fn addSimpleSelectedTestRun(
    b: *std.Build,
    root_module: *std.Build.Module,
    default_filters: []const []const u8,
    skip_filters: []const []const u8,
) *std.Build.Step.Run {
    const tests = b.addTest(.{
        .root_module = root_module,
        .filters = selectTestFilters(b, default_filters),
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run = b.addRunArtifact(tests);
    for (skip_filters) |filter| {
        run.addArgs(&.{ "--skip-test-filter", filter });
    }
    return run;
}

fn addFocusedTestStep(
    b: *std.Build,
    name: []const u8,
    description: []const u8,
    run: *std.Build.Step.Run,
) void {
    const step = b.step(name, description);
    step.dependOn(&run.step);
}

fn addMetadataTestRun(
    b: *std.Build,
    root_module: *std.Build.Module,
    name: []const u8,
    description: []const u8,
    default_filters: []const []const u8,
    select_filters: bool,
) MetadataTestRun {
    var test_run = addMetadataTestRunArtifact(b, root_module, default_filters, select_filters);
    const step = b.step(name, description);
    step.dependOn(&test_run.run.step);
    test_run.step = step;
    return test_run;
}

fn addMetadataTestRunArtifact(
    b: *std.Build,
    root_module: *std.Build.Module,
    default_filters: []const []const u8,
    select_filters: bool,
) MetadataTestRun {
    const filters = if (select_filters) selectTestFilters(b, default_filters) else default_filters;
    const tests = b.addTest(.{
        .root_module = root_module,
        .filters = filters,
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run = b.addRunArtifact(tests);
    return .{
        .tests = tests,
        .run = run,
        .step = &run.step,
    };
}

pub fn addMetadataTestSteps(
    b: *std.Build,
    root_module: *std.Build.Module,
    foreign_key_module: *std.Build.Module,
) MetadataTestSteps {
    const root = addMetadataTestRun(b, root_module, "metadata-test", "Run root-module and foreign-key metadata tests", &MetadataTestFilters.root, true);
    const foreign_key_tests = b.addTest(.{
        .root_module = foreign_key_module,
        .filters = &MetadataTestFilters.foreign_key,
    });
    const run_foreign_key_tests = b.addRunArtifact(foreign_key_tests);
    root.step.dependOn(&run_foreign_key_tests.step);
    const table_workflow = addMetadataTestRunArtifact(b, root_module, &MetadataTestFilters.table_workflow, false);
    const service = addMetadataTestRunArtifact(b, root_module, &MetadataTestFilters.service, false);
    const logic = addMetadataTestRunArtifact(b, root_module, &MetadataTestFilters.logic, false);
    root.step.dependOn(&table_workflow.run.step);
    root.step.dependOn(&service.run.step);
    root.step.dependOn(&logic.run.step);

    const sim = addMetadataTestRunArtifact(b, root_module, &MetadataTestFilters.sim, true);
    const sim_core = addMetadataTestRunArtifact(b, root_module, &MetadataTestFilters.sim_core, true);
    const sim_smoke = addMetadataTestRunArtifact(b, root_module, &MetadataTestFilters.sim_smoke, true);
    const vopr = addMetadataTestRunArtifact(b, root_module, &MetadataTestFilters.vopr, true);
    const sim_public = addMetadataTestRunArtifact(b, root_module, &MetadataTestFilters.sim_public, false);
    const sim_forward = addMetadataTestRunArtifact(b, root_module, &MetadataTestFilters.sim_forward, false);
    const sim_all = b.step("metadata-sim-test", "Run metadata simulation and VOPR tests");
    sim_all.dependOn(&sim.run.step);
    sim_all.dependOn(&sim_core.run.step);
    sim_all.dependOn(&sim_smoke.run.step);
    sim_all.dependOn(&vopr.run.step);
    sim_all.dependOn(&sim_public.run.step);
    sim_all.dependOn(&sim_forward.run.step);

    const vopr_chaos = addMetadataTestRunArtifact(b, root_module, &MetadataTestFilters.vopr_chaos, true);
    const transition_chaos = chainLabeledFilteredTests(b, root_module, "metadata-chaos.transition", selectTestFilters(b, &MetadataTestFilters.transition_chaos), null);
    const public_chaos = chainLabeledFilteredTests(b, root_module, "metadata-chaos.public", selectTestFilters(b, &MetadataTestFilters.public_chaos), null);
    const relational_public_chaos = chainLabeledFilteredTests(b, root_module, "metadata-chaos.relational-public", selectTestFilters(b, &MetadataTestFilters.relational_public_chaos), null);
    const placement_chaos = chainLabeledFilteredTests(b, root_module, "metadata-chaos.placement", selectTestFilters(b, &MetadataTestFilters.placement_chaos), null);

    const all_chaos = b.step("metadata-chaos-test", "Run metadata delayed/restart/partition chaos simulations");
    all_chaos.dependOn(&vopr_chaos.run.step);
    all_chaos.dependOn(transition_chaos);
    all_chaos.dependOn(public_chaos);
    all_chaos.dependOn(relational_public_chaos);
    all_chaos.dependOn(placement_chaos);

    return .{
        .root = root,
        .table_workflow = table_workflow,
        .sim = sim,
        .sim_core = sim_core,
        .sim_smoke = sim_smoke,
        .vopr = vopr,
        .vopr_chaos = vopr_chaos,
        .sim_public = sim_public,
        .sim_forward = sim_forward,
        .sim_all = sim_all,
        .service = service,
        .logic = logic,
        .chaos = .{
            .transition = transition_chaos,
            .public = public_chaos,
            .relational_public = relational_public_chaos,
            .placement = placement_chaos,
            .all = all_chaos,
        },
    };
}

pub fn chainMetadataChaosSoakTests(
    b: *std.Build,
    root_module: *std.Build.Module,
    previous: ?*std.Build.Step,
) *std.Build.Step {
    var tail = previous;
    tail = chainLabeledFilteredTests(b, root_module, "metadata-chaos.transition", selectTestFilters(b, &MetadataTestFilters.transition_chaos), tail);
    tail = chainLabeledFilteredTests(b, root_module, "metadata-chaos.public", selectTestFilters(b, &MetadataTestFilters.public_chaos), tail);
    tail = chainLabeledFilteredTests(b, root_module, "metadata-chaos.placement", selectTestFilters(b, &MetadataTestFilters.placement_chaos), tail);
    return tail.?;
}

fn addStorageTestRun(
    b: *std.Build,
    root_module: *std.Build.Module,
    name: ?[]const u8,
    description: []const u8,
    default_filters: []const []const u8,
    select_filters: bool,
    skip_filters: []const []const u8,
) StorageTestRun {
    const filters = if (select_filters) selectTestFilters(b, default_filters) else default_filters;
    const tests = b.addTest(.{
        .root_module = root_module,
        .filters = filters,
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run = b.addRunArtifact(tests);
    for (skip_filters) |filter| {
        run.addArgs(&.{ "--skip-test-filter", filter });
    }
    const step = if (name) |step_name| blk: {
        const storage_step = b.step(step_name, description);
        storage_step.dependOn(&run.step);
        break :blk storage_step;
    } else null;
    return .{
        .tests = tests,
        .run = run,
        .step = step,
    };
}

pub fn addStorageTestSteps(
    b: *std.Build,
    root_module: *std.Build.Module,
    progress_skip_filters: []const []const u8,
) StorageTestSteps {
    return .{
        .root = addStorageTestRun(b, root_module, "lib-storage-test", "Run root-module storage tests only", &StorageTestFilters.root, true, &.{}),
        .ha = addStorageTestRun(b, root_module, null, "Run hot-standby HA storage tests", &StorageTestFilters.ha, false, &ha_storage_default_skip_filters),
        .progress = addStorageTestRun(b, root_module, null, "Run root-module storage tests with progress skips", &StorageTestFilters.root, true, progress_skip_filters),
        .lsm_backend = addStorageTestRun(b, root_module, "lsm-backend-test", "Run LSM backend unit tests only", &StorageTestFilters.lsm_backend, false, &.{}),
        .resource_budget = addStorageTestRun(b, root_module, "resource-budget-test", "Run storage resource-manager accounting tests", &StorageTestFilters.resource_budget, false, &.{}),
    };
}

pub fn addRootTestStep(
    b: *std.Build,
    root_module: *std.Build.Module,
) RootTestStep {
    const tests = b.addTest(.{
        .root_module = root_module,
        .filters = selectTestFilters(b, &RootTestFilters.fast),
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run = b.addRunArtifact(tests);
    for (RootTestFilters.skip) |filter| {
        run.addArgs(&.{ "--skip-test-filter", filter });
    }
    const step = b.step("root-test", "Run fast root-module compile smoke tests");
    step.dependOn(&run.step);
    return .{
        .tests = tests,
        .run = run,
        .step = step,
    };
}

pub fn addGraphMetricTestSteps(
    b: *std.Build,
    modules: GraphMetricTestModules,
    root_test_skip_filters: []const []const u8,
) GraphMetricTestRuns {
    const runtime_unit = addSimpleSelectedTestRun(b, modules.root, &GraphMetricTestFilters.unit_runtime, root_test_skip_filters);
    const smoke = addSimpleSelectedTestRun(b, modules.root, &GraphMetricTestFilters.smoke, root_test_skip_filters);
    const lifecycle = addSimpleSelectedTestRun(b, modules.root, &GraphMetricTestFilters.lifecycle, root_test_skip_filters);
    const query_fan_in = addSimpleSelectedTestRun(b, modules.query_fan_in, &GraphMetricTestFilters.query_fan_in, &.{});
    const operations = addSimpleSelectedTestRun(b, modules.root, &GraphMetricTestFilters.operations, root_test_skip_filters);
    const runtime_operations = addSimpleSelectedTestRun(b, modules.root, &GraphMetricTestFilters.runtime_operations, root_test_skip_filters);
    const cleanup = addSimpleSelectedTestRun(b, modules.root, &GraphMetricTestFilters.cleanup, root_test_skip_filters);
    const degree_canary = addSimpleSelectedTestRun(b, modules.root, &GraphMetricTestFilters.degree_canary, root_test_skip_filters);
    const default_gate = addSimpleSelectedTestRun(b, modules.root, &GraphMetricTestFilters.default_gate, root_test_skip_filters);

    const unit = b.step("graph-metric-unit-test", "Run cheap graph metric runtime, fan-in, and API logic tests");
    unit.dependOn(&runtime_unit.step);
    unit.dependOn(&query_fan_in.step);
    unit.dependOn(&operations.step);

    addFocusedTestStep(b, "graph-metric-smoke-test", "Run one small graph metric DB smoke test", smoke);
    addFocusedTestStep(b, "graph-metric-lifecycle-test", "Run focused graph metric planned lifecycle tests", lifecycle);
    addFocusedTestStep(b, "graph-metric-operations-test", "Run focused graph metric operation tests", operations);
    addFocusedTestStep(b, "graph-metric-runtime-operations-test", "Run focused graph metric runtime operation tests", runtime_operations);
    addFocusedTestStep(b, "graph-metric-degree-canary-test", "Run focused graph metric degree-canary tests", degree_canary);
    addFocusedTestStep(b, "graph-metric-default-gate-test", "Run focused graph metric default-gate tests", default_gate);

    const integration = b.step("graph-metric-integration-test", "Run graph metric scheduler, worker, cleanup, and runtime lifecycle tests");
    integration.dependOn(&lifecycle.step);
    integration.dependOn(&runtime_operations.step);
    integration.dependOn(&cleanup.step);
    integration.dependOn(&degree_canary.step);
    integration.dependOn(&default_gate.step);

    return .{
        .unit = unit,
        .smoke = smoke,
        .lifecycle = lifecycle,
        .query_fan_in = query_fan_in,
        .operations = operations,
        .runtime_operations = runtime_operations,
        .cleanup = cleanup,
        .degree_canary = degree_canary,
        .default_gate = default_gate,
        .integration = integration,
    };
}

pub fn addAPITableTestSteps(
    b: *std.Build,
    modules: APITableTestModules,
) APITableTestRuns {
    const docid = addSimpleAPITestRun(b, modules.root, &APITestFilters.docid, false);
    const serverless_docid = addSimpleAPITestRun(b, modules.root, &APITestFilters.serverless_docid, false);
    const transactions_docid = addSimpleAPITestRun(b, modules.transactions_docid, &APITestFilters.transactions_docid, false);
    const table_writes = addSimpleAPITestRun(b, modules.table_writes, &APITestFilters.table_writes, true);
    const provisioned_query_visibility = addSimpleAPITestRun(b, modules.table_writes, &APITestFilters.provisioned_query_visibility, true);
    const table_reads = addSimpleAPITestRun(b, modules.table_reads, &APITestFilters.table_reads, false);
    const table_reads_graph_metric = addSimpleAPITestRun(b, modules.table_reads, &APITestFilters.table_reads_graph_metric, false);
    const public_table_http_docid = addSimpleAPITestRun(b, modules.public_table_http_docid, &APITestFilters.public_table_http_docid, false);
    const rows = addSimpleAPITestRun(b, modules.rows, &APITestFilters.rows, true);
    const sql_api_parity = addSimpleAPITestRun(b, modules.rows, &APITestFilters.sql_api_parity, false);
    const sql_api_parity_fixture_promote = addSimpleAPITestRun(b, modules.rows, &APITestFilters.sql_api_parity_fixture, false);
    sql_api_parity_fixture_promote.setEnvironmentVariable("ANTFLY_SQL_API_PARITY_FIXTURE_PROMOTE", "pkg/antfly/src/sql/fixtures/sql_api_parity_corpus.json");
    const sql_api_parity_fixture_check = addSimpleAPITestRun(b, modules.rows, &APITestFilters.sql_api_parity_fixture, false);
    sql_api_parity_fixture_check.setEnvironmentVariable("ANTFLY_SQL_API_PARITY_FIXTURE_CHECK", "pkg/antfly/src/sql/fixtures/sql_api_parity_corpus.json");
    const internal_group_write_routes = addSimpleAPITestRun(b, modules.internal_group_write_routes, &APITestFilters.internal_group_write_routes, false);
    const raft_transition_runtime_docid = addSimpleAPITestRun(b, modules.raft_transition_runtime_docid, &APITestFilters.raft_transition_runtime_docid, false);

    addFocusedAPITestStep(b, "api-transactions-test", "Run focused API transaction coordinator tests", transactions_docid);
    addFocusedAPITestStep(b, "api-table-writes-test", "Run focused API table write tests", table_writes);
    addFocusedAPITestStep(b, "provisioned-query-visibility-test", "Run focused provisioned query visibility tests", provisioned_query_visibility);
    addFocusedAPITestStep(b, "api-table-reads-test", "Run focused API table read tests", table_reads);
    addFocusedAPITestStep(b, "api-internal-group-write-routes-test", "Run focused internal group write route tests", internal_group_write_routes);
    addFocusedAPITestStep(b, "api-rows-test", "Run focused relational row API tests", rows);
    addFocusedAPITestStep(b, "sql-api-parity-test", "Run SQL/API typed-plan parity corpus tests", sql_api_parity);
    addFocusedAPITestStep(b, "sql-api-parity-fixture-promote", "Regenerate the SQL/API typed-plan parity fixture from the source corpus", sql_api_parity_fixture_promote);
    addFocusedAPITestStep(b, "sql-api-parity-fixture-check", "Check that the SQL/API typed-plan parity fixture matches the source corpus", sql_api_parity_fixture_check);
    return .{
        .docid = docid,
        .serverless_docid = serverless_docid,
        .transactions_docid = transactions_docid,
        .table_writes = table_writes,
        .provisioned_query_visibility = provisioned_query_visibility,
        .table_reads = table_reads,
        .table_reads_graph_metric = table_reads_graph_metric,
        .public_table_http_docid = public_table_http_docid,
        .rows = rows,
        .sql_api_parity = sql_api_parity,
        .sql_api_parity_fixture_promote = sql_api_parity_fixture_promote,
        .sql_api_parity_fixture_check = sql_api_parity_fixture_check,
        .internal_group_write_routes = internal_group_write_routes,
        .raft_transition_runtime_docid = raft_transition_runtime_docid,
    };
}

const api_table_test_roots = .{
    .{
        .field = "transactions_docid",
        .path = "pkg/antfly/src/api_transactions_test_root.zig",
    },
    .{
        .field = "table_writes",
        .path = "pkg/antfly/src/api_table_writes_test_root.zig",
    },
    .{
        .field = "table_reads",
        .path = "pkg/antfly/src/api_table_reads_test_root.zig",
    },
    .{
        .field = "public_table_http_docid",
        .path = "pkg/antfly/src/api_public_table_http_test_root.zig",
    },
    .{
        .field = "rows",
        .path = "pkg/antfly/src/api_rows_test_root.zig",
    },
    .{
        .field = "internal_group_write_routes",
        .path = "pkg/antfly/src/api_internal_group_write_routes_test_root.zig",
    },
    .{
        .field = "raft_transition_runtime_docid",
        .path = "pkg/antfly/src/raft_transition_runtime_test_root.zig",
    },
};

fn makeAPITableTestModule(
    b: *std.Build,
    root_path: []const u8,
    options: APITableTestRootOptions,
    imports: anytype,
) *std.Build.Module {
    const module = b.createModule(.{
        .root_source_file = b.path(root_path),
        .target = options.target,
        .optimize = options.optimize,
    });
    imports.configure(b, module, true, true);
    return module;
}

pub fn makeAPITableTestModules(
    b: *std.Build,
    options: APITableTestRootOptions,
    imports: anytype,
) APITableTestModules {
    var modules: APITableTestModules = undefined;
    modules.root = options.root;
    inline for (api_table_test_roots) |root| {
        @field(modules, root.field) = makeAPITableTestModule(b, root.path, options, imports);
    }
    return modules;
}

pub fn addAPITableTestRootSteps(
    b: *std.Build,
    options: APITableTestRootOptions,
    imports: anytype,
) APITableTestRuns {
    return addAPITableTestSteps(b, makeAPITableTestModules(b, options, imports));
}

pub fn addAPITableAggregateTestStep(
    b: *std.Build,
    runs: APITableTestRuns,
    deps: APITableAggregateDependencies,
) *std.Build.Step {
    const step = b.step("lib-api-test", "Run focused API boundary tests");
    step.dependOn(&runs.docid.step);
    step.dependOn(&runs.serverless_docid.step);
    step.dependOn(&runs.transactions_docid.step);
    step.dependOn(&runs.table_reads.step);
    step.dependOn(&runs.table_writes.step);
    step.dependOn(&runs.public_table_http_docid.step);
    step.dependOn(&runs.rows.step);
    step.dependOn(&runs.internal_group_write_routes.step);
    step.dependOn(&runs.raft_transition_runtime_docid.step);
    step.dependOn(&deps.data_storage.step);
    step.dependOn(&deps.data_runtime.step);
    step.dependOn(&deps.metadata_sim_smoke.step);
    step.dependOn(&deps.metadata_sim_public.step);
    step.dependOn(&deps.metadata_vopr.step);
    step.dependOn(&deps.metadata_vopr_chaos.step);
    step.dependOn(deps.metadata_public_chaos);
    step.dependOn(&deps.db_result_shape.step);
    return step;
}

pub fn addDocIdTestStep(
    b: *std.Build,
    focused: APIFocusedTestRun,
    runs: APITableTestRuns,
    db_result_shape: *std.Build.Step.Run,
) *std.Build.Step {
    const step = b.step("docid-test", "Run DOCID lifecycle, storage, and query focused tests");
    step.dependOn(&focused.run.step);
    step.dependOn(&runs.docid.step);
    step.dependOn(&db_result_shape.step);
    return step;
}

pub fn dependOnAPITableUnitTestRuns(
    step: *std.Build.Step,
    runs: APITableTestRuns,
) void {
    step.dependOn(&runs.provisioned_query_visibility.step);
    step.dependOn(&runs.docid.step);
    step.dependOn(&runs.rows.step);
    step.dependOn(&runs.sql_api_parity.step);
    step.dependOn(&runs.internal_group_write_routes.step);
    step.dependOn(&runs.table_reads_graph_metric.step);
}

pub fn dependOnAPIGeneratedChecks(
    step: *std.Build.Step,
    runs: APITableTestRuns,
) void {
    step.dependOn(&runs.sql_api_parity_fixture_check.step);
}

pub fn addDBRootTestStep(
    b: *std.Build,
    root_module: *std.Build.Module,
) DBRootTestStep {
    const tests = addTestArtifact(b, root_module, &DBTestFilters.root, true);
    const run = b.addRunArtifact(tests);
    const step = b.step(db_root_step_name, "Run root-module DB tests only");
    step.dependOn(&run.step);
    return .{
        .tests = tests,
        .run = run,
    };
}

pub fn addDBRootModuleTestSteps(
    b: *std.Build,
    root_module: *std.Build.Module,
) DBRootModuleTestSteps {
    var result_shape: ?*std.Build.Step.Run = null;
    for (db_root_module_steps) |db_step| {
        const run = addDBFilteredTestStep(b, root_module, db_step);
        if (isDBResultShapeStep(db_step)) {
            result_shape = run;
        }
    }

    return .{
        .result_shape = result_shape orelse @panic("missing DB result-shape test step"),
    };
}

fn addDBStorageTestStep(
    b: *std.Build,
    root_module: *std.Build.Module,
) *std.Build.Step.Run {
    const tests = b.addTest(.{
        .root_module = root_module,
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run = b.addRunArtifact(tests);
    const step = b.step(db_storage_step_name, "Run storage/db unit tests");
    step.dependOn(&run.step);
    return run;
}

pub fn addDBStorageTestSteps(
    b: *std.Build,
    root_module: *std.Build.Module,
) DBStorageTestSteps {
    const all = addDBStorageTestStep(b, root_module);

    var sim: ?*std.Build.Step.Run = null;
    for (db_storage_module_steps) |db_step| {
        const run = addDBFilteredTestStep(b, root_module, db_step);
        if (isDBSimStep(db_step)) {
            sim = run;
        }
    }

    return .{
        .all = all,
        .sim = sim orelse @panic("missing DB simulation test step"),
    };
}

pub fn addDBFilteredTestStep(
    b: *std.Build,
    root_module: *std.Build.Module,
    db_step: DBTestStep,
) *std.Build.Step.Run {
    const tests = addTestArtifact(b, root_module, db_step.filters, db_step.simple_runner);
    const run = b.addRunArtifact(tests);
    const step = b.step(db_step.name, db_step.description);
    step.dependOn(&run.step);
    return run;
}

fn isDBResultShapeStep(db_step: DBTestStep) bool {
    return std.mem.eql(u8, db_step.name, db_result_shape_step_name);
}

fn isDBSimStep(db_step: DBTestStep) bool {
    return std.mem.eql(u8, db_step.name, db_sim_step_name);
}

pub fn selectTestFilters(
    b: *std.Build,
    default_filters: []const []const u8,
) []const []const u8 {
    const args = b.args orelse return default_filters;
    if (args.len == 0) return default_filters;

    if (std.mem.eql(u8, args[0], "--test-filter")) {
        if (args.len <= 1) return default_filters;
        return args[1..];
    }
    return args;
}
