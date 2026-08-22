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
const builtin = @import("builtin");
const build_test_filters = @import("build_test_filters.zig");
const antfly_benches_build = @import("pkg/antfly/build/benches.zig");
const antfly_embedded_build = @import("pkg/antfly/build/embedded.zig");
const antfly_storage_build = @import("pkg/antfly/build/storage.zig");
const antfly_tests_build = @import("pkg/antfly/build/tests.zig");
const inference_runtime_build = @import("pkg/inference/build/runtime.zig");
const platform_build = @import("lib/platform/build_support.zig");

const LmdbBackend = antfly_storage_build.LmdbBackend;
const chainLabeledFilteredTests = antfly_tests_build.chainLabeledFilteredTests;
const chainLabeledRun = antfly_tests_build.chainLabeledRun;
const chainLabeledRunStep = antfly_tests_build.chainLabeledRunStep;
const configureEmbeddedModule = antfly_embedded_build.configureModule;
const lmdb_c_flags = antfly_storage_build.lmdb_c_flags;
const makeLmdbBuildOptions = antfly_storage_build.makeLmdbBuildOptions;
const makeLmdbEngineModule = antfly_storage_build.makeLmdbEngineModule;
const makeLmdbModule = antfly_storage_build.makeLmdbModule;
const makeRootBuildOptions = antfly_storage_build.makeRootBuildOptions;
const selectTestFilters = antfly_tests_build.selectTestFilters;

const RuntimeArtifactRole = enum {
    cli,
    data,
    inference,
    metadata,
    standalone,
};

const RuntimeLibraryUnit = enum {
    api_kernel,
    distributed,
    inference,
    // Remote/client commands do not own storage or server runtimes.
    cli,
};

// Static archives must be presented from consumers to providers. The
// distributed/application unit calls into both the API kernel and inference
// unit, while the remote CLI is an executable-facing leaf. Keep this separate
// from RuntimeLibraryUnit declaration order: declaration order controls build
// graph construction, not the final link's dependency topology.
const runtime_library_link_order = [_]RuntimeLibraryUnit{
    .cli,
    .distributed,
    .api_kernel,
    .inference,
};

comptime {
    const unit_count = std.meta.fields(RuntimeLibraryUnit).len;
    if (runtime_library_link_order.len != unit_count)
        @compileError("runtime_library_link_order must contain every runtime library unit exactly once");
    var seen = [_]bool{false} ** unit_count;
    for (runtime_library_link_order) |unit| {
        const index = @intFromEnum(unit);
        if (seen[index])
            @compileError("runtime_library_link_order contains a duplicate runtime library unit");
        seen[index] = true;
    }
}

const snowball_languages = [_][]const u8{
    "danish",
    "dutch",
    "finnish",
    "french",
    "german",
    "italian",
    "norwegian",
    "portuguese",
    "spanish",
    "swedish",
};

const snowball_generated_root = "pkg/antfly/src/search/snowball/generated";
const sql_grammar_source = "lib/sql/grammar/antfly_sql.y";
const sql_grammar_generated_root = "lib/sql/grammar/generated/root.zig";

const snowball_compiler_sources = [_][]const u8{
    "compiler/analyser.c",
    "compiler/driver.c",
    "compiler/generator.c",
    "compiler/generator_ada.c",
    "compiler/generator_csharp.c",
    "compiler/generator_dart.c",
    "compiler/generator_go.c",
    "compiler/generator_java.c",
    "compiler/generator_js.c",
    "compiler/generator_pascal.c",
    "compiler/generator_php.c",
    "compiler/generator_python.c",
    "compiler/generator_rust.c",
    "compiler/generator_zig.c",
    "compiler/space.c",
    "compiler/tokeniser.c",
};

fn pathExists(b: *std.Build, path: []const u8) bool {
    const io = b.graph.io;
    std.Io.Dir.cwd().access(io, path, .{}) catch return false;
    return true;
}

fn addMacosSdkPaths(b: *std.Build, module: *std.Build.Module, target: std.Build.ResolvedTarget) void {
    if (target.result.os.tag != .macos) return;
    const sdk_root = b.sysroot orelse
        b.graph.environ_map.get("SDK_PATH") orelse
        std.zig.system.darwin.getSdk(b.allocator, b.graph.io, &target.result) orelse
        return;
    module.addSystemIncludePath(.{ .cwd_relative = b.fmt("{s}/usr/include", .{sdk_root}) });
    module.addLibraryPath(.{ .cwd_relative = b.fmt("{s}/usr/lib", .{sdk_root}) });
    module.addFrameworkPath(.{ .cwd_relative = b.fmt("{s}/System/Library/Frameworks", .{sdk_root}) });
}

fn addScriptsPythonCommand(b: *std.Build, script_path: []const u8, args: []const []const u8) *std.Build.Step.Run {
    const run = b.addSystemCommand(&.{
        "uv",
        "run",
        "--project",
        "../scripts",
        "--locked",
        "python",
    });
    run.addFileArg(b.path(script_path));
    run.addArgs(args);
    return run;
}

const openapi_join_input_paths = [_][]const u8{
    "../scripts/join_openapi.py",
    "../scripts/openapi_joiner.py",
    "../specs/openapi/antfly/audio.yaml",
    "../specs/openapi/antfly/chunking.yaml",
    "../specs/openapi/antfly/config.yaml",
    "../specs/openapi/antfly/embeddings.yaml",
    "../specs/openapi/antfly/eval.yaml",
    "../specs/openapi/antfly/generating.yaml",
    "../specs/openapi/antfly/metadata.yaml",
    "../specs/openapi/antfly/query.yaml",
    "../specs/openapi/antfly/reranking.yaml",
    "../specs/openapi/antfly/sort.yaml",
    "../specs/openapi/antfly/websearch.yaml",
    "../specs/openapi/auth/api.yaml",
    "../specs/openapi/extensions/api.yaml",
    "../specs/openapi/inference/api.yaml",
    "../specs/openapi/inference/config.yaml",
    "../specs/openapi/shared/generating.yaml",
    "../specs/openapi/antfly/schema.yaml",
    "../specs/openapi/antfly/indexes.yaml",
};

fn addOpenApiJoinInputs(b: *std.Build, run: *std.Build.Step.Run) void {
    for (openapi_join_input_paths) |path| {
        run.addFileInput(b.path(path));
    }
}

const inference_delegated_steps = [_][]const u8{
    "run",
    "finetune",
    "bench-paged-attention",
    "bench-training",
    "bench-linalg",
    "bench-audio",
    "bench-gliner2-native",
    "gliner2-entity-training-readiness",
    "test-finetune",
    "test",
    "wasm",
};

const release_scale_test_filters = [_][]const u8{
    "db dense default dynamic 0.2 percent numeric filter exact scores bounded candidates",
    "one percent native filter routes through integrated dense search exactly",
    "db one real delete keeps filtered full text on complement path across restart",
    "db production ingest preserves high-frequency keyword recall across clean restarts",
};

const DelegatedPackageStep = struct {
    run: *std.Build.Step.Run,
    step: *std.Build.Step,
};

const DelegatedInferenceBuildSteps = struct {
    inference_test: *std.Build.Step,
    inference_finetune_test: *std.Build.Step,
};

fn dependOnAll(step: *std.Build.Step, dependencies: []const *std.Build.Step) void {
    for (dependencies) |dependency| {
        step.dependOn(dependency);
    }
}

fn assignDefaultAggregateMaxRss(
    b: *std.Build,
    root: *std.Build.Step,
    compile_max_rss: usize,
    run_max_rss: usize,
) void {
    var visited = std.AutoHashMap(*std.Build.Step, void).init(b.allocator);
    defer visited.deinit();
    assignDefaultAggregateMaxRssRecursive(root, compile_max_rss, run_max_rss, &visited);
}

fn assignDefaultAggregateMaxRssRecursive(
    step: *std.Build.Step,
    compile_max_rss: usize,
    run_max_rss: usize,
    visited: *std.AutoHashMap(*std.Build.Step, void),
) void {
    const entry = visited.getOrPut(step) catch @panic("OOM");
    if (entry.found_existing) return;
    if (step.max_rss == 0) switch (step.id) {
        .compile => step.max_rss = compile_max_rss,
        .run => step.max_rss = run_max_rss,
        else => {},
    };
    for (step.dependencies.items) |dependency| {
        assignDefaultAggregateMaxRssRecursive(
            dependency,
            compile_max_rss,
            run_max_rss,
            visited,
        );
    }
}

fn addRuntimeTestFilters(
    b: *std.Build,
    run: *std.Build.Step.Run,
    filters: []const []const u8,
) void {
    for (filters) |filter| {
        run.addArgs(&.{ "--test-filter", filter });
    }
    build_test_filters.addRuntimeControls(run, b.args orelse &.{});
}

fn addRuntimeSkipTestFilters(run: *std.Build.Step.Run, filters: []const []const u8) void {
    for (filters) |filter| {
        run.addArgs(&.{ "--skip-test-filter", filter });
    }
}

fn configureUnitStorageTestRun(
    b: *std.Build,
    run: *std.Build.Step.Run,
    runtime_filters: []const []const u8,
    allow_empty_filter: bool,
    unit_skip_filters: []const []const u8,
    root_skip_filters: []const []const u8,
    extra_skip_filters: []const []const u8,
    is_ha_shard: bool,
) void {
    addRuntimeTestFilters(b, run, runtime_filters);
    if (allow_empty_filter) run.addArg("--allow-empty-test-filter");
    addRuntimeSkipTestFilters(run, unit_skip_filters);
    for (root_skip_filters) |filter| {
        // `storage.ha` keeps the HA suite out of broad root-module test runs.
        // Applying it to the dedicated shard would select zero tests.
        if (is_ha_shard and std.mem.eql(u8, filter, "storage.ha")) continue;
        run.addArgs(&.{ "--skip-test-filter", filter });
    }
    addRuntimeSkipTestFilters(run, extra_skip_filters);
    addRuntimeSkipTestFilters(run, &release_scale_test_filters);
}

fn compileFiltersWithAnchors(
    b: *std.Build,
    anchors: []const []const u8,
    runtime_filters: []const []const u8,
) []const []const u8 {
    const filters = b.allocator.alloc([]const u8, anchors.len + runtime_filters.len) catch @panic("OOM");
    var count: usize = 0;
    for (anchors) |anchor| {
        filters[count] = anchor;
        count += 1;
    }
    for (runtime_filters) |filter| {
        var duplicate = false;
        for (filters[0..count]) |existing| {
            if (std.mem.eql(u8, existing, filter)) {
                duplicate = true;
                break;
            }
        }
        if (duplicate) continue;
        filters[count] = filter;
        count += 1;
    }
    return filters[0..count];
}

/// Zig's compile-time filters can retain imported anonymous tests needed for
/// semantic analysis. Give every filtered artifact the exact-filter runner and
/// apply the caller's independently selected runtime filters so compile-only
/// reachability anchors never become executed tests.
fn addFilteredTestRunArtifactWithRuntimeFilters(
    b: *std.Build,
    tests: *std.Build.Step.Compile,
    runtime_filters: []const []const u8,
) *std.Build.Step.Run {
    if (tests.test_runner == null) {
        const runner_path = b.path("pkg/antfly/src/test_runner.zig");
        tests.test_runner = .{ .path = runner_path, .mode = .simple };
        runner_path.addStepDependencies(&tests.step);
    }
    const run = b.addRunArtifact(tests);
    addRuntimeTestFilters(b, run, runtime_filters);
    return run;
}

fn addFilteredTestRunArtifact(b: *std.Build, tests: *std.Build.Step.Compile) *std.Build.Step.Run {
    return addFilteredTestRunArtifactWithRuntimeFilters(b, tests, tests.filters);
}

fn addDelegatedPackageStep(
    b: *std.Build,
    package_step_prefix: []const u8,
    package_dir: []const u8,
    step_name: []const u8,
    package_name: []const u8,
) DelegatedPackageStep {
    const run = b.addSystemCommand(&.{
        b.graph.zig_exe,
        "build",
        step_name,
    });
    run.setCwd(b.path(package_dir));
    const delegated = b.step(
        b.fmt("{s}-{s}", .{ package_step_prefix, step_name }),
        b.fmt("Delegate to {s} zig build {s}", .{ package_name, step_name }),
    );
    delegated.dependOn(&run.step);
    return .{
        .run = run,
        .step = delegated,
    };
}

fn forwardBuildArgs(b: *std.Build, run: *std.Build.Step.Run) void {
    if (b.args) |args| {
        run.addArg("--");
        run.addArgs(args);
    }
}

fn addDelegatedInferenceOptions(
    b: *std.Build,
    run: *std.Build.Step.Run,
    enable_metal: bool,
    enable_onnx: bool,
    onnx_root: []const u8,
    enable_cuda: bool,
    cuda_artifacts: []const u8,
    enable_pjrt: bool,
    enable_system_blas: bool,
    blas_root: ?[]const u8,
) void {
    run.addArg("-Dshared-lib-root=../..");
    run.addArg(if (enable_metal) "-Dmetal=true" else "-Dmetal=false");
    run.addArg(if (enable_onnx) "-Donnx=true" else "-Donnx=false");
    if (enable_onnx) {
        run.addArg(b.fmt("-Donnx-root={s}", .{onnx_root}));
    }
    run.addArg(if (enable_cuda) "-Dcuda=true" else "-Dcuda=false");
    run.addArg(b.fmt("-Dcuda-artifacts={s}", .{cuda_artifacts}));
    run.addArg(if (enable_pjrt) "-Dpjrt=true" else "-Dpjrt=false");
    run.addArg(if (enable_system_blas) "-Dsystem-blas=true" else "-Dsystem-blas=false");
    if (enable_system_blas) {
        if (blas_root) |root| run.addArg(b.fmt("-Dblas-root={s}", .{root}));
    }
}

fn expectQuietSuccess(run: *std.Build.Step.Run) *std.Build.Step {
    run.has_side_effects = true;
    run.expectExitCode(0);
    run.expectStdErrMatch("");
    return &run.step;
}

fn addDelegatedInferenceBuildSteps(
    b: *std.Build,
    enable_metal: bool,
    enable_onnx: bool,
    onnx_root: []const u8,
    enable_cuda: bool,
    cuda_artifacts: []const u8,
    enable_pjrt: bool,
    enable_system_blas: bool,
    blas_root: ?[]const u8,
) DelegatedInferenceBuildSteps {
    var test_step: ?*std.Build.Step = null;
    var finetune_test_step: ?*std.Build.Step = null;
    for (inference_delegated_steps) |step_name| {
        const delegated = addDelegatedPackageStep(b, "inference", "pkg/inference", step_name, "pkg/inference");
        const run = delegated.run;
        addDelegatedInferenceOptions(b, run, enable_metal, enable_onnx, onnx_root, enable_cuda, cuda_artifacts, enable_pjrt, enable_system_blas, blas_root);
        forwardBuildArgs(b, run);
        if (std.mem.eql(u8, step_name, "test")) {
            test_step = delegated.step;
        } else if (std.mem.eql(u8, step_name, "test-finetune")) {
            finetune_test_step = delegated.step;
        }
    }
    return .{
        .inference_test = test_step.?,
        .inference_finetune_test = finetune_test_step.?,
    };
}

const FfmpegPaths = struct {
    include_dir: []const u8,
    lib_dir: []const u8,
};

const SpngPaths = struct {
    include_dir: []const u8,
    lib_dir: []const u8,
};

fn defaultInferenceOnnxRoot(b: *std.Build, target: std.Build.ResolvedTarget) []const u8 {
    const platform_str = switch (target.result.os.tag) {
        .macos => "darwin",
        .linux => "linux",
        else => "unknown",
    };
    const arch_str = switch (target.result.cpu.arch) {
        .aarch64 => "arm64",
        .x86_64 => "amd64",
        else => "unknown",
    };
    return b.fmt("pkg/inference/onnxruntime/{s}-{s}", .{ platform_str, arch_str });
}

fn detectFfmpegPaths(b: *std.Build, target: std.Build.ResolvedTarget) ?FfmpegPaths {
    const macos_candidates = [_]FfmpegPaths{
        .{ .include_dir = "/opt/homebrew/include", .lib_dir = "/opt/homebrew/lib" },
        .{ .include_dir = "/opt/homebrew/opt/ffmpeg/include", .lib_dir = "/opt/homebrew/opt/ffmpeg/lib" },
        .{ .include_dir = "/usr/local/include", .lib_dir = "/usr/local/lib" },
        .{ .include_dir = "/usr/local/opt/ffmpeg/include", .lib_dir = "/usr/local/opt/ffmpeg/lib" },
    };
    const linux_candidates = [_]FfmpegPaths{
        .{ .include_dir = "/usr/include", .lib_dir = "/usr/lib/x86_64-linux-gnu" },
        .{ .include_dir = "/usr/include", .lib_dir = "/usr/lib/aarch64-linux-gnu" },
        .{ .include_dir = "/usr/include", .lib_dir = "/usr/lib64" },
        .{ .include_dir = "/usr/include", .lib_dir = "/usr/lib" },
        .{ .include_dir = "/usr/local/include", .lib_dir = "/usr/local/lib64" },
        .{ .include_dir = "/usr/local/include", .lib_dir = "/usr/local/lib" },
    };
    const candidates: []const FfmpegPaths = switch (target.result.os.tag) {
        .macos => macos_candidates[0..],
        .linux => linux_candidates[0..],
        else => return null,
    };

    for (candidates) |candidate| {
        const header = b.fmt("{s}/libavformat/avformat.h", .{candidate.include_dir});
        const dylib = b.fmt("{s}/libavformat.dylib", .{candidate.lib_dir});
        const so = b.fmt("{s}/libavformat.so", .{candidate.lib_dir});
        if (pathExists(b, header) and (pathExists(b, dylib) or pathExists(b, so))) return candidate;
    }
    return null;
}

fn detectSpngPaths(b: *std.Build, target: std.Build.ResolvedTarget) ?SpngPaths {
    const macos_candidates = [_]SpngPaths{
        .{ .include_dir = "/opt/homebrew/include", .lib_dir = "/opt/homebrew/lib" },
        .{ .include_dir = "/usr/local/include", .lib_dir = "/usr/local/lib" },
    };
    const linux_candidates = [_]SpngPaths{
        .{ .include_dir = "/usr/include", .lib_dir = "/usr/lib/x86_64-linux-gnu" },
        .{ .include_dir = "/usr/include", .lib_dir = "/usr/lib/aarch64-linux-gnu" },
        .{ .include_dir = "/usr/include", .lib_dir = "/usr/lib64" },
        .{ .include_dir = "/usr/include", .lib_dir = "/usr/lib" },
        .{ .include_dir = "/usr/local/include", .lib_dir = "/usr/local/lib64" },
        .{ .include_dir = "/usr/local/include", .lib_dir = "/usr/local/lib" },
    };
    const candidates: []const SpngPaths = switch (target.result.os.tag) {
        .macos => macos_candidates[0..],
        .linux => linux_candidates[0..],
        else => return null,
    };

    for (candidates) |candidate| {
        const header = b.fmt("{s}/spng.h", .{candidate.include_dir});
        const dylib = b.fmt("{s}/libspng.dylib", .{candidate.lib_dir});
        const so = b.fmt("{s}/libspng.so", .{candidate.lib_dir});
        const static_lib = b.fmt("{s}/libspng.a", .{candidate.lib_dir});
        if (pathExists(b, header) and (pathExists(b, dylib) or pathExists(b, so) or pathExists(b, static_lib))) return candidate;
    }
    return null;
}

fn addLocalSentencePieceProtoModule(
    b: *std.Build,
    protobuf_dep: *std.Build.Dependency,
) *std.Build.Module {
    const codegen = b.addRunArtifact(protobuf_dep.artifact("protoc-zig"));
    codegen.addArg("--desc");
    codegen.addFileArg(b.path("lib/tokenizer/proto/sentencepiece_model.desc"));
    codegen.addArg("--output");
    const raw_dir = codegen.addOutputDirectoryArg("sentencepiece_proto_raw");

    const fixup_tool = b.addExecutable(.{
        .name = "patch_sentencepiece_proto",
        .root_module = b.createModule(.{
            .root_source_file = b.path("pkg/inference/tools/patch_sentencepiece_proto.zig"),
            .target = b.graph.host,
            .optimize = .ReleaseSafe,
        }),
    });
    const fixup_run = b.addRunArtifact(fixup_tool);
    fixup_run.addFileArg(raw_dir.path(b, "root.zig"));
    fixup_run.addFileArg(raw_dir.path(b, "sentencepiece.zig"));
    const gen_dir = fixup_run.addOutputDirectoryArg("sentencepiece_proto");

    const mod = b.createModule(.{
        .root_source_file = gen_dir.path(b, "root.zig"),
    });
    mod.addImport("protobuf", protobuf_dep.module("protobuf"));
    return mod;
}

fn addLocalOpenApiCodegen(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    httpx_mod: *std.Build.Module,
) *std.Build.Step.Compile {
    const openapi_mod = b.createModule(.{
        .root_source_file = b.path("lib/openapi/src/openapi.zig"),
        .target = target,
        .optimize = optimize,
    });

    const exe = b.addExecutable(.{
        .name = "openapi-zig",
        .root_module = b.createModule(.{
            .root_source_file = b.path("lib/openapi/src/main.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    exe.root_module.addImport("openapi", openapi_mod);
    exe.root_module.addImport("httpx", httpx_mod);
    return exe;
}

fn addLocalYaccCodegen(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
) *std.Build.Step.Compile {
    const yacc_mod = b.createModule(.{
        .root_source_file = b.path("lib/yacc/src/root.zig"),
        .target = target,
        .optimize = optimize,
    });

    const exe = b.addExecutable(.{
        .name = "yacc-zig",
        .root_module = b.createModule(.{
            .root_source_file = b.path("lib/yacc/src/main.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    exe.root_module.addImport("yacc", yacc_mod);
    return exe;
}

const YaccSteps = struct {
    run_yacc_tests: *std.Build.Step.Run,
    run_parser_tests: *std.Build.Step.Run,
};

fn addYaccSteps(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
) YaccSteps {
    const yacc_codegen = addLocalYaccCodegen(b, target, optimize);
    const install_yacc_codegen = b.addInstallArtifact(yacc_codegen, .{});
    const yacc_codegen_step = b.step("yacc-zig", "Build and install the standalone Zig yacc generator");
    yacc_codegen_step.dependOn(&install_yacc_codegen.step);

    const yacc_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("lib/yacc/src/root.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_yacc_tests = b.addRunArtifact(yacc_tests);
    const yacc_test_step = b.step("yacc-test", "Run standalone lib/yacc parser generator tests");
    yacc_test_step.dependOn(&run_yacc_tests.step);

    const regen_run = b.addRunArtifact(yacc_codegen);
    regen_run.addFileArg(b.path(sql_grammar_source));
    const regen_output = regen_run.addOutputFileArg("regen_sql_grammar_root.zig");
    regen_run.addArg(sql_grammar_source);
    const update = b.addUpdateSourceFiles();
    update.addCopyFileToSource(regen_output, sql_grammar_generated_root);
    const regen_fmt = b.addSystemCommand(&.{ b.graph.zig_exe, "fmt", sql_grammar_generated_root });
    regen_fmt.step.dependOn(&update.step);
    const regen_step = b.step("regen-sql-grammar", "Regenerate checked-in Antfly SQL grammar metadata");
    regen_step.dependOn(&regen_fmt.step);

    const check_run = b.addRunArtifact(yacc_codegen);
    check_run.addFileArg(b.path(sql_grammar_source));
    const check_output = check_run.addOutputFileArg("check_sql_grammar_root.zig");
    check_run.addArg(sql_grammar_source);
    const check_fmt = b.addSystemCommand(&.{ b.graph.zig_exe, "fmt" });
    check_fmt.addFileArg(check_output);
    const compare = b.addRunArtifact(addFileCompareTool(b));
    compare.step.dependOn(&check_fmt.step);
    compare.addFileArg(check_output);
    compare.addFileArg(b.path(sql_grammar_generated_root));

    const generated_compile = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path(sql_grammar_generated_root),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_generated_compile = b.addRunArtifact(generated_compile);
    const check_step = b.step("sql-grammar-generated-check", "Check and compile the generated Antfly SQL grammar metadata");
    check_step.dependOn(&compare.step);
    check_step.dependOn(&run_generated_compile.step);

    const parser_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("lib/sql/root.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_parser_tests = b.addRunArtifact(parser_tests);
    const parser_test_step = b.step("sql-parser-test", "Run the storage-independent SQL lexer and parser tests");
    parser_test_step.dependOn(&run_parser_tests.step);

    const parser_bench = b.addExecutable(.{
        .name = "sql-parser-bench",
        .root_module = b.createModule(.{
            .root_source_file = b.path("lib/sql/parser_bench.zig"),
            .target = target,
            .optimize = .ReleaseFast,
        }),
    });
    const run_parser_bench = b.addRunArtifact(parser_bench);
    if (b.args) |args| run_parser_bench.addArgs(args);
    const parser_bench_step = b.step("sql-parser-bench", "Benchmark generated SQL parser latency, throughput, and allocations");
    parser_bench_step.dependOn(&run_parser_bench.step);

    return .{
        .run_yacc_tests = run_yacc_tests,
        .run_parser_tests = run_parser_tests,
    };
}

fn addLocalHttpxModule(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
) *std.Build.Module {
    return b.createModule(.{
        .root_source_file = b.path("lib/httpx/src/httpx.zig"),
        .target = target,
        .optimize = optimize,
    });
}

fn setStripRecursively(module: *std.Build.Module, visited: *std.AutoHashMap(*std.Build.Module, void)) void {
    const result = visited.getOrPut(module) catch @panic("OOM");
    if (result.found_existing) return;

    module.strip = true;
    for (module.import_table.values()) |imported_module| {
        setStripRecursively(imported_module, visited);
    }
}

const AntflyRootImports = struct {
    build_options: *std.Build.Step.Options,
    lmdb_engine: *std.Build.Module,
    raft_engine: *std.Build.Module,
    public_openapi: *std.Build.Module,
    client_openapi: *std.Build.Module,
    schema_openapi: *std.Build.Module,
    indexes_openapi: *std.Build.Module,
    sort_openapi: *std.Build.Module,
    generating_api_openapi: *std.Build.Module,
    eval_openapi: *std.Build.Module,
    query_openapi: *std.Build.Module,
    admin_openapi: *std.Build.Module,
    internal_openapi: *std.Build.Module,
    metadata_openapi: *std.Build.Module,
    usermgr_openapi: *std.Build.Module,
    logging_openapi: *std.Build.Module,
    audio_openapi: *std.Build.Module,
    middleware_openapi: *std.Build.Module,
    scraping_openapi: *std.Build.Module,
    scraping: *std.Build.Module,
    s3_openapi: *std.Build.Module,
    inference_config_openapi: *std.Build.Module,
    chunking_api_openapi: *std.Build.Module,
    chunking_openapi: *std.Build.Module,
    chunking: *std.Build.Module,
    embeddings_openapi: *std.Build.Module,
    embeddings: *std.Build.Module,
    common_openapi: *std.Build.Module,
    generating_openapi: *std.Build.Module,
    reranking_openapi: *std.Build.Module,
    extraction_openapi: *std.Build.Module,
    transcribing: *std.Build.Module,
    reader_config: *std.Build.Module,
    readers: *std.Build.Module,
    extracting: *std.Build.Module,
    synthesizing: *std.Build.Module,
    httpx: *std.Build.Module,
    google: *std.Build.Module,
    objectstore: *std.Build.Module,
    bloom: *std.Build.Module,
    vector: *std.Build.Module,
    vectorindex: *std.Build.Module,
    matcher: *std.Build.Module,
    resolver: *std.Build.Module,
    casbin: *std.Build.Module,
    vellum: *std.Build.Module,
    regex: *std.Build.Module,
    json: *std.Build.Module,
    jsonschema: *std.Build.Module,
    mcp: *std.Build.Module,
    a2a: *std.Build.Module,
    generating: *std.Build.Module,
    reranking: *std.Build.Module,
    inference_api: *std.Build.Module,
    inference_hf_tokenizer: *std.Build.Module,
    inference_fixed_tokenizer_data: *std.Build.Module,
    inference_chunker: *std.Build.Module,
    image: *std.Build.Module,
    font: *std.Build.Module,
    pdf: *std.Build.Module,
    openai_api: *std.Build.Module,
    handlebars: *std.Build.Module,
    inference_server: *std.Build.Module,
    prometheus: *std.Build.Module,
    structlog: *std.Build.Module,
    platform: *std.Build.Module,
    platform_link_libc: bool,
    platform_target: std.Build.ResolvedTarget,
    filesystem_capacity_source_file: std.Build.LazyPath,

    const import_table = [_]struct { name: []const u8, field: []const u8 }{
        .{ .name = "lmdb_engine", .field = "lmdb_engine" },
        .{ .name = "raft_engine", .field = "raft_engine" },
        .{ .name = "antfly_public_openapi", .field = "public_openapi" },
        .{ .name = "antfly_client_openapi", .field = "client_openapi" },
        .{ .name = "antfly_schema_openapi", .field = "schema_openapi" },
        .{ .name = "antfly_indexes_openapi", .field = "indexes_openapi" },
        .{ .name = "antfly_sort_openapi", .field = "sort_openapi" },
        .{ .name = "antfly_generating_api_openapi", .field = "generating_api_openapi" },
        .{ .name = "antfly_eval_openapi", .field = "eval_openapi" },
        .{ .name = "antfly_query_openapi", .field = "query_openapi" },
        .{ .name = "antfly_admin_openapi", .field = "admin_openapi" },
        .{ .name = "antfly_internal_openapi", .field = "internal_openapi" },
        .{ .name = "antfly_metadata_openapi", .field = "metadata_openapi" },
        .{ .name = "antfly_usermgr_openapi", .field = "usermgr_openapi" },
        .{ .name = "antfly_logging_openapi", .field = "logging_openapi" },
        .{ .name = "antfly_audio_openapi", .field = "audio_openapi" },
        .{ .name = "antfly_middleware_openapi", .field = "middleware_openapi" },
        .{ .name = "antfly_scraping_openapi", .field = "scraping_openapi" },
        .{ .name = "antfly_scraping", .field = "scraping" },
        .{ .name = "antfly_s3_openapi", .field = "s3_openapi" },
        .{ .name = "antfly_inference_config_openapi", .field = "inference_config_openapi" },
        .{ .name = "antfly_chunking_api_openapi", .field = "chunking_api_openapi" },
        .{ .name = "antfly_chunking_openapi", .field = "chunking_openapi" },
        .{ .name = "antfly_chunking", .field = "chunking" },
        .{ .name = "antfly_embeddings_openapi", .field = "embeddings_openapi" },
        .{ .name = "antfly_embeddings", .field = "embeddings" },
        .{ .name = "antfly_common_openapi", .field = "common_openapi" },
        .{ .name = "antfly_generating_openapi", .field = "generating_openapi" },
        .{ .name = "antfly_reranking_openapi", .field = "reranking_openapi" },
        .{ .name = "antfly_extraction_openapi", .field = "extraction_openapi" },
        .{ .name = "antfly_transcribing", .field = "transcribing" },
        .{ .name = "antfly_reader_config", .field = "reader_config" },
        .{ .name = "antfly_readers", .field = "readers" },
        .{ .name = "antfly_extracting", .field = "extracting" },
        .{ .name = "antfly_synthesizing", .field = "synthesizing" },
        .{ .name = "httpx", .field = "httpx" },
        .{ .name = "antfly_google", .field = "google" },
        .{ .name = "objectstore", .field = "objectstore" },
        .{ .name = "bloom", .field = "bloom" },
        .{ .name = "antfly_vector", .field = "vector" },
        .{ .name = "antfly_vectorindex", .field = "vectorindex" },
        .{ .name = "antfly_matcher", .field = "matcher" },
        .{ .name = "antfly_resolver", .field = "resolver" },
        .{ .name = "antfly_casbin", .field = "casbin" },
        .{ .name = "antfly_vellum", .field = "vellum" },
        .{ .name = "antfly_regex", .field = "regex" },
        .{ .name = "antfly-json", .field = "json" },
        .{ .name = "antfly_jsonschema", .field = "jsonschema" },
        .{ .name = "antfly_mcp", .field = "mcp" },
        .{ .name = "antfly_a2a", .field = "a2a" },
        .{ .name = "antfly_generating", .field = "generating" },
        .{ .name = "antfly_reranking", .field = "reranking" },
        .{ .name = "inference_api", .field = "inference_api" },
        .{ .name = "inference_hf_tokenizer", .field = "inference_hf_tokenizer" },
        .{ .name = "inference_fixed_tokenizer_data", .field = "inference_fixed_tokenizer_data" },
        .{ .name = "inference_chunker", .field = "inference_chunker" },
        .{ .name = "antfly_image", .field = "image" },
        .{ .name = "antfly_font", .field = "font" },
        .{ .name = "antfly_pdf", .field = "pdf" },
        .{ .name = "openai_api", .field = "openai_api" },
        .{ .name = "handlebars", .field = "handlebars" },
        .{ .name = "inference_server", .field = "inference_server" },
        .{ .name = "prometheus", .field = "prometheus" },
        .{ .name = "structlog", .field = "structlog" },
    };

    fn configure(self: @This(), b: *std.Build, mod: *std.Build.Module, include_lmdb_c: bool, link_libc: bool) void {
        self.configureRuntime(b, mod, include_lmdb_c, link_libc, true);
    }

    /// Install the production runtime imports while keeping the heavyweight
    /// inference server graph out of compilation units that only exchange its
    /// language-neutral bridge types. Supporting inference API, chunking,
    /// extraction, and audio modules remain available because distributed
    /// server roles genuinely use them.
    fn configureRuntime(
        self: @This(),
        b: *std.Build,
        mod: *std.Build.Module,
        include_lmdb_c: bool,
        link_libc: bool,
        include_inference_server: bool,
    ) void {
        mod.addOptions("build_options", self.build_options);
        inline for (import_table) |entry| {
            if (include_inference_server or !std.mem.eql(u8, entry.name, "inference_server")) {
                mod.addImport(entry.name, @field(self, entry.field));
            }
        }
        mod.addImport("antfly_platform", self.platform);
        if (link_libc and !self.platform_link_libc) {
            platform_build.addFilesystemCapacitySource(
                mod,
                self.filesystem_capacity_source_file,
                self.platform_target,
            );
        }
        mod.addIncludePath(b.path("lib/lmdb"));
        if (include_lmdb_c) {
            mod.addCSourceFiles(.{
                .files = &.{ "lib/lmdb/mdb.c", "lib/lmdb/midl.c" },
                .flags = &lmdb_c_flags,
            });
        }
        mod.link_libc = link_libc;
        addSnowballModule(b, mod);
    }
};

fn addSnowballModule(b: *std.Build, lib_mod: *std.Build.Module) void {
    const snowball_mod = b.addModule("snowball", .{
        .root_source_file = b.path(snowball_generated_root ++ "/root.zig"),
    });

    lib_mod.addImport("snowball", snowball_mod);
}

fn snowballGeneratedPath(b: *std.Build, comptime fmt: []const u8, args: anytype) []const u8 {
    return b.fmt(snowball_generated_root ++ "/" ++ fmt, args);
}

fn snowballRootContents(b: *std.Build) []const u8 {
    const fragments = b.allocator.alloc([]const u8, 1 + snowball_languages.len) catch @panic("OOM");
    fragments[0] =
        "pub const Env = @import(\"env.zig\").Env;\n" ++
        "pub const Among = @import(\"env.zig\").Among;\n";
    for (snowball_languages, 0..) |lang, idx| {
        fragments[1 + idx] = b.fmt("pub const {s} = @import(\"{s}_stemmer.zig\");\n", .{ lang, lang });
    }
    return std.mem.concat(b.allocator, u8, fragments) catch @panic("OOM");
}

fn addSnowballCompiler(b: *std.Build) *std.Build.Step.Compile {
    const snowball_dep = b.path("deps/snowball");

    const snowball_compiler = b.addExecutable(.{
        .name = "snowball",
        .root_module = b.createModule(.{
            .root_source_file = null,
            .target = b.graph.host,
        }),
    });
    snowball_compiler.root_module.link_libc = true;
    for (snowball_compiler_sources) |src| {
        snowball_compiler.root_module.addCSourceFiles(.{
            .root = snowball_dep,
            .files = &.{src},
            .flags = &.{ "-O2", "-W", "-Wall" },
        });
    }

    return snowball_compiler;
}

fn addFileCompareTool(b: *std.Build) *std.Build.Step.Compile {
    return b.addExecutable(.{
        .name = "check-files-equal",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tools/check_files_equal.zig"),
            .target = b.graph.host,
        }),
    });
}

fn addSnowballGeneratedOutputs(
    b: *std.Build,
    snowball_compiler: *std.Build.Step.Compile,
) struct {
    root: std.Build.LazyPath,
    env: std.Build.LazyPath,
    stemmers: [snowball_languages.len]std.Build.LazyPath,
} {
    const snowball_dep = b.path("deps/snowball");

    const wf = b.addWriteFiles();
    const root = wf.add("root.zig", snowballRootContents(b));
    const env = wf.addCopyFile(snowball_dep.path(b, "zig/env.zig"), "env.zig");

    var stemmers: [snowball_languages.len]std.Build.LazyPath = undefined;
    inline for (snowball_languages, 0..) |lang, idx| {
        const run = b.addRunArtifact(snowball_compiler);
        run.addFileArg(snowball_dep.path(b, b.fmt("algorithms/{s}.sbl", .{lang})));
        run.addArg("-zig");
        run.addArg("-o");
        stemmers[idx] = run.addOutputFileArg(b.fmt("{s}_stemmer.zig", .{lang}));
    }

    return .{
        .root = root,
        .env = env,
        .stemmers = stemmers,
    };
}

fn addSnowballRegenStep(b: *std.Build) void {
    const regen_step = b.step("regen-snowball", "Regenerate checked-in Zig Snowball stemmers");
    const snowball_compiler = addSnowballCompiler(b);
    const generated = addSnowballGeneratedOutputs(b, snowball_compiler);

    const update = b.addUpdateSourceFiles();
    update.addCopyFileToSource(generated.root, snowball_generated_root ++ "/root.zig");
    update.addCopyFileToSource(generated.env, snowball_generated_root ++ "/env.zig");
    for (snowball_languages, 0..) |lang, idx| {
        update.addCopyFileToSource(generated.stemmers[idx], snowballGeneratedPath(b, "{s}_stemmer.zig", .{lang}));
    }

    const fmt = b.addSystemCommand(&.{
        b.graph.zig_exe,
        "fmt",
        snowball_generated_root,
    });
    fmt.step.dependOn(&update.step);
    regen_step.dependOn(&fmt.step);
}

fn addSnowballCheckStep(b: *std.Build) void {
    const check_step = b.step("check-snowball", "Check checked-in Zig Snowball stemmers are current");
    const snowball_compiler = addSnowballCompiler(b);
    const generated = addSnowballGeneratedOutputs(b, snowball_compiler);

    const fmt = b.addSystemCommand(&.{
        b.graph.zig_exe,
        "fmt",
    });
    fmt.addFileArg(generated.root);
    fmt.addFileArg(generated.env);
    for (snowball_languages, 0..) |_, idx| {
        fmt.addFileArg(generated.stemmers[idx]);
    }

    const compare_tool = addFileCompareTool(b);
    const compare = b.addRunArtifact(compare_tool);
    compare.step.dependOn(&fmt.step);
    compare.addFileArg(generated.root);
    compare.addFileArg(b.path(snowball_generated_root ++ "/root.zig"));
    compare.addFileArg(generated.env);
    compare.addFileArg(b.path(snowball_generated_root ++ "/env.zig"));
    for (snowball_languages, 0..) |lang, idx| {
        compare.addFileArg(generated.stemmers[idx]);
        compare.addFileArg(b.path(snowballGeneratedPath(b, "{s}_stemmer.zig", .{lang})));
    }
    check_step.dependOn(&compare.step);
}

fn addOpenApiModuleFromYamlPath(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    openapi_codegen: *std.Build.Step.Compile,
    source_path: std.Build.LazyPath,
    package_name: []const u8,
    output_dir_name: []const u8,
    generate_what: []const u8,
    import_mappings: []const [2][]const u8,
) *std.Build.Module {
    _ = target;
    _ = optimize;

    const convert = addScriptsPythonCommand(b, "../scripts/yaml_to_json.py", &.{});
    convert.addFileArg(source_path);
    const json_spec = convert.addOutputFileArg(b.fmt("{s}.json", .{output_dir_name}));

    const codegen = b.addRunArtifact(openapi_codegen);
    codegen.addArgs(&.{"--spec"});
    codegen.addFileArg(json_spec);
    codegen.addArgs(&.{ "--package", package_name });
    codegen.addArgs(&.{ "--generate", generate_what });
    for (import_mappings) |mapping| {
        codegen.addArgs(&.{"--import-mapping"});
        codegen.addArg(b.fmt("{s}={s}", .{ mapping[0], mapping[1] }));
    }
    codegen.addArgs(&.{"--output"});
    const gen_dir = codegen.addOutputDirectoryArg(output_dir_name);

    return b.addModule(package_name, .{
        .root_source_file = gen_dir.path(b, "root.zig"),
    });
}

fn addYamlOpenApiModule(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    openapi_codegen: *std.Build.Step.Compile,
    source_path: []const u8,
    package_name: []const u8,
    output_dir_name: []const u8,
    generate_what: []const u8,
    import_mappings: []const [2][]const u8,
) *std.Build.Module {
    return addOpenApiModuleFromYamlPath(
        b,
        target,
        optimize,
        openapi_codegen,
        b.path(source_path),
        package_name,
        output_dir_name,
        generate_what,
        import_mappings,
    );
}

fn addOpenApiRootCheckStep(b: *std.Build) *std.Build.Step.Run {
    const check = addScriptsPythonCommand(b, "../scripts/join_public_openapi.py", &.{"--compare"});
    addOpenApiJoinInputs(b, check);
    check.addFileArg(b.path("../openapi.yaml"));
    return check;
}

fn addJoinedPublicOpenApiSpec(b: *std.Build) std.Build.LazyPath {
    const join = addScriptsPythonCommand(b, "../scripts/join_openapi.py", &.{"--joined-only"});
    addOpenApiJoinInputs(b, join);
    return join.addOutputFileArg("openapi.public.joined.yaml");
}

fn addPrefixedPublicOpenApiSpec(b: *std.Build) std.Build.LazyPath {
    const join = addScriptsPythonCommand(b, "../scripts/join_public_openapi.py", &.{});
    addOpenApiJoinInputs(b, join);
    return join.addOutputFileArg("openapi.public.prefixed.yaml");
}

fn addPublicOpenApiModule(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    openapi_codegen: *std.Build.Step.Compile,
) *std.Build.Module {
    return addOpenApiModuleFromYamlPath(
        b,
        target,
        optimize,
        openapi_codegen,
        addJoinedPublicOpenApiSpec(b),
        "antfly_public_openapi",
        "antfly_public_openapi",
        "types,extractors",
        &.{
            .{ "specs/openapi/antfly/schema.yaml", "antfly_schema_openapi" },
            .{ "specs/openapi/antfly/indexes.yaml", "antfly_indexes_openapi" },
            .{ "specs/openapi/antfly/sort.yaml", "antfly_sort_openapi" },
            .{ "specs/openapi/antfly/generating.yaml", "antfly_generating_api_openapi" },
            .{ "specs/openapi/antfly/eval.yaml", "antfly_eval_openapi" },
            .{ "specs/openapi/shared/generating.yaml", "antfly_generating_openapi" },
            .{ "specs/openapi/antfly/reranking.yaml", "antfly_reranking_openapi" },
            .{ "specs/openapi/antfly/query.yaml", "antfly_query_openapi" },
        },
    );
}

fn addPublicClientOpenApiModule(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    openapi_codegen: *std.Build.Step.Compile,
    httpx_mod: *std.Build.Module,
) *std.Build.Module {
    return addOpenApiModuleWithHttpxFromYamlPath(
        b,
        target,
        optimize,
        openapi_codegen,
        addPrefixedPublicOpenApiSpec(b),
        "antfly_client_openapi",
        "antfly_client_openapi",
        "types,client",
        &.{
            .{ "specs/openapi/antfly/schema.yaml", "antfly_schema_openapi" },
            .{ "specs/openapi/antfly/indexes.yaml", "antfly_indexes_openapi" },
            .{ "specs/openapi/antfly/sort.yaml", "antfly_sort_openapi" },
            .{ "specs/openapi/antfly/generating.yaml", "antfly_generating_api_openapi" },
            .{ "specs/openapi/antfly/eval.yaml", "antfly_eval_openapi" },
            .{ "specs/openapi/shared/generating.yaml", "antfly_generating_openapi" },
            .{ "specs/openapi/antfly/reranking.yaml", "antfly_reranking_openapi" },
            .{ "specs/openapi/antfly/query.yaml", "antfly_query_openapi" },
        },
        httpx_mod,
    );
}

/// Like addYamlOpenApiModule but also wires in httpx for client generation.
fn addOpenApiModuleWithHttpxFromYamlPath(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    openapi_codegen: *std.Build.Step.Compile,
    source_path: std.Build.LazyPath,
    package_name: []const u8,
    output_dir_name: []const u8,
    generate_what: []const u8,
    import_mappings: []const [2][]const u8,
    httpx_mod: *std.Build.Module,
) *std.Build.Module {
    _ = target;
    _ = optimize;

    const convert = addScriptsPythonCommand(b, "../scripts/yaml_to_json.py", &.{});
    convert.addFileArg(source_path);
    const json_spec = convert.addOutputFileArg(b.fmt("{s}.json", .{output_dir_name}));

    const codegen = b.addRunArtifact(openapi_codegen);
    codegen.addArgs(&.{"--spec"});
    codegen.addFileArg(json_spec);
    codegen.addArgs(&.{ "--package", package_name });
    codegen.addArgs(&.{ "--generate", generate_what });
    for (import_mappings) |mapping| {
        codegen.addArgs(&.{"--import-mapping"});
        codegen.addArg(b.fmt("{s}={s}", .{ mapping[0], mapping[1] }));
    }
    codegen.addArgs(&.{"--output"});
    const gen_dir = codegen.addOutputDirectoryArg(output_dir_name);

    const mod = b.addModule(package_name, .{
        .root_source_file = gen_dir.path(b, "root.zig"),
    });
    mod.addImport("httpx", httpx_mod);
    return mod;
}

fn addYamlOpenApiModuleWithHttpx(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    openapi_codegen: *std.Build.Step.Compile,
    source_path: []const u8,
    package_name: []const u8,
    output_dir_name: []const u8,
    generate_what: []const u8,
    import_mappings: []const [2][]const u8,
    httpx_mod: *std.Build.Module,
) *std.Build.Module {
    return addOpenApiModuleWithHttpxFromYamlPath(
        b,
        target,
        optimize,
        openapi_codegen,
        b.path(source_path),
        package_name,
        output_dir_name,
        generate_what,
        import_mappings,
        httpx_mod,
    );
}

fn addCommittedOpenApiModule(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    package_name: []const u8,
    generated_dir: []const u8,
) *std.Build.Module {
    return b.addModule(package_name, .{
        .root_source_file = b.path(b.fmt("{s}/root.zig", .{generated_dir})),
        .target = target,
        .optimize = optimize,
    });
}

fn addCommittedOpenApiModuleWithHttpx(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    package_name: []const u8,
    generated_dir: []const u8,
    httpx_mod: *std.Build.Module,
) *std.Build.Module {
    const mod = addCommittedOpenApiModule(b, target, optimize, package_name, generated_dir);
    mod.addImport("httpx", httpx_mod);
    return mod;
}

fn addOpenApiRegenRun(
    b: *std.Build,
    openapi_codegen: *std.Build.Step.Compile,
    source_path: std.Build.LazyPath,
    package_name: []const u8,
    generated_dir: []const u8,
    generate_what: []const u8,
    import_mappings: []const [2][]const u8,
) *std.Build.Step.Run {
    const convert = addScriptsPythonCommand(b, "../scripts/yaml_to_json.py", &.{});
    convert.addFileArg(source_path);
    const json_spec = convert.addOutputFileArg(b.fmt("{s}.json", .{package_name}));

    const codegen = b.addRunArtifact(openapi_codegen);
    codegen.addArgs(&.{"--spec"});
    codegen.addFileArg(json_spec);
    codegen.addArgs(&.{ "--package", package_name });
    codegen.addArgs(&.{ "--generate", generate_what });
    for (import_mappings) |mapping| {
        codegen.addArgs(&.{"--import-mapping"});
        codegen.addArg(b.fmt("{s}={s}", .{ mapping[0], mapping[1] }));
    }
    codegen.addArgs(&.{ "--output", generated_dir });
    return codegen;
}

fn addOpenApiRegenStep(
    b: *std.Build,
    openapi_codegen: *std.Build.Step.Compile,
) void {
    const regen_step = b.step("regen-openapi", "Regenerate checked-in Zig OpenAPI modules");

    const antfly_generated_root = "pkg/antfly/src/openapi/generated";
    const inference_generated_root = "pkg/inference/src/api/generated";
    const runs = [_]*std.Build.Step.Run{
        addOpenApiRegenRun(b, openapi_codegen, addJoinedPublicOpenApiSpec(b), "antfly_public_openapi", antfly_generated_root ++ "/antfly_public_openapi", "types,extractors", &.{
            .{ "specs/openapi/antfly/schema.yaml", "antfly_schema_openapi" },
            .{ "specs/openapi/antfly/indexes.yaml", "antfly_indexes_openapi" },
            .{ "specs/openapi/antfly/sort.yaml", "antfly_sort_openapi" },
            .{ "specs/openapi/antfly/generating.yaml", "antfly_generating_api_openapi" },
            .{ "specs/openapi/antfly/eval.yaml", "antfly_eval_openapi" },
            .{ "specs/openapi/shared/generating.yaml", "antfly_generating_openapi" },
            .{ "specs/openapi/antfly/reranking.yaml", "antfly_reranking_openapi" },
            .{ "specs/openapi/antfly/query.yaml", "antfly_query_openapi" },
        }),
        addOpenApiRegenRun(b, openapi_codegen, addPrefixedPublicOpenApiSpec(b), "antfly_client_openapi", antfly_generated_root ++ "/antfly_client_openapi", "types,client", &.{
            .{ "specs/openapi/antfly/schema.yaml", "antfly_schema_openapi" },
            .{ "specs/openapi/antfly/indexes.yaml", "antfly_indexes_openapi" },
            .{ "specs/openapi/antfly/sort.yaml", "antfly_sort_openapi" },
            .{ "specs/openapi/antfly/generating.yaml", "antfly_generating_api_openapi" },
            .{ "specs/openapi/antfly/eval.yaml", "antfly_eval_openapi" },
            .{ "specs/openapi/shared/generating.yaml", "antfly_generating_openapi" },
            .{ "specs/openapi/antfly/reranking.yaml", "antfly_reranking_openapi" },
            .{ "specs/openapi/antfly/query.yaml", "antfly_query_openapi" },
        }),
        addOpenApiRegenRun(b, openapi_codegen, b.path("../specs/openapi/antfly/schema.yaml"), "antfly_schema_openapi", antfly_generated_root ++ "/antfly_schema_openapi", "types", &.{}),
        addOpenApiRegenRun(b, openapi_codegen, b.path("../specs/openapi/antfly/sort.yaml"), "antfly_sort_openapi", antfly_generated_root ++ "/antfly_sort_openapi", "types", &.{}),
        addOpenApiRegenRun(b, openapi_codegen, b.path("../specs/openapi/antfly/indexes.yaml"), "antfly_indexes_openapi", antfly_generated_root ++ "/antfly_indexes_openapi", "types", &.{
            .{ "sort.yaml", "antfly_sort_openapi" },
            .{ "embeddings.yaml", "antfly_embeddings_openapi" },
            .{ "../shared/generating.yaml", "antfly_generating_openapi" },
            .{ "chunking.yaml", "antfly_chunking_openapi" },
        }),
        addOpenApiRegenRun(b, openapi_codegen, b.path("../specs/openapi/antfly/websearch.yaml"), "antfly_websearch_openapi", antfly_generated_root ++ "/antfly_websearch_openapi", "types", &.{
            .{ "../shared/s3.yaml", "antfly_s3_openapi" },
        }),
        addOpenApiRegenRun(b, openapi_codegen, b.path("../specs/openapi/antfly/eval.yaml"), "antfly_eval_openapi", antfly_generated_root ++ "/antfly_eval_openapi", "types", &.{
            .{ "../shared/generating.yaml", "antfly_generating_openapi" },
        }),
        addOpenApiRegenRun(b, openapi_codegen, b.path("../specs/openapi/antfly/query.yaml"), "antfly_query_openapi", antfly_generated_root ++ "/antfly_query_openapi", "types", &.{}),
        addOpenApiRegenRun(b, openapi_codegen, b.path("../specs/openapi/antfly/admin.yaml"), "antfly_admin_openapi", antfly_generated_root ++ "/antfly_admin_openapi", "types,server", &.{}),
        addOpenApiRegenRun(b, openapi_codegen, b.path("../specs/openapi/antfly/internal.yaml"), "antfly_internal_openapi", antfly_generated_root ++ "/antfly_internal_openapi", "types,server", &.{}),
        addOpenApiRegenRun(b, openapi_codegen, b.path("../specs/openapi/auth/api.yaml"), "antfly_usermgr_openapi", antfly_generated_root ++ "/antfly_usermgr_openapi", "types,server", &.{}),
        addOpenApiRegenRun(b, openapi_codegen, b.path("../specs/openapi/antfly/metadata.yaml"), "antfly_metadata_openapi", antfly_generated_root ++ "/antfly_metadata_openapi", "types,server", &.{
            .{ "../auth/api.yaml", "antfly_usermgr_openapi" },
            .{ "indexes.yaml", "antfly_indexes_openapi" },
            .{ "sort.yaml", "antfly_sort_openapi" },
            .{ "schema.yaml", "antfly_schema_openapi" },
            .{ "generating.yaml", "antfly_generating_api_openapi" },
            .{ "eval.yaml", "antfly_eval_openapi" },
            .{ "../shared/generating.yaml", "antfly_generating_openapi" },
            .{ "reranking.yaml", "antfly_reranking_openapi" },
            .{ "query.yaml", "antfly_query_openapi" },
        }),
        addOpenApiRegenRun(b, openapi_codegen, b.path("../specs/openapi/shared/logging.yaml"), "antfly_logging_openapi", antfly_generated_root ++ "/antfly_logging_openapi", "types", &.{}),
        addOpenApiRegenRun(b, openapi_codegen, b.path("../specs/openapi/antfly/audio.yaml"), "antfly_audio_openapi", antfly_generated_root ++ "/antfly_audio_openapi", "types", &.{
            .{ "../shared/s3.yaml", "antfly_s3_openapi" },
        }),
        addOpenApiRegenRun(b, openapi_codegen, b.path("../specs/openapi/shared/middleware.yaml"), "antfly_middleware_openapi", antfly_generated_root ++ "/antfly_middleware_openapi", "types", &.{}),
        addOpenApiRegenRun(b, openapi_codegen, b.path("../specs/openapi/shared/scraping.yaml"), "antfly_scraping_openapi", antfly_generated_root ++ "/antfly_scraping_openapi", "types", &.{}),
        addOpenApiRegenRun(b, openapi_codegen, b.path("../specs/openapi/shared/s3.yaml"), "antfly_s3_openapi", antfly_generated_root ++ "/antfly_s3_openapi", "types", &.{}),
        addOpenApiRegenRun(b, openapi_codegen, b.path("../specs/openapi/inference/config.yaml"), "antfly_inference_config_openapi", antfly_generated_root ++ "/antfly_inference_config_openapi", "types", &.{
            .{ "../shared/chunking.yaml", "antfly_chunking_api_openapi" },
            .{ "../shared/scraping.yaml", "antfly_scraping_openapi" },
            .{ "../shared/s3.yaml", "antfly_s3_openapi" },
            .{ "../shared/logging.yaml", "antfly_logging_openapi" },
            .{ "../shared/generating.yaml", "antfly_generating_openapi" },
        }),
        addOpenApiRegenRun(b, openapi_codegen, b.path("../specs/openapi/shared/chunking.yaml"), "antfly_chunking_api_openapi", antfly_generated_root ++ "/antfly_chunking_api_openapi", "types", &.{
            .{ "generating.yaml", "antfly_generating_openapi" },
        }),
        addOpenApiRegenRun(b, openapi_codegen, b.path("../specs/openapi/antfly/chunking.yaml"), "antfly_chunking_openapi", antfly_generated_root ++ "/antfly_chunking_openapi", "types", &.{
            .{ "../shared/chunking.yaml", "antfly_chunking_api_openapi" },
        }),
        addOpenApiRegenRun(b, openapi_codegen, b.path("../specs/openapi/antfly/embeddings.yaml"), "antfly_embeddings_openapi", antfly_generated_root ++ "/antfly_embeddings_openapi", "types", &.{}),
        addOpenApiRegenRun(b, openapi_codegen, b.path("../specs/openapi/antfly/config.yaml"), "antfly_common_openapi", antfly_generated_root ++ "/antfly_common_openapi", "types", &.{
            .{ "../shared/logging.yaml", "antfly_logging_openapi" },
            .{ "audio.yaml", "antfly_audio_openapi" },
            .{ "../shared/middleware.yaml", "antfly_middleware_openapi" },
            .{ "embeddings.yaml", "antfly_embeddings_openapi" },
            .{ "../shared/generating.yaml", "antfly_generating_openapi" },
            .{ "reranking.yaml", "antfly_reranking_openapi" },
            .{ "chunking.yaml", "antfly_chunking_openapi" },
            .{ "../shared/scraping.yaml", "antfly_scraping_openapi" },
            .{ "../shared/s3.yaml", "antfly_s3_openapi" },
            .{ "../inference/config.yaml", "antfly_inference_config_openapi" },
        }),
        addOpenApiRegenRun(b, openapi_codegen, b.path("../specs/openapi/shared/generating.yaml"), "antfly_generating_openapi", antfly_generated_root ++ "/antfly_generating_openapi", "types", &.{}),
        addOpenApiRegenRun(b, openapi_codegen, b.path("../specs/openapi/antfly/reranking.yaml"), "antfly_reranking_openapi", antfly_generated_root ++ "/antfly_reranking_openapi", "types", &.{}),
        addOpenApiRegenRun(b, openapi_codegen, b.path("../specs/openapi/ai/extraction.yaml"), "antfly_extraction_openapi", antfly_generated_root ++ "/antfly_extraction_openapi", "types", &.{
            .{ "../shared/generating.yaml", "antfly_generating_openapi" },
        }),
        addOpenApiRegenRun(b, openapi_codegen, b.path("../specs/openapi/antfly/generating.yaml"), "antfly_generating_api_openapi", antfly_generated_root ++ "/antfly_generating_api_openapi", "types", &.{
            .{ "../shared/generating.yaml", "antfly_generating_openapi" },
            .{ "websearch.yaml", "antfly_websearch_openapi" },
        }),
        addOpenApiRegenRun(b, openapi_codegen, b.path("../specs/openapi/inference/api.yaml"), "inference_api", inference_generated_root ++ "/inference_api", "types,server", &.{
            .{ "../shared/generating.yaml", "antfly_generating_openapi" },
            .{ "../shared/chunking.yaml", "antfly_chunking_api_openapi" },
            .{ "../ai/extraction.yaml", "antfly_extraction_openapi" },
        }),
        addOpenApiRegenRun(b, openapi_codegen, b.path("specs/openai-openapi.yaml"), "openai_api", antfly_generated_root ++ "/openai_api", "types", &.{}),
    };

    const fmt = b.addSystemCommand(&.{
        b.graph.zig_exe,
        "fmt",
        antfly_generated_root,
        inference_generated_root,
    });
    for (runs) |run| {
        fmt.step.dependOn(&run.step);
    }
    regen_step.dependOn(&fmt.step);
}

pub fn build(b: *std.Build) void {
    // On Linux, an implicit native target can cause Zig 0.16.0 to discover and
    // link against the host distro's crt startup objects. Newer glibc/binutils
    // builds may include .sframe sections with relocation types that Zig's
    // linker cannot yet handle. Defaulting Linux builds to an explicit GNU
    // target keeps user-supplied -Dtarget overrides intact while making the
    // no-argument path use Zig's bundled libc startup objects.
    const default_target: std.Target.Query = if (builtin.os.tag == .linux)
        .{
            .cpu_arch = builtin.cpu.arch,
            .os_tag = .linux,
            .abi = .gnu,
        }
    else
        .{};
    const target = b.standardTargetOptions(.{ .default_target = default_target });
    const optimize = b.standardOptimizeOption(.{});
    const strip = b.option(bool, "strip", "Omit debug information from release artifacts") orelse false;
    const wasm_target = b.resolveTargetQuery(.{
        .cpu_arch = .wasm32,
        .os_tag = .freestanding,
        .cpu_features_add = std.Target.wasm.featureSet(&.{ .atomics, .bulk_memory, .simd128 }),
    });
    const lmdb_backend = b.option(LmdbBackend, "lmdb_backend", "Select the LMDB backend scaffold (c or zig)") orelse .zig;
    const lmdb_evented_async_io = b.option(bool, "lmdb_evented_async_io", "Use std.Io.Evented for the Zig LMDB async_io backend") orelse false;
    const with_tla = b.option(bool, "with_tla", "Enable TLA+ trace instrumentation (ndjson event logging)") orelse false;
    const link_libc = b.option(bool, "link-libc", "Link Antfly runtime modules against libc") orelse true;
    const sanitize_thread = b.option(bool, "sanitize-thread", "Enable ThreadSanitizer for the Antfly runtime") orelse false;
    const runtime_artifact_role = b.option(RuntimeArtifactRole, "runtime-artifact-role", "Build one focused runtime artifact: cli, data, inference, metadata, or standalone");
    const antfly_bin_name = b.option([]const u8, "antfly-bin-name", "Installed filename for the top-level Antfly CLI") orelse "antfly";
    if (antfly_bin_name.len == 0 or std.mem.indexOfAny(u8, antfly_bin_name, "/\\") != null) {
        @panic("-Dantfly-bin-name must be a non-empty filename, not a path");
    }
    if (!link_libc and lmdb_backend == .c) {
        @panic("-Dlink-libc=false requires -Dlmdb_backend=zig");
    }
    const inference_onnx_option = b.option(bool, "onnx", "Enable ONNX Runtime support for embedded inference");
    const inference_enable_onnx = if (link_libc)
        inference_onnx_option orelse false
    else
        false;
    const inference_onnx_root_opt = b.option([]const u8, "onnx-root", "Path to ONNX Runtime root for embedded inference");
    const inference_onnx_root = inference_onnx_root_opt orelse defaultInferenceOnnxRoot(b, target);
    const inference_enable_metal = if (link_libc)
        b.option(bool, "metal", "Enable Apple Metal kernels for embedded inference") orelse (target.result.os.tag == .macos)
    else
        false;
    const inference_enable_cuda = b.option(bool, "cuda", "Enable CUDA inference support through the NVIDIA Driver API") orelse false;
    const inference_cuda_artifacts = b.option([]const u8, "cuda-artifacts", "CUDA artifact bundle: fatbin SASS+PTX, portable PTX, or sm89 cubin") orelse "fatbin";
    if (!std.mem.eql(u8, inference_cuda_artifacts, "portable") and !std.mem.eql(u8, inference_cuda_artifacts, "fatbin") and !std.mem.eql(u8, inference_cuda_artifacts, "sm89")) {
        @panic("invalid -Dcuda-artifacts (expected portable, fatbin, or sm89)");
    }
    const inference_enable_pjrt = if (link_libc)
        b.option(bool, "pjrt", "Enable PJRT inference support through runtime-loaded plugins") orelse false
    else
        false;
    const inference_blas_root_opt = b.option([]const u8, "blas-root", "Path to system BLAS root with include/ and lib/ for non-macOS native acceleration");
    const inference_system_blas_available = link_libc and (target.result.os.tag == .macos or inference_blas_root_opt != null);
    const inference_enable_system_blas = if (link_libc)
        b.option(bool, "system-blas", "Enable system BLAS acceleration for native CPU math") orelse inference_system_blas_available
    else
        false;
    const inference_blas_root = if (inference_enable_system_blas and target.result.os.tag != .macos)
        inference_blas_root_opt
    else
        null;
    const antfly_version = b.option([]const u8, "antfly-version", "Antfly version string") orelse "dev";
    const lite_local_inference_runtime = b.option(bool, "lite-local-inference-runtime", "Advertise an embedded local inference runtime in Antfly Lite status") orelse false;
    if (inference_enable_onnx) {
        const inference_onnx_available = pathExists(b, b.fmt("{s}/include/onnxruntime_c_api.h", .{inference_onnx_root})) and
            pathExists(b, b.fmt("{s}/lib", .{inference_onnx_root}));
        if (!inference_onnx_available) {
            @panic("-Donnx=true requires an ONNX Runtime install; pass -Donnx-root=<path>");
        }
    }
    const delegated_inference_steps = addDelegatedInferenceBuildSteps(
        b,
        inference_enable_metal,
        inference_enable_onnx,
        inference_onnx_root,
        inference_enable_cuda,
        inference_cuda_artifacts,
        inference_enable_pjrt,
        inference_enable_system_blas,
        inference_blas_root,
    );

    const lmdb_build_options = makeLmdbBuildOptions(b, lmdb_backend, lmdb_evented_async_io, false);
    const build_options = makeRootBuildOptions(b, lmdb_backend, lmdb_evented_async_io, false, with_tla, link_libc, false, lite_local_inference_runtime, true, antfly_version);
    const standalone_runtime_build_options = makeRootBuildOptions(b, lmdb_backend, lmdb_evented_async_io, false, with_tla, link_libc, true, lite_local_inference_runtime, true, antfly_version);
    const production_build_options = makeRootBuildOptions(b, lmdb_backend, lmdb_evented_async_io, false, with_tla, link_libc, false, lite_local_inference_runtime, false, antfly_version);
    const lmdb_engine_mod = makeLmdbEngineModule(b, target, optimize, link_libc, lmdb_build_options);
    const lmdb_engine_wasm_mod = makeLmdbEngineModule(b, wasm_target, optimize, false, lmdb_build_options);
    const raft_engine_mod = b.createModule(.{
        .root_source_file = b.path("lib/raft/src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    const httpx_mod = addLocalHttpxModule(b, target, optimize);
    const prometheus_mod = b.createModule(.{
        .root_source_file = b.path("lib/prometheus/src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    const structlog_mod = b.createModule(.{
        .root_source_file = b.path("lib/structlog/src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    addSnowballRegenStep(b);
    addSnowballCheckStep(b);
    const openapi_codegen = addLocalOpenApiCodegen(b, target, optimize, httpx_mod);
    addOpenApiRegenStep(b, openapi_codegen);
    const yacc_steps = addYaccSteps(b, target, optimize);
    const openapi_root_check = addOpenApiRootCheckStep(b);
    const antfly_generated_root = "pkg/antfly/src/openapi/generated";
    const public_openapi_mod = addCommittedOpenApiModule(b, target, optimize, "antfly_public_openapi", antfly_generated_root ++ "/antfly_public_openapi");
    const client_openapi_mod = addCommittedOpenApiModuleWithHttpx(b, target, optimize, "antfly_client_openapi", antfly_generated_root ++ "/antfly_client_openapi", httpx_mod);
    const schema_openapi_mod = addCommittedOpenApiModule(b, target, optimize, "antfly_schema_openapi", antfly_generated_root ++ "/antfly_schema_openapi");
    const indexes_openapi_mod = addCommittedOpenApiModule(b, target, optimize, "antfly_indexes_openapi", antfly_generated_root ++ "/antfly_indexes_openapi");
    const sort_openapi_mod = addCommittedOpenApiModule(b, target, optimize, "antfly_sort_openapi", antfly_generated_root ++ "/antfly_sort_openapi");
    const websearch_openapi_mod = addCommittedOpenApiModule(b, target, optimize, "antfly_websearch_openapi", antfly_generated_root ++ "/antfly_websearch_openapi");
    const eval_openapi_mod = addCommittedOpenApiModule(b, target, optimize, "antfly_eval_openapi", antfly_generated_root ++ "/antfly_eval_openapi");
    const query_openapi_mod = addCommittedOpenApiModule(b, target, optimize, "antfly_query_openapi", antfly_generated_root ++ "/antfly_query_openapi");
    const admin_openapi_mod = addCommittedOpenApiModuleWithHttpx(b, target, optimize, "antfly_admin_openapi", antfly_generated_root ++ "/antfly_admin_openapi", httpx_mod);
    const internal_openapi_mod = addCommittedOpenApiModuleWithHttpx(b, target, optimize, "antfly_internal_openapi", antfly_generated_root ++ "/antfly_internal_openapi", httpx_mod);
    const usermgr_openapi_mod = addCommittedOpenApiModuleWithHttpx(b, target, optimize, "antfly_usermgr_openapi", antfly_generated_root ++ "/antfly_usermgr_openapi", httpx_mod);
    const metadata_openapi_mod = addCommittedOpenApiModuleWithHttpx(b, target, optimize, "antfly_metadata_openapi", antfly_generated_root ++ "/antfly_metadata_openapi", httpx_mod);
    const logging_openapi_mod = addCommittedOpenApiModule(b, target, optimize, "antfly_logging_openapi", antfly_generated_root ++ "/antfly_logging_openapi");
    const audio_openapi_mod = addCommittedOpenApiModule(b, target, optimize, "antfly_audio_openapi", antfly_generated_root ++ "/antfly_audio_openapi");
    const middleware_openapi_mod = addCommittedOpenApiModule(b, target, optimize, "antfly_middleware_openapi", antfly_generated_root ++ "/antfly_middleware_openapi");
    const scraping_openapi_mod = addCommittedOpenApiModule(b, target, optimize, "antfly_scraping_openapi", antfly_generated_root ++ "/antfly_scraping_openapi");
    const s3_openapi_mod = addCommittedOpenApiModule(b, target, optimize, "antfly_s3_openapi", antfly_generated_root ++ "/antfly_s3_openapi");
    const inference_config_openapi_mod = addCommittedOpenApiModule(b, target, optimize, "antfly_inference_config_openapi", antfly_generated_root ++ "/antfly_inference_config_openapi");
    const chunking_api_openapi_mod = addCommittedOpenApiModule(b, target, optimize, "antfly_chunking_api_openapi", antfly_generated_root ++ "/antfly_chunking_api_openapi");
    const chunking_openapi_mod = addCommittedOpenApiModule(b, target, optimize, "antfly_chunking_openapi", antfly_generated_root ++ "/antfly_chunking_openapi");
    const embeddings_openapi_mod = addCommittedOpenApiModule(b, target, optimize, "antfly_embeddings_openapi", antfly_generated_root ++ "/antfly_embeddings_openapi");
    const common_openapi_mod = addCommittedOpenApiModule(b, target, optimize, "antfly_common_openapi", antfly_generated_root ++ "/antfly_common_openapi");
    const generating_openapi_mod = addCommittedOpenApiModule(b, target, optimize, "antfly_generating_openapi", antfly_generated_root ++ "/antfly_generating_openapi");
    const reranking_openapi_mod = addCommittedOpenApiModule(b, target, optimize, "antfly_reranking_openapi", antfly_generated_root ++ "/antfly_reranking_openapi");
    const generating_api_openapi_mod = addCommittedOpenApiModule(b, target, optimize, "antfly_generating_api_openapi", antfly_generated_root ++ "/antfly_generating_api_openapi");
    const extraction_openapi_mod = addCommittedOpenApiModule(b, target, optimize, "antfly_extraction_openapi", antfly_generated_root ++ "/antfly_extraction_openapi");
    extraction_openapi_mod.addImport("antfly_generating_openapi", generating_openapi_mod);
    indexes_openapi_mod.addImport("antfly_embeddings_openapi", embeddings_openapi_mod);
    indexes_openapi_mod.addImport("antfly_generating_openapi", generating_openapi_mod);
    indexes_openapi_mod.addImport("antfly_chunking_openapi", chunking_openapi_mod);
    indexes_openapi_mod.addImport("antfly_sort_openapi", sort_openapi_mod);
    websearch_openapi_mod.addImport("antfly_s3_openapi", s3_openapi_mod);
    eval_openapi_mod.addImport("antfly_generating_openapi", generating_openapi_mod);
    generating_api_openapi_mod.addImport("antfly_generating_openapi", generating_openapi_mod);
    generating_api_openapi_mod.addImport("antfly_websearch_openapi", websearch_openapi_mod);
    public_openapi_mod.addImport("antfly_schema_openapi", schema_openapi_mod);
    public_openapi_mod.addImport("antfly_indexes_openapi", indexes_openapi_mod);
    public_openapi_mod.addImport("antfly_sort_openapi", sort_openapi_mod);
    public_openapi_mod.addImport("antfly_generating_api_openapi", generating_api_openapi_mod);
    public_openapi_mod.addImport("antfly_eval_openapi", eval_openapi_mod);
    public_openapi_mod.addImport("antfly_generating_openapi", generating_openapi_mod);
    public_openapi_mod.addImport("antfly_reranking_openapi", reranking_openapi_mod);
    public_openapi_mod.addImport("antfly_query_openapi", query_openapi_mod);
    client_openapi_mod.addImport("antfly_schema_openapi", schema_openapi_mod);
    client_openapi_mod.addImport("antfly_indexes_openapi", indexes_openapi_mod);
    client_openapi_mod.addImport("antfly_sort_openapi", sort_openapi_mod);
    client_openapi_mod.addImport("antfly_generating_api_openapi", generating_api_openapi_mod);
    client_openapi_mod.addImport("antfly_eval_openapi", eval_openapi_mod);
    client_openapi_mod.addImport("antfly_generating_openapi", generating_openapi_mod);
    client_openapi_mod.addImport("antfly_reranking_openapi", reranking_openapi_mod);
    client_openapi_mod.addImport("antfly_query_openapi", query_openapi_mod);
    metadata_openapi_mod.addImport("antfly_usermgr_openapi", usermgr_openapi_mod);
    metadata_openapi_mod.addImport("antfly_indexes_openapi", indexes_openapi_mod);
    metadata_openapi_mod.addImport("antfly_sort_openapi", sort_openapi_mod);
    metadata_openapi_mod.addImport("antfly_schema_openapi", schema_openapi_mod);
    metadata_openapi_mod.addImport("antfly_generating_api_openapi", generating_api_openapi_mod);
    metadata_openapi_mod.addImport("antfly_eval_openapi", eval_openapi_mod);
    metadata_openapi_mod.addImport("antfly_generating_openapi", generating_openapi_mod);
    metadata_openapi_mod.addImport("antfly_reranking_openapi", reranking_openapi_mod);
    metadata_openapi_mod.addImport("antfly_query_openapi", query_openapi_mod);
    chunking_api_openapi_mod.addImport("antfly_generating_openapi", generating_openapi_mod);
    chunking_openapi_mod.addImport("antfly_chunking_api_openapi", chunking_api_openapi_mod);
    audio_openapi_mod.addImport("antfly_s3_openapi", s3_openapi_mod);
    inference_config_openapi_mod.addImport("antfly_chunking_api_openapi", chunking_api_openapi_mod);
    inference_config_openapi_mod.addImport("antfly_scraping_openapi", scraping_openapi_mod);
    inference_config_openapi_mod.addImport("antfly_s3_openapi", s3_openapi_mod);
    inference_config_openapi_mod.addImport("antfly_logging_openapi", logging_openapi_mod);
    inference_config_openapi_mod.addImport("antfly_generating_openapi", generating_openapi_mod);
    common_openapi_mod.addImport("antfly_logging_openapi", logging_openapi_mod);
    common_openapi_mod.addImport("antfly_audio_openapi", audio_openapi_mod);
    common_openapi_mod.addImport("antfly_middleware_openapi", middleware_openapi_mod);
    common_openapi_mod.addImport("antfly_embeddings_openapi", embeddings_openapi_mod);
    common_openapi_mod.addImport("antfly_generating_openapi", generating_openapi_mod);
    common_openapi_mod.addImport("antfly_reranking_openapi", reranking_openapi_mod);
    common_openapi_mod.addImport("antfly_chunking_openapi", chunking_openapi_mod);
    common_openapi_mod.addImport("antfly_scraping_openapi", scraping_openapi_mod);
    common_openapi_mod.addImport("antfly_s3_openapi", s3_openapi_mod);
    common_openapi_mod.addImport("antfly_inference_config_openapi", inference_config_openapi_mod);

    // Handlebars template engine
    const handlebars_dep = b.dependency("handlebars", .{});
    const handlebars_mod = handlebars_dep.module("handlebars");

    // Protobuf wire format
    const protobuf_dep = b.dependency("protobuf", .{});
    const protobuf_mod = protobuf_dep.module("protobuf");
    const platform_mod = platform_build.createModule(b, .{
        .root_source_file = b.path("lib/platform/src/root.zig"),
        .filesystem_capacity_source_file = b.path("lib/platform/src/filesystem_capacity.c"),
        .target = target,
        .optimize = optimize,
        .link_libc = link_libc,
    });
    const wasm_platform_mod = platform_build.createModule(b, .{
        .root_source_file = b.path("lib/platform/src/root.zig"),
        .filesystem_capacity_source_file = b.path("lib/platform/src/filesystem_capacity.c"),
        .target = wasm_target,
        .optimize = optimize,
        .link_libc = false,
    });
    const objectstore_mod = b.createModule(.{
        .root_source_file = b.path("lib/objectstore/src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    const google_mod = b.createModule(.{
        .root_source_file = b.path("lib/google/src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    google_mod.addImport("httpx", httpx_mod);
    google_mod.addImport("antfly_platform", platform_mod);
    objectstore_mod.addImport("httpx", httpx_mod);
    objectstore_mod.addImport("antfly_platform", platform_mod);
    objectstore_mod.addImport("antfly_google", google_mod);
    const wasm_objectstore_mod = b.createModule(.{
        .root_source_file = b.path("lib/objectstore/src/root.zig"),
        .target = wasm_target,
        .optimize = optimize,
    });
    const wasm_google_mod = b.createModule(.{
        .root_source_file = b.path("lib/google/src/root.zig"),
        .target = wasm_target,
        .optimize = optimize,
    });
    wasm_google_mod.addImport("httpx", httpx_mod);
    wasm_google_mod.addImport("antfly_platform", wasm_platform_mod);
    wasm_objectstore_mod.addImport("httpx", httpx_mod);
    wasm_objectstore_mod.addImport("antfly_platform", wasm_platform_mod);
    wasm_objectstore_mod.addImport("antfly_google", wasm_google_mod);
    const bloom_mod = b.createModule(.{
        .root_source_file = b.path("lib/bloom/src/mod.zig"),
        .target = target,
        .optimize = optimize,
    });
    const vector_mod = b.createModule(.{
        .root_source_file = b.path("lib/vector/src/mod.zig"),
        .target = target,
        .optimize = optimize,
    });
    vector_mod.addImport("protobuf", protobuf_mod);
    const wasm_vector_mod = b.createModule(.{
        .root_source_file = b.path("lib/vector/src/mod.zig"),
        .target = wasm_target,
        .optimize = optimize,
    });
    wasm_vector_mod.addImport("protobuf", protobuf_mod);
    const vectorindex_mod = b.createModule(.{
        .root_source_file = b.path("lib/vectorindex/src/mod.zig"),
        .target = target,
        .optimize = optimize,
    });
    vectorindex_mod.addImport("antfly_vector", vector_mod);
    vectorindex_mod.addImport("antfly_platform", platform_mod);
    if (target.result.os.tag == .macos) {
        addMacosSdkPaths(b, vectorindex_mod, target);
        vectorindex_mod.linkFramework("Foundation", .{});
        vectorindex_mod.linkFramework("Metal", .{});
        vectorindex_mod.addCSourceFile(.{ .file = b.path("lib/vectorindex/src/kmeans_metal.m"), .flags = &.{"-fobjc-arc"} });
    }
    const wasm_vectorindex_mod = b.createModule(.{
        .root_source_file = b.path("lib/vectorindex/src/mod.zig"),
        .target = wasm_target,
        .optimize = optimize,
    });
    wasm_vectorindex_mod.addImport("antfly_vector", wasm_vector_mod);
    wasm_vectorindex_mod.addImport("antfly_platform", wasm_platform_mod);
    const casbin_mod = b.createModule(.{
        .root_source_file = b.path("lib/casbin/src/mod.zig"),
        .target = target,
        .optimize = optimize,
    });
    const storage_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/storage_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    storage_mod.addImport("bloom", bloom_mod);
    storage_mod.addImport("antfly_platform", platform_mod);
    const usermgr_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/usermgr/mod.zig"),
        .target = target,
        .optimize = optimize,
    });
    usermgr_mod.link_libc = link_libc;
    usermgr_mod.addImport("antfly_casbin", casbin_mod);
    usermgr_mod.addImport("usermgr_storage", storage_mod);
    const wasm_bloom_mod = b.createModule(.{
        .root_source_file = b.path("lib/bloom/src/mod.zig"),
        .target = wasm_target,
        .optimize = optimize,
    });
    const vellum_mod = b.createModule(.{
        .root_source_file = b.path("lib/vellum/src/mod.zig"),
        .target = target,
        .optimize = optimize,
    });
    const regex_mod = b.createModule(.{
        .root_source_file = b.path("lib/regex/src/mod.zig"),
        .target = target,
        .optimize = optimize,
    });
    regex_mod.addImport("antfly_vellum", vellum_mod);
    const jsonschema_mod = b.createModule(.{
        .root_source_file = b.path("lib/jsonschema/src/mod.zig"),
        .target = target,
        .optimize = optimize,
    });
    const json_mod = b.addModule("antfly-json", .{
        .root_source_file = b.path("lib/json/src/mod.zig"),
        .target = target,
        .optimize = optimize,
    });
    const toon_mod = b.addModule("antfly_toon", .{
        .root_source_file = b.path("lib/toon/src/mod.zig"),
        .target = target,
        .optimize = optimize,
    });
    const mcp_mod = b.addModule("antfly_mcp", .{
        .root_source_file = b.path("lib/mcp/src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    mcp_mod.addImport("antfly-json", json_mod);
    const a2a_mod = b.addModule("antfly_a2a", .{
        .root_source_file = b.path("lib/a2a/src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    a2a_mod.addImport("antfly-json", json_mod);
    const matcher_mod = b.addModule("antfly_matcher", .{
        .root_source_file = b.path("lib/matcher/src/mod.zig"),
        .target = target,
        .optimize = optimize,
    });
    const resolver_mod = b.addModule("antfly_resolver", .{
        .root_source_file = b.path("lib/resolver/src/mod.zig"),
        .target = target,
        .optimize = optimize,
    });
    resolver_mod.addImport("antfly_matcher", matcher_mod);
    httpx_mod.addImport("antfly-json", json_mod);
    jsonschema_mod.addImport("antfly_regex", regex_mod);
    jsonschema_mod.addImport("antfly-json", json_mod);
    const generating_mod = b.createModule(.{
        .root_source_file = b.path("lib/generating/src/mod.zig"),
        .target = target,
        .optimize = optimize,
    });
    generating_mod.addImport("antfly-json", json_mod);
    generating_mod.addImport("antfly_generating_openapi", generating_openapi_mod);
    const chunking_mod = b.createModule(.{
        .root_source_file = b.path("lib/chunking/src/mod.zig"),
        .target = target,
        .optimize = optimize,
    });
    chunking_mod.addImport("antfly-json", json_mod);
    chunking_mod.addImport("antfly_chunking_api_openapi", chunking_api_openapi_mod);
    chunking_mod.addImport("antfly_chunking_openapi", chunking_openapi_mod);
    const embeddings_mod = b.createModule(.{
        .root_source_file = b.path("lib/embeddings/src/mod.zig"),
        .target = target,
        .optimize = optimize,
    });
    embeddings_mod.addImport("antfly-json", json_mod);
    embeddings_mod.addImport("antfly_embeddings_openapi", embeddings_openapi_mod);
    const scraping_mod = b.createModule(.{
        .root_source_file = b.path("lib/scraping/src/mod.zig"),
        .target = target,
        .optimize = optimize,
    });
    scraping_mod.addImport("objectstore", objectstore_mod);
    scraping_mod.addImport("httpx", httpx_mod);
    const reranking_mod = b.createModule(.{
        .root_source_file = b.path("lib/reranking/src/mod.zig"),
        .target = target,
        .optimize = optimize,
    });
    reranking_mod.addImport("antfly-json", json_mod);
    reranking_mod.addImport("antfly_reranking_openapi", reranking_openapi_mod);
    const extracting_mod = b.createModule(.{
        .root_source_file = b.path("lib/extracting/src/mod.zig"),
        .target = target,
        .optimize = optimize,
    });
    extracting_mod.addImport("httpx", httpx_mod);
    extracting_mod.addImport("antfly_extraction_openapi", extraction_openapi_mod);

    // Inference dependencies
    const openai_api_mod = addCommittedOpenApiModuleWithHttpx(b, target, optimize, "openai_api", antfly_generated_root ++ "/openai_api", httpx_mod);

    // --- Inference backend detection (must precede module creation) ---
    const inference_ffmpeg_paths = if (link_libc) detectFfmpegPaths(b, target) else null;
    const image_mod = b.createModule(.{
        .root_source_file = b.path("lib/image/src/mod.zig"),
        .target = target,
        .optimize = optimize,
    });
    const pdf_mod = b.createModule(.{
        .root_source_file = b.path("lib/pdf/src/mod.zig"),
        .target = target,
        .optimize = optimize,
    });
    const font_mod = b.createModule(.{
        .root_source_file = b.path("lib/font/src/mod.zig"),
        .target = target,
        .optimize = optimize,
    });
    pdf_mod.addImport("antfly_image", image_mod);
    pdf_mod.addImport("antfly_font", font_mod);
    if (target.result.os.tag == .macos) {
        addMacosSdkPaths(b, pdf_mod, target);
        pdf_mod.linkFramework("CoreFoundation", .{});
        pdf_mod.linkFramework("CoreGraphics", .{});
    }
    const wasm_image_mod = b.createModule(.{
        .root_source_file = b.path("lib/image/src/mod.zig"),
        .target = wasm_target,
        .optimize = optimize,
    });
    const wasm_pdf_mod = b.createModule(.{
        .root_source_file = b.path("lib/pdf/src/mod.zig"),
        .target = wasm_target,
        .optimize = optimize,
    });
    const wasm_font_mod = b.createModule(.{
        .root_source_file = b.path("lib/font/src/mod.zig"),
        .target = wasm_target,
        .optimize = optimize,
    });
    wasm_pdf_mod.addImport("antfly_image", wasm_image_mod);
    wasm_pdf_mod.addImport("antfly_font", wasm_font_mod);

    const sentencepiece_proto_mod = addLocalSentencePieceProtoModule(b, protobuf_dep);
    const inference_jinja_mod = b.createModule(.{
        .root_source_file = b.path("lib/jinja/src/jinja.zig"),
        .target = target,
        .optimize = optimize,
    });
    const inference_ml_mod = b.createModule(.{
        .root_source_file = b.path("lib/ml/src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    inference_ml_mod.addImport("antfly_platform", platform_mod);
    const ml_tabular_mod = b.addModule("ml_tabular", .{
        .root_source_file = b.path("lib/ml/tabular/src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    const inference_onnx_graph_mod = b.addModule("inference_onnx_graph", .{
        .root_source_file = b.path("lib/onnx/src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    inference_onnx_graph_mod.addImport("protobuf", protobuf_mod);
    inference_onnx_graph_mod.addImport("ml", inference_ml_mod);
    inference_onnx_graph_mod.addImport("structlog", structlog_mod);
    const inference_pjrt_xla_proto_mod = b.createModule(.{
        .root_source_file = b.path("lib/pjrt/proto/xla_proto_stub.zig"),
        .target = target,
        .optimize = optimize,
    });
    inference_pjrt_xla_proto_mod.addImport("protobuf", protobuf_mod);
    const inference_pjrt_mod = b.createModule(.{
        .root_source_file = b.path("lib/pjrt/src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    inference_pjrt_mod.addImport("protobuf", protobuf_mod);
    inference_pjrt_mod.addImport("xla_proto", inference_pjrt_xla_proto_mod);

    const inference_graph = inference_runtime_build.create(.{
        .b = b,
        .target = target,
        .optimize = optimize,
        .paths = .{
            .inference_root = "pkg/inference",
            .shared_lib_root = "",
        },
        .backend = .{
            .enable_onnx = inference_enable_onnx,
            .onnx_root = inference_onnx_root,
            .enable_metal = inference_enable_metal,
            .enable_cuda = inference_enable_cuda,
            .cuda_artifacts = inference_cuda_artifacts,
            .enable_pjrt = inference_enable_pjrt,
            .enable_native = true,
            .enable_system_blas = inference_enable_system_blas,
            .blas_root = inference_blas_root,
            .enable_ffmpeg_audio = inference_ffmpeg_paths != null,
            .ffmpeg_paths = if (inference_ffmpeg_paths) |paths| .{
                .include_dir = paths.include_dir,
                .lib_dir = paths.lib_dir,
            } else null,
            .link_libc = link_libc,
            .skip_openapi = false,
            .inference_version = antfly_version,
        },
        .shared = .{
            .json = json_mod,
            .httpx = httpx_mod,
            .platform = platform_mod,
            .vellum = vellum_mod,
            .scraping = scraping_mod,
            .google = google_mod,
            .objectstore = objectstore_mod,
            .regex = regex_mod,
            .jsonschema = jsonschema_mod,
            .image = image_mod,
            .prometheus = prometheus_mod,
            .structlog = structlog_mod,
            .jinja = inference_jinja_mod,
            .protobuf = protobuf_mod,
            .sentencepiece_proto = sentencepiece_proto_mod,
            .ml = inference_ml_mod,
            .ml_tabular = ml_tabular_mod,
            .onnx_graph = inference_onnx_graph_mod,
            .pjrt = inference_pjrt_mod,
            .generating_openapi = generating_openapi_mod,
            .extraction_openapi = extraction_openapi_mod,
            .extracting = extracting_mod,
        },
    });
    const inference_build_options_mod = inference_graph.build_options_mod;
    const inference_api_mod = inference_graph.inference_api_mod;
    inference_api_mod.addImport("antfly_generating_openapi", generating_openapi_mod);
    inference_api_mod.addImport("antfly_chunking_api_openapi", chunking_api_openapi_mod);
    inference_api_mod.addImport("antfly_extraction_openapi", extraction_openapi_mod);
    const inference_hf_tokenizer_mod = inference_graph.inference_hf_tokenizer_mod;
    const inference_fixed_tokenizer_data_mod = inference_graph.inference_fixed_tokenizer_data_mod;
    const inference_chunker_mod = inference_graph.inference_chunker_mod;
    const inference_server_mod = inference_graph.inference_mod;
    const hf_tokenizer_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("lib/tokenizer/src/hf_tokenizer.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
        }),
    });
    hf_tokenizer_tests.root_module.addImport(
        "sentencepiece_proto",
        sentencepiece_proto_mod,
    );
    const run_hf_tokenizer_tests = b.addRunArtifact(hf_tokenizer_tests);
    const hf_tokenizer_test_step = b.step(
        "hf-tokenizer-test",
        "Run Hugging Face tokenizer tests",
    );
    hf_tokenizer_test_step.dependOn(&run_hf_tokenizer_tests.step);

    const transcribing_mod = b.createModule(.{
        .root_source_file = b.path("lib/transcribing/src/mod.zig"),
        .target = target,
        .optimize = optimize,
    });
    transcribing_mod.addImport("antfly_audio_openapi", audio_openapi_mod);
    transcribing_mod.addImport("httpx", httpx_mod);
    transcribing_mod.addImport("inference_api", inference_api_mod);
    transcribing_mod.addImport("antfly_scraping", scraping_mod);
    transcribing_mod.addImport("antfly_google", google_mod);
    const reader_config_mod = b.createModule(.{
        .root_source_file = b.path("lib/readers/src/config.zig"),
        .target = target,
        .optimize = optimize,
    });
    const readers_mod = b.createModule(.{
        .root_source_file = b.path("lib/readers/src/mod.zig"),
        .target = target,
        .optimize = optimize,
    });
    readers_mod.addImport("httpx", httpx_mod);
    readers_mod.addImport("inference_api", inference_api_mod);
    readers_mod.addImport("antfly_google", google_mod);
    readers_mod.addImport("antfly_reader_config", reader_config_mod);
    inference_server_mod.addImport("antfly_readers", readers_mod);
    inference_server_mod.addImport("antfly_transcribing", transcribing_mod);
    inference_server_mod.addImport("antfly_extracting", extracting_mod);
    const synthesizing_mod = b.createModule(.{
        .root_source_file = b.path("lib/synthesizing/src/mod.zig"),
        .target = target,
        .optimize = optimize,
    });
    synthesizing_mod.addImport("antfly_audio_openapi", audio_openapi_mod);
    synthesizing_mod.addImport("httpx", httpx_mod);

    const antfly_imports = AntflyRootImports{
        .build_options = build_options,
        .lmdb_engine = lmdb_engine_mod,
        .raft_engine = raft_engine_mod,
        .public_openapi = public_openapi_mod,
        .client_openapi = client_openapi_mod,
        .schema_openapi = schema_openapi_mod,
        .indexes_openapi = indexes_openapi_mod,
        .sort_openapi = sort_openapi_mod,
        .generating_api_openapi = generating_api_openapi_mod,
        .eval_openapi = eval_openapi_mod,
        .query_openapi = query_openapi_mod,
        .admin_openapi = admin_openapi_mod,
        .internal_openapi = internal_openapi_mod,
        .metadata_openapi = metadata_openapi_mod,
        .usermgr_openapi = usermgr_openapi_mod,
        .logging_openapi = logging_openapi_mod,
        .audio_openapi = audio_openapi_mod,
        .middleware_openapi = middleware_openapi_mod,
        .scraping_openapi = scraping_openapi_mod,
        .scraping = scraping_mod,
        .s3_openapi = s3_openapi_mod,
        .inference_config_openapi = inference_config_openapi_mod,
        .chunking_api_openapi = chunking_api_openapi_mod,
        .chunking_openapi = chunking_openapi_mod,
        .chunking = chunking_mod,
        .embeddings_openapi = embeddings_openapi_mod,
        .embeddings = embeddings_mod,
        .common_openapi = common_openapi_mod,
        .generating_openapi = generating_openapi_mod,
        .reranking_openapi = reranking_openapi_mod,
        .extraction_openapi = extraction_openapi_mod,
        .transcribing = transcribing_mod,
        .reader_config = reader_config_mod,
        .readers = readers_mod,
        .extracting = extracting_mod,
        .synthesizing = synthesizing_mod,
        .httpx = httpx_mod,
        .google = google_mod,
        .objectstore = objectstore_mod,
        .bloom = bloom_mod,
        .vector = vector_mod,
        .vectorindex = vectorindex_mod,
        .matcher = matcher_mod,
        .resolver = resolver_mod,
        .casbin = casbin_mod,
        .vellum = vellum_mod,
        .regex = regex_mod,
        .json = json_mod,
        .jsonschema = jsonschema_mod,
        .mcp = mcp_mod,
        .a2a = a2a_mod,
        .generating = generating_mod,
        .reranking = reranking_mod,
        .inference_api = inference_api_mod,
        .inference_hf_tokenizer = inference_hf_tokenizer_mod,
        .inference_fixed_tokenizer_data = inference_fixed_tokenizer_data_mod,
        .inference_chunker = inference_chunker_mod,
        .image = image_mod,
        .font = font_mod,
        .pdf = pdf_mod,
        .openai_api = openai_api_mod,
        .handlebars = handlebars_mod,
        .inference_server = inference_server_mod,
        .prometheus = prometheus_mod,
        .structlog = structlog_mod,
        .platform = platform_mod,
        .platform_link_libc = link_libc,
        .platform_target = target,
        .filesystem_capacity_source_file = b.path("lib/platform/src/filesystem_capacity.c"),
    };
    var production_antfly_imports = antfly_imports;
    production_antfly_imports.build_options = production_build_options;

    // Library module
    const lib_mod = b.addModule("antfly-zig", .{
        .root_source_file = b.path("pkg/antfly/src/root.zig"),
        .target = target,
        .optimize = optimize,
        .sanitize_thread = sanitize_thread,
    });
    antfly_imports.configure(b, lib_mod, false, link_libc);

    const lib_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    antfly_imports.configure(b, lib_test_mod, true, true);

    const api_http_runtime_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/api_http_runtime_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    antfly_imports.configure(b, api_http_runtime_test_mod, true, true);

    const metadata_unit_test_root_paths = [_][]const u8{
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
    var metadata_unit_test_mods: [metadata_unit_test_root_paths.len]*std.Build.Module = undefined;
    for (metadata_unit_test_root_paths, &metadata_unit_test_mods) |root_path, *test_mod| {
        test_mod.* = b.createModule(.{
            .root_source_file = b.path(root_path),
            .target = target,
            .optimize = optimize,
        });
        antfly_imports.configure(b, test_mod.*, true, true);
    }

    const raft_sim_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/raft_sim_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    antfly_imports.configure(b, raft_sim_test_mod, true, true);

    const introducer_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/introducer.zig"),
        .target = target,
        .optimize = optimize,
    });
    antfly_imports.configure(b, introducer_test_mod, true, true);

    const data_runtime_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/data_runtime_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    antfly_imports.configure(b, data_runtime_test_mod, true, true);

    const raft_runtime_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/raft_runtime_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    antfly_imports.configure(b, raft_runtime_test_mod, true, true);

    const raft_restore_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/raft_restore_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    antfly_imports.configure(b, raft_restore_test_mod, true, true);

    const filesystem_capacity_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/filesystem_capacity_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    antfly_imports.configure(b, filesystem_capacity_test_mod, true, true);

    const data_storage_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/data_storage_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    antfly_imports.configure(b, data_storage_test_mod, true, true);

    const usermgr_storage_lib_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/usermgr/storage_imports.zig"),
        .target = target,
        .optimize = optimize,
    });
    usermgr_storage_lib_mod.addImport("antfly_root", lib_mod);
    usermgr_storage_lib_mod.addImport("antfly_platform", platform_mod);
    lib_mod.addImport("usermgr_storage", usermgr_storage_lib_mod);

    const usermgr_storage_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/usermgr/storage_imports.zig"),
        .target = target,
        .optimize = optimize,
    });
    usermgr_storage_test_mod.addImport("antfly_root", lib_test_mod);
    usermgr_storage_test_mod.addImport("antfly_platform", platform_mod);
    lib_test_mod.addImport("usermgr_storage", usermgr_storage_test_mod);

    const usermgr_storage_data_runtime_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/usermgr/storage_imports.zig"),
        .target = target,
        .optimize = optimize,
    });
    usermgr_storage_data_runtime_test_mod.addImport("antfly_root", data_runtime_test_mod);
    usermgr_storage_data_runtime_test_mod.addImport("antfly_platform", platform_mod);
    data_runtime_test_mod.addImport("usermgr_storage", usermgr_storage_data_runtime_test_mod);

    const embedded_deps = .{
        build_options,
        lmdb_engine_mod,
        json_mod,
        public_openapi_mod,
        query_openapi_mod,
        indexes_openapi_mod,
        sort_openapi_mod,
        metadata_openapi_mod,
        reranking_mod,
        objectstore_mod,
        platform_mod,
        chunking_mod,
        bloom_mod,
        vector_mod,
        vectorindex_mod,
        vellum_mod,
        regex_mod,
        image_mod,
        font_mod,
        pdf_mod,
        handlebars_mod,
    };

    const embedded_support_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/embedded_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    @call(.auto, configureEmbeddedModule, .{ b, embedded_support_mod } ++ embedded_deps ++ .{addSnowballModule});
    embedded_support_mod.addImport("antfly_scraping", scraping_mod);

    const embedded_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/embedded/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    embedded_mod.addImport("embedded_support", embedded_support_mod);

    const embedded_db_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/embedded/db.zig"),
        .target = target,
        .optimize = optimize,
    });
    embedded_db_mod.addImport("embedded_support", embedded_support_mod);

    const embedded_api_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/embedded/api.zig"),
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

    const embedded_wasm_deps = .{
        build_options,
        lmdb_engine_wasm_mod,
        json_mod,
        public_openapi_mod,
        query_openapi_mod,
        indexes_openapi_mod,
        sort_openapi_mod,
        metadata_openapi_mod,
        reranking_mod,
        wasm_objectstore_mod,
        wasm_platform_mod,
        chunking_mod,
        wasm_bloom_mod,
        wasm_vector_mod,
        wasm_vectorindex_mod,
        vellum_mod,
        regex_mod,
        wasm_image_mod,
        wasm_font_mod,
        wasm_pdf_mod,
        handlebars_mod,
    };

    const embedded_support_wasm_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/embedded_root.zig"),
        .target = wasm_target,
        .optimize = optimize,
    });
    @call(.auto, configureEmbeddedModule, .{ b, embedded_support_wasm_mod } ++ embedded_wasm_deps ++ .{addSnowballModule});

    const embedded_wasm_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/embedded/root.zig"),
        .target = wasm_target,
        .optimize = optimize,
    });
    embedded_wasm_mod.addImport("embedded_support", embedded_support_wasm_mod);

    const embedded_db_wasm_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/embedded/db.zig"),
        .target = wasm_target,
        .optimize = optimize,
    });
    embedded_db_wasm_mod.addImport("embedded_support", embedded_support_wasm_mod);

    const embedded_api_wasm_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/embedded/api.zig"),
        .target = wasm_target,
        .optimize = optimize,
    });
    embedded_api_wasm_mod.addImport("embedded_support", embedded_support_wasm_mod);
    embedded_api_wasm_mod.addImport("embedded_db_surface", embedded_db_wasm_mod);
    embedded_wasm_mod.addImport("embedded_db_surface", embedded_db_wasm_mod);
    embedded_wasm_mod.addImport("embedded_api_surface", embedded_api_wasm_mod);

    const antfly_embedded_db_pkg_wasm_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/db.zig"),
        .target = wasm_target,
        .optimize = optimize,
    });
    antfly_embedded_db_pkg_wasm_mod.addImport("embedded_db_surface", embedded_db_wasm_mod);

    const antfly_embedded_api_pkg_wasm_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/api.zig"),
        .target = wasm_target,
        .optimize = optimize,
    });
    antfly_embedded_api_pkg_wasm_mod.addImport("embedded_api_surface", embedded_api_wasm_mod);

    const antfly_embedded_pkg_wasm_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/root.zig"),
        .target = wasm_target,
        .optimize = optimize,
    });
    antfly_embedded_pkg_wasm_mod.addImport("embedded_surface", embedded_wasm_mod);

    // --- Inference WASM modules for unified antfly.wasm ---
    const inference_wasm_build_options = b.addOptions();
    inference_wasm_build_options.addOption(bool, "enable_onnx", false);
    inference_wasm_build_options.addOption(bool, "enable_pjrt", false);
    inference_wasm_build_options.addOption(bool, "enable_cuda", false);
    inference_wasm_build_options.addOption([]const u8, "cuda_artifacts", "portable");
    inference_wasm_build_options.addOption(bool, "enable_metal", false);
    inference_wasm_build_options.addOption(bool, "enable_native", false);
    inference_wasm_build_options.addOption(bool, "enable_system_blas", false);
    inference_wasm_build_options.addOption(bool, "enable_wasm", true);
    inference_wasm_build_options.addOption(bool, "enable_webgpu", true);
    inference_wasm_build_options.addOption(bool, "enable_ffmpeg_audio", false);
    inference_wasm_build_options.addOption(bool, "link_libc", false);
    inference_wasm_build_options.addOption(bool, "skip_openapi", false);
    inference_wasm_build_options.addOption([]const u8, "inference_version", antfly_version);
    inference_wasm_build_options.addOption([]const u8, "wasm_memory_model", "wasm32");
    const inference_wasm_build_options_mod = inference_wasm_build_options.createModule();

    const wasm_inference_jinja_mod = b.createModule(.{
        .root_source_file = b.path("lib/jinja/src/jinja.zig"),
        .target = wasm_target,
        .optimize = .ReleaseSafe,
        .single_threaded = true,
    });
    const wasm_inference_tokenizer_mod = b.createModule(.{
        .root_source_file = b.path("lib/tokenizer/src/tokenizer.zig"),
        .target = wasm_target,
        .optimize = .ReleaseSafe,
    });
    wasm_inference_tokenizer_mod.addImport("sentencepiece_proto", sentencepiece_proto_mod);
    const wasm_inference_hf_tokenizer_mod = b.createModule(.{
        .root_source_file = b.path("lib/tokenizer/src/hf_root.zig"),
        .target = wasm_target,
        .optimize = .ReleaseSafe,
    });
    wasm_inference_hf_tokenizer_mod.addImport("inference_tokenizer", wasm_inference_tokenizer_mod);
    const wasm_inference_linalg_mod = b.createModule(.{
        .root_source_file = b.path("lib/linalg/src/mod.zig"),
        .target = wasm_target,
        .optimize = .ReleaseSafe,
    });
    const wasm_inference_ml_mod = b.createModule(.{
        .root_source_file = b.path("lib/ml/src/root.zig"),
        .target = wasm_target,
        .optimize = .ReleaseSafe,
        .single_threaded = true,
    });
    wasm_inference_ml_mod.addImport("antfly_platform", wasm_platform_mod);
    const wasm_inference_onnx_graph_mod = b.createModule(.{
        .root_source_file = b.path("lib/onnx/src/root.zig"),
        .target = wasm_target,
        .optimize = .ReleaseSafe,
        .single_threaded = true,
    });
    wasm_inference_onnx_graph_mod.addImport("protobuf", protobuf_mod);
    wasm_inference_onnx_graph_mod.addImport("ml", wasm_inference_ml_mod);
    const wasm_inference_audio_mod = b.createModule(.{
        .root_source_file = b.path("lib/audio/src/mod.zig"),
        .target = wasm_target,
        .optimize = .ReleaseSafe,
    });
    const inference_wasm_inference_mod = b.createModule(.{
        .root_source_file = b.path("pkg/inference/src/wasm_entry.zig"),
        .target = wasm_target,
        .optimize = .ReleaseSafe,
    });
    inference_wasm_inference_mod.addImport("build_options", inference_wasm_build_options_mod);
    inference_wasm_inference_mod.addImport("inference_audio", wasm_inference_audio_mod);
    inference_wasm_inference_mod.addImport("inference_linalg", wasm_inference_linalg_mod);
    inference_wasm_inference_mod.addImport("inference_tokenizer", wasm_inference_tokenizer_mod);
    inference_wasm_inference_mod.addImport("inference_hf_tokenizer", wasm_inference_hf_tokenizer_mod);
    inference_wasm_inference_mod.addImport("antfly_image", wasm_image_mod);
    inference_wasm_inference_mod.addImport("antfly_platform", wasm_platform_mod);
    inference_wasm_inference_mod.addImport("jinja", wasm_inference_jinja_mod);
    inference_wasm_inference_mod.addImport("ml", wasm_inference_ml_mod);
    inference_wasm_inference_mod.addImport("onnx_graph", wasm_inference_onnx_graph_mod);

    const antfly_wasm_mod = b.createModule(.{
        .root_source_file = b.path("examples/antfly_wasm.zig"),
        .target = wasm_target,
        .optimize = .ReleaseSafe,
    });
    antfly_wasm_mod.addImport("antfly_embedded_db", antfly_embedded_db_pkg_wasm_mod);
    antfly_wasm_mod.addImport("antfly_embedded_api", antfly_embedded_api_pkg_wasm_mod);
    antfly_wasm_mod.addImport("inference_runtime", inference_wasm_inference_mod);

    const antfly_wasm = b.addExecutable(.{
        .name = "antfly_wasm",
        .root_module = antfly_wasm_mod,
    });
    antfly_wasm.entry = .disabled;
    antfly_wasm.rdynamic = true;
    antfly_wasm.export_memory = true;
    const install_antfly_wasm = b.addInstallArtifact(antfly_wasm, .{
        .dest_dir = .{ .override = .prefix },
        .dest_sub_path = "antfly-wasm/antfly.wasm",
    });
    const install_antfly_wasm_smoke_run = b.addInstallFile(
        b.path("pkg/antfly-embedded/wasm_smoke_run.mjs"),
        "antfly-wasm/run.mjs",
    );
    const install_antfly_wasm_client = b.addInstallFile(
        b.path("pkg/antfly-embedded/wasm_client.mjs"),
        "antfly-wasm/antfly_embedded_wasm_client.mjs",
    );
    const install_antfly_wasm_browser = b.addInstallFile(
        b.path("pkg/antfly-embedded/wasm_smoke_browser.mjs"),
        "antfly-wasm/browser.mjs",
    );
    const install_antfly_wasm_index = b.addInstallFile(
        b.path("pkg/antfly-embedded/wasm_smoke_index.html"),
        "antfly-wasm/index.html",
    );
    const install_antfly_wasm_readme = b.addInstallFile(
        b.path("pkg/antfly-embedded/WASM.md"),
        "antfly-wasm/README.md",
    );

    const install_antfly_wasm_webgpu_ops = b.addInstallFile(
        b.path("pkg/antfly-embedded/webgpu_ops.mjs"),
        "antfly-wasm/webgpu_ops.mjs",
    );
    const shader_names = [_][]const u8{
        "attention",            "causal_attention",     "cross_attention",
        "gqa_cached_attention", "gqa_causal_attention", "layer_norm",
        "matmul",               "matmul_transb",        "matmul_transb_q4_0",
        "matmul_transb_q4_1",   "matmul_transb_q5_0",   "matmul_transb_q5_1",
        "matmul_transb_q8_0",   "matmul_transb_q8_1",   "matmul_transb_iq4_nl",
        "matmul_transb_iq4_xs", "matmul_transb_q2_k",   "matmul_transb_q3_k",
        "matmul_transb_q4_k",   "matmul_transb_q5_k",   "matmul_transb_q6_k",
        "matmul_transb_q8_k",   "rms_norm",
    };
    var install_shader_steps: [shader_names.len]*std.Build.Step = undefined;
    for (shader_names, 0..) |name, i| {
        const install_shader = b.addInstallFile(
            b.path(b.fmt("pkg/antfly-embedded/shaders/{s}.wgsl", .{name})),
            b.fmt("antfly-wasm/shaders/{s}.wgsl", .{name}),
        );
        install_shader_steps[i] = &install_shader.step;
    }

    const install_wasm_step = b.step("install-wasm", "Build and install the unified antfly wasm target (antfly-embedded + inference runtime)");
    install_wasm_step.dependOn(&install_antfly_wasm.step);
    install_wasm_step.dependOn(&install_antfly_wasm_smoke_run.step);
    install_wasm_step.dependOn(&install_antfly_wasm_client.step);
    install_wasm_step.dependOn(&install_antfly_wasm_browser.step);
    install_wasm_step.dependOn(&install_antfly_wasm_index.step);
    install_wasm_step.dependOn(&install_antfly_wasm_readme.step);
    install_wasm_step.dependOn(&install_antfly_wasm_webgpu_ops.step);
    for (&install_shader_steps) |step| {
        install_wasm_step.dependOn(step);
    }

    const run_antfly_wasm_smoke = b.addSystemCommand(&.{
        "node",
        b.getInstallPath(.prefix, "antfly-wasm/run.mjs"),
    });
    run_antfly_wasm_smoke.step.dependOn(&install_antfly_wasm.step);
    run_antfly_wasm_smoke.step.dependOn(&install_antfly_wasm_smoke_run.step);
    run_antfly_wasm_smoke.step.dependOn(&install_antfly_wasm_client.step);

    const wasm_step = b.step("wasm", "Build and run the antfly wasm smoke test under Node");
    wasm_step.dependOn(&run_antfly_wasm_smoke.step);

    // Static library
    const lib = b.addLibrary(.{
        .linkage = .static,
        .name = "antfly-zig",
        .root_module = lib_mod,
    });
    _ = lib;

    const capi_root_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/capi_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    antfly_imports.configure(b, capi_root_mod, false, link_libc);
    const capi_usermgr_storage_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/usermgr/storage_imports.zig"),
        .target = target,
        .optimize = optimize,
    });
    capi_usermgr_storage_mod.addImport("antfly_root", capi_root_mod);
    capi_usermgr_storage_mod.addImport("antfly_platform", platform_mod);
    capi_root_mod.addImport("usermgr_storage", capi_usermgr_storage_mod);

    const capi_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/capi/db.zig"),
        .target = target,
        .optimize = optimize,
        .pic = true,
    });
    capi_mod.addImport("antfly_storage_root", capi_root_mod);
    capi_mod.addImport("antfly_vector", vector_mod);
    capi_mod.addImport("structlog", structlog_mod);

    // The public C ABI and executable reuse the distributed PIC storage
    // archive, so production builds analyze and optimize that graph once.
    const libantfly_link_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/capi/link_anchor.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = link_libc,
        .strip = strip,
    });
    addMacosSdkPaths(b, libantfly_link_mod, target);
    const libantfly = b.addLibrary(.{
        .linkage = .dynamic,
        .name = "antfly",
        .root_module = libantfly_link_mod,
        .max_rss = 2 * 1024 * 1024 * 1024,
    });
    libantfly.link_gc_sections = true;
    const install_libantfly = b.addInstallArtifact(libantfly, .{});
    const install_capi_header = b.addInstallFileWithDir(
        b.path("pkg/antfly/include/antfly.h"),
        .header,
        "antfly.h",
    );

    const capi_step = b.step("capi", "Build the public libantfly C ABI shared library");
    capi_step.dependOn(&install_libantfly.step);
    capi_step.dependOn(&install_capi_header.step);

    const capi_smoke_mod = b.createModule(.{
        .target = target,
        .optimize = optimize,
    });
    capi_smoke_mod.link_libc = true;
    capi_smoke_mod.addIncludePath(b.path("pkg/antfly/include"));
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

    const run_lite_go_tests = b.addSystemCommand(&.{
        "env",
        "GOWORK=off",
        "go",
        "test",
        "-tags",
        "antflylite_capi",
        "-count=1",
        "./...",
    });
    run_lite_go_tests.setCwd(b.path("../go/pkg/antflylite"));
    run_lite_go_tests.step.dependOn(&install_libantfly.step);
    run_lite_go_tests.step.dependOn(&install_capi_header.step);
    const lite_go_test_step = b.step("lite-go-test", "Run Go Antfly Lite binding tests against libantfly");
    lite_go_test_step.dependOn(&run_lite_go_tests.step);

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
        "capi artifact decode and lookup json",
        "capi lite opens exports imports checks and vacuums aflite",
        "capi zero buffer helper wipes bytes before free",
        "capi lite exposes hosted and status-only profiles",
        "capi lite open options validate and configure ttl cleanup",
        "capi execute graph queries honors identity read generation",
        "capi search rejects stale identity generation before readable lease hook",
        "capi search json returns stamped identity generation",
        "packed dense response exposes public ids not doc ordinals",
        "dense response identity generation footer",
        "capi aggregate hits rejects stale identity generation before aggregation materialization",
    };
    const capi_tests = b.addTest(.{
        .root_module = capi_mod,
        .filters = selectTestFilters(b, &capi_default_filters),
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_capi_tests = addFilteredTestRunArtifact(b, capi_tests);
    const capi_test_step = b.step("capi-test", "Run C API tests");
    capi_test_step.dependOn(&run_capi_tests.step);

    // Tests
    const lib_regex_tests = b.addTest(.{
        .root_module = regex_mod,
    });
    const run_lib_regex_tests = b.addRunArtifact(lib_regex_tests);
    const lib_regex_test_step = b.step("lib-regex-test", "Run standalone lib/regex tests");
    lib_regex_test_step.dependOn(&run_lib_regex_tests.step);

    const lib_scraping_tests = b.addTest(.{
        .root_module = scraping_mod,
    });
    const run_lib_scraping_tests = b.addRunArtifact(lib_scraping_tests);
    const lib_scraping_test_step = b.step("lib-scraping-test", "Run standalone lib/scraping tests");
    lib_scraping_test_step.dependOn(&run_lib_scraping_tests.step);

    const lib_jsonschema_tests = b.addTest(.{
        .root_module = jsonschema_mod,
    });
    const run_lib_jsonschema_tests = b.addRunArtifact(lib_jsonschema_tests);
    const lib_jsonschema_test_step = b.step("lib-jsonschema-test", "Run standalone lib/jsonschema tests");
    lib_jsonschema_test_step.dependOn(&run_lib_jsonschema_tests.step);

    const lib_json_tests = b.addTest(.{
        .root_module = json_mod,
    });
    const run_lib_json_tests = b.addRunArtifact(lib_json_tests);
    const lib_json_test_step = b.step("lib-json-test", "Run standalone lib/json tests");
    lib_json_test_step.dependOn(&run_lib_json_tests.step);

    const lib_ml_tabular_tests = b.addTest(.{
        .root_module = ml_tabular_mod,
    });
    const run_lib_ml_tabular_tests = b.addRunArtifact(lib_ml_tabular_tests);
    const lib_ml_tabular_test_step = b.step("lib-ml-tabular-test", "Run standalone lib/ml/tabular tests");
    lib_ml_tabular_test_step.dependOn(&run_lib_ml_tabular_tests.step);

    const lib_onnx_tests = b.addTest(.{
        .root_module = inference_onnx_graph_mod,
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lib_onnx_tests = b.addRunArtifact(lib_onnx_tests);
    run_lib_onnx_tests.setEnvironmentVariable("ANTFLY_TEST_FAIL_ON_ERROR_LOGS", "0");
    const lib_onnx_test_step = b.step("lib-onnx-test", "Run standalone lib/onnx tests");
    lib_onnx_test_step.dependOn(&run_lib_onnx_tests.step);

    const fuzz_tabular_loader = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("lib/ml/tabular/src/fuzz_loader.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_fuzz_tabular_loader = b.addRunArtifact(fuzz_tabular_loader);
    const fuzz_tabular_loader_step = b.step("fuzz-tabular-loader", "Fuzz the tabular_model.json loader (--fuzz to keep running)");
    fuzz_tabular_loader_step.dependOn(&run_fuzz_tabular_loader.step);

    const lib_toon_tests = b.addTest(.{
        .root_module = toon_mod,
    });
    const run_lib_toon_tests = b.addRunArtifact(lib_toon_tests);
    const lib_toon_test_step = b.step("lib-toon-test", "Run standalone lib/toon tests");
    lib_toon_test_step.dependOn(&run_lib_toon_tests.step);

    const lib_mcp_tests = b.addTest(.{
        .root_module = mcp_mod,
    });
    const run_lib_mcp_tests = b.addRunArtifact(lib_mcp_tests);
    const lib_mcp_test_step = b.step("lib-mcp-test", "Run standalone lib/mcp tests");
    lib_mcp_test_step.dependOn(&run_lib_mcp_tests.step);

    const lib_a2a_tests = b.addTest(.{
        .root_module = a2a_mod,
    });
    const run_lib_a2a_tests = b.addRunArtifact(lib_a2a_tests);
    const lib_a2a_test_step = b.step("lib-a2a-test", "Run standalone lib/a2a tests");
    lib_a2a_test_step.dependOn(&run_lib_a2a_tests.step);

    const lib_matcher_tests = b.addTest(.{
        .root_module = matcher_mod,
    });
    const run_lib_matcher_tests = b.addRunArtifact(lib_matcher_tests);
    const lib_matcher_test_step = b.step("lib-matcher-test", "Run standalone lib/matcher tests");
    lib_matcher_test_step.dependOn(&run_lib_matcher_tests.step);

    const lib_resolver_tests = b.addTest(.{
        .root_module = resolver_mod,
    });
    const run_lib_resolver_tests = b.addRunArtifact(lib_resolver_tests);
    const lib_resolver_test_step = b.step("lib-resolver-test", "Run standalone lib/resolver tests");
    lib_resolver_test_step.dependOn(&run_lib_resolver_tests.step);

    const lib_toon_conformance = b.addExecutable(.{
        .name = "lib-toon-conformance",
        .root_module = b.createModule(.{
            .root_source_file = b.path("lib/toon/toon_conformance.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    lib_toon_conformance.root_module.addImport("antfly_toon", toon_mod);

    const fetch_lib_toon_conformance = b.addRunArtifact(lib_toon_conformance);
    fetch_lib_toon_conformance.addArg("fetch");
    fetch_lib_toon_conformance.addArg("/tmp/toon-format-spec");
    const lib_toon_conformance_fetch_step = b.step("lib-toon-conformance-fetch", "Fetch the lib/toon upstream conformance fixtures");
    lib_toon_conformance_fetch_step.dependOn(&fetch_lib_toon_conformance.step);

    const fetch_lib_toon_conformance_quiet = b.addRunArtifact(lib_toon_conformance);
    fetch_lib_toon_conformance_quiet.addArg("fetch");
    fetch_lib_toon_conformance_quiet.addArg("/tmp/toon-format-spec");
    const fetch_lib_toon_conformance_quiet_step = expectQuietSuccess(fetch_lib_toon_conformance_quiet);

    const run_lib_toon_conformance = b.addRunArtifact(lib_toon_conformance);
    run_lib_toon_conformance.addArg("run");
    run_lib_toon_conformance.addArg("/tmp/toon-format-spec");
    run_lib_toon_conformance.addArg("--no-fetch");
    const lib_toon_conformance_run_step = b.step("lib-toon-conformance-run", "Run lib/toon conformance suite without fetching fixtures");
    lib_toon_conformance_run_step.dependOn(&run_lib_toon_conformance.step);

    const run_lib_toon_conformance_after_fetch = b.addRunArtifact(lib_toon_conformance);
    run_lib_toon_conformance_after_fetch.addArg("run");
    run_lib_toon_conformance_after_fetch.addArg("/tmp/toon-format-spec");
    run_lib_toon_conformance_after_fetch.addArg("--no-fetch");
    run_lib_toon_conformance_after_fetch.step.dependOn(&fetch_lib_toon_conformance.step);
    const lib_toon_conformance_step = b.step("lib-toon-conformance", "Fetch and run lib/toon conformance suite");
    lib_toon_conformance_step.dependOn(&run_lib_toon_conformance_after_fetch.step);

    const run_lib_toon_conformance_after_fetch_quiet = b.addRunArtifact(lib_toon_conformance);
    run_lib_toon_conformance_after_fetch_quiet.addArg("run");
    run_lib_toon_conformance_after_fetch_quiet.addArg("/tmp/toon-format-spec");
    run_lib_toon_conformance_after_fetch_quiet.addArg("--no-fetch");
    run_lib_toon_conformance_after_fetch_quiet.step.dependOn(fetch_lib_toon_conformance_quiet_step);
    const run_lib_toon_conformance_after_fetch_quiet_step = expectQuietSuccess(run_lib_toon_conformance_after_fetch_quiet);

    const httpx_json_test_mod = b.createModule(.{
        .root_source_file = b.path("lib/httpx/src/util/json.zig"),
        .target = target,
        .optimize = optimize,
    });
    httpx_json_test_mod.addImport("antfly-json", json_mod);
    const httpx_json_tests = b.addTest(.{
        .root_module = httpx_json_test_mod,
    });
    const run_httpx_json_tests = b.addRunArtifact(httpx_json_tests);
    const lib_httpx_json_test_step = b.step("lib-httpx-json-test", "Run standalone lib/httpx JSON helper tests");
    lib_httpx_json_test_step.dependOn(&run_httpx_json_tests.step);

    const httpx_tests = b.addTest(.{
        .root_module = httpx_mod,
        .filters = selectTestFilters(b, &.{}),
    });
    const run_httpx_tests = b.addRunArtifact(httpx_tests);
    const lib_httpx_test_step = b.step("lib-httpx-test", "Run standalone lib/httpx tests");
    lib_httpx_test_step.dependOn(&run_httpx_tests.step);

    const objectstore_tests = b.addTest(.{
        .root_module = objectstore_mod,
        .filters = selectTestFilters(b, &.{}),
    });
    const run_objectstore_tests = b.addRunArtifact(objectstore_tests);
    const lib_objectstore_test_step = b.step("lib-objectstore-test", "Run standalone lib/objectstore tests");
    lib_objectstore_test_step.dependOn(&run_objectstore_tests.step);

    const common_http_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/common_http_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    common_http_test_mod.addImport("raft_engine", raft_engine_mod);
    common_http_test_mod.addImport("antfly_platform", platform_mod);
    common_http_test_mod.addImport("httpx", httpx_mod);
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

    const httpx_transport_regression_tests = b.addTest(.{
        .root_module = httpx_mod,
        .filters = &.{"H2 response serialization strips connection-specific headers"},
    });
    const run_httpx_transport_regression_tests = b.addRunArtifact(httpx_transport_regression_tests);

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
    const lib_api_json_helpers_test_step = b.step("lib-api-json-helpers-test", "Run standalone api/json_helpers tests");
    lib_api_json_helpers_test_step.dependOn(&run_api_json_helpers_tests.step);

    const api_artifact_reprocess_jobs_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/api_artifact_reprocess_jobs_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    antfly_imports.configure(b, api_artifact_reprocess_jobs_test_mod, true, true);
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
    const lib_api_artifact_reprocess_jobs_test_step = b.step("lib-api-artifact-reprocess-jobs-test", "Run artifact reprocess job store tests");
    lib_api_artifact_reprocess_jobs_test_step.dependOn(&run_api_artifact_reprocess_jobs_tests.step);

    const api_restore_jobs_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/api_restore_jobs_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    antfly_imports.configure(b, api_restore_jobs_test_mod, true, true);
    const api_restore_jobs_tests = b.addTest(.{
        .root_module = api_restore_jobs_test_mod,
        .filters = &.{
            "delayed replicated restore refresh cannot regress a running job",
            "restore job store is idempotent and fenced",
            "restore idempotency keys are scoped by principal and resource",
            "successful restore completion wins a racing cancellation",
            "retryable restore contention durably requeues progress and honors cancellation",
            "delayed restore contention yields FIFO capacity to unrelated jobs",
            "restore job runnable queue drains incrementally and preserves insertion order",
            "replicated restore leadership rebuild preserves FIFO and recovers running attempts",
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
    const lib_api_restore_jobs_test_step = b.step("lib-api-restore-jobs-test", "Run durable restore job store tests");
    lib_api_restore_jobs_test_step.dependOn(&run_api_restore_jobs_tests.step);

    const portable_backup_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/portable_backup_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    antfly_imports.configure(b, portable_backup_test_mod, true, true);
    const portable_backup_tests = b.addTest(.{
        .root_module = portable_backup_test_mod,
        .filters = &.{
            "export and import documents round trip",
            "file import restores Go cross-backend portable fixture",
            "file import restores production Go portable fixture",
            "file import rejects oversized portable blocks before allocation",
            "document batch round-trip",
        },
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_portable_backup_tests = addFilteredTestRunArtifact(b, portable_backup_tests);
    const lib_portable_backup_test_step = b.step("lib-portable-backup-test", "Run bounded portable backup tests");
    lib_portable_backup_test_step.dependOn(&run_portable_backup_tests.step);

    const lib_generating_tests = b.addTest(.{
        .root_module = generating_mod,
    });
    const run_lib_generating_tests = b.addRunArtifact(lib_generating_tests);
    const lib_generating_test_step = b.step("lib-generating-test", "Run standalone lib/generating tests");
    lib_generating_test_step.dependOn(&run_lib_generating_tests.step);

    const lib_embeddings_tests = b.addTest(.{
        .root_module = embeddings_mod,
    });
    const run_lib_embeddings_tests = b.addRunArtifact(lib_embeddings_tests);
    const lib_embeddings_test_step = b.step("lib-embeddings-test", "Run standalone lib/embeddings tests");
    lib_embeddings_test_step.dependOn(&run_lib_embeddings_tests.step);

    const lib_vectorindex_tests = b.addTest(.{
        .root_module = vectorindex_mod,
    });
    const run_lib_vectorindex_tests = b.addRunArtifact(lib_vectorindex_tests);
    const lib_vectorindex_test_step = b.step("lib-vectorindex-test", "Run standalone lib/vectorindex tests");
    lib_vectorindex_test_step.dependOn(&run_lib_vectorindex_tests.step);

    const vector_cancellation_tests = b.addTest(.{
        .root_module = vector_mod,
        .filters = &.{"RaBitQuantizer checks cancellation inside distance scans"},
    });
    const run_vector_cancellation_tests = b.addRunArtifact(vector_cancellation_tests);
    const vector_cancellation_test_step = b.step("lib-vector-cancellation-test", "Run bounded vector-kernel cancellation tests");
    vector_cancellation_test_step.dependOn(&run_vector_cancellation_tests.step);

    const lib_chunking_tests = b.addTest(.{
        .root_module = chunking_mod,
    });
    const run_lib_chunking_tests = b.addRunArtifact(lib_chunking_tests);
    const lib_chunking_test_step = b.step("lib-chunking-test", "Run standalone lib/chunking tests");
    lib_chunking_test_step.dependOn(&run_lib_chunking_tests.step);

    const lib_readers_tests = b.addTest(.{
        .root_module = readers_mod,
    });
    const run_lib_readers_tests = b.addRunArtifact(lib_readers_tests);
    const lib_readers_test_step = b.step("lib-readers-test", "Run standalone lib/readers tests");
    lib_readers_test_step.dependOn(&run_lib_readers_tests.step);

    const lib_extracting_tests = b.addTest(.{
        .root_module = extracting_mod,
    });
    const run_lib_extracting_tests = b.addRunArtifact(lib_extracting_tests);
    const lib_extracting_test_step = b.step("lib-extracting-test", "Run standalone lib/extracting tests");
    lib_extracting_test_step.dependOn(&run_lib_extracting_tests.step);

    const image_test_mod = b.createModule(.{
        .root_source_file = b.path("lib/image/image_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    image_test_mod.addImport("antfly_image", image_mod);
    const lib_image_tests = b.addTest(.{
        .root_module = image_test_mod,
    });
    const run_lib_image_tests = b.addRunArtifact(lib_image_tests);
    const lib_image_test_step = b.step("lib-image-test", "Run shared image tests");
    lib_image_test_step.dependOn(&run_lib_image_tests.step);

    const pdf_test_mod = b.createModule(.{
        .root_source_file = b.path("lib/pdf/pdf_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    pdf_test_mod.addImport("antfly_image", image_mod);
    pdf_test_mod.addImport("antfly_font", font_mod);
    if (target.result.os.tag == .macos) {
        addMacosSdkPaths(b, pdf_test_mod, target);
        pdf_test_mod.linkFramework("CoreFoundation", .{});
        pdf_test_mod.linkFramework("CoreGraphics", .{});
    }
    const lib_pdf_tests = b.addTest(.{
        .root_module = pdf_test_mod,
    });
    lib_pdf_tests.root_module.link_libc = true;
    const run_lib_pdf_tests = b.addRunArtifact(lib_pdf_tests);
    const lib_pdf_test_step = b.step("lib-pdf-test", "Run shared PDF tests");
    lib_pdf_test_step.dependOn(&run_lib_pdf_tests.step);

    const lib_image_bench_build_options = b.addOptions();
    const lib_image_spng_paths = detectSpngPaths(b, target);
    const lib_image_enable_spng = lib_image_spng_paths != null;
    lib_image_bench_build_options.addOption(bool, "enable_spng", lib_image_enable_spng);
    const lib_image_bench_mod = b.createModule(.{
        .root_source_file = b.path("lib/image/src/image_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    lib_image_bench_mod.addOptions("build_options", lib_image_bench_build_options);
    if (lib_image_spng_paths) |spng_paths| {
        lib_image_bench_mod.addIncludePath(.{ .cwd_relative = spng_paths.include_dir });
    }
    const lib_image_bench = b.addExecutable(.{
        .name = "lib-image-bench",
        .root_module = lib_image_bench_mod,
    });
    if (lib_image_spng_paths) |spng_paths| {
        lib_image_bench.root_module.addLibraryPath(.{ .cwd_relative = spng_paths.lib_dir });
        lib_image_bench.root_module.addRPath(.{ .cwd_relative = spng_paths.lib_dir });
        lib_image_bench.root_module.linkSystemLibrary("spng", .{});
        lib_image_bench.root_module.link_libc = true;
    }
    const run_lib_image_bench = b.addRunArtifact(lib_image_bench);
    if (b.args) |args| {
        run_lib_image_bench.addArgs(args);
    } else {
        run_lib_image_bench.addArgs(&.{
            "image-decode-suite",
            "25",
        });
    }
    const lib_image_bench_step = b.step("lib-image-bench", "Run lib/image decode benchmarks");
    lib_image_bench_step.dependOn(&run_lib_image_bench.step);

    const bench_image_step = b.step("bench-image", "Run lib/image decode benchmarks");
    bench_image_step.dependOn(&run_lib_image_bench.step);

    const pdf_bench_image_mod = b.createModule(.{
        .root_source_file = b.path("lib/image/src/mod.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    const pdf_bench_font_mod = b.createModule(.{
        .root_source_file = b.path("lib/font/src/mod.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    const pdf_bench_pdf_mod = b.createModule(.{
        .root_source_file = b.path("lib/pdf/src/mod.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    pdf_bench_pdf_mod.addImport("antfly_image", pdf_bench_image_mod);
    pdf_bench_pdf_mod.addImport("antfly_font", pdf_bench_font_mod);
    if (target.result.os.tag == .macos) {
        addMacosSdkPaths(b, pdf_bench_pdf_mod, target);
        pdf_bench_pdf_mod.linkFramework("CoreFoundation", .{});
        pdf_bench_pdf_mod.linkFramework("CoreGraphics", .{});
    }
    const pdf_bench_mod = b.createModule(.{
        .root_source_file = b.path("lib/pdf/src/pdf_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    pdf_bench_mod.addImport("antfly_pdf", pdf_bench_pdf_mod);
    const lib_pdf_bench = b.addExecutable(.{
        .name = "lib-pdf-bench",
        .root_module = pdf_bench_mod,
    });
    const run_lib_pdf_bench = b.addRunArtifact(lib_pdf_bench);
    if (b.args) |args| {
        run_lib_pdf_bench.addArgs(args);
    } else {
        run_lib_pdf_bench.addArgs(&.{
            "suite",
            "lib/pdf/testdata/simple_text_fixture.pdf",
            "25",
        });
    }
    const lib_pdf_bench_step = b.step("lib-pdf-bench", "Run lib/pdf benchmarks");
    lib_pdf_bench_step.dependOn(&run_lib_pdf_bench.step);

    const bench_pdf_step = b.step("bench-pdf", "Run lib/pdf benchmarks");
    bench_pdf_step.dependOn(&run_lib_pdf_bench.step);

    const lib_pdf_safety_tests = b.addTest(.{
        .root_module = pdf_mod,
        .filters = &.{
            "native backend renders simple pdf first page png",
            "stream decoders enforce the decoded byte budget before growth",
            "xref parser rejects a cyclic Prev chain",
        },
    });
    const run_lib_pdf_safety_tests = addFilteredTestRunArtifact(b, lib_pdf_safety_tests);
    const lib_pdf_safety_test_step = b.step("lib-pdf-safety-test", "Run focused PDF OCR rendering and parser safety tests");
    lib_pdf_safety_test_step.dependOn(&run_lib_pdf_safety_tests.step);

    const lib_image_conformance_test_mod = b.createModule(.{
        .root_source_file = b.path("lib/image/src/mod.zig"),
        .target = target,
        .optimize = optimize,
    });
    const lib_image_conformance_tests = b.addTest(.{
        .root_module = lib_image_conformance_test_mod,
        .filters = &.{"conformance corpus"},
    });
    const run_lib_image_conformance_tests = addFilteredTestRunArtifact(b, lib_image_conformance_tests);
    const lib_image_conformance_run_step = b.step("lib-image-conformance-run", "Run lib/image conformance suites without fetching fixtures");
    lib_image_conformance_run_step.dependOn(&run_lib_image_conformance_tests.step);

    const lib_image_corpus_build_options = b.addOptions();
    lib_image_corpus_build_options.addOption(bool, "enable_spng", lib_image_enable_spng);
    const lib_image_corpus_mod = b.createModule(.{
        .root_source_file = b.path("lib/image/src/image_corpus.zig"),
        .target = target,
        .optimize = optimize,
    });
    lib_image_corpus_mod.addOptions("build_options", lib_image_corpus_build_options);
    if (lib_image_spng_paths) |spng_paths| {
        lib_image_corpus_mod.addIncludePath(.{ .cwd_relative = spng_paths.include_dir });
    }
    const lib_image_corpus = b.addExecutable(.{
        .name = "lib-image-corpus",
        .root_module = lib_image_corpus_mod,
    });
    if (lib_image_spng_paths) |spng_paths| {
        lib_image_corpus.root_module.addLibraryPath(.{ .cwd_relative = spng_paths.lib_dir });
        lib_image_corpus.root_module.addRPath(.{ .cwd_relative = spng_paths.lib_dir });
        lib_image_corpus.root_module.linkSystemLibrary("spng", .{});
        lib_image_corpus.root_module.link_libc = true;
    }
    const run_lib_image_corpus_verify_jpeg = b.addRunArtifact(lib_image_corpus);
    run_lib_image_corpus_verify_jpeg.addArg("verify-jpeg");
    lib_image_conformance_run_step.dependOn(&run_lib_image_corpus_verify_jpeg.step);

    const run_lib_image_corpus_verify_jpeg_quiet = b.addRunArtifact(lib_image_corpus);
    run_lib_image_corpus_verify_jpeg_quiet.addArg("verify-jpeg");
    const run_lib_image_corpus_verify_jpeg_quiet_step = expectQuietSuccess(run_lib_image_corpus_verify_jpeg_quiet);

    const run_lib_image_corpus_verify_png = b.addRunArtifact(lib_image_corpus);
    run_lib_image_corpus_verify_png.addArg("verify-png");
    lib_image_conformance_run_step.dependOn(&run_lib_image_corpus_verify_png.step);

    const run_lib_image_corpus_verify_png_quiet = b.addRunArtifact(lib_image_corpus);
    run_lib_image_corpus_verify_png_quiet.addArg("verify-png");
    const run_lib_image_corpus_verify_png_quiet_step = expectQuietSuccess(run_lib_image_corpus_verify_png_quiet);

    const run_lib_image_corpus_verify_png_spng = b.addRunArtifact(lib_image_corpus);
    run_lib_image_corpus_verify_png_spng.addArg("verify-png-spng");
    lib_image_conformance_run_step.dependOn(&run_lib_image_corpus_verify_png_spng.step);

    const run_lib_image_corpus_verify_png_spng_quiet = b.addRunArtifact(lib_image_corpus);
    run_lib_image_corpus_verify_png_spng_quiet.addArg("verify-png-spng");
    const run_lib_image_corpus_verify_png_spng_quiet_step = expectQuietSuccess(run_lib_image_corpus_verify_png_spng_quiet);

    const run_lib_image_corpus_verify_gif = b.addRunArtifact(lib_image_corpus);
    run_lib_image_corpus_verify_gif.addArg("verify-gif");
    lib_image_conformance_run_step.dependOn(&run_lib_image_corpus_verify_gif.step);

    const run_lib_image_corpus_verify_gif_quiet = b.addRunArtifact(lib_image_corpus);
    run_lib_image_corpus_verify_gif_quiet.addArg("verify-gif");
    const run_lib_image_corpus_verify_gif_quiet_step = expectQuietSuccess(run_lib_image_corpus_verify_gif_quiet);

    const run_lib_image_corpus_verify_bmp = b.addRunArtifact(lib_image_corpus);
    run_lib_image_corpus_verify_bmp.addArg("verify-bmp");
    lib_image_conformance_run_step.dependOn(&run_lib_image_corpus_verify_bmp.step);

    const run_lib_image_corpus_verify_bmp_quiet = b.addRunArtifact(lib_image_corpus);
    run_lib_image_corpus_verify_bmp_quiet.addArg("verify-bmp");
    const run_lib_image_corpus_verify_bmp_quiet_step = expectQuietSuccess(run_lib_image_corpus_verify_bmp_quiet);

    const run_lib_image_corpus_verify_webp = b.addRunArtifact(lib_image_corpus);
    run_lib_image_corpus_verify_webp.addArg("verify-webp");
    lib_image_conformance_run_step.dependOn(&run_lib_image_corpus_verify_webp.step);

    const run_lib_image_corpus_verify_webp_quiet = b.addRunArtifact(lib_image_corpus);
    run_lib_image_corpus_verify_webp_quiet.addArg("verify-webp");
    const run_lib_image_corpus_verify_webp_quiet_step = expectQuietSuccess(run_lib_image_corpus_verify_webp_quiet);

    const image_jpeg_seed_corpora_e2e = b.addExecutable(.{
        .name = "image-jpeg-seed-corpora-e2e",
        .root_module = b.createModule(.{
            .root_source_file = b.path("lib/image/src/image_jpeg_seed_corpora_e2e.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const image_jpeg_seed_corpora_e2e_step = b.step("image-jpeg-seed-corpora-e2e", "Build the lib/image upstream JPEG seed-corpora e2e runner");
    image_jpeg_seed_corpora_e2e_step.dependOn(&image_jpeg_seed_corpora_e2e.step);

    const fetch_image_jpeg_seed_corpora_e2e = b.addRunArtifact(image_jpeg_seed_corpora_e2e);
    fetch_image_jpeg_seed_corpora_e2e.addArg("fetch");
    fetch_image_jpeg_seed_corpora_e2e.addArg("/tmp/libjpeg-turbo-seed-corpora");
    const image_jpeg_seed_corpora_e2e_fetch_step = b.step("image-jpeg-seed-corpora-e2e-fetch", "Fetch or refresh the upstream lib/image JPEG seed-corpora checkout");
    image_jpeg_seed_corpora_e2e_fetch_step.dependOn(&fetch_image_jpeg_seed_corpora_e2e.step);

    const fetch_image_jpeg_seed_corpora_e2e_quiet = b.addRunArtifact(image_jpeg_seed_corpora_e2e);
    fetch_image_jpeg_seed_corpora_e2e_quiet.addArg("fetch");
    fetch_image_jpeg_seed_corpora_e2e_quiet.addArg("/tmp/libjpeg-turbo-seed-corpora");
    const fetch_image_jpeg_seed_corpora_e2e_quiet_step = expectQuietSuccess(fetch_image_jpeg_seed_corpora_e2e_quiet);

    const run_image_jpeg_seed_corpora_e2e = b.addRunArtifact(image_jpeg_seed_corpora_e2e);
    run_image_jpeg_seed_corpora_e2e.addArg("run");
    run_image_jpeg_seed_corpora_e2e.addArg("/tmp/libjpeg-turbo-seed-corpora");
    run_image_jpeg_seed_corpora_e2e.addArg("--no-fetch");
    const image_jpeg_seed_corpora_e2e_run_step = b.step("image-jpeg-seed-corpora-e2e-run", "Run the lib/image upstream JPEG seed-corpora e2e runner");
    image_jpeg_seed_corpora_e2e_run_step.dependOn(&run_image_jpeg_seed_corpora_e2e.step);

    const run_image_jpeg_seed_corpora_e2e_after_fetch_quiet = b.addRunArtifact(image_jpeg_seed_corpora_e2e);
    run_image_jpeg_seed_corpora_e2e_after_fetch_quiet.addArg("run");
    run_image_jpeg_seed_corpora_e2e_after_fetch_quiet.addArg("/tmp/libjpeg-turbo-seed-corpora");
    run_image_jpeg_seed_corpora_e2e_after_fetch_quiet.addArg("--no-fetch");
    run_image_jpeg_seed_corpora_e2e_after_fetch_quiet.addArg("--quiet-failures");
    run_image_jpeg_seed_corpora_e2e_after_fetch_quiet.step.dependOn(fetch_image_jpeg_seed_corpora_e2e_quiet_step);
    const run_image_jpeg_seed_corpora_e2e_after_fetch_quiet_step = expectQuietSuccess(run_image_jpeg_seed_corpora_e2e_after_fetch_quiet);

    const triage_image_jpeg_seed_corpora_e2e = b.addRunArtifact(image_jpeg_seed_corpora_e2e);
    triage_image_jpeg_seed_corpora_e2e.addArg("triage-djpeg");
    triage_image_jpeg_seed_corpora_e2e.addArg("/tmp/libjpeg-turbo-seed-corpora");
    triage_image_jpeg_seed_corpora_e2e.addArg("--no-fetch");
    const image_jpeg_seed_corpora_e2e_triage_step = b.step("image-jpeg-seed-corpora-e2e-triage", "Triage upstream JPEG decode failures against local djpeg");
    image_jpeg_seed_corpora_e2e_triage_step.dependOn(&triage_image_jpeg_seed_corpora_e2e.step);

    const jpeg2000_fuzz = b.addExecutable(.{
        .name = "jpeg2000-fuzz",
        .root_module = b.createModule(.{
            .root_source_file = b.path("lib/image/src/jpeg2000_fuzz.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    jpeg2000_fuzz.root_module.addImport("antfly_image", image_mod);
    const install_jpeg2000_fuzz = b.addInstallArtifact(jpeg2000_fuzz, .{});
    const jpeg2000_fuzz_step = b.step("image-jpeg2000-fuzz", "Build the JPEG 2000 fuzz runner");
    jpeg2000_fuzz_step.dependOn(&install_jpeg2000_fuzz.step);

    // External lib/image conformance fixtures. The fetcher shallow-clones
    // openjpeg-data into /tmp; normal tests skip gracefully when the checkout
    // is missing.
    const lib_image_conformance_fetcher = b.addExecutable(.{
        .name = "lib-image-conformance-fetch",
        .root_module = b.createModule(.{
            .root_source_file = b.path("lib/image/src/jpeg2000_conformance_fixtures.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const fetch_lib_image_conformance_fixtures = b.addRunArtifact(lib_image_conformance_fetcher);
    fetch_lib_image_conformance_fixtures.addArg("fetch");
    fetch_lib_image_conformance_fixtures.addArg("/tmp/openjpeg-data");
    const lib_image_conformance_fetch_step = b.step(
        "lib-image-conformance-fetch",
        "Fetch the lib/image external conformance fixtures",
    );
    lib_image_conformance_fetch_step.dependOn(&fetch_lib_image_conformance_fixtures.step);

    const fetch_lib_image_conformance_fixtures_quiet = b.addRunArtifact(lib_image_conformance_fetcher);
    fetch_lib_image_conformance_fixtures_quiet.addArg("fetch");
    fetch_lib_image_conformance_fixtures_quiet.addArg("/tmp/openjpeg-data");
    const fetch_lib_image_conformance_fixtures_quiet_step = expectQuietSuccess(fetch_lib_image_conformance_fixtures_quiet);

    const run_lib_image_conformance_tests_after_fetch = addFilteredTestRunArtifact(b, lib_image_conformance_tests);
    run_lib_image_conformance_tests_after_fetch.step.dependOn(&fetch_lib_image_conformance_fixtures.step);
    const lib_image_conformance_step = b.step("lib-image-conformance", "Fetch and run lib/image conformance suites");
    lib_image_conformance_step.dependOn(&run_lib_image_conformance_tests_after_fetch.step);
    lib_image_conformance_step.dependOn(&run_lib_image_corpus_verify_jpeg.step);
    lib_image_conformance_step.dependOn(&run_lib_image_corpus_verify_png.step);
    lib_image_conformance_step.dependOn(&run_lib_image_corpus_verify_png_spng.step);
    lib_image_conformance_step.dependOn(&run_lib_image_corpus_verify_gif.step);
    lib_image_conformance_step.dependOn(&run_lib_image_corpus_verify_bmp.step);
    lib_image_conformance_step.dependOn(&run_lib_image_corpus_verify_webp.step);

    const run_lib_image_conformance_tests_after_fetch_quiet = addFilteredTestRunArtifact(b, lib_image_conformance_tests);
    run_lib_image_conformance_tests_after_fetch_quiet.step.dependOn(fetch_lib_image_conformance_fixtures_quiet_step);

    const lib_generating_runtime_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = &.{ "generating backend factory executes fallback chain across providers", "asset producer runtime" },
    });
    const run_lib_generating_runtime_tests = addFilteredTestRunArtifact(b, lib_generating_runtime_tests);
    const lib_generating_runtime_test_step = b.step("lib-generating-runtime-test", "Run generating backend adapter tests");
    lib_generating_runtime_test_step.dependOn(&run_lib_generating_runtime_tests.step);

    const lib_reranking_tests = b.addTest(.{
        .root_module = reranking_mod,
    });
    const run_lib_reranking_tests = b.addRunArtifact(lib_reranking_tests);
    const lib_reranking_test_step = b.step("lib-reranking-test", "Run standalone lib/reranking tests");
    lib_reranking_test_step.dependOn(&run_lib_reranking_tests.step);

    const lib_reranking_runtime_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = &.{"reranking runtime"},
    });
    const run_lib_reranking_runtime_tests = addFilteredTestRunArtifact(b, lib_reranking_runtime_tests);
    const lib_reranking_runtime_test_step = b.step("lib-reranking-runtime-test", "Run reranking backend adapter tests");
    lib_reranking_runtime_test_step.dependOn(&run_lib_reranking_runtime_tests.step);

    const lib_common_default_filters = [_][]const u8{ "provider registry", "std http listener", "std http executor", "threaded connector", "health server", "runtime lifecycle" };
    const lib_common_runtime_filters = selectTestFilters(b, &lib_common_default_filters);
    const lib_common_tests = b.addTest(.{
        .root_module = lib_test_mod,
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
    const lib_common_test_step = b.step("lib-common-test", "Run common/provider registry tests");
    lib_common_test_step.dependOn(&run_lib_common_tests.step);

    const lib_common_config_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = &.{"common config"},
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lib_common_config_tests = addFilteredTestRunArtifact(b, lib_common_config_tests);
    run_lib_common_config_tests.setEnvironmentVariable("ANTFLY_TEST_FAIL_ON_ERROR_LOGS", "0");
    const lib_common_config_test_step = b.step("lib-common-config-test", "Run common/config tests");
    lib_common_config_test_step.dependOn(&run_lib_common_config_tests.step);

    const lib_common_secrets_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = &.{ "file secret store", "remote content runtime" },
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lib_common_secrets_tests = addFilteredTestRunArtifact(b, lib_common_secrets_tests);
    const lib_common_secrets_test_step = b.step("lib-common-secrets-test", "Run common secret and remote-content reload tests");
    lib_common_secrets_test_step.dependOn(&run_lib_common_secrets_tests.step);

    const api_cluster_secret_status_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/api_cluster_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    antfly_imports.configure(b, api_cluster_secret_status_test_mod, true, true);
    const api_cluster_secret_status_tests = b.addTest(.{
        .root_module = api_cluster_secret_status_test_mod,
        .filters = &.{"cluster status carries non-secret"},
    });
    const run_api_cluster_secret_status_tests = addFilteredTestRunArtifact(b, api_cluster_secret_status_tests);
    lib_common_secrets_test_step.dependOn(&run_api_cluster_secret_status_tests.step);

    const lib_casbin_tests = b.addTest(.{
        .root_module = casbin_mod,
    });
    const run_lib_casbin_tests = b.addRunArtifact(lib_casbin_tests);
    const lib_casbin_test_step = b.step("lib-casbin-test", "Run standalone lib/casbin tests");
    lib_casbin_test_step.dependOn(&run_lib_casbin_tests.step);

    const lib_usermgr_tests = b.addTest(.{
        .root_module = usermgr_mod,
    });
    const run_lib_usermgr_tests = b.addRunArtifact(lib_usermgr_tests);
    const lib_usermgr_test_step = b.step("lib-usermgr-test", "Run standalone pkg/antfly/src/usermgr tests");
    lib_usermgr_test_step.dependOn(&run_lib_usermgr_tests.step);

    const embedded_tests = b.addTest(.{
        .root_module = embedded_mod,
        .filters = &.{"embedded"},
    });
    const run_embedded_tests = addFilteredTestRunArtifact(b, embedded_tests);
    const embedded_test_step = b.step("embedded-test", "Run embedded API tests");
    embedded_test_step.dependOn(&run_embedded_tests.step);

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
        .filters = &.{"antfly client pkg compiles"},
    });
    const run_antfly_client_pkg_tests = addFilteredTestRunArtifact(b, antfly_client_pkg_tests);
    const antfly_client_pkg_test_step = b.step("antfly-client-test", "Run the standalone antfly-client package compile test");
    antfly_client_pkg_test_step.dependOn(&run_antfly_client_pkg_tests.step);

    const root_test_skip_filters = [_][]const u8{
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
        "metadata sim ",
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
    const unit_progress_skip_filters = root_test_skip_filters;
    const lib_unit_default_filters = [_][]const u8{
        "restore job store is idempotent and fenced",
        "restore requests without idempotency keys create independent opaque jobs",
        "restore runtime store persists checkpoints and requeues interrupted work",
        "restore job store rejects oversized request state",
        "restore filesystem scope containment handles filesystem roots and component boundaries",
        ".test_0",
        "module compiles",
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
        "managed embedder rejects malformed provider vectors",
        "semantic query planning reuses equivalent embeddings",
        "batch parser preserves oversized value errors",
        "batch parser accepts raw payload value under public request cap",
        "batch parser safely rejects unsupported transform after initialized operations",
        "linear merge request parser accepts raw payload value under public request cap",
        "http response uses its owning allocator",
        "public index contract exposes runtime status metadata",
        "enrichment index status encodes worker lifecycle diagnostics",
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
        "backend runtime durable lane runs inline jobs",
        "backend runtime durable lane leaves inline failed jobs owned by caller",
        "backend runtime threaded durable lane rejects jobs after owner close",
        "backend runtime API lane leases expose and release the interface",
        "backend runtime rejects API lane leases after shutdown begins",
        "backend runtime control lane leases are isolated from API leases",
        "backend runtime inference lane has an isolated bounded executor",
        "backend runtime rejects control lane leases after shutdown begins",
        "provisioned table write cache retires stale db when index metadata changes",
        "managed startup catch-up advances counterless incomplete dense repair",
        "provisioned leader admission rejects uncommitted writes under dense repair pressure",
        "api maintenance resumes recovered durable named index cancellation without client advance",
        "embeddings index status ignores inactive stale catch-up progress once dense coverage is visible",
        "managed embeddings readiness ignores inactive stale catch-up after rate-limit recovery",
        "managed embedder sends antfly media parts when local provider is configured",
        "managed embedder normalizes local admission overload across embedding modes",
        "partial coverage embeddings readiness counts skipped source units",
        "partial coverage embeddings readiness does not mask pending enrichment",
        "create table raw parser merges default full text with quickstart embedding index",
        "create table raw parser accepts its canonical full text output",
        "table contract rejects unsupported index kinds before admission",
        "table contract preserves artifact-backed public full text indexes",
        "table contract rejects invalid inline artifact enrichments before admission",
        "public enrichment validation rejects invalid execution and producer config",
        "provisioned primary lookup lease fails on identity namespace mismatch",
        "inference pull recognizes help before model resolution",
        "inference run recognizes help before server startup",
        "inference pull classifies order independent value flags",
        "inference pull rejects flags from the other model domain",
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
        "retrieval agent isolates query predicates while applying accumulated filters",
        "retrieval agent installs canonical mandatory predicates once",
        "query builder infers graph multi hop pattern from intent",
        "retrieval root scan pushes row inclusion and exclusion predicates into one filter",
        "retrieval contains filter treats wildcard operators as literals",
        "wildcard matching distinguishes operators from escaped literals",
        "wildcard literal escaping round trips metacharacters",
        "wildcard search plans preserve escaped exact literals and prefixes",
        "algebraic wildcard helpers preserve escaped literals",
        "algebraic traversal intersects query-scoped node admission",
        "exact two-edge pattern uses typed batch probes without paths",
        "exact two-edge probe plan is equivalent to generic expansion",
        "exact two-edge probe honors incoming final direction",
        "exact endpoint constrains the final pattern step before limiting",
        "exact pattern targets preserve table identity",
        "inapplicable exact plan does not consume generic fallback budget",
        "graph exact edge probes stay aligned and preserve payloads",
        "query merge allocation scales with the selected page",
        "pattern response omits paths unless requested",
        "parse supported graph queries accepts pattern requests",
        "distributed graph edges request preserves typed graph edge access path",
        "distributed graph edge reader carries identity generation",
        "pattern hit shaping is lazy but preserves graph dependencies",
        "db unfiltered graph search retains algebraic execution",
        "db preflightSearchRequest validates live lane bindings",
        "db graph search filters result nodes and hidden traversal intermediates",
        "db graph shortest path searches through admitted alternatives",
        "db graph artifact external node targets return ids without document hydration",
        "db graph hydration fails closed for a not-yet-promoted entity node",
        "api distributed graph cross-table hydrate enforces target authorization",
        "authenticated single-group graph queries require distributed coordination",
    };
    const lib_unit_filters = selectTestFilters(b, &lib_unit_default_filters);
    const lib_unit_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = compileFiltersWithAnchors(b, &.{"api module compiles"}, lib_unit_filters),
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

    const api_http_runtime_default_filters = [_][]const u8{
        "api http client round-trips public status and internal capability routes",
        "api http retryable embedding failures provide retry guidance",
        "api http server obtains query embedding policy from resource manager",
        "api http stale hierarchy cursor response is actionable and machine readable",
        "api http point lookup retries bounded local readiness races",
        "api http hierarchy traversal preserves policy and cursor across remote hydration seam",
        "api http public sort gate accepts synthetic hierarchy child positions",
        "api http public sort capability gate validates mapped sortable fields",
        "api http public sort capability gate fails closed for uncovered observed dynamic fields",
        "table read source distinguishes unavailable physical capability observation",
        "generated route policy inventory is unique and describes wire modes",
        "linked API dispatch preserves kernel-owned ingress policy",
        "linked transport projects the universal request cancellation callback",
        "linked transport admits a streaming body before the kernel pulls it",
        "linked callbacks preserve streaming and cancellation semantics",
        "outbound stream callbacks preserve terminal status classes",
        "outbound callbacks prefer cancellation that arrives during transport IO",
        "linked request bodies remain lazy and transport neutral",
        "native executor borrows validate before reconstructing std.Io",
        "httpx production path sheds 128 abandoned queries and preserves control recovery",
        "httpx write admission rejects saturated table mutations",
        "httpx inference connection uses the configured shared admission owner",
        "local inference connection admission is owned exactly once by its target",
        "httpx inference connection requires inference write permission",
        "httpx inference connection propagates failures after stream commit",
        "inference connection invocation forwards streaming and deadline through stable target ABI",
        "inference invocation remaining deadline rounds up and expires",
        "inference connection ABI reclaims partial responses on target failure",
        "inference connection ABI rejects malformed responses without dereferencing invalid ownership",
        "local inference connection ABI retains C layout and validates capabilities",
        "local inference response validation contains malformed ownership",
        "inference connection invocation requires inference write permission",
        "httpx inference connection preserves upstream retry guidance",
        "typed internal HTTP errors preserve conflict semantics",
        "request admission bounds positive capacity and preserves unlimited mode",
        "request admission metrics use the shared admission namespace",
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
    const api_http_runtime_test_step = b.step("api-http-runtime-test", "Run focused API HTTP and linked-boundary tests");
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
    antfly_imports.configure(b, lite_native_test_mod, true, true);
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
    antfly_imports.configure(b, cmd_test_mod, true, true);
    const cmd_usermgr_storage_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/usermgr/storage_imports.zig"),
        .target = target,
        .optimize = optimize,
    });
    cmd_usermgr_storage_mod.addImport("antfly_root", cmd_test_mod);
    cmd_usermgr_storage_mod.addImport("antfly_platform", platform_mod);
    cmd_test_mod.addImport("usermgr_storage", cmd_usermgr_storage_mod);
    cmd_test_mod.addImport("antfly-zig", lib_mod);
    cmd_test_mod.addImport("antfly-client", antfly_client_pkg_mod);
    const cmd_tests = b.addTest(.{
        .root_module = cmd_test_mod,
        .filters = &.{ "cmd.lite", "cmd.serverless", "cmd.cli.backup", "cmd.cli.index", "cmd.cli.mod" },
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
        // Mach-O debug codegen retained 10.3 GiB for this intentionally broad
        // command root. Linux remains within the aggregate's 7 GiB claim.
        .max_rss = @as(usize, if (target.result.os.tag == .macos) 12 else 7) * 1024 * 1024 * 1024,
    });
    const run_cmd_tests = addFilteredTestRunArtifact(b, cmd_tests);
    const cmd_test_step = b.step("cmd-test", "Run Antfly command and client CLI tests");
    cmd_test_step.dependOn(&run_cmd_tests.step);

    const lite_cmd_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/lite_cmd_test.zig"),
        .target = target,
        .optimize = optimize,
    });
    antfly_imports.configure(b, lite_cmd_test_mod, true, true);
    const lite_cmd_usermgr_storage_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/usermgr/storage_imports.zig"),
        .target = target,
        .optimize = optimize,
    });
    lite_cmd_usermgr_storage_mod.addImport("antfly_root", lite_cmd_test_mod);
    lite_cmd_usermgr_storage_mod.addImport("antfly_platform", platform_mod);
    lite_cmd_test_mod.addImport("usermgr_storage", lite_cmd_usermgr_storage_mod);
    lite_cmd_test_mod.addImport("antfly-zig", lib_mod);
    lite_cmd_test_mod.addImport("antfly-client", antfly_client_pkg_mod);
    const lite_cmd_tests = b.addTest(.{
        .root_module = lite_cmd_test_mod,
        .filters = &.{ "cmd.lite", "cmd.cli.backup", "cmd.cli.index", "cmd.cli.mod" },
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lite_cmd_tests = addFilteredTestRunArtifact(b, lite_cmd_tests);
    const lite_cmd_test_step = b.step("lite-cmd-test", "Run command tests owned by Antfly Lite profiles");
    lite_cmd_test_step.dependOn(&run_lite_cmd_tests.step);

    const recall_test_step = b.step("recall-test", "Run HBC vector recall quality tests");

    const raft_unit_default_filters = [_][]const u8{"raft."};
    const raft_unit_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = selectTestFilters(b, &raft_unit_default_filters),
    });
    const run_raft_unit_tests = addFilteredTestRunArtifact(b, raft_unit_tests);

    // The Antfly-rooted Raft tests below cover integration call sites but do
    // not collect tests declared by the raft library's own root module.
    const raft_library_tests = b.addTest(.{
        .root_module = raft_engine_mod,
        .filters = selectTestFilters(b, &.{}),
    });
    const run_raft_library_tests = addFilteredTestRunArtifact(b, raft_library_tests);
    const raft_library_test_step = b.step("lib-raft-test", "Run standalone raft library tests");
    raft_library_test_step.dependOn(&run_raft_library_tests.step);

    const raft_runtime_default_filters = [_][]const u8{
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
        "transition service preserves nested guarded adapter identity",
        "transition service retries split bootstrap after leader recovery",
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
            "multi raft backpressure rejects async ready before cloning messages",
        },
    });
    const run_raft_ready_continuation_tests = addFilteredTestRunArtifact(b, raft_ready_continuation_tests);

    const raft_transport_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = &.{ "raft integration module compiles", "raft.transport." },
    });
    const run_raft_transport_tests = addFilteredTestRunArtifact(b, raft_transport_tests);

    // Keep this as the stable behavioral suffix of the declaration rather
    // than duplicating its descriptive worker-model prefix. The exact-filter
    // runner still fails when the regression is no longer declared.
    const http_low_fd_ratchet_filter = "recovers descriptors after cancellation storms";
    const http_low_fd_ratchet_tests = b.addTest(.{
        .root_module = lib_test_mod,
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

    const lib_raft_sim_default_filters = [_][]const u8{
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
    const lib_raft_sim_tests = b.addTest(.{
        .root_module = raft_sim_test_mod,
        .filters = &lib_raft_sim_default_filters,
    });
    const run_lib_raft_sim_tests = addFilteredTestRunArtifact(b, lib_raft_sim_tests);
    const lib_raft_sim_test_step = b.step("lib-raft-sim-test", "Run raft simulation harness tests");
    lib_raft_sim_test_step.dependOn(&run_lib_raft_sim_tests.step);

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
        .root_module = raft_sim_test_mod,
        .filters = &lib_raft_chaos_default_filters,
    });
    const run_lib_raft_chaos_tests = addFilteredTestRunArtifact(b, lib_raft_chaos_tests);
    const lib_raft_chaos_test_step = b.step("lib-raft-chaos-test", "Run longer raft restart/HTTP simulation campaigns");
    lib_raft_chaos_test_step.dependOn(&run_lib_raft_chaos_tests.step);

    const lib_lsm_backend_sim_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = &.{"lsm backend simulation"},
    });
    const run_lib_lsm_backend_sim_tests = addFilteredTestRunArtifact(b, lib_lsm_backend_sim_tests);
    const lib_lsm_backend_sim_test_step = b.step("lib-lsm-backend-sim-test", "Run LSM backend storage workload simulation tests");
    lib_lsm_backend_sim_test_step.dependOn(&run_lib_lsm_backend_sim_tests.step);

    const lib_lsm_backend_chaos_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = &.{"lsm backend compaction chaos campaign"},
    });
    const run_lib_lsm_backend_chaos_tests = addFilteredTestRunArtifact(b, lib_lsm_backend_chaos_tests);
    const lib_lsm_backend_chaos_test_step = b.step("lib-lsm-backend-chaos-test", "Run longer LSM backend compaction chaos campaigns");
    lib_lsm_backend_chaos_test_step.dependOn(&run_lib_lsm_backend_chaos_tests.step);
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
        .root_module = lib_test_mod,
        .filters = selectTestFilters(b, &lib_ha_chaos_default_filters),
    });
    const run_lib_ha_chaos_tests = addFilteredTestRunArtifact(b, lib_ha_chaos_tests);
    const lib_ha_chaos_test_step = b.step("ha-chaos-test", "Run HA hot-standby crash and partition hardening tests");
    lib_ha_chaos_test_step.dependOn(&run_lib_ha_chaos_tests.step);
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
        .root_module = lib_test_mod,
        .filters = selectTestFilters(b, &lib_ha_compat_default_filters),
    });
    const run_lib_ha_compat_tests = addFilteredTestRunArtifact(b, lib_ha_compat_tests);
    const lib_ha_compat_test_step = b.step("ha-compat-test", "Run HA replication format compatibility tests");
    lib_ha_compat_test_step.dependOn(&run_lib_ha_compat_tests.step);

    const test_step = b.step("test", "Run default package test aggregates");
    const antfly_test_step = b.step("antfly-test", "Run default Antfly unit, simulation, integration, chaos, and recall checks");
    const conformance_test_step = b.step("conformance-test", "Fetch and run conformance suites");
    const soak_test_step = b.step("soak-test", "Run long-running soak test aggregates");

    dependOnAll(conformance_test_step, &.{
        run_lib_toon_conformance_after_fetch_quiet_step,
        &run_lib_image_conformance_tests_after_fetch_quiet.step,
        run_lib_image_corpus_verify_jpeg_quiet_step,
        run_lib_image_corpus_verify_png_quiet_step,
        run_lib_image_corpus_verify_png_spng_quiet_step,
        run_lib_image_corpus_verify_gif_quiet_step,
        run_lib_image_corpus_verify_bmp_quiet_step,
        run_lib_image_corpus_verify_webp_quiet_step,
        run_image_jpeg_seed_corpora_e2e_after_fetch_quiet_step,
    });

    const unit_test_step = b.step("unit-test", "Run hermetic unit and focused integration test buckets without metadata chaos simulations");
    const unit_test_progress_step = b.step("unit-test-progress", "Run labeled major unit test suites to expose slow or stuck phases");
    unit_test_step.dependOn(&yacc_steps.run_yacc_tests.step);
    unit_test_step.dependOn(&yacc_steps.run_parser_tests.step);

    const lib_db_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = selectTestFilters(b, &.{
            "storage.db.db.test.",
            "storage.db.promotion_runtime.test.",
            "unsupported transforms fail atomically instead of reporting success",
            "supported transform on a missing document remains a no-op without upsert",
            "io threaded applied callback observes published watermark outside runtime lock",
            "io threaded wait observes worker-owned catch-up close",
            "io threaded wait requests prompt worker catch-up close",
            "io threaded wait observes failed worker-owned catch-up close",
        }),
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lib_db_tests = addFilteredTestRunArtifact(b, lib_db_tests);
    addRuntimeSkipTestFilters(run_lib_db_tests, &release_scale_test_filters);
    const lib_db_test_step = b.step("lib-db-test", "Run root-module DB tests only");
    lib_db_test_step.dependOn(&run_lib_db_tests.step);

    const serverless_default_filters = [_][]const u8{"serverless"};
    const serverless_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = &serverless_default_filters,
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_serverless_tests = addFilteredTestRunArtifact(b, serverless_tests);
    const serverless_test_step = b.step("serverless-test", "Run serverless and serverless transport tests");
    serverless_test_step.dependOn(&run_serverless_tests.step);

    const serverless_manifest_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/serverless_manifest_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    antfly_imports.configure(b, serverless_manifest_test_mod, true, true);
    const serverless_manifest_tests = b.addTest(.{
        .root_module = serverless_manifest_test_mod,
        .filters = &.{
            "objectstore-backed manifest store supports publish and list",
            "manifest head CAS verifies a stat ETag when GET omits it",
            "objectstore-backed manifest store resolves conditional create races by content",
            "host object storage delegates through callbacks",
        },
        .test_runner = .{ .path = b.path("pkg/antfly/src/test_runner.zig"), .mode = .simple },
    });
    const run_serverless_manifest_tests = addFilteredTestRunArtifact(b, serverless_manifest_tests);
    const serverless_manifest_test_step = b.step("lib-serverless-manifest-test", "Run focused serverless manifest object-store tests");
    serverless_manifest_test_step.dependOn(&run_serverless_manifest_tests.step);

    const lib_data_runtime_default_filters = [_][]const u8{
        "failed full index enrichment does not make resident reads unavailable",
        "enrichment runtime status reports worker lifecycle diagnostics",
        "enrichment index status encodes worker lifecycle diagnostics",
        "index repair scan periodically rediscovers debt after a lost wake",
        "index repair lost-wakeup fallback stays bounded at large group counts",
        "index repair lost-wakeup audit meets rotation target within supported envelope",
        "index repair fallback advances past a non-local prefix without skipping local debt",
        "index repair queue retains debt while leadership is temporarily unknown",
        "index repair queue removes only authoritative non-local ownership",
        "group status binds leadership to the membership Raft snapshot",
        "store capacity reporting preserves the last good observation on probe failure",
        "data server repair owner cancels and drains through backend runtime",
        "data server rejects replicated transition admission after owner shutdown",
        "data runtime health metrics include replay debt and provisioned warmup counters",
        "data runtime status refresh publishes synthetic missing status for absent local group db",
        "data runtime local group status does not open roots owned by transitions",
        "data runtime local group status provider collects and caches group statuses",
        "data runtime storage ownership fingerprint excludes transient placement progress",
        "data descriptor factory separates bootstrap voters from transport peers",
        "data descriptor factory bootstraps pristine group from complete intent peer set",
        "placement peer collection preserves complete intent peers during partial projection",
        "placement topology refuses partial transition bootstrap voters",
        "data runtime local group status reflects active transition readiness",
        "data runtime local group status uses metadata transition observation when local pair is absent",
        "data runtime local group status prefers merged snapshot readiness fallback",
        "data runtime status refresh publishes placeholder when live managed writer is busy and cache entry is missing",
        "data runtime status refresh publishes sibling placeholder when only one group has managed writer",
        "data runtime status refresh budget preserves fresh cached group status for visible generation",
        "data runtime status refresh reuses managed writer snapshot instead of reopening table db",
        "data runtime keeps status refresh dirty for non-startup async index work",
        "runtime status observation cannot erase a startup catch-up retry",
        "data runtime runRound does not refresh provisioned replica root inline while worker is active",
        "data runtime runRound backs off retryable provision metadata failures",
        "data runtime provisioned root refresh worker backs off retryable metadata failures",
        "data runtime data changes mark provisioned startup catch-up dirty",
        "data runtime reconciled changes invalidate status without scheduling root catch-up",
        "data runtime repair debt hook targets the affected group queue",
        "data runtime repair failures preserve durable backoff and increase retry delay",
        "index repair fallback backoff never blocks an exact durable wake",
        "data runtime repair queue links and removes debt in constant time",
        "data runtime startup catch-up parks scheduler when only quarantined debt remains",
        "data runtime raft status changes force immediate store status publication",
        "data runtime reallocation request refreshes group status once per request",
        "data runtime replicated split policy is identity and phase aware",
        "data runtime structural changes preserve physical root generations",
        "data raft draining leader remains stable through membership expansion",
        "data raft removed leader handoff campaigns preferred serving survivor",
        "data raft source split lifecycle commands bypass document db apply",
        "data raft apply records transaction conflicts without stopping replica progress",
        "data runtime structural changes preserve writer-published runtime status",
        "data runtime startup catch-up prefers cached admin snapshot",
        "data runtime startup catch-up clears dirty bit for terminal degraded index load",
        "data runtime startup catch-up clears no-debt busy writer groups",
        "data runtime provisioned root refresh spawn failure preserves retry bookkeeping",
        "data runtime background maintenance is due for dense posting cadence without lsm debt",
        "remote metadata source pins one cluster incarnation across cache invalidation",
        "remote metadata source retains mutation authority across cache invalidation",
        "remote metadata cache orders heads monotonically within an incarnation",
        "remote metadata source shares backend runtime io across a bounded executor pool",
        "data runtime treats metadata leadership churn as retryable bootstrap failure",
        "data runtime metadata bootstrap retry delay is bounded and jittered",
        "idle cached runtime status stays fresh only for the published root generation",
        "runtime status disk usage cache is scoped to one root generation",
        "runtime status disk scan retries across a reallocation fence and group invalidation remains scoped",
        "data runtime stamps one producer generation on every reported group",
        "data runtime live writer source follows raft apply ownership",
        "placement topology promotes cutover-ready learners to voters",
        "placement topology bootstraps active split destination as a new voter generation",
        "placement topology uses authoritative split peer set during partial projection",
        "data runtime local split fallback preserves source identity namespace",
        "data runtime split apply store seeding reuses cached source writer",
        "data runtime local merge fallback uses its durable table contract",
        "data runtime resolves extension package store env before local default",
        "data runtime cli accepts ARD identity flags",
        "data runtime parses experimental flag",
        "data public API listener uses public API request body limit",
        "data server can register a store without enabling data raft",
        "data server registered data raft uses wal state backend by default",
        "data raft ticker advances consensus independently of control rounds",
        "raft batch round trips table batch payload",
        "raft batch round trips deterministic transaction begin",
        "raft protocol barrier is fail closed for legacy batch parsers",
        "raft protocol barrier rejects unsupported future versions",
        "raft proposal materializes a default batch timestamp exactly once",
        "raft batch protocol preflight fingerprint fences every applying replica set",
        "raft batch protocol plan resolves only current group applying peers",
        "raft batch protocol cache reuses only short lived negative evidence",
        "raft batch protocol activation is reusable only in its accepted leader term",
        "raft batch protocol activation cleanup preserves in flight references",
        "data raft forwarding distinguishes safe retries from ambiguous outcomes",
        "expired data raft deadline snapshots never wait and release before returning",
        "data raft batch forwarding bounds routing campaigns deadlines and deterministic fallback",
        "internal batch forwarding headers are all-or-none and strictly parsed",
        "metadata http client shares deadline and cancellation across retries",
        "data server wires configured HA executors into API server",
        "data server mirrors managed primary writes into HA replication log",
        "data server fail-closed sync policy rejects primary writes before local commit",
        "data server block sync policy waits for standby acknowledgement before commit returns",
        "data server propagates standby HA write gate into provisioned write sources",
        "storage.ha data runtime default seed snapshot derives standalone groups from metadata only",
        "storage.ha data runtime rejects concurrent seed capture before waiting on mutation barrier",
        "storage.ha data server rejects writes and owner jobs after primary promotion fence",
        "data server applies routed HA replication records through standby write gate",
        "data server pulls and applies HA standby replication through internal HTTP client",
        "data server HA state change synchronously adopts promotion and rewires live HTTP executor",
        "data server promotion open failure preserves retryable standby",
        "data server resumes HA standby replication from durable progress after restart",
        "data runtime records and backs off HA standby replication round failures",
        "data runtime HA replication HTTP budget covers base64 apply envelope",
        "data server keeps upstream replication availability failures nonfatal",
        "data runtime records HA standby apply failures without stopping run round",
    };
    const lib_data_runtime_tests = b.addTest(.{
        .root_module = data_runtime_test_mod,
        .filters = selectTestFilters(b, &lib_data_runtime_default_filters),
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
        // The broad macOS ReleaseFast runtime root has measured at 11.37 GiB.
        // Keep normal aggregate parallelism while giving the scheduler an
        // honest reservation instead of forcing this root through -j1.
        .max_rss = @as(usize, if (target.result.os.tag == .macos) 12 else 7) * 1024 * 1024 * 1024,
    });
    const run_lib_data_runtime_tests = addFilteredTestRunArtifact(b, lib_data_runtime_tests);
    const lib_data_runtime_test_step = b.step("lib-data-runtime-test", "Run focused data runtime tests");
    lib_data_runtime_test_step.dependOn(&run_lib_data_runtime_tests.step);

    const lib_data_storage_default_filters = [_][]const u8{
        "data storage module tests are reachable",
        "db split destination read-only open does not create missing root",
        "db split destination applies handoff and filtered split deltas",
        "db split destination persists handoff state across reopen",
        "db split sync coordinator allocates destination identity namespace",
        "db split status rejects stale destination identity namespace",
        "db split status borrows the live raft apply store without a second writer",
        "db split status uses source acknowledgement without opening destination",
        "derive split transition phases",
        "db split coordinator rejects mismatched active transition destination",
        "db split coordinator remains closed after failed reopen",
        "db split sync coordinator resumes catch-up across source and destination reopen",
        "db split sync coordinator can prepare source split again after rollback",
        "db split successor bootstrap atomically replaces stale destination generation",
        "data raft apply store applies delete operations into group state",
        "data raft protocol barrier persists and transfers in snapshots",
        "data raft protocol request observation never waits for generation preparation",
        "data raft apply store prepared snapshot retains its MVCC view across later writes",
        "data raft apply store orders independent groups through separate shards",
        "data raft apply store admits one writable owner per root",
        "data raft apply store skips persisted split commands in overlapping replay",
        "data raft apply store recovers committed split start after projection generation gap",
        "data raft apply store recovers exact split replay after injected projection corruption",
        "data raft apply store reconciles inherited documents while preserving active split control",
        "data raft apply store rejects a regressing source generation during active split",
        "data raft apply store rejects mismatched terminal split identity",
        "data raft apply store persists split destination acknowledgements",
        "data raft split cursors are stable across apply batching and acknowledge same-batch writes",
        "data raft apply store seeds pre-raft snapshots once at reserved index zero",
        "data raft apply store refuses stale snapshot projection regression",
        "data raft snapshot staging blocks only the target group",
        "file replica catalog rejects an existing truncated empty file",
        "file replica catalog rejects checksum mismatch and missing footer",
        "raft batch round trips internal split checkpoint",
        "raft batch round trips internal split replication identity",
        "paged authoritative reconciliation removes stale out-of-range documents before publication",
        "paged authoritative reconciliation is allocation-failure safe",
        "group state range scan is allocation-failure safe",
        "shard state store persists split lifecycle and ownership",
        "shard state store decodes legacy split acknowledgement layouts",
        "shard state snapshot round trips split control state",
        "shard state snapshot rejects duplicate and out-of-range documents",
        "shard state store finalize split reclaims right-hand document range",
        "raft snapshot durability tests are reachable",
        "raft snapshot payload envelope validates identity length and checksum",
        "raft snapshot payload publication rejects an artifact length contract violation",
        "raft snapshot payload cleanup retains only the durable identity",
        "persistent replica state rejects corrupt unchecked and structurally invalid files",
        "persistent replica state refuses a corrupt durable snapshot payload on reopen",
        "persistent replica state publishes an artifact snapshot and reopens it",
        "persistent replica state recovers both snapshot publication crash windows",
        "wal replica state migrates legacy checkpoints and delta tails",
        "wal replica state rejects corrupt or oversized applied watermark sidecars",
        "wal replica state persists semantic compaction snapshot and preserves suffix",
        "wal replica state refuses a missing durable snapshot payload on reopen",
        "wal replica provider wires host through WAL-backed local state",
        "db merge coordinator opt-in applies configured receiver identity namespace",
        "db merge coordinator reapplies target namespace for persisted reassignment opt-in",
        "db merge coordinator rollback reapplies target namespace for persisted reassignment opt-in",
    };
    const lib_data_storage_runtime_filters = selectTestFilters(b, &lib_data_storage_default_filters);
    const lib_data_storage_tests = b.addTest(.{
        .root_module = data_storage_test_mod,
        .filters = compileFiltersWithAnchors(
            b,
            &.{"data storage module tests are reachable"},
            lib_data_storage_runtime_filters,
        ),
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lib_data_storage_tests = addFilteredTestRunArtifactWithRuntimeFilters(
        b,
        lib_data_storage_tests,
        lib_data_storage_runtime_filters,
    );
    const lib_data_storage_test_step = b.step("lib-data-storage-test", "Run focused data storage tests");
    lib_data_storage_test_step.dependOn(&run_lib_data_storage_tests.step);

    const lib_db_enrichment_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = &.{
            "storage.db.db.test.db batch marks generated enrichment replay",
            "storage.db.db.test.db computeEnrichments",
            "storage.db.db.test.db leased enrichment",
            "storage.db.db.test.db shared embedding enrichment",
            "storage.db.db.test.db dense index can reference existing",
            "storage.db.db.test.db persists shorthand chunk enrichment",
            "storage.db.db.test.db listEnrichments",
            "storage.db.db.test.db dense parent paging",
            "storage.db.db.test.db batch persists per-index applied sequence",
            "storage.db.db.test.db batch truncates replay logs",
            "storage.db.db.test.db async replay truncation retains durable enrichment debt",
            "storage.db.db.test.db restart after provider failure resumes enrichment",
            "storage.db.db.test.db foreign inference provider failure releases enrichment waiter as terminal",
            "storage.db.db.test.db terminal enrichment marker cleanup isolates corrupt cross issue reverse entries",
            "storage.db.db.test.db terminal enrichment marker retirement is bounded and fences stale generations",
            "storage.db.db.test.db enrichment reconfigure refreshes durable state after old worker joins",
            "storage.db.db.test.db io_threaded executor processes indexed writes",
            "storage.db.db.test.db reopen replays pending derived embeddings",
            "storage.db.db.test.db replay respects per-index applied watermarks",
            "storage.db.db.test.db replay applies dense embeddings from artifact payloads",
            "storage.db.db.test.db split cutover",
            "storage.db.db.test.db merge-style cutover",
            "remote fetch classification retries only transient failures",
            "remote HTTP failures consume retry budget before terminal coverage",
            "enrichment retries unknown errors and isolates known permanent errors",
            "enrichment worker attempt budget includes the current request",
            "pipeline retry fingerprint is stable across alternating errors",
            "alternating pipeline errors exhaust one durable retry budget",
            "pipeline failures retain their retry budget across replay pass reset",
            "pipeline failure replaces stale request retry identity",
            "mixed request and pipeline failures exhaust one no-progress budget",
            "durable enrichment retry progress preserves unrelated request debt across restart",
            "ordinary startup target preserves restored retry debt",
            "enrichment terminal failure envelope remains conservative across sparse durable debt",
            "enrichment terminal failure envelope uses exact durable lookup for sparse gaps",
            "isolated enrichment request error does not mark worker failed",
            "enrichment runtime status",
            "enrichment runtime restore",
            "worker retry preserves only an explicitly authorized request identity",
            "permanent remote HTTP failure cannot authorize request retry identity",
            "enrichment visibility wait wakes immediately on applied state",
            "enrichment visibility wait has a hard liveness timeout",
            "enrichment visibility wait is cancelable",
            "enrichment visibility wait observes borrowed request cancellation",
            "foreground enrichment rejects providers without a bounded-operation contract",
            "document extraction reserves PDF decoder peak memory atomically",
            "PDF decoder credit and OCR transient allocations compose without double charging",
            "reserved PDF working set is bounded without duplicate resource charges",
            "budgeted document download composes with materialization accounting",
            "retained document collection allocations compose with the hard working-set cap",
            "document replay payloads are admitted before persistent allocation",
            "document extraction generated OCR bypasses unsupported native batch",
            "OCR pending metadata construction is allocation-failure safe",
            "generated text provider config is validated while parsing extraction config",
            "public enrichment validation rejects invalid execution and producer config",
            "enrichment runtime document extraction manifest uses v2 range and merge shape",
            "enrichment runtime document extraction state parses byte-array keys",
            "enrichment runtime navigation cleanup removes superseded and deleted blocks",
            "enrichment runtime rejects unbounded navigation block counts",
            "db document extraction failure manifest preserves prior artifacts",
            "document unit fingerprint canonically separates variable field boundaries",
            "document unit fingerprint distinguishes absent and empty optional fields",
            "document unit fingerprint state version rejects legacy encodings",
            "db canonical hierarchy traversal rejects typed retrieval controls",
            "db document unit payload preserves pdf page provenance",
            "db document extraction asset materializes unit artifacts from data url",
            "db document extraction chunks units through source artifact enrichment",
            "db public artifact lookup strips private hierarchy metadata without changing internal records",
            "db hierarchy navigation seeks only the descriptor blocks needed by the page",
            "db hierarchy cursor binds every unit artifact revision under its source",
        },
    });
    const run_lib_db_enrichment_tests = addFilteredTestRunArtifact(b, lib_db_enrichment_tests);
    const lib_db_enrichment_step = b.step("lib-db-enrichment-test", "Run root-module DB enrichment/replay/cutover tests");
    lib_db_enrichment_step.dependOn(&run_lib_db_enrichment_tests.step);

    const lib_db_enrichment_worker_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = &.{
            "storage.db.db.test.db batch marks generated enrichment replay",
            "storage.db.db.test.db computeEnrichments",
            "storage.db.db.test.db leased enrichment worker",
            "storage.db.db.test.db shared embedding enrichment",
            "storage.db.db.test.db dense index can reference existing",
            "storage.db.db.test.db persists shorthand chunk enrichment",
            "storage.db.db.test.db listEnrichments",
            "storage.db.db.test.db dense parent paging",
        },
    });
    const run_lib_db_enrichment_worker_tests = addFilteredTestRunArtifact(b, lib_db_enrichment_worker_tests);
    const lib_db_enrichment_worker_step = b.step("lib-db-enrichment-worker-test", "Run root-module DB enrichment worker tests");
    lib_db_enrichment_worker_step.dependOn(&run_lib_db_enrichment_worker_tests.step);

    const lib_db_enrichment_replay_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = &.{
            "storage.db.db.test.db batch persists per-index applied sequence",
            "storage.db.db.test.db batch truncates replay logs",
            "storage.db.db.test.db async replay truncation retains durable enrichment debt",
            "storage.db.db.test.db restart after provider failure resumes enrichment",
            "storage.db.db.test.db io_threaded executor processes indexed writes",
            "storage.db.db.test.db reopen replays pending derived embeddings",
            "storage.db.db.test.db replay respects per-index applied watermarks",
            "storage.db.db.test.db replay applies dense embeddings from artifact payloads",
        },
    });
    const run_lib_db_enrichment_replay_tests = addFilteredTestRunArtifact(b, lib_db_enrichment_replay_tests);
    const lib_db_enrichment_replay_step = b.step("lib-db-enrichment-replay-test", "Run root-module DB enrichment replay tests");
    lib_db_enrichment_replay_step.dependOn(&run_lib_db_enrichment_replay_tests.step);

    const lib_db_enrichment_cutover_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = &.{
            "storage.db.db.test.db split cutover",
            "storage.db.db.test.db merge-style cutover",
        },
    });
    const run_lib_db_enrichment_cutover_tests = addFilteredTestRunArtifact(b, lib_db_enrichment_cutover_tests);
    const lib_db_enrichment_cutover_step = b.step("lib-db-enrichment-cutover-test", "Run root-module DB enrichment cutover tests");
    lib_db_enrichment_cutover_step.dependOn(&run_lib_db_enrichment_cutover_tests.step);

    const lib_db_enrichment_split_cutover_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = &.{
            "storage.db.db.test.db split cutover fences enrichment to the owning range",
            "storage.db.db.test.db split cutover preserves enrichment resume and fencing across reopen",
        },
    });
    const run_lib_db_enrichment_split_cutover_tests = addFilteredTestRunArtifact(b, lib_db_enrichment_split_cutover_tests);
    const lib_db_enrichment_split_cutover_step = b.step("lib-db-enrichment-split-cutover-test", "Run root-module DB enrichment split cutover tests");
    lib_db_enrichment_split_cutover_step.dependOn(&run_lib_db_enrichment_split_cutover_tests.step);

    const lib_db_enrichment_merge_cutover_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = &.{
            "storage.db.db.test.db merge-style cutover fences enrichment to the merged receiver range",
            "storage.db.db.test.db merge-style cutover preserves enrichment resume and fencing across reopen",
        },
    });
    const run_lib_db_enrichment_merge_cutover_tests = addFilteredTestRunArtifact(b, lib_db_enrichment_merge_cutover_tests);
    const lib_db_enrichment_merge_cutover_step = b.step("lib-db-enrichment-merge-cutover-test", "Run root-module DB enrichment merge cutover tests");
    lib_db_enrichment_merge_cutover_step.dependOn(&run_lib_db_enrichment_merge_cutover_tests.step);

    const lib_db_enrichment_split_cutover_reopen_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = &.{"storage.db.db.test.db split cutover preserves enrichment resume and fencing across reopen"},
    });
    const run_lib_db_enrichment_split_cutover_reopen_tests = addFilteredTestRunArtifact(b, lib_db_enrichment_split_cutover_reopen_tests);
    const lib_db_enrichment_split_cutover_reopen_step = b.step("lib-db-enrichment-split-cutover-reopen-test", "Run root-module DB split cutover reopen test");
    lib_db_enrichment_split_cutover_reopen_step.dependOn(&run_lib_db_enrichment_split_cutover_reopen_tests.step);

    const lib_db_enrichment_merge_cutover_reopen_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = &.{"storage.db.db.test.db merge-style cutover preserves enrichment resume and fencing across reopen"},
    });
    const run_lib_db_enrichment_merge_cutover_reopen_tests = addFilteredTestRunArtifact(b, lib_db_enrichment_merge_cutover_reopen_tests);
    const lib_db_enrichment_merge_cutover_reopen_step = b.step("lib-db-enrichment-merge-cutover-reopen-test", "Run root-module DB merge cutover reopen test");
    lib_db_enrichment_merge_cutover_reopen_step.dependOn(&run_lib_db_enrichment_merge_cutover_reopen_tests.step);

    const lib_db_query_default_filters = [_][]const u8{
        "grouped candidate budget parses disabled and fallback values",
        "adaptive candidate window covers requested offset page and grows bounded",
        "grouped result page satisfaction treats nested match count as a maximum",
        "storage.db.db.test.db full-text",
        "storage.db.db.test.db dense ",
        "storage.db.db.test.db sparse ",
        "storage.db.db.test.db graph ",
        "storage.db.db.test.db search ",
        "storage.db.db.test.db default dynamic schema vector term filters project through doc identity ordinals",
        "storage.db.db.test.db schema-present infer_types opt-in recursively infers nested fields after reopen",
        "storage.db.db.test.db document _edges",
        "storage.db.db.test.db document _embeddings",
        "sort execution plan dimension names are stable for profiles",
        "sort cursor contract classifies arity separately from type",
        "json sort values reject non-replayable numeric values at API boundaries",
        "stored json debug sort honors runtime missing null policy",
        "stored json debug sort normalizes runtime datetime values",
        "score sort source detection rejects non-scoring text queries",
        "vector score order helper is limited to internal score tuple decoration",
        "score sort rejects hits without finite scores",
        "match_all rejects score sort without score-bearing source",
        "match_all candidate sort rejects direct score sort execution",
        "distributed sorted hit merge uses typed sort tuple ordering and cursors",
        "distributed merge rejects provably incomplete exact shard windows",
        "distributed merge uses runtime schema for typed date cursors",
        "vector score top k sort profile uses common sort vocabulary",
        "native sort planner classifies mapping and cursor rejection reasons",
        "text score query exposes score top k sort profile",
        "native text sort planner requires live segment index sort coverage for sorted executor",
        "native text sort planner ignores fully deleted legacy segments for index sort coverage",
        "native sort coverage diagnostics classify physical doc value failures",
        "native sort cold coverage validation observes request deadline and cancellation",
        "match_all sorted segment seek merges sorted segments and applies cursors",
        "match_all sorted segment seek uses cursor seek within each segment",
        "match_all sorted segment seek enforces scan budget",
        "match_all sorted segment seek checks deadline while scanning",
        "match_all sorted segment seek zero limit returns profile without scanning",
        "match_all sorted segment seek rejects cursor when segment bounds are unavailable",
        "match_all projected source load rejects expired deadline before batch load",
        "dense projected source load rejects expired deadline before load",
        "match_all unordered source loads selected hits through projected batch",
        "text field sort uses sorted segment membership path when index sort matches",
        "text projected source load rejects expired deadline before stored load",
        "native numeric sort rejects non-finite doc values",
        "native sort zero limit avoids generic collector decoration",
        "text doc values sort zero limit avoids budget and decoration",
        "match_all native candidate sort zero limit avoids decoration",
        "match_all native ordinal doc values zero limit avoids budget and decoration",
        "match_all native stream sort zero limit counts without decoration",
        "match_all id seek zero limit exposes internal sort profile when sampled",
        "match_all id seek zero limit respects cursor bounds exactly",
        "match_all native candidate sort applies cursor before admission",
        "dense search route reports exact native filter budget decisions",
        "compiled stored filters honor canonical JSON pointer fields and escapes",
        "stored term filters preserve JSON scalar kinds",
        "document mapper emits default dynamic schema text fields",
        "document mapper emits schema keyword typed doc values",
        "document mapper omits multi-valued schema keyword typed doc values",
        "document mapper omits multi-valued schema numeric typed doc values",
        "document mapper preserves integer numeric doc values as i64",
        "document mapper preserves unsigned numeric doc values beyond i64 as u64",
        "document mapper preserves mixed numeric typed doc value domains",
        "document mapper omits non-finite numeric doc values",
        "document mapper flushes schema index_sort segments in physical sort order",
        "document mapper orders mixed numeric domains for index_sort field",
        "document mapper validates schema index_sort field capabilities",
        "segment append merge normalizes legacy mixed numeric doc values",
        "range filter on typed doc values",
        "search with stats aggregation",
        "merge preserves common sorted segment index_sort metadata",
        "sort planner rejects non-finite numeric index sort bounds",
        "schema keyword doc values back native sort planner",
        "schema link doc values back native sort planner",
        "schema numeric u64 doc values back native sort planner without rounding",
        "schema numeric i64 doc values back native sort planner",
        "mixed numeric concrete sort keys share one cursor domain",
        "schema boolean doc values back native sort planner",
        "db exact sort resolves mapped geo metadata filters from typed doc values",
    };
    const lib_db_query_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = &lib_db_query_default_filters,
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lib_db_query_tests = b.addRunArtifact(lib_db_query_tests);
    addRuntimeTestFilters(b, run_lib_db_query_tests, &lib_db_query_default_filters);
    const lib_db_query_step = b.step("lib-db-query-test", "Run root-module DB query/indexing tests");
    lib_db_query_step.dependOn(&run_lib_db_query_tests.step);

    const lib_db_text_query_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = &.{
            "text late visibility",
        },
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lib_db_text_query_tests = addFilteredTestRunArtifact(b, lib_db_text_query_tests);
    const lib_db_text_query_step = b.step("lib-db-text-query-test", "Run focused full-text query guardrail tests");
    lib_db_text_query_step.dependOn(&run_lib_db_text_query_tests.step);

    const lib_db_result_shape_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = &.{
            "dedupeSearchHitsById uses ordinals when hit page is complete",
            "applyStoredSearchPatternFilters reports lower-bound total for filtered page window",
            "applyStoredSearchPatternFilters resolves native doc id constraints to hit ordinals",
            "applyStoredSearchPatternFilters uses hit ordinals for resolved doc filters",
            "applyStoredSearchPatternFilters fails closed without resolved ordinal projection",
            "applyStoredSearchPatternFilters fails closed when ordinal projection is unsupported",
            "postprocessTextSearchResult preserves exact upstream total when page is unchanged",
            "unit relevance grouping rejects source-backed text and vector results",
            "postprocessTextSearchResult forwards batch stored loader to pattern filters",
            "normalizeChunkArtifactForQuery strips private unit revision metadata",
            "db lookup includes chunk artifacts when _chunks is requested",
            "db lookup includes unified artifact projection when _artifacts is requested",
            "stored structured filters preserve one-key field name collisions",
            "pattern bool filter preserves explicit minimum should match",
            "native dense constraints fail closed without ordinal vector mapping",
            "buildPatternDocumentHits preserves resolved binding ordinals",
            "executeSingleNonPatternQueryWithSets hydrates graph documents from include_documents",
            "executeSearchGraphWithSets preserves node ordinals",
            "cloneNamedSetAsResult preserves hit ordinals",
            "fuseNamedSets preserves source hit ordinals",
            "fuseNamedSets reports a lower bound while any source window is truncated",
            "db search marks a truncated fused candidate union as a lower bound",
            "fuseNamedSets deduplicates aliases by ordinal when complete",
            "fuseNamedSets drops conflicting source hit ordinals",
            "applyGraphUnion deduplicates by ordinals when hit pages are complete",
            "applyGraphIntersection uses ordinals when hit pages are complete",
            "reshapeChunkBackedResult uses the best descendant relevance score and distance",
            "reshapeChunkBackedResult groups matching chunks by unit",
            "unit grouping independently batch-hydrates projected unit and deduplicated source ancestors",
            "unit grouping batch-loads candidates and uses sanitized projected unit payloads",
            "distributed unit grouping defers payloads owned by another child range",
            "hierarchy positions round trip arbitrary components and preserve artifact ordering",
            "hierarchy stored-value sanitizer always removes the unit fingerprint",
        },
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lib_db_result_shape_tests = addFilteredTestRunArtifact(b, lib_db_result_shape_tests);
    const lib_db_result_shape_step = b.step("lib-db-result-shape-test", "Run focused DB query doc id boundary tests");
    lib_db_result_shape_step.dependOn(&run_lib_db_result_shape_tests.step);

    const lib_db_reopen_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = &.{
            "storage.db.db.test.db reopens persisted",
            "storage.db.db.test.db delete index persists",
            "storage.db.db.test.db indexed delete removes",
            "storage.db.db.test.db indexed overwrite replaces",
            "storage.db.db.test.db compacts tiny text segments",
            "storage.db.db.test.db phrase query survives",
            "storage.db.db.test.db prefix wildcard and regexp",
            "storage.db.db.test.db typed and dictionary queries survive",
            "storage.db.db.test.db mixed-type stored fields survive",
            "storage.db.db.test.db persists byte range across reopen",
            "storage.db.db.test.db snapshot copies current store and derived log",
            "storage.db.db.test.db updateRange constrains index backfill",
        },
    });
    const run_lib_db_reopen_tests = addFilteredTestRunArtifact(b, lib_db_reopen_tests);
    const lib_db_reopen_step = b.step("lib-db-reopen-test", "Run root-module DB reopen/compaction tests");
    lib_db_reopen_step.dependOn(&run_lib_db_reopen_tests.step);

    const lib_db_txn_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = &.{
            "storage.db.db.test.db writes and reads timestamp",
            "storage.db.db.test.db lookup hides expired",
            "storage.db.db.test.db search filters expired",
            "storage.db.db.test.db ttl cleanup",
            "storage.db.db.test.db exposes local transaction lifecycle",
            "storage.db.db.test.db transaction ",
            "storage.db.db.test.db explicit resolveTransactionIntents",
            "storage.db.db.test.db recoverTransactions",
            "storage.db.db.test.db participant recovery",
            "storage.db.db.test.db batch enforces optimistic version predicates",
            "coordinator recovery durably aborts a stale prepared transaction",
            "idempotent begin upgrades a legacy transaction coordinator role",
            "transaction recovery delegates stale coordinator abort to replicated resolver",
            "replicated recovery is coordinator-owned and acknowledges through hooks",
            "transaction recovery drains terminal HA outbox without remaining intents",
            "non-replicated transaction recovery honors the per-run page limit",
            "retained terminal transactions honor the extended retry cutoff",
            "topology fence retains committed coordinator recovery obligations",
        },
    });
    const run_lib_db_txn_tests = addFilteredTestRunArtifact(b, lib_db_txn_tests);
    const lib_db_txn_step = b.step("lib-db-txn-test", "Run root-module DB TTL/transaction tests");
    lib_db_txn_step.dependOn(&run_lib_db_txn_tests.step);

    const lib_metadata_runtime_filters = selectTestFilters(b, &.{"metadata."});
    const lib_metadata_test_step = b.step("lib-metadata-test", "Run root-module metadata tests only");

    const lib_metadata_table_workflow_tests = b.addTest(.{
        .root_module = lib_test_mod,
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
    const lib_metadata_table_workflow_test_step = b.step("lib-metadata-table-workflow-test", "Run focused metadata table workflow tests");
    lib_metadata_table_workflow_test_step.dependOn(&run_lib_metadata_table_workflow_tests.step);

    const lib_metadata_sim_default_filters = [_][]const u8{"metadata http cluster simulation"};
    const lib_metadata_sim_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = selectTestFilters(b, &lib_metadata_sim_default_filters),
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lib_metadata_sim_tests = addFilteredTestRunArtifact(b, lib_metadata_sim_tests);
    const lib_metadata_sim_test_step = b.step("lib-metadata-sim-test", "Run metadata real-HTTP simulation tests only");
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
        .root_module = lib_test_mod,
        .filters = selectTestFilters(b, &lib_metadata_sim_core_default_filters),
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lib_metadata_sim_core_tests = addFilteredTestRunArtifact(b, lib_metadata_sim_core_tests);
    const lib_metadata_sim_core_test_step = b.step("lib-metadata-sim-core-test", "Run deterministic metadata virtual-transport simulation tests without public API or chaos");
    lib_metadata_sim_core_test_step.dependOn(&run_lib_metadata_sim_core_tests.step);

    const lib_metadata_sim_smoke_default_filters = [_][]const u8{
        "metadata sim split runtime preserves source identity namespace",
        "metadata sim merge runtime records doc identity reassignment opt-in",
        "metadata http cluster simulation drives table placement convergence",
        "metadata http cluster simulation drives split intent through the control loop",
    };
    const lib_metadata_sim_smoke_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = selectTestFilters(b, &lib_metadata_sim_smoke_default_filters),
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lib_metadata_sim_smoke_tests = addFilteredTestRunArtifact(b, lib_metadata_sim_smoke_tests);
    const lib_metadata_sim_smoke_test_step = b.step("lib-metadata-sim-smoke-test", "Run fast metadata virtual-transport simulation smoke tests");
    lib_metadata_sim_smoke_test_step.dependOn(&run_lib_metadata_sim_smoke_tests.step);

    const lib_metadata_vopr_default_filters = [_][]const u8{
        "metadata VOPR seeded smoke campaign",
    };
    const lib_metadata_vopr_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = selectTestFilters(b, &lib_metadata_vopr_default_filters),
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lib_metadata_vopr_tests = addFilteredTestRunArtifact(b, lib_metadata_vopr_tests);
    const lib_metadata_vopr_test_step = b.step("lib-metadata-vopr-test", "Run seeded metadata virtual-operation campaign tests");
    lib_metadata_vopr_test_step.dependOn(&run_lib_metadata_vopr_tests.step);

    const lib_metadata_vopr_chaos_default_filters = [_][]const u8{
        "metadata VOPR expanded generated workload campaign",
    };
    const lib_metadata_vopr_chaos_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = selectTestFilters(b, &lib_metadata_vopr_chaos_default_filters),
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lib_metadata_vopr_chaos_tests = addFilteredTestRunArtifact(b, lib_metadata_vopr_chaos_tests);
    const lib_metadata_vopr_chaos_test_step = b.step("lib-metadata-vopr-chaos-test", "Run expanded metadata VOPR generated workload campaigns");
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

    const lib_metadata_transition_chaos_test_step = b.step("lib-metadata-transition-chaos-test", "Run metadata split/merge transition restart and partition chaos simulations");
    var metadata_transition_chaos_progress_tail: ?*std.Build.Step = null;
    metadata_transition_chaos_progress_tail = chainLabeledFilteredTests(b, lib_test_mod, "lib-metadata-transition-chaos-test", lib_metadata_transition_chaos_filters, metadata_transition_chaos_progress_tail);
    lib_metadata_transition_chaos_test_step.dependOn(metadata_transition_chaos_progress_tail.?);

    const lib_metadata_public_chaos_test_step = b.step("lib-metadata-public-chaos-test", "Run metadata public traffic split/merge chaos simulations");
    var metadata_public_chaos_progress_tail: ?*std.Build.Step = null;
    metadata_public_chaos_progress_tail = chainLabeledFilteredTests(b, lib_test_mod, "lib-metadata-public-chaos-test", lib_metadata_public_chaos_filters, metadata_public_chaos_progress_tail);
    lib_metadata_public_chaos_test_step.dependOn(metadata_public_chaos_progress_tail.?);

    const lib_metadata_placement_chaos_test_step = b.step("lib-metadata-placement-chaos-test", "Run metadata placement restart chaos simulations");
    var metadata_placement_chaos_progress_tail: ?*std.Build.Step = null;
    metadata_placement_chaos_progress_tail = chainLabeledFilteredTests(b, lib_test_mod, "lib-metadata-placement-chaos-test", lib_metadata_placement_chaos_filters, metadata_placement_chaos_progress_tail);
    lib_metadata_placement_chaos_test_step.dependOn(metadata_placement_chaos_progress_tail.?);

    const lib_metadata_chaos_test_step = b.step("lib-metadata-chaos-test", "Run metadata delayed/restart/partition chaos simulations");
    var metadata_chaos_progress_tail: ?*std.Build.Step = null;
    metadata_chaos_progress_tail = chainLabeledFilteredTests(b, lib_test_mod, "lib-metadata-transition-chaos-test", lib_metadata_transition_chaos_filters, metadata_chaos_progress_tail);
    metadata_chaos_progress_tail = chainLabeledFilteredTests(b, lib_test_mod, "lib-metadata-public-chaos-test", lib_metadata_public_chaos_filters, metadata_chaos_progress_tail);
    metadata_chaos_progress_tail = chainLabeledFilteredTests(b, lib_test_mod, "lib-metadata-placement-chaos-test", lib_metadata_placement_chaos_filters, metadata_chaos_progress_tail);
    lib_metadata_chaos_test_step.dependOn(metadata_chaos_progress_tail.?);

    const lib_metadata_sim_public_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = &.{
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
    const lib_metadata_sim_public_test_step = b.step("lib-metadata-sim-public-test", "Run metadata public lifecycle/split/merge simulation tests");
    lib_metadata_sim_public_test_step.dependOn(&run_lib_metadata_sim_public_tests.step);

    const public_api_parity_default_filters = [_][]const u8{
        "public openapi contract module is generated and wired",
        "admin openapi contract module is generated and wired",
        "internal openapi contract module is generated and wired",
        "metadata openapi module generates extractor surface for routed endpoints",
        "usermgr openapi module generates extractor surface for routed endpoints",
        "client openapi module resolves shared refs through owner modules",
        "batch parser accepts supported Go transform op spelling",
        "batch parser rejects every recognized but unsupported transform operator",
        "public table batch handler rejects unsupported missing-document transform before execution",
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
        "api http server executes public Query filter roots and compositions",
        "public table query handler preserves structured filter diagnostics",
        "api http server serves retrieval agent response envelope",
        "api http server serves table batch writes",
        "api http server routes table batches through the batch commit hook",
        "auto bulk max-window request waits for idle finish",
        "auto bulk group writes release leases so idle finish can publish",
        "auto bulk background finish skips entries with active foreground leases",
        "provisioned table write source seeds doc identity namespace from table range",
        "provisioned table write source cached runtime status does not fetch catalog coverage",
        "managed startup catch-up uses provided indexes json without catalog fetch",
        "managed startup catch-up marks FileNotFound index open terminal degraded",
        "managed startup catch-up preserves restore repair debt while index load is terminal",
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
        "api http server serves local index runtime backfill status",
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
        "api http server cluster backup succeeds after load balanced metadata timeout retry",
        "api http server does not advertise a retry after cluster backup side effects begin",
        "public metadata mutation retries transient authority loss only within its deadline",
        "api http server rejects restore before persistence without an asynchronous worker",
        "configured api http server attaches durable restore job persistence",
        "restore job list paginates after authorization filtering",
        "restore metadata intent topology accepts interrupted prefixes and rejects foreign ranges",
        "restore job list bounds authorization scans with an empty continuation page",
        "api http server backs up and restores a table through public routes",
        "api http server cluster overwrite restores from read-only repository without dropping live table",
        "api http server durability-pending restore preserves committed metadata",
        "api http server cluster restore rehydrates extension metadata",
        "api http server prefers metadata-owned restore over inline write-source restore",
        "api http server retries stale metadata table-exists restore race",
        "api http server retries interrupted metadata restore publication",
        "public API request body limit matches Go linear merge contract",
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
        "distributed join context forwards one absolute deadline to every query callback",
        "distributed join search hit JSON normalizes non-finite scores",
        "distributed join unmatched worker returns only unmatched synthetic hits",
        "distributed join applies auth row filter to right table filter query",
        "distributed join preserves native public filters when adding join predicates",
        "scan request errors map to stable client responses",
        "httpx antfly reads map missing table errors to not found",
        "httpx antfly scan honors optional body and documented bad requests",
        "httpx multi batch route uses the batch commit hook and public response contract",
        "httpx stable transaction commit durably hands off recovery before acknowledgement",
        "httpx shared registrar keeps root probes and rejects removed data aliases",
        "httpx storage maintenance routes call typed operations directly",
        "httpx antfly routes require auth and enforce admin middleware",
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
        "public api smoke e2e creates table inserts and queries documents",
        "provisioned table write source routes batch writes across ranges",
        "public api e2e recreates managed embeddings index after corrupt artifact",
        "public api split e2e uses distributed global text stats for bm25 and significant_terms",
        "public api multi-node e2e routes CRUD from a non-host node",
    };
    const public_api_parity_runtime_filters = selectTestFilters(b, &public_api_parity_default_filters);
    const public_api_parity_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = compileFiltersWithAnchors(b, &.{"api module compiles"}, public_api_parity_runtime_filters),
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
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

    const lib_resolution_source_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = &.{
            "DistributedCandidateSource",
            "prefixUpperBoundAlloc",
            "DistributedEntitySink",
        },
    });
    const run_lib_resolution_source_tests = addFilteredTestRunArtifact(b, lib_resolution_source_tests);
    const lib_resolution_source_test_step = b.step("lib-resolution-source-test", "Run focused cross-shard resolution candidate-source and entity-sink tests");
    lib_resolution_source_test_step.dependOn(&run_lib_resolution_source_tests.step);

    const lib_api_auth_default_filters = [_][]const u8{
        "api http server requires auth on public routes when enabled",
        "continuous HA rejects non-replicated public mutations before handlers",
        "HA mutation middleware fails closed for unregistered HTTP methods",
        "continuous HA freezes pre-existing restore workers and resumption",
        "continuous HA allows a configured RemoteApply batch write",
        "api http server document scan requires table read permission",
        "api http server returns retryable not leader when local reconcile lease is lost",
        "api http server returns retryable not leader when metadata proposal is dropped",
        "api http server returns retryable not leader through public table adapter mutation",
        "api http server returns retryable not leader when cluster backup read barrier times out",
        "api http server cluster backup succeeds after load balanced metadata timeout retry",
        "api http server does not advertise a retry after cluster backup side effects begin",
        "typed HA route operation dispatches admin and internal executors",
        "typed HA route operation requires exact bearer token for internal replication routes",
        "api http server forbids non-admin secret access when auth is enabled",
        "api http server query builder requires table read permission when auth is enabled",
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
    const lib_api_auth_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = lib_api_auth_runtime_filters,
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lib_api_auth_tests = addFilteredTestRunArtifactWithRuntimeFilters(
        b,
        lib_api_auth_tests,
        lib_api_auth_runtime_filters,
    );
    run_lib_api_auth_tests.step.dependOn(&openapi_root_check.step);
    const lib_api_auth_test_step = b.step("lib-api-auth-test", "Run focused API auth/usermgr HTTP tests");
    lib_api_auth_test_step.dependOn(&run_lib_api_auth_tests.step);

    const algebraic_dynamic_template_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = &.{
            "dynamic template",
            "child cardinality cache",
            "public algebraic index definitions",
            "metadata.algebraic schema regeneration",
            "db managed algebraic admission builds and reopens",
        },
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_algebraic_dynamic_template_tests = b.addRunArtifact(algebraic_dynamic_template_tests);
    run_algebraic_dynamic_template_tests.step.dependOn(&openapi_root_check.step);
    const algebraic_dynamic_template_test_step = b.step(
        "algebraic-dynamic-template-test",
        "Run focused algebraic dynamic-template and cardinality-cache safety tests",
    );
    algebraic_dynamic_template_test_step.dependOn(&run_algebraic_dynamic_template_tests.step);

    const lib_storage_maintenance_tests = b.addTest(.{
        .root_module = lib_test_mod,
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
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lib_storage_maintenance_tests = addFilteredTestRunArtifact(b, lib_storage_maintenance_tests);
    const lib_storage_maintenance_test_step = b.step("lib-storage-maintenance-test", "Run storage maintenance coordinator ownership and concurrency tests");
    lib_storage_maintenance_test_step.dependOn(&run_lib_storage_maintenance_tests.step);

    const api_connections_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/api_connections_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    antfly_imports.configure(b, api_connections_test_mod, true, true);
    const lib_api_connections_tests = b.addTest(.{
        .root_module = api_connections_test_mod,
        .filters = &.{
            "object probe cache identity covers every bucket and credential source",
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
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lib_api_connections_tests = addFilteredTestRunArtifact(b, lib_api_connections_tests);
    const lib_api_connections_test_step = b.step("lib-api-connections-test", "Run connection inventory and probe-admission tests");
    lib_api_connections_test_step.dependOn(&run_lib_api_connections_tests.step);

    const api_storage_authority_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/api_storage_authority_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    antfly_imports.configure(b, api_storage_authority_test_mod, true, true);
    const lib_api_storage_authority_tests = b.addTest(.{
        .root_module = api_storage_authority_test_mod,
        .filters = &.{
            "public table backup and restore require named connections",
            "public table backup handler rejects an existing backup id",
            "cluster backup APIs require named connections",
            "cluster backup format defaults portable and preserves explicit native",
            "cluster backup and restore reject duplicate table selectors",
            "backup API requests reject unknown operational fields",
            "backup manifest round trips through metadata path",
            "backup manifest round trips through remote objectstore location",
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
            "cluster backup attempt markers reject overlapping cleanup identities",
            "stale owned cluster backup attempt releases fences and retires authoritative head",
            "expired recovery preserves an oversized remote commit record",
            "cluster backup reservation heartbeat fences premature and stale recovery",
            "filesystem cluster backup lease supports the maximum owner identity",
            "filesystem stale attempt reclamation index prevents directory-order starvation",
            "filesystem completed attempt tickets are deleted instead of durably rotated",
            "filesystem attempt publication tolerates concurrent bounded maintenance",
            "filesystem attempt maintenance removes only stale staged tickets",
            "filesystem stale attempt reclamation recovers an abandoned claim",
            "remote stale attempt reclamation cursor prevents prefix starvation",
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
            "backup staging uses configured storage authority and exclusive generations",
            "owned restore verifies declared artifact identity instead of accepting staged bytes",
            "cluster restore repository errors preserve operational failure semantics",
        },
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lib_api_storage_authority_tests = addFilteredTestRunArtifact(b, lib_api_storage_authority_tests);
    const lib_api_storage_authority_test_step = b.step("lib-api-storage-authority-test", "Run remote backup credential-boundary tests");
    lib_api_storage_authority_test_step.dependOn(&run_lib_api_storage_authority_tests.step);

    const api_session_maintenance_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/api_session_maintenance_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    antfly_imports.configure(b, api_session_maintenance_test_mod, true, true);
    const lib_api_session_maintenance_tests = b.addTest(.{
        .root_module = api_session_maintenance_test_mod,
        .filters = &.{
            "durable session mutations publish only after persistence succeeds",
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
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lib_api_session_maintenance_tests = addFilteredTestRunArtifact(b, lib_api_session_maintenance_tests);
    const lib_api_session_maintenance_test_step = b.step("lib-api-session-maintenance-test", "Run durable session concurrency and background-maintenance tests");
    lib_api_session_maintenance_test_step.dependOn(&run_lib_api_session_maintenance_tests.step);

    const lib_api_docid_tests = b.addTest(.{
        .root_module = lib_test_mod,
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
            "distributed graph edge reader carries identity generation",
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
            "api http client preserves group doc identity conflicts",
            "aggregation context rejects non-current identity generation",
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
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const api_transactions_docid_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/api_transactions_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    antfly_imports.configure(b, api_transactions_docid_test_mod, true, true);
    const api_table_writes_docid_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/api_table_writes_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    antfly_imports.configure(b, api_table_writes_docid_test_mod, true, true);
    const api_table_reads_docid_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/api_table_reads_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    antfly_imports.configure(b, api_table_reads_docid_test_mod, true, true);
    const api_public_table_http_docid_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/api_public_table_http_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    antfly_imports.configure(b, api_public_table_http_docid_test_mod, true, true);
    const raft_transition_runtime_docid_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/raft_transition_runtime_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    antfly_imports.configure(b, raft_transition_runtime_docid_test_mod, true, true);
    const api_transactions_docid_tests = b.addTest(.{
        .root_module = api_transactions_docid_test_mod,
        .filters = &.{
            "transaction request parsers reject invalid unsigned integers and accept legacy epochs",
            "transaction read snapshot map keys preserve embedded delimiters",
            "transaction session commit response includes retry hints for doc identity availability conflicts",
        },
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const api_table_writes_docid_tests = b.addTest(.{
        .root_module = api_table_writes_docid_test_mod,
        .filters = selectTestFilters(b, &.{
            "auto bulk group writes release leases so idle finish can publish",
            "provisioned table write source has a finite worker ceiling",
            "provisioned native storage metrics bypass an empty busy write cache",
            "provisioned table write source rejects stale doc identity namespace before write",
            "replicated split destination seeds inherited doc identity before range publication",
            "internal batch parser rejects mixed split transition commands",
            "internal batch parser requires source acknowledgements to be metadata-only",
            "internal batch codec preserves timestamps and rejects public injection",
            "internal batch split identity round trips the full u64 id space",
            "internal batch codec round trips replicated transaction phases",
            "txn resolve codec preserves sync level and accepts legacy requests",
            "distributed txn classifies local and transported visibility outcomes identically",
            "distributed txn coordinator groups by range and commits all participants",
            "stable distributed transaction retry resumes a durable commit decision",
            "distributed txn retries an ambiguous coordinator decision under the same id",
            "distributed txn bounds unresolved coordinator decision retries",
            "distributed txn participant fanout is bounded and concurrent",
            "distributed txn coordinator aborts begun participants on prepare failure",
            "distributed txn coordinator never aborts after durable commit decision",
            "distributed txn coordinator never restarts a transaction id on topology change",
            "db transaction recovery runtime resolves table-group participants through distributed txn resolver",
            "compiled table write boundary transports cancellation and committed failure identity",
            "bound stable single-group transaction retry does not reapply transforms",
            "bound single-group batch reports prepared intent conflicts",
            "provisioned predicate-only batch validates matching and stale versions",
            "provisioned single-group commit preserves transaction graph transform contract",
            "raft single-group fast path excludes graph projection transforms only",
            "resident writer repair state distinguishes clean and metadata-pending writers",
            "api http client preserves group doc identity conflicts",
            "api http client transports txn resolve cancellation and visibility reason",
            "api http client preserves public batch retry safety classifications",
            "api http client forwards bounded raft batch routing context without allocation",
            "api http client preserves committed visibility outcomes for forwarded raft batches",
            "api http client rejects unsupported routed batch protocol without legacy replay",
            "api http client requires explicit not-proposed marker and tracks delivery phase",
            "raft batch aggregation makes failures after an accepted group non-retryable",
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
            "provisioned create index updates cached writer in place",
            "write cache metadata refresh preserves inactive adoptable seed",
            "write cache adopts active just-created db across generation bump",
            "write cache local mutation reuses live stale-generation writer",
            "write cache structural local mutation finishes auto bulk before reuse",
            "write cache local mutation preempts stale startup writer",
            "hosted runtime status prefers live writer over stale hosted snapshot",
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
            "live managed repair upgrades broad recovery alongside status before bounded index repair",
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
            "table runtime snapshot cache clones stored status",
            "table runtime snapshot cache rejects a late stale live observation",
            "table runtime snapshot cache replacement preserves a newer live observation",
            "structural reconcile reconfigures retained writer before managed dense writes",
            "provisioned managed replay tails converge and publish without later traffic",
            "provisioned runtime status overlays live writer replay target without republishing stats",
            "provisioned runtime status live replay overlay preserves cold dense visibility refresh",
            "provisioned runtime status live replay overlay clears ambiguous replay-only backfill",
            "provisioned runtime status live replay overlay preserves non-replay backfill",
            "managed source status-only open drains stale pending close before retry",
            "hosted status-only open drains stale pending close before retry",
            "write cache HA gate clear drains inactive pending closes before returning",
            "write cache retires shared HA generation stale entries before reuse",
        }),
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const api_table_reads_docid_tests = b.addTest(.{
        .root_module = api_table_reads_docid_test_mod,
        .filters = &.{
            "aggregation completeness requires exact total relation",
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
            "encode query request preserves unit grouping ancestor projections",
            "parseRemoteSearchResult preserves grouped hierarchy matches",
            "remote query returns the shard-selected identity generation",
            "remote simple vector query uses vector worker route",
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
            "parseRemoteSearchResult preserves fused index scores",
            "api query contract serializes derived hierarchy ancestry",
            "api query contract preserves the internal grouped unit revision envelope",
            "api query contract serializes hydrated unit ancestor for direct unit hits",
            "table read distributed sorted merge uses catalog runtime schema and rejects incomplete shard windows",
            "provisioned standby read gate permits stale reads and routes non-stale reads to primary",
            "provisioned local query reuses resident generation without readonly open",
            "provisioned auxiliary reads publish resident databases outside read admission",
            "provisioned graph hydrate completes consistency before resident read admission",
            "provisioned consistency read reroutes after topology changes before admission",
            "provisioned stale read admits before routing without a redundant catalog validation",
            "distributed graph source read rejects topology change before aggregation",
            "provisioned reads reject a group removed from the table topology",
            "provisioned table read source falls back from read_index to stale on not leader",
            "catalog backed router skips non-serving relocation placements",
        },
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const api_public_table_http_docid_tests = b.addTest(.{
        .root_module = api_public_table_http_docid_test_mod,
        .filters = &.{
            "public table batch handler maps doc identity unavailable errors",
            "public table batch handler maps write unavailable errors",
            "public table batch handler exposes pending HA durability without claiming rollback",
            "public table batch handler preserves ambiguous write outcomes",
            "public table batch handler maps HA write gate errors",
            "public table batch handler returns concise dense repair backpressure",
            "public table api carries borrowed cancellation into batch execution",
            "public create index exposes retryable storage descriptor exhaustion",
            "public table query handler maps doc identity unavailable errors",
            "public table query handler preserves retryable failure status",
            "public table query handler maps HA read gate errors",
            "public table query handler maps unsupported exact sort",
            "public table query handler exposes stable count-only sort rejection reason",
            "public table query handler surfaces exact sort rejection diagnostics",
            "public table query view handler maps doc identity unavailable errors",
            "public table backup handler accepts portable format",
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
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const raft_transition_runtime_docid_tests = b.addTest(.{
        .root_module = raft_transition_runtime_docid_test_mod,
        .filters = &.{
            "transition runtime fails closed when doc identity reassignment callback is missing",
        },
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const lib_serverless_docid_runtime_filters = &.{
        "serverless query module compiles",
        "search plan rejects internal doc identity controls",
        "serverless graph plans reject internal doc identity controls",
    };
    const lib_serverless_docid_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = compileFiltersWithAnchors(b, &.{"serverless module compiles"}, lib_serverless_docid_runtime_filters),
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lib_api_docid_tests = addFilteredTestRunArtifact(b, lib_api_docid_tests);
    const lib_api_graph_snapshot_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = &.{
            "api distributed graph preserves per-shard snapshots across result refs expansion and hydration",
        },
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lib_api_graph_snapshot_tests = addFilteredTestRunArtifact(b, lib_api_graph_snapshot_tests);
    const lib_api_graph_snapshot_test_step = b.step("lib-api-graph-snapshot-test", "Run distributed graph snapshot-vector regression tests");
    lib_api_graph_snapshot_test_step.dependOn(&run_lib_api_graph_snapshot_tests.step);
    const api_derived_coverage_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/api_derived_coverage_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    antfly_imports.configure(b, api_derived_coverage_test_mod, true, true);
    const lib_api_derived_coverage_tests = b.addTest(.{
        .root_module = api_derived_coverage_test_mod,
        .filters = &.{
            "coverage policy accepts only the public embeddings contract",
            "coverage policy assigns persistent private incarnations only to embeddings",
            "restore manifest preserves trusted coverage incarnation metadata",
            "public index config encoders redact coverage incarnation",
            "identical index mutation retries preserve coverage incarnation",
            "derived coverage evaluation is policy exact and observation gated",
            "settled terminal enrichment debt is degraded rather than rebuilding",
            "derived coverage aggregation rejects mixed config observations",
            "index status exposes compact repair state without internal diagnostics",
            "rebuild quarantine remains an explicit failed public index status",
            "index repair aggregation exposes a waiting shard over rebuilding shards",
            "derived coverage aggregation rejects stale index incarnations",
            "derived coverage reasons expose counter mismatch",
            "derived coverage rejects unknown freshness for aggregate and shard views",
            "cached all-skipped coverage observation is a runtime fact",
            "live writer artifact regression keeps authoritative source deletions",
            "table runtime snapshot cache clones stored status",
            "table runtime snapshot cache batch publication is table epoch atomic",
            "table runtime snapshot cache lifecycle transition replaces and fences observations",
            "table runtime snapshot cache batch preserves newer group observations",
            "runtime status cache stable absence removal retires the old table epoch",
            "partial coverage embeddings readiness counts skipped source units",
            "partial coverage embeddings readiness does not mask pending enrichment",
            "derived coverage reasons deduplicate overlapping freshness signals",
            "managed embeddings readiness ignores inactive stale catch-up after rate-limit recovery",
            "single embeddings index encoder keeps retrying coverage gaps catch-up coherent",
            "managed embeddings skipped terminal sources complete backfill without fabricating replay debt",
            "enrichment aggregation preserves telemetry and fences mixed checkpoint identity",
            "table storage status indexes one distributed snapshot by table and owner",
            "distributed join uses row estimates when byte statistics are unavailable",
            "distributed join preserves no-stat strategy when one side is unknown",
            "api http server join planner uses complete fresh local stats before metadata publication",
            "external embeddings index readiness does not require table doc coverage",
            "api http server preserves public query availability errors",
            "api http maps missing physical index only for rebuilding lifecycle",
            "api http missing index classification requires active rebuild evidence",
            "api http lifecycle classification preserves catching-up writer beside fresh read snapshot",
            "remote rebuild quarantine preserves its source and index failure",
            "api http server create index waits for exact target config projection",
        },
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lib_api_derived_coverage_tests = addFilteredTestRunArtifact(b, lib_api_derived_coverage_tests);
    run_lib_api_derived_coverage_tests.step.dependOn(&openapi_root_check.step);
    const lib_api_derived_coverage_test_step = b.step("lib-api-derived-coverage-test", "Run focused public derived coverage tests");
    lib_api_derived_coverage_test_step.dependOn(&run_lib_api_derived_coverage_tests.step);
    const run_lib_serverless_docid_tests = addFilteredTestRunArtifactWithRuntimeFilters(
        b,
        lib_serverless_docid_tests,
        lib_serverless_docid_runtime_filters,
    );
    const run_api_transactions_docid_tests = addFilteredTestRunArtifact(b, api_transactions_docid_tests);
    const run_api_table_writes_docid_tests = addFilteredTestRunArtifact(b, api_table_writes_docid_tests);
    const run_api_table_reads_docid_tests = addFilteredTestRunArtifact(b, api_table_reads_docid_tests);
    const run_api_public_table_http_docid_tests = addFilteredTestRunArtifact(b, api_public_table_http_docid_tests);
    const run_raft_transition_runtime_docid_tests = addFilteredTestRunArtifact(b, raft_transition_runtime_docid_tests);
    const api_transactions_docid_test_step = b.step("api-transactions-docid-test", "Run focused API transaction tests");
    api_transactions_docid_test_step.dependOn(&run_api_transactions_docid_tests.step);
    const api_table_writes_docid_test_step = b.step("api-table-writes-docid-test", "Run focused API table write tests");
    api_table_writes_docid_test_step.dependOn(&run_api_table_writes_docid_tests.step);
    const api_table_writes_production_regression_tests = b.addTest(.{
        .root_module = api_table_writes_docid_test_mod,
        .filters = &.{
            "table write source restore acquires lifecycle unless caller reserves it",
            "provisioned native backup restore repeats through shared read and write owners",
            "provisioned create succeeds when post-commit runtime status is fenced",
            "provisioned create reuses a generation opened by startup reconciliation",
            "hosted backup forwarding preserves external io authority",
            "provisioned table restore retry repairs exact incomplete restore state through active writer",
            "provisioned table restore preparation blocks writes and competing structural mutation",
            "provisioned table restore preparation blocks writes while allowing reads",
            "provisioned table transition activity excludes writers but preserves reads",
            "provisioned table transition waiter queues ahead of later writers",
            "provisioned table transition waiter queues ahead of later readers",
            "provisioned table write source read request permits replicated apply activity",
            "provisioned table group operation waiter queues ahead of later readers",
            "provisioned table write source drop table cancels index repair before structural admission",
            "provisioned table write request queues structural reconcile ahead of later writes",
            "structural reconcile reservation defers metadata group refresh without blocking admitted work",
            "queued structural reconcile reserves write admission before its worker starts",
            "queued index catch-up does not reserve table write admission",
            "index reconciliation request enqueues a catalog-deleted target without create admission",
            "structural reconcile retry backoff is bounded",
            "structural reconcile productive quantum yields immediately while blocked work backs off",
            "structural reconcile retries a transient worker failure",
            "structural reconcile returns a bounded pending quantum while a group is busy",
            "structural reconcile publishes durable index repair debt once per group",
            "resident DB retry preparation waits outside admission for writer publication",
            "admitted resident DB lease never waits for an in-flight writer publication",
            "write cache local mutation preempts stale startup writer",
            "structural reconcile pending set never revisits completed groups",
            "structural reconcile completion rejects ranges added after contract capture",
            "structural reconcile production topology fence rejects ranges added after capture",
            "structural reconcile production catalog fails closed without table publication fence",
            "structural reconcile fences incarnation initialization and discards empty topology",
            "provisioned structural reconcile blocks table write admission",
            "provisioned schema reconcile keeps reads and status available",
            "busy startup open preserves fresh writer runtime status",
            "managed startup catch-up marks FileNotFound index open terminal degraded",
            "managed startup catch-up preserves restore repair debt while index load is terminal",
            "managed startup catch-up allocation failure preserves bounded retry",
            "standby HA replay reconciles managed indexes without opening the public write gate",
            "managed structural catch-up delegates durable generation repair without rebuilding inline",
            "managed structural catch-up leaves pending enrichment with the asynchronous owner",
            "db managed vector admission captures writes while durable repair is pending",
            "db managed algebraic admission builds and reopens",
            "db algebraic generation build yields and resumes from its durable source cursor",
            "db forced algebraic repair persists an operator generation intent before execution",
            "db algebraic post-commit activation crash recovers through generation repair",
            "table provisioner admits algebraic index on a non-empty table through generation repair",
            "target index reconciliation never mutates sibling indexes",
            "target index reconciliation retires orphaned inline enrichments after deletion retry",
            "managed db open modes never drain resolver backfill on raft apply",
            "replica root reconcile enqueues newly admitted managed full text repair",
            "managed repair visibility edges retire cached readers and runtime status",
            "table runtime snapshot cache invalidation fences a stale observed publisher",
            "runtime status hook orders completed observation without crossing invalidation",
            "structural repair publication advances the table lifecycle epoch",
            "table runtime snapshot cache live publication does not starve structural refresh",
            "table runtime snapshot cache preserves live completion over regressing persisted projection",
            "table runtime snapshot cache table fences isolate unrelated invalidations",
            "runtime status cache publishes unaffected tables and retries only invalidated tables",
            "runtime status cache stable absence removal retires the old table epoch",
            "provisioned named index repair keeps group queued for aggregate debt audit",
            "dirty table tracking stays bounded to writer cache ownership",
            "writer cache eviction retires dirty ownership after the last cache owner",
            "prepared writer open evicts an inactive sibling group before retrying descriptor pressure",
            "prepared writer open reclaims descriptor capacity from startup cache",
            "forwarded write sources use the local writer owner dirty lifecycle",
            "HA ownership transition invalidates cached visibility and dirty identities",
            "HA ownership transition serializes with active writer cache mutation",
            "startup cache clear retires dirty identity without a serving owner",
            "dirty auto bulk writer publishes runtime status without closing the cached writer",
            "split transition auto bulk publication retries while a writer lease is active",
            "median key lookup reuses startup writer instead of reopening its root",
            "write cache retirement is allocation-free after entry installation",
            "writer cache bulk transition fences only its table",
            "provisioned read cache retirement is allocation-free after entry installation",
            "provisioned group storage prunes stale visible root generations",
            "provisioned Raft snapshot install publishes a fenced group generation",
            "provisioned Raft snapshot install rejects a changed catalog contract",
            "prepared generation publication rolls back before serving admission",
            "prepared generation reconciliation rolls back an exchanged candidate",
            "prepared first generation reconciliation removes an unvalidated candidate",
            "committed generation reconciliation preserves the validated candidate",
            "generation publication marker parsing preserves allocator exhaustion",
        },
    });
    const run_api_table_writes_production_regression_tests = addFilteredTestRunArtifact(b, api_table_writes_production_regression_tests);
    const run_api_table_writes_production_regression_unit_tests = addFilteredTestRunArtifact(b, api_table_writes_production_regression_tests);
    // These stateful suites each open several DB/index runtimes. Keep their
    // aggregate-gate runs on one lane so bounded CI hosts do not convert
    // aggregate memory pressure into allocator failures. Focused aliases use
    // the independent run artifacts above.
    run_api_table_writes_production_regression_unit_tests.step.dependOn(&run_public_api_parity_aggregate_tests.step);
    const api_table_writes_production_regression_step = b.step("api-table-writes-production-regression-test", "Run focused restore and writer-cache lifecycle regressions");
    api_table_writes_production_regression_step.dependOn(&run_api_table_writes_production_regression_tests.step);
    const api_table_writes_restore_repeat_step = b.step("api-table-writes-restore-repeat-test", "Run the focused shared-owner native restore regression");
    api_table_writes_restore_repeat_step.dependOn(&run_api_table_writes_production_regression_tests.step);
    const api_table_writes_cache_lifecycle_step = b.step("api-table-writes-cache-lifecycle-test", "Run focused writer-cache dirty ownership regressions");
    api_table_writes_cache_lifecycle_step.dependOn(&run_api_table_writes_production_regression_tests.step);
    const api_table_reads_docid_test_step = b.step("api-table-reads-docid-test", "Run focused API table read tests");
    api_table_reads_docid_test_step.dependOn(&run_api_table_reads_docid_tests.step);
    const api_public_table_http_docid_test_step = b.step("api-public-table-http-docid-test", "Run focused public table HTTP read-unavailable tests");
    api_public_table_http_docid_test_step.dependOn(&run_api_public_table_http_docid_tests.step);
    const lib_docid_lifecycle_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = &.{
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
        },
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lib_docid_lifecycle_tests = addFilteredTestRunArtifact(b, lib_docid_lifecycle_tests);
    const docid_lifecycle_test_step = b.step("docid-lifecycle-test", "Run focused DOCID lifecycle and distributed snapshot hardening tests");
    docid_lifecycle_test_step.dependOn(&run_lib_docid_lifecycle_tests.step);
    docid_lifecycle_test_step.dependOn(&run_api_transactions_docid_tests.step);
    docid_lifecycle_test_step.dependOn(&run_api_table_reads_docid_tests.step);
    docid_lifecycle_test_step.dependOn(&run_api_table_writes_docid_tests.step);
    docid_lifecycle_test_step.dependOn(&run_api_public_table_http_docid_tests.step);
    docid_lifecycle_test_step.dependOn(&run_raft_transition_runtime_docid_tests.step);
    docid_lifecycle_test_step.dependOn(&run_lib_db_result_shape_tests.step);

    const docid_operational_hardening_test_step = b.step("docid-operational-hardening-test", "Run extended DOCID lifecycle, metadata chaos, and compaction hardening tests");
    docid_operational_hardening_test_step.dependOn(docid_lifecycle_test_step);
    docid_operational_hardening_test_step.dependOn(lib_metadata_transition_chaos_test_step);
    docid_operational_hardening_test_step.dependOn(lib_metadata_public_chaos_test_step);
    docid_operational_hardening_test_step.dependOn(lib_lsm_backend_chaos_test_step);

    const lib_api_docid_test_step = b.step("lib-api-docid-test", "Run focused API DOCID boundary tests");
    lib_api_docid_test_step.dependOn(&run_lib_api_docid_tests.step);
    lib_api_docid_test_step.dependOn(&run_lib_serverless_docid_tests.step);
    lib_api_docid_test_step.dependOn(&run_api_transactions_docid_tests.step);
    lib_api_docid_test_step.dependOn(&run_api_table_reads_docid_tests.step);
    lib_api_docid_test_step.dependOn(&run_api_table_writes_docid_tests.step);
    lib_api_docid_test_step.dependOn(&run_api_public_table_http_docid_tests.step);
    lib_api_docid_test_step.dependOn(&run_raft_transition_runtime_docid_tests.step);
    lib_api_docid_test_step.dependOn(&run_lib_data_storage_tests.step);
    lib_api_docid_test_step.dependOn(&run_lib_data_runtime_tests.step);
    lib_api_docid_test_step.dependOn(&run_lib_metadata_sim_smoke_tests.step);
    lib_api_docid_test_step.dependOn(&run_lib_metadata_sim_public_tests.step);
    lib_api_docid_test_step.dependOn(&run_lib_metadata_vopr_tests.step);
    lib_api_docid_test_step.dependOn(&run_lib_metadata_vopr_chaos_tests.step);
    lib_api_docid_test_step.dependOn(lib_metadata_public_chaos_test_step);
    lib_api_docid_test_step.dependOn(&run_lib_db_result_shape_tests.step);

    const api_backup_restore_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/api_backup_restore_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    antfly_imports.configure(b, api_backup_restore_test_mod, true, true);
    const lib_api_standalone_backup_restore_tests = b.addTest(.{
        .root_module = api_backup_restore_test_mod,
        .filters = &.{
            "public api standalone-like e2e backs up drops and restores a table",
            "api restore rollback preserves a concurrently replaced table definition",
            "api http server cluster overwrite restores from read-only repository without dropping live table",
            "api http server rejects an empty cluster backup without publishing a manifest",
            "backup manifest validation rejects ambiguous or unbound artifacts",
            "cluster backup manifest rejects incomplete coverage",
            "restore source identities are bounded and canonical",
            "portable backup integrity rejects changed staged bytes",
            "db explicit restore runtime repair repairs managed chunked dense embeddings once for restored shard",
            "db incomplete deferred restore import recovers before runtime repair",
            "db restore state uses strict structured content identity markers",
        },
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lib_api_standalone_backup_restore_tests = addFilteredTestRunArtifact(b, lib_api_standalone_backup_restore_tests);
    const lib_api_standalone_backup_restore_test_step = b.step("lib-api-standalone-backup-restore-test", "Run the focused standalone-like backup/restore e2e test");
    lib_api_standalone_backup_restore_test_step.dependOn(&run_lib_api_standalone_backup_restore_tests.step);

    const openapi_root_check_step = b.step("openapi-root-check", "Check that the bundled root OpenAPI spec matches the modular Zig specs");
    openapi_root_check_step.dependOn(&openapi_root_check.step);

    const lib_metadata_sim_forward_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = &.{"forwards public table io"},
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lib_metadata_sim_forward_tests = addFilteredTestRunArtifact(b, lib_metadata_sim_forward_tests);
    const lib_metadata_sim_forward_test_step = b.step("lib-metadata-sim-forward-test", "Run public table IO forwarding simulation tests only");
    lib_metadata_sim_forward_test_step.dependOn(&run_lib_metadata_sim_forward_tests.step);

    const lib_metadata_service_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = &.{
            "metadata service ",
            "table workflow can drive real metadata service topology and split setup",
            "table workflow can drive placement intents through the real metadata control loop",
            "metadata http service catalog cache is independent from volatile projection traffic",
        },
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lib_metadata_service_tests = addFilteredTestRunArtifact(b, lib_metadata_service_tests);
    const lib_metadata_service_test_step = b.step("lib-metadata-service-test", "Run metadata service/control-loop integration tests");
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
        "metadata http server serves status and filtered admin routes",
        "metadata http server accepts internal reallocate and split merge routes",
        "metadata http server returns 400 for invalid internal restore backup locations",
        "metadata http server returns retryable authority response when reconcile lease is not held",
    };
    const lib_metadata_logic_runtime_filters = selectTestFilters(b, &lib_metadata_logic_default_filters);
    const lib_metadata_logic_tests = b.addTest(.{
        .root_module = lib_test_mod,
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
    const lib_metadata_logic_test_step = b.step("lib-metadata-logic-test", "Run metadata logic/state/planner tests");
    lib_metadata_logic_test_step.dependOn(&run_lib_metadata_logic_tests.step);

    const lib_storage_default_filters = [_][]const u8{
        "storage.",
    };
    const lib_storage_runtime_filters = selectTestFilters(b, &lib_storage_default_filters);
    const lib_storage_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = lib_storage_runtime_filters,
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lib_storage_tests = addFilteredTestRunArtifact(b, lib_storage_tests);
    addRuntimeSkipTestFilters(run_lib_storage_tests, &release_scale_test_filters);
    const lib_storage_test_step = b.step("lib-storage-test", "Run root-module storage tests only");
    lib_storage_test_step.dependOn(&run_lib_storage_tests.step);

    const ha_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = &.{"storage.ha"},
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_ha_tests = addFilteredTestRunArtifact(b, ha_tests);
    const ha_test_step = b.step("ha-test", "Run hot-standby HA storage tests");
    ha_test_step.dependOn(&run_ha_tests.step);

    // cmd/ha.zig is owned by the distributed runtime unit. Keep its focused
    // parser root inside pkg/antfly/src so relative imports stay within the Zig
    // module boundary, without pulling the command back into the CLI unit.
    const ha_cli_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/ha_cmd_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    ha_cli_test_mod.addImport("antfly-zig", lib_mod);
    const ha_cli_tests = b.addTest(.{
        .root_module = ha_cli_test_mod,
        .filters = &.{"ha cmd artifact"},
    });
    const run_ha_cli_tests = b.addRunArtifact(ha_cli_tests);
    ha_test_step.dependOn(&run_ha_cli_tests.step);

    const lsm_backend_runtime_filters = selectTestFilters(b, &.{"storage.lsm_backend."});
    const lsm_backend_tests = b.addTest(.{
        .root_module = lib_test_mod,
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
        "batch reservation is atomic across inference resource slices",
        "classified batch reservation distinguishes size from contention",
        "resource manager observes over-budget external usage",
        "resource manager evaluates projected admission with configured action",
        "resource manager bounds soft write throttling without waiting for compaction publication",
        "resource manager records index repair activation pause separately from cleanup",
        "catchUpIndex refuses to open an apply window after its deadline",
        "cache reports shared byte usage to resource manager",
        "lsm backend resource manager throttles projected immutable state",
        "lsm backend resource manager rejects before wal apply",
        "derived backlog tracker accounts and releases payload bytes",
        "derived backlog tracker fails closed when sequence accounting allocation fails",
        "derived backlog tracker bounds sequence-only admission drain window",
        "hbc shared cache namespaces entries",
        "hbc shared cache evicts across namespaces under one resource budget",
        "hbc cache reports byte usage to resource manager",
        "hbc cache shrinks to resource budget under pressure",
        "resource-managed mapped residency evicts cold segments and preserves hot mappings",
        "provisioned group storage derives all resource budgets",
        "resource manager capacity source is immutable after composition",
        "capacity reservation revalidation fails closed when available space falls",
        "resource manager background deferral follows slice policy",
        "budgeted allocator admits before allocation and releases exact live bytes",
        "budgeted allocator allows concurrent operations within the shared hard limit",
        "budgeted allocator amortizes manager reservations and releases idle credit",
    };
    // Retain the API declaration walk that owns provisioned_storage. Zig
    // compile filters otherwise prune that module before the exact runtime
    // filter can select its resource-budget test.
    const resource_budget_compile_filters = [_][]const u8{"api module compiles"} ++ resource_budget_runtime_filters;
    const resource_budget_tests = b.addTest(.{
        .root_module = lib_test_mod,
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
        .root_module = lib_test_mod,
        .filters = &.{
            "index repair state root-generation reset atomically rebinds replacement debt",
            "index repair state persists through backend storage",
            "index repair state persists intent and provisional replay pin atomically",
            "db managed operator repair persists intent without running reconstruction inline",
            "db root generation rollover preserves activated repair debt fail closed",
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
            "db dense artifact surplus uses quarantined generation replacement",
            "db dense artifact planner does not let stale status override authoritative counter",
            "db dense artifact counter bootstrap combines snapshot with concurrent write delta",
            "db dense artifact counter bootstrap restarts from a fresh snapshot",
            "db dense artifact counter bootstrap fences stale concurrent attempt",
            "db malformed quarantined dense config does not block healthy artifact counters",
            "db query repair gate revalidates stale debt",
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
        .filters = &.{"index status exposes compact repair state without internal diagnostics"},
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

    const sim_test_step = b.step("sim-test", "Run mocked-time Antfly simulation suites");
    sim_test_step.dependOn(&run_lib_metadata_sim_smoke_tests.step);
    sim_test_step.dependOn(&run_lib_metadata_vopr_tests.step);
    sim_test_step.dependOn(&run_lib_raft_sim_tests.step);

    const integration_test_step = b.step("integration-test", "Run focused real HTTP and public API integration suites");
    integration_test_step.dependOn(&run_lib_metadata_sim_public_tests.step);
    integration_test_step.dependOn(&run_lib_metadata_sim_forward_tests.step);
    // Both aggregates share this run node, so the default test DAG executes
    // the stateful parity suite once. The focused alias remains independent.
    integration_test_step.dependOn(&run_public_api_parity_aggregate_tests.step);

    const chaos_test_step = b.step("chaos-test", "Run bounded generated chaos campaigns with labeled progress");
    var chaos_progress_tail: ?*std.Build.Step = null;
    chaos_progress_tail = chainLabeledRun(b, lib_metadata_vopr_chaos_tests, "lib-metadata-vopr-chaos-test", chaos_progress_tail);
    chaos_progress_tail = chainLabeledRun(b, lib_lsm_backend_chaos_tests, "lib-lsm-backend-chaos-test", chaos_progress_tail);
    chaos_progress_tail = chainLabeledRun(b, lib_ha_chaos_tests, "ha-chaos-test", chaos_progress_tail);
    chaos_test_step.dependOn(chaos_progress_tail.?);

    const chaos_soak_test_step = b.step("chaos-soak-test", "Run broad legacy metadata and raft chaos simulation soaks");
    var chaos_soak_progress_tail: ?*std.Build.Step = null;
    chaos_soak_progress_tail = chainLabeledFilteredTests(b, lib_test_mod, "lib-metadata-transition-chaos-test", lib_metadata_transition_chaos_filters, chaos_soak_progress_tail);
    chaos_soak_progress_tail = chainLabeledFilteredTests(b, lib_test_mod, "lib-metadata-public-chaos-test", lib_metadata_public_chaos_filters, chaos_soak_progress_tail);
    chaos_soak_progress_tail = chainLabeledFilteredTests(b, lib_test_mod, "lib-metadata-placement-chaos-test", lib_metadata_placement_chaos_filters, chaos_soak_progress_tail);
    chaos_soak_progress_tail = chainLabeledRun(b, lib_raft_chaos_tests, "lib-raft-chaos-test", chaos_soak_progress_tail);
    chaos_soak_test_step.dependOn(chaos_soak_progress_tail.?);
    soak_test_step.dependOn(chaos_soak_test_step);

    const template_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/template_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    antfly_imports.configure(b, template_test_mod, false, true);
    const lib_template_tests = b.addTest(.{
        .root_module = template_test_mod,
    });
    const run_lib_template_tests = b.addRunArtifact(lib_template_tests);
    const lib_template_test_step = b.step("lib-template-test", "Run template rendering tests");
    lib_template_test_step.dependOn(&run_lib_template_tests.step);

    const audio_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/audio_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    antfly_imports.configure(b, audio_test_mod, false, true);
    const lib_audio_tests = b.addTest(.{
        .root_module = audio_test_mod,
    });
    const run_lib_audio_tests = b.addRunArtifact(lib_audio_tests);
    const lib_transcribing_tests = b.addTest(.{
        .root_module = transcribing_mod,
    });
    const run_lib_transcribing_tests = b.addRunArtifact(lib_transcribing_tests);
    const lib_audio_test_step = b.step("lib-audio-test", "Run audio transcribing and synthesizing runtime tests");
    lib_audio_test_step.dependOn(&run_lib_audio_tests.step);
    lib_audio_test_step.dependOn(&run_lib_transcribing_tests.step);

    const lib_audio_xiph_conformance = b.addExecutable(.{
        .name = "lib-audio-xiph-conformance",
        .root_module = b.createModule(.{
            .root_source_file = b.path("lib/audio/audio_xiph_corpora_e2e.zig"),
            .target = target,
            .optimize = .ReleaseFast,
        }),
    });
    lib_audio_xiph_conformance.root_module.addImport("build_options", inference_build_options_mod);
    if (inference_ffmpeg_paths) |ffmpeg_paths| {
        lib_audio_xiph_conformance.root_module.addIncludePath(.{ .cwd_relative = ffmpeg_paths.include_dir });
    }
    lib_audio_xiph_conformance.root_module.link_libc = true;

    const lib_audio_misc_conformance = b.addExecutable(.{
        .name = "lib-audio-misc-conformance",
        .root_module = b.createModule(.{
            .root_source_file = b.path("lib/audio/audio_misc_corpora_e2e.zig"),
            .target = target,
            .optimize = .ReleaseFast,
        }),
    });
    lib_audio_misc_conformance.root_module.addImport("build_options", inference_build_options_mod);
    if (inference_ffmpeg_paths) |ffmpeg_paths| {
        lib_audio_misc_conformance.root_module.addIncludePath(.{ .cwd_relative = ffmpeg_paths.include_dir });
    }
    lib_audio_misc_conformance.root_module.link_libc = true;

    const fetch_lib_audio_xiph_conformance = b.addRunArtifact(lib_audio_xiph_conformance);
    fetch_lib_audio_xiph_conformance.addArg("fetch");
    fetch_lib_audio_xiph_conformance.addArg("/tmp/audio-xiph-corpora");

    const fetch_lib_audio_misc_conformance = b.addRunArtifact(lib_audio_misc_conformance);
    fetch_lib_audio_misc_conformance.addArg("fetch");
    fetch_lib_audio_misc_conformance.addArg("/tmp/audio-misc-corpora");

    const lib_audio_conformance_fetch_step = b.step("lib-audio-conformance-fetch", "Fetch the lib/audio external conformance fixtures");
    lib_audio_conformance_fetch_step.dependOn(&fetch_lib_audio_xiph_conformance.step);
    lib_audio_conformance_fetch_step.dependOn(&fetch_lib_audio_misc_conformance.step);

    const fetch_lib_audio_xiph_conformance_quiet = b.addRunArtifact(lib_audio_xiph_conformance);
    fetch_lib_audio_xiph_conformance_quiet.addArg("fetch");
    fetch_lib_audio_xiph_conformance_quiet.addArg("/tmp/audio-xiph-corpora");
    const fetch_lib_audio_xiph_conformance_quiet_step = expectQuietSuccess(fetch_lib_audio_xiph_conformance_quiet);

    const fetch_lib_audio_misc_conformance_quiet = b.addRunArtifact(lib_audio_misc_conformance);
    fetch_lib_audio_misc_conformance_quiet.addArg("fetch");
    fetch_lib_audio_misc_conformance_quiet.addArg("/tmp/audio-misc-corpora");
    const fetch_lib_audio_misc_conformance_quiet_step = expectQuietSuccess(fetch_lib_audio_misc_conformance_quiet);

    const run_lib_audio_xiph_conformance = b.addRunArtifact(lib_audio_xiph_conformance);
    run_lib_audio_xiph_conformance.addArg("run");
    run_lib_audio_xiph_conformance.addArg("/tmp/audio-xiph-corpora");
    run_lib_audio_xiph_conformance.addArg("--no-fetch");

    const run_lib_audio_misc_conformance = b.addRunArtifact(lib_audio_misc_conformance);
    run_lib_audio_misc_conformance.addArg("run");
    run_lib_audio_misc_conformance.addArg("/tmp/audio-misc-corpora");
    run_lib_audio_misc_conformance.addArg("--no-fetch");

    const lib_audio_conformance_run_step = b.step("lib-audio-conformance-run", "Run lib/audio conformance suites without fetching fixtures");
    lib_audio_conformance_run_step.dependOn(&run_lib_audio_xiph_conformance.step);
    lib_audio_conformance_run_step.dependOn(&run_lib_audio_misc_conformance.step);

    const run_lib_audio_xiph_conformance_after_fetch = b.addRunArtifact(lib_audio_xiph_conformance);
    run_lib_audio_xiph_conformance_after_fetch.addArg("run");
    run_lib_audio_xiph_conformance_after_fetch.addArg("/tmp/audio-xiph-corpora");
    run_lib_audio_xiph_conformance_after_fetch.addArg("--no-fetch");
    run_lib_audio_xiph_conformance_after_fetch.step.dependOn(&fetch_lib_audio_xiph_conformance.step);

    const run_lib_audio_misc_conformance_after_fetch = b.addRunArtifact(lib_audio_misc_conformance);
    run_lib_audio_misc_conformance_after_fetch.addArg("run");
    run_lib_audio_misc_conformance_after_fetch.addArg("/tmp/audio-misc-corpora");
    run_lib_audio_misc_conformance_after_fetch.addArg("--no-fetch");
    run_lib_audio_misc_conformance_after_fetch.step.dependOn(&fetch_lib_audio_misc_conformance.step);

    const lib_audio_conformance_step = b.step("lib-audio-conformance", "Fetch and run lib/audio conformance suites");
    lib_audio_conformance_step.dependOn(&run_lib_audio_xiph_conformance_after_fetch.step);
    lib_audio_conformance_step.dependOn(&run_lib_audio_misc_conformance_after_fetch.step);

    const run_lib_audio_xiph_conformance_after_fetch_quiet = b.addRunArtifact(lib_audio_xiph_conformance);
    run_lib_audio_xiph_conformance_after_fetch_quiet.addArg("run");
    run_lib_audio_xiph_conformance_after_fetch_quiet.addArg("/tmp/audio-xiph-corpora");
    run_lib_audio_xiph_conformance_after_fetch_quiet.addArg("--no-fetch");
    run_lib_audio_xiph_conformance_after_fetch_quiet.step.dependOn(fetch_lib_audio_xiph_conformance_quiet_step);
    const run_lib_audio_xiph_conformance_after_fetch_quiet_step = expectQuietSuccess(run_lib_audio_xiph_conformance_after_fetch_quiet);

    const run_lib_audio_misc_conformance_after_fetch_quiet = b.addRunArtifact(lib_audio_misc_conformance);
    run_lib_audio_misc_conformance_after_fetch_quiet.addArg("run");
    run_lib_audio_misc_conformance_after_fetch_quiet.addArg("/tmp/audio-misc-corpora");
    run_lib_audio_misc_conformance_after_fetch_quiet.addArg("--no-fetch");
    run_lib_audio_misc_conformance_after_fetch_quiet.step.dependOn(fetch_lib_audio_misc_conformance_quiet_step);
    const run_lib_audio_misc_conformance_after_fetch_quiet_step = expectQuietSuccess(run_lib_audio_misc_conformance_after_fetch_quiet);
    conformance_test_step.dependOn(run_lib_audio_xiph_conformance_after_fetch_quiet_step);
    conformance_test_step.dependOn(run_lib_audio_misc_conformance_after_fetch_quiet_step);

    const standalone_runtime_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/standalone_runtime_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    var standalone_runtime_imports = antfly_imports;
    standalone_runtime_imports.build_options = standalone_runtime_build_options;
    standalone_runtime_imports.configure(b, standalone_runtime_test_mod, true, true);
    const usermgr_storage_standalone_runtime_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/usermgr/storage_imports.zig"),
        .target = target,
        .optimize = optimize,
    });
    usermgr_storage_standalone_runtime_test_mod.addImport("antfly_root", standalone_runtime_test_mod);
    usermgr_storage_standalone_runtime_test_mod.addImport("antfly_platform", platform_mod);
    standalone_runtime_test_mod.addImport("usermgr_storage", usermgr_storage_standalone_runtime_test_mod);
    const lib_standalone_runtime_tests = b.addTest(.{
        .root_module = standalone_runtime_test_mod,
        .filters = &.{
            "standalone runtime module compiles",
            "standalone runtime local generator accepts media url data uris",
            "standalone runtime local dense embed preserves borrowed binary media",
            "standalone runtime local generator preflights mixed resident media exactly",
            "standalone runtime local generator refuses decode allocation beyond preflight",
            "standalone inference middleware reuses public API authentication",
            "standalone CORS middleware enforces dynamic configuration",
            "standalone runtime local replica reconcile permit blocks only active startup catch-up",
            "standalone runtime parses experimental flag",
            "standalone runtime antfarm path guards keep api routes reserved",
            "standalone startup checkpoint readiness requires applied and safe-read progress",
            "parse cli accepts config path",
            "parse cli accepts secret store path",
            "parse cli accepts ARD identity flags",
            "parse cli accepts canonical host port and models dir flags",
            "parse cli accepts HA primary runtime flags",
            "parse cli accepts HA primary sync policy flags",
            "parse cli accepts HA standby runtime flags",
            "standalone HA standby replication flags require upstream and slot",
            "standalone HA string classifier distinguishes missing padded and valid values",
            "standalone HA runtime rejects ambiguous role flags",
            "standalone continuous HA mutation guard follows role lifecycle",
            "antfly config uses cli override before common config",
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
            "standalone kernel JIT mode precedence is CLI then environment then config",
            "inference config falls back to common config",
            "standalone prompt cache detaches resource observer before owner teardown",
            "inference admission bridge charges combined native residency to resource manager",
            "standalone inference keep alive parses compound durations and zero",
            "standalone linked inference ABI validates the supported function-table prefix",
            "standalone local inference lifetime distinguishes deadline from upstream cancellation",
            "standalone runtime resolves paths from common storage base dir",
            "standalone runtime resolves extension package store env before local default",
            "standalone Lite enforces one shard and one replica",
            "standalone Lite adoption preserves deterministic embedded document identity",
            "standalone validates effective Lite CLI and config settings",
            "standalone metadata rolls back an undurable catalog mutation",
            "standalone metadata rejects corrupt catalog without double-freeing owned paths",
            "standalone metadata finalizes schema migration from resident runtime evidence",
            "standalone unified server lifecycle propagates startup failure",
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
    });
    const lib_standalone_runtime_test_step = b.step("lib-standalone-runtime-test", "Run focused standalone runtime tests");
    const run_lib_standalone_runtime_tests = addFilteredTestRunArtifact(b, lib_standalone_runtime_tests);
    lib_standalone_runtime_test_step.dependOn(&run_lib_standalone_runtime_tests.step);

    const raft_test_step = b.step("raft-test", "Run raft integration unit tests");
    raft_test_step.dependOn(&run_raft_unit_tests.step);
    raft_test_step.dependOn(&run_raft_runtime_tests.step);
    raft_test_step.dependOn(&run_raft_restore_tests.step);
    raft_test_step.dependOn(&run_raft_library_tests.step);
    raft_test_step.dependOn(&run_raft_ready_continuation_tests.step);

    const raft_runtime_test_step = b.step("raft-runtime-test", "Run focused managed Raft runtime tests");
    raft_runtime_test_step.dependOn(&run_raft_runtime_tests.step);
    raft_runtime_test_step.dependOn(&run_raft_ready_continuation_tests.step);

    const raft_restore_test_step = b.step("raft-restore-test", "Run focused Raft restore authority and restart tests");
    raft_restore_test_step.dependOn(&run_raft_restore_tests.step);

    const raft_transport_test_step = b.step("raft-transport-test", "Run raft transport unit tests");
    raft_transport_test_step.dependOn(&run_raft_transport_tests.step);

    unit_test_step.dependOn(&run_lib_regex_tests.step);
    unit_test_step.dependOn(&run_raft_library_tests.step);
    unit_test_step.dependOn(&run_lib_jsonschema_tests.step);
    unit_test_step.dependOn(&run_lib_generating_tests.step);
    unit_test_step.dependOn(&run_lib_embeddings_tests.step);
    unit_test_step.dependOn(&run_lib_vectorindex_tests.step);
    unit_test_step.dependOn(&run_vector_cancellation_tests.step);
    unit_test_step.dependOn(&run_lib_chunking_tests.step);
    unit_test_step.dependOn(&run_lib_generating_runtime_tests.step);
    unit_test_step.dependOn(&run_lib_reranking_tests.step);
    unit_test_step.dependOn(&run_lib_reranking_runtime_tests.step);
    unit_test_step.dependOn(&run_lib_common_tests.step);
    unit_test_step.dependOn(&run_lib_common_config_tests.step);
    unit_test_step.dependOn(&run_lib_common_secrets_tests.step);
    unit_test_step.dependOn(&run_httpx_transport_regression_tests.step);
    unit_test_step.dependOn(&run_api_http_runtime_tests.step);
    unit_test_step.dependOn(&run_lib_casbin_tests.step);
    unit_test_step.dependOn(&run_lib_usermgr_tests.step);
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
    unit_test_step.dependOn(&run_lib_api_auth_tests.step);
    unit_test_step.dependOn(&run_algebraic_dynamic_template_tests.step);
    unit_test_step.dependOn(&run_api_artifact_reprocess_jobs_tests.step);
    unit_test_step.dependOn(&run_api_restore_jobs_tests.step);
    unit_test_step.dependOn(&run_portable_backup_tests.step);
    unit_test_step.dependOn(&run_public_api_parity_aggregate_tests.step);
    unit_test_step.dependOn(&run_lib_template_tests.step);
    unit_test_step.dependOn(&run_lib_toon_tests.step);
    unit_test_step.dependOn(&run_lib_mcp_tests.step);
    unit_test_step.dependOn(&run_lib_a2a_tests.step);
    unit_test_step.dependOn(&run_lib_image_tests.step);
    unit_test_step.dependOn(&run_lib_pdf_tests.step);
    unit_test_step.dependOn(&run_lib_scraping_tests.step);
    unit_test_step.dependOn(&run_lib_audio_tests.step);
    unit_test_step.dependOn(&run_hf_tokenizer_tests.step);
    unit_test_step.dependOn(delegated_inference_steps.inference_test);
    unit_test_step.dependOn(delegated_inference_steps.inference_finetune_test);
    unit_test_step.dependOn(lib_standalone_runtime_test_step);
    // The aggregate's storage HA shard owns the library tests. Keep only the
    // command-root coverage that the shard cannot discover; `ha-test` remains
    // available as the convenient focused target containing both artifacts.
    unit_test_step.dependOn(&run_ha_cli_tests.step);
    unit_test_step.dependOn(&run_raft_unit_tests.step);
    unit_test_step.dependOn(&run_raft_runtime_tests.step);
    unit_test_step.dependOn(&run_raft_restore_tests.step);
    // The standalone Raft library and Antfly-rooted Raft artifacts already
    // contain the ready-continuation and transport selections, respectively.
    // Preserve their focused targets without executing them twice in `unit-test`.

    // Progress mode uses one union-filtered root artifact too. Storage and
    // metadata module paths overlap, while raft-transport is a strict subset
    // of raft; separate artifacts therefore repeated tests as well as builds.
    const unit_progress_root_default_filters = lib_storage_default_filters ++ [_][]const u8{
        "metadata.",
        "raft.",
        "serverless",
    };
    const unit_progress_root_filters = selectTestFilters(b, &unit_progress_root_default_filters);
    const unit_progress_root_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = unit_progress_root_filters,
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_unit_progress_root_tests = b.addRunArtifact(unit_progress_root_tests);
    addRuntimeTestFilters(b, run_unit_progress_root_tests, unit_progress_root_filters);
    for (unit_progress_skip_filters) |filter| {
        run_unit_progress_root_tests.addArgs(&.{ "--skip-test-filter", filter });
    }
    addRuntimeSkipTestFilters(run_unit_progress_root_tests, &release_scale_test_filters);

    var unit_progress_tail: ?*std.Build.Step = null;
    unit_progress_tail = chainLabeledRunStep(b, run_unit_progress_root_tests, "lib-root-test", unit_progress_tail);
    unit_progress_tail = chainLabeledRun(b, ha_tests, "ha-test", unit_progress_tail);
    unit_progress_tail = chainLabeledRun(b, lib_template_tests, "lib-template-test", unit_progress_tail);
    unit_test_progress_step.dependOn(unit_progress_tail.?);

    const lmdb_unit_tests = b.addTest(.{
        .root_module = lmdb_engine_mod,
    });
    const run_lmdb_unit_tests = b.addRunArtifact(lmdb_unit_tests);

    const lmdb_test_step = b.step("lmdb-test", "Run Zig LMDB port unit tests");
    lmdb_test_step.dependOn(&run_lmdb_unit_tests.step);

    const storage_lmdb_test_mod = makeLmdbModule(b, "pkg/antfly/src/storage/lmdb.zig", target, optimize, build_options, lmdb_engine_mod, platform_mod);
    const storage_lmdb_unit_tests = b.addTest(.{
        .root_module = storage_lmdb_test_mod,
    });
    const run_storage_lmdb_unit_tests = b.addRunArtifact(storage_lmdb_unit_tests);

    const storage_lmdb_test_step = b.step("storage-lmdb-test", "Run storage/lmdb wrapper unit tests");
    storage_lmdb_test_step.dependOn(&run_storage_lmdb_unit_tests.step);

    const storage_lmdb_replay_tests = b.addTest(.{
        .root_module = storage_lmdb_test_mod,
        .filters = &.{"LMDB replay fixtures stay green"},
    });
    const run_storage_lmdb_replay_tests = addFilteredTestRunArtifact(b, storage_lmdb_replay_tests);
    const storage_lmdb_replay_step = b.step("lmdb-replay-fixtures", "Run only the LMDB replay fixture test");
    storage_lmdb_replay_step.dependOn(&run_storage_lmdb_replay_tests.step);

    const storage_sim_runtime_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/storage_sim_runtime_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    const storage_sim_runtime_tests = b.addTest(.{
        .root_module = storage_sim_runtime_test_mod,
    });
    const run_storage_sim_runtime_tests = b.addRunArtifact(storage_sim_runtime_tests);
    const storage_sim_runtime_test_step = b.step("storage-sim-runtime-test", "Run storage simulation runtime and modeled device tests");
    storage_sim_runtime_test_step.dependOn(&run_storage_sim_runtime_tests.step);

    const storage_lmdb_soak_build_options = makeLmdbBuildOptions(b, lmdb_backend, lmdb_evented_async_io, true);
    const storage_lmdb_soak_engine_mod = makeLmdbEngineModule(b, target, optimize, true, storage_lmdb_soak_build_options);
    const storage_lmdb_soak_test_mod = makeLmdbModule(b, "pkg/antfly/src/storage/lmdb.zig", target, optimize, storage_lmdb_soak_build_options, storage_lmdb_soak_engine_mod, platform_mod);
    const storage_lmdb_soak_tests = b.addTest(.{
        .root_module = storage_lmdb_soak_test_mod,
        .filters = &.{"LMDB sim soak stays green"},
    });
    const run_storage_lmdb_soak_tests = addFilteredTestRunArtifact(b, storage_lmdb_soak_tests);
    const storage_lmdb_soak_step = b.step("lmdb-sim-soak", "Run only the LMDB simulation soak test");
    storage_lmdb_soak_step.dependOn(&run_storage_lmdb_soak_tests.step);

    const docstore_test_mod = makeLmdbModule(b, "pkg/antfly/src/docstore_test_root.zig", target, optimize, build_options, lmdb_engine_mod, platform_mod);
    docstore_test_mod.addImport("bloom", bloom_mod);
    const docstore_unit_tests = b.addTest(.{
        .root_module = docstore_test_mod,
    });
    const run_docstore_unit_tests = b.addRunArtifact(docstore_unit_tests);

    const docstore_test_step = b.step("docstore-test", "Run storage/docstore unit tests");
    docstore_test_step.dependOn(&run_docstore_unit_tests.step);

    const shard_test_mod = makeLmdbModule(b, "pkg/antfly/src/shard_test_root.zig", target, optimize, build_options, lmdb_engine_mod, platform_mod);
    shard_test_mod.addImport("bloom", bloom_mod);
    const shard_unit_tests = b.addTest(.{
        .root_module = shard_test_mod,
    });
    const run_shard_unit_tests = b.addRunArtifact(shard_unit_tests);

    const shard_test_step = b.step("shard-test", "Run storage/shard unit tests");
    shard_test_step.dependOn(&run_shard_unit_tests.step);

    const wal_test_mod = makeLmdbModule(b, "pkg/antfly/src/wal_test_root.zig", target, optimize, build_options, lmdb_engine_mod, platform_mod);
    wal_test_mod.addImport("bloom", bloom_mod);
    wal_test_mod.addImport("structlog", structlog_mod);
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

    const wal_sim_tests = b.addTest(.{
        .root_module = wal_test_mod,
        .filters = &.{"wal sim"},
    });
    const run_wal_sim_tests = addFilteredTestRunArtifact(b, wal_sim_tests);
    const wal_sim_test_step = b.step("wal-sim-test", "Run only the WAL simulation workload tests");
    wal_sim_test_step.dependOn(&run_wal_sim_tests.step);

    const wal_vopr_tests = b.addTest(.{
        .root_module = wal_test_mod,
        .filters = &.{
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
    const wal_soak_test_mod = makeLmdbModule(b, "pkg/antfly/src/wal_test_root.zig", target, optimize, wal_soak_build_options, wal_soak_engine_mod, platform_mod);
    wal_soak_test_mod.addImport("bloom", bloom_mod);
    const wal_soak_tests = b.addTest(.{
        .root_module = wal_soak_test_mod,
        .filters = &.{"wal sim soak stays green"},
    });
    const run_wal_soak_tests = addFilteredTestRunArtifact(b, wal_soak_tests);
    const wal_soak_step = b.step("wal-sim-soak", "Run only the WAL simulation soak test");
    wal_soak_step.dependOn(&run_wal_soak_tests.step);

    const storage_sim_soak_step = b.step("storage-sim-soak", "Run the LMDB and WAL simulation soak tests");
    storage_sim_soak_step.dependOn(&run_storage_lmdb_soak_tests.step);
    storage_sim_soak_step.dependOn(&run_wal_soak_tests.step);
    soak_test_step.dependOn(storage_sim_soak_step);

    const persistent_test_mod = makeLmdbModule(b, "pkg/antfly/src/persistent_test_root.zig", target, optimize, build_options, lmdb_engine_mod, platform_mod);
    persistent_test_mod.addImport("bloom", bloom_mod);
    persistent_test_mod.addImport("antfly_vellum", vellum_mod);
    persistent_test_mod.addImport("antfly_regex", regex_mod);
    persistent_test_mod.addImport("antfly_vector", vector_mod);
    persistent_test_mod.addImport("antfly_vectorindex", vectorindex_mod);
    persistent_test_mod.addImport("antfly_reranking", reranking_mod);
    persistent_test_mod.addImport("structlog", structlog_mod);
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

    const persistent_sim_tests = b.addTest(.{
        .root_module = persistent_test_mod,
        .filters = &.{"persistent sim workloads stay green"},
    });
    const run_persistent_sim_tests = addFilteredTestRunArtifact(b, persistent_sim_tests);
    const persistent_sim_step = b.step("persistent-sim-test", "Run only the persistent simulation workload tests");
    persistent_sim_step.dependOn(&run_persistent_sim_tests.step);

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
        },
    });
    const run_persistent_vopr_tests = addFilteredTestRunArtifact(b, persistent_vopr_tests);
    const persistent_vopr_step = b.step("persistent-vopr-test", "Run persistent modeled-storage VOPR smoke tests");
    persistent_vopr_step.dependOn(&run_persistent_vopr_tests.step);

    const persistent_soak_build_options = makeLmdbBuildOptions(b, lmdb_backend, lmdb_evented_async_io, true);
    const persistent_soak_engine_mod = makeLmdbEngineModule(b, target, optimize, true, persistent_soak_build_options);
    const persistent_soak_test_mod = makeLmdbModule(b, "pkg/antfly/src/persistent_test_root.zig", target, optimize, persistent_soak_build_options, persistent_soak_engine_mod, platform_mod);
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
    const persistent_soak_step = b.step("persistent-sim-soak", "Run only the persistent simulation soak test");
    persistent_soak_step.dependOn(&run_persistent_soak_tests.step);

    storage_sim_soak_step.dependOn(&run_persistent_soak_tests.step);

    const index_manager_test_mod = makeLmdbModule(b, "pkg/antfly/src/index_manager_test_root.zig", target, optimize, build_options, lmdb_engine_mod, platform_mod);
    addSnowballModule(b, index_manager_test_mod);
    index_manager_test_mod.addImport("bloom", bloom_mod);
    index_manager_test_mod.addImport("antfly_vellum", vellum_mod);
    index_manager_test_mod.addImport("antfly_vector", vector_mod);
    index_manager_test_mod.addImport("antfly_vectorindex", vectorindex_mod);
    index_manager_test_mod.addImport("antfly_matcher", matcher_mod);
    index_manager_test_mod.addImport("antfly_resolver", resolver_mod);
    index_manager_test_mod.addImport("antfly_chunking", chunking_mod);
    index_manager_test_mod.addImport("antfly_regex", regex_mod);
    index_manager_test_mod.addImport("antfly_reader_config", reader_config_mod);
    index_manager_test_mod.addImport("structlog", structlog_mod);
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

    const index_manager_sim_tests = b.addTest(.{
        .root_module = index_manager_test_mod,
        .filters = &.{"index manager sim workloads stay green"},
    });
    const run_index_manager_sim_tests = addFilteredTestRunArtifact(b, index_manager_sim_tests);
    const index_manager_sim_step = b.step("index-manager-sim-test", "Run only the index manager simulation workload tests");
    index_manager_sim_step.dependOn(&run_index_manager_sim_tests.step);

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
        },
    });
    const run_index_manager_vopr_tests = addFilteredTestRunArtifact(b, index_manager_vopr_tests);
    const index_manager_vopr_step = b.step("index-manager-vopr-test", "Run index manager modeled-storage VOPR smoke tests");
    index_manager_vopr_step.dependOn(&run_index_manager_vopr_tests.step);

    const db_test_mod = makeLmdbModule(b, "pkg/antfly/src/db_test_root.zig", target, optimize, build_options, lmdb_engine_mod, platform_mod);
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

    const db_split_sim_default_filters = [_][]const u8{
        "db split sim default workload stays green",
        "db split sim reopen-heavy workload stays green",
    };
    const db_split_sim_tests = b.addTest(.{
        .root_module = db_test_mod,
        .filters = selectTestFilters(b, &db_split_sim_default_filters),
    });
    const run_db_split_sim_tests = addFilteredTestRunArtifact(b, db_split_sim_tests);
    const db_split_sim_step = b.step("db-split-sim-test", "Run only the DB split simulation workload tests");
    db_split_sim_step.dependOn(&run_db_split_sim_tests.step);

    const db_split_vopr_tests = b.addTest(.{
        .root_module = db_test_mod,
        .filters = &.{
            "db split modeled replay fixtures stay green",
            "db split modeled sim workloads stay green",
        },
    });
    const run_db_split_vopr_tests = addFilteredTestRunArtifact(b, db_split_vopr_tests);
    const db_split_vopr_step = b.step("db-split-vopr-test", "Run only the DB split modeled-storage replay fixture tests");
    db_split_vopr_step.dependOn(&run_db_split_vopr_tests.step);

    const storage_workload_sim_step = b.step("storage-sim-test", "Run legacy deterministic storage workload simulations that still use real storage I/O");
    storage_workload_sim_step.dependOn(&run_wal_sim_tests.step);
    storage_workload_sim_step.dependOn(&run_persistent_sim_tests.step);
    storage_workload_sim_step.dependOn(&run_index_manager_sim_tests.step);

    const storage_vopr_step = b.step("storage-vopr-test", "Run storage modeled-time/model-I/O VOPR smoke and simulation checks");
    storage_vopr_step.dependOn(&run_storage_sim_runtime_tests.step);
    storage_vopr_step.dependOn(&run_lib_lsm_backend_sim_tests.step);
    storage_vopr_step.dependOn(&run_wal_vopr_tests.step);
    storage_vopr_step.dependOn(&run_persistent_vopr_tests.step);
    storage_vopr_step.dependOn(&run_index_manager_vopr_tests.step);
    storage_vopr_step.dependOn(&run_db_split_vopr_tests.step);
    sim_test_step.dependOn(storage_vopr_step);

    const db_unit_tests = b.addTest(.{
        .root_module = db_test_mod,
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_db_unit_tests = b.addRunArtifact(db_unit_tests);
    if (b.args) |args| run_db_unit_tests.addArgs(args);
    addRuntimeSkipTestFilters(run_db_unit_tests, &release_scale_test_filters);
    const db_test_step = b.step("db-test", "Run storage/db unit tests");
    db_test_step.dependOn(&run_db_unit_tests.step);

    // Keep the small, deterministic release-blocker primitives in the PR/base
    // unit gate. The corpus-scale fixtures below protect thresholds that only
    // appear at thousands of documents and run in the zig-full gate instead.
    const release_blocker_regression_filters = [_][]const u8{
        "non-visible doc set complements visibility per generation",
        "built-in exact dense scorer filters metadata before vector reads",
        "one percent filtered route preserves exact recall with candidate-linear IO",
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
    const db_restore_identity_step = b.step("db-restore-identity-test", "Run the focused run-backed identity restore regression");
    db_restore_identity_step.dependOn(&run_db_restore_identity_tests.step);

    // These focused regressions protect production paths introduced by this
    // branch. Keep them in the PR/base gate instead of defining orphan steps
    // that run only when invoked manually.
    unit_test_step.dependOn(&run_lib_api_derived_coverage_tests.step);
    unit_test_step.dependOn(&run_api_table_writes_production_regression_unit_tests.step);

    const db_enrichment_tests = b.addTest(.{
        .root_module = db_test_mod,
        .filters = &.{"enrichment"},
    });
    const run_db_enrichment_tests = addFilteredTestRunArtifact(b, db_enrichment_tests);
    const db_enrichment_test_step = b.step("db-enrichment-test", "Run storage/db enrichment-related unit tests");
    db_enrichment_test_step.dependOn(&run_db_enrichment_tests.step);

    const db_enrichment_single_tests = b.addTest(.{
        .root_module = db_test_mod,
        .filters = &.{"db dense index can reference existing whole-doc embedding enrichment"},
    });
    const run_db_enrichment_single_tests = addFilteredTestRunArtifact(b, db_enrichment_single_tests);
    const db_enrichment_single_step = b.step("db-enrichment-single-test", "Run the focused whole-doc enrichment DB test");
    db_enrichment_single_step.dependOn(&run_db_enrichment_single_tests.step);

    const db_restore_managed_tests = b.addTest(.{
        .root_module = db_test_mod,
        .filters = &.{"db restore snapshot replays managed chunked dense embeddings"},
    });
    const run_db_restore_managed_tests = addFilteredTestRunArtifact(b, db_restore_managed_tests);
    const db_restore_managed_step = b.step("db-restore-managed-test", "Run the focused managed chunked dense restore DB test");
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
    const db_embeddings_update_step = b.step("db-embeddings-update-test", "Run the explicit _embeddings update DB test");
    db_embeddings_update_step.dependOn(&run_db_embeddings_update_tests.step);

    const db_merge_cutover_tests = b.addTest(.{
        .root_module = db_test_mod,
        .filters = &.{"db merge-style cutover preserves enrichment resume and fencing across reopen"},
    });
    const run_db_merge_cutover_tests = addFilteredTestRunArtifact(b, db_merge_cutover_tests);
    const db_merge_cutover_step = b.step("db-merge-cutover-test", "Run the merge cutover enrichment reopen DB test");
    db_merge_cutover_step.dependOn(&run_db_merge_cutover_tests.step);

    const db_shared_embedding_tests = b.addTest(.{
        .root_module = db_test_mod,
        .filters = &.{"db shared embedding enrichment feeds multiple dense indexes"},
    });
    const run_db_shared_embedding_tests = addFilteredTestRunArtifact(b, db_shared_embedding_tests);
    const db_shared_embedding_step = b.step("db-shared-embedding-test", "Run the shared embedding enrichment DB test");
    db_shared_embedding_step.dependOn(&run_db_shared_embedding_tests.step);

    const db_dense_parent_paging_tests = b.addTest(.{
        .root_module = db_test_mod,
        .filters = &.{"db dense parent paging fetches enough chunk hits before grouping"},
    });
    const run_db_dense_parent_paging_tests = addFilteredTestRunArtifact(b, db_dense_parent_paging_tests);
    const db_dense_parent_paging_step = b.step("db-dense-parent-paging-test", "Run the dense parent paging enrichment DB test");
    db_dense_parent_paging_step.dependOn(&run_db_dense_parent_paging_tests.step);

    const db_split_replay_tests = b.addTest(.{
        .root_module = db_test_mod,
        .filters = &.{"db split replay fixtures stay green"},
    });
    const run_db_split_replay_tests = addFilteredTestRunArtifact(b, db_split_replay_tests);
    const db_split_replay_step = b.step("db-split-replay-fixtures", "Run only the DB split replay fixture tests");
    db_split_replay_step.dependOn(&run_db_split_replay_tests.step);

    const sparse_test_mod = makeLmdbModule(b, "pkg/antfly/src/sparse_test_root.zig", target, optimize, build_options, lmdb_engine_mod, platform_mod);
    sparse_test_mod.addImport("bloom", bloom_mod);
    const sparse_unit_tests = b.addTest(.{
        .root_module = sparse_test_mod,
    });
    // This aggregate exercises long-running storage lifecycle tests. Use the
    // simple runner so failures retain per-test/cleanup attribution instead of
    // collapsing into an opaque test-server process exit.
    const run_sparse_unit_tests = addFilteredTestRunArtifact(b, sparse_unit_tests);

    const sparse_test_step = b.step("sparse-test", "Run sparse index unit tests");
    sparse_test_step.dependOn(&run_sparse_unit_tests.step);

    const derived_log_test_mod = makeLmdbModule(b, "pkg/antfly/src/derived_log_test_root.zig", target, optimize, build_options, lmdb_engine_mod, platform_mod);
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
            "storage.db.document_mapper.",
            "storage.db.document_query.",
            "storage.db.generation_lifecycle.",
            "storage.db.lease.",
            "storage.db.mod.",
            "storage.db.ownership.",
            "storage.db.planning_stats.",
            "storage.db.promotion_runtime.",
            "storage.db.query_metrics.",
            "storage.db.range_state.",
            "storage.db.resolution_handoff.",
            "storage.db.resolution_runtime.",
            "storage.db.root_identity.",
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
        },
        &.{
            "storage.backend_adapter.",
            "storage.backend_conformance_test.",
            "storage.backend_erased.",
            "storage.backend_types.",
            "storage.background_runtime.",
            "storage.backup_codec.",
            "storage.coverage_identity.",
            "storage.derived_log_test_root.",
            "storage.docstore.",
            "storage.enrichment.",
            "storage.filesystem_capacity.",
            "storage.hbc_adapter.",
            "storage.hierarchy_navigation.",
            "storage.internal_keys.",
            "storage.lmdb.",
            "storage.lmdb_backend.",
            "storage.maintenance.",
            "storage.mem_backend.",
            "storage.mem_ordered.",
            "storage.object_storage.",
            "storage.persistent.",
            "storage.portable_backup.",
            "storage.resource_manager.",
            "storage.schema.",
            "storage.shard.",
            "storage.sim_runtime.",
            "storage.transactions.",
            "storage.ttl.",
            "storage.wal.",
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
        "Run the storage portion of the default unit-test target in bounded codegen shards",
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
        .root_module = lib_test_mod,
        .filters = unit_storage_support_compile_filters,
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
        .max_rss = 8 * 1024 * 1024 * 1024,
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
        .root_module = lib_test_mod,
        .filters = unit_storage_engine_compile_filters,
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
        .max_rss = 8 * 1024 * 1024 * 1024,
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
        .root_module = lib_test_mod,
        .filters = unit_storage_shard_filters[unit_storage_db_core_shard_index],
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
        .max_rss = 8 * 1024 * 1024 * 1024,
    });
    unit_storage_db_core_tests.step.dependOn(&unit_storage_shard_audit.step);
    const unit_storage_compile_step = b.step(
        "unit-storage-compile",
        "Compile the three storage unit-test artifacts without running them",
    );
    unit_storage_compile_step.dependOn(&unit_storage_support_tests.step);
    unit_storage_compile_step.dependOn(&unit_storage_engine_tests.step);
    unit_storage_compile_step.dependOn(&unit_storage_db_core_tests.step);

    // Keep an explicit, opt-in equivalence check for future changes to these
    // groupings. It compiles the former seven-artifact layout and compares the
    // union of declared named tests with the default three-artifact layout.
    // Neither the baseline artifacts nor this comparison are dependencies of
    // unit-test, unit-storage-test, or recall-test.
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
            .root_module = lib_test_mod,
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

    // The complete metadata namespace pulls in the simulation harness and a
    // large amount of control-plane code even though the default unit target
    // excludes simulations at runtime. Compile the production metadata tests
    // in module-owned shards so no individual Linux test image has to load the
    // entire namespace. An explicit, flat production test root gives every
    // shard a stable compile-time ownership prefix without traversing the
    // public metadata namespace or its simulation imports. The sets are
    // disjoint, and the default runtime selection uses the same ownership
    // prefixes so an accidental empty shard is a hard failure.
    const unit_metadata_shard_filters = [_][]const []const u8{
        &.{"metadata.reconciler."},
        &.{
            "metadata.service.",
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
    const unit_metadata_sharded_test_step = b.step(
        "unit-metadata-test",
        "Run the production metadata portion of the default unit-test target in bounded shards",
    );
    const unit_metadata_shard_names = [_][]const u8{
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
    const unit_metadata_shard_max_rss = [_]usize{
        5 * 1024 * 1024 * 1024,
        5 * 1024 * 1024 * 1024,
        // The core shard now compiles the contextual public router and its
        // compatibility manifest; Debug codegen peaks above 5 GiB on macOS.
        6 * 1024 * 1024 * 1024,
        5 * 1024 * 1024 * 1024,
        // The server shard compiles the public API kernel and listener stack;
        // Debug codegen currently peaks around 6.4 GB on aarch64 macOS.
        7 * 1024 * 1024 * 1024,
        5 * 1024 * 1024 * 1024,
        5 * 1024 * 1024 * 1024,
        7 * 1024 * 1024 * 1024,
        6 * 1024 * 1024 * 1024,
    };
    const metadata_runtime_filter_is_default =
        lib_metadata_runtime_filters.len == 1 and
        std.mem.eql(u8, lib_metadata_runtime_filters[0], "metadata.");
    const unit_metadata_shard_lanes = [_]usize{ 0, 1, 0, 1, 0, 1, 0, 1, 0 };
    var unit_metadata_compile_tails = [_]?*std.Build.Step{ null, null };
    var unit_metadata_aggregate_run_tails = [_]?*std.Build.Step{ null, null };
    var unit_metadata_focused_run_tails = [_]?*std.Build.Step{ null, null };
    for (
        unit_metadata_shard_filters,
        unit_metadata_shard_names,
        metadata_unit_test_mods,
        unit_metadata_shard_max_rss,
        unit_metadata_shard_lanes,
    ) |shard_filters, shard_name, test_mod, shard_max_rss, lane| {
        const is_runtime_exclusive = std.mem.eql(u8, shard_name, "metadata-replication-backfill-tests");
        const unit_metadata_tests = b.addTest(.{
            .name = shard_name,
            .root_module = test_mod,
            .filters = shard_filters,
            .test_runner = .{
                .path = b.path("pkg/antfly/src/test_runner.zig"),
                .mode = .simple,
            },
            .max_rss = shard_max_rss,
        });
        if (unit_metadata_compile_tails[lane]) |previous| unit_metadata_tests.step.dependOn(previous);
        unit_metadata_compile_tails[lane] = &unit_metadata_tests.step;
        const run_unit_metadata_tests = b.addRunArtifact(unit_metadata_tests);
        const runtime_filters = if (metadata_runtime_filter_is_default)
            shard_filters
        else
            lib_metadata_runtime_filters;
        addRuntimeTestFilters(b, run_unit_metadata_tests, runtime_filters);
        // A focused caller filter generally belongs to only one aggregate
        // artifact. Permit the other metadata shards to be empty, while
        // keeping the default CI selection strict so a stale shard filter
        // cannot silently remove coverage.
        if (!metadata_runtime_filter_is_default) {
            run_unit_metadata_tests.addArg("--allow-empty-test-filter");
        }
        addRuntimeSkipTestFilters(run_unit_metadata_tests, lib_unit_filters);
        for (root_test_skip_filters) |filter| {
            run_unit_metadata_tests.addArgs(&.{ "--skip-test-filter", filter });
        }
        if (is_runtime_exclusive) {
            for (unit_metadata_aggregate_run_tails) |previous| {
                if (previous) |previous_step| run_unit_metadata_tests.step.dependOn(previous_step);
            }
        } else if (unit_metadata_aggregate_run_tails[lane]) |previous| {
            run_unit_metadata_tests.step.dependOn(previous);
        }
        unit_test_step.dependOn(&run_unit_metadata_tests.step);
        if (is_runtime_exclusive) {
            unit_metadata_aggregate_run_tails = .{ &run_unit_metadata_tests.step, &run_unit_metadata_tests.step };
        } else {
            unit_metadata_aggregate_run_tails[lane] = &run_unit_metadata_tests.step;
        }

        // The standalone metadata step owns its selected metadata tests. Use a
        // separate run policy so a caller filter is not mistaken for the root
        // aggregate's overlap exclusion and skipped everywhere.
        const run_focused_metadata_tests = b.addRunArtifact(unit_metadata_tests);
        addRuntimeTestFilters(b, run_focused_metadata_tests, runtime_filters);
        if (!metadata_runtime_filter_is_default) {
            run_focused_metadata_tests.addArg("--allow-empty-test-filter");
        }
        for (root_test_skip_filters) |filter| {
            run_focused_metadata_tests.addArgs(&.{ "--skip-test-filter", filter });
        }
        if (is_runtime_exclusive) {
            for (unit_metadata_focused_run_tails) |previous| {
                if (previous) |previous_step| run_focused_metadata_tests.step.dependOn(previous_step);
            }
        } else if (unit_metadata_focused_run_tails[lane]) |previous| {
            run_focused_metadata_tests.step.dependOn(previous);
        }
        unit_metadata_sharded_test_step.dependOn(&run_focused_metadata_tests.step);
        lib_metadata_test_step.dependOn(&run_focused_metadata_tests.step);
        if (is_runtime_exclusive) {
            unit_metadata_focused_run_tails = .{ &run_focused_metadata_tests.step, &run_focused_metadata_tests.step };
        } else {
            unit_metadata_focused_run_tails[lane] = &run_focused_metadata_tests.step;
        }
    }

    // Default Antfly unit coverage is hermetic: no network fetchers, no
    // benchmarks, and no soak/conformance suites that require external corpora.
    // Runtime exclusions above give explicit API filters first ownership,
    // storage second ownership, and module-sharded metadata third ownership.
    dependOnAll(unit_test_step, &.{
        &run_lib_json_tests.step,
        &run_lib_onnx_tests.step,
        &run_httpx_json_tests.step,
        &run_httpx_tests.step,
        &run_common_http_tests.step,
        &run_api_json_helpers_tests.step,
        &run_antfly_client_pkg_tests.step,
        &run_lib_unit_tests.step,
        &run_sparse_unit_tests.step,
    });

    const lmdb_bench_engine_options_c = makeLmdbBuildOptions(b, .c, false, false);
    const lmdb_bench_build_options_c = makeRootBuildOptions(b, .c, false, false, false, true, false, lite_local_inference_runtime, true, antfly_version);
    const lmdb_bench_engine_mod_c = makeLmdbEngineModule(b, target, .ReleaseFast, true, lmdb_bench_engine_options_c);
    const lmdb_bench_wrapper_mod_c = makeLmdbModule(b, "pkg/antfly/src/storage/lmdb.zig", target, .ReleaseFast, lmdb_bench_build_options_c, lmdb_bench_engine_mod_c, platform_mod);
    const lmdb_bench_mod_c = b.createModule(.{
        .root_source_file = b.path("bench/storage/lmdb_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    lmdb_bench_mod_c.addImport("lmdb", lmdb_bench_wrapper_mod_c);
    lmdb_bench_mod_c.addImport("lmdb_engine", lmdb_bench_engine_mod_c);

    const lmdb_bench_c = b.addExecutable(.{
        .name = "lmdb_bench_c",
        .root_module = lmdb_bench_mod_c,
    });

    const lmdb_bench_engine_options_zig = makeLmdbBuildOptions(b, .zig, lmdb_evented_async_io, false);
    const lmdb_bench_build_options_zig = makeRootBuildOptions(b, .zig, lmdb_evented_async_io, false, false, true, false, lite_local_inference_runtime, true, antfly_version);
    const lmdb_bench_engine_mod_zig = makeLmdbEngineModule(b, target, .ReleaseFast, true, lmdb_bench_engine_options_zig);
    const lmdb_bench_wrapper_mod_zig = makeLmdbModule(b, "pkg/antfly/src/storage/lmdb.zig", target, .ReleaseFast, lmdb_bench_build_options_zig, lmdb_bench_engine_mod_zig, platform_mod);
    const lmdb_bench_mod_zig = b.createModule(.{
        .root_source_file = b.path("bench/storage/lmdb_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    lmdb_bench_mod_zig.addImport("lmdb", lmdb_bench_wrapper_mod_zig);
    lmdb_bench_mod_zig.addImport("lmdb_engine", lmdb_bench_engine_mod_zig);

    const lmdb_bench_zig = b.addExecutable(.{
        .name = "lmdb_bench_zig",
        .root_module = lmdb_bench_mod_zig,
    });

    const run_lmdb_bench_c = b.addRunArtifact(lmdb_bench_c);
    run_lmdb_bench_c.addArgs(&.{ "--cycles", "8", "--keys", "512", "--dups", "32", "--named-keys", "128" });

    const run_lmdb_bench_zig = b.addRunArtifact(lmdb_bench_zig);
    run_lmdb_bench_zig.addArgs(&.{ "--cycles", "8", "--keys", "512", "--dups", "32", "--named-keys", "128" });

    const lmdb_bench_step = b.step("lmdb-bench", "Compare LMDB wrapper benchmarks on c and zig backends");
    lmdb_bench_step.dependOn(&run_lmdb_bench_c.step);
    lmdb_bench_step.dependOn(&run_lmdb_bench_zig.step);

    const run_lmdb_bench_worker_zig = b.addRunArtifact(lmdb_bench_zig);
    run_lmdb_bench_worker_zig.addArgs(&.{ "--cycles", "8", "--keys", "512", "--dups", "32", "--named-keys", "128", "--worker-thread" });
    const lmdb_bench_worker_step = b.step("lmdb-bench-worker", "Run LMDB wrapper benchmarks on zig worker-thread commit backend");
    lmdb_bench_worker_step.dependOn(&run_lmdb_bench_worker_zig.step);

    const run_lmdb_bench_async_zig = b.addRunArtifact(lmdb_bench_zig);
    run_lmdb_bench_async_zig.addArgs(&.{ "--cycles", "8", "--keys", "512", "--dups", "32", "--named-keys", "128", "--async-io" });
    const lmdb_bench_async_step = b.step("lmdb-bench-async", "Run LMDB wrapper benchmarks on zig async-io commit backend");
    lmdb_bench_async_step.dependOn(&run_lmdb_bench_async_zig.step);

    const run_lmdb_bench_adaptive_zig = b.addRunArtifact(lmdb_bench_zig);
    run_lmdb_bench_adaptive_zig.addArgs(&.{ "--cycles", "8", "--keys", "512", "--dups", "32", "--named-keys", "128", "--adaptive" });
    const lmdb_bench_adaptive_step = b.step("lmdb-bench-adaptive", "Run LMDB wrapper benchmarks on zig adaptive commit backend");
    lmdb_bench_adaptive_step.dependOn(&run_lmdb_bench_adaptive_zig.step);

    const run_lmdb_bench_repeat_c = b.addRunArtifact(lmdb_bench_c);
    run_lmdb_bench_repeat_c.addArgs(&.{ "--samples", "5", "--cycles", "8", "--keys", "512", "--dups", "32", "--named-keys", "128" });

    const run_lmdb_bench_repeat_zig = b.addRunArtifact(lmdb_bench_zig);
    run_lmdb_bench_repeat_zig.addArgs(&.{ "--samples", "5", "--cycles", "8", "--keys", "512", "--dups", "32", "--named-keys", "128" });

    const lmdb_bench_repeat_step = b.step("lmdb-bench-repeat", "Repeat LMDB wrapper benchmarks on c and zig backends");
    lmdb_bench_repeat_step.dependOn(&run_lmdb_bench_repeat_c.step);
    lmdb_bench_repeat_step.dependOn(&run_lmdb_bench_repeat_zig.step);

    const run_lmdb_bench_zig_mmap = b.addRunArtifact(lmdb_bench_zig);
    run_lmdb_bench_zig_mmap.addArgs(&.{ "--cycles", "8", "--keys", "512", "--dups", "32", "--named-keys", "128", "--write-map", "--map-async" });

    const lmdb_bench_mmap_step = b.step("lmdb-bench-mmap", "Run LMDB wrapper benchmarks on zig mmap modes");
    lmdb_bench_mmap_step.dependOn(&run_lmdb_bench_zig_mmap.step);

    const split_bench_engine_options = makeLmdbBuildOptions(b, lmdb_backend, lmdb_evented_async_io, false);
    const split_bench_build_options = makeRootBuildOptions(b, lmdb_backend, lmdb_evented_async_io, false, false, true, false, lite_local_inference_runtime, true, antfly_version);
    const split_bench_engine_mod = makeLmdbEngineModule(b, target, .ReleaseFast, true, split_bench_engine_options);
    const split_bench_root_mod = makeLmdbModule(b, antfly_benches_build.split_bench_root, target, .ReleaseFast, split_bench_build_options, split_bench_engine_mod, platform_mod);
    const split_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/storage/split_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    split_bench_mod.addImport("split_storage", split_bench_root_mod);

    const split_bench = b.addExecutable(.{
        .name = "split_bench",
        .root_module = split_bench_mod,
    });

    const run_split_bench = b.addRunArtifact(split_bench);
    const split_bench_step = b.step("split-bench", "Benchmark median-key selection and split range copy");
    split_bench_step.dependOn(&run_split_bench.step);

    const run_split_bench_repeat = b.addRunArtifact(split_bench);
    run_split_bench_repeat.addArgs(&.{ "--samples", "5" });
    const split_bench_repeat_step = b.step("split-bench-repeat", "Benchmark median-key selection and split range copy with repeated samples");
    split_bench_repeat_step.dependOn(&run_split_bench_repeat.step);

    const db_split_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/storage/db_split_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    db_split_bench_mod.addImport("antfly-zig", lib_mod);

    const db_split_bench = b.addExecutable(.{
        .name = "db_split_bench",
        .root_module = db_split_bench_mod,
    });

    const run_db_split_bench = b.addRunArtifact(db_split_bench);
    const db_split_bench_step = b.step("db-split-bench", "Benchmark DB split preparation old vs current");
    db_split_bench_step.dependOn(&run_db_split_bench.step);

    const run_db_split_bench_repeat = b.addRunArtifact(db_split_bench);
    run_db_split_bench_repeat.addArgs(&.{ "--samples", "5" });
    const db_split_bench_repeat_step = b.step("db-split-bench-repeat", "Benchmark DB split preparation old vs current with repeated samples");
    db_split_bench_repeat_step.dependOn(&run_db_split_bench_repeat.step);

    const docid_doc_set_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/storage/docid_doc_set_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    const docid_doc_set_bench_root_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/docid_doc_set_bench_root.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    docid_doc_set_bench_mod.addImport("docid_doc_set_bench_root", docid_doc_set_bench_root_mod);

    const docid_doc_set_bench = b.addExecutable(.{
        .name = "docid_doc_set_bench",
        .root_module = docid_doc_set_bench_mod,
    });

    const run_docid_doc_set_bench = b.addRunArtifact(docid_doc_set_bench);
    if (b.args) |args| {
        run_docid_doc_set_bench.addArgs(args);
    } else {
        run_docid_doc_set_bench.addArgs(&.{ "--samples", "1", "--repeats", "16", "--small", "32", "--medium", "1024", "--large", "16384" });
    }
    const docid_doc_set_bench_step = b.step("docid-doc-set-bench", "Benchmark DOCID doc-set representations against sparse id baselines");
    docid_doc_set_bench_step.dependOn(&run_docid_doc_set_bench.step);

    const backend_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/storage/backend_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    backend_bench_mod.addImport("antfly_zig", lib_mod);
    const backend_bench = b.addExecutable(.{
        .name = "backend_bench",
        .root_module = backend_bench_mod,
    });

    const run_backend_bench = b.addRunArtifact(backend_bench);
    if (b.args) |args| {
        run_backend_bench.addArgs(args);
    } else {
        run_backend_bench.addArgs(&.{ "--samples", "3", "--keys", "20000", "--value-size", "128", "--hit-repeats", "3", "--miss-repeats", "3", "--scan-repeats", "5" });
    }
    const backend_bench_step = b.step("backend-bench", "Benchmark shared backend workloads across LMDB and LSM backends");
    backend_bench_step.dependOn(&run_backend_bench.step);

    const graph_pattern_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/graph/pattern_query_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    graph_pattern_bench_mod.addImport("antfly_zig", lib_mod);
    const graph_pattern_bench = b.addExecutable(.{
        .name = "graph_pattern_query_bench",
        .root_module = graph_pattern_bench_mod,
    });
    const graph_pattern_bench_build_step = b.step(
        "graph-pattern-bench-build",
        "Build the local graph-pattern latency and demand-working-set benchmark",
    );
    graph_pattern_bench_build_step.dependOn(&graph_pattern_bench.step);

    const run_graph_pattern_bench = b.addRunArtifact(graph_pattern_bench);
    if (b.args) |args| {
        run_graph_pattern_bench.addArgs(args);
    } else {
        run_graph_pattern_bench.addArgs(&.{
            "--mode",          "exact",
            "--fanout",        "10000",
            "--tags-per-post", "8",
            "--target-degree", "100000",
            "--match-every",   "10",
            "--warmup",        "5",
            "--samples",       "30",
        });
    }
    const graph_pattern_bench_step = b.step(
        "graph-pattern-bench",
        "Benchmark exact or generic graph-pattern latency, allocations, and process peak RSS",
    );
    graph_pattern_bench_step.dependOn(&run_graph_pattern_bench.step);

    const lsm_backend_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/storage/lsm_backend_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    lsm_backend_bench_mod.addImport("antfly_zig", lib_mod);
    const lsm_backend_bench = b.addExecutable(.{
        .name = "lsm_backend_bench",
        .root_module = lsm_backend_bench_mod,
    });

    const run_lsm_backend_bench = b.addRunArtifact(lsm_backend_bench);
    if (b.args) |args| {
        run_lsm_backend_bench.addArgs(args);
    } else {
        run_lsm_backend_bench.addArgs(&.{
            "--samples",            "3",
            "--keys",               "20000",
            "--value-size",         "128",
            "--hit-repeats",        "5",
            "--miss-repeats",       "5",
            "--short-scan-len",     "64",
            "--short-scan-repeats", "16",
            "--full-scan-repeats",  "5",
            "--reopen-repeats",     "5",
            "--mixed-repeats",      "3",
            "--storage",            "host",
            "--cache",              "both",
        });
    }
    const lsm_backend_bench_step = b.step("lsm-backend-bench", "Benchmark LSM read and scan paths with optional cache and storage instrumentation");
    lsm_backend_bench_step.dependOn(&run_lsm_backend_bench.step);

    const hbc_storage_read_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/vectors/hbc_storage_read_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    hbc_storage_read_bench_mod.addImport("antfly-zig", lib_mod);
    const hbc_storage_read_bench = b.addExecutable(.{
        .name = "hbc_storage_read_bench",
        .root_module = hbc_storage_read_bench_mod,
    });

    const run_hbc_storage_read_bench = b.addRunArtifact(hbc_storage_read_bench);
    if (b.args) |args| {
        run_hbc_storage_read_bench.addArgs(args);
    } else {
        run_hbc_storage_read_bench.addArgs(&.{
            "--docs",       "75000",
            "--dims",       "512",
            "--queries",    "1000",
            "--candidates", "800",
        });
    }
    const hbc_storage_read_bench_build_step = b.step("hbc-storage-read-bench-build", "Build the HBC-shaped LSM hot-read benchmark");
    hbc_storage_read_bench_build_step.dependOn(&hbc_storage_read_bench.step);
    const hbc_storage_read_bench_step = b.step("hbc-storage-read-bench", "Benchmark HBC-shaped metadata/vector artifact reads through the LSM");
    hbc_storage_read_bench_step.dependOn(&run_hbc_storage_read_bench.step);

    const lsm_write_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/storage/lsm_write_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    const lsm_write_bench_root_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/lsm_write_bench_root.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    lsm_write_bench_root_mod.addImport("bloom", bloom_mod);
    lsm_write_bench_root_mod.addImport("antfly_platform", platform_mod);
    lsm_write_bench_mod.addImport("antfly_zig", lsm_write_bench_root_mod);
    const lsm_write_bench = b.addExecutable(.{
        .name = "lsm_write_bench",
        .root_module = lsm_write_bench_mod,
    });

    const run_lsm_write_bench = b.addRunArtifact(lsm_write_bench);
    if (b.args) |args| {
        run_lsm_write_bench.addArgs(args);
    } else {
        run_lsm_write_bench.addArgs(&.{
            "--samples",          "3",
            "--keys",             "20000",
            "--hot-keys",         "1000",
            "--overwrite-rounds", "20",
            "--value-size",       "128",
            "--batch-size",       "1000",
            "--storage",          "host",
            "--mode",             "both",
        });
    }
    const lsm_write_bench_step = b.step("lsm-write-bench", "Benchmark LSM write amplification across sorted, random, overwrite, and delete workloads");
    lsm_write_bench_step.dependOn(&run_lsm_write_bench.step);

    const lsm_write_bench_compare_mod = b.createModule(.{
        .root_source_file = b.path("bench/storage/lsm_write_bench_compare.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    const lsm_write_bench_compare = b.addExecutable(.{
        .name = "lsm_write_bench_compare",
        .root_module = lsm_write_bench_compare_mod,
    });

    const run_lsm_write_bench_compare = b.addRunArtifact(lsm_write_bench_compare);
    if (b.args) |args| {
        run_lsm_write_bench_compare.addArgs(args);
    } else {
        run_lsm_write_bench_compare.addArgs(&.{
            "--before",
            "/tmp/lsm-write-before.jsonl",
            "--after",
            "/tmp/lsm-write-after.jsonl",
        });
    }
    const lsm_write_bench_compare_step = b.step("lsm-write-bench-compare", "Compare two LSM write bench JSONL outputs by scenario and workload");
    lsm_write_bench_compare_step.dependOn(&run_lsm_write_bench_compare.step);

    const text_segment_write_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/full_text/text_segment_write_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    const text_segment_bench_root_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/text_segment_bench_root.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    text_segment_bench_root_mod.addImport("bloom", bloom_mod);
    text_segment_bench_root_mod.addImport("antfly_vellum", vellum_mod);
    text_segment_bench_root_mod.addImport("antfly_platform", platform_mod);
    text_segment_write_bench_mod.addImport("antfly_text_bench", text_segment_bench_root_mod);
    const text_segment_write_bench = b.addExecutable(.{
        .name = "text_segment_write_bench",
        .root_module = text_segment_write_bench_mod,
    });

    const run_text_segment_write_bench = b.addRunArtifact(text_segment_write_bench);
    if (b.args) |args| {
        run_text_segment_write_bench.addArgs(args);
    } else {
        run_text_segment_write_bench.addArgs(&.{
            "--samples",       "3",
            "--docs",          "20000",
            "--batch-size",    "1000",
            "--terms-per-doc", "12",
            "--merge-width",   "8",
            "--storage",       "host",
        });
    }
    const text_segment_write_bench_step = b.step("text-segment-write-bench", "Benchmark full-text segment build, on-disk publish, merge, and force-merge");
    text_segment_write_bench_step.dependOn(&run_text_segment_write_bench.step);

    const lsm_backend_bench_compare_mod = b.createModule(.{
        .root_source_file = b.path("bench/storage/lsm_backend_bench_compare.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    const lsm_backend_bench_compare = b.addExecutable(.{
        .name = "lsm_backend_bench_compare",
        .root_module = lsm_backend_bench_compare_mod,
    });

    const run_lsm_backend_bench_compare = b.addRunArtifact(lsm_backend_bench_compare);
    if (b.args) |args| {
        run_lsm_backend_bench_compare.addArgs(args);
    } else {
        run_lsm_backend_bench_compare.addArgs(&.{
            "--before",
            "/tmp/lsm-before.jsonl",
            "--after",
            "/tmp/lsm-after.jsonl",
        });
    }
    const lsm_backend_bench_compare_step = b.step("lsm-backend-bench-compare", "Compare two LSM backend bench JSONL outputs by scenario and workload");
    lsm_backend_bench_compare_step.dependOn(&run_lsm_backend_bench_compare.step);

    const regex_bench_mod = b.createModule(.{
        .root_source_file = b.path("lib/regex/bench/regex_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    regex_bench_mod.addImport("antfly_regex", regex_mod);
    regex_bench_mod.addImport("antfly_vellum", vellum_mod);
    const regex_bench = b.addExecutable(.{
        .name = "regex_bench",
        .root_module = regex_bench_mod,
    });

    const run_regex_bench = b.addRunArtifact(regex_bench);
    if (b.args) |args| {
        run_regex_bench.addArgs(args);
    }
    const regex_bench_step = b.step("regex-bench", "Benchmark regex haystack matching and vellum automaton traversal");
    regex_bench_step.dependOn(&run_regex_bench.step);

    const wal_bench_engine_options = makeLmdbBuildOptions(b, lmdb_backend, lmdb_evented_async_io, false);
    const wal_bench_build_options = makeRootBuildOptions(b, lmdb_backend, lmdb_evented_async_io, false, false, true, false, lite_local_inference_runtime, true, antfly_version);
    const wal_bench_engine_mod = makeLmdbEngineModule(b, target, .ReleaseFast, true, wal_bench_engine_options);
    const wal_bench_wal_mod = makeLmdbModule(b, antfly_benches_build.wal_bench_root, target, .ReleaseFast, wal_bench_build_options, wal_bench_engine_mod, platform_mod);
    const wal_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/storage/wal_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    wal_bench_mod.addImport("wal", wal_bench_wal_mod);
    wal_bench_wal_mod.addImport("bloom", bloom_mod);

    const wal_bench = b.addExecutable(.{
        .name = "wal_bench",
        .root_module = wal_bench_mod,
    });

    const run_wal_bench = b.addRunArtifact(wal_bench);
    const wal_bench_step = b.step("wal-bench", "Benchmark WAL append throughput with and without group commit");
    wal_bench_step.dependOn(&run_wal_bench.step);

    const run_wal_bench_repeat = b.addRunArtifact(wal_bench);
    run_wal_bench_repeat.addArgs(&.{ "--samples", "5" });
    const wal_bench_repeat_step = b.step("wal-bench-repeat", "Benchmark WAL append throughput with repeated samples");
    wal_bench_repeat_step.dependOn(&run_wal_bench_repeat.step);

    const run_wal_bench_repeat_long = b.addRunArtifact(wal_bench);
    run_wal_bench_repeat_long.addArgs(&.{ "--samples", "15" });
    const wal_bench_repeat_long_step = b.step("wal-bench-repeat-long", "Benchmark WAL sync backend with 15 repeated samples");
    wal_bench_repeat_long_step.dependOn(&run_wal_bench_repeat_long.step);

    const run_wal_bench_repeat_stress = b.addRunArtifact(wal_bench);
    run_wal_bench_repeat_stress.addArgs(&.{ "--samples", "5", "--sync-delay-us", "2000" });
    const wal_bench_repeat_stress_step = b.step("wal-bench-repeat-stress", "Benchmark WAL sync backend with repeated stressed samples");
    wal_bench_repeat_stress_step.dependOn(&run_wal_bench_repeat_stress.step);

    const run_wal_bench_worker = b.addRunArtifact(wal_bench);
    run_wal_bench_worker.addArg("--worker-thread");
    const wal_bench_worker_step = b.step("wal-bench-worker", "Benchmark WAL append throughput with worker-thread commit backend");
    wal_bench_worker_step.dependOn(&run_wal_bench_worker.step);

    const run_wal_bench_worker_repeat = b.addRunArtifact(wal_bench);
    run_wal_bench_worker_repeat.addArgs(&.{ "--samples", "5", "--worker-thread" });
    const wal_bench_worker_repeat_step = b.step("wal-bench-worker-repeat", "Benchmark WAL append throughput with repeated worker-thread samples");
    wal_bench_worker_repeat_step.dependOn(&run_wal_bench_worker_repeat.step);

    const run_wal_bench_worker_repeat_stress = b.addRunArtifact(wal_bench);
    run_wal_bench_worker_repeat_stress.addArgs(&.{ "--samples", "5", "--worker-thread", "--sync-delay-us", "2000" });
    const wal_bench_worker_repeat_stress_step = b.step("wal-bench-worker-repeat-stress", "Benchmark WAL worker-thread backend with repeated stressed samples");
    wal_bench_worker_repeat_stress_step.dependOn(&run_wal_bench_worker_repeat_stress.step);

    const run_wal_bench_async = b.addRunArtifact(wal_bench);
    run_wal_bench_async.addArg("--async-io");
    const wal_bench_async_step = b.step("wal-bench-async", "Benchmark WAL append throughput with async-io commit backend");
    wal_bench_async_step.dependOn(&run_wal_bench_async.step);

    const run_wal_bench_async_repeat = b.addRunArtifact(wal_bench);
    run_wal_bench_async_repeat.addArgs(&.{ "--samples", "5", "--async-io" });
    const wal_bench_async_repeat_step = b.step("wal-bench-async-repeat", "Benchmark WAL append throughput with repeated async-io samples");
    wal_bench_async_repeat_step.dependOn(&run_wal_bench_async_repeat.step);

    const run_wal_bench_async_repeat_long = b.addRunArtifact(wal_bench);
    run_wal_bench_async_repeat_long.addArgs(&.{ "--samples", "15", "--async-io" });
    const wal_bench_async_repeat_long_step = b.step("wal-bench-async-repeat-long", "Benchmark WAL append throughput with 15 async-io samples");
    wal_bench_async_repeat_long_step.dependOn(&run_wal_bench_async_repeat_long.step);

    const run_wal_bench_async_repeat_stress = b.addRunArtifact(wal_bench);
    run_wal_bench_async_repeat_stress.addArgs(&.{ "--samples", "5", "--async-io", "--sync-delay-us", "2000" });
    const wal_bench_async_repeat_stress_step = b.step("wal-bench-async-repeat-stress", "Benchmark WAL async-io backend with repeated stressed samples");
    wal_bench_async_repeat_stress_step.dependOn(&run_wal_bench_async_repeat_stress.step);

    const run_wal_bench_adaptive = b.addRunArtifact(wal_bench);
    run_wal_bench_adaptive.addArg("--adaptive");
    const wal_bench_adaptive_step = b.step("wal-bench-adaptive", "Benchmark WAL append throughput with adaptive commit backend");
    wal_bench_adaptive_step.dependOn(&run_wal_bench_adaptive.step);

    const run_wal_bench_adaptive_repeat = b.addRunArtifact(wal_bench);
    run_wal_bench_adaptive_repeat.addArgs(&.{ "--samples", "5", "--adaptive" });
    const wal_bench_adaptive_repeat_step = b.step("wal-bench-adaptive-repeat", "Benchmark WAL append throughput with repeated adaptive samples");
    wal_bench_adaptive_repeat_step.dependOn(&run_wal_bench_adaptive_repeat.step);

    const run_wal_bench_adaptive_repeat_long = b.addRunArtifact(wal_bench);
    run_wal_bench_adaptive_repeat_long.addArgs(&.{ "--samples", "15", "--adaptive" });
    const wal_bench_adaptive_repeat_long_step = b.step("wal-bench-adaptive-repeat-long", "Benchmark WAL append throughput with 15 adaptive samples");
    wal_bench_adaptive_repeat_long_step.dependOn(&run_wal_bench_adaptive_repeat_long.step);

    const run_wal_bench_adaptive_stress = b.addRunArtifact(wal_bench);
    run_wal_bench_adaptive_stress.addArgs(&.{ "--samples", "5", "--adaptive", "--sync-delay-us", "2000" });
    const wal_bench_adaptive_stress_step = b.step("wal-bench-adaptive-stress", "Benchmark WAL adaptive backend with artificial sync delay");
    wal_bench_adaptive_stress_step.dependOn(&run_wal_bench_adaptive_stress.step);

    const derived_log_bench_engine_options = makeLmdbBuildOptions(b, lmdb_backend, lmdb_evented_async_io, false);
    const derived_log_bench_build_options = makeRootBuildOptions(b, lmdb_backend, lmdb_evented_async_io, false, false, true, false, lite_local_inference_runtime, true, antfly_version);
    const derived_log_bench_engine_mod = makeLmdbEngineModule(b, target, .ReleaseFast, true, derived_log_bench_engine_options);
    const derived_log_bench_root_mod = b.createModule(.{
        .root_source_file = b.path(antfly_benches_build.derived_log_bench_root),
        .target = target,
        .optimize = .ReleaseFast,
    });
    derived_log_bench_root_mod.addOptions("build_options", derived_log_bench_build_options);
    derived_log_bench_root_mod.addImport("lmdb_engine", derived_log_bench_engine_mod);
    derived_log_bench_root_mod.addImport("bloom", bloom_mod);
    derived_log_bench_root_mod.addImport("antfly_platform", platform_mod);
    derived_log_bench_root_mod.link_libc = true;
    const derived_log_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/storage/derived_log_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    derived_log_bench_mod.addImport("derived_log", derived_log_bench_root_mod);

    const derived_log_bench = b.addExecutable(.{
        .name = "derived_log_bench",
        .root_module = derived_log_bench_mod,
    });

    const run_derived_log_bench = b.addRunArtifact(derived_log_bench);
    const derived_log_bench_step = b.step("derived-log-bench", "Benchmark derived log throughput with and without group commit");
    derived_log_bench_step.dependOn(&run_derived_log_bench.step);

    const run_derived_log_bench_repeat = b.addRunArtifact(derived_log_bench);
    run_derived_log_bench_repeat.addArgs(&.{ "--samples", "5" });
    const derived_log_bench_repeat_step = b.step("derived-log-bench-repeat", "Benchmark derived log throughput with repeated samples");
    derived_log_bench_repeat_step.dependOn(&run_derived_log_bench_repeat.step);

    const run_derived_log_bench_repeat_long = b.addRunArtifact(derived_log_bench);
    run_derived_log_bench_repeat_long.addArgs(&.{ "--samples", "15" });
    const derived_log_bench_repeat_long_step = b.step("derived-log-bench-repeat-long", "Benchmark derived log sync backend with 15 repeated samples");
    derived_log_bench_repeat_long_step.dependOn(&run_derived_log_bench_repeat_long.step);

    const run_derived_log_bench_repeat_stress = b.addRunArtifact(derived_log_bench);
    run_derived_log_bench_repeat_stress.addArgs(&.{ "--samples", "5", "--sync-delay-us", "2000" });
    const derived_log_bench_repeat_stress_step = b.step("derived-log-bench-repeat-stress", "Benchmark derived log sync backend with repeated stressed samples");
    derived_log_bench_repeat_stress_step.dependOn(&run_derived_log_bench_repeat_stress.step);

    const run_derived_log_bench_worker = b.addRunArtifact(derived_log_bench);
    run_derived_log_bench_worker.addArg("--worker-thread");
    const derived_log_bench_worker_step = b.step("derived-log-bench-worker", "Benchmark derived log throughput with worker-thread commit backend");
    derived_log_bench_worker_step.dependOn(&run_derived_log_bench_worker.step);

    const run_derived_log_bench_worker_repeat = b.addRunArtifact(derived_log_bench);
    run_derived_log_bench_worker_repeat.addArgs(&.{ "--samples", "5", "--worker-thread" });
    const derived_log_bench_worker_repeat_step = b.step("derived-log-bench-worker-repeat", "Benchmark derived log throughput with repeated worker-thread samples");
    derived_log_bench_worker_repeat_step.dependOn(&run_derived_log_bench_worker_repeat.step);

    const run_derived_log_bench_worker_repeat_stress = b.addRunArtifact(derived_log_bench);
    run_derived_log_bench_worker_repeat_stress.addArgs(&.{ "--samples", "5", "--worker-thread", "--sync-delay-us", "2000" });
    const derived_log_bench_worker_repeat_stress_step = b.step("derived-log-bench-worker-repeat-stress", "Benchmark derived log worker-thread backend with repeated stressed samples");
    derived_log_bench_worker_repeat_stress_step.dependOn(&run_derived_log_bench_worker_repeat_stress.step);

    const run_derived_log_bench_async = b.addRunArtifact(derived_log_bench);
    run_derived_log_bench_async.addArg("--async-io");
    const derived_log_bench_async_step = b.step("derived-log-bench-async", "Benchmark derived log throughput with async-io commit backend");
    derived_log_bench_async_step.dependOn(&run_derived_log_bench_async.step);

    const run_derived_log_bench_async_repeat = b.addRunArtifact(derived_log_bench);
    run_derived_log_bench_async_repeat.addArgs(&.{ "--samples", "5", "--async-io" });
    const derived_log_bench_async_repeat_step = b.step("derived-log-bench-async-repeat", "Benchmark derived log throughput with repeated async-io samples");
    derived_log_bench_async_repeat_step.dependOn(&run_derived_log_bench_async_repeat.step);

    const run_derived_log_bench_async_repeat_long = b.addRunArtifact(derived_log_bench);
    run_derived_log_bench_async_repeat_long.addArgs(&.{ "--samples", "15", "--async-io" });
    const derived_log_bench_async_repeat_long_step = b.step("derived-log-bench-async-repeat-long", "Benchmark derived log throughput with 15 async-io samples");
    derived_log_bench_async_repeat_long_step.dependOn(&run_derived_log_bench_async_repeat_long.step);

    const run_derived_log_bench_async_repeat_stress = b.addRunArtifact(derived_log_bench);
    run_derived_log_bench_async_repeat_stress.addArgs(&.{ "--samples", "5", "--async-io", "--sync-delay-us", "2000" });
    const derived_log_bench_async_repeat_stress_step = b.step("derived-log-bench-async-repeat-stress", "Benchmark derived log async-io backend with repeated stressed samples");
    derived_log_bench_async_repeat_stress_step.dependOn(&run_derived_log_bench_async_repeat_stress.step);

    const run_derived_log_bench_adaptive = b.addRunArtifact(derived_log_bench);
    run_derived_log_bench_adaptive.addArg("--adaptive");
    const derived_log_bench_adaptive_step = b.step("derived-log-bench-adaptive", "Benchmark derived log throughput with adaptive commit backend");
    derived_log_bench_adaptive_step.dependOn(&run_derived_log_bench_adaptive.step);

    const run_derived_log_bench_adaptive_repeat = b.addRunArtifact(derived_log_bench);
    run_derived_log_bench_adaptive_repeat.addArgs(&.{ "--samples", "5", "--adaptive" });
    const derived_log_bench_adaptive_repeat_step = b.step("derived-log-bench-adaptive-repeat", "Benchmark derived log throughput with repeated adaptive samples");
    derived_log_bench_adaptive_repeat_step.dependOn(&run_derived_log_bench_adaptive_repeat.step);

    const run_derived_log_bench_adaptive_repeat_long = b.addRunArtifact(derived_log_bench);
    run_derived_log_bench_adaptive_repeat_long.addArgs(&.{ "--samples", "15", "--adaptive" });
    const derived_log_bench_adaptive_repeat_long_step = b.step("derived-log-bench-adaptive-repeat-long", "Benchmark derived log throughput with 15 adaptive samples");
    derived_log_bench_adaptive_repeat_long_step.dependOn(&run_derived_log_bench_adaptive_repeat_long.step);

    const run_derived_log_bench_adaptive_stress = b.addRunArtifact(derived_log_bench);
    run_derived_log_bench_adaptive_stress.addArgs(&.{ "--samples", "5", "--adaptive", "--sync-delay-us", "2000" });
    const derived_log_bench_adaptive_stress_step = b.step("derived-log-bench-adaptive-stress", "Benchmark derived log adaptive backend with artificial sync delay");
    derived_log_bench_adaptive_stress_step.dependOn(&run_derived_log_bench_adaptive_stress.step);

    const json_bench_mod = b.createModule(.{
        .root_source_file = b.path("lib/json/bench/json_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    json_bench_mod.addImport("antfly-json", json_mod);

    const json_bench = b.addExecutable(.{
        .name = "json_bench",
        .root_module = json_bench_mod,
    });

    const run_json_bench = b.addRunArtifact(json_bench);
    if (b.args) |args| {
        run_json_bench.addArgs(args);
    }
    const json_bench_step = b.step("json-bench", "Benchmark std.json vs antfly-json parsing");
    json_bench_step.dependOn(&run_json_bench.step);

    const tokenizer_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/tokenizer_benchmark.zig"),
        .target = target,
        .optimize = .ReleaseFast,
        .link_libc = true,
    });
    tokenizer_bench_mod.addImport("inference_tokenizer", inference_graph.inference_tokenizer_mod);
    const tokenizer_bench = b.addExecutable(.{
        .name = "tokenizer_benchmark",
        .root_module = tokenizer_bench_mod,
    });
    const install_tokenizer_bench = b.addInstallArtifact(tokenizer_bench, .{});
    const tokenizer_bench_build_step = b.step(
        "bench-tokenizer-build",
        "Build the native Zig HuggingFace tokenizer benchmark binary",
    );
    tokenizer_bench_build_step.dependOn(&install_tokenizer_bench.step);
    const run_tokenizer_bench = b.addRunArtifact(tokenizer_bench);
    if (b.args) |args| {
        run_tokenizer_bench.addArgs(args);
    }
    const tokenizer_bench_step = b.step("bench-tokenizer", "Benchmark the native Zig HuggingFace tokenizer");
    tokenizer_bench_step.dependOn(&run_tokenizer_bench.step);

    // Benchmark executable
    const bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    bench_mod.addImport("antfly-zig", lib_mod);

    const bench = b.addExecutable(.{
        .name = "bench",
        .root_module = bench_mod,
    });

    const run_bench = b.addRunArtifact(bench);
    const bench_step = b.step("bench", "Run benchmarks");
    bench_step.dependOn(&run_bench.step);

    // Quickstart-shaped benchmark: mirrors the workload of
    // `test_text_quickstart_and_document_artifact` (e2e/antfly/test_quickstart.py)
    // so the per-iteration cost can be compared against the per-primitive
    // numbers reported by `bench`. Uses a slim root module so it only depends
    // on text/search code (and skips OpenAPI codegen).
    const quickstart_bench_root_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/quickstart_bench_root.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    quickstart_bench_root_mod.addImport("antfly_vellum", vellum_mod);
    quickstart_bench_root_mod.addImport("bloom", bloom_mod);
    addSnowballModule(b, quickstart_bench_root_mod);

    const quickstart_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/quickstart_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
        .link_libc = true,
    });
    quickstart_bench_mod.addImport("antfly_quickstart_bench", quickstart_bench_root_mod);

    const quickstart_bench = b.addExecutable(.{
        .name = "quickstart_bench",
        .root_module = quickstart_bench_mod,
    });

    const run_quickstart_bench = b.addRunArtifact(quickstart_bench);
    if (b.args) |args| {
        run_quickstart_bench.addArgs(args);
    }
    const quickstart_bench_step = b.step("quickstart-bench", "Run the quickstart-shaped end-to-end benchmark");
    quickstart_bench_step.dependOn(&run_quickstart_bench.step);

    const compat_mod = b.createModule(.{
        .root_source_file = b.path("bench/compat_runner.zig"),
        .target = target,
        .optimize = optimize,
    });
    compat_mod.addImport("antfly-zig", lib_mod);

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
        .optimize = .ReleaseFast,
    });
    search_benchmark_index_mod.addImport("antfly-zig", lib_mod);
    const search_benchmark_index = b.addExecutable(.{
        .name = "search_benchmark_index",
        .root_module = search_benchmark_index_mod,
    });
    const install_search_benchmark_index = b.addInstallArtifact(search_benchmark_index, .{});

    const search_benchmark_query_mod = b.createModule(.{
        .root_source_file = b.path("bench/full_text/search_benchmark_query.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    search_benchmark_query_mod.addImport("antfly-zig", lib_mod);
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
    search_benchmark_common_test_mod.addImport("antfly-zig", lib_mod);
    const search_benchmark_common_tests = b.addTest(.{
        .root_module = search_benchmark_common_test_mod,
    });
    const run_search_benchmark_common_tests = b.addRunArtifact(search_benchmark_common_tests);
    const search_bench_test_step = b.step("search-bench-test", "Run search benchmark grammar and protocol tests");
    search_bench_test_step.dependOn(&run_search_benchmark_common_tests.step);

    const search_performance_tests = b.addTest(.{
        .root_module = lib_test_mod,
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
        .optimize = .ReleaseFast,
    });
    search_benchmark_codec_bench_mod.addImport("antfly-zig", lib_mod);
    const search_benchmark_codec_bench = b.addExecutable(.{
        .name = "search_benchmark_codec_bench",
        .root_module = search_benchmark_codec_bench_mod,
    });
    const install_search_benchmark_codec_bench = b.addInstallArtifact(search_benchmark_codec_bench, .{});

    const run_search_benchmark_codec_bench = b.addRunArtifact(search_benchmark_codec_bench);
    if (b.args) |args| {
        run_search_benchmark_codec_bench.addArgs(args);
    }
    const search_bench_codec_step = b.step("search-bench-codec-bench", "Benchmark StreamVByte codec used by search postings");
    search_bench_codec_step.dependOn(&run_search_benchmark_codec_bench.step);

    const search_benchmark_bitpack_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/full_text/search_benchmark_bitpack_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    search_benchmark_bitpack_bench_mod.addImport("antfly-zig", lib_mod);
    const search_benchmark_bitpack_bench = b.addExecutable(.{
        .name = "search_benchmark_bitpack_bench",
        .root_module = search_benchmark_bitpack_bench_mod,
    });
    const install_search_benchmark_bitpack_bench = b.addInstallArtifact(search_benchmark_bitpack_bench, .{});
    const run_search_benchmark_bitpack_bench = b.addRunArtifact(search_benchmark_bitpack_bench);
    if (b.args) |args| run_search_benchmark_bitpack_bench.addArgs(args);
    const search_bench_bitpack_step = b.step("search-bench-bitpack-bench", "Benchmark portable Zig vector BP128 against horizontal bit packing");
    search_bench_bitpack_step.dependOn(&run_search_benchmark_bitpack_bench.step);

    const search_impact_layout_analyze_mod = b.createModule(.{
        .root_source_file = b.path("bench/full_text/search_impact_layout_analyze.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    search_impact_layout_analyze_mod.addImport("antfly-zig", lib_mod);
    const search_impact_layout_analyze = b.addExecutable(.{
        .name = "search_impact_layout_analyze",
        .root_module = search_impact_layout_analyze_mod,
    });
    const run_search_impact_layout_analyze = b.addRunArtifact(search_impact_layout_analyze);
    if (b.args) |args| run_search_impact_layout_analyze.addArgs(args);
    const search_impact_layout_analyze_step = b.step("search-impact-layout-analyze", "Project exact adaptive impact-column density for segment files");
    search_impact_layout_analyze_step.dependOn(&run_search_impact_layout_analyze.step);

    const wand_skip_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/full_text/wand_skip_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    wand_skip_bench_mod.addImport("antfly-zig", lib_mod);
    const wand_skip_bench = b.addExecutable(.{
        .name = "wand_skip_bench",
        .root_module = wand_skip_bench_mod,
    });
    const run_wand_skip_bench = b.addRunArtifact(wand_skip_bench);
    if (b.args) |args| {
        run_wand_skip_bench.addArgs(args);
    }
    const wand_skip_bench_step = b.step("wand-skip-bench", "Profile WAND advance vs score iter.next() ratio across query shapes");
    wand_skip_bench_step.dependOn(&run_wand_skip_bench.step);

    const search_bench_build_step = b.step("search-bench-build", "Build search-benchmark-game antfly-zig adapter and search codec benchmark binaries");
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

    const run_storage_fixture_promote = b.addRunArtifact(storage_fixture_promote);
    if (b.args) |args| {
        run_storage_fixture_promote.addArgs(args);
    }
    const storage_fixture_promote_step = b.step("storage-fixture-promote", "Promote a storage sim fixture into the checked-in replay corpus");
    storage_fixture_promote_step.dependOn(&run_storage_fixture_promote.step);

    const lmdb_fixture_promote_step = b.step("lmdb-fixture-promote", "Promote an LMDB replay fixture into pkg/antfly/src/storage/lmdb_sim_fixtures");
    lmdb_fixture_promote_step.dependOn(&run_storage_fixture_promote.step);

    const merge_cycle_mod = b.createModule(.{
        .root_source_file = b.path("bench/full_text/merge_cycle_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    merge_cycle_mod.addImport("antfly-zig", lib_mod);

    const merge_cycle = b.addExecutable(.{
        .name = "merge_cycle_bench",
        .root_module = merge_cycle_mod,
    });

    const run_merge_cycle = b.addRunArtifact(merge_cycle);
    const merge_cycle_step = b.step("merge-cycle", "Run the merge-cycle benchmark");
    merge_cycle_step.dependOn(&run_merge_cycle.step);

    const merge_cost_mod = b.createModule(.{
        .root_source_file = b.path("bench/full_text/merge_cost_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    merge_cost_mod.addImport("antfly-zig", lib_mod);

    const merge_cost = b.addExecutable(.{
        .name = "merge_cost_bench",
        .root_module = merge_cost_mod,
    });

    const run_merge_cost = b.addRunArtifact(merge_cost);
    const merge_cost_step = b.step("merge-cost", "Run the direct merge cost benchmark");
    merge_cost_step.dependOn(&run_merge_cost.step);

    const hbc_parity_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/tools/hbc_parity.zig"),
        .target = target,
        .optimize = optimize,
    });
    hbc_parity_mod.addImport("antfly-zig", lib_mod);

    const hbc_parity = b.addExecutable(.{
        .name = "hbc_parity",
        .root_module = hbc_parity_mod,
    });

    const run_hbc_parity = b.addRunArtifact(hbc_parity);
    const hbc_parity_step = b.step("hbc-parity", "Run the deterministic HBC parity harness");
    hbc_parity_step.dependOn(&run_hbc_parity.step);

    const hbc_bench_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/bench/hbc_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    hbc_bench_mod.addImport("antfly-zig", lib_mod);

    const hbc_bench = b.addExecutable(.{
        .name = "hbc_bench",
        .root_module = hbc_bench_mod,
    });

    const run_hbc_bench = b.addRunArtifact(hbc_bench);
    if (b.args) |args| {
        run_hbc_bench.addArgs(args);
    }
    const hbc_bench_step = b.step("hbc-bench", "Benchmark HBC kmeans vs hilbert split algorithms");
    hbc_bench_step.dependOn(&run_hbc_bench.step);

    const hbc_write_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/vectors/hbc_write_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    hbc_write_bench_mod.addImport("antfly-zig", lib_mod);

    const hbc_write_bench = b.addExecutable(.{
        .name = "hbc_write_bench",
        .root_module = hbc_write_bench_mod,
    });

    const run_hbc_write_bench = b.addRunArtifact(hbc_write_bench);
    if (b.args) |args| {
        run_hbc_write_bench.addArgs(args);
    } else {
        run_hbc_write_bench.addArgs(&.{
            "--samples",    "3",
            "--vectors",    "10000",
            "--dims",       "128",
            "--batch-size", "1000",
            "--leaf-size",  "128",
            "--storage",    "host",
        });
    }
    const hbc_write_bench_step = b.step("hbc-write-bench", "Benchmark HBC bulk build and online batched write amplification");
    hbc_write_bench_step.dependOn(&run_hbc_write_bench.step);

    const run_hbc_write_guardrail = b.addRunArtifact(hbc_write_bench);
    if (b.args) |args| {
        run_hbc_write_guardrail.addArgs(args);
    } else {
        run_hbc_write_guardrail.addArgs(&.{
            "--samples",    "1",
            "--vectors",    "5000",
            "--dims",       "1536",
            "--batch-size", "500",
            "--leaf-size",  "168",
            "--storage",    "host",
        });
    }
    const hbc_write_guardrail_step = b.step("hbc-write-guardrail", "Run a VectorDBBench-shaped HBC write-amplification smoke guardrail");
    hbc_write_guardrail_step.dependOn(&run_hbc_write_guardrail.step);

    const hbc_read_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/vectors/hbc_read_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    hbc_read_bench_mod.addImport("antfly-zig", lib_mod);

    const hbc_read_bench = b.addExecutable(.{
        .name = "hbc_read_bench",
        .root_module = hbc_read_bench_mod,
    });

    const run_hbc_read_bench = b.addRunArtifact(hbc_read_bench);
    if (b.args) |args| {
        run_hbc_read_bench.addArgs(args);
    } else {
        run_hbc_read_bench.addArgs(&.{
            "--samples",    "3",
            "--vectors",    "10000",
            "--dims",       "128",
            "--queries",    "200",
            "--k",          "10",
            "--batch-size", "1000",
            "--leaf-size",  "128",
            "--storage",    "host",
            "--build",      "both",
        });
    }
    const hbc_read_bench_step = b.step("hbc-read-bench", "Benchmark HBC query read paths with storage and search-profile counters");
    hbc_read_bench_step.dependOn(&run_hbc_read_bench.step);

    const hbc_isolate_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/tools/hbc_isolate.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    const hbc_isolate_root_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/hbc_isolate_root.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    hbc_isolate_root_mod.addImport("lmdb_engine", lmdb_engine_mod);
    hbc_isolate_root_mod.addImport("bloom", bloom_mod);
    hbc_isolate_root_mod.addImport("antfly_vector", vector_mod);
    hbc_isolate_root_mod.addImport("antfly_vectorindex", vectorindex_mod);
    hbc_isolate_root_mod.addImport("antfly_platform", platform_mod);
    hbc_isolate_mod.addImport("antfly_hbc_isolate_root", hbc_isolate_root_mod);

    const hbc_isolate = b.addExecutable(.{
        .name = "hbc_isolate",
        .root_module = hbc_isolate_mod,
    });

    const run_hbc_isolate = b.addRunArtifact(hbc_isolate);
    if (b.args) |args| {
        run_hbc_isolate.addArgs(args);
    }
    const hbc_isolate_step = b.step("hbc-isolate", "Run the deterministic raw Zig HBC isolate benchmark");
    hbc_isolate_step.dependOn(&run_hbc_isolate.step);

    const dense_stack_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/vectors/dense_stack_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    dense_stack_bench_mod.addImport("antfly-zig", lib_mod);
    const capi_bench_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/capi/root.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    capi_bench_mod.addImport("antfly-zig", lib_mod);
    dense_stack_bench_mod.addImport("antfly_capi", capi_bench_mod);

    const dense_stack_bench = b.addExecutable(.{
        .name = "dense_stack_bench",
        .root_module = dense_stack_bench_mod,
    });

    const run_dense_stack_bench = b.addRunArtifact(dense_stack_bench);
    if (b.args) |args| {
        run_dense_stack_bench.addArgs(args);
    }
    const build_dense_stack_bench_step = b.step("dense-stack-bench-build", "Build dense_stack_bench without running it");
    build_dense_stack_bench_step.dependOn(&dense_stack_bench.step);
    const dense_stack_bench_step = b.step("dense-stack-bench", "Benchmark dense DB search vs dense CAPI layers");
    dense_stack_bench_step.dependOn(&run_dense_stack_bench.step);

    const replay_bench_build_options = b.addOptions();
    replay_bench_build_options.addOption([]const u8, "lmdb_backend", @tagName(lmdb_backend));
    replay_bench_build_options.addOption(bool, "lmdb_evented_async_io", lmdb_evented_async_io);
    replay_bench_build_options.addOption(bool, "storage_sim_soak", false);
    replay_bench_build_options.addOption(bool, "with_tla", with_tla);
    replay_bench_build_options.addOption(bool, "link_libc", true);
    replay_bench_build_options.addOption(bool, "standalone_runtime_focused_test", false);
    replay_bench_build_options.addOption(bool, "lmdb_enabled", true);
    replay_bench_build_options.addOption(bool, "bench_minimal_deps", true);

    const replay_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/storage/replay_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    const replay_bench_root_mod = b.createModule(.{
        .root_source_file = b.path(antfly_benches_build.replay_bench_root),
        .target = target,
        .optimize = .ReleaseFast,
    });
    replay_bench_root_mod.addOptions("build_options", replay_bench_build_options);
    replay_bench_root_mod.addImport("lmdb_engine", lmdb_engine_mod);
    replay_bench_root_mod.addImport("antfly-json", json_mod);
    replay_bench_root_mod.addImport("bloom", bloom_mod);
    replay_bench_root_mod.addImport("antfly_vector", vector_mod);
    replay_bench_root_mod.addImport("antfly_vectorindex", vectorindex_mod);
    replay_bench_root_mod.addImport("antfly_matcher", matcher_mod);
    replay_bench_root_mod.addImport("antfly_resolver", resolver_mod);
    replay_bench_root_mod.addImport("antfly_vellum", vellum_mod);
    replay_bench_root_mod.addImport("antfly_regex", regex_mod);
    replay_bench_root_mod.addImport("antfly_reranking", reranking_mod);
    replay_bench_root_mod.addImport("antfly_scraping", scraping_mod);
    replay_bench_root_mod.addImport("antfly_platform", platform_mod);
    addSnowballModule(b, replay_bench_root_mod);
    replay_bench_mod.addImport("antfly-zig", replay_bench_root_mod);

    const replay_bench = b.addExecutable(.{
        .name = "replay_bench",
        .root_module = replay_bench_mod,
    });

    const run_replay_bench = b.addRunArtifact(replay_bench);
    if (b.args) |args| {
        run_replay_bench.addArgs(args);
    }
    const replay_bench_step = b.step("replay-bench", "Benchmark replay stream write and catch-up paths");
    replay_bench_step.dependOn(&run_replay_bench.step);

    const dense_ingest_guardrail_mod = b.createModule(.{
        .root_source_file = b.path("bench/vectors/dense_ingest_guardrail.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    dense_ingest_guardrail_mod.addImport("antfly-zig", replay_bench_root_mod);

    const dense_ingest_guardrail = b.addExecutable(.{
        .name = "dense_ingest_guardrail",
        .root_module = dense_ingest_guardrail_mod,
    });
    const install_dense_ingest_guardrail = b.addInstallArtifact(dense_ingest_guardrail, .{});

    const run_dense_ingest_guardrail = b.addRunArtifact(dense_ingest_guardrail);
    if (b.args) |args| {
        run_dense_ingest_guardrail.addArgs(args);
    } else {
        run_dense_ingest_guardrail.addArgs(&.{
            "--docs",
            "5000",
            "--dims",
            "1536",
            "--batch-size",
            "500",
            "--sync-level",
            "write",
            "--status-probe-every",
            "1",
            "--max-dense-lsm-run-bytes",
            "1073741824",
            "--max-dense-l0-runs",
            "64",
            "--max-status-probe-ns",
            "500000000",
        });
    }
    const build_dense_ingest_guardrail_step = b.step("dense-ingest-guardrail-build", "Build the dedicated dense ingest guardrail without running it");
    build_dense_ingest_guardrail_step.dependOn(&dense_ingest_guardrail.step);
    const install_dense_ingest_guardrail_step = b.step("dense-ingest-guardrail-install", "Build and install the dedicated dense ingest guardrail");
    install_dense_ingest_guardrail_step.dependOn(&install_dense_ingest_guardrail.step);

    const batch_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/storage/batch_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    batch_bench_mod.addImport("antfly-zig", replay_bench_root_mod);

    const batch_bench = b.addExecutable(.{
        .name = "batch_bench",
        .root_module = batch_bench_mod,
    });

    const run_batch_bench = b.addRunArtifact(batch_bench);
    if (b.args) |args| {
        run_batch_bench.addArgs(args);
    }
    const batch_bench_step = b.step("batch-bench", "Benchmark overwrite-heavy batch writes and bulk-session coalescing");
    batch_bench_step.dependOn(&run_batch_bench.step);

    const docid_write_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/storage/docid_write_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    docid_write_bench_mod.addImport("antfly-zig", replay_bench_root_mod);

    const docid_write_bench = b.addExecutable(.{
        .name = "docid_write_bench",
        .root_module = docid_write_bench_mod,
    });

    const run_docid_write_bench = b.addRunArtifact(docid_write_bench);
    if (b.args) |args| {
        run_docid_write_bench.addArgs(args);
    } else {
        run_docid_write_bench.addArgs(&.{ "--docs", "512", "--batch-size", "128", "--body-repeat", "1" });
    }
    const docid_write_bench_step = b.step("docid-write-bench", "Benchmark DOCID write-path identity metadata overhead across sync levels");
    docid_write_bench_step.dependOn(&run_docid_write_bench.step);

    const docid_query_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/storage/docid_query_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    docid_query_bench_mod.addImport("antfly-zig", replay_bench_root_mod);

    const docid_query_bench = b.addExecutable(.{
        .name = "docid_query_bench",
        .root_module = docid_query_bench_mod,
    });

    const run_docid_query_bench = b.addRunArtifact(docid_query_bench);
    if (b.args) |args| {
        run_docid_query_bench.addArgs(args);
    } else {
        run_docid_query_bench.addArgs(&.{ "--docs", "4096", "--queries", "16", "--repeats", "8", "--filter-size", "256", "--limit", "32" });
    }
    const docid_query_bench_step = b.step("docid-query-bench", "Benchmark real DB query shapes with public IDs, ordinal doc sets, and sparse-ID projection");
    docid_query_bench_step.dependOn(&run_docid_query_bench.step);
    const build_docid_query_bench_step = b.step("docid-query-bench-build", "Build docid_query_bench without running it");
    build_docid_query_bench_step.dependOn(&docid_query_bench.step);

    const algebraic_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/storage/algebraic_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    const algebraic_bench_root_mod = b.createModule(.{
        .root_source_file = b.path(antfly_benches_build.algebraic_bench_root),
        .target = target,
        .optimize = .ReleaseFast,
    });
    algebraic_bench_root_mod.addOptions("build_options", replay_bench_build_options);
    algebraic_bench_root_mod.addImport("lmdb_engine", lmdb_engine_mod);
    algebraic_bench_root_mod.addImport("antfly-json", json_mod);
    algebraic_bench_root_mod.addImport("bloom", bloom_mod);
    algebraic_bench_root_mod.addImport("antfly_vector", vector_mod);
    algebraic_bench_root_mod.addImport("antfly_vectorindex", vectorindex_mod);
    algebraic_bench_root_mod.addImport("antfly_matcher", matcher_mod);
    algebraic_bench_root_mod.addImport("antfly_vellum", vellum_mod);
    algebraic_bench_root_mod.addImport("antfly_regex", regex_mod);
    algebraic_bench_root_mod.addImport("antfly_platform", platform_mod);
    algebraic_bench_root_mod.addImport("antfly_reranking", reranking_mod);
    algebraic_bench_root_mod.addImport("antfly_resolver", resolver_mod);
    algebraic_bench_root_mod.addImport("antfly_reader_config", reader_config_mod);
    addSnowballModule(b, algebraic_bench_root_mod);
    algebraic_bench_mod.addImport("antfly-zig", algebraic_bench_root_mod);

    const algebraic_bench = b.addExecutable(.{
        .name = "algebraic_bench",
        .root_module = algebraic_bench_mod,
    });

    const run_algebraic_bench = b.addRunArtifact(algebraic_bench);
    if (b.args) |args| {
        run_algebraic_bench.addArgs(args);
    } else {
        run_algebraic_bench.addArgs(&.{
            "--docs",
            "20000",
            "--repeats",
            "25",
            "--batch-size",
            "500",
        });
    }
    const algebraic_bench_step = b.step("algebraic-bench", "Benchmark algebraic aggregations against document-scan aggregations");
    algebraic_bench_step.dependOn(&run_algebraic_bench.step);

    const algebraic_summary_mod = b.createModule(.{
        .root_source_file = b.path("bench/storage/algebraic_summary.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    const algebraic_summary = b.addExecutable(.{
        .name = "algebraic_summary",
        .root_module = algebraic_summary_mod,
    });
    const run_algebraic_summary = b.addRunArtifact(algebraic_summary);
    if (b.args) |args| {
        run_algebraic_summary.addArgs(args);
    }
    const algebraic_summary_step = b.step("algebraic-summary", "Summarize algebraic benchmark JSONL output");
    algebraic_summary_step.dependOn(&run_algebraic_summary.step);

    const run_algebraic_performance_guardrail = b.addRunArtifact(algebraic_summary);
    if (b.args) |args| {
        run_algebraic_performance_guardrail.addArgs(args);
    } else {
        run_algebraic_performance_guardrail.addArgs(&.{
            "--input",
            "bench/storage/algebraic_performance_guardrail_fixture.jsonl",
            "--baseline",
            "bench/storage/algebraic_performance_guardrail_baseline.jsonl",
            "--require-performance-evidence",
            "--min-lsm-dataset-cases",
            "1",
            "--min-lsm-query-records",
            "3",
            "--min-cold-query-records",
            "2",
            "--min-warm-query-records",
            "2",
            "--min-constrained-query-records",
            "3",
            "--min-wide-query-records",
            "3",
            "--min-stats-query-records",
            "3",
            "--min-cardinality-query-records",
            "3",
            "--min-range-query-records",
            "3",
            "--min-histogram-query-records",
            "3",
            "--min-fanout-dataset-cases",
            "1",
            "--min-public-query-comparison-pairs",
            "2",
            "--min-lsm-sorted-ingest-runs",
            "1",
            "--max-lsm-flushes",
            "0",
            "--max-lsm-write-pressure-compactions",
            "0",
            "--max-correctness-failures",
            "0",
            "--max-algebraic-query-ms",
            "2",
            "--max-public-query-http-us",
            "100",
            "--max-algebraic-bytes-per-doc",
            "10",
            "--max-symbol-bytes-per-doc",
            "0",
            "--max-support-bytes-per-doc",
            "0",
            "--max-accumulator-flush-count",
            "0",
            "--max-path-dictionary-fst-rebuild-count",
            "1",
            "--max-public-query-load-rss-peak-bytes",
            "0",
            "--max-public-query-search-rss-peak-bytes",
            "0",
            "--max-churn-algebraic-update-ms",
            "2",
            "--max-algebraic-query-ms-ratio-vs-baseline",
            "1.0",
            "--max-public-query-http-us-ratio-vs-baseline",
            "1.0",
            "--max-algebraic-bytes-per-doc-ratio-vs-baseline",
            "1.0",
            "--max-churn-algebraic-update-ms-ratio-vs-baseline",
            "1.0",
        });
    }
    const algebraic_performance_guardrail_step = b.step("algebraic-performance-guardrail", "Run the algebraic benchmark summary coverage and baseline-ratio guardrail fixture");
    algebraic_performance_guardrail_step.dependOn(&run_algebraic_performance_guardrail.step);

    const algebraic_planner_ownership_guardrail_mod = b.createModule(.{
        .root_source_file = b.path("tools/guardrails/algebraic_planner_ownership_guardrail.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    const algebraic_planner_ownership_guardrail = b.addExecutable(.{
        .name = "algebraic_planner_ownership_guardrail",
        .root_module = algebraic_planner_ownership_guardrail_mod,
    });
    const run_algebraic_planner_ownership_guardrail = b.addRunArtifact(algebraic_planner_ownership_guardrail);
    if (b.args) |args| {
        run_algebraic_planner_ownership_guardrail.addArgs(args);
    }
    const algebraic_planner_ownership_guardrail_step = b.step("algebraic-planner-ownership-guardrail", "Verify algebraic tensor programs are built by the planner layer outside tests");
    algebraic_planner_ownership_guardrail_step.dependOn(&run_algebraic_planner_ownership_guardrail.step);

    const algebraic_archive_guardrail_mod = b.createModule(.{
        .root_source_file = b.path("bench/storage/algebraic_archive_guardrail.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    const algebraic_archive_guardrail = b.addExecutable(.{
        .name = "algebraic_archive_guardrail",
        .root_module = algebraic_archive_guardrail_mod,
    });
    const run_algebraic_archive_guardrail = b.addRunArtifact(algebraic_archive_guardrail);
    if (b.args) |args| {
        run_algebraic_archive_guardrail.addArgs(args);
    } else {
        run_algebraic_archive_guardrail.addArgs(&.{
            "--archive",
            "bench/storage/algebraic_production_archive_fixture",
            "--require-thresholds",
            "--require-baseline",
            "--require-non-smoke",
            "--min-docs",
            "100",
            "--min-repeats",
            "1",
            "--min-churn-ops",
            "1",
            "--min-public-docs",
            "100",
            "--min-graph-docs",
            "100",
        });
    }
    const algebraic_archive_guardrail_step = b.step("algebraic-archive-guardrail", "Verify archived algebraic production-hardening run evidence");
    algebraic_archive_guardrail_step.dependOn(&run_algebraic_archive_guardrail.step);

    const algebraic_roadmap_guardrail_step = b.step("algebraic-roadmap-guardrail", "Run CI-safe algebraic roadmap guardrails");
    algebraic_roadmap_guardrail_step.dependOn(&run_algebraic_performance_guardrail.step);
    algebraic_roadmap_guardrail_step.dependOn(&run_algebraic_planner_ownership_guardrail.step);
    algebraic_roadmap_guardrail_step.dependOn(&run_algebraic_archive_guardrail.step);

    const rw_lock_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/storage/rw_lock_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    rw_lock_bench_mod.addImport("antfly-zig", lib_mod);

    const rw_lock_bench = b.addExecutable(.{
        .name = "rw_lock_bench",
        .root_module = rw_lock_bench_mod,
    });

    const run_rw_lock_bench = b.addRunArtifact(rw_lock_bench);
    if (b.args) |args| {
        run_rw_lock_bench.addArgs(args);
    }
    const rw_lock_bench_step = b.step("rw-lock-bench", "Benchmark mixed search/write load against the DB RW apply lock");
    rw_lock_bench_step.dependOn(&run_rw_lock_bench.step);

    const open_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/storage/open_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    open_bench_mod.addImport("antfly-zig", lib_mod);

    const open_bench = b.addExecutable(.{
        .name = "open_bench",
        .root_module = open_bench_mod,
    });

    const run_open_bench = b.addRunArtifact(open_bench);
    if (b.args) |args| {
        run_open_bench.addArgs(args);
    }
    const open_bench_step = b.step("open-bench", "Benchmark DB.open for configurable index mixes and replay backlog");
    open_bench_step.dependOn(&run_open_bench.step);

    const artifact_rebuild_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/storage/artifact_rebuild_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    artifact_rebuild_bench_mod.addImport("antfly-zig", lib_mod);

    const artifact_rebuild_bench = b.addExecutable(.{
        .name = "artifact_rebuild_bench",
        .root_module = artifact_rebuild_bench_mod,
    });

    const run_artifact_rebuild_bench = b.addRunArtifact(artifact_rebuild_bench);
    if (b.args) |args| {
        run_artifact_rebuild_bench.addArgs(args);
    }
    const build_artifact_rebuild_bench_step = b.step("artifact-rebuild-bench-build", "Build artifact_rebuild_bench without running it");
    build_artifact_rebuild_bench_step.dependOn(&artifact_rebuild_bench.step);
    const artifact_rebuild_bench_step = b.step("artifact-rebuild-bench", "Benchmark loaded-root startup artifact rebuild progress and reopen cost");
    artifact_rebuild_bench_step.dependOn(&run_artifact_rebuild_bench.step);

    const provisioned_warmup_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/storage/provisioned_warmup_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    provisioned_warmup_bench_mod.addImport("antfly-zig", lib_mod);

    const provisioned_warmup_bench = b.addExecutable(.{
        .name = "provisioned_warmup_bench",
        .root_module = provisioned_warmup_bench_mod,
    });

    const run_provisioned_warmup_bench = b.addRunArtifact(provisioned_warmup_bench);
    if (b.args) |args| {
        run_provisioned_warmup_bench.addArgs(args);
    }
    const provisioned_warmup_bench_step = b.step("provisioned-warmup-bench", "Benchmark provisioned cache warmup against first read/write latency");
    provisioned_warmup_bench_step.dependOn(&run_provisioned_warmup_bench.step);

    const provisioned_dense_ingest_guardrail_mod = b.createModule(.{
        .root_source_file = b.path("bench/vectors/provisioned_dense_ingest_guardrail.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    provisioned_dense_ingest_guardrail_mod.addImport("antfly-zig", lib_mod);

    const provisioned_dense_ingest_guardrail = b.addExecutable(.{
        .name = "provisioned_dense_ingest_guardrail",
        .root_module = provisioned_dense_ingest_guardrail_mod,
    });
    const install_provisioned_dense_ingest_guardrail = b.addInstallArtifact(provisioned_dense_ingest_guardrail, .{});

    const run_provisioned_dense_ingest_guardrail = b.addRunArtifact(provisioned_dense_ingest_guardrail);
    if (b.args) |args| {
        run_provisioned_dense_ingest_guardrail.addArgs(args);
    } else {
        run_provisioned_dense_ingest_guardrail.addArgs(&.{
            "--docs",
            "50000",
            "--dims",
            "1536",
            "--batch-size",
            "100",
            "--sync-level",
            "write",
        });
    }
    const build_provisioned_dense_ingest_guardrail_step = b.step("provisioned-dense-ingest-guardrail-build", "Build the provisioned table dense ingest guardrail without running it");
    build_provisioned_dense_ingest_guardrail_step.dependOn(&provisioned_dense_ingest_guardrail.step);
    const install_provisioned_dense_ingest_guardrail_step = b.step("provisioned-dense-ingest-guardrail-install", "Build and install the provisioned table dense ingest guardrail");
    install_provisioned_dense_ingest_guardrail_step.dependOn(&install_provisioned_dense_ingest_guardrail.step);
    const provisioned_dense_ingest_guardrail_step = b.step("provisioned-dense-ingest-guardrail", "Benchmark the provisioned table write path without HTTP for VectorDBBench-shaped dense ingest");
    provisioned_dense_ingest_guardrail_step.dependOn(&run_provisioned_dense_ingest_guardrail.step);

    const public_query_guardrail_mod = b.createModule(.{
        .root_source_file = b.path("bench/storage/public_query_guardrail.zig"),
        .target = target,
        .optimize = optimize,
    });
    public_query_guardrail_mod.addImport("antfly-zig", lib_mod);

    const public_query_guardrail = b.addExecutable(.{
        .name = "public_query_guardrail",
        .root_module = public_query_guardrail_mod,
    });

    const run_public_query_guardrail = b.addRunArtifact(public_query_guardrail);
    if (b.args) |args| {
        run_public_query_guardrail.addArgs(args);
    } else {
        run_public_query_guardrail.addArgs(&.{
            "--docs",
            "5000",
            "--dims",
            "384",
            "--queries",
            "25",
            "--repeats",
            "10",
            "--k",
            "100",
            "--batch-size",
            "250",
            "--search-threads",
            "5",
            "--sync-level",
            "write",
        });
    }
    const build_public_query_guardrail_step = b.step("public-query-guardrail-build", "Build the dedicated public query guardrail without running it");
    build_public_query_guardrail_step.dependOn(&public_query_guardrail.step);
    const public_query_guardrail_step = b.step("public-query-guardrail", "Benchmark the public /db/v1/tables/<table>/query path against direct DB search and health responsiveness");
    public_query_guardrail_step.dependOn(&run_public_query_guardrail.step);
    const raft_apply_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/storage/raft_apply_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    raft_apply_bench_mod.addImport("antfly-zig", lib_mod);
    raft_apply_bench_mod.addImport("raft_engine", raft_engine_mod);

    const raft_apply_bench = b.addExecutable(.{
        .name = "raft_apply_bench",
        .root_module = raft_apply_bench_mod,
    });

    const run_raft_apply_bench = b.addRunArtifact(raft_apply_bench);
    if (b.args) |args| {
        run_raft_apply_bench.addArgs(args);
    }
    const raft_apply_bench_step = b.step("raft-apply-bench", "Benchmark committed-entry encoding and data raft apply store persistence");
    raft_apply_bench_step.dependOn(&run_raft_apply_bench.step);

    const managed_host_wal_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/storage/managed_host_wal_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    managed_host_wal_bench_mod.addImport("antfly-zig", lib_mod);
    managed_host_wal_bench_mod.addImport("raft_engine", raft_engine_mod);

    const managed_host_wal_bench = b.addExecutable(.{
        .name = "managed_host_wal_bench",
        .root_module = managed_host_wal_bench_mod,
    });

    const run_managed_host_wal_bench = b.addRunArtifact(managed_host_wal_bench);
    if (b.args) |args| {
        run_managed_host_wal_bench.addArgs(args);
    }
    const managed_host_wal_bench_step = b.step("managed-host-wal-bench", "Benchmark ManagedHost proposal persistence with WAL-backed raft state and restart");
    managed_host_wal_bench_step.dependOn(&run_managed_host_wal_bench.step);

    const dense_ingest_guardrail_step = b.step("dense-ingest-guardrail", "Run a VectorDBBench-shaped dense ingest smoke guardrail");
    dense_ingest_guardrail_step.dependOn(&run_dense_ingest_guardrail.step);

    const vector_write_guardrails_step = b.step("vector-write-guardrails", "Run local VectorDBBench-shaped vector write guardrails");
    vector_write_guardrails_step.dependOn(hbc_write_guardrail_step);
    vector_write_guardrails_step.dependOn(dense_ingest_guardrail_step);

    const dense_profile_summary = b.addExecutable(.{
        .name = "dense_profile_summary",
        .root_module = b.createModule(.{
            .root_source_file = b.path("bench/vectors/dense_profile_summary.zig"),
            .target = target,
            .optimize = .ReleaseFast,
        }),
    });

    const run_dense_profile_summary = b.addRunArtifact(dense_profile_summary);
    if (b.args) |args| {
        run_dense_profile_summary.addArgs(args);
    }
    const dense_profile_summary_step = b.step("dense-profile-summary", "Summarize dense-stack-bench profile JSONL output");
    dense_profile_summary_step.dependOn(&run_dense_profile_summary.step);

    const lmdb_commit_compare_mod = b.createModule(.{
        .root_source_file = b.path("bench/storage/lmdb_commit_compare.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    lmdb_commit_compare_mod.addImport("antfly-zig", lib_mod);

    const lmdb_commit_compare = b.addExecutable(.{
        .name = "lmdb_commit_compare",
        .root_module = lmdb_commit_compare_mod,
    });

    const run_lmdb_commit_compare = b.addRunArtifact(lmdb_commit_compare);
    if (b.args) |args| {
        run_lmdb_commit_compare.addArgs(args);
    }
    const lmdb_commit_compare_step = b.step("lmdb-commit-compare", "Benchmark LMDB commit cost in isolation");
    lmdb_commit_compare_step.dependOn(&run_lmdb_commit_compare.step);

    const hbc_split_bench_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/bench/hbc_split_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    hbc_split_bench_mod.addImport("antfly-zig", lib_mod);

    const hbc_split_bench = b.addExecutable(.{
        .name = "hbc_split_bench",
        .root_module = hbc_split_bench_mod,
    });

    const run_hbc_split_bench = b.addRunArtifact(hbc_split_bench);
    if (b.args) |args| {
        run_hbc_split_bench.addArgs(args);
    }
    const hbc_split_bench_step = b.step("hbc-split-bench", "Benchmark dense-only HBC split child rebuild");
    hbc_split_bench_step.dependOn(&run_hbc_split_bench.step);

    const sparse_split_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/vectors/sparse_split_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    sparse_split_bench_mod.addImport("antfly-zig", lib_mod);

    const sparse_split_bench = b.addExecutable(.{
        .name = "sparse_split_bench",
        .root_module = sparse_split_bench_mod,
    });

    const run_sparse_split_bench = b.addRunArtifact(sparse_split_bench);
    if (b.args) |args| {
        run_sparse_split_bench.addArgs(args);
    }
    const sparse_split_bench_step = b.step("sparse-split-bench", "Benchmark sparse-only split handoff");
    sparse_split_bench_step.dependOn(&run_sparse_split_bench.step);

    const rabitq_bench_mod = b.createModule(.{
        .root_source_file = b.path("bench/vectors/rabitq_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    rabitq_bench_mod.addImport("antfly-zig", lib_mod);
    rabitq_bench_mod.addImport("antfly_vector", vector_mod);

    const rabitq_bench = b.addExecutable(.{
        .name = "rabitq_bench",
        .root_module = rabitq_bench_mod,
    });

    const run_rabitq_bench = b.addRunArtifact(rabitq_bench);
    if (b.args) |args| {
        run_rabitq_bench.addArgs(args);
    }
    const rabitq_bench_step = b.step("rabitq-bench", "Benchmark RaBitQ primitives and estimator");
    rabitq_bench_step.dependOn(&run_rabitq_bench.step);

    const recall_harness_mod = b.createModule(.{
        .root_source_file = b.path("bench/vectors/recall_harness.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    recall_harness_mod.addImport("antfly-zig", lib_mod);

    const recall_harness = b.addExecutable(.{
        .name = "recall_harness",
        .root_module = recall_harness_mod,
    });

    const run_recall_harness = b.addRunArtifact(recall_harness);
    run_recall_harness.stdio = .inherit;
    if (b.args) |args| {
        run_recall_harness.addArgs(args);
    }
    const recall_harness_step = b.step("recall-harness", "Run Zig recall suites against exported vector datasets");
    recall_harness_step.dependOn(&run_recall_harness.step);

    // Allow full CI to compile this ReleaseFast executable alongside the
    // release-scale regressions, then reuse the cached artifact for the recall
    // phase instead of paying its compile cost on the recall critical path.
    const recall_harness_build_step = b.step("recall-harness-build", "Build the recall harness without running it");
    recall_harness_build_step.dependOn(&recall_harness.step);

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
        "recall-ci-test",
        "Run storage-backed and per-metric recall checks concurrently",
    );
    recall_ci_test_step.dependOn(&run_recall_checks.step);

    const antfly_main_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/main.zig"),
        .target = target,
        .optimize = optimize,
        .sanitize_thread = sanitize_thread,
    });
    antfly_main_mod.addImport("antfly-client", antfly_client_pkg_mod);
    antfly_main_mod.addImport("structlog", structlog_mod);
    antfly_main_mod.addImport("antfly_platform", platform_mod);
    antfly_main_mod.addOptions("build_options", production_build_options);
    addMacosSdkPaths(b, antfly_main_mod, target);

    const antfly_main = b.addExecutable(.{
        .name = "antfly",
        .root_module = antfly_main_mod,
    });

    var runtime_library_artifacts: [std.meta.fields(RuntimeLibraryUnit).len]?*std.Build.Step.Compile = @splat(null);
    inline for (std.meta.tags(RuntimeLibraryUnit)) |unit| {
        // The executable, C API, and focused artifacts reuse their owning
        // runtime units instead of recompiling implementations in each root.
        const unit_options = b.addOptions();
        unit_options.addOption(RuntimeLibraryUnit, "unit", unit);

        const role_mod = b.createModule(.{
            .root_source_file = b.path("pkg/antfly/src/runtime_artifact_lib.zig"),
            .target = target,
            .optimize = optimize,
            .sanitize_thread = sanitize_thread,
            .pic = if (unit == .distributed) true else null,
        });
        production_antfly_imports.configureRuntime(
            b,
            role_mod,
            false,
            link_libc,
            unit == .inference,
        );
        addMacosSdkPaths(b, role_mod, target);
        role_mod.addImport("antfly-client", antfly_client_pkg_mod);
        if (unit == .distributed) role_mod.addImport("antfly_storage_root", role_mod);
        role_mod.addOptions("runtime_library_options", unit_options);
        const role_usermgr_storage_mod = b.createModule(.{
            .root_source_file = b.path("pkg/antfly/src/usermgr/storage_imports.zig"),
            .target = target,
            .optimize = optimize,
        });
        role_usermgr_storage_mod.addImport("antfly_root", role_mod);
        role_usermgr_storage_mod.addImport("antfly_platform", platform_mod);
        role_mod.addImport("usermgr_storage", role_usermgr_storage_mod);

        const role_artifact = b.addLibrary(.{
            .name = if (unit == .distributed)
                "antfly-storage-kernel"
            else
                b.fmt("antfly-runtime-{s}", .{@tagName(unit)}),
            .root_module = role_mod,
            .linkage = .static,
            .max_rss = switch (unit) {
                // Claims conservatively cover clean production ReleaseFast
                // peaks measured for both aarch64-linux-musl and explicit
                // aarch64-macos (including Metal and Accelerate). They are
                // scheduling reservations, not hard process limits. A 48 GiB
                // budget can overlap all four units while a smaller cgroup
                // automatically schedules only the subset that fits.
                // aarch64-macOS ReleaseFast codegen reached 9.95 GB with
                // platform frameworks. Clean aarch64-linux-musl ReleaseFast
                // API-kernel codegen reached 8.10 GB; reserve 10 GiB so the
                // scheduler serializes competing roots instead of rejecting
                // an otherwise successful production artifact build.
                .api_kernel => @as(usize, if (target.result.os.tag == .macos) 11 else 10) * 1024 * 1024 * 1024,
                // Clean aarch64-macOS ReleaseFast storage codegen reached
                // 17.42 GB (16.23 GiB) with the platform frameworks enabled.
                // An aarch64-linux-musl cross-build on an aarch64-macOS host
                // reached 10.00 GB; reserve 12 GiB there so the scheduler does
                // not reject a successful local KinD artifact build. Native
                // Linux CI retains the tighter measured 8 GiB reservation.
                .distributed => @as(usize, if (target.result.os.tag == .macos)
                    18
                else if (builtin.os.tag == .macos)
                    12
                else
                    8) * 1024 * 1024 * 1024,
                // The broad aarch64-macOS ReleaseFast inference root now
                // reaches roughly 13.6 GB after storage/runtime integration.
                // Reserve enough headroom for mode-dependent IR; the build
                // scheduler can overlap whichever roots fit without forcing
                // callers to serialize the whole build.
                .inference => 16 * 1024 * 1024 * 1024,
                // Clean aarch64-macOS ReleaseFast codegen currently peaks
                // around 2.23 GB, just above the former 2 GiB reservation.
                .cli => 3 * 1024 * 1024 * 1024,
            },
        });
        runtime_library_artifacts[@intFromEnum(unit)] = role_artifact;
        if (unit == .distributed) {
            // The executable and C ABI libraries share this one optimized
            // PIC object. Give the final links enough section granularity
            // to retain only the C ABI roots in the shared libraries while
            // the executable retains the runtime entry points as well.
            role_artifact.link_function_sections = true;
            role_artifact.link_data_sections = true;
        }
        // Zig's build runner uses these claims to run as many LLVM codegen
        // steps concurrently as fit in available RAM. The distributed
        // archive is PIC because the executable and C ABI libraries share
        // it; both consumers therefore reuse the same analyzed and
        // optimized storage graph.
        if (unit == .distributed) {
            libantfly_link_mod.linkLibrary(role_artifact);
        }
        if (strip) {
            var visited = std.AutoHashMap(*std.Build.Module, void).init(b.allocator);
            defer visited.deinit();
            setStripRecursively(role_mod, &visited);
        }
    }

    for (runtime_library_link_order) |unit| {
        antfly_main.root_module.linkLibrary(runtime_library_artifacts[@intFromEnum(unit)].?);
    }

    if (runtime_artifact_role) |role| {
        const role_options = b.addOptions();
        role_options.addOption(RuntimeArtifactRole, "role", role);

        const role_mod = b.createModule(.{
            .root_source_file = b.path("pkg/antfly/src/runtime_artifact_main.zig"),
            .target = target,
            .optimize = optimize,
            .sanitize_thread = sanitize_thread,
        });
        role_mod.addImport("structlog", structlog_mod);
        role_mod.link_libc = link_libc;
        addMacosSdkPaths(b, role_mod, target);
        role_mod.addOptions("runtime_artifact_options", role_options);

        const role_name = @tagName(role);
        const role_exe = b.addExecutable(.{
            .name = b.fmt("antfly-{s}", .{role_name}),
            .root_module = role_mod,
        });
        role_exe.link_gc_sections = true;
        switch (role) {
            .cli => {
                role_exe.root_module.linkLibrary(runtime_library_artifacts[@intFromEnum(RuntimeLibraryUnit.cli)].?);
                role_exe.root_module.linkLibrary(runtime_library_artifacts[@intFromEnum(RuntimeLibraryUnit.distributed)].?);
            },
            .data, .metadata => {
                role_exe.root_module.linkLibrary(runtime_library_artifacts[@intFromEnum(RuntimeLibraryUnit.distributed)].?);
                role_exe.root_module.linkLibrary(runtime_library_artifacts[@intFromEnum(RuntimeLibraryUnit.api_kernel)].?);
            },
            .inference => {
                role_exe.root_module.linkLibrary(runtime_library_artifacts[@intFromEnum(RuntimeLibraryUnit.inference)].?);
            },
            .standalone => {
                role_exe.root_module.linkLibrary(runtime_library_artifacts[@intFromEnum(RuntimeLibraryUnit.distributed)].?);
                role_exe.root_module.linkLibrary(runtime_library_artifacts[@intFromEnum(RuntimeLibraryUnit.api_kernel)].?);
                role_exe.root_module.linkLibrary(runtime_library_artifacts[@intFromEnum(RuntimeLibraryUnit.inference)].?);
            },
        }
        if (strip) {
            var visited = std.AutoHashMap(*std.Build.Module, void).init(b.allocator);
            defer visited.deinit();
            setStripRecursively(role_mod, &visited);
        }
        const install_role = b.addInstallArtifact(role_exe, .{});
        const role_step = b.step("runtime-artifact", "Build and install one focused server runtime artifact");
        role_step.dependOn(&install_role.step);
    }
    if (strip) {
        var visited = std.AutoHashMap(*std.Build.Module, void).init(b.allocator);
        defer visited.deinit();
        setStripRecursively(antfly_main_mod, &visited);
        setStripRecursively(capi_mod, &visited);
    }
    const antfly_main_tests = b.addTest(.{
        .root_module = antfly_main_mod,
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_antfly_main_tests = b.addRunArtifact(antfly_main_tests);
    addRuntimeTestFilters(b, run_antfly_main_tests, selectTestFilters(b, &.{}));
    const antfly_main_test_step = b.step("antfly-main-test", "Run top-level Antfly CLI tests");
    antfly_main_test_step.dependOn(&run_antfly_main_tests.step);
    unit_test_step.dependOn(&run_antfly_main_tests.step);

    // The aggregate intentionally runs with normal CPU concurrency. Give every
    // compile step a conservative scheduler claim unless it already has a
    // measured, domain-specific claim above; CI supplies the cgroup-aware
    // aggregate budget through --maxrss.
    assignDefaultAggregateMaxRss(
        b,
        unit_test_step,
        7 * 1024 * 1024 * 1024,
        6 * 1024 * 1024 * 1024,
    );

    const install_antfly = b.addInstallArtifact(antfly_main, .{ .dest_sub_path = antfly_bin_name });
    const install_antfarm_assets = b.addInstallDirectory(.{
        .source_dir = b.path("pkg/antfly/antfarm"),
        .install_dir = .prefix,
        .install_subdir = "share/antfly/antfarm",
    });
    b.getInstallStep().dependOn(&install_antfly.step);
    b.getInstallStep().dependOn(&install_antfarm_assets.step);
    const antfly_step = b.step("antfly", "Build and install the top-level Antfly CLI");
    antfly_step.dependOn(&install_antfly.step);
    antfly_step.dependOn(&install_antfarm_assets.step);

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
    const lite_cli_smoke_step = b.step("lite-cli-smoke", "Run black-box Antfly Lite CLI smoke tests");
    lite_cli_smoke_step.dependOn(&run_lite_core_cli_smoke.step);
    lite_cli_smoke_step.dependOn(&run_lite_full_cli_smoke.step);
    const lite_core_main_tests = b.addTest(.{
        .root_module = lite_core_main_mod,
        .filters = &.{"lite core main compiles"},
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lite_core_main_tests = addFilteredTestRunArtifact(b, lite_core_main_tests);
    const lite_core_test_step = b.step("lite-core-test", "Run Antfly Lite core wrapper tests");
    lite_core_test_step.dependOn(&run_lite_core_main_tests.step);
    lite_core_test_step.dependOn(&run_lite_cmd_tests.step);
    lite_core_test_step.dependOn(&run_lite_native_tests.step);
    lite_core_test_step.dependOn(&run_capi_smoke.step);
    lite_core_test_step.dependOn(&run_lite_go_tests.step);
    lite_core_test_step.dependOn(&run_lite_go_example.step);
    lite_core_test_step.dependOn(&run_lite_go_retrieval_template.step);
    lite_core_test_step.dependOn(&run_lite_core_cli_smoke.step);
    lite_core_test_step.dependOn(&run_antfly_embedded_pkg_tests.step);
    const install_lite_core_main = b.addInstallArtifact(lite_core_main, .{ .dest_sub_path = antfly_bin_name });

    const lite_core_step = b.step("lite-core", "Build Antfly Lite core CLI, embedded package check, and libantfly C ABI");
    lite_core_step.dependOn(&install_lite_core_main.step);
    lite_core_step.dependOn(&install_libantfly.step);
    lite_core_step.dependOn(&install_capi_header.step);
    lite_core_step.dependOn(&run_lite_core_main_tests.step);
    lite_core_step.dependOn(&run_capi_smoke.step);
    lite_core_step.dependOn(&run_lite_go_tests.step);
    lite_core_step.dependOn(&run_lite_go_example.step);
    lite_core_step.dependOn(&run_lite_go_retrieval_template.step);
    lite_core_step.dependOn(&run_lite_core_cli_smoke.step);
    lite_core_step.dependOn(&run_antfly_embedded_pkg_tests.step);

    const lite_full_step = b.step("lite-full", "Build the full Antfly CLI with Lite commands, local inference runtime capability, embedded package check, and libantfly C ABI");
    if (!lite_local_inference_runtime) {
        lite_full_step.dependOn(&b.addFail("lite-full requires -Dlite-local-inference-runtime=true so Lite status and bindings advertise the local inference runtime").step);
    }
    lite_full_step.dependOn(&install_antfly.step);
    lite_full_step.dependOn(&install_libantfly.step);
    lite_full_step.dependOn(&install_capi_header.step);
    lite_full_step.dependOn(&run_antfly_main_tests.step);
    lite_full_step.dependOn(&run_lite_cmd_tests.step);
    lite_full_step.dependOn(&run_lite_native_tests.step);
    lite_full_step.dependOn(&run_capi_smoke.step);
    lite_full_step.dependOn(&run_lite_go_tests.step);
    lite_full_step.dependOn(&run_lite_go_example.step);
    lite_full_step.dependOn(&run_lite_go_retrieval_template.step);
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
    const lite_wasm_step = b.step("lite-wasm", "Build the Antfly Lite hosted/manual-maintenance WASM profile");
    lite_wasm_step.dependOn(&install_lite_wasm_profile.step);
    lite_wasm_step.dependOn(&run_lite_wasm_profile_tests.step);

    const lite_dev_step = b.step("lite-dev", "Build the Antfly Lite development profile with CLI diagnostics and C ABI checks");
    lite_dev_step.dependOn(&install_antfly.step);
    lite_dev_step.dependOn(&install_libantfly.step);
    lite_dev_step.dependOn(&install_capi_header.step);
    lite_dev_step.dependOn(&run_antfly_main_tests.step);
    lite_dev_step.dependOn(&run_lite_core_main_tests.step);
    lite_dev_step.dependOn(&run_lite_cmd_tests.step);
    lite_dev_step.dependOn(&run_lite_native_tests.step);
    lite_dev_step.dependOn(&run_capi_smoke.step);
    lite_dev_step.dependOn(&run_lite_go_tests.step);
    lite_dev_step.dependOn(&run_lite_go_example.step);
    lite_dev_step.dependOn(&run_lite_go_retrieval_template.step);
    lite_dev_step.dependOn(&run_lite_core_cli_smoke.step);
    lite_dev_step.dependOn(&run_lite_full_cli_smoke.step);
    lite_dev_step.dependOn(&install_lite_wasm_profile.step);
    lite_dev_step.dependOn(&run_lite_wasm_profile_tests.step);
    lite_dev_step.dependOn(&run_cabi_packaging_tests.step);
    lite_dev_step.dependOn(&run_capi_tests.step);
    lite_dev_step.dependOn(&run_antfly_embedded_pkg_tests.step);

    dependOnAll(antfly_test_step, &.{
        unit_test_step,
        sim_test_step,
        integration_test_step,
        recall_ci_test_step,
        chaos_test_step,
    });

    dependOnAll(test_step, &.{
        antfly_test_step,
        delegated_inference_steps.inference_test,
    });

    // `test` owns more than the Antfly unit-test subgraph. Fill in claims for
    // simulation, integration, recall, chaos, and delegated inference steps
    // too so --maxrss bounds the complete aggregate instead of only one arm.
    assignDefaultAggregateMaxRss(
        b,
        test_step,
        7 * 1024 * 1024 * 1024,
        6 * 1024 * 1024 * 1024,
    );

    const hbc_trace_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/tools/hbc_trace.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    hbc_trace_mod.addImport("antfly-zig", lib_mod);
    const recall_common_mod = b.createModule(.{
        .root_source_file = b.path("bench/vectors/recall_common.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    recall_common_mod.addImport("antfly-zig", lib_mod);
    hbc_trace_mod.addImport("recall_common", recall_common_mod);

    const hbc_trace = b.addExecutable(.{
        .name = "hbc_trace",
        .root_module = hbc_trace_mod,
    });

    const run_hbc_trace = b.addRunArtifact(hbc_trace);
    if (b.args) |args| {
        run_hbc_trace.addArgs(args);
    }
    const hbc_trace_step = b.step("hbc-trace", "Trace one Zig HBC query against an exported vector dataset");
    hbc_trace_step.dependOn(&run_hbc_trace.step);

    const hbc_leaf_debug_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/tools/hbc_leaf_debug.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    hbc_leaf_debug_mod.addImport("antfly-zig", lib_mod);
    hbc_leaf_debug_mod.addImport("recall_common", recall_common_mod);

    const hbc_leaf_debug = b.addExecutable(.{
        .name = "hbc_leaf_debug",
        .root_module = hbc_leaf_debug_mod,
    });

    const run_hbc_leaf_debug = b.addRunArtifact(hbc_leaf_debug);
    if (b.args) |args| {
        run_hbc_leaf_debug.addArgs(args);
    }
    const hbc_leaf_debug_step = b.step("hbc-leaf-debug", "Inspect cached versus fresh quantized HBC leaf scoring");
    hbc_leaf_debug_step.dependOn(&run_hbc_leaf_debug.step);
}
