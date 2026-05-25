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
const antfly_benches_build = @import("pkg/antfly/build/benches.zig");
const antfly_embedded_build = @import("pkg/antfly/build/embedded.zig");
const antfly_storage_build = @import("pkg/antfly/build/storage.zig");
const antfly_tests_build = @import("pkg/antfly/build/tests.zig");
const termite_runtime_build = @import("pkg/termite/build/runtime.zig");

const LmdbBackend = antfly_storage_build.LmdbBackend;
const chainLabeledFilteredTests = antfly_tests_build.chainLabeledFilteredTests;
const chainLabeledRun = antfly_tests_build.chainLabeledRun;
const configureEmbeddedModule = antfly_embedded_build.configureModule;
const lmdb_c_flags = antfly_storage_build.lmdb_c_flags;
const makeLmdbBuildOptions = antfly_storage_build.makeLmdbBuildOptions;
const makeLmdbEngineModule = antfly_storage_build.makeLmdbEngineModule;
const makeLmdbModule = antfly_storage_build.makeLmdbModule;
const makeRootBuildOptions = antfly_storage_build.makeRootBuildOptions;
const selectTestFilters = antfly_tests_build.selectTestFilters;

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

fn mlxRootAvailable(b: *std.Build, target: std.Build.ResolvedTarget, root: []const u8) bool {
    if (target.result.os.tag != .macos) return false;
    const header = b.fmt("{s}/include/mlx/c/mlx.h", .{root});
    const library = b.fmt("{s}/lib/libmlxc.dylib", .{root});
    return pathExists(b, header) and pathExists(b, library);
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

const termite_delegated_steps = [_][]const u8{
    "run",
    "finetune",
    "bench-paged-attention",
    "bench-training",
    "bench-linalg",
    "bench-audio",
    "bench-gliner2-native",
    "test-finetune",
    "test",
    "wasm",
};

const DelegatedPackageStep = struct {
    run: *std.Build.Step.Run,
    step: *std.Build.Step,
};

const DelegatedTermiteBuildSteps = struct {
    termite_install: *std.Build.Step,
    termite_test: *std.Build.Step,
};

fn dependOnAll(step: *std.Build.Step, dependencies: []const *std.Build.Step) void {
    for (dependencies) |dependency| {
        step.dependOn(dependency);
    }
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

fn addDelegatedTermiteOptions(
    b: *std.Build,
    run: *std.Build.Step.Run,
    enable_mlx: bool,
    mlx_root: ?[]const u8,
    enable_metal: bool,
    enable_onnx: bool,
    onnx_root: []const u8,
    enable_cuda: bool,
    cuda_artifacts: []const u8,
    enable_system_blas: bool,
    blas_root: ?[]const u8,
) void {
    run.addArg("-Dshared-lib-root=../..");
    if (enable_mlx) {
        run.addArg("-Dmlx=true");
        if (mlx_root) |root| run.addArg(b.fmt("-Dmlx-root={s}", .{root}));
    } else {
        run.addArg("-Dmlx=false");
    }
    run.addArg(if (enable_metal) "-Dmetal=true" else "-Dmetal=false");
    run.addArg(if (enable_onnx) "-Donnx=true" else "-Donnx=false");
    if (enable_onnx) {
        run.addArg(b.fmt("-Donnx-root={s}", .{onnx_root}));
    }
    run.addArg(if (enable_cuda) "-Dcuda=true" else "-Dcuda=false");
    run.addArg(b.fmt("-Dcuda-artifacts={s}", .{cuda_artifacts}));
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

fn addDelegatedTermiteBuildSteps(
    b: *std.Build,
    enable_mlx: bool,
    mlx_root: ?[]const u8,
    enable_metal: bool,
    enable_onnx: bool,
    onnx_root: []const u8,
    enable_cuda: bool,
    cuda_artifacts: []const u8,
    enable_system_blas: bool,
    blas_root: ?[]const u8,
) DelegatedTermiteBuildSteps {
    var test_step: ?*std.Build.Step = null;
    for (termite_delegated_steps) |step_name| {
        const delegated = addDelegatedPackageStep(b, "termite", "pkg/termite", step_name, "pkg/termite");
        const run = delegated.run;
        addDelegatedTermiteOptions(b, run, enable_mlx, mlx_root, enable_metal, enable_onnx, onnx_root, enable_cuda, cuda_artifacts, enable_system_blas, blas_root);
        forwardBuildArgs(b, run);
        if (std.mem.eql(u8, step_name, "test")) {
            test_step = delegated.step;
        }
    }
    const install_run = b.addSystemCommand(&.{
        b.graph.zig_exe,
        "build",
        "install",
        "--prefix",
        b.install_path,
    });
    install_run.setCwd(b.path("pkg/termite"));
    addDelegatedTermiteOptions(b, install_run, enable_mlx, mlx_root, enable_metal, enable_onnx, onnx_root, enable_cuda, cuda_artifacts, enable_system_blas, blas_root);
    forwardBuildArgs(b, install_run);
    const termite_install_step = b.step("install-termite", "Build and install the top-level Termite CLI");
    termite_install_step.dependOn(&install_run.step);
    return .{
        .termite_install = termite_install_step,
        .termite_test = test_step.?,
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

fn detectMlxRoot(b: *std.Build, target: std.Build.ResolvedTarget) ?[]const u8 {
    if (target.result.os.tag != .macos) return null;

    const candidates = [_][]const u8{
        "/opt/homebrew",
        "/opt/homebrew/opt/mlx-c",
        "/usr/local",
        "/usr/local/opt/mlx-c",
    };
    for (candidates) |root| {
        if (mlxRootAvailable(b, target, root)) return root;
    }
    return null;
}

fn defaultTermiteOnnxRoot(b: *std.Build, target: std.Build.ResolvedTarget) []const u8 {
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
    return b.fmt("pkg/termite/onnxruntime/{s}-{s}", .{ platform_str, arch_str });
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
            .root_source_file = b.path("pkg/termite/tools/patch_sentencepiece_proto.zig"),
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

const AntflyRootImports = struct {
    build_options: *std.Build.Step.Options,
    lmdb_engine: *std.Build.Module,
    raft_engine: *std.Build.Module,
    public_openapi: *std.Build.Module,
    client_openapi: *std.Build.Module,
    schema_openapi: *std.Build.Module,
    indexes_openapi: *std.Build.Module,
    ai_openapi: *std.Build.Module,
    eval_openapi: *std.Build.Module,
    query_openapi: *std.Build.Module,
    metadata_openapi: *std.Build.Module,
    usermgr_openapi: *std.Build.Module,
    logging_openapi: *std.Build.Module,
    audio_openapi: *std.Build.Module,
    middleware_openapi: *std.Build.Module,
    scraping_openapi: *std.Build.Module,
    scraping: *std.Build.Module,
    s3_openapi: *std.Build.Module,
    termite_config_openapi: *std.Build.Module,
    chunking_api_openapi: *std.Build.Module,
    chunking_openapi: *std.Build.Module,
    chunking: *std.Build.Module,
    embeddings_openapi: *std.Build.Module,
    embeddings: *std.Build.Module,
    common_openapi: *std.Build.Module,
    generating_openapi: *std.Build.Module,
    reranking_openapi: *std.Build.Module,
    transcribing: *std.Build.Module,
    synthesizing: *std.Build.Module,
    httpx: *std.Build.Module,
    objectstore: *std.Build.Module,
    bloom: *std.Build.Module,
    vector: *std.Build.Module,
    vectorindex: *std.Build.Module,
    casbin: *std.Build.Module,
    vellum: *std.Build.Module,
    regex: *std.Build.Module,
    json: *std.Build.Module,
    jsonschema: *std.Build.Module,
    mcp: *std.Build.Module,
    a2a: *std.Build.Module,
    generating: *std.Build.Module,
    reranking: *std.Build.Module,
    termite_api: *std.Build.Module,
    termite_hf_tokenizer: *std.Build.Module,
    termite_fixed_tokenizer_data: *std.Build.Module,
    termite_chunker: *std.Build.Module,
    image: *std.Build.Module,
    font: *std.Build.Module,
    pdf: *std.Build.Module,
    openai_api: *std.Build.Module,
    handlebars: *std.Build.Module,
    termite_server: *std.Build.Module,
    prometheus: *std.Build.Module,
    structlog: *std.Build.Module,
    platform: *std.Build.Module,

    const import_table = [_]struct { name: []const u8, field: []const u8 }{
        .{ .name = "lmdb_engine", .field = "lmdb_engine" },
        .{ .name = "raft_engine", .field = "raft_engine" },
        .{ .name = "antfly_public_openapi", .field = "public_openapi" },
        .{ .name = "antfly_client_openapi", .field = "client_openapi" },
        .{ .name = "antfly_schema_openapi", .field = "schema_openapi" },
        .{ .name = "antfly_indexes_openapi", .field = "indexes_openapi" },
        .{ .name = "antfly_ai_openapi", .field = "ai_openapi" },
        .{ .name = "antfly_eval_openapi", .field = "eval_openapi" },
        .{ .name = "antfly_query_openapi", .field = "query_openapi" },
        .{ .name = "antfly_metadata_openapi", .field = "metadata_openapi" },
        .{ .name = "antfly_usermgr_openapi", .field = "usermgr_openapi" },
        .{ .name = "antfly_logging_openapi", .field = "logging_openapi" },
        .{ .name = "antfly_audio_openapi", .field = "audio_openapi" },
        .{ .name = "antfly_middleware_openapi", .field = "middleware_openapi" },
        .{ .name = "antfly_scraping_openapi", .field = "scraping_openapi" },
        .{ .name = "antfly_scraping", .field = "scraping" },
        .{ .name = "antfly_s3_openapi", .field = "s3_openapi" },
        .{ .name = "antfly_termite_config_openapi", .field = "termite_config_openapi" },
        .{ .name = "antfly_chunking_api_openapi", .field = "chunking_api_openapi" },
        .{ .name = "antfly_chunking_openapi", .field = "chunking_openapi" },
        .{ .name = "antfly_chunking", .field = "chunking" },
        .{ .name = "antfly_embeddings_openapi", .field = "embeddings_openapi" },
        .{ .name = "antfly_embeddings", .field = "embeddings" },
        .{ .name = "antfly_common_openapi", .field = "common_openapi" },
        .{ .name = "antfly_generating_openapi", .field = "generating_openapi" },
        .{ .name = "antfly_reranking_openapi", .field = "reranking_openapi" },
        .{ .name = "antfly_transcribing", .field = "transcribing" },
        .{ .name = "antfly_synthesizing", .field = "synthesizing" },
        .{ .name = "httpx", .field = "httpx" },
        .{ .name = "objectstore", .field = "objectstore" },
        .{ .name = "bloom", .field = "bloom" },
        .{ .name = "antfly_vector", .field = "vector" },
        .{ .name = "antfly_vectorindex", .field = "vectorindex" },
        .{ .name = "antfly_casbin", .field = "casbin" },
        .{ .name = "antfly_vellum", .field = "vellum" },
        .{ .name = "antfly_regex", .field = "regex" },
        .{ .name = "antfly-json", .field = "json" },
        .{ .name = "antfly_jsonschema", .field = "jsonschema" },
        .{ .name = "antfly_mcp", .field = "mcp" },
        .{ .name = "antfly_a2a", .field = "a2a" },
        .{ .name = "antfly_generating", .field = "generating" },
        .{ .name = "antfly_reranking", .field = "reranking" },
        .{ .name = "termite_api", .field = "termite_api" },
        .{ .name = "termite_hf_tokenizer", .field = "termite_hf_tokenizer" },
        .{ .name = "termite_fixed_tokenizer_data", .field = "termite_fixed_tokenizer_data" },
        .{ .name = "termite_chunker", .field = "termite_chunker" },
        .{ .name = "antfly_image", .field = "image" },
        .{ .name = "antfly_font", .field = "font" },
        .{ .name = "antfly_pdf", .field = "pdf" },
        .{ .name = "openai_api", .field = "openai_api" },
        .{ .name = "handlebars", .field = "handlebars" },
        .{ .name = "termite_server", .field = "termite_server" },
        .{ .name = "prometheus", .field = "prometheus" },
        .{ .name = "structlog", .field = "structlog" },
        .{ .name = "antfly_platform", .field = "platform" },
    };

    fn configure(self: @This(), b: *std.Build, mod: *std.Build.Module, include_lmdb_c: bool, link_libc: bool) void {
        mod.addOptions("build_options", self.build_options);
        inline for (import_table) |entry| {
            mod.addImport(entry.name, @field(self, entry.field));
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
    check.addFileArg(b.path("../openapi.yaml"));
    return check;
}

fn addJoinedPublicOpenApiSpec(b: *std.Build) std.Build.LazyPath {
    const join = addScriptsPythonCommand(b, "../scripts/join_openapi.py", &.{"--joined-only"});
    return join.addOutputFileArg("openapi.public.joined.yaml");
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
            .{ "go/pkg/antfly/lib/schema/openapi.yaml", "antfly_schema_openapi" },
            .{ "go/pkg/antfly/src/store/db/indexes/openapi.yaml", "antfly_indexes_openapi" },
            .{ "go/pkg/antfly/lib/ai/openapi.yaml", "antfly_ai_openapi" },
            .{ "go/pkg/antfly/lib/ai/eval/openapi.yaml", "antfly_eval_openapi" },
            .{ "go/pkg/generating/openapi.yaml", "antfly_generating_openapi" },
            .{ "go/pkg/antfly/lib/reranking/openapi.yaml", "antfly_reranking_openapi" },
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
        addJoinedPublicOpenApiSpec(b),
        "antfly_client_openapi",
        "antfly_client_openapi",
        "types,client",
        &.{
            .{ "go/pkg/antfly/lib/schema/openapi.yaml", "antfly_schema_openapi" },
            .{ "go/pkg/antfly/src/store/db/indexes/openapi.yaml", "antfly_indexes_openapi" },
            .{ "go/pkg/antfly/lib/ai/openapi.yaml", "antfly_ai_openapi" },
            .{ "go/pkg/antfly/lib/ai/eval/openapi.yaml", "antfly_eval_openapi" },
            .{ "go/pkg/generating/openapi.yaml", "antfly_generating_openapi" },
            .{ "go/pkg/antfly/lib/reranking/openapi.yaml", "antfly_reranking_openapi" },
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
    const termite_generated_root = "pkg/termite/src/api/generated";
    const runs = [_]*std.Build.Step.Run{
        addOpenApiRegenRun(b, openapi_codegen, addJoinedPublicOpenApiSpec(b), "antfly_public_openapi", antfly_generated_root ++ "/antfly_public_openapi", "types,extractors", &.{
            .{ "go/pkg/antfly/lib/schema/openapi.yaml", "antfly_schema_openapi" },
            .{ "go/pkg/antfly/src/store/db/indexes/openapi.yaml", "antfly_indexes_openapi" },
            .{ "go/pkg/antfly/lib/ai/openapi.yaml", "antfly_ai_openapi" },
            .{ "go/pkg/antfly/lib/ai/eval/openapi.yaml", "antfly_eval_openapi" },
            .{ "go/pkg/generating/openapi.yaml", "antfly_generating_openapi" },
            .{ "go/pkg/antfly/lib/reranking/openapi.yaml", "antfly_reranking_openapi" },
            .{ "specs/openapi/antfly/query.yaml", "antfly_query_openapi" },
        }),
        addOpenApiRegenRun(b, openapi_codegen, addJoinedPublicOpenApiSpec(b), "antfly_client_openapi", antfly_generated_root ++ "/antfly_client_openapi", "types,client", &.{
            .{ "go/pkg/antfly/lib/schema/openapi.yaml", "antfly_schema_openapi" },
            .{ "go/pkg/antfly/src/store/db/indexes/openapi.yaml", "antfly_indexes_openapi" },
            .{ "go/pkg/antfly/lib/ai/openapi.yaml", "antfly_ai_openapi" },
            .{ "go/pkg/antfly/lib/ai/eval/openapi.yaml", "antfly_eval_openapi" },
            .{ "go/pkg/generating/openapi.yaml", "antfly_generating_openapi" },
            .{ "go/pkg/antfly/lib/reranking/openapi.yaml", "antfly_reranking_openapi" },
            .{ "specs/openapi/antfly/query.yaml", "antfly_query_openapi" },
        }),
        addOpenApiRegenRun(b, openapi_codegen, b.path("../go/pkg/antfly/lib/schema/openapi.yaml"), "antfly_schema_openapi", antfly_generated_root ++ "/antfly_schema_openapi", "types", &.{}),
        addOpenApiRegenRun(b, openapi_codegen, b.path("../go/pkg/antfly/src/store/db/indexes/openapi.yaml"), "antfly_indexes_openapi", antfly_generated_root ++ "/antfly_indexes_openapi", "types", &.{
            .{ "../../../../lib/embeddings/openapi.yaml", "antfly_embeddings_openapi" },
            .{ "../../../../../generating/openapi.yaml", "antfly_generating_openapi" },
            .{ "../../../../lib/chunking/openapi.yaml", "antfly_chunking_openapi" },
        }),
        addOpenApiRegenRun(b, openapi_codegen, b.path("../go/pkg/antfly/lib/websearch/openapi.yaml"), "antfly_websearch_openapi", antfly_generated_root ++ "/antfly_websearch_openapi", "types", &.{
            .{ "../../../libaf/s3/openapi.yaml", "antfly_s3_openapi" },
        }),
        addOpenApiRegenRun(b, openapi_codegen, b.path("../go/pkg/antfly/lib/ai/eval/openapi.yaml"), "antfly_eval_openapi", antfly_generated_root ++ "/antfly_eval_openapi", "types", &.{
            .{ "../../../../generating/openapi.yaml", "antfly_generating_openapi" },
        }),
        addOpenApiRegenRun(b, openapi_codegen, b.path("../specs/openapi/antfly/query.yaml"), "antfly_query_openapi", antfly_generated_root ++ "/antfly_query_openapi", "types", &.{}),
        addOpenApiRegenRun(b, openapi_codegen, b.path("../specs/openapi/antfly/usermgr.yaml"), "antfly_usermgr_openapi", antfly_generated_root ++ "/antfly_usermgr_openapi", "types,server", &.{}),
        addOpenApiRegenRun(b, openapi_codegen, b.path("../specs/openapi/antfly/metadata.yaml"), "antfly_metadata_openapi", antfly_generated_root ++ "/antfly_metadata_openapi", "types,server", &.{
            .{ "usermgr.yaml", "antfly_usermgr_openapi" },
            .{ "../../../src/store/db/indexes/openapi.yaml", "antfly_indexes_openapi" },
            .{ "../../../lib/schema/openapi.yaml", "antfly_schema_openapi" },
            .{ "../../../lib/ai/openapi.yaml", "antfly_ai_openapi" },
            .{ "../../../lib/ai/eval/openapi.yaml", "antfly_eval_openapi" },
            .{ "../../../go/pkg/generating/openapi.yaml", "antfly_generating_openapi" },
            .{ "../../../lib/reranking/openapi.yaml", "antfly_reranking_openapi" },
            .{ "query.yaml", "antfly_query_openapi" },
        }),
        addOpenApiRegenRun(b, openapi_codegen, b.path("../go/pkg/libaf/logging/openapi.yaml"), "antfly_logging_openapi", antfly_generated_root ++ "/antfly_logging_openapi", "types", &.{}),
        addOpenApiRegenRun(b, openapi_codegen, b.path("../go/pkg/antfly/lib/audio/openapi.yaml"), "antfly_audio_openapi", antfly_generated_root ++ "/antfly_audio_openapi", "types", &.{
            .{ "../../../libaf/s3/openapi.yaml", "antfly_s3_openapi" },
        }),
        addOpenApiRegenRun(b, openapi_codegen, b.path("../go/pkg/antfly/lib/middleware/openapi.yaml"), "antfly_middleware_openapi", antfly_generated_root ++ "/antfly_middleware_openapi", "types", &.{}),
        addOpenApiRegenRun(b, openapi_codegen, b.path("../go/pkg/libaf/scraping/openapi.yaml"), "antfly_scraping_openapi", antfly_generated_root ++ "/antfly_scraping_openapi", "types", &.{}),
        addOpenApiRegenRun(b, openapi_codegen, b.path("../go/pkg/libaf/s3/openapi.yaml"), "antfly_s3_openapi", antfly_generated_root ++ "/antfly_s3_openapi", "types", &.{}),
        addOpenApiRegenRun(b, openapi_codegen, b.path("../go/pkg/termite/openapi.yaml"), "antfly_termite_config_openapi", antfly_generated_root ++ "/antfly_termite_config_openapi", "types", &.{
            .{ "../libaf/chunking/openapi.yaml", "antfly_chunking_api_openapi" },
            .{ "../libaf/scraping/openapi.yaml", "antfly_scraping_openapi" },
            .{ "../libaf/s3/openapi.yaml", "antfly_s3_openapi" },
            .{ "../libaf/logging/openapi.yaml", "antfly_logging_openapi" },
        }),
        addOpenApiRegenRun(b, openapi_codegen, b.path("../go/pkg/libaf/chunking/openapi.yaml"), "antfly_chunking_api_openapi", antfly_generated_root ++ "/antfly_chunking_api_openapi", "types", &.{}),
        addOpenApiRegenRun(b, openapi_codegen, b.path("../go/pkg/antfly/lib/chunking/openapi.yaml"), "antfly_chunking_openapi", antfly_generated_root ++ "/antfly_chunking_openapi", "types", &.{
            .{ "../../../libaf/chunking/openapi.yaml", "antfly_chunking_api_openapi" },
        }),
        addOpenApiRegenRun(b, openapi_codegen, b.path("../go/pkg/antfly/lib/embeddings/openapi.yaml"), "antfly_embeddings_openapi", antfly_generated_root ++ "/antfly_embeddings_openapi", "types", &.{}),
        addOpenApiRegenRun(b, openapi_codegen, b.path("../go/pkg/antfly/src/common/openapi.yaml"), "antfly_common_openapi", antfly_generated_root ++ "/antfly_common_openapi", "types", &.{
            .{ "../../../libaf/logging/openapi.yaml", "antfly_logging_openapi" },
            .{ "../../lib/audio/openapi.yaml", "antfly_audio_openapi" },
            .{ "../../lib/middleware/openapi.yaml", "antfly_middleware_openapi" },
            .{ "../../lib/embeddings/openapi.yaml", "antfly_embeddings_openapi" },
            .{ "../../../generating/openapi.yaml", "antfly_generating_openapi" },
            .{ "../../lib/reranking/openapi.yaml", "antfly_reranking_openapi" },
            .{ "../../lib/chunking/openapi.yaml", "antfly_chunking_openapi" },
            .{ "../../../libaf/scraping/openapi.yaml", "antfly_scraping_openapi" },
            .{ "../../../libaf/s3/openapi.yaml", "antfly_s3_openapi" },
            .{ "../../../termite/openapi.yaml", "antfly_termite_config_openapi" },
        }),
        addOpenApiRegenRun(b, openapi_codegen, b.path("../go/pkg/generating/openapi.yaml"), "antfly_generating_openapi", antfly_generated_root ++ "/antfly_generating_openapi", "types", &.{}),
        addOpenApiRegenRun(b, openapi_codegen, b.path("../go/pkg/antfly/lib/reranking/openapi.yaml"), "antfly_reranking_openapi", antfly_generated_root ++ "/antfly_reranking_openapi", "types", &.{}),
        addOpenApiRegenRun(b, openapi_codegen, b.path("../go/pkg/antfly/lib/ai/openapi.yaml"), "antfly_ai_openapi", antfly_generated_root ++ "/antfly_ai_openapi", "types", &.{
            .{ "../../../generating/openapi.yaml", "antfly_generating_openapi" },
            .{ "../websearch/openapi.yaml", "antfly_websearch_openapi" },
        }),
        addOpenApiRegenRun(b, openapi_codegen, b.path("../specs/openapi/termite/api.yaml"), "termite_api", termite_generated_root ++ "/termite_api", "types,server", &.{}),
        addOpenApiRegenRun(b, openapi_codegen, b.path("specs/openai-openapi.yaml"), "openai_api", antfly_generated_root ++ "/openai_api", "types", &.{}),
    };

    const fmt = b.addSystemCommand(&.{
        b.graph.zig_exe,
        "fmt",
        antfly_generated_root,
        termite_generated_root,
    });
    for (runs) |run| {
        fmt.step.dependOn(&run.step);
    }
    regen_step.dependOn(&fmt.step);
}

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});
    const wasm_target = b.resolveTargetQuery(.{
        .cpu_arch = .wasm32,
        .os_tag = .freestanding,
        .cpu_features_add = std.Target.wasm.featureSet(&.{ .atomics, .bulk_memory, .simd128 }),
    });
    const lmdb_backend = b.option(LmdbBackend, "lmdb_backend", "Select the LMDB backend scaffold (c or zig)") orelse .zig;
    const lmdb_evented_async_io = b.option(bool, "lmdb_evented_async_io", "Use std.Io.Evented for the Zig LMDB async_io backend") orelse false;
    const with_tla = b.option(bool, "with_tla", "Enable TLA+ trace instrumentation (ndjson event logging)") orelse false;
    const link_libc = b.option(bool, "link-libc", "Link Antfly runtime modules against libc") orelse true;
    const antfly_bin_name = b.option([]const u8, "antfly-bin-name", "Installed filename for the top-level Antfly CLI") orelse "antfly";
    if (antfly_bin_name.len == 0 or std.mem.indexOfAny(u8, antfly_bin_name, "/\\") != null) {
        @panic("-Dantfly-bin-name must be a non-empty filename, not a path");
    }
    if (!link_libc and lmdb_backend == .c) {
        @panic("-Dlink-libc=false requires -Dlmdb_backend=zig");
    }
    const termite_mlx_option = b.option(bool, "mlx", "Enable MLX termite support when available");
    const termite_mlx_requested = if (link_libc)
        termite_mlx_option orelse false
    else
        false;
    const termite_mlx_root_opt = b.option([]const u8, "mlx-root", "Path to MLX C root with include/ and lib/");
    const termite_onnx_option = b.option(bool, "onnx", "Enable ONNX Runtime support for embedded Termite");
    const termite_enable_onnx = if (link_libc)
        termite_onnx_option orelse false
    else
        false;
    const termite_onnx_root_opt = b.option([]const u8, "onnx-root", "Path to ONNX Runtime root for embedded Termite");
    const termite_onnx_root = termite_onnx_root_opt orelse defaultTermiteOnnxRoot(b, target);
    const termite_enable_metal = if (link_libc)
        b.option(bool, "metal", "Enable Apple Metal kernels for embedded Termite") orelse if (target.result.os.tag == .macos) true else termite_mlx_requested
    else
        false;
    const termite_enable_mlx = termite_enable_metal and termite_mlx_requested;
    const termite_mlx_root = if (termite_enable_mlx)
        termite_mlx_root_opt orelse detectMlxRoot(b, target)
    else
        termite_mlx_root_opt;
    const termite_enable_cuda = b.option(bool, "cuda", "Enable CUDA termite support through the NVIDIA Driver API") orelse false;
    const termite_cuda_artifacts = b.option([]const u8, "cuda-artifacts", "CUDA artifact bundle: portable PTX; fatbin is not implemented yet") orelse "portable";
    if (!std.mem.eql(u8, termite_cuda_artifacts, "portable")) {
        @panic("invalid -Dcuda-artifacts (expected portable; fatbin is not implemented yet)");
    }
    const termite_blas_root_opt = b.option([]const u8, "blas-root", "Path to system BLAS root with include/ and lib/ for non-macOS native acceleration");
    const termite_system_blas_available = link_libc and (target.result.os.tag == .macos or termite_blas_root_opt != null);
    const termite_enable_system_blas = if (link_libc)
        b.option(bool, "system-blas", "Enable system BLAS acceleration for native CPU math") orelse termite_system_blas_available
    else
        false;
    const termite_blas_root = if (termite_enable_system_blas and target.result.os.tag != .macos)
        termite_blas_root_opt
    else
        null;
    const antfly_version = b.option([]const u8, "antfly-version", "Antfly version string") orelse "dev";
    if (termite_enable_mlx) {
        const root = termite_mlx_root orelse @panic("-Dmlx=true requires an MLX C install; pass -Dmlx-root=<path>");
        if (!mlxRootAvailable(b, target, root)) {
            @panic("-Dmlx=true requires an MLX C install with include/mlx/c/mlx.h and lib/libmlxc.dylib");
        }
    }
    if (termite_enable_onnx) {
        const termite_onnx_available = pathExists(b, b.fmt("{s}/include/onnxruntime_c_api.h", .{termite_onnx_root})) and
            pathExists(b, b.fmt("{s}/lib", .{termite_onnx_root}));
        if (!termite_onnx_available) {
            @panic("-Donnx=true requires an ONNX Runtime install; pass -Donnx-root=<path>");
        }
    }
    const delegated_termite_steps = addDelegatedTermiteBuildSteps(
        b,
        termite_enable_mlx,
        termite_mlx_root,
        termite_enable_metal,
        termite_enable_onnx,
        termite_onnx_root,
        termite_enable_cuda,
        termite_cuda_artifacts,
        termite_enable_system_blas,
        termite_blas_root,
    );

    const lmdb_build_options = makeLmdbBuildOptions(b, lmdb_backend, lmdb_evented_async_io, false);
    const build_options = makeRootBuildOptions(b, lmdb_backend, lmdb_evented_async_io, false, with_tla, link_libc, false, antfly_version);
    const swarm_runtime_build_options = makeRootBuildOptions(b, lmdb_backend, lmdb_evented_async_io, false, with_tla, link_libc, true, antfly_version);
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
    const openapi_root_check = addOpenApiRootCheckStep(b);
    const antfly_generated_root = "pkg/antfly/src/openapi/generated";
    const public_openapi_mod = addCommittedOpenApiModule(b, target, optimize, "antfly_public_openapi", antfly_generated_root ++ "/antfly_public_openapi");
    const client_openapi_mod = addCommittedOpenApiModuleWithHttpx(b, target, optimize, "antfly_client_openapi", antfly_generated_root ++ "/antfly_client_openapi", httpx_mod);
    const schema_openapi_mod = addCommittedOpenApiModule(b, target, optimize, "antfly_schema_openapi", antfly_generated_root ++ "/antfly_schema_openapi");
    const indexes_openapi_mod = addCommittedOpenApiModule(b, target, optimize, "antfly_indexes_openapi", antfly_generated_root ++ "/antfly_indexes_openapi");
    const websearch_openapi_mod = addCommittedOpenApiModule(b, target, optimize, "antfly_websearch_openapi", antfly_generated_root ++ "/antfly_websearch_openapi");
    const eval_openapi_mod = addCommittedOpenApiModule(b, target, optimize, "antfly_eval_openapi", antfly_generated_root ++ "/antfly_eval_openapi");
    const query_openapi_mod = addCommittedOpenApiModule(b, target, optimize, "antfly_query_openapi", antfly_generated_root ++ "/antfly_query_openapi");
    const usermgr_openapi_mod = addCommittedOpenApiModuleWithHttpx(b, target, optimize, "antfly_usermgr_openapi", antfly_generated_root ++ "/antfly_usermgr_openapi", httpx_mod);
    const metadata_openapi_mod = addCommittedOpenApiModuleWithHttpx(b, target, optimize, "antfly_metadata_openapi", antfly_generated_root ++ "/antfly_metadata_openapi", httpx_mod);
    const logging_openapi_mod = addCommittedOpenApiModule(b, target, optimize, "antfly_logging_openapi", antfly_generated_root ++ "/antfly_logging_openapi");
    const audio_openapi_mod = addCommittedOpenApiModule(b, target, optimize, "antfly_audio_openapi", antfly_generated_root ++ "/antfly_audio_openapi");
    const middleware_openapi_mod = addCommittedOpenApiModule(b, target, optimize, "antfly_middleware_openapi", antfly_generated_root ++ "/antfly_middleware_openapi");
    const scraping_openapi_mod = addCommittedOpenApiModule(b, target, optimize, "antfly_scraping_openapi", antfly_generated_root ++ "/antfly_scraping_openapi");
    const s3_openapi_mod = addCommittedOpenApiModule(b, target, optimize, "antfly_s3_openapi", antfly_generated_root ++ "/antfly_s3_openapi");
    const termite_config_openapi_mod = addCommittedOpenApiModule(b, target, optimize, "antfly_termite_config_openapi", antfly_generated_root ++ "/antfly_termite_config_openapi");
    const chunking_api_openapi_mod = addCommittedOpenApiModule(b, target, optimize, "antfly_chunking_api_openapi", antfly_generated_root ++ "/antfly_chunking_api_openapi");
    const chunking_openapi_mod = addCommittedOpenApiModule(b, target, optimize, "antfly_chunking_openapi", antfly_generated_root ++ "/antfly_chunking_openapi");
    const embeddings_openapi_mod = addCommittedOpenApiModule(b, target, optimize, "antfly_embeddings_openapi", antfly_generated_root ++ "/antfly_embeddings_openapi");
    const common_openapi_mod = addCommittedOpenApiModule(b, target, optimize, "antfly_common_openapi", antfly_generated_root ++ "/antfly_common_openapi");
    const generating_openapi_mod = addCommittedOpenApiModule(b, target, optimize, "antfly_generating_openapi", antfly_generated_root ++ "/antfly_generating_openapi");
    const reranking_openapi_mod = addCommittedOpenApiModule(b, target, optimize, "antfly_reranking_openapi", antfly_generated_root ++ "/antfly_reranking_openapi");
    const ai_openapi_mod = addCommittedOpenApiModule(b, target, optimize, "antfly_ai_openapi", antfly_generated_root ++ "/antfly_ai_openapi");
    indexes_openapi_mod.addImport("antfly_embeddings_openapi", embeddings_openapi_mod);
    indexes_openapi_mod.addImport("antfly_generating_openapi", generating_openapi_mod);
    indexes_openapi_mod.addImport("antfly_chunking_openapi", chunking_openapi_mod);
    websearch_openapi_mod.addImport("antfly_s3_openapi", s3_openapi_mod);
    eval_openapi_mod.addImport("antfly_generating_openapi", generating_openapi_mod);
    ai_openapi_mod.addImport("antfly_generating_openapi", generating_openapi_mod);
    ai_openapi_mod.addImport("antfly_websearch_openapi", websearch_openapi_mod);
    public_openapi_mod.addImport("antfly_schema_openapi", schema_openapi_mod);
    public_openapi_mod.addImport("antfly_indexes_openapi", indexes_openapi_mod);
    public_openapi_mod.addImport("antfly_ai_openapi", ai_openapi_mod);
    public_openapi_mod.addImport("antfly_eval_openapi", eval_openapi_mod);
    public_openapi_mod.addImport("antfly_generating_openapi", generating_openapi_mod);
    public_openapi_mod.addImport("antfly_reranking_openapi", reranking_openapi_mod);
    public_openapi_mod.addImport("antfly_query_openapi", query_openapi_mod);
    client_openapi_mod.addImport("antfly_schema_openapi", schema_openapi_mod);
    client_openapi_mod.addImport("antfly_indexes_openapi", indexes_openapi_mod);
    client_openapi_mod.addImport("antfly_ai_openapi", ai_openapi_mod);
    client_openapi_mod.addImport("antfly_eval_openapi", eval_openapi_mod);
    client_openapi_mod.addImport("antfly_generating_openapi", generating_openapi_mod);
    client_openapi_mod.addImport("antfly_reranking_openapi", reranking_openapi_mod);
    client_openapi_mod.addImport("antfly_query_openapi", query_openapi_mod);
    metadata_openapi_mod.addImport("antfly_usermgr_openapi", usermgr_openapi_mod);
    metadata_openapi_mod.addImport("antfly_indexes_openapi", indexes_openapi_mod);
    metadata_openapi_mod.addImport("antfly_schema_openapi", schema_openapi_mod);
    metadata_openapi_mod.addImport("antfly_ai_openapi", ai_openapi_mod);
    metadata_openapi_mod.addImport("antfly_eval_openapi", eval_openapi_mod);
    metadata_openapi_mod.addImport("antfly_generating_openapi", generating_openapi_mod);
    metadata_openapi_mod.addImport("antfly_reranking_openapi", reranking_openapi_mod);
    metadata_openapi_mod.addImport("antfly_query_openapi", query_openapi_mod);
    chunking_openapi_mod.addImport("antfly_chunking_api_openapi", chunking_api_openapi_mod);
    audio_openapi_mod.addImport("antfly_s3_openapi", s3_openapi_mod);
    termite_config_openapi_mod.addImport("antfly_chunking_api_openapi", chunking_api_openapi_mod);
    termite_config_openapi_mod.addImport("antfly_scraping_openapi", scraping_openapi_mod);
    termite_config_openapi_mod.addImport("antfly_s3_openapi", s3_openapi_mod);
    termite_config_openapi_mod.addImport("antfly_logging_openapi", logging_openapi_mod);
    common_openapi_mod.addImport("antfly_logging_openapi", logging_openapi_mod);
    common_openapi_mod.addImport("antfly_audio_openapi", audio_openapi_mod);
    common_openapi_mod.addImport("antfly_middleware_openapi", middleware_openapi_mod);
    common_openapi_mod.addImport("antfly_embeddings_openapi", embeddings_openapi_mod);
    common_openapi_mod.addImport("antfly_generating_openapi", generating_openapi_mod);
    common_openapi_mod.addImport("antfly_reranking_openapi", reranking_openapi_mod);
    common_openapi_mod.addImport("antfly_chunking_openapi", chunking_openapi_mod);
    common_openapi_mod.addImport("antfly_scraping_openapi", scraping_openapi_mod);
    common_openapi_mod.addImport("antfly_s3_openapi", s3_openapi_mod);
    common_openapi_mod.addImport("antfly_termite_config_openapi", termite_config_openapi_mod);

    // Handlebars template engine
    const handlebars_dep = b.dependency("handlebars", .{});
    const handlebars_mod = handlebars_dep.module("handlebars");

    // Protobuf wire format
    const protobuf_dep = b.dependency("protobuf", .{});
    const protobuf_mod = protobuf_dep.module("protobuf");
    const platform_mod = b.createModule(.{
        .root_source_file = b.path("lib/platform/src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    const wasm_platform_mod = b.createModule(.{
        .root_source_file = b.path("lib/platform/src/root.zig"),
        .target = wasm_target,
        .optimize = optimize,
    });
    const objectstore_mod = b.createModule(.{
        .root_source_file = b.path("lib/objectstore/src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    objectstore_mod.addImport("httpx", httpx_mod);
    objectstore_mod.addImport("antfly_platform", platform_mod);
    const wasm_objectstore_mod = b.createModule(.{
        .root_source_file = b.path("lib/objectstore/src/root.zig"),
        .target = wasm_target,
        .optimize = optimize,
    });
    wasm_objectstore_mod.addImport("httpx", httpx_mod);
    wasm_objectstore_mod.addImport("antfly_platform", wasm_platform_mod);
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
    const a2a_mod = b.addModule("antfly_a2a", .{
        .root_source_file = b.path("lib/a2a/src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
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
    const reranking_mod = b.createModule(.{
        .root_source_file = b.path("lib/reranking/src/mod.zig"),
        .target = target,
        .optimize = optimize,
    });
    reranking_mod.addImport("antfly-json", json_mod);
    reranking_mod.addImport("antfly_reranking_openapi", reranking_openapi_mod);

    // Inference dependencies
    const openai_api_mod = addCommittedOpenApiModuleWithHttpx(b, target, optimize, "openai_api", antfly_generated_root ++ "/openai_api", httpx_mod);

    // --- Termite backend detection (must precede module creation) ---
    const termite_ffmpeg_paths = if (link_libc) detectFfmpegPaths(b, target) else null;
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
    const termite_jinja_mod = b.createModule(.{
        .root_source_file = b.path("lib/jinja/src/jinja.zig"),
        .target = target,
        .optimize = optimize,
    });
    const termite_ml_mod = b.createModule(.{
        .root_source_file = b.path("lib/ml/src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    const termite_onnx_graph_mod = b.addModule("termite_onnx_graph", .{
        .root_source_file = b.path("lib/onnx/src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    termite_onnx_graph_mod.addImport("protobuf", protobuf_mod);
    termite_onnx_graph_mod.addImport("ml", termite_ml_mod);
    const termite_pjrt_xla_proto_mod = b.createModule(.{
        .root_source_file = b.path("lib/pjrt/proto/xla_proto_stub.zig"),
        .target = target,
        .optimize = optimize,
    });
    termite_pjrt_xla_proto_mod.addImport("protobuf", protobuf_mod);
    const termite_pjrt_mod = b.createModule(.{
        .root_source_file = b.path("lib/pjrt/src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    termite_pjrt_mod.addImport("protobuf", protobuf_mod);
    termite_pjrt_mod.addImport("xla_proto", termite_pjrt_xla_proto_mod);

    const termite_graph = termite_runtime_build.create(.{
        .b = b,
        .target = target,
        .optimize = optimize,
        .paths = .{
            .termite_root = "pkg/termite",
            .shared_lib_root = "",
        },
        .backend = .{
            .enable_onnx = termite_enable_onnx,
            .onnx_root = termite_onnx_root,
            .enable_mlx = termite_enable_mlx,
            .mlx_root = termite_mlx_root,
            .enable_metal = termite_enable_metal,
            .enable_cuda = termite_enable_cuda,
            .cuda_artifacts = termite_cuda_artifacts,
            .enable_pjrt = false,
            .enable_native = true,
            .enable_system_blas = termite_enable_system_blas,
            .blas_root = termite_blas_root,
            .enable_ffmpeg_audio = termite_ffmpeg_paths != null,
            .ffmpeg_paths = if (termite_ffmpeg_paths) |paths| .{
                .include_dir = paths.include_dir,
                .lib_dir = paths.lib_dir,
            } else null,
            .link_libc = link_libc,
            .skip_openapi = false,
            .termite_version = "0.1.0",
            .git_commit = "embedded",
            .build_time = "embedded",
            .go_version = "n/a",
            .allow_downloads = true,
        },
        .shared = .{
            .json = json_mod,
            .httpx = httpx_mod,
            .platform = platform_mod,
            .vellum = vellum_mod,
            .scraping = scraping_mod,
            .objectstore = objectstore_mod,
            .regex = regex_mod,
            .jsonschema = jsonschema_mod,
            .image = image_mod,
            .prometheus = prometheus_mod,
            .structlog = structlog_mod,
            .jinja = termite_jinja_mod,
            .protobuf = protobuf_mod,
            .sentencepiece_proto = sentencepiece_proto_mod,
            .ml = termite_ml_mod,
            .onnx_graph = termite_onnx_graph_mod,
            .pjrt = termite_pjrt_mod,
        },
    });
    const termite_build_options_mod = termite_graph.build_options_mod;
    const termite_api_mod = termite_graph.termite_api_mod;
    const termite_hf_tokenizer_mod = termite_graph.termite_hf_tokenizer_mod;
    const termite_fixed_tokenizer_data_mod = termite_graph.termite_fixed_tokenizer_data_mod;
    const termite_chunker_mod = termite_graph.termite_chunker_mod;
    const termite_server_mod = termite_graph.termite_mod;

    const transcribing_mod = b.createModule(.{
        .root_source_file = b.path("lib/transcribing/src/mod.zig"),
        .target = target,
        .optimize = optimize,
    });
    transcribing_mod.addImport("antfly_audio_openapi", audio_openapi_mod);
    transcribing_mod.addImport("httpx", httpx_mod);
    transcribing_mod.addImport("termite_api", termite_api_mod);
    transcribing_mod.addImport("antfly_scraping", scraping_mod);
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
        .ai_openapi = ai_openapi_mod,
        .eval_openapi = eval_openapi_mod,
        .query_openapi = query_openapi_mod,
        .metadata_openapi = metadata_openapi_mod,
        .usermgr_openapi = usermgr_openapi_mod,
        .logging_openapi = logging_openapi_mod,
        .audio_openapi = audio_openapi_mod,
        .middleware_openapi = middleware_openapi_mod,
        .scraping_openapi = scraping_openapi_mod,
        .scraping = scraping_mod,
        .s3_openapi = s3_openapi_mod,
        .termite_config_openapi = termite_config_openapi_mod,
        .chunking_api_openapi = chunking_api_openapi_mod,
        .chunking_openapi = chunking_openapi_mod,
        .chunking = chunking_mod,
        .embeddings_openapi = embeddings_openapi_mod,
        .embeddings = embeddings_mod,
        .common_openapi = common_openapi_mod,
        .generating_openapi = generating_openapi_mod,
        .reranking_openapi = reranking_openapi_mod,
        .transcribing = transcribing_mod,
        .synthesizing = synthesizing_mod,
        .httpx = httpx_mod,
        .objectstore = objectstore_mod,
        .bloom = bloom_mod,
        .vector = vector_mod,
        .vectorindex = vectorindex_mod,
        .casbin = casbin_mod,
        .vellum = vellum_mod,
        .regex = regex_mod,
        .json = json_mod,
        .jsonschema = jsonschema_mod,
        .mcp = mcp_mod,
        .a2a = a2a_mod,
        .generating = generating_mod,
        .reranking = reranking_mod,
        .termite_api = termite_api_mod,
        .termite_hf_tokenizer = termite_hf_tokenizer_mod,
        .termite_fixed_tokenizer_data = termite_fixed_tokenizer_data_mod,
        .termite_chunker = termite_chunker_mod,
        .image = image_mod,
        .font = font_mod,
        .pdf = pdf_mod,
        .openai_api = openai_api_mod,
        .handlebars = handlebars_mod,
        .termite_server = termite_server_mod,
        .prometheus = prometheus_mod,
        .structlog = structlog_mod,
        .platform = platform_mod,
    };

    // Library module
    const lib_mod = b.addModule("antfly-zig", .{
        .root_source_file = b.path("pkg/antfly/src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    antfly_imports.configure(b, lib_mod, false, link_libc);

    const lib_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    antfly_imports.configure(b, lib_test_mod, true, true);

    const data_runtime_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/data_runtime_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    antfly_imports.configure(b, data_runtime_test_mod, true, true);

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

    const embedded_deps = .{
        build_options,
        lmdb_engine_mod,
        json_mod,
        public_openapi_mod,
        query_openapi_mod,
        indexes_openapi_mod,
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

    // --- Termite WASM modules for unified antfly.wasm ---
    const termite_wasm_build_options = b.addOptions();
    termite_wasm_build_options.addOption(bool, "enable_onnx", false);
    termite_wasm_build_options.addOption(bool, "enable_mlx", false);
    termite_wasm_build_options.addOption(bool, "enable_pjrt", false);
    termite_wasm_build_options.addOption(bool, "enable_cuda", false);
    termite_wasm_build_options.addOption([]const u8, "cuda_artifacts", "portable");
    termite_wasm_build_options.addOption(bool, "enable_metal", false);
    termite_wasm_build_options.addOption(bool, "enable_native", false);
    termite_wasm_build_options.addOption(bool, "enable_system_blas", false);
    termite_wasm_build_options.addOption(bool, "enable_wasm", true);
    termite_wasm_build_options.addOption(bool, "enable_webgpu", true);
    termite_wasm_build_options.addOption(bool, "enable_ffmpeg_audio", false);
    termite_wasm_build_options.addOption(bool, "link_libc", false);
    termite_wasm_build_options.addOption(bool, "skip_openapi", false);
    termite_wasm_build_options.addOption([]const u8, "termite_version", "0.1.0");
    termite_wasm_build_options.addOption([]const u8, "git_commit", "embedded");
    termite_wasm_build_options.addOption([]const u8, "build_time", "embedded");
    termite_wasm_build_options.addOption([]const u8, "go_version", "n/a");
    termite_wasm_build_options.addOption(bool, "allow_downloads", false);
    termite_wasm_build_options.addOption([]const u8, "wasm_memory_model", "wasm32");
    const termite_wasm_build_options_mod = termite_wasm_build_options.createModule();

    const wasm_termite_jinja_mod = b.createModule(.{
        .root_source_file = b.path("lib/jinja/src/jinja.zig"),
        .target = wasm_target,
        .optimize = .ReleaseSafe,
        .single_threaded = true,
    });
    const wasm_termite_tokenizer_mod = b.createModule(.{
        .root_source_file = b.path("lib/tokenizer/src/tokenizer.zig"),
        .target = wasm_target,
        .optimize = .ReleaseSafe,
    });
    wasm_termite_tokenizer_mod.addImport("sentencepiece_proto", sentencepiece_proto_mod);
    const wasm_termite_hf_tokenizer_mod = b.createModule(.{
        .root_source_file = b.path("lib/tokenizer/src/hf_root.zig"),
        .target = wasm_target,
        .optimize = .ReleaseSafe,
    });
    wasm_termite_hf_tokenizer_mod.addImport("termite_tokenizer", wasm_termite_tokenizer_mod);
    const wasm_termite_linalg_mod = b.createModule(.{
        .root_source_file = b.path("lib/linalg/src/mod.zig"),
        .target = wasm_target,
        .optimize = .ReleaseSafe,
    });
    const wasm_termite_ml_mod = b.createModule(.{
        .root_source_file = b.path("lib/ml/src/root.zig"),
        .target = wasm_target,
        .optimize = .ReleaseSafe,
        .single_threaded = true,
    });
    const wasm_termite_audio_mod = b.createModule(.{
        .root_source_file = b.path("lib/audio/src/mod.zig"),
        .target = wasm_target,
        .optimize = .ReleaseSafe,
    });
    const termite_wasm_inference_mod = b.createModule(.{
        .root_source_file = b.path("pkg/termite/src/wasm_entry.zig"),
        .target = wasm_target,
        .optimize = .ReleaseSafe,
    });
    termite_wasm_inference_mod.addImport("build_options", termite_wasm_build_options_mod);
    termite_wasm_inference_mod.addImport("termite_audio", wasm_termite_audio_mod);
    termite_wasm_inference_mod.addImport("termite_linalg", wasm_termite_linalg_mod);
    termite_wasm_inference_mod.addImport("termite_tokenizer", wasm_termite_tokenizer_mod);
    termite_wasm_inference_mod.addImport("termite_hf_tokenizer", wasm_termite_hf_tokenizer_mod);
    termite_wasm_inference_mod.addImport("antfly_image", wasm_image_mod);
    termite_wasm_inference_mod.addImport("jinja", wasm_termite_jinja_mod);
    termite_wasm_inference_mod.addImport("ml", wasm_termite_ml_mod);

    const antfly_wasm_mod = b.createModule(.{
        .root_source_file = b.path("examples/antfly_wasm.zig"),
        .target = wasm_target,
        .optimize = .ReleaseSafe,
    });
    antfly_wasm_mod.addImport("antfly_embedded_db", antfly_embedded_db_pkg_wasm_mod);
    antfly_wasm_mod.addImport("antfly_embedded_api", antfly_embedded_api_pkg_wasm_mod);
    antfly_wasm_mod.addImport("termite_inference", termite_wasm_inference_mod);

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

    const install_wasm_step = b.step("install-wasm", "Build and install the unified antfly wasm target (antfly-embedded + termite inference)");
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
    b.installArtifact(lib);

    const capi_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/capi/db.zig"),
        .target = target,
        .optimize = optimize,
    });
    capi_mod.addImport("antfly-zig", lib_mod);
    capi_mod.addImport("structlog", structlog_mod);

    const capi_lib = b.addLibrary(.{
        .linkage = .dynamic,
        .name = "antfly_zig_capi",
        .root_module = capi_mod,
    });
    const install_capi_lib = b.addInstallArtifact(capi_lib, .{});

    const capi_step = b.step("capi", "Build the Zig C API shared library");
    capi_step.dependOn(&install_capi_lib.step);

    const capi_default_filters = [_][]const u8{
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
    const run_capi_tests = b.addRunArtifact(capi_tests);
    const capi_test_step = b.step("capi-test", "Run C API tests");
    capi_test_step.dependOn(&run_capi_tests.step);

    // Tests
    const lib_regex_tests = b.addTest(.{
        .root_module = regex_mod,
    });
    const run_lib_regex_tests = b.addRunArtifact(lib_regex_tests);
    const lib_regex_test_step = b.step("lib-regex-test", "Run standalone lib/regex tests");
    lib_regex_test_step.dependOn(&run_lib_regex_tests.step);

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
    });
    const run_httpx_tests = b.addRunArtifact(httpx_tests);
    const lib_httpx_test_step = b.step("lib-httpx-test", "Run standalone lib/httpx tests");
    lib_httpx_test_step.dependOn(&run_httpx_tests.step);

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

    const lib_chunking_tests = b.addTest(.{
        .root_module = chunking_mod,
    });
    const run_lib_chunking_tests = b.addRunArtifact(lib_chunking_tests);
    const lib_chunking_test_step = b.step("lib-chunking-test", "Run standalone lib/chunking tests");
    lib_chunking_test_step.dependOn(&run_lib_chunking_tests.step);

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
    b.installArtifact(lib_image_bench);
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
    b.installArtifact(lib_pdf_bench);
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

    const lib_image_conformance_test_mod = b.createModule(.{
        .root_source_file = b.path("lib/image/src/mod.zig"),
        .target = target,
        .optimize = optimize,
    });
    const lib_image_conformance_tests = b.addTest(.{
        .root_module = lib_image_conformance_test_mod,
        .filters = &.{"conformance corpus"},
    });
    const run_lib_image_conformance_tests = b.addRunArtifact(lib_image_conformance_tests);
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

    const run_lib_image_conformance_tests_after_fetch = b.addRunArtifact(lib_image_conformance_tests);
    run_lib_image_conformance_tests_after_fetch.step.dependOn(&fetch_lib_image_conformance_fixtures.step);
    const lib_image_conformance_step = b.step("lib-image-conformance", "Fetch and run lib/image conformance suites");
    lib_image_conformance_step.dependOn(&run_lib_image_conformance_tests_after_fetch.step);
    lib_image_conformance_step.dependOn(&run_lib_image_corpus_verify_jpeg.step);
    lib_image_conformance_step.dependOn(&run_lib_image_corpus_verify_png.step);
    lib_image_conformance_step.dependOn(&run_lib_image_corpus_verify_png_spng.step);
    lib_image_conformance_step.dependOn(&run_lib_image_corpus_verify_gif.step);

    const run_lib_image_conformance_tests_after_fetch_quiet = b.addRunArtifact(lib_image_conformance_tests);
    run_lib_image_conformance_tests_after_fetch_quiet.step.dependOn(fetch_lib_image_conformance_fixtures_quiet_step);

    const lib_generating_runtime_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = &.{"generating backend factory executes fallback chain across providers"},
    });
    const run_lib_generating_runtime_tests = b.addRunArtifact(lib_generating_runtime_tests);
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
    const run_lib_reranking_runtime_tests = b.addRunArtifact(lib_reranking_runtime_tests);
    const lib_reranking_runtime_test_step = b.step("lib-reranking-runtime-test", "Run reranking backend adapter tests");
    lib_reranking_runtime_test_step.dependOn(&run_lib_reranking_runtime_tests.step);

    const lib_common_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = &.{"provider registry"},
    });
    const run_lib_common_tests = b.addRunArtifact(lib_common_tests);
    const lib_common_test_step = b.step("lib-common-test", "Run common/provider registry tests");
    lib_common_test_step.dependOn(&run_lib_common_tests.step);

    const lib_common_config_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = &.{"common config"},
    });
    const run_lib_common_config_tests = b.addRunArtifact(lib_common_config_tests);
    const lib_common_config_test_step = b.step("lib-common-config-test", "Run common/config tests");
    lib_common_config_test_step.dependOn(&run_lib_common_config_tests.step);

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
    const run_embedded_tests = b.addRunArtifact(embedded_tests);
    const embedded_test_step = b.step("embedded-test", "Run embedded API tests");
    embedded_test_step.dependOn(&run_embedded_tests.step);

    const antfly_embedded_pkg_tests = b.addTest(.{
        .root_module = antfly_embedded_pkg_mod,
        .filters = &.{"pkg antfly embedded root"},
    });
    const run_antfly_embedded_pkg_tests = b.addRunArtifact(antfly_embedded_pkg_tests);
    const antfly_embedded_pkg_test_step = b.step("antfly-embedded-test", "Run the standalone antfly-embedded package compile test");
    antfly_embedded_pkg_test_step.dependOn(&run_antfly_embedded_pkg_tests.step);

    const antfly_client_pkg_tests = b.addTest(.{
        .root_module = antfly_client_pkg_mod,
        .filters = &.{"antfly client pkg compiles"},
    });
    const run_antfly_client_pkg_tests = b.addRunArtifact(antfly_client_pkg_tests);
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
        "HBC recall",
    };
    const unit_progress_skip_filters = root_test_skip_filters ++ [_][]const u8{
        "lsm backend compaction chaos campaign",
    };
    const lib_unit_default_filters = [_][]const u8{
        ".test_0",
        "module compiles",
        "provisioned read cache keeps leased entry cleanup reachable when retirement bookkeeping allocation fails",
        "write cache keeps leased entry cleanup reachable when retirement bookkeeping allocation fails",
    };
    const lib_unit_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = selectTestFilters(b, &lib_unit_default_filters),
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lib_unit_tests = b.addRunArtifact(lib_unit_tests);
    for (root_test_skip_filters) |filter| {
        run_lib_unit_tests.addArgs(&.{ "--skip-test-filter", filter });
    }
    const root_test_step = b.step("root-test", "Run fast root-module compile smoke tests");
    root_test_step.dependOn(&run_lib_unit_tests.step);

    const lib_recall_default_filters = [_][]const u8{
        "HBC recall",
    };
    const lib_recall_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = selectTestFilters(b, &lib_recall_default_filters),
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lib_recall_tests = b.addRunArtifact(lib_recall_tests);
    const recall_test_step = b.step("recall-test", "Run HBC vector recall quality tests");
    recall_test_step.dependOn(&run_lib_recall_tests.step);

    const raft_unit_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = selectTestFilters(b, &.{"raft."}),
    });
    const run_raft_unit_tests = b.addRunArtifact(raft_unit_tests);

    const raft_transport_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = &.{"raft.transport."},
    });
    const run_raft_transport_tests = b.addRunArtifact(raft_transport_tests);

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
        .root_module = lib_test_mod,
        .filters = &lib_raft_sim_default_filters,
    });
    const run_lib_raft_sim_tests = b.addRunArtifact(lib_raft_sim_tests);
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
    const lib_raft_chaos_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = &lib_raft_chaos_default_filters,
    });
    const run_lib_raft_chaos_tests = b.addRunArtifact(lib_raft_chaos_tests);
    const lib_raft_chaos_test_step = b.step("lib-raft-chaos-test", "Run longer raft restart/HTTP simulation campaigns");
    lib_raft_chaos_test_step.dependOn(&run_lib_raft_chaos_tests.step);

    const lib_lsm_backend_sim_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = &.{"lsm backend simulation"},
    });
    const run_lib_lsm_backend_sim_tests = b.addRunArtifact(lib_lsm_backend_sim_tests);
    const lib_lsm_backend_sim_test_step = b.step("lib-lsm-backend-sim-test", "Run LSM backend storage workload simulation tests");
    lib_lsm_backend_sim_test_step.dependOn(&run_lib_lsm_backend_sim_tests.step);

    const lib_lsm_backend_chaos_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = &.{"lsm backend compaction chaos campaign"},
    });
    const run_lib_lsm_backend_chaos_tests = b.addRunArtifact(lib_lsm_backend_chaos_tests);
    const lib_lsm_backend_chaos_test_step = b.step("lib-lsm-backend-chaos-test", "Run longer LSM backend compaction chaos campaigns");
    lib_lsm_backend_chaos_test_step.dependOn(&run_lib_lsm_backend_chaos_tests.step);

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
        run_image_jpeg_seed_corpora_e2e_after_fetch_quiet_step,
    });

    const unit_test_step = b.step("unit-test", "Run hermetic unit and focused integration test buckets without metadata chaos simulations");
    const unit_test_progress_step = b.step("unit-test-progress", "Run labeled major unit test suites to expose slow or stuck phases");

    const lib_db_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = selectTestFilters(b, &.{"storage.db.db.test."}),
    });
    const run_lib_db_tests = b.addRunArtifact(lib_db_tests);
    const lib_db_test_step = b.step("lib-db-test", "Run root-module DB tests only");
    lib_db_test_step.dependOn(&run_lib_db_tests.step);

    const serverless_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = &.{"serverless"},
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_serverless_tests = b.addRunArtifact(serverless_tests);
    const serverless_test_step = b.step("serverless-test", "Run serverless and serverless transport tests");
    serverless_test_step.dependOn(&run_serverless_tests.step);

    const lib_data_runtime_default_filters = [_][]const u8{
        "data runtime status refresh publishes synthetic missing status for absent local group db",
        "data runtime status refresh budget reuses cached group status instead of opening db",
        "data runtime status refresh reuses managed writer snapshot instead of reopening table db",
        "data runtime runRound does not refresh provisioned replica root inline while worker is active",
        "data runtime data changes mark provisioned startup catch-up dirty",
        "data runtime structural changes preserve writer-published runtime status",
        "data runtime startup catch-up prefers cached admin snapshot",
        "data runtime provisioned root refresh spawn failure preserves retry bookkeeping",
        "data runtime local split fallback preserves source identity namespace",
        "data runtime local merge fallback derives receiver identity namespace from catalog",
        "data server can register a store without enabling data raft",
        "data server registered data raft uses wal state backend by default",
    };
    const lib_data_runtime_tests = b.addTest(.{
        .root_module = data_runtime_test_mod,
        .filters = selectTestFilters(b, &lib_data_runtime_default_filters),
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lib_data_runtime_tests = b.addRunArtifact(lib_data_runtime_tests);
    const lib_data_runtime_test_step = b.step("lib-data-runtime-test", "Run focused data runtime tests");
    lib_data_runtime_test_step.dependOn(&run_lib_data_runtime_tests.step);

    const lib_data_storage_default_filters = [_][]const u8{
        "db split sync coordinator allocates destination identity namespace",
        "db split status rejects stale destination identity namespace",
        "db merge coordinator opt-in applies configured receiver identity namespace",
        "db merge coordinator reapplies target namespace for persisted reassignment opt-in",
        "db merge coordinator rollback reapplies target namespace for persisted reassignment opt-in",
    };
    const lib_data_storage_tests = b.addTest(.{
        .root_module = data_storage_test_mod,
        .filters = selectTestFilters(b, &lib_data_storage_default_filters),
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lib_data_storage_tests = b.addRunArtifact(lib_data_storage_tests);
    const lib_data_storage_test_step = b.step("lib-data-storage-test", "Run focused data storage tests");
    lib_data_storage_test_step.dependOn(&run_lib_data_storage_tests.step);

    const lib_db_enrichment_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = &.{
            "storage.db.db.test.db batch plans generated enrichment",
            "storage.db.db.test.db computeEnrichments",
            "storage.db.db.test.db leased enrichment",
            "storage.db.db.test.db shared embedding enrichment",
            "storage.db.db.test.db dense index can reference existing",
            "storage.db.db.test.db persists shorthand chunk enrichment",
            "storage.db.db.test.db listEnrichments",
            "storage.db.db.test.db dense parent paging",
            "storage.db.db.test.db batch persists per-index applied sequence",
            "storage.db.db.test.db batch truncates derived log",
            "storage.db.db.test.db io_threaded executor processes indexed writes",
            "storage.db.db.test.db reopen replays pending derived embeddings",
            "storage.db.db.test.db replay respects per-index applied watermarks",
            "storage.db.db.test.db replay applies dense embeddings from artifact payloads",
            "storage.db.db.test.db split cutover",
            "storage.db.db.test.db merge-style cutover",
        },
    });
    const run_lib_db_enrichment_tests = b.addRunArtifact(lib_db_enrichment_tests);
    const lib_db_enrichment_step = b.step("lib-db-enrichment-test", "Run root-module DB enrichment/replay/cutover tests");
    lib_db_enrichment_step.dependOn(&run_lib_db_enrichment_tests.step);

    const lib_db_enrichment_worker_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = &.{
            "storage.db.db.test.db batch plans generated enrichment",
            "storage.db.db.test.db computeEnrichments",
            "storage.db.db.test.db leased enrichment worker",
            "storage.db.db.test.db shared embedding enrichment",
            "storage.db.db.test.db dense index can reference existing",
            "storage.db.db.test.db persists shorthand chunk enrichment",
            "storage.db.db.test.db listEnrichments",
            "storage.db.db.test.db dense parent paging",
        },
    });
    const run_lib_db_enrichment_worker_tests = b.addRunArtifact(lib_db_enrichment_worker_tests);
    const lib_db_enrichment_worker_step = b.step("lib-db-enrichment-worker-test", "Run root-module DB enrichment worker tests");
    lib_db_enrichment_worker_step.dependOn(&run_lib_db_enrichment_worker_tests.step);

    const lib_db_enrichment_replay_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = &.{
            "storage.db.db.test.db batch persists per-index applied sequence",
            "storage.db.db.test.db batch truncates derived log",
            "storage.db.db.test.db io_threaded executor processes indexed writes",
            "storage.db.db.test.db reopen replays pending derived embeddings",
            "storage.db.db.test.db replay respects per-index applied watermarks",
            "storage.db.db.test.db replay applies dense embeddings from artifact payloads",
        },
    });
    const run_lib_db_enrichment_replay_tests = b.addRunArtifact(lib_db_enrichment_replay_tests);
    const lib_db_enrichment_replay_step = b.step("lib-db-enrichment-replay-test", "Run root-module DB enrichment replay tests");
    lib_db_enrichment_replay_step.dependOn(&run_lib_db_enrichment_replay_tests.step);

    const lib_db_enrichment_cutover_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = &.{
            "storage.db.db.test.db split cutover",
            "storage.db.db.test.db merge-style cutover",
        },
    });
    const run_lib_db_enrichment_cutover_tests = b.addRunArtifact(lib_db_enrichment_cutover_tests);
    const lib_db_enrichment_cutover_step = b.step("lib-db-enrichment-cutover-test", "Run root-module DB enrichment cutover tests");
    lib_db_enrichment_cutover_step.dependOn(&run_lib_db_enrichment_cutover_tests.step);

    const lib_db_enrichment_split_cutover_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = &.{
            "storage.db.db.test.db split cutover fences enrichment to the owning range",
            "storage.db.db.test.db split cutover preserves enrichment resume and fencing across reopen",
        },
    });
    const run_lib_db_enrichment_split_cutover_tests = b.addRunArtifact(lib_db_enrichment_split_cutover_tests);
    const lib_db_enrichment_split_cutover_step = b.step("lib-db-enrichment-split-cutover-test", "Run root-module DB enrichment split cutover tests");
    lib_db_enrichment_split_cutover_step.dependOn(&run_lib_db_enrichment_split_cutover_tests.step);

    const lib_db_enrichment_merge_cutover_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = &.{
            "storage.db.db.test.db merge-style cutover fences enrichment to the merged receiver range",
            "storage.db.db.test.db merge-style cutover preserves enrichment resume and fencing across reopen",
        },
    });
    const run_lib_db_enrichment_merge_cutover_tests = b.addRunArtifact(lib_db_enrichment_merge_cutover_tests);
    const lib_db_enrichment_merge_cutover_step = b.step("lib-db-enrichment-merge-cutover-test", "Run root-module DB enrichment merge cutover tests");
    lib_db_enrichment_merge_cutover_step.dependOn(&run_lib_db_enrichment_merge_cutover_tests.step);

    const lib_db_enrichment_split_cutover_reopen_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = &.{"storage.db.db.test.db split cutover preserves enrichment resume and fencing across reopen"},
    });
    const run_lib_db_enrichment_split_cutover_reopen_tests = b.addRunArtifact(lib_db_enrichment_split_cutover_reopen_tests);
    const lib_db_enrichment_split_cutover_reopen_step = b.step("lib-db-enrichment-split-cutover-reopen-test", "Run root-module DB split cutover reopen test");
    lib_db_enrichment_split_cutover_reopen_step.dependOn(&run_lib_db_enrichment_split_cutover_reopen_tests.step);

    const lib_db_enrichment_merge_cutover_reopen_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = &.{"storage.db.db.test.db merge-style cutover preserves enrichment resume and fencing across reopen"},
    });
    const run_lib_db_enrichment_merge_cutover_reopen_tests = b.addRunArtifact(lib_db_enrichment_merge_cutover_reopen_tests);
    const lib_db_enrichment_merge_cutover_reopen_step = b.step("lib-db-enrichment-merge-cutover-reopen-test", "Run root-module DB merge cutover reopen test");
    lib_db_enrichment_merge_cutover_reopen_step.dependOn(&run_lib_db_enrichment_merge_cutover_reopen_tests.step);

    const lib_db_query_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = &.{
            "storage.db.db.test.db full-text",
            "storage.db.db.test.db dense ",
            "storage.db.db.test.db sparse ",
            "storage.db.db.test.db graph ",
            "storage.db.db.test.db search ",
            "storage.db.db.test.db document _edges",
            "storage.db.db.test.db document _embeddings",
        },
    });
    const run_lib_db_query_tests = b.addRunArtifact(lib_db_query_tests);
    const lib_db_query_step = b.step("lib-db-query-test", "Run root-module DB query/indexing tests");
    lib_db_query_step.dependOn(&run_lib_db_query_tests.step);

    const lib_db_result_shape_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = &.{
            "dedupeSearchHitsById uses ordinals when hit page is complete",
            "applyStoredSearchPatternFilters resolves native doc id constraints to hit ordinals",
            "applyStoredSearchPatternFilters uses hit ordinals for resolved doc filters",
            "applyStoredSearchPatternFilters fails closed without resolved ordinal projection",
            "applyStoredSearchPatternFilters fails closed when ordinal projection is unsupported",
            "native dense constraints fail closed without ordinal vector mapping",
            "buildPatternDocumentHits preserves resolved binding ordinals",
            "executeSingleNonPatternQueryWithSets hydrates graph documents from include_documents",
            "executeSearchGraphWithSets preserves node ordinals",
            "cloneNamedSetAsResult preserves hit ordinals",
            "fuseNamedSets preserves source hit ordinals",
            "fuseNamedSets deduplicates aliases by ordinal when complete",
            "fuseNamedSets drops conflicting source hit ordinals",
            "applyGraphUnion deduplicates by ordinals when hit pages are complete",
            "applyGraphIntersection uses ordinals when hit pages are complete",
        },
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lib_db_result_shape_tests = b.addRunArtifact(lib_db_result_shape_tests);
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
    const run_lib_db_reopen_tests = b.addRunArtifact(lib_db_reopen_tests);
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
        },
    });
    const run_lib_db_txn_tests = b.addRunArtifact(lib_db_txn_tests);
    const lib_db_txn_step = b.step("lib-db-txn-test", "Run root-module DB TTL/transaction tests");
    lib_db_txn_step.dependOn(&run_lib_db_txn_tests.step);

    const lib_metadata_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = selectTestFilters(b, &.{"metadata."}),
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lib_metadata_tests = b.addRunArtifact(lib_metadata_tests);
    const lib_metadata_test_step = b.step("lib-metadata-test", "Run root-module metadata tests only");
    lib_metadata_test_step.dependOn(&run_lib_metadata_tests.step);

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
    const run_lib_metadata_table_workflow_tests = b.addRunArtifact(lib_metadata_table_workflow_tests);
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
    const run_lib_metadata_sim_tests = b.addRunArtifact(lib_metadata_sim_tests);
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
    const run_lib_metadata_sim_core_tests = b.addRunArtifact(lib_metadata_sim_core_tests);
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
    const run_lib_metadata_sim_smoke_tests = b.addRunArtifact(lib_metadata_sim_smoke_tests);
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
    const run_lib_metadata_vopr_tests = b.addRunArtifact(lib_metadata_vopr_tests);
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
    const run_lib_metadata_vopr_chaos_tests = b.addRunArtifact(lib_metadata_vopr_chaos_tests);
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
    });
    const run_lib_metadata_sim_public_tests = b.addRunArtifact(lib_metadata_sim_public_tests);
    const lib_metadata_sim_public_test_step = b.step("lib-metadata-sim-public-test", "Run metadata public lifecycle/split/merge simulation tests");
    lib_metadata_sim_public_test_step.dependOn(&run_lib_metadata_sim_public_tests.step);

    const public_api_parity_default_filters = [_][]const u8{
        "public openapi contract module is generated and wired",
        "metadata openapi module generates extractor surface for routed endpoints",
        "usermgr openapi module generates extractor surface for routed endpoints",
        "client openapi module resolves shared refs through owner modules",
        "batch parser accepts Go transform op spelling",
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
        "api http server serves retrieval agent response envelope",
        "api http server serves table batch writes",
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
        "api http server serves provisioned index runtime backfill status across shards",
        "api http server serves table create and drop",
        "api http server serves table metadata routes against real metadata service",
        "api http server create table with replication sources returns encoded table detail",
        "api http server lists cluster backups through public route",
        "api http server backs up and restores a table through public routes",
        "api http server prefers metadata-owned restore over inline write-source restore",
        "public api smoke e2e creates table inserts and queries documents",
        "public api e2e recreates managed embeddings index after corrupt artifact",
        "public api split e2e uses distributed global text stats for bm25 and significant_terms",
        "public api multi-node e2e routes CRUD from a non-host node",
    };
    const public_api_parity_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = selectTestFilters(b, &public_api_parity_default_filters),
    });
    const run_public_api_parity_tests = b.addRunArtifact(public_api_parity_tests);
    run_public_api_parity_tests.step.dependOn(&openapi_root_check.step);
    const public_api_parity_test_step = b.step("public-api-parity-test", "Run focused stateful public API parity tests");
    public_api_parity_test_step.dependOn(&run_public_api_parity_tests.step);

    const lib_api_auth_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = &.{
            "api http server requires auth on public routes when enabled",
            "api http server forbids non-admin secret access when auth is enabled",
            "api http server query builder requires table read permission when auth is enabled",
            "api http server restricts runtime schema debug to admins when auth is enabled",
            "api http server serves user management routes when auth is enabled",
            "api http server serves api key and row filter routes",
            "api http server returns json user auth errors",
            "api http server serves mcp and a2a protocol surfaces",
            "auth row filter resolver expands username references",
            "auth row filter resolver expands metadata references",
            "auth row filter validator accepts username references",
            "auth row filter resolver rejects unsupported auth paths",
            "auth row filter validator rejects malformed auth node",
            "effective resolved row filter prefers table filter before wildcard",
        },
    });
    const run_lib_api_auth_tests = b.addRunArtifact(lib_api_auth_tests);
    run_lib_api_auth_tests.step.dependOn(&openapi_root_check.step);
    const lib_api_auth_test_step = b.step("lib-api-auth-test", "Run focused API auth/usermgr HTTP tests");
    lib_api_auth_test_step.dependOn(&run_lib_api_auth_tests.step);

    const lib_api_docid_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = &.{
            "api table reads reject stale doc identity before multigroup fanout",
            "distributed table reads reject stale doc identity before multigroup fanout",
            "api public table query rejects only top-level internal fields",
            "api query contract rejects doc identity control fields when with relaxes schema",
            "api query contract public parser rejects internal shard doc identity controls",
            "api distributed graph hydrate carries identity generation and clears cross-range ordinals",
            "distributed graph rejects doc identity rebuild before cross-range fanout",
            "distributed graph rejects unstamped result refs before cross-range fanout",
            "distributed graph edge reader carries identity generation",
            "query merge preserves common identity read generation",
            "query encoder does not expose internal doc ordinals",
            "graph edge local read rejects stale identity generation",
            "catalog doc identity readiness checks table range health",
            "catalog resolved filter validation accepts preserved split identity domains",
            "metadata merge request validation rejects incompatible doc identity namespaces",
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
            "internal group write routes map shard doc identity mismatch to conflict",
            "internal group join routes map doc identity mismatch to conflict",
            "internal group read routes map doc identity mismatch to conflict",
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
            "dense vector id ignores ordinal metadata for a different doc",
            "dense metadata prefetch includes legacy ordinal vector ids",
            "db dense index stores stable vector ids with ordinal filter mappings",
            "db dense artifact rebuild preserves stable vector ids distinct from ordinals",
            "db sparse index keeps physical doc nums distinct from doc identity ordinals",
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
            "segment doc ordinal sidecar roundtrip and merge preserve live order",
            "db text compaction preserves ordinal filters across reopen",
            "structured filter doc set cache returns owned clones",
            "structured filter doc set cache separates shared namespace generation keys",
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
        .filters = &.{
            "provisioned table write source rejects stale doc identity namespace before write",
            "bound table write source backs up and restores a local table",
            "provisioned table restore rejects mismatched doc identity namespace",
            "provisioned restore repair open rejects stale doc identity namespace",
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
            "public table query handler maps doc identity unavailable errors",
            "public table query view handler maps doc identity unavailable errors",
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
    const lib_serverless_docid_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = &.{
            "serverless query module compiles",
            "search plan rejects internal doc identity controls",
            "serverless graph plans reject internal doc identity controls",
        },
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lib_api_docid_tests = b.addRunArtifact(lib_api_docid_tests);
    const run_lib_serverless_docid_tests = b.addRunArtifact(lib_serverless_docid_tests);
    const run_api_transactions_docid_tests = b.addRunArtifact(api_transactions_docid_tests);
    const run_api_table_writes_docid_tests = b.addRunArtifact(api_table_writes_docid_tests);
    const run_api_public_table_http_docid_tests = b.addRunArtifact(api_public_table_http_docid_tests);
    const run_raft_transition_runtime_docid_tests = b.addRunArtifact(raft_transition_runtime_docid_tests);
    const lib_docid_lifecycle_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = &.{
            "metadata reconciler does not automatically split ordinal exhausted doc identity",
            "metadata state classifies mixed-version doc identity lifecycle reports",
            "metadata state marks doc identity rebuild required on range namespace mismatch",
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
            "db text compaction preserves ordinal filters across reopen",
            "db lsm primary compaction preserves doc identity ordinals",
            "db allocates final document ordinal with all index families present",
            "db stats flag document identity ordinal capacity exhaustion",
            "db rejects new document writes at ordinal exhaustion for every sync level",
            "db transaction intent writes reject new documents at ordinal exhaustion",
            "db search requests default to current identity generation snapshot",
            "db validates internal resolved doc filter wire namespace and generation",
            "db resolved doc-set projection honors identity read generation",
        },
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lib_docid_lifecycle_tests = b.addRunArtifact(lib_docid_lifecycle_tests);
    const docid_lifecycle_test_step = b.step("docid-lifecycle-test", "Run focused DOCID lifecycle and distributed snapshot hardening tests");
    docid_lifecycle_test_step.dependOn(&run_lib_docid_lifecycle_tests.step);
    docid_lifecycle_test_step.dependOn(&run_api_transactions_docid_tests.step);
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

    const lib_api_swarm_backup_restore_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = &.{
            "public api swarm-like e2e backs up drops and restores a table",
        },
    });
    const run_lib_api_swarm_backup_restore_tests = b.addRunArtifact(lib_api_swarm_backup_restore_tests);
    const lib_api_swarm_backup_restore_test_step = b.step("lib-api-swarm-backup-restore-test", "Run the focused swarm-like backup/restore e2e test");
    lib_api_swarm_backup_restore_test_step.dependOn(&run_lib_api_swarm_backup_restore_tests.step);

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
    const run_lib_metadata_sim_forward_tests = b.addRunArtifact(lib_metadata_sim_forward_tests);
    const lib_metadata_sim_forward_test_step = b.step("lib-metadata-sim-forward-test", "Run metadata HTTP forwarding simulation tests only");
    lib_metadata_sim_forward_test_step.dependOn(&run_lib_metadata_sim_forward_tests.step);

    const lib_metadata_service_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = &.{
            "metadata service ",
            "metadata control loop can drive the real metadata service",
            "table workflow can drive real metadata service topology and split setup",
            "table workflow can drive placement intents through the real metadata control loop",
        },
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lib_metadata_service_tests = b.addRunArtifact(lib_metadata_service_tests);
    const lib_metadata_service_test_step = b.step("lib-metadata-service-test", "Run metadata service/control-loop integration tests");
    lib_metadata_service_test_step.dependOn(&run_lib_metadata_service_tests.step);

    const lib_metadata_logic_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = &.{
            "metadata reconciler",
            "metadata transition state",
            "metadata server module compiles",
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
            "metadata state machine projects transitions through metadata apply store",
            "table provisioner restore rejects mismatched doc identity namespace",
        },
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lib_metadata_logic_tests = b.addRunArtifact(lib_metadata_logic_tests);
    const lib_metadata_logic_test_step = b.step("lib-metadata-logic-test", "Run metadata logic/state/planner tests");
    lib_metadata_logic_test_step.dependOn(&run_lib_metadata_logic_tests.step);

    const lib_storage_default_filters = [_][]const u8{
        "storage.",
    };
    const lib_storage_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = selectTestFilters(b, &lib_storage_default_filters),
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lib_storage_tests = b.addRunArtifact(lib_storage_tests);
    const lib_storage_test_step = b.step("lib-storage-test", "Run root-module storage tests only");
    lib_storage_test_step.dependOn(&run_lib_storage_tests.step);

    const lib_storage_progress_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = selectTestFilters(b, &lib_storage_default_filters),
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lib_storage_progress_tests = b.addRunArtifact(lib_storage_progress_tests);
    for (unit_progress_skip_filters) |filter| {
        run_lib_storage_progress_tests.addArgs(&.{ "--skip-test-filter", filter });
    }

    const lsm_backend_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = &.{"storage.lsm_backend."},
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lsm_backend_tests = b.addRunArtifact(lsm_backend_tests);
    const lsm_backend_test_step = b.step("lsm-backend-test", "Run LSM backend unit tests only");
    lsm_backend_test_step.dependOn(&run_lsm_backend_tests.step);

    const resource_budget_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = &.{
            "resource manager observes over-budget external usage",
            "cache reports shared byte usage to resource manager",
            "derived backlog tracker accounts and releases payload bytes",
            "hbc shared cache namespaces entries",
            "hbc shared cache evicts across namespaces under one resource budget",
            "hbc cache reports byte usage to resource manager",
            "hbc cache shrinks to resource budget under pressure",
            "provisioned group storage derives all resource budgets",
        },
    });
    const run_resource_budget_tests = b.addRunArtifact(resource_budget_tests);
    const resource_budget_test_step = b.step("resource-budget-test", "Run storage resource-manager accounting tests");
    resource_budget_test_step.dependOn(&run_resource_budget_tests.step);

    const sim_test_step = b.step("sim-test", "Run mocked-time Antfly simulation suites");
    sim_test_step.dependOn(&run_lib_metadata_sim_smoke_tests.step);
    sim_test_step.dependOn(&run_lib_metadata_vopr_tests.step);
    sim_test_step.dependOn(&run_lib_raft_sim_tests.step);

    const integration_test_step = b.step("integration-test", "Run focused real HTTP and public API integration suites");
    integration_test_step.dependOn(&run_lib_metadata_sim_public_tests.step);
    integration_test_step.dependOn(&run_lib_metadata_sim_forward_tests.step);
    integration_test_step.dependOn(&run_public_api_parity_tests.step);

    const chaos_test_step = b.step("chaos-test", "Run bounded generated chaos campaigns with labeled progress");
    var chaos_progress_tail: ?*std.Build.Step = null;
    chaos_progress_tail = chainLabeledRun(b, lib_metadata_vopr_chaos_tests, "lib-metadata-vopr-chaos-test", chaos_progress_tail);
    chaos_progress_tail = chainLabeledRun(b, lib_lsm_backend_chaos_tests, "lib-lsm-backend-chaos-test", chaos_progress_tail);
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
    const lib_audio_test_step = b.step("lib-audio-test", "Run audio transcribing and synthesizing runtime tests");
    lib_audio_test_step.dependOn(&run_lib_audio_tests.step);

    const lib_audio_xiph_conformance = b.addExecutable(.{
        .name = "lib-audio-xiph-conformance",
        .root_module = b.createModule(.{
            .root_source_file = b.path("lib/audio/audio_xiph_corpora_e2e.zig"),
            .target = target,
            .optimize = .ReleaseFast,
        }),
    });
    lib_audio_xiph_conformance.root_module.addImport("build_options", termite_build_options_mod);
    if (termite_ffmpeg_paths) |ffmpeg_paths| {
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
    lib_audio_misc_conformance.root_module.addImport("build_options", termite_build_options_mod);
    if (termite_ffmpeg_paths) |ffmpeg_paths| {
        lib_audio_misc_conformance.root_module.addIncludePath(.{ .cwd_relative = ffmpeg_paths.include_dir });
    }
    lib_audio_misc_conformance.root_module.link_libc = true;

    const fetch_lib_audio_xiph_conformance = b.addRunArtifact(lib_audio_xiph_conformance);
    fetch_lib_audio_xiph_conformance.addArg("fetch");
    fetch_lib_audio_xiph_conformance.addArg("/tmp/termite-audio-xiph-corpora");

    const fetch_lib_audio_misc_conformance = b.addRunArtifact(lib_audio_misc_conformance);
    fetch_lib_audio_misc_conformance.addArg("fetch");
    fetch_lib_audio_misc_conformance.addArg("/tmp/termite-audio-misc-corpora");

    const lib_audio_conformance_fetch_step = b.step("lib-audio-conformance-fetch", "Fetch the lib/audio external conformance fixtures");
    lib_audio_conformance_fetch_step.dependOn(&fetch_lib_audio_xiph_conformance.step);
    lib_audio_conformance_fetch_step.dependOn(&fetch_lib_audio_misc_conformance.step);

    const fetch_lib_audio_xiph_conformance_quiet = b.addRunArtifact(lib_audio_xiph_conformance);
    fetch_lib_audio_xiph_conformance_quiet.addArg("fetch");
    fetch_lib_audio_xiph_conformance_quiet.addArg("/tmp/termite-audio-xiph-corpora");
    const fetch_lib_audio_xiph_conformance_quiet_step = expectQuietSuccess(fetch_lib_audio_xiph_conformance_quiet);

    const fetch_lib_audio_misc_conformance_quiet = b.addRunArtifact(lib_audio_misc_conformance);
    fetch_lib_audio_misc_conformance_quiet.addArg("fetch");
    fetch_lib_audio_misc_conformance_quiet.addArg("/tmp/termite-audio-misc-corpora");
    const fetch_lib_audio_misc_conformance_quiet_step = expectQuietSuccess(fetch_lib_audio_misc_conformance_quiet);

    const run_lib_audio_xiph_conformance = b.addRunArtifact(lib_audio_xiph_conformance);
    run_lib_audio_xiph_conformance.addArg("run");
    run_lib_audio_xiph_conformance.addArg("/tmp/termite-audio-xiph-corpora");
    run_lib_audio_xiph_conformance.addArg("--no-fetch");

    const run_lib_audio_misc_conformance = b.addRunArtifact(lib_audio_misc_conformance);
    run_lib_audio_misc_conformance.addArg("run");
    run_lib_audio_misc_conformance.addArg("/tmp/termite-audio-misc-corpora");
    run_lib_audio_misc_conformance.addArg("--no-fetch");

    const lib_audio_conformance_run_step = b.step("lib-audio-conformance-run", "Run lib/audio conformance suites without fetching fixtures");
    lib_audio_conformance_run_step.dependOn(&run_lib_audio_xiph_conformance.step);
    lib_audio_conformance_run_step.dependOn(&run_lib_audio_misc_conformance.step);

    const run_lib_audio_xiph_conformance_after_fetch = b.addRunArtifact(lib_audio_xiph_conformance);
    run_lib_audio_xiph_conformance_after_fetch.addArg("run");
    run_lib_audio_xiph_conformance_after_fetch.addArg("/tmp/termite-audio-xiph-corpora");
    run_lib_audio_xiph_conformance_after_fetch.addArg("--no-fetch");
    run_lib_audio_xiph_conformance_after_fetch.step.dependOn(&fetch_lib_audio_xiph_conformance.step);

    const run_lib_audio_misc_conformance_after_fetch = b.addRunArtifact(lib_audio_misc_conformance);
    run_lib_audio_misc_conformance_after_fetch.addArg("run");
    run_lib_audio_misc_conformance_after_fetch.addArg("/tmp/termite-audio-misc-corpora");
    run_lib_audio_misc_conformance_after_fetch.addArg("--no-fetch");
    run_lib_audio_misc_conformance_after_fetch.step.dependOn(&fetch_lib_audio_misc_conformance.step);

    const lib_audio_conformance_step = b.step("lib-audio-conformance", "Fetch and run lib/audio conformance suites");
    lib_audio_conformance_step.dependOn(&run_lib_audio_xiph_conformance_after_fetch.step);
    lib_audio_conformance_step.dependOn(&run_lib_audio_misc_conformance_after_fetch.step);

    const run_lib_audio_xiph_conformance_after_fetch_quiet = b.addRunArtifact(lib_audio_xiph_conformance);
    run_lib_audio_xiph_conformance_after_fetch_quiet.addArg("run");
    run_lib_audio_xiph_conformance_after_fetch_quiet.addArg("/tmp/termite-audio-xiph-corpora");
    run_lib_audio_xiph_conformance_after_fetch_quiet.addArg("--no-fetch");
    run_lib_audio_xiph_conformance_after_fetch_quiet.step.dependOn(fetch_lib_audio_xiph_conformance_quiet_step);
    const run_lib_audio_xiph_conformance_after_fetch_quiet_step = expectQuietSuccess(run_lib_audio_xiph_conformance_after_fetch_quiet);

    const run_lib_audio_misc_conformance_after_fetch_quiet = b.addRunArtifact(lib_audio_misc_conformance);
    run_lib_audio_misc_conformance_after_fetch_quiet.addArg("run");
    run_lib_audio_misc_conformance_after_fetch_quiet.addArg("/tmp/termite-audio-misc-corpora");
    run_lib_audio_misc_conformance_after_fetch_quiet.addArg("--no-fetch");
    run_lib_audio_misc_conformance_after_fetch_quiet.step.dependOn(fetch_lib_audio_misc_conformance_quiet_step);
    const run_lib_audio_misc_conformance_after_fetch_quiet_step = expectQuietSuccess(run_lib_audio_misc_conformance_after_fetch_quiet);
    conformance_test_step.dependOn(run_lib_audio_xiph_conformance_after_fetch_quiet_step);
    conformance_test_step.dependOn(run_lib_audio_misc_conformance_after_fetch_quiet_step);

    const swarm_runtime_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/swarm_runtime_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    var swarm_runtime_imports = antfly_imports;
    swarm_runtime_imports.build_options = swarm_runtime_build_options;
    swarm_runtime_imports.configure(b, swarm_runtime_test_mod, true, true);
    const usermgr_storage_swarm_runtime_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/usermgr/storage_imports.zig"),
        .target = target,
        .optimize = optimize,
    });
    usermgr_storage_swarm_runtime_test_mod.addImport("antfly_root", swarm_runtime_test_mod);
    usermgr_storage_swarm_runtime_test_mod.addImport("antfly_platform", platform_mod);
    swarm_runtime_test_mod.addImport("usermgr_storage", usermgr_storage_swarm_runtime_test_mod);
    const lib_swarm_runtime_tests = b.addTest(.{
        .root_module = swarm_runtime_test_mod,
        .filters = &.{
            "swarm runtime module compiles",
            "swarm runtime local replica reconcile permit stays blocked while startup debt is unresolved",
            "swarm runtime registers internal group routes explicitly",
            "parse cli accepts config path",
            "parse cli accepts canonical host port and models dir flags",
            "termite config uses cli override before common config",
            "swarm public api caps keep alive request reuse",
            "parse cli accepts termite budget overrides",
            "termite config falls back to common config",
            "swarm runtime resolves paths from common storage base dir",
        },
    });
    const lib_swarm_runtime_test_step = b.step("lib-swarm-runtime-test", "Run focused swarm runtime tests");
    const run_lib_swarm_runtime_tests = b.addRunArtifact(lib_swarm_runtime_tests);
    lib_swarm_runtime_test_step.dependOn(&run_lib_swarm_runtime_tests.step);

    const raft_test_step = b.step("raft-test", "Run raft integration unit tests");
    raft_test_step.dependOn(&run_raft_unit_tests.step);

    const raft_transport_test_step = b.step("raft-transport-test", "Run raft transport unit tests");
    raft_transport_test_step.dependOn(&run_raft_transport_tests.step);

    unit_test_step.dependOn(&run_lib_regex_tests.step);
    unit_test_step.dependOn(&run_lib_jsonschema_tests.step);
    unit_test_step.dependOn(&run_lib_generating_tests.step);
    unit_test_step.dependOn(&run_lib_embeddings_tests.step);
    unit_test_step.dependOn(&run_lib_vectorindex_tests.step);
    unit_test_step.dependOn(&run_lib_chunking_tests.step);
    unit_test_step.dependOn(&run_lib_generating_runtime_tests.step);
    unit_test_step.dependOn(&run_lib_reranking_tests.step);
    unit_test_step.dependOn(&run_lib_reranking_runtime_tests.step);
    unit_test_step.dependOn(&run_lib_common_tests.step);
    unit_test_step.dependOn(&run_lib_common_config_tests.step);
    unit_test_step.dependOn(&run_lib_casbin_tests.step);
    unit_test_step.dependOn(&run_lib_usermgr_tests.step);
    unit_test_step.dependOn(&run_embedded_tests.step);
    unit_test_step.dependOn(&run_antfly_embedded_pkg_tests.step);
    unit_test_step.dependOn(&run_capi_tests.step);
    unit_test_step.dependOn(&run_lib_db_tests.step);
    unit_test_step.dependOn(&run_lib_db_result_shape_tests.step);
    unit_test_step.dependOn(&run_serverless_tests.step);
    unit_test_step.dependOn(&run_lib_data_runtime_tests.step);
    unit_test_step.dependOn(&run_lib_data_storage_tests.step);
    unit_test_step.dependOn(&run_lib_metadata_logic_tests.step);
    unit_test_step.dependOn(&run_lib_metadata_service_tests.step);
    unit_test_step.dependOn(&run_lib_api_docid_tests.step);
    unit_test_step.dependOn(&run_lib_api_auth_tests.step);
    unit_test_step.dependOn(&run_public_api_parity_tests.step);
    unit_test_step.dependOn(&run_lib_template_tests.step);
    unit_test_step.dependOn(&run_lib_toon_tests.step);
    unit_test_step.dependOn(&run_lib_mcp_tests.step);
    unit_test_step.dependOn(&run_lib_a2a_tests.step);
    unit_test_step.dependOn(&run_lib_image_tests.step);
    unit_test_step.dependOn(&run_lib_audio_tests.step);
    unit_test_step.dependOn(lib_swarm_runtime_test_step);
    unit_test_step.dependOn(&run_raft_unit_tests.step);
    unit_test_step.dependOn(&run_raft_transport_tests.step);

    var unit_progress_tail: ?*std.Build.Step = null;
    unit_progress_tail = chainLabeledRun(b, lib_storage_progress_tests, "lib-storage-test", unit_progress_tail);
    unit_progress_tail = chainLabeledRun(b, lib_db_tests, "lib-db-test", unit_progress_tail);
    unit_progress_tail = chainLabeledRun(b, lib_metadata_tests, "lib-metadata-test", unit_progress_tail);
    unit_progress_tail = chainLabeledRun(b, raft_unit_tests, "raft-test", unit_progress_tail);
    unit_progress_tail = chainLabeledRun(b, raft_transport_tests, "raft-transport-test", unit_progress_tail);
    unit_progress_tail = chainLabeledRun(b, serverless_tests, "serverless-test", unit_progress_tail);
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
    const run_storage_lmdb_replay_tests = b.addRunArtifact(storage_lmdb_replay_tests);
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
    const run_storage_lmdb_soak_tests = b.addRunArtifact(storage_lmdb_soak_tests);
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
    const wal_unit_tests = b.addTest(.{
        .root_module = wal_test_mod,
    });
    const run_wal_unit_tests = b.addRunArtifact(wal_unit_tests);

    const wal_test_step = b.step("wal-test", "Run storage/wal unit tests");
    wal_test_step.dependOn(&run_wal_unit_tests.step);

    const wal_sim_tests = b.addTest(.{
        .root_module = wal_test_mod,
        .filters = &.{"wal sim"},
    });
    const run_wal_sim_tests = b.addRunArtifact(wal_sim_tests);
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
    const run_wal_vopr_tests = b.addRunArtifact(wal_vopr_tests);
    const wal_vopr_test_step = b.step("wal-vopr-test", "Run WAL modeled-time VOPR smoke tests");
    wal_vopr_test_step.dependOn(&run_wal_vopr_tests.step);

    const wal_replay_tests = b.addTest(.{
        .root_module = wal_test_mod,
        .filters = &.{"wal replay fixtures stay green"},
    });
    const run_wal_replay_tests = b.addRunArtifact(wal_replay_tests);
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
    const run_wal_soak_tests = b.addRunArtifact(wal_soak_tests);
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
    const persistent_unit_tests = b.addTest(.{
        .root_module = persistent_test_mod,
    });
    const run_persistent_unit_tests = b.addRunArtifact(persistent_unit_tests);

    const persistent_test_step = b.step("persistent-test", "Run storage/persistent unit tests");
    persistent_test_step.dependOn(&run_persistent_unit_tests.step);

    const persistent_sim_tests = b.addTest(.{
        .root_module = persistent_test_mod,
        .filters = &.{"persistent sim workloads stay green"},
    });
    const run_persistent_sim_tests = b.addRunArtifact(persistent_sim_tests);
    const persistent_sim_step = b.step("persistent-sim-test", "Run only the persistent simulation workload tests");
    persistent_sim_step.dependOn(&run_persistent_sim_tests.step);

    const persistent_replay_tests = b.addTest(.{
        .root_module = persistent_test_mod,
        .filters = &.{"persistent replay fixtures stay green"},
    });
    const run_persistent_replay_tests = b.addRunArtifact(persistent_replay_tests);
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
    const run_persistent_vopr_tests = b.addRunArtifact(persistent_vopr_tests);
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
    const run_persistent_soak_tests = b.addRunArtifact(persistent_soak_tests);
    const persistent_soak_step = b.step("persistent-sim-soak", "Run only the persistent simulation soak test");
    persistent_soak_step.dependOn(&run_persistent_soak_tests.step);

    storage_sim_soak_step.dependOn(&run_persistent_soak_tests.step);

    const index_manager_test_mod = makeLmdbModule(b, "pkg/antfly/src/index_manager_test_root.zig", target, optimize, build_options, lmdb_engine_mod, platform_mod);
    addSnowballModule(b, index_manager_test_mod);
    index_manager_test_mod.addImport("bloom", bloom_mod);
    index_manager_test_mod.addImport("antfly_vellum", vellum_mod);
    index_manager_test_mod.addImport("antfly_vector", vector_mod);
    index_manager_test_mod.addImport("antfly_vectorindex", vectorindex_mod);
    index_manager_test_mod.addImport("antfly_chunking", chunking_mod);
    index_manager_test_mod.addImport("antfly_regex", regex_mod);
    const index_manager_unit_tests = b.addTest(.{
        .root_module = index_manager_test_mod,
        .filters = selectTestFilters(b, &.{}),
    });
    const run_index_manager_unit_tests = b.addRunArtifact(index_manager_unit_tests);

    const index_manager_test_step = b.step("index-manager-test", "Run storage/db/catalog/index_manager unit tests");
    index_manager_test_step.dependOn(&run_index_manager_unit_tests.step);

    const index_manager_resource_tests = b.addTest(.{
        .root_module = index_manager_test_mod,
        .filters = &.{"text merge resource manager accounts pending bytes and active buffers"},
    });
    const run_index_manager_resource_tests = b.addRunArtifact(index_manager_resource_tests);
    const index_manager_resource_step = b.step("index-manager-resource-test", "Run index manager resource-manager accounting tests");
    index_manager_resource_step.dependOn(&run_index_manager_resource_tests.step);

    const index_manager_sim_tests = b.addTest(.{
        .root_module = index_manager_test_mod,
        .filters = &.{"index manager sim workloads stay green"},
    });
    const run_index_manager_sim_tests = b.addRunArtifact(index_manager_sim_tests);
    const index_manager_sim_step = b.step("index-manager-sim-test", "Run only the index manager simulation workload tests");
    index_manager_sim_step.dependOn(&run_index_manager_sim_tests.step);

    const index_manager_replay_tests = b.addTest(.{
        .root_module = index_manager_test_mod,
        .filters = &.{"index manager replay fixtures stay green"},
    });
    const run_index_manager_replay_tests = b.addRunArtifact(index_manager_replay_tests);
    const index_manager_replay_step = b.step("index-manager-replay-fixtures", "Run only the index manager replay fixture tests");
    index_manager_replay_step.dependOn(&run_index_manager_replay_tests.step);

    const index_manager_vopr_tests = b.addTest(.{
        .root_module = index_manager_test_mod,
        .filters = &.{
            "index manager modeled replay fixtures stay green",
            "index manager modeled crash fixtures stay green",
        },
    });
    const run_index_manager_vopr_tests = b.addRunArtifact(index_manager_vopr_tests);
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
    db_test_mod.addImport("antfly_chunking", chunking_mod);
    db_test_mod.addImport("antfly_regex", regex_mod);
    db_test_mod.addImport("raft_engine", raft_engine_mod);
    db_test_mod.addImport("termite_chunker", termite_chunker_mod);
    db_test_mod.addImport("termite_api", termite_api_mod);
    db_test_mod.addImport("antfly_reranking", reranking_mod);
    db_test_mod.addImport("antfly_scraping", scraping_mod);
    db_test_mod.addImport("antfly_transcribing", transcribing_db_test_stub_mod);
    db_test_mod.addImport("httpx", httpx_mod);
    db_test_mod.addImport("antfly_pdf", pdf_mod);
    db_test_mod.addImport("antfly_image", image_mod);
    db_test_mod.addImport("antfly_font", font_mod);

    const db_split_sim_default_filters = [_][]const u8{
        "db split sim default workload stays green",
        "db split sim reopen-heavy workload stays green",
    };
    const db_split_sim_tests = b.addTest(.{
        .root_module = db_test_mod,
        .filters = selectTestFilters(b, &db_split_sim_default_filters),
    });
    const run_db_split_sim_tests = b.addRunArtifact(db_split_sim_tests);
    const db_split_sim_step = b.step("db-split-sim-test", "Run only the DB split simulation workload tests");
    db_split_sim_step.dependOn(&run_db_split_sim_tests.step);

    const db_split_vopr_tests = b.addTest(.{
        .root_module = db_test_mod,
        .filters = &.{
            "db split modeled replay fixtures stay green",
            "db split modeled sim workloads stay green",
        },
    });
    const run_db_split_vopr_tests = b.addRunArtifact(db_split_vopr_tests);
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
    });
    const run_db_unit_tests = b.addRunArtifact(db_unit_tests);
    const db_test_step = b.step("db-test", "Run storage/db unit tests");
    db_test_step.dependOn(&run_db_unit_tests.step);

    const db_enrichment_tests = b.addTest(.{
        .root_module = db_test_mod,
        .filters = &.{"enrichment"},
    });
    const run_db_enrichment_tests = b.addRunArtifact(db_enrichment_tests);
    const db_enrichment_test_step = b.step("db-enrichment-test", "Run storage/db enrichment-related unit tests");
    db_enrichment_test_step.dependOn(&run_db_enrichment_tests.step);

    const db_enrichment_single_tests = b.addTest(.{
        .root_module = db_test_mod,
        .filters = &.{"db dense index can reference existing whole-doc embedding enrichment"},
    });
    const run_db_enrichment_single_tests = b.addRunArtifact(db_enrichment_single_tests);
    const db_enrichment_single_step = b.step("db-enrichment-single-test", "Run the focused whole-doc enrichment DB test");
    db_enrichment_single_step.dependOn(&run_db_enrichment_single_tests.step);

    const db_restore_managed_tests = b.addTest(.{
        .root_module = db_test_mod,
        .filters = &.{"db restore snapshot replays managed chunked dense embeddings"},
    });
    const run_db_restore_managed_tests = b.addRunArtifact(db_restore_managed_tests);
    const db_restore_managed_step = b.step("db-restore-managed-test", "Run the focused managed chunked dense restore DB test");
    db_restore_managed_step.dependOn(&run_db_restore_managed_tests.step);

    const provisioned_write_cache_failed_close_tests = b.addTest(.{
        .root_module = db_test_mod,
        .filters = &.{"provisioned write cache invalidation closes failed managed enrichment db without aborting"},
    });
    const run_provisioned_write_cache_failed_close_tests = b.addRunArtifact(provisioned_write_cache_failed_close_tests);
    const provisioned_write_cache_failed_close_step = b.step(
        "provisioned-write-cache-failed-close-test",
        "Run the focused provisioned write-cache failed-enrichment close regression",
    );
    provisioned_write_cache_failed_close_step.dependOn(&run_provisioned_write_cache_failed_close_tests.step);

    const provisioned_query_visibility_tests = b.addTest(.{
        .root_module = db_test_mod,
        .filters = &.{
            "provisioned table write source invalidates cached query db after managed dense replay becomes visible",
            "managed visibility publish hook updates runtime status cache from live writer",
            "provisioned read preparation invalidates readers without closing dirty writer cache",
            "provisioned read preparation does not block on same-table batch after early dirty publication",
            "provisioned table write source runtime status does not inspect read cache hbc stats when dirty",
            "provisioned table write source read cache overlay preserves live replay status",
            "read preparation keeps write cache dirty while auto bulk ingest is active",
            "dirty runtime status refresh finishes expired auto bulk before collecting leases",
            "managed startup catch-up ignores stale dirty bit after writer cache entry is gone",
            "provisioned table write source deinit drains restore repair jobs",
        },
    });
    const run_provisioned_query_visibility_tests = b.addRunArtifact(provisioned_query_visibility_tests);
    const provisioned_query_visibility_step = b.step(
        "provisioned-query-visibility-test",
        "Run the focused managed dense query-visibility cache invalidation regression",
    );
    provisioned_query_visibility_step.dependOn(&run_provisioned_query_visibility_tests.step);
    unit_test_step.dependOn(&run_provisioned_query_visibility_tests.step);

    const db_embeddings_update_tests = b.addTest(.{
        .root_module = db_test_mod,
        .filters = &.{"db document _embeddings update vector index and strip stored special fields"},
    });
    const run_db_embeddings_update_tests = b.addRunArtifact(db_embeddings_update_tests);
    const db_embeddings_update_step = b.step("db-embeddings-update-test", "Run the explicit _embeddings update DB test");
    db_embeddings_update_step.dependOn(&run_db_embeddings_update_tests.step);

    const db_merge_cutover_tests = b.addTest(.{
        .root_module = db_test_mod,
        .filters = &.{"db merge-style cutover preserves enrichment resume and fencing across reopen"},
    });
    const run_db_merge_cutover_tests = b.addRunArtifact(db_merge_cutover_tests);
    const db_merge_cutover_step = b.step("db-merge-cutover-test", "Run the merge cutover enrichment reopen DB test");
    db_merge_cutover_step.dependOn(&run_db_merge_cutover_tests.step);

    const db_shared_embedding_tests = b.addTest(.{
        .root_module = db_test_mod,
        .filters = &.{"db shared embedding enrichment feeds multiple dense indexes"},
    });
    const run_db_shared_embedding_tests = b.addRunArtifact(db_shared_embedding_tests);
    const db_shared_embedding_step = b.step("db-shared-embedding-test", "Run the shared embedding enrichment DB test");
    db_shared_embedding_step.dependOn(&run_db_shared_embedding_tests.step);

    const db_dense_parent_paging_tests = b.addTest(.{
        .root_module = db_test_mod,
        .filters = &.{"db dense parent paging fetches enough chunk hits before grouping"},
    });
    const run_db_dense_parent_paging_tests = b.addRunArtifact(db_dense_parent_paging_tests);
    const db_dense_parent_paging_step = b.step("db-dense-parent-paging-test", "Run the dense parent paging enrichment DB test");
    db_dense_parent_paging_step.dependOn(&run_db_dense_parent_paging_tests.step);

    const db_split_replay_tests = b.addTest(.{
        .root_module = db_test_mod,
        .filters = &.{"db split replay fixtures stay green"},
    });
    const run_db_split_replay_tests = b.addRunArtifact(db_split_replay_tests);
    const db_split_replay_step = b.step("db-split-replay-fixtures", "Run only the DB split replay fixture tests");
    db_split_replay_step.dependOn(&run_db_split_replay_tests.step);

    const sparse_test_mod = makeLmdbModule(b, "pkg/antfly/src/sparse_test_root.zig", target, optimize, build_options, lmdb_engine_mod, platform_mod);
    sparse_test_mod.addImport("bloom", bloom_mod);
    const sparse_unit_tests = b.addTest(.{
        .root_module = sparse_test_mod,
    });
    const run_sparse_unit_tests = b.addRunArtifact(sparse_unit_tests);

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

    // Default Antfly unit coverage is hermetic: no network fetchers, no
    // benchmarks, and no soak/conformance suites that require external corpora.
    // Focused aliases stay available as separate steps; broader module suites
    // are wired here once.
    dependOnAll(unit_test_step, &.{
        &run_lib_json_tests.step,
        &run_httpx_json_tests.step,
        &run_httpx_tests.step,
        &run_api_json_helpers_tests.step,
        &run_antfly_client_pkg_tests.step,
        &run_lib_unit_tests.step,
        &run_lib_metadata_tests.step,
        &run_lib_storage_tests.step,
        &run_lsm_backend_tests.step,
        &run_resource_budget_tests.step,
        &run_lmdb_unit_tests.step,
        &run_storage_lmdb_unit_tests.step,
        &run_docstore_unit_tests.step,
        &run_shard_unit_tests.step,
        &run_wal_unit_tests.step,
        &run_persistent_unit_tests.step,
        &run_index_manager_unit_tests.step,
        &run_db_unit_tests.step,
        &run_sparse_unit_tests.step,
        &run_derived_log_unit_tests.step,
    });

    const lmdb_bench_engine_options_c = makeLmdbBuildOptions(b, .c, false, false);
    const lmdb_bench_build_options_c = makeRootBuildOptions(b, .c, false, false, false, true, false, antfly_version);
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
    b.installArtifact(lmdb_bench_c);

    const lmdb_bench_engine_options_zig = makeLmdbBuildOptions(b, .zig, lmdb_evented_async_io, false);
    const lmdb_bench_build_options_zig = makeRootBuildOptions(b, .zig, lmdb_evented_async_io, false, false, true, false, antfly_version);
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
    b.installArtifact(lmdb_bench_zig);

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
    const split_bench_build_options = makeRootBuildOptions(b, lmdb_backend, lmdb_evented_async_io, false, false, true, false, antfly_version);
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
    b.installArtifact(split_bench);

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
    b.installArtifact(db_split_bench);

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
    b.installArtifact(docid_doc_set_bench);

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
    b.installArtifact(backend_bench);

    const run_backend_bench = b.addRunArtifact(backend_bench);
    if (b.args) |args| {
        run_backend_bench.addArgs(args);
    } else {
        run_backend_bench.addArgs(&.{ "--samples", "3", "--keys", "20000", "--value-size", "128", "--hit-repeats", "3", "--miss-repeats", "3", "--scan-repeats", "5" });
    }
    const backend_bench_step = b.step("backend-bench", "Benchmark shared backend workloads across LMDB and LSM backends");
    backend_bench_step.dependOn(&run_backend_bench.step);

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
    b.installArtifact(lsm_backend_bench);

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
    b.installArtifact(hbc_storage_read_bench);

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
    b.installArtifact(lsm_write_bench);

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
    b.installArtifact(lsm_write_bench_compare);

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
    b.installArtifact(text_segment_write_bench);

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
    b.installArtifact(lsm_backend_bench_compare);

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
    b.installArtifact(regex_bench);

    const run_regex_bench = b.addRunArtifact(regex_bench);
    if (b.args) |args| {
        run_regex_bench.addArgs(args);
    }
    const regex_bench_step = b.step("regex-bench", "Benchmark regex haystack matching and vellum automaton traversal");
    regex_bench_step.dependOn(&run_regex_bench.step);

    const wal_bench_engine_options = makeLmdbBuildOptions(b, lmdb_backend, lmdb_evented_async_io, false);
    const wal_bench_build_options = makeRootBuildOptions(b, lmdb_backend, lmdb_evented_async_io, false, false, true, false, antfly_version);
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
    b.installArtifact(wal_bench);

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
    const derived_log_bench_build_options = makeRootBuildOptions(b, lmdb_backend, lmdb_evented_async_io, false, false, true, false, antfly_version);
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
    b.installArtifact(derived_log_bench);

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
    b.installArtifact(json_bench);

    const run_json_bench = b.addRunArtifact(json_bench);
    if (b.args) |args| {
        run_json_bench.addArgs(args);
    }
    const json_bench_step = b.step("json-bench", "Benchmark std.json vs antfly-json parsing");
    json_bench_step.dependOn(&run_json_bench.step);

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
    b.installArtifact(bench);

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
    b.installArtifact(quickstart_bench);

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
    b.installArtifact(compat);

    const run_compat = b.addRunArtifact(compat);
    run_compat.addArg("compat/cases");
    const compat_step = b.step("compat", "Run the shared compatibility corpus");
    compat_step.dependOn(&run_compat.step);

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

    const storage_fixture_promote_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/storage_fixture_promote.zig"),
        .target = target,
        .optimize = optimize,
    });
    const storage_fixture_promote = b.addExecutable(.{
        .name = "storage_fixture_promote",
        .root_module = storage_fixture_promote_mod,
    });
    b.installArtifact(storage_fixture_promote);

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
    b.installArtifact(merge_cycle);

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
    b.installArtifact(merge_cost);

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
    b.installArtifact(hbc_parity);

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
    b.installArtifact(hbc_bench);

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
    b.installArtifact(hbc_write_bench);

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
    b.installArtifact(hbc_read_bench);

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
    b.installArtifact(hbc_isolate);

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
    b.installArtifact(dense_stack_bench);

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
    replay_bench_build_options.addOption(bool, "swarm_runtime_focused_test", false);
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
    b.installArtifact(replay_bench);

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
    b.installArtifact(batch_bench);

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
    b.installArtifact(docid_write_bench);

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
    b.installArtifact(docid_query_bench);

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
    algebraic_bench_root_mod.addImport("antfly_vellum", vellum_mod);
    algebraic_bench_root_mod.addImport("antfly_regex", regex_mod);
    algebraic_bench_root_mod.addImport("antfly_platform", platform_mod);
    algebraic_bench_root_mod.addImport("antfly_reranking", reranking_mod);
    addSnowballModule(b, algebraic_bench_root_mod);
    algebraic_bench_mod.addImport("antfly-zig", algebraic_bench_root_mod);

    const algebraic_bench = b.addExecutable(.{
        .name = "algebraic_bench",
        .root_module = algebraic_bench_mod,
    });
    b.installArtifact(algebraic_bench);

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
    b.installArtifact(algebraic_summary);
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
    b.installArtifact(rw_lock_bench);

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
    b.installArtifact(open_bench);

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
    b.installArtifact(artifact_rebuild_bench);

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
    b.installArtifact(provisioned_warmup_bench);

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
    b.installArtifact(public_query_guardrail);

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
    const public_query_guardrail_step = b.step("public-query-guardrail", "Benchmark the public /api/v1/tables/<table>/query path against direct DB search and health responsiveness");
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
    b.installArtifact(raft_apply_bench);

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
    b.installArtifact(managed_host_wal_bench);

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
    b.installArtifact(dense_profile_summary);

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
    b.installArtifact(lmdb_commit_compare);

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
    b.installArtifact(hbc_split_bench);

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
    b.installArtifact(sparse_split_bench);

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
    b.installArtifact(rabitq_bench);

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
    b.installArtifact(recall_harness);

    const run_recall_harness = b.addRunArtifact(recall_harness);
    if (b.args) |args| {
        run_recall_harness.addArgs(args);
    }
    const recall_harness_step = b.step("recall-harness", "Run Zig recall suites against exported vector datasets");
    recall_harness_step.dependOn(&run_recall_harness.step);

    const antfly_main_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/main.zig"),
        .target = target,
        .optimize = optimize,
    });
    antfly_main_mod.addImport("antfly-zig", lib_mod);
    antfly_main_mod.addImport("antfly-client", antfly_client_pkg_mod);
    antfly_main_mod.addImport("httpx", httpx_mod);
    antfly_main_mod.addImport("antfly_vellum", vellum_mod);
    antfly_main_mod.addImport("raft_engine", raft_engine_mod);
    antfly_main_mod.addImport("structlog", structlog_mod);
    antfly_main_mod.addImport("antfly_platform", platform_mod);
    antfly_main_mod.addOptions("build_options", build_options);

    const antfly_main = b.addExecutable(.{
        .name = "antfly",
        .root_module = antfly_main_mod,
    });
    const install_antfly = b.addInstallArtifact(antfly_main, .{ .dest_sub_path = antfly_bin_name });
    const install_antfarm_assets = b.addInstallDirectory(.{
        .source_dir = b.path("../go/pkg/antfly/src/metadata/antfarm"),
        .install_dir = .prefix,
        .install_subdir = "share/antfly/antfarm",
    });
    b.getInstallStep().dependOn(&install_antfly.step);
    b.getInstallStep().dependOn(&install_antfarm_assets.step);

    const run_antfly = b.addRunArtifact(antfly_main);
    if (b.args) |args| {
        run_antfly.addArgs(args);
    }
    const antfly_step = b.step("antfly", "Run the top-level Antfly CLI");
    antfly_step.dependOn(&run_antfly.step);
    const install_antfly_step = b.step("install-antfly", "Build and install the top-level Antfly CLI");
    install_antfly_step.dependOn(&install_antfly.step);
    install_antfly_step.dependOn(&install_antfarm_assets.step);

    const run_recall_harness_default = b.addRunArtifact(recall_harness);
    run_recall_harness_default.addArgs(&.{
        "--dataset-dir",
        "testdata/vectorsets",
    });
    dependOnAll(antfly_test_step, &.{
        unit_test_step,
        sim_test_step,
        integration_test_step,
        recall_test_step,
        &run_recall_harness_default.step,
        chaos_test_step,
    });

    dependOnAll(test_step, &.{
        antfly_test_step,
        delegated_termite_steps.termite_test,
    });

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
    b.installArtifact(hbc_trace);

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
    b.installArtifact(hbc_leaf_debug);

    const run_hbc_leaf_debug = b.addRunArtifact(hbc_leaf_debug);
    if (b.args) |args| {
        run_hbc_leaf_debug.addArgs(args);
    }
    const hbc_leaf_debug_step = b.step("hbc-leaf-debug", "Inspect cached versus fresh quantized HBC leaf scoring");
    hbc_leaf_debug_step.dependOn(&run_hbc_leaf_debug.step);
}
