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
const antfly_benches_build = @import("pkg/antfly/build/benches.zig");
const antfly_embedded_build = @import("pkg/antfly/build/embedded.zig");
const antfly_storage_build = @import("pkg/antfly/build/storage.zig");
const antfly_tests_build = @import("pkg/antfly/build/tests.zig");
const inference_runtime_build = @import("pkg/inference/build/runtime.zig");

const LmdbBackend = antfly_storage_build.LmdbBackend;
const chainLabeledRun = antfly_tests_build.chainLabeledRun;
const configureEmbeddedModule = antfly_embedded_build.configureModule;
const lmdb_c_flags = antfly_storage_build.lmdb_c_flags;
const makeLmdbBuildOptions = antfly_storage_build.makeLmdbBuildOptions;
const makeLmdbEngineModule = antfly_storage_build.makeLmdbEngineModule;
const makeLmdbModule = antfly_storage_build.makeLmdbModule;
const makeRootBuildOptions = antfly_storage_build.makeRootBuildOptions;
const selectTestFilters = antfly_tests_build.selectTestFilters;

const BuildEdition = enum {
    full,
    inference,
};

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
const sql_grammar_source = "pkg/antfly/src/sql/grammar/antfly_sql.y";
const sql_grammar_generated_root = "pkg/antfly/src/sql/grammar/generated/root.zig";

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
        std.zig.system.darwin.getSdk(b.allocator, b.graph.io, &target.result) orelse
        b.graph.environ_map.get("SDK_PATH") orelse
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
    "gliner2-production-readiness",
    "test-finetune",
    "test",
    "wasm",
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
    enable_system_blas: bool,
    blas_root: ?[]const u8,
) DelegatedInferenceBuildSteps {
    var test_step: ?*std.Build.Step = null;
    var finetune_test_step: ?*std.Build.Step = null;
    for (inference_delegated_steps) |step_name| {
        const delegated = addDelegatedPackageStep(b, "inference", "pkg/inference", step_name, "pkg/inference");
        const run = delegated.run;
        addDelegatedInferenceOptions(b, run, enable_metal, enable_onnx, onnx_root, enable_cuda, cuda_artifacts, enable_system_blas, blas_root);
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

    const import_table = [_]struct { name: []const u8, field: []const u8 }{
        .{ .name = "lmdb_engine", .field = "lmdb_engine" },
        .{ .name = "raft_engine", .field = "raft_engine" },
        .{ .name = "antfly_public_openapi", .field = "public_openapi" },
        .{ .name = "antfly_client_openapi", .field = "client_openapi" },
        .{ .name = "antfly_schema_openapi", .field = "schema_openapi" },
        .{ .name = "antfly_indexes_openapi", .field = "indexes_openapi" },
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
        .{ .name = "antfly_platform", .field = "platform" },
    };

    pub fn configure(self: @This(), b: *std.Build, mod: *std.Build.Module, include_lmdb_c: bool, link_libc: bool) void {
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

fn addYaccTestStep(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
) *std.Build.Step {
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
    return yacc_test_step;
}

fn addSqlGrammarRegenStep(b: *std.Build, yacc_codegen: *std.Build.Step.Compile) *std.Build.Step {
    const regen_step = b.step("regen-sql-grammar", "Regenerate checked-in Antfly SQL grammar metadata");
    const run = b.addRunArtifact(yacc_codegen);
    run.addFileArg(b.path(sql_grammar_source));
    run.addArg(sql_grammar_generated_root);
    run.addArg(sql_grammar_source);
    regen_step.dependOn(&run.step);
    return regen_step;
}

fn addSqlGrammarGeneratedCheckStep(b: *std.Build, yacc_codegen: *std.Build.Step.Compile) *std.Build.Step {
    const check_step = b.step("sql-grammar-generated-check", "Check checked-in Antfly SQL grammar metadata is current");
    const run = b.addRunArtifact(yacc_codegen);
    run.addFileArg(b.path(sql_grammar_source));
    const generated = run.addOutputFileArg("sql_grammar_root.zig");
    run.addArg(sql_grammar_source);

    const fmt = b.addSystemCommand(&.{
        b.graph.zig_exe,
        "fmt",
    });
    fmt.addFileArg(generated);

    const compare_tool = addFileCompareTool(b);
    const compare = b.addRunArtifact(compare_tool);
    compare.step.dependOn(&fmt.step);
    compare.addFileArg(generated);
    compare.addFileArg(b.path(sql_grammar_generated_root));
    check_step.dependOn(&compare.step);
    return check_step;
}

fn addDirCompareTool(b: *std.Build) *std.Build.Step.Compile {
    return b.addExecutable(.{
        .name = "check-dirs-equal",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tools/check_dirs_equal.zig"),
            .target = b.graph.host,
            .optimize = .Debug,
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

fn addSnowballRegenStep(b: *std.Build) *std.Build.Step {
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
    return regen_step;
}

fn addSnowballCheckStep(b: *std.Build) *std.Build.Step {
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

    const snowball_check_step = b.step("snowball-check", "Check checked-in Zig Snowball stemmers are current");
    snowball_check_step.dependOn(check_step);
    return snowball_check_step;
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

const antfly_openapi_generated_root = "pkg/antfly/src/openapi/generated";
const inference_openapi_generated_root = "pkg/inference/src/api/generated";

const OpenApiGeneratedMode = union(enum) {
    regen: struct {
        fmt: *std.Build.Step.Run,
    },
    check: struct {
        fmt: *std.Build.Step.Run,
        compare: *std.Build.Step.Run,
    },
};

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

fn addOpenApiCheckOutput(
    b: *std.Build,
    openapi_codegen: *std.Build.Step.Compile,
    source_path: std.Build.LazyPath,
    package_name: []const u8,
    output_dir_name: []const u8,
    generate_what: []const u8,
    import_mappings: []const [2][]const u8,
) std.Build.LazyPath {
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
    return codegen.addOutputDirectoryArg(output_dir_name);
}

fn addOpenApiGeneratedPackage(
    b: *std.Build,
    openapi_codegen: *std.Build.Step.Compile,
    source_path: std.Build.LazyPath,
    package_name: []const u8,
    generated_dir: []const u8,
    generate_what: []const u8,
    import_mappings: []const [2][]const u8,
    mode: OpenApiGeneratedMode,
) void {
    switch (mode) {
        .regen => |regen| {
            const run = addOpenApiRegenRun(b, openapi_codegen, source_path, package_name, generated_dir, generate_what, import_mappings);
            regen.fmt.step.dependOn(&run.step);
        },
        .check => |check| {
            const generated = addOpenApiCheckOutput(b, openapi_codegen, source_path, package_name, b.fmt("check_{s}", .{package_name}), generate_what, import_mappings);
            check.fmt.addFileArg(generated);
            check.compare.addFileArg(generated);
            check.compare.addFileArg(b.path(generated_dir));
        },
    }
}

fn addOpenApiGeneratedPackages(
    b: *std.Build,
    openapi_codegen: *std.Build.Step.Compile,
    mode: OpenApiGeneratedMode,
) void {
    addOpenApiGeneratedPackage(b, openapi_codegen, addJoinedPublicOpenApiSpec(b), "antfly_public_openapi", antfly_openapi_generated_root ++ "/antfly_public_openapi", "types,extractors", &.{
        .{ "specs/openapi/antfly/schema.yaml", "antfly_schema_openapi" },
        .{ "specs/openapi/antfly/indexes.yaml", "antfly_indexes_openapi" },
        .{ "specs/openapi/antfly/generating.yaml", "antfly_generating_api_openapi" },
        .{ "specs/openapi/antfly/eval.yaml", "antfly_eval_openapi" },
        .{ "specs/openapi/shared/generating.yaml", "antfly_generating_openapi" },
        .{ "specs/openapi/antfly/reranking.yaml", "antfly_reranking_openapi" },
        .{ "specs/openapi/antfly/query.yaml", "antfly_query_openapi" },
    }, mode);
    addOpenApiGeneratedPackage(b, openapi_codegen, addPrefixedPublicOpenApiSpec(b), "antfly_client_openapi", antfly_openapi_generated_root ++ "/antfly_client_openapi", "types,client", &.{
        .{ "specs/openapi/antfly/schema.yaml", "antfly_schema_openapi" },
        .{ "specs/openapi/antfly/indexes.yaml", "antfly_indexes_openapi" },
        .{ "specs/openapi/antfly/generating.yaml", "antfly_generating_api_openapi" },
        .{ "specs/openapi/antfly/eval.yaml", "antfly_eval_openapi" },
        .{ "specs/openapi/shared/generating.yaml", "antfly_generating_openapi" },
        .{ "specs/openapi/antfly/reranking.yaml", "antfly_reranking_openapi" },
        .{ "specs/openapi/antfly/query.yaml", "antfly_query_openapi" },
    }, mode);
    addOpenApiGeneratedPackage(b, openapi_codegen, b.path("../specs/openapi/antfly/schema.yaml"), "antfly_schema_openapi", antfly_openapi_generated_root ++ "/antfly_schema_openapi", "types", &.{}, mode);
    addOpenApiGeneratedPackage(b, openapi_codegen, b.path("../specs/openapi/antfly/indexes.yaml"), "antfly_indexes_openapi", antfly_openapi_generated_root ++ "/antfly_indexes_openapi", "types", &.{
        .{ "embeddings.yaml", "antfly_embeddings_openapi" },
        .{ "../shared/generating.yaml", "antfly_generating_openapi" },
        .{ "chunking.yaml", "antfly_chunking_openapi" },
    }, mode);
    addOpenApiGeneratedPackage(b, openapi_codegen, b.path("../specs/openapi/antfly/websearch.yaml"), "antfly_websearch_openapi", antfly_openapi_generated_root ++ "/antfly_websearch_openapi", "types", &.{
        .{ "../shared/s3.yaml", "antfly_s3_openapi" },
    }, mode);
    addOpenApiGeneratedPackage(b, openapi_codegen, b.path("../specs/openapi/antfly/eval.yaml"), "antfly_eval_openapi", antfly_openapi_generated_root ++ "/antfly_eval_openapi", "types", &.{
        .{ "../shared/generating.yaml", "antfly_generating_openapi" },
    }, mode);
    addOpenApiGeneratedPackage(b, openapi_codegen, b.path("../specs/openapi/antfly/query.yaml"), "antfly_query_openapi", antfly_openapi_generated_root ++ "/antfly_query_openapi", "types", &.{}, mode);
    addOpenApiGeneratedPackage(b, openapi_codegen, b.path("../specs/openapi/antfly/admin.yaml"), "antfly_admin_openapi", antfly_openapi_generated_root ++ "/antfly_admin_openapi", "types,server", &.{}, mode);
    addOpenApiGeneratedPackage(b, openapi_codegen, b.path("../specs/openapi/antfly/internal.yaml"), "antfly_internal_openapi", antfly_openapi_generated_root ++ "/antfly_internal_openapi", "types,server", &.{}, mode);
    addOpenApiGeneratedPackage(b, openapi_codegen, b.path("../specs/openapi/auth/api.yaml"), "antfly_usermgr_openapi", antfly_openapi_generated_root ++ "/antfly_usermgr_openapi", "types,server", &.{}, mode);
    addOpenApiGeneratedPackage(b, openapi_codegen, b.path("../specs/openapi/antfly/metadata.yaml"), "antfly_metadata_openapi", antfly_openapi_generated_root ++ "/antfly_metadata_openapi", "types,server", &.{
        .{ "../auth/api.yaml", "antfly_usermgr_openapi" },
        .{ "indexes.yaml", "antfly_indexes_openapi" },
        .{ "schema.yaml", "antfly_schema_openapi" },
        .{ "generating.yaml", "antfly_generating_api_openapi" },
        .{ "eval.yaml", "antfly_eval_openapi" },
        .{ "../shared/generating.yaml", "antfly_generating_openapi" },
        .{ "reranking.yaml", "antfly_reranking_openapi" },
        .{ "query.yaml", "antfly_query_openapi" },
    }, mode);
    addOpenApiGeneratedPackage(b, openapi_codegen, b.path("../specs/openapi/shared/logging.yaml"), "antfly_logging_openapi", antfly_openapi_generated_root ++ "/antfly_logging_openapi", "types", &.{}, mode);
    addOpenApiGeneratedPackage(b, openapi_codegen, b.path("../specs/openapi/antfly/audio.yaml"), "antfly_audio_openapi", antfly_openapi_generated_root ++ "/antfly_audio_openapi", "types", &.{
        .{ "../shared/s3.yaml", "antfly_s3_openapi" },
    }, mode);
    addOpenApiGeneratedPackage(b, openapi_codegen, b.path("../go/pkg/antfly/lib/middleware/openapi.yaml"), "antfly_middleware_openapi", antfly_openapi_generated_root ++ "/antfly_middleware_openapi", "types", &.{}, mode);
    addOpenApiGeneratedPackage(b, openapi_codegen, b.path("../specs/openapi/shared/scraping.yaml"), "antfly_scraping_openapi", antfly_openapi_generated_root ++ "/antfly_scraping_openapi", "types", &.{}, mode);
    addOpenApiGeneratedPackage(b, openapi_codegen, b.path("../specs/openapi/shared/s3.yaml"), "antfly_s3_openapi", antfly_openapi_generated_root ++ "/antfly_s3_openapi", "types", &.{}, mode);
    addOpenApiGeneratedPackage(b, openapi_codegen, b.path("../specs/openapi/inference/config.yaml"), "antfly_inference_config_openapi", antfly_openapi_generated_root ++ "/antfly_inference_config_openapi", "types", &.{
        .{ "../shared/chunking.yaml", "antfly_chunking_api_openapi" },
        .{ "../shared/scraping.yaml", "antfly_scraping_openapi" },
        .{ "../shared/s3.yaml", "antfly_s3_openapi" },
        .{ "../shared/logging.yaml", "antfly_logging_openapi" },
        .{ "../shared/generating.yaml", "antfly_generating_openapi" },
    }, mode);
    addOpenApiGeneratedPackage(b, openapi_codegen, b.path("../specs/openapi/shared/chunking.yaml"), "antfly_chunking_api_openapi", antfly_openapi_generated_root ++ "/antfly_chunking_api_openapi", "types", &.{}, mode);
    addOpenApiGeneratedPackage(b, openapi_codegen, b.path("../specs/openapi/antfly/chunking.yaml"), "antfly_chunking_openapi", antfly_openapi_generated_root ++ "/antfly_chunking_openapi", "types", &.{
        .{ "../shared/chunking.yaml", "antfly_chunking_api_openapi" },
    }, mode);
    addOpenApiGeneratedPackage(b, openapi_codegen, b.path("../specs/openapi/antfly/embeddings.yaml"), "antfly_embeddings_openapi", antfly_openapi_generated_root ++ "/antfly_embeddings_openapi", "types", &.{}, mode);
    addOpenApiGeneratedPackage(b, openapi_codegen, b.path("../specs/openapi/antfly/config.yaml"), "antfly_common_openapi", antfly_openapi_generated_root ++ "/antfly_common_openapi", "types", &.{
        .{ "../shared/logging.yaml", "antfly_logging_openapi" },
        .{ "audio.yaml", "antfly_audio_openapi" },
        .{ "../../../go/pkg/antfly/lib/middleware/openapi.yaml", "antfly_middleware_openapi" },
        .{ "embeddings.yaml", "antfly_embeddings_openapi" },
        .{ "../shared/generating.yaml", "antfly_generating_openapi" },
        .{ "reranking.yaml", "antfly_reranking_openapi" },
        .{ "chunking.yaml", "antfly_chunking_openapi" },
        .{ "../shared/scraping.yaml", "antfly_scraping_openapi" },
        .{ "../shared/s3.yaml", "antfly_s3_openapi" },
        .{ "../inference/config.yaml", "antfly_inference_config_openapi" },
    }, mode);
    addOpenApiGeneratedPackage(b, openapi_codegen, b.path("../specs/openapi/shared/generating.yaml"), "antfly_generating_openapi", antfly_openapi_generated_root ++ "/antfly_generating_openapi", "types", &.{}, mode);
    addOpenApiGeneratedPackage(b, openapi_codegen, b.path("../specs/openapi/antfly/reranking.yaml"), "antfly_reranking_openapi", antfly_openapi_generated_root ++ "/antfly_reranking_openapi", "types", &.{}, mode);
    addOpenApiGeneratedPackage(b, openapi_codegen, b.path("../specs/openapi/ai/extraction.yaml"), "antfly_extraction_openapi", antfly_openapi_generated_root ++ "/antfly_extraction_openapi", "types", &.{
        .{ "../shared/generating.yaml", "antfly_generating_openapi" },
    }, mode);
    addOpenApiGeneratedPackage(b, openapi_codegen, b.path("../specs/openapi/antfly/generating.yaml"), "antfly_generating_api_openapi", antfly_openapi_generated_root ++ "/antfly_generating_api_openapi", "types", &.{
        .{ "../shared/generating.yaml", "antfly_generating_openapi" },
        .{ "websearch.yaml", "antfly_websearch_openapi" },
    }, mode);
    addOpenApiGeneratedPackage(b, openapi_codegen, b.path("../specs/openapi/inference/api.yaml"), "inference_api", inference_openapi_generated_root ++ "/inference_api", "types,server", &.{
        .{ "../shared/generating.yaml", "antfly_generating_openapi" },
        .{ "../ai/extraction.yaml", "antfly_extraction_openapi" },
    }, mode);
    addOpenApiGeneratedPackage(b, openapi_codegen, b.path("specs/openai-openapi.yaml"), "openai_api", antfly_openapi_generated_root ++ "/openai_api", "types", &.{}, mode);
}

fn addOpenApiRegenStep(
    b: *std.Build,
    openapi_codegen: *std.Build.Step.Compile,
) *std.Build.Step {
    const regen_step = b.step("regen-openapi", "Regenerate checked-in Zig OpenAPI modules");
    const fmt = b.addSystemCommand(&.{
        b.graph.zig_exe,
        "fmt",
        antfly_openapi_generated_root,
        inference_openapi_generated_root,
    });
    addOpenApiGeneratedPackages(b, openapi_codegen, .{ .regen = .{ .fmt = fmt } });
    regen_step.dependOn(&fmt.step);

    const openapi_generate_step = b.step("openapi-generate", "Regenerate checked-in Zig OpenAPI modules");
    openapi_generate_step.dependOn(regen_step);
    return openapi_generate_step;
}

fn addOpenApiGeneratedCheckStep(
    b: *std.Build,
    openapi_codegen: *std.Build.Step.Compile,
) *std.Build.Step {
    const check_step = b.step("openapi-generated-check", "Check checked-in Zig OpenAPI modules are current");
    const dir_compare_tool = addDirCompareTool(b);
    const fmt = b.addSystemCommand(&.{ b.graph.zig_exe, "fmt" });
    const compare = b.addRunArtifact(dir_compare_tool);

    addOpenApiGeneratedPackages(b, openapi_codegen, .{ .check = .{
        .fmt = fmt,
        .compare = compare,
    } });
    compare.step.dependOn(&fmt.step);
    check_step.dependOn(&compare.step);
    return check_step;
}

pub fn build(b: *std.Build) void {
    // Keep focused test inventories in pkg/antfly/build/tests.zig. build.zig
    // wires durable suites and modules; it should not grow exact test-title
    // lists as API/storage refactors move coverage closer to implementation.
    antfly_tests_build.assertBuildZigDoesNotOwnTestInventory(b);
    antfly_tests_build.assertDBRefactorBoundary(b);

    // On Linux, an implicit native target can cause Zig 0.16.0 to discover and
    // link against the host distro's crt startup objects. Newer glibc/binutils
    // builds may include .sframe sections with relocation types that Zig's
    // linker cannot yet handle. Defaulting Linux builds to an explicit GNU
    // target keeps user-supplied -Dtarget overrides intact while making the
    // no-argument path use Zig's bundled libc startup objects.
    const default_target: std.Target.Query = if (builtin.os.tag == .linux)
        .{ .cpu_arch = builtin.cpu.arch, .os_tag = .linux, .abi = .gnu }
    else
        .{};
    const target = b.standardTargetOptions(.{ .default_target = default_target });
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
    const edition = b.option(BuildEdition, "edition", "Build edition: full or inference") orelse .full;
    const antfly_bin_name = b.option([]const u8, "antfly-bin-name", "Installed filename for the top-level Antfly CLI") orelse "antfly";
    if (antfly_bin_name.len == 0 or std.mem.indexOfAny(u8, antfly_bin_name, "/\\") != null) {
        @panic("-Dantfly-bin-name must be a non-empty filename, not a path");
    }
    if (!link_libc and lmdb_backend == .c) {
        @panic("-Dlink-libc=false requires -Dlmdb_backend=zig");
    }
    const termite_onnx_option = b.option(bool, "onnx", "Enable ONNX Runtime support for embedded inference");
    const termite_enable_onnx = if (link_libc)
        termite_onnx_option orelse false
    else
        false;
    const termite_onnx_root_opt = b.option([]const u8, "onnx-root", "Path to ONNX Runtime root for embedded inference");
    const termite_onnx_root = termite_onnx_root_opt orelse defaultInferenceOnnxRoot(b, target);
    const termite_enable_metal = if (link_libc)
        b.option(bool, "metal", "Enable Apple Metal kernels for embedded inference") orelse (target.result.os.tag == .macos)
    else
        false;
    const termite_enable_cuda = b.option(bool, "cuda", "Enable CUDA inference support through the NVIDIA Driver API") orelse false;
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
    const lite_local_inference_runtime = b.option(bool, "lite-local-inference-runtime", "Advertise an embedded local inference runtime in Antfly Lite status") orelse false;
    if (termite_enable_onnx) {
        const termite_onnx_available = pathExists(b, b.fmt("{s}/include/onnxruntime_c_api.h", .{termite_onnx_root})) and
            pathExists(b, b.fmt("{s}/lib", .{termite_onnx_root}));
        if (!termite_onnx_available) {
            @panic("-Donnx=true requires an ONNX Runtime install; pass -Donnx-root=<path>");
        }
    }
    const delegated_inference_steps = addDelegatedInferenceBuildSteps(
        b,
        termite_enable_metal,
        termite_enable_onnx,
        termite_onnx_root,
        termite_enable_cuda,
        termite_cuda_artifacts,
        termite_enable_system_blas,
        termite_blas_root,
    );

    const lmdb_build_options = makeLmdbBuildOptions(b, lmdb_backend, lmdb_evented_async_io, false);
    const build_options = makeRootBuildOptions(b, lmdb_backend, lmdb_evented_async_io, false, with_tla, link_libc, false, lite_local_inference_runtime, antfly_version);
    const swarm_runtime_build_options = makeRootBuildOptions(b, lmdb_backend, lmdb_evented_async_io, false, with_tla, link_libc, true, lite_local_inference_runtime, antfly_version);
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
    const snowball_regen_step = addSnowballRegenStep(b);
    const snowball_check_step = addSnowballCheckStep(b);
    const openapi_codegen = addLocalOpenApiCodegen(b, target, optimize, httpx_mod);
    const yacc_codegen = addLocalYaccCodegen(b, target, optimize);
    _ = addYaccTestStep(b, target, optimize);
    const openapi_generate_step = addOpenApiRegenStep(b, openapi_codegen);
    const openapi_generated_check_step = addOpenApiGeneratedCheckStep(b, openapi_codegen);
    const sql_grammar_regen_step = addSqlGrammarRegenStep(b, yacc_codegen);
    const snowball_sources_available = pathExists(b, "deps/snowball/zig/env.zig") and pathExists(b, "deps/snowball/compiler/analyser.c");

    const generate_step = b.step("generate", "Regenerate checked-in Zig generated artifacts");
    generate_step.dependOn(openapi_generate_step);
    generate_step.dependOn(sql_grammar_regen_step);
    if (snowball_sources_available) {
        generate_step.dependOn(snowball_regen_step);
    }
    const openapi_root_check = addOpenApiRootCheckStep(b);
    const antfly_generated_root = "pkg/antfly/src/openapi/generated";
    const public_openapi_mod = addCommittedOpenApiModule(b, target, optimize, "antfly_public_openapi", antfly_generated_root ++ "/antfly_public_openapi");
    const client_openapi_mod = addCommittedOpenApiModuleWithHttpx(b, target, optimize, "antfly_client_openapi", antfly_generated_root ++ "/antfly_client_openapi", httpx_mod);
    const schema_openapi_mod = addCommittedOpenApiModule(b, target, optimize, "antfly_schema_openapi", antfly_generated_root ++ "/antfly_schema_openapi");
    const indexes_openapi_mod = addCommittedOpenApiModule(b, target, optimize, "antfly_indexes_openapi", antfly_generated_root ++ "/antfly_indexes_openapi");
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
    websearch_openapi_mod.addImport("antfly_s3_openapi", s3_openapi_mod);
    eval_openapi_mod.addImport("antfly_generating_openapi", generating_openapi_mod);
    generating_api_openapi_mod.addImport("antfly_generating_openapi", generating_openapi_mod);
    generating_api_openapi_mod.addImport("antfly_websearch_openapi", websearch_openapi_mod);
    public_openapi_mod.addImport("antfly_schema_openapi", schema_openapi_mod);
    public_openapi_mod.addImport("antfly_indexes_openapi", indexes_openapi_mod);
    public_openapi_mod.addImport("antfly_generating_api_openapi", generating_api_openapi_mod);
    public_openapi_mod.addImport("antfly_eval_openapi", eval_openapi_mod);
    public_openapi_mod.addImport("antfly_generating_openapi", generating_openapi_mod);
    public_openapi_mod.addImport("antfly_reranking_openapi", reranking_openapi_mod);
    public_openapi_mod.addImport("antfly_query_openapi", query_openapi_mod);
    client_openapi_mod.addImport("antfly_schema_openapi", schema_openapi_mod);
    client_openapi_mod.addImport("antfly_indexes_openapi", indexes_openapi_mod);
    client_openapi_mod.addImport("antfly_generating_api_openapi", generating_api_openapi_mod);
    client_openapi_mod.addImport("antfly_eval_openapi", eval_openapi_mod);
    client_openapi_mod.addImport("antfly_generating_openapi", generating_openapi_mod);
    client_openapi_mod.addImport("antfly_reranking_openapi", reranking_openapi_mod);
    client_openapi_mod.addImport("antfly_query_openapi", query_openapi_mod);
    metadata_openapi_mod.addImport("antfly_usermgr_openapi", usermgr_openapi_mod);
    metadata_openapi_mod.addImport("antfly_indexes_openapi", indexes_openapi_mod);
    metadata_openapi_mod.addImport("antfly_schema_openapi", schema_openapi_mod);
    metadata_openapi_mod.addImport("antfly_generating_api_openapi", generating_api_openapi_mod);
    metadata_openapi_mod.addImport("antfly_eval_openapi", eval_openapi_mod);
    metadata_openapi_mod.addImport("antfly_generating_openapi", generating_openapi_mod);
    metadata_openapi_mod.addImport("antfly_reranking_openapi", reranking_openapi_mod);
    metadata_openapi_mod.addImport("antfly_query_openapi", query_openapi_mod);
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
    const a2a_mod = b.addModule("antfly_a2a", .{
        .root_source_file = b.path("lib/a2a/src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
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
    const ml_tabular_mod = b.addModule("ml_tabular", .{
        .root_source_file = b.path("lib/ml/tabular/src/root.zig"),
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

    const inference_graph = inference_runtime_build.create(.{
        .b = b,
        .target = target,
        .optimize = optimize,
        .paths = .{
            .inference_root = "pkg/inference",
            .shared_lib_root = "",
        },
        .backend = .{
            .enable_onnx = termite_enable_onnx,
            .onnx_root = termite_onnx_root,
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
            .jinja = termite_jinja_mod,
            .protobuf = protobuf_mod,
            .sentencepiece_proto = sentencepiece_proto_mod,
            .ml = termite_ml_mod,
            .ml_tabular = ml_tabular_mod,
            .onnx_graph = termite_onnx_graph_mod,
            .pjrt = termite_pjrt_mod,
            .generating_openapi = generating_openapi_mod,
        },
    });
    const inference_build_options_mod = inference_graph.build_options_mod;
    const inference_api_mod = inference_graph.inference_api_mod;
    inference_api_mod.addImport("antfly_generating_openapi", generating_openapi_mod);
    inference_api_mod.addImport("antfly_extraction_openapi", extraction_openapi_mod);
    const inference_hf_tokenizer_mod = inference_graph.inference_hf_tokenizer_mod;
    const inference_fixed_tokenizer_data_mod = inference_graph.inference_fixed_tokenizer_data_mod;
    const inference_chunker_mod = inference_graph.inference_chunker_mod;
    const inference_server_mod = inference_graph.inference_mod;

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
    const readers_mod = b.createModule(.{
        .root_source_file = b.path("lib/readers/src/mod.zig"),
        .target = target,
        .optimize = optimize,
    });
    readers_mod.addImport("httpx", httpx_mod);
    readers_mod.addImport("inference_api", inference_api_mod);
    readers_mod.addImport("antfly_google", google_mod);
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

    const api_query_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/api_query_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    antfly_imports.configure(b, api_query_test_mod, true, true);

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

    const lite_capi_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/capi/db.zig"),
        .target = target,
        .optimize = optimize,
    });
    lite_capi_mod.addImport("antfly-zig", lib_mod);
    lite_capi_mod.addImport("structlog", structlog_mod);

    const lite_capi_lib = b.addLibrary(.{
        .linkage = .dynamic,
        .name = "antfly",
        .root_module = lite_capi_mod,
    });
    const install_lite_capi_lib = b.addInstallArtifact(lite_capi_lib, .{});
    const install_lite_capi_header = b.addInstallFileWithDir(
        b.path("pkg/antfly/include/antfly.h"),
        .header,
        "antfly.h",
    );

    const capi_step = b.step("capi", "Build the Zig C API shared libraries");
    capi_step.dependOn(&install_capi_lib.step);
    capi_step.dependOn(&install_lite_capi_lib.step);
    capi_step.dependOn(&install_lite_capi_header.step);

    const lite_capi_step = b.step("lite-capi", "Build the libantfly C ABI shared library");
    lite_capi_step.dependOn(&install_lite_capi_lib.step);
    lite_capi_step.dependOn(&install_lite_capi_header.step);

    const lite_capi_smoke_mod = b.createModule(.{
        .target = target,
        .optimize = optimize,
    });
    lite_capi_smoke_mod.link_libc = true;
    lite_capi_smoke_mod.addIncludePath(b.path("pkg/antfly/include"));
    lite_capi_smoke_mod.addCSourceFile(.{
        .file = b.path("examples/antfly_lite_c_smoke.c"),
        .flags = &.{ "-std=c11", "-Wall", "-Wextra", "-Werror" },
    });
    const lite_capi_smoke = b.addExecutable(.{
        .name = "antfly-lite-c-smoke",
        .root_module = lite_capi_smoke_mod,
    });
    lite_capi_smoke.root_module.linkLibrary(lite_capi_lib);
    const run_lite_capi_smoke = b.addRunArtifact(lite_capi_smoke);
    const lite_capi_smoke_step = b.step("lite-capi-smoke", "Compile and run a C consumer smoke test for libantfly");
    lite_capi_smoke_step.dependOn(&run_lite_capi_smoke.step);

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
    run_lite_go_tests.step.dependOn(&install_lite_capi_lib.step);
    run_lite_go_tests.step.dependOn(&install_lite_capi_header.step);
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
    run_lite_go_example.step.dependOn(&install_lite_capi_lib.step);
    run_lite_go_example.step.dependOn(&install_lite_capi_header.step);
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
    run_lite_go_retrieval_template.step.dependOn(&install_lite_capi_lib.step);
    run_lite_go_retrieval_template.step.dependOn(&install_lite_capi_header.step);
    const lite_go_retrieval_template_step = b.step("lite-go-retrieval-template", "Run the embedded Go Antfly Lite retrieval template");
    lite_go_retrieval_template_step.dependOn(&run_lite_go_retrieval_template.step);

    const run_cabi_packaging_tests = b.addSystemCommand(&.{
        "env",
        "PYTHONPYCACHEPREFIX=/tmp/antfly-pycache",
        "python3",
        "scripts/packaging/test_cabi_packaging.py",
    });
    run_cabi_packaging_tests.setCwd(b.path(".."));
    const lite_package_test_step = b.step("lite-package-test", "Run Antfly C ABI release packaging regression tests");
    lite_package_test_step.dependOn(&run_cabi_packaging_tests.step);

    const capi_test = antfly_tests_build.addModuleTestStep(b, capi_mod, "capi-test", "Run C API tests", .{
        .filters = &antfly_tests_build.capi_default_filters,
        .simple_runner = true,
    });
    const run_capi_tests = capi_test.run;

    const fuzz_tabular_loader_mod = b.createModule(.{
        .root_source_file = b.path("lib/ml/tabular/src/fuzz_loader.zig"),
        .target = target,
        .optimize = optimize,
    });

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

    const api_json_helpers_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/api/json_helpers.zig"),
        .target = target,
        .optimize = optimize,
    });
    api_json_helpers_test_mod.addImport("antfly-json", json_mod);

    const api_artifact_reprocess_jobs_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/api_artifact_reprocess_jobs_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    antfly_imports.configure(b, api_artifact_reprocess_jobs_test_mod, true, true);
    const run_api_artifact_reprocess_jobs_tests = antfly_tests_build.addModuleTestStep(
        b,
        api_artifact_reprocess_jobs_test_mod,
        "lib-api-artifact-reprocess-jobs-test",
        "Run artifact reprocess job store tests",
        .{
            .filters = &antfly_tests_build.ArtifactReprocessJobTestFilters.store,
            .select_filters = false,
            .simple_runner = true,
        },
    ).run;

    const image_test_mod = b.createModule(.{
        .root_source_file = b.path("lib/image/image_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    image_test_mod.addImport("antfly_image", image_mod);

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

    const lib_image_conformance_test_mod = b.createModule(.{
        .root_source_file = b.path("lib/image/src/mod.zig"),
        .target = target,
        .optimize = optimize,
    });
    const lib_image_conformance_tests = b.addTest(.{
        .root_module = lib_image_conformance_test_mod,
        .filters = selectTestFilters(b, &antfly_tests_build.PackageTestFilters.image_conformance),
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

    const run_lib_generating_runtime_tests = antfly_tests_build.addModuleTestStep(
        b,
        lib_test_mod,
        "lib-generating-runtime-test",
        "Run generating backend adapter tests",
        .{ .filters = &antfly_tests_build.PackageTestFilters.generating_runtime },
    ).run;

    const run_lib_reranking_runtime_tests = antfly_tests_build.addModuleTestStep(
        b,
        lib_test_mod,
        "lib-reranking-runtime-test",
        "Run reranking backend adapter tests",
        .{ .filters = &antfly_tests_build.PackageTestFilters.reranking_runtime },
    ).run;

    const run_lib_common_tests = antfly_tests_build.addModuleTestStep(
        b,
        lib_test_mod,
        "lib-common-test",
        "Run common/provider registry tests",
        .{ .filters = &antfly_tests_build.PackageTestFilters.common },
    ).run;

    const run_lib_common_config_tests = antfly_tests_build.addModuleTestStep(
        b,
        lib_test_mod,
        "lib-common-config-test",
        "Run common/config tests",
        .{
            .filters = &antfly_tests_build.PackageTestFilters.common_config,
            .simple_runner = true,
        },
    ).run;

    const embedded_test_run = antfly_tests_build.addModuleTestStep(
        b,
        embedded_mod,
        "embedded-test",
        "Run embedded API tests",
        .{ .filters = &antfly_tests_build.PackageTestFilters.embedded },
    );
    const run_embedded_tests = embedded_test_run.run;

    const antfly_embedded_pkg_tests = b.addTest(.{
        .root_module = antfly_embedded_pkg_mod,
        .filters = selectTestFilters(b, &antfly_tests_build.PackageTestFilters.antfly_embedded_root),
    });
    const run_antfly_embedded_pkg_tests = b.addRunArtifact(antfly_embedded_pkg_tests);
    const antfly_embedded_db_pkg_tests = b.addTest(.{
        .root_module = antfly_embedded_db_pkg_mod,
        .filters = selectTestFilters(b, &antfly_tests_build.PackageTestFilters.antfly_embedded_db),
    });
    const run_antfly_embedded_db_pkg_tests = b.addRunArtifact(antfly_embedded_db_pkg_tests);
    const antfly_embedded_api_pkg_tests = b.addTest(.{
        .root_module = antfly_embedded_api_pkg_mod,
        .filters = selectTestFilters(b, &antfly_tests_build.PackageTestFilters.antfly_embedded_api),
    });
    const run_antfly_embedded_api_pkg_tests = b.addRunArtifact(antfly_embedded_api_pkg_tests);
    const antfly_embedded_pkg_test_step = b.step("antfly-embedded-test", "Run the standalone antfly-embedded package compile test");
    antfly_embedded_pkg_test_step.dependOn(&run_antfly_embedded_pkg_tests.step);
    antfly_embedded_pkg_test_step.dependOn(&run_antfly_embedded_db_pkg_tests.step);
    antfly_embedded_pkg_test_step.dependOn(&run_antfly_embedded_api_pkg_tests.step);

    const run_antfly_client_pkg_tests = antfly_tests_build.addModuleTestStep(
        b,
        antfly_client_pkg_mod,
        "antfly-client-test",
        "Run the standalone antfly-client package compile test",
        .{ .filters = &antfly_tests_build.PackageTestFilters.antfly_client },
    ).run;

    const root_test_skip_filters = antfly_tests_build.RootTestFilters.skip;
    const unit_progress_skip_filters = antfly_tests_build.RootTestFilters.unit_progress_skip;
    const root_module_tests = antfly_tests_build.addRootTestStep(b, lib_test_mod);
    const root_test_step = root_module_tests.step;

    const lake_scaffold_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/lake_scaffold_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    const lake_scaffold_transcribing_stub_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/testing/transcribing_stub.zig"),
        .target = target,
        .optimize = optimize,
    });
    lake_scaffold_transcribing_stub_mod.addImport("httpx", httpx_mod);
    lake_scaffold_test_mod.addImport("objectstore", objectstore_mod);
    lake_scaffold_test_mod.addImport("antfly_vector", vector_mod);
    lake_scaffold_test_mod.addImport("raft_engine", raft_engine_mod);
    lake_scaffold_test_mod.addImport("antfly_pdf", pdf_mod);
    lake_scaffold_test_mod.addImport("antfly_scraping", scraping_mod);
    lake_scaffold_test_mod.addImport("antfly_platform", platform_mod);
    lake_scaffold_test_mod.addImport("handlebars", handlebars_mod);
    lake_scaffold_test_mod.addImport("antfly_transcribing", lake_scaffold_transcribing_stub_mod);
    const lake_scaffold_tests = b.addTest(.{
        .root_module = lake_scaffold_test_mod,
    });
    const run_lake_scaffold_tests = b.addRunArtifact(lake_scaffold_tests);
    const lake_scaffold_test_step = b.step("lake-scaffold-test", "Run Antfly-owned lake-native scaffold tests");
    lake_scaffold_test_step.dependOn(&run_lake_scaffold_tests.step);
    root_test_step.dependOn(&run_lake_scaffold_tests.step);

    const graph_metric_tests = antfly_tests_build.addGraphMetricTestSteps(b, .{
        .root = lib_test_mod,
        .query_fan_in = api_query_test_mod,
    }, &root_test_skip_filters);
    const lite_native_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/lite_native_test.zig"),
        .target = target,
        .optimize = optimize,
    });
    antfly_imports.configure(b, lite_native_test_mod, true, true);
    const run_lite_native_tests = antfly_tests_build.addModuleTestStep(
        b,
        lite_native_test_mod,
        "lite-native-test",
        "Run Lite native backend tests",
        .{
            .filters = &antfly_tests_build.PackageTestFilters.lite_native,
            .simple_runner = true,
        },
    ).run;

    const lite_cli_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/lite_cli_test.zig"),
        .target = target,
        .optimize = optimize,
    });
    lite_cli_test_mod.addImport("antfly-zig", lib_mod);
    lite_cli_test_mod.addImport("antfly-client", antfly_client_pkg_mod);
    lite_cli_test_mod.addImport("httpx", httpx_mod);
    lite_cli_test_mod.addImport("antfly_vellum", vellum_mod);
    lite_cli_test_mod.addImport("raft_engine", raft_engine_mod);
    lite_cli_test_mod.addImport("structlog", structlog_mod);
    lite_cli_test_mod.addImport("antfly_platform", platform_mod);
    lite_cli_test_mod.addImport("handlebars", handlebars_mod);
    lite_cli_test_mod.addOptions("build_options", build_options);
    const run_lite_cli_tests = antfly_tests_build.addModuleTestStep(
        b,
        lite_cli_test_mod,
        "lite-cli-test",
        "Run Antfly Lite CLI tests",
        .{
            .filters = &antfly_tests_build.PackageTestFilters.lite_cli,
            .simple_runner = true,
        },
    ).run;

    const recall_test_run = antfly_tests_build.addModuleTestStep(
        b,
        lib_test_mod,
        "recall-test",
        "Run HBC vector recall quality tests",
        .{
            .filters = &antfly_tests_build.RecallTestFilters.hbc,
            .simple_runner = true,
        },
    );
    const recall_test_step = recall_test_run.step;

    const raft_unit_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = selectTestFilters(b, &antfly_tests_build.RaftTestFilters.root),
    });
    const run_raft_unit_tests = b.addRunArtifact(raft_unit_tests);

    const raft_transport_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = &antfly_tests_build.RaftTestFilters.transport,
    });
    const run_raft_transport_tests = b.addRunArtifact(raft_transport_tests);

    const lib_raft_sim_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = &antfly_tests_build.RaftTestFilters.sim,
    });
    const run_lib_raft_sim_tests = b.addRunArtifact(lib_raft_sim_tests);
    const lib_raft_sim_test_step = b.step("lib-raft-sim-test", "Run raft simulation harness tests");
    lib_raft_sim_test_step.dependOn(&run_lib_raft_sim_tests.step);

    const lib_raft_chaos_tests = b.addTest(.{
        .root_module = lib_test_mod,
        .filters = &antfly_tests_build.RaftTestFilters.chaos,
    });
    const run_lib_raft_chaos_tests = b.addRunArtifact(lib_raft_chaos_tests);
    const lib_raft_chaos_test_step = b.step("lib-raft-chaos-test", "Run longer raft restart/HTTP simulation campaigns");
    lib_raft_chaos_test_step.dependOn(&run_lib_raft_chaos_tests.step);

    const run_lib_lsm_backend_sim_tests = antfly_tests_build.addModuleTestStep(
        b,
        lib_test_mod,
        "lib-lsm-backend-sim-test",
        "Run LSM backend storage workload simulation tests",
        .{ .filters = &antfly_tests_build.PackageTestFilters.lsm_backend_sim },
    ).run;

    const lib_lsm_backend_chaos_test_run = antfly_tests_build.addModuleTestStep(
        b,
        lib_test_mod,
        "lib-lsm-backend-chaos-test",
        "Run longer LSM backend compaction chaos campaigns",
        .{ .filters = &antfly_tests_build.PackageTestFilters.lsm_backend_chaos },
    );
    const lib_lsm_backend_chaos_tests = lib_lsm_backend_chaos_test_run.tests;
    const lib_lsm_backend_chaos_test_step = lib_lsm_backend_chaos_test_run.step;
    const ha_chaos_test_run = antfly_tests_build.addModuleTestStep(
        b,
        lib_test_mod,
        "ha-chaos-test",
        "Run HA hot-standby crash and partition hardening tests",
        .{ .filters = &antfly_tests_build.HATestFilters.chaos },
    );
    const lib_ha_chaos_tests = ha_chaos_test_run.tests;
    const run_lib_ha_compat_tests = antfly_tests_build.addModuleTestStep(
        b,
        lib_test_mod,
        "ha-compat-test",
        "Run HA replication format compatibility tests",
        .{ .filters = &antfly_tests_build.HATestFilters.compat },
    ).run;

    const test_step = b.step("test", "Run default package test aggregates");
    const antfly_test_step = b.step("antfly-test", "Run default Antfly unit, simulation, integration, chaos, and recall checks");
    const conformance_test_step = b.step("conformance-test", "Fetch and run conformance suites");
    const promotion_test_step = b.step("promotion-test", "Run explicit promotion and release qualification suites");
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

    const lib_db_test = antfly_tests_build.addDBRootTestStep(b, lib_test_mod);

    const serverless_test_run = antfly_tests_build.addModuleTestStep(
        b,
        lib_test_mod,
        "serverless-test",
        "Run serverless and serverless transport tests",
        .{
            .filters = &antfly_tests_build.PackageTestFilters.serverless,
            .simple_runner = true,
        },
    );
    const serverless_tests = serverless_test_run.tests;
    const run_serverless_tests = serverless_test_run.run;

    const run_lib_data_runtime_tests = antfly_tests_build.addModuleTestStep(
        b,
        data_runtime_test_mod,
        "lib-data-runtime-test",
        "Run focused data runtime tests",
        .{
            .filters = &antfly_tests_build.DataTestFilters.runtime,
            .simple_runner = true,
        },
    ).run;

    const run_lib_data_storage_tests = antfly_tests_build.addModuleTestStep(
        b,
        data_storage_test_mod,
        "lib-data-storage-test",
        "Run focused data storage tests",
        .{
            .filters = &antfly_tests_build.DataTestFilters.storage,
            .simple_runner = true,
        },
    ).run;

    const lib_db_module_tests = antfly_tests_build.addDBRootModuleTestSteps(b, lib_test_mod);

    const metadata_fk_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/metadata_fk_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    antfly_imports.configure(b, metadata_fk_test_mod, true, true);
    const metadata_tests = antfly_tests_build.addMetadataTestSteps(b, lib_test_mod, metadata_fk_test_mod);
    const lib_metadata_tests = metadata_tests.root.tests;
    const run_lib_metadata_tests = metadata_tests.root.run;
    const run_lib_metadata_sim_smoke_tests = metadata_tests.sim_smoke.run;
    const run_lib_metadata_vopr_tests = metadata_tests.vopr.run;
    const lib_metadata_vopr_chaos_tests = metadata_tests.vopr_chaos.tests;
    const run_lib_metadata_vopr_chaos_tests = metadata_tests.vopr_chaos.run;
    const lib_metadata_transition_chaos_test_step = metadata_tests.chaos.transition;
    const lib_metadata_public_chaos_test_step = metadata_tests.chaos.public;
    const run_lib_metadata_sim_public_tests = metadata_tests.sim_public.run;

    const api_focused_tests = antfly_tests_build.addAPIFocusedTestSteps(b, lib_test_mod, &openapi_root_check.step);
    const run_public_api_parity_tests = api_focused_tests.public_api_parity.run;
    const run_public_api_graph_metric_e2e_tests = api_focused_tests.public_api_graph_metric_e2e.run;
    const run_lib_api_auth_tests = api_focused_tests.auth.run;
    const run_lib_api_logic_tests = api_focused_tests.logic.run;

    // Keep API tests wired at stable suite granularity. Leaf implementation
    // tests should join these roots via pkg/antfly/build/tests.zig filters,
    // not by adding one top-level build step per regression.
    const api_docid_tests = antfly_tests_build.addAPIDocIdTestRootSteps(b, .{
        .root = lib_test_mod,
        .target = target,
        .optimize = optimize,
    }, antfly_imports);
    const docid_lifecycle_test_step = antfly_tests_build.addAPIDocIdLifecycleDependencies(
        api_focused_tests.docid_lifecycle,
        api_docid_tests,
        lib_db_module_tests.result_shape,
    );

    const docid_operational_hardening_test_step = b.step("docid-operational-hardening-test", "Run extended DOCID lifecycle, metadata chaos, and compaction hardening tests");
    docid_operational_hardening_test_step.dependOn(docid_lifecycle_test_step);
    docid_operational_hardening_test_step.dependOn(lib_metadata_transition_chaos_test_step);
    docid_operational_hardening_test_step.dependOn(lib_metadata_public_chaos_test_step);
    docid_operational_hardening_test_step.dependOn(lib_lsm_backend_chaos_test_step);

    _ = antfly_tests_build.addAPIDocIdAggregateTestStep(b, api_docid_tests, .{
        .data_storage = run_lib_data_storage_tests,
        .data_runtime = run_lib_data_runtime_tests,
        .metadata_sim_smoke = run_lib_metadata_sim_smoke_tests,
        .metadata_sim_public = run_lib_metadata_sim_public_tests,
        .metadata_vopr = run_lib_metadata_vopr_tests,
        .metadata_vopr_chaos = run_lib_metadata_vopr_chaos_tests,
        .metadata_public_chaos = lib_metadata_public_chaos_test_step,
        .db_result_shape = lib_db_module_tests.result_shape,
    });

    const openapi_root_check_step = b.step("openapi-root-check", "Check that the bundled root OpenAPI spec matches the modular Zig specs");
    openapi_root_check_step.dependOn(&openapi_root_check.step);
    const openapi_check_step = b.step("openapi-check", "Check checked-in OpenAPI artifacts are current");
    openapi_check_step.dependOn(openapi_root_check_step);
    openapi_check_step.dependOn(openapi_generated_check_step);

    const generated_check_step = b.step("generated-check", "Check checked-in Zig generated artifacts are current");
    generated_check_step.dependOn(openapi_check_step);
    generated_check_step.dependOn(addSqlGrammarGeneratedCheckStep(b, yacc_codegen));
    if (snowball_sources_available) {
        generated_check_step.dependOn(snowball_check_step);
    }
    antfly_tests_build.dependOnAPIGeneratedChecks(generated_check_step, api_docid_tests);

    const run_lib_metadata_sim_forward_tests = metadata_tests.sim_forward.run;
    const run_lib_metadata_service_tests = metadata_tests.service.run;
    const run_lib_metadata_logic_tests = metadata_tests.logic.run;

    const storage_tests = antfly_tests_build.addStorageTestSteps(b, lib_test_mod, &unit_progress_skip_filters);
    const run_lib_storage_tests = storage_tests.root.run;
    const run_ha_tests = storage_tests.ha.run;
    const lib_storage_progress_tests = storage_tests.progress.tests;
    const run_lsm_backend_tests = storage_tests.lsm_backend.run;
    const run_resource_budget_tests = storage_tests.resource_budget.run;

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
    chaos_progress_tail = chainLabeledRun(b, lib_ha_chaos_tests, "ha-chaos-test", chaos_progress_tail);
    chaos_test_step.dependOn(chaos_progress_tail.?);

    const chaos_soak_test_step = b.step("chaos-soak-test", "Run broad legacy metadata and raft chaos simulation soaks");
    var chaos_soak_progress_tail: ?*std.Build.Step = null;
    chaos_soak_progress_tail = antfly_tests_build.chainMetadataChaosSoakTests(b, lib_test_mod, chaos_soak_progress_tail);
    chaos_soak_progress_tail = chainLabeledRun(b, lib_raft_chaos_tests, "lib-raft-chaos-test", chaos_soak_progress_tail);
    chaos_soak_test_step.dependOn(chaos_soak_progress_tail.?);
    soak_test_step.dependOn(chaos_soak_test_step);

    const template_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/template_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    antfly_imports.configure(b, template_test_mod, false, true);
    const standalone_module_tests = antfly_tests_build.addStandaloneModuleTestSteps(b, .{
        .regex = regex_mod,
        .jsonschema = jsonschema_mod,
        .json = json_mod,
        .ml_tabular = ml_tabular_mod,
        .fuzz_tabular_loader = fuzz_tabular_loader_mod,
        .toon = toon_mod,
        .mcp = mcp_mod,
        .a2a = a2a_mod,
        .matcher = matcher_mod,
        .resolver = resolver_mod,
        .httpx_json = httpx_json_test_mod,
        .httpx = httpx_mod,
        .api_json_helpers = api_json_helpers_test_mod,
        .generating = generating_mod,
        .embeddings = embeddings_mod,
        .vectorindex = vectorindex_mod,
        .chunking = chunking_mod,
        .readers = readers_mod,
        .extracting = extracting_mod,
        .image = image_test_mod,
        .reranking = reranking_mod,
        .casbin = casbin_mod,
        .usermgr = usermgr_mod,
        .template = template_test_mod,
    });

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
    lib_audio_misc_conformance.root_module.addImport("build_options", inference_build_options_mod);
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
        .filters = &antfly_tests_build.SwarmRuntimeTestFilters.focused,
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const lib_swarm_runtime_test_step = b.step("lib-swarm-runtime-test", "Run focused swarm runtime tests");
    const run_lib_swarm_runtime_tests = b.addRunArtifact(lib_swarm_runtime_tests);
    lib_swarm_runtime_test_step.dependOn(&run_lib_swarm_runtime_tests.step);

    const raft_test_step = b.step("raft-test", "Run raft integration unit tests");
    raft_test_step.dependOn(&run_raft_unit_tests.step);

    const raft_transport_test_step = b.step("raft-transport-test", "Run raft transport unit tests");
    raft_transport_test_step.dependOn(&run_raft_transport_tests.step);

    unit_test_step.dependOn(&standalone_module_tests.regex.run.step);
    unit_test_step.dependOn(&standalone_module_tests.jsonschema.run.step);
    unit_test_step.dependOn(&standalone_module_tests.generating.run.step);
    unit_test_step.dependOn(&standalone_module_tests.embeddings.run.step);
    unit_test_step.dependOn(&standalone_module_tests.vectorindex.run.step);
    unit_test_step.dependOn(&standalone_module_tests.chunking.run.step);
    unit_test_step.dependOn(&run_lib_generating_runtime_tests.step);
    unit_test_step.dependOn(&standalone_module_tests.reranking.run.step);
    unit_test_step.dependOn(&run_lib_reranking_runtime_tests.step);
    unit_test_step.dependOn(&run_lib_common_tests.step);
    unit_test_step.dependOn(&run_lib_common_config_tests.step);
    unit_test_step.dependOn(&standalone_module_tests.casbin.run.step);
    unit_test_step.dependOn(&standalone_module_tests.usermgr.run.step);
    unit_test_step.dependOn(&run_embedded_tests.step);
    unit_test_step.dependOn(&run_antfly_embedded_pkg_tests.step);
    unit_test_step.dependOn(&run_capi_tests.step);
    unit_test_step.dependOn(&run_lite_native_tests.step);
    unit_test_step.dependOn(&run_lite_cli_tests.step);
    unit_test_step.dependOn(&lib_db_test.run.step);
    unit_test_step.dependOn(&lib_db_module_tests.result_shape.step);
    unit_test_step.dependOn(&run_serverless_tests.step);
    unit_test_step.dependOn(&run_lib_data_runtime_tests.step);
    unit_test_step.dependOn(&run_lib_data_storage_tests.step);
    unit_test_step.dependOn(&run_lib_metadata_logic_tests.step);
    unit_test_step.dependOn(&run_lib_metadata_service_tests.step);
    antfly_tests_build.dependOnAPIDocIdUnitTestRuns(unit_test_step, api_docid_tests);
    unit_test_step.dependOn(&run_lib_api_auth_tests.step);
    unit_test_step.dependOn(&run_lib_api_logic_tests.step);
    unit_test_step.dependOn(&run_api_artifact_reprocess_jobs_tests.step);
    unit_test_step.dependOn(&run_public_api_parity_tests.step);
    unit_test_step.dependOn(&standalone_module_tests.template.run.step);
    unit_test_step.dependOn(&standalone_module_tests.toon.run.step);
    unit_test_step.dependOn(&standalone_module_tests.mcp.run.step);
    unit_test_step.dependOn(&standalone_module_tests.a2a.run.step);
    unit_test_step.dependOn(&standalone_module_tests.image.run.step);
    unit_test_step.dependOn(&run_lib_audio_tests.step);
    unit_test_step.dependOn(delegated_inference_steps.inference_test);
    unit_test_step.dependOn(delegated_inference_steps.inference_finetune_test);
    unit_test_step.dependOn(lib_swarm_runtime_test_step);
    unit_test_step.dependOn(&run_ha_tests.step);
    unit_test_step.dependOn(&run_raft_unit_tests.step);
    unit_test_step.dependOn(&run_raft_transport_tests.step);

    var unit_progress_tail: ?*std.Build.Step = null;
    unit_progress_tail = chainLabeledRun(b, lib_storage_progress_tests, "lib-storage-test", unit_progress_tail);
    unit_progress_tail = chainLabeledRun(b, lib_db_test.tests, antfly_tests_build.db_root_step_name, unit_progress_tail);
    unit_progress_tail = chainLabeledRun(b, lib_metadata_tests, "lib-metadata-test", unit_progress_tail);
    unit_progress_tail = chainLabeledRun(b, raft_unit_tests, "raft-test", unit_progress_tail);
    unit_progress_tail = chainLabeledRun(b, raft_transport_tests, "raft-transport-test", unit_progress_tail);
    unit_progress_tail = chainLabeledRun(b, serverless_tests, "serverless-test", unit_progress_tail);
    unit_progress_tail = chainLabeledRun(b, standalone_module_tests.template.tests, "lib-template-test", unit_progress_tail);
    unit_test_progress_step.dependOn(unit_progress_tail.?);

    const storage_lmdb_test_mod = makeLmdbModule(b, "pkg/antfly/src/storage/lmdb.zig", target, optimize, build_options, lmdb_engine_mod, platform_mod);

    const storage_sim_runtime_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/storage_sim_runtime_root.zig"),
        .target = target,
        .optimize = optimize,
    });

    const storage_lmdb_soak_build_options = makeLmdbBuildOptions(b, lmdb_backend, lmdb_evented_async_io, true);
    const storage_lmdb_soak_engine_mod = makeLmdbEngineModule(b, target, optimize, true, storage_lmdb_soak_build_options);
    const storage_lmdb_soak_test_mod = makeLmdbModule(b, "pkg/antfly/src/storage/lmdb.zig", target, optimize, storage_lmdb_soak_build_options, storage_lmdb_soak_engine_mod, platform_mod);

    const docstore_test_mod = makeLmdbModule(b, "pkg/antfly/src/docstore_test_root.zig", target, optimize, build_options, lmdb_engine_mod, platform_mod);
    docstore_test_mod.addImport("bloom", bloom_mod);
    docstore_test_mod.addImport("antfly_vellum", vellum_mod);
    docstore_test_mod.addImport("antfly_regex", regex_mod);

    const shard_test_mod = makeLmdbModule(b, "pkg/antfly/src/shard_test_root.zig", target, optimize, build_options, lmdb_engine_mod, platform_mod);
    shard_test_mod.addImport("bloom", bloom_mod);
    shard_test_mod.addImport("antfly_vellum", vellum_mod);
    shard_test_mod.addImport("antfly_regex", regex_mod);

    const wal_test_mod = makeLmdbModule(b, "pkg/antfly/src/wal_test_root.zig", target, optimize, build_options, lmdb_engine_mod, platform_mod);
    wal_test_mod.addImport("bloom", bloom_mod);
    wal_test_mod.addImport("antfly_vellum", vellum_mod);
    wal_test_mod.addImport("antfly_regex", regex_mod);
    wal_test_mod.addImport("structlog", structlog_mod);

    const wal_soak_build_options = makeLmdbBuildOptions(b, lmdb_backend, lmdb_evented_async_io, true);
    const wal_soak_engine_mod = makeLmdbEngineModule(b, target, optimize, true, wal_soak_build_options);
    const wal_soak_test_mod = makeLmdbModule(b, "pkg/antfly/src/wal_test_root.zig", target, optimize, wal_soak_build_options, wal_soak_engine_mod, platform_mod);
    wal_soak_test_mod.addImport("bloom", bloom_mod);
    wal_soak_test_mod.addImport("antfly_vellum", vellum_mod);
    wal_soak_test_mod.addImport("antfly_regex", regex_mod);

    const persistent_test_mod = makeLmdbModule(b, "pkg/antfly/src/persistent_test_root.zig", target, optimize, build_options, lmdb_engine_mod, platform_mod);
    persistent_test_mod.addImport("bloom", bloom_mod);
    persistent_test_mod.addImport("antfly_vellum", vellum_mod);
    persistent_test_mod.addImport("antfly_regex", regex_mod);
    persistent_test_mod.addImport("antfly_vector", vector_mod);
    persistent_test_mod.addImport("antfly_vectorindex", vectorindex_mod);
    persistent_test_mod.addImport("antfly_reranking", reranking_mod);
    persistent_test_mod.addImport("structlog", structlog_mod);

    const persistent_soak_build_options = makeLmdbBuildOptions(b, lmdb_backend, lmdb_evented_async_io, true);
    const persistent_soak_engine_mod = makeLmdbEngineModule(b, target, optimize, true, persistent_soak_build_options);
    const persistent_soak_test_mod = makeLmdbModule(b, "pkg/antfly/src/persistent_test_root.zig", target, optimize, persistent_soak_build_options, persistent_soak_engine_mod, platform_mod);
    persistent_soak_test_mod.addImport("bloom", bloom_mod);
    persistent_soak_test_mod.addImport("antfly_vellum", vellum_mod);
    persistent_soak_test_mod.addImport("antfly_regex", regex_mod);
    persistent_soak_test_mod.addImport("antfly_vector", vector_mod);
    persistent_soak_test_mod.addImport("antfly_vectorindex", vectorindex_mod);
    persistent_soak_test_mod.addImport("antfly_reranking", reranking_mod);

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
    index_manager_test_mod.addImport("structlog", structlog_mod);

    const db_test_mod = makeLmdbModule(b, "pkg/antfly/src/db_test_root.zig", target, optimize, build_options, lmdb_engine_mod, platform_mod);
    const transcribing_db_test_stub_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/testing/transcribing_stub.zig"),
        .target = target,
        .optimize = optimize,
    });
    transcribing_db_test_stub_mod.addImport("httpx", httpx_mod);
    antfly_imports.configure(b, db_test_mod, false, link_libc);
    db_test_mod.addImport("antfly_transcribing", transcribing_db_test_stub_mod);
    const usermgr_storage_db_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/usermgr/storage_imports.zig"),
        .target = target,
        .optimize = optimize,
    });
    usermgr_storage_db_test_mod.addImport("antfly_root", db_test_mod);
    usermgr_storage_db_test_mod.addImport("antfly_platform", platform_mod);
    db_test_mod.addImport("usermgr_storage", usermgr_storage_db_test_mod);

    const db_storage_tests = antfly_tests_build.addDBStorageTestSteps(b, db_test_mod);
    sim_test_step.dependOn(&db_storage_tests.sim.step);

    const sparse_test_mod = makeLmdbModule(b, "pkg/antfly/src/sparse_test_root.zig", target, optimize, build_options, lmdb_engine_mod, platform_mod);
    sparse_test_mod.addImport("bloom", bloom_mod);
    sparse_test_mod.addImport("antfly_vellum", vellum_mod);
    sparse_test_mod.addImport("antfly_regex", regex_mod);

    const derived_log_test_mod = makeLmdbModule(b, "pkg/antfly/src/derived_log_test_root.zig", target, optimize, build_options, lmdb_engine_mod, platform_mod);
    derived_log_test_mod.addImport("bloom", bloom_mod);
    derived_log_test_mod.addImport("antfly_vellum", vellum_mod);
    derived_log_test_mod.addImport("antfly_regex", regex_mod);
    const storage_backend_tests = antfly_tests_build.addStorageBackendTestSteps(b, .{
        .lmdb_engine = lmdb_engine_mod,
        .storage_lmdb = storage_lmdb_test_mod,
        .storage_lmdb_soak = storage_lmdb_soak_test_mod,
        .storage_sim_runtime = storage_sim_runtime_test_mod,
        .docstore = docstore_test_mod,
        .shard = shard_test_mod,
        .wal = wal_test_mod,
        .wal_soak = wal_soak_test_mod,
        .persistent = persistent_test_mod,
        .persistent_soak = persistent_soak_test_mod,
        .index_manager = index_manager_test_mod,
        .sparse = sparse_test_mod,
        .derived_log = derived_log_test_mod,
    }, .{
        .lsm_backend_sim = run_lib_lsm_backend_sim_tests,
    });
    sim_test_step.dependOn(storage_backend_tests.storage_vopr);
    soak_test_step.dependOn(storage_backend_tests.storage_sim_soak);

    // Default Antfly unit coverage is hermetic: no network fetchers, no
    // benchmarks, and no soak/conformance suites that require external corpora.
    // Focused aliases stay available as separate steps; broader module suites
    // are wired here once.
    dependOnAll(unit_test_step, &.{
        &standalone_module_tests.json.run.step,
        &standalone_module_tests.httpx_json.run.step,
        &standalone_module_tests.httpx.run.step,
        &standalone_module_tests.api_json_helpers.run.step,
        &run_antfly_client_pkg_tests.step,
        &root_module_tests.run.step,
        &run_lib_metadata_tests.step,
        &run_lib_storage_tests.step,
        &run_lsm_backend_tests.step,
        &run_resource_budget_tests.step,
        &storage_backend_tests.lmdb.run.step,
        &storage_backend_tests.storage_lmdb.run.step,
        &storage_backend_tests.docstore.run.step,
        &storage_backend_tests.shard.run.step,
        &storage_backend_tests.wal.run.step,
        &storage_backend_tests.persistent.run.step,
        &storage_backend_tests.index_manager.run.step,
        &db_storage_tests.all.step,
        &storage_backend_tests.sparse.run.step,
        &storage_backend_tests.derived_log.run.step,
        &graph_metric_tests.lifecycle.step,
        &graph_metric_tests.query_fan_in.step,
        &graph_metric_tests.cleanup.step,
        &graph_metric_tests.degree_canary.step,
        &graph_metric_tests.default_gate.step,
        &run_public_api_graph_metric_e2e_tests.step,
    });

    const lmdb_bench_engine_options_c = makeLmdbBuildOptions(b, .c, false, false);
    const lmdb_bench_build_options_c = makeRootBuildOptions(b, .c, false, false, false, true, false, lite_local_inference_runtime, antfly_version);
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
    const lmdb_bench_build_options_zig = makeRootBuildOptions(b, .zig, lmdb_evented_async_io, false, false, true, false, lite_local_inference_runtime, antfly_version);
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
    const split_bench_build_options = makeRootBuildOptions(b, lmdb_backend, lmdb_evented_async_io, false, false, true, false, lite_local_inference_runtime, antfly_version);
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

    const sql_parser_bench_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/sql/parser_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    const sql_parser_bench = b.addExecutable(.{
        .name = "sql_parser_bench",
        .root_module = sql_parser_bench_mod,
    });
    const run_sql_parser_bench = b.addRunArtifact(sql_parser_bench);
    if (b.args) |args| {
        run_sql_parser_bench.addArgs(args);
    }
    const sql_parser_bench_step = b.step("sql-parser-bench", "Benchmark generated SQL parser corpus throughput");
    sql_parser_bench_step.dependOn(&run_sql_parser_bench.step);

    const sql_parser_fuzz_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/sql/parser_fuzz.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    const sql_parser_fuzz = b.addExecutable(.{
        .name = "sql_parser_fuzz",
        .root_module = sql_parser_fuzz_mod,
    });
    const run_sql_parser_fuzz = b.addRunArtifact(sql_parser_fuzz);
    if (b.args) |args| {
        run_sql_parser_fuzz.addArgs(args);
    }
    const sql_parser_fuzz_step = b.step("sql-parser-fuzz", "Run deterministic generated SQL parser fuzzing");
    sql_parser_fuzz_step.dependOn(&run_sql_parser_fuzz.step);

    const wal_bench_engine_options = makeLmdbBuildOptions(b, lmdb_backend, lmdb_evented_async_io, false);
    const wal_bench_build_options = makeRootBuildOptions(b, lmdb_backend, lmdb_evented_async_io, false, false, true, false, lite_local_inference_runtime, antfly_version);
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
    const derived_log_bench_build_options = makeRootBuildOptions(b, lmdb_backend, lmdb_evented_async_io, false, false, true, false, lite_local_inference_runtime, antfly_version);
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

    const antfly_main_mod = if (edition == .full) blk: {
        const mod = b.createModule(.{
            .root_source_file = b.path("pkg/antfly/src/main.zig"),
            .target = target,
            .optimize = optimize,
        });
        mod.addImport("antfly-zig", lib_mod);
        mod.addImport("antfly-client", antfly_client_pkg_mod);
        mod.addImport("httpx", httpx_mod);
        mod.addImport("antfly_vellum", vellum_mod);
        mod.addImport("raft_engine", raft_engine_mod);
        mod.addImport("structlog", structlog_mod);
        mod.addImport("antfly_platform", platform_mod);
        mod.addImport("handlebars", handlebars_mod);
        mod.addOptions("build_options", build_options);
        break :blk mod;
    } else blk: {
        const inference_cli_mod = b.createModule(.{
            .root_source_file = b.path("pkg/inference/src/main.zig"),
            .target = target,
            .optimize = optimize,
        });
        inference_cli_mod.addImport("inference", inference_server_mod);
        inference_cli_mod.addImport("build_options", inference_build_options_mod);
        inference_cli_mod.addImport("antfly_platform", platform_mod);
        inference_cli_mod.addImport("structlog", structlog_mod);

        const mod = b.createModule(.{
            .root_source_file = b.path("pkg/antfly/src/inference_main.zig"),
            .target = target,
            .optimize = optimize,
        });
        mod.addImport("inference_cli", inference_cli_mod);
        mod.addImport("antfly_platform", platform_mod);
        mod.addImport("structlog", structlog_mod);
        mod.addOptions("build_options", build_options);
        break :blk mod;
    };

    const antfly_main = b.addExecutable(.{
        .name = "antfly",
        .root_module = antfly_main_mod,
    });
    const graph_metric_maintenance_test_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/cmd_graph_metric_maintenance_test_root.zig"),
        .target = target,
        .optimize = optimize,
    });
    graph_metric_maintenance_test_mod.addImport("antfly-zig", lib_mod);
    graph_metric_maintenance_test_mod.addImport("antfly-client", antfly_client_pkg_mod);
    graph_metric_maintenance_test_mod.addImport("httpx", httpx_mod);
    graph_metric_maintenance_test_mod.addImport("antfly_vellum", vellum_mod);
    graph_metric_maintenance_test_mod.addImport("raft_engine", raft_engine_mod);
    graph_metric_maintenance_test_mod.addImport("structlog", structlog_mod);
    graph_metric_maintenance_test_mod.addImport("antfly_platform", platform_mod);
    graph_metric_maintenance_test_mod.addImport("handlebars", handlebars_mod);
    graph_metric_maintenance_test_mod.addOptions("build_options", build_options);
    const antfly_main_tests = b.addTest(.{
        .root_module = antfly_main_mod,
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_antfly_main_tests = b.addRunArtifact(antfly_main_tests);
    const antfly_main_test_step = b.step("antfly-main-test", "Run top-level Antfly CLI tests");
    antfly_main_test_step.dependOn(&run_antfly_main_tests.step);
    unit_test_step.dependOn(&run_antfly_main_tests.step);

    const graph_metric_operations_command_tests = b.addTest(.{
        .root_module = graph_metric_maintenance_test_mod,
        .filters = &antfly_tests_build.GraphMetricCommandTestFilters.operations,
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_graph_metric_operations_command_tests = b.addRunArtifact(graph_metric_operations_command_tests);
    unit_test_step.dependOn(&graph_metric_tests.operations.step);
    unit_test_step.dependOn(&run_graph_metric_operations_command_tests.step);

    if (edition == .full) {
        const graph_metric_process_harness_mod = b.createModule(.{
            .root_source_file = b.path("pkg/antfly/src/cmd/graph_metric_process_harness.zig"),
            .target = target,
            .optimize = optimize,
        });
        graph_metric_process_harness_mod.addImport("antfly-zig", lib_mod);
        graph_metric_process_harness_mod.addImport("antfly_platform", platform_mod);
        const graph_metric_process_harness = b.addExecutable(.{
            .name = "graph-metric-process-harness",
            .root_module = graph_metric_process_harness_mod,
        });
        graph_metric_process_harness.step.dependOn(&antfly_main.step);
        const run_graph_metric_process_harness = b.addRunArtifact(graph_metric_process_harness);
        run_graph_metric_process_harness.addArtifactArg(antfly_main);
        run_graph_metric_process_harness.addArgs(&.{ "--profile", "promotion" });
        run_graph_metric_process_harness.has_side_effects = true;
        promotion_test_step.dependOn(&run_graph_metric_process_harness.step);
    }

    const install_antfly = b.addInstallArtifact(antfly_main, .{ .dest_sub_path = antfly_bin_name });
    const install_antfarm_assets = b.addInstallDirectory(.{
        .source_dir = b.path("../go/pkg/antfly/src/metadata/antfarm"),
        .install_dir = .prefix,
        .install_subdir = "share/antfly/antfarm",
    });
    b.getInstallStep().dependOn(&install_antfly.step);
    if (edition == .full) {
        b.getInstallStep().dependOn(&install_antfarm_assets.step);
    }

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
        .filters = selectTestFilters(b, &antfly_tests_build.PackageTestFilters.lite_core_main),
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    const run_lite_core_main_tests = b.addRunArtifact(lite_core_main_tests);
    const lite_core_test_step = b.step("lite-core-test", "Run Antfly Lite core wrapper tests");
    lite_core_test_step.dependOn(&run_lite_core_main_tests.step);
    lite_core_test_step.dependOn(&run_lite_cli_tests.step);
    lite_core_test_step.dependOn(&run_lite_native_tests.step);
    lite_core_test_step.dependOn(&run_lite_capi_smoke.step);
    lite_core_test_step.dependOn(&run_lite_go_tests.step);
    lite_core_test_step.dependOn(&run_lite_go_example.step);
    lite_core_test_step.dependOn(&run_lite_go_retrieval_template.step);
    lite_core_test_step.dependOn(&run_lite_core_cli_smoke.step);
    lite_core_test_step.dependOn(&run_antfly_embedded_pkg_tests.step);
    const install_lite_core_main = b.addInstallArtifact(lite_core_main, .{ .dest_sub_path = antfly_bin_name });

    const lite_core_step = b.step("lite-core", "Build Antfly Lite core CLI, embedded package check, and libantfly C ABI");
    lite_core_step.dependOn(&install_lite_core_main.step);
    lite_core_step.dependOn(&install_lite_capi_lib.step);
    lite_core_step.dependOn(&install_lite_capi_header.step);
    lite_core_step.dependOn(&run_lite_core_main_tests.step);
    lite_core_step.dependOn(&run_lite_capi_smoke.step);
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
    lite_full_step.dependOn(&install_lite_capi_lib.step);
    lite_full_step.dependOn(&install_lite_capi_header.step);
    lite_full_step.dependOn(&run_antfly_main_tests.step);
    lite_full_step.dependOn(&run_lite_cli_tests.step);
    lite_full_step.dependOn(&run_lite_native_tests.step);
    lite_full_step.dependOn(&run_lite_capi_smoke.step);
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
    lite_dev_step.dependOn(&install_lite_capi_lib.step);
    lite_dev_step.dependOn(&install_lite_capi_header.step);
    lite_dev_step.dependOn(&run_antfly_main_tests.step);
    lite_dev_step.dependOn(&run_lite_core_main_tests.step);
    lite_dev_step.dependOn(&run_lite_cli_tests.step);
    lite_dev_step.dependOn(&run_lite_native_tests.step);
    lite_dev_step.dependOn(&run_lite_capi_smoke.step);
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

    const run_antfly = b.addRunArtifact(antfly_main);
    if (b.args) |args| {
        run_antfly.addArgs(args);
    }
    const antfly_step = b.step("antfly", "Run the top-level Antfly CLI");
    antfly_step.dependOn(&run_antfly.step);

    const run_recall_harness_default = b.addRunArtifact(recall_harness);
    run_recall_harness_default.stdio = .inherit;
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
        delegated_inference_steps.inference_test,
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
