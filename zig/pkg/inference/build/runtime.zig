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

pub const FfmpegPaths = struct {
    include_dir: []const u8,
    lib_dir: []const u8,
};

pub const BackendOptions = struct {
    enable_onnx: bool = false,
    onnx_root: []const u8 = "onnxruntime/unknown-unknown",
    enable_mlx: bool = false,
    mlx_root: ?[]const u8 = null,
    enable_metal: bool = false,
    enable_cuda: bool = false,
    cuda_artifacts: []const u8 = "portable",
    enable_pjrt: bool = false,
    enable_native: bool = true,
    enable_system_blas: bool = false,
    blas_root: ?[]const u8 = null,
    enable_wasm: bool = false,
    enable_webgpu: bool = false,
    wasm_memory_model: []const u8 = "wasm32",
    enable_ffmpeg_audio: bool = false,
    ffmpeg_paths: ?FfmpegPaths = null,
    link_libc: bool = true,
    skip_openapi: bool = false,
    inference_version: []const u8 = "dev",
    enable_native_quant_dispatch_stats: bool = false,
};

pub const Paths = struct {
    /// Path from the active build.zig directory to pkg/inference.
    inference_root: []const u8,
    /// Path from the active build.zig directory to the monorepo root.
    shared_lib_root: []const u8,
};

pub const SharedModules = struct {
    json: ?*std.Build.Module = null,
    httpx: ?*std.Build.Module = null,
    platform: ?*std.Build.Module = null,
    vellum: ?*std.Build.Module = null,
    scraping: ?*std.Build.Module = null,
    google: ?*std.Build.Module = null,
    objectstore: ?*std.Build.Module = null,
    regex: ?*std.Build.Module = null,
    jsonschema: ?*std.Build.Module = null,
    image: ?*std.Build.Module = null,
    prometheus: ?*std.Build.Module = null,
    structlog: ?*std.Build.Module = null,
    jinja: ?*std.Build.Module = null,
    protobuf: ?*std.Build.Module = null,
    sentencepiece_proto: ?*std.Build.Module = null,
    ml: ?*std.Build.Module = null,
    onnx_graph: ?*std.Build.Module = null,
    pjrt: ?*std.Build.Module = null,
    inference_api: ?*std.Build.Module = null,
    generating_openapi: ?*std.Build.Module = null,
    extraction_openapi: ?*std.Build.Module = null,
    inference_client: ?*std.Build.Module = null,
};

pub const Config = struct {
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    paths: Paths,
    backend: BackendOptions,
    shared: SharedModules = .{},
    register_public_modules: bool = false,
};

pub const Graph = struct {
    build_options: *std.Build.Step.Options,
    build_options_mod: *std.Build.Module,
    audio_open_corpus_build_options_mod: *std.Build.Module,
    json_mod: *std.Build.Module,
    httpx_mod: *std.Build.Module,
    platform_mod: *std.Build.Module,
    vellum_mod: *std.Build.Module,
    scraping_mod: *std.Build.Module,
    objectstore_mod: *std.Build.Module,
    regex_mod: *std.Build.Module,
    jsonschema_mod: *std.Build.Module,
    image_mod: *std.Build.Module,
    prometheus_mod: *std.Build.Module,
    structlog_mod: *std.Build.Module,
    jinja_mod: *std.Build.Module,
    protobuf_mod: *std.Build.Module,
    sentencepiece_proto_mod: *std.Build.Module,
    ml_mod: *std.Build.Module,
    onnx_graph_mod: *std.Build.Module,
    pjrt_mod: *std.Build.Module,
    inference_api_mod: *std.Build.Module,
    inference_client_mod: ?*std.Build.Module,
    inference_tokenizer_mod: *std.Build.Module,
    inference_hf_tokenizer_mod: *std.Build.Module,
    inference_linalg_mod: *std.Build.Module,
    inference_fixed_tokenizer_data_mod: *std.Build.Module,
    inference_audio_mod: *std.Build.Module,
    inference_chunker_mod: *std.Build.Module,
    generating_openapi_mod: *std.Build.Module,
    inference_mod: *std.Build.Module,
    inference_internal_mod: *std.Build.Module,
};

pub fn create(config: Config) Graph {
    const b = config.b;
    const backend = config.backend;
    const shared = config.shared;
    const paths = config.paths;
    const target = config.target;
    const optimize = config.optimize;

    const build_options = addBuildOptions(b, backend);
    const build_options_mod = build_options.createModule();
    const audio_open_corpus_build_options_mod = addAudioOpenCorpusBuildOptions(b, backend).createModule();

    const json_mod = shared.json orelse createSharedModule(config, "lib/json/src/mod.zig");
    const httpx_mod = shared.httpx orelse blk: {
        const mod = createSharedModule(config, "lib/httpx/src/httpx.zig");
        mod.addImport("antfly-json", json_mod);
        break :blk mod;
    };
    const platform_mod = shared.platform orelse createSharedModule(config, "lib/platform/src/root.zig");
    const vellum_mod = shared.vellum orelse createSharedModule(config, "lib/vellum/src/mod.zig");
    const google_mod = shared.google orelse blk: {
        const mod = createSharedModule(config, "lib/google/src/root.zig");
        mod.addImport("httpx", httpx_mod);
        mod.addImport("antfly_platform", platform_mod);
        break :blk mod;
    };
    const objectstore_mod = shared.objectstore orelse blk: {
        const mod = createSharedModule(config, "lib/objectstore/src/root.zig");
        mod.addImport("httpx", httpx_mod);
        mod.addImport("antfly_platform", platform_mod);
        mod.addImport("antfly_google", google_mod);
        break :blk mod;
    };
    const scraping_mod = shared.scraping orelse blk: {
        const mod = createSharedModule(config, "lib/scraping/src/mod.zig");
        mod.addImport("objectstore", objectstore_mod);
        break :blk mod;
    };
    const regex_mod = shared.regex orelse blk: {
        const mod = createSharedModule(config, "lib/regex/src/mod.zig");
        mod.addImport("antfly_vellum", vellum_mod);
        break :blk mod;
    };
    const jsonschema_mod = shared.jsonschema orelse blk: {
        const mod = createSharedModule(config, "lib/jsonschema/src/mod.zig");
        mod.addImport("antfly-json", json_mod);
        mod.addImport("antfly_regex", regex_mod);
        break :blk mod;
    };
    const image_mod = shared.image orelse createSharedModule(config, "lib/image/src/mod.zig");
    const prometheus_mod = shared.prometheus orelse createOptionalSharedModule(config, "lib/prometheus/src/root.zig", "src/compat/prometheus.zig");
    const structlog_mod = shared.structlog orelse createOptionalSharedModule(config, "lib/structlog/src/root.zig", "src/compat/structlog.zig");
    const jinja_mod = shared.jinja orelse b.dependency("jinja", .{
        .target = target,
        .optimize = optimize,
    }).module("jinja");
    const protobuf_dep = b.dependency("protobuf", .{
        .target = target,
        .optimize = optimize,
    });
    const protobuf_mod = shared.protobuf orelse protobuf_dep.module("protobuf");
    const sentencepiece_proto_mod = shared.sentencepiece_proto orelse addSentencePieceProtoModule(b, protobuf_dep, paths, config.register_public_modules);
    const ml_mod = shared.ml orelse createSharedModuleNamed(config, "ml", "lib/ml/src/root.zig");
    const onnx_graph_mod = shared.onnx_graph orelse blk: {
        const mod = b.dependency("onnx_graph", .{
            .target = target,
            .optimize = optimize,
        }).module("onnx");
        mod.addImport("ml", ml_mod);
        break :blk mod;
    };
    const pjrt_mod = shared.pjrt orelse b.dependency("pjrt", .{
        .target = target,
        .optimize = optimize,
    }).module("pjrt");

    const generating_openapi_mod = shared.generating_openapi orelse addOrCreateModule(b, config.register_public_modules, "antfly_generating_openapi", .{
        .root_source_file = b.path(pathJoin(b, paths.shared_lib_root, "pkg/antfly/src/openapi/generated/antfly_generating_openapi/root.zig")),
        .target = target,
        .optimize = optimize,
    });
    var shared_with_generating = shared;
    shared_with_generating.generating_openapi = generating_openapi_mod;
    const inference_api_mod = shared.inference_api orelse addInferenceApiModule(b, target, optimize, httpx_mod, backend.skip_openapi, paths, config.register_public_modules, shared_with_generating);
    const inference_client_mod = shared.inference_client orelse if (!backend.skip_openapi) blk: {
        const mod = addOrCreateModule(b, config.register_public_modules, "inference_client", .{
            .root_source_file = b.path(pathJoin(b, paths.inference_root, "../inference-client/src/root.zig")),
            .target = target,
            .optimize = optimize,
        });
        mod.addImport("httpx", httpx_mod);
        mod.addImport("inference_api", inference_api_mod);
        break :blk mod;
    } else null;

    const inference_tokenizer_mod = addOrCreateModule(b, config.register_public_modules, "inference_tokenizer", .{
        .root_source_file = b.path(pathJoin(b, paths.shared_lib_root, "lib/tokenizer/src/tokenizer.zig")),
        .target = target,
        .optimize = optimize,
    });
    inference_tokenizer_mod.addImport("protobuf", protobuf_mod);
    inference_tokenizer_mod.addImport("sentencepiece_proto", sentencepiece_proto_mod);

    const inference_hf_tokenizer_mod = addOrCreateModule(b, config.register_public_modules, "inference_hf_tokenizer", .{
        .root_source_file = b.path(pathJoin(b, paths.shared_lib_root, "lib/tokenizer/src/hf_root.zig")),
        .target = target,
        .optimize = optimize,
    });
    inference_hf_tokenizer_mod.addImport("inference_tokenizer", inference_tokenizer_mod);

    const inference_linalg_mod = addOrCreateModule(b, config.register_public_modules, "inference_linalg", .{
        .root_source_file = b.path(pathJoin(b, paths.shared_lib_root, "lib/linalg/src/mod.zig")),
        .target = target,
        .optimize = optimize,
    });

    const inference_fixed_tokenizer_data_mod = addTokenizerDataModule(b, paths, config.register_public_modules);

    const inference_audio_mod = addOrCreateModule(b, config.register_public_modules, "inference_audio", .{
        .root_source_file = b.path(pathJoin(b, paths.shared_lib_root, "lib/audio/src/mod.zig")),
        .target = target,
        .optimize = optimize,
    });
    inference_audio_mod.addImport("build_options", build_options_mod);
    if (backend.ffmpeg_paths) |ffmpeg_paths| {
        inference_audio_mod.addIncludePath(.{ .cwd_relative = ffmpeg_paths.include_dir });
    }

    const inference_chunker_mod = addOrCreateModule(b, config.register_public_modules, "inference_chunker", .{
        .root_source_file = b.path(pathJoin(b, paths.shared_lib_root, "lib/chunker/src/mod.zig")),
        .target = target,
        .optimize = optimize,
    });
    inference_chunker_mod.addImport("inference_audio", inference_audio_mod);
    inference_chunker_mod.addImport("inference_hf_tokenizer", inference_hf_tokenizer_mod);
    inference_chunker_mod.addImport("inference_fixed_tokenizer_data", inference_fixed_tokenizer_data_mod);
    inference_chunker_mod.addImport("antfly_image", image_mod);

    const inference_mod = b.createModule(.{
        .root_source_file = b.path(pathJoin(b, paths.inference_root, "src/inference.zig")),
        .target = target,
        .optimize = optimize,
    });
    addInferenceRootImports(inference_mod, .{
        .build_options_mod = build_options_mod,
        .httpx_mod = httpx_mod,
        .inference_api_mod = inference_api_mod,
        .inference_audio_mod = inference_audio_mod,
        .inference_chunker_mod = inference_chunker_mod,
        .jinja_mod = jinja_mod,
        .inference_tokenizer_mod = inference_tokenizer_mod,
        .inference_hf_tokenizer_mod = inference_hf_tokenizer_mod,
        .inference_linalg_mod = inference_linalg_mod,
        .inference_fixed_tokenizer_data_mod = inference_fixed_tokenizer_data_mod,
        .jsonschema_mod = jsonschema_mod,
        .scraping_mod = scraping_mod,
        .image_mod = image_mod,
        .ml_mod = ml_mod,
        .prometheus_mod = prometheus_mod,
        .structlog_mod = structlog_mod,
        .onnx_graph_mod = onnx_graph_mod,
        .pjrt_mod = pjrt_mod,
        .platform_mod = platform_mod,
        .protobuf_mod = protobuf_mod,
        .inference_client_mod = inference_client_mod,
    });
    inference_mod.addImport("antfly_generating_openapi", generating_openapi_mod);
    configureRuntimeLinks(b, inference_mod, target, backend, paths);
    inference_mod.link_libc = backend.link_libc;

    const inference_internal_mod = b.createModule(.{
        .root_source_file = b.path(pathJoin(b, paths.inference_root, "src/inference_internal.zig")),
        .target = target,
        .optimize = optimize,
    });
    inference_internal_mod.addImport("build_options", build_options_mod);
    inference_internal_mod.addImport("jinja", jinja_mod);
    inference_internal_mod.addImport("inference_tokenizer", inference_tokenizer_mod);
    inference_internal_mod.addImport("inference_hf_tokenizer", inference_hf_tokenizer_mod);
    inference_internal_mod.addImport("inference_fixed_tokenizer_data", inference_fixed_tokenizer_data_mod);
    inference_internal_mod.addImport("antfly_image", image_mod);
    inference_internal_mod.addImport("inference_audio", inference_audio_mod);
    inference_internal_mod.addImport("ml", ml_mod);
    inference_internal_mod.addImport("pjrt", pjrt_mod);
    inference_internal_mod.addImport("inference_linalg", inference_linalg_mod);
    inference_internal_mod.addImport("protobuf", protobuf_mod);
    inference_internal_mod.addImport("antfly_platform", platform_mod);
    inference_internal_mod.addImport("onnx_graph", onnx_graph_mod);
    configureOnnxRuntime(b, inference_internal_mod, backend.enable_onnx, backend.onnx_root);

    inference_mod.addImport("inference_internal", inference_mod);

    return .{
        .build_options = build_options,
        .build_options_mod = build_options_mod,
        .audio_open_corpus_build_options_mod = audio_open_corpus_build_options_mod,
        .json_mod = json_mod,
        .httpx_mod = httpx_mod,
        .platform_mod = platform_mod,
        .vellum_mod = vellum_mod,
        .scraping_mod = scraping_mod,
        .objectstore_mod = objectstore_mod,
        .regex_mod = regex_mod,
        .jsonschema_mod = jsonschema_mod,
        .image_mod = image_mod,
        .prometheus_mod = prometheus_mod,
        .structlog_mod = structlog_mod,
        .jinja_mod = jinja_mod,
        .protobuf_mod = protobuf_mod,
        .sentencepiece_proto_mod = sentencepiece_proto_mod,
        .ml_mod = ml_mod,
        .onnx_graph_mod = onnx_graph_mod,
        .pjrt_mod = pjrt_mod,
        .inference_api_mod = inference_api_mod,
        .inference_client_mod = inference_client_mod,
        .inference_tokenizer_mod = inference_tokenizer_mod,
        .inference_hf_tokenizer_mod = inference_hf_tokenizer_mod,
        .inference_linalg_mod = inference_linalg_mod,
        .inference_fixed_tokenizer_data_mod = inference_fixed_tokenizer_data_mod,
        .inference_audio_mod = inference_audio_mod,
        .inference_chunker_mod = inference_chunker_mod,
        .generating_openapi_mod = generating_openapi_mod,
        .inference_mod = inference_mod,
        .inference_internal_mod = inference_internal_mod,
    };
}

pub fn addStandaloneExecutable(b: *std.Build, graph: Graph, target: std.Build.ResolvedTarget, optimize: std.builtin.OptimizeMode, inference_root: []const u8, link_libc: bool) *std.Build.Step.Compile {
    const exe = b.addExecutable(.{
        .name = "antfly-inference",
        .root_module = b.createModule(.{
            .root_source_file = b.path(pathJoin(b, inference_root, "src/main.zig")),
            .target = target,
            .optimize = optimize,
        }),
    });
    exe.root_module.addImport("inference", graph.inference_mod);
    exe.root_module.addImport("build_options", graph.build_options_mod);
    exe.root_module.addImport("structlog", graph.structlog_mod);
    exe.root_module.addImport("antfly_platform", graph.platform_mod);
    exe.root_module.link_libc = link_libc;
    b.installArtifact(exe);
    return exe;
}

const InferenceRootImports = struct {
    build_options_mod: *std.Build.Module,
    httpx_mod: *std.Build.Module,
    inference_api_mod: *std.Build.Module,
    inference_audio_mod: *std.Build.Module,
    inference_chunker_mod: *std.Build.Module,
    jinja_mod: *std.Build.Module,
    inference_tokenizer_mod: *std.Build.Module,
    inference_hf_tokenizer_mod: *std.Build.Module,
    inference_linalg_mod: *std.Build.Module,
    inference_fixed_tokenizer_data_mod: *std.Build.Module,
    jsonschema_mod: *std.Build.Module,
    scraping_mod: *std.Build.Module,
    image_mod: *std.Build.Module,
    ml_mod: *std.Build.Module,
    prometheus_mod: *std.Build.Module,
    structlog_mod: *std.Build.Module,
    onnx_graph_mod: *std.Build.Module,
    pjrt_mod: *std.Build.Module,
    platform_mod: *std.Build.Module,
    protobuf_mod: *std.Build.Module,
    inference_client_mod: ?*std.Build.Module,
};

pub fn addInferenceRootImports(module: *std.Build.Module, imports: InferenceRootImports) void {
    module.addImport("build_options", imports.build_options_mod);
    module.addImport("httpx", imports.httpx_mod);
    module.addImport("inference_api", imports.inference_api_mod);
    module.addImport("inference_audio", imports.inference_audio_mod);
    module.addImport("inference_chunker", imports.inference_chunker_mod);
    module.addImport("jinja", imports.jinja_mod);
    module.addImport("inference_tokenizer", imports.inference_tokenizer_mod);
    module.addImport("inference_hf_tokenizer", imports.inference_hf_tokenizer_mod);
    module.addImport("inference_linalg", imports.inference_linalg_mod);
    module.addImport("inference_fixed_tokenizer_data", imports.inference_fixed_tokenizer_data_mod);
    module.addImport("antfly_jsonschema", imports.jsonschema_mod);
    module.addImport("antfly_scraping", imports.scraping_mod);
    module.addImport("antfly_image", imports.image_mod);
    module.addImport("ml", imports.ml_mod);
    module.addImport("prometheus", imports.prometheus_mod);
    module.addImport("structlog", imports.structlog_mod);
    module.addImport("onnx_graph", imports.onnx_graph_mod);
    module.addImport("pjrt", imports.pjrt_mod);
    module.addImport("antfly_platform", imports.platform_mod);
    module.addImport("protobuf", imports.protobuf_mod);
    if (imports.inference_client_mod) |mod| {
        module.addImport("inference_client", mod);
    }
}

fn addBuildOptions(b: *std.Build, backend: BackendOptions) *std.Build.Step.Options {
    const options = b.addOptions();
    addCommonOptions(options, backend);
    options.addOption(bool, "enable_ffmpeg_audio", backend.enable_ffmpeg_audio);
    options.addOption(bool, "enable_native_quant_dispatch_stats", backend.enable_native_quant_dispatch_stats);
    return options;
}

fn addAudioOpenCorpusBuildOptions(b: *std.Build, backend: BackendOptions) *std.Build.Step.Options {
    const options = b.addOptions();
    addCommonOptions(options, backend);
    return options;
}

fn addCommonOptions(options: *std.Build.Step.Options, backend: BackendOptions) void {
    options.addOption(bool, "enable_onnx", backend.enable_onnx);
    options.addOption(bool, "enable_mlx", backend.enable_mlx);
    options.addOption(bool, "enable_metal", backend.enable_metal);
    options.addOption(bool, "enable_cuda", backend.enable_cuda);
    options.addOption([]const u8, "cuda_artifacts", backend.cuda_artifacts);
    options.addOption(bool, "enable_pjrt", backend.enable_pjrt);
    options.addOption(bool, "enable_native", backend.enable_native);
    options.addOption(bool, "enable_system_blas", backend.enable_system_blas);
    options.addOption(bool, "enable_wasm", backend.enable_wasm);
    options.addOption(bool, "enable_webgpu", backend.enable_webgpu);
    options.addOption(bool, "link_libc", backend.link_libc);
    options.addOption([]const u8, "wasm_memory_model", backend.wasm_memory_model);
    options.addOption(bool, "skip_openapi", backend.skip_openapi);
    options.addOption([]const u8, "inference_version", backend.inference_version);
}

fn addInferenceApiModule(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    httpx_mod: *std.Build.Module,
    skip_openapi: bool,
    paths: Paths,
    register_public_modules: bool,
    shared: SharedModules,
) *std.Build.Module {
    const generating_openapi_mod = shared.generating_openapi orelse addOrCreateModule(b, register_public_modules, "antfly_generating_openapi", .{
        .root_source_file = b.path(pathJoin(b, paths.shared_lib_root, "pkg/antfly/src/openapi/generated/antfly_generating_openapi/root.zig")),
        .target = target,
        .optimize = optimize,
    });

    if (skip_openapi) {
        const empty_wf = b.addWriteFiles();
        const empty_root = empty_wf.add("inference_api_stub.zig",
            \\comptime {
            \\    @compileError("inference_api is unavailable because -Dskip-openapi=true was set; build a target that does not import the generated HTTP server/client API.");
            \\}
            \\
        );
        const mod = addOrCreateModule(b, register_public_modules, "inference_api", .{
            .root_source_file = empty_root,
            .target = target,
            .optimize = optimize,
        });
        mod.addImport("httpx", httpx_mod);
        mod.addImport("antfly_generating_openapi", generating_openapi_mod);
        return mod;
    }

    const spec_path_override = b.option(
        []const u8,
        "inference-openapi-spec",
        "Path to the inference OpenAPI YAML spec used to generate inference_api",
    );

    if (spec_path_override == null) {
        const mod = addOrCreateModule(b, register_public_modules, "inference_api", .{
            .root_source_file = b.path(pathJoin(b, paths.inference_root, "src/api/generated/inference_api/root.zig")),
            .target = target,
            .optimize = optimize,
        });
        mod.addImport("httpx", httpx_mod);
        mod.addImport("antfly_generating_openapi", generating_openapi_mod);
        mod.addImport("antfly_extraction_openapi", shared.extraction_openapi orelse addExtractionOpenApiModule(b, target, optimize, paths, generating_openapi_mod));
        return mod;
    }

    const openapi_dep = b.dependency("openapi", .{
        .target = target,
        .optimize = optimize,
    });
    const convert = b.addSystemCommand(&.{
        "uv",
        "run",
        "--directory",
        b.fmt("{s}/scripts", .{paths.shared_lib_root}),
        "yaml_to_json.py",
    });
    convert.addFileArg(b.path(spec_path_override.?));
    const json_spec = convert.addOutputFileArg("inference.openapi.json");
    const codegen = b.addRunArtifact(openapi_dep.artifact("openapi-zig"));
    codegen.addArg("--spec");
    codegen.addFileArg(json_spec);
    codegen.addArgs(&.{ "--package", "inference_api" });
    codegen.addArgs(&.{ "--generate", "types,server,client" });
    codegen.addArgs(&.{"--import-mapping"});
    codegen.addArg(b.fmt("{s}={s}", .{ "../shared/generating.yaml", "antfly_generating_openapi" }));
    codegen.addArg(b.fmt("{s}={s}", .{ "../ai/extraction.yaml", "antfly_extraction_openapi" }));
    codegen.addArg("--output");
    const gen_dir = codegen.addOutputDirectoryArg("inference_api");
    const mod = addOrCreateModule(b, register_public_modules, "inference_api", .{
        .root_source_file = gen_dir.path(b, "root.zig"),
        .target = target,
        .optimize = optimize,
    });
    mod.addImport("httpx", httpx_mod);
    mod.addImport("antfly_generating_openapi", generating_openapi_mod);
    mod.addImport("antfly_extraction_openapi", shared.extraction_openapi orelse addExtractionOpenApiModule(b, target, optimize, paths, generating_openapi_mod));
    return mod;
}

fn addExtractionOpenApiModule(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    paths: Paths,
    generating_openapi_mod: *std.Build.Module,
) *std.Build.Module {
    const mod = b.createModule(.{
        .root_source_file = b.path(pathJoin(b, paths.shared_lib_root, "pkg/antfly/src/openapi/generated/antfly_extraction_openapi/root.zig")),
        .target = target,
        .optimize = optimize,
    });
    mod.addImport("antfly_generating_openapi", generating_openapi_mod);
    return mod;
}

fn addTokenizerDataModule(b: *std.Build, paths: Paths, register_public_modules: bool) *std.Build.Module {
    const write_files = b.addWriteFiles();
    _ = write_files.addCopyFile(
        b.path(pathJoin(b, paths.shared_lib_root, "lib/tokenizer/testdata/embedder/tokenizer.json")),
        "tokenizer.json",
    );
    const root = write_files.add(
        "root.zig",
        "pub const tokenizer_json = @embedFile(\"tokenizer.json\");\n",
    );
    return addOrCreateModule(b, register_public_modules, "inference_fixed_tokenizer_data", .{
        .root_source_file = root,
    });
}

fn addSentencePieceProtoModule(
    b: *std.Build,
    protobuf_dep: *std.Build.Dependency,
    paths: Paths,
    register_public_modules: bool,
) *std.Build.Module {
    const codegen = b.addRunArtifact(protobuf_dep.artifact("protoc-zig"));
    codegen.addArg("--desc");
    codegen.addFileArg(b.path(pathJoin(b, paths.shared_lib_root, "lib/tokenizer/proto/sentencepiece_model.desc")));
    codegen.addArg("--output");
    const raw_dir = codegen.addOutputDirectoryArg("sentencepiece_proto_raw");

    const fixup_tool = b.addExecutable(.{
        .name = "patch_sentencepiece_proto",
        .root_module = b.createModule(.{
            .root_source_file = b.path(pathJoin(b, paths.inference_root, "tools/patch_sentencepiece_proto.zig")),
            .target = b.graph.host,
            .optimize = .ReleaseSafe,
        }),
    });
    const fixup_run = b.addRunArtifact(fixup_tool);
    fixup_run.addFileArg(raw_dir.path(b, "root.zig"));
    fixup_run.addFileArg(raw_dir.path(b, "sentencepiece.zig"));
    const gen_dir = fixup_run.addOutputDirectoryArg("sentencepiece_proto");

    const mod = addOrCreateModule(b, register_public_modules, "sentencepiece_proto", .{
        .root_source_file = gen_dir.path(b, "root.zig"),
    });
    mod.addImport("protobuf", protobuf_dep.module("protobuf"));
    return mod;
}

fn createSharedModule(config: Config, relative_path: []const u8) *std.Build.Module {
    return config.b.createModule(.{
        .root_source_file = config.b.path(pathJoin(config.b, config.paths.shared_lib_root, relative_path)),
        .target = config.target,
        .optimize = config.optimize,
    });
}

fn createSharedModuleNamed(config: Config, public_name: []const u8, relative_path: []const u8) *std.Build.Module {
    return addOrCreateModule(config.b, config.register_public_modules, public_name, .{
        .root_source_file = config.b.path(pathJoin(config.b, config.paths.shared_lib_root, relative_path)),
        .target = config.target,
        .optimize = config.optimize,
    });
}

fn addOrCreateModule(b: *std.Build, register_public_modules: bool, name: []const u8, options: std.Build.Module.CreateOptions) *std.Build.Module {
    if (register_public_modules) return b.addModule(name, options);
    return b.createModule(options);
}

fn createOptionalSharedModule(config: Config, relative_path: []const u8, fallback_path: []const u8) *std.Build.Module {
    const shared_path = pathJoin(config.b, config.paths.shared_lib_root, relative_path);
    if (pathExists(config.b, shared_path)) {
        return config.b.createModule(.{
            .root_source_file = config.b.path(shared_path),
            .target = config.target,
            .optimize = config.optimize,
        });
    }
    return config.b.createModule(.{
        .root_source_file = config.b.path(pathJoin(config.b, config.paths.inference_root, fallback_path)),
        .target = config.target,
        .optimize = config.optimize,
    });
}

fn configureRuntimeLinks(
    b: *std.Build,
    module: *std.Build.Module,
    target: std.Build.ResolvedTarget,
    backend: BackendOptions,
    paths: Paths,
) void {
    if (backend.enable_system_blas) {
        configureSystemBlas(b, module, target, backend.blas_root);
    }
    configureOnnxRuntime(b, module, backend.enable_onnx, backend.onnx_root);
    configureMetal(b, module, target, backend.enable_metal, paths);
    configureMlx(b, module, target, backend.enable_mlx, backend.mlx_root);
    if (backend.ffmpeg_paths) |ffmpeg_paths| {
        module.addIncludePath(.{ .cwd_relative = ffmpeg_paths.include_dir });
        module.addLibraryPath(.{ .cwd_relative = ffmpeg_paths.lib_dir });
        module.addRPath(.{ .cwd_relative = ffmpeg_paths.lib_dir });
        module.linkSystemLibrary("avformat", .{});
        module.linkSystemLibrary("avcodec", .{});
        module.linkSystemLibrary("avutil", .{});
        module.linkSystemLibrary("swresample", .{});
    }
}

pub fn configureSystemBlas(
    b: *std.Build,
    module: *std.Build.Module,
    target: std.Build.ResolvedTarget,
    blas_root: ?[]const u8,
) void {
    if (target.result.os.tag == .macos) {
        module.linkFramework("Accelerate", .{});
        return;
    }
    if (blas_root) |root| {
        module.addIncludePath(.{ .cwd_relative = b.fmt("{s}/include", .{root}) });
        module.addLibraryPath(.{ .cwd_relative = b.fmt("{s}/lib", .{root}) });
        module.addRPath(.{ .cwd_relative = b.fmt("{s}/lib", .{root}) });
    }
    module.linkSystemLibrary("openblas", .{});
}

pub fn configureOnnxRuntime(
    b: *std.Build,
    module: *std.Build.Module,
    enable_onnx: bool,
    onnx_root: []const u8,
) void {
    if (!enable_onnx) return;
    module.addIncludePath(.{ .cwd_relative = b.fmt("{s}/include", .{onnx_root}) });
    module.addLibraryPath(.{ .cwd_relative = b.fmt("{s}/lib", .{onnx_root}) });
    module.addRPath(.{ .cwd_relative = b.fmt("{s}/lib", .{onnx_root}) });
    if (std.mem.startsWith(u8, onnx_root, "pkg/")) {
        module.addRPath(.{ .cwd_relative = b.fmt("zig/{s}/lib", .{onnx_root}) });
    }
    module.linkSystemLibrary("onnxruntime", .{});
    module.linkSystemLibrary("onnxruntime-genai", .{});
}

pub fn configureMetal(
    b: *std.Build,
    module: *std.Build.Module,
    target: std.Build.ResolvedTarget,
    enable_metal: bool,
    paths: Paths,
) void {
    if (!enable_metal or target.result.os.tag != .macos) return;
    module.linkFramework("Foundation", .{});
    module.linkFramework("Metal", .{});
    module.linkFramework("MetalPerformanceShaders", .{});
    module.linkFramework("MetalPerformanceShadersGraph", .{});
    module.addCSourceFile(.{ .file = b.path(pathJoin(b, paths.inference_root, "src/backends/metal_kernels.m")), .flags = &.{"-fobjc-arc"} });
}

pub fn configureMlx(
    b: *std.Build,
    module: *std.Build.Module,
    target: std.Build.ResolvedTarget,
    enable_mlx: bool,
    mlx_root: ?[]const u8,
) void {
    if (!enable_mlx or target.result.os.tag != .macos) return;
    if (mlx_root) |root| {
        module.addLibraryPath(.{ .cwd_relative = b.fmt("{s}/lib", .{root}) });
        module.addRPath(.{ .cwd_relative = b.fmt("{s}/lib", .{root}) });
    }
    module.linkSystemLibrary("mlxc", .{});
}

fn pathExists(b: *std.Build, path: []const u8) bool {
    const io = b.graph.io;
    std.Io.Dir.cwd().access(io, path, .{}) catch return false;
    return true;
}

fn pathJoin(b: *std.Build, root: []const u8, relative_path: []const u8) []const u8 {
    if (root.len == 0) return relative_path;
    return b.fmt("{s}/{s}", .{ root, relative_path });
}
