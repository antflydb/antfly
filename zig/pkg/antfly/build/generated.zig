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

pub const snowball_generated_root = "pkg/antfly/src/search/snowball/generated";
const sql_grammar_source = "pkg/antfly/src/sql/grammar/antfly_sql.y";
const sql_grammar_generated_root = "pkg/antfly/src/sql/grammar/generated/root.zig";
const onnx_proto_desc = "lib/onnx/proto/onnx.desc";
const sentencepiece_proto_desc = "lib/tokenizer/proto/sentencepiece_model.desc";
const sentencepiece_proto_patch_tool = "pkg/inference/tools/patch_sentencepiece_proto.zig";
const xla_hlo_proto_desc = "lib/pjrt/proto/hlo.desc";

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

const antfly_openapi_generated_root = "pkg/antfly/src/openapi/generated";
const inference_openapi_generated_root = "pkg/inference/src/api/generated";

pub const GeneratedArtifactSteps = struct {
    openapi_root_check: *std.Build.Step.Run,
    generated_check: *std.Build.Step,
};

pub const OpenApiModules = struct {
    public: *std.Build.Module,
    client: *std.Build.Module,
    schema: *std.Build.Module,
    indexes: *std.Build.Module,
    websearch: *std.Build.Module,
    eval: *std.Build.Module,
    query: *std.Build.Module,
    admin: *std.Build.Module,
    internal: *std.Build.Module,
    usermgr: *std.Build.Module,
    metadata: *std.Build.Module,
    logging: *std.Build.Module,
    audio: *std.Build.Module,
    middleware: *std.Build.Module,
    scraping: *std.Build.Module,
    s3: *std.Build.Module,
    inference_config: *std.Build.Module,
    chunking_api: *std.Build.Module,
    chunking: *std.Build.Module,
    embeddings: *std.Build.Module,
    common: *std.Build.Module,
    generating: *std.Build.Module,
    reranking: *std.Build.Module,
    generating_api: *std.Build.Module,
    extraction: *std.Build.Module,
    openai: *std.Build.Module,
};

pub fn addCommittedOpenApiModules(ctx: anytype) OpenApiModules {
    const b = ctx.b;
    const target = ctx.target;
    const optimize = ctx.optimize;
    const httpx_mod = ctx.httpx_mod;

    const public_openapi_mod = addCommittedOpenApiModule(b, target, optimize, "antfly_public_openapi", antfly_openapi_generated_root ++ "/antfly_public_openapi");
    const client_openapi_mod = addCommittedOpenApiModuleWithHttpx(b, target, optimize, "antfly_client_openapi", antfly_openapi_generated_root ++ "/antfly_client_openapi", httpx_mod);
    const schema_openapi_mod = addCommittedOpenApiModule(b, target, optimize, "antfly_schema_openapi", antfly_openapi_generated_root ++ "/antfly_schema_openapi");
    const indexes_openapi_mod = addCommittedOpenApiModule(b, target, optimize, "antfly_indexes_openapi", antfly_openapi_generated_root ++ "/antfly_indexes_openapi");
    const websearch_openapi_mod = addCommittedOpenApiModule(b, target, optimize, "antfly_websearch_openapi", antfly_openapi_generated_root ++ "/antfly_websearch_openapi");
    const eval_openapi_mod = addCommittedOpenApiModule(b, target, optimize, "antfly_eval_openapi", antfly_openapi_generated_root ++ "/antfly_eval_openapi");
    const query_openapi_mod = addCommittedOpenApiModule(b, target, optimize, "antfly_query_openapi", antfly_openapi_generated_root ++ "/antfly_query_openapi");
    const admin_openapi_mod = addCommittedOpenApiModuleWithHttpx(b, target, optimize, "antfly_admin_openapi", antfly_openapi_generated_root ++ "/antfly_admin_openapi", httpx_mod);
    const internal_openapi_mod = addCommittedOpenApiModuleWithHttpx(b, target, optimize, "antfly_internal_openapi", antfly_openapi_generated_root ++ "/antfly_internal_openapi", httpx_mod);
    const usermgr_openapi_mod = addCommittedOpenApiModuleWithHttpx(b, target, optimize, "antfly_usermgr_openapi", antfly_openapi_generated_root ++ "/antfly_usermgr_openapi", httpx_mod);
    const metadata_openapi_mod = addCommittedOpenApiModuleWithHttpx(b, target, optimize, "antfly_metadata_openapi", antfly_openapi_generated_root ++ "/antfly_metadata_openapi", httpx_mod);
    const logging_openapi_mod = addCommittedOpenApiModule(b, target, optimize, "antfly_logging_openapi", antfly_openapi_generated_root ++ "/antfly_logging_openapi");
    const audio_openapi_mod = addCommittedOpenApiModule(b, target, optimize, "antfly_audio_openapi", antfly_openapi_generated_root ++ "/antfly_audio_openapi");
    const middleware_openapi_mod = addCommittedOpenApiModule(b, target, optimize, "antfly_middleware_openapi", antfly_openapi_generated_root ++ "/antfly_middleware_openapi");
    const scraping_openapi_mod = addCommittedOpenApiModule(b, target, optimize, "antfly_scraping_openapi", antfly_openapi_generated_root ++ "/antfly_scraping_openapi");
    const s3_openapi_mod = addCommittedOpenApiModule(b, target, optimize, "antfly_s3_openapi", antfly_openapi_generated_root ++ "/antfly_s3_openapi");
    const inference_config_openapi_mod = addCommittedOpenApiModule(b, target, optimize, "antfly_inference_config_openapi", antfly_openapi_generated_root ++ "/antfly_inference_config_openapi");
    const chunking_api_openapi_mod = addCommittedOpenApiModule(b, target, optimize, "antfly_chunking_api_openapi", antfly_openapi_generated_root ++ "/antfly_chunking_api_openapi");
    const chunking_openapi_mod = addCommittedOpenApiModule(b, target, optimize, "antfly_chunking_openapi", antfly_openapi_generated_root ++ "/antfly_chunking_openapi");
    const embeddings_openapi_mod = addCommittedOpenApiModule(b, target, optimize, "antfly_embeddings_openapi", antfly_openapi_generated_root ++ "/antfly_embeddings_openapi");
    const common_openapi_mod = addCommittedOpenApiModule(b, target, optimize, "antfly_common_openapi", antfly_openapi_generated_root ++ "/antfly_common_openapi");
    const generating_openapi_mod = addCommittedOpenApiModule(b, target, optimize, "antfly_generating_openapi", antfly_openapi_generated_root ++ "/antfly_generating_openapi");
    const reranking_openapi_mod = addCommittedOpenApiModule(b, target, optimize, "antfly_reranking_openapi", antfly_openapi_generated_root ++ "/antfly_reranking_openapi");
    const generating_api_openapi_mod = addCommittedOpenApiModule(b, target, optimize, "antfly_generating_api_openapi", antfly_openapi_generated_root ++ "/antfly_generating_api_openapi");
    const extraction_openapi_mod = addCommittedOpenApiModule(b, target, optimize, "antfly_extraction_openapi", antfly_openapi_generated_root ++ "/antfly_extraction_openapi");
    const openai_api_mod = addCommittedOpenApiModuleWithHttpx(b, target, optimize, "openai_api", antfly_openapi_generated_root ++ "/openai_api", httpx_mod);

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

    return .{
        .public = public_openapi_mod,
        .client = client_openapi_mod,
        .schema = schema_openapi_mod,
        .indexes = indexes_openapi_mod,
        .websearch = websearch_openapi_mod,
        .eval = eval_openapi_mod,
        .query = query_openapi_mod,
        .admin = admin_openapi_mod,
        .internal = internal_openapi_mod,
        .usermgr = usermgr_openapi_mod,
        .metadata = metadata_openapi_mod,
        .logging = logging_openapi_mod,
        .audio = audio_openapi_mod,
        .middleware = middleware_openapi_mod,
        .scraping = scraping_openapi_mod,
        .s3 = s3_openapi_mod,
        .inference_config = inference_config_openapi_mod,
        .chunking_api = chunking_api_openapi_mod,
        .chunking = chunking_openapi_mod,
        .embeddings = embeddings_openapi_mod,
        .common = common_openapi_mod,
        .generating = generating_openapi_mod,
        .reranking = reranking_openapi_mod,
        .generating_api = generating_api_openapi_mod,
        .extraction = extraction_openapi_mod,
        .openai = openai_api_mod,
    };
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

pub fn addGeneratedArtifactSteps(ctx: anytype) GeneratedArtifactSteps {
    const b = ctx.b;
    const target = ctx.target;
    const optimize = ctx.optimize;
    const httpx_mod = ctx.httpx_mod;
    const protobuf_dep = ctx.protobuf_dep;

    const snowball_regen_step = addSnowballRegenStep(b);
    const snowball_check_step = addSnowballCheckStep(b);
    const openapi_codegen = addLocalOpenApiCodegen(b, target, optimize, httpx_mod);
    const yacc_codegen = addLocalYaccCodegen(b, target, optimize);
    _ = addYaccTestStep(b, target, optimize);
    const openapi_generate_step = addOpenApiRegenStep(b, openapi_codegen);
    const openapi_generated_check_step = addOpenApiGeneratedCheckStep(b, openapi_codegen);
    const sql_grammar_regen_step = addSqlGrammarRegenStep(b, yacc_codegen);
    const sql_grammar_generated_check_step = addSqlGrammarGeneratedCheckStep(b, yacc_codegen);
    const protobuf_generated_check_step = addProtobufDescriptorSmokeCheckStep(b, target, optimize, protobuf_dep);
    const snowball_sources_available = pathExists(b, "deps/snowball/zig/env.zig") and pathExists(b, "deps/snowball/compiler/analyser.c");

    const generate_step = b.step("generate", "Regenerate checked-in Zig generated artifacts");
    generate_step.dependOn(openapi_generate_step);
    generate_step.dependOn(sql_grammar_regen_step);
    if (snowball_sources_available) {
        generate_step.dependOn(snowball_regen_step);
    }

    const openapi_root_check = addOpenApiRootCheckRun(b);
    const openapi_root_check_step = b.step("openapi-root-check", "Check that the bundled root OpenAPI spec matches the modular Zig specs");
    openapi_root_check_step.dependOn(&openapi_root_check.step);

    const openapi_check_step = b.step("openapi-check", "Check checked-in OpenAPI artifacts are current");
    openapi_check_step.dependOn(openapi_root_check_step);
    openapi_check_step.dependOn(openapi_generated_check_step);

    const generated_check_step = b.step("generated-check", "Check checked-in Zig generated artifacts are current");
    generated_check_step.dependOn(openapi_check_step);
    generated_check_step.dependOn(sql_grammar_generated_check_step);
    generated_check_step.dependOn(protobuf_generated_check_step);
    if (snowball_sources_available) {
        generated_check_step.dependOn(snowball_check_step);
    }

    return .{
        .openapi_root_check = openapi_root_check,
        .generated_check = generated_check_step,
    };
}

fn pathExists(b: *std.Build, path: []const u8) bool {
    const io = b.graph.io;
    std.Io.Dir.cwd().access(io, path, .{}) catch return false;
    return true;
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

fn addOpenApiJoinInputs(b: *std.Build, run: *std.Build.Step.Run) void {
    for (openapi_join_input_paths) |path| {
        run.addFileInput(b.path(path));
    }
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

pub fn addSentencePieceProtoModule(
    b: *std.Build,
    protobuf_dep: *std.Build.Dependency,
) *std.Build.Module {
    const raw_dir = addProtobufGeneratedDirectory(
        b,
        protobuf_dep,
        b.path(sentencepiece_proto_desc),
        "sentencepiece_proto_raw",
        &.{},
    );

    const fixup_tool = b.addExecutable(.{
        .name = "patch_sentencepiece_proto",
        .root_module = b.createModule(.{
            .root_source_file = b.path(sentencepiece_proto_patch_tool),
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

pub fn addProtobufGeneratedModule(
    b: *std.Build,
    protobuf_dep: *std.Build.Dependency,
    desc_file: std.Build.LazyPath,
    module_name: []const u8,
    extra_args: []const []const u8,
) *std.Build.Module {
    const gen_dir = addProtobufGeneratedDirectory(b, protobuf_dep, desc_file, module_name, extra_args);
    const mod = b.createModule(.{
        .root_source_file = gen_dir.path(b, "root.zig"),
    });
    mod.addImport("protobuf", protobuf_dep.module("protobuf"));
    return mod;
}

pub fn addOnnxProtoModule(b: *std.Build, protobuf_dep: *std.Build.Dependency) *std.Build.Module {
    return addProtobufGeneratedModule(b, protobuf_dep, b.path(onnx_proto_desc), "onnx_proto", &.{});
}

pub fn addXlaProtoModule(b: *std.Build, protobuf_dep: *std.Build.Dependency) *std.Build.Module {
    return addProtobufGeneratedModule(b, protobuf_dep, b.path(xla_hlo_proto_desc), "xla_proto", &.{});
}

fn addProtobufDescriptorSmokeCheckStep(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    protobuf_dep: *std.Build.Dependency,
) *std.Build.Step {
    const check_step = b.step("protobuf-generated-check", "Compile generated ONNX and XLA protobuf descriptor bindings");
    const onnx_proto_mod = addProtobufGeneratedModule(b, protobuf_dep, b.path(onnx_proto_desc), "onnx_proto_check", &.{});
    const xla_proto_mod = addProtobufGeneratedModule(b, protobuf_dep, b.path(xla_hlo_proto_desc), "xla_proto_check", &.{});

    const smoke_test = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.addWriteFiles().add("protobuf_generated_smoke.zig",
                \\const std = @import("std");
                \\const onnx = @import("onnx_proto").onnx;
                \\const xla = @import("xla_proto").xla;
                \\
                \\test "generated ONNX proto bindings encode and decode a model" {
                \\    const alloc = std.testing.allocator;
                \\    var opsets = [_]onnx.OperatorSetIdProto{.{ .domain = "", .version = 17 }};
                \\    var model = onnx.ModelProto{
                \\        .ir_version = 8,
                \\        .graph = .{ .name = "generated-smoke" },
                \\        .opset_import = opsets[0..],
                \\    };
                \\    const bytes = try model.encode(alloc);
                \\    defer alloc.free(bytes);
                \\
                \\    var decoded = try onnx.ModelProto.decode(alloc, bytes);
                \\    defer decoded.deinit(alloc);
                \\    try std.testing.expectEqual(@as(i64, 8), decoded.ir_version);
                \\    try std.testing.expectEqualStrings("generated-smoke", decoded.graph.name);
                \\    try std.testing.expectEqual(@as(usize, 1), decoded.opset_import.len);
                \\    try std.testing.expectEqual(@as(i64, 17), decoded.opset_import[0].version);
                \\}
                \\
                \\test "generated XLA HLO proto bindings encode and decode a module" {
                \\    const alloc = std.testing.allocator;
                \\    var module = xla.HloModuleProto{
                \\        .name = "generated-xla-smoke",
                \\        .entry_computation_name = "entry",
                \\    };
                \\    const bytes = try module.encode(alloc);
                \\    defer alloc.free(bytes);
                \\
                \\    var decoded = try xla.HloModuleProto.decode(alloc, bytes);
                \\    defer decoded.deinit(alloc);
                \\    try std.testing.expectEqualStrings("generated-xla-smoke", decoded.name);
                \\    try std.testing.expectEqualStrings("entry", decoded.entry_computation_name);
                \\}
            ),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "onnx_proto", .module = onnx_proto_mod },
                .{ .name = "xla_proto", .module = xla_proto_mod },
            },
        }),
    });
    check_step.dependOn(&b.addRunArtifact(smoke_test).step);
    return check_step;
}

fn addProtobufGeneratedDirectory(
    b: *std.Build,
    protobuf_dep: *std.Build.Dependency,
    desc_file: std.Build.LazyPath,
    output_dir_name: []const u8,
    extra_args: []const []const u8,
) std.Build.LazyPath {
    const codegen = b.addRunArtifact(protobuf_dep.artifact("protoc-zig"));
    codegen.addArg("--desc");
    codegen.addFileArg(desc_file);
    codegen.addArg("--output");
    const gen_dir = codegen.addOutputDirectoryArg(output_dir_name);
    for (extra_args) |arg| codegen.addArg(arg);
    return gen_dir;
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

    const generated = addSqlGrammarGeneratedRootOutput(b, yacc_codegen, "regen_sql_grammar_root.zig");
    const update = b.addUpdateSourceFiles();
    update.addCopyFileToSource(generated, sql_grammar_generated_root);

    const fmt = b.addSystemCommand(&.{
        b.graph.zig_exe,
        "fmt",
        sql_grammar_generated_root,
    });
    fmt.step.dependOn(&update.step);
    regen_step.dependOn(&fmt.step);
    return regen_step;
}

fn addSqlGrammarGeneratedRootOutput(
    b: *std.Build,
    yacc_codegen: *std.Build.Step.Compile,
    output_name: []const u8,
) std.Build.LazyPath {
    const run = b.addRunArtifact(yacc_codegen);
    run.addFileArg(b.path(sql_grammar_source));
    const generated = run.addOutputFileArg(output_name);
    run.addArg(sql_grammar_source);
    return generated;
}

fn addSqlGrammarGeneratedCheckStep(b: *std.Build, yacc_codegen: *std.Build.Step.Compile) *std.Build.Step {
    const check_step = b.step("sql-grammar-generated-check", "Check checked-in Antfly SQL grammar metadata is current");
    const generated = addSqlGrammarGeneratedRootOutput(b, yacc_codegen, "check_sql_grammar_root.zig");

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

fn addOpenApiRootCheckRun(b: *std.Build) *std.Build.Step.Run {
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
    const json_spec = convert.addOutputFileArg(b.fmt("regen_{s}.json", .{package_name}));

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
    const json_spec = convert.addOutputFileArg(b.fmt("check_{s}.json", .{package_name}));

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
        .{ "schema.yaml", "antfly_schema_openapi" },
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
