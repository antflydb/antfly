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

fn addScriptsPythonCommand(b: *std.Build, script_path: []const u8, args: []const []const u8) *std.Build.Step.Run {
    const run = b.addSystemCommand(&.{
        "uv",
        "run",
        "--project",
        "../scripts",
        "--locked",
        "python",
    });
    run.addFileInput(b.path("../scripts/pyproject.toml"));
    run.addFileInput(b.path("../scripts/uv.lock"));
    run.addFileArg(b.path(script_path));
    run.addArgs(args);
    return run;
}

/// Each join reports the schemas it reads through a depfile at execution time.
fn addOpenApiJoinInputs(b: *std.Build, run: *std.Build.Step.Run, prefixed: bool) void {
    for ([_][]const u8{ "openapi_joiner.py", "openapi_inputs.py" }) |script| {
        run.addFileInput(b.path(b.pathJoin(&.{ "../scripts", script })));
    }
    if (prefixed) {
        run.addFileInput(b.path("../scripts/join_openapi.py"));
        run.addFileInput(b.path("../scripts/public_openapi_overlays.py"));
    }
    run.addArg("--depfile");
    _ = run.addDepFileOutputArg("openapi.d");
}

fn addGeneratedDirectory(
    b: *std.Build,
    comptime openapi_build: type,
    openapi_codegen: *std.Build.Step.Compile,
    source_path: std.Build.LazyPath,
    package_name: []const u8,
    generate_what: []const u8,
    import_mappings: []const [2][]const u8,
) std.Build.LazyPath {
    return openapi_build.addGeneratedDirectory(b, .{
        .compiler = openapi_codegen,
        .scripts_root = b.path("../scripts"),
        .spec = source_path,
        .package_name = package_name,
        .generate = generate_what,
        .import_mappings = import_mappings,
        .zig_type_mappings = &.{
            .{ "raw_json", "@import(\"antfly-json\").RawValue" },
            .{ "raw_json_object", "@import(\"antfly-json\").RawObject" },
        },
    });
}

pub fn addOpenApiRootCheckStep(b: *std.Build) *std.Build.Step.Run {
    const check = addScriptsPythonCommand(b, "../scripts/join_public_openapi.py", &.{"--compare"});
    addOpenApiJoinInputs(b, check, true);
    check.addFileArg(b.path("../openapi.yaml"));
    return check;
}

fn addJoinedPublicOpenApiSpec(b: *std.Build) std.Build.LazyPath {
    const join = addScriptsPythonCommand(b, "../scripts/join_openapi.py", &.{"--joined-only"});
    addOpenApiJoinInputs(b, join, false);
    join.addArg("--output");
    return join.addOutputFileArg("openapi.public.joined.yaml");
}

fn addPrefixedPublicOpenApiSpec(b: *std.Build) std.Build.LazyPath {
    const join = addScriptsPythonCommand(b, "../scripts/join_public_openapi.py", &.{});
    addOpenApiJoinInputs(b, join, true);
    join.addArg("--output");
    return join.addOutputFileArg("openapi.public.prefixed.yaml");
}

/// Embed source schemas through Zig's ordinary file inputs, independently of
/// build options. Source reads happen when compiling a schema consumer.
pub fn addEmbeddedSpecs(b: *std.Build, options: struct {
    root_source_file: std.Build.LazyPath,
    schema_root: std.Build.LazyPath,
    public_spec: std.Build.LazyPath,
}) *std.Build.Module {
    const module = b.createModule(.{ .root_source_file = options.root_source_file });
    const inputs = .{
        .{ "ard.yaml", options.schema_root.path(b, "ard/api.yaml") },
        .{ "antfly.yaml", options.public_spec },
        .{ "metadata.yaml", options.schema_root.path(b, "antfly/metadata.yaml") },
        .{ "extensions.yaml", options.schema_root.path(b, "extensions/api.yaml") },
        .{ "auth.yaml", options.schema_root.path(b, "auth/api.yaml") },
        .{ "inference-config.yaml", options.schema_root.path(b, "inference/config.yaml") },
    };
    inline for (inputs) |input| {
        module.addAnonymousImport(input[0], .{ .root_source_file = input[1] });
    }
    return module;
}

const GeneratedModule = struct {
    directory: std.Build.LazyPath,
    destination: []const u8,
};

fn addGeneratedModule(
    b: *std.Build,
    comptime openapi_build: type,
    openapi_codegen: *std.Build.Step.Compile,
    source_path: std.Build.LazyPath,
    package_name: []const u8,
    generated_dir: []const u8,
    generate_what: []const u8,
    import_mappings: []const [2][]const u8,
) GeneratedModule {
    const provider_mappings = [_][2][]const u8{
        .{ "../shared/provider.yaml", "antfly_provider_openapi" },
        .{ "./provider.yaml", "antfly_provider_openapi" },
        .{ "specs/openapi/shared/provider.yaml", "antfly_provider_openapi" },
    };
    const mappings = std.mem.concat(b.allocator, [2][]const u8, &.{ &provider_mappings, import_mappings }) catch @panic("OOM");
    return .{
        .directory = addGeneratedDirectory(b, openapi_build, openapi_codegen, source_path, package_name, generate_what, mappings),
        .destination = generated_dir,
    };
}

pub fn addOpenApiSourceSteps(
    b: *std.Build,
    comptime openapi_build: type,
    openapi_codegen: *std.Build.Step.Compile,
) struct { regen: *std.Build.Step.Run, check: *std.Build.Step.Run, public_spec: std.Build.LazyPath } {
    const regen = b.addSystemCommand(&.{"python3"});
    regen.addFileArg(b.path("tools/sync_generated.py"));
    regen.addArg("sync");
    regen.has_side_effects = true;
    const check = b.addSystemCommand(&.{"python3"});
    check.addFileArg(b.path("tools/sync_generated.py"));
    check.addArg("check");
    // Always inspect the destination, even when generation is cached. Missing
    // and extra files must be detected without mutating the source tree.
    check.has_side_effects = true;

    const antfly_generated_root = "pkg/antfly/src/openapi/generated";
    const inference_generated_root = "pkg/inference/src/api/generated";
    const public_spec = addPrefixedPublicOpenApiSpec(b);
    const modules = [_]GeneratedModule{
        addGeneratedModule(b, openapi_build, openapi_codegen, b.path("../specs/openapi/shared/provider.yaml"), "antfly_provider_openapi", antfly_generated_root ++ "/antfly_provider_openapi", "types", &.{}),
        addGeneratedModule(b, openapi_build, openapi_codegen, addJoinedPublicOpenApiSpec(b), "antfly_public_openapi", antfly_generated_root ++ "/antfly_public_openapi", "types,extractors", &.{
            .{ "specs/openapi/antfly/schema.yaml", "antfly_schema_openapi" },
            .{ "specs/openapi/antfly/indexes.yaml", "antfly_indexes_openapi" },
            .{ "specs/openapi/antfly/sort.yaml", "antfly_sort_openapi" },
            .{ "specs/openapi/antfly/embeddings.yaml", "antfly_embeddings_openapi" },
            .{ "specs/openapi/antfly/generating.yaml", "antfly_generating_api_openapi" },
            .{ "specs/openapi/antfly/eval.yaml", "antfly_eval_openapi" },
            .{ "specs/openapi/shared/generating.yaml", "antfly_generating_openapi" },
            .{ "specs/openapi/antfly/reranking.yaml", "antfly_reranking_openapi" },
            .{ "specs/openapi/antfly/query.yaml", "antfly_query_openapi" },
        }),
        addGeneratedModule(b, openapi_build, openapi_codegen, public_spec, "antfly_client_openapi", antfly_generated_root ++ "/antfly_client_openapi", "types,client", &.{
            .{ "specs/openapi/antfly/schema.yaml", "antfly_schema_openapi" },
            .{ "specs/openapi/antfly/indexes.yaml", "antfly_indexes_openapi" },
            .{ "specs/openapi/antfly/sort.yaml", "antfly_sort_openapi" },
            .{ "specs/openapi/antfly/generating.yaml", "antfly_generating_api_openapi" },
            .{ "specs/openapi/antfly/eval.yaml", "antfly_eval_openapi" },
            .{ "specs/openapi/shared/generating.yaml", "antfly_generating_openapi" },
            .{ "specs/openapi/antfly/reranking.yaml", "antfly_reranking_openapi" },
            .{ "specs/openapi/antfly/query.yaml", "antfly_query_openapi" },
        }),
        addGeneratedModule(b, openapi_build, openapi_codegen, b.path("../specs/openapi/antfly/schema.yaml"), "antfly_schema_openapi", antfly_generated_root ++ "/antfly_schema_openapi", "types", &.{}),
        addGeneratedModule(b, openapi_build, openapi_codegen, b.path("../specs/openapi/antfly/generated/graph_identifier.yaml"), "antfly_graph_identifier_openapi", antfly_generated_root ++ "/antfly_graph_identifier_openapi", "types", &.{}),
        addGeneratedModule(b, openapi_build, openapi_codegen, b.path("../specs/openapi/antfly/sort.yaml"), "antfly_sort_openapi", antfly_generated_root ++ "/antfly_sort_openapi", "types", &.{}),
        addGeneratedModule(b, openapi_build, openapi_codegen, b.path("../specs/openapi/antfly/indexes.yaml"), "antfly_indexes_openapi", antfly_generated_root ++ "/antfly_indexes_openapi", "types", &.{
            .{ "sort.yaml", "antfly_sort_openapi" },
            .{ "embeddings.yaml", "antfly_embeddings_openapi" },
            .{ "../shared/generating.yaml", "antfly_generating_openapi" },
            .{ "chunking.yaml", "antfly_chunking_openapi" },
            .{ "query.yaml", "antfly_query_openapi" },
            .{ "generated/graph_identifier.yaml", "antfly_graph_identifier_openapi" },
        }),
        addGeneratedModule(b, openapi_build, openapi_codegen, b.path("../specs/openapi/antfly/websearch.yaml"), "antfly_websearch_openapi", antfly_generated_root ++ "/antfly_websearch_openapi", "types", &.{
            .{ "../shared/s3.yaml", "antfly_s3_openapi" },
        }),
        addGeneratedModule(b, openapi_build, openapi_codegen, b.path("../specs/openapi/antfly/eval.yaml"), "antfly_eval_openapi", antfly_generated_root ++ "/antfly_eval_openapi", "types", &.{
            .{ "../shared/generating.yaml", "antfly_generating_openapi" },
        }),
        addGeneratedModule(b, openapi_build, openapi_codegen, b.path("../specs/openapi/antfly/query.yaml"), "antfly_query_openapi", antfly_generated_root ++ "/antfly_query_openapi", "types", &.{}),
        addGeneratedModule(b, openapi_build, openapi_codegen, b.path("../specs/openapi/antfly/admin.yaml"), "antfly_admin_openapi", antfly_generated_root ++ "/antfly_admin_openapi", "types,server", &.{}),
        addGeneratedModule(b, openapi_build, openapi_codegen, b.path("../specs/openapi/antfly/internal.yaml"), "antfly_internal_openapi", antfly_generated_root ++ "/antfly_internal_openapi", "types,server", &.{}),
        addGeneratedModule(b, openapi_build, openapi_codegen, b.path("../specs/openapi/auth/api.yaml"), "antfly_usermgr_openapi", antfly_generated_root ++ "/antfly_usermgr_openapi", "types,server", &.{}),
        addGeneratedModule(b, openapi_build, openapi_codegen, b.path("../specs/openapi/antfly/metadata.yaml"), "antfly_metadata_openapi", antfly_generated_root ++ "/antfly_metadata_openapi", "types,server", &.{
            .{ "../auth/api.yaml", "antfly_usermgr_openapi" },
            .{ "indexes.yaml", "antfly_indexes_openapi" },
            .{ "sort.yaml", "antfly_sort_openapi" },
            .{ "embeddings.yaml", "antfly_embeddings_openapi" },
            .{ "schema.yaml", "antfly_schema_openapi" },
            .{ "generating.yaml", "antfly_generating_api_openapi" },
            .{ "eval.yaml", "antfly_eval_openapi" },
            .{ "../shared/generating.yaml", "antfly_generating_openapi" },
            .{ "reranking.yaml", "antfly_reranking_openapi" },
            .{ "query.yaml", "antfly_query_openapi" },
        }),
        addGeneratedModule(b, openapi_build, openapi_codegen, b.path("../specs/openapi/shared/logging.yaml"), "antfly_logging_openapi", antfly_generated_root ++ "/antfly_logging_openapi", "types", &.{}),
        addGeneratedModule(b, openapi_build, openapi_codegen, b.path("../specs/openapi/antfly/audio.yaml"), "antfly_audio_openapi", antfly_generated_root ++ "/antfly_audio_openapi", "types", &.{
            .{ "../shared/s3.yaml", "antfly_s3_openapi" },
        }),
        addGeneratedModule(b, openapi_build, openapi_codegen, b.path("../specs/openapi/shared/middleware.yaml"), "antfly_middleware_openapi", antfly_generated_root ++ "/antfly_middleware_openapi", "types", &.{}),
        addGeneratedModule(b, openapi_build, openapi_codegen, b.path("../specs/openapi/shared/scraping.yaml"), "antfly_scraping_openapi", antfly_generated_root ++ "/antfly_scraping_openapi", "types", &.{}),
        addGeneratedModule(b, openapi_build, openapi_codegen, b.path("../specs/openapi/shared/s3.yaml"), "antfly_s3_openapi", antfly_generated_root ++ "/antfly_s3_openapi", "types", &.{}),
        addGeneratedModule(b, openapi_build, openapi_codegen, b.path("../specs/openapi/inference/config.yaml"), "antfly_inference_config_openapi", antfly_generated_root ++ "/antfly_inference_config_openapi", "types", &.{
            .{ "../shared/chunking.yaml", "antfly_chunking_api_openapi" },
            .{ "../shared/scraping.yaml", "antfly_scraping_openapi" },
            .{ "../shared/s3.yaml", "antfly_s3_openapi" },
            .{ "../shared/logging.yaml", "antfly_logging_openapi" },
            .{ "../shared/generating.yaml", "antfly_generating_openapi" },
        }),
        addGeneratedModule(b, openapi_build, openapi_codegen, b.path("../specs/openapi/shared/chunking.yaml"), "antfly_chunking_api_openapi", antfly_generated_root ++ "/antfly_chunking_api_openapi", "types", &.{
            .{ "generating.yaml", "antfly_generating_openapi" },
        }),
        addGeneratedModule(b, openapi_build, openapi_codegen, b.path("../specs/openapi/antfly/chunking.yaml"), "antfly_chunking_openapi", antfly_generated_root ++ "/antfly_chunking_openapi", "types", &.{
            .{ "../shared/chunking.yaml", "antfly_chunking_api_openapi" },
        }),
        addGeneratedModule(b, openapi_build, openapi_codegen, b.path("../specs/openapi/antfly/embeddings.yaml"), "antfly_embeddings_openapi", antfly_generated_root ++ "/antfly_embeddings_openapi", "types", &.{}),
        addGeneratedModule(b, openapi_build, openapi_codegen, b.path("../specs/openapi/antfly/config.yaml"), "antfly_common_openapi", antfly_generated_root ++ "/antfly_common_openapi", "types", &.{
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
        addGeneratedModule(b, openapi_build, openapi_codegen, b.path("../specs/openapi/shared/generating.yaml"), "antfly_generating_openapi", antfly_generated_root ++ "/antfly_generating_openapi", "types", &.{}),
        addGeneratedModule(b, openapi_build, openapi_codegen, b.path("../specs/openapi/antfly/reranking.yaml"), "antfly_reranking_openapi", antfly_generated_root ++ "/antfly_reranking_openapi", "types", &.{}),
        addGeneratedModule(b, openapi_build, openapi_codegen, b.path("../specs/openapi/ai/extraction.yaml"), "antfly_extraction_openapi", antfly_generated_root ++ "/antfly_extraction_openapi", "types", &.{
            .{ "../shared/generating.yaml", "antfly_generating_openapi" },
        }),
        addGeneratedModule(b, openapi_build, openapi_codegen, b.path("../specs/openapi/antfly/generating.yaml"), "antfly_generating_api_openapi", antfly_generated_root ++ "/antfly_generating_api_openapi", "types", &.{
            .{ "../shared/generating.yaml", "antfly_generating_openapi" },
            .{ "websearch.yaml", "antfly_websearch_openapi" },
        }),
        addGeneratedModule(b, openapi_build, openapi_codegen, b.path("../specs/openapi/inference/api.yaml"), "inference_api", inference_generated_root ++ "/inference_api", "types,server", &.{
            .{ "../shared/generating.yaml", "antfly_generating_openapi" },
            .{ "../shared/chunking.yaml", "antfly_chunking_api_openapi" },
            .{ "../ai/extraction.yaml", "antfly_extraction_openapi" },
        }),
        addGeneratedModule(b, openapi_build, openapi_codegen, b.path("specs/openai-openapi.yaml"), "openai_api", antfly_generated_root ++ "/openai_api", "types", &.{}),
    };

    // Assemble complete owner trees so removing a module from this inventory
    // also removes its obsolete checked-in directory during synchronization.
    for ([_][]const u8{ antfly_generated_root, inference_generated_root }) |destination| {
        const tree = b.addWriteFiles();
        const prefix = b.fmt("{s}/", .{destination});
        for (modules) |module| {
            if (std.mem.startsWith(u8, module.destination, prefix)) {
                _ = tree.addCopyDirectory(module.directory, module.destination[prefix.len..], .{});
            }
        }
        regen.addDirectoryArg(tree.getDirectory());
        regen.addArg(b.pathFromRoot(destination));
        check.addDirectoryArg(tree.getDirectory());
        check.addArg(b.pathFromRoot(destination));
    }
    return .{ .regen = regen, .check = check, .public_spec = public_spec };
}

/// Configure each runtime's schema modules while sharing the committed source tree.
pub const CommittedOptions = struct {
    root: std.Build.LazyPath,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    httpx: *std.Build.Module,
    json: *std.Build.Module,
    export_modules: bool = false,
};

pub const CommittedModules = struct {
    public: *std.Build.Module,
    client: *std.Build.Module,
    schema: *std.Build.Module,
    graph_identifier: *std.Build.Module,
    indexes: *std.Build.Module,
    sort: *std.Build.Module,
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
    provider: *std.Build.Module,
    common: *std.Build.Module,
    generating: *std.Build.Module,
    reranking: *std.Build.Module,
    generating_api: *std.Build.Module,
    extraction: *std.Build.Module,
    openai_api: *std.Build.Module,
};

fn committedModule(b: *std.Build, options: CommittedOptions, name: []const u8, httpx: bool) *std.Build.Module {
    const settings: std.Build.Module.CreateOptions = .{
        .root_source_file = options.root.path(b, b.fmt("{s}/root.zig", .{name})),
        .target = options.target,
        .optimize = options.optimize,
    };
    const module = if (options.export_modules) b.addModule(name, settings) else b.createModule(settings);
    if (httpx) module.addImport("httpx", options.httpx);
    return module;
}

pub fn createCommittedModules(b: *std.Build, options: CommittedOptions) CommittedModules {
    const public_openapi_mod = committedModule(b, options, "antfly_public_openapi", false);
    const client_openapi_mod = committedModule(b, options, "antfly_client_openapi", true);
    const schema_openapi_mod = committedModule(b, options, "antfly_schema_openapi", false);
    const graph_identifier_openapi_mod = committedModule(b, options, "antfly_graph_identifier_openapi", false);
    const indexes_openapi_mod = committedModule(b, options, "antfly_indexes_openapi", false);
    const sort_openapi_mod = committedModule(b, options, "antfly_sort_openapi", false);
    const websearch_openapi_mod = committedModule(b, options, "antfly_websearch_openapi", false);
    const eval_openapi_mod = committedModule(b, options, "antfly_eval_openapi", false);
    const query_openapi_mod = committedModule(b, options, "antfly_query_openapi", false);
    const admin_openapi_mod = committedModule(b, options, "antfly_admin_openapi", true);
    const internal_openapi_mod = committedModule(b, options, "antfly_internal_openapi", true);
    const usermgr_openapi_mod = committedModule(b, options, "antfly_usermgr_openapi", true);
    const metadata_openapi_mod = committedModule(b, options, "antfly_metadata_openapi", true);
    const logging_openapi_mod = committedModule(b, options, "antfly_logging_openapi", false);
    const audio_openapi_mod = committedModule(b, options, "antfly_audio_openapi", false);
    const middleware_openapi_mod = committedModule(b, options, "antfly_middleware_openapi", false);
    const scraping_openapi_mod = committedModule(b, options, "antfly_scraping_openapi", false);
    const s3_openapi_mod = committedModule(b, options, "antfly_s3_openapi", false);
    const inference_config_openapi_mod = committedModule(b, options, "antfly_inference_config_openapi", false);
    const chunking_api_openapi_mod = committedModule(b, options, "antfly_chunking_api_openapi", false);
    const chunking_openapi_mod = committedModule(b, options, "antfly_chunking_openapi", false);
    const embeddings_openapi_mod = committedModule(b, options, "antfly_embeddings_openapi", false);
    const provider_openapi_mod = committedModule(b, options, "antfly_provider_openapi", false);
    const common_openapi_mod = committedModule(b, options, "antfly_common_openapi", false);
    const generating_openapi_mod = committedModule(b, options, "antfly_generating_openapi", false);
    const reranking_openapi_mod = committedModule(b, options, "antfly_reranking_openapi", false);
    embeddings_openapi_mod.addImport("antfly_provider_openapi", provider_openapi_mod);
    generating_openapi_mod.addImport("antfly_provider_openapi", provider_openapi_mod);
    reranking_openapi_mod.addImport("antfly_provider_openapi", provider_openapi_mod);
    public_openapi_mod.addImport("antfly_provider_openapi", provider_openapi_mod);
    client_openapi_mod.addImport("antfly_provider_openapi", provider_openapi_mod);
    const generating_api_openapi_mod = committedModule(b, options, "antfly_generating_api_openapi", false);
    const extraction_openapi_mod = committedModule(b, options, "antfly_extraction_openapi", false);
    extraction_openapi_mod.addImport("antfly_generating_openapi", generating_openapi_mod);
    indexes_openapi_mod.addImport("antfly_embeddings_openapi", embeddings_openapi_mod);
    indexes_openapi_mod.addImport("antfly_generating_openapi", generating_openapi_mod);
    indexes_openapi_mod.addImport("antfly_chunking_openapi", chunking_openapi_mod);
    indexes_openapi_mod.addImport("antfly_sort_openapi", sort_openapi_mod);
    indexes_openapi_mod.addImport("antfly_query_openapi", query_openapi_mod);
    indexes_openapi_mod.addImport("antfly_graph_identifier_openapi", graph_identifier_openapi_mod);
    websearch_openapi_mod.addImport("antfly_s3_openapi", s3_openapi_mod);
    eval_openapi_mod.addImport("antfly_generating_openapi", generating_openapi_mod);
    generating_api_openapi_mod.addImport("antfly_generating_openapi", generating_openapi_mod);
    generating_api_openapi_mod.addImport("antfly_websearch_openapi", websearch_openapi_mod);
    public_openapi_mod.addImport("antfly_schema_openapi", schema_openapi_mod);
    public_openapi_mod.addImport("antfly_indexes_openapi", indexes_openapi_mod);
    public_openapi_mod.addImport("antfly_sort_openapi", sort_openapi_mod);
    public_openapi_mod.addImport("antfly_embeddings_openapi", embeddings_openapi_mod);
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
    metadata_openapi_mod.addImport("antfly_embeddings_openapi", embeddings_openapi_mod);
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

    const openai_api_mod = committedModule(b, options, "openai_api", true);
    public_openapi_mod.addImport("antfly-json", options.json);
    client_openapi_mod.addImport("antfly-json", options.json);
    metadata_openapi_mod.addImport("antfly-json", options.json);
    return .{
        .public = public_openapi_mod,
        .client = client_openapi_mod,
        .schema = schema_openapi_mod,
        .graph_identifier = graph_identifier_openapi_mod,
        .indexes = indexes_openapi_mod,
        .sort = sort_openapi_mod,
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
        .provider = provider_openapi_mod,
        .common = common_openapi_mod,
        .generating = generating_openapi_mod,
        .reranking = reranking_openapi_mod,
        .generating_api = generating_api_openapi_mod,
        .extraction = extraction_openapi_mod,
        .openai_api = openai_api_mod,
    };
}
