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

pub fn addScriptsPythonCommand(b: *std.Build, script_path: []const u8, args: []const []const u8) *std.Build.Step.Run {
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

pub const openapi_join_input_paths = [_][]const u8{
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
    "../specs/openapi/shared/provider.yaml",
    "../specs/openapi/antfly/schema.yaml",
    "../specs/openapi/antfly/indexes.yaml",
    "../specs/openapi/antfly/generated/graph_identifier.yaml",
};

pub fn addOpenApiJoinInputs(b: *std.Build, run: *std.Build.Step.Run) void {
    for (openapi_join_input_paths) |path| {
        run.addFileInput(b.path(path));
    }
}

pub const antfly_zig_type_mapping_args = [_][]const u8{
    "raw_json=@import(\"antfly-json\").RawValue",
    "raw_json_object=@import(\"antfly-json\").RawObject",
};

pub fn addAntflyZigTypeMappings(codegen: *std.Build.Step.Run) void {
    for (antfly_zig_type_mapping_args) |mapping| {
        codegen.addArgs(&.{"--zig-type-mapping"});
        codegen.addArg(mapping);
    }
}

pub fn addOpenApiModuleFromYamlPath(
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
    addAntflyZigTypeMappings(codegen);
    codegen.addArgs(&.{"--output"});
    const gen_dir = codegen.addOutputDirectoryArg(output_dir_name);

    return b.addModule(package_name, .{
        .root_source_file = gen_dir.path(b, "root.zig"),
    });
}

pub fn addYamlOpenApiModule(
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

pub fn addOpenApiRootCheckStep(b: *std.Build) *std.Build.Step.Run {
    const check = addScriptsPythonCommand(b, "../scripts/join_public_openapi.py", &.{"--compare"});
    addOpenApiJoinInputs(b, check);
    check.addFileArg(b.path("../openapi.yaml"));
    return check;
}

pub fn addJoinedPublicOpenApiSpec(b: *std.Build) std.Build.LazyPath {
    const join = addScriptsPythonCommand(b, "../scripts/join_openapi.py", &.{"--joined-only"});
    addOpenApiJoinInputs(b, join);
    return join.addOutputFileArg("openapi.public.joined.yaml");
}

pub fn addPrefixedPublicOpenApiSpec(b: *std.Build) std.Build.LazyPath {
    const join = addScriptsPythonCommand(b, "../scripts/join_public_openapi.py", &.{});
    addOpenApiJoinInputs(b, join);
    return join.addOutputFileArg("openapi.public.prefixed.yaml");
}

pub fn addPublicOpenApiModule(
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
            .{ "specs/openapi/antfly/embeddings.yaml", "antfly_embeddings_openapi" },
            .{ "specs/openapi/antfly/generating.yaml", "antfly_generating_api_openapi" },
            .{ "specs/openapi/antfly/eval.yaml", "antfly_eval_openapi" },
            .{ "specs/openapi/shared/generating.yaml", "antfly_generating_openapi" },
            .{ "specs/openapi/antfly/reranking.yaml", "antfly_reranking_openapi" },
            .{ "specs/openapi/antfly/query.yaml", "antfly_query_openapi" },
        },
    );
}

pub fn addPublicClientOpenApiModule(
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
pub fn addOpenApiModuleWithHttpxFromYamlPath(
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
    addAntflyZigTypeMappings(codegen);
    codegen.addArgs(&.{"--output"});
    const gen_dir = codegen.addOutputDirectoryArg(output_dir_name);

    const mod = b.addModule(package_name, .{
        .root_source_file = gen_dir.path(b, "root.zig"),
    });
    mod.addImport("httpx", httpx_mod);
    return mod;
}

pub fn addYamlOpenApiModuleWithHttpx(
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

pub fn addCommittedOpenApiModule(
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

pub fn addCommittedOpenApiModuleWithHttpx(
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

pub fn addOpenApiRegenRun(
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
    codegen.addArgs(&.{ "--import-mapping", "../shared/provider.yaml=antfly_provider_openapi", "--import-mapping", "./provider.yaml=antfly_provider_openapi", "--import-mapping", "specs/openapi/shared/provider.yaml=antfly_provider_openapi" });
    for (import_mappings) |mapping| {
        codegen.addArgs(&.{"--import-mapping"});
        codegen.addArg(b.fmt("{s}={s}", .{ mapping[0], mapping[1] }));
    }
    addAntflyZigTypeMappings(codegen);
    codegen.addArgs(&.{ "--output", generated_dir });
    return codegen;
}

pub fn addOpenApiRegenStep(
    b: *std.Build,
    openapi_codegen: *std.Build.Step.Compile,
) void {
    const regen_step = b.step("regen-openapi", "Regenerate checked-in Zig OpenAPI modules");

    const antfly_generated_root = "pkg/antfly/src/openapi/generated";
    const inference_generated_root = "pkg/inference/src/api/generated";
    const runs = [_]*std.Build.Step.Run{
        addOpenApiRegenRun(b, openapi_codegen, b.path("../specs/openapi/shared/provider.yaml"), "antfly_provider_openapi", antfly_generated_root ++ "/antfly_provider_openapi", "types", &.{}),
        addOpenApiRegenRun(b, openapi_codegen, addJoinedPublicOpenApiSpec(b), "antfly_public_openapi", antfly_generated_root ++ "/antfly_public_openapi", "types,extractors", &.{
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
        addOpenApiRegenRun(b, openapi_codegen, b.path("../specs/openapi/antfly/generated/graph_identifier.yaml"), "antfly_graph_identifier_openapi", antfly_generated_root ++ "/antfly_graph_identifier_openapi", "types", &.{}),
        addOpenApiRegenRun(b, openapi_codegen, b.path("../specs/openapi/antfly/sort.yaml"), "antfly_sort_openapi", antfly_generated_root ++ "/antfly_sort_openapi", "types", &.{}),
        addOpenApiRegenRun(b, openapi_codegen, b.path("../specs/openapi/antfly/indexes.yaml"), "antfly_indexes_openapi", antfly_generated_root ++ "/antfly_indexes_openapi", "types", &.{
            .{ "sort.yaml", "antfly_sort_openapi" },
            .{ "embeddings.yaml", "antfly_embeddings_openapi" },
            .{ "../shared/generating.yaml", "antfly_generating_openapi" },
            .{ "chunking.yaml", "antfly_chunking_openapi" },
            .{ "query.yaml", "antfly_query_openapi" },
            .{ "generated/graph_identifier.yaml", "antfly_graph_identifier_openapi" },
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
            .{ "embeddings.yaml", "antfly_embeddings_openapi" },
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
