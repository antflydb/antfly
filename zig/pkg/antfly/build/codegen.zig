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

/// Joining follows external references throughout this schema tree. Discover
/// its inputs so a newly referenced schema cannot escape cache invalidation.
fn addOpenApiJoinInputs(b: *std.Build, run: *std.Build.Step.Run) void {
    for ([_][]const u8{ "join_openapi.py", "openapi_joiner.py", "public_openapi_overlays.py" }) |script| {
        run.addFileInput(b.path(b.pathJoin(&.{ "../scripts", script })));
    }
    const root = b.path("../specs/openapi");
    var dir = std.Io.Dir.cwd().openDir(b.graph.io, root.getPath(b), .{ .iterate = true }) catch @panic("cannot open OpenAPI schema directory");
    defer dir.close(b.graph.io);
    var walker = dir.walk(b.allocator) catch @panic("OOM");
    defer walker.deinit();
    var paths: std.ArrayList([]const u8) = .empty;
    while (walker.next(b.graph.io) catch @panic("cannot enumerate OpenAPI schemas")) |entry| {
        if (entry.kind == .file and (std.mem.endsWith(u8, entry.path, ".yaml") or std.mem.endsWith(u8, entry.path, ".yml") or std.mem.endsWith(u8, entry.path, ".json"))) {
            paths.append(b.allocator, b.dupe(entry.path)) catch @panic("OOM");
        }
    }
    std.mem.sort([]const u8, paths.items, {}, struct {
        fn lessThan(_: void, a: []const u8, c: []const u8) bool {
            return std.mem.lessThan(u8, a, c);
        }
    }.lessThan);
    for (paths.items) |path| run.addFileInput(root.path(b, path));
}

const antfly_zig_type_mapping_args = [_][]const u8{
    "raw_json=@import(\"antfly-json\").RawValue",
    "raw_json_object=@import(\"antfly-json\").RawObject",
};

fn addAntflyZigTypeMappings(codegen: *std.Build.Step.Run) void {
    for (antfly_zig_type_mapping_args) |mapping| {
        codegen.addArgs(&.{"--zig-type-mapping"});
        codegen.addArg(mapping);
    }
}

fn addGeneratedDirectory(
    b: *std.Build,
    openapi_codegen: *std.Build.Step.Compile,
    source_path: std.Build.LazyPath,
    package_name: []const u8,
    generate_what: []const u8,
    import_mappings: []const [2][]const u8,
) std.Build.LazyPath {
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
    addAntflyZigTypeMappings(codegen);
    codegen.addArgs(&.{"--output"});
    return codegen.addOutputDirectoryArg(package_name);
}

pub fn addOpenApiRootCheckStep(b: *std.Build) *std.Build.Step.Run {
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

const GeneratedModule = struct {
    directory: std.Build.LazyPath,
    destination: []const u8,
};

fn addGeneratedModule(
    b: *std.Build,
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
        .directory = addGeneratedDirectory(b, openapi_codegen, source_path, package_name, generate_what, mappings),
        .destination = generated_dir,
    };
}

pub fn addOpenApiSourceSteps(
    b: *std.Build,
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
        addGeneratedModule(b, openapi_codegen, b.path("../specs/openapi/shared/provider.yaml"), "antfly_provider_openapi", antfly_generated_root ++ "/antfly_provider_openapi", "types", &.{}),
        addGeneratedModule(b, openapi_codegen, addJoinedPublicOpenApiSpec(b), "antfly_public_openapi", antfly_generated_root ++ "/antfly_public_openapi", "types,extractors", &.{
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
        addGeneratedModule(b, openapi_codegen, public_spec, "antfly_client_openapi", antfly_generated_root ++ "/antfly_client_openapi", "types,client", &.{
            .{ "specs/openapi/antfly/schema.yaml", "antfly_schema_openapi" },
            .{ "specs/openapi/antfly/indexes.yaml", "antfly_indexes_openapi" },
            .{ "specs/openapi/antfly/sort.yaml", "antfly_sort_openapi" },
            .{ "specs/openapi/antfly/generating.yaml", "antfly_generating_api_openapi" },
            .{ "specs/openapi/antfly/eval.yaml", "antfly_eval_openapi" },
            .{ "specs/openapi/shared/generating.yaml", "antfly_generating_openapi" },
            .{ "specs/openapi/antfly/reranking.yaml", "antfly_reranking_openapi" },
            .{ "specs/openapi/antfly/query.yaml", "antfly_query_openapi" },
        }),
        addGeneratedModule(b, openapi_codegen, b.path("../specs/openapi/antfly/schema.yaml"), "antfly_schema_openapi", antfly_generated_root ++ "/antfly_schema_openapi", "types", &.{}),
        addGeneratedModule(b, openapi_codegen, b.path("../specs/openapi/antfly/generated/graph_identifier.yaml"), "antfly_graph_identifier_openapi", antfly_generated_root ++ "/antfly_graph_identifier_openapi", "types", &.{}),
        addGeneratedModule(b, openapi_codegen, b.path("../specs/openapi/antfly/sort.yaml"), "antfly_sort_openapi", antfly_generated_root ++ "/antfly_sort_openapi", "types", &.{}),
        addGeneratedModule(b, openapi_codegen, b.path("../specs/openapi/antfly/indexes.yaml"), "antfly_indexes_openapi", antfly_generated_root ++ "/antfly_indexes_openapi", "types", &.{
            .{ "sort.yaml", "antfly_sort_openapi" },
            .{ "embeddings.yaml", "antfly_embeddings_openapi" },
            .{ "../shared/generating.yaml", "antfly_generating_openapi" },
            .{ "chunking.yaml", "antfly_chunking_openapi" },
            .{ "query.yaml", "antfly_query_openapi" },
            .{ "generated/graph_identifier.yaml", "antfly_graph_identifier_openapi" },
        }),
        addGeneratedModule(b, openapi_codegen, b.path("../specs/openapi/antfly/websearch.yaml"), "antfly_websearch_openapi", antfly_generated_root ++ "/antfly_websearch_openapi", "types", &.{
            .{ "../shared/s3.yaml", "antfly_s3_openapi" },
        }),
        addGeneratedModule(b, openapi_codegen, b.path("../specs/openapi/antfly/eval.yaml"), "antfly_eval_openapi", antfly_generated_root ++ "/antfly_eval_openapi", "types", &.{
            .{ "../shared/generating.yaml", "antfly_generating_openapi" },
        }),
        addGeneratedModule(b, openapi_codegen, b.path("../specs/openapi/antfly/query.yaml"), "antfly_query_openapi", antfly_generated_root ++ "/antfly_query_openapi", "types", &.{}),
        addGeneratedModule(b, openapi_codegen, b.path("../specs/openapi/antfly/admin.yaml"), "antfly_admin_openapi", antfly_generated_root ++ "/antfly_admin_openapi", "types,server", &.{}),
        addGeneratedModule(b, openapi_codegen, b.path("../specs/openapi/antfly/internal.yaml"), "antfly_internal_openapi", antfly_generated_root ++ "/antfly_internal_openapi", "types,server", &.{}),
        addGeneratedModule(b, openapi_codegen, b.path("../specs/openapi/auth/api.yaml"), "antfly_usermgr_openapi", antfly_generated_root ++ "/antfly_usermgr_openapi", "types,server", &.{}),
        addGeneratedModule(b, openapi_codegen, b.path("../specs/openapi/antfly/metadata.yaml"), "antfly_metadata_openapi", antfly_generated_root ++ "/antfly_metadata_openapi", "types,server", &.{
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
        addGeneratedModule(b, openapi_codegen, b.path("../specs/openapi/shared/logging.yaml"), "antfly_logging_openapi", antfly_generated_root ++ "/antfly_logging_openapi", "types", &.{}),
        addGeneratedModule(b, openapi_codegen, b.path("../specs/openapi/antfly/audio.yaml"), "antfly_audio_openapi", antfly_generated_root ++ "/antfly_audio_openapi", "types", &.{
            .{ "../shared/s3.yaml", "antfly_s3_openapi" },
        }),
        addGeneratedModule(b, openapi_codegen, b.path("../specs/openapi/shared/middleware.yaml"), "antfly_middleware_openapi", antfly_generated_root ++ "/antfly_middleware_openapi", "types", &.{}),
        addGeneratedModule(b, openapi_codegen, b.path("../specs/openapi/shared/scraping.yaml"), "antfly_scraping_openapi", antfly_generated_root ++ "/antfly_scraping_openapi", "types", &.{}),
        addGeneratedModule(b, openapi_codegen, b.path("../specs/openapi/shared/s3.yaml"), "antfly_s3_openapi", antfly_generated_root ++ "/antfly_s3_openapi", "types", &.{}),
        addGeneratedModule(b, openapi_codegen, b.path("../specs/openapi/inference/config.yaml"), "antfly_inference_config_openapi", antfly_generated_root ++ "/antfly_inference_config_openapi", "types", &.{
            .{ "../shared/chunking.yaml", "antfly_chunking_api_openapi" },
            .{ "../shared/scraping.yaml", "antfly_scraping_openapi" },
            .{ "../shared/s3.yaml", "antfly_s3_openapi" },
            .{ "../shared/logging.yaml", "antfly_logging_openapi" },
            .{ "../shared/generating.yaml", "antfly_generating_openapi" },
        }),
        addGeneratedModule(b, openapi_codegen, b.path("../specs/openapi/shared/chunking.yaml"), "antfly_chunking_api_openapi", antfly_generated_root ++ "/antfly_chunking_api_openapi", "types", &.{
            .{ "generating.yaml", "antfly_generating_openapi" },
        }),
        addGeneratedModule(b, openapi_codegen, b.path("../specs/openapi/antfly/chunking.yaml"), "antfly_chunking_openapi", antfly_generated_root ++ "/antfly_chunking_openapi", "types", &.{
            .{ "../shared/chunking.yaml", "antfly_chunking_api_openapi" },
        }),
        addGeneratedModule(b, openapi_codegen, b.path("../specs/openapi/antfly/embeddings.yaml"), "antfly_embeddings_openapi", antfly_generated_root ++ "/antfly_embeddings_openapi", "types", &.{}),
        addGeneratedModule(b, openapi_codegen, b.path("../specs/openapi/antfly/config.yaml"), "antfly_common_openapi", antfly_generated_root ++ "/antfly_common_openapi", "types", &.{
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
        addGeneratedModule(b, openapi_codegen, b.path("../specs/openapi/shared/generating.yaml"), "antfly_generating_openapi", antfly_generated_root ++ "/antfly_generating_openapi", "types", &.{}),
        addGeneratedModule(b, openapi_codegen, b.path("../specs/openapi/antfly/reranking.yaml"), "antfly_reranking_openapi", antfly_generated_root ++ "/antfly_reranking_openapi", "types", &.{}),
        addGeneratedModule(b, openapi_codegen, b.path("../specs/openapi/ai/extraction.yaml"), "antfly_extraction_openapi", antfly_generated_root ++ "/antfly_extraction_openapi", "types", &.{
            .{ "../shared/generating.yaml", "antfly_generating_openapi" },
        }),
        addGeneratedModule(b, openapi_codegen, b.path("../specs/openapi/antfly/generating.yaml"), "antfly_generating_api_openapi", antfly_generated_root ++ "/antfly_generating_api_openapi", "types", &.{
            .{ "../shared/generating.yaml", "antfly_generating_openapi" },
            .{ "websearch.yaml", "antfly_websearch_openapi" },
        }),
        addGeneratedModule(b, openapi_codegen, b.path("../specs/openapi/inference/api.yaml"), "inference_api", inference_generated_root ++ "/inference_api", "types,server", &.{
            .{ "../shared/generating.yaml", "antfly_generating_openapi" },
            .{ "../shared/chunking.yaml", "antfly_chunking_api_openapi" },
            .{ "../ai/extraction.yaml", "antfly_extraction_openapi" },
        }),
        addGeneratedModule(b, openapi_codegen, b.path("specs/openai-openapi.yaml"), "openai_api", antfly_generated_root ++ "/openai_api", "types", &.{}),
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
