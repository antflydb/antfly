// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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
const platform_build = @import("antfly_platform");
const addSnowballModule = @import("../embedded/snowball.zig").addSnowballModule;

pub const AntflyRootImports = struct {
    sql_parser: *std.Build.Module,
    storage_boundary: @import("../embedded/storage_boundary.zig").Modules,
    cancellation: *std.Build.Module,
    cache_budget: *std.Build.Module,
    runtime_abi: *std.Build.Module,
    runtime_fs: *std.Build.Module,
    provision_contract: *std.Build.Module,
    read_state_observer: *std.Build.Module,
    private_error_diagnostics: *std.Build.Module,
    inference_bridge: *std.Build.Module,
    inference_provider_failure: *std.Build.Module,
    public_limits: *std.Build.Module,
    template_content: *std.Build.Module,
    sparse_embedding: *std.Build.Module,
    inference_worker_wire: *std.Build.Module,
    inference_worker_rpc: *std.Build.Module,
    inference_embedding_wire: *std.Build.Module,
    inference_types: *std.Build.Module,
    inference_work: *std.Build.Module,
    inference_openai: *std.Build.Module,
    inference_provider_defaults: *std.Build.Module,
    inference_bedrock: *std.Build.Module,
    inference_local: *std.Build.Module,
    inference_list_models: *std.Build.Module,
    inference_vertex: *std.Build.Module,
    inference_remote_capabilities: *std.Build.Module,
    inference_execution_context: *std.Build.Module,
    inference_request_types: *std.Build.Module,
    inference_runtime_paths: *std.Build.Module,
    inference_query_embedding_cache: *std.Build.Module,
    inference_host: *std.Build.Module,
    boundary_profile: @import("../embedded/storage_boundary.zig").Profile = .all,
    build_info: @import("../../lib/build_info/build_support.zig").BuildInfo,
    build_options: *std.Build.Step.Options,
    lite_options: *std.Build.Module,
    // HTTP schema serving is opt-in at the owning compilation roots.
    embedded_openapi: *std.Build.Module,
    raft_engine: *std.Build.Module,
    public_openapi: *std.Build.Module,
    public_server_openapi: *std.Build.Module,
    client_openapi: *std.Build.Module,
    schema_openapi: *std.Build.Module,
    indexes_openapi: *std.Build.Module,
    sort_openapi: *std.Build.Module,
    generating_api_openapi: *std.Build.Module,
    websearch_openapi: *std.Build.Module,
    eval_openapi: *std.Build.Module,
    query_openapi: *std.Build.Module,
    admin_openapi: *std.Build.Module,
    internal_openapi: *std.Build.Module,
    metadata_openapi: *std.Build.Module,
    metadata_server_openapi: *std.Build.Module,
    usermgr_openapi: *std.Build.Module,
    usermgr_server_openapi: *std.Build.Module,
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
    credentials: *std.Build.Module,
    google: *std.Build.Module,
    objectstore: *std.Build.Module,
    bloom: *std.Build.Module,
    vector: *std.Build.Module,
    vectorindex: *std.Build.Module,
    hash: *std.Build.Module,
    matcher: *std.Build.Module,
    resolver: *std.Build.Module,
    casbin: *std.Build.Module,
    fst: *std.Build.Module,
    regex: *std.Build.Module,
    json: *std.Build.Module,
    jsonschema: *std.Build.Module,
    mcp: *std.Build.Module,
    toon: *std.Build.Module,
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
    exa_api: *std.Build.Module,
    tavily_api: *std.Build.Module,
    handlebars: *std.Build.Module,
    inference_server: *std.Build.Module,
    prometheus: *std.Build.Module,
    structlog: *std.Build.Module,
    platform: *std.Build.Module,
    platform_link_libc: bool,
    platform_target: std.Build.ResolvedTarget,
    filesystem_capacity_source_file: std.Build.LazyPath,

    const import_table = [_]struct { name: []const u8, field: []const u8 }{
        .{ .name = "raft_engine", .field = "raft_engine" },
        .{ .name = "antfly_public_openapi", .field = "public_openapi" },
        .{ .name = "antfly_public_server_openapi", .field = "public_server_openapi" },
        .{ .name = "antfly_client_openapi", .field = "client_openapi" },
        .{ .name = "antfly_schema_openapi", .field = "schema_openapi" },
        .{ .name = "antfly_indexes_openapi", .field = "indexes_openapi" },
        .{ .name = "antfly_sort_openapi", .field = "sort_openapi" },
        .{ .name = "antfly_generating_api_openapi", .field = "generating_api_openapi" },
        .{ .name = "antfly_websearch_openapi", .field = "websearch_openapi" },
        .{ .name = "antfly_eval_openapi", .field = "eval_openapi" },
        .{ .name = "antfly_query_openapi", .field = "query_openapi" },
        .{ .name = "antfly_admin_openapi", .field = "admin_openapi" },
        .{ .name = "antfly_internal_openapi", .field = "internal_openapi" },
        .{ .name = "antfly_metadata_openapi", .field = "metadata_openapi" },
        .{ .name = "antfly_metadata_server_openapi", .field = "metadata_server_openapi" },
        .{ .name = "antfly_usermgr_openapi", .field = "usermgr_openapi" },
        .{ .name = "antfly_usermgr_server_openapi", .field = "usermgr_server_openapi" },
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
        .{ .name = "antfly_credentials", .field = "credentials" },
        .{ .name = "antfly_google", .field = "google" },
        .{ .name = "objectstore", .field = "objectstore" },
        .{ .name = "bloom", .field = "bloom" },
        .{ .name = "antfly_vector", .field = "vector" },
        .{ .name = "antfly_vectorindex", .field = "vectorindex" },
        .{ .name = "antfly_hash", .field = "hash" },
        .{ .name = "antfly_matcher", .field = "matcher" },
        .{ .name = "antfly_resolver", .field = "resolver" },
        .{ .name = "antfly_casbin", .field = "casbin" },
        .{ .name = "antfly_fst", .field = "fst" },
        .{ .name = "antfly_regex", .field = "regex" },
        .{ .name = "antfly-json", .field = "json" },
        .{ .name = "antfly_jsonschema", .field = "jsonschema" },
        .{ .name = "antfly_mcp", .field = "mcp" },
        .{ .name = "antfly_toon", .field = "toon" },
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
        .{ .name = "exa_api", .field = "exa_api" },
        .{ .name = "tavily_api", .field = "tavily_api" },
        .{ .name = "handlebars", .field = "handlebars" },
        .{ .name = "inference_server", .field = "inference_server" },
        .{ .name = "prometheus", .field = "prometheus" },
        .{ .name = "structlog", .field = "structlog" },
    };

    pub fn configure(self: @This(), b: *std.Build, mod: *std.Build.Module, link_libc: bool) void {
        // The public/test facade exposes the whole implementation. Production
        // archives use the owner constructors below to keep caches independent.
        self.configureBase(mod, link_libc);
        mod.addImport("antfly_inference_host", self.inference_host);
        self.configureServerContracts(mod);
        mod.addImport("antfly_lite_options", self.lite_options);
        inline for (import_table) |entry| mod.addImport(entry.name, @field(self, entry.field));
        addSnowballModule(b, mod);
        mod.addImport("build_info", self.build_info.module);
    }

    /// Remote commands depend on client contracts and transport. In particular,
    /// they do not depend on local tokenization, inference, or storage engines.
    pub fn configureCli(self: @This(), mod: *std.Build.Module, link_libc: bool) void {
        @import("../embedded/source_owner.zig").attach(mod);
        mod.addImport("antfly_platform", self.platform);
        mod.addImport("antfly_runtime_fs", self.runtime_fs);
        mod.addImport("antfly_runtime_abi", self.runtime_abi);
        mod.addImport("antfly_cancellation", self.cancellation);
        mod.addImport("antfly_credentials", self.credentials);
        mod.addImport("httpx", self.httpx);
        mod.addImport("antfly-json", self.json);
        mod.addImport("antfly_metadata_openapi", self.metadata_openapi);
        mod.addImport("antfly_hash", self.hash);
        mod.addImport("structlog", self.structlog);
        mod.addImport("handlebars", self.handlebars);
        mod.link_libc = link_libc;
    }

    pub fn configureInference(self: @This(), b: *std.Build, mod: *std.Build.Module, link_libc: bool) void {
        // The inference host consumes provider/configuration contracts, not
        // database engines or storage build settings.
        const options = b.addOptions();
        options.addOption(bool, "bench_minimal_deps", false);
        mod.addOptions("build_options", options);
        mod.addImport("antfly_platform", self.platform);
        mod.addImport("antfly_cancellation", self.cancellation);
        mod.addImport("antfly_cache_budget", self.cache_budget);
        mod.addImport("antfly_runtime_abi", self.runtime_abi);
        mod.addImport("antfly_runtime_fs", self.runtime_fs);
        mod.addImport("antfly_inference_bridge", self.inference_bridge);
        mod.addImport("antfly_sparse_embedding", self.sparse_embedding);
        mod.addImport("antfly_inference_worker_wire", self.inference_worker_wire);
        mod.addImport("antfly_private_error_diagnostics", self.private_error_diagnostics);
        mod.addImport("antfly_inference_provider_failure", self.inference_provider_failure);
        mod.addImport("antfly_inference_worker_rpc", self.inference_worker_rpc);
        mod.addImport("antfly_inference_embedding_wire", self.inference_embedding_wire);
        mod.addImport("antfly_inference_types", self.inference_types);
        mod.addImport("antfly_inference_work", self.inference_work);
        mod.addImport("antfly_inference_openai", self.inference_openai);
        mod.addImport("antfly_inference_provider_defaults", self.inference_provider_defaults);
        mod.addImport("antfly_inference_bedrock", self.inference_bedrock);
        mod.addImport("antfly_inference_local", self.inference_local);
        mod.addImport("antfly_inference_list_models", self.inference_list_models);
        mod.addImport("antfly_inference_vertex", self.inference_vertex);
        mod.addImport("antfly_inference_remote_capabilities", self.inference_remote_capabilities);
        mod.addImport("antfly_inference_execution_context", self.inference_execution_context);
        mod.addImport("antfly_inference_request_types", self.inference_request_types);
        mod.addImport("antfly_inference_runtime_paths", self.inference_runtime_paths);
        mod.addImport("antfly_inference_query_embedding_cache", self.inference_query_embedding_cache);
        mod.addImport("antfly_inference_host", self.inference_host);
        inline for (.{
            "httpx",              "common_openapi",  "inference_config_openapi", "logging_openapi",
            "middleware_openapi", "scraping",        "scraping_openapi",         "s3_openapi",
            "transcribing",       "readers",         "synthesizing",             "inference_server",
            "inference_api",      "extracting",      "google",                   "generating",
            "reranking",          "chunking",        "json",                     "handlebars",
            "openai_api",         "indexes_openapi", "embeddings_openapi",       "embeddings",
            "inference_chunker",  "vector",          "structlog",                "hash",
            "image",
        }) |field| self.addImport(mod, field);
        mod.link_libc = link_libc;
    }

    fn addImport(self: @This(), mod: *std.Build.Module, comptime field: []const u8) void {
        inline for (import_table) |entry| {
            if (comptime std.mem.eql(u8, entry.field, field)) {
                mod.addImport(entry.name, @field(self, field));
                return;
            }
        }
        @compileError("unknown Antfly dependency: " ++ field);
    }

    // Only dependencies consumed by all three database owners belong here.
    // Imports affect Zig's cache even when their declarations are never used;
    // keep owner-specific dependencies in their constructors below.
    fn configureDatabase(self: @This(), mod: *std.Build.Module, link_libc: bool) void {
        self.configureBase(mod, link_libc);
        inline for (.{
            "bloom",          "chunking",           "common_openapi",    "credentials",
            "embeddings",     "embeddings_openapi", "extracting",        "generating",
            "google",         "handlebars",         "hash",              "httpx",
            "image",          "indexes_openapi",    "inference_chunker", "inference_api",
            "json",           "logging_openapi",    "metadata_openapi",  "objectstore",
            "openai_api",     "pdf",                "query_openapi",     "reader_config",
            "readers",        "regex",              "reranking",         "scraping",
            "synthesizing",   "transcribing",       "vector",            "fst",
            "schema_openapi",
        }) |field| self.addImport(mod, field);
    }

    const storage_imports = .{
        "admin_openapi",            "casbin",           "extraction_openapi",
        "inference_config_openapi", "internal_openapi", "matcher",
        "middleware_openapi",       "raft_engine",      "resolver",
        "s3_openapi",               "scraping_openapi", "vectorindex",
    };
    const api_imports = .{
        "exa_api",
        "tavily_api",
        "websearch_openapi",
        "a2a",
        "casbin",
        "eval_openapi",
        "generating_api_openapi",
        "generating_openapi",
        "mcp",
        "matcher",
        "raft_engine",
        "toon",
        "usermgr_openapi",
        "public_server_openapi",
        "metadata_server_openapi",
        "usermgr_server_openapi",
    };

    // Native API processes and serverless hosts both own object-table runtimes.
    // Keep the bootstrap/remote inference contracts identical for those owners.
    const object_runtime_imports = .{
        "inference_api", "inference_config_openapi", "middleware_openapi",
        "s3_openapi",    "scraping_openapi",         "vectorindex",
        "matcher",
    };

    /// Public local C API: no server provisioning, quorum observer, or routers.
    pub fn configureEmbedded(self: @This(), b: *std.Build, mod: *std.Build.Module, link_libc: bool) void {
        self.configureDatabase(mod, link_libc);
        inline for (.{
            "casbin",     "extraction_openapi", "inference_config_openapi",
            "matcher",    "middleware_openapi", "resolver",
            "s3_openapi", "scraping_openapi",   "vectorindex",
        }) |field| self.addImport(mod, field);
        mod.addImport("antfly_lite_options", self.lite_options);
        mod.addImport("antfly_font", self.font);
        addSnowballModule(b, mod);
    }

    fn configureServerContracts(self: @This(), mod: *std.Build.Module) void {
        mod.addImport("antfly_provision_contract", self.provision_contract);
        mod.addImport("antfly_read_state_observer", self.read_state_observer);
    }

    pub fn configureStorage(self: @This(), b: *std.Build, mod: *std.Build.Module, link_libc: bool) void {
        self.configureStorageDependencies(b, mod, link_libc);
        mod.addImport("antfly_lite_options", self.lite_options);
    }

    fn configureStorageDependencies(self: @This(), b: *std.Build, mod: *std.Build.Module, link_libc: bool) void {
        self.configureDatabase(mod, link_libc);
        self.configureServerContracts(mod);
        inline for (storage_imports) |field| self.addImport(mod, field);
        addSnowballModule(b, mod);
    }

    pub fn configureEnrichment(self: @This(), b: *std.Build, mod: *std.Build.Module, link_libc: bool) void {
        const options = b.addOptions();
        options.addOption(bool, "bench_minimal_deps", false);
        mod.addOptions("build_options", options);
        // Document/media compute has no physical storage handle, including in
        // the independently compiled public embedded enrichment archive.
        self.storage_boundary.configureProfile(mod, true, false, self.boundary_profile);
        mod.addImport("antfly_platform", self.platform);
        mod.addImport("antfly_cancellation", self.cancellation);
        mod.addImport("antfly_template_content", self.template_content);
        mod.addImport("antfly_runtime_fs", self.runtime_fs);
        mod.link_libc = link_libc;
        inline for (.{ "image", "font", "pdf", "json", "scraping", "scraping_openapi", "reader_config", "chunking", "hash", "httpx", "structlog" }) |field| self.addImport(mod, field);
    }

    pub fn configureApi(self: @This(), mod: *std.Build.Module, link_libc: bool) void {
        self.configureDatabase(mod, link_libc);
        self.configureServerContracts(mod);
        inline for (api_imports) |field| self.addImport(mod, field);
        inline for (object_runtime_imports) |field| self.addImport(mod, field);
        mod.addImport("antfly_openapi_specs", self.embedded_openapi);
        // Native remote corpora share local analysis and scoring semantics.
        addSnowballModule(mod.owner, mod);
        self.addImport(mod, "vectorindex");
    }

    pub fn configureServerless(self: @This(), b: *std.Build, mod: *std.Build.Module, link_libc: bool) void {
        self.configureDatabase(mod, link_libc);
        mod.addImport("antfly_provision_contract", self.provision_contract);
        inline for (object_runtime_imports) |field| self.addImport(mod, field);
        addSnowballModule(b, mod);
    }

    /// This driver exercises storage and HTTP API implementations in one root.
    pub fn configureStorageBenchmark(self: @This(), b: *std.Build, mod: *std.Build.Module) void {
        mod.addImport("antfly_inference_host", self.inference_host);
        self.configureStorageDependencies(b, mod, true);
        inline for (api_imports) |field| self.addImport(mod, field);
        mod.addImport("antfly_openapi_specs", self.embedded_openapi);
    }

    /// Shared runtime contracts for standalone test and implementation roots.
    /// These imports own no storage, consensus, or inference implementation.
    pub fn configureRuntimeContracts(self: @This(), mod: *std.Build.Module) void {
        mod.addImport("antfly_cancellation", self.cancellation);
        mod.addImport("antfly_cache_budget", self.cache_budget);
        mod.addImport("antfly_runtime_abi", self.runtime_abi);
        mod.addImport("antfly_runtime_fs", self.runtime_fs);
        mod.addImport("antfly_template_content", self.template_content);
        mod.addImport("antfly_public_limits", self.public_limits);
    }

    /// Local DB callback and enrichment types, without model execution or server coordination.
    pub fn configureLocalDatabaseContracts(self: @This(), mod: *std.Build.Module) void {
        self.configureRuntimeContracts(mod);
        mod.addImport("antfly_inference_execution_context", self.inference_execution_context);
        mod.addImport("antfly_inference_request_types", self.inference_request_types);
        mod.addImport("antfly_inference_work", self.inference_work);
        mod.addImport("antfly_sparse_embedding", self.sparse_embedding);
    }

    fn configureBase(self: @This(), mod: *std.Build.Module, link_libc: bool) void {
        mod.addImport("sql_parser", self.sql_parser);
        self.storage_boundary.configureProfile(mod, false, false, self.boundary_profile);
        mod.addOptions("build_options", self.build_options);
        mod.addImport("antfly_platform", self.platform);
        self.configureLocalDatabaseContracts(mod);
        mod.addImport("antfly_inference_bridge", self.inference_bridge);
        mod.addImport("antfly_sparse_embedding", self.sparse_embedding);
        mod.addImport("antfly_inference_worker_wire", self.inference_worker_wire);
        mod.addImport("antfly_private_error_diagnostics", self.private_error_diagnostics);
        mod.addImport("antfly_inference_provider_failure", self.inference_provider_failure);
        mod.addImport("antfly_inference_worker_rpc", self.inference_worker_rpc);
        mod.addImport("antfly_inference_embedding_wire", self.inference_embedding_wire);
        mod.addImport("antfly_inference_types", self.inference_types);
        mod.addImport("antfly_inference_work", self.inference_work);
        mod.addImport("antfly_inference_openai", self.inference_openai);
        mod.addImport("antfly_inference_provider_defaults", self.inference_provider_defaults);
        mod.addImport("antfly_inference_bedrock", self.inference_bedrock);
        mod.addImport("antfly_inference_local", self.inference_local);
        mod.addImport("antfly_inference_list_models", self.inference_list_models);
        mod.addImport("antfly_inference_vertex", self.inference_vertex);
        mod.addImport("antfly_inference_remote_capabilities", self.inference_remote_capabilities);
        mod.addImport("antfly_inference_execution_context", self.inference_execution_context);
        mod.addImport("antfly_inference_request_types", self.inference_request_types);
        mod.addImport("antfly_inference_runtime_paths", self.inference_runtime_paths);
        mod.addImport("antfly_inference_query_embedding_cache", self.inference_query_embedding_cache);
        if (link_libc and !self.platform_link_libc) {
            platform_build.addFilesystemCapacitySource(
                mod,
                self.filesystem_capacity_source_file,
                self.platform_target,
            );
        }
        mod.link_libc = link_libc;
    }
};
