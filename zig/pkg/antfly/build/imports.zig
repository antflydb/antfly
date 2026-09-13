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
const platform_build = @import("../../../lib/platform/build_support.zig");
const addSnowballModule = @import("snowball.zig").addSnowballModule;

pub const AntflyRootImports = struct {
    storage_boundary: @import("storage_boundary.zig").Modules,
    boundary_profile: @import("storage_boundary.zig").Profile = .all,
    build_info: @import("../../../lib/build_info/build_support.zig").BuildInfo,
    build_options: *std.Build.Step.Options,
    lite_options: *std.Build.Module,
    // HTTP schema serving is opt-in at the owning compilation roots.
    embedded_openapi: *std.Build.Module,
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

    pub fn configure(self: @This(), b: *std.Build, mod: *std.Build.Module, link_libc: bool) void {
        // The public/test facade exposes the whole implementation. Production
        // archives use the owner constructors below to keep caches independent.
        self.configureBase(mod, link_libc);
        mod.addImport("antfly_lite_options", self.lite_options);
        inline for (import_table) |entry| mod.addImport(entry.name, @field(self, entry.field));
        addSnowballModule(b, mod);
        mod.addImport("build_info", self.build_info.module);
    }

    /// Remote commands depend on client contracts and transport. In particular,
    /// they do not depend on local tokenization, inference, or storage engines.
    pub fn configureCli(self: @This(), mod: *std.Build.Module, link_libc: bool) void {
        mod.addImport("antfly_platform", self.platform);
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
            "bloom",           "chunking",           "common_openapi",    "credentials",
            "embeddings",      "embeddings_openapi", "extracting",        "generating",
            "google",          "handlebars",         "hash",              "httpx",
            "image",           "indexes_openapi",    "inference_chunker", "json",
            "logging_openapi", "metadata_openapi",   "objectstore",       "openai_api",
            "pdf",             "query_openapi",      "reader_config",     "readers",
            "regex",           "reranking",          "scraping",          "synthesizing",
            "transcribing",    "vector",             "vellum",
        }) |field| self.addImport(mod, field);
    }

    const storage_imports = .{
        "admin_openapi",            "casbin",           "extraction_openapi", "inference_api",
        "inference_config_openapi", "internal_openapi", "matcher",            "middleware_openapi",
        "raft_engine",              "resolver",         "s3_openapi",         "scraping_openapi",
        "vectorindex",
    };
    const api_imports = .{
        "a2a", "casbin",      "eval_openapi",   "generating_api_openapi", "generating_openapi",
        "mcp", "raft_engine", "schema_openapi", "usermgr_openapi",
    };

    pub fn configureStorage(self: @This(), b: *std.Build, mod: *std.Build.Module, link_libc: bool) void {
        self.configureStorageDependencies(b, mod, link_libc);
        mod.addImport("antfly_lite_options", self.lite_options);
    }

    fn configureStorageDependencies(self: @This(), b: *std.Build, mod: *std.Build.Module, link_libc: bool) void {
        self.configureDatabase(mod, link_libc);
        inline for (storage_imports) |field| self.addImport(mod, field);
        addSnowballModule(b, mod);
    }

    pub fn configureEnrichment(self: @This(), b: *std.Build, mod: *std.Build.Module, link_libc: bool) void {
        const options = b.addOptions();
        options.addOption(bool, "bench_minimal_deps", false);
        mod.addOptions("build_options", options);
        self.storage_boundary.configureProfile(mod, false, false, self.boundary_profile);
        mod.addImport("antfly_platform", self.platform);
        mod.link_libc = link_libc;
        inline for (.{ "image", "font", "pdf", "json", "scraping", "scraping_openapi", "reader_config", "chunking", "hash", "httpx", "structlog" }) |field| self.addImport(mod, field);
    }

    pub fn configureApi(self: @This(), mod: *std.Build.Module, link_libc: bool) void {
        self.configureDatabase(mod, link_libc);
        inline for (api_imports) |field| self.addImport(mod, field);
        mod.addImport("antfly_openapi_specs", self.embedded_openapi);
    }

    pub fn configureServerless(self: @This(), b: *std.Build, mod: *std.Build.Module, link_libc: bool) void {
        self.configureDatabase(mod, link_libc);
        inline for (.{
            "inference_api", "inference_config_openapi", "middleware_openapi",
            "s3_openapi",    "scraping_openapi",         "vectorindex",
        }) |field| self.addImport(mod, field);
        addSnowballModule(b, mod);
    }

    /// This driver exercises storage and HTTP API implementations in one root.
    pub fn configureStorageBenchmark(self: @This(), b: *std.Build, mod: *std.Build.Module) void {
        self.configureStorageDependencies(b, mod, true);
        inline for (api_imports) |field| self.addImport(mod, field);
        mod.addImport("antfly_openapi_specs", self.embedded_openapi);
    }

    fn configureBase(self: @This(), mod: *std.Build.Module, link_libc: bool) void {
        self.storage_boundary.configureProfile(mod, false, false, self.boundary_profile);
        mod.addOptions("build_options", self.build_options);
        mod.addImport("antfly_platform", self.platform);
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
