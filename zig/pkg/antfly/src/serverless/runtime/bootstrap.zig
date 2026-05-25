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
const Allocator = std.mem.Allocator;
const objectstore = @import("objectstore");
const artifacts_object_store = @import("../artifacts/object_store.zig");
const manifest_object_store = @import("../manifest/object_store.zig");
const wal_object_store = @import("../wal/object_store.zig");
const catalog_object_store = @import("../catalog/object_store.zig");
const progress_object_store = @import("../catalog/object_progress_store.zig");
const remote_uri = @import("../remote_uri.zig");
const artifacts_mod = @import("../artifacts/mod.zig");
const manifest_mod = @import("../manifest/mod.zig");
const wal_mod = @import("../wal/mod.zig");
const catalog_mod = @import("../catalog/mod.zig");
const build_mod = @import("../build/mod.zig");
const query_mod = @import("../query/mod.zig");
const api_mod = @import("../api/mod.zig");
const enrichment_mod = @import("../enrichment/mod.zig");
const search_sources = @import("../search_sources.zig");
const runtime_manager = @import("manager.zig");
const managed_embedder = @import("../../inference/managed_embedder.zig");
const foreign_mod = @import("../../foreign/mod.zig");
const scraping = @import("antfly_scraping");

pub const BootstrapConfig = struct {
    artifacts_uri: []const u8,
    manifests_uri: []const u8,
    wal_uri: []const u8,
    progress_uri: []const u8,
    catalog_uri: []const u8,
    query_cache_dir: ?[]const u8 = null,
    query_cache_max_bytes: u64 = 0,
    query_cache_payload_max_bytes: u64 = 0,
    embedding_indexes_json: ?[]const u8 = null,
    sparse_embedding_index_name: []const u8 = search_sources.default_sparse_embedding_index_name,
    chunk_embedding_index_name: []const u8 = search_sources.default_chunk_embedding_index_name,
    chunk_embedding_dimensions: u32 = 8,
    tick_interval_ms: u64 = 25,
    role: api_mod.RuntimeRole = .combined,
    swarm_mode: bool = false,
    publish_enabled: bool = true,
    compaction_enabled: bool = true,
    prune_enabled: bool = true,
    enrichment_enabled: bool = true,
    foreign_registry: ?*const foreign_mod.Registry = null,
    remote_content: ?*const scraping.RemoteContentConfig = null,
};

pub const RuntimeStatus = api_mod.RuntimeStatusResult;

pub const OwnedStack = struct {
    alloc: Allocator,
    artifacts_impl: artifacts_object_store.ObjectStore,
    artifacts: artifacts_mod.ArtifactStore,
    manifests_impl: manifest_object_store.ObjectStore,
    manifests: manifest_mod.ManifestStore,
    wal_impl: wal_object_store.ObjectStore,
    wal: wal_mod.WalStore,
    progress_impl: progress_object_store.ObjectProgressStore,
    progress: catalog_mod.ProgressStore,
    catalog_impl: catalog_object_store.ObjectStore,
    catalog_store: catalog_mod.CatalogStore,
    builder: build_mod.Builder,
    catalog: catalog_mod.CatalogService,
    api: api_mod.Service,
    query_cache: ?query_mod.QueryCache = null,
    managed_query_embedder: ?managed_embedder.ManagedEmbedder = null,
    dense_query_index_name: ?[]u8 = null,
    sparse_query_index_name: []u8 = undefined,
    owned_foreign_registry: ?*foreign_mod.Registry = null,
    query: query_mod.QueryRuntime,
    runtime: runtime_manager.ManagedRuntime,
    status: RuntimeStatus,
    handler: api_mod.HttpHandler,

    pub fn init(self: *OwnedStack, alloc: Allocator, cfg: BootstrapConfig) !void {
        try validateConfig(alloc, cfg);
        self.alloc = alloc;
        self.query_cache = null;
        self.managed_query_embedder = null;
        self.dense_query_index_name = null;
        self.owned_foreign_registry = null;
        self.artifacts_impl = try artifacts_object_store.ObjectStore.initRemoteUri(alloc, cfg.artifacts_uri);
        errdefer self.artifacts_impl.deinit();
        self.artifacts = self.artifacts_impl.artifactStore();
        errdefer self.artifacts.deinit();

        self.manifests_impl = try manifest_object_store.ObjectStore.initRemoteUri(alloc, cfg.manifests_uri);
        errdefer self.manifests_impl.deinit();
        self.manifests = self.manifests_impl.manifestStore();
        errdefer self.manifests.deinit();

        self.wal_impl = try wal_object_store.ObjectStore.initRemoteUri(alloc, cfg.wal_uri);
        errdefer self.wal_impl.deinit();
        self.wal = self.wal_impl.walStore();
        errdefer self.wal.deinit();

        self.progress_impl = try progress_object_store.ObjectProgressStore.initRemoteUri(alloc, cfg.progress_uri);
        errdefer self.progress_impl.deinit();
        self.progress = self.progress_impl.progressStore();
        errdefer self.progress.deinit();

        self.catalog_impl = try catalog_object_store.ObjectStore.initRemoteUri(alloc, cfg.catalog_uri);
        errdefer self.catalog_impl.deinit();
        self.catalog_store = self.catalog_impl.catalogStore();
        errdefer self.catalog_store.deinit();

        self.builder = build_mod.Builder.init(alloc, &self.artifacts, &self.manifests, &self.progress, &self.wal);
        self.catalog = catalog_mod.CatalogService.init(alloc, &self.artifacts, &self.manifests, &self.progress, &self.wal, &self.builder, &self.catalog_store);
        self.api = api_mod.Service.init(alloc, &self.wal, &self.builder);
        if (cfg.query_cache_dir) |query_cache_dir| {
            self.query_cache = try query_mod.QueryCache.initWithConfig(alloc, query_cache_dir, .{
                .max_bytes = cfg.query_cache_max_bytes,
                .max_payload_bytes = cfg.query_cache_payload_max_bytes,
            });
            const query_cache = &self.query_cache.?;
            self.query = query_mod.QueryRuntime.initWithCache(alloc, &self.artifacts, &self.manifests, &self.progress, query_cache);
        } else {
            self.query_cache = null;
            self.query = query_mod.QueryRuntime.init(alloc, &self.artifacts, &self.manifests, &self.progress);
        }
        self.status = try runtimeStatusAlloc(alloc, cfg);
        self.runtime = runtime_manager.ManagedRuntime.init(alloc, .{
            .tick_interval_ms = cfg.tick_interval_ms,
            .role = cfg.role,
            .publish_enabled = cfg.publish_enabled,
            .compaction_enabled = cfg.compaction_enabled,
            .prune_enabled = cfg.prune_enabled,
            .enrichment_enabled = cfg.enrichment_enabled,
        }, &self.catalog, build_mod.Pruner.init(alloc, &self.artifacts, &self.manifests, &self.progress, &self.wal));
        self.runtime.setCompactor(build_mod.Compactor.init(alloc, &self.artifacts, &self.manifests, &self.progress));
        var enricher = enrichment_mod.SparseEnricher.init(alloc, &self.artifacts, &self.manifests, &self.progress, &self.wal);
        if (cfg.embedding_indexes_json) |indexes_json| {
            const embedder_options = managed_embedder.InitOptions{ .remote_content = cfg.remote_content };
            var query_embedder = try managed_embedder.ManagedEmbedder.initFromIndexesJsonWithOptions(alloc, indexes_json, embedder_options);
            errdefer query_embedder.deinit();
            if (query_embedder.hasDenseEntries()) {
                self.managed_query_embedder = query_embedder;
                self.dense_query_index_name = try alloc.dupe(u8, cfg.chunk_embedding_index_name);
            } else {
                query_embedder.deinit();
                self.managed_query_embedder = null;
                self.dense_query_index_name = null;
            }
            if (try managed_embedder.ManagedEmbedder.createSparseEmbedderWithOptions(alloc, indexes_json, embedder_options)) |sparse_embedder| {
                try enricher.setSparseEmbedder(sparse_embedder, cfg.sparse_embedding_index_name);
            }
            if (try managed_embedder.ManagedEmbedder.createDenseEmbedderWithOptions(alloc, indexes_json, embedder_options)) |dense_embedder| {
                try enricher.setChunkEmbedder(dense_embedder, cfg.chunk_embedding_index_name, cfg.chunk_embedding_dimensions);
            }
        } else {
            self.managed_query_embedder = null;
            self.dense_query_index_name = null;
        }
        self.sparse_query_index_name = try alloc.dupe(u8, cfg.sparse_embedding_index_name);
        self.runtime.setEnricher(enricher);
        self.handler = api_mod.HttpHandler.init(alloc, &self.api, &self.catalog, &self.manifests, &self.progress, &self.query, &self.status);
        self.handler.setRemoteContent(cfg.remote_content);
        if (self.query_cache) |*query_cache| self.handler.setQueryCache(query_cache);
        self.handler.setPublishedSearchSources(search_sources.publishedSearchSourcesForNames(
            self.dense_query_index_name,
            self.sparse_query_index_name,
        ));
        if (cfg.foreign_registry) |registry| {
            self.handler.setForeignRegistry(registry);
        } else {
            const registry = alloc.create(foreign_mod.Registry) catch null;
            if (registry) |value| {
                value.* = .{};
                if (foreign_mod.registerDefaultPostgresExecutor(alloc, value)) |_| {
                    self.owned_foreign_registry = value;
                    self.handler.setForeignRegistry(value);
                } else |_| {
                    value.deinit(alloc);
                    alloc.destroy(value);
                }
            }
        }
        if (self.managed_query_embedder) |*query_embedder| {
            self.handler.setManagedDenseQueryEmbedder(query_embedder, self.dense_query_index_name.?);
        }
        self.handler.setRuntimeMetrics(&self.runtime);
    }

    pub fn deinit(self: *OwnedStack) void {
        self.runtime.deinit();
        if (self.managed_query_embedder) |*query_embedder| query_embedder.deinit();
        if (self.dense_query_index_name) |index_name| self.alloc.free(index_name);
        self.alloc.free(self.sparse_query_index_name);
        if (self.owned_foreign_registry) |registry| {
            registry.deinit(self.alloc);
            self.alloc.destroy(registry);
        }
        self.status.deinit(self.alloc);
        self.query.deinit();
        if (self.query_cache) |*query_cache| query_cache.deinit();
        self.catalog_store.deinit();
        self.progress.deinit();
        self.wal.deinit();
        self.manifests.deinit();
        self.artifacts.deinit();
        self.* = undefined;
    }
};

pub fn validateConfig(alloc: Allocator, cfg: BootstrapConfig) !void {
    if (cfg.tick_interval_ms == 0) return error.InvalidTickInterval;
    if (cfg.query_cache_dir) |path| {
        if (std.mem.trim(u8, path, &std.ascii.whitespace).len == 0) return error.InvalidQueryCacheDir;
    }

    var requires_s3 = false;
    var requires_gcs = false;

    for ([_][]const u8{
        cfg.artifacts_uri,
        cfg.manifests_uri,
        cfg.wal_uri,
        cfg.progress_uri,
        cfg.catalog_uri,
    }) |uri| {
        var parsed = try remote_uri.parseAlloc(alloc, uri);
        defer switch (parsed) {
            .file => |value| alloc.free(value),
            .gcs => |*value| value.deinit(alloc),
            .s3 => |*value| value.deinit(alloc),
        };

        switch (parsed) {
            .file => {},
            .gcs => requires_gcs = true,
            .s3 => requires_s3 = true,
        }
    }

    if (requires_s3) {
        var s3_cfg = try objectstore.S3.fromEnvAlloc(alloc, null, true, null, null, null, null, .path);
        s3_cfg.deinit(alloc);
    }

    if (requires_gcs) {
        var gcs_cfg = try objectstore.Gcs.jsonApiClientConfigFromEnvAlloc(alloc);
        gcs_cfg.deinit(alloc);
    }
}

pub fn runtimeStatusAlloc(alloc: Allocator, cfg: BootstrapConfig) !RuntimeStatus {
    const targets = try alloc.alloc(api_mod.RuntimeStorageTarget, 5);
    errdefer alloc.free(targets);

    const lanes = [_][]const u8{ "artifacts", "manifests", "wal", "progress", "catalog" };
    const uris = [_][]const u8{
        cfg.artifacts_uri,
        cfg.manifests_uri,
        cfg.wal_uri,
        cfg.progress_uri,
        cfg.catalog_uri,
    };

    var initialized: usize = 0;
    errdefer {
        for (targets[0..initialized]) |*target| target.deinit(alloc);
        alloc.free(targets);
    }

    for (lanes, uris, 0..) |lane, uri, idx| {
        targets[idx] = try targetFromUriAlloc(alloc, lane, uri);
        initialized += 1;
    }

    return .{
        .role = cfg.role,
        .swarm_mode = cfg.swarm_mode,
        .tick_interval_ms = cfg.tick_interval_ms,
        .validated = true,
        .publish_enabled = cfg.publish_enabled,
        .compaction_enabled = cfg.compaction_enabled,
        .prune_enabled = cfg.prune_enabled,
        .enrichment_enabled = cfg.enrichment_enabled,
        .published_search_sources = try search_sources.publishedSearchSourcesForNamesAlloc(
            alloc,
            if (cfg.embedding_indexes_json != null) cfg.chunk_embedding_index_name else null,
            cfg.sparse_embedding_index_name,
        ),
        .targets = targets,
    };
}

fn targetFromUriAlloc(alloc: Allocator, lane: []const u8, uri: []const u8) !api_mod.RuntimeStorageTarget {
    var parsed = try remote_uri.parseAlloc(alloc, uri);
    defer switch (parsed) {
        .file => |value| alloc.free(value),
        .gcs => |*value| value.deinit(alloc),
        .s3 => |*value| value.deinit(alloc),
    };

    return switch (parsed) {
        .file => |path| .{
            .lane = try alloc.dupe(u8, lane),
            .uri = try alloc.dupe(u8, uri),
            .backend = .file,
            .path = try alloc.dupe(u8, path),
        },
        .gcs => |value| .{
            .lane = try alloc.dupe(u8, lane),
            .uri = try alloc.dupe(u8, uri),
            .backend = .gs,
            .bucket = try alloc.dupe(u8, value.bucket),
            .prefix = try alloc.dupe(u8, value.prefix),
        },
        .s3 => |value| .{
            .lane = try alloc.dupe(u8, lane),
            .uri = try alloc.dupe(u8, uri),
            .backend = .s3,
            .bucket = try alloc.dupe(u8, value.bucket),
            .prefix = try alloc.dupe(u8, value.prefix),
        },
    };
}

test "runtime bootstrap assembles serverless stack from uri config" {
    const alloc = std.testing.allocator;

    var artifacts_buf: [256]u8 = undefined;
    var manifests_buf: [256]u8 = undefined;
    var wal_buf: [256]u8 = undefined;
    var progress_buf: [256]u8 = undefined;
    var catalog_buf: [256]u8 = undefined;
    const artifacts_root = tmpPath(&artifacts_buf, "bootstrap-artifacts");
    const manifests_root = tmpPath(&manifests_buf, "bootstrap-manifests");
    const wal_root = tmpPath(&wal_buf, "bootstrap-wal");
    const progress_root = tmpPath(&progress_buf, "bootstrap-progress");
    const catalog_root = tmpPath(&catalog_buf, "bootstrap-catalog");
    defer cleanupTmp(artifacts_root);
    defer cleanupTmp(manifests_root);
    defer cleanupTmp(wal_root);
    defer cleanupTmp(progress_root);
    defer cleanupTmp(catalog_root);

    const artifacts_uri = try std.fmt.allocPrint(alloc, "file://{s}", .{std.mem.span(artifacts_root)});
    defer alloc.free(artifacts_uri);
    const manifests_uri = try std.fmt.allocPrint(alloc, "file://{s}", .{std.mem.span(manifests_root)});
    defer alloc.free(manifests_uri);
    const wal_uri = try std.fmt.allocPrint(alloc, "file://{s}", .{std.mem.span(wal_root)});
    defer alloc.free(wal_uri);
    const progress_uri = try std.fmt.allocPrint(alloc, "file://{s}", .{std.mem.span(progress_root)});
    defer alloc.free(progress_uri);
    const catalog_uri = try std.fmt.allocPrint(alloc, "file://{s}", .{std.mem.span(catalog_root)});
    defer alloc.free(catalog_uri);

    var stack: OwnedStack = undefined;
    try stack.init(alloc, .{
        .artifacts_uri = artifacts_uri,
        .manifests_uri = manifests_uri,
        .wal_uri = wal_uri,
        .progress_uri = progress_uri,
        .catalog_uri = catalog_uri,
        .query_cache_dir = null,
        .tick_interval_ms = 1,
        .role = .combined,
        .swarm_mode = true,
    });
    defer stack.deinit();

    try std.testing.expect(stack.status.swarm_mode);

    try std.testing.expect(try stack.catalog.ensureTable("docs", 100));

    const mutations = [_]api_mod.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "alpha" },
    };
    var ingest = try stack.api.ingestTableBatch(.{
        .table_name = "docs",
        .timestamp_ns = 123,
        .mutations = &mutations,
    });
    defer ingest.deinit(alloc);

    var build = try stack.catalog.buildTable("docs");
    defer build.deinit(alloc);
    try std.testing.expect(build.published);

    var resp = try stack.handler.handle(.{
        .method = .get,
        .path = "/tables/docs/query/published",
    });
    defer resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), resp.status);
    try std.testing.expect(std.mem.indexOf(u8, resp.body, "\"table_name\":\"docs\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, resp.body, "\"doc_id\":\"doc-a\"") != null);
}

test "runtime bootstrap wires foreign registry into public join handler" {
    const alloc = std.testing.allocator;

    const DummyForeign = struct {
        fn destroy(ptr: *anyopaque, inner_alloc: std.mem.Allocator) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            inner_alloc.destroy(self);
        }

        fn query(_: *anyopaque, inner_alloc: std.mem.Allocator, _: foreign_mod.QueryParams) !foreign_mod.QueryResult {
            const rows = try inner_alloc.alloc(std.json.Value, 1);
            rows[0] = try std.json.parseFromSliceLeaky(std.json.Value, inner_alloc, "{\"id\":\"cust:a\",\"name\":\"Alice\"}", .{});
            return .{ .rows = rows, .total = 1 };
        }

        fn statistics(_: *anyopaque, _: []const u8) !foreign_mod.TableStatistics {
            return .{ .row_count = 1, .size_bytes = 64 };
        }

        fn factory(inner_alloc: std.mem.Allocator, config: foreign_mod.Config) !foreign_mod.Source {
            var owned = config;
            defer owned.deinit(inner_alloc);
            const self = try inner_alloc.create(@This());
            return .{
                .ptr = self,
                .vtable = &.{
                    .deinit = destroy,
                    .query = query,
                    .statistics = statistics,
                },
            };
        }
    };

    var artifacts_buf: [256]u8 = undefined;
    var manifests_buf: [256]u8 = undefined;
    var wal_buf: [256]u8 = undefined;
    var progress_buf: [256]u8 = undefined;
    var catalog_buf: [256]u8 = undefined;
    const artifacts_root = tmpPath(&artifacts_buf, "bootstrap-foreign-artifacts");
    const manifests_root = tmpPath(&manifests_buf, "bootstrap-foreign-manifests");
    const wal_root = tmpPath(&wal_buf, "bootstrap-foreign-wal");
    const progress_root = tmpPath(&progress_buf, "bootstrap-foreign-progress");
    const catalog_root = tmpPath(&catalog_buf, "bootstrap-foreign-catalog");
    defer cleanupTmp(artifacts_root);
    defer cleanupTmp(manifests_root);
    defer cleanupTmp(wal_root);
    defer cleanupTmp(progress_root);
    defer cleanupTmp(catalog_root);

    const artifacts_uri = try std.fmt.allocPrint(alloc, "file://{s}", .{std.mem.span(artifacts_root)});
    defer alloc.free(artifacts_uri);
    const manifests_uri = try std.fmt.allocPrint(alloc, "file://{s}", .{std.mem.span(manifests_root)});
    defer alloc.free(manifests_uri);
    const wal_uri = try std.fmt.allocPrint(alloc, "file://{s}", .{std.mem.span(wal_root)});
    defer alloc.free(wal_uri);
    const progress_uri = try std.fmt.allocPrint(alloc, "file://{s}", .{std.mem.span(progress_root)});
    defer alloc.free(progress_uri);
    const catalog_uri = try std.fmt.allocPrint(alloc, "file://{s}", .{std.mem.span(catalog_root)});
    defer alloc.free(catalog_uri);

    var foreign_registry = foreign_mod.Registry{};
    defer foreign_registry.deinit(alloc);
    try foreign_registry.register(alloc, .postgres, DummyForeign.factory);

    var stack: OwnedStack = undefined;
    try stack.init(alloc, .{
        .artifacts_uri = artifacts_uri,
        .manifests_uri = manifests_uri,
        .wal_uri = wal_uri,
        .progress_uri = progress_uri,
        .catalog_uri = catalog_uri,
        .query_cache_dir = null,
        .tick_interval_ms = 1,
        .role = .combined,
        .swarm_mode = true,
        .foreign_registry = &foreign_registry,
    });
    defer stack.deinit();

    try std.testing.expect(try stack.catalog.ensureTable("orders", 100));

    const mutations = [_]api_mod.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "order:1", .body = "{\"body\":\"alpha order\",\"customer_id\":\"cust:a\"}" },
    };
    var ingest = try stack.api.ingestTableBatch(.{
        .table_name = "orders",
        .timestamp_ns = 123,
        .mutations = &mutations,
    });
    defer ingest.deinit(alloc);

    var build = try stack.catalog.buildTable("orders");
    defer build.deinit(alloc);
    try std.testing.expect(build.published);

    var resp = try stack.handler.handle(.{
        .method = .post,
        .path = "/tables/orders/query",
        .body =
        \\{"full_text_search":{"query":"body:order"},"fields":["customer_id"],"join":{"right_table":"pg_customers","join_type":"inner","on":{"left_field":"customer_id","right_field":"id","operator":"eq"}},"foreign_sources":{"pg_customers":{"type":"postgres","dsn":"postgres://db","postgres_table":"customers"}}}
        ,
    });
    defer resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), resp.status);
    try std.testing.expect(std.mem.indexOf(u8, resp.body, "\"pg_customers.name\":\"Alice\"") != null);
}

test "runtime bootstrap supports published semantic search with embedding_template and remoteText" {
    const alloc = std.testing.allocator;
    const http_common = @import("../../raft/transport/http_common.zig");
    const std_http_listener = @import("../../raft/transport/std_http_listener.zig");

    const FakeEmbeddingProvider = struct {
        fn executor() http_common.RequestExecutor {
            return .{ .ptr = undefined, .vtable = &.{ .execute = execute } };
        }

        fn execute(_: *anyopaque, req_alloc: Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
            try std.testing.expectEqual(http_common.Method.POST, req.method);
            try std.testing.expect(std.mem.endsWith(u8, req.uri, "/v1/embeddings"));
            const alpha = std.mem.indexOf(u8, req.body, "alpha") != null;
            const beta = std.mem.indexOf(u8, req.body, "beta") != null;
            const vector = if (alpha and !beta)
                "[1,0,0]"
            else if (beta and !alpha)
                "[0,1,0]"
            else
                "[0.5,0.5,0]";
            const body = try std.fmt.allocPrint(
                req_alloc,
                "{{\"object\":\"list\",\"data\":[{{\"object\":\"embedding\",\"index\":0,\"embedding\":{s}}}],\"model\":\"text-embedding-3-small\",\"usage\":{{\"prompt_tokens\":1,\"total_tokens\":1}}}}",
                .{vector},
            );
            return .{
                .status = 200,
                .content_type = try req_alloc.dupe(u8, "application/json"),
                .body = body,
            };
        }
    };

    const FakeRemoteAssets = struct {
        fn executor() http_common.RequestExecutor {
            return .{ .ptr = undefined, .vtable = &.{ .execute = execute } };
        }

        fn execute(_: *anyopaque, req_alloc: Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
            try std.testing.expectEqual(http_common.Method.GET, req.method);
            if (std.mem.endsWith(u8, req.uri, "/alpha.txt")) {
                return .{
                    .status = 200,
                    .content_type = try req_alloc.dupe(u8, "text/plain"),
                    .body = try req_alloc.dupe(u8, "alpha transcript"),
                };
            }
            if (std.mem.endsWith(u8, req.uri, "/beta.txt")) {
                return .{
                    .status = 200,
                    .content_type = try req_alloc.dupe(u8, "text/plain"),
                    .body = try req_alloc.dupe(u8, "beta transcript"),
                };
            }
            if (std.mem.endsWith(u8, req.uri, "/kitten.png")) {
                return .{
                    .status = 200,
                    .content_type = try req_alloc.dupe(u8, "image/png"),
                    .body = try req_alloc.dupe(u8, "png"),
                };
            }
            return .{
                .status = 404,
                .content_type = try req_alloc.dupe(u8, "text/plain"),
                .body = try req_alloc.dupe(u8, "missing"),
            };
        }
    };

    var artifacts_buf: [256]u8 = undefined;
    var manifests_buf: [256]u8 = undefined;
    var wal_buf: [256]u8 = undefined;
    var progress_buf: [256]u8 = undefined;
    var catalog_buf: [256]u8 = undefined;
    const artifacts_root = tmpPath(&artifacts_buf, "bootstrap-semantic-artifacts");
    const manifests_root = tmpPath(&manifests_buf, "bootstrap-semantic-manifests");
    const wal_root = tmpPath(&wal_buf, "bootstrap-semantic-wal");
    const progress_root = tmpPath(&progress_buf, "bootstrap-semantic-progress");
    const catalog_root = tmpPath(&catalog_buf, "bootstrap-semantic-catalog");
    defer cleanupTmp(artifacts_root);
    defer cleanupTmp(manifests_root);
    defer cleanupTmp(wal_root);
    defer cleanupTmp(progress_root);
    defer cleanupTmp(catalog_root);

    var embed_listener = std_http_listener.StdHttpListener.init(alloc, .{}, FakeEmbeddingProvider.executor());
    defer embed_listener.deinit();
    try embed_listener.start();
    const embed_base_uri = try embed_listener.baseUri(alloc);
    defer alloc.free(embed_base_uri);

    var remote_listener = std_http_listener.StdHttpListener.init(alloc, .{}, FakeRemoteAssets.executor());
    defer remote_listener.deinit();
    try remote_listener.start();
    const remote_base_uri = try remote_listener.baseUri(alloc);
    defer alloc.free(remote_base_uri);

    const artifacts_uri = try std.fmt.allocPrint(alloc, "file://{s}", .{std.mem.span(artifacts_root)});
    defer alloc.free(artifacts_uri);
    const manifests_uri = try std.fmt.allocPrint(alloc, "file://{s}", .{std.mem.span(manifests_root)});
    defer alloc.free(manifests_uri);
    const wal_uri = try std.fmt.allocPrint(alloc, "file://{s}", .{std.mem.span(wal_root)});
    defer alloc.free(wal_uri);
    const progress_uri = try std.fmt.allocPrint(alloc, "file://{s}", .{std.mem.span(progress_root)});
    defer alloc.free(progress_uri);
    const catalog_uri = try std.fmt.allocPrint(alloc, "file://{s}", .{std.mem.span(catalog_root)});
    defer alloc.free(catalog_uri);
    const indexes_json = try std.fmt.allocPrint(alloc,
        \\{{"serverless_chunk":{{"type":"embeddings","field":"text","dimension":3,"embedder":{{"provider":"openai","model":"text-embedding-3-small","url":"{s}"}}}},"serverless_sparse":{{"type":"embeddings","field":"text","sparse":true}}}}
    , .{embed_base_uri});
    defer alloc.free(indexes_json);

    var stack: OwnedStack = undefined;
    try stack.init(alloc, .{
        .artifacts_uri = artifacts_uri,
        .manifests_uri = manifests_uri,
        .wal_uri = wal_uri,
        .progress_uri = progress_uri,
        .catalog_uri = catalog_uri,
        .embedding_indexes_json = indexes_json,
        .chunk_embedding_index_name = "serverless_chunk",
        .chunk_embedding_dimensions = 3,
        .query_cache_dir = null,
        .tick_interval_ms = 1,
        .role = .combined,
    });
    defer stack.deinit();

    try std.testing.expect(try stack.catalog.ensureTableWithDefinition(
        "docs",
        100,
        .{
            .default_query_view = .published,
            .chunk_preview_enabled = true,
            .chunk_embeddings_enabled = true,
        },
        "",
        "",
        indexes_json,
    ));

    const alpha_url = try std.fmt.allocPrint(alloc, "{s}/alpha.txt", .{remote_base_uri});
    defer alloc.free(alpha_url);
    const beta_url = try std.fmt.allocPrint(alloc, "{s}/beta.txt", .{remote_base_uri});
    defer alloc.free(beta_url);
    const mutations = [_]api_mod.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = try std.fmt.allocPrint(alloc, "{{\"text\":\"alpha bravo\",\"transcript\":{f},\"sparse_embedding\":{{\"1\":1.0,\"2\":0.5}},\"graph_edges\":[{{\"target\":\"doc-b\",\"edge_type\":\"cites\",\"weight\":1.0}}]}}", .{std.json.fmt(alpha_url, .{})}) },
        .{ .kind = .upsert, .doc_id = "doc-b", .body = try std.fmt.allocPrint(alloc, "{{\"text\":\"beta charlie\",\"transcript\":{f},\"sparse_embedding\":{{\"9\":1.0}},\"graph_edges\":[{{\"target\":\"doc-a\",\"edge_type\":\"related\",\"weight\":1.0}}]}}", .{std.json.fmt(beta_url, .{})}) },
    };
    defer {
        alloc.free(mutations[0].body.?);
        alloc.free(mutations[1].body.?);
    }
    var ingest = try stack.api.ingestTableBatch(.{
        .table_name = "docs",
        .timestamp_ns = 123,
        .mutations = &mutations,
    });
    defer ingest.deinit(alloc);

    var build = try stack.catalog.buildTable("docs");
    defer build.deinit(alloc);
    try std.testing.expect(build.published);

    var rounds: usize = 0;
    while (rounds < 8) : (rounds += 1) _ = try stack.runtime.runOnce();

    var status = try stack.catalog.buildStatus("docs");
    defer status.deinit(alloc);
    try std.testing.expect(status.chunk_preview_complete);
    try std.testing.expect(status.chunk_embeddings_complete);

    const query_url = try std.fmt.allocPrint(alloc, "{s}/alpha.txt", .{remote_base_uri});
    defer alloc.free(query_url);
    const search_body = try std.fmt.allocPrint(alloc,
        \\{{"semantic_search":{f},"embedding_template":"{{{{remoteText url=this}}}}","indexes":["serverless_chunk"],"limit":5}}
    , .{std.json.fmt(query_url, .{})});
    defer alloc.free(search_body);

    var resp = try stack.handler.handle(.{
        .method = .post,
        .path = "/tables/docs/query/search",
        .body = search_body,
    });
    defer resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), resp.status);
    try std.testing.expect(std.mem.indexOf(u8, resp.body, "\"table_name\":\"docs\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, resp.body, "\"doc_id\":\"doc-a\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, resp.body, "\"mode\":\"vector\"") != null);

    var direct_resp = try stack.handler.handle(.{
        .method = .post,
        .path = "/tables/docs/query/search",
        .body = "{\"embeddings\":{\"serverless_chunk\":[1,0,0]},\"limit\":5}",
    });
    defer direct_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), direct_resp.status);
    try std.testing.expect(std.mem.indexOf(u8, direct_resp.body, "\"doc_id\":\"doc-a\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, direct_resp.body, "\"mode\":\"vector\"") != null);

    var sparse_resp = try stack.handler.handle(.{
        .method = .post,
        .path = "/tables/docs/query/search",
        .body = "{\"embeddings\":{\"serverless_sparse\":{\"indices\":[1,2],\"values\":[1.0,0.5]}},\"limit\":5}",
    });
    defer sparse_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), sparse_resp.status);
    try std.testing.expect(std.mem.indexOf(u8, sparse_resp.body, "\"doc_id\":\"doc-a\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, sparse_resp.body, "\"mode\":\"sparse\"") != null);

    var dense_graph_resp = try stack.handler.handle(.{
        .method = .post,
        .path = "/tables/docs/query",
        .body = "{\"embeddings\":{\"serverless_chunk\":[1,0,0]},\"graph_searches\":{\"neighbors_from_dense\":{\"type\":\"neighbors\",\"index_name\":\"graph_idx\",\"start_nodes\":{\"result_ref\":\"$embeddings_results\",\"limit\":1},\"params\":{\"edge_types\":[\"cites\"]}}},\"limit\":5}",
    });
    defer dense_graph_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), dense_graph_resp.status);
    try std.testing.expect(std.mem.indexOf(u8, dense_graph_resp.body, "\"neighbors_from_dense\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, dense_graph_resp.body, "\"key\":\"doc-b\"") != null);

    var sparse_graph_resp = try stack.handler.handle(.{
        .method = .post,
        .path = "/tables/docs/query",
        .body = "{\"embeddings\":{\"serverless_sparse\":{\"indices\":[1,2],\"values\":[1.0,0.5]}},\"graph_searches\":{\"neighbors_from_sparse\":{\"type\":\"neighbors\",\"index_name\":\"graph_idx\",\"start_nodes\":{\"result_ref\":\"$embeddings_results\",\"limit\":1},\"params\":{\"edge_types\":[\"cites\"]}}},\"limit\":5}",
    });
    defer sparse_graph_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), sparse_graph_resp.status);
    try std.testing.expect(std.mem.indexOf(u8, sparse_graph_resp.body, "\"neighbors_from_sparse\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, sparse_graph_resp.body, "\"key\":\"doc-b\"") != null);

    var hybrid_resp = try stack.handler.handle(.{
        .method = .post,
        .path = "/tables/docs/query/search",
        .body = "{\"full_text_search\":{\"match\":{\"field\":\"body\",\"text\":\"alpha\"}},\"embeddings\":{\"serverless_chunk\":[1,0,0]},\"limit\":5}",
    });
    defer hybrid_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), hybrid_resp.status);
    try std.testing.expect(std.mem.indexOf(u8, hybrid_resp.body, "\"doc_id\":\"doc-a\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, hybrid_resp.body, "\"mode\":\"hybrid\"") != null);

    const bad_query_url = try std.fmt.allocPrint(alloc, "{s}/kitten.png", .{remote_base_uri});
    defer alloc.free(bad_query_url);
    const bad_search_body = try std.fmt.allocPrint(alloc,
        \\{{"semantic_search":{f},"embedding_template":"{{{{remoteText url=this}}}}","indexes":["serverless_chunk"],"limit":5}}
    , .{std.json.fmt(bad_query_url, .{})});
    defer alloc.free(bad_search_body);

    var bad_resp = try stack.handler.handle(.{
        .method = .post,
        .path = "/tables/docs/query/search",
        .body = bad_search_body,
    });
    defer bad_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 400), bad_resp.status);
}

test "runtime bootstrap validation rejects unsupported uri scheme" {
    const alloc = std.testing.allocator;
    try std.testing.expectError(error.UnsupportedRemoteUri, validateConfig(alloc, .{
        .artifacts_uri = "r2://bucket/artifacts",
        .manifests_uri = "file:///tmp/antfly-manifests",
        .wal_uri = "file:///tmp/antfly-wal",
        .progress_uri = "file:///tmp/antfly-progress",
        .catalog_uri = "file:///tmp/antfly-catalog",
    }));
}

test "runtime bootstrap validation rejects invalid remote uri" {
    const alloc = std.testing.allocator;
    try std.testing.expectError(error.InvalidRemoteUri, validateConfig(alloc, .{
        .artifacts_uri = "s3://",
        .manifests_uri = "file:///tmp/antfly-manifests",
        .wal_uri = "file:///tmp/antfly-wal",
        .progress_uri = "file:///tmp/antfly-progress",
        .catalog_uri = "file:///tmp/antfly-catalog",
    }));
}

test "runtime bootstrap validation rejects zero tick interval" {
    const alloc = std.testing.allocator;
    try std.testing.expectError(error.InvalidTickInterval, validateConfig(alloc, .{
        .artifacts_uri = "file:///tmp/antfly-artifacts",
        .manifests_uri = "file:///tmp/antfly-manifests",
        .wal_uri = "file:///tmp/antfly-wal",
        .progress_uri = "file:///tmp/antfly-progress",
        .catalog_uri = "file:///tmp/antfly-catalog",
        .tick_interval_ms = 0,
    }));
}

test "runtime status describes configured backends" {
    const alloc = std.testing.allocator;
    var status = try runtimeStatusAlloc(alloc, .{
        .artifacts_uri = "file:///tmp/antfly-artifacts",
        .manifests_uri = "s3://bucket/manifests/dev",
        .wal_uri = "gs://bucket/wal/dev",
        .progress_uri = "file:///tmp/antfly-progress",
        .catalog_uri = "s3://bucket/catalog/dev",
        .tick_interval_ms = 25,
    });
    defer status.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 5), status.targets.len);
    try std.testing.expectEqual(api_mod.RuntimeStorageBackend.file, status.targets[0].backend);
    try std.testing.expectEqualStrings("/tmp/antfly-artifacts", status.targets[0].path.?);
    try std.testing.expectEqual(api_mod.RuntimeStorageBackend.s3, status.targets[1].backend);
    try std.testing.expectEqualStrings("bucket", status.targets[1].bucket.?);
    try std.testing.expectEqualStrings("manifests/dev", status.targets[1].prefix.?);
    try std.testing.expectEqual(api_mod.RuntimeStorageBackend.gs, status.targets[2].backend);
    try std.testing.expectEqualStrings("wal/dev", status.targets[2].prefix.?);
    try std.testing.expect(status.published_search_sources.findVector() == null);
    try std.testing.expectEqualStrings(search_sources.default_sparse_embedding_index_name, status.published_search_sources.findSparse().?.index_name);
    try std.testing.expectEqual(search_sources.SparseDocumentSource.sparse_embedding, status.published_search_sources.findSparse().?.document_source);
}

var test_nonce: std.atomic.Value(u64) = .init(0);

fn nowNs() u64 {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const now = std.Io.Timestamp.now(io_impl.io(), .awake);
    return @intCast(now.toNanoseconds());
}

fn tmpPath(buf: []u8, label: []const u8) [*:0]const u8 {
    const nonce = test_nonce.fetchAdd(1, .monotonic);
    const slice = std.fmt.bufPrint(buf, "/tmp/antfly-serverless-{s}-{d}-{d}\x00", .{ label, nowNs(), nonce }) catch unreachable;
    return @ptrCast(slice.ptr);
}

fn cleanupTmp(path: [*:0]const u8) void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), std.mem.span(path)) catch {};
}
