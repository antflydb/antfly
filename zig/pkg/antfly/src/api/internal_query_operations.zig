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
const CancellationToken = @import("antfly_cancellation").CancellationToken;
const metadata_api = @import("../metadata/api.zig");
const metadata_table_manager = @import("../metadata/table_manager.zig");
const metadata_transition_state = @import("../metadata/transition_state.zig");
const managed_embedder = @import("antfly_local_sources").inference_managed_embedder;
const query_embedding_cache = @import("antfly_inference_query_embedding_cache");
const cache_budget = @import("antfly_cache_budget");
const common_secrets = @import("antfly_local_sources").common_secrets;
const platform_time = @import("antfly_platform").time;
const scraping = @import("antfly_scraping");
const db_mod = @import("antfly_source_root").antfly_sources.selected_db;

pub const max_query_embedding_input_bytes: usize = 1024 * 1024;
pub const max_query_embedding_template_bytes: usize = 64 * 1024;
pub const default_query_embedding_timeout_ns: u64 = 30 * std.time.ns_per_s;
const db_embedder = @import("antfly_local_sources").storage_db_enrichment_embedder;
const algebraic_ir = @import("antfly_local_sources").storage_db_algebraic_ir;
const distributed_graph = @import("distributed_graph.zig");
const query_api = @import("antfly_local_sources").api_query;
const query_contract = @import("antfly_local_sources").api_query_contract;
const tables_api = @import("tables.zig");
const raft_reconciler = @import("../raft/reconciler.zig");

pub fn normalizeQueryEmbeddingOperationalError(err: anyerror) ?anyerror {
    return switch (err) {
        error.ProviderTokenBudgetExceeded => error.QueryEmbeddingInputTooLarge,
        error.ProviderQuotaRegistryFull => error.QueryEmbeddingOverloaded,
        error.QueueFull,
        error.EmbedAdmissionRefused,
        error.InferenceCapacityUnavailable,
        error.InferenceCapabilitiesStale,
        error.RemoteCapabilityDiscoveryTransient,
        error.RemoteCapabilitySelectorAbsent,
        error.TlsTransportReadFailed,
        => error.EmbedTransientFailure,
        error.QueryEmbeddingInputTooLarge,
        error.QueryEmbeddingOverloaded,
        error.EmbedRateLimited,
        error.EmbedTransientFailure,
        error.EmbedUpstreamFailure,
        error.Timeout,
        => err,
        error.EmbedRequestFailed,
        error.EmptyEmbeddingResponse,
        error.InvalidEmbeddingResponse,
        error.InvalidEmbeddingIdentity,
        error.EmbeddingIdentityMismatch,
        error.EmptyResponse,
        error.InvalidEmbeddingDimensions,
        error.SecretNotFound,
        error.InferenceEndpointAccessDenied,
        error.RemoteCapabilityDiscoveryRejected,
        error.InvalidInferenceCapabilities,
        error.IncompleteCapabilityLease,
        error.InvalidCapabilityRoutingToken,
        error.InvalidCapabilityRevision,
        error.InferenceCapabilitiesUnavailable,
        => error.EmbedUpstreamFailure,
        error.ConnectionRefused,
        error.ConnectionResetByPeer,
        error.NetworkUnreachable,
        error.HostUnreachable,
        error.TemporaryNameServerFailure,
        error.NameServerFailure,
        error.ConnectionTimedOut,
        error.Canceled,
        => error.EmbedTransientFailure,
        else => null,
    };
}

pub const CatalogSource = struct {
    ptr: *anyopaque,
    admin_snapshot: ?*const fn (ptr: *anyopaque) anyerror!metadata_api.AdminSnapshot = null,
    free_admin_snapshot: ?*const fn (ptr: *anyopaque, snapshot: *metadata_api.AdminSnapshot) void = null,

    fn adminSnapshot(self: CatalogSource) !?metadata_api.AdminSnapshot {
        const fn_ptr = self.admin_snapshot orelse return null;
        return try fn_ptr(self.ptr);
    }

    fn freeAdminSnapshot(self: CatalogSource, snapshot: *metadata_api.AdminSnapshot) void {
        const fn_ptr = self.free_admin_snapshot orelse return;
        fn_ptr(self.ptr, snapshot);
    }
};

pub const QueryRouter = struct {
    ptr: *anyopaque,
    route_query_to_read_schema: *const fn (ptr: *anyopaque, table_name: []const u8, req: *db_mod.types.SearchRequest) anyerror!void,

    fn route(self: QueryRouter, table_name: []const u8, req: *db_mod.types.SearchRequest) !void {
        return try self.route_query_to_read_schema(self.ptr, table_name, req);
    }
};

pub const Context = struct {
    catalog: CatalogSource,
    query_router: QueryRouter,
    query_planning: ?QueryPlanningContext = null,
    graph_execution_limits: @import("antfly_local_sources").graph_work_budget.Limits = .{},

    fn queryPlanning(self: Context) ?QueryPlanningContext {
        if (self.query_planning) |planning| return planning;
        return .{
            .ptr = self.catalog.ptr,
            .admin_snapshot = self.catalog.admin_snapshot orelse return null,
            .free_admin_snapshot = self.catalog.free_admin_snapshot orelse return null,
        };
    }

    /// Parse and semantically plan an internal query without depending on an
    /// HTTP request or response type. The returned request is owned by
    /// `alloc`.
    pub fn planQuery(
        self: Context,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        body: []const u8,
    ) !query_api.OwnedQueryRequest {
        var semantic_resolver = SemanticStatusResolver{ .planning = self.queryPlanning() };
        var request = try query_api.parseQueryRequest(alloc, semantic_resolver.iface(), table_name, body);
        request.req.graph_execution_limits = self.graph_execution_limits;
        return request;
    }

    /// Route a planned request to the schema generation that owns the read.
    pub fn routeQuery(self: Context, table_name: []const u8, req: *db_mod.types.SearchRequest) !void {
        return self.query_router.route(table_name, req);
    }
};

pub const QueryPlanningContext = struct {
    retry_not_before_ns: ?*u64 = null,
    /// Borrowed from the coordinator's immutable query catalog cut. Carrying
    /// the selected producer config avoids another metadata read after alias
    /// resolution and excludes control-state receipts from embedder parsing.
    indexes_json: ?[]const u8 = null,
    ptr: *anyopaque,
    admin_snapshot: *const fn (ptr: *anyopaque) anyerror!metadata_api.AdminSnapshot,
    free_admin_snapshot: *const fn (ptr: *anyopaque, snapshot: *metadata_api.AdminSnapshot) void,
    antfly_provider: ?managed_embedder.AntflyProvider = null,
    io: ?std.Io = null,
    remote_content: ?*const scraping.RemoteContentConfig = null,
    inference_api_url: ?[]const u8 = null,
    inference_api_key: ?[]const u8 = null,
    secret_store: ?*common_secrets.FileStore = null,
    provider_runtime: ?*managed_embedder.ProviderRuntime = null,
    query_embedding_cache: ?*query_embedding_cache.QueryEmbeddingCache = null,
    query_embedding_budget: ?*cache_budget.CacheBudget = null,
    query_embedding_security_domain: managed_embedder.QueryCacheSecurityDomain = .internal,
    query_embedding_security_scope: []const u8 = "internal",
    query_embedding_deadline_ns: ?u64 = null,
    query_cancellation: ?CancellationToken = null,
    request_id: []const u8 = "",

    fn adminSnapshot(self: QueryPlanningContext) !metadata_api.AdminSnapshot {
        return try self.admin_snapshot(self.ptr);
    }

    fn freeAdminSnapshot(self: QueryPlanningContext, snapshot: *metadata_api.AdminSnapshot) void {
        self.free_admin_snapshot(self.ptr, snapshot);
    }
};

pub const QueryEmbeddingCache = struct {
    cache: *query_embedding_cache.QueryEmbeddingCache,
    budget: *cache_budget.CacheBudget,
};

pub const QueryEmbeddingOptions = struct {
    retry_not_before_ns: ?*u64 = null,
    storage: ?QueryEmbeddingCache = null,
    domain: managed_embedder.QueryCacheSecurityDomain = .internal,
    scope: []const u8 = "internal",
    deadline_ns: ?u64 = null,
    cancellation: ?CancellationToken = null,
    request_id: []const u8 = "",
};

const QueryComputeContext = struct {
    prepared: *const managed_embedder.ManagedEmbedder.PreparedQueryEmbedding,
    text: []const u8,
    embedding_template: ?[]const u8,
    retry_not_before_ns: ?*u64,

    fn run(ptr: *anyopaque, alloc: std.mem.Allocator) ![]f32 {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        _ = db_mod.enrichment_types.interactive_embed_inflight.fetchAdd(1, .monotonic);
        defer _ = db_mod.enrichment_types.interactive_embed_inflight.fetchSub(1, .monotonic);
        return self.prepared.embedWithRetryHint(alloc, self.text, self.embedding_template, self.retry_not_before_ns);
    }
};

pub fn computeManagedQueryVector(
    runtime: *const managed_embedder.ManagedEmbedder,
    alloc: std.mem.Allocator,
    index_name: []const u8,
    text: []const u8,
    embedding_template: ?[]const u8,
    options: QueryEmbeddingOptions,
) ![]f32 {
    if (text.len > max_query_embedding_input_bytes) return error.QueryEmbeddingInputTooLarge;
    if (embedding_template) |value| {
        if (value.len > max_query_embedding_template_bytes) return error.QueryEmbeddingInputTooLarge;
    }
    if (options.retry_not_before_ns) |hint| hint.* = 0;
    const deadline = effectiveQueryEmbeddingDeadlineNs(options.deadline_ns, platform_time.monotonicNs());
    var attempt: usize = 0;
    while (true) : (attempt += 1) {
        if (deadline <= platform_time.monotonicNs()) return error.Timeout;
        var prepared = runtime.prepareQueryEmbedding(alloc, index_name, deadline, options.cancellation, options.request_id, options.retry_not_before_ns, embedding_template != null) catch |err| {
            if (err == error.InferenceCapabilitiesStale and attempt == 0) continue;
            return err;
        };
        defer prepared.deinit(alloc);
        var compute = QueryComputeContext{ .prepared = &prepared, .text = text, .embedding_template = embedding_template, .retry_not_before_ns = options.retry_not_before_ns };
        const result = computePreparedQueryVector(&compute, alloc, options, deadline);
        return result catch |err| {
            // Never publish a different revision under the rejected cache key.
            if (err == error.InferenceCapabilitiesStale and attempt == 0) continue;
            return err;
        };
    }
}

fn computePreparedQueryVector(compute: *QueryComputeContext, alloc: std.mem.Allocator, options: QueryEmbeddingOptions, deadline: u64) ![]f32 {
    const storage = options.storage orelse return QueryComputeContext.run(compute, alloc);
    const cache_deadline = storage.cache.deadlineFromNative(deadline);
    if (compute.embedding_template == null) {
        const key = compute.prepared.cacheKey(options.domain, options.scope, compute.text) catch |err| switch (err) {
            error.QueryEmbeddingNotCacheable => return storage.cache.computeUncached(alloc, cache_deadline, compute, QueryComputeContext.run),
            else => return err,
        };
        return storage.cache.getOrComputeWithRetryHint(storage.budget, alloc, key, cache_deadline, options.cancellation orelse .none, compute, QueryComputeContext.run, options.retry_not_before_ns, if (compute.prepared.cacheHasFixedExpiration()) .fixed else .sliding);
    }
    return storage.cache.computeUncached(alloc, cache_deadline, compute, QueryComputeContext.run);
}

fn effectiveQueryEmbeddingDeadlineNs(request_deadline_ns: ?u64, now_ns: u64) u64 {
    return request_deadline_ns orelse now_ns +| default_query_embedding_timeout_ns;
}

pub fn planSemanticQuery(
    planning: QueryPlanningContext,
    alloc: std.mem.Allocator,
    table_name: []const u8,
    index_name: []const u8,
    semantic_search: []const u8,
    embedding_template: ?[]const u8,
    limit: u32,
) !db_mod.types.DenseKnnQuery {
    if (semantic_search.len > max_query_embedding_input_bytes) return error.QueryEmbeddingInputTooLarge;
    if (embedding_template) |value| {
        if (value.len > max_query_embedding_template_bytes) return error.QueryEmbeddingInputTooLarge;
    }
    const now_ns = platform_time.monotonicNs();
    const embedding_deadline_ns = effectiveQueryEmbeddingDeadlineNs(planning.query_embedding_deadline_ns, now_ns);
    if (embedding_deadline_ns <= now_ns) return error.Timeout;

    var snapshot: ?metadata_api.AdminSnapshot = if (planning.indexes_json == null) try planning.adminSnapshot() else null;
    defer if (snapshot) |*value| planning.freeAdminSnapshot(value);
    const indexes_json = planning.indexes_json orelse (tables_api.findTableByName(&snapshot.?, table_name) orelse return error.TableNotFound).indexes_json;
    var runtime = try managed_embedder.ManagedEmbedder.initFromIndexesJsonWithOptions(alloc, indexes_json, .{
        .antfly_provider = planning.antfly_provider,
        .io = planning.io,
        .deadline_ns = embedding_deadline_ns,
        .cancellation = planning.query_cancellation,
        .remote_content = planning.remote_content,
        .inference_api_url = planning.inference_api_url,
        .inference_api_key = planning.inference_api_key,
        .secret_store = planning.secret_store,
        .provider_runtime = planning.provider_runtime,
        .remote_capability_cache = if (planning.provider_runtime) |runtime| &runtime.remote_capability_cache else null,
        .request_id = planning.request_id,
    });
    defer runtime.deinit();

    return .{
        .vector = try computeManagedQueryVector(&runtime, alloc, index_name, semantic_search, embedding_template, .{
            .storage = if (planning.query_embedding_cache) |cache|
                if (planning.query_embedding_budget) |budget| .{ .cache = cache, .budget = budget } else null
            else
                null,
            .domain = planning.query_embedding_security_domain,
            .scope = planning.query_embedding_security_scope,
            .deadline_ns = embedding_deadline_ns,
            .cancellation = planning.query_cancellation,
            .request_id = planning.request_id,
            .retry_not_before_ns = planning.retry_not_before_ns,
        }),
        .k = limit,
    };
}

pub fn resolveDenseQuery(
    planning: QueryPlanningContext,
    alloc: std.mem.Allocator,
    table_name: []const u8,
    index_name: []const u8,
    semantic_search: []const u8,
    embedding_template: ?[]const u8,
    limit: u32,
) !db_mod.types.DenseKnnQuery {
    return try planSemanticQuery(
        planning,
        alloc,
        table_name,
        index_name,
        semantic_search,
        embedding_template,
        limit,
    );
}

test "semantic query planning reuses equivalent embeddings across tables and isolates principals" {
    const alloc = std.testing.allocator;
    try std.testing.expectEqual(@as(u64, 123 + default_query_embedding_timeout_ns), effectiveQueryEmbeddingDeadlineNs(null, 123));
    try std.testing.expectEqual(@as(u64, 456), effectiveQueryEmbeddingDeadlineNs(456, 123));
    const FakeCatalog = struct {
        const indexes_json =
            \\{"semantic_idx":{"type":"embeddings","field":"body","dimension":3,"embedder":{"provider":"antfly","model":"antflydb/test-embedder"}}}
        ;

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{
                    .{ .table_id = 1, .name = "docs_a", .schema_json = "{}", .indexes_json = indexes_json, .placement_role = "data" },
                    .{ .table_id = 2, .name = "docs_b", .schema_json = "{}", .indexes_json = indexes_json, .placement_role = "data" },
                })[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };
    const FakeProvider = struct {
        calls: usize = 0,

        fn provider(self: *@This()) managed_embedder.AntflyProvider {
            return .{
                .ptr = self,
                .embed_dense_texts = embedDense,
                .embed_sparse_texts = embedSparse,
            };
        }

        fn embedDense(ptr: *anyopaque, inner_alloc: std.mem.Allocator, _: []const u8, texts: []const []const u8) ![][]f32 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            const vectors = try inner_alloc.alloc([]f32, texts.len);
            errdefer inner_alloc.free(vectors);
            var initialized: usize = 0;
            errdefer for (vectors[0..initialized]) |vector| inner_alloc.free(vector);
            for (vectors) |*vector| {
                vector.* = try inner_alloc.dupe(f32, &.{ 1, 2, 3 });
                initialized += 1;
            }
            return vectors;
        }

        fn embedSparse(_: *anyopaque, inner_alloc: std.mem.Allocator, _: []const u8, _: []const []const u8) ![]db_embedder.SparseEmbedding {
            return try inner_alloc.alloc(db_embedder.SparseEmbedding, 0);
        }
    };

    var budget = cache_budget.CacheBudget.init(1024 * 1024);
    var cache = query_embedding_cache.QueryEmbeddingCache.init(alloc, std.Io.Threaded.global_single_threaded.io(), .{});
    defer cache.deinit(&budget);
    var provider = FakeProvider{};
    const base: QueryPlanningContext = .{
        .ptr = undefined,
        .admin_snapshot = FakeCatalog.adminSnapshot,
        .free_admin_snapshot = FakeCatalog.freeAdminSnapshot,
        .antfly_provider = provider.provider(),
        .query_embedding_cache = &cache,
        .query_embedding_budget = &budget,
        .query_embedding_security_domain = .principal,
        .query_embedding_security_scope = "alice",
    };

    const first = try planSemanticQuery(base, alloc, "docs_a", "semantic_idx", "same query", null, 5);
    defer alloc.free(first.vector);
    const equivalent = try planSemanticQuery(base, alloc, "docs_b", "semantic_idx", "same query", null, 5);
    defer alloc.free(equivalent.vector);
    try std.testing.expectEqual(@as(usize, 1), provider.calls);
    try std.testing.expectEqualSlices(f32, first.vector, equivalent.vector);

    var other_principal = base;
    other_principal.query_embedding_security_scope = "bob";
    const isolated = try planSemanticQuery(other_principal, alloc, "docs_b", "semantic_idx", "same query", null, 5);
    defer alloc.free(isolated.vector);
    try std.testing.expectEqual(@as(usize, 2), provider.calls);

    const templated = try planSemanticQuery(base, alloc, "docs_a", "semantic_idx", "same query", "{{this}}", 5);
    defer alloc.free(templated.vector);
    const templated_again = try planSemanticQuery(base, alloc, "docs_a", "semantic_idx", "same query", "{{this}}", 5);
    defer alloc.free(templated_again.vector);
    try std.testing.expectEqual(@as(usize, 4), provider.calls);
    try std.testing.expectEqual(@as(u64, 2), cache.stats(&budget).uncached_computations);

    const oversized = try alloc.alloc(u8, max_query_embedding_input_bytes + 1);
    defer alloc.free(oversized);
    @memset(oversized, 'x');
    try std.testing.expectError(
        error.QueryEmbeddingInputTooLarge,
        planSemanticQuery(base, alloc, "docs_a", "semantic_idx", oversized, null, 5),
    );
    try std.testing.expectEqual(@as(usize, 4), provider.calls);

    const oversized_template = try alloc.alloc(u8, max_query_embedding_template_bytes + 1);
    defer alloc.free(oversized_template);
    @memset(oversized_template, 'x');
    try std.testing.expectError(
        error.QueryEmbeddingInputTooLarge,
        planSemanticQuery(base, alloc, "docs_a", "semantic_idx", "same query", oversized_template, 5),
    );
    try std.testing.expectEqual(@as(usize, 4), provider.calls);

    var expired = base;
    expired.query_embedding_deadline_ns = 1;
    try std.testing.expectError(
        error.Timeout,
        planSemanticQuery(expired, alloc, "docs_a", "semantic_idx", "same query", null, 5),
    );
    try std.testing.expectEqual(@as(usize, 4), provider.calls);
}

const SemanticStatusResolver = struct {
    planning: ?QueryPlanningContext,

    fn iface(self: *SemanticStatusResolver) query_contract.SemanticResolver {
        return .{
            .ptr = self,
            .vtable = &.{
                .resolve_dense_query = resolveDenseQueryImpl,
            },
        };
    }

    fn resolveDenseQueryImpl(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        index_name: []const u8,
        semantic_search: []const u8,
        embedding_template: ?[]const u8,
        limit: u32,
    ) !db_mod.types.DenseKnnQuery {
        const self: *SemanticStatusResolver = @ptrCast(@alignCast(ptr));
        return try planSemanticQuery(self.planning orelse return error.UnsupportedQueryRequest, alloc, table_name, index_name, semantic_search, embedding_template, limit);
    }
};
