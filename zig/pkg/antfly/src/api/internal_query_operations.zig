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
const CancellationToken = @import("../common/cancellation.zig").CancellationToken;
const metadata_api = @import("../metadata/api.zig");
const metadata_table_manager = @import("../metadata/table_manager.zig");
const metadata_transition_state = @import("../metadata/transition_state.zig");
const managed_embedder = @import("../inference/managed_embedder.zig");
const query_embedding_cache = @import("../inference/query_embedding_cache.zig");
const cache_budget = @import("../common/cache_budget.zig");
const common_secrets = @import("../common/secrets.zig");
const platform_time = @import("antfly_platform").time;
const scraping = @import("antfly_scraping");
const db_mod = @import("../storage/db/mod.zig");

pub const max_query_embedding_input_bytes: usize = 1024 * 1024;
pub const max_query_embedding_template_bytes: usize = 64 * 1024;
pub const default_query_embedding_timeout_ns: u64 = 30 * std.time.ns_per_s;
const db_embedder = @import("../storage/db/enrichment/embedder.zig");
const algebraic_ir = @import("../storage/db/algebraic/ir.zig");
const distributed_graph = @import("distributed_graph.zig");
const query_api = @import("query.zig");
const query_contract = @import("query_contract.zig");
const tables_api = @import("tables.zig");
const raft_reconciler = @import("../raft/reconciler.zig");

pub fn normalizeQueryEmbeddingOperationalError(err: anyerror) ?anyerror {
    return switch (err) {
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
        error.InvalidEmbeddingDimensions,
        error.SecretNotFound,
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
    graph_execution_limits: @import("../graph/work_budget.zig").Limits = .{},

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
    ptr: *anyopaque,
    admin_snapshot: *const fn (ptr: *anyopaque) anyerror!metadata_api.AdminSnapshot,
    free_admin_snapshot: *const fn (ptr: *anyopaque, snapshot: *metadata_api.AdminSnapshot) void,
    antfly_provider: ?managed_embedder.AntflyProvider = null,
    io: ?std.Io = null,
    remote_content: ?*const scraping.RemoteContentConfig = null,
    inference_api_url: ?[]const u8 = null,
    inference_api_key: ?[]const u8 = null,
    secret_store: ?*common_secrets.FileStore = null,
    query_embedding_cache: ?*query_embedding_cache.QueryEmbeddingCache = null,
    query_embedding_budget: ?*cache_budget.CacheBudget = null,
    query_embedding_security_domain: managed_embedder.QueryCacheSecurityDomain = .internal,
    query_embedding_security_scope: []const u8 = "internal",
    query_embedding_deadline_ns: ?u64 = null,
    query_cancellation: ?CancellationToken = null,

    fn adminSnapshot(self: QueryPlanningContext) !metadata_api.AdminSnapshot {
        return try self.admin_snapshot(self.ptr);
    }

    fn freeAdminSnapshot(self: QueryPlanningContext, snapshot: *metadata_api.AdminSnapshot) void {
        self.free_admin_snapshot(self.ptr, snapshot);
    }
};

const DenseQueryComputeContext = struct {
    runtime: *const managed_embedder.ManagedEmbedder,
    index_name: []const u8,
    text: []const u8,

    fn run(ptr: *anyopaque, alloc: std.mem.Allocator) ![]f32 {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return embedInteractive(self.runtime, alloc, self.index_name, self.text);
    }
};

const TemplateQueryComputeContext = struct {
    runtime: *const managed_embedder.ManagedEmbedder,
    index_name: []const u8,
    text: []const u8,
    embedding_template: []const u8,

    fn run(ptr: *anyopaque, alloc: std.mem.Allocator) ![]f32 {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return embedTemplateInteractive(self.runtime, alloc, self.index_name, self.text, self.embedding_template);
    }
};

fn embedInteractive(
    runtime: *const managed_embedder.ManagedEmbedder,
    alloc: std.mem.Allocator,
    index_name: []const u8,
    text: []const u8,
) ![]f32 {
    _ = db_mod.enrichment_types.interactive_embed_inflight.fetchAdd(1, .monotonic);
    defer _ = db_mod.enrichment_types.interactive_embed_inflight.fetchSub(1, .monotonic);
    return try runtime.embedQuery(alloc, index_name, text);
}

fn embedTemplateInteractive(
    runtime: *const managed_embedder.ManagedEmbedder,
    alloc: std.mem.Allocator,
    index_name: []const u8,
    text: []const u8,
    embedding_template: []const u8,
) ![]f32 {
    _ = db_mod.enrichment_types.interactive_embed_inflight.fetchAdd(1, .monotonic);
    defer _ = db_mod.enrichment_types.interactive_embed_inflight.fetchSub(1, .monotonic);
    return try runtime.embedQueryWithTemplate(alloc, index_name, text, embedding_template);
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

    var snapshot = try planning.adminSnapshot();
    defer planning.freeAdminSnapshot(&snapshot);

    const table = tables_api.findTableByName(&snapshot, table_name) orelse return error.TableNotFound;
    var runtime = try managed_embedder.ManagedEmbedder.initFromIndexesJsonWithOptions(alloc, table.indexes_json, .{
        .antfly_provider = planning.antfly_provider,
        .io = planning.io,
        .deadline_ns = embedding_deadline_ns,
        .cancellation = planning.query_cancellation,
        .remote_content = planning.remote_content,
        .inference_api_url = planning.inference_api_url,
        .inference_api_key = planning.inference_api_key,
        .secret_store = planning.secret_store,
    });
    defer runtime.deinit();

    return .{
        .vector = if (embedding_template) |value| blk: {
            var compute_context = TemplateQueryComputeContext{
                .runtime = &runtime,
                .index_name = index_name,
                .text = semantic_search,
                .embedding_template = value,
            };
            const cache = planning.query_embedding_cache orelse
                break :blk try TemplateQueryComputeContext.run(&compute_context, alloc);
            break :blk try cache.computeUncached(alloc, embedding_deadline_ns, &compute_context, TemplateQueryComputeContext.run);
        } else blk: {
            var compute_context = DenseQueryComputeContext{
                .runtime = &runtime,
                .index_name = index_name,
                .text = semantic_search,
            };
            const cache = planning.query_embedding_cache orelse
                break :blk try DenseQueryComputeContext.run(&compute_context, alloc);
            const budget = planning.query_embedding_budget orelse
                break :blk try cache.computeUncached(alloc, embedding_deadline_ns, &compute_context, DenseQueryComputeContext.run);
            const key = runtime.queryCacheKey(index_name, planning.query_embedding_security_domain, planning.query_embedding_security_scope, semantic_search) catch |err| switch (err) {
                error.QueryEmbeddingNotCacheable => break :blk try cache.computeUncached(alloc, embedding_deadline_ns, &compute_context, DenseQueryComputeContext.run),
                else => return err,
            };
            break :blk try cache.getOrCompute(budget, alloc, key, embedding_deadline_ns, &compute_context, DenseQueryComputeContext.run);
        },
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
