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
const metadata_api = @import("../metadata/api.zig");
const metadata_table_manager = @import("../metadata/table_manager.zig");
const metadata_transition_state = @import("../metadata/transition_state.zig");
const managed_embedder = @import("../inference/managed_embedder.zig");
const query_embedding_cache = @import("../inference/query_embedding_cache.zig");
const cache_budget = @import("../common/cache_budget.zig");
const common_secrets = @import("../common/secrets.zig");
const platform_time = @import("../platform/time.zig");
const scraping = @import("antfly_scraping");
const db_mod = @import("../storage/db/mod.zig");

pub const max_query_embedding_input_bytes: usize = 1024 * 1024;
pub const max_query_embedding_template_bytes: usize = 64 * 1024;
pub const default_query_embedding_timeout_ns: u64 = 30 * std.time.ns_per_s;
const db_embedder = @import("../storage/db/enrichment/embedder.zig");
const algebraic_ir = @import("../storage/db/algebraic/ir.zig");
const metadata_openapi = @import("antfly_metadata_openapi");
const distributed_graph = @import("distributed_graph.zig");
const http_route_helpers = @import("http_route_helpers.zig");
const query_api = @import("query.zig");
const query_contract = @import("query_contract.zig");
const table_reads = @import("table_reads.zig");
const tables_api = @import("tables.zig");
const http_common = @import("../raft/transport/http_common.zig");
const raft_reconciler = @import("../raft/reconciler.zig");
const routes = @import("http_routes.zig");

const QueryPreflightRequestWire = struct {
    query_request: std.json.Value,
    max_work: u32 = 0,
};

fn parseQueryPreflightRequest(
    alloc: std.mem.Allocator,
    body: []const u8,
) !struct { query_request_body: []u8, max_work: u32 } {
    var parsed = std.json.parseFromSlice(QueryPreflightRequestWire, alloc, body, .{ .allocate = .alloc_always }) catch {
        return .{
            .query_request_body = try alloc.dupe(u8, body),
            .max_work = 0,
        };
    };
    defer parsed.deinit();
    return .{
        .query_request_body = try std.json.Stringify.valueAlloc(alloc, parsed.value.query_request, .{}),
        .max_work = parsed.value.max_work,
    };
}

fn queryEmbeddingOperationalResponse(alloc: std.mem.Allocator, err: anyerror) !?http_common.HttpResponse {
    const normalized = normalizeQueryEmbeddingOperationalError(err) orelse return null;
    return switch (normalized) {
        error.QueryEmbeddingInputTooLarge => try http_route_helpers.textResponse(alloc, 413, "query embedding input too large"),
        error.QueryEmbeddingOverloaded => try http_route_helpers.textResponse(alloc, 429, "query embedding overloaded"),
        error.EmbedRateLimited => try http_route_helpers.textResponse(alloc, 429, "query embedding rate limited"),
        error.EmbedTransientFailure => try http_route_helpers.textResponse(alloc, 503, "query embedding temporarily unavailable"),
        error.EmbedUpstreamFailure => try http_route_helpers.textResponse(alloc, 502, "query embedding provider failed"),
        error.Timeout => try http_route_helpers.textResponse(alloc, 504, "query embedding timed out"),
        else => null,
    };
}

fn normalizeQueryEmbeddingOperationalError(err: anyerror) ?anyerror {
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
    alloc: std.mem.Allocator,
    reads: ?table_reads.TableReadSource,
    catalog: CatalogSource,
    query_router: QueryRouter,
    query_planning: ?QueryPlanningContext = null,

    fn queryPlanning(self: Context) ?QueryPlanningContext {
        if (self.query_planning) |planning| return planning;
        return .{
            .ptr = self.catalog.ptr,
            .admin_snapshot = self.catalog.admin_snapshot orelse return null,
            .free_admin_snapshot = self.catalog.free_admin_snapshot orelse return null,
        };
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

const DocumentArtifactManifestResponse = struct {
    const ChildRange = struct {
        range_id: []const u8,
        range_kind: []const u8,
        artifact_name: []const u8,
        split_boundary: []const u8,
        placement: []const u8,
        owner_group_id: ?u64,
        placement_generation: ?u64,
        route_status: ?[]const u8,
        split_eligible: ?bool,
        start_key: []const u8,
        end_key_exclusive: []const u8,
        last_key: []const u8,
        child_count: usize,
        text_bytes: ?usize,
    };

    document_id: []const u8,
    artifact_name: []const u8,
    artifact_id: []const u8,
    manifest_version: u64,
    generation: u64,
    source_url: []const u8,
    source_fingerprint: []const u8,
    content_type: []const u8,
    route_type: []const u8,
    unsupported_reason: ?[]const u8,
    unit_count: usize,
    chunk_count: usize,
    child_ranges: []const ChildRange,
    child_range_count: usize,
    merge_status: []const u8,
    merge_from_generation: u64,
    merge_to_generation: u64,
    merge_operation_granularity: []const u8,
    merge_operation_count: usize,
    last_error_code: ?[]const u8,
    last_error_message: ?[]const u8,
    manifest_json: []const u8,
    state_json: ?[]const u8,
};

const DocumentArtifactManifestsResponse = struct {
    document_id: []const u8,
    artifacts: []const DocumentArtifactManifestResponse,
};

fn childRangeResponsesAlloc(alloc: std.mem.Allocator, child_ranges: []const db_mod.types.DocumentArtifactChildRange) ![]DocumentArtifactManifestResponse.ChildRange {
    const out = try alloc.alloc(DocumentArtifactManifestResponse.ChildRange, child_ranges.len);
    for (child_ranges, out) |child_range, *item| {
        item.* = .{
            .range_id = child_range.range_id,
            .range_kind = child_range.range_kind,
            .artifact_name = child_range.artifact_name,
            .split_boundary = child_range.split_boundary,
            .placement = child_range.placement,
            .owner_group_id = child_range.owner_group_id,
            .placement_generation = child_range.placement_generation,
            .route_status = child_range.route_status,
            .split_eligible = child_range.split_eligible,
            .start_key = child_range.start_key,
            .end_key_exclusive = child_range.end_key_exclusive,
            .last_key = child_range.last_key,
            .child_count = child_range.child_count,
            .text_bytes = child_range.text_bytes,
        };
    }
    return out;
}

pub fn handle(ctx: Context, req: http_common.HttpRequest, path: []const u8, query: []const u8) !?http_common.HttpResponse {
    const alloc = ctx.alloc;
    const source = ctx.reads;
    if (req.method == .GET) {
        if (routes.Routes.matchGroupLookup(path)) |lookup| {
            const reads = source orelse return try http_route_helpers.textResponse(alloc, 404, "not found");
            const decoded_key = try http_route_helpers.decodePercentEncodedPathComponentAlloc(alloc, lookup.key);
            defer alloc.free(decoded_key);
            var lookup_opts = try http_route_helpers.parseLookupOptions(alloc, query);
            defer lookup_opts.deinit(alloc);

            var result = (reads.lookupGroupLocal(
                alloc,
                lookup.group_id,
                lookup.table_name,
                decoded_key,
                lookup_opts.opts,
                .read_index,
            ) catch |err| switch (err) {
                error.TopologyChanged => return try http_route_helpers.textResponse(alloc, 409, "topology changed"),
                error.DocIdentityNamespaceMismatch => return try http_route_helpers.textResponse(alloc, 409, "doc identity namespace mismatch"),
                else => return err,
            }) orelse return try http_route_helpers.textResponse(alloc, 404, "not found");
            defer result.deinit(alloc);
            var version_buf: [20]u8 = undefined;
            const version = try std.fmt.bufPrint(&version_buf, "{d}", .{result.version});
            return try http_route_helpers.jsonWithHeadersResponse(alloc, 200, result.json, &.{
                .{
                    .name = "X-Antfly-Version",
                    .value = version,
                },
            });
        }
    }

    if (req.method == .GET) {
        if (routes.Routes.matchGroupDocumentArtifacts(path)) |artifact_route| {
            const reads = source orelse return try http_route_helpers.textResponse(alloc, 404, "not found");
            const decoded_key = try http_route_helpers.decodePercentEncodedPathComponentAlloc(alloc, artifact_route.key);
            defer alloc.free(decoded_key);

            var list = (reads.documentArtifactManifestsGroupLocal(
                alloc,
                artifact_route.group_id,
                artifact_route.table_name,
                decoded_key,
                .read_index,
            ) catch |err| switch (err) {
                error.TopologyChanged => return try http_route_helpers.textResponse(alloc, 409, "topology changed"),
                error.DocIdentityNamespaceMismatch => return try http_route_helpers.textResponse(alloc, 409, "doc identity namespace mismatch"),
                error.UnknownGroup, error.TableNotFound, error.NotFound => return try http_route_helpers.textResponse(alloc, 404, "not found"),
                else => return err,
            }) orelse return try http_route_helpers.textResponse(alloc, 404, "not found");
            defer list.deinit(alloc);

            const artifacts = try alloc.alloc(DocumentArtifactManifestResponse, list.artifacts.len);
            defer alloc.free(artifacts);
            var child_range_sets = std.ArrayListUnmanaged([]DocumentArtifactManifestResponse.ChildRange).empty;
            defer {
                for (child_range_sets.items) |child_ranges| alloc.free(child_ranges);
                child_range_sets.deinit(alloc);
            }
            for (list.artifacts, artifacts) |manifest, *out| {
                const child_ranges = try childRangeResponsesAlloc(alloc, manifest.child_ranges);
                errdefer alloc.free(child_ranges);
                try child_range_sets.append(alloc, child_ranges);
                out.* = .{
                    .document_id = manifest.document_id,
                    .artifact_name = manifest.artifact_name,
                    .artifact_id = manifest.artifact_id,
                    .manifest_version = manifest.manifest_version,
                    .generation = manifest.generation,
                    .source_url = manifest.source_url,
                    .source_fingerprint = manifest.source_fingerprint,
                    .content_type = manifest.content_type,
                    .route_type = manifest.route_type,
                    .unsupported_reason = manifest.unsupported_reason,
                    .unit_count = manifest.unit_count,
                    .chunk_count = manifest.chunk_count,
                    .child_ranges = child_ranges,
                    .child_range_count = manifest.child_range_count,
                    .merge_status = manifest.merge_status,
                    .merge_from_generation = manifest.merge_from_generation,
                    .merge_to_generation = manifest.merge_to_generation,
                    .merge_operation_granularity = manifest.merge_operation_granularity,
                    .merge_operation_count = manifest.merge_operation_count,
                    .last_error_code = manifest.last_error_code,
                    .last_error_message = manifest.last_error_message,
                    .manifest_json = manifest.manifest_json,
                    .state_json = manifest.state_json,
                };
            }

            return try http_route_helpers.jsonResponse(alloc, DocumentArtifactManifestsResponse{
                .document_id = list.document_id,
                .artifacts = artifacts,
            });
        }

        if (routes.Routes.matchGroupDocumentArtifact(path)) |artifact_route| {
            const reads = source orelse return try http_route_helpers.textResponse(alloc, 404, "not found");
            const decoded_key = try http_route_helpers.decodePercentEncodedPathComponentAlloc(alloc, artifact_route.key);
            defer alloc.free(decoded_key);
            const decoded_artifact_name = try http_route_helpers.decodePercentEncodedPathComponentAlloc(alloc, artifact_route.artifact_name);
            defer alloc.free(decoded_artifact_name);

            var manifest = (reads.documentArtifactManifestGroupLocal(
                alloc,
                artifact_route.group_id,
                artifact_route.table_name,
                decoded_key,
                decoded_artifact_name,
                .read_index,
            ) catch |err| switch (err) {
                error.TopologyChanged => return try http_route_helpers.textResponse(alloc, 409, "topology changed"),
                error.DocIdentityNamespaceMismatch => return try http_route_helpers.textResponse(alloc, 409, "doc identity namespace mismatch"),
                error.UnknownGroup, error.TableNotFound, error.NotFound => return try http_route_helpers.textResponse(alloc, 404, "not found"),
                else => return err,
            }) orelse return try http_route_helpers.textResponse(alloc, 404, "not found");
            defer manifest.deinit(alloc);

            const child_ranges = try childRangeResponsesAlloc(alloc, manifest.child_ranges);
            defer alloc.free(child_ranges);
            return try http_route_helpers.jsonResponse(alloc, DocumentArtifactManifestResponse{
                .document_id = manifest.document_id,
                .artifact_name = manifest.artifact_name,
                .artifact_id = manifest.artifact_id,
                .manifest_version = manifest.manifest_version,
                .generation = manifest.generation,
                .source_url = manifest.source_url,
                .source_fingerprint = manifest.source_fingerprint,
                .content_type = manifest.content_type,
                .route_type = manifest.route_type,
                .unsupported_reason = manifest.unsupported_reason,
                .unit_count = manifest.unit_count,
                .chunk_count = manifest.chunk_count,
                .child_ranges = child_ranges,
                .child_range_count = manifest.child_range_count,
                .merge_status = manifest.merge_status,
                .merge_from_generation = manifest.merge_from_generation,
                .merge_to_generation = manifest.merge_to_generation,
                .merge_operation_granularity = manifest.merge_operation_granularity,
                .merge_operation_count = manifest.merge_operation_count,
                .last_error_code = manifest.last_error_code,
                .last_error_message = manifest.last_error_message,
                .manifest_json = manifest.manifest_json,
                .state_json = manifest.state_json,
            });
        }
    }

    if (req.method == .POST) {
        if (routes.Routes.matchGroupScan(path)) |scan| {
            const reads = source orelse return try http_route_helpers.textResponse(alloc, 404, "not found");
            var scan_req = try http_route_helpers.parseScanKeysRequest(alloc, req.body);
            defer scan_req.deinit(alloc);

            var result = (reads.scanGroupLocal(
                alloc,
                scan.group_id,
                scan.table_name,
                scan_req.from,
                scan_req.to,
                scan_req.opts,
                .read_index,
            ) catch |err| switch (err) {
                error.TopologyChanged => return try http_route_helpers.textResponse(alloc, 409, "topology changed"),
                error.DocIdentityNamespaceMismatch => return try http_route_helpers.textResponse(alloc, 409, "doc identity namespace mismatch"),
                else => return err,
            }) orelse return try http_route_helpers.textResponse(alloc, 404, "not found");
            defer result.deinit(alloc);
            return try http_route_helpers.ndjsonResponse(alloc, 200, result.ndjson);
        }
        if (routes.Routes.matchGroupGraphExpand(path)) |graph_expand_route| {
            const reads = source orelse return try http_route_helpers.textResponse(alloc, 404, "not found");
            var expand_req = distributed_graph.parseGraphExpandRequest(alloc, req.body) catch |err| switch (err) {
                error.OutOfMemory => return err,
                else => return try http_route_helpers.textResponse(alloc, 400, "invalid graph expand request"),
            };
            defer expand_req.deinit(alloc);

            var result = (reads.graphExpandGroupLocal(
                alloc,
                graph_expand_route.group_id,
                graph_expand_route.table_name,
                expand_req,
                .read_index,
            ) catch |err| switch (err) {
                error.InvalidQueryRequest, error.UnsupportedQueryRequest, error.InvalidArgument, error.IndexNotFound => return try http_route_helpers.textResponse(alloc, 400, @errorName(err)),
                error.TopologyChanged => return try http_route_helpers.textResponse(alloc, 409, "topology changed"),
                error.DocIdentityNamespaceMismatch => return try http_route_helpers.textResponse(alloc, 409, "doc identity namespace mismatch"),
                error.UnknownGroup, error.TableNotFound => return try http_route_helpers.textResponse(alloc, 404, "not found"),
                else => return err,
            }) orelse return try http_route_helpers.textResponse(alloc, 404, "not found");
            defer result.deinit(alloc);
            return try http_route_helpers.jsonResponse(alloc, result);
        }
        if (routes.Routes.matchGroupQuery(path)) |query_route| {
            const reads = source orelse return try http_route_helpers.textResponse(alloc, 404, "not found");
            var semantic_resolver = SemanticStatusResolver{ .planning = ctx.queryPlanning() };
            var query_req = query_api.parseQueryRequest(alloc, semantic_resolver.iface(), query_route.table_name, req.body) catch |err| switch (err) {
                error.InvalidQueryRequest, error.UnsupportedQueryRequest => return try http_route_helpers.textResponse(alloc, 400, @errorName(err)),
                else => {
                    if (try queryEmbeddingOperationalResponse(alloc, err)) |response| return response;
                    return err;
                },
            };
            defer query_req.deinit(alloc);
            ctx.query_router.route(query_route.table_name, &query_req.req) catch |err| switch (err) {
                error.TableNotFound => return try http_route_helpers.textResponse(alloc, 404, @errorName(err)),
                error.InvalidSchemaUpdateRequest, error.InvalidTableIndexMetadata => return try http_route_helpers.textResponse(alloc, 500, "invalid table metadata"),
                else => return err,
            };

            var result = (reads.queryGroupLocal(
                alloc,
                query_route.group_id,
                query_route.table_name,
                query_req.req,
                .read_index,
            ) catch |err| switch (err) {
                error.InvalidQueryRequest, error.UnsupportedQueryRequest, error.InvalidArgument, error.IndexNotFound => return try http_route_helpers.textResponse(alloc, 400, @errorName(err)),
                error.TopologyChanged => return try http_route_helpers.textResponse(alloc, 409, "topology changed"),
                error.DocIdentityNamespaceMismatch => return try http_route_helpers.textResponse(alloc, 409, "doc identity namespace mismatch"),
                error.UnknownGroup, error.TableNotFound => return try http_route_helpers.textResponse(alloc, 404, @errorName(err)),
                else => return err,
            }) orelse return try http_route_helpers.textResponse(alloc, 404, "not found");
            defer result.deinit(alloc);

            var arena_impl = std.heap.ArenaAllocator.init(alloc);
            defer arena_impl.deinit();
            const response = try std.json.parseFromSliceLeaky(metadata_openapi.QueryResponses, arena_impl.allocator(), result.json, .{
                .allocate = .alloc_always,
            });
            return try http_route_helpers.jsonResponse(alloc, response);
        }
        if (routes.Routes.matchGroupVectorWorker(path)) |vector_route| {
            const reads = source orelse return try http_route_helpers.textResponse(alloc, 404, "not found");
            var envelope = query_contract.parseAlgebraicVectorWorkerRequestEnvelopeAlloc(alloc, req.body) catch |err| switch (err) {
                error.OutOfMemory => return err,
                else => return try http_route_helpers.textResponse(alloc, 400, "invalid vector worker request"),
            };
            defer envelope.deinit(alloc);
            var vector_req = table_reads.searchRequestFromVectorWorkerEnvelope(&envelope);
            defer if (vector_req.primary_text_index_name) |index_name| alloc.free(index_name);
            ctx.query_router.route(vector_route.table_name, &vector_req) catch |err| switch (err) {
                error.TableNotFound => return try http_route_helpers.textResponse(alloc, 404, @errorName(err)),
                error.InvalidSchemaUpdateRequest, error.InvalidTableIndexMetadata => return try http_route_helpers.textResponse(alloc, 500, "invalid table metadata"),
                else => return err,
            };

            var result = (reads.queryGroupLocal(
                alloc,
                vector_route.group_id,
                vector_route.table_name,
                vector_req,
                .read_index,
            ) catch |err| switch (err) {
                error.InvalidQueryRequest, error.UnsupportedQueryRequest, error.InvalidArgument, error.IndexNotFound => return try http_route_helpers.textResponse(alloc, 400, @errorName(err)),
                error.TopologyChanged => return try http_route_helpers.textResponse(alloc, 409, "topology changed"),
                error.DocIdentityNamespaceMismatch => return try http_route_helpers.textResponse(alloc, 409, "doc identity namespace mismatch"),
                error.UnknownGroup, error.TableNotFound => return try http_route_helpers.textResponse(alloc, 404, @errorName(err)),
                else => return err,
            }) orelse return try http_route_helpers.textResponse(alloc, 404, "not found");
            defer result.deinit(alloc);

            var arena_impl = std.heap.ArenaAllocator.init(alloc);
            defer arena_impl.deinit();
            const response = try std.json.parseFromSliceLeaky(metadata_openapi.QueryResponses, arena_impl.allocator(), result.json, .{
                .allocate = .alloc_always,
            });
            return try http_route_helpers.jsonResponse(alloc, response);
        }
        if (routes.Routes.matchGroupQueryPreflight(path)) |query_route| {
            const reads = source orelse return try http_route_helpers.textResponse(alloc, 404, "not found");
            const preflight_req = try parseQueryPreflightRequest(alloc, req.body);
            defer alloc.free(preflight_req.query_request_body);
            var semantic_resolver = SemanticStatusResolver{ .planning = ctx.queryPlanning() };
            var query_req = query_api.parseQueryRequest(alloc, semantic_resolver.iface(), query_route.table_name, preflight_req.query_request_body) catch |err| switch (err) {
                error.InvalidQueryRequest, error.UnsupportedQueryRequest => return try http_route_helpers.textResponse(alloc, 400, @errorName(err)),
                else => {
                    if (try queryEmbeddingOperationalResponse(alloc, err)) |response| return response;
                    return err;
                },
            };
            defer query_req.deinit(alloc);
            ctx.query_router.route(query_route.table_name, &query_req.req) catch |err| switch (err) {
                error.TableNotFound => return try http_route_helpers.textResponse(alloc, 404, @errorName(err)),
                error.InvalidSchemaUpdateRequest, error.InvalidTableIndexMetadata => return try http_route_helpers.textResponse(alloc, 500, "invalid table metadata"),
                else => return err,
            };

            var summary = (reads.preflightQueryGroupLocal(
                alloc,
                query_route.group_id,
                query_route.table_name,
                query_req.req,
                .read_index,
                preflight_req.max_work,
            ) catch |err| switch (err) {
                error.InvalidQueryRequest, error.UnsupportedQueryRequest, error.InvalidArgument, error.IndexNotFound => return try http_route_helpers.textResponse(alloc, 400, @errorName(err)),
                error.UnknownGroup, error.TableNotFound => return try http_route_helpers.textResponse(alloc, 404, @errorName(err)),
                error.TopologyChanged => return try http_route_helpers.textResponse(alloc, 409, @errorName(err)),
                error.DocIdentityNamespaceMismatch => return try http_route_helpers.textResponse(alloc, 409, "doc identity namespace mismatch"),
                else => return err,
            }) orelse return try http_route_helpers.textResponse(alloc, 404, "not found");
            defer summary.deinit(alloc);
            return try http_route_helpers.jsonResponse(alloc, summary);
        }
        if (routes.Routes.matchGroupGraphHydrate(path)) |graph_hydrate_route| {
            const reads = source orelse return try http_route_helpers.textResponse(alloc, 404, "not found");
            var hydrate_req = distributed_graph.parseGraphHydrateRequest(alloc, req.body) catch |err| switch (err) {
                error.OutOfMemory => return err,
                else => return try http_route_helpers.textResponse(alloc, 400, "invalid graph hydrate request"),
            };
            defer hydrate_req.deinit(alloc);

            var result = (reads.graphHydrateGroupLocal(
                alloc,
                graph_hydrate_route.group_id,
                graph_hydrate_route.table_name,
                hydrate_req,
                .read_index,
            ) catch |err| switch (err) {
                error.TopologyChanged => return try http_route_helpers.textResponse(alloc, 409, "topology changed"),
                error.DocIdentityNamespaceMismatch => return try http_route_helpers.textResponse(alloc, 409, "doc identity namespace mismatch"),
                error.UnknownGroup, error.TableNotFound => return try http_route_helpers.textResponse(alloc, 404, "not found"),
                else => return err,
            }) orelse return try http_route_helpers.textResponse(alloc, 404, "not found");
            defer result.deinit(alloc);
            return try http_route_helpers.jsonResponse(alloc, result);
        }
        if (routes.Routes.matchGroupGraphEdges(path)) |graph_edges_route| {
            const reads = source orelse return try http_route_helpers.textResponse(alloc, 404, "not found");
            var edges_req = distributed_graph.parseGraphEdgesRequest(alloc, req.body) catch |err| switch (err) {
                error.OutOfMemory => return err,
                else => return try http_route_helpers.textResponse(alloc, 400, "invalid graph edges request"),
            };
            defer edges_req.deinit(alloc);

            var result = (reads.graphEdgesGroupLocal(
                alloc,
                graph_edges_route.group_id,
                graph_edges_route.table_name,
                edges_req,
                .read_index,
            ) catch |err| switch (err) {
                error.TopologyChanged => return try http_route_helpers.textResponse(alloc, 409, "topology changed"),
                error.DocIdentityNamespaceMismatch => return try http_route_helpers.textResponse(alloc, 409, "doc identity namespace mismatch"),
                error.UnknownGroup, error.TableNotFound => return try http_route_helpers.textResponse(alloc, 404, "not found"),
                error.InvalidQueryRequest, error.IndexNotFound => return try http_route_helpers.textResponse(alloc, 400, "invalid graph edges request"),
                else => return err,
            }) orelse return try http_route_helpers.textResponse(alloc, 404, "not found");
            defer result.deinit(alloc);
            return try http_route_helpers.jsonResponse(alloc, result);
        }
        if (routes.Routes.matchGroupTextStats(path)) |text_stats_route| {
            const reads = source orelse return try http_route_helpers.textResponse(alloc, 404, "not found");
            var text_stats_result = reads.textStatsGroupLocal(alloc, text_stats_route.group_id, text_stats_route.table_name, req.body) catch |err| switch (err) {
                error.InvalidQueryRequest, error.UnsupportedQueryRequest => return try http_route_helpers.textResponse(alloc, 400, "invalid text stats request"),
                error.TableNotFound, error.UnknownGroup => return try http_route_helpers.textResponse(alloc, 404, "not found"),
                error.TopologyChanged => return try http_route_helpers.textResponse(alloc, 409, "topology changed"),
                error.DocIdentityNamespaceMismatch => return try http_route_helpers.textResponse(alloc, 409, "doc identity namespace mismatch"),
                else => return err,
            } orelse return try http_route_helpers.textResponse(alloc, 404, "not found");
            defer text_stats_result.deinit(alloc);
            const body = try alloc.dupe(u8, text_stats_result.json);
            defer alloc.free(body);
            var arena_impl = std.heap.ArenaAllocator.init(alloc);
            defer arena_impl.deinit();
            var response = try table_reads.parseTextStatsHttpResponse(arena_impl.allocator(), req.body, body);
            defer response.deinit(arena_impl.allocator());
            return switch (response) {
                .fields => |value| try http_route_helpers.jsonResponse(alloc, value),
                .background_fields => |value| try http_route_helpers.jsonResponse(alloc, value),
            };
        }
        if (routes.Routes.matchGroupAlgebraicPartials(path)) |partials_route| {
            const reads = source orelse return try http_route_helpers.textResponse(alloc, 404, "not found");
            var partials_result = reads.algebraicPartialsGroupLocal(alloc, partials_route.group_id, partials_route.table_name, req.body) catch |err| switch (err) {
                error.InvalidQueryRequest, error.UnsupportedQueryRequest => return try http_route_helpers.textResponse(alloc, 400, "invalid algebraic partials request"),
                error.TableNotFound, error.UnknownGroup => return try http_route_helpers.textResponse(alloc, 404, "not found"),
                error.TopologyChanged => return try http_route_helpers.textResponse(alloc, 409, "topology changed"),
                error.DocIdentityNamespaceMismatch => return try http_route_helpers.textResponse(alloc, 409, "doc identity namespace mismatch"),
                else => return err,
            } orelse return try http_route_helpers.textResponse(alloc, 404, "not found");
            defer partials_result.deinit(alloc);
            return try http_route_helpers.jsonResponse(alloc, partials_result);
        }
    }

    return null;
}

test "internal group read routes handle text stats errors" {
    const alloc = std.testing.allocator;

    const FakeReads = struct {
        fn source() table_reads.TableReadSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                    .text_stats_group_local = textStatsGroupLocal,
                },
            };
        }

        fn lookup(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: @import("../storage/db/mod.zig").types.LookupOptions,
            _: @import("../raft/mod.zig").ReadConsistency,
        ) !?table_reads.LookupResponse {
            return null;
        }

        fn scan(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: []const u8,
            _: @import("../storage/db/mod.zig").types.ScanOptions,
            _: @import("../raft/mod.zig").ReadConsistency,
        ) !?table_reads.ScanResponse {
            return null;
        }

        fn query(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: @import("../storage/db/mod.zig").types.SearchRequest,
            _: @import("../raft/mod.zig").ReadConsistency,
        ) !?@import("query.zig").QueryResponse {
            return null;
        }

        fn textStatsGroupLocal(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: u64,
            _: []const u8,
            _: []const u8,
        ) !?@import("query.zig").QueryResponse {
            return error.InvalidQueryRequest;
        }
    };

    var resp = (try handle(.{
        .alloc = alloc,
        .reads = FakeReads.source(),
        .catalog = .{
            .ptr = undefined,
        },
        .query_router = .{
            .ptr = undefined,
            .route_query_to_read_schema = struct {
                fn route(_: *anyopaque, _: []const u8, _: *@import("../storage/db/mod.zig").types.SearchRequest) !void {}
            }.route,
        },
    }, .{
        .method = .POST,
        .uri = "/internal/v1/groups/7/tables/docs/text-stats",
        .body = "{}",
    }, "/internal/v1/groups/7/tables/docs/text-stats", "")).?;
    defer resp.deinit(alloc);

    try std.testing.expectEqual(@as(u16, 400), resp.status);
    try std.testing.expectEqualStrings("invalid text stats request", resp.body);
}

test "internal group read routes map doc identity mismatch to conflict" {
    const alloc = std.testing.allocator;

    const FakeReads = struct {
        fn source() table_reads.TableReadSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                    .query_group_local = queryGroupLocal,
                },
            };
        }

        fn lookup(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: db_mod.types.LookupOptions,
            _: @import("../raft/mod.zig").ReadConsistency,
        ) !?table_reads.LookupResponse {
            return null;
        }

        fn scan(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: []const u8,
            _: db_mod.types.ScanOptions,
            _: @import("../raft/mod.zig").ReadConsistency,
        ) !?table_reads.ScanResponse {
            return null;
        }

        fn query(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: db_mod.types.SearchRequest,
            _: @import("../raft/mod.zig").ReadConsistency,
        ) !?query_api.QueryResponse {
            return null;
        }

        fn queryGroupLocal(
            _: *anyopaque,
            _: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            _: db_mod.types.SearchRequest,
            _: @import("../raft/mod.zig").ReadConsistency,
        ) !?query_api.QueryResponse {
            try std.testing.expectEqual(@as(u64, 7), group_id);
            try std.testing.expectEqualStrings("docs", table_name);
            return error.DocIdentityNamespaceMismatch;
        }
    };

    var resp = (try handle(.{
        .alloc = alloc,
        .reads = FakeReads.source(),
        .catalog = .{
            .ptr = undefined,
        },
        .query_router = .{
            .ptr = undefined,
            .route_query_to_read_schema = struct {
                fn route(_: *anyopaque, _: []const u8, _: *db_mod.types.SearchRequest) !void {}
            }.route,
        },
    }, .{
        .method = .POST,
        .uri = "/internal/v1/groups/7/tables/docs/query",
        .body = "{\"full_text_search\":{\"query\":\"hello\"}}",
    }, "/internal/v1/groups/7/tables/docs/query", "")).?;
    defer resp.deinit(alloc);

    try std.testing.expectEqual(@as(u16, 409), resp.status);
    try std.testing.expectEqualStrings("doc identity namespace mismatch", resp.body);
}

test "internal group vector worker rejects unsupported identity generation" {
    const alloc = std.testing.allocator;

    const access_path = algebraic_ir.vectorAccessPath("dense_idx", .dense_vector);
    const candidate_input = algebraic_ir.TensorExpr{
        .fragment = .slice,
        .output_dims = &.{.doc},
        .semantic_id = "native_doc_id_constraints",
    };
    const program = algebraic_ir.TensorProgram{
        .inputs = &.{candidate_input},
        .steps = &.{.{
            .expr = .{
                .fragment = .vector_search,
                .input_dims = &.{.doc},
                .output_dims = &.{ .doc, .score },
                .owner = "dense_idx",
                .layout = .dense_vector,
            },
            .inputs = &.{.{ .input = 0 }},
        }},
        .output = .{ .step = 0 },
    };
    const body = try query_contract.encodeAlgebraicVectorWorkerRequestEnvelopeAlloc(
        alloc,
        "dense_idx",
        .dense_vector,
        .{ .dense = .{ .vector = &.{ 0.25, 0.5 }, .k = 7 } },
        .{ .identity_read_generation = 12345 },
        .{
            .positive_filter = true,
            .include_doc_ids = &.{"doc:a"},
        },
        null,
        null,
        &.{access_path},
        program,
    );
    defer alloc.free(body);

    const FakeReads = struct {
        fn source() table_reads.TableReadSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                    .query_group_local = queryGroupLocal,
                },
            };
        }

        fn lookup(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: db_mod.types.LookupOptions,
            _: @import("../raft/mod.zig").ReadConsistency,
        ) !?table_reads.LookupResponse {
            return null;
        }

        fn scan(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: []const u8,
            _: db_mod.types.ScanOptions,
            _: @import("../raft/mod.zig").ReadConsistency,
        ) !?table_reads.ScanResponse {
            return null;
        }

        fn query(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: db_mod.types.SearchRequest,
            _: @import("../raft/mod.zig").ReadConsistency,
        ) !?query_api.QueryResponse {
            return null;
        }

        fn queryGroupLocal(
            _: *anyopaque,
            _: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            req: db_mod.types.SearchRequest,
            _: @import("../raft/mod.zig").ReadConsistency,
        ) !?query_api.QueryResponse {
            try std.testing.expectEqual(@as(u64, 7), group_id);
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expectEqual(@as(?u64, 12345), req.identity_read_generation);
            return error.UnsupportedQueryRequest;
        }
    };

    var resp = (try handle(.{
        .alloc = alloc,
        .reads = FakeReads.source(),
        .catalog = .{
            .ptr = undefined,
        },
        .query_router = .{
            .ptr = undefined,
            .route_query_to_read_schema = struct {
                fn route(_: *anyopaque, _: []const u8, _: *db_mod.types.SearchRequest) !void {}
            }.route,
        },
    }, .{
        .method = .POST,
        .uri = "/internal/v1/groups/7/tables/docs/vector-worker",
        .body = body,
    }, "/internal/v1/groups/7/tables/docs/vector-worker", "")).?;
    defer resp.deinit(alloc);

    try std.testing.expectEqual(@as(u16, 400), resp.status);
    try std.testing.expectEqualStrings("UnsupportedQueryRequest", resp.body);
}

test "internal group graph expand rejects unsupported identity generation" {
    const alloc = std.testing.allocator;

    const FakeReads = struct {
        fn source() table_reads.TableReadSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                    .graph_expand_group_local = graphExpandGroupLocal,
                },
            };
        }

        fn lookup(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: db_mod.types.LookupOptions,
            _: @import("../raft/mod.zig").ReadConsistency,
        ) !?table_reads.LookupResponse {
            return null;
        }

        fn scan(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: []const u8,
            _: db_mod.types.ScanOptions,
            _: @import("../raft/mod.zig").ReadConsistency,
        ) !?table_reads.ScanResponse {
            return null;
        }

        fn query(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: db_mod.types.SearchRequest,
            _: @import("../raft/mod.zig").ReadConsistency,
        ) !?query_api.QueryResponse {
            return null;
        }

        fn graphExpandGroupLocal(
            _: *anyopaque,
            _: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            req: distributed_graph.GraphExpandRequest,
            _: @import("../raft/mod.zig").ReadConsistency,
        ) !?distributed_graph.GraphExpandResponse {
            try std.testing.expectEqual(@as(u64, 7), group_id);
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expectEqual(@as(?u64, 12345), req.identity_read_generation);
            return error.UnsupportedQueryRequest;
        }
    };

    var resp = (try handle(.{
        .alloc = alloc,
        .reads = FakeReads.source(),
        .catalog = .{
            .ptr = undefined,
        },
        .query_router = .{
            .ptr = undefined,
            .route_query_to_read_schema = struct {
                fn route(_: *anyopaque, _: []const u8, _: *db_mod.types.SearchRequest) !void {}
            }.route,
        },
    }, .{
        .method = .POST,
        .uri = "/internal/v1/groups/7/tables/docs/graph-expand",
        .body =
        \\{
        \\  "name":"g",
        \\  "index_name":"graph_idx",
        \\  "frontier":[{"id":0,"key":"doc:a"}],
        \\  "identity_read_generation":12345,
        \\  "params":{"direction":"out","edge_types":[],"max_depth":1}
        \\}
        ,
    }, "/internal/v1/groups/7/tables/docs/graph-expand", "")).?;
    defer resp.deinit(alloc);

    try std.testing.expectEqual(@as(u16, 400), resp.status);
    try std.testing.expectEqualStrings("UnsupportedQueryRequest", resp.body);
}

test "internal group read routes handle query preflight" {
    const alloc = std.testing.allocator;

    const FakeReads = struct {
        fn source() table_reads.TableReadSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                    .preflight_query_group_local = preflightQueryGroupLocal,
                },
            };
        }

        fn lookup(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: @import("../storage/db/mod.zig").types.LookupOptions,
            _: @import("../raft/mod.zig").ReadConsistency,
        ) !?table_reads.LookupResponse {
            return null;
        }

        fn scan(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: []const u8,
            _: @import("../storage/db/mod.zig").types.ScanOptions,
            _: @import("../raft/mod.zig").ReadConsistency,
        ) !?table_reads.ScanResponse {
            return null;
        }

        fn query(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: @import("../storage/db/mod.zig").types.SearchRequest,
            _: @import("../raft/mod.zig").ReadConsistency,
        ) !?@import("query.zig").QueryResponse {
            return null;
        }

        fn preflightQueryGroupLocal(
            _: *anyopaque,
            alloc_inner: std.mem.Allocator,
            _: u64,
            _: []const u8,
            _: @import("../storage/db/mod.zig").types.SearchRequest,
            _: @import("../raft/mod.zig").ReadConsistency,
            _: u32,
        ) !?@import("../storage/db/mod.zig").RuntimePreflightSummary {
            const result_refs = try alloc_inner.alloc([]const u8, 1);
            errdefer alloc_inner.free(result_refs);
            result_refs[0] = try alloc_inner.dupe(u8, "$embeddings_results");
            errdefer alloc_inner.free(result_refs[0]);
            const text_indexes = try alloc_inner.dupe(@import("../storage/db/mod.zig").TextIndexEstimate, &.{.{
                .name = try alloc_inner.dupe(u8, "search_idx"),
                .doc_count = 42,
                .chunk_backed = false,
                .group_chunk_parents = false,
            }});
            errdefer {
                alloc_inner.free(text_indexes[0].name);
                alloc_inner.free(text_indexes);
            }
            var summary: @import("../storage/db/mod.zig").RuntimePreflightSummary = .{
                .result_refs = result_refs,
                .graph_query_order = &.{},
                .text_indexes = text_indexes,
                .structured_filter_doc_count_sample_estimate = 5,
                .structured_filter_count_sample_size = 3,
                .stored_projection_doc_upper_bound_total = 8,
                .rerank_doc_upper_bound = 4,
                .aggregation_may_scan_full_results = true,
                .shard_count = 1,
            };
            @import("../storage/db/mod.zig").deriveRuntimePreflightEstimates(&summary);
            return summary;
        }
    };

    var resp = (try handle(.{
        .alloc = alloc,
        .reads = FakeReads.source(),
        .catalog = .{
            .ptr = undefined,
        },
        .query_router = .{
            .ptr = undefined,
            .route_query_to_read_schema = struct {
                fn route(_: *anyopaque, _: []const u8, _: *@import("../storage/db/mod.zig").types.SearchRequest) !void {}
            }.route,
        },
    }, .{
        .method = .POST,
        .uri = "/internal/v1/groups/7/tables/docs/query-preflight",
        .body = "{\"embeddings\":{\"dense_idx\":[1.0,0.0,0.0]},\"indexes\":[\"dense_idx\"],\"limit\":3}",
    }, "/internal/v1/groups/7/tables/docs/query-preflight", "")).?;
    defer resp.deinit(alloc);

    try std.testing.expectEqual(@as(u16, 200), resp.status);
    var parsed = try std.json.parseFromSlice(@import("../storage/db/mod.zig").RuntimePreflightSummary, alloc, resp.body, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 1), parsed.value.result_refs.len);
    try std.testing.expectEqualStrings("$embeddings_results", parsed.value.result_refs[0]);
    try std.testing.expectEqual(@as(?u64, 42), parsed.value.corpus_doc_count_estimate);
    try std.testing.expectEqual(@as(?u32, 5), parsed.value.result_doc_estimate);
    try std.testing.expectEqual(@as(?u64, 5), parsed.value.effective_stored_projection_doc_estimate_total);
    try std.testing.expectEqual(@as(?u32, 4), parsed.value.effective_rerank_doc_upper_bound);
    try std.testing.expectEqual(@as(?u32, 5), parsed.value.aggregation_second_pass_doc_estimate);
}
