// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! Query result transforms shared by local and remote execution, independent
//! of routing, replication, and physical storage coordination.
const std = @import("std");
const builtin = @import("builtin");
const local = @import("antfly_local_sources");
const db_mod = local.storage_db_control_root;
const query_api = local.api_query;
const common_secrets = local.common_secrets;
const managed_embedder = local.inference_managed_embedder;
const remote_capabilities = @import("antfly_inference_remote_capabilities");
const execution_context = @import("antfly_inference_execution_context");
const inference_request_context = execution_context;
const scraping = @import("antfly_scraping");
const httpx = @import("httpx");
const platform_time = @import("antfly_platform").time;
const checkQueryDeadline = local.api_local_query_contract.checkQueryDeadline;
const reranking_runtime = @import("../reranking/mod.zig");
const template_mod = local.template;
const template_remote = if (builtin.os.tag == .freestanding)
    local.storage_db_template_remote_stub
else
    local.template_remote;

pub const ManagedReadRuntimeConfig = struct {
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime = null,
    antfly_provider: ?managed_embedder.AntflyProvider = null,
    decision_registry: ?*const @import("antfly_local_sources").common_provider_registry.Registry = null,
    inference_api_url: ?[]const u8 = null,
    secret_store: ?*common_secrets.FileStore = null,
    reranker_runtime: ?*reranking_runtime.Runtime = null,
    remote_content: ?*const scraping.RemoteContentConfig = null,
    remote_capability_cache: ?*remote_capabilities.Cache = null,
    source_table: []const u8 = "",

    pub fn forTable(self: ManagedReadRuntimeConfig, table_name: []const u8) ManagedReadRuntimeConfig {
        var routed = self;
        routed.source_table = table_name;
        return routed;
    }
};

pub fn applyQueryPostProcessing(
    alloc: std.mem.Allocator,
    req: db_mod.types.SearchRequest,
    result: *db_mod.types.SearchResult,
    meta: *query_api.QueryResponseMeta,
    runtime_cfg: ManagedReadRuntimeConfig,
) !void {
    if (req.evaluation_limit > 0) {
        try @import("antfly_local_sources").functions_query_eval.apply(alloc, req, result, meta, runtime_cfg);
        return;
    }
    if ((req.reranker == null and req.pruner == null) or result.hits.len == 0) return;
    const candidate_count = if (req.reranker != null)
        try applyReranker(alloc, req, result, meta, runtime_cfg)
    else
        result.hits.len;
    const pruned_count = pruneSearchHitPrefix(req, result.hits[0..candidate_count]).len;
    const output_limit = if (req.reranker) |reranker| rerankerOutputLimit(req.limit, reranker.top_n) else req.limit;
    try pageSearchHitsAfterScoreTransforms(alloc, result, pruned_count, req.offset, output_limit);
}

pub fn applyReranker(
    alloc: std.mem.Allocator,
    req: db_mod.types.SearchRequest,
    result: *db_mod.types.SearchResult,
    meta: *query_api.QueryResponseMeta,
    runtime_cfg: ManagedReadRuntimeConfig,
) !usize {
    const cfg = req.reranker orelse return 0;
    if (req.reranker_query_text.len == 0) return error.UnsupportedQueryRequest;
    try checkQueryDeadline(req);

    const output_limit = rerankerOutputLimit(req.limit, cfg.top_n);
    const rerank_count = rerankerCandidateCount(result.hits.len, cfg.candidate_count, req.offset, output_limit);

    var inference_lane: ?db_mod.background_runtime.BackendRuntime.InferenceLaneLease = null;
    defer if (inference_lane) |*lease| lease.release();
    var fallback_io: ?std.Io.Threaded = null;
    defer if (fallback_io) |*io_impl| io_impl.deinit();
    const io = if (runtime_cfg.reranker_runtime) |runtime|
        runtime.io
    else if (runtime_cfg.backend_runtime) |backend| blk: {
        inference_lane = try backend.acquireInferenceLane();
        break :blk inference_lane.?.io();
    } else blk: {
        fallback_io = std.Io.Threaded.init(std.heap.page_allocator, .{});
        break :blk fallback_io.?.io();
    };

    var fallback_http: ?httpx.Client = null;
    defer if (fallback_http) |*http| http.deinit();
    const http = if (runtime_cfg.reranker_runtime) |runtime|
        &runtime.http
    else blk: {
        fallback_http = httpx.Client.initWithConfig(alloc, io, .{ .keep_alive = false });
        break :blk &fallback_http.?;
    };
    const inference_context = inference_request_context.RequestContext{
        .io = io,
        .deadline_ns = req.execution_deadline_ns,
        .cancellation = req.cancellation,
    };
    var admission_lease: ?reranking_runtime.AdmissionLease = if (runtime_cfg.reranker_runtime) |runtime|
        try runtime.acquire(inference_context)
    else
        null;
    defer if (admission_lease) |*lease| lease.release();
    const rerank_start_ns = platform_time.monotonicNs();

    const doc_template = if (cfg.template.len > 0)
        try alloc.dupe(u8, cfg.template)
    else
        try std.fmt.allocPrint(alloc, "{{{{{s}}}}}", .{cfg.field});
    defer alloc.free(doc_template);

    var documents = try RerankerDocuments.init(alloc, rerank_count);
    defer documents.deinit();
    const render_config = rerankerRenderConfig(runtime_cfg, req, io);
    for (result.hits[0..rerank_count], 0..) |hit, i| {
        if ((i & 31) == 0) try inference_context.check();
        try documents.render(i, doc_template, hit, render_config);
    }
    const content = try documents.content();

    const dependencies: reranking_runtime.Options = .{
        .antfly_provider = runtime_cfg.antfly_provider,
        .secret_store = runtime_cfg.secret_store,
        .capability_cache = runtime_cfg.remote_capability_cache,
        .execution = execution_context.Context{
            .default_endpoint = runtime_cfg.inference_api_url,
            .capability_cache = runtime_cfg.remote_capability_cache,
            .io = io,
            .routing = .{ .source_table = runtime_cfg.source_table },
            .deadline_ns = req.execution_deadline_ns,
            .cancellation = req.cancellation orelse .none,
        },
        .execution_context = inference_context,
    };
    const scores = if (runtime_cfg.reranker_runtime) |runtime|
        runtime.rerankContentAdmitted(alloc, cfg, dependencies, req.reranker_query_text, content)
    else
        reranking_runtime.rerankContentWithOptions(
            alloc,
            http,
            cfg,
            dependencies,
            req.reranker_query_text,
            content,
        );
    const owned_scores = scores catch |err| switch (err) {
        error.InvalidRateLimitPolicy,
        error.ConflictingRateLimitPolicy,
        error.ProviderTokenBudgetExceeded,
        error.UnsupportedMediaTokenBudget,
        error.UnsupportedLocalRateLimit,
        error.InvalidRerankerConfig,
        error.UnsupportedRerankerProvider,
        error.MissingVertexCredentials,
        error.SecretNotFound,
        // The template produced images for a provider or model that cannot
        // score them; the query's reranker configuration must change.
        error.RerankerMediaUnsupported,
        => return error.InvalidQueryRequest,
        else => {
            std.log.debug("reranker provider request failed provider={s} err={s}", .{
                @tagName(cfg.provider),
                @errorName(err),
            });
            const normalized = reranking_runtime.normalizeOperationalError(err);
            return switch (normalized) {
                error.OutOfMemory,
                error.Timeout,
                error.Canceled,
                error.Cancelled,
                error.RerankRateLimited,
                error.RerankTransientFailure,
                error.RerankUpstreamFailure,
                => normalized,
                // Provider implementations expose transport- and parser-
                // specific errors. Keep those details out of the public API.
                else => error.RerankUpstreamFailure,
            };
        },
    };
    defer alloc.free(owned_scores);
    try checkQueryDeadline(req);
    if (owned_scores.len != rerank_count) return error.RerankUpstreamFailure;

    for (result.hits[0..rerank_count], 0..) |*hit, i| {
        hit.score = owned_scores[i];
    }
    std.sort.pdq(db_mod.types.SearchHit, result.hits[0..rerank_count], {}, struct {
        fn lessThan(_: void, a: db_mod.types.SearchHit, b: db_mod.types.SearchHit) bool {
            const a_score = a.score orelse 0;
            const b_score = b.score orelse 0;
            if (a_score != b_score) return a_score > b_score;
            return std.mem.order(u8, a.id, b.id) == .lt;
        }
    }.lessThan);

    meta.reranker = .{
        .provider = cfg.provider,
        .model = cfg.model,
        .documents_reranked = @intCast(owned_scores.len),
        .duration_ms = @intCast(@divTrunc(platform_time.monotonicNs() - rerank_start_ns, std.time.ns_per_ms)),
    };
    return rerank_count;
}

const SearchHitPrunerAdapter = struct {
    pub fn score(hit: db_mod.types.SearchHit) f64 {
        return @floatCast(hit.score orelse 0);
    }

    pub fn indexCount(hit: db_mod.types.SearchHit) usize {
        return hit.index_scores.len;
    }
};

fn pruneSearchHitPrefix(req: db_mod.types.SearchRequest, hits: []db_mod.types.SearchHit) []db_mod.types.SearchHit {
    const pruner = req.pruner orelse return hits;
    return pruner.pruneWith(hits, SearchHitPrunerAdapter);
}

pub fn rerankerCandidateCount(
    hit_count: usize,
    candidate_count: ?u32,
    offset: u32,
    output_limit: u32,
) usize {
    const window = candidate_count orelse offset +| output_limit;
    return @min(hit_count, window);
}

pub fn rerankerOutputLimit(query_limit: u32, top_n: ?u32) u32 {
    return top_n orelse query_limit;
}

fn rerankerRenderConfig(
    runtime_cfg: ManagedReadRuntimeConfig,
    req: db_mod.types.SearchRequest,
    io: std.Io,
) template_remote.RenderConfig {
    var config: template_remote.RenderConfig = .{};
    if (comptime @hasField(template_remote.RenderConfig, "remote_content")) config.remote_content = runtime_cfg.remote_content;
    if (comptime @hasField(template_remote.RenderConfig, "secret_store")) config.secret_store = runtime_cfg.secret_store;
    if (comptime @hasField(template_remote.RenderConfig, "io")) config.io = io;
    if (comptime @hasField(template_remote.RenderConfig, "deadline_ns")) config.deadline_ns = req.execution_deadline_ns;
    if (comptime @hasField(template_remote.RenderConfig, "cancellation")) {
        if (req.cancellation) |token| if (token.ptr != null and token.is_cancelled_fn != null) {
            config.cancellation = scraping.CancellationToken.fromCallback(token.ptr.?, token.is_cancelled_fn.?);
        };
    }
    return config;
}

/// Only the directives the template helpers emit select part parsing. Plain
/// text that merely contains `<<<`, such as indexed logs, must reach the
/// scorer unchanged rather than be trimmed or stripped by the part parser.
pub fn hasRenderedDirective(rendered: []const u8) bool {
    for ([_][]const u8{ "<<<dotprompt:media:url ", "<<<error:status=", "<<<error:message=" }) |prefix| {
        if (std.mem.indexOf(u8, rendered, prefix) != null) return true;
    }
    return false;
}

fn isFatalRerankerRenderError(err: anyerror) bool {
    return err == error.OutOfMemory or err == error.Timeout or err == error.Canceled or err == error.Cancelled;
}

/// Rendered reranker candidates. A template renders each hit to text; one
/// that uses `media` or `remoteMedia` also yields image parts, which only a
/// multimodal Antfly reranker can score.
const RerankerDocuments = struct {
    alloc: std.mem.Allocator,
    texts: [][]const u8,
    /// Owned parts for documents whose rendering produced media markers.
    owned_parts: []?[]template_mod.ContentPart,
    /// Single text part per document, used as the parts view of a document
    /// without media when another document in the request has media.
    text_parts: []template_mod.ContentPart,
    parts_view: [][]const template_mod.ContentPart,
    initialized: usize = 0,

    fn init(alloc: std.mem.Allocator, count: usize) !RerankerDocuments {
        const texts = try alloc.alloc([]const u8, count);
        errdefer alloc.free(texts);
        const owned_parts = try alloc.alloc(?[]template_mod.ContentPart, count);
        errdefer alloc.free(owned_parts);
        @memset(owned_parts, null);
        const text_parts = try alloc.alloc(template_mod.ContentPart, count);
        errdefer alloc.free(text_parts);
        const parts_view = try alloc.alloc([]const template_mod.ContentPart, count);
        return .{ .alloc = alloc, .texts = texts, .owned_parts = owned_parts, .text_parts = text_parts, .parts_view = parts_view };
    }

    pub fn deinit(self: *RerankerDocuments) void {
        for (self.texts[0..self.initialized]) |text| self.alloc.free(text);
        for (self.owned_parts) |maybe_parts| if (maybe_parts) |parts| template_mod.freeContentParts(self.alloc, parts);
        self.alloc.free(self.texts);
        self.alloc.free(self.owned_parts);
        self.alloc.free(self.text_parts);
        self.alloc.free(self.parts_view);
        self.* = undefined;
    }

    /// Renders hit `index`. A hit without stored data, or one whose template
    /// fails to render, becomes an empty document so one bad row cannot fail
    /// the query; cancellation, deadline, and allocation failures propagate.
    fn render(
        self: *RerankerDocuments,
        index: usize,
        doc_template: []const u8,
        hit: db_mod.types.SearchHit,
        config: template_remote.RenderConfig,
    ) !void {
        std.debug.assert(index == self.initialized);
        const raw = hit.stored_data orelse {
            self.texts[index] = try self.alloc.dupe(u8, "");
            self.initialized += 1;
            return;
        };
        const rendered = template_remote.renderJsonToTextWithConfig(self.alloc, doc_template, raw, config) catch |err| {
            if (isFatalRerankerRenderError(err)) return err;
            self.texts[index] = try self.alloc.dupe(u8, "");
            self.initialized += 1;
            return;
        };
        if (!hasRenderedDirective(rendered)) {
            self.texts[index] = rendered;
            self.initialized += 1;
            return;
        }
        defer self.alloc.free(rendered);
        // Markers are present: split media from text and drop the error
        // directives a failed remote fetch leaves behind.
        const parts = try template_mod.textToParts(self.alloc, rendered);
        var joined: std.ArrayListUnmanaged(u8) = .empty;
        errdefer joined.deinit(self.alloc);
        var has_media = false;
        for (parts) |part| switch (part) {
            .text => |text| {
                if (joined.items.len > 0) try joined.append(self.alloc, '\n');
                try joined.appendSlice(self.alloc, text);
            },
            else => has_media = true,
        };
        self.texts[index] = try joined.toOwnedSlice(self.alloc);
        self.initialized += 1;
        if (has_media) {
            self.owned_parts[index] = parts;
        } else {
            template_mod.freeContentParts(self.alloc, parts);
        }
    }

    fn content(self: *RerankerDocuments) !reranking_runtime.Documents {
        std.debug.assert(self.initialized == self.texts.len);
        var has_media = false;
        for (self.owned_parts) |parts| if (parts != null) {
            has_media = true;
        };
        if (!has_media) return .{ .texts = self.texts };
        for (self.parts_view, self.owned_parts, self.text_parts, self.texts) |*view, owned, *text_part, text| {
            if (owned) |parts| {
                view.* = parts;
            } else {
                text_part.* = .{ .text = text };
                view.* = @as(*const [1]template_mod.ContentPart, text_part);
            }
        }
        return .{ .texts = self.texts, .parts = self.parts_view };
    }
};

pub fn pageSearchHitsAfterScoreTransforms(
    alloc: std.mem.Allocator,
    result: *db_mod.types.SearchResult,
    candidate_count: usize,
    offset: u32,
    limit: u32,
) !void {
    const old_hits = result.hits;
    const candidate_end = @min(candidate_count, old_hits.len);
    const start = @min(@as(usize, offset), candidate_end);
    const keep_len = @min(@as(usize, limit), candidate_end - start);
    if (start == 0 and keep_len == old_hits.len and candidate_end == old_hits.len) return;
    var kept = try alloc.alloc(db_mod.types.SearchHit, keep_len);
    for (old_hits, 0..) |*hit, i| {
        if (i >= start and i < start + keep_len) {
            kept[i - start] = hit.*;
            hit.* = undefined;
        } else {
            hit.deinit(alloc);
        }
    }
    alloc.free(old_hits);
    result.hits = kept;
}
