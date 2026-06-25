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
const indexes_openapi = @import("antfly_indexes_openapi");
const metadata_openapi = @import("antfly_metadata_openapi");
const db_mod = @import("../../storage/db/mod.zig");
const doc_set = @import("../../storage/db/doc_set.zig");
const graph_mod = @import("../../graph/graph.zig");
const graph_paths = @import("../../graph/paths.zig");
const graph_query_mod = @import("../../graph/query.zig");
const distributed_stats_mod = @import("../../search/distributed_stats.zig");
const query_api = @import("../query.zig");
const query_contract = @import("../query_contract.zig");

pub fn searchRequestHasResolvedDocFilter(req: db_mod.types.SearchRequest) bool {
    if (comptime @hasField(db_mod.types.SearchRequest, "resolved_doc_filter")) {
        return req.resolved_doc_filter != null;
    }
    return false;
}

pub fn searchRequestHasUnserializableResolvedDocFilter(req: db_mod.types.SearchRequest) bool {
    return searchRequestHasResolvedDocFilter(req) and req.resolved_doc_filter_wire_context == null;
}

pub fn parseRemoteSearchResultForHostedQuery(alloc: std.mem.Allocator, body: []const u8) !db_mod.types.SearchResult {
    return parseRemoteSearchResult(alloc, body) catch |err| switch (err) {
        error.OutOfMemory => error.OutOfMemory,
        else => error.UnsupportedQueryRequest,
    };
}

pub fn encodeLookupFields(alloc: std.mem.Allocator, opts: db_mod.types.LookupOptions) !?[]u8 {
    if (opts.include_all_fields or opts.fields.len == 0) return null;
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    for (opts.fields, 0..) |field, i| {
        if (i > 0) try out.append(alloc, ',');
        try out.appendSlice(alloc, field);
    }
    return try out.toOwnedSlice(alloc);
}

pub fn encodeScanRequest(
    alloc: std.mem.Allocator,
    from_key: []const u8,
    to_key: []const u8,
    opts: db_mod.types.ScanOptions,
) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.append(alloc, '{');
    var first = true;
    if (from_key.len > 0) {
        try appendJsonFieldString(alloc, &out, &first, "from", from_key);
    }
    if (to_key.len > 0) {
        try appendJsonFieldString(alloc, &out, &first, "to", to_key);
    }
    if (opts.limit > 0) {
        try appendJsonFieldU32(alloc, &out, &first, "limit", opts.limit);
    }
    if (opts.fields.len > 0 and !opts.include_all_fields) {
        try appendJsonFieldNames(alloc, &out, &first, "fields", opts.fields);
    }
    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

pub fn encodeQueryRequest(alloc: std.mem.Allocator, req: db_mod.types.SearchRequest) ![]u8 {
    if (searchRequestHasUnserializableResolvedDocFilter(req)) return error.UnsupportedQueryRequest;
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.append(alloc, '{');
    var first = true;

    if (req.fields.len > 0 and !req.include_all_fields) {
        try appendJsonFieldNames(alloc, &out, &first, "fields", req.fields);
    }
    if (req.limit != 10) {
        try appendJsonFieldU32(alloc, &out, &first, "limit", req.limit);
    }
    if (req.offset != 0) {
        try appendJsonFieldU32(alloc, &out, &first, "offset", req.offset);
    }
    if (req.count_only) {
        try appendJsonFieldBool(alloc, &out, &first, "count", true);
    }
    if (req.profile) {
        try appendJsonFieldBool(alloc, &out, &first, "profile", true);
    }
    if (req.filter_prefix.len > 0) {
        try appendJsonFieldString(alloc, &out, &first, "filter_prefix", req.filter_prefix);
    }
    if (req.distance_over) |value| {
        try appendJsonFieldF32(alloc, &out, &first, "distance_over", value);
    }
    if (req.distance_under) |value| {
        try appendJsonFieldF32(alloc, &out, &first, "distance_under", value);
    }
    if (req.merge_config) |merge_config| {
        try appendMergeConfigField(alloc, &out, &first, merge_config);
    }
    if (req.pruner) |pruner| {
        try appendPrunerField(alloc, &out, &first, pruner);
    }
    if (req.distributed_text_stats.len > 0) {
        try appendDistributedTextStatsField(alloc, &out, &first, req.distributed_text_stats);
    }
    if (req.identity_read_generation) |generation| {
        try appendJsonFieldU64(alloc, &out, &first, "_identity_read_generation", generation);
    }
    if (req.resolved_doc_filter != null) {
        try db_mod.doc_filter_wire.appendSearchRequestFieldAlloc(alloc, &out, &first, req);
    }
    const native_doc_id_constraints = query_contract.nativeDocIdConstraintEnvelopeFromSearchRequest(req);
    if (native_doc_id_constraints.hasConstraints()) {
        try appendNativeDocIdConstraintsField(alloc, &out, &first, native_doc_id_constraints);
    }
    if (req.filter_query_json.len > 0) {
        try appendJsonFieldString(alloc, &out, &first, "_filter_query_json", req.filter_query_json);
    }
    if (req.exclusion_query_json.len > 0) {
        try appendJsonFieldString(alloc, &out, &first, "_exclusion_query_json", req.exclusion_query_json);
    }
    if (req.graph_queries.len > 0) {
        try appendGraphQueriesField(alloc, &out, &first, req.graph_queries);
    }
    if (req.graph_metric_queries.len > 0) {
        try appendGraphMetricQueryField(alloc, &out, &first, req.graph_metric_queries);
    }
    if (req.graph_metric_rerank) |rerank| {
        try appendGraphMetricRerankField(alloc, &out, &first, rerank);
    }
    if (req.expand_strategy) |expand_strategy| {
        try appendJsonFieldString(alloc, &out, &first, "expand_strategy", switch (expand_strategy) {
            .@"union" => "union",
            .intersection => "intersection",
        });
    }
    if (req.dense_queries.len > 0 or req.sparse_queries.len > 0) {
        try appendEmbeddingsField(alloc, &out, &first, req.dense_queries, req.sparse_queries);
    }
    if (req.full_text) |full_text| {
        try appendTextQueryField(alloc, &out, &first, "full_text_search", full_text);
    } else {
        try appendQueryField(alloc, &out, &first, req.query, req.limit);
    }

    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

fn appendNativeDocIdConstraintsField(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    constraints: query_contract.NativeDocIdConstraintEnvelope,
) !void {
    const encoded = try query_contract.encodeNativeDocIdConstraintEnvelopeAlloc(alloc, constraints);
    defer alloc.free(encoded);
    try appendJsonFieldName(alloc, out, first, "native_doc_id_constraints");
    try out.appendSlice(alloc, encoded);
}

fn appendDistributedTextStatsField(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    items: []const distributed_stats_mod.TextFieldStats,
) !void {
    try appendJsonFieldName(alloc, out, first, "_distributed_text_stats");
    try out.append(alloc, '[');
    for (items, 0..) |item, i| {
        if (i > 0) try out.append(alloc, ',');
        try out.append(alloc, '{');
        var field_first = true;
        try appendJsonFieldString(alloc, out, &field_first, "field", item.field);
        try appendJsonFieldU32(alloc, out, &field_first, "global_doc_count", item.global_doc_count);
        try appendJsonFieldU64(alloc, out, &field_first, "global_total_field_len", item.global_total_field_len);
        try appendJsonFieldName(alloc, out, &field_first, "term_doc_freqs");
        try out.append(alloc, '[');
        for (item.term_doc_freqs, 0..) |term, term_idx| {
            if (term_idx > 0) try out.append(alloc, ',');
            try out.append(alloc, '{');
            var term_first = true;
            try appendJsonFieldString(alloc, out, &term_first, "term", term.term);
            try appendJsonFieldU32(alloc, out, &term_first, "doc_freq", term.doc_freq);
            try out.append(alloc, '}');
        }
        try out.appendSlice(alloc, "]}");
    }
    try out.append(alloc, ']');
}

pub fn appendJsonFieldU64(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    name: []const u8,
    value: u64,
) !void {
    try appendJsonFieldName(alloc, out, first, name);
    var buf: [32]u8 = undefined;
    const rendered = try std.fmt.bufPrint(&buf, "{d}", .{value});
    try out.appendSlice(alloc, rendered);
}

fn appendMergeConfigField(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    merge_config: db_mod.types.MergeConfig,
) !void {
    try appendJsonFieldName(alloc, out, first, "merge_config");
    try out.append(alloc, '{');
    var merge_first = true;
    try appendJsonFieldString(alloc, out, &merge_first, "strategy", switch (merge_config.strategy) {
        .rrf => "rrf",
        .rsf => "rsf",
    });
    if (merge_config.rank_constant != 60.0) {
        try appendJsonFieldF64(alloc, out, &merge_first, "rank_constant", merge_config.rank_constant);
    }
    if (merge_config.window_size != 0) {
        try appendJsonFieldU32(alloc, out, &merge_first, "window_size", merge_config.window_size);
    }
    if (merge_config.weights.len > 0) {
        try appendJsonFieldName(alloc, out, &merge_first, "weights");
        try out.append(alloc, '{');
        for (merge_config.weights, 0..) |weight, i| {
            if (i > 0) try out.append(alloc, ',');
            try appendJsonString(alloc, out, weight.name);
            try out.append(alloc, ':');
            var weight_buf: [32]u8 = undefined;
            const rendered = try std.fmt.bufPrint(&weight_buf, "{d}", .{weight.weight});
            try out.appendSlice(alloc, rendered);
        }
        try out.append(alloc, '}');
    }
    try out.append(alloc, '}');
}

fn appendPrunerField(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    pruner: @import("../../search/fusion.zig").Pruner,
) !void {
    try appendJsonFieldName(alloc, out, first, "pruner");
    try out.append(alloc, '{');
    var pruner_first = true;
    if (pruner.min_score_ratio > 0) {
        try appendJsonFieldF64(alloc, out, &pruner_first, "min_score_ratio", pruner.min_score_ratio);
    }
    if (pruner.max_score_gap_percent > 0) {
        try appendJsonFieldF64(alloc, out, &pruner_first, "max_score_gap_percent", pruner.max_score_gap_percent);
    }
    if (pruner.min_absolute_score > 0) {
        try appendJsonFieldF64(alloc, out, &pruner_first, "min_absolute_score", pruner.min_absolute_score);
    }
    if (pruner.require_multi_index) {
        try appendJsonFieldBool(alloc, out, &pruner_first, "require_multi_index", true);
    }
    if (pruner.std_dev_threshold > 0) {
        try appendJsonFieldF64(alloc, out, &pruner_first, "std_dev_threshold", pruner.std_dev_threshold);
    }
    try out.append(alloc, '}');
}

fn appendGraphQueriesField(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    graph_queries: []const db_mod.types.NamedGraphQuery,
) !void {
    try appendJsonFieldName(alloc, out, first, "graph_searches");
    try out.append(alloc, '{');
    for (graph_queries, 0..) |graph_query, i| {
        if (i > 0) try out.append(alloc, ',');
        try appendJsonString(alloc, out, graph_query.name);
        try out.append(alloc, ':');
        try appendGraphQueryValue(alloc, out, graph_query.query);
    }
    try out.append(alloc, '}');
}

fn appendGraphMetricQueryField(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    queries: []const db_mod.types.NamedGraphMetricQuery,
) !void {
    if (queries.len != 1) return;
    const named = queries[0];

    try appendJsonFieldName(alloc, out, first, "graph_metric");
    try out.append(alloc, '{');
    var metric_first = true;
    try appendJsonFieldString(alloc, out, &metric_first, "name", named.name);
    try appendJsonFieldString(alloc, out, &metric_first, "index", named.query.index_name);
    try appendJsonFieldString(alloc, out, &metric_first, "metric", named.query.metric_name);
    try appendJsonFieldU32(alloc, out, &metric_first, "top_k", named.query.top_k);
    try appendJsonFieldString(alloc, out, &metric_first, "metric_freshness", switch (named.query.freshness) {
        .published => "published",
        .fresh => "fresh",
    });
    try out.append(alloc, '}');
}

fn appendGraphMetricRerankField(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    rerank: db_mod.types.GraphMetricRerank,
) !void {
    try appendJsonFieldName(alloc, out, first, "graph_metric_rerank");
    try out.append(alloc, '{');
    var rerank_first = true;
    try appendJsonFieldString(alloc, out, &rerank_first, "index", rerank.index_name);
    try appendJsonFieldString(alloc, out, &rerank_first, "metric", rerank.metric_name);
    try appendJsonFieldF64(alloc, out, &rerank_first, "base_weight", rerank.base_weight);
    try appendJsonFieldF64(alloc, out, &rerank_first, "weight", rerank.weight);
    try appendJsonFieldF64(alloc, out, &rerank_first, "missing_score", rerank.missing_score);
    try appendJsonFieldString(alloc, out, &rerank_first, "metric_freshness", switch (rerank.freshness) {
        .published => "published",
        .fresh => "fresh",
    });
    try out.append(alloc, '}');
}

fn appendGraphQueryValue(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    query: graph_query_mod.GraphQuery,
) !void {
    try out.append(alloc, '{');
    var first = true;
    try appendJsonFieldString(alloc, out, &first, "type", switch (query.query_type) {
        .traverse => "traverse",
        .neighbors => "neighbors",
        .shortest_path => "shortest_path",
        .k_shortest_paths => "k_shortest_paths",
        .pattern => "pattern",
    });
    try appendJsonFieldString(alloc, out, &first, "index_name", query.index_name);
    try appendGraphNodeSelectorField(alloc, out, &first, "start_nodes", query.start_nodes);
    if (query.target_nodes) |target_nodes| {
        try appendGraphNodeSelectorField(alloc, out, &first, "target_nodes", target_nodes);
    }
    try appendGraphQueryParamsField(alloc, out, &first, query.params, query.k);
    if (query.metrics.len > 0) {
        var metric_names = try alloc.alloc([]const u8, query.metrics.len);
        defer alloc.free(metric_names);
        for (query.metrics, 0..) |metric, i| metric_names[i] = metric.name;
        try appendJsonFieldNames(alloc, out, &first, "metrics", metric_names);
        try appendJsonFieldString(alloc, out, &first, "metric_freshness", switch (query.metrics[0].freshness) {
            .published => "published",
            .fresh => "fresh",
        });
    }
    if (query.order_by.len > 0) {
        try appendJsonFieldName(alloc, out, &first, "order_by");
        try out.append(alloc, '[');
        for (query.order_by, 0..) |order, i| {
            if (i > 0) try out.append(alloc, ',');
            try out.append(alloc, '{');
            var order_first = true;
            try appendJsonFieldString(alloc, out, &order_first, "metric", order.name);
            try appendJsonFieldString(alloc, out, &order_first, "direction", switch (order.direction) {
                .asc => "asc",
                .desc => "desc",
            });
            try appendJsonFieldString(alloc, out, &order_first, "nulls", switch (order.nulls) {
                .first => "first",
                .last => "last",
            });
            try out.append(alloc, '}');
        }
        try out.append(alloc, ']');
        if (query.metrics.len == 0) {
            try appendJsonFieldString(alloc, out, &first, "metric_freshness", switch (query.order_by[0].freshness) {
                .published => "published",
                .fresh => "fresh",
            });
        }
    }
    if (query.where_metric.len > 0) {
        try appendJsonFieldName(alloc, out, &first, "where_metric");
        try out.append(alloc, '[');
        for (query.where_metric, 0..) |filter, i| {
            if (i > 0) try out.append(alloc, ',');
            try out.append(alloc, '{');
            var filter_first = true;
            try appendJsonFieldString(alloc, out, &filter_first, "metric", filter.name);
            try appendJsonFieldString(alloc, out, &filter_first, "op", switch (filter.op) {
                .gt => ">",
                .gte => ">=",
                .lt => "<",
                .lte => "<=",
                .eq => "==",
                .neq => "!=",
            });
            try appendJsonFieldF64(alloc, out, &filter_first, "value", filter.value);
            try out.append(alloc, '}');
        }
        try out.append(alloc, ']');
        if (query.metrics.len == 0 and query.order_by.len == 0) {
            try appendJsonFieldString(alloc, out, &first, "metric_freshness", switch (query.where_metric[0].freshness) {
                .published => "published",
                .fresh => "fresh",
            });
        }
    }
    if (query.include_metric_status) try appendJsonFieldBool(alloc, out, &first, "include_metric_status", true);
    try out.append(alloc, '}');
}

fn appendGraphNodeSelectorField(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    name: []const u8,
    selector: graph_query_mod.NodeSelector,
) !void {
    try appendJsonFieldName(alloc, out, first, name);
    try out.append(alloc, '{');
    var selector_first = true;
    switch (selector) {
        .keys => |keys| {
            try appendJsonFieldName(alloc, out, &selector_first, "keys");
            try out.append(alloc, '[');
            for (keys, 0..) |key, i| {
                if (i > 0) try out.append(alloc, ',');
                try appendJsonString(alloc, out, key);
            }
            try out.append(alloc, ']');
        },
        .result_ref => |result_ref| {
            try appendJsonFieldString(alloc, out, &selector_first, "result_ref", result_ref.ref);
            if (result_ref.limit > 0) try appendJsonFieldU32(alloc, out, &selector_first, "limit", result_ref.limit);
        },
    }
    try out.append(alloc, '}');
}

fn appendGraphQueryParamsField(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    params: graph_query_mod.QueryParams,
    k: u32,
) !void {
    try appendJsonFieldName(alloc, out, first, "params");
    try out.append(alloc, '{');
    var params_first = true;
    if (params.edge_types.len > 0) try appendJsonFieldNames(alloc, out, &params_first, "edge_types", params.edge_types);
    if (params.direction != .out) try appendJsonFieldString(alloc, out, &params_first, "direction", switch (params.direction) {
        .out => "out",
        .in => "in",
        .both => "both",
    });
    if (params.max_depth != 3) try appendJsonFieldU32(alloc, out, &params_first, "max_depth", params.max_depth);
    if (params.min_weight != 0) try appendJsonFieldF64(alloc, out, &params_first, "min_weight", params.min_weight);
    if (params.max_weight != 0) try appendJsonFieldF64(alloc, out, &params_first, "max_weight", params.max_weight);
    if (params.max_results != 100) try appendJsonFieldU32(alloc, out, &params_first, "max_results", params.max_results);
    if (!params.deduplicate) try appendJsonFieldBool(alloc, out, &params_first, "deduplicate_nodes", false);
    if (params.include_paths) try appendJsonFieldBool(alloc, out, &params_first, "include_paths", true);
    if (params.weight_mode != .min_hops) try appendJsonFieldString(alloc, out, &params_first, "weight_mode", switch (params.weight_mode) {
        .min_hops => "min_hops",
        .min_weight => "min_weight",
        .max_weight => "max_weight",
    });
    if (k > 1) try appendJsonFieldU32(alloc, out, &params_first, "k", k);
    try out.append(alloc, '}');
}

fn appendEmbeddingsField(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    dense_queries: []const db_mod.types.NamedDenseQuery,
    sparse_queries: []const db_mod.types.NamedSparseQuery,
) !void {
    try appendJsonFieldName(alloc, out, first, "embeddings");
    try out.append(alloc, '{');
    var entry_index: usize = 0;
    for (dense_queries) |dense_query| {
        if (entry_index > 0) try out.append(alloc, ',');
        try appendJsonString(alloc, out, dense_query.index_name);
        try out.appendSlice(alloc, ":[");
        for (dense_query.query.vector, 0..) |value, lane| {
            if (lane > 0) try out.append(alloc, ',');
            try out.print(alloc, "{d}", .{value});
        }
        try out.append(alloc, ']');
        entry_index += 1;
    }
    for (sparse_queries) |sparse_query| {
        if (entry_index > 0) try out.append(alloc, ',');
        try appendJsonString(alloc, out, sparse_query.index_name);
        try out.appendSlice(alloc, ":{\"indices\":[");
        for (sparse_query.query.indices, 0..) |value, lane| {
            if (lane > 0) try out.append(alloc, ',');
            try out.print(alloc, "{d}", .{value});
        }
        try out.appendSlice(alloc, "],\"values\":[");
        for (sparse_query.query.values, 0..) |value, lane| {
            if (lane > 0) try out.append(alloc, ',');
            try out.print(alloc, "{d}", .{value});
        }
        try out.appendSlice(alloc, "]}");
        entry_index += 1;
    }
    try out.append(alloc, '}');
}

fn appendQueryField(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    query: db_mod.types.Query,
    default_k: u32,
) !void {
    try appendJsonFieldName(alloc, out, first, "full_text_search");
    switch (query) {
        .match_all => try out.appendSlice(alloc, "{\"match_all\":{}}"),
        .term => |term| {
            try out.appendSlice(alloc, "{\"term\":");
            try appendJsonString(alloc, out, term.term);
            try out.appendSlice(alloc, ",\"field\":");
            try appendJsonString(alloc, out, term.field);
            try out.append(alloc, '}');
        },
        .match => |match| {
            try out.appendSlice(alloc, "{\"match\":");
            try appendJsonString(alloc, out, match.text);
            try out.appendSlice(alloc, ",\"field\":");
            try appendJsonString(alloc, out, match.field);
            try out.append(alloc, '}');
        },
        .dense_knn => |dense| {
            try out.appendSlice(alloc, "{\"dense_knn\":{\"vector\":[");
            for (dense.vector, 0..) |value, i| {
                if (i > 0) try out.append(alloc, ',');
                try out.print(alloc, "{d}", .{value});
            }
            try out.appendSlice(alloc, "],\"k\":");
            try out.print(alloc, "{d}", .{if (dense.k == 0) default_k else dense.k});
            try out.appendSlice(alloc, "}}");
        },
        .sparse_knn => |sparse| {
            try out.appendSlice(alloc, "{\"sparse_knn\":{\"indices\":[");
            for (sparse.indices, 0..) |value, i| {
                if (i > 0) try out.append(alloc, ',');
                try out.print(alloc, "{d}", .{value});
            }
            try out.appendSlice(alloc, "],\"values\":[");
            for (sparse.values, 0..) |value, i| {
                if (i > 0) try out.append(alloc, ',');
                try out.print(alloc, "{d}", .{value});
            }
            try out.appendSlice(alloc, "],\"k\":");
            try out.print(alloc, "{d}", .{if (sparse.k == 0) default_k else sparse.k});
            try out.appendSlice(alloc, "}}");
        },
        else => return error.UnsupportedQueryRequest,
    }
}

fn appendTextQueryField(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    name: []const u8,
    query: db_mod.types.TextQuery,
) !void {
    try appendJsonFieldName(alloc, out, first, name);
    try appendTextQueryValue(alloc, out, query);
}

fn appendTextQueryValue(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    query: db_mod.types.TextQuery,
) !void {
    switch (query) {
        .match_all => try out.appendSlice(alloc, "{\"match_all\":{}}"),
        .match_none => try out.appendSlice(alloc, "{\"match_none\":{}}"),
        .term => |term| {
            try out.appendSlice(alloc, "{\"term\":");
            try appendJsonString(alloc, out, term.term);
            try out.appendSlice(alloc, ",\"field\":");
            try appendJsonString(alloc, out, term.field);
            try out.append(alloc, '}');
        },
        .match => |match| {
            try out.appendSlice(alloc, "{\"match\":");
            try appendJsonString(alloc, out, match.text);
            try out.appendSlice(alloc, ",\"field\":");
            try appendJsonString(alloc, out, match.field);
            if (match.analyzer) |analyzer| {
                try out.appendSlice(alloc, ",\"analyzer\":");
                try appendJsonString(alloc, out, analyzer);
            }
            try out.append(alloc, '}');
        },
        .multi_match_bool_prefix => |multi_match| {
            try out.appendSlice(alloc, "{\"multi_match\":{\"query\":");
            try appendJsonString(alloc, out, multi_match.query);
            try out.appendSlice(alloc, ",\"type\":\"bool_prefix\",\"fields\":[");
            for (multi_match.fields, 0..) |field, i| {
                if (i > 0) try out.append(alloc, ',');
                if (field.boost == 1.0) {
                    try appendJsonString(alloc, out, field.field);
                } else {
                    const boosted_field = try std.fmt.allocPrint(alloc, "{s}^{d}", .{ field.field, field.boost });
                    defer alloc.free(boosted_field);
                    try appendJsonString(alloc, out, boosted_field);
                }
            }
            try out.append(alloc, ']');
            if (multi_match.boost != 1.0) {
                try out.appendSlice(alloc, ",\"boost\":");
                try out.print(alloc, "{d}", .{multi_match.boost});
            }
            try out.appendSlice(alloc, "}}");
        },
        .match_phrase => |phrase| {
            try out.appendSlice(alloc, "{\"match_phrase\":");
            try appendJsonString(alloc, out, phrase.text);
            try out.appendSlice(alloc, ",\"field\":");
            try appendJsonString(alloc, out, phrase.field);
            if (phrase.analyzer) |analyzer| {
                try out.appendSlice(alloc, ",\"analyzer\":");
                try appendJsonString(alloc, out, analyzer);
            }
            if (phrase.auto_fuzzy) {
                try out.appendSlice(alloc, ",\"fuzziness\":\"auto\"");
            } else if (phrase.max_edits > 0) {
                try out.appendSlice(alloc, ",\"fuzziness\":");
                try out.print(alloc, "{d}", .{phrase.max_edits});
            }
            try out.append(alloc, '}');
        },
        .fuzzy => |fuzzy| {
            try out.appendSlice(alloc, "{\"term\":");
            try appendJsonString(alloc, out, fuzzy.term);
            try out.appendSlice(alloc, ",\"field\":");
            try appendJsonString(alloc, out, fuzzy.field);
            if (fuzzy.prefix_len > 0) {
                try out.appendSlice(alloc, ",\"prefix_length\":");
                try out.print(alloc, "{d}", .{fuzzy.prefix_len});
            }
            if (fuzzy.auto_fuzzy) {
                try out.appendSlice(alloc, ",\"fuzziness\":\"auto\"");
            } else {
                try out.appendSlice(alloc, ",\"fuzziness\":");
                try out.print(alloc, "{d}", .{fuzzy.max_edits});
            }
            try out.append(alloc, '}');
        },
        .prefix => |prefix| {
            try out.appendSlice(alloc, "{\"prefix\":");
            try appendJsonString(alloc, out, prefix.prefix);
            try out.appendSlice(alloc, ",\"field\":");
            try appendJsonString(alloc, out, prefix.field);
            try out.append(alloc, '}');
        },
        .wildcard => |wildcard| {
            try out.appendSlice(alloc, "{\"wildcard\":");
            try appendJsonString(alloc, out, wildcard.pattern);
            try out.appendSlice(alloc, ",\"field\":");
            try appendJsonString(alloc, out, wildcard.field);
            try out.append(alloc, '}');
        },
        .regexp => |regexp| {
            try out.appendSlice(alloc, "{\"regexp\":");
            try appendJsonString(alloc, out, regexp.pattern);
            try out.appendSlice(alloc, ",\"field\":");
            try appendJsonString(alloc, out, regexp.field);
            try out.append(alloc, '}');
        },
        .numeric_range => |range_query| {
            try out.append(alloc, '{');
            var first = true;
            if (range_query.min) |min| {
                try appendJsonFieldName(alloc, out, &first, "min");
                try out.print(alloc, "{d}", .{min});
            }
            if (range_query.max) |max| {
                try appendJsonFieldName(alloc, out, &first, "max");
                try out.print(alloc, "{d}", .{max});
            }
            try appendJsonFieldString(alloc, out, &first, "field", range_query.field);
            if (!range_query.inclusive_min) try appendJsonFieldBool(alloc, out, &first, "inclusive_min", false);
            if (range_query.inclusive_max) try appendJsonFieldBool(alloc, out, &first, "inclusive_max", true);
            try out.append(alloc, '}');
        },
        .date_range => |range_query| {
            try out.append(alloc, '{');
            var first = true;
            if (range_query.start_ns) |start_ns| {
                const text = try formatRfc3339Ns(alloc, start_ns);
                defer alloc.free(text);
                try appendJsonFieldString(alloc, out, &first, "start", text);
            }
            if (range_query.end_ns) |end_ns| {
                const text = try formatRfc3339Ns(alloc, end_ns);
                defer alloc.free(text);
                try appendJsonFieldString(alloc, out, &first, "end", text);
            }
            try appendJsonFieldString(alloc, out, &first, "field", range_query.field);
            if (!range_query.inclusive_start) try appendJsonFieldBool(alloc, out, &first, "inclusive_start", false);
            if (range_query.inclusive_end) try appendJsonFieldBool(alloc, out, &first, "inclusive_end", true);
            try out.append(alloc, '}');
        },
        .term_range => |range_query| {
            try out.append(alloc, '{');
            var first = true;
            if (range_query.min) |min| try appendJsonFieldString(alloc, out, &first, "min", min);
            if (range_query.max) |max| try appendJsonFieldString(alloc, out, &first, "max", max);
            try appendJsonFieldString(alloc, out, &first, "field", range_query.field);
            if (!range_query.inclusive_min) try appendJsonFieldBool(alloc, out, &first, "inclusive_min", false);
            if (range_query.inclusive_max) try appendJsonFieldBool(alloc, out, &first, "inclusive_max", true);
            try out.append(alloc, '}');
        },
        .doc_id => |doc_id| {
            try out.appendSlice(alloc, "{\"ids\":[");
            for (doc_id.ids, 0..) |id, i| {
                if (i > 0) try out.append(alloc, ',');
                try appendJsonString(alloc, out, id);
            }
            try out.appendSlice(alloc, "]}");
        },
        .bool_field => |bool_field| {
            try out.appendSlice(alloc, "{\"bool\":");
            try out.appendSlice(alloc, if (bool_field.value) "true" else "false");
            try out.appendSlice(alloc, ",\"field\":");
            try appendJsonString(alloc, out, bool_field.field);
            try out.append(alloc, '}');
        },
        .bool_query => |bool_query| {
            try out.append(alloc, '{');
            var first = true;
            if (bool_query.must.len > 0) {
                try appendJsonFieldName(alloc, out, &first, "must");
                try out.appendSlice(alloc, "{\"conjuncts\":[");
                for (bool_query.must, 0..) |item, i| {
                    if (i > 0) try out.append(alloc, ',');
                    try appendTextQueryValue(alloc, out, item);
                }
                try out.appendSlice(alloc, "]}");
            }
            if (bool_query.should.len > 0) {
                try appendJsonFieldName(alloc, out, &first, "should");
                try out.appendSlice(alloc, "{\"disjuncts\":[");
                for (bool_query.should, 0..) |item, i| {
                    if (i > 0) try out.append(alloc, ',');
                    try appendTextQueryValue(alloc, out, item);
                }
                try out.append(alloc, ']');
                if (bool_query.min_should > 0) {
                    try out.appendSlice(alloc, ",\"min\":");
                    try out.print(alloc, "{d}", .{bool_query.min_should});
                }
                try out.append(alloc, '}');
            }
            if (bool_query.must_not.len > 0) {
                try appendJsonFieldName(alloc, out, &first, "must_not");
                try out.appendSlice(alloc, "{\"disjuncts\":[");
                for (bool_query.must_not, 0..) |item, i| {
                    if (i > 0) try out.append(alloc, ',');
                    try appendTextQueryValue(alloc, out, item);
                }
                try out.appendSlice(alloc, "]}");
            }
            try out.append(alloc, '}');
        },
        else => return error.UnsupportedQueryRequest,
    }
}

pub fn parseRemoteSearchResult(alloc: std.mem.Allocator, body: []const u8) !db_mod.types.SearchResult {
    var parsed = try std.json.parseFromSlice(metadata_openapi.QueryResponses, alloc, body, .{});
    defer parsed.deinit();
    const responses = parsed.value.responses orelse return error.InvalidQueryRequest;
    if (responses.len == 0) return error.InvalidQueryRequest;
    const response = responses[0];
    const hits_obj = response.hits orelse return error.InvalidQueryRequest;
    const hits_value = hits_obj.hits orelse return error.InvalidQueryRequest;

    const hits = try alloc.alloc(db_mod.types.SearchHit, hits_value.len);
    var initialized: usize = 0;
    errdefer {
        for (hits[0..initialized]) |*hit| hit.deinit(alloc);
        alloc.free(hits);
    }
    for (hits_value, 0..) |item, i| {
        hits[i] = .{
            .id = try alloc.dupe(u8, item._id),
            .score = item._score,
            .stored_data = if (item._source) |value| try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})}) else null,
        };
        initialized += 1;
    }

    const graph_results: []db_mod.types.GraphSearchResult = if (response.graph_results) |graph_results_value|
        try parseRemoteGraphResults(alloc, graph_results_value)
    else
        @constCast((&[_]db_mod.types.GraphSearchResult{})[0..]);
    errdefer {
        for (graph_results) |*graph_result| graph_result.deinit(alloc);
        if (graph_results.len > 0) alloc.free(graph_results);
    }
    const graph_metric_results: []db_mod.types.GraphMetricResult = if (response.graph_metric_results) |graph_metric_results_value|
        try parseRemoteGraphMetricResults(alloc, graph_metric_results_value)
    else
        @constCast((&[_]db_mod.types.GraphMetricResult{})[0..]);
    errdefer {
        for (graph_metric_results) |*metric_result| metric_result.deinit(alloc);
        if (graph_metric_results.len > 0) alloc.free(graph_metric_results);
    }

    return .{
        .alloc = alloc,
        .hits = hits,
        .total_hits = @intCast(hits_obj.total orelse 0),
        .graph_results = graph_results,
        .graph_metric_results = graph_metric_results,
    };
}

fn parseRemoteGraphMetricResults(
    alloc: std.mem.Allocator,
    value: std.json.ArrayHashMap(indexes_openapi.GraphMetricResult),
) ![]db_mod.types.GraphMetricResult {
    const results = try alloc.alloc(db_mod.types.GraphMetricResult, value.map.count());
    var initialized: usize = 0;
    errdefer {
        for (results[0..initialized]) |*metric_result| metric_result.deinit(alloc);
        alloc.free(results);
    }

    var it = value.map.iterator();
    while (it.next()) |entry| {
        const result_value = entry.value_ptr.*;
        const scores = try alloc.alloc(db_mod.types.GraphMetricScore, result_value.scores.len);
        var initialized_scores: usize = 0;
        errdefer {
            for (scores[0..initialized_scores]) |*score| score.deinit(alloc);
            alloc.free(scores);
        }
        for (result_value.scores, 0..) |score, i| {
            scores[i] = .{
                .node = try alloc.dupe(u8, score.node),
                .score = score.score,
            };
            initialized_scores += 1;
        }

        results[initialized] = .{
            .name = try alloc.dupe(u8, entry.key_ptr.*),
            .index_name = try alloc.dupe(u8, result_value.index_name),
            .metric_name = try alloc.dupe(u8, result_value.metric),
            .scores = scores,
            .status = try parseRemoteGraphMetricStatusValue(alloc, result_value.metric, result_value.status),
        };
        initialized += 1;
    }

    return results;
}

fn parseRemoteGraphResults(
    alloc: std.mem.Allocator,
    value: std.json.ArrayHashMap(indexes_openapi.GraphQueryResult),
) ![]db_mod.types.GraphSearchResult {
    const results = try alloc.alloc(db_mod.types.GraphSearchResult, value.map.count());
    var initialized: usize = 0;
    errdefer {
        for (results[0..initialized]) |*graph_result| graph_result.deinit(alloc);
        alloc.free(results);
    }

    var it = value.map.iterator();
    while (it.next()) |entry| {
        const result_value = entry.value_ptr.*;
        const parsed_nodes = if (result_value.nodes) |nodes_value|
            try parseRemoteGraphNodes(alloc, nodes_value)
        else
            ParsedRemoteGraphNodes{};
        errdefer parsed_nodes.deinit(alloc);
        const parsed_matches = if (result_value.matches) |matches_value|
            try parseRemoteGraphMatches(alloc, matches_value)
        else
            ParsedRemoteGraphMatches{};
        errdefer parsed_matches.deinit(alloc);
        const paths: []graph_paths.Path = if (result_value.paths) |paths_value|
            try parseRemoteGraphPaths(alloc, paths_value)
        else
            @constCast((&[_]graph_paths.Path{})[0..]);
        errdefer {
            for (paths) |path| graph_paths.freePath(alloc, path);
            if (paths.len > 0) alloc.free(paths);
        }

        results[initialized] = .{
            .name = try alloc.dupe(u8, entry.key_ptr.*),
            .nodes = parsed_nodes.nodes,
            .paths = paths,
            .matches = parsed_matches.matches,
            .hits = try concatGraphResultHits(alloc, parsed_nodes.hits, parsed_matches.hits),
            .total_hits = @intCast(result_value.total),
            .metric_status = try parseRemoteGraphMetricStatusMap(alloc, result_value.metric_status),
        };
        initialized += 1;
    }

    return results;
}

const ParsedRemoteGraphNodes = struct {
    nodes: []graph_query_mod.GraphResultNode = &.{},
    hits: []db_mod.types.SearchHit = &.{},

    fn deinit(self: ParsedRemoteGraphNodes, alloc: std.mem.Allocator) void {
        for (self.nodes) |*node| node.deinit(alloc);
        if (self.nodes.len > 0) alloc.free(self.nodes);
        for (self.hits) |*hit| hit.deinit(alloc);
        if (self.hits.len > 0) alloc.free(self.hits);
    }
};

const ParsedRemoteGraphMatches = struct {
    matches: []db_mod.types.GraphPatternMatch = &.{},
    hits: []db_mod.types.SearchHit = &.{},

    fn deinit(self: ParsedRemoteGraphMatches, alloc: std.mem.Allocator) void {
        for (self.matches) |*match| match.deinit(alloc);
        if (self.matches.len > 0) alloc.free(self.matches);
        for (self.hits) |*hit| hit.deinit(alloc);
        if (self.hits.len > 0) alloc.free(self.hits);
    }
};

fn parseRemoteGraphNodes(
    alloc: std.mem.Allocator,
    value: []const indexes_openapi.GraphResultNode,
) !ParsedRemoteGraphNodes {
    const nodes = try alloc.alloc(graph_query_mod.GraphResultNode, value.len);
    var initialized: usize = 0;
    errdefer {
        for (nodes[0..initialized]) |*node| node.deinit(alloc);
        alloc.free(nodes);
    }
    var hits = std.ArrayListUnmanaged(db_mod.types.SearchHit).empty;
    errdefer {
        for (hits.items) |*hit| hit.deinit(alloc);
        hits.deinit(alloc);
    }

    for (value, 0..) |item, i| {
        nodes[i] = .{
            .key = try alloc.dupe(u8, item.key),
            .depth = @intCast(item.depth orelse 0),
            .distance = item.distance orelse 0,
            .path = if (item.path) |path| try cloneRemoteGraphNodePath(alloc, path) else null,
            .path_edges = if (item.path_edges) |path_edges| try cloneRemoteGraphNodePathEdges(alloc, path_edges) else null,
            .provenance = if (item.provenance) |provenance| try cloneRemoteGraphNodePath(alloc, provenance) else null,
            .metrics = try parseRemoteGraphNodeMetrics(alloc, item.metrics),
        };
        if (item.document) |document| {
            try hits.append(alloc, .{
                .id = try alloc.dupe(u8, item.key),
                .score = null,
                .stored_data = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(document, .{})}),
            });
        }
        initialized += 1;
    }
    return .{
        .nodes = nodes,
        .hits = try hits.toOwnedSlice(alloc),
    };
}

fn parseRemoteGraphNodeWithKey(
    alloc: std.mem.Allocator,
    key: []const u8,
    item: indexes_openapi.GraphResultNode,
) !graph_query_mod.GraphResultNode {
    return .{
        .key = try alloc.dupe(u8, key),
        .depth = @intCast(item.depth orelse 0),
        .distance = item.distance orelse 0,
        .path = if (item.path) |path| try cloneRemoteGraphNodePath(alloc, path) else null,
        .path_edges = if (item.path_edges) |path_edges| try cloneRemoteGraphNodePathEdges(alloc, path_edges) else null,
        .provenance = if (item.provenance) |provenance| try cloneRemoteGraphNodePath(alloc, provenance) else null,
        .metrics = try parseRemoteGraphNodeMetrics(alloc, item.metrics),
    };
}

fn parseRemoteGraphNodeMetrics(
    alloc: std.mem.Allocator,
    value: ?std.json.Value,
) ![]graph_query_mod.GraphMetricValue {
    const metrics_value = value orelse return &.{};
    if (metrics_value != .object) return error.InvalidQueryResponse;
    const metrics = metrics_value.object;
    const out = try alloc.alloc(graph_query_mod.GraphMetricValue, metrics.count());
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*metric| metric.deinit(alloc);
        if (out.len > 0) alloc.free(out);
    }
    var it = metrics.iterator();
    while (it.next()) |entry| {
        const score: ?f64 = switch (entry.value_ptr.*) {
            .null => null,
            .float => |score| score,
            .integer => |score| @floatFromInt(score),
            else => return error.InvalidQueryResponse,
        };
        out[initialized] = .{
            .name = try alloc.dupe(u8, entry.key_ptr.*),
            .score = score,
        };
        initialized += 1;
    }
    return out;
}

fn parseRemoteGraphMetricStatusMap(
    alloc: std.mem.Allocator,
    value: ?std.json.ArrayHashMap(indexes_openapi.GraphMetricStatus),
) ![]db_mod.types.GraphMetricStatus {
    const statuses = value orelse return &.{};
    const out = try alloc.alloc(db_mod.types.GraphMetricStatus, statuses.map.count());
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*status| status.deinit(alloc);
        if (out.len > 0) alloc.free(out);
    }
    var it = statuses.map.iterator();
    while (it.next()) |entry| {
        const status = entry.value_ptr.*;
        const name = try alloc.dupe(u8, entry.key_ptr.*);
        var name_moved = false;
        errdefer if (!name_moved) alloc.free(name);
        var edge_filter = try parseRemoteGraphMetricEdgeFilterStatus(alloc, status.edge_filter);
        var edge_filter_moved = false;
        errdefer if (!edge_filter_moved) edge_filter.deinit(alloc);
        out[initialized] = try parseRemoteGraphMetricStatusValueWithOwnedName(alloc, name, status, edge_filter);
        name_moved = true;
        edge_filter_moved = true;
        initialized += 1;
    }
    return out;
}

fn parseRemoteGraphMetricStatusValue(
    alloc: std.mem.Allocator,
    metric_name: []const u8,
    status: indexes_openapi.GraphMetricStatus,
) !db_mod.types.GraphMetricStatus {
    const name = try alloc.dupe(u8, metric_name);
    var name_moved = false;
    errdefer if (!name_moved) alloc.free(name);
    var edge_filter = try parseRemoteGraphMetricEdgeFilterStatus(alloc, status.edge_filter);
    var edge_filter_moved = false;
    errdefer if (!edge_filter_moved) edge_filter.deinit(alloc);
    const out = try parseRemoteGraphMetricStatusValueWithOwnedName(alloc, name, status, edge_filter);
    name_moved = true;
    edge_filter_moved = true;
    return out;
}

fn parseRemoteGraphMetricStatusValueWithOwnedName(
    alloc: std.mem.Allocator,
    owned_name: []u8,
    status: indexes_openapi.GraphMetricStatus,
    owned_edge_filter: graph_mod.GraphMetricEdgeFilter,
) !db_mod.types.GraphMetricStatus {
    const last_error = if (status.last_error) |last_error|
        try alloc.dupe(u8, last_error)
    else
        "";
    var last_error_moved = false;
    errdefer if (!last_error_moved and last_error.len > 0) alloc.free(last_error);
    const build_worker_id = if (status.build_worker_id) |worker_id|
        try alloc.dupe(u8, worker_id)
    else
        "";
    var build_worker_id_moved = false;
    errdefer if (!build_worker_id_moved and build_worker_id.len > 0) alloc.free(build_worker_id);
    const out = db_mod.types.GraphMetricStatus{
        .name = owned_name,
        .state = graphMetricStateFromName(status.state) orelse return error.InvalidQueryRequest,
        .phase = graphMetricPhaseFromName(status.phase) orelse return error.InvalidQueryRequest,
        .edge_filter = owned_edge_filter,
        .metadata_version = @intCast(@max(status.metadata_version orelse 0, 0)),
        .maintenance_paused = status.maintenance_paused orelse false,
        .build_queued = status.build_queued,
        .published_generation = @intCast(@max(status.published_generation, 0)),
        .edge_generation = @intCast(@max(status.edge_generation, 0)),
        .target_edge_generation = @intCast(@max(status.target_edge_generation, 0)),
        .queued_generation = @intCast(@max(status.queued_generation orelse 0, 0)),
        .building_generation = @intCast(@max(status.building_generation orelse 0, 0)),
        .build_job_id = @intCast(@max(status.build_job_id orelse 0, 0)),
        .build_started_at_ms = @intCast(@max(status.build_started_at_ms orelse 0, 0)),
        .build_iteration = @intCast(@max(status.build_iteration orelse 0, 0)),
        .build_lease_expires_at_ms = @intCast(@max(status.build_lease_expires_at_ms orelse 0, 0)),
        .build_worker_id = build_worker_id,
        .retry_count = @intCast(@max(status.retry_count orelse 0, 0)),
        .last_error = last_error,
        .progress = status.progress,
        .converged = status.converged,
        .iterations_completed = @intCast(@max(status.iterations_completed, 0)),
        .delta = status.delta,
        .computed_at_ms = @intCast(@max(status.computed_at_ms, 0)),
        .last_event = try parseRemoteGraphMetricEvent(status.last_event),
        .recent_events = try parseRemoteGraphMetricEvents(alloc, status.recent_events),
    };
    last_error_moved = true;
    build_worker_id_moved = true;
    return out;
}

fn parseRemoteGraphMetricEdgeFilterStatus(
    alloc: std.mem.Allocator,
    maybe_filter: ?indexes_openapi.GraphMetricEdgeFilterStatus,
) !graph_mod.GraphMetricEdgeFilter {
    const filter = maybe_filter orelse return .{};
    if (std.mem.eql(u8, filter.mode, "all")) return .{};
    if (!std.mem.eql(u8, filter.mode, "types")) return error.InvalidQueryRequest;
    const raw_types = filter.types orelse return error.InvalidQueryRequest;
    if (raw_types.len == 0) return error.InvalidQueryRequest;
    const types = try alloc.alloc([]const u8, raw_types.len);
    var initialized: usize = 0;
    errdefer {
        for (types[0..initialized]) |edge_type| alloc.free(edge_type);
        alloc.free(types);
    }
    for (raw_types, 0..) |edge_type, i| {
        if (edge_type.len == 0) return error.InvalidQueryRequest;
        types[i] = try alloc.dupe(u8, edge_type);
        initialized += 1;
    }
    return .{ .mode = .types, .types = types };
}

fn parseRemoteGraphMetricEvent(
    maybe_event: ?indexes_openapi.GraphMetricEvent,
) !?graph_mod.GraphIndex.GraphMetricEvent {
    const event = maybe_event orelse return null;
    return try parseRemoteGraphMetricEventValue(event);
}

fn parseRemoteGraphMetricEventValue(
    event: indexes_openapi.GraphMetricEvent,
) !graph_mod.GraphIndex.GraphMetricEvent {
    return .{
        .sequence = @intCast(@max(event.sequence, 0)),
        .kind = graphMetricEventKindFromName(event.kind) orelse return error.InvalidQueryRequest,
        .at_ms = @intCast(@max(event.at_ms, 0)),
        .target_edge_generation = @intCast(@max(event.target_edge_generation, 0)),
        .published_generation = @intCast(@max(event.published_generation, 0)),
        .score_count = @intCast(@max(event.score_count, 0)),
    };
}

fn parseRemoteGraphMetricEvents(
    alloc: std.mem.Allocator,
    maybe_events: ?[]const indexes_openapi.GraphMetricEvent,
) ![]graph_mod.GraphIndex.GraphMetricEvent {
    const events = maybe_events orelse return &.{};
    const out = try alloc.alloc(graph_mod.GraphIndex.GraphMetricEvent, events.len);
    for (events, 0..) |event, i| {
        out[i] = try parseRemoteGraphMetricEventValue(event);
    }
    return out;
}

fn graphMetricEventKindFromName(name: []const u8) ?graph_mod.GraphIndex.GraphMetricEventKind {
    if (std.mem.eql(u8, name, "publish")) return .publish;
    if (std.mem.eql(u8, name, "delete")) return .delete;
    if (std.mem.eql(u8, name, "pause")) return .pause;
    if (std.mem.eql(u8, name, "resume")) return .@"resume";
    if (std.mem.eql(u8, name, "failed")) return .failed;
    return null;
}

fn graphMetricStateFromName(name: []const u8) ?graph_mod.GraphIndex.GraphMetricState {
    if (std.mem.eql(u8, name, "disabled")) return .disabled;
    if (std.mem.eql(u8, name, "not_ready")) return .not_ready;
    if (std.mem.eql(u8, name, "fresh")) return .fresh;
    if (std.mem.eql(u8, name, "stale")) return .stale;
    if (std.mem.eql(u8, name, "building")) return .building;
    if (std.mem.eql(u8, name, "failed")) return .failed;
    return null;
}

fn graphMetricPhaseFromName(name: []const u8) ?graph_mod.GraphIndex.GraphMetricBuildPhase {
    if (std.mem.eql(u8, name, "idle")) return .idle;
    if (std.mem.eql(u8, name, "computing")) return .computing;
    if (std.mem.eql(u8, name, "publishing")) return .publishing;
    if (std.mem.eql(u8, name, "complete")) return .complete;
    if (std.mem.eql(u8, name, "prepare_generation")) return .prepare_generation;
    if (std.mem.eql(u8, name, "scan_edges_and_out_degree")) return .scan_edges_and_out_degree;
    if (std.mem.eql(u8, name, "initialize_ranks")) return .initialize_ranks;
    if (std.mem.eql(u8, name, "iterate_contributions")) return .iterate_contributions;
    if (std.mem.eql(u8, name, "reduce_ranks")) return .reduce_ranks;
    if (std.mem.eql(u8, name, "check_convergence")) return .check_convergence;
    if (std.mem.eql(u8, name, "publish_generation")) return .publish_generation;
    if (std.mem.eql(u8, name, "cleanup_old_generations")) return .cleanup_old_generations;
    return null;
}

fn parseRemoteGraphMatches(
    alloc: std.mem.Allocator,
    value: []const indexes_openapi.PatternMatch,
) !ParsedRemoteGraphMatches {
    const matches = try alloc.alloc(db_mod.types.GraphPatternMatch, value.len);
    var initialized_matches: usize = 0;
    errdefer {
        for (matches[0..initialized_matches]) |*match| match.deinit(alloc);
        alloc.free(matches);
    }
    var hits = std.ArrayListUnmanaged(db_mod.types.SearchHit).empty;
    errdefer {
        for (hits.items) |*hit| hit.deinit(alloc);
        hits.deinit(alloc);
    }

    for (value, 0..) |item, i| {
        const bindings_value = item.bindings orelse return error.InvalidQueryRequest;

        const bindings = try alloc.alloc(db_mod.types.GraphPatternBinding, bindings_value.map.count());
        var initialized_bindings: usize = 0;
        errdefer {
            for (bindings[0..initialized_bindings]) |*binding| binding.deinit(alloc);
            if (bindings.len > 0) alloc.free(bindings);
        }

        var binding_it = bindings_value.map.iterator();
        while (binding_it.next()) |binding_entry| {
            const node_value = binding_entry.value_ptr.*;
            const node = try parseRemoteGraphNodeWithKey(alloc, node_value.key, node_value);
            bindings[initialized_bindings] = .{
                .alias = try alloc.dupe(u8, binding_entry.key_ptr.*),
                .node = node,
            };
            if (node_value.document) |document| {
                try hits.append(alloc, .{
                    .id = try alloc.dupe(u8, node_value.key),
                    .score = null,
                    .stored_data = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(document, .{})}),
                });
            }
            initialized_bindings += 1;
        }

        matches[i] = .{
            .bindings = bindings,
            .path = if (item.path) |path_value| try cloneRemoteGraphNodePathEdges(alloc, path_value) else @constCast((&[_]graph_query_mod.PathEdgeInfo{})[0..]),
        };
        initialized_matches += 1;
    }

    return .{
        .matches = matches,
        .hits = try hits.toOwnedSlice(alloc),
    };
}

fn concatGraphResultHits(
    alloc: std.mem.Allocator,
    left: []db_mod.types.SearchHit,
    right: []db_mod.types.SearchHit,
) ![]db_mod.types.SearchHit {
    const out = try alloc.alloc(db_mod.types.SearchHit, left.len + right.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*hit| hit.deinit(alloc);
        if (out.len > 0) alloc.free(out);
    }
    for (left) |hit| {
        out[initialized] = try hit.clone(alloc);
        initialized += 1;
    }
    for (right) |hit| {
        out[initialized] = try hit.clone(alloc);
        initialized += 1;
    }
    return out;
}

fn cloneRemoteGraphNodePath(alloc: std.mem.Allocator, value: []const []const u8) ![][]const u8 {
    const out = try alloc.alloc([]const u8, value.len);
    errdefer alloc.free(out);
    for (value, 0..) |item, i| {
        out[i] = try alloc.dupe(u8, item);
    }
    return out;
}

fn cloneRemoteGraphNodePathEdges(
    alloc: std.mem.Allocator,
    value: []const indexes_openapi.PathEdge,
) ![]graph_query_mod.PathEdgeInfo {
    const edges = try alloc.alloc(graph_query_mod.PathEdgeInfo, value.len);
    errdefer alloc.free(edges);
    for (value, 0..) |item, i| {
        edges[i] = .{
            .source = try alloc.dupe(u8, item.source orelse return error.InvalidQueryRequest),
            .target = try alloc.dupe(u8, item.target orelse return error.InvalidQueryRequest),
            .edge_type = try alloc.dupe(u8, item.type orelse return error.InvalidQueryRequest),
            .weight = item.weight orelse return error.InvalidQueryRequest,
        };
    }
    return edges;
}

fn parseRemoteGraphPaths(alloc: std.mem.Allocator, value: []const indexes_openapi.Path) ![]graph_paths.Path {
    const paths = try alloc.alloc(graph_paths.Path, value.len);
    var initialized: usize = 0;
    errdefer {
        for (paths[0..initialized]) |path| graph_paths.freePath(alloc, path);
        alloc.free(paths);
    }
    for (value, 0..) |item, i| {
        paths[i] = .{
            .nodes = try cloneRemoteGraphNodePath(alloc, item.nodes orelse return error.InvalidQueryRequest),
            .edges = try parseRemotePathEdges(alloc, item.edges orelse return error.InvalidQueryRequest),
            .total_weight = item.total_weight orelse return error.InvalidQueryRequest,
            .length = @intCast(item.length orelse return error.InvalidQueryRequest),
        };
        initialized += 1;
    }
    return paths;
}

fn parseRemotePathEdges(alloc: std.mem.Allocator, value: []const indexes_openapi.PathEdge) ![]graph_paths.PathEdge {
    const edges = try alloc.alloc(graph_paths.PathEdge, value.len);
    errdefer alloc.free(edges);
    for (value, 0..) |item, i| {
        edges[i] = .{
            .source = try alloc.dupe(u8, item.source orelse return error.InvalidQueryRequest),
            .target = try alloc.dupe(u8, item.target orelse return error.InvalidQueryRequest),
            .edge_type = try alloc.dupe(u8, item.type orelse return error.InvalidQueryRequest),
            .weight = item.weight orelse return error.InvalidQueryRequest,
        };
    }
    return edges;
}

pub fn appendJsonFieldName(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    name: []const u8,
) !void {
    if (!first.*) try out.append(alloc, ',');
    first.* = false;
    try appendJsonString(alloc, out, name);
    try out.append(alloc, ':');
}

pub fn appendJsonFieldString(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    name: []const u8,
    value: []const u8,
) !void {
    try appendJsonFieldName(alloc, out, first, name);
    try appendJsonString(alloc, out, value);
}

pub fn appendJsonFieldU32(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    name: []const u8,
    value: u32,
) !void {
    try appendJsonFieldName(alloc, out, first, name);
    try out.print(alloc, "{d}", .{value});
}

pub fn appendJsonFieldF32(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    name: []const u8,
    value: f32,
) !void {
    try appendJsonFieldName(alloc, out, first, name);
    try out.print(alloc, "{d}", .{value});
}

pub fn appendJsonFieldF64(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    name: []const u8,
    value: f64,
) !void {
    try appendJsonFieldName(alloc, out, first, name);
    try out.print(alloc, "{d}", .{value});
}

pub fn appendJsonFieldBool(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    name: []const u8,
    value: bool,
) !void {
    try appendJsonFieldName(alloc, out, first, name);
    try out.appendSlice(alloc, if (value) "true" else "false");
}

pub fn appendJsonFieldNames(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    name: []const u8,
    fields: []const []const u8,
) !void {
    try appendJsonFieldName(alloc, out, first, name);
    try out.append(alloc, '[');
    for (fields, 0..) |field, i| {
        if (i > 0) try out.append(alloc, ',');
        try appendJsonString(alloc, out, field);
    }
    try out.append(alloc, ']');
}

pub fn appendJsonString(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), value: []const u8) !void {
    const escaped = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})});
    defer alloc.free(escaped);
    try out.appendSlice(alloc, escaped);
}

const CivilDate = struct {
    year: i64,
    month: i64,
    day: i64,
};

fn formatRfc3339Ns(alloc: std.mem.Allocator, value_ns: u64) ![]u8 {
    const secs_total: u64 = @divFloor(value_ns, std.time.ns_per_s);
    const nanos: u64 = @mod(value_ns, std.time.ns_per_s);
    const days: i64 = @intCast(@divFloor(secs_total, 86_400));
    const secs_of_day: u64 = @mod(secs_total, 86_400);
    const date = civilFromDays(days);
    const year: u64 = @intCast(date.year);
    const month: u64 = @intCast(date.month);
    const day: u64 = @intCast(date.day);
    const hour: u64 = secs_of_day / 3_600;
    const minute: u64 = (secs_of_day % 3_600) / 60;
    const second: u64 = secs_of_day % 60;
    if (nanos == 0) {
        return try std.fmt.allocPrint(alloc, "{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}Z", .{
            year, month, day, hour, minute, second,
        });
    }
    return try std.fmt.allocPrint(alloc, "{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}.{d:0>9}Z", .{
        year, month, day, hour, minute, second, nanos,
    });
}

fn civilFromDays(days_since_epoch: i64) CivilDate {
    const z = days_since_epoch + 719_468;
    const era = @divFloor(if (z >= 0) z else z - 146_096, 146_097);
    const doe = z - era * 146_097;
    const yoe = @divFloor(doe - @divFloor(doe, 1_460) + @divFloor(doe, 36_524) - @divFloor(doe, 146_096), 365);
    const y = yoe + era * 400;
    const doy = doe - (365 * yoe + @divFloor(yoe, 4) - @divFloor(yoe, 100));
    const mp = @divFloor(5 * doy + 2, 153);
    const day = doy - @divFloor(153 * mp + 2, 5) + 1;
    const month = mp + (if (mp < 10) @as(i64, 3) else @as(i64, -9));
    const year = y + (if (month <= 2) @as(i64, 1) else @as(i64, 0));
    return .{ .year = year, .month = month, .day = day };
}

fn parseJsonTestBody(comptime T: type, alloc: std.mem.Allocator, body: []const u8) !std.json.Parsed(T) {
    return try std.json.parseFromSlice(T, alloc, body, .{});
}

test "remote query parser preserves graph metric results" {
    const alloc = std.testing.allocator;
    var parsed = try parseRemoteSearchResult(alloc,
        \\{"responses":[{"hits":{"total":0,"hits":[]},"graph_metric_results":{"central":{"index_name":"graph_idx","metric":"pagerank","scores":[{"node":"doc:b","score":0.8},{"node":"doc:a","score":0.9}],"status":{"state":"fresh","phase":"complete","maintenance_paused":false,"build_queued":false,"published_generation":7,"edge_generation":7,"target_edge_generation":7,"queued_generation":0,"building_generation":0,"build_job_id":12345,"build_started_at_ms":1780000000123,"build_lease_expires_at_ms":0,"progress":1.0,"converged":true,"iterations_completed":12,"delta":0.0,"computed_at_ms":1780000000000}}}}]}
    );
    defer parsed.deinit();

    try std.testing.expectEqual(@as(usize, 1), parsed.graph_metric_results.len);
    const result = parsed.graph_metric_results[0];
    try std.testing.expectEqualStrings("central", result.name);
    try std.testing.expectEqualStrings("graph_idx", result.index_name);
    try std.testing.expectEqualStrings("pagerank", result.metric_name);
    try std.testing.expectEqual(@as(usize, 2), result.scores.len);
    try std.testing.expectEqualStrings("doc:b", result.scores[0].node);
    try std.testing.expectApproxEqAbs(@as(f64, 0.8), result.scores[0].score, 0.001);
    try std.testing.expectEqual(graph_mod.GraphIndex.GraphMetricState.fresh, result.status.state);
    try std.testing.expectEqual(@as(u32, 0), result.status.metadata_version);
    try std.testing.expectEqual(@as(u64, 7), result.status.published_generation);
    try std.testing.expectEqual(@as(u64, 12345), result.status.build_job_id);
    try std.testing.expectEqual(@as(u64, 1780000000123), result.status.build_started_at_ms);
}

test "encode query request round-trips composed bleve full_text queries" {
    const alloc = std.testing.allocator;

    const encoded = try encodeQueryRequest(alloc, .{
        .full_text = .{
            .bool_query = .{
                .must = &.{
                    .{ .match = .{ .field = "body", .text = "hello" } },
                    .{ .numeric_range = .{
                        .field = "score",
                        .min = 10,
                        .max = 20,
                        .inclusive_max = true,
                    } },
                },
                .must_not = &.{
                    .{ .date_range = .{
                        .field = "created_at",
                        .start_ns = 1_772_323_200 * std.time.ns_per_s,
                        .inclusive_end = true,
                    } },
                },
            },
        },
        .limit = 5,
        .identity_read_generation = 77,
    });
    defer alloc.free(encoded);

    var parsed = try parseJsonTestBody(std.json.Value, alloc, encoded);
    defer parsed.deinit();
    const full_text = parsed.value.object.get("full_text_search").?.object;
    try std.testing.expectEqual(@as(i64, 77), parsed.value.object.get("_identity_read_generation").?.integer);
    const must = full_text.get("must").?.object.get("conjuncts").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), must.len);
    try std.testing.expectEqual(true, must[1].object.get("inclusive_max").?.bool);
    try std.testing.expectEqualStrings("2026-03-01T00:00:00Z", full_text.get("must_not").?.object.get("disjuncts").?.array.items[0].object.get("start").?.string);
    try std.testing.expect(full_text.get("fuzziness") == null);

    const fuzzy = try encodeQueryRequest(alloc, .{
        .full_text = .{
            .fuzzy = .{
                .field = "body",
                .term = "helo",
                .max_edits = 1,
            },
        },
    });
    defer alloc.free(fuzzy);
    var parsed_fuzzy = try parseJsonTestBody(std.json.Value, alloc, fuzzy);
    defer parsed_fuzzy.deinit();
    try std.testing.expectEqual(@as(i64, 1), parsed_fuzzy.value.object.get("full_text_search").?.object.get("fuzziness").?.integer);
}

test "encode query request includes named vector embeddings for routed semantic search" {
    const alloc = std.testing.allocator;

    const encoded = try encodeQueryRequest(alloc, .{
        .dense_queries = &.{
            .{
                .name = "semantic_idx",
                .index_name = "semantic_idx",
                .query = .{
                    .vector = &.{ 0.25, 0.5, 0.75 },
                    .k = 4,
                },
            },
        },
        .sparse_queries = &.{
            .{
                .name = "sparse_idx",
                .index_name = "sparse_idx",
                .query = .{
                    .indices = &.{ 1, 7 },
                    .values = &.{ 0.4, 0.9 },
                    .k = 4,
                },
            },
        },
        .limit = 4,
    });
    defer alloc.free(encoded);

    var parsed = try parseJsonTestBody(std.json.Value, alloc, encoded);
    defer parsed.deinit();
    const embeddings = parsed.value.object.get("embeddings").?.object;
    const dense = embeddings.get("semantic_idx").?.array.items;
    try std.testing.expectEqual(@as(usize, 3), dense.len);
    try std.testing.expectEqual(@as(f64, 0.25), dense[0].float);
    try std.testing.expectEqual(@as(f64, 0.75), dense[2].float);
    const sparse = embeddings.get("sparse_idx").?.object;
    try std.testing.expectEqual(@as(i64, 1), sparse.get("indices").?.array.items[0].integer);
    try std.testing.expectEqual(@as(i64, 7), sparse.get("indices").?.array.items[1].integer);
    try std.testing.expectEqual(@as(f64, 0.4), sparse.get("values").?.array.items[0].float);
    try std.testing.expectEqual(@as(f64, 0.9), sparse.get("values").?.array.items[1].float);
}

test "encode query request includes graph metric read and rerank" {
    const alloc = std.testing.allocator;

    const graph_metric_queries = [_]db_mod.types.NamedGraphMetricQuery{.{
        .name = "pagerank",
        .query = .{
            .index_name = "graph_idx",
            .metric_name = "pagerank",
            .top_k = 25,
            .freshness = .fresh,
        },
    }};
    const encoded = try encodeQueryRequest(alloc, .{
        .full_text = .{ .match_all = {} },
        .graph_metric_queries = &graph_metric_queries,
        .graph_metric_rerank = .{
            .index_name = "graph_idx",
            .metric_name = "pagerank",
            .freshness = .fresh,
            .base_weight = 0.5,
            .weight = 2.5,
            .missing_score = -0.25,
        },
        .limit = 25,
    });
    defer alloc.free(encoded);

    var parsed = try parseJsonTestBody(std.json.Value, alloc, encoded);
    defer parsed.deinit();
    const graph_metric = parsed.value.object.get("graph_metric").?.object;
    try std.testing.expectEqualStrings("pagerank", graph_metric.get("name").?.string);
    try std.testing.expectEqualStrings("graph_idx", graph_metric.get("index").?.string);
    try std.testing.expectEqualStrings("pagerank", graph_metric.get("metric").?.string);
    try std.testing.expectEqual(@as(i64, 25), graph_metric.get("top_k").?.integer);
    try std.testing.expectEqualStrings("fresh", graph_metric.get("metric_freshness").?.string);
    const rerank = parsed.value.object.get("graph_metric_rerank").?.object;
    try std.testing.expectEqualStrings("graph_idx", rerank.get("index").?.string);
    try std.testing.expectEqualStrings("pagerank", rerank.get("metric").?.string);
    try std.testing.expectEqual(@as(f64, 0.5), rerank.get("base_weight").?.float);
    try std.testing.expectEqual(@as(f64, 2.5), rerank.get("weight").?.float);
    try std.testing.expectEqual(@as(f64, -0.25), rerank.get("missing_score").?.float);
    try std.testing.expectEqualStrings("fresh", rerank.get("metric_freshness").?.string);
}

test "encode query request includes merge config and pruner but omits reranker" {
    const alloc = std.testing.allocator;

    const encoded = try encodeQueryRequest(alloc, .{
        .query = .{ .match = .{ .field = "body", .text = "hello" } },
        .merge_config = .{
            .strategy = .rsf,
            .window_size = 25,
            .rank_constant = 42.0,
            .weights = &.{
                .{ .name = "full_text", .weight = 0.5 },
                .{ .name = "semantic_idx", .weight = 1.5 },
            },
        },
        .pruner = .{
            .min_score_ratio = 0.5,
            .require_multi_index = true,
        },
        .reranker = .{
            .provider = .antfly,
            .model = "cross-encoder/ms-marco-MiniLM-L-6-v2",
            .field = "body",
        },
        .reranker_query_text = "hello",
    });
    defer alloc.free(encoded);

    var parsed = try parseJsonTestBody(std.json.Value, alloc, encoded);
    defer parsed.deinit();
    const merge_config = parsed.value.object.get("merge_config").?.object;
    try std.testing.expectEqualStrings("rsf", merge_config.get("strategy").?.string);
    try std.testing.expectEqual(@as(f64, 0.5), merge_config.get("weights").?.object.get("full_text").?.float);
    try std.testing.expectEqual(@as(f64, 1.5), merge_config.get("weights").?.object.get("semantic_idx").?.float);
    const pruner = parsed.value.object.get("pruner").?.object;
    try std.testing.expectEqual(@as(f64, 0.5), pruner.get("min_score_ratio").?.float);
    try std.testing.expectEqual(true, pruner.get("require_multi_index").?.bool);
    try std.testing.expect(parsed.value.object.get("reranker") == null);
}

test "encode query request includes distributed text stats for internal shard scoring" {
    const alloc = std.testing.allocator;

    const encoded = try encodeQueryRequest(alloc, .{
        .query = .{ .match = .{ .field = "body", .text = "hello world" } },
        .distributed_text_stats = &.{.{
            .field = "body",
            .global_doc_count = 9,
            .global_total_field_len = 45,
            .term_doc_freqs = &.{
                .{ .term = "hello", .doc_freq = 4 },
                .{ .term = "world", .doc_freq = 2 },
            },
        }},
    });
    defer alloc.free(encoded);

    var parsed = try parseJsonTestBody(std.json.Value, alloc, encoded);
    defer parsed.deinit();
    const stats = parsed.value.object.get("_distributed_text_stats").?.array.items;
    try std.testing.expectEqual(@as(usize, 1), stats.len);
    try std.testing.expectEqualStrings("body", stats[0].object.get("field").?.string);
    try std.testing.expectEqual(@as(i64, 9), stats[0].object.get("global_doc_count").?.integer);
    try std.testing.expectEqual(@as(i64, 45), stats[0].object.get("global_total_field_len").?.integer);
    const freqs = stats[0].object.get("term_doc_freqs").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), freqs.len);
    try std.testing.expectEqualStrings("hello", freqs[0].object.get("term").?.string);
    try std.testing.expectEqual(@as(i64, 4), freqs[0].object.get("doc_freq").?.integer);
}

test "encode query request with distributed text stats parses through query contract" {
    const alloc = std.testing.allocator;

    const encoded = try encodeQueryRequest(alloc, .{
        .full_text = .{ .match = .{ .field = "body", .text = "hello world" } },
        .fields = &.{"title"},
        .include_all_fields = false,
        .limit = 7,
        .distributed_text_stats = &.{.{
            .field = "body",
            .global_doc_count = 9,
            .global_total_field_len = 45,
            .term_doc_freqs = &.{
                .{ .term = "hello", .doc_freq = 4 },
                .{ .term = "world", .doc_freq = 2 },
            },
        }},
    });
    defer alloc.free(encoded);

    var owned = try query_api.parseQueryRequest(alloc, null, "docs", encoded);
    defer owned.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 7), owned.req.limit);
    try std.testing.expectEqual(@as(usize, 1), owned.fields.len);
    try std.testing.expectEqualStrings("title", owned.fields[0]);
    try std.testing.expect(owned.req.full_text != null);
    try std.testing.expectEqual(@as(usize, 1), owned.req.distributed_text_stats.len);
    try std.testing.expectEqualStrings("body", owned.req.distributed_text_stats[0].field);
    try std.testing.expectEqual(@as(u32, 9), owned.req.distributed_text_stats[0].global_doc_count);
    try std.testing.expectEqual(@as(u64, 45), owned.req.distributed_text_stats[0].global_total_field_len);
    try std.testing.expectEqual(@as(usize, 2), owned.req.distributed_text_stats[0].term_doc_freqs.len);
    try std.testing.expectEqualStrings("hello", owned.req.distributed_text_stats[0].term_doc_freqs[0].term);
}

test "encode query request carries internal native doc id constraints through query contract" {
    const alloc = std.testing.allocator;

    const encoded = try encodeQueryRequest(alloc, .{
        .dense_queries = &.{
            .{
                .name = "semantic_idx",
                .index_name = "semantic_idx",
                .query = .{
                    .vector = &.{ 0.25, 0.5 },
                    .k = 5,
                },
            },
        },
        .filter_doc_ids = &.{ "doc:a", "doc:b" },
        .filter_doc_ids_positive = true,
        .exclude_doc_ids = &.{"doc:c"},
        .limit = 5,
    });
    defer alloc.free(encoded);

    var parsed = try parseJsonTestBody(std.json.Value, alloc, encoded);
    defer parsed.deinit();
    const constraints = parsed.value.object.get("native_doc_id_constraints").?.object;
    try std.testing.expectEqual(true, constraints.get("positive_filter").?.bool);
    try std.testing.expectEqualStrings("doc:a", constraints.get("include_doc_ids").?.array.items[0].string);
    try std.testing.expectEqualStrings("doc:c", constraints.get("exclude_doc_ids").?.array.items[0].string);
    try std.testing.expect(parsed.value.object.get("_filter_doc_ids_positive") == null);
    try std.testing.expect(parsed.value.object.get("_filter_doc_ids") == null);
    try std.testing.expect(parsed.value.object.get("_exclude_doc_ids") == null);

    var owned = try query_api.parseQueryRequest(alloc, null, "docs", encoded);
    defer owned.deinit(alloc);

    try std.testing.expect(owned.req.filter_doc_ids_positive);
    try std.testing.expectEqual(@as(usize, 2), owned.req.filter_doc_ids.len);
    try std.testing.expectEqualStrings("doc:a", owned.req.filter_doc_ids[0]);
    try std.testing.expectEqualStrings("doc:b", owned.req.filter_doc_ids[1]);
    try std.testing.expectEqual(@as(usize, 1), owned.req.exclude_doc_ids.len);
    try std.testing.expectEqualStrings("doc:c", owned.req.exclude_doc_ids[0]);
}

test "encode query request rejects in-memory resolved doc filters" {
    const alloc = std.testing.allocator;
    var filter = doc_set.ResolvedDocFilter{ .include = try doc_set.fromOrdinalsAlloc(alloc, &.{1}) };
    defer filter.deinit(alloc);

    try std.testing.expectError(error.UnsupportedQueryRequest, encodeQueryRequest(alloc, .{
        .query = .{ .match_all = {} },
        .resolved_doc_filter = &filter,
    }));
}

test "encode query request preserves empty positive internal doc id filter" {
    const alloc = std.testing.allocator;

    const encoded = try encodeQueryRequest(alloc, .{
        .query = .{ .match_all = {} },
        .filter_doc_ids_positive = true,
    });
    defer alloc.free(encoded);

    var parsed = try parseJsonTestBody(std.json.Value, alloc, encoded);
    defer parsed.deinit();
    const constraints = parsed.value.object.get("native_doc_id_constraints").?.object;
    try std.testing.expectEqual(true, constraints.get("positive_filter").?.bool);
    try std.testing.expectEqual(@as(usize, 0), constraints.get("include_doc_ids").?.array.items.len);

    var owned = try query_api.parseQueryRequest(alloc, null, "docs", encoded);
    defer owned.deinit(alloc);

    try std.testing.expect(owned.req.filter_doc_ids_positive);
    try std.testing.expectEqual(@as(usize, 0), owned.req.filter_doc_ids.len);
}
