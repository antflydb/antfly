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
const chunking_openapi = @import("antfly_chunking_openapi");
const embeddings_openapi = @import("antfly_embeddings_openapi");
const indexes_openapi = @import("antfly_indexes_openapi");
const metadata_openapi = @import("antfly_metadata_openapi");
const schema_openapi = @import("antfly_schema_openapi");
const bleve_openapi = @import("antfly_bleve_query_openapi");

fn stringifyJsonAlloc(alloc: std.mem.Allocator, value: anytype) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})});
}

pub fn encodeCreateTableRequest(alloc: std.mem.Allocator, description: []const u8) ![]u8 {
    return try stringifyJsonAlloc(alloc, metadata_openapi.CreateTableRequest{
        .num_shards = 1,
        .description = description,
    });
}

pub fn encodeSchemaUpdateRequest(alloc: std.mem.Allocator) ![]u8 {
    var parsed = try std.json.parseFromSlice(
        schema_openapi.TableSchema,
        alloc,
        "{\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"text\"},\"status\":{\"type\":\"keyword\"}}}}}}",
        .{},
    );
    defer parsed.deinit();
    return try stringifyJsonAlloc(alloc, parsed.value);
}

pub fn encodeCreateIndexRequest(alloc: std.mem.Allocator, index_name: []const u8) ![]u8 {
    return try std.fmt.allocPrint(
        alloc,
        "{{\"name\":{f},\"type\":\"embeddings\",\"external\":true,\"dimension\":384}}",
        .{std.json.fmt(index_name, .{})},
    );
}

pub fn encodeManagedEmbeddingsIndexRequest(
    alloc: std.mem.Allocator,
    index_name: []const u8,
    field_name: []const u8,
    dimension: i64,
    embedder: embeddings_openapi.EmbedderConfig,
    chunker: ?chunking_openapi.ChunkerConfig,
) ![]u8 {
    const embedder_json = try stringifyJsonAlloc(alloc, embedder);
    defer alloc.free(embedder_json);

    if (chunker) |chunker_cfg| {
        const chunker_json = try stringifyJsonAlloc(alloc, chunker_cfg);
        defer alloc.free(chunker_json);
        return try std.fmt.allocPrint(
            alloc,
            "{{\"name\":{f},\"type\":\"embeddings\",\"field\":{f},\"dimension\":{d},\"embedder\":{s},\"chunker\":{s}}}",
            .{
                std.json.fmt(index_name, .{}),
                std.json.fmt(field_name, .{}),
                dimension,
                embedder_json,
                chunker_json,
            },
        );
    }

    return try std.fmt.allocPrint(
        alloc,
        "{{\"name\":{f},\"type\":\"embeddings\",\"field\":{f},\"dimension\":{d},\"embedder\":{s}}}",
        .{
            std.json.fmt(index_name, .{}),
            std.json.fmt(field_name, .{}),
            dimension,
            embedder_json,
        },
    );
}

pub fn encodeManagedEmbeddingsIndexTemplateRequest(
    alloc: std.mem.Allocator,
    index_name: []const u8,
    template_source: []const u8,
    dimension: i64,
    embedder: embeddings_openapi.EmbedderConfig,
) ![]u8 {
    const embedder_json = try stringifyJsonAlloc(alloc, embedder);
    defer alloc.free(embedder_json);

    return try std.fmt.allocPrint(
        alloc,
        "{{\"name\":{f},\"type\":\"embeddings\",\"template\":{f},\"dimension\":{d},\"embedder\":{s}}}",
        .{
            std.json.fmt(index_name, .{}),
            std.json.fmt(template_source, .{}),
            dimension,
            embedder_json,
        },
    );
}

pub fn encodeManagedEmbeddingsIndexTemplateWithChunkerRequest(
    alloc: std.mem.Allocator,
    index_name: []const u8,
    template_source: []const u8,
    dimension: i64,
    embedder: embeddings_openapi.EmbedderConfig,
    chunker: chunking_openapi.ChunkerConfig,
) ![]u8 {
    const embedder_json = try stringifyJsonAlloc(alloc, embedder);
    defer alloc.free(embedder_json);
    const chunker_json = try stringifyJsonAlloc(alloc, chunker);
    defer alloc.free(chunker_json);

    return try std.fmt.allocPrint(
        alloc,
        "{{\"name\":{f},\"type\":\"embeddings\",\"template\":{f},\"dimension\":{d},\"embedder\":{s},\"chunker\":{s}}}",
        .{
            std.json.fmt(index_name, .{}),
            std.json.fmt(template_source, .{}),
            dimension,
            embedder_json,
            chunker_json,
        },
    );
}

pub fn encodeManagedSparseEmbeddingsIndexRequest(
    alloc: std.mem.Allocator,
    index_name: []const u8,
    field_name: []const u8,
    embedder: embeddings_openapi.EmbedderConfig,
) ![]u8 {
    const embedder_json = try stringifyJsonAlloc(alloc, embedder);
    defer alloc.free(embedder_json);

    return try std.fmt.allocPrint(
        alloc,
        "{{\"name\":{f},\"type\":\"embeddings\",\"field\":{f},\"sparse\":true,\"embedder\":{s}}}",
        .{
            std.json.fmt(index_name, .{}),
            std.json.fmt(field_name, .{}),
            embedder_json,
        },
    );
}

pub const TransactionReadRef = struct {
    table_name: []const u8,
    key: []const u8,
    version: []const u8,
};

pub const TransactionTableBatch = struct {
    table_name: []const u8,
    batch_json: []const u8,
};

pub fn encodeTransactionCommitRequest(
    alloc: std.mem.Allocator,
    read_set: []const TransactionReadRef,
    tables: []const TransactionTableBatch,
    sync_level: ?[]const u8,
) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);

    try out.appendSlice(alloc, "{\"read_set\":[");
    for (read_set, 0..) |item, i| {
        if (i > 0) try out.append(alloc, ',');
        const encoded = try std.fmt.allocPrint(
            alloc,
            "{{\"table\":{f},\"key\":{f},\"version\":{f}}}",
            .{
                std.json.fmt(item.table_name, .{}),
                std.json.fmt(item.key, .{}),
                std.json.fmt(item.version, .{}),
            },
        );
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
    }
    try out.appendSlice(alloc, "],\"tables\":{");
    for (tables, 0..) |table, i| {
        if (i > 0) try out.append(alloc, ',');
        const key = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(table.table_name, .{})});
        defer alloc.free(key);
        try out.appendSlice(alloc, key);
        try out.append(alloc, ':');
        try out.appendSlice(alloc, table.batch_json);
    }
    try out.append(alloc, '}');
    if (sync_level) |level| {
        const encoded = try std.fmt.allocPrint(alloc, ",\"sync_level\":{f}", .{std.json.fmt(level, .{})});
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
    }
    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

pub fn encodeTransactionBeginRequest(
    alloc: std.mem.Allocator,
    sync_level: ?[]const u8,
) ![]u8 {
    if (sync_level) |level| {
        return try std.fmt.allocPrint(alloc, "{{\"sync_level\":{f}}}", .{std.json.fmt(level, .{})});
    }
    return try alloc.dupe(u8, "{}");
}

pub fn encodeTransactionStageReadRequest(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    key: []const u8,
    version: []const u8,
) ![]u8 {
    return try std.fmt.allocPrint(
        alloc,
        "{{\"table\":{f},\"key\":{f},\"version\":{f}}}",
        .{
            std.json.fmt(table_name, .{}),
            std.json.fmt(key, .{}),
            std.json.fmt(version, .{}),
        },
    );
}

pub fn encodeTransactionStageWriteRequest(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    key: []const u8,
    document_json: []const u8,
) ![]u8 {
    return try std.fmt.allocPrint(
        alloc,
        "{{\"table\":{f},\"key\":{f},\"document\":{s}}}",
        .{
            std.json.fmt(table_name, .{}),
            std.json.fmt(key, .{}),
            document_json,
        },
    );
}

pub fn encodeTransactionStageDeleteRequest(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    key: []const u8,
) ![]u8 {
    return try std.fmt.allocPrint(
        alloc,
        "{{\"table\":{f},\"key\":{f}}}",
        .{
            std.json.fmt(table_name, .{}),
            std.json.fmt(key, .{}),
        },
    );
}

pub fn normalizeBatchRequest(alloc: std.mem.Allocator, body: []const u8) ![]u8 {
    var typed = try std.json.parseFromSlice(metadata_openapi.BatchRequest, alloc, body, .{});
    defer typed.deinit();

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{ .allocate = .alloc_always });
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidBatchRequest;

    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;
    var wrote_field = false;
    try writer.writeByte('{');
    inline for (.{ "inserts", "deletes", "transforms", "sync_level" }) |field_name| {
        if (parsed.value.object.get(field_name)) |value| {
            if (wrote_field) try writer.writeByte(',');
            try writer.print("{f}:{f}", .{
                std.json.fmt(field_name, .{}),
                std.json.fmt(value, .{}),
            });
            wrote_field = true;
        }
    }
    try writer.writeByte('}');
    return try out.toOwnedSlice();
}

pub fn encodeMatchQueryRequest(
    alloc: std.mem.Allocator,
    field: []const u8,
    text: []const u8,
    fields: []const []const u8,
    limit: i64,
) ![]u8 {
    return try encodeMatchQueryRequestWithFlags(alloc, field, text, fields, limit, false, false);
}

pub fn encodeMatchQueryRequestWithFlags(
    alloc: std.mem.Allocator,
    field: []const u8,
    text: []const u8,
    fields: []const []const u8,
    limit: i64,
    count: bool,
    profile: bool,
) ![]u8 {
    const full_text_json = try stringifyJsonAlloc(alloc, bleve_openapi.MatchQuery{
        .match = text,
        .field = field,
    });
    defer alloc.free(full_text_json);

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, full_text_json, .{});
    defer parsed.deinit();

    return try stringifyJsonAlloc(alloc, metadata_openapi.QueryRequest{
        .full_text_search = parsed.value,
        .fields = fields,
        .limit = limit,
        .count = count,
        .profile = profile,
    });
}

pub fn encodeFilteredQueryRequest(
    alloc: std.mem.Allocator,
    match_field: []const u8,
    match_text: []const u8,
    filter_field: []const u8,
    filter_term: []const u8,
    exclusion_field: []const u8,
    exclusion_term: []const u8,
    fields: []const []const u8,
    limit: i64,
) ![]u8 {
    const full_text_json = try stringifyJsonAlloc(alloc, bleve_openapi.MatchQuery{
        .match = match_text,
        .field = match_field,
    });
    defer alloc.free(full_text_json);
    var full_text = try std.json.parseFromSlice(std.json.Value, alloc, full_text_json, .{});
    defer full_text.deinit();

    const filter_json = try stringifyJsonAlloc(alloc, bleve_openapi.TermQuery{
        .term = filter_term,
        .field = filter_field,
    });
    defer alloc.free(filter_json);
    var filter = try std.json.parseFromSlice(std.json.Value, alloc, filter_json, .{});
    defer filter.deinit();

    const exclusion_json = try stringifyJsonAlloc(alloc, bleve_openapi.TermQuery{
        .term = exclusion_term,
        .field = exclusion_field,
    });
    defer alloc.free(exclusion_json);
    var exclusion = try std.json.parseFromSlice(std.json.Value, alloc, exclusion_json, .{});
    defer exclusion.deinit();

    return try stringifyJsonAlloc(alloc, metadata_openapi.QueryRequest{
        .full_text_search = full_text.value,
        .filter_query = filter.value,
        .exclusion_query = exclusion.value,
        .fields = fields,
        .limit = limit,
    });
}

pub fn encodeBleveQueryRequest(
    alloc: std.mem.Allocator,
    query_value: anytype,
    fields: []const []const u8,
    limit: i64,
) ![]u8 {
    const full_text_json = try stringifyJsonAlloc(alloc, query_value);
    defer alloc.free(full_text_json);
    var full_text = try std.json.parseFromSlice(std.json.Value, alloc, full_text_json, .{});
    defer full_text.deinit();

    return try stringifyJsonAlloc(alloc, metadata_openapi.QueryRequest{
        .full_text_search = full_text.value,
        .fields = fields,
        .limit = limit,
    });
}

pub fn encodeSemanticQueryRequest(
    alloc: std.mem.Allocator,
    semantic_search: []const u8,
    indexes: []const []const u8,
    limit: i64,
) ![]u8 {
    return try stringifyJsonAlloc(alloc, metadata_openapi.QueryRequest{
        .semantic_search = semantic_search,
        .indexes = indexes,
        .limit = limit,
    });
}

pub fn encodeSemanticQueryWithTemplateRequest(
    alloc: std.mem.Allocator,
    semantic_search: []const u8,
    embedding_template: []const u8,
    indexes: []const []const u8,
    limit: i64,
) ![]u8 {
    return try stringifyJsonAlloc(alloc, metadata_openapi.QueryRequest{
        .semantic_search = semantic_search,
        .embedding_template = embedding_template,
        .indexes = indexes,
        .limit = limit,
    });
}

pub fn encodeSparseEmbeddingsQueryRequest(
    alloc: std.mem.Allocator,
    index_name: []const u8,
    indices: []const u32,
    values: []const f32,
    limit: i64,
) ![]u8 {
    var sparse_json = std.ArrayListUnmanaged(u8).empty;
    defer sparse_json.deinit(alloc);
    try sparse_json.appendSlice(alloc, "{\"indices\":[");
    for (indices, 0..) |value, i| {
        if (i > 0) try sparse_json.append(alloc, ',');
        try sparse_json.print(alloc, "{d}", .{value});
    }
    try sparse_json.appendSlice(alloc, "],\"values\":[");
    for (values, 0..) |value, i| {
        if (i > 0) try sparse_json.append(alloc, ',');
        try sparse_json.print(alloc, "{d}", .{value});
    }
    try sparse_json.appendSlice(alloc, "]}");

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, sparse_json.items, .{});
    defer parsed.deinit();

    var embeddings = std.json.ArrayHashMap(metadata_openapi.Embedding){};
    defer embeddings.deinit(alloc);
    try embeddings.map.put(alloc, index_name, parsed.value);

    return try stringifyJsonAlloc(alloc, metadata_openapi.QueryRequest{
        .embeddings = embeddings,
        .limit = limit,
    });
}

pub fn encodeSemanticSparseHybridQueryRequest(
    alloc: std.mem.Allocator,
    semantic_search: []const u8,
    dense_index_name: []const u8,
    sparse_index_name: []const u8,
    indices: []const u32,
    values: []const f32,
    limit: i64,
) ![]u8 {
    var sparse_json = std.ArrayListUnmanaged(u8).empty;
    defer sparse_json.deinit(alloc);
    try sparse_json.appendSlice(alloc, "{\"indices\":[");
    for (indices, 0..) |value, i| {
        if (i > 0) try sparse_json.append(alloc, ',');
        try sparse_json.print(alloc, "{d}", .{value});
    }
    try sparse_json.appendSlice(alloc, "],\"values\":[");
    for (values, 0..) |value, i| {
        if (i > 0) try sparse_json.append(alloc, ',');
        try sparse_json.print(alloc, "{d}", .{value});
    }
    try sparse_json.appendSlice(alloc, "]}");

    const embeddings_json = try std.fmt.allocPrint(alloc, "{{\"{s}\":{s}}}", .{ sparse_index_name, sparse_json.items });
    defer alloc.free(embeddings_json);
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, embeddings_json, .{});
    defer parsed.deinit();

    var embeddings = std.json.ArrayHashMap(metadata_openapi.Embedding){};
    defer embeddings.deinit(alloc);
    try embeddings.map.put(alloc, sparse_index_name, parsed.value.object.get(sparse_index_name).?);

    const indexes = [_][]const u8{ dense_index_name, sparse_index_name };
    return try stringifyJsonAlloc(alloc, metadata_openapi.QueryRequest{
        .semantic_search = semantic_search,
        .embeddings = embeddings,
        .indexes = indexes[0..],
        .limit = limit,
    });
}

pub fn encodeGraphNeighborsQueryRequest(
    alloc: std.mem.Allocator,
    name: []const u8,
    index_name: []const u8,
    start_keys: []const []const u8,
    edge_types: []const []const u8,
    limit: i64,
) ![]u8 {
    var graph_searches = std.json.ArrayHashMap(indexes_openapi.GraphQuery){};
    defer graph_searches.deinit(alloc);
    try graph_searches.map.put(alloc, name, .{
        .type = .neighbors,
        .index_name = index_name,
        .start_nodes = .{ .keys = start_keys },
        .params = .{
            .edge_types = edge_types,
        },
    });
    return try stringifyJsonAlloc(alloc, metadata_openapi.QueryRequest{
        .graph_searches = graph_searches,
        .limit = limit,
    });
}

pub fn encodeGraphTraverseQueryRequest(
    alloc: std.mem.Allocator,
    name: []const u8,
    index_name: []const u8,
    start_keys: []const []const u8,
    edge_types: []const []const u8,
    max_depth: i64,
    limit: i64,
) ![]u8 {
    var graph_searches = std.json.ArrayHashMap(indexes_openapi.GraphQuery){};
    defer graph_searches.deinit(alloc);
    try graph_searches.map.put(alloc, name, .{
        .type = .traverse,
        .index_name = index_name,
        .start_nodes = .{ .keys = start_keys },
        .params = .{
            .edge_types = edge_types,
            .max_depth = max_depth,
        },
    });
    return try stringifyJsonAlloc(alloc, metadata_openapi.QueryRequest{
        .graph_searches = graph_searches,
        .limit = limit,
    });
}

pub fn encodeGraphTraverseQueryRequestWithPaths(
    alloc: std.mem.Allocator,
    name: []const u8,
    index_name: []const u8,
    start_keys: []const []const u8,
    edge_types: []const []const u8,
    max_depth: i64,
    limit: i64,
) ![]u8 {
    var graph_searches = std.json.ArrayHashMap(indexes_openapi.GraphQuery){};
    defer graph_searches.deinit(alloc);
    try graph_searches.map.put(alloc, name, .{
        .type = .traverse,
        .index_name = index_name,
        .start_nodes = .{ .keys = start_keys },
        .params = .{
            .edge_types = edge_types,
            .max_depth = max_depth,
            .include_paths = true,
        },
    });
    return try stringifyJsonAlloc(alloc, metadata_openapi.QueryRequest{
        .graph_searches = graph_searches,
        .limit = limit,
    });
}

pub fn encodeGraphShortestPathQueryRequest(
    alloc: std.mem.Allocator,
    name: []const u8,
    index_name: []const u8,
    start_keys: []const []const u8,
    target_keys: []const []const u8,
    edge_types: []const []const u8,
    max_depth: i64,
    limit: i64,
) ![]u8 {
    var graph_searches = std.json.ArrayHashMap(indexes_openapi.GraphQuery){};
    defer graph_searches.deinit(alloc);
    try graph_searches.map.put(alloc, name, .{
        .type = .shortest_path,
        .index_name = index_name,
        .start_nodes = .{ .keys = start_keys },
        .target_nodes = .{ .keys = target_keys },
        .params = .{
            .edge_types = edge_types,
            .max_depth = max_depth,
            .include_paths = true,
        },
    });
    return try stringifyJsonAlloc(alloc, metadata_openapi.QueryRequest{
        .graph_searches = graph_searches,
        .limit = limit,
    });
}

pub fn encodeWeightedGraphShortestPathQueryRequest(
    alloc: std.mem.Allocator,
    name: []const u8,
    index_name: []const u8,
    start_keys: []const []const u8,
    target_keys: []const []const u8,
    edge_types: []const []const u8,
    max_depth: i64,
    limit: i64,
    weight_mode: indexes_openapi.PathWeightMode,
) ![]u8 {
    var graph_searches = std.json.ArrayHashMap(indexes_openapi.GraphQuery){};
    defer graph_searches.deinit(alloc);
    try graph_searches.map.put(alloc, name, .{
        .type = .shortest_path,
        .index_name = index_name,
        .start_nodes = .{ .keys = start_keys },
        .target_nodes = .{ .keys = target_keys },
        .params = .{
            .edge_types = edge_types,
            .max_depth = max_depth,
            .include_paths = true,
            .weight_mode = weight_mode,
        },
    });
    return try stringifyJsonAlloc(alloc, metadata_openapi.QueryRequest{
        .graph_searches = graph_searches,
        .limit = limit,
    });
}

pub fn encodeWeightedGraphKShortestPathsQueryRequest(
    alloc: std.mem.Allocator,
    name: []const u8,
    index_name: []const u8,
    start_keys: []const []const u8,
    target_keys: []const []const u8,
    edge_types: []const []const u8,
    max_depth: i64,
    limit: i64,
    k: i64,
    weight_mode: indexes_openapi.PathWeightMode,
) ![]u8 {
    var graph_searches = std.json.ArrayHashMap(indexes_openapi.GraphQuery){};
    defer graph_searches.deinit(alloc);
    try graph_searches.map.put(alloc, name, .{
        .type = .k_shortest_paths,
        .index_name = index_name,
        .start_nodes = .{ .keys = start_keys },
        .target_nodes = .{ .keys = target_keys },
        .params = .{
            .edge_types = edge_types,
            .max_depth = max_depth,
            .include_paths = true,
            .weight_mode = weight_mode,
            .k = k,
        },
    });
    return try stringifyJsonAlloc(alloc, metadata_openapi.QueryRequest{
        .graph_searches = graph_searches,
        .limit = limit,
    });
}

pub fn encodeMatchGraphTraverseFromResultRefQueryRequest(
    alloc: std.mem.Allocator,
    field: []const u8,
    text: []const u8,
    graph_name: []const u8,
    index_name: []const u8,
    result_ref: []const u8,
    max_depth: i64,
    limit: i64,
) ![]u8 {
    const full_text_json = try stringifyJsonAlloc(alloc, bleve_openapi.MatchQuery{
        .match = text,
        .field = field,
    });
    defer alloc.free(full_text_json);
    var full_text = try std.json.parseFromSlice(std.json.Value, alloc, full_text_json, .{});
    defer full_text.deinit();

    var graph_searches = std.json.ArrayHashMap(indexes_openapi.GraphQuery){};
    defer graph_searches.deinit(alloc);
    try graph_searches.map.put(alloc, graph_name, .{
        .type = .traverse,
        .index_name = index_name,
        .start_nodes = .{ .result_ref = result_ref },
        .params = .{
            .max_depth = max_depth,
        },
    });

    return try stringifyJsonAlloc(alloc, metadata_openapi.QueryRequest{
        .full_text_search = full_text.value,
        .graph_searches = graph_searches,
        .limit = limit,
    });
}
