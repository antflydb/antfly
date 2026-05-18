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

pub const HttpMethod = enum {
    get,
    post,
    put,
    delete,
};

pub const Route = union(enum) {
    health,
    healthz,
    readyz,
    metrics,
    status,
    list_namespaces,
    list_tables,
    ensure_namespace: NamespaceRoute,
    ensure_table: TableRoute,
    table_indexes: TableRoute,
    table_index: TableIndexRoute,
    ingest_batch: NamespaceRoute,
    ingest_table_batch: TableRoute,
    table_batch: TableRoute,
    build_namespace: NamespaceRoute,
    build_status: NamespaceRoute,
    policy: NamespaceRoute,
    internal_table_build: TableRoute,
    internal_table_build_status: TableRoute,
    internal_table_policy: TableRoute,
    head: NamespaceRoute,
    publish_head: NamespaceRoute,
    query: NamespaceRoute,
    table_query: TableRoute,
    table_query_request: TableRoute,
    table_query_published: TableRoute,
    table_query_latest: TableRoute,
    query_search: NamespaceRoute,
    table_query_search: TableRoute,
    table_query_graph_neighbors: TableRoute,
    table_query_graph_traverse: TableRoute,
    table_query_graph_shortest_path: TableRoute,
    query_graph_neighbors: NamespaceRoute,
    query_graph_traverse: NamespaceRoute,
    query_graph_shortest_path: NamespaceRoute,
    query_head: NamespaceRoute,
    query_latest: NamespaceRoute,
    query_version: VersionRoute,
    query_version_graph_neighbors: VersionRoute,
    query_version_graph_traverse: VersionRoute,
    query_version_graph_shortest_path: VersionRoute,
    query_head_artifact: ArtifactRoute,
    query_version_artifact: ArtifactRoute,
};

pub const NamespaceRoute = struct {
    namespace: []const u8,
};

pub const TableRoute = struct {
    table_name: []const u8,
};

pub const TableIndexRoute = struct {
    table_name: []const u8,
    index_name: []const u8,
};

pub const VersionRoute = struct {
    namespace: []const u8,
    version: u64,
};

pub const ArtifactRoute = struct {
    namespace: []const u8,
    version: ?u64 = null,
    artifact_index: usize,
};

pub fn isInternal(route: Route) bool {
    return switch (route) {
        .list_namespaces,
        .ensure_namespace,
        .ingest_batch,
        .build_namespace,
        .build_status,
        .policy,
        .internal_table_build,
        .internal_table_build_status,
        .internal_table_policy,
        .head,
        .publish_head,
        .query,
        .query_search,
        .query_graph_neighbors,
        .query_graph_traverse,
        .query_graph_shortest_path,
        .query_head,
        .query_latest,
        .query_version,
        .query_version_graph_neighbors,
        .query_version_graph_traverse,
        .query_version_graph_shortest_path,
        .query_head_artifact,
        .query_version_artifact,
        => true,
        else => false,
    };
}

pub fn match(method: HttpMethod, path: []const u8) ?Route {
    if (!std.mem.startsWith(u8, path, "/")) return null;
    if (path.len == 1) return null;
    if (method == .get and std.mem.eql(u8, path, "/health")) return .health;
    if (method == .get and std.mem.eql(u8, path, "/healthz")) return .healthz;
    if (method == .get and std.mem.eql(u8, path, "/readyz")) return .readyz;
    if (method == .get and std.mem.eql(u8, path, "/metrics")) return .metrics;
    if (method == .get and std.mem.eql(u8, path, "/status")) return .status;

    var segments = std.mem.splitScalar(u8, path[1..], '/');
    const root = segments.next() orelse return null;
    if (std.mem.eql(u8, root, "internal")) {
        const version = segments.next() orelse return null;
        if (!std.mem.eql(u8, version, "v1")) return null;
        const scope = segments.next() orelse return null;
        if (std.mem.eql(u8, scope, "namespaces")) {
            return matchNamespaceRoute(method, &segments);
        }
        if (std.mem.eql(u8, scope, "tables")) {
            return matchInternalTableRoute(method, &segments);
        }
        return null;
    }
    if (std.mem.eql(u8, root, "namespaces")) {
        return null;
    }
    if (std.mem.eql(u8, root, "tables")) {
        return matchTableRoute(method, &segments);
    }
    return null;
}

fn matchInternalTableRoute(method: HttpMethod, segments: *std.mem.SplitIterator(u8, .scalar)) ?Route {
    const table_name = segments.next() orelse return null;
    if (table_name.len == 0) return null;

    const suffix = segments.next() orelse return null;
    if (std.mem.eql(u8, suffix, "build")) {
        if (method != .post or segments.next() != null) return null;
        return .{ .internal_table_build = .{ .table_name = table_name } };
    }
    if (std.mem.eql(u8, suffix, "build-status")) {
        if (method != .get or segments.next() != null) return null;
        return .{ .internal_table_build_status = .{ .table_name = table_name } };
    }
    if (std.mem.eql(u8, suffix, "policy")) {
        if (segments.next() != null) return null;
        return switch (method) {
            .get, .put => .{ .internal_table_policy = .{ .table_name = table_name } },
            else => null,
        };
    }
    return null;
}

fn matchNamespaceRoute(method: HttpMethod, segments: *std.mem.SplitIterator(u8, .scalar)) ?Route {
    const namespace = segments.next() orelse {
        if (method == .get) return .list_namespaces;
        return null;
    };
    if (namespace.len == 0) return null;

    const suffix = segments.next() orelse {
        if (segments.next() != null) return null;
        return switch (method) {
            .put => .{ .ensure_namespace = .{ .namespace = namespace } },
            else => null,
        };
    };

    if (std.mem.eql(u8, suffix, "ingest-batch")) {
        if (method != .put or segments.next() != null) return null;
        return .{ .ingest_batch = .{ .namespace = namespace } };
    }
    if (std.mem.eql(u8, suffix, "build")) {
        if (method != .post or segments.next() != null) return null;
        return .{ .build_namespace = .{ .namespace = namespace } };
    }
    if (std.mem.eql(u8, suffix, "build-status")) {
        if (method != .get or segments.next() != null) return null;
        return .{ .build_status = .{ .namespace = namespace } };
    }
    if (std.mem.eql(u8, suffix, "policy")) {
        if (segments.next() != null) return null;
        return switch (method) {
            .get, .put => .{ .policy = .{ .namespace = namespace } },
            else => null,
        };
    }
    if (std.mem.eql(u8, suffix, "head")) {
        if (segments.next() != null) return null;
        return switch (method) {
            .get => .{ .head = .{ .namespace = namespace } },
            .put => .{ .publish_head = .{ .namespace = namespace } },
            else => null,
        };
    }
    if (std.mem.eql(u8, suffix, "query")) {
        return matchNamespaceQueryRoute(method, namespace, segments);
    }
    return null;
}

fn matchNamespaceQueryRoute(method: HttpMethod, namespace: []const u8, segments: *std.mem.SplitIterator(u8, .scalar)) ?Route {
    const query_suffix = segments.next() orelse {
        if (method != .get) return null;
        return .{ .query = .{ .namespace = namespace } };
    };
    if (std.mem.eql(u8, query_suffix, "latest")) {
        if (method != .get or segments.next() != null) return null;
        return .{ .query_latest = .{ .namespace = namespace } };
    }
    if (std.mem.eql(u8, query_suffix, "search")) {
        if (method != .post or segments.next() != null) return null;
        return .{ .query_search = .{ .namespace = namespace } };
    }
    if (std.mem.eql(u8, query_suffix, "graph")) {
        const graph_suffix = segments.next() orelse return null;
        if (std.mem.eql(u8, graph_suffix, "neighbors")) {
            if (method != .post or segments.next() != null) return null;
            return .{ .query_graph_neighbors = .{ .namespace = namespace } };
        }
        if (std.mem.eql(u8, graph_suffix, "traverse")) {
            if (method != .post or segments.next() != null) return null;
            return .{ .query_graph_traverse = .{ .namespace = namespace } };
        }
        if (std.mem.eql(u8, graph_suffix, "shortest-path")) {
            if (method != .post or segments.next() != null) return null;
            return .{ .query_graph_shortest_path = .{ .namespace = namespace } };
        }
        return null;
    }
    if (std.mem.eql(u8, query_suffix, "head")) {
        const maybe_artifacts = segments.next() orelse {
            if (method != .get) return null;
            return .{ .query_head = .{ .namespace = namespace } };
        };
        if (!std.mem.eql(u8, maybe_artifacts, "artifacts")) return null;
        const artifact_index = parseUsize(segments.next() orelse return null) orelse return null;
        if (method != .get or segments.next() != null) return null;
        return .{ .query_head_artifact = .{
            .namespace = namespace,
            .artifact_index = artifact_index,
        } };
    }
    if (!std.mem.eql(u8, query_suffix, "versions")) return null;

    const version = parseU64(segments.next() orelse return null) orelse return null;
    const maybe_artifacts = segments.next() orelse {
        if (method != .get) return null;
        return .{ .query_version = .{
            .namespace = namespace,
            .version = version,
        } };
    };
    if (std.mem.eql(u8, maybe_artifacts, "graph")) {
        const graph_suffix = segments.next() orelse return null;
        if (std.mem.eql(u8, graph_suffix, "neighbors")) {
            if (method != .post or segments.next() != null) return null;
            return .{ .query_version_graph_neighbors = .{
                .namespace = namespace,
                .version = version,
            } };
        }
        if (std.mem.eql(u8, graph_suffix, "traverse")) {
            if (method != .post or segments.next() != null) return null;
            return .{ .query_version_graph_traverse = .{
                .namespace = namespace,
                .version = version,
            } };
        }
        if (std.mem.eql(u8, graph_suffix, "shortest-path")) {
            if (method != .post or segments.next() != null) return null;
            return .{ .query_version_graph_shortest_path = .{
                .namespace = namespace,
                .version = version,
            } };
        }
        return null;
    }
    if (!std.mem.eql(u8, maybe_artifacts, "artifacts")) return null;
    const artifact_index = parseUsize(segments.next() orelse return null) orelse return null;
    if (method != .get or segments.next() != null) return null;
    return .{ .query_version_artifact = .{
        .namespace = namespace,
        .version = version,
        .artifact_index = artifact_index,
    } };
}

fn matchTableRoute(method: HttpMethod, segments: *std.mem.SplitIterator(u8, .scalar)) ?Route {
    const table_name = segments.next() orelse {
        if (method == .get) return .list_tables;
        return null;
    };
    if (table_name.len == 0) return null;

    const suffix = segments.next() orelse {
        if (segments.next() != null) return null;
        return switch (method) {
            .put, .post => .{ .ensure_table = .{ .table_name = table_name } },
            else => null,
        };
    };

    if (std.mem.eql(u8, suffix, "ingest-batch")) {
        if (method != .put or segments.next() != null) return null;
        return .{ .ingest_table_batch = .{ .table_name = table_name } };
    }
    if (std.mem.eql(u8, suffix, "indexes")) {
        const index_name = segments.next() orelse {
            if (method != .get) return null;
            return .{ .table_indexes = .{ .table_name = table_name } };
        };
        if (index_name.len == 0 or segments.next() != null) return null;
        return switch (method) {
            .get, .post, .delete => .{ .table_index = .{ .table_name = table_name, .index_name = index_name } },
            else => null,
        };
    }
    if (std.mem.eql(u8, suffix, "batch")) {
        if (method != .post or segments.next() != null) return null;
        return .{ .table_batch = .{ .table_name = table_name } };
    }
    if (!std.mem.eql(u8, suffix, "query")) return null;

    const query_suffix = segments.next() orelse {
        return switch (method) {
            .get => .{ .table_query = .{ .table_name = table_name } },
            .post => .{ .table_query_request = .{ .table_name = table_name } },
            else => null,
        };
    };
    if (std.mem.eql(u8, query_suffix, "published")) {
        if (method != .get or segments.next() != null) return null;
        return .{ .table_query_published = .{ .table_name = table_name } };
    }
    if (std.mem.eql(u8, query_suffix, "latest")) {
        if (method != .get or segments.next() != null) return null;
        return .{ .table_query_latest = .{ .table_name = table_name } };
    }
    if (std.mem.eql(u8, query_suffix, "search")) {
        if (method != .post or segments.next() != null) return null;
        return .{ .table_query_search = .{ .table_name = table_name } };
    }
    if (std.mem.eql(u8, query_suffix, "graph")) {
        const graph_suffix = segments.next() orelse return null;
        if (std.mem.eql(u8, graph_suffix, "neighbors")) {
            if (method != .post or segments.next() != null) return null;
            return .{ .table_query_graph_neighbors = .{ .table_name = table_name } };
        }
        if (std.mem.eql(u8, graph_suffix, "traverse")) {
            if (method != .post or segments.next() != null) return null;
            return .{ .table_query_graph_traverse = .{ .table_name = table_name } };
        }
        if (std.mem.eql(u8, graph_suffix, "shortest-path")) {
            if (method != .post or segments.next() != null) return null;
            return .{ .table_query_graph_shortest_path = .{ .table_name = table_name } };
        }
        return null;
    }
    return null;
}

fn parseU64(raw: []const u8) ?u64 {
    return std.fmt.parseInt(u64, raw, 10) catch null;
}

fn parseUsize(raw: []const u8) ?usize {
    return std.fmt.parseInt(usize, raw, 10) catch null;
}

test "http routes match internal namespace endpoints" {
    try std.testing.expectEqual(Route.health, match(.get, "/health").?);
    try std.testing.expectEqual(Route.metrics, match(.get, "/metrics").?);
    try std.testing.expectEqual(Route.status, match(.get, "/status").?);
    try std.testing.expectEqual(Route.list_namespaces, match(.get, "/internal/v1/namespaces").?);

    const ensure = match(.put, "/internal/v1/namespaces/docs").?;
    try std.testing.expectEqualStrings("docs", ensure.ensure_namespace.namespace);

    const ingest = match(.put, "/internal/v1/namespaces/docs/ingest-batch").?;
    try std.testing.expectEqualStrings("docs", ingest.ingest_batch.namespace);

    const build = match(.post, "/internal/v1/namespaces/docs/build").?;
    try std.testing.expectEqualStrings("docs", build.build_namespace.namespace);

    const status = match(.get, "/internal/v1/namespaces/docs/build-status").?;
    try std.testing.expectEqualStrings("docs", status.build_status.namespace);

    const head = match(.get, "/internal/v1/namespaces/docs/head").?;
    try std.testing.expectEqualStrings("docs", head.head.namespace);

    const policy_get = match(.get, "/internal/v1/namespaces/docs/policy").?;
    try std.testing.expectEqualStrings("docs", policy_get.policy.namespace);

    const policy_put = match(.put, "/internal/v1/namespaces/docs/policy").?;
    try std.testing.expectEqualStrings("docs", policy_put.policy.namespace);

    const internal_table_build = match(.post, "/internal/v1/tables/docs/build").?;
    try std.testing.expectEqualStrings("docs", internal_table_build.internal_table_build.table_name);

    const internal_table_status = match(.get, "/internal/v1/tables/docs/build-status").?;
    try std.testing.expectEqualStrings("docs", internal_table_status.internal_table_build_status.table_name);

    const internal_table_policy_get = match(.get, "/internal/v1/tables/docs/policy").?;
    try std.testing.expectEqualStrings("docs", internal_table_policy_get.internal_table_policy.table_name);

    const internal_table_policy_put = match(.put, "/internal/v1/tables/docs/policy").?;
    try std.testing.expectEqualStrings("docs", internal_table_policy_put.internal_table_policy.table_name);

    const publish_head = match(.put, "/internal/v1/namespaces/docs/head").?;
    try std.testing.expectEqualStrings("docs", publish_head.publish_head.namespace);

    const default_query = match(.get, "/internal/v1/namespaces/docs/query").?;
    try std.testing.expectEqualStrings("docs", default_query.query.namespace);

    const query = match(.get, "/internal/v1/namespaces/docs/query/head").?;
    try std.testing.expectEqualStrings("docs", query.query_head.namespace);

    const latest = match(.get, "/internal/v1/namespaces/docs/query/latest").?;
    try std.testing.expectEqualStrings("docs", latest.query_latest.namespace);

    const search = match(.post, "/internal/v1/namespaces/docs/query/search").?;
    try std.testing.expectEqualStrings("docs", search.query_search.namespace);

    const graph_neighbors = match(.post, "/internal/v1/namespaces/docs/query/graph/neighbors").?;
    try std.testing.expectEqualStrings("docs", graph_neighbors.query_graph_neighbors.namespace);
    const graph_traverse = match(.post, "/internal/v1/namespaces/docs/query/graph/traverse").?;
    try std.testing.expectEqualStrings("docs", graph_traverse.query_graph_traverse.namespace);
    const graph_shortest = match(.post, "/internal/v1/namespaces/docs/query/graph/shortest-path").?;
    try std.testing.expectEqualStrings("docs", graph_shortest.query_graph_shortest_path.namespace);
    const version_graph_neighbors = match(.post, "/internal/v1/namespaces/docs/query/versions/7/graph/neighbors").?;
    try std.testing.expectEqualStrings("docs", version_graph_neighbors.query_version_graph_neighbors.namespace);
    try std.testing.expectEqual(@as(u64, 7), version_graph_neighbors.query_version_graph_neighbors.version);
    const version_graph_traverse = match(.post, "/internal/v1/namespaces/docs/query/versions/7/graph/traverse").?;
    try std.testing.expectEqualStrings("docs", version_graph_traverse.query_version_graph_traverse.namespace);
    try std.testing.expectEqual(@as(u64, 7), version_graph_traverse.query_version_graph_traverse.version);
    const version_graph_shortest = match(.post, "/internal/v1/namespaces/docs/query/versions/7/graph/shortest-path").?;
    try std.testing.expectEqualStrings("docs", version_graph_shortest.query_version_graph_shortest_path.namespace);
    try std.testing.expectEqual(@as(u64, 7), version_graph_shortest.query_version_graph_shortest_path.version);

    const versioned = match(.get, "/internal/v1/namespaces/docs/query/versions/7").?;
    try std.testing.expectEqualStrings("docs", versioned.query_version.namespace);
    try std.testing.expectEqual(@as(u64, 7), versioned.query_version.version);

    const head_artifact = match(.get, "/internal/v1/namespaces/docs/query/head/artifacts/3").?;
    try std.testing.expectEqualStrings("docs", head_artifact.query_head_artifact.namespace);
    try std.testing.expectEqual(@as(usize, 3), head_artifact.query_head_artifact.artifact_index);

    const version_artifact = match(.get, "/internal/v1/namespaces/docs/query/versions/9/artifacts/2").?;
    try std.testing.expectEqualStrings("docs", version_artifact.query_version_artifact.namespace);
    try std.testing.expectEqual(@as(?u64, 9), version_artifact.query_version_artifact.version);
    try std.testing.expectEqual(@as(usize, 2), version_artifact.query_version_artifact.artifact_index);
}

test "http routes match the table public endpoints" {
    try std.testing.expectEqual(Route.list_tables, match(.get, "/tables").?);

    const ensure = match(.put, "/tables/docs").?;
    try std.testing.expectEqualStrings("docs", ensure.ensure_table.table_name);

    const ingest = match(.put, "/tables/docs/ingest-batch").?;
    try std.testing.expectEqualStrings("docs", ingest.ingest_table_batch.table_name);

    const indexes = match(.get, "/tables/docs/indexes").?;
    try std.testing.expectEqualStrings("docs", indexes.table_indexes.table_name);

    const index = match(.get, "/tables/docs/indexes/embed_idx").?;
    try std.testing.expectEqualStrings("docs", index.table_index.table_name);
    try std.testing.expectEqualStrings("embed_idx", index.table_index.index_name);
    const create_index = match(.post, "/tables/docs/indexes/embed_idx").?;
    try std.testing.expectEqualStrings("docs", create_index.table_index.table_name);
    try std.testing.expectEqualStrings("embed_idx", create_index.table_index.index_name);
    const delete_index = match(.delete, "/tables/docs/indexes/embed_idx").?;
    try std.testing.expectEqualStrings("docs", delete_index.table_index.table_name);
    try std.testing.expectEqualStrings("embed_idx", delete_index.table_index.index_name);

    const batch = match(.post, "/tables/docs/batch").?;
    try std.testing.expectEqualStrings("docs", batch.table_batch.table_name);

    try std.testing.expect(match(.post, "/tables/docs/build") == null);
    try std.testing.expect(match(.get, "/tables/docs/build-status") == null);
    try std.testing.expect(match(.get, "/tables/docs/policy") == null);
    try std.testing.expect(match(.put, "/tables/docs/policy") == null);

    const default_query = match(.get, "/tables/docs/query").?;
    try std.testing.expectEqualStrings("docs", default_query.table_query.table_name);

    const published = match(.get, "/tables/docs/query/published").?;
    try std.testing.expectEqualStrings("docs", published.table_query_published.table_name);

    const latest = match(.get, "/tables/docs/query/latest").?;
    try std.testing.expectEqualStrings("docs", latest.table_query_latest.table_name);

    const search_via_query = match(.post, "/tables/docs/query").?;
    try std.testing.expectEqualStrings("docs", search_via_query.table_query_request.table_name);

    const search = match(.post, "/tables/docs/query/search").?;
    try std.testing.expectEqualStrings("docs", search.table_query_search.table_name);

    const graph_neighbors = match(.post, "/tables/docs/query/graph/neighbors").?;
    try std.testing.expectEqualStrings("docs", graph_neighbors.table_query_graph_neighbors.table_name);

    const graph_traverse = match(.post, "/tables/docs/query/graph/traverse").?;
    try std.testing.expectEqualStrings("docs", graph_traverse.table_query_graph_traverse.table_name);

    const graph_shortest = match(.post, "/tables/docs/query/graph/shortest-path").?;
    try std.testing.expectEqualStrings("docs", graph_shortest.table_query_graph_shortest_path.table_name);
}

test "http routes reject invalid paths and methods" {
    try std.testing.expectEqual(@as(?Route, null), match(.post, "/health"));
    try std.testing.expectEqual(@as(?Route, null), match(.post, "/metrics"));
    try std.testing.expectEqual(@as(?Route, null), match(.post, "/status"));
    try std.testing.expectEqual(@as(?Route, null), match(.post, "/namespaces"));
    try std.testing.expectEqual(@as(?Route, null), match(.get, "/namespaces"));
    try std.testing.expectEqual(@as(?Route, null), match(.get, "/"));
    try std.testing.expectEqual(@as(?Route, null), match(.get, "namespaces/docs"));
    try std.testing.expectEqual(@as(?Route, null), match(.get, "/namespaces/"));
    try std.testing.expectEqual(@as(?Route, null), match(.post, "/internal/v1/namespaces"));
    try std.testing.expectEqual(@as(?Route, null), match(.get, "/internal/v1/namespaces/"));
    try std.testing.expectEqual(@as(?Route, null), match(.post, "/internal/v1/namespaces/docs/ingest-batch"));
    try std.testing.expectEqual(@as(?Route, null), match(.get, "/internal/v1/namespaces/docs/query/search"));
    try std.testing.expectEqual(@as(?Route, null), match(.get, "/internal/v1/namespaces/docs/query/graph/neighbors"));
    try std.testing.expectEqual(@as(?Route, null), match(.get, "/internal/v1/namespaces/docs/query/graph/traverse"));
    try std.testing.expectEqual(@as(?Route, null), match(.get, "/internal/v1/namespaces/docs/query/graph/shortest-path"));
    try std.testing.expectEqual(@as(?Route, null), match(.get, "/internal/v1/namespaces/docs/query/versions/7/graph/neighbors"));
    try std.testing.expectEqual(@as(?Route, null), match(.get, "/internal/v1/namespaces/docs/query/versions/7/graph/traverse"));
    try std.testing.expectEqual(@as(?Route, null), match(.get, "/internal/v1/namespaces/docs/query/versions/7/graph/shortest-path"));
    try std.testing.expectEqual(@as(?Route, null), match(.get, "/internal/v1/namespaces/docs/query/head/extra"));
    try std.testing.expectEqual(@as(?Route, null), match(.get, "/internal/v1/namespaces/docs/query/versions/not-a-number"));
    try std.testing.expectEqual(@as(?Route, null), match(.get, "/internal/v1/namespaces/docs/query/versions/7/artifacts/nope"));
    try std.testing.expectEqual(@as(?Route, null), match(.post, "/tables"));
    try std.testing.expectEqual(@as(?Route, null), match(.get, "/tables/"));
    try std.testing.expectEqual(@as(?Route, null), match(.post, "/tables/docs/indexes"));
    try std.testing.expectEqual(@as(?Route, null), match(.get, "/tables/docs/query/search"));
    try std.testing.expectEqual(@as(?Route, null), match(.get, "/tables/docs/query/nope"));
    try std.testing.expectEqual(@as(?Route, null), match(.get, "/tables/docs/query/graph/neighbors"));
    try std.testing.expectEqual(@as(?Route, null), match(.get, "/tables/docs/query/graph/traverse"));
    try std.testing.expectEqual(@as(?Route, null), match(.get, "/tables/docs/query/graph/shortest-path"));
}

test "http routes classify internal and public surfaces explicitly" {
    try std.testing.expect(isInternal(match(.get, "/internal/v1/namespaces").?));
    try std.testing.expect(isInternal(match(.get, "/internal/v1/namespaces/docs/head").?));
    try std.testing.expect(isInternal(match(.get, "/internal/v1/namespaces/docs/query/versions/7").?));
    try std.testing.expect(isInternal(match(.get, "/internal/v1/namespaces/docs/query/head/artifacts/3").?));

    try std.testing.expect(!isInternal(match(.get, "/health").?));
    try std.testing.expect(!isInternal(match(.get, "/tables").?));
    try std.testing.expect(!isInternal(match(.put, "/tables/docs").?));
    try std.testing.expect(!isInternal(match(.get, "/tables/docs/query").?));
    try std.testing.expect(!isInternal(match(.post, "/tables/docs/query/search").?));
    try std.testing.expect(!isInternal(match(.post, "/tables/docs/query/graph/neighbors").?));
}
