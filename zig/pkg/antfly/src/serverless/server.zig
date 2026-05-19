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
const runtime_mod = @import("runtime/mod.zig");
const serverless_http_server = @import("../serverless_http_server.zig");
const raft_transport = @import("../raft/transport/mod.zig");
const test_backend = @import("test_backend.zig");

pub const ServerlessServerConfig = struct {
    bootstrap: runtime_mod.BootstrapConfig,
    http: serverless_http_server.ServerlessHttpServerConfig = .{},
    listener: ?raft_transport.StdHttpListenerConfig = null,
};

pub const ServerlessServer = struct {
    alloc: std.mem.Allocator,
    stack: *runtime_mod.OwnedStack,
    owned_http_server: *serverless_http_server.ServerlessHttpServer,
    owned_listener: ?*raft_transport.StdHttpListener = null,

    pub fn init(alloc: std.mem.Allocator, cfg: ServerlessServerConfig) !ServerlessServer {
        const stack = try alloc.create(runtime_mod.OwnedStack);
        errdefer alloc.destroy(stack);
        try stack.init(alloc, cfg.bootstrap);
        errdefer stack.deinit();

        const http_server = try alloc.create(serverless_http_server.ServerlessHttpServer);
        errdefer alloc.destroy(http_server);
        http_server.* = serverless_http_server.ServerlessHttpServer.init(alloc, cfg.http, &stack.handler);

        var owned_listener: ?*raft_transport.StdHttpListener = null;
        errdefer if (owned_listener) |listener| {
            listener.deinit();
            alloc.destroy(listener);
        };

        if (cfg.listener) |listener_cfg| {
            const listener = try alloc.create(raft_transport.StdHttpListener);
            listener.* = raft_transport.StdHttpListener.init(alloc, listener_cfg, http_server.executor());
            owned_listener = listener;
        }

        return .{
            .alloc = alloc,
            .stack = stack,
            .owned_http_server = http_server,
            .owned_listener = owned_listener,
        };
    }

    pub fn deinit(self: *ServerlessServer) void {
        if (self.owned_listener) |listener| {
            listener.deinit();
            self.alloc.destroy(listener);
        }
        self.alloc.destroy(self.owned_http_server);
        self.stack.deinit();
        self.alloc.destroy(self.stack);
        self.* = undefined;
    }

    pub fn start(self: *ServerlessServer) !void {
        try self.stack.runtime.start();
        errdefer self.stack.runtime.stop();
        if (self.owned_listener) |listener| try listener.start();
    }

    pub fn stop(self: *ServerlessServer) void {
        if (self.owned_listener) |listener| listener.stop();
        self.stack.runtime.stop();
    }

    pub fn baseUri(self: *ServerlessServer, alloc: std.mem.Allocator) ![]u8 {
        const listener = self.owned_listener orelse return error.MissingListener;
        return try listener.baseUri(alloc);
    }

    pub fn httpServer(self: *ServerlessServer) *serverless_http_server.ServerlessHttpServer {
        return self.owned_http_server;
    }

    pub fn runtimeStatus(self: *const ServerlessServer) *const runtime_mod.RuntimeStatus {
        return &self.stack.status;
    }
};

test "serverless server module compiles" {
    _ = ServerlessServerConfig;
    _ = ServerlessServer;
}

test "serverless server starts managed runtime and serves listener requests" {
    const alloc = std.testing.allocator;
    const serverless_http_client = @import("../serverless_http_client.zig");
    const api_mod = @import("api/mod.zig");

    var artifacts_buf: [256]u8 = undefined;
    var manifests_buf: [256]u8 = undefined;
    var wal_buf: [256]u8 = undefined;
    var progress_buf: [256]u8 = undefined;
    var catalog_buf: [256]u8 = undefined;
    const artifacts_root = tmpPath(&artifacts_buf, "server-artifacts");
    const manifests_root = tmpPath(&manifests_buf, "server-manifests");
    const wal_root = tmpPath(&wal_buf, "server-wal");
    const progress_root = tmpPath(&progress_buf, "server-progress");
    const catalog_root = tmpPath(&catalog_buf, "server-catalog");
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

    var server = try ServerlessServer.init(alloc, .{
        .bootstrap = .{
            .artifacts_uri = artifacts_uri,
            .manifests_uri = manifests_uri,
            .wal_uri = wal_uri,
            .progress_uri = progress_uri,
            .catalog_uri = catalog_uri,
            .query_cache_dir = null,
            .tick_interval_ms = 1,
            .role = .combined,
        },
        .listener = .{},
    });
    defer server.deinit();
    try server.start();
    defer server.stop();

    const base_uri = try server.baseUri(alloc);
    defer alloc.free(base_uri);

    var client_exec = raft_transport.StdHttpExecutor.init(alloc, .{});
    defer client_exec.deinit();
    var client = serverless_http_client.ServerlessHttpClient.init(alloc, client_exec.executor());
    const tables = client.tables();

    var ensure = try tables.ensure(base_uri, "docs", 100);
    defer ensure.deinit(alloc);
    try std.testing.expect(ensure.created);

    const mutations = [_]api_mod.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "alpha" },
    };
    var ingest = try tables.ingest(base_uri, .{
        .table_name = "docs",
        .timestamp_ns = 123,
        .mutations = &mutations,
    });
    defer ingest.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), ingest.start_lsn);

    try waitForPublishedTable(tables, base_uri, "docs", 1, 250);

    var query = try tables.queryPublished(base_uri, "docs");
    defer query.deinit();
    try std.testing.expectEqual(@as(u64, 1), query.value.version);
    try std.testing.expectEqualStrings("docs", query.value.table_name);
    try std.testing.expectEqual(@as(usize, 1), query.value.documents.len);
    try std.testing.expectEqualStrings("doc-a", query.value.documents[0].doc_id);
    try std.testing.expectEqualStrings("alpha", query.value.documents[0].body);
}

test "serverless server query-only role rejects maintenance routes but serves reads" {
    const alloc = std.testing.allocator;
    const serverless_http_client = @import("../serverless_http_client.zig");
    const api_mod = @import("api/mod.zig");

    var artifacts_buf: [256]u8 = undefined;
    var manifests_buf: [256]u8 = undefined;
    var wal_buf: [256]u8 = undefined;
    var progress_buf: [256]u8 = undefined;
    var catalog_buf: [256]u8 = undefined;
    var cache_buf: [256]u8 = undefined;
    const artifacts_root = tmpPath(&artifacts_buf, "server-query-only-artifacts");
    const manifests_root = tmpPath(&manifests_buf, "server-query-only-manifests");
    const wal_root = tmpPath(&wal_buf, "server-query-only-wal");
    const progress_root = tmpPath(&progress_buf, "server-query-only-progress");
    const catalog_root = tmpPath(&catalog_buf, "server-query-only-catalog");
    const cache_root = tmpPath(&cache_buf, "server-query-only-cache");
    defer cleanupTmp(artifacts_root);
    defer cleanupTmp(manifests_root);
    defer cleanupTmp(wal_root);
    defer cleanupTmp(progress_root);
    defer cleanupTmp(catalog_root);
    defer cleanupTmp(cache_root);

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

    var server = try ServerlessServer.init(alloc, .{
        .bootstrap = .{
            .artifacts_uri = artifacts_uri,
            .manifests_uri = manifests_uri,
            .wal_uri = wal_uri,
            .progress_uri = progress_uri,
            .catalog_uri = catalog_uri,
            .query_cache_dir = std.mem.span(cache_root),
            .tick_interval_ms = 1,
            .role = .query_only,
        },
        .listener = .{},
    });
    defer server.deinit();
    try server.start();
    defer server.stop();

    const base_uri = try server.baseUri(alloc);
    defer alloc.free(base_uri);

    var client_exec = raft_transport.StdHttpExecutor.init(alloc, .{});
    defer client_exec.deinit();
    var client = serverless_http_client.ServerlessHttpClient.init(alloc, client_exec.executor());
    const tables = client.tables();
    const internal = client.internal();

    var ensure = tables.ensure(base_uri, "docs", 100) catch |err| switch (err) {
        error.UnexpectedHttpStatus => null,
        else => return err,
    };
    if (ensure) |*resp| resp.deinit(alloc);
    try std.testing.expect(ensure == null);

    const mutations = [_]api_mod.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "alpha bravo" },
    };
    var ingest_before = tables.ingest(base_uri, .{
        .table_name = "docs",
        .timestamp_ns = 123,
        .mutations = &mutations,
    }) catch |err| switch (err) {
        error.UnexpectedHttpStatus => null,
        else => return err,
    };
    if (ingest_before) |*resp| resp.deinit(alloc);
    try std.testing.expect(ingest_before == null);

    var query_before = tables.queryPublished(base_uri, "docs") catch |err| switch (err) {
        error.UnexpectedHttpStatus => null,
        else => return err,
    };
    if (query_before) |*resp| resp.deinit();
    try std.testing.expect(query_before == null);

    try std.testing.expect(try server.stack.catalog.ensureTable("docs", 100));
    var ingest = try server.stack.api.ingestTableBatch(.{
        .table_name = "docs",
        .timestamp_ns = 123,
        .mutations = &mutations,
    });
    defer ingest.deinit(alloc);
    var build = try server.stack.catalog.buildTable("docs");
    defer build.deinit(alloc);
    try std.testing.expect(build.published);

    var build_err = internal.buildTable(base_uri, "docs") catch |err| switch (err) {
        error.UnexpectedHttpStatus => null,
        else => return err,
    };
    if (build_err) |*resp| resp.deinit(alloc);
    try std.testing.expect(build_err == null);

    var search = try tables.search(base_uri, "docs", .{
        .text = @constCast("bravo"),
        .limit = 5,
    });
    defer search.deinit();
    try std.testing.expectEqualStrings("docs", search.value.table_name);
    try std.testing.expectEqual(@as(usize, 1), search.value.hits.len);
    try std.testing.expectEqualStrings("doc-a", search.value.hits[0].doc_id);
}

test "serverless server public table routes preserve published and latest cutover semantics" {
    const alloc = std.testing.allocator;
    const serverless_http_client = @import("../serverless_http_client.zig");
    const api_mod = @import("api/mod.zig");

    var artifacts_buf: [256]u8 = undefined;
    var manifests_buf: [256]u8 = undefined;
    var wal_buf: [256]u8 = undefined;
    var progress_buf: [256]u8 = undefined;
    var catalog_buf: [256]u8 = undefined;
    const artifacts_root = tmpPath(&artifacts_buf, "server-cutover-artifacts");
    const manifests_root = tmpPath(&manifests_buf, "server-cutover-manifests");
    const wal_root = tmpPath(&wal_buf, "server-cutover-wal");
    const progress_root = tmpPath(&progress_buf, "server-cutover-progress");
    const catalog_root = tmpPath(&catalog_buf, "server-cutover-catalog");
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

    var server = try ServerlessServer.init(alloc, .{
        .bootstrap = .{
            .artifacts_uri = artifacts_uri,
            .manifests_uri = manifests_uri,
            .wal_uri = wal_uri,
            .progress_uri = progress_uri,
            .catalog_uri = catalog_uri,
            .query_cache_dir = null,
            .tick_interval_ms = 1,
            .role = .combined,
        },
        .listener = .{},
    });
    defer server.deinit();
    try server.owned_listener.?.start();
    defer server.owned_listener.?.stop();

    const base_uri = try server.baseUri(alloc);
    defer alloc.free(base_uri);

    var client_exec = raft_transport.StdHttpExecutor.init(alloc, .{});
    defer client_exec.deinit();
    var client = serverless_http_client.ServerlessHttpClient.init(alloc, client_exec.executor());
    const tables = client.tables();
    const internal = client.internal();

    var ensure = try tables.ensureWithPolicy(base_uri, "docs", .{
        .created_at_ns = 100,
        .policy = .{
            .default_query_view = .published,
        },
    });
    defer ensure.deinit(alloc);
    try std.testing.expect(ensure.created);

    const initial_mutations = [_]api_mod.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "alpha" },
    };
    var ingest_initial = try tables.ingest(base_uri, .{
        .table_name = "docs",
        .timestamp_ns = 123,
        .mutations = &initial_mutations,
    });
    defer ingest_initial.deinit(alloc);
    try std.testing.expectEqualStrings("docs", ingest_initial.table_name);

    var build_v1 = try internal.buildTable(base_uri, "docs");
    defer build_v1.deinit(alloc);
    try std.testing.expect(build_v1.published);

    try waitForPublishedTable(tables, base_uri, "docs", 1, 250);

    var published_v1 = try tables.queryPublished(base_uri, "docs");
    defer published_v1.deinit();
    try std.testing.expectEqual(@as(u64, 1), published_v1.value.version);
    try std.testing.expectEqual(@as(usize, 1), published_v1.value.documents.len);
    try std.testing.expect(hasDocId(published_v1.value.documents, "doc-a"));
    try std.testing.expect(!hasDocId(published_v1.value.documents, "doc-b"));
    try std.testing.expectEqual(@as(usize, 0), published_v1.value.overlay_mutation_count);

    const next_mutations = [_]api_mod.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-b", .body = "beta" },
    };
    var ingest_next = try tables.ingest(base_uri, .{
        .table_name = "docs",
        .timestamp_ns = 124,
        .mutations = &next_mutations,
    });
    defer ingest_next.deinit(alloc);

    var status_before_cutover = try internal.buildTableStatus(base_uri, "docs");
    defer status_before_cutover.deinit(alloc);
    try std.testing.expectEqualStrings("docs", status_before_cutover.table_name);
    try std.testing.expectEqual(@as(u64, 1), status_before_cutover.pending_records);
    try std.testing.expectEqual(@as(u64, 1), status_before_cutover.freshness_lag_records);

    var published_before_cutover = try tables.queryPublished(base_uri, "docs");
    defer published_before_cutover.deinit();
    try std.testing.expectEqual(@as(u64, 1), published_before_cutover.value.version);
    try std.testing.expectEqual(@as(usize, 1), published_before_cutover.value.documents.len);
    try std.testing.expect(!hasDocId(published_before_cutover.value.documents, "doc-b"));
    try std.testing.expectEqual(@as(usize, 0), published_before_cutover.value.overlay_mutation_count);

    var latest_before_cutover = try tables.queryLatest(base_uri, "docs");
    defer latest_before_cutover.deinit();
    try std.testing.expectEqualStrings("docs", latest_before_cutover.value.table_name);
    try std.testing.expectEqual(@as(usize, 2), latest_before_cutover.value.documents.len);
    try std.testing.expect(hasDocId(latest_before_cutover.value.documents, "doc-a"));
    try std.testing.expect(hasDocId(latest_before_cutover.value.documents, "doc-b"));
    try std.testing.expectEqual(@as(usize, 1), latest_before_cutover.value.overlay_mutation_count);

    var build_v2 = try internal.buildTable(base_uri, "docs");
    defer build_v2.deinit(alloc);
    try std.testing.expect(build_v2.published);

    try waitForPublishedTable(tables, base_uri, "docs", 2, 250);

    var published_v2 = try tables.queryPublished(base_uri, "docs");
    defer published_v2.deinit();
    try std.testing.expectEqual(@as(u64, 2), published_v2.value.version);
    try std.testing.expectEqual(@as(usize, 2), published_v2.value.documents.len);
    try std.testing.expect(hasDocId(published_v2.value.documents, "doc-a"));
    try std.testing.expect(hasDocId(published_v2.value.documents, "doc-b"));
    try std.testing.expectEqual(@as(usize, 0), published_v2.value.overlay_mutation_count);

    var search = try tables.search(base_uri, "docs", .{
        .text = @constCast("beta"),
        .limit = 5,
    });
    defer search.deinit();
    try std.testing.expectEqualStrings("docs", search.value.table_name);
    try std.testing.expectEqual(@as(u64, 2), search.value.version);
    try std.testing.expectEqual(@as(usize, 1), search.value.hits.len);
    try std.testing.expectEqualStrings("doc-b", search.value.hits[0].doc_id);
}

test "serverless server hides remapped serving namespaces behind public table routes" {
    const alloc = std.testing.allocator;
    const serverless_http_client = @import("../serverless_http_client.zig");
    const api_mod = @import("api/mod.zig");

    var artifacts_buf: [256]u8 = undefined;
    var manifests_buf: [256]u8 = undefined;
    var wal_buf: [256]u8 = undefined;
    var progress_buf: [256]u8 = undefined;
    var catalog_buf: [256]u8 = undefined;
    const artifacts_root = tmpPath(&artifacts_buf, "server-remap-artifacts");
    const manifests_root = tmpPath(&manifests_buf, "server-remap-manifests");
    const wal_root = tmpPath(&wal_buf, "server-remap-wal");
    const progress_root = tmpPath(&progress_buf, "server-remap-progress");
    const catalog_root = tmpPath(&catalog_buf, "server-remap-catalog");
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

    var server = try ServerlessServer.init(alloc, .{
        .bootstrap = .{
            .artifacts_uri = artifacts_uri,
            .manifests_uri = manifests_uri,
            .wal_uri = wal_uri,
            .progress_uri = progress_uri,
            .catalog_uri = catalog_uri,
            .query_cache_dir = null,
            .tick_interval_ms = 1,
            .role = .combined,
        },
        .listener = .{},
    });
    defer server.deinit();

    try std.testing.expect(try server.stack.catalog_store.ensureTable("docs", "docs-serving", 100, .{
        .default_query_view = .published,
        .keep_latest_versions = 3,
    }, "", "", "{}"));
    const resolved_namespace = try server.stack.catalog.resolveTableNamespaceAlloc("docs");
    defer alloc.free(resolved_namespace);
    try std.testing.expectEqualStrings("docs-serving", resolved_namespace);

    try server.owned_listener.?.start();
    defer server.owned_listener.?.stop();

    const base_uri = try server.baseUri(alloc);
    defer alloc.free(base_uri);

    var client_exec = raft_transport.StdHttpExecutor.init(alloc, .{});
    defer client_exec.deinit();
    var client = serverless_http_client.ServerlessHttpClient.init(alloc, client_exec.executor());
    const tables = client.tables();
    const internal = client.internal();

    var listed = try tables.list(base_uri);
    defer listed.deinit();
    try std.testing.expectEqual(@as(usize, 1), listed.value.len);
    try std.testing.expectEqualStrings("docs", listed.value[0].table_name);

    var policy = try internal.fetchTablePolicy(base_uri, "docs");
    defer policy.deinit(alloc);
    try std.testing.expectEqualStrings("docs", policy.table_name);
    try std.testing.expectEqual(.published, policy.policy.default_query_view);
    try std.testing.expectEqual(@as(usize, 3), policy.policy.keep_latest_versions);

    var updated_policy = try internal.updateTablePolicy(base_uri, "docs", .{
        .default_query_view = .latest,
        .keep_latest_versions = 2,
    });
    defer updated_policy.deinit(alloc);
    try std.testing.expectEqualStrings("docs", updated_policy.table_name);
    try std.testing.expectEqual(.latest, updated_policy.policy.default_query_view);
    try std.testing.expectEqual(@as(usize, 2), updated_policy.policy.keep_latest_versions);

    const mutations = [_]api_mod.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "alpha" },
    };
    var ingest = try tables.ingest(base_uri, .{
        .table_name = "docs",
        .timestamp_ns = 456,
        .mutations = &mutations,
    });
    defer ingest.deinit(alloc);
    try std.testing.expectEqualStrings("docs", ingest.table_name);

    var build = try internal.buildTable(base_uri, "docs");
    defer build.deinit(alloc);
    try std.testing.expectEqualStrings("docs", build.table_name);

    try waitForPublishedTable(tables, base_uri, "docs", 1, 250);

    var status = try internal.buildTableStatus(base_uri, "docs");
    defer status.deinit(alloc);
    try std.testing.expectEqualStrings("docs", status.table_name);
    try std.testing.expectEqual(@as(u64, 1), status.head_version);
    try std.testing.expectEqual(@as(u64, 0), status.pending_records);

    var published = try tables.queryPublished(base_uri, "docs");
    defer published.deinit();
    try std.testing.expectEqualStrings("docs", published.value.table_name);
    try std.testing.expectEqual(@as(u64, 1), published.value.version);
    try std.testing.expectEqual(@as(usize, 1), published.value.documents.len);
    try std.testing.expect(hasDocId(published.value.documents, "doc-a"));

    var search = try tables.search(base_uri, "docs", .{
        .text = @constCast("alpha"),
        .limit = 5,
    });
    defer search.deinit();
    try std.testing.expectEqualStrings("docs", search.value.table_name);
    try std.testing.expectEqual(@as(usize, 1), search.value.hits.len);
    try std.testing.expectEqualStrings("doc-a", search.value.hits[0].doc_id);

    const later_mutations = [_]api_mod.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-b", .body = "beta" },
    };
    var later_ingest = try tables.ingest(base_uri, .{
        .table_name = "docs",
        .timestamp_ns = 457,
        .mutations = &later_mutations,
    });
    defer later_ingest.deinit(alloc);

    var pending_status = try internal.buildTableStatus(base_uri, "docs");
    defer pending_status.deinit(alloc);
    try std.testing.expectEqualStrings("docs", pending_status.table_name);
    try std.testing.expectEqual(@as(u64, 1), pending_status.head_version);
    try std.testing.expectEqual(@as(u64, 1), pending_status.pending_records);

    var default_query = try tables.query(base_uri, "docs");
    defer default_query.deinit();
    try std.testing.expectEqualStrings("docs", default_query.value.table_name);
    try std.testing.expectEqual(@as(u64, 1), default_query.value.version);
    try std.testing.expectEqual(.latest, default_query.value.view);
    try std.testing.expectEqual(@as(usize, 2), default_query.value.documents.len);
    try std.testing.expectEqual(@as(usize, 1), default_query.value.overlay_mutation_count);
    try std.testing.expect(hasDocId(default_query.value.documents, "doc-b"));
}

test "serverless server public table graph routes stay pinned until publish cutover" {
    const alloc = std.testing.allocator;
    const serverless_http_client = @import("../serverless_http_client.zig");
    const api_mod = @import("api/mod.zig");

    var artifacts_buf: [256]u8 = undefined;
    var manifests_buf: [256]u8 = undefined;
    var wal_buf: [256]u8 = undefined;
    var progress_buf: [256]u8 = undefined;
    var catalog_buf: [256]u8 = undefined;
    const artifacts_root = tmpPath(&artifacts_buf, "server-graph-artifacts");
    const manifests_root = tmpPath(&manifests_buf, "server-graph-manifests");
    const wal_root = tmpPath(&wal_buf, "server-graph-wal");
    const progress_root = tmpPath(&progress_buf, "server-graph-progress");
    const catalog_root = tmpPath(&catalog_buf, "server-graph-catalog");
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

    var server = try ServerlessServer.init(alloc, .{
        .bootstrap = .{
            .artifacts_uri = artifacts_uri,
            .manifests_uri = manifests_uri,
            .wal_uri = wal_uri,
            .progress_uri = progress_uri,
            .catalog_uri = catalog_uri,
            .query_cache_dir = null,
            .tick_interval_ms = 1,
            .role = .combined,
        },
        .listener = .{},
    });
    defer server.deinit();
    try server.owned_listener.?.start();
    defer server.owned_listener.?.stop();

    const base_uri = try server.baseUri(alloc);
    defer alloc.free(base_uri);

    var client_exec = raft_transport.StdHttpExecutor.init(alloc, .{});
    defer client_exec.deinit();
    var client = serverless_http_client.ServerlessHttpClient.init(alloc, client_exec.executor());
    const tables = client.tables();
    const internal = client.internal();

    var ensure = try tables.ensureWithPolicy(base_uri, "docs", .{
        .created_at_ns = 100,
        .policy = .{
            .default_query_view = .published,
        },
    });
    defer ensure.deinit(alloc);
    try std.testing.expect(ensure.created);

    const initial_mutations = [_]api_mod.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "{\"text\":\"alpha\",\"graph_edges\":[{\"target\":\"doc-b\",\"edge_type\":\"cites\",\"weight\":1.0}]}" },
        .{ .kind = .upsert, .doc_id = "doc-b", .body = "{\"text\":\"beta\"}" },
        .{ .kind = .upsert, .doc_id = "doc-c", .body = "{\"text\":\"gamma\"}" },
    };
    var ingest_initial = try tables.ingest(base_uri, .{
        .table_name = "docs",
        .timestamp_ns = 200,
        .mutations = &initial_mutations,
    });
    defer ingest_initial.deinit(alloc);

    var build_v1 = try internal.buildTable(base_uri, "docs");
    defer build_v1.deinit(alloc);
    try std.testing.expect(build_v1.published);

    try waitForPublishedTable(tables, base_uri, "docs", 1, 250);

    var neighbors_v1 = try tables.graphNeighbors(base_uri, "docs", .{
        .doc_id = @constCast("doc-a"),
        .direction = .out,
        .limit = 10,
    });
    defer neighbors_v1.deinit();
    try std.testing.expectEqualStrings("docs", neighbors_v1.value.table_name);
    try std.testing.expectEqual(@as(u64, 1), neighbors_v1.value.version);
    try std.testing.expectEqual(@as(usize, 1), neighbors_v1.value.neighbors.len);
    try std.testing.expectEqualStrings("doc-b", neighbors_v1.value.neighbors[0].doc_id);

    var shortest_before = try tables.graphShortestPath(base_uri, "docs", .{
        .start_doc_id = @constCast("doc-a"),
        .end_doc_id = @constCast("doc-c"),
        .direction = .out,
        .edge_types = &.{@constCast("cites")},
        .max_depth = 4,
    });
    defer shortest_before.deinit();
    try std.testing.expectEqualStrings("docs", shortest_before.value.table_name);
    try std.testing.expectEqual(@as(u64, 1), shortest_before.value.version);
    try std.testing.expect(!shortest_before.value.found);

    const next_mutations = [_]api_mod.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-b", .body = "{\"text\":\"beta\",\"graph_edges\":[{\"target\":\"doc-c\",\"edge_type\":\"cites\",\"weight\":2.0}]}" },
    };
    var ingest_next = try tables.ingest(base_uri, .{
        .table_name = "docs",
        .timestamp_ns = 201,
        .mutations = &next_mutations,
    });
    defer ingest_next.deinit(alloc);

    var shortest_still_published = try tables.graphShortestPath(base_uri, "docs", .{
        .start_doc_id = @constCast("doc-a"),
        .end_doc_id = @constCast("doc-c"),
        .direction = .out,
        .edge_types = &.{@constCast("cites")},
        .max_depth = 4,
    });
    defer shortest_still_published.deinit();
    try std.testing.expectEqual(@as(u64, 1), shortest_still_published.value.version);
    try std.testing.expect(!shortest_still_published.value.found);

    var build_v2 = try internal.buildTable(base_uri, "docs");
    defer build_v2.deinit(alloc);
    try std.testing.expect(build_v2.published);

    try waitForPublishedTable(tables, base_uri, "docs", 2, 250);

    var shortest_after = try tables.graphShortestPath(base_uri, "docs", .{
        .start_doc_id = @constCast("doc-a"),
        .end_doc_id = @constCast("doc-c"),
        .direction = .out,
        .edge_types = &.{@constCast("cites")},
        .max_depth = 4,
    });
    defer shortest_after.deinit();
    try std.testing.expectEqualStrings("docs", shortest_after.value.table_name);
    try std.testing.expectEqual(@as(u64, 2), shortest_after.value.version);
    try std.testing.expect(shortest_after.value.found);
    try std.testing.expectEqual(@as(?usize, 3), if (shortest_after.value.node_path) |path| path.len else null);
    try std.testing.expectEqualStrings("doc-b", shortest_after.value.node_path.?[1]);

    var traverse_after = try tables.graphTraverse(base_uri, "docs", .{
        .start_doc_id = @constCast("doc-a"),
        .direction = .out,
        .edge_types = &.{@constCast("cites")},
        .max_depth = 3,
        .limit = 10,
        .include_start = true,
    });
    defer traverse_after.deinit();
    try std.testing.expectEqualStrings("docs", traverse_after.value.table_name);
    try std.testing.expectEqual(@as(u64, 2), traverse_after.value.version);
    try std.testing.expectEqual(@as(usize, 3), traverse_after.value.nodes.len);
    try std.testing.expectEqualStrings("doc-c", traverse_after.value.nodes[2].doc_id);
}

test "serverless server serves requests over env-configured s3 backend" {
    const alloc = std.testing.allocator;
    try test_backend.requireEnabled(.s3);

    const bucket = try test_backend.requiredBucketOwned(alloc, .s3);
    defer alloc.free(bucket);

    const prefix_root = try std.fmt.allocPrint(alloc, "serverless-e2e/{d}", .{test_backend.integrationNonce()});
    defer alloc.free(prefix_root);

    var uris = try test_backend.makeNamespaceUris(alloc, .s3, bucket, prefix_root);
    defer uris.deinit(alloc);

    var server = try ServerlessServer.init(alloc, .{
        .bootstrap = .{
            .artifacts_uri = uris.artifacts,
            .manifests_uri = uris.manifests,
            .wal_uri = uris.wal,
            .progress_uri = uris.progress,
            .catalog_uri = uris.catalog,
            .query_cache_dir = null,
            .tick_interval_ms = 5,
            .role = .combined,
        },
        .listener = .{},
    });
    defer server.deinit();
    try server.start();
    defer server.stop();

    const base_uri = try server.baseUri(alloc);
    defer alloc.free(base_uri);

    var client_exec = raft_transport.StdHttpExecutor.init(alloc, .{});
    defer client_exec.deinit();
    var client = @import("../serverless_http_client.zig").ServerlessHttpClient.init(alloc, client_exec.executor());
    const tables = client.tables();

    var ensure = try tables.ensure(base_uri, "docs", 100);
    defer ensure.deinit(alloc);
    try std.testing.expect(ensure.created);

    const mutations = [_]@import("api/mod.zig").DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-s3", .body = "bravo" },
    };
    var ingest = try tables.ingest(base_uri, .{
        .table_name = "docs",
        .timestamp_ns = 456,
        .mutations = &mutations,
    });
    defer ingest.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), ingest.start_lsn);

    try waitForPublishedTable(tables, base_uri, "docs", 1, 400);

    var query = try tables.queryPublished(base_uri, "docs");
    defer query.deinit();
    try std.testing.expectEqual(@as(u64, 1), query.value.version);
    try std.testing.expectEqualStrings("docs", query.value.table_name);
    try std.testing.expectEqual(@as(usize, 1), query.value.documents.len);
    try std.testing.expectEqualStrings("doc-s3", query.value.documents[0].doc_id);
    try std.testing.expectEqualStrings("bravo", query.value.documents[0].body);
}

test "serverless server serves requests over env-configured gs backend" {
    const alloc = std.testing.allocator;
    try test_backend.requireEnabled(.gs);

    const bucket = try test_backend.requiredBucketOwned(alloc, .gs);
    defer alloc.free(bucket);

    const prefix_root = try std.fmt.allocPrint(alloc, "serverless-gcs-e2e/{d}", .{test_backend.integrationNonce()});
    defer alloc.free(prefix_root);

    var uris = try test_backend.makeNamespaceUris(alloc, .gs, bucket, prefix_root);
    defer uris.deinit(alloc);

    var server = try ServerlessServer.init(alloc, .{
        .bootstrap = .{
            .artifacts_uri = uris.artifacts,
            .manifests_uri = uris.manifests,
            .wal_uri = uris.wal,
            .progress_uri = uris.progress,
            .catalog_uri = uris.catalog,
            .query_cache_dir = null,
            .tick_interval_ms = 5,
            .role = .combined,
        },
        .listener = .{},
    });
    defer server.deinit();
    try server.start();
    defer server.stop();

    const base_uri = try server.baseUri(alloc);
    defer alloc.free(base_uri);

    var client_exec = raft_transport.StdHttpExecutor.init(alloc, .{});
    defer client_exec.deinit();
    var client = @import("../serverless_http_client.zig").ServerlessHttpClient.init(alloc, client_exec.executor());
    const tables = client.tables();

    var ensure = try tables.ensure(base_uri, "docs", 100);
    defer ensure.deinit(alloc);
    try std.testing.expect(ensure.created);

    const mutations = [_]@import("api/mod.zig").DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-gs", .body = "charlie" },
    };
    var ingest = try tables.ingest(base_uri, .{
        .table_name = "docs",
        .timestamp_ns = 789,
        .mutations = &mutations,
    });
    defer ingest.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), ingest.start_lsn);

    try waitForPublishedTable(tables, base_uri, "docs", 1, 400);

    var query = try tables.queryPublished(base_uri, "docs");
    defer query.deinit();
    try std.testing.expectEqual(@as(u64, 1), query.value.version);
    try std.testing.expectEqualStrings("docs", query.value.table_name);
    try std.testing.expectEqual(@as(usize, 1), query.value.documents.len);
    try std.testing.expectEqualStrings("doc-gs", query.value.documents[0].doc_id);
    try std.testing.expectEqualStrings("charlie", query.value.documents[0].body);
}

fn waitForPublishedTable(
    tables: @import("../serverless_http_client.zig").ServerlessTableHttpClient,
    base_uri: []const u8,
    table_name: []const u8,
    expected_version: u64,
    attempts: usize,
) !void {
    var remaining = attempts;
    while (remaining > 0) : (remaining -= 1) {
        var parsed = tables.queryPublished(base_uri, table_name) catch |err| switch (err) {
            error.UnexpectedHttpStatus => {
                sleepMs(5);
                continue;
            },
            else => return err,
        };
        defer parsed.deinit();
        if (parsed.value.version == expected_version) return;
        sleepMs(5);
    }
    return error.ExpectedPublishedVersionUnavailable;
}

fn hasDocId(items: anytype, doc_id: []const u8) bool {
    for (items) |item| {
        if (std.mem.eql(u8, item.doc_id, doc_id)) return true;
    }
    return false;
}

var test_nonce: std.atomic.Value(u64) = .init(0);

fn threadedIo() std.Io.Threaded {
    return std.Io.Threaded.init(std.heap.page_allocator, .{});
}

fn nowNs() u64 {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    const now = std.Io.Timestamp.now(io_impl.io(), .awake);
    return @intCast(now.toNanoseconds());
}

fn tmpPath(buf: []u8, label: []const u8) [*:0]const u8 {
    const nonce = test_nonce.fetchAdd(1, .monotonic);
    const slice = std.fmt.bufPrint(buf, "/tmp/antfly-serverless-server-{s}-{d}-{d}\x00", .{
        label,
        nowNs(),
        nonce,
    }) catch unreachable;
    return @ptrCast(slice.ptr);
}

fn cleanupTmp(path: [*:0]const u8) void {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), std.mem.span(path)) catch {};
}

fn sleepMs(ms: u64) void {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    std.Io.Clock.Duration.sleep(.{
        .clock = .awake,
        .raw = .fromMilliseconds(@intCast(if (ms == 0) @as(u64, 1) else ms)),
    }, io_impl.io()) catch {};
}

fn envEnabled(comptime env_name: []const u8) bool {
    const value_z = std.c.getenv(env_name ++ "\x00") orelse return false;
    const value = std.mem.span(value_z);
    return value.len > 0 and !std.mem.eql(u8, value, "0") and !std.mem.eql(u8, value, "false");
}

fn envRequiredOwned(alloc: std.mem.Allocator, env_name: []const u8) ![]u8 {
    const env_name_z = try alloc.dupeZ(u8, env_name);
    defer alloc.free(env_name_z);
    const value_z = std.c.getenv(env_name_z.ptr) orelse return error.MissingEnvironmentVariable;
    return try alloc.dupe(u8, std.mem.span(value_z));
}

fn integrationNonce() u64 {
    return nowNs() + test_nonce.fetchAdd(1, .monotonic);
}
