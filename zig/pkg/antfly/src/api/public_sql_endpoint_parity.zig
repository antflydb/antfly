// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

const std = @import("std");

const api_http_server = @import("http_server.zig");
const catalog_resources = @import("catalog_resources.zig");
const db_mod = @import("../storage/db/mod.zig");
const distributed_txn = @import("distributed_txn.zig");
const http_common = @import("../raft/transport/http_common.zig");
const metadata_api = @import("../metadata/api.zig");
const metadata_openapi = @import("antfly_metadata_openapi");
const metadata_table_manager = @import("../metadata/table_manager.zig");
const query_api = @import("query.zig");
const raft_mod = @import("../raft/mod.zig");
const runtime_schema_mod = @import("../storage/schema.zig");
const sql_adapter = @import("../sql/mod.zig");
const table_reads = @import("table_reads.zig");
const table_writes = @import("table_writes.zig");
const tables_api = @import("../metadata/catalog/table_ddl.zig");
const test_contract_helpers = @import("test_contract_helpers.zig");
const usermgr = @import("../usermgr/mod.zig");

const ApiHttpServer = api_http_server.ApiHttpServer;
const AuthenticatedIdentity = api_http_server.AuthenticatedIdentity;
const StatusSource = api_http_server.StatusSource;

fn publicSqlBodyAlloc(alloc: std.mem.Allocator, sql: []const u8) ![]u8 {
    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    try out.writer.print("{{\"sql\":{f}}}", .{std.json.fmt(sql, .{})});
    return try out.toOwnedSlice();
}

fn handlePublicSqlEndpoint(server: *ApiHttpServer, alloc: std.mem.Allocator, sql: []const u8) !http_common.HttpResponse {
    const body = try publicSqlBodyAlloc(alloc, sql);
    defer alloc.free(body);
    return try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = body,
    });
}

fn handlePublicSqlDirect(server: *ApiHttpServer, alloc: std.mem.Allocator, sql: []const u8, identity: ?AuthenticatedIdentity) !http_common.HttpResponse {
    const body = try publicSqlBodyAlloc(alloc, sql);
    defer alloc.free(body);
    return try server.handlePublicSql(body, identity);
}

fn documentKeyScanPayloadAlloc(alloc: std.mem.Allocator, key_prefix: []const u8, count: usize) ![]u8 {
    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    var index: usize = 0;
    while (index < count) : (index += 1) {
        try out.writer.print("{{\"key\":\"{s}{d}\"}}\n", .{ key_prefix, index });
    }
    return try out.toOwnedSlice();
}

fn expectPublicSqlDiagnosticBody(
    alloc: std.mem.Allocator,
    body: []const u8,
    expected_phase: []const u8,
    expected_code: []const u8,
    expected_message: []const u8,
    expected_span_start: ?i64,
    expected_span_end: ?i64,
) !void {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{ .allocate = .alloc_always });
    defer parsed.deinit();
    const diagnostic = parsed.value.object.get("error") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings(expected_phase, diagnostic.object.get("phase").?.string);
    try std.testing.expectEqualStrings(expected_code, diagnostic.object.get("code").?.string);
    try std.testing.expectEqualStrings(expected_message, diagnostic.object.get("message").?.string);
    const span = diagnostic.object.get("span").?.object;
    if (expected_span_start) |span_start| try std.testing.expectEqual(span_start, span.get("start").?.integer);
    if (expected_span_end) |span_end| try std.testing.expectEqual(span_end, span.get("end").?.integer);
}

fn expectPublicSqlDiagnosticMissingNativeModel(
    alloc: std.mem.Allocator,
    body: []const u8,
    expected_missing_native_model: []const u8,
) !void {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{ .allocate = .alloc_always });
    defer parsed.deinit();
    const diagnostic = parsed.value.object.get("error") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings(expected_missing_native_model, diagnostic.object.get("missing_native_model").?.string);
}

fn expectDocumentJoinedWriteProofDiagnostic(
    alloc: std.mem.Allocator,
    schema_json: []const u8,
    path_suffix: []const u8,
    sql: []const u8,
    expected_code: []const u8,
    expected_message: []const u8,
) !void {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/{s}", .{ tmp.sub_path, path_suffix });
    defer alloc.free(path);

    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();
    try db.applyTableSchemaJson(alloc, schema_json, .{});

    var read_source = table_reads.BoundTableReadSource.init("docs", 1, &db, raft_mod.read_gate.noopReadableLeaseRequester());
    var write_source = table_writes.BoundTableWriteSource.init("docs", &db);

    const FakeSource = struct {
        tables: [1]metadata_table_manager.TableRecord,

        fn iface(self: *@This()) StatusSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{
                .status = try status(ptr),
                .tables = self.tables[0..],
                .ranges = &.{},
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = FakeSource{ .tables = .{.{
        .table_id = 1,
        .name = "docs",
        .schema_json = schema_json,
        .desired_replica_count = 1,
    }} };
    var server = ApiHttpServer.init(alloc, .{}, source.iface(), read_source.source(), write_source.source());
    defer server.deinit();

    var resp = try handlePublicSqlEndpoint(&server, alloc, sql);
    defer resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 400), resp.status);
    try expectPublicSqlDiagnosticBody(alloc, resp.body, "plan", expected_code, expected_message, null, null);
}

test "api public SQL endpoint admits document truncate table emptying barrier" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"document","default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"title":{"type":"text"},"status":{"type":"keyword"}},"additionalProperties":true}}}}
    ;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/public-sql-document-truncate", .{tmp.sub_path});
    defer alloc.free(path);

    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();
    try db.applyTableSchemaJson(alloc, schema_json, .{});
    try db.batch(.{ .writes = &.{
        .{ .key = "doc:truncate-a", .value = "{\"title\":\"alpha\",\"status\":\"drop\"}" },
        .{ .key = "doc:truncate-b", .value = "{\"title\":\"beta\",\"status\":\"drop\"}" },
    } });

    var read_source = table_reads.BoundTableReadSource.init("docs", 1, &db, raft_mod.read_gate.noopReadableLeaseRequester());
    var write_source = table_writes.BoundTableWriteSource.init("docs", &db);

    const FakeSource = struct {
        tables: [1]metadata_table_manager.TableRecord,
        ranges: [1]metadata_table_manager.RangeRecord,
        table_emptying_jobs: std.ArrayListUnmanaged(metadata_table_manager.TableEmptyingJobRecord) = .empty,

        fn deinit(self: *@This(), allocator: std.mem.Allocator) void {
            for (self.table_emptying_jobs.items) |record| metadata_table_manager.freeTableEmptyingJob(allocator, record);
            self.table_emptying_jobs.deinit(allocator);
        }

        fn iface(self: *@This()) StatusSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                    .upsert_table_emptying_job = upsertTableEmptyingJob,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{
                .status = try status(ptr),
                .tables = self.tables[0..],
                .ranges = self.ranges[0..],
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}

        fn upsertTableEmptyingJob(ptr: *anyopaque, record: metadata_table_manager.TableEmptyingJobRecord) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const owned = try metadata_table_manager.cloneTableEmptyingJob(std.testing.allocator, record);
            errdefer metadata_table_manager.freeTableEmptyingJob(std.testing.allocator, owned);
            try self.table_emptying_jobs.append(std.testing.allocator, owned);
        }
    };

    const FakePublicSqlAudit = struct {
        records: [4]api_http_server.PublicSqlAuditRecord = undefined,
        record_count: usize = 0,

        fn sink(self: *@This()) api_http_server.PublicSqlAuditSink {
            return .{
                .ptr = self,
                .record = record,
            };
        }

        fn record(ptr: *anyopaque, event: api_http_server.PublicSqlAuditRecord) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.record_count >= self.records.len) return error.TooManyAuditRecords;
            self.records[self.record_count] = event;
            self.record_count += 1;
        }
    };

    var source = FakeSource{
        .tables = .{.{
            .table_id = 1,
            .name = "docs",
            .schema_json = schema_json,
            .desired_replica_count = 1,
        }},
        .ranges = .{.{ .group_id = 11, .range_id = 101, .table_id = 1, .start_key = "", .end_key = null }},
    };
    defer source.deinit(alloc);
    var audit = FakePublicSqlAudit{};
    var server = ApiHttpServer.init(alloc, .{ .public_sql_audit_sink = audit.sink() }, source.iface(), read_source.source(), write_source.source());
    defer server.deinit();

    var row_filters = try alloc.alloc(usermgr.RowFilterEntry, 1);
    row_filters[0] = try usermgr.RowFilterEntry.initOwned(alloc, "docs", "{\"term\":{\"status\":\"drop\"}}");
    var filtered_identity = AuthenticatedIdentity{
        .username = try alloc.dupe(u8, "document_sql_truncate_filtered"),
        .row_filter = row_filters,
    };
    defer filtered_identity.deinit(alloc);
    var filtered_truncate_resp = try handlePublicSqlDirect(&server, alloc, "TRUNCATE docs;", filtered_identity);
    defer filtered_truncate_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 403), filtered_truncate_resp.status);
    try std.testing.expectEqualStrings("row filter pushdown required", filtered_truncate_resp.body);
    try std.testing.expectEqual(@as(usize, 0), source.table_emptying_jobs.items.len);
    try std.testing.expectEqual(@as(usize, 1), audit.record_count);
    try std.testing.expectEqual(api_http_server.PublicSqlAuditOutcome.denied, audit.records[0].outcome);
    try std.testing.expectEqual(sql_adapter.SqlWriteStatementKind.truncate, audit.records[0].statement_kind);
    try std.testing.expectEqual(api_http_server.PublicSqlNativeRoute.table_emptying_barrier, audit.records[0].native_route);
    try std.testing.expectEqual(usermgr.ResourceType.table, audit.records[0].required_resource_type);
    try std.testing.expectEqual(usermgr.PermissionType.write, audit.records[0].required_permission);
    try std.testing.expectEqual(@as(usize, 0), audit.records[0].row_count);
    try std.testing.expectEqualStrings("antfly", audit.records[0].target.database_name);
    try std.testing.expectEqualStrings("public", audit.records[0].target.namespace_name);
    try std.testing.expectEqualStrings("docs", audit.records[0].target.table_name);
    try std.testing.expectEqualStrings("document_sql_truncate_filtered", audit.records[0].authenticated_subject orelse return error.TestUnexpectedResult);
    try std.testing.expectEqualStrings("RowFilterPushdownRequired", audit.records[0].error_name orelse return error.TestUnexpectedResult);

    var truncate_resp = try handlePublicSqlEndpoint(&server, alloc, "TRUNCATE docs;");
    defer truncate_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), truncate_resp.status);
    var parsed_truncate = try std.json.parseFromSlice(std.json.Value, alloc, truncate_resp.body, .{ .allocate = .alloc_always });
    defer parsed_truncate.deinit();
    try std.testing.expectEqualStrings("ddl", parsed_truncate.value.object.get("kind").?.string);
    try std.testing.expectEqualStrings("truncate", parsed_truncate.value.object.get("statement_kind").?.string);
    try std.testing.expectEqual(@as(usize, 1), source.table_emptying_jobs.items.len);
    try std.testing.expectEqual(@as(u64, 1), source.table_emptying_jobs.items[0].table_id);
    try std.testing.expectEqual(@as(u64, 11), source.table_emptying_jobs.items[0].group_id);
    try std.testing.expect(!source.table_emptying_jobs.items[0].restart_identity);
    try std.testing.expect(!source.table_emptying_jobs.items[0].cascade);
    try std.testing.expectEqual(@as(usize, 1), source.table_emptying_jobs.items[0].affected_table_ids.len);
    try std.testing.expectEqual(@as(u64, 1), source.table_emptying_jobs.items[0].affected_table_ids[0]);
    try std.testing.expectEqual(@as(usize, 2), audit.record_count);
    try std.testing.expectEqual(api_http_server.PublicSqlAuditOutcome.applied, audit.records[1].outcome);
    try std.testing.expectEqual(sql_adapter.SqlWriteStatementKind.truncate, audit.records[1].statement_kind);
    try std.testing.expectEqual(api_http_server.PublicSqlNativeRoute.table_emptying_barrier, audit.records[1].native_route);
    try std.testing.expectEqual(usermgr.ResourceType.table, audit.records[1].required_resource_type);
    try std.testing.expectEqual(usermgr.PermissionType.write, audit.records[1].required_permission);
    try std.testing.expectEqual(@as(usize, 1), audit.records[1].row_count);
    try std.testing.expectEqualStrings("antfly", audit.records[1].target.database_name);
    try std.testing.expectEqualStrings("public", audit.records[1].target.namespace_name);
    try std.testing.expectEqualStrings("docs", audit.records[1].target.table_name);
    try std.testing.expect(audit.records[1].authenticated_subject == null);
    try std.testing.expect(audit.records[1].error_name == null);
}

test "api public SQL endpoint rejects unsupported document truncate variants" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"document","default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"title":{"type":"text"},"status":{"type":"keyword"}},"additionalProperties":true}}}}
    ;

    const cases = [_]struct {
        path_suffix: []const u8,
        sql: []const u8,
    }{
        .{
            .path_suffix = "public-sql-document-truncate-multi-table",
            .sql = "TRUNCATE docs, docs_archive;",
        },
        .{
            .path_suffix = "public-sql-document-truncate-restart-identity",
            .sql = "TRUNCATE docs RESTART IDENTITY;",
        },
        .{
            .path_suffix = "public-sql-document-truncate-cascade",
            .sql = "TRUNCATE docs CASCADE;",
        },
    };
    for (cases) |case| {
        try expectDocumentJoinedWriteProofDiagnostic(
            alloc,
            schema_json,
            case.path_suffix,
            case.sql,
            "document_sql_write_unsupported",
            "document_sql_write_unsupported",
        );
    }
}

test "api public SQL endpoint executes document merge matched branches" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"document","default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"title":{"type":"text"},"note":{"type":"text"},"status":{"type":"keyword"}},"additionalProperties":true}}}}
    ;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/public-sql-document-merge", .{tmp.sub_path});
    defer alloc.free(path);

    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();
    try db.applyTableSchemaJson(alloc, schema_json, .{});
    try db.batch(.{ .writes = &.{
        .{ .key = "doc:merge-update", .value = "{\"title\":\"copied title\",\"note\":\"old note\",\"status\":\"ready\"}" },
        .{ .key = "doc:merge-delete", .value = "{\"title\":\"delete title\",\"note\":\"delete note\",\"status\":\"delete\"}" },
        .{ .key = "doc:merge-noop", .value = "{\"title\":\"noop title\",\"note\":\"keep note\",\"status\":\"noop\"}" },
        .{ .key = "doc:merge-version", .value = "{\"title\":\"version title\",\"note\":\"version old\",\"status\":\"versioned\"}" },
        .{ .key = "doc:merge-stale", .value = "{\"title\":\"stale title\",\"note\":\"stale old\",\"status\":\"stale-version\"}" },
        .{ .key = "doc:merge-conflict", .value = "{\"title\":\"conflict title\",\"note\":\"conflict old\",\"status\":\"merge-conflict\"}" },
        .{ .key = "doc:merge-filtered", .value = "{\"title\":\"filtered title\",\"note\":\"filtered old\",\"status\":\"filtered\"}" },
    } });

    const native_table_name = try catalog_resources.defaultPublicTableResourceNameAlloc(alloc, "docs");
    defer alloc.free(native_table_name);

    const DualNameReads = struct {
        public: table_reads.BoundTableReadSource,
        native: table_reads.BoundTableReadSource,
        stale_version_key: ?[]const u8 = null,

        fn source(self: *@This()) table_reads.TableReadSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                },
            };
        }

        fn route(self: *@This(), table_name: []const u8) table_reads.TableReadSource {
            if (std.mem.eql(u8, table_name, "docs")) return self.public.source();
            return self.native.source();
        }

        fn lookup(
            ptr: *anyopaque,
            inner_alloc: std.mem.Allocator,
            table_name: []const u8,
            key: []const u8,
            opts: db_mod.types.LookupOptions,
            consistency: raft_mod.ReadConsistency,
        ) !?table_reads.LookupResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            var response = (try self.route(table_name).lookup(inner_alloc, table_name, key, opts, consistency)) orelse return null;
            if (self.stale_version_key) |stale_key| {
                if (std.mem.eql(u8, table_name, "docs") and std.mem.eql(u8, key, stale_key)) {
                    response.version = if (response.version > 0) response.version - 1 else 0;
                }
            }
            return response;
        }

        fn scan(
            ptr: *anyopaque,
            inner_alloc: std.mem.Allocator,
            table_name: []const u8,
            from_key: []const u8,
            to_key: []const u8,
            opts: db_mod.types.ScanOptions,
            consistency: raft_mod.ReadConsistency,
        ) !?table_reads.ScanResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try self.route(table_name).scan(inner_alloc, table_name, from_key, to_key, opts, consistency);
        }

        fn query(
            ptr: *anyopaque,
            inner_alloc: std.mem.Allocator,
            table_name: []const u8,
            req: db_mod.types.SearchRequest,
            consistency: raft_mod.ReadConsistency,
        ) !?query_api.QueryResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try self.route(table_name).query(inner_alloc, table_name, req, consistency);
        }
    };

    var read_source = DualNameReads{
        .public = table_reads.BoundTableReadSource.init("docs", 1, &db, raft_mod.read_gate.noopReadableLeaseRequester()),
        .native = table_reads.BoundTableReadSource.init(native_table_name, 1, &db, raft_mod.read_gate.noopReadableLeaseRequester()),
    };
    var write_source = table_writes.BoundTableWriteSource.init("docs", &db);

    const FakeSource = struct {
        tables: [1]metadata_table_manager.TableRecord,

        fn iface(self: *@This()) StatusSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{
                .status = try status(ptr),
                .tables = self.tables[0..],
                .ranges = &.{},
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const FakePublicSqlAudit = struct {
        records: [8]api_http_server.PublicSqlAuditRecord = undefined,
        record_count: usize = 0,

        fn sink(self: *@This()) api_http_server.PublicSqlAuditSink {
            return .{
                .ptr = self,
                .record = record,
            };
        }

        fn record(ptr: *anyopaque, event: api_http_server.PublicSqlAuditRecord) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.record_count >= self.records.len) return error.TooManyAuditRecords;
            self.records[self.record_count] = event;
            self.record_count += 1;
        }
    };

    var source = FakeSource{ .tables = .{.{
        .table_id = 1,
        .name = "docs",
        .schema_json = schema_json,
        .desired_replica_count = 1,
    }} };
    var audit = FakePublicSqlAudit{};
    var server = ApiHttpServer.init(alloc, .{ .public_sql_audit_sink = audit.sink() }, source.iface(), read_source.source(), write_source.source());
    defer server.deinit();

    var merge_resp = try handlePublicSqlEndpoint(
        &server,
        alloc,
        "MERGE INTO docs USING docs AS source ON docs._id = source._id WHEN MATCHED AND source.status = 'ready' THEN UPDATE SET note = source.title WHEN MATCHED AND source.status = 'delete' THEN DELETE WHEN MATCHED THEN DO NOTHING;",
    );
    defer merge_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), merge_resp.status);
    try std.testing.expectEqual(@as(usize, 1), audit.record_count);
    try std.testing.expectEqual(api_http_server.PublicSqlAuditOutcome.applied, audit.records[0].outcome);
    try std.testing.expectEqual(sql_adapter.SqlWriteStatementKind.merge, audit.records[0].statement_kind);
    try std.testing.expectEqual(api_http_server.PublicSqlNativeRoute.rows_batch, audit.records[0].native_route);
    try std.testing.expectEqual(usermgr.ResourceType.table, audit.records[0].required_resource_type);
    try std.testing.expectEqual(usermgr.PermissionType.write, audit.records[0].required_permission);
    try std.testing.expectEqual(@as(usize, 2), audit.records[0].row_count);
    try std.testing.expectEqualStrings("antfly", audit.records[0].target.database_name);
    try std.testing.expectEqualStrings("public", audit.records[0].target.namespace_name);
    try std.testing.expectEqualStrings("docs", audit.records[0].target.table_name);
    try std.testing.expect(audit.records[0].authenticated_subject == null);
    try std.testing.expect(audit.records[0].error_name == null);

    const updated = (try db.get(alloc, "doc:merge-update")) orelse return error.TestUnexpectedResult;
    defer alloc.free(updated);
    var parsed_updated = try std.json.parseFromSlice(std.json.Value, alloc, updated, .{});
    defer parsed_updated.deinit();
    try std.testing.expectEqualStrings("copied title", parsed_updated.value.object.get("note").?.string);

    try std.testing.expect((try db.get(alloc, "doc:merge-delete")) == null);

    const noop = (try db.get(alloc, "doc:merge-noop")) orelse return error.TestUnexpectedResult;
    defer alloc.free(noop);
    var parsed_noop = try std.json.parseFromSlice(std.json.Value, alloc, noop, .{});
    defer parsed_noop.deinit();
    try std.testing.expectEqualStrings("keep note", parsed_noop.value.object.get("note").?.string);

    const merge_version = try db.getTimestamp(alloc, "doc:merge-version");
    const merge_version_sql = try std.fmt.allocPrint(
        alloc,
        "MERGE INTO docs USING docs AS source ON docs._id = source._id WHEN MATCHED AND docs.status = 'versioned' AND docs._version = {d} THEN UPDATE SET note = source.title WHEN MATCHED AND docs.status = 'stale-version' AND docs._version = 999999 THEN UPDATE SET note = source.title WHEN MATCHED THEN DO NOTHING;",
        .{merge_version},
    );
    defer alloc.free(merge_version_sql);
    var merge_version_resp = try handlePublicSqlEndpoint(&server, alloc, merge_version_sql);
    defer merge_version_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), merge_version_resp.status);
    var merge_version_parsed = try std.json.parseFromSlice(std.json.Value, alloc, merge_version_resp.body, .{ .allocate = .alloc_always });
    defer merge_version_parsed.deinit();
    try std.testing.expectEqual(@as(i64, 1), merge_version_parsed.value.object.get("result").?.object.get("transformed").?.integer);
    try std.testing.expectEqual(@as(usize, 2), audit.record_count);
    try std.testing.expectEqual(api_http_server.PublicSqlAuditOutcome.applied, audit.records[1].outcome);
    try std.testing.expectEqual(sql_adapter.SqlWriteStatementKind.merge, audit.records[1].statement_kind);
    try std.testing.expectEqual(api_http_server.PublicSqlNativeRoute.rows_batch, audit.records[1].native_route);
    try std.testing.expectEqual(@as(usize, 1), audit.records[1].row_count);
    try std.testing.expect(audit.records[1].authenticated_subject == null);
    try std.testing.expect(audit.records[1].error_name == null);

    const versioned = (try db.get(alloc, "doc:merge-version")) orelse return error.TestUnexpectedResult;
    defer alloc.free(versioned);
    var parsed_versioned = try std.json.parseFromSlice(std.json.Value, alloc, versioned, .{});
    defer parsed_versioned.deinit();
    try std.testing.expectEqualStrings("version title", parsed_versioned.value.object.get("note").?.string);

    const stale = (try db.get(alloc, "doc:merge-stale")) orelse return error.TestUnexpectedResult;
    defer alloc.free(stale);
    var parsed_stale = try std.json.parseFromSlice(std.json.Value, alloc, stale, .{});
    defer parsed_stale.deinit();
    try std.testing.expectEqualStrings("stale old", parsed_stale.value.object.get("note").?.string);

    var merge_row_filters = try alloc.alloc(usermgr.RowFilterEntry, 1);
    merge_row_filters[0] = try usermgr.RowFilterEntry.initOwned(alloc, "docs", "{\"term\":{\"status\":\"allowed\"}}");
    var merge_filtered_identity = AuthenticatedIdentity{
        .username = try alloc.dupe(u8, "document_merge_filtered"),
        .row_filter = merge_row_filters,
    };
    defer merge_filtered_identity.deinit(alloc);
    var merge_filtered_resp = try handlePublicSqlDirect(
        &server,
        alloc,
        "MERGE INTO docs USING docs AS source ON docs._id = source._id WHEN MATCHED AND docs.status = 'filtered' THEN UPDATE SET note = source.title WHEN MATCHED THEN DO NOTHING;",
        merge_filtered_identity,
    );
    defer merge_filtered_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 403), merge_filtered_resp.status);
    try std.testing.expectEqualStrings("row filter rejected sql write", merge_filtered_resp.body);
    try std.testing.expectEqual(@as(usize, 3), audit.record_count);
    try std.testing.expectEqual(api_http_server.PublicSqlAuditOutcome.denied, audit.records[2].outcome);
    try std.testing.expectEqual(sql_adapter.SqlWriteStatementKind.merge, audit.records[2].statement_kind);
    try std.testing.expectEqual(api_http_server.PublicSqlNativeRoute.rows_batch, audit.records[2].native_route);
    try std.testing.expectEqual(@as(usize, 0), audit.records[2].row_count);
    try std.testing.expectEqualStrings("document_merge_filtered", audit.records[2].authenticated_subject orelse return error.TestUnexpectedResult);
    try std.testing.expectEqualStrings("PermissionDenied", audit.records[2].error_name orelse return error.TestUnexpectedResult);

    const filtered = (try db.get(alloc, "doc:merge-filtered")) orelse return error.TestUnexpectedResult;
    defer alloc.free(filtered);
    var parsed_filtered = try std.json.parseFromSlice(std.json.Value, alloc, filtered, .{});
    defer parsed_filtered.deinit();
    try std.testing.expectEqualStrings("filtered old", parsed_filtered.value.object.get("note").?.string);

    read_source.stale_version_key = "doc:merge-conflict";
    var merge_conflict_resp = try handlePublicSqlEndpoint(
        &server,
        alloc,
        "MERGE INTO docs USING docs AS source ON docs._id = source._id WHEN MATCHED AND docs.status = 'merge-conflict' THEN UPDATE SET note = source.title WHEN MATCHED THEN DO NOTHING;",
    );
    defer merge_conflict_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 409), merge_conflict_resp.status);
    try std.testing.expectEqualStrings("version conflict", merge_conflict_resp.body);
    try std.testing.expectEqual(@as(usize, 4), audit.record_count);
    try std.testing.expectEqual(api_http_server.PublicSqlAuditOutcome.failed, audit.records[3].outcome);
    try std.testing.expectEqual(sql_adapter.SqlWriteStatementKind.merge, audit.records[3].statement_kind);
    try std.testing.expectEqual(api_http_server.PublicSqlNativeRoute.rows_batch, audit.records[3].native_route);
    try std.testing.expectEqual(@as(usize, 0), audit.records[3].row_count);
    try std.testing.expect(audit.records[3].authenticated_subject == null);
    try std.testing.expectEqualStrings("VersionConflict", audit.records[3].error_name orelse return error.TestUnexpectedResult);

    const conflict = (try db.get(alloc, "doc:merge-conflict")) orelse return error.TestUnexpectedResult;
    defer alloc.free(conflict);
    var parsed_conflict = try std.json.parseFromSlice(std.json.Value, alloc, conflict, .{});
    defer parsed_conflict.deinit();
    try std.testing.expectEqualStrings("conflict old", parsed_conflict.value.object.get("note").?.string);
}

test "api public SQL endpoint executes document joined write parity cases" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"document","default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"title":{"type":"text"},"status":{"type":"keyword","x-antfly-cardinality-proof":"unique"},"category":{"type":"keyword","x-antfly-index":false},"status_lower":{"type":"keyword","generated":{"op":"lower","field":"status"}},"status_slug":{"type":"keyword","generated":{"op":"expression","expression":{"op":"replace","args":[{"op":"lower","args":[{"field":"status"}]},{"value":" "},{"value":"-"}]}}},"amount":{"type":"numeric"},"amount_abs":{"type":"numeric","generated":{"op":"expression","expression":{"op":"abs","args":[{"field":"amount"}]}}},"note":{"type":"text"},"metadata":{"type":"json"},"tags":{"type":"array","items":{"type":"keyword"}}},"required":["title"],"additionalProperties":true}}}}
    ;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/public-sql-document-write-parity", .{tmp.sub_path});
    defer alloc.free(path);

    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();
    try db.applyTableSchemaJson(alloc, schema_json, .{});
    try db.addIndex(.{ .name = "full_text_index_v0", .kind = .full_text, .config_json = "{}" });

    const native_table_name = try catalog_resources.defaultPublicTableResourceNameAlloc(alloc, "docs");
    defer alloc.free(native_table_name);

    const DualNameReads = struct {
        public: table_reads.BoundTableReadSource,
        native: table_reads.BoundTableReadSource,
        forced_scan_payload: ?[]const u8 = null,
        stale_version_key: ?[]const u8 = null,

        fn source(self: *@This()) table_reads.TableReadSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                },
            };
        }

        fn route(self: *@This(), table_name: []const u8) table_reads.TableReadSource {
            if (std.mem.eql(u8, table_name, "docs")) return self.public.source();
            return self.native.source();
        }

        fn lookup(
            ptr: *anyopaque,
            inner_alloc: std.mem.Allocator,
            table_name: []const u8,
            key: []const u8,
            opts: db_mod.types.LookupOptions,
            consistency: raft_mod.ReadConsistency,
        ) !?table_reads.LookupResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            var response = (try self.route(table_name).lookup(inner_alloc, table_name, key, opts, consistency)) orelse return null;
            if (self.stale_version_key) |stale_key| {
                if (std.mem.eql(u8, table_name, "docs") and std.mem.eql(u8, key, stale_key)) {
                    response.version = if (response.version > 0) response.version - 1 else 0;
                }
            }
            return response;
        }

        fn scan(
            ptr: *anyopaque,
            inner_alloc: std.mem.Allocator,
            table_name: []const u8,
            from_key: []const u8,
            to_key: []const u8,
            opts: db_mod.types.ScanOptions,
            consistency: raft_mod.ReadConsistency,
        ) !?table_reads.ScanResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.forced_scan_payload) |payload| {
                return .{ .ndjson = try inner_alloc.dupe(u8, payload) };
            }
            return try self.route(table_name).scan(inner_alloc, table_name, from_key, to_key, opts, consistency);
        }

        fn query(
            ptr: *anyopaque,
            inner_alloc: std.mem.Allocator,
            table_name: []const u8,
            req: db_mod.types.SearchRequest,
            consistency: raft_mod.ReadConsistency,
        ) !?query_api.QueryResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (std.mem.eql(u8, table_name, "docs") and std.mem.indexOf(u8, req.filter_query_json, "source-duplicate") != null) {
                return .{ .json = try inner_alloc.dupe(u8,
                    \\{"responses":[{"hits":{"total":2,"hits":[{"_id":"doc:source-duplicate"},{"_id":"doc:source-duplicate"}]}}]}
                ) };
            }
            if (std.mem.eql(u8, table_name, "docs") and std.mem.indexOf(u8, req.filter_query_json, "duplicate-source") != null) {
                return .{ .json = try inner_alloc.dupe(u8,
                    \\{"responses":[{"hits":{"total":2,"hits":[{"_id":"doc:dup-target"},{"_id":"doc:dup-source"}]}}]}
                ) };
            }
            return try self.route(table_name).query(inner_alloc, table_name, req, consistency);
        }
    };

    var read_source = DualNameReads{
        .public = table_reads.BoundTableReadSource.init("docs", 1, &db, raft_mod.read_gate.noopReadableLeaseRequester()),
        .native = table_reads.BoundTableReadSource.init(native_table_name, 1, &db, raft_mod.read_gate.noopReadableLeaseRequester()),
    };
    var write_source = table_writes.BoundTableWriteSource.init("docs", &db);

    const FakeSource = struct {
        tables: [1]metadata_table_manager.TableRecord,

        fn iface(self: *@This()) StatusSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{
                .status = try status(ptr),
                .tables = self.tables[0..],
                .ranges = &.{},
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = FakeSource{ .tables = .{.{
        .table_id = 1,
        .name = "docs",
        .schema_json = schema_json,
        .desired_replica_count = 1,
    }} };
    var server = ApiHttpServer.init(alloc, .{}, source.iface(), read_source.source(), write_source.source());
    defer server.deinit();

    var write_resp = try handlePublicSqlEndpoint(
        &server,
        alloc,
        "INSERT INTO docs (_id, _doc) VALUES ('doc:c', '{\"title\":\"gamma\",\"status\":\"queued\"}'::jsonb);",
    );
    defer write_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), write_resp.status);
    var write_parsed = try std.json.parseFromSlice(std.json.Value, alloc, write_resp.body, .{ .allocate = .alloc_always });
    defer write_parsed.deinit();
    try std.testing.expectEqualStrings("write", write_parsed.value.object.get("kind").?.string);
    try std.testing.expectEqualStrings("insert", write_parsed.value.object.get("statement_kind").?.string);
    try std.testing.expectEqual(@as(i64, 1), write_parsed.value.object.get("result").?.object.get("inserted").?.integer);

    var returning_insert_resp = try handlePublicSqlEndpoint(
        &server,
        alloc,
        "INSERT INTO docs (_id, title, status) VALUES ('doc:returning', 'returned title', 'ready') RETURNING _id, title AS returned_title, _doc, _version AS returned_version;",
    );
    defer returning_insert_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), returning_insert_resp.status);
    var returning_insert_parsed = try std.json.parseFromSlice(std.json.Value, alloc, returning_insert_resp.body, .{ .allocate = .alloc_always });
    defer returning_insert_parsed.deinit();
    const returning_result = returning_insert_parsed.value.object.get("result").?.object;
    try std.testing.expectEqual(@as(i64, 1), returning_result.get("inserted").?.integer);
    const returning_row = returning_result.get("returning").?.array.items[0].object;
    try std.testing.expectEqualStrings("doc:returning", returning_row.get("_id").?.string);
    try std.testing.expectEqualStrings("returned title", returning_row.get("returned_title").?.string);
    try std.testing.expectEqualStrings("ready", returning_row.get("_doc").?.object.get("status").?.string);
    try std.testing.expectEqual(@as(i64, @intCast(try db.getTimestamp(alloc, "doc:returning"))), returning_row.get("returned_version").?.integer);

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:source-insert", .value = "{\"title\":\"source projection\",\"status\":\"source-ready\"}" },
            .{ .key = "doc:source-indexed", .value = "{\"title\":\"indexed source projection\",\"status\":\"source-indexed\"}" },
            .{ .key = "doc:source-bounded-a", .value = "{\"title\":\"bounded source a\",\"category\":\"source-bounded\"}" },
            .{ .key = "doc:source-bounded-b", .value = "{\"title\":\"bounded source b\",\"category\":\"source-bounded\"}" },
            .{ .key = "doc:source-limit-a", .value = "{\"title\":\"limited source a\",\"category\":\"source-limit\"}" },
            .{ .key = "doc:source-limit-b", .value = "{\"title\":\"limited source b\",\"category\":\"source-limit\"}" },
            .{ .key = "doc:source-duplicate", .value = "{\"title\":\"duplicate source\",\"status\":\"source-duplicate\"}" },
            .{ .key = "doc:source-returning", .value = "{\"title\":\"returning source projection\",\"status\":\"source-returning\"}" },
            .{ .key = "doc:source-generate", .value = "{\"title\":\"generated source projection\",\"status\":\"source-generate\"}" },
        },
        .sync_level = .full_index,
    });

    var source_insert_resp = try handlePublicSqlEndpoint(
        &server,
        alloc,
        "INSERT INTO docs (_id, title, note) SELECT _id, title, status FROM docs WHERE _id = 'doc:source-insert';",
    );
    defer source_insert_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), source_insert_resp.status);
    var source_insert_parsed = try std.json.parseFromSlice(std.json.Value, alloc, source_insert_resp.body, .{ .allocate = .alloc_always });
    defer source_insert_parsed.deinit();
    try std.testing.expectEqualStrings("write", source_insert_parsed.value.object.get("kind").?.string);
    try std.testing.expectEqualStrings("insert_source", source_insert_parsed.value.object.get("statement_kind").?.string);
    try std.testing.expectEqual(@as(i64, 1), source_insert_parsed.value.object.get("result").?.object.get("inserted").?.integer);
    const source_insert_doc = (try db.get(alloc, "doc:source-insert")) orelse return error.TestExpectedEqual;
    defer alloc.free(source_insert_doc);
    var source_insert_doc_parsed = try std.json.parseFromSlice(std.json.Value, alloc, source_insert_doc, .{});
    defer source_insert_doc_parsed.deinit();
    try std.testing.expectEqualStrings("source projection", source_insert_doc_parsed.value.object.get("title").?.string);
    try std.testing.expectEqualStrings("source-ready", source_insert_doc_parsed.value.object.get("note").?.string);

    var indexed_source_insert_resp = try handlePublicSqlEndpoint(
        &server,
        alloc,
        "INSERT INTO docs (_id, title, note) SELECT _id, title, status FROM docs WHERE status = 'source-indexed';",
    );
    defer indexed_source_insert_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), indexed_source_insert_resp.status);
    var indexed_source_insert_parsed = try std.json.parseFromSlice(std.json.Value, alloc, indexed_source_insert_resp.body, .{ .allocate = .alloc_always });
    defer indexed_source_insert_parsed.deinit();
    try std.testing.expectEqualStrings("insert_source", indexed_source_insert_parsed.value.object.get("statement_kind").?.string);
    try std.testing.expectEqual(@as(i64, 1), indexed_source_insert_parsed.value.object.get("result").?.object.get("inserted").?.integer);
    const indexed_source_insert_doc = (try db.get(alloc, "doc:source-indexed")) orelse return error.TestExpectedEqual;
    defer alloc.free(indexed_source_insert_doc);
    var indexed_source_insert_doc_parsed = try std.json.parseFromSlice(std.json.Value, alloc, indexed_source_insert_doc, .{});
    defer indexed_source_insert_doc_parsed.deinit();
    try std.testing.expectEqualStrings("indexed source projection", indexed_source_insert_doc_parsed.value.object.get("title").?.string);
    try std.testing.expectEqualStrings("source-indexed", indexed_source_insert_doc_parsed.value.object.get("note").?.string);

    var bounded_source_insert_resp = try handlePublicSqlEndpoint(
        &server,
        alloc,
        "INSERT INTO docs (_id, title, note) SELECT _id, title, category FROM docs WHERE category = 'source-bounded';",
    );
    defer bounded_source_insert_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), bounded_source_insert_resp.status);
    var bounded_source_insert_parsed = try std.json.parseFromSlice(std.json.Value, alloc, bounded_source_insert_resp.body, .{ .allocate = .alloc_always });
    defer bounded_source_insert_parsed.deinit();
    try std.testing.expectEqualStrings("insert_source", bounded_source_insert_parsed.value.object.get("statement_kind").?.string);
    try std.testing.expectEqual(@as(i64, 2), bounded_source_insert_parsed.value.object.get("result").?.object.get("inserted").?.integer);
    const bounded_source_insert_doc = (try db.get(alloc, "doc:source-bounded-a")) orelse return error.TestExpectedEqual;
    defer alloc.free(bounded_source_insert_doc);
    var bounded_source_insert_doc_parsed = try std.json.parseFromSlice(std.json.Value, alloc, bounded_source_insert_doc, .{});
    defer bounded_source_insert_doc_parsed.deinit();
    try std.testing.expectEqualStrings("bounded source a", bounded_source_insert_doc_parsed.value.object.get("title").?.string);
    try std.testing.expectEqualStrings("source-bounded", bounded_source_insert_doc_parsed.value.object.get("note").?.string);

    var limited_source_insert_resp = try handlePublicSqlEndpoint(
        &server,
        alloc,
        "INSERT INTO docs (_id, title, note) SELECT _id, title, category FROM docs WHERE category = 'source-limit' LIMIT 1;",
    );
    defer limited_source_insert_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), limited_source_insert_resp.status);
    var limited_source_insert_parsed = try std.json.parseFromSlice(std.json.Value, alloc, limited_source_insert_resp.body, .{ .allocate = .alloc_always });
    defer limited_source_insert_parsed.deinit();
    try std.testing.expectEqualStrings("insert_source", limited_source_insert_parsed.value.object.get("statement_kind").?.string);
    try std.testing.expectEqual(@as(i64, 1), limited_source_insert_parsed.value.object.get("result").?.object.get("inserted").?.integer);
    const limited_source_a = (try db.get(alloc, "doc:source-limit-a")) orelse return error.TestExpectedEqual;
    defer alloc.free(limited_source_a);
    const limited_source_b = (try db.get(alloc, "doc:source-limit-b")) orelse return error.TestExpectedEqual;
    defer alloc.free(limited_source_b);
    var limited_source_a_parsed = try std.json.parseFromSlice(std.json.Value, alloc, limited_source_a, .{});
    defer limited_source_a_parsed.deinit();
    var limited_source_b_parsed = try std.json.parseFromSlice(std.json.Value, alloc, limited_source_b, .{});
    defer limited_source_b_parsed.deinit();
    const limited_source_a_written = limited_source_a_parsed.value.object.get("note") != null;
    const limited_source_b_written = limited_source_b_parsed.value.object.get("note") != null;
    try std.testing.expect(limited_source_a_written != limited_source_b_written);
    if (limited_source_a_written) {
        try std.testing.expectEqualStrings("source-limit", limited_source_a_parsed.value.object.get("note").?.string);
    } else {
        try std.testing.expectEqualStrings("source-limit", limited_source_b_parsed.value.object.get("note").?.string);
    }

    var ordered_source_insert_resp = try handlePublicSqlEndpoint(
        &server,
        alloc,
        "INSERT INTO docs (_id, title) SELECT _id, title FROM docs WHERE category = 'source-limit' ORDER BY _id LIMIT 1;",
    );
    defer ordered_source_insert_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 400), ordered_source_insert_resp.status);

    var duplicate_source_insert_resp = try handlePublicSqlEndpoint(
        &server,
        alloc,
        "INSERT INTO docs (_id, title) SELECT _id, title FROM docs WHERE status = 'source-duplicate';",
    );
    defer duplicate_source_insert_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 409), duplicate_source_insert_resp.status);
    try std.testing.expectEqualStrings("duplicate source row", duplicate_source_insert_resp.body);

    var source_insert_returning_resp = try handlePublicSqlEndpoint(
        &server,
        alloc,
        "INSERT INTO docs (_id, title, note) SELECT _id, title, status FROM docs WHERE _id = 'doc:source-returning' RETURNING _id, title, note, _doc, _version;",
    );
    defer source_insert_returning_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), source_insert_returning_resp.status);
    var source_insert_returning_parsed = try std.json.parseFromSlice(std.json.Value, alloc, source_insert_returning_resp.body, .{ .allocate = .alloc_always });
    defer source_insert_returning_parsed.deinit();
    try std.testing.expectEqualStrings("insert_source", source_insert_returning_parsed.value.object.get("statement_kind").?.string);
    const source_insert_returning_result = source_insert_returning_parsed.value.object.get("result").?.object;
    try std.testing.expectEqual(@as(i64, 1), source_insert_returning_result.get("inserted").?.integer);
    const source_insert_returning_row = source_insert_returning_result.get("returning").?.array.items[0].object;
    try std.testing.expectEqualStrings("doc:source-returning", source_insert_returning_row.get("_id").?.string);
    try std.testing.expectEqualStrings("returning source projection", source_insert_returning_row.get("title").?.string);
    try std.testing.expectEqualStrings("source-returning", source_insert_returning_row.get("note").?.string);
    try std.testing.expectEqualStrings("source-returning", source_insert_returning_row.get("_doc").?.object.get("note").?.string);
    try std.testing.expectEqual(@as(i64, @intCast(try db.getTimestamp(alloc, "doc:source-returning"))), source_insert_returning_row.get("_version").?.integer);

    var generated_source_insert_resp = try handlePublicSqlEndpoint(
        &server,
        alloc,
        "INSERT INTO docs (title) SELECT title FROM docs WHERE _id = 'doc:source-generate' RETURNING _id, title, _doc, _version;",
    );
    defer generated_source_insert_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), generated_source_insert_resp.status);
    var generated_source_insert_parsed = try std.json.parseFromSlice(std.json.Value, alloc, generated_source_insert_resp.body, .{ .allocate = .alloc_always });
    defer generated_source_insert_parsed.deinit();
    try std.testing.expectEqualStrings("insert_source", generated_source_insert_parsed.value.object.get("statement_kind").?.string);
    const generated_source_insert_result = generated_source_insert_parsed.value.object.get("result").?.object;
    try std.testing.expectEqual(@as(i64, 1), generated_source_insert_result.get("inserted").?.integer);
    const generated_source_insert_row = generated_source_insert_result.get("returning").?.array.items[0].object;
    const generated_source_id = generated_source_insert_row.get("_id").?.string;
    try std.testing.expect(!std.mem.eql(u8, generated_source_id, "doc:source-generate"));
    try std.testing.expect(std.mem.startsWith(u8, generated_source_id, "doc:"));
    try std.testing.expectEqual(@as(usize, 36), generated_source_id.len);
    try std.testing.expectEqualStrings("generated source projection", generated_source_insert_row.get("title").?.string);
    try std.testing.expectEqualStrings("generated source projection", generated_source_insert_row.get("_doc").?.object.get("title").?.string);
    try std.testing.expectEqual(@as(i64, @intCast(try db.getTimestamp(alloc, generated_source_id))), generated_source_insert_row.get("_version").?.integer);
    const generated_source_doc = (try db.get(alloc, generated_source_id)) orelse return error.TestExpectedEqual;
    defer alloc.free(generated_source_doc);
    var generated_source_doc_parsed = try std.json.parseFromSlice(std.json.Value, alloc, generated_source_doc, .{});
    defer generated_source_doc_parsed.deinit();
    try std.testing.expectEqualStrings("generated source projection", generated_source_doc_parsed.value.object.get("title").?.string);

    var generated_indexed_source_resp = try handlePublicSqlEndpoint(
        &server,
        alloc,
        "INSERT INTO docs (title) SELECT title FROM docs WHERE status = 'source-indexed' RETURNING _id, title;",
    );
    defer generated_indexed_source_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), generated_indexed_source_resp.status);
    var generated_indexed_source_parsed = try std.json.parseFromSlice(std.json.Value, alloc, generated_indexed_source_resp.body, .{ .allocate = .alloc_always });
    defer generated_indexed_source_parsed.deinit();
    const generated_indexed_source_result = generated_indexed_source_parsed.value.object.get("result").?.object;
    try std.testing.expectEqual(@as(i64, 1), generated_indexed_source_result.get("inserted").?.integer);
    const generated_indexed_source_row = generated_indexed_source_result.get("returning").?.array.items[0].object;
    const generated_indexed_source_id = generated_indexed_source_row.get("_id").?.string;
    try std.testing.expect(!std.mem.eql(u8, generated_indexed_source_id, "doc:source-indexed"));
    try std.testing.expect(std.mem.startsWith(u8, generated_indexed_source_id, "doc:"));
    try std.testing.expectEqual(@as(usize, 36), generated_indexed_source_id.len);
    try std.testing.expectEqualStrings("indexed source projection", generated_indexed_source_row.get("title").?.string);

    var generated_bounded_source_resp = try handlePublicSqlEndpoint(
        &server,
        alloc,
        "INSERT INTO docs (title) SELECT title FROM docs WHERE category = 'source-bounded' RETURNING _id, title;",
    );
    defer generated_bounded_source_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), generated_bounded_source_resp.status);
    var generated_bounded_source_parsed = try std.json.parseFromSlice(std.json.Value, alloc, generated_bounded_source_resp.body, .{ .allocate = .alloc_always });
    defer generated_bounded_source_parsed.deinit();
    const generated_bounded_source_result = generated_bounded_source_parsed.value.object.get("result").?.object;
    try std.testing.expectEqual(@as(i64, 2), generated_bounded_source_result.get("inserted").?.integer);
    const generated_bounded_rows = generated_bounded_source_result.get("returning").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), generated_bounded_rows.len);
    const generated_bounded_a = generated_bounded_rows[0].object.get("_id").?.string;
    const generated_bounded_b = generated_bounded_rows[1].object.get("_id").?.string;
    try std.testing.expect(!std.mem.eql(u8, generated_bounded_a, generated_bounded_b));
    try std.testing.expect(std.mem.startsWith(u8, generated_bounded_a, "doc:"));
    try std.testing.expect(std.mem.startsWith(u8, generated_bounded_b, "doc:"));
    try std.testing.expectEqual(@as(usize, 36), generated_bounded_a.len);
    try std.testing.expectEqual(@as(usize, 36), generated_bounded_b.len);

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:producer-returning", .value = "{\"title\":\"producer initial\",\"status\":\"producer-returning\",\"category\":\"producer-update\"}" },
            .{ .key = "doc:producer-delete-returning", .value = "{\"title\":\"producer delete initial\",\"status\":\"producer-delete\",\"category\":\"producer-delete\"}" },
        },
        .sync_level = .full_index,
    });

    var producer_update_return_resp = try handlePublicSqlEndpoint(
        &server,
        alloc,
        "UPDATE docs SET title = 'producer returned' WHERE status = 'producer-returning' RETURNING _id, title, _doc, _version;",
    );
    defer producer_update_return_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), producer_update_return_resp.status);
    var producer_update_return_parsed = try std.json.parseFromSlice(std.json.Value, alloc, producer_update_return_resp.body, .{ .allocate = .alloc_always });
    defer producer_update_return_parsed.deinit();
    const producer_update_return_result = producer_update_return_parsed.value.object.get("result").?.object;
    try std.testing.expectEqual(@as(i64, 1), producer_update_return_result.get("transformed").?.integer);
    const producer_update_return_row = producer_update_return_result.get("returning").?.array.items[0].object;
    try std.testing.expectEqualStrings("doc:producer-returning", producer_update_return_row.get("_id").?.string);
    try std.testing.expectEqualStrings("producer returned", producer_update_return_row.get("title").?.string);
    try std.testing.expectEqualStrings("producer returned", producer_update_return_row.get("_doc").?.object.get("title").?.string);
    try std.testing.expectEqual(@as(i64, @intCast(try db.getTimestamp(alloc, "doc:producer-returning"))), producer_update_return_row.get("_version").?.integer);

    const producer_delete_pre_version = try db.getTimestamp(alloc, "doc:producer-delete-returning");
    var producer_delete_return_resp = try handlePublicSqlEndpoint(
        &server,
        alloc,
        "DELETE FROM docs WHERE category = 'producer-delete' RETURNING _id, title, _doc, _version;",
    );
    defer producer_delete_return_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), producer_delete_return_resp.status);
    var producer_delete_return_parsed = try std.json.parseFromSlice(std.json.Value, alloc, producer_delete_return_resp.body, .{ .allocate = .alloc_always });
    defer producer_delete_return_parsed.deinit();
    const producer_delete_return_result = producer_delete_return_parsed.value.object.get("result").?.object;
    try std.testing.expectEqual(@as(i64, 1), producer_delete_return_result.get("deleted").?.integer);
    const producer_delete_return_row = producer_delete_return_result.get("returning").?.array.items[0].object;
    try std.testing.expectEqualStrings("doc:producer-delete-returning", producer_delete_return_row.get("_id").?.string);
    try std.testing.expectEqualStrings("producer delete initial", producer_delete_return_row.get("title").?.string);
    try std.testing.expectEqualStrings("producer delete initial", producer_delete_return_row.get("_doc").?.object.get("title").?.string);
    try std.testing.expectEqual(@as(i64, @intCast(producer_delete_pre_version)), producer_delete_return_row.get("_version").?.integer);
    if (try db.get(alloc, "doc:producer-delete-returning")) |deleted_doc| {
        defer alloc.free(deleted_doc);
        return error.TestUnexpectedResult;
    }

    var conflict_insert_resp = try handlePublicSqlEndpoint(
        &server,
        alloc,
        "INSERT INTO docs (_id, title, status) VALUES ('doc:conflict-new', 'conflict title', 'ready') ON CONFLICT (_id) DO NOTHING;",
    );
    defer conflict_insert_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), conflict_insert_resp.status);
    var conflict_insert_parsed = try std.json.parseFromSlice(std.json.Value, alloc, conflict_insert_resp.body, .{ .allocate = .alloc_always });
    defer conflict_insert_parsed.deinit();
    try std.testing.expectEqual(@as(i64, 1), conflict_insert_parsed.value.object.get("result").?.object.get("inserted").?.integer);

    var conflict_noop_resp = try handlePublicSqlEndpoint(
        &server,
        alloc,
        "INSERT INTO docs (_id, title, status) VALUES ('doc:conflict-new', 'ignored title', 'ignored') ON CONFLICT (_id) DO NOTHING;",
    );
    defer conflict_noop_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), conflict_noop_resp.status);
    var conflict_noop_parsed = try std.json.parseFromSlice(std.json.Value, alloc, conflict_noop_resp.body, .{ .allocate = .alloc_always });
    defer conflict_noop_parsed.deinit();
    try std.testing.expectEqual(@as(i64, 0), conflict_noop_parsed.value.object.get("result").?.object.get("inserted").?.integer);
    const conflict_noop_doc = (try db.get(alloc, "doc:conflict-new")) orelse return error.TestExpectedEqual;
    defer alloc.free(conflict_noop_doc);
    var conflict_noop_doc_parsed = try std.json.parseFromSlice(std.json.Value, alloc, conflict_noop_doc, .{});
    defer conflict_noop_doc_parsed.deinit();
    try std.testing.expectEqualStrings("conflict title", conflict_noop_doc_parsed.value.object.get("title").?.string);
    try std.testing.expectEqualStrings("ready", conflict_noop_doc_parsed.value.object.get("status").?.string);

    var conflict_update_resp = try handlePublicSqlEndpoint(
        &server,
        alloc,
        "INSERT INTO docs (_id, title, status, note) VALUES ('doc:conflict-new', 'updated title', 'updated', 'literal note') ON CONFLICT (_id) DO UPDATE SET title = excluded.title, status = 'updated-literal', note = excluded.note;",
    );
    defer conflict_update_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), conflict_update_resp.status);
    var conflict_update_parsed = try std.json.parseFromSlice(std.json.Value, alloc, conflict_update_resp.body, .{ .allocate = .alloc_always });
    defer conflict_update_parsed.deinit();
    try std.testing.expectEqual(@as(i64, 0), conflict_update_parsed.value.object.get("result").?.object.get("inserted").?.integer);
    try std.testing.expectEqual(@as(i64, 1), conflict_update_parsed.value.object.get("result").?.object.get("transformed").?.integer);
    const conflict_updated_doc = (try db.get(alloc, "doc:conflict-new")) orelse return error.TestExpectedEqual;
    defer alloc.free(conflict_updated_doc);
    var conflict_updated_doc_parsed = try std.json.parseFromSlice(std.json.Value, alloc, conflict_updated_doc, .{});
    defer conflict_updated_doc_parsed.deinit();
    try std.testing.expectEqualStrings("updated title", conflict_updated_doc_parsed.value.object.get("title").?.string);
    try std.testing.expectEqualStrings("updated-literal", conflict_updated_doc_parsed.value.object.get("status").?.string);
    try std.testing.expectEqualStrings("literal note", conflict_updated_doc_parsed.value.object.get("note").?.string);

    var conflict_update_insert_resp = try handlePublicSqlEndpoint(
        &server,
        alloc,
        "INSERT INTO docs (_id, title, status) VALUES ('doc:conflict-update-insert', 'insert via update action', 'ready') ON CONFLICT (_id) DO UPDATE SET title = excluded.title;",
    );
    defer conflict_update_insert_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), conflict_update_insert_resp.status);
    var conflict_update_insert_parsed = try std.json.parseFromSlice(std.json.Value, alloc, conflict_update_insert_resp.body, .{ .allocate = .alloc_always });
    defer conflict_update_insert_parsed.deinit();
    const conflict_update_insert_result = conflict_update_insert_parsed.value.object.get("result").?.object;
    try std.testing.expectEqual(@as(i64, 1), conflict_update_insert_result.get("inserted").?.integer);
    if (conflict_update_insert_result.get("transformed")) |transformed| {
        try std.testing.expectEqual(@as(i64, 0), transformed.integer);
    }
    const conflict_update_insert_doc = (try db.get(alloc, "doc:conflict-update-insert")) orelse return error.TestExpectedEqual;
    defer alloc.free(conflict_update_insert_doc);
    var conflict_update_insert_doc_parsed = try std.json.parseFromSlice(std.json.Value, alloc, conflict_update_insert_doc, .{});
    defer conflict_update_insert_doc_parsed.deinit();
    try std.testing.expectEqualStrings("insert via update action", conflict_update_insert_doc_parsed.value.object.get("title").?.string);

    var conflict_nested_return_resp = try handlePublicSqlEndpoint(
        &server,
        alloc,
        "INSERT INTO docs (_id, title, metadata) VALUES ('doc:conflict-new', 'nested proposed', '{\"profile\":{\"name\":\"Ada\",\"tier\":\"pro\"}}'::jsonb) ON CONFLICT (_id) DO UPDATE SET title = excluded.title, metadata = excluded.metadata RETURNING _id, metadata AS returned_metadata, _doc;",
    );
    defer conflict_nested_return_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), conflict_nested_return_resp.status);
    var conflict_nested_return_parsed = try std.json.parseFromSlice(std.json.Value, alloc, conflict_nested_return_resp.body, .{ .allocate = .alloc_always });
    defer conflict_nested_return_parsed.deinit();
    const conflict_nested_return_result = conflict_nested_return_parsed.value.object.get("result").?.object;
    try std.testing.expectEqual(@as(i64, 1), conflict_nested_return_result.get("transformed").?.integer);
    const conflict_nested_return_row = conflict_nested_return_result.get("returning").?.array.items[0].object;
    try std.testing.expectEqualStrings("doc:conflict-new", conflict_nested_return_row.get("_id").?.string);
    try std.testing.expectEqualStrings("Ada", conflict_nested_return_row.get("returned_metadata").?.object.get("profile").?.object.get("name").?.string);
    try std.testing.expectEqualStrings("pro", conflict_nested_return_row.get("_doc").?.object.get("metadata").?.object.get("profile").?.object.get("tier").?.string);
    const conflict_nested_doc = (try db.get(alloc, "doc:conflict-new")) orelse return error.TestExpectedEqual;
    defer alloc.free(conflict_nested_doc);
    var conflict_nested_doc_parsed = try std.json.parseFromSlice(std.json.Value, alloc, conflict_nested_doc, .{});
    defer conflict_nested_doc_parsed.deinit();
    try std.testing.expectEqualStrings("Ada", conflict_nested_doc_parsed.value.object.get("metadata").?.object.get("profile").?.object.get("name").?.string);

    var conflict_guard_apply_resp = try handlePublicSqlEndpoint(
        &server,
        alloc,
        "INSERT INTO docs (_id, title, status) VALUES ('doc:conflict-new', 'guard applied', 'guarded') ON CONFLICT (_id) DO UPDATE SET title = excluded.title WHERE status = 'updated-literal' AND excluded.status = 'guarded' RETURNING _id, title, _version AS version_after;",
    );
    defer conflict_guard_apply_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), conflict_guard_apply_resp.status);
    var conflict_guard_apply_parsed = try std.json.parseFromSlice(std.json.Value, alloc, conflict_guard_apply_resp.body, .{ .allocate = .alloc_always });
    defer conflict_guard_apply_parsed.deinit();
    const conflict_guard_apply_result = conflict_guard_apply_parsed.value.object.get("result").?.object;
    try std.testing.expectEqual(@as(i64, 1), conflict_guard_apply_result.get("transformed").?.integer);
    const conflict_guard_apply_row = conflict_guard_apply_result.get("returning").?.array.items[0].object;
    try std.testing.expectEqualStrings("doc:conflict-new", conflict_guard_apply_row.get("_id").?.string);
    try std.testing.expectEqualStrings("guard applied", conflict_guard_apply_row.get("title").?.string);
    try std.testing.expectEqual(@as(i64, @intCast(try db.getTimestamp(alloc, "doc:conflict-new"))), conflict_guard_apply_row.get("version_after").?.integer);

    var conflict_guard_skip_resp = try handlePublicSqlEndpoint(
        &server,
        alloc,
        "INSERT INTO docs (_id, title, status) VALUES ('doc:conflict-new', 'guard skipped', 'guarded') ON CONFLICT (_id) DO UPDATE SET title = excluded.title WHERE status = 'not-current';",
    );
    defer conflict_guard_skip_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), conflict_guard_skip_resp.status);
    var conflict_guard_skip_parsed = try std.json.parseFromSlice(std.json.Value, alloc, conflict_guard_skip_resp.body, .{ .allocate = .alloc_always });
    defer conflict_guard_skip_parsed.deinit();
    try std.testing.expectEqual(@as(i64, 0), conflict_guard_skip_parsed.value.object.get("result").?.object.get("transformed").?.integer);
    const conflict_guard_skip_doc = (try db.get(alloc, "doc:conflict-new")) orelse return error.TestExpectedEqual;
    defer alloc.free(conflict_guard_skip_doc);
    var conflict_guard_skip_doc_parsed = try std.json.parseFromSlice(std.json.Value, alloc, conflict_guard_skip_doc, .{});
    defer conflict_guard_skip_doc_parsed.deinit();
    try std.testing.expectEqualStrings("guard applied", conflict_guard_skip_doc_parsed.value.object.get("title").?.string);

    var conflict_expression_resp = try handlePublicSqlEndpoint(
        &server,
        alloc,
        "INSERT INTO docs (_id, title, status) VALUES ('doc:conflict-new', 'ignored expression title', 'PUBLISHED') ON CONFLICT (_id) DO UPDATE SET title = lower(excluded.status) WHERE title = 'guard applied' RETURNING title;",
    );
    defer conflict_expression_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), conflict_expression_resp.status);
    var conflict_expression_parsed = try std.json.parseFromSlice(std.json.Value, alloc, conflict_expression_resp.body, .{ .allocate = .alloc_always });
    defer conflict_expression_parsed.deinit();
    const conflict_expression_result = conflict_expression_parsed.value.object.get("result").?.object;
    try std.testing.expectEqual(@as(i64, 1), conflict_expression_result.get("transformed").?.integer);
    const conflict_expression_row = conflict_expression_result.get("returning").?.array.items[0].object;
    try std.testing.expectEqualStrings("published", conflict_expression_row.get("title").?.string);

    var conflict_unique_target_resp = try handlePublicSqlEndpoint(
        &server,
        alloc,
        "INSERT INTO docs (_id, title, status) VALUES ('doc:conflict-status-new', 'status conflict title', 'updated-literal') ON CONFLICT (status) DO UPDATE SET title = excluded.title RETURNING _id, title;",
    );
    defer conflict_unique_target_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), conflict_unique_target_resp.status);
    var conflict_unique_target_parsed = try std.json.parseFromSlice(std.json.Value, alloc, conflict_unique_target_resp.body, .{ .allocate = .alloc_always });
    defer conflict_unique_target_parsed.deinit();
    const conflict_unique_target_result = conflict_unique_target_parsed.value.object.get("result").?.object;
    try std.testing.expectEqual(@as(i64, 0), conflict_unique_target_result.get("inserted").?.integer);
    try std.testing.expectEqual(@as(i64, 1), conflict_unique_target_result.get("transformed").?.integer);
    const conflict_unique_target_row = conflict_unique_target_result.get("returning").?.array.items[0].object;
    try std.testing.expectEqualStrings("doc:conflict-new", conflict_unique_target_row.get("_id").?.string);
    try std.testing.expectEqualStrings("status conflict title", conflict_unique_target_row.get("title").?.string);
    const unexpected_unique_insert = try db.get(alloc, "doc:conflict-status-new");
    defer if (unexpected_unique_insert) |value| alloc.free(value);
    try std.testing.expect(unexpected_unique_insert == null);

    var conflict_schema_failure_resp = try handlePublicSqlEndpoint(
        &server,
        alloc,
        "INSERT INTO docs (_id, title, status) VALUES ('doc:conflict-new', NULL, 'invalid') ON CONFLICT (_id) DO UPDATE SET title = excluded.title;",
    );
    defer conflict_schema_failure_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 400), conflict_schema_failure_resp.status);
    try std.testing.expectEqualStrings("invalid sql write", conflict_schema_failure_resp.body);
    const conflict_schema_failure_unchanged = (try db.get(alloc, "doc:conflict-new")) orelse return error.TestExpectedEqual;
    defer alloc.free(conflict_schema_failure_unchanged);
    var conflict_schema_failure_unchanged_parsed = try std.json.parseFromSlice(std.json.Value, alloc, conflict_schema_failure_unchanged, .{});
    defer conflict_schema_failure_unchanged_parsed.deinit();
    try std.testing.expectEqualStrings("status conflict title", conflict_schema_failure_unchanged_parsed.value.object.get("title").?.string);

    read_source.stale_version_key = "doc:conflict-new";
    var conflict_stale_resp = try handlePublicSqlEndpoint(
        &server,
        alloc,
        "INSERT INTO docs (_id, title, status) VALUES ('doc:conflict-new', 'stale rejected', 'stale') ON CONFLICT (_id) DO UPDATE SET title = excluded.title;",
    );
    read_source.stale_version_key = null;
    defer conflict_stale_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 409), conflict_stale_resp.status);
    try std.testing.expectEqualStrings("version conflict", conflict_stale_resp.body);
    const conflict_stale_unchanged = (try db.get(alloc, "doc:conflict-new")) orelse return error.TestExpectedEqual;
    defer alloc.free(conflict_stale_unchanged);
    var conflict_stale_unchanged_parsed = try std.json.parseFromSlice(std.json.Value, alloc, conflict_stale_unchanged, .{});
    defer conflict_stale_unchanged_parsed.deinit();
    try std.testing.expectEqualStrings("status conflict title", conflict_stale_unchanged_parsed.value.object.get("title").?.string);

    var conflict_return_insert_resp = try handlePublicSqlEndpoint(
        &server,
        alloc,
        "INSERT INTO docs (_id, title, status) VALUES ('doc:conflict-returning', 'return insert', 'ready') ON CONFLICT (_id) DO NOTHING RETURNING _id, title, _doc;",
    );
    defer conflict_return_insert_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), conflict_return_insert_resp.status);
    var conflict_return_insert_parsed = try std.json.parseFromSlice(std.json.Value, alloc, conflict_return_insert_resp.body, .{ .allocate = .alloc_always });
    defer conflict_return_insert_parsed.deinit();
    const conflict_return_insert_result = conflict_return_insert_parsed.value.object.get("result").?.object;
    try std.testing.expectEqual(@as(i64, 1), conflict_return_insert_result.get("inserted").?.integer);
    const conflict_return_insert_row = conflict_return_insert_result.get("returning").?.array.items[0].object;
    try std.testing.expectEqualStrings("doc:conflict-returning", conflict_return_insert_row.get("_id").?.string);
    try std.testing.expectEqualStrings("return insert", conflict_return_insert_row.get("title").?.string);
    try std.testing.expectEqualStrings("ready", conflict_return_insert_row.get("_doc").?.object.get("status").?.string);

    var conflict_return_noop_resp = try handlePublicSqlEndpoint(
        &server,
        alloc,
        "INSERT INTO docs (_id, title, status) VALUES ('doc:conflict-returning', 'ignored returning', 'ignored') ON CONFLICT (_id) DO NOTHING RETURNING _id, title;",
    );
    defer conflict_return_noop_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), conflict_return_noop_resp.status);
    var conflict_return_noop_parsed = try std.json.parseFromSlice(std.json.Value, alloc, conflict_return_noop_resp.body, .{ .allocate = .alloc_always });
    defer conflict_return_noop_parsed.deinit();
    const conflict_return_noop_result = conflict_return_noop_parsed.value.object.get("result").?.object;
    try std.testing.expectEqual(@as(i64, 0), conflict_return_noop_result.get("inserted").?.integer);
    try std.testing.expect(conflict_return_noop_result.get("returning") == null);

    var conflict_return_update_resp = try handlePublicSqlEndpoint(
        &server,
        alloc,
        "INSERT INTO docs (_id, title, status) VALUES ('doc:conflict-returning', 'return update', 'updated') ON CONFLICT (_id) DO UPDATE SET title = excluded.title, status = 'returned-updated' RETURNING _id, title, status, _doc;",
    );
    defer conflict_return_update_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), conflict_return_update_resp.status);
    var conflict_return_update_parsed = try std.json.parseFromSlice(std.json.Value, alloc, conflict_return_update_resp.body, .{ .allocate = .alloc_always });
    defer conflict_return_update_parsed.deinit();
    const conflict_return_update_result = conflict_return_update_parsed.value.object.get("result").?.object;
    try std.testing.expectEqual(@as(i64, 1), conflict_return_update_result.get("transformed").?.integer);
    const conflict_return_update_row = conflict_return_update_result.get("returning").?.array.items[0].object;
    try std.testing.expectEqualStrings("doc:conflict-returning", conflict_return_update_row.get("_id").?.string);
    try std.testing.expectEqualStrings("return update", conflict_return_update_row.get("title").?.string);
    try std.testing.expectEqualStrings("returned-updated", conflict_return_update_row.get("status").?.string);
    try std.testing.expectEqualStrings("returned-updated", conflict_return_update_row.get("_doc").?.object.get("status").?.string);

    var read_only_permissions = try alloc.alloc(usermgr.Permission, 1);
    read_only_permissions[0] = try usermgr.Permission.initOwned(alloc, .table, "docs", .read);
    var read_only_document_identity = AuthenticatedIdentity{
        .username = try alloc.dupe(u8, "document_sql_read_only"),
        .permissions = read_only_permissions,
    };
    defer read_only_document_identity.deinit(alloc);
    var joined_auth_denied_resp = try handlePublicSqlDirect(
        &server,
        alloc,
        "UPDATE docs SET status = 'blocked' FROM docs AS source WHERE docs._id = source._id AND docs._id = 'doc:c';",
        read_only_document_identity,
    );
    defer joined_auth_denied_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 403), joined_auth_denied_resp.status);
    try expectPublicSqlDiagnosticBody(alloc, joined_auth_denied_resp.body, "bind", "permission_denied", "permission denied", null, null);
    const joined_auth_denied_unchanged = (try db.get(alloc, "doc:c")) orelse return error.TestExpectedEqual;
    defer alloc.free(joined_auth_denied_unchanged);
    var joined_auth_denied_unchanged_parsed = try std.json.parseFromSlice(std.json.Value, alloc, joined_auth_denied_unchanged, .{});
    defer joined_auth_denied_unchanged_parsed.deinit();
    try std.testing.expectEqualStrings("gamma", joined_auth_denied_unchanged_parsed.value.object.get("title").?.string);
    try std.testing.expectEqualStrings("queued", joined_auth_denied_unchanged_parsed.value.object.get("status").?.string);

    var joined_update_resp = try handlePublicSqlEndpoint(
        &server,
        alloc,
        "UPDATE docs SET status = 'copied', title = source.title FROM docs AS source WHERE docs._id = source._id AND docs._id = 'doc:c';",
    );
    defer joined_update_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), joined_update_resp.status);
    var joined_update_parsed = try std.json.parseFromSlice(std.json.Value, alloc, joined_update_resp.body, .{ .allocate = .alloc_always });
    defer joined_update_parsed.deinit();
    try std.testing.expectEqualStrings("write", joined_update_parsed.value.object.get("kind").?.string);
    try std.testing.expectEqualStrings("update_joined_source", joined_update_parsed.value.object.get("statement_kind").?.string);
    try std.testing.expectEqual(@as(i64, 1), joined_update_parsed.value.object.get("result").?.object.get("transformed").?.integer);
    const joined_updated = (try db.get(alloc, "doc:c")) orelse return error.TestExpectedEqual;
    defer alloc.free(joined_updated);
    var joined_updated_parsed = try std.json.parseFromSlice(std.json.Value, alloc, joined_updated, .{});
    defer joined_updated_parsed.deinit();
    try std.testing.expectEqualStrings("gamma", joined_updated_parsed.value.object.get("title").?.string);
    try std.testing.expectEqualStrings("copied", joined_updated_parsed.value.object.get("status").?.string);

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:mapped", .value = "{\"title\":\"mapped target\",\"status\":\"draft\",\"note\":\"mapped source\"}" },
        },
        .sync_level = .full_index,
    });

    var joined_mapped_update_resp = try handlePublicSqlEndpoint(
        &server,
        alloc,
        "UPDATE docs SET title = source.note FROM docs AS source WHERE docs.status = source.status AND docs.status = 'draft';",
    );
    defer joined_mapped_update_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), joined_mapped_update_resp.status);
    var joined_mapped_update_parsed = try std.json.parseFromSlice(std.json.Value, alloc, joined_mapped_update_resp.body, .{ .allocate = .alloc_always });
    defer joined_mapped_update_parsed.deinit();
    try std.testing.expectEqualStrings("write", joined_mapped_update_parsed.value.object.get("kind").?.string);
    try std.testing.expectEqualStrings("update_joined_source", joined_mapped_update_parsed.value.object.get("statement_kind").?.string);
    try std.testing.expectEqual(@as(i64, 1), joined_mapped_update_parsed.value.object.get("result").?.object.get("transformed").?.integer);
    const joined_mapped_updated = (try db.get(alloc, "doc:mapped")) orelse return error.TestExpectedEqual;
    defer alloc.free(joined_mapped_updated);
    var joined_mapped_updated_parsed = try std.json.parseFromSlice(std.json.Value, alloc, joined_mapped_updated, .{});
    defer joined_mapped_updated_parsed.deinit();
    try std.testing.expectEqualStrings("mapped source", joined_mapped_updated_parsed.value.object.get("title").?.string);
    try std.testing.expectEqualStrings("draft", joined_mapped_updated_parsed.value.object.get("status").?.string);

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:generated-lower", .value = "{\"title\":\"generated lower target\",\"status\":\"Review Ready\",\"amount\":12,\"note\":\"generated lower source\"}" },
            .{ .key = "doc:generated-slug", .value = "{\"title\":\"generated slug target\",\"status\":\"Ready To Ship\",\"amount\":15,\"note\":\"generated slug source\"}" },
        },
        .sync_level = .full_index,
    });

    var joined_generated_lower_update_resp = try handlePublicSqlEndpoint(
        &server,
        alloc,
        "UPDATE docs SET title = source.note FROM docs AS source WHERE docs.status = source.status AND lower(docs.status) = 'review ready';",
    );
    defer joined_generated_lower_update_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), joined_generated_lower_update_resp.status);
    var joined_generated_lower_update_parsed = try std.json.parseFromSlice(std.json.Value, alloc, joined_generated_lower_update_resp.body, .{ .allocate = .alloc_always });
    defer joined_generated_lower_update_parsed.deinit();
    try std.testing.expectEqualStrings("write", joined_generated_lower_update_parsed.value.object.get("kind").?.string);
    try std.testing.expectEqualStrings("update_joined_source", joined_generated_lower_update_parsed.value.object.get("statement_kind").?.string);
    try std.testing.expectEqual(@as(i64, 1), joined_generated_lower_update_parsed.value.object.get("result").?.object.get("transformed").?.integer);
    const joined_generated_lower_updated = (try db.get(alloc, "doc:generated-lower")) orelse return error.TestExpectedEqual;
    defer alloc.free(joined_generated_lower_updated);
    var joined_generated_lower_updated_parsed = try std.json.parseFromSlice(std.json.Value, alloc, joined_generated_lower_updated, .{});
    defer joined_generated_lower_updated_parsed.deinit();
    try std.testing.expectEqualStrings("generated lower source", joined_generated_lower_updated_parsed.value.object.get("title").?.string);
    try std.testing.expectEqualStrings("Review Ready", joined_generated_lower_updated_parsed.value.object.get("status").?.string);

    var joined_generated_expression_update_resp = try handlePublicSqlEndpoint(
        &server,
        alloc,
        "UPDATE docs SET title = source.note FROM docs AS source WHERE docs.status = source.status AND replace(lower(docs.status), ' ', '-') = 'ready-to-ship';",
    );
    defer joined_generated_expression_update_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), joined_generated_expression_update_resp.status);
    var joined_generated_expression_update_parsed = try std.json.parseFromSlice(std.json.Value, alloc, joined_generated_expression_update_resp.body, .{ .allocate = .alloc_always });
    defer joined_generated_expression_update_parsed.deinit();
    try std.testing.expectEqualStrings("update_joined_source", joined_generated_expression_update_parsed.value.object.get("statement_kind").?.string);
    try std.testing.expectEqual(@as(i64, 1), joined_generated_expression_update_parsed.value.object.get("result").?.object.get("transformed").?.integer);
    const joined_generated_expression_updated = (try db.get(alloc, "doc:generated-slug")) orelse return error.TestExpectedEqual;
    defer alloc.free(joined_generated_expression_updated);
    var joined_generated_expression_updated_parsed = try std.json.parseFromSlice(std.json.Value, alloc, joined_generated_expression_updated, .{});
    defer joined_generated_expression_updated_parsed.deinit();
    try std.testing.expectEqualStrings("generated slug source", joined_generated_expression_updated_parsed.value.object.get("title").?.string);
    try std.testing.expectEqualStrings("Ready To Ship", joined_generated_expression_updated_parsed.value.object.get("status").?.string);

    var joined_generated_source_rejection_resp = try handlePublicSqlEndpoint(
        &server,
        alloc,
        "UPDATE docs SET title = 'source generated rejected' FROM docs AS source WHERE docs.status = source.status AND lower(source.status) = 'review ready';",
    );
    defer joined_generated_source_rejection_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 400), joined_generated_source_rejection_resp.status);
    try expectPublicSqlDiagnosticBody(
        alloc,
        joined_generated_source_rejection_resp.body,
        "plan",
        "document_sql_write_join_missing_exact_producer",
        "document SQL joined writes require exact target and source producers",
        null,
        null,
    );
    const joined_generated_source_rejection_unchanged = (try db.get(alloc, "doc:generated-lower")) orelse return error.TestExpectedEqual;
    defer alloc.free(joined_generated_source_rejection_unchanged);
    var joined_generated_source_rejection_unchanged_parsed = try std.json.parseFromSlice(std.json.Value, alloc, joined_generated_source_rejection_unchanged, .{});
    defer joined_generated_source_rejection_unchanged_parsed.deinit();
    try std.testing.expectEqualStrings("generated lower source", joined_generated_source_rejection_unchanged_parsed.value.object.get("title").?.string);

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:bounded-update", .value = "{\"title\":\"bounded target\",\"status\":\"bounded-update\",\"category\":\"bounded-release\",\"note\":\"bounded source\"}" },
            .{ .key = "doc:bounded-other", .value = "{\"title\":\"bounded other\",\"status\":\"bounded-other\",\"category\":\"bounded-other\",\"note\":\"other source\"}" },
            .{ .key = "doc:bounded-stale", .value = "{\"title\":\"bounded stale target\",\"status\":\"bounded-stale\",\"category\":\"bounded-stale\",\"note\":\"stale source\"}" },
            .{ .key = "doc:bounded-delete", .value = "{\"title\":\"bounded delete target\",\"status\":\"bounded-delete\",\"category\":\"bounded-delete\",\"note\":\"delete source\"}" },
            .{ .key = "doc:bounded-delete-stale", .value = "{\"title\":\"bounded delete stale target\",\"status\":\"bounded-delete-stale\",\"category\":\"bounded-delete-stale\",\"note\":\"delete stale source\"}" },
        },
        .sync_level = .full_index,
    });

    var joined_bounded_update_resp = try handlePublicSqlEndpoint(
        &server,
        alloc,
        "UPDATE docs SET title = source.note FROM docs AS source WHERE docs.status = source.status AND docs.category = 'bounded-release' RETURNING _id, title, _doc;",
    );
    defer joined_bounded_update_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), joined_bounded_update_resp.status);
    var joined_bounded_update_parsed = try std.json.parseFromSlice(std.json.Value, alloc, joined_bounded_update_resp.body, .{ .allocate = .alloc_always });
    defer joined_bounded_update_parsed.deinit();
    try std.testing.expectEqualStrings("write", joined_bounded_update_parsed.value.object.get("kind").?.string);
    try std.testing.expectEqualStrings("update_joined_source", joined_bounded_update_parsed.value.object.get("statement_kind").?.string);
    const joined_bounded_update_result = joined_bounded_update_parsed.value.object.get("result").?.object;
    try std.testing.expectEqual(@as(i64, 1), joined_bounded_update_result.get("transformed").?.integer);
    const joined_bounded_update_row = joined_bounded_update_result.get("returning").?.array.items[0].object;
    try std.testing.expectEqualStrings("doc:bounded-update", joined_bounded_update_row.get("_id").?.string);
    try std.testing.expectEqualStrings("bounded source", joined_bounded_update_row.get("title").?.string);
    try std.testing.expectEqualStrings("bounded source", joined_bounded_update_row.get("_doc").?.object.get("title").?.string);
    const joined_bounded_updated = (try db.get(alloc, "doc:bounded-update")) orelse return error.TestExpectedEqual;
    defer alloc.free(joined_bounded_updated);
    var joined_bounded_updated_parsed = try std.json.parseFromSlice(std.json.Value, alloc, joined_bounded_updated, .{});
    defer joined_bounded_updated_parsed.deinit();
    try std.testing.expectEqualStrings("bounded source", joined_bounded_updated_parsed.value.object.get("title").?.string);
    const joined_bounded_other = (try db.get(alloc, "doc:bounded-other")) orelse return error.TestExpectedEqual;
    defer alloc.free(joined_bounded_other);
    var joined_bounded_other_parsed = try std.json.parseFromSlice(std.json.Value, alloc, joined_bounded_other, .{});
    defer joined_bounded_other_parsed.deinit();
    try std.testing.expectEqualStrings("bounded other", joined_bounded_other_parsed.value.object.get("title").?.string);

    var joined_bounded_update_no_match_resp = try handlePublicSqlEndpoint(
        &server,
        alloc,
        "UPDATE docs SET title = 'bounded missing' FROM docs AS source WHERE docs.status = source.status AND docs.category = 'bounded-missing';",
    );
    defer joined_bounded_update_no_match_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), joined_bounded_update_no_match_resp.status);
    var joined_bounded_update_no_match_parsed = try std.json.parseFromSlice(std.json.Value, alloc, joined_bounded_update_no_match_resp.body, .{ .allocate = .alloc_always });
    defer joined_bounded_update_no_match_parsed.deinit();
    try std.testing.expectEqual(@as(i64, 0), joined_bounded_update_no_match_parsed.value.object.get("result").?.object.get("transformed").?.integer);

    var joined_bounded_update_stale_resp = try handlePublicSqlEndpoint(
        &server,
        alloc,
        "UPDATE docs SET title = 'bounded stale rejected' FROM docs AS source WHERE docs.status = source.status AND docs.category = 'bounded-stale' AND docs._version = 999999;",
    );
    defer joined_bounded_update_stale_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 409), joined_bounded_update_stale_resp.status);
    try std.testing.expectEqualStrings("version conflict", joined_bounded_update_stale_resp.body);
    const joined_bounded_stale_unchanged = (try db.get(alloc, "doc:bounded-stale")) orelse return error.TestExpectedEqual;
    defer alloc.free(joined_bounded_stale_unchanged);
    var joined_bounded_stale_unchanged_parsed = try std.json.parseFromSlice(std.json.Value, alloc, joined_bounded_stale_unchanged, .{});
    defer joined_bounded_stale_unchanged_parsed.deinit();
    try std.testing.expectEqualStrings("bounded stale target", joined_bounded_stale_unchanged_parsed.value.object.get("title").?.string);

    const bounded_row_cap_payload = try documentKeyScanPayloadAlloc(alloc, "doc:bounded-row-cap-", sql_adapter.default_document_sql_bounded_scan_rows + 1);
    defer alloc.free(bounded_row_cap_payload);
    read_source.forced_scan_payload = bounded_row_cap_payload;
    var joined_bounded_row_cap_resp = try handlePublicSqlEndpoint(
        &server,
        alloc,
        "UPDATE docs SET title = 'bounded row cap rejected' FROM docs AS source WHERE docs.status = source.status AND docs.category = 'bounded-row-cap';",
    );
    read_source.forced_scan_payload = null;
    defer joined_bounded_row_cap_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 400), joined_bounded_row_cap_resp.status);
    try expectPublicSqlDiagnosticBody(
        alloc,
        joined_bounded_row_cap_resp.body,
        "execute",
        "document_sql_bounded_scan_row_cap_exceeded",
        "document SQL bounded scan row cap exceeded",
        null,
        null,
    );

    const bounded_byte_cap_len: usize = @intCast(sql_adapter.default_document_sql_bounded_scan_bytes + 1);
    const bounded_byte_cap_payload = try alloc.alloc(u8, bounded_byte_cap_len);
    defer alloc.free(bounded_byte_cap_payload);
    @memset(bounded_byte_cap_payload, 'x');
    read_source.forced_scan_payload = bounded_byte_cap_payload;
    var joined_bounded_byte_cap_resp = try handlePublicSqlEndpoint(
        &server,
        alloc,
        "DELETE FROM docs USING docs AS source WHERE docs.status = source.status AND docs.category = 'bounded-byte-cap';",
    );
    read_source.forced_scan_payload = null;
    defer joined_bounded_byte_cap_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 400), joined_bounded_byte_cap_resp.status);
    try expectPublicSqlDiagnosticBody(
        alloc,
        joined_bounded_byte_cap_resp.body,
        "execute",
        "document_sql_bounded_scan_byte_cap_exceeded",
        "document SQL bounded scan byte cap exceeded",
        null,
        null,
    );

    var joined_bounded_delete_resp = try handlePublicSqlEndpoint(
        &server,
        alloc,
        "DELETE FROM docs USING docs AS source WHERE docs.status = source.status AND docs.category = 'bounded-delete' RETURNING _id, title, _doc;",
    );
    defer joined_bounded_delete_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), joined_bounded_delete_resp.status);
    var joined_bounded_delete_parsed = try std.json.parseFromSlice(std.json.Value, alloc, joined_bounded_delete_resp.body, .{ .allocate = .alloc_always });
    defer joined_bounded_delete_parsed.deinit();
    try std.testing.expectEqualStrings("write", joined_bounded_delete_parsed.value.object.get("kind").?.string);
    try std.testing.expectEqualStrings("delete_joined_source", joined_bounded_delete_parsed.value.object.get("statement_kind").?.string);
    const joined_bounded_delete_result = joined_bounded_delete_parsed.value.object.get("result").?.object;
    try std.testing.expectEqual(@as(i64, 1), joined_bounded_delete_result.get("deleted").?.integer);
    const joined_bounded_delete_row = joined_bounded_delete_result.get("returning").?.array.items[0].object;
    try std.testing.expectEqualStrings("doc:bounded-delete", joined_bounded_delete_row.get("_id").?.string);
    try std.testing.expectEqualStrings("bounded delete target", joined_bounded_delete_row.get("title").?.string);
    try std.testing.expectEqualStrings("bounded delete target", joined_bounded_delete_row.get("_doc").?.object.get("title").?.string);
    if (try db.get(alloc, "doc:bounded-delete")) |deleted_doc| {
        defer alloc.free(deleted_doc);
        return error.TestUnexpectedResult;
    }

    var joined_bounded_delete_no_match_resp = try handlePublicSqlEndpoint(
        &server,
        alloc,
        "DELETE FROM docs USING docs AS source WHERE docs.status = source.status AND docs.category = 'bounded-delete-missing';",
    );
    defer joined_bounded_delete_no_match_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), joined_bounded_delete_no_match_resp.status);
    var joined_bounded_delete_no_match_parsed = try std.json.parseFromSlice(std.json.Value, alloc, joined_bounded_delete_no_match_resp.body, .{ .allocate = .alloc_always });
    defer joined_bounded_delete_no_match_parsed.deinit();
    try std.testing.expectEqual(@as(i64, 0), joined_bounded_delete_no_match_parsed.value.object.get("result").?.object.get("deleted").?.integer);

    var joined_bounded_delete_stale_resp = try handlePublicSqlEndpoint(
        &server,
        alloc,
        "DELETE FROM docs USING docs AS source WHERE docs.status = source.status AND docs.category = 'bounded-delete-stale' AND docs._version = 999999;",
    );
    defer joined_bounded_delete_stale_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 409), joined_bounded_delete_stale_resp.status);
    try std.testing.expectEqualStrings("version conflict", joined_bounded_delete_stale_resp.body);
    const joined_bounded_delete_stale_unchanged = (try db.get(alloc, "doc:bounded-delete-stale")) orelse return error.TestExpectedEqual;
    defer alloc.free(joined_bounded_delete_stale_unchanged);
    var joined_bounded_delete_stale_unchanged_parsed = try std.json.parseFromSlice(std.json.Value, alloc, joined_bounded_delete_stale_unchanged, .{});
    defer joined_bounded_delete_stale_unchanged_parsed.deinit();
    try std.testing.expectEqualStrings("bounded delete stale target", joined_bounded_delete_stale_unchanged_parsed.value.object.get("title").?.string);

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:mapped-delete", .value = "{\"title\":\"mapped delete target\",\"status\":\"delete-ready\",\"note\":\"delete source\"}" },
            .{ .key = "doc:mapped-delete-stale", .value = "{\"title\":\"mapped stale target\",\"status\":\"delete-stale\",\"note\":\"stale source\"}" },
            .{ .key = "doc:generated-delete", .value = "{\"title\":\"generated delete target\",\"status\":\"delete-generated\",\"amount\":-7,\"note\":\"delete generated source\"}" },
            .{ .key = "doc:generated-delete-stale", .value = "{\"title\":\"generated stale target\",\"status\":\"delete-generated-stale\",\"amount\":-8,\"note\":\"delete generated stale source\"}" },
        },
        .sync_level = .full_index,
    });

    var joined_mapped_delete_resp = try handlePublicSqlEndpoint(
        &server,
        alloc,
        "DELETE FROM docs USING docs AS source WHERE docs.status = source.status AND docs.status = 'delete-ready';",
    );
    defer joined_mapped_delete_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), joined_mapped_delete_resp.status);
    var joined_mapped_delete_parsed = try std.json.parseFromSlice(std.json.Value, alloc, joined_mapped_delete_resp.body, .{ .allocate = .alloc_always });
    defer joined_mapped_delete_parsed.deinit();
    try std.testing.expectEqualStrings("write", joined_mapped_delete_parsed.value.object.get("kind").?.string);
    try std.testing.expectEqualStrings("delete_joined_source", joined_mapped_delete_parsed.value.object.get("statement_kind").?.string);
    try std.testing.expectEqual(@as(i64, 1), joined_mapped_delete_parsed.value.object.get("result").?.object.get("deleted").?.integer);
    if (try db.get(alloc, "doc:mapped-delete")) |deleted_doc| {
        defer alloc.free(deleted_doc);
        return error.TestUnexpectedResult;
    }

    var joined_mapped_delete_no_match_resp = try handlePublicSqlEndpoint(
        &server,
        alloc,
        "DELETE FROM docs USING docs AS source WHERE docs.status = source.status AND docs.status = 'delete-missing';",
    );
    defer joined_mapped_delete_no_match_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), joined_mapped_delete_no_match_resp.status);
    var joined_mapped_delete_no_match_parsed = try std.json.parseFromSlice(std.json.Value, alloc, joined_mapped_delete_no_match_resp.body, .{ .allocate = .alloc_always });
    defer joined_mapped_delete_no_match_parsed.deinit();
    try std.testing.expectEqual(@as(i64, 0), joined_mapped_delete_no_match_parsed.value.object.get("result").?.object.get("deleted").?.integer);

    var joined_mapped_delete_stale_resp = try handlePublicSqlEndpoint(
        &server,
        alloc,
        "DELETE FROM docs USING docs AS source WHERE docs.status = source.status AND docs.status = 'delete-stale' AND docs._version = 999999;",
    );
    defer joined_mapped_delete_stale_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 409), joined_mapped_delete_stale_resp.status);
    try std.testing.expectEqualStrings("version conflict", joined_mapped_delete_stale_resp.body);
    const joined_mapped_delete_stale_unchanged = (try db.get(alloc, "doc:mapped-delete-stale")) orelse return error.TestExpectedEqual;
    defer alloc.free(joined_mapped_delete_stale_unchanged);
    var joined_mapped_delete_stale_unchanged_parsed = try std.json.parseFromSlice(std.json.Value, alloc, joined_mapped_delete_stale_unchanged, .{});
    defer joined_mapped_delete_stale_unchanged_parsed.deinit();
    try std.testing.expectEqualStrings("mapped stale target", joined_mapped_delete_stale_unchanged_parsed.value.object.get("title").?.string);

    var joined_generated_delete_resp = try handlePublicSqlEndpoint(
        &server,
        alloc,
        "DELETE FROM docs USING docs AS source WHERE docs.status = source.status AND abs(docs.amount) = 7;",
    );
    defer joined_generated_delete_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), joined_generated_delete_resp.status);
    var joined_generated_delete_parsed = try std.json.parseFromSlice(std.json.Value, alloc, joined_generated_delete_resp.body, .{ .allocate = .alloc_always });
    defer joined_generated_delete_parsed.deinit();
    try std.testing.expectEqualStrings("write", joined_generated_delete_parsed.value.object.get("kind").?.string);
    try std.testing.expectEqualStrings("delete_joined_source", joined_generated_delete_parsed.value.object.get("statement_kind").?.string);
    try std.testing.expectEqual(@as(i64, 1), joined_generated_delete_parsed.value.object.get("result").?.object.get("deleted").?.integer);
    if (try db.get(alloc, "doc:generated-delete")) |deleted_doc| {
        defer alloc.free(deleted_doc);
        return error.TestUnexpectedResult;
    }

    var joined_generated_delete_no_match_resp = try handlePublicSqlEndpoint(
        &server,
        alloc,
        "DELETE FROM docs USING docs AS source WHERE docs.status = source.status AND abs(docs.amount) = 999;",
    );
    defer joined_generated_delete_no_match_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), joined_generated_delete_no_match_resp.status);
    var joined_generated_delete_no_match_parsed = try std.json.parseFromSlice(std.json.Value, alloc, joined_generated_delete_no_match_resp.body, .{ .allocate = .alloc_always });
    defer joined_generated_delete_no_match_parsed.deinit();
    try std.testing.expectEqual(@as(i64, 0), joined_generated_delete_no_match_parsed.value.object.get("result").?.object.get("deleted").?.integer);

    var joined_generated_delete_stale_resp = try handlePublicSqlEndpoint(
        &server,
        alloc,
        "DELETE FROM docs USING docs AS source WHERE docs.status = source.status AND abs(docs.amount) = 8 AND docs._version = 999999;",
    );
    defer joined_generated_delete_stale_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 409), joined_generated_delete_stale_resp.status);
    try std.testing.expectEqualStrings("version conflict", joined_generated_delete_stale_resp.body);
    const joined_generated_delete_stale_unchanged = (try db.get(alloc, "doc:generated-delete-stale")) orelse return error.TestExpectedEqual;
    defer alloc.free(joined_generated_delete_stale_unchanged);
    var joined_generated_delete_stale_unchanged_parsed = try std.json.parseFromSlice(std.json.Value, alloc, joined_generated_delete_stale_unchanged, .{});
    defer joined_generated_delete_stale_unchanged_parsed.deinit();
    try std.testing.expectEqualStrings("generated stale target", joined_generated_delete_stale_unchanged_parsed.value.object.get("title").?.string);

    var joined_no_match_resp = try handlePublicSqlEndpoint(
        &server,
        alloc,
        "UPDATE docs SET status = 'missing' FROM docs AS source WHERE docs._id = source._id AND docs._id = 'doc:missing';",
    );
    defer joined_no_match_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), joined_no_match_resp.status);
    var joined_no_match_parsed = try std.json.parseFromSlice(std.json.Value, alloc, joined_no_match_resp.body, .{ .allocate = .alloc_always });
    defer joined_no_match_parsed.deinit();
    try std.testing.expectEqual(@as(i64, 0), joined_no_match_parsed.value.object.get("result").?.object.get("transformed").?.integer);

    var joined_stale_resp = try handlePublicSqlEndpoint(
        &server,
        alloc,
        "UPDATE docs SET status = 'stale' FROM docs AS source WHERE docs._id = source._id AND docs._id = 'doc:c' AND docs._version = 999999;",
    );
    defer joined_stale_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 409), joined_stale_resp.status);
    try std.testing.expectEqualStrings("version conflict", joined_stale_resp.body);
    const joined_stale_unchanged = (try db.get(alloc, "doc:c")) orelse return error.TestExpectedEqual;
    defer alloc.free(joined_stale_unchanged);
    var joined_stale_unchanged_parsed = try std.json.parseFromSlice(std.json.Value, alloc, joined_stale_unchanged, .{});
    defer joined_stale_unchanged_parsed.deinit();
    try std.testing.expectEqualStrings("copied", joined_stale_unchanged_parsed.value.object.get("status").?.string);

    var write_row_filters = try alloc.alloc(usermgr.RowFilterEntry, 1);
    write_row_filters[0] = try usermgr.RowFilterEntry.initOwned(alloc, "docs", "{\"term\":{\"status\":\"active\"}}");
    var filtered_writer = AuthenticatedIdentity{
        .username = try alloc.dupe(u8, "document_sql_writer"),
        .row_filter = write_row_filters,
    };
    defer filtered_writer.deinit(alloc);
    var joined_filtered_resp = try handlePublicSqlDirect(
        &server,
        alloc,
        "UPDATE docs SET title = 'filtered rejected' FROM docs AS source WHERE docs._id = source._id AND docs._id = 'doc:c';",
        filtered_writer,
    );
    defer joined_filtered_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 403), joined_filtered_resp.status);
    try std.testing.expectEqualStrings("row filter rejected sql write", joined_filtered_resp.body);
    const joined_filtered_unchanged = (try db.get(alloc, "doc:c")) orelse return error.TestExpectedEqual;
    defer alloc.free(joined_filtered_unchanged);
    var joined_filtered_unchanged_parsed = try std.json.parseFromSlice(std.json.Value, alloc, joined_filtered_unchanged, .{});
    defer joined_filtered_unchanged_parsed.deinit();
    try std.testing.expectEqualStrings("gamma", joined_filtered_unchanged_parsed.value.object.get("title").?.string);
    try std.testing.expectEqualStrings("copied", joined_filtered_unchanged_parsed.value.object.get("status").?.string);

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:dup-target", .value = "{\"title\":\"duplicate target\",\"status\":\"duplicate-source\",\"category\":\"bounded-duplicate-target\"}" },
            .{ .key = "doc:dup-source", .value = "{\"title\":\"duplicate source\",\"status\":\"duplicate-source\"}" },
        },
        .sync_level = .full_index,
    });
    var joined_duplicate_source_resp = try handlePublicSqlEndpoint(
        &server,
        alloc,
        "UPDATE docs SET title = 'duplicate rejected' FROM docs AS source WHERE docs.status = source.status AND docs._id = 'doc:dup-target';",
    );
    defer joined_duplicate_source_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 400), joined_duplicate_source_resp.status);
    try expectPublicSqlDiagnosticBody(
        alloc,
        joined_duplicate_source_resp.body,
        "execute",
        "document_sql_write_duplicate_source",
        "document SQL joined write source produced duplicate join keys",
        null,
        null,
    );
    const duplicate_source_unchanged = (try db.get(alloc, "doc:dup-target")) orelse return error.TestExpectedEqual;
    defer alloc.free(duplicate_source_unchanged);
    var duplicate_source_unchanged_parsed = try std.json.parseFromSlice(std.json.Value, alloc, duplicate_source_unchanged, .{});
    defer duplicate_source_unchanged_parsed.deinit();
    try std.testing.expectEqualStrings("duplicate target", duplicate_source_unchanged_parsed.value.object.get("title").?.string);

    var joined_bounded_duplicate_source_resp = try handlePublicSqlEndpoint(
        &server,
        alloc,
        "UPDATE docs SET title = 'bounded duplicate rejected' FROM docs AS source WHERE docs.status = source.status AND docs.category = 'bounded-duplicate-target';",
    );
    defer joined_bounded_duplicate_source_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 400), joined_bounded_duplicate_source_resp.status);
    try expectPublicSqlDiagnosticBody(
        alloc,
        joined_bounded_duplicate_source_resp.body,
        "execute",
        "document_sql_write_duplicate_source",
        "document SQL joined write source produced duplicate join keys",
        null,
        null,
    );
    const bounded_duplicate_source_unchanged = (try db.get(alloc, "doc:dup-target")) orelse return error.TestExpectedEqual;
    defer alloc.free(bounded_duplicate_source_unchanged);
    var bounded_duplicate_source_unchanged_parsed = try std.json.parseFromSlice(std.json.Value, alloc, bounded_duplicate_source_unchanged, .{});
    defer bounded_duplicate_source_unchanged_parsed.deinit();
    try std.testing.expectEqualStrings("duplicate target", bounded_duplicate_source_unchanged_parsed.value.object.get("title").?.string);

    var joined_delete_resp = try handlePublicSqlEndpoint(
        &server,
        alloc,
        "DELETE FROM docs USING docs AS source WHERE docs._id = source._id AND docs._id = 'doc:c';",
    );
    defer joined_delete_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), joined_delete_resp.status);
    var joined_delete_parsed = try std.json.parseFromSlice(std.json.Value, alloc, joined_delete_resp.body, .{ .allocate = .alloc_always });
    defer joined_delete_parsed.deinit();
    try std.testing.expectEqualStrings("write", joined_delete_parsed.value.object.get("kind").?.string);
    try std.testing.expectEqualStrings("delete_joined_source", joined_delete_parsed.value.object.get("statement_kind").?.string);
    try std.testing.expectEqual(@as(i64, 1), joined_delete_parsed.value.object.get("result").?.object.get("deleted").?.integer);
    if (try db.get(alloc, "doc:c")) |deleted_doc| {
        defer alloc.free(deleted_doc);
        return error.TestUnexpectedResult;
    }
}

test "api public SQL endpoint rejects document joined generated proof gaps" {
    const alloc = std.testing.allocator;
    const ready_generated_schema =
        \\{"version":1,"storage_mode":"document","default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"title":{"type":"text"},"status":{"type":"keyword","x-antfly-cardinality-proof":"unique"},"status_lower":{"type":"keyword","generated":{"op":"lower","field":"status"}}},"additionalProperties":true}}}}
    ;
    try expectDocumentJoinedWriteProofDiagnostic(
        alloc,
        ready_generated_schema,
        "public-sql-joined-source-generated-rejection",
        "UPDATE docs SET title = 'Rejected' FROM docs AS source WHERE docs.status = source.status AND lower(source.status) = 'draft';",
        "document_sql_write_join_missing_exact_producer",
        "document SQL joined writes require exact target and source producers",
    );

    const missing_generated_schema =
        \\{"version":1,"storage_mode":"document","default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"title":{"type":"text"},"status":{"type":"keyword","x-antfly-cardinality-proof":"unique"}},"additionalProperties":true}}}}
    ;
    try expectDocumentJoinedWriteProofDiagnostic(
        alloc,
        missing_generated_schema,
        "public-sql-joined-generated-missing-index",
        "UPDATE docs SET title = 'Rejected' FROM docs AS source WHERE docs.status = source.status AND lower(docs.status) = 'draft';",
        "document_sql_write_join_missing_index_proof",
        "document SQL joined write requires indexed join fields",
    );

    const stale_generated_schema =
        \\{"version":1,"storage_mode":"document","default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"title":{"type":"text"},"status":{"type":"keyword","x-antfly-cardinality-proof":"unique"},"status_lower":{"type":"keyword","generated":{"op":"lower","field":"status"},"x-antfly-index-lifecycle":"building"}},"additionalProperties":true}}}}
    ;
    try expectDocumentJoinedWriteProofDiagnostic(
        alloc,
        stale_generated_schema,
        "public-sql-joined-generated-stale-index",
        "UPDATE docs SET title = 'Rejected' FROM docs AS source WHERE docs.status = source.status AND lower(docs.status) = 'draft';",
        "document_sql_write_join_stale_index_proof",
        "document SQL joined write cannot use stale or rebuilding join indexes",
    );

    const partial_generated_schema =
        \\{"version":1,"storage_mode":"document","default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"title":{"type":"text"},"status":{"type":"keyword","x-antfly-cardinality-proof":"unique"},"status_lower":{"type":"keyword","generated":{"op":"lower","field":"status"},"x-antfly-index-where":{"all":[{"field":"status_lower","op":"is_not_null"}]}}},"additionalProperties":true}}}}
    ;
    try expectDocumentJoinedWriteProofDiagnostic(
        alloc,
        partial_generated_schema,
        "public-sql-joined-generated-partial-index",
        "UPDATE docs SET title = 'Rejected' FROM docs AS source WHERE docs.status = source.status AND lower(docs.status) = 'draft';",
        "document_sql_write_join_partial_index_proof",
        "document SQL joined write cannot use partial join indexes",
    );

    const ordered_generated_schema =
        \\{"version":1,"storage_mode":"document","default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"title":{"type":"text"},"status":{"type":"keyword","x-antfly-cardinality-proof":"unique"},"status_lower":{"type":"keyword","generated":{"op":"lower","field":"status"},"x-antfly-index-name":"status_lower_idx","x-antfly-index-keys":[{"column":"status_lower","direction":"desc"}]}},"additionalProperties":true}}}}
    ;
    try expectDocumentJoinedWriteProofDiagnostic(
        alloc,
        ordered_generated_schema,
        "public-sql-joined-generated-ordered-index",
        "UPDATE docs SET title = 'Rejected' FROM docs AS source WHERE docs.status = source.status AND lower(docs.status) = 'draft';",
        "document_sql_write_join_ordered_index_proof",
        "document SQL joined write cannot use ordered or composite join indexes",
    );
}

test "api public SQL endpoint rejects document joined stable write diagnostics" {
    const alloc = std.testing.allocator;
    const no_cardinality_schema =
        \\{"version":1,"storage_mode":"document","default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"title":{"type":"text"},"status":{"type":"keyword"}},"additionalProperties":true}}}}
    ;
    try expectDocumentJoinedWriteProofDiagnostic(
        alloc,
        no_cardinality_schema,
        "public-sql-joined-update-missing-cardinality",
        "UPDATE docs SET title = 'Rejected' FROM docs AS source WHERE docs.status = source.status AND docs.status = 'draft';",
        "document_sql_write_join_missing_cardinality_proof",
        "document SQL joined write lacks a document cardinality proof",
    );
    try expectDocumentJoinedWriteProofDiagnostic(
        alloc,
        no_cardinality_schema,
        "public-sql-joined-delete-missing-cardinality",
        "DELETE FROM docs USING docs AS source WHERE docs.status = source.status AND docs.status = 'draft';",
        "document_sql_write_join_missing_cardinality_proof",
        "document SQL joined write lacks a document cardinality proof",
    );

    const ambiguous_source_schema =
        \\{"version":1,"storage_mode":"document","default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"title":{"type":"text"}},"additionalProperties":true}}}}
    ;
    try expectDocumentJoinedWriteProofDiagnostic(
        alloc,
        ambiguous_source_schema,
        "public-sql-joined-source-assignment-ambiguous",
        "UPDATE docs AS source SET title = source.title FROM docs AS source WHERE source._id = source._id AND source._id = 'doc:a';",
        "document_sql_write_source_assignment_ambiguous_reference",
        "document SQL source assignment has an ambiguous target/source reference",
    );
}

test "api public SQL endpoint executes SQL reads through typed row plan ingress" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"amount":{"type":"numeric"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/public-sql-read", .{tmp.sub_path});
    defer alloc.free(path);

    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();
    try db.applyTableSchemaJson(alloc, schema_json, .{});

    var read_source = table_reads.BoundTableReadSource.init("usage_records", 1, &db, raft_mod.read_gate.noopReadableLeaseRequester());
    var write_source = table_writes.BoundTableWriteSource.init("usage_records", &db);

    const FakeSource = struct {
        tables: [1]metadata_table_manager.TableRecord,

        fn iface(self: *@This()) StatusSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{
                .status = try status(ptr),
                .tables = self.tables[0..],
                .ranges = &.{},
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = FakeSource{ .tables = .{.{
        .table_id = 1,
        .name = "usage_records",
        .schema_json = schema_json,
        .desired_replica_count = 1,
    }} };
    var server = ApiHttpServer.init(alloc, .{}, source.iface(), read_source.source(), write_source.source());
    defer server.deinit();

    var insert_resp = try server.handle(.{
        .method = .POST,
        .uri = "/tables/usage_records/rows/batch",
        .content_type = "application/json",
        .body = "{\"operations\":[{\"op\":\"insert\",\"row\":{\"id\":\"u1\",\"status\":\"open\",\"amount\":10}},{\"op\":\"insert\",\"row\":{\"id\":\"u2\",\"status\":\"closed\",\"amount\":90}},{\"op\":\"insert\",\"row\":{\"id\":\"u3\",\"status\":\"open\",\"amount\":20}}]}",
    });
    defer insert_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 201), insert_resp.status);

    var query_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"SELECT id, amount FROM usage_records WHERE status = 'open' ORDER BY amount DESC;\"}",
    });
    defer query_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), query_resp.status);
    try std.testing.expectEqualStrings("application/json", query_resp.content_type.?);

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, query_resp.body, .{ .allocate = .alloc_always });
    defer parsed.deinit();
    try std.testing.expectEqualStrings("read", parsed.value.object.get("kind").?.string);
    try std.testing.expectEqualStrings("query", parsed.value.object.get("statement_kind").?.string);
    try std.testing.expect(parsed.value.object.get("session_id").?.integer > 0);
    const result = parsed.value.object.get("result").?.object;
    try std.testing.expectEqual(@as(i64, 2), result.get("total").?.integer);
    const rows = result.get("rows").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), rows.len);
    try std.testing.expectEqualStrings("u3", rows[0].object.get("id").?.string);
    try std.testing.expectEqual(@as(i64, 20), rows[0].object.get("amount").?.integer);
    try std.testing.expectEqualStrings("u1", rows[1].object.get("id").?.string);
    try std.testing.expectEqual(@as(i64, 10), rows[1].object.get("amount").?.integer);

    var exists_subquery_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"SELECT id FROM usage_records WHERE EXISTS (SELECT 1 FROM usage_records WHERE status = 'open');\"}",
    });
    defer exists_subquery_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 501), exists_subquery_resp.status);
    try expectPublicSqlDiagnosticBody(alloc, exists_subquery_resp.body, "plan", "unsupported_sql_statement", "unsupported sql statement", null, null);
    try expectPublicSqlDiagnosticMissingNativeModel(alloc, exists_subquery_resp.body, "semijoin execution for EXISTS subquery predicate");

    var quantified_subquery_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"SELECT id FROM usage_records WHERE status = ANY (SELECT status FROM usage_records);\"}",
    });
    defer quantified_subquery_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 501), quantified_subquery_resp.status);
    try expectPublicSqlDiagnosticBody(alloc, quantified_subquery_resp.body, "plan", "unsupported_sql_statement", "unsupported sql statement", null, null);
    try expectPublicSqlDiagnosticMissingNativeModel(alloc, quantified_subquery_resp.body, "quantified comparison execution for read-subquery predicate");

    var describe_exists_subquery = try server.handlePublicSqlDescribeRequestResult(.{
        .sql = "SELECT id FROM usage_records WHERE EXISTS (SELECT 1 FROM usage_records WHERE status = 'open');",
    }, null);
    defer describe_exists_subquery.deinit(alloc);
    switch (describe_exists_subquery) {
        .response => |response| {
            try std.testing.expectEqual(@as(u16, 501), response.status);
            try expectPublicSqlDiagnosticBody(alloc, response.body, "plan", "unsupported_sql_statement", "unsupported sql statement", null, null);
            try expectPublicSqlDiagnosticMissingNativeModel(alloc, response.body, "semijoin execution for EXISTS subquery predicate");
        },
        .result => return error.TestUnexpectedResult,
    }

    var aggregate_claim_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"SELECT status, COUNT(*) AS count FROM usage_records GROUP BY status FOR UPDATE;\"}",
    });
    defer aggregate_claim_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 501), aggregate_claim_resp.status);
    try expectPublicSqlDiagnosticBody(alloc, aggregate_claim_resp.body, "plan", "unsupported_sql_statement", "unsupported sql statement", null, null);
    try expectPublicSqlDiagnosticMissingNativeModel(alloc, aggregate_claim_resp.body, "lockable base-row source for aggregate row claim");

    var describe_aggregate_claim = try server.handlePublicSqlDescribeRequestResult(.{
        .sql = "SELECT status, COUNT(*) AS count FROM usage_records GROUP BY status FOR UPDATE;",
    }, null);
    defer describe_aggregate_claim.deinit(alloc);
    switch (describe_aggregate_claim) {
        .response => |response| {
            try std.testing.expectEqual(@as(u16, 501), response.status);
            try expectPublicSqlDiagnosticBody(alloc, response.body, "plan", "unsupported_sql_statement", "unsupported sql statement", null, null);
            try expectPublicSqlDiagnosticMissingNativeModel(alloc, response.body, "lockable base-row source for aggregate row claim");
        },
        .result => return error.TestUnexpectedResult,
    }

    var join_claim_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"SELECT target.id FROM usage_records AS target JOIN usage_records AS source ON target.status = source.status FOR UPDATE;\"}",
    });
    defer join_claim_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 501), join_claim_resp.status);
    try expectPublicSqlDiagnosticBody(alloc, join_claim_resp.body, "plan", "unsupported_sql_statement", "unsupported sql statement", null, null);
    try expectPublicSqlDiagnosticMissingNativeModel(alloc, join_claim_resp.body, "lockable base-row source for join row claim");

    var window_claim_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"SELECT id, row_number() OVER (ORDER BY amount) AS rn FROM usage_records FOR UPDATE;\"}",
    });
    defer window_claim_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 501), window_claim_resp.status);
    try expectPublicSqlDiagnosticBody(alloc, window_claim_resp.body, "plan", "unsupported_sql_statement", "unsupported sql statement", null, null);
    try expectPublicSqlDiagnosticMissingNativeModel(alloc, window_claim_resp.body, "lockable base-row source for window row claim");

    var cte_claim_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"WITH locked_usage AS (SELECT id FROM usage_records FOR UPDATE) SELECT id FROM locked_usage;\"}",
    });
    defer cte_claim_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 501), cte_claim_resp.status);
    try expectPublicSqlDiagnosticBody(alloc, cte_claim_resp.body, "plan", "unsupported_sql_statement", "unsupported sql statement", null, null);
    try expectPublicSqlDiagnosticMissingNativeModel(alloc, cte_claim_resp.body, "lockable base-row source for materialized CTE row claim");
}

test "api public SQL endpoint executes domain SQL fixtures through typed row plan parity" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"kind":{"type":"keyword"},"tenant_id":{"type":"keyword"},"subject_id":{"type":"keyword"},"access_role":{"type":"keyword"},"resource_id":{"type":"keyword"},"status":{"type":"keyword"},"amount":{"type":"numeric"},"priority":{"type":"numeric"},"created_at":{"type":"numeric"},"migrated":{"type":"boolean"},"updated_at":{"type":"numeric"}},"required":["id","kind"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/domain-sql-typed-parity", .{tmp.sub_path});
    defer alloc.free(path);

    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();
    try db.applyTableSchemaJson(alloc, schema_json, .{});

    var read_source = table_reads.BoundTableReadSource.init("domain_records", 1, &db, raft_mod.read_gate.noopReadableLeaseRequester());
    var write_source = table_writes.BoundTableWriteSource.init("domain_records", &db);

    const FakeSource = struct {
        tables: [1]metadata_table_manager.TableRecord,

        fn iface(self: *@This()) StatusSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{
                .status = try status(ptr),
                .tables = self.tables[0..],
                .ranges = &.{},
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = FakeSource{ .tables = .{.{
        .table_id = 1,
        .name = "domain_records",
        .schema_json = schema_json,
        .desired_replica_count = 1,
    }} };
    var server = ApiHttpServer.init(alloc, .{}, source.iface(), read_source.source(), write_source.source());
    defer server.deinit();

    var insert_resp = try server.handle(.{
        .method = .POST,
        .uri = "/tables/domain_records/rows/batch",
        .content_type = "application/json",
        .body = "{\"operations\":[{\"op\":\"insert\",\"row\":{\"id\":\"u1\",\"kind\":\"usage\",\"tenant_id\":\"t1\",\"status\":\"active\",\"amount\":10,\"created_at\":10}},{\"op\":\"insert\",\"row\":{\"id\":\"u2\",\"kind\":\"usage\",\"tenant_id\":\"t1\",\"status\":\"active\",\"amount\":25,\"created_at\":20}},{\"op\":\"insert\",\"row\":{\"id\":\"u3\",\"kind\":\"usage\",\"tenant_id\":\"t1\",\"status\":\"archived\",\"amount\":5,\"created_at\":30}},{\"op\":\"insert\",\"row\":{\"id\":\"u4\",\"kind\":\"usage\",\"tenant_id\":\"t2\",\"status\":\"active\",\"amount\":7,\"created_at\":40}},{\"op\":\"insert\",\"row\":{\"id\":\"m1\",\"kind\":\"membership\",\"tenant_id\":\"t1\",\"subject_id\":\"user:1\",\"access_role\":\"admin\",\"resource_id\":\"project:1\",\"status\":\"active\",\"migrated\":true}},{\"op\":\"insert\",\"row\":{\"id\":\"m2\",\"kind\":\"membership\",\"tenant_id\":\"t1\",\"subject_id\":\"user:1\",\"access_role\":\"viewer\",\"resource_id\":\"project:2\",\"status\":\"active\",\"migrated\":true}},{\"op\":\"insert\",\"row\":{\"id\":\"m3\",\"kind\":\"membership\",\"tenant_id\":\"t2\",\"subject_id\":\"user:1\",\"access_role\":\"admin\",\"resource_id\":\"project:9\",\"status\":\"active\",\"migrated\":true}},{\"op\":\"insert\",\"row\":{\"id\":\"w1\",\"kind\":\"wake_job\",\"tenant_id\":\"t1\",\"status\":\"queued\",\"priority\":5,\"created_at\":100}},{\"op\":\"insert\",\"row\":{\"id\":\"w2\",\"kind\":\"wake_job\",\"tenant_id\":\"t1\",\"status\":\"queued\",\"priority\":10,\"created_at\":90}},{\"op\":\"insert\",\"row\":{\"id\":\"w3\",\"kind\":\"wake_job\",\"tenant_id\":\"t1\",\"status\":\"running\",\"priority\":100,\"created_at\":80}},{\"op\":\"insert\",\"row\":{\"id\":\"b1\",\"kind\":\"backfill\",\"tenant_id\":\"t1\",\"status\":\"validated\",\"migrated\":true,\"updated_at\":1000}},{\"op\":\"insert\",\"row\":{\"id\":\"b2\",\"kind\":\"backfill\",\"tenant_id\":\"t1\",\"status\":\"pending\",\"migrated\":false,\"updated_at\":1001}}]}",
    });
    defer insert_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 201), insert_resp.status);

    var sql_usage_resp = try server.handlePublicSql("{\"sql\":\"SELECT tenant_id, status, SUM(amount) AS amount_sum, COUNT(*) AS row_count FROM domain_records WHERE kind = 'usage' GROUP BY tenant_id, status ORDER BY tenant_id ASC, status ASC;\"}", null);
    defer sql_usage_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), sql_usage_resp.status);
    var parsed_sql_usage = try std.json.parseFromSlice(std.json.Value, alloc, sql_usage_resp.body, .{ .allocate = .alloc_always });
    defer parsed_sql_usage.deinit();
    const sql_usage_rows = parsed_sql_usage.value.object.get("result").?.object.get("rows").?.array.items;
    try std.testing.expectEqual(@as(usize, 3), sql_usage_rows.len);
    try std.testing.expectEqualStrings("t1", sql_usage_rows[0].object.get("tenant_id").?.string);
    try std.testing.expectEqualStrings("active", sql_usage_rows[0].object.get("status").?.string);
    try std.testing.expectEqual(@as(i64, 35), sql_usage_rows[0].object.get("amount_sum").?.integer);
    try std.testing.expectEqual(@as(i64, 2), sql_usage_rows[0].object.get("row_count").?.integer);

    var typed_usage_resp = try server.handlePublicTableRowsAggregate("domain_records", "{\"aggregate\":{\"source\":{\"where\":{\"field\":\"kind\",\"op\":\"eq\",\"value\":\"usage\"}},\"group_by\":[\"tenant_id\",\"status\"],\"aggregations\":[{\"name\":\"amount_sum\",\"op\":\"sum\",\"field\":\"amount\"},{\"name\":\"row_count\",\"op\":\"count\"}],\"order_by\":[{\"field\":\"tenant_id\"},{\"field\":\"status\"}]}}", null);
    defer typed_usage_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), typed_usage_resp.status);
    var parsed_typed_usage = try std.json.parseFromSlice(std.json.Value, alloc, typed_usage_resp.body, .{ .allocate = .alloc_always });
    defer parsed_typed_usage.deinit();
    const typed_usage_rows = parsed_typed_usage.value.object.get("rows").?.array.items;
    try std.testing.expectEqual(@as(usize, 3), typed_usage_rows.len);
    try std.testing.expectEqualStrings("t1", typed_usage_rows[0].object.get("tenant_id").?.string);
    try std.testing.expectEqualStrings("active", typed_usage_rows[0].object.get("status").?.string);
    try std.testing.expectEqual(@as(i64, 35), typed_usage_rows[0].object.get("amount_sum").?.integer);
    try std.testing.expectEqual(@as(i64, 2), typed_usage_rows[0].object.get("row_count").?.integer);

    var sql_rbac_resp = try server.handlePublicSql("{\"sql\":\"SELECT resource_id, access_role FROM domain_records WHERE kind = 'membership' AND tenant_id = 't1' AND subject_id = 'user:1' AND status = 'active' ORDER BY resource_id ASC;\"}", null);
    defer sql_rbac_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), sql_rbac_resp.status);
    var parsed_sql_rbac = try std.json.parseFromSlice(std.json.Value, alloc, sql_rbac_resp.body, .{ .allocate = .alloc_always });
    defer parsed_sql_rbac.deinit();
    const sql_rbac_rows = parsed_sql_rbac.value.object.get("result").?.object.get("rows").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), sql_rbac_rows.len);
    try std.testing.expectEqualStrings("project:1", sql_rbac_rows[0].object.get("resource_id").?.string);
    try std.testing.expectEqualStrings("admin", sql_rbac_rows[0].object.get("access_role").?.string);

    var typed_rbac_resp = try server.handlePublicTableRowsQuery("domain_records", "{\"query\":{\"where\":{\"all\":[{\"field\":\"kind\",\"op\":\"eq\",\"value\":\"membership\"},{\"field\":\"tenant_id\",\"op\":\"eq\",\"value\":\"t1\"},{\"field\":\"subject_id\",\"op\":\"eq\",\"value\":\"user:1\"},{\"field\":\"status\",\"op\":\"eq\",\"value\":\"active\"}]},\"select\":[\"resource_id\",\"access_role\"],\"order_by\":[{\"field\":\"resource_id\"}]}}", null);
    defer typed_rbac_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), typed_rbac_resp.status);
    var parsed_typed_rbac = try std.json.parseFromSlice(std.json.Value, alloc, typed_rbac_resp.body, .{ .allocate = .alloc_always });
    defer parsed_typed_rbac.deinit();
    const typed_rbac_rows = parsed_typed_rbac.value.object.get("rows").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), typed_rbac_rows.len);
    try std.testing.expectEqualStrings("project:1", typed_rbac_rows[0].object.get("resource_id").?.string);
    try std.testing.expectEqualStrings("admin", typed_rbac_rows[0].object.get("access_role").?.string);

    var sql_wake_resp = try server.handlePublicSql("{\"sql\":\"SELECT id, priority FROM domain_records WHERE kind = 'wake_job' AND status = 'queued' ORDER BY priority DESC, created_at ASC LIMIT 1;\"}", null);
    defer sql_wake_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), sql_wake_resp.status);
    var parsed_sql_wake = try std.json.parseFromSlice(std.json.Value, alloc, sql_wake_resp.body, .{ .allocate = .alloc_always });
    defer parsed_sql_wake.deinit();
    const sql_wake_rows = parsed_sql_wake.value.object.get("result").?.object.get("rows").?.array.items;
    try std.testing.expectEqual(@as(usize, 1), sql_wake_rows.len);
    try std.testing.expectEqualStrings("w2", sql_wake_rows[0].object.get("id").?.string);
    try std.testing.expectEqual(@as(i64, 10), sql_wake_rows[0].object.get("priority").?.integer);

    var typed_wake_resp = try server.handlePublicTableRowsQuery("domain_records", "{\"query\":{\"where\":{\"all\":[{\"field\":\"kind\",\"op\":\"eq\",\"value\":\"wake_job\"},{\"field\":\"status\",\"op\":\"eq\",\"value\":\"queued\"}]},\"select\":[\"id\",\"priority\"],\"order_by\":[{\"field\":\"priority\",\"direction\":\"desc\"},{\"field\":\"created_at\",\"direction\":\"asc\"}],\"limit\":1}}", null);
    defer typed_wake_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), typed_wake_resp.status);
    var parsed_typed_wake = try std.json.parseFromSlice(std.json.Value, alloc, typed_wake_resp.body, .{ .allocate = .alloc_always });
    defer parsed_typed_wake.deinit();
    const typed_wake_rows = parsed_typed_wake.value.object.get("rows").?.array.items;
    try std.testing.expectEqual(@as(usize, 1), typed_wake_rows.len);
    try std.testing.expectEqualStrings("w2", typed_wake_rows[0].object.get("id").?.string);
    try std.testing.expectEqual(@as(i64, 10), typed_wake_rows[0].object.get("priority").?.integer);

    var sql_dashboard_resp = try server.handlePublicSql("{\"sql\":\"SELECT status, COUNT(*) AS row_count, SUM(amount) AS amount_sum FROM domain_records WHERE kind = 'usage' AND tenant_id = 't1' GROUP BY status ORDER BY status ASC;\"}", null);
    defer sql_dashboard_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), sql_dashboard_resp.status);
    var parsed_sql_dashboard = try std.json.parseFromSlice(std.json.Value, alloc, sql_dashboard_resp.body, .{ .allocate = .alloc_always });
    defer parsed_sql_dashboard.deinit();
    const sql_dashboard_rows = parsed_sql_dashboard.value.object.get("result").?.object.get("rows").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), sql_dashboard_rows.len);
    try std.testing.expectEqualStrings("active", sql_dashboard_rows[0].object.get("status").?.string);
    try std.testing.expectEqual(@as(i64, 2), sql_dashboard_rows[0].object.get("row_count").?.integer);
    try std.testing.expectEqual(@as(i64, 35), sql_dashboard_rows[0].object.get("amount_sum").?.integer);

    var typed_dashboard_resp = try server.handlePublicTableRowsAggregate("domain_records", "{\"aggregate\":{\"source\":{\"where\":{\"all\":[{\"field\":\"kind\",\"op\":\"eq\",\"value\":\"usage\"},{\"field\":\"tenant_id\",\"op\":\"eq\",\"value\":\"t1\"}]}},\"group_by\":[\"status\"],\"aggregations\":[{\"name\":\"row_count\",\"op\":\"count\"},{\"name\":\"amount_sum\",\"op\":\"sum\",\"field\":\"amount\"}],\"order_by\":[{\"field\":\"status\"}]}}", null);
    defer typed_dashboard_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), typed_dashboard_resp.status);
    var parsed_typed_dashboard = try std.json.parseFromSlice(std.json.Value, alloc, typed_dashboard_resp.body, .{ .allocate = .alloc_always });
    defer parsed_typed_dashboard.deinit();
    const typed_dashboard_rows = parsed_typed_dashboard.value.object.get("rows").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), typed_dashboard_rows.len);
    try std.testing.expectEqualStrings("active", typed_dashboard_rows[0].object.get("status").?.string);
    try std.testing.expectEqual(@as(i64, 2), typed_dashboard_rows[0].object.get("row_count").?.integer);
    try std.testing.expectEqual(@as(i64, 35), typed_dashboard_rows[0].object.get("amount_sum").?.integer);

    var sql_backfill_resp = try server.handlePublicSql("{\"sql\":\"SELECT COUNT(*) AS missing_count FROM domain_records WHERE kind = 'backfill' AND migrated = false;\"}", null);
    defer sql_backfill_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), sql_backfill_resp.status);
    var parsed_sql_backfill = try std.json.parseFromSlice(std.json.Value, alloc, sql_backfill_resp.body, .{ .allocate = .alloc_always });
    defer parsed_sql_backfill.deinit();
    const sql_backfill_rows = parsed_sql_backfill.value.object.get("result").?.object.get("rows").?.array.items;
    try std.testing.expectEqual(@as(usize, 1), sql_backfill_rows.len);
    try std.testing.expectEqual(@as(i64, 1), sql_backfill_rows[0].object.get("missing_count").?.integer);

    var typed_backfill_resp = try server.handlePublicTableRowsAggregate("domain_records", "{\"aggregate\":{\"source\":{\"where\":{\"all\":[{\"field\":\"kind\",\"op\":\"eq\",\"value\":\"backfill\"},{\"field\":\"migrated\",\"op\":\"eq\",\"value\":false}]}},\"aggregations\":[{\"name\":\"missing_count\",\"op\":\"count\"}]}}", null);
    defer typed_backfill_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), typed_backfill_resp.status);
    var parsed_typed_backfill = try std.json.parseFromSlice(std.json.Value, alloc, typed_backfill_resp.body, .{ .allocate = .alloc_always });
    defer parsed_typed_backfill.deinit();
    const typed_backfill_rows = parsed_typed_backfill.value.object.get("rows").?.array.items;
    try std.testing.expectEqual(@as(usize, 1), typed_backfill_rows.len);
    try std.testing.expectEqual(@as(i64, 1), typed_backfill_rows[0].object.get("missing_count").?.integer);
}

test "api public SQL endpoint executes document SQL reads through typed document plan ingress" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"title":{"type":"text"},"status":{"type":"keyword","x-antfly-cardinality-proof":"unique"},"amount":{"type":"numeric"},"note":{"type":"text"},"metadata":{"type":"json"},"tags":{"type":"array","items":{"type":"keyword"}}},"additionalProperties":true}}}}
    ;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/public-document-sql-read", .{tmp.sub_path});
    defer alloc.free(path);

    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();
    try db.applyTableSchemaJson(alloc, schema_json, .{});
    try db.addIndex(.{ .name = "full_text_index_v0", .kind = .full_text, .config_json = "{}" });
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"status\":\"active\",\"amount\":10,\"note\":null,\"metadata\":{\"plan\":\"pro\"},\"tags\":[\"urgent\",\"vip\"]}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"status\":\"archived\",\"amount\":20,\"metadata\":{\"plan\":\"free\"},\"tags\":[\"stale\"]}" },
        },
        .sync_level = .full_index,
    });

    const native_table_name = try catalog_resources.defaultPublicTableResourceNameAlloc(alloc, "docs");
    defer alloc.free(native_table_name);
    const DualNameReads = struct {
        public: table_reads.BoundTableReadSource,
        native: table_reads.BoundTableReadSource,

        fn source(self: *@This()) table_reads.TableReadSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                },
            };
        }

        fn route(self: *@This(), table_name: []const u8) table_reads.TableReadSource {
            if (std.mem.eql(u8, table_name, "docs")) return self.public.source();
            return self.native.source();
        }

        fn lookup(
            ptr: *anyopaque,
            inner_alloc: std.mem.Allocator,
            table_name: []const u8,
            key: []const u8,
            opts: db_mod.types.LookupOptions,
            consistency: raft_mod.ReadConsistency,
        ) !?table_reads.LookupResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try self.route(table_name).lookup(inner_alloc, table_name, key, opts, consistency);
        }

        fn scan(
            ptr: *anyopaque,
            inner_alloc: std.mem.Allocator,
            table_name: []const u8,
            from_key: []const u8,
            to_key: []const u8,
            opts: db_mod.types.ScanOptions,
            consistency: raft_mod.ReadConsistency,
        ) !?table_reads.ScanResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try self.route(table_name).scan(inner_alloc, table_name, from_key, to_key, opts, consistency);
        }

        fn query(
            ptr: *anyopaque,
            inner_alloc: std.mem.Allocator,
            table_name: []const u8,
            req: db_mod.types.SearchRequest,
            consistency: raft_mod.ReadConsistency,
        ) !?query_api.QueryResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (std.mem.eql(u8, table_name, "docs") and std.mem.indexOf(u8, req.filter_query_json, "duplicate-source") != null) {
                return .{ .json = try inner_alloc.dupe(u8,
                    \\{"responses":[{"hits":{"total":2,"hits":[{"_id":"doc:dup-target"},{"_id":"doc:dup-source"}]}}]}
                ) };
            }
            return try self.route(table_name).query(inner_alloc, table_name, req, consistency);
        }
    };
    var read_source = DualNameReads{
        .public = table_reads.BoundTableReadSource.init("docs", 1, &db, raft_mod.read_gate.noopReadableLeaseRequester()),
        .native = table_reads.BoundTableReadSource.init(native_table_name, 1, &db, raft_mod.read_gate.noopReadableLeaseRequester()),
    };
    var write_source = table_writes.BoundTableWriteSource.init("docs", &db);

    const FakeSource = struct {
        tables: [1]metadata_table_manager.TableRecord,

        fn iface(self: *@This()) StatusSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{
                .status = try status(ptr),
                .tables = self.tables[0..],
                .ranges = &.{},
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = FakeSource{ .tables = .{.{
        .table_id = 1,
        .name = "docs",
        .schema_json = schema_json,
        .desired_replica_count = 1,
    }} };
    var server = ApiHttpServer.init(alloc, .{}, source.iface(), read_source.source(), write_source.source());
    defer server.deinit();

    var query_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"SELECT _id, _doc, title, metadata->>'plan' AS plan FROM docs WHERE _id = 'doc:a';\"}",
    });
    defer query_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), query_resp.status);
    try std.testing.expectEqualStrings("application/json", query_resp.content_type.?);

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, query_resp.body, .{ .allocate = .alloc_always });
    defer parsed.deinit();
    try std.testing.expectEqualStrings("read", parsed.value.object.get("kind").?.string);
    try std.testing.expectEqualStrings("query", parsed.value.object.get("statement_kind").?.string);
    const result = parsed.value.object.get("result").?.object;
    try std.testing.expectEqual(@as(i64, 1), result.get("total").?.integer);
    const row = result.get("rows").?.array.items[0].object;
    try std.testing.expectEqualStrings("doc:a", row.get("_id").?.string);
    try std.testing.expectEqualStrings("alpha", row.get("title").?.string);
    try std.testing.expectEqualStrings("pro", row.get("plan").?.string);
    const doc = row.get("_doc").?.object;
    try std.testing.expectEqualStrings("active", doc.get("status").?.string);

    var alias_star_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"SELECT d.* FROM docs AS d WHERE d._id = 'doc:a';\"}",
    });
    defer alias_star_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), alias_star_resp.status);
    try std.testing.expectEqualStrings("application/json", alias_star_resp.content_type.?);

    var alias_star_parsed = try std.json.parseFromSlice(std.json.Value, alloc, alias_star_resp.body, .{ .allocate = .alloc_always });
    defer alias_star_parsed.deinit();
    try std.testing.expectEqualStrings("read", alias_star_parsed.value.object.get("kind").?.string);
    try std.testing.expectEqualStrings("query", alias_star_parsed.value.object.get("statement_kind").?.string);
    const alias_star_result = alias_star_parsed.value.object.get("result").?.object;
    try std.testing.expectEqual(@as(i64, 1), alias_star_result.get("total").?.integer);
    const alias_star_row = alias_star_result.get("rows").?.array.items[0].object;
    try std.testing.expectEqualStrings("doc:a", alias_star_row.get("_id").?.string);
    try std.testing.expectEqualStrings("alpha", alias_star_row.get("title").?.string);
    try std.testing.expectEqualStrings("active", alias_star_row.get("status").?.string);
    try std.testing.expectEqualStrings("pro", alias_star_row.get("metadata").?.object.get("plan").?.string);
    try std.testing.expectEqualStrings("active", alias_star_row.get("_doc").?.object.get("status").?.string);

    var alias_project_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"SELECT d._id, d.title, d.metadata->>'plan' AS plan FROM docs AS d WHERE d.status = 'active' LIMIT 10;\"}",
    });
    defer alias_project_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), alias_project_resp.status);
    try std.testing.expectEqualStrings("application/json", alias_project_resp.content_type.?);

    var alias_project_parsed = try std.json.parseFromSlice(std.json.Value, alloc, alias_project_resp.body, .{ .allocate = .alloc_always });
    defer alias_project_parsed.deinit();
    try std.testing.expectEqualStrings("read", alias_project_parsed.value.object.get("kind").?.string);
    try std.testing.expectEqualStrings("query", alias_project_parsed.value.object.get("statement_kind").?.string);
    const alias_project_result = alias_project_parsed.value.object.get("result").?.object;
    try std.testing.expectEqual(@as(i64, 1), alias_project_result.get("total").?.integer);
    const alias_project_row = alias_project_result.get("rows").?.array.items[0].object;
    try std.testing.expectEqualStrings("doc:a", alias_project_row.get("_id").?.string);
    try std.testing.expectEqualStrings("alpha", alias_project_row.get("title").?.string);
    try std.testing.expectEqualStrings("pro", alias_project_row.get("plan").?.string);

    var full_text_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"SELECT _id, title FROM docs WHERE full_text_search('title:alpha') LIMIT 5;\"}",
    });
    defer full_text_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), full_text_resp.status);
    try std.testing.expectEqualStrings("application/json", full_text_resp.content_type.?);

    var full_text_parsed = try std.json.parseFromSlice(std.json.Value, alloc, full_text_resp.body, .{ .allocate = .alloc_always });
    defer full_text_parsed.deinit();
    try std.testing.expectEqualStrings("read", full_text_parsed.value.object.get("kind").?.string);
    try std.testing.expectEqualStrings("query", full_text_parsed.value.object.get("statement_kind").?.string);
    const full_text_result = full_text_parsed.value.object.get("result").?.object;
    try std.testing.expectEqual(@as(i64, 1), full_text_result.get("total").?.integer);
    const full_text_row = full_text_result.get("rows").?.array.items[0].object;
    try std.testing.expectEqualStrings("doc:a", full_text_row.get("_id").?.string);
    try std.testing.expectEqualStrings("alpha", full_text_row.get("title").?.string);

    const native_query_body = try test_contract_helpers.encodeMatchQueryRequest(alloc, "title", "alpha", &.{ "title", "status" }, 5);
    defer alloc.free(native_query_body);
    var native_query_resp = try server.handle(.{
        .method = .POST,
        .uri = "/tables/docs/query",
        .content_type = "application/json",
        .body = native_query_body,
    });
    defer native_query_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), native_query_resp.status);
    try std.testing.expectEqualStrings("application/json", native_query_resp.content_type.?);
    var native_query_parsed = try std.json.parseFromSlice(metadata_openapi.QueryResponses, alloc, native_query_resp.body, .{});
    defer native_query_parsed.deinit();
    try std.testing.expectEqual(@as(usize, 1), native_query_parsed.value.responses.?.len);
    try std.testing.expectEqualStrings(full_text_row.get("_id").?.string, native_query_parsed.value.responses.?[0].hits.?.hits.?[0]._id);

    var full_text_residual_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"SELECT _id, status FROM docs WHERE full_text_search('title:alpha') AND status = 'active' LIMIT 5;\"}",
    });
    defer full_text_residual_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), full_text_residual_resp.status);
    try std.testing.expectEqualStrings("application/json", full_text_residual_resp.content_type.?);

    var full_text_residual_parsed = try std.json.parseFromSlice(std.json.Value, alloc, full_text_residual_resp.body, .{ .allocate = .alloc_always });
    defer full_text_residual_parsed.deinit();
    try std.testing.expectEqualStrings("read", full_text_residual_parsed.value.object.get("kind").?.string);
    try std.testing.expectEqualStrings("query", full_text_residual_parsed.value.object.get("statement_kind").?.string);
    const full_text_residual_result = full_text_residual_parsed.value.object.get("result").?.object;
    try std.testing.expectEqual(@as(i64, 1), full_text_residual_result.get("total").?.integer);
    const full_text_residual_row = full_text_residual_result.get("rows").?.array.items[0].object;
    try std.testing.expectEqualStrings("doc:a", full_text_residual_row.get("_id").?.string);
    try std.testing.expectEqualStrings("active", full_text_residual_row.get("status").?.string);

    var bounded_scalar_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"SELECT _id, status FROM docs WHERE status = 'active' LIMIT 10;\"}",
    });
    defer bounded_scalar_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), bounded_scalar_resp.status);
    try std.testing.expectEqualStrings("application/json", bounded_scalar_resp.content_type.?);

    var bounded_scalar_parsed = try std.json.parseFromSlice(std.json.Value, alloc, bounded_scalar_resp.body, .{ .allocate = .alloc_always });
    defer bounded_scalar_parsed.deinit();
    try std.testing.expectEqualStrings("read", bounded_scalar_parsed.value.object.get("kind").?.string);
    try std.testing.expectEqualStrings("query", bounded_scalar_parsed.value.object.get("statement_kind").?.string);
    const bounded_scalar_result = bounded_scalar_parsed.value.object.get("result").?.object;
    try std.testing.expectEqual(@as(i64, 1), bounded_scalar_result.get("total").?.integer);
    const bounded_scalar_row = bounded_scalar_result.get("rows").?.array.items[0].object;
    try std.testing.expectEqualStrings("doc:a", bounded_scalar_row.get("_id").?.string);
    try std.testing.expectEqualStrings("active", bounded_scalar_row.get("status").?.string);

    var scalar_ops_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"SELECT _id, amount FROM docs WHERE status IN ('active', 'pending') AND title LIKE 'alp%' AND amount BETWEEN 5 AND 15 LIMIT 10;\"}",
    });
    defer scalar_ops_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), scalar_ops_resp.status);
    try std.testing.expectEqualStrings("application/json", scalar_ops_resp.content_type.?);

    var scalar_ops_parsed = try std.json.parseFromSlice(std.json.Value, alloc, scalar_ops_resp.body, .{ .allocate = .alloc_always });
    defer scalar_ops_parsed.deinit();
    try std.testing.expectEqualStrings("read", scalar_ops_parsed.value.object.get("kind").?.string);
    try std.testing.expectEqualStrings("query", scalar_ops_parsed.value.object.get("statement_kind").?.string);
    const scalar_ops_result = scalar_ops_parsed.value.object.get("result").?.object;
    try std.testing.expectEqual(@as(i64, 1), scalar_ops_result.get("total").?.integer);
    const scalar_ops_row = scalar_ops_result.get("rows").?.array.items[0].object;
    try std.testing.expectEqualStrings("doc:a", scalar_ops_row.get("_id").?.string);
    try std.testing.expectEqual(@as(i64, 10), scalar_ops_row.get("amount").?.integer);

    var null_predicate_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"SELECT _id FROM docs WHERE note IS NULL ORDER BY _id ASC LIMIT 10;\"}",
    });
    defer null_predicate_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), null_predicate_resp.status);
    try std.testing.expectEqualStrings("application/json", null_predicate_resp.content_type.?);

    var null_predicate_parsed = try std.json.parseFromSlice(std.json.Value, alloc, null_predicate_resp.body, .{ .allocate = .alloc_always });
    defer null_predicate_parsed.deinit();
    try std.testing.expectEqualStrings("read", null_predicate_parsed.value.object.get("kind").?.string);
    try std.testing.expectEqualStrings("query", null_predicate_parsed.value.object.get("statement_kind").?.string);
    const null_predicate_rows = null_predicate_parsed.value.object.get("result").?.object.get("rows").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), null_predicate_rows.len);
    try std.testing.expectEqualStrings("doc:a", null_predicate_rows[0].object.get("_id").?.string);
    try std.testing.expectEqualStrings("doc:b", null_predicate_rows[1].object.get("_id").?.string);

    var not_null_predicate_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"SELECT _id FROM docs WHERE status IS NOT NULL ORDER BY _id ASC LIMIT 10;\"}",
    });
    defer not_null_predicate_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), not_null_predicate_resp.status);
    try std.testing.expectEqualStrings("application/json", not_null_predicate_resp.content_type.?);

    var not_null_predicate_parsed = try std.json.parseFromSlice(std.json.Value, alloc, not_null_predicate_resp.body, .{ .allocate = .alloc_always });
    defer not_null_predicate_parsed.deinit();
    try std.testing.expectEqualStrings("read", not_null_predicate_parsed.value.object.get("kind").?.string);
    try std.testing.expectEqualStrings("query", not_null_predicate_parsed.value.object.get("statement_kind").?.string);
    const not_null_predicate_rows = not_null_predicate_parsed.value.object.get("result").?.object.get("rows").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), not_null_predicate_rows.len);
    try std.testing.expectEqualStrings("doc:a", not_null_predicate_rows[0].object.get("_id").?.string);
    try std.testing.expectEqualStrings("doc:b", not_null_predicate_rows[1].object.get("_id").?.string);

    var json_path_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"SELECT _id, metadata->>'plan' AS plan FROM docs WHERE metadata->>'plan' = 'pro' LIMIT 10;\"}",
    });
    defer json_path_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), json_path_resp.status);
    try std.testing.expectEqualStrings("application/json", json_path_resp.content_type.?);

    var json_path_parsed = try std.json.parseFromSlice(std.json.Value, alloc, json_path_resp.body, .{ .allocate = .alloc_always });
    defer json_path_parsed.deinit();
    try std.testing.expectEqualStrings("read", json_path_parsed.value.object.get("kind").?.string);
    try std.testing.expectEqualStrings("query", json_path_parsed.value.object.get("statement_kind").?.string);
    const json_path_result = json_path_parsed.value.object.get("result").?.object;
    try std.testing.expectEqual(@as(i64, 1), json_path_result.get("total").?.integer);
    const json_path_row = json_path_result.get("rows").?.array.items[0].object;
    try std.testing.expectEqualStrings("doc:a", json_path_row.get("_id").?.string);
    try std.testing.expectEqualStrings("pro", json_path_row.get("plan").?.string);

    var unnest_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"SELECT d._id, tag FROM docs AS d, UNNEST(d.tags) AS tag WHERE tag = 'urgent' LIMIT 10;\"}",
    });
    defer unnest_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), unnest_resp.status);
    try std.testing.expectEqualStrings("application/json", unnest_resp.content_type.?);

    var unnest_parsed = try std.json.parseFromSlice(std.json.Value, alloc, unnest_resp.body, .{ .allocate = .alloc_always });
    defer unnest_parsed.deinit();
    try std.testing.expectEqualStrings("read", unnest_parsed.value.object.get("kind").?.string);
    try std.testing.expectEqualStrings("query", unnest_parsed.value.object.get("statement_kind").?.string);
    const unnest_result = unnest_parsed.value.object.get("result").?.object;
    try std.testing.expectEqual(@as(i64, 1), unnest_result.get("total").?.integer);
    const unnest_row = unnest_result.get("rows").?.array.items[0].object;
    try std.testing.expectEqualStrings("doc:a", unnest_row.get("_id").?.string);
    try std.testing.expectEqualStrings("urgent", unnest_row.get("tag").?.string);

    var unnest_in_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"SELECT d._id, tag FROM docs AS d, UNNEST(d.tags) AS tag WHERE tag IN ('urgent', 'vip') LIMIT 10;\"}",
    });
    defer unnest_in_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), unnest_in_resp.status);
    try std.testing.expectEqualStrings("application/json", unnest_in_resp.content_type.?);

    var unnest_in_parsed = try std.json.parseFromSlice(std.json.Value, alloc, unnest_in_resp.body, .{ .allocate = .alloc_always });
    defer unnest_in_parsed.deinit();
    try std.testing.expectEqualStrings("read", unnest_in_parsed.value.object.get("kind").?.string);
    try std.testing.expectEqualStrings("query", unnest_in_parsed.value.object.get("statement_kind").?.string);
    const unnest_in_result = unnest_in_parsed.value.object.get("result").?.object;
    try std.testing.expectEqual(@as(i64, 2), unnest_in_result.get("total").?.integer);
    const unnest_in_rows = unnest_in_result.get("rows").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), unnest_in_rows.len);
    try std.testing.expectEqualStrings("doc:a", unnest_in_rows[0].object.get("_id").?.string);
    try std.testing.expectEqualStrings("urgent", unnest_in_rows[0].object.get("tag").?.string);
    try std.testing.expectEqualStrings("doc:a", unnest_in_rows[1].object.get("_id").?.string);
    try std.testing.expectEqualStrings("vip", unnest_in_rows[1].object.get("tag").?.string);

    var indexed_unnest_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"SELECT d._id, tag FROM docs AS d, UNNEST(d.tags) AS tag WHERE full_text_search('title:alpha') AND tag = 'vip' LIMIT 10;\"}",
    });
    defer indexed_unnest_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), indexed_unnest_resp.status);
    try std.testing.expectEqualStrings("application/json", indexed_unnest_resp.content_type.?);

    var indexed_unnest_parsed = try std.json.parseFromSlice(std.json.Value, alloc, indexed_unnest_resp.body, .{ .allocate = .alloc_always });
    defer indexed_unnest_parsed.deinit();
    try std.testing.expectEqualStrings("read", indexed_unnest_parsed.value.object.get("kind").?.string);
    try std.testing.expectEqualStrings("query", indexed_unnest_parsed.value.object.get("statement_kind").?.string);
    const indexed_unnest_result = indexed_unnest_parsed.value.object.get("result").?.object;
    try std.testing.expectEqual(@as(i64, 1), indexed_unnest_result.get("total").?.integer);
    const indexed_unnest_row = indexed_unnest_result.get("rows").?.array.items[0].object;
    try std.testing.expectEqualStrings("doc:a", indexed_unnest_row.get("_id").?.string);
    try std.testing.expectEqualStrings("vip", indexed_unnest_row.get("tag").?.string);

    var ordered_indexed_unnest_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"SELECT d._id, tag FROM docs AS d, UNNEST(d.tags) AS tag WHERE full_text_search('title:alpha') ORDER BY tag ASC LIMIT 2;\"}",
    });
    defer ordered_indexed_unnest_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), ordered_indexed_unnest_resp.status);
    try std.testing.expectEqualStrings("application/json", ordered_indexed_unnest_resp.content_type.?);

    var ordered_indexed_unnest_parsed = try std.json.parseFromSlice(std.json.Value, alloc, ordered_indexed_unnest_resp.body, .{ .allocate = .alloc_always });
    defer ordered_indexed_unnest_parsed.deinit();
    const ordered_indexed_unnest_rows = ordered_indexed_unnest_parsed.value.object.get("result").?.object.get("rows").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), ordered_indexed_unnest_rows.len);
    try std.testing.expectEqualStrings("doc:a", ordered_indexed_unnest_rows[0].object.get("_id").?.string);
    try std.testing.expectEqualStrings("urgent", ordered_indexed_unnest_rows[0].object.get("tag").?.string);
    try std.testing.expectEqualStrings("doc:a", ordered_indexed_unnest_rows[1].object.get("_id").?.string);
    try std.testing.expectEqualStrings("vip", ordered_indexed_unnest_rows[1].object.get("tag").?.string);

    var ordered_unnest_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"SELECT d._id, tag FROM docs AS d, UNNEST(d.tags) AS tag ORDER BY tag ASC LIMIT 3;\"}",
    });
    defer ordered_unnest_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), ordered_unnest_resp.status);
    try std.testing.expectEqualStrings("application/json", ordered_unnest_resp.content_type.?);

    var ordered_unnest_parsed = try std.json.parseFromSlice(std.json.Value, alloc, ordered_unnest_resp.body, .{ .allocate = .alloc_always });
    defer ordered_unnest_parsed.deinit();
    try std.testing.expectEqualStrings("read", ordered_unnest_parsed.value.object.get("kind").?.string);
    try std.testing.expectEqualStrings("query", ordered_unnest_parsed.value.object.get("statement_kind").?.string);
    const ordered_unnest_rows = ordered_unnest_parsed.value.object.get("result").?.object.get("rows").?.array.items;
    try std.testing.expectEqual(@as(usize, 3), ordered_unnest_rows.len);
    try std.testing.expectEqualStrings("doc:b", ordered_unnest_rows[0].object.get("_id").?.string);
    try std.testing.expectEqualStrings("stale", ordered_unnest_rows[0].object.get("tag").?.string);
    try std.testing.expectEqualStrings("doc:a", ordered_unnest_rows[1].object.get("_id").?.string);
    try std.testing.expectEqualStrings("urgent", ordered_unnest_rows[1].object.get("tag").?.string);
    try std.testing.expectEqualStrings("doc:a", ordered_unnest_rows[2].object.get("_id").?.string);
    try std.testing.expectEqualStrings("vip", ordered_unnest_rows[2].object.get("tag").?.string);

    var ordered_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"SELECT _id, title FROM docs ORDER BY title DESC LIMIT 2;\"}",
    });
    defer ordered_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), ordered_resp.status);
    try std.testing.expectEqualStrings("application/json", ordered_resp.content_type.?);

    var ordered_parsed = try std.json.parseFromSlice(std.json.Value, alloc, ordered_resp.body, .{ .allocate = .alloc_always });
    defer ordered_parsed.deinit();
    try std.testing.expectEqualStrings("read", ordered_parsed.value.object.get("kind").?.string);
    try std.testing.expectEqualStrings("query", ordered_parsed.value.object.get("statement_kind").?.string);
    const ordered_result = ordered_parsed.value.object.get("result").?.object;
    try std.testing.expectEqual(@as(i64, 2), ordered_result.get("total").?.integer);
    const ordered_rows = ordered_result.get("rows").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), ordered_rows.len);
    try std.testing.expectEqualStrings("doc:b", ordered_rows[0].object.get("_id").?.string);
    try std.testing.expectEqualStrings("beta", ordered_rows[0].object.get("title").?.string);
    try std.testing.expectEqualStrings("doc:a", ordered_rows[1].object.get("_id").?.string);
    try std.testing.expectEqualStrings("alpha", ordered_rows[1].object.get("title").?.string);

    var count_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"SELECT count(*) AS row_count FROM docs;\"}",
    });
    defer count_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), count_resp.status);
    try std.testing.expectEqualStrings("application/json", count_resp.content_type.?);

    var count_parsed = try std.json.parseFromSlice(std.json.Value, alloc, count_resp.body, .{ .allocate = .alloc_always });
    defer count_parsed.deinit();
    try std.testing.expectEqualStrings("read", count_parsed.value.object.get("kind").?.string);
    try std.testing.expectEqualStrings("aggregate", count_parsed.value.object.get("statement_kind").?.string);
    const count_result = count_parsed.value.object.get("result").?.object;
    try std.testing.expectEqual(@as(i64, 1), count_result.get("total_groups").?.integer);
    const count_row = count_result.get("rows").?.array.items[0].object;
    try std.testing.expectEqual(@as(i64, 2), count_row.get("row_count").?.integer);

    var grouped_count_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"SELECT count(*) AS row_count FROM docs GROUP BY status LIMIT 10;\"}",
    });
    defer grouped_count_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), grouped_count_resp.status);
    try std.testing.expectEqualStrings("application/json", grouped_count_resp.content_type.?);

    var grouped_count_parsed = try std.json.parseFromSlice(std.json.Value, alloc, grouped_count_resp.body, .{ .allocate = .alloc_always });
    defer grouped_count_parsed.deinit();
    try std.testing.expectEqualStrings("read", grouped_count_parsed.value.object.get("kind").?.string);
    try std.testing.expectEqualStrings("aggregate", grouped_count_parsed.value.object.get("statement_kind").?.string);
    const grouped_count_result = grouped_count_parsed.value.object.get("result").?.object;
    try std.testing.expectEqual(@as(i64, 2), grouped_count_result.get("total_groups").?.integer);
    const grouped_count_rows = grouped_count_result.get("rows").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), grouped_count_rows.len);
    try std.testing.expectEqualStrings("active", grouped_count_rows[0].object.get("status").?.string);
    try std.testing.expectEqual(@as(i64, 1), grouped_count_rows[0].object.get("row_count").?.integer);
    try std.testing.expectEqualStrings("archived", grouped_count_rows[1].object.get("status").?.string);
    try std.testing.expectEqual(@as(i64, 1), grouped_count_rows[1].object.get("row_count").?.integer);

    var row_filters = try alloc.alloc(usermgr.RowFilterEntry, 1);
    row_filters[0] = try usermgr.RowFilterEntry.initOwned(alloc, "docs", "{\"term\":{\"status\":\"active\"}}");
    var identity = AuthenticatedIdentity{
        .username = try alloc.dupe(u8, "document_sql_reader"),
        .row_filter = row_filters,
    };
    defer identity.deinit(alloc);

    var filtered_lookup_resp = try server.handlePublicSql("{\"sql\":\"SELECT _id, status FROM docs WHERE _id IN ('doc:a', 'doc:b') LIMIT 10;\"}", identity);
    defer filtered_lookup_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), filtered_lookup_resp.status);
    var filtered_lookup_parsed = try std.json.parseFromSlice(std.json.Value, alloc, filtered_lookup_resp.body, .{ .allocate = .alloc_always });
    defer filtered_lookup_parsed.deinit();
    const filtered_lookup_result = filtered_lookup_parsed.value.object.get("result").?.object;
    try std.testing.expectEqual(@as(i64, 1), filtered_lookup_result.get("total").?.integer);
    const filtered_lookup_row = filtered_lookup_result.get("rows").?.array.items[0].object;
    try std.testing.expectEqualStrings("doc:a", filtered_lookup_row.get("_id").?.string);
    try std.testing.expectEqualStrings("active", filtered_lookup_row.get("status").?.string);

    var filtered_ordered_resp = try server.handlePublicSql("{\"sql\":\"SELECT _id, title FROM docs ORDER BY title DESC LIMIT 10;\"}", identity);
    defer filtered_ordered_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), filtered_ordered_resp.status);
    var filtered_ordered_parsed = try std.json.parseFromSlice(std.json.Value, alloc, filtered_ordered_resp.body, .{ .allocate = .alloc_always });
    defer filtered_ordered_parsed.deinit();
    const filtered_ordered_result = filtered_ordered_parsed.value.object.get("result").?.object;
    try std.testing.expectEqual(@as(i64, 1), filtered_ordered_result.get("total").?.integer);
    const filtered_ordered_row = filtered_ordered_result.get("rows").?.array.items[0].object;
    try std.testing.expectEqualStrings("doc:a", filtered_ordered_row.get("_id").?.string);
    try std.testing.expectEqualStrings("alpha", filtered_ordered_row.get("title").?.string);

    var filtered_full_text_resp = try server.handlePublicSql("{\"sql\":\"SELECT _id, title FROM docs WHERE full_text_search('title:beta') LIMIT 10;\"}", identity);
    defer filtered_full_text_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), filtered_full_text_resp.status);
    var filtered_full_text_parsed = try std.json.parseFromSlice(std.json.Value, alloc, filtered_full_text_resp.body, .{ .allocate = .alloc_always });
    defer filtered_full_text_parsed.deinit();
    const filtered_full_text_result = filtered_full_text_parsed.value.object.get("result").?.object;
    try std.testing.expectEqual(@as(i64, 0), filtered_full_text_result.get("total").?.integer);
    try std.testing.expectEqual(@as(usize, 0), filtered_full_text_result.get("rows").?.array.items.len);

    var filtered_count_resp = try server.handlePublicSql("{\"sql\":\"SELECT count(*) AS row_count FROM docs;\"}", identity);
    defer filtered_count_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), filtered_count_resp.status);
    var filtered_count_parsed = try std.json.parseFromSlice(std.json.Value, alloc, filtered_count_resp.body, .{ .allocate = .alloc_always });
    defer filtered_count_parsed.deinit();
    const filtered_count_result = filtered_count_parsed.value.object.get("result").?.object;
    try std.testing.expectEqual(@as(i64, 1), filtered_count_result.get("total_groups").?.integer);
    const filtered_count_row = filtered_count_result.get("rows").?.array.items[0].object;
    try std.testing.expectEqual(@as(i64, 1), filtered_count_row.get("row_count").?.integer);

    var filtered_unnest_resp = try server.handlePublicSql("{\"sql\":\"SELECT d._id, tag FROM docs AS d, UNNEST(d.tags) AS tag WHERE tag = 'stale' LIMIT 10;\"}", identity);
    defer filtered_unnest_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), filtered_unnest_resp.status);
    var filtered_unnest_parsed = try std.json.parseFromSlice(std.json.Value, alloc, filtered_unnest_resp.body, .{ .allocate = .alloc_always });
    defer filtered_unnest_parsed.deinit();
    const filtered_unnest_result = filtered_unnest_parsed.value.object.get("result").?.object;
    try std.testing.expectEqual(@as(i64, 0), filtered_unnest_result.get("total").?.integer);
    try std.testing.expectEqual(@as(usize, 0), filtered_unnest_result.get("rows").?.array.items.len);

    var bounded_scan_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"SELECT _id FROM docs;\"}",
    });
    defer bounded_scan_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 400), bounded_scan_resp.status);
    try expectPublicSqlDiagnosticBody(alloc, bounded_scan_resp.body, "plan", "invalid_sql_request", "document_sql_requires_bounded_scan", 0, 0);

    var array_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"SELECT _id FROM docs WHERE tags = 'urgent' LIMIT 10;\"}",
    });
    defer array_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 400), array_resp.status);
    try expectPublicSqlDiagnosticBody(alloc, array_resp.body, "plan", "invalid_sql_request", "document_sql_array_requires_unnest", 0, 0);

    var unsupported_unnest_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"SELECT d._id, tag, label FROM docs AS d, UNNEST(d.tags) AS tag, UNNEST(d.labels) AS label LIMIT 10;\"}",
    });
    defer unsupported_unnest_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 400), unsupported_unnest_resp.status);
    try expectPublicSqlDiagnosticBody(alloc, unsupported_unnest_resp.body, "plan", "invalid_sql_request", "document_sql_unnest_unsupported", 0, 0);

    var join_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"SELECT d._id FROM docs AS d JOIN docs AS e ON d._id = e._id LIMIT 10;\"}",
    });
    defer join_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 400), join_resp.status);
    try expectPublicSqlDiagnosticBody(alloc, join_resp.body, "plan", "invalid_sql_request", "document_sql_unsupported_join", 0, 0);

    var distinct_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"SELECT DISTINCT _id FROM docs WHERE _id = 'doc:a';\"}",
    });
    defer distinct_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 400), distinct_resp.status);
    try expectPublicSqlDiagnosticBody(alloc, distinct_resp.body, "plan", "invalid_sql_request", "document_sql_projection_modifier_unsupported", 0, 0);

    var offset_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"SELECT _id FROM docs WHERE status = 'active' OFFSET 1;\"}",
    });
    defer offset_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 400), offset_resp.status);
    try expectPublicSqlDiagnosticBody(alloc, offset_resp.body, "plan", "invalid_sql_request", "document_sql_pagination_unsupported", 0, 0);

    var locking_tail_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"SELECT _id FROM docs WHERE _id = 'doc:a' FOR UPDATE;\"}",
    });
    defer locking_tail_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 400), locking_tail_resp.status);
    try expectPublicSqlDiagnosticBody(alloc, locking_tail_resp.body, "plan", "invalid_sql_request", "document_sql_locking_unsupported", 0, 0);

    var window_tail_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"SELECT _id FROM docs WINDOW w AS () LIMIT 10;\"}",
    });
    defer window_tail_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 400), window_tail_resp.status);
    try expectPublicSqlDiagnosticBody(alloc, window_tail_resp.body, "plan", "invalid_sql_request", "document_sql_window_unsupported", 0, 0);

    var aggregate_unsupported_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"SELECT count(*) AS row_count FROM docs HAVING count(*) > 0;\"}",
    });
    defer aggregate_unsupported_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 400), aggregate_unsupported_resp.status);
    try expectPublicSqlDiagnosticBody(alloc, aggregate_unsupported_resp.body, "plan", "invalid_sql_request", "document_sql_aggregate_unsupported", 0, 0);

    var aggregate_filter_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"SELECT count(*) FILTER (WHERE status = 'active') AS row_count FROM docs GROUP BY status LIMIT 10;\"}",
    });
    defer aggregate_filter_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 400), aggregate_filter_resp.status);
    try expectPublicSqlDiagnosticBody(alloc, aggregate_filter_resp.body, "plan", "invalid_sql_request", "document_sql_aggregate_unsupported", 0, 0);

    var native_search_predicate_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"SELECT _id FROM docs WHERE antfly.hybrid_search(table_name => 'docs', query => 'alpha', limit => 10) LIMIT 10;\"}",
    });
    defer native_search_predicate_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 400), native_search_predicate_resp.status);
    try expectPublicSqlDiagnosticBody(alloc, native_search_predicate_resp.body, "plan", "invalid_sql_request", "document_sql_native_search_requires_table_function", 0, 0);

    var view_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"CREATE VIEW docs_view(doc_id, title) AS SELECT _id, title FROM docs;\"}",
    });
    defer view_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 400), view_resp.status);
    try expectPublicSqlDiagnosticBody(alloc, view_resp.body, "plan", "document_sql_view_mapping_unsupported", "document_sql_view_mapping_unsupported", 0, 67);
}

test "api public SQL endpoint executes SQL point writes through typed row batch ingress" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"amount":{"type":"numeric"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    const child_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"parent_id":{"type":"keyword"},"status":{"type":"keyword"},"amount":{"type":"numeric"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"foreign_keys":[{"name":"usage_children_parent_fkey","columns":["parent_id"],"references":{"table":"usage_records","columns":["id"]},"on_delete":"restrict"}]}
    ;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/public-sql-write", .{tmp.sub_path});
    defer alloc.free(path);

    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();
    try db.applyTableSchemaJson(alloc, schema_json, .{});

    var read_source = table_reads.BoundTableReadSource.init("usage_records", 1, &db, raft_mod.read_gate.noopReadableLeaseRequester());
    var write_source = table_writes.BoundTableWriteSource.init("usage_records", &db);

    const FakeSource = struct {
        tables: [4]metadata_table_manager.TableRecord,
        ranges: [4]metadata_table_manager.RangeRecord,
        table_emptying_jobs: std.ArrayListUnmanaged(metadata_table_manager.TableEmptyingJobRecord) = .empty,

        fn deinit(self: *@This(), allocator: std.mem.Allocator) void {
            for (self.table_emptying_jobs.items) |record| metadata_table_manager.freeTableEmptyingJob(allocator, record);
            self.table_emptying_jobs.deinit(allocator);
        }

        fn iface(self: *@This()) StatusSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                    .upsert_table_emptying_job = upsertTableEmptyingJob,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{
                .status = try status(ptr),
                .tables = self.tables[0..],
                .ranges = self.ranges[0..],
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}

        fn upsertTableEmptyingJob(ptr: *anyopaque, record: metadata_table_manager.TableEmptyingJobRecord) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const owned = try metadata_table_manager.cloneTableEmptyingJob(std.testing.allocator, record);
            errdefer metadata_table_manager.freeTableEmptyingJob(std.testing.allocator, owned);
            for (self.table_emptying_jobs.items, 0..) |existing, i| {
                if (existing.job_id != owned.job_id) continue;
                metadata_table_manager.freeTableEmptyingJob(std.testing.allocator, self.table_emptying_jobs.items[i]);
                self.table_emptying_jobs.items[i] = owned;
                return;
            }
            try self.table_emptying_jobs.append(std.testing.allocator, owned);
        }
    };

    var source = FakeSource{
        .tables = .{
            .{
                .table_id = 1,
                .name = "usage_records",
                .schema_json = schema_json,
                .desired_replica_count = 1,
            },
            .{
                .table_id = 2,
                .name = "usage_archive",
                .schema_json = schema_json,
                .desired_replica_count = 1,
            },
            .{
                .table_id = 3,
                .name = "usage_children",
                .schema_json = child_schema_json,
                .desired_replica_count = 1,
            },
            .{
                .table_id = 4,
                .name = "usage_records",
                .namespace_name = "tenant",
                .schema_json = schema_json,
                .desired_replica_count = 1,
            },
        },
        .ranges = .{
            .{ .group_id = 11, .range_id = 101, .table_id = 1, .start_key = "", .end_key = null },
            .{ .group_id = 12, .range_id = 102, .table_id = 2, .start_key = "", .end_key = null },
            .{ .group_id = 13, .range_id = 103, .table_id = 3, .start_key = "", .end_key = null },
            .{ .group_id = 14, .range_id = 104, .table_id = 4, .start_key = "", .end_key = null },
        },
    };
    defer source.deinit(alloc);
    var server = ApiHttpServer.init(alloc, .{}, source.iface(), read_source.source(), write_source.source());
    defer server.deinit();

    var insert_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"INSERT INTO usage_records (id, status, amount) VALUES ('u1', 'open', 10) RETURNING id, status;\"}",
    });
    defer insert_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), insert_resp.status);
    var parsed_insert = try std.json.parseFromSlice(std.json.Value, alloc, insert_resp.body, .{ .allocate = .alloc_always });
    defer parsed_insert.deinit();
    try std.testing.expectEqualStrings("write", parsed_insert.value.object.get("kind").?.string);
    try std.testing.expectEqualStrings("insert", parsed_insert.value.object.get("statement_kind").?.string);
    const insert_result = parsed_insert.value.object.get("result").?.object;
    try std.testing.expectEqual(@as(i64, 1), insert_result.get("inserted").?.integer);
    try std.testing.expectEqualStrings("u1", insert_result.get("returning").?.array.items[0].object.get("id").?.string);

    var update_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"UPDATE usage_records SET status = 'closed', amount = amount + 5 WHERE id = 'u1' RETURNING id, status, amount;\"}",
    });
    defer update_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), update_resp.status);
    var parsed_update = try std.json.parseFromSlice(std.json.Value, alloc, update_resp.body, .{ .allocate = .alloc_always });
    defer parsed_update.deinit();
    try std.testing.expectEqualStrings("write", parsed_update.value.object.get("kind").?.string);
    try std.testing.expectEqualStrings("update", parsed_update.value.object.get("statement_kind").?.string);
    const update_result = parsed_update.value.object.get("result").?.object;
    try std.testing.expectEqual(@as(i64, 1), update_result.get("transformed").?.integer);
    const update_row = update_result.get("returning").?.array.items[0].object;
    try std.testing.expectEqualStrings("u1", update_row.get("id").?.string);
    try std.testing.expectEqualStrings("closed", update_row.get("status").?.string);
    try std.testing.expectEqual(@as(i64, 15), update_row.get("amount").?.integer);

    var insert_source_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"INSERT INTO usage_records (id, status, amount) SELECT concat(id, '-copy') AS id, lower(status) AS status, amount + 1 AS amount FROM usage_records WHERE id = 'u1' RETURNING id, status, amount;\"}",
    });
    defer insert_source_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), insert_source_resp.status);
    var parsed_insert_source = try std.json.parseFromSlice(std.json.Value, alloc, insert_source_resp.body, .{ .allocate = .alloc_always });
    defer parsed_insert_source.deinit();
    try std.testing.expectEqualStrings("write", parsed_insert_source.value.object.get("kind").?.string);
    try std.testing.expectEqualStrings("insert_source", parsed_insert_source.value.object.get("statement_kind").?.string);
    const insert_source_result = parsed_insert_source.value.object.get("result").?.object;
    try std.testing.expectEqual(@as(i64, 1), insert_source_result.get("matched").?.integer);
    try std.testing.expectEqual(@as(i64, 1), insert_source_result.get("staged").?.integer);
    const insert_source_row = insert_source_result.get("returning").?.array.items[0].object;
    try std.testing.expectEqualStrings("u1-copy", insert_source_row.get("id").?.string);
    try std.testing.expectEqualStrings("closed", insert_source_row.get("status").?.string);
    try std.testing.expectEqual(@as(i64, 16), insert_source_row.get("amount").?.integer);

    var delete_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"DELETE FROM usage_records WHERE id = 'u1' RETURNING id, status;\"}",
    });
    defer delete_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), delete_resp.status);
    var parsed_delete = try std.json.parseFromSlice(std.json.Value, alloc, delete_resp.body, .{ .allocate = .alloc_always });
    defer parsed_delete.deinit();
    try std.testing.expectEqualStrings("write", parsed_delete.value.object.get("kind").?.string);
    try std.testing.expectEqualStrings("delete", parsed_delete.value.object.get("statement_kind").?.string);
    const delete_result = parsed_delete.value.object.get("result").?.object;
    try std.testing.expectEqual(@as(i64, 1), delete_result.get("deleted").?.integer);
    const delete_row = delete_result.get("returning").?.array.items[0].object;
    try std.testing.expectEqualStrings("u1", delete_row.get("id").?.string);
    try std.testing.expectEqualStrings("closed", delete_row.get("status").?.string);

    var sql_returning_insert_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"INSERT INTO usage_records (id, status, amount) VALUES ('sql-returning-insert', 'ready', 21) RETURNING id, status, amount;\"}",
    });
    defer sql_returning_insert_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), sql_returning_insert_resp.status);
    var parsed_sql_returning_insert = try std.json.parseFromSlice(std.json.Value, alloc, sql_returning_insert_resp.body, .{ .allocate = .alloc_always });
    defer parsed_sql_returning_insert.deinit();
    const sql_returning_insert_row = parsed_sql_returning_insert.value.object.get("result").?.object.get("returning").?.array.items[0].object;
    try std.testing.expectEqualStrings("sql-returning-insert", sql_returning_insert_row.get("id").?.string);
    try std.testing.expectEqualStrings("ready", sql_returning_insert_row.get("status").?.string);
    try std.testing.expectEqual(@as(i64, 21), sql_returning_insert_row.get("amount").?.integer);

    var typed_returning_insert_resp = try server.handle(.{
        .method = .POST,
        .uri = "/tables/usage_records/rows/batch",
        .content_type = "application/json",
        .body = "{\"operations\":[{\"op\":\"insert\",\"row\":{\"id\":\"typed-returning-insert\",\"status\":\"ready\",\"amount\":21},\"returning\":[\"id\",\"status\",\"amount\"]}]}",
    });
    defer typed_returning_insert_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 201), typed_returning_insert_resp.status);
    var parsed_typed_returning_insert = try std.json.parseFromSlice(std.json.Value, alloc, typed_returning_insert_resp.body, .{ .allocate = .alloc_always });
    defer parsed_typed_returning_insert.deinit();
    const typed_returning_insert_row = parsed_typed_returning_insert.value.object.get("returning").?.array.items[0].object;
    try std.testing.expectEqualStrings("typed-returning-insert", typed_returning_insert_row.get("id").?.string);
    try std.testing.expectEqualStrings(sql_returning_insert_row.get("status").?.string, typed_returning_insert_row.get("status").?.string);
    try std.testing.expectEqual(sql_returning_insert_row.get("amount").?.integer, typed_returning_insert_row.get("amount").?.integer);

    var sql_conflict_seed_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"INSERT INTO usage_records (id, status, amount) VALUES ('sql-returning-conflict', 'old', 1), ('typed-returning-conflict', 'old', 1), ('sql-returning-noop', 'old', 2), ('typed-returning-noop', 'old', 2);\"}",
    });
    defer sql_conflict_seed_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), sql_conflict_seed_resp.status);

    var sql_conflict_update_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"INSERT INTO usage_records (id, status, amount) VALUES ('sql-returning-conflict', 'new', 5) ON CONFLICT (id) DO UPDATE SET status = excluded.status, amount = excluded.amount RETURNING id, status, amount;\"}",
    });
    defer sql_conflict_update_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), sql_conflict_update_resp.status);
    var parsed_sql_conflict_update = try std.json.parseFromSlice(std.json.Value, alloc, sql_conflict_update_resp.body, .{ .allocate = .alloc_always });
    defer parsed_sql_conflict_update.deinit();
    const sql_conflict_update_row = parsed_sql_conflict_update.value.object.get("result").?.object.get("returning").?.array.items[0].object;
    try std.testing.expectEqualStrings("sql-returning-conflict", sql_conflict_update_row.get("id").?.string);
    try std.testing.expectEqualStrings("new", sql_conflict_update_row.get("status").?.string);
    try std.testing.expectEqual(@as(i64, 5), sql_conflict_update_row.get("amount").?.integer);

    var typed_conflict_update_resp = try server.handle(.{
        .method = .POST,
        .uri = "/tables/usage_records/rows/batch",
        .content_type = "application/json",
        .body = "{\"operations\":[{\"op\":\"insert\",\"row\":{\"id\":\"typed-returning-conflict\",\"status\":\"new\",\"amount\":5},\"on_conflict\":{\"target\":{\"primary\":true},\"action\":\"update\",\"patch\":{\"status\":\"new\",\"amount\":5}},\"returning\":[\"id\",\"status\",\"amount\"]}]}",
    });
    defer typed_conflict_update_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 201), typed_conflict_update_resp.status);
    var parsed_typed_conflict_update = try std.json.parseFromSlice(std.json.Value, alloc, typed_conflict_update_resp.body, .{ .allocate = .alloc_always });
    defer parsed_typed_conflict_update.deinit();
    const typed_conflict_update_row = parsed_typed_conflict_update.value.object.get("returning").?.array.items[0].object;
    try std.testing.expectEqualStrings("typed-returning-conflict", typed_conflict_update_row.get("id").?.string);
    try std.testing.expectEqualStrings(sql_conflict_update_row.get("status").?.string, typed_conflict_update_row.get("status").?.string);
    try std.testing.expectEqual(sql_conflict_update_row.get("amount").?.integer, typed_conflict_update_row.get("amount").?.integer);

    var sql_conflict_noop_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"INSERT INTO usage_records (id, status, amount) VALUES ('sql-returning-noop', 'ignored', 9) ON CONFLICT (id) DO NOTHING RETURNING id, status, amount;\"}",
    });
    defer sql_conflict_noop_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), sql_conflict_noop_resp.status);
    var parsed_sql_conflict_noop = try std.json.parseFromSlice(std.json.Value, alloc, sql_conflict_noop_resp.body, .{ .allocate = .alloc_always });
    defer parsed_sql_conflict_noop.deinit();
    const sql_conflict_noop_result = parsed_sql_conflict_noop.value.object.get("result").?.object;
    try std.testing.expect(sql_conflict_noop_result.get("returning") == null or sql_conflict_noop_result.get("returning").?.array.items.len == 0);

    var typed_conflict_noop_resp = try server.handle(.{
        .method = .POST,
        .uri = "/tables/usage_records/rows/batch",
        .content_type = "application/json",
        .body = "{\"operations\":[{\"op\":\"insert\",\"row\":{\"id\":\"typed-returning-noop\",\"status\":\"ignored\",\"amount\":9},\"on_conflict\":{\"target\":{\"primary\":true},\"action\":\"nothing\"},\"returning\":[\"id\",\"status\",\"amount\"]}]}",
    });
    defer typed_conflict_noop_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 201), typed_conflict_noop_resp.status);
    var parsed_typed_conflict_noop = try std.json.parseFromSlice(std.json.Value, alloc, typed_conflict_noop_resp.body, .{ .allocate = .alloc_always });
    defer parsed_typed_conflict_noop.deinit();
    try std.testing.expect(parsed_typed_conflict_noop.value.object.get("returning") == null or parsed_typed_conflict_noop.value.object.get("returning").?.array.items.len == 0);

    var sql_claim_seed_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"INSERT INTO usage_records (id, status, amount) VALUES ('sql-returning-claim', 'queued', 3), ('typed-returning-claim', 'queued', 3), ('sql-returning-delete', 'done', 4), ('typed-returning-delete', 'done', 4);\"}",
    });
    defer sql_claim_seed_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), sql_claim_seed_resp.status);

    var sql_claim_update_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"UPDATE usage_records SET status = 'claimed', amount = amount + 1 WHERE id = 'sql-returning-claim' FOR UPDATE RETURNING id, status, amount;\"}",
    });
    defer sql_claim_update_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), sql_claim_update_resp.status);
    var parsed_sql_claim_update = try std.json.parseFromSlice(std.json.Value, alloc, sql_claim_update_resp.body, .{ .allocate = .alloc_always });
    defer parsed_sql_claim_update.deinit();
    const sql_claim_update_row = parsed_sql_claim_update.value.object.get("result").?.object.get("returning").?.array.items[0].object;
    try std.testing.expectEqualStrings("sql-returning-claim", sql_claim_update_row.get("id").?.string);
    try std.testing.expectEqualStrings("claimed", sql_claim_update_row.get("status").?.string);
    try std.testing.expectEqual(@as(i64, 4), sql_claim_update_row.get("amount").?.integer);

    const typed_claim_update_txn_hex = "11112233445566778899aabbccddeeff";
    const typed_claim_update_txn_id = try distributed_txn.parseTxnIdHex(typed_claim_update_txn_hex);
    _ = try db.beginTransactionWithId(typed_claim_update_txn_id, 1_100);
    var typed_claim_update_resp = try server.handle(.{
        .method = .POST,
        .uri = "/tables/usage_records/rows/mutation-source",
        .content_type = "application/json",
        .body = "{\"op\":\"update\",\"source\":{\"where\":{\"field\":\"id\",\"op\":\"eq\",\"value\":\"typed-returning-claim\"},\"row_claim\":{\"mode\":\"for_update\",\"owner_id\":\"typed:returning-update\",\"transaction_id\":\"11112233445566778899aabbccddeeff\"}},\"patch\":{\"status\":\"claimed\"},\"increment\":{\"amount\":1},\"returning\":[\"id\",\"status\",\"amount\"]}",
    });
    defer typed_claim_update_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), typed_claim_update_resp.status);
    var parsed_typed_claim_update = try std.json.parseFromSlice(std.json.Value, alloc, typed_claim_update_resp.body, .{ .allocate = .alloc_always });
    defer parsed_typed_claim_update.deinit();
    const typed_claim_update_row = parsed_typed_claim_update.value.object.get("returning").?.array.items[0].object;
    try std.testing.expectEqualStrings("typed-returning-claim", typed_claim_update_row.get("id").?.string);
    try std.testing.expectEqualStrings(sql_claim_update_row.get("status").?.string, typed_claim_update_row.get("status").?.string);
    try std.testing.expectEqual(sql_claim_update_row.get("amount").?.integer, typed_claim_update_row.get("amount").?.integer);
    try db.commitTransaction(typed_claim_update_txn_id, 1_101);

    var sql_claim_delete_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"DELETE FROM usage_records WHERE id = 'sql-returning-delete' FOR UPDATE RETURNING id, status, amount;\"}",
    });
    defer sql_claim_delete_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), sql_claim_delete_resp.status);
    var parsed_sql_claim_delete = try std.json.parseFromSlice(std.json.Value, alloc, sql_claim_delete_resp.body, .{ .allocate = .alloc_always });
    defer parsed_sql_claim_delete.deinit();
    const sql_claim_delete_row = parsed_sql_claim_delete.value.object.get("result").?.object.get("returning").?.array.items[0].object;
    try std.testing.expectEqualStrings("sql-returning-delete", sql_claim_delete_row.get("id").?.string);
    try std.testing.expectEqualStrings("done", sql_claim_delete_row.get("status").?.string);
    try std.testing.expectEqual(@as(i64, 4), sql_claim_delete_row.get("amount").?.integer);

    const typed_claim_delete_txn_hex = "22112233445566778899aabbccddeeff";
    const typed_claim_delete_txn_id = try distributed_txn.parseTxnIdHex(typed_claim_delete_txn_hex);
    _ = try db.beginTransactionWithId(typed_claim_delete_txn_id, 1_200);
    var typed_claim_delete_resp = try server.handle(.{
        .method = .POST,
        .uri = "/tables/usage_records/rows/mutation-source",
        .content_type = "application/json",
        .body = "{\"op\":\"delete\",\"source\":{\"where\":{\"field\":\"id\",\"op\":\"eq\",\"value\":\"typed-returning-delete\"},\"row_claim\":{\"mode\":\"for_update\",\"owner_id\":\"typed:returning-delete\",\"transaction_id\":\"22112233445566778899aabbccddeeff\"}},\"returning\":[\"id\",\"status\",\"amount\"]}",
    });
    defer typed_claim_delete_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), typed_claim_delete_resp.status);
    var parsed_typed_claim_delete = try std.json.parseFromSlice(std.json.Value, alloc, typed_claim_delete_resp.body, .{ .allocate = .alloc_always });
    defer parsed_typed_claim_delete.deinit();
    const typed_claim_delete_row = parsed_typed_claim_delete.value.object.get("returning").?.array.items[0].object;
    try std.testing.expectEqualStrings("typed-returning-delete", typed_claim_delete_row.get("id").?.string);
    try std.testing.expectEqualStrings(sql_claim_delete_row.get("status").?.string, typed_claim_delete_row.get("status").?.string);
    try std.testing.expectEqual(sql_claim_delete_row.get("amount").?.integer, typed_claim_delete_row.get("amount").?.integer);
    try db.commitTransaction(typed_claim_delete_txn_id, 1_201);

    var query_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"SELECT id FROM usage_records WHERE id = 'u1';\"}",
    });
    defer query_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), query_resp.status);
    var parsed_query = try std.json.parseFromSlice(std.json.Value, alloc, query_resp.body, .{ .allocate = .alloc_always });
    defer parsed_query.deinit();
    try std.testing.expectEqual(@as(i64, 0), parsed_query.value.object.get("result").?.object.get("total").?.integer);

    var copied_query_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"SELECT id, status, amount FROM usage_records WHERE id = 'u1-copy';\"}",
    });
    defer copied_query_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), copied_query_resp.status);
    var parsed_copied_query = try std.json.parseFromSlice(std.json.Value, alloc, copied_query_resp.body, .{ .allocate = .alloc_always });
    defer parsed_copied_query.deinit();
    const copied_result = parsed_copied_query.value.object.get("result").?.object;
    try std.testing.expectEqual(@as(i64, 1), copied_result.get("total").?.integer);
    const copied_row = copied_result.get("rows").?.array.items[0].object;
    try std.testing.expectEqualStrings("u1-copy", copied_row.get("id").?.string);
    try std.testing.expectEqualStrings("closed", copied_row.get("status").?.string);
    try std.testing.expectEqual(@as(i64, 16), copied_row.get("amount").?.integer);

    var update_source_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"UPDATE usage_records SET status = 'processed', amount = amount + 2 WHERE status = 'closed' ORDER BY id ASC LIMIT 1 FOR UPDATE SKIP LOCKED RETURNING id, status, amount;\"}",
    });
    defer update_source_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), update_source_resp.status);
    var parsed_update_source = try std.json.parseFromSlice(std.json.Value, alloc, update_source_resp.body, .{ .allocate = .alloc_always });
    defer parsed_update_source.deinit();
    try std.testing.expectEqualStrings("write", parsed_update_source.value.object.get("kind").?.string);
    try std.testing.expectEqualStrings("update_source", parsed_update_source.value.object.get("statement_kind").?.string);
    const update_source_result = parsed_update_source.value.object.get("result").?.object;
    try std.testing.expectEqual(@as(i64, 1), update_source_result.get("matched").?.integer);
    try std.testing.expectEqual(@as(i64, 1), update_source_result.get("staged").?.integer);
    const update_source_row = update_source_result.get("returning").?.array.items[0].object;
    try std.testing.expectEqualStrings("u1-copy", update_source_row.get("id").?.string);
    try std.testing.expectEqualStrings("processed", update_source_row.get("status").?.string);
    try std.testing.expectEqual(@as(i64, 18), update_source_row.get("amount").?.integer);

    var delete_source_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"DELETE FROM usage_records WHERE status = 'processed' FOR UPDATE RETURNING id, status;\"}",
    });
    defer delete_source_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), delete_source_resp.status);
    var parsed_delete_source = try std.json.parseFromSlice(std.json.Value, alloc, delete_source_resp.body, .{ .allocate = .alloc_always });
    defer parsed_delete_source.deinit();
    try std.testing.expectEqualStrings("write", parsed_delete_source.value.object.get("kind").?.string);
    try std.testing.expectEqualStrings("delete_source", parsed_delete_source.value.object.get("statement_kind").?.string);
    const delete_source_result = parsed_delete_source.value.object.get("result").?.object;
    try std.testing.expectEqual(@as(i64, 1), delete_source_result.get("matched").?.integer);
    try std.testing.expectEqual(@as(i64, 1), delete_source_result.get("staged").?.integer);
    const delete_source_row = delete_source_result.get("returning").?.array.items[0].object;
    try std.testing.expectEqualStrings("u1-copy", delete_source_row.get("id").?.string);
    try std.testing.expectEqualStrings("processed", delete_source_row.get("status").?.string);

    var deleted_source_query_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"SELECT id FROM usage_records WHERE id = 'u1-copy';\"}",
    });
    defer deleted_source_query_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), deleted_source_query_resp.status);
    var parsed_deleted_source_query = try std.json.parseFromSlice(std.json.Value, alloc, deleted_source_query_resp.body, .{ .allocate = .alloc_always });
    defer parsed_deleted_source_query.deinit();
    try std.testing.expectEqual(@as(i64, 0), parsed_deleted_source_query.value.object.get("result").?.object.get("total").?.integer);

    var joined_seed_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"INSERT INTO usage_records (id, status, amount) VALUES ('joined-target', 'join-key', 1), ('joined-source', 'join-key', 33);\"}",
    });
    defer joined_seed_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), joined_seed_resp.status);

    var update_joined_source_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"UPDATE usage_records AS target SET status = 'joined', amount = source.amount FROM usage_records AS source WHERE target.status = source.status AND target.id = 'joined-target' AND source.id = 'joined-source' FOR UPDATE OF target RETURNING target.id, target.status, target.amount;\"}",
    });
    defer update_joined_source_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), update_joined_source_resp.status);
    var parsed_update_joined_source = try std.json.parseFromSlice(std.json.Value, alloc, update_joined_source_resp.body, .{ .allocate = .alloc_always });
    defer parsed_update_joined_source.deinit();
    try std.testing.expectEqualStrings("write", parsed_update_joined_source.value.object.get("kind").?.string);
    try std.testing.expectEqualStrings("update_joined_source", parsed_update_joined_source.value.object.get("statement_kind").?.string);
    const update_joined_source_result = parsed_update_joined_source.value.object.get("result").?.object;
    try std.testing.expectEqual(@as(i64, 1), update_joined_source_result.get("matched").?.integer);
    try std.testing.expectEqual(@as(i64, 1), update_joined_source_result.get("staged").?.integer);
    const update_joined_source_row = update_joined_source_result.get("returning").?.array.items[0].object;
    try std.testing.expectEqualStrings("joined-target", update_joined_source_row.get("id").?.string);
    try std.testing.expectEqualStrings("joined", update_joined_source_row.get("status").?.string);
    try std.testing.expectEqual(@as(i64, 33), update_joined_source_row.get("amount").?.integer);

    var delete_joined_source_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"DELETE FROM usage_records AS target USING usage_records AS source WHERE target.amount = source.amount AND target.id = 'joined-target' AND source.id = 'joined-source' FOR UPDATE OF target RETURNING target.id, target.status;\"}",
    });
    defer delete_joined_source_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), delete_joined_source_resp.status);
    var parsed_delete_joined_source = try std.json.parseFromSlice(std.json.Value, alloc, delete_joined_source_resp.body, .{ .allocate = .alloc_always });
    defer parsed_delete_joined_source.deinit();
    try std.testing.expectEqualStrings("write", parsed_delete_joined_source.value.object.get("kind").?.string);
    try std.testing.expectEqualStrings("delete_joined_source", parsed_delete_joined_source.value.object.get("statement_kind").?.string);
    const delete_joined_source_result = parsed_delete_joined_source.value.object.get("result").?.object;
    try std.testing.expectEqual(@as(i64, 1), delete_joined_source_result.get("matched").?.integer);
    try std.testing.expectEqual(@as(i64, 1), delete_joined_source_result.get("staged").?.integer);
    const delete_joined_source_row = delete_joined_source_result.get("returning").?.array.items[0].object;
    try std.testing.expectEqualStrings("joined-target", delete_joined_source_row.get("id").?.string);
    try std.testing.expectEqualStrings("joined", delete_joined_source_row.get("status").?.string);

    var deleted_joined_source_query_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"SELECT id FROM usage_records WHERE id = 'joined-target';\"}",
    });
    defer deleted_joined_source_query_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), deleted_joined_source_query_resp.status);
    var parsed_deleted_joined_source_query = try std.json.parseFromSlice(std.json.Value, alloc, deleted_joined_source_query_resp.body, .{ .allocate = .alloc_always });
    defer parsed_deleted_joined_source_query.deinit();
    try std.testing.expectEqual(@as(i64, 0), parsed_deleted_joined_source_query.value.object.get("result").?.object.get("total").?.integer);

    var recursive_seed_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"INSERT INTO usage_records (id, status, amount) VALUES ('recursive-a', 'root', 1), ('recursive-b', 'recursive-a', 2), ('recursive-c', 'other', 3);\"}",
    });
    defer recursive_seed_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), recursive_seed_resp.status);

    var recursive_update_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"WITH RECURSIVE source_rows AS (SELECT id FROM usage_records WHERE status = 'root' UNION ALL SELECT child.id FROM usage_records AS child JOIN source_rows AS parent ON child.status = parent.id) UPDATE usage_records SET status = 'done' WHERE id IN (SELECT id FROM source_rows) RETURNING id, status;\"}",
    });
    defer recursive_update_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), recursive_update_resp.status);
    var parsed_recursive_update = try std.json.parseFromSlice(std.json.Value, alloc, recursive_update_resp.body, .{ .allocate = .alloc_always });
    defer parsed_recursive_update.deinit();
    try std.testing.expectEqualStrings("write", parsed_recursive_update.value.object.get("kind").?.string);
    try std.testing.expectEqualStrings("update_joined_source", parsed_recursive_update.value.object.get("statement_kind").?.string);
    const recursive_update_result = parsed_recursive_update.value.object.get("result").?.object;
    try std.testing.expectEqual(@as(i64, 2), recursive_update_result.get("matched").?.integer);
    try std.testing.expectEqual(@as(i64, 2), recursive_update_result.get("staged").?.integer);
    const recursive_returning = recursive_update_result.get("returning").?.array;
    try std.testing.expectEqual(@as(usize, 2), recursive_returning.items.len);

    var recursive_query_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"SELECT id, status FROM usage_records WHERE id IN ('recursive-a', 'recursive-b', 'recursive-c') ORDER BY id ASC;\"}",
    });
    defer recursive_query_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), recursive_query_resp.status);
    var parsed_recursive_query = try std.json.parseFromSlice(std.json.Value, alloc, recursive_query_resp.body, .{ .allocate = .alloc_always });
    defer parsed_recursive_query.deinit();
    const recursive_rows = parsed_recursive_query.value.object.get("result").?.object.get("rows").?.array.items;
    try std.testing.expectEqual(@as(usize, 3), recursive_rows.len);
    try std.testing.expectEqualStrings("recursive-a", recursive_rows[0].object.get("id").?.string);
    try std.testing.expectEqualStrings("done", recursive_rows[0].object.get("status").?.string);
    try std.testing.expectEqualStrings("recursive-b", recursive_rows[1].object.get("id").?.string);
    try std.testing.expectEqualStrings("done", recursive_rows[1].object.get("status").?.string);
    try std.testing.expectEqualStrings("recursive-c", recursive_rows[2].object.get("id").?.string);
    try std.testing.expectEqualStrings("other", recursive_rows[2].object.get("status").?.string);

    var merge_seed_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"INSERT INTO usage_records (id, status, amount) VALUES ('merge-target', 'old', 1), ('merge-source-update', 'merge-target', 44), ('merge-source-insert', 'merge-inserted', 55);\"}",
    });
    defer merge_seed_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), merge_seed_resp.status);

    var merge_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"MERGE INTO usage_records AS target USING usage_records AS source ON target.id = source.status WHEN MATCHED AND source.id = 'merge-source-update' THEN UPDATE SET status = 'merged', amount = source.amount WHEN NOT MATCHED AND source.id = 'merge-source-insert' THEN INSERT (id, status, amount) VALUES (source.status, 'inserted', source.amount) RETURNING target.id, target.status, target.amount;\"}",
    });
    defer merge_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), merge_resp.status);
    var parsed_merge = try std.json.parseFromSlice(std.json.Value, alloc, merge_resp.body, .{ .allocate = .alloc_always });
    defer parsed_merge.deinit();
    try std.testing.expectEqualStrings("write", parsed_merge.value.object.get("kind").?.string);
    try std.testing.expectEqualStrings("merge", parsed_merge.value.object.get("statement_kind").?.string);
    const merge_result = parsed_merge.value.object.get("result").?.object;
    try std.testing.expectEqual(@as(i64, 1), merge_result.get("inserted").?.integer);
    try std.testing.expectEqual(@as(i64, 1), merge_result.get("transformed").?.integer);
    try std.testing.expectEqual(@as(usize, 2), merge_result.get("returning").?.array.items.len);

    var merge_query_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"SELECT id, status, amount FROM usage_records WHERE id IN ('merge-inserted', 'merge-target') ORDER BY id ASC;\"}",
    });
    defer merge_query_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), merge_query_resp.status);
    var parsed_merge_query = try std.json.parseFromSlice(std.json.Value, alloc, merge_query_resp.body, .{ .allocate = .alloc_always });
    defer parsed_merge_query.deinit();
    const merge_rows = parsed_merge_query.value.object.get("result").?.object.get("rows").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), merge_rows.len);
    try std.testing.expectEqualStrings("merge-inserted", merge_rows[0].object.get("id").?.string);
    try std.testing.expectEqualStrings("inserted", merge_rows[0].object.get("status").?.string);
    try std.testing.expectEqual(@as(i64, 55), merge_rows[0].object.get("amount").?.integer);
    try std.testing.expectEqualStrings("merge-target", merge_rows[1].object.get("id").?.string);
    try std.testing.expectEqualStrings("merged", merge_rows[1].object.get("status").?.string);
    try std.testing.expectEqual(@as(i64, 44), merge_rows[1].object.get("amount").?.integer);

    var cte_merge_seed_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"INSERT INTO usage_records (id, status, amount) VALUES ('cte-merge-target', 'old', 1), ('cte-merge-source-update', 'cte-merge-target', 64), ('cte-merge-source-insert', 'cte-merge-inserted', 65);\"}",
    });
    defer cte_merge_seed_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), cte_merge_seed_resp.status);

    var cte_merge_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"WITH source_rows AS (SELECT id, status, amount FROM usage_records WHERE id IN ('cte-merge-source-update', 'cte-merge-source-insert')) MERGE INTO usage_records AS target USING source_rows AS source ON target.id = source.status WHEN MATCHED THEN UPDATE SET status = 'cte-merged', amount = source.amount WHEN NOT MATCHED AND source.id = 'cte-merge-source-insert' THEN INSERT (id, status, amount) VALUES (source.status, 'cte-inserted', source.amount) RETURNING target.id, target.status, target.amount;\"}",
    });
    defer cte_merge_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), cte_merge_resp.status);
    var parsed_cte_merge = try std.json.parseFromSlice(std.json.Value, alloc, cte_merge_resp.body, .{ .allocate = .alloc_always });
    defer parsed_cte_merge.deinit();
    try std.testing.expectEqualStrings("write", parsed_cte_merge.value.object.get("kind").?.string);
    try std.testing.expectEqualStrings("merge", parsed_cte_merge.value.object.get("statement_kind").?.string);
    const cte_merge_result = parsed_cte_merge.value.object.get("result").?.object;
    try std.testing.expectEqual(@as(i64, 1), cte_merge_result.get("inserted").?.integer);
    try std.testing.expectEqual(@as(i64, 1), cte_merge_result.get("transformed").?.integer);
    try std.testing.expectEqual(@as(usize, 2), cte_merge_result.get("returning").?.array.items.len);

    var cte_merge_query_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"SELECT id, status, amount FROM usage_records WHERE id IN ('cte-merge-inserted', 'cte-merge-target') ORDER BY id ASC;\"}",
    });
    defer cte_merge_query_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), cte_merge_query_resp.status);
    var parsed_cte_merge_query = try std.json.parseFromSlice(std.json.Value, alloc, cte_merge_query_resp.body, .{ .allocate = .alloc_always });
    defer parsed_cte_merge_query.deinit();
    const cte_merge_rows = parsed_cte_merge_query.value.object.get("result").?.object.get("rows").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), cte_merge_rows.len);
    try std.testing.expectEqualStrings("cte-merge-inserted", cte_merge_rows[0].object.get("id").?.string);
    try std.testing.expectEqualStrings("cte-inserted", cte_merge_rows[0].object.get("status").?.string);
    try std.testing.expectEqual(@as(i64, 65), cte_merge_rows[0].object.get("amount").?.integer);
    try std.testing.expectEqualStrings("cte-merge-target", cte_merge_rows[1].object.get("id").?.string);
    try std.testing.expectEqualStrings("cte-merged", cte_merge_rows[1].object.get("status").?.string);
    try std.testing.expectEqual(@as(i64, 64), cte_merge_rows[1].object.get("amount").?.integer);

    var recursive_merge_seed_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"INSERT INTO usage_records (id, status, amount) VALUES ('recursive-merge-a', 'merge-root', 7), ('recursive-merge-b', 'recursive-merge-a', 8), ('recursive-merge-c', 'merge-other', 9);\"}",
    });
    defer recursive_merge_seed_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), recursive_merge_seed_resp.status);

    var recursive_merge_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"WITH RECURSIVE source_rows AS (SELECT id, status FROM usage_records WHERE status = 'merge-root' UNION ALL SELECT child.id, child.status FROM usage_records AS child JOIN source_rows AS parent ON child.status = parent.id) MERGE INTO usage_records AS target USING source_rows AS source ON target.id = source.id WHEN MATCHED THEN UPDATE SET status = lower(source.status) RETURNING target.id, target.status;\"}",
    });
    defer recursive_merge_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), recursive_merge_resp.status);
    var parsed_recursive_merge = try std.json.parseFromSlice(std.json.Value, alloc, recursive_merge_resp.body, .{ .allocate = .alloc_always });
    defer parsed_recursive_merge.deinit();
    try std.testing.expectEqualStrings("write", parsed_recursive_merge.value.object.get("kind").?.string);
    try std.testing.expectEqualStrings("merge", parsed_recursive_merge.value.object.get("statement_kind").?.string);
    const recursive_merge_result = parsed_recursive_merge.value.object.get("result").?.object;
    try std.testing.expectEqual(@as(i64, 2), recursive_merge_result.get("transformed").?.integer);
    try std.testing.expectEqual(@as(usize, 2), recursive_merge_result.get("returning").?.array.items.len);

    var recursive_merge_query_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"SELECT id, status FROM usage_records WHERE id IN ('recursive-merge-a', 'recursive-merge-b', 'recursive-merge-c') ORDER BY id ASC;\"}",
    });
    defer recursive_merge_query_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), recursive_merge_query_resp.status);
    var parsed_recursive_merge_query = try std.json.parseFromSlice(std.json.Value, alloc, recursive_merge_query_resp.body, .{ .allocate = .alloc_always });
    defer parsed_recursive_merge_query.deinit();
    const recursive_merge_rows = parsed_recursive_merge_query.value.object.get("result").?.object.get("rows").?.array.items;
    try std.testing.expectEqual(@as(usize, 3), recursive_merge_rows.len);
    try std.testing.expectEqualStrings("recursive-merge-a", recursive_merge_rows[0].object.get("id").?.string);
    try std.testing.expectEqualStrings("merge-root", recursive_merge_rows[0].object.get("status").?.string);
    try std.testing.expectEqualStrings("recursive-merge-b", recursive_merge_rows[1].object.get("id").?.string);
    try std.testing.expectEqualStrings("recursive-merge-a", recursive_merge_rows[1].object.get("status").?.string);
    try std.testing.expectEqualStrings("recursive-merge-c", recursive_merge_rows[2].object.get("id").?.string);
    try std.testing.expectEqualStrings("merge-other", recursive_merge_rows[2].object.get("status").?.string);

    var truncate_seed_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"INSERT INTO usage_records (id, status, amount) VALUES ('truncate-a', 'drop', 1), ('truncate-b', 'drop', 2);\"}",
    });
    defer truncate_seed_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), truncate_seed_resp.status);

    var truncate_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"TRUNCATE usage_records;\"}",
    });
    defer truncate_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), truncate_resp.status);
    var parsed_truncate = try std.json.parseFromSlice(std.json.Value, alloc, truncate_resp.body, .{ .allocate = .alloc_always });
    defer parsed_truncate.deinit();
    try std.testing.expectEqualStrings("ddl", parsed_truncate.value.object.get("kind").?.string);
    try std.testing.expectEqualStrings("truncate", parsed_truncate.value.object.get("statement_kind").?.string);
    try std.testing.expectEqual(@as(usize, 1), source.table_emptying_jobs.items.len);
    try std.testing.expectEqual(@as(u64, 1), source.table_emptying_jobs.items[0].table_id);
    try std.testing.expectEqual(@as(u64, 11), source.table_emptying_jobs.items[0].group_id);
    try std.testing.expect(!source.table_emptying_jobs.items[0].restart_identity);
    try std.testing.expect(!source.table_emptying_jobs.items[0].cascade);
    try std.testing.expectEqual(@as(usize, 1), source.table_emptying_jobs.items[0].affected_table_ids.len);
    try std.testing.expectEqual(@as(u64, 1), source.table_emptying_jobs.items[0].affected_table_ids[0]);

    var truncate_continue_seed_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"INSERT INTO usage_records (id, status, amount) VALUES ('truncate-continue-a', 'drop', 1), ('truncate-continue-b', 'drop', 2);\"}",
    });
    defer truncate_continue_seed_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), truncate_continue_seed_resp.status);

    var truncate_continue_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"TRUNCATE TABLE ONLY public.usage_records CONTINUE IDENTITY RESTRICT;\"}",
    });
    defer truncate_continue_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), truncate_continue_resp.status);
    var parsed_truncate_continue = try std.json.parseFromSlice(std.json.Value, alloc, truncate_continue_resp.body, .{ .allocate = .alloc_always });
    defer parsed_truncate_continue.deinit();
    try std.testing.expectEqualStrings("ddl", parsed_truncate_continue.value.object.get("kind").?.string);
    try std.testing.expectEqualStrings("truncate", parsed_truncate_continue.value.object.get("statement_kind").?.string);
    try std.testing.expectEqual(@as(usize, 1), source.table_emptying_jobs.items.len);
    try std.testing.expectEqual(@as(u64, 1), source.table_emptying_jobs.items[0].table_id);
    try std.testing.expect(!source.table_emptying_jobs.items[0].restart_identity);
    try std.testing.expect(!source.table_emptying_jobs.items[0].cascade);
    try std.testing.expectEqual(@as(usize, 1), source.table_emptying_jobs.items[0].affected_table_ids.len);
    try std.testing.expectEqual(@as(u64, 1), source.table_emptying_jobs.items[0].affected_table_ids[0]);

    var truncate_cascade_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"TRUNCATE usage_records CASCADE;\"}",
    });
    defer truncate_cascade_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), truncate_cascade_resp.status);
    var parsed_truncate_cascade = try std.json.parseFromSlice(std.json.Value, alloc, truncate_cascade_resp.body, .{ .allocate = .alloc_always });
    defer parsed_truncate_cascade.deinit();
    try std.testing.expectEqualStrings("ddl", parsed_truncate_cascade.value.object.get("kind").?.string);
    try std.testing.expectEqualStrings("truncate", parsed_truncate_cascade.value.object.get("statement_kind").?.string);
    try std.testing.expectEqual(@as(usize, 3), source.table_emptying_jobs.items.len);
    try std.testing.expectEqual(@as(u64, 1), source.table_emptying_jobs.items[1].table_id);
    try std.testing.expectEqual(@as(u64, 11), source.table_emptying_jobs.items[1].group_id);
    try std.testing.expectEqual(@as(u64, 3), source.table_emptying_jobs.items[2].table_id);
    try std.testing.expectEqual(@as(u64, 13), source.table_emptying_jobs.items[2].group_id);
    for (source.table_emptying_jobs.items[1..3]) |job| {
        try std.testing.expect(!job.restart_identity);
        try std.testing.expect(job.cascade);
        try std.testing.expectEqual(@as(usize, 2), job.affected_table_ids.len);
        try std.testing.expectEqual(@as(u64, 1), job.affected_table_ids[0]);
        try std.testing.expectEqual(@as(u64, 3), job.affected_table_ids[1]);
    }

    var truncate_restart_identity_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"TRUNCATE usage_records RESTART IDENTITY;\"}",
    });
    defer truncate_restart_identity_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 501), truncate_restart_identity_resp.status);
    try std.testing.expectEqual(@as(usize, 3), source.table_emptying_jobs.items.len);

    var truncate_multi_table_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"TRUNCATE usage_records, usage_archive;\"}",
    });
    defer truncate_multi_table_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), truncate_multi_table_resp.status);
    var parsed_truncate_multi_table = try std.json.parseFromSlice(std.json.Value, alloc, truncate_multi_table_resp.body, .{ .allocate = .alloc_always });
    defer parsed_truncate_multi_table.deinit();
    try std.testing.expectEqualStrings("ddl", parsed_truncate_multi_table.value.object.get("kind").?.string);
    try std.testing.expectEqualStrings("truncate", parsed_truncate_multi_table.value.object.get("statement_kind").?.string);
    try std.testing.expectEqual(@as(usize, 5), source.table_emptying_jobs.items.len);
    try std.testing.expectEqual(@as(u64, 1), source.table_emptying_jobs.items[3].table_id);
    try std.testing.expectEqual(@as(u64, 11), source.table_emptying_jobs.items[3].group_id);
    try std.testing.expectEqual(@as(u64, 2), source.table_emptying_jobs.items[4].table_id);
    try std.testing.expectEqual(@as(u64, 12), source.table_emptying_jobs.items[4].group_id);
    for (source.table_emptying_jobs.items[3..5]) |job| {
        try std.testing.expect(!job.restart_identity);
        try std.testing.expect(!job.cascade);
        try std.testing.expectEqual(@as(usize, 2), job.affected_table_ids.len);
        try std.testing.expectEqual(@as(u64, 1), job.affected_table_ids[0]);
        try std.testing.expectEqual(@as(u64, 2), job.affected_table_ids[1]);
    }

    var tenant_truncate_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"namespace\":\"tenant\",\"sql\":\"TRUNCATE usage_records RESTART IDENTITY;\"}",
    });
    defer tenant_truncate_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 501), tenant_truncate_resp.status);
    try std.testing.expectEqual(@as(usize, 5), source.table_emptying_jobs.items.len);
}

test "api public SQL endpoint applies SQL row triggers to public SQL writes" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/public-sql-write-triggers", .{tmp.sub_path});
    defer alloc.free(path);

    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();
    try db.applyTableSchemaJson(alloc, schema_json, .{});

    var read_source = table_reads.BoundTableReadSource.init("events", 1, &db, raft_mod.read_gate.noopReadableLeaseRequester());
    var write_source = table_writes.BoundTableWriteSource.init("events", &db);

    const FakeSource = struct {
        tables: [1]metadata_table_manager.TableRecord,

        fn iface(self: *@This()) StatusSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{
                .status = try status(ptr),
                .tables = self.tables[0..],
                .ranges = &.{},
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = FakeSource{ .tables = .{.{
        .table_id = 1,
        .name = "events",
        .schema_json = schema_json,
        .desired_replica_count = 1,
    }} };
    var server = ApiHttpServer.init(alloc, .{}, source.iface(), read_source.source(), write_source.source());
    defer server.deinit();
    var session = try sql_adapter.OwnedSqlCatalogSession.fromSessionAlloc(alloc, catalog_resources.SqlCatalogSession.default());
    defer session.deinit(alloc);

    var baseline_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"INSERT INTO events (id, status) VALUES ('evt-existing', 'open') RETURNING id;\"}",
    });
    defer baseline_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), baseline_resp.status);

    var created_insert_function = try server.applyRelationalSqlDdlWithSession(
        "CREATE FUNCTION skip_event_insert() RETURNS trigger LANGUAGE plpgsql AS 'BEGIN RETURN NULL; END';",
        &session,
    );
    defer created_insert_function.deinit(alloc);
    var created_insert_trigger = try server.applyRelationalSqlDdlWithSession(
        "CREATE TRIGGER skip_event_insert BEFORE INSERT ON events FOR EACH ROW EXECUTE FUNCTION skip_event_insert();",
        &session,
    );
    defer created_insert_trigger.deinit(alloc);

    var skipped_insert_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"INSERT INTO events (id, status) VALUES ('evt-skipped', 'new') RETURNING id;\"}",
    });
    defer skipped_insert_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), skipped_insert_resp.status);
    var parsed_skipped_insert = try std.json.parseFromSlice(std.json.Value, alloc, skipped_insert_resp.body, .{ .allocate = .alloc_always });
    defer parsed_skipped_insert.deinit();
    const skipped_result = parsed_skipped_insert.value.object.get("result").?.object;
    try std.testing.expectEqual(@as(i64, 0), skipped_result.get("inserted").?.integer);
    if (skipped_result.get("returning")) |returning| {
        try std.testing.expectEqual(@as(usize, 0), returning.array.items.len);
    }

    var created_update_function = try server.applyRelationalSqlDdlWithSession(
        "CREATE FUNCTION keep_event_update_old() RETURNS trigger LANGUAGE plpgsql AS 'BEGIN RETURN OLD; END';",
        &session,
    );
    defer created_update_function.deinit(alloc);
    var created_update_trigger = try server.applyRelationalSqlDdlWithSession(
        "CREATE TRIGGER keep_event_update_old BEFORE UPDATE ON events FOR EACH ROW EXECUTE FUNCTION keep_event_update_old();",
        &session,
    );
    defer created_update_trigger.deinit(alloc);

    var update_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"UPDATE events SET status = 'closed' WHERE id = 'evt-existing' RETURNING id, status;\"}",
    });
    defer update_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 501), update_resp.status);

    var created_delete_function = try server.applyRelationalSqlDdlWithSession(
        "CREATE FUNCTION skip_event_delete() RETURNS trigger LANGUAGE plpgsql AS 'BEGIN RETURN NULL; END';",
        &session,
    );
    defer created_delete_function.deinit(alloc);
    var created_delete_trigger = try server.applyRelationalSqlDdlWithSession(
        "CREATE TRIGGER skip_event_delete BEFORE DELETE ON events FOR EACH ROW EXECUTE FUNCTION skip_event_delete();",
        &session,
    );
    defer created_delete_trigger.deinit(alloc);

    var delete_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"DELETE FROM events WHERE id = 'evt-existing' RETURNING id;\"}",
    });
    defer delete_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 501), delete_resp.status);

    var query_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body = "{\"sql\":\"SELECT id, status FROM events ORDER BY id ASC;\"}",
    });
    defer query_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), query_resp.status);
    var parsed_query = try std.json.parseFromSlice(std.json.Value, alloc, query_resp.body, .{ .allocate = .alloc_always });
    defer parsed_query.deinit();
    const result = parsed_query.value.object.get("result").?.object;
    try std.testing.expectEqual(@as(i64, 1), result.get("total").?.integer);
    const row = result.get("rows").?.array.items[0].object;
    try std.testing.expectEqualStrings("evt-existing", row.get("id").?.string);
    try std.testing.expectEqualStrings("open", row.get("status").?.string);
}

test "api public SQL endpoint executes public SQL COPY FROM STDIN payload" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"status_key":{"type":"keyword","generated":{"op":"lower","field":"status"}}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    const FakeSource = struct {
        fn iface(self: *@This()) StatusSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{
                .metadata_group_id = 91,
                .metrics = .{},
                .projected_stores = 1,
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 91, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 17,
                    .name = "events",
                    .database_name = "tenant_ops",
                    .namespace_name = "analytics",
                    .schema_json = schema_json,
                    .indexes_json = tables_api.default_indexes_json,
                    .placement_role = "data",
                }})[0..]),
                .ranges = &.{},
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };
    const FakeWrites = struct {
        calls: usize = 0,
        first_row_json: []u8 = &.{},

        fn deinit(self: *@This(), allocator: std.mem.Allocator) void {
            if (self.first_row_json.len > 0) allocator.free(self.first_row_json);
        }

        fn source(self: *@This()) table_writes.TableWriteSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .batch = batch,
                    .batch_catalog = batchCatalog,
                },
            };
        }

        fn batch(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.BatchRequest) anyerror!?void {
            return error.UnsupportedOperation;
        }

        fn batchCatalog(ptr: *anyopaque, allocator: std.mem.Allocator, target: catalog_resources.TableTarget, req: db_mod.types.BatchRequest) anyerror!?void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("tenant_ops", target.database_name);
            try std.testing.expectEqualStrings("analytics", target.namespace_name);
            try std.testing.expectEqualStrings("events", target.table_name);
            try std.testing.expectEqual(@as(usize, 1), req.writes.len);
            if (self.first_row_json.len > 0) allocator.free(self.first_row_json);
            self.first_row_json = try allocator.dupe(u8, req.writes[0].value);
            self.calls += 1;
            return {};
        }
    };
    const FakeReads = struct {
        calls: usize = 0,

        fn source(self: *@This()) table_reads.TableReadSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                    .rows_query_plan_catalog = rowsQueryPlanCatalog,
                },
            };
        }

        fn lookup(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: db_mod.types.LookupOptions, _: raft_mod.ReadConsistency) anyerror!?table_reads.LookupResponse {
            return error.UnsupportedOperation;
        }

        fn scan(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: []const u8, _: db_mod.types.ScanOptions, _: raft_mod.ReadConsistency) anyerror!?table_reads.ScanResponse {
            return error.UnsupportedOperation;
        }

        fn query(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.SearchRequest, _: raft_mod.ReadConsistency) anyerror!?query_api.QueryResponse {
            return error.UnsupportedOperation;
        }

        fn rowsQueryPlanCatalog(
            ptr: *anyopaque,
            allocator: std.mem.Allocator,
            target: catalog_resources.TableTarget,
            runtime_schema: runtime_schema_mod.TableSchema,
            plan: db_mod.types.RelationalRowsQueryPlan,
            consistency: raft_mod.ReadConsistency,
        ) anyerror!?db_mod.types.RelationalRowsQueryResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(raft_mod.ReadConsistency.read_index, consistency);
            try std.testing.expectEqualStrings("tenant_ops", target.database_name);
            try std.testing.expectEqualStrings("analytics", target.namespace_name);
            try std.testing.expectEqualStrings("events", target.table_name);
            try std.testing.expectEqual(@as(usize, 3), runtime_schema.relational_columns.len);
            try std.testing.expect(!plan.query.select_all);
            try std.testing.expectEqual(@as(usize, 2), plan.query.select.len);
            try std.testing.expectEqualStrings("id", plan.query.select[0]);
            try std.testing.expectEqualStrings("status", plan.query.select[1]);
            self.calls += 1;

            const rows = try allocator.alloc([]const u8, 1);
            errdefer allocator.free(rows);
            rows[0] = try allocator.dupe(u8, "{\"id\":\"u_export\",\"status\":\"Ready\"}");
            return .{ .rows = rows, .total = 1 };
        }
    };

    var source = FakeSource{};
    var reads = FakeReads{};
    var writes = FakeWrites{};
    defer writes.deinit(alloc);
    var server = ApiHttpServer.init(alloc, .{}, source.iface(), reads.source(), writes.source());
    defer server.deinit();

    var resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body =
        \\{"database":"tenant_ops","namespace":"analytics","sql":"COPY events (id, status) FROM STDIN WITH (FORMAT csv, HEADER true);","stdin_payload":"id,status\nu_public,Ready\n"}
        ,
    });
    defer resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), resp.status);
    try std.testing.expectEqual(@as(usize, 1), writes.calls);

    var parsed_body = try std.json.parseFromSlice(std.json.Value, alloc, resp.body, .{ .allocate = .alloc_always });
    defer parsed_body.deinit();
    try std.testing.expectEqualStrings("write", parsed_body.value.object.get("kind").?.string);
    try std.testing.expectEqualStrings("bulk_io", parsed_body.value.object.get("statement_kind").?.string);

    var first_row = try std.json.parseFromSlice(std.json.Value, alloc, writes.first_row_json, .{});
    defer first_row.deinit();
    try std.testing.expectEqualStrings("u_public", first_row.value.object.get("id").?.string);
    try std.testing.expectEqualStrings("Ready", first_row.value.object.get("status").?.string);
    try std.testing.expectEqualStrings("ready", first_row.value.object.get("status_key").?.string);

    var copy_to_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body =
        \\{"database":"tenant_ops","namespace":"analytics","read_only":true,"sql":"COPY events (id, status) TO STDOUT WITH (FORMAT csv, HEADER true);"}
        ,
    });
    defer copy_to_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), copy_to_resp.status);
    try std.testing.expectEqual(@as(usize, 1), reads.calls);
    var parsed_copy_to = try std.json.parseFromSlice(std.json.Value, alloc, copy_to_resp.body, .{ .allocate = .alloc_always });
    defer parsed_copy_to.deinit();
    try std.testing.expectEqualStrings("bulk_io", parsed_copy_to.value.object.get("kind").?.string);
    try std.testing.expectEqualStrings("bulk_io", parsed_copy_to.value.object.get("statement_kind").?.string);
    const copy_to_result = parsed_copy_to.value.object.get("result").?.object;
    try std.testing.expectEqualStrings("export_rows", copy_to_result.get("operation").?.string);
    try std.testing.expectEqualStrings("stdout", copy_to_result.get("stream").?.string);
    try std.testing.expectEqual(@as(i64, 1), copy_to_result.get("row_count").?.integer);
    try std.testing.expectEqualStrings("id,status\nu_export,Ready\n", copy_to_result.get("payload").?.string);

    var missing_payload_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body =
        \\{"database":"tenant_ops","namespace":"analytics","sql":"COPY events (id, status) FROM STDIN WITH (FORMAT csv, HEADER true);"}
        ,
    });
    defer missing_payload_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 400), missing_payload_resp.status);
    try expectPublicSqlDiagnosticBody(alloc, missing_payload_resp.body, "bind", "invalid_sql_request", "invalid sql request", null, null);

    var prepare_copy_resp = try server.handle(.{
        .method = .POST,
        .uri = "/db/v1/sql",
        .content_type = "application/json",
        .body =
        \\{"database":"tenant_ops","namespace":"analytics","sql":"PREPARE copy_plan AS COPY events (id, status) FROM STDIN WITH (FORMAT csv, HEADER true);"}
        ,
    });
    defer prepare_copy_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 501), prepare_copy_resp.status);
    try expectPublicSqlDiagnosticBody(alloc, prepare_copy_resp.body, "plan", "unsupported_sql_statement", "unsupported sql statement", null, null);
}
