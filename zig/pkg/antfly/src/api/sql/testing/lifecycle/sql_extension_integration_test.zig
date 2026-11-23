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

//! Configured local WASM package lifecycle, not upstream GIS/crypto compatibility.
const std = @import("std");
const local = @import("antfly_local_sources");
const fixture = @import("../owner_test_fixture.zig");
const client = @import("../owner_test_client.zig");
const extensions = @import("../../../../extensions/mod.zig");
const lifecycle = @import("../../../../system_catalog/sql_extensions.zig");
const http = @import("../../../../raft/transport/http_common.zig");
const Wire = @import("antfly_metadata_openapi").types;
const Original = struct { id: []const u8, sql: []const u8, source_entry_sha256: []const u8, sql_sha256: []const u8 };
const Originals = struct { entries: []const Original };
const Version = struct { name: []const u8, version: []const u8, default: []const u8, member: []const u8, marker: i64, from: []const []const u8 = &.{}, dependency: ?[]const u8 = null };
const versions = [_]Version{
    .{ .name = "vector", .version = "1.0.0", .default = "1.0.0", .member = "vector_version_marker", .marker = 1 },
    .{ .name = "pgcrypto", .version = "1.0.0", .default = "1.0.0", .member = "pgcrypto_version_marker", .marker = 1 },
    .{ .name = "postgis", .version = "3.4.0", .default = "3.5.0", .member = "postgis_version_marker", .marker = 340 },
    .{ .name = "postgis", .version = "3.4.5", .default = "3.5.0", .member = "postgis_version_marker", .marker = 345, .from = &.{"3.4.0"} },
    .{ .name = "postgis", .version = "3.5.0", .default = "3.5.0", .member = "postgis_version_marker", .marker = 350, .from = &.{"3.4.5"} },
    .{ .name = "uuid-ossp", .version = "1.0.0", .default = "1.0.0", .member = "uuid_ossp_version_marker", .marker = 100 },
    .{ .name = "spatial_tools", .version = "1.0.0", .default = "1.0.0", .member = "spatial_tools_version_marker", .marker = 1, .dependency = "postgis" },
};

// Preserve the complete real metadata pool, executor pointers and clocks. Only
// one already-accepted extension reply is lost; no SQL body is replayed here.
const LostReply = struct {
    forwards: [4]Forward = undefined,
    armed: std.atomic.Value(bool) = .init(false),
    ready: std.atomic.Value(bool) = .init(false),
    bytes: [8192]u8 = undefined,
    len: usize = 0,
    const Forward = struct {
        inner: http.RequestExecutor,
        owner: *LostReply,
        vtable: http.RequestExecutor.VTable,
        fn init(inner: http.RequestExecutor, owner: *LostReply) @This() {
            return .{ .inner = inner, .owner = owner, .vtable = .{ .execute = execute, .execute_stream = if (inner.vtable.execute_stream != null) stream else null, .supports_concurrent_requests = if (inner.vtable.supports_concurrent_requests != null) concurrent else null } };
        }
        fn executor(self: *@This()) http.RequestExecutor {
            return .{ .ptr = self, .vtable = &self.vtable, .clock_io = self.inner.clock_io, .realtime_ns_fn = if (self.inner.realtime_ns_fn != null) realtime else null };
        }
        fn realtime(raw: *anyopaque) i128 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return self.inner.realtimeNs().?;
        }
        fn concurrent(raw: *const anyopaque) bool {
            const self: *const @This() = @ptrCast(@alignCast(raw));
            return self.inner.supportsConcurrentRequests();
        }
        fn stream(raw: *anyopaque, a: std.mem.Allocator, request: http.HttpRequest, writer: http.StreamWriter) !bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return (try self.inner.executeStream(a, request, writer)) orelse error.TestUnexpectedResult;
        }
        fn execute(raw: *anyopaque, a: std.mem.Allocator, request: http.HttpRequest) !http.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (request.method != .POST or !std.mem.endsWith(u8, request.uri, "/internal/v1/system-catalog") or std.mem.indexOf(u8, request.body, "sql_extension_mutate") == null) return self.inner.execute(a, request);
            var parsed = try std.json.parseFromSlice(@import("../../../../system_catalog/server_call.zig").Call, a, request.body, .{});
            defer parsed.deinit();
            if (parsed.value != .sql_extension_mutate or !std.mem.eql(u8, parsed.value.sql_extension_mutate.name, "postgis") or !self.owner.armed.swap(false, .acq_rel)) return self.inner.execute(a, request);
            const observed = try lifecycle.observation(a, parsed.value.sql_extension_mutate);
            const body = try std.json.Stringify.valueAlloc(a, observed, .{});
            defer a.free(body);
            if (body.len > self.owner.bytes.len) return error.TestUnexpectedResult;
            var response = try self.inner.execute(a, request);
            defer response.deinit(a);
            try std.testing.expectEqual(@as(u16, 200), response.status);
            @memcpy(self.owner.bytes[0..body.len], body);
            self.owner.len = body.len;
            self.owner.ready.store(true, .release);
            return error.DeadlineExceeded;
        }
    };
};

fn query(ctx: *fixture.Context, statement: []const u8) !std.json.Parsed(Wire.SQLResponse) {
    var authorized = ctx.*;
    authorized.headers = ctx.admin_headers;
    var response = try client.execute(&authorized, statement, .never);
    defer response.deinit(ctx.alloc);
    if (response.status != 200 and response.status != 202) {
        std.debug.print("extension statement={s} status={d} body={s}\n", .{ statement, response.status, response.body[0..@min(response.body.len, 4096)] });
        return error.ExtensionOriginalFailed;
    }
    return std.json.parseFromSlice(Wire.SQLResponse, ctx.alloc, response.body, .{ .allocate = .alloc_always });
}
fn recover(ctx: *fixture.Context, receipt: Wire.SQLCatalogOperationReceipt) !void {
    const body = try std.json.Stringify.valueAlloc(ctx.alloc, Wire.SQLCatalogRecoveryRequest{ .recovery_token = receipt.recovery_token }, .{});
    defer ctx.alloc.free(body);
    const deadline = @import("antfly_platform").time.monotonicNs() +| 30 * std.time.ns_per_s;
    while (@import("antfly_platform").time.monotonicNs() < deadline) {
        try ctx.checkDrivers();
        var response = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.base, "/db/v1/sql/operations/status", .POST, body, 5000);
        defer response.deinit(ctx.alloc);
        if (response.status == 503 or response.status == 504) continue;
        try std.testing.expectEqual(@as(u16, 200), response.status);
        var result = try std.json.parseFromSlice(Wire.SQLCatalogOperationReceipt, ctx.alloc, response.body, .{});
        defer result.deinit();
        try std.testing.expectEqualStrings(receipt.recovery_token, result.value.recovery_token);
        try std.testing.expectEqual(receipt.operation, result.value.operation);
        try std.testing.expectEqual(receipt.kind, result.value.kind);
        inline for (.{ "database", "namespace", "name" }) |field| try std.testing.expectEqualStrings(@field(receipt, field), @field(result.value, field));
        if (result.value.state == .committed) return;
        if (result.value.state == .rejected) return error.ExtensionAdmissionRejected;
        try ctx.io.sleep(.fromMilliseconds(10), .awake);
    }
    return error.ExtensionRecoveryDeadline;
}
fn command(ctx: *fixture.Context, statement: []const u8) !void {
    var result = try query(ctx, statement);
    defer result.deinit();
    if (result.value.catalog_receipt) |receipt| try recover(ctx, receipt);
}
fn rejected(ctx: *fixture.Context, statement: []const u8, code: []const u8, other_owner: bool) !void {
    var authorized = ctx.*;
    authorized.headers = if (other_owner) ctx.reader_headers else ctx.admin_headers;
    var response = try client.execute(&authorized, statement, .never);
    defer response.deinit(ctx.alloc);
    try std.testing.expect(response.status >= 400);
    var diagnostic = try std.json.parseFromSlice(struct { code: []const u8 }, ctx.alloc, response.body, .{ .ignore_unknown_fields = true });
    defer diagnostic.deinit();
    try std.testing.expectEqualStrings(code, diagnostic.value.code);
}
fn prepareResource(ctx: *fixture.Context, connection: []const u8) ![]u8 {
    const body = try std.json.Stringify.valueAlloc(ctx.alloc, Wire.SQLPrepareRequest{ .statement = "SELECT postgis_version_marker($1::bigint)", .connection_id = connection }, .{});
    defer ctx.alloc.free(body);
    var response = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.base, "/db/v1/sql/prepared", .POST, body, 15000);
    defer response.deinit(ctx.alloc);
    if (response.status != 200) std.debug.print("extension prepare HTTP status={d} body={s}\n", .{ response.status, response.body[0..@min(response.body.len, 2048)] });
    try std.testing.expectEqual(@as(u16, 200), response.status);
    var result = try std.json.parseFromSlice(Wire.SQLPreparedResponse, ctx.alloc, response.body, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 32), result.value.prepared_id.len);
    try std.testing.expectEqual(@as(usize, 1), result.value.parameter_types.len);
    try std.testing.expectEqual(Wire.SQLColumnType.integer, result.value.parameter_types[0]);
    try std.testing.expectEqual(@as(usize, 1), result.value.parameter_descriptors.len);
    return ctx.alloc.dupe(u8, result.value.prepared_id);
}
fn executePreparedResource(ctx: *fixture.Context, connection: ?[]const u8, id: []const u8) !http.HttpResponse {
    const body = try std.json.Stringify.valueAlloc(ctx.alloc, Wire.SQLPreparedExecutionRequest{ .parameters = &.{.{ .integer = 10 }}, .connection_id = connection }, .{});
    defer ctx.alloc.free(body);
    const path = try std.fmt.allocPrint(ctx.alloc, "/db/v1/sql/prepared/{s}/execute", .{id});
    defer ctx.alloc.free(path);
    return fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.base, path, .POST, body, 15000);
}
fn preparedRows(ctx: *fixture.Context, connection: []const u8, id: []const u8, expected: i64) !void {
    var response = try executePreparedResource(ctx, connection, id);
    defer response.deinit(ctx.alloc);
    if (response.status != 200) std.debug.print("extension execute HTTP status={d} body={s}\n", .{ response.status, response.body[0..@min(response.body.len, 2048)] });
    try std.testing.expectEqual(@as(u16, 200), response.status);
    var result = try std.json.parseFromSlice(Wire.SQLResponse, ctx.alloc, response.body, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.value.rows.len);
    try std.testing.expectEqual(@as(usize, 1), result.value.rows[0].len);
    try std.testing.expectEqual(expected, try fixture.sqlInteger(result.value.rows[0][0]));
}
fn closePreparedResource(ctx: *fixture.Context, connection: []const u8, id: []const u8) !void {
    const headers = try ctx.alloc.alloc(http.RequestHeader, ctx.admin_headers.len + 1);
    defer ctx.alloc.free(headers);
    @memcpy(headers[0..ctx.admin_headers.len], ctx.admin_headers);
    // The close route takes its connection binding from this public header;
    // it deliberately does not parse a JSON request body.
    headers[ctx.admin_headers.len] = .{ .name = "x-antfly-sql-connection-id", .value = connection };
    const path = try std.fmt.allocPrint(ctx.alloc, "/db/v1/sql/prepared/{s}", .{id});
    defer ctx.alloc.free(path);
    var response = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, headers, ctx.base, path, .DELETE, null, 15000);
    defer response.deinit(ctx.alloc);
    try std.testing.expectEqual(@as(u16, 200), response.status);
}
fn closePreparedConnection(ctx: *fixture.Context, connection: []const u8) !void {
    const path = try std.fmt.allocPrint(ctx.alloc, "/db/v1/sql/connections/{s}", .{connection});
    defer ctx.alloc.free(path);
    var response = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.base, path, .DELETE, null, 30000);
    defer response.deinit(ctx.alloc);
    try std.testing.expect(response.status == 200 or response.status == 204);
}
fn snapshot(ctx: *fixture.Context, name: []const u8) !std.json.Parsed(lifecycle.Snapshot) {
    const request: lifecycle.SnapshotRequest = .{ .database = "default", .namespace = "public", .name = name };
    var transport = ctx.catalogClient();
    var response = try transport.readSystemCatalog(ctx.metadata_uri, .{ .sql_extension_snapshot = request }, 5000, null);
    defer response.deinit(ctx.alloc);
    var result = try std.json.parseFromSlice(lifecycle.Snapshot, ctx.alloc, response.body, .{ .allocate = .alloc_always });
    errdefer result.deinit();
    try result.value.validate(request);
    return result;
}
fn installed(ctx: *fixture.Context, name: []const u8, version: []const u8, member: []const u8, value: i64) !lifecycle.Identity {
    var state = try snapshot(ctx, name);
    defer state.deinit();
    const current = state.value.installed orelse return error.ExtensionNotInstalled;
    try std.testing.expectEqualStrings(name, current.record.package_name);
    try std.testing.expectEqualStrings(version, current.record.package_version);
    try std.testing.expect(current.record.owner == .user);
    try std.testing.expectEqualStrings("pk-rewriter", current.record.owner.user.name);
    try std.testing.expect(!std.mem.allEqual(u8, &current.record.owner.user.instance_id, 0));
    try std.testing.expectEqual(@as(usize, 1), current.record.members.len);
    try std.testing.expectEqualStrings(member, current.record.members[0].name);
    try std.testing.expect(current.record.members[0].id != 0 and current.record.members[0].generation != 0);
    var matched = false;
    for (state.value.packages) |package| if (std.mem.eql(u8, package.name, name) and std.mem.eql(u8, package.version, version)) {
        try std.testing.expectEqualDeep(try extensions.sql_package.digest(package.digest), current.record.package_digest);
        matched = true;
    };
    try std.testing.expect(matched);
    const sql = try std.fmt.allocPrint(ctx.alloc, "SELECT {s}(10::bigint)", .{member});
    defer ctx.alloc.free(sql);
    var result = try query(ctx, sql);
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.value.rows.len);
    try std.testing.expectEqual(@as(usize, 1), result.value.rows[0].len);
    try std.testing.expectEqual(value, try fixture.sqlInteger(result.value.rows[0][0]));
    return .{ .id = current.record.id, .generation = current.record.generation };
}
fn absent(ctx: *fixture.Context, name: []const u8) !void {
    var state = try snapshot(ctx, name);
    defer state.deinit();
    try std.testing.expect(state.value.installed == null);
}

const Campaign = struct {
    pub const native_auth = true;
    pub const native_sessions = true;
    pub const cold_catalog = true;
    package_root: []const u8,
    packages: []const extensions.PackageManifest,
    lost: LostReply = .{},
    credentials: client.ApiKeyHeaders = .{},
    other_authorization: [192]u8 = undefined,
    other_headers: [1]http.RequestHeader = undefined,
    administrator_authorization: [192]u8 = undefined,
    administrator_headers: [1]http.RequestHeader = undefined,
    administrator_key_id: [128]u8 = undefined,
    administrator_key_id_len: usize = 0,
    receipt: [8192]u8 = undefined,
    receipt_len: usize = 0,
    vector: lifecycle.Identity = undefined,
    crypto: lifecycle.Identity = undefined,
    uuid: lifecycle.Identity = undefined,
    pub fn configureAuth(manager: *@import("../../../../usermgr/mod.zig").UserManager) !void {
        const users = @import("../../../../usermgr/mod.zig");
        inline for (.{ users.ResourceType.database, users.ResourceType.namespace }) |kind| {
            // Metadata administration and reading are distinct grants. The
            // read-only key below must delegate an actual existing grant.
            inline for (.{ users.PermissionType.admin, users.PermissionType.read }) |operation| {
                var permission = try users.Permission.initOwned(manager.alloc, kind, "*", operation);
                defer permission.deinit(manager.alloc);
                inline for (.{ "pk-rewriter", "pk-reader" }) |name| try manager.addPermissionToUser(name, permission);
            }
        }
    }
    pub fn configureData(self: *@This(), data: *@import("../../../../data/runtime.zig").DataServer, _: std.Io) !void {
        data.api_server_cfg.extension_package_store_dir = self.package_root;
        const remote = data.remote_metadata orelse return error.TestUnexpectedResult;
        try std.testing.expectEqual(@as(usize, 0), remote.request_executors.len);
        try std.testing.expectEqual(self.lost.forwards.len, remote.http_executors.len);
        const pool = try remote.alloc.alloc(http.RequestExecutor, remote.http_executors.len);
        for (remote.http_executors, pool, 0..) |*inner, *out, i| {
            self.lost.forwards[i] = LostReply.Forward.init(inner.executor(), &self.lost);
            out.* = self.lost.forwards[i].executor();
        }
        remote.request_executors = pool;
    }
    fn authorize(self: *@This(), ctx: *fixture.Context) !void {
        try self.credentials.activateWithCatalog(ctx, true);
        // Antfly database administration deliberately manages schema objects
        // created by other users. Use a read-attenuated credential for the
        // negative PostgreSQL-role witness, then test that override separately.
        const users = @import("../../../../usermgr/mod.zig");
        const manager = (ctx.metadata.server.owned_public_http_server orelse return error.TestUnexpectedResult).cfg.user_manager orelse return error.TestUnexpectedResult;
        var database = try users.Permission.initOwned(ctx.alloc, .database, "*", .read);
        defer database.deinit(ctx.alloc);
        var namespace = try users.Permission.initOwned(ctx.alloc, .namespace, "*", .read);
        defer namespace.deinit(ctx.alloc);
        var key = try manager.createApiKey("pk-reader", "extension-read-only-negative", &.{ database, namespace }, &.{}, null);
        defer key.deinit(manager.alloc);
        self.other_headers[0] = .{ .name = "authorization", .value = try std.fmt.bufPrint(&self.other_authorization, "ApiKey {s}", .{key.encoded}) };
        ctx.reader_headers = &self.other_headers;
        var database_admin = try users.Permission.initOwned(ctx.alloc, .database, "*", .admin);
        defer database_admin.deinit(ctx.alloc);
        var namespace_admin = try users.Permission.initOwned(ctx.alloc, .namespace, "*", .admin);
        defer namespace_admin.deinit(ctx.alloc);
        var elevated = try manager.createApiKey("pk-reader", "extension-administrative-management", &.{ database_admin, namespace_admin }, &.{}, null);
        defer elevated.deinit(manager.alloc);
        self.administrator_headers[0] = .{ .name = "authorization", .value = try std.fmt.bufPrint(&self.administrator_authorization, "ApiKey {s}", .{elevated.encoded}) };
        if (elevated.key.key_id.len > self.administrator_key_id.len) return error.TestUnexpectedResult;
        @memcpy(self.administrator_key_id[0..elevated.key.key_id.len], elevated.key.key_id);
        self.administrator_key_id_len = elevated.key.key_id.len;
    }
    fn administrativeOverride(self: *@This(), ctx: *fixture.Context) !void {
        var before = try snapshot(ctx, "vector");
        defer before.deinit();
        var administrator = ctx.*;
        administrator.admin_headers = &self.administrator_headers;
        administrator.headers = &self.administrator_headers;
        try command(&administrator, "COMMENT ON EXTENSION vector IS 'explicit database administration'");
        var after = try snapshot(ctx, "vector");
        defer after.deinit();
        try std.testing.expectEqualStrings("explicit database administration", after.value.installed.?.description.?);
        try std.testing.expectEqualDeep(before.value.installed.?.record, after.value.installed.?.record);
        // Revoking the elevated key also revokes schema administration.
        const manager = (ctx.metadata.server.owned_public_http_server orelse return error.TestUnexpectedResult).cfg.user_manager orelse return error.TestUnexpectedResult;
        try manager.deleteApiKey("pk-reader", self.administrator_key_id[0..self.administrator_key_id_len]);
        var denied = try client.execute(&administrator, "COMMENT ON EXTENSION vector IS 'revoked administrator'", .never);
        defer denied.deinit(ctx.alloc);
        if (denied.status != 401 and denied.status != 403) {
            try std.testing.expectEqual(@as(u16, 400), denied.status);
            var diagnostic = try std.json.parseFromSlice(struct { code: []const u8 }, ctx.alloc, denied.body, .{ .ignore_unknown_fields = true });
            defer diagnostic.deinit();
            try std.testing.expect(std.mem.eql(u8, diagnostic.value.code, "42501") or std.mem.eql(u8, diagnostic.value.code, "28000"));
        }
        var unchanged = try snapshot(ctx, "vector");
        defer unchanged.deinit();
        try std.testing.expectEqualStrings("explicit database administration", unchanged.value.installed.?.description.?);
        try std.testing.expectEqualDeep(after.value.installed.?.record, unchanged.value.installed.?.record);
    }
    fn original(self: *@This(), ctx: *fixture.Context, id: []const u8, capture: bool) !void {
        var cases = try std.json.parseFromSlice(Originals, ctx.alloc, @embedFile("../fixtures/sql_extension_lifecycle_originals.json"), .{ .ignore_unknown_fields = true });
        defer cases.deinit();
        const found = for (cases.value.entries) |entry| {
            if (std.mem.eql(u8, entry.id, id)) break entry;
        } else return error.ExtensionOriginalMissing;
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(found.sql, &digest, .{});
        try std.testing.expectEqualStrings(found.sql_sha256, &std.fmt.bytesToHex(digest, .lower));
        try std.testing.expectEqual(@as(usize, 64), found.source_entry_sha256.len);
        var response = try query(ctx, found.sql);
        defer response.deinit();
        const receipt = response.value.catalog_receipt orelse return error.ExtensionCatalogReceiptMissing;
        if (capture) {
            const encoded = try std.json.Stringify.valueAlloc(ctx.alloc, receipt, .{});
            defer ctx.alloc.free(encoded);
            if (encoded.len > self.receipt.len) return error.TestUnexpectedResult;
            @memcpy(self.receipt[0..encoded.len], encoded);
            self.receipt_len = encoded.len;
        }
        try recover(ctx, receipt);
    }
    fn retained(self: *@This(), ctx: *fixture.Context) !void {
        try std.testing.expect(self.lost.ready.load(.acquire));
        var request = try std.json.parseFromSlice(lifecycle.OutcomeRequest, ctx.alloc, self.lost.bytes[0..self.lost.len], .{});
        defer request.deinit();
        var transport = ctx.catalogClient();
        var response = try transport.readSystemCatalog(ctx.metadata_uri, .{ .sql_extension_outcome = request.value }, 5000, null);
        defer response.deinit(ctx.alloc);
        var outcome = try std.json.parseFromSlice(?lifecycle.Outcome, ctx.alloc, response.body, .{});
        defer outcome.deinit();
        try std.testing.expect(outcome.value != null and outcome.value.? == .success);
        try std.testing.expectEqual(.drop, request.value.operation);
        try std.testing.expect(outcome.value.?.success.identity == null);
        try std.testing.expect(outcome.value.?.success.removed != 0);
        var receipt = try std.json.parseFromSlice(Wire.SQLCatalogOperationReceipt, ctx.alloc, self.receipt[0..self.receipt_len], .{});
        defer receipt.deinit();
        try recover(ctx, receipt.value);
    }
    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        try self.authorize(ctx);
        for (self.packages) |package| try ctx.metadata.server.svc.upsertExtensionPackage(package);
        try command(ctx, "CREATE EXTENSION vector");
        try self.original(ctx, "sql-0148", false);
        var comment = try snapshot(ctx, "vector");
        defer comment.deinit();
        try std.testing.expectEqualStrings("vector search extension", comment.value.installed.?.description.?);
        try rejected(ctx, "COMMENT ON EXTENSION vector IS 'wrong owner'", "42501", true);
        try self.administrativeOverride(ctx);
        try command(ctx, "COMMENT ON EXTENSION vector IS NULL");
        self.vector = try installed(ctx, "vector", "1.0.0", "vector_version_marker", 11);
        var cleared = try snapshot(ctx, "vector");
        defer cleared.deinit();
        try std.testing.expect(cleared.value.installed.?.description == null);
        try self.original(ctx, "sql-0754", false);
        const first_crypto = try installed(ctx, "pgcrypto", "1.0.0", "pgcrypto_version_marker", 11);
        try self.original(ctx, "sql-0754", false);
        try std.testing.expectEqualDeep(first_crypto, try installed(ctx, "pgcrypto", "1.0.0", "pgcrypto_version_marker", 11));
        try command(ctx, "DROP EXTENSION pgcrypto");
        try self.original(ctx, "sql-0755", false);
        self.crypto = try installed(ctx, "pgcrypto", "1.0.0", "pgcrypto_version_marker", 11);
        try self.original(ctx, "sql-0755", false);
        try std.testing.expectEqualDeep(self.crypto, try installed(ctx, "pgcrypto", "1.0.0", "pgcrypto_version_marker", 11));
        try command(ctx, "COMMENT ON EXTENSION pgcrypto IS 'crypto package comment survives restart'");
        try rejected(ctx, "CREATE EXTENSION postgis VERSION '9.9.9'", "22023", false);
        try absent(ctx, "postgis");
        try self.original(ctx, "sql-0756", false);
        _ = try installed(ctx, "postgis", "3.5.0", "postgis_version_marker", 360);
        try command(ctx, "DROP EXTENSION postgis");
        try self.original(ctx, "sql-0757", false);
        const initial = try installed(ctx, "postgis", "3.4.0", "postgis_version_marker", 350);
        try rejected(ctx, "CREATE EXTENSION postgis", "42710", false);
        try rejected(ctx, "ALTER EXTENSION postgis UPDATE TO '9.9.9'", "22023", false);
        try rejected(ctx, "DROP EXTENSION postgis", "42501", true);
        try rejected(ctx, "DROP FUNCTION postgis_version_marker(bigint)", "2BP01", false);
        try std.testing.expectEqualDeep(initial, try installed(ctx, "postgis", "3.4.0", "postgis_version_marker", 350));
        // PREPARE retains immutable binding identities, not a callable or a
        // Wasmtime instance. Each EXECUTE opens its own actual owned module.
        var opened = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.base, "/db/v1/sql/connections", .POST, "{}", 15000);
        defer opened.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 200), opened.status);
        var connection_result = try std.json.parseFromSlice(struct { connection_id: []const u8 }, ctx.alloc, opened.body, .{ .ignore_unknown_fields = true });
        defer connection_result.deinit();
        const connection = connection_result.value.connection_id;
        var connection_live = true;
        defer if (connection_live) closePreparedConnection(ctx, connection) catch {};
        const first_prepared = try prepareResource(ctx, connection);
        defer ctx.alloc.free(first_prepared);
        var first_prepared_live = true;
        defer if (first_prepared_live) closePreparedResource(ctx, connection, first_prepared) catch {};
        // A genuine connection-bound resource cannot execute without its
        // original connection, even under the same authenticated principal.
        var unbound = try executePreparedResource(ctx, null, first_prepared);
        defer unbound.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 400), unbound.status);
        var inaccessible = try std.json.parseFromSlice(struct { code: []const u8 }, ctx.alloc, unbound.body, .{ .ignore_unknown_fields = true });
        defer inaccessible.deinit();
        try std.testing.expectEqualStrings("26000", inaccessible.value.code);
        try preparedRows(ctx, connection, first_prepared, 350);
        // The public default update must traverse both real declared edges.
        try command(ctx, "ALTER EXTENSION postgis UPDATE TO '3.5.0'");
        var stale = try executePreparedResource(ctx, connection, first_prepared);
        defer stale.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 409), stale.status);
        var diagnostic = try std.json.parseFromSlice(struct { code: []const u8 }, ctx.alloc, stale.body, .{ .ignore_unknown_fields = true });
        defer diagnostic.deinit();
        try std.testing.expectEqualStrings("40001", diagnostic.value.code);
        // Only an explicit new public preparation binds the new version.
        try closePreparedResource(ctx, connection, first_prepared);
        first_prepared_live = false;
        const current_prepared = try prepareResource(ctx, connection);
        defer ctx.alloc.free(current_prepared);
        var current_prepared_live = true;
        defer if (current_prepared_live) closePreparedResource(ctx, connection, current_prepared) catch {};
        try preparedRows(ctx, connection, current_prepared, 360);
        try closePreparedResource(ctx, connection, current_prepared);
        current_prepared_live = false;
        try closePreparedConnection(ctx, connection);
        connection_live = false;
        const advanced = try installed(ctx, "postgis", "3.5.0", "postgis_version_marker", 360);
        try std.testing.expectEqual(initial.id, advanced.id);
        try std.testing.expectEqual(initial.generation + 2, advanced.generation);
        try self.original(ctx, "sql-0758", false);
        try std.testing.expectEqualDeep(advanced, try installed(ctx, "postgis", "3.5.0", "postgis_version_marker", 360));
        try self.original(ctx, "sql-0759", false);
        self.uuid = try installed(ctx, "uuid-ossp", "1.0.0", "uuid_ossp_version_marker", 110);
        try command(ctx, "DROP EXTENSION postgis");
        try command(ctx, "CREATE EXTENSION postgis VERSION '3.4.0'");
        try self.original(ctx, "sql-0760", false);
        _ = try installed(ctx, "postgis", "3.5.0", "postgis_version_marker", 360);
        try command(ctx, "DROP EXTENSION postgis");
        try command(ctx, "CREATE EXTENSION postgis VERSION '3.4.0'");
        try self.original(ctx, "sql-0761", false);
        _ = try installed(ctx, "postgis", "3.5.0", "postgis_version_marker", 360);
        try self.original(ctx, "sql-0762", false);
        try absent(ctx, "postgis");
        try rejected(ctx, "SELECT postgis_version_marker(10::bigint)", "42883", false);
        try command(ctx, "CREATE EXTENSION postgis VERSION '3.4.0'");
        // Persisted expressions currently reject named routines. Keep that
        // function-only boundary explicit; PostgreSQL's external VIEW cascade
        // witness is future native work, not evidence supplied by this test.
        try rejected(ctx, "CREATE VIEW extension_future_external_view AS SELECT postgis_version_marker(10::bigint)", "0A000", false);
        var view_client = ctx.catalogClient();
        var missing_view = try view_client.readSystemCatalog(ctx.metadata_uri, .{ .resolve_many = .{ .sql_sources = &.{.{ .name = .{ .table = "extension_future_external_view" } }} } }, 5000, null);
        defer missing_view.deinit(ctx.alloc);
        var view_cut = try std.json.parseFromSlice(local.system_catalog_domain.ResolvedMany, ctx.alloc, missing_view.body, .{});
        defer view_cut.deinit();
        try std.testing.expectEqual(@as(usize, 1), view_cut.value.sql_sources.len);
        try std.testing.expect(view_cut.value.sql_sources[0] == null);
        try command(ctx, "CREATE EXTENSION spatial_tools");
        var dependent = try snapshot(ctx, "spatial_tools");
        defer dependent.deinit();
        try std.testing.expectEqual(@as(usize, 1), dependent.value.installed.?.record.dependencies.len);
        try std.testing.expectEqualStrings("postgis", dependent.value.installed.?.record.dependencies[0].name);
        try rejected(ctx, "DROP EXTENSION postgis", "2BP01", false);
        // Keep this actual accepted lost reply recent within the bounded
        // shared outcome ring; retained outcomes are not promised forever.
        self.lost.armed.store(true, .release);
        try self.original(ctx, "sql-0763", true);
        try absent(ctx, "postgis");
        try absent(ctx, "spatial_tools");
        try rejected(ctx, "SELECT spatial_tools_version_marker(10::bigint)", "42883", false);
        // Historical retained receipt must not reinstall an already-dropped
        // extension or invoke its former WASM routine while recovering.
        try self.retained(ctx);
        try absent(ctx, "postgis");
    }
    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        try self.authorize(ctx);
        try self.retained(ctx);
        try absent(ctx, "postgis");
        try absent(ctx, "spatial_tools");
        try std.testing.expectEqualDeep(self.vector, try installed(ctx, "vector", "1.0.0", "vector_version_marker", 11));
        try std.testing.expectEqualDeep(self.crypto, try installed(ctx, "pgcrypto", "1.0.0", "pgcrypto_version_marker", 11));
        var crypto_comment = try snapshot(ctx, "pgcrypto");
        defer crypto_comment.deinit();
        try std.testing.expectEqualStrings("crypto package comment survives restart", crypto_comment.value.installed.?.description.?);
        const old_member = crypto_comment.value.installed.?.record.members[0].id;
        // The annotation belongs to the retired extension identity. A later
        // package install at the same name must create fresh owned members
        // and must not inherit that description from the former incarnation.
        try command(ctx, "DROP EXTENSION pgcrypto");
        try absent(ctx, "pgcrypto");
        try command(ctx, "CREATE EXTENSION pgcrypto");
        const replacement = try installed(ctx, "pgcrypto", "1.0.0", "pgcrypto_version_marker", 11);
        try std.testing.expect(replacement.id != self.crypto.id);
        var fresh_crypto = try snapshot(ctx, "pgcrypto");
        defer fresh_crypto.deinit();
        // As PostgreSQL's extension control comment does, the immutable
        // package description initializes each fresh installation. It must
        // replace the prior user's annotation, not be suppressed as NULL.
        const declared_description = for (self.packages) |package| {
            if (std.mem.eql(u8, package.name, "pgcrypto") and std.mem.eql(u8, package.version, "1.0.0")) break package.description;
        } else return error.TestUnexpectedResult;
        try std.testing.expectEqualStrings(declared_description, fresh_crypto.value.installed.?.description.?);
        try std.testing.expect(!std.mem.eql(u8, "crypto package comment survives restart", fresh_crypto.value.installed.?.description.?));
        try std.testing.expect(old_member != fresh_crypto.value.installed.?.record.members[0].id);

        try std.testing.expectEqualDeep(self.uuid, try installed(ctx, "uuid-ossp", "1.0.0", "uuid_ossp_version_marker", 110));
        var state = try snapshot(ctx, "vector");
        defer state.deinit();
        try std.testing.expect(state.value.installed.?.description == null);
    }
};

fn packages(a: std.mem.Allocator, directory: std.Io.Dir) ![]const extensions.PackageManifest {
    const result = try a.alloc(extensions.PackageManifest, versions.len);
    for (versions, result) |version, *out| {
        const module = try @import("../../../../extensions/wasmtime_runtime.zig").test_support.markerModule(a, version.marker);
        defer a.free(module);
        var artifact_digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(module, &artifact_digest, .{});
        var package_hash = std.crypto.hash.sha2.Sha256.init(.{});
        package_hash.update("antfly:extension-lifecycle-owned-fixture:v1\x00");
        package_hash.update(version.name);
        package_hash.update(&.{0});
        package_hash.update(version.version);
        package_hash.update(&artifact_digest);
        const package_digest = package_hash.finalResult();
        const artifact_text = try std.fmt.allocPrint(a, "sha256:{s}", .{std.fmt.bytesToHex(artifact_digest, .lower)});
        const package_text = try std.fmt.allocPrint(a, "sha256:{s}", .{std.fmt.bytesToHex(package_digest, .lower)});
        const path = try std.fmt.allocPrint(a, "sha256/{s}", .{std.fmt.bytesToHex(package_digest, .lower)});
        try directory.createDirPath(std.testing.io, path);
        const wasm_path = try std.fmt.allocPrint(a, "{s}/module.wasm", .{path});
        try directory.writeFile(std.testing.io, .{ .sub_path = wasm_path, .data = module });
        const export_list = try a.alloc(extensions.sql_package.Export, 1);
        export_list[0] = .{ .name = version.member, .runtime = "sql", .export_name = version.member, .arguments = &.{.{ .type = .{ .scalar = .int64 } }}, .result = .{ .scalar = .int64 }, .strict = true, .volatility = .immutable };
        const runtime_list = try a.alloc(extensions.RuntimeDecl, 1);
        runtime_list[0] = .{ .name = "sql", .mode = .wasm, .artifact = "module.wasm" };
        const artifact_list = try a.alloc(extensions.PackageArtifact, 1);
        artifact_list[0] = .{ .kind = .wasm, .path = "module.wasm", .digest = artifact_text };
        const dependencies = if (version.dependency) |name| block: {
            const entries = try a.alloc(extensions.PackageDependency, 1);
            entries[0] = .{ .name = name };
            break :block entries;
        } else &.{};
        out.* = .{ .name = version.name, .version = version.version, .description = "Controlled lifecycle fixture, not the upstream implementation", .digest = package_text, .relocatable = true, .dependencies = dependencies, .artifacts = artifact_list, .install = .{ .scopes_supported = &.{.embedded_db}, .runtimes = runtime_list }, .sql = .{ .default_version = version.default, .exports = export_list, .updates_from = version.from } };
        try out.validate();
        const manifest = try std.json.Stringify.valueAlloc(a, out.*, .{});
        const manifest_path = try std.fmt.allocPrint(a, "{s}/{s}", .{ path, extensions.package_manifest_filename });
        try directory.writeFile(std.testing.io, .{ .sub_path = manifest_path, .data = manifest });
    }
    return result;
}

test "mounted SQL extension originals own real WASM members and retain exact lifecycle outcomes across cold restart" {
    // sql-0148 sql-0754 sql-0755 sql-0756 sql-0757 sql-0758 sql-0759
    // sql-0760 sql-0761 sql-0762 sql-0763: exact configured-package originals.
    // No upstream GIS/crypto/UUID/vector payload or external-view claim.
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", arena.allocator());
    try fixture.runWithState(Campaign, .{ .package_root = root, .packages = try packages(arena.allocator(), tmp.dir) });
}
