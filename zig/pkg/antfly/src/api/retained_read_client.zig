// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
const std = @import("std");
const rpc = @import("retained_read_rpc.zig");
const registry = @import("../storage/retained_read_registry.zig");
const reads = @import("table_read_source.zig");
const types = @import("../storage/db/types.zig");
const metadata = @import("../metadata/api.zig");
const http = @import("../common/http/http_common.zig");
const time = @import("antfly_platform").time;

/// Owns endpoint and catalog capability bytes. Executor must be a stable
/// server-owned authenticated transport, never a stack-built hosted adapter.
pub const Client = struct {
    alloc: std.mem.Allocator,
    executor: http.RequestExecutor,
    uri: []u8,
    route: []u8,
    schema_version: u32,
    deadline_ns: u64,
    token: registry.Token,
    connection: u128,
    sequence: u64 = 0,
    poisoned: bool = false,

    fn create(alloc: std.mem.Allocator, executor: http.RequestExecutor, base_uri: []const u8, table: []const u8, route: metadata.CatalogRouteFence, schema_version: u32, deadline_ns: ?u64) !*Client {
        // Table names are escaped at the path boundary, not interpolated raw.
        const escaped = try @import("http_client.zig").percentEncodePathComponent(alloc, table);
        defer alloc.free(escaped);
        const self = try alloc.create(Client);
        errdefer alloc.destroy(self);
        const uri = try std.fmt.allocPrint(alloc, "{s}/internal/v1/groups/{d}/tables/{s}/retained-read", .{ std.mem.trimEnd(u8, base_uri, "/"), route.route.group_id, escaped });
        errdefer alloc.free(uri);
        const encoded_route = try std.json.Stringify.valueAlloc(alloc, route, .{});
        errdefer alloc.free(encoded_route);
        const clock = executor.clock_io orelse return error.ReadUnavailable;
        var receiver = try clock.receive();
        var nonce: [16]u8 = undefined;
        try receiver.io().randomSecure(&nonce);
        const connection = std.mem.readInt(u128, &nonce, .little);
        if (connection == 0) return error.ReadUnavailable;
        self.* = .{ .alloc = alloc, .executor = executor, .uri = uri, .route = encoded_route, .schema_version = schema_version, .deadline_ns = deadline_ns orelse time.monotonicNs() +| 30 * std.time.ns_per_s, .token = undefined, .connection = connection };
        return self;
    }

    fn destroy(self: *Client) void {
        const alloc = self.alloc;
        alloc.free(self.uri);
        alloc.free(self.route);
        alloc.destroy(self);
    }

    pub fn admit(alloc: std.mem.Allocator, executor: http.RequestExecutor, base_uri: []const u8, table: []const u8, route: metadata.CatalogRouteFence, schema_version: u32, deadline_ns: ?u64) !void {
        const self = try create(alloc, executor, base_uri, table, route, schema_version, deadline_ns);
        defer self.destroy();
        var response = try self.call(.{ .operation = .admit, .schema_version = schema_version });
        defer response.deinit(alloc);
    }

    pub fn capture(alloc: std.mem.Allocator, executor: http.RequestExecutor, base_uri: []const u8, table: []const u8, route: metadata.CatalogRouteFence, schema_version: u32, deadline_ns: ?u64) !?reads.StatementReadFence {
        const self = try create(alloc, executor, base_uri, table, route, schema_version, deadline_ns);
        errdefer self.destroy();
        errdefer self.cancelUnknown();
        var response = try self.call(.{ .operation = .capture, .schema_version = schema_version, .lease_ms = 5000, .connection = self.connection });
        defer response.deinit(alloc);
        var parsed = try std.json.parseFromSlice(rpc.Response, alloc, response.body, .{});
        defer parsed.deinit();
        if (parsed.value.busy and parsed.value.token == null) {
            self.destroy();
            return null;
        }
        self.token = parsed.value.token orelse return error.InvalidRetainedReadResponse;
        return .{ .ptr = self, .vtable = &.{ .validate = validate, .open = open, .capture_snapshot = captureSnapshot, .release = close } };
    }

    fn call(self: *Client, request: rpc.Request) !http.HttpResponse {
        const cleanup = request.operation == .close or request.operation == .cancel;
        if (self.poisoned and !cleanup) return error.RetainedReadRestartRequired;
        const now = time.monotonicNs();
        if (now >= self.deadline_ns and !cleanup) return error.DeadlineExceeded;
        const timeout: u32 = if (cleanup) 100 else @intCast(@max(1, @min(5000, (self.deadline_ns -| now) / std.time.ns_per_ms)));
        const body = try std.json.Stringify.valueAlloc(self.alloc, request, .{});
        defer self.alloc.free(body);
        if (body.len > rpc.max_request_bytes) return error.InvalidRetainedReadQuery;
        var timeout_buffer: [16]u8 = undefined;
        const headers = [_]http.RequestHeader{
            .{ .name = metadata.catalog_route_fence_header, .value = self.route },
            .{ .name = metadata.catalog_route_deadline_ms_header, .value = try std.fmt.bufPrint(&timeout_buffer, "{d}", .{timeout}) },
        };
        // NEVER retry: capture/open may have allocated a capability; next may
        // already have advanced. Unknown handles are bounded by owner expiry.
        var response = self.executor.execute(self.alloc, .{ .method = .POST, .uri = self.uri, .headers = &headers, .content_type = "application/json", .body = body, .timeout_ms = timeout, .max_response_bytes = rpc.max_response_bytes }) catch |err| {
            self.poisoned = true;
            return err;
        };
        errdefer response.deinit(self.alloc);
        // This negative capability response is safe to classify without a
        // success acknowledgement. It authorizes no cursor/page reuse: the
        // caller must release captures and explicitly activate via Raft.
        if (response.status == 409 and std.mem.eql(u8, response.body, "SqlRangeTrackingRequired")) {
            self.poisoned = true;
            return error.SqlRangeTrackingRequired;
        }
        if (response.status != 200 or !std.mem.eql(u8, response.header(metadata.catalog_route_fence_ack_header) orelse "", metadata.catalog_route_fence_ack_value)) {
            self.poisoned = true;
            return error.RetainedReadRestartRequired;
        }
        return response;
    }

    fn validate(ptr: *anyopaque) !void {
        const self: *Client = @ptrCast(@alignCast(ptr));
        var response = try self.call(.{ .operation = .validate, .schema_version = self.schema_version, .token = self.token });
        defer response.deinit(self.alloc);
    }

    fn captureSnapshot(ptr: *anyopaque, alloc: std.mem.Allocator) !@import("../storage/statement_read_fence.zig").Snapshot {
        const self: *Client = @ptrCast(@alignCast(ptr));
        var response = try self.call(.{ .operation = .snapshot, .schema_version = self.schema_version, .token = self.token, .connection = self.connection, .lease_ms = rpc.max_lease_ms });
        defer response.deinit(self.alloc);
        var parsed = try std.json.parseFromSlice(rpc.Response, alloc, response.body, .{});
        defer parsed.deinit();
        const token = parsed.value.token orelse return error.InvalidRetainedReadResponse;
        errdefer {
            var cleanup: ?http.HttpResponse = self.call(.{ .operation = .close, .schema_version = self.schema_version, .token = token }) catch null;
            if (cleanup) |*value| value.deinit(self.alloc);
        }
        const snapshot = try alloc.create(Client);
        errdefer alloc.destroy(snapshot);
        const uri = try alloc.dupe(u8, self.uri);
        errdefer alloc.free(uri);
        const route = try alloc.dupe(u8, self.route);
        errdefer alloc.free(route);
        snapshot.* = .{ .alloc = alloc, .executor = self.executor, .uri = uri, .route = route, .schema_version = self.schema_version, .deadline_ns = self.deadline_ns, .token = token, .connection = self.connection };
        return .{ .ptr = snapshot, .vtable = &.{ .open = openSnapshot, .release = close } };
    }

    fn open(ptr: *anyopaque, alloc: std.mem.Allocator, from: []const u8, to: []const u8, opts: types.ScanOptions) !reads.RelationalReadView {
        const self: *Client = @ptrCast(@alignCast(ptr));
        return self.openKind(alloc, from, to, opts, false);
    }

    fn openSnapshot(ptr: *anyopaque, alloc: std.mem.Allocator, from: []const u8, to: []const u8, opts: types.ScanOptions) !reads.RelationalReadView {
        const self: *Client = @ptrCast(@alignCast(ptr));
        return self.openKind(alloc, from, to, opts, true);
    }

    fn openKind(self: *Client, alloc: std.mem.Allocator, from: []const u8, to: []const u8, opts: types.ScanOptions, delayed: bool) !reads.RelationalReadView {
        var parsed_query: ?std.json.Parsed(types.RelationalRowQuery) = null;
        defer if (parsed_query) |*parsed| parsed.deinit();
        const query = opts.relational_query orelse blk: {
            parsed_query = try std.json.parseFromSlice(types.RelationalRowQuery, alloc, opts.relational_query_json, .{ .parse_numbers = false });
            break :blk parsed_query.?.value;
        };
        // Preallocate the local owner before creating the remote capability.
        const cursor = try alloc.create(Client);
        errdefer alloc.destroy(cursor);
        const uri = try alloc.dupe(u8, self.uri);
        errdefer alloc.free(uri);
        const route = try alloc.dupe(u8, self.route);
        errdefer alloc.free(route);
        errdefer self.cancelUnknown();
        var response = try self.call(.{ .operation = if (delayed) .open_snapshot else .open, .schema_version = self.schema_version, .token = self.token, .connection = self.connection, .lease_ms = rpc.max_lease_ms, .from = from, .to = to, .query = query, .filter_query_json = opts.filter_query_json, .inclusive_from = opts.inclusive_from, .exclusive_to = opts.exclusive_to, .sql_document_preimage = opts.sql_document_preimage, .include_content_hashes = opts.include_content_hashes, .include_range_proofs = opts.include_range_proofs, .row_policy_principal_proof = opts.row_policy_principal_proof, .row_policy_database = opts.row_policy_database });
        defer response.deinit(self.alloc);
        var parsed = try std.json.parseFromSlice(rpc.Response, alloc, response.body, .{});
        defer parsed.deinit();
        cursor.* = .{ .alloc = alloc, .executor = self.executor, .uri = uri, .route = route, .schema_version = self.schema_version, .deadline_ns = self.deadline_ns, .token = parsed.value.token orelse return error.InvalidRetainedReadResponse, .connection = self.connection };
        return .{ .ptr = cursor, .vtable = &.{ .next = next, .close = close, .normalize = normalize, .range_proofs = rangeProofs } };
    }

    fn next(ptr: *anyopaque, alloc: std.mem.Allocator, limit: u32) !reads.RelationalReadView.Page {
        const self: *Client = @ptrCast(@alignCast(ptr));
        errdefer self.poisoned = true;
        var response = try self.call(.{ .operation = .next, .schema_version = self.schema_version, .token = self.token, .sequence = self.sequence, .limit = limit });
        defer response.deinit(self.alloc);
        const page = try rpc.decodePage(alloc, response.body, self.sequence, self.schema_version, limit);
        self.sequence += 1;
        return page;
    }

    fn close(ptr: *anyopaque) void {
        const self: *Client = @ptrCast(@alignCast(ptr));
        if (self.call(.{ .operation = .close, .schema_version = self.schema_version, .token = self.token })) |value| {
            var response = value;
            response.deinit(self.alloc);
        } else |_| {}
        self.destroy();
    }

    fn rangeProofs(ptr: *anyopaque, alloc: std.mem.Allocator) ![]@import("../storage/range_protection.zig").Proof {
        const self: *Client = @ptrCast(@alignCast(ptr));
        var response = try self.call(.{ .operation = .range_proofs, .schema_version = self.schema_version, .token = self.token });
        defer response.deinit(self.alloc);
        var parsed = try std.json.parseFromSlice(rpc.Response, self.alloc, response.body, .{});
        defer parsed.deinit();
        const proofs = parsed.value.range_proofs;
        if (proofs.len == 0 or proofs.len > @import("range_read_guards.zig").max_proofs) return error.InvalidRetainedReadResponse;
        for (proofs, 0..) |proof, i| {
            const tracking = @import("../storage/range_protection.zig");
            tracking.validateProof(proof) catch return error.InvalidRetainedReadResponse;
            if (i != 0 and !tracking.proofLess(proofs[i - 1], proof)) return error.InvalidRetainedReadResponse;
        }
        return alloc.dupe(@import("../storage/range_protection.zig").Proof, proofs);
    }

    fn normalize(ptr: *anyopaque, alloc: std.mem.Allocator, writes: []const types.BatchWrite) ![]types.BatchWrite {
        const self: *Client = @ptrCast(@alignCast(ptr));
        var response = try self.call(.{ .operation = .normalize, .schema_version = self.schema_version, .token = self.token, .writes = writes });
        defer response.deinit(self.alloc);
        var parsed = try std.json.parseFromSlice(rpc.Response, self.alloc, response.body, .{});
        defer parsed.deinit();
        if (parsed.value.writes.len != writes.len) return error.InvalidRetainedReadResponse;
        const result = try alloc.alloc(types.BatchWrite, writes.len);
        var initialized: usize = 0;
        errdefer {
            for (result[0..initialized]) |row| {
                alloc.free(row.key);
                alloc.free(row.value);
                for (row.json_null_fields) |field| alloc.free(field);
                if (row.json_null_fields.len != 0) alloc.free(row.json_null_fields);
            }
            alloc.free(result);
        }
        for (writes, parsed.value.writes, result) |input, row, *out| {
            if (!std.mem.eql(u8, input.key, row.key)) return error.InvalidRetainedReadResponse;
            const key = try alloc.dupe(u8, row.key);
            errdefer alloc.free(key);
            const value = try alloc.dupe(u8, row.value);
            errdefer alloc.free(value);
            out.* = .{ .key = key, .value = value, .json_null_fields = try types.cloneJsonNullFields(alloc, row.json_null_fields) };
            initialized += 1;
        }
        return result;
    }

    fn cancelUnknown(self: *Client) void {
        if (self.call(.{ .operation = .cancel, .schema_version = self.schema_version, .connection = self.connection })) |value| {
            var response = value;
            response.deinit(self.alloc);
        } else |_| {}
    }
};

pub const consumer_tests = consumerTests();
comptime {
    if (@import("builtin").is_test) _ = consumer_tests;
}
fn consumerTests() type {
    if (!@import("builtin").is_test) return struct {};
    const root = @import("antfly_source_root");
    if (@hasDecl(root, "implementation_tests_only") and root.implementation_tests_only) return struct {};
    return struct {
        test "retained read client never replays an ambiguous page request" {
            const Fixture = struct {
                calls: usize = 0,
                fn execute(raw: *anyopaque, _: std.mem.Allocator, _: http.HttpRequest) !http.HttpResponse {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    self.calls += 1;
                    return error.ConnectionResetByPeer;
                }
            };
            var fixture: Fixture = .{};
            const alloc = std.testing.allocator;
            const client = try Client.create(alloc, .{ .ptr = &fixture, .clock_io = @import("../runtime_io_abi.zig").Borrow.init(&std.testing.io), .vtable = &.{ .execute = Fixture.execute } }, "http://peer", "a/b", .{ .metadata_group_id = 1, .catalog_revision = 1, .table_id = 2, .topology_epoch = 4, .route = .{ .group_id = 3, .range_id = 3, .identity_namespace = .{ .table_id = 2, .shard_id = 3, .range_id = 3 } } }, 5, null);
            defer client.destroy();
            client.token = .{ .incarnation = 1, .sequence = 1, .slot = 0 };
            try std.testing.expect(std.mem.endsWith(u8, client.uri, "/tables/a%2Fb/retained-read"));
            try std.testing.expectError(error.ConnectionResetByPeer, Client.next(client, alloc, 1));
            try std.testing.expectError(error.RetainedReadRestartRequired, Client.next(client, alloc, 1));
            try std.testing.expectEqual(@as(usize, 1), fixture.calls);
        }

        test "retained read client round trips owned snapshots and exact typed pages" {
            const Fixture = struct {
                registry_owner: *registry.Registry,
                capture_closed: usize = 0,
                cursor_closed: usize = 0,
                advances: usize = 0,
                fail_delivery: bool = false,
                lose_capture: bool = false,
                const scope = registry.Scope{ .principal = @splat(1), .authorization_revision = 1, .table_id = 2, .group_id = 3, .topology_revision = 4, .schema_version = 5 };
                const route = metadata.CatalogRouteFence{ .metadata_group_id = 1, .catalog_revision = 1, .table_id = 2, .topology_epoch = 4, .route = .{ .group_id = 3, .range_id = 3, .identity_namespace = .{ .table_id = 2, .shard_id = 3, .range_id = 3 } } };
                fn capture(raw: *anyopaque, _: std.mem.Allocator, _: metadata.CatalogRouteFence, _: u64, _: []const u8, _: types.ScanOptions, _: @import("../raft/read_gate.zig").ReadConsistency) !?reads.StatementReadFence {
                    return .{ .ptr = raw, .vtable = &.{ .validate = validate, .open = open, .release = release } };
                }
                fn validate(_: *anyopaque) !void {}
                fn release(raw: *anyopaque) void {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    self.capture_closed += 1;
                }
                fn open(raw: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, opts: types.ScanOptions) !reads.RelationalReadView {
                    try std.testing.expectEqual(@as(?u32, 5), opts.relational_query.?.schema_version);
                    try std.testing.expect(opts.sql_document_preimage and opts.include_content_hashes);
                    try std.testing.expect(opts.include_range_proofs);
                    return .{ .ptr = raw, .vtable = &.{ .next = next, .close = close, .normalize = normalizeWrites, .range_proofs = proofRows } };
                }
                fn proofRows(_: *anyopaque, alloc: std.mem.Allocator) ![]@import("../storage/range_protection.zig").Proof {
                    return alloc.dupe(@import("../storage/range_protection.zig").Proof, &.{ .{ .bucket = 0, .generation = null }, .{ .bucket = 256, .generation = std.math.maxInt(u64) } });
                }
                fn normalizeWrites(_: *anyopaque, alloc: std.mem.Allocator, writes: []const types.BatchWrite) ![]types.BatchWrite {
                    const result = try alloc.alloc(types.BatchWrite, writes.len);
                    // Fixture uses the owner's temporary arena; production
                    // providers must likewise return owned normalized bytes.
                    for (writes, result) |row, *out| out.* = .{ .key = try alloc.dupe(u8, row.key), .value = try alloc.dupe(u8, row.value), .json_null_fields = try types.cloneJsonNullFields(alloc, row.json_null_fields) };
                    return result;
                }
                fn close(raw: *anyopaque) void {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    self.cursor_closed += 1;
                }
                fn next(raw: *anyopaque, alloc: std.mem.Allocator, _: u32) !reads.RelationalReadView.Page {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    self.advances += 1;
                    var arena = std.heap.ArenaAllocator.init(alloc);
                    errdefer arena.deinit();
                    const rows = try arena.allocator().alloc(reads.RelationalReadView.Row, 1);
                    rows[0] = .{ .id = "a", .version = 7, .schema_version = 5, .value = try std.json.parseFromSliceLeaky(std.json.Value, arena.allocator(), "{\"n\":9007199254740993,\"j\":null}", .{ .parse_numbers = false }), .sql_nulls = &.{ false, false }, .expected_content_digest = @splat(17), .document = try std.json.parseFromSliceLeaky(std.json.Value, arena.allocator(), "{\"n\":9007199254740993,\"extra\":{\"retained\":true}}", .{ .parse_numbers = false }) };
                    return .{ .arena = arena, .rows = rows, .after = null };
                }
                fn execute(raw: *anyopaque, alloc: std.mem.Allocator, request: http.HttpRequest) !http.HttpResponse {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    var parsed = try std.json.parseFromSlice(rpc.Request, alloc, request.body, .{ .parse_numbers = false });
                    defer parsed.deinit();
                    const body = try rpc.execute(alloc, .{ .registry = self.registry_owner, .source = .{ .ptr = self, .route_fence = route, .remote_statement_fences_safe = true, .vtable = &.{ .lookup = undefined, .scan = undefined, .query = undefined, .try_statement_read_fence_group_local_routed = capture } } }, scope, "rows", parsed.value);
                    errdefer alloc.free(body);
                    if ((self.fail_delivery and parsed.value.operation == .next) or (self.lose_capture and parsed.value.operation == .capture)) return error.ConnectionResetByPeer;
                    const headers = try alloc.alloc(http.Header, 1);
                    errdefer alloc.free(headers);
                    const name = try alloc.dupe(u8, metadata.catalog_route_fence_ack_header);
                    errdefer alloc.free(name);
                    headers[0] = .{ .name = name, .value = try alloc.dupe(u8, metadata.catalog_route_fence_ack_value) };
                    return .{ .status = 200, .body = body, .headers = headers };
                }
            };
            const alloc = std.testing.allocator;
            var lifetime = try registry.Registry.init(alloc, std.testing.io, 1, 8, 8, 30 * std.time.ns_per_s);
            defer lifetime.deinit();
            var fixture: Fixture = .{ .registry_owner = &lifetime };
            const executor = http.RequestExecutor{ .ptr = &fixture, .clock_io = @import("../runtime_io_abi.zig").Borrow.init(&std.testing.io), .vtable = &.{ .execute = Fixture.execute } };
            const fence = (try Client.capture(alloc, executor, "http://peer", "rows", Fixture.route, 5, null)).?;
            const view = try fence.open(alloc, "", "", .{ .relational_query = .{ .fields = &.{ "n", "j" } }, .sql_document_preimage = true, .include_content_hashes = true, .include_range_proofs = true });
            try fence.validate();
            fence.deinit();
            try std.testing.expectEqual(@as(usize, 1), fixture.capture_closed);
            const proofs = try view.rangeProofs(alloc);
            defer alloc.free(proofs);
            try std.testing.expectEqual(@as(usize, 2), proofs.len);
            try std.testing.expectEqual(null, proofs[0].generation);
            try std.testing.expectEqual(@as(?u64, std.math.maxInt(u64)), proofs[1].generation);
            var page = try view.next(alloc, 1);
            defer page.deinit();
            try std.testing.expectEqualStrings("9007199254740993", page.rows[0].value.object.get("n").?.number_string);
            try std.testing.expect(!page.rows[0].sql_nulls.?[1]);
            try std.testing.expectEqual(@as([32]u8, @splat(17)), page.rows[0].expected_content_digest.?);
            try std.testing.expectEqualStrings("9007199254740993", page.rows[0].document.?.object.get("n").?.number_string);
            try std.testing.expect(page.rows[0].document.?.object.get("extra").?.object.get("retained").?.bool);
            var normalized_arena = std.heap.ArenaAllocator.init(alloc);
            defer normalized_arena.deinit();
            const normalized = try view.normalize(normalized_arena.allocator(), &.{.{ .key = "a", .value = "{\"j\":null}", .json_null_fields = &.{"j"} }});
            try std.testing.expectEqualStrings("a", normalized[0].key);
            try std.testing.expectEqualStrings("{\"j\":null}", normalized[0].value);
            try std.testing.expectEqualStrings("j", normalized[0].json_null_fields[0]);
            fixture.fail_delivery = true;
            try std.testing.expectError(error.ConnectionResetByPeer, view.next(alloc, 1));
            try std.testing.expectError(error.RetainedReadRestartRequired, view.next(alloc, 1));
            try std.testing.expectEqual(@as(usize, 2), fixture.advances);
            view.deinit();
            try std.testing.expectEqual(@as(usize, 1), fixture.cursor_closed);
            // Capture completed on the owner, but its token never reached the
            // caller. Scoped connection cancellation promptly releases it.
            fixture.lose_capture = true;
            try std.testing.expectError(error.ConnectionResetByPeer, Client.capture(alloc, executor, "http://peer", "rows", Fixture.route, 5, null));
            try std.testing.expectEqual(@as(usize, 2), fixture.capture_closed);
        }
    };
}
