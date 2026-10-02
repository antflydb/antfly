// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Mounted self-FK publication and recovery proof for the public SQL path.
const std = @import("std");
const platform = @import("antfly_platform");
const metadata_runtime = @import("../metadata/runtime.zig");
const data_runtime = @import("../data/runtime.zig");
const raft = @import("../raft/mod.zig");
const executor_mod = @import("../raft/transport/std_http_executor.zig");
const http = @import("../raft/transport/http_common.zig");
const http_server = @import("http_server.zig");
const http_client = @import("http_client.zig");
const table_catalog = @import("table_catalog.zig");
const table_router = @import("table_router.zig");
const publication = @import("../metadata/fk_generation_publication.zig");
const test_helpers = @import("../public_test_helpers.zig");
const usermgr = @import("../usermgr/mod.zig");
const casbin = @import("antfly_casbin");

fn deinitDriverIfLive(driver: *raft.ManagedProgressDriver, live: *bool) void {
    if (!live.*) return;
    driver.deinit();
    live.* = false;
}

// A real second/third hosted data Raft voter, not another handle to the
// first owner's process. Keep the server address stable while its drivers run.
pub const DataPeer = struct {
    replica_root: []const u8,
    catalog: []const u8,
    server: data_runtime.DataServer,
    raft_driver: raft.ManagedProgressDriver,
    control_driver: raft.ManagedProgressDriver,
    raft_live: bool = false,
    control_live: bool = false,
    paused_fk: ?*http_server.ApiHttpServer = null,

    pub fn create(alloc: std.mem.Allocator, process_alloc: std.mem.Allocator, io: std.Io, root: []const u8, metadata_uri: []const u8, node_id: u64, trusted_secret: []const u8, internal_secret: []const u8, issuer: []const u8) !*DataPeer {
        const peer = try alloc.create(DataPeer);
        errdefer alloc.destroy(peer);
        peer.raft_live = false;
        peer.control_live = false;
        peer.paused_fk = null;
        const replica_root = try std.fmt.allocPrint(alloc, "{s}/data-{d}", .{ root, node_id });
        errdefer alloc.free(replica_root);
        const catalog = try std.fmt.allocPrint(alloc, "{s}/data-catalog-{d}", .{ root, node_id });
        errdefer alloc.free(catalog);
        peer.replica_root = replica_root;
        peer.catalog = catalog;
        peer.server = try data_runtime.DataServer.initFromMetadataApiUrl(process_alloc, .{
            .replica_root_dir = replica_root,
            .replica_catalog_path = catalog,
            .store_registration = .{ .node_id = node_id, .store_id = node_id, .role = "data" },
            .api_server_cfg = .{ .deployment_mode = .distributed, .trusted_principal_secret = trusted_secret, .trusted_principal_issuer = issuer, .internal_service_secret = internal_secret, .internal_service_issuer = issuer, .internal_service_auth_capability = "v1; mode=enforce" },
        }, metadata_uri);
        errdefer peer.server.deinit();
        try peer.server.start();
        try awaitStoreRegistration(io, &peer.server);
        peer.raft_driver = raft.ManagedProgressDriver.init(io, .{ .ptr = &peer.server, .run_once = dataRaft }, std.time.ns_per_ms);
        try peer.raft_driver.start();
        peer.raft_live = true;
        errdefer if (peer.raft_live) peer.raft_driver.deinit();
        peer.control_driver = raft.ManagedProgressDriver.init(io, .{ .ptr = &peer.server, .run_once = dataControl }, std.time.ns_per_ms);
        try peer.control_driver.start();
        peer.control_live = true;
        return peer;
    }

    pub fn destroy(peer: *DataPeer, alloc: std.mem.Allocator) void {
        if (peer.paused_fk) |server| http_server.ApiHttpServer.FkGenerationPublicationTestDriver.resumeBackground(server);
        if (peer.control_live) peer.control_driver.deinit();
        if (peer.raft_live) peer.raft_driver.deinit();
        peer.server.deinit();
        alloc.free(peer.catalog);
        alloc.free(peer.replica_root);
        alloc.destroy(peer);
    }
};

fn metadataRaft(ptr: *anyopaque) !void {
    const server: *metadata_runtime.Server = @ptrCast(@alignCast(ptr));
    try server.runRaftRoundOnly();
}
fn metadataControl(ptr: *anyopaque) !void {
    const server: *metadata_runtime.Server = @ptrCast(@alignCast(ptr));
    try server.runControlRoundOnly();
    try server.runCdcRound();
}
fn dataRaft(ptr: *anyopaque) !void {
    const server: *data_runtime.DataServer = @ptrCast(@alignCast(ptr));
    try server.runRaftRoundOnly();
}
fn dataControl(ptr: *anyopaque) !void {
    const server: *data_runtime.DataServer = @ptrCast(@alignCast(ptr));
    try server.runControlRoundOnly();
}

fn request(alloc: std.mem.Allocator, transport: http.RequestExecutor, headers: []const http.RequestHeader, base: []const u8, suffix: []const u8, method: http.Method, body: ?[]const u8) !http.HttpResponse {
    const uri = try std.fmt.allocPrint(alloc, "{s}{s}", .{ base, suffix });
    defer alloc.free(uri);
    return transport.execute(alloc, .{ .method = method, .uri = uri, .headers = headers, .content_type = if (body == null) null else "application/json", .body = body orelse "", .timeout_ms = 3_000 });
}
fn sql(alloc: std.mem.Allocator, transport: http.RequestExecutor, headers: []const http.RequestHeader, base: []const u8, statement: []const u8) !http.HttpResponse {
    const body = try std.json.Stringify.valueAlloc(alloc, .{ .statement = statement }, .{});
    defer alloc.free(body);
    return request(alloc, transport, headers, base, "/db/v1/sql", .POST, body);
}
fn retryablePrecommitSqlRead(alloc: std.mem.Allocator, response: http.HttpResponse) bool {
    if (response.status != 503 or response.body.len > 4096) return false;
    const Diagnostic = struct {
        code: []const u8,
        message: []const u8,
        retryable: bool,
        transaction_status: ?[]const u8 = null,
        transaction_id: ?[]const u8 = null,
    };
    var parsed = std.json.parseFromSlice(Diagnostic, alloc, response.body, .{ .ignore_unknown_fields = true }) catch return false;
    defer parsed.deinit();
    const short_message = "A consistent SQL statement read is temporarily unavailable.";
    const full_message = short_message ++ " Retry the complete statement after a bounded delay.";
    return std.mem.eql(u8, parsed.value.code, "53300") and
        (std.mem.eql(u8, parsed.value.message, short_message) or std.mem.eql(u8, parsed.value.message, full_message)) and parsed.value.retryable and
        parsed.value.transaction_status != null and std.mem.eql(u8, parsed.value.transaction_status.?, "idle") and
        parsed.value.transaction_id == null;
}
fn executePreparedAfterReadReady(alloc: std.mem.Allocator, io: std.Io, transport: http.RequestExecutor, headers: []const http.RequestHeader, base: []const u8, suffix: []const u8) !http.HttpResponse {
    const deadline = platform.time.monotonicNs() +| 30 * std.time.ns_per_s;
    while (true) {
        var response = try request(alloc, transport, headers, base, suffix, .POST, "{}");
        if (!retryablePrecommitSqlRead(alloc, response) or platform.time.monotonicNs() >= deadline) return response;
        response.deinit(alloc);
        try io.sleep(.fromMilliseconds(50), .awake);
    }
}
fn pgwireRetryableReadUnavailable(payload: []const u8) bool {
    return std.mem.indexOf(u8, payload, "C53300\x00MA consistent SQL statement read is temporarily unavailable. Retry the complete statement after a bounded delay.\x00") != null and
        std.mem.indexOf(u8, payload, "D{\"retryable\":true}") != null and
        std.mem.indexOf(u8, payload, "transaction_id") == null;
}
fn pgwirePrecommitReadUnavailable(payload: []const u8, prepared_count: usize, mutation_count: usize, read_count: usize) bool {
    return prepared_count == 1 and mutation_count == 0 and read_count == 0 and pgwireRetryableReadUnavailable(payload);
}
fn pgwireCommittedReadUnavailable(payload: []const u8, prepared_count: usize, mutation_count: usize, read_count: usize) bool {
    return prepared_count == 1 and mutation_count == 1 and read_count == 0 and pgwireRetryableReadUnavailable(payload);
}
fn pgwireFrame(out: *std.Io.Writer, tag: u8, payload: []const u8) !void {
    try out.writeByte(tag);
    try out.writeInt(u32, @intCast(payload.len + 4), .big);
    try out.writeAll(payload);
}
fn pgwirePreparedInput(alloc: std.mem.Allocator, out: *std.Io.Writer, original: []const u8, execute: []const u8, select: ?[]const u8) !void {
    const startup = "user\x00prepared_writer\x00database\x00default\x00\x00";
    try out.writeInt(u32, @intCast(startup.len + 8), .big);
    try out.writeInt(u32, 196608, .big);
    try out.writeAll(startup);
    try pgwireFrame(out, 'p', "secret\x00");
    const prepare_command = try std.fmt.allocPrint(alloc, "{s}\x00", .{original});
    defer alloc.free(prepare_command);
    try pgwireFrame(out, 'Q', prepare_command);
    const execute_command = try std.fmt.allocPrint(alloc, "{s}\x00", .{execute});
    defer alloc.free(execute_command);
    try pgwireFrame(out, 'Q', execute_command);
    if (select) |query| {
        const select_command = try std.fmt.allocPrint(alloc, "{s}\x00", .{query});
        defer alloc.free(select_command);
        try pgwireFrame(out, 'Q', select_command);
    }
    try pgwireFrame(out, 'X', "");
}
test "hosted CTE MERGE retries only proven precommit read unavailability" {
    const safe = http.HttpResponse{ .status = 503, .body = @constCast("{\"code\":\"53300\",\"message\":\"A consistent SQL statement read is temporarily unavailable.\",\"retryable\":true,\"transaction_status\":\"idle\"}") };
    try std.testing.expect(retryablePrecommitSqlRead(std.testing.allocator, safe));
    const hinted = http.HttpResponse{ .status = 503, .body = @constCast("{\"code\":\"53300\",\"message\":\"A consistent SQL statement read is temporarily unavailable. Retry the complete statement after a bounded delay.\",\"retryable\":true,\"transaction_status\":\"idle\"}") };
    try std.testing.expect(retryablePrecommitSqlRead(std.testing.allocator, hinted));
    const unknown = http.HttpResponse{ .status = 409, .body = @constCast("{\"code\":\"40003\",\"message\":\"outcome unknown\",\"retryable\":false,\"transaction_status\":\"failed\"}") };
    try std.testing.expect(!retryablePrecommitSqlRead(std.testing.allocator, unknown));
    const receipt = http.HttpResponse{ .status = 503, .body = @constCast("{\"code\":\"53300\",\"message\":\"A consistent SQL statement read is temporarily unavailable.\",\"retryable\":true,\"transaction_status\":\"idle\",\"transaction_id\":\"00000000000000000000000000000001\"}") };
    try std.testing.expect(!retryablePrecommitSqlRead(std.testing.allocator, receipt));
    const pgwire_safe = "SERROR\x00C53300\x00MA consistent SQL statement read is temporarily unavailable. Retry the complete statement after a bounded delay.\x00D{\"retryable\":true}\x00\x00";
    try std.testing.expect(pgwirePrecommitReadUnavailable(pgwire_safe, 1, 0, 0));
    try std.testing.expect(!pgwirePrecommitReadUnavailable(pgwire_safe, 1, 1, 0));
    try std.testing.expect(!pgwirePrecommitReadUnavailable(pgwire_safe, 1, 0, 1));
    try std.testing.expect(pgwireCommittedReadUnavailable(pgwire_safe, 1, 1, 0));
    try std.testing.expect(!pgwireCommittedReadUnavailable(pgwire_safe, 1, 0, 0));
    try std.testing.expect(!pgwireCommittedReadUnavailable("SERROR\x00C40003\x00Mwrite outcome unknown\x00", 1, 1, 0));
    try std.testing.expect(!pgwirePrecommitReadUnavailable("SERROR\x00C40003\x00Mwrite outcome unknown\x00", 1, 0, 0));
}
fn runHostedPgwirePreparedOnce(alloc: std.mem.Allocator, io: std.Io, api: *http_server.ApiHttpServer, original: []const u8, execute: []const u8, select: []const u8, completion_tag: []const u8, expected_row: []const []const u8) !enum { committed, retry_read, committed_read_unavailable } {
    var pg_adapter: @import("sql_pgwire.zig").Adapter = .{ .server = api };
    var input = std.Io.Writer.Allocating.init(alloc);
    defer input.deinit();
    try pgwirePreparedInput(alloc, &input.writer, original, execute, select);
    var reader = std.Io.Reader.fixed(input.written());
    var output = std.Io.Writer.Allocating.init(alloc);
    defer output.deinit();
    var session: @import("../pgwire/protocol.zig").Session = .{ .alloc = alloc, .io = io, .source = pg_adapter.backend(), .reader = &reader, .writer = &output.writer };
    defer session.deinit();
    try session.run();
    var frames: @import("../pgwire/protocol.zig").Cursor = .{ .bytes = output.written() };
    var prepared_count: usize = 0;
    var mutation_count: usize = 0;
    var read_count: usize = 0;
    while (frames.offset < frames.bytes.len) {
        const tag = try frames.int(u8);
        const size = try frames.int(u32);
        const payload = try frames.take(size - 4);
        if (tag == 'E') {
            if (pgwirePrecommitReadUnavailable(payload, prepared_count, mutation_count, read_count)) return .retry_read;
            if (pgwireCommittedReadUnavailable(payload, prepared_count, mutation_count, read_count)) return .committed_read_unavailable;
            std.debug.print("hosted pgwire CTE mutation error after prepared={} mutated={} reads={}: {s}\n", .{ prepared_count, mutation_count, read_count, payload });
            return error.TestUnexpectedPgwireMutationError;
        }
        if (tag == 'C' and std.mem.eql(u8, payload, "PREPARE\x00")) prepared_count += 1;
        if (tag == 'C' and std.mem.eql(u8, payload, completion_tag)) mutation_count += 1;
        if (tag != 'D') continue;
        var row: @import("../pgwire/protocol.zig").Cursor = .{ .bytes = payload };
        try std.testing.expectEqual(@as(u16, @intCast(expected_row.len)), try row.int(u16));
        for (expected_row) |expected| {
            const length = try row.int(i32);
            try std.testing.expect(length >= 0);
            try std.testing.expectEqualStrings(expected, try row.take(@intCast(length)));
        }
        read_count += 1;
    }
    try std.testing.expectEqual(@as(usize, 1), prepared_count);
    try std.testing.expectEqual(@as(usize, 1), mutation_count);
    try std.testing.expectEqual(@as(usize, 1), read_count);
    return .committed;
}
fn awaitHostedReadBack(alloc: std.mem.Allocator, io: std.Io, transport: http.RequestExecutor, headers: []const http.RequestHeader, base: []const u8, select: []const u8, expected_row: []const []const u8) !void {
    const deadline = platform.time.monotonicNs() +| 60 * std.time.ns_per_s;
    while (true) {
        var response = try sql(alloc, transport, headers, base, select);
        defer response.deinit(alloc);
        if (response.status == 200) {
            var parsed = try std.json.parseFromSlice(std.json.Value, alloc, response.body, .{});
            defer parsed.deinit();
            const rows = parsed.value.object.get("rows").?.array.items;
            try std.testing.expectEqual(@as(usize, 1), rows.len);
            try std.testing.expectEqual(expected_row.len, rows[0].array.items.len);
            for (expected_row, rows[0].array.items) |expected, actual| try std.testing.expectEqualStrings(expected, actual.string);
            return;
        }
        if (!retryablePrecommitSqlRead(alloc, response) or platform.time.monotonicNs() >= deadline) {
            std.debug.print("hosted committed CTE read-back status={d} body={s}\n", .{ response.status, response.body });
            return error.TestUnexpectedCommittedReadError;
        }
        try io.sleep(.fromMilliseconds(50), .awake);
    }
}
fn runHostedPgwireRecursiveReadOnce(alloc: std.mem.Allocator, io: std.Io, api: *http_server.ApiHttpServer, original: []const u8) !enum { complete, retry_read } {
    var pg_adapter: @import("sql_pgwire.zig").Adapter = .{ .server = api };
    var input = std.Io.Writer.Allocating.init(alloc);
    defer input.deinit();
    try pgwirePreparedInput(alloc, &input.writer, original, "EXECUTE recursive_usage_read_plan", null);
    var reader = std.Io.Reader.fixed(input.written());
    var output = std.Io.Writer.Allocating.init(alloc);
    defer output.deinit();
    var session: @import("../pgwire/protocol.zig").Session = .{ .alloc = alloc, .io = io, .source = pg_adapter.backend(), .reader = &reader, .writer = &output.writer };
    defer session.deinit();
    try session.run();
    var frames: @import("../pgwire/protocol.zig").Cursor = .{ .bytes = output.written() };
    var prepared_count: usize = 0;
    var select_count: usize = 0;
    var parent_count: usize = 0;
    var child_count: usize = 0;
    while (frames.offset < frames.bytes.len) {
        const tag = try frames.int(u8);
        const size = try frames.int(u32);
        const payload = try frames.take(size - 4);
        if (tag == 'E') {
            if (pgwirePrecommitReadUnavailable(payload, prepared_count, select_count, parent_count + child_count)) return .retry_read;
            std.debug.print("hosted recursive pgwire read error after prepared={} rows={}: {s}\n", .{ prepared_count, parent_count + child_count, payload });
            return error.TestUnexpectedPgwireReadError;
        }
        if (tag == 'C' and std.mem.eql(u8, payload, "PREPARE\x00")) prepared_count += 1;
        if (tag == 'C' and std.mem.eql(u8, payload, "SELECT 3\x00")) select_count += 1;
        if (tag != 'D') continue;
        var row: @import("../pgwire/protocol.zig").Cursor = .{ .bytes = payload };
        try std.testing.expectEqual(@as(u16, 1), try row.int(u16));
        const length = try row.int(i32);
        try std.testing.expect(length >= 0);
        const id = try row.take(@intCast(length));
        if (std.mem.eql(u8, id, "prepared_id")) {
            parent_count += 1;
        } else if (std.mem.eql(u8, id, "child_id")) {
            child_count += 1;
        } else return error.TestUnexpectedPgwireRow;
    }
    try std.testing.expectEqual(@as(usize, 1), prepared_count);
    try std.testing.expectEqual(@as(usize, 1), select_count);
    try std.testing.expectEqual(@as(usize, 1), parent_count);
    try std.testing.expectEqual(@as(usize, 2), child_count);
    return .complete;
}
fn table(alloc: std.mem.Allocator, transport: http.RequestExecutor, headers: []const http.RequestHeader, base: []const u8) !http.HttpResponse {
    return tableNamed(alloc, transport, headers, base, "nodes");
}
fn tableNamed(alloc: std.mem.Allocator, transport: http.RequestExecutor, headers: []const http.RequestHeader, base: []const u8, name: []const u8) !http.HttpResponse {
    const suffix = try std.fmt.allocPrint(alloc, "/db/v1/tables/{s}", .{name});
    defer alloc.free(suffix);
    return request(alloc, transport, headers, base, suffix, .GET, null);
}
fn awaitConstraintCoverage(alloc: std.mem.Allocator, io: std.Io, transport: http.RequestExecutor, headers: []const http.RequestHeader, base: []const u8, drivers: []const *const raft.ManagedProgressDriver) !void {
    const deadline = platform.time.monotonicNs() +| 30 * std.time.ns_per_s;
    var last_status: u16 = 0;
    var logged_unavailable = false;
    while (platform.time.monotonicNs() < deadline) {
        for (drivers) |driver| try driver.checkFailure();
        var response = request(alloc, transport, headers, base, "/db/v1/tables/nodes/constraints/status", .GET, null) catch |err| switch (err) {
            error.Timeout, error.ConnectionRefused, error.ConnectionResetByPeer => {
                try io.sleep(.fromMilliseconds(100), .awake);
                continue;
            },
            else => return err,
        };
        defer response.deinit(alloc);
        last_status = response.status;
        if (response.status == 200) {
            var parsed = try std.json.parseFromSlice(struct { state: []const u8, ranges: []const std.json.Value }, alloc, response.body, .{ .ignore_unknown_fields = true });
            defer parsed.deinit();
            if (parsed.value.ranges.len > 0 and std.mem.eql(u8, parsed.value.state, "enforced")) return;
        } else if (response.status != 409 and response.status != 503 and response.status != 504) {
            std.debug.print("self-FK constraint coverage status={d} body={s}\n", .{ response.status, response.body });
            return error.ConstraintCoverageUnavailable;
        } else if (!logged_unavailable and response.status == 503) {
            logged_unavailable = true;
            std.debug.print("self-FK constraint coverage initially unavailable body={s}\n", .{response.body});
        }
        try io.sleep(.fromMilliseconds(100), .awake);
    }
    std.debug.print("self-FK constraint coverage timeout status={d}\n", .{last_status});
    return error.ConstraintCoverageUnavailable;
}
fn batchOnce(alloc: std.mem.Allocator, transport: http.RequestExecutor, headers: []const http.RequestHeader, base: []const u8, body: []const u8) !http.HttpResponse {
    return batchOnceWithTimeout(alloc, transport, headers, base, body, 15_000);
}
fn batchOnceWithTimeout(alloc: std.mem.Allocator, transport: http.RequestExecutor, headers: []const http.RequestHeader, base: []const u8, body: []const u8, timeout_ms: u32) !http.HttpResponse {
    const uri = try std.fmt.allocPrint(alloc, "{s}/db/v1/tables/nodes/batch", .{base});
    defer alloc.free(uri);
    // A write with a lost reply can have committed. The fixture never retries
    // generic 503 or transport failures without an exact durable receipt.
    return transport.execute(alloc, .{ .method = .POST, .uri = uri, .headers = headers, .content_type = "application/json", .body = body, .timeout_ms = timeout_ms });
}
fn definitelyAbortedBeforeCommit(alloc: std.mem.Allocator, response: http.HttpResponse) bool {
    if (response.status != 503 or response.body.len > 1024) return false;
    var parsed = std.json.parseFromSlice(struct { code: []const u8, retryable: bool }, alloc, response.body, .{ .ignore_unknown_fields = true }) catch return false;
    defer parsed.deinit();
    return parsed.value.retryable and std.mem.eql(u8, parsed.value.code, "transaction_precommit_aborted");
}
fn batchAfterDefiniteAbort(alloc: std.mem.Allocator, io: std.Io, transport: http.RequestExecutor, headers: []const http.RequestHeader, base: []const u8, body: []const u8) !http.HttpResponse {
    const deadline = platform.time.monotonicNs() +| 20 * std.time.ns_per_s;
    for (0..8) |attempt| {
        const remaining_ms = @max(@as(u64, 1), (deadline -| platform.time.monotonicNs()) / std.time.ns_per_ms);
        var response = try batchOnceWithTimeout(alloc, transport, headers, base, body, @intCast(@min(remaining_ms, 15_000)));
        if (!definitelyAbortedBeforeCommit(alloc, response) or attempt == 7 or platform.time.monotonicNs() >= deadline) return response;
        response.deinit(alloc);
        try io.sleep(.fromMilliseconds(@min(@as(u64, 50), (deadline -| platform.time.monotonicNs()) / std.time.ns_per_ms)), .awake);
    }
    unreachable;
}

test "self-FK retries only a proven durable precommit abort" {
    const Mode = enum { proven_abort, generic_unavailable, unknown_outcome, malformed, transport_failure };
    const Fake = struct {
        mode: Mode,
        calls: usize = 0,

        fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, _: http.HttpRequest) !http.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            if (self.mode == .transport_failure) return error.ConnectionResetByPeer;
            const response: struct { status: u16, body: []const u8 } = switch (self.mode) {
                .proven_abort => if (self.calls == 1)
                    .{ .status = @as(u16, 503), .body = "{\"code\":\"transaction_precommit_aborted\",\"retryable\":true}" }
                else
                    .{ .status = @as(u16, 201), .body = "{}" },
                .generic_unavailable => .{ .status = @as(u16, 503), .body = "write unavailable" },
                .unknown_outcome => .{ .status = @as(u16, 409), .body = "write outcome unknown" },
                .malformed => .{ .status = @as(u16, 503), .body = "{\"code\":\"transaction_precommit_aborted\",\"retryable\":false}" },
                .transport_failure => unreachable,
            };
            return .{ .status = response.status, .body = try alloc.dupe(u8, response.body) };
        }
    };
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    inline for (std.meta.tags(Mode)) |mode| {
        var fake: Fake = .{ .mode = mode };
        const executor: http.RequestExecutor = .{ .ptr = &fake, .vtable = &.{ .execute = Fake.execute } };
        const result = batchAfterDefiniteAbort(std.testing.allocator, io_impl.io(), executor, &.{}, "http://owner.invalid", "{}");
        if (mode == .transport_failure) {
            try std.testing.expectError(error.ConnectionResetByPeer, result);
        } else {
            var response = try result;
            defer response.deinit(std.testing.allocator);
            try std.testing.expectEqual(@as(u16, if (mode == .proven_abort) 201 else if (mode == .unknown_outcome) 409 else 503), response.status);
        }
        try std.testing.expectEqual(@as(usize, if (mode == .proven_abort) 2 else 1), fake.calls);
    }
}
fn awaitTable(alloc: std.mem.Allocator, io: std.Io, transport: http.RequestExecutor, headers: []const http.RequestHeader, base: []const u8) !u64 {
    return awaitTableNamed(alloc, io, transport, headers, base, "nodes");
}
fn awaitTableNamed(alloc: std.mem.Allocator, io: std.Io, transport: http.RequestExecutor, headers: []const http.RequestHeader, base: []const u8, name: []const u8) !u64 {
    for (0..600) |_| {
        var response = try tableNamed(alloc, transport, headers, base, name);
        defer response.deinit(alloc);
        if (response.status == 200) {
            var parsed = try std.json.parseFromSlice(std.json.Value, alloc, response.body, .{});
            defer parsed.deinit();
            const value = parsed.value.object.get("table_id") orelse return error.UnexpectedTableResponse;
            return std.fmt.parseUnsigned(u64, value.string, 10);
        }
        try io.sleep(.fromMilliseconds(10), .awake);
    }
    return error.TablePlacementTimeout;
}
fn awaitStoreRegistration(io: std.Io, data: *data_runtime.DataServer) !void {
    // registerNodeIfConfigured verifies the exact store record in a fresh
    // metadata snapshot. A newly restarted store may need another metadata
    // control round before that record is visible.
    const deadline = platform.time.monotonicNs() +| 10 * std.time.ns_per_s;
    while (platform.time.monotonicNs() < deadline) {
        data.registerNodeIfConfigured() catch |err| switch (err) {
            error.StoreRegistrationNotVisible => {
                try io.sleep(.fromMilliseconds(10), .awake);
                continue;
            },
            else => return err,
        };
        return;
    }
    return error.StoreRegistrationNotVisible;
}
fn hasSelfFk(alloc: std.mem.Allocator, response: http.HttpResponse) !bool {
    if (response.status != 200) return error.UnexpectedTableResponse;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, response.body, .{});
    defer parsed.deinit();
    const schema = parsed.value.object.get("schema") orelse return error.UnexpectedTableResponse;
    const keys = schema.object.get("foreign_keys") orelse return false;
    return keys == .array and keys.array.items.len != 0;
}
fn awaitPublication(alloc: std.mem.Allocator, io: std.Io, metadata: *metadata_runtime.Server, table_id: u64) !void {
    const source = http_server.StatusSource.fromMetadataHttpService(metadata.server.svc);
    const deadline = platform.time.monotonicNs() +| 45 * std.time.ns_per_s;
    while (platform.time.monotonicNs() < deadline) {
        const encoded = try source.systemCatalog(alloc, .{
            .deadline_ns = @min(deadline, platform.time.monotonicNs() +| 2 * std.time.ns_per_s),
            .fk_generation_publication_authority = true,
        }, .{ .fk_generation_publication_status = table_id });
        defer alloc.free(encoded);
        var status = try std.json.parseFromSlice(publication.Publication, alloc, encoded, .{ .ignore_unknown_fields = true });
        defer status.deinit();
        try status.value.validateState(alloc);
        if (status.value.phase == .published) {
            try std.testing.expectEqual(@as(usize, 1), status.value.child_fenced.len);
            try std.testing.expectEqual(@as(usize, 1), status.value.parent_activated.len);
            try std.testing.expectEqual(@as(usize, 1), status.value.parent_acknowledged.len);
            try std.testing.expectEqual(@as(usize, 1), status.value.child_installed.len);
            return;
        }
        if (status.value.phase == .canceled) return error.UnexpectedPublicationCancel;
        try io.sleep(.fromMilliseconds(20), .awake);
    }
    return error.PublicationTimeout;
}

const PublicationPosition = struct {
    plan_id: publication.Id,
    revision: u64,
    phase: publication.Phase,
};

fn publicationPosition(alloc: std.mem.Allocator, metadata: *metadata_runtime.Server, table_id: u64) !PublicationPosition {
    const source = http_server.StatusSource.fromMetadataHttpService(metadata.server.svc);
    const encoded = try source.systemCatalog(alloc, .{
        .deadline_ns = platform.time.monotonicNs() +| 2 * std.time.ns_per_s,
        .fk_generation_publication_authority = true,
    }, .{ .fk_generation_publication_status = table_id });
    defer alloc.free(encoded);
    var status = try std.json.parseFromSlice(publication.Publication, alloc, encoded, .{ .ignore_unknown_fields = true });
    defer status.deinit();
    try status.value.validateState(alloc);
    return .{ .plan_id = status.value.plan.id, .revision = status.value.revision, .phase = status.value.phase };
}

fn publicationChildGroup(alloc: std.mem.Allocator, metadata: *metadata_runtime.Server, table_id: u64) !u64 {
    const source = http_server.StatusSource.fromMetadataHttpService(metadata.server.svc);
    const encoded = try source.systemCatalog(alloc, .{
        .deadline_ns = platform.time.monotonicNs() +| 2 * std.time.ns_per_s,
        .fk_generation_publication_authority = true,
    }, .{ .fk_generation_publication_status = table_id });
    defer alloc.free(encoded);
    var status = try std.json.parseFromSlice(publication.Publication, alloc, encoded, .{ .ignore_unknown_fields = true });
    defer status.deinit();
    try status.value.validateState(alloc);
    return status.value.plan.child_ranges[0].group_id;
}

const PhysicalTableGroup = struct {
    name: []u8,
    group_id: u64,
};

fn tableGroup(alloc: std.mem.Allocator, metadata: *metadata_runtime.Server, table_id: u64) !PhysicalTableGroup {
    var snapshot = try metadata.server.svc.adminSnapshot();
    defer metadata.server.svc.freeAdminSnapshot(&snapshot);
    const physical_name = for (snapshot.tables) |record| {
        if (record.table_id == table_id) break record.name;
    } else return error.TableRecordUnavailable;
    for (snapshot.ranges) |range| {
        if (range.table_id == table_id) return .{ .name = try alloc.dupe(u8, physical_name), .group_id = range.group_id };
    }
    return error.TableRangeUnavailable;
}

fn raftStatus(data: *data_runtime.DataServer, group_id: u64) ?@import("raft_engine").core.Status {
    const service = data.data_raft orelse return null;
    return service.raftStatus(group_id);
}

pub fn awaitThreeVoters(io: std.Io, first: *data_runtime.DataServer, peers: [2]*DataPeer, group_id: u64) !u64 {
    const deadline = platform.time.monotonicNs() +| 20 * std.time.ns_per_s;
    while (platform.time.monotonicNs() < deadline) {
        const a = raftStatus(first, group_id);
        const b = raftStatus(&peers[0].server, group_id);
        const c = raftStatus(&peers[1].server, group_id);
        const committed = if (a != null and b != null and c != null) @max(a.?.hard.commit_index, @max(b.?.hard.commit_index, c.?.hard.commit_index)) else 0;
        if (a != null and b != null and c != null and committed > 0 and
            a.?.conf_state.voters.len == 3 and b.?.conf_state.voters.len == 3 and c.?.conf_state.voters.len == 3 and
            a.?.soft.leader_id != null and a.?.soft.leader_id == b.?.soft.leader_id and a.?.soft.leader_id == c.?.soft.leader_id and
            a.?.applied_index >= committed and b.?.applied_index >= committed and c.?.applied_index >= committed)
        {
            return a.?.soft.leader_id.?;
        }
        try io.sleep(.fromMilliseconds(10), .awake);
    }
    std.debug.print("self-FK three-voter readiness timeout group={d} raft={any},{any},{any}\n", .{
        group_id,
        raftStatus(first, group_id),
        raftStatus(&peers[0].server, group_id),
        raftStatus(&peers[1].server, group_id),
    });
    return error.ThreeVoterPlacementTimeout;
}

fn builderOwnerReadReady(alloc: std.mem.Allocator, metadata: *metadata_runtime.Server, physical_name: []const u8) !void {
    // Exercise the exact routed read-index source used by FK plan construction,
    // not merely the public table catalog projection.
    const api = metadata.server.owned_public_http_server orelse return error.PublicationSupervisorUnavailable;
    const reads = api.table_reads orelse return error.OwnerReadNotReady;
    var identity = (try reads.lookup(alloc, physical_name, "", .{
        .relational_topology_json = "{\"mode\":\"identity\"}",
        .execution_deadline_ns = platform.time.monotonicNs() +| std.time.ns_per_s,
    }, .read_index)) orelse return error.OwnerIdentityNotReady;
    defer identity.deinit(alloc);
    var catalog = (try reads.integrityCatalog(alloc, physical_name)) orelse return error.OwnerCatalogNotReady;
    defer catalog.deinit(alloc);
}

fn awaitRestartedLocalIntegrityCatalog(alloc: std.mem.Allocator, io: std.Io, data: *data_runtime.DataServer, physical_name: []const u8, group_id: u64) !void {
    // Metadata table visibility does not prove the restarted local owner has
    // reopened and caught up. Do not replay a mutation to test readiness.
    const deadline = platform.time.monotonicNs() +| 20 * std.time.ns_per_s;
    var last_error: ?anyerror = null;
    while (platform.time.monotonicNs() < deadline) {
        const request_deadline = @min(deadline, platform.time.monotonicNs() +| std.time.ns_per_s);
        const response = data.read_source.source().lookupGroupLocal(alloc, group_id, physical_name, "", .{
            .relational_integrity_catalog = true,
            .execution_deadline_ns = request_deadline,
        }, .read_index) catch |err| {
            switch (err) {
                error.StorageReadTemporarilyUnavailable,
                error.StorageKernelOwnerUnavailable,
                error.IntegrityCatalogUnavailable,
                error.IntegrityTopologyBusy,
                error.UnknownGroup,
                error.NotLeader,
                => {
                    last_error = err;
                    try io.sleep(.fromMilliseconds(20), .awake);
                    continue;
                },
                else => return err,
            }
        };
        if (response) |value| {
            var ready = value;
            ready.deinit(alloc);
            return;
        }
        try io.sleep(.fromMilliseconds(20), .awake);
    }
    std.debug.print("self-FK restarted local catalog timeout group={d} last_error={s} raft={any}\n", .{ group_id, if (last_error) |err| @errorName(err) else "none", raftStatus(data, group_id) });
    return error.RestartedIntegrityCatalogNotReady;
}

fn printCompactRoute(alloc: std.mem.Allocator, label: []const u8, catalog: table_catalog.CatalogSource, table_name: []const u8, group_id: u64) void {
    const epoch = table_catalog.groupTopologyEpoch(alloc, catalog, table_name, group_id) catch |err| blk: {
        std.debug.print("self-FK compact route {s} group={d} epoch err={s}\n", .{ label, group_id, @errorName(err) });
        break :blk null;
    };
    if (epoch) |value| std.debug.print("self-FK compact route {s} group={d} epoch={d}\n", .{ label, group_id, value });
    var routing = catalog.vtable.routing_snapshot(catalog.ptr, null) catch |err| {
        std.debug.print("self-FK compact snapshot {s} err={s}\n", .{ label, @errorName(err) });
        return;
    };
    defer catalog.vtable.free_routing_snapshot(catalog.ptr, &routing);
    var table_found = false;
    var range_found = false;
    for (routing.tables) |record| {
        std.debug.print("self-FK compact snapshot {s} table name={s} id={d}\n", .{ label, record.name, record.table_id });
        if (!std.mem.eql(u8, record.name, table_name)) continue;
        table_found = true;
        for (routing.ranges) |range| {
            if (range.table_id == record.table_id and range.group_id == group_id) range_found = true;
        }
    }
    for (routing.ranges) |range| std.debug.print("self-FK compact snapshot {s} range table_id={d} group_id={d}\n", .{ label, range.table_id, range.group_id });
    std.debug.print("self-FK compact snapshot {s} revision={d} tables={d} ranges={d} nodes={} target_range={}\n", .{ label, routing.catalog_revision, routing.tables.len, routing.ranges.len, table_found, range_found });
}

fn printOwnerReadState(alloc: std.mem.Allocator, transport: http.RequestExecutor, data: *data_runtime.DataServer, table_name: []const u8, group_id: u64, node_id: u64, encoded_fence: ?[]const u8) void {
    std.debug.print(
        "self-FK owner node={d} root refresh started={d} completed={d} failed={d} active={} dirty={} poll_same_head={d} same_head={d} no_groups={d} deferred_catch_up={d} restore_pending={d} same_fingerprint={d} reconciled_groups={d}\n",
        .{
            node_id,
            data.provisioned_root_refresh_started.load(.acquire),
            data.provisioned_root_refresh_completed.load(.acquire),
            data.provisioned_root_refresh_failed.load(.acquire),
            data.provisioned_root_refresh_active.load(.acquire),
            data.provisioned_root_refresh_dirty.load(.acquire),
            data.provisioned_root_probe_poll_same_head.load(.acquire),
            data.provisioned_root_probe_same_head.load(.acquire),
            data.provisioned_root_probe_no_local_groups.load(.acquire),
            data.provisioned_root_probe_deferred_catch_up.load(.acquire),
            data.provisioned_root_probe_restore_pending.load(.acquire),
            data.provisioned_root_probe_same_fingerprint.load(.acquire),
            data.provisioned_root_probe_reconciled_groups.load(.acquire),
        },
    );
    std.debug.print(
        "self-FK owner node={d} startup started={d} completed={d} failed={d} active={} dirty={} paired_head_mismatch={d} groups={d} debt={d}\n",
        .{
            node_id,
            data.provisioned_startup_catch_up_started.load(.acquire),
            data.provisioned_startup_catch_up_completed.load(.acquire),
            data.provisioned_startup_catch_up_failed.load(.acquire),
            data.provisioned_startup_catch_up_active.load(.acquire),
            data.provisioned_startup_catch_up_dirty.load(.acquire),
            data.provisioned_startup_probe_head_mismatch.load(.acquire),
            data.provisioned_startup_catch_up_last_group_count.load(.acquire),
            data.provisioned_startup_catch_up_last_groups_with_debt.load(.acquire),
        },
    );
    if (data.remote_metadata) |remote| std.debug.print(
        "self-FK owner node={d} paired-head invalidated={d} cache_changed={d} public_changed={d} private_changed={d}\n",
        .{
            node_id,
            remote.test_faults.paired_head_invalidated.load(.acquire),
            remote.test_faults.paired_head_cache_changed.load(.acquire),
            remote.test_faults.paired_head_public_changed.load(.acquire),
            remote.test_faults.paired_head_private_changed.load(.acquire),
        },
    );
    const label = std.fmt.allocPrint(alloc, "node-{d}", .{node_id}) catch return;
    defer alloc.free(label);
    printCompactRoute(alloc, label, data.read_source.catalog, table_name, group_id);
    const reader = data.read_source.source();
    const local = reader.lookupGroupLocal(alloc, group_id, table_name, "", .{ .relational_topology_json = "{\"mode\":\"identity\"}" }, .read_index) catch |err| blk: {
        std.debug.print("self-FK direct owner node={d} identity err={s}\n", .{ node_id, @errorName(err) });
        break :blk null;
    };
    if (local) |value| {
        var response = value;
        defer response.deinit(alloc);
        std.debug.print("self-FK direct owner node={d} identity=present bytes={d}\n", .{ node_id, response.json.len });
    } else std.debug.print("self-FK direct owner node={d} identity=null\n", .{node_id});
    const base = data.baseUri(alloc) catch return;
    defer alloc.free(base);
    const uri = std.fmt.allocPrint(alloc, "{s}/internal/v1/groups/{d}/tables/{s}/documents/%00relational_control?read_consistency=read_index&_relational_topology=%7B%22mode%22%3A%22identity%22%7D", .{ base, group_id, table_name }) catch return;
    defer alloc.free(uri);
    var client = http_client.ApiHttpClient.init(alloc, transport);
    _ = client.withInternalServiceAuth("hosted-self-fk-internal-v1", "hosted-self-fk");
    const headers: []const http.RequestHeader = if (encoded_fence) |fence| &[_]http.RequestHeader{.{ .name = @import("../metadata/api.zig").catalog_route_fence_header, .value = fence }} else &.{};
    var raw = client.executeRequest(.{ .method = .GET, .uri = uri, .headers = headers, .timeout_ms = 2000 }) catch |err| {
        std.debug.print("self-FK raw owner node={d} transport err={s}\n", .{ node_id, @errorName(err) });
        return;
    };
    defer raw.deinit(alloc);
    std.debug.print("self-FK raw owner node={d} status={d} body={s}\n", .{ node_id, raw.status, raw.body[0..@min(raw.body.len, 160)] });
}

fn printBuilderRouteState(alloc: std.mem.Allocator, transport: http.RequestExecutor, metadata: *metadata_runtime.Server, first: *data_runtime.DataServer, peers: [2]*DataPeer, table_name: []const u8, group_id: u64) void {
    const read_source = metadata.server.owned_public_read_source orelse return;
    const catalog = read_source.catalog;
    printCompactRoute(alloc, "metadata-api", catalog, table_name, group_id);
    const route_fence = if (catalog.vtable.route_fence) |resolve| resolve(catalog.ptr, group_id) catch |err| blk: {
        std.debug.print("self-FK metadata catalog route fence err={s}\n", .{@errorName(err)});
        break :blk null;
    } else null;
    const encoded_fence = if (route_fence) |fence| std.json.Stringify.valueAlloc(alloc, fence, .{}) catch null else null;
    defer if (encoded_fence) |value| alloc.free(value);
    std.debug.print("self-FK metadata route fence present={}\n", .{route_fence != null});
    var snapshot = metadata.server.svc.adminSnapshot() catch |err| {
        std.debug.print("self-FK route state snapshot err={s}\n", .{@errorName(err)});
        return;
    };
    defer metadata.server.svc.freeAdminSnapshot(&snapshot);
    for (snapshot.tables) |record| std.debug.print("self-FK admin snapshot table name={s} id={d}\n", .{ record.name, record.table_id });
    for (snapshot.ranges) |range| std.debug.print("self-FK admin snapshot range table_id={d} group_id={d}\n", .{ range.table_id, range.group_id });
    for (snapshot.placement_intents) |intent| {
        if (intent.record.group_id != group_id) continue;
        std.debug.print("self-FK route placement node={d} serving={s}\n", .{ intent.record.local_node_id, @tagName(intent.serving_state) });
    }
    for (snapshot.stores) |store| {
        if (store.node_id < 9 or store.node_id > 11) continue;
        var group_reports: usize = 0;
        for (store.group_statuses) |status| {
            if (status.group_id != group_id) continue;
            group_reports += 1;
            std.debug.print("self-FK route store node={d} group leader={} empty={} docs={d}\n", .{ store.node_id, status.local_leader, status.empty, status.doc_count });
        }
        std.debug.print("self-FK route store node={d} live={} health={s} api={} raft={} group_reports={d}\n", .{ store.node_id, store.live, store.health_class, store.api_url.len != 0, store.raft_url.len != 0, group_reports });
    }
    printOwnerReadState(alloc, transport, first, table_name, group_id, 9, encoded_fence);
    printOwnerReadState(alloc, transport, &peers[0].server, table_name, group_id, 10, encoded_fence);
    printOwnerReadState(alloc, transport, &peers[1].server, table_name, group_id, 11, encoded_fence);
}

fn awaitBuilderOwnerReadiness(alloc: std.mem.Allocator, io: std.Io, transport: http.RequestExecutor, metadata: *metadata_runtime.Server, first: *data_runtime.DataServer, peers: [2]*DataPeer, table_name: []const u8, group_id: u64) !void {
    _ = try awaitThreeVoters(io, first, peers, group_id);
    awaitBuilderCatalogReadiness(alloc, io, metadata, table_name) catch |err| {
        printBuilderRouteState(alloc, transport, metadata, first, peers, table_name, group_id);
        return err;
    };
}

fn awaitBuilderCatalogReadiness(alloc: std.mem.Allocator, io: std.Io, metadata: *metadata_runtime.Server, table_name: []const u8) !void {
    const deadline = platform.time.monotonicNs() +| 15 * std.time.ns_per_s;
    var last_error: ?anyerror = null;
    while (platform.time.monotonicNs() < deadline) {
        if (builderOwnerReadReady(alloc, metadata, table_name)) |_| return else |err| {
            switch (err) {
                error.StorageReadTemporarilyUnavailable,
                error.IntegrityCatalogUnavailable,
                error.IntegrityTopologyBusy,
                error.OwnerIdentityNotReady,
                error.OwnerCatalogNotReady,
                => {},
                else => return err,
            }
            if (last_error == null or last_error.? != err) {
                std.debug.print("self-FK builder owner probe err={s}\n", .{@errorName(err)});
                last_error = err;
            }
        }
        try io.sleep(.fromMilliseconds(10), .awake);
    }
    return error.BuilderOwnerReadinessTimeout;
}

pub fn transferOwnerLeadership(io: std.Io, first: *data_runtime.DataServer, peers: [2]*DataPeer, group_id: u64) !void {
    const old_leader = try awaitThreeVoters(io, first, peers, group_id);
    const first_status = raftStatus(first, group_id) orelse return error.OwnerRaftStatusUnavailable;
    const candidate: *data_runtime.DataServer = if (first_status.id != old_leader) first else if ((raftStatus(&peers[0].server, group_id) orelse return error.OwnerRaftStatusUnavailable).id != old_leader) &peers[0].server else &peers[1].server;
    const leader: *data_runtime.DataServer = if (first_status.id == old_leader) first else if ((raftStatus(&peers[0].server, group_id) orelse return error.OwnerRaftStatusUnavailable).id == old_leader) &peers[0].server else &peers[1].server;
    const candidate_status = raftStatus(candidate, group_id) orelse return error.OwnerRaftStatusUnavailable;
    try std.testing.expect(candidate_status.applied_index >= first_status.hard.commit_index);
    // A follower campaign cannot displace a healthy lease-holding leader.
    // Request the Raft protocol's explicit, caught-up leadership transfer.
    try leader.data_raft.?.host.http_host.transferLeader(group_id, candidate_status.id);
    const deadline = platform.time.monotonicNs() +| 20 * std.time.ns_per_s;
    while (platform.time.monotonicNs() < deadline) {
        const a = raftStatus(first, group_id);
        const b = raftStatus(&peers[0].server, group_id);
        const c = raftStatus(&peers[1].server, group_id);
        const committed = if (a != null and b != null and c != null) @max(a.?.hard.commit_index, @max(b.?.hard.commit_index, c.?.hard.commit_index)) else 0;
        if (a != null and b != null and c != null and
            a.?.soft.leader_id == candidate_status.id and
            b.?.soft.leader_id == candidate_status.id and
            c.?.soft.leader_id == candidate_status.id and
            a.?.applied_index >= committed and b.?.applied_index >= committed and c.?.applied_index >= committed) return;
        try io.sleep(.fromMilliseconds(10), .awake);
    }
    std.debug.print("self-FK owner transfer timeout old={d} candidate={d} raft={any},{any},{any}\n", .{ old_leader, candidate_status.id, raftStatus(first, group_id), raftStatus(&peers[0].server, group_id), raftStatus(&peers[1].server, group_id) });
    return error.OwnerLeadershipTransferTimeout;
}

// Emit one bounded snapshot only when the mounted DROP continuation fails.
// In particular, compare the metadata API's actual parent-control route with
// all three Raft/apply owners instead of inferring health from the HTTP status.
fn printDropParentFailureState(alloc: std.mem.Allocator, metadata: *metadata_runtime.Server, first: *data_runtime.DataServer, peers: [2]*DataPeer, table_id: u64, table_name: []const u8, group_id: u64) void {
    if (publicationPosition(alloc, metadata, table_id)) |position|
        std.debug.print("self-FK DROP publication phase={s} revision={d}\n", .{ @tagName(position.phase), position.revision })
    else |err|
        std.debug.print("self-FK DROP publication status_err={s}\n", .{@errorName(err)});
    const source = http_server.StatusSource.fromMetadataHttpService(metadata.server.svc);
    if (source.systemCatalog(alloc, .{ .fk_generation_publication_authority = true }, .{ .fk_generation_publication_status = table_id })) |encoded| {
        defer alloc.free(encoded);
        if (std.json.parseFromSlice(publication.Publication, alloc, encoded, .{ .ignore_unknown_fields = true })) |parsed_value| {
            var parsed = parsed_value;
            defer parsed.deinit();
            const expected = parsed.value.plan.child_before;
            var admin = metadata.server.svc.adminSnapshot() catch return;
            defer metadata.server.svc.freeAdminSnapshot(&admin);
            for (admin.tables) |current| if (current.table_id == table_id) {
                std.debug.print("self-FK DROP metadata cut table_equal={} schema_equal={} read_schema_equal={} indexes_equal={} retirement_equal={} ranges={d}\n", .{
                    @import("../metadata/table_manager.zig").tableDefinitionsEqual(current, expected),
                    std.mem.eql(u8, current.schema_json, expected.schema_json),
                    std.mem.eql(u8, current.read_schema_json, expected.read_schema_json),
                    std.mem.eql(u8, current.indexes_json, expected.indexes_json),
                    std.mem.eql(u8, current.relational_retirement_json, expected.relational_retirement_json),
                    admin.ranges.len,
                });
                break;
            };
        } else |err| std.debug.print("self-FK DROP metadata cut decode_err={s}\n", .{@errorName(err)});
    } else |err| std.debug.print("self-FK DROP metadata cut status_err={s}\n", .{@errorName(err)});
    const servers = [_]*data_runtime.DataServer{ first, &peers[0].server, &peers[1].server };
    for (servers, 0..) |server, index| {
        const node_id: u64 = 9 + @as(u64, @intCast(index));
        const generation = server.provisioned_storage.visibleRootGenerationForGroup(group_id);
        if (raftStatus(server, group_id)) |status| {
            std.debug.print("self-FK DROP owner node={d} group={d} id={d} leader={any} term={d} commit={d} applied={d} root_generation={d} root_refresh_failed={d} catch_up_failed={d}\n", .{
                node_id,                                               group_id,                                                  status.id, status.soft.leader_id, status.hard.current_term, status.hard.commit_index, status.applied_index, generation,
                server.provisioned_root_refresh_failed.load(.acquire), server.provisioned_startup_catch_up_failed.load(.acquire),
            });
        } else std.debug.print("self-FK DROP owner node={d} group={d} raft=absent root_generation={d}\n", .{ node_id, group_id, generation });
        const local_status = server.read_source.source().lookupGroupLocal(alloc, group_id, table_name, "", .{
            .relational_topology_json = "{\"mode\":\"identity\"}",
            .execution_deadline_ns = platform.time.monotonicNs() +| std.time.ns_per_s,
        }, .read_index) catch |err| {
            std.debug.print("self-FK DROP owner node={d} identity_err={s}\n", .{ node_id, @errorName(err) });
            continue;
        };
        if (local_status) |value| {
            var response = value;
            defer response.deinit(alloc);
            std.debug.print("self-FK DROP owner node={d} identity=present bytes={d}\n", .{ node_id, response.json.len });
        } else std.debug.print("self-FK DROP owner node={d} identity=absent\n", .{node_id});
        const publication_status = server.read_source.source().lookupGroupLocal(alloc, group_id, table_name, "", .{
            .relational_topology_json = "{\"mode\":\"generation_publication\"}",
            .execution_deadline_ns = platform.time.monotonicNs() +| std.time.ns_per_s,
        }, .read_index) catch |err| {
            std.debug.print("self-FK DROP owner node={d} publication_err={s}\n", .{ node_id, @errorName(err) });
            continue;
        };
        if (publication_status) |value| {
            var response = value;
            defer response.deinit(alloc);
            var parsed = std.json.parseFromSlice(@import("antfly_local_sources").storage_db_relational_integrity_generation_admission.OwnerStatus, alloc, response.json, .{}) catch |err| {
                std.debug.print("self-FK DROP owner node={d} publication_decode_err={s}\n", .{ node_id, @errorName(err) });
                continue;
            };
            defer parsed.deinit();
            const status = parsed.value;
            std.debug.print("self-FK DROP owner node={d} fence={} source_fence={any} staged={any} activated={any} acked={any} installed={any}\n", .{
                node_id,
                status.fence != null,
                if (status.source_fence_receipt) |receipt| receipt.index else null,
                if (status.staged_receipt) |receipt| receipt.index else null,
                if (status.activation_receipt) |receipt| receipt.index else null,
                if (status.acknowledged_receipt) |receipt| receipt.index else null,
                if (status.source_install_receipt) |receipt| receipt.index else null,
            });
        } else std.debug.print("self-FK DROP owner node={d} publication=absent\n", .{node_id});
    }
    const api = metadata.server.owned_public_http_server orelse return;
    const read_source = metadata.server.owned_public_read_source orelse return;
    const catalog = read_source.catalog;
    var fallback = table_router.CatalogBackedGroupRouter.init(catalog, api.localSessionNodeId());
    const router = api.cfg.session_router orelse fallback.router();
    std.debug.print("self-FK DROP parent route local_node={d} local_status={s} leader={any}\n", .{ router.localNodeId(), @tagName(router.localStatus(group_id)), router.groupLeaderNodeId(group_id) });
    var route = table_router.resolveGroupRoute(alloc, catalog, router, group_id, .prefer_leader) catch |err| {
        std.debug.print("self-FK DROP parent route error={s}\n", .{@errorName(err)});
        return;
    } orelse {
        std.debug.print("self-FK DROP parent route absent\n", .{});
        return;
    };
    defer route.deinit(alloc);
    switch (route) {
        .local => std.debug.print("self-FK DROP parent route target=local\n", .{}),
        .remote => |remote| std.debug.print("self-FK DROP parent route target_node={d} base_uri={s}\n", .{ remote.node_id, remote.base_uri }),
    }
    var snapshot = metadata.server.svc.adminSnapshot() catch return;
    defer metadata.server.svc.freeAdminSnapshot(&snapshot);
    for (snapshot.placement_intents) |intent| {
        if (intent.record.group_id != group_id) continue;
        std.debug.print("self-FK DROP placement node={d} serving={s}\n", .{ intent.record.local_node_id, @tagName(intent.serving_state) });
    }
}

fn awaitPublicationPosition(alloc: std.mem.Allocator, io: std.Io, metadata: *metadata_runtime.Server, table_id: u64, expected: PublicationPosition) !void {
    const deadline = platform.time.monotonicNs() +| 10 * std.time.ns_per_s;
    while (true) {
        if (publicationPosition(alloc, metadata, table_id)) |observed| {
            try std.testing.expectEqualDeep(expected, observed);
            return;
        } else |err| {
            if (platform.time.monotonicNs() >= deadline) return err;
            try io.sleep(.fromMilliseconds(10), .awake);
        }
    }
}

const RecoveredOwnerStatus = struct {
    digest_matches: bool,
    schema_matches: bool,
    installed: bool,
    activation_state: @import("antfly_local_sources").storage_db_relational_integrity_activation_contract.State,
    activation_schema_version: u32,
};

fn inspectRecoveredOwner(alloc: std.mem.Allocator, metadata: *metadata_runtime.Server, data: *data_runtime.DataServer, table_id: u64) !RecoveredOwnerStatus {
    const source = http_server.StatusSource.fromMetadataHttpService(metadata.server.svc);
    const encoded = try source.systemCatalog(alloc, .{
        .deadline_ns = platform.time.monotonicNs() +| 2 * std.time.ns_per_s,
        .fk_generation_publication_authority = true,
    }, .{ .fk_generation_publication_status = table_id });
    defer alloc.free(encoded);
    var publication_status = try std.json.parseFromSlice(publication.Publication, alloc, encoded, .{ .ignore_unknown_fields = true });
    defer publication_status.deinit();
    try publication_status.value.validateState(alloc);
    const record = publication_status.value;
    const group_id = record.plan.child_ranges[0].group_id;
    const owner_table_name = record.plan.child_before.name;
    const reader = data.read_source.source();
    var owner_identity = (try reader.lookupGroupLocal(alloc, group_id, owner_table_name, "", .{ .relational_topology_json = "{\"mode\":\"identity\"}" }, .read_index)) orelse return error.OwnerStatusUnavailable;
    defer owner_identity.deinit(alloc);
    var parsed_identity = try std.json.parseFromSlice(@import("antfly_local_sources").storage_db_relational_integrity_topology_contract.Identity, alloc, owner_identity.json, .{ .ignore_unknown_fields = true });
    defer parsed_identity.deinit();
    var owner_schema = (try reader.lookupGroupLocal(alloc, group_id, owner_table_name, "", .{ .relational_topology_json = "{\"mode\":\"public_schema\"}" }, .read_index)) orelse return error.OwnerStatusUnavailable;
    defer owner_schema.deinit(alloc);
    var parsed_schema = try std.json.parseFromSlice([]const u8, alloc, owner_schema.json, .{});
    defer parsed_schema.deinit();
    var owner_publication = (try reader.lookupGroupLocal(alloc, group_id, owner_table_name, "", .{ .relational_topology_json = "{\"mode\":\"generation_publication\"}" }, .read_index)) orelse return error.OwnerStatusUnavailable;
    defer owner_publication.deinit(alloc);
    var parsed_owner_publication = try std.json.parseFromSlice(@import("antfly_local_sources").storage_db_relational_integrity_generation_admission.OwnerStatus, alloc, owner_publication.json, .{ .ignore_unknown_fields = true });
    defer parsed_owner_publication.deinit();
    var activation_response = (try reader.integrityActivation(alloc, owner_table_name, record.plan.child_ranges[0].start_key, "{\"mode\":\"status\"}")) orelse return error.OwnerStatusUnavailable;
    defer activation_response.deinit(alloc);
    var activation = try std.json.parseFromSlice(struct {
        state: @import("antfly_local_sources").storage_db_relational_integrity_activation_contract.State,
        schema_version: u32,
    }, alloc, activation_response.json, .{ .ignore_unknown_fields = true });
    defer activation.deinit();
    const digest_matches = std.mem.eql(u8, &parsed_identity.value.catalog_digest, &record.child_identity.after_catalog_digest);
    const schema_matches = std.mem.eql(u8, parsed_schema.value, record.plan.child_after.schema_json);
    const installed = parsed_owner_publication.value.fence == null and parsed_owner_publication.value.source_install_receipt != null and parsed_owner_publication.value.acknowledged_receipt != null;
    return .{ .digest_matches = digest_matches, .schema_matches = schema_matches, .installed = installed, .activation_state = activation.value.state, .activation_schema_version = activation.value.schema_version };
}

fn awaitRecoveredOwnerReady(alloc: std.mem.Allocator, io: std.Io, metadata: *metadata_runtime.Server, data: *data_runtime.DataServer, table_id: u64, drivers: []const *const raft.ManagedProgressDriver) !void {
    const deadline = platform.time.monotonicNs() +| 10 * std.time.ns_per_s;
    var prior_state: ?@import("antfly_local_sources").storage_db_relational_integrity_activation_contract.State = null;
    while (true) {
        for (drivers) |driver| try driver.checkFailure();
        const observed = inspectRecoveredOwner(alloc, metadata, data, table_id) catch |err| switch (err) {
            error.StorageReadTemporarilyUnavailable,
            error.StorageKernelOwnerTransitionRequired,
            error.StorageKernelOwnerStaleDescriptor,
            error.OwnerStatusUnavailable,
            error.GroupLeaderUnavailable,
            error.NotLeader,
            error.ReadIndexTimeout,
            => {
                if (platform.time.monotonicNs() >= deadline) return err;
                try io.sleep(.fromMilliseconds(20), .awake);
                continue;
            },
            else => return err,
        };
        if (prior_state == null or prior_state.? != observed.activation_state) {
            std.debug.print("self-FK recovered owner digest_match={} schema_match={} installed={} activation={s} activation_version={d}\n", .{ observed.digest_matches, observed.schema_matches, observed.installed, @tagName(observed.activation_state), observed.activation_schema_version });
            prior_state = observed.activation_state;
        }
        if (!observed.digest_matches or !observed.schema_matches or !observed.installed) return error.OwnerPublicationMismatch;
        switch (observed.activation_state) {
            .enforced => return,
            .invalid => return error.OwnerActivationInvalid,
            .validating => {
                if (platform.time.monotonicNs() >= deadline) return error.OwnerActivationPending;
                try io.sleep(.fromMilliseconds(20), .awake);
            },
        }
    }
}

fn stepUntilInjected(
    alloc: std.mem.Allocator,
    io: std.Io,
    metadata: *metadata_runtime.Server,
    table_id: u64,
    server: *http_server.ApiHttpServer,
    expected: PublicationPosition,
    fault: http_server.ApiHttpServer.FkGenerationPublicationTestDriver.Fault,
) !void {
    const driver = http_server.ApiHttpServer.FkGenerationPublicationTestDriver;
    const deadline = platform.time.monotonicNs() +| 10 * std.time.ns_per_s;
    while (true) {
        driver.step(server, fault) catch |err| switch (err) {
            error.InjectedPublicationReplyLoss => return,
            error.GenerationAdmissionPending => {
                // The newly admitted owner route can lag its metadata phase.
                // Keep retrying the same exact revision only while the
                // authoritative publication phase is unchanged.
                try std.testing.expectEqualDeep(expected, try publicationPosition(alloc, metadata, table_id));
                if (platform.time.monotonicNs() >= deadline) return err;
                try io.sleep(.fromMilliseconds(10), .awake);
                continue;
            },
            error.MetadataMutationOutcomeUnknown => {
                if (fault != .after_metadata_mutate) return err;
                // The Raft proposal may have committed even though its reply
                // was lost. Only an exact, linearizable next-revision read
                // counts as the completed step; never resubmit an ambiguous
                // command on a mere timeout or non-linearizable observation.
                const observed = publicationPosition(alloc, metadata, table_id) catch |status_err| {
                    std.debug.print("self-FK ambiguous metadata reply status read failed: {s}\n", .{@errorName(status_err)});
                    return err;
                };
                if (std.mem.eql(u8, &observed.plan_id, &expected.plan_id) and
                    observed.revision == expected.revision + 1 and
                    observed.phase != expected.phase) return;
                std.debug.print("self-FK ambiguous metadata reply remained at phase={s} revision={d}, expected phase={s} revision={d}\n", .{ @tagName(observed.phase), observed.revision, @tagName(expected.phase), expected.revision });
                return err;
            },
            else => return err,
        };
        return error.ExpectedInjectedPublicationReplyLoss;
    }
}

fn drivePublicationWithLostReplies(
    alloc: std.mem.Allocator,
    io: std.Io,
    metadata: *metadata_runtime.Server,
    table_id: u64,
    transport: http.RequestExecutor,
    headers: []const http.RequestHeader,
    base: []const u8,
    stop_after_ack: bool,
) !bool {
    return drivePublicationWithLostRepliesDiagnostic(alloc, io, metadata, table_id, transport, headers, base, stop_after_ack, null, null);
}

fn drivePublicationWithLostRepliesDiagnostic(
    alloc: std.mem.Allocator,
    io: std.Io,
    metadata: *metadata_runtime.Server,
    table_id: u64,
    transport: http.RequestExecutor,
    headers: []const http.RequestHeader,
    base: []const u8,
    stop_after_ack: bool,
    leader_base: ?[]const u8,
    probe_unproven: ?*bool,
) !bool {
    const server = metadata.server.owned_public_http_server orelse return error.PublicationSupervisorUnavailable;
    const driver = http_server.ApiHttpServer.FkGenerationPublicationTestDriver;
    driver.forgetVolatileCursor(server);
    // A fresh process resumed after the ACK cut has already passed the
    // fenced-write probe; the first process ran it before being replaced.
    var fenced_write_checked = (try publicationPosition(alloc, metadata, table_id)).phase == .publishing_child;
    for (0..8) |_| {
        const before = try publicationPosition(alloc, metadata, table_id);
        if (stop_after_ack and before.phase == .publishing_child) {
            try std.testing.expect(fenced_write_checked);
            return false;
        }
        if (before.phase == .published) {
            try std.testing.expect(fenced_write_checked);
            try awaitPublication(alloc, io, metadata, table_id);
            return true;
        }
        if (before.phase == .staging_parents and !fenced_write_checked) {
            // The dual-role owner has durably fenced the old generation, but
            // metadata has not published the successor. A concurrent write
            // must not slip through either the child or parent role.
            if (leader_base) |leader| {
                const started = platform.time.monotonicNs();
                const leader_write = batchOnce(alloc, transport, headers, leader, "{\"inserts\":{\"during-fence-leader-probe\":{\"id\":98}},\"sync_level\":\"full_text\"}") catch |err| {
                    std.debug.print("self-FK leader fenced probe elapsed_ms={d} err={s}\n", .{ (platform.time.monotonicNs() -| started) / std.time.ns_per_ms, @errorName(err) });
                    if (probe_unproven) |unproven| unproven.* = true;
                    fenced_write_checked = true;
                    continue;
                };
                var response = leader_write;
                defer response.deinit(alloc);
                if (response.status != 409)
                    std.debug.print("self-FK leader fenced probe elapsed_ms={d} status={d}\n", .{ (platform.time.monotonicNs() -| started) / std.time.ns_per_ms, response.status });
                if (response.status == 503) {
                    // A retryable refusal does not prove the fence. Do not
                    // replay this possibly delivered key or probe another
                    // writer; finish publication and prove absence first.
                    if (probe_unproven) |unproven| unproven.* = true;
                    fenced_write_checked = true;
                    continue;
                }
                try std.testing.expectEqual(@as(u16, 409), response.status);
            }
            const started = platform.time.monotonicNs();
            const follower_body = if (leader_base == null)
                "{\"inserts\":{\"during-fence\":{\"id\":99}},\"sync_level\":\"full_text\"}"
            else
                "{\"inserts\":{\"during-fence-follower-probe\":{\"id\":97}},\"sync_level\":\"full_text\"}";
            var fenced_write = batchOnce(alloc, transport, headers, base, follower_body) catch |err| {
                if (leader_base == null) return err;
                std.debug.print("self-FK follower fenced probe elapsed_ms={d} err={s}\n", .{ (platform.time.monotonicNs() -| started) / std.time.ns_per_ms, @errorName(err) });
                if (probe_unproven) |unproven| unproven.* = true;
                fenced_write_checked = true;
                continue;
            };
            defer fenced_write.deinit(alloc);
            // A transferred leader may have to reopen a fence-deferred owner.
            // 503 is retryable, not a proof that no proposal occurred. The
            // authoritative post-publication read below proves this exact
            // write did not commit before treating the probe as successful.
            if (fenced_write.status != 409 and fenced_write.status != 503)
                std.debug.print("self-FK fenced write status={d} body={s}\n", .{ fenced_write.status, fenced_write.body });
            try std.testing.expect(fenced_write.status == 409 or fenced_write.status == 503);
            fenced_write_checked = true;
        }
        // First lose the owner reply before metadata records it. Replaying
        // the same action must return an idempotent owner receipt. Then lose
        // the metadata reply after its durable CAS and restart the volatile
        // supervisor cursor before reading the next authoritative phase.
        try stepUntilInjected(alloc, io, metadata, table_id, server, before, .before_metadata_mutate);
        try std.testing.expectEqualDeep(before, try publicationPosition(alloc, metadata, table_id));
        driver.forgetVolatileCursor(server);
        try stepUntilInjected(alloc, io, metadata, table_id, server, before, .after_metadata_mutate);
        const after = try publicationPosition(alloc, metadata, table_id);
        try std.testing.expect(std.mem.eql(u8, &after.plan_id, &before.plan_id));
        try std.testing.expectEqual(before.revision + 1, after.revision);
        try std.testing.expect(after.phase != before.phase);
        driver.forgetVolatileCursor(server);
    }
    return error.PublicationTimeout;
}

fn mountedSelfFk(lost_replies: bool, restart_after_ack: bool, leader_transfer: bool, snapshot_probe: bool) !void {
    const alloc = std.testing.allocator;
    const process_alloc = platform.allocator.processAllocator(alloc);
    const trusted_secret = "hosted-self-fk-trusted-v1";
    const internal_secret = "hosted-self-fk-internal-v1";
    const issuer = "hosted-self-fk";
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", alloc);
    defer alloc.free(root);
    const meta_root = try std.fmt.allocPrint(alloc, "{s}/metadata", .{root});
    defer alloc.free(meta_root);
    const data_root = try std.fmt.allocPrint(alloc, "{s}/data", .{root});
    defer alloc.free(data_root);
    const meta_catalog = try std.fmt.allocPrint(alloc, "{s}/metadata-catalog", .{root});
    defer alloc.free(meta_catalog);
    const data_catalog = try std.fmt.allocPrint(alloc, "{s}/data-catalog", .{root});
    defer alloc.free(data_catalog);
    const session_path = try std.fmt.allocPrint(alloc, "{s}/sql-sessions", .{root});
    defer alloc.free(session_path);
    const snapshots = try std.fmt.allocPrint(alloc, "{s}/snapshots", .{root});
    defer alloc.free(snapshots);
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var metadata = try metadata_runtime.Server.init(process_alloc, .{
        .local_node_id = 1,
        .metadata_group_id = 2297,
        .replica_root_dir = meta_root,
        .replica_catalog_path = meta_catalog,
        .snapshot_root_dir = snapshots,
        .observe_local_replica_root = true,
        .api_server_cfg = .{ .trusted_principal_secret = trusted_secret, .trusted_principal_issuer = issuer, .internal_service_secret = internal_secret, .internal_service_issuer = issuer, .internal_service_auth_capability = "v1; mode=enforce" },
    });
    var metadata_live = true;
    defer if (metadata_live) metadata.deinit();
    try metadata.start();
    try metadata.bootstrapLocal(2297, 1);
    var meta_raft = raft.ManagedProgressDriver.init(io, .{ .ptr = &metadata, .run_once = metadataRaft }, std.time.ns_per_ms);
    var meta_raft_live = true;
    defer if (meta_raft_live) meta_raft.deinit();
    try meta_raft.start();
    var meta_control = raft.ManagedProgressDriver.init(io, .{ .ptr = &metadata, .run_once = metadataControl }, std.time.ns_per_ms);
    var meta_control_live = true;
    defer if (meta_control_live) meta_control.deinit();
    try meta_control.start();
    for (0..600) |_| {
        if (try metadata.server.svc.metadataIncarnation() != null) break;
        try io.sleep(.fromMilliseconds(10), .awake);
    } else return error.MetadataIncarnationUnavailable;
    var metadata_uri = try metadata.adminBaseUri(alloc);
    defer alloc.free(metadata_uri);
    var user_store = usermgr.MemoryStore.init(alloc);
    defer user_store.deinit();
    var policies = casbin.MemoryAdapter.init(alloc);
    defer policies.deinit();
    var user_manager = try usermgr.UserManager.init(alloc, user_store.iface(), try usermgr.initDefaultEnforcer(alloc, policies.iface()));
    defer user_manager.deinit();
    if (snapshot_probe) {
        var read_grant = try usermgr.Permission.initOwned(alloc, .table, "usage_records", .read);
        defer read_grant.deinit(alloc);
        var write_grant = try usermgr.Permission.initOwned(alloc, .table, "usage_records", .write);
        defer write_grant.deinit(alloc);
        var archive_read = try usermgr.Permission.initOwned(alloc, .table, "archived_records", .read);
        defer archive_read.deinit(alloc);
        var archive_write = try usermgr.Permission.initOwned(alloc, .table, "archived_records", .write);
        defer archive_write.deinit(alloc);
        var user = try user_manager.createUser("prepared_writer", "secret", &.{ read_grant, write_grant, archive_read, archive_write });
        user.deinit(alloc);
    }
    var data = try data_runtime.DataServer.initFromMetadataApiUrl(process_alloc, .{
        .replica_root_dir = data_root,
        .replica_catalog_path = data_catalog,
        .store_registration = .{ .node_id = 9, .store_id = 9, .role = "data" },
        .api_server_cfg = .{ .deployment_mode = .distributed, .trusted_principal_secret = trusted_secret, .trusted_principal_issuer = issuer, .internal_service_secret = internal_secret, .internal_service_issuer = issuer, .internal_service_auth_capability = "v1; mode=enforce", .session_store_path = if (snapshot_probe) session_path else null, .session_owner_lease_ttl_ns = if (snapshot_probe) 30 * std.time.ns_per_s else null, .user_manager = if (snapshot_probe) &user_manager else null },
    }, metadata_uri);
    var data_live = true;
    defer if (data_live) {
        deinitDriverIfLive(&meta_control, &meta_control_live);
        data.deinit();
    };
    try data.start();
    try awaitStoreRegistration(io, &data);
    var data_raft = raft.ManagedProgressDriver.init(io, .{ .ptr = &data, .run_once = dataRaft }, std.time.ns_per_ms);
    var data_raft_live = true;
    defer if (data_raft_live) {
        deinitDriverIfLive(&meta_control, &meta_control_live);
        data_raft.deinit();
    };
    try data_raft.start();
    var data_control = raft.ManagedProgressDriver.init(io, .{ .ptr = &data, .run_once = dataControl }, std.time.ns_per_ms);
    var data_control_live = true;
    defer if (data_control_live) {
        deinitDriverIfLive(&meta_control, &meta_control_live);
        data_control.deinit();
    };
    try data_control.start();
    var peers: [2]?*DataPeer = .{ null, null };
    defer {
        deinitDriverIfLive(&meta_control, &meta_control_live);
        for (peers) |peer| if (peer) |active| active.destroy(alloc);
    }
    if (leader_transfer) {
        peers[0] = try DataPeer.create(alloc, process_alloc, io, root, metadata_uri, 10, trusted_secret, internal_secret, issuer);
        peers[1] = try DataPeer.create(alloc, process_alloc, io, root, metadata_uri, 11, trusted_secret, internal_secret, issuer);
    }
    var base = try data.baseUri(alloc);
    defer alloc.free(base);
    var executor = executor_mod.StdHttpExecutor.init(alloc, .{});
    defer executor.deinit();
    const transport = executor.executor();
    const now: i64 = @intCast(@divFloor(platform.time.realtimeNs(), std.time.ns_per_s));
    const claims = try std.fmt.allocPrint(alloc,
        \\{{"iss":"{s}","sub":"user:hosted-self-fk-admin","tenant":"test","admin":true,"iat":{d},"exp":{d}}}
    , .{ issuer, now, now + 3600 });
    defer alloc.free(claims);
    const token = try test_helpers.encodeTrustedPrincipalToken(alloc, trusted_secret, claims);
    defer alloc.free(token);
    const headers = [_]http.RequestHeader{.{ .name = http_server.trusted_principal_header, .value = token }};

    var create = try sql(alloc, transport, &headers, base, if (snapshot_probe)
        "CREATE TABLE usage_records (id TEXT PRIMARY KEY, status TEXT, organization_id TEXT)"
    else
        "CREATE TABLE nodes (id BIGINT PRIMARY KEY, parent_id BIGINT)");
    defer create.deinit(alloc);
    // Placement may still be converging after the catalog commit, even for
    // one owner. awaitTable below is the public readiness barrier.
    try std.testing.expect(create.status == 200 or create.status == 202);
    const table_id = if (snapshot_probe)
        try awaitTableNamed(alloc, io, transport, &headers, base, "usage_records")
    else
        try awaitTable(alloc, io, transport, &headers, base);
    if (snapshot_probe) {
        const published_reads = if (data.http_server) |*server| server.table_reads orelse return error.OwnerReadNotReady else return error.OwnerReadNotReady;
        try std.testing.expect(published_reads.remote_statement_fences_safe);
        const physical = try tableGroup(alloc, &metadata, table_id);
        defer alloc.free(physical.name);
        try awaitRestartedLocalIntegrityCatalog(alloc, io, &data, physical.name, physical.group_id);
        var inserted = try sql(alloc, transport, &headers, base, "INSERT INTO usage_records (id,status) VALUES ('prepared_id','open')");
        defer inserted.deinit(alloc);
        if (inserted.status != 200) std.debug.print("hosted MERGE seed status={d} body={s}\n", .{ inserted.status, inserted.body });
        try std.testing.expectEqual(@as(u16, 200), inserted.status);
        const corpus = try std.json.parseFromSlice(std.json.Value, alloc, @import("antfly_local_sources").sql_parity_fixtures.inventory, .{});
        defer corpus.deinit();
        const original = for (corpus.value.object.get("entries").?.array.items) |entry| {
            if (std.mem.eql(u8, entry.object.get("id").?.string, "sql-0008")) break entry.object.get("sql").?.string;
        } else return error.TestMissingCorpusCase;
        const separator = std.mem.indexOf(u8, original, " AS ") orelse return error.TestInvalidCorpusCase;
        const prepare_body = try std.json.Stringify.valueAlloc(alloc, .{ .statement = original[separator + " AS ".len ..] }, .{});
        defer alloc.free(prepare_body);
        var prepared = try request(alloc, transport, &headers, base, "/db/v1/sql/prepared", .POST, prepare_body);
        defer prepared.deinit(alloc);
        if (prepared.status != 200) std.debug.print("hosted CTE MERGE prepare status={d} body={s}\n", .{ prepared.status, prepared.body });
        try std.testing.expectEqual(@as(u16, 200), prepared.status);
        var prepared_json = try std.json.parseFromSlice(std.json.Value, alloc, prepared.body, .{});
        defer prepared_json.deinit();
        const prepared_id = prepared_json.value.object.get("prepared_id").?.string;
        const execute_suffix = try std.fmt.allocPrint(alloc, "/db/v1/sql/prepared/{s}/execute", .{prepared_id});
        defer alloc.free(execute_suffix);
        var merged = try executePreparedAfterReadReady(alloc, io, transport, &headers, base, execute_suffix);
        defer merged.deinit(alloc);
        if (merged.status != 200) std.debug.print("hosted CTE MERGE status={d} body={s}\n", .{ merged.status, merged.body });
        try std.testing.expectEqual(@as(u16, 200), merged.status);
        var result = try std.json.parseFromSlice(struct { command_tag: []const u8, rows_affected: i64 }, alloc, merged.body, .{ .ignore_unknown_fields = true });
        defer result.deinit();
        try std.testing.expectEqualStrings("MERGE", result.value.command_tag);
        try std.testing.expectEqual(@as(i64, 1), result.value.rows_affected);
        var read_back = try sql(alloc, transport, &headers, base, "SELECT id,status FROM usage_records WHERE id = 'prepared_id'");
        defer read_back.deinit(alloc);
        try std.testing.expectEqual(@as(u16, 200), read_back.status);
        var row_result = try std.json.parseFromSlice(std.json.Value, alloc, read_back.body, .{});
        defer row_result.deinit();
        const rows = row_result.value.object.get("rows").?.array.items;
        try std.testing.expectEqual(@as(usize, 1), rows.len);
        try std.testing.expectEqualStrings("prepared_id", rows[0].array.items[0].string);
        try std.testing.expectEqualStrings("open", rows[0].array.items[1].string);

        // Run the full original PREPARE/EXECUTE text through pgwire's
        // authenticated protocol session, borrowing this same hosted owner.
        const api = if (data.http_server) |*server| server else return error.OwnerReadNotReady;
        const pgwire_deadline = platform.time.monotonicNs() +| 60 * std.time.ns_per_s;
        const merge_select = "SELECT id,status FROM usage_records WHERE id = 'prepared_id'";
        while (true) {
            switch (try runHostedPgwirePreparedOnce(alloc, io, api, original, "EXECUTE cte_merge_plan", merge_select, "MERGE 1\x00", &.{ "prepared_id", "open" })) {
                .committed => break,
                .committed_read_unavailable => {
                    try awaitHostedReadBack(alloc, io, transport, &headers, base, merge_select, &.{ "prepared_id", "open" });
                    break;
                },
                .retry_read => {},
            }
            if (platform.time.monotonicNs() >= pgwire_deadline) return error.PgwireReadNotReady;
            try io.sleep(.fromMilliseconds(50), .awake);
        }

        // The exact sql-0005 cross-table CTE INSERT must use the same mounted
        // owner path, not only the protocol mock or the one-table MERGE path.
        var create_archive = try sql(alloc, transport, &headers, base, "CREATE TABLE archived_records (id TEXT PRIMARY KEY)");
        defer create_archive.deinit(alloc);
        try std.testing.expect(create_archive.status == 200 or create_archive.status == 202);
        _ = try awaitTableNamed(alloc, io, transport, &headers, base, "archived_records");
        const original_insert = for (corpus.value.object.get("entries").?.array.items) |entry| {
            if (std.mem.eql(u8, entry.object.get("id").?.string, "sql-0005")) break entry.object.get("sql").?.string;
        } else return error.TestMissingCorpusCase;
        const insert_deadline = platform.time.monotonicNs() +| 60 * std.time.ns_per_s;
        const insert_select = "SELECT id FROM archived_records WHERE id = 'prepared_id'";
        while (true) {
            switch (try runHostedPgwirePreparedOnce(alloc, io, api, original_insert, "EXECUTE cte_insert_plan", insert_select, "INSERT 0 1\x00", &.{"prepared_id"})) {
                .committed => break,
                .committed_read_unavailable => {
                    try awaitHostedReadBack(alloc, io, transport, &headers, base, insert_select, &.{"prepared_id"});
                    break;
                },
                .retry_read => {},
            }
            if (platform.time.monotonicNs() >= insert_deadline) return error.PgwireReadNotReady;
            try io.sleep(.fromMilliseconds(50), .awake);
        }

        // Keep the exact recursive source nontrivial: the anchor includes both
        // rows and the delta worklist must discover the child once more.
        var child = try sql(alloc, transport, &headers, base, "INSERT INTO usage_records (id,status,organization_id) VALUES ('child_id','open','prepared_id')");
        defer child.deinit(alloc);
        try std.testing.expectEqual(@as(u16, 200), child.status);
        const recursive_read = for (corpus.value.object.get("entries").?.array.items) |entry| {
            if (std.mem.eql(u8, entry.object.get("id").?.string, "sql-0009")) break entry.object.get("sql").?.string;
        } else return error.TestMissingCorpusCase;
        const read_deadline = platform.time.monotonicNs() +| 60 * std.time.ns_per_s;
        while (true) {
            if (try runHostedPgwireRecursiveReadOnce(alloc, io, api, recursive_read) == .complete) break;
            if (platform.time.monotonicNs() >= read_deadline) return error.PgwireReadNotReady;
            try io.sleep(.fromMilliseconds(50), .awake);
        }
        const recursive_update = for (corpus.value.object.get("entries").?.array.items) |entry| {
            if (std.mem.eql(u8, entry.object.get("id").?.string, "sql-0010")) break entry.object.get("sql").?.string;
        } else return error.TestMissingCorpusCase;
        const recursive_deadline = platform.time.monotonicNs() +| 60 * std.time.ns_per_s;
        const recursive_select = "SELECT id,status FROM usage_records WHERE id = 'child_id'";
        while (true) {
            switch (try runHostedPgwirePreparedOnce(alloc, io, api, recursive_update, "EXECUTE recursive_usage_plan", recursive_select, "UPDATE 2\x00", &.{ "child_id", "done" })) {
                .committed => break,
                .committed_read_unavailable => {
                    try awaitHostedReadBack(alloc, io, transport, &headers, base, recursive_select, &.{ "child_id", "done" });
                    break;
                },
                .retry_read => {},
            }
            if (platform.time.monotonicNs() >= recursive_deadline) return error.PgwireReadNotReady;
            try io.sleep(.fromMilliseconds(50), .awake);
        }
        return;
    }
    {
        const physical = try tableGroup(alloc, &metadata, table_id);
        defer alloc.free(physical.name);
        // Public catalog readiness does not establish the metadata frontend's
        // routed owner ReadIndex/catalog view. Prove that prerequisite before
        // the first DDL proposal, including the single-voter recovery cases.
        if (leader_transfer)
            try awaitBuilderOwnerReadiness(alloc, io, transport, &metadata, &data, .{ peers[0].?, peers[1].? }, physical.name, physical.group_id)
        else
            try awaitBuilderCatalogReadiness(alloc, io, &metadata, physical.name);
    }
    var paused_metadata_fk_server: ?*http_server.ApiHttpServer = null;
    defer if (paused_metadata_fk_server) |server| http_server.ApiHttpServer.FkGenerationPublicationTestDriver.resumeBackground(server);
    var paused_data_fk_server: ?*http_server.ApiHttpServer = null;
    defer if (paused_data_fk_server) |server| http_server.ApiHttpServer.FkGenerationPublicationTestDriver.resumeBackground(server);
    if (lost_replies) {
        const server = metadata.server.owned_public_http_server orelse return error.PublicationSupervisorUnavailable;
        try http_server.ApiHttpServer.FkGenerationPublicationTestDriver.pauseBackground(server, io);
        paused_metadata_fk_server = server;
        const data_server = if (data.http_server) |*api| api else return error.PublicationSupervisorUnavailable;
        try http_server.ApiHttpServer.FkGenerationPublicationTestDriver.pauseBackground(data_server, io);
        paused_data_fk_server = data_server;
        if (leader_transfer) {
            for (peers) |peer| {
                const active = peer orelse return error.MissingDataPeer;
                const api = if (active.server.http_server) |*peer_server| peer_server else return error.PublicationSupervisorUnavailable;
                try http_server.ApiHttpServer.FkGenerationPublicationTestDriver.pauseBackground(api, io);
                active.paused_fk = api;
            }
        }
    }
    const add_statement = "ALTER TABLE nodes ADD CONSTRAINT self_parent FOREIGN KEY (parent_id) REFERENCES nodes(id)";
    var add = try sql(alloc, transport, &headers, metadata_uri, add_statement);
    defer add.deinit(alloc);
    if (add.status != 202) std.debug.print("mounted self-FK ADD status={d} body={s}\n", .{ add.status, add.body });
    try std.testing.expectEqual(@as(u16, 202), add.status);
    if (lost_replies) {
        const child_group = if (leader_transfer) try publicationChildGroup(alloc, &metadata, table_id) else 0;
        if (leader_transfer) _ = try awaitThreeVoters(io, &data, .{ peers[0].?, peers[1].? }, child_group);
        const completed = try drivePublicationWithLostReplies(alloc, io, &metadata, table_id, transport, &headers, base, restart_after_ack or leader_transfer);
        if (leader_transfer) {
            try std.testing.expect(!completed);
            const acknowledged = try publicationPosition(alloc, &metadata, table_id);
            try std.testing.expectEqual(publication.Phase.publishing_child, acknowledged.phase);
            try transferOwnerLeadership(io, &data, .{ peers[0].?, peers[1].? }, child_group);
            try std.testing.expectEqualDeep(acknowledged, try publicationPosition(alloc, &metadata, table_id));
            try std.testing.expect(try drivePublicationWithLostReplies(alloc, io, &metadata, table_id, transport, &headers, base, false));
        } else if (restart_after_ack) {
            try std.testing.expect(!completed);
            const acknowledged = try publicationPosition(alloc, &metadata, table_id);
            try std.testing.expectEqual(publication.Phase.publishing_child, acknowledged.phase);

            // Replace both processes while the dual-role fence is still
            // active. The new metadata supervisor must select work from the
            // persisted ACK, not from the old process's volatile cursor; the
            // data owner must reopen its exact old-generation fence.
            if (paused_data_fk_server) |server| {
                http_server.ApiHttpServer.FkGenerationPublicationTestDriver.resumeBackground(server);
                paused_data_fk_server = null;
            }
            deinitDriverIfLive(&meta_control, &meta_control_live);
            data_control.deinit();
            data_control_live = false;
            data_raft.deinit();
            data_raft_live = false;
            data.deinit();
            data_live = false;
            if (paused_metadata_fk_server) |server| {
                http_server.ApiHttpServer.FkGenerationPublicationTestDriver.resumeBackground(server);
                paused_metadata_fk_server = null;
            }
            meta_raft.deinit();
            meta_raft_live = false;
            metadata.deinit();
            metadata_live = false;

            metadata = try metadata_runtime.Server.init(process_alloc, .{
                .local_node_id = 1,
                .metadata_group_id = 2297,
                .replica_root_dir = meta_root,
                .replica_catalog_path = meta_catalog,
                .snapshot_root_dir = snapshots,
                .observe_local_replica_root = true,
                .api_server_cfg = .{ .trusted_principal_secret = trusted_secret, .trusted_principal_issuer = issuer, .internal_service_secret = internal_secret, .internal_service_issuer = issuer, .internal_service_auth_capability = "v1; mode=enforce" },
            });
            metadata_live = true;
            try metadata.start();
            try metadata.bootstrapLocal(2297, 1);
            const new_meta_api = metadata.server.owned_public_http_server orelse return error.PublicationSupervisorUnavailable;
            try http_server.ApiHttpServer.FkGenerationPublicationTestDriver.pauseBackground(new_meta_api, io);
            paused_metadata_fk_server = new_meta_api;
            meta_raft = raft.ManagedProgressDriver.init(io, .{ .ptr = &metadata, .run_once = metadataRaft }, std.time.ns_per_ms);
            meta_raft_live = true;
            try meta_raft.start();
            meta_control = raft.ManagedProgressDriver.init(io, .{ .ptr = &metadata, .run_once = metadataControl }, std.time.ns_per_ms);
            meta_control_live = true;
            try meta_control.start();
            for (0..600) |_| {
                if (try metadata.server.svc.metadataIncarnation() != null) break;
                try io.sleep(.fromMilliseconds(10), .awake);
            } else return error.MetadataIncarnationUnavailable;
            try awaitPublicationPosition(alloc, io, &metadata, table_id, acknowledged);
            const new_metadata_uri = try metadata.adminBaseUri(alloc);
            alloc.free(metadata_uri);
            metadata_uri = new_metadata_uri;

            data = try data_runtime.DataServer.initFromMetadataApiUrl(process_alloc, .{
                .replica_root_dir = data_root,
                .replica_catalog_path = data_catalog,
                .store_registration = .{ .node_id = 9, .store_id = 9, .role = "data" },
                .api_server_cfg = .{ .deployment_mode = .distributed, .trusted_principal_secret = trusted_secret, .trusted_principal_issuer = issuer, .internal_service_secret = internal_secret, .internal_service_issuer = issuer, .internal_service_auth_capability = "v1; mode=enforce" },
            }, metadata_uri);
            data_live = true;
            try data.start();
            const new_data_api = if (data.http_server) |*api| api else return error.PublicationSupervisorUnavailable;
            try http_server.ApiHttpServer.FkGenerationPublicationTestDriver.pauseBackground(new_data_api, io);
            paused_data_fk_server = new_data_api;
            try awaitStoreRegistration(io, &data);
            data_raft = raft.ManagedProgressDriver.init(io, .{ .ptr = &data, .run_once = dataRaft }, std.time.ns_per_ms);
            data_raft_live = true;
            try data_raft.start();
            data_control = raft.ManagedProgressDriver.init(io, .{ .ptr = &data, .run_once = dataControl }, std.time.ns_per_ms);
            data_control_live = true;
            try data_control.start();
            const new_base = try data.baseUri(alloc);
            alloc.free(base);
            base = new_base;
            try std.testing.expect(try drivePublicationWithLostReplies(alloc, io, &metadata, table_id, transport, &headers, base, false));
        } else try std.testing.expect(completed);
    } else try awaitPublication(alloc, io, &metadata, table_id);
    var added = try table(alloc, transport, &headers, base);
    defer added.deinit(alloc);
    try std.testing.expect(try hasSelfFk(alloc, added));
    if (restart_after_ack) try awaitRecoveredOwnerReady(alloc, io, &metadata, &data, table_id, &.{ &meta_raft, &meta_control, &data_raft, &data_control });
    try awaitConstraintCoverage(alloc, io, transport, &headers, base, &.{ &meta_raft, &meta_control, &data_raft, &data_control });
    if (lost_replies) {
        var absent = try request(alloc, transport, &headers, base, "/db/v1/tables/nodes/documents/during-fence", .GET, null);
        defer absent.deinit(alloc);
        try std.testing.expectEqual(@as(u16, 404), absent.status);
        const physical = try tableGroup(alloc, &metadata, table_id);
        defer alloc.free(physical.name);
        const api = metadata.server.owned_public_http_server orelse return error.PublicationSupervisorUnavailable;
        const reads = api.table_reads orelse return error.OwnerReadNotReady;
        var visible = try reads.lookup(alloc, physical.name, "during-fence", .{
            .execution_deadline_ns = platform.time.monotonicNs() +| 5 * std.time.ns_per_s,
        }, .read_index);
        if (visible) |*response| {
            response.deinit(alloc);
            return error.FencedWriteCommitted;
        }
    }
    var parent_row = try batchOnce(alloc, transport, &headers, base, "{\"inserts\":{\"p\":{\"id\":1}},\"sync_level\":\"full_text\"}");
    defer parent_row.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 201), parent_row.status);
    var child_row = try batchOnce(alloc, transport, &headers, base, "{\"inserts\":{\"c\":{\"id\":2,\"parent_id\":1}},\"sync_level\":\"full_text\"}");
    defer child_row.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 201), child_row.status);
    var blocked = try batchOnce(alloc, transport, &headers, base, "{\"deletes\":[\"p\"],\"sync_level\":\"full_text\"}");
    defer blocked.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 409), blocked.status);
    var drop = try sql(alloc, transport, &headers, metadata_uri, "ALTER TABLE nodes DROP CONSTRAINT self_parent");
    defer drop.deinit(alloc);
    if (drop.status != 202) std.debug.print("mounted self-FK DROP status={d} body={s}\n", .{ drop.status, drop.body });
    try std.testing.expectEqual(@as(u16, 202), drop.status);
    if (leader_transfer) {
        const child_group = try publicationChildGroup(alloc, &metadata, table_id);
        const leader_id = try awaitThreeVoters(io, &data, .{ peers[0].?, peers[1].? }, child_group);
        const leader_server: *data_runtime.DataServer = if (leader_id == 9) &data else if (leader_id == 10) &peers[0].?.server else if (leader_id == 11) &peers[1].?.server else return error.OwnerLeaderUnknown;
        const follower_server: *data_runtime.DataServer = if (leader_id != 9) &data else &peers[0].?.server;
        const leader_base = try leader_server.baseUri(alloc);
        defer alloc.free(leader_base);
        const follower_base = try follower_server.baseUri(alloc);
        defer alloc.free(follower_base);
        var probe_unproven = false;
        const stopped_at_ack = drivePublicationWithLostRepliesDiagnostic(alloc, io, &metadata, table_id, transport, &headers, follower_base, true, leader_base, &probe_unproven) catch |err| {
            const physical = tableGroup(alloc, &metadata, table_id) catch return err;
            defer alloc.free(physical.name);
            printDropParentFailureState(alloc, &metadata, &data, .{ peers[0].?, peers[1].? }, table_id, physical.name, child_group);
            return err;
        };
        try std.testing.expect(!stopped_at_ack);
        const acknowledged = try publicationPosition(alloc, &metadata, table_id);
        try std.testing.expectEqual(publication.Phase.publishing_child, acknowledged.phase);
        try transferOwnerLeadership(io, &data, .{ peers[0].?, peers[1].? }, child_group);
        try std.testing.expectEqualDeep(acknowledged, try publicationPosition(alloc, &metadata, table_id));
        try std.testing.expect(try drivePublicationWithLostReplies(alloc, io, &metadata, table_id, transport, &headers, base, false));
        const physical = try tableGroup(alloc, &metadata, table_id);
        defer alloc.free(physical.name);
        const api = metadata.server.owned_public_http_server orelse return error.PublicationSupervisorUnavailable;
        const reads = api.table_reads orelse return error.OwnerReadNotReady;
        for ([_][]const u8{ "during-fence-leader-probe", "during-fence-follower-probe" }) |key| {
            // A transferred owner may be reopening after publication. Retry
            // only this linearizable read; never replay an ambiguous write.
            const deadline = platform.time.monotonicNs() +| 20 * std.time.ns_per_s;
            while (true) {
                var visible = reads.lookup(alloc, physical.name, key, .{
                    .execution_deadline_ns = @min(deadline, platform.time.monotonicNs() +| 2 * std.time.ns_per_s),
                }, .read_index) catch |err| switch (err) {
                    error.StorageReadTemporarilyUnavailable, error.StorageKernelOwnerUnavailable, error.ConcurrencyUnavailable => {
                        if (platform.time.monotonicNs() >= deadline) return err;
                        try io.sleep(.fromMilliseconds(20), .awake);
                        continue;
                    },
                    else => return err,
                };
                if (visible) |*response| {
                    response.deinit(alloc);
                    return error.FencedWriteCommitted;
                }
                break;
            }
        }
        if (probe_unproven) return error.FencedWriteProbeUnproven;
    } else if (lost_replies) try std.testing.expect(try drivePublicationWithLostReplies(alloc, io, &metadata, table_id, transport, &headers, base, false)) else try awaitPublication(alloc, io, &metadata, table_id);
    if (paused_data_fk_server) |server| {
        http_server.ApiHttpServer.FkGenerationPublicationTestDriver.resumeBackground(server);
        paused_data_fk_server = null;
    }
    deinitDriverIfLive(&meta_control, &meta_control_live);
    data_control.deinit();
    data_control_live = false;
    data_raft.deinit();
    data_raft_live = false;
    data.deinit();
    data_live = false;
    data = try data_runtime.DataServer.initFromMetadataApiUrl(process_alloc, .{
        .replica_root_dir = data_root,
        .replica_catalog_path = data_catalog,
        .store_registration = .{ .node_id = 9, .store_id = 9, .role = "data" },
        .api_server_cfg = .{ .deployment_mode = .distributed, .trusted_principal_secret = trusted_secret, .trusted_principal_issuer = issuer, .internal_service_secret = internal_secret, .internal_service_issuer = issuer, .internal_service_auth_capability = "v1; mode=enforce" },
    }, metadata_uri);
    data_live = true;
    try data.start();
    meta_control = raft.ManagedProgressDriver.init(io, .{ .ptr = &metadata, .run_once = metadataControl }, std.time.ns_per_ms);
    meta_control_live = true;
    try meta_control.start();
    try awaitStoreRegistration(io, &data);
    data_raft = raft.ManagedProgressDriver.init(io, .{ .ptr = &data, .run_once = dataRaft }, std.time.ns_per_ms);
    data_raft_live = true;
    try data_raft.start();
    data_control = raft.ManagedProgressDriver.init(io, .{ .ptr = &data, .run_once = dataControl }, std.time.ns_per_ms);
    data_control_live = true;
    try data_control.start();
    const restarted_base = try data.baseUri(alloc);
    defer alloc.free(restarted_base);
    try std.testing.expectEqual(table_id, try awaitTable(alloc, io, transport, &headers, restarted_base));
    var after_drop = try table(alloc, transport, &headers, restarted_base);
    defer after_drop.deinit(alloc);
    try std.testing.expect(!try hasSelfFk(alloc, after_drop));
    const restarted_physical = try tableGroup(alloc, &metadata, table_id);
    defer alloc.free(restarted_physical.name);
    if (leader_transfer) {
        _ = try awaitThreeVoters(io, &data, .{ peers[0].?, peers[1].? }, restarted_physical.group_id);
    }
    try awaitRestartedLocalIntegrityCatalog(alloc, io, &data, restarted_physical.name, restarted_physical.group_id);
    // Readiness is a distributed, epoch-fenced coverage fact, not merely a
    // successful point read. Wait only on the read-only public status route;
    // an unavailable write must never be replayed to probe activation.
    try awaitConstraintCoverage(alloc, io, transport, &headers, restarted_base, &.{ &meta_raft, &meta_control, &data_raft, &data_control });
    if (leader_transfer) {
        // The restarted endpoint may forward the final write to either
        // voter. Verify every current owner can serve a linearizable read
        // before the mutation; only an exact durable-abort response permits
        // retry, never an ambiguous write or generic 503.
        for (peers) |peer| try awaitRestartedLocalIntegrityCatalog(alloc, io, &peer.?.server, restarted_physical.name, restarted_physical.group_id);
    }
    var released = try batchAfterDefiniteAbort(alloc, io, transport, &headers, restarted_base, "{\"deletes\":[\"p\"],\"sync_level\":\"full_text\"}");
    defer released.deinit(alloc);
    if (released.status != 201) std.debug.print("self-FK post-restart delete status={d} body={s}\n", .{ released.status, released.body });
    try std.testing.expectEqual(@as(u16, 201), released.status);
}

test "mounted hosted self-FK ADD DROP restart" {
    try mountedSelfFk(false, false, false, false);
}

test "mounted hosted self-FK publication resumes after lost owner and metadata replies" {
    try mountedSelfFk(true, false, false, false);
}

test "mounted hosted self-FK resumes after metadata and owner cold restart at parent ACK" {
    try mountedSelfFk(true, true, false, false);
}

test "mounted hosted self-FK survives three-voter owner leadership transfer at ADD and DROP ACK" {
    try mountedSelfFk(true, false, true, false);
}

test "mounted hosted prepared CTE mutations and recursive read retain owner fences" {
    // Exact mounted corpus cases sql-0005, sql-0008, sql-0009, and sql-0010.
    try mountedSelfFk(false, false, false, true);
}
