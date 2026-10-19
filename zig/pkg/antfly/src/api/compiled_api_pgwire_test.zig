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

//! Real TCP scalar and native-table parked portals through the compiled API.
//! Remote streams and process-restart proofs remain separate.
const std = @import("std");
const bridge = @import("kernel_bridge.zig");
const native = @import("http_server.zig");
const usermgr = @import("../usermgr/mod.zig");
const casbin = @import("antfly_casbin");

const Client = @import("pgwire_pool_test_client.zig").Client;

fn waitNoQueries(server: *bridge.ApiHttpServer) !void {
    const io = std.testing.io;
    const until = std.Io.Clock.awake.now(io).addDuration(.fromSeconds(2));
    while (server.queryAdmissionStats().in_flight != 0) {
        if (std.Io.Clock.awake.now(io).nanoseconds >= until.nanoseconds) return error.PgwireFixtureQueryDidNotRetire;
        try io.sleep(.fromMilliseconds(1), .awake);
    }
}

const NativeTable = @import("compiled_api_native_table_fixture.zig").Fixture;
const native_query = "SELECT id AS x FROM usage_records WHERE id = $1 UNION ALL SELECT id + 1 FROM usage_records WHERE id = $1";
const Mode = enum { idle, portals, native_portals, native_refusals, native_cancel, native_stream_cancel, native_stream_deadline };
fn waitParkedOrQueued(server: *bridge.ApiHttpServer, queued: bool) !void {
    const io = std.testing.io;
    const until = std.Io.Clock.awake.now(io).addDuration(.fromSeconds(2));
    while (true) {
        const admission = server.queryAdmissionStats();
        const scheduling = server.runnableSchedulingStats();
        if (admission.in_flight == 2 and admission.queued == @as(usize, @intFromBool(queued)) and
            scheduling.operations >= 2 and scheduling.output_blocked >= 2 and scheduling.runnable_slots == 0) return;
        if (std.Io.Clock.awake.now(io).nanoseconds >= until.nanoseconds) return error.PgwireFixturePortalDidNotPark;
        try io.sleep(.fromMilliseconds(1), .awake);
    }
}
fn portalProgress(server: *bridge.ApiHttpServer, clients: [6]?*Client, native_tables: bool) !void {
    // Reuse identical statement and portal names with different bound values
    // on two connections. Their remaining rows must retain session affinity.
    const query = if (native_tables) native_query else "SELECT $1 AS x UNION ALL SELECT $1 + 1";
    try clients[0].?.suspendPortalQuery('7', query);
    try clients[1].?.suspendPortalQuery('4', query);
    try waitParkedOrQueued(server, false);
    try clients[2].?.send('Q', "SELECT 1\x00");
    try waitParkedOrQueued(server, true);
    // Query admission is full. Closing an existing portal needs no new query
    // permit; its protected cleanup must admit the already queued scalar.
    try clients[0].?.closePortal();
    try clients[2].?.expectScalar();
    // Closing the first session's portal must not close or rebind the other.
    try clients[1].?.finishPortal('5');
    try waitNoQueries(server);
    try clients[0].?.scalar();
    try clients[1].?.scalar();
    try waitNoQueries(server);
}

test "API kernel compiled pgwire authenticates idle TCP connections without consuming query admission" {
    try runFixture(.idle);
}
test "API kernel compiled pgwire parks isolated portals and releases queued SQL through protected close" {
    try runFixture(.portals);
}
test "API kernel compiled pgwire retains native table portals and retires disconnected sessions through checked callbacks" {
    try runFixture(.native_portals);
}
test "API kernel compiled pgwire retires native captures after checked nested refusal and reuses the session" {
    try runFixture(.native_refusals);
}
test "API kernel compiled pgwire cancels queued SQL at connection capacity and preserves native portals" {
    try runFixture(.native_cancel);
}
test "API kernel compiled pgwire preserves live native snapshots across queued cancellation at connection capacity" {
    try runFixture(.native_stream_cancel);
}
test "API kernel compiled pgwire preserves live native snapshots and session reuse after queued statement timeout" {
    try runFixture(.native_stream_deadline);
}
fn runFixture(mode: Mode) !void {
    const deadline_mode = mode == .native_stream_deadline;
    const streaming = mode == .native_stream_cancel or deadline_mode;
    const cancel_mode = mode == .native_cancel or streaming;
    const native_tables = mode == .native_portals or mode == .native_refusals or cancel_mode;
    comptime {
        if (bridge.ApiHttpServer == native.ApiHttpServer) @compileError("pgwire TCP fixture requires the compiled opaque API bridge");
    }
    const allocator = std.testing.allocator;
    // Threaded's stock POSIX connector does not implement connect timeouts.
    // Use the same bounded connector as production transports; the original
    // connection deadline still also covers authentication and query I/O.
    const connector = @import("antfly_local_sources").common_threaded_connect_io;
    var client_threads = std.Io.Threaded.init(allocator, .{ .concurrent_limit = .limited(16) });
    const client_vtable = connector.createVTable(allocator, &client_threads) catch |err| {
        client_threads.deinit();
        return err;
    };
    defer {
        client_threads.deinit();
        allocator.destroy(client_vtable);
    }
    const client_io = connector.io(&client_threads, client_vtable);
    var store = usermgr.MemoryStore.init(allocator);
    defer store.deinit();
    var policy = casbin.MemoryAdapter.init(allocator);
    defer policy.deinit();
    var manager = try usermgr.UserManager.init(allocator, store.iface(), try usermgr.initDefaultEnforcer(allocator, policy.iface()));
    defer manager.deinit();
    var permission = try usermgr.Permission.initOwned(allocator, .namespace, "*", .read);
    defer permission.deinit(allocator);
    var table_permission = try usermgr.Permission.initOwned(allocator, .table, "usage_records", .read);
    defer table_permission.deinit(allocator);
    var user = try manager.createUser("image-reader", "local-test-password", if (native_tables) &.{ permission, table_permission } else &.{permission});
    defer user.deinit(allocator);
    const background = @import("antfly_local_sources").storage_background_runtime;
    var runtime = try background.BackendRuntimeHandle.init(allocator, .{ .backend = .io_threaded, .lane_limits = .{ .worker_capacity = 32 } });
    defer runtime.deinit();
    var native_table: NativeTable = .{};
    if (native_tables) try native_table.init(allocator, runtime.ptr());
    defer if (native_tables) native_table.deinit();
    if (streaming) try native_table.addContinuationRows(allocator);
    var token: u8 = 0;
    const Status = struct {
        fn get(_: *anyopaque) !@import("../metadata/api.zig").MetadataStatus {
            return .{ .metadata_group_id = 1, .metadata_epoch = 7, .metadata_incarnation = "0123456789abcdef0123456789abcdef".*, .metrics = .{} };
        }
        fn catalog(_: *anyopaque, a: std.mem.Allocator, context: @import("antfly_local_sources").api_operation.RequestContext, call: @import("../system_catalog/server_call.zig").Call) ![]u8 {
            try context.ensureActive();
            if (call == .setting_snapshot) {
                const principal = context.setting_read_principal orelse return error.Forbidden;
                if (context.setting_admin or !std.mem.eql(u8, principal, call.setting_snapshot.principal) or !std.mem.eql(u8, call.setting_snapshot.database, "default")) return error.Forbidden;
                return std.json.Stringify.valueAlloc(a, .{ .scope = call.setting_snapshot, .epoch = @as(u64, 1), .definitions = &.{} }, .{});
            }
            if (call == .read and call.read.kind == .namespace and std.mem.eql(u8, call.read.database, "default")) {
                const name = call.read.name orelse return error.PgwireFixtureUnexpectedCatalogCall;
                if (std.mem.eql(u8, name, "public")) return a.dupe(u8, "{}");
            }
            if (call == .policy_publication_status) return a.dupe(u8, "null");
            if (call == .resolve_many and call.resolve_many.targets.len == 0 and call.resolve_many.storage_names.len == 0) {
                if (call.resolve_many.expected_revision) |revision| if (revision != 7) return error.CatalogGenerationChanged;
                return std.json.Stringify.valueAlloc(a, @import("antfly_local_sources").system_catalog_domain.ResolvedMany{ .revision = 7, .tables = &.{} }, .{});
            }
            return error.PgwireFixtureUnexpectedCatalogCall;
        }
    };
    // Initialize auth before construction: the producer owns a config copy.
    var api = try bridge.ApiHttpServer.initWithConfig(allocator, .{
        .backend_runtime = runtime.ptr(),
        .user_manager = &manager,
        .auth_enabled = true,
        .request_memory_bytes = 16 << 20,
        .retained_memory_bytes = 16 << 20,
        .query_max_concurrent_requests = 2,
        .query_waiting = .{ .max_queued_requests = 4, .max_queued_bytes = 65536, .max_wait_ms = 5000 },
        .runnable_policy = .{ .slots = 4, .cleanup_slots = 1, .max_operations = 16, .read_isolation = true, .max_wait_ms = 5000, .control_bytes = @import("antfly_local_sources").common_config.default_runnable_policy.control_bytes },
        .pgwire = .{ .enabled = true, .bind_port = 0, .max_connections = if (cancel_mode) 6 else 8, .max_session_bytes = 8 << 20, .max_retained_bytes = if (mode != .idle) 4 << 20 else 512 << 10, .retained_bytes_per_connection = if (mode != .idle) 512 << 10 else 64 << 10 },
    }, if (native_tables) try native_table.statusSource() else .{ .ptr = &token, .vtable = &.{ .status = Status.get, .system_catalog = Status.catalog, .supports_query_definitions = true } }, if (native_tables) native_table.readSource() else null, null);
    var api_live = true;
    defer if (api_live) api.deinit();
    const port = (try api.pgwireBoundPort()) orelse return error.PgwireFixtureMissingListener;
    if (port == 0) return error.PgwireFixtureMissingListener;
    {
        var clients: [6]?*Client = @splat(null);
        defer for (clients) |client| if (client) |value| value.close();
        for (&clients) |*client| client.* = try Client.connect(allocator, client_io, port, "local-test-password");
        try waitNoQueries(&api);
        if (!cancel_mode) try std.testing.expectError(error.PgwireAuthenticationRejected, Client.connect(allocator, client_io, port, "incorrect-password"));
        if (mode == .portals or mode == .native_portals) try portalProgress(&api, clients, native_tables);
        if (cancel_mode) {
            if (deadline_mode) try clients[2].?.command("SET statement_timeout = '100ms'", "SET\x00");
            const stream_query = "SELECT id + $1 AS x FROM usage_records";
            if (streaming) {
                try clients[0].?.suspendPortalQueryExpected('0', stream_query, '4');
                try clients[1].?.suspendPortalQueryExpected('1', stream_query, '5');
            } else {
                try clients[0].?.suspendPortalQuery('7', native_query);
                try clients[1].?.suspendPortalQuery('4', native_query);
            }
            try waitParkedOrQueued(&api, false);
            const captured = native_table.captures.load(.acquire);
            const closed = native_table.closes.load(.acquire);
            const live = native_table.live_captures.load(.acquire);
            std.debug.print("real cancel fixture before packets: captures={d} closes={d} live={d}\n", .{ captured, closed, live });
            // Native UNION plans may retain materialized portal output after
            // storage captures retire. Count both statement and single-read
            // owners and assert their actual conservation, not an assumed cut.
            if (streaming) {
                try std.testing.expectEqual(@as(usize, 2), captured);
                try std.testing.expectEqual(@as(usize, 2), live);
                try std.testing.expectEqual(@as(usize, 0), closed);
            }
            try std.testing.expect(captured >= 2);
            try std.testing.expectEqual(captured, closed + live);
            try clients[2].?.send('Q', "SELECT id FROM usage_records\x00");
            try waitParkedOrQueued(&api, true);
            // All six authenticated slots are occupied. CancelRequest uses
            // the protected unauthenticated handshake path and exact key pair.
            const canceled_at = std.Io.Clock.awake.now(std.testing.io);
            if (!deadline_mode) {
                try clients[2].?.cancelQuery(port, .wrong);
                try waitParkedOrQueued(&api, true);
                try std.testing.expectEqual(captured, native_table.captures.load(.acquire));
                try clients[2].?.cancelQuery(port, .valid);
            }
            try clients[2].?.expectError("57014");
            // Distinguish the packet's cancellation from the five-second
            // admission ceiling. This is a scoped native timing observation.
            try std.testing.expect(std.Io.Clock.awake.now(std.testing.io).nanoseconds - canceled_at.nanoseconds < 2 * std.time.ns_per_s);
            try waitParkedOrQueued(&api, false);
            try std.testing.expectEqual(captured, native_table.captures.load(.acquire));
            std.debug.print("real cancel fixture after cancellation: captures={d} closes={d} live={d}\n", .{ native_table.captures.load(.acquire), native_table.closes.load(.acquire), native_table.live_captures.load(.acquire) });
            try std.testing.expectEqual(closed, native_table.closes.load(.acquire));
            try std.testing.expectEqual(live, native_table.live_captures.load(.acquire));
            try std.testing.expectEqual(captured, native_table.closes.load(.acquire) + native_table.live_captures.load(.acquire));
            // Reuse the canceled session's queue capacity and connection while
            // the other sessions retain their original native parameters.
            if (deadline_mode) try clients[2].?.command("RESET statement_timeout", "RESET\x00");
            try clients[2].?.send('Q', "SELECT 1\x00");
            try waitParkedOrQueued(&api, true);
            try clients[0].?.closePortal();
            try clients[2].?.expectScalar();
            if (streaming) {
                var remaining: [301]i64 = undefined;
                remaining[0] = 8;
                for (remaining[1..], 0..) |*value, index| value.* = @intCast(index + 101);
                try clients[1].?.finishIntegerPortal(&remaining);
                try waitNoQueries(&api);
                try clients[2].?.suspendPortalQueryExpected('0', stream_query, '4');
                remaining[0] = 7;
                for (remaining[1..], 0..) |*value, index| value.* = @intCast(index + 100);
                try clients[2].?.finishIntegerPortal(&remaining);
            } else {
                try clients[1].?.finishPortal('5');
                try waitNoQueries(&api);
                try clients[2].?.suspendPortalQuery('7', native_query);
                try clients[2].?.finishPortal('8');
            }
            try waitNoQueries(&api);
            try std.testing.expectEqual(@as(usize, 0), native_table.live_captures.load(.acquire));
            try std.testing.expectEqual(native_table.captures.load(.acquire), native_table.closes.load(.acquire));
        }
        if (mode == .native_refusals) {
            inline for (.{
                .{ NativeTable.FaultStage.capture, NativeTable.FaultReason.memory, "53200" },
                .{ NativeTable.FaultStage.page, NativeTable.FaultReason.canceled, "57014" },
                .{ NativeTable.FaultStage.after_page, NativeTable.FaultReason.unavailable, "53300" },
            }) |fault| {
                const before = native_table.captures.load(.acquire);
                const refusals = native_table.refusals.load(.acquire);
                const pages = native_table.pages.load(.acquire);
                native_table.refuseNext(fault[0], fault[1]);
                try clients[0].?.expectQueryError("SELECT id FROM usage_records", fault[2]);
                try waitNoQueries(&api);
                try std.testing.expectEqual(before + 1, native_table.captures.load(.acquire));
                try std.testing.expectEqual(before + 1, native_table.closes.load(.acquire));
                try std.testing.expectEqual(@as(usize, 0), native_table.live_captures.load(.acquire));
                try std.testing.expectEqual(refusals + 1, native_table.refusals.load(.acquire));
                try std.testing.expectEqual(@as(u8, 0), native_table.fault.load(.acquire));
                if (fault[0] == .after_page) try std.testing.expect(native_table.pages.load(.acquire) > pages);
                // A fresh query uses the same authenticated TCP session. The
                // failed operation produced no rows/completion and was not replayed.
                try clients[0].?.scalar();
                try waitNoQueries(&api);
            }
            try std.testing.expect(native_table.nested_calls.load(.acquire) != 0);
            try portalProgress(&api, clients, true);
            try std.testing.expectEqual(@as(usize, 0), native_table.live_captures.load(.acquire));
        }
        if (mode == .native_portals) {
            try std.testing.expect(NativeTable.boundary_calls.load(.acquire) != 0);
            // Repeat with both native portals retained, then disconnect one
            // session instead of issuing protocol Close. The other portal's
            // statement and bound parameter must survive the cleanup.
            try clients[0].?.suspendPreparedPortal('7');
            try clients[1].?.suspendPreparedPortal('4');
            try waitParkedOrQueued(&api, false);
            try clients[2].?.send('Q', "SELECT 1\x00");
            try waitParkedOrQueued(&api, true);
            clients[0].?.close();
            clients[0] = null;
            try clients[2].?.expectScalar();
            try clients[1].?.finishPortal('5');
            try waitNoQueries(&api);
        }
        // Six idle sessions exceed the two query permits, yet every connection
        // can use the same bounded executor and release its permit afterward.
        for (clients) |client| if (client) |value| {
            try value.scalar();
            try waitNoQueries(&api);
        };
    }
    try waitNoQueries(&api);
    // The opaque producer must claim the host's shared physical roles, rather
    // than alias raw general IO or create a private pool per connection.
    try std.testing.expectEqual(@as(usize, 12), runtime.ptr().sharedSqlStats(.execution).retained_capacity);
    const bounded_workers = runtime.ptr().sharedSqlStats(.bounded_read);
    try std.testing.expectEqual(@as(usize, 1), bounded_workers.retained_capacity);
    try std.testing.expectEqual(@as(usize, 1), bounded_workers.borrowers);
    api.deinit();
    api_live = false;
    try std.testing.expectEqual(@as(usize, 0), runtime.ptr().laneStats().reserved_workers);
    try std.testing.expectEqual(@as(usize, 0), runtime.ptr().laneStats().worker_active_leases);
    const address: std.Io.net.IpAddress = .{ .ip4 = .loopback(port) };
    var rebound = try address.listen(std.testing.io, .{ .reuse_address = true });
    defer rebound.deinit(std.testing.io);
}
