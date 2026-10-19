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

const std = @import("std");
const ApiHttpServer = @import("http_server.zig").ApiHttpServer;
const metadata_api = @import("../metadata/api.zig");
const operation_contract = @import("antfly_local_sources").api_operation;
const system_catalog = @import("antfly_local_sources").system_catalog_domain;
const table_reads = @import("antfly_source_root").antfly_sources.table_reads;
const db_mod = @import("antfly_source_root").antfly_sources.selected_db;
const raft_mod = @import("../raft/mod.zig");
const usermgr = @import("../usermgr/mod.zig");
const casbin = @import("antfly_casbin");

const TestAuthManager = struct {
    store: usermgr.MemoryStore,
    policy_store: casbin.MemoryAdapter,
    manager: usermgr.UserManager,
};

fn initTestAuthManager(alloc: std.mem.Allocator) !TestAuthManager {
    return .{
        .store = usermgr.MemoryStore.init(alloc),
        .policy_store = casbin.MemoryAdapter.init(alloc),
        .manager = undefined,
    };
}

fn bindTestAuthManager(alloc: std.mem.Allocator, auth: *TestAuthManager) !void {
    auth.manager = try usermgr.UserManager.init(
        alloc,
        auth.store.iface(),
        try usermgr.initDefaultEnforcer(alloc, auth.policy_store.iface()),
    );
}

// The fixture seeds one non-replicated group before serving reads and never
// mutates it afterward. Its fixed authority certifies that cut while the real
// native statement fence is held. This exercises frozen-proof capture and
// validation; it does not qualify production Raft quorum behavior.
const FixedReadAuthority = struct {
    incarnation: u128,
    group_id: u64 = 7001,
    term: u64 = 1,
    applied_index: u64 = 1,
    captures: std.atomic.Value(usize) = .init(0),
    validations: std.atomic.Value(usize) = .init(0),
    const Barrier = raft_mod.read_gate.ReadSafetyBarrier;
    const Cancellation = operation_contract.CancellationToken;
    const Port = @import("antfly_local_sources").common_read_admission.Port;

    fn barrier(self: *FixedReadAuthority) Barrier {
        return .{ .ptr = self, .vtable = &.{ .wait_read_safe = wait, .wait_read_safe_with_execution = waitWithExecution, .capture_frozen = capture, .validate_frozen = validate, .validate_frozen_with_admission = validateWithAdmission, .validate_retained_snapshot = validateRetained } };
    }
    fn checkGroup(self: *FixedReadAuthority, group: u64) !void {
        if (group != self.group_id or self.incarnation == 0) return error.NotLeader;
    }
    fn wait(raw: *anyopaque, group: u64, _: []const u8) !void {
        const self: *FixedReadAuthority = @ptrCast(@alignCast(raw));
        try self.checkGroup(group);
    }
    fn waitWithExecution(raw: *anyopaque, group: u64, context: []const u8, _: @import("antfly_local_sources").common_execution_wait.Boundary) !void {
        try wait(raw, group, context);
    }
    fn capture(raw: *anyopaque, group: u64) !Barrier.FrozenProof {
        const self: *FixedReadAuthority = @ptrCast(@alignCast(raw));
        try self.checkGroup(group);
        _ = self.captures.fetchAdd(1, .acq_rel);
        return .{ .incarnation = self.incarnation, .term = self.term, .applied_index = self.applied_index };
    }
    fn check(deadline: ?u64, cancellation: Cancellation) !void {
        if (cancellation.isCancelled()) return error.Canceled;
        if (deadline) |until| if (@import("antfly_platform").time.monotonicNs() >= until) return error.DeadlineExceeded;
    }
    fn validate(raw: *anyopaque, group: u64, proof: Barrier.FrozenProof, deadline: ?u64, cancellation: Cancellation) !void {
        const self: *FixedReadAuthority = @ptrCast(@alignCast(raw));
        try self.checkGroup(group);
        try check(deadline, cancellation);
        try proof.validate(self.incarnation, self.term, true, self.applied_index);
        _ = self.validations.fetchAdd(1, .acq_rel);
    }
    fn validateWithAdmission(raw: *anyopaque, group: u64, proof: Barrier.FrozenProof, deadline: ?u64, cancellation: Cancellation, control: Port) !void {
        try control.checkpoint();
        try validate(raw, group, proof, deadline, cancellation);
    }
    fn validateRetained(raw: *anyopaque, group: u64, proof: Barrier.FrozenProof, deadline: ?u64, cancellation: Cancellation, control: ?Port) !void {
        if (control) |port| try port.checkpoint();
        const self: *FixedReadAuthority = @ptrCast(@alignCast(raw));
        try self.checkGroup(group);
        try check(deadline, cancellation);
        try proof.validateRetained(self.incarnation, self.term, true, self.applied_index, self.applied_index);
        _ = self.validations.fetchAdd(1, .acq_rel);
    }
};

// CatalogBackedGroupRouter deliberately cannot claim that a catalog placement
// is actually hosted locally. This fixed, nonreplicated fixture has a real
// native owner and the explicit authority above; expose only that one local
// group. Unknown groups have no route. This is not a Raft leader assertion.
const FixedLocalReadRouter = struct {
    owners: *@import("kernel_owner_source.zig").ProvisionedKernelOwnerSource,
    authority: *FixedReadAuthority,
    node_id: u64,

    fn router(self: *FixedLocalReadRouter) @import("table_router.zig").HostedGroupRouter {
        return .{ .ptr = self, .vtable = &.{ .local_node_id = localNodeId, .local_status = localStatus, .group_leader_node_id = leader, .node_base_uri = noRemoteUri } };
    }
    fn localNodeId(raw: *anyopaque) u64 {
        const self: *FixedLocalReadRouter = @ptrCast(@alignCast(raw));
        return self.node_id;
    }
    fn localStatus(raw: *anyopaque, group: u64) @import("../raft/host.zig").HostedReplicaStatus {
        const self: *FixedLocalReadRouter = @ptrCast(@alignCast(raw));
        if (group != self.authority.group_id or self.authority.incarnation == 0 or self.owners.ownerCountForTest() != 1) return .absent;
        return .active;
    }
    fn leader(raw: *anyopaque, group: u64) ?u64 {
        if (localStatus(raw, group) != .active) return null;
        return localNodeId(raw);
    }
    fn noRemoteUri(_: *anyopaque, _: std.mem.Allocator, _: u64) !?[]u8 {
        return null;
    }
};

// This uses the actual SQL adapter, provisioned routing and native owner source.
// Keep it focused so extending it to remote routing does not run the SQL corpus.
test "SQL provisioned owned stream parks and retires native storage capture" {
    try qualify(.local);
}

test "SQL provisioned owned stream retains signed remote capture and endpoint binding" {
    try qualify(.remote);
}

test "SQL provisioned owned stream releases local admission while real TCP replies are lost" {
    try qualify(.response_loss);
}

test "SQL provisioned owned stream HTTP worker releases input and admission while TCP replies are lost" {
    try qualify(.http_response_loss);
}

const HttpResult = struct {
    status: u16,
    body: []u8,
};

// The HTTP context, authenticated identity and request body are destroyed
// before the caller checks remote uncertainty or restores any peer replies.
fn executeHttpSql(alloc: std.mem.Allocator, server: *ApiHttpServer, statement: []const u8, parameters: []const std.json.Value) !HttpResult {
    const httpx = @import("httpx");
    const body = try std.json.Stringify.valueAlloc(alloc, .{ .statement = statement, .parameters = parameters, .limit = 2 }, .{});
    defer alloc.free(body);
    var request = try httpx.Request.init(alloc, .POST, "http://localhost/db/v1/sql");
    defer request.deinit();
    const credentials = "pool-reader:secret";
    var encoded: [std.base64.standard.Encoder.calcSize(credentials.len)]u8 = undefined;
    const authorization = try std.fmt.allocPrint(alloc, "Basic {s}", .{std.base64.standard.Encoder.encode(&encoded, credentials)});
    defer alloc.free(authorization);
    try request.headers.append("authorization", authorization);
    request.body = body;
    var context = httpx.Context.init(alloc, std.testing.io, &request);
    defer context.deinit();
    var handler: @import("httpx_handler.zig").AntflyApiHandler = .{ .api_server = server };
    var response = try handler.executeSQL(&context);
    defer response.deinit();
    return .{ .status = response.status.code, .body = try alloc.dupe(u8, response.body orelse return error.TestMissingSqlHttpResponse) };
}

test "SQL provisioned owned stream HTTP transport retires context before remote uncertainty" {
    try qualify(.http_transport_response_loss);
}

fn executeHttpSqlTcp(alloc: std.mem.Allocator, executor: @import("antfly_local_sources").common_http_http_common.RequestExecutor, base_uri: []const u8, statement: []const u8, parameters: []const std.json.Value) !HttpResult {
    const body = try std.json.Stringify.valueAlloc(alloc, .{ .statement = statement, .parameters = parameters, .limit = 2 }, .{});
    defer alloc.free(body);
    const uri = try std.fmt.allocPrint(alloc, "{s}/db/v1/sql", .{base_uri});
    defer alloc.free(uri);
    const credentials = "pool-reader:secret";
    var encoded: [std.base64.standard.Encoder.calcSize(credentials.len)]u8 = undefined;
    const authorization = try std.fmt.allocPrint(alloc, "Basic {s}", .{std.base64.standard.Encoder.encode(&encoded, credentials)});
    defer alloc.free(authorization);
    var response = try executor.execute(alloc, .{ .method = .POST, .uri = uri, .body = body, .headers = &.{.{ .name = "authorization", .value = authorization }}, .content_type = "application/json", .timeout_ms = 10000, .max_response_bytes = 64 << 10 });
    defer response.deinit(alloc);
    return .{ .status = response.status, .body = try alloc.dupe(u8, response.body) };
}

// A received response can precede the listener's final context destructor.
// Wait for its query roots to retire before asserting no request storage is
// needed by remote cleanup. This does not restore the lost peer replies.
fn waitNoHttpSqlRoots(server: *ApiHttpServer) !void {
    const scheduler = if (server.runnable_scheduler) |*value| value else return error.TestMissingScheduler;
    const clock = @import("antfly_platform").time;
    const until = clock.monotonicNs() + 5 * std.time.ns_per_s;
    while (true) {
        var queries: usize = 0;
        while (!scheduler.mutex.tryLock()) std.atomic.spinLoopHint();
        var entry = scheduler.entries;
        while (entry) |current| : (entry = current.all_next) {
            if (current.class == .query) queries += 1;
        }
        scheduler.mutex.unlock();
        if (queries == 0 and server.queryAdmissionStats().in_flight == 0) return;
        if (clock.monotonicNs() >= until) return error.TestHttpContextRetirementTimeout;
        try std.testing.io.sleep(.fromMilliseconds(1), .awake);
    }
}

test "SQL provisioned owned stream isolates healthy remote reads from a full uncertain destination" {
    try qualify(.healthy_remote_isolation);
}

test "SQL provisioned owned stream retires unknown native capture after its TCP reply is lost" {
    try qualify(.capture_response_loss);
}

test "SQL provisioned owned stream retires unknown native cursor after its TCP reply is lost" {
    try qualify(.open_response_loss);
}

test "SQL provisioned owned stream never replays a native page after its TCP reply is lost" {
    try qualify(.page_response_loss);
}

test "SQL provisioned owned stream serves local HTTP and pgwire while remote recovery is closed" {
    try qualify(.recovering_local);
}

test "SQL provisioned owned stream keeps local reads independent of full control metadata" {
    try qualify(.recovery_metadata_pressure);
}

test "SQL provisioned owned stream refuses remote metadata pressure before publication and recovers" {
    try qualify(.ready_remote_metadata_pressure);
}

test "SQL provisioned owned stream reports temporary remote readiness refusal and recovers without replay" {
    try qualify(.remote_readiness);
}

const Mode = enum { local, remote_readiness, remote, response_loss, http_response_loss, http_transport_response_loss, healthy_remote_isolation, capture_response_loss, open_response_loss, page_response_loss, recovering_local, recovery_metadata_pressure, ready_remote_metadata_pressure };
fn qualify(comptime mode: Mode) !void {
    const remote = mode != .local;
    const remote_readiness = mode == .remote_readiness;
    const ready_metadata_pressure = mode == .ready_remote_metadata_pressure;
    const metadata_pressure = mode == .recovery_metadata_pressure or ready_metadata_pressure;
    const recovering = mode == .recovering_local or mode == .recovery_metadata_pressure;
    const healthy = mode == .healthy_remote_isolation;
    const phase_loss = mode == .capture_response_loss or mode == .open_response_loss or mode == .page_response_loss;
    const http_transport = mode == .http_transport_response_loss or healthy or phase_loss or ready_metadata_pressure or remote_readiness;
    const http_loss = mode == .http_response_loss or http_transport or recovering;
    const response_loss = !recovering and !ready_metadata_pressure and !remote_readiness and (mode == .response_loss or http_loss);
    const realm = "0123456789abcdef0123456789abcdef".*;
    const secret = "0123456789abcdef0123456789abcdef";
    const members = @import("../common/remote_membership.zig");
    const metadata_records = @import("../metadata/table_manager.zig");
    const initialization = @import("../metadata/remote_attempt_initialization.zig");
    const alloc = std.testing.allocator;
    const FixtureLifetime = struct {
        references: std.atomic.Value(usize) = .init(1),

        fn retain(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const previous = self.references.fetchAdd(1, .monotonic);
            std.debug.assert(previous != 0 and previous != std.math.maxInt(usize));
        }

        fn release(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            // The decrement is the callback's last access to its owner.
            const previous = self.references.fetchSub(1, .acq_rel);
            std.debug.assert(previous > 1);
        }

        fn drain(self: *@This()) void {
            const io = std.testing.io;
            const protection = io.swapCancelProtection(.blocked);
            defer _ = io.swapCancelProtection(protection);
            while (self.references.load(.acquire) != 1)
                io.sleep(.fromMilliseconds(1), .awake) catch unreachable;
        }
    };
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"name":{"type":"keyword"},"amount":{"type":"integer"},"quantity":{"type":"integer"},"status":{"type":"keyword"},"enabled":{"type":"boolean"},"customer_id":{"type":"integer"},"tenant_id":{"type":"integer"},"created_at":{"type":"keyword"},"metadata":{"type":"object","properties":{"source":{"type":"keyword"}},"additionalProperties":false}},"additionalProperties":false}}}}
    ;
    const Source = struct {
        schema: []const u8 = schema_json,
        records: [if (healthy) 2 else 1]@import("antfly_local_sources").common_topology_records.TableRecord = if (healthy)
            .{ .{ .table_id = 7, .name = "usage_records", .schema_json = schema_json, .indexes_json = "{}" }, .{ .table_id = 8, .name = "healthy_records", .schema_json = schema_json, .indexes_json = "{}" } }
        else
            .{.{ .table_id = 7, .name = "usage_records", .schema_json = schema_json, .indexes_json = "{}" }},
        ranges: [if (healthy) 2 else 1]@import("antfly_local_sources").common_topology_records.RangeRecord = if (healthy)
            .{ .{ .table_id = 7, .group_id = 7001, .range_id = 1, .start_key = "", .end_key = null }, .{ .table_id = 8, .group_id = 7002, .range_id = 2, .start_key = "", .end_key = null } }
        else
            .{.{ .table_id = 7, .group_id = 7001, .range_id = 1, .start_key = "", .end_key = null }},
        nodes: []metadata_records.NodeRecord = &.{},
        stores: []metadata_records.StoreRecord = &.{},
        placements: []@import("../raft/reconciler.zig").PlacementIntent = &.{},
        initializations: [if (healthy) 3 else 2]?initialization.Entry = @splat(null),
        calls: usize = 0,
        calls_mutex: std.atomic.Mutex = .unlocked,
        replace_after_open: bool = false,
        fn findRecord(self: *const @This(), name: []const u8) ?@import("antfly_local_sources").common_topology_records.TableRecord {
            for (self.records) |record| if (std.mem.eql(u8, record.name, name)) return record;
            return null;
        }
        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metadata_epoch = 7, .metadata_incarnation = "0123456789abcdef0123456789abcdef".*, .metrics = .{} };
        }
        fn catalog(ptr: *anyopaque, a: std.mem.Allocator, context: operation_contract.RequestContext, input: @import("../system_catalog/server_call.zig").Call) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try context.ensureActive();
            while (!self.calls_mutex.tryLock()) std.atomic.spinLoopHint();
            defer self.calls_mutex.unlock();
            if (input == .remote_attempt_initialization) {
                const command = input.remote_attempt_initialization;
                try command.validate();
                if (!std.mem.eql(u8, &command.identity.metadata_incarnation, &realm)) return error.StoreRootEnrollmentChanged;
                const index = for (self.stores, 0..) |store, index| {
                    if (store.node_id == command.identity.node_id and store.store_id == command.identity.store_id) break index;
                } else return error.StoreRootEnrollmentChanged;
                const store = self.stores[index];
                if (store.replica_root_incarnation != command.identity.root_incarnation or
                    store.reporter_incarnation != command.reporter_incarnation or
                    !std.mem.eql(u8, &store.replica_root_public_key, &command.identity.public_key)) return error.StoreRootEnrollmentChanged;
                if (command.action == .status) return std.json.Stringify.valueAlloc(a, self.initializations[index] orelse return error.CatalogNotFound, .{});
                self.initializations[index] = try initialization.transition(self.initializations[index], command);
                return std.json.Stringify.valueAlloc(a, self.initializations[index].?, .{});
            }
            // This fixture has no row-policy publication. The production
            // admission path treats that absence as an unprotected table.
            if (input == .policy_publication_status) return a.dupe(u8, "null");
            if (input == .setting_snapshot) return std.json.Stringify.valueAlloc(a, .{ .scope = input.setting_snapshot, .epoch = @as(u64, 1), .definitions = &.{} }, .{});
            if (input == .read and input.read.kind == .namespace) return a.dupe(u8, "{}");
            self.calls += 1;
            if (self.replace_after_open and self.calls == 3) return error.CatalogGenerationChanged;
            if (input == .write_validation) return std.json.Stringify.valueAlloc(a, .{ .schema_json = self.schema }, .{});
            if (input != .resolve_many) return error.UnexpectedCatalogCall;
            if (input.resolve_many.expected_revision) |revision| if (revision != 7) return error.CatalogGenerationChanged;
            if (input.resolve_many.storage_names.len != 0) {
                if (input.resolve_many.storage_names.len != 1) return error.UnexpectedCatalogCall;
                const record = self.findRecord(input.resolve_many.storage_names[0]) orelse return error.UnexpectedCatalogCall;
                const name = try (system_catalog.Target{ .table = record.name }).resourceNameAlloc(a);
                defer a.free(name);
                return std.json.Stringify.valueAlloc(a, system_catalog.ResolvedMany{ .revision = 7, .tables = &.{}, .logical_names = &.{name} }, .{});
            }
            if (input.resolve_many.targets.len == 0) return std.json.Stringify.valueAlloc(a, system_catalog.ResolvedMany{ .revision = 7, .tables = &.{} }, .{});
            if (input.resolve_many.targets.len != 1) return error.UnexpectedCatalogCall;
            const record = self.findRecord(input.resolve_many.targets[0].table) orelse return error.UnexpectedCatalogCall;
            const tables = [_]?system_catalog.ResolvedTable{.{ .table_id = record.table_id, .name = record.name, .query_definition = if (input.resolve_many.include_query_definitions) .{ .table_id = record.table_id, .schema_json = self.schema, .read_schema_json = "", .indexes_json = "{}" } else null }};
            return std.json.Stringify.valueAlloc(a, system_catalog.ResolvedMany{ .revision = 7, .tables = &tables }, .{});
        }
        fn snapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            while (!self.calls_mutex.tryLock()) std.atomic.spinLoopHint();
            defer self.calls_mutex.unlock();
            const nodes = try std.testing.allocator.dupe(metadata_records.NodeRecord, self.nodes);
            errdefer std.testing.allocator.free(nodes);
            const stores = try std.testing.allocator.dupe(metadata_records.StoreRecord, self.stores);
            return .{ .status = try status(ptr), .nodes = nodes, .stores = stores, .tables = &self.records, .ranges = &self.ranges, .placement_intents = self.placements, .split_transitions = &.{}, .merge_transitions = &.{} };
        }
        fn validate(ptr: *anyopaque, contract: metadata_api.CatalogPublicationContract) !bool {
            var current = try snapshot(ptr);
            defer freeSnapshot(ptr, &current);
            return contract.matches(&current);
        }
        fn routingSnapshot(ptr: *anyopaque, deadline_ns: ?u64) !metadata_api.CatalogRoutingSnapshot {
            const Adapter = @import("table_catalog.zig").TestAdminRoutingAdapter(snapshot, freeSnapshot);
            var value = try Adapter.routingSnapshot(ptr, deadline_ns);
            value.catalog_revision = 7;
            value.change_token.revision = 7;
            return value;
        }
        fn tableSnapshot(ptr: *anyopaque, table: []const u8, deadline_ns: ?u64) !metadata_api.CatalogRoutingSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.findRecord(table) == null) {
                try (@import("table_catalog.zig").RoutingBudget{ .deadline_ns = deadline_ns }).checkpoint();
                return .{ .metadata_group_id = 1, .metadata_incarnation = (try status(ptr)).metadata_incarnation, .catalog_revision = 7, .change_token = .{ .metadata_group_id = 1, .metadata_incarnation = (try status(ptr)).metadata_incarnation, .revision = 7 }, .tables = &.{}, .ranges = &.{} };
            }
            // This fixture has at most two tables/ranges; clone work stays bounded.
            return routingSnapshot(ptr, deadline_ns);
        }
        fn routingSource(self: *@This()) @import("table_catalog.zig").CatalogSource {
            const Adapter = @import("table_catalog.zig").TestAdminRoutingAdapter(snapshot, freeSnapshot);
            return .{ .ptr = self, .vtable = &.{
                .admin_snapshot = snapshot,
                .free_admin_snapshot = freeSnapshot,
                .routing_snapshot = routingSnapshot,
                .linearizable_routing_snapshot = routingSnapshot,
                .table_routing_snapshot = tableSnapshot,
                .linearizable_table_routing_snapshot = tableSnapshot,
                .free_routing_snapshot = Adapter.freeRoutingSnapshot,
                .validate_publication = validate,
            } };
        }
        fn freeSnapshot(_: *anyopaque, value: *metadata_api.AdminSnapshot) void {
            std.testing.allocator.free(value.nodes);
            std.testing.allocator.free(value.stores);
        }
        fn linearizable(ptr: *anyopaque, request: operation_contract.RequestContext) !?metadata_api.AdminSnapshot {
            try request.ensureActive();
            return try snapshot(ptr);
        }
    };

    var directory = try @import("antfly_local_sources").common_test_directory.TestDirectory.init("antfly-provisioned-sql-pool");
    defer directory.cleanup();
    const TestDirectory = @import("antfly_local_sources").common_test_directory.TestDirectory;
    var coordinator_directory: ?TestDirectory = if (remote) try TestDirectory.init("antfly-sql-remote-coordinator") else null;
    defer if (coordinator_directory) |*value| value.cleanup();
    var healthy_directory: ?TestDirectory = if (healthy) try TestDirectory.init("antfly-sql-healthy-remote") else null;
    defer if (healthy_directory) |*value| value.cleanup();
    var source: Source = .{};
    var nodes: [if (healthy) 3 else 2]metadata_records.NodeRecord = if (healthy) .{ .{ .node_id = 7 }, .{ .node_id = 9 }, .{ .node_id = 10 } } else .{ .{ .node_id = 7 }, .{ .node_id = 9 } };
    var stores: [if (healthy) 3 else 2]metadata_records.StoreRecord = undefined;
    var placements: [if (healthy) 2 else 1]@import("../raft/reconciler.zig").PlacementIntent = undefined;
    placements[0] = if (recovering)
        .{ .record = .{ .group_id = 7001, .replica_id = 7, .local_node_id = 7 }, .store_id = 11, .peer_node_ids = &.{7} }
    else
        .{ .record = .{ .group_id = 7001, .replica_id = 9, .local_node_id = 9 }, .store_id = 22, .peer_node_ids = &.{9} };
    if (healthy) placements[1] = .{ .record = .{ .group_id = 7002, .replica_id = 10, .local_node_id = 10 }, .store_id = 33, .peer_node_ids = &.{10} };
    const locals: [if (healthy) 3 else 2]members.Local = if (remote) enrolled: {
        const signing = @import("antfly_local_sources").storage_db_root_signing_identity;
        const coordinator = try signing.loadOrCreate(alloc, std.testing.io, coordinator_directory.?.path());
        const receiver = try signing.loadOrCreate(alloc, std.testing.io, directory.path());
        const healthy_root: ?signing.State = if (healthy) try signing.loadOrCreate(alloc, std.testing.io, healthy_directory.?.path()) else null;
        stores[0] = .{ .node_id = 7, .store_id = 11, .reporter_incarnation = 101, .replica_root_incarnation = coordinator.root_incarnation, .replica_root_public_key = coordinator.public_key, .api_url = "http://127.0.0.1:1" };
        stores[1] = .{ .node_id = 9, .store_id = 22, .reporter_incarnation = 202, .replica_root_incarnation = receiver.root_incarnation, .replica_root_public_key = receiver.public_key, .api_url = "http://127.0.0.1:1" };
        if (healthy) stores[2] = .{ .node_id = 10, .store_id = 33, .reporter_incarnation = 303, .replica_root_incarnation = healthy_root.?.root_incarnation, .replica_root_public_key = healthy_root.?.public_key, .api_url = "http://127.0.0.1:1" };
        source.nodes = &nodes;
        source.stores = &stores;
        source.placements = &placements;
        var result: [if (healthy) 3 else 2]members.Local = undefined;
        result[0] = .{ .metadata_incarnation = realm, .node_id = 7, .store_id = 11, .reporter_incarnation = 101, .root_incarnation = coordinator.root_incarnation };
        result[1] = .{ .metadata_incarnation = realm, .node_id = 9, .store_id = 22, .reporter_incarnation = 202, .root_incarnation = receiver.root_incarnation };
        if (healthy) result[2] = .{ .metadata_incarnation = realm, .node_id = 10, .store_id = 33, .reporter_incarnation = 303, .root_incarnation = healthy_root.?.root_incarnation };
        break :enrolled result;
    } else undefined;
    var receiver_authority: FixedReadAuthority = .{ .incarnation = if (remote) locals[1].root_incarnation else 1 };
    var healthy_authority: FixedReadAuthority = .{ .incarnation = if (healthy) locals[2].root_incarnation else 1, .group_id = 7002 };
    const healthy_barrier = healthy_authority.barrier();
    var coordinator_authority: FixedReadAuthority = .{ .incarnation = if (remote) locals[0].root_incarnation else 1 };
    const receiver_barrier = if (remote) receiver_authority.barrier() else raft_mod.read_gate.alreadyReadSafeBarrier();
    const coordinator_barrier = if (remote) coordinator_authority.barrier() else raft_mod.read_gate.alreadyReadSafeBarrier();
    var owners = @import("kernel_owner_source.zig").ProvisionedKernelOwnerSource.init(alloc, directory.path(), source.routingSource(), receiver_barrier);
    defer owners.deinit();
    _ = try owners.reconcileTableGroup(7001, "usage_records");
    _ = (try owners.writeSource().batchGroupLocal(alloc, 7001, "usage_records", .{
        .writes = if (remote) &.{
            .{ .key = "exact", .value = "{\"id\":9007199254740993,\"name\":\"exact\"}" },
            .{ .key = "second", .value = "{\"id\":9007199254740993,\"name\":\"exact\"}" },
        } else &.{.{ .key = "exact", .value = "{\"id\":9007199254740993,\"name\":\"exact\"}" }},
        .timestamp_ns = 42,
    })) orelse return error.ExpectedNativeWrite;
    var healthy_owners: ?@import("kernel_owner_source.zig").ProvisionedKernelOwnerSource = if (healthy) @import("kernel_owner_source.zig").ProvisionedKernelOwnerSource.init(alloc, healthy_directory.?.path(), source.routingSource(), healthy_barrier) else null;
    defer if (healthy_owners) |*value| value.deinit();
    if (healthy) {
        _ = try healthy_owners.?.reconcileTableGroup(7002, "healthy_records");
        _ = (try healthy_owners.?.writeSource().batchGroupLocal(alloc, 7002, "healthy_records", .{ .writes = &.{.{ .key = "healthy", .value = "{\"id\":9007199254740993,\"name\":\"healthy\"}" }}, .timestamp_ns = 42 })) orelse return error.ExpectedNativeWrite;
    }
    var backend_runtime = try db_mod.background_runtime.BackendRuntimeHandle.init(alloc, .{ .backend = .io_threaded });
    defer backend_runtime.deinit();
    const owner_source = @import("kernel_owner_source.zig").ProvisionedKernelOwnerSource;
    var coordinator_owners: ?owner_source = if (remote) owner_source.init(alloc, coordinator_directory.?.path(), source.routingSource(), coordinator_barrier) else null;
    defer if (coordinator_owners) |*value| value.deinit();
    if (recovering) {
        _ = try coordinator_owners.?.reconcileTableGroup(7001, "usage_records");
        _ = (try coordinator_owners.?.writeSource().batchGroupLocal(alloc, 7001, "usage_records", .{
            .writes = &.{ .{ .key = "exact", .value = "{\"id\":9007199254740993,\"name\":\"exact\"}" }, .{ .key = "second", .value = "{\"id\":9007199254740993,\"name\":\"exact\"}" } },
            .timestamp_ns = 42,
        })) orelse return error.ExpectedNativeWrite;
    }
    var receiver_reads = table_reads.ProvisionedTableReadSource.init(directory.path(), source.routingSource(), receiver_barrier);
    _ = receiver_reads.withLocalReadSource(owners.readSource());
    receiver_reads.backend_runtime = backend_runtime.ptr();
    var healthy_reads: ?table_reads.ProvisionedTableReadSource = if (healthy) table_reads.ProvisionedTableReadSource.init(healthy_directory.?.path(), source.routingSource(), healthy_barrier) else null;
    if (healthy_reads) |*value| {
        _ = value.withLocalReadSource(healthy_owners.?.readSource());
        value.backend_runtime = backend_runtime.ptr();
    }
    var reads = table_reads.ProvisionedTableReadSource.init(if (remote) coordinator_directory.?.path() else directory.path(), source.routingSource(), coordinator_barrier);
    _ = reads.withLocalReadSource(if (remote) coordinator_owners.?.readSource() else owners.readSource());
    reads.backend_runtime = backend_runtime.ptr();
    var router = @import("table_router.zig").CatalogBackedGroupRouter.init(source.routingSource(), 7);
    var local_router: FixedLocalReadRouter = .{ .owners = if (remote) &coordinator_owners.? else &owners, .authority = &coordinator_authority, .node_id = 7 };
    const selected_router = if (recovering) local_router.router() else router.router();
    if (recovering) {
        // Verify the fixed local authority before testing recovery isolation;
        // a placement-only router must never fabricate local hosting.
        var route = (try @import("table_router.zig").resolveGroupRoute(alloc, source.routingSource(), selected_router, 7001, .prefer_leader)) orelse return error.TestMissingLocalReadAuthority;
        defer route.deinit(alloc);
        try std.testing.expect(route == .local);
    }
    var transport = @import("../common/http/std_http_executor.zig").StdHttpExecutor.init(alloc, .{});
    defer transport.deinit();
    if (remote) _ = reads.withDistributedRouting(selected_router, transport.executor(), secret, "cluster");
    const status_source: @import("http_server.zig").StatusSource = .{ .ptr = &source, .routing = try source.routingSource().routingSource(), .vtable = &.{ .status = Source.status, .system_catalog = Source.catalog, .supports_query_definitions = true, .admin_snapshot = Source.snapshot, .linearizable_snapshot = Source.linearizable, .free_admin_snapshot = Source.freeSnapshot } };
    var config: @import("http_server.zig").ApiHttpServerConfig = .{
        .backend_runtime = backend_runtime.ptr(),
        .auth_enabled = http_loss,
        .request_memory_bytes = 16 << 20,
        .query_max_concurrent_requests = if (response_loss) 1 else 4,
        .runnable_policy = .{ .slots = 4, .cleanup_slots = 1, .read_isolation = true, .checkpoint_budget = 1, .max_wait_ms = 5000, .max_bytes = @import("antfly_local_sources").common_config.default_runnable_policy.max_bytes, .control_bytes = @import("antfly_local_sources").common_config.default_runnable_policy.control_bytes },
        .query_waiting = .{ .max_queued_requests = 4, .max_queued_bytes = 65536, .max_wait_ms = 5000 },
    };
    if (remote) {
        config.retained_memory_bytes = 16 << 20;
        config.internal_service_secret = secret;
        config.internal_service_issuer = "cluster";
        config.remote_read_capabilities = .{ .group_query = true, .retained_read = true };
    }
    // Metadata snapshots borrow these URI bytes through both API joins.
    var receiver_uri: ?[]u8 = null;
    defer if (receiver_uri) |value| alloc.free(value);
    var coordinator_uri: ?[]u8 = null;
    defer if (coordinator_uri) |value| alloc.free(value);
    var healthy_uri: ?[]u8 = null;
    defer if (healthy_uri) |value| alloc.free(value);
    var receiver: ?ApiHttpServer = if (remote) try ApiHttpServer.initWithConfig(alloc, config, status_source, receiver_reads.source(), null) else null;
    defer if (receiver) |*value| value.deinit();
    var healthy_server: ?ApiHttpServer = if (healthy) try ApiHttpServer.initWithConfig(alloc, config, status_source, healthy_reads.?.source(), null) else null;
    defer if (healthy_server) |*value| value.deinit();
    var server = try ApiHttpServer.initWithConfig(alloc, config, status_source, reads.source(), null);
    defer server.deinit();
    const Runtime = @import("http_test_runtime.zig").Runtime;
    var fault: @import("http_response_loss_fixture.zig").ResponseLoss = .{ .phase = switch (mode) {
        .capture_response_loss => .capture,
        .open_response_loss => .open,
        .page_response_loss => .next,
        else => .all_replies,
    } };
    var receiver_listener: ?Runtime = if (response_loss) try Runtime.startOwnedWithMiddleware(alloc, &receiver.?, fault.middleware()) else if (remote) try Runtime.startOwned(alloc, &receiver.?) else null;
    defer if (receiver_listener) |*value| value.deinit();
    var healthy_listener: ?Runtime = if (healthy) try Runtime.startOwned(alloc, &healthy_server.?) else null;
    defer if (healthy_listener) |*value| value.deinit();
    var coordinator_listener: ?Runtime = if (remote) try Runtime.startOwned(alloc, &server) else null;
    defer if (coordinator_listener) |*value| value.deinit();
    if (remote) {
        receiver_uri = try receiver_listener.?.baseUri(alloc);
        coordinator_uri = try coordinator_listener.?.baseUri(alloc);
        if (healthy) healthy_uri = try healthy_listener.?.baseUri(alloc);
        while (!source.calls_mutex.tryLock()) std.atomic.spinLoopHint();
        stores[0].api_url = coordinator_uri.?;
        stores[1].api_url = receiver_uri.?;
        if (healthy) stores[2].api_url = healthy_uri.?;
        source.calls_mutex.unlock();
        const Bootstrap = @import("remote_attempt_owner.zig").Bootstrap;
        const policy: @import("remote_reconciliation.zig").Policy = .{ .interval_ms = 5, .pass_duration_ns = 250 * std.time.ns_per_ms, .rpc_timeout_ns = 100 * std.time.ns_per_ms };
        const coordinator_targets: @FieldType(Bootstrap, "targets") = if (healthy)
            &.{ .{ .local = locals[1], .base_uri = receiver_uri.?, .max_attempts = 4, .max_bytes = 2 << 20 }, .{ .local = locals[2], .base_uri = healthy_uri.?, .max_attempts = 4, .max_bytes = 2 << 20 } }
        else
            &.{.{ .local = locals[1], .base_uri = receiver_uri.?, .max_attempts = 4, .max_bytes = 2 << 20 }};
        try server.bootstrapRemoteAttempts(Bootstrap{ .root_path = coordinator_directory.?.path(), .local = locals[0], .targets = coordinator_targets, .max_outbound_attempts = if (healthy) 8 else 4, .max_outbound_bytes = if (healthy) 4 << 20 else 2 << 20, .reconciliation = policy });
        try receiver.?.bootstrapRemoteAttempts(Bootstrap{ .root_path = directory.path(), .local = locals[1], .targets = &.{.{ .local = locals[0], .base_uri = coordinator_uri.?, .max_attempts = 4, .max_bytes = 2 << 20 }}, .max_outbound_attempts = 4, .max_outbound_bytes = 2 << 20, .reconciliation = policy });
        if (healthy) try healthy_server.?.bootstrapRemoteAttempts(Bootstrap{ .root_path = healthy_directory.?.path(), .local = locals[2], .targets = &.{.{ .local = locals[0], .base_uri = coordinator_uri.?, .max_attempts = 4, .max_bytes = 2 << 20 }}, .max_outbound_attempts = 4, .max_outbound_bytes = 2 << 20, .reconciliation = policy });
        const clock = @import("antfly_platform").time;
        const until = clock.monotonicNs() + 10 * std.time.ns_per_s;
        while (true) {
            const coordinator_state = try server.remoteAttemptOwnerStats();
            const receiver_state = try receiver.?.remoteAttemptOwnerStats();
            const coordinator_client = server.remoteRetainedReadClient();
            const receiver_client = receiver.?.remoteRetainedReadClient();
            const healthy_ready = if (healthy) ready: {
                const healthy_state = try healthy_server.?.remoteAttemptOwnerStats();
                const healthy_client = healthy_server.?.remoteRetainedReadClient();
                if (healthy_client == null or coordinator_client == null) break :ready false;
                break :ready healthy_state.startup_fences_remaining == 0 and try healthy_client.?.supportsRead(0, true) and try coordinator_client.?.supportsRead(1, true);
            } else true;
            if (healthy_ready and coordinator_state.startup_fences_remaining == 0 and receiver_state.startup_fences_remaining == 0 and coordinator_client != null and receiver_client != null and try coordinator_client.?.supportsRead(0, true) and try receiver_client.?.supportsRead(0, true)) break;
            if (clock.monotonicNs() >= until) return error.TestOwnedTransportDiscoveryTimeout;
            try std.testing.io.sleep(.fromMilliseconds(1), .awake);
        }
        try expectRemoteOwnership(&server, &receiver.?, false);
    }
    var auth = try initTestAuthManager(alloc);
    defer auth.store.deinit();
    defer auth.policy_store.deinit();
    try bindTestAuthManager(alloc, &auth);
    defer auth.manager.deinit();
    var permission = try usermgr.Permission.initOwned(alloc, .table, "usage_records", .read);
    defer permission.deinit(alloc);
    var namespace_permission = try usermgr.Permission.initOwned(alloc, .namespace, "*", .read);
    defer namespace_permission.deinit(alloc);
    var healthy_permission: ?usermgr.Permission = if (healthy) try usermgr.Permission.initOwned(alloc, .table, "healthy_records", .read) else null;
    defer if (healthy_permission) |*value| value.deinit(alloc);
    var user = try auth.manager.createUser("pool-reader", "secret", if (healthy) &.{ permission, namespace_permission, healthy_permission.? } else &.{ permission, namespace_permission });
    defer user.deinit(alloc);
    server.cfg.user_manager = &auth.manager;
    defer server.cfg.user_manager = null;
    var pg = @import("sql_pgwire.zig").Adapter{ .server = &server };
    const wire = @import("../pgwire/backend.zig");
    const backend = pg.backend();
    const identity = try backend.vtable.authenticate(backend.context, alloc, "pool-reader", "secret");
    defer identity.release(identity.context, alloc);
    var lifetime: FixtureLifetime = .{};
    defer lifetime.drain();
    var cancel: std.atomic.Value(bool) = .init(false);
    const request: wire.Request = .{
        .retained_lifetime = .{ .context = &lifetime, .retain_fn = FixtureLifetime.retain, .release_fn = FixtureLifetime.release },
        .statement = if (remote) "SELECT id, name FROM usage_records WHERE id = $1 LIMIT 2" else "SELECT id, name FROM usage_records WHERE id = $1 LIMIT 1",
        .parameters = &.{.{ .integer = 9007199254740993 }},
        .parameter_types = &.{.integer},
        .limit = if (remote) 2 else 1,
        .io = std.testing.io,
        .cancel_requested = &cancel,
        .deadline = .{ .clock = .awake, .raw = .{ .nanoseconds = std.Io.Clock.awake.now(std.testing.io).nanoseconds + 10 * std.time.ns_per_s } },
    };
    // The controlled gate fixture does not claim a durable restart proof.
    // It exercises the same published rejecting descriptor reachable after
    // reopening an already-published journal, without changing journal data.
    const previous_recovery = if (recovering) try setFixtureRecovery(&server, false) else false;
    defer if (recovering) {
        _ = setFixtureRecovery(&server, previous_recovery) catch unreachable;
    };
    // Fill the fixed metadata baseline allowance, leaving its nonborrowable
    // control/cleanup floor and ordinary query capacity available.
    var metadata_hold: ?@import("http_server.zig").RunnableScheduler.Scheduler.ControlBaseline = if (metadata_pressure) try reserveFixtureRemoteMetadata(&server) else null;
    defer if (metadata_hold) |*reservation| reservation.release();
    if (remote_readiness) {
        const tcp = @import("../pgwire/server.zig");
        const TestClient = @import("pgwire_pool_test_client.zig").Client;
        var pg_transport = tcp.initTransport(alloc, 1);
        defer pg_transport.deinit();
        var pg_listener = try tcp.start(alloc, .{ .io = pg_transport.io(), .backend = backend, .bind_port = 0, .max_connections = 1, .max_session_bytes = 4 << 20, .max_retained_bytes = 1 << 20, .limits = .{ .frame_bytes = 64 << 10, .connection_bytes = 2 << 20, .retained_bytes = 512 << 10 } });
        defer pg_listener.deinit();
        const connector = @import("antfly_local_sources").common_threaded_connect_io;
        var client_transport = std.Io.Threaded.init(alloc, .{ .concurrent_limit = .limited(4) });
        const client_vtable = connector.createVTable(alloc, &client_transport) catch |err| {
            client_transport.deinit();
            return err;
        };
        defer {
            client_transport.deinit();
            alloc.destroy(client_vtable);
        }
        const peer = try TestClient.connectAs(alloc, connector.io(&client_transport, client_vtable), pg_listener.address().getPort(), "pool-reader", "secret");
        defer peer.close();
        const tcp_query = "SELECT id, name FROM usage_records WHERE id = 9007199254740993 LIMIT 2\x00";
        const previous = try setFixtureRecovery(&server, false);
        var restored = false;
        defer if (!restored) {
            _ = setFixtureRecovery(&server, previous) catch unreachable;
        };
        const client = server.remoteRetainedReadClient() orelse return error.TestMissingNodeOwner;
        const before = client.ledger.snapshot();
        const sequence = try fixtureAttemptSequence(&server);
        const captures = receiver_authority.captures.load(.acquire);
        const validations = receiver_authority.validations.load(.acquire);
        {
            const response = try executeHttpSqlTcp(alloc, transport.executor(), coordinator_uri.?, request.statement, request.parameters);
            defer alloc.free(response.body);
            try std.testing.expectEqual(@as(u16, 503), response.status);
            // Verify the public SQL diagnostic without depending on request
            // identifiers or disclosing the internal Zig error spelling.
            var parsed = try std.json.parseFromSlice(@import("antfly_metadata_openapi").types.SQLDiagnostic, alloc, response.body, .{});
            defer parsed.deinit();
            try std.testing.expectEqualStrings("53300", parsed.value.code);
            try std.testing.expectEqual(@as(?bool, false), parsed.value.retryable);
        }
        try std.testing.expectError(error.RemoteReadUnavailable, backend.vtable.open_stream.?(backend.context, alloc, identity, request));
        try qualifyPgwireReadinessReply(peer, tcp_query, false);
        try waitNoHttpSqlRoots(&server);
        try std.testing.expectEqual(@as(usize, 1), lifetime.references.load(.acquire));
        try std.testing.expectEqual(sequence, try fixtureAttemptSequence(&server));
        const after = client.ledger.snapshot();
        try std.testing.expectEqual(before.attempts, after.attempts);
        try std.testing.expectEqual(before.bytes, after.bytes);
        try std.testing.expectEqual(before.queued, after.queued);
        try std.testing.expectEqual(captures, receiver_authority.captures.load(.acquire));
        try std.testing.expectEqual(validations, receiver_authority.validations.load(.acquire));
        try expectRemoteOwnership(&server, &receiver.?, false);
        _ = try setFixtureRecovery(&server, previous);
        restored = true;
        {
            // Hold the node's pre-signing permit before invalidating discovery.
            // This fixed test-harness caller prevents the background consumer
            // from refreshing the profile until both frontends have refused.
            // Recovery itself stays open throughout this second refusal phase.
            const state = server.owned_remote_attempts.load(.acquire) orelse return error.TestMissingNodeOwner;
            const cache = state.capabilities orelse return error.TestMissingCapabilityCache;
            const clock = @import("antfly_platform").time;
            const readiness_deadline = clock.monotonicNs() + 5 * std.time.ns_per_s;
            var discovery_hold = try state.control_admission.acquireIdle(.{ .deadline_ns = readiness_deadline });
            defer discovery_hold.release();
            const ready_client = server.remoteRetainedReadClient() orelse return error.TestMissingNodeOwner;
            try std.testing.expect(ready_client.reads_ready);
            const authority = ready_client.targets[0].authority;
            try cache.invalidate(0, authority);
            try std.testing.expectEqual(@import("remote_capability_publication.zig").Cache.ProfileTag.unverified, std.meta.activeTag(try cache.profile(0, authority)));
            const response = try executeHttpSqlTcp(alloc, transport.executor(), coordinator_uri.?, request.statement, request.parameters);
            defer alloc.free(response.body);
            try std.testing.expectEqual(@as(u16, 503), response.status);
            var parsed = try std.json.parseFromSlice(@import("antfly_metadata_openapi").types.SQLDiagnostic, alloc, response.body, .{});
            defer parsed.deinit();
            try std.testing.expectEqualStrings("53300", parsed.value.code);
            try std.testing.expectEqual(@as(?bool, false), parsed.value.retryable);
            try std.testing.expectError(error.RemoteReadUnavailable, backend.vtable.open_stream.?(backend.context, alloc, identity, request));
            try qualifyPgwireReadinessReply(peer, tcp_query, false);
            try waitNoHttpSqlRoots(&server);
            try std.testing.expectEqual(@as(usize, 1), lifetime.references.load(.acquire));
            try std.testing.expectEqual(sequence, try fixtureAttemptSequence(&server));
            const unpublished = ready_client.ledger.snapshot();
            try std.testing.expectEqual(before.attempts, unpublished.attempts);
            try std.testing.expectEqual(before.bytes, unpublished.bytes);
            try std.testing.expectEqual(before.queued, unpublished.queued);
            try std.testing.expectEqual(captures, receiver_authority.captures.load(.acquire));
            try std.testing.expectEqual(validations, receiver_authority.validations.load(.acquire));
            try expectRemoteOwnership(&server, &receiver.?, false);
            try std.testing.expect((server.remoteRetainedReadClient() orelse return error.TestMissingNodeOwner).reads_ready);
            try std.testing.expectEqual(@import("remote_capability_publication.zig").Cache.ProfileTag.unverified, std.meta.activeTag(try cache.profile(0, authority)));
            discovery_hold.release();
            // Only the production background consumer restores eligibility.
            // Keep the original observation deadline; never inject a profile
            // or replay either refused SQL operation to discover capabilities.
            while (!(try cache.snapshot(0, authority)).retained_read) {
                if (clock.monotonicNs() >= readiness_deadline) return error.TestDiscoveryDidNotRestoreReadiness;
                try std.testing.io.sleep(.fromMilliseconds(1), .awake);
            }
            try std.testing.expectEqual(sequence, try fixtureAttemptSequence(&server));
            try std.testing.expectEqual(captures, receiver_authority.captures.load(.acquire));
            try std.testing.expectEqual(validations, receiver_authority.validations.load(.acquire));
        }
        {
            // A distinct fresh request succeeds; the refused execution is
            // never resumed or replayed by the fixture or runtime.
            const response = try executeHttpSqlTcp(alloc, transport.executor(), coordinator_uri.?, request.statement, request.parameters);
            defer alloc.free(response.body);
            try std.testing.expectEqual(@as(u16, 200), response.status);
            var parsed = try std.json.parseFromSlice(@import("antfly_metadata_openapi").types.SQLResponse, alloc, response.body, .{});
            defer parsed.deinit();
            try std.testing.expectEqual(@as(usize, 2), parsed.value.rows.len);
            for (parsed.value.rows) |row| {
                try std.testing.expectEqual(@as(usize, 2), row.len);
                try std.testing.expect(row[0] == .string and row[1] == .string);
                try std.testing.expectEqualStrings("9007199254740993", row[0].string);
                try std.testing.expectEqualStrings("exact", row[1].string);
            }
        }
        try waitNoHttpSqlRoots(&server);
        try waitRemoteRetirement(&server, &receiver.?);
        try expectRemoteOwnership(&server, &receiver.?, false);
        try qualifyPgwireReadinessReply(peer, tcp_query, true);
        try waitNoHttpSqlRoots(&server);
        try waitRemoteRetirement(&server, &receiver.?);
        try expectRemoteOwnership(&server, &receiver.?, false);
        try std.testing.expect((try fixtureAttemptSequence(&server)) > sequence);
        // Continue through ordinary remote page, park and cancel coverage.
    }
    if (ready_metadata_pressure) {
        const client = server.remoteRetainedReadClient() orelse return error.TestMissingNodeOwner;
        try std.testing.expect(client.reads_ready);
        try std.testing.expect(try client.supportsRead(0, true));
        const sequence = try fixtureAttemptSequence(&server);
        const before = client.ledger.snapshot();
        const captures = receiver_authority.captures.load(.acquire);
        const validations = receiver_authority.validations.load(.acquire);
        {
            const response = try executeHttpSqlTcp(alloc, transport.executor(), coordinator_uri.?, request.statement, request.parameters);
            defer alloc.free(response.body);
            try std.testing.expectEqual(@as(u16, 503), response.status);
            var parsed = try std.json.parseFromSlice(@import("antfly_metadata_openapi").types.SQLDiagnostic, alloc, response.body, .{});
            defer parsed.deinit();
            try std.testing.expectEqualStrings("53300", parsed.value.code);
            try std.testing.expectEqual(@as(?bool, true), parsed.value.retryable);
        }
        try waitNoHttpSqlRoots(&server);
        if (backend.vtable.open_stream.?(backend.context, alloc, identity, request)) |opened| {
            if (opened) |stream| stream.close(stream.context);
            return error.TestExpectedRemoteMetadataRefusal;
        } else |cause| if (cause != error.SchedulerFull) return cause;
        try waitNoHttpSqlRoots(&server);
        const clock = @import("antfly_platform").time;
        const until = clock.monotonicNs() + 5 * std.time.ns_per_s;
        while (lifetime.references.load(.acquire) != 1) {
            if (clock.monotonicNs() >= until) return error.TestRefusalRetainedForeground;
            try std.testing.io.sleep(.fromMilliseconds(1), .awake);
        }
        // Local progress while remote creation is refused: no table access,
        // no remote owner, and no artificial increase of any pool allowance.
        {
            const response = try executeHttpSqlTcp(alloc, transport.executor(), coordinator_uri.?, "SELECT 1", &.{});
            defer alloc.free(response.body);
            try std.testing.expectEqual(@as(u16, 200), response.status);
            var parsed = try std.json.parseFromSlice(@import("antfly_metadata_openapi").types.SQLResponse, alloc, response.body, .{});
            defer parsed.deinit();
            try std.testing.expectEqual(@as(usize, 1), parsed.value.rows.len);
            try std.testing.expectEqual(@as(usize, 1), parsed.value.rows[0].len);
            try std.testing.expect(parsed.value.rows[0][0] == .string);
            try std.testing.expectEqualStrings("1", parsed.value.rows[0][0].string);
        }
        try waitNoHttpSqlRoots(&server);
        try std.testing.expectEqual(sequence, try fixtureAttemptSequence(&server));
        const after = client.ledger.snapshot();
        try std.testing.expectEqual(before.attempts, after.attempts);
        try std.testing.expectEqual(before.bytes, after.bytes);
        try std.testing.expectEqual(before.queued, after.queued);
        try std.testing.expectEqual(captures, receiver_authority.captures.load(.acquire));
        try std.testing.expectEqual(validations, receiver_authority.validations.load(.acquire));
        try expectRemoteOwnership(&server, &receiver.?, false);
        // Release the actual reservation, then issue distinct fresh requests.
        // No failed execution is resumed or replayed by the fixture/runtime.
        metadata_hold.?.release();
        metadata_hold = null;
        {
            const response = try executeHttpSqlTcp(alloc, transport.executor(), coordinator_uri.?, request.statement, request.parameters);
            defer alloc.free(response.body);
            try std.testing.expectEqual(@as(u16, 200), response.status);
            var parsed = try std.json.parseFromSlice(@import("antfly_metadata_openapi").types.SQLResponse, alloc, response.body, .{});
            defer parsed.deinit();
            try std.testing.expectEqual(@as(usize, 2), parsed.value.rows.len);
            for (parsed.value.rows) |row| {
                try std.testing.expectEqual(@as(usize, 2), row.len);
                try std.testing.expect(row[0] == .string and row[1] == .string);
                try std.testing.expectEqualStrings("9007199254740993", row[0].string);
                try std.testing.expectEqualStrings("exact", row[1].string);
            }
        }
        try waitNoHttpSqlRoots(&server);
        try waitRemoteRetirement(&server, &receiver.?);
        try expectRemoteOwnership(&server, &receiver.?, false);
        try std.testing.expect((try fixtureAttemptSequence(&server)) > sequence);
        // Continue through the ordinary remote pgwire stream/page/park/cancel
        // assertions below with metadata available and original authority.
    }
    if (recovering) {
        const client = server.remoteRetainedReadClient() orelse return error.TestMissingNodeOwner;
        try std.testing.expect(!client.reads_ready);
        try std.testing.expect(!try client.supportsRead(0, true));
        const before = client.ledger.snapshot();
        const sequence = client.ledger.next_sequence;
        try std.testing.expectError(error.RemoteReadUnavailable, client.dispatchRead(0, 0x7f01, 512 << 10, @import("antfly_platform").time.monotonicNs() + std.time.ns_per_s, .{ .method = .POST, .uri = "/internal/v1/groups/7001/tables/usage_records/query", .body = "{}" }));
        try std.testing.expectEqual(sequence, client.ledger.next_sequence);
        try std.testing.expectEqual(before.attempts, client.ledger.snapshot().attempts);
        try std.testing.expectEqual(before.bytes, client.ledger.snapshot().bytes);
        const response = try executeHttpSqlTcp(alloc, transport.executor(), coordinator_uri.?, request.statement, request.parameters);
        defer alloc.free(response.body);
        try std.testing.expectEqual(@as(u16, 200), response.status);
        var parsed = try std.json.parseFromSlice(@import("antfly_metadata_openapi").types.SQLResponse, alloc, response.body, .{});
        defer parsed.deinit();
        try std.testing.expectEqual(@as(usize, 2), parsed.value.rows.len);
        try std.testing.expectEqual(@as(usize, 2), parsed.value.columns.len);
        try std.testing.expectEqual(@import("antfly_metadata_openapi").types.SQLColumnType.integer, parsed.value.columns[0].type);
        for (parsed.value.rows) |row| {
            try std.testing.expect(row[0] == .string and row[1] == .string);
            try std.testing.expectEqualStrings("9007199254740993", row[0].string);
            try std.testing.expectEqualStrings("exact", row[1].string);
        }
        try waitNoHttpSqlRoots(&server);
        try expectRemoteOwnership(&server, &receiver.?, false);
        // Continue through the existing real pgwire adapter stream, page,
        // parked-root and cancellation assertions with this gate still closed.
    }
    if (response_loss) {
        fault.enabled.store(true, .release);
        // This defer runs before lifetime.drain and API teardown on failures.
        defer fault.enabled.store(false, .release);
        errdefer {
            fault.enabled.store(false, .release);
            // Preserve the original failure while giving exact remote cleanup
            // a bounded chance to retire before strict API destruction.
            waitRemoteRetirement(&server, &receiver.?) catch {};
        }
        const capability_client = server.remoteRetainedReadClient() orelse return error.TestMissingNodeOwner;
        try probeFixtureCapabilities(capability_client, 0, @import("antfly_platform").time.monotonicNs() + std.time.ns_per_s);
        try std.testing.expect(fault.capability_replies.load(.acquire) > 0);
        if (healthy) {
            try qualifyHealthyPeer(alloc, &server, &receiver.?, &healthy_server.?, transport.executor(), coordinator_uri.?, &fault, request.statement, request.parameters);
            try std.testing.expect(healthy_authority.captures.load(.acquire) > 0);
            try std.testing.expect(healthy_authority.validations.load(.acquire) > 0);
            try std.testing.expectEqual(@as(usize, 0), coordinator_owners.?.ownerCountForTest());
            return;
        }
        if (http_loss) {
            const response = if (http_transport) try executeHttpSqlTcp(alloc, transport.executor(), coordinator_uri.?, request.statement, request.parameters) else try executeHttpSql(alloc, &server, request.statement, request.parameters);
            defer alloc.free(response.body);
            try std.testing.expect(response.status >= 400 and response.status < 600);
        } else {
            if (backend.vtable.open_stream.?(backend.context, alloc, identity, request)) |opened| {
                if (opened) |stream| stream.close(stream.context);
                return error.TestExpectedLostCreationResponse;
            } else |cause| {
                // Only a published attempt with an uncertain response exercises
                // this fixture. Surface setup/admission failures directly.
                if (cause != error.RemoteOutcomeUncertain) return cause;
            }
        }
        if (phase_loss) {
            // Successful token/page decoding happened after the production
            // native handler, before the TCP shutdown. Failure must not replay
            // either creation or cursor advancement.
            try std.testing.expectEqual(@as(usize, 1), fault.phase_requests.load(.acquire));
            try std.testing.expectEqual(@as(usize, 1), fault.phase_hits.load(.acquire));
            if (mode == .page_response_loss) {
                try std.testing.expect(fault.advanced_rows.load(.acquire) > 0);
            } else try std.testing.expectEqual(@as(usize, 1), fault.token_replies.load(.acquire));
        }
        try std.testing.expect(fault.successful_drops.load(.acquire) > 0);
        try std.testing.expectEqual(@as(usize, 0), fault.missing_h1.load(.acquire));
        const until = @import("antfly_platform").time.monotonicNs() + 5 * std.time.ns_per_s;
        while (lifetime.references.load(.acquire) != 1) {
            if (@import("antfly_platform").time.monotonicNs() >= until) return error.TestUncertaintyRetainedForeground;
            try std.testing.io.sleep(.fromMilliseconds(1), .awake);
        }
        if (http_transport) try waitNoHttpSqlRoots(&server);
        try std.testing.expectEqual(@as(usize, 0), server.queryAdmissionStats().in_flight);
        try expectSqlRootsParked(&server, 0);
        try expectCoordinatorUncertainty(&server);
        // Use the actual SQL backend with its sole coarse admission permit.
        var local_request = request;
        local_request.statement = "SELECT 1";
        local_request.parameters = &.{};
        local_request.parameter_types = &.{};
        local_request.limit = 1;
        local_request.deadline = .{ .clock = .awake, .raw = .{ .nanoseconds = std.Io.Clock.awake.now(std.testing.io).nanoseconds + 10 * std.time.ns_per_s } };
        if (http_loss) {
            const response = if (http_transport) try executeHttpSqlTcp(alloc, transport.executor(), coordinator_uri.?, "SELECT 1", &.{}) else try executeHttpSql(alloc, &server, "SELECT 1", &.{});
            defer alloc.free(response.body);
            try std.testing.expectEqual(@as(u16, 200), response.status);
            var parsed = try std.json.parseFromSlice(@import("antfly_metadata_openapi").types.SQLResponse, alloc, response.body, .{});
            defer parsed.deinit();
            try std.testing.expectEqual(@as(usize, 1), parsed.value.columns.len);
            try std.testing.expectEqual(@import("antfly_metadata_openapi").types.SQLColumnType.integer, parsed.value.columns[0].type);
            try std.testing.expectEqual(@as(usize, 1), parsed.value.rows.len);
            try std.testing.expect(parsed.value.rows[0][0] == .string);
            try std.testing.expectEqualStrings("1", parsed.value.rows[0][0].string);
            if (http_transport) try waitNoHttpSqlRoots(&server);
            try std.testing.expectEqual(@as(usize, 0), server.queryAdmissionStats().in_flight);
            try expectSqlRootsParked(&server, 0);
        } else {
            // Table-free SELECT uses the normal materialized backend path.
            var arena = std.heap.ArenaAllocator.init(alloc);
            defer arena.deinit();
            var result = try backend.vtable.execute(backend.context, arena.allocator(), identity, local_request);
            defer result.deinit();
            try std.testing.expectEqual(@as(usize, 1), result.rowCount());
            const value = try result.cell(arena.allocator(), 0, 0);
            // Materialized SQL bigint cells use the lossless text wire form.
            try std.testing.expectEqual(@import("../pgwire/backend.zig").Type.integer, result.columns[0].type);
            try std.testing.expect(!value.sql_null);
            try std.testing.expect(value.value == .string);
            try std.testing.expectEqualStrings("1", value.value.string);
        }
        const local_until = @import("antfly_platform").time.monotonicNs() + 5 * std.time.ns_per_s;
        while (lifetime.references.load(.acquire) != 1) {
            if (@import("antfly_platform").time.monotonicNs() >= local_until) return error.TestLocalQueryRetirementTimeout;
            try std.testing.io.sleep(.fromMilliseconds(1), .awake);
        }
        // A fresh signed capability response proves control progress, never
        // retirement of the original attempt or its retained resources.
        try probeFixtureCapabilities(capability_client, 0, @import("antfly_platform").time.monotonicNs() + std.time.ns_per_s);
        try expectCoordinatorUncertainty(&server);
        if (phase_loss) {
            try std.testing.expectEqual(@as(usize, 1), fault.phase_requests.load(.acquire));
            try std.testing.expectEqual(@as(usize, 1), fault.phase_hits.load(.acquire));
        }
        fault.enabled.store(false, .release);
        try waitRemoteRetirement(&server, &receiver.?);
        try expectRemoteOwnership(&server, &receiver.?, false);
        return;
    }
    {
        const stream = (try backend.vtable.open_stream.?(backend.context, alloc, identity, request)) orelse return error.ExpectedReadStream;
        defer stream.close(stream.context);
        try std.testing.expectEqual(@as(usize, 2), lifetime.references.load(.acquire));
        try std.testing.expectEqual(@as(usize, 1), server.queryAdmissionStats().in_flight);
        try expectSqlRootsParked(&server, 1);
        var page = try stream.next(stream.context, alloc, request, 1);
        defer page.result.deinit();
        try std.testing.expectEqual(@as(usize, 1), page.result.rowCount());
        const id = try page.result.cell(alloc, 0, 0);
        const name = try page.result.cell(alloc, 0, 1);
        try std.testing.expectEqual(@as(i64, 9007199254740993), id.value.integer);
        try std.testing.expectEqualStrings("exact", name.value.string);
        if (recovering) {
            try std.testing.expect(!page.exhausted);
            try std.testing.expectEqual(@as(usize, 1), coordinator_owners.?.ownerCountForTest());
            try std.testing.expect(coordinator_authority.captures.load(.acquire) > 0);
            try std.testing.expect(coordinator_authority.validations.load(.acquire) > 0);
            try expectRemoteOwnership(&server, &receiver.?, false);
        } else if (remote) {
            try std.testing.expect(!page.exhausted);
            try std.testing.expectEqual(@as(usize, 0), coordinator_owners.?.ownerCountForTest());
            try expectRemoteOwnership(&server, &receiver.?, true);
            try std.testing.expect(receiver_authority.captures.load(.acquire) > 0);
            try std.testing.expect(receiver_authority.validations.load(.acquire) > 0);
        }
        cancel.store(true, .release);
        try std.testing.expectError(error.QueryCanceled, stream.next(stream.context, alloc, request, 1));
    }
    lifetime.drain();
    try std.testing.expectEqual(@as(usize, 0), server.queryAdmissionStats().in_flight);
    try expectSqlRootsParked(&server, 0);
    try std.testing.expectEqual(@as(usize, 1), owners.ownerCountForTest());
    if (recovering) {
        try expectRemoteOwnership(&server, &receiver.?, false);
        try std.testing.expectEqual(@as(usize, 1), coordinator_owners.?.ownerCountForTest());
    } else if (remote) {
        // Foreground retirement no longer implies remote physical retirement.
        try waitRemoteRetirement(&server, &receiver.?);
        try expectRemoteOwnership(&server, &receiver.?, false);
        try std.testing.expectEqual(@as(usize, 0), coordinator_owners.?.ownerCountForTest());
    }
}

fn expectRemoteOwnership(coordinator: *ApiHttpServer, receiver: *ApiHttpServer, active: bool) !void {
    const state = coordinator.owned_remote_attempts.load(.acquire) orelse return error.TestMissingNodeOwner;
    const endpoints = state.endpoint_source orelse return error.TestMissingEndpointSource;
    {
        while (!endpoints.mutex.tryLock()) std.atomic.spinLoopHint();
        defer endpoints.mutex.unlock();
        try std.testing.expectEqual(@as(usize, if (active) 1 else 0), endpoints.bindings);
    }
    const stats = try coordinator.remoteAttemptOwnerStats();
    if (active) try std.testing.expect(stats.outbound_bytes > 0) else try std.testing.expectEqual(@as(usize, 0), stats.outbound_bytes);
    if (receiver.retained_read_runtime) |runtime| {
        const registry = &runtime.registry;
        registry.mutex.lockUncancelable(registry.io);
        defer registry.mutex.unlock(registry.io);
        var retained: usize = 0;
        for (registry.slots) |slot| if (slot.entry != null) {
            retained += 1;
        };
        if (active) try std.testing.expect(retained > 0) else try std.testing.expectEqual(@as(usize, 0), retained);
    } else try std.testing.expect(!active);
}

// Background discovery and retirement retain their own protected control roots.
// Inspect only SQL entries under the scheduler lock; report after unlocking.
fn expectSqlRootsParked(server: *ApiHttpServer, expected: usize) !void {
    const scheduler = if (server.runnable_scheduler) |*value| value else return error.TestMissingScheduler;
    var queries: usize = 0;
    var parked: usize = 0;
    while (!scheduler.mutex.tryLock()) std.atomic.spinLoopHint();
    var entry = scheduler.entries;
    while (entry) |current| : (entry = current.all_next) {
        if (current.class == .query) {
            queries += 1;
            if (current.state == .output_blocked) parked += 1;
        }
    }
    scheduler.mutex.unlock();
    try std.testing.expectEqual(expected, queries);
    try std.testing.expectEqual(expected, parked);
}

fn expectCoordinatorUncertainty(coordinator: *ApiHttpServer) !void {
    const state = coordinator.owned_remote_attempts.load(.acquire) orelse return error.TestMissingNodeOwner;
    const endpoints = state.endpoint_source orelse return error.TestMissingEndpointSource;
    while (!endpoints.mutex.tryLock()) std.atomic.spinLoopHint();
    const bindings = endpoints.bindings;
    endpoints.mutex.unlock();
    try std.testing.expectEqual(@as(usize, 1), bindings);
    try std.testing.expect((try coordinator.remoteAttemptOwnerStats()).outbound_bytes > 0);
}

fn waitRemoteRetirement(coordinator: *ApiHttpServer, receiver: *ApiHttpServer) !void {
    const clock = @import("antfly_platform").time;
    const until = clock.monotonicNs() + 10 * std.time.ns_per_s;
    const state = coordinator.owned_remote_attempts.load(.acquire) orelse return error.TestMissingNodeOwner;
    const endpoints = state.endpoint_source orelse return error.TestMissingEndpointSource;
    while (true) {
        while (!endpoints.mutex.tryLock()) std.atomic.spinLoopHint();
        const bindings = endpoints.bindings;
        endpoints.mutex.unlock();
        var physical: usize = 0;
        if (receiver.retained_read_runtime) |runtime| {
            const registry = &runtime.registry;
            registry.mutex.lockUncancelable(registry.io);
            for (registry.slots) |slot| if (slot.entry != null) {
                physical += 1;
            };
            registry.mutex.unlock(registry.io);
        }
        if (bindings == 0 and physical == 0 and (try coordinator.remoteAttemptOwnerStats()).outbound_bytes == 0) return;
        if (clock.monotonicNs() >= until) return error.TestRemotePhysicalRetirementTimeout;
        try std.testing.io.sleep(.fromMilliseconds(1), .awake);
    }
}

const IsolationCredits = struct {
    bad_attempts: usize,
    bad_bytes: usize,
    healthy_attempts: usize,
    healthy_bytes: usize,
    total_attempts: usize,
    total_bytes: usize,
    next_sequence: u64,
    queued: usize,
};

fn isolationCredits(server: *ApiHttpServer) !IsolationCredits {
    const state = server.owned_remote_attempts.load(.acquire) orelse return error.TestMissingNodeOwner;
    const ledger = &state.ledger;
    while (!ledger.mutex.tryLock()) std.atomic.spinLoopHint();
    defer ledger.mutex.unlock();
    if (ledger.destinations.len != 2) return error.TestMissingHealthyDestination;
    return .{ .bad_attempts = ledger.destinations[0].attempts, .bad_bytes = ledger.destinations[0].bytes, .healthy_attempts = ledger.destinations[1].attempts, .healthy_bytes = ledger.destinations[1].bytes, .total_attempts = ledger.attempts, .total_bytes = ledger.bytes, .next_sequence = ledger.next_sequence, .queued = ledger.waiting_count };
}
fn isolationBindings(server: *ApiHttpServer) !usize {
    const state = server.owned_remote_attempts.load(.acquire) orelse return error.TestMissingNodeOwner;
    const source = state.endpoint_source orelse return error.TestMissingEndpointSource;
    while (!source.mutex.tryLock()) std.atomic.spinLoopHint();
    defer source.mutex.unlock();
    return source.bindings;
}
fn isolationPhysical(server: *ApiHttpServer) usize {
    const runtime = server.retained_read_runtime orelse return 0;
    const registry = &runtime.registry;
    registry.mutex.lockUncancelable(registry.io);
    defer registry.mutex.unlock(registry.io);
    var count: usize = 0;
    for (registry.slots) |slot| if (slot.entry != null) {
        count += 1;
    };
    return count;
}

fn qualifyHealthyPeer(alloc: std.mem.Allocator, server: *ApiHttpServer, bad_server: *ApiHttpServer, healthy_server: *ApiHttpServer, executor: @import("antfly_local_sources").common_http_http_common.RequestExecutor, base_uri: []const u8, fault: *@import("http_response_loss_fixture.zig").ResponseLoss, statement: []const u8, parameters: []const std.json.Value) !void {
    const clock = @import("antfly_platform").time;
    const client = server.remoteRetainedReadClient() orelse return error.TestMissingNodeOwner;
    // Fill the failing destination's unchanged four-attempt/two-MiB allowance.
    // Every request must publish exactly once, then release HTTP/query lifetime.
    for (0..4) |index| {
        const before_credit = try isolationCredits(server);
        const response = try executeHttpSqlTcp(alloc, executor, base_uri, statement, parameters);
        defer alloc.free(response.body);
        try std.testing.expect(response.status >= 400 and response.status < 600);
        try waitNoHttpSqlRoots(server);
        const held = try isolationCredits(server);
        try std.testing.expectEqual(index + 1, held.bad_attempts);
        try std.testing.expectEqual((index + 1) * (512 << 10), held.bad_bytes);
        try std.testing.expectEqual(before_credit.next_sequence + 1, held.next_sequence);
        try std.testing.expectEqual(@as(usize, 0), held.healthy_attempts);
        try std.testing.expectEqual(index + 1, try isolationBindings(server));
    }
    try std.testing.expect(fault.successful_drops.load(.acquire) >= 4);
    try std.testing.expectEqual(@as(usize, 0), fault.missing_h1.load(.acquire));
    const before_rejection = try isolationCredits(server);
    // No wait boundary is supplied: this is an explicitly fail-fast
    // prepublication credit check, not an unsafe retry of lost SQL execution.
    try std.testing.expectError(error.RemoteAttemptCapacity, client.dispatchRead(0, 0x7f00, 512 << 10, clock.monotonicNs() + std.time.ns_per_s, .{ .method = .POST, .uri = "/internal/v1/groups/7001/tables/usage_records/query", .body = "{}" }));
    const rejected = try isolationCredits(server);
    try std.testing.expectEqual(before_rejection.next_sequence, rejected.next_sequence);
    try std.testing.expectEqual(before_rejection.total_attempts, rejected.total_attempts);
    try std.testing.expectEqual(before_rejection.total_bytes, rejected.total_bytes);
    try std.testing.expectEqual(@as(usize, 0), rejected.queued);
    try std.testing.expectEqual(@as(usize, 0), server.queryAdmissionStats().in_flight);
    // The other destination performs an actual native owner-backed read.
    const response = try executeHttpSqlTcp(alloc, executor, base_uri, "SELECT id, name FROM healthy_records WHERE id = $1 LIMIT 1", parameters);
    defer alloc.free(response.body);
    try std.testing.expectEqual(@as(u16, 200), response.status);
    var parsed = try std.json.parseFromSlice(@import("antfly_metadata_openapi").types.SQLResponse, alloc, response.body, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 2), parsed.value.columns.len);
    try std.testing.expectEqual(@import("antfly_metadata_openapi").types.SQLColumnType.integer, parsed.value.columns[0].type);
    try std.testing.expectEqual(@as(usize, 1), parsed.value.rows.len);
    try std.testing.expect(parsed.value.rows[0][0] == .string);
    try std.testing.expectEqualStrings("9007199254740993", parsed.value.rows[0][0].string);
    try std.testing.expect(parsed.value.rows[0][1] == .string);
    try std.testing.expectEqualStrings("healthy", parsed.value.rows[0][1].string);
    try waitNoHttpSqlRoots(server);
    const until = clock.monotonicNs() + 10 * std.time.ns_per_s;
    while (true) {
        const credit = try isolationCredits(server);
        if (credit.healthy_attempts == 0 and isolationPhysical(healthy_server) == 0 and try isolationBindings(server) == 4) break;
        if (clock.monotonicNs() >= until) return error.TestHealthyPeerRetirementTimeout;
        try std.testing.io.sleep(.fromMilliseconds(1), .awake);
    }
    try probeFixtureCapabilities(client, 0, clock.monotonicNs() + std.time.ns_per_s);
    const held = try isolationCredits(server);
    try std.testing.expectEqual(@as(usize, 4), held.bad_attempts);
    try std.testing.expectEqual(@as(usize, 2 << 20), held.bad_bytes);
    try std.testing.expectEqual(@as(usize, 0), held.healthy_attempts);
    try std.testing.expectEqual(@as(usize, 0), held.healthy_bytes);
    try std.testing.expectEqual(@as(usize, 4), held.total_attempts);
    try std.testing.expectEqual(@as(usize, 2 << 20), held.total_bytes);
    // Only exact restored retirement replies release the failing peer's credit.
    fault.enabled.store(false, .release);
    try waitRemoteRetirement(server, bad_server);
    try std.testing.expectEqual(@as(usize, 0), isolationPhysical(healthy_server));
    try expectRemoteOwnership(server, bad_server, false);
}

// This test-only caller competes with the node's single background control
// connection. Retry only capacity refusal of the idempotent capability probe,
// never data creation/paging, and never renew the original deadline.
fn probeFixtureCapabilities(client: @import("remote_attempt_client.zig").Client, destination: usize, deadline_ns: u64) !void {
    const clock = @import("antfly_platform").time;
    while (true) {
        if (clock.monotonicNs() >= deadline_ns) return error.DeadlineExceeded;
        _ = client.probeCapabilities(destination, deadline_ns, null) catch |cause| {
            if (cause != error.OutboundCapacityUnavailable) return cause;
            if (clock.monotonicNs() >= deadline_ns) return error.DeadlineExceeded;
            try std.testing.io.sleep(.fromMilliseconds(1), .awake);
            continue;
        };
        return;
    }
}

fn setFixtureRecovery(server: *ApiHttpServer, complete: bool) !bool {
    const state = server.owned_remote_attempts.load(.acquire) orelse return error.TestMissingNodeOwner;
    const registry = &state.registry;
    while (!registry.mutex.tryLock()) std.atomic.spinLoopHint();
    defer registry.mutex.unlock();
    const previous = registry.recovery_complete;
    registry.recovery_complete = complete;
    return previous;
}

fn reserveFixtureRemoteMetadata(server: *ApiHttpServer) !@import("http_server.zig").RunnableScheduler.Scheduler.ControlBaseline {
    const scheduler = if (server.runnable_owner) |owner| &owner.scheduler else if (server.runnable_scheduler) |*scheduler| scheduler else return error.TestMissingScheduler;
    while (!scheduler.mutex.tryLock()) std.atomic.spinLoopHint();
    const available = scheduler.config.control_bytes / 4 -| scheduler.control_baseline_bytes;
    scheduler.mutex.unlock();
    if (available == 0) return error.TestMissingRemoteMetadataHeadroom;
    var reservation = try scheduler.reserveControlBaseline(available);
    errdefer reservation.release();
    if (scheduler.reserveControlBaseline(1)) |extra| {
        var unexpected = extra;
        unexpected.release();
        return error.TestRemoteMetadataNotFull;
    } else |cause| if (cause != error.SchedulerFull) return cause;
    return reservation;
}

fn fixtureAttemptSequence(server: *ApiHttpServer) !u64 {
    const state = server.owned_remote_attempts.load(.acquire) orelse return error.TestMissingNodeOwner;
    const ledger = &state.ledger;
    while (!ledger.mutex.tryLock()) std.atomic.spinLoopHint();
    defer ledger.mutex.unlock();
    return ledger.next_sequence;
}

fn qualifyPgwireReadinessReply(peer: *@import("pgwire_pool_test_client.zig").Client, query: []const u8, ready: bool) !void {
    const TestClient = @import("pgwire_pool_test_client.zig").Client;
    try peer.send('Q', query);
    var rows: usize = 0;
    var errors: usize = 0;
    var completed = false;
    for (0..32) |_| {
        const frame = try peer.next();
        switch (frame.tag) {
            'T' => if (!ready) return error.TestReadinessPublishedColumns,
            'D' => {
                if (!ready or frame.body.len < 2 or std.mem.readInt(u16, frame.body[0..2], .big) != 2) return error.TestReadinessUnexpectedRow;
                var offset: usize = 2;
                for ([_][]const u8{ "9007199254740993", "exact" }) |expected| {
                    if (frame.body.len - offset < 4) return error.TestReadinessUnexpectedRow;
                    const length = std.mem.readInt(i32, frame.body[offset..][0..4], .big);
                    offset += 4;
                    if (length < 0 or @as(usize, @intCast(length)) > frame.body.len - offset) return error.TestReadinessUnexpectedRow;
                    const count: usize = @intCast(length);
                    try std.testing.expectEqualStrings(expected, frame.body[offset..][0..count]);
                    offset += count;
                }
                if (offset != frame.body.len) return error.TestReadinessUnexpectedRow;
                rows += 1;
            },
            'E' => {
                if (ready or errors != 0) return error.TestReadinessUnexpectedError;
                try std.testing.expectEqualStrings("53300", try TestClient.errorCode(frame.body));
                errors += 1;
            },
            'C' => {
                if (!ready or completed) return error.TestReadinessUnexpectedCompletion;
                try std.testing.expectEqualStrings("SELECT 2\x00", frame.body);
                completed = true;
            },
            'Z' => {
                if (frame.body.len != 1 or frame.body[0] != 'I') return error.TestReadinessInvalidTransactionState;
                try std.testing.expectEqual(@as(usize, if (ready) 2 else 0), rows);
                try std.testing.expectEqual(@as(usize, if (ready) 0 else 1), errors);
                try std.testing.expectEqual(ready, completed);
                return;
            },
            else => return error.TestReadinessUnexpectedFrame,
        }
    }
    return error.TestReadinessFrameLimit;
}
