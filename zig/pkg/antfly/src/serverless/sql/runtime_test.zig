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

const std = @import("std");
const local = @import("antfly_local_sources");
const runtime = @import("runtime.zig");
const plan = @import("plan.zig");
const runner = @import("runner.zig");
const state = @import("state.zig");
const storage = @import("objectstore");
const A = std.mem.Allocator;
const Datum = local.sql_scalar.Datum;
const Context = local.serverless_query_lake_read_context.Context;
const Fixture = struct {
    now: u64 = 1000,
    opens: usize = 0,
    retained: usize = 0,
    filters: usize = 0,
    left: [3][1]Datum = .{ .{Datum.json(.{ .integer = 1 })}, .{Datum.json(.{ .integer = 2 })}, .{.{}} },
    right: [4][1]Datum = .{ .{Datum.json(.{ .integer = 2 })}, .{Datum.json(.{ .integer = 2 })}, .{Datum.json(.{ .integer = 3 })}, .{.{}} },
    fn clock(raw: ?*anyopaque) u64 {
        const self: *Fixture = @ptrCast(@alignCast(raw.?));
        return self.now;
    }
    fn retain(raw: *anyopaque, _: A, spec: plan.Plan, context: Context) !void {
        const self: *Fixture = @ptrCast(@alignCast(raw));
        try context.ensureActive();
        if (!std.mem.eql(u8, spec.left.snapshot, "old-left") or !std.mem.eql(u8, spec.right.snapshot, "old-right") or spec.left.wal_cut != 7 or spec.right.wal_cut != 8) return error.SqlExecutionUnauthorized;
        self.retained += 1;
    }
    const InputStream = struct {
        a: A,
        fixture: *Fixture,
        input: u64,
        split: u32,
        partitions: u32,
        position: usize = 0,
        filter: ?*const local.sql_dynamic_filter.Filter,
        columns: []const plan.Column,
        fn next(raw: *anyopaque, a: A, maximum: usize) !?[]const []const Datum {
            const self: *InputStream = @ptrCast(@alignCast(raw));
            var rows: std.ArrayList([]const Datum) = .empty;
            const count: usize = if (self.input == 1) self.fixture.left.len else self.fixture.right.len;
            while (self.position < count and rows.items.len < maximum) {
                const index = self.position;
                self.position += 1;
                if (index % self.partitions != self.split) continue;
                const cells: []const Datum = if (self.input == 1) &self.fixture.left[index] else &self.fixture.right[index];
                const projected = try a.alloc(Datum, self.columns.len);
                for (self.columns, projected) |column, *out| {
                    out.* = if (std.mem.eql(u8, column.name, "id")) cells[0] else if (std.mem.eql(u8, column.name, "_id")) Datum.json(.{ .string = "row" }) else return error.InvalidSqlBackendResponse;
                }
                if (self.filter) |filter| {
                    var filter_keys: [32]Datum = undefined;
                    for (filter.columns, 0..) |column, i| filter_keys[i] = if (std.mem.eql(u8, column.name, "id")) cells[0] else Datum.json(.{ .string = "row" });
                    if (!try filter.contains(filter_keys[0..filter.columns.len])) continue;
                }
                try rows.append(a, projected);
            }
            return if (rows.items.len == 0) null else rows.items;
        }
        fn close(raw: *anyopaque) void {
            const self: *InputStream = @ptrCast(@alignCast(raw));
            self.a.destroy(self);
        }
    };
    fn open(raw: *anyopaque, a: A, _: state.Digest, input: plan.Input, split: u32, filter: ?*const local.sql_dynamic_filter.Filter, context: Context) !runner.Stream {
        const self: *Fixture = @ptrCast(@alignCast(raw));
        try context.ensureActive();
        if (!std.mem.eql(u8, input.cut, "retained-accepted-cut") or split >= input.splits) return error.SqlExecutionUnauthorized;
        self.opens += 1;
        if (filter != null) self.filters += 1;
        const stream = try a.create(InputStream);
        stream.* = .{ .a = a, .fixture = self, .input = input.table_id, .split = split, .partitions = input.splits, .filter = filter, .columns = input.columns };
        return .{ .ptr = stream, .next_fn = InputStream.next, .close_fn = InputStream.close };
    }
    fn service(self: *Fixture, a: A, client: storage.Client, owner: []const u8) runtime.Service {
        return .{
            .a = a,
            .state_store = .{ .client = client, .bucket = "queries", .prefix = "tenant", .now_ms = self.now },
            .exchange_store = .{ .client = client, .bucket = "exchange", .prefix = "tenant" },
            .tenant = @splat('a'),
            .role = .query_only,
            .owner = owner,
            .clock = .{ .ptr = self, .read = clock },
            .retention = .{ .ptr = self, .ensure = retain },
            .source = .{ .ptr = self, .open = open },
        };
    }
};
const Dispatch = struct {
    workers: [2]runtime.Service,
    next: usize = 0,
    drop_once: bool = true,
    fn run(raw: *anyopaque, tickets: []const plan.Ticket, context: Context) !void {
        const self: *Dispatch = @ptrCast(@alignCast(raw));
        for (tickets) |ticket| {
            try context.ensureActive();
            if (self.drop_once) {
                self.drop_once = false;
                continue;
            }
            const worker = self.workers[self.next % 2];
            self.next += 1;
            try worker.execute(ticket);
            // At-least-once delivery must not duplicate the accepted output.
            try self.workers[(self.next + 1) % 2].execute(ticket);
        }
    }
};

test "distributed SQL runtime executes broadcast and shuffle SQL join semantics across workers and coordinator takeover" {
    const a = std.testing.allocator;
    const Case = struct { kind: @FieldType(plan.Join, "kind"), count: u64 };
    for ([_]Case{ .{ .kind = .inner, .count = 2 }, .{ .kind = .left, .count = 4 }, .{ .kind = .right, .count = 4 }, .{ .kind = .full, .count = 6 }, .{ .kind = .semi, .count = 1 }, .{ .kind = .anti, .count = 2 } }) |case| {
        for ([_]plan.Strategy{ .broadcast, .shuffle }) |strategy| {
            for ([_]bool{ false, true }) |aggregate| {
                if (strategy == .broadcast and (case.kind == .right or case.kind == .full)) continue;
                var arena = std.heap.ArenaAllocator.init(a);
                defer arena.deinit();
                const sa = arena.allocator();
                var memory = storage.MemoryClient.init(a);
                defer memory.deinit();
                var fixture: Fixture = .{};
                const coordinator = fixture.service(a, memory.client(), "coordinator-a");
                var dispatch: Dispatch = .{ .workers = .{ fixture.service(a, memory.client(), "worker-a"), fixture.service(a, memory.client(), "worker-b") } };
                var temporary = try local.common_test_directory.TestDirectory.init("distributed-sql-spill");
                defer temporary.cleanup();
                const spill_root = temporary.path();
                try std.Io.Dir.cwd().createDirPath(std.testing.io, spill_root);
                var dummy: u8 = 0;
                var spill: local.sql_spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = struct {
                    fn check(_: *anyopaque) !void {}
                }.check, .root = spill_root };
                defer spill.deinit();
                if (strategy == .shuffle) for (&dispatch.workers) |*worker| {
                    worker.spill = &spill;
                };
                const column: plan.Column = .{ .name = "id", .type = .integer };
                var spec: plan.Plan = .{
                    .tenant = @splat('a'),
                    .expires_ms = 1000000,
                    .left = .{ .table_id = 1, .schema = @splat('b'), .snapshot = "old-left", .wal_cut = 7, .cut = "retained-accepted-cut", .columns = &.{column}, .estimated_bytes = 100, .estimated_rows = 3, .splits = 2 },
                    .right = .{ .table_id = 2, .schema = @splat('c'), .snapshot = "old-right", .wal_cut = 8, .cut = "retained-accepted-cut", .columns = &.{column}, .estimated_bytes = 100, .estimated_rows = 4, .splits = 2 },
                    .join = .{ .kind = case.kind, .left_keys = &.{0}, .right_keys = &.{0} },
                    .strategy = strategy,
                    .partitions = if (strategy == .shuffle) 4 else 1,
                    .limits = .{},
                };
                spec.limits.query_exchange_bytes = 16 * 1024 * 1024 * 1024;
                if (aggregate) {
                    spec.left.splits = 33; // Exercise the bounded fan-in tree.
                    spec.aggregate = .{ .inputs = &.{null}, .specs = &.{.{ .kind = .count }} };
                }
                const query = try coordinator.submit(spec);
                const claim = (try coordinator.acquire(query)).?;
                _ = try coordinator.tick(query, claim, .{ .ptr = &dispatch, .dispatch = Dispatch.run });
                // Lose the coordinator after task dispatch. A new process resumes
                // accepted task records against the original retained input cuts.
                fixture.now = 92001;
                var replacement = fixture.service(a, memory.client(), "coordinator-b");
                var next = (try replacement.acquire(query)).?;
                try std.testing.expectError(error.SqlExecutionFenced, coordinator.tick(query, claim, .{ .ptr = &dispatch, .dispatch = Dispatch.run }));
                var complete = false;
                var resumed_adaptive = false;
                for (0..128) |_| {
                    complete = try replacement.tick(query, next, .{ .ptr = &dispatch, .dispatch = Dispatch.run });
                    if (complete) break;
                    if (strategy == .shuffle and aggregate and !resumed_adaptive) {
                        const record_key = try replacement.state_store.key(sa, query, null, 0);
                        var record = (try replacement.state_store.load(sa, record_key)).?;
                        defer record.deinit(sa);
                        if (record.value.adaptive != null) {
                            // Lose the process after a bounded statistics batch.
                            // The next owner resumes its authenticated progress.
                            fixture.now += 92001;
                            const stale = replacement;
                            const stale_claim = next;
                            replacement = fixture.service(a, memory.client(), "coordinator-c");
                            next = (try replacement.acquire(query)).?;
                            try std.testing.expectError(error.SqlExecutionFenced, stale.tick(query, stale_claim, .{ .ptr = &dispatch, .dispatch = Dispatch.run }));
                            resumed_adaptive = true;
                        }
                    }
                }
                if (strategy == .shuffle and aggregate) try std.testing.expect(resumed_adaptive);
                try std.testing.expect(complete);
                const result = (try replacement.pollResult(sa, query)).?;
                if (strategy == .shuffle) {
                    const record_key = try replacement.state_store.key(sa, query, null, 0);
                    var record = (try replacement.state_store.load(sa, record_key)).?;
                    defer record.deinit(sa);
                    try std.testing.expectEqualSlices(u16, &.{ 0, 4 }, record.value.reduce_boundaries);
                    try std.testing.expectEqual(@as(usize, 1), result.pages.len);
                }
                try std.testing.expectEqual(if (aggregate) @as(u64, 1) else case.count, result.rows);
                var actual: u64 = 0;
                for (result.pages) |page| {
                    var data = try replacement.exchange_store.read(sa, page, 0, 16 * 1024 * 1024);
                    while (try data.next(sa, 16 * 1024 * 1024)) |block| {
                        defer block.deinit();
                        actual += block.count();
                        for (0..block.count()) |index| {
                            if (aggregate) try std.testing.expectEqual(@as(i64, @intCast(case.count)), (try block.cell(index, 0)).value.integer);
                            if (!aggregate and case.kind == .inner) {
                                try std.testing.expectEqual(@as(i64, 2), (try block.cell(index, 0)).value.integer);
                                try std.testing.expectEqual(@as(i64, 2), (try block.cell(index, 1)).value.integer);
                            }
                        }
                    }
                }
                try std.testing.expectEqual(if (aggregate) @as(u64, 1) else case.count, actual);
                try std.testing.expect(fixture.retained > 2);
                if (case.kind == .left or case.kind == .anti) try std.testing.expectEqual(@as(usize, 0), fixture.filters);
                var api = replacement;
                api.role = .api_only;
                try std.testing.expectError(error.SqlQueryRoleRequired, api.acquire(query));
                api.role = .maintenance_only;
                try std.testing.expectError(error.SqlQueryRoleRequired, api.acquire(query));
            }
        }
    }
}

test "distributed SQL native lake source retains accepted WAL cuts across ingestion and splits overlays exactly once" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const sa = arena.allocator();
    var directory = try local.common_test_directory.TestDirectory.init("distributed-sql-lake");
    defer directory.cleanup();
    const encoded = try std.json.Stringify.valueAlloc(sa, .{ .deployment_mode = "standalone", .storage = .{ .engine = "local", .local = .{ .base_dir = directory.path() } } }, .{});
    var config = try local.common_config.Config.parseFromSlice(a, encoded);
    defer config.deinit();
    const schema = try std.json.Stringify.valueAlloc(sa, .{ .base_source = .{ .kind = "external", .table_id = "events", .format = "iceberg", .uri = try std.fmt.allocPrint(sa, "file://{s}/warehouse", .{directory.path()}), .schema_fingerprint = "auto", .write_policy = "iceberg_writer", .catalog = .{ .type = "managed" } } }, .{});
    const binding = (try local.serverless_external_source_schema_binding.externalBindingFromSchemaJsonAlloc(sa, schema)).?;
    const ingestion = @import("../lake_ingestion.zig");
    const configured = @import("../configured_object_store_support.zig");
    const options: configured.BindingObjectStoreOpenOptions = .{ .node_config = &config, .catalog_table_id = 7 };
    var opened = try configured.openBindingObjectStoreAlloc(a, binding.binding, options);
    defer opened.deinit();
    const lake = local.serverless_external_source_mod.lake_catalog;
    var managed: lake.managed.Managed = .{ .client = opened.client, .bucket = opened.bucket, .prefix = opened.prefix, .source_uri = binding.binding.source_uri };
    _ = try managed.create(sa, "create", "{\"schema\":{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"id\",\"type\":\"long\",\"required\":true}]}}", 1);
    _ = try ingestion.accept(sa, binding.binding, options, .{}, "{\"batch_id\":\"base\",\"source\":\"s\",\"epoch\":\"1\",\"checkpoint\":\"1\",\"key_fields\":[\"id\"],\"changes\":[{\"op\":\"upsert\",\"row\":{\"id\":1}},{\"op\":\"upsert\",\"row\":{\"id\":2}}]}");
    try std.testing.expect(try ingestion.drain(sa, binding.binding, options, .{}));
    _ = try ingestion.accept(sa, binding.binding, options, .{}, "{\"batch_id\":\"overlay\",\"source\":\"s\",\"epoch\":\"1\",\"checkpoint\":\"2\",\"expected_checkpoint\":\"1\",\"key_fields\":[\"id\"],\"changes\":[{\"op\":\"delete\",\"row\":{\"id\":2}},{\"op\":\"upsert\",\"row\":{\"id\":3}}]}");
    const Authorization = struct {
        table: local.sql_catalog.Table,
        fn resolve(raw: *anyopaque, _: A, tenant: state.Digest, id: u64, _: Context) !local.sql_catalog.Table {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (!std.mem.eql(u8, &tenant, &@as(state.Digest, @splat('a'))) or id != self.table.id) return error.SqlExecutionUnauthorized;
            return self.table;
        }
    };
    var auth: Authorization = .{ .table = .{ .id = 7, .physical_name = "events", .schema_version = 1, .external_base_source = binding, .columns = &.{.{ .name = "id", .path = "id", .type = .integer }} } };
    const current_table = try managed.load(sa);
    const detected = try local.serverless_query_lake_schema.icebergSchema(sa, current_table.metadata_json, null);
    auth.table.external_base_source.?.binding.schema_fingerprint = detected.fingerprint;
    var objects = try @import("../../api/lake_index_store.zig").Store.open(a, &config, null, false);
    defer objects.deinit();
    var artifacts = objects.artifactStore();
    var host: @import("lake_source.zig").Host = .{ .artifacts = &artifacts, .store_identity = try @import("../../api/native_retained_cut.zig").storeIdentity(sa, objects.locator), .options = options, .io = std.testing.io, .authorization = .{ .ptr = &auth, .resolve = Authorization.resolve } };
    const now: u64 = @intCast(@import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms);
    const input = try host.capture(sa, @splat('a'), 7, &.{.{ .name = "id", .type = .integer }}, 2, now + 600000, .{ .io = std.testing.io });
    // An independently opened worker needs no in-process catalog resolver.
    // Its only authority is the immutable scope-bound admitted read grant.
    var portable_host = host;
    portable_host.authorization = null;
    const portable = portable_host.source();
    const probe = try portable.open(portable.ptr, sa, @splat('a'), input, 0, null, .{ .io = std.testing.io });
    probe.close();
    try std.testing.expectError(error.SqlExecutionUnauthorized, portable.open(portable.ptr, sa, @splat('b'), input, 0, null, .{ .io = std.testing.io }));
    const execution_root = try std.fmt.allocPrint(sa, "{s}/execution", .{directory.path()});
    try std.Io.Dir.cwd().createDirPath(std.testing.io, try std.fmt.allocPrint(sa, "{s}/sql-state", .{execution_root}));
    try std.Io.Dir.cwd().createDirPath(std.testing.io, try std.fmt.allocPrint(sa, "{s}/sql-exchange", .{execution_root}));
    const execution_uri = try std.fmt.allocPrint(sa, "file://{s}", .{execution_root});
    const deployed = try @import("deployment.zig").Owned.init(a, std.testing.io, .{ .state_uri = execution_uri, .exchange_uri = execution_uri, .workers = &.{"http://127.0.0.1:1"}, .authentication = .{ .secret = "0123456789abcdef0123456789abcdef", .issuer = "sql-test" } }, .query_only, &config, null, .standalone, directory.path());
    defer deployed.deinit();

    _ = try ingestion.accept(sa, binding.binding, options, .{}, "{\"batch_id\":\"later\",\"source\":\"s\",\"epoch\":\"1\",\"checkpoint\":\"3\",\"expected_checkpoint\":\"2\",\"key_fields\":[\"id\"],\"changes\":[{\"op\":\"delete\",\"row\":{\"id\":1}},{\"op\":\"upsert\",\"row\":{\"id\":4}}]}");
    // Both publication and WAL truncation can advance after capture. Readers
    // must use the copied overlay, never acquire a fresh pending suffix.
    try std.testing.expect(try ingestion.drain(sa, binding.binding, options, .{}));
    var service = deployed.service;
    service.tenant = @splat('a');
    const specification = try plan.choose(input, input, @splat('a'), now + 600000, .{ .left_keys = &.{0}, .right_keys = &.{0} }, .{}, now);
    const query = try service.submit(specification);
    const claim = (try service.acquire(query)).?;
    const Direct = struct {
        fn dispatch(raw: *anyopaque, tickets: []const plan.Ticket, _: Context) !void {
            const worker: *runtime.Service = @ptrCast(@alignCast(raw));
            for (tickets) |ticket| try worker.execute(ticket);
        }
    };
    var done = false;
    for (0..32) |_| {
        done = try service.tick(query, claim, .{ .ptr = &service, .dispatch = Direct.dispatch });
        if (done) break;
    }
    try std.testing.expect(done);
    const result = (try service.pollResult(sa, query)).?;
    try std.testing.expectEqual(@as(u64, 2), result.rows);
    var keys: std.AutoHashMapUnmanaged(i64, void) = .empty;
    for (result.pages) |page| {
        var data = try service.exchange_store.read(sa, page, 0, 1 << 20);
        while (try data.next(sa, 1 << 20)) |block| {
            defer block.deinit();
            for (0..block.count()) |row| try keys.put(sa, (try block.cell(row, 0)).value.integer, {});
        }
    }
    try std.testing.expect(keys.contains(1) and keys.contains(3));
    try std.testing.expect(!keys.contains(2) and !keys.contains(4));
}

test "distributed SQL signed task RPC uses query listeners and recovery driver without maintenance" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const sa = arena.allocator();
    var memory = storage.MemoryClient.init(a);
    defer memory.deinit();
    var fixture: Fixture = .{};
    const coordinator = fixture.service(a, memory.client(), "coordinator");
    const Scope = struct {
        worker: runtime.Service,
        fn open(raw: *anyopaque, _: A, tenant: state.Digest, context: Context) !runtime.Service {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (!std.mem.eql(u8, &tenant, &self.worker.tenant)) return error.SqlExecutionUnauthorized;
            var worker = self.worker;
            worker.context = context;
            return worker;
        }
        fn noDispatch(_: *anyopaque, _: []const plan.Ticket, _: Context) !void {
            return error.SqlWorkerUnavailable;
        }
    };
    var scope_a: Scope = .{ .worker = fixture.service(a, memory.client(), "worker-a") };
    var scope_b: Scope = .{ .worker = fixture.service(a, memory.client(), "worker-b") };
    const transport = @import("transport.zig");
    const secret = "0123456789abcdef0123456789abcdef";
    const authentication: @import("../../api/internal_service_auth.zig").Config = .{ .secret = secret, .issuer = "sql-workers" };
    var admission = local.common_request_admission.RequestAdmission.init(2);
    const Dummy = struct {
        pub fn handle(_: *@This(), _: @import("../api/http_types.zig").HttpRequest) !@import("../api/http_types.zig").HttpResponse {
            return error.UnexpectedPublicRequest;
        }
    };
    var dummy: Dummy = .{};
    const Server = @import("../../serverless_http_server.zig").ServerlessHttpServer;
    var server_a = Server.init(a, .{ .distributed_sql = .{ .factory = .{ .ptr = &scope_a, .open = Scope.open, .dispatcher = .{ .ptr = &scope_a, .dispatch = Scope.noDispatch } }, .authentication = authentication, .admission = &admission } }, &dummy);
    var server_b = Server.init(a, .{ .distributed_sql = .{ .factory = .{ .ptr = &scope_b, .open = Scope.open, .dispatcher = .{ .ptr = &scope_b, .dispatch = Scope.noDispatch } }, .authentication = authentication, .admission = &admission } }, &dummy);
    var listener_a = try @import("../../serverless_http_server.zig").HttpxRuntime.start(a, std.testing.io, &server_a);
    defer listener_a.deinit();
    var listener_b = try @import("../../serverless_http_server.zig").HttpxRuntime.start(a, std.testing.io, &server_b);
    defer listener_b.deinit();
    var executor = @import("../../raft/transport/mod.zig").StdHttpExecutor.init(a, .{});
    defer executor.deinit();
    var client: transport.Client = .{ .a = a, .executor = executor.executor(), .authentication = authentication, .workers = &.{ listener_a.base_uri, listener_b.base_uri } };
    var unsigned = try server_a.handle(.{ .method = .POST, .uri = transport.task_path, .body = "{}" });
    defer unsigned.deinit(a);
    try std.testing.expectEqual(@as(u16, 401), unsigned.status);
    const column: plan.Column = .{ .name = "id", .type = .integer };
    const input: plan.Input = .{ .table_id = 1, .schema = @splat('b'), .snapshot = "old-left", .wal_cut = 7, .cut = "retained-accepted-cut", .columns = &.{column}, .estimated_bytes = 100, .estimated_rows = 3, .splits = 2 };
    var right = input;
    right.table_id = 2;
    right.snapshot = "old-right";
    right.wal_cut = 8;
    const spec = try plan.choose(input, right, @splat('a'), 1000000, .{ .kind = .left, .left_keys = &.{0}, .right_keys = &.{0} }, .{}, 1000);
    server_a.cfg.distributed_sql.?.factory.dispatcher = client.dispatcher();
    server_b.cfg.distributed_sql.?.factory.dispatcher = client.dispatcher();
    var gateway = client;
    gateway.workers = &.{ "http://127.0.0.1:1", listener_a.base_uri, listener_b.base_uri };
    const query = try gateway.submit(spec, .{});
    try std.testing.expect(try client.result(sa, .{ .tenant = spec.tenant, .query = query }, .{}) == null);
    _ = try client.turn(.{ .tenant = spec.tenant, .query = query }, .{});
    // A gateway-created query survives losing its first query coordinator.
    fixture.now = 92001;
    // A persistently unavailable query earlier in the discovery page must
    // not hide later submissions or stall their coordinator recovery.
    var unavailable = spec;
    unavailable.expires_ms -= 1;
    const broken_query = try coordinator.submit(unavailable);
    const RejectOne = struct {
        client: *transport.Client,
        broken: state.Digest,
        fn dispatch(raw: *anyopaque, tickets: []const plan.Ticket, context: Context) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            for (tickets) |ticket| if (std.mem.eql(u8, &ticket.query, &self.broken)) return error.SqlWorkerUnavailable;
            const dispatcher_ = self.client.dispatcher();
            try dispatcher_.dispatch(dispatcher_.ptr, tickets, context);
        }
    };
    var reject: RejectOne = .{ .client = &client, .broken = broken_query };
    var driver: @import("driver.zig").Driver = .{ .service = coordinator, .dispatcher = .{ .ptr = &reject, .dispatch = RejectOne.dispatch } };
    defer driver.stop();
    var complete = false;
    for (0..64) |_| {
        _ = try driver.tick();
        if (try coordinator.pollResult(sa, query)) |result| {
            try std.testing.expectEqual(@as(u64, 4), result.rows);
            complete = true;
            break;
        }
    }
    try std.testing.expect(complete and fixture.opens >= 4);
    try std.testing.expectEqual(@as(u64, 4), (try client.result(sa, .{ .tenant = spec.tenant, .query = query }, .{})).?.rows);
    try std.testing.expectEqual(@as(usize, 0), admission.stats().in_flight);
    fixture.now = spec.expires_ms + 30001;
    var store = coordinator.state_store;
    store.now_ms = fixture.now;
    for (0..32) |_| _ = try @import("collector.zig").collect(sa, store, 2);
    for (0..32) |_| _ = try coordinator.exchange_store.collect(sa, fixture.now, 2);
    var client_store = memory.client();
    var leftovers = try client_store.listObjects("queries", .{ .prefix = "tenant/", .max_keys = 256 });
    defer leftovers.deinit(a);
    try std.testing.expectEqual(@as(usize, 0), leftovers.entries.len);
    var exchange_leftovers = try client_store.listObjects("exchange", .{ .prefix = "tenant/", .max_keys = 256 });
    defer exchange_leftovers.deinit(a);
    try std.testing.expectEqual(@as(usize, 0), exchange_leftovers.entries.len);
    try driver.start(std.testing.io);
    driver.stop();
}

test "distributed SQL compiler bridge preserves local SQL filters outer residuals ordering and typed results" {
    const a = std.testing.allocator;
    const Backend = struct {
        const Self = @This();
        fixture: *Fixture,
        captures: usize = 0,
        scanned: usize = 0,
        const Read = struct {
            a: A,
            fixture: *Fixture,
            backend: *Self,
            cursors: []local.sql_catalog.Cursor,
            owners: []Scan,
            fn close(raw: *anyopaque) void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                self.a.free(self.cursors);
                self.a.free(self.owners);
                self.a.destroy(self);
            }
        };
        const Scan = struct {
            fixture: *Fixture,
            backend: *Self,
            table: u64,
            position: usize = 0,
            fn capability(raw: *anyopaque, _: A) !?local.sql_catalog.ReadCapability {
                const self: *@This() = @ptrCast(@alignCast(raw));
                self.backend.captures += 1;
                return .{ .token = "retained-accepted-cut", .schema = @splat('b'), .snapshot = if (self.table == 1) "old-left" else "old-right", .wal_cut = if (self.table == 1) 7 else 8, .expires_ms = 1000000, .estimated_bytes = 100, .estimated_rows = if (self.table == 1) 3 else 4 };
            }
            fn close(_: *anyopaque) void {}
            fn next(raw: *anyopaque, a_: A, maximum: u32) !local.sql_catalog.Page {
                const self: *@This() = @ptrCast(@alignCast(raw));
                self.backend.scanned += 1;
                var arena = std.heap.ArenaAllocator.init(a_);
                errdefer arena.deinit();
                const sa = arena.allocator();
                var rows: std.ArrayList(local.sql_catalog.Row) = .empty;
                const count = if (self.table == 1) self.fixture.left.len else self.fixture.right.len;
                while (self.position < count and rows.items.len < maximum) {
                    const cells: []const Datum = if (self.table == 1) &self.fixture.left[self.position] else &self.fixture.right[self.position];
                    var object: std.json.Value = .{ .object = .empty };
                    try object.object.put(sa, "id", cells[0].value);
                    try rows.append(sa, .{ .id = "row", .version = 0, .value = object });
                    self.position += 1;
                }
                return .{ .rows = rows.items, .owned_arena = arena, .after = if (self.position == count) null else "more" };
            }
        };
        fn resolve(_: *anyopaque, _: A, name: local.sql_ast.Name, _: local.sql_catalog.Action) !local.sql_catalog.Table {
            return .{ .id = if (std.mem.eql(u8, name.table, "l")) 1 else 2, .physical_name = name.table, .schema_version = 1, .columns = &.{.{ .name = "id", .path = "id", .type = .integer }} };
        }
        fn scan(_: *anyopaque, _: A, _: local.sql_catalog.Table, _: local.sql_catalog.Scan) !local.sql_catalog.Page {
            return error.UnexpectedBackendCall;
        }
        fn mutate(_: *anyopaque, _: A, _: A, _: local.sql_catalog.Table, _: []const local.sql_catalog.Mutation) !local.sql_catalog.MutationOutcome {
            return error.UnexpectedBackendCall;
        }
        fn open(raw: *anyopaque, a_: A, scans: []const local.sql_catalog.StatementScan) !local.sql_catalog.StatementRead {
            const self: *Self = @ptrCast(@alignCast(raw));
            const owner = try a_.create(Read);
            errdefer a_.destroy(owner);
            const cursors = try a_.alloc(local.sql_catalog.Cursor, scans.len);
            errdefer a_.free(cursors);
            const owners = try a_.alloc(Scan, scans.len);
            for (scans, cursors, owners) |request, *cursor, *child| {
                child.* = .{ .fixture = self.fixture, .backend = self, .table = request.table.id };
                cursor.* = .{ .ptr = child, .next = Scan.next, .close = Scan.close, .estimated_bytes = 100, .estimated_rows = 4, .export_snapshot = .{ .ptr = child, .export_cut = Scan.capability } };
            }
            owner.* = .{ .a = a_, .fixture = self.fixture, .backend = self, .cursors = cursors, .owners = owners };
            return .{ .ptr = owner, .cursors = cursors, .close = Read.close };
        }
        fn checkpoint(_: *anyopaque) !void {}
        fn backend(self: *Self) local.sql_catalog.Backend {
            return .{ .ptr = self, .vtable = &.{ .resolve = resolve, .scan = scan, .mutate = mutate, .open_statement = open, .checkpoint = checkpoint } };
        }
    };
    const queries = [_][]const u8{
        "SELECT l.id, r.id FROM l JOIN r ON l.id=r.id ORDER BY l.id, r.id",
        "SELECT l.id, r.id FROM l LEFT JOIN r ON l.id=r.id AND r.id>5 ORDER BY l.id NULLS LAST, r.id NULLS LAST",
        "SELECT l.id, r.id FROM l FULL JOIN r ON l.id=r.id ORDER BY l.id NULLS LAST, r.id NULLS LAST",
        "SELECT l.id, r.id FROM l LEFT JOIN r ON l.id=r.id WHERE l.id>1 ORDER BY l.id, r.id",
        "SELECT count(*) AS n FROM l LEFT JOIN r ON l.id=r.id",
    };
    for (queries) |sql| {
        var memory = storage.MemoryClient.init(a);
        defer memory.deinit();
        var fixture: Fixture = .{};
        var backend: Backend = .{ .fixture = &fixture };
        var dispatch: Dispatch = .{ .workers = .{ fixture.service(a, memory.client(), "worker-a"), fixture.service(a, memory.client(), "worker-b") }, .drop_once = false };
        var bridge: @import("bridge.zig").Bridge = .{ .service = fixture.service(a, memory.client(), "coordinator"), .dispatcher = .{ .ptr = &dispatch, .dispatch = Dispatch.run }, .local_bytes = 0 };
        var compiled = try local.sql_compiler.compile(a, sql, .{});
        defer compiled.deinit();
        var ordinary = try local.sql_runtime.execute(a, backend.backend(), &compiled, &.{}, .{});
        defer ordinary.deinit();
        const baseline = try std.json.Stringify.valueAlloc(a, ordinary.output.rows, .{});
        defer a.free(baseline);
        backend.scanned = 0;
        var distributed = backend.backend();
        distributed.distributed_join = bridge.port();
        var actual = local.sql_runtime.execute(a, distributed, &compiled, &.{}, .{}) catch |err| {
            std.debug.print("failed distributed SQL parity query: {s}\n", .{sql});
            return err;
        };
        defer actual.deinit();
        const result = try std.json.Stringify.valueAlloc(a, actual.output.rows, .{});
        defer a.free(result);
        try std.testing.expectEqualStrings(baseline, result);
        try std.testing.expect(backend.captures >= 2);
        var canceled: bool = true;
        distributed.distributed_context.cancellation = .{ .ptr = &canceled, .is_cancelled_fn = struct {
            fn check(raw: *const anyopaque) bool {
                return @as(*const bool, @ptrCast(@alignCast(raw))).*;
            }
        }.check };
        try std.testing.expectError(error.Canceled, local.sql_runtime.execute(a, distributed, &compiled, &.{}, .{}));
        try std.testing.expectEqual(@as(usize, 0), backend.scanned);
    }
}

test "distributed SQL coordinator fills idle capacity while an earlier task remains leased" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const sa = arena.allocator();
    var memory = storage.MemoryClient.init(a);
    defer memory.deinit();
    var fixture: Fixture = .{};
    const coordinator = fixture.service(a, memory.client(), "coordinator");
    const SlowFirst = struct {
        worker: runtime.Service,
        fn run(raw: *anyopaque, tickets: []const plan.Ticket, _: Context) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            for (tickets) |ticket| {
                if (ticket.stage == 0 and ticket.task == 0) {
                    const key = try self.worker.state_store.key(self.worker.a, ticket.query, ticket.stage, ticket.task);
                    defer self.worker.a.free(key);
                    _ = try self.worker.state_store.claim(self.worker.a, key, "delayed-worker", 60000);
                } else try self.worker.execute(ticket);
            }
        }
    };
    var dispatch: SlowFirst = .{ .worker = fixture.service(a, memory.client(), "worker") };
    const input: plan.Input = .{ .table_id = 1, .schema = @splat('b'), .snapshot = "old-left", .wal_cut = 7, .cut = "retained-accepted-cut", .columns = &.{.{ .name = "id", .type = .integer }}, .estimated_bytes = 100, .estimated_rows = 3, .splits = 8 };
    var right = input;
    right.table_id = 2;
    right.snapshot = "old-right";
    right.wal_cut = 8;
    const spec = try plan.choose(input, right, @splat('a'), 1000000, .{ .left_keys = &.{0}, .right_keys = &.{0} }, .{ .concurrency = 2 }, fixture.now);
    const query = try coordinator.submit(spec);
    const claim = (try coordinator.acquire(query)).?;
    for (0..4) |_| _ = try coordinator.tick(query, claim, .{ .ptr = &dispatch, .dispatch = SlowFirst.run });
    var first = (try coordinator.state_store.load(sa, try coordinator.state_store.key(sa, query, 0, 0))).?;
    defer first.deinit(sa);
    var later = (try coordinator.state_store.load(sa, try coordinator.state_store.key(sa, query, 0, 3))).?;
    defer later.deinit(sa);
    try std.testing.expectEqual(state.Status.running, first.value.status);
    try std.testing.expectEqual(state.Status.succeeded, later.value.status);
}

test "distributed SQL task budget failure is terminal and reported to the query" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const sa = arena.allocator();
    var memory = storage.MemoryClient.init(a);
    defer memory.deinit();
    var fixture: Fixture = .{};
    const payload = try sa.alloc(u8, 1024 * 1024);
    @memset(payload, 'x');
    fixture.right[0][0] = Datum.json(.{ .string = payload });
    const coordinator = fixture.service(a, memory.client(), "coordinator");
    var dispatch: Dispatch = .{ .workers = .{ fixture.service(a, memory.client(), "worker-a"), fixture.service(a, memory.client(), "worker-b") }, .drop_once = false };
    const input: plan.Input = .{ .table_id = 1, .schema = @splat('b'), .snapshot = "old-left", .wal_cut = 7, .cut = "retained-accepted-cut", .columns = &.{.{ .name = "id", .type = .string }}, .estimated_bytes = 100, .estimated_rows = 3, .splits = 2 };
    var right = input;
    right.table_id = 2;
    right.snapshot = "old-right";
    right.wal_cut = 8;
    const spec = try plan.choose(input, right, @splat('a'), 1000000, .{ .left_keys = &.{0}, .right_keys = &.{0} }, .{ .task_memory = 1024 * 1024 }, fixture.now);
    const query = try coordinator.submit(spec);
    const claim = (try coordinator.acquire(query)).?;
    for (0..4) |_| if (try coordinator.tick(query, claim, .{ .ptr = &dispatch, .dispatch = Dispatch.run })) break;
    try std.testing.expectError(error.SqlJoinBudgetExceeded, coordinator.pollResult(sa, query));
}
