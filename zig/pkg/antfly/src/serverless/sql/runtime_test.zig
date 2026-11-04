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
    custom_right: ?[]const []const Datum = null,
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
        bucketed: bool = false,
        position: usize = 0,
        filter: ?*const local.sql_dynamic_filter.Filter,
        columns: []const plan.Column,
        fn next(raw: *anyopaque, a: A, maximum: usize) !?[]const []const Datum {
            const self: *InputStream = @ptrCast(@alignCast(raw));
            var rows: std.ArrayList([]const Datum) = .empty;
            const count: usize = if (self.input == 1) self.fixture.left.len else (if (self.fixture.custom_right) |rows_| rows_.len else self.fixture.right.len);
            while (self.position < count and rows.items.len < maximum) {
                const index = self.position;
                self.position += 1;
                if (!self.bucketed and index % self.partitions != self.split) continue;
                const cells: []const Datum = if (self.input == 1) &self.fixture.left[index] else if (self.fixture.custom_right) |rows_| rows_[index] else &self.fixture.right[index];
                if (self.bucketed) {
                    const hash = if (cells[0].sql_null) 0 else local.serverless_external_source_mod.lake_catalog.bucket.hashInteger(cells[0].value.integer) & 0x7fffffff;
                    if (hash % self.partitions != self.split) continue;
                }
                const projected = try a.alloc(Datum, self.columns.len);
                for (self.columns, projected) |column, *out| {
                    out.* = if (std.mem.eql(u8, column.name, "id")) cells[0] else if (std.mem.eql(u8, column.name, "payload")) cells[1] else if (std.mem.eql(u8, column.name, "flag")) cells[2] else if (std.mem.eql(u8, column.name, "_id")) Datum.json(.{ .string = "row" }) else return error.InvalidSqlBackendResponse;
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
        if (!std.mem.eql(u8, input.cut, "retained-accepted-cut") or split >= (if (input.bucket_group) |g| g.count else input.splits)) return error.SqlExecutionUnauthorized;
        self.opens += 1;
        if (filter != null) self.filters += 1;
        const stream = try a.create(InputStream);
        stream.* = .{ .a = a, .fixture = self, .input = input.table_id, .split = split, .partitions = if (input.bucket_group) |g| g.count else input.splits, .bucketed = input.bucket_group != null, .filter = filter, .columns = input.columns };
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
        for ([_]plan.Strategy{ .broadcast, .shuffle, .bucket }) |strategy| {
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
                    .partitions = if (strategy == .broadcast) 1 else 4,
                    .limits = .{},
                };
                if (strategy == .bucket) {
                    spec.left.bucket = .{ .column = 0, .count = 12 };
                    spec.right.bucket = .{ .column = 0, .count = 8 };
                }
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
                    try std.testing.expectEqualSlices(u16, &.{ 0, 4 }, (try replacement.resolvePhysical(sa, spec, record.value)).reduce_boundaries);
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
                try std.testing.expectEqual(@as(usize, 1), fixture.retained);
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
    try nativeLakeSource(false);
    try nativeLakeSource(true);
}
fn nativeLakeSource(bucketed: bool) !void {
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
    _ = try managed.create(sa, "create", if (bucketed) "{\"schema\":{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"id\",\"type\":\"long\",\"required\":true},{\"id\":2,\"name\":\"key\",\"type\":\"string\",\"required\":false}]},\"partition-spec\":{\"spec-id\":1,\"fields\":[{\"source-id\":2,\"field-id\":1000,\"name\":\"key_bucket\",\"transform\":\"bucket[4]\"}]}}" else "{\"schema\":{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"id\",\"type\":\"long\",\"required\":true}]}}", 1);
    _ = try ingestion.accept(sa, binding.binding, options, .{}, if (bucketed) "{\"batch_id\":\"base\",\"source\":\"s\",\"epoch\":\"1\",\"checkpoint\":\"1\",\"key_fields\":[\"id\"],\"changes\":[{\"op\":\"upsert\",\"row\":{\"id\":1,\"key\":\"old\"}},{\"op\":\"upsert\",\"row\":{\"id\":2,\"key\":\"deleted\"}}]}" else "{\"batch_id\":\"base\",\"source\":\"s\",\"epoch\":\"1\",\"checkpoint\":\"1\",\"key_fields\":[\"id\"],\"changes\":[{\"op\":\"upsert\",\"row\":{\"id\":1}},{\"op\":\"upsert\",\"row\":{\"id\":2}}]}");
    try std.testing.expect(try ingestion.drain(sa, binding.binding, options, .{}));
    _ = try ingestion.accept(sa, binding.binding, options, .{}, if (bucketed) "{\"batch_id\":\"overlay\",\"source\":\"s\",\"epoch\":\"1\",\"checkpoint\":\"2\",\"expected_checkpoint\":\"1\",\"key_fields\":[\"id\"],\"changes\":[{\"op\":\"delete\",\"row\":{\"id\":2}},{\"op\":\"upsert\",\"row\":{\"id\":1,\"key\":\"moved\"}},{\"op\":\"upsert\",\"row\":{\"id\":3,\"key\":\"new\"}}]}" else "{\"batch_id\":\"overlay\",\"source\":\"s\",\"epoch\":\"1\",\"checkpoint\":\"2\",\"expected_checkpoint\":\"1\",\"key_fields\":[\"id\"],\"changes\":[{\"op\":\"delete\",\"row\":{\"id\":2}},{\"op\":\"upsert\",\"row\":{\"id\":3}}]}");
    const Authorization = struct {
        table: local.sql_catalog.Table,
        fn resolve(raw: *anyopaque, _: A, tenant: state.Digest, id: u64, _: Context) !local.sql_catalog.Table {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (!std.mem.eql(u8, &tenant, &@as(state.Digest, @splat('a'))) or id != self.table.id) return error.SqlExecutionUnauthorized;
            return self.table;
        }
    };
    var auth: Authorization = .{ .table = .{ .id = 7, .physical_name = "events", .schema_version = 1, .external_base_source = binding, .columns = if (bucketed) &.{ .{ .name = "id", .path = "id", .type = .integer }, .{ .name = "key", .path = "key", .type = .string } } else &.{.{ .name = "id", .path = "id", .type = .integer }} } };
    const current_table = try managed.load(sa);
    const detected = try local.serverless_query_lake_schema.icebergSchema(sa, current_table.metadata_json, null);
    auth.table.external_base_source.?.binding.schema_fingerprint = detected.fingerprint;
    var objects = try @import("../../api/lake_index_store.zig").Store.open(a, &config, null, false);
    defer objects.deinit();
    var artifacts = objects.artifactStore();
    var host: @import("lake_source.zig").Host = .{ .artifacts = &artifacts, .store_identity = try @import("../../api/native_retained_cut.zig").storeIdentity(sa, objects.locator), .options = options, .io = std.testing.io, .authorization = .{ .ptr = &auth, .resolve = Authorization.resolve } };
    const now: u64 = @intCast(@import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms);
    const input = try host.capture(sa, @splat('a'), 7, if (bucketed) &.{ .{ .name = "id", .type = .integer }, .{ .name = "key", .type = .string } } else &.{.{ .name = "id", .type = .integer }}, 2, now + 600000, .{ .io = std.testing.io });
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
    const specification = try plan.choose(input, input, @splat('a'), now + 600000, .{ .left_keys = if (bucketed) &.{1} else &.{0}, .right_keys = if (bucketed) &.{1} else &.{0} }, .{}, now);
    try std.testing.expectEqual(if (bucketed) plan.Strategy.bucket else plan.Strategy.broadcast, specification.strategy);
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
        alloc: A,
        ready: std.atomic.Value(bool) = .init(false),
        pub fn handle(self: *@This(), request_: @import("../api/http_types.zig").HttpRequest) !@import("../api/http_types.zig").HttpResponse {
            if (!std.mem.eql(u8, request_.path, "/readyz")) return error.UnexpectedPublicRequest;
            return .{ .status = if (self.ready.load(.acquire)) 200 else 503, .content_type = try self.alloc.dupe(u8, "application/json"), .body = try self.alloc.dupe(u8, "{}") };
        }
    };
    var dummy: Dummy = .{ .alloc = a };
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
    const probe_uri = try std.fmt.allocPrint(sa, "{s}/readyz/sql", .{listener_a.base_uri});
    var unready = try executor.executor().execute(a, .{ .method = .GET, .uri = probe_uri });
    defer unready.deinit(a);
    try std.testing.expectEqual(@as(u16, 503), unready.status);
    dummy.ready.store(true, .release);
    var ready = try executor.executor().execute(a, .{ .method = .GET, .uri = probe_uri });
    defer ready.deinit(a);
    try std.testing.expectEqual(@as(u16, 200), ready.status);
    var unsupported = Server.init(a, .{}, &dummy);
    var absent = try unsupported.handle(.{ .method = .GET, .uri = "/readyz/sql" });
    defer absent.deinit(a);
    try std.testing.expectEqual(@as(u16, 503), absent.status);
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
        broken: state.Query,
        fn dispatch(raw: *anyopaque, tickets: []const plan.Ticket, context: Context) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            for (tickets) |ticket| if (std.meta.eql(ticket.query, self.broken)) return error.SqlWorkerUnavailable;
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
    const Http = local.common_http_http_common;
    const LostSubmit = struct {
        base: Http.RequestExecutor,
        dropped: bool = false,
        calls: usize = 0,
        fn execute(raw: *anyopaque, allocator: A, request_: Http.HttpRequest) !Http.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(raw));
            var response_ = try self.base.execute(allocator, request_);
            if (std.mem.endsWith(u8, request_.uri, transport.submit_path)) {
                self.calls += 1;
                if (!self.dropped) {
                    self.dropped = true;
                    response_.deinit(allocator);
                    return error.ConnectionResetByPeer;
                }
            }
            return response_;
        }
    };
    var lost: LostSubmit = .{ .base = executor.executor() };
    var single = client;
    single.workers = &.{listener_a.base_uri};
    single.executor = .{ .ptr = &lost, .vtable = &.{ .execute = LostSubmit.execute } };
    var abandoned = spec;
    abandoned.expires_ms -= 2;
    const abandoned_query = try single.submit(abandoned, .{});
    try std.testing.expectEqual(@as(usize, 2), lost.calls);
    try single.cancel(.{ .tenant = spec.tenant, .query = abandoned_query }, .{});
    for (0..4) |_| _ = try driver.tick();
    try std.testing.expectError(error.Canceled, single.result(sa, .{ .tenant = spec.tenant, .query = abandoned_query }, .{}));
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
                return .{ .token = "retained-accepted-cut", .schema = @splat('b'), .snapshot = if (self.table == 1) "old-left" else "old-right", .wal_cut = if (self.table == 1) 7 else 8, .expires_ms = 1000000, .estimated_bytes = if (self.table == 1) 400000 else 100, .estimated_rows = if (self.table == 1) 3 else 4 };
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
        var bridge: @import("bridge.zig").Bridge = .{ .service = fixture.service(a, memory.client(), "coordinator"), .dispatcher = .{ .ptr = &dispatch, .dispatch = Dispatch.run }, .local_bytes = 0, .limits = .{ .task_output_bytes = 16384 } };
        var compiled = try local.sql_compiler.compile(a, sql, .{});
        defer compiled.deinit();
        var ordinary = try local.sql_runtime.execute(a, backend.backend(), &compiled, &.{}, .{});
        defer ordinary.deinit();
        const baseline = try std.json.Stringify.valueAlloc(a, ordinary.output.rows, .{});
        defer a.free(baseline);
        backend.scanned = 0;
        const ReadGuard = struct {
            var canceled: bool = false;
            var reads: usize = 0;
            var original: *const storage.Client.VTable = undefined;
            fn check(raw: *const anyopaque) bool {
                return @as(*const bool, @ptrCast(@alignCast(raw))).*;
            }
            fn get(raw: *anyopaque, alloc: A, bucket: []const u8, key: []const u8, options: storage.GetOptions) !storage.GetResult {
                // Simulate cancellation while the transport owns a result read.
                // The borrowed token must observe it without waiting for GET.
                canceled = true;
                defer canceled = false;
                const token = options.cancellation orelse return error.MissingStatementCancellation;
                try std.testing.expectError(error.Canceled, token.check());
                reads += 1;
                canceled = false;
                return original.get_object(raw, alloc, bucket, key, options);
            }
        };
        ReadGuard.original = memory.client().vtable;
        ReadGuard.reads = 0;
        var guarded = ReadGuard.original.*;
        guarded.get_object = ReadGuard.get;
        bridge.service.exchange_store.client.vtable = &guarded;
        var distributed = backend.backend();
        distributed.distributed_context.cancellation = .{ .ptr = &ReadGuard.canceled, .is_cancelled_fn = ReadGuard.check };
        distributed.distributed_join = bridge.port();
        var actual = local.sql_runtime.execute(a, distributed, &compiled, &.{}, .{}) catch |err| {
            std.debug.print("failed distributed SQL parity query: {s}\n", .{sql});
            return err;
        };
        defer actual.deinit();
        const result = try std.json.Stringify.valueAlloc(a, actual.output.rows, .{});
        defer a.free(result);
        try std.testing.expectEqualStrings(baseline, result);
        try std.testing.expect(ReadGuard.reads > 0);
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

test "distributed SQL hosted bound-client execution reopens native cuts and typed Parquet" {
    try hostedLakeSource(false, false, false);
    try hostedLakeSource(true, false, false);
}
fn hostedLakeSource(bucketed: bool, large: bool, paged: bool) !void {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const sa = arena.allocator();
    const hosted = @import("hosted.zig");
    const lake = local.serverless_external_source_mod.lake_catalog;
    var memory = storage.MemoryClient.init(a);
    defer memory.deinit();
    var managed: lake.managed.Managed = .{ .client = memory.client(), .bucket = "source", .prefix = "table", .source_uri = "s3://source/table" };
    const initial = try managed.create(sa, "create", if (large) "{\"schema\":{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"id\",\"type\":\"long\",\"required\":true},{\"id\":2,\"name\":\"payload\",\"type\":\"string\",\"required\":false}]}}" else if (bucketed) "{\"schema\":{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"id\",\"type\":\"long\",\"required\":true}]},\"partition-spec\":{\"spec-id\":1,\"fields\":[{\"source-id\":1,\"field-id\":1000,\"name\":\"id_bucket\",\"transform\":\"bucket[4]\"}]}}" else "{\"schema\":{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"id\",\"type\":\"long\",\"required\":true}]}}", 1);
    const images = try std.json.parseFromSliceLeaky(std.json.Value, sa, "[{\"id\":1},{\"id\":2}]", .{});
    const batch: lake.row_commit.Batch = .{ .batch_id = "base", .source = "s", .epoch = "1", .checkpoint = "1", .key_fields = &.{"id"}, .changes = &.{ .{ .op = .upsert, .row = images.array.items[0] }, .{ .op = .upsert, .row = images.array.items[1] } } };
    var selected_batch = batch;
    if (large) {
        const changes = try sa.alloc(lake.row_commit.Batch.Change, 16384);
        for (changes, 1..) |*change, id| change.* = .{ .op = .upsert, .row = try std.json.parseFromSliceLeaky(std.json.Value, sa, try std.json.Stringify.valueAlloc(sa, .{ .id = id, .payload = "abcdefghijklmnopabcdefghijklmnop" }, .{}), .{}) };
        selected_batch.changes = changes;
    }
    const prepared = try lake.row_commit.prepare(sa, initial, .{ .client = memory.client(), .bucket = "source", .prefix = "table", .uri = "s3://source/table" }, selected_batch, 1, 2);
    const committed = try managed.commit(sa, .{ .id = "base", .expected_metadata_location = initial.metadata_location, .body = prepared.body, .timestamp_ms = 2 });
    const schema = "{\"base_source\":{\"kind\":\"external\",\"table_id\":\"events\",\"format\":\"iceberg\",\"uri\":\"s3://source/table\",\"schema_fingerprint\":\"auto\",\"write_policy\":\"iceberg_writer\",\"catalog\":{\"type\":\"managed\"}}}";
    const binding = (try local.serverless_external_source_schema_binding.externalBindingFromSchemaJsonAlloc(sa, schema)).?;
    var table: local.sql_catalog.Table = .{ .id = 7, .physical_name = "events", .schema_version = 1, .external_base_source = binding, .columns = if (large) &.{ .{ .name = "id", .path = "id", .type = .integer }, .{ .name = "payload", .path = "payload", .type = .string } } else &.{.{ .name = "id", .path = "id", .type = .integer }} };
    const detected = try local.serverless_query_lake_schema.icebergSchema(sa, committed.metadata_json, null);
    table.external_base_source.?.binding.schema_fingerprint = detected.fingerprint;
    const config: hosted.Config = .{ .state = .{ .bucket = "execution", .prefix = "state" }, .exchange = .{ .bucket = "execution", .prefix = "exchange" }, .artifacts = .{ .protocol = .s3, .connection = "source", .bucket = "source", .prefix = "artifacts", .root = "account.r2.cloudflarestorage.com" }, .source_buckets = &.{"source"} };
    var object_artifacts = try @import("../artifacts/object_store.zig").ObjectStore.initWithClientOptions(a, memory.client(), "source", "artifacts", .{ .skip_bucket_probe = true });
    var artifacts = object_artifacts.artifactStore();
    defer artifacts.deinit();
    const expires = @as(u64, @intCast(@import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms)) + 3600000;
    const root = try lake.metadata.parse(sa, committed.metadata_json);
    const snapshot = try std.fmt.allocPrint(sa, "{d}", .{try lake.metadata.int(try lake.metadata.get(root, "current-snapshot-id"))});
    const recent = try std.json.parseFromSliceLeaky(std.json.Value, sa, "[{\"id\":2},{\"id\":3}]", .{});
    var descriptor: @import("../../api/lake_sql_retained_cut.zig").Descriptor = .{ .table_id = 7, .schema_version = 1, .object_generation = 0, .expires_ms = expires, .metadata_location = committed.metadata_location, .metadata_json = committed.metadata_json, .table_uuid = try lake.metadata.str(try lake.metadata.get(root, "table-uuid")), .snapshot_id = snapshot, .pending = .{ .lsn = 2, .key_fields = &.{"id"}, .changes = &.{ .{ .op = .delete, .row = recent.array.items[0] }, .{ .op = .upsert, .row = recent.array.items[1] } } }, .authorization = .{ .scope = @splat('a'), .table = @import("../../api/lake_sql_retained_cut.zig").ReadTable.from(table) } };
    if (paged) {
        const changes = try sa.alloc(lake.row_commit.Batch.Change, 9000);
        const payload = try sa.alloc(u8, 1024);
        @memset(payload, 'x');
        for (changes, 1..) |*change, id| change.* = .{ .op = if (id == 2) .delete else .upsert, .row = try std.json.parseFromSliceLeaky(std.json.Value, sa, try std.json.Stringify.valueAlloc(sa, .{ .id = id, .payload = payload }, .{}), .{}) };
        const wide = try sa.alloc(u8, 64 * 1024);
        @memset(wide, 'w');
        try changes[0].row.object.put(sa, "payload", .{ .string = wide });
        descriptor.pending.changes = changes;
    }
    const identity = try @import("../../api/native_retained_cut.zig").storeIdentity(sa, config.artifacts);
    const saved = try @import("../../api/lake_sql_retained_cut.zig").saveDistributed(sa, &artifacts, identity, std.testing.io, descriptor, .none);
    try std.testing.expectEqual(paged, saved.paged);
    const cut = saved.token;
    if (paged) {
        const loaded = try @import("../../api/lake_sql_retained_cut.zig").load(sa, &artifacts, identity, cut, table, expires - 1000, .none);
        try std.testing.expect(loaded.paged == null);
        try std.testing.expectEqual(@as(usize, 9000), loaded.pending.changes.len);
        try std.testing.expectEqual(@as(usize, 64 * 1024), loaded.pending.changes[0].row.object.get("payload").?.string.len);
    }
    const input: plan.Input = .{ .table_id = 7, .schema = try @import("lake_source.zig").Host.fingerprint(sa, table), .snapshot = snapshot, .wal_cut = 2, .cut = cut, .columns = if (large) &.{ .{ .name = "id", .type = .integer }, .{ .name = "payload", .type = .string } } else &.{.{ .name = "id", .type = .integer }}, .estimated_bytes = if (large) 4 * 1024 * 1024 else 100, .estimated_rows = if (paged) 25384 else if (large) 16384 else 2, .splits = if (paged) 200 else if (large) 8 else 2, .bucket = if (bucketed) .{ .column = 0, .count = 4 } else null };
    const spec: plan.Plan = .{ .tenant = @splat('a'), .expires_ms = expires, .left = input, .right = input, .join = .{ .left_keys = &.{0}, .right_keys = &.{0}, .projection = if (large) &.{0} else &.{} }, .strategy = if (bucketed) .bucket else .shuffle, .partitions = if (paged) 64 else if (large) 16 else 4, .limits = .{ .query_requests = if (paged) 1000000 else 100000, .query_exchange_bytes = if (paged) 16 * 1024 * 1024 * 1024 else 8 * 1024 * 1024 * 1024, .task_memory = config.task_memory, .task_output_bytes = if (large) 2 * 1024 * 1024 else 16 * 1024 * 1024 } };
    const raw_plan = try spec.encode(sa);
    if (std.testing.environ.getPosix(if (paged) "ANTFLY_SQL_HOSTED_WAL_FIXTURE" else if (large) "ANTFLY_SQL_HOSTED_RANGE_FIXTURE" else if (bucketed) "ANTFLY_SQL_HOSTED_BUCKET_FIXTURE" else "ANTFLY_SQL_HOSTED_FIXTURE")) |path| {
        const Item = struct { bucket: []const u8, key: []const u8, body_base64: []const u8 };
        var items: std.ArrayList(Item) = .empty;
        var buckets = memory.buckets.iterator();
        while (buckets.next()) |bucket| {
            var objects = bucket.value_ptr.iterator();
            while (objects.next()) |object| {
                const bytes = object.value_ptr.body;
                const encoded = try sa.alloc(u8, std.base64.standard.Encoder.calcSize(bytes.len));
                _ = std.base64.standard.Encoder.encode(encoded, bytes);
                try items.append(sa, .{ .bucket = bucket.key_ptr.*, .key = object.key_ptr.*, .body_base64 = encoded });
            }
        }
        const encoded = try std.json.Stringify.valueAlloc(sa, .{ .config = config, .raw_plan = raw_plan, .tenant = spec.tenant, .objects = items.items, .expected_rows = if (large) @as(u64, 16383) else 2, .expected_pages = if (large) @as(?u32, null) else if (bucketed) @as(?u32, 4) else @as(?u32, 1) }, .{});
        try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = path, .data = encoded });
    }
    var request: hosted.Request = .{ .op = .sql_submit, .config = config, .tenant = spec.tenant, .owner = "hosted-coordinator", .raw_plan = raw_plan };
    const submit = try hosted.execute(sa, memory.client(), std.testing.io, .{ .io = std.testing.io }, try std.json.Stringify.valueAlloc(sa, request, .{}));
    request.query = (try std.json.parseFromSliceLeaky(struct { query: state.Query }, sa, submit, .{})).query;
    request.raw_plan = null;
    var complete = false;
    for (0..256) |_| {
        request.op = .sql_turn;
        const turn = try hosted.execute(sa, memory.client(), std.testing.io, .{ .io = std.testing.io }, try std.json.Stringify.valueAlloc(sa, request, .{}));
        const value = try std.json.parseFromSliceLeaky(struct { complete: bool, tickets: []const plan.Ticket }, sa, turn, .{});
        if (value.complete) {
            complete = true;
            break;
        }
        for (value.tickets) |ticket| {
            var worker = request;
            worker.op = .sql_execute;
            worker.owner = "hosted-worker";
            worker.ticket = ticket;
            _ = try hosted.execute(sa, memory.client(), std.testing.io, .{ .io = std.testing.io }, try std.json.Stringify.valueAlloc(sa, worker, .{}));
        }
    }
    try std.testing.expect(complete);
    request.op = .sql_result;
    const result = try hosted.execute(sa, memory.client(), std.testing.io, .{ .io = std.testing.io }, try std.json.Stringify.valueAlloc(sa, request, .{}));
    const value = try std.json.parseFromSliceLeaky(runtime.Result, sa, result, .{});
    try std.testing.expectEqual(if (large) @as(u64, 16383) else 2, value.rows);
    if (!large) try std.testing.expectEqual(if (bucketed) @as(usize, 4) else 1, value.pages.len);
    if (!large and !bucketed) if (std.testing.environ.getPosix("ANTFLY_SQL_HOSTED_SKEW_FIXTURE")) |path| try exportSkewFixture(sa, &memory, config, input, expires - 1, path);
}

test "distributed SQL cancellation is durable before dispatch and scope checked" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const sa = arena.allocator();
    var memory = storage.MemoryClient.init(a);
    defer memory.deinit();
    var fixture: Fixture = .{};
    const service = fixture.service(a, memory.client(), "coordinator");
    const column: plan.Column = .{ .name = "id", .type = .integer };
    const input: plan.Input = .{ .table_id = 1, .schema = @splat('b'), .snapshot = "old-left", .wal_cut = 7, .cut = "retained-accepted-cut", .columns = &.{column}, .estimated_bytes = 100, .estimated_rows = 3, .splits = 1 };
    var right = input;
    right.table_id = 2;
    right.snapshot = "old-right";
    right.wal_cut = 8;
    const spec: plan.Plan = .{ .tenant = @splat('a'), .expires_ms = 1000000, .left = input, .right = right, .join = .{ .left_keys = &.{0}, .right_keys = &.{0} }, .strategy = .broadcast, .partitions = 1, .limits = .{} };
    const query = try service.submit(spec);
    var stranger = service;
    stranger.tenant = @splat('b');
    try std.testing.expectError(error.SqlExecutionUnauthorized, stranger.requestCancel(query));
    try service.requestCancel(query);
    const before = fixture.retained;
    const claim = (try service.acquire(query)).?;
    var dispatch: Dispatch = .{ .workers = .{ service, service } };
    try std.testing.expect(try service.tick(query, claim, .{ .ptr = &dispatch, .dispatch = Dispatch.run }));
    try std.testing.expectEqual(before, fixture.retained);
    try std.testing.expectError(error.Canceled, service.pollResult(sa, query));
    try std.testing.expect((try service.state_store.load(sa, try service.state_store.key(sa, query, 0, 0))) == null);
}

test "distributed SQL maintenance deployment opens without source table credentials" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const sa = arena.allocator();
    var directory = try local.common_test_directory.TestDirectory.init("sql-maintenance-only");
    defer directory.cleanup();
    const uri = try std.fmt.allocPrint(sa, "file://{s}", .{directory.path()});
    const owned = try @import("deployment.zig").Owned.init(a, std.testing.io, .{ .state_uri = uri, .exchange_uri = uri, .workers = &.{"http://query-pool:12390"}, .authentication = .{ .secret = "0123456789abcdef0123456789abcdef", .issuer = "maintenance" } }, .maintenance_only, null, null, .serverless, null);
    defer owned.deinit();
    try std.testing.expect(owned.artifacts == null);
    try std.testing.expect(owned.host == null);
    try std.testing.expectError(error.SqlQueryRoleRequired, owned.service.acquire(.{ .id = @splat('a'), .expires_ms = 1000000 }));
}

test "distributed SQL skew tiles merge match evidence across pods for every join kind" {
    const Case = struct { kind: @FieldType(plan.Join, "kind"), rows: u64 };
    for ([_]Case{ .{ .kind = .inner, .rows = 4 }, .{ .kind = .left, .rows = 6 }, .{ .kind = .right, .rows = 8 }, .{ .kind = .full, .rows = 10 }, .{ .kind = .semi, .rows = 1 }, .{ .kind = .anti, .rows = 2 } }) |case| {
        for ([_]bool{ false, true }) |aggregate| {
            const a = std.testing.allocator;
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const sa = arena.allocator();
            var memory = storage.MemoryClient.init(a);
            defer memory.deinit();
            const payload = try sa.alloc(u8, 160 * 1024);
            @memset(payload, 'x');
            var wide: [8][3]Datum = undefined;
            var rows_: [8][]const Datum = undefined;
            for (&wide, &rows_, 0..) |*row, *reference, index| {
                row.* = .{ Datum.json(.{ .integer = 2 }), Datum.json(.{ .string = payload }), Datum.json(.{ .bool = index % 2 == 0 }) };
                reference.* = row;
            }
            var fixture: Fixture = .{ .custom_right = &rows_ };
            var service = fixture.service(a, memory.client(), "coordinator-a");
            var dispatch: Dispatch = .{ .workers = .{ fixture.service(a, memory.client(), "worker-a"), fixture.service(a, memory.client(), "worker-b") }, .drop_once = false };
            const column: plan.Column = .{ .name = "id", .type = .integer };
            const left: plan.Input = .{ .table_id = 1, .schema = @splat('b'), .snapshot = "old-left", .wal_cut = 7, .cut = "retained-accepted-cut", .columns = &.{column}, .estimated_bytes = 100, .estimated_rows = 3, .splits = 2 };
            var right = left;
            right.table_id = 2;
            right.snapshot = "old-right";
            right.wal_cut = 8;
            right.columns = &.{ column, .{ .name = "payload", .type = .string }, .{ .name = "flag", .type = .boolean } };
            right.splits = 4;
            right.estimated_bytes = 8 * 160 * 1024;
            right.estimated_rows = 8;
            var spec: plan.Plan = .{ .tenant = @splat('a'), .expires_ms = 1000000, .left = left, .right = right, .join = .{ .kind = case.kind, .left_keys = &.{0}, .right_keys = &.{0}, .projection = &.{0}, .condition = .{ .instructions = &.{.{ .type = .{ .kind = .boolean }, .operation = .{ .column = 3 } }}, .root = 0, .output = .{ .kind = .boolean } } }, .strategy = .shuffle, .partitions = 1, .limits = .{ .task_memory = 8 * 1024 * 1024, .task_output_bytes = 2 * 1024 * 1024, .query_exchange_bytes = 2 * 1024 * 1024 * 1024 } };
            if (aggregate) spec.aggregate = .{ .inputs = &.{null}, .specs = &.{.{ .kind = .count }} };
            const query = try service.submit(spec);
            var claim = (try service.acquire(query)).?;
            var complete = false;
            var moved = false;
            for (0..128) |_| {
                complete = try service.tick(query, claim, .{ .ptr = &dispatch, .dispatch = Dispatch.run });
                if (complete) break;
                const key = try service.state_store.key(sa, query, null, 0);
                var record = (try service.state_store.load(sa, key)).?;
                defer record.deinit(sa);
                if (record.value.stage == 2 and !moved) {
                    try std.testing.expectEqual(@as(usize, 1), (try service.resolvePhysical(sa, spec, record.value)).skew.len);
                    fixture.now += 92001;
                    service = fixture.service(a, memory.client(), "coordinator-b");
                    claim = (try service.acquire(query)).?;
                    moved = true;
                }
            }
            try std.testing.expect(complete);
            try std.testing.expect(moved);
            const result = (try service.pollResult(sa, query)).?;
            try std.testing.expectEqual(if (aggregate) @as(u64, 1) else case.rows, result.rows);
            if (aggregate) for (result.pages) |page| {
                var data = try service.exchange_store.read(sa, page, 0, 2 * 1024 * 1024);
                while (try data.next(sa, 2 * 1024 * 1024)) |block| {
                    defer block.deinit();
                    try std.testing.expectEqual(@as(i64, @intCast(case.rows)), (try block.cell(0, 0)).value.integer);
                }
            };
        }
    }
}

/// Seed accepted map outputs to stress the portable distributed join kernel.
/// Parquet/capability execution is covered by the independent source fixtures.
fn exportSkewFixture(a: A, memory: *storage.MemoryClient, config: @import("hosted.zig").Config, original: plan.Input, expires: u64, path: []const u8) !void {
    const hosted = @import("hosted.zig");
    var left = original;
    left.splits = 2;
    var right = original;
    right.splits = 300;
    right.estimated_rows = 153600;
    right.estimated_bytes = 2 * 1024 * 1024;
    const spec: plan.Plan = .{ .tenant = @splat('a'), .expires_ms = expires, .left = left, .right = right, .join = .{ .kind = .full, .left_keys = &.{0}, .right_keys = &.{0}, .projection = &.{0} }, .strategy = .shuffle, .partitions = 1, .limits = .{ .task_memory = config.task_memory, .task_output_bytes = 2 * 1024 * 1024, .query_exchange_bytes = 16 * 1024 * 1024 * 1024 } };
    const raw = try spec.encode(a);
    const submitted = try hosted.execute(a, memory.client(), std.testing.io, .{ .io = std.testing.io }, try std.json.Stringify.valueAlloc(a, hosted.Request{ .op = .sql_submit, .config = config, .tenant = spec.tenant, .owner = "seed", .raw_plan = raw }, .{}));
    const query = (try std.json.parseFromSliceLeaky(struct { query: state.Query }, a, submitted, .{})).query;
    const now: u64 = @intCast(@import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms);
    const store: state.Store = .{ .client = memory.client(), .bucket = config.state.bucket, .prefix = config.state.prefix, .now_ms = now };
    const objects: @import("exchange.zig").Store = .{ .client = memory.client(), .bucket = config.exchange.bucket, .prefix = config.exchange.prefix };
    for (0..302) |id| {
        var cells: [512][1]Datum = undefined;
        var rows_: [512]local.sql_operators.Row = undefined;
        const count: usize = if (id < 300) 512 else if (id == 300) 2 else 1;
        for (cells[0..count], rows_[0..count], 0..) |*row, *out, index| {
            row.* = .{if (id < 300) (if (index == 511) Datum{} else Datum.json(.{ .integer = 2 })) else if (id == 300) (if (index == 0) Datum.json(.{ .integer = 1 }) else Datum{}) else Datum.json(.{ .integer = 2 })};
            out.* = .{ .values = row, .keys = &.{}, .ordinal = index };
        }
        const key = try store.key(a, query, 0, @intCast(id));
        _ = try store.ensure(a, key, query.id, expires);
        const claim = (try store.claim(a, key, "seed-map", 60000)).?;
        const ref = try objects.put(a, query.id, expires, &.{rows_[0..count]}, 2 * 1024 * 1024);
        try store.complete(a, key, claim, ref);
    }
    const Item = struct { bucket: []const u8, key: []const u8, body_base64: []const u8 };
    var items: std.ArrayList(Item) = .empty;
    var buckets = memory.buckets.iterator();
    while (buckets.next()) |bucket| {
        var entries = bucket.value_ptr.iterator();
        while (entries.next()) |entry| {
            const bytes = entry.value_ptr.body;
            const encoded = try a.alloc(u8, std.base64.standard.Encoder.calcSize(bytes.len));
            _ = std.base64.standard.Encoder.encode(encoded, bytes);
            try items.append(a, .{ .bucket = bucket.key_ptr.*, .key = entry.key_ptr.*, .body_base64 = encoded });
        }
    }
    try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = path, .data = try std.json.Stringify.valueAlloc(a, .{ .config = config, .raw_plan = raw, .tenant = spec.tenant, .query = query, .expected_rows = 153602, .expected_pages = 902, .objects = items.items }, .{}) });
}

test "distributed SQL row ranges bound a large single Parquet row group" {
    try hostedLakeSource(false, true, false);
}

test "distributed SQL paged layouts exceed legacy metadata sizes and remain recoverable" {
    try pagedLayout(96, 128, false, 4096, false);
    try pagedLayout(1000, 1, true, 4096, false);
    try pagedLayout(40, 1, true, 64, true);
}
fn pagedLayout(maps: u32, partitions: u16, hot: bool, max_tasks: u32, budget_failure: bool) !void {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const sa = arena.allocator();
    var memory = storage.MemoryClient.init(a);
    defer memory.deinit();
    var fixture: Fixture = .{};
    var service = fixture.service(a, memory.client(), "owner-a");
    const column: plan.Column = .{ .name = "id", .type = .integer };
    const left: plan.Input = .{ .table_id = 1, .schema = @splat('b'), .snapshot = "old-left", .wal_cut = 7, .cut = "retained-accepted-cut", .columns = &.{column}, .estimated_bytes = 1024, .estimated_rows = 1, .splits = 1 };
    var right = left;
    right.table_id = 2;
    right.snapshot = "old-right";
    right.wal_cut = 8;
    right.splits = maps;
    const spec: plan.Plan = .{ .tenant = @splat('a'), .expires_ms = 1000000, .left = left, .right = right, .join = .{ .kind = .full, .left_keys = &.{0}, .right_keys = &.{0} }, .strategy = .shuffle, .partitions = partitions, .limits = .{ .task_memory = 8 * 1024 * 1024, .task_output_bytes = 2 * 1024 * 1024, .query_exchange_bytes = 128 * 1024 * 1024 * 1024, .query_requests = 2000000, .max_tasks = max_tasks } };
    const query = try service.submit(spec);
    var claim = (try service.acquire(query)).?;
    const width: usize = if (hot) 512 else 1;
    const rows_ = try sa.alloc(local.sql_operators.Row, width);
    const cells = try sa.alloc([1]Datum, width);
    for (rows_, cells, 0..) |*row, *cell, id| {
        cell.* = .{if (hot and id == 511) Datum{} else Datum.json(.{ .integer = 2 })};
        row.* = .{ .values = cell, .keys = &.{}, .ordinal = id };
    }
    const runs = try sa.alloc([]const local.sql_operators.Row, partitions);
    @memset(runs, rows_);
    const ref = try service.exchange_store.put(sa, query.id, query.expires_ms, runs, 2 * 1024 * 1024);
    const left_cells = [_]Datum{Datum.json(.{ .integer = 987654 })};
    const left_rows = [_]local.sql_operators.Row{.{ .values = &left_cells, .keys = &.{}, .ordinal = 0 }};
    const left_runs = try sa.alloc([]const local.sql_operators.Row, partitions);
    @memset(left_runs, &left_rows);
    const left_ref = try service.exchange_store.put(sa, query.id, query.expires_ms, left_runs, 2 * 1024 * 1024);
    var records = service.state_store;
    records.now_ms = fixture.now;
    for (0..maps + 1) |id| {
        const key = try records.key(sa, query, 0, @intCast(id));
        _ = try records.ensure(sa, key, query.id, query.expires_ms);
        const owner = (try records.claim(sa, key, "seed", 60000)).?;
        try records.complete(sa, key, owner, if (id < maps) ref else left_ref);
    }
    const Ignore = struct {
        fn dispatch(_: *anyopaque, _: []const plan.Ticket, _: Context) !void {}
    };
    var moved = false;
    var advanced = false;
    const query_key = try records.key(sa, query, null, 0);
    for (0..512) |turn| {
        _ = try service.tick(query, claim, .{ .ptr = &fixture, .dispatch = Ignore.dispatch });
        var record = (try records.load(sa, query_key)).?;
        defer record.deinit(sa);
        if (record.value.terminal()) {
            try std.testing.expect(budget_failure);
            try std.testing.expectEqual(state.Status.failed, record.value.status);
            try std.testing.expectEqualStrings("SqlJoinBudgetExceeded", record.value.failure.?);
            return;
        }
        if (record.value.stage == 1) {
            try std.testing.expect(!budget_failure);
            try std.testing.expect(record.value.physical != null);
            try std.testing.expectEqual(@as(usize, 0), record.value.skew.len);
            const resolved = try service.resolvePhysical(sa, spec, record.value);
            if (hot) {
                try std.testing.expectEqual(maps + 1, resolved.skew[0].leftCount() + resolved.skew[0].rightCount());
                try std.testing.expectEqual(@as(usize, (maps + 127) / 128), resolved.skew[0].right_pages.len);
            }
            advanced = true;
            break;
        }
        if (turn == 2 and !moved) {
            fixture.now += 92001;
            service = fixture.service(a, memory.client(), "owner-b");
            claim = (try service.acquire(query)).?;
            moved = true;
        }
    }
    try std.testing.expect(advanced);
    try std.testing.expect(moved);
    var dispatch: Dispatch = .{ .workers = .{ service, service }, .drop_once = false };
    for (0..256) |_| {
        var record = (try records.load(sa, query_key)).?;
        defer record.deinit(sa);
        if (record.value.stage == spec.joinStage()) break;
        _ = try service.tick(query, claim, .{ .ptr = &dispatch, .dispatch = Dispatch.run });
    }
    _ = try service.tick(query, claim, .{ .ptr = &fixture, .dispatch = Ignore.dispatch });
    try service.execute(.{ .tenant = spec.tenant, .query = query, .stage = spec.joinStage(), .task = 0 });
    const task_key = try records.key(sa, query, spec.joinStage(), 0);
    var task = (try records.load(sa, task_key)).?;
    defer task.deinit(sa);
    try std.testing.expectEqual(state.Status.succeeded, task.value.status);
}

test "distributed SQL result manifests checkpoint bounded pages across coordinator takeover" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const sa = arena.allocator();
    var memory = storage.MemoryClient.init(a);
    defer memory.deinit();
    var fixture: Fixture = .{};
    var service = fixture.service(a, memory.client(), "result-a");
    const input: plan.Input = .{ .table_id = 1, .schema = @splat('b'), .snapshot = "old-left", .wal_cut = 7, .cut = "retained-accepted-cut", .columns = &.{.{ .name = "id", .type = .integer }}, .estimated_bytes = 100, .estimated_rows = 1000, .splits = 1000 };
    var right = input;
    right.splits = 1;
    right.snapshot = "old-right";
    right.wal_cut = 8;
    const spec: plan.Plan = .{ .tenant = service.tenant, .expires_ms = 1000000, .left = input, .right = right, .join = .{ .left_keys = &.{0}, .right_keys = &.{0} }, .strategy = .broadcast, .partitions = 1, .limits = .{ .task_output_bytes = 4096 } };
    const query = try service.submit(spec);
    var claim = (try service.acquire(query)).?;
    const key = try service.state_store.key(sa, query, null, 0);
    var records = service.state_store;
    records.now_ms = fixture.now;
    const row = [_]local.sql_operators.Row{.{ .values = &.{Datum.json(.{ .integer = 42 })}, .keys = &.{}, .ordinal = 0 }};
    const output = try service.exchange_store.put(sa, query.id, query.expires_ms, &.{&row}, 4096);
    for (0..1000) |id| {
        const task_key = try records.key(sa, query, 1, @intCast(id));
        try records.create(sa, task_key, query.id, query.expires_ms);
        const owner = (try records.claim(sa, task_key, "seed", 60000)).?;
        try records.complete(sa, task_key, owner, output);
    }
    try records.checkpointTasks(sa, key, claim, 2, 0, &.{});
    const Ignore = struct {
        fn run(_: *anyopaque, _: []const plan.Ticket, _: Context) !void {}
    };
    for (0..16) |turn| {
        try std.testing.expectEqual(turn == 15, try service.tick(query, claim, .{ .ptr = &fixture, .dispatch = Ignore.run }));
        if (turn == 3) {
            fixture.now += 92001;
            service = fixture.service(a, memory.client(), "result-b");
            claim = (try service.acquire(query)).?;
        }
    }
    const result = (try service.pollResult(sa, query)).?;
    try std.testing.expectEqual(@as(u64, 1000), result.rows);
    try std.testing.expectEqual(@as(usize, 0), result.pages.len);
    try std.testing.expectEqual(@as(usize, 1), result.manifests.len);
    var rows: u64 = 0;
    var next: ?state.Output = result.manifests[0];
    var chunks: usize = 0;
    while (next) |ref| {
        const page = try service.readResultPage(sa, ref);
        try std.testing.expect(page.pages.len <= 64);
        for (page.pages) |child| rows += child.rows;
        next = page.previous;
        chunks += 1;
    }
    try std.testing.expectEqual(@as(usize, 16), chunks);
    try std.testing.expectEqual(result.rows, rows);
}

test "distributed SQL bounded match trees preserve every join kind and partial aggregates" {
    const Case = struct { kind: @FieldType(plan.Join, "kind"), rows: u64 };
    for ([_]Case{ .{ .kind = .inner, .rows = 39 }, .{ .kind = .left, .rows = 41 }, .{ .kind = .right, .rows = 40 }, .{ .kind = .full, .rows = 42 }, .{ .kind = .semi, .rows = 1 }, .{ .kind = .anti, .rows = 2 } }) |case| {
        for ([_]bool{ false, true }) |aggregate| {
            const a = std.testing.allocator;
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const sa = arena.allocator();
            var memory = storage.MemoryClient.init(a);
            defer memory.deinit();
            var rows: [40][1]Datum = undefined;
            const values = try sa.alloc([]const Datum, rows.len);
            for (&rows, values, 0..) |*row, *value, id| {
                row.* = .{if (id == 39) Datum{} else Datum.json(.{ .integer = 2 })};
                value.* = row;
            }
            var fixture: Fixture = .{ .custom_right = values };
            var service = fixture.service(a, memory.client(), "tree-a");
            var dispatch: Dispatch = .{ .workers = .{ fixture.service(a, memory.client(), "worker-a"), fixture.service(a, memory.client(), "worker-b") } };
            const left: plan.Input = .{ .table_id = 1, .schema = @splat('b'), .snapshot = "old-left", .wal_cut = 7, .cut = "retained-accepted-cut", .columns = &.{.{ .name = "id", .type = .integer }}, .estimated_bytes = 100, .estimated_rows = 3, .splits = 1 };
            var right = left;
            right.table_id = 2;
            right.snapshot = "old-right";
            right.wal_cut = 8;
            right.splits = 40;
            right.estimated_rows = 40;
            var spec: plan.Plan = .{ .tenant = service.tenant, .expires_ms = 1000000, .left = left, .right = right, .join = .{ .kind = case.kind, .left_keys = &.{0}, .right_keys = &.{0} }, .strategy = .shuffle, .partitions = 1, .limits = .{ .task_scan_rows = 32, .task_output_bytes = 4096 } };
            if (aggregate) spec.aggregate = .{ .inputs = &.{null}, .specs = &.{.{ .kind = .count }} };
            const query = try service.submit(spec);
            var claim = (try service.acquire(query)).?;
            var moved = false;
            var complete = false;
            for (0..256) |_| {
                complete = try service.tick(query, claim, .{ .ptr = &dispatch, .dispatch = Dispatch.run });
                if (complete) break;
                var record = (try service.state_store.load(sa, try service.state_store.key(sa, query, null, 0))).?;
                defer record.deinit(sa);
                const resolved = try service.resolvePhysical(sa, spec, record.value);
                if (!moved and record.value.stage == resolved.joinStage() + 1 and resolved.matchLevels() != 0) {
                    fixture.now += 92001;
                    service = fixture.service(a, memory.client(), "tree-b");
                    claim = (try service.acquire(query)).?;
                    moved = true;
                }
            }
            try std.testing.expect(complete);
            // Inner joins need no match-evidence tree.
            if (case.kind != .inner and case.kind != .right) try std.testing.expect(moved);
            const result = (try service.pollResult(sa, query)).?;
            try std.testing.expectEqual(if (aggregate) @as(u64, 1) else case.rows, result.rows);
            if (aggregate) for (result.pages) |page| {
                var data = try service.exchange_store.read(sa, page, 0, 4096);
                while (try data.next(sa, 4096)) |block| {
                    defer block.deinit();
                    try std.testing.expectEqual(@as(i64, @intCast(case.rows)), (try block.cell(0, 0)).value.integer);
                }
            };
        }
    }
}

test "distributed SQL paged WAL bounds worker memory and preserves global changed-key suppression" {
    try hostedLakeSource(false, true, true);
}
