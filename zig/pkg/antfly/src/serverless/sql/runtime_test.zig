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
        for ([_]plan.Input{ spec.left, spec.right }) |input| {
            const left = input.table_id == 1;
            if (!std.mem.eql(u8, input.snapshot, if (left) "old-left" else "old-right") or input.wal_cut != (if (left) @as(u64, 7) else 8)) return error.SqlExecutionUnauthorized;
        }
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
                    out.* = if (std.mem.eql(u8, column.name, "id")) cells[0] else if (std.mem.eql(u8, column.name, "payload")) (if (cells.len > 1) cells[1] else Datum{}) else if (std.mem.eql(u8, column.name, "flag")) (if (cells.len > 2) cells[2] else Datum{}) else if (std.mem.eql(u8, column.name, "_id")) Datum.json(.{ .string = "row" }) else return error.InvalidSqlBackendResponse;
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
                var builds: @import("build_cache.zig").Cache = .{ .a = a, .bytes = 32 * 1024 * 1024 };
                defer builds.deinit();
                for (&dispatch.workers) |*worker| worker.build_cache = &builds;
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
                if (strategy == .broadcast and aggregate) try std.testing.expect(builds.hits > 0);
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
    try deployed.worker_cancellation.start();

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
    // Listener workers and recovery lanes genuinely run concurrently. Use the
    // production backend's conditional-write locking, not the single-threaded
    // in-memory fake, so the regression exercises real ownership semantics.
    var temporary = try local.common_test_directory.TestDirectory.init("distributed-sql-listeners");
    defer temporary.cleanup();
    var memory = try storage.FilesystemClient.initWithIo(a, temporary.path(), std.testing.io);
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
    var routing: transport.Routing = .{};
    var client: transport.Client = .{ .io = std.testing.io, .routing = &routing, .a = a, .executor = executor.executor(), .authentication = authentication, .workers = &.{ listener_a.base_uri, listener_b.base_uri } };
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
        reverse_estimates: bool = false,
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
                return .{ .token = "retained-accepted-cut", .schema = @splat('b'), .snapshot = if (self.table == 1) "old-left" else "old-right", .wal_cut = if (self.table == 1) 7 else 8, .expires_ms = 1000000, .estimated_bytes = if ((self.table == 1) != self.backend.reverse_estimates) 400000 else 100, .estimated_rows = if (self.table == 1) 3 else 4 };
            }
            fn close(_: *anyopaque) void {}
            fn next(raw: *anyopaque, a_: A, maximum: u32) !local.sql_catalog.Page {
                const self: *@This() = @ptrCast(@alignCast(raw));
                self.backend.scanned += 1;
                var arena = std.heap.ArenaAllocator.init(a_);
                errdefer arena.deinit();
                const sa = arena.allocator();
                var rows: std.ArrayList(local.sql_catalog.Row) = .empty;
                const count = if (self.table == 1) self.fixture.left.len else if (self.fixture.custom_right) |rows_| rows_.len else self.fixture.right.len;
                const layout = try local.sql_catalog.Row.TypedLayout.init(sa, &.{ "id", "payload", "flag" });
                while (self.position < count and rows.items.len < maximum) {
                    const cells: []const Datum = if (self.table == 1) &self.fixture.left[self.position] else if (self.fixture.custom_right) |rows_| rows_[self.position] else &self.fixture.right[self.position];
                    try rows.append(sa, try local.sql_catalog.Row.fromDatums(sa, "row", layout, &.{ cells[0], if (cells.len > 1) cells[1] else .{}, if (cells.len > 2) cells[2] else .{} }));
                    self.position += 1;
                }
                return .{ .rows = rows.items, .owned_arena = arena, .after = if (self.position == count) null else "more" };
            }
        };
        fn resolve(_: *anyopaque, _: A, name: local.sql_ast.Name, _: local.sql_catalog.Action) !local.sql_catalog.Table {
            return .{ .id = if (std.mem.eql(u8, name.table, "l")) 1 else 2, .physical_name = name.table, .schema_version = 1, .columns = &.{ .{ .name = "id", .path = "id", .type = .integer }, .{ .name = "payload", .path = "payload", .type = .json }, .{ .name = "flag", .path = "flag", .type = .array, .element_type = .int64 } } };
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
    const Remote = struct {
        service: runtime.Service,
        dispatcher: runtime.Dispatcher,
        polls: usize = 0,
        turns: usize = 0,
        fn execute(raw: *anyopaque, allocator: A, request_: local.common_http_http_common.HttpRequest) !local.common_http_http_common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const transport = @import("transport.zig");
            var arena = std.heap.ArenaAllocator.init(allocator);
            defer arena.deinit();
            const scratch = arena.allocator();
            if (std.mem.endsWith(u8, request_.uri, transport.submit_path)) {
                const input = try std.json.parseFromSliceLeaky(plan.Plan, scratch, request_.body, .{});
                const query = try self.service.submit(input);
                // Simulate an independent remote driver completing the work.
                // Visibility is delayed for several status polls below, so a
                // foreground turn cannot be hidden by immediate completion.
                var done = false;
                for (0..128) |_| {
                    done = try self.service.turn(query, self.dispatcher);
                    if (done) break;
                }
                try std.testing.expect(done);
                return .{ .status = 200, .owner_allocator = allocator, .body = try std.json.Stringify.valueAlloc(allocator, .{ .query = query }, .{}) };
            }
            if (std.mem.endsWith(u8, request_.uri, transport.result_path)) {
                self.polls += 1;
                if (self.polls < 4) return .{ .status = 200, .owner_allocator = allocator, .body = try allocator.dupe(u8, "null") };
                const input = try std.json.parseFromSliceLeaky(transport.QueryRequest, scratch, request_.body, .{});
                const result = try self.service.pollResult(scratch, input.query);
                return .{ .status = 200, .owner_allocator = allocator, .body = try std.json.Stringify.valueAlloc(allocator, result, .{}) };
            }
            if (std.mem.endsWith(u8, request_.uri, transport.turn_path)) self.turns += 1;
            return error.UnexpectedForegroundControl;
        }
    };
    const Query = struct { sql: []const u8, parameters: []const std.json.Value = &.{}, wide_payload: bool = false };
    const queries = [_]Query{
        .{ .sql = "SELECT l.id, r.payload, r.flag FROM l JOIN r ON l.id=r.id WHERE l.id=2 ORDER BY r.payload", .wide_payload = true },
        .{ .sql = "SELECT r.payload, count(*) FROM l JOIN r ON l.id=r.id GROUP BY r.payload ORDER BY r.payload", .wide_payload = true },
        .{ .sql = "SELECT l.id, r.id FROM l JOIN r ON l.id=r.id ORDER BY l.id, r.id" },
        .{ .sql = "SELECT l.id, r.id FROM l LEFT JOIN r ON l.id=r.id AND r.id>5 ORDER BY l.id NULLS LAST, r.id NULLS LAST" },
        .{ .sql = "SELECT l.id, r.id FROM l FULL JOIN r ON l.id=r.id ORDER BY l.id NULLS LAST, r.id NULLS LAST" },
        .{ .sql = "SELECT l.id, r.id FROM l LEFT JOIN r ON l.id=r.id WHERE l.id>1 ORDER BY l.id, r.id" },
        .{ .sql = "SELECT l.id, r.id FROM l RIGHT JOIN r ON l.id=r.id AND l.id>1 ORDER BY l.id NULLS LAST, r.id NULLS LAST" },
        .{ .sql = "SELECT count(*) AS n FROM l LEFT JOIN r ON l.id=r.id" },
        .{ .sql = "SELECT count(*) AS n FROM l JOIN r ON l.id=r.id" },
        .{ .sql = "SELECT count(*), sum(c.id), min(b.id), max(c.id) FROM l a JOIN r b ON a.id=b.id JOIN l c ON b.id=c.id", .wide_payload = true },
        .{ .sql = "SELECT b.id, count(*), sum(c.id) FROM l a JOIN r b ON a.id=b.id JOIN l c ON b.id=c.id GROUP BY b.id ORDER BY b.id", .wide_payload = true },
        .{ .sql = "SELECT l.id, r.id FROM l LEFT JOIN r ON l.id=r.id AND r.id>$1 ORDER BY l.id NULLS LAST, r.id NULLS LAST", .parameters = &.{.{ .integer = 5 }} },
        .{ .sql = "SELECT l.id, r.id FROM l JOIN r ON l.id=r.id AND NOT is_missing(r.id) ORDER BY l.id, r.id" },
        .{ .sql = "SELECT l.id, r.id FROM l LEFT JOIN r ON l.id=r.id AND r.id = ANY($1::bigint[]) ORDER BY l.id NULLS LAST, r.id NULLS LAST", .parameters = &.{.{ .string = "[-2:0]={1,6,NULL}" }} },
        .{ .sql = "SELECT l.id, r.id FROM l LEFT JOIN r ON l.id=r.id AND r.id::numeric < $1::numeric ORDER BY l.id NULLS LAST, r.id NULLS LAST", .parameters = &.{.{ .string = "5.00000000000000000001" }} },
        .{ .sql = "SELECT l.id, r.id FROM l LEFT JOIN r ON l.id=r.id AND r.id::numeric < 5.00000000000000000001 ORDER BY l.id NULLS LAST, r.id NULLS LAST" },
    };
    for ([_]bool{ false, true }) |reverse_estimates| for ([_]bool{ false, true }) |remote| for (queries) |query_case| {
        const sql = query_case.sql;
        var memory = storage.MemoryClient.init(a);
        defer memory.deinit();
        var fixture: Fixture = .{};
        var payload_arena = std.heap.ArenaAllocator.init(a);
        defer payload_arena.deinit();
        if (query_case.wide_payload) {
            const work = payload_arena.allocator();
            const text = try work.alloc(u8, 512);
            @memset(text, 'x');
            var json: std.json.Value = .{ .object = .empty };
            try json.object.put(work, "payload", .{ .string = text });
            try json.object.put(work, "exact", .{ .number_string = "9007199254740993.1250" });
            const array = try work.create(local.sql_array_value.Value);
            array.* = try local.sql_array_value.Value.init(.int64, &.{.{ .length = 2, .lower = -3 }}, &.{ Datum.json(.{ .integer = 9007199254740993 }), .{} }, .{});
            try json.object.put(work, "group", .{ .integer = 0 });
            var other = json;
            other.object = try json.object.clone(work);
            try other.object.put(work, "group", .{ .integer = 1 });
            const cells = try work.dupe(Datum, &.{ Datum.json(.{ .integer = 2 }), Datum.json(json), Datum.typedArray(array) });
            const other_cells = try work.dupe(Datum, &.{ cells[0], Datum.json(other), cells[2] });
            const rows = try work.alloc([]const Datum, 300);
            for (rows, 0..) |*row, index| row.* = if (index < 150) cells else other_cells;
            fixture.custom_right = rows;
        }
        var backend: Backend = .{ .fixture = &fixture, .reverse_estimates = reverse_estimates };
        var dispatch: Dispatch = .{ .workers = .{ fixture.service(a, memory.client(), "worker-a"), fixture.service(a, memory.client(), "worker-b") }, .drop_once = false };
        var bridge: @import("bridge.zig").Bridge = .{ .service = fixture.service(a, memory.client(), "coordinator"), .dispatcher = .{ .ptr = &dispatch, .dispatch = Dispatch.run }, .local_bytes = 0, .limits = .{ .task_output_bytes = if (query_case.wide_payload) 1 << 20 else 16384 } };
        var remote_service: Remote = .{ .service = bridge.service, .dispatcher = bridge.dispatcher };
        var routing: @import("routing.zig").Routing = .{};
        var client: @import("transport.zig").Client = .{ .a = a, .routing = &routing, .executor = .{ .ptr = &remote_service, .vtable = &.{ .execute = Remote.execute } }, .authentication = .{ .secret = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", .issuer = "test" }, .workers = &.{"http://query"} };
        if (remote) bridge.gateway = &client;
        var compiled = try local.sql_compiler.compile(a, sql, .{});
        defer compiled.deinit();
        const execution_limits: local.sql_runtime.Limits = .{ .result_rows = if (query_case.wide_payload) 512 else 128 };
        var ordinary = try local.sql_runtime.execute(a, backend.backend(), &compiled, query_case.parameters, execution_limits);
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
        var actual = local.sql_runtime.execute(a, distributed, &compiled, query_case.parameters, execution_limits) catch |err| {
            std.debug.print("failed distributed SQL parity query: {s}\n", .{sql});
            return err;
        };
        defer actual.deinit();
        const result = try std.json.Stringify.valueAlloc(a, actual.output.rows, .{});
        defer a.free(result);
        try std.testing.expectEqualStrings(baseline, result);
        if (remote) {
            try std.testing.expectEqual(@as(usize, 4), remote_service.polls);
            try std.testing.expectEqual(@as(usize, 0), remote_service.turns);
        }
        try std.testing.expect(ReadGuard.reads > 0);
        try std.testing.expect(backend.captures >= 2);
        const scanned_before_cancel = backend.scanned;
        var canceled: bool = true;
        distributed.distributed_context.cancellation = .{ .ptr = &canceled, .is_cancelled_fn = struct {
            fn check(raw: *const anyopaque) bool {
                return @as(*const bool, @ptrCast(@alignCast(raw))).*;
            }
        }.check };
        try std.testing.expectError(error.Canceled, local.sql_runtime.execute(a, distributed, &compiled, query_case.parameters, execution_limits));
        try std.testing.expectEqual(scanned_before_cancel, backend.scanned);
    };
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
    var publication_budget: local.sql_memory_budget = .{ .backing = a, .limit = 16 * 1024 * 1024 };
    var publication_arena = std.heap.ArenaAllocator.init(publication_budget.allocator());
    defer publication_arena.deinit();
    const saved = try @import("../../api/lake_sql_retained_cut.zig").saveDistributed(publication_arena.allocator(), &artifacts, identity, std.testing.io, descriptor, .none);
    // Only the capability survives publication, including for a 9 MB WAL.
    try std.testing.expect(publication_budget.live < 64 * 1024);
    try std.testing.expectEqual(paged, saved.paged);
    try std.testing.expectEqual(if (paged) @as(u32, 71) else 1, saved.minimumSplits());
    const cut = saved.token;
    if (paged) {
        const loaded = try @import("../../api/lake_sql_retained_cut.zig").load(sa, &artifacts, identity, cut, table, expires - 1000, .none);
        try std.testing.expect(loaded.paged == null);
        try std.testing.expectEqual(@as(usize, 9000), loaded.pending.changes.len);
        try std.testing.expectEqual(@as(usize, 64 * 1024), loaded.pending.changes[0].row.object.get("payload").?.string.len);
    }
    const input: plan.Input = .{ .table_id = 7, .schema = try @import("lake_source.zig").Host.fingerprint(sa, table), .snapshot = snapshot, .wal_cut = 2, .cut = cut, .columns = if (large) &.{ .{ .name = "id", .type = .integer }, .{ .name = "payload", .type = .string } } else &.{.{ .name = "id", .type = .integer }}, .estimated_bytes = if (large) 4 * 1024 * 1024 else 100, .estimated_rows = if (paged) 25384 else if (large) 16384 else 2, .splits = if (paged) saved.minimumSplits() else if (large) 8 else 2, .bucket = if (bucketed) .{ .column = 0, .count = 4 } else null };
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
    if (paged) {
        var undersized = spec;
        undersized.left.splits = saved.minimumSplits() - 1;
        var rejected = request;
        rejected.raw_plan = try undersized.encode(sa);
        try std.testing.expectError(error.SqlJoinBudgetExceeded, hosted.execute(sa, memory.client(), std.testing.io, .{ .io = std.testing.io }, try std.json.Stringify.valueAlloc(sa, rejected, .{})));
    }
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
            var builds: @import("build_cache.zig").Cache = .{ .a = a, .bytes = 32 * 1024 * 1024 };
            defer builds.deinit();
            for (&dispatch.workers) |*worker| worker.build_cache = &builds;
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
    try pagedLayout(96, 16, true, 16384, false);
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
    const metrics = storage.operation_metrics;
    const get_before = metrics.runtime.snapshot().calls[@backingInt(metrics.Operation.get)];
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
        var tile_checkpoint = false;
        if (hot and partitions > 1 and !moved) {
            if (record.value.adaptive) |progress_ref| {
                var client = service.exchange_store.client;
                client.allocator = sa;
                var raw = try client.getObject(service.exchange_store.bucket, progress_ref.key, .{});
                defer raw.deinit(sa);
                const progress = try std.json.parseFromSliceLeaky(struct { tile_buffers: ?state.Output = null }, sa, raw.body, .{ .ignore_unknown_fields = true });
                tile_checkpoint = progress.tile_buffers != null;
            }
        }
        if (!moved and (if (hot and partitions > 1) tile_checkpoint else turn == 2)) {
            fixture.now += 92001;
            service = fixture.service(a, memory.client(), "owner-b");
            claim = (try service.acquire(query)).?;
            moved = true;
        }
    }
    try std.testing.expect(advanced);
    try std.testing.expect(moved);
    if (hot and partitions > 1) {
        const gets = metrics.runtime.snapshot().calls[@backingInt(metrics.Operation.get)] - get_before;
        // Two shared passes plus bounded CAS/checkpoint overhead. A separate
        // map scan for each hot partition exceeds this bound by several times.
        try std.testing.expect(gets < @as(u64, maps + 1) * 24);
    }
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
    right.table_id = 2;
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

test "distributed SQL recovery retires terminal hints and backs off idle sweeps" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const sa = arena.allocator();
    var memory = storage.MemoryClient.init(a);
    defer memory.deinit();
    var fixture: Fixture = .{};
    const service = fixture.service(a, memory.client(), "query-pod");
    const store = service.state_store;
    var client = memory.client();
    const query: state.Query = .{ .id = @splat('a'), .expires_ms = 1000000 };
    const key = try store.key(sa, query, null, 0);
    try store.create(sa, key, query.id, query.expires_ms);
    const marker = "tenant/sql-query-expiry/00000000000001000000/" ++ "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.json";
    var result = try client.putObject("queries", marker, "{}", .{});
    result.deinit(a);
    const claim = (try store.claim(sa, key, "owner", 60000)).?;
    try store.terminate(sa, key, claim, .cancelled, "Cancelled");
    var listed = try client.listObjects("queries", .{ .prefix = "tenant/sql-query-expiry/" });
    defer listed.deinit(a);
    try std.testing.expectEqual(@as(usize, 0), listed.entries.len);
    // Recreate a legacy terminal marker; recovery must repair old leftovers.
    var legacy = try client.putObject("queries", marker, "{}", .{});
    legacy.deinit(a);
    const DispatchNone = struct {
        fn dispatch(_: *anyopaque, _: []const plan.Ticket, _: Context) !void {
            return error.UnexpectedDispatch;
        }
    };
    var driver: @import("driver.zig").Driver = .{ .service = service, .dispatcher = .{ .ptr = &fixture, .dispatch = DispatchNone.dispatch }, .principal_scopes = true };
    defer driver.stop();
    try std.testing.expectEqual(@as(usize, 0), try driver.tick());
    const metrics = storage.operation_metrics;
    const before = metrics.runtime.snapshot();
    for (0..16) |_| try std.testing.expectEqual(@as(usize, 0), try driver.tick());
    const after = metrics.runtime.snapshot();
    try std.testing.expectEqual(before.calls[@backingInt(metrics.Operation.get)], after.calls[@backingInt(metrics.Operation.get)]);
    try std.testing.expectEqual(@as(u32, 30000), driver.delay_ms);
    try std.testing.expect(driver.sleepDelay() >= 30000 and driver.sleepDelay() < 33750);
    // Foreign live leases require one record read, without plan I/O. Their
    // owner can be replaced after the standard claim grace following a crash.
    const column: plan.Column = .{ .name = "id", .type = .integer };
    const left: plan.Input = .{ .table_id = 1, .schema = @splat('b'), .snapshot = "old-left", .wal_cut = 7, .cut = "retained-accepted-cut", .columns = &.{column}, .estimated_bytes = 100, .estimated_rows = 3, .splits = 2 };
    var right = left;
    right.table_id = 2;
    right.snapshot = "old-right";
    right.wal_cut = 8;
    const spec = try plan.choose(left, right, service.tenant, 2000000, .{ .left_keys = &.{0}, .right_keys = &.{0} }, .{}, fixture.now);
    const foreign = try service.submit(spec);
    const foreign_key = try store.key(sa, foreign, null, 0);
    _ = (try store.claim(sa, foreign_key, "other-pod", 60000)).?;
    const leased_before = metrics.runtime.snapshot();
    try std.testing.expectEqual(@as(usize, 0), try driver.tick());
    const leased_after = metrics.runtime.snapshot();
    try std.testing.expectEqual(@as(u64, 1), leased_after.calls[@backingInt(metrics.Operation.get)] - leased_before.calls[@backingInt(metrics.Operation.get)]);
    fixture.now = 92001;
    _ = try driver.tick(); // wrap the discovery cursor
    _ = try driver.tick(); // reclaim the expired foreign owner
    var recovered = (try store.load(sa, foreign_key)).?;
    defer recovered.deinit(sa);
    try std.testing.expectEqualStrings(service.owner, recovered.value.owner);
    try std.testing.expectEqual(@as(u64, 2), recovered.value.epoch);
    // Existing expiry GC still sees terminal state after hint retirement.
    var expired = store;
    expired.now_ms = query.expires_ms + 30001;
    try std.testing.expectEqual(@as(usize, 1), try @import("collector.zig").collect(sa, expired, 64));
}

test "distributed SQL notifications interrupt idle waits and retain durable cancellation" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const sa = arena.allocator();
    var memory = storage.MemoryClient.init(a);
    defer memory.deinit();
    var fixture: Fixture = .{};
    var service = fixture.service(a, memory.client(), "query-pod");
    var dispatch: Dispatch = .{ .workers = .{ fixture.service(a, memory.client(), "worker-a"), fixture.service(a, memory.client(), "worker-b") }, .drop_once = false };
    var driver: @import("driver.zig").Driver = .{ .service = service, .dispatcher = .{ .ptr = &dispatch, .dispatch = Dispatch.run }, .io = std.testing.io };
    defer driver.stop();
    service.notification = driver.notification();
    for (0..16) |_| _ = try driver.tick();
    try std.testing.expect(driver.sleepDelay() >= 30000);
    const column: plan.Column = .{ .name = "id", .type = .integer };
    const left: plan.Input = .{ .table_id = 1, .schema = @splat('b'), .snapshot = "old-left", .wal_cut = 7, .cut = "retained-accepted-cut", .columns = &.{column}, .estimated_bytes = 100, .estimated_rows = 3, .splits = 2 };
    var right = left;
    right.table_id = 2;
    right.snapshot = "old-right";
    right.wal_cut = 8;
    var spec = try plan.choose(left, right, service.tenant, fixture.now + 15000, .{ .left_keys = &.{0}, .right_keys = &.{0} }, .{}, fixture.now);
    const Wait = struct {
        wake: *std.Io.Event,
        entered: std.Io.Event = .unset,
        done: std.Io.Event = .unset,
        fn run(self: *@This(), io: std.Io) void {
            self.entered.set(io);
            self.wake.waitTimeout(io, .{ .duration = .{ .raw = .fromSeconds(30), .clock = .awake } }) catch return;
            self.done.set(io);
        }
    };
    var waiter: Wait = .{ .wake = &driver.wake };
    var group: std.Io.Group = .init;
    defer group.cancel(std.testing.io);
    try group.concurrent(std.testing.io, Wait.run, .{ &waiter, std.testing.io });
    try waiter.entered.waitTimeout(std.testing.io, .{ .duration = .{ .raw = .fromSeconds(2), .clock = .awake } });
    const query = try service.submit(spec);
    try waiter.done.waitTimeout(std.testing.io, .{ .duration = .{ .raw = .fromSeconds(2), .clock = .awake } });
    try group.await(std.testing.io);
    // Repeated hints coalesce, and clearing a pre-wait signal cannot lose work.
    _ = try service.submit(spec);
    try std.testing.expectEqual(@as(usize, 1), driver.hint_count);
    driver.wake.reset();
    driver.after = try a.dupe(u8, "tenant/sql-query-expiry/99999999999999999999/z.json");
    // Failure after draining must preserve the hint and the fast retry delay.
    var failed = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    driver.service.a = failed.allocator();
    try std.testing.expectError(error.OutOfMemory, driver.tick());
    driver.service.a = a;
    try std.testing.expectEqual(@as(usize, 1), driver.hint_count);
    try std.testing.expectEqual(driver.interval_ms, driver.sleepDelay());
    var complete = false;
    for (0..64) |_| {
        _ = try driver.tick();
        if (try service.pollResult(sa, query)) |result| {
            try std.testing.expectEqual(@as(u64, 2), result.rows);
            complete = true;
            break;
        }
        fixture.now += driver.sleepDelay();
    }
    try std.testing.expect(complete and fixture.now < spec.expires_ms);
    // A cancellation notification arriving during a turn must remain signaled.
    spec.expires_ms -= 1;
    const cancelled = try service.submit(spec);
    const CancelDuringTurn = struct {
        dispatch: *Dispatch,
        service: runtime.Service,
        query: state.Query,
        once: bool = false,
        fn run(raw: *anyopaque, tickets: []const plan.Ticket, context: Context) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try Dispatch.run(self.dispatch, tickets, context);
            if (!self.once) {
                self.once = true;
                try self.service.requestCancel(self.query);
            }
        }
    };
    var cancel: CancelDuringTurn = .{ .dispatch = &dispatch, .service = service, .query = cancelled };
    driver.dispatcher = .{ .ptr = &cancel, .dispatch = CancelDuringTurn.run };
    driver.wake.reset();
    _ = try driver.tick();
    try std.testing.expect(driver.wake.isSet());
    _ = try driver.tick();
    try std.testing.expectError(error.Canceled, service.pollResult(sa, cancelled));
    // Bounded notification overflow still falls back to durable discovery.
    driver.dispatcher = .{ .ptr = &dispatch, .dispatch = Dispatch.run };
    const notification = driver.notification();
    for (0..300) |i| notification.notify(notification.ptr, service.tenant, .{ .id = local.serverless_external_source_mod.lake_catalog.types.digestHex(try std.fmt.allocPrint(sa, "missing-{d}", .{i})), .expires_ms = spec.expires_ms });
    try std.testing.expectEqual(@as(usize, 256), driver.hint_count);
    spec.expires_ms -= 1;
    const overflowed = try service.submit(spec);
    try std.testing.expectEqual(@as(usize, 256), driver.hint_count);
    var recovered = false;
    for (0..64) |_| {
        _ = try driver.tick();
        if (try service.pollResult(sa, overflowed)) |_| {
            recovered = true;
            break;
        }
        fixture.now += driver.sleepDelay();
    }
    try std.testing.expect(recovered);
}

test "distributed SQL asynchronous delivery isolates short queries and renews leases during slow RPCs" {
    const a = std.testing.allocator;
    var memory = storage.MemoryClient.init(a);
    defer memory.deinit();
    var fixture: Fixture = .{};
    var service = fixture.service(a, memory.client(), "query-pod");
    service.context.io = std.testing.io;
    const Delivery = struct {
        slow: state.Query = undefined,
        slow_entered: std.Io.Event = .unset,
        short_entered: std.Io.Event = .unset,
        release: std.Io.Event = .unset,
        calls: std.atomic.Value(u32) = .init(0),
        fn run(raw: *anyopaque, tickets: []const plan.Ticket, context: Context) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            _ = self.calls.fetchAdd(1, .monotonic);
            if (std.meta.eql(tickets[0].query, self.slow)) {
                self.slow_entered.set(context.io.?);
                try self.release.wait(context.io.?);
            } else self.short_entered.set(context.io.?);
        }
    };
    var delivery: Delivery = .{};
    const scheduler_mod = @import("scheduler.zig");
    var turns: scheduler_mod.Turns = .{};
    service.coordination = turns.port();
    var scheduler: scheduler_mod.Scheduler = .{ .io = std.testing.io, .clock = .{ .read = struct {
        fn read(_: ?*anyopaque) u64 {
            return 1000;
        }
    }.read }, .delivery = .{ .ptr = &delivery, .dispatch = Delivery.run } };
    turns.scheduler = &scheduler;
    var driver: @import("driver.zig").Driver = .{ .service = service, .dispatcher = scheduler.dispatcher(), .io = std.testing.io };
    defer driver.stop();
    scheduler.notification = driver.notification();
    service.notification = driver.notification();
    try scheduler.start(2);
    defer scheduler.stop();
    const column: plan.Column = .{ .name = "id", .type = .integer };
    const left: plan.Input = .{ .table_id = 1, .schema = @splat('b'), .snapshot = "old-left", .wal_cut = 7, .cut = "retained-accepted-cut", .columns = &.{column}, .estimated_bytes = 100, .estimated_rows = 3, .splits = 1 };
    var right = left;
    right.table_id = 2;
    right.snapshot = "old-right";
    right.wal_cut = 8;
    var spec = try plan.choose(left, right, service.tenant, fixture.now + 120000, .{ .left_keys = &.{0}, .right_keys = &.{0} }, .{}, fixture.now);
    delivery.slow = try service.submit(spec);
    const coordination = service.coordination.?;
    try std.testing.expect(coordination.enter(coordination.ptr, delivery.slow));
    try std.testing.expect(!try service.turn(delivery.slow, scheduler.dispatcher()));
    coordination.leave(coordination.ptr, delivery.slow);
    _ = try driver.tick();
    try delivery.slow_entered.waitTimeout(std.testing.io, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
    // Polls while delivery is outstanding do not renew/rewrite the query.
    const slow_key = try service.state_store.key(a, delivery.slow, null, 0);
    defer a.free(slow_key);
    var before = (try service.state_store.load(a, slow_key)).?;
    defer before.deinit(a);
    fixture.now += 1000;
    for (0..10) |_| {
        _ = try driver.tick();
        _ = try service.turn(delivery.slow, scheduler.dispatcher());
    }
    var after = (try service.state_store.load(a, slow_key)).?;
    defer after.deinit(a);
    try std.testing.expectEqual(before.value.until_ms, after.value.until_ms);
    // The first task remains parked throughout every following coordinator turn.
    spec.expires_ms = fixture.now + 15000;
    const short = try service.submit(spec);
    _ = try driver.tick();
    try delivery.short_entered.waitTimeout(std.testing.io, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
    try std.testing.expect(fixture.now < short.expires_ms);
    fixture.now += 45000;
    _ = try driver.tick();
    const key = try service.state_store.key(a, delivery.slow, null, 0);
    defer a.free(key);
    var current = (try service.state_store.load(a, key)).?;
    defer current.deinit(a);
    try std.testing.expectEqual(fixture.now + 60000, current.value.until_ms);
    // Replayed pending tickets must not launch a duplicate while RPC is parked.
    _ = try driver.tick();
    try std.testing.expectEqual(@as(u32, 2), delivery.calls.load(.acquire));
    try service.requestCancel(delivery.slow);
    _ = try driver.tick();
    try std.testing.expectError(error.Canceled, service.pollResult(a, delivery.slow));
    // stop joins/cancels the parked delivery; no request contexts are borrowed.
}

test "distributed SQL control binds tenant scope before results or ownership changes" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var memory = storage.MemoryClient.init(a);
    defer memory.deinit();
    var fixture: Fixture = .{};
    const service = fixture.service(a, memory.client(), "owner");
    const column: plan.Column = .{ .name = "id", .type = .integer };
    const left: plan.Input = .{ .table_id = 1, .schema = @splat('b'), .snapshot = "old-left", .wal_cut = 7, .cut = "retained-accepted-cut", .columns = &.{column}, .estimated_bytes = 100, .estimated_rows = 3, .splits = 1 };
    var right = left;
    right.table_id = 2;
    right.snapshot = "old-right";
    right.wal_cut = 8;
    const spec = try plan.choose(left, right, service.tenant, fixture.now + 120000, .{ .left_keys = &.{0}, .right_keys = &.{0} }, .{}, fixture.now);
    const query = try service.submit(spec);
    var dispatch: Dispatch = .{ .workers = .{ fixture.service(a, memory.client(), "worker-a"), fixture.service(a, memory.client(), "worker-b") }, .drop_once = false };
    for (0..32) |_| {
        if (try service.turn(query, .{ .ptr = &dispatch, .dispatch = Dispatch.run })) break;
    }
    var stranger = service;
    stranger.tenant = @splat('c');
    try std.testing.expectError(error.SqlExecutionUnauthorized, stranger.loadPlan(arena.allocator(), query));
    try std.testing.expectError(error.SqlExecutionUnauthorized, stranger.pollResult(arena.allocator(), query));
    const get = @backingInt(storage.operation_metrics.Operation.get);
    const get_before = storage.operation_metrics.runtime.snapshot().calls[get];
    try std.testing.expectEqual(@as(u64, 2), (try service.pollResult(arena.allocator(), query)).?.rows);
    try std.testing.expectEqual(@as(u64, 2), storage.operation_metrics.runtime.snapshot().calls[get] - get_before);
    // Pre-scope jobs remain readable only after validating their immutable plan.
    const result_key = try service.state_store.key(arena.allocator(), query, null, 0);
    var legacy = (try service.state_store.load(a, result_key)).?;
    defer legacy.deinit(a);
    var legacy_record = legacy.value;
    legacy_record.tenant = null;
    const legacy_bytes = try std.json.Stringify.valueAlloc(arena.allocator(), legacy_record, .{});
    var raw_client = memory.client();
    var written = try raw_client.compareAndPut(service.state_store.bucket, result_key, legacy_bytes, legacy.object.metadata.writeRevision(), .{});
    written.deinit(a);
    try std.testing.expectError(error.SqlExecutionUnauthorized, stranger.pollResult(arena.allocator(), query));
    try std.testing.expectEqual(@as(u64, 2), (try service.pollResult(arena.allocator(), query)).?.rows);
    var second_spec = spec;
    second_spec.expires_ms -= 1;
    const pending = try service.submit(second_spec);
    stranger.owner = "other-pod";
    try std.testing.expectError(error.SqlExecutionUnauthorized, stranger.turn(pending, .{ .ptr = &dispatch, .dispatch = Dispatch.run }));
    const key = try service.state_store.key(arena.allocator(), pending, null, 0);
    var claimed = (try service.state_store.load(a, key)).?;
    defer claimed.deinit(a);
    try std.testing.expectEqual(state.Status.pending, claimed.value.status);
    try std.testing.expectEqualStrings("", claimed.value.owner);
    try std.testing.expectError(error.SqlExecutionUnauthorized, stranger.acquire(pending));
    try std.testing.expectError(error.SqlExecutionUnauthorized, stranger.requestCancel(pending));
    const legitimate = (try service.acquire(pending)).?;
    try std.testing.expectError(error.SqlExecutionUnauthorized, stranger.cancel(pending, legitimate));
    try std.testing.expectError(error.SqlExecutionUnauthorized, stranger.execute(.{ .tenant = stranger.tenant, .query = pending, .stage = 0, .task = 0 }));
}

test "distributed SQL empty collection backs off independently of maintenance ticks" {
    const a = std.testing.allocator;
    var memory = storage.MemoryClient.init(a);
    defer memory.deinit();
    const collector: @import("collector.zig").Collector = .{
        .state_store = .{ .client = memory.client(), .bucket = "state", .prefix = "sql", .now_ms = 0 },
        .exchange_store = .{ .client = memory.client(), .bucket = "exchange", .prefix = "sql" },
    };
    const list = @backingInt(storage.operation_metrics.Operation.list);
    const before = storage.operation_metrics.runtime.snapshot().calls[list];
    var schedule: @import("collector.zig").Schedule = .{};
    for (0..10) |tick| try std.testing.expectEqual(@as(usize, 0), try schedule.runAt(a, collector, .{}, tick * 10));
    const after = storage.operation_metrics.runtime.snapshot().calls[list];
    try std.testing.expectEqual(@as(u64, 3), after - before);
    var now: u64 = schedule.next_ms;
    for (0..16) |_| {
        _ = try schedule.runAt(a, collector, .{}, now);
        now = schedule.next_ms;
    }
    try std.testing.expectEqual(@as(u32, 30_000), schedule.delay_ms);
    try std.testing.expectEqual(@as(u64, 30_000), schedule.waitMs(now - 30_000));
    // Newly visible expired objects drain promptly after the bounded idle wait.
    const store = collector.state_store;
    const query: state.Query = .{ .id = @splat('b'), .expires_ms = 1 };
    const key = try store.key(a, query, null, 0);
    defer a.free(key);
    try store.create(a, key, query.id, query.expires_ms);
    try std.testing.expectEqual(@as(usize, 1), try schedule.runAt(a, collector, .{}, now));
    try std.testing.expectEqual(now + 1, schedule.next_ms);
    try std.testing.expectEqual(@as(u32, 100), schedule.delay_ms);
    // Transient DELETE failure preserves a bounded retry instead of hot polling.
    try store.create(a, key, query.id, query.expires_ms);
    var faults = storage.ScriptedFaultClient.init(a, memory.client());
    defer faults.deinit();
    faults.next_delete = .{ .fail_before = error.ConnectionResetByPeer };
    var faulty = collector;
    faulty.state_store.client = faults.client();
    now = schedule.next_ms;
    try std.testing.expectError(error.ConnectionResetByPeer, schedule.runAt(a, faulty, .{}, now));
    try std.testing.expect(schedule.next_ms > now);
    try std.testing.expectEqual(@as(usize, 0), try schedule.runAt(a, faulty, .{}, now + 1));
    try std.testing.expectEqual(@as(usize, 1), try schedule.runAt(a, faulty, .{}, schedule.next_ms));
}

test "distributed SQL coordinator metadata lanes isolate blocked reads and preserve cancellation" {
    const a = std.testing.allocator;
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const path = try temporary.dir.realPathFileAlloc(std.testing.io, ".", a);
    defer a.free(path);
    var filesystem = try storage.FilesystemClient.initWithIo(a, path, std.testing.io);
    defer filesystem.deinit();
    var base = filesystem.client();
    try base.makeBucket("queries");
    try base.makeBucket("exchange");
    const Gate = struct {
        var current: *@This() = undefined;
        base_get: @FieldType(storage.Client.VTable, "get_object"),
        key: []const u8 = "",
        once: std.atomic.Value(bool) = .init(true),
        entered: std.Io.Event = .unset,
        finished: std.Io.Event = .unset,
        release: std.Io.Event = .unset,
        short: std.Io.Event = .unset,
        fn get(raw: *anyopaque, allocator: A, bucket: []const u8, key: []const u8, options: storage.GetOptions) !storage.GetResult {
            const self = current;
            var value = try self.base_get(raw, allocator, bucket, key, options);
            errdefer value.deinit(allocator);
            if (std.mem.eql(u8, self.key, key) and self.once.swap(false, .acq_rel)) {
                self.entered.set(std.testing.io);
                defer self.finished.set(std.testing.io);
                while (!self.release.isSet()) {
                    if (options.cancellation) |token| try token.check();
                    self.release.waitTimeout(std.testing.io, .{ .duration = .{ .raw = .fromMilliseconds(10), .clock = .awake } }) catch |err| switch (err) {
                        error.Timeout => continue,
                        error.Canceled => return err,
                    };
                }
            }
            return value;
        }
        fn dispatch(raw: *anyopaque, tickets: []const plan.Ticket, _: Context) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            for (tickets) |ticket| if (ticket.query.expires_ms == 16000) self.short.set(std.testing.io);
        }
    };
    var gate: Gate = .{ .base_get = base.vtable.get_object };
    Gate.current = &gate;
    var table = base.vtable.*;
    table.get_object = Gate.get;
    var client = base;
    client.vtable = &table;
    var fixture: Fixture = .{};
    var service = fixture.service(a, client, "query-pod");
    service.context.io = std.testing.io;
    var turns: @import("scheduler.zig").Turns = .{};
    service.coordination = turns.port();
    var driver: @import("driver.zig").Driver = .{ .service = service, .dispatcher = .{ .ptr = &gate, .dispatch = Gate.dispatch }, .metadata_budget_ms = 10000 };
    defer driver.stop();
    service.notification = driver.notification();
    const column: plan.Column = .{ .name = "id", .type = .integer };
    const left: plan.Input = .{ .table_id = 1, .schema = @splat('b'), .snapshot = "old-left", .wal_cut = 7, .cut = "retained-accepted-cut", .columns = &.{column}, .estimated_bytes = 100, .estimated_rows = 3, .splits = 1 };
    var right = left;
    right.table_id = 2;
    right.snapshot = "old-right";
    right.wal_cut = 8;
    var spec = try plan.choose(left, right, service.tenant, 121000, .{ .left_keys = &.{0}, .right_keys = &.{0} }, .{}, fixture.now);
    const slow = try service.submit(spec);
    gate.key = try service.state_store.key(a, slow, null, 0);
    defer a.free(gate.key);
    try driver.start(std.testing.io);
    // stop must join every metadata worker before its borrowed key is freed.
    defer driver.stop();
    try gate.entered.waitTimeout(std.testing.io, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
    spec.expires_ms = 16000;
    const short = try service.submit(spec);
    try gate.short.waitTimeout(std.testing.io, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
    try std.testing.expect(!gate.finished.isSet());
    try service.requestCancel(short);
    // A short query's cancellation also settles while the other GET is parked.
    for (0..100) |_| {
        const key = try service.state_store.key(a, short, null, 0);
        defer a.free(key);
        var record = (try service.state_store.load(a, key)).?;
        defer record.deinit(a);
        if (record.value.status == .cancelled) break;
        try std.Io.sleep(std.testing.io, .fromMilliseconds(10), .awake);
    }
    try std.testing.expectError(error.Canceled, service.pollResult(a, short));
    try std.testing.expect(!gate.finished.isSet());
    // Group cancellation interrupts the blocked provider and joins its worker.
    driver.stop();
    try std.testing.expect(gate.finished.isSet());
}

test "distributed SQL metadata quanta refill a wide dispatch window before completion" {
    const a = std.testing.allocator;
    var memory = storage.MemoryClient.init(a);
    defer memory.deinit();
    var fixture: Fixture = .{};
    var service = fixture.service(a, memory.client(), "owner");
    var scheduler: @import("scheduler.zig").Scheduler = .{ .io = std.testing.io, .clock = service.clock, .delivery = undefined };
    defer scheduler.stop();
    var turns: @import("scheduler.zig").Turns = .{ .scheduler = &scheduler };
    service.coordination = turns.port();
    const column: plan.Column = .{ .name = "id", .type = .integer };
    const left: plan.Input = .{ .table_id = 1, .schema = @splat('b'), .snapshot = "old-left", .wal_cut = 7, .cut = "retained-accepted-cut", .columns = &.{column}, .estimated_bytes = 100, .estimated_rows = 3, .splits = 1 };
    var right = left;
    right.table_id = 2;
    right.snapshot = "old-right";
    right.wal_cut = 8;
    right.splits = 32;
    var spec = try plan.choose(left, right, service.tenant, 121000, .{ .left_keys = &.{0}, .right_keys = &.{0} }, .{ .concurrency = 32, .query_exchange_bytes = 8 * 1024 * 1024 * 1024 }, fixture.now);
    const query = try service.submit(spec);
    for (0..4) |_| try std.testing.expect(!try service.turn(query, scheduler.dispatcher()));
    const key = try service.state_store.key(a, query, null, 0);
    defer a.free(key);
    var value = (try service.state_store.load(a, key)).?;
    defer value.deinit(a);
    try std.testing.expectEqual(@as(usize, 32), value.value.pending.len);
    try std.testing.expectEqual(@as(u32, 32), value.value.cursor);
    const get = @backingInt(storage.operation_metrics.Operation.get);
    const before = storage.operation_metrics.runtime.snapshot().calls[get];
    try std.testing.expect(!try service.turn(query, scheduler.dispatcher()));
    try std.testing.expectEqual(@as(u64, 1), storage.operation_metrics.runtime.snapshot().calls[get] - before);
    // RPC-facing turns authorize and notify, without claiming or executing work.
    spec.expires_ms -= 1;
    const other = try service.submit(spec);
    service.async_control = true;
    try std.testing.expect(!try service.turn(other, undefined));
    const other_key = try service.state_store.key(a, other, null, 0);
    defer a.free(other_key);
    var untouched = (try service.state_store.load(a, other_key)).?;
    defer untouched.deinit(a);
    try std.testing.expectEqual(state.Status.pending, untouched.value.status);
}

test "distributed SQL result assembly checkpoints slow storage within its turn and survives takeover" {
    try slowResultAssembly(false);
    try slowResultAssembly(true);
}
fn slowResultAssembly(parallel: bool) !void {
    const a = std.testing.allocator;
    var memory = storage.MemoryClient.init(a);
    defer memory.deinit();
    var fixture: Fixture = .{};
    var service = fixture.service(a, memory.client(), "owner");
    const column: plan.Column = .{ .name = "id", .type = .integer };
    const left: plan.Input = .{ .table_id = 1, .schema = @splat('b'), .snapshot = "old-left", .wal_cut = 7, .cut = "retained-accepted-cut", .columns = &.{column}, .estimated_bytes = 100, .estimated_rows = 3, .splits = 64 };
    var right = left;
    right.table_id = 2;
    right.snapshot = "old-right";
    right.wal_cut = 8;
    right.splits = 1;
    var spec = try plan.choose(left, right, service.tenant, 121000, .{ .left_keys = &.{0}, .right_keys = &.{0} }, .{ .query_exchange_bytes = 16 * 1024 * 1024 * 1024 }, fixture.now);
    // Exercise coordinator fan-out independently of strategy cost heuristics.
    spec.strategy = .broadcast;
    spec.partitions = 1;
    try spec.validate(fixture.now);
    try std.testing.expectEqual(@as(u32, 64), try spec.stageTasks(spec.finalStage() - 1));
    const query = try service.submit(spec);
    const claim = (try service.acquire(query)).?;
    var records = service.state_store;
    records.tenant = service.tenant;
    const query_key = try records.key(a, query, null, 0);
    defer a.free(query_key);
    for (0..64) |id| {
        const key = try records.key(a, query, spec.finalStage() - 1, @intCast(id));
        defer a.free(key);
        try records.create(a, key, query.id, query.expires_ms);
        const task_claim = (try records.claim(a, key, "worker", 60000)).?;
        const output_key = try std.fmt.allocPrint(a, "accepted-{d}", .{id});
        defer a.free(output_key);
        try records.complete(a, key, task_claim, .{ .key = output_key, .hash = @splat('c'), .bytes = 0, .rows = 1 });
    }
    try records.checkpointTasks(a, query_key, claim, spec.finalStage(), 0, &.{});
    const Slow = struct {
        var base_get: @FieldType(storage.Client.VTable, "get_object") = undefined;
        fn get(raw: *anyopaque, allocator: A, bucket: []const u8, key: []const u8, options: storage.GetOptions) !storage.GetResult {
            if (std.mem.indexOf(u8, key, "/stage/") != null) try std.Io.sleep(std.testing.io, .fromMilliseconds(50), .awake);
            if (options.cancellation) |token| try token.check();
            return base_get(raw, allocator, bucket, key, options);
        }
    };
    Slow.base_get = service.state_store.client.vtable.get_object;
    var table = service.state_store.client.vtable.*;
    table.get_object = Slow.get;
    service.state_store.client.vtable = &table;
    var turns: @import("scheduler.zig").Turns = .{};
    service.coordination = turns.port();
    var complete = false;
    var moved = false;
    for (0..16) |turn| {
        service.context.io = if (parallel) std.testing.io else null;
        service.context.deadline_ns = @import("antfly_platform").time.monotonicNs() + std.time.ns_per_s;
        complete = try service.turn(query, undefined);
        if (complete) break;
        var current = (try records.load(a, query_key)).?;
        defer current.deinit(a);
        try std.testing.expect(current.value.result_progress != null);
        if (turn == 0) {
            fixture.now += 92001;
            service = fixture.service(a, service.state_store.client, "replacement");
            service.coordination = turns.port();
            moved = true;
        }
    }
    try std.testing.expect(complete);
    if (!parallel) try std.testing.expect(moved);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    service.context = .{};
    const result = (try service.pollResult(arena.allocator(), query)).?;
    try std.testing.expectEqual(@as(u64, 64), result.rows);
    var seen: [64]bool = @splat(false);
    const Check = struct {
        fn pages(values: []const state.Output, visited: *[64]bool) !void {
            for (values) |page| {
                const id = try std.fmt.parseInt(usize, page.key["accepted-".len..], 10);
                try std.testing.expect(id < visited.len and !visited[id]);
                visited[id] = true;
            }
        }
    };
    try Check.pages(result.pages, &seen);
    for (result.manifests) |head| {
        var next: ?state.Output = head;
        while (next) |ref| {
            const page = try service.readResultPage(arena.allocator(), ref);
            try Check.pages(page.pages, &seen);
            next = page.previous;
        }
    }
    for (seen) |present| try std.testing.expect(present);
}

test "distributed SQL pending reconciliation resumes after takeover and revisits slow early tasks" {
    try reconcileWindow(false);
    try reconcileWindow(true);
}
fn reconcileWindow(early_running: bool) !void {
    const a = std.testing.allocator;
    var memory = storage.MemoryClient.init(a);
    defer memory.deinit();
    var fixture: Fixture = .{};
    var service = fixture.service(a, memory.client(), "owner");
    const column: plan.Column = .{ .name = "id", .type = .integer };
    const left: plan.Input = .{ .table_id = 1, .schema = @splat('b'), .snapshot = "old-left", .wal_cut = 7, .cut = "retained-accepted-cut", .columns = &.{column}, .estimated_bytes = 100, .estimated_rows = 3, .splits = 256 };
    var right = left;
    right.table_id = 2;
    right.snapshot = "old-right";
    right.wal_cut = 8;
    right.splits = 1;
    var spec = try plan.choose(left, right, service.tenant, 1000000, .{ .left_keys = &.{0}, .right_keys = &.{0} }, .{ .concurrency = 256, .task_output_bytes = 4096 }, fixture.now);
    // Exercise coordinator fan-out independently of strategy cost heuristics.
    spec.strategy = .broadcast;
    spec.partitions = 1;
    try spec.validate(fixture.now);
    const query = try service.submit(spec);
    const claim = (try service.acquire(query)).?;
    var records = service.state_store;
    records.tenant = service.tenant;
    const query_key = try records.key(a, query, null, 0);
    defer a.free(query_key);
    var ids: [256]u32 = undefined;
    var held: ?state.Claim = null;
    for (&ids, 0..) |*id, index| {
        id.* = @intCast(index);
        const key = try records.key(a, query, spec.outputStage(), id.*);
        defer a.free(key);
        try records.create(a, key, query.id, query.expires_ms);
        const task_claim = (try records.claim(a, key, "worker", 60000)).?;
        if (early_running and index == 0) {
            held = task_claim;
            continue;
        }
        try records.complete(a, key, task_claim, .{ .key = "accepted", .hash = @splat('c'), .bytes = 0, .rows = 1 });
    }
    try records.checkpointTasks(a, query_key, claim, spec.outputStage(), 256, &ids);
    const Slow = struct {
        var base_get: @FieldType(storage.Client.VTable, "get_object") = undefined;
        var slow_first: std.atomic.Value(bool) = .init(false);
        fn get(raw: *anyopaque, allocator: A, bucket: []const u8, key: []const u8, options: storage.GetOptions) !storage.GetResult {
            if (std.mem.indexOf(u8, key, "/stage/") != null) {
                const delay: i64 = if (std.mem.endsWith(u8, key, "/task/0.json") and slow_first.swap(false, .acq_rel)) 600 else 100;
                try std.Io.sleep(std.testing.io, .fromMilliseconds(delay), .awake);
            }
            if (options.cancellation) |token| try token.check();
            return base_get(raw, allocator, bucket, key, options);
        }
    };
    Slow.base_get = service.state_store.client.vtable.get_object;
    Slow.slow_first.store(early_running, .release);
    var table = service.state_store.client.vtable.*;
    table.get_object = Slow.get;
    service.state_store.client.vtable = &table;
    service.context.io = std.testing.io;
    const Scheduler = @import("scheduler.zig");
    var scheduler: Scheduler.Scheduler = .{ .io = std.testing.io, .delivery = undefined };
    defer scheduler.stop();
    const dispatcher = scheduler.dispatcher();
    const ticket: plan.Ticket = .{ .tenant = service.tenant, .query = query, .stage = spec.outputStage(), .task = 0 };
    try dispatcher.dispatch(dispatcher.ptr, &.{ticket}, .{});
    var turns: Scheduler.Turns = .{ .scheduler = &scheduler };
    const coordination = turns.port();
    service.coordination = coordination;
    coordination.retry(coordination.ptr, query);
    var reached = false;
    // A quantum bounds work, not a fixed number of completed waves: Debug
    // allocation accounting and a loaded host can defer additional reads.
    // Require the durable cursor to finish within one bounded campaign.
    const campaign_deadline = @import("antfly_platform").time.monotonicNs() + 30 * std.time.ns_per_s;
    for (0..64) |turn| {
        const now = @import("antfly_platform").time.monotonicNs();
        try std.testing.expect(now < campaign_deadline);
        service.context.deadline_ns = @min(campaign_deadline, now + std.time.ns_per_s);
        _ = try service.turn(query, dispatcher);
        var current = (try records.load(a, query_key)).?;
        defer current.deinit(a);
        if (turn == 0) {
            try std.testing.expect(current.value.pending.len < ids.len and current.value.pending.len > 0);
            try std.testing.expect(current.value.pending_after != null);
            if (early_running) try std.testing.expectEqual(@as(u32, 0), current.value.pending[0]);
            fixture.now += 92001;
            service = fixture.service(a, service.state_store.client, "replacement");
            service.context.io = std.testing.io;
            service.coordination = coordination;
            _ = (try service.acquire(query)).?;
            records.now_ms = fixture.now;
            if (early_running) {
                const key = try records.key(a, query, spec.outputStage(), 0);
                defer a.free(key);
                held = (try records.claim(a, key, "replacement-worker", 60000)).?;
            }
            try std.testing.expectError(error.SqlExecutionFenced, records.checkpointTasksProgress(a, query_key, claim, current.value.stage, current.value.cursor, current.value.pending, current.value.pending_after));
        }
        if (early_running and current.value.pending.len == 1 and current.value.pending_after == null) {
            try std.testing.expectEqual(@as(u32, 0), current.value.pending[0]);
            const key = try records.key(a, query, spec.outputStage(), 0);
            defer a.free(key);
            try records.complete(a, key, held.?, .{ .key = "accepted", .hash = @splat('c'), .bytes = 0, .rows = 1 });
            coordination.retry(coordination.ptr, query);
            const completion_now = @import("antfly_platform").time.monotonicNs();
            try std.testing.expect(completion_now < campaign_deadline);
            service.context.deadline_ns = @min(campaign_deadline, completion_now + std.time.ns_per_s);
            _ = try service.turn(query, dispatcher);
            reached = true;
            break;
        }
        if (current.value.stage == spec.finalStage()) {
            reached = true;
            break;
        }
        // Simulate a restarted delivery registry with no fresh completion hint.
        // Durable partial reconciliation must bypass this idle fast path.
        _ = coordination.delivery_ready(coordination.ptr, query);
    }
    try std.testing.expect(reached);
    var final = (try records.load(a, query_key)).?;
    defer final.deinit(a);
    try std.testing.expectEqual(spec.finalStage(), final.value.stage);
    try std.testing.expectEqual(@as(usize, 0), final.value.pending.len);
    try std.testing.expect(final.value.pending_after == null);
}

test "distributed SQL control failover distinguishes worker failures from terminal query errors" {
    const transport = @import("transport.zig");
    const Http = local.common_http_http_common;
    const a = std.testing.allocator;
    const Fake = struct {
        calls: usize = 0,
        status: u16 = 504,
        failure: ?anyerror = null,
        always: bool = false,
        fn execute(raw: *anyopaque, allocator: A, _: Http.HttpRequest) !Http.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            const fail = self.always or self.calls == 1;
            if (fail) if (self.failure) |err| return err;
            return .{ .status = if (fail) self.status else 200, .owner_allocator = allocator, .body = try allocator.dupe(u8, "{\"complete\":false}") };
        }
    };
    var fake: Fake = .{};
    var routing: transport.Routing = .{};
    var client: transport.Client = .{ .routing = &routing, .a = a, .executor = .{ .ptr = &fake, .vtable = &.{ .execute = Fake.execute } }, .authentication = .{ .secret = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", .issuer = "test" }, .workers = &.{ "http://first", "http://healthy" } };
    const query: transport.QueryRequest = .{ .tenant = @splat('a'), .query = .{ .id = @splat('b'), .expires_ms = 1000000 } };
    const context: Context = .{ .deadline_ns = @import("antfly_platform").time.monotonicNs() + 60 * std.time.ns_per_s };
    for ([_]u16{ 504, 503 }) |status| {
        routing = .{};
        fake = .{ .status = status };
        try std.testing.expect(!try client.turn(query, context));
        try std.testing.expectEqual(@as(usize, 2), fake.calls);
    }
    for ([_]anyerror{ error.Canceled, error.Cancelled, error.DeadlineExceeded, error.Timeout }) |err| {
        routing = .{};
        fake = .{ .failure = err };
        try std.testing.expect(!try client.turn(query, context));
        try std.testing.expectEqual(@as(usize, 2), fake.calls);
    }
    const Case = struct { status: u16, failure: anyerror };
    for ([_]Case{ .{ .status = 408, .failure = error.Canceled }, .{ .status = 409, .failure = error.SqlExecutionFailed }, .{ .status = 410, .failure = error.SqlQueryExpired }, .{ .status = 403, .failure = error.SqlExecutionUnauthorized }, .{ .status = 422, .failure = error.SqlJoinBudgetExceeded } }) |case| {
        routing = .{};
        fake = .{ .status = case.status };
        try std.testing.expectError(case.failure, client.turn(query, context));
        try std.testing.expectEqual(@as(usize, 1), fake.calls);
    }
    for ([_]anyerror{ error.DeadlineExceeded, error.Timeout, error.Canceled }) |err| {
        routing = .{};
        fake = .{ .failure = err, .always = true };
        try std.testing.expectError(error.SqlWorkerUnavailable, client.turn(query, context));
    }
    try context.ensureActive();
    fake = .{};
    try std.testing.expectError(error.DeadlineExceeded, client.turn(query, .{ .deadline_ns = @import("antfly_platform").time.monotonicNs() - 1 }));
    try std.testing.expectEqual(@as(usize, 0), fake.calls);
}

test "distributed SQL control attempt deadlines leave time for healthy peers and respect parent cancellation" {
    const transport = @import("transport.zig");
    const Http = local.common_http_http_common;
    const a = std.testing.allocator;
    const Fake = struct {
        calls: usize = 0,
        first_uri: ?[]u8 = null,
        cancel_parent: bool = false,
        canceled: bool = false,
        fn check(raw: *const anyopaque) bool {
            const self: *const @This() = @ptrCast(@alignCast(raw));
            return self.canceled;
        }
        fn execute(raw: *anyopaque, allocator: A, request_: Http.HttpRequest) !Http.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            try std.testing.expect(request_.timeout_ms.? <= 20);
            if (self.calls == 1) {
                self.first_uri = try allocator.dupe(u8, request_.uri);
                if (self.cancel_parent) {
                    self.canceled = true;
                    return error.Canceled;
                }
                while (!request_.cancellation.?.isCancelled()) try std.Io.sleep(std.testing.io, .fromMilliseconds(1), .awake);
                return error.Canceled;
            }
            try std.testing.expect(!std.mem.eql(u8, self.first_uri.?, request_.uri));
            return .{ .status = 200, .owner_allocator = allocator, .body = try allocator.dupe(u8, "{\"complete\":false}") };
        }
    };
    var fake: Fake = .{};
    defer if (fake.first_uri) |uri| a.free(uri);
    var routing: transport.Routing = .{};
    var client: transport.Client = .{ .routing = &routing, .a = a, .executor = .{ .ptr = &fake, .vtable = &.{ .execute = Fake.execute } }, .authentication = .{ .secret = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", .issuer = "test" }, .workers = &.{ "http://first", "http://healthy" }, .control_attempt_ms = 20 };
    const query: transport.QueryRequest = .{ .tenant = @splat('a'), .query = .{ .id = @splat('b'), .expires_ms = 1000000 } };
    const context: Context = .{ .deadline_ns = @import("antfly_platform").time.monotonicNs() + 2 * std.time.ns_per_s, .cancellation = .{ .ptr = &fake, .is_cancelled_fn = Fake.check } };
    try std.testing.expect(!try client.turn(query, context));
    try context.ensureActive();
    try std.testing.expectEqual(@as(usize, 2), fake.calls);
    a.free(fake.first_uri.?);
    fake = .{ .cancel_parent = true };
    try std.testing.expectError(error.Canceled, client.turn(query, context));
    try std.testing.expectEqual(@as(usize, 1), fake.calls);
}

test "distributed SQL admission replay separates creation from status inspection within a short quantum" {
    const a = std.testing.allocator;
    var memory = storage.MemoryClient.init(a);
    defer memory.deinit();
    var fixture: Fixture = .{};
    var service = fixture.service(a, memory.client(), "owner");
    const input: plan.Input = .{ .table_id = 1, .schema = @splat('b'), .snapshot = "old-left", .wal_cut = 7, .cut = "retained-accepted-cut", .columns = &.{.{ .name = "id", .type = .integer }}, .estimated_bytes = 100, .estimated_rows = 3, .splits = 1 };
    var right = input;
    right.table_id = 2;
    right.snapshot = "old-right";
    right.wal_cut = 8;
    const spec = try plan.choose(input, right, service.tenant, 1000000, .{ .left_keys = &.{0}, .right_keys = &.{0} }, .{ .concurrency = 1 }, fixture.now);
    const query = try service.submit(spec);
    var records = service.state_store;
    records.tenant = service.tenant;
    const task_key = try records.key(a, query, 0, 0);
    defer a.free(task_key);
    try records.create(a, task_key, query.id, query.expires_ms);
    const worker = (try records.claim(a, task_key, "worker", 60000)).?;
    try records.complete(a, task_key, worker, .{ .key = "accepted", .hash = @splat('c'), .bytes = 0 });
    const Slow = struct {
        var base_get: @FieldType(storage.Client.VTable, "get_object") = undefined;
        var base_put: @FieldType(storage.Client.VTable, "put_object") = undefined;
        fn delay(key: []const u8) !void {
            if (std.mem.indexOf(u8, key, "/stage/") != null) try std.Io.sleep(std.testing.io, .fromMilliseconds(200), .awake);
        }
        fn get(raw: *anyopaque, allocator: A, bucket: []const u8, key: []const u8, options: storage.GetOptions) !storage.GetResult {
            try delay(key);
            if (options.cancellation) |token| try token.check();
            return base_get(raw, allocator, bucket, key, options);
        }
        fn put(raw: *anyopaque, allocator: A, bucket: []const u8, key: []const u8, body: []const u8, options: storage.PutOptions) !storage.PutResult {
            try delay(key);
            if (options.cancellation) |token| try token.check();
            return base_put(raw, allocator, bucket, key, body, options);
        }
    };
    Slow.base_get = service.state_store.client.vtable.get_object;
    Slow.base_put = service.state_store.client.vtable.put_object;
    var table = service.state_store.client.vtable.*;
    table.get_object = Slow.get;
    table.put_object = Slow.put;
    service.state_store.client.vtable = &table;
    service.context.io = std.testing.io;
    const Scheduler = @import("scheduler.zig");
    var scheduler: Scheduler.Scheduler = .{ .io = std.testing.io, .delivery = undefined };
    defer scheduler.stop();
    const queue = scheduler.dispatcher();
    try queue.dispatch(queue.ptr, &.{.{ .tenant = service.tenant, .query = query, .stage = 0, .task = 0 }}, .{});
    var turns: Scheduler.Turns = .{ .scheduler = &scheduler };
    service.coordination = turns.port();
    const Reject = struct {
        fn dispatch(_: *anyopaque, _: []const plan.Ticket, _: Context) !void {
            return error.CompletedTaskWasRedispatched;
        }
    };
    const dispatcher: runtime.Dispatcher = .{ .ptr = &fixture, .dispatch = Reject.dispatch };
    const key = try records.key(a, query, null, 0);
    defer a.free(key);
    for (0..2) |turn| {
        service.context.deadline_ns = @import("antfly_platform").time.monotonicNs() + 700 * std.time.ns_per_ms;
        _ = try service.turn(query, dispatcher);
        var value = (try records.load(a, key)).?;
        defer value.deinit(a);
        if (turn == 0) {
            try std.testing.expectEqual(@as(u32, 1), value.value.cursor);
            try std.testing.expectEqualSlices(u32, &.{0}, value.value.pending);
        } else {
            try std.testing.expectEqual(@as(u32, 1), value.value.stage);
            try std.testing.expectEqual(@as(usize, 0), value.value.pending.len);
        }
    }
}

test "distributed SQL admission priority advances a slow partially filled window" {
    const a = std.testing.allocator;
    var memory = storage.MemoryClient.init(a);
    defer memory.deinit();
    var fixture: Fixture = .{};
    var service = fixture.service(a, memory.client(), "owner");
    const input: plan.Input = .{ .table_id = 1, .schema = @splat('b'), .snapshot = "old-left", .wal_cut = 7, .cut = "retained-accepted-cut", .columns = &.{.{ .name = "id", .type = .integer }}, .estimated_bytes = 100, .estimated_rows = 3, .splits = 1 };
    var right = input;
    right.table_id = 2;
    right.snapshot = "old-right";
    right.wal_cut = 8;
    right.splits = 2;
    const spec = try plan.choose(input, right, service.tenant, 1000000, .{ .left_keys = &.{0}, .right_keys = &.{0} }, .{ .concurrency = 2 }, fixture.now);
    const query = try service.submit(spec);
    const claim = (try service.acquire(query)).?;
    var records = service.state_store;
    records.tenant = service.tenant;
    const key = try records.key(a, query, null, 0);
    defer a.free(key);
    const task_key = try records.key(a, query, 0, 0);
    defer a.free(task_key);
    try records.create(a, task_key, query.id, query.expires_ms);
    _ = (try records.claim(a, task_key, "worker", 60000)).?;
    try records.checkpointTasks(a, key, claim, 0, 1, &.{0});
    const Slow = struct {
        var get_base: @FieldType(storage.Client.VTable, "get_object") = undefined;
        var put_base: @FieldType(storage.Client.VTable, "put_object") = undefined;
        fn wait(key_: []const u8) !void {
            if (std.mem.indexOf(u8, key_, "/stage/") != null) try std.Io.sleep(std.testing.io, .fromMilliseconds(200), .awake);
        }
        fn get(raw: *anyopaque, allocator: A, bucket: []const u8, key_: []const u8, options: storage.GetOptions) !storage.GetResult {
            try wait(key_);
            if (options.cancellation) |token| try token.check();
            return get_base(raw, allocator, bucket, key_, options);
        }
        fn put(raw: *anyopaque, allocator: A, bucket: []const u8, key_: []const u8, body: []const u8, options: storage.PutOptions) !storage.PutResult {
            try wait(key_);
            if (options.cancellation) |token| try token.check();
            return put_base(raw, allocator, bucket, key_, body, options);
        }
    };
    Slow.get_base = service.state_store.client.vtable.get_object;
    Slow.put_base = service.state_store.client.vtable.put_object;
    var table = service.state_store.client.vtable.*;
    table.get_object = Slow.get;
    table.put_object = Slow.put;
    service.state_store.client.vtable = &table;
    service.context.io = std.testing.io;
    var turns: @import("scheduler.zig").Turns = .{};
    service.coordination = turns.port();
    const Delivery = struct {
        tasks: usize = 0,
        fn dispatch(raw: *anyopaque, tickets: []const plan.Ticket, _: Context) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            for (tickets) |ticket| {
                try std.testing.expectEqual(@as(u32, 1), ticket.task);
                self.tasks += 1;
            }
        }
    };
    var delivery: Delivery = .{};
    const dispatcher: runtime.Dispatcher = .{ .ptr = &delivery, .dispatch = Delivery.dispatch };
    var admitted = false;
    const campaign_deadline = @import("antfly_platform").time.monotonicNs() + 15 * std.time.ns_per_s;
    for (0..16) |turn| {
        const now = @import("antfly_platform").time.monotonicNs();
        try std.testing.expect(now < campaign_deadline);
        service.context.deadline_ns = @min(campaign_deadline, now + 700 * std.time.ns_per_ms);
        _ = try service.turn(query, dispatcher);
        var value = (try records.load(a, key)).?;
        defer value.deinit(a);
        if (turn == 0) {
            try std.testing.expect(value.value.pending_admission);
            try std.testing.expectEqual(@as(u32, 1), value.value.cursor);
        } else if (value.value.pending_admission) {
            // A suspended admission retains its exact durable cursor. The next
            // turn must keep prioritizing it rather than rescan the slow task.
            try std.testing.expectEqual(@as(u32, 1), value.value.cursor);
            try std.testing.expectEqualSlices(u32, &.{0}, value.value.pending);
        } else {
            try std.testing.expectEqual(@as(u32, 2), value.value.cursor);
            try std.testing.expectEqualSlices(u32, &.{ 0, 1 }, value.value.pending);
            admitted = true;
            break;
        }
    }
    try std.testing.expect(admitted);
    try std.testing.expectEqual(@as(usize, 1), delivery.tasks);
}

test "distributed SQL single worker control does not reserve a nonexistent peer budget" {
    const a = std.testing.allocator;
    const transport = @import("transport.zig");
    const Http = local.common_http_http_common;
    const Fake = struct {
        fn execute(_: *anyopaque, allocator: A, request_: Http.HttpRequest) !Http.HttpResponse {
            try std.testing.expect(request_.timeout_ms.? > 1000);
            return .{ .status = 200, .owner_allocator = allocator, .body = try allocator.dupe(u8, "{\"complete\":false}") };
        }
    };
    var marker: u8 = 0;
    var routing: transport.Routing = .{};
    var client: transport.Client = .{ .routing = &routing, .a = a, .executor = .{ .ptr = &marker, .vtable = &.{ .execute = Fake.execute } }, .authentication = .{ .secret = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", .issuer = "test" }, .workers = &.{"http://only"} };
    const request: transport.QueryRequest = .{ .tenant = @splat('a'), .query = .{ .id = @splat('b'), .expires_ms = 1000000 } };
    const context: Context = .{ .deadline_ns = @import("antfly_platform").time.monotonicNs() + 2 * std.time.ns_per_s };
    try std.testing.expect(!try client.turn(request, context));
}

test "distributed SQL cancellation stops active delivery and immediately serves another query" {
    const a = std.testing.allocator;
    var memory = storage.MemoryClient.init(a);
    defer memory.deinit();
    var fixture: Fixture = .{};
    var service = fixture.service(a, memory.client(), "owner");
    const input: plan.Input = .{ .table_id = 1, .schema = @splat('b'), .snapshot = "old-left", .wal_cut = 7, .cut = "retained-accepted-cut", .columns = &.{.{ .name = "id", .type = .integer }}, .estimated_bytes = 100, .estimated_rows = 3, .splits = 1 };
    var right = input;
    right.table_id = 2;
    right.snapshot = "old-right";
    right.wal_cut = 8;
    const spec = try plan.choose(input, right, service.tenant, 1000000, .{ .left_keys = &.{0}, .right_keys = &.{0} }, .{}, fixture.now);
    const query = try service.submit(spec);
    const Gate = struct {
        entered: std.Io.Event = .unset,
        release: std.Io.Event = .unset,
        finished: std.Io.Event = .unset,
        cancelled: std.atomic.Value(bool) = .init(false),
        fn dispatch(raw: *anyopaque, _: []const plan.Ticket, context: Context) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.entered.set(std.testing.io);
            defer self.finished.set(std.testing.io);
            while (!self.release.isSet()) {
                context.ensureActive() catch |err| {
                    self.cancelled.store(true, .release);
                    return err;
                };
                self.release.waitTimeout(std.testing.io, .{ .duration = .{ .raw = .fromMilliseconds(10), .clock = .awake } }) catch |err| switch (err) {
                    error.Timeout => continue,
                    error.Canceled => return err,
                };
            }
        }
    };
    var gate: Gate = .{};
    const Scheduler = @import("scheduler.zig");
    var scheduler: Scheduler.Scheduler = .{ .io = std.testing.io, .clock = service.clock, .delivery = .{ .ptr = &gate, .dispatch = Gate.dispatch } };
    try scheduler.start(1);
    defer {
        gate.release.set(std.testing.io);
        scheduler.stop();
    }
    var turns: Scheduler.Turns = .{ .scheduler = &scheduler };
    service.coordination = turns.port();
    _ = try service.turn(query, scheduler.dispatcher());
    try gate.entered.waitTimeout(std.testing.io, .{ .duration = .{ .raw = .fromMilliseconds(1000), .clock = .awake } });
    try service.requestCancel(query);
    try std.testing.expect(try service.turn(query, scheduler.dispatcher()));
    try std.testing.expectError(error.Canceled, service.pollResult(a, query));
    try gate.finished.waitTimeout(std.testing.io, .{ .duration = .{ .raw = .fromMilliseconds(1000), .clock = .awake } });
    try std.testing.expect(gate.cancelled.load(.acquire));
    // The one delivery lane is available without releasing the blocked gate.
    var second = spec;
    second.expires_ms -= 1;
    const next = try service.submit(second);
    gate.entered.reset();
    gate.release.set(std.testing.io);
    _ = try service.turn(next, scheduler.dispatcher());
    try gate.entered.waitTimeout(std.testing.io, .{ .duration = .{ .raw = .fromMilliseconds(1000), .clock = .awake } });
}

test "distributed SQL expired task claims preserve durable routing epochs across coordinator takeover" {
    const a = std.testing.allocator;
    var memory = storage.MemoryClient.init(a);
    defer memory.deinit();
    var fixture: Fixture = .{};
    var service = fixture.service(a, memory.client(), "owner");
    const input: plan.Input = .{ .table_id = 1, .schema = @splat('b'), .snapshot = "old-left", .wal_cut = 7, .cut = "retained-accepted-cut", .columns = &.{.{ .name = "id", .type = .integer }}, .estimated_bytes = 100, .estimated_rows = 3, .splits = 1 };
    var right = input;
    right.table_id = 2;
    right.snapshot = "old-right";
    right.wal_cut = 8;
    const spec = try plan.choose(input, right, service.tenant, 1000000, .{ .left_keys = &.{0}, .right_keys = &.{0} }, .{ .concurrency = 1 }, fixture.now);
    const query = try service.submit(spec);
    const Capture = struct {
        ticket: ?plan.Ticket = null,
        fn dispatch(raw: *anyopaque, tickets: []const plan.Ticket, _: Context) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqual(@as(usize, 1), tickets.len);
            self.ticket = tickets[0];
        }
    };
    var capture: Capture = .{};
    const delivery: runtime.Dispatcher = .{ .ptr = &capture, .dispatch = Capture.dispatch };
    _ = try service.turn(query, delivery);
    const original = capture.ticket.?;
    try std.testing.expectEqual(@as(u64, 0), original.attempt);
    var records = service.state_store;
    records.now_ms = fixture.now;
    records.tenant = service.tenant;
    const key = try records.key(a, query, original.stage, original.task);
    defer a.free(key);
    const claim = (try records.claim(a, key, "stalled-worker", 60000)).?;
    fixture.now = 92001;
    capture.ticket = null;
    service.owner = "replacement-coordinator";
    _ = try service.turn(query, delivery);
    try std.testing.expect(original.sameTask(capture.ticket.?));
    try std.testing.expectEqual(claim.epoch, capture.ticket.?.attempt);
}

test "distributed SQL layout failures obey the durable task attempt limit" {
    const a = std.testing.allocator;
    var memory = storage.MemoryClient.init(a);
    defer memory.deinit();
    var fixture: Fixture = .{};
    var service = fixture.service(a, memory.client(), "owner");
    const input: plan.Input = .{ .table_id = 1, .schema = @splat('b'), .snapshot = "old-left", .wal_cut = 7, .cut = "retained-accepted-cut", .columns = &.{.{ .name = "id", .type = .integer }}, .estimated_bytes = 100, .estimated_rows = 3, .splits = 1 };
    var right = input;
    right.table_id = 2;
    right.snapshot = "old-right";
    right.wal_cut = 8;
    var spec = try plan.choose(input, right, service.tenant, 1000000, .{ .left_keys = &.{0}, .right_keys = &.{0} }, .{ .max_attempts = 1 }, fixture.now);
    spec.strategy = .shuffle;
    const query = try service.submit(spec);
    const coordinator = (try service.acquire(query)).?;
    var records = service.state_store;
    records.now_ms = fixture.now;
    records.tenant = service.tenant;
    const query_key = try records.key(a, query, null, 0);
    defer a.free(query_key);
    const layout = "{\"boundaries\":[0,1],\"skew\":[]}";
    const hash = local.serverless_external_source_mod.lake_catalog.types.digestHex(layout);
    const physical_key = try std.fmt.allocPrint(a, "{s}/sql-exchange/{d:0>20}/{s}/{s}.physical.json", .{ service.exchange_store.prefix, query.expires_ms, query.id, hash });
    defer a.free(physical_key);
    var client = memory.client();
    try client.makeBucket(service.exchange_store.bucket);
    var uploaded = try client.putObject(service.exchange_store.bucket, physical_key, layout, .{});
    uploaded.deinit(a);
    try records.checkpointResolved(a, query_key, coordinator, 1, 0, &.{}, null, null, .{ .key = physical_key, .hash = hash, .bytes = layout.len });
    const Fault = struct {
        var key: []const u8 = undefined;
        var original: @FieldType(storage.Client.VTable, "get_object") = undefined;
        fn get(raw: *anyopaque, allocator: A, bucket: []const u8, path: []const u8, options: storage.GetOptions) !storage.GetResult {
            if (std.mem.eql(u8, path, key)) return error.NetworkUnreachable;
            return original(raw, allocator, bucket, path, options);
        }
    };
    Fault.key = physical_key;
    Fault.original = service.exchange_store.client.vtable.get_object;
    var table = service.exchange_store.client.vtable.*;
    table.get_object = Fault.get;
    service.exchange_store.client.vtable = &table;
    const task_key = try records.key(a, query, 1, 0);
    defer a.free(task_key);
    try records.create(a, task_key, query.id, query.expires_ms);
    const ticket: plan.Ticket = .{ .tenant = service.tenant, .query = query, .stage = 1, .task = 0 };
    try std.testing.expectError(error.NetworkUnreachable, service.execute(ticket));
    fixture.now += 91001;
    try service.execute(ticket);
    try service.execute(ticket);
    var current = (try records.load(a, task_key)).?;
    defer current.deinit(a);
    try std.testing.expectEqual(state.Status.failed, current.value.status);
    try std.testing.expectEqual(@as(u64, 2), current.value.epoch);
    try std.testing.expectEqualStrings("TaskRetryBudgetExceeded", current.value.failure.?);
}

test "distributed SQL plan read failures obey the durable task attempt limit" {
    for ([_]enum { admitted, legacy, unavailable }{ .admitted, .legacy, .unavailable }) |mode| {
        const a = std.testing.allocator;
        var memory = storage.MemoryClient.init(a);
        defer memory.deinit();
        var fixture: Fixture = .{};
        var service = fixture.service(a, memory.client(), "owner");
        const input: plan.Input = .{ .table_id = 1, .schema = @splat('b'), .snapshot = "old-left", .wal_cut = 7, .cut = "retained-accepted-cut", .columns = &.{.{ .name = "id", .type = .integer }}, .estimated_bytes = 100, .estimated_rows = 3, .splits = 1 };
        var right = input;
        right.table_id = 2;
        right.snapshot = "old-right";
        right.wal_cut = 8;
        var spec = try plan.choose(input, right, service.tenant, 10000000, .{ .left_keys = &.{0}, .right_keys = &.{0} }, .{ .max_attempts = 1 }, fixture.now);
        spec.strategy = .shuffle;
        const query = try service.submit(spec);
        const coordinator = (try service.acquire(query)).?;
        var records = service.state_store;
        records.now_ms = fixture.now;
        records.tenant = service.tenant;
        const query_key = try records.key(a, query, null, 0);
        defer a.free(query_key);
        const layout = "{\"boundaries\":[0,1],\"skew\":[]}";
        const hash = local.serverless_external_source_mod.lake_catalog.types.digestHex(layout);
        const physical_key = try std.fmt.allocPrint(a, "{s}/sql-exchange/{d:0>20}/{s}/{s}.physical.json", .{ service.exchange_store.prefix, query.expires_ms, query.id, hash });
        defer a.free(physical_key);
        var client = memory.client();
        try client.makeBucket(service.exchange_store.bucket);
        var uploaded = try client.putObject(service.exchange_store.bucket, physical_key, layout, .{});
        uploaded.deinit(a);
        try records.checkpointResolved(a, query_key, coordinator, 1, 0, &.{}, null, null, .{ .key = physical_key, .hash = hash, .bytes = layout.len });
        const Fault = struct {
            var sidecar_unavailable: bool = false;
            var key: []const u8 = undefined;
            var original: @FieldType(storage.Client.VTable, "get_object") = undefined;
            fn get(raw: *anyopaque, allocator: A, bucket: []const u8, path: []const u8, options: storage.GetOptions) !storage.GetResult {
                if (std.mem.eql(u8, path, key) or (sidecar_unavailable and std.mem.endsWith(u8, path, "/admission-v1.json"))) return error.NetworkUnreachable;
                return original(raw, allocator, bucket, path, options);
            }
        };
        const plan_key = try std.fmt.allocPrint(a, "{s}/sql-execution-v2/{d:0>20}/{s}/plan.json", .{ records.prefix, query.expires_ms, query.id });
        defer a.free(plan_key);
        Fault.sidecar_unavailable = mode == .unavailable;
        if (mode == .legacy) {
            const admission_key = try std.fmt.allocPrint(a, "{s}/sql-execution-v2/{d:0>20}/{s}/admission-v1.json", .{ records.prefix, query.expires_ms, query.id });
            defer a.free(admission_key);
            try client.deleteObject(service.state_store.bucket, admission_key, .{});
        }
        Fault.key = plan_key;
        Fault.original = service.state_store.client.vtable.get_object;
        var table = service.state_store.client.vtable.*;
        table.get_object = Fault.get;
        service.state_store.client.vtable = &table;
        const task_key = try records.key(a, query, 1, 0);
        defer a.free(task_key);
        try records.create(a, task_key, query.id, query.expires_ms);
        const ticket: plan.Ticket = .{ .tenant = service.tenant, .query = query, .stage = 1, .task = 0 };
        const attempts: u64 = if (mode == .legacy) state.max_task_attempts else 1;
        for (0..attempts) |_| {
            try std.testing.expectError(error.NetworkUnreachable, service.execute(ticket));
            fixture.now += 91001;
        }
        if (mode == .unavailable) {
            for (0..3) |_| try std.testing.expectError(error.NetworkUnreachable, service.execute(ticket));
            var unchanged = (try records.load(a, task_key)).?;
            defer unchanged.deinit(a);
            try std.testing.expectEqual(@as(u64, 1), unchanged.value.epoch);
            Fault.sidecar_unavailable = false;
        }
        try service.execute(ticket);
        try service.execute(ticket);
        var current = (try records.load(a, task_key)).?;
        defer current.deinit(a);
        try std.testing.expectEqual(state.Status.failed, current.value.status);
        try std.testing.expectEqual(attempts + 1, current.value.epoch);
        try std.testing.expectEqualStrings("TaskRetryBudgetExceeded", current.value.failure.?);
    }
}

test "distributed SQL layout preparation allocation failures terminate within the admitted memory budget" {
    const a = std.testing.allocator;
    var memory = storage.MemoryClient.init(a);
    defer memory.deinit();
    var fixture: Fixture = .{};
    var service = fixture.service(a, memory.client(), "owner");
    const input: plan.Input = .{ .table_id = 1, .schema = @splat('b'), .snapshot = "old-left", .wal_cut = 7, .cut = "retained-accepted-cut", .columns = &.{.{ .name = "id", .type = .integer }}, .estimated_bytes = 100, .estimated_rows = 3, .splits = 1 };
    var right = input;
    right.table_id = 2;
    right.snapshot = "old-right";
    right.wal_cut = 8;
    var spec = try plan.choose(input, right, service.tenant, 1000000, .{ .left_keys = &.{0}, .right_keys = &.{0} }, .{ .max_attempts = 1, .task_memory = 1024 * 1024 }, fixture.now);
    spec.strategy = .shuffle;
    const query = try service.submit(spec);
    const coordinator = (try service.acquire(query)).?;
    var records = service.state_store;
    records.now_ms = fixture.now;
    records.tenant = service.tenant;
    const query_key = try records.key(a, query, null, 0);
    defer a.free(query_key);
    const layout = "{\"boundaries\":[0,1],\"skew\":[]}";
    const hash = local.serverless_external_source_mod.lake_catalog.types.digestHex(layout);
    const physical_key = try std.fmt.allocPrint(a, "{s}/sql-exchange/{d:0>20}/{s}/{s}.physical.json", .{ service.exchange_store.prefix, query.expires_ms, query.id, hash });
    defer a.free(physical_key);
    var client = memory.client();
    try client.makeBucket(service.exchange_store.bucket);
    var uploaded = try client.putObject(service.exchange_store.bucket, physical_key, layout, .{});
    uploaded.deinit(a);
    try records.checkpointResolved(a, query_key, coordinator, 1, 0, &.{}, null, null, .{ .key = physical_key, .hash = hash, .bytes = layout.len });
    const Fault = struct {
        var key: []const u8 = undefined;
        var original: @FieldType(storage.Client.VTable, "get_object") = undefined;
        fn get(raw: *anyopaque, allocator: A, bucket: []const u8, path: []const u8, options: storage.GetOptions) !storage.GetResult {
            if (std.mem.eql(u8, path, key)) {
                const bytes = try allocator.alloc(u8, 2 * 1024 * 1024);
                allocator.free(bytes);
                return error.UnexpectedLayoutAllocation;
            }
            return original(raw, allocator, bucket, path, options);
        }
    };
    Fault.key = physical_key;
    Fault.original = service.exchange_store.client.vtable.get_object;
    var table = service.exchange_store.client.vtable.*;
    table.get_object = Fault.get;
    service.exchange_store.client.vtable = &table;
    const task_key = try records.key(a, query, 1, 0);
    defer a.free(task_key);
    try records.create(a, task_key, query.id, query.expires_ms);
    const ticket: plan.Ticket = .{ .tenant = service.tenant, .query = query, .stage = 1, .task = 0 };
    try service.execute(ticket);
    try service.execute(ticket);
    try service.execute(ticket);
    var current = (try records.load(a, task_key)).?;
    defer current.deinit(a);
    try std.testing.expectEqual(state.Status.failed, current.value.status);
    try std.testing.expectEqual(@as(u64, 1), current.value.epoch);
    try std.testing.expectEqualStrings("SqlJoinBudgetExceeded", current.value.failure.?);
}

// Frozen wire schema from before inline retry admission. Keep this reader
// strict: adding a Record field must not silently break rolling deployments.
const OriginalV2Record = struct {
    tenant: ?state.Digest = null,
    version: u8 = 2,
    plan: state.Digest,
    expires_ms: u64,
    status: state.Status = .pending,
    owner: []const u8 = "",
    epoch: u64 = 0,
    until_ms: u64 = 0,
    stage: u32 = 0,
    cursor: u32 = 0,
    pending: []const u32 = &.{},
    pending_after: ?u32 = null,
    pending_admission: bool = false,
    reduce_boundaries: []const u16 = &.{},
    skew: []const @import("physical.zig").Part = &.{},
    physical: ?state.Output = null,
    adaptive: ?state.Output = null,
    result_progress: ?state.Output = null,
    cancel_requested: bool = false,
    output: ?state.Output = null,
    failure: ?[]const u8 = null,
};

test "distributed SQL preserves strict v2 readers and avoids admission reads on successful first attempts" {
    const a = std.testing.allocator;
    var memory = storage.MemoryClient.init(a);
    defer memory.deinit();
    const Observe = struct {
        var base_get: @FieldType(storage.Client.VTable, "get_object") = undefined;
        var base_put: @FieldType(storage.Client.VTable, "put_object") = undefined;
        var admission_reads: usize = 0;
        var records: usize = 0;
        var lost_ack: bool = true;
        fn get(raw: *anyopaque, allocator: A, bucket: []const u8, key: []const u8, options: storage.GetOptions) !storage.GetResult {
            if (std.mem.endsWith(u8, key, "/admission-v1.json")) admission_reads += 1;
            return base_get(raw, allocator, bucket, key, options);
        }
        fn put(raw: *anyopaque, allocator: A, bucket: []const u8, key: []const u8, body: []const u8, options: storage.PutOptions) !storage.PutResult {
            if (std.mem.endsWith(u8, key, "/query.json") or std.mem.indexOf(u8, key, "/task/") != null) {
                const parsed = try std.json.parseFromSlice(OriginalV2Record, allocator, body, .{ .allocate = .alloc_always });
                defer parsed.deinit();
                records += 1;
            }
            var accepted = try base_put(raw, allocator, bucket, key, body, options);
            if (lost_ack and std.mem.endsWith(u8, key, "/admission-v1.json")) {
                lost_ack = false;
                accepted.deinit(allocator);
                return error.NetworkUnreachable;
            }
            return accepted;
        }
    };
    Observe.admission_reads = 0;
    Observe.records = 0;
    Observe.lost_ack = true;
    var client = memory.client();
    Observe.base_get = client.vtable.get_object;
    Observe.base_put = client.vtable.put_object;
    var table = client.vtable.*;
    table.get_object = Observe.get;
    table.put_object = Observe.put;
    client.vtable = &table;
    var fixture: Fixture = .{};
    const service = fixture.service(a, client, "owner");
    const input: plan.Input = .{ .table_id = 1, .schema = @splat('b'), .snapshot = "old-left", .wal_cut = 7, .cut = "retained-accepted-cut", .columns = &.{.{ .name = "id", .type = .integer }}, .estimated_bytes = 100, .estimated_rows = 3, .splits = 1 };
    var right = input;
    right.table_id = 2;
    right.snapshot = "old-right";
    right.wal_cut = 8;
    const spec = try plan.choose(input, right, service.tenant, 1000000, .{ .left_keys = &.{0}, .right_keys = &.{0} }, .{}, fixture.now);
    const query = try service.submit(spec);
    try std.testing.expectEqual(@as(usize, 1), Observe.admission_reads);
    Observe.admission_reads = 0;
    var dispatch: Dispatch = .{ .workers = .{ service, service }, .drop_once = false };
    var complete = false;
    for (0..32) |_| {
        if (try service.turn(query, .{ .ptr = &dispatch, .dispatch = Dispatch.run })) {
            complete = true;
            break;
        }
    }
    try std.testing.expect(complete);
    var result_arena = std.heap.ArenaAllocator.init(a);
    defer result_arena.deinit();
    const result = (try service.pollResult(result_arena.allocator(), query)).?;
    try std.testing.expectEqual(@as(u64, 2), result.rows);
    try std.testing.expectEqual(@as(usize, 0), Observe.admission_reads);
    try std.testing.expect(Observe.records >= 6);
}

test "distributed SQL inline admission is read-only and checkpoints retain the original v2 schema" {
    const a = std.testing.allocator;
    const bytes = try std.json.Stringify.valueAlloc(a, .{ .plan = @as(state.Digest, @splat('a')), .expires_ms = @as(u64, 1000), .task_attempt_limit = @as(u16, 1) }, .{});
    defer a.free(bytes);
    const old = try std.json.parseFromSlice(state.Record, a, bytes, .{});
    defer old.deinit();
    try std.testing.expectEqual(@as(?u16, 1), old.value.task_attempt_limit);
    const checkpoint = try std.json.Stringify.valueAlloc(a, old.value, .{});
    defer a.free(checkpoint);
    const compatible = try std.json.parseFromSlice(OriginalV2Record, a, checkpoint, .{});
    defer compatible.deinit();
    try std.testing.expectEqual(old.value.plan, compatible.value.plan);
}

test "distributed SQL migrates inline retry authority before claims renewals and checkpoints" {
    const a = std.testing.allocator;
    for ([_]enum { claim, renew, checkpoint, cancel, terminate }{ .claim, .renew, .checkpoint, .cancel, .terminate }) |operation| {
        var memory = storage.MemoryClient.init(a);
        defer memory.deinit();
        var client = memory.client();
        try client.makeBucket("queries");
        const Fault = struct {
            var base: @FieldType(storage.Client.VTable, "put_object") = undefined;
            var fail: bool = true;
            fn put(raw: *anyopaque, allocator: A, bucket: []const u8, key: []const u8, body: []const u8, options: storage.PutOptions) !storage.PutResult {
                if (fail and std.mem.endsWith(u8, key, "/admission-v1.json")) return error.NetworkUnreachable;
                return base(raw, allocator, bucket, key, body, options);
            }
        };
        Fault.base = client.vtable.put_object;
        Fault.fail = true;
        var table = client.vtable.*;
        table.put_object = Fault.put;
        client.vtable = &table;
        const records: state.Store = .{ .client = client, .bucket = "queries", .prefix = "tenant", .tenant = @splat('a'), .now_ms = 1000 };
        const query: state.Query = .{ .id = @splat('b'), .expires_ms = 1000000 };
        const key = try records.key(a, query, null, 0);
        defer a.free(key);
        // Emit the exact previous inline format, bypassing the new serializer.
        const bytes = try std.json.Stringify.valueAlloc(a, .{ .plan = query.id, .expires_ms = query.expires_ms, .tenant = records.tenant, .task_attempt_limit = @as(u16, 1), .status = if (operation == .claim) state.Status.pending else state.Status.running, .owner = if (operation == .claim) "" else "owner", .epoch = @as(u64, if (operation == .claim) 0 else 1), .until_ms = @as(u64, if (operation == .claim) 0 else 60000) }, .{});
        defer a.free(bytes);
        var original = try client.putObject(records.bucket, key, bytes, .{});
        original.deinit(a);
        const Write = struct {
            fn run(store: state.Store, allocator: A, path: []const u8, kind: @TypeOf(operation)) !void {
                const claim: state.Claim = .{ .owner = "owner", .epoch = 1 };
                switch (kind) {
                    .claim => _ = try store.claim(allocator, path, "owner", 60000),
                    .renew => try store.renew(allocator, path, claim, 60000),
                    .checkpoint => try store.checkpointTasks(allocator, path, claim, 0, 1, &.{0}),
                    .cancel => try store.requestCancel(allocator, path),
                    .terminate => try store.terminate(allocator, path, claim, .failed, "TaskFailed"),
                }
            }
        };
        if (operation == .cancel or operation == .terminate) {
            try Write.run(records, a, key, operation);
            var stopped = try client.getObject(records.bucket, key, .{});
            defer stopped.deinit(a);
            const compatible = try std.json.parseFromSlice(OriginalV2Record, a, stopped.body, .{});
            defer compatible.deinit();
            try std.testing.expect(if (operation == .cancel) compatible.value.cancel_requested else compatible.value.status == .failed);
            const admission_key = try records.admissionKey(a, query);
            defer a.free(admission_key);
            try std.testing.expect((try records.load(a, admission_key)) == null);
            continue;
        }
        try std.testing.expectError(error.NetworkUnreachable, Write.run(records, a, key, operation));
        var unchanged = (try records.load(a, key)).?;
        defer unchanged.deinit(a);
        try std.testing.expectEqual(@as(?u16, 1), unchanged.value.task_attempt_limit);
        Fault.fail = false;
        try Write.run(records, a, key, operation);
        var updated = try client.getObject(records.bucket, key, .{});
        defer updated.deinit(a);
        const compatible = try std.json.parseFromSlice(OriginalV2Record, a, updated.body, .{});
        defer compatible.deinit();
        try std.testing.expectEqual(query.id, compatible.value.plan);
        const admission_key = try records.admissionKey(a, query);
        defer a.free(admission_key);
        var admission = try client.getObject(records.bucket, admission_key, .{});
        defer admission.deinit(a);
        const authority = try std.json.parseFromSlice(state.Admission, a, admission.body, .{});
        defer authority.deinit();
        try std.testing.expectEqual(query, authority.value.query);
        try std.testing.expectEqual(records.tenant.?, authority.value.tenant);
        try std.testing.expectEqual(@as(u16, 1), authority.value.task_attempt_limit);
    }
}

test "distributed SQL spill admission bounds concurrent envelopes and releases reservations" {
    const a = std.testing.allocator;
    var dummy: u8 = 0;
    var parent: local.sql_spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = struct {
        fn check(_: *anyopaque) !void {}
    }.check, .max_bytes = 2048, .max_files = 128 };
    defer parent.deinit();
    var first = parent.admit(a, 1024).?;
    var second = parent.admit(a, 1024).?;
    try std.testing.expect(parent.admit(a, 1) == null);
    try std.testing.expectEqual(@as(usize, 0), parent.fileHeadroom());
    try std.testing.expectEqual(@as(usize, 64), first.fileHeadroom());
    first.deinit();
    try std.testing.expect(parent.admit(a, 1025) == null);
    var third = parent.admit(a, 512).?;
    try std.testing.expect(parent.admit(a, 1) == null);
    third.deinit();
    second.deinit();
    try std.testing.expectEqual(@as(u64, 0), parent.reserved_bytes);
    try std.testing.expectEqual(@as(usize, 128), parent.fileHeadroom());
    var full = parent.admit(a, 4096).?;
    try std.testing.expectEqual(@as(u64, 2048), full.max_bytes);
    full.deinit();
}

test "distributed SQL spill contention preserves attempts and resumes after release" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const sa = arena.allocator();
    var memory = storage.MemoryClient.init(a);
    defer memory.deinit();
    var fixture: Fixture = .{};
    const coordinator = fixture.service(a, memory.client(), "coordinator");
    var temporary = try local.common_test_directory.TestDirectory.init("distributed-sql-admission");
    defer temporary.cleanup();
    try std.Io.Dir.cwd().createDirPath(std.testing.io, temporary.path());
    var dummy: u8 = 0;
    var parent: local.sql_spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = struct {
        fn check(_: *anyopaque) !void {}
    }.check, .root = temporary.path() };
    defer parent.deinit();
    var held: ?local.sql_spill.Manager = parent.admit(a, parent.max_bytes).?;
    defer if (held) |*manager| manager.deinit();
    const BusyDispatch = struct {
        worker: runtime.Service,
        busy: usize = 0,
        maps: usize = 0,
        fn run(raw: *anyopaque, tickets: []const plan.Ticket, _: Context) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            for (tickets) |ticket| {
                self.worker.execute(ticket) catch |err| {
                    if (err != error.SqlTaskAdmissionUnavailable) return err;
                    const key = try self.worker.state_store.key(self.worker.a, ticket.query, ticket.stage, ticket.task);
                    defer self.worker.a.free(key);
                    var record = (try self.worker.state_store.load(self.worker.a, key)).?;
                    defer record.deinit(self.worker.a);
                    try std.testing.expectEqual(@as(u64, 0), record.value.epoch);
                    try std.testing.expectEqual(state.Status.pending, record.value.status);
                    self.busy += 1;
                    continue;
                };
                if (ticket.stage == 0) self.maps += 1;
            }
        }
    };
    var dispatch: BusyDispatch = .{ .worker = fixture.service(a, memory.client(), "worker") };
    dispatch.worker.spill = &parent;
    const input: plan.Input = .{ .table_id = 1, .schema = @splat('b'), .snapshot = "old-left", .wal_cut = 7, .cut = "retained-accepted-cut", .columns = &.{.{ .name = "id", .type = .integer }}, .estimated_bytes = 100, .estimated_rows = 3, .splits = 1 };
    var right = input;
    right.table_id = 2;
    right.snapshot = "old-right";
    right.wal_cut = 8;
    var spec = try plan.choose(input, right, @splat('a'), 1000000, .{ .left_keys = &.{0}, .right_keys = &.{0} }, .{ .max_attempts = 1, .query_exchange_bytes = 16 * 1024 * 1024 * 1024 }, fixture.now);
    spec.strategy = .shuffle;
    const query = try coordinator.submit(spec);
    const claim = (try coordinator.acquire(query)).?;
    for (0..64) |_| {
        try std.testing.expect(!try coordinator.tick(query, claim, .{ .ptr = &dispatch, .dispatch = BusyDispatch.run }));
        if (dispatch.busy > 16) break;
    }
    try std.testing.expect(dispatch.maps > 0);
    try std.testing.expect(dispatch.busy > 16);
    held.?.deinit();
    held = null;
    var done = false;
    for (0..128) |_| {
        done = try coordinator.tick(query, claim, .{ .ptr = &dispatch, .dispatch = BusyDispatch.run });
        if (done) break;
    }
    try std.testing.expect(done);
    const result = (try coordinator.pollResult(sa, query)).?;
    try std.testing.expectEqual(@as(u64, 2), result.rows);
    try std.testing.expectEqual(@as(u64, 0), parent.reserved_bytes);
    try std.testing.expectEqual(@as(usize, 0), parent.reserved_files);
}

test "distributed SQL admitted spill isolates real file and byte quotas between siblings" {
    const a = std.testing.allocator;
    var temporary = try local.common_test_directory.TestDirectory.init("distributed-sql-spill-isolation");
    defer temporary.cleanup();
    try std.Io.Dir.cwd().createDirPath(std.testing.io, temporary.path());
    var dummy: u8 = 0;
    var parent: local.sql_spill.Manager = .{ .alloc = a, .io = std.testing.io, .root = temporary.path(), .context = &dummy, .checkpoint = struct {
        fn check(_: *anyopaque) !void {}
    }.check, .max_bytes = 2048, .max_files = 128 };
    defer parent.deinit();
    var first = parent.admit(a, 1024).?;
    defer first.deinit();
    var second = parent.admit(a, 1024).?;
    defer second.deinit();
    var files: [64]local.sql_spill.File = undefined;
    var count: usize = 0;
    defer for (files[0..count]) |*file| file.close();
    for (&files) |*file| {
        file.* = try first.create();
        count += 1;
    }
    try std.testing.expectError(error.SqlProgramLimitExceeded, first.create());
    // One task reaching its descriptor cap cannot steal its sibling's slots.
    var sibling = try second.create();
    defer sibling.close();
    try files[0].writeRaw(0, &@as([1024]u8, @splat('a')));
    try std.testing.expectError(error.SqlProgramLimitExceeded, files[0].writeRaw(1024, "x"));
    try sibling.writeRaw(0, &@as([1024]u8, @splat('b')));
    try std.testing.expectEqual(@as(u64, 1024), first.live_bytes);
    try std.testing.expectEqual(@as(u64, 1024), second.live_bytes);
}

test "distributed SQL stale expired and collected task deliveries acknowledge without quarantining healthy workers" {
    const a = std.testing.allocator;
    var memory = storage.MemoryClient.init(a);
    defer memory.deinit();
    var fixture: Fixture = .{};
    var service = fixture.service(a, memory.client(), "worker");
    var records = service.state_store;
    records.tenant = service.tenant;
    const query: state.Query = .{ .id = @splat('b'), .expires_ms = 200000 };
    const key = try records.key(a, query, null, 0);
    defer a.free(key);
    try records.create(a, key, query.id, query.expires_ms);
    const claim = (try records.claim(a, key, "coordinator", 60000)).?;
    try records.checkpointTasks(a, key, claim, 1, 0, &.{});
    const expired: state.Query = .{ .id = @splat('d'), .expires_ms = 500 };
    const expired_key = try records.key(a, expired, null, 0);
    defer a.free(expired_key);
    records.now_ms = 0;
    try records.create(a, expired_key, expired.id, expired.expires_ms);
    const cases = [_]struct { query: state.Query, failure: anyerror }{
        .{ .query = query, .failure = error.SqlExecutionFenced },
        .{ .query = expired, .failure = error.SqlQueryExpired },
        .{ .query = .{ .id = @splat('c'), .expires_ms = 200000 }, .failure = error.SqlExecutionNotFound },
    };
    const transport = @import("transport.zig");
    const auth = @import("../../api/internal_service_auth.zig");
    const Scope = struct {
        fn open(raw: *anyopaque, _: A, tenant: state.Digest, context: Context) !runtime.Service {
            var scoped = @as(*runtime.Service, @ptrCast(@alignCast(raw))).*;
            if (!std.meta.eql(tenant, scoped.tenant)) return error.SqlExecutionUnauthorized;
            scoped.context = context;
            return scoped;
        }
    };
    const config: auth.Config = .{ .secret = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", .issuer = "test" };
    var admission = local.common_request_admission.RequestAdmission.init(2);
    const Fake = struct {
        server: transport.Server,
        calls: usize = 0,
        expected_status: u16 = 204,
        fn execute(raw: *anyopaque, allocator: A, request_: local.common_http_http_common.HttpRequest) !local.common_http_http_common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            var credential: ?[]const u8 = null;
            for (request_.headers) |header| if (std.ascii.eqlIgnoreCase(header.name, auth.header_name)) {
                credential = header.value;
            };
            const response = try self.server.handle(allocator, transport.task_path, credential, request_.body, .{});
            try std.testing.expectEqual(self.expected_status, response.status);
            return response;
        }
    };
    var fake: Fake = .{ .server = .{ .factory = .{ .ptr = &service, .open = Scope.open, .dispatcher = undefined }, .authentication = config, .admission = &admission } };
    var routing: transport.Routing = .{ .cooldown_ms = 30000 };
    var client: transport.Client = .{ .a = a, .routing = &routing, .executor = .{ .ptr = &fake, .vtable = &.{ .execute = Fake.execute } }, .authentication = config, .workers = &.{ "http://a", "http://b" } };
    for (cases, 0..) |case, i| {
        const ticket: plan.Ticket = .{ .tenant = service.tenant, .query = case.query, .stage = 0, .task = 0 };
        try std.testing.expectError(case.failure, service.execute(ticket));
        try client.execute(ticket, .{});
        try std.testing.expectEqual(i + 1, fake.calls);
        const now = @import("antfly_platform").time.monotonicNs();
        try std.testing.expectEqual(.healthy, routing.admit("http://a", now).kind);
        try std.testing.expectEqual(.healthy, routing.admit("http://b", now).kind);
    }
    try std.testing.expectEqual(@as(usize, 0), fixture.opens);
    try std.testing.expectEqual(@as(usize, 0), admission.stats().in_flight);
    {
        var first = admission.tryAcquireLease().?;
        defer first.release();
        var second = admission.tryAcquireLease().?;
        defer second.release();
        fake.expected_status = 429;
        try std.testing.expectError(error.SqlWorkerUnavailable, client.execute(.{ .tenant = service.tenant, .query = query, .stage = 0, .task = 0 }, .{}));
        const now = @import("antfly_platform").time.monotonicNs();
        try std.testing.expectEqual(.healthy, routing.admit("http://a", now).kind);
        try std.testing.expectEqual(.healthy, routing.admit("http://b", now).kind);
    }
    try std.testing.expectEqual(@as(usize, 0), admission.stats().in_flight);
    var unchanged = (try records.load(a, key)).?;
    defer unchanged.deinit(a);
    try std.testing.expectEqual(@as(u32, 1), unchanged.value.stage);
    try std.testing.expectEqual(state.Status.running, unchanged.value.status);
}

test "distributed SQL malformed scalar submission performs no durable or source operations" {
    const a = std.testing.allocator;
    var memory = storage.MemoryClient.init(a);
    defer memory.deinit();
    var fixture: Fixture = .{};
    const service = fixture.service(a, memory.client(), "coordinator");
    const input: plan.Input = .{ .table_id = 1, .schema = @splat('b'), .snapshot = "old-left", .wal_cut = 7, .cut = "retained-accepted-cut", .columns = &.{.{ .name = "id", .type = .integer }}, .estimated_bytes = 100, .estimated_rows = 3, .splits = 1 };
    var spec = try plan.choose(input, input, service.tenant, 1000000, .{ .left_keys = &.{0}, .right_keys = &.{0} }, .{}, fixture.now);
    spec.join.condition = .{ .instructions = &.{.{ .type = .{ .kind = .boolean }, .operation = .{ .call = .{ .function = .is_missing, .args = &.{} } } }}, .root = 0, .output = .{ .kind = .boolean } };
    try std.testing.expectError(error.InvalidSqlDistributedPlan, service.submit(spec));
    try std.testing.expectEqual(@as(usize, 0), fixture.retained);
    try std.testing.expectEqual(@as(u64, 0), memory.operationCount());
}
