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
const db = local.storage_db_mod;
const contract = local.storage_db_relational_population_contract;
const staging = local.storage_db_restore_staging_contract;
const catalog = local.sql_catalog;
const operation = local.api_operation;
const owner = @import("restore_owner.zig");
const delivery = @import("../api/sql_population_delivery.zig");

const Backend = struct {
    fn resolve(_: *anyopaque, _: std.mem.Allocator, _: local.sql_ast.Name, _: catalog.Action) !catalog.Table {
        return error.TableNotFound;
    }
    fn scan(_: *anyopaque, _: std.mem.Allocator, _: catalog.Table, _: catalog.Scan) !catalog.Page {
        return error.UnexpectedRowScan;
    }
    fn mutate(_: *anyopaque, _: std.mem.Allocator, _: std.mem.Allocator, _: catalog.Table, _: []const catalog.Mutation) !catalog.MutationOutcome {
        return error.UnexpectedMutation;
    }
    fn checkpoint(_: *anyopaque) !void {}
    fn ddl(_: *anyopaque, _: catalog.DdlAllocators, _: catalog.Ddl) !catalog.DdlOutcome {
        return error.UnexpectedMutation;
    }
};
const Receiver = struct {
    target: *db.DB,
    runtime: *db.background_runtime.BackendRuntime,
    path: []const u8,
    options: db.OpenOptions,
    scope: staging.Scope,
    live: *bool,
    index: u64 = 0,
    page_loss: bool = false,
    seal_loss: bool = false,
    corrupt_checked: bool = false,
    fn environment(self: *@This()) owner.Environment {
        return .{ .io = std.testing.io, .runtime = self.runtime, .location_options = .{}, .cache_path = self.path, .proposer = .{ .ptr = self, .propose = propose } };
    }
    fn propose(raw: *anyopaque, input: db.types.BatchRequest, context: operation.RequestContext) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        try context.ensureActive();
        var batch = input;
        batch.restore_staging_scope = self.scope.digest();
        if (input.writes.len != 0 and !self.corrupt_checked) {
            // Exercise the native admission fence directly, independently of
            // the preparation-time range check and the routing adapter.
            var writes: [contract.max_page_rows]db.types.BatchWrite = undefined;
            @memcpy(writes[0..input.writes.len], input.writes);
            var ordinal: u64 = 0;
            while (self.target.getRange().contains(&contract.rowKey(self.scope.population.?.capture_id, ordinal))) ordinal += 1;
            const wrong_key = contract.rowKey(self.scope.population.?.capture_id, ordinal);
            writes[0].key = &wrong_key;
            var corrupt = batch;
            corrupt.writes = writes[0..input.writes.len];
            var timestamps: [contract.max_page_rows]staging.Timestamp = undefined;
            @memcpy(timestamps[0..input.writes.len], input.restore_staging.?.import_page.timestamps);
            timestamps[0].key = &wrong_key;
            corrupt.restore_staging.?.import_page.timestamps = timestamps[0..input.writes.len];
            try std.testing.expectError(error.KeyOutOfRange, @import("server_db_adapter.zig").applyOrdered(self.target, corrupt, .{ .term = 1, .index = self.index + 1 }));
            self.corrupt_checked = true;
        }
        try @import("server_db_adapter.zig").applyOrdered(self.target, batch, .{ .term = 1, .index = self.index + 1 });
        self.index += 1;
        if (input.restore_staging) |command| if (command == .import_page) {
            if (command.import_page.population.? == .page and !self.page_loss) {
                self.page_loss = true;
                return error.InjectedPopulationReplyLoss;
            }
            if (command.import_page.population.? == .complete and !self.seal_loss) {
                self.seal_loss = true;
                return error.InjectedPopulationReplyLoss;
            }
        };
    }
    fn execute(raw: *anyopaque, a: std.mem.Allocator, _: []const u8, group: u64, request: owner.Request, context: operation.RequestContext) !owner.Response {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (group != self.scope.target_namespace.shard_id) return error.InvalidRelationPopulation;
        const outcome = owner.executeResident(a, self.target, self.environment(), request, context);
        if (request.action == .import_page) {
            self.target.close();
            self.live.* = false;
            self.target.* = try db.DB.open(a, self.path, self.options);
            self.live.* = true;
        }
        return outcome;
    }
};

test "restore owner SQL population routes one typed query across native LSM owners with lost replies and independent reopen" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var arena: std.heap.ArenaAllocator = .init(a);
    defer arena.deinit();
    const region = arena.allocator();
    const root = try tmp.dir.realPathFileAlloc(io, ".", region);
    var text: std.Io.Writer.Allocating = .init(a);
    defer text.deinit();
    try text.writer.writeAll("CREATE TABLE dst(n,j) AS VALUES ");
    for (0..257) |i| {
        if (i != 0) try text.writer.writeByte(',');
        try text.writer.print("({d}::BIGINT,{s}::JSONB)", .{ i, if (i % 2 == 0) "'null'" else "NULL" });
    }
    var compiled = try local.sql_compiler.compile(a, text.written(), .{});
    defer compiled.deinit();
    var unused: u8 = 0;
    const backend: catalog.Backend = .{ .ptr = &unused, .execution_io = io, .vtable = &.{ .resolve = Backend.resolve, .scan = Backend.scan, .mutate = Backend.mutate, .checkpoint = Backend.checkpoint, .ddl = Backend.ddl } };
    const source = try local.sql_relation_population.Source.init(region, backend, &compiled, compiled.statement.catalog_ddl.population.?, &.{}, .{ .result_rows = 1 });
    defer source.deinit();
    var runtime = try db.background_runtime.BackendRuntime.init(a, .{ .backend = .manual, .filesystem_io = io });
    defer runtime.deinit();
    var targets: [2]db.DB = undefined;
    var live: [2]bool = @splat(false);
    defer for (&targets, live) |*target, opened| if (opened) target.close();
    var receivers: [2]Receiver = undefined;
    var destinations: [2]delivery.Destination = undefined;
    for (&targets, &receivers, &destinations, 0..) |*target, *receiver, *destination, i| {
        const range: db.types.ByteRange = if (i == 0) .{ .start = "", .end = "8" } else .{ .start = "8", .end = "" };
        const path = try std.fmt.allocPrint(region, "{s}/owner-{d}", .{ root, i });
        const options: db.OpenOptions = .{ .backend_runtime = &runtime, .identity_namespace = .{ .table_id = 31, .shard_id = 32 + i, .range_id = 32 + i }, .primary_backend = .{ .lsm = .{} }, .start_optional_runtimes = false, .start_index_workers = false };
        target.* = try db.DB.open(a, path, options);
        live[i] = true;
        try target.updateRange(range);
        try target.setSchemaJson(a, source.schema_json);
        const native_schema = try local.storage_schema.serializeSchema(region, target.core.schema.?);
        const binding: contract.Binding = .{ .capture_id = @splat(1), .definition = source.definition, .source_definition = source.source_definition, .target_schema = staging.digest(native_schema), .principal_instance = @splat(5), .catalog_revision = 6, .write_version = 42, .partition = 32 + i };
        const scope: staging.Scope = .{ .plan_id = @splat(1), .plan_digest = @splat(2), .source_artifact_digest = binding.fingerprint(), .source_namespace = .{ .table_id = 0, .shard_id = 0, .range_id = 0 }, .target_namespace = options.identity_namespace.?, .target_schema_digest = binding.target_schema, .population = binding };
        try target.reserveRestoreStagingScoped(a, scope);
        receiver.* = .{ .target = target, .runtime = &runtime, .path = path, .options = options, .scope = scope, .live = &live[i] };
        destination.* = .{ .port = .{ .ptr = receiver, .execute_fn = Receiver.execute }, .table = "dst", .group = 32 + i, .scope = scope, .range = range };
    }
    var stats: delivery.Stats = .{};
    const receipts = try delivery.deliverCohort(a, &destinations, source.iface(), .{}, .{ .stats = &stats });
    defer a.free(receipts);
    try std.testing.expectEqual(@as(u64, 257), source.emitted);
    try std.testing.expectEqual(@as(u64, 257), stats.source_rows);
    try std.testing.expect(stats.page_requests <= 4);
    var rows: usize = 0;
    for (&receivers, destinations, receipts) |*receiver, destination, receipt| {
        try std.testing.expect(receiver.page_loss and receiver.seal_loss and receiver.corrupt_checked);
        try std.testing.expect(receipt.sealed and receipt.rows > 64);
        try std.testing.expectEqual(@as(?u64, 257), receipt.source_rows);
        _ = try owner.executeResident(a, receiver.target, receiver.environment(), .{ .scope = destination.scope, .action = .validate }, .{});
        _ = try owner.executeResident(a, receiver.target, receiver.environment(), .{ .scope = destination.scope, .action = .publish }, .{});
        var reader = try receiver.target.beginRelationalRows(a, .{ .fields = &.{ "n", "j" } });
        defer reader.deinit();
        while (true) {
            var page = try reader.nextTypedPage(a, io, .{ .time_ns = std.time.ns_per_s });
            defer page.deinit();
            for (page.rows) |row| {
                const ordinal = try std.fmt.parseInt(u64, row.key[row.key.len - 16 ..], 16);
                try std.testing.expect(ordinal < 257 and destination.range.contains(row.key));
                try std.testing.expectEqualSlices(u8, &contract.rowKey(destination.scope.population.?.capture_id, ordinal), row.key);
                try std.testing.expectEqualSlices(bool, &.{ false, ordinal % 2 != 0 }, row.sql_nulls.?);
                const decoded = row.typed.?.object.get("n").?;
                try std.testing.expectEqual(@as(i64, @intCast(ordinal)), decoded.integer);
                rows += 1;
            }
            if (!page.more) break;
        }
    }
    try std.testing.expectEqual(@as(usize, 257), rows);
    const replay = try delivery.deliverCohort(a, &destinations, source.iface(), .{}, .{});
    defer a.free(replay);
    try std.testing.expectEqualDeep(receipts, replay);
    try std.testing.expectEqual(@as(u64, 257), source.emitted);
}
