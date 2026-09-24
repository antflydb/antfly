// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
const std = @import("std");
const http = @import("http_server.zig");
const truncate = @import("sql_truncate.zig");
const domain = @import("../system_catalog/domain.zig");
const metadata = @import("../metadata/api.zig");
const records = @import("../common/topology_records.zig");
const stages = @import("../metadata/restore_staging.zig");
const jobs = @import("restore_jobs.zig");
const compiler = @import("../sql/compiler.zig");
const reads = @import("table_read_source.zig");
const operation = @import("operation.zig");
const alloc = std.testing.allocator;

const Fixture = struct {
    logical_name: []const u8 = "rows",
    second_logical_name: ?[]const u8 = null,
    unknown: bool = false,
    wrong_owner: bool = false,
    catalog_reads: usize = 0,
    owner_reads: usize = 0,
    integrity_reads: usize = 0,
    admissions: usize = 0,
    job_key: ?[]u8 = null,
    job_value: ?[]u8 = null,
    plan: ?[]u8 = null,
    tables: [2]records.TableRecord = .{ .{ .table_id = 100, .name = "physical", .min_ranges = 1, .indexes_json = "{}" }, .{ .table_id = 101, .name = "physical_archive", .min_ranges = 1, .indexes_json = "{}" } },
    ranges: [2]records.RangeRecord = .{ .{ .table_id = 100, .group_id = 200, .range_id = 200, .doc_identity_shard_id = 200, .doc_identity_range_id = 200, .start_key = "", .end_key = null }, .{ .table_id = 101, .group_id = 201, .range_id = 201, .doc_identity_shard_id = 201, .doc_identity_range_id = 201, .start_key = "", .end_key = null } },

    fn tableCount(self: *const @This()) usize {
        return if (self.second_logical_name == null) 1 else 2;
    }

    fn cast(ptr: *anyopaque) *@This() {
        return @ptrCast(@alignCast(ptr));
    }
    fn deinit(self: *@This()) void {
        if (self.job_key) |value| alloc.free(value);
        if (self.job_value) |value| alloc.free(value);
        if (self.plan) |value| alloc.free(value);
    }
    fn status(_: *anyopaque) !metadata.MetadataStatus {
        return .{ .metadata_group_id = 1, .metrics = .{} };
    }
    fn catalog(ptr: *anyopaque, a: std.mem.Allocator, _: operation.RequestContext, call: domain.Call) ![]u8 {
        const self = cast(ptr);
        self.catalog_reads += 1;
        return switch (call) {
            .snapshot => blk: {
                const resources = try a.alloc(domain.Resource, 2 + self.tableCount());
                resources[0] = domain.default_database;
                resources[1] = domain.default_namespace;
                resources[2] = .{ .kind = .table, .id = 100, .parent_id = domain.default_namespace_id, .name = self.logical_name, .storage_name = "physical" };
                if (self.second_logical_name) |name| resources[3] = .{ .kind = .table, .id = 101, .parent_id = domain.default_namespace_id, .name = name, .storage_name = "physical_archive" };
                break :blk std.json.Stringify.valueAlloc(a, domain.State{ .revision = 7, .next_id = 102, .resources = resources }, .{});
            },
            .resolve_many => |query| blk: {
                if (query.storage_names.len != 0) {
                    const logical = try a.alloc(?[]const u8, query.storage_names.len);
                    const empty = try a.alloc(?domain.ResolvedTable, 0);
                    for (query.storage_names, logical) |physical, *name| {
                        name.* = if (std.mem.eql(u8, physical, "physical")) self.logical_name else if (std.mem.eql(u8, physical, "physical_archive")) self.second_logical_name orelse return error.UnexpectedCall else return error.UnexpectedCall;
                    }
                    break :blk std.json.Stringify.valueAlloc(a, domain.ResolvedMany{ .revision = 7, .tables = empty, .logical_names = logical }, .{});
                }
                const found = try a.alloc(?domain.ResolvedTable, query.targets.len);
                const names = try a.alloc(?[]const u8, query.targets.len);
                for (query.targets, found, names) |target, *resolved, *name| {
                    const archive = self.second_logical_name != null and std.mem.eql(u8, target.table, self.second_logical_name.?);
                    if (!archive and !std.mem.eql(u8, target.table, self.logical_name)) return error.UnexpectedCall;
                    resolved.* = .{ .table_id = if (archive) 101 else 100, .name = if (archive) "physical_archive" else "physical" };
                    name.* = target.table;
                }
                break :blk std.json.Stringify.valueAlloc(a, domain.ResolvedMany{ .revision = 7, .tables = found, .logical_names = names }, .{});
            },
            else => error.UnexpectedCall,
        };
    }
    fn snapshot(ptr: *anyopaque, _: operation.RequestContext) !?metadata.AdminSnapshot {
        const self = cast(ptr);
        return .{ .status = try status(ptr), .tables = self.tables[0..self.tableCount()], .ranges = self.ranges[0..self.tableCount()], .stores = &.{}, .placement_intents = &.{}, .split_transitions = &.{}, .merge_transitions = &.{} };
    }
    fn freeSnapshot(_: *anyopaque, _: *metadata.AdminSnapshot) void {}
    fn lookup(ptr: *anyopaque, a: std.mem.Allocator, table: []const u8, _: []const u8, options: @import("../storage/db/types.zig").LookupOptions, _: @import("../raft/read_gate.zig").ReadConsistency) !?reads.LookupResponse {
        const self = cast(ptr);
        const archive = std.mem.eql(u8, table, "physical_archive");
        try std.testing.expect(archive or std.mem.eql(u8, table, "physical"));
        if (options.relational_integrity_catalog) {
            try std.testing.expect(archive);
            self.integrity_reads += 1;
            const schema_api = @import("../schema/mod.zig");
            const native_schema = @import("../storage/schema.zig");
            const declarations = @import("../schema/relational_declarations.zig");
            const integrity_catalog = @import("../storage/db/relational_integrity_catalog.zig");
            var parsed = try schema_api.parseValidatedTableSchema(a, self.tables[1].schema_json);
            defer parsed.deinit(a);
            const runtime = try schema_api.deriveRuntimeTableSchema(a, parsed);
            defer native_schema.freeSchema(a, runtime);
            const bytes = try native_schema.serializeSchema(a, runtime);
            defer a.free(bytes);
            var digest: [32]u8 = undefined;
            std.crypto.hash.Blake3.hash(bytes, &digest, .{});
            const definitions = try declarations.definitionFingerprints(a, parsed, runtime);
            defer declarations.freeDefinitions(a, definitions);
            var prepared = try integrity_catalog.prepare(a, null, try integrity_catalog.incarnationFromTableId(101), runtime.version, digest, definitions);
            defer prepared.deinit();
            const encoded = try a.alloc(u8, std.base64.standard.Encoder.calcSize(prepared.value.len));
            _ = std.base64.standard.Encoder.encode(encoded, prepared.value);
            return .{ .version = 0, .json = try std.json.Stringify.valueAlloc(a, .{ .catalog = encoded, .schema_version = runtime.version, .table_id = "101" }, .{}) };
        }
        try std.testing.expectEqualStrings("{\"mode\":\"identity\"}", options.relational_topology_json);
        try std.testing.expectEqual(@as(usize, 0), self.admissions);
        self.owner_reads += 1;
        return .{ .version = 0, .json = try std.json.Stringify.valueAlloc(a, .{ .namespace = .{ .table_id = @as(u64, if (self.wrong_owner) 999 else if (archive) 101 else 100), .shard_id = @as(u64, if (archive) 201 else 200), .range_id = @as(u64, if (archive) 201 else 200) }, .catalog_digest = @as([32]u8, @splat(3)), .next_epoch = @as(u64, 1) }, .{}) };
    }
    fn load(ptr: *anyopaque, a: std.mem.Allocator) ![]jobs.ReplicatedPersistence.OwnedRow {
        const self = cast(ptr);
        const result = try a.alloc(jobs.ReplicatedPersistence.OwnedRow, if (self.job_key != null) 1 else 0);
        if (self.job_key) |key| result[0] = .{ .key = try a.dupe(u8, key), .value = try a.dupe(u8, self.job_value.?) };
        return result;
    }
    fn get(ptr: *anyopaque, a: std.mem.Allocator, key: []const u8) !?[]u8 {
        const self = cast(ptr);
        if (self.job_key) |stored| if (std.mem.eql(u8, stored, key)) return try a.dupe(u8, self.job_value.?);
        return null;
    }
    fn create(ptr: *anyopaque, a: std.mem.Allocator, key: []const u8, value: []const u8, plan: []const u8, _: u64) ![]u8 {
        const self = cast(ptr);
        try std.testing.expectEqual(self.tableCount(), self.owner_reads);
        try std.testing.expectEqual(@as(usize, 0), self.admissions);
        var decoded = try std.json.parseFromSlice(stages.Plan, a, plan, .{});
        defer decoded.deinit();
        try decoded.value.validate(a);
        self.job_key = try alloc.dupe(u8, key);
        self.job_value = try alloc.dupe(u8, value);
        self.plan = try alloc.dupe(u8, plan);
        self.admissions += 1;
        if (self.unknown) return error.MetadataMutationOutcomeUnknown;
        return a.dupe(u8, value);
    }
    fn put(_: *anyopaque, _: []const u8, _: []const u8, _: u64) !void {
        return error.UnexpectedCall;
    }
    fn delete(_: *anyopaque, _: []const u8, _: u64) !void {
        return error.UnexpectedCall;
    }
    fn deleteMany(_: *anyopaque, _: []const []const u8, _: u64) !void {
        return error.UnexpectedCall;
    }
    fn persistence(self: *@This()) jobs.ReplicatedPersistence {
        return jobs.ReplicatedPersistence.fromLocal(self, .{ .load = load, .get = get, .create_with_staging = create, .put = put, .delete = delete, .delete_many = deleteMany });
    }
    fn server(self: *@This()) !http.ApiHttpServer {
        var result = http.ApiHttpServer.init(alloc, .{}, .{ .ptr = self, .vtable = &.{ .status = status, .system_catalog = catalog, .linearizable_snapshot = snapshot, .free_admin_snapshot = freeSnapshot } }, .{ .ptr = self, .vtable = &.{ .lookup = lookup, .scan = undefined, .query = undefined } }, null);
        errdefer result.deinit();
        result.restore_job_store.io = std.testing.io;
        try result.restore_job_store.attachReplicated(self.persistence());
        // No owner mutation capability is provided. Admission must only
        // persist the compound metadata proposal, never dispatch live writes.
        return result;
    }
};

const ddl: @import("../sql/ast.zig").CatalogDdl = .{ .kind = .table, .action = .truncate, .name = .{ .table = "rows" }, .truncate_tables = &.{.{ .table = "rows" }} };

test "SQL TRUNCATE original single-table source cases use durable generation admission" {
    // sql-0160, sql-0161, sql-0162, sql-0163, sql-1101. The original
    // mutation-source plan is superseded by the durable empty-generation
    // barrier; it intentionally requires whole-table admin authority.
    for ([_]struct { sql: []const u8, logical_name: []const u8, restart: bool }{
        .{ .sql = "TRUNCATE TABLE usage_records;", .logical_name = "usage_records", .restart = false },
        .{ .sql = "TRUNCATE ONLY public.usage_records;", .logical_name = "usage_records", .restart = false },
        .{ .sql = "TRUNCATE TABLE usage_records CONTINUE IDENTITY RESTRICT;", .logical_name = "usage_records", .restart = false },
        .{ .sql = "TRUNCATE TABLE usage_records RESTART IDENTITY;", .logical_name = "usage_records", .restart = true },
        .{ .sql = "TRUNCATE docs", .logical_name = "docs", .restart = false },
    }) |case| {
        var parsed = try compiler.compile(alloc, case.sql, .{});
        defer parsed.deinit();
        try std.testing.expect(parsed.statement == .catalog_ddl);
        const statement = parsed.statement.catalog_ddl;
        try std.testing.expectEqual(@as(usize, 1), statement.truncate_tables.len);
        try std.testing.expectEqual(case.restart, statement.restart_identity);
        var fixture: Fixture = .{ .logical_name = case.logical_name };
        defer fixture.deinit();
        var server = try fixture.server();
        defer server.deinit();
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const outcome = try truncate.execute(&server, null, .{}, "default", "public", arena.allocator(), statement);
        try std.testing.expectEqual(@as(usize, 1), fixture.admissions);
        try std.testing.expectEqual(@as(@TypeOf(outcome.receipt.?.state), .pending), outcome.receipt.?.state);
        var plan = try std.json.parseFromSlice(stages.Plan, alloc, fixture.plan.?, .{});
        defer plan.deinit();
        try std.testing.expect(plan.value.targets[0].empty_generation);
        try std.testing.expectEqualStrings(case.logical_name, plan.value.targets[0].catalog_binding.?.name);
    }
}

test "SQL TRUNCATE original multi-table case admits one atomic empty cohort" {
    // sql-0164: both tables enter one immutable staging plan and one job.
    var parsed = try compiler.compile(alloc, "TRUNCATE TABLE usage_records, archived_records;", .{});
    defer parsed.deinit();
    try std.testing.expect(parsed.statement == .catalog_ddl);
    try std.testing.expectEqual(@as(usize, 2), parsed.statement.catalog_ddl.truncate_tables.len);
    var fixture: Fixture = .{ .logical_name = "usage_records", .second_logical_name = "archived_records" };
    defer fixture.deinit();
    var server = try fixture.server();
    defer server.deinit();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const outcome = try truncate.execute(&server, null, .{}, "default", "public", arena.allocator(), parsed.statement.catalog_ddl);
    try std.testing.expectEqual(@as(@TypeOf(outcome.receipt.?.state), .pending), outcome.receipt.?.state);
    try std.testing.expectEqual(@as(usize, 2), fixture.owner_reads);
    try std.testing.expectEqual(@as(usize, 1), fixture.admissions);
    var plan = try std.json.parseFromSlice(stages.Plan, alloc, fixture.plan.?, .{});
    defer plan.deinit();
    try std.testing.expectEqual(@as(usize, 2), plan.value.targets.len);
    try std.testing.expectEqualStrings("usage_records", plan.value.targets[0].catalog_binding.?.name);
    try std.testing.expectEqualStrings("archived_records", plan.value.targets[1].catalog_binding.?.name);
    for (plan.value.targets) |target| {
        try std.testing.expect(target.empty_generation);
        try std.testing.expect(target.table.table_id != target.source_table_id);
        try std.testing.expectEqual(@as(usize, 1), target.ranges.len);
    }
}

test "SQL TRUNCATE original CASCADE case closes incoming FK cohort" {
    // sql-0165: CASCADE adds the referencing child, never an outgoing parent.
    var parsed = try compiler.compile(alloc, "TRUNCATE TABLE usage_records CASCADE;", .{});
    defer parsed.deinit();
    try std.testing.expect(parsed.statement == .catalog_ddl);
    try std.testing.expect(parsed.statement.catalog_ddl.cascade);
    var fixture: Fixture = .{ .logical_name = "usage_records", .second_logical_name = "archived_records" };
    defer fixture.deinit();
    fixture.tables[0].schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"pk","columns":["id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
    ;
    fixture.tables[1].schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","foreign_keys":[{"name":"fk","child_columns":["parent_id"],"parent_table":"physical","parent_columns":["id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"parent_id":{"type":"integer"}},"additionalProperties":false}}}}
    ;
    var server = try fixture.server();
    defer server.deinit();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const outcome = try truncate.execute(&server, null, .{}, "default", "public", arena.allocator(), parsed.statement.catalog_ddl);
    try std.testing.expectEqual(@as(@TypeOf(outcome.receipt.?.state), .pending), outcome.receipt.?.state);
    try std.testing.expectEqual(@as(usize, 2), fixture.owner_reads);
    try std.testing.expectEqual(@as(usize, 1), fixture.admissions);
    var plan = try std.json.parseFromSlice(stages.Plan, alloc, fixture.plan.?, .{});
    defer plan.deinit();
    try std.testing.expectEqual(@as(usize, 2), plan.value.targets.len);
    try std.testing.expectEqualStrings("usage_records", plan.value.targets[0].catalog_binding.?.name);
    try std.testing.expectEqualStrings("archived_records", plan.value.targets[1].catalog_binding.?.name);
}

test "SQL TRUNCATE child-only external parent admits exact retirement plan and reconciles unknown reply" {
    for ([_]bool{ false, true }) |unknown| {
        var fixture: Fixture = .{ .logical_name = "parent", .second_logical_name = "child", .unknown = unknown };
        defer fixture.deinit();
        fixture.tables[0].schema_json =
            \\{"version":1,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"pk","columns":["id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
        ;
        fixture.tables[1].schema_json =
            \\{"version":1,"storage_mode":"relational","default_type":"row","foreign_keys":[{"name":"fk","child_columns":["parent_id"],"parent_table":"physical","parent_columns":["id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"parent_id":{"type":"integer"}},"additionalProperties":false}}}}
        ;
        var parsed = try compiler.compile(alloc, "TRUNCATE child", .{});
        defer parsed.deinit();
        var server = try fixture.server();
        defer server.deinit();
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const outcome = try truncate.execute(&server, null, .{}, "default", "public", arena.allocator(), parsed.statement.catalog_ddl);
        try std.testing.expectEqual(@as(usize, 1), fixture.integrity_reads);
        try std.testing.expectEqual(@as(usize, 2), fixture.owner_reads);
        try std.testing.expectEqual(@as(usize, 1), fixture.admissions);
        try std.testing.expectEqual(@as(@TypeOf(outcome.receipt.?.state), if (unknown) .admission_unknown else .pending), outcome.receipt.?.state);
        var plan = try std.json.parseFromSlice(stages.Plan, alloc, fixture.plan.?, .{});
        defer plan.deinit();
        try std.testing.expectEqual(@as(usize, 1), plan.value.targets.len);
        try std.testing.expectEqual(@as(usize, 1), plan.value.external_fk_parents.len);
        const parent = plan.value.external_fk_parents[0];
        try std.testing.expectEqual(@as(u64, 100), parent.table.table_id);
        try std.testing.expectEqual(@as(u64, 200), parent.fences[0].owner_group_id);
        try std.testing.expectEqual(@as(u64, 101), parent.foreign_keys[0].child_table_id);
        try std.testing.expectEqualStrings("fk", parent.foreign_keys[0].constraint_name);
        try std.testing.expect(!std.mem.eql(u8, &parent.foreign_keys[0].generation, &parent.foreign_keys[0].next_generation));
    }
}

test "SQL TRUNCATE native admission persists fresh plan and reconciles unknown reply" {
    for ([_]struct { unknown: bool, restart_identity: bool }{
        .{ .unknown = false, .restart_identity = false },
        .{ .unknown = true, .restart_identity = false },
        .{ .unknown = false, .restart_identity = true },
    }) |case| {
        const unknown = case.unknown;
        var fixture: Fixture = .{ .unknown = unknown };
        defer fixture.deinit();
        var server = try fixture.server();
        defer server.deinit();
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        var statement = ddl;
        statement.restart_identity = case.restart_identity;
        const outcome = try truncate.execute(&server, null, .{}, "default", "public", arena.allocator(), statement);
        try std.testing.expectEqual(@as(usize, 1), fixture.admissions);
        try std.testing.expectEqual(@as(u64, 100), fixture.tables[0].table_id);
        try std.testing.expectEqual(if (unknown) null else @as(?@import("../sql/catalog.zig").MutationOutcome, .committed_pending), outcome.mutation_outcome);
        try std.testing.expectEqual(@as(@TypeOf(outcome.receipt.?.state), if (unknown) .admission_unknown else .pending), outcome.receipt.?.state);
        const id = try std.fmt.parseInt(u64, outcome.receipt.?.restore_job_id.?, 10);
        var recovered = jobs.Store.initWithIo(alloc, std.testing.io);
        defer recovered.deinit();
        try recovered.attachReplicated(fixture.persistence());
        const bytes = (try recovered.load(alloc, id)).?;
        defer alloc.free(bytes);
        var job = try std.json.parseFromSlice(jobs.JobState, alloc, bytes, .{});
        defer job.deinit();
        try std.testing.expectEqual(.empty_generation, job.value.source_kind);
        try std.testing.expectEqual(.queued, job.value.phase);
        var plan = try std.json.parseFromSlice(stages.Plan, alloc, fixture.plan.?, .{});
        defer plan.deinit();
        const target = plan.value.targets[0];
        try std.testing.expect(target.empty_generation and target.table.table_id != 100);
        try std.testing.expectEqual(@as(u64, 100), target.replace.?.table.table_id);
        try std.testing.expectEqual(target.table.table_id, target.catalog_binding.?.id);
        try std.testing.expectEqualStrings("rows", target.catalog_binding.?.name);
        try std.testing.expectEqual(@as(usize, 0), target.source_artifacts.len);
        try std.testing.expectEqual(@as(usize, 0), target.rewrite_sources.len);
    }
}

test "SQL TRUNCATE authorization and stale owner fail before durable admission" {
    var fixture: Fixture = .{};
    defer fixture.deinit();
    var server = try fixture.server();
    defer server.deinit();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    var identity: http.AuthenticatedIdentity = .{ .username = @constCast("user") };
    try std.testing.expectError(error.Forbidden, truncate.execute(&server, identity, .{}, "default", "public", arena.allocator(), ddl));
    try std.testing.expectEqual(@as(usize, 0), fixture.catalog_reads);
    var permissions = [_]@import("../usermgr/mod.zig").Permission{.{ .resource = @constCast("*"), .resource_type = .table, .type = .admin }};
    var filters = [_]@import("../usermgr/mod.zig").RowFilterEntry{.{ .table = @constCast("rows"), .filter = @constCast("{}") }};
    identity.permissions = &permissions;
    identity.row_filter = &filters;
    try std.testing.expectError(error.Forbidden, truncate.execute(&server, identity, .{}, "default", "public", arena.allocator(), ddl));
    try std.testing.expectEqual(@as(usize, 0), fixture.catalog_reads);
    fixture.wrong_owner = true;
    try std.testing.expectError(error.TableGenerationChanged, truncate.execute(&server, null, .{}, "default", "public", arena.allocator(), ddl));
    try std.testing.expectEqual(@as(usize, 1), fixture.owner_reads);
    try std.testing.expectEqual(@as(usize, 0), fixture.admissions);
    try std.testing.expect(fixture.plan == null and fixture.job_value == null);
}
