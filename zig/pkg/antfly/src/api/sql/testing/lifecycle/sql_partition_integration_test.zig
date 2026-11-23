// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const local = @import("antfly_local_sources");
const fixture = @import("../owner_test_fixture.zig");
const client = @import("../owner_test_client.zig");
const wire = @import("antfly_metadata_openapi").types;

test "mounted SQL partition declaration rejects a unique key missing the partition column" {
    const Campaign = struct {
        pub const native_auth = true;
        sql: []const u8,

        pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
            var response = try client.execute(ctx, self.sql, .never);
            defer response.deinit(ctx.alloc);
            try std.testing.expectEqual(@as(u16, 501), response.status);
            var diagnostic = try std.json.parseFromSlice(wire.SQLDiagnostic, ctx.alloc, response.body, .{ .ignore_unknown_fields = true });
            defer diagnostic.deinit();
            try std.testing.expectEqualStrings("0A000", diagnostic.value.code);
            try std.testing.expectEqualStrings("unique constraint on partitioned table must include all partitioning columns", diagnostic.value.message);
        }

        pub fn afterRestart(_: *@This(), ctx: *fixture.Context) !void {
            var response = try client.execute(ctx, "SELECT * FROM usage_events", .read);
            defer response.deinit(ctx.alloc);
            var diagnostic = try std.json.parseFromSlice(wire.SQLDiagnostic, ctx.alloc, response.body, .{ .ignore_unknown_fields = true });
            defer diagnostic.deinit();
            try std.testing.expectEqualStrings("42P01", diagnostic.value.code);
        }
    };
    var corpus = try local.sql_parity_fixtures.Corpus.init(std.testing.allocator);
    defer corpus.deinit();
    try fixture.runWithState(Campaign, .{ .sql = (try corpus.get("sql-0421")).sql });
}

test "mounted SQL partition declaration routes inserts and publishes membership across cold recovery" {
    const Campaign = struct {
        pub const native_sessions = true;
        pub const cold_catalog = true;
        statements: [3][]const u8,
        parent_version: u32 = 0,
        leaf_version: u32 = 0,

        fn executeDdl(ctx: *fixture.Context, statement: []const u8) !?u64 {
            var response = try client.execute(ctx, statement, .write_admission);
            defer response.deinit(ctx.alloc);
            errdefer std.debug.print("Partition DDL: {s}\nHTTP {d}: {s}\n", .{ statement, response.status, response.body });
            if (response.status != 200 and response.status != 202) return error.UnexpectedHttpStatus;
            var result = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
            defer result.deinit();
            const receipt = result.value.ddl_receipt orelse return null;
            return try std.fmt.parseInt(u64, receipt.table_id, 10);
        }

        fn waitForBirth(ctx: *fixture.Context, table_id: u64) !fixture.ServingSchema {
            return fixture.waitServingSchema(ctx, "usage_events_2026", 0) catch |err| {
                const server = if (ctx.data.http_server) |*api| api else return err;
                const request: local.api_operation.RequestContext = .{ .setting_admin = true, .fk_generation_publication_authority = true, .schema_validation_authority = true };
                const bytes = server.source.systemCatalog(ctx.alloc, request, .{ .fk_initial_create_status = table_id }) catch return err;
                defer ctx.alloc.free(bytes);
                const publication = @import("../../../../metadata/fk_generation_publication.zig");
                var parsed = std.json.parseFromSlice(publication.InitialPublication, ctx.alloc, bytes, .{}) catch return err;
                defer parsed.deinit();
                const status = parsed.value;
                std.debug.print("Partition publication {s} revision={d} provisioned={d}/{d} linked={} ready={} released={d}\n", .{ @tagName(status.phase), status.revision, status.child_provisioned.len, status.plan.child_ranges.len, status.partition_journal != null, status.partition_ready, status.child_released.len });
                if (status.partition_journal) |lookup| {
                    const raw = server.source.systemCatalog(ctx.alloc, request, .{ .schema_validation_receipt_read = .{ .lookup = lookup, .expected = .admission } }) catch return err;
                    defer ctx.alloc.free(raw);
                    const receipt = @import("../../../../metadata/schema_validation/receipt.zig");
                    var observation = std.json.parseFromSlice(receipt.Observation, ctx.alloc, raw, .{}) catch return err;
                    defer observation.deinit();
                    const head = observation.value.head orelse return err;
                    const work = @import("../../../../metadata/schema_validation/work.zig");
                    const query: work.Query = .{ .group = lookup.group, .metadata_incarnation = lookup.metadata_incarnation, .cursor = .{ .scan = 0 } };
                    const page_bytes = server.source.systemCatalog(ctx.alloc, request, .{ .schema_validation_work = query }) catch |work_error| {
                        std.debug.print("Partition work discovery {s}\n", .{@errorName(work_error)});
                        return err;
                    };
                    defer ctx.alloc.free(page_bytes);
                    var page = std.json.parseFromSlice(work.Page, ctx.alloc, page_bytes, .{}) catch return err;
                    defer page.deinit();
                    std.debug.print("Partition work tasks={d}\n", .{page.value.len});
                    std.debug.print("Partition parent journal {s} ready={d} installed={d} released={d} recovery_attempts={d} last_recovery_stage={s} last_recovery_error={s}\n", .{ @tagName(head.phase), head.ready, head.installed, head.released, ctx.data.schema_recovery_attempts.load(.acquire), ctx.data.schemaValidationLastStageForTest(), ctx.data.schemaValidationLastErrorForTest() });
                }
                return err;
            };
        }

        fn expectCount(ctx: *fixture.Context, query: []const u8, expected: i64) !void {
            var response = try client.execute(ctx, query, .read);
            defer response.deinit(ctx.alloc);
            errdefer std.debug.print("Partition query: {s}\nHTTP {d}: {s}\n", .{ query, response.status, response.body });
            try std.testing.expectEqual(@as(u16, 200), response.status);
            var parsed = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
            defer parsed.deinit();
            try std.testing.expectEqual(@as(usize, 1), parsed.value.rows.len);
            try std.testing.expectEqual(@as(usize, 1), parsed.value.rows[0].len);
            const value = parsed.value.rows[0][0];
            try std.testing.expectEqual(.string, std.meta.activeTag(value));
            try std.testing.expectEqual(expected, try std.fmt.parseInt(i64, value.string, 10));
        }

        fn expectPartitionRejection(ctx: *fixture.Context, statement: []const u8) !void {
            var response = try client.execute(ctx, statement, .never);
            defer response.deinit(ctx.alloc);
            errdefer std.debug.print("Partition write: {s}\nHTTP {d}: {s}\n", .{ statement, response.status, response.body });
            var diagnostic = try std.json.parseFromSlice(wire.SQLDiagnostic, ctx.alloc, response.body, .{ .ignore_unknown_fields = true });
            defer diagnostic.deinit();
            try std.testing.expectEqualStrings("23514", diagnostic.value.code);
        }

        pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
            _ = try executeDdl(ctx, "CREATE TABLE usage_events(tenant_id text,id uuid,created_at timestamptz,PRIMARY KEY(tenant_id,id,created_at)) PARTITION BY RANGE(created_at)");
            const parent = try fixture.waitServingSchema(ctx, "usage_events", 0);
            const child_id = (try executeDdl(ctx, self.statements[0])) orelse return error.MissingSchemaValidationToken;
            const leaf = try waitForBirth(ctx, child_id);
            _ = try fixture.waitServingSchema(ctx, "usage_events", parent.version + 1);
            var response = try client.execute(ctx, "INSERT INTO usage_events VALUES('tenant','00000000-0000-0000-0000-000000000001','2026-06-01')", .write_admission);
            defer response.deinit(ctx.alloc);
            errdefer std.debug.print("Partition insert HTTP {d}: {s}\n", .{ response.status, response.body });
            try std.testing.expectEqual(@as(u16, 200), response.status);
            var parsed = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
            defer parsed.deinit();
            try std.testing.expectEqual(@as(i64, 1), parsed.value.rows_affected);
            try expectCount(ctx, "SELECT count(*) FROM usage_events", 1);

            _ = try executeDdl(ctx, self.statements[2]);
            _ = try fixture.waitServingSchema(ctx, "usage_events_2026", leaf.version + 1);
            _ = try fixture.waitServingSchema(ctx, "usage_events", parent.version + 2);
            try expectCount(ctx, "SELECT count(*) FROM usage_events", 0);
            try expectCount(ctx, "SELECT count(*) FROM usage_events_2026", 1);

            _ = try executeDdl(ctx, self.statements[1]);
            self.leaf_version = (try fixture.waitServingSchema(ctx, "usage_events_2026", leaf.version + 2)).version;
            self.parent_version = (try fixture.waitServingSchema(ctx, "usage_events", parent.version + 3)).version;
            try expectCount(ctx, "SELECT count(*) FROM usage_events", 1);
            try expectCount(ctx, "SELECT count(*) FROM usage_events_2026", 1);
        }

        pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
            _ = try fixture.waitServingSchema(ctx, "usage_events", self.parent_version);
            _ = try fixture.waitServingSchema(ctx, "usage_events_2026", self.leaf_version);
            try expectPartitionRejection(ctx, "INSERT INTO usage_events VALUES('tenant','00000000-0000-0000-0000-000000000002','2027-01-01')");
            try expectCount(ctx, "SELECT count(*) FROM usage_events", 1);
            try expectCount(ctx, "SELECT count(*) FROM usage_events_2026", 1);

            _ = try executeDdl(ctx, self.statements[2]);
            _ = try fixture.waitServingSchema(ctx, "usage_events_2026", self.leaf_version + 1);
            _ = try fixture.waitServingSchema(ctx, "usage_events", self.parent_version + 1);
            try expectCount(ctx, "SELECT count(*) FROM usage_events", 0);
            try expectCount(ctx, "SELECT count(*) FROM usage_events_2026", 1);
        }
    };
    var corpus = try local.sql_parity_fixtures.Corpus.init(std.testing.allocator);
    defer corpus.deinit();
    try fixture.runWithState(Campaign, .{ .statements = .{
        (try corpus.get("sql-0422")).sql,
        (try corpus.get("sql-0423")).sql,
        (try corpus.get("sql-0424")).sql,
    } });
}
