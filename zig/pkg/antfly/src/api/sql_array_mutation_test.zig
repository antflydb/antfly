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
const local = @import("antfly_local_sources");
const db_mod = local.storage_db_selected_root.db;
const parity = @import("sql_parity_reference.zig");
const Source = @import("sql_parity_sources.zig").Tables(1);
const Server = @import("http_server.zig").ApiHttpServer;
const Handler = @import("httpx_handler.zig").AntflyApiHandler;
const reads = @import("table_reads.zig");

const Golden = struct {
    campaigns: []const struct { name: []const u8, reference: parity.PostgresMutationReference },
    uuid_contract: struct { id: []const u8, columns: []const []const u8, column_oids: []const u32, affected: i64, target_key: []const u8, storage_column: []const u8 },
};

test "httpx SQL original typed array mutations retain bounds nulls and complete cold postimages" {
    const a = std.testing.allocator;
    var golden = try std.json.parseFromSlice(Golden, a, local.sql_parity_fixtures.array_mutation_reference, .{ .ignore_unknown_fields = true });
    defer golden.deinit();
    try std.testing.expectEqual(@as(usize, 2), golden.value.campaigns.len);
    var completed: usize = 0;
    for (golden.value.campaigns) |campaign| for (campaign.reference.entries) |entry| {
        errdefer std.debug.print("typed-array mutation {s}/{s}\n", .{ campaign.name, entry.id });
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const scratch = arena.allocator();
        const profile = campaign.reference.profile;
        try std.testing.expectEqual(@as(usize, 0), profile.additional_tables.len);
        var declaration = profile.schema;
        if (profile.primary_key.len != 0) {
            try std.testing.expect(try local.sql_schema_ddl.apply(scratch, &declaration, .{
                .kind = .table,
                .action = .alter_schema,
                .name = .{ .table = "usage_records" },
                .schema_change = .{ .add_unique = .{ .name = "usage_records_pkey", .columns = profile.primary_key, .primary = true } },
            }));
        }
        const schema = try std.json.Stringify.valueAlloc(scratch, declaration, .{});
        var directory = try local.common_test_directory.TestDirectory.init("sql-array-mutation");
        defer directory.cleanup();
        const options: db_mod.OpenOptions = .{ .start_optional_runtimes = false, .start_index_workers = false, .primary_backend = .{ .lsm = .{} }, .identity_namespace = .{ .table_id = 7, .shard_id = 1, .range_id = 1 } };
        var database = try db_mod.DB.open(a, directory.path(), options);
        var opened = true;
        defer if (opened) database.close();
        try database.setSchemaJson(a, schema);
        {
            var ranges = [_]local.common_topology_records.RangeRecord{.{ .table_id = 7, .group_id = 7, .range_id = 1, .start_key = "", .doc_identity_shard_id = 1, .doc_identity_range_id = 1 }};
            var source: Source = .{
                .records = .{.{ .table_id = 7, .name = "usage_records", .schema_json = schema }},
                .reads = .{reads.BoundTableReadSource.init("usage_records", 7, &database, @import("../raft/mod.zig").read_gate.alreadyReadSafeBarrier())},
                .ranges = &ranges,
            };
            var writes = @import("antfly_source_root").antfly_sources.table_writes.BoundTableWriteSource.init("usage_records", &database);
            var runtime = try db_mod.background_runtime.BackendRuntimeHandle.init(a, .{ .backend = .io_threaded });
            defer runtime.deinit();
            var server = Server.init(a, .{ .backend_runtime = runtime.ptr() }, .{ .ptr = &source, .vtable = &.{ .status = Source.status, .system_catalog = Source.systemCatalog, .admin_snapshot = Source.snapshot, .free_admin_snapshot = Source.freeSnapshot, .supports_query_definitions = true } }, source.source(), writes.source());
            defer server.deinit();
            var handler: Handler = .{ .api_server = &server };
            var reference = campaign.reference;
            reference.entries = &.{entry};
            const tables = [_]struct { name: []const u8, db: *db_mod.DB, requires_integrity: bool }{.{ .name = "usage_records", .db = &database, .requires_integrity = profile.primary_key.len != 0 or profile.unique.len != 0 }};
            try parity.runPostgresMutations(a, &handler, &tables, &source.records, reference, &.{entry.id});
        }
        var postimage = try database.scan(scratch, "", "", .{ .include_documents = true, .limit = 4097 });
        defer postimage.deinit(scratch);
        const versions = try scratch.alloc(u64, postimage.documents.len);
        const digests = try scratch.alloc([32]u8, postimage.documents.len);
        for (postimage.documents, versions, digests) |document, *version, *digest| {
            var stored = (try database.lookup(scratch, document.id, .{ .include_primary_digest = true })) orelse return error.MissingArrayPostimage;
            defer stored.deinit(scratch);
            version.* = stored.version orelse return error.MissingArrayVersion;
            digest.* = stored.expected_content_digest orelse return error.MissingArrayDigest;
        }
        database.close();
        opened = false;
        var cold = try db_mod.DB.open(a, directory.path(), options);
        defer cold.close();
        const final = entry.final_tables.?.map.get("usage_records") orelse return error.MissingArrayFinalTable;
        try parity.expectPostgresMutationStorage(a, &cold, final);
        for (postimage.documents, versions, digests) |document, version, digest| {
            var stored = (try cold.lookup(scratch, document.id, .{ .include_primary_digest = true })) orelse return error.MissingColdArray;
            defer stored.deinit(scratch);
            try std.testing.expectEqual(version, stored.version.?);
            try std.testing.expectEqual(digest, stored.expected_content_digest.?);
            try std.testing.expectEqualStrings(document.json, stored.json);
        }
        completed += 1;
    };
    try std.testing.expectEqual(@as(usize, 11), completed);
}

test "httpx SQL original UUID mutation preserves complete typed postimages and cold identity" {
    const a = std.testing.allocator;
    var golden = try std.json.parseFromSlice(Golden, a, local.sql_parity_fixtures.array_mutation_reference, .{ .ignore_unknown_fields = true });
    defer golden.deinit();
    const contract = golden.value.uuid_contract;
    const profile = golden.value.campaigns[1].reference.profile;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var declaration = profile.schema;
    try std.testing.expect(try local.sql_schema_ddl.apply(scratch, &declaration, .{
        .kind = .table,
        .action = .alter_schema,
        .name = .{ .table = "usage_records" },
        .schema_change = .{ .add_unique = .{ .name = "usage_records_pkey", .columns = profile.primary_key, .primary = true } },
    }));
    const schema = try std.json.Stringify.valueAlloc(scratch, declaration, .{});
    var directory = try local.common_test_directory.TestDirectory.init("sql-uuid-mutation");
    defer directory.cleanup();
    const options: db_mod.OpenOptions = .{ .start_optional_runtimes = false, .start_index_workers = false, .primary_backend = .{ .lsm = .{} }, .identity_namespace = .{ .table_id = 7, .shard_id = 1, .range_id = 1 } };
    var database = try db_mod.DB.open(a, directory.path(), options);
    var opened = true;
    defer if (opened) database.close();
    try database.setSchemaJson(a, schema);
    {
        var ranges = [_]local.common_topology_records.RangeRecord{.{ .table_id = 7, .group_id = 7, .range_id = 1, .start_key = "", .doc_identity_shard_id = 1, .doc_identity_range_id = 1 }};
        var source: Source = .{
            .records = .{.{ .table_id = 7, .name = "usage_records", .schema_json = schema }},
            .reads = .{reads.BoundTableReadSource.init("usage_records", 7, &database, @import("../raft/mod.zig").read_gate.alreadyReadSafeBarrier())},
            .ranges = &ranges,
        };
        var writes_source = @import("antfly_source_root").antfly_sources.table_writes.BoundTableWriteSource.init("usage_records", &database);
        var runtime = try db_mod.background_runtime.BackendRuntimeHandle.init(a, .{ .backend = .io_threaded });
        defer runtime.deinit();
        var server = Server.init(a, .{ .backend_runtime = runtime.ptr() }, .{ .ptr = &source, .vtable = &.{ .status = Source.status, .system_catalog = Source.systemCatalog, .admin_snapshot = Source.snapshot, .free_admin_snapshot = Source.freeSnapshot, .supports_query_definitions = true } }, source.source(), writes_source.source());
        defer server.deinit();
        var handler: Handler = .{ .api_server = &server };
        const writes = try scratch.alloc(local.storage_db_types.TransactionWrite, profile.rows.len);
        for (profile.rows, writes) |seed, *write| write.* = .{ .key = seed.key, .value = try std.json.Stringify.valueAlloc(scratch, seed.value, .{}) };
        try parity.commitMutationFixture(scratch, server.table_reads.?, &source.records, .{ .name = "usage_records", .db = &database }, writes, &.{}, 2001);
        var corpus = try local.sql_parity_fixtures.Corpus.init(a);
        defer corpus.deinit();
        const original = try corpus.get(contract.id);
        var previous: ?[]const u8 = null;
        for (0..4) |_| {
            var response = try parity.execute(scratch, &handler, original);
            defer response.deinit();
            try std.testing.expectEqual(contract.affected, response.value.rows_affected);
            try std.testing.expectEqual(@as(usize, 1), response.value.rows.len);
            try std.testing.expectEqualStrings(contract.columns[0], response.value.columns[0].name);
            try std.testing.expectEqual(@as(u32, 25), contract.column_oids[0]);
            try std.testing.expectEqual(.string, response.value.columns[0].type);
            const generated = response.value.rows[0][0].string;
            try std.testing.expectEqual(@as(usize, 36), generated.len);
            for (generated, 0..) |byte, i| if (i == 8 or i == 13 or i == 18 or i == 23) {
                try std.testing.expectEqual(@as(u8, '-'), byte);
            } else try std.testing.expect(std.ascii.isHex(byte));
            try std.testing.expectEqual(@as(u8, '4'), generated[14]);
            try std.testing.expect(std.mem.indexOfScalar(u8, "89ab", generated[19]) != null);
            if (previous) |prior| try std.testing.expect(!std.mem.eql(u8, prior, generated));
            previous = try scratch.dupe(u8, generated);
            var current = try database.scan(scratch, "", "", .{ .include_documents = true, .limit = 4097 });
            defer current.deinit(scratch);
            try std.testing.expectEqual(profile.rows.len, current.documents.len);
            for (profile.rows) |seed| {
                var stored = (try database.lookup(scratch, seed.key, .{ .include_primary_digest = true })) orelse return error.MissingUUIDPostimage;
                defer stored.deinit(scratch);
                var parsed = try std.json.parseFromSlice(std.json.Value, scratch, stored.json, .{});
                defer parsed.deinit();
                if (std.mem.eql(u8, seed.key, contract.target_key)) {
                    const value = parsed.value.object.getPtr(contract.storage_column) orelse return error.MissingUUIDStorageColumn;
                    try std.testing.expectEqualStrings(generated, value.string);
                    value.* = seed.value.object.get(contract.storage_column).?;
                }
                try std.testing.expect(try parity.rowMatchesWithNulls(scratch, &.{.{ .name = "row", .type = .json }}, &.{parsed.value}, &.{false}, &.{seed.value}, &.{false}));
            }
        }
    }
    var postimage = try database.scan(scratch, "", "", .{ .include_documents = true, .limit = 4097 });
    defer postimage.deinit(scratch);
    const versions = try scratch.alloc(u64, postimage.documents.len);
    const digests = try scratch.alloc([32]u8, postimage.documents.len);
    for (postimage.documents, versions, digests) |document, *version, *digest| {
        var stored = (try database.lookup(scratch, document.id, .{ .include_primary_digest = true })) orelse return error.MissingUUIDPostimage;
        defer stored.deinit(scratch);
        version.* = stored.version.?;
        digest.* = stored.expected_content_digest.?;
    }
    database.close();
    opened = false;
    var cold = try db_mod.DB.open(a, directory.path(), options);
    defer cold.close();
    for (postimage.documents, versions, digests) |document, version, digest| {
        var stored = (try cold.lookup(scratch, document.id, .{ .include_primary_digest = true })) orelse return error.MissingColdUUID;
        defer stored.deinit(scratch);
        try std.testing.expectEqual(version, stored.version.?);
        try std.testing.expectEqual(digest, stored.expected_content_digest.?);
        try std.testing.expectEqualStrings(document.json, stored.json);
    }
}
