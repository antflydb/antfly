// Copyright 2026 Antfly, Inc.
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

//! TTL is a conditional delete, not a second integrity implementation. A
//! private native observation enters the normal FK planner and durable 2PC.
//! RESTRICT leaves expired rows visible; each root is independent so a blocked
//! parent does not starve unrelated expiration in the same scan page.
const std = @import("std");
const expiry = @import("../storage/coordinated_ttl.zig");
const catalog = @import("table_catalog.zig");
const reads = @import("table_read_source.zig");
const writes = @import("table_write_source.zig");
const integrity = @import("relational_integrity_commit.zig");
const operation = @import("operation.zig");
const schema = @import("../schema/mod.zig");
const topology = @import("../common/topology_records.zig");
const contract = @import("distributed_txn_contract.zig");
const types = @import("../storage/db/types.zig");

const CommitCancellation = struct {
    request: operation.RequestContext,
    fn canceled(ptr: *const anyopaque) bool {
        const self: *const CommitCancellation = @ptrCast(@alignCast(ptr));
        self.request.ensureActive() catch return true;
        return false;
    }
};

/// This capability is private to maintenance. Its scope is the transitive
/// inbound FK graph of one table incarnation, never all authenticated tables.
const Authorization = struct {
    allowed: std.StringHashMapUnmanaged(void) = .empty,

    fn allows(ptr: *const anyopaque, name: []const u8) bool {
        const self: *const Authorization = @ptrCast(@alignCast(ptr));
        return self.allowed.contains(name);
    }

    fn build(alloc: std.mem.Allocator, tables: []const topology.TableRecord, root: []const u8, control: operation.RequestContext) !Authorization {
        var children = std.StringHashMapUnmanaged(std.ArrayList([]const u8)).empty;
        for (tables) |table| {
            try control.ensureActive();
            if (table.schema_json.len == 0) continue;
            var parsed = try schema.parseValidatedTableSchema(alloc, table.schema_json);
            defer parsed.deinit(alloc);
            const declarations = parsed.foreign_keys orelse continue;
            for (declarations.value) |foreign| {
                const entry = try children.getOrPut(alloc, foreign.parent_table);
                if (!entry.found_existing) {
                    entry.key_ptr.* = try alloc.dupe(u8, foreign.parent_table);
                    entry.value_ptr.* = .empty;
                }
                try entry.value_ptr.append(alloc, table.name);
            }
        }
        var result: Authorization = .{};
        var queue = std.ArrayList([]const u8).empty;
        try result.allowed.put(alloc, root, {});
        try queue.append(alloc, root);
        var cursor: usize = 0;
        while (cursor < queue.items.len) : (cursor += 1) {
            try control.ensureActive();
            if (children.get(queue.items[cursor])) |list| for (list.items) |child| {
                if ((try result.allowed.getOrPut(alloc, child)).found_existing) continue;
                try queue.append(alloc, child);
            };
        }
        return result;
    }
};

pub fn expire(alloc: std.mem.Allocator, source: catalog.CatalogSource, reader: reads.TableReadSource, writer: writes.TableWriteSource, request: expiry.Request, cancellation: operation.CancellationToken) !u32 {
    const result = try expireStep(alloc, source, reader, writer, request, cancellation, .{ .max_candidates = 128 });
    if (result.last_error) |err| return err;
    return result.expired;
}

pub const StepOptions = struct {
    next_candidate: usize = 0,
    max_candidates: usize = 16,
    budget_ns: u64 = 5 * std.time.ns_per_s,
};
pub const StepResult = struct {
    expired: u32 = 0,
    next_candidate: usize,
    failed: u32 = 0,
    last_error: ?anyerror = null,

    fn failedAttempt(self: *StepResult, err: anyerror) void {
        self.failed += 1;
        self.last_error = err;
    }
};

/// A step resumes an owned observation page, not a new scan. An attempted root
/// advances even when its closure times out or exceeds admission limits: that
/// root stays visible and is retried on the next native sweep, while later roots
/// in this page cannot be perpetually starved by it. Durable 2PC resolves any
/// uncertain commit independently; physical guards make future retries safe.
pub fn expireStep(alloc: std.mem.Allocator, source: catalog.CatalogSource, reader: reads.TableReadSource, writer: writes.TableWriteSource, request: expiry.Request, cancellation: operation.CancellationToken, options: StepOptions) !StepResult {
    var result: StepResult = .{ .next_candidate = options.next_candidate };
    if (options.next_candidate > request.candidates.len or options.max_candidates == 0 or options.budget_ns == 0) return error.InvalidTtlObservation;
    if (options.next_candidate == request.candidates.len) return result;
    if (request.candidates.len > 128 or request.ttl_duration_ns == 0 or request.table_id == 0 or request.group_id == 0)
        return error.InvalidTtlObservation;
    const budget = source.budget(null);
    const control: operation.RequestContext = .{
        .cancellation = cancellation,
        .principal = .{ .kind = .internal, .subject = "relational-ttl" },
        .deadline_ns = budget.nowNs() +| options.budget_ns,
        .deadline_io = source.io,
    };
    try control.ensureActive();
    var snapshot = try (try source.routingSource()).linearizableSnapshot(control.deadline_ns);
    defer snapshot.deinit();
    const table = for (snapshot.value.tables) |record| {
        if (record.table_id == request.table_id) break record;
    } else return error.TableNotFound;
    if (table.restore_backup_id.len != 0 or table.relational_retirement_json.len != 0)
        return error.PreparedGenerationChanged;
    const range = for (snapshot.value.ranges) |record| {
        if (record.table_id == table.table_id and record.group_id == request.group_id) break record;
    } else return error.KeyOutOfRange;
    var declaration = try schema.parseValidatedTableSchema(alloc, table.schema_json);
    defer declaration.deinit(alloc);
    if (declaration.version != request.schema_version or declaration.ttl_duration_ns != request.ttl_duration_ns or
        !std.mem.eql(u8, declaration.ttl_field, request.ttl_field)) return error.PreparedGenerationChanged;
    const initial: contract.TableCommitRequest = .{ .table_name = table.name, .relational_schema_version = request.schema_version };
    if (!try integrity.metadataRequiresCoordination(alloc, snapshot.value.tables, &.{initial})) return error.InvalidTtlObservation;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const authorization = try Authorization.build(arena.allocator(), snapshot.value.tables, table.name, control);
    var authorized = control;
    authorized.table_write_authorization = .{ .ptr = &authorization, .allows = Authorization.allows };
    const commit_control: CommitCancellation = .{ .request = control };
    const commit_cancellation: operation.CancellationToken = .{ .ptr = &commit_control, .is_cancelled_fn = CommitCancellation.canceled };
    const page_end = @min(request.candidates.len, options.next_candidate +| options.max_candidates);
    for (request.candidates[options.next_candidate..page_end], options.next_candidate..) |candidate, index| {
        control.ensureActive() catch |err| {
            if (cancellation.isCancelled()) return err;
            return result;
        };
        result.next_candidate = index + 1;
        if (candidate.row_version == 0 or candidate.row_version != candidate.ttl_timestamp_ns or
            std.mem.order(u8, candidate.key, range.start_key) == .lt or
            (if (range.end_key) |end| end.len != 0 and std.mem.order(u8, candidate.key, end) != .lt else false))
            return error.InvalidTtlObservation;
        const age = std.math.add(u64, request.ttl_duration_ns, request.grace_period_ns) catch continue;
        const eligible_at = std.math.add(u64, candidate.ttl_timestamp_ns, age) catch continue;
        if (eligible_at >= request.observed_at_unix_ns) continue;
        const predicate: types.TransactionVersionPredicate = .{
            .key = candidate.key,
            .expected_version = candidate.row_version,
            .expected_content_digest = candidate.expected_content_digest,
        };
        var deletion = initial;
        deletion.predicates = &.{predicate};
        deletion.deletes = &.{candidate.key};
        var prepared = integrity.prepareWithCoverageControlled(alloc, reader, snapshot.value.tables, snapshot.value.ranges, &.{deletion}, authorized) catch |err| switch (err) {
            error.ForeignKeyReferenced, error.VersionConflict, error.PreparedReadSetChanged => continue,
            else => {
                result.failedAttempt(err);
                return result;
            },
        };
        defer prepared.deinit();
        authorized.ensureActive() catch |err| {
            result.failedAttempt(err);
            return result;
        };
        try integrity.authorizePrimaryMutations(authorized, true, prepared.tables);
        const outcome = (writer.commitBatchWithCancellation(alloc, prepared.tables, .write, commit_cancellation) catch |err| switch (err) {
            error.ForeignKeyReferenced, error.VersionConflict, error.PreparedReadSetChanged => continue,
            else => {
                result.failedAttempt(err);
                return result;
            },
        }) orelse return error.UnsupportedOperation;
        switch (outcome) {
            .committed => result.expired += 1,
            .conflict => {},
        }
    }
    return result;
}

test "distributed txn ttl shares cascade restrict and set null semantics" {
    const db_mod = @import("antfly_source_root").antfly_sources.physical_db;
    const metadata = @import("../metadata/api.zig");
    const alloc = std.testing.allocator;
    for ([_][]const u8{ "cascade", "restrict", "set_null" }) |action| for ([_]bool{ false, true }) |inject_refresh| for ([_]bool{ false, true }) |inject_timeout| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/ttl", .{tmp.sub_path});
        defer alloc.free(path);
        var db = try db_mod.DB.open(alloc, path, .{ .start_optional_runtimes = false, .start_index_workers = false, .identity_namespace = .{ .table_id = 500, .shard_id = 501 }, .primary_backend = .{ .lsm = .{} } });
        defer db.close();
        const template =
            \\{"version":1,"storage_mode":"relational","default_type":"row","ttl":{"duration":"1ns","field":"expires"},"unique_constraints":[{"name":"pk","columns":["id"]}],"foreign_keys":[{"name":"fk","child_columns":["parent"],"parent_table":"rows","parent_columns":["id"],"on_delete":"ACTION"}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"parent":{"type":"integer","nullable":true},"expires":{"type":"datetime"},"note":{"type":"keyword","nullable":true}},"additionalProperties":false}}}}
        ;
        const declaration = try std.mem.replaceOwned(u8, alloc, template, "ACTION", action);
        defer alloc.free(declaration);
        try db.setSchemaJson(alloc, declaration);
        const Fixture = struct {
            db: *db_mod.DB,
            tables: [1]topology.TableRecord,
            ranges: [1]topology.RangeRecord = .{.{ .table_id = 500, .group_id = 501, .start_key = "" }},
            next_txn: u8 = 1,
            inject_refresh: bool = false,
            inject_timeout: bool = false,
            fn admin(_: *anyopaque) !metadata.AdminSnapshot {
                return error.UnexpectedCall;
            }
            fn freeAdmin(_: *anyopaque, _: *metadata.AdminSnapshot) void {
                unreachable;
            }
            fn routing(ptr: *anyopaque, _: ?u64) !metadata.CatalogRoutingSnapshot {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                return .{ .tables = &self.tables, .ranges = &self.ranges };
            }
            fn freeRouting(_: *anyopaque, _: *metadata.CatalogRoutingSnapshot) void {}
            fn batch(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: types.BatchRequest) !?void {
                return error.UnexpectedCall;
            }
            fn commit(ptr: *anyopaque, allocator: std.mem.Allocator, requests: []const contract.TableCommitRequest, _: types.SyncLevel) anyerror!?contract.CommitOutcome {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                try std.testing.expectEqual(@as(usize, 1), requests.len);
                const request = requests[0];
                if (self.inject_timeout) for (request.deletes) |key| {
                    if (!std.mem.eql(u8, key, "p")) continue;
                    self.inject_timeout = false;
                    return error.DeadlineExceeded;
                };
                if (self.inject_refresh) for (request.deletes) |key| {
                    if (!std.mem.eql(u8, key, "p")) continue;
                    self.inject_refresh = false;
                    var reader = @import("antfly_source_root").antfly_sources.table_reads.BoundTableReadSource.init("rows", 501, self.db, @import("../raft/read_gate.zig").alreadyReadSafeBarrier());
                    var refreshed = try integrity.prepareWithCoverage(allocator, reader.source(), &self.tables, &self.ranges, &.{.{ .table_name = "rows", .writes = &.{.{
                        .key = "c",
                        .value = "{\"id\":2,\"parent\":1,\"expires\":\"1970-01-01T00:00:00.000000001Z\",\"note\":\"fresh\"}",
                    }} }});
                    defer refreshed.deinit();
                    _ = try commit(self, allocator, refreshed.tables, .write);
                    break;
                };
                const id = self.next_txn;
                self.next_txn += 1;
                const txn = try self.db.beginTransactionWithId(@splat(id), @as(u64, id) * 100);
                errdefer self.db.abortTransaction(txn, @as(u64, id) * 100 + 1) catch {};
                try self.db.writeTransaction(txn, .{
                    .relational_schema_version = request.relational_schema_version,
                    .relational_integrity_generation_set = request.relational_integrity_generation_set,
                    .writes = request.writes,
                    .deletes = request.deletes,
                    .predicates = request.predicates,
                    .integrity_commands = request.integrity_commands,
                });
                try self.db.commitTransaction(txn, @as(u64, id) * 100 + 1);
                return .{ .committed = .{ .participant_count = 1 } };
            }
            fn candidate(self: *@This(), key: []const u8) !expiry.Candidate {
                const physical = try @import("../storage/db/relational_store.zig").keyAlloc(std.testing.allocator, key);
                defer std.testing.allocator.free(physical);
                var read = try self.db.core.store.beginReadTxn();
                defer read.abort();
                var digest: [32]u8 = undefined;
                std.crypto.hash.sha2.Sha256.hash(try read.get(physical), &digest, .{});
                return .{ .key = key, .row_version = 1, .ttl_timestamp_ns = 1, .expected_content_digest = digest };
            }
        };
        var fixture: Fixture = .{ .db = &db, .tables = .{.{ .table_id = 500, .name = "rows", .schema_json = declaration }} };
        const source: catalog.CatalogSource = .{ .ptr = &fixture, .vtable = &.{ .admin_snapshot = Fixture.admin, .free_admin_snapshot = Fixture.freeAdmin, .routing_snapshot = Fixture.routing, .linearizable_routing_snapshot = Fixture.routing, .free_routing_snapshot = Fixture.freeRouting } };
        var bound = @import("antfly_source_root").antfly_sources.table_reads.BoundTableReadSource.init("rows", 501, &db, @import("../raft/read_gate.zig").alreadyReadSafeBarrier());
        const writer: writes.TableWriteSource = .{ .ptr = &fixture, .vtable = &.{ .batch = Fixture.batch, .commit_batch = Fixture.commit } };
        var inserted = try integrity.prepareWithCoverage(alloc, bound.source(), &fixture.tables, &fixture.ranges, &.{.{ .table_name = "rows", .writes = &.{
            .{ .key = "p", .value = "{\"id\":1,\"parent\":null,\"expires\":\"1970-01-01T00:00:00.000000001Z\"}" },
            .{ .key = "c", .value = "{\"id\":2,\"parent\":1,\"expires\":\"1970-01-01T00:00:00.000000001Z\"}" },
            .{ .key = "free", .value = "{\"id\":3,\"parent\":null,\"expires\":\"1970-01-01T00:00:00.000000001Z\"}" },
        } }});
        defer inserted.deinit();
        _ = try Fixture.commit(&fixture, alloc, inserted.tables, .write);
        const candidates = [_]expiry.Candidate{ try fixture.candidate("p"), try fixture.candidate("free") };
        fixture.inject_refresh = inject_refresh;
        fixture.inject_timeout = inject_timeout;
        const request: expiry.Request = .{ .table_id = 500, .group_id = 501, .schema_version = 1, .ttl_duration_ns = 1, .ttl_field = "expires", .observed_at_unix_ns = 100, .grace_period_ns = 0, .candidates = &candidates };
        var stale = request;
        stale.schema_version = 2;
        try std.testing.expectError(error.PreparedGenerationChanged, expire(alloc, source, bound.source(), writer, stale, .none));
        const count = if (inject_timeout) blk: {
            const first = try expireStep(alloc, source, bound.source(), writer, request, .none, .{ .max_candidates = 1 });
            try std.testing.expectEqual(@as(usize, 1), first.next_candidate);
            try std.testing.expectEqual(@as(u32, 0), first.expired);
            // RESTRICT may reject before the injected transport timeout; both
            // are completed attempts, so the successor still reaches "free".
            if (first.last_error) |err| try std.testing.expectEqual(error.DeadlineExceeded, err);
            const second = try expireStep(alloc, source, bound.source(), writer, request, .none, .{ .next_candidate = first.next_candidate, .max_candidates = 1 });
            try std.testing.expectEqual(@as(usize, 2), second.next_candidate);
            const done = try expireStep(alloc, source, bound.source(), writer, request, .none, .{ .next_candidate = second.next_candidate });
            try std.testing.expectEqual(@as(u32, 0), done.expired);
            break :blk first.expired + second.expired;
        } else try expire(alloc, source, bound.source(), writer, request, .none);
        const restricted = std.mem.eql(u8, action, "restrict");
        try std.testing.expectEqual(@as(u32, if (restricted or inject_refresh or inject_timeout) 1 else 2), count);
        var parent = try db.lookup(alloc, "p", .{});
        defer if (parent) |*row| row.deinit(alloc);
        try std.testing.expectEqual(restricted or inject_refresh or inject_timeout, parent != null);
        var child = try db.lookup(alloc, "c", .{});
        defer if (child) |*row| row.deinit(alloc);
        try std.testing.expectEqual(inject_refresh or inject_timeout or !std.mem.eql(u8, action, "cascade"), child != null);
        if (inject_refresh and !inject_timeout) {
            var parsed = try std.json.parseFromSlice(std.json.Value, alloc, child.?.json, .{});
            defer parsed.deinit();
            try std.testing.expectEqualStrings("fresh", parsed.value.object.get("note").?.string);
        } else if (!inject_timeout and std.mem.eql(u8, action, "set_null")) {
            var parsed = try std.json.parseFromSlice(std.json.Value, alloc, child.?.json, .{});
            defer parsed.deinit();
            try std.testing.expect(parsed.value.object.get("parent").? == .null);
        }
        var free = try db.lookup(alloc, "free", .{});
        defer if (free) |*row| row.deinit(alloc);
        try std.testing.expect(free == null);
    };
}
