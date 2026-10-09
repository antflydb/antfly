// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Connection-scoped SQL transactions. Statements read a pinned native view
//! plus their own staged postimages. COMMIT prepares native intents and uses
//! the durable database coordinator; SQL is never replayed to commit a session.
const h = @import("handles.zig");
const api = @import("db.zig");
const sql = @import("sql.zig");
const std = h.std;
const d = h.antfly.capi_dependencies;
const catalog = d.sql_catalog;
const types = h.db_mod.types;
const Entry = struct { table: catalog.Table, mutation: catalog.Mutation };
const Savepoint = struct { name: []const u8, length: usize };
pub const Session = struct {
    handle: *h.Handle,
    budget: d.sql_memory_budget,
    arena: std.heap.ArenaAllocator,
    entries: std.ArrayList(Entry) = .empty,
    savepoints: std.ArrayList(Savepoint) = .empty,
    active: bool = false,
    failed: bool = false,
    read_only: bool = false,
    bytes: usize = 0,

    pub fn reset(self: *Session) void {
        self.arena.deinit();
        self.budget = .{ .backing = self.handle.alloc, .limit = 64 * 1024 * 1024 };
        self.arena = std.heap.ArenaAllocator.init(self.budget.allocator());
        self.entries = .empty;
        self.savepoints = .empty;
        self.active = false;
        self.failed = false;
        self.read_only = false;
        self.bytes = 0;
    }

    pub fn stage(self: *Session, table: catalog.Table, mutations: []const catalog.Mutation) !catalog.MutationOutcome {
        if (!self.active) return error.SqlTransactionNotActive;
        if (self.failed) return error.SqlTransactionAborted;
        if (self.read_only) return error.SqlReadOnlyTransaction;
        const before = self.entries.items.len;
        errdefer self.entries.items.len = before;
        const a = self.arena.allocator();
        for (mutations) |mutation| {
            const bytes = if (mutation.row) |row| try std.json.Stringify.valueAlloc(a, row, .{}) else "";
            self.bytes +|= bytes.len +| mutation.key.len +| 256;
            if (self.bytes > 64 * 1024 * 1024 or self.entries.items.len >= 4096) return error.SqlProgramLimitExceeded;
            var copy = mutation;
            copy.key = try a.dupe(u8, mutation.key);
            copy.row = if (mutation.row != null) try std.json.parseFromSliceLeaky(std.json.Value, a, bytes, .{ .parse_numbers = false }) else null;
            copy.json_null_fields = try a.alloc([]const u8, mutation.json_null_fields.len);
            for (mutation.json_null_fields, @constCast(copy.json_null_fields)) |field, *out| out.* = try a.dupe(u8, field);
            copy.conflict_guard = null;
            copy.previous = null;
            var saved_table = table;
            saved_table.physical_name = try a.dupe(u8, table.physical_name);
            saved_table.columns = &.{};
            try self.entries.append(a, .{ .table = saved_table, .mutation = copy });
        }
        try self.validateStaged(table, before);
        return .committed;
    }

    // Enforce immediate integrity against the transaction's complete postimage
    // and retain cascades as session writes, so subsequent SELECTs see them.
    fn validateStaged(self: *Session, target: catalog.Table, statement_start: usize) !void {
        var arena = std.heap.ArenaAllocator.init(self.handle.alloc);
        defer arena.deinit();
        const a = arena.allocator();
        var adapter = sql.Adapter(h.antfly){ .handle = self.handle, .db = try @import("tables.zig").get(self.handle, target.physical_name), .table_name = target.physical_name };
        const metadata = try adapter.localCatalog(a, target.schema_version);
        var coordinated = false;
        for (metadata.table) |table| if (try d.api_relational_integrity_commit.requiresCoordination(a, table.schema_json)) {
            coordinated = true;
            break;
        };
        if (!coordinated) return;
        const requests = try self.commitRequests(a);
        const previous = try requestsForEntries(a, self.entries.items[0..statement_start]);
        const statement = try requestsForEntries(a, self.entries.items[statement_start..]);
        var prepared = try d.api_relational_integrity_commit.prepareSessionStatement(self.handle.alloc, adapter.localSource(), metadata.table, metadata.range, previous, statement, .{});
        defer prepared.deinit();
        const owned = self.arena.allocator();
        for (prepared.tables) |request| {
            const resolved = try adapter.backend().vtable.resolve(adapter.backend().ptr, a, .{ .table = request.table_name }, .write);
            var saved_table = resolved;
            saved_table.physical_name = try owned.dupe(u8, request.table_name);
            saved_table.columns = &.{};
            for (request.writes) |write| {
                var original: ?[]const u8 = null;
                for (requests) |input| if (std.mem.eql(u8, input.table_name, request.table_name)) {
                    for (input.writes) |prior| if (std.mem.eql(u8, prior.key, write.key)) {
                        original = prior.value;
                        break;
                    };
                };
                if (original) |bytes| if (std.mem.eql(u8, bytes, write.value)) continue;
                var mutation: catalog.Mutation = .{ .expected_version = 0, .key = try owned.dupe(u8, write.key), .row = try std.json.parseFromSliceLeaky(std.json.Value, owned, write.value, .{ .parse_numbers = false }) };
                mutation.json_null_fields = try owned.alloc([]const u8, write.json_null_fields.len);
                for (write.json_null_fields, @constCast(mutation.json_null_fields)) |field, *out| out.* = try owned.dupe(u8, field);
                for (request.predicates) |predicate| if (std.mem.eql(u8, predicate.key, write.key)) {
                    mutation.expected_version = predicate.expected_version;
                    mutation.expected_content_digest = predicate.expected_content_digest;
                    mutation.unique_absence = predicate.unique_absence;
                    break;
                };
                if (self.entries.items.len >= 4096) return error.SqlProgramLimitExceeded;
                try self.entries.append(owned, .{ .table = saved_table, .mutation = mutation });
            }
            for (request.deletes) |key| {
                const already = outer: for (requests) |input| {
                    if (!std.mem.eql(u8, input.table_name, request.table_name)) continue;
                    for (input.deletes) |prior| if (std.mem.eql(u8, prior, key)) break :outer true;
                } else false;
                if (already) continue;
                var mutation: catalog.Mutation = .{ .expected_version = 0, .key = try owned.dupe(u8, key), .row = null };
                for (request.predicates) |predicate| if (std.mem.eql(u8, predicate.key, key)) {
                    mutation.expected_version = predicate.expected_version;
                    mutation.expected_content_digest = predicate.expected_content_digest;
                    mutation.unique_absence = predicate.unique_absence;
                    break;
                };
                if (self.entries.items.len >= 4096) return error.SqlProgramLimitExceeded;
                try self.entries.append(owned, .{ .table = saved_table, .mutation = mutation });
            }
        }
    }

    pub fn merged(self: *Session, alloc: std.mem.Allocator) ![]Entry {
        return mergeEntries(alloc, self.entries.items);
    }

    fn mergeEntries(alloc: std.mem.Allocator, entries: []const Entry) ![]Entry {
        return @import("../sql/mutation_fold.zig").merge(Entry, alloc, entries);
    }

    pub fn commitRequests(self: *Session, a: std.mem.Allocator) ![]d.api_distributed_txn_contract.TableCommitRequest {
        return requestsForEntries(a, self.entries.items);
    }

    fn requestsForEntries(a: std.mem.Allocator, input: []const Entry) ![]d.api_distributed_txn_contract.TableCommitRequest {
        const entries = try mergeEntries(a, input);
        defer a.free(entries);
        const Group = struct {
            table: catalog.Table,
            writes: std.ArrayList(types.TransactionWrite) = .empty,
            deletes: std.ArrayList([]const u8) = .empty,
            predicates: std.ArrayList(types.TransactionVersionPredicate) = .empty,
        };
        var names: std.StringHashMapUnmanaged(usize) = .empty;
        defer names.deinit(a);
        var groups: std.ArrayList(Group) = .empty;
        defer groups.deinit(a);
        errdefer for (groups.items) |*group| {
            for (group.writes.items) |write| a.free(write.value);
            group.writes.deinit(a);
            group.deletes.deinit(a);
            group.predicates.deinit(a);
        };
        // One visit per folded row; never rescan every row for every table.
        // First-seen table order also makes coordinator requests deterministic.
        for (entries) |entry| {
            const name = try names.getOrPut(a, entry.table.physical_name);
            if (!name.found_existing) {
                name.value_ptr.* = groups.items.len;
                try groups.append(a, .{ .table = entry.table });
            }
            const group = &groups.items[name.value_ptr.*];
            const m = entry.mutation;
            try group.predicates.append(a, .{ .key = m.key, .expected_version = m.expected_version, .expected_content_digest = m.expected_content_digest, .unique_absence = m.unique_absence });
            if (m.predicate_only) continue;
            if (m.row) |row| {
                const json = try std.json.Stringify.valueAlloc(a, row, .{});
                errdefer a.free(json);
                try group.writes.append(a, .{ .key = m.key, .value = json, .json_null_fields = m.json_null_fields });
            } else try group.deletes.append(a, m.key);
        }
        const requests = try a.alloc(d.api_distributed_txn_contract.TableCommitRequest, groups.items.len);
        errdefer a.free(requests);
        var transferred: usize = 0;
        errdefer for (requests[0..transferred]) |request| {
            for (request.writes) |write| a.free(write.value);
            a.free(request.writes);
            a.free(request.deletes);
            a.free(request.predicates);
        };
        for (groups.items, requests) |*group, *request| {
            const table = group.table;
            request.* = .{ .table_name = table.physical_name, .schema_version = table.schema_version, .relational_schema_version = if (table.storage_mode == .relational) table.schema_version else null };
            transferred += 1;
            request.writes = try group.writes.toOwnedSlice(a);
            request.deletes = try group.deletes.toOwnedSlice(a);
            request.predicates = try group.predicates.toOwnedSlice(a);
        }
        return requests;
    }

    pub fn commit(self: *Session, out_id: *?types.TxnId) !catalog.MutationOutcome {
        if (!self.active) return .committed;
        if (self.failed) {
            return error.SqlTransactionAborted;
        }
        var arena = std.heap.ArenaAllocator.init(self.handle.alloc);
        defer arena.deinit();
        const a = arena.allocator();
        const requests = try self.commitRequests(a);
        if (requests.len == 0) {
            self.reset();
            return .committed;
        }
        var adapter = sql.Adapter(h.antfly){ .handle = self.handle, .db = try @import("tables.zig").get(self.handle, requests[0].table_name), .table_name = requests[0].table_name };
        const metadata = try adapter.localCatalog(a, requests[0].schema_version.?);
        var prepared = try d.api_relational_integrity_commit.prepareWithCoverage(self.handle.alloc, adapter.localSource(), metadata.table, metadata.range, requests);
        defer prepared.deinit();
        const outcome = @import("sql_commit.zig").commit(self.handle, prepared.tables, out_id) catch |err| {
            self.failed = true;
            return err;
        };
        self.reset();
        return outcome;
    }

    pub fn control(self: *Session, statement: d.sql_ast.Statement, out_id: *?types.TxnId) !?sql.runtime.Result {
        if (self.handle.sql_decision_uncertain) return error.SqlMutationOutcomeUnknown;
        const tag: []const u8 = switch (statement) {
            .begin => |options| blk: {
                if (self.active) return error.SqlTransactionAlreadyActive;
                if (options.isolation != .read_committed) return error.UnsupportedSqlExecution;
                self.active = true;
                self.read_only = options.mode == .read_only;
                break :blk "BEGIN";
            },
            .commit => {
                var result = try sql.runtime.Result.empty(self.handle.alloc, "COMMIT");
                errdefer result.deinit();
                const outcome = try self.commit(out_id);
                result.output.mutation_outcome = outcome;
                return result;
            },
            .rollback => blk: {
                self.reset();
                break :blk "ROLLBACK";
            },
            .savepoint => |name| blk: {
                if (!self.active) return error.SqlTransactionNotActive;
                if (self.failed) return error.SqlTransactionAborted;
                if (self.savepoints.items.len >= 64) return error.SavepointLimitExceeded;
                try self.savepoints.append(self.arena.allocator(), .{ .name = try self.arena.allocator().dupe(u8, name), .length = self.entries.items.len });
                break :blk "SAVEPOINT";
            },
            .rollback_to_savepoint, .release_savepoint => |name| blk: {
                if (!self.active) return error.SqlTransactionNotActive;
                var i = self.savepoints.items.len;
                while (i > 0) {
                    i -= 1;
                    if (std.mem.eql(u8, name, self.savepoints.items[i].name)) break;
                }
                if (self.savepoints.items.len == 0 or !std.mem.eql(u8, name, self.savepoints.items[i].name)) return error.InvalidSavepointName;
                if (statement == .rollback_to_savepoint) {
                    self.entries.items.len = self.savepoints.items[i].length;
                    self.savepoints.items.len = i + 1;
                    self.failed = false;
                    break :blk "ROLLBACK";
                }
                if (self.failed) return error.SqlTransactionAborted;
                self.savepoints.items.len = i;
                break :blk "RELEASE";
            },
            else => return null,
        };
        return try sql.runtime.Result.empty(self.handle.alloc, tag);
    }
};

test "capi SQL session fold groups requests in first-table order and owns every allocation fault" {
    const a = std.testing.allocator;
    const first: catalog.Table = .{ .id = 51, .physical_name = "first", .schema_version = 2, .columns = &.{} };
    const second: catalog.Table = .{ .id = 52, .physical_name = "second", .schema_version = 3, .columns = &.{}, .storage_mode = .document };
    const entries: []const Entry = &.{
        .{ .table = first, .mutation = .{ .key = "same", .expected_version = 7, .expected_content_digest = @splat(3), .row = .{ .integer = 1 } } },
        .{ .table = second, .mutation = .{ .key = "same", .expected_version = 9, .row = .{ .integer = 10 } } },
        .{ .table = first, .mutation = .{ .key = "same", .expected_version = 99, .row = .{ .integer = 2 }, .json_null_fields = &.{"j"} } },
        .{ .table = second, .mutation = .{ .key = "gone", .expected_version = 11, .row = null } },
        .{ .table = first, .mutation = .{ .key = "read", .expected_version = 12, .row = null, .predicate_only = true } },
    };
    const Fault = struct {
        fn run(alloc: std.mem.Allocator, input: []const Entry) !void {
            const requests = try Session.requestsForEntries(alloc, input);
            defer alloc.free(requests);
            defer for (requests) |request| {
                for (request.writes) |write| alloc.free(write.value);
                alloc.free(request.writes);
                alloc.free(request.deletes);
                alloc.free(request.predicates);
            };
            try std.testing.expectEqual(@as(usize, 2), requests.len);
            try std.testing.expectEqualStrings("first", requests[0].table_name);
            try std.testing.expectEqual(@as(?u32, 2), requests[0].relational_schema_version);
            try std.testing.expectEqual(@as(usize, 1), requests[0].writes.len);
            try std.testing.expectEqualStrings("2", requests[0].writes[0].value);
            try std.testing.expectEqualStrings("j", requests[0].writes[0].json_null_fields[0]);
            try std.testing.expectEqual(@as(usize, 2), requests[0].predicates.len);
            try std.testing.expectEqual(@as(u64, 7), requests[0].predicates[0].expected_version);
            try std.testing.expectEqual(@as(?[32]u8, @splat(3)), requests[0].predicates[0].expected_content_digest);
            try std.testing.expectEqualStrings("read", requests[0].predicates[1].key);
            try std.testing.expectEqualStrings("second", requests[1].table_name);
            try std.testing.expect(requests[1].relational_schema_version == null);
            try std.testing.expectEqualStrings("10", requests[1].writes[0].value);
            try std.testing.expectEqualStrings("gone", requests[1].deletes[0]);
            try std.testing.expectEqual(@as(usize, 2), requests[1].predicates.len);
        }
    };
    try Fault.run(a, entries);
    var no_resize = std.testing.FailingAllocator.init(a, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(no_resize.allocator(), Fault.run, .{entries});
}

pub fn closeAll(handle: *h.Handle) void {
    var iterator = handle.sql_sessions.valueIterator();
    while (iterator.next()) |session| {
        session.*.arena.deinit();
        handle.alloc.destroy(session.*);
    }
    handle.sql_sessions.deinit(handle.alloc);
}
pub export fn antfly_db_sql_session_open(ptr: ?*anyopaque, out: *u64) h.capi.ErrorCode {
    out.* = 0;
    const guard = api.enterHandle(ptr, .exclusive) orelse return .invalid_argument;
    defer guard.leave();
    const handle = guard.handle;
    if (handle.parent_id != null) return .invalid_argument;
    if (handle.sql_sessions.count() >= 64) return .busy;
    const session = handle.alloc.create(Session) catch return .internal;
    session.* = .{ .handle = handle, .budget = .{ .backing = handle.alloc, .limit = 64 * 1024 * 1024 }, .arena = undefined };
    session.arena = std.heap.ArenaAllocator.init(session.budget.allocator());
    const id = handle.next_sql_session_id;
    handle.next_sql_session_id = std.math.add(u64, id, 1) catch {
        handle.alloc.destroy(session);
        return .internal;
    };
    handle.sql_sessions.putNoClobber(handle.alloc, id, session) catch {
        session.arena.deinit();
        handle.alloc.destroy(session);
        return .internal;
    };
    out.* = id;
    return .ok;
}
pub export fn antfly_db_sql_session_close(ptr: ?*anyopaque, id: u64) h.capi.ErrorCode {
    const guard = api.enterHandle(ptr, .exclusive) orelse return .invalid_argument;
    defer guard.leave();
    const handle = guard.handle;
    if (handle.parent_id != null) return .invalid_argument;
    const entry = handle.sql_sessions.fetchRemove(id) orelse return .invalid_argument;
    @import("sql_cursor.zig").closeSession(handle, id);
    entry.value.arena.deinit();
    handle.alloc.destroy(entry.value);
    return .ok;
}
