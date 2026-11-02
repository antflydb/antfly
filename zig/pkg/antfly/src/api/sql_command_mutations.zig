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

//! Request-local command journal. Events own prepared bytes once; a flat key
//! directory selects final postimages without repeatedly cloning prior rows.
//! This is not durable authority: the SQL session stages the folded request.
const std = @import("std");
const txn = @import("transactions.zig");
const types = @import("antfly_local_sources").storage_db_types;
const contract = @import("antfly_local_sources").api_distributed_txn_contract;
const A = std.mem.Allocator;

pub const Journal = struct {
    arena: std.heap.ArenaAllocator,
    metadata: txn.OwnedTransactionCommitRequest = .{},
    events: std.ArrayList(txn.OwnedTransactionCommitRequest) = .empty,
    groups: std.StringArrayHashMapUnmanaged(Group) = .empty,
    rows: usize = 0,
    const Group = struct { latest: std.StringArrayHashMapUnmanaged(?types.BatchWrite) = .empty, observed: std.StringHashMapUnmanaged(types.TransactionVersionPredicate) = .empty };

    pub fn init(a: A) Journal {
        return .{ .arena = .init(a) };
    }
    pub fn deinit(self: *Journal) void {
        self.arena.deinit();
    }

    pub fn append(self: *Journal, statement: *const txn.OwnedTransactionCommitRequest) !void {
        const a = self.arena.allocator();
        if (statement.tables.len != 1 or statement.read_set.len != 0 or statement.constraint_timing.items.len != 0) return error.InvalidTransactionCommitRequest;
        const count = statement.tables[0].batch.writes.len +| statement.tables[0].batch.deletes.len;
        if (count > 4096 -| self.rows or self.events.items.len == 4096) return error.TransactionTooLarge;
        var owned = try statement.clone(a);
        const table = &owned.tables[0];
        if (table.batch.transforms.len != 0) return error.UnsupportedSqlExecution;
        const physical = owned.physicalName(table.table_name);
        if (!self.groups.contains(physical) and self.groups.count() == 64) return error.TransactionTooLarge;
        const entry = try self.groups.getOrPut(a, physical);
        if (!entry.found_existing) entry.value_ptr.* = .{};
        const group = entry.value_ptr;
        // Reinserting a retired tuple must fence its original durable version,
        // rather than asking the participant to prove physical absence.
        for (table.predicates.items) |*predicate| {
            if (group.observed.getPtr(predicate.key)) |original| {
                if (predicate.unique_absence and predicate.expected_version == 0) {
                    if (group.latest.get(predicate.key)) |latest| {
                        if (latest != null) return error.UniqueConstraintViolation;
                        predicate.expected_version = original.expected_version;
                        predicate.expected_content_digest = original.expected_content_digest;
                        predicate.unique_absence = original.unique_absence;
                    }
                }
                if (original.expected_version != predicate.expected_version) return error.VersionConflict;
                if (original.expected_content_digest) |digest| {
                    if (predicate.expected_content_digest) |next| if (!std.mem.eql(u8, &digest, &next)) return error.VersionConflict;
                } else original.expected_content_digest = predicate.expected_content_digest;
                original.unique_absence = original.unique_absence or predicate.unique_absence;
                if (predicate.row_policy_requirement != .native) original.row_policy_requirement = predicate.row_policy_requirement;
            } else try group.observed.put(a, predicate.key, predicate.*);
        }
        for (table.batch.writes) |write| try group.latest.put(a, write.key, write);
        for (table.batch.deletes) |key| try group.latest.put(a, key, null);
        // Only schema/auth/guards are merged repeatedly. Prepared row payloads
        // remain in the event arena until the one final validated stage.
        var header = table.*;
        header.batch = .{};
        header.predicates = .empty;
        var headers = [_]txn.TableCommitRequest{header};
        var metadata = owned;
        metadata.tables = &headers;
        try self.metadata.mergeFrom(a, &metadata);
        try self.events.append(a, owned);
        self.rows += count;
    }

    /// Borrowed descriptors. The journal outlives native staging and integrity
    /// preparation; the session copies them only at its durable CAS boundary.
    pub fn folded(self: *Journal, a: A) !txn.OwnedTransactionCommitRequest {
        var result = self.metadata;
        result.tables = try a.dupe(txn.TableCommitRequest, self.metadata.tables);
        for (result.tables) |*table| {
            const group = self.groups.get(result.physicalName(table.table_name)) orelse return error.InvalidTransactionCommitRequest;
            var writes: std.ArrayList(types.BatchWrite) = .empty;
            var deletes: std.ArrayList([]const u8) = .empty;
            for (group.latest.keys(), group.latest.values()) |key, value| {
                if (value) |write| try writes.append(a, write) else try deletes.append(a, key);
            }
            table.batch = .{ .writes = try writes.toOwnedSlice(a), .deletes = try deletes.toOwnedSlice(a) };
            const predicates = try a.alloc(types.TransactionVersionPredicate, group.observed.count());
            var iterator = group.observed.valueIterator();
            for (predicates) |*predicate| predicate.* = iterator.next().?.*;
            table.predicates = .{ .items = predicates, .capacity = predicates.len, .pointer_stability = .{} };
            table.batch.req.writes = table.batch.writes;
            table.batch.req.deletes = table.batch.deletes;
        }
        return result;
    }
    pub fn distributedEvents(self: *Journal, a: A) ![]const contract.TableCommitRequest {
        var result: std.ArrayList(contract.TableCommitRequest) = .empty;
        for (self.events.items) |*event| {
            const view = try event.distributedTables(a);
            try result.appendSlice(a, view.tables);
        }
        return result.toOwnedSlice(a);
    }
};

test "distributed txn SQL command journal folds postimages and preserves retirement fences" {
    const a = std.testing.allocator;
    var journal: Journal = .init(a);
    defer journal.deinit();
    var removed = try txn.parseCommitRequest(a, "{\"read_set\":[],\"tables\":{\"p\":{\"deletes\":[\"x\"]}}}");
    defer removed.deinit(a);
    // Construct native version predicates: public JSON cannot author them.
    const predicates = [_]types.TransactionVersionPredicate{.{ .key = "x", .expected_version = 7 }};
    removed.tables[0].predicates.clearRetainingCapacity();
    try removed.tables[0].predicates.append(a, .{ .key = try a.dupe(u8, predicates[0].key), .expected_version = 7 });
    try removed.bind(a, "p", "physical-parent");
    try journal.append(&removed);
    var inserted = try txn.parseCommitRequest(a, "{\"read_set\":[],\"tables\":{\"p\":{\"inserts\":{\"x\":{\"id\":1}}}}}");
    defer inserted.deinit(a);
    try inserted.tables[0].predicates.append(a, .{ .key = try a.dupe(u8, "x"), .expected_version = 0, .unique_absence = true });
    try inserted.bind(a, "p", "physical-parent");
    try journal.append(&inserted);
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    const folded = try journal.folded(scratch.allocator());
    try std.testing.expectEqual(@as(usize, 1), folded.tables.len);
    try std.testing.expectEqual(@as(usize, 1), folded.tables[0].batch.writes.len);
    try std.testing.expectEqual(@as(usize, 0), folded.tables[0].batch.deletes.len);
    try std.testing.expectEqual(@as(u64, 7), folded.tables[0].predicates.items[0].expected_version);
    try std.testing.expect(!folded.tables[0].predicates.items[0].unique_absence);
    const events = try journal.distributedEvents(scratch.allocator());
    try std.testing.expectEqual(@as(usize, 2), events.len);
    try std.testing.expectEqualStrings("x", events[0].deletes[0]);
    try std.testing.expectEqualStrings("physical-parent", events[1].table_name);
    try std.testing.expectEqual(@as(usize, 1), events[1].writes.len);
}
