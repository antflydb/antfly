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

//! Cross-shard entity upsert for the promoter (see zig/RESOLUTION.md).
//!
//! The promoter runs on the source shard but canonical entities live in a
//! dedicated entity table, usually on another shard. It writes them through the
//! `db_mod.EntitySink` seam; `DistributedEntitySink` implements that seam over
//! the api layer's routing-aware `TableWriteSource`, which routes each write to
//! whichever group owns the entity key (local commit or remote raft proposal).
//!
//! The upsert is an idempotent merge `DocumentTransform`: it sets the entity
//! type, unions the surface form into `aliases`, and sets the canonical name
//! (`upsert` so the document is created if absent). Replaying the same promotion
//! is a no-op; two mentions resolving to one entity union their aliases instead
//! of clobbering. This is the decoupled, fail-closed phase-1 placement from
//! RESOLUTION.md (the entity write is independent of the source-shard edges).

const std = @import("std");
const db_mod = @import("../storage/db/mod.zig");
const table_writes = @import("table_write_source.zig");
const distributed_txn = @import("distributed_txn.zig");

const EntitySink = db_mod.EntitySink;

/// Adapts the routing-aware `TableWriteSource` to the promoter's `EntitySink`.
/// Holds only borrowed handles, so it must not outlive the write source.
pub const DistributedEntitySink = struct {
    writes: table_writes.TableWriteSource,
    /// Sync level for entity upserts. `write` (durable, not full-index) keeps
    /// promotion latency low; the entity shard indexes asynchronously.
    sync_level: db_mod.types.SyncLevel = .write,
    /// When set, commit the entity upsert through the source's atomic batch
    /// contract. First-party sources collapse a single participant to one
    /// fenced shard batch and use 2PC only when operations span groups. This is
    /// the reachable foundation for deferred multi-participant entity+edge
    /// coupling (RESOLUTION.md option 1). Falls back to a plain table batch when
    /// the write source does not implement an atomic commit path.
    transactional: bool = false,

    pub fn entitySink(self: *DistributedEntitySink) EntitySink {
        return .{ .ptr = self, .vtable = &vtable };
    }

    const vtable = EntitySink.VTable{ .upsert = upsertFn, .upsert_batch = upsertBatchFn };

    /// Promote all of a document's entities in one transaction. With
    /// `transactional` set, this is a single multi-participant 2PC commit across
    /// the entity table's shards, so a document never lands a partial set of its
    /// entities; otherwise it falls back to independent per-entity upserts.
    fn upsertBatchFn(
        ptr: *anyopaque,
        allocator: std.mem.Allocator,
        entries: []const db_mod.EntityUpsert,
    ) anyerror!void {
        const self: *DistributedEntitySink = @ptrCast(@alignCast(ptr));
        if (entries.len == 0) return;
        if (!self.transactional) {
            for (entries) |e| try upsertFn(ptr, allocator, e.table, e.key, e.doc_json);
            return;
        }

        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const a = arena.allocator();

        // Group merge transforms by table (one TableCommitRequest per table;
        // commitTransaction routes each key to its group and commits atomically).
        var tables = std.ArrayListUnmanaged([]const u8).empty;
        var table_ops = std.ArrayListUnmanaged(std.ArrayListUnmanaged(db_mod.types.DocumentTransform)).empty;
        for (entries) |e| {
            const ops = try buildMergeOps(a, e.doc_json);
            if (ops.len == 0) continue;
            const transform = db_mod.types.DocumentTransform{ .key = e.key, .operations = ops, .upsert = true };
            const idx = blk: {
                for (tables.items, 0..) |t, i| {
                    if (std.mem.eql(u8, t, e.table)) break :blk i;
                }
                try tables.append(a, e.table);
                try table_ops.append(a, .empty);
                break :blk tables.items.len - 1;
            };
            try table_ops.items[idx].append(a, transform);
        }
        if (tables.items.len == 0) return;

        var reqs = std.ArrayListUnmanaged(distributed_txn.TableCommitRequest).empty;
        for (tables.items, 0..) |t, i| {
            try reqs.append(a, .{ .table_name = t, .transforms = table_ops.items[i].items });
        }

        // Promotion is a stateless, idempotent batch. Use the batch commit
        // contract so first-party sources can safely retry topology races and
        // collapse a single-shard promotion to one fenced Raft batch. The
        // implementation still uses 2PC when the entities span groups, so the
        // document-level atomicity contract is unchanged.
        const outcome = try self.writes.commitBatch(allocator, reqs.items, self.sync_level);
        if (outcome) |result| {
            switch (result) {
                .committed => return,
                .conflict => return error.EntityPromotionConflict,
            }
        }
        // Transaction path unsupported by this write source; fall back to
        // independent per-entity upserts (non-atomic).
        for (entries) |e| try upsertFn(ptr, allocator, e.table, e.key, e.doc_json);
    }

    fn upsertFn(
        ptr: *anyopaque,
        allocator: std.mem.Allocator,
        table: []const u8,
        key: []const u8,
        doc_json: []const u8,
    ) anyerror!void {
        const self: *DistributedEntitySink = @ptrCast(@alignCast(ptr));

        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const a = arena.allocator();

        const ops = try buildMergeOps(a, doc_json);
        if (ops.len == 0) return;
        const transform = db_mod.types.DocumentTransform{ .key = key, .operations = ops, .upsert = true };

        if (self.transactional) {
            // Commit the merge through the atomic batch path. A null outcome
            // means the write source has no atomic commit callback, so fall
            // back to a plain table batch.
            const outcome = try self.writes.commitBatch(allocator, &.{.{ .table_name = table, .transforms = &.{transform} }}, self.sync_level);
            if (outcome) |result| {
                switch (result) {
                    .committed => return,
                    // The idempotent merge carries no version predicate, so a
                    // conflict means a genuine topology/intent clash; surface it
                    // so the promoter retries on the next catch-up.
                    .conflict => return error.EntityPromotionConflict,
                }
            }
        }

        return self.batchUpsert(allocator, table, transform);
    }

    fn batchUpsert(self: *DistributedEntitySink, allocator: std.mem.Allocator, table: []const u8, transform: db_mod.types.DocumentTransform) anyerror!void {
        const req = db_mod.types.BatchRequest{
            .transforms = &.{transform},
            .sync_level = self.sync_level,
        };
        // null means the table is unknown to this node's routing (e.g. not yet
        // created). Keep the promotion sequence unapplied so replay retries once
        // metadata/routing catches up.
        _ = (try self.writes.batch(allocator, table, req)) orelse return error.EntityPromotionUnavailable;
    }
};

/// Build the merge operations from a canonical entity document
/// (`{entity_type, canonical_name, aliases:[...]}`): seed scalar fields only on
/// insert and `add_to_set` each alias so replay does not clobber curated entity
/// fields while concurrent promotions still union aliases.
fn buildMergeOps(a: std.mem.Allocator, doc_json: []const u8) ![]db_mod.types.TransformOp {
    var parsed = std.json.parseFromSlice(std.json.Value, a, doc_json, .{}) catch return &.{};
    defer parsed.deinit();
    if (parsed.value != .object) return &.{};
    const obj = parsed.value.object;

    var ops = std.ArrayListUnmanaged(db_mod.types.TransformOp).empty;

    if (obj.get("entity_type")) |v| {
        if (v == .string) try ops.append(a, .{ .op = .set_on_insert, .path = "entity_type", .value_json = try jsonStringAlloc(a, v.string) });
    }
    if (obj.get("canonical_name")) |v| {
        if (v == .string) try ops.append(a, .{ .op = .set_on_insert, .path = "canonical_name", .value_json = try jsonStringAlloc(a, v.string) });
    }
    if (obj.get("aliases")) |v| {
        if (v == .array) {
            for (v.array.items) |item| {
                if (item == .string) try ops.append(a, .{ .op = .add_to_set, .path = "aliases", .value_json = try jsonStringAlloc(a, item.string) });
            }
        }
    }
    return try ops.toOwnedSlice(a);
}

/// JSON-encode `s` as a quoted string value (the `value_json` a transform op
/// expects), reusing std's escaping.
fn jsonStringAlloc(a: std.mem.Allocator, s: []const u8) ![]u8 {
    return try std.fmt.allocPrint(a, "{f}", .{std.json.fmt(s, .{})});
}

const testing = std.testing;

/// Fake routing-aware write source: records the batch requests it receives for
/// a single table so the sink's transform construction can be asserted without
/// a cluster.
const FakeTableWriteSource = struct {
    alloc: std.mem.Allocator,
    table: []const u8,
    keys: std.ArrayListUnmanaged([]u8) = .empty,
    transforms_json: std.ArrayListUnmanaged([]u8) = .empty,
    /// Set so the source advertises the transaction vtable method.
    support_transactions: bool = false,
    /// Set so the source advertises the optimized stateless batch commit.
    support_commit_batch: bool = false,
    commit_calls: usize = 0,
    commit_batch_calls: usize = 0,

    fn deinit(self: *FakeTableWriteSource) void {
        for (self.keys.items) |k| self.alloc.free(k);
        for (self.transforms_json.items) |t| self.alloc.free(t);
        self.keys.deinit(self.alloc);
        self.transforms_json.deinit(self.alloc);
    }

    fn source(self: *FakeTableWriteSource) table_writes.TableWriteSource {
        return .{ .ptr = self, .vtable = if (self.support_commit_batch)
            &batch_commit_vtable
        else if (self.support_transactions)
            &txn_vtable
        else
            &vtable };
    }

    const vtable = table_writes.TableWriteSource.VTable{ .batch = batch };
    const txn_vtable = table_writes.TableWriteSource.VTable{ .batch = batch, .commit_transaction = commitTransaction };
    const batch_commit_vtable = table_writes.TableWriteSource.VTable{
        .batch = batch,
        .commit_transaction = commitTransaction,
        .commit_batch = commitBatch,
    };

    fn commitTransaction(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        tables: []const distributed_txn.TableCommitRequest,
        sync_level: db_mod.types.SyncLevel,
    ) anyerror!?distributed_txn.CommitOutcome {
        _ = sync_level;
        const self: *FakeTableWriteSource = @ptrCast(@alignCast(ptr));
        self.commit_calls += 1;
        for (tables) |t| {
            if (!std.mem.eql(u8, t.table_name, self.table)) return null;
            try recordTransforms(self, alloc, t.transforms);
        }
        return .{ .committed = .{ .participant_count = tables.len } };
    }

    fn commitBatch(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        tables: []const distributed_txn.TableCommitRequest,
        sync_level: db_mod.types.SyncLevel,
    ) anyerror!?distributed_txn.CommitOutcome {
        _ = sync_level;
        const self: *FakeTableWriteSource = @ptrCast(@alignCast(ptr));
        self.commit_batch_calls += 1;
        for (tables) |t| {
            if (!std.mem.eql(u8, t.table_name, self.table)) return null;
            try recordTransforms(self, alloc, t.transforms);
        }
        return .{ .committed = .{ .participant_count = tables.len } };
    }

    fn recordTransforms(self: *FakeTableWriteSource, alloc: std.mem.Allocator, transforms: []const db_mod.types.DocumentTransform) anyerror!void {
        _ = alloc;
        for (transforms) |t| {
            try self.keys.append(self.alloc, try self.alloc.dupe(u8, t.key));
            var buf = std.ArrayListUnmanaged(u8).empty;
            defer buf.deinit(self.alloc);
            for (t.operations) |op| {
                try buf.appendSlice(self.alloc, @tagName(op.op));
                try buf.append(self.alloc, ' ');
                try buf.appendSlice(self.alloc, op.path);
                try buf.append(self.alloc, '=');
                try buf.appendSlice(self.alloc, op.value_json orelse "");
                try buf.append(self.alloc, ';');
            }
            try self.transforms_json.append(self.alloc, try self.alloc.dupe(u8, buf.items));
        }
    }

    fn batch(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        req: db_mod.types.BatchRequest,
    ) anyerror!?void {
        _ = alloc;
        const self: *FakeTableWriteSource = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table)) return null;
        for (req.transforms) |t| {
            try self.keys.append(self.alloc, try self.alloc.dupe(u8, t.key));
            // Flatten the ops into a debug string for assertions.
            var buf = std.ArrayListUnmanaged(u8).empty;
            defer buf.deinit(self.alloc);
            for (t.operations) |op| {
                try buf.appendSlice(self.alloc, @tagName(op.op));
                try buf.append(self.alloc, ' ');
                try buf.appendSlice(self.alloc, op.path);
                try buf.append(self.alloc, '=');
                try buf.appendSlice(self.alloc, op.value_json orelse "");
                try buf.append(self.alloc, ';');
            }
            try self.transforms_json.append(self.alloc, try self.alloc.dupe(u8, buf.items));
        }
    }
};

test "DistributedEntitySink upserts a merge transform per entity" {
    const alloc = testing.allocator;
    var fake = FakeTableWriteSource{ .alloc = alloc, .table = "entities" };
    defer fake.deinit();

    var sink_impl = DistributedEntitySink{ .writes = fake.source() };
    const sink = sink_impl.entitySink();

    try sink.upsert(alloc, "entities", "person/ada_lovelace",
        \\{"entity_type":"person","canonical_name":"Ada Lovelace","aliases":["Ada Lovelace"]}
    );

    try testing.expectEqual(@as(usize, 1), fake.keys.items.len);
    try testing.expectEqualStrings("person/ada_lovelace", fake.keys.items[0]);
    const ops = fake.transforms_json.items[0];
    // Seeds scalar fields only when creating the entity and unions the alias.
    try testing.expect(std.mem.indexOf(u8, ops, "set_on_insert entity_type=\"person\"") != null);
    try testing.expect(std.mem.indexOf(u8, ops, "set_on_insert canonical_name=\"Ada Lovelace\"") != null);
    try testing.expect(std.mem.indexOf(u8, ops, "add_to_set aliases=\"Ada Lovelace\"") != null);
}

test "DistributedEntitySink fails closed on an unknown table" {
    const alloc = testing.allocator;
    var fake = FakeTableWriteSource{ .alloc = alloc, .table = "entities" };
    defer fake.deinit();
    var sink_impl = DistributedEntitySink{ .writes = fake.source() };
    const sink = sink_impl.entitySink();

    // Routed to a table this source does not serve -> keep promotion unapplied so
    // replay can retry after metadata/routing catches up.
    try testing.expectError(error.EntityPromotionUnavailable, sink.upsert(alloc, "other", "person/x",
        \\{"entity_type":"person","canonical_name":"X","aliases":["X"]}
    ));
    try testing.expectEqual(@as(usize, 0), fake.keys.items.len);
}

test "DistributedEntitySink merge ops preserve curated canonical fields" {
    const alloc = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();

    const ops = try buildMergeOps(a,
        \\{"entity_type":"person","canonical_name":"Ada Lovelace","aliases":["Ada Lovelace"]}
    );
    const transform = db_mod.types.DocumentTransform{
        .key = "person/ada_lovelace",
        .operations = ops,
        .upsert = true,
    };
    const resolved = try db_mod.transform.resolveDocumentTransform(
        alloc,
        "{\"entity_type\":\"human\",\"canonical_name\":\"Countess of Lovelace\",\"aliases\":[\"A. A. L.\"]}",
        transform,
    );
    defer alloc.free(resolved.?);

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, resolved.?, .{});
    defer parsed.deinit();
    try testing.expectEqualStrings("human", parsed.value.object.get("entity_type").?.string);
    try testing.expectEqualStrings("Countess of Lovelace", parsed.value.object.get("canonical_name").?.string);
    try testing.expectEqual(@as(usize, 2), parsed.value.object.get("aliases").?.array.items.len);
}

test "DistributedEntitySink skips a malformed document" {
    const alloc = testing.allocator;
    var fake = FakeTableWriteSource{ .alloc = alloc, .table = "entities" };
    defer fake.deinit();
    var sink_impl = DistributedEntitySink{ .writes = fake.source() };
    const sink = sink_impl.entitySink();

    try sink.upsert(alloc, "entities", "person/x", "not json");
    try testing.expectEqual(@as(usize, 0), fake.keys.items.len);
}

test "DistributedEntitySink transactional promotion batch prefers stateless batch commit" {
    const alloc = testing.allocator;
    var fake = FakeTableWriteSource{
        .alloc = alloc,
        .table = "entities",
        .support_transactions = true,
        .support_commit_batch = true,
    };
    defer fake.deinit();

    var sink_impl = DistributedEntitySink{ .writes = fake.source(), .transactional = true };
    const sink = sink_impl.entitySink();

    try sink.upsertBatch(alloc, &.{
        .{
            .table = "entities",
            .key = "person/ada_lovelace",
            .doc_json = "{\"entity_type\":\"person\",\"canonical_name\":\"Ada Lovelace\",\"aliases\":[\"Ada Lovelace\"]}",
        },
        .{
            .table = "entities",
            .key = "org/antfly",
            .doc_json = "{\"entity_type\":\"org\",\"canonical_name\":\"Antfly\",\"aliases\":[\"Antfly\"]}",
        },
    });

    // Routed through the stateless batch contract rather than forcing 2PC.
    try testing.expectEqual(@as(usize, 0), fake.commit_calls);
    try testing.expectEqual(@as(usize, 1), fake.commit_batch_calls);
    try testing.expectEqual(@as(usize, 2), fake.keys.items.len);
    try testing.expectEqualStrings("person/ada_lovelace", fake.keys.items[0]);
    try testing.expectEqualStrings("org/antfly", fake.keys.items[1]);
    try testing.expect(std.mem.indexOf(u8, fake.transforms_json.items[0], "add_to_set aliases=\"Ada Lovelace\"") != null);
}

test "DistributedEntitySink batch commit remains compatible with transaction-only sources" {
    const alloc = testing.allocator;
    var fake = FakeTableWriteSource{ .alloc = alloc, .table = "entities", .support_transactions = true };
    defer fake.deinit();

    var sink_impl = DistributedEntitySink{ .writes = fake.source(), .transactional = true };
    const sink = sink_impl.entitySink();

    try sink.upsert(alloc, "entities", "person/ada_lovelace",
        \\{"entity_type":"person","canonical_name":"Ada Lovelace","aliases":["Ada Lovelace"]}
    );

    // TableWriteSource.commitBatch falls back to the transaction callback for
    // older/custom sources that have not implemented the optimized contract.
    try testing.expectEqual(@as(usize, 1), fake.commit_calls);
    try testing.expectEqual(@as(usize, 0), fake.commit_batch_calls);
    try testing.expectEqual(@as(usize, 1), fake.keys.items.len);
}

test "DistributedEntitySink transactional mode falls back to batch when unsupported" {
    const alloc = testing.allocator;
    // support_transactions = false -> the source has no commit_transaction vtable.
    var fake = FakeTableWriteSource{ .alloc = alloc, .table = "entities" };
    defer fake.deinit();

    var sink_impl = DistributedEntitySink{ .writes = fake.source(), .transactional = true };
    const sink = sink_impl.entitySink();

    try sink.upsert(alloc, "entities", "person/ada_lovelace",
        \\{"entity_type":"person","canonical_name":"Ada Lovelace","aliases":["Ada Lovelace"]}
    );

    // commitTransaction returned null (unwired) -> fell back to batch, still wrote.
    try testing.expectEqual(@as(usize, 0), fake.commit_calls);
    try testing.expectEqual(@as(usize, 1), fake.keys.items.len);
}
