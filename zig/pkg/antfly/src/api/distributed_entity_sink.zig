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
const table_writes = @import("table_writes.zig");
const distributed_txn = @import("distributed_txn.zig");

const EntitySink = db_mod.EntitySink;

/// Adapts the routing-aware `TableWriteSource` to the promoter's `EntitySink`.
/// Holds only borrowed handles, so it must not outlive the write source.
pub const DistributedEntitySink = struct {
    writes: table_writes.TableWriteSource,
    /// Sync level for entity upserts. `write` (durable, not full-index) keeps
    /// promotion latency low; the entity shard indexes asynchronously.
    sync_level: db_mod.types.SyncLevel = .write,
    /// When set, commit the entity upsert through the distributed-transaction
    /// (2PC) path rather than a plain batch. Single participant today (the entity
    /// table), so it behaves like a transactional batch; it is the reachable
    /// foundation for the deferred multi-participant entity+edge coupling
    /// (RESOLUTION.md option 1). Falls back to a batch when the write source does
    /// not implement the transaction path.
    transactional: bool = false,

    pub fn entitySink(self: *DistributedEntitySink) EntitySink {
        return .{ .ptr = self, .vtable = &vtable };
    }

    const vtable = EntitySink.VTable{ .upsert = upsertFn };

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
            // Commit the merge through the 2PC path. A null outcome means the
            // write source does not implement the transaction vtable, so fall
            // back to a plain batch.
            const outcome = self.writes.commitTransaction(allocator, &.{.{ .table_name = table, .transforms = &.{transform} }}, self.sync_level) catch |err| switch (err) {
                error.UnexpectedHttpStatus, error.TableNotFound => return,
                else => return err,
            };
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
        // created); drop the promotion rather than failing the stage. Replay
        // re-promotes once the entity table exists.
        _ = self.writes.batch(allocator, table, req) catch |err| switch (err) {
            error.UnexpectedHttpStatus, error.TableNotFound => return,
            else => return err,
        } orelse return;
    }
};

/// Build the merge operations from a canonical entity document
/// (`{entity_type, canonical_name, aliases:[...]}`): `set` the scalar fields and
/// `add_to_set` each alias so concurrent promotions union rather than clobber.
fn buildMergeOps(a: std.mem.Allocator, doc_json: []const u8) ![]db_mod.types.TransformOp {
    var parsed = std.json.parseFromSlice(std.json.Value, a, doc_json, .{}) catch return &.{};
    defer parsed.deinit();
    if (parsed.value != .object) return &.{};
    const obj = parsed.value.object;

    var ops = std.ArrayListUnmanaged(db_mod.types.TransformOp).empty;

    if (obj.get("entity_type")) |v| {
        if (v == .string) try ops.append(a, .{ .op = .set, .path = "entity_type", .value_json = try jsonStringAlloc(a, v.string) });
    }
    if (obj.get("canonical_name")) |v| {
        if (v == .string) try ops.append(a, .{ .op = .set, .path = "canonical_name", .value_json = try jsonStringAlloc(a, v.string) });
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
    commit_calls: usize = 0,

    fn deinit(self: *FakeTableWriteSource) void {
        for (self.keys.items) |k| self.alloc.free(k);
        for (self.transforms_json.items) |t| self.alloc.free(t);
        self.keys.deinit(self.alloc);
        self.transforms_json.deinit(self.alloc);
    }

    fn source(self: *FakeTableWriteSource) table_writes.TableWriteSource {
        return .{ .ptr = self, .vtable = if (self.support_transactions) &txn_vtable else &vtable };
    }

    const vtable = table_writes.TableWriteSource.VTable{ .batch = batch };
    const txn_vtable = table_writes.TableWriteSource.VTable{ .batch = batch, .commit_transaction = commitTransaction };

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
    // Sets the scalar fields and unions the alias.
    try testing.expect(std.mem.indexOf(u8, ops, "set entity_type=\"person\"") != null);
    try testing.expect(std.mem.indexOf(u8, ops, "set canonical_name=\"Ada Lovelace\"") != null);
    try testing.expect(std.mem.indexOf(u8, ops, "add_to_set aliases=\"Ada Lovelace\"") != null);
}

test "DistributedEntitySink ignores an unknown table" {
    const alloc = testing.allocator;
    var fake = FakeTableWriteSource{ .alloc = alloc, .table = "entities" };
    defer fake.deinit();
    var sink_impl = DistributedEntitySink{ .writes = fake.source() };
    const sink = sink_impl.entitySink();

    // Routed to a table this source does not serve -> dropped, no transform.
    try sink.upsert(alloc, "other", "person/x",
        \\{"entity_type":"person","canonical_name":"X","aliases":["X"]}
    );
    try testing.expectEqual(@as(usize, 0), fake.keys.items.len);
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

test "DistributedEntitySink transactional mode commits through the 2PC path" {
    const alloc = testing.allocator;
    var fake = FakeTableWriteSource{ .alloc = alloc, .table = "entities", .support_transactions = true };
    defer fake.deinit();

    var sink_impl = DistributedEntitySink{ .writes = fake.source(), .transactional = true };
    const sink = sink_impl.entitySink();

    try sink.upsert(alloc, "entities", "person/ada_lovelace",
        \\{"entity_type":"person","canonical_name":"Ada Lovelace","aliases":["Ada Lovelace"]}
    );

    // Routed through commitTransaction, not batch, and carried the merge ops.
    try testing.expectEqual(@as(usize, 1), fake.commit_calls);
    try testing.expectEqual(@as(usize, 1), fake.keys.items.len);
    try testing.expectEqualStrings("person/ada_lovelace", fake.keys.items[0]);
    try testing.expect(std.mem.indexOf(u8, fake.transforms_json.items[0], "add_to_set aliases=\"Ada Lovelace\"") != null);
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
