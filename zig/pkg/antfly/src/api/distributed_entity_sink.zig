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

const EntitySink = db_mod.EntitySink;

/// Adapts the routing-aware `TableWriteSource` to the promoter's `EntitySink`.
/// Holds only borrowed handles, so it must not outlive the write source.
pub const DistributedEntitySink = struct {
    writes: table_writes.TableWriteSource,
    /// Sync level for entity upserts. `write` (durable, not full-index) keeps
    /// promotion latency low; the entity shard indexes asynchronously.
    sync_level: db_mod.types.SyncLevel = .write,

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

        const req = db_mod.types.BatchRequest{
            .transforms = &.{.{ .key = key, .operations = ops, .upsert = true }},
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

    fn deinit(self: *FakeTableWriteSource) void {
        for (self.keys.items) |k| self.alloc.free(k);
        for (self.transforms_json.items) |t| self.alloc.free(t);
        self.keys.deinit(self.alloc);
        self.transforms_json.deinit(self.alloc);
    }

    fn source(self: *FakeTableWriteSource) table_writes.TableWriteSource {
        return .{ .ptr = self, .vtable = &vtable };
    }

    const vtable = table_writes.TableWriteSource.VTable{ .batch = batch };

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
