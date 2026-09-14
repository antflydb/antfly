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

//! Physical ordered-index records and transactional mutation staging.
//! Forward: [private namespace][12-byte ID][raw tuple][encoded document][u32 length].
//! Reverse: [document prefix][reverse kind][12-byte ID] -> [v1][tuple][CRC32C].
//! Reverse records participate in document-range ownership. Range transfer must
//! derive their forward companions and exclude unselected global forward keys.
//! Neither this module nor a write plan establishes query readiness.

const std = @import("std");
const internal = @import("../internal_keys.zig");
const native = @import("../relational_index.zig");
const plans = @import("relational_index_plan.zig");
const Allocator = std.mem.Allocator;
pub const Id = native.RelationalIndexId;
/// Private metadata namespace; cannot be mistaken for a primary user row.
/// This is an unpublished format, not the mega branch's escaped tuple format.
pub const forward_namespace = "\x00\x00R\x01";
const reverse_version: u8 = 1;
pub const forward_prefix_len = forward_namespace.len + Id.encoded_len;

pub fn forwardPrefix(id: Id) ![forward_prefix_len]u8 {
    if (id.generation == 0) return error.InvalidRelationalIndexId;
    var prefix: [forward_prefix_len]u8 = undefined;
    @memcpy(prefix[0..forward_namespace.len], forward_namespace);
    @memcpy(prefix[forward_namespace.len..], &id.encode());
    return prefix;
}

pub fn isForwardKey(key: []const u8) bool {
    return std.mem.startsWith(u8, key, forward_namespace);
}

pub fn appendReverseKey(alloc: Allocator, out: *std.ArrayList(u8), id: Id, document: []const u8) !void {
    if (id.generation == 0) return error.InvalidRelationalIndexId;
    const start = out.items.len;
    errdefer out.shrinkRetainingCapacity(start);
    try internal.appendDocumentPrefix(out, alloc, document);
    try out.append(alloc, internal.relational_index_reverse_kind);
    try out.appendSlice(alloc, &id.encode());
}

const ReverseKey = struct {
    id: Id,
    /// Includes the terminator. Reuse it without decoding/escaping on transfer.
    document_component: []const u8,
};

fn parseReverseKey(key: []const u8) !ReverseKey {
    if (!internal.isRelationalIndexReverseKey(key)) return error.InvalidRelationalIndexReverseKey;
    const term = internal.findComponentTerminator(key, 1).?;
    return .{ .id = try Id.decode(key[term + 3 ..]), .document_component = key[1 .. term + 2] };
}

fn checksum(key: []const u8, body: []const u8) u32 {
    var crc = @import("antfly_hash").Crc32c.init();
    crc.update(key);
    crc.update(body);
    return crc.final();
}

pub fn appendReverseValue(alloc: Allocator, out: *std.ArrayList(u8), reverse_key: []const u8, tuple: []const u8) !void {
    _ = try parseReverseKey(reverse_key);
    if (tuple.len == 0) return error.InvalidRelationalIndexTuple;
    const start = out.items.len;
    errdefer out.shrinkRetainingCapacity(start);
    try out.append(alloc, reverse_version);
    try out.appendSlice(alloc, tuple);
    var crc: [4]u8 = undefined;
    std.mem.writeInt(u32, &crc, checksum(reverse_key, out.items[start..]), .little);
    try out.appendSlice(alloc, &crc);
}

/// CRC detects physical corruption/copy-to-wrong-owner, not hostile tampering.
/// The tuple is opaque here; its generation supplies schema-bound validation.
pub fn reverseTuple(reverse_key: []const u8, value: []const u8) ![]const u8 {
    _ = try parseReverseKey(reverse_key);
    if (value.len < 6 or value[0] != reverse_version) return error.InvalidRelationalIndexReverseValue;
    const end = value.len - 4;
    if (std.mem.readInt(u32, value[end..][0..4], .little) != checksum(reverse_key, value[0..end]))
        return error.RelationalIndexReverseChecksumMismatch;
    return value[1..end];
}

fn appendForwardEncoded(alloc: Allocator, out: *std.ArrayList(u8), id: Id, tuple: []const u8, document_component: []const u8) !void {
    if (tuple.len == 0) return error.InvalidRelationalIndexTuple;
    const start = out.items.len;
    errdefer out.shrinkRetainingCapacity(start);
    try out.appendSlice(alloc, &(try forwardPrefix(id)));
    // Tuple framing is already schema-bound; escaping it again inflates
    // numeric keys and destroys the direct left-prefix scan boundary.
    try out.appendSlice(alloc, tuple);
    try out.appendSlice(alloc, document_component);
    // A fixed footer makes ownership inspectable without a retired schema or
    // tuple decoder. It follows the document terminator, preserving key order.
    var length: [4]u8 = undefined;
    std.mem.writeInt(u32, &length, std.math.cast(u32, document_component.len) orelse return error.InvalidRelationalIndexForwardKey, .big);
    try out.appendSlice(alloc, &length);
}

/// Companion reconstruction for range transfer, cleanup, and integrity jobs.
/// It needs only the document-owned record, not the primary row or current
/// schema; retired generations retain their own physical identities.
pub fn appendForwardFromReverse(alloc: Allocator, out: *std.ArrayList(u8), key: []const u8, value: []const u8) !void {
    const reverse = try parseReverseKey(key);
    const tuple = try reverseTuple(key, value);
    try appendForwardEncoded(alloc, out, reverse.id, tuple, reverse.document_component);
}

pub const Forward = struct {
    tuple: []const u8,
    document_component: []const u8,
};

pub fn forwardOwnership(key: []const u8) !Forward {
    if (!isForwardKey(key) or key.len < forward_prefix_len + 7) return error.InvalidRelationalIndexForwardKey;
    _ = try Id.decode(key[forward_namespace.len..forward_prefix_len]);
    const end = key.len - 4;
    const size = std.mem.readInt(u32, key[end..][0..4], .big);
    if (size < 2 or size >= end - forward_prefix_len) return error.InvalidRelationalIndexForwardKey;
    const start = end - size;
    const term = internal.findComponentTerminator(key, start) orelse return error.InvalidRelationalIndexForwardKey;
    if (term + 2 != end) return error.InvalidRelationalIndexForwardKey;
    return .{ .tuple = key[forward_prefix_len..start], .document_component = key[start..end] };
}

pub fn parseForward(key: []const u8, index: plans.BoundIndex) !Forward {
    const prefix = try forwardPrefix(index.id());
    if (!std.mem.startsWith(u8, key, &prefix)) return error.RelationalIndexGenerationMismatch;
    const owner = try forwardOwnership(key);
    if (try index.tuple.prefixLen(owner.tuple) != owner.tuple.len) return error.InvalidRelationalIndexForwardKey;
    return owner;
}

/// Explicitly distinguish backfill/new rows from already indexed rows. Missing
/// reverse state for an indexed row is not silently treated as a new insertion.
pub const Presence = enum { new_or_building, indexed, ready_generation };
pub const Effect = enum { inserted, updated, deleted, unchanged };

/// Worker/request-owned scratch, reusable across rows. Txn.put MUST copy its
/// input before returning (DocStore.Txn does). The caller holds its mutation
/// fence and must commit primary rows + these effects together, or abort ALL
/// of them on any error. This object never owns/commits a transaction.
pub const Writer = struct {
    alloc: Allocator,
    reverse_key: std.ArrayList(u8) = .empty,
    reverse_value: std.ArrayList(u8) = .empty,
    old_forward: std.ArrayList(u8) = .empty,
    new_forward: std.ArrayList(u8) = .empty,

    pub fn init(alloc: Allocator) Writer {
        return .{ .alloc = alloc };
    }

    pub fn deinit(self: *Writer) void {
        self.reverse_key.deinit(self.alloc);
        self.reverse_value.deinit(self.alloc);
        self.old_forward.deinit(self.alloc);
        self.new_forward.deinit(self.alloc);
        self.* = undefined;
    }

    /// Capacity can be reserved before entering the serialized apply section.
    pub fn reserve(self: *Writer, encoded_document_bytes: usize, tuple_bytes: usize) !void {
        const reverse_len = try std.math.add(usize, encoded_document_bytes, 2 + Id.encoded_len);
        const forward_len = try std.math.add(usize, try std.math.add(usize, forward_prefix_len + 4, encoded_document_bytes), tuple_bytes);
        try self.reverse_key.ensureTotalCapacity(self.alloc, reverse_len);
        try self.reverse_value.ensureTotalCapacity(self.alloc, try std.math.add(usize, tuple_bytes, 5));
        try self.old_forward.ensureTotalCapacity(self.alloc, forward_len);
        try self.new_forward.ensureTotalCapacity(self.alloc, forward_len);
    }

    fn reset(self: *Writer) void {
        self.reverse_key.clearRetainingCapacity();
        self.reverse_value.clearRetainingCapacity();
        self.old_forward.clearRetainingCapacity();
        self.new_forward.clearRetainingCapacity();
    }

    pub fn upsert(
        self: *Writer,
        txn: anytype,
        index: plans.BoundIndex,
        document: []const u8,
        tuple: []const u8,
        presence: Presence,
    ) !Effect {
        self.reset();
        if (try index.tuple.prefixLen(tuple) != tuple.len) return error.InvalidRelationalIndexTuple;
        try appendReverseKey(self.alloc, &self.reverse_key, index.id(), document);
        const old = txn.get(self.reverse_key.items) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        if (old) |value| {
            const old_tuple = try reverseTuple(self.reverse_key.items, value);
            if (try index.tuple.prefixLen(old_tuple) != old_tuple.len) return error.InvalidRelationalIndexTuple;
            // The reverse record is authoritative under the atomic pair
            // invariant. Integrity scrubs verify forward existence separately.
            // An unchanged tuple must not create index WAL/LSM churn.
            if (std.mem.eql(u8, old_tuple, tuple)) return .unchanged;
            try appendForwardFromReverse(self.alloc, &self.old_forward, self.reverse_key.items, value);
        } else if (presence == .indexed) return error.MissingRelationalIndexReverse else if (presence == .ready_generation) {
            // Only a missing reverse entry needs the primary existence probe.
            // Ordinary updates and no-op keys retain their single lookup path.
            try self.old_forward.appendSlice(self.alloc, self.reverse_key.items[0 .. self.reverse_key.items.len - Id.encoded_len - 1]);
            try self.old_forward.append(self.alloc, internal.relational_row_kind);
            const primary = txn.get(self.old_forward.items) catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            };
            if (primary != null) return error.MissingRelationalIndexReverse;
            self.old_forward.clearRetainingCapacity();
        }
        const owner = try parseReverseKey(self.reverse_key.items);
        try appendForwardEncoded(self.alloc, &self.new_forward, index.id(), tuple, owner.document_component);
        try appendReverseValue(self.alloc, &self.reverse_value, self.reverse_key.items, tuple);
        // All scratch allocation and validation precedes the first mutation.
        if (old != null) try deleteIfPresent(txn, self.old_forward.items);
        try txn.put(self.new_forward.items, "");
        try txn.put(self.reverse_key.items, self.reverse_value.items);
        return if (old != null) .updated else .inserted;
    }

    pub fn delete(self: *Writer, txn: anytype, id: Id, document: []const u8, presence: Presence) !Effect {
        self.reset();
        try appendReverseKey(self.alloc, &self.reverse_key, id, document);
        const old = txn.get(self.reverse_key.items) catch |err| switch (err) {
            error.NotFound => if (presence == .indexed) return error.MissingRelationalIndexReverse else return .unchanged,
            else => return err,
        };
        try appendForwardFromReverse(self.alloc, &self.old_forward, self.reverse_key.items, old);
        try deleteIfPresent(txn, self.old_forward.items);
        try txn.delete(self.reverse_key.items);
        return .deleted;
    }

    /// Consume the already-prepared multi-index row under the current plan
    /// fence. The caller chooses presence from row/generation lifecycle facts.
    pub fn upsertPrepared(self: *Writer, txn: anytype, current: plans.View, batch: *const plans.Batch, row: usize, document: []const u8, presence: Presence) !void {
        if (!batch.isForPlan(current)) return error.PreparedGenerationChanged;
        for (current.boundIndexes(), 0..) |index, i|
            _ = try self.upsert(txn, index, document, (try batch.key(row, i)).bytes, presence);
    }
};

fn deleteIfPresent(txn: anytype, key: []const u8) !void {
    txn.delete(key) catch |err| switch (err) {
        error.NotFound => {},
        else => return err,
    };
}

/// Resumable ownership reconciliation for an unpublished split destination or
/// apply-fenced source. Each call releases its read snapshot and commits at
/// most 256 inspected records with a 1 MiB soft byte budget (one oversized
/// record still makes progress); no whole-index materialization.
/// Keep the range and store incarnation fixed for the cursor's lifetime.
/// Publication must wait for done. A failed page leaves the cursor unchanged.
pub const RangeRepair = struct {
    alloc: Allocator,
    phase: enum { prune, reconstruct, done } = .prune,
    after: ?[]u8 = null,

    pub fn deinit(self: *RangeRepair) void {
        if (self.after) |key| self.alloc.free(key);
        self.* = undefined;
    }

    pub fn done(self: RangeRepair) bool {
        return self.phase == .done;
    }

    pub fn step(self: *RangeRepair, store: *docstore.DocStore, lower: []const u8, upper: []const u8) !usize {
        if (self.done()) return 0;
        var arena = std.heap.ArenaAllocator.init(self.alloc);
        defer arena.deinit();
        const alloc = arena.allocator();
        var lower_prefix = std.ArrayList(u8).empty;
        try internal.appendDocumentPrefix(&lower_prefix, alloc, lower);
        var upper_prefix = std.ArrayList(u8).empty;
        if (upper.len != 0) try internal.appendDocumentPrefix(&upper_prefix, alloc, upper);
        const prefix = if (self.phase == .prune) forward_namespace else lower_prefix.items;
        var read = try store.beginReadTxn();
        defer read.abort();
        var cursor = try read.openCursor();
        defer cursor.close();
        var writes = std.ArrayList(docstore.KVPair).empty;
        var deletes = std.ArrayList([]const u8).empty;
        var scanned: usize = 0;
        var bytes: usize = 0;
        var last: ?[]const u8 = null;
        var exhausted = true;
        var entry = try cursor.seekAtOrAfter(self.after orelse prefix);
        while (entry) |kv| : (entry = try cursor.next()) {
            if (self.after) |after| if (std.mem.order(u8, kv.key, after) != .gt) continue;
            if (self.phase == .prune) {
                if (!isForwardKey(kv.key)) break;
            } else {
                if (kv.key.len == 0 or kv.key[0] != internal.user_namespace) break;
                if (upper.len != 0 and std.mem.order(u8, kv.key, upper_prefix.items) != .lt) break;
            }
            scanned += 1;
            bytes +|= kv.key.len;
            last = try alloc.dupe(u8, kv.key);
            if (self.phase == .prune) {
                const owner = try forwardOwnership(kv.key);
                const outside = std.mem.order(u8, owner.document_component, lower_prefix.items[1..]) == .lt or
                    (upper.len != 0 and std.mem.order(u8, owner.document_component, upper_prefix.items[1..]) != .lt);
                if (outside) try deletes.append(alloc, last.?);
            } else if (internal.isRelationalIndexReverseKey(kv.key)) {
                var forward = std.ArrayList(u8).empty;
                try appendForwardFromReverse(alloc, &forward, kv.key, kv.value);
                const existing: ?[]const u8 = read.get(forward.items) catch |err| switch (err) {
                    error.NotFound => null,
                    else => return err,
                };
                if (existing) |value| {
                    if (value.len != 0) return error.InvalidRelationalIndexForwardValue;
                } else try writes.append(alloc, .{ .key = forward.items, .value = "" });
                bytes +|= kv.value.len;
            }
            if (scanned >= 256 or bytes >= 1024 * 1024) {
                exhausted = false;
                break;
            }
        }
        const next = if (!exhausted) try self.alloc.dupe(u8, last.?) else null;
        errdefer if (next) |key| self.alloc.free(key);
        if (writes.items.len != 0 or deletes.items.len != 0)
            try store.putBatch(writes.items, deletes.items);
        if (self.after) |key| self.alloc.free(key);
        self.after = next;
        if (exhausted) self.phase = if (self.phase == .prune) .reconstruct else .done;
        return writes.items.len + deletes.items.len;
    }
};

/// Bridge to the DB's existing write/delete batch, without starting a second
/// commit. The caller holds the apply fence from base-read creation through
/// the final primary/index/outbox commit, and validates the pinned plan there.
/// Repeated changes to a key collapse to one final effect: the backend applies
/// deletes before puts, so simply concatenating each row's deltas is incorrect
/// for insert/update/delete of the same document in one request.
pub const Staged = struct {
    arena: std.heap.ArenaAllocator,
    base: *docstore.DocStore.Txn,
    pending: std.StringHashMapUnmanaged(?[]const u8) = .empty,
    state: enum { open, failed, sealed } = .open,

    pub const Effects = struct {
        writes: []const docstore.KVPair,
        deletes: []const []const u8,
    };

    pub fn init(alloc: Allocator, base: *docstore.DocStore.Txn) Staged {
        return .{ .arena = std.heap.ArenaAllocator.init(alloc), .base = base };
    }

    pub fn deinit(self: *Staged) void {
        self.arena.deinit();
        self.* = undefined;
    }

    fn requireOpen(self: *const Staged) !void {
        if (self.state != .open) return error.RelationalIndexStageClosed;
    }

    pub fn get(self: *Staged, key: []const u8) ![]const u8 {
        try self.requireOpen();
        if (self.pending.get(key)) |value| return value orelse error.NotFound;
        return self.base.get(key);
    }

    pub fn put(self: *Staged, key: []const u8, value: []const u8) !void {
        try self.set(key, value);
    }

    pub fn delete(self: *Staged, key: []const u8) anyerror!void {
        // Batch deletion is idempotent, including keys introduced then removed
        // within this stage. Reads observe this tombstone, not the base value.
        try self.set(key, null);
    }

    fn set(self: *Staged, key: []const u8, value: ?[]const u8) !void {
        try self.requireOpen();
        errdefer self.state = .failed;
        const alloc = self.arena.allocator();
        const owned_value = if (value) |bytes| try alloc.dupe(u8, bytes) else null;
        if (self.pending.getPtr(key)) |existing| {
            existing.* = owned_value;
            return;
        }
        const owned_key = try alloc.dupe(u8, key);
        try self.pending.put(alloc, owned_key, owned_value);
    }

    /// Any row-level failure poisons the whole stage, including effects from
    /// earlier indexes. The caller cannot accidentally seal a partial row.
    pub fn upsertPrepared(self: *Staged, writer: *Writer, current: plans.View, batch: *const plans.Batch, row: usize, document: []const u8, presence: Presence) !void {
        try self.requireOpen();
        errdefer self.state = .failed;
        try writer.upsertPrepared(self, current, batch, row, document, presence);
    }

    pub fn upsertPreparedWithReadiness(self: *Staged, writer: *Writer, current: plans.View, batch: *const plans.Batch, row: usize, document: []const u8, ready: []const bool) !void {
        try self.requireOpen();
        errdefer self.state = .failed;
        if (!batch.isForPlan(current) or ready.len != current.boundIndexes().len) return error.PreparedGenerationChanged;
        for (current.boundIndexes(), ready, 0..) |index, is_ready, i|
            _ = try writer.upsert(self, index, document, (try batch.key(row, i)).bytes, if (is_ready) .ready_generation else .new_or_building);
    }

    pub fn deleteIndex(self: *Staged, writer: *Writer, id: Id, document: []const u8, presence: Presence) !Effect {
        try self.requireOpen();
        errdefer self.state = .failed;
        return try writer.delete(self, id, document, presence);
    }

    /// Remove every generation owned by this document, including retired
    /// generations and rows introduced earlier in this same atomic batch.
    /// Collect identities before mutating the overlay so hash-map growth cannot
    /// invalidate an iterator. Reverse ownership avoids decoding primary rows.
    pub fn deleteDocument(self: *Staged, writer: *Writer, document: []const u8) !void {
        try self.requireOpen();
        errdefer self.state = .failed;
        const alloc = self.arena.allocator();
        var prefix = std.ArrayList(u8).empty;
        try internal.appendDocumentPrefix(&prefix, alloc, document);
        try prefix.append(alloc, internal.relational_index_reverse_kind);
        var ids = std.AutoHashMapUnmanaged(u128, Id).empty;
        var cursor = try self.base.openCursor();
        defer cursor.close();
        var entry = try cursor.seekAtOrAfter(prefix.items);
        while (entry) |kv| : (entry = try cursor.next()) {
            if (!std.mem.startsWith(u8, kv.key, prefix.items)) break;
            const id = (try parseReverseKey(kv.key)).id;
            try ids.put(alloc, id.mapKey(), id);
        }
        var pending = self.pending.iterator();
        while (pending.next()) |kv| {
            if (kv.value_ptr.* == null or !std.mem.startsWith(u8, kv.key_ptr.*, prefix.items)) continue;
            const id = (try parseReverseKey(kv.key_ptr.*)).id;
            try ids.put(alloc, id.mapKey(), id);
        }
        var values = ids.valueIterator();
        while (values.next()) |id| _ = try writer.delete(self, id.*, document, .new_or_building);
    }

    /// Borrowed output stays valid through deinit; no later writes are allowed.
    /// Append these arrays to the same batch as primary rows and HA/replay.
    /// This does not commit, establish readiness, or release the apply fence.
    pub fn seal(self: *Staged) !Effects {
        try self.requireOpen();
        errdefer self.state = .failed;
        var write_count: usize = 0;
        var values = self.pending.valueIterator();
        while (values.next()) |value| if (value.* != null) {
            write_count += 1;
        };
        const writes = try self.arena.allocator().alloc(docstore.KVPair, write_count);
        const deletes = try self.arena.allocator().alloc([]const u8, self.pending.count() - write_count);
        var entries = self.pending.iterator();
        var wi: usize = 0;
        var di: usize = 0;
        while (entries.next()) |entry| {
            if (entry.value_ptr.*) |value| {
                writes[wi] = .{ .key = entry.key_ptr.*, .value = value };
                wi += 1;
            } else {
                deletes[di] = entry.key_ptr.*;
                di += 1;
            }
        }
        self.state = .sealed;
        return .{ .writes = writes, .deletes = deletes };
    }
};

const schema = @import("../schema.zig");
const schema_registry = @import("schema_registry.zig");
const mapper = @import("document_mapper.zig");
const docstore = @import("../docstore.zig");

fn testPlan(alloc: Allocator) !plans.View {
    const columns = [_]schema.RelationalColumn{
        .{ .name = "id", .path = "id", .column_type = .integer },
        .{ .name = "tenant", .path = "tenant", .column_type = .integer },
        .{ .name = "label", .path = "label", .column_type = .string, .allows_null = true },
    };
    var registry = try schema_registry.Registry.initCloned(alloc, std.testing.io, .{ .version = 7, .storage_mode = .relational, .relational_columns = &columns });
    defer registry.deinit();
    var view = registry.acquire().?;
    defer view.release();
    return plans.View.init(alloc, view, &.{
        .{ .name = "by_tenant_id", .generation = 3, .slot = 0, .keys = &.{ .{ .column = "tenant" }, .{ .column = "id", .direction = .desc } } },
        .{ .name = "by_label", .generation = 3, .slot = 1, .keys = &.{.{ .column = "label", .collation = "ci" }} },
    });
}

fn testRow(alloc: Allocator, plan: plans.View, document: []const u8, json: []const u8) !mapper.PreparedRelationalWrite {
    const view = plan.schemaView();
    var row = try mapper.PreparedRelationalWrite.init(alloc, document, json, null, view.tableSchema().*, view.physicalLayout());
    errdefer row.deinit(alloc);
    try row.finalizeMetadata(1);
    return row;
}

const CountingTxn = struct {
    txn: *docstore.DocStore.Txn,
    writes: usize = 0,
    deletes: usize = 0,
    fail_write: ?usize = null,

    pub fn get(self: *@This(), key: []const u8) ![]const u8 {
        return self.txn.get(key);
    }
    pub fn put(self: *@This(), key: []const u8, value: []const u8) !void {
        self.writes += 1;
        if (self.fail_write == self.writes) return error.OutOfMemory;
        try self.txn.put(key, value);
    }
    pub fn delete(self: *@This(), key: []const u8) !void {
        self.deletes += 1;
        try self.txn.delete(key);
    }
};

fn testMutations(comptime Backend: type) !void {
    const alloc = std.testing.allocator;
    var backend = Backend.init(alloc, .{});
    defer backend.close();
    var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
    defer runtime.deinit();
    var store = try docstore.DocStore.openRuntime(alloc, &runtime);
    defer store.close();
    var plan = try testPlan(alloc);
    defer plan.release();
    const document = "doc\x00:1";
    const primary_key = try internal.relationalRowKeyAlloc(alloc, document);
    defer alloc.free(primary_key);
    var old = try testRow(alloc, plan, document, "{\"id\":7,\"tenant\":1,\"label\":\"Alpha\"}");
    defer old.deinit(alloc);
    var updated = try testRow(alloc, plan, document, "{\"id\":8,\"tenant\":1,\"label\":\"Beta\"}");
    defer updated.deinit(alloc);
    var batch = plans.Batch.init(alloc, plan);
    defer batch.deinit();
    _ = try batch.appendPrepared(&old);
    _ = try batch.appendPrepared(&updated);
    var writer = Writer.init(alloc);
    defer writer.deinit();
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try txn.put(primary_key, old.packed_row);
        var counted = CountingTxn{ .txn = &txn };
        try writer.upsertPrepared(&counted, plan, &batch, 0, document, .new_or_building);
        try std.testing.expectEqual(@as(usize, 4), counted.writes);
        try txn.commit();
    }
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        try txn.put(primary_key, updated.packed_row);
        var counted = CountingTxn{ .txn = &txn, .fail_write = 2 };
        try std.testing.expectError(error.OutOfMemory, writer.upsertPrepared(&counted, plan, &batch, 1, document, .indexed));
        // The caller aborts primary and every partial index effect together.
    }
    {
        var txn = try store.beginReadTxn();
        defer txn.abort();
        try std.testing.expectEqualSlices(u8, old.packed_row, try txn.get(primary_key));
        for (plan.boundIndexes(), 0..) |index, i| {
            writer.reset();
            try appendReverseKey(alloc, &writer.reverse_key, index.id(), document);
            const reverse = try txn.get(writer.reverse_key.items);
            try std.testing.expectEqualSlices(u8, (try batch.key(0, i)).bytes, try reverseTuple(writer.reverse_key.items, reverse));
            try appendForwardFromReverse(alloc, &writer.old_forward, writer.reverse_key.items, reverse);
            try std.testing.expectEqualStrings("", try txn.get(writer.old_forward.items));
        }
    }
    // Replaying an identical logical index key produces no index mutations.
    try writer.reserve(internal.encodedComponentLen(document), 64);
    {
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        var counted = CountingTxn{ .txn = &txn };
        var no_alloc = std.testing.FailingAllocator.init(alloc, .{ .fail_index = 0, .resize_fail_index = 0 });
        writer.alloc = no_alloc.allocator();
        defer writer.alloc = alloc;
        for (0..200) |_| try writer.upsertPrepared(&counted, plan, &batch, 0, document, .indexed);
        try std.testing.expectEqual(@as(usize, 0), counted.writes);
        try std.testing.expectEqual(@as(usize, 0), counted.deletes);
        try std.testing.expectEqual(@as(usize, 0), no_alloc.allocations);
    }
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try txn.put(primary_key, updated.packed_row);
        var counted = CountingTxn{ .txn = &txn };
        try writer.upsertPrepared(&counted, plan, &batch, 1, document, .indexed);
        try std.testing.expectEqual(@as(usize, 4), counted.writes);
        try std.testing.expectEqual(@as(usize, 2), counted.deletes);
        try txn.commit();
    }
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try txn.delete(primary_key);
        for (plan.boundIndexes()) |index| {
            try std.testing.expectEqual(Effect.deleted, try writer.delete(&txn, index.id(), document, .indexed));
            try std.testing.expectEqual(Effect.unchanged, try writer.delete(&txn, index.id(), document, .new_or_building));
            try std.testing.expectError(error.MissingRelationalIndexReverse, writer.delete(&txn, index.id(), document, .indexed));
        }
        try txn.commit();
    }
    {
        var txn = try store.beginReadTxn();
        defer txn.abort();
        try std.testing.expectError(error.NotFound, txn.get(primary_key));
        for (plan.boundIndexes(), 0..) |index, i| {
            writer.reset();
            try appendReverseKey(alloc, &writer.reverse_key, index.id(), document);
            try std.testing.expectError(error.NotFound, txn.get(writer.reverse_key.items));
            const owner = try parseReverseKey(writer.reverse_key.items);
            for (0..2) |row| {
                writer.old_forward.clearRetainingCapacity();
                try appendForwardEncoded(alloc, &writer.old_forward, index.id(), (try batch.key(row, i)).bytes, owner.document_component);
                try std.testing.expectError(error.NotFound, txn.get(writer.old_forward.items));
            }
        }
    }
}

fn testRangeRepair(comptime Backend: type) !void {
    const alloc = std.testing.allocator;
    var backend = Backend.init(alloc, .{});
    defer backend.close();
    var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
    defer runtime.deinit();
    var store = try docstore.DocStore.openRuntime(alloc, &runtime);
    defer store.close();
    var plan = try testPlan(alloc);
    defer plan.release();
    var row = try testRow(alloc, plan, "doc", "{\"id\":7,\"tenant\":1,\"label\":\"Alpha\"}");
    defer row.deinit(alloc);
    var batch = plans.Batch.init(alloc, plan);
    defer batch.deinit();
    _ = try batch.appendPrepared(&row);
    var writer = Writer.init(alloc);
    defer writer.deinit();
    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        for (0..300) |i| {
            var name: [16]u8 = undefined;
            const document = try std.fmt.bufPrint(&name, "doc{d:0>3}\x00", .{i});
            try writer.upsertPrepared(&txn, plan, &batch, 0, document, .new_or_building);
            // Simulate page-split namespace placement: selected documents own
            // reverse records, but their global forward companions are absent.
            if (i >= 120 and i < 180) for (plan.boundIndexes()) |index| {
                writer.reset();
                try appendReverseKey(alloc, &writer.reverse_key, index.id(), document);
                const reverse = try txn.get(writer.reverse_key.items);
                try appendForwardFromReverse(alloc, &writer.old_forward, writer.reverse_key.items, reverse);
                try txn.delete(writer.old_forward.items);
            };
        }
        try txn.commit();
    }
    var repair = RangeRepair{ .alloc = alloc };
    defer repair.deinit();
    var calls: usize = 0;
    var effects: usize = 0;
    while (!repair.done()) {
        effects += try repair.step(&store, "doc120\x00", "doc180\x00");
        calls += 1;
        try std.testing.expect(calls < 12);
    }
    try std.testing.expect(calls >= 3);
    try std.testing.expectEqual(@as(usize, 600), effects);
    const stored = try store.scanPrefix(alloc, forward_namespace);
    defer docstore.DocStore.freeResults(alloc, stored);
    try std.testing.expectEqual(@as(usize, 120), stored.len);
    for (stored) |entry| {
        const owner = try forwardOwnership(entry.key);
        try std.testing.expect(owner.tuple.len != 0);
    }
    var replay = RangeRepair{ .alloc = alloc };
    defer replay.deinit();
    while (!replay.done()) try std.testing.expectEqual(@as(usize, 0), try replay.step(&store, "doc120\x00", "doc180\x00"));
    var corrupt = try alloc.dupe(u8, stored[0].key);
    defer alloc.free(corrupt);
    @memset(corrupt[corrupt.len - 4 ..], 0xff);
    try std.testing.expectError(error.InvalidRelationalIndexForwardKey, forwardOwnership(corrupt));
}

test "relational index records reconcile split ownership in bounded idempotent pages" {
    try testRangeRepair(@import("../mem_backend.zig").Backend);
    try testRangeRepair(@import("../lsm_backend.zig").Backend);
}

test "relational index records maintain atomic primary forward and reverse pairs on memory and LSM" {
    try testMutations(@import("../mem_backend.zig").Backend);
    try testMutations(@import("../lsm_backend.zig").Backend);
}

test "relational index records retain document range ownership and reconstruct compact companions" {
    const alloc = std.testing.allocator;
    var plan = try testPlan(alloc);
    defer plan.release();
    const document = "ab\x00c";
    var row = try testRow(alloc, plan, document, "{\"id\":7,\"tenant\":1,\"label\":\"Alpha\"}");
    defer row.deinit(alloc);
    var batch = plans.Batch.init(alloc, plan);
    defer batch.deinit();
    _ = try batch.appendPrepared(&row);
    var reverse_key = std.ArrayList(u8).empty;
    defer reverse_key.deinit(alloc);
    var reverse_value = std.ArrayList(u8).empty;
    defer reverse_value.deinit(alloc);
    var forward = std.ArrayList(u8).empty;
    defer forward.deinit(alloc);
    const index = plan.boundIndexes()[1]; // composite integer index after name sorting
    const tuple = (try batch.key(0, 1)).bytes;
    try std.testing.expectEqual(@as(usize, 18), tuple.len);
    try appendReverseKey(alloc, &reverse_key, index.id(), document);
    try appendReverseValue(alloc, &reverse_value, reverse_key.items, tuple);
    try appendForwardFromReverse(alloc, &forward, reverse_key.items, reverse_value.items);
    try std.testing.expect(internal.isRelationalIndexReverseKey(reverse_key.items));
    try std.testing.expect(!internal.isStoredDocumentRowKey(reverse_key.items));
    try std.testing.expect(!internal.isInternalUserKey(forward.items));
    const prefix = try internal.documentExactPrefixAlloc(alloc, document);
    defer alloc.free(prefix);
    try std.testing.expect(std.mem.startsWith(u8, reverse_key.items, prefix));
    const lower = try internal.documentRangeLowerAlloc(alloc, "ab");
    defer alloc.free(lower);
    const upper = (try internal.documentRangeUpperAlloc(alloc, "ab")).?;
    defer alloc.free(upper);
    try std.testing.expect(std.mem.order(u8, reverse_key.items, lower) != .lt);
    try std.testing.expect(std.mem.lessThan(u8, reverse_key.items, upper));
    const parsed = try parseForward(forward.items, index);
    try std.testing.expectEqualSlices(u8, tuple, parsed.tuple);
    const raw = try internal.decodeBodyAlloc(alloc, parsed.document_component[0 .. parsed.document_component.len - 2]);
    defer alloc.free(raw);
    try std.testing.expectEqualStrings(document, raw);
    try std.testing.expectEqual(forward_prefix_len + 18 + internal.encodedComponentLen(document) + 4, forward.items.len);
    // The checksum binds a reverse value to its document and generation.
    reverse_key.items[reverse_key.items.len - 1] ^= 1;
    try std.testing.expectError(error.RelationalIndexReverseChecksumMismatch, reverseTuple(reverse_key.items, reverse_value.items));
    reverse_key.items[reverse_key.items.len - 1] ^= 1;
    reverse_value.items[2] ^= 1;
    try std.testing.expectError(error.RelationalIndexReverseChecksumMismatch, reverseTuple(reverse_key.items, reverse_value.items));
}

test "relational index records reject cross generation forward keys and stale batches" {
    const alloc = std.testing.allocator;
    var plan = try testPlan(alloc);
    defer plan.release();
    var other = try testPlan(alloc);
    defer other.release();
    var batch = plans.Batch.init(alloc, plan);
    defer batch.deinit();
    var writer = Writer.init(alloc);
    defer writer.deinit();
    const NeverTxn = struct {
        pub fn get(_: *@This(), _: []const u8) anyerror![]const u8 {
            return error.UnexpectedStoreAccess;
        }
        pub fn put(_: *@This(), _: []const u8, _: []const u8) anyerror!void {
            return error.UnexpectedStoreAccess;
        }
        pub fn delete(_: *@This(), _: []const u8) anyerror!void {
            return error.UnexpectedStoreAccess;
        }
    };
    var txn = NeverTxn{};
    try std.testing.expectError(error.PreparedGenerationChanged, writer.upsertPrepared(&txn, other, &batch, 0, "doc", .new_or_building));
    const prefix = try forwardPrefix(.{ .generation = 100, .slot = 0 });
    try std.testing.expectError(error.RelationalIndexGenerationMismatch, parseForward(&prefix, plan.boundIndexes()[0]));
}

fn testStagedMutations(comptime Backend: type) !void {
    const alloc = std.testing.allocator;
    var backend = Backend.init(alloc, .{});
    defer backend.close();
    var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
    defer runtime.deinit();
    var store = try docstore.DocStore.openRuntime(alloc, &runtime);
    defer store.close();
    var plan = try testPlan(alloc);
    defer plan.release();
    const document = "same-document";
    var old = try testRow(alloc, plan, document, "{\"id\":7,\"tenant\":1,\"label\":\"Alpha\"}");
    defer old.deinit(alloc);
    var updated = try testRow(alloc, plan, document, "{\"id\":8,\"tenant\":2,\"label\":\"Beta\"}");
    defer updated.deinit(alloc);
    var batch = plans.Batch.init(alloc, plan);
    defer batch.deinit();
    _ = try batch.appendPrepared(&old);
    _ = try batch.appendPrepared(&updated);
    var writer = Writer.init(alloc);
    defer writer.deinit();
    var base = try store.beginReadTxn();
    defer base.abort();
    var staged = Staged.init(alloc, &base);
    defer staged.deinit();
    try staged.upsertPrepared(&writer, plan, &batch, 0, document, .new_or_building);
    try staged.upsertPrepared(&writer, plan, &batch, 1, document, .indexed);
    for (plan.boundIndexes()) |index| {
        try std.testing.expectEqual(Effect.deleted, try staged.deleteIndex(&writer, index.id(), document, .indexed));
        try std.testing.expectEqual(Effect.unchanged, try staged.deleteIndex(&writer, index.id(), document, .new_or_building));
    }
    try staged.upsertPrepared(&writer, plan, &batch, 1, document, .new_or_building);
    const effects = try staged.seal();
    try std.testing.expectEqual(@as(usize, 4), effects.writes.len);
    try std.testing.expectEqual(@as(usize, 2), effects.deletes.len);
    try std.testing.expectError(error.RelationalIndexStageClosed, staged.seal());
    try std.testing.expectError(error.RelationalIndexStageClosed, staged.upsertPrepared(&writer, plan, &batch, 0, document, .indexed));
    for (effects.writes) |kv| {
        try std.testing.expectError(error.NotFound, base.get(kv.key));
        for (effects.deletes) |key| try std.testing.expect(!std.mem.eql(u8, kv.key, key));
    }
    const primary_key = try internal.relationalRowKeyAlloc(alloc, document);
    defer alloc.free(primary_key);
    const outbox_key = "\x00\x00__metadata__:index_stage_test_outbox";
    const writes = try std.mem.concat(alloc, docstore.KVPair, &.{ effects.writes, &.{
        .{ .key = primary_key, .value = updated.packed_row },
        .{ .key = outbox_key, .value = "pending" },
    } });
    defer alloc.free(writes);
    try store.putBatchWithReplay(std.testing.io, writes, effects.deletes, null);
    var committed = try store.beginReadTxn();
    defer committed.abort();
    try std.testing.expectEqualSlices(u8, updated.packed_row, try committed.get(primary_key));
    try std.testing.expectEqualStrings("pending", try committed.get(outbox_key));
    for (effects.writes) |kv| try std.testing.expectEqualSlices(u8, kv.value, try committed.get(kv.key));
    for (effects.deletes) |key| try std.testing.expectError(error.NotFound, committed.get(key));
    // Staging against committed base data elides unchanged rows entirely.
    var no_op = Staged.init(alloc, &committed);
    defer no_op.deinit();
    try no_op.upsertPrepared(&writer, plan, &batch, 1, document, .indexed);
    const empty = try no_op.seal();
    try std.testing.expectEqual(@as(usize, 0), empty.writes.len + empty.deletes.len);
    // A later invalid row must prevent exporting earlier successful effects.
    var failed = Staged.init(alloc, &committed);
    defer failed.deinit();
    try failed.upsertPrepared(&writer, plan, &batch, 0, document, .indexed);
    try std.testing.expectError(error.MissingRelationalIndexReverse, failed.deleteIndex(&writer, .{ .generation = 100, .slot = 0 }, document, .indexed));
    try std.testing.expectError(error.RelationalIndexStageClosed, failed.seal());
}

test "relational index records stage coalesced effects for one primary and outbox batch" {
    try testStagedMutations(@import("../mem_backend.zig").Backend);
    try testStagedMutations(@import("../lsm_backend.zig").Backend);
}

fn testStagedAllocationFailure(alloc: Allocator, base: *docstore.DocStore.Txn, plan: plans.View, batch: *const plans.Batch) !void {
    var staged = Staged.init(alloc, base);
    defer staged.deinit();
    var writer = Writer.init(alloc);
    defer writer.deinit();
    staged.upsertPrepared(&writer, plan, batch, 0, "doc", .new_or_building) catch |err| {
        try std.testing.expectError(error.RelationalIndexStageClosed, staged.seal());
        return err;
    };
    _ = try staged.seal();
}

test "relational index records staging releases and poisons every allocation failure" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
    defer runtime.deinit();
    var store = try docstore.DocStore.openRuntime(alloc, &runtime);
    defer store.close();
    var base = try store.beginReadTxn();
    defer base.abort();
    var plan = try testPlan(alloc);
    defer plan.release();
    var row = try testRow(alloc, plan, "doc", "{\"id\":7,\"tenant\":1,\"label\":\"Alpha\"}");
    defer row.deinit(alloc);
    var batch = plans.Batch.init(alloc, plan);
    defer batch.deinit();
    _ = try batch.appendPrepared(&row);
    try std.testing.checkAllAllocationFailures(alloc, testStagedAllocationFailure, .{ &base, plan, &batch });
}

test "relational index records preserve primary and index pairs across LSM reopen update and delete" {
    const alloc = std.testing.allocator;
    const Backend = @import("../lsm_backend.zig").Backend;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buffer: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buffer, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    const document = "persisted";
    const primary = try internal.relationalRowKeyAlloc(alloc, document);
    defer alloc.free(primary);
    for (0..4) |phase| {
        var backend = try Backend.open(alloc, path, .{ .flush_threshold = 2, .wal_enabled = true });
        defer backend.close();
        var runtime = try backend.runtimeStore(alloc, .{ .name = "docs" });
        defer runtime.deinit();
        var store = try docstore.DocStore.openRuntime(alloc, &runtime);
        defer store.close();
        // Recompile a fresh runtime plan: only physical IDs/bytes survive open.
        var plan = try testPlan(alloc);
        defer plan.release();
        var old = try testRow(alloc, plan, document, "{\"id\":7,\"tenant\":1,\"label\":\"Alpha\"}");
        defer old.deinit(alloc);
        var updated = try testRow(alloc, plan, document, "{\"id\":8,\"tenant\":2,\"label\":\"Beta\"}");
        defer updated.deinit(alloc);
        var batch = plans.Batch.init(alloc, plan);
        defer batch.deinit();
        _ = try batch.appendPrepared(&old);
        _ = try batch.appendPrepared(&updated);
        var writer = Writer.init(alloc);
        defer writer.deinit();
        if (phase > 0) {
            var read = try store.beginReadTxn();
            defer read.abort();
            if (phase == 3) {
                try std.testing.expectError(error.NotFound, read.get(primary));
            } else try std.testing.expectEqualSlices(u8, if (phase == 1) old.packed_row else updated.packed_row, try read.get(primary));
            for (plan.boundIndexes(), 0..) |index, i| {
                writer.reset();
                try appendReverseKey(alloc, &writer.reverse_key, index.id(), document);
                if (phase == 3) {
                    try std.testing.expectError(error.NotFound, read.get(writer.reverse_key.items));
                } else {
                    const value = try read.get(writer.reverse_key.items);
                    try std.testing.expectEqualSlices(u8, (try batch.key(phase - 1, i)).bytes, try reverseTuple(writer.reverse_key.items, value));
                }
                const owner = try parseReverseKey(writer.reverse_key.items);
                for (0..2) |row| {
                    writer.old_forward.clearRetainingCapacity();
                    try appendForwardEncoded(alloc, &writer.old_forward, index.id(), (try batch.key(row, i)).bytes, owner.document_component);
                    if (phase != 3 and row == phase - 1) {
                        try std.testing.expectEqualStrings("", try read.get(writer.old_forward.items));
                    } else try std.testing.expectError(error.NotFound, read.get(writer.old_forward.items));
                }
            }
        }
        if (phase == 3) continue;
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        if (phase < 2) {
            try txn.put(primary, if (phase == 0) old.packed_row else updated.packed_row);
            try writer.upsertPrepared(&txn, plan, &batch, phase, document, if (phase == 0) .new_or_building else .indexed);
        } else {
            try txn.delete(primary);
            for (plan.boundIndexes()) |index| _ = try writer.delete(&txn, index.id(), document, .indexed);
        }
        try txn.commit();
    }
}
