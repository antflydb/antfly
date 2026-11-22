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

//! Durable outbound consumer authority. Delivery, client ACK and native
//! reclamation are distinct. Private runtime proofs certify enrollment, cuts,
//! page completeness and cleanup; opaque cursors never authorize access.
const std = @import("std");
const A = std.mem.Allocator;
pub const Id = [16]u8;
pub const Digest = [32]u8;
pub const Namespace = [24]u8;
pub const protocol_version: u16 = 120;
pub const verified_bootstrap_protocol_version: u16 = 122;
pub const qualified_bootstrap_protocol_version: u16 = 123;
pub const slot_snapshot_start_protocol_version: u16 = 124;
pub const provider_history_protocol_version: u16 = 125;
pub const physical_history_protocol_version: u16 = 126;
pub const shared_namespace_plan_protocol_version: u16 = 127;
pub const slot_recreation_protocol_version: u16 = 133;
pub const retired_slot_root_protocol_version: u16 = 134;
pub const native_collection_protocol_version: u16 = 135;
pub const absent_public_root_protocol_version: u16 = 136;
pub const root_intent = @import("root_control_intent.zig");
pub const pg_slot = @import("pg_slot.zig");
pub const max_tables = 64;
pub const artifact_event: u8 = 2;
pub const max_roots = 4096;
pub const max_cursors = 128;
pub const max_provider_context_bytes = 64 * 1024;
pub const max_advances = 128;
pub const max_definition_bytes = 128 * 1024;
pub const max_cursor_bytes = 1024 * 1024;
pub const max_state_bytes = 8 * 1024 * 1024;
pub const max_lease_seconds = 7 * 24 * 60 * 60;
pub const max_bootstrap_seconds = 60 * 60;
fn nz(value: anytype) bool {
    return !std.mem.allEqual(u8, &value, 0);
}
fn text(value: []const u8, max: usize, empty: bool) bool {
    return (empty or value.len != 0) and value.len <= max and std.unicode.utf8ValidateSlice(value);
}
fn equal(left: anytype, right: @TypeOf(left)) bool {
    return std.mem.eql(u8, &left, &right);
}
pub fn hash(value: anytype) Digest {
    const Output = struct {
        writer: std.Io.Writer = .{ .vtable = &.{ .drain = drain, .flush = std.Io.Writer.noopFlush }, .buffer = &.{} },
        sha: std.crypto.hash.sha2.Sha256 = .init(.{}),
        fn drain(writer: *std.Io.Writer, data: []const []const u8, splat: usize) std.Io.Writer.Error!usize {
            const self: *@This() = @alignCast(@fieldParentPtr("writer", writer));
            for (data[0 .. data.len - 1]) |bytes| self.sha.update(bytes);
            for (0..splat) |_| self.sha.update(data[data.len - 1]);
            return std.Io.Writer.countSplat(data, splat);
        }
    };
    var output: Output = .{};
    std.json.Stringify.value(value, .{}, &output.writer) catch unreachable;
    return output.sha.finalResult();
}
fn encodedSize(value: anytype) u64 {
    var buffer: [256]u8 = undefined;
    var out = std.Io.Writer.Discarding.init(&buffer);
    std.json.Stringify.value(value, .{}, &out.writer) catch unreachable;
    return out.fullCount();
}
fn validNumber(value: []const u8) bool {
    if (!text(value, 4096, false)) return false;
    var scanner = std.json.Scanner.initCompleteInput(std.heap.page_allocator, value);
    defer scanner.deinit();
    if ((scanner.next() catch return false) != .number) return false;
    return (scanner.next() catch return false) == .end_of_document;
}
pub fn validProviderContext(value: []const u8) bool {
    if (value.len > max_provider_context_bytes) return false;
    for (value) |byte| if (!std.ascii.isAlphanumeric(byte) and byte != '-' and byte != '_') return false;
    return true;
}
pub const Start = enum { now, snapshot_then_changes };
pub const Projection = enum { keys_and_versions, rows };
pub const Contract = enum { native_document, native_relational, writable_iceberg_rows, external_snapshot_lifecycle };
pub fn catalogBacked(contract: Contract) bool {
    return contract == .writable_iceberg_rows or contract == .external_snapshot_lifecycle;
}
pub const Predicate = struct { field: []const u8, value: std.json.Value };
pub const Table = struct {
    table_id: u64,
    binding: Digest,
    /// Authorization/projection generation, independent of root retention configuration.
    policy_revision: u64,
    contract: Contract,
    fields: []const []const u8 = &.{},
    predicates: []const Predicate = &.{},
};
pub const Definition = struct {
    owner: Digest,
    authorization: Digest,
    tables: []const Table,
    start: Start = .now,
    projection: Projection = .keys_and_versions,
    /// document_committed=1, artifact_published=2, publication_activated=4.
    events: u8 = 1,
    cursor_window: u16 = 64,
    cursor_byte_limit: u64 = max_cursor_bytes,
    pub fn validate(self: Definition) !void {
        if (!nz(self.owner) or !nz(self.authorization) or self.tables.len == 0 or self.tables.len > max_tables or
            self.events == 0 or self.events & ~@as(u8, 7) != 0 or self.cursor_window == 0 or self.cursor_window > max_cursors or
            self.cursor_byte_limit < 4096 or self.cursor_byte_limit > max_cursor_bytes) return error.InvalidChangefeed;
        for (self.tables, 0..) |table, index| {
            if (self.events == artifact_event and (self.start != .now or catalogBacked(table.contract) or
                table.fields.len != 0 or table.predicates.len != 0)) return error.InvalidChangefeed;
            if (table.table_id == 0 or !nz(table.binding) or table.fields.len > 64 or table.predicates.len > 16 or
                (index != 0 and self.tables[index - 1].table_id >= table.table_id) or
                (table.contract == .external_snapshot_lifecycle and self.events & 1 != 0)) return error.InvalidChangefeed;
            for (table.fields, 0..) |field, i| if (!text(field, 255, false) or (i != 0 and std.mem.order(u8, table.fields[i - 1], field) != .lt)) return error.InvalidChangefeed;
            for (table.predicates, 0..) |predicate, i| {
                if (!text(predicate.field, 255, false) or (i != 0 and std.mem.order(u8, table.predicates[i - 1].field, predicate.field) != .lt)) return error.InvalidChangefeed;
                switch (predicate.value) {
                    .array, .object => return error.InvalidChangefeed,
                    .string => |value| if (!text(value, 4096, true)) return error.InvalidChangefeed,
                    .number_string => |value| if (!validNumber(value)) return error.InvalidChangefeed,
                    .float => |value| if (!std.math.isFinite(value)) return error.InvalidChangefeed,
                    else => {},
                }
            }
        }
        if (encodedSize(self) > max_definition_bytes) return error.InvalidChangefeed;
    }
    fn usesProviderHistory(self: Definition) bool {
        if (self.events != 1) return true;
        for (self.tables) |table| if (catalogBacked(table.contract)) return true;
        return false;
    }
    pub fn identity(self: Definition) Digest {
        return hash(.{ .domain = "antfly:changefeed-definition:v1", .definition = self });
    }
    pub fn contains(self: Definition, table_id: u64) bool {
        for (self.tables) |table| if (table.table_id == table_id) return true;
        return false;
    }
};
pub const Position = struct {
    /// Fully consumed frames. event>0 is an ordinal in sequence+1; a root ACK
    /// may reclaim only sequence, never the partially consumed next frame.
    sequence: u64 = 0,
    event: u32 = 0,
    pub fn order(self: Position, other: Position) std.math.Order {
        const first = std.math.order(self.sequence, other.sequence);
        return if (first != .eq) first else std.math.order(self.event, other.event);
    }
};
pub const Snapshot = struct { id: Id, digest: Digest, expires_at: u64 };
pub const BootstrapPosition = struct { page: u64 = 0, continuation: []const u8 = "", done: bool = false };
pub const PhysicalRoot = struct {
    group_id: u64,
    range_id: u64,

    pub fn valid(self: PhysicalRoot) bool {
        return self.group_id != 0 and self.range_id != 0;
    }
};

pub const SplitHandoff = struct {
    predecessor: PhysicalRoot,
    successor: PhysicalRoot,
    cut: struct {
        namespace: Namespace,
        transition: u64,
        attempt: u64,
        successor_group: u64,
        term: u64,
        index: u64,
        latest: u64,
        reclaimed: u64,
        root_generation: u64,
        reserved_bytes: u64,
        boundary: Digest,
    },

    fn validate(self: SplitHandoff, namespace: Namespace) !void {
        if (!self.predecessor.valid() or !self.successor.valid() or self.predecessor.group_id == self.successor.group_id or
            self.cut.successor_group != self.successor.group_id or !equal(self.cut.namespace, namespace) or
            self.cut.transition == 0 or self.cut.attempt == 0 or self.cut.term == 0 or self.cut.index == 0 or
            self.cut.reclaimed > self.cut.latest or !nz(self.cut.boundary))
            return error.InvalidChangefeed;
    }
};

pub const MergeDrain = struct {
    donor: PhysicalRoot,
    receiver: PhysicalRoot,
    transition: u64,
    term: u64,
    index: u64,
    latest: u64,
    root_generation: u64,

    fn validate(self: MergeDrain) !void {
        if (!self.donor.valid() or !self.receiver.valid() or self.donor.group_id == self.receiver.group_id or
            self.transition == 0 or self.term == 0 or self.index == 0)
            return error.InvalidChangefeed;
    }
};

fn writePhysicalRootRecord(value: anytype, writer: anytype) @TypeOf(writer.*).Error!void {
    try writer.beginObject();
    inline for (@typeInfo(@TypeOf(value)).@"struct".field_names) |name| {
        if (comptime std.mem.eql(u8, name, "physical")) {
            if (value.physical) |physical| {
                try writer.objectField(name);
                try writer.write(physical);
            }
        } else if (comptime (std.mem.eql(u8, name, "split_handoff") or std.mem.eql(u8, name, "split_grant_sequence") or std.mem.eql(u8, name, "split_consumer_epoch") or std.mem.eql(u8, name, "storage_namespace") or std.mem.eql(u8, name, "merge_drain") or std.mem.eql(u8, name, "native_collected_through"))) {
            if (@field(value, name)) |handoff| {
                try writer.objectField(name);
                try writer.write(handoff);
            }
        } else {
            try writer.objectField(name);
            try writer.write(@field(value, name));
        }
    }
    try writer.endObject();
}

pub const Root = struct {
    namespace: Namespace,
    ownership_epoch: u64,
    grant_sequence: u64,
    consumer_epoch: u64 = 0,
    start: u64 = 0,
    delivered: Position = .{},
    acknowledged: Position = .{},
    /// Whole-frame ACK propagated to this native consumer. Other consumers
    /// may still retain these frames in the shared physical log.
    reclaimed_through: u64 = 0,
    /// Completion of bounded collection passes, separate from the root ACK.
    native_collected_through: ?u64 = null,
    renewed_through: u64 = 0,
    snapshot: ?Snapshot = null,
    snapshot_delivered: BootstrapPosition = .{},
    snapshot_acknowledged: BootstrapPosition = .{},
    snapshot_released: bool = false,
    released: bool = false,
    pending_control: ?root_intent.State = null,
    last_control: ?root_intent.Settlement = null,
    last_reconciliation: ?root_intent.Reconciliation = null,
    physical: ?PhysicalRoot = null,
    split_handoff: ?SplitHandoff = null,
    split_grant_sequence: ?u64 = null,
    split_consumer_epoch: ?u64 = null,
    storage_namespace: ?Namespace = null,
    merge_drain: ?MergeDrain = null,

    pub fn storageNamespace(self: Root) Namespace {
        return self.storage_namespace orelse self.namespace;
    }

    pub fn mergeDrained(self: Root) bool {
        const drain = self.merge_drain orelse return false;
        return self.acknowledged.order(.{ .sequence = drain.latest }) == .eq and
            self.reclaimed_through >= drain.latest and self.pending_control == null and
            (self.snapshot == null or self.snapshot_released);
    }

    pub fn transitionLimit(self: Root) ?u64 {
        if (self.merge_drain) |drain| return drain.latest;
        if (self.split_handoff) |handoff| return handoff.cut.latest;
        return null;
    }

    pub fn jsonStringify(self: Root, writer: anytype) @TypeOf(writer.*).Error!void {
        return writePhysicalRootRecord(self, writer);
    }
};
pub fn splitStreamNamespace(namespace: Namespace, physical: PhysicalRoot) Namespace {
    var result = namespace;
    result[8..24].* = hash(.{ .domain = "antfly:changefeed-split-stream:v1", .namespace = namespace, .physical = physical })[0..16].*;
    return result;
}

pub const RootPlan = struct {
    namespace: Namespace,
    ownership_epoch: u64,
    physical: ?PhysicalRoot = null,
    storage_namespace: ?Namespace = null,

    pub fn jsonStringify(self: RootPlan, writer: anytype) @TypeOf(writer.*).Error!void {
        return writePhysicalRootRecord(self, writer);
    }
};

/// Catalog-backed history has one logical root per table incarnation and
/// remains stable across placement or index topology changes.
pub fn catalogRootNamespace(table: Table) Namespace {
    var namespace: Namespace = undefined;
    std.mem.writeInt(u64, namespace[0..8], table.table_id, .big);
    const identity = hash(.{ .domain = "antfly:catalog-history-root:v1", .binding = table.binding });
    namespace[8..24].* = identity[0..16].*;
    return namespace;
}
pub const Advance = struct { namespace: Namespace, position: Position, snapshot: ?BootstrapPosition = null };
pub const Delivery = struct {
    sequence: u64,
    before: Digest,
    cursor: Digest,
    authority: Digest,
    issued_at: u64,
    advances: []const Advance,
    /// Private versioned base64url continuation, never a public credential or
    /// tuple WAL. Bound to this receipt and retained with its ACK checkpoint.
    provider_context: []const u8 = "",
    pub fn identity(self: Delivery, id: Id, epoch: u64) Digest {
        if (self.provider_context.len != 0) return hash(.{ .domain = "antfly:changefeed-cursor:v2", .id = id, .epoch = epoch, .sequence = self.sequence, .before = self.before, .authority = self.authority, .issued_at = self.issued_at, .advances = self.advances, .provider_context = self.provider_context });
        return hash(.{ .domain = "antfly:changefeed-cursor:v1", .id = id, .epoch = epoch, .sequence = self.sequence, .before = self.before, .authority = self.authority, .issued_at = self.issued_at, .advances = self.advances });
    }
};
pub const MaintenanceScan = struct {
    pub const limit = 32;
    ids: [limit]Id = undefined,
    count: usize = 0,
    /// Exclusive position of the last inspected row, including ineligible
    /// rows. A sparse or empty page must still make forward progress.
    next: ?Id = null,

    pub fn items(self: *const MaintenanceScan) []const Id {
        return self.ids[0..self.count];
    }
};
pub const Phase = enum { empty, enrolling, bootstrap, streaming, reset_required, closing, deleted };
pub const Reset = enum { none, policy_changed, table_retired, history_expired, bootstrap_expired, lease_expired, topology_lost, quota };
pub const Stamp = struct { incarnation: Digest, sequence: u64 };
pub const State = struct {
    version: u32 = 1,
    revision: u64 = 0,
    id: Id = @splat(0),
    epoch: u64 = 0,
    authority: Digest = @splat(0),
    authority_sequence: u64 = 0,
    definition: ?Definition = null,
    phase: Phase = .empty,
    created_at: u64 = 0,
    expires_at: u64 = 0,
    bootstrap_expires_at: u64 = 0,
    renewal_pending: bool = false,
    cut: Digest = @splat(0),
    roots: []const Root = &.{},
    latest_delivery: u64 = 0,
    acknowledged_delivery: u64 = 0,
    latest_cursor: Digest = @splat(0),
    acknowledged_cursor: Digest = @splat(0),
    latest_provider_context: []const u8 = "",
    acknowledged_provider_context: []const u8 = "",
    cursors: []const Delivery = &.{},
    reset_reason: Reset = .none,
    slot: ?pg_slot.State = null,
    /// New slots that reuse a name must reject offsets from all prior lives.
    slot_lsn_floor: u64 = 0,

    pub fn jsonStringify(self: State, writer: anytype) @TypeOf(writer.*).Error!void {
        try writer.beginObject();
        inline for (@typeInfo(State).@"struct".field_names) |name| {
            if (!std.mem.eql(u8, name, "slot_lsn_floor") or self.slot_lsn_floor != 0) {
                try writer.objectField(name);
                try writer.write(@field(self, name));
            }
        }
        try writer.endObject();
    }
    pub fn rootIndex(self: State, namespace: Namespace) !usize {
        var low: usize = 0;
        var high = self.roots.len;
        while (low < high) {
            const mid = low + (high - low) / 2;
            switch (std.mem.order(u8, &self.roots[mid].namespace, &namespace)) {
                .lt => low = mid + 1,
                .gt => high = mid,
                .eq => return mid,
            }
        }
        return error.ChangefeedConflict;
    }
    pub fn expiryDue(self: State, now: u64) bool {
        return switch (self.phase) {
            .enrolling, .bootstrap, .streaming => now >= self.expires_at or
                (self.phase != .streaming and self.bootstrap_expires_at != 0 and now >= self.bootstrap_expires_at),
            else => false,
        };
    }

    pub fn needsPressureProbe(self: State, now: u64) bool {
        if (self.slot != null or (self.phase != .streaming and self.phase != .bootstrap) or self.expiryDue(now)) return false;
        const definition = self.definition orelse return false;
        if (definition.events != 1) return false;
        for (definition.tables) |table| {
            if (table.contract == .native_document or table.contract == .native_relational) return true;
        }
        return false;
    }

    pub fn needsMaintenance(self: State, now: u64) bool {
        return switch (self.phase) {
            .closing => true,
            .bootstrap => self.bootstrapComplete() or self.expiryDue(now),
            .streaming => for (self.roots) |root| {
                if (root.snapshot != null and !root.snapshot_released) break true;
            } else self.renewal_pending or self.pendingCatalogAcknowledgment() != null or self.pendingNativeAcknowledgment() != null or self.expiryDue(now),
            .reset_required => for (self.roots) |root| {
                if (!root.released or (root.snapshot != null and !root.snapshot_released)) break true;
            } else if (self.slot) |slot| slot.spool.records.len != 0 else false,
            else => self.expiryDue(now),
        };
    }

    pub fn pendingNativeAcknowledgment(self: State) ?Root {
        const definition = self.definition orelse return null;
        if (self.slot != null or definition.events != 1) return null;
        for (self.roots) |root| {
            if (root.released or (root.native_collected_through orelse root.start) >= root.acknowledged.sequence) continue;
            const table_id = std.mem.readInt(u64, root.namespace[0..8], .big);
            for (definition.tables) |table| {
                if (table.table_id != table_id) continue;
                if (table.contract == .native_document or table.contract == .native_relational) return root;
                break;
            }
        }
        return null;
    }

    pub fn pendingCatalogAcknowledgment(self: State) ?Table {
        const definition = self.definition orelse return null;
        for (definition.tables) |table| {
            if (table.contract != .writable_iceberg_rows) continue;
            const root = self.roots[self.rootIndex(catalogRootNamespace(table)) catch continue];
            if (!root.released and root.reclaimed_through < root.acknowledged.sequence) return table;
        }
        return null;
    }

    pub fn bootstrapComplete(self: State) bool {
        if (self.phase != .bootstrap or self.roots.len == 0 or self.latest_delivery != self.acknowledged_delivery) return false;
        for (self.roots) |root| {
            if (root.snapshot == null or !root.snapshot_acknowledged.done or root.snapshot_released or root.released) return false;
        }
        return true;
    }

    /// Bind the cutover receipt to every durably acknowledged root snapshot.
    /// The issuing service obtains the snapshot pins from their native owners.
    pub fn bootstrapCompletionProof(self: State) !Digest {
        if (!self.bootstrapComplete()) return error.ChangefeedConflict;
        var digest = hash(.{
            .domain = "antfly:changefeed-bootstrap-completion:v1",
            .authority = self.authority,
            .feed = self.id,
            .epoch = self.epoch,
            .definition = (self.definition orelse return error.ChangefeedConflict).identity(),
            .cut = self.cut,
            .cursor = self.acknowledged_cursor,
            .delivery = self.acknowledged_delivery,
        });
        for (self.roots) |root| {
            const snapshot = root.snapshot.?;
            digest = hash(.{
                .previous = digest,
                .namespace = root.namespace,
                .ownership_epoch = root.ownership_epoch,
                .consumer_epoch = root.consumer_epoch,
                .start = root.start,
                .snapshot = snapshot,
                .position = root.snapshot_acknowledged,
            });
        }
        return digest;
    }

    pub fn consumerId(self: State) Id {
        return hash(.{ .domain = "antfly:changefeed-consumer:v1", .authority = self.authority, .id = self.id, .epoch = self.epoch })[0..16].*;
    }
    /// Resolve an issued cursor into a private root vector. Only the bounded
    /// retained window is resumable; a cursor is never an access credential.
    /// The caller must reauthorize the immutable definition before using it.
    pub fn rootsAt(self: State, a: A, cursor: Digest) ![]Root {
        if (self.phase != .bootstrap and self.phase != .streaming) return error.ChangefeedResynchronizationRequired;
        const end: usize = if (equal(cursor, self.acknowledged_cursor)) 0 else for (self.cursors, 0..) |entry, index| {
            if (equal(entry.cursor, cursor)) break index + 1;
        } else return error.ChangefeedCursorExpired;
        const roots = try a.dupe(Root, self.roots);
        errdefer a.free(roots);
        for (roots) |*root| {
            root.delivered = root.acknowledged;
            root.snapshot_delivered = root.snapshot_acknowledged;
        }
        for (self.cursors[0..end]) |entry| for (entry.advances) |advance| {
            const root = &roots[try self.rootIndex(advance.namespace)];
            root.delivered = advance.position;
            if (advance.snapshot) |position| root.snapshot_delivered = position;
        };
        return roots;
    }
    pub fn atDeliveryBoundary(self: State) bool {
        if (self.cursors.len != 0) return false;
        for (self.roots) |root| if (root.acknowledged.event != 0) return false;
        if (self.slot) |slot| {
            if (slot.entries.len != 0 or !equal(slot.mapped_cursor, self.acknowledged_cursor)) return false;
            for (slot.spool.records) |record| if (record.phase == .uploading) return false;
        }
        return true;
    }

    pub fn contextAt(self: State, cursor: Digest) ![]const u8 {
        if (self.phase != .streaming and self.phase != .bootstrap) return error.ChangefeedResynchronizationRequired;
        if (equal(cursor, self.acknowledged_cursor)) return self.acknowledged_provider_context;
        for (self.cursors) |receipt| if (equal(cursor, receipt.cursor)) return receipt.provider_context;
        return error.ChangefeedCursorExpired;
    }
    pub fn validate(self: State) !void {
        if (self.version != 1 or self.roots.len > max_roots or self.cursors.len > max_cursors) return error.InvalidChangefeed;
        if (self.slot_lsn_floor != 0 and (self.epoch < 2 or self.slot_lsn_floor < 2)) return error.InvalidChangefeed;
        if (self.slot) |slot| if (slot.consistent_lsn < self.slot_lsn_floor) return error.InvalidChangefeed;
        if (self.phase == .empty) {
            if (self.revision != 0 or self.epoch != 0 or nz(self.id) or nz(self.authority) or self.authority_sequence != 0 or self.definition != null or
                self.created_at != 0 or self.expires_at != 0 or self.bootstrap_expires_at != 0 or self.renewal_pending or nz(self.cut) or self.roots.len != 0 or self.cursors.len != 0 or
                self.latest_delivery != 0 or self.acknowledged_delivery != 0 or nz(self.latest_cursor) or nz(self.acknowledged_cursor) or self.reset_reason != .none or self.latest_provider_context.len != 0 or self.acknowledged_provider_context.len != 0 or self.slot != null) return error.InvalidChangefeed;
            return;
        }
        const definition = self.definition orelse return error.InvalidChangefeed;
        if (!validProviderContext(self.latest_provider_context) or !validProviderContext(self.acknowledged_provider_context)) return error.InvalidChangefeed;
        try definition.validate();
        if (self.revision == 0 or !nz(self.id) or self.epoch == 0 or !nz(self.authority) or self.authority_sequence == 0 or
            self.created_at == 0 or self.expires_at <= self.created_at or self.acknowledged_delivery > self.latest_delivery or
            self.cursors.len != self.latest_delivery - self.acknowledged_delivery or self.cursors.len > definition.cursor_window or encodedSize(self.cursors) > definition.cursor_byte_limit) return error.InvalidChangefeed;
        if ((definition.start == .snapshot_then_changes) != (self.bootstrap_expires_at != 0) or
            (self.bootstrap_expires_at != 0 and (self.bootstrap_expires_at <= self.created_at or self.bootstrap_expires_at > self.expires_at))) return error.InvalidChangefeed;
        if ((self.phase == .reset_required) != (self.reset_reason != .none)) return error.InvalidChangefeed;
        if (self.renewal_pending and (self.phase != .streaming or self.slot != null)) return error.InvalidChangefeed;
        for (self.roots, 0..) |root, index| {
            if (root.last_reconciliation) |observed| {
                try observed.validate();
                if (observed.sequence > self.authority_sequence or root.last_control != null) return error.InvalidChangefeed;
            }
            if (root.last_control) |settlement| {
                try settlement.validate();
                if (settlement.sequence > self.authority_sequence) return error.InvalidChangefeed;
            }
            if (root.pending_control) |pending| {
                try pending.validate();
                if (pending.sequence > self.authority_sequence or pending.ownership_epoch != root.ownership_epoch or pending.grant_sequence != root.grant_sequence or root.released) return error.InvalidChangefeed;
            }
            if (!nz(root.namespace) or root.ownership_epoch == 0 or root.grant_sequence == 0 or root.grant_sequence > self.authority_sequence or
                (root.physical != null and !root.physical.?.valid()) or
                !definition.contains(std.mem.readInt(u64, root.namespace[0..8], .big)) or
                (index != 0 and std.mem.order(u8, &self.roots[index - 1].namespace, &root.namespace) != .lt)) return error.InvalidChangefeed;
            if (root.split_handoff) |handoff| {
                try handoff.validate(root.storageNamespace());
                if (!std.meta.eql(root.physical, @as(?PhysicalRoot, handoff.predecessor)) or root.consumer_epoch == 0)
                    return error.InvalidChangefeed;
                if (root.split_grant_sequence) |grant| {
                    if (grant == 0 or grant > self.authority_sequence) return error.InvalidChangefeed;
                }
                if (root.split_consumer_epoch) |epoch| {
                    if (epoch == 0 or root.split_grant_sequence == null) return error.InvalidChangefeed;
                }
            } else if (root.split_grant_sequence != null or root.split_consumer_epoch != null) return error.InvalidChangefeed;
            if (root.merge_drain) |drain| {
                try drain.validate();
                if (root.split_handoff != null or root.consumer_epoch == 0 or
                    !std.meta.eql(root.physical, @as(?PhysicalRoot, drain.donor)) or
                    root.delivered.order(.{ .sequence = drain.latest }) == .gt)
                    return error.InvalidChangefeed;
            }
            if (root.storage_namespace) |namespace| {
                const physical = root.physical orelse return error.InvalidChangefeed;
                if (!nz(namespace) or !std.mem.eql(u8, namespace[0..8], root.namespace[0..8]) or
                    !equal(root.namespace, splitStreamNamespace(namespace, physical))) return error.InvalidChangefeed;
            }
            if (root.native_collected_through) |through| {
                if (through <= root.start or through > root.reclaimed_through) return error.InvalidChangefeed;
            }
            if (root.renewed_through > self.expires_at or (root.consumer_epoch == 0 and root.renewed_through != 0)) return error.InvalidChangefeed;
            if (root.consumer_epoch == 0) {
                if (root.start != 0 or root.delivered.order(.{}) != .eq or root.acknowledged.order(.{}) != .eq or root.reclaimed_through != 0 or root.snapshot != null) return error.InvalidChangefeed;
            } else if (root.acknowledged.sequence < root.start or root.acknowledged.order(root.delivered) == .gt or root.reclaimed_through < root.start or root.reclaimed_through > root.acknowledged.sequence) return error.InvalidChangefeed;
            if (root.snapshot) |pin| {
                if (definition.start != .snapshot_then_changes or !nz(pin.id) or !nz(pin.digest) or pin.expires_at != self.bootstrap_expires_at) return error.InvalidChangefeed;
            }
            for ([_]BootstrapPosition{ root.snapshot_delivered, root.snapshot_acknowledged }) |position| if (!text(position.continuation, 4096, true) or (position.page == 0 and (position.continuation.len != 0 or position.done))) return error.InvalidChangefeed;
            if (root.snapshot_acknowledged.page > root.snapshot_delivered.page or (root.snapshot_acknowledged.done and !root.snapshot_delivered.done)) return error.InvalidChangefeed;
            if (root.snapshot == null and (root.snapshot_delivered.page != 0 or root.snapshot_acknowledged.page != 0 or root.snapshot_released)) return error.InvalidChangefeed;
            if (self.phase == .deleted and (!root.released or (root.snapshot != null and !root.snapshot_released))) return error.InvalidChangefeed;
        }
        if (self.phase == .bootstrap or self.phase == .streaming) {
            if (!nz(self.cut) or self.roots.len == 0 or !nz(self.latest_cursor) or !nz(self.acknowledged_cursor)) return error.InvalidChangefeed;
            for (self.roots) |root| if (root.consumer_epoch == 0 or (root.released and (self.phase == .bootstrap or root.merge_drain == null)) or (definition.start == .snapshot_then_changes and root.snapshot == null and (self.phase == .bootstrap or root.storage_namespace == null))) return error.InvalidChangefeed;
        }
        if (self.slot) |slot| {
            if (self.phase == .enrolling) return error.InvalidChangefeed;
            try slot.validate();
            if (slot.spool_epoch != 0 and slot.spool_epoch != self.epoch) return error.InvalidChangefeed;
            if (slot.spool.definition) |identity| if (!equal(identity, definition.identity())) return error.InvalidChangefeed;
            if ((self.phase == .closing or self.phase == .reset_required or self.phase == .deleted) and slot.spool.next_id != 1 and !slot.spool_retired) return error.InvalidChangefeed;
            if (self.phase == .deleted and slot.spool.records.len != 0) return error.InvalidChangefeed;
            if (!equal(self.id, pg_slot.feedId(slot.database, slot.name)) or slot.session_expires_at > self.expires_at or !equal(slot.acknowledged_cursor, self.acknowledged_cursor)) return error.InvalidChangefeed;
            try slotDefinition(definition);
            var partial: [max_roots]bool = @splat(false);
            var partial_count: usize = 0;
            for (self.roots, 0..) |root, root_index| {
                partial[root_index] = root.acknowledged.event != 0;
                if (partial[root_index]) partial_count += 1;
            }
            // A slot cannot begin at a native partial-frame acknowledgement.
            if (partial_count != 0) return error.InvalidChangefeed;
            var cursor = slot.acknowledged_cursor;
            var index: usize = 0;
            for (slot.entries) |entry| {
                if (!equal(entry.transaction.before, cursor)) return error.InvalidChangefeed;
                while (index < self.cursors.len) : (index += 1) {
                    for (self.cursors[index].advances) |advance| {
                        const root_index = try self.rootIndex(advance.namespace);
                        if (partial[root_index]) partial_count -= 1;
                        partial[root_index] = advance.position.event != 0;
                        if (partial[root_index]) partial_count += 1;
                    }
                    if (equal(self.cursors[index].cursor, entry.transaction.after)) break;
                }
                if (index == self.cursors.len or partial_count != 0) return error.InvalidChangefeed;
                cursor = entry.transaction.after;
                index += 1;
            }
        }
        var previous = self.acknowledged_cursor;
        for (self.cursors, 0..) |cursor, i| {
            if (cursor.sequence != self.acknowledged_delivery + i + 1 or !equal(cursor.before, previous) or !nz(cursor.authority) or cursor.issued_at == 0 or
                (!validProviderContext(cursor.provider_context) or !equal(cursor.cursor, cursor.identity(self.id, self.epoch)))) return error.InvalidChangefeed;
            try validateAdvances(self, cursor.advances);
            previous = cursor.cursor;
        }
        if (self.cursors.len != 0 and !equal(previous, self.latest_cursor)) return error.InvalidChangefeed;
        if (self.cursors.len != 0 and !std.mem.eql(u8, self.latest_provider_context, self.cursors[self.cursors.len - 1].provider_context)) return error.InvalidChangefeed;
        if (self.cursors.len == 0 and !std.mem.eql(u8, self.latest_provider_context, self.acknowledged_provider_context)) return error.InvalidChangefeed;
        if (self.cursors.len == 0 and !equal(self.latest_cursor, self.acknowledged_cursor)) return error.InvalidChangefeed;
    }
};
fn validateAdvances(state: State, advances: []const Advance) !void {
    if (advances.len > max_advances) return error.InvalidChangefeed;
    for (advances, 0..) |advance, index| {
        _ = try state.rootIndex(advance.namespace);
        if (index != 0 and std.mem.order(u8, &advances[index - 1].namespace, &advance.namespace) != .lt) return error.InvalidChangefeed;
        if (advance.snapshot) |position| if (!text(position.continuation, 4096, true) or position.page == 0) return error.InvalidChangefeed;
    }
}
pub const RootControlMutation = union(enum) {
    prepare: struct { ownership_epoch: u64, grant_sequence: u64, payload: root_intent.Payload },
    prepare_fence: struct { previous_sequence: u64, retirement_sequence: u64, payload: root_intent.Payload },
    begin: struct { sequence: u64, actor: root_intent.Actor },
    accepted: struct { sequence: u64, actor: root_intent.Actor, receipt: root_intent.Receipt },
    settled: root_intent.Settlement,
    reconciled: root_intent.Reconciliation,
    cancel_prepared: u64,
    pub fn validate(self: RootControlMutation) !void {
        switch (self) {
            .prepare => |v| {
                if (v.ownership_epoch == 0 or v.grant_sequence == 0) return error.InvalidChangefeed;
                try v.payload.validate();
            },
            .prepare_fence => |v| {
                if (v.previous_sequence == 0 or v.retirement_sequence == 0) return error.InvalidChangefeed;
                try v.payload.validate();
            },
            .begin => |v| if (v.sequence == 0 or !nz(v.actor)) return error.InvalidChangefeed,
            .accepted => |v| {
                if (v.sequence == 0 or !nz(v.actor)) return error.InvalidChangefeed;
                try v.receipt.validate();
            },
            .settled => |v| try v.validate(),
            .reconciled => |v| try v.validate(),
            .cancel_prepared => |sequence| if (sequence == 0) return error.InvalidChangefeed,
        }
    }
};
pub const CatalogStart = struct { namespace: Namespace, through: u64, snapshot: ?Snapshot = null };

pub const Seal = struct {
    cut: Digest,
    slot: ?pg_slot.Configuration = null,
    provider_context: []const u8 = "",
    /// Catalog roots have no native consumer RPC. Enrollment and the starting
    /// provider position commit together against the planned root.
    catalog_starts: []const CatalogStart = &.{},

    pub fn jsonStringify(self: Seal, writer: anytype) @TypeOf(writer.*).Error!void {
        try writer.beginObject();
        try writer.objectField("cut");
        try writer.write(self.cut);
        try writer.objectField("slot");
        try writer.write(self.slot);
        if (self.provider_context.len != 0) {
            try writer.objectField("provider_context");
            try writer.write(self.provider_context);
        }
        if (self.catalog_starts.len != 0) {
            try writer.objectField("catalog_starts");
            try writer.write(self.catalog_starts);
        }
        try writer.endObject();
    }
};

pub const Mutation = union(enum) {
    root_control: struct { namespace: Namespace, operation: RootControlMutation },
    slot: pg_slot.Mutation,
    create: struct { id: Id, definition: Definition, now: u64, expires_at: u64, bootstrap_expires_at: u64 = 0 },
    recreate_slot: struct { definition: Definition, now: u64, expires_at: u64, bootstrap_expires_at: u64 = 0 },
    plan_roots: []const RootPlan,
    enroll: struct { namespace: Namespace, ownership_epoch: u64, grant_sequence: u64, consumer_epoch: u64, start: u64, snapshot: ?Snapshot = null },
    rebind: struct { namespace: Namespace, previous: u64, next: u64, proof: Digest },
    prune_merge_donor: struct { namespace: Namespace, transition: u64 },
    release_merge_donor: struct { namespace: Namespace, transition: u64, receipt_term: u64, receipt_index: u64 },
    stage_merge_drain: struct { namespace: Namespace, ownership_epoch: u64, drain: MergeDrain },
    stage_split_handoff: struct { namespace: Namespace, ownership_epoch: u64, handoff: SplitHandoff },
    release_split_successor: struct { namespace: Namespace, grant_sequence: u64, transition: u64 },
    confirm_split_successor: struct { namespace: Namespace, grant_sequence: u64, consumer_epoch: u64 },
    activate_split_successor: struct { namespace: Namespace, grant_sequence: u64 },
    refresh_grant: struct { namespace: Namespace, previous: u64 },
    stage_catalog_start: []const u8,
    seal: Seal,
    skip_slot_projection: struct { session: pg_slot.Session, now: u64, delivery: Delivery },
    issue: Delivery,
    acknowledge: Digest,
    reclaimed: struct { namespace: Namespace, consumer_epoch: u64, previous: u64, next: u64 },
    native_collected: struct { namespace: Namespace, consumer_epoch: u64, through: u64 },
    // Retained for replay of entries admitted before certified cutover.
    finish_bootstrap: struct { proof: Digest },
    finish_verified_bootstrap: struct { proof: Digest },
    start_exported_slot: struct { name: SlotSnapshot, now: u64, requested_lsn: u64 },
    release_snapshot: struct { namespace: Namespace, snapshot_id: Id, proof: Digest },
    renew: struct { now: u64, expires_at: u64 },
    renew_catalog: struct { now: u64, expires_at: u64 },
    expire: u64,
    reset: Reset,
    close,
    release_root: struct { namespace: Namespace, ownership_epoch: u64, proof: Digest },
    retire_absent_slot_root: Namespace,
    retire_absent_public_root: Namespace,
    finish_close,
    restart: struct { definition: Definition, now: u64, expires_at: u64, bootstrap_expires_at: u64 = 0 },
    pub fn requiredProtocol(self: Mutation) u16 {
        switch (self) {
            .retire_absent_slot_root => return retired_slot_root_protocol_version,
            .retire_absent_public_root => return absent_public_root_protocol_version,
            .native_collected => return native_collection_protocol_version,
            .recreate_slot => return slot_recreation_protocol_version,
            .create => |create| if (create.definition.usesProviderHistory()) return provider_history_protocol_version,
            .restart => |restart| if (restart.definition.usesProviderHistory()) return provider_history_protocol_version,
            .stage_catalog_start, .renew_catalog => return provider_history_protocol_version,
            .prune_merge_donor, .release_merge_donor, .stage_merge_drain, .stage_split_handoff, .release_split_successor, .confirm_split_successor, .activate_split_successor => return physical_history_protocol_version,
            .plan_roots => |plans| {
                var needs_physical = false;
                for (plans) |plan| {
                    if (plan.storage_namespace != null) return shared_namespace_plan_protocol_version;
                    needs_physical = needs_physical or plan.physical != null;
                }
                if (needs_physical) return physical_history_protocol_version;
            },
            .seal => |sealed| if (sealed.catalog_starts.len != 0) return provider_history_protocol_version,
            else => {},
        }
        if (self == .root_control) switch (self.root_control.operation) {
            .reconciled => |result| if (result.effect == .progress and result.effect.progress == .snapshot_enrolled) return qualified_bootstrap_protocol_version,
            .settled => |result| if (result.effect == .rejected and result.effect.rejected == 8) return qualified_bootstrap_protocol_version,
            else => {},
        };
        if (self == .start_exported_slot) return slot_snapshot_start_protocol_version;
        return if (self == .finish_verified_bootstrap) verified_bootstrap_protocol_version else protocol_version;
    }

    pub fn validate(self: Mutation) !void {
        switch (self) {
            .retire_absent_slot_root, .retire_absent_public_root => |namespace| if (!nz(namespace)) return error.InvalidChangefeed,
            .recreate_slot => |value| {
                try value.definition.validate();
                try slotDefinition(value.definition);
                try lease(value.now, value.expires_at);
                try bootstrapLease(value.definition.start, value.now, value.expires_at, value.bootstrap_expires_at);
            },
            .root_control => |v| {
                if (!nz(v.namespace)) return error.InvalidChangefeed;
                try v.operation.validate();
            },
            .slot => |operation| try operation.validate(),
            .create => |value| {
                if (!nz(value.id)) return error.InvalidChangefeed;
                try value.definition.validate();
                try lease(value.now, value.expires_at);
                try bootstrapLease(value.definition.start, value.now, value.expires_at, value.bootstrap_expires_at);
            },
            .plan_roots => |plans| {
                if (plans.len == 0 or plans.len > max_roots) return error.InvalidChangefeed;
                for (plans, 0..) |plan, index| {
                    if (!nz(plan.namespace) or plan.ownership_epoch == 0 or (plan.physical != null and !plan.physical.?.valid()) or (index != 0 and std.mem.order(u8, &plans[index - 1].namespace, &plan.namespace) != .lt)) return error.InvalidChangefeed;
                    if (plan.storage_namespace) |namespace| {
                        const physical = plan.physical orelse return error.InvalidChangefeed;
                        if (!nz(namespace) or !equal(plan.namespace, splitStreamNamespace(namespace, physical))) return error.InvalidChangefeed;
                    }
                }
            },
            .enroll => |value| {
                if (!nz(value.namespace) or value.ownership_epoch == 0 or value.grant_sequence == 0 or value.consumer_epoch == 0) return error.InvalidChangefeed;
                if (value.snapshot) |pin| if (!nz(pin.id) or !nz(pin.digest) or pin.expires_at == 0) return error.InvalidChangefeed;
            },
            .prune_merge_donor => |value| if (!nz(value.namespace) or value.transition == 0) return error.InvalidChangefeed,
            .release_merge_donor => |value| if (!nz(value.namespace) or value.transition == 0 or value.receipt_term == 0 or value.receipt_index == 0) return error.InvalidChangefeed,
            .stage_merge_drain => |value| {
                if (!nz(value.namespace) or value.ownership_epoch == 0) return error.InvalidChangefeed;
                try value.drain.validate();
            },
            .stage_split_handoff => |value| {
                if (!nz(value.namespace) or value.ownership_epoch == 0) return error.InvalidChangefeed;
                try value.handoff.validate(value.handoff.cut.namespace);
                if (!std.mem.eql(u8, value.namespace[0..8], value.handoff.cut.namespace[0..8])) return error.InvalidChangefeed;
            },
            .activate_split_successor => |value| if (!nz(value.namespace) or value.grant_sequence == 0) return error.InvalidChangefeed,
            .release_split_successor => |value| if (!nz(value.namespace) or value.transition == 0) return error.InvalidChangefeed,
            .confirm_split_successor => |value| {
                if (!nz(value.namespace) or value.grant_sequence == 0 or value.consumer_epoch == 0) return error.InvalidChangefeed;
            },
            .refresh_grant => |value| if (!nz(value.namespace) or value.previous == 0) return error.InvalidChangefeed,
            .rebind => |value| if (!nz(value.namespace) or value.previous == 0 or value.next <= value.previous or !nz(value.proof)) return error.InvalidChangefeed,
            .stage_catalog_start => |provider| if (provider.len == 0 or !validProviderContext(provider)) return error.InvalidChangefeed,
            .seal => |value| {
                if (!nz(value.cut) or !validProviderContext(value.provider_context)) return error.InvalidChangefeed;
                if (value.catalog_starts.len > max_roots or (value.catalog_starts.len != 0 and (value.provider_context.len == 0 or value.slot != null))) return error.InvalidChangefeed;
                for (value.catalog_starts, 0..) |start, index| {
                    if (!nz(start.namespace) or (index != 0 and std.mem.order(u8, &value.catalog_starts[index - 1].namespace, &start.namespace) != .lt)) return error.InvalidChangefeed;
                    if (start.snapshot) |snapshot| if (!nz(snapshot.id) or !nz(snapshot.digest) or snapshot.expires_at == 0) return error.InvalidChangefeed;
                }
                if (value.slot) |configuration| {
                    if (value.provider_context.len != 0) return error.InvalidChangefeed;
                    try configuration.validate();
                }
            },
            .skip_slot_projection => |skip| {
                if (skip.now == 0 or skip.delivery.issued_at > skip.now or skip.delivery.advances.len == 0) return error.InvalidChangefeed;
                for (skip.delivery.advances) |advance| if (advance.position.event != 0 or advance.snapshot != null) return error.InvalidChangefeed;
                try (Mutation{ .issue = skip.delivery }).validate();
            },
            .issue => |value| {
                if (!validProviderContext(value.provider_context)) return error.InvalidChangefeed;
                if (value.sequence == 0 or !nz(value.before) or !nz(value.cursor) or !nz(value.authority) or value.issued_at == 0 or value.advances.len > max_advances) return error.InvalidChangefeed;
                for (value.advances, 0..) |advance, index| {
                    if (!nz(advance.namespace) or advance.position.event >= 65536 or (index != 0 and std.mem.order(u8, &value.advances[index - 1].namespace, &advance.namespace) != .lt)) return error.InvalidChangefeed;
                    if (advance.snapshot) |position| if (position.page == 0 or !text(position.continuation, 4096, true)) return error.InvalidChangefeed;
                }
            },
            .acknowledge => |value| if (!nz(value)) return error.InvalidChangefeed,
            .reclaimed => |value| if (!nz(value.namespace) or value.consumer_epoch == 0 or value.next < value.previous) return error.InvalidChangefeed,
            .native_collected => |value| if (!nz(value.namespace) or value.consumer_epoch == 0 or value.through == 0) return error.InvalidChangefeed,
            .finish_bootstrap => |value| if (!nz(value.proof)) return error.InvalidChangefeed,
            .finish_verified_bootstrap => |value| if (!nz(value.proof)) return error.InvalidChangefeed,
            .start_exported_slot => |value| if (!nz(value.name.feed) or !nz(value.name.proof) or value.now == 0) return error.InvalidChangefeed,
            .release_snapshot => |value| if (!nz(value.namespace) or !nz(value.snapshot_id) or !nz(value.proof)) return error.InvalidChangefeed,
            .renew => |value| try lease(value.now, value.expires_at),
            .renew_catalog => |value| try lease(value.now, value.expires_at),
            .expire => |value| if (value == 0) return error.InvalidChangefeed,
            .reset => |value| if (value == .none) return error.InvalidChangefeed,
            .close, .finish_close => {},
            .release_root => |value| if (!nz(value.namespace) or value.ownership_epoch == 0 or !nz(value.proof)) return error.InvalidChangefeed,
            .restart => |value| {
                try value.definition.validate();
                try lease(value.now, value.expires_at);
                try bootstrapLease(value.definition.start, value.now, value.expires_at, value.bootstrap_expires_at);
            },
        }
    }
    pub fn apply(self: Mutation, a: A, state: State, stamp: Stamp) !State {
        try self.validate();
        try state.validate();
        if (!nz(stamp.incarnation) or stamp.sequence == 0 or stamp.sequence <= state.authority_sequence or (state.phase != .empty and !equal(stamp.incarnation, state.authority))) return error.ChangefeedConflict;
        var next = state;
        // A new incarnation/grant or semantic root update must not discard an
        // unresolved exact request. Close/reset preserve it for cleanup recovery.
        const changed_root: ?Namespace = switch (self) {
            .enroll => |v| v.namespace,
            .rebind => |v| v.namespace,
            .release_merge_donor => |v| v.namespace,
            .stage_merge_drain => |v| v.namespace,
            .stage_split_handoff => |v| v.namespace,
            .activate_split_successor => |v| v.namespace,
            .refresh_grant => |v| v.namespace,
            .reclaimed => |v| v.namespace,
            .release_root => |v| v.namespace,
            else => null,
        };
        if (changed_root) |namespace| if (state.roots[try state.rootIndex(namespace)].pending_control != null) return error.ChangefeedConflict;
        switch (self) {
            .root_control => |value| {
                if (state.phase == .empty) return error.ChangefeedConflict;
                const index = try state.rootIndex(value.namespace);
                const root = state.roots[index];
                if (value.operation == .settled and root.pending_control == null) {
                    if (root.last_control) |last| if (std.meta.eql(last, value.operation.settled)) return state;
                    return error.ChangefeedConflict;
                }
                if (value.operation == .reconciled and root.pending_control == null) {
                    if (root.last_reconciliation) |last| if (std.meta.eql(last, value.operation.reconciled)) return state;
                    return error.ChangefeedConflict;
                }
                if (state.phase == .deleted or root.released) return error.ChangefeedConflict;
                const roots = try a.dupe(Root, state.roots);
                switch (value.operation) {
                    .prepare => |v| {
                        if (root.pending_control != null or v.ownership_epoch != root.ownership_epoch or v.grant_sequence != root.grant_sequence) return error.ChangefeedConflict;
                        roots[index].pending_control = try root_intent.State.init(stamp.sequence, v.ownership_epoch, v.grant_sequence, v.payload);
                    },
                    .prepare_fence => |v| {
                        if (state.phase != .closing and state.phase != .reset_required) return error.ChangefeedConflict;
                        const previous = root.pending_control orelse return error.ChangefeedConflict;
                        if (v.previous_sequence != previous.sequence or v.retirement_sequence != state.authority_sequence or v.retirement_sequence <= root.grant_sequence or !std.mem.eql(u8, v.payload.table, previous.payload.table) or !std.mem.eql(u8, v.payload.fence, previous.payload.fence)) return error.ChangefeedConflict;
                        var replacement = try root_intent.State.init(stamp.sequence, root.ownership_epoch, root.grant_sequence, v.payload);
                        replacement.retirement_fence = true;
                        replacement.displaced = root_intent.Displaced.from(previous);
                        try replacement.validate();
                        roots[index].pending_control = replacement;
                    },
                    .begin => |v| {
                        const pending = root.pending_control orelse return error.ChangefeedConflict;
                        if (v.sequence != pending.sequence) return error.ChangefeedConflict;
                        roots[index].pending_control = try pending.begin(v.actor);
                    },
                    .accepted => |v| {
                        const pending = root.pending_control orelse return error.ChangefeedConflict;
                        if (v.sequence != pending.sequence) return error.ChangefeedConflict;
                        const accepted = try pending.accept(v.actor, v.receipt);
                        if (pending.stage == .accepted) return state;
                        roots[index].pending_control = accepted;
                    },
                    .settled => |settlement| {
                        const pending = root.pending_control orelse return error.ChangefeedConflict;
                        try pending.verifySettlement(settlement);
                        try applyRootControlEffect(state, &roots[index], pending, settlement.effect);
                        // The result receipt and corresponding metadata effect
                        // become durable in the same metadata transaction.
                        roots[index].last_control = settlement;
                        roots[index].last_reconciliation = null;
                        roots[index].pending_control = null;
                    },
                    .reconciled => |observed| {
                        const pending = root.pending_control orelse return error.ChangefeedConflict;
                        try pending.verifyReconciliation(observed);
                        switch (observed.effect) {
                            .progress => |effect| try applyRootControlEffect(state, &roots[index], pending, effect),
                            .released => {
                                if (state.phase != .closing and state.phase != .reset_required) return error.ChangefeedConflict;
                                roots[index].released = true;
                            },
                        }
                        roots[index].last_reconciliation = observed;
                        roots[index].last_control = null;
                        roots[index].pending_control = null;
                    },
                    .cancel_prepared => |sequence| {
                        const pending = root.pending_control orelse return error.ChangefeedConflict;
                        if (sequence != pending.sequence or pending.stage != .prepared) return error.ChangefeedConflict;
                        roots[index].pending_control = null;
                    },
                }
                next.roots = roots;
            },
            .slot => |operation| {
                const cleanup = (operation == .reclaim_spool or operation == .checkpoint_spool_cleanup) and (state.phase == .closing or state.phase == .reset_required);
                if (operation != .drop and state.phase != .streaming and state.phase != .bootstrap and !cleanup) return error.ChangefeedResynchronizationRequired;
                if (state.phase == .empty) return error.ChangefeedConflict;
                try slotDefinition(state.definition.?);
                switch (operation) {
                    .configure => |configuration| {
                        if (!equal(state.id, pg_slot.feedId(configuration.database, configuration.name))) return error.ChangefeedConflict;
                        if (state.slot) |existing| {
                            if (!equal(existing.database, configuration.database) or !std.mem.eql(u8, existing.name, configuration.name) or existing.byte_limit != configuration.byte_limit or existing.consistent_lsn != configuration.consistent_lsn or !configuration.matchesBinding(existing.binding)) return error.ChangefeedConflict;
                            return state;
                        }
                        if (state.cursors.len != 0) return error.ChangefeedConflict;
                        for (state.roots) |root| if (root.acknowledged.event != 0) return error.ChangefeedConflict;
                        next.slot = try pg_slot.State.initializeAt(configuration.database, configuration.name, state.acknowledged_cursor, configuration.byte_limit, configuration.consistent_lsn);
                        next.slot.?.binding = configuration.binding;
                    },
                    .drop => |drop| {
                        const slot = state.slot orelse return error.ChangefeedConflict;
                        if (!equal(slot.database, drop.database) or !std.mem.eql(u8, slot.name, drop.name)) return error.ChangefeedConflict;
                        if (state.phase == .closing or state.phase == .deleted) return state;
                        if (drop.now < slot.observed_at) return error.ChangefeedConflict;
                        if (slot.session != null and drop.now < slot.session_expires_at and state.phase != .reset_required) return error.ChangefeedConflict;
                        next.phase = .closing;
                        next.reset_reason = .none;
                    },
                    .acquire => |claim| {
                        if (claim.now >= state.expires_at or claim.expires > state.expires_at) return error.ChangefeedLeaseExpired;
                        next.slot = try (state.slot orelse return error.ChangefeedConflict).acquire(claim.actor, claim.expected, claim.now, claim.expires);
                    },
                    .release => |release| {
                        if (release.now >= state.expires_at) return error.ChangefeedLeaseExpired;
                        next.slot = try (state.slot orelse return error.ChangefeedConflict).release(release.session, release.now);
                    },
                    .issue => |issue| {
                        if (state.phase != .streaming) return error.ChangefeedConflict;
                        if (issue.now >= state.expires_at) return error.ChangefeedLeaseExpired;
                        const roots = try state.rootsAt(a, issue.transaction.after);
                        defer a.free(roots);
                        // A page boundary within a retained frame cannot be
                        // certified as a complete PostgreSQL transaction.
                        for (roots) |root| if (root.delivered.event != 0) return error.ChangefeedConflict;
                        next.slot = try (state.slot orelse return error.ChangefeedConflict).issue(a, issue.session, issue.now, issue.transaction);
                    },
                    .reserve_spool => |reservation| {
                        if (state.phase != .streaming or !equal(reservation.spec.feed, state.id) or reservation.spec.epoch != state.epoch or !equal(reservation.spec.definition, state.definition.?.identity())) return error.ChangefeedConflict;
                        if (reservation.now >= state.expires_at) return error.ChangefeedLeaseExpired;
                        const roots = try state.rootsAt(a, reservation.spec.after);
                        defer a.free(roots);
                        for (roots) |root| if (root.delivered.event != 0) return error.ChangefeedConflict;
                        next.slot = try (state.slot orelse return error.ChangefeedConflict).reserveSpool(a, reservation.session, reservation.now, reservation.id, reservation.scope, reservation.spec);
                    },
                    .bind => |binding| {
                        if (state.phase != .streaming or binding.now >= state.expires_at) return error.ChangefeedLeaseExpired;
                        next.slot = try (state.slot orelse return error.ChangefeedConflict).bind(binding.session, binding.now, binding.binding);
                    },
                    .feedback => |feedback| {
                        if (state.phase != .streaming or feedback.now >= state.expires_at or feedback.expires > state.expires_at) return error.ChangefeedLeaseExpired;
                        const slot = state.slot orelse return error.ChangefeedConflict;
                        const confirmed = try slot.feedback(a, feedback.session, feedback.now, feedback.expires, feedback.flushed);
                        if (!equal(confirmed.acknowledged_cursor, slot.acknowledged_cursor)) try acknowledge(a, state, &next, confirmed.acknowledged_cursor);
                        next.slot = confirmed;
                    },
                    .begin_spool => |build| {
                        if (state.phase != .streaming) return error.ChangefeedConflict;
                        if (build.now >= state.expires_at) return error.ChangefeedLeaseExpired;
                        next.slot = try (state.slot orelse return error.ChangefeedConflict).beginSpool(a, build.session, build.now, build.id, build.token);
                    },
                    .publish_spool => |publication| {
                        if (state.phase != .streaming) return error.ChangefeedConflict;
                        if (publication.now >= state.expires_at) return error.ChangefeedLeaseExpired;
                        next.slot = try (state.slot orelse return error.ChangefeedConflict).publishSpool(a, publication.session, publication.now, publication.id, publication.manifest, publication.content);
                    },
                    .abandon_spool => |abandonment| {
                        if (abandonment.now >= state.expires_at) return error.ChangefeedLeaseExpired;
                        next.slot = try (state.slot orelse return error.ChangefeedConflict).abandonSpool(a, abandonment.session, abandonment.now, abandonment.id);
                    },
                    .checkpoint_spool_cleanup => |checkpoint| next.slot = try (state.slot orelse return error.ChangefeedConflict).checkpointSpoolCleanup(a, checkpoint.id, checkpoint.storage, checkpoint.expected, checkpoint.progress),
                    .reclaim_spool => |reclamation| next.slot = try (state.slot orelse return error.ChangefeedConflict).reclaimSpool(a, reclamation.id, reclamation.storage, reclamation.proof),
                    .confirm => |confirmation| {
                        if (state.phase != .streaming) return error.ChangefeedConflict;
                        if (confirmation.now >= state.expires_at) return error.ChangefeedLeaseExpired;
                        const slot = state.slot orelse return error.ChangefeedConflict;
                        const confirmed = try slot.confirm(a, confirmation.session, confirmation.now, confirmation.flushed);
                        if (!equal(confirmed.acknowledged_cursor, slot.acknowledged_cursor)) try acknowledge(a, state, &next, confirmed.acknowledged_cursor);
                        next.slot = confirmed;
                    },
                }
            },
            .create => |create| {
                if (state.phase != .empty or !nz(create.id)) return error.ChangefeedConflict;
                try create.definition.validate();
                try lease(create.now, create.expires_at);
                try bootstrapLease(create.definition.start, create.now, create.expires_at, create.bootstrap_expires_at);
                next = .{ .id = create.id, .epoch = 1, .authority = stamp.incarnation, .definition = create.definition, .phase = .enrolling, .created_at = create.now, .expires_at = create.expires_at, .bootstrap_expires_at = create.bootstrap_expires_at };
            },
            .recreate_slot => |create| {
                if (state.phase != .deleted or !equal(state.definition.?.owner, create.definition.owner)) return error.ChangefeedConflict;
                const floor = if (state.slot) |previous|
                    std.math.add(u64, previous.next_lsn, 1) catch return error.ChangefeedConflict
                else if (state.slot_lsn_floor != 0)
                    state.slot_lsn_floor
                else
                    return error.ChangefeedConflict;
                next = .{
                    .id = state.id,
                    .epoch = std.math.add(u64, state.epoch, 1) catch return error.ChangefeedConflict,
                    .authority = state.authority,
                    .definition = create.definition,
                    .phase = .enrolling,
                    .created_at = create.now,
                    .expires_at = create.expires_at,
                    .bootstrap_expires_at = create.bootstrap_expires_at,
                    .slot_lsn_floor = floor,
                };
            },
            .plan_roots => |plans| {
                if (state.phase != .enrolling or state.roots.len != 0 or plans.len == 0 or plans.len > max_roots) return error.ChangefeedConflict;
                const roots = try a.alloc(Root, plans.len);
                for (plans, roots) |plan, *root| root.* = .{ .namespace = plan.namespace, .ownership_epoch = plan.ownership_epoch, .grant_sequence = stamp.sequence, .physical = plan.physical, .storage_namespace = plan.storage_namespace };
                next.roots = roots;
            },
            .enroll => |enrollment| {
                if (state.phase != .enrolling or enrollment.consumer_epoch == 0) return error.ChangefeedConflict;
                const index = try state.rootIndex(enrollment.namespace);
                const previous = state.roots[index];
                if (previous.ownership_epoch != enrollment.ownership_epoch or previous.grant_sequence != enrollment.grant_sequence) return error.ChangefeedConflict;
                if (previous.consumer_epoch != 0) {
                    if (previous.consumer_epoch != enrollment.consumer_epoch or previous.start != enrollment.start or !std.meta.eql(previous.snapshot, enrollment.snapshot)) return error.ChangefeedConflict;
                    return state;
                }
                if ((state.definition.?.start == .snapshot_then_changes) != (enrollment.snapshot != null)) return error.InvalidChangefeed;
                const roots = try a.dupe(Root, state.roots);
                roots[index].consumer_epoch = enrollment.consumer_epoch;
                roots[index].start = enrollment.start;
                roots[index].delivered = .{ .sequence = enrollment.start };
                roots[index].acknowledged = .{ .sequence = enrollment.start };
                roots[index].reclaimed_through = enrollment.start;
                roots[index].snapshot = enrollment.snapshot;
                next.roots = roots;
            },
            .prune_merge_donor => |pruned| {
                if (state.phase != .streaming or !state.atDeliveryBoundary() or state.roots.len < 2)
                    return error.ChangefeedConflict;
                const index = try state.rootIndex(pruned.namespace);
                const donor = state.roots[index];
                const drain = donor.merge_drain orelse return error.ChangefeedConflict;
                if (!donor.released or !donor.mergeDrained() or drain.transition != pruned.transition)
                    return error.ChangefeedConflict;
                const roots = try a.alloc(Root, state.roots.len - 1);
                @memcpy(roots[0..index], state.roots[0..index]);
                @memcpy(roots[index..], state.roots[index + 1 ..]);
                next.roots = roots;
            },
            .release_merge_donor => |released| {
                if (state.phase != .streaming or !state.atDeliveryBoundary()) return error.ChangefeedConflict;
                const index = try state.rootIndex(released.namespace);
                const donor = state.roots[index];
                const drain = donor.merge_drain orelse return error.ChangefeedConflict;
                if (!donor.mergeDrained() or drain.transition != released.transition or
                    released.receipt_index <= drain.index or released.receipt_term < drain.term)
                    return error.ChangefeedConflict;
                if (donor.released) return state;
                const roots = try a.dupe(Root, state.roots);
                roots[index].released = true;
                next.roots = roots;
            },
            .stage_merge_drain => |staged| {
                if (state.phase != .streaming) return error.ChangefeedConflict;
                const index = try state.rootIndex(staged.namespace);
                const donor = state.roots[index];
                if (donor.ownership_epoch != staged.ownership_epoch or donor.consumer_epoch == 0 or donor.released or donor.split_handoff != null or
                    !std.meta.eql(donor.physical, @as(?PhysicalRoot, staged.drain.donor)) or
                    donor.delivered.order(.{ .sequence = staged.drain.latest }) == .gt)
                    return error.ChangefeedConflict;
                const has_receiver = for (state.roots) |root| {
                    if (root.consumer_epoch != 0 and !root.released and std.meta.eql(root.physical, @as(?PhysicalRoot, staged.drain.receiver)) and
                        std.mem.eql(u8, root.namespace[0..8], donor.namespace[0..8])) break true;
                } else false;
                if (!has_receiver) return error.ChangefeedResynchronizationRequired;
                if (donor.merge_drain) |existing| {
                    if (!std.meta.eql(existing, staged.drain)) return error.ChangefeedConflict;
                    return state;
                }
                const roots = try a.dupe(Root, state.roots);
                roots[index].merge_drain = staged.drain;
                next.roots = roots;
            },
            .stage_split_handoff => |staged| {
                if (state.phase != .streaming) return error.ChangefeedConflict;
                const index = try state.rootIndex(staged.namespace);
                const previous = state.roots[index];
                try staged.handoff.validate(previous.storageNamespace());
                if (previous.ownership_epoch != staged.ownership_epoch or previous.consumer_epoch == 0 or previous.released or previous.merge_drain != null or
                    !std.meta.eql(previous.physical, @as(?PhysicalRoot, staged.handoff.predecessor))) return error.ChangefeedConflict;
                if (previous.split_handoff) |handoff| {
                    if (!std.meta.eql(handoff, staged.handoff)) return error.ChangefeedConflict;
                    if (previous.split_grant_sequence != null) return state;
                }
                if (staged.handoff.cut.reclaimed > previous.acknowledged.sequence)
                    return error.ChangefeedResynchronizationRequired;
                const roots = try a.dupe(Root, state.roots);
                roots[index].split_handoff = staged.handoff;
                roots[index].split_grant_sequence = stamp.sequence;
                next.roots = roots;
            },
            .release_split_successor => |released| {
                if (state.phase != .closing and state.phase != .reset_required) return error.ChangefeedConflict;
                const index = try state.rootIndex(released.namespace);
                const predecessor = state.roots[index];
                const handoff = predecessor.split_handoff orelse return state;
                if (handoff.cut.transition != released.transition or (predecessor.split_grant_sequence orelse 0) != released.grant_sequence)
                    return error.ChangefeedConflict;
                const roots = try a.dupe(Root, state.roots);
                roots[index].split_handoff = null;
                roots[index].split_grant_sequence = null;
                roots[index].split_consumer_epoch = null;
                next.roots = roots;
            },
            .confirm_split_successor => |confirmed| {
                if (state.phase != .streaming) return error.ChangefeedConflict;
                const index = try state.rootIndex(confirmed.namespace);
                const previous = state.roots[index];
                if (previous.split_handoff == null or previous.split_grant_sequence != confirmed.grant_sequence)
                    return error.ChangefeedConflict;
                if (previous.split_consumer_epoch) |epoch| {
                    if (epoch != confirmed.consumer_epoch) return error.ChangefeedConflict;
                    return state;
                }
                const roots = try a.dupe(Root, state.roots);
                roots[index].split_consumer_epoch = confirmed.consumer_epoch;
                next.roots = roots;
            },
            .activate_split_successor => |activation| {
                if (state.phase != .streaming or !state.atDeliveryBoundary() or state.roots.len == max_roots)
                    return error.ChangefeedConflict;
                const index = try state.rootIndex(activation.namespace);
                const predecessor = state.roots[index];
                const handoff = predecessor.split_handoff orelse return error.ChangefeedConflict;
                const epoch = predecessor.split_consumer_epoch orelse return error.ChangefeedConflict;
                if (predecessor.split_grant_sequence != activation.grant_sequence or handoff.cut.reserved_bytes != 0 or
                    predecessor.acknowledged.order(.{ .sequence = handoff.cut.latest }) != .eq)
                    return error.ChangefeedConflict;
                const namespace = splitStreamNamespace(predecessor.storageNamespace(), handoff.successor);
                const roots = try a.alloc(Root, state.roots.len + 1);
                @memcpy(roots[0..state.roots.len], state.roots);
                roots[index].split_handoff = null;
                roots[index].split_grant_sequence = null;
                roots[index].split_consumer_epoch = null;
                roots[state.roots.len] = .{
                    .namespace = namespace,
                    .storage_namespace = predecessor.storageNamespace(),
                    .ownership_epoch = 1,
                    .grant_sequence = activation.grant_sequence,
                    .consumer_epoch = epoch,
                    .physical = handoff.successor,
                };
                std.mem.sort(Root, roots, {}, struct {
                    fn less(_: void, left: Root, right: Root) bool {
                        return std.mem.order(u8, &left.namespace, &right.namespace) == .lt;
                    }
                }.less);
                next.roots = roots;
            },
            .rebind => |binding| {
                if (state.phase == .empty or state.phase == .deleted) return error.ChangefeedConflict;
                const index = try state.rootIndex(binding.namespace);
                if (state.roots[index].ownership_epoch == binding.next) return state;
                if (state.roots[index].ownership_epoch != binding.previous) return error.ChangefeedConflict;
                const roots = try a.dupe(Root, state.roots);
                roots[index].ownership_epoch = binding.next;
                // A closing/reset feed can move to its surviving storage owner,
                // but cannot issue a new enrollment grant after its close stamp.
                if (state.phase != .closing and state.phase != .reset_required) roots[index].grant_sequence = stamp.sequence;
                next.roots = roots;
            },
            .refresh_grant => |refresh| {
                if (state.phase != .enrolling and state.phase != .bootstrap and state.phase != .streaming) return error.ChangefeedConflict;
                const index = try state.rootIndex(refresh.namespace);
                if (state.roots[index].grant_sequence != refresh.previous or stamp.sequence <= refresh.previous or state.roots[index].released) return error.ChangefeedConflict;
                const roots = try a.dupe(Root, state.roots);
                roots[index].grant_sequence = stamp.sequence;
                next.roots = roots;
            },
            .stage_catalog_start => |provider| {
                const definition = state.definition orelse return error.ChangefeedConflict;
                if (state.phase != .enrolling) return error.ChangefeedConflict;
                var catalog_roots: usize = 0;
                for (definition.tables) |table| {
                    if (!catalogBacked(table.contract)) continue;
                    _ = try state.rootIndex(catalogRootNamespace(table));
                    catalog_roots += 1;
                }
                if (catalog_roots == 0 and (definition.events != artifact_event or state.roots.len == 0)) return error.ChangefeedConflict;
                if (definition.events == artifact_event and provider.len != std.base64.url_safe_no_pad.Encoder.calcSize(state.roots.len * @sizeOf(u64)))
                    return error.ChangefeedConflict;
                if (state.latest_provider_context.len != 0) {
                    if (std.mem.eql(u8, state.latest_provider_context, provider)) return state;
                    return error.ChangefeedConflict;
                }
                next.latest_provider_context = provider;
                next.acknowledged_provider_context = provider;
            },
            .seal => |sealed| {
                if (state.phase != .enrolling or state.roots.len == 0 or !nz(sealed.cut)) return error.ChangefeedConflict;
                if (sealed.catalog_starts.len != 0) {
                    if (!std.mem.eql(u8, state.latest_provider_context, sealed.provider_context)) return error.ChangefeedConflict;
                    const definition = state.definition.?;
                    const roots = try a.dupe(Root, state.roots);
                    var start_index: usize = 0;
                    const artifact = definition.events == artifact_event;
                    const count = if (artifact) roots.len else definition.tables.len;
                    for (0..count) |index| {
                        const namespace = if (artifact) roots[index].namespace else blk: {
                            const table = definition.tables[index];
                            if (!catalogBacked(table.contract)) continue;
                            break :blk catalogRootNamespace(table);
                        };
                        if (start_index >= sealed.catalog_starts.len) return error.ChangefeedConflict;
                        const start = sealed.catalog_starts[start_index];
                        start_index += 1;
                        if (!equal(start.namespace, namespace) or
                            (definition.start == .snapshot_then_changes) != (start.snapshot != null)) return error.ChangefeedConflict;
                        if (start.snapshot) |snapshot| if (snapshot.expires_at != state.bootstrap_expires_at) return error.ChangefeedConflict;
                        const root = &roots[try state.rootIndex(start.namespace)];
                        if (root.consumer_epoch != 0 or root.pending_control != null) return error.ChangefeedConflict;
                        root.consumer_epoch = state.epoch;
                        root.start = start.through;
                        root.delivered = .{ .sequence = start.through };
                        root.acknowledged = .{ .sequence = start.through };
                        root.reclaimed_through = start.through;
                        root.snapshot = start.snapshot;
                    }
                    if (start_index != sealed.catalog_starts.len) return error.ChangefeedConflict;
                    next.roots = roots;
                }
                for (next.roots) |root| if (root.consumer_epoch == 0) return error.ChangefeedConflict;
                for (state.definition.?.tables) |table| {
                    var found = false;
                    for (next.roots) |root| if (std.mem.readInt(u64, root.namespace[0..8], .big) == table.table_id) {
                        found = true;
                        break;
                    };
                    if (!found) return error.ChangefeedConflict;
                }
                next.cut = sealed.cut;
                next.phase = if (state.definition.?.start == .snapshot_then_changes) .bootstrap else .streaming;
                next.latest_cursor = if (sealed.provider_context.len == 0)
                    initialCursor(state, sealed.cut)
                else
                    hash(.{ .domain = "antfly:changefeed-cut:v2", .base = initialCursor(state, sealed.cut), .provider_context = sealed.provider_context });
                next.acknowledged_cursor = next.latest_cursor;
                next.latest_provider_context = sealed.provider_context;
                next.acknowledged_provider_context = sealed.provider_context;
                if (sealed.slot) |configuration| {
                    try slotDefinition(state.definition.?);
                    if (!equal(state.id, pg_slot.feedId(configuration.database, configuration.name))) return error.ChangefeedConflict;
                    next.slot = try pg_slot.State.initializeAt(configuration.database, configuration.name, next.acknowledged_cursor, configuration.byte_limit, configuration.consistent_lsn);
                    next.slot.?.binding = configuration.binding;
                }
            },
            .skip_slot_projection => |skip| {
                if (state.phase != .streaming or skip.now >= state.expires_at) return error.ChangefeedLeaseExpired;
                const slot = state.slot orelse return error.ChangefeedConflict;
                try slot.verifySession(skip.session, skip.now);
                if (slot.binding == null or slot.entries.len != 0 or state.cursors.len != 0) return error.ChangefeedConflict;
                for (slot.spool.records) |record| if (record.phase == .uploading) return error.ChangefeedConflict;
                for (skip.delivery.advances) |advance| {
                    const root = state.roots[try state.rootIndex(advance.namespace)];
                    if (advance.position.order(root.acknowledged) != .gt) return error.ChangefeedConflict;
                }
                // The authorized producer attests the complete projection is
                // empty. Stage and acknowledge it within this one mutation.
                var staged_arena = std.heap.ArenaAllocator.init(a);
                defer staged_arena.deinit();
                const staged = try (Mutation{ .issue = skip.delivery }).apply(staged_arena.allocator(), state, stamp);
                try acknowledge(a, staged, &next, skip.delivery.cursor);
                next.latest_delivery = staged.latest_delivery;
                next.latest_cursor = staged.latest_cursor;
                next.latest_provider_context = skip.delivery.provider_context;
                next.slot.?.acknowledged_cursor = skip.delivery.cursor;
                next.slot.?.mapped_cursor = skip.delivery.cursor;
                next.slot.?.observed_at = skip.now;
            },
            .issue => |delivery| {
                if (state.phase != .streaming and state.phase != .bootstrap) return error.ChangefeedResynchronizationRequired;
                if (delivery.sequence != (std.math.add(u64, state.latest_delivery, 1) catch return error.ChangefeedBackpressure) or !equal(delivery.before, state.latest_cursor) or
                    !nz(delivery.authority) or delivery.issued_at < state.created_at or delivery.issued_at >= state.expires_at or !equal(delivery.cursor, delivery.identity(state.id, state.epoch))) return error.ChangefeedConflict;
                if (state.phase == .bootstrap and delivery.issued_at >= state.bootstrap_expires_at) return error.ChangefeedLeaseExpired;
                if (state.cursors.len >= state.definition.?.cursor_window) return error.ChangefeedBackpressure;
                try validateAdvances(state, delivery.advances);
                const roots = try a.dupe(Root, state.roots);
                for (delivery.advances) |advance| {
                    const root = &roots[try state.rootIndex(advance.namespace)];
                    if (advance.position.order(root.delivered) == .lt) return error.ChangefeedConflict;
                    if (state.phase == .bootstrap) {
                        const snapshot = advance.snapshot orelse return error.InvalidChangefeed;
                        if (advance.position.order(.{ .sequence = root.start }) != .eq or root.snapshot_delivered.done or snapshot.page != (std.math.add(u64, root.snapshot_delivered.page, 1) catch return error.ChangefeedBackpressure)) return error.ChangefeedConflict;
                        root.snapshot_delivered = snapshot;
                    } else if (advance.snapshot != null) return error.InvalidChangefeed;
                    root.delivered = advance.position;
                }
                const cursors = try a.alloc(Delivery, state.cursors.len + 1);
                @memcpy(cursors[0..state.cursors.len], state.cursors);
                cursors[state.cursors.len] = delivery;
                if (encodedSize(cursors) > state.definition.?.cursor_byte_limit) return error.ChangefeedBackpressure;
                next.roots = roots;
                next.cursors = cursors;
                next.latest_delivery = delivery.sequence;
                next.latest_cursor = delivery.cursor;
                next.latest_provider_context = delivery.provider_context;
            },
            .acknowledge => |cursor| {
                if (state.phase != .streaming and state.phase != .bootstrap) return error.ChangefeedResynchronizationRequired;
                if (state.slot != null and state.phase == .streaming) return error.ChangefeedConflict;
                if (equal(cursor, state.acknowledged_cursor)) return state;
                try acknowledge(a, state, &next, cursor);
                if (next.slot) |*slot| {
                    if (slot.entries.len != 0) return error.ChangefeedConflict;
                    slot.acknowledged_cursor = cursor;
                    slot.mapped_cursor = cursor;
                }
            },
            .reclaimed => |progress| {
                const index = try state.rootIndex(progress.namespace);
                const previous = state.roots[index];
                if (previous.consumer_epoch != progress.consumer_epoch or progress.previous != previous.reclaimed_through or progress.next < progress.previous or progress.next > previous.acknowledged.sequence) return error.ChangefeedConflict;
                if (progress.next == progress.previous) return state;
                const roots = try a.dupe(Root, state.roots);
                roots[index].reclaimed_through = progress.next;
                next.roots = roots;
            },
            .native_collected => |progress| {
                if (state.phase != .streaming or state.slot != null or state.definition.?.events != 1) return error.ChangefeedConflict;
                const index = try state.rootIndex(progress.namespace);
                const root = state.roots[index];
                if (root.released or root.consumer_epoch != progress.consumer_epoch or progress.through > root.reclaimed_through)
                    return error.ChangefeedConflict;
                const table_id = std.mem.readInt(u64, progress.namespace[0..8], .big);
                for (state.definition.?.tables) |table| {
                    if (table.table_id != table_id) continue;
                    if (table.contract != .native_document and table.contract != .native_relational) return error.ChangefeedConflict;
                    break;
                } else return error.ChangefeedConflict;
                if (progress.through <= (root.native_collected_through orelse root.start)) return state;
                const roots = try a.dupe(Root, state.roots);
                roots[index].native_collected_through = progress.through;
                next.roots = roots;
            },
            .finish_bootstrap => |completion| {
                if (state.phase != .bootstrap or !nz(completion.proof) or state.latest_delivery != state.acknowledged_delivery) return error.ChangefeedConflict;
                for (state.roots) |root| if (!root.snapshot_acknowledged.done) return error.ChangefeedConflict;
                next.phase = .streaming;
            },
            .finish_verified_bootstrap => |completion| {
                if (!equal(completion.proof, try state.bootstrapCompletionProof())) return error.ChangefeedConflict;
                next.phase = .streaming;
                const definition = state.definition.?;
                var catalog_roots: ?[]Root = null;
                for (definition.tables) |table| {
                    if (!catalogBacked(table.contract)) continue;
                    // The WAL anchor remains after the snapshot scan.
                    const roots = catalog_roots orelse try a.dupe(Root, state.roots);
                    roots[try state.rootIndex(catalogRootNamespace(table))].snapshot_released = true;
                    catalog_roots = roots;
                }
                if (catalog_roots) |roots| next.roots = roots;
            },
            .start_exported_slot => |start| {
                const slot = state.slot orelse return error.ReplicationSlotNotFound;
                try start.name.verify(state, slot.database, state.definition.?.owner, start.now);
                _ = try slot.restartAt(start.requested_lsn);
                const roots = try a.dupe(Root, state.roots);
                // START explicitly adopts the exported cut. It does not
                // allocate or acknowledge a native delivery or a WAL entry.
                for (roots) |*root| {
                    root.snapshot_delivered = .{ .page = 1, .done = true };
                    root.snapshot_acknowledged = root.snapshot_delivered;
                }
                next.roots = roots;
                next.phase = .streaming;
            },
            .release_snapshot => |release| {
                const index = try state.rootIndex(release.namespace);
                const root = state.roots[index];
                const snapshot = root.snapshot orelse return error.ChangefeedConflict;
                if (!equal(snapshot.id, release.snapshot_id) or !nz(release.proof) or (state.phase != .closing and state.phase != .reset_required and (state.phase != .streaming or !root.snapshot_acknowledged.done))) return error.ChangefeedConflict;
                if (root.snapshot_released) return state;
                const roots = try a.dupe(Root, state.roots);
                roots[index].snapshot_released = true;
                next.roots = roots;
            },
            .renew => |renewal| {
                if (state.phase == .empty or state.phase == .reset_required or state.phase == .closing or state.phase == .deleted or renewal.now >= state.expires_at or renewal.expires_at <= state.expires_at) return error.ChangefeedLeaseExpired;
                try lease(renewal.now, renewal.expires_at);
                next.expires_at = renewal.expires_at;
                next.renewal_pending = state.phase == .streaming and state.slot == null;
            },
            .renew_catalog => |renewal| {
                const definition = state.definition orelse return error.ChangefeedConflict;
                if (state.phase != .streaming or state.slot != null) return error.ChangefeedConflict;
                if (renewal.now >= state.expires_at or renewal.expires_at <= state.expires_at) return error.ChangefeedLeaseExpired;
                const roots = try a.dupe(Root, state.roots);
                var catalog_count: usize = 0;
                for (definition.tables) |table| {
                    if (!catalogBacked(table.contract)) continue;
                    roots[try state.rootIndex(catalogRootNamespace(table))].renewed_through = renewal.expires_at;
                    catalog_count += 1;
                }
                if (definition.events == artifact_event) {
                    for (roots) |*root| root.renewed_through = renewal.expires_at;
                    catalog_count = roots.len;
                }
                if (catalog_count == 0) return error.ChangefeedConflict;
                next.roots = roots;
                next.expires_at = renewal.expires_at;
                next.renewal_pending = catalog_count != roots.len;
            },
            .expire => |now| {
                if (state.phase == .empty or state.phase == .closing or state.phase == .deleted or state.phase == .reset_required) return state;
                if (now >= state.expires_at) {
                    next.phase = .reset_required;
                    next.reset_reason = .lease_expired;
                } else if ((state.phase == .enrolling or state.phase == .bootstrap) and state.bootstrap_expires_at != 0 and now >= state.bootstrap_expires_at) {
                    next.phase = .reset_required;
                    next.reset_reason = .bootstrap_expired;
                } else return state;
            },
            .reset => |reason| {
                if (reason == .none or state.phase == .empty or state.phase == .deleted or state.phase == .closing) return error.ChangefeedConflict;
                if (state.phase == .reset_required) {
                    if (state.reset_reason != reason) return error.ChangefeedConflict;
                    return state;
                }
                next.phase = .reset_required;
                next.reset_reason = reason;
            },
            .close => {
                if (state.phase == .empty) return error.ChangefeedConflict;
                if (state.phase == .closing or state.phase == .deleted) return state;
                next.phase = .closing;
                next.reset_reason = .none;
            },
            .release_root => |release| {
                if (state.phase != .closing and state.phase != .reset_required) return error.ChangefeedConflict;
                const index = try state.rootIndex(release.namespace);
                if (state.roots[index].ownership_epoch != release.ownership_epoch or !nz(release.proof)) return error.ChangefeedConflict;
                if (state.roots[index].released) return state;
                const roots = try a.dupe(Root, state.roots);
                roots[index].released = true;
                next.roots = roots;
            },
            .retire_absent_slot_root, .retire_absent_public_root => |namespace| {
                // The metadata apply store must certify the table ID is absent
                // in the same transaction as this state transition.
                if ((state.phase != .closing and state.phase != .reset_required) or ((state.slot != null) != (self == .retire_absent_slot_root))) return error.ChangefeedConflict;
                const index = try state.rootIndex(namespace);
                if (state.roots[index].released) return state;
                const roots = try a.dupe(Root, state.roots);
                const root = &roots[index];
                root.pending_control = null;
                root.split_handoff = null;
                root.split_grant_sequence = null;
                root.split_consumer_epoch = null;
                root.snapshot_released = root.snapshot != null;
                root.released = true;
                next.roots = roots;
            },
            .finish_close => {
                if (state.phase != .closing) return error.ChangefeedConflict;
                for (state.roots) |root| if (!root.released or root.pending_control != null or root.split_handoff != null or (root.snapshot != null and !root.snapshot_released)) return error.ChangefeedConflict;
                if (state.slot) |slot| if (slot.spool.records.len != 0) return error.ChangefeedConflict;
                next.phase = .deleted;
            },
            .restart => |restart| {
                // Invalidated slots retain their ordering domain; a native
                // feed restart must not silently reuse their LSNs.
                if (state.slot != null) return error.ChangefeedCapabilityUnsupported;
                if (state.phase != .reset_required) return error.ChangefeedConflict;
                for (state.roots) |root| if (!root.released or root.pending_control != null or (root.snapshot != null and !root.snapshot_released)) return error.ChangefeedConflict;
                try restart.definition.validate();
                if (!equal(restart.definition.owner, state.definition.?.owner)) return error.ChangefeedConflict;
                try lease(restart.now, restart.expires_at);
                try bootstrapLease(restart.definition.start, restart.now, restart.expires_at, restart.bootstrap_expires_at);
                next = .{ .id = state.id, .epoch = std.math.add(u64, state.epoch, 1) catch return error.ChangefeedConflict, .authority = state.authority, .definition = restart.definition, .phase = .enrolling, .created_at = restart.now, .expires_at = restart.expires_at, .bootstrap_expires_at = restart.bootstrap_expires_at, .slot_lsn_floor = state.slot_lsn_floor };
            },
        }
        if (next.slot) |slot| if (next.phase == .closing or next.phase == .reset_required) {
            next.slot = try slot.retireSpools(a);
        };
        if (next.renewal_pending) {
            next.renewal_pending = next.phase == .streaming and for (next.roots) |root| {
                if (root.renewed_through < next.expires_at) break true;
            } else false;
        }
        next.revision = std.math.add(u64, state.revision, 1) catch return error.ChangefeedConflict;
        next.authority_sequence = stamp.sequence;
        try next.validate();
        return next;
    }
};
fn applyRootControlEffect(state: State, target: *Root, pending: root_intent.State, effect: root_intent.Effect) !void {
    const before = target.*;
    switch (effect) {
        .enrolled => |v| {
            // Grant refresh may recover an existing epoch
            // without replacing its progress or snapshot.
            if (state.definition.?.start != .now and before.consumer_epoch == 0) return error.ChangefeedCapabilityUnsupported;
            if (before.consumer_epoch != 0) {
                if (before.consumer_epoch != v.consumer_epoch or before.start != v.start) return error.ChangefeedConflict;
            } else {
                target.consumer_epoch = v.consumer_epoch;
                target.start = v.start;
                target.delivered = .{ .sequence = v.start };
                target.acknowledged = .{ .sequence = v.start };
                target.reclaimed_through = v.start;
            }
        },
        .snapshot_enrolled => |v| {
            if (state.phase != .enrolling or state.definition.?.start != .snapshot_then_changes or before.consumer_epoch != 0 or v.expires_at != state.bootstrap_expires_at)
                return error.ChangefeedConflict;
            target.consumer_epoch = v.consumer_epoch;
            target.start = v.start;
            target.delivered = .{ .sequence = v.start };
            target.acknowledged = .{ .sequence = v.start };
            target.reclaimed_through = v.start;
            target.snapshot = .{ .id = state.consumerId(), .digest = v.digest, .expires_at = v.expires_at };
        },
        .acknowledged => |v| {
            if (before.consumer_epoch != v.consumer_epoch or before.reclaimed_through != v.previous or v.next > before.acknowledged.sequence) return error.ChangefeedConflict;
            target.reclaimed_through = v.next;
        },
        .renewed => |v| {
            if (before.consumer_epoch != v.consumer_epoch or v.expires_at < before.renewed_through or v.expires_at > state.expires_at) return error.ChangefeedConflict;
            target.renewed_through = v.expires_at;
        },
        .retired => |v| {
            if ((state.phase != .closing and state.phase != .reset_required) or v.sequence <= before.grant_sequence or v.sequence >= pending.sequence) return error.ChangefeedConflict;
            target.released = true;
        },
        .maintenance, .rejected => {},
    }
}
fn acknowledge(a: A, state: State, next: *State, cursor: Digest) !void {
    if (state.phase != .streaming and state.phase != .bootstrap) return error.ChangefeedResynchronizationRequired;
    if (equal(cursor, state.acknowledged_cursor)) return;
    const selected = for (state.cursors, 0..) |entry, i| {
        if (equal(entry.cursor, cursor)) break i;
    } else return error.ChangefeedCursorExpired;
    const roots = try a.dupe(Root, state.roots);
    for (state.cursors[0 .. selected + 1]) |entry| for (entry.advances) |advance| {
        const root = &roots[try state.rootIndex(advance.namespace)];
        root.acknowledged = advance.position;
        if (advance.snapshot) |position| root.snapshot_acknowledged = position;
    };
    next.roots = roots;
    next.acknowledged_delivery = state.cursors[selected].sequence;
    next.acknowledged_cursor = cursor;
    next.acknowledged_provider_context = state.cursors[selected].provider_context;
    next.cursors = try a.dupe(Delivery, state.cursors[selected + 1 ..]);
}
fn slotDefinition(definition: Definition) !void {
    if (definition.projection != .rows or definition.events != 1) return error.ChangefeedCapabilityUnsupported;
    for (definition.tables) |table| if (table.contract != .native_relational) return error.ChangefeedCapabilityUnsupported;
}
fn lease(now: u64, expires: u64) !void {
    if (now == 0 or expires <= now or expires - now > max_lease_seconds) return error.InvalidChangefeed;
}
fn bootstrapLease(start: Start, now: u64, expires: u64, bootstrap: u64) !void {
    if (start == .now) {
        if (bootstrap != 0) return error.InvalidChangefeed;
    } else if (bootstrap <= now or bootstrap > expires or bootstrap - now > max_bootstrap_seconds) return error.InvalidChangefeed;
}
fn initialCursor(state: State, cut: Digest) Digest {
    return hash(.{
        .domain = "antfly:changefeed-cut:v1",
        .id = state.id,
        .epoch = state.epoch,
        .authority = state.authority,
        .definition = state.definition.?.identity(),
        .cut = cut,
    });
}

pub const SlotStart = struct {
    consistent_lsn: u64,
    cursor: Digest,
    snapshot: ?SlotSnapshot = null,
};

/// Recover the original start after an ambiguous seal. The caller must supply
/// freshly authorized state from a linearizable metadata read.
pub fn recoverSlotStart(state: State, original: Write, now: u64) !SlotStart {
    try original.validate();
    try state.validate();
    if (original.mutation != .seal) return error.InvalidChangefeed;
    const seal = original.mutation.seal;
    const configuration = seal.slot orelse return error.InvalidChangefeed;
    if (state.phase != .bootstrap and state.phase != .streaming) return error.ChangefeedResynchronizationRequired;
    if (now < state.created_at) return error.InvalidChangefeed;
    if (now >= state.expires_at or (state.phase == .bootstrap and now >= state.bootstrap_expires_at)) return error.ChangefeedLeaseExpired;
    const definition = state.definition.?;
    const slot = state.slot orelse return error.ChangefeedConflict;
    if (state.revision <= original.expected_revision or state.epoch != original.expected_epoch or
        !equal(state.id, original.id) or !equal(state.authority, original.authority) or
        !equal(definition.owner, original.owner) or !equal(definition.authorization, original.authorization) or
        !equal(state.cut, seal.cut) or !equal(slot.database, configuration.database) or
        !std.mem.eql(u8, slot.name, configuration.name) or slot.byte_limit != configuration.byte_limit or
        slot.consistent_lsn != configuration.consistent_lsn or !configuration.matchesBinding(slot.binding)) return error.ChangefeedConflict;
    return .{
        .consistent_lsn = slot.consistent_lsn,
        .cursor = initialCursor(state, state.cut),
        .snapshot = if (definition.start == .snapshot_then_changes) try SlotSnapshot.capture(state, configuration.database, original.owner, now) else null,
    };
}

/// The name identifies the exact durable slot cut. Import must also recheck
/// the caller's database, replication privilege and publication binding.
pub const SlotSnapshot = struct {
    feed: Id,
    proof: Digest,
    const prefix = "antfly_v1_";
    pub const name_size = prefix.len + 2 * (@sizeOf(Id) + @sizeOf(Digest));

    pub fn parse(encoded: []const u8) !SlotSnapshot {
        if (encoded.len != name_size or !std.mem.startsWith(u8, encoded, prefix)) return error.InvalidReplicationSnapshot;
        var value: SlotSnapshot = undefined;
        const split = prefix.len + 2 * @sizeOf(Id);
        _ = std.fmt.hexToBytes(&value.feed, encoded[prefix.len..split]) catch return error.InvalidReplicationSnapshot;
        _ = std.fmt.hexToBytes(&value.proof, encoded[split..]) catch return error.InvalidReplicationSnapshot;
        if (!nz(value.feed) or !nz(value.proof) or !std.mem.eql(u8, encoded, &value.name())) return error.InvalidReplicationSnapshot;
        return value;
    }

    pub fn name(self: SlotSnapshot) [name_size]u8 {
        return prefix.* ++ std.fmt.bytesToHex(self.feed, .lower) ++ std.fmt.bytesToHex(self.proof, .lower);
    }

    pub fn capture(state: State, database: Digest, owner: Digest, now: u64) !SlotSnapshot {
        try state.validate();
        const definition = state.definition orelse return error.ReplicationSlotNotFound;
        const slot = state.slot orelse return error.ReplicationSlotNotFound;
        if (!equal(slot.database, database) or !std.crypto.timing_safe.eql(Digest, definition.owner, owner)) return error.Forbidden;
        if (state.phase != .bootstrap or definition.start != .snapshot_then_changes or state.latest_delivery != 0 or slot.session != null)
            return error.ChangefeedResynchronizationRequired;
        if (now < state.created_at or state.expiryDue(now)) return error.ChangefeedLeaseExpired;
        const binding = slot.binding orelse return error.ChangefeedCapabilityUnsupported;
        var proof = hash(.{
            .domain = "antfly:pg-slot-exported-snapshot:v1",
            .feed = state.id,
            .authority = state.authority,
            .epoch = state.epoch,
            .database = database,
            .definition = definition.identity(),
            .binding = binding.identity(),
            .lsn = slot.consistent_lsn,
            .cut = state.cut,
        });
        for (state.roots) |root| {
            const pin = root.snapshot orelse return error.ChangefeedCorrupt;
            if (root.released or root.snapshot_released) return error.ChangefeedResynchronizationRequired;
            proof = hash(.{ .previous = proof, .namespace = root.namespace, .ownership_epoch = root.ownership_epoch, .grant = root.grant_sequence, .consumer_epoch = root.consumer_epoch, .start = root.start, .pin = pin });
        }
        return .{ .feed = state.id, .proof = proof };
    }

    pub fn verify(self: SlotSnapshot, state: State, database: Digest, owner: Digest, now: u64) !void {
        const qualified = try capture(state, database, owner, now);
        if (!equal(self.feed, qualified.feed) or !std.crypto.timing_safe.eql(Digest, self.proof, qualified.proof)) return error.InvalidReplicationSnapshot;
    }
};

pub const Write = struct {
    id: Id,
    authority: Digest,
    owner: Digest,
    authorization: Digest,
    expected_revision: u64,
    expected_epoch: u64,
    mutation: Mutation,
    pub fn validate(self: Write) !void {
        if (!nz(self.id) or !nz(self.authority) or !nz(self.owner) or !nz(self.authorization)) return error.InvalidChangefeed;
        try self.mutation.validate();
    }
    pub fn apply(self: Write, a: A, state: State, sequence: u64) !State {
        try self.validate();
        if (state.revision != self.expected_revision or state.epoch != self.expected_epoch or (state.phase != .empty and (!equal(state.id, self.id) or !equal(state.authority, self.authority) or !equal(state.definition.?.owner, self.owner) or !equal(state.definition.?.authorization, self.authorization)))) return error.ChangefeedConflict;
        if (self.mutation == .create and (!equal(self.mutation.create.id, self.id) or !equal(self.mutation.create.definition.owner, self.owner) or !equal(self.mutation.create.definition.authorization, self.authorization))) return error.InvalidChangefeed;
        return self.mutation.apply(a, state, .{ .incarnation = self.authority, .sequence = sequence });
    }
};
const Envelope = struct { state: State, digest: Digest };
const RawEnvelope = struct { state: std.json.Value, digest: Digest };
pub const Parsed = struct {
    value: State,
    envelope: std.json.Parsed(RawEnvelope),
    typed: std.json.Parsed(State),
    pub fn deinit(self: *@This()) void {
        self.typed.deinit();
        self.envelope.deinit();
        self.* = undefined;
    }
};
pub fn encode(a: A, state: State) ![]u8 {
    try state.validate();
    const bytes = try std.json.Stringify.valueAlloc(a, Envelope{ .state = state, .digest = hash(state) }, .{});
    errdefer a.free(bytes);
    if (bytes.len > max_state_bytes) return error.InvalidChangefeed;
    return bytes;
}
pub fn parse(a: A, bytes: []const u8) !Parsed {
    if (bytes.len == 0 or bytes.len > max_state_bytes) return error.ChangefeedCorrupt;
    var envelope = std.json.parseFromSlice(RawEnvelope, a, bytes, .{ .allocate = .alloc_always, .parse_numbers = false }) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.ChangefeedCorrupt,
    };
    errdefer envelope.deinit();
    var typed = std.json.parseFromValue(State, a, envelope.value.state, .{ .allocate = .alloc_always, .parse_numbers = false }) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.ChangefeedCorrupt,
    };
    errdefer typed.deinit();
    typed.value.validate() catch return error.ChangefeedCorrupt;
    // Reject unknown/deep values through the bounded typed contract before
    // traversing JSON for its digest. Hash the stored representation so missing
    // optional legacy fields do not change existing cursor identities.
    if (!equal(envelope.value.digest, hash(envelope.value.state))) return error.ChangefeedCorrupt;
    return .{ .value = typed.value, .envelope = envelope, .typed = typed };
}
pub fn encodeWrite(a: A, write: Write) ![]u8 {
    try write.validate();
    const bytes = try std.json.Stringify.valueAlloc(a, write, .{});
    errdefer a.free(bytes);
    if (bytes.len > max_state_bytes) return error.InvalidChangefeed;
    return bytes;
}
pub fn requiredProtocolEncoded(bytes: []const u8) u16 {
    var write = parseWrite(std.heap.page_allocator, bytes) catch return verified_bootstrap_protocol_version;
    defer write.deinit();
    return write.value.mutation.requiredProtocol();
}

pub fn parseWrite(a: A, bytes: []const u8) !std.json.Parsed(Write) {
    if (bytes.len == 0 or bytes.len > max_state_bytes) return error.InvalidChangefeed;
    var parsed = std.json.parseFromSlice(Write, a, bytes, .{ .allocate = .alloc_always, .parse_numbers = false }) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.InvalidChangefeed,
    };
    errdefer parsed.deinit();
    try parsed.value.validate();
    return parsed;
}
pub fn validateEncodedWrite(bytes: []const u8) !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    var parsed = try parseWrite(arena.allocator(), bytes);
    defer parsed.deinit();
}
/// Binds the authoritative base incarnation, excluding names, schema evolution
/// and index publication. Runtime capability proof still selects exposure kind.
pub fn tableBinding(table: anytype) Digest {
    return @import("antfly_local_sources").common_topology_records.tableStorageBinding(table);
}
const AuthorityRecord = struct { stamp: Stamp, digest: Digest };
pub fn encodeAuthority(a: A, stamp: Stamp) ![]u8 {
    if (!nz(stamp.incarnation) or stamp.sequence == 0) return error.InvalidChangefeed;
    return std.json.Stringify.valueAlloc(a, AuthorityRecord{ .stamp = stamp, .digest = hash(.{ .domain = "antfly:feed-authority:v1", .stamp = stamp }) }, .{});
}
pub fn parseAuthority(a: A, bytes: []const u8) !Stamp {
    if (bytes.len == 0 or bytes.len > 4096) return error.ChangefeedCorrupt;
    var parsed = std.json.parseFromSlice(AuthorityRecord, a, bytes, .{}) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.ChangefeedCorrupt,
    };
    defer parsed.deinit();
    const record = parsed.value;
    if (!nz(record.stamp.incarnation) or record.stamp.sequence == 0 or !equal(record.digest, hash(.{ .domain = "antfly:feed-authority:v1", .stamp = record.stamp }))) return error.ChangefeedCorrupt;
    return record.stamp;
}
pub fn prefixForGroup(buffer: []u8, group: u64) ![]const u8 {
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:changefeeds:{d}:", .{group});
}
pub fn statePrefixForGroup(buffer: []u8, group: u64) ![]const u8 {
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:changefeeds:{d}:state:", .{group});
}
pub fn keyForGroup(buffer: []u8, group: u64, id: Id) ![]const u8 {
    const prefix = try statePrefixForGroup(buffer, group);
    const hex = std.fmt.bytesToHex(id, .lower);
    if (buffer.len - prefix.len < hex.len) return error.NoSpaceLeft;
    @memcpy(buffer[prefix.len..][0..hex.len], &hex);
    return buffer[0 .. prefix.len + hex.len];
}
pub fn authorityKey(buffer: []u8, group: u64) ![]const u8 {
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:changefeeds:{d}:authority", .{group});
}

fn testNamespace(table: u64) Namespace {
    var result: Namespace = @splat(0);
    std.mem.writeInt(u64, result[0..8], table, .big);
    std.mem.writeInt(u64, result[8..16], 1, .big);
    std.mem.writeInt(u64, result[16..24], 1, .big);
    return result;
}
fn testDefinition(start: Start) Definition {
    return .{ .owner = @splat(1), .authorization = @splat(2), .tables = &.{.{ .table_id = 7, .binding = @splat(3), .policy_revision = 1, .contract = .native_relational }}, .start = start };
}
fn step(a: A, state: State, mutation: Mutation) !State {
    return mutation.apply(a, state, .{ .incarnation = @splat(9), .sequence = state.authority_sequence + 1 });
}
fn testState(a: A, start: Start) !State {
    var state = try step(a, .{}, .{ .create = .{ .id = @splat(4), .definition = testDefinition(start), .now = 100, .expires_at = 1000, .bootstrap_expires_at = if (start == .now) 0 else 500 } });
    state = try step(a, state, .{ .plan_roots = &.{.{ .namespace = testNamespace(7), .ownership_epoch = 1 }} });
    state = try step(a, state, .{ .enroll = .{ .namespace = testNamespace(7), .ownership_epoch = 1, .grant_sequence = state.roots[0].grant_sequence, .consumer_epoch = 6, .start = 10, .snapshot = if (start == .now) null else .{ .id = @splat(5), .digest = @splat(6), .expires_at = 500 } } });
    return step(a, state, .{ .seal = .{ .cut = @splat(7) } });
}
fn testDelivery(state: State, advances: []const Advance) Delivery {
    var delivery: Delivery = .{ .sequence = state.latest_delivery + 1, .before = state.latest_cursor, .cursor = @splat(0), .authority = @splat(8), .issued_at = 200, .advances = advances };
    delivery.cursor = delivery.identity(state.id, state.epoch);
    return delivery;
}

test "changefeed stages a split proof without changing retained cursor positions" {
    const allocator = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var state = try testState(a, .now);
    const roots = try a.dupe(Root, state.roots);
    roots[0].physical = .{ .group_id = 11, .range_id = 21 };
    state.roots = roots;
    const namespace = roots[0].namespace;
    state = try step(a, state, .{ .issue = testDelivery(state, &.{.{ .namespace = namespace, .position = .{ .sequence = 11 } }}) });
    var mutation: Mutation = .{ .stage_split_handoff = .{
        .namespace = namespace,
        .ownership_epoch = 1,
        .handoff = .{
            .predecessor = roots[0].physical.?,
            .successor = .{ .group_id = 12, .range_id = 22 },
            .cut = .{
                .namespace = namespace,
                .transition = 35,
                .attempt = 1,
                .successor_group = 12,
                .term = 2,
                .index = 45,
                .latest = 11,
                .reclaimed = 10,
                .root_generation = 3,
                .reserved_bytes = 0,
                .boundary = @splat(1),
            },
        },
    } };
    const staged = try step(a, state, mutation);
    try std.testing.expectEqual(state.acknowledged_cursor, staged.acknowledged_cursor);
    try std.testing.expectEqual(state.latest_cursor, staged.latest_cursor);
    try std.testing.expectEqual(@as(u64, 11), (try staged.rootsAt(a, staged.latest_cursor))[0].delivered.sequence);
    var decoded = try parse(a, try encode(a, staged));
    defer decoded.deinit();
    try std.testing.expectEqualDeep(staged.roots[0].split_handoff, decoded.value.roots[0].split_handoff);
    try std.testing.expectEqual(staged.revision, (try step(a, staged, mutation)).revision);
    var confirmation: Mutation = .{ .confirm_split_successor = .{
        .namespace = namespace,
        .grant_sequence = staged.roots[0].split_grant_sequence.?,
        .consumer_epoch = 7,
    } };
    const ready = try step(a, staged, confirmation);
    try std.testing.expectEqual(@as(?u64, 7), ready.roots[0].split_consumer_epoch);
    try std.testing.expectEqual(staged.latest_cursor, ready.latest_cursor);
    const closed = try step(a, ready, .close);
    const cleanup: Mutation = .{ .release_split_successor = .{
        .namespace = namespace,
        .grant_sequence = staged.roots[0].split_grant_sequence.?,
        .transition = mutation.stage_split_handoff.handoff.cut.transition,
    } };
    try std.testing.expectError(error.ChangefeedConflict, step(a, ready, cleanup));
    const cleaned = try step(a, closed, cleanup);
    try std.testing.expectEqual(@as(?SplitHandoff, null), cleaned.roots[0].split_handoff);
    try std.testing.expectEqual(@as(?u64, null), cleaned.roots[0].split_consumer_epoch);
    try std.testing.expectEqual(ready.revision, (try step(a, ready, confirmation)).revision);
    const activation: Mutation = .{ .activate_split_successor = .{
        .namespace = namespace,
        .grant_sequence = staged.roots[0].split_grant_sequence.?,
    } };
    try std.testing.expectError(error.ChangefeedConflict, step(a, ready, activation));
    const drained = try step(a, ready, .{ .acknowledge = ready.latest_cursor });
    const active = try step(a, drained, activation);
    try std.testing.expectEqual(drained.latest_cursor, active.latest_cursor);
    const positions = try active.rootsAt(a, drained.latest_cursor);
    try std.testing.expectEqual(@as(usize, 2), positions.len);
    const child = positions[try active.rootIndex(splitStreamNamespace(namespace, .{ .group_id = 12, .range_id = 22 }))];
    try std.testing.expectEqual(namespace, child.storageNamespace());
    try std.testing.expectEqual(@as(u64, 0), child.delivered.sequence);
    try std.testing.expectEqual(@as(u64, 11), positions[try active.rootIndex(namespace)].delivered.sequence);
    var merge: Mutation = .{ .stage_merge_drain = .{
        .namespace = child.namespace,
        .ownership_epoch = child.ownership_epoch,
        .drain = .{
            .donor = child.physical.?,
            .receiver = .{ .group_id = 11, .range_id = 21 },
            .transition = 50,
            .term = 2,
            .index = 60,
            .latest = 3,
            .root_generation = 4,
        },
    } };
    const merging = try step(a, active, merge);
    const donor = merging.roots[try merging.rootIndex(child.namespace)];
    try std.testing.expectEqual(@as(?u64, 3), donor.transitionLimit());
    try std.testing.expectEqual(active.latest_cursor, merging.latest_cursor);
    try std.testing.expectEqual(merging.revision, (try step(a, merging, merge)).revision);
    merge.stage_merge_drain.drain.receiver.range_id += 1;
    try std.testing.expectError(error.ChangefeedResynchronizationRequired, step(a, merging, merge));
    var release: Mutation = .{ .release_merge_donor = .{
        .namespace = child.namespace,
        .transition = 50,
        .receipt_term = 2,
        .receipt_index = 61,
    } };
    try std.testing.expectError(error.ChangefeedConflict, step(a, merging, release));
    var draining = try step(a, merging, .{ .issue = testDelivery(merging, &.{.{ .namespace = child.namespace, .position = .{ .sequence = 3 } }}) });
    draining = try step(a, draining, .{ .acknowledge = draining.latest_cursor });
    draining = try step(a, draining, .{ .reclaimed = .{ .namespace = child.namespace, .consumer_epoch = child.consumer_epoch, .previous = 0, .next = 3 } });
    release.release_merge_donor.receipt_index = 60;
    try std.testing.expectError(error.ChangefeedConflict, step(a, draining, release));
    release.release_merge_donor.receipt_index = 61;
    const released = try step(a, draining, release);
    try std.testing.expect(released.roots[try released.rootIndex(child.namespace)].released);
    try std.testing.expectEqual(draining.latest_cursor, released.latest_cursor);
    const prune: Mutation = .{ .prune_merge_donor = .{ .namespace = child.namespace, .transition = 50 } };
    try std.testing.expectError(error.ChangefeedConflict, step(a, draining, prune));
    const pruned = try step(a, released, prune);
    try std.testing.expectEqual(released.latest_cursor, pruned.latest_cursor);
    const retained = try pruned.rootsAt(a, pruned.latest_cursor);
    try std.testing.expectEqual(@as(usize, 1), retained.len);
    try std.testing.expectEqual(namespace, retained[0].namespace);
    confirmation.confirm_split_successor.consumer_epoch += 1;
    try std.testing.expectError(error.ChangefeedConflict, step(a, ready, confirmation));
    mutation.stage_split_handoff.handoff.cut.root_generation += 1;
    try std.testing.expectError(error.ChangefeedConflict, step(a, staged, mutation));
}

test "changefeed control separates partial delivery ACK and native reclamation" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var state = try testState(scratch, .now);
    const first = testDelivery(state, &.{.{ .namespace = testNamespace(7), .position = .{ .sequence = 10, .event = 3 } }});
    state = try step(scratch, state, .{ .issue = first });
    try std.testing.expectEqual(@as(u32, 3), state.roots[0].delivered.event);
    try std.testing.expectEqual(@as(u32, 0), state.roots[0].acknowledged.event);
    try std.testing.expectError(error.ChangefeedConflict, step(scratch, state, .{ .reclaimed = .{ .namespace = testNamespace(7), .consumer_epoch = 6, .previous = 10, .next = 11 } }));
    try std.testing.expectError(error.ChangefeedCursorExpired, step(scratch, state, .{ .acknowledge = @splat(99) }));
    state = try step(scratch, state, .{ .acknowledge = first.cursor });
    try std.testing.expectEqual(@as(u32, 3), state.roots[0].acknowledged.event);
    try std.testing.expect(!state.atDeliveryBoundary());
    try std.testing.expectEqual(@as(u64, 10), state.roots[0].reclaimed_through);
    try std.testing.expectError(error.ChangefeedConflict, step(scratch, state, .{ .reclaimed = .{ .namespace = testNamespace(7), .consumer_epoch = 6, .previous = 10, .next = 11 } }));
    const second = testDelivery(state, &.{.{ .namespace = testNamespace(7), .position = .{ .sequence = 11 } }});
    state = try step(scratch, state, .{ .issue = second });
    state = try step(scratch, state, .{ .acknowledge = second.cursor });
    try std.testing.expect(state.atDeliveryBoundary());
    try std.testing.expectEqual(@as(u64, 11), state.pendingNativeAcknowledgment().?.acknowledged.sequence);
    try std.testing.expect(state.needsMaintenance(102));
    state = try step(scratch, state, .{ .reclaimed = .{ .namespace = testNamespace(7), .consumer_epoch = 6, .previous = 10, .next = 11 } });
    try std.testing.expectEqual(@as(u64, 11), state.roots[0].reclaimed_through);
    try std.testing.expect(state.pendingNativeAcknowledgment() != null);
    state = try step(scratch, state, .{ .native_collected = .{ .namespace = testNamespace(7), .consumer_epoch = 6, .through = 11 } });
    try std.testing.expect(state.pendingNativeAcknowledgment() == null);
    try std.testing.expect(!state.needsMaintenance(102));
    try std.testing.expectError(error.ChangefeedCursorExpired, step(scratch, state, .{ .acknowledge = first.cursor }));
    const encoded = try encode(a, state);
    defer a.free(encoded);
    var reopened = try parse(a, encoded);
    defer reopened.deinit();
    try std.testing.expectEqual(state.acknowledged_cursor, reopened.value.acknowledged_cursor);
    try std.testing.expectEqual(@as(u64, 11), reopened.value.roots[0].reclaimed_through);
}

test "changefeed control bounds unacknowledged cursors and rejects a forged advance" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var state = try testState(scratch, .now);
    var definition = state.definition.?;
    definition.cursor_window = 2;
    state.definition = definition;
    const first = testDelivery(state, &.{});
    state = try step(scratch, state, .{ .issue = first });
    var forged = testDelivery(state, &.{});
    forged.cursor = @splat(1);
    try std.testing.expectError(error.ChangefeedConflict, step(scratch, state, .{ .issue = forged }));
    const second = testDelivery(state, &.{});
    state = try step(scratch, state, .{ .issue = second });
    try std.testing.expectError(error.ChangefeedBackpressure, step(scratch, state, .{ .issue = testDelivery(state, &.{}) }));
    state = try step(scratch, state, .{ .acknowledge = first.cursor });
    state = try step(scratch, state, .{ .issue = testDelivery(state, &.{}) });
    try std.testing.expectEqual(@as(usize, 2), state.cursors.len);
}

test "changefeed control bootstrap completes only after snapshot ACK and retains cleanup debt" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var state = try testState(scratch, .snapshot_then_changes);
    try std.testing.expectError(error.InvalidChangefeed, step(scratch, state, .{ .issue = testDelivery(state, &.{.{ .namespace = testNamespace(7), .position = .{ .sequence = 11 } }}) }));
    const page = testDelivery(state, &.{.{ .namespace = testNamespace(7), .position = .{ .sequence = 10 }, .snapshot = .{ .page = 1, .continuation = "opaque-page-cut", .done = true } }});
    state = try step(scratch, state, .{ .issue = page });
    try std.testing.expectError(error.ChangefeedConflict, step(scratch, state, .{ .finish_verified_bootstrap = .{ .proof = @splat(1) } }));
    try std.testing.expect(!state.needsMaintenance(200));
    state = try step(scratch, state, .{ .acknowledge = page.cursor });
    try std.testing.expect(state.needsMaintenance(200));
    const proof = try state.bootstrapCompletionProof();
    const legacy_finish: Mutation = .{ .finish_bootstrap = .{ .proof = @splat(1) } };
    const verified_finish: Mutation = .{ .finish_verified_bootstrap = .{ .proof = proof } };
    try std.testing.expectEqual(protocol_version, legacy_finish.requiredProtocol());
    const definition = state.definition.?;
    const verified_bytes = try encodeWrite(scratch, .{
        .id = state.id,
        .authority = state.authority,
        .owner = definition.owner,
        .authorization = definition.authorization,
        .expected_revision = state.revision,
        .expected_epoch = state.epoch,
        .mutation = verified_finish,
    });
    try std.testing.expectEqual(verified_bootstrap_protocol_version, requiredProtocolEncoded(verified_bytes));
    var different_cut = state;
    different_cut.cut[0] ^= 1;
    var different_epoch = state;
    different_epoch.epoch += 1;
    try std.testing.expectError(error.ChangefeedConflict, step(scratch, state, .{ .finish_verified_bootstrap = .{ .proof = try different_cut.bootstrapCompletionProof() } }));
    try std.testing.expectError(error.ChangefeedConflict, step(scratch, state, .{ .finish_verified_bootstrap = .{ .proof = try different_epoch.bootstrapCompletionProof() } }));
    try std.testing.expectEqual(Phase.streaming, (try step(scratch, state, legacy_finish)).phase);
    state = try step(scratch, state, verified_finish);
    try std.testing.expectEqual(Phase.streaming, state.phase);
    try std.testing.expect(!state.roots[0].snapshot_released);
    state = try step(scratch, state, .{ .release_snapshot = .{ .namespace = testNamespace(7), .snapshot_id = @splat(5), .proof = @splat(1) } });
    try std.testing.expect(state.roots[0].snapshot_released);
    state = try step(scratch, state, .{ .close = {} });
    try std.testing.expectError(error.ChangefeedConflict, step(scratch, state, .finish_close));
    state = try step(scratch, state, .{ .release_root = .{ .namespace = testNamespace(7), .ownership_epoch = 1, .proof = @splat(1) } });
    state = try step(scratch, state, .finish_close);
    try std.testing.expectEqual(Phase.deleted, state.phase);
}

test "changefeed control policy reset expires cursors and changes consumer epoch only after cleanup" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var state = try testState(scratch, .now);
    const consumer = state.consumerId();
    state = try step(scratch, state, .{ .reset = .policy_changed });
    try std.testing.expectError(error.ChangefeedResynchronizationRequired, step(scratch, state, .{ .issue = testDelivery(state, &.{}) }));
    var definition = state.definition.?;
    definition.authorization = @splat(10);
    const restart: Mutation = .{ .restart = .{ .definition = definition, .now = 300, .expires_at = 1000 } };
    try std.testing.expectError(error.ChangefeedConflict, step(scratch, state, restart));
    state = try step(scratch, state, .{ .release_root = .{ .namespace = testNamespace(7), .ownership_epoch = 1, .proof = @splat(1) } });
    state = try step(scratch, state, restart);
    try std.testing.expectEqual(@as(u64, 2), state.epoch);
    try std.testing.expect(!equal(consumer, state.consumerId()));
    try std.testing.expectEqual(Phase.enrolling, state.phase);
    var bootstrap = try testState(scratch, .snapshot_then_changes);
    bootstrap = try step(scratch, bootstrap, .{ .expire = 500 });
    try std.testing.expectEqual(Reset.bootstrap_expired, bootstrap.reset_reason);
    try std.testing.expectError(error.ChangefeedLeaseExpired, step(scratch, bootstrap, .{ .renew = .{ .now = 501, .expires_at = 1100 } }));
}

test "public feed pressure reset stops probes and allows absent-root retirement" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var state = try testState(a, .now);
    const retire: Mutation = .{ .retire_absent_public_root = testNamespace(7) };
    try std.testing.expect(state.needsPressureProbe(200));
    try std.testing.expectError(error.ChangefeedConflict, step(a, state, retire));
    state = try step(a, state, .{ .reset = .quota });
    try std.testing.expect(!state.needsPressureProbe(200));
    try std.testing.expect(state.needsMaintenance(200));
    state = try step(a, state, retire);
    try std.testing.expect(state.roots[0].released);
    try std.testing.expect(!state.needsMaintenance(200));
}

test "changefeed control verifies complete table enrollment owner rebinding and corruption" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var state = try testState(scratch, .now);
    state = try step(scratch, state, .{ .rebind = .{ .namespace = testNamespace(7), .previous = 1, .next = 2, .proof = @splat(1) } });
    try std.testing.expectEqual(@as(u64, 2), state.roots[0].ownership_epoch);
    try std.testing.expectEqual(@as(u64, 10), state.roots[0].acknowledged.sequence);
    state = try step(scratch, state, .{ .close = {} });
    try std.testing.expectError(error.ChangefeedConflict, step(scratch, state, .{ .release_root = .{ .namespace = testNamespace(7), .ownership_epoch = 1, .proof = @splat(1) } }));
    const raw = try encode(a, state);
    defer a.free(raw);
    var root = try std.json.parseFromSlice(std.json.Value, a, raw, .{});
    defer root.deinit();
    root.value.object.getPtr("state").?.object.getPtr("revision").?.* = .{ .integer = 999 };
    const corrupt = try std.json.Stringify.valueAlloc(a, root.value, .{});
    defer a.free(corrupt);
    try std.testing.expectError(error.ChangefeedCorrupt, parse(a, corrupt));
    var definition = testDefinition(.now);
    definition.tables = &.{ .{ .table_id = 7, .binding = @splat(3), .policy_revision = 1, .contract = .native_relational }, .{ .table_id = 8, .binding = @splat(4), .policy_revision = 1, .contract = .native_relational } };
    var partial = try step(scratch, .{}, .{ .create = .{ .id = @splat(4), .definition = definition, .now = 100, .expires_at = 1000 } });
    partial = try step(scratch, partial, .{ .plan_roots = &.{.{ .namespace = testNamespace(7), .ownership_epoch = 1 }} });
    partial = try step(scratch, partial, .{ .enroll = .{ .namespace = testNamespace(7), .ownership_epoch = 1, .grant_sequence = partial.roots[0].grant_sequence, .consumer_epoch = 1, .start = 0 } });
    try std.testing.expectError(error.ChangefeedConflict, step(scratch, partial, .{ .seal = .{ .cut = @splat(1) } }));
    definition.tables = &.{.{ .table_id = 7, .binding = @splat(3), .policy_revision = 1, .contract = .external_snapshot_lifecycle }};
    try std.testing.expectError(error.InvalidChangefeed, definition.validate());
}

pub fn readOnService(svc: anytype, a: A, id: Id, request: anytype) ![]u8 {
    if (comptime @hasDecl(@typeInfo(@TypeOf(svc)).pointer.child, "readChangefeedState")) return svc.readChangefeedState(a, id, request);
    try svc.ensureLinearizableReadWithContext(request);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    return store.getChangefeedControl(a, svc.metadata_group_id, id);
}
pub fn mutateOnService(svc: anytype, a: A, write: Write, request: anytype) !void {
    if (write.mutation == .finish_bootstrap) return error.InvalidChangefeed;
    if (comptime @hasDecl(@typeInfo(@TypeOf(svc)).pointer.child, "mutateChangefeedState")) return svc.mutateChangefeedState(a, write, request);
    try write.validate();
    try request.ensureActive();
    _ = try svc.ensureTableTopologyProtocolReadyWithContext(request, write.mutation.requiredProtocol());
    try svc.ensureLinearizableReadWithContext(request);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    const bytes = try store.getChangefeedControl(a, svc.metadata_group_id, write.id);
    defer a.free(bytes);
    var before = try parse(a, bytes);
    defer before.deinit();
    const authority = try store.getChangefeedAuthority(a, svc.metadata_group_id);
    if (authority) |current| if (!equal(current.incarnation, write.authority)) return error.ChangefeedConflict;
    const minimum_sequence = if (authority) |current| std.math.add(u64, current.sequence, 1) catch return error.ChangefeedConflict else 1;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const predicted = try write.apply(arena.allocator(), before.value, minimum_sequence);
    if (predicted.revision == before.value.revision) return;
    const command = try encodeWrite(arena.allocator(), write);
    try request.ensureActive();
    const receipt = try svc.proposeTransitionCommandWithReceipt(.{ .apply_changefeed_control = command });
    svc.waitForTransitionAppliedWithContext(receipt, request) catch return error.MetadataMutationOutcomeUnknown;
    const observed_bytes = try store.getChangefeedControl(a, svc.metadata_group_id, write.id);
    defer a.free(observed_bytes);
    var observed = try parse(a, observed_bytes);
    defer observed.deinit();
    if (observed.value.authority_sequence < minimum_sequence) return error.MetadataMutationOutcomeUnknown;
    // Other feeds can advance the shared authority counter before this command
    // commits. Recompute with the actual stamp, preserving exact state proof.
    const expected = try write.apply(arena.allocator(), before.value, observed.value.authority_sequence);
    if (!equal(hash(expected), hash(observed.value))) return error.MetadataMutationOutcomeUnknown;
}

test "changefeed service verifies applied outcomes across lost ACK and shared authority races" {
    const a = std.testing.allocator;
    const Context = struct {
        pub fn ensureActive(_: @This()) !void {}
    };
    const Service = struct {
        a: A,
        value: []u8,
        metadata_group_id: u64 = 1,
        authority: ?Stamp = null,
        proposals: u64 = 0,
        blocked: bool = true,
        lose_ack: bool = false,
        reject: bool = false,
        interleave: bool = false,
        pub fn ensureTableTopologyProtocolReadyWithContext(self: *@This(), _: Context, version: u16) !void {
            try std.testing.expectEqual(protocol_version, version);
            if (self.blocked) return error.TableTopologyProtocolUpgradeRequired;
        }
        pub fn ensureLinearizableReadWithContext(_: *@This(), _: Context) !void {}
        pub fn projectedStore(self: *@This()) ?*@This() {
            return self;
        }
        pub fn getChangefeedControl(self: *@This(), allocator: A, _: u64, _: Id) ![]u8 {
            return allocator.dupe(u8, self.value);
        }
        pub fn getChangefeedAuthority(self: *@This(), _: A, _: u64) !?Stamp {
            return self.authority;
        }
        pub fn proposeTransitionCommandWithReceipt(self: *@This(), command: anytype) !u64 {
            self.proposals += 1;
            if (!self.reject) {
                var write = try parseWrite(self.a, command.apply_changefeed_control);
                defer write.deinit();
                var current = try parse(self.a, self.value);
                defer current.deinit();
                var arena = std.heap.ArenaAllocator.init(self.a);
                defer arena.deinit();
                const sequence = (if (self.authority) |stamp| stamp.sequence else @as(u64, 0)) + 1 + @as(u64, if (self.interleave) 1 else 0);
                const next = try write.value.apply(arena.allocator(), current.value, sequence);
                const encoded = try encode(self.a, next);
                self.a.free(self.value);
                self.value = encoded;
                self.authority = .{ .incarnation = write.value.authority, .sequence = sequence };
            }
            return self.proposals;
        }
        pub fn waitForTransitionAppliedWithContext(self: *@This(), _: u64, _: Context) !void {
            if (self.lose_ack) return error.TestLostAcknowledgment;
        }
    };
    var svc: Service = .{ .a = a, .value = try encode(a, .{}) };
    defer a.free(svc.value);
    const definition = testDefinition(.now);
    var write: Write = .{ .id = @splat(4), .authority = @splat(9), .owner = definition.owner, .authorization = definition.authorization, .expected_revision = 0, .expected_epoch = 0, .mutation = .{ .create = .{ .id = @splat(4), .definition = definition, .now = 100, .expires_at = 1000 } } };
    try std.testing.expectError(error.TableTopologyProtocolUpgradeRequired, mutateOnService(&svc, a, write, Context{}));
    try std.testing.expectEqual(@as(u64, 0), svc.proposals);
    svc.blocked = false;
    svc.lose_ack = true;
    try std.testing.expectError(error.MetadataMutationOutcomeUnknown, mutateOnService(&svc, a, write, Context{}));
    const recovered = try readOnService(&svc, a, write.id, Context{});
    defer a.free(recovered);
    var state = try parse(a, recovered);
    defer state.deinit();
    try std.testing.expectEqual(Phase.enrolling, state.value.phase);
    try std.testing.expectEqual(@as(u64, 1), state.value.authority_sequence);
    svc.lose_ack = false;
    write.expected_revision = state.value.revision;
    write.expected_epoch = state.value.epoch;
    write.mutation = .{ .plan_roots = &.{.{ .namespace = testNamespace(7), .ownership_epoch = 1 }} };
    svc.reject = true;
    try std.testing.expectError(error.MetadataMutationOutcomeUnknown, mutateOnService(&svc, a, write, Context{}));
    svc.reject = false;
    svc.interleave = true;
    try mutateOnService(&svc, a, write, Context{});
    var planned = try parse(a, svc.value);
    defer planned.deinit();
    try std.testing.expectEqual(@as(u64, 3), planned.value.authority_sequence);
    try std.testing.expectEqual(@as(u64, 3), planned.value.roots[0].grant_sequence);
    write.expected_revision = planned.value.revision;
    write.owner = @splat(99);
    try std.testing.expectError(error.ChangefeedConflict, mutateOnService(&svc, a, write, Context{}));
}

test "changefeed cursor reconstruction preserves each retained partial frame position" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var state = try testState(scratch, .now);
    const initial = state.latest_cursor;
    var first: Delivery = .{ .sequence = 1, .before = initial, .cursor = @splat(0), .authority = @splat(1), .issued_at = 101, .advances = &.{.{ .namespace = testNamespace(7), .position = .{ .sequence = 10, .event = 3 } }} };
    first.cursor = first.identity(state.id, state.epoch);
    state = try step(scratch, state, .{ .issue = first });
    var second: Delivery = .{ .sequence = 2, .before = first.cursor, .cursor = @splat(0), .authority = @splat(1), .issued_at = 102, .advances = &.{.{ .namespace = testNamespace(7), .position = .{ .sequence = 11 } }} };
    second.cursor = second.identity(state.id, state.epoch);
    state = try step(scratch, state, .{ .issue = second });
    try std.testing.expectEqual(@as(u64, 10), (try state.rootsAt(scratch, initial))[0].delivered.sequence);
    try std.testing.expectEqual(@as(u32, 0), (try state.rootsAt(scratch, initial))[0].delivered.event);
    try std.testing.expectEqual(@as(u32, 3), (try state.rootsAt(scratch, first.cursor))[0].delivered.event);
    try std.testing.expectEqual(@as(u64, 11), (try state.rootsAt(scratch, second.cursor))[0].delivered.sequence);
    state = try step(scratch, state, .{ .acknowledge = first.cursor });
    try std.testing.expectError(error.ChangefeedCursorExpired, state.rootsAt(scratch, initial));
    try std.testing.expectEqual(@as(u32, 3), (try state.rootsAt(scratch, first.cursor))[0].delivered.event);
    var enrolling = try step(scratch, .{}, .{ .create = .{ .id = @splat(4), .definition = testDefinition(.snapshot_then_changes), .now = 100, .expires_at = 1000, .bootstrap_expires_at = 500 } });
    enrolling = try step(scratch, enrolling, .{ .expire = 500 });
    try std.testing.expectEqual(Reset.bootstrap_expired, enrolling.reset_reason);
}

test "changefeed control refreshes old grants without changing root owner or read position" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var state = try testState(a, .now);
    const root = state.roots[0];
    state = try step(a, state, .{ .refresh_grant = .{ .namespace = root.namespace, .previous = root.grant_sequence } });
    try std.testing.expect(state.roots[0].grant_sequence > root.grant_sequence);
    try std.testing.expectEqual(root.ownership_epoch, state.roots[0].ownership_epoch);
    try std.testing.expectEqual(root.consumer_epoch, state.roots[0].consumer_epoch);
    try std.testing.expectEqual(root.acknowledged, state.roots[0].acknowledged);
    try std.testing.expectEqual(root.delivered, state.roots[0].delivered);
    try std.testing.expectError(error.ChangefeedConflict, step(a, state, .{ .refresh_grant = .{ .namespace = root.namespace, .previous = root.grant_sequence } }));
    state = try step(a, state, .close);
    try std.testing.expectError(error.ChangefeedConflict, step(a, state, .{ .refresh_grant = .{ .namespace = root.namespace, .previous = state.roots[0].grant_sequence } }));
}

test "changefeed closing owner relocation preserves the retired enrollment grant" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var state = try testState(a, .now);
    state = try step(a, state, .close);
    const grant = state.roots[0].grant_sequence;
    state = try step(a, state, .{ .rebind = .{ .namespace = state.roots[0].namespace, .previous = 1, .next = 2, .proof = @splat(1) } });
    try std.testing.expectEqual(grant, state.roots[0].grant_sequence);
    try std.testing.expect(state.authority_sequence > grant);
    try std.testing.expectEqual(@as(u64, 2), state.roots[0].ownership_epoch);
}

test "changefeed control preserves private continuation at each cursor across ACK and reopen" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const start = Start.now;
    var enrolled = try step(a, .{}, .{ .create = .{ .id = @splat(4), .definition = testDefinition(start), .now = 100, .expires_at = 1000, .bootstrap_expires_at = 0 } });
    enrolled = try step(a, enrolled, .{ .plan_roots = &.{.{ .namespace = testNamespace(7), .ownership_epoch = 1 }} });
    enrolled = try step(a, enrolled, .{ .enroll = .{ .namespace = testNamespace(7), .ownership_epoch = 1, .grant_sequence = enrolled.roots[0].grant_sequence, .consumer_epoch = 6, .start = 10 } });
    var state = try step(a, enrolled, .{ .seal = .{ .cut = @splat(7), .provider_context = "Yw" } });
    try std.testing.expect(!equal(state.latest_cursor, (try step(a, enrolled, .{ .seal = .{ .cut = @splat(7) } })).latest_cursor));
    const initial = state.latest_cursor;
    var first: Delivery = .{ .sequence = 1, .before = initial, .cursor = @splat(0), .authority = @splat(1), .issued_at = 101, .provider_context = "YQ", .advances = &.{.{ .namespace = testNamespace(7), .position = .{ .sequence = 10, .event = 3 } }} };
    first.cursor = first.identity(state.id, state.epoch);
    state = try step(a, state, .{ .issue = first });
    try std.testing.expectEqualStrings("Yw", try state.contextAt(initial));
    try std.testing.expectEqualStrings("YQ", try state.contextAt(first.cursor));
    state = try step(a, state, .{ .acknowledge = first.cursor });
    const bytes = try encode(a, state);
    var reopened = try parse(std.testing.allocator, bytes);
    defer reopened.deinit();
    try std.testing.expectEqualStrings("YQ", try reopened.value.contextAt(first.cursor));
    try std.testing.expectError(error.ChangefeedCursorExpired, reopened.value.contextAt(initial));
    var second: Delivery = .{ .sequence = 2, .before = first.cursor, .cursor = @splat(0), .authority = @splat(2), .issued_at = 102, .provider_context = "Yg", .advances = &.{.{ .namespace = testNamespace(7), .position = .{ .sequence = 10, .event = 5 } }} };
    second.cursor = second.identity(state.id, state.epoch);
    state = try step(a, reopened.value, .{ .issue = second });
    try std.testing.expectEqualStrings("YQ", try state.contextAt(first.cursor));
    try std.testing.expectEqualStrings("Yg", try state.contextAt(second.cursor));
    try std.testing.expect(!validProviderContext("not-json:{}"));
}

test "changefeed control reads legacy envelopes without rebinding cursor identities" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var state = try testState(a, .now);
    var delivery: Delivery = .{ .sequence = 1, .before = state.latest_cursor, .cursor = @splat(0), .authority = @splat(1), .issued_at = 101, .advances = &.{} };
    delivery.cursor = delivery.identity(state.id, state.epoch);
    state = try step(a, state, .{ .issue = delivery });
    var raw = try std.json.parseFromSlice(std.json.Value, a, try encode(a, state), .{ .parse_numbers = false });
    defer raw.deinit();
    const record = raw.value.object.getPtr("state").?;
    _ = record.object.orderedRemove("slot");
    for (record.object.getPtr("roots").?.array.items) |*root| {
        _ = root.object.orderedRemove("pending_control");
        _ = root.object.orderedRemove("last_control");
        _ = root.object.orderedRemove("last_reconciliation");
    }
    _ = record.object.orderedRemove("latest_provider_context");
    _ = record.object.orderedRemove("acknowledged_provider_context");
    for (record.object.getPtr("cursors").?.array.items) |*receipt| _ = receipt.object.orderedRemove("provider_context");
    const checksum = try std.json.Stringify.valueAlloc(a, hash(record.*), .{});
    raw.value.object.getPtr("digest").?.* = try std.json.parseFromSliceLeaky(std.json.Value, a, checksum, .{ .parse_numbers = false });
    const legacy = try std.json.Stringify.valueAlloc(a, raw.value, .{});
    var parsed = try parse(std.testing.allocator, legacy);
    defer parsed.deinit();
    try std.testing.expectEqual(delivery.cursor, parsed.value.latest_cursor);
    try std.testing.expectEqualStrings("", try parsed.value.contextAt(delivery.cursor));
    try std.testing.expectEqual(@as(usize, 1), parsed.value.cursors.len);
}

test "changefeed control rejects deeply nested unknown state before digest traversal" {
    const a = std.testing.allocator;
    var bytes: std.ArrayList(u8) = .empty;
    defer bytes.deinit(a);
    try bytes.appendSlice(a, "{\"state\":{\"unknown\":");
    for (0..10000) |_| try bytes.append(a, '[');
    try bytes.appendSlice(a, "null");
    for (0..10000) |_| try bytes.append(a, ']');
    try bytes.appendSlice(a, "},\"digest\":[");
    for (0..32) |i| try bytes.appendSlice(a, if (i == 0) "0" else ",0");
    try bytes.appendSlice(a, "]}");
    try std.testing.expectError(error.ChangefeedCorrupt, parse(a, bytes.items));
}

fn testSlotState(a: A) !State {
    return testSlotStateAt(a, 1);
}

fn testSlotStateAt(a: A, lsn: u64) !State {
    var definition = testDefinition(.now);
    definition.projection = .rows;
    const id = pg_slot.feedId(@splat(1), "slot");
    var state = try step(a, .{}, .{ .create = .{ .id = id, .definition = definition, .now = 100, .expires_at = 1000 } });
    state = try step(a, state, .{ .plan_roots = &.{.{ .namespace = testNamespace(7), .ownership_epoch = 1 }} });
    state = try step(a, state, .{ .enroll = .{ .namespace = testNamespace(7), .ownership_epoch = 1, .grant_sequence = state.roots[0].grant_sequence, .consumer_epoch = 6, .start = 10 } });
    const configuration: pg_slot.Configuration = .{ .database = @splat(1), .name = "slot", .consistent_lsn = lsn };
    state = try step(a, state, .{ .seal = .{ .cut = @splat(7), .slot = configuration } });
    try std.testing.expectEqual(state.acknowledged_cursor, state.slot.?.mapped_cursor);
    try std.testing.expectEqual(lsn, state.slot.?.confirmed_lsn);
    return step(a, state, .{ .slot = .{ .acquire = .{ .actor = @splat(2), .expected = 0, .now = 100, .expires = 150 } } });
}
test "recreating a retired logical slot preserves its LSN high water and fences former writes" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const original = try testSlotStateAt(a, 4096);
    var definition = original.definition.?;
    definition.authorization = @splat(91);
    const recreate: Mutation = .{ .recreate_slot = .{ .definition = definition, .now = 102, .expires_at = 1000 } };
    try std.testing.expectError(error.ChangefeedConflict, step(a, original, recreate));
    var state = try step(a, original, .close);
    try std.testing.expectError(error.ChangefeedConflict, step(a, state, recreate));
    state = try step(a, state, .{ .release_root = .{ .namespace = testNamespace(7), .ownership_epoch = 1, .proof = @splat(19) } });
    state = try step(a, state, .finish_close);
    var wrong_owner = recreate;
    wrong_owner.recreate_slot.definition.owner = @splat(92);
    try std.testing.expectError(error.ChangefeedConflict, step(a, state, wrong_owner));
    const write: Write = .{
        .id = state.id,
        .authority = state.authority,
        .owner = state.definition.?.owner,
        .authorization = state.definition.?.authorization,
        .expected_revision = state.revision,
        .expected_epoch = state.epoch,
        .mutation = recreate,
    };
    try std.testing.expectEqual(slot_recreation_protocol_version, requiredProtocolEncoded(try encodeWrite(a, write)));
    state = try write.apply(a, state, state.authority_sequence + 1);
    try std.testing.expectError(error.ChangefeedConflict, write.apply(a, state, state.authority_sequence + 1));
    try std.testing.expectEqual(original.epoch + 1, state.epoch);
    try std.testing.expectEqual(@as(u64, 4097), state.slot_lsn_floor);
    var parsed = try parse(a, try encode(a, state));
    defer parsed.deinit();
    state = try step(a, parsed.value, .{ .plan_roots = &.{.{ .namespace = testNamespace(7), .ownership_epoch = 1 }} });
    state = try step(a, state, .{ .enroll = .{ .namespace = testNamespace(7), .ownership_epoch = 1, .grant_sequence = state.roots[0].grant_sequence, .consumer_epoch = 7, .start = 15 } });
    const configuration: pg_slot.Configuration = .{ .database = @splat(1), .name = "slot", .consistent_lsn = state.slot_lsn_floor };
    state = try step(a, state, .{ .seal = .{ .cut = @splat(8), .slot = configuration } });
    try std.testing.expectError(error.ChangefeedResynchronizationRequired, state.slot.?.restartAt(4096));
    try std.testing.expectEqual(@as(u64, 4097), try state.slot.?.restartAt(0));
}

test "changefeed logical slot confirms native ACK atomically and fences ordinary acknowledgements" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const lsn = 4096;
    var state = try testSlotStateAt(a, lsn);
    try std.testing.expectError(error.ChangefeedConflict, step(a, state, .{ .slot = .{ .configure = .{ .database = @splat(1), .name = "slot", .consistent_lsn = lsn + 1 } } }));
    const original: Write = .{
        .id = state.id,
        .authority = state.authority,
        .owner = state.definition.?.owner,
        .authorization = state.definition.?.authorization,
        .expected_revision = state.revision - 2,
        .expected_epoch = state.epoch,
        .mutation = .{ .seal = .{ .cut = state.cut, .slot = .{ .database = @splat(1), .name = "slot", .consistent_lsn = lsn } } },
    };
    const initial = state.latest_cursor;
    const session = state.slot.?.session.?;
    const delivery = testDelivery(state, &.{.{ .namespace = testNamespace(7), .position = .{ .sequence = 11 } }});
    state = try step(a, state, .{ .issue = delivery });
    state = try step(a, state, .{ .slot = .{ .configure = .{ .database = @splat(1), .name = "slot", .consistent_lsn = lsn } } });
    const transaction: pg_slot.Prepared = .{ .before = initial, .after = delivery.cursor, .content = @splat(3), .completion = @splat(4), .bytes = 100, .commit_offset = 74, .xid = 42, .commit_time = -1 };
    state = try step(a, state, .{ .slot = .{ .issue = .{ .session = session, .now = 101, .transaction = transaction } } });
    try std.testing.expectError(error.ChangefeedConflict, step(a, state, .{ .acknowledge = delivery.cursor }));
    const encoded = try encode(a, state);
    var parsed = try parse(std.testing.allocator, encoded);
    defer parsed.deinit();
    try std.testing.expectEqual(@as(u64, lsn + 100), parsed.value.slot.?.next_lsn);
    const partial = try step(a, parsed.value, .{ .slot = .{ .confirm = .{ .session = session, .now = 102, .flushed = lsn + 99 } } });
    try std.testing.expectEqual(@as(u64, 0), partial.acknowledged_delivery);
    state = try step(a, partial, .{ .slot = .{ .confirm = .{ .session = session, .now = 103, .flushed = lsn + 100 } } });
    try std.testing.expectEqual(@as(u64, 1), state.acknowledged_delivery);
    try std.testing.expectEqual(@as(u64, 11), state.roots[0].acknowledged.sequence);
    try std.testing.expectEqual(@as(u64, 10), state.roots[0].reclaimed_through);
    try std.testing.expectEqual(@as(u64, lsn + 100), state.slot.?.confirmed_lsn);
    const start = try recoverSlotStart(state, original, 103);
    try std.testing.expectEqual(@as(u64, lsn), start.consistent_lsn);
    try std.testing.expectEqual(initial, start.cursor);
    var mismatched = original;
    mismatched.mutation.seal.cut[0] ^= 1;
    try std.testing.expectError(error.ChangefeedConflict, recoverSlotStart(state, mismatched, 103));
    mismatched = original;
    mismatched.expected_epoch += 1;
    try std.testing.expectError(error.ChangefeedConflict, recoverSlotStart(state, mismatched, 103));
    try std.testing.expectError(error.ChangefeedLeaseExpired, recoverSlotStart(state, original, state.expires_at));
    try std.testing.expectEqual(@as(usize, 0), state.cursors.len);
    try std.testing.expectEqual(@as(usize, 0), state.slot.?.entries.len);
    state = try step(a, state, .{ .slot = .{ .acquire = .{ .actor = @splat(3), .expected = 1, .now = 150, .expires = 200 } } });
    try std.testing.expectError(error.ChangefeedConflict, step(a, state, .{ .slot = .{ .confirm = .{ .session = session, .now = 151, .flushed = lsn + 100 } } }));
}
test "changefeed logical slot skips a complete invisible projection without WAL positions" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var state = try testSlotStateAt(a, 8192);
    const session = state.slot.?.session.?;
    state = try step(a, state, .{ .slot = .{ .bind = .{ .session = session, .now = 100, .binding = .{ .publications = &.{"rows_pub"}, .projection = @splat(8) } } } });
    var delivery = testDelivery(state, &.{.{ .namespace = testNamespace(7), .position = .{ .sequence = 11 } }});
    delivery.issued_at = 101;
    delivery.cursor = delivery.identity(state.id, state.epoch);
    const skip: Mutation = .{ .skip_slot_projection = .{ .session = session, .now = 101, .delivery = delivery } };
    const skipped = try step(a, state, skip);
    try std.testing.expectEqual(@as(u64, 8192), skipped.slot.?.confirmed_lsn);
    try std.testing.expectEqual(@as(u64, 8192), skipped.slot.?.next_lsn);
    try std.testing.expectEqual(delivery.cursor, skipped.slot.?.mapped_cursor);
    try std.testing.expectEqual(@as(u64, 11), skipped.roots[0].acknowledged.sequence);
    try std.testing.expectEqual(@as(usize, 0), skipped.cursors.len);
    try std.testing.expectError(error.ChangefeedConflict, step(a, skipped, skip));
}

test "changefeed logical slot drop waits for the lease and preserves cleanup ownership" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const state = try testSlotStateAt(a, 8192);
    var mutation: Mutation = .{ .slot = .{ .drop = .{ .database = @splat(1), .name = "slot", .now = 149 } } };
    try std.testing.expectError(error.ChangefeedConflict, step(a, state, mutation));
    mutation.slot.drop.now = 150;
    mutation.slot.drop.database[0] ^= 1;
    try std.testing.expectError(error.ChangefeedConflict, step(a, state, mutation));
    mutation.slot.drop.database[0] ^= 1;
    const closing = try step(a, state, mutation);
    try std.testing.expectEqual(Phase.closing, closing.phase);
    try std.testing.expect(closing.slot.?.spool_retired);
    try std.testing.expectEqual(@as(u64, 8192), closing.slot.?.consistent_lsn);
    try std.testing.expect(!closing.roots[0].released);
    try std.testing.expectEqualDeep(closing, try step(a, closing, mutation));
    try std.testing.expectError(error.ChangefeedConflict, step(a, closing, .finish_close));
    try std.testing.expectError(error.ChangefeedResynchronizationRequired, step(a, closing, .{ .slot = .{ .acquire = .{ .actor = @splat(3), .expected = 1, .now = 150, .expires = 200 } } }));
}

test "changefeed logical slot rejects partial frame endpoints and document table compatibility" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var state = try testSlotState(a);
    const initial = state.latest_cursor;
    const delivery = testDelivery(state, &.{.{ .namespace = testNamespace(7), .position = .{ .sequence = 10, .event = 1 } }});
    state = try step(a, state, .{ .issue = delivery });
    const transaction: pg_slot.Prepared = .{ .before = initial, .after = delivery.cursor, .content = @splat(3), .completion = @splat(4), .bytes = 100, .commit_offset = 74, .xid = 42, .commit_time = -1 };
    try std.testing.expectError(error.ChangefeedConflict, step(a, state, .{ .slot = .{ .issue = .{ .session = state.slot.?.session.?, .now = 101, .transaction = transaction } } }));
    var document = try testState(a, .now);
    document.id = pg_slot.feedId(@splat(1), "slot");
    var definition = document.definition.?;
    definition.projection = .rows;
    const tables = try a.dupe(Table, definition.tables);
    tables[0].contract = .native_document;
    definition.tables = tables;
    document.definition = definition;
    try std.testing.expectError(error.ChangefeedCapabilityUnsupported, step(a, document, .{ .slot = .{ .configure = .{ .database = @splat(1), .name = "slot" } } }));
}

test "changefeed closed state rejects even an already acknowledged native cursor" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var state = try testState(a, .now);
    const cursor = state.acknowledged_cursor;
    state = try step(a, state, .close);
    try std.testing.expectError(error.ChangefeedResynchronizationRequired, step(a, state, .{ .acknowledge = cursor }));
}

test "changefeed slot atomically publishes reserved spool and retains cleanup debt across close and reopen" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var state = try testSlotState(a);
    const initial = state.latest_cursor;
    const session = state.slot.?.session.?;
    const delivery = testDelivery(state, &.{.{ .namespace = testNamespace(7), .position = .{ .sequence = 11 } }});
    state = try step(a, state, .{ .issue = delivery });
    var scope: pg_slot.spool_control.Scope = .{ .namespace = @splat(9), .domain = pg_slot.spool_control.Scope.domainFor(state.id, state.epoch), .attempt = @splat(7) };
    std.mem.writeInt(u64, scope.attempt[0..8], 1, .big);
    const spec: pg_slot.spool_control.Spec = .{ .feed = state.id, .epoch = state.epoch, .definition = state.definition.?.identity(), .projection = @splat(8), .storage = scope.storageIdentity(), .before = initial, .after = delivery.cursor, .completion = @splat(4), .start_lsn = 1, .bytes = 100, .commit_offset = 74, .xid = 42, .commit_time = -1 };
    state = try step(a, state, .{ .slot = .{ .reserve_spool = .{ .session = session, .now = 101, .id = 1, .scope = scope, .spec = spec } } });
    const charge = state.slot.?.spool.charged_bytes;
    try std.testing.expectEqual(@as(u64, 1), state.slot.?.next_lsn);
    var parsed = try parse(std.testing.allocator, try encode(a, state));
    defer parsed.deinit();
    try std.testing.expectEqual(charge, parsed.value.slot.?.spool.charged_bytes);
    state = try step(a, parsed.value, .{ .slot = .{ .publish_spool = .{ .session = session, .now = 102, .id = 1, .manifest = @splat(6), .content = @splat(7) } } });
    try std.testing.expectEqual(@as(u64, 101), state.slot.?.next_lsn);
    try std.testing.expectEqual(@as(u64, 0), state.acknowledged_delivery);
    state = try step(a, state, .{ .slot = .{ .confirm = .{ .session = session, .now = 103, .flushed = 101 } } });
    try std.testing.expectEqual(@as(u64, 1), state.acknowledged_delivery);
    try std.testing.expectEqual(charge, state.slot.?.spool.charged_bytes);
    state = try step(a, state, .close);
    try std.testing.expect(state.slot.?.session == null);
    state = try step(a, state, .{ .release_root = .{ .namespace = testNamespace(7), .ownership_epoch = 1, .proof = @splat(5) } });
    try std.testing.expectError(error.ChangefeedConflict, step(a, state, .finish_close));
    state = try step(a, state, .{ .slot = .{ .checkpoint_spool_cleanup = .{ .id = 1, .storage = spec.storage, .expected = 0, .progress = .{ .revision = 1, .fenced = true, .complete = true, .proof = @splat(5) } } } });
    state = try step(a, state, .{ .slot = .{ .reclaim_spool = .{ .id = 1, .storage = spec.storage, .proof = @splat(5) } } });
    state = try step(a, state, .finish_close);
    var closed = try parse(std.testing.allocator, try encode(a, state));
    defer closed.deinit();
    try std.testing.expectEqual(.deleted, closed.value.phase);
    try std.testing.expectEqual(@as(u64, 1), closed.value.slot.?.spool.collected_through);
    try std.testing.expectEqual(@as(u64, 0), closed.value.slot.?.spool.charged_bytes);
}

test "changefeed root control journal retains exact accepted request across close and serialization" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const ns = testNamespace(7);
    var state = try step(scratch, .{}, .{ .create = .{ .id = @splat(4), .definition = testDefinition(.now), .now = 100, .expires_at = 1000 } });
    state = try step(scratch, state, .{ .plan_roots = &.{.{ .namespace = ns, .ownership_epoch = 2 }} });
    const grant = state.roots[0].grant_sequence;
    const payload: root_intent.Payload = .{ .command = "{\"version\":1,\"operation\":\"submit\"}", .fence = "{\"group\":7}", .table = "docs" };
    const prepare: Mutation = .{ .root_control = .{ .namespace = ns, .operation = .{ .prepare = .{ .ownership_epoch = 2, .grant_sequence = grant, .payload = payload } } } };
    state = try step(scratch, state, prepare);
    const first = state.roots[0].pending_control.?.sequence;
    try std.testing.expectError(error.ChangefeedConflict, step(scratch, state, prepare));
    state = try step(scratch, state, .{ .root_control = .{ .namespace = ns, .operation = .{ .cancel_prepared = first } } });
    try std.testing.expect(state.roots[0].pending_control == null);
    state = try step(scratch, state, prepare);
    const sequence = state.roots[0].pending_control.?.sequence;
    try std.testing.expect(sequence > first);
    const begin: Mutation = .{ .root_control = .{ .namespace = ns, .operation = .{ .begin = .{ .sequence = sequence, .actor = @splat(3) } } } };
    state = try step(scratch, state, begin);
    try std.testing.expectError(error.ChangefeedConflict, step(scratch, state, begin));
    try std.testing.expectError(error.ChangefeedConflict, step(scratch, state, .{ .root_control = .{ .namespace = ns, .operation = .{ .cancel_prepared = sequence } } }));
    try std.testing.expectError(error.ChangefeedConflict, step(scratch, state, .{ .rebind = .{ .namespace = ns, .previous = 2, .next = 3, .proof = @splat(1) } }));
    try std.testing.expectError(error.ChangefeedConflict, step(scratch, state, .{ .refresh_grant = .{ .namespace = ns, .previous = grant } }));
    const accepted: Mutation = .{ .root_control = .{ .namespace = ns, .operation = .{ .accepted = .{ .sequence = sequence, .actor = @splat(3), .receipt = .{ .term = 8, .index = 500 } } } } };
    const encoded_write = try encodeWrite(a, .{ .id = state.id, .authority = state.authority, .owner = state.definition.?.owner, .authorization = state.definition.?.authorization, .expected_revision = state.revision, .expected_epoch = state.epoch, .mutation = accepted });
    defer a.free(encoded_write);
    var write = try parseWrite(a, encoded_write);
    defer write.deinit();
    state = try write.value.apply(scratch, state, state.authority_sequence + 1);
    const raw = try encode(a, state);
    defer a.free(raw);
    var reopened = try parse(a, raw);
    defer reopened.deinit();
    const pending = reopened.value.roots[0].pending_control.?;
    try std.testing.expectEqualDeep(reopened.value, try step(scratch, reopened.value, accepted));
    try std.testing.expectEqualDeep(state.roots[0].pending_control.?, pending);
    try std.testing.expectEqualStrings(payload.command, pending.payload.command);
    try std.testing.expectEqualStrings(payload.fence, pending.payload.fence);
    try std.testing.expectEqual(@as(u64, 500), pending.receipt.?.index);
    var stale = accepted;
    stale.root_control.operation.accepted.sequence = first;
    try std.testing.expectError(error.ChangefeedConflict, step(scratch, reopened.value, stale));
    stale = accepted;
    stale.root_control.operation.accepted.receipt.index = 501;
    try std.testing.expectError(error.ChangefeedConflict, step(scratch, reopened.value, stale));
    state = try step(scratch, reopened.value, .close);
    try std.testing.expectEqualDeep(pending, state.roots[0].pending_control.?);
    try std.testing.expectError(error.ChangefeedConflict, step(scratch, state, .{ .release_root = .{ .namespace = ns, .ownership_epoch = 2, .proof = @splat(1) } }));
    try std.testing.expectError(error.ChangefeedConflict, step(scratch, state, .finish_close));
}

fn testPendingControl(a: A, before: State) !State {
    const root = before.roots[0];
    var state = try step(a, before, .{ .root_control = .{ .namespace = root.namespace, .operation = .{ .prepare = .{ .ownership_epoch = root.ownership_epoch, .grant_sequence = root.grant_sequence, .payload = .{ .command = "{}", .fence = "{}", .table = "docs" } } } } });
    const sequence = state.roots[0].pending_control.?.sequence;
    state = try step(a, state, .{ .root_control = .{ .namespace = root.namespace, .operation = .{ .begin = .{ .sequence = sequence, .actor = @splat(3) } } } });
    return step(a, state, .{ .root_control = .{ .namespace = root.namespace, .operation = .{ .accepted = .{ .sequence = sequence, .actor = @splat(3), .receipt = .{ .term = 4, .index = sequence + 100 } } } } });
}
fn testSettlement(state: State, effect: root_intent.Effect) Mutation {
    const pending = state.roots[0].pending_control.?;
    return .{ .root_control = .{ .namespace = state.roots[0].namespace, .operation = .{ .settled = .{ .sequence = pending.sequence, .actor = pending.actor, .receipt = pending.receipt.?, .request_digest = pending.digest, .proof = @splat(8), .effect = effect } } } };
}
test "changefeed accepted control settlement atomically folds authorized effects and retains lost reply proof" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const ns = testNamespace(7);
    var state = try step(a, .{}, .{ .create = .{ .id = @splat(4), .definition = testDefinition(.now), .now = 100, .expires_at = 1000 } });
    state = try step(a, state, .{ .plan_roots = &.{.{ .namespace = ns, .ownership_epoch = 1 }} });
    state = try testPendingControl(a, state);
    const enroll = testSettlement(state, .{ .enrolled = .{ .consumer_epoch = 7, .start = 42 } });
    var forged = enroll;
    forged.root_control.operation.settled.request_digest[0] ^= 1;
    try std.testing.expectError(error.ChangefeedConflict, step(a, state, forged));
    forged = enroll;
    forged.root_control.operation.settled.receipt.index += 1;
    try std.testing.expectError(error.ChangefeedConflict, step(a, state, forged));
    var ambiguous = state;
    const roots = try a.dupe(Root, state.roots);
    roots[0].pending_control.?.stage = .dispatching;
    roots[0].pending_control.?.receipt = null;
    ambiguous.roots = roots;
    try std.testing.expectError(error.ChangefeedConflict, step(a, ambiguous, enroll));
    state = try step(a, state, enroll);
    try std.testing.expect(state.roots[0].pending_control == null);
    try std.testing.expectEqual(@as(u64, 42), state.roots[0].reclaimed_through);
    try std.testing.expectEqualDeep(state, try step(a, state, enroll));
    const raw = try encode(a, state);
    var parsed = try parse(std.testing.allocator, raw);
    defer parsed.deinit();
    try std.testing.expectEqualDeep(state.roots[0].last_control, parsed.value.roots[0].last_control);
    state = try step(a, state, .{ .seal = .{ .cut = @splat(1) } });
    const delivery = testDelivery(state, &.{.{ .namespace = ns, .position = .{ .sequence = 50 } }});
    state = try step(a, state, .{ .issue = delivery });
    state = try step(a, state, .{ .acknowledge = delivery.cursor });
    state = try testPendingControl(a, state);
    var ack = testSettlement(state, .{ .acknowledged = .{ .consumer_epoch = 7, .previous = 42, .next = 51 } });
    try std.testing.expectError(error.ChangefeedConflict, step(a, state, ack));
    ack.root_control.operation.settled.effect.acknowledged.next = 50;
    state = try step(a, state, ack);
    try std.testing.expectEqual(@as(u64, 50), state.roots[0].reclaimed_through);
    state = try step(a, state, .{ .refresh_grant = .{ .namespace = ns, .previous = state.roots[0].grant_sequence } });
    state = try testPendingControl(a, state);
    const refresh = testSettlement(state, .{ .enrolled = .{ .consumer_epoch = 7, .start = 42 } });
    var moved_start = refresh;
    moved_start.root_control.operation.settled.effect.enrolled.start = 43;
    try std.testing.expectError(error.ChangefeedConflict, step(a, state, moved_start));
    state = try step(a, state, refresh);
    try std.testing.expectEqual(@as(u64, 50), state.roots[0].reclaimed_through);
    try std.testing.expectEqual(@as(u64, 50), state.roots[0].acknowledged.sequence);
    state = try testPendingControl(a, state);
    const retire_early = testSettlement(state, .{ .retired = .{ .sequence = state.roots[0].grant_sequence } });
    try std.testing.expectError(error.ChangefeedConflict, step(a, state, retire_early));
    const rejected = testSettlement(state, .{ .rejected = 5 });
    state = try step(a, state, rejected);
    try std.testing.expectEqual(@as(u64, 50), state.roots[0].reclaimed_through);
    try std.testing.expect(!state.roots[0].released);
    state = try step(a, state, .close);
    const close_sequence = state.authority_sequence;
    state = try testPendingControl(a, state);
    const retired = testSettlement(state, .{ .retired = .{ .sequence = close_sequence } });
    state = try step(a, state, retired);
    try std.testing.expect(state.roots[0].released);
    try std.testing.expectEqualDeep(state, try step(a, state, retired));
    state = try step(a, state, .finish_close);
    try std.testing.expectEqual(Phase.deleted, state.phase);
    try std.testing.expectEqualDeep(state, try step(a, state, retired));
    var changed_receipt = retired;
    changed_receipt.root_control.operation.settled.receipt.index += 1;
    try std.testing.expectError(error.ChangefeedConflict, step(a, state, changed_receipt));
}

test "changefeed retirement fence displaces only irrevocable cleanup and retains bounded original identity" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const ns = testNamespace(7);
    var state = try step(a, .{}, .{ .create = .{ .id = @splat(4), .definition = testDefinition(.now), .now = 100, .expires_at = 1000 } });
    state = try step(a, state, .{ .plan_roots = &.{.{ .namespace = ns, .ownership_epoch = 1 }} });
    state = try testPendingControl(a, state);
    const original = state.roots[0].pending_control.?;
    var operation: Mutation = .{ .root_control = .{ .namespace = ns, .operation = .{ .prepare_fence = .{ .previous_sequence = original.sequence, .retirement_sequence = state.authority_sequence, .payload = original.payload } } } };
    try std.testing.expectError(error.ChangefeedConflict, step(a, state, operation));
    state = try step(a, state, .close);
    operation.root_control.operation.prepare_fence.retirement_sequence = state.authority_sequence;
    state = try step(a, state, operation);
    const first = state.roots[0].pending_control.?;
    try std.testing.expect(first.retirement_fence);
    try std.testing.expectEqual(.prepared, first.stage);
    try std.testing.expectEqualDeep(root_intent.Displaced.from(original), first.displaced.?);
    try std.testing.expect(first.sequence > original.sequence);
    try std.testing.expectError(error.ChangefeedConflict, step(a, state, operation));
    state = try step(a, state, .{ .root_control = .{ .namespace = ns, .operation = .{ .begin = .{ .sequence = first.sequence, .actor = @splat(8) } } } });
    operation.root_control.operation.prepare_fence.previous_sequence = first.sequence;
    operation.root_control.operation.prepare_fence.retirement_sequence = state.authority_sequence;
    state = try step(a, state, operation);
    const second = state.roots[0].pending_control.?;
    try std.testing.expect(second.sequence > first.sequence);
    try std.testing.expectEqualDeep(first.displaced, second.displaced);
    const bytes = try encode(std.testing.allocator, state);
    defer std.testing.allocator.free(bytes);
    var restored = try parse(std.testing.allocator, bytes);
    defer restored.deinit();
    try std.testing.expectEqualDeep(second, restored.value.roots[0].pending_control.?);
    var forged = operation;
    forged.root_control.operation.prepare_fence.previous_sequence = second.sequence;
    forged.root_control.operation.prepare_fence.retirement_sequence = state.authority_sequence;
    forged.root_control.operation.prepare_fence.payload.table = "other";
    try std.testing.expectError(error.ChangefeedConflict, step(a, state, forged));
}

test "changefeed slot snapshot name binds pins and initial LSN across metadata reopen" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var definition = testDefinition(.snapshot_then_changes);
    definition.projection = .rows;
    const database: Digest = @splat(1);
    const id = pg_slot.feedId(database, "snapshot_slot");
    var state = try step(a, .{}, .{ .create = .{ .id = id, .definition = definition, .now = 100, .expires_at = 1000, .bootstrap_expires_at = 500 } });
    const namespace = testNamespace(7);
    state = try step(a, state, .{ .plan_roots = &.{.{ .namespace = namespace, .ownership_epoch = 1 }} });
    state = try step(a, state, .{ .enroll = .{ .namespace = namespace, .ownership_epoch = 1, .grant_sequence = state.roots[0].grant_sequence, .consumer_epoch = 6, .start = 10, .snapshot = .{ .id = state.consumerId(), .digest = @splat(4), .expires_at = 500 } } });
    const config: pg_slot.Configuration = .{ .database = database, .name = "snapshot_slot", .consistent_lsn = 1024, .binding = .{ .publications = &.{"docs_publication"}, .projection = @splat(5) } };
    const enrollment = state;
    state = try step(a, state, .{ .seal = .{ .cut = @splat(7), .slot = config } });
    const snapshot = try SlotSnapshot.capture(state, database, definition.owner, 101);
    const original: Write = .{ .id = id, .authority = state.authority, .owner = definition.owner, .authorization = definition.authorization, .expected_revision = enrollment.revision, .expected_epoch = enrollment.epoch, .mutation = .{ .seal = .{ .cut = state.cut, .slot = config } } };
    const recovered = try recoverSlotStart(state, original, 101);
    try std.testing.expectEqual(config.consistent_lsn, recovered.consistent_lsn);
    try std.testing.expectEqualDeep(snapshot, recovered.snapshot.?);

    const name = snapshot.name();
    var reopened = try parse(a, try encode(a, state));
    defer reopened.deinit();
    const imported = try SlotSnapshot.parse(&name);
    try imported.verify(reopened.value, database, definition.owner, 101);
    try std.testing.expectError(error.Forbidden, imported.verify(reopened.value, @splat(8), definition.owner, 101));
    try std.testing.expectError(error.Forbidden, imported.verify(reopened.value, database, @splat(8), 101));
    try std.testing.expectError(error.ChangefeedLeaseExpired, imported.verify(reopened.value, database, definition.owner, 500));
    var altered_config = config;
    altered_config.consistent_lsn += 1;
    const different_lsn = try step(a, enrollment, .{ .seal = .{ .cut = @splat(7), .slot = altered_config } });
    try std.testing.expectError(error.InvalidReplicationSnapshot, imported.verify(different_lsn, database, definition.owner, 101));
    const start: Mutation = .{ .start_exported_slot = .{ .name = imported, .now = 101, .requested_lsn = config.consistent_lsn } };
    try std.testing.expectEqual(slot_snapshot_start_protocol_version, start.requiredProtocol());
    var invalid_start = start;
    invalid_start.start_exported_slot.requested_lsn += 1;
    try std.testing.expectError(error.ChangefeedConflict, step(a, state, invalid_start));
    const streaming = try step(a, state, start);
    try std.testing.expectEqual(Phase.streaming, streaming.phase);
    try std.testing.expectEqualDeep(state.slot.?, streaming.slot.?);
    try std.testing.expectEqual(state.acknowledged_cursor, streaming.acknowledged_cursor);
    try std.testing.expect(streaming.roots[0].snapshot_acknowledged.done);
    try std.testing.expect(streaming.needsMaintenance(101));
    try std.testing.expectError(error.ChangefeedResynchronizationRequired, imported.verify(streaming, database, definition.owner, 101));
    const issued = testDelivery(state, &.{.{ .namespace = namespace, .position = .{ .sequence = 10 }, .snapshot = .{ .page = 1, .done = true } }});
    state = try step(a, state, .{ .issue = issued });
    try std.testing.expectError(error.ChangefeedResynchronizationRequired, imported.verify(state, database, definition.owner, 101));
}

test "changefeed control seals the complete catalog root vector atomically" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var definition = testDefinition(.now);
    definition.tables = &.{
        .{ .table_id = 7, .binding = @splat(3), .policy_revision = 1, .contract = .writable_iceberg_rows },
        .{ .table_id = 9, .binding = @splat(4), .policy_revision = 1, .contract = .writable_iceberg_rows },
    };
    const first = catalogRootNamespace(definition.tables[0]);
    const second = catalogRootNamespace(definition.tables[1]);
    var state = try step(a, .{}, .{ .create = .{ .id = @splat(4), .definition = definition, .now = 100, .expires_at = 1000 } });
    state = try step(a, state, .{ .plan_roots = &.{ .{ .namespace = first, .ownership_epoch = 1 }, .{ .namespace = second, .ownership_epoch = 1 } } });
    state = try step(a, state, .{ .stage_catalog_start = "YQ" });
    try std.testing.expectError(error.ChangefeedConflict, step(a, state, .{ .seal = .{ .cut = @splat(7), .provider_context = "YQ", .catalog_starts = &.{.{ .namespace = first, .through = 3 }} } }));
    const sealed = try step(a, state, .{ .seal = .{ .cut = @splat(7), .provider_context = "YQ", .catalog_starts = &.{ .{ .namespace = first, .through = 3 }, .{ .namespace = second, .through = 10 } } } });
    try std.testing.expectEqual(.streaming, sealed.phase);
    try std.testing.expectEqual(@as(u64, 3), sealed.roots[0].acknowledged.sequence);
    try std.testing.expectEqual(@as(u64, 10), sealed.roots[1].acknowledged.sequence);
    try std.testing.expectEqualStrings("YQ", try sealed.contextAt(sealed.acknowledged_cursor));
    try std.testing.expect(!sealed.needsMaintenance(200));
    const receipt = testDelivery(sealed, &.{.{ .namespace = second, .position = .{ .sequence = 11 } }});
    state = try step(a, sealed, .{ .issue = receipt });
    try std.testing.expect(!state.needsMaintenance(200));
    state = try step(a, state, .{ .acknowledge = receipt.cursor });
    try std.testing.expect(state.needsMaintenance(200));
    try std.testing.expectEqual(definition.tables[1].table_id, state.pendingCatalogAcknowledgment().?.table_id);
    state = try step(a, state, .{ .reclaimed = .{
        .namespace = second,
        .consumer_epoch = state.roots[1].consumer_epoch,
        .previous = 10,
        .next = 11,
    } });
    try std.testing.expect(!state.needsMaintenance(200));
}

test "changefeed control mixed renewal leaves native root work durable" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var definition = testDefinition(.now);
    definition.tables = &.{
        .{ .table_id = 7, .binding = @splat(3), .policy_revision = 1, .contract = .native_relational },
        .{ .table_id = 9, .binding = @splat(4), .policy_revision = 1, .contract = .writable_iceberg_rows },
    };
    const native = testNamespace(7);
    const iceberg = catalogRootNamespace(definition.tables[1]);
    var state = try step(a, .{}, .{ .create = .{ .id = @splat(4), .definition = definition, .now = 100, .expires_at = 1000 } });
    state = try step(a, state, .{ .plan_roots = &.{ .{ .namespace = native, .ownership_epoch = 1 }, .{ .namespace = iceberg, .ownership_epoch = 1 } } });
    state = try step(a, state, .{ .enroll = .{ .namespace = native, .ownership_epoch = 1, .grant_sequence = state.roots[0].grant_sequence, .consumer_epoch = 3, .start = 10 } });
    state = try step(a, state, .{ .stage_catalog_start = "YQ" });
    state = try step(a, state, .{ .seal = .{ .cut = @splat(7), .provider_context = "YQ", .catalog_starts = &.{.{ .namespace = iceberg, .through = 3 }} } });
    state = try step(a, state, .{ .renew_catalog = .{ .now = 200, .expires_at = 2000 } });
    try std.testing.expect(state.renewal_pending);
    try std.testing.expectEqual(@as(u64, 0), state.roots[0].renewed_through);
    try std.testing.expectEqual(@as(u64, 2000), state.roots[1].renewed_through);
}

test "changefeed control binds artifact offsets to the enrolled physical root generations" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var definition = testDefinition(.now);
    definition.events = artifact_event;
    const create: Mutation = .{ .create = .{ .id = @splat(4), .definition = definition, .now = 100, .expires_at = 1000 } };
    try std.testing.expectEqual(provider_history_protocol_version, create.requiredProtocol());
    const first = testNamespace(definition.tables[0].table_id);
    var second = first;
    second[second.len - 1] += 1;
    const generations = "AAAAAAAAAAAAAAAAAAAAAQ";
    var state = try step(a, .{}, create);
    state = try step(a, state, .{ .plan_roots = &.{ .{ .namespace = first, .ownership_epoch = 1 }, .{ .namespace = second, .ownership_epoch = 1 } } });
    try std.testing.expectError(error.ChangefeedConflict, step(a, state, .{ .stage_catalog_start = "YQ" }));
    state = try step(a, state, .{ .stage_catalog_start = generations });
    try std.testing.expectError(error.ChangefeedConflict, step(a, state, .{ .seal = .{ .cut = @splat(7), .provider_context = generations, .catalog_starts = &.{.{ .namespace = first, .through = 3 }} } }));
    state = try step(a, state, .{ .seal = .{ .cut = @splat(7), .provider_context = generations, .catalog_starts = &.{ .{ .namespace = first, .through = 3 }, .{ .namespace = second, .through = 10 } } } });
    try std.testing.expectEqualStrings(generations, try state.contextAt(state.acknowledged_cursor));
    try std.testing.expectEqual(@as(u64, 3), state.roots[0].acknowledged.sequence);
    try std.testing.expectEqual(@as(u64, 10), state.roots[1].acknowledged.sequence);
    state = try step(a, state, .{ .renew_catalog = .{ .now = 200, .expires_at = 2000 } });
    try std.testing.expect(!state.renewal_pending);
}

test "an enrollment plan preserves distinct cursors for a shared storage namespace" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const parent = testNamespace(7);
    const physical: PhysicalRoot = .{ .group_id = 22, .range_id = 22 };
    const child = splitStreamNamespace(parent, physical);
    const table: Table = .{ .table_id = 7, .binding = @splat(3), .policy_revision = 1, .contract = .native_document };
    const definition: Definition = .{ .owner = @splat(1), .authorization = @splat(2), .tables = &.{table} };
    const id: Id = @splat(4);
    const incarnation: Digest = @splat(5);
    const state = try (Mutation{ .create = .{ .id = id, .definition = definition, .now = 1, .expires_at = 60 } }).apply(a, .{}, .{ .incarnation = incarnation, .sequence = 1 });
    var plans = [_]RootPlan{
        .{ .namespace = parent, .physical = .{ .group_id = 11, .range_id = 11 }, .ownership_epoch = 1 },
        .{ .namespace = child, .physical = physical, .ownership_epoch = 1, .storage_namespace = parent },
    };
    std.mem.sort(RootPlan, &plans, {}, struct {
        fn less(_: void, left: RootPlan, right: RootPlan) bool {
            return std.mem.order(u8, &left.namespace, &right.namespace) == .lt;
        }
    }.less);
    const bytes = try encodeWrite(a, .{
        .id = id,
        .authority = incarnation,
        .owner = definition.owner,
        .authorization = definition.authorization,
        .expected_revision = state.revision,
        .expected_epoch = state.epoch,
        .mutation = .{ .plan_roots = &plans },
    });
    try std.testing.expectEqual(shared_namespace_plan_protocol_version, requiredProtocolEncoded(bytes));
    var write = try parseWrite(a, bytes);
    defer write.deinit();
    const planned = try write.value.apply(a, state, 2);
    var recovered = try parse(a, try encode(a, planned));
    defer recovered.deinit();
    try std.testing.expectEqual(parent, recovered.value.roots[try recovered.value.rootIndex(parent)].storageNamespace());
    const successor = recovered.value.roots[try recovered.value.rootIndex(child)];
    try std.testing.expectEqual(parent, successor.storageNamespace());
    try std.testing.expectEqual(physical, successor.physical.?);
}
