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
pub const protocol_version: u16 = 91;
pub const max_tables = 64;
pub const max_roots = 4096;
pub const max_cursors = 128;
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
pub const Start = enum { now, snapshot_then_changes };
pub const Projection = enum { keys_and_versions, rows };
pub const Contract = enum { native_document, native_relational, writable_iceberg_rows, external_snapshot_lifecycle };
pub const Predicate = struct { field: []const u8, value: std.json.Value };
pub const Table = struct {
    table_id: u64,
    binding: Digest,
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
pub const Root = struct {
    namespace: Namespace,
    ownership_epoch: u64,
    grant_sequence: u64,
    consumer_epoch: u64 = 0,
    start: u64 = 0,
    delivered: Position = .{},
    acknowledged: Position = .{},
    reclaimed_through: u64 = 0,
    snapshot: ?Snapshot = null,
    snapshot_delivered: BootstrapPosition = .{},
    snapshot_acknowledged: BootstrapPosition = .{},
    snapshot_released: bool = false,
    released: bool = false,
};
pub const RootPlan = struct { namespace: Namespace, ownership_epoch: u64 };
pub const Advance = struct { namespace: Namespace, position: Position, snapshot: ?BootstrapPosition = null };
pub const Delivery = struct {
    sequence: u64,
    before: Digest,
    cursor: Digest,
    authority: Digest,
    issued_at: u64,
    advances: []const Advance,
    pub fn identity(self: Delivery, id: Id, epoch: u64) Digest {
        return hash(.{ .domain = "antfly:changefeed-cursor:v1", .id = id, .epoch = epoch, .sequence = self.sequence, .before = self.before, .authority = self.authority, .issued_at = self.issued_at, .advances = self.advances });
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
    cut: Digest = @splat(0),
    roots: []const Root = &.{},
    latest_delivery: u64 = 0,
    acknowledged_delivery: u64 = 0,
    latest_cursor: Digest = @splat(0),
    acknowledged_cursor: Digest = @splat(0),
    cursors: []const Delivery = &.{},
    reset_reason: Reset = .none,
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
    pub fn validate(self: State) !void {
        if (self.version != 1 or self.roots.len > max_roots or self.cursors.len > max_cursors) return error.InvalidChangefeed;
        if (self.phase == .empty) {
            if (self.revision != 0 or self.epoch != 0 or nz(self.id) or nz(self.authority) or self.authority_sequence != 0 or self.definition != null or
                self.created_at != 0 or self.expires_at != 0 or self.bootstrap_expires_at != 0 or nz(self.cut) or self.roots.len != 0 or self.cursors.len != 0 or
                self.latest_delivery != 0 or self.acknowledged_delivery != 0 or nz(self.latest_cursor) or nz(self.acknowledged_cursor) or self.reset_reason != .none) return error.InvalidChangefeed;
            return;
        }
        const definition = self.definition orelse return error.InvalidChangefeed;
        try definition.validate();
        if (self.revision == 0 or !nz(self.id) or self.epoch == 0 or !nz(self.authority) or self.authority_sequence == 0 or
            self.created_at == 0 or self.expires_at <= self.created_at or self.acknowledged_delivery > self.latest_delivery or
            self.cursors.len != self.latest_delivery - self.acknowledged_delivery or self.cursors.len > definition.cursor_window or encodedSize(self.cursors) > definition.cursor_byte_limit) return error.InvalidChangefeed;
        if ((definition.start == .snapshot_then_changes) != (self.bootstrap_expires_at != 0) or
            (self.bootstrap_expires_at != 0 and (self.bootstrap_expires_at <= self.created_at or self.bootstrap_expires_at > self.expires_at))) return error.InvalidChangefeed;
        if ((self.phase == .reset_required) != (self.reset_reason != .none)) return error.InvalidChangefeed;
        for (self.roots, 0..) |root, index| {
            if (!nz(root.namespace) or root.ownership_epoch == 0 or root.grant_sequence == 0 or root.grant_sequence > self.authority_sequence or
                !definition.contains(std.mem.readInt(u64, root.namespace[0..8], .big)) or
                (index != 0 and std.mem.order(u8, &self.roots[index - 1].namespace, &root.namespace) != .lt)) return error.InvalidChangefeed;
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
            for (self.roots) |root| if (root.consumer_epoch == 0 or root.released or (definition.start == .snapshot_then_changes and root.snapshot == null)) return error.InvalidChangefeed;
        }
        var previous = self.acknowledged_cursor;
        for (self.cursors, 0..) |cursor, i| {
            if (cursor.sequence != self.acknowledged_delivery + i + 1 or !equal(cursor.before, previous) or !nz(cursor.authority) or cursor.issued_at == 0 or
                !equal(cursor.cursor, cursor.identity(self.id, self.epoch))) return error.InvalidChangefeed;
            try validateAdvances(self, cursor.advances);
            previous = cursor.cursor;
        }
        if (self.cursors.len != 0 and !equal(previous, self.latest_cursor)) return error.InvalidChangefeed;
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
pub const Mutation = union(enum) {
    create: struct { id: Id, definition: Definition, now: u64, expires_at: u64, bootstrap_expires_at: u64 = 0 },
    plan_roots: []const RootPlan,
    enroll: struct { namespace: Namespace, ownership_epoch: u64, grant_sequence: u64, consumer_epoch: u64, start: u64, snapshot: ?Snapshot = null },
    rebind: struct { namespace: Namespace, previous: u64, next: u64, proof: Digest },
    refresh_grant: struct { namespace: Namespace, previous: u64 },
    seal: struct { cut: Digest },
    issue: Delivery,
    acknowledge: Digest,
    reclaimed: struct { namespace: Namespace, consumer_epoch: u64, previous: u64, next: u64 },
    finish_bootstrap: struct { proof: Digest },
    release_snapshot: struct { namespace: Namespace, snapshot_id: Id, proof: Digest },
    renew: struct { now: u64, expires_at: u64 },
    expire: u64,
    reset: Reset,
    close,
    release_root: struct { namespace: Namespace, ownership_epoch: u64, proof: Digest },
    finish_close,
    restart: struct { definition: Definition, now: u64, expires_at: u64, bootstrap_expires_at: u64 = 0 },
    pub fn validate(self: Mutation) !void {
        switch (self) {
            .create => |value| {
                if (!nz(value.id)) return error.InvalidChangefeed;
                try value.definition.validate();
                try lease(value.now, value.expires_at);
                try bootstrapLease(value.definition.start, value.now, value.expires_at, value.bootstrap_expires_at);
            },
            .plan_roots => |plans| {
                if (plans.len == 0 or plans.len > max_roots) return error.InvalidChangefeed;
                for (plans, 0..) |plan, index| if (!nz(plan.namespace) or plan.ownership_epoch == 0 or (index != 0 and std.mem.order(u8, &plans[index - 1].namespace, &plan.namespace) != .lt)) return error.InvalidChangefeed;
            },
            .enroll => |value| {
                if (!nz(value.namespace) or value.ownership_epoch == 0 or value.grant_sequence == 0 or value.consumer_epoch == 0) return error.InvalidChangefeed;
                if (value.snapshot) |pin| if (!nz(pin.id) or !nz(pin.digest) or pin.expires_at == 0) return error.InvalidChangefeed;
            },
            .refresh_grant => |value| if (!nz(value.namespace) or value.previous == 0) return error.InvalidChangefeed,
            .rebind => |value| if (!nz(value.namespace) or value.previous == 0 or value.next <= value.previous or !nz(value.proof)) return error.InvalidChangefeed,
            .seal => |value| if (!nz(value.cut)) return error.InvalidChangefeed,
            .issue => |value| {
                if (value.sequence == 0 or !nz(value.before) or !nz(value.cursor) or !nz(value.authority) or value.issued_at == 0 or value.advances.len > max_advances) return error.InvalidChangefeed;
                for (value.advances, 0..) |advance, index| {
                    if (!nz(advance.namespace) or advance.position.event >= 65536 or (index != 0 and std.mem.order(u8, &value.advances[index - 1].namespace, &advance.namespace) != .lt)) return error.InvalidChangefeed;
                    if (advance.snapshot) |position| if (position.page == 0 or !text(position.continuation, 4096, true)) return error.InvalidChangefeed;
                }
            },
            .acknowledge => |value| if (!nz(value)) return error.InvalidChangefeed,
            .reclaimed => |value| if (!nz(value.namespace) or value.consumer_epoch == 0 or value.next < value.previous) return error.InvalidChangefeed,
            .finish_bootstrap => |value| if (!nz(value.proof)) return error.InvalidChangefeed,
            .release_snapshot => |value| if (!nz(value.namespace) or !nz(value.snapshot_id) or !nz(value.proof)) return error.InvalidChangefeed,
            .renew => |value| try lease(value.now, value.expires_at),
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
        switch (self) {
            .create => |create| {
                if (state.phase != .empty or !nz(create.id)) return error.ChangefeedConflict;
                try create.definition.validate();
                try lease(create.now, create.expires_at);
                try bootstrapLease(create.definition.start, create.now, create.expires_at, create.bootstrap_expires_at);
                next = .{ .id = create.id, .epoch = 1, .authority = stamp.incarnation, .definition = create.definition, .phase = .enrolling, .created_at = create.now, .expires_at = create.expires_at, .bootstrap_expires_at = create.bootstrap_expires_at };
            },
            .plan_roots => |plans| {
                if (state.phase != .enrolling or state.roots.len != 0 or plans.len == 0 or plans.len > max_roots) return error.ChangefeedConflict;
                const roots = try a.alloc(Root, plans.len);
                for (plans, roots) |plan, *root| root.* = .{ .namespace = plan.namespace, .ownership_epoch = plan.ownership_epoch, .grant_sequence = stamp.sequence };
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
            .rebind => |binding| {
                if (state.phase == .empty or state.phase == .deleted) return error.ChangefeedConflict;
                const index = try state.rootIndex(binding.namespace);
                if (state.roots[index].ownership_epoch == binding.next) return state;
                if (state.roots[index].ownership_epoch != binding.previous) return error.ChangefeedConflict;
                const roots = try a.dupe(Root, state.roots);
                roots[index].ownership_epoch = binding.next;
                roots[index].grant_sequence = stamp.sequence;
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
            .seal => |sealed| {
                if (state.phase != .enrolling or state.roots.len == 0 or !nz(sealed.cut)) return error.ChangefeedConflict;
                for (state.roots) |root| if (root.consumer_epoch == 0) return error.ChangefeedConflict;
                for (state.definition.?.tables) |table| {
                    var found = false;
                    for (state.roots) |root| if (std.mem.readInt(u64, root.namespace[0..8], .big) == table.table_id) {
                        found = true;
                        break;
                    };
                    if (!found) return error.ChangefeedConflict;
                }
                next.cut = sealed.cut;
                next.phase = if (state.definition.?.start == .snapshot_then_changes) .bootstrap else .streaming;
                next.latest_cursor = hash(.{ .domain = "antfly:changefeed-cut:v1", .id = state.id, .epoch = state.epoch, .authority = state.authority, .definition = state.definition.?.identity(), .cut = sealed.cut });
                next.acknowledged_cursor = next.latest_cursor;
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
            },
            .acknowledge => |cursor| {
                if (state.phase != .streaming and state.phase != .bootstrap) return error.ChangefeedResynchronizationRequired;
                if (equal(cursor, state.acknowledged_cursor)) return state;
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
                next.cursors = try a.dupe(Delivery, state.cursors[selected + 1 ..]);
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
            .finish_bootstrap => |completion| {
                if (state.phase != .bootstrap or !nz(completion.proof) or state.latest_delivery != state.acknowledged_delivery) return error.ChangefeedConflict;
                for (state.roots) |root| if (!root.snapshot_acknowledged.done) return error.ChangefeedConflict;
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
            .finish_close => {
                if (state.phase != .closing) return error.ChangefeedConflict;
                for (state.roots) |root| if (!root.released or (root.snapshot != null and !root.snapshot_released)) return error.ChangefeedConflict;
                next.phase = .deleted;
            },
            .restart => |restart| {
                if (state.phase != .reset_required) return error.ChangefeedConflict;
                for (state.roots) |root| if (!root.released or (root.snapshot != null and !root.snapshot_released)) return error.ChangefeedConflict;
                try restart.definition.validate();
                if (!equal(restart.definition.owner, state.definition.?.owner)) return error.ChangefeedConflict;
                try lease(restart.now, restart.expires_at);
                try bootstrapLease(restart.definition.start, restart.now, restart.expires_at, restart.bootstrap_expires_at);
                next = .{ .id = state.id, .epoch = std.math.add(u64, state.epoch, 1) catch return error.ChangefeedConflict, .authority = state.authority, .definition = restart.definition, .phase = .enrolling, .created_at = restart.now, .expires_at = restart.expires_at, .bootstrap_expires_at = restart.bootstrap_expires_at };
            },
        }
        next.revision = std.math.add(u64, state.revision, 1) catch return error.ChangefeedConflict;
        next.authority_sequence = stamp.sequence;
        try next.validate();
        return next;
    }
};
fn lease(now: u64, expires: u64) !void {
    if (now == 0 or expires <= now or expires - now > max_lease_seconds) return error.InvalidChangefeed;
}
fn bootstrapLease(start: Start, now: u64, expires: u64, bootstrap: u64) !void {
    if (start == .now) {
        if (bootstrap != 0) return error.InvalidChangefeed;
    } else if (bootstrap <= now or bootstrap > expires or bootstrap - now > max_bootstrap_seconds) return error.InvalidChangefeed;
}
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
pub const Parsed = struct {
    value: State,
    envelope: std.json.Parsed(Envelope),
    pub fn deinit(self: *@This()) void {
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
    var envelope = std.json.parseFromSlice(Envelope, a, bytes, .{ .allocate = .alloc_always, .parse_numbers = false }) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.ChangefeedCorrupt,
    };
    errdefer envelope.deinit();
    envelope.value.state.validate() catch return error.ChangefeedCorrupt;
    if (!equal(envelope.value.digest, hash(envelope.value.state))) return error.ChangefeedCorrupt;
    return .{ .value = envelope.value.state, .envelope = envelope };
}
pub fn encodeWrite(a: A, write: Write) ![]u8 {
    try write.validate();
    const bytes = try std.json.Stringify.valueAlloc(a, write, .{});
    errdefer a.free(bytes);
    if (bytes.len > max_state_bytes) return error.InvalidChangefeed;
    return bytes;
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
    return hash(.{ .domain = "antfly:changefeed-table-binding:v1", .table_id = table.table_id, .engine = table.storage.engine, .object_generation = table.object_storage_generation, .object_identity = table.object_storage_identity, .restore_backup_id = table.restore_backup_id, .restore_location = table.restore_location });
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
pub fn keyForGroup(buffer: []u8, group: u64, id: Id) ![]const u8 {
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:changefeeds:{d}:state:{s}", .{ group, std.fmt.bytesToHex(id, .lower) });
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
    try std.testing.expectEqual(@as(u64, 10), state.roots[0].reclaimed_through);
    try std.testing.expectError(error.ChangefeedConflict, step(scratch, state, .{ .reclaimed = .{ .namespace = testNamespace(7), .consumer_epoch = 6, .previous = 10, .next = 11 } }));
    const second = testDelivery(state, &.{.{ .namespace = testNamespace(7), .position = .{ .sequence = 11 } }});
    state = try step(scratch, state, .{ .issue = second });
    state = try step(scratch, state, .{ .acknowledge = second.cursor });
    state = try step(scratch, state, .{ .reclaimed = .{ .namespace = testNamespace(7), .consumer_epoch = 6, .previous = 10, .next = 11 } });
    try std.testing.expectEqual(@as(u64, 11), state.roots[0].reclaimed_through);
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
    try std.testing.expectError(error.ChangefeedConflict, step(scratch, state, .{ .finish_bootstrap = .{ .proof = @splat(1) } }));
    state = try step(scratch, state, .{ .acknowledge = page.cursor });
    state = try step(scratch, state, .{ .finish_bootstrap = .{ .proof = @splat(1) } });
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
    try svc.ensureLinearizableReadWithContext(request);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    return store.getChangefeedControl(a, svc.metadata_group_id, id);
}
pub fn mutateOnService(svc: anytype, a: A, write: Write, request: anytype) !void {
    try write.validate();
    try request.ensureActive();
    _ = try svc.ensureTableTopologyProtocolReadyWithContext(request, protocol_version);
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
