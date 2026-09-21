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

//! One retained frame/read lease, many bounded logical fragments. The frame is
//! authenticated once, never downloaded/rehashed for each 128-row page. The
//! caller must keep the source DB/owner lease alive until Session.deinit.
const std = @import("std");
const db_mod = @import("mod.zig");
const retained = @import("../retained_effects.zig");
const online = @import("online_source.zig");
const pages = @import("merge_page_contract.zig");
const internal = @import("../internal_keys.zig");
const types = @import("types.zig");
const SchemaView = @import("schema_registry.zig").SchemaView;

pub const Fragment = struct {
    arena: std.heap.ArenaAllocator,
    writes: []const types.BatchWrite,
    deletes: []const []const u8,
    timestamps: []const u64,
    integrity: []const pages.IntegrityEffect,
    tail: pages.Tail,
    frame_complete: bool,
    scope: online.Scope,
    admission: online.Progress,

    pub fn deinit(self: *Fragment) void {
        self.arena.deinit();
        self.* = undefined;
    }

    /// A consumer epoch is not a snapshot certificate. Only the native source
    /// publication record may bind these logical effects to a receiver copy.
    pub fn request(self: *const Fragment, source: pages.Source, context: types.MergeReplicationContext, sequence: u64) !types.BatchRequest {
        const result = try self.baseRequest(source, context, sequence);
        if (self.writes.len == 1 and self.writes[0].value.len +| self.writes[0].key.len > pages.max_bytes) return error.MergePageChunkRequired;
        return result;
    }

    /// Snapshot senders use the same RowChunks type with their immutable row.
    /// Keep this Fragment alive until the final chunk is acknowledged; only
    /// the receiver's completed page authorizes advancing/acknowledging frames.
    pub fn chunkRequests(self: *const Fragment, source: pages.Source, context: types.MergeReplicationContext, sequence: u64) !pages.RowChunks(types.BatchRequest) {
        return pages.RowChunks(types.BatchRequest).init(try self.baseRequest(source, context, sequence));
    }

    fn baseRequest(self: *const Fragment, source: pages.Source, context: types.MergeReplicationContext, sequence: u64) !types.BatchRequest {
        if (!std.meta.eql(source.integrity, if (self.admission.published_certificate) |certificate| certificate.integrity else null)) return error.SourceSnapshotCutMismatch;
        if (std.mem.allEqual(u8, &self.admission.snapshot_certificate, 0) or
            !std.mem.eql(u8, &source.pin_digest, &self.admission.snapshot_certificate) or
            !source.namespace.eql(self.scope.fence.namespace) or source.applied_index != self.admission.admitted_applied_index or
            source.retention == null or source.retention.?.epoch != self.scope.consumer_epoch or
            source.retention.?.after_sequence != self.admission.start or !context.identity_namespace.eql(self.scope.receiver_namespace) or
            context.transition_id != self.scope.fence.transition_id or context.donor_group_id != self.scope.fence.owner_group_id or
            context.receiver_group_id != self.scope.fence.peer_group_id or
            !std.meta.eql(context.copy_attempt, self.scope.copy_attempt)) return error.SourceSnapshotCutMismatch;
        var result: types.BatchRequest = .{
            .writes = self.writes,
            .deletes = self.deletes,
            .merge_replication = context,
            .merge_page = .{ .source = source, .sequence = sequence, .phase = .tail, .exhausted = false, .digest = @splat(0), .timestamps = self.timestamps, .tail = self.tail, .integrity = self.integrity },
        };
        result.merge_page.?.digest = pages.commandDigest(result);
        try pages.validateRequest(result);
        return result;
    }
};

pub const Session = struct {
    db: *db_mod.DB,
    txn: @import("../docstore.zig").DocStore.Txn,
    reader: retained.Reader,
    scope: online.Scope,
    admission: online.Progress,
    sequence: u64,
    total: u32,
    offset: u32 = 0,
    previous_primary: []const u8 = "",
    historical: ?SchemaView = null,

    pub fn open(db: *db_mod.DB, scope: online.Scope, after_sequence: u64) !?Session {
        try scope.validate();
        if (!db.core.identity_namespace.eql(scope.fence.namespace)) return error.OnlineSourceScopeChanged;
        var txn = try db.core.store.beginReadTxnWithBlockCacheAdmission(.transient);
        errdefer txn.abort();
        const admission = try online.status(&txn, scope);
        if (admission.phase == .released) return error.OnlineSourceScopeChanged;
        const reader = (try retained.read(&txn, scope.namespace(), scope.consumer_epoch, scope.pin(), after_sequence)) orelse {
            txn.abort();
            return null;
        };
        return .{ .db = db, .txn = txn, .reader = reader, .scope = scope, .admission = admission, .sequence = after_sequence + 1, .total = reader.remaining };
    }

    pub fn deinit(self: *Session) void {
        if (self.historical) |*view| view.release();
        self.txn.abort();
        self.* = undefined;
    }

    /// The caller may retry allocation/cancellation failure: no cursor moves
    /// until the entire owned fragment is prepared successfully.
    pub fn next(self: *Session, alloc: std.mem.Allocator, max_rows: usize, max_bytes: usize, cancellation: types.CancellationToken) !?Fragment {
        if (max_rows == 0 or max_rows > pages.max_rows or max_bytes == 0 or max_bytes > pages.max_bytes) return error.InvalidMergePage;
        try cancellation.check();
        if (self.reader.remaining == 0) return null;
        var arena = std.heap.ArenaAllocator.init(alloc);
        errdefer arena.deinit();
        const owned = arena.allocator();
        var writes: std.ArrayList(types.BatchWrite) = .empty;
        var deletes: std.ArrayList([]const u8) = .empty;
        var timestamps: std.ArrayList(u64) = .empty;
        var integrity: std.ArrayList(pages.IntegrityEffect) = .empty;
        var reader = self.reader;
        var previous = self.previous_primary;
        var count: usize = 0;
        var size: usize = 0;
        while (count < max_rows) {
            try cancellation.check();
            var peek = reader;
            const effect = (try peek.next()) orelse break;
            // Avoid decoding an obviously oversized next row just to defer it.
            if (count != 0 and effect.key.len +| (if (effect.value) |value| value.len else 0) > max_bytes -| size) break;
            if (effect.isIntegrity()) {
                const key = try owned.dupe(u8, effect.key);
                const value = if (effect.value) |raw| try owned.dupe(u8, raw) else null;
                try integrity.append(owned, .{ .key = key, .value = value });
                size +|= key.len +| if (value) |raw| raw.len else 0;
                count += 1;
                reader = peek;
                previous = effect.key;
                continue;
            }
            if (previous.len == effect.key.len and previous.len != 0 and std.mem.eql(u8, previous[0 .. previous.len - 1], effect.key[0 .. effect.key.len - 1])) return error.RetainedEffectsCorrupt;
            const logical = (try internal.decodeStoredDocumentRowKeyAlloc(owned, effect.key)) orelse return error.RetainedEffectsCorrupt;
            // Capture guarantees the typed row and TTL sidecar agree. Repeat
            // the check at this immutable read boundary before emitting rows.
            const timestamp = effect.timestamp;
            const value: ?[]const u8 = if (effect.value) |raw| if (internal.isRelationalRowKey(effect.key)) logical_value: {
                const version = try @import("relational_store.zig").rowSchemaVersion(raw);
                if (self.historical == null or self.historical.?.version() != version) {
                    if (self.historical) |*view| view.release();
                    self.historical = null;
                    self.historical = (try self.db.core.acquireSchemaVersionView(version)) orelse return error.UnknownSchemaVersion;
                }
                const view = self.historical.?;
                const row = try @import("algebraic/relational_row_codec.zig").ordinalRowViewSelective(raw, view.tableSchema().*, view.physicalLayout());
                if (row.writeTimestampNs() != timestamp) return error.RetainedEffectsCorrupt;
                break :logical_value try row.reconstructValueAlloc(owned);
            } else try owned.dupe(u8, raw) else null;
            const bytes = logical.len +| if (value) |raw| raw.len else 0;
            if (count != 0 and bytes > max_bytes -| size) break;
            if (value) |raw| {
                try writes.append(owned, .{ .key = logical, .value = raw });
                try timestamps.append(owned, timestamp);
            } else try deletes.append(owned, logical);
            size +|= bytes;
            count += 1;
            previous = effect.key;
            reader = peek;
        }
        try cancellation.check();
        const result: Fragment = .{
            .arena = arena,
            .writes = writes.items,
            .deletes = deletes.items,
            .timestamps = timestamps.items,
            .integrity = integrity.items,
            .tail = .{ .fragment = .{ .sequence = self.sequence, .offset = self.offset, .total_effects = self.total, .frame_digest = self.reader.frame_digest } },
            .frame_complete = reader.remaining == 0,
            .scope = self.scope,
            .admission = self.admission,
        };
        self.offset += @intCast(count);
        self.reader = reader;
        self.previous_primary = previous;
        return result;
    }
};
