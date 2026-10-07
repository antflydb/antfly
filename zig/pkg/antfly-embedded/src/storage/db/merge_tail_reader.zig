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

//! One retained frame/read lease, many bounded logical fragments. The frame is
//! authenticated once, never downloaded/rehashed for each 128-row page. The
//! caller must keep the source DB/owner lease alive until Session.deinit.
const std = @import("std");
const db_mod = @import("mod.zig");
const retained = @import("../retained_effects.zig");
const retained_frame = @import("../retained_frame.zig");
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
    artifact_effects: []const pages.IntegrityEffect,
    tail: pages.Tail,
    frame_complete: bool,
    scope: online.Scope,
    admission: online.Progress,
    streamed: ?StreamedEffect = null,

    const StreamedEffect = struct {
        session: *Session,
        key: []const u8,
        value_offset: u32,
        value_len: u32,
        timestamp: u64,
        payload: pages.ChunkPayload,
        digest: pages.Digest,
        scratch: []u8,
    };

    pub fn deinit(self: *Fragment) void {
        self.arena.deinit();
        self.* = undefined;
    }

    /// A consumer epoch is not a snapshot certificate. Only the native source
    /// publication record may bind these logical effects to a receiver copy.
    pub fn request(self: *const Fragment, source: pages.Source, context: types.MergeReplicationContext, sequence: u64) !types.BatchRequest {
        if (self.streamed != null) return error.MergePageChunkRequired;
        const result = try self.baseRequest(source, context, sequence);
        if (self.writes.len == 1 and self.writes[0].value.len +| self.writes[0].key.len > pages.max_bytes) return error.MergePageChunkRequired;
        if (self.artifact_effects.len == 1) if (self.artifact_effects[0].value) |value| if (value.len +| self.artifact_effects[0].key.len > pages.max_bytes) return error.MergePageChunkRequired;
        return result;
    }

    /// Snapshot senders use the same RowChunks type with their immutable row.
    /// Keep this Fragment alive until the final chunk is acknowledged; only
    /// the receiver's completed page authorizes advancing/acknowledging frames.
    pub const ChunkRequests = union(enum) {
        inline_row: pages.RowChunks(types.BatchRequest),
        streamed: struct { fragment: *Fragment, source: pages.Source, context: types.MergeReplicationContext, sequence: u64 },

        pub fn requestAt(self: ChunkRequests, offset: u64) !types.BatchRequest {
            return switch (self) {
                .inline_row => |row| row.requestAt(offset),
                .streamed => |value| value.fragment.streamRequestAt(value.source, value.context, value.sequence, offset),
            };
        }
    };

    pub fn chunkRequests(self: *Fragment, source: pages.Source, context: types.MergeReplicationContext, sequence: u64) !ChunkRequests {
        if (self.streamed != null) return .{ .streamed = .{ .fragment = self, .source = source, .context = context, .sequence = sequence } };
        return .{ .inline_row = try pages.RowChunks(types.BatchRequest).init(try self.baseRequest(source, context, sequence)) };
    }

    fn streamRequestAt(self: *Fragment, source: pages.Source, context: types.MergeReplicationContext, sequence: u64, offset: u64) !types.BatchRequest {
        const stream = &self.streamed.?;
        if (offset >= stream.value_len or offset % pages.chunk_bytes != 0) return error.InvalidMergePage;
        const len: usize = @intCast(@min(@as(u64, pages.chunk_bytes), @as(u64, stream.value_len) - offset));
        const view = stream.session.chunkView();
        if (try view.readAt(stream.value_offset + @as(u32, @intCast(offset)), stream.scratch[0..len], &stream.session.chunk_cache.?) != len) return error.RetainedEffectsCorrupt;
        var chunk_digest: pages.Digest = undefined;
        std.crypto.hash.sha2.Sha256.hash(stream.scratch[0..len], &chunk_digest, .{});
        var result = try self.baseRequestUnchecked(source, context, sequence);
        result.merge_page.?.chunk = .{ .payload = stream.payload, .row_key = stream.key, .timestamp = stream.timestamp, .total_bytes = stream.value_len, .row_digest = stream.digest, .offset = offset, .data = stream.scratch[0..len], .chunk_digest = chunk_digest };
        result.merge_page.?.digest = pages.commandDigest(result);
        try pages.validateRequest(result);
        return result;
    }

    fn baseRequest(self: *const Fragment, source: pages.Source, context: types.MergeReplicationContext, sequence: u64) !types.BatchRequest {
        const result = try self.baseRequestUnchecked(source, context, sequence);
        try pages.validateRequest(result);
        return result;
    }

    fn baseRequestUnchecked(self: *const Fragment, source: pages.Source, context: types.MergeReplicationContext, sequence: u64) !types.BatchRequest {
        if (!std.meta.eql(source.artifact_catalog, self.admission.artifact_catalog)) return error.SourceSnapshotCutMismatch;
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
            .merge_page = .{ .source = source, .sequence = sequence, .phase = .tail, .exhausted = false, .digest = @splat(0), .timestamps = self.timestamps, .tail = self.tail, .integrity = self.integrity, .artifact_effects = self.artifact_effects },
        };
        result.merge_page.?.digest = pages.commandDigest(result);
        return result;
    }
};

pub const Session = struct {
    db: *db_mod.DB,
    txn: @import("../docstore.zig").DocStore.Txn,
    frame: retained.Frame,
    chunk_buffer: ?[]u8 = null,
    chunk_cache: ?retained_frame.View.ChunkCache = null,
    previous_primary_owned: ?[]u8 = null,
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
        var legacy: ?retained.Reader = null;
        var needs_chunked = false;
        legacy = retained.read(&txn, scope.namespace(), scope.consumer_epoch, scope.pin(), after_sequence) catch |err| switch (err) {
            error.RetainedEffectsUnsupported => blk: {
                needs_chunked = true;
                break :blk null;
            },
            else => return err,
        };
        if (!needs_chunked) {
            const reader = legacy orelse {
                txn.abort();
                return null;
            };
            return .{ .db = db, .txn = txn, .frame = .{ .contiguous = reader }, .scope = scope, .admission = admission, .sequence = after_sequence + 1, .total = reader.remaining };
        }
        const buffer = try db.alloc.alloc(u8, retained_frame.chunk_bytes);
        errdefer db.alloc.free(buffer);
        var cache: retained_frame.View.ChunkCache = .{ .bytes = buffer };
        const frame = (try retained.readFrame(&txn, scope.namespace(), scope.consumer_epoch, scope.pin(), after_sequence, &cache)) orelse return error.RetainedEffectsCorrupt;
        const view = switch (frame) {
            .chunked => |value| value,
            .contiguous => return error.RetainedEffectsCorrupt,
        };
        return .{ .db = db, .txn = txn, .frame = .{ .chunked = view }, .chunk_buffer = buffer, .chunk_cache = cache, .scope = scope, .admission = admission, .sequence = after_sequence + 1, .total = view.effect_count };
    }

    pub fn deinit(self: *Session) void {
        if (self.historical) |*view| view.release();
        if (self.previous_primary_owned) |key| self.db.alloc.free(key);
        self.txn.abort();
        if (self.chunk_buffer) |buffer| self.db.alloc.free(buffer);
        self.* = undefined;
    }

    pub fn frameDigest(self: *const Session) retained_frame.Digest {
        return self.frame.digest();
    }

    pub fn frameTotal(self: *const Session) usize {
        return switch (self.frame) {
            .contiguous => |reader| reader.encoded_frame.len,
            .chunked => |view| view.total,
        };
    }

    pub fn frameDescriptor(self: *const Session) ?[]const u8 {
        return switch (self.frame) {
            .contiguous => null,
            .chunked => |view| view.descriptor,
        };
    }

    pub fn readEncoded(self: *Session, offset: u32, out: []u8) !usize {
        return switch (self.frame) {
            .contiguous => |reader| blk: {
                if (offset > reader.encoded_frame.len) return error.RetainedEffectsCursorMismatch;
                const len = @min(out.len, reader.encoded_frame.len - offset);
                @memcpy(out[0..len], reader.encoded_frame[offset..][0..len]);
                break :blk len;
            },
            .chunked => |stored_view| blk: {
                var view = stored_view;
                view.source = retained.chunkSource(&self.txn);
                break :blk try view.readAt(offset, out, &self.chunk_cache.?);
            },
        };
    }

    pub fn skipOne(self: *Session) !void {
        switch (self.frame) {
            .contiguous => |*reader| {
                const effect = (try reader.next()) orelse return error.RetainedEffectsCorrupt;
                self.previous_primary = effect.key;
            },
            .chunked => |stored_view| {
                if (self.offset >= self.total) return error.RetainedEffectsCorrupt;
                var view = stored_view;
                view.source = retained.chunkSource(&self.txn);
                const effect = try view.effectAt(self.offset, &self.chunk_cache.?);
                if (effect.key_len > pages.max_cursor_bytes) return error.RetainedEffectsCorrupt;
                const key = try self.db.alloc.alloc(u8, effect.key_len);
                errdefer self.db.alloc.free(key);
                if (try view.readAt(effect.key_offset, key, &self.chunk_cache.?) != key.len) return error.RetainedEffectsCorrupt;
                if (self.previous_primary_owned) |old| self.db.alloc.free(old);
                self.previous_primary_owned = key;
                self.previous_primary = key;
            },
        }
        self.offset += 1;
    }

    fn chunkView(self: *Session) retained_frame.View {
        var view = self.frame.chunked;
        view.source = retained.chunkSource(&self.txn);
        return view;
    }

    fn nextChunked(self: *Session, alloc: std.mem.Allocator, max_rows: usize, max_bytes: usize, cancellation: types.CancellationToken) !Fragment {
        var arena = std.heap.ArenaAllocator.init(alloc);
        errdefer arena.deinit();
        const owned = arena.allocator();
        var writes: std.ArrayList(types.BatchWrite) = .empty;
        var deletes: std.ArrayList([]const u8) = .empty;
        var timestamps: std.ArrayList(u64) = .empty;
        var integrity: std.ArrayList(pages.IntegrityEffect) = .empty;
        var artifact_effects: std.ArrayList(pages.IntegrityEffect) = .empty;
        const view = self.chunkView();
        var previous = self.previous_primary;
        var last_key: ?[]const u8 = null;
        var count: usize = 0;
        var size: usize = 0;
        var streamed: ?Fragment.StreamedEffect = null;
        while (count < max_rows and self.offset + count < self.total) {
            try cancellation.check();
            const effect = try view.effectAt(self.offset + @as(u32, @intCast(count)), &self.chunk_cache.?);
            if (effect.key_len > pages.max_cursor_bytes) return error.RetainedEffectsCorrupt;
            const key = try owned.alloc(u8, effect.key_len);
            if (try view.readAt(effect.key_offset, key, &self.chunk_cache.?) != key.len) return error.RetainedEffectsCorrupt;
            if (previous.len != 0 and std.mem.order(u8, previous, key) != .lt) return error.RetainedEffectsCorrupt;
            const is_integrity = @import("relational_integrity_contract.zig").isKey(key);
            const is_vector = @import("online_vector_artifacts.zig").isKey(key);
            const is_graph = @import("online_graph_artifacts.zig").isKey(key);
            const is_artifact = is_vector or is_graph;
            if (!is_integrity and !is_artifact and !internal.isStoredDocumentRowKey(key)) return error.RetainedEffectsCorrupt;
            if (is_integrity or is_artifact) {
                if (effect.timestamp != 0 or (is_artifact and !view.direct_vectors) or (is_graph and !view.graph_artifacts)) return error.RetainedEffectsCorrupt;
            }
            const logical = if (is_integrity or is_artifact) key else (try internal.decodeStoredDocumentRowKeyAlloc(owned, key)) orelse return error.RetainedEffectsCorrupt;
            const raw_len: usize = effect.value_len orelse 0;
            if (count != 0 and logical.len +| raw_len > max_bytes -| size) break;
            if (effect.value_len != null and raw_len +| logical.len > max_bytes and !internal.isRelationalRowKey(key) and !is_integrity) {
                if (count != 0) break;
                // A single oversized immutable after-image is streamed through
                // receiver chunk assembly. Hash once while traversing the
                // descriptor-authenticated bytes, never allocate the value.
                const scratch = try owned.alloc(u8, pages.chunk_bytes);
                var hash = std.crypto.hash.sha2.Sha256.init(.{});
                var value_pos: u32 = 0;
                while (value_pos < effect.value_len.?) {
                    try cancellation.check();
                    const n: usize = @intCast(@min(@as(u32, pages.chunk_bytes), effect.value_len.? - value_pos));
                    if (try view.readAt(effect.value_offset + value_pos, scratch[0..n], &self.chunk_cache.?) != n) return error.RetainedEffectsCorrupt;
                    hash.update(scratch[0..n]);
                    value_pos += @intCast(n);
                }
                streamed = .{ .session = self, .key = logical, .value_offset = effect.value_offset, .value_len = effect.value_len.?, .timestamp = effect.timestamp, .payload = if (is_artifact) .artifact else .row, .digest = hash.finalResult(), .scratch = scratch };
                count = 1;
                last_key = key;
                break;
            }
            const value: ?[]const u8 = if (effect.value_len) |raw_length| blk: {
                const raw = try owned.alloc(u8, raw_length);
                if (try view.readAt(effect.value_offset, raw, &self.chunk_cache.?) != raw.len) return error.RetainedEffectsCorrupt;
                if (internal.isRelationalRowKey(key)) {
                    const version = try @import("relational_store.zig").rowSchemaVersion(raw);
                    if (self.historical == null or self.historical.?.version() != version) {
                        if (self.historical) |*historical| historical.release();
                        self.historical = null;
                        self.historical = (try self.db.core.acquireSchemaVersionView(version)) orelse return error.UnknownSchemaVersion;
                    }
                    const historical = self.historical.?;
                    const row = try @import("algebraic/relational_row_codec.zig").ordinalRowViewSelective(raw, historical.tableSchema().*, historical.physicalLayout());
                    if (row.writeTimestampNs() != effect.timestamp) return error.RetainedEffectsCorrupt;
                    break :blk try row.reconstructValueAlloc(owned);
                }
                break :blk raw;
            } else null;
            if (is_integrity) {
                _ = @import("relational_integrity_contract.zig").parseKey(key) catch return error.RetainedEffectsCorrupt;
                if (value) |raw| _ = @import("relational_integrity_contract.zig").validateTransferRecord(key, raw) catch return error.RetainedEffectsCorrupt;
                try integrity.append(owned, .{ .key = key, .value = value });
            } else if (is_artifact) {
                if (@import("online_vector_artifacts.zig").isKey(key)) {
                    try @import("online_vector_artifacts.zig").validate(key, value);
                } else try @import("online_graph_artifacts.zig").validate(key, value);
                try artifact_effects.append(owned, .{ .key = key, .value = value });
            } else if (value) |raw| {
                try writes.append(owned, .{ .key = logical, .value = raw });
                try timestamps.append(owned, effect.timestamp);
            } else try deletes.append(owned, logical);
            size +|= logical.len +| if (value) |raw| raw.len else 0;
            count += 1;
            previous = key;
            last_key = key;
        }
        if (count == 0) return error.RetainedEffectsCorrupt;
        try cancellation.check();
        // The cursor's order key is independent of the returned fragment's
        // arena; it survives retries after that fragment is released.
        const owned_previous = try self.db.alloc.dupe(u8, last_key.?);
        if (self.previous_primary_owned) |old| self.db.alloc.free(old);
        self.previous_primary_owned = owned_previous;
        self.previous_primary = owned_previous;
        const result: Fragment = .{
            .arena = arena,
            .writes = writes.items,
            .deletes = deletes.items,
            .timestamps = timestamps.items,
            .integrity = integrity.items,
            .artifact_effects = artifact_effects.items,
            .tail = .{ .fragment = .{ .sequence = self.sequence, .offset = self.offset, .total_effects = self.total, .frame_digest = self.frameDigest() } },
            .frame_complete = self.offset + count == self.total,
            .scope = self.scope,
            .admission = self.admission,
            .streamed = streamed,
        };
        self.offset += @intCast(count);
        return result;
    }

    /// The caller may retry allocation/cancellation failure: no cursor moves
    /// until the entire owned fragment is prepared successfully.
    pub fn next(self: *Session, alloc: std.mem.Allocator, max_rows: usize, max_bytes: usize, cancellation: types.CancellationToken) !?Fragment {
        if (max_rows == 0 or max_rows > pages.max_rows or max_bytes == 0 or max_bytes > pages.max_bytes) return error.InvalidMergePage;
        try cancellation.check();
        if (self.offset == self.total) return null;
        if (self.frame == .chunked) return try self.nextChunked(alloc, max_rows, max_bytes, cancellation);
        var arena = std.heap.ArenaAllocator.init(alloc);
        errdefer arena.deinit();
        const owned = arena.allocator();
        var writes: std.ArrayList(types.BatchWrite) = .empty;
        var deletes: std.ArrayList([]const u8) = .empty;
        var timestamps: std.ArrayList(u64) = .empty;
        var integrity: std.ArrayList(pages.IntegrityEffect) = .empty;
        var artifact_effects: std.ArrayList(pages.IntegrityEffect) = .empty;
        var reader = self.frame.contiguous;
        var previous = self.previous_primary;
        var count: usize = 0;
        var size: usize = 0;
        while (count < max_rows) {
            try cancellation.check();
            var peek = reader;
            const effect = (try peek.next()) orelse break;
            // Avoid decoding an obviously oversized next row just to defer it.
            if (count != 0 and effect.key.len +| (if (effect.value) |value| value.len else 0) > max_bytes -| size) break;
            if (effect.isIntegrity() or effect.isVector()) {
                const key = try owned.dupe(u8, effect.key);
                const value = if (effect.value) |raw| try owned.dupe(u8, raw) else null;
                if (effect.isVector()) try artifact_effects.append(owned, .{ .key = key, .value = value }) else try integrity.append(owned, .{ .key = key, .value = value });
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
            .artifact_effects = artifact_effects.items,
            .tail = .{ .fragment = .{ .sequence = self.sequence, .offset = self.offset, .total_effects = self.total, .frame_digest = self.frameDigest() } },
            .frame_complete = reader.remaining == 0,
            .scope = self.scope,
            .admission = self.admission,
        };
        self.offset += @intCast(count);
        self.frame = .{ .contiguous = reader };
        self.previous_primary = previous;
        return result;
    }
};
