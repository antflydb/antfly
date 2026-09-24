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

//! Attempt-bound receiver progress. This is not a source pin issuer: only a
//! coordinator holding an authenticated, transferable immutable source may
//! opt into this protocol. Live DB snapshots are not an acceptable fallback.
const std = @import("std");
const Namespace = @import("doc_identity_namespace.zig").Namespace;
const Attempt = @import("relational_integrity_handoff_contract.zig").MergeCopyAttempt;

pub const key = "\x00\x00__metadata__:raftmerge:page";
pub const max_rows = 128;
pub const max_bytes = 1024 * 1024;
pub const max_cursor_bytes = 1024 * 1024;
pub const Digest = [32]u8;
pub const chunk_bytes = 1024 * 1024;
pub const max_chunk_bytes = chunk_bytes;
pub const max_row_bytes = std.math.maxInt(u32);
pub const Chunk = struct {
    row_key: []const u8,
    timestamp: u64,
    total_bytes: u64,
    row_digest: Digest,
    offset: u64,
    data: []const u8,
    chunk_digest: Digest,

    pub fn complete(self: Chunk) bool {
        return self.offset +| self.data.len == self.total_bytes;
    }
};
pub const Assembly = struct {
    transfer_digest: Digest,
    next_offset: u64,
    last_digest: Digest,
};
pub const Retention = struct { epoch: u64, after_sequence: u64 };
/// Derived from the transferable source certificate, never a live catalog
/// lookup. Both owners must retain exactly this constraint generation set.
pub const IntegrityBinding = struct {
    catalog_digest: Digest,
    generation_set: Digest,
};
pub const IntegrityEffect = struct { key: []const u8, value: ?[]const u8 };

/// Source-certified archive location, distinct from the logical row cursor.
/// It lets another donor replica resume without rescanning earlier rows.
pub const SnapshotPosition = struct {
    object: u32,
    offset: u64,
    remaining: u32,
    pub fn order(a: SnapshotPosition, b: SnapshotPosition) std.math.Order {
        const object_order = std.math.order(a.object, b.object);
        return if (object_order != .eq) object_order else std.math.order(a.offset, b.offset);
    }
};

pub const Source = struct {
    namespace: Namespace,
    pin_digest: Digest,
    applied_index: u64,
    retention: ?Retention = null,
    integrity: ?IntegrityBinding = null,

    pub fn jsonStringify(self: @This(), stream: anytype) !void {
        try @import("relational_integrity_json.zig").write(self, stream);
    }

    pub fn eql(a: Source, b: Source) bool {
        return a.namespace.eql(b.namespace) and a.applied_index == b.applied_index and std.mem.eql(u8, &a.pin_digest, &b.pin_digest) and std.meta.eql(a.retention, b.retention) and std.meta.eql(a.integrity, b.integrity);
    }
    pub fn validate(self: Source) !void {
        if (self.namespace.table_id == 0 or self.namespace.shard_id == 0 or self.namespace.range_id == 0 or
            self.applied_index == 0 or std.mem.allEqual(u8, &self.pin_digest, 0)) return error.InvalidMergePage;
        if (self.retention) |retention| if (retention.epoch == 0) return error.InvalidMergePage;
        if (self.integrity) |binding| if (self.retention == null or std.mem.allEqual(u8, &binding.catalog_digest, 0) or std.mem.allEqual(u8, &binding.generation_set, 0)) return error.InvalidMergePage;
    }
};

pub const Phase = enum { cleanup, cleanup_integrity, rows, artifacts, tail, complete };
pub const Tail = union(enum) {
    fragment: struct {
        sequence: u64,
        offset: u32,
        total_effects: u32,
        /// SHA-256 of the complete immutable retained frame, not its fragment.
        frame_digest: Digest,
    },
    finish: struct {
        through_sequence: u64,
        applied_index: u64,
        /// Authenticated source cut/barrier proof. This contract never issues it.
        cut_digest: Digest,
    },
};

pub const Command = struct {
    next_snapshot_position: ?SnapshotPosition = null,
    source: Source,
    sequence: u64,
    phase: Phase,
    /// Exclusive durable cursor. Empty means the beginning of this phase.
    after: []const u8 = "",
    next: []const u8 = "",
    exhausted: bool,
    digest: Digest,
    /// Original metadata for each sorted row; never synthesized at replay time.
    timestamps: []const u64 = &.{},
    tail: ?Tail = null,
    chunk: ?Chunk = null,
    /// Native, already-keyed claim/reference/job afterimages. They never pass
    /// through the document mapper or an ordinary caller-authored batch.
    integrity: []const IntegrityEffect = &.{},

    pub fn jsonStringify(self: @This(), stream: anytype) !void {
        try @import("merge_page_wire.zig").write(self, stream);
    }

    pub fn jsonParse(alloc: std.mem.Allocator, source: anytype, options: std.json.ParseOptions) !@This() {
        return @import("merge_page_wire.zig").parse(alloc, source, options);
    }

    pub fn jsonParseFromValue(alloc: std.mem.Allocator, value: std.json.Value, options: std.json.ParseOptions) !@This() {
        return @import("merge_page_wire.zig").parseValueLeaky(alloc, value, options);
    }
};

pub const Progress = struct {
    snapshot_position: ?SnapshotPosition = null,
    version: u8 = 1,
    transition_id: u64,
    donor_group_id: u64,
    receiver_group_id: u64,
    receiver_namespace: Namespace,
    attempt: Attempt,
    source: Source,
    phase: Phase = .cleanup,
    cursor: []const u8 = "",
    sequence: u64 = 0,
    last_digest: Digest = @splat(0),
    /// Only fully consumed frames may authorize source acknowledgement/GC.
    tail_sequence: u64 = 0,
    tail_offset: u32 = 0,
    tail_total_effects: u32 = 0,
    tail_frame_digest: Digest = @splat(0),
    final_applied_index: u64 = 0,
    final_cut_digest: Digest = @splat(0),
    assembly: ?Assembly = null,
    last_completed_transfer: Digest = @splat(0),

    pub fn jsonStringify(self: @This(), stream: anytype) !void {
        try @import("relational_integrity_json.zig").write(self, stream);
    }
    pub fn matches(self: Progress, context: anytype) bool {
        return self.transition_id == context.transition_id and self.donor_group_id == context.donor_group_id and
            self.receiver_group_id == context.receiver_group_id and self.receiver_namespace.eql(context.identity_namespace) and
            self.attempt.order(context.copy_attempt) == .eq;
    }
    /// Shared allocation-free validation for both durable decoding and inline
    /// private HTTP receipts. Parsing a typed struct is not admission proof.
    pub fn validate(self: Progress) !void {
        if ((self.version != 4 and self.version != 3 and self.version != (if (self.source.retention != null) @as(u8, 2) else @as(u8, 1))) or self.cursor.len > max_cursor_bytes) return error.InvalidMergePage;
        if (self.snapshot_position) |position| if (self.version != 4 or self.source.retention == null or (self.phase != .rows and !(self.phase == .artifacts and self.source.integrity != null)) or position.offset < 4) return error.InvalidMergePage;
        if (self.assembly) |assembly| if ((self.version != 3 and self.version != 4) or assembly.next_offset == 0 or assembly.next_offset % chunk_bytes != 0 or
            assembly.next_offset >= max_row_bytes or std.mem.allEqual(u8, &assembly.transfer_digest, 0) or (self.phase != .rows and self.phase != .tail)) return error.InvalidMergePage;
        try self.source.validate();
        if (self.source.retention) |retention| {
            if (self.tail_sequence < retention.after_sequence or self.tail_offset > self.tail_total_effects or
                self.tail_total_effects > 65536 or (self.tail_offset != 0 and std.mem.allEqual(u8, &self.tail_frame_digest, 0))) return error.InvalidMergePage;
            if (self.phase == .complete and (self.final_applied_index < self.source.applied_index or
                self.tail_offset != 0 or std.mem.allEqual(u8, &self.final_cut_digest, 0))) return error.InvalidMergePage;
        } else if (self.phase == .tail) return error.InvalidMergePage;
    }
};

pub fn decode(alloc: std.mem.Allocator, raw: []const u8) !std.json.Parsed(Progress) {
    if (raw.len > 8 * 1024 * 1024) return error.InvalidMergePage;
    var parsed = try std.json.parseFromSlice(Progress, alloc, raw, .{ .allocate = .alloc_always });
    errdefer parsed.deinit();
    try parsed.value.validate();
    return parsed;
}

pub fn encode(alloc: std.mem.Allocator, progress: Progress) ![]u8 {
    return std.json.Stringify.valueAlloc(alloc, progress, .{});
}

fn bytes(hasher: *std.crypto.hash.sha2.Sha256, value: []const u8) void {
    var length: [8]u8 = undefined;
    std.mem.writeInt(u64, &length, value.len, .little);
    hasher.update(&length);
    hasher.update(value);
}

fn hashCount(hasher: *std.crypto.hash.sha2.Sha256, value: u64) void {
    var length: [8]u8 = undefined;
    std.mem.writeInt(u64, &length, value, .little);
    hasher.update(&length);
}

/// The digest covers ordered final API row/artifact effects and every cursor
/// field. A retry cannot change content while retaining the same page number.
pub fn commandDigest(request: anytype) Digest {
    const command = request.merge_page.?;
    const context = request.merge_replication.?;
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    bytes(&hash, "antfly-merge-page-v1");
    inline for (.{ context.transition_id, context.donor_group_id, context.receiver_group_id, context.identity_namespace.table_id, context.identity_namespace.shard_id, context.identity_namespace.range_id, context.copy_attempt.donor_term, context.copy_attempt.sequence, command.source.namespace.table_id, command.source.namespace.shard_id, command.source.namespace.range_id, command.source.applied_index, command.sequence }) |number| {
        var buffer: [8]u8 = undefined;
        std.mem.writeInt(u64, &buffer, number, .little);
        hash.update(&buffer);
    }
    hash.update(&command.source.pin_digest);
    if (command.source.retention) |retention| {
        bytes(&hash, "retained-tail-v1");
        hashCount(&hash, retention.epoch);
        hashCount(&hash, retention.after_sequence);
    }
    if (command.source.integrity) |binding| {
        bytes(&hash, "integrity-shadow-v1");
        hash.update(&binding.catalog_digest);
        hash.update(&binding.generation_set);
    }
    bytes(&hash, @tagName(command.phase));
    bytes(&hash, command.after);
    bytes(&hash, command.next);
    if (command.next_snapshot_position) |position| {
        bytes(&hash, "snapshot-position-v1");
        hashCount(&hash, position.object);
        hashCount(&hash, position.offset);
        hashCount(&hash, position.remaining);
    }
    hash.update(&.{@intFromBool(command.exhausted)});
    if (command.tail) |tail| switch (tail) {
        .fragment => |fragment| {
            bytes(&hash, "fragment");
            hashCount(&hash, fragment.sequence);
            hashCount(&hash, fragment.offset);
            hashCount(&hash, fragment.total_effects);
            hash.update(&fragment.frame_digest);
        },
        .finish => |finish| {
            bytes(&hash, "finish");
            hashCount(&hash, finish.through_sequence);
            hashCount(&hash, finish.applied_index);
            hash.update(&finish.cut_digest);
        },
    };
    if (command.chunk) |chunk| {
        bytes(&hash, "row-chunk-v1");
        bytes(&hash, chunk.row_key);
        hashCount(&hash, chunk.timestamp);
        hashCount(&hash, chunk.total_bytes);
        hash.update(&chunk.row_digest);
        hashCount(&hash, chunk.offset);
        hash.update(&chunk.chunk_digest);
        bytes(&hash, chunk.data);
        return hash.finalResult();
    }
    hashCount(&hash, command.timestamps.len);
    for (command.timestamps) |timestamp| {
        var buffer: [8]u8 = undefined;
        std.mem.writeInt(u64, &buffer, timestamp, .little);
        hash.update(&buffer);
    }
    bytes(&hash, "writes");
    hashCount(&hash, request.writes.len);
    for (request.writes) |row| {
        bytes(&hash, row.key);
        bytes(&hash, row.value);
    }
    bytes(&hash, "deletes");
    hashCount(&hash, request.deletes.len);
    for (request.deletes) |row| bytes(&hash, row);
    bytes(&hash, "artifacts");
    hashCount(&hash, request.merge_artifacts.len);
    for (request.merge_artifacts) |row| {
        bytes(&hash, row.key);
        bytes(&hash, row.value);
    }
    if (command.integrity.len != 0) {
        bytes(&hash, "integrity-effects-v1");
        hashCount(&hash, command.integrity.len);
        for (command.integrity) |effect| {
            bytes(&hash, effect.key);
            hash.update(&.{@intFromBool(effect.value != null)});
            if (effect.value) |value| bytes(&hash, value);
        }
    }
    return hash.finalResult();
}

/// Identical for every chunk of this exact row/page/frame and copy attempt.
pub fn transferDigest(request: anytype) Digest {
    var identity_request = request;
    var chunk = identity_request.merge_page.?.chunk.?;
    chunk.offset = 0;
    chunk.data = "";
    chunk.chunk_digest = @splat(0);
    identity_request.merge_page.?.chunk = chunk;
    return commandDigest(identity_request);
}

/// Borrowed immutable logical row; hashing is once per row, independently of
/// retries. The receiver's durable assembly offset is the resume coordinate.
pub fn RowChunks(comptime Request: type) type {
    return struct {
        base: Request,
        row_digest: Digest,
        pub fn init(request: Request) !@This() {
            try validateRequest(request);
            if (request.merge_page.?.chunk != null or request.merge_page.?.integrity.len != 0 or request.writes.len != 1 or request.deletes.len != 0 or request.merge_artifacts.len != 0 or
                (request.merge_page.?.phase != .rows and request.merge_page.?.phase != .tail)) return error.InvalidMergePage;
            if (request.writes[0].value.len > max_row_bytes) return error.TransactionTooLarge;
            var digest: Digest = undefined;
            std.crypto.hash.sha2.Sha256.hash(request.writes[0].value, &digest, .{});
            return .{ .base = request, .row_digest = digest };
        }
        pub fn requestAt(self: @This(), offset: u64) !Request {
            const row = self.base.writes[0];
            if (offset >= row.value.len or offset % chunk_bytes != 0) return error.InvalidMergePage;
            const start: usize = @intCast(offset);
            const data = row.value[start..@min(row.value.len, start +| chunk_bytes)];
            var checksum: Digest = undefined;
            std.crypto.hash.sha2.Sha256.hash(data, &checksum, .{});
            var result = self.base;
            result.writes = &.{};
            result.merge_page.?.timestamps = &.{};
            result.merge_page.?.chunk = .{ .row_key = row.key, .timestamp = self.base.merge_page.?.timestamps[0], .total_bytes = row.value.len, .row_digest = self.row_digest, .offset = offset, .data = data, .chunk_digest = checksum };
            result.merge_page.?.digest = commandDigest(result);
            try validateRequest(result);
            return result;
        }
    };
}

pub fn effectCount(request: anytype) usize {
    return if (request.merge_page.?.chunk != null) 1 else request.writes.len + request.deletes.len + request.merge_artifacts.len + request.merge_page.?.integrity.len;
}

/// Shape validation happens before row preparation; memory/CPU cost is bounded
/// by one page, except for one indivisible oversized row or artifact.
pub fn validateRequest(request: anytype) !void {
    const command = request.merge_page orelse return;
    if (request.merge_replication == null or request.merge_checkpoint != null or request.restore_staging != null or
        request.restore_staging_scope != null or request.restore_staging_plan_id != null or request.transaction != null or request.relational_topology != null or
        request.split_replication != null or request.split_checkpoint != null or request.split_transition != null or
        request.merge_source_transition != null or request.online_source != null or request.transforms.len != 0 or request.predicates.len != 0 or
        request.graph_writes.len != 0 or request.graph_deletes.len != 0 or request.integrity.len != 0 or
        request.integrity_commands.len != 0 or request.relational_activation != null or request.relational_retirement != null or
        request.relational_index_maintenance != null or request.relational_repair)
        return error.InvalidMergePage;
    try command.source.validate();
    if (command.next_snapshot_position) |position| if (command.source.retention == null or (command.phase != .rows and !(command.phase == .artifacts and command.source.integrity != null)) or position.offset < 4) return error.InvalidMergePage;
    if (command.integrity.len != 0 and (command.source.integrity == null or (command.phase != .cleanup_integrity and command.phase != .artifacts and command.phase != .tail) or request.merge_artifacts.len != 0 or command.chunk != null)) return error.InvalidMergePage;
    if (request.timestamp_ns != 0 or (command.chunk == null and command.timestamps.len != request.writes.len)) return error.InvalidMergePage;
    if (command.chunk) |chunk| {
        if ((command.phase != .rows and command.phase != .tail) or request.deletes.len != 0 or request.merge_artifacts.len != 0 or
            command.timestamps.len != 0 or chunk.row_key.len == 0 or chunk.row_key.len > max_cursor_bytes or chunk.total_bytes == 0 or
            chunk.offset % chunk_bytes != 0 or chunk.offset >= chunk.total_bytes or chunk.data.len == 0 or chunk.data.len > chunk_bytes or
            chunk.data.len > chunk.total_bytes - chunk.offset or (!chunk.complete() and chunk.data.len != chunk_bytes)) return error.InvalidMergePage;
        if (chunk.total_bytes > max_row_bytes) return error.TransactionTooLarge;
        var checksum: Digest = undefined;
        std.crypto.hash.sha2.Sha256.hash(chunk.data, &checksum, .{});
        if (!std.mem.eql(u8, &checksum, &chunk.chunk_digest)) return error.InvalidMergePage;
        // The native finalizer may attach the assembled row for preparation;
        // wire adapters must leave ordinary row effects empty for chunks.
        if (request.writes.len != 0) {
            if (!chunk.complete() or request.writes.len != 1 or !std.mem.eql(u8, request.writes[0].key, chunk.row_key) or request.writes[0].value.len != chunk.total_bytes) return error.InvalidMergePage;
            std.crypto.hash.sha2.Sha256.hash(request.writes[0].value, &checksum, .{});
            if (!std.mem.eql(u8, &checksum, &chunk.row_digest)) return error.InvalidMergePage;
        }
    }
    if (command.phase == .complete or command.sequence == 0 or command.after.len > max_cursor_bytes or command.next.len > max_cursor_bytes)
        return error.InvalidMergePage;
    if ((command.phase == .tail) != (command.tail != null)) return error.InvalidMergePage;
    switch (command.phase) {
        .cleanup => if (request.writes.len != 0 or request.merge_artifacts.len != 0) return error.InvalidMergePage,
        .cleanup_integrity => if (command.source.integrity == null or request.writes.len != 0 or request.deletes.len != 0 or request.merge_artifacts.len != 0) return error.InvalidMergePage,
        .rows => if (request.deletes.len != 0 or request.merge_artifacts.len != 0) return error.InvalidMergePage,
        .artifacts => if (request.writes.len != 0 or request.deletes.len != 0) return error.InvalidMergePage,
        .tail => if (request.merge_artifacts.len != 0 or command.source.retention == null or command.after.len != 0 or command.next.len != 0) return error.InvalidMergePage,
        .complete => unreachable,
    }
    const count = effectCount(request);
    if (count > max_rows or (command.phase != .tail and count == 0 and (!command.exhausted or command.next.len != 0))) return error.InvalidMergePage;
    if (command.tail) |tail| switch (tail) {
        .fragment => |fragment| {
            if (command.exhausted or fragment.sequence == 0 or fragment.total_effects == 0 or fragment.total_effects > 65536 or
                count == 0 or fragment.offset >= fragment.total_effects or count > fragment.total_effects - fragment.offset or
                std.mem.allEqual(u8, &fragment.frame_digest, 0)) return error.InvalidMergePage;
        },
        .finish => |finish| if (!command.exhausted or count != 0 or finish.applied_index < command.source.applied_index or
            std.mem.allEqual(u8, &finish.cut_digest, 0)) return error.InvalidMergePage,
    };
    var size: usize = 0;
    var previous = command.after;
    if (command.chunk) |chunk| if (request.writes.len == 0) try ordered(&previous, chunk.row_key);
    for (request.writes) |row| {
        try ordered(&previous, row.key);
        size +|= row.key.len +| row.value.len;
    }
    if (command.phase == .tail) previous = "";
    for (request.deletes) |row| {
        try ordered(&previous, row);
        size +|= row.len;
    }
    for (request.merge_artifacts) |row| {
        try ordered(&previous, row.key);
        size +|= row.key.len +| row.value.len;
    }
    if (command.phase == .tail) previous = "";
    const integrity_contract = @import("relational_integrity_contract.zig");
    for (command.integrity) |effect| {
        try ordered(&previous, effect.key);
        _ = integrity_contract.parseKey(effect.key) catch return error.InvalidMergePage;
        if (effect.value) |value| {
            if (command.phase == .cleanup_integrity) return error.InvalidMergePage;
            _ = integrity_contract.validateTransferRecord(effect.key, value) catch return error.InvalidMergePage;
            size +|= value.len;
        } else if (command.phase != .tail and command.phase != .cleanup_integrity) return error.InvalidMergePage;
        size +|= effect.key.len;
    }
    if (count > 1 and size > max_bytes) return error.InvalidMergePage;
    if (command.phase == .tail) {
        // Arrays partition a single ordered source fragment. A primary key
        // cannot be both written and deleted by that source transaction.
        var write_index: usize = 0;
        var delete_index: usize = 0;
        while (write_index < request.writes.len and delete_index < request.deletes.len) {
            switch (std.mem.order(u8, request.writes[write_index].key, request.deletes[delete_index])) {
                .lt => write_index += 1,
                .gt => delete_index += 1,
                .eq => return error.InvalidMergePage,
            }
        }
    } else if (count != 0 and !std.mem.eql(u8, previous, command.next)) return error.InvalidMergePage;
    if (!std.mem.eql(u8, &command.digest, &commandDigest(request))) return error.InvalidMergePage;
}

fn ordered(previous: *[]const u8, key_bytes: []const u8) !void {
    if (key_bytes.len == 0 or (previous.*.len != 0 and std.mem.order(u8, previous.*, key_bytes) != .lt)) return error.InvalidMergePage;
    previous.* = key_bytes;
}

pub const Plan = union(enum) { replay, apply: Progress };

pub const CheckpointPlan = union(enum) { unchanged, clear, bind: Progress };

/// Shared native/projection control fold. A replayed begin cannot reset the
/// cursor; a newer attempt must restart cleanup and bind its own source pin.
pub fn checkpointPlan(prior: anytype, next: anytype, checkpoint: anytype, progress: ?Progress) !CheckpointPlan {
    if ((checkpoint.page_source != null) != (checkpoint.page_receiver_namespace != null) or
        (checkpoint.page_source != null and checkpoint.kind != .begin_copy and !(checkpoint.kind == .accept and checkpoint.page_source.?.integrity != null))) return error.InvalidMergeCheckpoint;
    const current = next.transition_id == checkpoint.transition_id and next.copy_attempt.order(checkpoint.copy_attempt) == .eq;
    const matching = if (progress) |value| value.transition_id == checkpoint.transition_id and
        value.donor_group_id == checkpoint.donor_group_id and value.receiver_group_id == checkpoint.receiver_group_id and
        value.attempt.order(checkpoint.copy_attempt) == .eq else false;
    if (checkpoint.kind == .rollback and current and matching and next.phase == .rolled_back) return .clear;
    if (checkpoint.kind == .begin_copy and current and next.phase == .accepting) {
        const new_attempt = if (prior) |state| state.copy_attempt.order(checkpoint.copy_attempt) != .eq else true;
        if (new_attempt) {
            if (checkpoint.page_source) |source| {
                try source.validate();
                const namespace = checkpoint.page_receiver_namespace.?;
                if (namespace.table_id == 0 or namespace.shard_id == 0 or namespace.range_id == 0) return error.InvalidMergePage;
                return .{ .bind = .{ .version = if (source.retention != null) 2 else 1, .transition_id = checkpoint.transition_id, .donor_group_id = checkpoint.donor_group_id, .receiver_group_id = checkpoint.receiver_group_id, .receiver_namespace = namespace, .attempt = checkpoint.copy_attempt, .source = source, .tail_sequence = if (source.retention) |retention| retention.after_sequence else 0 } };
            }
            return .clear;
        }
        if (checkpoint.page_source) |source| {
            if (!matching or !progress.?.source.eql(source) or !progress.?.receiver_namespace.eql(checkpoint.page_receiver_namespace.?)) return error.MergePageSourceMissing;
        } else if (matching) return error.MergePageRequired;
    }
    if (current and matching and (checkpoint.kind == .bootstrap_complete or checkpoint.kind == .finalize)) {
        const expected_index = if (progress.?.source.retention != null) progress.?.final_applied_index else progress.?.source.applied_index;
        if (progress.?.phase != .complete or checkpoint.bootstrap_applied_index != expected_index) return error.MergePageIncomplete;
    }
    return .unchanged;
}

pub fn validateRange(alloc: std.mem.Allocator, state: anytype, request: anytype) !void {
    const merged = state.merged_range orelse return error.InvalidMergeState;
    if (request.merge_page.?.chunk) |chunk| if (!merged.contains(chunk.row_key) or state.receiver_base_range.contains(chunk.row_key)) return error.KeyOutOfRange;
    for (request.writes) |row| if (!merged.contains(row.key) or state.receiver_base_range.contains(row.key)) return error.KeyOutOfRange;
    for (request.deletes) |row| if (!merged.contains(row) or state.receiver_base_range.contains(row)) return error.KeyOutOfRange;
    for (request.merge_artifacts) |row| {
        const owner = (try @import("../internal_keys.zig").decodeDocumentComponentAlloc(alloc, row.key)) orelse return error.InvalidBatchRequest;
        defer alloc.free(owner);
        if (!merged.contains(owner) or state.receiver_base_range.contains(owner)) return error.KeyOutOfRange;
    }
    for (request.merge_page.?.integrity) |effect| {
        const parsed = try @import("relational_integrity_contract.zig").parseKey(effect.key);
        if (!merged.contains(&parsed.address.routing) or state.receiver_base_range.contains(&parsed.address.routing)) return error.KeyOutOfRange;
    }
}

pub fn plan(progress: Progress, request: anytype) !Plan {
    const command = request.merge_page.?;
    if (!progress.matches(request.merge_replication.?) or !progress.source.eql(command.source)) return error.MergeCopyFenced;
    // Once a later page is durable, an old acknowledged page has no effects
    // left to apply. Its shape/digest was checked before this fold; only the
    // latest digest needs retention to detect conflicting immediate retries.
    if (command.sequence < progress.sequence) return .replay;
    if (command.sequence == progress.sequence) {
        if (command.chunk) |chunk| if (std.mem.eql(u8, &transferDigest(request), &progress.last_completed_transfer)) {
            if (chunk.complete() and !std.mem.eql(u8, &command.digest, &progress.last_digest)) return error.InvalidMergePage;
            return .replay;
        };
        if (!std.mem.eql(u8, &command.digest, &progress.last_digest)) return error.InvalidMergePage;
        return .replay;
    }
    if (command.sequence != try std.math.add(u64, progress.sequence, 1) or command.phase != progress.phase or
        !std.mem.eql(u8, command.after, progress.cursor)) return error.MergePageSequenceGap;
    if (command.tail) |tail| switch (tail) {
        .fragment => |fragment| {
            if (fragment.sequence != try std.math.add(u64, progress.tail_sequence, 1) or fragment.offset != progress.tail_offset) return error.MergePageSequenceGap;
            if (progress.tail_offset != 0 and (fragment.total_effects != progress.tail_total_effects or !std.mem.eql(u8, &fragment.frame_digest, &progress.tail_frame_digest))) return error.InvalidMergePage;
        },
        .finish => {},
    };
    var next = progress;
    if (command.next_snapshot_position) |position| {
        if (progress.snapshot_position) |previous| if (position.order(previous) != .gt) return error.InvalidMergePage;
    } else if (progress.snapshot_position != null and !command.exhausted) return error.InvalidMergePage;
    if (command.chunk) |chunk| {
        const transfer = transferDigest(request);
        const expected_offset = if (progress.assembly) |assembly| blk: {
            if (!std.mem.eql(u8, &assembly.transfer_digest, &transfer)) return error.InvalidMergePage;
            if (chunk.offset < assembly.next_offset) {
                if (chunk.offset + chunk.data.len == assembly.next_offset and !std.mem.eql(u8, &command.digest, &assembly.last_digest)) return error.InvalidMergePage;
                return .replay;
            }
            break :blk assembly.next_offset;
        } else 0;
        if (chunk.offset != expected_offset) return error.MergePageSequenceGap;
        next.version = if (command.next_snapshot_position != null or next.version == 4) 4 else 3;
        if (!chunk.complete()) {
            next.assembly = .{ .transfer_digest = transfer, .next_offset = chunk.offset + chunk.data.len, .last_digest = command.digest };
            return .{ .apply = next };
        }
        next.assembly = null;
        next.last_completed_transfer = transfer;
    } else {
        if (progress.assembly != null) return error.MergePageIncomplete;
        next.last_completed_transfer = @splat(0);
    }
    next.sequence = command.sequence;
    next.last_digest = command.digest;
    next.cursor = command.next;
    if (command.next_snapshot_position) |position| {
        next.version = 4;
        next.snapshot_position = position;
    }
    if (command.tail) |tail| {
        switch (tail) {
            .fragment => |fragment| {
                if (fragment.sequence != try std.math.add(u64, progress.tail_sequence, 1) or fragment.offset != progress.tail_offset)
                    return error.MergePageSequenceGap;
                if (progress.tail_offset != 0 and (fragment.total_effects != progress.tail_total_effects or
                    !std.mem.eql(u8, &fragment.frame_digest, &progress.tail_frame_digest))) return error.InvalidMergePage;
                next.tail_offset = fragment.offset + @as(u32, @intCast(effectCount(request)));
                next.tail_total_effects = fragment.total_effects;
                next.tail_frame_digest = fragment.frame_digest;
                if (next.tail_offset == fragment.total_effects) {
                    next.tail_sequence = fragment.sequence;
                    next.tail_offset = 0;
                    next.tail_total_effects = 0;
                    next.tail_frame_digest = @splat(0);
                }
            },
            .finish => |finish| {
                if (progress.tail_offset != 0 or finish.through_sequence != progress.tail_sequence) return error.MergePageSequenceGap;
                next.final_applied_index = finish.applied_index;
                next.final_cut_digest = finish.cut_digest;
                next.phase = .complete;
            },
        }
        return .{ .apply = next };
    }
    if (command.exhausted) {
        next.snapshot_position = null;
        next.phase = switch (command.phase) {
            .cleanup => if (progress.source.integrity != null) .cleanup_integrity else .rows,
            .cleanup_integrity => .rows,
            .rows => .artifacts,
            .artifacts => if (progress.source.retention != null) .tail else .complete,
            .tail => unreachable,
            .complete => unreachable,
        };
        next.cursor = "";
    }
    return .{ .apply = next };
}
